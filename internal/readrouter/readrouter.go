// Package readrouter decides, per read statement arriving on the MySQL-protocol
// port, whether MySQL or the analytical copy should answer it, and forwards to
// MySQL the ones the copy should not take (issue #2038).
//
// The decision is deliberately small: a fast structured rule with a fixed
// threshold, and a slow path (MySQL) that is always correct.
//
//	not a SELECT, inside a transaction, a session variable was set    -> MySQL
//	a construct the copy answers differently (Veto)                   -> MySQL
//	copy older than the server's max copy age                         -> MySQL (the handler's check)
//	EXPLAIN FORMAT=JSON: cheap plan, no large full scan               -> MySQL
//	expensive plan                                                    -> copy; the copy refuses it -> MySQL
//
// The plan is read for two things only: the top-level query_cost and, for
// every table in the plan, its access_type and rows_examined_per_scan. The
// optimizer's cost is in its own units (one random row read is about 1); it
// misleads on LIMIT, on cached data and on skewed values, which is why the
// threshold is a knob, the copy refusing is a fallback and not an error, and
// the copy only ever takes statements where the plan says "a lot of work".
package readrouter

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// Kind is the statement class the router keys its first decision on.
type Kind int

const (
	// KindOther is anything the copy never takes: writes, SHOW, DDL, EXPLAIN,
	// administrative statements. Forwarded to MySQL as is.
	KindOther Kind = iota
	// KindSelect is a read the plan decides.
	KindSelect
	// KindSet is a statement that changes what later statements on this
	// connection mean: SET (session settings the copy does not honour),
	// CREATE TEMPORARY TABLE (a name that shadows a real table on MySQL
	// only), LOCK/UNLOCK TABLES, PREPARE. Forwarded, and the connection
	// stops routing to the copy. A SET inside MySQL's executable comment
	// (`/*!40101 SET ... */`, what dumps and drivers send) counts.
	KindSet
	// KindTxnBegin / KindTxnEnd bound an explicit transaction. Everything
	// inside goes to MySQL (read-your-writes).
	KindTxnBegin
	KindTxnEnd
	// KindWrite is a statement that changes the source: writes and DDL.
	// Forwarded like KindOther, but named so the port can log it.
	KindWrite
)

func (k Kind) String() string {
	switch k {
	case KindSelect:
		return "select"
	case KindSet:
		return "set"
	case KindTxnBegin:
		return "begin"
	case KindTxnEnd:
		return "end"
	case KindWrite:
		return "write"
	default:
		return "other"
	}
}

var (
	// leadingComment strips ordinary comments; an executable comment
	// (`/*! ... */`, and MariaDB's `/*M! ... */`) is code the server may
	// run, so Classify looks inside it instead. The empty comment `/**/`
	// and `/*M*/` are spelled out: read as `/*`, the characters that rule
	// out an executable comment, and the rest, their `*` would be one of
	// those characters and the comment would run on to the next `*/`,
	// anywhere in the statement, taking the statement with it.
	leadingComment = regexp.MustCompile(`^(?s)(\s*(/\*\*/|/\*M\*/|/\*(?:[^!M]|M[^!]).*?\*/|--[^\n]*\n|#[^\n]*\n))*`)
	execComment    = regexp.MustCompile(`(?s)^\s*/\*M?!\d*\s*(.*?)\*/\s*$`)
	selectRE       = regexp.MustCompile(`(?is)^\s*\(*\s*(select|with)\b`)
	pinRE          = regexp.MustCompile(`(?is)^\s*(set|create\s+temporary|lock\s+tables|unlock\s+tables|prepare)\b`)
	beginRE        = regexp.MustCompile(`(?is)^\s*(begin|start\s+transaction)\b`)
	endRE          = regexp.MustCompile(`(?is)^\s*(commit|rollback)\b`)
	writeRE        = regexp.MustCompile(`(?is)^\s*(insert|update|delete|replace|create|alter|drop|truncate|rename|grant|revoke|load|call|flush|kill|optimize|analyze|repair|install|uninstall|reset|purge)\b`)
)

// execCommentStart: an executable comment opens the statement.
var execCommentStart = regexp.MustCompile(`^\s*/\*M?!`)

// Classify names the statement's class from its leading keyword, comments
// stripped. It never looks past the first keyword: a SELECT that writes
// (SELECT ... INTO OUTFILE) is caught by Veto, not here.
func Classify(stmt string) Kind {
	s := leadingComment.ReplaceAllString(mysqlSpaces(stmt), "")
	if m := execComment.FindStringSubmatch(s); m != nil {
		return Classify(m[1])
	}
	switch {
	case execCommentStart.MatchString(s):
		// An executable comment that is only the statement's beginning
		// (`/*!50000 SET x = 1, */ time_zone = '...'`): MySQL runs its
		// content as part of what follows, and this function reads no
		// keyword there. Classified as the kind that keeps the connection
		// on MySQL, which is right whatever the statement turns out to be.
		return KindSet
	case pinRE.MatchString(s):
		return KindSet
	case beginRE.MatchString(s):
		return KindTxnBegin
	case endRE.MatchString(s):
		return KindTxnEnd
	case selectRE.MatchString(s):
		return KindSelect
	case writeRE.MatchString(s):
		return KindWrite
	default:
		return KindOther
	}
}

var keywordRE = regexp.MustCompile(`(?i)^\s*([a-z]+)`)

// LeadingKeyword is the statement's first word, upper-cased, for a log line
// that must not carry the statement (its literals may be data).
func LeadingKeyword(stmt string) string {
	s := leadingComment.ReplaceAllString(mysqlSpaces(stmt), "")
	if m := execComment.FindStringSubmatch(s); m != nil {
		s = m[1]
	}
	if m := keywordRE.FindStringSubmatch(s); m != nil {
		return strings.ToUpper(m[1])
	}
	return "?"
}

// mysqlSpaces returns the statement with the control bytes MySQL reads as
// white space turned into spaces (a vertical tab, a form feed and the rest
// below 0x20), except the ones `\s` already matches. A statement that opens
// with a vertical tab is the statement after it to the server, and was an
// unknown one to the patterns above.
func mysqlSpaces(stmt string) string {
	clean := true
	for i := 0; i < len(stmt); i++ {
		if c := stmt[i]; c < ' ' && c != '\t' && c != '\n' && c != '\r' {
			clean = false
			break
		}
	}
	if clean {
		return stmt
	}
	b := []byte(stmt)
	for i, c := range b {
		if c < ' ' && c != '\t' && c != '\n' && c != '\r' {
			b[i] = ' '
		}
	}
	return string(b)
}

// vetoes are constructs the copy would answer DIFFERENTLY without an error
// (so "the copy refused it" cannot catch them), plus the reads that are only
// meaningful on the live server. Matched on the statement with its string
// literals blanked, case-insensitively, as whole words. The list grows as
// the comparison of real workloads finds more; it never shrinks without a
// comparison showing equality.
var vetoes = []struct {
	name string
	re   *regexp.Regexp
}{
	{"GROUP_CONCAT", regexp.MustCompile(`(?i)\bgroup_concat\s*\(`)}, // MySQL cuts at group_concat_max_len
	{"NOW/CURDATE/CURTIME/CURRENT_TIMESTAMP", regexp.MustCompile(`(?i)\b(now|curdate|curtime|current_timestamp|current_date|current_time|sysdate|utc_timestamp|utc_date|utc_time|unix_timestamp|localtime|localtimestamp)\b`)}, // MySQL session time zone
	// MySQL numbers weeks by default_week_format (from Sunday by default),
	// the copy by ISO weeks.
	{"WEEK/YEARWEEK", regexp.MustCompile(`(?i)\b(week|yearweek)\s*\(`)},
	{"EXTRACT(WEEK ...)", regexp.MustCompile(`(?i)\bextract\s*\(\s*week\b`)},
	{"STR_TO_DATE", regexp.MustCompile(`(?i)\bstr_to_date\s*\(`)},                                  // NULL in MySQL, error in DuckDB on a bad date
	{"TIMESTAMPDIFF/DATEDIFF", regexp.MustCompile(`(?i)\b(timestampdiff|datediff|timediff)\s*\(`)}, // day counting differs
	{"COLLATE", regexp.MustCompile(`(?i)\bcollate\b`)},                                             // collations do not exist on the copy
	{"CAST AS UNSIGNED/SIGNED", regexp.MustCompile(`(?i)\bas\s+(un)?signed\b`)},                    // overflow and truncation differ
	{"DIV", regexp.MustCompile(`(?i)\bdiv\b`)},                                                     // integer division differs
	{"RAND/UUID", regexp.MustCompile(`(?i)\b(rand|uuid|uuid_short)\s*\(`)},                         // nondeterministic
	{"FOUND_ROWS/LAST_INSERT_ID/ROW_COUNT", regexp.MustCompile(`(?i)\b(found_rows|last_insert_id|row_count|sql_calc_found_rows)\b`)},
	{"CONNECTION_ID/USER/DATABASE/VERSION", regexp.MustCompile(`(?i)\b(connection_id|current_user|session_user|system_user|user|database|schema|version)\s*\(`)},
	// Without parentheses CURRENT_USER is still the function on MySQL and
	// MariaDB (root@localhost), and the copy has its own (duckdb); so is
	// CURRENT_ROLE, with or without them (NONE on MySQL, NULL on MariaDB,
	// duckdb on the copy). Measured on MySQL 8.4.9, MariaDB 11.4 and the
	// copy (#2131). A leading quote, word character or dot means a quoted
	// name, a longer word or a column of that name.
	{"CURRENT_USER or CURRENT_ROLE without parentheses, or CURRENT_ROLE() (the source's user or role; the copy's own)", regexp.MustCompile(`(?i)(^|[^\x60"\w.])(current_user|current_role)\b`)},
	{"user or system variable", regexp.MustCompile(`@`)},
	{"locking read", regexp.MustCompile(`(?i)\b(for\s+update|lock\s+in\s+share\s+mode|for\s+share)\b`)},
	{"INTO (OUTFILE/DUMPFILE/variables)", regexp.MustCompile(`(?i)\binto\s+(outfile|dumpfile|@)`)},
	{"system schema", regexp.MustCompile(`(?i)\b(information_schema|performance_schema|mysql|sys)[\x60"]?\s*\.`)}, // quoted or not: the copy has an information_schema of its own, and would answer
	{"MATCH AGAINST", regexp.MustCompile(`(?i)\bmatch\s*\(.*\)\s*against\b`)},
	{"binary string comparison", regexp.MustCompile(`(?i)\bbinary\s+[\w\x60'"(]`)},
	{"LIKE/REGEXP (case-sensitive on the copy, case-insensitive on MySQL)", regexp.MustCompile(`(?i)\b(like|regexp|rlike)\b`)}, // the copy's default collation (nocase.icu_noaccent) does not reach LIKE (DuckDB #10416)
	{"DISTINCT inside an aggregate (not folded by the copy's collation)", regexp.MustCompile(`(?i)\b(count|sum|avg|min|max|group_concat)\s*\(\s*distinct\b`)},
	{"INSTR/LOCATE/POSITION/STRCMP (case-sensitive on the copy)", regexp.MustCompile(`(?i)\b(instr|locate|position|strcmp|field|find_in_set)\s*\(`)},
	{"|| (string concatenation on the copy, logical OR on MySQL)", regexp.MustCompile(`\|\|`)},
	{"JSON function or -> operator (missing or different on the copy)", regexp.MustCompile(`(?i)\bjson_[a-z_]+\s*\(|->>?`)}, // JSON_UNQUOTE does not exist on the copy; JSON_EXTRACT paths and quoting differ
	{"^ (power on the copy, bitwise XOR on MySQL)", regexp.MustCompile(`\^`)},
	{"-- without a space after it (two minus signs on MySQL, a comment on the copy)", regexp.MustCompile(`--`)}, // scrub already removed what MySQL reads as a comment, so any `--` left is arithmetic there (5--3 is 8) and the copy would drop the rest of the line
	// UNION, INTERSECT and EXCEPT that remove duplicates: the copy compares
	// the rows by bytes there, whatever its collation ('a' and 'A' stay two
	// rows; MySQL keeps one). UNION ALL removes nothing and is not matched: a
	// UNION is vetoed when what follows it is DISTINCT or the next query
	// (SELECT, VALUES, TABLE, an opening parenthesis). INTERSECT and EXCEPT
	// are vetoed with ALL too, since ALL still pairs rows up by comparing
	// them. A leading quote (a backtick, or the double quote it is rewritten
	// to for the copy), word character or dot means a quoted name, a
	// longer word or a column of that name, not the operator.
	{"UNION/INTERSECT/EXCEPT without ALL (duplicates removed by bytes on the copy, by collation on MySQL)", regexp.MustCompile(`(?i)(^|[^\x60"\w.])(intersect|except)([^\x60"\w]|$)|(^|[^\x60"\w.])union\s*(distinct\b|select\b|values\b|table\b|\()`)},
	// MySQL stops a recursion at cte_max_recursion_depth with an error and
	// MariaDB cuts it at max_recursive_iterations (1000 by default on both);
	// the copy runs it to the end.
	{"WITH RECURSIVE", regexp.MustCompile(`(?i)\bwith\s+recursive\b`)},
	// MariaDB takes $ for a character of a name ($$ and $x$ are names, and
	// MySQL takes a$b$ for one); the copy opens a dollar-quoted string
	// there, which runs to the next one. The class is "not ASCII" by code
	// point: \x80-\xff in a pattern would mean U+0080 to U+00FF only. Measured: `SELECT a AS $$, 2 AS $$`
	// is two columns on MariaDB 11.4 and on MySQL 8.0, and one on the copy;
	// MySQL 8.4 refuses a name that starts with $.
	{"$...$ (a name on the source, a dollar-quoted string on the copy)", regexp.MustCompile(`\$(?:\w|[^\x00-\x7f])*\$`)},
}

// hintComment matches an optimizer hint (`/*+`) and a comment the server
// executes: MySQL's and MariaDB's `/*!`, and MariaDB's `/*M!` (with a capital M:
// MariaDB 11.4 reads `/*m!` as a plain comment). It is matched on the raw
// text, so `SELECT '/*!'` is kept on MySQL too: an over-veto on purpose, since
// it means a hint cannot hide inside what only looks like a literal.
var hintComment = regexp.MustCompile(`/\*(M?!|\+)`)

// Veto returns the name of the first construct that keeps the statement on
// MySQL, or "" when none applies. The statement is read once (scan): comments
// are removed and string literals blanked, so a keyword inside a string or a
// quote inside a comment cannot hide or fake a match, and backtick-quoted
// names are written the way the copy gets them (ForCopy), so the list runs
// on the text the copy would run.
func Veto(stmt string) string {
	if hintComment.MatchString(stmt) {
		return vetoHintComment
	}
	sc := scan(stmt)
	if why := sc.literalVeto(); why != "" {
		return why
	}
	for _, v := range vetoes {
		if v.re.MatchString(sc.blankedCopy) {
			return v.name
		}
	}
	if why := shapeVeto(sc.blankedCopy); why != "" {
		return why
	}
	if why := copyReservedVeto(sc.blankedCopy); why != "" {
		return why
	}
	if sc.hash {
		// MySQL reads `#` to the end of the line as a comment, which scan
		// has removed as MySQL does; DuckDB reads `#2` as the second column
		// of the select. `SELECT #2<newline> alpha FROM t` is column alpha on
		// MySQL and the table's second column, named alpha, on the copy.
		return vetoHash
	}
	return ""
}

// Scrub is scrub for other packages: the statement with comments removed
// and string literals blanked, for structural checks such as "is there an
// ORDER BY at the top level".
func Scrub(stmt string) string {
	s, _, _, _ := scrub(stmt)
	return s
}

// scrub returns the statement with its comments removed (`/* */`, `-- `,
// `#`), its string literals replaced by ” and its backtick identifiers
// kept, plus whether a double-quoted string literal occurred and whether
// any string literal held a backslash, and whether a `#` comment occurred.
// See scan.
func scrub(stmt string) (blanked string, doubleQuoted, backslash, hash bool) {
	sc := scan(stmt)
	return sc.blanked, sc.doubleQuoted, sc.backslash, sc.hash
}

// Plan is what the router reads out of EXPLAIN FORMAT=JSON.
type Plan struct {
	// Cost is the top-level query_block's query_cost (optimizer units).
	Cost float64
	// CostUnknown is true when the plan carries no MySQL query cost: a
	// MariaDB plan. MariaDB before 11.0 reports no cost at all, and from
	// 11.0 on reports one in its own unit (about milliseconds: a 200,000-row
	// scan is 32, where MySQL says 20,000), which a threshold written for
	// MySQL's cannot be compared with. The cost rule does not apply then;
	// the scan rule does.
	CostUnknown bool
	// Tables is how many table accesses the plan has, derived tables included.
	Tables int
	// FullScans is how many of them are access_type ALL.
	FullScans int
	// MaxScanRows is the largest rows_examined_per_scan (MySQL) or rows
	// (MariaDB) over every table.
	MaxScanRows int64
	// MaxIndexScanRows is the largest row estimate over every table read by
	// walking a whole index (access_type index), outside subqueries. Under
	// an ORDER BY the index serves with a LIMIT, the server reports the few
	// rows it expects to walk, so a large value here is an index read end
	// to end. An index scan inside a subquery is left out: for EXISTS it
	// stops at the first entry, and the plan does not say which kind of
	// subquery it is. Only consulted when the plan has no cost.
	MaxIndexScanRows int64
	// Message is the optimizer's shortcut when there is no plan ("no matching
	// row in const table", "Impossible WHERE"): the query is trivial.
	Message string
	// Filesort is true when any ordering_operation sorts (using_filesort),
	// i.e. the ORDER BY is not served by an index. With a LIMIT, that is the
	// difference between MySQL reading the whole table and reading N rows.
	Filesort bool
	// ScanFilter is true when the plan both scans a table (access_type ALL,
	// index or range) and filters somewhere (an attached_condition on any
	// table): a condition no index serves, which MySQL evaluates row by row
	// as it scans. With a LIMIT under an index-served ORDER BY,
	// rows_examined_per_scan is then only the optimizer's guess (LIMIT over
	// the filter's assumed selectivity, across the join when the filter sits
	// on a joined table), while a rare or absent value makes MySQL walk the
	// whole index.
	ScanFilter bool
	// scans and conditions are the two halves ScanFilter is computed from.
	scans, conditions bool
	// costInfo records that some node of the plan carries MySQL's cost_info.
	costInfo bool
	// RowsRead estimates the rows the plan reads across its joins and its
	// per-row subqueries (joinrows.go): the parts that multiply. A table
	// read alone is not in it. Only for a plan with no cost (CostUnknown),
	// which has nothing else to tell a heavy join by; 0 on a MySQL plan
	// and when RowsReadUnknown says why there is no estimate.
	RowsRead int64
	// Joined is true when the plan has such a part: a table read once per
	// row of the tables before it, or a subquery or derived table run once
	// per outer row. False for one table, and for a UNION of single
	// tables.
	Joined bool
	// topSort is how the top block sorts (sortNone, sortFirstTable,
	// sortWholeJoin) and sortedFirstRows the rows of the table sorted
	// alone: what decides whether a LIMIT can end the join early.
	topSort         int8
	sortedFirstRows int64
	// RowsReadUnknown is why a plan with no cost has no RowsRead: a shape
	// the estimate does not know. The scan rules alone decide that plan.
	RowsReadUnknown string
	// ReadIsResult is true when the plan is one table, read alone, where
	// every row the access path reads is a row of the result: the table or
	// an index read whole with no condition, or an index read by key or by
	// range with nothing left to check row by row (exactread.go). With no
	// sort, a LIMIT then stops the read once it has its rows.
	ReadIsResult bool
	// ResultRows is the plan's row estimate for that table: an estimate of
	// the rows the statement returns, before any LIMIT, aggregate or
	// DISTINCT. Only meaningful with ReadIsResult.
	ResultRows int64
}

// ParsePlan reads the two things the decision needs from EXPLAIN FORMAT=JSON.
// It walks the whole tree: MySQL nests tables under nested_loop,
// grouping_operation, ordering_operation, duplicates_removal,
// materialized_from_subquery, union_result.query_specifications and more,
// and a plan's shape is not fixed, so every object with a "table" or a
// "cost_info" is visited wherever it sits.
func ParsePlan(explainJSON []byte) (Plan, error) {
	var root map[string]any
	if err := json.Unmarshal(explainJSON, &root); err != nil {
		return Plan{}, fmt.Errorf("explain output is not JSON: %w", err)
	}
	qb, ok := root["query_block"].(map[string]any)
	if !ok {
		return Plan{}, fmt.Errorf("explain output has no query_block")
	}
	var p Plan
	if ci, ok := qb["cost_info"].(map[string]any); ok {
		p.Cost = number(ci["query_cost"])
	}
	if m, ok := qb["message"].(string); ok {
		p.Message = m
	} else if t, ok := qb["table"].(map[string]any); ok {
		// MariaDB puts the shortcut on a table node of the top block
		// ("Impossible WHERE", "No tables used"). Only there: the same
		// message deeper down is one branch of a UNION, and the other
		// branches still do work. (A top block with no tables and a heavy
		// scalar subquery is called trivial too, on MySQL as well: it
		// stays on the source.)
		if m, ok := t["message"].(string); ok {
			p.Message = m
		}
	}
	walk(qb, &p, false)
	// A MySQL plan carries a cost_info on every table and block, even when
	// the top block has none (a UNION); a plan with none anywhere is not
	// MySQL's.
	p.CostUnknown = !p.costInfo
	p.ScanFilter = p.scans && p.conditions
	if p.CostUnknown && p.Message == "" {
		estimateRowsRead(qb, &p)
	}
	if p.Message == "" {
		readIsResult(qb, &p)
	}
	return p, nil
}

// walk visits every node of the plan. inSubquery is true below a
// "subqueries" key (MariaDB's list of the block's subqueries).
func walk(v any, p *Plan, inSubquery bool) {
	switch x := v.(type) {
	case map[string]any:
		if _, ok := x["cost_info"]; ok {
			p.costInfo = true
		}
		if fs, ok := x["using_filesort"].(bool); ok && fs {
			p.Filesort = true
		}
		if _, ok := x["filesort"].(map[string]any); ok {
			// MariaDB's spelling: a "filesort" node wraps what it sorts.
			p.Filesort = true
		}
		if t, ok := x["table"].(map[string]any); ok {
			if at, ok := t["access_type"].(string); ok {
				p.Tables++
				if at == "ALL" {
					p.FullScans++
				}
				if at == "ALL" || at == "index" || at == "range" {
					p.scans = true
				}
				if cond, _ := t["attached_condition"].(string); cond != "" {
					p.conditions = true
				}
				rows, ok := t["rows_examined_per_scan"]
				if !ok {
					rows = t["rows"] // MariaDB
				}
				n := int64(number(rows))
				if n > p.MaxScanRows {
					p.MaxScanRows = n
				}
				if at == "index" && !inSubquery && n > p.MaxIndexScanRows {
					p.MaxIndexScanRows = n
				}
			}
		}
		for key, child := range x {
			walk(child, p, inSubquery || key == "subqueries")
		}
	case []any:
		for _, child := range x {
			walk(child, p, inSubquery)
		}
	}
}

// number reads a JSON number whether MySQL wrote it as a number or, as it
// does for costs, as a string.
func number(v any) float64 {
	switch x := v.(type) {
	case float64:
		return x
	case string:
		f, _ := strconv.ParseFloat(strings.TrimSpace(x), 64)
		return f
	case json.Number:
		f, _ := x.Float64()
		return f
	}
	return 0
}

// Policy is the threshold: a statement goes to the copy when its plan costs
// at least CostThreshold, OR when a full scan examines at least ScanRows
// rows, OR, for a plan with no cost (MariaDB), when its joins read at least
// ScanRows rows in all. Both zero means "never to the copy".
type Policy struct {
	CostThreshold float64
	ScanRows      int64
}

// limitBoundRows is the largest top-level LIMIT (offset included) that
// counts as "a few rows", and the largest plan row estimate under which a
// LIMIT-bounded statement is MySQL's.
const limitBoundRows = 1000

var (
	// topLimit matches a LIMIT that ends the statement: "LIMIT n",
	// "LIMIT off, n", "LIMIT n OFFSET off", with an optional trailing ";".
	topLimit = regexp.MustCompile(`(?i)\blimit\s+(\d+)(?:\s*,\s*(\d+)|\s+offset\s+(\d+))?\s*;?\s*$`)
	// unboundedWork matches what makes MySQL read past the LIMIT before it
	// can emit the first row: aggregation, grouping, DISTINCT, window
	// functions and UNION all materialize or sort first.
	unboundedWork = regexp.MustCompile(`(?i)\b(group\s+by|having|distinct|distinctrow|union|intersect|except|rollup|over|sql_calc_found_rows)\b|\b(count|sum|avg|min|max|group_concat|any_value|grouping|std|stddev|stddev_pop|stddev_samp|var_pop|var_samp|variance|bit_and|bit_or|bit_xor|json_arrayagg|json_objectagg)\s*\(`)
	// anyLimit counts LIMITs: a second one belongs to a derived table or
	// CTE, which MySQL materializes whole before the outer LIMIT applies.
	anyLimit = regexp.MustCompile(`(?i)\blimit\b`)
	// bareLimit is the one shape decided without a plan: a select list with
	// no parentheses (so no subquery, no function call), FROM one table with
	// an optional alias, LIMIT, nothing else. No WHERE, JOIN, comma join,
	// ORDER BY or GROUP BY fits between the table and the LIMIT.
	bareLimit = regexp.MustCompile("(?is)^\\s*select\\s+[^()]*?\\s+from\\s+[\\w.`]+(?:\\s+(?:as\\s+)?[\\w`]+)?\\s+limit\\s+\\d+(?:\\s*,\\s*\\d+|\\s+offset\\s+\\d+)?\\s*;?\\s*$")
)

// limitBounded reports whether a top-level LIMIT caps the rows MySQL must
// PRODUCE at a few (limitBoundRows, offset included), with nothing in the
// statement that forces it to read everything first (no aggregate, GROUP
// BY, HAVING, DISTINCT, window function or UNION, and no second LIMIT in a
// derived table or CTE, which is materialized whole). It says nothing about
// the rows MySQL must READ to find them: a filter with no index behind it
// scans until it has n matches, which is why the plan's row estimate still
// decides (DecideStatement). Matched on the scrubbed statement, so a literal
// cannot fake a LIMIT; a MySQL executable comment (`/*!... */`), which
// scrub blanks, disqualifies, since it may hide a GROUP BY.
func limitBounded(stmt string) (rows int64, ok bool) {
	rows, _, ok = limitBoundedScrubbed(stmt)
	return rows, ok
}

// limitBoundedScrubbed is limitBounded returning the scrubbed text too, so
// a caller that reads it further (Prejudge) scrubs once.
func limitBoundedScrubbed(stmt string) (rows int64, blanked string, ok bool) {
	if hintComment.MatchString(stmt) {
		return 0, "", false
	}
	blanked, _, _, _ = scrub(stmt)
	m := topLimit.FindStringSubmatch(blanked)
	if m == nil || unboundedWork.MatchString(blanked) || len(anyLimit.FindAllStringIndex(blanked, 2)) > 1 {
		return 0, "", false
	}
	var total int64
	for _, part := range m[1:] {
		if part == "" {
			continue
		}
		n, err := strconv.ParseInt(part, 10, 64)
		if err != nil || n > limitBoundRows {
			return 0, "", false // an unparseable or huge number is never "a few"
		}
		total += n
	}
	if total > limitBoundRows {
		return 0, "", false
	}
	return total, blanked, true
}

// Rule names which rule of the policy decided, for callers that count
// decisions (the shim's routing tally) without parsing the reason text.
type Rule string

const (
	RuleTrivial      Rule = "trivial"       // the optimizer short-circuited the plan
	RuleCost         Rule = "cost"          // query_cost at or above CostThreshold
	RuleScan         Rule = "scan"          // a full scan over ScanRows rows or more
	RuleJoinRows     Rule = "join_rows"     // a plan with no cost whose joins read ScanRows rows or more in all
	RuleCheap        Rule = "cheap"         // below both thresholds
	RuleBoundedLimit Rule = "bounded_limit" // a small LIMIT MySQL answers without reading past it
)

// Decision is the policy's verdict on one statement.
type Decision struct {
	ToCopy bool
	// Reason is the verdict in words an operator can read in the audit
	// trail and the debug log.
	Reason string
	Rule   Rule
	// ResultRows, on a statement the plan sends to the copy, is the plan's
	// estimate of the rows the statement returns (Plan.ResultRows cut by the
	// statement's own LIMIT), or 0 when the plan has no estimate to trust: a
	// join, a filter checked row by row, an aggregate, GROUP BY or DISTINCT,
	// a LIMIT given as a placeholder. The copy returns no result over its
	// row cap, which the caller knows and this package does not: a statement
	// whose ResultRows is over the cap is not worth trying there (#2115).
	ResultRows int64
}

// Prejudge decides the one statement shape that needs no plan: a select
// list without parentheses FROM one table, LIMIT a few rows, nothing else
// (no WHERE, JOIN, ORDER BY, GROUP BY, subquery or function). On a base
// table MySQL stops after those rows whatever the table's size, while its
// plan cost, which ignores LIMIT, would send it to the copy. The text
// cannot tell a view from a table: a view that aggregates or joins is built
// whole first, and is still MySQL's here. ok is false for every other
// statement: those need EXPLAIN and DecideStatement.
func (pol Policy) Prejudge(stmt string) (d Decision, ok bool) {
	n, blanked, bounded := limitBoundedScrubbed(stmt)
	if !bounded || !bareLimit.MatchString(blanked) {
		return Decision{}, false
	}
	return Decision{Reason: fmt.Sprintf("LIMIT %d on one table with no filter, join or sort: a base table stops after %d rows", n, n), Rule: RuleBoundedLimit}, true
}

// DecideStatement is Decide with the statement text in hand. A
// LIMIT-bounded statement (limitBounded) whose plan sorts nothing
// (Filesort) is MySQL's, whatever the plan's cost says, in two cases.
//
// One: the plan filters nothing while scanning (ScanFilter) and examines at
// most limitBoundRows rows per table scan. The cost ignores LIMIT, the row
// estimate does not, so an ORDER BY served from an index reads n rows
// (rows_examined_per_scan: n) and a filter an index serves (ref access) is
// bounded by the index.
//
// Two (#2115): the plan reads only its result (Plan.ReadIsResult), however
// many rows its range holds, and the statement calls no function that could
// be an aggregate (callsOnlyRowFunctions). A range of 246,857 rows with LIMIT 500 is
// estimated whole (rows_examined_per_scan: 599,380, cost 544,453) and read
// for 500: every row the index hands over is returned, so the read stops
// when the LIMIT is met. Measured at 24 ms on MySQL 8.4 against 65 ms on
// the copy, and 1.6 ms when an ORDER BY follows the same index.
//
// A filter no index serves, on the scanned table or on one joined to it,
// falls through to Decide, as before: under a full scan its estimate is the
// table; under an index-served order its estimate is only a guess from the
// filter's assumed selectivity, and a rare value walks the whole index.
//
// The rows a plan with no cost reads across its joins (Plan.RowsRead) are
// left out when the statement's LIMIT can end the join early
// (limitEndsJoin); the scan rules then decide alone.
//
// A statement sent to the copy carries the plan's estimate of its result
// (Decision.ResultRows), for the caller to hold against the copy's row cap.
func (pol Policy) DecideStatement(stmt string, p Plan) Decision {
	n, blanked, bounded := limitBoundedScrubbed(stmt)
	if bounded && !p.Filesort && p.Message == "" {
		if !p.ScanFilter && p.MaxScanRows <= limitBoundRows {
			return Decision{Reason: fmt.Sprintf("LIMIT %d served without a sort or an unindexed filter: at most %d rows per table scan", n, p.MaxScanRows), Rule: RuleBoundedLimit}
		}
		if p.ReadIsResult && callsOnlyRowFunctions(blanked) {
			return Decision{Reason: fmt.Sprintf("LIMIT %d over one table with no sort and no filter left to check row by row: the read stops once it has %d rows, of about %s behind it", n, n, groupDigits(p.ResultRows)), Rule: RuleBoundedLimit}
		}
	}
	d := pol.decide(p, pol.limitEndsJoin(stmt, p))
	if d.ToCopy {
		d.ResultRows = resultRows(stmt, p)
	}
	return d
}

// joinLimit is topLimit accepting a placeholder for either number: the
// prepared path decides on the statement's text.
var joinLimit = regexp.MustCompile(`(?i)\blimit\s+(\d+|\?)(?:\s*,\s*(\d+|\?)|\s+offset\s+(\d+|\?))?\s*;?\s*$`)

// limitEndsJoin says why the rows read across a join are not to be counted
// for this statement, or "" when they are. They are what the join reads
// when run to its end, and MariaDB does not cut them for a LIMIT; a
// top-level LIMIT (offset included) under the scan threshold, with nothing
// in the statement that reads everything first (unboundedWork) and no sort
// or temporary table over the join in the plan, ends the nested loop after
// about that many rows. A table sorted alone before the join is read whole
// and counts with the LIMIT. A placeholder is a bound not known here: the
// statement is decided as it was before the estimate existed. This is for
// the join estimate only: the bounded-limit rule and the scan rules are not
// touched. A LIMIT inside a derived table is not seen (it does not end the
// statement), and that block is counted whole.
func (pol Policy) limitEndsJoin(stmt string, p Plan) string {
	if p.topSort == sortWholeJoin || hintComment.MatchString(stmt) {
		return ""
	}
	blanked, _, _, _ := scrub(stmt)
	m := joinLimit.FindStringSubmatch(blanked)
	if m == nil || unboundedWork.MatchString(blanked) || len(anyLimit.FindAllStringIndex(blanked, 2)) > 1 {
		return ""
	}
	var total int64
	for _, part := range m[1:] {
		switch part {
		case "":
		case "?":
			return "LIMIT ? with no sort over the join can stop it early, by a bound not known here: the rows read across it are not counted"
		default:
			n, err := strconv.ParseInt(part, 10, 64)
			if err != nil || n >= pol.ScanRows || total+n >= pol.ScanRows {
				return ""
			}
			total += n
		}
	}
	if p.topSort == sortFirstTable {
		if p.sortedFirstRows+total >= pol.ScanRows {
			return ""
		}
		return fmt.Sprintf("%s rows sorted, then LIMIT %d stops the join early: the rows read across it are not counted", groupDigits(p.sortedFirstRows), total)
	}
	return fmt.Sprintf("LIMIT %d with no sort over the join can stop it early: the rows read across it are not counted", total)
}

// DefaultPolicy: 10,000 cost units (a point lookup is about 1; a full scan
// over 200k rows about 20,000, measured on MySQL 8.4) or a full scan over
// 100,000 rows.
func DefaultPolicy() Policy { return Policy{CostThreshold: 10000, ScanRows: 100000} }

// Decide reports whether the plan is expensive enough for the copy, and why
// either way, in words an operator can read in the audit trail.
func (pol Policy) Decide(p Plan) Decision { return pol.decide(p, "") }

// decide is Decide; a non-empty rowsLeftOut is why the rows read across
// joins are not to be counted for this statement (DecideStatement).
func (pol Policy) decide(p Plan, rowsLeftOut string) Decision {
	if p.Message != "" {
		return Decision{Reason: "trivial plan: " + p.Message, Rule: RuleTrivial}
	}
	if pol.CostThreshold > 0 && !p.CostUnknown && p.Cost >= pol.CostThreshold {
		return Decision{ToCopy: true, Reason: fmt.Sprintf("plan cost %.0f >= %.0f", p.Cost, pol.CostThreshold), Rule: RuleCost}
	}
	if pol.ScanRows > 0 && p.FullScans > 0 && p.MaxScanRows >= pol.ScanRows {
		return Decision{ToCopy: true, Reason: fmt.Sprintf("full scan over %d rows >= %d", p.MaxScanRows, pol.ScanRows), Rule: RuleScan}
	}
	if pol.ScanRows > 0 && p.CostUnknown && p.MaxIndexScanRows >= pol.ScanRows {
		// Without a cost to go by, an index walked end to end over that
		// many rows is the same work as a full scan.
		return Decision{ToCopy: true, Reason: fmt.Sprintf("full index scan over %d rows >= %d", p.MaxIndexScanRows, pol.ScanRows), Rule: RuleScan}
	}
	if p.CostUnknown {
		// With no cost, a join that walks a small table and probes a big
		// one by key shows no scan at all: the rows it reads in all are
		// compared with the same threshold.
		cheap := fmt.Sprintf("no full scan over %d rows (the plan carries no cost: the cost rule does not apply)", pol.ScanRows)
		switch {
		case pol.ScanRows <= 0:
		case p.RowsReadUnknown != "":
			cheap += "; rows read across joins not estimated: " + p.RowsReadUnknown
		case !p.Joined:
		case rowsLeftOut != "":
			cheap += "; " + rowsLeftOut
		case p.RowsRead >= pol.ScanRows:
			return Decision{ToCopy: true, Reason: fmt.Sprintf("plan reads about %s rows across a join (threshold %s)", groupDigits(roundRows(p.RowsRead)), groupDigits(pol.ScanRows)), Rule: RuleJoinRows}
		default:
			shown := roundRows(p.RowsRead)
			if shown >= pol.ScanRows {
				shown = p.RowsRead // rounding must not print the threshold for a plan under it
			}
			cheap += fmt.Sprintf("; about %s rows read across a join", groupDigits(shown))
		}
		return Decision{Reason: cheap, Rule: RuleCheap}
	}
	return Decision{Reason: fmt.Sprintf("plan cost %.0f below %.0f, no full scan over %d rows", p.Cost, pol.CostThreshold, pol.ScanRows), Rule: RuleCheap}
}
