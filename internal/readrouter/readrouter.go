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
	// (`/*! ... */`) is MySQL code, so Classify looks inside it instead.
	leadingComment = regexp.MustCompile(`^(?s)(\s*(/\*[^!].*?\*/|--[^\n]*\n|#[^\n]*\n))*`)
	execComment    = regexp.MustCompile(`(?s)^\s*/\*!\d*\s*(.*?)\*/\s*$`)
	selectRE       = regexp.MustCompile(`(?is)^\s*\(*\s*(select|with)\b`)
	pinRE          = regexp.MustCompile(`(?is)^\s*(set|create\s+temporary|lock\s+tables|unlock\s+tables|prepare)\b`)
	beginRE        = regexp.MustCompile(`(?is)^\s*(begin|start\s+transaction)\b`)
	endRE          = regexp.MustCompile(`(?is)^\s*(commit|rollback)\b`)
	writeRE        = regexp.MustCompile(`(?is)^\s*(insert|update|delete|replace|create|alter|drop|truncate|rename|grant|revoke|load|call|flush|kill|optimize|analyze|repair|install|uninstall|reset|purge)\b`)
	// harmlessSet: session settings a driver sends when it connects, which
	// change nothing the copy answers differently (the connection's
	// character set, autocommit left on). Forwarded, but they do not pin
	// the connection to MySQL.
	harmlessSet = regexp.MustCompile(`(?is)^\s*set\s+(names\b|character_set_\w+\s*=|autocommit\s*=\s*(1|on|true)\s*$)`)
)

// Classify names the statement's class from its leading keyword, comments
// stripped. It never looks past the first keyword: a SELECT that writes
// (SELECT ... INTO OUTFILE) is caught by Veto, not here.
func Classify(stmt string) Kind {
	s := leadingComment.ReplaceAllString(stmt, "")
	if m := execComment.FindStringSubmatch(s); m != nil {
		return Classify(m[1])
	}
	switch {
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
	s := leadingComment.ReplaceAllString(stmt, "")
	if m := execComment.FindStringSubmatch(s); m != nil {
		s = m[1]
	}
	if m := keywordRE.FindStringSubmatch(s); m != nil {
		return strings.ToUpper(m[1])
	}
	return "?"
}

// HarmlessSet reports whether a KindSet statement is one of the connect-time
// settings that need not pin the connection to MySQL (see harmlessSet).
func HarmlessSet(stmt string) bool {
	return harmlessSet.MatchString(leadingComment.ReplaceAllString(stmt, ""))
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
	{"STR_TO_DATE", regexp.MustCompile(`(?i)\bstr_to_date\s*\(`)},                                  // NULL in MySQL, error in DuckDB on a bad date
	{"TIMESTAMPDIFF/DATEDIFF", regexp.MustCompile(`(?i)\b(timestampdiff|datediff|timediff)\s*\(`)}, // day counting differs
	{"COLLATE", regexp.MustCompile(`(?i)\bcollate\b`)},                                             // collations do not exist on the copy
	{"CAST AS UNSIGNED/SIGNED", regexp.MustCompile(`(?i)\bas\s+(un)?signed\b`)},                    // overflow and truncation differ
	{"DIV", regexp.MustCompile(`(?i)\bdiv\b`)},                                                     // integer division differs
	{"RAND/UUID", regexp.MustCompile(`(?i)\b(rand|uuid|uuid_short)\s*\(`)},                         // nondeterministic
	{"FOUND_ROWS/LAST_INSERT_ID/ROW_COUNT", regexp.MustCompile(`(?i)\b(found_rows|last_insert_id|row_count|sql_calc_found_rows)\b`)},
	{"CONNECTION_ID/USER/DATABASE/VERSION", regexp.MustCompile(`(?i)\b(connection_id|current_user|session_user|system_user|user|database|schema|version)\s*\(`)},
	{"user or system variable", regexp.MustCompile(`@`)},
	{"locking read", regexp.MustCompile(`(?i)\b(for\s+update|lock\s+in\s+share\s+mode|for\s+share)\b`)},
	{"INTO (OUTFILE/DUMPFILE/variables)", regexp.MustCompile(`(?i)\binto\s+(outfile|dumpfile|@)`)},
	{"system schema", regexp.MustCompile(`(?i)\b(information_schema|performance_schema|mysql|sys)\s*\.`)},
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
	// them. A leading backtick, word character or dot means a quoted name, a
	// longer word or a column of that name, not the operator.
	{"UNION/INTERSECT/EXCEPT without ALL (duplicates removed by bytes on the copy, by collation on MySQL)", regexp.MustCompile(`(?i)(^|[^\x60\w.])(intersect|except)([^\x60\w]|$)|(^|[^\x60\w.])union\s*(distinct\b|select\b|values\b|table\b|\()`)},
}

var hintComment = regexp.MustCompile(`/\*[!+]`)

// Veto returns the name of the first construct that keeps the statement on
// MySQL, or "" when none applies. Comments are removed and string literals
// blanked first (scrub), so a keyword inside a string or a quote inside a
// comment cannot hide or fake a match.
func Veto(stmt string) string {
	if hintComment.MatchString(stmt) {
		return "optimizer hint or MySQL comment"
	}
	blanked, doubleQuoted, backslash, hash := scrub(stmt)
	if doubleQuoted {
		// MySQL reads "x" as a string; DuckDB as an identifier, which
		// resolves without an error whenever a column has that name.
		return "double-quoted string literal"
	}
	if backslash {
		// MySQL reads a backslash inside a string as an escape ('a\\b' is
		// a\b, '\_' in LIKE is a literal underscore); DuckDB reads it as a
		// plain character, so the same text names a different value and
		// the copy answers, silently, about another string.
		return "backslash in a string literal (an escape on MySQL, a plain character on the copy)"
	}
	for _, v := range vetoes {
		if v.re.MatchString(blanked) {
			return v.name
		}
	}
	if hash {
		// MySQL reads `#` to the end of the line as a comment, which scrub
		// has removed as MySQL does; DuckDB reads `#2` as the second column
		// of the select. `SELECT #2<newline> alpha FROM t` is column alpha on
		// MySQL and the table's second column, named alpha, on the copy.
		return "# starts a comment on MySQL and a column-position reference on the copy"
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
// any string literal held a backslash. One pass over the bytes, tracking
// what is open, so a quote inside a comment (`-- don't`) does not blank the
// statement after it and a `#` inside a string does not start a comment.
func scrub(stmt string) (blanked string, doubleQuoted, backslash, hash bool) {
	var b strings.Builder
	n := len(stmt)
	for i := 0; i < n; {
		c := stmt[i]
		switch {
		case c == '/' && i+1 < n && stmt[i+1] == '*':
			end := strings.Index(stmt[i+2:], "*/")
			if end < 0 {
				return b.String(), doubleQuoted, backslash, hash
			}
			b.WriteByte(' ')
			i += end + 4
		case c == '#', c == '-' && i+1 < n && stmt[i+1] == '-' && (i+2 >= n || stmt[i+2] == ' ' || stmt[i+2] == '\t' || stmt[i+2] == '\n'):
			if c == '#' {
				hash = true
			}
			nl := strings.IndexByte(stmt[i:], '\n')
			if nl < 0 {
				return b.String(), doubleQuoted, backslash, hash
			}
			b.WriteByte('\n')
			i += nl + 1
		case c == '\'' || c == '"':
			if c == '"' {
				doubleQuoted = true
			}
			j := i + 1
			for j < n {
				if stmt[j] == '\\' {
					backslash = true
					j += 2
					continue
				}
				if stmt[j] == c {
					if j+1 < n && stmt[j+1] == c {
						j += 2
						continue
					}
					break
				}
				j++
			}
			b.WriteString("''")
			i = j + 1
		case c == '`':
			j := strings.IndexByte(stmt[i+1:], '`')
			if j < 0 {
				b.WriteString(stmt[i:])
				return b.String(), doubleQuoted, backslash, hash
			}
			b.WriteString(stmt[i : i+j+2])
			i += j + 2
		default:
			b.WriteByte(c)
			i++
		}
	}
	return b.String(), doubleQuoted, backslash, hash
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
	// RowsRead estimates the rows the plan reads in all, across its joins
	// and subqueries (joinrows.go). Only for a plan with no cost
	// (CostUnknown), which has nothing else to tell a heavy join by; 0 on
	// a MySQL plan and when RowsReadUnknown says why there is no estimate.
	RowsRead int64
	// Joined is true when the estimate multiplies anywhere: a table read
	// once per row of the tables before it, or a subquery or derived table
	// run once per outer row. False for one table, and for a UNION of
	// single tables.
	Joined bool
	// RowsReadUnknown is why a plan with no cost has no RowsRead: a shape
	// the estimate does not know. The scan rules alone decide that plan.
	RowsReadUnknown string
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
// (Filesort), filters nothing while scanning (ScanFilter) and examines at
// most limitBoundRows rows per table scan is MySQL's, whatever the plan's
// cost says: the cost ignores LIMIT, the row estimate does not, so an ORDER
// BY served from an index reads n rows (rows_examined_per_scan: n) and a
// filter an index serves (ref access) is bounded by the index. A filter no
// index serves, on the scanned table or on one joined to it, falls through
// to Decide, as before: under a full scan its estimate is the table; under
// an index-served order its estimate is only a guess from the filter's
// assumed selectivity, and a rare value walks the whole index.
//
// The rows a plan with no cost reads across its joins (Plan.RowsRead) are
// what the join reads when run to its end. A LIMIT-bounded statement with
// no sort ends it early, so that estimate is left out for it and the scan
// rules alone decide, as they did before the estimate existed.
func (pol Policy) DecideStatement(stmt string, p Plan) Decision {
	n, bounded := limitBounded(stmt)
	if bounded && !p.Filesort && !p.ScanFilter && p.Message == "" && p.MaxScanRows <= limitBoundRows {
		return Decision{Reason: fmt.Sprintf("LIMIT %d served without a sort or an unindexed filter: at most %d rows per table scan", n, p.MaxScanRows), Rule: RuleBoundedLimit}
	}
	if bounded && !p.Filesort {
		return pol.decide(p, fmt.Sprintf("LIMIT %d with no sort can stop the join early: the rows read across it are not counted", n))
	}
	return pol.decide(p, "")
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
			cheap += fmt.Sprintf("; about %s rows read across a join", groupDigits(roundRows(p.RowsRead)))
		}
		return Decision{Reason: cheap, Rule: RuleCheap}
	}
	return Decision{Reason: fmt.Sprintf("plan cost %.0f below %.0f, no full scan over %d rows", p.Cost, pol.CostThreshold, pol.ScanRows), Rule: RuleCheap}
}
