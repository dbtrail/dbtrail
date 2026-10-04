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
	{"|| (string concatenation on the copy, logical OR on MySQL)", regexp.MustCompile(`\|\|`)},
	{"^ (power on the copy, bitwise XOR on MySQL)", regexp.MustCompile(`\^`)},
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
	blanked, doubleQuoted := scrub(stmt)
	if doubleQuoted {
		// MySQL reads "x" as a string; DuckDB as an identifier, which
		// resolves without an error whenever a column has that name.
		return "double-quoted string literal"
	}
	for _, v := range vetoes {
		if v.re.MatchString(blanked) {
			return v.name
		}
	}
	return ""
}

// Scrub is scrub for other packages: the statement with comments removed
// and string literals blanked, for structural checks such as "is there an
// ORDER BY at the top level".
func Scrub(stmt string) string {
	s, _ := scrub(stmt)
	return s
}

// scrub returns the statement with its comments removed (`/* */`, `-- `,
// `#`), its string literals replaced by ” and its backtick identifiers
// kept, plus whether a double-quoted string literal occurred. One pass over
// the bytes, tracking what is open, so a quote inside a comment (`-- don't`)
// does not blank the statement after it and a `#` inside a string does not
// start a comment.
func scrub(stmt string) (blanked string, doubleQuoted bool) {
	var b strings.Builder
	n := len(stmt)
	for i := 0; i < n; {
		c := stmt[i]
		switch {
		case c == '/' && i+1 < n && stmt[i+1] == '*':
			end := strings.Index(stmt[i+2:], "*/")
			if end < 0 {
				return b.String(), doubleQuoted
			}
			b.WriteByte(' ')
			i += end + 4
		case c == '#', c == '-' && i+1 < n && stmt[i+1] == '-' && (i+2 >= n || stmt[i+2] == ' ' || stmt[i+2] == '\t' || stmt[i+2] == '\n'):
			nl := strings.IndexByte(stmt[i:], '\n')
			if nl < 0 {
				return b.String(), doubleQuoted
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
				return b.String(), doubleQuoted
			}
			b.WriteString(stmt[i : i+j+2])
			i += j + 2
		default:
			b.WriteByte(c)
			i++
		}
	}
	return b.String(), doubleQuoted
}

// Plan is what the router reads out of EXPLAIN FORMAT=JSON.
type Plan struct {
	// Cost is the top-level query_block's query_cost (optimizer units).
	Cost float64
	// Tables is how many table accesses the plan has, derived tables included.
	Tables int
	// FullScans is how many of them are access_type ALL.
	FullScans int
	// MaxScanRows is the largest rows_examined_per_scan over every table.
	MaxScanRows int64
	// Message is the optimizer's shortcut when there is no plan ("no matching
	// row in const table", "Impossible WHERE"): the query is trivial.
	Message string
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
	}
	walk(qb, &p)
	return p, nil
}

func walk(v any, p *Plan) {
	switch x := v.(type) {
	case map[string]any:
		if t, ok := x["table"].(map[string]any); ok {
			if at, ok := t["access_type"].(string); ok {
				p.Tables++
				if at == "ALL" {
					p.FullScans++
				}
				if n := int64(number(t["rows_examined_per_scan"])); n > p.MaxScanRows {
					p.MaxScanRows = n
				}
			}
		}
		for _, child := range x {
			walk(child, p)
		}
	case []any:
		for _, child := range x {
			walk(child, p)
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
// rows. Both zero means "never to the copy".
type Policy struct {
	CostThreshold float64
	ScanRows      int64
}

// DefaultPolicy: 10,000 cost units (a point lookup is about 1; a full scan
// over 200k rows about 20,000, measured on MySQL 8.4) or a full scan over
// 100,000 rows.
func DefaultPolicy() Policy { return Policy{CostThreshold: 10000, ScanRows: 100000} }

// Decide reports whether the plan is expensive enough for the copy, and why
// either way, in words an operator can read in the audit trail.
func (pol Policy) Decide(p Plan) (toCopy bool, reason string) {
	if p.Message != "" {
		return false, "trivial plan: " + p.Message
	}
	if pol.CostThreshold > 0 && p.Cost >= pol.CostThreshold {
		return true, fmt.Sprintf("plan cost %.0f >= %.0f", p.Cost, pol.CostThreshold)
	}
	if pol.ScanRows > 0 && p.FullScans > 0 && p.MaxScanRows >= pol.ScanRows {
		return true, fmt.Sprintf("full scan over %d rows >= %d", p.MaxScanRows, pol.ScanRows)
	}
	return false, fmt.Sprintf("plan cost %.0f below %.0f, no full scan over %d rows", p.Cost, pol.CostThreshold, pol.ScanRows)
}
