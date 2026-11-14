package readrouter

import (
	"regexp"
	"strings"
)

// collationFreeCalls are the names a statement may write before a
// parenthesis and still be one whose answer the connection's collation
// cannot change (CollationFree): keywords that open a list or a group, and
// functions that return a number or a date, or that return their column
// argument with its own collation (MIN and MAX). A function that returns
// text from something other than a column (CONCAT, DATE_FORMAT, CAST, LOWER,
// IFNULL and the rest) is left out: MySQL gives that text the connection's
// collation, and comparing, sorting or grouping it follows that collation.
var collationFreeCalls = map[string]bool{
	"in": true, "and": true, "or": true, "not": true, "exists": true, "from": true, "join": true, "on": true,
	"using": true, "where": true, "select": true, "over": true, "distinct": true, "by": true, "when": true,
	"then": true, "else": true, "union": true, "all": true, "having": true,
	"count": true, "sum": true, "avg": true, "min": true, "max": true,
	"abs": true, "round": true, "floor": true, "ceil": true, "ceiling": true, "mod": true,
	"year": true, "month": true, "day": true, "dayofmonth": true, "dayofyear": true, "quarter": true,
	"hour": true, "minute": true, "second": true,
	"now": true, "curdate": true, "utc_timestamp": true, "utc_date": true,
	"unix_timestamp": true, "datediff": true, "timestampdiff": true,
}

var (
	// collationFreeName is a word written before a parenthesis.
	collationFreeName = regexp.MustCompile(`(?i)([a-z_][a-z0-9_$]*)` + callOpen)
	// backtickName is a name in backticks, which may hold anything.
	backtickName = regexp.MustCompile("`[^`]*`")
	// hexOrBitNumber is 0x41 or 0b101: a binary string to MySQL.
	hexOrBitNumber = regexp.MustCompile(`(?i)(?:^|[^a-z0-9_$.])0[xb][0-9a-f]`)
)

// CollationFree reports whether nothing in stmt is text that takes the
// connection's collation, so that its answer is the same under every
// connection collation (#2836). MySQL gives the connection's collation to a
// string literal and to the text a function makes from something that is not
// a column; a column carries its own collation, which also wins when it is
// compared with a literal. So a statement with no string literal (nor a
// double-quoted string, a hex or bit number, or a placeholder, which may be
// bound to text), and whose only calls are of collationFreeCalls, compares,
// sorts and groups text by its columns' collations alone. Measured on MySQL
// 8.0.46 under utf8mb4_general_ci and utf8mb4_0900_ai_ci: ORDER BY, GROUP
// BY, DISTINCT, MIN and MAX over columns of three collations, a comparison
// and LIKE between two columns, IN and CASE over columns.
func CollationFree(stmt string) bool {
	sc := scan(stmt)
	if sc.doubleQuoted || strings.ContainsAny(sc.blanked, "'?") || hexOrBitNumber.MatchString(sc.blanked) {
		return false
	}
	text := backtickName.ReplaceAllString(sc.blanked, "``")
	for _, m := range collationFreeName.FindAllStringSubmatchIndex(text, -1) {
		// A longer word (xmin), or a name after a dot (db.min, a stored
		// function), is not the call.
		if m[2] > 0 && (wordByte(text[m[2]-1]) || text[m[2]-1] == '.') {
			return false
		}
		if !collationFreeCalls[strings.ToLower(text[m[2]:m[3]])] {
			return false
		}
	}
	return true
}
