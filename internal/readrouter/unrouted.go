package readrouter

import (
	"regexp"
	"strings"
)

// UnroutedRefusal is a construct refused on a connection the copy answers
// alone (#2532): What names it, Instead says how to write it for the copy.
type UnroutedRefusal struct {
	What, Instead string
}

// Message is the refusal as the client is shown it.
func (r UnroutedRefusal) Message() string {
	return "this connection is answered by the copy, which runs DuckDB SQL: " + r.What +
		" is answered differently there than on MySQL, without an error. Write it for the copy instead (" + r.Instead +
		"), or turn on Engine Routing so that MySQL answers it"
}

// unroutedVetoes are the constructs of the veto list that only a statement
// written for MySQL holds, each with the copy's way of writing it. A
// connection without routing has no MySQL to forward to, so where the
// routed port sends such a statement to MySQL this one refuses it: a
// refusal that says why, instead of the copy's different answer.
//
// Deliberately absent, and answered with the copy's meaning (listed in
// docs/time-travel-sql.md): what is also how the copy is written (||, ^,
// UNION, INTERSECT, EXCEPT, WITH RECURSIVE, JSON functions and ->>,
// COLLATE, INTERVAL, date arithmetic, bit operators, ~, $$ strings,
// double-quoted names, a backslash in a string, -- and # comments, CAST to
// a time type, the copy's reserved words, a number that starts a name,
// 1_000); a string that reads as a date with a two-digit year, since any
// text of that shape ('10-20-30', '21-04-2026') would be refused with it;
// the case rules of the copy's
// collation (LIKE, DISTINCT inside an aggregate, INSTR and POSITION); the
// system schemas, which clients read as they connect; NOW() and the
// copy's other clock functions, which answer in the zone the connection
// set (#2035), MySQL's own ones (CURDATE, UNIX_TIMESTAMP) failing there by
// themselves; RAND and UUID,
// different on every run anyway; and what the copy refuses by itself with
// an error (FIELD, FIND_IN_SET, STRCMP, ORDER BY NULL).
var unroutedVetoes = []struct {
	re            *regexp.Regexp
	what, instead string
}{
	{regexp.MustCompile(`(?i)\bgroup_concat\s*\(`), "GROUP_CONCAT (cut at group_concat_max_len on MySQL)", "string_agg(x, ',')"},
	{regexp.MustCompile(`(?i)\b(week|yearweek)\s*\(|\bextract\s*\(\s*week\b`), "WEEK, YEARWEEK and EXTRACT(WEEK) (weeks from Sunday on MySQL, ISO weeks on the copy)", "weekofyear(d) for ISO weeks, strftime(d, '%U') for weeks from Sunday"},
	{regexp.MustCompile(`(?i)\bstr_to_date\s*\(`), "STR_TO_DATE (NULL on MySQL for a bad date)", "strptime(text, format), or try_strptime for NULL on a bad date"},
	// The copy's own datediff takes a quoted unit first, which MySQL's
	// never does: datediff('day', a, b) is answered.
	{regexp.MustCompile(`(?i)\b(timestampdiff|timediff)\s*\(|\bdatediff\s*\(\s*[^'\s]`), "DATEDIFF, TIMESTAMPDIFF and TIMEDIFF (days and units counted another way)", "date_diff('day', start, end) for a count, end - start for a time"},
	{regexp.MustCompile(`(?i)\bas\s+(un)?signed\b`), "CAST AS SIGNED or UNSIGNED (overflow and truncation differ)", "CAST(x AS BIGINT) or CAST(x AS UBIGINT)"},
	{regexp.MustCompile(`(?i)\bdiv\b`), "DIV (integer division)", "a // b"},
	{regexp.MustCompile(`(?i)\b(found_rows|last_insert_id|row_count)\s*\(|\bsql_calc_found_rows\b`), "FOUND_ROWS, LAST_INSERT_ID and ROW_COUNT (MySQL's session counters)", "the copy has none: run it on MySQL"},
	{regexp.MustCompile(`(?i)\b(connection_id|current_user|session_user|system_user|user|database|schema|version)\s*\(|(^|[^"\w.])(current_user|current_role)\b`),
		"DATABASE(), USER(), VERSION() and the other session functions (the source's own on MySQL)", "current_database(), current_schema(), or library_version FROM pragma_version()"},
	// @ before a name, not the copy's operators @>, <@ and ^@ (@x, the
	// copy's absolute value of x, reads as MySQL's variable and is refused:
	// abs(x) says it either way).
	{regexp.MustCompile(`(^|[^<^@])@@?[\pL_$"\x60]`), "a user or system variable (@x, @@x)", "the copy has no variables: write the value into the statement (abs(x) for the copy's @x)"},
	{regexp.MustCompile(`(?i)\b(for\s+update|lock\s+in\s+share\s+mode|for\s+share)\b`), "a locking read (FOR UPDATE, FOR SHARE, LOCK IN SHARE MODE)", "the copy takes no locks: drop the clause"},
	{regexp.MustCompile(`(?i)\binto\s+(outfile|dumpfile)\b`), "INTO OUTFILE or DUMPFILE", "read the rows with the client and write them there"},
	{regexp.MustCompile(`(?i)\bmatch\s*\(.*\)\s*against\b`), "MATCH ... AGAINST (MySQL's full-text search)", "the copy has no full-text index: ILIKE or regexp_matches"},
	{regexp.MustCompile(`(?i)\bbinary\s+[\w"'(]`), "BINARY before a value (a byte comparison on MySQL)", "compare the values the copy's way, or run it on MySQL"},
	{regexp.MustCompile(`(?i)\b(regexp|rlike)\b`), "REGEXP and RLIKE (case-insensitive on MySQL)", "regexp_matches(x, 'pattern', 'i')"},
	{regexp.MustCompile(`(?i)\b[xb]''`), "x'..' and b'..' (a hexadecimal or bit string on MySQL)", "a decimal number, or a hexadecimal string the copy's way: '\\x41'::BLOB"},
	{regexp.MustCompile(`(?i)\blimit\s+[\d?]+\s*,`), "LIMIT offset, count", "LIMIT count OFFSET offset"},
	{regexp.MustCompile(`(?i)\b_binary\s*''`), "_binary before a string", "drop _binary"},
	{dayNumbering, "DAYOFWEEK, WEEKDAY and MICROSECOND (days numbered another way, microseconds that count the seconds too)", "isodow(d) % 7 + 1 for MySQL's DAYOFWEEK, isodow(d) - 1 for WEEKDAY, microsecond(ts) % 1000000 for MICROSECOND"},
}

// executableComment is MySQL's and MariaDB's /*! and MariaDB's /*M!: MySQL
// runs what is inside, the copy skips it as a comment. An optimizer hint
// (/*+) changes no answer and is answered.
var executableComment = regexp.MustCompile(`/\*M?!`)

// hexNumber is MySQL's hexadecimal or bit number, 0x10 or 0b101: the copy
// reads 0 under the alias x10. A number with underscores (1_000) is the
// copy's way of writing one and is answered.
var hexNumber = regexp.MustCompile(`(?i)\b0[xb][0-9a-f]+\b`)

// singleQuoted is a string literal the copy's way: no backslash escapes.
var singleQuoted = regexp.MustCompile(`'(?:[^']|'')*'`)

// lineCommentTail is a -- comment the copy's way, with or without a space
// after it: text the copy skips is not searched.
var lineCommentTail = regexp.MustCompile(`--[^\n]*`)

// UnroutedVeto says whether a statement sent to a connection the copy
// answers alone (no Engine Routing) must be refused, and why (#2532). The
// statement is read as the routed veto reads it, comments removed and string
// literals blanked, so a construct inside a string or a comment refuses
// nothing.
func UnroutedVeto(stmt string) (UnroutedRefusal, bool) {
	if executableComment.MatchString(lineCommentTail.ReplaceAllString(singleQuoted.ReplaceAllString(stmt, "''"), "")) {
		return UnroutedRefusal{"a MySQL executable comment (/*! ... */, which MySQL runs and the copy skips)", "write what is inside it without the comment"}, true
	}
	// The copy reads a backslash in a string as a plain character, where
	// scan reads MySQL's escape: 'C:\' ends there on the copy. Outside a
	// string a backslash is no SQL on either side.
	sc := scan(strings.ReplaceAll(stmt, `\`, " "))
	text := lineCommentTail.ReplaceAllString(sc.blankedCopy, "")
	for _, v := range unroutedVetoes {
		if v.re.MatchString(text) {
			return UnroutedRefusal{v.what, v.instead}, true
		}
	}
	if hexNumber.MatchString(quotedName.ReplaceAllString(text, `""`)) {
		return UnroutedRefusal{"0x.. and 0b.. (a hexadecimal or bit number on MySQL; a number and an alias on the copy)", "the number in decimal, or a hexadecimal string the copy's way: '\\x41'::BLOB"}, true
	}
	return UnroutedRefusal{}, false
}
