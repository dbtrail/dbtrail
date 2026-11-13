package readrouter

import (
	"regexp"
	"strings"
)

// UnroutedRefusal is a construct refused on a connection the copy answers
// alone (#2532): What names it, Instead says how to write it for the copy.
// Why, when set, says what happens to it in place of "answered differently
// there than on MySQL, without an error". MySQLRefuses: MySQL refuses it too,
// so the message does not offer Engine Routing as a way to have it answered.
type UnroutedRefusal struct {
	What, Instead, Why string
	MySQLRefuses       bool
}

// Message is the refusal as the client is shown it.
func (r UnroutedRefusal) Message() string {
	why := "is answered differently there than on MySQL, without an error"
	if r.Why != "" {
		why = r.Why
	}
	msg := "this connection is answered by the copy, which runs DuckDB SQL: " + r.What + " " + why +
		". Write it for the copy instead (" + r.Instead + ")"
	if r.MySQLRefuses {
		return msg
	}
	return msg + ", or turn on Engine Routing so that MySQL answers it"
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
	// DATEDIFF is written for the copy before it runs, or refused then
	// (ForCopyOpts, UnroutedFunctionVeto, #2628).
	{regexp.MustCompile(`(?i)\b(timestampdiff|timediff)\s*\(`), "TIMESTAMPDIFF and TIMEDIFF (units counted another way)", "date_diff('day', start, end) for a count, end - start for a time"},
	{regexp.MustCompile(`(?i)\bas\s+(un)?signed\b`), "CAST AS SIGNED or UNSIGNED (overflow and truncation differ)", "CAST(x AS BIGINT) or CAST(x AS UBIGINT)"},
	// The copy's // matches DIV between two whole numbers (toward zero, NULL
	// for 0), and divides without truncating a DECIMAL or a DOUBLE: 7.5 // 2
	// is 3.75 there and 7.5 DIV 2 is 3 on MySQL (MySQL 8.0.46, DuckDB 1.5.6,
	// #2628). The types are not known here, so DIV stays refused, with both.
	{regexp.MustCompile(`(?i)\bdiv\b`), "DIV (integer division; the copy's // truncates only whole numbers)", "a // b for two whole numbers, CAST(trunc(a / nullif(b, 0)) AS BIGINT) when either is a DECIMAL or a DOUBLE"},
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
		return UnroutedRefusal{What: "a MySQL executable comment (/*! ... */, which MySQL runs and the copy skips)", Instead: "write what is inside it without the comment"}, true
	}
	// The copy reads a backslash in a string as a plain character, where
	// scan reads MySQL's escape: 'C:\' ends there on the copy. Outside a
	// string a backslash is no SQL on either side.
	sc := scan(strings.ReplaceAll(stmt, `\`, " "))
	text := lineCommentTail.ReplaceAllString(sc.blankedCopy, "")
	for _, v := range unroutedVetoes {
		if v.re.MatchString(text) {
			return UnroutedRefusal{What: v.what, Instead: v.instead}, true
		}
	}
	// Names emptied first: a -- inside one (`x-- y`) must not cut the rest
	// of the line, a LIMIT after it with it.
	if limitInSetSubquery(lineCommentTail.ReplaceAllString(quotedName.ReplaceAllString(sc.blankedCopy, `""`), "")) {
		return limitInSetRefusal, true
	}
	if hexNumber.MatchString(quotedName.ReplaceAllString(text, `""`)) {
		return UnroutedRefusal{What: "0x.. and 0b.. (a hexadecimal or bit number on MySQL; a number and an alias on the copy)", Instead: "the number in decimal, or a hexadecimal string the copy's way: '\\x41'::BLOB"}, true
	}
	return UnroutedRefusal{}, false
}

// limitInSetRefusal is MySQL's own refusal of a LIMIT in an IN, ALL, ANY or
// SOME subquery, which the copy answers.
var limitInSetRefusal = UnroutedRefusal{
	What:         "a LIMIT inside an IN, ALL, ANY or SOME subquery",
	Why:          "is refused by MySQL with error 1235 (\"This version of MySQL doesn't yet support 'LIMIT & IN/ALL/ANY/SOME subquery'\"), and answered by the copy",
	Instead:      "the LIMIT in a derived table, which MySQL takes too: IN (SELECT a FROM (SELECT a FROM t LIMIT 2) AS d)",
	MySQLRefuses: true,
}

// limitInSetSubquery reports whether text, a statement blanked as UnroutedVeto
// reads it (names in double quotes, strings ”), holds a LIMIT of the query
// of an IN, ALL, ANY or SOME subquery: MySQL refuses it with error 1235, the
// copy answers it (#2628). Measured on MySQL 8.0.46: refused is the LIMIT of
// the subquery's own query, after a UNION in it, after a WITH, and inside a
// second pair of parentheses; answered is a LIMIT in a derived table or a CTE
// inside the subquery, in a scalar subquery and in EXISTS. Read with the depth
// of parentheses, each one marked by whether it opens such a subquery.
func limitInSetSubquery(text string) bool {
	set := []bool{false}
	for i := 0; i < len(text); {
		c := text[i]
		switch {
		case c == '"':
			// A quoted name: skipped whole, whatever it holds.
			end := strings.IndexByte(text[i+1:], '"')
			if end < 0 {
				return false
			}
			i += end + 2
		case c == '(':
			set = append(set, opensSetSubquery(text[:i], set[len(set)-1]))
			i++
		case c == ')':
			if len(set) > 1 {
				set = set[:len(set)-1]
			}
			i++
		case wordByte(c):
			j := i
			for j < len(text) && wordByte(text[j]) {
				j++
			}
			if set[len(set)-1] && strings.EqualFold(text[i:j], "limit") && !namedByDot(text, i, j-i) {
				return true
			}
			i = j
		default:
			i++
		}
	}
	return false
}

// opensSetSubquery reports whether the parenthesis after before opens the
// query of an IN, ALL, ANY or SOME subquery: IN right before it, or ALL, ANY
// or SOME after a comparison (= ANY, > ALL; UNION ALL is not one). Right
// after another parenthesis, or after a UNION, INTERSECT or EXCEPT, it is
// still the query the enclosing one opened, so it takes enclosing.
func opensSetSubquery(before string, enclosing bool) bool {
	b := strings.TrimRight(before, " \t\n\r\f\v")
	if strings.HasSuffix(b, "(") {
		return enclosing
	}
	i := len(b)
	for i > 0 && wordByte(b[i-1]) {
		i--
	}
	w := strings.ToLower(b[i:])
	rest := strings.TrimRight(b[:i], " \t\n\r\f\v")
	if strings.HasSuffix(rest, ".") {
		return false // a name: t.in(...)
	}
	switch w {
	case "in":
		return true
	case "union", "intersect", "except":
		return enclosing
	case "any", "some", "all", "distinct":
		if w != "distinct" && rest != "" && strings.IndexByte("=<>", rest[len(rest)-1]) >= 0 {
			return true
		}
		k := len(rest)
		for k > 0 && wordByte(rest[k-1]) {
			k--
		}
		switch strings.ToLower(rest[k:]) {
		case "union", "intersect", "except":
			return enclosing
		}
	}
	return false
}

// limitCommaAny is MySQL's LIMIT offset, count with any one token first
// (limitComma, the routed veto's, takes a number or a placeholder only): -1,
// NULL, '1' and 1.5 are what a prepared statement's arguments can write.
var limitCommaAny = regexp.MustCompile(`(?i)\blimit\s+[-+]?[\w.?']+\s*,`)

// UnroutedLimitVeto says whether text, the statement about to be sent to the
// copy on a connection without routing, still holds MySQL's LIMIT offset,
// count: one ForCopyOpts did not rewrite (a number past 9223372036854775807,
// a comment between the two, an argument that is not a whole number) or a
// statement sent as the client wrote it. The copy only fails on it, with its
// own parser error; the refusal says how to write it. Read as UnroutedVeto
// reads a statement, so a LIMIT inside a string or a comment refuses nothing.
func UnroutedLimitVeto(text string) (UnroutedRefusal, bool) {
	sc := scan(strings.ReplaceAll(text, `\`, " "))
	if limitCommaAny.MatchString(lineCommentTail.ReplaceAllString(sc.blankedCopy, "")) {
		return UnroutedRefusal{
			What:    "LIMIT offset, count",
			Why:     "is not read there. This port writes it for the copy only with two whole numbers of at most 9223372036854775807, nothing but white space around the comma, and in a statement whose names and strings it rewrites with certainty (no # comment, no $, no backslash or double-quoted string)",
			Instead: "LIMIT count OFFSET offset",
		}, true
	}
	return UnroutedRefusal{}, false
}
