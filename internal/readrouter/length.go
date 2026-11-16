package readrouter

import (
	"regexp"
	"strings"
)

// VetoLength is why LengthVeto keeps a statement on MySQL; the reason it
// returns adds what kept it ("kept by: LENGTH(l) ...").
const VetoLength = "LENGTH, OCTET_LENGTH, BIT_LENGTH, CHAR_LENGTH or CHARACTER_LENGTH of something other than a text column in utf8mb4, utf8mb3 or ascii " +
	"(MySQL counts the bytes of the column's own character set, or turns a number, a date or JSON into text its own way)"

// lengthCall opens a call of one of the length functions, its name bare or
// quoted (the shape writes a backtick as a double quote).
var lengthCall = regexp.MustCompile(`(?i)"?(?:length|octet_length|bit_length|char_length|character_length)"?` + callOpen)

// LengthVeto says why the copy must not answer a statement because of a
// LENGTH, OCTET_LENGTH, BIT_LENGTH, CHAR_LENGTH or CHARACTER_LENGTH call, or
// "" when it can (#2918). MySQL's LENGTH and OCTET_LENGTH count the bytes of
// the text in the column's character set, BIT_LENGTH eight times as many,
// CHAR_LENGTH and CHARACTER_LENGTH its characters. The copy holds every text
// as UTF-8 and is sent the first two as strlen (CopyOptions.MySQLLength), so
// over a column whose bytes are that UTF-8 every one of them gives MySQL's
// number: 'é' is 2, 16 and 1, an emoji 4, 32 and 1 (MySQL 8.0.46, MariaDB
// 10.11.19, DuckDB 1.5.6). shape is Shape of the statement; safe are the
// columns of the tables it reads that are such text (baseline.LengthColumns),
// other all their other columns, the ones whose type is not known among them.
//
// The copy may answer when every call's one argument is a bare column whose
// name is in safe and in no table's other. Everything else stays on MySQL: a
// latin1 column (one byte a letter on MySQL, two on the copy), a binary one,
// JSON, a number or a date (made text by MySQL's rules), an expression, a
// literal, a placeholder; a quoted name before the parenthesis, or one after
// a dot (a stored function on MySQL); and what DistinctVeto keeps for the
// same reasons, a name the statement may define itself (any subquery,
// derived table or WITH) and a call after GROUP BY, HAVING or ORDER BY,
// where MySQL reads a select-list alias before the column.
func LengthVeto(shape string, safe, other []string) string {
	locs := callsOf(lengthCall, shape)
	if len(locs) == 0 {
		return ""
	}
	if subquery.MatchString(shape) {
		return lengthReason("a subquery, derived table or WITH names columns the statement may define itself")
	}
	if c := aliasClause.FindStringIndex(shape); c != nil && c[0] < locs[len(locs)-1][0] {
		return lengthReason("a call after GROUP BY, HAVING or ORDER BY, where MySQL reads a select-list alias before a column")
	}
	counted := knownNames(safe, other)
	for _, l := range locs {
		if shape[l[0]] == '"' || strings.HasSuffix(strings.TrimRight(shape[:l[0]], " \t\n\r\f\v"), ".") {
			return lengthReason(strings.TrimRight(shape[l[0]:l[1]], " \t\n\r\f\v(") + " is a quoted or dotted name, which on MySQL may be a stored function")
		}
		body, ok := closingBody(shape, l[1])
		if !ok {
			return lengthReason(strings.TrimRight(shape[l[0]:l[1]], " \t\n\r\f\v(") + " has no closing parenthesis")
		}
		call := strings.TrimRight(shape[l[0]:l[1]], " \t\n\r\f\v(") + "(" + strings.TrimSpace(body) + ")"
		m := columnRef.FindStringSubmatch(unwrap(strings.TrimSpace(body)))
		if m == nil {
			return lengthReason(call + " takes something other than a column")
		}
		name := m[1]
		if strings.HasPrefix(name, `"`) {
			name = name[1 : len(name)-1]
		}
		if !counted[sameName(name)] {
			return lengthReason(call + ": column " + name + " is not known as text in utf8mb4, utf8mb3 or ascii in every table the statement reads")
		}
	}
	return ""
}

// lengthReason is VetoLength with what kept the statement on MySQL.
func lengthReason(detail string) string { return VetoLength + "; kept by: " + detail }

// LengthVeto is LengthVeto over this shape.
func (s StatementShape) LengthVeto(safe, other []string) string {
	return LengthVeto(string(s), safe, other)
}
