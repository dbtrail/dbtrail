package readrouter

import (
	"regexp"
	"strings"
)

// offsetAfterTime is a string that writes a time with its seconds and,
// right after it, a UTC offset: '2026-01-15 02:00:00+05:00', '...00.5-05:00',
// and the offsets MySQL does not take (+0500, +05, -00:00, +14:01). Measured
// on MySQL 8.0.46 (#2790): MySQL 8.0.19+ moves such a time into the
// session's time zone before DATEDIFF takes its date, or reads NULL for an
// offset it does not take, where the copy takes the date as written. White
// space before the offset, an offset after a date with no time or a time
// with no seconds, and a Z or ' UTC' at the end are left out: MySQL reads
// none of them as an offset, and both answer alike.
var offsetAfterTime = regexp.MustCompile(`\d:\d{1,2}:\d{1,2}(\.\d*)?[+-]\d`)

// dateDiffOffsetRefusal is UnroutedDateDiffOffsetVeto's refusal.
var dateDiffOffsetRefusal = UnroutedRefusal{
	What: "DATEDIFF of a string with a UTC offset after its time ('2026-01-15 02:00:00+05:00')",
	Why: "is answered differently there than on MySQL, without an error: MySQL moves the time into the connection's time zone before it takes the date, " +
		"and reads NULL for an offset it does not take (+0500, +05, -00:00, past +14:00); the copy takes the date as written",
	Instead: "date_diff('day', CAST(b AS DATE), CAST(CAST(a AS TIMESTAMPTZ) AS DATE)) for a string a with an offset",
}

// UnroutedDateDiffOffsetVeto says whether stmt, a statement sent to a
// connection the copy answers alone, gives MySQL's two-argument DATEDIFF a
// string that writes a UTC offset after its time (offsetAfterTime), on its
// own or inside an argument (COALESCE('...+05:00', d), TIMESTAMP '...').
// The port writes that DATEDIFF for the copy (mysqlDateCall), whose answer
// can be another day than MySQL's (#2790); refused instead, with the copy's
// way of writing it.
//
// stmt is the client's text, a prepared statement's with its arguments
// written in. Strings, names in backticks or double quotes and comments are
// read as MySQL reads them, so a DATEDIFF inside a string or a comment
// refuses nothing, nor a name next to a dot (db.datediff, a stored function),
// nor the copy's own datediff, whose three arguments start with a unit. A
// column or an expression that only makes such a string while the statement
// runs (CONCAT(t, '+05:00')) is not seen here: docs/time-travel-sql.md lists
// it.
func UnroutedDateDiffOffsetVeto(stmt string) (UnroutedRefusal, bool) {
	// A backslash escapes the next character in MySQL's string, a quote
	// among them; it is no SQL outside one. As UnroutedVeto reads it.
	s := blankBacktickNames(strings.ReplaceAll(stmt, `\`, " "))
	for i := 0; i < len(s); {
		if end, _, skipped := skipQuoted(s, i); skipped {
			i = end
			continue
		}
		if !wordByte(s[i]) {
			i++
			continue
		}
		j := i
		for j < len(s) && wordByte(s[j]) {
			j++
		}
		if strings.EqualFold(s[i:j], "datediff") && !namedByDot(s, i, j-i) {
			if args, _, ok := callArgs(s[j:]); ok && len(args) == 2 {
				for _, a := range args {
					for _, lit := range literalsIn(a) {
						if offsetAfterTime.MatchString(lit) {
							return dateDiffOffsetRefusal, true
						}
					}
				}
			}
		}
		// On past the name only: a DATEDIFF inside this one's arguments is
		// read next.
		i = j
	}
	return UnroutedRefusal{}, false
}

// skipQuoted reads what starts at s[i] when it is a string in single
// quotes, a name or a string in double quotes, a name in backticks or a
// comment (/* */, # and -- with white space after it, as MySQL reads them).
// It returns where that ends and, for a string in single quotes, its text
// with each doubled quote read as one. skipped is false when s[i] starts
// none of them. One left open runs to the end of s.
func skipQuoted(s string, i int) (end int, lit string, skipped bool) {
	n := len(s)
	switch c := s[i]; {
	case c == '\'':
		var b strings.Builder
		for j := i + 1; j < n; j++ {
			if s[j] != '\'' {
				b.WriteByte(s[j])
				continue
			}
			if j+1 < n && s[j+1] == '\'' {
				b.WriteByte('\'')
				j++
				continue
			}
			return j + 1, b.String(), true
		}
		return n, b.String(), true
	case c == '"' || c == '`':
		k := strings.IndexByte(s[i+1:], c)
		if k < 0 {
			return n, "", true
		}
		return i + k + 2, "", true
	case c == '/' && i+1 < n && s[i+1] == '*':
		k := strings.Index(s[i+2:], "*/")
		if k < 0 {
			return n, "", true
		}
		return i + k + 4, "", true
	case c == '#', c == '-' && i+1 < n && s[i+1] == '-' && (i+2 == n || strings.IndexByte(" \t\n", s[i+2]) >= 0):
		k := strings.IndexByte(s[i:], '\n')
		if k < 0 {
			return n, "", true
		}
		return i + k + 1, "", true
	}
	return i, "", false
}

// blankBacktickNames is s with each name in backticks written "": callArgs
// does not read backticks, and a name such as `a,b`, `a)b` or `a'b` would
// split, close or open the arguments where MySQL and the rewrite for the
// copy read one name.
func blankBacktickNames(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); {
		end, _, skipped := skipQuoted(s, i)
		if !skipped {
			b.WriteByte(s[i])
			i++
			continue
		}
		if s[i] == '`' {
			b.WriteString(`""`)
		} else {
			b.WriteString(s[i:end])
		}
		i = end
	}
	return b.String()
}

// literalsIn is the text of each string in single quotes in s, read as
// skipQuoted reads them. Strings with nothing but white space or comments
// between them are one, as MySQL reads them: '2026-01-15 02:00:00' '+05:00'.
func literalsIn(s string) []string {
	var lits []string
	joins := false
	for i := 0; i < len(s); {
		end, lit, skipped := skipQuoted(s, i)
		switch {
		case !skipped:
			if strings.IndexByte(" \t\n\r\f\v", s[i]) < 0 {
				joins = false
			}
			i++
			continue
		case s[i] == '\'':
			if joins {
				lits[len(lits)-1] += lit
			} else {
				lits = append(lits, lit)
			}
			joins = true
		case s[i] == '"' || s[i] == '`':
			joins = false
		}
		i = end
	}
	return lits
}
