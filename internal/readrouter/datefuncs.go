package readrouter

import "strings"

// dateFormatSpecs are MySQL's DATE_FORMAT specifiers the copy's strftime
// writes with the same text, each with the copy's spelling (#2628). Measured
// one by one on MySQL 8.0.46 and DuckDB 1.5.6 over a DATE, a DATETIME, a
// DATETIME(6) and a TIMESTAMP, at midnight, noon, February 29 and the turns of
// a year: names in English (lc_time_names en_US, as MONTHNAME and DAYNAME on
// the copy), 12 for midnight under %h, %I and %l, AM at midnight and PM at
// noon, 000000 for the fraction of a DATE and 00 for its hour. %U is weeks
// from Sunday on both (MySQL's WEEK mode 0, C's %U), %v and %x the ISO week
// and its year (mode 3, the copy's %V and %G).
//
// Left out, so a format holding one is not rewritten: %D (29th), which the
// copy has no way to write; %u, %V and %X, weeks counted in ways the copy's
// strftime has none of; and every other letter, which MySQL prints as the
// letter itself and the copy reads as a specifier or refuses.
var dateFormatSpecs = map[byte]string{
	'a': "%a", 'b': "%b", 'c': "%-m", 'd': "%d", 'e': "%-d", 'f': "%f",
	'H': "%H", 'h': "%I", 'I': "%I", 'i': "%M", 'j': "%j", 'k': "%-H",
	'l': "%-I", 'M': "%B", 'm': "%m", 'p': "%p", 'r': "%I:%M:%S %p",
	'S': "%S", 's': "%S", 'T': "%H:%M:%S", 'U': "%U", 'v': "%V", 'W': "%A",
	'w': "%w", 'x': "%G", 'Y': "%Y", 'y': "%y", '%': "%%",
}

// strftimeFormat is MySQL's DATE_FORMAT format f, the text between its
// quotes, written for the copy's strftime. ok is false for an empty format
// (the copy refuses one), a % at the end, and a specifier dateFormatSpecs
// does not hold. A doubled quote inside f stays doubled: both sides read it
// as one quote.
func strftimeFormat(f string) (string, bool) {
	if f == "" {
		return "", false
	}
	var b strings.Builder
	for i := 0; i < len(f); i++ {
		if f[i] != '%' {
			b.WriteByte(f[i])
			continue
		}
		if i+1 == len(f) {
			return "", false
		}
		spec, ok := dateFormatSpecs[f[i+1]]
		if !ok {
			return "", false
		}
		b.WriteString(spec)
		i++
	}
	return b.String(), true
}

// mysqlDateCall rewrites MySQL's DATE_FORMAT(d, 'format') or DATEDIFF(a, b)
// for the copy, which has neither (#2628). word is the function's name, rest
// the text sent to the copy right after it. It returns the call the copy is
// sent and how many bytes of rest it replaces (its arguments and their
// parentheses); ok is false, and nothing is rewritten, when the call is not
// one it can write with certainty.
//
//   - DATE_FORMAT(d, 'format') is strftime(d, 'format'), the format in the
//     copy's specifiers (strftimeFormat), when the format is one string
//     literal written in the statement. A DATE is a DATE on the copy, a
//     DATETIME a wall clock and a TIMESTAMP an instant shown in the
//     connection's zone (#2035), so strftime writes what DATE_FORMAT writes.
//   - DATEDIFF(a, b) is date_diff('day', CAST(b AS DATE), CAST(a AS DATE)):
//     the days between the two dates, their times left out as MySQL leaves
//     them out, each cast in the connection's zone. Two arguments: the copy's
//     own datediff takes a unit first and three.
func mysqlDateCall(word, rest string) (call string, n int, ok bool) {
	args, n, ok := callArgs(rest)
	if !ok || len(args) != 2 {
		return "", 0, false
	}
	for _, a := range args {
		if strings.TrimSpace(a) == "" {
			return "", 0, false
		}
	}
	switch word {
	case "date_format":
		f, ok := singleLiteral(args[1])
		if !ok {
			return "", 0, false
		}
		format, ok := strftimeFormat(f)
		if !ok {
			return "", 0, false
		}
		return "strftime(" + args[0] + ", '" + format + "')", n, true
	case "datediff":
		return "date_diff('day', CAST(" + argText(args[1]) + " AS DATE), CAST(" + argText(args[0]) + " AS DATE))", n, true
	}
	return "", 0, false
}

// argText is an argument moved into another call: without the white space
// before it, nor the one after it but a line feed, which may end a -- comment
// inside it.
func argText(a string) string {
	return strings.TrimRight(strings.TrimLeft(a, " \t\n\r\f\v"), " \t\r\f\v")
}

// singleLiteral reports whether arg, white space aside, is one string literal
// in single quotes and nothing else, and returns the text between its quotes.
// Two literals side by side ('%Y' '-%m', one string on MySQL), a prefix
// (_utf8mb4'%Y', N'%Y') or a comment beside it are not one.
func singleLiteral(arg string) (string, bool) {
	t := strings.TrimSpace(arg)
	if len(t) < 2 || t[0] != '\'' || t[len(t)-1] != '\'' {
		return "", false
	}
	inner := t[1 : len(t)-1]
	for i := 0; i < len(inner); i++ {
		if inner[i] == '\'' {
			if i+1 == len(inner) || inner[i+1] != '\'' {
				return "", false
			}
			i++
		}
	}
	return inner, true
}

// callArgs reads a call's arguments from s, the text right after the
// function's name: white space, a parenthesis, and the arguments up to the
// parenthesis that closes it. It returns each argument as written (split at
// the commas outside parentheses, strings, double-quoted names and comments)
// and how many bytes of s the call takes. ok is false when s does not open
// with the parenthesis or never closes it.
//
// s is text scan has read: a backslash, a # comment and a double-quoted
// string are refused before it gets here, so every double quote quotes a
// name, and a -- comment is one only with white space after it, as MySQL
// reads it.
func callArgs(s string) (args []string, n int, ok bool) {
	i := 0
	for i < len(s) && strings.IndexByte(" \t\n\r\f\v", s[i]) >= 0 {
		i++
	}
	if i == len(s) || s[i] != '(' {
		return nil, 0, false
	}
	i++
	start, depth := i, 0
	for i < len(s) {
		c := s[i]
		switch {
		case c == '\'':
			j := i + 1
			for {
				k := strings.IndexByte(s[j:], '\'')
				if k < 0 {
					return nil, 0, false
				}
				j += k + 1
				if j < len(s) && s[j] == '\'' {
					j++
					continue
				}
				break
			}
			i = j
			continue
		case c == '"':
			k := strings.IndexByte(s[i+1:], '"')
			if k < 0 {
				return nil, 0, false
			}
			i += k + 2
			continue
		case c == '/' && i+1 < len(s) && s[i+1] == '*':
			k := strings.Index(s[i+2:], "*/")
			if k < 0 {
				return nil, 0, false
			}
			i += k + 4
			continue
		case c == '-' && i+1 < len(s) && s[i+1] == '-' && (i+2 == len(s) || s[i+2] == ' ' || s[i+2] == '\t' || s[i+2] == '\n'):
			k := strings.IndexByte(s[i:], '\n')
			if k < 0 {
				return nil, 0, false
			}
			i += k + 1
			continue
		case c == '(':
			depth++
		case c == ')':
			if depth == 0 {
				return append(args, s[start:i]), i + 1, true
			}
			depth--
		case c == ',' && depth == 0:
			args = append(args, s[start:i])
			start = i + 1
		}
		i++
	}
	return nil, 0, false
}

// The refusals of a DATE_FORMAT or a DATEDIFF of MySQL's still in the text
// the copy is about to run (UnroutedFunctionVeto).
var (
	dateFormatRefusal = UnroutedRefusal{
		What: "DATE_FORMAT",
		Why: "is not a function there. This port writes it strftime(d, format) for the copy only with a format written in the statement as one string, " +
			"not empty, whose specifiers the copy writes as MySQL does (all of MySQL's but %D, %u, %V and %X), " +
			"and in a statement whose names and strings it rewrites with certainty (no # comment, no dollar-quoted string, no -- comment without a space after it, no backslash or double-quoted string)",
		Instead: "strftime(d, format), in the copy's specifiers: %M for the minutes, %B for the month's name, %-d for the day without a zero",
	}
	dateDiffRefusal = UnroutedRefusal{
		What: "DATEDIFF(a, b)",
		Why: "is not read there with two arguments (the copy's own datediff takes a unit first). " +
			"This port writes it for the copy only in a statement whose names and strings it rewrites with certainty (no # comment, no dollar-quoted string, no -- comment without a space after it, no backslash or double-quoted string)",
		Instead: "date_diff('day', CAST(b AS DATE), CAST(a AS DATE))",
	}
)

// UnroutedFunctionVeto says whether text, the statement about to be sent to
// the copy on a connection without routing, still calls MySQL's DATE_FORMAT,
// or DATEDIFF with two arguments: one ForCopyOpts did not rewrite (a format
// that is not one string, a specifier the copy writes otherwise) or a
// statement sent as the client wrote it. The copy only fails on either, with
// its own error; the refusal says how to write it. Read as UnroutedVeto reads
// a statement, so a call inside a string or a comment refuses nothing, nor a
// name next to a dot (t.datediff) or in double quotes.
func UnroutedFunctionVeto(text string) (UnroutedRefusal, bool) {
	// Names emptied first: a -- inside one (`x-- y`) must not cut the rest
	// of the line, a call after it with it.
	bl := lineCommentTail.ReplaceAllString(quotedName.ReplaceAllString(scan(strings.ReplaceAll(text, `\`, " ")).blankedCopy, `""`), "")
	for i := 0; i < len(bl); {
		c := bl[i]
		switch {
		case c == '"':
			k := strings.IndexByte(bl[i+1:], '"')
			if k < 0 {
				return UnroutedRefusal{}, false
			}
			i += k + 2
		case wordByte(c):
			j := i
			for j < len(bl) && wordByte(bl[j]) {
				j++
			}
			w := strings.ToLower(bl[i:j])
			if (w == "date_format" || w == "datediff") && !namedByDot(bl, i, j-i) {
				if args, _, ok := callArgs(bl[j:]); ok {
					if w == "date_format" {
						return dateFormatRefusal, true
					}
					if len(args) == 2 {
						return dateDiffRefusal, true
					}
				}
			}
			i = j
		default:
			i++
		}
	}
	return UnroutedRefusal{}, false
}
