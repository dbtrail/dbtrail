package readrouter

import (
	"regexp"
	"strings"
)

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

// vetoDateDiff is the name of the veto that keeps TIMESTAMPDIFF, DATEDIFF
// and TIMEDIFF on MySQL; CopyOptions.MySQLDateFunctions narrows it to the
// first and the last (dateDiffsRewritten).
const vetoDateDiff = "TIMESTAMPDIFF/DATEDIFF"

var (
	// timeDiffOnly is what the DATEDIFF veto still matches when the copy is
	// sent MySQL's DATEDIFF as date_diff.
	timeDiffOnly = regexp.MustCompile(`(?i)\b(timestampdiff|timediff)` + callOpen)
	// dateDiffCall opens a DATEDIFF call; callsOf keeps out a longer name.
	dateDiffCall = regexp.MustCompile(`(?i)datediff` + callOpen)
)

// dateDiffsRewritten reports whether every DATEDIFF call of blanked (a
// statement's blankedCopy) is one copyText writes as date_diff: two
// arguments, neither empty, the name not next to a dot (db.datediff is a
// stored function on MySQL). callArgs reads the arguments here as
// mysqlDateCall reads them in the copy's text: the comments the blanked text
// has dropped sit inside an argument, and a string literal is one there and
// here.
func dateDiffsRewritten(blanked string) bool {
	for _, l := range callsOf(dateDiffCall, blanked) {
		if namedByDot(blanked, l[0], len("datediff")) {
			return false
		}
		args, _, ok := callArgs(blanked[l[0]+len("datediff"):])
		if !ok || len(args) != 2 {
			return false
		}
		for _, a := range args {
			if strings.TrimSpace(a) == "" {
				return false
			}
		}
	}
	return true
}

// VetoDateFunction is why DateFunctionVeto keeps a statement on MySQL.
const VetoDateFunction = "DATE_FORMAT or DATEDIFF of something other than a DATE, DATETIME or TIMESTAMP column, NOW(), CURDATE(), UTC_TIMESTAMP() or UTC_DATE() (MySQL reads a string, a number or an expression into a date its own way)"

var (
	// dateFunctionCall opens a DATE_FORMAT or a DATEDIFF call.
	dateFunctionCall = regexp.MustCompile(`(?i)(date_format|datediff)` + callOpen)
	// wholeClockCall is an argument that is one of the four clock calls and
	// nothing else.
	wholeClockCall = regexp.MustCompile(`(?i)^(?:now|curdate|utc_timestamp|utc_date)\s*\(\s*\)$`)
)

// stringAliased reports whether a blanked shape names a select item with a
// string (s 'd', COUNT(*) AS 'n'): a literal, blanked to two quotes, right after AS,
// a parenthesis, a quoted name or a word that is not a keyword a value
// follows. aliased cannot read such a name, which the blanking has emptied.
func stringAliased(shape string) bool {
	for i := 0; ; {
		k := strings.Index(shape[i:], "''")
		if k < 0 {
			return false
		}
		at := i + k
		i = at + 2
		before := strings.TrimRight(shape[:at], " \t\n\r\f\v")
		if before == "" {
			continue
		}
		if c := before[len(before)-1]; c == ')' || c == '"' {
			return true
		}
		j := len(before)
		for j > 0 && wordByte(before[j-1]) {
			j--
		}
		if w := strings.ToLower(before[j:]); w != "" && (w == "as" || !stringAliasOK[w]) {
			return true
		}
	}
}

// stringAliasOK are the words a string literal follows as a value, not as a
// name.
var stringAliasOK = map[string]bool{"like": true, "then": true, "else": true, "when": true, "and": true, "or": true,
	"not": true, "in": true, "between": true, "is": true, "interval": true, "by": true, "select": true, "on": true,
	"where": true, "having": true, "case": true, "values": true, "escape": true, "regexp": true, "rlike": true,
	"xor": true, "distinct": true, "all": true, "any": true, "some": true, "limit": true, "offset": true, "return": true}

// DateFunctionVeto says why the copy must not answer a routed statement
// because of what its DATE_FORMAT and DATEDIFF calls are given, or "" when
// it can (#2611). Under routing the copy is sent DATE_FORMAT(d, 'format') as
// strftime and DATEDIFF(a, b) as date_diff over two casts to DATE
// (mysqlDateCall), which give MySQL's answer over a date: a DATE, a DATETIME
// (a wall clock on the copy), a TIMESTAMP (an instant both show in the
// session's zone), and NOW(), CURDATE(), UTC_TIMESTAMP() and UTC_DATE(),
// which the copy's session has with MySQL's values. Over text MySQL parses
// the date its own way: it takes 20260115 for a date, and reads an offset
// written in the text ('2026-01-15 02:00:00+05:00') into the session's zone,
// where the copy refuses the first and casts the second to its own date. So
// each date argument (DATE_FORMAT's first, both of DATEDIFF's) must be a bare
// column in dates and in no table's notDates, or one of the four calls. The
// format is not read here: one copyText could not write is sent as written,
// and the copy refuses DATE_FORMAT.
//
// As DistinctVeto and UnixTimeVeto keep it, a name the statement may define
// itself stays on MySQL: any subquery, derived table or WITH, and a column
// name the statement also gives a select item (AS d, or s d before a comma),
// which MySQL reads as the column in some clauses and the copy as the alias.
// A call next to a dot (db.date_format) is a stored function on MySQL.
func DateFunctionVeto(shape string, dates, notDates []string) string {
	calls := callsOf(dateFunctionCall, shape)
	if len(calls) == 0 {
		return ""
	}
	if subquery.MatchString(shape) {
		return VetoDateFunction
	}
	if stringAliased(shape) {
		// A name given in quotes ('d'): blanked here, so not one aliased
		// can read.
		return VetoDateFunction
	}
	known := knownNames(dates, notDates)
	// Without the double quotes the shape writes every name in: AS `d` is
	// AS "d" here, and aliased reads a bare word.
	low := strings.ToLower(strings.ReplaceAll(shape, `"`, ""))
	for _, l := range calls {
		name := strings.ToLower(dateFunctionCall.FindStringSubmatch(shape[l[0]:l[1]])[1])
		if namedByDot(shape, l[0], len(name)) {
			return VetoDateFunction
		}
		args, _, ok := callArgs(shape[l[0]+len(name):])
		if !ok || len(args) != 2 {
			return VetoDateFunction
		}
		dateArgs := args
		if name == "date_format" {
			dateArgs = args[:1]
		}
		for _, a := range dateArgs {
			a = unwrap(strings.TrimSpace(a))
			if wholeClockCall.MatchString(a) {
				continue
			}
			m := columnRef.FindStringSubmatch(a)
			if m == nil {
				return VetoDateFunction
			}
			col := unquote(m[1])
			if !known[sameName(col)] || aliased(low, strings.ToLower(col)) {
				return VetoDateFunction
			}
		}
	}
	return ""
}

// DateFunctionVeto is DateFunctionVeto over this shape.
func (s StatementShape) DateFunctionVeto(dates, notDates []string) string {
	return DateFunctionVeto(string(s), dates, notDates)
}

// wholeDateFunctionArg reports whether the text from start to end of the
// blanked, lowercased statement b is one whole argument of a DATEDIFF, the
// first argument of a DATE_FORMAT, or the second or third of a TIMESTAMPDIFF:
// a ( or a , right before it, a , or a ) right after it, white space aside,
// and the call it sits in one of those three, its name not next to a dot. dateUses lets CURDATE() and UTC_DATE()
// stand there (#2611): DATEDIFF casts each argument to a date and
// DATE_FORMAT writes it, so a date is the same date on both sides.
func wholeDateFunctionArg(b string, start, end int) bool {
	before := strings.TrimRight(b[:start], " \t\n\r\f\v")
	after := strings.TrimLeft(b[end:], " \t\n\r\f\v")
	if before == "" || after == "" || !strings.ContainsRune("(,", rune(before[len(before)-1])) || !strings.ContainsRune(",)", rune(after[0])) {
		return false
	}
	// Back to the parenthesis that opens the call, counting the commas at
	// its depth. A quoted name may hold any character: skipped whole.
	arg, depth, open := 0, 0, -1
	for i := start - 1; i >= 0 && open < 0; i-- {
		switch c := b[i]; {
		case c == '"':
			k := strings.LastIndexByte(b[:i], '"')
			if k < 0 {
				return false
			}
			i = k
		case c == ')':
			depth++
		case c == '(' && depth > 0:
			depth--
		case c == '(':
			open = i
		case c == ',' && depth == 0:
			arg++
		}
	}
	if open < 0 {
		return false
	}
	head := strings.TrimRight(b[:open], " \t\n\r\f\v")
	j := len(head)
	for j > 0 && wordByte(head[j-1]) {
		j--
	}
	if namedByDot(head, j, len(head)-j) {
		return false
	}
	switch head[j:] {
	case "datediff":
		return arg <= 1
	case "date_format":
		return arg == 0
	case "timestampdiff":
		// Either date (#2611): read at its midnight on both sides.
		return arg == 1 || arg == 2
	}
	return false
}

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
