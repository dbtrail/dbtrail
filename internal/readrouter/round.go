package readrouter

import (
	"regexp"
	"strings"
	"unicode"
	"unicode/utf8"
)

// VetoRound is why RoundVeto keeps a statement on MySQL.
const VetoRound = "ROUND of a FLOAT, a DOUBLE, text, an expression or a column not known to be a whole number or an exact DECIMAL (a half rounds to even on MySQL, away from zero on the copy)"

var (
	// roundCall opens a ROUND call, its name bare or quoted (the shape writes
	// a backtick as a double quote, and MySQL still calls the built-in
	// function). A dot before it is matched too: a qualified name is a
	// stored function on MySQL, which the copy does not have.
	roundCall = regexp.MustCompile(`(?i)"?round"?[\s\v]*\(`)
	// exactAggregate is SUM, MIN or MAX of what follows, up to the closing
	// parenthesis: over an exact column each is exact on both sides. AVG is
	// not: the copy returns a DOUBLE.
	exactAggregate = regexp.MustCompile(`(?i)^(?:sum|min|max)[\s\v]*\(`)
	// exactNumber is a number written without an exponent: a DECIMAL (or a
	// whole number) on both sides. With an exponent it is a DOUBLE.
	exactNumber = regexp.MustCompile(`^[+-]?\s*(?:\d+(?:\.\d*)?|\.\d+)$`)
	// roundDigits is the second argument the copy may be given: a whole
	// number written in the statement, negative or not.
	roundDigits = regexp.MustCompile(`^[+-]?\s*\d+$`)
)

// maxExactLiteralDigits is the widest number, in digits, the copy reads as a
// DECIMAL: DECIMAL(38).
const maxExactLiteralDigits = 38

// RoundVeto says why the copy must not answer a statement because of a ROUND
// call, or "" when it can (#2641). Over a FLOAT or a DOUBLE MySQL rounds a
// half to even and the copy away from zero: ROUND(2.5e0) is 2 on MySQL and
// 3 on the copy, ROUND(0.125e0, 2) 0.12 and 0.13 (MySQL 8.0.46, DuckDB
// 1.5.6). MySQL reads text as a DOUBLE there too. Over a whole number or an
// exact DECIMAL both round a half away from zero and print the same digits.
// shape is Shape of the statement; safe are the columns of the tables it
// reads that are a whole number or an exact DECIMAL the copy holds as a
// decimal (baseline.DistinctColumns: the same split DISTINCT needs), other
// all their other columns, the ones whose type is not known among them.
//
// The copy may answer when every ROUND's first argument is a bare column
// whose name is in safe and in no table's other, SUM, MIN or MAX of one, or
// a number written without an exponent, and its second argument, if any, a
// whole number written in the statement. Everything else stays on MySQL:
// AVG (a DOUBLE on the copy), arithmetic, another call, a placeholder. So
// does what DistinctVeto keeps there for the same reasons: a name the
// statement may define itself (any subquery, derived table or WITH) and a
// call after GROUP BY, HAVING or ORDER BY, where MySQL reads a select-list
// alias before the column.
func RoundVeto(shape string, safe, other []string) string {
	locs := callsOf(roundCall, shape)
	if len(locs) == 0 {
		return ""
	}
	if subquery.MatchString(shape) {
		return VetoRound
	}
	if c := aliasClause.FindStringIndex(shape); c != nil && c[0] < locs[len(locs)-1][0] {
		return VetoRound
	}
	exact := knownNames(safe, other)
	for _, l := range locs {
		body, ok := closingBody(shape, l[1])
		if !ok {
			return VetoRound
		}
		args, ok := topLevelArgs(body)
		if !ok || len(args) < 1 || len(args) > 2 {
			return VetoRound
		}
		if len(args) == 2 && !roundDigits.MatchString(unwrap(args[1])) {
			return VetoRound
		}
		if !exactArgument(unwrap(args[0]), exact) {
			return VetoRound
		}
	}
	return ""
}

// callsOf is where re matches in s and is not the end of a longer name: the
// character before the match is not a letter, a digit, _ or $. The
// character before is read apart from the match, so a call right inside
// another (ROUND(ROUND(x))) is found too: a pattern that matched it would
// have consumed the parenthesis the inner call needs.
func callsOf(re *regexp.Regexp, s string) [][]int {
	var out [][]int
	for from := 0; from < len(s); {
		l := re.FindStringIndex(s[from:])
		if l == nil {
			break
		}
		l[0], l[1] = l[0]+from, l[1]+from
		if r, _ := utf8.DecodeLastRuneInString(s[:l[0]]); l[0] == 0 || !(unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_' || r == '$') {
			out = append(out, l)
		}
		from = l[1]
	}
	return out
}

// RoundVeto is RoundVeto over this shape.
func (s StatementShape) RoundVeto(safe, other []string) string {
	return RoundVeto(string(s), safe, other)
}

// exactArgument reports whether arg, trimmed and unwrapped, is known to be
// exact on both sides: a number written without an exponent, a bare column
// in exact, or SUM, MIN or MAX of such a column.
func exactArgument(arg string, exact map[string]bool) bool {
	if exactNumber.MatchString(arg) {
		// Past 38 digits in all (leading zeros and decimals included) the
		// copy reads the literal as a DOUBLE, where MySQL keeps a DECIMAL of
		// up to 65 (measured on DuckDB 1.5.6).
		digits := 0
		for _, r := range arg {
			if r >= '0' && r <= '9' {
				digits++
			}
		}
		return digits <= maxExactLiteralDigits
	}
	if m := exactAggregate.FindStringIndex(arg); m != nil {
		body, ok := closingBody(arg, m[1])
		if !ok || m[1]+len(body)+1 != len(arg) {
			return false
		}
		arg = unwrap(strings.TrimSpace(body))
	}
	c := columnRef.FindStringSubmatch(arg)
	return c != nil && exact[sameName(unquote(c[1]))]
}

// topLevelArgs splits a call's body at the commas outside parentheses, each
// argument trimmed. A body with no text is no argument; an empty argument
// between two commas is not a body any call here takes.
func topLevelArgs(body string) ([]string, bool) {
	if strings.TrimSpace(body) == "" {
		return nil, true
	}
	var args []string
	depth, start := 0, 0
	for i := 0; i < len(body); i++ {
		switch body[i] {
		case '(':
			depth++
		case ')':
			depth--
		case ',':
			if depth == 0 {
				args = append(args, strings.TrimSpace(body[start:i]))
				start = i + 1
			}
		}
	}
	args = append(args, strings.TrimSpace(body[start:]))
	for _, a := range args {
		if a == "" {
			return nil, false
		}
	}
	return args, true
}
