package readrouter

import (
	"regexp"
	"strings"
	"unicode"
)

// VetoUnixTime is why UnixTimeVeto keeps a statement on MySQL because of a
// UNIX_TIMESTAMP call, VetoFromUnixTime because of a FROM_UNIXTIME call.
const (
	VetoUnixTime     = "UNIX_TIMESTAMP of something other than a DATE, or a DATETIME or TIMESTAMP column without fractional seconds (a decimal or another reading on MySQL)"
	VetoFromUnixTime = "FROM_UNIXTIME of something MySQL does not type as a whole number, or with a format (MySQL prints a fraction the copy does not)"
)

// unixTimeCall and fromUnixTimeCall open a call of the function, its name
// bare or quoted (the shape writes a backtick as a double quote, and MySQL
// still calls the built-in function), not a column or a qualified name that
// happens to be called that.
var (
	unixTimeCall     = regexp.MustCompile(`(?i)(?:^|[^\pL\pN_$."])(?:unix_timestamp|"unix_timestamp")\s*\(`)
	fromUnixTimeCall = regexp.MustCompile(`(?i)(?:^|[^\pL\pN_$."])(?:from_unixtime|"from_unixtime")\s*\(`)
)

// TimeColumns are the columns of the tables a statement reads, by name, all
// tables together, the way UnixTimeVeto needs them: Dates the columns
// UNIX_TIMESTAMP gives MySQL's number of (baseline.UnixTimeColumns) and
// NotDates the rest, Whole the columns MySQL types as a whole number
// (baseline.WholeNumberColumns) and NotWhole the rest. A name in both lists
// of a pair (two tables, two types) counts as the second.
type TimeColumns struct {
	Dates, NotDates, Whole, NotWhole []string
}

// UnixTimeVeto says why the copy must not answer a statement because of a
// UNIX_TIMESTAMP or FROM_UNIXTIME call, or "" when it can (#2493).
//
// The copy's unix_timestamp (a macro the SQL worker defines) gives the
// seconds of a date or a time in the session's zone as a whole number. MySQL
// gives the same number for a DATE and for a DATETIME or TIMESTAMP without
// fractional seconds, and a decimal for one with them; over a string or a
// number it reads the value as a date its own way. So every call's argument
// must be a bare column in Dates.
//
// The copy's from_unixtime prints no fraction. MySQL prints as many fraction
// digits as the argument's TYPE has, whatever its value, so the argument must
// be one MySQL types as a whole number (wholeExpr).
//
// The rest stays on MySQL as DistinctVeto keeps it: a name the statement may
// define itself (any subquery, derived table or WITH), and a call after GROUP
// BY, HAVING or ORDER BY, where MySQL reads a select-list alias before the
// column. The no-argument UNIX_TIMESTAMP (now) is vetoed from the text
// (vetoes).
func UnixTimeVeto(shape string, c TimeColumns) string {
	unix := unixTimeCall.FindAllStringIndex(shape, -1)
	from := fromUnixTimeCall.FindAllStringIndex(shape, -1)
	if len(unix) == 0 && len(from) == 0 {
		return ""
	}
	// The reason of the first function the statement calls, for a refusal
	// that is about the statement and not one call.
	whole := VetoFromUnixTime
	if len(unix) > 0 && (len(from) == 0 || unix[0][0] < from[0][0]) {
		whole = VetoUnixTime
	}
	if subquery.MatchString(shape) {
		return whole
	}
	last := 0
	for _, l := range append(append([][]int(nil), unix...), from...) {
		last = max(last, l[0])
	}
	if a := aliasClause.FindStringIndex(shape); a != nil && a[0] < last {
		return whole
	}
	dates := knownNames(c.Dates, c.NotDates)
	for _, l := range unix {
		body, ok := closingBody(shape, l[1])
		if !ok {
			return VetoUnixTime
		}
		m := columnRef.FindStringSubmatch(unwrap(strings.TrimSpace(body)))
		if m == nil || !dates[sameName(unquote(m[1]))] {
			return VetoUnixTime
		}
	}
	ints := knownNames(c.Whole, c.NotWhole)
	for _, l := range from {
		body, ok := closingBody(shape, l[1])
		if !ok {
			return VetoFromUnixTime
		}
		p := &wholeParser{s: body, ints: ints}
		if !p.expr(false) || !p.end() {
			return VetoFromUnixTime
		}
	}
	return ""
}

// UnixTimeVeto is UnixTimeVeto over this shape, with TimeColumns' four lists
// (sqlsandbox.ColumnTypes, which does not name this package's types).
func (s StatementShape) UnixTimeVeto(dates, notDates, whole, notWhole []string) string {
	return UnixTimeVeto(string(s), TimeColumns{Dates: dates, NotDates: notDates, Whole: whole, NotWhole: notWhole})
}

// knownNames is the names in safe and not in other, compared as MySQL
// compares column names.
func knownNames(safe, other []string) map[string]bool {
	notSafe := make(map[string]bool, len(other))
	for _, n := range other {
		notSafe[sameName(n)] = true
	}
	known := make(map[string]bool, len(safe))
	for _, n := range safe {
		if f := sameName(n); !notSafe[f] {
			known[f] = true
		}
	}
	return known
}

func unquote(name string) string {
	if strings.HasPrefix(name, `"`) && len(name) >= 2 {
		return name[1 : len(name)-1]
	}
	return name
}

// wholeParser reads a FROM_UNIXTIME argument and accepts only an expression
// MySQL types as a whole number, the forms measured on MySQL 8.0.46 to print
// no fraction:
//
//	expr    = operand { ("+" | "-" | "*" | DIV) operand }
//	operand = ("+" | "-") operand | digits | whole-number column
//	        | "(" expr ")" | FLOOR/CEIL/CEILING "(" quotient ")"
//	        | UNIX_TIMESTAMP "(" ... ")"
//
// where a quotient is an expr that may also divide with "/": MySQL's / gives
// a decimal, and FLOOR or CEIL of it a whole number computed over the exact
// quotient (FLOOR(86399/86400) is 0 although 86399/86400 prints 1.0000). The
// copy divides as a DOUBLE, exact only while every number stays under 2^53:
// FLOOR(1700000000999999999/1000000000) is 1700000001 there. So a quotient
// holds no column and no "*", only UNIX_TIMESTAMP calls (at most 2^31) and
// literals of at most seven digits, joined by +, - and /. A decimal or a
// DOUBLE anywhere is refused, inside FLOOR too: FLOOR of a DOUBLE stays a
// DOUBLE, and times 3600 prints .000000. UNIX_TIMESTAMP's own argument is
// checked by the caller; it is a whole number over a column in Dates.
type wholeParser struct {
	s    string
	i    int
	ints map[string]bool
}

func (p *wholeParser) skip() {
	for p.i < len(p.s) && unicode.IsSpace(rune(p.s[p.i])) {
		p.i++
	}
}

func (p *wholeParser) end() bool {
	p.skip()
	return p.i == len(p.s)
}

func (p *wholeParser) expr(quotient bool) bool {
	if !p.operand(quotient) {
		return false
	}
	for {
		p.skip()
		if p.i >= len(p.s) {
			return true
		}
		switch ch := p.s[p.i]; {
		case ch == '+' || ch == '-' || (ch == '*' && !quotient) || (ch == '/' && quotient):
			p.i++
		case p.word() == "div" && !quotient:
			p.i += 3
		default:
			return true
		}
		if !p.operand(quotient) {
			return false
		}
	}
}

// word is the identifier that starts at the cursor, lower-cased, or "".
func (p *wholeParser) word() string {
	j := p.i
	for j < len(p.s) && isNameByte(p.s[j]) {
		j++
	}
	return strings.ToLower(p.s[p.i:j])
}

func isNameByte(b byte) bool {
	return b == '_' || b == '$' || b >= 0x80 || ('a' <= b && b <= 'z') || ('A' <= b && b <= 'Z') || ('0' <= b && b <= '9')
}

func (p *wholeParser) operand(quotient bool) bool {
	p.skip()
	if p.i >= len(p.s) {
		return false
	}
	switch ch := p.s[p.i]; {
	case ch == '+' || ch == '-':
		p.i++
		return p.operand(quotient)
	case ch == '(':
		body, ok := closingBody(p.s, p.i+1)
		if !ok {
			return false
		}
		in := &wholeParser{s: body, ints: p.ints}
		if !in.expr(quotient) || !in.end() {
			return false
		}
		p.i += 1 + len(body) + 1
		return true
	case '0' <= ch && ch <= '9':
		j := p.i
		for j < len(p.s) && '0' <= p.s[j] && p.s[j] <= '9' {
			j++
		}
		// 1.5, 1e3 and 0x10 are not whole-number literals on MySQL.
		if j < len(p.s) && (p.s[j] == '.' || isNameByte(p.s[j])) {
			return false
		}
		if quotient && j-p.i > 7 {
			return false
		}
		p.i = j
		return true
	}
	// A name: a column, or a function when "(" follows.
	start := p.i
	for p.i < len(p.s) {
		switch b := p.s[p.i]; {
		case b == '"':
			end := strings.IndexByte(p.s[p.i+1:], '"')
			if end < 0 {
				return false
			}
			p.i += end + 2
		case isNameByte(b) || b == '.':
			p.i++
		default:
			goto named
		}
	}
named:
	name := p.s[start:p.i]
	if name == "" {
		return false
	}
	p.skip()
	if p.i < len(p.s) && p.s[p.i] == '(' {
		body, ok := closingBody(p.s, p.i+1)
		if !ok {
			return false
		}
		p.i += 1 + len(body) + 1
		switch strings.ToLower(unquote(name)) {
		case "floor", "ceil", "ceiling":
			in := &wholeParser{s: body, ints: p.ints}
			return in.expr(true) && in.end()
		case "unix_timestamp":
			return true
		}
		return false
	}
	if quotient {
		return false
	}
	m := columnRef.FindStringSubmatch(name)
	return m != nil && p.ints[sameName(unquote(m[1]))]
}
