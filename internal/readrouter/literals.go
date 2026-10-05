package readrouter

import (
	"regexp"
	"sort"
	"strings"
)

// Constructs both sides answer, each with another value, that a pattern over
// single words cannot name (#2122). Each was measured on MySQL 8.4, MariaDB
// 11.4 and the copy before it was added.
const (
	vetoDigitWord      = "word that starts with a digit (a hexadecimal or bit literal, or a name, on MySQL; a number and an alias on the copy)"
	vetoPrefixString   = "x, b or e right against a string literal (a hexadecimal or bit string, or an aliased column, on MySQL; another string on the copy)"
	vetoIntervalUnit   = "INTERVAL with a quoted amount and a two-part unit (the copy reads the unit as an alias)"
	vetoTilde          = "~ (bitwise NOT over 64 unsigned bits on MySQL; signed, or a regular expression match, on the copy)"
	vetoDateArithmetic = "date or timestamp, or a group that holds one, next to + or -, or under AVG (a number built from its digits on MySQL; a date, a count of days or an interval on the copy)"
)

// shapeVetoes run after the pattern list, on the text with string literals
// blanked, comments removed and each quoted name emptied: what is inside a
// name is not syntax. Each reads the statement in time that grows with its
// length and no faster: Veto runs on every statement, in the process that
// captures, and nothing caps a statement's length before it.
var shapeVetoes = []struct {
	name string
	hit  func(bare string) bool
}{
	{vetoDigitWord, digitWord},
	{vetoPrefixString, prefixString.MatchString},
	{vetoIntervalUnit, intervalUnit.MatchString},
	{vetoTilde, func(bare string) bool { return strings.IndexByte(bare, '~') >= 0 }},
	{vetoDateArithmetic, dateArithmetic},
}

// quotedName is a name in the text the pattern list reads: scan has written
// every one in double quotes, refused a name that holds a double quote, and
// blanked the string literals, so each pair of double quotes is one name.
var quotedName = regexp.MustCompile(`"[^"]*"`)

// shapeVeto returns the first of shapeVetoes the statement matches, or "".
func shapeVeto(blankedCopy string) string {
	bare := quotedName.ReplaceAllString(blankedCopy, `""`)
	for _, v := range shapeVetoes {
		if v.hit(bare) {
			return v.name
		}
	}
	return ""
}

// digitWord reports a word that starts with a digit and that the two sides
// read differently. MySQL reads such a word as one token: `0x10` and `0b101`
// are a hexadecimal and a bit literal, `2fa`, `1_000`, `0X10` and `1e` are
// names. The copy reads the digits as a number and the rest as its alias:
// `SELECT 0x10` is 0 in a column named x10 there, `SELECT 2fa FROM users`
// the number 2 in a column named fa. Numbers are left alone: digits, and
// digits with an exponent (`1e5`, `1e+5`; what follows the exponent's digits
// is an alias on both sides).
//
// After a dot the digits are a number's decimals on both sides, and a letter
// after them starts an alias on both (`1.5abc`; the copy refuses `t.2fa`),
// with two exceptions that are reported. An underscore: the copy takes it
// for a digit separator, so `1.5_5` is 1.55 there and 1.5 under the alias _5
// on MySQL (any word that starts with a digit and holds an underscore is
// reported, `1e1_0` and `1.5_x` included). And an e with no exponent after
// it (`1.5e`, `1.e`, `1.5ex`): MySQL refuses the statement and the copy
// answers 1.5 under the alias e.
func digitWord(bare string) bool {
	n := len(bare)
	// numberEnd is where the last word made of digits alone ended: a dot
	// right there is a decimal point.
	numberEnd := -1
	for i := 0; i < n; {
		if !wordByte(bare[i]) {
			i++
			continue
		}
		j := i
		for j < n && wordByte(bare[j]) {
			j++
		}
		word, start := bare[i:j], i
		afterDot := start > 0 && bare[start-1] == '.'
		i = j
		if !asciiDigit(word[0]) {
			// `1.e2` is a number; `1.e` and `1.ex` are not one on MySQL.
			if afterDot && start-1 == numberEnd && (word[0] == 'e' || word[0] == 'E') && !exponent(bare, word, 0, j) {
				return true
			}
			continue
		}
		if strings.IndexByte(word, '_') >= 0 {
			return true
		}
		d := 0
		for d < len(word) && asciiDigit(word[d]) {
			d++
		}
		switch {
		case d == len(word):
			numberEnd = j
		case word[d] == 'e' || word[d] == 'E':
			if !exponent(bare, word, d, j) {
				return true
			}
		case !afterDot:
			return true
		}
	}
	return false
}

// exponent reports whether the e at word[e] opens an exponent: a digit
// follows it (1e5, and 1e5x on both sides), or the word ends at the e and a
// sign and a digit follow (1e+5, 1e-5). end is where the word ends in bare.
func exponent(bare, word string, e, end int) bool {
	if e+1 < len(word) {
		return asciiDigit(word[e+1])
	}
	return end+1 < len(bare) && (bare[end] == '+' || bare[end] == '-') && asciiDigit(bare[end+1])
}

func asciiDigit(c byte) bool { return '0' <= c && c <= '9' }

// prefixString is the letter x, b or e, as a whole word, written right
// against a string literal. x'41' is a hexadecimal string and b'1' a bit
// string on MySQL, where the copy answers the texts x41 and b1; e'x' is the
// column e under the alias x on MySQL and an escaped string on the copy.
// With white space between them the copy refuses the statement (it has no
// type of that name), and N'a' is the same string on both: neither is
// matched.
var prefixString = regexp.MustCompile(`(?i)\b[xbe]''`)

// intervalUnit is INTERVAL with a quoted amount (or a placeholder, which
// reaches the copy quoted) and a unit of two parts: MINUTE_SECOND,
// DAY_HOUR, YEAR_MONTH. The copy has no such units. It reads INTERVAL '1:30'
// by its own rules (an hour and a half) and takes the unit for the column's
// alias, so `d + INTERVAL '1:30' MINUTE_SECOND` is 00:01:30 on MySQL and
// 01:30:00 on the copy. With the amount unquoted, or with a unit of one
// word, the copy refuses the statement or agrees.
var intervalUnit = regexp.MustCompile(`(?i)\binterval\s*(?:''|\?)\s*[a-z]+_[a-z]+`)

var (
	// A typed literal: DATE '2026-01-01', TIMESTAMP '2026-01-01 10:00:00'.
	temporalLiteral = regexp.MustCompile(`(?i)\b(?:date|timestamp)\s*''`)
	// The opening of DATE(...) and of CAST(...).
	temporalCall = regexp.MustCompile(`(?i)\b(date|cast)\s*\(`)
)

// A group is a pair of parentheses or a CASE ... END.
type group struct {
	open   int // the opening parenthesis, or the C of CASE
	end    int // one past the closing parenthesis or the END; -1 while open
	parent int // the group this one is in, or -1
	isCase bool
}

// A temporal is a place where the text itself says "this is a date".
type temporal struct {
	start, end int // end is set for a typed literal
	paren      int // DATE( and CAST(: where the parenthesis opens; else -1
	cast       bool
	call       int // the group of that parenthesis
	in         int // the innermost group the shape is in, or -1
	word       bool
}

// dateArithmetic reports a + or a - written next to something the text
// itself says is a date or a date and time: a typed literal (DATE '...',
// TIMESTAMP '...'), DATE(...), or CAST(... AS DATE) and CAST(... AS
// DATETIME). MySQL turns the date into the number its digits spell (DATE
// '2026-01-01' + 1 is 20260102, and one date minus another is the
// difference of two such numbers); the copy answers a date (2026-01-02), a
// count of days or an interval.
//
// The date may sit inside something: GREATEST(DATE '...', d) + 1,
// (SELECT DATE(ts) FROM t) + 1, CASE WHEN a THEN DATE(ts) END + 1 and
// MAX(DATE(ts)) OVER () - 1 are numbers on MySQL and dates on the copy as
// well. So every group the date is in, each pair of parentheses and each
// CASE ... END, out to the whole statement, is looked at too: a + or - next
// to the group (before the name of the function, for a call) is reported,
// and so is a group that AVG is called on (AVG of a date is a number on
// MySQL and a date and time on the copy). A + or - that INTERVAL follows
// does not count: that is the same day on both (selected, MySQL shows a
// date and the copy a date and time: a difference listed in the docs, and
// one the text cannot show for a column either).
//
// It is the text's shape and no more. What the group returns is not known,
// so SUM(IF(d >= DATE '...', amount, 0)) - 1 and YEAR(DATE '...') + 1 are
// kept on MySQL too, for nothing. And a column, or an expression of one
// (created_on + 1, max(d) - min(d)), is not seen: nothing in the text or in
// the plan says what type it has.
func dateArithmetic(bare string) bool {
	shapes := temporalShapes(bare)
	if len(shapes) == 0 {
		return false
	}
	groups := readGroups(bare, shapes)
	seen := make([]bool, len(groups))
	for _, t := range shapes {
		if !t.word {
			continue // $date( and the like: another name
		}
		end := t.end
		if t.paren >= 0 {
			g := groups[t.call]
			if g.end < 0 || t.cast && !castToTemporal(bare, t.paren, g.end-1) {
				continue
			}
			end = g.end
		}
		if signBefore(bare, t.start) || signAfter(bare, end) {
			return true
		}
		for gi := t.in; gi >= 0 && !seen[gi]; gi = groups[gi].parent {
			seen[gi] = true
			if groupInArithmetic(bare, groups, gi) {
				return true
			}
		}
	}
	return false
}

// temporalShapes lists the typed literals and the DATE( and CAST( openings,
// in the order they are written.
func temporalShapes(bare string) []temporal {
	lits := temporalLiteral.FindAllStringIndex(bare, -1)
	calls := temporalCall.FindAllStringSubmatchIndex(bare, -1)
	shapes := make([]temporal, 0, len(lits)+len(calls))
	for len(lits) > 0 || len(calls) > 0 {
		if len(calls) == 0 || len(lits) > 0 && lits[0][0] < calls[0][0] {
			shapes = append(shapes, temporal{start: lits[0][0], end: lits[0][1], paren: -1, in: -1})
			lits = lits[1:]
			continue
		}
		m := calls[0]
		shapes = append(shapes, temporal{start: m[0], paren: m[1] - 1, cast: bare[m[2]] == 'c' || bare[m[2]] == 'C', in: -1})
		calls = calls[1:]
	}
	return shapes
}

// readGroups reads the statement once and returns its groups, in the order
// they open. On the way it fills in, for each shape, the group it is in,
// the group of its own parenthesis, and whether it starts a word.
func readGroups(bare string, shapes []temporal) []group {
	var groups []group
	top := -1      // the innermost open group
	si, pi := 0, 0 // the next shape to place, and the next to find the parenthesis of
	n := len(bare)
	for i := 0; i < n; {
		c := bare[i]
		switch {
		case c == '(':
			groups = append(groups, group{open: i, end: -1, parent: top})
			top = len(groups) - 1
			for pi < len(shapes) && (shapes[pi].paren < 0 || shapes[pi].paren < i) {
				pi++
			}
			if pi < len(shapes) && shapes[pi].paren == i {
				shapes[pi].call = top
				pi++
			}
			i++
		case c == ')':
			// A CASE left open inside these parentheses never closes.
			for top >= 0 && groups[top].isCase {
				top = groups[top].parent
			}
			if top >= 0 {
				groups[top].end = i + 1
				top = groups[top].parent
			}
			i++
		case wordByte(c):
			j := i
			for j < n && wordByte(bare[j]) {
				j++
			}
			for si < len(shapes) && shapes[si].start < j {
				shapes[si].in, shapes[si].word = top, shapes[si].start == i
				si++
			}
			if i == 0 || bare[i-1] != '.' {
				switch {
				case j-i == 4 && strings.EqualFold(bare[i:j], "case"):
					groups = append(groups, group{open: i, end: -1, parent: top, isCase: true})
					top = len(groups) - 1
				case j-i == 3 && top >= 0 && groups[top].isCase && strings.EqualFold(bare[i:j], "end"):
					groups[top].end = j
					top = groups[top].parent
				}
			}
			i = j
		default:
			i++
		}
	}
	return groups
}

// groupInArithmetic reports a + or - next to the group, or AVG called on it.
// For a call the left side is read before the function's name, and a window
// (OVER (...) or OVER name) after the call is stepped over.
func groupInArithmetic(bare string, groups []group, gi int) bool {
	g := groups[gi]
	if g.end < 0 {
		return false
	}
	left, right := g.open, g.end
	if !g.isCase {
		l := left - 1
		for l >= 0 && sqlSpace(bare[l]) {
			l--
		}
		w := l
		for w >= 0 && wordByte(bare[w]) {
			w--
		}
		if w < l {
			if strings.EqualFold(bare[w+1:l+1], "avg") {
				return true
			}
			left = w + 1
		}
		right = afterWindow(bare, groups, right)
	}
	return signBefore(bare, left) || signAfter(bare, right)
}

// afterWindow returns where the call that ends at pos ends once its window
// is counted: past OVER (...) or OVER name when one follows, else pos.
func afterWindow(bare string, groups []group, pos int) int {
	r := pos
	for r < len(bare) && sqlSpace(bare[r]) {
		r++
	}
	if r+4 > len(bare) || !strings.EqualFold(bare[r:r+4], "over") || r+4 < len(bare) && wordByte(bare[r+4]) {
		return pos
	}
	r += 4
	for r < len(bare) && sqlSpace(bare[r]) {
		r++
	}
	if r < len(bare) && bare[r] == '(' {
		// Groups are in the order they open.
		k := sort.Search(len(groups), func(k int) bool { return groups[k].open >= r })
		if k == len(groups) || groups[k].open != r || groups[k].end < 0 {
			return pos
		}
		return groups[k].end
	}
	for r < len(bare) && wordByte(bare[r]) {
		r++
	}
	return r
}

// signBefore reports a + or - as the last thing before pos, white space
// aside.
func signBefore(bare string, pos int) bool {
	l := pos - 1
	for l >= 0 && sqlSpace(bare[l]) {
		l--
	}
	return l >= 0 && (bare[l] == '+' || bare[l] == '-')
}

// signAfter reports a + or - as the first thing from pos on, white space
// aside, unless INTERVAL follows it.
func signAfter(bare string, pos int) bool {
	r := pos
	for r < len(bare) && sqlSpace(bare[r]) {
		r++
	}
	if r >= len(bare) || bare[r] != '+' && bare[r] != '-' {
		return false
	}
	r++
	for r < len(bare) && sqlSpace(bare[r]) {
		r++
	}
	const interval = "interval"
	if r+len(interval) > len(bare) || !strings.EqualFold(bare[r:r+len(interval)], interval) {
		return true
	}
	return r+len(interval) < len(bare) && wordByte(bare[r+len(interval)])
}

// castToTemporal reports whether the CAST whose parentheses are at open and
// closing ends in AS DATE or AS DATETIME, with or without a precision. It
// reads backwards from the closing parenthesis and no further than the type.
func castToTemporal(bare string, open, closing int) bool {
	p := closing - 1
	back := func(skip func(byte) bool) {
		for p > open && skip(bare[p]) {
			p--
		}
	}
	back(sqlSpace)
	if bare[p] == ')' {
		p--
		back(func(c byte) bool { return sqlSpace(c) || asciiDigit(c) })
		if bare[p] != '(' {
			return false
		}
		p--
		back(sqlSpace)
	}
	e := p
	back(wordByte)
	if typ := bare[p+1 : e+1]; !strings.EqualFold(typ, "date") && !strings.EqualFold(typ, "datetime") {
		return false
	}
	back(sqlSpace)
	e = p
	back(wordByte)
	return strings.EqualFold(bare[p+1:e+1], "as")
}

func sqlSpace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == '\v'
}
