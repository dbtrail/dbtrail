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
	vetoDigitWord        = "word that starts with a digit (a hexadecimal or bit literal, or a name, on MySQL; a number and an alias on the copy)"
	vetoPrefixString     = "x, b or e right against a string literal (a hexadecimal or bit string, or an aliased column, on MySQL; another string on the copy)"
	vetoIntervalAmount   = "INTERVAL with a quoted amount that is not a whole number (cut at the first character that is not a digit on MySQL; read as a number on the copy)"
	vetoIntervalExpr     = "INTERVAL with an amount in parentheses or a placeholder (rounded on MySQL, cut on the copy)"
	vetoIntervalUnit     = "INTERVAL with a quoted amount and a two-part unit (the copy reads the unit as an alias)"
	vetoTilde            = "~ (bitwise NOT over 64 unsigned bits on MySQL; signed, or a regular expression match, on the copy)"
	vetoDateArithmetic   = "date or timestamp, or a group that holds one, next to + or -, or under AVG (a number built from its digits on MySQL; a date, a count of days or an interval on the copy)"
	vetoLimitComma       = "LIMIT offset, count (the copy only reads LIMIT count OFFSET offset)"
	vetoOrderByNull      = "ORDER BY NULL (the copy refuses to sort by a constant)"
	vetoBinaryIntroducer = "_binary before a string literal (the copy has no such thing)"
	vetoBitOperator      = "bit operator or function: |, &, >>, BIT_COUNT, BIT_AND, BIT_OR, BIT_XOR (64 unsigned bits on MySQL, signed on the copy; over no rows, a number on MySQL and NULL on the copy)"
	vetoCastDatetime     = "CAST to DATETIME or TIME (a fraction of a second is rounded on MySQL, cut on MariaDB and kept on the copy)"
	vetoTwoDigitYear     = "string that starts with a two-digit year (year 2026 or 1970 on MySQL; year 26 or 70 on the copy)"
)

// shapeText is the text the shape checks read: the statement with string
// literals blanked, comments removed and each quoted name emptied (what is
// inside a name is not syntax), and the groups found in it.
//
// Veto runs on every statement, in the process that captures, and nothing
// caps a statement's length before it, so these checks must do work that
// grows with the statement's length and no faster. To let a test hold them
// to that, they read the text and the groups ONLY through the methods
// below, each of which counts what it hands out. Code that reads t.s or
// t.groups directly is work the test cannot see.
type shapeText struct {
	s      string
	groups []group
	steps  int
}

func (t *shapeText) len() int { return len(t.s) }

// at is one byte of the text.
func (t *shapeText) at(i int) byte {
	t.steps++
	return t.s[i]
}

// sub is a piece of the text, counted by its length.
func (t *shapeText) sub(from, to int) string {
	t.steps += to - from + 1
	return t.s[from:to]
}

// group is one group, by the order it opens in.
func (t *shapeText) group(k int) *group {
	t.steps++
	return &t.groups[k]
}

// all is the whole text, for a single pass of a pattern over it.
func (t *shapeText) all() string {
	t.steps += len(t.s)
	return t.s
}

var shapeVetoes = []struct {
	name string
	hit  func(t *shapeText) bool
}{
	{vetoDigitWord, digitWord},
	{vetoPrefixString, func(t *shapeText) bool { return prefixString.MatchString(t.all()) }},
	{vetoIntervalExpr, func(t *shapeText) bool { return intervalExpr.MatchString(t.all()) }},
	{vetoIntervalUnit, func(t *shapeText) bool { return intervalUnit.MatchString(t.all()) }},
	{vetoTilde, func(t *shapeText) bool { return strings.IndexByte(t.all(), '~') >= 0 }},
	{vetoDateArithmetic, dateArithmetic},
	{vetoLimitComma, func(t *shapeText) bool { return limitComma.MatchString(t.all()) }},
	{vetoOrderByNull, orderByNullKey},
	{vetoBinaryIntroducer, func(t *shapeText) bool { return binaryIntroducer.MatchString(t.all()) }},
	{vetoBitOperator, func(t *shapeText) bool { return bitOperator.MatchString(t.all()) }},
	{vetoCastDatetime, func(t *shapeText) bool { return castDatetime.MatchString(t.all()) }},
}

// bitOperator is |, & or >> anywhere, or a call of BIT_COUNT, BIT_AND, BIT_OR
// or BIT_XOR (#2133). MySQL and MariaDB compute them over 64 unsigned bits and
// the copy over signed numbers: -1 | 0 is 18446744073709551615 on the source
// and -1 on the copy, -8 >> 1 is 9223372036854775804 and -4, BIT_COUNT(-1) is
// 64 and 32, and WHERE n | 0 > 0 keeps other rows. Over no rows BIT_AND is
// 18446744073709551615 and BIT_OR and BIT_XOR are 0 on the source, and all
// three are NULL on the copy. Over operands that are not negative both sides
// agree, and the text does not say which a column holds, so every one is
// kept on the source: they are rare in what an application sends.
//
// << is not matched: where it would differ (a negative operand, a result
// past 31 bits) the copy refuses the statement and the source answers.
// || (the logical OR), ^ and ~ are vetoed before this, and && is refused by
// the copy; the & here covers it too.
var bitOperator = regexp.MustCompile(`(?i)[|&]|>>|\bbit_(?:count|and|or|xor)\s*\(`)

// castDatetime is CAST(... AS DATETIME) and CAST(... AS TIME), with or
// without a precision (#2133). A value with more decimals of a second than
// the type keeps is rounded by MySQL, cut by MariaDB and kept whole by the
// copy: CAST('2026-01-01 10:00:00.6' AS DATETIME) is 10:00:01 on MySQL 8.4,
// 10:00:00 on MariaDB 11.4 and 10:00:00.6 on the copy, and a DATETIME(6)
// column under the same cast likewise. What the value holds is not in the
// text, so the cast itself is kept on the source. The closing parenthesis is
// asked for so that an alias (`created_at AS time`, what a dashboard sends)
// is not taken for one. CAST(... AS DATE) is the same day on both.
var castDatetime = regexp.MustCompile(`(?i)\bas\s+(?:datetime|time)\s*(?:\(\s*\d*\s*\))?\s*\)`)

// The three below are not differences: the copy REFUSES each of them, so
// without the veto the statement is sent there, fails, and MySQL answers
// after the failed attempt (37 to 55 ms measured against 0.2 ms, #2114).
// Kept on MySQL from the text, the attempt is not made. A statement one of
// them misses is still answered by MySQL, the slow way.

// limitComma is LIMIT with the offset first and a comma (`LIMIT 0, 20`,
// what SQLAlchemy sends), with numbers or placeholders: a syntax error on
// the copy, which only reads `LIMIT 20 OFFSET 0`. A comma that follows a
// subquery's LIMIT comes after its closing parenthesis and is not matched.
var limitComma = regexp.MustCompile(`(?i)\blimit\s+(?:\d+|\?)\s*,`)

// orderByNull is ORDER BY with the constant NULL as its first key (what
// Django sends after GROUP BY, to ask for no sort): the copy refuses to
// sort by a constant that is not a column position. NULL further down the
// list (`ORDER BY a, NULL`) is refused there too and not matched here.
//
// The first group is what tells a window apart: `OVER (ORDER BY NULL)` and
// `OVER (PARTITION BY a ORDER BY NULL)` are answered by the copy, with the
// same rows, and are not kept back. A window's ORDER BY comes right after
// the opening parenthesis or after its PARTITION BY list; a statement's or
// a subquery's never does. A PARTITION BY list with parentheses in it is
// not followed, and that window stays on MySQL.
var orderByNull = regexp.MustCompile(`(?i)(\(\s*|\bpartition\s+by\b[^()]*)?\border\s+by\s+(?:\(\s*)*null\b`)

func orderByNullKey(t *shapeText) bool {
	for _, m := range orderByNull.FindAllStringSubmatchIndex(t.all(), -1) {
		if m[2] < 0 {
			return true
		}
	}
	return false
}

// binaryIntroducer is the _binary that MySQL reads before a string literal
// (`= _binary'x'`, how drivers write a bytes argument): the copy takes it
// for a type it does not have.
var binaryIntroducer = regexp.MustCompile(`(?i)\b_binary\b`)

// quotedName is a name in the text the pattern list reads: scan has written
// every one in double quotes, refused a name that holds a double quote, and
// blanked the string literals, so each pair of double quotes is one name.
var quotedName = regexp.MustCompile(`"[^"]*"`)

// shapeVeto returns the first of shapeVetoes the statement matches, or "".
func shapeVeto(blankedCopy string) string {
	why, _ := shapeVetoWork(blankedCopy)
	return why
}

// shapeVetoWork is shapeVeto with the work it took: how many bytes and
// groups the checks looked at, counted by shapeText.
func shapeVetoWork(blankedCopy string) (why string, steps int) {
	t := &shapeText{s: quotedName.ReplaceAllString(blankedCopy, `""`)}
	for _, v := range shapeVetoes {
		if v.hit(t) {
			return v.name, t.steps
		}
	}
	return "", t.steps
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
func digitWord(t *shapeText) bool {
	n := t.len()
	// numberEnd is where the last word made of digits alone ended: a dot
	// right there is a decimal point.
	numberEnd := -1
	for i := 0; i < n; {
		if !wordByte(t.at(i)) {
			i++
			continue
		}
		j := i
		for j < n && wordByte(t.at(j)) {
			j++
		}
		word, start := t.sub(i, j), i
		afterDot := start > 0 && t.at(start-1) == '.'
		i = j
		if !asciiDigit(word[0]) {
			// `1.e2` is a number; `1.e` and `1.ex` are not one on MySQL.
			if afterDot && start-1 == numberEnd && (word[0] == 'e' || word[0] == 'E') && !exponent(t, word, 0, j) {
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
			if !exponent(t, word, d, j) {
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
// sign and a digit follow (1e+5, 1e-5). end is where the word ends in t.
func exponent(t *shapeText, word string, e, end int) bool {
	if e+1 < len(word) {
		return asciiDigit(word[e+1])
	}
	return end+1 < t.len() && (t.at(end) == '+' || t.at(end) == '-') && asciiDigit(t.at(end+1))
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

// intervalExpr is INTERVAL with its amount in parentheses, or with a
// placeholder for it. In parentheses the amount is an expression: MySQL
// rounds it and the copy cuts it, so `d + INTERVAL (1.5) DAY` is two days
// later on MySQL and one on the copy, and INTERVAL ('1:30') MINUTE_SECOND
// has the difference of intervalUnit. A placeholder is no better: a text
// bound to it is rounded by MySQL 8.4 as a number ('1.5' is two days) and
// reaches the copy as the quoted '1.5', which is one. A statement that
// calls the function INTERVAL(n, n1, ...) is matched too.
var intervalExpr = regexp.MustCompile(`(?i)\binterval\s*[(?]`)

// intervalUnit is INTERVAL with a quoted amount and a unit of two parts:
// MINUTE_SECOND, DAY_HOUR, YEAR_MONTH. The copy has no such units. It reads
// INTERVAL '90' by its own rules and takes the unit for the column's alias.
// With a bare number for the amount (INTERVAL 1 DAY_HOUR) the copy refuses
// the statement. An amount that is not a whole number ('1:30') is refused
// before this, by scan (vetoIntervalAmount).
var intervalUnit = regexp.MustCompile(`(?i)\binterval\s*''\s*[a-z]+_[a-z]+`)

var (
	// A typed literal: DATE '2026-01-01', TIMESTAMP '2026-01-01 10:00:00',
	// TIME '10:00:00'.
	temporalLiteral = regexp.MustCompile(`(?i)\b(?:date|timestamp|time)\s*''`)
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
	left       int // where the shape starts for a sign before it: start, or the first qualifier of a column's name
	paren      int // DATE( and CAST(: where the parenthesis opens; else -1
	cast       bool
	call       int // the group of that parenthesis
	in         int // the innermost group the shape is in, or -1
	word       bool
}

// dateArithmetic reports a + or a - written next to something the text
// itself says is a date, a time or a date and time: a typed literal (DATE
// '...', TIMESTAMP '...', TIME '...'), DATE(...), or CAST(... AS DATE),
// CAST(... AS DATETIME) and CAST(... AS TIME). MySQL turns the date into the
// number its digits spell (DATE '2026-01-01' + 1 is 20260102, and one date
// minus another is the difference of two such numbers); the copy answers a
// date (2026-01-02), a count of days or an interval.
//
// The date may sit inside something: GREATEST(DATE '...', d) + 1,
// (SELECT DATE(ts) FROM t) + 1, CASE WHEN a THEN DATE(ts) END + 1 and
// MAX(DATE(ts)) OVER () - 1 are numbers on MySQL and dates on the copy as
// well. So every group the date is in, each pair of parentheses and each
// CASE ... END, out to the whole statement, is looked at too: a + or - next
// to the group (before the name of the function, for a call) is reported,
// and so is a group that AVG is called on (AVG of a date or a time is a
// number on MySQL and a date and time, or a time, on the copy). A + or -
// that INTERVAL follows does not count: that is the same day on both
// (selected, MySQL shows a date and the copy a date and time: a difference
// listed in the docs, and one the text cannot show for a column either).
//
// TIME is a shape for AVG's sake. The copy refuses + and - on a time, so
// reporting them keeps nothing back that it would have answered.
//
// It is the text's shape and no more. What the group returns is not known,
// so SUM(IF(d >= DATE '...', amount, 0)) - 1 and YEAR(DATE '...') + 1 are
// kept on MySQL too, for nothing. And a column, or an expression of one
// (created_on + 1, max(d) - min(d)), is not seen: nothing in the text or in
// the plan says what type it has. ColumnVeto is the same rule for a column,
// asked by the side that knows the table's column types (#2133).
func dateArithmetic(t *shapeText) bool {
	shapes := temporalShapes(t)
	if len(shapes) == 0 {
		return false
	}
	return shapesInArithmetic(t, shapes)
}

// shapesInArithmetic reads the statement's groups and reports whether one of
// the shapes, or a group that holds one, stands next to a + or a - that
// INTERVAL does not follow, or under AVG. shapes are in the order written.
func shapesInArithmetic(t *shapeText, shapes []temporal) bool {
	readGroups(t, shapes)
	seen := make([]bool, len(t.groups))
	for _, sh := range shapes {
		if !sh.word {
			continue // $date( and the like: another name
		}
		end := sh.end
		if sh.paren >= 0 {
			g := t.group(sh.call)
			if g.end < 0 || sh.cast && !castToTemporal(t, sh.paren, g.end-1) {
				continue
			}
			end = g.end
		}
		if signBefore(t, sh.left) || signAfter(t, end) {
			return true
		}
		for gi := sh.in; gi >= 0 && !seen[gi]; gi = t.group(gi).parent {
			seen[gi] = true
			if groupInArithmetic(t, gi) {
				return true
			}
		}
	}
	return false
}

// temporalShapes lists the typed literals and the DATE( and CAST( openings,
// in the order they are written.
func temporalShapes(t *shapeText) []temporal {
	lits := temporalLiteral.FindAllStringIndex(t.all(), -1)
	calls := temporalCall.FindAllStringSubmatchIndex(t.all(), -1)
	shapes := make([]temporal, 0, len(lits)+len(calls))
	for len(lits) > 0 || len(calls) > 0 {
		if len(calls) == 0 || len(lits) > 0 && lits[0][0] < calls[0][0] {
			shapes = append(shapes, temporal{start: lits[0][0], left: lits[0][0], end: lits[0][1], paren: -1, in: -1})
			lits = lits[1:]
			continue
		}
		m := calls[0]
		c := t.at(m[2])
		shapes = append(shapes, temporal{start: m[0], left: m[0], paren: m[1] - 1, cast: c == 'c' || c == 'C', in: -1})
		calls = calls[1:]
	}
	return shapes
}

// readGroups reads the statement once and fills t.groups, in the order the
// groups open. On the way it fills in, for each shape, the group it is in,
// the group of its own parenthesis, and whether it starts a word.
func readGroups(t *shapeText, shapes []temporal) {
	top := -1      // the innermost open group
	si, pi := 0, 0 // the next shape to place, and the next to find the parenthesis of
	n := t.len()
	for i := 0; i < n; {
		c := t.at(i)
		switch {
		case c == '(':
			t.groups = append(t.groups, group{open: i, end: -1, parent: top})
			top = len(t.groups) - 1
			for pi < len(shapes) && shapes[pi].paren < i {
				pi++
			}
			if pi < len(shapes) && shapes[pi].paren == i {
				shapes[pi].call = top
				pi++
			}
			i++
		case c == ')':
			// A CASE left open inside these parentheses never closes.
			for top >= 0 && t.group(top).isCase {
				top = t.group(top).parent
			}
			if top >= 0 {
				t.group(top).end = i + 1
				top = t.group(top).parent
			}
			i++
		case wordByte(c):
			j := i
			for j < n && wordByte(t.at(j)) {
				j++
			}
			for si < len(shapes) && shapes[si].start < j {
				shapes[si].in, shapes[si].word = top, shapes[si].start == i
				si++
			}
			if i == 0 || t.at(i-1) != '.' {
				switch {
				case j-i == 4 && strings.EqualFold(t.sub(i, j), "case"):
					t.groups = append(t.groups, group{open: i, end: -1, parent: top, isCase: true})
					top = len(t.groups) - 1
				case j-i == 3 && top >= 0 && t.group(top).isCase && strings.EqualFold(t.sub(i, j), "end"):
					t.group(top).end = j
					top = t.group(top).parent
				}
			}
			i = j
		default:
			i++
		}
	}
}

// groupInArithmetic reports a + or - next to the group, or AVG called on it.
// For a call the left side is read before the function's name, and a window
// (OVER (...) or OVER name) after the call is stepped over.
func groupInArithmetic(t *shapeText, gi int) bool {
	g := *t.group(gi)
	if g.end < 0 {
		return false
	}
	left, right := g.open, g.end
	if !g.isCase {
		l := left - 1
		for l >= 0 && sqlSpace(t.at(l)) {
			l--
		}
		w := l
		for w >= 0 && wordByte(t.at(w)) {
			w--
		}
		if w < l {
			if strings.EqualFold(t.sub(w+1, l+1), "avg") {
				return true
			}
			left = w + 1
		}
		right = afterWindow(t, right)
	}
	return signBefore(t, left) || signAfter(t, right)
}

// afterWindow returns where the call that ends at pos ends once its window
// is counted: past OVER (...) or OVER name when one follows, else pos.
func afterWindow(t *shapeText, pos int) int {
	n := t.len()
	r := pos
	for r < n && sqlSpace(t.at(r)) {
		r++
	}
	if r+4 > n || !strings.EqualFold(t.sub(r, r+4), "over") || r+4 < n && wordByte(t.at(r+4)) {
		return pos
	}
	r += 4
	for r < n && sqlSpace(t.at(r)) {
		r++
	}
	if r < n && t.at(r) == '(' {
		// Groups are in the order they open.
		k := sort.Search(len(t.groups), func(k int) bool { return t.group(k).open >= r })
		if k == len(t.groups) || t.group(k).open != r || t.group(k).end < 0 {
			return pos
		}
		return t.group(k).end
	}
	for r < n && wordByte(t.at(r)) {
		r++
	}
	return r
}

// signBefore reports a + or - as the last thing before pos, white space
// aside.
func signBefore(t *shapeText, pos int) bool {
	l := pos - 1
	for l >= 0 && sqlSpace(t.at(l)) {
		l--
	}
	return l >= 0 && (t.at(l) == '+' || t.at(l) == '-')
}

// signAfter reports a + or - as the first thing from pos on, white space
// aside, unless INTERVAL follows it.
func signAfter(t *shapeText, pos int) bool {
	n := t.len()
	r := pos
	for r < n && sqlSpace(t.at(r)) {
		r++
	}
	if r >= n || t.at(r) != '+' && t.at(r) != '-' {
		return false
	}
	r++
	for r < n && sqlSpace(t.at(r)) {
		r++
	}
	const interval = "interval"
	if r+len(interval) > n || !strings.EqualFold(t.sub(r, r+len(interval)), interval) {
		return true
	}
	return r+len(interval) < n && wordByte(t.at(r+len(interval)))
}

// castToTemporal reports whether the CAST whose parentheses are at open and
// closing ends in AS DATE, AS DATETIME or AS TIME, with or without a
// precision. It reads backwards from the closing parenthesis and no further
// than the type.
func castToTemporal(t *shapeText, open, closing int) bool {
	p := closing - 1
	back := func(skip func(byte) bool) {
		for p > open && skip(t.at(p)) {
			p--
		}
	}
	back(sqlSpace)
	if t.at(p) == ')' {
		p--
		back(func(c byte) bool { return sqlSpace(c) || asciiDigit(c) })
		if t.at(p) != '(' {
			return false
		}
		p--
		back(sqlSpace)
	}
	e := p
	back(wordByte)
	if typ := t.sub(p+1, e+1); !strings.EqualFold(typ, "date") && !strings.EqualFold(typ, "datetime") && !strings.EqualFold(typ, "time") {
		return false
	}
	back(sqlSpace)
	e = p
	back(wordByte)
	return strings.EqualFold(t.sub(p+1, e+1), "as")
}

func sqlSpace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == '\v'
}
