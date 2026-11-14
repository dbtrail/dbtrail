package readrouter

import (
	"regexp"
	"strings"
	"unicode/utf8"
)

// unsignedDiffers is what arithmetic on an UNSIGNED whole number does on each
// side (#2848), measured through the MySQL port on MySQL 8.0.46.
const unsignedDiffers = "MySQL computes it as a BIGINT UNSIGNED and refuses a negative result with error 1690; " +
	"the copy answers another type and, for a negative result, a number"

// vetoWindowNumber keeps on MySQL a statement that does arithmetic on
// ROW_NUMBER, RANK, DENSE_RANK or NTILE (#2848). MySQL types each as a BIGINT
// UNSIGNED, so ROW_NUMBER() OVER (ORDER BY k) - 10 is refused there with
// error 1690 from the first row; the copy types it as a signed BIGINT and
// answers -9.
const vetoWindowNumber = "ROW_NUMBER, RANK, DENSE_RANK or NTILE in arithmetic (a BIGINT UNSIGNED on MySQL, " +
	"which refuses a negative result with error 1690; a signed BIGINT on the copy)"

// windowNumberCall is a call of a window function MySQL types as a BIGINT
// UNSIGNED, with the white space MySQL takes before the parenthesis.
var windowNumberCall = regexp.MustCompile(`(?i)\b(?:row_number|rank|dense_rank|ntile)` + callOpen)

// notUnsignedCalls are the calls whose result is not an unsigned whole number
// whatever they are given, so arithmetic next to one is not arithmetic on
// the UNSIGNED value inside: SUM is a DECIMAL and COUNT a signed BIGINT on
// both sides. Measured: SUM(ui) - 100 and COUNT(ui) - 100 over an INT
// UNSIGNED column, the same type and value on MySQL and on the copy.
var notUnsignedCalls = map[string]bool{"sum": true, "count": true}

// UnsignedVeto says why the copy must not answer a statement because it does
// arithmetic on an UNSIGNED whole-number column, or "" when it can (#2848).
// shape is Shape of the statement as the client sent it (of the template,
// for a prepared statement); unsigned are the UNSIGNED TINYINT, SMALLINT,
// MEDIUMINT, INT and BIGINT columns of the tables the statement reads, all
// tables together, by name; star says the statement holds a star that stands
// for columns, when the caller knows from a parse of it.
//
// MySQL computes +, -, *, % and MOD with an UNSIGNED operand as a BIGINT
// UNSIGNED: b - u over a BIGINT and a BIGINT UNSIGNED is one, and refused
// with error 1690 where a row's result is negative. The copy holds TINYINT to
// INT UNSIGNED as signed integers and a BIGINT UNSIGNED as a UBIGINT: b - u
// is a 128-bit integer there (sent as a DECIMAL), ui - 4 a signed BIGINT that
// answers -1, and -u wraps around to 18446744073709551613 where MySQL answers
// -3. So the statement is kept on MySQL when one of these columns, or a group
// that holds it (a pair of parentheses, a call with its window, a CASE ...
// END), stands next to +, -, *, /, %, <<, MOD or DIV, on either side. A group
// that is a call of SUM or COUNT ends the walk (notUnsignedCalls).
// Comparisons are not arithmetic: u > -1, u BETWEEN -5 AND 5 and u > b were
// measured equal on both sides.
//
// The names are looked for as ColumnVeto looks for a date, and the same two
// statements are kept on MySQL because the column could stand in the
// arithmetic under another name: one with a subquery, a derived table or a
// WITH and arithmetic anywhere, when it names one of the columns or has a
// star (SELECT x - 1 FROM (SELECT u AS x FROM t) q); and one that names one
// of the columns and has arithmetic at or after its first GROUP BY, HAVING
// or ORDER BY, where MySQL takes a select-list alias for its expression
// (SELECT ui AS x ... ORDER BY x - 4). Another table's column of the same
// name, an alias or a function called the same costs a statement the copy
// could have answered, never a wrong answer.
//
// Three statements are refused without being searched, because the search
// could miss: one whose shape was not handed over, one that is not valid
// UTF-8, and any over such a column whose own name is not made of word
// characters alone.
func UnsignedVeto(shape string, unsigned []string, star bool) string {
	why, _ := unsignedVetoWork(shape, unsigned, star)
	return why
}

// UnsignedVeto is UnsignedVeto over this shape.
func (s StatementShape) UnsignedVeto(unsigned []string, star bool) string {
	return UnsignedVeto(string(s), unsigned, star)
}

// unsignedVetoWork is UnsignedVeto with the work it took, counted by
// shapeText.
func unsignedVetoWork(shape string, unsigned []string, star bool) (why string, steps int) {
	if len(unsigned) == 0 {
		return "", 0
	}
	kinds := make(map[string]byte, len(unsigned))
	for _, name := range unsigned {
		if !utf8.ValidString(name) || !wordName(name) {
			return "the column " + name + " is an UNSIGNED whole number, and that name cannot be looked for in a statement", 0
		}
		kinds[sameName(name)] = 1
	}
	if shape == "" {
		return "the statement could not be searched for arithmetic on its UNSIGNED columns", 0
	}
	if !utf8.ValidString(shape) {
		return "the statement is not valid UTF-8, so it cannot be searched for arithmetic on UNSIGNED columns", len(shape)
	}
	t := &shapeText{s: namesKept(shape, kinds)}
	var shapes []temporal
	n := t.len()
	for i := 0; i < n; {
		if !wordByte(t.at(i)) {
			i++
			continue
		}
		j := i
		for j < n && wordByte(t.at(j)) {
			j++
		}
		word := sameName(t.sub(i, j))
		if kinds[word] == 0 {
			// A quoted name is written with a $ before it (namesKept).
			word = strings.TrimPrefix(word, "$")
		}
		if kinds[word] != 0 {
			shapes = append(shapes, temporal{start: i, left: qualifiedStart(t, i), end: j, paren: -1, in: -1})
		}
		i = j
	}
	text := t.all()
	star = star || starItem.MatchString(text)
	derived := subquery.MatchString(text)
	if len(shapes) == 0 && !(derived && star) {
		return "", t.steps
	}
	readGroups(t, shapes)
	closes := groupCloses(t)
	if derived && mathFrom(t, closes, 0) {
		return "the statement reads a table with an UNSIGNED column (" + unsigned[0] + "), holds a subquery or a WITH, and has arithmetic in it: " +
			"the column could stand there under another name (" + unsignedDiffers + ")", t.steps
	}
	if len(shapes) == 0 {
		return "", t.steps
	}
	name := sameName(strings.TrimPrefix(lastWord(t, shapes[0].end), "$"))
	if loc := aliasClause.FindStringIndex(text); loc != nil && mathFrom(t, closes, loc[1]) {
		return "the statement names the UNSIGNED column " + name + " and has arithmetic in its GROUP BY, HAVING or ORDER BY, " +
			"where an alias of the column could stand (" + unsignedDiffers + ")", t.steps
	}
	if shapesInMath(t, shapes) {
		return "the UNSIGNED column " + name + ", or another UNSIGNED column of the tables read, stands in arithmetic (" + unsignedDiffers + ")", t.steps
	}
	return "", t.steps
}

// windowNumberVeto is vetoWindowNumber when blanked (the statement as the
// vetoes read it) does arithmetic on a call of ROW_NUMBER, RANK, DENSE_RANK
// or NTILE, or "". The rule is UnsignedVeto's with the call for the column:
// next to the arithmetic, in a group next to it, or anywhere in a statement
// with a subquery, a derived table or a WITH, or after its first GROUP BY,
// HAVING or ORDER BY, where an alias can stand for it. A call alone is
// answered by the copy: the value is the same, typed BIGINT where MySQL types
// it BIGINT UNSIGNED.
func windowNumberVeto(blanked string) string {
	why, _ := windowNumberVetoWork(blanked)
	return why
}

// windowNumberVetoWork is windowNumberVeto with the work it took.
func windowNumberVetoWork(blanked string) (why string, steps int) {
	if !windowNumberCall.MatchString(blanked) {
		return "", len(blanked)
	}
	// Every quoted name emptied: what is inside a name is not syntax.
	t := &shapeText{s: namesKept(blanked, nil)}
	text := t.all()
	var shapes []temporal
	for _, m := range windowNumberCall.FindAllStringIndex(text, -1) {
		shapes = append(shapes, temporal{start: m[0], left: m[0], paren: m[1] - 1, in: -1})
	}
	if len(shapes) == 0 {
		return "", t.steps
	}
	readGroups(t, shapes)
	closes := groupCloses(t)
	if subquery.MatchString(text) && mathFrom(t, closes, 0) {
		return vetoWindowNumber, t.steps
	}
	if loc := aliasClause.FindStringIndex(text); loc != nil && mathFrom(t, closes, loc[1]) {
		return vetoWindowNumber, t.steps
	}
	if shapesInMath(t, shapes) {
		return vetoWindowNumber, t.steps
	}
	return "", t.steps
}

// groupCloses maps where each pair of parentheses ends to its group, as
// dateSign reads it.
func groupCloses(t *shapeText) map[int]int {
	closes := make(map[int]int, len(t.groups))
	for gi := range t.groups {
		if g := t.group(gi); g.end >= 0 && !g.isCase {
			closes[g.end] = gi
		}
	}
	return closes
}

// shapesInMath reports whether one of the shapes, or a group that holds one,
// stands next to arithmetic (mathBefore, mathAfter), or is an argument of
// MOD. A shape with a parenthesis (a call) is read as its call with the
// window after it. The walk out stops at a call of a function in
// notUnsignedCalls. readGroups has run.
func shapesInMath(t *shapeText, shapes []temporal) bool {
	seen := make([]bool, len(t.groups))
	for _, sh := range shapes {
		if !sh.word {
			continue // $rank( and the like: another name
		}
		end := sh.end
		if sh.paren >= 0 {
			g := t.group(sh.call)
			if g.end < 0 {
				continue // a call left open: a statement both sides refuse
			}
			end = afterWindow(t, g.end)
		}
		if mathBefore(t, sh.left) || mathAfter(t, end) {
			return true
		}
		for gi := sh.in; gi >= 0 && !seen[gi]; gi = t.group(gi).parent {
			seen[gi] = true
			f := groupFunc(t, gi)
			if notUnsignedCalls[f] {
				break
			}
			if f == "mod" || groupInMath(t, gi) {
				return true
			}
		}
	}
	return false
}

// groupInMath reports arithmetic next to the group: for a call, before the
// function's name and after its window (OVER (...) or OVER name).
func groupInMath(t *shapeText, gi int) bool {
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
			left = w + 1
		}
		right = afterWindow(t, right)
	}
	return mathBefore(t, left) || mathAfter(t, right)
}

// mathOperator reports a byte that is an arithmetic operator wherever it
// stands next to a value: + - * / %. A star next to a value is a product.
func mathOperator(c byte) bool { return strings.IndexByte("+-*/%", c) >= 0 }

// mathWord reports MOD or DIV.
func mathWord(w string) bool { return strings.EqualFold(w, "mod") || strings.EqualFold(w, "div") }

// mathBefore reports an arithmetic operator, a <<, MOD or DIV as the last
// thing before pos, white space aside. << is here because MySQL shifts an
// UNSIGNED value as a BIGINT UNSIGNED and the copy as a signed number; the
// other bit operators are kept on MySQL from the text (bitOperator).
func mathBefore(t *shapeText, pos int) bool {
	l := pos - 1
	for l >= 0 && sqlSpace(t.at(l)) {
		l--
	}
	if l < 0 {
		return false
	}
	if c := t.at(l); !wordByte(c) {
		return mathOperator(c) || c == '<' && l > 0 && t.at(l-1) == '<'
	}
	w := l
	for w >= 0 && wordByte(t.at(w)) {
		w--
	}
	return l-w == 3 && mathWord(t.sub(w+1, l+1)) && (w < 0 || t.at(w) != '.')
}

// mathAfter reports an arithmetic operator, a <<, MOD or DIV as the first
// thing from pos on, white space aside. An INTERVAL after a + or a - counts: an
// UNSIGNED number stepped by one is not a date on the copy either.
func mathAfter(t *shapeText, pos int) bool {
	n := t.len()
	r := pos
	for r < n && sqlSpace(t.at(r)) {
		r++
	}
	if r >= n {
		return false
	}
	if c := t.at(r); !wordByte(c) {
		return mathOperator(c) || c == '<' && r+1 < n && t.at(r+1) == '<'
	}
	e := r
	for e < n && wordByte(t.at(e)) {
		e++
	}
	return e-r == 3 && mathWord(t.sub(r, e))
}

// mathFrom reports arithmetic at or after from that could be on a value the
// text does not name: a + or a - that is not a number's sign (dateSign), a
// * that is not a star (starAt), a /, a %, a <<, or the word MOD or DIV.
func mathFrom(t *shapeText, closes map[int]int, from int) bool {
	n := t.len()
	for i := from; i < n; i++ {
		c := t.at(i)
		switch {
		case c == '+' || c == '-':
			if dateSign(t, closes, i) {
				return true
			}
		case c == '*':
			if !starAt(t, i) {
				return true
			}
		case c == '/' || c == '%' || c == '<' && i+1 < n && t.at(i+1) == '<':
			return true
		case wordByte(c):
			j := i
			for j < n && wordByte(t.at(j)) {
				j++
			}
			if j-i == 3 && mathWord(t.sub(i, j)) && (i == 0 || t.at(i-1) != '.') {
				return true
			}
			i = j - 1
		}
	}
	return false
}

// starAt reports whether the * at i stands for columns (SELECT *, t.*, a
// list's next item) or is COUNT(*)'s, and not a product: what stands before
// it is nothing, a parenthesis, a comma, a dot, or SELECT, ALL, DISTINCT or
// DISTINCTROW.
func starAt(t *shapeText, i int) bool {
	l := i - 1
	for l >= 0 && sqlSpace(t.at(l)) {
		l--
	}
	if l < 0 {
		return true
	}
	if c := t.at(l); !wordByte(c) {
		return c == '(' || c == ',' || c == '.'
	}
	w := l
	for w >= 0 && wordByte(t.at(w)) {
		w--
	}
	switch strings.ToLower(t.sub(w+1, l+1)) {
	case "select", "all", "distinct", "distinctrow":
		return w < 0 || t.at(w) != '.'
	}
	return false
}

// UnsignedArgumentVeto says why the copy must not answer a prepared
// statement whose arguments include one bound as an unsigned integer, or ""
// when no placeholder of stmt (the template) stands in arithmetic (#2848).
// MySQL types such an argument as a BIGINT UNSIGNED: k - ? with 10 bound
// unsigned, over a k of 3, is refused there with error 1690, and the copy,
// sent the number 10, answers -7. The placeholders are looked for as
// UnsignedVeto looks for a column, with the same rules for groups, aliases
// and subqueries.
func UnsignedArgumentVeto(stmt string) string {
	// A placeholder is written as a word of its own, $, which no name the
	// statement holds is: a quoted name is emptied, and an unquoted $ alone
	// is not a name MySQL takes.
	shape := strings.ReplaceAll(Shape(stmt), "?", " $ ")
	if why, _ := unsignedVetoWork(shape, []string{"$"}, false); why != "" {
		return "an argument is bound as an unsigned integer and a placeholder may stand in arithmetic " +
			"(MySQL computes it as a BIGINT UNSIGNED and refuses a negative result with error 1690; the copy is sent a plain number)"
	}
	return ""
}
