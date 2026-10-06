package readrouter

import (
	"regexp"
	"strings"
	"unicode/utf8"
)

// Shape is the statement as the shape checks read it: comments removed,
// string literals blanked, every name in double quotes. A caller that asks
// ColumnVeto about a statement hands it this text, so that the columns are
// looked for in the same reading of the statement the rest of the veto is
// built on.
func Shape(stmt string) string { return scan(stmt).blankedCopy }

// StatementShape is a statement's Shape, kept to be asked about the columns
// of the tables the statement reads once those are known
// (sqlsandbox.Session.Types).
type StatementShape string

// ShapeOf is Shape as a StatementShape.
func ShapeOf(stmt string) StatementShape { return StatementShape(Shape(stmt)) }

// ColumnVeto is ColumnVeto over this shape.
func (s StatementShape) ColumnVeto(dates, whole []string, star bool) string {
	return ColumnVeto(string(s), dates, whole, star)
}

// ColumnVeto says why the copy must not answer a statement because of the
// TYPE of a column it names, or "" when no column's type is in the way
// (#2133). shape is Shape of the statement as the client sent it (of the
// template, for a prepared statement: a placeholder is then read as a
// number could be). The two lists are the columns of the tables the
// statement reads, by name, all tables together so that the statement is
// read once:
//
//   - dates are their DATE, DATETIME and TIMESTAMP columns (and any column
//     whose type the caller could not tell). MySQL and MariaDB turn one into
//     the number its digits spell wherever a number is asked for
//     (created_on + 1 is 20260102, one date minus another the difference of
//     two such numbers, AVG(created_on) their mean), and the copy answers a
//     date, a count of days, an interval or a date and time. Everything else
//     that takes a number (*, /, %, SUM, ABS, a comparison with a number) the
//     copy refuses, and the source answers. So the statement is kept on the
//     source when one of these names, or a group that holds it, stands next
//     to a + or a - that INTERVAL does not follow, or under AVG or MEDIAN:
//     the rule dateArithmetic applies to a date the text spells, with the
//     column's name for the shape. A call of a function in numericOfDate
//     ends the group walk: YEAR(created_on) + 1 is a number plus one on
//     both sides.
//   - whole are their TIME and YEAR columns, which the copy holds under
//     another type: a TIME as text, so tm >= '9:00:00' compares letters, and
//     a YEAR as a number, so yr = 26 is not the year 2026. A statement that
//     names one at all is kept on the source, and so is one with a star,
//     which reaches the column without its name.
//
// star says the statement holds a star that stands for columns, when the
// caller knows from a parse of it; the text is searched for one as well.
//
// The names are looked for as whole words, whether or not they are quoted,
// so t.created_on, `created_on` and db.t.created_on are all found; so is
// another table's column of the same name, a table or an alias called that,
// and a function (a YEAR column named year keeps every statement that calls
// YEAR() off the copy). That costs a statement the copy could have answered
// and never a wrong answer. Two names are the same when they hold the same
// letters up to case, as MySQL compares the names of columns: no accent is
// dropped (año is not ano), and case is folded the way sameName says. A
// word is what MySQL reads as one: letters, digits, _, $ and every
// character outside ASCII.
//
// What the text cannot follow is a date under another name, so two more
// statements are kept on the source:
//
//   - one that holds a subquery, a derived table or a WITH, and a counted +
//     or - (dateSign), or an AVG or a MEDIAN, anywhere, when it names one of
//     the dates or has a star that could bring one in. An alias given inside
//     (SELECT d + 1 FROM (SELECT created_on AS d FROM t) x) or a column list
//     over a star (FROM (SELECT * FROM t) AS q(a, b)) is the date outside;
//   - one that names one of the dates and has a counted + or - at or after
//     its first GROUP BY, HAVING or ORDER BY: MySQL takes a select-list
//     alias for its expression there (SELECT d AS x, d2 AS y ... HAVING x -
//     y > 5).
//
// Three statements are refused without being searched, because the search
// could miss: one that is not valid UTF-8, any statement over a table with
// such a column whose own name is not made of word characters alone (a
// space, a dot, a quote), and one whose shape was not handed over.
func ColumnVeto(shape string, dates, whole []string, star bool) string {
	why, _ := columnVetoWork(shape, dates, whole, star)
	return why
}

var (
	// subquery is the opening of a subquery, of a derived table written
	// TABLE t or VALUES, or of a WITH.
	subquery = regexp.MustCompile(`(?i)\(\s*(?:select|table|values)\b|\bwith\b`)
	// starItem is a star that stands for columns (SELECT *, SELECT*, t.*, a
	// list's next item) and not count(*) or a product, or the TABLE t that
	// stands for one. A column called table can only be written quoted, and
	// a quoted name is not in the text this reads.
	starItem = regexp.MustCompile(`(?i)\bselect(?:\s+(?:all|distinct))?\s*\*|[,.]\s*\*|\btable\b`)
	// avgCall is AVG or MEDIAN called on anything. MEDIAN(d) OVER () is
	// MariaDB's: 20260118 there, 2026-01-18 00:00:00 on the copy.
	avgCall = regexp.MustCompile(`(?i)\b(?:avg|median)\s*\(`)
	// aliasClause opens a clause where MySQL lets a select-list alias stand
	// for its expression.
	aliasClause = regexp.MustCompile(`(?i)\b(?:having|group\s+by|order\s+by)\b`)
)

// numericOfDate are the functions whose call is a number on both sides
// whatever it is given, a date included. Each was measured on MySQL 8.4.9
// and MariaDB 11.4 against the copy as F(d) + 1 and F(d) - F(d2) over DATE
// and DATETIME columns, with equal answers; SUM is here because the copy
// refuses SUM of a date, so a SUM it answers is a sum of numbers. Not here,
// because they differ or the copy refuses them: DAYOFWEEK, WEEKDAY and
// MICROSECOND (vetoed from the text), TO_DAYS, TO_SECONDS, and everything
// the older vetoes keep back (WEEK, DATEDIFF, UNIX_TIMESTAMP).
var numericOfDate = map[string]bool{
	"year": true, "month": true, "day": true, "dayofmonth": true, "dayofyear": true, "quarter": true,
	"hour": true, "minute": true, "second": true, "extract": true, "count": true, "sum": true,
}

// beforeANumber are the reserved words a signed number can follow: after
// one of them a + or a - before a digit is the number's sign. Reserved, so
// none of them can be an unquoted column or alias where a word stands on its
// own. After a dot it can: MySQL takes any word there for a name (q.limit),
// and dateSign does not read such a word as one of these.
var beforeANumber = map[string]bool{
	"select": true, "where": true, "and": true, "or": true, "not": true, "then": true, "else": true, "when": true,
	"between": true, "in": true, "by": true, "limit": true, "on": true, "having": true, "case": true, "is": true,
	"like": true, "distinct": true, "all": true, "xor": true, "div": true, "mod": true, "values": true, "union": true,
}

// sameName folds a name for comparison the way MySQL compares the names of
// columns: by letters, without regard to case, accents kept. Upper and then
// lower brings the dotless i, the long s and the Kelvin sign onto the ASCII
// letter a server that compares without case may take them for; that can
// only find a name where a stricter fold would not.
func sameName(s string) string {
	for i := 0; i < len(s); i++ {
		if s[i] >= 0x80 {
			return strings.ToLower(strings.ToUpper(s))
		}
	}
	return strings.ToLower(s)
}

// wordName reports whether name is made of word bytes alone, so that it is
// one word wherever a statement writes it.
func wordName(name string) bool {
	for i := 0; i < len(name); i++ {
		if !wordByte(name[i]) {
			return false
		}
	}
	return name != ""
}

// columnVetoWork is ColumnVeto with the work it took, counted by shapeText.
func columnVetoWork(shape string, dates, whole []string, star bool) (why string, steps int) {
	if len(dates) == 0 && len(whole) == 0 {
		return "", 0
	}
	// 1 = a date, 2 = a column named at all.
	kinds := make(map[string]byte, len(dates)+len(whole))
	for kind, names := range [][]string{dates, whole} {
		for _, name := range names {
			if !utf8.ValidString(name) || !wordName(name) {
				return "the column " + name + " is a date, a time or a year, and that name cannot be looked for in a statement", 0
			}
			kinds[sameName(name)] = byte(kind + 1)
		}
	}
	if shape == "" {
		return "the statement could not be searched for its date, time and year columns", 0
	}
	if !utf8.ValidString(shape) {
		return "the statement is not valid UTF-8, so it cannot be searched for date, time and year columns", len(shape)
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
		switch kinds[word] {
		case 1:
			shapes = append(shapes, temporal{start: i, left: qualifiedStart(t, i), end: j, paren: -1, in: -1})
		case 2:
			return "the statement names " + word + ", a TIME or YEAR column, which the copy holds as text or as a plain number", t.steps
		}
		i = j
	}
	text := t.all()
	derived := subquery.MatchString(text)
	star = star || starItem.MatchString(text)
	if len(whole) > 0 && star {
		// A star reaches the column without its name: ORDER BY 2 sorts a
		// TIME as text on the copy, and a column list over the star
		// ((SELECT * FROM t) q(a, b)) gives it another name to compare by.
		return "the statement has a star over a table with a TIME or YEAR column (" + whole[0] + "), which the copy holds as text or as a plain number", t.steps
	}
	if len(dates) == 0 || len(shapes) == 0 && !(derived && star) {
		return "", t.steps
	}
	const arithmetic = " (a number built from the date's digits on MySQL; a date, a count of days or an interval on the copy)"
	readGroups(t, shapes)
	closes := make(map[int]int, len(t.groups))
	for gi := range t.groups {
		if g := t.group(gi); g.end >= 0 && !g.isCase {
			closes[g.end] = gi
		}
	}
	if derived && (dateSignFrom(t, closes, 0) || avgCall.MatchString(text)) {
		// The date can reach the + or the AVG under another name: an alias
		// given inside the subquery, or a column list after it over a star.
		return "the statement reads a table with a date column (" + dates[0] + "), holds a subquery or a WITH, and has a +, a - or an AVG in it: " +
			"the date could stand there under another name" + arithmetic, t.steps
	}
	if len(shapes) == 0 {
		return "", t.steps
	}
	name := sameName(strings.TrimPrefix(lastWord(t, shapes[0].end), "$"))
	if loc := aliasClause.FindStringIndex(text); loc != nil && dateSignFrom(t, closes, loc[1]) {
		// GROUP BY, HAVING and ORDER BY take a select-list alias for its
		// expression on MySQL: SELECT d AS x ... HAVING x - y > 5.
		return "the statement names the date column " + name + " and has a + or a - in its GROUP BY, HAVING or ORDER BY, where an alias of the date could stand" + arithmetic, t.steps
	}
	if groupsInArithmetic(t, shapes, numericOfDate) {
		return "the date column " + name + ", or another date column of the tables read, stands next to + or -, or under AVG" + arithmetic, t.steps
	}
	return "", t.steps
}

// namesKept is the shape with each quoted name emptied, as shapeVetoWork
// reads it, except the names in kinds: those are written as a word, a space
// and a $ in place of the quotes, so that the word scan finds a quoted
// column as it finds an unquoted one. The $ keeps a quoted column called
// end, case, over, avg or interval from being read as that keyword by the
// group and sign checks; an unquoted $created_on is taken for the column
// too, which only keeps a statement back.
func namesKept(shape string, kinds map[string]byte) string {
	var b strings.Builder
	b.Grow(len(shape))
	for i := 0; i < len(shape); {
		if shape[i] != '"' {
			b.WriteByte(shape[i])
			i++
			continue
		}
		j := strings.IndexByte(shape[i+1:], '"')
		if j < 0 {
			b.WriteString(shape[i:])
			break
		}
		name := shape[i+1 : i+1+j]
		if kinds[sameName(name)] != 0 {
			b.WriteString(" $")
			b.WriteString(name)
		} else {
			b.WriteString(`""`)
		}
		i += j + 2
	}
	return b.String()
}

// qualifiedStart is where the name that ends in the word at start begins once
// its qualifiers are counted: t.created_on and ""."".created_on start at the
// first of them, so a + or a - before the qualifier is next to the column.
func qualifiedStart(t *shapeText, start int) int {
	// Two qualifiers at most (schema.table.column): a longer chain is not a
	// name, and following one from each of its words would be work that grows
	// with the square of its length.
	for hops := 0; hops < 2; hops++ {
		p := start - 1
		for p >= 0 && sqlSpace(t.at(p)) {
			p--
		}
		if p < 0 || t.at(p) != '.' {
			return start
		}
		p--
		for p >= 0 && sqlSpace(t.at(p)) {
			p--
		}
		q := p
		for q >= 0 && (wordByte(t.at(q)) || t.at(q) == '"') {
			q--
		}
		if q == p {
			return start
		}
		start = q + 1
	}
	return start
}

// lastWord is the word that ends at end.
func lastWord(t *shapeText, end int) string {
	i := end
	for i > 0 && wordByte(t.at(i-1)) {
		i--
	}
	return t.sub(i, end)
}

// dateSignFrom reports a counted + or - at or after from (dateSign).
func dateSignFrom(t *shapeText, closes map[int]int, from int) bool {
	n := t.len()
	for i := from; i < n; i++ {
		if c := t.at(i); (c == '+' || c == '-') && dateSign(t, closes, i) {
			return true
		}
	}
	return false
}

// dateSign reports whether the + or - at i could be arithmetic on a date
// that stands there under a name the text does not show. It is not when:
//
//   - INTERVAL follows it (signAfter);
//   - it is the sign of a number: a digit follows, and before it stands
//     nothing, an operator, an opening parenthesis, a comma or a reserved
//     word a number can follow (amount > -1, BETWEEN -5 AND 5), or it is in
//     an exponent (1e-5);
//   - both of its operands are numbers whatever the names in them mean: a
//     number, or a call of a function in numericOfDate (SUM(a) - SUM(b),
//     YEAR(x) + 1, COUNT(*) - 1).
//
// closes maps where each pair of parentheses ends to its group.
func dateSign(t *shapeText, closes map[int]int, i int) bool {
	if !signAfter(t, i) {
		return false
	}
	n := t.len()
	r := i + 1
	for r < n && sqlSpace(t.at(r)) {
		r++
	}
	l := i - 1
	for l >= 0 && sqlSpace(t.at(l)) {
		l--
	}
	number := r < n && (asciiDigit(t.at(r)) || t.at(r) == '.')
	if l < 0 {
		return !number
	}
	lc := t.at(l)
	var before string // the word that ends at l, in lower case
	if wordByte(lc) {
		before = strings.ToLower(lastWord(t, l+1))
	}
	// A word after a dot is a name, whatever it spells.
	reserved := beforeANumber[before]
	if reserved {
		p := l - len(before)
		for p >= 0 && sqlSpace(t.at(p)) {
			p--
		}
		reserved = p < 0 || t.at(p) != '.'
	}
	if number {
		switch {
		case strings.IndexByte("(,=<>!*/%+-|&^", lc) >= 0, reserved:
			return false
		case l == i-1 && r == i+1 && len(before) > 1 && asciiDigit(before[0]) && before[len(before)-1] == 'e':
			return false
		}
	}
	rightNumeric := number
	if !rightNumeric && r < n && wordByte(t.at(r)) {
		e := r
		for e < n && wordByte(t.at(e)) {
			e++
		}
		f := strings.ToLower(t.sub(r, e))
		for e < n && sqlSpace(t.at(e)) {
			e++
		}
		rightNumeric = e < n && t.at(e) == '(' && numericOfDate[f]
	}
	leftNumeric := before != "" && asciiDigit(before[0])
	if lc == ')' {
		if gi, ok := closes[l+1]; ok {
			leftNumeric = numericOfDate[groupFunc(t, gi)]
		}
	}
	return !(leftNumeric && rightNumeric)
}
