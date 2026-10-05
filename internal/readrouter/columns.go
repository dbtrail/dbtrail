package readrouter

import (
	"regexp"
	"strings"
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
func (s StatementShape) ColumnVeto(dates, whole []string) string {
	return ColumnVeto(string(s), dates, whole)
}

// ColumnVeto says why the copy must not answer a statement because of the
// TYPE of a column it names, or "" when no column's type is in the way
// (#2133). shape is Shape of the statement as the client sent it (of the
// template, for a prepared statement: a placeholder is then read as a
// number could be). The two lists are the columns of ONE table the statement
// reads, by name:
//
//   - dates are its DATE, DATETIME and TIMESTAMP columns (and any column
//     whose type the caller could not tell). MySQL and MariaDB turn one into
//     the number its digits spell wherever a number is asked for
//     (created_on + 1 is 20260102, one date minus another the difference of
//     two such numbers, AVG(created_on) their mean), and the copy answers a
//     date, a count of days, an interval or a date and time. Everything else
//     that takes a number (*, /, %, SUM, ABS, a comparison with a number) the
//     copy refuses, and the source answers. So the statement is kept on the
//     source when one of these names, or a group that holds it, stands next
//     to a + or a - that INTERVAL does not follow, or under AVG: the rule
//     dateArithmetic applies to a date the text spells, with the column's
//     name for the shape.
//   - whole are its TIME and YEAR columns, which the copy holds under
//     another type: a TIME as text, so tm >= '9:00:00' compares letters, and
//     a YEAR as a number, so yr = 26 is not the year 2026. A statement that
//     names one at all is kept on the source.
//
// The names are looked for as whole words, without regard to ASCII case and
// whether or not they are quoted, so t.created_on, `created_on` and
// db.t.created_on are all found; so is another table's column of the same
// name, a table or an alias called that, and a function (a YEAR column named
// year keeps every statement that calls YEAR() off the copy). That costs a
// statement the copy could have answered and never a wrong answer.
//
// What the text cannot follow is a date under another name: an alias of the
// column, or of an expression of it, used from outside the subquery or the
// WITH that gives it (SELECT d + 1 FROM (SELECT created_on AS d FROM t) x).
// So a statement that names one of the dates AND holds a subquery or a WITH
// is kept on the source for any + or - in it that INTERVAL does not follow,
// wherever it stands.
//
// Three statements are refused without being searched, because the search
// could miss: one with a character outside ASCII outside its string literals
// and comments (which letters a server folds onto an ASCII one when it
// compares names depends on the server), any statement over a table with
// such a column whose own name is not plain ASCII letters, digits, _ and $,
// and one whose shape was not handed over.
func ColumnVeto(shape string, dates, whole []string) string {
	why, _ := columnVetoWork(shape, dates, whole)
	return why
}

// plainName is a column name a statement can only spell one way, up to case.
var plainName = regexp.MustCompile(`^[A-Za-z0-9_$]+$`)

// subquery is the opening of a subquery or of a WITH.
var subquery = regexp.MustCompile(`(?i)\(\s*select\b|\bwith\b`)

// columnVetoWork is ColumnVeto with the work it took, counted by shapeText.
func columnVetoWork(shape string, dates, whole []string) (why string, steps int) {
	if len(dates) == 0 && len(whole) == 0 {
		return "", 0
	}
	// 1 = a date, 2 = a column named at all.
	kinds := make(map[string]byte, len(dates)+len(whole))
	for kind, names := range [][]string{dates, whole} {
		for _, name := range names {
			if !plainName.MatchString(name) {
				return "its column " + name + " is a date, a time or a year, and that name cannot be looked for in a statement", 0
			}
			kinds[strings.ToLower(name)] = byte(kind + 1)
		}
	}
	if shape == "" {
		return "the statement could not be searched for its date, time and year columns", 0
	}
	for i := 0; i < len(shape); i++ {
		if shape[i] >= 0x80 {
			return "the statement holds characters outside ASCII outside its strings, so it cannot be searched for the table's date, time and year columns", len(shape)
		}
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
		word := strings.TrimPrefix(t.sub(i, j), "$")
		switch kinds[strings.ToLower(word)] {
		case 1:
			shapes = append(shapes, temporal{start: i, left: qualifiedStart(t, i), end: j, paren: -1, in: -1})
		case 2:
			return "the statement names " + word + ", a TIME or YEAR column, which the copy holds as text or as a plain number", t.steps
		}
		i = j
	}
	if len(shapes) == 0 {
		return "", t.steps
	}
	const arithmetic = " (a number built from the date's digits on MySQL; a date, a count of days or an interval on the copy)"
	name := strings.ToLower(strings.TrimPrefix(lastWord(t, shapes[0].end), "$"))
	if subquery.MatchString(t.all()) && anySign(t) {
		return "the statement names the date column " + name + ", holds a subquery or a WITH, and has a + or a - in it: " +
			"an alias of the date could stand next to it" + arithmetic, t.steps
	}
	if shapesInArithmetic(t, shapes) {
		return "a date column of the table (" + name + " is the first the statement names) stands next to + or -, or under AVG" + arithmetic, t.steps
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
		if kinds[strings.ToLower(name)] != 0 {
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

// anySign reports a + or a - anywhere in the text that INTERVAL does not
// follow.
func anySign(t *shapeText) bool {
	n := t.len()
	for i := 0; i < n; i++ {
		if c := t.at(i); (c == '+' || c == '-') && signAfter(t, i) {
			return true
		}
	}
	return false
}
