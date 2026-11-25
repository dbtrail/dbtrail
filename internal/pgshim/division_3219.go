package pgshim

import (
	"strconv"
	"strings"
)

// pgDiv is the copy's macro for a division written on its own, n / d
// (sqlsandbox pgRoundMacros), of numerics and whole numbers: the exact
// quotient, rounded once to 16 decimals, a numeric as PostgreSQL's is.
// PostgreSQL writes its quotient with 16 significant digits or more, so the
// two are the same number written with other decimals: the same text for a
// quotient from 1 to 9999 (7.5000000000000000), 16 decimals where PostgreSQL
// writes 12 for a larger one and 20 for a smaller one (#3219). A divisor of
// zero is PostgreSQL's error, where the engine answers NULL.
//
// pgAvgN is avg(x) of a numeric or of whole numbers on the same terms: the
// exact sum over the count. pgAvg stays the avg of a float, the engine's.
//
// One macro answers one type, so which of the two an avg is, and whether a
// column over a whole number (amount / 100, qty / 2) is the macro's division
// or the engine's division of whole numbers, is decided by the column's
// type (retypeForCopy), and the screen lets a division or an avg through only
// when it can show from the statement's text that every side is a numeric or
// a whole number (exactClass): the macros refuse nothing by type when the
// statement runs.
const (
	pgDiv  = "_bintrail_pg_div"
	pgAvgN = "_bintrail_pg_avgn"
)

// divLeftStop are the words that end the left side of a division read
// backwards: the side starts after them (SELECT a / b, WHEN x THEN a / b).
var divLeftStop = map[string]bool{
	"select": true, "distinct": true, "all": true, "where": true, "and": true, "or": true, "not": true,
	"when": true, "then": true, "else": true, "on": true, "having": true, "by": true, "between": true,
	"in": true, "like": true, "ilike": true, "returning": true,
}

// divGiveUp are the words a side of a division may not be, or start after:
// each makes the side something this rewrite does not read (the value of a
// CASE, a typed literal, an aggregate's FILTER), or puts the quotient where
// PostgreSQL takes a whole number of rows and rounds it its own way (LIMIT,
// OFFSET). Such a division is left as written, and the screen sends the
// statement to the source as it did before.
var divGiveUp = map[string]bool{
	"case": true, "end": true, "is": true, "null": true, "true": true, "false": true,
	"similar": true, "escape": true, "collate": true, "at": true, "time": true, "zone": true,
	"over": true, "filter": true, "within": true, "interval": true, "any": true, "some": true,
	"exists": true, "array": true, "row": true, "from": true, "as": true, "isnull": true, "notnull": true,
	"cast": true, "limit": true, "offset": true, "rows": true, "range": true, "groups": true,
	"fetch": true, "first": true, "next": true, "for": true, "symmetric": true,
}

// maxDivisions is how many divisions of one statement rewriteDivisions
// reads. A statement with more keeps them all as written, and goes to the
// source as it did before: the rewrite reads the statement's text again
// after each division it writes.
const maxDivisions = 64

// maxDivisionText is the longest statement rewriteDivisions reads: a longer
// one keeps its divisions as written, and goes to the source as before.
const maxDivisionText = 256 << 10

// rewriteDivisions writes each division of q that it can read as the copy's
// macro, _bintrail_pg_div(n, d), and leaves every other one as written: n is
// the product to the left of the slash (PostgreSQL reads a * b / c as
// (a * b) / c, and a / b / c as (a / b) / c), d the one value to its right.
//
// Left as written: a division inside one of the port's own round or floor
// macros (they take the division themselves, or refuse it) or inside a
// round, floor, ceil or trunc left as written (roundsItsValue); one with a
// cast, a parameter, a string, a typed literal, a subquery, an exponent or a
// subscript on a side, at any depth; a CASE just before or after the slash
// (sum(CASE ... END) is read, and the screen decides it by caseClass); one
// whose divisor is an
// aggregate with OVER or FILTER; one in LIMIT or OFFSET; a division more
// than two deep in others (a / b / c / d, a / (b / (c / d)): the macro's text
// holds each side several times over); two whole numbers written in the
// text (7 / 2); and one keepRaw says to leave.
//
// keepRaw is asked about a name over a whole number other than zero written
// in the text (qty / 2, pack * qty / 100: the name is the last token before
// the slash), and given its left side: the form the screen lets the engine
// divide when the name is a column of whole numbers or of floats. Without
// the columns' types every such division is left (nil); with them, one whose
// left side is a numeric is written as the macro (retypeForCopy).
func rewriteDivisions(q string, keepRaw func(left []rtToken) bool) string {
	if len(q) > maxDivisionText {
		return q
	}
	toks, spans, why := lexSQLSpans(q)
	if why != "" || len(spans) != len(toks) {
		return q
	}
	slashes := 0
	for i := range toks {
		if isOpTok(toks, i, "/") {
			slashes++
		}
	}
	if slashes == 0 || slashes > maxDivisions {
		return q
	}
	// nth counts the divisions left as written so far: each one written is
	// gone from the text, so the next to read is again the nth.
	for nth := 0; ; {
		slash, seen := -1, 0
		for i := range toks {
			if isOpTok(toks, i, "/") {
				if seen == nth {
					slash = i
					break
				}
				seen++
			}
		}
		if slash < 0 {
			return q
		}
		l, r, ok := divisionSides(toks, slash, keepRaw)
		if !ok {
			nth++
			continue
		}
		q = q[:spans[l][0]] + pgDiv + "(" + q[spans[l][0]:spans[slash-1][1]] + ", " +
			q[spans[slash+1][0]:spans[r-1][1]] + ")" + q[spans[r-1][1]:]
		toks, spans, why = lexSQLSpans(q)
		if why != "" || len(spans) != len(toks) {
			return q
		}
	}
}

// divisionSides reads the division at toks[slash]: its left side is
// toks[l:slash], its right side toks[slash+1:r]. False when it is one
// rewriteDivisions leaves as written.
func divisionSides(toks []rtToken, slash int, keepRaw func(left []rtToken) bool) (l, r int, ok bool) {
	if inOwnMacro(toks, slash) {
		return 0, 0, false
	}
	l, ok = divisionLeft(toks, slash)
	if !ok {
		return 0, 0, false
	}
	r, ok = divisionRight(toks, slash)
	if !ok {
		return 0, 0, false
	}
	left, right := toks[l:slash], toks[slash+1:r]
	if holdsCast(left) || holdsCast(right) || holdsUnread(left) || holdsUnread(right) || countWord(left, "select")+countWord(right, "select") > 0 {
		return 0, 0, false
	}
	// The macro's text holds each side several times over, so a division
	// inside a division inside a division would be bound some thousand
	// times: this one is written only if it stays within maxDivDepth, with
	// the divisions already written around it and inside its sides.
	if enclosingDivs(toks, slash)+max(divDepth(left), divDepth(right))+1 > maxDivDepth {
		return 0, 0, false
	}
	if wholeLiteral(left) && wholeLiteral(right) {
		return 0, 0, false
	}
	if nameOverWhole(toks, slash, r) && (keepRaw == nil || keepRaw(left)) {
		return 0, 0, false
	}
	return l, r, true
}

// beforeString are the words a string may follow without being the value of
// a typed literal (LIKE 'PR%', THEN 'x').
var beforeString = map[string]bool{
	"like": true, "ilike": true, "then": true, "else": true, "when": true, "and": true, "or": true, "not": true, "between": true,
}

// holdsUnread reports whether toks hold, at any depth, a parameter or a
// typed literal (DATE '2026-01-01', numeric '2.5': a word and then a
// string). A parameter's value is written into the statement as a cast when
// the statement is bound (params.go), and a side with a cast is left as
// written: the statement the screen read, with $1, must be divided the way
// the statement that runs is.
func holdsUnread(toks []rtToken) bool {
	for i, t := range toks {
		switch {
		case t.kind == rtParam, t.kind == rtTyped:
			return true
		case t.kind == rtString && i > 0 && toks[i-1].kind == rtWord && !beforeString[toks[i-1].text]:
			return true
		}
	}
	return false
}

// maxDivDepth is how deep the macro's divisions may stand one inside
// another (a / b / c is two deep: the first is the left side of the second).
const maxDivDepth = 2

// enclosingDivs is how many of the macro's calls hold toks[at] in their
// parentheses.
func enclosingDivs(toks []rtToken, at int) int {
	n, depth := 0, 0
	for j := at - 1; j >= 0; j-- {
		switch {
		case isOpTok(toks, j, ")"):
			depth++
		case isOpTok(toks, j, "("):
			if depth > 0 {
				depth--
				continue
			}
			if j > 0 && toks[j-1].kind == rtWord && toks[j-1].text == pgDiv {
				n++
			}
		}
	}
	return n
}

// divDepth is how deep the macro's calls stand one inside another in toks.
func divDepth(toks []rtToken) int {
	var open []bool
	deepest, depth := 0, 0
	for i := range toks {
		switch {
		case isOpTok(toks, i, "("):
			isDiv := i > 0 && toks[i-1].kind == rtWord && toks[i-1].text == pgDiv
			open = append(open, isDiv)
			if isDiv {
				depth++
				deepest = max(deepest, depth)
			}
		case isOpTok(toks, i, ")") && len(open) > 0:
			if open[len(open)-1] {
				depth--
			}
			open = open[:len(open)-1]
		}
	}
	return deepest
}

func countWord(toks []rtToken, word string) int {
	n := 0
	for _, t := range toks {
		if t.kind == rtWord && t.text == word {
			n++
		}
	}
	return n
}

// roundsItsValue are the functions that cut a value to a whole number or to
// a number of decimals. A quotient that PostgreSQL holds as a numeric and
// the copy as a double precision can fall on the other side of the cut (a
// floor of 2.9999999999999996 where PostgreSQL has 3), which is another
// number and not another decimal: a division inside one of them is left as
// written. round and floor of the shapes the copy answers were written as
// its macros before this pass.
var roundsItsValue = map[string]bool{"round": true, "floor": true, "ceil": true, "ceiling": true, "trunc": true}

// inOwnMacro reports whether toks[at] is inside the parentheses of one of
// the port's macros other than pgDiv and the two avgs, at any depth, or of
// a function that cuts its value (roundsItsValue).
func inOwnMacro(toks []rtToken, at int) bool {
	depth := 0
	for j := at - 1; j >= 0; j-- {
		switch {
		case isOpTok(toks, j, ")"):
			depth++
		case isOpTok(toks, j, "("):
			if depth > 0 {
				depth--
				continue
			}
			if j > 0 && (toks[j-1].kind == rtWord || toks[j-1].kind == rtQuoted) {
				name := strings.ToLower(toks[j-1].text)
				if roundsItsValue[name] || strings.HasPrefix(name, "_bintrail_pg_") && name != pgDiv && name != pgAvg && name != pgAvgN {
					return true
				}
			}
		}
	}
	return false
}

// endsValue reports whether toks[j] can be the last token of a value, so
// that a plus or minus sign after it is an operator between two values and
// not the sign of the next one.
func endsValue(toks []rtToken, j int) bool {
	if j < 0 {
		return false
	}
	t := toks[j]
	switch t.kind {
	case rtNumber, rtString, rtQuoted, rtParam, rtTyped:
		return true
	case rtWord:
		return !divLeftStop[t.text]
	case rtOp:
		return t.text == ")" || t.text == "]"
	}
	return false
}

// divisionLeft is where the left side of the division at toks[slash] starts.
// Read backwards, the side is values joined by *, / and %: a name (with its
// qualifiers), a number, a function call, or what parentheses hold, each
// with a sign or without. It starts after the first token that is neither a
// value where one is due nor one of those operators where one is due:
// SELECT DISTINCT ON (a) b / c divides b, not what the parentheses hold.
func divisionLeft(toks []rtToken, slash int) (int, bool) {
	j, start := slash-1, -1
	for j >= 0 {
		// A value, read backwards from toks[j].
		t := toks[j]
		switch {
		case isOpTok(toks, j, ")"):
			depth := 0
			for ; j >= 0; j-- {
				if isOpTok(toks, j, ")") {
					depth++
				} else if isOpTok(toks, j, "(") {
					if depth--; depth == 0 {
						break
					}
				}
			}
			if j < 0 {
				return 0, false
			}
			// The function it is the call of, if a name stands before it.
			if j > 0 && toks[j-1].kind == rtQuoted {
				j--
			} else if j > 0 && toks[j-1].kind == rtWord && !divLeftStop[toks[j-1].text] {
				if divGiveUp[toks[j-1].text] {
					return 0, false
				}
				j--
			}
		case t.kind == rtNumber, t.kind == rtQuoted:
		case t.kind == rtWord:
			if divGiveUp[t.text] || divLeftStop[t.text] {
				return 0, false
			}
		default:
			// A string, a parameter, a typed literal, a subscript, an operator
			// where a value is due.
			return 0, false
		}
		// Its qualifiers: schema.table.column, pg_catalog.abs(...).
		for j >= 2 && isOpTok(toks, j-1, ".") && (toks[j-2].kind == rtWord || toks[j-2].kind == rtQuoted) {
			j -= 2
		}
		if j >= 1 && isOpTok(toks, j-1, ".") {
			return 0, false
		}
		// Its sign, when what stands before the sign is not a value.
		if j >= 1 && (isOpTok(toks, j-1, "-") || isOpTok(toks, j-1, "+")) && !endsValue(toks, j-2) {
			j--
		}
		start = j
		// What joins it to the value before it.
		if j >= 1 && (isOpTok(toks, j-1, "*") || isOpTok(toks, j-1, "/") || isOpTok(toks, j-1, "%")) {
			j -= 2
			continue
		}
		if j >= 1 && (isOpTok(toks, j-1, "::") || isOpTok(toks, j-1, "^") || isOpTok(toks, j-1, "]")) {
			return 0, false
		}
		if j >= 1 && toks[j-1].kind == rtWord && divGiveUp[toks[j-1].text] {
			return 0, false
		}
		break
	}
	if start < 0 || start >= slash {
		return 0, false
	}
	return start, true
}

// divisionRight is where the right side of the division at toks[slash] ends
// (one past its last token): one value, with a sign or without.
func divisionRight(toks []rtToken, slash int) (int, bool) {
	k := slash + 1
	if isOpTok(toks, k, "-") || isOpTok(toks, k, "+") {
		k++
	}
	if k >= len(toks) {
		return 0, false
	}
	r := 0
	switch t := toks[k]; {
	case isOpTok(toks, k, "("):
		end := closingParen(toks, k)
		if end < 0 {
			return 0, false
		}
		r = end + 1
	case t.kind == rtNumber:
		r = k + 1
	case t.kind == rtWord || t.kind == rtQuoted:
		if t.kind == rtWord && (divLeftStop[t.text] || divGiveUp[t.text]) {
			return 0, false
		}
		r = k + 1
		for isOpTok(toks, r, ".") && r+1 < len(toks) && (toks[r+1].kind == rtWord || toks[r+1].kind == rtQuoted) {
			r += 2
		}
		if isOpTok(toks, r, "(") {
			end := closingParen(toks, r)
			if end < 0 {
				return 0, false
			}
			r = end + 1
		}
	default:
		return 0, false
	}
	// What binds to the value tighter than the division does, or reads it
	// otherwise: a cast, a subscript, an exponent, a typed literal's string
	// (numeric '2.5').
	if isOpTok(toks, r, "::") || isOpTok(toks, r, "[") || isOpTok(toks, r, "^") || isOpTok(toks, r, ".") {
		return 0, false
	}
	if r < len(toks) && (toks[r].kind == rtString || toks[r].kind == rtTyped) {
		return 0, false
	}
	if r < len(toks) && toks[r].kind == rtWord {
		switch toks[r].text {
		case "over", "filter", "within", "at", "collate":
			return 0, false
		}
	}
	return r, true
}

// nameOverWhole reports whether the division at toks[slash], whose right
// side ends at r, is a name over a whole number other than zero written in
// the text: the token before the slash is a name (the screen's own test of
// such a division, route.go).
func nameOverWhole(toks []rtToken, slash, r int) bool {
	if r != slash+2 || slash < 1 {
		return false
	}
	n := toks[slash+1]
	if n.kind != rtNumber || strings.ContainsAny(n.text, ".eE") || strings.Trim(n.text, "0") == "" {
		return false
	}
	p := toks[slash-1]
	return p.kind == rtQuoted || p.kind == rtWord && !divLeftStop[p.text] && !divGiveUp[p.text]
}

// wholeLiteral reports whether toks are one whole number written in the
// text, with a sign or without (7, -2): two of them divide as whole numbers
// on PostgreSQL and on the engine alike, and the division is left as
// written.
func wholeLiteral(toks []rtToken) bool {
	toks = unsigned(toks)
	return len(toks) == 1 && toks[0].kind == rtNumber && !strings.ContainsAny(toks[0].text, ".eE")
}

// unsigned is toks without a leading sign.
func unsigned(toks []rtToken) []rtToken {
	if len(toks) > 1 && (isOpTok(toks, 0, "-") || isOpTok(toks, 0, "+")) {
		return toks[1:]
	}
	return toks
}

// RewriteForCopyTyped is RewriteForCopy followed by what the columns' types
// decide (retypeForCopy): the text a connection that can reach its source
// hands the copy, and the text its screen reads. types is the copy's column
// types by lowercased name (session.columnTypes, CopyScreen); nil, when they
// are not known, leaves RewriteForCopy's text. For internal/pgcompare.
func RewriteForCopyTyped(stmt string, types map[string][]string) string {
	return retypeForCopy(RewriteForCopy(stmt), types)
}

// retypeForCopy writes into text, as RewriteForCopy left it, the two things
// a column's type decides:
//
//   - a numeric over a whole number written in the text (amount / 100,
//     amount * qty / 100) becomes the macro's division; whole numbers, a
//     float, and a name no column has keep the engine's (qty / 2 is
//     PostgreSQL's division of whole numbers there);
//   - avg(x) of numerics and whole numbers becomes pgAvgN; any other stays
//     pgAvg, the avg of a float.
//
// The types here are the columns' own. A name the statement defines itself
// (an alias, a WITH's column) may share a column's name and be something
// else: the screen, which reads those definitions, then finds a side it
// cannot show to be a numeric and sends the statement to the source. Nothing
// written here is answered without the screen's leave.
func retypeForCopy(text string, types map[string][]string) string {
	if types == nil {
		return text
	}
	of := func(name string) []string { return types[name] }
	text = rewriteDivisions(text, func(left []rtToken) bool { return exactClass(left, of) != clsDecimal })
	toks, spans, why := lexSQLSpans(text)
	if why != "" || len(spans) != len(toks) {
		return text
	}
	var b strings.Builder
	last := 0
	for i, t := range toks {
		if t.kind != rtWord || t.text != pgAvg || !isOpTok(toks, i+1, "(") {
			continue
		}
		end := closingParen(toks, i+1)
		if end < 0 || exactClass(toks[i+2:end], of) == clsUnknown {
			continue
		}
		b.WriteString(text[last:spans[i][0]])
		b.WriteString(pgAvgN)
		last = spans[i][1]
	}
	if last == 0 {
		return text
	}
	b.WriteString(text[last:])
	return b.String()
}

// needsTypes reports whether text, as RewriteForCopy left it, holds what
// retypeForCopy decides by a column's type: a division left as written or an
// avg.
func needsTypes(text string) bool {
	toks, _ := lexSQL(text)
	for i, t := range toks {
		if isOpTok(toks, i, "/") || t.kind == rtWord && t.text == pgAvg {
			return true
		}
	}
	return false
}

// holdsDivisionOrAvg reports whether text, as RewriteForCopy left it, holds
// the macro's division or an avg.
func holdsDivisionOrAvg(text string) bool {
	toks, _ := lexSQL(text)
	return countWord(toks, pgDiv)+countWord(toks, pgAvg) > 0
}

// A class is what a value is known to be from the statement's text and its
// columns' types.
type exactCls int

const (
	clsUnknown exactCls = iota // not known to be a numeric or a whole number
	clsWhole32                 // a whole number of at most 32 bits (smallint, integer)
	clsWhole64                 // a bigint
	clsDecimal                 // a numeric
)

// classOfTypes is the class of a column from the types the copy has for its
// name, across the tables read. Every one must be a DECIMAL, or every one a
// whole number: a name with any other type among them (a float, text, the
// mark of a name the statement defines) is not known.
func classOfTypes(ts []string) exactCls {
	cls := clsUnknown
	for _, ty := range ts {
		var c exactCls
		switch ty {
		case "TINYINT", "SMALLINT", "INTEGER", "UTINYINT", "USMALLINT":
			c = clsWhole32
		case "BIGINT", "UINTEGER":
			c = clsWhole64
		default:
			if !typeIsDividable(ty) {
				return clsUnknown
			}
			c = clsDecimal
		}
		switch {
		case cls == clsUnknown:
			cls = c
		case cls == clsDecimal || c == clsDecimal:
			if cls != c {
				return clsUnknown
			}
		case c > cls:
			cls = c
		}
	}
	return cls
}

// typeIsDividable reports whether ty is a DECIMAL the macros divide in 128
// bits for any sum of it: at most 20 digits, 8 of them decimals. A numeric
// declared without precision is a DECIMAL(38,18) on the copy, which would
// overflow them from a few hundred up, after the statement had run, and
// goes to the source up front.
func typeIsDividable(ty string) bool {
	rest, ok := strings.CutPrefix(strings.ReplaceAll(ty, " ", ""), "DECIMAL(")
	if !ok {
		return false
	}
	rest, ok = strings.CutSuffix(rest, ")")
	if !ok {
		return false
	}
	ps, ss, ok := strings.Cut(rest, ",")
	if !ok {
		return false
	}
	p, err1 := strconv.Atoi(ps)
	sc, err2 := strconv.Atoi(ss)
	return err1 == nil && err2 == nil && p <= 20 && sc <= 8
}

// joinCls is the class of a sum, a difference or a product of two values:
// a numeric when either is, the wider whole number otherwise.
func joinCls(a, b exactCls) exactCls {
	if a == clsUnknown || b == clsUnknown {
		return clsUnknown
	}
	return max(a, b)
}

// maxClassTokens is the longest side exactClass reads. A longer one is not
// known, and the statement goes to the source.
const maxClassTokens = 512

// exactClass is the class of the value toks write, clsUnknown unless the
// text shows a numeric or a whole number: numbers written without an
// exponent, columns (typesOf gives a name's types), +, - and *, parentheses,
// and the functions whose answer's type follows from their arguments'. It
// is what PostgreSQL makes of the value: sum of an integer is a bigint, and
// sum of a bigint a numeric, which the engine keeps a whole number, so that
// one is not known. A CASE is what its values are (caseClass). Anything else
// (a float, a string, a division left as written, a function not listed, a
// subquery) is not known.
func exactClass(toks []rtToken, typesOf func(string) []string) exactCls {
	if len(toks) == 0 || len(toks) > maxClassTokens {
		return clsUnknown
	}
	cls, values := clsUnknown, 0
	take := func(c exactCls) bool {
		if c == clsUnknown {
			return false
		}
		if values == 0 {
			cls = c
		} else {
			cls = joinCls(cls, c)
		}
		values++
		return true
	}
	for i := 0; i < len(toks); i++ {
		t := toks[i]
		switch {
		case isOpTok(toks, i, "+") || isOpTok(toks, i, "-") || isOpTok(toks, i, "*"):
		case isOpTok(toks, i, "("):
			end := closingParen(toks, i)
			if end < 0 || !take(exactClass(toks[i+1:end], typesOf)) {
				return clsUnknown
			}
			i = end
		case t.kind == rtNumber:
			c := clsWhole32
			switch {
			case strings.ContainsAny(t.text, "eE"):
				return clsUnknown
			case strings.Contains(t.text, "."):
				c = clsDecimal
			case len(t.text) > 9:
				c = clsWhole64
			}
			take(c)
		case t.kind == rtWord && t.text == "case":
			end := caseEnd(toks, i)
			if end < 0 || !take(caseClass(toks[i:end+1], typesOf)) {
				return clsUnknown
			}
			i = end
		case (t.kind == rtWord || t.kind == rtQuoted) && isOpTok(toks, i+1, "("):
			end := closingParen(toks, i+1)
			if end < 0 || !take(callClass(strings.ToLower(t.text), toks[i+2:end], typesOf)) {
				return clsUnknown
			}
			// OVER or FILTER after it is a word no column has: not known.
			i = end
		case t.kind == rtWord || t.kind == rtQuoted:
			// A column, with its qualifiers or without: the last name is the
			// column's.
			for isOpTok(toks, i+1, ".") && i+2 < len(toks) && (toks[i+2].kind == rtWord || toks[i+2].kind == rtQuoted) {
				i += 2
			}
			if isOpTok(toks, i+1, "(") || isOpTok(toks, i+1, ".") {
				return clsUnknown
			}
			if !take(classOfTypes(typesOf(strings.ToLower(toks[i].text)))) {
				return clsUnknown
			}
		default:
			return clsUnknown
		}
	}
	if values == 0 {
		return clsUnknown
	}
	return cls
}

// callClass is the class of name(args), clsUnknown for a function whose
// answer's type the port does not derive.
func callClass(name string, args []rtToken, typesOf func(string) []string) exactCls {
	switch name {
	case "count":
		return clsWhole64
	case "length", "char_length", "octet_length", "position", pgAscii:
		return clsWhole32
	case pgDiv, pgAvgN, pgRoundDiv, pgRoundAvg, pgRoundK, pgRoundNeg, pgRoundWhole:
		return clsDecimal
	case pgAvg:
		// Not yet renamed when a division around it is decided
		// (retypeForCopy): of numerics or whole numbers it will be pgAvgN.
		if exactClass(args, typesOf) != clsUnknown {
			return clsDecimal
		}
		return clsUnknown
	case "sum":
		switch exactClass(args, typesOf) {
		case clsWhole32:
			return clsWhole64
		case clsDecimal:
			return clsDecimal
		}
		// Of a bigint PostgreSQL sums to a numeric, the engine to a whole
		// number: a division of it is another division on each side.
		return clsUnknown
	case "min", "max", "abs":
		return exactClass(args, typesOf)
	case "coalesce", "nullif", "greatest", "least":
		cls, depth, from := clsUnknown, 0, 0
		for i := 0; i <= len(args); i++ {
			switch {
			case i < len(args) && isOpTok(args, i, "("):
				depth++
			case i < len(args) && isOpTok(args, i, ")"):
				depth--
			case i == len(args) || depth == 0 && isOpTok(args, i, ","):
				c := exactClass(args[from:i], typesOf)
				if c == clsUnknown {
					return clsUnknown
				}
				if from == 0 {
					cls = c
				} else {
					cls = joinCls(cls, c)
				}
				from = i + 1
			}
		}
		return cls
	}
	return clsUnknown
}

// divisionVeto is why a division the rewrite wrote as pgDiv(n, d) goes to the
// source, "" when the copy answers it: a divisor written as zero,
// PostgreSQL's error; two whole numbers, which PostgreSQL divides as whole
// numbers (7 / 2 is 3) where the macro answers a numeric; and a side the
// statement's text does not show to be a numeric or a whole number, which
// the macro would refuse once the statement had run. typesOf is the screen's
// types, the names the statement defines among them; untyped says they are
// not the columns' own, and only the divisor of zero is told.
func divisionVeto(a [][]rtToken, typesOf func(string) []string, untyped bool) string {
	if len(a) != 2 || len(a[0]) == 0 || len(a[1]) == 0 {
		return "a division the port does not read"
	}
	if d := unsigned(a[1]); len(d) == 1 && d[0].kind == rtNumber && strings.Trim(d[0].text, "0.") == "" {
		return "a division by zero"
	}
	if untyped {
		// The columns' types are not known yet (screenUntyped, which reads
		// every name as a whole number): the typed screen shows the sides.
		return ""
	}
	n, d := exactClass(a[0], typesOf), exactClass(a[1], typesOf)
	switch {
	case n == clsUnknown || d == clsUnknown:
		return "a division of a value the port cannot show to be a numeric or a whole number"
	case n != clsDecimal && d != clsDecimal:
		return "a division of two whole numbers (PostgreSQL divides them as whole numbers)"
	}
	return ""
}

// avgVeto is why an avg the rewrite wrote as pgAvgN(x) goes to the source,
// "" when the copy answers it.
func avgVeto(a [][]rtToken, typesOf func(string) []string) string {
	if len(a) != 1 || exactClass(a[0], typesOf) == clsUnknown {
		return "avg() of a value the port cannot show to be a numeric or a whole number"
	}
	return ""
}
