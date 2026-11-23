package pgshim

import (
	"regexp"
	"strconv"
	"strings"
)

// Three rules of the screen, each for an answer pgcompare measured the copy
// giving otherwise than PostgreSQL 16 (screenExactness there):
//
//   - substring() with a start or a length that is not a positive whole
//     number written in the statement (#3071);
//   - a date, timestamp or time literal that is not written in the one form
//     both read alike (#3072);
//   - arithmetic and sum() over a real column (#3069).
//
// Each sends the statement to the source. None looks at a value: the rule is
// by what the statement writes and by the copy's column types.

// --- #3071: substring ----------------------------------------------------------

const substringVeto = "substring() with a start or a length other than a positive whole number (the copy counts a negative start from the end)"

// positiveWhole reports whether arg is one whole number above zero, written
// as digits.
func positiveWhole(arg []rtToken) bool {
	return len(arg) == 1 && arg[0].kind == rtNumber && !strings.Contains(arg[0].text, ".") && strings.Trim(arg[0].text, "0") != ""
}

// substringOK reports whether a substring call, its arguments split at their
// commas, is one pgcompare measured: substring(x, n), substring(x, n, m),
// substring(x FROM n) and substring(x FROM n FOR m), n and m positive whole
// numbers. With a start of zero or below PostgreSQL counts the positions
// before the text as part of the length (substring('apple', -1, 3) is 'a'),
// and the copy counts a negative start from the end ('e'). A start that is a
// column or an expression may be either, and substring(x FROM 'pattern') and
// substring(x FOR m) are not measured.
func substringOK(args [][]rtToken) bool {
	switch len(args) {
	case 2:
		return len(args[0]) > 0 && positiveWhole(args[1])
	case 3:
		return len(args[0]) > 0 && positiveWhole(args[1]) && positiveWhole(args[2])
	case 1:
	default:
		return false
	}
	a := args[0]
	from, forAt, depth := -1, -1, 0
	for j, t := range a {
		switch {
		case t.kind == rtOp && (t.text == "(" || t.text == "["):
			depth++
		case t.kind == rtOp && (t.text == ")" || t.text == "]"):
			depth--
		case depth == 0 && t.kind == rtWord && t.text == "from" && from < 0:
			from = j
		case depth == 0 && t.kind == rtWord && t.text == "for" && from >= 0 && forAt < 0:
			forAt = j
		}
	}
	if from < 1 {
		return false
	}
	if forAt < 0 {
		return positiveWhole(a[from+1:])
	}
	return positiveWhole(a[from+1:forAt]) && positiveWhole(a[forAt+1:])
}

// --- #3072: date and time literals ---------------------------------------------

const looseTypedVeto = "a date or time literal not written as YYYY-MM-DD, HH:MM:SS with at most six decimals and an hour below 24 (PostgreSQL reads other forms differently)"

const (
	strictDateForm = `\d{4}-\d{2}-\d{2}`
	strictTimeForm = `([01]\d|2[0-3]):[0-5]\d(:[0-5]\d(\.\d{1,6})?)?`
)

var (
	strictDate      = regexp.MustCompile(`^` + strictDateForm + `$`)
	strictTime      = regexp.MustCompile(`^` + strictTimeForm + `$`)
	strictTimestamp = regexp.MustCompile(`^` + strictDateForm + `([ T]` + strictTimeForm + `)?$`)
)

// strictTyped reports whether text, the string of a literal of type typ
// (date, timestamp or time), is written in the form PostgreSQL and the copy
// were measured reading alike: a year of four digits, a month and a day of
// two, an hour below 24, at most six decimals of a second. Outside it they
// part: '26-01-05' is 2026-01-05 on PostgreSQL and the year 26 on the copy, a
// seventh decimal is rounded there and cut here, and 24:00:00 is a time
// there and midnight here. Any other type (an interval) is not judged.
func strictTyped(typ, text string) bool {
	switch typ {
	case "date":
		return strictDate.MatchString(text)
	case "timestamp":
		return strictTimestamp.MatchString(text)
	case "time":
		return strictTime.MatchString(text)
	}
	return true
}

// literalFamily is the literal type a column of the copy's type ty takes a
// string as when it is compared with one: date, timestamp or time, "" for
// any other type (one with a time zone included: its strings may name one).
func literalFamily(ty string) string {
	switch ty {
	case "DATE":
		return "date"
	case "TIMESTAMP":
		return "timestamp"
	case "TIME":
		return "time"
	}
	return ""
}

// --- #3069: what kind of number a value is -------------------------------------

// numKind is what the screen knows of a value as a number.
type numKind int

const (
	numUnknown  numKind = iota // nothing is known of it
	numNull                    // NULL
	numScaled                  // a whole number, or a numeric with a known number of decimals
	numAnyScale                // a numeric whose number of decimals is not known here
	numDouble                  // a double precision
	numReal                    // a real, or something that may be one
	numNot                     // known not to be a number: text, a date, a boolean
)

type numValue struct {
	kind  numKind
	scale int // of a numScaled
}

// numReader reads values as numbers from a statement's own text: its
// literals, the copy's column types, and the names the statement defines
// (an alias of a select item is what that item is).
type numReader struct {
	types map[string][]string    // the copy's column types (columnTypes); nil when not known
	defs  map[string][][]rtToken // aliasDefinitions
	// listed: the statement names columns in a list (AS t(a, b)), which the
	// definitions do not read: no defined name is known then.
	listed bool
	// known is what each name was read as, kept for the statement: a
	// definition is worked out once, however many times its name is used.
	// busy are the names being worked out now: one used inside its own
	// definition is read there as the column of that name alone.
	known map[string]numValue
	busy  map[string]bool
	// steps is the work done so far, in tokens read, and depth how many
	// definitions deep the reading is. Past numReadSteps or numReadDepth the
	// reader gives up (over): the statement is not one it can judge, and the
	// screen sends it to the source.
	steps, depth int
	over         bool
}

// The most a statement's reading may cost. A report of a few thousand tokens
// with a dozen names costs a few thousand steps; these bound a statement
// written to cost more (names defined from names in a long chain,
// parentheses nested by the thousand).
const (
	numReadSteps = 200000
	numReadDepth = 64
)

const numReadVeto = "a statement whose names and parentheses nest deeper than the port reads to tell its numbers apart"

func newNumReader(toks []rtToken, types map[string][]string, match []int) *numReader {
	return &numReader{types: types, defs: aliasDefinitions(toks), listed: listsColumns(toks, match), known: map[string]numValue{}, busy: map[string]bool{}}
}

// matchParens is, for each token that opens or closes a parenthesis or a
// bracket, where its other end is, and -1 for every other token and for one
// with no other end. One pass over the statement: nothing here looks for a
// parenthesis' other end by walking the statement again, which a statement
// of nothing but opening parentheses would make cost its length squared.
func matchParens(toks []rtToken) []int {
	match := make([]int, len(toks))
	var open []int
	for i, t := range toks {
		match[i] = -1
		if t.kind != rtOp {
			continue
		}
		switch t.text {
		case "(", "[":
			open = append(open, i)
		case ")", "]":
			if n := len(open); n > 0 {
				match[i], match[open[n-1]] = open[n-1], i
				open = open[:n-1]
			}
		}
	}
	return match
}

// listsColumns is columnList (aliastypes.go) read with the statement's
// parentheses already matched: whether the statement names columns in a list
// (AS b(n), ) b(n), c(n) AS (...)).
func listsColumns(toks []rtToken, match []int) bool {
	for i, t := range toks {
		if t.kind != rtWord || !isOpTok(toks, i+1, "(") || parenWords[t.text] || t.text == "over" || t.text == "within" {
			continue
		}
		if i > 0 && (toks[i-1].kind == rtWord && toks[i-1].text == "as" || isOpTok(toks, i-1, ")")) {
			return true
		}
		if end := match[i+1]; end > 0 && end+1 < len(toks) && toks[end+1].kind == rtWord && toks[end+1].text == "as" &&
			(isOpTok(toks, end+2, "(") || end+2 < len(toks) && toks[end+2].kind == rtWord && (toks[end+2].text == "materialized" || toks[end+2].text == "not")) {
			return true
		}
	}
	return false
}

// spend counts n tokens of work, and reports whether the reader may go on.
func (r *numReader) spend(n int) bool {
	r.steps += n + 1
	if r.steps > numReadSteps {
		r.over = true
	}
	return !r.over
}

var decimalType = regexp.MustCompile(`^(DECIMAL|NUMERIC)\((\d+),(\d+)\)$`)

// typeNum is the kind of a column of the copy's type ty.
func typeNum(ty string) numValue {
	ty = strings.ReplaceAll(strings.ToUpper(ty), " ", "")
	switch {
	case typeIsInteger(ty):
		return numValue{kind: numScaled}
	case ty == unsizedNumeric || ty == "DECIMAL" || ty == "NUMERIC" || ty == notTextType:
		return numValue{kind: numAnyScale}
	case ty == "DOUBLE" || ty == "FLOAT8":
		return numValue{kind: numDouble}
	case ty == "FLOAT" || ty == "REAL" || ty == "FLOAT4":
		return numValue{kind: numReal}
	}
	if m := decimalType.FindStringSubmatch(ty); m != nil {
		s, _ := strconv.Atoi(m[3])
		return numValue{kind: numScaled, scale: s}
	}
	return numValue{kind: numNot}
}

// decimals is how many digits a number literal has after its point.
func decimals(n string) int {
	if dot := strings.IndexByte(n, '.'); dot >= 0 {
		return len(n) - dot - 1
	}
	return 0
}

// together is the one kind of several readings of the same name (a column
// of that name in two tables, a name defined twice): theirs when they agree,
// the most careful one when they do not.
func together(vals []numValue) numValue {
	if len(vals) == 0 {
		return numValue{}
	}
	same, hasReal, scaled := true, false, false
	for _, v := range vals {
		if v != vals[0] {
			same = false
		}
		if v.kind == numReal {
			hasReal = true
		}
		if v.kind == numAnyScale || v.kind == numScaled && v.scale > 0 {
			scaled = true
		}
	}
	switch {
	case hasReal:
		return numValue{kind: numReal}
	case same:
		return vals[0]
	case scaled:
		return numValue{kind: numAnyScale}
	}
	return numValue{}
}

// name is the kind of what a name stands for: every column of that name, and
// every expression the statement defines it as.
func (r *numReader) name(n string) numValue {
	if v, ok := r.known[n]; ok {
		return v
	}
	var vals []numValue
	exprs, defined := r.defs[n]
	inside := r.busy[n]
	if defined && !inside {
		if r.listed {
			r.known[n] = numValue{}
			return numValue{}
		}
		if r.depth >= numReadDepth {
			r.over = true
			return numValue{}
		}
		r.busy[n] = true
		r.depth++
		for _, e := range exprs {
			vals = append(vals, r.value(e))
		}
		r.depth--
		delete(r.busy, n)
	}
	for _, ty := range r.types[n] {
		vals = append(vals, typeNum(ty))
	}
	v := together(vals)
	if !inside && !r.over {
		// Inside its own definition the name was read as the column alone,
		// which is not what it stands for elsewhere: not kept.
		r.known[n] = v
	}
	return v
}

// tokenName is the name a token writes, "" for a token that is not one.
func tokenName(t rtToken) string {
	switch t.kind {
	case rtWord:
		return t.text
	case rtQuoted:
		return strings.ToLower(t.text)
	}
	return ""
}

// value is the kind of the expression toks: a literal, a column (with or
// without its table's name), count, min, max or sum of one, or, for anything
// else, the most careful kind of what it names (holds).
func (r *numReader) value(toks []rtToken) numValue {
	if !r.spend(len(toks)) {
		return numValue{}
	}
	for len(toks) >= 2 && isOpTok(toks, 0, "(") && closingParen(toks, 0) == len(toks)-1 {
		toks = toks[1 : len(toks)-1]
		if !r.spend(len(toks)) {
			return numValue{}
		}
	}
	if len(toks) == 2 && toks[0].kind == rtOp && (toks[0].text == "-" || toks[0].text == "+") {
		toks = toks[1:]
	}
	if len(toks) == 0 {
		return numValue{}
	}
	if len(toks) == 1 {
		t := toks[0]
		switch t.kind {
		case rtNumber:
			return numValue{kind: numScaled, scale: decimals(t.text)}
		case rtWord:
			switch t.text {
			case "null":
				return numValue{kind: numNull}
			case "true", "false":
				return numValue{kind: numNot}
			}
			return r.name(t.text)
		case rtQuoted:
			return r.name(strings.ToLower(t.text))
		}
		// A string takes the type of what stands beside it, a parameter any.
		return numValue{}
	}
	// t.col, s.t.col: the column.
	qualified := len(toks)%2 == 1
	for j := 0; qualified && j < len(toks); j++ {
		if j%2 == 1 {
			qualified = isOpTok(toks, j, ".")
		} else {
			qualified = tokenName(toks[j]) != ""
		}
	}
	if qualified {
		return r.name(tokenName(toks[len(toks)-1]))
	}
	if toks[0].kind == rtWord && isOpTok(toks, 1, "(") && closingParen(toks, 1) == len(toks)-1 {
		arg := toks[2 : len(toks)-1]
		switch toks[0].text {
		case "count":
			return numValue{kind: numScaled}
		case "min", "max", "sum":
			// Of a numeric they keep its decimals on both sides, and of a
			// whole number they answer one.
			if len(arg) > 0 && arg[0].kind == rtWord && (arg[0].text == "distinct" || arg[0].text == "all") {
				arg = arg[1:]
			}
			if v := r.value(arg); v.kind != numUnknown && v.kind != numNull {
				return v
			}
			return r.holds(arg)
		}
	}
	return r.holds(toks)
}

// holds is the kind of an expression the reader does not work out, from the
// names and numbers in it: a real when it names one, a numeric of unknown
// decimals when it names a numeric or writes a number with decimals, nothing
// known otherwise. A function's name and a table's before a dot are not
// values.
func (r *numReader) holds(toks []rtToken) numValue {
	out := numValue{}
	if !r.spend(len(toks)) {
		return out
	}
	for j, t := range toks {
		var v numValue
		switch {
		case t.kind == rtNumber:
			v = numValue{kind: numScaled, scale: decimals(t.text)}
		case tokenName(t) != "" && !isOpTok(toks, j+1, "(") && !isOpTok(toks, j+1, "."):
			v = r.name(tokenName(t))
		default:
			continue
		}
		switch {
		case v.kind == numReal:
			return v
		case v.kind == numAnyScale || v.kind == numScaled && v.scale > 0:
			out = numValue{kind: numAnyScale}
		}
	}
	return out
}

// --- #3069: arithmetic and sum over a real -------------------------------------

const (
	realArithmeticVeto = "arithmetic over a real column (PostgreSQL computes it as a double precision, the copy as a real)"
	realSumVeto        = "sum() over a real column (PostgreSQL answers a real, the copy a double precision)"
)

// realUse names arithmetic (+ - * /) with a real column on either side, or a
// sum() over one, "" when none. PostgreSQL widens a real to a double
// precision when it computes with another type (r + 1 with r = -0.1 is
// 0.8999999985098839 there and 0.9 on the copy), and answers sum(r) as a
// real where the copy answers a double precision (4.4 and
// 4.399999998509884). A side is what stands right beside the operator: a
// column, a name the statement gives one, a parenthesis or a function call
// that names one. A double precision column is not a real, and a real read,
// compared, counted or taken the min or max of stays on the copy.
func realUse(toks []rtToken, types map[string][]string) string {
	if types == nil {
		return ""
	}
	// Without a real column in the copy's tables nothing here is one.
	found := false
	for _, ts := range types {
		for _, ty := range ts {
			if typeNum(ty).kind == numReal {
				found = true
			}
		}
	}
	if !found {
		return ""
	}
	match := matchParens(toks)
	r := newNumReader(toks, types, match)
	for i, t := range toks {
		if r.over {
			return numReadVeto
		}
		switch {
		case t.kind == rtWord && t.text == "sum" && isOpTok(toks, i+1, "(") && !isOpTok(toks, i-1, "."):
			if end := match[i+1]; end > 0 && r.holds(toks[i+2:end]).kind == numReal {
				return realSumVeto
			}
		case t.kind == rtOp && (t.text == "+" || t.text == "-" || t.text == "*" || t.text == "/"):
			// The side before it: a name, or what a parenthesis closes, with
			// the function's name before it.
			if i > 0 {
				from := i - 1
				if isOpTok(toks, from, ")") {
					if open := match[from]; open >= 0 {
						from = open
						if open > 0 && toks[open-1].kind == rtWord {
							from = open - 1
						}
					}
				}
				if r.side(toks[from:i]) {
					return realArithmeticVeto
				}
			}
			// The side after it: past a table's name, a name, a call or a
			// parenthesis.
			from := i + 1
			for from+2 < len(toks) && tokenName(toks[from]) != "" && isOpTok(toks, from+1, ".") {
				from += 2
			}
			if from >= len(toks) {
				continue
			}
			to := from + 1
			open := -1
			switch {
			case isOpTok(toks, from, "("):
				open = from
			case toks[from].kind == rtWord && isOpTok(toks, from+1, "("):
				open = from + 1
			}
			if open >= 0 {
				if end := match[open]; end > 0 {
					to = end + 1
				}
			}
			if r.side(toks[from:to]) {
				return realArithmeticVeto
			}
		}
	}
	if r.over {
		return numReadVeto
	}
	return ""
}

// side reports whether one side of an operator may be a real.
func (r *numReader) side(toks []rtToken) bool {
	if len(toks) == 1 && toks[0].kind == rtWord && aliasStop[toks[0].text] {
		// A keyword (THEN -1, AND -x): no value stands there.
		return false
	}
	if r.value(toks).kind == numReal {
		return true
	}
	return r.holds(toks).kind == numReal
}
