package pgshim

import (
	"regexp"
	"strconv"
	"strings"
)

// Four rules of the screen, each for an answer pgcompare measured the copy
// giving otherwise than PostgreSQL 16 (screenExactness there):
//
//   - substring() with a start or a length that is not a positive whole
//     number written in the statement (#3071);
//   - a date, timestamp or time literal that is not written in the one form
//     both read alike (#3072);
//   - CASE, COALESCE, GREATEST and LEAST over numbers PostgreSQL writes with
//     different decimals (#3070);
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

// --- #3070 and #3069: what kind of number a value is ---------------------------

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
	busy   map[string]bool
}

func newNumReader(toks []rtToken, types map[string][]string) *numReader {
	return &numReader{types: types, defs: aliasDefinitions(toks), listed: columnList(toks), busy: map[string]bool{}}
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
	var vals []numValue
	if exprs, defined := r.defs[n]; defined && !r.busy[n] {
		if r.listed {
			return numValue{}
		}
		r.busy[n] = true
		for _, e := range exprs {
			vals = append(vals, r.value(e))
		}
		delete(r.busy, n)
	}
	for _, ty := range r.types[n] {
		vals = append(vals, typeNum(ty))
	}
	return together(vals)
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
	for len(toks) >= 2 && isOpTok(toks, 0, "(") && closingParen(toks, 0) == len(toks)-1 {
		toks = toks[1 : len(toks)-1]
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

// --- #3070: CASE, COALESCE, GREATEST and LEAST over numbers --------------------

const numericMixVeto = " over numbers of different decimals, which PostgreSQL writes with each value's own (0 where the copy writes 0.00)"

// caseValues are the values of the CASE at toks[i:end+1], as [from, to)
// token spans: from after each THEN or ELSE of this CASE (not of one inside
// it, nor inside parentheses) to the next WHEN, ELSE or its END.
func caseValues(toks []rtToken, i, end int) [][2]int {
	var values [][2]int
	start, depth, nested := -1, 0, 0
	for j := i + 1; j <= end; j++ {
		t := toks[j]
		switch {
		case isOpTok(toks, j, "("):
			depth++
			continue
		case isOpTok(toks, j, ")"):
			depth--
			continue
		}
		if t.kind != rtWord || depth != 0 || isOpTok(toks, j-1, ".") {
			continue
		}
		switch t.text {
		case "case":
			nested++
			continue
		case "end":
			if nested > 0 {
				nested--
				continue
			}
		}
		if nested > 0 {
			continue
		}
		switch t.text {
		case "when", "else", "end":
			if start >= 0 {
				values = append(values, [2]int{start, j})
				start = -1
			}
			if t.text == "else" {
				start = j + 1
			}
		case "then":
			start = j + 1
		}
	}
	return values
}

// callArgs are the arguments of the call whose "(" is at open and ")" at
// end, as [from, to) token spans.
func callArgs(toks []rtToken, open, end int) [][2]int {
	var args [][2]int
	from, depth := open+1, 0
	for j := open + 1; j <= end; j++ {
		switch {
		case isOpTok(toks, j, "(") || isOpTok(toks, j, "["):
			depth++
		case (isOpTok(toks, j, ")") || isOpTok(toks, j, "]")) && j < end:
			depth--
		case depth == 0 && (isOpTok(toks, j, ",") || j == end):
			args = append(args, [2]int{from, j})
			from = j + 1
		}
	}
	return args
}

// numericMix names a CASE, COALESCE, GREATEST or LEAST whose values are
// numbers PostgreSQL writes with different decimals, "" when none: the copy
// gives them all one type and writes every one with that type's decimals
// (CASE WHEN n > 3 THEN n ELSE 0 END is 0 on PostgreSQL and 0.00 here,
// greatest(n, n4) is 10.50 there and 10.5000 here). It stays on the copy
// when every value that is not NULL is known to have the same number of
// decimals (a numeric(12,2) beside 0.00, whole numbers beside whole numbers),
// when a value is a double precision or a real (both sides answer a float
// then, which has no decimals to keep), and when no value is known to be a
// numeric with decimals. One that
// holds a rewritten round is checked value by value where it runs instead
// (packMixedScale, scaleMixRisk).
//
// literalsOnly is the screen before the columns' types are known
// (screenUntyped): only numbers written in the statement are judged there,
// which no column's type can change.
func numericMix(toks []rtToken, types map[string][]string, literalsOnly bool) string {
	var r *numReader
	for i, t := range toks {
		if t.kind != rtWord || isOpTok(toks, i-1, ".") {
			continue
		}
		var values [][2]int
		what := ""
		switch t.text {
		case "case":
			end := caseEnd(toks, i)
			if end < 0 {
				continue
			}
			values, what = caseValues(toks, i, end), "CASE"
		case "coalesce", "greatest", "least":
			if !isOpTok(toks, i+1, "(") {
				continue
			}
			end := closingParen(toks, i+1)
			if end < 0 {
				continue
			}
			values, what = callArgs(toks, i+1, end), t.text+"()"
		default:
			continue
		}
		if r == nil {
			r = newNumReader(toks, types)
		}
		scales := map[int]bool{}
		count, anyScale, unknown, float, macro := 0, 0, 0, false, false
		for _, v := range values {
			if v[1] <= v[0] {
				continue
			}
			branch := toks[v[0]:v[1]]
			for _, b := range branch {
				if b.kind == rtWord && strings.HasPrefix(b.text, "_bintrail_pg_") {
					macro = true
				}
			}
			var k numValue
			if literalsOnly {
				if len(branch) == 1 && branch[0].kind == rtNumber {
					k = numValue{kind: numScaled, scale: decimals(branch[0].text)}
				} else {
					continue
				}
			} else {
				k = r.value(branch)
			}
			if k.kind == numNull {
				continue
			}
			count++
			switch k.kind {
			case numScaled:
				scales[k.scale] = true
			case numAnyScale:
				anyScale++
			case numDouble, numReal:
				float = true
			case numUnknown:
				unknown++
			}
		}
		if macro || count < 2 {
			continue
		}
		decimalsKnown := false
		for s := range scales {
			if s > 0 {
				decimalsKnown = true
			}
		}
		switch {
		case float:
			continue
		case len(scales) >= 2,
			anyScale > 0 && (len(scales) > 0 || anyScale > 1 || unknown > 0),
			decimalsKnown && unknown > 0:
			return what + numericMixVeto
		}
	}
	return ""
}

// --- #3069: arithmetic and sum over a real -------------------------------------

const (
	realArithmeticVeto = "arithmetic over a real column (PostgreSQL computes it as a double precision, the copy as a real)"
	realSumVeto        = "sum() over a real column (PostgreSQL answers a real, the copy a double precision)"
)

// openingParen finds the "(" that the ")" at end closes, -1 if none.
func openingParen(toks []rtToken, end int) int {
	depth := 0
	for j := end; j >= 0; j-- {
		switch {
		case isOpTok(toks, j, ")") || isOpTok(toks, j, "]"):
			depth++
		case isOpTok(toks, j, "(") || isOpTok(toks, j, "["):
			depth--
			if depth == 0 {
				return j
			}
		}
	}
	return -1
}

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
	r := newNumReader(toks, types)
	for i, t := range toks {
		switch {
		case t.kind == rtWord && t.text == "sum" && isOpTok(toks, i+1, "(") && !isOpTok(toks, i-1, "."):
			if end := closingParen(toks, i+1); end > 0 && r.holds(toks[i+2:end]).kind == numReal {
				return realSumVeto
			}
		case t.kind == rtOp && (t.text == "+" || t.text == "-" || t.text == "*" || t.text == "/"):
			// The side before it: a name, or what a parenthesis closes, with
			// the function's name before it.
			if i > 0 {
				from := i - 1
				if isOpTok(toks, from, ")") {
					if open := openingParen(toks, from); open >= 0 {
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
				if end := closingParen(toks, open); end > 0 {
					to = end + 1
				}
			}
			if r.side(toks[from:to]) {
				return realArithmeticVeto
			}
		}
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
