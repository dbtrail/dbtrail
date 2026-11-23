package pgshim

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// copyText is text as this connection hands it to the copy (RewriteForCopy
// when it can send a statement to its source, RewriteForCopyAlone when it
// cannot), or the error of a statement the copy would answer otherwise than
// PostgreSQL and that has nowhere else to go.
//
// Every connection gets its round and avg rewritten: without a forwarding
// account the copy answers every statement alone, and a round it writes
// with other decimals than PostgreSQL's (500 for 500.00) would reach the
// client as the answer. What the copy cannot round as PostgreSQL does it
// refuses, an error the client sees, never another number.
func (s *session) copyText(text string) (string, error) {
	if s.forwarding() {
		return RewriteForCopy(text), nil
	}
	return RewriteForCopyAlone(text)
}

// RewriteForCopy writes stmt for the copy of a PostgreSQL source, on a
// connection that can send what the copy refuses to the source: each
// round(...) and avg(...) becomes the copy's macro for it (sqlsandbox
// pgRoundMacros), which answers as PostgreSQL does or refuses, and so do
// floor(a / b), extract(epoch from x) and to_timestamp(n); a CASE or a
// COALESCE around one of them is checked value by value (packMixedScale).
// The rest of the text is left as written, byte for byte, and so is a round
// of any other shape: the screen then sends it to the source (round is not
// among copyFuncs). Every statement such a connection hands the copy goes
// through it, and so does the screen (CopyScreen, routeFree), so what is
// screened is what runs.
func RewriteForCopy(stmt string) string {
	out, _ := rewriteRounds(quoteCopyWords(stmt), true)
	return packMixedScale(out)
}

// RewriteForCopyAlone is RewriteForCopy for a connection whose statements
// the copy answers alone (no forwarding account, or a server that hides
// tables or columns): round and avg are rewritten the same way, floor,
// extract and to_timestamp are left as written (their macros refuse what the
// engine answers as PostgreSQL does in most cases, and here a refusal has no
// source to go to). What is left that the copy would answer otherwise (a
// round or an avg of another shape, a round beside a UNION...) is an error,
// 0A000, and nothing is run.
func RewriteForCopyAlone(stmt string) (string, error) {
	if w := ownName(stmt); w != "" {
		// PostgreSQL has no such function: the statement fails there.
		return "", &Error{Code: "42883", Message: "function does not exist: " + w}
	}
	out, _ := rewriteRounds(quoteCopyWords(stmt), false)
	out = packMixedScale(out)
	if why := aloneRefusal(out); why != "" {
		return "", &Error{Code: "0A000", Message: "the copy does not answer this as PostgreSQL would: " + why +
			"; this connection sends nothing to the source, so the statement is refused rather than answered otherwise"}
	}
	return out, nil
}

// The macros the rewrite writes, which the screen lets through.
const (
	pgRoundDiv         = "_bintrail_pg_round_div"
	pgRoundAvg         = "_bintrail_pg_round_avg"
	pgRoundK           = "_bintrail_pg_round_k"
	pgRoundNeg         = "_bintrail_pg_round_neg"
	pgRoundWhole       = "_bintrail_pg_round_whole"
	pgFloorDiv         = "_bintrail_pg_floor_div"
	pgFloorEpoch       = "_bintrail_pg_floor_epoch"
	pgEpochMacro       = "_bintrail_pg_epoch"
	pgToTimestampMacro = "_bintrail_pg_to_timestamp"
	pgPack             = "_bintrail_pg_pack"
	pgPackN            = "_bintrail_pg_packn"
	pgUnpack           = "_bintrail_pg_unpack"
	// avg(x) on its own: the screen does not let it through (the macro
	// answers only a float's, and refuses the rest).
	pgAvg = "_bintrail_pg_avg"
)

// maxRoundDecimals is the most decimals a rewritten round may ask for, the
// shift of round(10^j * avg(x), k) included: PostgreSQL's division keeps at
// least 16 decimals of a quotient under 1, so the macro refuses 16 and past
// it (l - kk < 1), and a unit of 16 decimals or more is a number the lexer
// refuses (more than 18 characters).
const maxRoundDecimals = 15

// maxRoundTens is the most a negative k of round(x, -k) may be: a numeric of
// the copy has at most 38 digits.
const maxRoundTens = 38

// ownName names a word of the client's statement that DBTrail keeps for the
// copy's own macros (_bintrail_...), "" when none: on PostgreSQL there is no
// such function and the statement fails, so the copy must not answer it.
func ownName(stmt string) string {
	toks, _ := lexSQL(stmt)
	for _, t := range toks {
		if (t.kind == rtWord || t.kind == rtQuoted) && strings.HasPrefix(strings.ToLower(t.text), "_bintrail_") {
			return "the name " + t.text + ", which DBTrail keeps for the copy's own use"
		}
	}
	return ""
}

// rewriteRounds writes q's round and avg calls as the copy's macros, and,
// when full (a connection that can reach its source), its floor(a / b),
// extract(epoch from x) and to_timestamp(n) too.
func rewriteRounds(q string, full bool) (string, bool) {
	toks, spans, why := lexSQLSpans(q)
	if why != "" || len(spans) != len(toks) {
		return q, false
	}
	var b strings.Builder
	last, changed := 0, false
	for i := 0; i < len(toks); i++ {
		if toks[i].kind != rtWord || !isOpTok(toks, i+1, "(") || isOpTok(toks, i-1, ".") {
			continue
		}
		var call func(string, []rtToken, [][2]int, int, int, bool) (string, bool)
		switch toks[i].text {
		case "round":
			call = roundCall
		case "avg":
			call = avgCall
		case "floor":
			call = floorCall
		case "extract":
			call = epochCall
		case "to_timestamp":
			call = toTimestampCall
		default:
			continue
		}
		if !full && toks[i].text != "round" && toks[i].text != "avg" {
			continue
		}
		end := closingParen(toks, i+1)
		if end < 0 {
			continue
		}
		repl, ok := call(q, toks, spans, i+2, end, full)
		if !ok {
			// Left as written; a round inside it is still read.
			continue
		}
		b.WriteString(q[last:spans[i][0]])
		b.WriteString(repl)
		last = spans[end][1]
		i = end
		changed = true
	}
	if !changed {
		return q, false
	}
	b.WriteString(q[last:])
	return b.String(), true
}

func isOpTok(toks []rtToken, i int, op string) bool {
	return i >= 0 && i < len(toks) && toks[i].kind == rtOp && toks[i].text == op
}

// closingParen is the index of the ")" that closes the "(" at open, -1 if
// none.
func closingParen(toks []rtToken, open int) int {
	depth := 0
	for i := open; i < len(toks); i++ {
		switch {
		case isOpTok(toks, i, "("):
			depth++
		case isOpTok(toks, i, ")"):
			depth--
			if depth == 0 {
				return i
			}
		}
	}
	return -1
}

// powerOfTen reads a multiplier of avg written as 10^j: 1, 100, 100.0,
// 10.00. j is how many places it moves the decimal point.
var powerOfTen = regexp.MustCompile(`^1(0*)(\.0*)?$`)

// notInADivision are the words a side of a rewritten division may not hold
// outside parentheses: each makes the side something other than a product
// of values, or reads it otherwise (a typed literal, a time zone).
var notInADivision = map[string]bool{
	"case": true, "when": true, "then": true, "else": true, "end": true,
	"and": true, "or": true, "not": true, "is": true, "null": true, "true": true, "false": true,
	"between": true, "in": true, "like": true, "ilike": true, "similar": true, "escape": true,
	"collate": true, "at": true, "time": true, "zone": true, "over": true, "filter": true,
	"distinct": true, "interval": true, "any": true, "all": true, "some": true, "exists": true,
	"array": true, "row": true, "select": true, "from": true, "as": true, "isnull": true, "notnull": true,
}

// roundCall is the macro call for round(<toks[from:to]>), false when the
// call is not one the copy rounds as PostgreSQL does.
func roundCall(q string, toks []rtToken, spans [][2]int, from, to int, full bool) (string, bool) {
	// One argument, or two: k must be written as a whole number, with a
	// minus sign or not (not $1, not 2.0).
	comma, depth := -1, 0
	for i := from; i < to; i++ {
		switch {
		case isOpTok(toks, i, "("):
			depth++
		case isOpTok(toks, i, ")"):
			depth--
		case depth == 0 && isOpTok(toks, i, ","):
			if comma >= 0 {
				return "", false
			}
			comma = i
		}
	}
	a0, a1, k, hasK := from, to, 0, comma >= 0
	if hasK {
		a1 = comma
		kt := toks[comma+1 : to]
		negative := len(kt) == 2 && isOpTok(kt, 0, "-")
		if negative {
			kt = kt[1:]
		}
		if len(kt) != 1 || kt[0].kind != rtNumber || strings.Trim(kt[0].text, "0123456789") != "" {
			return "", false
		}
		n, err := strconv.Atoi(kt[0].text)
		if err != nil || n > maxRoundTens {
			return "", false
		}
		k = n
		if negative {
			k = -n
		}
	}
	if a1 <= a0 || k > maxRoundDecimals || holdsCast(toks[a0:a1]) {
		return "", false
	}
	text := func(a, b int) string { return q[spans[a][0]:spans[b-1][1]] }
	unit := "1"
	if k > 0 {
		unit = "0." + strings.Repeat("0", k-1) + "1"
	}
	// date_part is a double precision on PostgreSQL, where round(x, k) of
	// one fails, and a whole number on the copy, where the macro would
	// round it.
	for i := a0; i < a1; i++ {
		if toks[i].kind == rtWord && toks[i].text == "date_part" && isOpTok(toks, i+1, "(") {
			return "", false
		}
	}
	other := func() (string, bool) { return otherRound(q, toks, spans, a0, a1, k, hasK, unit, full) }
	if k < 0 {
		return other()
	}

	// round(avg(x), k) and round(10^j * avg(x), k).
	at, shift := a0, 0
	if a1-a0 > 2 && toks[a0].kind == rtNumber && isOpTok(toks, a0+1, "*") {
		m := powerOfTen.FindStringSubmatch(toks[a0].text)
		if m == nil {
			return roundDivision(q, toks, spans, a0, a1, k, unit, text, other, full)
		}
		at, shift = a0+2, len(m[1])
	}
	if toks[at].kind == rtWord && toks[at].text == "avg" && isOpTok(toks, at+1, "(") && !isOpTok(toks, at-1, ".") {
		if closingParen(toks, at+1) != a1-1 || a1-1 == at+2 {
			// Something after avg(...) (OVER, FILTER, an operator), or avg().
			return roundDivision(q, toks, spans, a0, a1, k, unit, text, other, full)
		}
		inner0, inner1 := at+2, a1-1
		if toks[inner0].kind == rtWord && (toks[inner0].text == "distinct" || toks[inner0].text == "all") {
			return "", false
		}
		d := 0
		for i := inner0; i < inner1; i++ {
			switch {
			case isOpTok(toks, i, "("):
				d++
			case isOpTok(toks, i, ")"):
				d--
			case d == 0 && (isOpTok(toks, i, ",") || toks[i].kind == rtWord && toks[i].text == "order"):
				return "", false
			}
		}
		if k+shift > maxRoundDecimals {
			return "", false
		}
		inner, _ := rewriteRounds(text(inner0, inner1), full)
		return pgRoundAvg + "(" + inner + ", " + strconv.Itoa(k+shift) + ", " + unit + ")", true
	}
	return roundDivision(q, toks, spans, a0, a1, k, unit, text, other, full)
}

// roundDivision is the macro call for round(P / Q, k), where P is a product
// of values and Q one value: the division is the last thing done, as
// PostgreSQL reads a * b / c ((a * b) / c). Anything else at the top of the
// argument (a + b / c, a / b * c, a / b / c, -a / b, a cast with ::) is not,
// and is rounded as any other value (other).
func roundDivision(q string, toks []rtToken, spans [][2]int, a0, a1, k int, unit string, text func(int, int) string, other func() (string, bool), full bool) (string, bool) {
	p, d, ok := splitDivision(toks, a0, a1, text, full)
	if !ok {
		return other()
	}
	return pgRoundDiv + "(" + p + ", " + d + ", " + strconv.Itoa(k) + ", " + unit + ")", true
}

// otherRound is the macro call for round(x, k), round(x, -k) or round(x) of
// a value that is not an avg or a division the macros above take: of a
// numeric or a whole number the copy holds as PostgreSQL does, rounded to
// exactly k decimals (_bintrail_pg_round_k), to tens and past
// (_bintrail_pg_round_neg), or to none (_bintrail_pg_round_whole). False,
// and left as written, when x holds something the copy may hold as another
// number than PostgreSQL: a division (a sum of bigints is a numeric there
// and a whole number here, so sum(b) / 2 divides otherwise), or extract and
// date_part (a whole number here, a fraction or a double precision there).
func otherRound(q string, toks []rtToken, spans [][2]int, a0, a1, k int, hasK bool, unit string, full bool) (string, bool) {
	// Read as rewritten: a division or an epoch a macro took is the
	// macro's, exact.
	x, _ := rewriteRounds(q[spans[a0][0]:spans[a1-1][1]], full)
	xt, why := lexSQL(x)
	if why != "" {
		return "", false
	}
	for i, t := range xt {
		if isOpTok(xt, i, "/") || t.kind == rtWord && (t.text == "extract" || t.text == "date_part" ||
			doubleOfWhole[t.text] && isOpTok(xt, i+1, "(")) {
			return "", false
		}
	}
	switch {
	case !hasK:
		return pgRoundWhole + "(" + x + ")", true
	case k < 0:
		return pgRoundNeg + "(" + x + ", " + strconv.Itoa(k) + ")", true
	}
	return pgRoundK + "(" + x + ", " + strconv.Itoa(k) + ", " + unit + ")", true
}

// doubleOfWhole are the functions PostgreSQL answers as a double precision
// when given a whole number (it has no integer version, and an integer goes
// to double precision first), where the copy keeps the whole number: so
// round(ceil(qty), 2), an error on PostgreSQL, would be 5.00 here.
var doubleOfWhole = map[string]bool{"ceil": true, "ceiling": true, "floor": true, "trunc": true, "sign": true}

// holdsCast reports whether toks hold a cast: the copy's numeric without
// precision is a DECIMAL(18,3), which rounds a value to three decimals
// before a round sees it (round(x::numeric, 2) of 0.0045 would be 0.01
// here, 0.00 on PostgreSQL).
func holdsCast(toks []rtToken) bool {
	for i, t := range toks {
		if isOpTok(toks, i, "::") || t.kind == rtWord && t.text == "cast" {
			return true
		}
	}
	return false
}

// avgCall is the macro call for avg(<toks[from:to]>) of one value, false for
// avg(DISTINCT x), avg(x ORDER BY y), a window avg and an avg with FILTER,
// which a macro does not take.
func avgCall(q string, toks []rtToken, spans [][2]int, from, to int, full bool) (string, bool) {
	if from >= to || toks[from].kind == rtWord && (toks[from].text == "distinct" || toks[from].text == "all") || holdsCast(toks[from:to]) {
		return "", false
	}
	if t := to + 1; t < len(toks) && toks[t].kind == rtWord && (toks[t].text == "over" || toks[t].text == "filter" || toks[t].text == "within") {
		return "", false
	}
	depth := 0
	for i := from; i < to; i++ {
		switch {
		case isOpTok(toks, i, "("):
			depth++
		case isOpTok(toks, i, ")"):
			depth--
		case depth == 0 && (isOpTok(toks, i, ",") || toks[i].kind == rtWord && toks[i].text == "order"):
			return "", false
		}
	}
	x, _ := rewriteRounds(q[spans[from][0]:spans[to-1][1]], full)
	return pgAvg + "(" + x + ")", true
}
// floorCall is the macro call for floor(<toks[from:to]>) of a division,
// false for any other floor: floor(x) of a numeric is already PostgreSQL's
// on the copy, but floor of a float or a column is not something this
// port has measured, and it stays with the source.
func floorCall(q string, toks []rtToken, spans [][2]int, from, to int, full bool) (string, bool) {
	if from >= to {
		return "", false
	}
	text := func(a, b int) string { return q[spans[a][0]:spans[b-1][1]] }
	for i := from; i < to; i++ {
		if toks[i].kind == rtWord && toks[i].text == "date_part" && isOpTok(toks, i+1, "(") {
			return "", false
		}
	}
	if x, n, ok := epochOverSeconds(q, toks, spans, from, to, full); ok {
		return pgFloorEpoch + "(" + x + ", " + n + ")", true
	}
	p, d, ok := splitDivision(toks, from, to, text, full)
	if !ok {
		return "", false
	}
	return pgFloorDiv + "(" + p + ", " + d + ")", true
}

// maxEpochBucketSeconds is the largest N the copy's
// _bintrail_pg_floor_epoch takes (9 digits, about 31 years): its arithmetic
// is done in 64-bit whole numbers of microseconds, and 2000 times N
// million stays well inside them.
const maxEpochBucketSeconds = 999999999

// epochOverSeconds reads toks[from:to] as extract(epoch from <x>) / N, N a
// whole number of seconds written as digits, 1 to maxEpochBucketSeconds:
// the time bucket of floor(extract(epoch from x) / 600). It gives x (its
// own rounds rewritten) and N as the copy reads it. Any other division,
// a parameter, a cast or another operator among them, goes the general way.
func epochOverSeconds(q string, toks []rtToken, spans [][2]int, from, to int, full bool) (string, string, bool) {
	if to-from < 8 || toks[from].kind != rtWord || toks[from].text != "extract" || !isOpTok(toks, from+1, "(") {
		return "", "", false
	}
	end := closingParen(toks, from+1)
	if end < 0 || end+3 != to || !isOpTok(toks, end+1, "/") || toks[end+2].kind != rtNumber {
		return "", "", false
	}
	digits := toks[end+2].text
	if digits == "" || strings.Trim(digits, "0123456789") != "" {
		return "", "", false
	}
	n, err := strconv.Atoi(digits)
	if err != nil || n < 1 || n > maxEpochBucketSeconds {
		return "", "", false
	}
	if end-(from+2) < 3 || toks[from+2].kind != rtWord || toks[from+2].text != "epoch" || toks[from+3].kind != rtWord || toks[from+3].text != "from" {
		return "", "", false
	}
	x, _ := rewriteRounds(q[spans[from+4][0]:spans[end-1][1]], full)
	return x, strconv.Itoa(n), true
}

// epochCall is the macro call for extract(epoch from <x>), false for any
// other field: the screen lets year, month and day through as they are.
func epochCall(q string, toks []rtToken, spans [][2]int, from, to int, full bool) (string, bool) {
	if to-from < 3 || toks[from].kind != rtWord || toks[from].text != "epoch" || toks[from+1].kind != rtWord || toks[from+1].text != "from" {
		return "", false
	}
	x, _ := rewriteRounds(q[spans[from+2][0]:spans[to-1][1]], full)
	return pgEpochMacro + "(" + x + ")", true
}

// toTimestampCall is the macro call for to_timestamp(<x>) of one argument
// (to_timestamp(text, format) is another function).
func toTimestampCall(q string, toks []rtToken, spans [][2]int, from, to int, full bool) (string, bool) {
	if from >= to {
		return "", false
	}
	depth := 0
	for i := from; i < to; i++ {
		switch {
		case isOpTok(toks, i, "("):
			depth++
		case isOpTok(toks, i, ")"):
			depth--
		case depth == 0 && isOpTok(toks, i, ","):
			return "", false
		}
	}
	x, _ := rewriteRounds(q[spans[from][0]:spans[to-1][1]], full)
	return pgToTimestampMacro + "(" + x + ")", true
}

// splitDivision reads toks[a0:a1] as P / Q, where P is a product of values
// and Q one value, and gives P and Q as written (each rewritten in turn).
func splitDivision(toks []rtToken, a0, a1 int, text func(int, int) string, full bool) (string, string, bool) {
	if toks[a0].kind == rtOp && toks[a0].text != "(" {
		return "", "", false
	}
	slash, depth := -1, 0
	for i := a0; i < a1; i++ {
		t := toks[i]
		switch {
		case isOpTok(toks, i, "("):
			depth++
			continue
		case isOpTok(toks, i, ")"):
			depth--
			continue
		}
		if depth > 0 {
			continue
		}
		switch t.kind {
		case rtOp:
			switch t.text {
			case "/":
				if slash >= 0 {
					return "", "", false
				}
				slash = i
			case "*":
				if slash >= 0 {
					return "", "", false
				}
			case ".":
			default:
				return "", "", false
			}
		case rtWord:
			if notInADivision[t.text] {
				return "", "", false
			}
		case rtString, rtParam, rtTyped:
			return "", "", false
		}
	}
	if slash <= a0 || slash+1 >= a1 {
		return "", "", false
	}
	p, _ := rewriteRounds(text(a0, slash), full)
	d, _ := rewriteRounds(text(slash+1, a1), full)
	return p, d, true
}

// scaleMacro reports whether t names a macro whose answer has a number of
// decimals the rewrite fixed as PostgreSQL's (a round, a floor, an epoch), or
// the check of a CASE around one.
func scaleMacro(t rtToken) bool {
	if t.kind != rtWord || !strings.HasPrefix(t.text, "_bintrail_pg_") {
		return false
	}
	switch t.text {
	case pgAvg, pgToTimestampMacro, pgPack, pgPackN:
		return false
	}
	return true
}

func anyScaleMacro(toks []rtToken) bool {
	for _, t := range toks {
		if scaleMacro(t) {
			return true
		}
	}
	return false
}

// scaleNames are the names that stand, somewhere in the statement, for a
// value with decimals the rewrite fixed: the alias of a select item that
// holds such a macro (with AS or without), a WITH's column list over one,
// and the names PostgreSQL gives such an item when it has no alias (round,
// floor, extract). A CASE that reads such a name from a subquery or a WITH
// mixes its decimals as one that holds the macro does. Empty when the
// statement has no such macro.
func scaleNames(toks []rtToken) map[string]bool {
	names := map[string]bool{}
	if !anyScaleMacro(toks) {
		return names
	}
	names["round"], names["floor"], names["extract"] = true, true, true
	name := func(t rtToken) (string, bool) {
		switch t.kind {
		case rtWord:
			return t.text, true
		case rtQuoted:
			return strings.ToLower(t.text), true
		}
		return "", false
	}
	// itemStart is where the select item that ends before i starts: after
	// the nearest ",", SELECT or DISTINCT at its depth, or "(" above it.
	itemStart := func(i int) int {
		depth := 0
		for j := i - 1; j >= 0; j-- {
			switch {
			case isOpTok(toks, j, ")"):
				depth++
			case isOpTok(toks, j, "("):
				if depth == 0 {
					return j + 1
				}
				depth--
			case depth == 0 && (isOpTok(toks, j, ",") || toks[j].kind == rtWord && (toks[j].text == "select" || toks[j].text == "distinct" || toks[j].text == "all")):
				return j + 1
			}
		}
		return 0
	}
	for i := range toks {
		t := toks[i]
		var alias string
		var ok bool
		switch {
		case t.kind == rtWord && t.text == "as" && i+1 < len(toks):
			if isOpTok(toks, i+1, "(") {
				// WITH s(a, b) AS (...): the column list names the body's
				// items.
				end := closingParen(toks, i+1)
				if end > 0 && anyScaleMacro(toks[i+2:end]) && isOpTok(toks, i-1, ")") {
					for j := i - 2; j >= 0 && !isOpTok(toks, j, "("); j-- {
						if n, ok := name(toks[j]); ok {
							names[n] = true
						}
					}
				}
				continue
			}
			alias, ok = name(toks[i+1])
			if ok && anyScaleMacro(toks[itemStart(i):i]) {
				names[alias] = true
			}
		case i > 0 && (t.kind == rtWord || t.kind == rtQuoted) && i+1 < len(toks) &&
			(isOpTok(toks, i+1, ",") || toks[i+1].kind == rtWord && toks[i+1].text == "from") &&
			(isOpTok(toks, i-1, ")") || toks[i-1].kind == rtNumber || toks[i-1].kind == rtWord || toks[i-1].kind == rtQuoted):
			// round(x, 2) r FROM ..., round(x, 2) * 2 r, ...: an alias
			// without AS, after a value.
			if t.kind == rtWord && notInADivision[t.text] || toks[i-1].kind == rtWord && notInADivision[toks[i-1].text] {
				continue
			}
			alias, _ = name(t)
			if anyScaleMacro(toks[itemStart(i):i]) {
				names[alias] = true
			}
		}
	}
	return names
}

// holdsScale reports whether toks hold a rewritten round, or a name that
// stands for one elsewhere in the statement (names, from scaleNames).
func holdsScale(toks []rtToken, names map[string]bool) bool {
	if anyScaleMacro(toks) {
		return true
	}
	for i, t := range toks {
		if isOpTok(toks, i+1, "(") {
			continue
		}
		if t.kind == rtWord && names[t.text] || t.kind == rtQuoted && names[strings.ToLower(t.text)] {
			return true
		}
	}
	return false
}

// packMixedScale checks each value of a CASE or a COALESCE that holds a
// rewritten round against the one type the copy gives them all: PostgreSQL
// keeps each value's own decimals (CASE WHEN c THEN round(x, 2) ELSE 0 END
// is 0 there), the copy writes them all with the type's (0.00). Each value
// is packed with its own type's decimals (_bintrail_pg_pack, or
// _bintrail_pg_packn for a COALESCE, which takes the first that is not
// NULL) and the whole is checked by _bintrail_pg_unpack, which refuses a
// value whose decimals are not the type's (sqlsandbox pgRoundMacros). A
// CASE or COALESCE with no such round in its values is left as written: a
// round in a condition decides a branch and is not one.
func packMixedScale(q string) string {
	toks, spans, why := lexSQLSpans(q)
	if why != "" || len(spans) != len(toks) || !anyScaleMacro(toks) {
		return q
	}
	return packWith(q, toks, spans, scaleNames(toks))
}

// packWith is packMixedScale of q, lexed as toks and spans, with names the
// statement's scaleNames.
func packWith(q string, toks []rtToken, spans [][2]int, names map[string]bool) string {
	var b strings.Builder
	last, changed := 0, false
	for i := 0; i < len(toks); i++ {
		t := toks[i]
		if t.kind != rtWord {
			continue
		}
		var repl string
		end := -1
		switch {
		case t.text == "case" && !isOpTok(toks, i-1, "."):
			end = caseEnd(toks, i)
			if end >= 0 {
				repl = packCase(q, toks, spans, i, end, names)
			}
		case t.text == "coalesce" && isOpTok(toks, i+1, "(") && !isOpTok(toks, i-1, "."):
			end = closingParen(toks, i+1)
			if end >= 0 {
				repl = packCoalesce(q, toks, spans, i, end, names)
			}
		}
		if repl == "" {
			continue
		}
		b.WriteString(q[last:spans[i][0]])
		b.WriteString(repl)
		last = spans[end][1]
		i = end
		changed = true
	}
	if !changed {
		return q
	}
	b.WriteString(q[last:])
	return b.String()
}

// caseEnd is the index of the END that closes the CASE at i, -1 if none.
func caseEnd(toks []rtToken, i int) int {
	nested := 0
	for j := i + 1; j < len(toks); j++ {
		if toks[j].kind != rtWord || isOpTok(toks, j-1, ".") {
			continue
		}
		switch toks[j].text {
		case "case":
			nested++
		case "end":
			if nested == 0 {
				return j
			}
			nested--
		}
	}
	return -1
}

// packCase is the CASE at toks[i:end+1] with each value packed, "" when no
// value holds a rewritten round.
func packCase(q string, toks []rtToken, spans [][2]int, i, end int, names map[string]bool) string {
	// The values: from after each THEN or ELSE of this CASE (not of one
	// inside it, nor inside parentheses) to the next WHEN, ELSE or its END.
	type span struct{ from, to int }
	var values []span
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
				values = append(values, span{start, j})
				start = -1
			}
			if t.text == "else" {
				start = j + 1
			}
		case "then":
			start = j + 1
		}
	}
	held := false
	for _, v := range values {
		if v.to <= v.from {
			return ""
		}
		if holdsScale(toks[v.from:v.to], names) {
			held = true
		}
	}
	if !held {
		return ""
	}
	var b strings.Builder
	b.WriteString(pgUnpack + "(")
	last := spans[i][0]
	for _, v := range values {
		if v.to-v.from == 1 && toks[v.from].kind == rtWord && toks[v.from].text == "null" {
			continue
		}
		b.WriteString(q[last:spans[v.from][0]])
		b.WriteString(pgPack + "(" + packSpan(q, toks, spans, v.from, v.to, names) + ")")
		last = spans[v.to-1][1]
	}
	b.WriteString(q[last:spans[end][1]])
	b.WriteString(")")
	return b.String()
}

// packCoalesce is the COALESCE at toks[i:end+1] with each argument packed,
// "" when no argument holds a rewritten round.
func packCoalesce(q string, toks []rtToken, spans [][2]int, i, end int, names map[string]bool) string {
	var args [][2]int
	from, depth := i+2, 0
	for j := i + 2; j <= end; j++ {
		switch {
		case isOpTok(toks, j, "("):
			depth++
		case isOpTok(toks, j, ")") && j < end:
			depth--
		case depth == 0 && (isOpTok(toks, j, ",") || j == end):
			if j <= from {
				return ""
			}
			args = append(args, [2]int{from, j})
			from = j + 1
		}
	}
	held := false
	for _, a := range args {
		if holdsScale(toks[a[0]:a[1]], names) {
			held = true
		}
	}
	if !held {
		return ""
	}
	packed := make([]string, len(args))
	for n, a := range args {
		packed[n] = pgPackN + "(" + packSpan(q, toks, spans, a[0], a[1], names) + ")"
	}
	return pgUnpack + "(" + q[spans[i][0]:spans[i][1]] + "(" + strings.Join(packed, ", ") + "))"
}

// packSpan is the text of toks[from:to] with the CASEs and COALESCEs inside
// it packed, read with the whole statement's names.
func packSpan(q string, toks []rtToken, spans [][2]int, from, to int, names map[string]bool) string {
	base := spans[from][0]
	sub := make([][2]int, to-from)
	for j := from; j < to; j++ {
		sub[j-from] = [2]int{spans[j][0] - base, spans[j][1] - base}
	}
	return packWith(q[base:spans[to-1][1]], toks[from:to], sub, names)
}

// scaleMixRisk names what in a statement with a rewritten round would put
// that round's value beside others in one type, where PostgreSQL keeps each
// value's own decimals and the copy writes them all with the type's: a set
// operation, VALUES or ARRAY, greatest() or least() of a round, nullif() of
// one. "" when nothing, or when no round was rewritten.
func scaleMixRisk(toks []rtToken) string {
	if !anyScaleMacro(toks) {
		return ""
	}
	const why = ", which PostgreSQL writes with each value's own decimals"
	names := scaleNames(toks)
	for i, t := range toks {
		if t.kind != rtWord || isOpTok(toks, i-1, ".") {
			continue
		}
		switch t.text {
		case "union", "intersect", "except", "values", "array":
			return strings.ToUpper(t.text) + " beside a round()" + why
		case "greatest", "least", "nullif", "lag", "lead":
			if !isOpTok(toks, i+1, "(") {
				continue
			}
			end := closingParen(toks, i+1)
			if end < 0 {
				continue
			}
			upTo := end
			if t.text == "lag" || t.text == "lead" {
				// Only a default (the third argument) puts another value
				// beside the one read.
				commas, depth := 0, 0
				for j := i + 2; j < end; j++ {
					if isOpTok(toks, j, "(") {
						depth++
					} else if isOpTok(toks, j, ")") {
						depth--
					} else if depth == 0 && isOpTok(toks, j, ",") {
						commas++
					}
				}
				if commas < 2 {
					continue
				}
			}
			if t.text == "nullif" {
				// nullif answers its first argument, or NULL.
				depth := 0
				for j := i + 2; j < end; j++ {
					if isOpTok(toks, j, "(") {
						depth++
					} else if isOpTok(toks, j, ")") {
						depth--
					} else if depth == 0 && isOpTok(toks, j, ",") {
						upTo = j
						break
					}
				}
			}
			if holdsScale(toks[i+2:upTo], names) {
				return t.text + "() of a round()" + why
			}
		}
	}
	return ""
}

// leftoverCall finds a round or an avg call in text the lexer could not read.
// Any mention counts: a comment or a quote may stand between the name and
// its parenthesis.
var leftoverCall = regexp.MustCompile(`(?i)(round|avg)`)

// aloneRefusal says what in q, as RewriteForCopyAlone wrote it, the copy
// would answer otherwise than PostgreSQL, "" when nothing: a round or an avg
// the rewrite left as written (another shape, DISTINCT, OVER, a $n for k, a
// qualified or quoted name), or one in text the lexer cannot read, or a
// rewritten round beside other values (scaleMixRisk).
func aloneRefusal(q string) string {
	toks, why := lexSQL(q)
	if why != "" {
		if leftoverCall.MatchString(q) {
			return fmt.Sprintf("round() or avg() in a statement whose text the port does not read closely enough (%s)", why)
		}
		return ""
	}
	for i, t := range toks {
		if (t.kind != rtWord && t.kind != rtQuoted) || !isOpTok(toks, i+1, "(") {
			continue
		}
		switch strings.ToLower(t.text) {
		case "round":
			return "round() of this shape (the copy rounds round(x, k), round(x, -k) and round(x) of a numeric, with k written as a whole number up to 15)"
		case "avg":
			return "avg() of this shape (DISTINCT, ORDER BY, FILTER or OVER), whose decimals PostgreSQL writes otherwise"
		}
	}
	return scaleMixRisk(toks)
}
