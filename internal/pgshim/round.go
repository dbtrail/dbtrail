package pgshim

import (
	"regexp"
	"strconv"
	"strings"
)

// copyText is text as this connection hands it to the copy: rewritten
// (RewriteForCopy) when it can send a statement to its source, since a
// round the copy then refuses goes there; otherwise only the copy's own
// words are quoted, since there the copy answers every statement and a
// refusal would be an error the client sees (a round of two whole numbers
// is a common one).
func (s *session) copyText(text string) string {
	if s.forwarding() {
		return RewriteForCopy(text)
	}
	// A word the copy keeps for itself and PostgreSQL reads as a name is
	// quoted on every connection: that turns the copy's syntax error into
	// PostgreSQL's answer, and refuses nothing it answered before.
	return quoteCopyWords(text)
}

// RewriteForCopy writes stmt for the copy of a PostgreSQL source: each
// round(a / b, k) and round(avg(x), k), round(10^j * avg(x), k) among them,
// becomes the copy's macro for it (sqlsandbox pgRoundMacros), which rounds
// as PostgreSQL rounds a numeric and refuses what it cannot. The rest of
// the text is left as written, byte for byte, and so is a round of any
// other shape: the screen then sends it to the source (round is not among
// copyFuncs). Every statement the port hands the copy goes through it, and
// so does the screen (CopyScreen, routeFree), so what is screened is what
// runs.
func RewriteForCopy(stmt string) string {
	out, _ := rewriteRounds(quoteCopyWords(stmt))
	return out
}

// The macros the rewrite writes, which the screen lets through.
const (
	pgRoundDiv         = "_bintrail_pg_round_div"
	pgRoundAvg         = "_bintrail_pg_round_avg"
	pgFloorDiv         = "_bintrail_pg_floor_div"
	pgEpochMacro       = "_bintrail_pg_epoch"
	pgToTimestampMacro = "_bintrail_pg_to_timestamp"
)

// maxRoundDecimals is the most decimals a rewritten round may ask for, the
// shift of round(10^j * avg(x), k) included: PostgreSQL's division keeps at
// least 16 decimals of a quotient under 1, so the macro refuses 16 and past
// it (l - kk < 1), and a unit of 16 decimals or more is a number the lexer
// refuses (more than 18 characters).
const maxRoundDecimals = 15

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

func rewriteRounds(q string) (string, bool) {
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
		var call func(string, []rtToken, [][2]int, int, int) (string, bool)
		switch toks[i].text {
		case "round":
			call = roundCall
		case "floor":
			call = floorCall
		case "extract":
			call = epochCall
		case "to_timestamp":
			call = toTimestampCall
		default:
			continue
		}
		end := closingParen(toks, i+1)
		if end < 0 {
			continue
		}
		repl, ok := call(q, toks, spans, i+2, end)
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
func roundCall(q string, toks []rtToken, spans [][2]int, from, to int) (string, bool) {
	// Two arguments: round(x) of a numeric is another rounding, and k must
	// be written as a whole number (not $1, not -1, not 2.0).
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
	if comma <= from || comma+2 != to || toks[comma+1].kind != rtNumber || strings.Trim(toks[comma+1].text, "0123456789") != "" {
		return "", false
	}
	k, err := strconv.Atoi(toks[comma+1].text)
	if err != nil || k > maxRoundDecimals {
		return "", false
	}
	text := func(a, b int) string { return q[spans[a][0]:spans[b-1][1]] }
	unit := "1"
	if k > 0 {
		unit = "0." + strings.Repeat("0", k-1) + "1"
	}
	a0, a1 := from, comma
	// date_part is a double precision on PostgreSQL, where round(x, k) of
	// one fails, and a whole number on the copy, where the macro would
	// round it.
	for i := a0; i < a1; i++ {
		if toks[i].kind == rtWord && toks[i].text == "date_part" && isOpTok(toks, i+1, "(") {
			return "", false
		}
	}

	// round(avg(x), k) and round(10^j * avg(x), k).
	at, shift := a0, 0
	if a1-a0 > 2 && toks[a0].kind == rtNumber && isOpTok(toks, a0+1, "*") {
		m := powerOfTen.FindStringSubmatch(toks[a0].text)
		if m == nil {
			return roundDivision(q, toks, spans, a0, a1, k, unit, text)
		}
		at, shift = a0+2, len(m[1])
	}
	if toks[at].kind == rtWord && toks[at].text == "avg" && isOpTok(toks, at+1, "(") && !isOpTok(toks, at-1, ".") {
		if closingParen(toks, at+1) != a1-1 || a1-1 == at+2 {
			// Something after avg(...) (OVER, FILTER, an operator), or avg().
			return roundDivision(q, toks, spans, a0, a1, k, unit, text)
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
		inner, _ := rewriteRounds(text(inner0, inner1))
		return pgRoundAvg + "(" + inner + ", " + strconv.Itoa(k+shift) + ", " + unit + ")", true
	}
	return roundDivision(q, toks, spans, a0, a1, k, unit, text)
}

// roundDivision is the macro call for round(P / Q, k), where P is a product
// of values and Q one value: the division is the last thing done, as
// PostgreSQL reads a * b / c ((a * b) / c). Anything else at the top of the
// argument (a + b / c, a / b * c, a / b / c, -a / b, a cast with ::) is not.
func roundDivision(q string, toks []rtToken, spans [][2]int, a0, a1, k int, unit string, text func(int, int) string) (string, bool) {
	p, d, ok := splitDivision(toks, a0, a1, text)
	if !ok {
		return "", false
	}
	return pgRoundDiv + "(" + p + ", " + d + ", " + strconv.Itoa(k) + ", " + unit + ")", true
}

// floorCall is the macro call for floor(<toks[from:to]>) of a division,
// false for any other floor: floor(x) of a numeric is already PostgreSQL's
// on the copy, but floor of a float or a column is not something this
// port has measured, and it stays with the source.
func floorCall(q string, toks []rtToken, spans [][2]int, from, to int) (string, bool) {
	if from >= to {
		return "", false
	}
	text := func(a, b int) string { return q[spans[a][0]:spans[b-1][1]] }
	for i := from; i < to; i++ {
		if toks[i].kind == rtWord && toks[i].text == "date_part" && isOpTok(toks, i+1, "(") {
			return "", false
		}
	}
	p, d, ok := splitDivision(toks, from, to, text)
	if !ok {
		return "", false
	}
	return pgFloorDiv + "(" + p + ", " + d + ")", true
}

// epochCall is the macro call for extract(epoch from <x>), false for any
// other field: the screen lets year, month and day through as they are.
func epochCall(q string, toks []rtToken, spans [][2]int, from, to int) (string, bool) {
	if to-from < 3 || toks[from].kind != rtWord || toks[from].text != "epoch" || toks[from+1].kind != rtWord || toks[from+1].text != "from" {
		return "", false
	}
	x, _ := rewriteRounds(q[spans[from+2][0]:spans[to-1][1]])
	return pgEpochMacro + "(" + x + ")", true
}

// toTimestampCall is the macro call for to_timestamp(<x>) of one argument
// (to_timestamp(text, format) is another function).
func toTimestampCall(q string, toks []rtToken, spans [][2]int, from, to int) (string, bool) {
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
	x, _ := rewriteRounds(q[spans[from][0]:spans[to-1][1]])
	return pgToTimestampMacro + "(" + x + ")", true
}

// splitDivision reads toks[a0:a1] as P / Q, where P is a product of values
// and Q one value, and gives P and Q as written (each rewritten in turn).
func splitDivision(toks []rtToken, a0, a1 int, text func(int, int) string) (string, string, bool) {
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
	p, _ := rewriteRounds(text(a0, slash))
	d, _ := rewriteRounds(text(slash+1, a1))
	return p, d, true
}
