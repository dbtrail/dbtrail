package readrouter

import (
	"regexp"
	"strconv"
	"strings"
)

// dateAddMacro is the copy's name for a date stepped forward by an INTERVAL
// (#2800), a macro the SQL worker defines for a client that writes MySQL
// (sqlsandbox's dateAddMacro), as it defines dateSubMacro. The copy computes
// a date past the year 9999 or before the year 1 where MySQL answers NULL;
// the two macros refuse there, and the router sends the statement to MySQL.
const dateAddMacro = "_bintrail_date_add"

// VetoDateAdd is why the routing port keeps on MySQL a statement with a date
// stepped by an INTERVAL that the copy could not be sent inside its macros
// (wrapIntervalSteps, dateAddVeto).
const VetoDateAdd = "a date stepped by an INTERVAL the copy cannot check for a result outside the years 1 to 9999 (NULL on MySQL): " +
	"an INTERVAL first, an amount that is not a number written in the statement, a placeholder, ADDDATE without INTERVAL, " +
	"or an operand the text does not delimit (arithmetic before it, or after the unit)"

// mysqlDateAddCall rewrites MySQL's DATE_ADD(d, INTERVAL n unit) or
// ADDDATE(d, INTERVAL n unit) as the copy's macro over the same two
// arguments, read as mysqlDateSubCall reads DATE_SUB: the same call under
// the other macro. ok is false, and nothing is rewritten, where that one
// would not be.
func mysqlDateAddCall(rest, blRest string) (call string, n int, ok bool) {
	call, n, ok = mysqlDateSubCall(rest, blRest)
	if !ok {
		return "", 0, false
	}
	return dateAddMacro + strings.TrimPrefix(call, dateSubMacro), n, true
}

// dateAddCall opens a DATE_ADD or an ADDDATE call; callsOf keeps out a
// longer name (_bintrail_date_add).
var dateAddCall = regexp.MustCompile(`(?i)(date_add|adddate)[\s\v]*\(`)

// dateAddVeto says why the routing port keeps stmt on MySQL because of a
// date stepped by an INTERVAL, or "" (#2800). shape is Shape(stmt). Each
// DATE_ADD and ADDDATE must be one copyText writes as the macro, with no
// placeholder in it: a prepared statement is rewritten a piece at a time
// between its placeholders (shim copyParts), and a call cut there would
// reach the copy as its own date_add, unchecked. Each + or - before an
// INTERVAL must be one wrapIntervalSteps wraps.
//
// What the copy is given is not checked here: a DATE stepped by a day is a
// date and time on the copy (a known difference, in the docs), and other
// checks keep on MySQL arithmetic on a date (ColumnVeto, nowVeto).
func dateAddVeto(stmt, shape string) string {
	if why := dateAddCallsVeto(shape); why != "" {
		return why
	}
	if _, ok := wrapIntervalSteps(stmt); !ok {
		return VetoDateAdd
	}
	return ""
}

// dateAddCallsVeto is dateAddVeto's half about DATE_ADD and ADDDATE calls,
// which ForCopyOpts refuses too.
func dateAddCallsVeto(shape string) string {
	for _, l := range callsOf(dateAddCall, shape) {
		name := dateAddCall.FindStringSubmatch(shape[l[0]:l[1]])[1]
		if namedByDot(shape, l[0], len(name)) {
			// db.date_add(...): a stored function on MySQL.
			return VetoDateAdd
		}
		rest := shape[l[0]+len(name):]
		_, n, ok := mysqlDateAddCall(rest, rest)
		if !ok || strings.IndexByte(rest[:n], '?') >= 0 {
			return VetoDateAdd
		}
	}
	return ""
}

// wrapIntervalSteps writes each date stepped by an INTERVAL with + or - in
// stmt, MySQL's text, as the copy's macro over the same two operands:
// d + INTERVAL n unit as _bintrail_date_add(d, INTERVAL n unit), and
// d - INTERVAL n unit as _bintrail_date_sub(d, INTERVAL n unit), which are
// d + i and d - i on the copy, refused outside the years 1 to 9999 (#2800).
// The result is still MySQL's text, for ForCopyOpts to write for the copy
// as it writes the rest. Steps are taken from the first one written, so the
// operand of a second step (d + INTERVAL 1 DAY + INTERVAL 1 HOUR) is the
// first one's macro.
//
// The copy's answer must not change but where the macro refuses, so a step
// is wrapped only where the text says exactly what its operand is, as the
// copy reads it: a name (qualified or quoted), a number, a string literal
// (with DATE, TIME or TIMESTAMP before it), or a call or a group in
// parentheses; with before it nothing, an opening parenthesis, a comma, a
// comparison or a word that binds looser than + (SELECT, WHERE, AND, OR,
// NOT, BETWEEN, WHEN, THEN, ELSE, CASE, DISTINCT, ON, BY, ...); and after the
// unit nothing that binds tighter than + (*, /, %, ^, DIV, MOD). The amount
// must be a number or a string literal written in the statement: the copy
// refuses a sign before a number, and a placeholder or an expression is the
// veto list's (vetoIntervalExpr). An INTERVAL first (INTERVAL 1 DAY + d)
// is not wrapped either. ok is false at the first step that cannot be
// wrapped: the routing port then keeps the statement on MySQL.
//
// A step of NOW(), CURDATE(), UTC_TIMESTAMP(), UTC_DATE() or a clock keyword
// by an amount written in the statement that cannot leave the years 1 to
// 9999 (clockStepSafe) is left as written.
func wrapIntervalSteps(stmt string) (string, bool) {
	for {
		sc := scan(stmt)
		if sc.refusal != "" || sc.hash {
			// The statement is refused for its text before this matters.
			return stmt, true
		}
		bl, at := sc.blanked, sc.blankedAt
		wrapped := false
		for _, w := range intervalWords(bl) {
			amount, unit, end, ok := intervalStep(bl, w)
			if !ok {
				return stmt, false
			}
			sign := lastNonSpace(bl, w)
			if sign < 0 || bl[sign] != '+' && bl[sign] != '-' {
				// An argument (DATE_ADD's second), or an INTERVAL first.
				if f := firstNonSpace(bl, end); f < len(bl) && (bl[f] == '+' || bl[f] == '-') {
					return stmt, false
				}
				continue
			}
			if !looserAfter(bl, end) {
				return stmt, false
			}
			s, e, ok := operandEndingAt(bl, sign)
			if !ok {
				return stmt, false
			}
			if clockStepSafe(bl[s:e], stmt[at[amount[0]]:at[amount[1]]], bl[unit[0]:unit[1]]) && !stepFollows(bl, end) {
				continue
			}
			macro := dateAddMacro
			if bl[sign] == '-' {
				macro = dateSubMacro
			}
			S, E, I, U := at[s], at[e], at[w], at[end]
			stmt = stmt[:S] + macro + "(" + stmt[S:E] + ", " + stmt[I:U] + ")" + stmt[U:]
			wrapped = true
			break
		}
		if !wrapped {
			return stmt, true
		}
	}
}

// intervalWords are where the word INTERVAL starts in bl, a blanked text:
// read as a word, not inside a quoted name, and not next to a dot (a name).
func intervalWords(bl string) []int {
	const kw = "interval"
	var out []int
	for i := 0; i+len(kw) <= len(bl); i++ {
		if !strings.EqualFold(bl[i:i+len(kw)], kw) {
			continue
		}
		if i > 0 && (wordByte(bl[i-1]) || bl[i-1] == '`') || i+len(kw) < len(bl) && (wordByte(bl[i+len(kw)]) || bl[i+len(kw)] == '`') {
			continue
		}
		if namedByDot(bl, i, len(kw)) {
			continue
		}
		out = append(out, i)
	}
	return out
}

// intervalStep reads INTERVAL amount unit at w in bl: the amount a number
// (digits, a decimal point) or a string literal (two quotes once blanked), and the
// unit one word. It returns where each is in bl and where the step ends.
func intervalStep(bl string, w int) (amount, unit [2]int, end int, ok bool) {
	i := w + len("interval")
	j := firstNonSpace(bl, i)
	switch {
	case j < len(bl) && bl[j] == '\'':
		if j+1 >= len(bl) || bl[j+1] != '\'' {
			return amount, unit, 0, false
		}
		amount = [2]int{j, j + 2}
	case j > i && j < len(bl) && (asciiDigit(bl[j]) || bl[j] == '.'):
		k := j
		for k < len(bl) && (asciiDigit(bl[k]) || bl[k] == '.') {
			k++
		}
		if k < len(bl) && wordByte(bl[k]) {
			return amount, unit, 0, false
		}
		amount = [2]int{j, k}
	default:
		return amount, unit, 0, false
	}
	u := firstNonSpace(bl, amount[1])
	k := u
	for k < len(bl) && (bl[k] == '_' || 'a' <= bl[k]|0x20 && bl[k]|0x20 <= 'z') {
		k++
	}
	if k == u || k < len(bl) && wordByte(bl[k]) {
		return amount, unit, 0, false
	}
	return amount, [2]int{u, k}, k, true
}

// looserAfter reports whether what follows a step's unit at end binds
// looser than + on the copy, so that the step's value is what it is
// stepped into: not *, /, %, ^, a cast (::), a subscript, a placeholder, DIV,
// MOD or COLLATE.
func looserAfter(bl string, end int) bool {
	f := firstNonSpace(bl, end)
	if f == len(bl) {
		return true
	}
	if strings.IndexByte("*/%^[:?", bl[f]) >= 0 {
		return false
	}
	if wordByte(bl[f]) {
		k := f
		for k < len(bl) && wordByte(bl[k]) {
			k++
		}
		switch strings.ToLower(bl[f:k]) {
		case "div", "mod", "collate":
			return false
		}
	}
	return true
}

// operandEndingAt is the operand of the + or - at sign in bl: where it
// starts and ends. ok is false unless operandStart delimits it and what
// stands before it binds looser than + (looserBefore).
func operandEndingAt(bl string, sign int) (s, e int, ok bool) {
	q := sign - 1
	for q >= 0 && sqlSpace(bl[q]) {
		q--
	}
	if q < 0 {
		return 0, 0, false
	}
	s, ok = operandStart(bl, q)
	if !ok || strings.IndexByte(bl[s:q+1], '?') >= 0 || !looserBefore(bl, s) {
		return 0, 0, false
	}
	return s, q + 1, true
}

// notOperands are words that end no operand: what stands before them is
// another clause, or (END, a THEN's value) more than the text delimits.
var notOperands = wordSet("select", "where", "and", "or", "xor", "not", "on", "having", "when", "then", "else", "end",
	"between", "by", "case", "in", "is", "like", "as", "distinct", "all", "any", "some", "exists", "interval", "from",
	"join", "union", "limit", "order", "group", "with", "regexp", "rlike", "div", "mod", "escape", "binary", "collate")

// looserWords are words after which an operand starts: they bind looser
// than + on both sides.
var looserWords = wordSet("select", "where", "and", "or", "xor", "not", "on", "having", "when", "then", "else",
	"between", "case", "distinct", "all", "by")

// operandStart is where the operand that ends at q (its last byte) starts:
// a word (a name, a number, NULL), a quoted name, a string literal (two quotes
// once blanked, with the type word of a typed literal or a prefix before it), or
// a group in parentheses with the name of a call right before it; each with
// any qualifier before a dot.
func operandStart(bl string, q int) (int, bool) {
	s, ok := atomStart(bl, q)
	if !ok {
		return 0, false
	}
	if bl[q] == '\'' {
		// A prefix glued to it (_utf8mb4'..', N'..', X'..'), or the type of
		// a typed literal (DATE '..'). Another literal before it would be
		// part of the same string on MySQL.
		if s > 0 && wordByte(bl[s-1]) {
			k := s
			for k > 0 && wordByte(bl[k-1]) {
				k--
			}
			s = k
		} else if p := lastNonSpace(bl, s); p >= 0 && wordByte(bl[p]) {
			k := p + 1
			for k > 0 && wordByte(bl[k-1]) {
				k--
			}
			switch strings.ToLower(bl[k : p+1]) {
			case "date", "time", "timestamp":
				s = k
			}
		} else if p >= 0 && bl[p] == '\'' {
			return 0, false
		}
		return s, true
	}
	if wordByte(bl[q]) && notOperands[strings.ToLower(bl[s:q+1])] {
		return 0, false
	}
	// db.t.col, t.`col`, db.f(x).
	for s > 0 && bl[s-1] == '.' {
		if s-2 < 0 || !(wordByte(bl[s-2]) || bl[s-2] == '`') {
			return 0, false
		}
		if s, ok = atomStart(bl, s-2); !ok {
			return 0, false
		}
	}
	return s, true
}

// atomStart is where the word, quoted name, blanked string or parenthesized
// group whose last byte is at q starts. A group takes the name of a call
// glued to it.
func atomStart(bl string, q int) (int, bool) {
	switch c := bl[q]; {
	case c == ')':
		depth := 0
		for p := q; p >= 0; p-- {
			switch bl[p] {
			case ')':
				depth++
			case '(':
				depth--
			}
			if depth == 0 {
				for p > 0 && wordByte(bl[p-1]) {
					p--
				}
				return p, true
			}
		}
		return 0, false
	case c == '`':
		p := strings.LastIndexByte(bl[:q], '`')
		return p, p >= 0
	case c == '\'':
		if q < 1 || bl[q-1] != '\'' {
			return 0, false
		}
		return q - 1, true
	case wordByte(c):
		p := q
		for p > 0 && wordByte(bl[p-1]) {
			p--
		}
		return p, true
	}
	return 0, false
}

// looserBefore reports whether what stands before an operand starting at s
// binds looser than +: nothing, an opening parenthesis, a comma, a
// comparison, or one of looserWords not named by a dot.
func looserBefore(bl string, s int) bool {
	p := lastNonSpace(bl, s)
	if p < 0 {
		return true
	}
	switch c := bl[p]; {
	case c == '(' || c == ',' || c == '=' || c == '<' || c == '>':
		return true
	case wordByte(c):
		k := p + 1
		for k > 0 && wordByte(bl[k-1]) {
			k--
		}
		return looserWords[strings.ToLower(bl[k:p+1])] && !namedByDot(bl, k, p+1-k)
	}
	return false
}

// clockCallOperand is NOW(), CURDATE(), UTC_TIMESTAMP(), UTC_DATE() or a
// clock keyword, written with or without its parentheses, once spaces are
// removed and the text lowercased.
var clockCallOperand = regexp.MustCompile(`^(now\(\)|curdate\(\)|utc_timestamp\(\)|utc_date\(\)|(current_timestamp|current_date|localtime|localtimestamp)(\(\))?)$`)

// clockSafe is, per unit, the largest step that keeps a clock of the years
// 1001 to 8999 inside the years 1 to 9999: a thousand years of each.
var clockSafe = map[string]int64{"second": 31_536_000_000, "minute": 525_600_000, "hour": 8_760_000, "day": 365_000,
	"week": 52_000, "month": 12_000, "quarter": 4_000, "year": 1_000}

// clockStepSafe reports whether a step of operand (bl text) by amount (the
// statement's text: digits, or a quoted whole number) in unit cannot leave
// the years 1 to 9999: operand a clock call, and the amount at most a
// thousand years. Such a step is sent as written, which keeps the commonest
// report filter (dt >= NOW() - INTERVAL 30 DAY) the text it was.
func clockStepSafe(operand, amount, unit string) bool {
	op := strings.ToLower(strings.Join(strings.Fields(operand), ""))
	if !clockCallOperand.MatchString(op) {
		return false
	}
	amount = strings.TrimSpace(amount)
	if len(amount) >= 2 && amount[0] == '\'' && amount[len(amount)-1] == '\'' {
		amount = strings.TrimSpace(amount[1 : len(amount)-1])
	}
	n, err := strconv.ParseInt(amount, 10, 64)
	limit, known := clockSafe[strings.ToLower(unit)]
	return err == nil && known && n >= 0 && n <= limit
}

// stepFollows reports whether another step starts right after end: a + or a
// - and the word INTERVAL. A step left as written would be read as the
// operand of that one, which the text does not delimit.
func stepFollows(bl string, end int) bool {
	f := firstNonSpace(bl, end)
	if f == len(bl) || bl[f] != '+' && bl[f] != '-' {
		return false
	}
	g := firstNonSpace(bl, f+1)
	return g+len("interval") <= len(bl) && strings.EqualFold(bl[g:g+len("interval")], "interval") &&
		(g+len("interval") == len(bl) || !wordByte(bl[g+len("interval")]))
}

// lastNonSpace is the last byte before i in bl that is not white space, or
// -1.
func lastNonSpace(bl string, i int) int {
	p := i - 1
	for p >= 0 && sqlSpace(bl[p]) {
		p--
	}
	return p
}

// firstNonSpace is the first byte from i in bl that is not white space, or
// len(bl).
func firstNonSpace(bl string, i int) int {
	for i < len(bl) && sqlSpace(bl[i]) {
		i++
	}
	return i
}
