package pgshim

import (
	"strconv"
	"strings"
)

// The type PostgreSQL gives an expression where the copy's engine gives
// another (#3073). A column is announced with the type the engine answers it
// with (coltypes.go), and for a few functions that is not PostgreSQL's:
// length() is a bigint there and an integer on PostgreSQL, the sum of an
// integer column a number wider than a bigint there and a bigint on
// PostgreSQL. A client that decodes by the announced type reads the value as
// another type than the one it would get from the source.
//
// So the statement is written for the engine with a cast around each call of
// those functions, to PostgreSQL's type for it, wherever it stands: in the
// select list, under an alias, in a subquery or a WITH, in WHERE or GROUP BY,
// inside another expression. The engine then works the type of everything
// built on it out by itself (length(x) + 1, a star over the subquery, a
// Describe), the announced type follows the engine's as it always did, and a
// cell's text and binary forms are still written from one value: the port
// never announces a type and sends the bytes of another.
//
// Only casts that cannot change an answer are written: to a whole number type
// narrower than the engine's, which every use of the value widens again by
// itself and which holds every value the function gives. The table, measured
// against PostgreSQL 16 on the wire in text and in binary (internal/pgcompare
// TestIntegrationPGCompareAnnouncedTypes3073):
//
//   - length, char_length, character_length, position, strpos and ntile are
//     an integer. None can pass one: a text holds less than 2^31 bytes on
//     PostgreSQL, and ntile's argument is an integer.
//   - sum of a smallint or an integer is a bigint (of a bigint it is a
//     numeric, as the engine has it). The argument's type decides, so it is
//     read from the statement (wholeType) and the cast is written only when
//     that is sure; the engine then checks it (sumGuardText). A sum past a
//     bigint is an error on PostgreSQL too.
//
// One rule decides where: a function is cast at every call of it in the
// statement, or at none (off). The same expression in the select list, in
// GROUP BY, in HAVING and in ORDER BY then stays the same expression to the
// engine. No name is given to anything: a column keeps the heading it had.
//
// extract, date_part and a lone NULL, which the engine types otherwise than
// PostgreSQL too, are not in the table: the type PostgreSQL gives them is
// not a narrower whole number, and a value of it read again inside the
// engine (cast to text, divided, handed to a function) can answer otherwise
// or be refused. docs/postgres.md lists them.

// int4Calls are the functions that answer an integer on PostgreSQL and a
// bigint on the copy.
var int4Calls = map[string]bool{
	"length": true, "char_length": true, "character_length": true, "position": true, "strpos": true, "ntile": true,
}

// tableCalls are the functions of this file's table.
var tableCalls = func() map[string]bool {
	m := map[string]bool{"sum": true}
	for f := range int4Calls {
		m[f] = true
	}
	return m
}()

// castWhole are the names of PostgreSQL's whole number types in a cast.
var castWhole = map[string]string{
	"smallint": "int2", "int2": "int2", "int": "int4", "integer": "int4", "int4": "int4", "bigint": "int8", "int8": "int8",
}

// The Describe of a prepared statement asks the copy for the statement's
// columns inside this subquery (describeWith).
const (
	describeHead = "SELECT * FROM (\n"
	describeTail = "\n) AS dbtrail_describe LIMIT 0"
)

// notWholeSum is what the engine answers when a sum this file took for one
// of whole numbers is of something else: an error, never a rounded sum.
const notWholeSum = "the copy cannot tell this sum''s type as PostgreSQL would; cast its argument (sum(x::numeric))"

// sumGuardText is what stands around a sum's argument x, after
// CASE WHEN typeof(x): the whole number types, THEN (x goes at sumGuardThen)
// and the error for anything else.
const (
	sumGuardOpen = "CASE WHEN typeof("
	sumGuardText = ") IN ('TINYINT', 'SMALLINT', 'INTEGER', 'BIGINT') THEN  ELSE error('" + notWholeSum + "') END"
	sumGuardThen = len(") IN ('TINYINT', 'SMALLINT', 'INTEGER', 'BIGINT') THEN ")
)

// The bounds of the rewrite. A statement past any of them is handed to the
// copy as it was written, which is what the port did before this file: its
// columns keep the engine's types, and nothing here can cost more than a
// pass over the statement or write more than the port takes in one.
const (
	// maxCastDepth is how deep casts may stand one inside another
	// (length(length(...))).
	maxCastDepth = 32
	// maxSumArgument is the most tokens of a sum's argument whose type is
	// read from the text.
	maxSumArgument = 64
	// maxTypedTokens is the longest statement, in tokens, whose names are
	// worked out for a sum of a column.
	maxTypedTokens = 4096
)

// selectListEnd are the words that end a select list.
var selectListEnd = map[string]bool{
	"from": true, "where": true, "group": true, "having": true, "order": true, "limit": true, "offset": true,
	"window": true, "fetch": true, "for": true, "union": true, "intersect": true, "except": true,
}

// asPostgresTypes is text, as the connection is about to hand it to the copy
// (copyText), with PostgreSQL's type cast around the calls of this file's
// table. The copy's column types are read only for a statement with a sum to
// decide, and what the connection knows of a busy copy is left as it was.
// When they cannot be read now (a busy copy), the last ones this connection
// read stand in, so the same sum is not a bigint in one answer and a numeric
// in the next.
//
// For a prepared statement (session.screened), a sum is cast only when the
// statement as the client wrote it is a sum of whole numbers. Its arguments
// are written into text as bare numbers, whatever their declared type: a
// bigint 5 reads as an integer there, and a cast to bigint would refuse a
// total that PostgreSQL's numeric holds.
func (s *session) asPostgresTypes(text string) string {
	return asPostgresTypesOf(text, s.screened, func() map[string][]string {
		busy := s.catalogBusy
		types, unread := s.columnTypes(text)
		s.catalogBusy = busy
		if !unread {
			return types
		}
		if s.catalogTables != nil {
			return screenTypes(s.catalogTables, tablesRead(text, s.catalogTables))
		}
		s.logger.Debug("pgshim: the copy's column types could not be read, so a sum of a column is announced with the copy's type (numeric) in this answer")
		return nil
	})
}

// AsPostgresTypes is what the port does to a statement after RewriteForCopy
// or RewriteForCopyAlone, before the copy runs it. types is the copy's
// column types by lowercased name (CopyScreen's); nil when not known, and
// then no sum of a column is decided. For internal/pgcompare.
func AsPostgresTypes(stmt string, types map[string][]string) string {
	return asPostgresTypesOf(stmt, "", func() map[string][]string { return types })
}

func asPostgresTypesOf(stmt, screened string, types func() map[string][]string) string {
	if len(stmt) > maxStatementMessage {
		return stmt
	}
	if body, ok := strings.CutPrefix(stmt, describeHead); ok {
		if body, ok = strings.CutSuffix(body, describeTail); ok {
			return describeHead + asPostgresTypesOf(body, screened, types) + describeTail
		}
	}
	toks, spans, why := lexSQLSpans(stmt)
	if why != "" || len(toks) == 0 || len(spans) != len(toks) {
		return stmt
	}
	if w := firstWord(toks); w != "select" && w != "with" {
		return stmt
	}
	r := &typeRewrite{
		q: stmt, toks: toks, spans: spans, match: matchParens(toks), columns: types,
		off: map[string]bool{}, steps: 16*len(toks) + 4096,
	}
	r.leftAsWritten()
	if screened != "" {
		r.holdSumsTo(screened)
	}
	out := r.render(0, len(toks))
	if r.over || !r.changed {
		return stmt
	}
	out = stmt[:spans[0][0]] + out + stmt[spans[len(toks)-1][1]:]
	if len(out) > maxStatementMessage {
		return stmt
	}
	return out
}

type typeRewrite struct {
	q     string
	toks  []rtToken
	spans [][2]int
	// match is each parenthesis' other end (matchParens): found once, never
	// by walking the statement again.
	match []int
	// columns reads the copy's column types; types are those, without the
	// names the statement may define itself, once a sum asked for them.
	columns func() map[string][]string
	types   map[string][]string
	asked   bool
	// off are the functions of the table left as written at every call in
	// the statement (leftAsWritten).
	off map[string]bool
	// sumOK, when not nil, are the sums that may be cast, by the token their
	// name is at (holdSumsTo).
	sumOK map[int]bool
	// inSum: the argument of a sum is being written, which is written twice.
	inSum bool
	depth int
	// written is how many bytes the casts added, steps how much work the
	// passes over the statement may still do; over, that one of the bounds
	// was passed, and the statement is handed over as written.
	written, steps int
	over           bool
	changed        bool
}

// step counts one unit of work.
func (r *typeRewrite) step() bool {
	if r.steps--; r.steps < 0 {
		r.over = true
	}
	return !r.over
}

// take counts n bytes a cast adds to the statement, and stops the rewrite
// before it writes more than the port takes in one statement.
func (r *typeRewrite) take(n int) bool {
	if r.written += n; len(r.q)+r.written > maxStatementMessage {
		r.over = true
	}
	return !r.over
}

// leftAsWritten finds the functions of the table that are not cast in this
// statement (off). A function is left as written at every call when one call
// of it cannot be cast:
//
//   - a call inside one of the port's own macros (RewriteForCopy), which
//     keep their arguments as measured;
//   - a word that may be a relation's name rather than a call: after WITH,
//     AS, FROM, JOIN or a closing parenthesis (WITH length(n) AS (...),
//     FROM f() AS position(a, b), FROM (...) length(a)), or before AS (...);
//   - a call in an item with no AS of a select list whose columns the engine
//     heads with the item's own text: a subquery's, or one that holds a star.
//     A cast there would show in the heading (CAST(length(t) AS INTEGER)).
//     The statement's own list without a star is not one: the copy heads its
//     columns as PostgreSQL does (sqlsandbox PostgresNames).
func (r *typeRewrite) leftAsWritten() {
	toks := r.toks
	// offIn turns off every function of the table called in toks[from:to].
	offIn := func(from, to int) bool {
		for j := from; j < to; j++ {
			if !r.step() {
				return false
			}
			if toks[j].kind == rtWord && tableCalls[toks[j].text] && isOpTok(toks, j+1, "(") {
				r.off[toks[j].text] = true
			}
		}
		return true
	}
	depth := 0
	for s, t := range toks {
		switch {
		case isOpTok(toks, s, "(") || isOpTok(toks, s, "["):
			depth++
		case isOpTok(toks, s, ")") || isOpTok(toks, s, "]"):
			depth--
		}
		if t.kind != rtWord || !isOpTok(toks, s+1, "(") && t.text != "select" {
			continue
		}
		switch {
		case strings.HasPrefix(t.text, "_bintrail_pg_"):
			if end := r.match[s+1]; end < 0 || !offIn(s+2, end) {
				r.over = true
				return
			}
		case tableCalls[t.text]:
			end := r.match[s+1]
			before := isOpTok(toks, s-1, ")") || s > 0 && toks[s-1].kind == rtWord &&
				(toks[s-1].text == "as" || toks[s-1].text == "with" || toks[s-1].text == "from" || toks[s-1].text == "join")
			after := end >= 0 && end+1 < len(toks) && toks[end+1].kind == rtWord && toks[end+1].text == "as" &&
				(isOpTok(toks, end+2, "(") || end+2 < len(toks) && toks[end+2].kind == rtWord && (toks[end+2].text == "materialized" || toks[end+2].text == "not"))
			if before || after {
				r.off[t.text] = true
			}
		}
		if t.text != "select" {
			continue
		}
		// The list's items, at the list's own level.
		var list [][2]int
		start, i := s+1, s+1
	items:
		for ; i < len(toks); i++ {
			if !r.step() {
				return
			}
			switch {
			case isOpTok(toks, i, "(") || isOpTok(toks, i, "["):
				if r.match[i] < 0 {
					break items
				}
				i = r.match[i]
			case isOpTok(toks, i, ")") || isOpTok(toks, i, "]") || isOpTok(toks, i, ";"):
				break items
			case toks[i].kind == rtWord && toks[i].text == "from" && toks[i-1].kind == rtWord && toks[i-1].text == "distinct" &&
				toks[i-2].kind == rtWord && (toks[i-2].text == "is" || toks[i-2].text == "not"):
				// a IS [NOT] DISTINCT FROM b
			case toks[i].kind == rtWord && selectListEnd[toks[i].text]:
				break items
			case isOpTok(toks, i, ","):
				list = append(list, [2]int{start, i})
				start = i + 1
			}
		}
		list = append(list, [2]int{start, i})
		star := false
		for n, it := range list {
			from := it[0]
			if n == 0 && from < it[1] && toks[from].kind == rtWord && (toks[from].text == "distinct" || toks[from].text == "all") {
				from++
			}
			if from < it[1] && (isOpTok(toks, from, "*") || it[1]-from >= 3 && isOpTok(toks, from+1, ".") && isOpTok(toks, from+2, "*")) {
				star = true
			}
		}
		if depth == 0 && !star {
			continue
		}
		for _, it := range list {
			if it[1]-it[0] >= 3 && toks[it[1]-2].kind == rtWord && toks[it[1]-2].text == "as" {
				continue
			}
			if !offIn(it[0], it[1]) {
				return
			}
		}
	}
}

// render is the text of toks[from:to] with the casts written in. Text is
// only ever cut at a token's edge, so nothing is written into a comment.
func (r *typeRewrite) render(from, to int) string {
	if from >= to {
		return ""
	}
	if r.depth >= maxCastDepth {
		r.over = true
	}
	if r.over {
		return r.q[r.spans[from][0]:r.spans[to-1][1]]
	}
	r.depth++
	defer func() { r.depth-- }()
	var b strings.Builder
	last := r.spans[from][0]
	for i := from; i < to && !r.over; i++ {
		if t := r.toks[i]; t.kind == rtWord && strings.HasPrefix(t.text, "_bintrail_pg_") && isOpTok(r.toks, i+1, "(") {
			// One of the port's own macros: nothing inside it is cast
			// (leftAsWritten turned its calls off).
			if end := r.match[i+1]; end > 0 && end < to {
				i = end
			}
			continue
		}
		repl, end := r.castAt(i, to)
		if end < 0 {
			continue
		}
		b.WriteString(r.q[last:r.spans[i][0]])
		b.WriteString(repl)
		last, i, r.changed = r.spans[end][1], end, true
	}
	b.WriteString(r.q[last:r.spans[to-1][1]])
	return b.String()
}

// castAt is the cast written in place of the call that starts at toks[i] and
// where that ends, or -1 when no call of the table starts there.
func (r *typeRewrite) castAt(i, to int) (string, int) {
	toks := r.toks
	// f(...) of the table, read by its name before anything else is looked
	// for. A function of a schema (my.length) is not PostgreSQL's own, and
	// one written pg_catalog.f never reaches the copy.
	fn := toks[i].text
	if toks[i].kind != rtWord || !tableCalls[fn] || r.off[fn] || !isOpTok(toks, i+1, "(") || isOpTok(toks, i-1, ".") {
		return "", -1
	}
	closeAt := r.match[i+1]
	if closeAt < 0 || closeAt >= to {
		return "", -1
	}
	// With what follows the call and belongs to it: FILTER (...), OVER (...),
	// OVER w.
	end := closeAt
	for end+2 < to && toks[end+1].kind == rtWord && (toks[end+1].text == "filter" || toks[end+1].text == "over") {
		if isOpTok(toks, end+2, "(") {
			c := r.match[end+2]
			if c < 0 || c >= to {
				return "", -1
			}
			end = c
			continue
		}
		if toks[end+1].text != "over" || toks[end+2].kind != rtWord {
			break
		}
		end += 2
	}
	a0 := i + 2
	as, args := "INTEGER", ""
	if fn == "sum" {
		arg, set := a0, ""
		if arg < closeAt && toks[arg].kind == rtWord && (toks[arg].text == "distinct" || toks[arg].text == "all") {
			set = strings.ToUpper(toks[arg].text) + " "
			arg++
		}
		if closeAt-arg > maxSumArgument || r.sumOK != nil && !r.sumOK[i] {
			return "", -1
		}
		if t := wholeType(toks[arg:closeAt], r.columnTypes); t != "int2" && t != "int4" {
			return "", -1
		}
		if r.inSum {
			// A sum of whole numbers inside the argument of another: that
			// argument is written twice, and a statement of them one inside
			// another would double each time. No statement PostgreSQL takes
			// holds one; it is handed over as written.
			r.over = true
			return "", -1
		}
		// The engine checks what was read from the text: the sum of anything
		// but whole numbers must not be cast to one.
		r.inSum = true
		x := r.render(arg, closeAt)
		r.inSum = false
		if !r.take(len(sumGuardOpen) + len(sumGuardText) + len(x)) {
			return "", -1
		}
		args = set + sumGuardOpen + x + sumGuardText[:sumGuardThen] + x + sumGuardText[sumGuardThen:]
		as = "BIGINT"
	}
	if !r.take(len("CAST( AS )") + len(as)) {
		return "", -1
	}
	if args == "" {
		args = r.render(a0, closeAt)
	}
	out := "CAST(" + r.q[r.spans[i][0]:r.spans[i+1][1]] + args + r.render(closeAt, end+1) + " AS " + as + ")"
	if r.over {
		return "", -1
	}
	return out, end
}

// sumCalls are the tokens at which a call of sum starts.
func sumCalls(toks []rtToken) []int {
	var at []int
	for i, t := range toks {
		if t.kind == rtWord && t.text == "sum" && isOpTok(toks, i+1, "(") && !isOpTok(toks, i-1, ".") {
			at = append(at, i)
		}
	}
	return at
}

// holdSumsTo lets a sum be cast only when the same call in screened, the
// statement with its $n still in it, is surely of smallints or integers. A
// $n has no type there, so a sum whose value comes from one keeps the
// engine's type. When the two texts' sums cannot be paired, none is cast.
func (r *typeRewrite) holdSumsTo(screened string) {
	r.sumOK = map[int]bool{}
	here := sumCalls(r.toks)
	if len(here) == 0 {
		return
	}
	toks, why := lexSQL(RewriteForCopy(screened))
	if why != "" {
		return
	}
	there := sumCalls(toks)
	if len(there) != len(here) {
		return
	}
	match := matchParens(toks)
	for k, at := range there {
		closeAt := match[at+1]
		if closeAt < 0 {
			return
		}
		arg := at + 2
		if arg < closeAt && toks[arg].kind == rtWord && (toks[arg].text == "distinct" || toks[arg].text == "all") {
			arg++
		}
		if closeAt-arg > maxSumArgument {
			continue
		}
		if t := wholeType(toks[arg:closeAt], r.columnTypes); t == "int2" || t == "int4" {
			r.sumOK[here[k]] = true
		}
	}
}

// columnTypes are the types of the columns a sum's argument may name: the
// copy's, less every name the statement may define itself, which stands for
// whatever it was defined as. Nil when the copy's are not known, or when the
// statement names columns in a list (AS t(a, b)), which defines names this
// does not read, or is too long to have its names worked out.
func (r *typeRewrite) columnTypes() map[string][]string {
	if r.asked {
		return r.types
	}
	r.asked = true
	types := r.columns()
	if types == nil || len(r.toks) > maxTypedTokens || listsColumns(r.toks, r.match) {
		return nil
	}
	out := make(map[string][]string, len(types))
	for k, v := range types {
		out[k] = v
	}
	// Every name the statement may define: one after AS, or right after a
	// value (SELECT 1.5 i, FROM f(x) i, CASE ... END i, NULL i), and every
	// quoted name. More than the statement defines, never less.
	valueWord := map[string]bool{"as": true, "end": true, "null": true, "true": true, "false": true}
	for i, t := range r.toks {
		switch {
		case t.kind == rtQuoted:
			delete(out, strings.ToLower(t.text))
		case t.kind != rtWord || aliasStop[t.text] || i == 0:
		default:
			prev := r.toks[i-1]
			if prev.kind == rtString || prev.kind == rtNumber || prev.kind == rtQuoted || prev.kind == rtParam || isOpTok(r.toks, i-1, ")") || isOpTok(r.toks, i-1, "]") ||
				prev.kind == rtWord && (valueWord[prev.text] || !aliasStop[prev.text]) {
				delete(out, t.text)
			}
		}
	}
	r.types = out
	return out
}

// wholeType is PostgreSQL's type for e when e is surely a whole number
// there: int2, int4 or int8, "" when it is anything else or not known. It
// reads a whole number written out, a column whose every type (types, read
// when a column is met) is that one, a cast to one, + - * / % between them
// (the wider side's type, as on PostgreSQL), a CASE whose every answer is
// one, and the calls it knows.
func wholeType(e []rtToken, types func() map[string][]string) string {
	for len(e) >= 2 && isOpTok(e, 0, "(") && closingParen(e, 0) == len(e)-1 {
		e = e[1 : len(e)-1]
	}
	if len(e) == 0 {
		return ""
	}
	if e[0].kind == rtWord && e[0].text == "case" && caseEnd(e, 0) == len(e)-1 {
		answers := caseAnswers(e)
		if len(answers) == 0 {
			return ""
		}
		wide := ""
		for _, a := range answers {
			if len(a) == 1 && a[0].kind == rtWord && a[0].text == "null" {
				continue
			}
			if wide = wider(wide, wholeType(a, types), wide == ""); wide == "" {
				return ""
			}
		}
		return wide
	}
	// Operands between the arithmetic operators of the top level; any other
	// operator there makes it something else.
	wide, depth, start, first := "", 0, 0, true
	operand := func(o []rtToken) bool {
		wide = wider(wide, operandType(o, types), first)
		first = false
		return wide != ""
	}
	for i, t := range e {
		switch {
		case isOpTok(e, i, "(") || isOpTok(e, i, "["):
			depth++
		case isOpTok(e, i, ")") || isOpTok(e, i, "]"):
			depth--
		case depth > 0 || t.kind != rtOp || t.text == "::" || t.text == ".":
		case t.text == "+" || t.text == "-" || t.text == "*" || t.text == "/" || t.text == "%":
			if i == start && (t.text == "-" || t.text == "+") {
				// A sign.
				start = i + 1
				continue
			}
			if !operand(e[start:i]) {
				return ""
			}
			start = i + 1
		default:
			return ""
		}
	}
	if !operand(e[start:]) {
		return ""
	}
	return wide
}

// wider is the wider of two whole number types, "" when either is not one;
// first: a is not set yet.
func wider(a, b string, first bool) string {
	if b == "" || !first && a == "" {
		return ""
	}
	if first || b > a {
		// int2 < int4 < int8, as their names sort.
		return b
	}
	return a
}

// operandType is wholeType for one operand: a value, with or without a cast
// after it.
func operandType(o []rtToken, types func() map[string][]string) string {
	n := len(o)
	switch {
	case n == 0:
		return ""
	case n >= 3 && isOpTok(o, n-2, "::") && o[n-1].kind == rtWord:
		// One value before the cast: a name, a number, a call, parentheses.
		v := o[:n-2]
		if len(v) == 1 || len(v) == 3 && isOpTok(v, 1, ".") || isOpTok(v, 0, "(") && closingParen(v, 0) == len(v)-1 ||
			len(v) >= 3 && v[0].kind == rtWord && isOpTok(v, 1, "(") && closingParen(v, 1) == len(v)-1 {
			return castWhole[o[n-1].text]
		}
		return ""
	case isOpTok(o, 0, "(") && closingParen(o, 0) == n-1:
		return wholeType(o, types)
	case n == 1 && o[0].kind == rtNumber:
		v, err := strconv.ParseInt(o[0].text, 10, 64)
		switch {
		case err != nil:
			return ""
		case v <= 1<<31-1:
			return "int4"
		}
		return "int8"
	case n == 1 && o[0].kind == rtWord, n == 3 && o[0].kind == rtWord && isOpTok(o, 1, ".") && o[2].kind == rtWord:
		// A column, with its table before it or not.
		ts := types()[o[n-1].text]
		if len(ts) == 0 {
			return ""
		}
		for _, t := range ts {
			if t != ts[0] {
				return ""
			}
		}
		return map[string]string{"SMALLINT": "int2", "INTEGER": "int4", "BIGINT": "int8"}[ts[0]]
	case n >= 3 && o[0].kind == rtWord && isOpTok(o, 1, "(") && closingParen(o, 1) == n-1:
		switch fn := o[0].text; {
		case int4Calls[fn]:
			return "int4"
		case fn == "count":
			return "int8"
		case fn == "abs":
			return wholeType(o[2:n-1], types)
		case fn == "cast" && n >= 6 && o[n-3].kind == rtWord && o[n-3].text == "as" && o[n-2].kind == rtWord:
			return castWhole[o[n-2].text]
		}
	}
	return ""
}
