package pgshim

import (
	"strconv"
	"strings"
)

// The type PostgreSQL gives an expression where the copy's engine gives
// another (#3073). A column is announced with the type the engine answers it
// with (coltypes.go), and for a few expressions that is not PostgreSQL's:
// length() is a bigint there and an integer on PostgreSQL, the sum of an
// integer column a number wider than a bigint there and a bigint on
// PostgreSQL. A client that decodes by the announced type reads the value as
// another type than the one it would get from the source.
//
// So the statement is written for the engine with a cast around each such
// expression, to PostgreSQL's type for it, wherever it stands: in the select
// list, under an alias, in a subquery or a WITH, inside another expression
// (extract and date_part only as a whole item of the select list).
// The engine then works the type of everything built on it out by itself
// (length(x) + 1, a star over the subquery, a Describe), the announced type
// follows the engine's as it always did, and a cell's text and binary forms
// are still written from one value: the port never announces a type and
// sends the bytes of another. A value the cast cannot hold is an error of
// the statement, never another value.
//
// The table, measured against PostgreSQL 16 on the wire in text and in
// binary (internal/pgcompare TestIntegrationPGCompareAnnouncedTypes3073):
//
//   - length, char_length, character_length, position, strpos and ntile are
//     an integer. None can pass one: a text holds less than 2^31 bytes on
//     PostgreSQL, and ntile's argument is an integer.
//   - sum of a smallint or an integer is a bigint (of a bigint it is a
//     numeric, as the engine has it). The argument's type decides, so it is
//     read from the statement (wholeType) and the cast is written only when
//     that is sure. A sum past a bigint is an error on PostgreSQL too.
//   - extract of a field that is a whole number (year, month, day, hour...)
//     is a numeric with no decimals, and date_part of one a double
//     precision, each where it is a whole item of the statement's own
//     select list. Fields with a fraction (second, epoch, milliseconds) are left as they
//     are: PostgreSQL writes them with decimals the engine's whole number
//     does not hold.
//   - NULL as a whole item of the select list is text.

// int4Calls are the functions that answer an integer on PostgreSQL and a
// bigint on the copy.
var int4Calls = map[string]bool{
	"length": true, "char_length": true, "character_length": true, "position": true, "strpos": true, "ntile": true,
}

// wholeFields are the fields of extract and date_part that are whole
// numbers on PostgreSQL and on the copy alike.
var wholeFields = map[string]bool{
	"year": true, "month": true, "day": true, "hour": true, "minute": true, "quarter": true, "dow": true, "doy": true,
	"week": true, "isodow": true, "isoyear": true, "decade": true, "century": true, "millennium": true,
}

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

// textNull is NULL as a whole select item, which is text on PostgreSQL;
// unnamedNull is one the statement gives no name, under the heading
// PostgreSQL gives it (the engine would head it with the cast's own text).
const (
	textNull    = "CAST(NULL AS VARCHAR)"
	unnamedNull = textNull + ` AS "?column?"`
)

// notWholeSum is what the engine answers when a sum this file took for one
// of whole numbers is of something else: an error, never a rounded sum.
const notWholeSum = "the copy cannot tell this sum''s type as PostgreSQL would; cast its argument (sum(x::numeric))"

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

// tableCalls are the functions of this file's table.
var tableCalls = func() map[string]bool {
	m := map[string]bool{"sum": true, "extract": true, "date_part": true}
	for f := range int4Calls {
		m[f] = true
	}
	return m
}()

// selectListEnd are the words that end a select list.
var selectListEnd = map[string]bool{
	"from": true, "where": true, "group": true, "having": true, "order": true, "limit": true, "offset": true,
	"window": true, "fetch": true, "for": true, "union": true, "intersect": true, "except": true,
}

// asPostgresTypes is text, as the connection is about to hand it to the copy
// (copyText), with PostgreSQL's type cast around the expressions of this
// file's table. The copy's column types are read only for a statement with a
// sum to decide, and what the connection knows of a busy copy is left as it
// was. When they cannot be read now (a busy copy), the last ones this
// connection read stand in, so the same sum is not a bigint in one answer
// and a numeric in the next.
func (s *session) asPostgresTypes(text string) string {
	return asPostgresTypes(text, func() map[string][]string {
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
	return asPostgresTypes(stmt, func() map[string][]string { return types })
}

func asPostgresTypes(stmt string, types func() map[string][]string) string {
	if len(stmt) > maxStatementMessage {
		return stmt
	}
	if body, ok := strings.CutPrefix(stmt, describeHead); ok {
		if body, ok = strings.CutSuffix(body, describeTail); ok {
			return describeHead + asPostgresTypes(body, types) + describeTail
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
		off: map[string]bool{}, alias: map[int]string{},
		room: maxStatementMessage - len(stmt), steps: 16*len(toks) + 4096,
	}
	var combined bool
	r.items, combined = selectItems(toks)
	if !combined {
		r.nulls = loneNulls(toks, r.items)
	}
	r.insideMacros()
	r.nameItems()
	out := r.render(0, len(toks))
	if r.over || !r.changed {
		return stmt
	}
	return stmt[:spans[0][0]] + out + stmt[spans[len(toks)-1][1]:]
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
	// items are the places where an item of the statement's own select list
	// starts; nulls what to write for a NULL that is one (loneNulls).
	items map[int]bool
	nulls map[int]string
	// off are the functions of the table left as written everywhere in the
	// statement (insideMacros, nameItems): cast in one place and not in
	// another, an expression would no longer be the one GROUP BY names.
	off map[string]bool
	// alias is the name to give a cast that is a whole select item with no
	// name of its own, by where the call starts (nameItems).
	alias map[int]string
	// inSum: the argument of a sum is being written, which is written twice.
	inSum bool
	depth int
	// room is how many bytes the casts may still add, steps how much work
	// the passes over the statement may still do; over, that one of the
	// bounds was passed, and the statement is handed over as written.
	room, steps int
	over        bool
	changed     bool
}

// step counts one unit of work.
func (r *typeRewrite) step() bool {
	if r.steps--; r.steps < 0 {
		r.over = true
	}
	return !r.over
}

// take counts n bytes a cast adds to the statement.
func (r *typeRewrite) take(n int) bool {
	if r.room -= n; r.room < 0 {
		r.over = true
	}
	return !r.over
}

// insideMacros turns off the functions of the table called inside one of the
// port's own macros (RewriteForCopy), which keep their arguments as measured.
func (r *typeRewrite) insideMacros() {
	toks := r.toks
	for i := 0; i < len(toks); i++ {
		if toks[i].kind != rtWord || !strings.HasPrefix(toks[i].text, "_bintrail_pg_") || !isOpTok(toks, i+1, "(") {
			continue
		}
		end := r.match[i+1]
		if end < 0 {
			end = len(toks) - 1
		}
		for j := i + 2; j < end; j++ {
			if toks[j].kind == rtWord && tableCalls[toks[j].text] && isOpTok(toks, j+1, "(") {
				r.off[toks[j].text] = true
			}
		}
		i = end
	}
}

// nameItems keeps the heading of every column as it was, or as PostgreSQL
// has it. The engine heads an item the statement gives no name with the
// item's own text, so a cast written into one would show in the heading
// (CAST(length(t) AS INTEGER)). In the statement's own select list the copy
// renames such columns the way PostgreSQL does (sqlsandbox PostgresNames),
// but not when the list holds a star, and never in a subquery's list, whose
// names a star outside it shows. So in those lists:
//
//   - an item with AS, or a call of the table followed by a bare alias, is
//     cast as anywhere else;
//   - an item that is one call of the table and nothing else is cast and
//     given the function's name, which is what PostgreSQL heads it with
//     (alias), the first of each name in a list;
//   - any other item is left as written, and so is every call, anywhere in
//     the statement, of the functions it calls (off).
func (r *typeRewrite) nameItems() {
	toks := r.toks
	depth := 0
	for s, t := range toks {
		switch {
		case isOpTok(toks, s, "(") || isOpTok(toks, s, "["):
			depth++
		case isOpTok(toks, s, ")") || isOpTok(toks, s, "]"):
			depth--
		}
		if t.kind != rtWord || t.text != "select" {
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
			case toks[i].kind == rtWord && selectListEnd[toks[i].text]:
				break items
			case isOpTok(toks, i, ","):
				list = append(list, [2]int{start, i})
				start = i + 1
			}
		}
		list = append(list, [2]int{start, i})
		if first := &list[0]; first[0] < first[1] && toks[first[0]].kind == rtWord && (toks[first[0]].text == "distinct" || toks[first[0]].text == "all") {
			first[0]++
		}
		star := false
		for _, it := range list {
			if it[0] < it[1] && (isOpTok(toks, it[0], "*") || it[1]-it[0] >= 3 && isOpTok(toks, it[0]+1, ".") && isOpTok(toks, it[0]+2, "*")) {
				star = true
			}
		}
		if depth == 0 && !star {
			continue
		}
		seen := map[string]bool{}
		for _, it := range list {
			from, to := it[0], it[1]
			if to-from >= 3 && toks[to-2].kind == rtWord && toks[to-2].text == "as" {
				continue
			}
			if from < to && toks[from].kind == rtWord && tableCalls[toks[from].text] && isOpTok(toks, from+1, "(") {
				switch end := r.callEnd(from, to); {
				case end == to-1 && !seen[toks[from].text]:
					seen[toks[from].text] = true
					r.alias[from] = toks[from].text
					continue
				case end >= 0 && end == to-2 && (toks[to-1].kind == rtWord || toks[to-1].kind == rtQuoted):
					continue
				}
			}
			for j := from; j < to; j++ {
				if !r.step() {
					return
				}
				if toks[j].kind == rtWord && tableCalls[toks[j].text] && isOpTok(toks, j+1, "(") {
					r.off[toks[j].text] = true
				}
			}
		}
	}
}

// callEnd is where the call that starts at toks[name] ends, with what
// follows it and belongs to it (FILTER (...), OVER (...), OVER w); -1 when
// it does not end before to.
func (r *typeRewrite) callEnd(name, to int) int {
	toks := r.toks
	end := r.match[name+1]
	if end < 0 || end >= to {
		return -1
	}
	for end+2 < to && toks[end+1].kind == rtWord && (toks[end+1].text == "filter" || toks[end+1].text == "over") {
		if isOpTok(toks, end+2, "(") {
			c := r.match[end+2]
			if c < 0 || c >= to {
				return -1
			}
			end = c
			continue
		}
		if toks[end+1].text != "over" || toks[end+2].kind != rtWord {
			break
		}
		end += 2 // OVER w
	}
	return end
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
			// One of the port's own macros (RewriteForCopy): measured with
			// its arguments as they are.
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

// castAt is the cast written in place of what starts at toks[i] and where
// that ends, or -1 when nothing of the table starts there.
func (r *typeRewrite) castAt(i, to int) (string, int) {
	toks := r.toks
	if null, ok := r.nulls[i]; ok && r.take(len(null)) {
		return null, i
	}
	// f(...) of the table, read by its name before anything else is looked
	// for: a function of a schema (my.length) is not PostgreSQL's own, and
	// one written pg_catalog.f never reaches the copy.
	fn := toks[i].text
	if toks[i].kind != rtWord || !tableCalls[fn] || r.off[fn] || !isOpTok(toks, i+1, "(") || isOpTok(toks, i-1, ".") {
		return "", -1
	}
	closeAt := r.match[i+1]
	end := r.callEnd(i, to)
	if end < 0 {
		return "", -1
	}
	// A relation that has a function's name: WITH length(n) AS (...),
	// FROM f() AS position(a, b), FROM (...) length(a).
	if isOpTok(toks, i-1, ")") ||
		i > 0 && toks[i-1].kind == rtWord && (toks[i-1].text == "as" || toks[i-1].text == "with" || toks[i-1].text == "from" || toks[i-1].text == "join") ||
		closeAt+2 < len(toks) && toks[closeAt+1].kind == rtWord && toks[closeAt+1].text == "as" &&
			(isOpTok(toks, closeAt+2, "(") || toks[closeAt+2].kind == rtWord && (toks[closeAt+2].text == "materialized" || toks[closeAt+2].text == "not")) {
		return "", -1
	}
	// extract and date_part only as a whole item of the statement's own
	// select list, where nothing in the engine reads the value again. Read
	// again, the cast could change an answer or refuse a statement the copy
	// took: the engine writes a double precision as text with a decimal
	// (5.0, where PostgreSQL writes 5), and a function that takes a bigint
	// (make_date) does not take the wider whole number extract is cast to.
	whole := r.items[i] && endsItem(toks, end+1)
	a0 := i + 2
	as, args := "", ""
	switch {
	case int4Calls[fn]:
		as = "INTEGER"
	case fn == "extract":
		// extract(year FROM x), extract('year' FROM x). A whole number wider
		// than a bigint is announced as a numeric with no precision, which is
		// how PostgreSQL announces extract, and divides as the bigint did.
		if whole && closeAt-a0 >= 3 && (toks[a0].kind == rtWord || toks[a0].kind == rtString) && wholeFields[strings.ToLower(toks[a0].text)] &&
			toks[a0+1].kind == rtWord && toks[a0+1].text == "from" {
			as = "HUGEINT"
		}
	case fn == "date_part":
		if whole && closeAt-a0 >= 3 && toks[a0].kind == rtString && wholeFields[strings.ToLower(toks[a0].text)] && isOpTok(toks, a0+1, ",") {
			as = "DOUBLE"
		}
	case fn == "sum":
		arg, set := a0, ""
		if arg < closeAt && toks[arg].kind == rtWord && (toks[arg].text == "distinct" || toks[arg].text == "all") {
			set = strings.ToUpper(toks[arg].text) + " "
			arg++
		}
		// Not a sum inside another's argument: the argument is written
		// twice, and so would each one inside it be, twice over.
		if r.inSum || closeAt-arg > maxSumArgument {
			break
		}
		if t := wholeType(toks[arg:closeAt], r.columnTypes); t == "int2" || t == "int4" {
			// The engine checks what was read from the text: the sum of
			// anything but whole numbers must not be cast to one.
			r.inSum = true
			x := r.render(arg, closeAt)
			r.inSum = false
			if !r.take(len(x) + len(sumGuardText)) {
				return "", -1
			}
			args = set + "CASE WHEN typeof(" + x + ")" + sumGuardText[:sumGuardThen] + x + sumGuardText[sumGuardThen:]
			as = "BIGINT"
		}
	}
	if as == "" || !r.take(len("CAST( AS )")+len(as)+len(fn)+len(` AS ""`)) {
		return "", -1
	}
	if args == "" {
		args = r.render(a0, closeAt)
	}
	out := "CAST(" + r.q[r.spans[i][0]:r.spans[i+1][1]] + args + r.render(closeAt, end+1) + " AS " + as + ")"
	if name := r.alias[i]; name != "" {
		out += ` AS "` + name + `"`
	}
	if r.over {
		return "", -1
	}
	return out, end
}

// sumGuardText is what stands around a sum's argument x, after
// CASE WHEN typeof(x): the whole number types, THEN (x goes at sumGuardThen)
// and the error for anything else.
const (
	sumGuardText = " IN ('TINYINT', 'SMALLINT', 'INTEGER', 'BIGINT') THEN  ELSE error('" + notWholeSum + "') END"
	sumGuardThen = len(" IN ('TINYINT', 'SMALLINT', 'INTEGER', 'BIGINT') THEN ")
)

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
	for k, v := range withAliasTypes(r.toks, types) {
		out[k] = v
	}
	// A name after AS, or right after a value (SELECT 1.5 i, FROM f(x) i,
	// CASE ... END i, NULL i), and every quoted name: more than the
	// statement defines, never less.
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

// selectItems are the places where each item of the statement's own select
// list starts (not a subquery's), and whether the statement combines selects
// (UNION, INTERSECT, EXCEPT) or holds a VALUES list.
func selectItems(toks []rtToken) (starts map[int]bool, combined bool) {
	starts = map[int]bool{}
	depth, list := 0, false
	for i, t := range toks {
		switch {
		case isOpTok(toks, i, "(") || isOpTok(toks, i, "["):
			depth++
		case isOpTok(toks, i, ")") || isOpTok(toks, i, "]"):
			depth--
		case t.kind == rtWord && (t.text == "union" || t.text == "intersect" || t.text == "except" || t.text == "values"):
			combined = true
		case depth != 0:
		case t.kind == rtWord && t.text == "select":
			list = true
			starts[i+1] = true
		case t.kind == rtWord && selectListEnd[t.text]:
			list = false
		case list && isOpTok(toks, i, ","):
			starts[i+1] = true
		}
	}
	return starts, combined
}

// endsItem reports whether a select item ends before toks[i]: at a comma,
// its AS, FROM, or the end of the statement.
func endsItem(toks []rtToken, i int) bool {
	return i >= len(toks) || isOpTok(toks, i, ",") || isOpTok(toks, i, ";") ||
		toks[i].kind == rtWord && (toks[i].text == "from" || toks[i].text == "as")
}

// loneNulls is what to write in place of each NULL that is a whole item of
// the statement's own select list (items), which PostgreSQL answers as text,
// by its place. Not asked of a statement that combines selects or holds a
// VALUES list, where a NULL takes the type of what stands beside it; and a
// NULL in a subquery PostgreSQL reads by what uses it. Of the ones the
// statement gives no name, only the first: a Describe reads the statement
// inside a subquery, where two columns of one name are renamed.
func loneNulls(toks []rtToken, items map[int]bool) map[int]string {
	out := map[int]string{}
	unnamed := false
	for i, t := range toks {
		if !items[i] || t.kind != rtWord || t.text != "null" || !endsItem(toks, i+1) {
			continue
		}
		if i+1 < len(toks) && toks[i+1].kind == rtWord && toks[i+1].text == "as" {
			out[i] = textNull
		} else if !unnamed {
			out[i], unnamed = unnamedNull, true
		}
	}
	return out
}
