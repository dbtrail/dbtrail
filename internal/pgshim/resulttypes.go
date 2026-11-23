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
// list, under an alias, in a subquery or a WITH, inside another expression.
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
//     precision. Fields with a fraction (second, epoch, milliseconds) are
//     left as they are: PostgreSQL writes them with decimals the engine's
//     whole number does not hold.
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

// lonelyNull is NULL as a whole select item: text, with no name of its own
// (an operator's result is headed ?column?, as PostgreSQL heads a NULL).
const lonelyNull = "(CAST(NULL AS VARCHAR) || '')"

// notWholeSum is what the engine answers when a sum this file took for one
// of whole numbers is of something else: an error, never a rounded sum.
const notWholeSum = "the copy cannot tell this sum''s type as PostgreSQL would; cast its argument (sum(x::numeric))"

// asPostgresTypes is text, as the connection is about to hand it to the copy
// (copyText), with PostgreSQL's type cast around the expressions of this
// file's table. The copy's column types are read only for a statement with a
// sum to decide, and what the connection knows of a busy copy is left as it
// was.
func (s *session) asPostgresTypes(text string) string {
	return asPostgresTypes(text, func() map[string][]string {
		busy := s.catalogBusy
		types, _ := s.columnTypes(text)
		s.catalogBusy = busy
		return types
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
	r := &typeRewrite{q: stmt, toks: toks, spans: spans, columns: types, nulls: loneNulls(toks)}
	out := r.render(0, len(toks))
	if !r.changed {
		return stmt
	}
	return stmt[:spans[0][0]] + out + stmt[spans[len(toks)-1][1]:]
}

type typeRewrite struct {
	q     string
	toks  []rtToken
	spans [][2]int
	// columns reads the copy's column types; types are those, without the
	// names the statement may define itself, once a sum asked for them.
	columns func() map[string][]string
	types   map[string][]string
	asked   bool
	nulls   map[int]bool
	changed bool
}

// render is the text of toks[from:to] with the casts written in. Text is
// only ever cut at a token's edge, so nothing is written into a comment.
func (r *typeRewrite) render(from, to int) string {
	if from >= to {
		return ""
	}
	var b strings.Builder
	last := r.spans[from][0]
	for i := from; i < to; i++ {
		if t := r.toks[i]; t.kind == rtWord && strings.HasPrefix(t.text, "_bintrail_pg_") && isOpTok(r.toks, i+1, "(") {
			// One of the port's own macros (RewriteForCopy): measured with
			// its arguments as they are.
			if end := closingParen(r.toks, i+1); end > 0 && end < to {
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
	if r.nulls[i] {
		return lonelyNull, i
	}
	if toks[i].kind != rtWord {
		return "", -1
	}
	// f(...), or pg_catalog.f(...); a function of any other schema is not
	// PostgreSQL's own.
	name := i
	if toks[i].text == "pg_catalog" && isOpTok(toks, i+1, ".") && i+2 < to && toks[i+2].kind == rtWord {
		name = i + 2
	} else if isOpTok(toks, i-1, ".") {
		return "", -1
	}
	if !isOpTok(toks, name+1, "(") {
		return "", -1
	}
	closeAt := closingParen(toks, name+1)
	if closeAt < 0 || closeAt >= to {
		return "", -1
	}
	a0 := name + 2
	as, args := "", ""
	switch fn := toks[name].text; {
	case int4Calls[fn]:
		as = "INTEGER"
	case fn == "extract":
		// extract(year FROM x), extract('year' FROM x).
		if closeAt-a0 >= 3 && (toks[a0].kind == rtWord || toks[a0].kind == rtString) && wholeFields[strings.ToLower(toks[a0].text)] &&
			toks[a0+1].kind == rtWord && toks[a0+1].text == "from" {
			as = "DECIMAL(18,0)"
		}
	case fn == "date_part":
		if closeAt-a0 >= 3 && toks[a0].kind == rtString && wholeFields[strings.ToLower(toks[a0].text)] && isOpTok(toks, a0+1, ",") {
			as = "DOUBLE"
		}
	case fn == "sum":
		arg, set := a0, ""
		if arg < closeAt && toks[arg].kind == rtWord && (toks[arg].text == "distinct" || toks[arg].text == "all") {
			set = strings.ToUpper(toks[arg].text) + " "
			arg++
		}
		if t := wholeType(toks[arg:closeAt], r.columnTypes); t == "int2" || t == "int4" {
			// The engine checks what was read from the text: the sum of
			// anything but whole numbers must not be cast to one.
			x := r.render(arg, closeAt)
			args = set + "CASE WHEN typeof(" + x + ") IN ('TINYINT', 'SMALLINT', 'INTEGER', 'BIGINT') THEN " + x + " ELSE error('" + notWholeSum + "') END"
			as = "BIGINT"
		}
	}
	if as == "" {
		return "", -1
	}
	if args == "" {
		args = r.render(a0, closeAt)
	}
	// With what follows the call and belongs to it: FILTER (...), OVER (...).
	end := closeAt
	for end+2 < to && toks[end+1].kind == rtWord && (toks[end+1].text == "filter" || toks[end+1].text == "over") {
		if isOpTok(toks, end+2, "(") {
			c := closingParen(toks, end+2)
			if c < 0 || c >= to {
				return "", -1
			}
			end = c
			continue
		}
		if toks[end+1].text != "over" || toks[end+2].kind != rtWord {
			break
		}
		end += 2 // OVER w
	}
	return "CAST(" + r.q[r.spans[i][0]:r.spans[name+1][1]] + args + r.render(closeAt, end+1) + " AS " + as + ")", end
}

// columnTypes are the types of the columns a sum's argument may name: the
// copy's, less every name the statement may define itself, which stands for
// whatever it was defined as. Nil when the copy's are not known, or when the
// statement names columns in a list (AS t(a, b)), which defines names this
// does not read.
func (r *typeRewrite) columnTypes() map[string][]string {
	if r.asked {
		return r.types
	}
	r.asked = true
	types := r.columns()
	if types == nil || columnList(r.toks) {
		return nil
	}
	out := make(map[string][]string, len(types))
	for k, v := range withAliasTypes(r.toks, types) {
		out[k] = v
	}
	// A name after AS, or right after a value (SELECT 1.5 i, FROM f(x) i),
	// and every quoted name: more than the statement defines, never less.
	for i, t := range r.toks {
		switch {
		case t.kind == rtQuoted:
			delete(out, strings.ToLower(t.text))
		case t.kind != rtWord || aliasStop[t.text] || i == 0:
		default:
			prev := r.toks[i-1]
			if prev.kind == rtString || prev.kind == rtNumber || isOpTok(r.toks, i-1, ")") || isOpTok(r.toks, i-1, "]") ||
				prev.kind == rtWord && (prev.text == "as" || !aliasStop[prev.text]) {
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
		return castWhole[o[n-1].text]
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

// loneNulls are the places of each NULL that is a whole item of the
// statement's own select list, which PostgreSQL answers as text. None in a
// statement with a UNION, an INTERSECT, an EXCEPT or a VALUES list, where a
// NULL takes the type of what stands beside it, nor in a subquery, where
// PostgreSQL reads it by what uses it.
func loneNulls(toks []rtToken) map[int]bool {
	selectListEnd := map[string]bool{
		"from": true, "where": true, "group": true, "having": true, "order": true, "limit": true, "offset": true,
		"window": true, "fetch": true, "for": true,
	}
	out := map[int]bool{}
	depth, list := 0, false
	for i, t := range toks {
		switch {
		case isOpTok(toks, i, "("):
			depth++
		case isOpTok(toks, i, ")"):
			depth--
		case t.kind != rtWord:
		case t.text == "union" || t.text == "intersect" || t.text == "except" || t.text == "values":
			return nil
		case depth != 0:
		case t.text == "select":
			list = true
		case t.text == "null":
			before := isOpTok(toks, i-1, ",") || i > 0 && toks[i-1].kind == rtWord && toks[i-1].text == "select"
			after := i+1 == len(toks) || isOpTok(toks, i+1, ",") || isOpTok(toks, i+1, ";") ||
				toks[i+1].kind == rtWord && (toks[i+1].text == "from" || toks[i+1].text == "as")
			if list && before && after {
				out[i] = true
			}
		case selectListEnd[t.text]:
			list = false
		}
	}
	return out
}
