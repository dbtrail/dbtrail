package pgshim

import "strings"

// notTextType is the type the screen gives a name the statement defines
// (an alias of a select list, a column of a WITH or a subquery) whose every
// definition is known not to be text: a count, a sum, a number, a numeric
// column renamed. Only that much is known of it, so it is neither a whole
// number nor a float to the screen (a division by it is still refused) and
// no string concatenates as it would on PostgreSQL.
//
// Its name must not hold a word typeIsText looks for (TEXT, CHAR, JSON,
// ENUM, STRING): TestNotTextType pins it.
const notTextType = "DERIVED"

// withAliasTypes is types for a statement that defines names of its own.
// A name the statement defines may stand for an expression, so a column's
// type says nothing of it (SELECT name AS id ... ORDER BY id sorts text):
// such a name is of unknown type, as it always was, unless every definition
// of it in the statement is known not to be text, when it keeps the types
// of the columns of that name and gains notTextType. A name used where the
// column is meant (WHERE id > 'x', PostgreSQL never reads an alias there)
// is then judged by both, and a text column of that name still counts.
//
// A definition may name another (a WITH's column read in the next one), so
// what is known grows until it stops changing. Types not known (nil) stay
// not known: every name may be text.
func withAliasTypes(toks []rtToken, types map[string][]string, untyped bool) map[string][]string {
	defs := aliasDefinitions(toks)
	if types == nil || len(defs) == 0 {
		return types
	}
	if columnList(toks) {
		// Names given in a column list (AS b(n), WITH c(n) AS) are not
		// among the definitions read here, so one of them may share a name
		// with a definition that is: nothing defined is known then.
		return mergeAliasTypes(types, defs, nil, untyped)
	}
	notText := map[string]bool{}
	cur := mergeAliasTypes(types, defs, notText, untyped)
	for range len(defs) + 1 {
		changed := false
		for name, exprs := range defs {
			if notText[name] {
				continue
			}
			known := true
			// Inside its own definition a name is the column of that name
			// (SELECT amount AS amount), not what the statement makes of it.
			own, had := cur[name]
			if column, ok := types[name]; ok {
				cur[name] = column
			}
			for _, e := range exprs {
				if !exprNotText(e, cur) {
					known = false
					break
				}
			}
			if had {
				cur[name] = own
			} else {
				delete(cur, name)
			}
			if known {
				notText[name] = true
				changed = true
			}
		}
		if !changed {
			break
		}
		cur = mergeAliasTypes(types, defs, notText, untyped)
	}
	return cur
}

// mergeAliasTypes is types with the names the statement defines read as what
// they stand for. A defined name known not to be text (notText) keeps the
// types of the columns of that name and gains notTextType. Any other
// defined name may stand for text: when no column has its name it is of
// unknown type (absent), as it always was, and so it is when the columns of
// its name are not text (the rules already take an unknown name for possibly
// text where that matters) and when none of its definitions reads text (a
// table's alias). When a text
// column has its name it keeps that column's types and gains maybeTextType,
// so that it never loses the text of the column it stands for (SELECT name
// AS name ... ORDER BY name sorts text, and so does min(name) AS name).
//
// untyped: the types are not the columns' own (screenUntyped), every name
// is a whole number there, and a defined name is of unknown type as before:
// that screen may not veto what the typed one lets through.
func mergeAliasTypes(types map[string][]string, defs map[string][][]rtToken, notText map[string]bool, untyped bool) map[string][]string {
	out := make(map[string][]string, len(types)+len(defs))
	for k, v := range types {
		if _, defined := defs[k]; !defined {
			out[k] = v
		}
	}
	for name := range defs {
		column := types[name]
		switch {
		case notText[name]:
			out[name] = append(append([]string(nil), column...), notTextType)
		case !untyped && anyTextType(column) && readsText(defs[name], types, defs, notText):
			out[name] = append(append([]string(nil), column...), maybeTextType)
		}
	}
	return out
}

// maybeTextType is the type a defined name gains when it shares its name
// with a text column and is not known to stand for something that is not
// text. typeIsText must take it for text (TestMaybeTextType pins it), and it
// is no column's type, so that a rule that asks what a value is, and not
// only whether it may be text, can tell it (concatOK).
const maybeTextType = "MAYBE TEXT"

// anyTextType reports whether one of a name's columns is text.
func anyTextType(types []string) bool {
	for _, ty := range types {
		if typeIsText(ty) {
			return true
		}
	}
	return false
}

// readsText reports whether one of a name's definitions reads something
// that is text: a text column, a string, a function that makes text, ||. A
// table's alias (FROM t AS c, whose definition is the table's name) reads
// none, and neither does a name given to something the screen knows nothing
// of; both stay of unknown type.
//
// Another name the statement defines counts as text unless it is known not
// to be (notText): a text column given one name in a subquery and its own
// name again outside (SELECT u AS name FROM (SELECT name AS u ...)) is read
// through the name between.
func readsText(exprs [][]rtToken, types map[string][]string, defs map[string][][]rtToken, notText map[string]bool) bool {
	named := func(n string) bool {
		if anyTextType(types[n]) {
			return true
		}
		_, other := defs[n]
		return other && !notText[n]
	}
	for _, e := range exprs {
		for _, t := range e {
			switch t.kind {
			case rtString:
				return true
			case rtOp:
				if t.text == "||" {
					return true
				}
			case rtWord:
				if textMakers[t.text] || named(t.text) {
					return true
				}
			case rtQuoted:
				if named(strings.ToLower(t.text)) {
					return true
				}
			}
		}
	}
	return false
}

// columnList reports whether the statement names columns in a list: a FROM
// item's (AS b(n), ) b(n)) or a WITH's (c(n) AS (...)).
func columnList(toks []rtToken) bool {
	for i, t := range toks {
		if t.kind != rtWord || !isOpTok(toks, i+1, "(") || parenWords[t.text] || t.text == "over" || t.text == "within" {
			// ) OVER (, ) FILTER (, ) AND (: keywords, not a FROM item's name.
			continue
		}
		if i > 0 && (toks[i-1].kind == rtWord && toks[i-1].text == "as" || isOpTok(toks, i-1, ")")) {
			return true
		}
		if end := closingParen(toks, i+1); end > 0 && end+1 < len(toks) && toks[end+1].kind == rtWord && toks[end+1].text == "as" &&
			(isOpTok(toks, end+2, "(") || end+2 < len(toks) && toks[end+2].kind == rtWord && (toks[end+2].text == "materialized" || toks[end+2].text == "not")) {
			return true
		}
	}
	return false
}

// aliasDefinitions are the expressions each name the statement defines
// stands for, by name: the select item before `AS name` (or before a bare
// alias). A FROM item's alias stands for a table, which is never known not
// to be text, and a definition this cannot read is an empty expression,
// which is not either.
func aliasDefinitions(toks []rtToken) map[string][][]rtToken {
	_, at := aliases(toks)
	if len(at) == 0 {
		return nil
	}
	defs := map[string][][]rtToken{}
	for i := range at {
		end := i
		if end > 0 && toks[end-1].kind == rtWord && toks[end-1].text == "as" {
			end--
		}
		start, depth := end, 0
	back:
		for j := end - 1; j >= 0; j-- {
			t := toks[j]
			switch {
			case t.kind == rtOp && (t.text == ")" || t.text == "]"):
				depth++
			case t.kind == rtOp && (t.text == "(" || t.text == "["):
				if depth == 0 {
					break back
				}
				depth--
			case depth == 0 && t.kind == rtOp && t.text == ",":
				break back
			case depth == 0 && t.kind == rtWord && definitionStart[t.text]:
				break back
			}
			start = j
		}
		defs[toks[i].text] = append(defs[toks[i].text], toks[start:end])
	}
	return defs
}

// definitionStart are the words before a select item or a FROM item.
var definitionStart = map[string]bool{
	"select": true, "distinct": true, "all": true, "from": true, "join": true, "on": true,
	"using": true, "where": true, "by": true, "with": true, "having": true, "union": true,
	"intersect": true, "except": true, "lateral": true,
}

// neverText are the functions whose answer is never text, whatever they
// are given: counts, sums, averages, ranks, numbers of a date, roundings,
// booleans of a group.
var neverText = map[string]bool{
	"count": true, "sum": true, "avg": true, "row_number": true, "rank": true, "dense_rank": true,
	"ntile": true, "percent_rank": true, "cume_dist": true, "abs": true, "length": true,
	"char_length": true, "octet_length": true, "position": true, "extract": true, "date_part": true,
	"round": true, "floor": true, "ceil": true, "ceiling": true, "trunc": true, "stddev": true,
	"stddev_pop": true, "stddev_samp": true, "variance": true, "var_pop": true, "var_samp": true,
	"bool_and": true, "bool_or": true, "every": true, pgRoundDiv: true, pgRoundAvg: true, pgRoundK: true, pgRoundNeg: true, pgRoundWhole: true, pgAvg: true, pgAvgN: true, pgDiv: true, pgFloorDiv: true,
	pgFloorEpoch: true, pgEpochMacro: true, pgToTimestampMacro: true, "to_timestamp": true,
}

// asTheirArguments are the functions that answer a value of their first
// argument's type (min, lag) or of every argument's (coalesce, greatest).
var asTheirArguments = map[string]int{
	"min": 1, "max": 1, "lag": 1, "lead": 1, "first_value": 1, "last_value": 1, "any_value": 1, "nullif": 1,
	"coalesce": -1, "greatest": -1, "least": -1,
}

// exprNotText reports whether an expression is known not to be text:
// a number; a column, or a name defined so, whose every type is not text;
// arithmetic (+ - * / %), whose answer on PostgreSQL is a number, a date or
// an interval; a CASE whose every answer is; or a call of a function in
// neverText, or in asTheirArguments over arguments that are. Anything else,
// a string or || anywhere at its top level included, is not known.
func exprNotText(e []rtToken, types map[string][]string) bool {
	// Parentheses around the whole of it.
	for len(e) >= 2 && isOpTok(e, 0, "(") && closingParen(e, 0) == len(e)-1 {
		e = e[1 : len(e)-1]
	}
	if len(e) == 0 {
		return false
	}
	nonText := func(name string) bool {
		ts, found := types[name]
		if !found || len(ts) == 0 {
			return false
		}
		for _, t := range ts {
			if typeIsText(t) {
				return false
			}
		}
		return true
	}
	// A qualified name is its column.
	if len(e) == 3 && e[0].kind == rtWord && isOpTok(e, 1, ".") && e[2].kind == rtWord {
		e = e[2:]
	}
	if len(e) == 1 {
		switch e[0].kind {
		case rtNumber:
			return true
		case rtWord:
			return nonText(e[0].text)
		}
		return false
	}
	// A subquery's answer is whatever it selects, and its own conditions
	// are no part of it.
	for _, t := range e {
		if t.kind == rtWord && (t.text == "select" || t.text == "from") {
			return false
		}
	}
	if e[0].kind == rtWord && e[0].text == "case" {
		return caseNotText(e, types)
	}
	// The top level: a string, a parameter or || makes it unknown;
	// arithmetic makes it a number, a date or an interval.
	arith, depth := false, 0
	for i, t := range e {
		switch {
		case isOpTok(e, i, "(") || isOpTok(e, i, "["):
			depth++
		case isOpTok(e, i, ")") || isOpTok(e, i, "]"):
			depth--
		case depth > 0:
		case t.kind == rtString || t.kind == rtParam || t.kind == rtQuoted:
			return false
		case t.kind == rtOp && t.text == "||":
			return false
		case t.kind == rtOp && (t.text == "+" || t.text == "-" || t.text == "*" || t.text == "/" || t.text == "%"):
			arith = true
		case t.kind == rtWord && (t.text == "case" || t.text == "when" || t.text == "then" || t.text == "else" || t.text == "end"):
			return false
		}
	}
	if arith {
		// Each operand must be known not to be text too: jsonb - 1 is a
		// jsonb, which PostgreSQL orders by its text.
		var operand []rtToken
		depth = 0
		for i, t := range e {
			switch {
			case isOpTok(e, i, "(") || isOpTok(e, i, "["):
				depth++
			case isOpTok(e, i, ")") || isOpTok(e, i, "]"):
				depth--
			case depth == 0 && t.kind == rtOp && (t.text == "+" || t.text == "-" || t.text == "*" || t.text == "/" || t.text == "%"):
				if len(operand) > 0 && !exprNotText(operand, types) {
					return false
				}
				operand = nil
				continue
			}
			operand = append(operand, t)
		}
		return len(operand) > 0 && exprNotText(operand, types)
	}
	// A call: f(...), with OVER (...) or FILTER (...) after it and nothing
	// else.
	if e[0].kind != rtWord || !isOpTok(e, 1, "(") {
		return false
	}
	end := closingParen(e, 1)
	for rest := end + 1; rest < len(e); {
		if e[rest].kind != rtWord || (e[rest].text != "over" && e[rest].text != "filter") || !isOpTok(e, rest+1, "(") {
			return false
		}
		rest = closingParen(e, rest+1) + 1
		if rest == 0 {
			return false
		}
	}
	if end < 0 {
		return false
	}
	if neverText[e[0].text] {
		return true
	}
	n, ok := asTheirArguments[e[0].text]
	if !ok {
		return false
	}
	args := splitArgs(e[2:end])
	if len(args) == 0 || len(args[0]) > 0 && args[0][0].kind == rtWord && args[0][0].text == "distinct" {
		return false
	}
	if n > 0 {
		args = args[:min(n, len(args))]
	}
	for _, a := range args {
		if !exprNotText(a, types) {
			return false
		}
	}
	return true
}

// caseNotText reports whether every answer of a CASE (each THEN and the
// ELSE, a missing ELSE being NULL) is known not to be text.
func caseNotText(e []rtToken, types map[string][]string) bool {
	if e[len(e)-1].kind != rtWord || e[len(e)-1].text != "end" {
		return false
	}
	var answers [][]rtToken
	var cur []rtToken
	in, depth, nested := false, 0, 0
	for i := 1; i < len(e)-1; i++ {
		t := e[i]
		switch {
		case isOpTok(e, i, "("):
			depth++
		case isOpTok(e, i, ")"):
			depth--
		}
		if depth == 0 && t.kind == rtWord {
			switch t.text {
			case "case":
				nested++
			case "end":
				nested--
			}
			if nested == 0 {
				switch t.text {
				case "then", "else":
					if in {
						answers = append(answers, cur)
					}
					in, cur = true, nil
					continue
				case "when":
					if in {
						answers = append(answers, cur)
					}
					in, cur = false, nil
					continue
				}
			}
		}
		if in {
			cur = append(cur, t)
		}
	}
	if in {
		answers = append(answers, cur)
	}
	if len(answers) == 0 {
		return false
	}
	for _, a := range answers {
		if len(a) == 1 && a[0].kind == rtWord && a[0].text == "null" {
			continue
		}
		if !exprNotText(a, types) {
			return false
		}
	}
	return true
}

// splitArgs splits a call's arguments at their top-level commas.
func splitArgs(toks []rtToken) [][]rtToken {
	var out [][]rtToken
	var cur []rtToken
	depth := 0
	for i, t := range toks {
		switch {
		case isOpTok(toks, i, "(") || isOpTok(toks, i, "["):
			depth++
		case isOpTok(toks, i, ")") || isOpTok(toks, i, "]"):
			depth--
		case depth == 0 && isOpTok(toks, i, ","):
			out = append(out, cur)
			cur = nil
			continue
		}
		cur = append(cur, t)
	}
	if len(cur) > 0 || len(out) > 0 {
		out = append(out, cur)
	}
	return out
}
