package pgshim

import "strings"

// What a name the statement defines is, as a number (#3219).
//
// exactClass reads a column's class from the copy's types for its name. A
// name the statement defines itself (a WITH's column, an alias of a select
// list) has no type there, so avg(spent) of WITH per AS (SELECT
// sum(ol_amount) AS spent ...) was not known to be a numeric's average and
// went to the source. aliasClassOf gives such a name the class of what it
// stands for, when every definition of it in the statement has one and they
// agree: sum(ol_amount) is a numeric wherever spent is read. A column of the
// same name is left to the column's types: which one a use means is not
// followed here, and a wrong class only costs a macro refused when the
// statement runs (the source answers then).

// classTypes are the copy's types aliasClassOf gives a defined name of each
// class, as classOfTypes reads them back.
var classTypes = map[exactCls]string{
	clsWhole32: "INTEGER",
	clsWhole64: "BIGINT",
	clsDecimal: "DECIMAL(18,6)",
}

// aliasClassOf is typesOf for toks with the names the statement defines read
// as the class of what they stand for (see above). It is typesOf unchanged
// when the statement defines no such name, or gives names in a column list
// other than a WITH's (AS t(a, b)), which aliasDefinitions does not read.
func aliasClassOf(toks []rtToken, typesOf func(string) []string) func(string) []string {
	defs := aliasDefinitions(toks)
	if len(defs) == 0 || columnListBesidesWith(toks) {
		return typesOf
	}
	known := map[string]exactCls{}
	// look reads a name; inside its own definition a name is the column of
	// that name (SELECT amount AS amount), not what the statement makes of it.
	look := func(self string) func(string) []string {
		return func(name string) []string {
			col := typesOf(name)
			c, ok := known[name]
			if !ok || name == self {
				return col
			}
			// The screen's mark of a defined name known not to be text
			// (withAliasTypes) says less than its class.
			out := []string{classTypes[c]}
			for _, ty := range col {
				if ty != notTextType {
					out = append(out, ty)
				}
			}
			return out
		}
	}
	for range len(defs) + 1 {
		changed := false
		for name, exprs := range defs {
			if _, ok := known[name]; ok {
				continue
			}
			if isColumnName(typesOf(name)) {
				// Also a column's name: where the statement means which is
				// not followed here, so the name keeps the column's types
				// alone (sum(qty) AS qty is still a sum of integers).
				continue
			}
			cls := clsUnknown
			for i, e := range exprs {
				c := exactClass(e, look(name))
				switch {
				case c == clsUnknown:
					cls = clsUnknown
				case i == 0:
					cls = c
				case (cls == clsDecimal) != (c == clsDecimal):
					// A numeric here and a whole number there: neither.
					cls = clsUnknown
				default:
					cls = max(cls, c)
				}
				if cls == clsUnknown {
					break
				}
			}
			if cls != clsUnknown {
				known[name] = cls
				changed = true
			}
		}
		if !changed {
			break
		}
	}
	if len(known) == 0 {
		return typesOf
	}
	return look("")
}

// pgSum32 is sum(x) of whole numbers of at most 32 bits (smallint, integer),
// which PostgreSQL answers as a bigint and the engine as a HUGEINT the port
// announces as a numeric: the macro casts the engine's sum to a BIGINT, and
// refuses an argument of any other type when the statement runs
// (sqlsandbox pgRoundMacros).
const pgSum32 = "_bintrail_pg_sum32"

// sumCall reports whether toks[i] begins sum(<args>) the macro may stand
// for: one argument of whole numbers of at most 32 bits, without DISTINCT,
// ALL, ORDER BY, FILTER or OVER, which a macro does not take. end is the
// argument's closing parenthesis.
func sumCall(toks []rtToken, i int, typesOf func(string) []string) (end int, ok bool) {
	if toks[i].kind != rtWord || toks[i].text != "sum" || !isOpTok(toks, i+1, "(") || isOpTok(toks, i-1, ".") {
		return 0, false
	}
	end = closingParen(toks, i+1)
	if end < 0 || end == i+2 {
		return 0, false
	}
	arg := toks[i+2 : end]
	if arg[0].kind == rtWord && (arg[0].text == "distinct" || arg[0].text == "all") {
		return 0, false
	}
	depth := 0
	for j := range arg {
		switch {
		case isOpTok(arg, j, "("):
			depth++
		case isOpTok(arg, j, ")"):
			depth--
		case depth == 0 && (isOpTok(arg, j, ",") || arg[j].kind == rtWord && arg[j].text == "order"):
			return 0, false
		}
	}
	if n := end + 1; n < len(toks) && toks[n].kind == rtWord && (toks[n].text == "filter" || toks[n].text == "over" || toks[n].text == "within") {
		return 0, false
	}
	if exactClass(arg, typesOf) != clsWhole32 || insideOwnMacro(toks, i) {
		return 0, false
	}
	return end, true
}

// insideOwnMacro reports whether toks[at] stands inside the arguments of one
// of the copy's own macros (_bintrail_pg_round_div(sum(qty), ...)), which
// are written for the engine's types: a sum there is left as it is.
func insideOwnMacro(toks []rtToken, at int) bool {
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
			if j > 0 && toks[j-1].kind == rtWord && strings.HasPrefix(toks[j-1].text, "_bintrail_pg_") {
				return true
			}
		}
	}
	return false
}

// isColumnName reports whether a name's types hold a column's, beside the
// screen's mark of a name the statement defines (withAliasTypes).
func isColumnName(ts []string) bool {
	for _, ty := range ts {
		if ty != notTextType {
			return true
		}
	}
	return false
}
