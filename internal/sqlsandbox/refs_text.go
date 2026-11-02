package sqlsandbox

import (
	"sort"
	"strings"
)

// TextUse is how a statement uses the values it compares, read from the parse
// (#2533). The copy's session folds case and nothing else, where MySQL's
// folding collations also equate accents and sort signs by their own weights,
// so read routing lets the copy answer only when the values a statement
// compares are ones the two compare alike: printable ASCII where they are
// compared for equality, letters, digits and inner spaces where they are put
// in order. This says which is which; the caller, which knows each column's
// collation, decides.
type TextUse struct {
	// Ordered are the column references the statement puts in order: inside
	// an ORDER BY (the statement's, an aggregate's, a window's), an argument
	// of min, max, greatest, least and the other functions that pick by
	// order (orderingFunctions), an operand of <, >, <=, >= or BETWEEN. A
	// reference is listed once per place it is in.
	Ordered []ColumnRef `json:"ordered,omitempty"`
	// Sorted are the ones inside an ORDER BY, also in Ordered: where MySQL
	// sorts an ENUM or a SET by its position in the declared list.
	Sorted []ColumnRef `json:"sorted,omitempty"`
	// OrderedUnknown says the statement puts in order something the walk
	// cannot name: an ordinal or a star in its ORDER BY, a comparison with
	// ANY or ALL of a subquery.
	OrderedUnknown bool `json:"ordered_unknown,omitempty"`
	// Aliases are the names the statement gives: a select item's, a WITH's
	// columns', a derived table's columns'. An ORDER BY name that is one of
	// them sorts whatever it stands for, which may be a column under another
	// name.
	Aliases []string `json:"aliases,omitempty"`
	// SetOperation says the statement holds a UNION, INTERSECT or EXCEPT
	// (ALL included). Its parts' columns go under the first part's names, so
	// an ORDER BY or a MIN over one of those names puts in order a column, or
	// a string, that the name does not say.
	SetOperation bool `json:"set_operation,omitempty"`
	// OddLiteral says a string literal holds a character outside printable
	// ASCII, or ends with a space, which a PAD SPACE collation does not see.
	OddLiteral bool `json:"odd_literal,omitempty"`
	// OrderedLiterals holds each place that puts in order a string literal
	// that is not letters, digits and inner spaces (sortsAlike). Over a date
	// or a number the literal is not compared as text, which the caller can
	// tell only from a bare column beside it.
	OrderedLiterals []OrderedLiteral `json:"ordered_literals,omitempty"`
	// LooseLiteral says some string literal anywhere in the statement is
	// not letters, digits and inner spaces. Where the statement sorts by an
	// ordinal or an alias, such a literal may be what is sorted.
	LooseLiteral bool `json:"loose_literal,omitempty"`
	// ByBytes are the column references under GREATEST, LEAST or a row
	// constructor (`(a, b) > (x, y)`), which the copy compares by their
	// bytes whatever the session's collation, case included; ByBytesLiteral
	// says a string literal is there too. Over text that is never MySQL's
	// answer, whatever the values.
	ByBytes        []ColumnRef `json:"by_bytes,omitempty"`
	ByBytesLiteral bool        `json:"by_bytes_literal,omitempty"`
	// Wrapped are the column references inside a call that can cut or
	// rebuild text (LEFT, SUBSTRING, REPLACE, a CAST, ...: every function
	// but keepsEnds). What it returns can end with a space the value held
	// inside ('Jo Ann' cut to three is 'Jo '), which a PAD SPACE collation
	// does not see and the copy does.
	Wrapped []ColumnRef `json:"wrapped,omitempty"`
}

// keepsEnds are the functions whose text answer cannot end with a space
// their argument held inside: they return it whole, changed letter by
// letter, joined to others whole or trimmed, or they return a number or a
// truth value (LIKE is ~~).
var keepsEnds = map[string]bool{
	"lower": true, "upper": true, "lcase": true, "ucase": true,
	"trim": true, "ltrim": true, "rtrim": true,
	"length": true, "char_length": true, "character_length": true, "octet_length": true, "strlen": true,
	"count": true, "count_star": true, "coalesce": true, "ifnull": true, "nullif": true, "concat": true,
	"min": true, "max": true, "any_value": true, "first": true,
	"sum": true, "avg": true, "string_agg": true, "abs": true, "round": true, "floor": true, "ceil": true,
	"~~": true, "!~~": true, "~~*": true, "!~~*": true,
}

// OrderedLiteral is one place that puts in order a string literal the copy
// does not sort as MySQL does: Columns are the bare column references among
// its operands, and Expression says an operand is something else (a call,
// a CONCAT, a CASE), whose type the walk does not know.
type OrderedLiteral struct {
	Columns    []ColumnRef `json:"columns,omitempty"`
	Expression bool        `json:"expression,omitempty"`
}

// byBytesFunctions compare their arguments by bytes on the copy, not under
// its collation (measured on DuckDB 1.4.5: greatest('a', 'B') is 'a').
var byBytesFunctions = map[string]bool{"greatest": true, "least": true, "row": true}

// orderingFunctions are the functions whose answer depends on the order of
// their arguments' values: their arguments are put in order.
var orderingFunctions = map[string]bool{
	"min": true, "max": true, "greatest": true, "least": true,
	"arg_min": true, "arg_max": true, "argmin": true, "argmax": true, "min_by": true, "max_by": true,
	"arg_min_null": true, "arg_max_null": true,
	"median": true, "quantile": true, "quantile_disc": true, "quantile_cont": true,
	"list_sort": true, "array_sort": true, "list_reverse_sort": true, "array_reverse_sort": true,
	"list_min": true, "list_max": true, "array_min": true, "array_max": true,
}

// orderingComparisons are the comparisons that put their operands in order.
var orderingComparisons = map[string]bool{
	"COMPARE_LESSTHAN": true, "COMPARE_GREATERTHAN": true,
	"COMPARE_LESSTHANOREQUALTO": true, "COMPARE_GREATERTHANOREQUALTO": true,
	"COMPARE_BETWEEN": true, "COMPARE_NOT_BETWEEN": true,
}

// textWalk is where collectTextUse stands: under something that puts its
// values in order, inside an ORDER BY, inside a call that can cut text.
type textWalk struct{ ordered, sorted, wrapped bool }

// collectTextUse fills r.Text from a statement tree.
func collectTextUse(stmt any, r *Refs) {
	walkText(stmt, textWalk{}, &r.Text)
}

func walkText(node any, at textWalk, u *TextUse) {
	switch v := node.(type) {
	case []any:
		for _, e := range v {
			walkText(e, at, u)
		}
	case map[string]any:
		if _, isExpr := v["class"]; isExpr {
			if a, _ := v["alias"].(string); a != "" {
				u.Aliases = append(u.Aliases, a)
			}
		}
		for _, k := range []string{"aliases", "column_name_alias"} {
			if names, ok := v[k].([]any); ok {
				for _, n := range names {
					if s, _ := n.(string); s != "" {
						u.Aliases = append(u.Aliases, s)
					}
				}
			}
		}
		if v["type"] == "SET_OPERATION_NODE" {
			u.SetOperation = true
		}
		switch v["class"] {
		case "COLUMN_REF":
			if c, ok := columnRefOf(v); ok {
				if at.ordered {
					u.Ordered = append(u.Ordered, c)
				}
				if at.sorted {
					u.Sorted = append(u.Sorted, c)
				}
				if at.wrapped {
					u.Wrapped = append(u.Wrapped, c)
				}
			}
		case "CONSTANT":
			if s, ok := varcharConstant(v); ok {
				u.OddLiteral = u.OddLiteral || oddLiteral(s)
				u.LooseLiteral = u.LooseLiteral || !sortsAlike(s)
			}
		case "STAR":
			if at.ordered {
				u.OrderedUnknown = true
			}
		case "SUBQUERY":
			if t, _ := v["comparison_type"].(string); orderingComparisons[t] {
				u.OrderedUnknown = true
			}
		case "COMPARISON", "BETWEEN":
			if t, _ := v["type"].(string); orderingComparisons[t] {
				orderedHere(v, at, u)
				return
			}
		case "CAST":
			at.wrapped = true
		case "FUNCTION", "WINDOW":
			// A window is its function too: MAX(x) OVER () puts x in order.
			f, _ := v["function_name"].(string)
			if byBytesFunctions[f] {
				byBytesHere(v, u)
			}
			if v["class"] == "FUNCTION" && !keepsEnds[f] && !orderingFunctions[f] && !byBytesFunctions[f] {
				at.wrapped = true
			}
			if orderingFunctions[f] {
				orderedHere(v, at, u)
				return
			}
		}
		walkTextKeys(v, at, u)
	}
}

// walkTextKeys walks a node's keys in a fixed order. An ORDER BY's items are
// sorted; the statement's own ORDER BY (its "modifiers") also reads an
// ordinal or a star there as sorting columns it does not name.
func walkTextKeys(v map[string]any, at textWalk, u *TextUse) {
	keys := make([]string, 0, len(v))
	for k := range v {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		switch k {
		case "orders":
			items, _ := v[k].([]any)
			for _, it := range items {
				m, _ := it.(map[string]any)
				e, _ := m["expression"].(map[string]any)
				if e["class"] != "CONSTANT" {
					sortedHere(e, u)
				}
			}
		case "modifiers":
			mods, _ := v[k].([]any)
			selectList, _ := v["select_list"].([]any)
			for _, mod := range mods {
				m, _ := mod.(map[string]any)
				if m["type"] != "ORDER_MODIFIER" {
					walkText(mod, at, u)
					continue
				}
				items, _ := m["orders"].([]any)
				for _, it := range items {
					o, _ := it.(map[string]any)
					e, _ := o["expression"].(map[string]any)
					target, known := sortTarget(e, selectList)
					switch {
					case !known:
						u.OrderedUnknown = true
					case target != nil:
						sortedHere(target, u)
					}
				}
			}
		default:
			walkText(v[k], at, u)
		}
	}
}

// sortTarget is what an item of a statement's ORDER BY sorts: an ordinal
// is the select item it numbers, a name a select item gives is that item (as
// on MySQL, which reads the alias before a column of the same name), any
// other expression is itself. A string constant sorts nothing (nil). known
// is false for what the walk cannot name: a star, or an ordinal past the
// list or over a star.
func sortTarget(e map[string]any, selectList []any) (map[string]any, bool) {
	switch e["class"] {
	case "STAR":
		return nil, false
	case "CONSTANT":
		if _, isText := varcharConstant(e); isText {
			return nil, true
		}
		val, _ := e["value"].(map[string]any)
		n, ok := val["value"].(float64)
		if !ok || n < 1 || int(n) > len(selectList) || n != float64(int(n)) {
			return nil, false
		}
		item, _ := selectList[int(n)-1].(map[string]any)
		if item["class"] == "STAR" {
			return nil, false
		}
		return withoutAlias(item), true
	case "COLUMN_REF":
		if names, _ := e["column_names"].([]any); len(names) == 1 {
			name, _ := names[0].(string)
			for _, it := range selectList {
				item, _ := it.(map[string]any)
				if a, _ := item["alias"].(string); a != "" && strings.EqualFold(a, name) {
					return withoutAlias(item), true
				}
			}
		}
	}
	return e, true
}

// withoutAlias is a select item without its alias, which the walk of the
// select list has recorded already.
func withoutAlias(item map[string]any) map[string]any {
	out := make(map[string]any, len(item))
	for k, v := range item {
		if k != "alias" {
			out[k] = v
		}
	}
	return out
}

// sortedHere walks an expression an ORDER BY sorts, and records a string
// literal in it that only letters, digits and inner spaces keep in MySQL's
// order.
func sortedHere(e map[string]any, u *TextUse) {
	if loose, _, _ := scanOrdered(e); loose {
		lit := OrderedLiteral{Expression: true}
		if e["class"] == "COLUMN_REF" {
			c, _ := columnRefOf(e)
			lit = OrderedLiteral{Columns: []ColumnRef{c}}
		}
		u.OrderedLiterals = append(u.OrderedLiterals, lit)
	}
	walkText(e, textWalk{ordered: true, sorted: true}, u)
}

// orderedHere walks a node that puts its operands in order, and records
// the string literals it puts in order that only letters, digits and inner
// spaces keep in MySQL's order, with what is beside them.
func orderedHere(v map[string]any, at textWalk, u *TextUse) {
	if loose, _, _ := scanOrdered(v); loose {
		lit := OrderedLiteral{}
		for _, op := range operands(v) {
			m, _ := op.(map[string]any)
			switch m["class"] {
			case "COLUMN_REF":
				if c, ok := columnRefOf(m); ok {
					lit.Columns = append(lit.Columns, c)
				}
			case "CONSTANT":
			default:
				lit.Expression = true
			}
		}
		u.OrderedLiterals = append(u.OrderedLiterals, lit)
	}
	walkTextKeys(v, textWalk{ordered: true, sorted: at.sorted, wrapped: at.wrapped}, u)
}

// operands are a comparison's, a BETWEEN's or a function's direct operands.
func operands(v map[string]any) []any {
	var out []any
	for _, k := range []string{"left", "right", "input", "lower", "upper"} {
		if op, ok := v[k]; ok {
			out = append(out, op)
		}
	}
	if ch, ok := v["children"].([]any); ok {
		out = append(out, ch...)
	}
	return out
}

// scanOrdered reads a subtree: whether it holds a string literal that is
// not letters, digits and inner spaces, its column references, and whether
// it holds a string literal at all.
func scanOrdered(n any) (loose bool, cols []ColumnRef, anyText bool) {
	var scan func(any)
	scan = func(n any) {
		switch x := n.(type) {
		case []any:
			for _, e := range x {
				scan(e)
			}
		case map[string]any:
			switch x["class"] {
			case "COLUMN_REF":
				if c, ok := columnRefOf(x); ok {
					cols = append(cols, c)
				}
			case "CONSTANT":
				if s, ok := varcharConstant(x); ok {
					anyText = true
					loose = loose || !sortsAlike(s)
				}
			}
			keys := make([]string, 0, len(x))
			for k := range x {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			for _, k := range keys {
				scan(x[k])
			}
		}
	}
	scan(n)
	return loose, cols, anyText
}

// byBytesHere records what a GREATEST, a LEAST or a row compares.
func byBytesHere(v map[string]any, u *TextUse) {
	_, cols, anyText := scanOrdered(v["children"])
	u.ByBytes = append(u.ByBytes, cols...)
	u.ByBytesLiteral = u.ByBytesLiteral || anyText
}

// columnRefOf reads a COLUMN_REF node as collectNames does.
func columnRefOf(v map[string]any) (ColumnRef, bool) {
	names, ok := v["column_names"].([]any)
	if !ok || len(names) == 0 {
		return ColumnRef{}, false
	}
	c := ColumnRef{}
	c.Name, _ = names[len(names)-1].(string)
	if len(names) > 1 {
		c.Qualifier, _ = names[len(names)-2].(string)
	}
	return c, true
}

// varcharConstant is a constant's text when it is a string.
func varcharConstant(v map[string]any) (string, bool) {
	val, _ := v["value"].(map[string]any)
	typ, _ := val["type"].(map[string]any)
	if typ["id"] != "VARCHAR" || val["is_null"] == true {
		return "", false
	}
	s, ok := val["value"].(string)
	return s, ok
}

// oddLiteral says a literal holds a character outside printable ASCII or ends
// with a space.
func oddLiteral(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] < 0x20 || s[i] > 0x7e {
			return true
		}
	}
	return len(s) > 0 && s[len(s)-1] == ' '
}

// sortsAlike says MySQL's folding collations and the copy put s in the same
// place among other such texts: letters, digits and spaces, no trailing one
// (measured, #2533).
func sortsAlike(s string) bool {
	for i := 0; i < len(s); i++ {
		ch := s[i]
		if !(ch == ' ' || ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch >= '0' && ch <= '9') {
			return false
		}
	}
	return len(s) == 0 || s[len(s)-1] != ' '
}
