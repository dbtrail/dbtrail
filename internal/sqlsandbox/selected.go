package sqlsandbox

import (
	"strconv"
	"strings"
)

// SelectedItem is what one item of a statement's select list returns, read
// from the parse for the fractional seconds MySQL declares a date and time
// result with (#2849). MySQL declares them from the item's sources: a
// column's own declaration (DATETIME(3) is 3), the largest of its operands
// for COALESCE, CASE, GREATEST and a date step, 6 for a step in
// microseconds, 0 for NOW(). The worker cannot see a column's declaration;
// the parent resolves Columns against the tables' CREATE TABLE.
type SelectedItem struct {
	// Columns are the columns the item's value is taken from.
	Columns []SelectedColumn `json:"columns,omitempty"`
	// Digits is the most fractional digits another part of the value
	// carries: 6 for a step by an INTERVAL in microseconds, 0 otherwise.
	Digits int `json:"digits,omitempty"`
	// Itself says the item returns one column's value as the column holds
	// it: the column, or MIN, MAX or ANY_VALUE of it. MySQL then declares
	// the column's own type, so a TIMESTAMP stays a TIMESTAMP; anything
	// computed from one is a DATETIME.
	Itself bool `json:"itself,omitempty"`
	// Clock says the value holds the copy's now() or utc_timestamp(), which
	// MySQL declares with no fraction when it is written without one.
	// They are that only where the copy defines them as MySQL's
	// (Session.MySQLClock).
	Clock bool `json:"clock,omitempty"`
	// FromUnixTime says the value is a FROM_UNIXTIME call, which MySQL
	// declares with as many fractional digits as its argument's TYPE has.
	// The walk does not read the argument: read routing lets the copy answer
	// one only over what MySQL types as a whole number
	// (readrouter.UnixTimeVeto), and nothing else here knows that.
	FromUnixTime bool `json:"from_unixtime,omitempty"`
	// DayStep says the value steps a date by an INTERVAL of a day or more,
	// which over a DATE MySQL answers as a DATE, and the copy as a date and
	// time.
	DayStep bool `json:"day_step,omitempty"`
	// Star is the one base table a star item stands for: the item is all of
	// its columns, in the order the table declares them.
	Star *TableRef `json:"star,omitempty"`
	// Unread says the item holds something this walk cannot tell the
	// fractional digits of: a constant, a cast, a subquery, a function it
	// has no rule for, a column of a derived table.
	Unread bool `json:"unread,omitempty"`
}

// SelectedColumn is a column a select item's value is taken from: its name,
// as written, and the base tables of the select's FROM it can belong to (the
// one its qualifier names, every one when it has none).
type SelectedColumn struct {
	Name   string     `json:"name"`
	Tables []TableRef `json:"tables"`
}

// collectSelected reads the select list of a statement tree item by item,
// for Refs.Selected. Nil when the statement is not read that way: not a
// SELECT (a SHOW, a DESCRIBE), or a set operation whose sides do not line
// up.
func collectSelected(stmt any) []SelectedItem {
	root, _ := stmt.(map[string]any)
	return selectedOfNode(root["node"])
}

// selectedOfNode is collectSelected for one query node: a SELECT, or a set
// operation, whose column I is the largest of its sides' column I on MySQL.
func selectedOfNode(n any) []SelectedItem {
	node, _ := n.(map[string]any)
	switch node["type"] {
	case "SELECT_NODE":
		return selectedOfSelect(node)
	case "SET_OPERATION_NODE":
		left, right := selectedOfNode(node["left"]), selectedOfNode(node["right"])
		if left == nil || len(left) != len(right) {
			return nil
		}
		out := make([]SelectedItem, len(left))
		for i := range left {
			l, r := left[i], right[i]
			out[i] = SelectedItem{
				Columns:      append(append([]SelectedColumn(nil), l.Columns...), r.Columns...),
				Digits:       max(l.Digits, r.Digits),
				Clock:        l.Clock || r.Clock,
				FromUnixTime: l.FromUnixTime || r.FromUnixTime,
				DayStep:      l.DayStep || r.DayStep,
				// A star's columns are the table's: under a set operation
				// they cannot be lined up with the other side's here.
				Unread: l.Unread || r.Unread || l.Star != nil || r.Star != nil,
			}
			if out[i].Unread {
				out[i] = SelectedItem{Unread: true}
			}
		}
		return out
	}
	return nil
}

// selectedOfSelect reads one SELECT's list.
func selectedOfSelect(sel map[string]any) []SelectedItem {
	list, ok := sel["select_list"].([]any)
	if !ok || len(list) == 0 {
		return nil
	}
	items, _, fromOK := fromItems(sel["from_table"])
	if sel["from_table"] == nil || isEmptyFrom(sel["from_table"]) {
		items, fromOK = nil, true // SELECT NOW(): no table, nothing to resolve
	}
	withs := map[string]bool{}
	if cm, ok := sel["cte_map"].(map[string]any); ok {
		entries, _ := cm["map"].([]any)
		for _, e := range entries {
			entry, _ := e.(map[string]any)
			if key, ok := entry["key"].(string); ok {
				withs[foldName(key)] = true
			}
		}
	}
	w := selectedWalk{from: items, fromOK: fromOK, withs: withs}
	out := make([]SelectedItem, len(list))
	for i, e := range list {
		item, _ := e.(map[string]any)
		var it SelectedItem
		if item["class"] == "STAR" {
			it = w.star(item)
		} else {
			w.value(item, &it)
			it.Itself = it.Itself || isColumnItself(item)
			if len(it.Columns) != 1 {
				it.Itself = false
			}
		}
		if it.Unread {
			it = SelectedItem{Unread: true} // what was read of it says nothing
		}
		out[i] = it
	}
	return out
}

// isEmptyFrom says a FROM clause names nothing (DuckDB's EMPTY table ref).
func isEmptyFrom(from any) bool {
	v, _ := from.(map[string]any)
	return v["type"] == "EMPTY"
}

// isColumnItself says an item is a column, or MIN, MAX or ANY_VALUE of one
// (a window's too), which return a value the column holds as it holds it.
func isColumnItself(item map[string]any) bool {
	switch item["class"] {
	case "COLUMN_REF":
		return true
	case "FUNCTION", "WINDOW":
		switch item["function_name"] {
		case "min", "max", "any_value", "first", "last", "arbitrary", "first_value", "last_value":
			kids, _ := item["children"].([]any)
			if len(kids) == 1 {
				k, _ := kids[0].(map[string]any)
				return k["class"] == "COLUMN_REF"
			}
		}
	}
	return false
}

// selectedWalk reads a select item's sources against its select's FROM.
type selectedWalk struct {
	from   []fromItem
	fromOK bool
	withs  map[string]bool
}

// star reads a star item: one base table's columns, or Unread.
func (w selectedWalk) star(item map[string]any) SelectedItem {
	unread := SelectedItem{Unread: true}
	if !w.fromOK {
		return unread
	}
	for _, k := range []string{"exclude_list", "qualified_exclude_list", "replace_list", "rename_list"} {
		if l, ok := item[k].([]any); !ok || len(l) > 0 {
			return unread
		}
	}
	if cols, _ := item["columns"].(bool); cols || item["expr"] != nil {
		return unread // COLUMNS(...): which columns is the pattern's
	}
	rel, ok := item["relation_name"].(string)
	if !ok {
		return unread
	}
	var hits []fromItem
	for _, it := range w.from {
		if rel == "" || foldName(it.called) == foldName(rel) {
			hits = append(hits, it)
		}
	}
	if len(hits) != 1 || !w.baseTable(hits[0]) {
		return unread
	}
	ref := hits[0].ref
	return SelectedItem{Star: &ref}
}

// baseTable says a FROM item is a base table, not a derived table or the
// name of a WITH.
func (w selectedWalk) baseTable(it fromItem) bool {
	return !it.derived && !(it.ref.Catalog == "" && it.ref.Schema == "" && w.withs[foldName(it.ref.Name)])
}

// value walks an expression in a position whose value the item returns.
func (w selectedWalk) value(n any, it *SelectedItem) {
	v, ok := n.(map[string]any)
	if !ok {
		it.Unread = true
		return
	}
	switch v["class"] {
	case "COLUMN_REF":
		w.column(v, it)
	case "CONSTANT":
		if val, _ := v["value"].(map[string]any); val["is_null"] == true {
			return // a NULL adds no type to COALESCE, CASE or IF on MySQL
		}
		it.Unread = true
	case "OPERATOR":
		if v["type"] != "OPERATOR_COALESCE" {
			it.Unread = true
			return
		}
		w.children(v, it, 0)
	case "CASE":
		checks, _ := v["case_checks"].([]any)
		for _, c := range checks {
			check, _ := c.(map[string]any)
			w.value(check["then_expr"], it) // the WHEN decides, the THEN is returned
		}
		w.value(v["else_expr"], it)
	case "WINDOW":
		switch v["function_name"] {
		case "min", "max", "any_value", "first_value", "last_value":
			w.children(v, it, 0)
		default:
			it.Unread = true
		}
	case "FUNCTION":
		w.function(v, it)
	default:
		it.Unread = true
	}
}

// children walks an expression's children from the one numbered from.
func (w selectedWalk) children(v map[string]any, it *SelectedItem, from int) {
	kids, _ := v["children"].([]any)
	if len(kids) <= from {
		it.Unread = true
		return
	}
	for _, k := range kids[from:] {
		w.value(k, it)
	}
}

// function walks a function call in a value position.
func (w selectedWalk) function(v map[string]any, it *SelectedItem) {
	name, _ := v["function_name"].(string)
	kids, _ := v["children"].([]any)
	if s, _ := v["schema"].(string); s != "" {
		it.Unread = true // a qualified name is not the built-in
		return
	}
	if distinct, _ := v["distinct"].(bool); distinct && name != "min" && name != "max" {
		it.Unread = true
		return
	}
	switch name {
	case "min", "max", "any_value", "first", "last", "arbitrary", "coalesce", "ifnull", "greatest", "least":
		w.children(v, it, 0)
	case "nullif":
		if len(kids) != 2 {
			it.Unread = true
			return
		}
		w.value(kids[0], it)
	case "if":
		if len(kids) != 3 {
			it.Unread = true
			return
		}
		w.children(v, it, 1)
	case "now", "utc_timestamp":
		if len(kids) != 0 {
			it.Unread = true // NOW(3): the copy's macro takes no precision
			return
		}
		it.Clock = true
	case "from_unixtime":
		if len(kids) != 1 {
			it.Unread = true
			return
		}
		it.FromUnixTime = true
	case "+", "-", "date_add", "_bintrail_date_add", "_bintrail_date_sub":
		// DuckDB's own date_add(x, i) is x + i; its date_sub counts the
		// units between two dates, a number, and is not here.
		if len(kids) != 2 {
			it.Unread = true
			return
		}
		// The date is the first operand; for + the INTERVAL may come first.
		date, step := kids[0], kids[1]
		if name == "+" && intervalUnit(date) != "" {
			date, step = step, date
		}
		unit := intervalUnit(step)
		if unit == "" {
			it.Unread = true // not a date step: arithmetic on numbers, or a step this walk cannot read
			return
		}
		switch unit {
		case "to_microseconds":
			it.Digits = max(it.Digits, 6)
		case "to_milliseconds":
			it.Digits = max(it.Digits, 3)
		case "to_days", "to_weeks", "to_months", "to_years":
			it.DayStep = true
		}
		w.value(date, it)
	default:
		it.Unread = true
	}
}

// column records a column reference, resolved to the FROM's base tables it
// can belong to.
func (w selectedWalk) column(v map[string]any, it *SelectedItem) {
	names, _ := v["column_names"].([]any)
	parts := make([]string, 0, len(names))
	for _, n := range names {
		s, ok := n.(string)
		if !ok {
			it.Unread = true
			return
		}
		parts = append(parts, s)
	}
	if len(parts) == 0 || len(parts) > 3 || !w.fromOK {
		it.Unread = true
		return
	}
	col := SelectedColumn{Name: parts[len(parts)-1]}
	if len(parts) == 1 {
		for _, f := range w.from {
			if !w.baseTable(f) {
				it.Unread = true // the name may be the derived table's
				return
			}
			col.Tables = append(col.Tables, f.ref)
		}
	} else {
		qual := parts[len(parts)-2]
		var hits []fromItem
		for _, f := range w.from {
			if foldName(f.called) == foldName(qual) {
				hits = append(hits, f)
			}
		}
		if len(hits) != 1 || !w.baseTable(hits[0]) {
			it.Unread = true // an outer select's table, or a derived one
			return
		}
		if len(parts) == 3 && foldName(hits[0].ref.Schema) != foldName(parts[0]) {
			it.Unread = true
			return
		}
		col.Tables = []TableRef{hits[0].ref}
	}
	if len(col.Tables) == 0 {
		it.Unread = true // no table: a name the copy binds some other way
		return
	}
	it.Columns = append(it.Columns, col)
}

// intervalUnit is the DuckDB function an INTERVAL n unit is parsed into
// (to_days, to_microseconds...), when its amount is a whole number written
// as a constant; "" for anything else. The parse wraps the amount in casts
// and a trunc: INTERVAL 1 DAY is to_days(CAST(trunc(CAST(1 AS DOUBLE)) AS
// INTEGER)).
func intervalUnit(n any) string {
	v, _ := n.(map[string]any)
	if v["class"] != "FUNCTION" {
		return ""
	}
	name, _ := v["function_name"].(string)
	switch name {
	case "to_microseconds", "to_milliseconds", "to_seconds", "to_minutes", "to_hours", "to_days", "to_weeks", "to_months", "to_years":
	default:
		return ""
	}
	kids, _ := v["children"].([]any)
	if len(kids) != 1 || !wholeAmount(kids[0]) {
		return ""
	}
	return name
}

// wholeAmount says an INTERVAL's amount is a whole number written as a
// constant (1, '2', '-1'), under the casts and the trunc the parse adds.
func wholeAmount(n any) bool {
	v, _ := n.(map[string]any)
	switch v["class"] {
	case "CAST":
		return wholeAmount(v["child"])
	case "FUNCTION":
		kids, _ := v["children"].([]any)
		if (v["function_name"] == "trunc" || v["function_name"] == "-") && len(kids) == 1 {
			return wholeAmount(kids[0])
		}
		return false
	case "CONSTANT":
		val, _ := v["value"].(map[string]any)
		if val["is_null"] == true {
			return false
		}
		switch x := val["value"].(type) {
		case float64:
			return x == float64(int64(x))
		case string:
			_, err := strconv.ParseInt(strings.TrimSpace(x), 10, 64)
			return err == nil
		}
	}
	return false
}
