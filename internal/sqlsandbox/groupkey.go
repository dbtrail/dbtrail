package sqlsandbox

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
)

// TableKey is the PRIMARY KEY of one of the copy's views: the view is
// Schema.Table, and Columns are the key's columns as the view names them.
type TableKey struct {
	Schema  string   `json:"schema"`
	Table   string   `json:"table"`
	Columns []string `json:"columns"`
}

// groupByKeyRewrite returns the statement to run instead of the one the user
// wrote, or "" to run that one as written (#2479).
//
// The session compares text under nocase.icu_noaccent so that it groups like
// MySQL. A text column in a GROUP BY therefore costs an ICU sort key on every
// row that reaches the grouping, which on a join is every joined row: that
// alone made a statement grouping by an item's key and its name about twice
// as slow. When the GROUP BY holds every column of a table's PRIMARY KEY, the
// key already fixes that table's other columns, so its text columns cannot
// split a group further: the statement groups by the key alone and reads
// each such column as any_value(col), the one value its group holds. Same
// groups, same values, and the ORDER BY and HAVING over them still compare
// under the session's collation.
//
// A statement is rewritten only where every part of it is understood; any
// shape this does not read leaves its SELECT as written, which is always
// correct, only slower. The rules, per SELECT:
//
//   - a plain GROUP BY (no ROLLUP, CUBE, GROUPING SETS or GROUP BY ALL) of
//     column references only, each naming one column of one relation;
//   - a FROM of the copy's views joined with ON (or CROSS JOIN): no USING,
//     NATURAL, SEMI, ANTI, subquery or table function, so every column a
//     reference can name is known from the catalog;
//   - the key's columns are not text: a text key groups under the collation
//     too, and two keys MySQL tells apart (under a _cs collation) can fold
//     into one group here, holding two different names;
//   - no window, subquery, lambda or star in the SELECT list, HAVING or
//     ORDER BY, no QUALIFY and no DISTINCT ON;
//   - an unaliased SELECT item that is not the bare column keeps its name only
//     if left alone, so the statement is left alone;
//   - a reference that a SELECT alias could be standing for is not read as
//     the column.
//
// The rewritten tree is turned back into text by the engine itself
// (json_deserialize_sql), and that text is parsed again and must give the
// same tree; if it does not, the statement runs as written.
func groupByKeyRewrite(ctx context.Context, conn *sql.Conn, stmt any, keys []TableKey, searchSchema string) (string, error) {
	if len(keys) == 0 || !hasGroupBy(stmt) {
		return "", nil
	}
	tree, err := deepCopy(stmt)
	if err != nil {
		return "", err
	}
	cat, err := loadRewriteCatalog(ctx, conn, keys)
	if err != nil {
		return "", err
	}
	cat.searchSchema = searchSchema
	cat.ctes = cteNames(tree)
	changed := false
	var walk func(v any)
	walk = func(v any) {
		switch x := v.(type) {
		case map[string]any:
			for _, child := range x {
				walk(child)
			}
			if x["type"] == "SELECT_NODE" && cat.rewriteNode(x) {
				changed = true
			}
		case []any:
			for _, child := range x {
				walk(child)
			}
		}
	}
	walk(tree)
	if !changed {
		return "", nil
	}
	wrapped, err := json.Marshal(map[string]any{"error": false, "statements": []any{tree}})
	if err != nil {
		return "", err
	}
	var text string
	if err := conn.QueryRowContext(ctx, "SELECT json_deserialize_sql(?::JSON)", string(wrapped)).Scan(&text); err != nil {
		return "", fmt.Errorf("render the rewritten statement: %w", err)
	}
	again, reason := parseStatement(ctx, conn, text)
	if reason != "" {
		return "", fmt.Errorf("the rewritten statement does not parse: %s", reason)
	}
	if !reflect.DeepEqual(withoutLocations(again), withoutLocations(tree)) {
		return "", fmt.Errorf("the rewritten statement does not parse back to the rewritten tree")
	}
	return text, nil
}

// hasGroupBy says whether any SELECT in the tree has a GROUP BY, so a
// statement without one costs no catalog read.
func hasGroupBy(v any) bool {
	switch x := v.(type) {
	case map[string]any:
		if g, ok := x["group_expressions"].([]any); ok && len(g) > 0 && x["type"] == "SELECT_NODE" {
			return true
		}
		for _, child := range x {
			if hasGroupBy(child) {
				return true
			}
		}
	case []any:
		for _, child := range x {
			if hasGroupBy(child) {
				return true
			}
		}
	}
	return false
}

func deepCopy(v any) (any, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	var out any
	err = json.Unmarshal(b, &out)
	return out, err
}

// withoutLocations is a copy of a tree with every query_location dropped:
// where an expression sat in the text is all a re-render may change.
func withoutLocations(v any) any {
	switch x := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(x))
		for k, child := range x {
			if k != "query_location" {
				out[k] = withoutLocations(child)
			}
		}
		return out
	case []any:
		out := make([]any, len(x))
		for i, child := range x {
			out[i] = withoutLocations(child)
		}
		return out
	}
	return v
}

// cteNames are the names every WITH in the tree binds, in lower case. A
// relation spelled like one of them is not read as a view, wherever the WITH
// is: telling which one is in scope is not worth the risk of a wrong key.
func cteNames(v any) map[string]bool {
	out := map[string]bool{}
	var walk func(v any)
	walk = func(v any) {
		switch x := v.(type) {
		case map[string]any:
			if m, ok := x["cte_map"].(map[string]any); ok {
				entries, _ := m["map"].([]any)
				for _, e := range entries {
					if em, ok := e.(map[string]any); ok {
						if k, ok := em["key"].(string); ok {
							out[asciiLower(k)] = true
						}
					}
				}
			}
			for _, child := range x {
				walk(child)
			}
		case []any:
			for _, child := range x {
				walk(child)
			}
		}
	}
	walk(v)
	return out
}

// rewriteCatalog is what the rewrite reads from the session: the columns of
// every view (by lower-case schema and name), the keys, and which function
// names are aggregates.
type rewriteCatalog struct {
	columns      map[[2]string][]catalogColumn
	keys         map[[2]string][]string
	aggregate    map[string]bool // an aggregate and nothing else
	ambiguous    map[string]bool // both an aggregate and a scalar function
	anyValue     map[string]any  // any_value(x), as the parser writes it
	searchSchema string
	ctes         map[string]bool
}

type catalogColumn struct {
	name, typ string
}

func loadRewriteCatalog(ctx context.Context, conn *sql.Conn, keys []TableKey) (*rewriteCatalog, error) {
	cat := &rewriteCatalog{
		columns:   map[[2]string][]catalogColumn{},
		keys:      map[[2]string][]string{},
		aggregate: map[string]bool{},
		ambiguous: map[string]bool{},
	}
	for _, k := range keys {
		cat.keys[[2]string{asciiLower(k.Schema), asciiLower(k.Table)}] = k.Columns
	}
	rows, err := conn.QueryContext(ctx, "SELECT schema_name, table_name, column_name, data_type FROM duckdb_columns() "+
		"WHERE database_name = current_database() ORDER BY schema_name, table_name, column_index")
	if err != nil {
		return nil, fmt.Errorf("read the views' columns: %w", err)
	}
	for rows.Next() {
		var schema, table string
		var c catalogColumn
		if err := rows.Scan(&schema, &table, &c.name, &c.typ); err != nil {
			rows.Close()
			return nil, fmt.Errorf("read the views' columns: %w", err)
		}
		id := [2]string{asciiLower(schema), asciiLower(table)}
		cat.columns[id] = append(cat.columns[id], c)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("read the views' columns: %w", err)
	}
	rows, err = conn.QueryContext(ctx, "SELECT lower(function_name), bool_or(function_type = 'aggregate'), "+
		"bool_or(function_type <> 'aggregate') FROM duckdb_functions() GROUP BY 1")
	if err != nil {
		return nil, fmt.Errorf("read the functions: %w", err)
	}
	for rows.Next() {
		var name string
		var agg, other bool
		if err := rows.Scan(&name, &agg, &other); err != nil {
			rows.Close()
			return nil, fmt.Errorf("read the functions: %w", err)
		}
		switch {
		case agg && other:
			cat.ambiguous[name] = true
		case agg:
			cat.aggregate[name] = true
		}
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("read the functions: %w", err)
	}
	var raw string
	if err := conn.QueryRowContext(ctx, "SELECT json_serialize_sql('SELECT any_value(bintrail_x)')::VARCHAR").Scan(&raw); err != nil {
		return nil, fmt.Errorf("parse any_value: %w", err)
	}
	var parsed struct {
		Statements []struct {
			Node struct {
				SelectList []map[string]any `json:"select_list"`
			} `json:"node"`
		} `json:"statements"`
	}
	if err := json.Unmarshal([]byte(raw), &parsed); err != nil || len(parsed.Statements) != 1 || len(parsed.Statements[0].Node.SelectList) != 1 {
		return nil, fmt.Errorf("parse any_value: unexpected shape")
	}
	cat.anyValue = parsed.Statements[0].Node.SelectList[0]
	return cat, nil
}

// binding is one relation of a FROM: the name its columns are qualified by,
// its columns, and its key when it has one.
type binding struct {
	name    string
	columns []catalogColumn
	key     []string
}

func (b binding) column(name string) (catalogColumn, bool) {
	for _, c := range b.columns {
		if asciiEqualFold(c.name, name) {
			return c, true
		}
	}
	return catalogColumn{}, false
}

// bindings lists the relations of a FROM, or false when it holds one this
// rewrite does not read.
func (cat *rewriteCatalog) bindings(from any) ([]binding, bool) {
	m, ok := from.(map[string]any)
	if !ok {
		return nil, false
	}
	switch m["type"] {
	case "BASE_TABLE":
		if m["catalog_name"] != "" || m["at_clause"] != nil || m["sample"] != nil {
			return nil, false
		}
		if a, _ := m["column_name_alias"].([]any); len(a) > 0 {
			return nil, false
		}
		table, _ := m["table_name"].(string)
		schema, _ := m["schema_name"].(string)
		if table == "" || cat.ctes[asciiLower(table)] {
			return nil, false
		}
		var id [2]string
		if schema != "" {
			id = [2]string{asciiLower(schema), asciiLower(table)}
		} else {
			// Unqualified: the USE schema, then main, as DuckDB searches.
			id = [2]string{asciiLower(cat.searchSchema), asciiLower(table)}
			if cat.searchSchema == "" || cat.columns[id] == nil {
				id = [2]string{"main", asciiLower(table)}
			}
		}
		cols := cat.columns[id]
		if cols == nil {
			return nil, false
		}
		name, _ := m["alias"].(string)
		if name == "" {
			name = table
		}
		return []binding{{name: name, columns: cols, key: cat.keys[id]}}, true
	case "JOIN":
		if u, _ := m["using_columns"].([]any); len(u) > 0 {
			return nil, false
		}
		if r := m["ref_type"]; r != "REGULAR" && r != "CROSS" {
			return nil, false
		}
		switch m["join_type"] {
		case "INNER", "LEFT", "RIGHT", "OUTER":
		default:
			return nil, false
		}
		left, ok := cat.bindings(m["left"])
		if !ok {
			return nil, false
		}
		right, ok := cat.bindings(m["right"])
		if !ok {
			return nil, false
		}
		return append(left, right...), true
	}
	return nil, false
}

// scope resolves column references against one SELECT's FROM.
type scope struct {
	bindings []binding
}

// resolve names the binding and column a reference reads, or false when it
// is not certainly one column of one relation of this FROM.
func (s scope) resolve(ref map[string]any) (int, catalogColumn, bool) {
	parts, _ := ref["column_names"].([]any)
	switch len(parts) {
	case 1:
		name, _ := parts[0].(string)
		found, at, col := 0, -1, catalogColumn{}
		for i, b := range s.bindings {
			if c, ok := b.column(name); ok {
				found, at, col = found+1, i, c
			}
		}
		return at, col, found == 1
	case 2:
		qual, _ := parts[0].(string)
		name, _ := parts[1].(string)
		at := -1
		for i, b := range s.bindings {
			// A qualifier that is also a column name could be a struct
			// field access: not read.
			if _, ok := b.column(qual); ok {
				return -1, catalogColumn{}, false
			}
			if asciiEqualFold(b.name, qual) {
				if at >= 0 {
					return -1, catalogColumn{}, false
				}
				at = i
			}
		}
		if at < 0 {
			return -1, catalogColumn{}, false
		}
		c, ok := s.bindings[at].column(name)
		return at, c, ok
	}
	return -1, catalogColumn{}, false
}

// keyTypes are the column types a key may have for its grouping to fix the
// rest of its row: the ones the session's collation does not touch.
var keyTypes = map[string]bool{
	"TINYINT": true, "SMALLINT": true, "INTEGER": true, "BIGINT": true, "HUGEINT": true,
	"UTINYINT": true, "USMALLINT": true, "UINTEGER": true, "UBIGINT": true, "UHUGEINT": true,
	"DATE": true, "TIME": true, "TIMESTAMP": true, "TIMESTAMP WITH TIME ZONE": true,
}

func keyType(t string) bool {
	return keyTypes[t] || strings.HasPrefix(t, "DECIMAL(")
}

// rewriteNode rewrites one SELECT in place and says whether it did. It
// decides everything before it changes anything, so a SELECT it gives up on
// is left exactly as it was.
func (cat *rewriteCatalog) rewriteNode(node map[string]any) bool {
	groups, _ := node["group_expressions"].([]any)
	if len(groups) == 0 || node["aggregate_handling"] != "STANDARD_HANDLING" || node["qualify"] != nil {
		return false
	}
	if !plainGroupingSet(node["group_sets"], len(groups)) {
		return false
	}
	modifiers, _ := node["modifiers"].([]any)
	for _, m := range modifiers {
		mm, _ := m.(map[string]any)
		switch mm["type"] {
		case "ORDER_MODIFIER", "LIMIT_MODIFIER", "LIMIT_PERCENT_MODIFIER":
		case "DISTINCT_MODIFIER":
			if t, _ := mm["distinct_on_targets"].([]any); len(t) > 0 {
				return false
			}
		default:
			return false
		}
	}
	bs, ok := cat.bindings(node["from_table"])
	if !ok {
		return false
	}
	seen := map[string]bool{}
	for _, b := range bs {
		if seen[asciiLower(b.name)] {
			return false
		}
		seen[asciiLower(b.name)] = true
	}
	sc := scope{bindings: bs}
	selectList, _ := node["select_list"].([]any)
	aliases := map[string]bool{}
	for _, item := range selectList {
		im, _ := item.(map[string]any)
		if a, _ := im["alias"].(string); a != "" {
			aliases[asciiLower(a)] = true
		}
	}

	// The GROUP BY: every entry one column of one relation.
	type grouped struct {
		at  int
		col catalogColumn
	}
	byBinding := map[int][]grouped{}
	entries := make([]grouped, len(groups))
	for i, g := range groups {
		gm, _ := g.(map[string]any)
		if gm["class"] != "COLUMN_REF" {
			return false
		}
		if parts, _ := gm["column_names"].([]any); len(parts) == 1 {
			if name, _ := parts[0].(string); aliases[asciiLower(name)] {
				return false
			}
		}
		at, col, ok := sc.resolve(gm)
		if !ok {
			return false
		}
		entries[i] = grouped{at, col}
		byBinding[at] = append(byBinding[at], grouped{at, col})
	}
	dropped := map[[2]string]catalogColumn{} // (binding, lower column) -> column
	for at, gs := range byBinding {
		key := bs[at].key
		if len(key) == 0 {
			continue
		}
		covered := true
		for _, k := range key {
			kc, ok := bs[at].column(k)
			if !ok || !keyType(kc.typ) {
				covered = false
				break
			}
			in := false
			for _, g := range gs {
				if asciiEqualFold(g.col.name, kc.name) {
					in = true
				}
			}
			if !in {
				covered = false
				break
			}
		}
		if !covered {
			continue
		}
		for _, g := range gs {
			if g.col.typ == "VARCHAR" {
				dropped[[2]string{asciiLower(bs[at].name), asciiLower(g.col.name)}] = g.col
			}
		}
	}
	if len(dropped) == 0 {
		return false
	}
	isDropped := func(ref map[string]any) (catalogColumn, bool) {
		at, col, ok := sc.resolve(ref)
		if !ok {
			return catalogColumn{}, false
		}
		c, ok := dropped[[2]string{asciiLower(bs[at].name), asciiLower(col.name)}]
		return c, ok
	}

	// Decide every replacement before making any.
	rw := exprRewriter{cat: cat, isDropped: isDropped, aliases: aliases}
	newSelect := make([]any, len(selectList))
	for i, item := range selectList {
		im, _ := item.(map[string]any)
		if im["class"] == "COLUMN_REF" {
			if col, ok := isDropped(im); ok {
				alias, _ := im["alias"].(string)
				if alias == "" {
					alias = col.name
				}
				newSelect[i] = cat.wrap(im, alias)
				continue
			}
		}
		out, changed, ok := rw.expr(item, false)
		if !ok {
			return false
		}
		if a, _ := im["alias"].(string); changed && a == "" {
			// Its name is the expression's text, which the rewrite changes.
			return false
		}
		newSelect[i] = out
	}
	newHaving, _, ok := rw.expr(node["having"], true)
	if !ok {
		return false
	}
	newModifiers := make([]any, len(modifiers))
	for i, m := range modifiers {
		mm, _ := m.(map[string]any)
		if mm["type"] != "ORDER_MODIFIER" {
			newModifiers[i] = m
			continue
		}
		orders, _ := mm["orders"].([]any)
		newOrders := make([]any, len(orders))
		for j, o := range orders {
			om, _ := o.(map[string]any)
			e, _, ok := rw.expr(om["expression"], true)
			if !ok {
				return false
			}
			no := shallowCopy(om)
			no["expression"] = e
			newOrders[j] = no
		}
		nm := shallowCopy(mm)
		nm["orders"] = newOrders
		newModifiers[i] = nm
	}
	var keep []any
	for i, g := range groups {
		if _, ok := dropped[[2]string{asciiLower(bs[entries[i].at].name), asciiLower(entries[i].col.name)}]; !ok {
			keep = append(keep, g)
		}
	}
	set := make([]any, len(keep))
	for i := range keep {
		set[i] = float64(i)
	}
	node["select_list"] = newSelect
	node["having"] = newHaving
	node["modifiers"] = newModifiers
	node["group_expressions"] = keep
	node["group_sets"] = []any{set}
	return true
}

// plainGroupingSet says the GROUP BY is one grouping set holding every entry
// once: what the parser writes for a GROUP BY without ROLLUP, CUBE or
// GROUPING SETS.
func plainGroupingSet(v any, n int) bool {
	sets, _ := v.([]any)
	if len(sets) != 1 {
		return false
	}
	set, _ := sets[0].([]any)
	if len(set) != n {
		return false
	}
	seen := make([]bool, n)
	for _, x := range set {
		f, ok := x.(float64)
		i := int(f)
		if !ok || float64(i) != f || i < 0 || i >= n || seen[i] {
			return false
		}
		seen[i] = true
	}
	return true
}

func shallowCopy(m map[string]any) map[string]any {
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// wrap is any_value(ref) under alias.
func (cat *rewriteCatalog) wrap(ref map[string]any, alias string) map[string]any {
	fn, _ := deepCopy(cat.anyValue)
	f := fn.(map[string]any)
	inner := shallowCopy(ref)
	inner["alias"] = ""
	f["children"] = []any{inner}
	f["alias"] = alias
	return f
}

// exprRewriter replaces, in one expression, every reference to a dropped
// column outside an aggregate with any_value of it.
type exprRewriter struct {
	cat       *rewriteCatalog
	isDropped func(map[string]any) (catalogColumn, bool)
	aliases   map[string]bool
}

// expr returns the expression rewritten, whether it changed, and false when
// it holds something this rewrite does not read. aliasScope is set for HAVING
// and ORDER BY, where a bare name can stand for a SELECT alias.
func (r exprRewriter) expr(v any, aliasScope bool) (any, bool, bool) {
	switch x := v.(type) {
	case map[string]any:
		switch x["class"] {
		case "WINDOW", "SUBQUERY", "LAMBDA", "STAR":
			return nil, false, false
		case "COLUMN_REF":
			if _, ok := r.isDropped(x); !ok {
				return x, false, true
			}
			if parts, _ := x["column_names"].([]any); aliasScope && len(parts) == 1 {
				if name, _ := parts[0].(string); r.aliases[asciiLower(name)] {
					return nil, false, false
				}
			}
			return r.cat.wrap(x, ""), true, true
		case "FUNCTION":
			name, _ := x["function_name"].(string)
			name = asciiLower(name)
			orders, _ := x["order_bys"].(map[string]any)
			ob, _ := orders["orders"].([]any)
			if r.cat.aggregate[name] || x["distinct"] == true || x["filter"] != nil || len(ob) > 0 {
				// The rows of the group: a reference inside reads them,
				// not the group's one value.
				if containsClass(x, "WINDOW", "SUBQUERY", "LAMBDA", "STAR") {
					return nil, false, false
				}
				return x, false, true
			}
			if r.cat.ambiguous[name] && r.refersToDropped(x) {
				return nil, false, false
			}
		}
		out := make(map[string]any, len(x))
		changed := false
		for k, child := range x {
			c, ch, ok := r.expr(child, aliasScope)
			if !ok {
				return nil, false, false
			}
			out[k] = c
			changed = changed || ch
		}
		if !changed {
			return x, false, true
		}
		return out, true, true
	case []any:
		out := make([]any, len(x))
		changed := false
		for i, child := range x {
			c, ch, ok := r.expr(child, aliasScope)
			if !ok {
				return nil, false, false
			}
			out[i] = c
			changed = changed || ch
		}
		if !changed {
			return x, false, true
		}
		return out, true, true
	}
	return v, false, true
}

func (r exprRewriter) refersToDropped(v any) bool {
	switch x := v.(type) {
	case map[string]any:
		if x["class"] == "COLUMN_REF" {
			_, ok := r.isDropped(x)
			return ok
		}
		for _, child := range x {
			if r.refersToDropped(child) {
				return true
			}
		}
	case []any:
		for _, child := range x {
			if r.refersToDropped(child) {
				return true
			}
		}
	}
	return false
}

func containsClass(v any, classes ...string) bool {
	switch x := v.(type) {
	case map[string]any:
		for _, c := range classes {
			if x["class"] == c {
				return true
			}
		}
		for _, child := range x {
			if containsClass(child, classes...) {
				return true
			}
		}
	case []any:
		for _, child := range x {
			if containsClass(child, classes...) {
				return true
			}
		}
	}
	return false
}

// asciiLower lowers ASCII letters only, the way DuckDB folds names
// (asciiEqualFold), so map keys built with it agree with that comparison.
func asciiLower(s string) string {
	b := []byte(s)
	for i, c := range b {
		if 'A' <= c && c <= 'Z' {
			b[i] = c + 'a' - 'A'
		}
	}
	return string(b)
}
