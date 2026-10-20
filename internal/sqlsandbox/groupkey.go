package sqlsandbox

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
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
// each such column as the one value its group holds. Same groups, same
// values, and the ORDER BY and HAVING over them still compare under the
// session's collation.
//
// "Fixes" is checked, not trusted (groupKeyCheckSQL): the key is read from
// the CREATE TABLE in the snapshot's file, and a view can hold one key value
// twice when that is not the key its changes were applied by (a key altered
// after the snapshot), or when the writer folded two values into one (zero
// dates are stored as NULL). Each replaced column is read through a check
// that its group holds one value, byte for byte, and the statement fails with
// groupKeyMixed otherwise, which runs it as written.
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
// same tree; if it does not, the statement runs as written. The tree is the
// parser's own, read with its numbers as their digits (parseExact): a
// literal read as a float64 would come back rounded past 15 digits, in the
// tree and in the text alike, so the comparison could not see it.
//
// stmt is the worker's parse of text, read only to skip a statement with no
// GROUP BY without a second parse.
func groupByKeyRewrite(ctx context.Context, conn *sql.Conn, text string, stmt any, keys []TableKey, searchSchema string) (string, error) {
	if len(keys) == 0 || !hasGroupBy(stmt) {
		return "", nil
	}
	tree, err := parseExact(ctx, conn, text)
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
	var out string
	if err := conn.QueryRowContext(ctx, "SELECT json_deserialize_sql(?::JSON)", string(wrapped)).Scan(&out); err != nil {
		return "", fmt.Errorf("render the rewritten statement: %w", err)
	}
	again, err := parseExact(ctx, conn, out)
	if err != nil {
		return "", fmt.Errorf("the rewritten statement does not parse: %w", err)
	}
	if !reflect.DeepEqual(withoutLocations(again), withoutLocations(tree)) {
		return "", fmt.Errorf("the rewritten statement does not parse back to the rewritten tree")
	}
	return out, nil
}

// parseExact is the parser's tree of one statement, with every number kept
// as the digits the parser wrote (json.Number).
func parseExact(ctx context.Context, conn *sql.Conn, text string) (any, error) {
	var raw string
	if err := conn.QueryRowContext(ctx, "SELECT json_serialize_sql(?::VARCHAR)::VARCHAR", text).Scan(&raw); err != nil {
		return nil, err
	}
	d := json.NewDecoder(strings.NewReader(raw))
	d.UseNumber()
	var parsed struct {
		Error      bool  `json:"error"`
		Statements []any `json:"statements"`
	}
	if err := d.Decode(&parsed); err != nil {
		return nil, err
	}
	if parsed.Error || len(parsed.Statements) != 1 {
		return nil, fmt.Errorf("not one statement")
	}
	return parsed.Statements[0], nil
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

// deepCopy copies a tree, numbers kept as their digits.
func deepCopy(v any) (any, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	d := json.NewDecoder(strings.NewReader(string(b)))
	d.UseNumber()
	var out any
	err = d.Decode(&out)
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
	searchSchema string
	ctes         map[string]bool

	// Read once a SELECT has a column to replace (functions).
	ctx       context.Context
	conn      *sql.Conn
	loaded    bool
	loadErr   error
	aggregate map[string]bool // an aggregate and nothing else
	ambiguous map[string]bool // both an aggregate and a scalar function
	check     map[string]any  // groupKeyCheckSQL's expression, as the parser writes it
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
	cat.ctx, cat.conn = ctx, conn
	return cat, nil
}

// groupKeyMixed is the error a replaced column's check raises when its group
// holds more than one value: the key did not fix it after all.
const groupKeyMixed = "bintrail: a grouping key of the copy holds more than one text value"

// groupKeyCheckSQL is what a replaced column becomes, with bintrail_x
// standing for it: its group's one value when every row of the group holds
// that value byte for byte (or every row holds NULL), and an error
// otherwise. The bytes, not the session's collation: two spellings that
// collation folds together are two groups under the key and one as written,
// so they are a mismatch too, and comparing bytes is what keeps the check
// cheap. any_value, not min, gives the value: min(x COLLATE C) would carry
// that collation into the ORDER BY over it.
const groupKeyCheckSQL = "SELECT CASE WHEN count(bintrail_x) = 0 OR (count(bintrail_x) = count(*) AND " +
	"min(bintrail_x COLLATE C) = max(bintrail_x COLLATE C)) THEN any_value(bintrail_x) " +
	"ELSE error('" + groupKeyMixed + "') END"

// functions reads which function names are aggregates, and the check's
// expression, the first time a SELECT has a column to replace.
func (cat *rewriteCatalog) functions() error {
	if cat.loaded {
		return cat.loadErr
	}
	cat.loaded = true
	cat.loadErr = cat.readFunctions()
	return cat.loadErr
}

func (cat *rewriteCatalog) readFunctions() error {
	cat.aggregate, cat.ambiguous = map[string]bool{}, map[string]bool{}
	rows, err := cat.conn.QueryContext(cat.ctx, "SELECT lower(function_name), bool_or(function_type = 'aggregate'), "+
		"bool_or(function_type <> 'aggregate') FROM duckdb_functions() GROUP BY 1")
	if err != nil {
		return fmt.Errorf("read the functions: %w", err)
	}
	for rows.Next() {
		var name string
		var agg, other bool
		if err := rows.Scan(&name, &agg, &other); err != nil {
			rows.Close()
			return fmt.Errorf("read the functions: %w", err)
		}
		switch {
		case agg && other:
			cat.ambiguous[name] = true
		case agg:
			cat.aggregate[name] = true
		}
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("read the functions: %w", err)
	}
	tree, err := parseExact(cat.ctx, cat.conn, groupKeyCheckSQL)
	if err != nil {
		return fmt.Errorf("parse the check: %w", err)
	}
	node, _ := tree.(map[string]any)["node"].(map[string]any)
	list, _ := node["select_list"].([]any)
	if len(list) != 1 {
		return fmt.Errorf("parse the check: unexpected shape")
	}
	cat.check, _ = list[0].(map[string]any)
	if cat.check == nil {
		return fmt.Errorf("parse the check: unexpected shape")
	}
	return nil
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
		// Each field that decides what the relation's names bind to must be
		// there: an engine that stopped writing one would read as "none".
		if !hasKeys(m, "catalog_name", "schema_name", "table_name", "alias", "at_clause", "sample", "column_name_alias") {
			return nil, false
		}
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
		if !hasKeys(m, "using_columns", "ref_type", "join_type", "left", "right") {
			return nil, false
		}
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
	if !hasKeys(node, "group_sets", "aggregate_handling", "qualify", "having", "modifiers", "select_list", "from_table") {
		return false
	}
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
	if len(dropped) == 0 || cat.functions() != nil {
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
	// The names the rewrite gives a bare column that had none. In ORDER BY
	// and HAVING a bare name can bind to such an alias where it bound to
	// nothing before (two relations hold the column: "ambiguous"), so a
	// bare name spelled like one must be that very column.
	added := map[string]bool{}
	for _, item := range selectList {
		im, _ := item.(map[string]any)
		if a, _ := im["alias"].(string); a == "" && im["class"] == "COLUMN_REF" {
			if col, ok := isDropped(im); ok {
				added[asciiLower(col.name)] = true
			}
		}
	}
	rw := exprRewriter{cat: cat, isDropped: isDropped, aliases: aliases, added: added}
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
		set[i] = json.Number(strconv.Itoa(i))
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
		num, ok := x.(json.Number)
		if !ok {
			return false
		}
		i64, err := num.Int64()
		i := int(i64)
		if err != nil || i < 0 || i >= n || seen[i] {
			return false
		}
		seen[i] = true
	}
	return true
}

func hasKeys(m map[string]any, keys ...string) bool {
	for _, k := range keys {
		if _, ok := m[k]; !ok {
			return false
		}
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

// wrap is groupKeyCheckSQL over ref, under alias.
func (cat *rewriteCatalog) wrap(ref map[string]any, alias string) map[string]any {
	inner := shallowCopy(ref)
	inner["alias"] = ""
	var sub func(v any) any
	sub = func(v any) any {
		switch x := v.(type) {
		case map[string]any:
			if x["class"] == "COLUMN_REF" {
				if parts, _ := x["column_names"].([]any); len(parts) == 1 && parts[0] == "bintrail_x" {
					c, _ := deepCopy(inner)
					return c
				}
			}
			out := make(map[string]any, len(x))
			for k, child := range x {
				out[k] = sub(child)
			}
			return out
		case []any:
			out := make([]any, len(x))
			for i, child := range x {
				out[i] = sub(child)
			}
			return out
		}
		return v
	}
	f := sub(cat.check).(map[string]any)
	f["alias"] = alias
	return f
}

// exprRewriter replaces, in one expression, every reference to a dropped
// column outside an aggregate with any_value of it.
type exprRewriter struct {
	cat       *rewriteCatalog
	isDropped func(map[string]any) (catalogColumn, bool)
	aliases   map[string]bool
	added     map[string]bool
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
			_, dropped := r.isDropped(x)
			if parts, _ := x["column_names"].([]any); aliasScope && len(parts) == 1 && !dropped {
				if name, _ := parts[0].(string); r.added[asciiLower(name)] {
					return nil, false, false
				}
			}
			if !dropped {
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
