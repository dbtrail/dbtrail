package sqlsandbox

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
)

// The connected order (Session.TryConnectedJoinOrder): a third order the
// join check tries for a statement whose plan is dangerous both in DuckDB's
// order and in the order written, before it is refused.
//
// The CH-benCHmark's Q21 is the case. In DuckDB's order it joins stock with
// orders on the warehouse alone, a condition DuckDB derives from the others,
// and runs out of memory; in the order written supplier comes first and has
// no condition with order_line, a cross product of 6e11 rows. With the join
// order optimizer off, DuckDB builds a left-deep plan in FROM order: the
// first table is the probe side and every later table a hash-build side. So
// the statement is written again with the largest table first, never built,
// and each next table one with a condition on the tables already placed, so
// no join is a cross product: FROM order_line l1, orders, stock, supplier,
// nation answered the whole Q21 in 6.8 to 8 s on a frozen copy of the
// snapshot's views (memory_limit 2GB, threads 4), with the source's 100 rows.
//
// Only a narrow shape is written again, and only through DuckDB's own parser
// (json_serialize_sql, then json_deserialize_sql of the reordered tree), so
// nothing is parsed here by hand:
//
//   - one SELECT, no WITH, no SAMPLE, and no *, COLUMNS(...) or #n anywhere
//     outside its subqueries (each depends on the order of the FROM);
//   - its FROM a chain of implicit cross products (FROM a, b, c) of plain
//     tables or views, aliases allowed: no JOIN, subquery, table function,
//     column aliases or AT clause;
//   - its WHERE an AND of conditions, or one condition (an OR at the top is
//     not). A subquery inside it (Q21's NOT EXISTS) is left as it is, and
//     adds no condition between the tables.
//
// A condition connects a table to the ones placed before it when it names
// columns of that table and of the placed ones only, whatever it is: a = b,
// mod(s_w_id * s_i_id, 10000) = su_suppkey, l1.ol_delivery_d > o_entry_d. A
// column with a table name or alias is that table's; one without is the one
// FROM table that has a column of that name (DESCRIBE, in the statement's
// session). A name more than one FROM table has, or none, or anything else
// it cannot place, means no rewrite.
//
// The order: the table DuckDB estimates with the most rows first (EXPLAIN of
// SELECT 1 FROM it, taken as the check takes every estimate, correctRows),
// then, again and again, of the tables a condition connects to the ones
// placed, the one with the fewest rows; a tie goes to the table written
// first. A FROM whose tables cannot all be reached so is a real cross
// product, and is not written again; nor is one whose order written is
// connected already, which is the order the check just refused.
//
// Reordering the inner joins of a FROM that is only cross products keeps the
// statement's meaning: every condition is in WHERE, and WHERE is applied to
// the same rows whatever their order. Two checks stand between the new text
// and DuckDB all the same: the new text is parsed again and must give the
// tree that was asked for (positions in the text aside), and its result must
// have the statement's own column names and types (DESCRIBE). Then the join
// check judges the new text's plan in the order written, as it judged the
// others; it runs only when that plan has no dangerous join. The new text is
// the worker's alone: the caller, the audit and the logs keep the
// statement's own.

// joinOrderConnected is wireResult.JoinOrder for a statement the check ran
// in a connected order of its tables.
const joinOrderConnected = "connected"

// fromTable is one table of a FROM of cross products.
type fromTable struct {
	// node is the table's own node of the parsed statement.
	node map[string]any
	// name is what qualifies its columns, lowercased: its alias, or its
	// table name when it has none.
	name string
	// ref is the table as SQL, its parts quoted, as the statement names it.
	ref string
}

// tableFacts is what the connected order reads of one table: its column
// names and DuckDB's estimate of its rows.
type tableFacts struct {
	columns []string
	rows    float64
}

// connectedNote is why a connected order was not used: why, "" when it
// was; failed when a step did not work (logged as a warning), not when the
// statement is of another shape or has no connected order; err the
// statement's context's error when it ended meanwhile.
type connectedNote struct {
	why    string
	failed bool
	err    error
	// writtenConnected: why is that the order written is connected
	// already (whyWrittenConnected), which the join order by shape
	// (shapeorder.go) runs as written.
	writtenConnected bool
}

// whyWrittenConnected is connectedOrder's why for a FROM whose order
// written is connected already.
const whyWrittenConnected = "the order written is connected already"

// connectedRewrite is a statement written again in a connected order: its
// text, and the order, by the names that qualify its tables.
type connectedRewrite struct {
	text  string
	order []string
}

// connectedOrderSQL writes statement again in a connected order of its
// tables, on conn, the statement's own session; facts says what each FROM
// table holds. The note says why it is not, and then the rewrite is empty:
// a statement of another shape, or (failed) a step that did not work.
func connectedOrderSQL(ctx context.Context, conn *sql.Conn, statement string, facts func(ref string) (tableFacts, error)) (connectedRewrite, connectedNote) {
	shape := func(why string) (connectedRewrite, connectedNote) { return connectedRewrite{}, connectedNote{why: why} }
	failed := func(why string) (connectedRewrite, connectedNote) {
		return connectedRewrite{}, connectedNote{why: why, failed: true}
	}
	doc, err := serializeSQL(ctx, conn, statement)
	if err != nil {
		return failed(err.Error())
	}
	node, why := connectedShape(doc)
	if why != "" {
		return shape(why)
	}
	tables, why := fromTables(node["from_table"])
	if why != "" {
		return shape(why)
	}
	if len(tables) < 2 {
		// connectedOrder's own answer, without reading the table first.
		return shape("one table")
	}
	conjuncts, why := whereConjuncts(node["where_clause"])
	if why != "" {
		return shape(why)
	}
	info := make([]tableFacts, len(tables))
	read := map[string]tableFacts{}
	for i, t := range tables {
		f, ok := read[t.ref]
		if !ok {
			if f, err = facts(t.ref); err != nil {
				return failed(fmt.Sprintf("the table %s could not be read: %s", t.ref, firstLine(err.Error())))
			}
			read[t.ref] = f
		}
		info[i] = f
	}
	conds, why := conditionTables(conjuncts, tables, info)
	if why != "" {
		return shape(why)
	}
	rows := make([]float64, len(info))
	for i := range info {
		rows[i] = info[i].rows
	}
	order, why := connectedOrder(rows, conds)
	if why != "" {
		rw, note := shape(why)
		note.writtenConnected = why == whyWrittenConnected
		return rw, note
	}
	node["from_table"] = crossChain(node["from_table"].(map[string]any), tables, order)
	want, err := json.Marshal(doc)
	if err != nil {
		return failed("the new order could not be written: " + err.Error())
	}
	var text string
	if err := conn.QueryRowContext(ctx, "SELECT json_deserialize_sql(?::JSON)::VARCHAR", string(want)).Scan(&text); err != nil {
		return failed("the new order could not be written: " + firstLine(err.Error()))
	}
	// The text DuckDB wrote must parse to the tree asked for.
	again, err := serializeSQL(ctx, conn, text)
	if err != nil {
		return failed("the statement in the new order could not be parsed: " + err.Error())
	}
	var asked any
	if err := decodeNumbers(want, &asked); err != nil {
		return failed(err.Error())
	}
	if !reflect.DeepEqual(withoutLocations(asked), withoutLocations(again)) {
		return failed("the statement in the new order does not parse to the same statement")
	}
	// And give the statement's own columns.
	own, err := describeResult(ctx, conn, statement)
	if err != nil {
		return failed("the statement's columns could not be had: " + firstLine(err.Error()))
	}
	theirs, err := describeResult(ctx, conn, text)
	if err != nil {
		return failed("the columns of the statement in the new order could not be had: " + firstLine(err.Error()))
	}
	if !reflect.DeepEqual(own, theirs) {
		return failed("the statement in the new order does not give the statement's own columns")
	}
	names := make([]string, len(order))
	for i, at := range order {
		names[i] = tables[at].name
	}
	return connectedRewrite{text: text, order: names}, connectedNote{}
}

// serializeSQL is DuckDB's parse of text (json_serialize_sql), its numbers
// kept as written: a BIGINT constant or a query_location past 2^53 would not
// survive a float64.
func serializeSQL(ctx context.Context, conn *sql.Conn, text string) (map[string]any, error) {
	var raw string
	if err := conn.QueryRowContext(ctx, "SELECT json_serialize_sql(?::VARCHAR)::VARCHAR", text).Scan(&raw); err != nil {
		return nil, fmt.Errorf("the statement could not be parsed: %s", firstLine(err.Error()))
	}
	var doc map[string]any
	if err := decodeNumbers([]byte(raw), &doc); err != nil {
		return nil, err
	}
	if failed, _ := doc["error"].(bool); failed {
		return nil, fmt.Errorf("the statement could not be parsed: %v", doc["error_message"])
	}
	return doc, nil
}

func decodeNumbers(raw []byte, v any) error {
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	if err := d.Decode(v); err != nil {
		return fmt.Errorf("the parsed statement could not be read: %w", err)
	}
	return nil
}

// withoutLocations is v without its query_location keys: where each part
// stands in the text, which a text written again changes.
func withoutLocations(v any) any {
	switch t := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, e := range t {
			if k != "query_location" {
				out[k] = withoutLocations(e)
			}
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i, e := range t {
			out[i] = withoutLocations(e)
		}
		return out
	}
	return v
}

// describeResult is the names and types of statement's result, which
// DESCRIBE binds without running it.
func describeResult(ctx context.Context, conn *sql.Conn, statement string) ([][2]string, error) {
	rows, err := conn.QueryContext(ctx, "DESCRIBE "+statement)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	var out [][2]string
	for rows.Next() {
		cells := make([]sql.NullString, len(cols))
		ptrs := make([]any, len(cols))
		for i := range cells {
			ptrs[i] = &cells[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		var name, typ string
		for i, c := range cols {
			switch c {
			case "column_name":
				name = cells[i].String
			case "column_type":
				typ = cells[i].String
			}
		}
		out = append(out, [2]string{name, typ})
	}
	return out, rows.Err()
}

// connectedShape is the statement's one SELECT node when the statement has
// the shape written again, why not otherwise.
func connectedShape(doc map[string]any) (map[string]any, string) {
	stmts, _ := doc["statements"].([]any)
	if len(stmts) != 1 {
		return nil, "not one statement"
	}
	stmt, _ := stmts[0].(map[string]any)
	node, _ := stmt["node"].(map[string]any)
	if node == nil || node["type"] != "SELECT_NODE" {
		return nil, "not a plain SELECT"
	}
	if ctes, _ := node["cte_map"].(map[string]any); ctes != nil {
		if m, _ := ctes["map"].([]any); len(m) > 0 {
			return nil, "a WITH"
		}
	}
	if node["sample"] != nil {
		return nil, "a SAMPLE"
	}
	// Anywhere in the statement, outside its subqueries: the select list,
	// WHERE, GROUP BY, HAVING, QUALIFY, ORDER BY. The FROM is judged on its
	// own (fromTables).
	for k, v := range node {
		if k != "from_table" && fromOrdered(v) {
			return nil, "a *, COLUMNS(...) or #n, which depend on the order of the FROM"
		}
	}
	return node, ""
}

func listOf(v any) []any {
	l, _ := v.([]any)
	return l
}

// fromOrdered reports whether e holds an expression whose meaning depends on
// the order of the FROM's tables, outside any subquery of it: a * or
// COLUMNS(...) (STAR), which expands to the columns in that order, or a #n
// (POSITIONAL_REFERENCE), the n-th of them.
func fromOrdered(e any) bool {
	switch t := e.(type) {
	case map[string]any:
		if t["class"] == "SUBQUERY" {
			return false
		}
		if t["class"] == "STAR" || t["class"] == "POSITIONAL_REFERENCE" || t["type"] == "POSITIONAL_REFERENCE" {
			return true
		}
		for _, v := range t {
			if fromOrdered(v) {
				return true
			}
		}
	case []any:
		for _, v := range t {
			if fromOrdered(v) {
				return true
			}
		}
	}
	return false
}

// fromTables is the tables of a FROM of cross products, in the order
// written, why not when the FROM is anything else.
func fromTables(from any) ([]fromTable, string) {
	n, _ := from.(map[string]any)
	if n == nil {
		return nil, "no FROM"
	}
	switch n["type"] {
	case "BASE_TABLE":
		if n["sample"] != nil || n["at_clause"] != nil || len(listOf(n["column_name_alias"])) > 0 {
			return nil, "a table with a SAMPLE, an AT clause or column aliases"
		}
		alias, _ := n["alias"].(string)
		table, _ := n["table_name"].(string)
		name := alias
		if name == "" {
			name = table
		}
		var parts []string
		for _, k := range []string{"catalog_name", "schema_name", "table_name"} {
			if s, _ := n[k].(string); s != "" {
				parts = append(parts, quoteIdent(s))
			}
		}
		return []fromTable{{node: n, name: strings.ToLower(name), ref: strings.Join(parts, ".")}}, ""
	case "JOIN":
		if n["ref_type"] != "CROSS" || n["join_type"] != "INNER" || n["condition"] != nil || len(listOf(n["using_columns"])) > 0 {
			return nil, "a JOIN in the FROM"
		}
		if a, _ := n["alias"].(string); a != "" || n["sample"] != nil {
			return nil, "a JOIN in the FROM"
		}
		left, why := fromTables(n["left"])
		if why != "" {
			return nil, why
		}
		right, why := fromTables(n["right"])
		if why != "" {
			return nil, why
		}
		return append(left, right...), ""
	}
	return nil, fmt.Sprintf("a %v in the FROM", n["type"])
}

func quoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

// whereConjuncts is the conditions of an AND at the top of WHERE, or WHERE
// itself when it is one condition; why not when there is none or the top is
// an OR.
func whereConjuncts(where any) ([]any, string) {
	n, _ := where.(map[string]any)
	if n == nil {
		return nil, "no WHERE"
	}
	switch n["type"] {
	case "CONJUNCTION_AND":
		var out []any
		for _, c := range listOf(n["children"]) {
			sub, why := whereConjuncts(c)
			if why != "" {
				return nil, why
			}
			out = append(out, sub...)
		}
		return out, ""
	case "CONJUNCTION_OR":
		return nil, "an OR at the top of WHERE"
	}
	return []any{n}, ""
}

// conditionTables is, for each condition, the FROM tables whose columns it
// names (by their index), why not when one of its columns cannot be placed
// in exactly one table.
func conditionTables(conjuncts []any, tables []fromTable, info []tableFacts) ([][]int, string) {
	has := make([]map[string]bool, len(tables))
	for i := range tables {
		has[i] = map[string]bool{}
		for _, c := range info[i].columns {
			has[i][strings.ToLower(c)] = true
		}
	}
	var out [][]int
	for _, c := range conjuncts {
		seen := map[int]bool{}
		var why string
		walkColumns(c, func(names []string) {
			if why != "" {
				return
			}
			at, w := placeColumn(names, tables, has)
			if w != "" {
				why = w
				return
			}
			seen[at] = true
		})
		if why != "" {
			return nil, why
		}
		var idx []int
		for i := range tables {
			if seen[i] {
				idx = append(idx, i)
			}
		}
		out = append(out, idx)
	}
	return out, ""
}

// walkColumns calls f with the names of each column e names, outside any
// subquery of it.
func walkColumns(e any, f func([]string)) {
	switch t := e.(type) {
	case map[string]any:
		if t["class"] == "SUBQUERY" {
			return
		}
		if t["class"] == "COLUMN_REF" {
			var names []string
			for _, p := range listOf(t["column_names"]) {
				s, _ := p.(string)
				names = append(names, s)
			}
			f(names)
			return
		}
		for _, v := range t {
			walkColumns(v, f)
		}
	case []any:
		for _, v := range t {
			walkColumns(v, f)
		}
	}
}

// placeColumn is the one FROM table a column belongs to.
func placeColumn(names []string, tables []fromTable, has []map[string]bool) (int, string) {
	switch len(names) {
	case 1:
		col := strings.ToLower(names[0])
		at := -1
		for i := range tables {
			if has[i][col] {
				if at >= 0 {
					return 0, fmt.Sprintf("the column %s is in more than one table", names[0])
				}
				at = i
			}
		}
		if at < 0 {
			return 0, fmt.Sprintf("the column %s is in none of the tables", names[0])
		}
		return at, ""
	case 2:
		q := strings.ToLower(names[0])
		at := -1
		for i := range tables {
			if tables[i].name == q {
				if at >= 0 {
					return 0, fmt.Sprintf("%s names more than one table", names[0])
				}
				at = i
			}
		}
		if at < 0 || !has[at][strings.ToLower(names[1])] {
			return 0, fmt.Sprintf("the column %s.%s is not a column of a table of the FROM", names[0], names[1])
		}
		return at, ""
	}
	return 0, "a column named with more than a table and a column name"
}

// connectedOrder is the order of the tables, by index: the one with the most
// rows first, then each time, of the ones a condition connects to those
// placed, the one with the fewest rows; a tie goes to the one written first.
// A condition connects table t when it names t and other tables, all placed.
// why says why there is none: the order written is connected already, or
// some table cannot be reached.
func connectedOrder(rows []float64, conds [][]int) ([]int, string) {
	n := len(rows)
	if n < 2 {
		return nil, "one table"
	}
	connects := func(t int, placed []bool) bool {
		for _, c := range conds {
			if len(c) < 2 {
				continue
			}
			mine, others := false, true
			for _, i := range c {
				if i == t {
					mine = true
				} else if !placed[i] {
					others = false
				}
			}
			if mine && others {
				return true
			}
		}
		return false
	}
	written := make([]bool, n)
	written[0] = true
	connected := true
	for t := 1; t < n && connected; t++ {
		connected = connects(t, written)
		written[t] = true
	}
	if connected {
		return nil, whyWrittenConnected
	}
	first := 0
	for i := 1; i < n; i++ {
		if rows[i] > rows[first] {
			first = i
		}
	}
	placed := make([]bool, n)
	placed[first] = true
	order := []int{first}
	for len(order) < n {
		next := -1
		for t := range n {
			if !placed[t] && connects(t, placed) && (next < 0 || rows[t] < rows[next]) {
				next = t
			}
		}
		if next < 0 {
			return nil, "a table of the FROM has no condition with the others"
		}
		placed[next] = true
		order = append(order, next)
	}
	return order, ""
}

// crossChain is a left-deep chain of cross products of tables in order,
// each link a copy of join, the statement's own first cross product.
func crossChain(join map[string]any, tables []fromTable, order []int) map[string]any {
	var chain map[string]any = tables[order[0]].node
	for _, at := range order[1:] {
		link := make(map[string]any, len(join))
		for k, v := range join {
			link[k] = v
		}
		link["left"], link["right"] = chain, tables[at].node
		chain = link
	}
	return chain
}

// sessionTableFacts is facts for connectedOrderSQL from the statement's
// session: the columns DESCRIBE gives, and DuckDB's estimate of the rows
// (correctRows of the plan of SELECT 1 FROM it). The caller plans these
// with the session's own list of optimizers off, not the order written's,
// in which a table read's estimate can be 0.
func sessionTableFacts(ctx context.Context, conn *sql.Conn) func(ref string) (tableFacts, error) {
	return func(ref string) (tableFacts, error) {
		cols, err := describeResult(ctx, conn, "SELECT * FROM "+ref)
		if err != nil {
			return tableFacts{}, err
		}
		var f tableFacts
		for _, c := range cols {
			f.columns = append(f.columns, c[0])
		}
		plan, err := explainJSON(ctx, conn, "SELECT 1 FROM "+ref)
		if err != nil {
			return tableFacts{}, err
		}
		var roots []planNode
		if err := json.Unmarshal(plan, &roots); err != nil || len(roots) == 0 {
			return tableFacts{}, fmt.Errorf("its plan could not be read")
		}
		f.rows = correctRows(&roots[0])
		return f, nil
	}
}

// ownOptimizersSQL is the SET of disabled_optimizers back to the session's
// own list, the order written's join_order taken out of it.
func ownOptimizersSQL(skipCommonSubplan bool) string {
	if s := disabledOptimizersSQL(false, skipCommonSubplan); s != "" {
		return s
	}
	return "SET disabled_optimizers = ''"
}
