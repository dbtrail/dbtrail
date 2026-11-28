package sqlsandbox

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"reflect"
	"regexp"
	"strings"
)

// The filters an OR implies (Session.ImpliedInFilters): a condition an OR
// of a WHERE holds in every branch, added beside it so the copy can filter
// the table as it reads it.
//
// The CH-benCHmark's Q19 is the case. Its WHERE is an OR of three branches,
// and each one says ol_w_id IN (...) among its other conditions. DuckDB
// keeps the OR whole above the join, so order_line is read in full; with
// ol_w_id IN (1, 2, 3, 4, 5) added, the union of the three lists, the scan
// filters it. On a copy of 100 warehouses (DuckDB 1.5.6) Q19 went from 1.07
// s to 0.42 s, with the same answer. Q7 has the shape too, on n1.n_name
// and n2.n_name.
//
// The filter is implied: every row a branch keeps has the column equal to
// one of that branch's literals, so it is in the union, and adding the IN
// in AND keeps exactly the rows the OR keeps. Nothing is parsed here by
// hand: the statement is read through DuckDB's parse (json_serialize_sql),
// the IN made from a template DuckDB parsed, and the text written back by
// DuckDB (json_deserialize_sql).
//
// Where, in every SELECT of the statement (its subqueries, WITHs, derived
// tables and the parts of a UNION each on their own):
//
//   - its WHERE, and the ON of an INNER JOIN, taken as an AND of conditions;
//     each condition that is an OR (nested ORs taken as one) is read;
//   - never HAVING, QUALIFY, the select list or a CASE, never under a NOT,
//     and never the ON of an outer, semi or anti join, where a row the ON
//     does not match is still kept (or dropped) and a filter there would
//     change which.
//
// A column counts when every branch has, as one of its ANDs, column =
// literal (either side) or column IN (literals), with the column written
// the same way in each (ol_w_id and order_line.ol_w_id are not taken for
// one, nor are two spellings in another case), and the literals all of one
// type and none NULL. A parameter, a subquery, NOT IN, <>, IS NULL, an
// expression or a cast in its place leaves that column alone. A condition
// inside a branch's subquery is the subquery's, never counted for the
// branch. An OR that is only those restrictions on that one column is the
// IN already, and is left as it is. One IN is added per column that counts.
//
// The same literals compare under the same rules: in DuckDB an IN of
// literals is the = of each, under the column's collation and the session's
// (nocase for a MySQL copy, a _bin column's COLLATE C, PostgreSQL's byte
// comparison), trailing spaces included. That was measured, not assumed
// (TestImpliedInFilters_textComparesAsEquals), so text columns are not
// set apart.
//
// It runs in the worker on the text that runs, after the read router
// counted (refs_text.go) and decided on the statement's own text. Under
// the join check (joinguard.go) it runs before the check, so the plan the
// check judges is the plan that runs; otherwise after the join order by
// shape (shapeorder.go), whose choice it does not change.
//
// Each literal must fit the column's type, which DuckDB is asked
// (DESCRIBE): a string literal fits any column, a number only a number
// column (see literalsFit). The new text
// must parse to the tree asked for and give the statement's own columns
// (DESCRIBE), as joinconnected.go asks of its own; when a step does not
// work, the statement runs as it is, and the parent logs a warning with a
// fixed note. A statement whose own columns do not bind runs as it is with
// no note (its error is its own), and one that calls random() or another
// function of a new value per call is left alone.

// impliedInFailedNote begins the note of a rewrite that did not work. The
// rest names the step, never the engine's text, which can quote a row's
// value or the statement's.
const impliedInFailedNote = "the filter an OR implies could not be added"

// orWord is a cheap test before the parse: a statement with no OR, in any
// case, has no OR to read.
var orWord = regexp.MustCompile(`(?i)\bor\b`)

// impliedIn is what the rule did with a statement: text is what runs, the
// statement itself unless added filters were added; note says why a step
// that did not work left it as it is.
type impliedIn struct {
	text  string
	added int
	note  string
}

// impliedInFilters adds, on conn (the statement's session, its views in
// place), the filters statement's ORs imply. It never fails: a statement
// it cannot rewrite comes back byte for byte.
func impliedInFilters(ctx context.Context, conn *sql.Conn, statement string) impliedIn {
	keep := impliedIn{text: statement}
	if !orWord.MatchString(statement) {
		return keep
	}
	failed := func(step string) impliedIn {
		return impliedIn{text: statement, note: impliedInFailedNote + ": " + step}
	}
	doc, err := serializeSQL(ctx, conn, statement)
	if err != nil {
		return failed("the statement could not be parsed")
	}
	if len(listOf(doc["statements"])) != 1 {
		return keep
	}
	in, and, err := impliedInTemplates(ctx, conn)
	if err != nil {
		return failed("the template of an IN could not be parsed")
	}
	if callsVolatile(doc) {
		return keep
	}
	probe, err := serializeSQL(ctx, conn, "SELECT a FROM t")
	if err != nil {
		return failed("the template of a column's type could not be parsed")
	}
	added := addImpliedIns(doc, in, and, columnTypeOf(ctx, conn, doc, probe))
	if added == 0 {
		return keep
	}
	// The statement's own columns first: one that does not bind (a column
	// or a table misspelled) fails as it is, and that is not this rule's
	// failure to report.
	own, err := describeResult(ctx, conn, statement)
	if err != nil {
		return keep
	}
	want, err := json.Marshal(doc)
	if err != nil {
		return failed("the statement with the filters could not be written")
	}
	var text string
	if err := conn.QueryRowContext(ctx, "SELECT json_deserialize_sql(?::JSON)::VARCHAR", string(want)).Scan(&text); err != nil {
		return failed("the statement with the filters could not be written")
	}
	again, err := serializeSQL(ctx, conn, text)
	if err != nil {
		return failed("the statement with the filters could not be parsed")
	}
	var asked any
	if err := decodeNumbers(want, &asked); err != nil {
		return failed("the statement with the filters could not be read")
	}
	if !reflect.DeepEqual(withoutLocations(asked), withoutLocations(again)) {
		return failed("the statement with the filters does not parse to the statement asked for")
	}
	theirs, err := describeResult(ctx, conn, text)
	if err != nil {
		return failed("the columns of the statement with the filters could not be had")
	}
	if !reflect.DeepEqual(own, theirs) {
		return failed("the statement with the filters does not give the statement's own columns")
	}
	return impliedIn{text: text, added: added}
}

// volatileFunctions answer another value at each call. WHERE may name a
// select item by its alias, which DuckDB binds by copying the item: over
// one of these, the IN added would be one more call, apart from the
// branches', and could drop a row the OR kept.
var volatileFunctions = map[string]bool{
	"random": true, "uuid": true, "gen_random_uuid": true, "uuidv4": true, "uuidv7": true, "nextval": true, "setseed": true,
}

// callsVolatile reports whether doc calls one of volatileFunctions
// anywhere, its subqueries included.
func callsVolatile(v any) bool {
	switch t := v.(type) {
	case map[string]any:
		if t["class"] == "FUNCTION" {
			if name, _ := t["function_name"].(string); volatileFunctions[strings.ToLower(name)] {
				return true
			}
		}
		for _, e := range t {
			if callsVolatile(e) {
				return true
			}
		}
	case []any:
		for _, e := range t {
			if callsVolatile(e) {
				return true
			}
		}
	}
	return false
}

// impliedInTemplates are an IN of literals and an AND as DuckDB's parse
// writes them, read from DuckDB itself so no field of either is written
// here by hand.
func impliedInTemplates(ctx context.Context, conn *sql.Conn) (in, and map[string]any, err error) {
	doc, err := serializeSQL(ctx, conn, "SELECT 1 WHERE a IN (1, 2) AND b")
	if err != nil {
		return nil, nil, err
	}
	stmt, _ := listOf(doc["statements"])[0].(map[string]any)
	node, _ := stmt["node"].(map[string]any)
	and, _ = node["where_clause"].(map[string]any)
	if and == nil || and["type"] != "CONJUNCTION_AND" || len(listOf(and["children"])) != 2 {
		return nil, nil, fmt.Errorf("not an AND")
	}
	in, _ = listOf(and["children"])[0].(map[string]any)
	if in == nil || in["type"] != "COMPARE_IN" {
		return nil, nil, fmt.Errorf("not an IN")
	}
	return in, and, nil
}

// impliedSite is a condition the rule reads: node[key] of a SELECT (its
// where_clause) or of an INNER JOIN (its condition).
type impliedSite struct {
	node map[string]any
	key  string
	// sel is the SELECT whose FROM the condition's columns are read in.
	sel map[string]any
}

// typeOfColumn is the type DuckDB gives column in sel's FROM (DESCRIBE),
// ok false when it cannot say.
type typeOfColumn func(sel map[string]any, column any) (string, bool)

// addImpliedIns adds to every WHERE and INNER JOIN ON of doc the filters
// its ORs imply, and says how many.
func addImpliedIns(doc map[string]any, in, and map[string]any, typeOf typeOfColumn) int {
	var sites []impliedSite
	var collect func(v any, sel map[string]any)
	collect = func(v any, sel map[string]any) {
		switch t := v.(type) {
		case map[string]any:
			switch {
			case t["type"] == "SELECT_NODE":
				sel = t
				if t["where_clause"] != nil {
					sites = append(sites, impliedSite{t, "where_clause", t})
				}
			case t["type"] == "JOIN" && t["join_type"] == "INNER" && t["ref_type"] == "REGULAR" && t["condition"] != nil && sel != nil:
				sites = append(sites, impliedSite{t, "condition", sel})
			}
			for _, e := range t {
				collect(e, sel)
			}
		case []any:
			for _, e := range t {
				collect(e, sel)
			}
		}
	}
	collect(doc, nil)
	added := 0
	for _, s := range sites {
		var adds []any
		for _, c := range flatten(s.node[s.key], "CONJUNCTION_AND") {
			if n, _ := c.(map[string]any); n != nil && n["type"] == "CONJUNCTION_OR" {
				adds = append(adds, impliedByOr(flatten(n, "CONJUNCTION_OR"), in, func(column any) (string, bool) {
					return typeOf(s.sel, column)
				})...)
			}
		}
		if len(adds) == 0 {
			continue
		}
		cond, _ := s.node[s.key].(map[string]any)
		if cond != nil && cond["type"] == "CONJUNCTION_AND" {
			cond["children"] = append(append([]any{}, listOf(cond["children"])...), adds...)
		} else {
			top := cloneJSON(and).(map[string]any)
			top["children"] = append([]any{s.node[s.key]}, adds...)
			s.node[s.key] = top
		}
		added += len(adds)
	}
	return added
}

// flatten is e's children when e is a conjunction of type kind, theirs when
// they are too, and e itself otherwise.
func flatten(e any, kind string) []any {
	n, _ := e.(map[string]any)
	if n == nil || n["type"] != kind {
		return []any{e}
	}
	var out []any
	for _, c := range listOf(n["children"]) {
		out = append(out, flatten(c, kind)...)
	}
	return out
}

// restriction is what one branch says of one column: the column's node and
// the literals it may equal; poisoned when one of them cannot be taken (a
// NULL).
type restriction struct {
	column   any
	literals []any
	poisoned bool
}

// branchRestrictions is, by column (its names as JSON), what branch's ANDs
// say of it, the columns in the order written, and how many of its ANDs
// are not such a restriction.
func branchRestrictions(branch any) (map[string]*restriction, []string, int) {
	out := map[string]*restriction{}
	var keys []string
	others := 0
	for _, c := range flatten(branch, "CONJUNCTION_AND") {
		column, literals, ok := columnLiterals(c)
		if !ok {
			others++
			continue
		}
		raw, _ := json.Marshal(column.(map[string]any)["column_names"])
		key := string(raw)
		r := out[key]
		if r == nil {
			r = &restriction{column: column}
			out[key] = r
			keys = append(keys, key)
		}
		for _, l := range literals {
			if v, _ := l.(map[string]any)["value"].(map[string]any); v == nil || v["is_null"] != false {
				r.poisoned = true
			}
		}
		r.literals = append(r.literals, literals...)
	}
	return out, keys, others
}

// columnLiterals reads e as column = literal (either side) or column IN
// (literals): the column's node and the literals' nodes.
func columnLiterals(e any) (any, []any, bool) {
	n, _ := e.(map[string]any)
	if n == nil {
		return nil, nil, false
	}
	isColumn := func(v any) bool {
		m, _ := v.(map[string]any)
		return m != nil && m["class"] == "COLUMN_REF" && len(listOf(m["column_names"])) > 0
	}
	isLiteral := func(v any) bool {
		m, _ := v.(map[string]any)
		return m != nil && m["class"] == "CONSTANT" && m["type"] == "VALUE_CONSTANT"
	}
	switch n["type"] {
	case "COMPARE_EQUAL":
		if n["class"] != "COMPARISON" {
			return nil, nil, false
		}
		l, r := n["left"], n["right"]
		switch {
		case isColumn(l) && isLiteral(r):
			return l, []any{r}, true
		case isLiteral(l) && isColumn(r):
			return r, []any{l}, true
		}
	case "COMPARE_IN":
		if n["class"] != "OPERATOR" {
			return nil, nil, false
		}
		ch := listOf(n["children"])
		if len(ch) < 2 || !isColumn(ch[0]) {
			return nil, nil, false
		}
		for _, l := range ch[1:] {
			if !isLiteral(l) {
				return nil, nil, false
			}
		}
		return ch[0], ch[1:], true
	}
	return nil, nil, false
}

// impliedByOr is the INs an OR of branches implies, one per column every
// branch restricts, made from in.
func impliedByOr(branches []any, in map[string]any, typeOf func(column any) (string, bool)) []any {
	if len(branches) < 2 {
		return nil
	}
	per := make([]map[string]*restriction, len(branches))
	others := make([]int, len(branches))
	var keys []string
	for i, b := range branches {
		var k []string
		per[i], k, others[i] = branchRestrictions(b)
		if i == 0 {
			keys = k
		}
	}
	var out []any
	for _, key := range keys {
		var literals []any
		every, onlyThis := true, true
		for i := range branches {
			r := per[i][key]
			if r == nil || r.poisoned {
				every = false
				break
			}
			if others[i] > 0 || len(per[i]) > 1 {
				onlyThis = false
			}
			literals = append(literals, r.literals...)
		}
		if !every || onlyThis {
			continue
		}
		union, typ, ok := literalUnion(literals)
		if !ok {
			continue
		}
		if colType, ok := typeOf(per[0][key].column); !ok || !literalsFit(colType, typ) {
			continue
		}
		node := cloneJSON(in).(map[string]any)
		children := []any{cloneJSON(per[0][key].column)}
		for _, l := range union {
			children = append(children, cloneJSON(l))
		}
		node["children"] = children
		out = append(out, node)
	}
	return out
}

// literalUnion is literals without repeats, in the order written, and
// their type; not ok when they are not all of one type.
func literalUnion(literals []any) ([]any, string, bool) {
	var typ string
	seen := map[string]bool{}
	var out []any
	for i, l := range literals {
		v, _ := l.(map[string]any)["value"].(map[string]any)
		t, _ := v["type"].(map[string]any)
		id, _ := t["id"].(string)
		if id == "" || (i > 0 && id != typ) {
			return nil, "", false
		}
		typ = id
		raw, err := json.Marshal(withoutLocations(v))
		if err != nil {
			return nil, "", false
		}
		if !seen[string(raw)] {
			seen[string(raw)] = true
			out = append(out, l)
		}
	}
	return out, typ, len(out) > 0
}

// numericTypes are DuckDB's number types, as a literal's type id names
// them and as DESCRIBE writes them (without a DECIMAL's width).
var numericTypes = map[string]bool{
	"TINYINT": true, "SMALLINT": true, "INTEGER": true, "BIGINT": true, "HUGEINT": true,
	"UTINYINT": true, "USMALLINT": true, "UINTEGER": true, "UBIGINT": true, "UHUGEINT": true,
	"DECIMAL": true, "FLOAT": true, "DOUBLE": true,
}

// baseType is a type as DESCRIBE writes it without what follows its name:
// DECIMAL(6,2) is DECIMAL, VARCHAR COLLATE C is VARCHAR.
func baseType(t string) string {
	t = strings.ToUpper(strings.TrimSpace(t))
	if i := strings.IndexAny(t, "( "); i >= 0 {
		t = t[:i]
	}
	return t
}

// literalsFit says the IN of literals of type literal is evaluated over a
// column of type column without casting the column: a string literal is
// cast to the column's type, as the = of each does, and a number meets a
// number. A number over a text column is not taken: there the column is
// cast to the number, and the IN, read in the scan, would cast rows the OR
// never reached, and fail on one that is not a number ('abc' = 1). Any
// other literal only over a column of its own type.
func literalsFit(column, literal string) bool {
	col := baseType(column)
	switch {
	case literal == "VARCHAR":
		return true
	case numericTypes[literal]:
		return numericTypes[col]
	}
	return col == literal
}

// columnTypeOf is typeOfColumn over the statement doc on conn: the
// column's type in DESCRIBE of SELECT column FROM sel's FROM, with the
// statement's WITHs. One that does not bind there (a select item's alias,
// a column of an outer query) is not known, and its column is left alone.
func columnTypeOf(ctx context.Context, conn *sql.Conn, doc, probe map[string]any) typeOfColumn {
	root := doc
	if stmts := listOf(doc["statements"]); len(stmts) == 1 {
		if st, _ := stmts[0].(map[string]any); st != nil {
			root, _ = st["node"].(map[string]any)
		}
	}
	return func(sel map[string]any, column any) (string, bool) {
		if sel == nil || sel["from_table"] == nil {
			return "", false
		}
		q := cloneJSON(probe).(map[string]any)
		st, _ := listOf(q["statements"])[0].(map[string]any)
		node, _ := st["node"].(map[string]any)
		if node == nil {
			return "", false
		}
		node["from_table"] = cloneJSON(sel["from_table"])
		node["select_list"] = []any{cloneJSON(column)}
		ctes := sel["cte_map"]
		if m, _ := ctes.(map[string]any); m == nil || len(listOf(m["map"])) == 0 {
			if root != nil {
				ctes = root["cte_map"]
			}
		}
		if ctes != nil {
			node["cte_map"] = cloneJSON(ctes)
		}
		raw, err := json.Marshal(q)
		if err != nil {
			return "", false
		}
		var text string
		if err := conn.QueryRowContext(ctx, "SELECT json_deserialize_sql(?::JSON)::VARCHAR", string(raw)).Scan(&text); err != nil {
			return "", false
		}
		cols, err := describeResult(ctx, conn, text)
		if err != nil || len(cols) != 1 {
			return "", false
		}
		return cols[0][1], true
	}
}

// cloneJSON is a deep copy of a parsed JSON value.
func cloneJSON(v any) any {
	switch t := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, e := range t {
			out[k] = cloneJSON(e)
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i, e := range t {
			out[i] = cloneJSON(e)
		}
		return out
	}
	return v
}

// logImpliedInFilters says what the rule did with out's statement: a
// warning when a step did not work (its fixed note), info when the text
// with filters added failed in the engine, debug when it added filters.
func logImpliedInFilters(out wireResult) {
	switch {
	case out.ImpliedInFiltersNote != "":
		slog.Warn("sql on the copy: the filters an OR of a statement implies were not added; the statement ran as it was", "why", out.ImpliedInFiltersNote)
	case out.ImpliedInFilters > 0 && out.Error != nil && out.Error.Kind == errQuery:
		// Not the engine's text, which can quote a row's value.
		slog.Info("sql on the copy: a statement failed in the engine with the filters its ORs imply added", "added", out.ImpliedInFilters)
	case out.ImpliedInFilters > 0:
		slog.Debug("sql on the copy: added the filters the ORs of a statement imply", "added", out.ImpliedInFilters)
	}
}
