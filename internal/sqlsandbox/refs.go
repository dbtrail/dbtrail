package sqlsandbox

// TableRef is one relation a statement names, spelled as the user typed it
// (case included): the catalog and schema are empty when not written.
type TableRef struct {
	Catalog string `json:"catalog,omitempty"`
	Schema  string `json:"schema,omitempty"`
	Name    string `json:"name"`
}

// Refs is what a statement names, read from DuckDB's own parse of it
// (#2029). The worker reports it before installing any view, so the caller
// can install only the views the statement reads.
type Refs struct {
	// Tables are the FROM-clause relations, in no particular order. A
	// reference that a WITH in scope binds is not one: it names the WITH.
	Tables []TableRef `json:"tables,omitempty"`
	// Unsure is set when the statement can depend on relations it does not
	// name: a catalog listing (SHOW TABLES), a table function (duckdb_tables()
	// reads the catalog), or a tree this walk cannot read. The caller then
	// installs every view.
	Unsure bool `json:"unsure,omitempty"`
	// Star is set when the statement holds a star anywhere: `*`, `t.*`,
	// `* EXCLUDE (...)`, `COLUMNS(...)`. What such a statement returns depends
	// on the order (and the set) of a relation's columns, which its text does
	// not name (#2111). `count(*)` is not one: the parser reads it as a
	// function with no argument.
	Star bool `json:"star,omitempty"`
	// StarTables are the base tables a star certainly expands, and
	// StarNamedJoin says an unqualified star stands over a join with USING
	// or NATURAL (attributeStars has the rules). Star is then left for the
	// stars the walk could not attribute.
	StarTables    []TableRef `json:"star_tables,omitempty"`
	StarNamedJoin bool       `json:"star_named_join,omitempty"`
	// NamedJoin is set when the statement joins with USING or NATURAL. A star
	// over such a join is ordered by the join itself, and not the same way
	// everywhere: MySQL returns the join's columns first, DuckDB leaves them
	// where the left table has them.
	NamedJoin bool `json:"named_join,omitempty"`
	// Natural is set when one of those joins is NATURAL. Such a join pairs on
	// every column the two tables share by name, so what it returns depends
	// on each table's column SET whether or not the statement holds a star.
	Natural bool `json:"natural,omitempty"`
}

// collectRefs walks a json_serialize_sql statement tree. A shape this walk
// passed over in silence would yield a SMALLER set, a view missing and a
// working query turned into "does not exist", so anything it is not sure of
// sets Unsure instead. The from-clause node types the pinned DuckDB produces:
// BASE_TABLE (recorded), JOIN and SUBQUERY (walked through), TABLE_FUNCTION
// (Unsure), EXPRESSION_LIST and EMPTY (name nothing), SHOW_REF (DESCRIBE,
// SUMMARIZE and SHOW x carry their query, walked through; a listing carries
// none and is Unsure). Subqueries in any expression are walked because the
// walk is over every key of every object.
//
// WITH names are scoped the way DuckDB scopes them, because a name a WITH
// binds is not a relation to install, and getting the scope wrong leaves one
// out: a WITH is visible in the query it is attached to (and that query's
// subqueries) but NOT in its own body, so `WITH duckdb_views AS (SELECT *
// FROM duckdb_views) ...` reads the catalog's duckdb_views inside the body,
// and a WITH inside a subquery binds nothing outside it. A body is walked
// with the enclosing scope only, which also reports a reference to a sibling
// or recursive WITH as a relation: one that matches no view, so the caller
// installs every view, which is the safe side.
func collectRefs(stmt any) Refs {
	var r Refs
	walkRefs(stmt, nil, &r)
	return r
}

func walkRefs(node any, scope map[string]bool, r *Refs) {
	switch v := node.(type) {
	case map[string]any:
		if v["class"] == "STAR" && v[starSeen] == nil {
			// A star attributeStars did not account for: inside an
			// expression, in an ORDER BY, anywhere this walk has no rule
			// for. It counts over every table the statement reads.
			r.Star = true
		}
		if v["class"] == "SUBQUERY" && v["subquery_type"] == "EXISTS" {
			markExists(v)
		}
		switch v["type"] {
		case "JOIN":
			using, _ := v["using_columns"].([]any)
			if len(using) > 0 || v["ref_type"] == "NATURAL" {
				r.NamedJoin = true
			}
			if v["ref_type"] == "NATURAL" {
				r.Natural = true
			}
		case "BASE_TABLE":
			name, ok := v["table_name"].(string)
			schema, ok2 := v["schema_name"].(string)
			catalog, ok3 := v["catalog_name"].(string)
			if !ok || !ok2 || !ok3 || name == "" {
				r.Unsure = true
				return
			}
			if !(catalog == "" && schema == "" && scope[foldName(name)]) {
				r.Tables = append(r.Tables, TableRef{Catalog: catalog, Schema: schema, Name: name})
			}
		case "TABLE_FUNCTION":
			r.Unsure = true
		case "SHOW_REF":
			if v["query"] == nil {
				r.Unsure = true
				return
			}
		}
		inner := scope
		// A WITH clause is serialized as {"cte_map":{"map":[{"key":"q",
		// "value":{...}}]}} on the node it is attached to.
		if cm, ok := v["cte_map"].(map[string]any); ok {
			entries, ok := cm["map"].([]any)
			if !ok {
				r.Unsure = true
				return
			}
			if len(entries) > 0 {
				inner = make(map[string]bool, len(scope)+len(entries))
				for k := range scope {
					inner[k] = true
				}
			}
			for _, e := range entries {
				entry, _ := e.(map[string]any)
				key, ok := entry["key"].(string)
				if !ok {
					r.Unsure = true
					return
				}
				inner[foldName(key)] = true
				walkRefs(entry["value"], scope, r) // the body: outer scope only
			}
		}
		if v["type"] == "SELECT_NODE" {
			attributeStars(v, inner, r)
		}
		for k, child := range v {
			if k != "cte_map" {
				walkRefs(child, inner, r)
			}
		}
	case []any:
		for _, child := range v {
			walkRefs(child, scope, r)
		}
	}
}

// starSeen and existsOnly are marks this walk leaves on the decoded tree: a
// star attributeStars accounted for, and a select that stands directly under
// EXISTS.
const (
	starSeen   = "\x00star_seen"
	existsOnly = "\x00exists_only"
)

// markExists marks the select an EXISTS asks about, when what it returns
// cannot matter: EXISTS only asks whether a row exists, and which columns a
// row has does not change that. With a LIMIT or an OFFSET it can (DISTINCT *
// over other columns is another number of rows, and an OFFSET counts them),
// so a select with any modifier is not marked, nor is a set operation.
func markExists(sub map[string]any) {
	q, _ := sub["subquery"].(map[string]any)
	node, _ := q["node"].(map[string]any)
	if node == nil || node["type"] != "SELECT_NODE" {
		return
	}
	if mods, ok := node["modifiers"].([]any); !ok || len(mods) != 0 {
		return
	}
	node[existsOnly] = true
}

// attributeStars decides, for the stars standing directly in one select's
// list, which tables' column lists they expand (#2111). Only what is certain
// is attributed; a star left unmarked is picked up by walkRefs as a star
// over everything.
//
//   - Directly under EXISTS (markExists) a star expands nothing that matters.
//   - An unqualified star expands every base table of this select's own FROM.
//     Over a join with USING or NATURAL it is also reordered by the join, and
//     not the same way everywhere (MySQL puts the join's columns first):
//     StarNamedJoin.
//   - A qualified star (t.*) expands the one FROM item called t, by its alias
//     or, with no alias, by its table name. No such item, or two, and the star
//     is left unattributed: it may name a table of an outer select. A
//     qualified star is not reordered by USING or NATURAL (measured on MySQL
//     8.4, MariaDB 11.4 and DuckDB).
//   - A FROM item that is a derived table, or the name of a WITH in scope,
//     adds nothing: its columns are what its own select lists say, and each of
//     those is a select this walk reads on its own.
//
// scope is the WITH names visible in this select.
func attributeStars(sel map[string]any, scope map[string]bool, r *Refs) {
	list, _ := sel["select_list"].([]any)
	var stars []map[string]any
	for _, e := range list {
		if m, ok := e.(map[string]any); ok && m["class"] == "STAR" {
			stars = append(stars, m)
		}
	}
	if len(stars) == 0 {
		return
	}
	if sel[existsOnly] != nil {
		for _, st := range stars {
			st[starSeen] = true
		}
		return
	}
	items, named, ok := fromItems(sel["from_table"])
	if !ok {
		return // a FROM this walk cannot read: the stars stay unattributed
	}
	for _, st := range stars {
		rel, isString := st["relation_name"].(string)
		if !isString {
			continue
		}
		var expands []fromItem
		if rel == "" {
			expands = items
			if named {
				r.StarNamedJoin = true
			}
		} else {
			for _, it := range items {
				if foldName(it.called) == foldName(rel) {
					expands = append(expands, it)
				}
			}
			if len(expands) != 1 {
				continue
			}
		}
		for _, it := range expands {
			if it.derived || (it.ref.Catalog == "" && it.ref.Schema == "" && scope[foldName(it.ref.Name)]) {
				continue
			}
			r.StarTables = append(r.StarTables, it.ref)
		}
		st[starSeen] = true
	}
}

// fromItem is one relation of a FROM clause: a base table (ref) or a derived
// table, and the name a qualified star reaches it by.
type fromItem struct {
	called  string
	derived bool
	ref     TableRef
}

// fromItems flattens a FROM clause into its relations. named says one of its
// joins is written with USING or NATURAL. ok is false for a shape this walk
// has no rule for (a table function, PIVOT, a VALUES list, a field of the
// wrong type): the caller then attributes nothing.
func fromItems(from any) (items []fromItem, named, ok bool) {
	v, isMap := from.(map[string]any)
	if !isMap {
		return nil, false, false
	}
	alias, _ := v["alias"].(string)
	switch v["type"] {
	case "JOIN":
		left, ln, lok := fromItems(v["left"])
		right, rn, rok := fromItems(v["right"])
		using, _ := v["using_columns"].([]any)
		return append(left, right...), ln || rn || len(using) > 0 || v["ref_type"] == "NATURAL", lok && rok
	case "BASE_TABLE":
		name, ok1 := v["table_name"].(string)
		schema, ok2 := v["schema_name"].(string)
		catalog, ok3 := v["catalog_name"].(string)
		if !ok1 || !ok2 || !ok3 || name == "" {
			return nil, false, false
		}
		called := alias
		if called == "" {
			called = name
		}
		return []fromItem{{called: called, ref: TableRef{Catalog: catalog, Schema: schema, Name: name}}}, false, true
	case "SUBQUERY":
		return []fromItem{{called: alias, derived: true}}, false, true
	}
	return nil, false, false
}

// foldName folds A-Z only, as DuckDB does when it compares names.
func foldName(s string) string {
	b := []byte(s)
	for i, c := range b {
		if 'A' <= c && c <= 'Z' {
			b[i] = c + ('a' - 'A')
		}
	}
	return string(b)
}
