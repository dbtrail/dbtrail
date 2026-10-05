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
	// NamedJoin is set when the statement joins with USING or NATURAL. A star
	// over such a join is ordered by the join itself, and not the same way
	// everywhere: MySQL returns the join's columns first, DuckDB leaves them
	// where the left table has them.
	NamedJoin bool `json:"named_join,omitempty"`
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
		if v["class"] == "STAR" {
			r.Star = true
		}
		switch v["type"] {
		case "JOIN":
			using, _ := v["using_columns"].([]any)
			if len(using) > 0 || v["ref_type"] == "NATURAL" {
				r.NamedJoin = true
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
