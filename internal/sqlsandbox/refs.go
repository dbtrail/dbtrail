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
	// Tables are the FROM-clause relations, in no particular order. A name
	// bound by the statement's own WITH appears here too: the parser does not
	// resolve names.
	Tables []TableRef `json:"tables,omitempty"`
	// CTEs are the names the statement's WITH clauses bind.
	CTEs []string `json:"ctes,omitempty"`
	// Unsure is set when the statement can depend on relations it does not
	// name: a catalog listing (SHOW TABLES), a table function (duckdb_tables()
	// reads the catalog), or a tree this walk cannot read. The caller then
	// installs every view.
	Unsure bool `json:"unsure,omitempty"`
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
func collectRefs(stmt any) Refs {
	var r Refs
	walkRefs(stmt, &r)
	return r
}

func walkRefs(node any, r *Refs) {
	switch v := node.(type) {
	case map[string]any:
		switch v["type"] {
		case "BASE_TABLE":
			name, ok := v["table_name"].(string)
			schema, ok2 := v["schema_name"].(string)
			catalog, ok3 := v["catalog_name"].(string)
			if !ok || !ok2 || !ok3 || name == "" {
				r.Unsure = true
				return
			}
			r.Tables = append(r.Tables, TableRef{Catalog: catalog, Schema: schema, Name: name})
		case "TABLE_FUNCTION":
			r.Unsure = true
		case "SHOW_REF":
			if v["query"] == nil {
				r.Unsure = true
				return
			}
		}
		// A WITH clause is serialized as {"cte_map":{"map":[{"key":"q",...}]}}.
		if cm, ok := v["cte_map"].(map[string]any); ok {
			entries, ok := cm["map"].([]any)
			if !ok {
				r.Unsure = true
				return
			}
			for _, e := range entries {
				entry, _ := e.(map[string]any)
				key, ok := entry["key"].(string)
				if !ok {
					r.Unsure = true
					return
				}
				r.CTEs = append(r.CTEs, key)
			}
		}
		for _, child := range v {
			walkRefs(child, r)
		}
	case []any:
		for _, child := range v {
			walkRefs(child, r)
		}
	}
}
