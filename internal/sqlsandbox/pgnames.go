package sqlsandbox

import "strings"

// pgUnnamed is what PostgreSQL heads a result column with when the statement
// gives it no name and its expression suggests none.
const pgUnnamed = "?column?"

// postgresColumnNames says how PostgreSQL heads the n columns of a
// statement's answer where the statement gives them no name of its own
// (Session.PostgresNames, #2803), read from the statement's parse: the rule
// is PostgreSQL's FigureColname (parse_target.c), followed over the engine's
// tree. An entry is "" where the engine's own name stays: a column with an
// alias, a bare column, and an item this walk is not sure of. Nil when the
// select list cannot be lined up with the answer's columns one to one (a
// star, a statement that is not a SELECT, a list of another length): every
// name is then the engine's.
//
// It never fails a statement: whatever it cannot read, it leaves alone.
func postgresColumnNames(stmt any, n int) (names []string) {
	defer func() {
		if recover() != nil {
			names = nil
		}
	}()
	root, _ := stmt.(map[string]any)
	if inner, ok := describeWrapped(root); ok {
		// The PostgreSQL port's Describe asks for a statement's columns as
		// SELECT * FROM (<statement>) AS dbtrail_describe LIMIT 0. A star
		// over a subquery takes the subquery's column names, on PostgreSQL
		// as on the engine: the names are the statement's own.
		return postgresColumnNames(inner, n)
	}
	list := pgSelectList(root["node"])
	if len(list) == 0 || len(list) != n {
		return nil
	}
	out := make([]string, n)
	for i, e := range list {
		item, ok := e.(map[string]any)
		if !ok || item["class"] == "STAR" {
			// A star stands for as many columns as its relation has: with
			// one in the list, an item's place is not its column's.
			return nil
		}
		if alias, _ := item["alias"].(string); alias != "" || item["class"] == "COLUMN_REF" {
			continue
		}
		name, strength, sure := pgFigureName(item)
		switch {
		case !sure:
		case strength == 0:
			out[i] = pgUnnamed
		default:
			out[i] = name
		}
	}
	return out
}

// describeWrapped reads exactly the PostgreSQL port's Describe wrapper,
// SELECT * FROM (<statement>) AS dbtrail_describe LIMIT 0, and returns the
// statement inside it. Anything else, or anything more (a WHERE, another
// limit, an offset, a star that excludes, renames or replaces, a column
// alias list), is not it: ok is false and the names are worked out as they
// always were.
func describeWrapped(root map[string]any) (inner any, ok bool) {
	node, _ := root["node"].(map[string]any)
	if node["type"] != "SELECT_NODE" || !emptyField(root, "named_param_map") {
		return nil, false
	}
	for _, k := range []string{"where_clause", "having", "qualify", "sample"} {
		if node[k] != nil {
			return nil, false
		}
	}
	for _, k := range []string{"group_expressions", "group_sets"} {
		if !emptyField(node, k) {
			return nil, false
		}
	}
	if cte, _ := node["cte_map"].(map[string]any); !emptyField(cte, "map") {
		return nil, false
	}
	if h, found := node["aggregate_handling"]; found && h != "STANDARD_HANDLING" {
		return nil, false
	}
	list, _ := node["select_list"].([]any)
	if len(list) != 1 {
		return nil, false
	}
	star, _ := list[0].(map[string]any)
	if star["class"] != "STAR" || star["type"] != "STAR" || star["expr"] != nil {
		return nil, false
	}
	if alias, _ := star["alias"].(string); alias != "" {
		return nil, false
	}
	if rel, _ := star["relation_name"].(string); rel != "" {
		return nil, false
	}
	if cols, _ := star["columns"].(bool); cols {
		return nil, false
	}
	for _, k := range []string{"exclude_list", "replace_list", "rename_list", "qualified_exclude_list"} {
		if !emptyField(star, k) {
			return nil, false
		}
	}
	from, _ := node["from_table"].(map[string]any)
	if from["type"] != "SUBQUERY" || from["alias"] != "dbtrail_describe" || from["sample"] != nil || !emptyField(from, "column_name_alias") {
		return nil, false
	}
	mods, _ := node["modifiers"].([]any)
	if len(mods) != 1 {
		return nil, false
	}
	mod, _ := mods[0].(map[string]any)
	if mod["type"] != "LIMIT_MODIFIER" || mod["offset"] != nil {
		return nil, false
	}
	limit, _ := mod["limit"].(map[string]any)
	val, _ := limit["value"].(map[string]any)
	if limit["class"] != "CONSTANT" || val["is_null"] != false || !isZero(val["value"]) {
		return nil, false
	}
	sub, _ := from["subquery"].(map[string]any)
	if sub == nil {
		return nil, false
	}
	return sub, true
}

// emptyField says m[k] is absent, null, or an empty list or object.
func emptyField(m map[string]any, k string) bool {
	switch v := m[k].(type) {
	case nil:
		return true
	case []any:
		return len(v) == 0
	case map[string]any:
		return len(v) == 0
	}
	return false
}

func isZero(v any) bool {
	switch x := v.(type) {
	case float64:
		return x == 0
	case int64:
		return x == 0
	case int:
		return x == 0
	}
	if n, ok := v.(interface{ String() string }); ok {
		return n.String() == "0"
	}
	return false
}

// pgSelectList is the select list that names a query node's columns: a
// SELECT's own, and for a set operation its leftmost SELECT's, as on
// PostgreSQL. Nil for anything else.
func pgSelectList(n any) []any {
	for range 64 {
		node, _ := n.(map[string]any)
		switch node["type"] {
		case "SELECT_NODE":
			list, _ := node["select_list"].([]any)
			return list
		case "SET_OPERATION_NODE":
			n = node["left"]
		default:
			return nil
		}
	}
	return nil
}

// pgFigureName is FigureColnameInternal: the name an expression suggests and
// how strongly (2: a name of its own; 1: a type's or "case", which a
// stronger one inside replaces; 0: none, so the column is ?column?). sure is
// false when the walk cannot tell what PostgreSQL would say.
func pgFigureName(n any) (name string, strength int, sure bool) {
	v, ok := n.(map[string]any)
	if !ok {
		return "", 0, false
	}
	switch v["class"] {
	case "COLUMN_REF":
		parts, _ := v["column_names"].([]any)
		if len(parts) == 0 {
			return "", 0, false
		}
		last, _ := parts[len(parts)-1].(string)
		// PostgreSQL folds a name written without quotes to lower case, and
		// the parse does not say whether this one had quotes.
		if !pgNameAsWritten(last) {
			return "", 0, false
		}
		return last, 2, true
	case "FUNCTION":
		if op, _ := v["is_operator"].(bool); op {
			return "", 0, true
		}
		return pgFunctionName(v)
	case "WINDOW":
		return pgFunctionName(v)
	case "CAST":
		if try, _ := v["try_cast"].(bool); try {
			return "", 0, false
		}
		if pgBooleanWord(v) {
			return "", 0, true
		}
		name, strength, sure = pgFigureName(v["child"])
		if !sure || strength > 1 {
			return name, strength, sure
		}
		if typ := pgTypeName(v["cast_type"]); typ != "" {
			return typ, 1, true
		}
		return "", 0, false
	case "COLLATE":
		return pgFigureName(v["child"])
	case "CASE":
		name, strength, sure = pgFigureName(v["else_expr"])
		if !sure || strength > 1 {
			return name, strength, sure
		}
		return "case", 1, true
	case "OPERATOR":
		switch v["type"] {
		case "OPERATOR_COALESCE":
			return "coalesce", 2, true
		case "OPERATOR_NULLIF":
			return "nullif", 2, true
		case "ARRAY_CONSTRUCTOR":
			return "array", 2, true
		case "ARRAY_EXTRACT", "ARRAY_SLICE":
			// A subscript is named after what it reads from.
			kids, _ := v["children"].([]any)
			if len(kids) == 0 {
				return "", 0, false
			}
			return pgFigureName(kids[0])
		case "OPERATOR_NOT", "OPERATOR_IS_NULL", "OPERATOR_IS_NOT_NULL", "COMPARE_IN", "COMPARE_NOT_IN":
			return "", 0, true
		}
		return "", 0, false
	case "CONSTANT", "COMPARISON", "CONJUNCTION", "BETWEEN":
		return "", 0, true
	case "SUBQUERY":
		switch v["subquery_type"] {
		case "EXISTS":
			return "exists", 2, true
		case "ANY", "NOT_EXISTS":
			return "", 0, true
		case "SCALAR":
			return pgSubqueryName(v["subquery"])
		}
	}
	return "", 0, false
}

// pgBooleanWord says whether a cast is how the engine's parser writes the
// words TRUE and FALSE: the text 't' or 'f' cast to BOOLEAN. PostgreSQL
// gives the word no name (?column?). A statement that writes that very cast
// itself ('t'::boolean, which PostgreSQL heads bool) parses to the same tree
// and is read as the word.
func pgBooleanWord(cast map[string]any) bool {
	if typ, _ := cast["cast_type"].(map[string]any); typ["id"] != "BOOLEAN" {
		return false
	}
	child, _ := cast["child"].(map[string]any)
	if child["class"] != "CONSTANT" {
		return false
	}
	val, _ := child["value"].(map[string]any)
	if typ, _ := val["type"].(map[string]any); typ["id"] != "VARCHAR" {
		return false
	}
	return val["value"] == "t" || val["value"] == "f"
}

// pgFunctionName is the name of a function or window call. count(*) is
// count_star in the engine's parse. A call the parser wrote itself from a
// syntax of its own (EXTRACT(... FROM ...), TRIM(...)) carries the schema
// "main", and PostgreSQL names each of those its own way.
func pgFunctionName(v map[string]any) (string, int, bool) {
	name, _ := v["function_name"].(string)
	if schema, _ := v["schema"].(string); schema == "main" {
		name = pgSyntaxFunctions[name]
	} else if name == "count_star" {
		name = "count"
	} else if strings.HasPrefix(name, "_bintrail_pg_round_") {
		// round(a / b, k) and round(avg(x), k), as the port writes them
		// for the copy (pgRoundMacros).
		name = "round"
	} else if strings.HasPrefix(name, "_bintrail_pg_floor_") {
		name = "floor"
	}
	// Folded: a function's name is written without quotes.
	name = foldName(name)
	if !pgNameAsWritten(name) {
		return "", 0, false
	}
	return name, 2, true
}

// pgSyntaxFunctions are the calls the engine's parser writes for a syntax of
// its own, and PostgreSQL's name for the same syntax.
var pgSyntaxFunctions = map[string]string{
	"date_part": "extract", // EXTRACT(field FROM x)
	"substring": "substring",
	"position":  "position",
	"overlay":   "overlay",
	"trim":      "btrim",
	"ltrim":     "ltrim",
	"rtrim":     "rtrim",
	"timezone":  "timezone", // x AT TIME ZONE z
}

// pgSubqueryName names a scalar subquery: by its one column's name.
func pgSubqueryName(sub any) (string, int, bool) {
	s, _ := sub.(map[string]any)
	list := pgSelectList(s["node"])
	if len(list) != 1 {
		return "", 0, false
	}
	item, ok := list[0].(map[string]any)
	if !ok || item["class"] == "STAR" {
		return "", 0, false
	}
	if alias, _ := item["alias"].(string); alias != "" {
		if !pgNameAsWritten(alias) {
			return "", 0, false
		}
		return alias, 2, true
	}
	name, strength, sure := pgFigureName(item)
	if !sure {
		return "", 0, false
	}
	if strength == 0 {
		name = pgUnnamed
	}
	return name, 2, true
}

// pgNameAsWritten says whether PostgreSQL would head a column with this name
// exactly: not empty, no upper-case letter it would fold when the name is
// written without quotes, and within the 63 bytes it keeps of a name.
func pgNameAsWritten(name string) bool {
	return name != "" && len(name) <= 63 && foldName(name) == name
}

// pgTypeName is PostgreSQL's name for the type of a cast, as it heads a
// column with it, or "" when the engine's type does not say which PostgreSQL
// type was written: VARCHAR is text, varchar and char alike, and FLOAT is
// real but also float, which PostgreSQL reads as float8.
func pgTypeName(t any) string {
	for range 8 {
		typ, _ := t.(map[string]any)
		id, _ := typ["id"].(string)
		info, _ := typ["type_info"].(map[string]any)
		switch id {
		case "LIST":
			// An array is headed with its element's type.
			t = info["child_type"]
			continue
		case "UNBOUND":
			// A type the parser left by name (json): the name as written.
			if c, _ := info["catalog"].(string); c != "" {
				return ""
			}
			if s, _ := info["schema"].(string); s != "" {
				return ""
			}
			name, _ := info["name"].(string)
			if name = foldName(name); pgNameAsWritten(name) {
				return name
			}
			return ""
		}
		return pgTypeNames[id]
	}
	return ""
}

var pgTypeNames = map[string]string{
	"SMALLINT":                 "int2",
	"INTEGER":                  "int4",
	"BIGINT":                   "int8",
	"DOUBLE":                   "float8",
	"DECIMAL":                  "numeric",
	"BOOLEAN":                  "bool",
	"DATE":                     "date",
	"TIME":                     "time",
	"TIME WITH TIME ZONE":      "timetz",
	"TIMESTAMP":                "timestamp",
	"TIMESTAMP WITH TIME ZONE": "timestamptz",
	"INTERVAL":                 "interval",
	"UUID":                     "uuid",
	"BLOB":                     "bytea",
}
