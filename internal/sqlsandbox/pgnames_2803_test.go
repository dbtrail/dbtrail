package sqlsandbox

import (
	"context"
	"database/sql"
	"reflect"
	"strings"
	"testing"
)

// parseForNames is the worker's own parse of a statement.
func parseForNames(t *testing.T, text string) any {
	t.Helper()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	stmt, reason := parseStatement(context.Background(), conn, text, false)
	if reason != "" {
		t.Fatalf("%q refused: %s", text, reason)
	}
	return stmt
}

// #2803: the names PostgreSQL heads unnamed columns with, read from the
// parse. "-" is a column whose name stays the engine's. The names expected
// here are the ones PostgreSQL 16 gave for the same statements
// (TestIntegrationPGRouter asks it).
func TestPostgresColumnNames_2803(t *testing.T) {
	const from = " FROM (VALUES (1, 'x', DATE '2026-01-01')) t(a, b, d)"
	for _, c := range []struct {
		sql  string
		want string // space separated; "nil" for no renaming at all
	}{
		{"SELECT count(*)" + from, "count"},
		// Upper case: a function's name is folded, as PostgreSQL folds it.
		{"SELECT COUNT(*), Sum(a), LOWER(b), count(DISTINCT a), sum(a) FILTER (WHERE a > 0)" + from, "count sum lower count sum"},
		// Operators and constants have no name.
		{"SELECT a + 1, -a, a = 1, a IS NULL, NOT (a = 1), a BETWEEN 1 AND 2, a IN (1, 2), b || 'x', b LIKE 'x%', a = 1 AND a = 2, 1, 'x', NULL, 1.5" + from,
			strings.TrimSpace(strings.Repeat(pgUnnamed+" ", 14))},
		// A function inside an operator is not the column's name; of nested
		// calls the outermost is.
		{"SELECT sum(a) + 1, upper(lower(b)), coalesce(a, 1), nullif(a, 1), greatest(a, 1), least(a, 1)" + from,
			"?column? upper coalesce nullif greatest least"},
		// A cast of something with no name takes the type's.
		{"SELECT (a + 1)::int, CAST(a + 1 AS bigint), 1::smallint, 1::numeric(10,2), 1::decimal, 1::double precision, '1'::boolean, '2020-01-01'::date, " +
			"DATE '2020-01-01', '2020-01-01'::timestamp, '2020-01-01'::timestamptz, INTERVAL '1 day', 'yes'::bool, '{}'::json, '{1}'::int[], '00:00'::time, " +
			"'x'::bytea, '00000000-0000-0000-0000-000000000000'::uuid",
			"int4 int8 int2 numeric numeric float8 bool date date timestamp timestamptz interval bool json int4 time bytea uuid"},
		// The words TRUE and FALSE have no name, though the engine's parser
		// writes each as a cast.
		{"SELECT TRUE, false, NOT true, true::int, 'true'::boolean", "?column? ?column? ?column? int4 bool"},
		// A cast of something with a name keeps that name.
		{"SELECT a::int, count(*)::int, lower(b)::int, a::bigint::int, (a + 1)::bigint::int, t.a::bigint" + from + " GROUP BY a, b", "a count lower a int4 a"},
		// Not sure, so the engine's name stays: a type the engine holds as
		// one where PostgreSQL has several, TRY_CAST, and a column written
		// with upper case, which PostgreSQL folds unless it was quoted.
		{"SELECT (a + 1)::text, 1::varchar, 1::char(3), 1::real, 1::float, TRY_CAST(b AS int), A::int, \"A\"::int" + from, "- - - - - - - -"},
		// A name the statement gave, and a bare column: the engine's.
		{"SELECT a, t.a, count(*) AS n, count(*) AS \"count_star()\", a AS \"A\", a + 1 AS \"?\", (a)" + from + " GROUP BY a", "- - - - - - -"},
		{"SELECT count(*), count(*), a, count(*) AS count" + from + " GROUP BY a", "count count - -"},
		{"SELECT CASE WHEN a = 1 THEN 2 END, CASE WHEN a = 1 THEN 2 ELSE a END, CASE WHEN a = 1 THEN 2 ELSE lower(b)::int END, CASE WHEN a = 1 THEN 2 ELSE a + 1 END" + from,
			"case a lower case"},
		{"SELECT EXISTS (SELECT 1), (SELECT 1), (SELECT a" + from + "), (SELECT count(*)" + from + "), (SELECT 1 AS x), a IN (SELECT 1), (SELECT 1 AS \"X\"), NOT EXISTS (SELECT 1)" + from,
			"exists ?column? a count x ?column? - ?column?"},
		{"SELECT row_number() OVER (), count(*) OVER (), sum(a) OVER (PARTITION BY b)" + from, "row_number count sum"},
		// Calls the parser writes from a syntax of its own.
		{"SELECT extract(year FROM d), substring(b FROM 1 FOR 2), position('x' IN b), trim(b), trim(LEADING 'x' FROM b), d AT TIME ZONE 'UTC', ARRAY[1, 2], b COLLATE \"C\", date_part('year', d)" + from,
			"extract substring position btrim ltrim timezone array b date_part"},
		{"SELECT pg_catalog.lower(b)" + from, "lower"},
		// A set operation is named by its first SELECT.
		{"SELECT count(*)" + from + " UNION ALL SELECT a" + from, "count"},
		{"SELECT a" + from + " UNION SELECT count(*)" + from, "-"},
		{"SELECT a" + from + " UNION SELECT a + 1" + from + " UNION SELECT count(*)" + from + " ORDER BY 1", "-"},
		{"WITH c AS (SELECT a" + from + ") SELECT count(*), max(a) FROM c", "count max"},
		{"SELECT count(*) FROM (SELECT a" + from + ") s ORDER BY 1 LIMIT 1", "count"},
		{"SELECT DISTINCT lower(b)" + from, "lower"},
		{"(SELECT count(*)" + from + ")", "count"},
		// A star: an item's place is not its column's, so nothing is renamed.
		{"SELECT *" + from, "nil"},
		{"SELECT t.*, count(*) OVER ()" + from, "nil"},
		{"SELECT count(*) OVER (), *" + from, "nil"},
		{"WITH c AS (SELECT count(*)" + from + ") SELECT * FROM c", "nil"},
		{"VALUES (1, 2)", "nil"},
		{"SHOW TABLES", "nil"},
		{"DESCRIBE SELECT count(*)" + from, "nil"},
	} {
		stmt := parseForNames(t, c.sql)
		var want []string
		if c.want != "nil" {
			want = strings.Fields(c.want)
			for i, w := range want {
				if w == "-" {
					want[i] = ""
				}
			}
		}
		n := len(want)
		if want == nil {
			// Whatever the answer's width, nothing is renamed.
			for _, n := range []int{1, 2, 3, 4} {
				if got := postgresColumnNames(stmt, n); got != nil {
					t.Errorf("%s\n  with %d columns: %q, want no renaming", c.sql, n, got)
				}
			}
			continue
		}
		got := postgresColumnNames(stmt, n)
		if !reflect.DeepEqual(got, want) {
			t.Errorf("%s\n   got %q\n  want %q", c.sql, got, want)
		}
		// An answer of another width than the select list: nothing is renamed.
		if got := postgresColumnNames(stmt, n+1); got != nil {
			t.Errorf("%s\n  with %d columns for %d items: %q, want no renaming", c.sql, n+1, n, got)
		}
	}
}

// A tree the walk cannot read renames nothing and does not panic.
func TestPostgresColumnNames_unreadableTree(t *testing.T) {
	sel := func(list any) any {
		return map[string]any{"node": map[string]any{"type": "SELECT_NODE", "select_list": list}}
	}
	for name, stmt := range map[string]any{
		"nil":                           nil,
		"a string":                      "SELECT 1",
		"no node":                       map[string]any{},
		"no select list":                sel(nil),
		"an empty list":                 sel([]any{}),
		"not a list":                    sel("count"),
		"an item that is not an object": sel([]any{"count"}),
	} {
		if got := postgresColumnNames(stmt, 1); got != nil {
			t.Errorf("%s: %q, want no renaming", name, got)
		}
	}
	for name, item := range map[string]any{
		"a function with no name":   map[string]any{"class": "FUNCTION", "alias": ""},
		"a function named \"\"":     map[string]any{"class": "FUNCTION", "function_name": ""},
		"a name past 63 bytes":      map[string]any{"class": "FUNCTION", "function_name": strings.Repeat("f", 64)},
		"a class not known":         map[string]any{"class": "LAMBDA"},
		"an operator not known":     map[string]any{"class": "OPERATOR", "type": "OPERATOR_TRY"},
		"a cast with no child":      map[string]any{"class": "CAST", "cast_type": map[string]any{"id": "INTEGER"}},
		"a cast to an unknown type": map[string]any{"class": "CAST", "child": map[string]any{"class": "CONSTANT"}, "cast_type": map[string]any{"id": "HUGEINT"}},
		"a column with no names":    map[string]any{"class": "CAST", "child": map[string]any{"class": "COLUMN_REF"}, "cast_type": map[string]any{"id": "INTEGER"}},
		"a subquery with no query":  map[string]any{"class": "SUBQUERY", "subquery_type": "SCALAR"},
		"a syntax call not listed":  map[string]any{"class": "FUNCTION", "function_name": "treat", "schema": "main"},
	} {
		if got := postgresColumnNames(sel([]any{item}), 1); !reflect.DeepEqual(got, []string{""}) {
			t.Errorf("%s: %q, want the engine's name kept", name, got)
		}
	}
	// A set operation nested without end is given up on, not followed.
	loop := map[string]any{"type": "SET_OPERATION_NODE"}
	loop["left"] = loop
	if got := postgresColumnNames(map[string]any{"node": loop}, 1); got != nil {
		t.Errorf("a set operation that never ends: %q, want no renaming", got)
	}
}
