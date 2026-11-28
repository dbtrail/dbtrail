package sqlsandbox

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"reflect"
	"strings"
	"testing"
)

// orConn is a DuckDB session with the CH-benCHmark tables Q19 and Q7 read,
// a few rows in each, and a table t of plain columns for the edge cases.
func orConn(t *testing.T) *sql.Conn {
	t.Helper()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	for _, stmt := range orTables {
		if _, err := conn.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	return conn
}

// orTables are the tables of the tests: Q19's order_line and item, Q7's
// supplier, stock, order_line, orders, customer and nation, filled so both
// reports answer rows, and t, u for the edge cases.
var orTables = []string{
	`CREATE TABLE item AS SELECT range::INT AS i_id, (range % 7 + 1)::INT AS i_price,
		chr(97 + (range % 4)::INT) || 'x' || chr(97 + (range % 3)::INT) AS i_data FROM range(1, 400)`,
	`CREATE TABLE order_line AS SELECT (range % 50)::INT AS ol_o_id, (range % 6 + 1)::INT AS ol_w_id, (range % 10)::INT AS ol_d_id,
		(range % 399 + 1)::INT AS ol_i_id, (range % 12)::INT AS ol_quantity, (range % 97)::DECIMAL(6,2) AS ol_amount,
		(range % 6 + 1)::INT AS ol_supply_w_id, TIMESTAMP '2007-01-02' + to_days((range % 400)::INT) AS ol_delivery_d FROM range(20000)`,
	`CREATE TABLE orders AS SELECT (range % 50)::INT AS o_id, (range % 6 + 1)::INT AS o_w_id, (range % 10)::INT AS o_d_id,
		(range % 30)::INT AS o_c_id, TIMESTAMP '2007-01-02' AS o_entry_d FROM range(3000)`,
	`CREATE TABLE customer AS SELECT (range % 30)::INT AS c_id, (range % 6 + 1)::INT AS c_w_id, (range % 10)::INT AS c_d_id,
		chr(65 + (range % 5)::INT) AS c_state FROM range(1800)`,
	`CREATE TABLE stock AS SELECT (range % 399 + 1)::INT AS s_i_id, (range % 6 + 1)::INT AS s_w_id FROM range(2394)`,
	`CREATE TABLE supplier AS SELECT range::INT AS su_suppkey, (range % 5)::INT AS su_nationkey FROM range(10000)`,
	`CREATE TABLE nation AS SELECT range::INT AS n_nationkey, ['Germany', 'Cambodia', 'Peru', 'Chad', 'Fiji'][range + 1] AS n_name FROM range(5)`,
	`CREATE TABLE t AS SELECT range::INT AS a, (range % 3)::INT AS b, (range % 5)::INT AS c, chr(97 + (range % 3)::INT) AS s FROM range(30)`,
	`CREATE TABLE u AS SELECT range::INT AS a, (range % 4)::INT AS d FROM range(30)`,
	// w.v is text, and 'abc' is on a row the join with u drops.
	`CREATE TABLE w AS SELECT * FROM (VALUES (1, '1'), (2, '2'), (5, '1'), (99, 'abc')) v(a, v)`,
}

// chQ19 is the CH-benCHmark's Q19 as the benchmark writes it.
const chQ19 = `SELECT sum(ol_amount) AS revenue FROM order_line, item WHERE (ol_i_id = i_id AND i_data LIKE '%a' AND ol_quantity >= 1 AND ol_quantity <= 10 AND i_price BETWEEN 1 AND 400000 AND ol_w_id IN (1, 2, 3)) OR (ol_i_id = i_id AND i_data LIKE '%b' AND ol_quantity >= 1 AND ol_quantity <= 10 AND i_price BETWEEN 1 AND 400000 AND ol_w_id IN (1, 2, 4)) OR (ol_i_id = i_id AND i_data LIKE '%c' AND ol_quantity >= 1 AND ol_quantity <= 10 AND i_price BETWEEN 1 AND 400000 AND ol_w_id IN (1, 5, 3))`

// chQ7 is the CH-benCHmark's Q7, with nations the tables hold.
const chQ7 = `SELECT su_nationkey AS supp_nation, substr(c_state, 1, 1) AS cust_nation, extract(year FROM o_entry_d) AS l_year, sum(ol_amount) AS revenue FROM supplier, stock, order_line, orders, customer, nation n1, nation n2 WHERE ol_supply_w_id = s_w_id AND ol_i_id = s_i_id AND mod((s_w_id * s_i_id), 10000) = su_suppkey AND ol_w_id = o_w_id AND ol_d_id = o_d_id AND ol_o_id = o_id AND c_id = o_c_id AND c_w_id = o_w_id AND c_d_id = o_d_id AND su_nationkey = n1.n_nationkey AND ascii(substr(c_state, 1, 1)) - 65 = n2.n_nationkey AND ((n1.n_name = 'Germany' AND n2.n_name = 'Cambodia') OR (n1.n_name = 'Cambodia' AND n2.n_name = 'Germany')) GROUP BY su_nationkey, substr(c_state, 1, 1), extract(year FROM o_entry_d) ORDER BY su_nationkey, cust_nation, l_year`

// Each edge case, written down before the rule: what it adds (in DuckDB's
// own text of the IN) or that the statement is left exactly as it is.
func TestImpliedInFilters(t *testing.T) {
	conn := orConn(t)
	ctx := context.Background()
	for _, tc := range []struct {
		name string
		sql  string
		// adds are the IN conjuncts the rewrite must hold, in DuckDB's text;
		// none: the statement comes back byte for byte.
		adds []string
		// not are texts the rewrite must not hold.
		not []string
	}{
		{name: "Q19", sql: chQ19, adds: []string{"(ol_w_id IN (1, 2, 3, 4, 5))"}},
		{name: "Q7: one per column", sql: chQ7, adds: []string{"(n1.n_name IN ('Germany', 'Cambodia'))", "(n2.n_name IN ('Cambodia', 'Germany'))"}},
		{name: "a branch without the column", sql: "SELECT a FROM t WHERE (b = 1 AND c = 2) OR (c = 3)", adds: []string{"(c IN (2, 3))"}, not: []string{"b IN"}},
		{name: "a branch with no restriction at all", sql: "SELECT a FROM t WHERE (b = 1 AND c = 2) OR (a > 3)"},
		{name: "= and IN mixed, repeated literals", sql: "SELECT a FROM t WHERE (b = 1 AND c > 0) OR (b IN (2, 1) AND c < 4) OR (b IN (2, 2) AND a = 7)", adds: []string{"(b IN (1, 2))"}},
		{name: "the literal on the left", sql: "SELECT a FROM t WHERE (1 = b AND c > 0) OR (b = 2 AND c < 4)", adds: []string{"(b IN (1, 2))"}},
		{name: "nested ORs", sql: "SELECT a FROM t WHERE ((b = 1 AND c > 0) OR (b = 2 AND c < 4)) OR (b = 0 AND a = 3)", adds: []string{"(b IN (1, 2, 0))"}},
		{name: "the OR as one of the AND", sql: "SELECT a FROM t WHERE a > 2 AND ((b = 1 AND c > 0) OR (b = 2 AND c < 4))", adds: []string{"(b IN (1, 2))"}},
		{name: "two ORs, each its own", sql: "SELECT a FROM t WHERE ((b = 1 AND c > 0) OR (b = 2 AND c < 4)) AND ((c = 1 AND a > 0) OR (c = 3 AND a < 9))", adds: []string{"(b IN (1, 2))", "(c IN (1, 3))"}},
		// The OR is the IN already: nothing to gain.
		{name: "only the column's restrictions", sql: "SELECT a FROM t WHERE b = 1 OR b = 2"},
		{name: "only the column's restrictions, IN", sql: "SELECT a FROM t WHERE b IN (1, 2) OR b = 0"},
		{name: "qualified in one branch only", sql: "SELECT a FROM t WHERE (b = 1 AND c > 0) OR (t.b = 2 AND c < 4)"},
		{name: "an alias in one branch only", sql: "SELECT x.a FROM t x WHERE (b = 1 AND c > 0) OR (x.b = 2 AND c < 4)"},
		{name: "the same qualified name", sql: "SELECT x.a FROM t x WHERE (x.b = 1 AND c > 0) OR (x.b = 2 AND c < 4)", adds: []string{"(x.b IN (1, 2))"}},
		{name: "another case", sql: "SELECT a FROM t WHERE (b = 1 AND c > 0) OR (B = 2 AND c < 4)"},
		{name: "NOT IN", sql: "SELECT a FROM t WHERE (b = 1 AND c > 0) OR (b NOT IN (2) AND c < 4)"},
		{name: "<>", sql: "SELECT a FROM t WHERE (b = 1 AND c > 0) OR (b <> 2 AND c < 4)"},
		{name: "IS NULL", sql: "SELECT a FROM t WHERE (b = 1 AND c > 0) OR (b IS NULL AND c < 4)"},
		{name: "a parameter ?", sql: "SELECT a FROM t WHERE (b = 1 AND c > 0) OR (b = ? AND c < 4)"},
		{name: "a parameter $1", sql: "SELECT a FROM t WHERE (b = 1 AND c > 0) OR (b = $1 AND c < 4)"},
		{name: "a parameter in an IN", sql: "SELECT a FROM t WHERE (b = 1 AND c > 0) OR (b IN (2, ?) AND c < 4)"},
		{name: "a subquery in the IN", sql: "SELECT a FROM t WHERE (b = 1 AND c > 0) OR (b IN (SELECT d FROM u) AND c < 4)"},
		{name: "an expression, not a column", sql: "SELECT a FROM t WHERE (b + 0 = 1 AND c > 0) OR (b + 0 = 2 AND c < 4)"},
		{name: "a column on both sides", sql: "SELECT a FROM t WHERE (b = c AND a > 0) OR (b = 2 AND c < 4)"},
		{name: "a COLLATE", sql: "SELECT a FROM t WHERE (s COLLATE nocase = 'a' AND c > 0) OR (s COLLATE nocase = 'b' AND c < 4)"},
		{name: "literals of two types", sql: "SELECT a FROM t WHERE (b = 1 AND c > 0) OR (b = '1' AND c < 4)"},
		{name: "a NULL", sql: "SELECT a FROM t WHERE (b = 1 AND c > 0) OR (b = NULL AND c < 4)"},
		{name: "a NULL in an IN", sql: "SELECT a FROM t WHERE (b = 1 AND c > 0) OR (b IN (2, NULL) AND c < 4)"},
		{name: "a cast literal", sql: "SELECT a FROM t WHERE (b = 1::BIGINT AND c > 0) OR (b = 2::BIGINT AND c < 4)"},
		{name: "an OR in the select list", sql: "SELECT (b = 1 AND c > 0) OR (b = 2 AND c < 4) AS x FROM t"},
		{name: "an OR in a CASE", sql: "SELECT sum(CASE WHEN (b = 1 AND c > 0) OR (b = 2 AND c < 4) THEN 1 ELSE 0 END) AS x FROM t"},
		{name: "an OR in HAVING", sql: "SELECT b, count(*) AS n FROM t GROUP BY b HAVING (b = 1 AND count(*) > 0) OR (b = 2 AND count(*) < 40)"},
		{name: "an OR under a NOT", sql: "SELECT a FROM t WHERE NOT ((b = 1 AND c > 0) OR (b = 2 AND c < 4))"},
		{name: "an INNER JOIN's ON", sql: "SELECT t.a FROM t JOIN u ON t.a = u.a AND ((t.b = 1 AND u.d > 0) OR (t.b = 2 AND u.d < 3))", adds: []string{"(t.b IN (1, 2))"}},
		{name: "a LEFT JOIN's ON", sql: "SELECT t.a, u.d FROM t LEFT JOIN u ON t.a = u.a AND ((t.b = 1 AND u.d > 0) OR (t.b = 2 AND u.d < 3))"},
		{name: "a RIGHT JOIN's ON", sql: "SELECT t.a, u.d FROM t RIGHT JOIN u ON t.a = u.a AND ((u.d = 1 AND t.b > 0) OR (u.d = 2 AND t.b < 3))"},
		{name: "a FULL JOIN's ON", sql: "SELECT t.a, u.d FROM t FULL JOIN u ON t.a = u.a AND ((u.d = 1 AND t.b > 0) OR (u.d = 2 AND t.b < 3))"},
		{name: "a SEMI JOIN's ON", sql: "SELECT t.a FROM t SEMI JOIN u ON t.a = u.a AND ((u.d = 1 AND t.b > 0) OR (u.d = 2 AND t.b < 3))"},
		// The WHERE above an outer join is the statement's filter, not the
		// join's: what it keeps does not change.
		{name: "WHERE over a LEFT JOIN", sql: "SELECT t.a, u.d FROM t LEFT JOIN u ON t.a = u.a WHERE (t.b = 1 AND u.d > 0) OR (t.b = 2 AND c < 3)", adds: []string{"(t.b IN (1, 2))"}},
		{name: "a subquery's own WHERE", sql: "SELECT a FROM t WHERE a IN (SELECT a FROM u WHERE (d = 1 AND a > 2) OR (d = 3 AND a < 20))", adds: []string{"(d IN (1, 3))"}},
		{name: "a WITH's own WHERE", sql: "WITH w AS (SELECT a, b FROM t WHERE (b = 1 AND c > 0) OR (b = 2 AND c < 4)) SELECT count(*) AS n FROM w", adds: []string{"(b IN (1, 2))"}},
		{name: "a derived table's WHERE", sql: "SELECT count(*) AS n FROM (SELECT a FROM t WHERE (b = 1 AND c > 0) OR (b = 2 AND c < 4)) x", adds: []string{"(b IN (1, 2))"}},
		{name: "each part of a UNION", sql: "SELECT a FROM t WHERE (b = 1 AND c > 0) OR (b = 2 AND c < 4) UNION ALL SELECT a FROM u WHERE (d = 0 AND a > 1) OR (d = 3 AND a < 9)", adds: []string{"(b IN (1, 2))", "(d IN (0, 3))"}},
		// A restriction inside a branch's subquery is the subquery's, never
		// counted in the outer union.
		{name: "levels do not mix", sql: "SELECT a FROM t WHERE (b = 1 AND EXISTS (SELECT 1 FROM u WHERE d = 7)) OR (b = 2 AND c < 4)", adds: []string{"(b IN (1, 2))"}, not: []string{"7)) AND", "IN (1, 2, 7)", "d IN"}},
		{name: "only the subquery restricts", sql: "SELECT a FROM t WHERE (EXISTS (SELECT 1 FROM u WHERE d = 7) AND c > 0) OR (EXISTS (SELECT 1 FROM u WHERE d = 8) AND c < 4)"},
		{name: "random() by its alias", sql: "SELECT (random() * 2)::INT AS r FROM t WHERE (r = 0 AND a > 1) OR (r = 1 AND a < 5)"},
		{name: "random() anywhere", sql: "SELECT a, random() AS x FROM t WHERE (b = 1 AND c > 0) OR (b = 2 AND c < 4)"},
		// A number over a text column casts the column, and the IN, read in
		// the scan, would cast 'abc', a row the OR never reaches.
		{name: "a number against a text column", sql: "SELECT w.a FROM w JOIN u ON w.a = u.a WHERE (w.v = 1 AND u.d > 0) OR (w.v = 2 AND u.d < 3)"},
		{name: "a string against a text column", sql: "SELECT w.a FROM w JOIN u ON w.a = u.a WHERE (w.v = '1' AND u.d > 0) OR (w.v = '2' AND u.d < 3)", adds: []string{"(w.v IN ('1', '2'))"}},
		{name: "a string against a number column", sql: "SELECT a FROM t WHERE (b = '1' AND c > 0) OR (b = '2' AND c < 4)", adds: []string{"(b IN ('1', '2'))"}},
		// WHERE may name a select item; DuckDB binds it by copying the item.
		{name: "a select item's alias", sql: "SELECT CAST(w.v AS INTEGER) AS r FROM w JOIN u ON w.a = u.a WHERE (r = 1 AND u.d > 0) OR (r = 2 AND u.d < 3)"},
		{name: "a column of an outer query", sql: "SELECT a FROM t WHERE EXISTS (SELECT 1 FROM u WHERE (t.b = 1 AND u.d > 0) OR (t.b = 2 AND u.d < 3))"},
		{name: "no OR", sql: "SELECT a FROM t WHERE b = 1 AND c > 0"},
		{name: "no WHERE", sql: "SELECT a FROM t"},
		{name: "'or' in a string only", sql: "SELECT a FROM t WHERE s = 'or' AND c > 0"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := impliedInFilters(ctx, conn, tc.sql)
			if got.note != "" {
				t.Fatalf("note %q", got.note)
			}
			if len(tc.adds) == 0 {
				if got.text != tc.sql || got.added != 0 {
					t.Fatalf("changed (%d): %s", got.added, got.text)
				}
				return
			}
			if got.added != len(tc.adds) {
				t.Errorf("added %d, want %d: %s", got.added, len(tc.adds), got.text)
			}
			for _, a := range tc.adds {
				if !strings.Contains(got.text, a) {
					t.Errorf("no %s in %s", a, got.text)
				}
			}
			for _, n := range tc.not {
				if strings.Contains(got.text, n) {
					t.Errorf("%s in %s", n, got.text)
				}
			}
			// What it answers does not change.
			if want, have := orRows(t, conn, tc.sql), orRows(t, conn, got.text); want != have {
				t.Errorf("rows changed:\n%s\nwant\n%s", have, want)
			}
		})
	}
}

// orRows is what statement answers on conn, as text, its rows sorted.
func orRows(t *testing.T, conn *sql.Conn, statement string) string {
	t.Helper()
	rows, err := conn.QueryContext(context.Background(), "SELECT * FROM ("+statement+") ORDER BY ALL")
	if err != nil {
		t.Fatalf("%s: %v", statement, err)
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}
	var b strings.Builder
	for rows.Next() {
		cells := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range cells {
			ptrs[i] = &cells[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatal(err)
		}
		fmt.Fprintln(&b, cells...)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return b.String()
}

// Q19 and Q7 answer rows, and the same rows rewritten.
func TestImpliedInFilters_q19q7SameAnswer(t *testing.T) {
	conn := orConn(t)
	ctx := context.Background()
	for name, q := range map[string]string{"Q19": chQ19, "Q7": chQ7} {
		got := impliedInFilters(ctx, conn, q)
		if got.added == 0 || got.note != "" {
			t.Fatalf("%s: not rewritten: %+v", name, got)
		}
		want, have := orRows(t, conn, q), orRows(t, conn, got.text)
		if want == "" || strings.Contains(want, "<nil>") && name == "Q19" {
			t.Fatalf("%s answers nothing to compare: %q", name, want)
		}
		if want != have {
			t.Errorf("%s: rows changed:\n%s\nwant\n%s", name, have, want)
		}
	}
}

// Text compares under the copy's collations as the statement's own = does:
// the IN added folds case under MySQL's nocase, keeps it under COLLATE C
// (a _bin column's view) and under PostgreSQL's byte comparison, and does
// not ignore a trailing space. Measured, so the rule needs no limit on
// text columns.
func TestImpliedInFilters_textComparesAsEquals(t *testing.T) {
	ctx := context.Background()
	for _, collation := range []string{"nocase", ""} {
		t.Run("default_collation="+collation, func(t *testing.T) {
			conn := orConn(t)
			for _, stmt := range []string{
				"SET default_collation = '" + collation + "'",
				`CREATE TABLE txt_raw AS SELECT * FROM (VALUES (1, 'abc', 1), (2, 'ABC', 1), (3, 'abc ', 1), (4, 'x', 2), (5, 'X', 2), (6, 'abc', 2)) v(id, s, k)`,
				"CREATE VIEW txt AS SELECT id, s, k FROM txt_raw",
				"CREATE VIEW txt_bin AS SELECT id, s COLLATE C AS s, k FROM txt_raw",
			} {
				if _, err := conn.ExecContext(ctx, stmt); err != nil {
					t.Fatalf("%s: %v", stmt, err)
				}
			}
			// And through Parquet files, as the copy reads its tables: one
			// file per row, so each row group's min and max are that row's,
			// where a filter pushed into the scan could skip it.
			dir := t.TempDir()
			for id := 1; id <= 6; id++ {
				stmt := fmt.Sprintf("COPY (SELECT * FROM txt_raw WHERE id = %d) TO '%s/txt_%d.parquet' (FORMAT parquet)", id, dir, id)
				if _, err := conn.ExecContext(ctx, stmt); err != nil {
					t.Fatalf("%s: %v", stmt, err)
				}
			}
			for _, stmt := range []string{
				"CREATE VIEW txt_pq AS SELECT id, s, k FROM read_parquet('" + dir + "/*.parquet')",
				"CREATE VIEW txt_pq_bin AS SELECT id, s COLLATE C AS s, k FROM read_parquet('" + dir + "/*.parquet')",
			} {
				if _, err := conn.ExecContext(ctx, stmt); err != nil {
					t.Fatalf("%s: %v", stmt, err)
				}
			}
			for _, view := range []string{"txt", "txt_bin", "txt_pq", "txt_pq_bin"} {
				for _, q := range []string{
					"SELECT id FROM " + view + " WHERE (s = 'ABC' AND k < 2) OR (s IN ('x') AND k > 1)",
					"SELECT id FROM " + view + " WHERE (s = 'abc' AND k < 2) OR (s = 'X' AND k > 1)",
					"SELECT id FROM " + view + " WHERE (s = 'abc ' AND k < 2) OR (s = 'x' AND k > 1)",
				} {
					got := impliedInFilters(ctx, conn, q)
					if got.added != 1 || got.note != "" {
						t.Fatalf("%s: %+v", q, got)
					}
					if want, have := orRows(t, conn, q), orRows(t, conn, got.text); want != have {
						t.Errorf("%s: rows changed:\n%s\nwant\n%s", q, have, want)
					}
				}
			}
		})
	}
}

// A statement DuckDB cannot parse comes back as it is, with a note of
// fixed text: no error, and no part of the statement in the note. One
// whose own columns do not bind comes back with no note.
func TestImpliedInFilters_failedStepKeepsTheStatement(t *testing.T) {
	conn := orConn(t)
	ctx := context.Background()
	bad := "SELEC Secret FROM t WHERE (b = 1 AND c > 0) OR (b = 2 AND c < 4)"
	got := impliedInFilters(ctx, conn, bad)
	if got.text != bad || got.added != 0 || !strings.HasPrefix(got.note, impliedInFailedNote+": ") || strings.Contains(got.note, "Secret") {
		t.Fatalf("%+v", got)
	}
	// A view whose table is gone: the statement parses, the check of its
	// columns (DESCRIBE) does not bind.
	for _, stmt := range []string{"CREATE TABLE gone (a INTEGER, b INTEGER)", "CREATE VIEW vg AS SELECT a, b FROM gone", "DROP TABLE gone"} {
		if _, err := conn.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	// Its own error, not the rule's: no note, so no warning.
	q := "SELECT a FROM vg WHERE (b = 1 AND a > 0) OR (b = 2 AND a < 4)"
	got = impliedInFilters(ctx, conn, q)
	if got.text != q || got.added != 0 || got.note != "" {
		t.Fatalf("%+v", got)
	}
}

// In the worker: the option rewrites only the text that runs, after the
// join order is picked, which it does not change: Q19's OR at the top of
// its WHERE keeps it out of the join order by shape, rewritten or not.
func TestWorker_impliedInFilters(t *testing.T) {
	if (Session{}).ImpliedInFilters {
		t.Fatal("on by default")
	}
	dir := t.TempDir()
	views := strings.Join(orViews(), ";\n") + ";"
	job := func(statement string, on, byShape bool) wireJob {
		return wireJob{CopyDirs: []string{dir}, ViewsSQL: views, SQL: statement, ImpliedInFilters: on, JoinOrderByShape: byShape,
			Threads: 1, MemoryLimit: "256MB", MaxRows: 1000, MaxResultBytes: 1 << 20}
	}
	for _, tc := range []struct {
		name  string
		sql   string
		added int
		order string
	}{
		{"Q19", chQ19, 1, ""},
		{"Q7", chQ7, 2, ""},
		{"no OR", "SELECT count(*) AS n FROM t WHERE b = 1", 0, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			want := runJob(job(tc.sql, false, true), nil, os.Stderr)
			if want.Error != nil || want.ImpliedInFilters != 0 {
				t.Fatalf("without the option: %+v %d", want.Error, want.ImpliedInFilters)
			}
			res := runJob(job(tc.sql, true, true), nil, os.Stderr)
			if res.Error != nil {
				t.Fatalf("with the option: %+v", res.Error)
			}
			if res.ImpliedInFilters != tc.added || res.ImpliedInFiltersNote != "" {
				t.Errorf("added %d note %q, want %d", res.ImpliedInFilters, res.ImpliedInFiltersNote, tc.added)
			}
			if res.JoinOrderByShape != tc.order || want.JoinOrderByShape != tc.order {
				t.Errorf("join order by shape %q (without the option %q), want %q", res.JoinOrderByShape, want.JoinOrderByShape, tc.order)
			}
			if !reflect.DeepEqual(res.Rows, want.Rows) || !reflect.DeepEqual(res.Columns, want.Columns) {
				t.Errorf("rows %v %v, want %v %v", res.Columns, res.Rows, want.Columns, want.Rows)
			}
		})
	}
}

// Under the join check the filters are added before it, so the plan it
// judges is the one that runs.
func TestWorker_impliedInFiltersUnderTheJoinCheck(t *testing.T) {
	dir := t.TempDir()
	views := strings.Join(orViews(), ";\n") + ";"
	job := func(on bool) wireJob {
		return wireJob{CopyDirs: []string{dir}, ViewsSQL: views, SQL: chQ19, ImpliedInFilters: on,
			JoinNDV: map[string]float64{"ol_i_id": 399, "i_id": 399}, TryConnectedJoinOrder: true,
			Threads: 1, MemoryLimit: "256MB", MaxRows: 1000, MaxResultBytes: 1 << 20}
	}
	want := runJob(job(false), nil, os.Stderr)
	if want.Error != nil {
		t.Fatalf("without the option: %+v", want.Error)
	}
	res := runJob(job(true), nil, os.Stderr)
	if res.Error != nil || res.ImpliedInFilters != 1 || res.ImpliedInFiltersNote != "" {
		t.Fatalf("with the option: %+v %d %q", res.Error, res.ImpliedInFilters, res.ImpliedInFiltersNote)
	}
	if !reflect.DeepEqual(res.Rows, want.Rows) {
		t.Errorf("rows %v, want %v", res.Rows, want.Rows)
	}
}

// orViews are orTables as views over ranges, as the worker's views script
// makes the copy's tables.
func orViews() []string {
	out := make([]string, len(orTables))
	for i, s := range orTables {
		out[i] = strings.Replace(s, "CREATE TABLE", "CREATE VIEW", 1)
	}
	return out
}

// Through the real worker process: the option travels with the job.
func TestRunner_impliedInFilters(t *testing.T) {
	dir := t.TempDir()
	r := newTestRunner(t, testLimits())
	ctx := context.Background()
	job := Job{CopyDirs: []string{dir}, ViewsSQL: strings.Join(orViews(), ";\n") + ";", SQL: chQ19}
	want, err := r.Run(ctx, job)
	if err != nil {
		t.Fatal(err)
	}
	job.Session.ImpliedInFilters = true
	got, err := r.Run(ctx, job)
	if err != nil {
		t.Fatal(err)
	}
	if got.ImpliedInFilters != 1 || want.ImpliedInFilters != 0 {
		t.Errorf("added %d (without the option %d)", got.ImpliedInFilters, want.ImpliedInFilters)
	}
	if fmt.Sprint(got.Rows) != fmt.Sprint(want.Rows) {
		t.Errorf("rows %v, want %v", got.Rows, want.Rows)
	}
}

// A rewrite that failed is a warning with the fixed note; one that added
// filters is debug; nothing else is logged.
func TestLogImpliedInFilters(t *testing.T) {
	var buf bytes.Buffer
	was := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(was) })
	logImpliedInFilters(wireResult{ImpliedInFiltersNote: impliedInFailedNote})
	logImpliedInFilters(wireResult{ImpliedInFilters: 2})
	logImpliedInFilters(wireResult{})
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 2 || !strings.Contains(lines[0], "level=WARN") || !strings.Contains(lines[0], impliedInFailedNote) ||
		!strings.Contains(lines[1], "level=DEBUG") || !strings.Contains(lines[1], "added=2") {
		t.Errorf("logged:\n%s", buf.String())
	}
}

// A column compared with literals of another type: where the filter is
// added the IN casts them as the = does, and the rows do not change.
func TestImpliedInFilters_mixedTypesSameAnswer(t *testing.T) {
	conn := orConn(t)
	ctx := context.Background()
	if _, err := conn.ExecContext(ctx, `CREATE TABLE m AS SELECT * FROM (VALUES
		('01', DATE '2020-01-02', 1.50::DECIMAL(5,2), 1), ('1', DATE '2020-01-03', 2.25::DECIMAL(5,2), 2),
		('2', DATE '2020-01-04', 1.00::DECIMAL(5,2), 1), ('02', DATE '2020-01-02', 2.50::DECIMAL(5,2), 2),
		('2 ', DATE '2020-01-03', 1.5::DECIMAL(5,2), 2)) v(v, d, n, k)`); err != nil {
		t.Fatal(err)
	}
	for q, added := range map[string]int{
		// Numbers over a text column: left alone (literalsFit).
		"SELECT v FROM m WHERE (v = 1 AND k < 2) OR (v = 2 AND k > 1)":       0,
		"SELECT v FROM m WHERE (v IN (1, 2) AND k < 2) OR (v = 2 AND k > 1)": 0,
		"SELECT v FROM m WHERE (v = '01' AND k < 2) OR (v = '2 ' AND k > 1)": 1,
		"SELECT v FROM m WHERE (d = '2020-01-02' AND k < 2) OR (d = '2020-1-3' AND k > 1)": 1,
		"SELECT v FROM m WHERE (n = 1.5 AND k < 2) OR (n = 2.25 AND k > 1)":                1,
		"SELECT v FROM m WHERE (n = 1.50 AND k < 2) OR (n IN (2.5, 1.5) AND k > 1)":        1,
		"SELECT v FROM m WHERE (n = 1 AND k < 2) OR (n = 2 AND k > 1)":                     1,
	} {
		got := impliedInFilters(ctx, conn, q)
		if got.note != "" || got.added != added {
			t.Fatalf("%s: %+v, want %d added", q, got, added)
		}
		if want, have := orRows(t, conn, q), orRows(t, conn, got.text); want != have {
			t.Errorf("%s (added %d): rows changed:\n%s\nwant\n%s", q, got.added, have, want)
		}
	}
}

// Q19 and Q7 over Parquet files, as the copy reads its tables, a few rows
// per row group: the same answer with the filters pushed into the scan.
func TestImpliedInFilters_q19q7OverParquet(t *testing.T) {
	conn := orConn(t)
	ctx := context.Background()
	dir := t.TempDir()
	for _, tb := range []string{"order_line", "item", "orders", "customer", "stock", "supplier", "nation"} {
		for _, stmt := range []string{
			fmt.Sprintf("COPY %[1]s TO '%[2]s/%[1]s.parquet' (FORMAT parquet, ROW_GROUP_SIZE 100)", tb, dir),
			"DROP TABLE " + tb,
			fmt.Sprintf("CREATE VIEW %[1]s AS SELECT * FROM read_parquet('%[2]s/%[1]s.parquet')", tb, dir),
		} {
			if _, err := conn.ExecContext(ctx, stmt); err != nil {
				t.Fatalf("%s: %v", stmt, err)
			}
		}
	}
	for name, q := range map[string]string{"Q19": chQ19, "Q7": chQ7} {
		got := impliedInFilters(ctx, conn, q)
		if got.added == 0 || got.note != "" {
			t.Fatalf("%s: not rewritten: %+v", name, got)
		}
		if want, have := orRows(t, conn, q), orRows(t, conn, got.text); want != have {
			t.Errorf("%s: rows changed:\n%s\nwant\n%s", name, have, want)
		}
	}
}
