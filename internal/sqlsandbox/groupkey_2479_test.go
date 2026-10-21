package sqlsandbox

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
)

// groupKeySession is an in-memory DuckDB under the copy's own session settings
// (fixedSettings: the collation and the NULL order) holding a schema s whose
// text differs only in case, accents and folding across rows of different
// keys, which is where a wrong grouping would show.
func groupKeySession(t *testing.T) *sql.Conn {
	t.Helper()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	for _, stmt := range append(slices.Clone(fixedSettings),
		"CREATE SCHEMA s",
		"CREATE TABLE s.item AS SELECT * FROM (VALUES (1, 'Café', 1.00), (2, 'cafe', 2.00), (3, 'CAFE', 3.00), "+
			"(4, NULL, 4.00), (5, 'b', 5.00), (6, 'B', 6.00), (7, 'ß', 7.00), (8, 'ss', 8.00)) t(i_id, i_name, i_price)",
		"CREATE TABLE s.ol AS SELECT * FROM (VALUES (1, 1, 10, 5.0), (2, 1, 11, 6.0), (3, 2, 10, 7.0), (4, 3, 12, 1.0), "+
			"(5, 4, 10, 2.0), (6, 5, 10, 3.0), (7, 6, 11, 4.0), (8, 7, 10, 9.0), (9, 8, 10, 9.0), (10, 99, 10, 1.0)) t(ol_id, ol_i_id, ol_w_id, amt)",
		"CREATE TABLE s.district AS SELECT * FROM (VALUES (1, 1, 'North'), (1, 2, 'north'), (2, 1, 'NÖRTH'), (2, 2, 'South')) t(w, d, d_name)",
		"CREATE TABLE s.code AS SELECT * FROM (VALUES ('a', 'x'), ('A', 'y')) t(c, c_name)",
		"CREATE TABLE main.mitem AS SELECT * FROM s.item",
		"CREATE TABLE s.big AS SELECT * FROM (VALUES (1234567890123456789, 'wanted', 0.123456789012345678::DECIMAL(19,18)), "+
			"(1234567890123456800, 'other', 0.123456789012345680::DECIMAL(19,18))) t(id, name, frac)",
		"CREATE TABLE s.dk AS SELECT * FROM (VALUES (NULL::DATE, 'zero-date row'), (DATE '2020-01-01', 'jan')) t(d, nm)",
		"CREATE TABLE s.dup AS SELECT * FROM (VALUES (1, 'Café'), (1, 'cafe'), (2, 'x'), (2, NULL), (3, 'y'), (3, 'y')) t(id, name)",
		"CREATE TABLE s.nokey AS SELECT * FROM (VALUES (1, 'Café'), (2, 'cafe')) t(x, i_name)",
	) {
		if _, err := conn.ExecContext(context.Background(), stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	return conn
}

var groupKeyKeys = []TableKey{
	{Schema: "s", Table: "item", Columns: []string{"i_id"}},
	{Schema: "s", Table: "ol", Columns: []string{"ol_id"}},
	{Schema: "S", Table: "DISTRICT", Columns: []string{"W", "d"}},
	{Schema: "s", Table: "code", Columns: []string{"c"}},
	{Schema: "main", Table: "mitem", Columns: []string{"i_id"}},
	{Schema: "s", Table: "big", Columns: []string{"id"}},
	{Schema: "s", Table: "dup", Columns: []string{"id"}},
	{Schema: "s", Table: "dk", Columns: []string{"d"}},
}

// answer is a statement's columns and rows, as text.
func answer(t *testing.T, conn *sql.Conn, text string) (cols []string, rows []string) {
	t.Helper()
	r, err := conn.QueryContext(context.Background(), text)
	if err != nil {
		t.Fatalf("%s: %v", text, err)
	}
	defer r.Close()
	cols, _ = r.Columns()
	for r.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := r.Scan(ptrs...); err != nil {
			t.Fatal(err)
		}
		rows = append(rows, fmt.Sprint(vals...))
	}
	if err := r.Err(); err != nil {
		t.Fatal(err)
	}
	return cols, rows
}

func rewriteOf(t *testing.T, conn *sql.Conn, text, search string) string {
	t.Helper()
	stmt, reason := parseStatement(context.Background(), conn, text)
	if reason != "" {
		t.Fatalf("%s: %s", text, reason)
	}
	out, err := groupByKeyRewrite(context.Background(), conn, text, stmt, groupKeyKeys, search)
	if err != nil {
		t.Fatalf("%s: %v", text, err)
	}
	return out
}

// Every statement here is rewritten, and must answer exactly what it answers
// as written: the same column names, the same rows, in the same order (each
// ORDER BY settles every tie).
func TestGroupByKeyRewrite_sameAnswer(t *testing.T) {
	conn := groupKeySession(t)
	for _, c := range []struct{ name, sql, search string }{
		{"the issue's shape", "SELECT i.i_id, i.i_name, i.i_price, SUM(ol.amt) AS revenue, COUNT(*) AS lines, COUNT(DISTINCT ol.ol_w_id) AS wh " +
			"FROM s.ol ol JOIN s.item i ON i.i_id = ol.ol_i_id GROUP BY i.i_id, i.i_name, i.i_price ORDER BY revenue DESC, i.i_id LIMIT 20", ""},
		{"unqualified, one table", "SELECT i_id, i_name, count(*) FROM s.item GROUP BY i_id, i_name ORDER BY i_name, i_id", ""},
		{"ordered by the text, NULL first", "SELECT i.i_id, i.i_name FROM s.item i GROUP BY i.i_id, i.i_name ORDER BY i.i_name DESC, i.i_id", ""},
		{"HAVING on the text", "SELECT i.i_id, i.i_name, count(*) AS n FROM s.ol ol JOIN s.item i ON i.i_id = ol.ol_i_id " +
			"GROUP BY i.i_id, i.i_name HAVING i.i_name = 'cafe' ORDER BY i.i_id", ""},
		{"aliased, ordered by the alias", "SELECT i.i_name AS n, i.i_id FROM s.item i GROUP BY i.i_id, i.i_name ORDER BY n, i.i_id", ""},
		{"composite key", "SELECT di.w, di.d, di.d_name, count(*) FROM s.district di GROUP BY di.w, di.d, di.d_name ORDER BY di.d_name, di.w, di.d", ""},
		{"outer join, key side missing", "SELECT ol.ol_i_id, i.i_id, i.i_name, count(*) AS n FROM s.ol ol LEFT JOIN s.item i ON i.i_id = ol.ol_i_id " +
			"GROUP BY ol.ol_i_id, i.i_id, i.i_name ORDER BY ol.ol_i_id", ""},
		{"through USE", "SELECT i_id, i_name FROM item GROUP BY i_id, i_name ORDER BY i_id", "s"},
		{"under USE, a table only main holds", "SELECT i_id, i_name FROM mitem GROUP BY i_id, i_name ORDER BY i_id", "s"},
		{"inside a WITH", "WITH x AS (SELECT i.i_id, i.i_name, count(*) AS c FROM s.item i GROUP BY i.i_id, i.i_name) " +
			"SELECT * FROM x ORDER BY i_id", ""},
		{"spelled in upper case", "SELECT i.I_ID, i.I_NAME FROM s.item i GROUP BY i.I_ID, i.I_NAME ORDER BY i.I_NAME, i.I_ID", ""},
		{"inside an aliased expression", "SELECT i.i_id, upper(i.i_name) AS u FROM s.item i GROUP BY i.i_id, i.i_name ORDER BY u, i.i_id", ""},
		{"aggregates over the text", "SELECT i.i_id, i.i_name, count(DISTINCT i.i_name) AS dn, string_agg(i.i_name, ',') AS sa " +
			"FROM s.ol ol JOIN s.item i ON i.i_id = ol.ol_i_id GROUP BY i.i_id, i.i_name ORDER BY i.i_id", ""},
		{"ordered by position", "SELECT i.i_id, i.i_name FROM s.item i GROUP BY i.i_id, i.i_name ORDER BY 2, 1", ""},
		{"text of a table without a key beside it", "SELECT i.i_id, i.i_name, n.i_name AS other, count(*) AS c FROM s.item i JOIN s.nokey n ON n.x = i.i_id " +
			"GROUP BY i.i_id, i.i_name, n.i_name ORDER BY i.i_id", ""},
		{"literals past a float's digits", "SELECT b.id, b.name, count(*) AS n FROM s.big b WHERE b.id = 1234567890123456789 " +
			"OR b.frac = 0.123456789012345678 GROUP BY b.id, b.name ORDER BY b.id LIMIT 9223372036854775807", ""},
		{"same table twice", "SELECT a.i_id, a.i_name, b.i_id AS b_id, b.i_name AS b_name FROM s.item a JOIN s.item b ON lower(a.i_name) = lower(b.i_name) " +
			"GROUP BY a.i_id, a.i_name, b.i_id, b.i_name ORDER BY a.i_id, b_id", ""},
	} {
		t.Run(c.name, func(t *testing.T) {
			if c.search != "" {
				if _, err := conn.ExecContext(context.Background(), "SET search_path = '"+c.search+"'"); err != nil {
					t.Fatal(err)
				}
				defer conn.ExecContext(context.Background(), "RESET search_path")
			}
			rewritten := rewriteOf(t, conn, c.sql, c.search)
			if !strings.Contains(rewritten, "any_value(") {
				t.Fatalf("not rewritten: %s", c.sql)
			}
			wantCols, wantRows := answer(t, conn, c.sql)
			gotCols, gotRows := answer(t, conn, rewritten)
			if !slices.Equal(gotCols, wantCols) || !slices.Equal(gotRows, wantRows) {
				t.Errorf("answers differ\n as written: %s\n %v %q\n rewritten: %s\n %v %q", c.sql, wantCols, wantRows, rewritten, gotCols, gotRows)
			}
		})
	}
}

// None of these may be rewritten: each is a shape where reading the text
// columns off the key is not certainly the same answer, or not certainly the
// same statement.
func TestGroupByKeyRewrite_leftAlone(t *testing.T) {
	conn := groupKeySession(t)
	for _, c := range []struct{ name, sql, search string }{
		{"no GROUP BY", "SELECT i_id, i_name FROM s.item ORDER BY i_id", ""},
		{"part of a composite key", "SELECT di.w, di.d_name FROM s.district di GROUP BY di.w, di.d_name", ""},
		{"text key", "SELECT cd.c, cd.c_name FROM s.code cd GROUP BY cd.c, cd.c_name", ""},
		{"table without a key", "SELECT x, i_name FROM s.nokey GROUP BY x, i_name", ""},
		{"a WITH named like the table", "WITH item AS (SELECT 1 AS i_id, 'x' AS i_name) SELECT i_id, i_name FROM item GROUP BY i_id, i_name", "s"},
		{"unqualified with no USE", "SELECT i_id, i_name FROM item GROUP BY i_id, i_name", ""},
		{"ROLLUP", "SELECT i_id, i_name FROM s.item GROUP BY ROLLUP (i_id, i_name)", ""},
		{"GROUPING SETS", "SELECT i_id, i_name FROM s.item GROUP BY GROUPING SETS ((i_id, i_name), (i_id))", ""},
		{"GROUP BY ALL", "SELECT i_id, i_name FROM s.item GROUP BY ALL", ""},
		{"GROUP BY position", "SELECT i_id, i_name FROM s.item GROUP BY 1, 2", ""},
		{"GROUP BY an expression", "SELECT i_id, i_name FROM s.item GROUP BY i_id, i_name, i_price + 1", ""},
		{"GROUP BY a SELECT alias", "SELECT i_id, i_name AS nm FROM s.item GROUP BY i_id, nm", ""},
		{"GROUP BY a name a SELECT alias shadows", "SELECT i_id, upper(i_price::VARCHAR) AS i_name FROM s.item GROUP BY i_id, i_name, i_price", ""},
		{"window", "SELECT i_id, i_name, row_number() OVER (ORDER BY i_id) AS r FROM s.item GROUP BY i_id, i_name", ""},
		{"QUALIFY", "SELECT i_id, i_name FROM s.item GROUP BY i_id, i_name QUALIFY row_number() OVER (ORDER BY i_id) < 3", ""},
		{"subquery in the SELECT", "SELECT i_id, i_name, (SELECT 1) AS one FROM s.item GROUP BY i_id, i_name", ""},
		{"unaliased expression over the text", "SELECT i_id, upper(i_name) FROM s.item GROUP BY i_id, i_name", ""},
		{"FROM a subquery", "SELECT i_id, i_name FROM (SELECT * FROM s.item) GROUP BY i_id, i_name", ""},
		{"USING", "SELECT a.i_id, a.i_name FROM s.item a JOIN s.item b USING (i_id) GROUP BY a.i_id, a.i_name", ""},
		{"NATURAL", "SELECT a.i_id, a.i_name FROM s.item a NATURAL JOIN s.item b GROUP BY a.i_id, a.i_name", ""},
		{"SEMI", "SELECT i.i_id, i.i_name FROM s.item i SEMI JOIN s.ol ol ON ol.ol_i_id = i.i_id GROUP BY i.i_id, i.i_name", ""},
		{"star", "SELECT * FROM s.item GROUP BY i_id, i_name, i_price", ""},
		{"a name two tables hold", "SELECT i.i_id, i_name FROM s.item i JOIN s.nokey n ON n.x = i.i_id GROUP BY i.i_id, i_name", ""},
		{"ORDER BY an alias spelled like the text", "SELECT i.i_id, count(*) AS i_name FROM s.item i GROUP BY i.i_id, i.i_name ORDER BY i_name", ""},
		{"HAVING over an alias spelled like the text", "SELECT i.i_id, count(*) AS i_name FROM s.item i GROUP BY i.i_id, i.i_name HAVING i_name > 0", ""},
		{"DISTINCT ON", "SELECT DISTINCT ON (i_name) i_id, i_name FROM s.item GROUP BY i_id, i_name", ""},
		{"alias that is a column name", "SELECT i_name.i_id, i_name.i_name FROM s.item AS i_name GROUP BY i_name.i_id, i_name.i_name", ""},
		{"renamed columns", "SELECT a, b FROM s.item AS t(a, b) GROUP BY a, b", ""},
		{"columns renamed into each other", "SELECT i_id, i_name FROM s.item AS t(i_name, i_id) GROUP BY i_id, i_name", ""},
		{"table function", "SELECT i, n FROM (VALUES (1, 'a')) v(i, n) GROUP BY i, n", ""},
		{"ORDER BY a name two tables hold, which the added alias would settle", "SELECT i.i_name, count(*) AS c FROM s.item i JOIN s.nokey n ON n.x = i.i_id " +
			"GROUP BY i.i_id, i.i_name, n.i_name ORDER BY i_name", ""},
		{"only non-text columns beside the key", "SELECT i_id, i_price FROM s.item GROUP BY i_id, i_price", ""},
	} {
		t.Run(c.name, func(t *testing.T) {
			if c.search != "" {
				if _, err := conn.ExecContext(context.Background(), "SET search_path = '"+c.search+"'"); err != nil {
					t.Fatal(err)
				}
				defer conn.ExecContext(context.Background(), "RESET search_path")
			}
			if got := rewriteOf(t, conn, c.sql, c.search); got != "" {
				t.Errorf("rewritten:\n %s\ninto\n %s", c.sql, got)
			}
		})
	}
}

// A job without keys reads no catalog and changes nothing, and the rewrite
// works on its own copy of the parsed statement, which the worker reads again
// afterwards (selectLimitApplies).
func TestGroupByKeyRewrite_noKeysCostsNothing(t *testing.T) {
	conn := groupKeySession(t)
	stmt, _ := parseStatement(context.Background(), conn, "SELECT i_id, i_name FROM s.item GROUP BY i_id, i_name")
	const q = "SELECT i_id, i_name FROM s.item GROUP BY i_id, i_name"
	got, err := groupByKeyRewrite(context.Background(), conn, q, stmt, nil, "")
	if got != "" || err != nil {
		t.Fatalf("with no keys: %q, %v", got, err)
	}
	before, _ := json.Marshal(stmt)
	if _, err := groupByKeyRewrite(context.Background(), conn, q, stmt, groupKeyKeys, ""); err != nil {
		t.Fatal(err)
	}
	if after, _ := json.Marshal(stmt); string(after) != string(before) {
		t.Fatal("the rewrite changed the parsed statement it was given")
	}
}

// The wiring: a job with Keys runs the narrowed statement in the real worker,
// and answers what it answers without them.
func TestGroupByKeyRewrite_throughTheWorker(t *testing.T) {
	f := newCopyFixture(t)
	r := newTestRunner(t, testLimits())
	const q = "SELECT id, status, count(*) AS n FROM shop.orders GROUP BY id, status ORDER BY status, id"
	plain, err := r.Run(context.Background(), f.job(q))
	if err != nil {
		t.Fatal(err)
	}
	job := f.job(q)
	job.Keys = []TableKey{{Schema: "shop", Table: "orders", Columns: []string{"id"}}}
	keyed, err := r.Run(context.Background(), job)
	if err != nil {
		t.Fatal(err)
	}
	if plain.GroupedByKey || !keyed.GroupedByKey {
		t.Fatalf("GroupedByKey: without keys %v, with them %v", plain.GroupedByKey, keyed.GroupedByKey)
	}
	if !reflect.DeepEqual(plain.Columns, keyed.Columns) || !reflect.DeepEqual(plain.Rows, keyed.Rows) {
		t.Fatalf("answers differ:\n%v %v\n%v %v", plain.Columns, plain.Rows, keyed.Columns, keyed.Rows)
	}
}

// A key value the view holds twice, with text that differs (byte for byte,
// or only by case and accents, or NULL beside a value): the narrowed
// statement refuses with groupKeyMixed instead of answering, and a key value
// held twice with the same text answers as written.
func TestGroupByKeyRewrite_keyNotUniqueRefuses(t *testing.T) {
	conn := groupKeySession(t)
	for _, c := range []struct {
		sql     string
		refuses bool
	}{
		{"SELECT id, name, count(*) AS n FROM s.dup WHERE id = 1 GROUP BY id, name", true},
		{"SELECT id, name, count(*) AS n FROM s.dup WHERE id = 2 GROUP BY id, name", true},
		{"SELECT id, name, count(*) AS n FROM s.dup WHERE id = 3 GROUP BY id, name ORDER BY id", false},
		// A key stored as NULL (a zero date) on the optional side of an outer
		// join shares its group with the rows that found no match.
		{"SELECT k.d, k.nm, count(*) AS n FROM s.item i LEFT JOIN s.dk k ON (k.d IS NULL AND i.i_id = 1) OR k.d = DATE '2020-01-01' + i.i_id - 2 " +
			"GROUP BY k.d, k.nm ORDER BY n", true},
	} {
		rewritten := rewriteOf(t, conn, c.sql, "")
		if rewritten == "" {
			t.Fatalf("not rewritten: %s", c.sql)
		}
		_, err := conn.ExecContext(context.Background(), rewritten)
		if got := err != nil && strings.Contains(err.Error(), groupKeyMixed); got != c.refuses {
			t.Errorf("%s: refused %v (%v), want %v", c.sql, got, err, c.refuses)
		}
		if !c.refuses {
			wantCols, wantRows := answer(t, conn, c.sql)
			gotCols, gotRows := answer(t, conn, rewritten)
			if !slices.Equal(gotCols, wantCols) || !slices.Equal(gotRows, wantRows) {
				t.Errorf("answers differ: %v %q, rewritten %v %q", wantCols, wantRows, gotCols, gotRows)
			}
		}
	}
}

// Through the worker: a key value the copy holds twice runs the statement as
// written, with its answer, and says the rewrite did not apply.
func TestGroupByKeyRewrite_throughTheWorker_keyNotUnique(t *testing.T) {
	f := newCopyFixture(t)
	r := newTestRunner(t, testLimits())
	job := f.job("SELECT id, name, count(*) AS n FROM dup.t GROUP BY id, name ORDER BY name")
	job.ViewsSQL += "\nCREATE SCHEMA dup;\nCREATE VIEW dup.t AS SELECT * FROM (VALUES (1, 'a'), (1, 'b')) v(id, name);\n"
	plain, err := r.Run(context.Background(), job)
	if err != nil {
		t.Fatal(err)
	}
	job.Keys = []TableKey{{Schema: "dup", Table: "t", Columns: []string{"id"}}}
	keyed, err := r.Run(context.Background(), job)
	if err != nil {
		t.Fatalf("a key held twice failed the statement instead of running it as written: %v", err)
	}
	if keyed.GroupedByKey || len(keyed.Rows) != 2 {
		t.Fatalf("GroupedByKey %v, rows %v", keyed.GroupedByKey, keyed.Rows)
	}
	if !reflect.DeepEqual(plain.Rows, keyed.Rows) {
		t.Fatalf("answers differ: %v, %v", plain.Rows, keyed.Rows)
	}
}

// A GROUP BY rewriteNode cannot narrow costs no second parse and no catalog
// read: one entry, or an entry that is not a bare column, never qualifies.
func TestGroupByKeyRewrite_hasGroupByScreens(t *testing.T) {
	conn := groupKeySession(t)
	for _, c := range []struct {
		sql  string
		want bool
	}{
		{"SELECT w, SUM(d) FROM s.district GROUP BY w", false},
		{"SELECT d_name, COUNT(*) FROM s.district GROUP BY d_name", false},
		{"SELECT w + 1, d_name, COUNT(*) FROM s.district GROUP BY w + 1, d_name", false},
		{"SELECT lower(d_name), w, d, COUNT(*) FROM s.district GROUP BY lower(d_name), w, d", false},
		{"SELECT COUNT(*) FROM s.district", false},
		{"SELECT w, d, d_name, COUNT(*) FROM s.district GROUP BY w, d, d_name", true},
		{"SELECT i_id, i_name, COUNT(*) FROM s.item GROUP BY 1, 2", false}, // positions: rewriteNode refuses them too
		{"SELECT x.n, COUNT(*) FROM (SELECT i_id, i_name AS n FROM s.item GROUP BY i_id, i_name) x GROUP BY x.n", true},
	} {
		stmt, reason := parseStatement(context.Background(), conn, c.sql)
		if reason != "" {
			t.Fatalf("%s: %s", c.sql, reason)
		}
		if got := hasGroupBy(stmt); got != c.want {
			t.Errorf("hasGroupBy(%s) = %v, want %v", c.sql, got, c.want)
		}
	}
}
