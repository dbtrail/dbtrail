package sqlsandbox

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"os"
	"reflect"
	"slices"
	"strings"
	"testing"
)

// The order alone, on tables by index: rows[i] is table i's estimate,
// conds[j] the tables condition j names.
func TestConnectedOrder(t *testing.T) {
	for _, tc := range []struct {
		name  string
		rows  []float64
		conds [][]int
		want  []int
		why   string
	}{
		{"connected as written: kept", []float64{1, 100}, [][]int{{0, 1}}, nil, "connected already"},
		// Q21: supplier, order_line l1, orders, stock, nation.
		{"Q21", []float64{1e4, 6e7, 6e6, 2e7, 62}, [][]int{{1, 2}, {1, 2}, {1, 3}, {1, 3}, {0, 3}, {1, 2}, {0, 4}, {4}}, []int{1, 2, 3, 0, 4}, ""},
		{"a table no condition reaches", []float64{10, 100, 1000}, [][]int{{0, 1}}, nil, "no condition"},
		{"no condition at all", []float64{10, 100}, nil, nil, "no condition"},
		{"one table", []float64{10}, nil, nil, "one table"},
		// A condition of three tables connects the third only once the
		// other two are placed.
		{"a condition of three tables", []float64{10, 1000, 100}, [][]int{{0, 1, 2}, {1, 2}}, []int{1, 2, 0}, ""},
		{"a condition of three tables, never two placed", []float64{10, 1000, 100}, [][]int{{0, 1, 2}}, nil, "no condition"},
		// A tie goes to the table written first, for the first and the next.
		{"ties", []float64{5, 100, 100, 5}, [][]int{{1, 0}, {1, 3}, {2, 3}}, []int{1, 0, 3, 2}, ""},
	} {
		got, why := connectedOrder(tc.rows, tc.conds)
		if !slices.Equal(got, tc.want) || (tc.why == "") != (why == "") || !strings.Contains(why, tc.why) {
			t.Errorf("%s: %v %q, want %v %q", tc.name, got, why, tc.want, tc.why)
		}
	}
}

// chTables are the CH-benCHmark tables Q21 reads, empty, and the rows each
// is taken to have.
var chTables = []struct {
	ddl  string
	ref  string
	rows float64
}{
	{"CREATE TABLE order_line (ol_o_id INT, ol_w_id INT, ol_d_id INT, ol_i_id INT, ol_delivery_d TIMESTAMP)", `"order_line"`, 6e7},
	{"CREATE TABLE orders (o_id INT, o_w_id INT, o_d_id INT, o_entry_d TIMESTAMP)", `"orders"`, 6e6},
	{"CREATE TABLE stock (s_w_id INT, s_i_id INT, s_quantity INT)", `"stock"`, 2e7},
	{`CREATE TABLE supplier (su_suppkey INT, su_name VARCHAR, su_nationkey INT, "Su Note" VARCHAR)`, `"supplier"`, 1e4},
	{"CREATE TABLE nation (n_nationkey INT, n_name VARCHAR)", `"nation"`, 62},
}

const chQ21 = "SELECT su_name, count(*) AS numwait FROM supplier, order_line l1, orders, stock, nation WHERE ol_o_id = o_id AND ol_w_id = o_w_id AND ol_d_id = o_d_id AND ol_w_id = s_w_id AND ol_i_id = s_i_id AND mod((s_w_id * s_i_id),10000) = su_suppkey AND l1.ol_delivery_d > o_entry_d AND NOT EXISTS (SELECT * FROM order_line l2 WHERE l2.ol_o_id = l1.ol_o_id AND l2.ol_w_id = l1.ol_w_id AND l2.ol_d_id = l1.ol_d_id AND l2.ol_delivery_d > l1.ol_delivery_d) AND su_nationkey = n_nationkey AND n_name = 'Germany' GROUP BY su_name ORDER BY numwait DESC, su_name LIMIT 100"

// chConn is a DuckDB session with chTables, and facts that read their
// columns from it and their rows from chTables.
func chConn(t *testing.T) (*sql.Conn, func(string) (tableFacts, error)) {
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
	rows := map[string]float64{}
	for _, tb := range chTables {
		if _, err := conn.ExecContext(ctx, tb.ddl); err != nil {
			t.Fatal(err)
		}
		rows[tb.ref] = tb.rows
	}
	read := sessionTableFacts(ctx, conn)
	return conn, func(ref string) (tableFacts, error) {
		f, err := read(ref)
		if err != nil {
			return f, err
		}
		r, ok := rows[ref]
		if !ok {
			return f, errors.New("no rows for " + ref)
		}
		f.rows = r
		return f, nil
	}
}

func TestConnectedOrderSQL(t *testing.T) {
	conn, facts := chConn(t)
	ctx := context.Background()
	for _, tc := range []struct {
		name  string
		sql   string
		order []string
		why   string
		has   []string
	}{
		{name: "Q21", sql: chQ21, order: []string{"l1", "orders", "stock", "supplier", "nation"},
			has: []string{"NOT EXISTS", "l2.ol_delivery_d > l1.ol_delivery_d", "'Germany'", "LIMIT 100"}},
		{name: "already connected", sql: strings.Replace(chQ21, "FROM supplier, order_line l1, orders, stock, nation", "FROM order_line l1, orders, stock, supplier, nation", 1),
			why: "connected already"},
		{name: "disconnected", sql: "SELECT count(*) AS n FROM supplier, nation, orders WHERE su_nationkey = n_nationkey",
			why: "no condition"},
		{name: "an unqualified column two tables have", sql: "SELECT count(*) AS n FROM orders o1, stock, orders o2 WHERE o1.o_id = s_i_id AND o_w_id = s_w_id",
			why: "more than one table"},
		{name: "an unqualified column no table has", sql: "SELECT count(*) AS n FROM supplier, nation, stock WHERE nope = s_w_id",
			why: "none of the tables"},
		{name: "a qualifier no table has", sql: "SELECT count(*) AS n FROM supplier, nation, stock WHERE x.su_name = s_w_id",
			why: "not a column"},
		{name: "explicit JOIN", sql: "SELECT count(*) AS n FROM supplier JOIN nation ON su_nationkey = n_nationkey, stock WHERE mod(s_w_id, 10) = su_suppkey",
			why: "JOIN"},
		{name: "a subquery in FROM", sql: "SELECT count(*) AS n FROM (SELECT * FROM supplier) s, nation, stock WHERE su_nationkey = n_nationkey AND mod(s_w_id, 10) = su_suppkey",
			why: "SUBQUERY"},
		{name: "a table function in FROM", sql: "SELECT count(*) AS n FROM supplier, range(3) r, stock WHERE r.range = su_suppkey AND mod(s_w_id, 10) = su_suppkey",
			why: "TABLE_FUNCTION"},
		// Two aliases of one table: each qualified column is its alias's.
		{name: "self-join", sql: "SELECT count(*) AS n FROM orders a, supplier, orders b WHERE a.o_id = b.o_id AND b.o_w_id = su_suppkey",
			order: []string{"a", "b", "supplier"}},
		// An expression and an inequality connect, and the round trip keeps
		// a quoted name, a cast, a BIGINT and a quote in a string.
		{name: "an expression join", sql: `SELECT su_name, count(*) AS n FROM supplier, nation, stock WHERE mod(s_w_id * s_i_id, 10000) = su_suppkey AND n_nationkey < s_w_id AND s_quantity < 12345678901234567 AND "Su Note" = CAST(1.50 AS DECIMAL(10,2))::VARCHAR AND su_name <> 'it''s' GROUP BY su_name`,
			order: []string{"stock", "nation", "supplier"}, has: []string{`"Su Note"`, "CAST(1.50 AS DECIMAL(10,2))", "12345678901234567", `'it''s'`}},
		{name: "an OR at the top of WHERE", sql: "SELECT count(*) AS n FROM supplier, nation, stock WHERE su_nationkey = n_nationkey OR mod(s_w_id, 10) = su_suppkey",
			why: "an OR"},
		{name: "a * in the select list", sql: "SELECT * FROM supplier, nation, stock WHERE mod(s_w_id, 10) = su_suppkey AND su_nationkey = n_nationkey",
			why: "a *"},
		{name: "a table's * in the select list", sql: "SELECT stock.* FROM supplier, nation, stock WHERE mod(s_w_id, 10) = su_suppkey AND su_nationkey = n_nationkey",
			why: "a *"},
		{name: "a WITH", sql: "WITH s AS (SELECT * FROM supplier) SELECT count(*) AS n FROM s, nation, stock WHERE mod(s_w_id, 10) = su_suppkey AND su_nationkey = n_nationkey",
			why: "WITH"},
		{name: "no WHERE", sql: "SELECT count(*) AS n FROM supplier, nation", why: "no WHERE"},
		{name: "a UNION", sql: "SELECT su_name FROM supplier, nation, stock WHERE mod(s_w_id, 10) = su_suppkey AND su_nationkey = n_nationkey UNION ALL SELECT n_name FROM nation",
			why: "plain SELECT"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rw, why := connectedOrderSQL(ctx, conn, tc.sql, facts)
			if tc.why != "" {
				if !strings.Contains(why, tc.why) || rw.text != "" {
					t.Fatalf("why %q, text %q; want why with %q", why, rw.text, tc.why)
				}
				return
			}
			if why != "" {
				t.Fatalf("no rewrite: %s", why)
			}
			if !slices.Equal(rw.order, tc.order) {
				t.Errorf("order %v, want %v", rw.order, tc.order)
			}
			for _, s := range tc.has {
				if !strings.Contains(rw.text, s) {
					t.Errorf("the new text lost %q: %s", s, rw.text)
				}
			}
			// The new text's FROM is the order asked, and the rest is the
			// statement's own.
			doc, err := serializeSQL(ctx, conn, rw.text)
			if err != nil {
				t.Fatal(err)
			}
			node, _ := connectedShape(doc)
			tables, why := fromTables(node["from_table"])
			if why != "" {
				t.Fatal(why)
			}
			var names []string
			for _, tb := range tables {
				names = append(names, tb.name)
			}
			if !slices.Equal(names, tc.order) {
				t.Errorf("the new text's FROM: %v", names)
			}
			own, _ := serializeSQL(ctx, conn, tc.sql)
			ownNode, _ := connectedShape(own)
			delete(node, "from_table")
			delete(ownNode, "from_table")
			if !reflect.DeepEqual(withoutLocations(node), withoutLocations(ownNode)) {
				t.Errorf("the new text changed more than the FROM: %s", rw.text)
			}
		})
	}
}

// joinViews are views over range() in the shape of Q21's failure: tl (1e6
// rows) joins ta and tb (2e5 each) by its own key of each and by a column of
// 10 values all three have. DuckDB's own order joins ta with tb first on
// that column alone, which it derives from the others (4e9 rows by the
// source's counts), and the order written does the same; a connected order
// starts at tl.
const joinViews = `CREATE VIEW ta AS SELECT range AS aid, range % 10 AS aw FROM range(200000);
CREATE VIEW tb AS SELECT range AS bid, range % 10 AS bw FROM range(200000);
CREATE VIEW tl AS SELECT range AS lid, ((range * 7) % 200000) % 10 AS lw, (range * 7) % 200000 AS la, (range * 13) % 200000 AS lb FROM range(1000000);
`

const joinViewsQuery = "SELECT aw, count(*) AS n FROM ta, tb, tl WHERE lw = aw AND la = aid AND lw = bw AND lb = bid GROUP BY aw ORDER BY aw"

var joinViewsNDV = map[string]float64{"aw": 10, "bw": 10, "lw": 10, "aid": 200000, "bid": 200000, "la": 200000, "lb": 200000}

// The reference: the connected order written by hand, run in the order
// written with no check.
func joinViewsReference(t *testing.T, dir string) [][]any {
	t.Helper()
	res := runJob(wireJob{CopyDirs: []string{dir}, ViewsSQL: joinViews, KeepJoinOrder: true,
		SQL:     "SELECT aw, count(*) AS n FROM tl, ta, tb WHERE lw = aw AND la = aid AND lw = bw AND lb = bid GROUP BY aw ORDER BY aw",
		Threads: 1, MemoryLimit: "256MB", MaxRows: 100, MaxResultBytes: 1 << 20}, nil, os.Stderr)
	if res.Error != nil {
		t.Fatalf("the reference: %+v", res.Error)
	}
	if len(res.Rows) == 0 {
		t.Fatal("the reference has no rows")
	}
	return res.Rows
}

// In the worker, through DuckDB's own plans and the real check: refused in
// both orders without the option, answered in the connected order with it.
func TestWorker_connectedOrder(t *testing.T) {
	dir := t.TempDir()
	want := joinViewsReference(t, dir)
	job := wireJob{CopyDirs: []string{dir}, ViewsSQL: joinViews, SQL: joinViewsQuery, SkipCommonSubplan: true,
		Threads: 1, MemoryLimit: "256MB", MaxRows: 100, MaxResultBytes: 1 << 20, JoinNDV: joinViewsNDV}
	res := runJob(job, nil, os.Stderr)
	if res.Error == nil || res.Error.Kind != errJoinTooBig || !strings.Contains(res.Error.Message, "in the order written (") {
		t.Fatalf("without the option: %+v", res.Error)
	}
	job.TryConnectedJoinOrder = true
	var stderr bytes.Buffer
	res = runJob(job, nil, &stderr)
	if res.Error != nil {
		t.Fatalf("with the option: %+v", res.Error)
	}
	if res.JoinOrder != joinOrderConnected || res.JoinOrderTables != "tl, ta, tb" {
		t.Errorf("order %q %q", res.JoinOrder, res.JoinOrderTables)
	}
	if !reflect.DeepEqual(res.Rows, want) {
		t.Errorf("rows %v, want %v", res.Rows, want)
	}
	// Said on stderr as the order written is, so a worker the kernel stops
	// is not run again in another order (oomretry.go).
	if !strings.Contains(stderr.String(), joinOrderWrittenLine+", its tables in a connected order: tl, ta, tb") {
		t.Errorf("stderr: %s", stderr.String())
	}
	// The statement's own column names.
	if len(res.Columns) != 2 || res.Columns[0].Name != "aw" || res.Columns[1].Name != "n" {
		t.Errorf("columns %+v", res.Columns)
	}
	// A statement of another shape is refused as before, word for word.
	job.SQL = strings.Replace(joinViewsQuery, "SELECT aw, count(*) AS n", "SELECT *", 1)
	job.SQL = strings.Replace(job.SQL, " GROUP BY aw ORDER BY aw", "", 1)
	res = runJob(job, nil, os.Stderr)
	if res.Error == nil || res.Error.Kind != errJoinTooBig {
		t.Fatalf("a * with the option: %+v", res.Error)
	}
	job.TryConnectedJoinOrder = false
	if again := runJob(job, nil, os.Stderr); again.Error == nil || again.Error.Message != res.Error.Message {
		t.Errorf("the refusal changed with the option: %q, without %+v", res.Error.Message, again.Error)
	}
}

// Through the real worker process: the option travels to it, is off unless
// asked, and the answer says the order.
func TestRunner_connectedOrder(t *testing.T) {
	if (Session{}).TryConnectedJoinOrder {
		t.Fatal("on by default")
	}
	dir := t.TempDir()
	want := joinViewsReference(t, dir)
	r := newTestRunner(t, testLimits())
	job := Job{CopyDirs: []string{dir}, ViewsSQL: joinViews, SQL: joinViewsQuery,
		Session: Session{SkipCommonSubplan: true, JoinNDV: &JoinCounts{NDV: joinViewsNDV}}}
	_, err := r.Run(context.Background(), job)
	var tooBig *JoinTooBigError
	if !errors.As(err, &tooBig) {
		t.Fatalf("without the option: %T %v", err, err)
	}
	job.Session.TryConnectedJoinOrder = true
	res, err := r.Run(context.Background(), job)
	if err != nil {
		t.Fatalf("with the option: %v", err)
	}
	if res.JoinOrderConnected != "tl, ta, tb" || res.JoinOrderWritten {
		t.Errorf("connected %q, written %v", res.JoinOrderConnected, res.JoinOrderWritten)
	}
	if !reflect.DeepEqual(normalizeRows(res.Rows), normalizeRows(want)) {
		t.Errorf("rows %v, want %v", res.Rows, want)
	}
}

// normalizeRows is rows with every number as a float64: the worker's answer
// is JSON through the parent, the reference the worker's own values.
func normalizeRows(rows [][]any) [][]any {
	out := make([][]any, len(rows))
	for i, r := range rows {
		out[i] = make([]any, len(r))
		for j, v := range r {
			switch n := v.(type) {
			case int64:
				out[i][j] = float64(n)
			case int32:
				out[i][j] = float64(n)
			case float64:
				out[i][j] = n
			default:
				out[i][j] = v
			}
		}
	}
	return out
}

// A connected order that runs out of memory ran in the order written: the
// parent does not run it again (oomretry.go), whichever way it learns it.
func TestRanWrittenOrder_connected(t *testing.T) {
	for order, want := range map[string]bool{joinOrderWritten: true, joinOrderConnected: true, "": false} {
		if got := ranWrittenOrder(order); got != want {
			t.Errorf("%q: %v", order, got)
		}
	}
	qe := &QueryError{Message: outOfMemoryPrefix + ": x", ranWrittenOrder: ranWrittenOrder(joinOrderConnected)}
	if want, skip := writtenOrderRetryWanted(Session{RetryOutOfMemoryInWrittenOrder: true}, qe); want || skip != WrittenOrderRetrySkippedCheckWritten {
		t.Errorf("retry wanted %v, skip %q", want, skip)
	}
}
