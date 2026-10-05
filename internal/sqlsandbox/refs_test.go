package sqlsandbox

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"
	"sort"
	"strings"
	"testing"
)

// parseForRefs runs the same parse the worker's gate runs and collects the
// relations from it.
func parseForRefs(t *testing.T, text string) Refs {
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
	stmt, reason := parseStatement(context.Background(), conn, text)
	if reason != "" {
		t.Fatalf("%q refused: %s", text, reason)
	}
	refs := collectRefs(stmt)
	sort.Slice(refs.Tables, func(i, j int) bool {
		a, b := refs.Tables[i], refs.Tables[j]
		return a.Catalog+"\x00"+a.Schema+"\x00"+a.Name < b.Catalog+"\x00"+b.Schema+"\x00"+b.Name
	})
	return refs
}

// #2029: what a statement names, as the parser wrote it. Every shape that
// can depend on a relation it does not name (a catalog listing, a table
// function) is Unsure, so the caller installs every view; the names keep
// the spelling the user typed, case included, because folding is the
// caller's (it knows how its views are named).
func TestCollectRefs(t *testing.T) {
	tr := func(cat, schema, name string) TableRef { return TableRef{Catalog: cat, Schema: schema, Name: name} }
	cases := []struct {
		sql  string
		want Refs
	}{
		{"SELECT 1", Refs{}},
		{"SELECT * FROM shop.orders", Refs{Tables: []TableRef{tr("", "shop", "orders")}}},
		{"FROM shop.orders", Refs{Tables: []TableRef{tr("", "shop", "orders")}}},
		{"SELECT * FROM orders", Refs{Tables: []TableRef{tr("", "", "orders")}}},
		{`SELECT * FROM "Shop"."Order.Items"`, Refs{Tables: []TableRef{tr("", "Shop", "Order.Items")}}},
		{"SELECT * FROM memory.main.events", Refs{Tables: []TableRef{tr("memory", "main", "events")}}},
		{"SELECT * FROM shop.orders o JOIN shop.items i ON o.id = i.order_id",
			Refs{Tables: []TableRef{tr("", "shop", "items"), tr("", "shop", "orders")}}},
		{"SELECT id FROM a.x UNION ALL SELECT id FROM b.y", Refs{Tables: []TableRef{tr("", "a", "x"), tr("", "b", "y")}}},
		{"SELECT (SELECT max(id) FROM a.x) FROM b.y WHERE EXISTS (SELECT 1 FROM c.z) AND id IN (SELECT id FROM d.w) LIMIT (SELECT 1 FROM e.v)",
			Refs{Tables: []TableRef{tr("", "a", "x"), tr("", "b", "y"), tr("", "c", "z"), tr("", "d", "w"), tr("", "e", "v")}}},
		{"SELECT * FROM (SELECT * FROM shop.orders) s", Refs{Tables: []TableRef{tr("", "shop", "orders")}}},
		{"WITH q AS (SELECT * FROM shop.orders) SELECT * FROM q", Refs{Tables: []TableRef{tr("", "shop", "orders")}}},
		{"WITH Q AS (SELECT 1) SELECT * FROM q, (SELECT * FROM q) s", Refs{}},
		// A WITH's own body does not see it: the name there is the relation.
		{"WITH events AS (SELECT * FROM events WHERE id > 0) SELECT count(*) FROM events",
			Refs{Tables: []TableRef{tr("", "", "events")}}},
		{"WITH duckdb_views AS (SELECT * FROM duckdb_views WHERE NOT internal) SELECT count(*) FROM duckdb_views",
			Refs{Tables: []TableRef{tr("", "", "duckdb_views")}}},
		// A WITH inside a subquery binds nothing outside it.
		{"SELECT (SELECT count(*) FROM (WITH duckdb_views AS (SELECT 1) SELECT * FROM duckdb_views)), (SELECT count(*) FROM duckdb_views)",
			Refs{Tables: []TableRef{tr("", "", "duckdb_views")}}},
		// A qualified name is never a WITH.
		{"WITH orders AS (SELECT 1) SELECT * FROM shop.orders", Refs{Tables: []TableRef{tr("", "shop", "orders")}}},
		// A sibling reference is reported (the safe side: it matches no view).
		{"WITH a AS (SELECT 1), b AS (SELECT * FROM a) SELECT * FROM b", Refs{Tables: []TableRef{tr("", "", "a")}}},
		{"DESCRIBE shop.orders", Refs{Tables: []TableRef{tr("", "shop", "orders")}}},
		{"SUMMARIZE shop.orders", Refs{Tables: []TableRef{tr("", "shop", "orders")}}},
		{"SELECT * FROM information_schema.tables", Refs{Tables: []TableRef{tr("", "information_schema", "tables")}}},
		// The catalog listings name nothing because they ask for the list.
		{"SHOW TABLES", Refs{Unsure: true}},
		{"SHOW ALL TABLES", Refs{Unsure: true}},
		// A table function can read the catalog (duckdb_tables) or anything
		// else; no attempt to tell which.
		{"SELECT * FROM duckdb_tables()", Refs{Unsure: true}},
		{"SELECT * FROM range(3)", Refs{Unsure: true}},
		{"SELECT * FROM shop.orders, range(3)", Refs{Tables: []TableRef{tr("", "shop", "orders")}, Unsure: true}},
	}
	for _, c := range cases {
		got := parseForRefs(t, c.sql)
		got.Star, got.NamedJoin, got.Natural, got.StarTables, got.StarNamedJoin = false, false, false, nil, false // TestCollectRefs_star has them
		if !reflect.DeepEqual(got, c.want) {
			t.Errorf("%s:\n got %+v\nwant %+v", c.sql, got, c.want)
		}
	}
}

// #2111: a star returns what a relation's column list says, in its order,
// and that is not always what MySQL returns. The caller needs to know WHICH
// tables a statement's stars expand, exactly: StarTables when the walk is
// certain, Star when it is not (then every table the statement reads counts).
func TestCollectRefs_star(t *testing.T) {
	type want struct {
		tables    string // the tables a star expands, "schema.name" sorted and joined by a space
		unsure    bool   // Star: a star the walk could not attribute
		namedJoin bool   // StarNamedJoin: an unqualified star over USING or NATURAL
	}
	for sqlText, w := range map[string]want{
		// An unqualified star expands the tables of its own FROM.
		"SELECT * FROM shop.orders":                                 {tables: "shop.orders"},
		"SELECT DISTINCT * FROM shop.orders":                        {tables: "shop.orders"},
		"SELECT * EXCLUDE (id) FROM shop.orders":                    {tables: "shop.orders"},
		"SELECT COLUMNS('a.*') FROM shop.orders":                    {tables: "shop.orders"},
		"FROM shop.orders":                                          {tables: "shop.orders"},
		"TABLE shop.orders":                                         {tables: "shop.orders"},
		"SELECT * FROM a.x JOIN b.y ON x.id = y.id":                 {tables: "a.x b.y"},
		"SELECT * FROM a.x, b.y":                                    {tables: "a.x b.y"},
		"SELECT *, 1 FROM a.x LEFT JOIN b.y ON true CROSS JOIN c.z": {tables: "a.x b.y c.z"},
		// A qualified star expands the one table it names, by alias or by name.
		"SELECT o.* FROM shop.orders o":                              {tables: "shop.orders"},
		"SELECT id, o.* FROM shop.orders o":                          {tables: "shop.orders"},
		"SELECT l.*, g.id FROM shop.lines l JOIN shop.gen g ON true": {tables: "shop.lines"},
		"SELECT orders.* FROM shop.orders JOIN shop.gen g ON true":   {tables: "shop.orders"},
		"SELECT ORDERS.* FROM shop.orders":                           {tables: "shop.orders"},
		"SELECT l.*, g.* FROM shop.lines l JOIN shop.gen g ON true":  {tables: "shop.gen shop.lines"},
		// ... and is not reordered by USING or NATURAL (MySQL 8.4, MariaDB 11.4 and DuckDB agree).
		"SELECT l.* FROM shop.lines l JOIN shop.lines m USING (id)": {tables: "shop.lines"},
		"SELECT * FROM shop.lines l JOIN shop.lines m USING (id)":   {tables: "shop.lines shop.lines", namedJoin: true},
		"SELECT * FROM a.x NATURAL JOIN b.y":                        {tables: "a.x b.y", namedJoin: true},
		// A derived table or a WITH is what its own select lists say: each
		// is read on its own, so a star OVER one adds nothing.
		"SELECT * FROM (SELECT id, a FROM shop.gen) x":           {},
		"SELECT x.* FROM (SELECT id, a FROM shop.gen) x":         {},
		"SELECT * FROM (SELECT * FROM shop.gen) x":               {tables: "shop.gen"},
		"SELECT id FROM (SELECT * FROM shop.orders)":             {tables: "shop.orders"},
		"SELECT count(*) FROM (SELECT * FROM shop.gen) x":        {tables: "shop.gen"}, // conservative: only the count is read
		"WITH q AS (SELECT * FROM shop.orders) SELECT id FROM q": {tables: "shop.orders"},
		"WITH q AS (SELECT id FROM shop.orders) SELECT * FROM q": {},
		"SELECT * FROM (SELECT id FROM a.x) q JOIN b.y ON true":  {tables: "b.y"},
		// Every select of a statement is read: a subquery's star counts...
		"SELECT id FROM shop.orders WHERE id IN (SELECT * FROM a.x)": {tables: "a.x"},
		"SELECT id FROM a.x UNION ALL SELECT * FROM b.y":             {tables: "b.y"},
		"SELECT (SELECT * FROM a.x LIMIT 1) FROM b.y":                {tables: "a.x"},
		// ... except directly under EXISTS, which asks only whether a row exists.
		"SELECT id FROM a.x WHERE EXISTS (SELECT * FROM b.y WHERE y.id = x.id)": {},
		"SELECT id FROM a.x WHERE NOT EXISTS (SELECT * FROM b.y)":               {},
		"SELECT x.* FROM a.x WHERE EXISTS (SELECT * FROM b.y)":                  {tables: "a.x"},
		"SELECT id FROM a.x WHERE EXISTS (SELECT * FROM (SELECT * FROM b.y) q)": {tables: "b.y"},
		// With a LIMIT or OFFSET the number of rows decides, and DISTINCT * makes that depend on the columns.
		"SELECT id FROM a.x WHERE EXISTS (SELECT DISTINCT * FROM b.y LIMIT 1 OFFSET 3)": {tables: "b.y"},
		// What the walk cannot attribute stays a star over everything.
		"SELECT nope.* FROM shop.orders o": {unsure: true},
		"SELECT o.* FROM shop.orders o JOIN shop.gen o2 ON true WHERE EXISTS (SELECT o.* FROM a.x)": {tables: "shop.orders"},
		"SELECT (SELECT o.* FROM a.x LIMIT 1) FROM shop.orders o":                                   {unsure: true},
		"SELECT t.* FROM a.t JOIN b.t ON true":                                                      {unsure: true},
		"SELECT COLUMNS('a.*') + 1 FROM shop.orders":                                                {unsure: true},
		// Not stars.
		"SELECT count(*) FROM shop.orders":                         {},
		"SELECT COUNT( * ) FROM shop.orders":                       {},
		"SELECT id * 2, qty*price FROM shop.orders":                {},
		"SELECT id, status FROM shop.orders WHERE note = '*'":      {},
		"SELECT count(*), sum(a * b) FROM shop.orders GROUP BY id": {},
		"SELECT 1": {},
	} {
		refs := parseForRefs(t, sqlText)
		var names []string
		for _, tb := range refs.StarTables {
			names = append(names, tb.Schema+"."+tb.Name)
		}
		sort.Strings(names)
		if got := strings.Join(names, " "); got != w.tables || refs.Star != w.unsure || refs.StarNamedJoin != w.namedJoin {
			t.Errorf("%s:\n  got  tables %q, Star %v, StarNamedJoin %v\n  want tables %q, Star %v, StarNamedJoin %v",
				sqlText, got, refs.Star, refs.StarNamedJoin, w.tables, w.unsure, w.namedJoin)
		}
	}
	for sqlText, want := range map[string]bool{
		"SELECT * FROM a.x JOIN b.y USING (id)":                       true,
		"SELECT * FROM a.x NATURAL JOIN b.y":                          true,
		"SELECT * FROM a.x LEFT JOIN b.y USING (id, k)":               true,
		"SELECT * FROM (SELECT * FROM a.x NATURAL LEFT JOIN b.y) q":   true,
		"SELECT * FROM a.x JOIN b.y ON x.id = y.id":                   false,
		"SELECT * FROM a.x, b.y":                                      false,
		"SELECT * FROM a.x CROSS JOIN b.y":                            false,
		"SELECT * FROM a.x":                                           false,
		"SELECT id FROM a.x WHERE note = 'natural join b using (id)'": false,
	} {
		refs := parseForRefs(t, sqlText)
		if refs.NamedJoin != want {
			t.Errorf("%s: NamedJoin = %v, want %v", sqlText, refs.NamedJoin, want)
		}
		if wantNatural := want && strings.Contains(sqlText, "NATURAL"); refs.Natural != wantNatural {
			t.Errorf("%s: Natural = %v, want %v", sqlText, refs.Natural, wantNatural)
		}
	}
}

// askingJob is f.job with the views handed out on the worker's question.
func askingJob(f copyFixture, sqlText string, seen *[]Refs, script func(Refs) (string, error)) Job {
	j := f.job(sqlText)
	j.ViewsSQL = ""
	j.ViewsFor = func(r Refs) (string, error) {
		*seen = append(*seen, r)
		return script(r)
	}
	return j
}

// #2029: the worker asks once, with what the statement names, and runs over
// the script it is answered with.
func TestRun_asksForTheViewsItNeeds(t *testing.T) {
	f := newCopyFixture(t)
	r := newTestRunner(t, testLimits())
	var seen []Refs
	res, err := r.Run(context.Background(), askingJob(f, "SELECT id, status FROM shop.orders ORDER BY id", &seen,
		func(Refs) (string, error) { return f.views, nil }))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(res.Rows) != 2 {
		t.Errorf("rows = %v, want 2", res.Rows)
	}
	want := []Refs{{Tables: []TableRef{{Schema: "shop", Name: "orders"}}}}
	if !reflect.DeepEqual(seen, want) {
		t.Errorf("asked with %+v, want %+v", seen, want)
	}
	if res.Phases.Views <= 0 || res.Phases.Total <= 0 {
		t.Errorf("phases = %+v, want the views and total phases measured", res.Phases)
	}

	// An empty answer installs nothing: the statement then fails the way a
	// missing view does, which proves the answer is what gets installed.
	seen = nil
	_, err = r.Run(context.Background(), askingJob(f, "SELECT id FROM shop.orders", &seen,
		func(Refs) (string, error) { return "", nil }))
	var qerr *QueryError
	if !errors.As(err, &qerr) || !strings.Contains(qerr.Message, "does not exist") {
		t.Errorf("empty answer: err = %v, want the query to fail on a missing table", err)
	}
}

// A statement the worker refuses never asks: the refusal comes back as it
// always has, and the parent's ViewsFor is not called.
func TestRun_refusedStatementNeverAsks(t *testing.T) {
	f := newCopyFixture(t)
	r := newTestRunner(t, testLimits())
	var seen []Refs
	_, err := r.Run(context.Background(), askingJob(f, "COPY (SELECT 1) TO 'x.csv'", &seen,
		func(Refs) (string, error) { return f.views, nil }))
	var refused *RefusedError
	if !errors.As(err, &refused) {
		t.Fatalf("err = %v, want a RefusedError", err)
	}
	if len(seen) != 0 {
		t.Errorf("ViewsFor called for a refused statement: %+v", seen)
	}
}

// A ViewsFor that fails ends the run as a worker error, with the worker
// stopped (the test would hang on a worker left waiting for its answer).
func TestRun_viewsForErrorIsAWorkerError(t *testing.T) {
	f := newCopyFixture(t)
	r := newTestRunner(t, testLimits())
	var seen []Refs
	_, err := r.Run(context.Background(), askingJob(f, "SELECT 1", &seen,
		func(Refs) (string, error) { return "", errors.New("boom") }))
	var werr *WorkerError
	if !errors.As(err, &werr) || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("err = %v, want a WorkerError carrying the cause", err)
	}
	if res, err := r.Run(context.Background(), f.job("SELECT 1 AS n")); err != nil || len(res.Rows) != 1 {
		t.Errorf("runner after a failed ask: %v %v, want it serving", res.Rows, err)
	}
}

// A statement longer than a pipe's buffer still reaches a worker that will
// ask: the job is written while the parent waits for the question.
func TestRun_longStatementWithAsk(t *testing.T) {
	f := newCopyFixture(t)
	r := newTestRunner(t, testLimits())
	var seen []Refs
	long := "SELECT count(*) AS n FROM shop.orders WHERE status <> '" + strings.Repeat("x", 300<<10) + "'"
	res, err := r.Run(context.Background(), askingJob(f, long, &seen,
		func(Refs) (string, error) { return f.views, nil }))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := res.Rows[0][0]; got != json.Number("2") {
		t.Errorf("count = %#v, want 2", got)
	}
}
