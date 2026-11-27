package sqlsandbox

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"
)

// shapeConn is a DuckDB session with the joinViews views, whose estimates
// DuckDB reads itself: tl 1e6 rows, ta and tb 7e4 each.
func shapeConn(t *testing.T) *sql.Conn {
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
	for _, stmt := range strings.Split(strings.TrimSpace(joinViews), "\n") {
		if _, err := conn.ExecContext(ctx, stmt); err != nil {
			t.Fatal(err)
		}
	}
	return conn
}

// shapeWritten is joinViewsQuery with its FROM in an order that is
// connected already: tl first, and ta and tb each joined to it.
var shapeWritten = strings.Replace(joinViewsQuery, "FROM ta, tb, tl", "FROM tl, ta, tb", 1)

// The decision alone, on each shape the rule names.
func TestPickShapeOrder(t *testing.T) {
	conn := shapeConn(t)
	ctx := context.Background()
	for _, tc := range []struct {
		name   string
		sql    string
		order  string
		tables []string
		note   string
	}{
		{name: "connected as written", sql: shapeWritten, order: JoinOrderByShapeWritten},
		{name: "not connected as written", sql: joinViewsQuery, order: JoinOrderByShapeConnected, tables: []string{"tl", "ta", "tb"}},
		{name: "one table", sql: "SELECT count(*) AS n FROM tl WHERE lw = 1", note: "one table"},
		{name: "an explicit JOIN", sql: "SELECT count(*) AS n FROM ta JOIN tb ON aw = bw, tl WHERE la = aid AND lb = bid", note: "JOIN"},
		{name: "a subquery in FROM", sql: "SELECT count(*) AS n FROM (SELECT * FROM ta) a, tb, tl WHERE la = aid AND lb = bid AND lw = aw", note: "SUBQUERY"},
		{name: "a WITH", sql: "WITH a AS (SELECT * FROM ta) SELECT count(*) AS n FROM a, tb, tl WHERE la = aid AND lb = bid", note: "WITH"},
		{name: "an OR at the top of WHERE", sql: "SELECT count(*) AS n FROM ta, tb, tl WHERE la = aid OR lb = bid", note: "an OR"},
		{name: "a *", sql: "SELECT * FROM ta, tb, tl WHERE la = aid AND lb = bid", note: "a *"},
		{name: "a table's *", sql: "SELECT tl.* FROM ta, tb, tl WHERE la = aid AND lb = bid", note: "a *"},
		{name: "a #n", sql: "SELECT aw FROM ta, tb, tl WHERE la = aid AND lb = bid ORDER BY #1", note: "#n"},
		{name: "COLUMNS(*)", sql: "SELECT max(COLUMNS(*)) FROM ta, tb, tl WHERE la = aid AND lb = bid", note: "COLUMNS"},
		{name: "a real cross product", sql: "SELECT count(*) AS n FROM ta, tb, tl WHERE la = aid", note: "no condition"},
		// One table twice, under two aliases: each qualified column is its
		// alias's, and y has no condition with x, so the order is rebuilt.
		{name: "the same table twice", sql: "SELECT count(*) AS n FROM ta x, ta y, tl WHERE x.aid = la AND y.aid = lb",
			order: JoinOrderByShapeConnected, tables: []string{"tl", "x", "y"}},
		{name: "the same table twice, connected as written", sql: "SELECT count(*) AS n FROM ta x, tl, ta y WHERE x.aid = la AND y.aid = lb",
			order: JoinOrderByShapeWritten},
		// A column two tables have, unqualified: no order, and the note
		// does not name it.
		{name: "an ambiguous unqualified column", sql: "SELECT count(*) AS n FROM ta x, ta y, tl WHERE aid = la AND y.aid = lb", note: shapeOrderColumnNote},
		// A MySQL string in double quotes is a column's name to the copy:
		// the note must not carry it.
		{name: "a string in double quotes", sql: `SELECT count(*) AS n FROM ta, tb, tl WHERE la = aid AND lb = bid AND aw = "Secret Value"`, note: shapeOrderColumnNote},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := pickShapeOrder(ctx, conn, tc.sql)
			if got.order != tc.order || !slices.Equal(got.tables, tc.tables) {
				t.Fatalf("order %q %v, want %q %v (note %q)", got.order, got.tables, tc.order, tc.tables, got.note)
			}
			if tc.order != "" {
				if got.note != "" {
					t.Errorf("a note with an order: %q", got.note)
				}
				if tc.order == JoinOrderByShapeWritten && got.text != tc.sql {
					t.Errorf("the order written runs another text: %q", got.text)
				}
				if tc.order == JoinOrderByShapeConnected && got.text == tc.sql {
					t.Error("the connected order runs the statement's own text")
				}
				return
			}
			if !strings.Contains(got.note, tc.note) || got.text != "" {
				t.Errorf("note %q text %q, want a note with %q", got.note, got.text, tc.note)
			}
			if strings.Contains(got.note, "Secret") || strings.Contains(got.note, "aid") {
				t.Errorf("the note names the statement's columns: %q", got.note)
			}
		})
	}
}

// A note of a step that did not work is fixed text: the step's own can
// quote the engine's, and so a row's value.
func TestShapeNoteForLog(t *testing.T) {
	for _, why := range []string{"the column Secret is in more than one table", "the column Secret is in none of the tables",
		"Secret names more than one table", "the column x.Secret is not a column of a table of the FROM"} {
		if got := shapeNoteForLog(why); got != shapeOrderColumnNote {
			t.Errorf("%q: %q", why, got)
		}
	}
	if got := shapeNoteForLog("a JOIN in the FROM"); got != "a JOIN in the FROM" {
		t.Errorf("a shape's why changed: %q", got)
	}
}

// shapeJob is a worker job over joinViews, with the option asked or not.
func shapeJob(dir, statement string, byShape bool) wireJob {
	return wireJob{CopyDirs: []string{dir}, ViewsSQL: joinViews, SQL: statement, JoinOrderByShape: byShape,
		Threads: 1, MemoryLimit: "256MB", MaxRows: 100, MaxResultBytes: 1 << 20}
}

// In the worker, through DuckDB's own plans: each order picked answers what
// the statement answers without the option, and says so.
func TestWorker_joinOrderByShape(t *testing.T) {
	dir := t.TempDir()
	for _, tc := range []struct {
		name, sql, order, tables string
	}{
		{"connected", joinViewsQuery, JoinOrderByShapeConnected, "tl, ta, tb"},
		{"written", shapeWritten, JoinOrderByShapeWritten, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			want := runJob(shapeJob(dir, tc.sql, false), nil, os.Stderr)
			if want.Error != nil || len(want.Rows) != 10 {
				t.Fatalf("without the option: %+v %v", want.Error, want.Rows)
			}
			if want.JoinOrderByShape != "" || want.JoinOrderByShapeNote != "" {
				t.Errorf("without the option: %q %q", want.JoinOrderByShape, want.JoinOrderByShapeNote)
			}
			var stderr bytes.Buffer
			res := runJob(shapeJob(dir, tc.sql, true), nil, &stderr)
			if res.Error != nil {
				t.Fatalf("with the option: %+v", res.Error)
			}
			if res.JoinOrderByShape != tc.order || res.JoinOrderTables != tc.tables || res.JoinOrderByShapeNote != "" {
				t.Errorf("order %q %q note %q", res.JoinOrderByShape, res.JoinOrderTables, res.JoinOrderByShapeNote)
			}
			// Not the join check's: the parent must not read it as that.
			if res.JoinOrder != "" {
				t.Errorf("JoinOrder %q", res.JoinOrder)
			}
			if !reflect.DeepEqual(res.Rows, want.Rows) {
				t.Errorf("rows %v, want %v", res.Rows, want.Rows)
			}
			if len(res.Columns) != 2 || res.Columns[0].Name != "g" || res.Columns[1].Name != "n" {
				t.Errorf("columns %+v", res.Columns)
			}
			// Said on stderr before it runs, so a worker the kernel stops is
			// not run again in another order (oomretry.go).
			line := joinOrderWrittenLine
			if tc.tables != "" {
				line = joinOrderConnectedLine + tc.tables
			}
			if !strings.Contains(stderr.String(), line) {
				t.Errorf("stderr: %s", stderr.String())
			}
		})
	}
	// Left alone: a statement of another shape, with why; one the client
	// asked the order written for (STRAIGHT_JOIN); one the join check reads.
	res := runJob(shapeJob(dir, "SELECT count(*) AS n FROM tl WHERE lw = 1", true), nil, os.Stderr)
	if res.Error != nil || res.JoinOrderByShape != "" || res.JoinOrderByShapeNote != "one table" {
		t.Errorf("one table: %+v %q %q", res.Error, res.JoinOrderByShape, res.JoinOrderByShapeNote)
	}
	keep := shapeJob(dir, joinViewsQuery, true)
	keep.KeepJoinOrder = true
	if res := runJob(keep, nil, os.Stderr); res.Error != nil || res.JoinOrderByShape != "" || res.JoinOrderByShapeNote != "" || res.JoinOrderTables != "" {
		t.Errorf("KeepJoinOrder: %+v %q %q", res.Error, res.JoinOrderByShape, res.JoinOrderByShapeNote)
	}
	check := shapeJob(dir, joinViewsQuery, true)
	check.JoinNDV = map[string]float64{"aid": 70000}
	if res := runJob(check, nil, os.Stderr); res.Error != nil || res.JoinOrderByShape != "" || res.JoinOrderByShapeNote != "" {
		t.Errorf("JoinNDV: %+v %q %q", res.Error, res.JoinOrderByShape, res.JoinOrderByShapeNote)
	}
	// An engine error in the connected order says it ran in that order,
	// so the parent does not run it again (oomretry.go).
	bad := shapeJob(dir, "SELECT aw % 10 AS g, count(*) AS n FROM ta, tb, tl WHERE lw = aw AND la = aid AND lw = bw AND lb = bid AND CAST('x' || lid AS INTEGER) > 0 GROUP BY g", true)
	if res := runJob(bad, nil, os.Stderr); res.Error == nil || res.Error.Kind != errQuery || res.JoinOrder != joinOrderConnected || res.JoinOrderTables != "tl, ta, tb" {
		t.Errorf("an engine error: %+v %q %q", res.Error, res.JoinOrder, res.JoinOrderTables)
	}
}

// blowupViews are joinViews with a column of two values where joinViews
// has unique ones: DuckDB's own order joins ta with tb on that column alone
// first (a condition it derives from the others), about 2.45e9 rows, which
// does not answer in a 256MB worker; in the connected order (tl, ta, tb)
// each join is on a key. small is how many rows ta and tb have, and tl
// holds each of their keys n times over.
func blowupViews(small, n int) string {
	return fmt.Sprintf(`CREATE VIEW ta AS SELECT range AS aid, range %% 2 AS aw FROM range(%[1]d);
CREATE VIEW tb AS SELECT range AS bid, range %% 2 AS bw FROM range(%[1]d);
CREATE VIEW tl AS SELECT range AS lid, (range %% %[1]d) %% 2 AS lw, range %% %[1]d AS la, range %% %[1]d AS lb FROM range(%[2]d);
`, small, small*n)
}

const blowupQuery = "SELECT aw AS g, count(*) AS n FROM ta, tb, tl WHERE lw = aw AND la = aid AND lw = bw AND lb = bid GROUP BY g ORDER BY g"

// Through the real worker process: a statement that does not answer in
// DuckDB's own order answers with the option, what it answers without the
// option where every order does (small data), and the option is off unless
// asked.
func TestRunner_joinOrderByShape(t *testing.T) {
	if (Session{}).JoinOrderByShape {
		t.Fatal("on by default")
	}
	dir := t.TempDir()
	limits := testLimits()
	limits.Threads, limits.MemoryLimit, limits.Timeout = 1, "256MB", 20*time.Second
	r := newTestRunner(t, limits)
	ctx := context.Background()

	small := Job{CopyDirs: []string{dir}, ViewsSQL: blowupViews(300, 3), SQL: blowupQuery}
	want, err := r.Run(ctx, small)
	if err != nil {
		t.Fatalf("small, without the option: %v", err)
	}
	small.Session.JoinOrderByShape = true
	got, err := r.Run(ctx, small)
	if err != nil {
		t.Fatalf("small, with the option: %v", err)
	}
	if fmt.Sprint(got.Rows) != fmt.Sprint(want.Rows) || got.JoinOrderByShape != JoinOrderByShapeConnected {
		t.Errorf("small: rows %v (%q), want %v", got.Rows, got.JoinOrderByShape, want.Rows)
	}
	if want.JoinOrderByShape != "" {
		t.Errorf("small, without the option: %q", want.JoinOrderByShape)
	}

	big := Job{CopyDirs: []string{dir}, ViewsSQL: blowupViews(70000, 15), SQL: blowupQuery}
	if res, err := r.Run(ctx, big); err == nil {
		t.Fatalf("big, without the option, answered: %v; the test no longer shows the blow-up", res.Rows)
	} else {
		var qe *QueryError
		var te *TimeoutError
		if !errors.As(err, &qe) && !errors.As(err, &te) {
			t.Fatalf("big, without the option: %T %v", err, err)
		}
	}
	big.Session.JoinOrderByShape = true
	res, err := r.Run(ctx, big)
	if err != nil {
		t.Fatalf("big, with the option: %v", err)
	}
	// Each tl row meets one ta and one tb row: half of them have aw 0.
	if fmt.Sprint(res.Rows) != "[[0 525000] [1 525000]]" || res.JoinOrderByShape != JoinOrderByShapeConnected {
		t.Errorf("big: rows %v, order %q", res.Rows, res.JoinOrderByShape)
	}
}
