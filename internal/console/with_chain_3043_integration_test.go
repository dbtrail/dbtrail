//go:build integration

package console

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

// #3043: in fresh mode, a statement that names a table of the copy reads
// the table's recent changes, whatever WITH blocks stand between the
// statement and the table. A block that reads another block used to keep
// every table of the statement from them, in silence.
//
// withChainShapes are statements over two tables: {T} has a recent change
// (its row 1 went from the snapshot's value to "paid") and can have a tail;
// {N} has one too, and capture does not record it, so it cannot. {V} is the
// expression that prints a row as id=status, {t} is {T} without its schema.
// Every statement returns the one row of one of the two tables in that form.
//
// snapshot marks the shapes answered from the snapshot alone with no note.
// That is a decision (2026-11-21, with #3043), not a gap, for two kinds:
// a statement over a table that cannot have a tail (its age is in the
// answer's lag, TestIntegrationFreshOverATableThatCannotHaveATail), and a
// statement whose tables the parse cannot tell (a table function, which can
// read a table it does not name; a qualified name that is no view of the
// copy): giving it tails would build one for every table of the copy.
var withChainShapes = []struct {
	name, stmt string
	snapshot   bool
}{
	{"no WITH", "SELECT {V} FROM {T} ORDER BY id", false},
	{"one block", "WITH x AS (SELECT * FROM {T}) SELECT {V} FROM x ORDER BY id", false},
	{"a block reading a block", "WITH x AS (SELECT * FROM {T}), y AS (SELECT x.* FROM x) SELECT {V} FROM y ORDER BY id", false},
	{"three levels", "WITH x AS (SELECT * FROM {T}), y AS (SELECT * FROM x), z AS (SELECT * FROM y) SELECT {V} FROM z ORDER BY id", false},
	{"one block used twice", "WITH x AS (SELECT * FROM {T}) SELECT {V} FROM x WHERE id IN (SELECT id FROM x) ORDER BY id", false},
	{"a block read by two blocks", "WITH x AS (SELECT * FROM {T}), y AS (SELECT * FROM x), z AS (SELECT id AS zid FROM x) SELECT {V} FROM y JOIN z ON y.id = z.zid ORDER BY id", false},
	{"two independent blocks", "WITH x AS (SELECT * FROM {T}), y AS (SELECT 1 AS one) SELECT {V} FROM x, y ORDER BY id", false},
	{"a recursive WITH beside the table", "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 1) SELECT {V} FROM {T}, n ORDER BY id", false},
	{"a recursive WITH over the table", "WITH RECURSIVE r AS (SELECT id, status, 1 AS depth FROM {T} UNION ALL SELECT id, status, depth + 1 FROM r WHERE depth < 1) SELECT {V} FROM r ORDER BY id", false},
	{"a block named as the table", "WITH {t} AS (SELECT * FROM {T}) SELECT {V} FROM {t} ORDER BY id", false},
	{"a block named as the table, read by a block", "WITH {t} AS (SELECT * FROM {T}), y AS (SELECT * FROM {t}) SELECT {V} FROM y ORDER BY id", false},
	{"a block with a column list", "WITH x(id, status) AS (SELECT id, status FROM {T}), y AS (SELECT * FROM x) SELECT {V} FROM y ORDER BY id", false},
	{"blocks inside a subquery", "SELECT {V} FROM (WITH x AS (SELECT * FROM {T}), y AS (SELECT * FROM x) SELECT * FROM y) q ORDER BY id", false},
	{"a block of the subquery reading a block of the statement", "WITH x AS (SELECT * FROM {T}) SELECT {V} FROM (WITH y AS (SELECT * FROM x) SELECT * FROM y) q ORDER BY id", false},
	{"MATERIALIZED and NOT MATERIALIZED", "WITH x AS MATERIALIZED (SELECT * FROM {T}), y AS NOT MATERIALIZED (SELECT * FROM x) SELECT {V} FROM y ORDER BY id", false},
	{"a block that reads no table", "WITH c AS (SELECT 1 AS one), y AS (SELECT one FROM c) SELECT {V} FROM {T}, y ORDER BY id", false},
	{"quoted and upper-case block names", `WITH "X" AS (SELECT * FROM {T}), Y AS (SELECT * FROM x) SELECT {V} FROM "y" ORDER BY id`, false},
	{"a system table beside the table", "SELECT {V} FROM {T} WHERE (SELECT count(*) FROM duckdb_views) >= 0 ORDER BY id", false},
	// By decision, as before #3043:
	{"a table that cannot have a tail", "SELECT {V} FROM {N} ORDER BY id", true},
	{"a block reading a block over a table that cannot have a tail", "WITH x AS (SELECT * FROM {N}), y AS (SELECT * FROM x) SELECT {V} FROM y ORDER BY id", true},
	{"a block over each table", "WITH x AS (SELECT * FROM {T}), n AS (SELECT id AS nid FROM {N}), y AS (SELECT x.* FROM x JOIN n ON x.id = n.nid) SELECT {V} FROM y ORDER BY id", true},
	{"a table function beside the table", "SELECT {V} FROM {T}, range(1) ORDER BY id", true},
	{"a table function beside a block reading a block", "WITH x AS (SELECT * FROM {T}), y AS (SELECT * FROM x) SELECT {V} FROM y, range(1) ORDER BY id", true},
	{"a qualified name that is no view of the copy", "SELECT {V} FROM {T} WHERE (SELECT count(*) FROM information_schema.schemata) >= 0 ORDER BY id", true},
}

// checkWithChainShapes runs every shape and holds its answer to what the
// shape says: the recent change, or the snapshot's row with no note. stale
// is the snapshot's row. run returns the rows as text and the note.
func checkWithChainShapes(t *testing.T, value, table, noTail, stale string, run func(stmt string) (rows []string, note string, err error)) {
	bare := table[strings.LastIndex(table, ".")+1:]
	for _, c := range withChainShapes {
		stmt := strings.NewReplacer("{V}", value, "{T}", table, "{N}", noTail, "{t}", bare).Replace(c.stmt)
		t.Run(c.name, func(t *testing.T) {
			// Twice, and the second answer is the one judged: a connection
			// whose last answer read the recent changes is told once when
			// the next one does not (snapshotAloneNote), whatever the
			// statement, and that is the connection's note, not this
			// statement's.
			if _, _, err := run(stmt); err != nil {
				t.Fatalf("%s\n\tfailed: %v", stmt, err)
			}
			rows, note, err := run(stmt)
			if err != nil {
				t.Fatalf("%s\n\tfailed: %v", stmt, err)
			}
			want := "1=paid"
			if c.snapshot {
				want = stale
			}
			if len(rows) != 1 || rows[0] != want || note != "" {
				t.Errorf("%s\n\tanswered %v with note %q, want [%s] and no note", stmt, rows, note, want)
			}
		})
	}
}

// On a PostgreSQL server's copy, by PostgreSQL's rules.
func TestIntegrationPGFreshWithBlocks_3043(t *testing.T) {
	t.Setenv("BINTRAIL_SQL_TAILS", "on")
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	r := newPGTailRig(t)
	r.b.pgSource = true
	r.s.sqlRunner = sandboxRunner{sqlsandbox.New(sqlsandbox.Config{Exe: exe, Args: []string{}, Limits: sqlsandbox.Limits{Timeout: 60 * time.Second}})}
	r.s.sqlLimits = resolveSQLLimits(sqlsandbox.Limits{})
	r.b.baselineConfigured = true
	EnablePostgresTails(true)
	t.Cleanup(func() { EnablePostgresTails(false) })

	tbl := r.table(nil, [2]string{"1", "one"})
	none := r.table(nil, [2]string{"1", "one"})
	r.s.tailCapturesHook = func(string) (func(string, string) bool, bool) {
		return func(_, table string) bool { return table != none.Table }, true
	}
	r.change(tbl.Table, event.EventUpdate, "1", "paid", 0x610, 20*time.Minute)
	r.change(none.Table, event.EventUpdate, "1", "paid", 0x620, 20*time.Minute)
	r.checkpoint(0x620)

	q := &SQLOnCopy{s: r.s, b: r.b, user: "u", id: bootServerID}
	run := func(stmt string) ([]string, string, error) {
		res, err := q.Run(context.Background(), stmt, "", sqlsandbox.Session{PostgresRules: true})
		if err != nil {
			return nil, "", err
		}
		q.NoteRoute("copy")
		var out []string
		for _, row := range res.Rows {
			out = append(out, row[0].(string))
		}
		return out, res.Note, nil
	}
	settleTails(t, r.s, func() { run("SELECT status FROM public." + tbl.Table) })
	checkWithChainShapes(t, "id || '=' || status", "public."+tbl.Table, "public."+none.Table, "1=one", run)
}

// On a MySQL server's copy: the same statements through the same steps.
func TestIntegrationFreshWithBlocks_3043(t *testing.T) {
	t.Setenv("BINTRAIL_SQL_TAILS", "on")
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	r, _ := newTailRig(t)
	r.s.sqlRunner = sandboxRunner{sqlsandbox.New(sqlsandbox.Config{Exe: exe, Args: []string{}, Limits: sqlsandbox.Limits{Timeout: 60 * time.Second}})}
	r.s.sqlLimits = resolveSQLLimits(sqlsandbox.Limits{})
	r.b.baselineConfigured = true

	tbl := r.table(nil) // 1=new in the published copy
	none := r.table(nil)
	r.s.tailCapturesHook = func(string) (func(string, string) bool, bool) {
		return func(_, table string) bool { return table != none.Table }, true
	}
	at := time.Now().UTC().Add(-time.Hour).Truncate(time.Second)
	r.change(tbl.Table, event.EventUpdate, "1", "paid", "binlog.000008", 100, at)
	r.change(none.Table, event.EventUpdate, "1", "paid", "binlog.000008", 200, at)

	q := &SQLOnCopy{s: r.s, b: r.b, user: "u", id: bootServerID}
	run := func(stmt string) ([]string, string, error) {
		res, err := q.Run(context.Background(), stmt, "", sqlsandbox.Session{})
		if err != nil {
			return nil, "", err
		}
		q.NoteRoute("copy")
		var out []string
		for _, row := range res.Rows {
			out = append(out, row[0].(string))
		}
		return out, res.Note, nil
	}
	settleTails(t, r.s, func() { run("SELECT status FROM shop." + tbl.Table) })
	checkWithChainShapes(t, "CAST(id AS VARCHAR) || '=' || status", "shop."+tbl.Table, "shop."+none.Table, "1=new", run)
}

// settleTails has the boot server's keeper build and publish the tail of the
// tables that read reads: a read starts the build, the follower publishes it.
func settleTails(t *testing.T, s *Server, read func()) {
	t.Helper()
	read()
	s.tails.mu.Lock()
	k := s.tails.by[bootServerID]
	s.tails.mu.Unlock()
	if k == nil {
		t.Fatal("no keeper for the server: fresh mode is not on")
	}
	k.followOnce(context.Background(), time.Minute)
	k.settle()
	k.followOnce(context.Background(), time.Minute)
}

// A statement that names two tables of a copy of six, and a WITH that reads
// another WITH, is given the tails of those two and the keeper is asked for
// no other: the rest of the copy is not followed for it (#3043).
func TestIntegrationFreshWithBlocksAsksOnlyForTheTablesNamed_3043(t *testing.T) {
	t.Setenv("BINTRAIL_SQL_TAILS", "on")
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	r, _ := newTailRig(t)
	r.s.sqlRunner = sandboxRunner{sqlsandbox.New(sqlsandbox.Config{Exe: exe, Args: []string{}, Limits: sqlsandbox.Limits{Timeout: 60 * time.Second}})}
	r.s.sqlLimits = resolveSQLLimits(sqlsandbox.Limits{})
	r.b.baselineConfigured = true
	r.s.tailCapturesHook = func(string) (func(string, string) bool, bool) { return nil, true }
	at := time.Now().UTC().Add(-time.Hour).Truncate(time.Second)
	var names []string
	for i := 0; i < 6; i++ {
		tbl := r.table(nil)
		names = append(names, tbl.Table)
		r.change(tbl.Table, event.EventUpdate, "1", "paid", "binlog.000008", uint64(100*(i+1)), at)
	}
	q := &SQLOnCopy{s: r.s, b: r.b, user: "u", id: bootServerID}
	stmt := "WITH x AS (SELECT * FROM shop." + names[1] + "), y AS (SELECT x.id, x.status || '/' || o.status AS both FROM x JOIN shop." + names[4] + " o ON o.id = x.id) SELECT both FROM y"
	read := func() string {
		res, err := q.Run(context.Background(), stmt, "", sqlsandbox.Session{})
		if err != nil {
			t.Fatal(err)
		}
		age := q.LastRunAge()
		q.NoteRoute("copy")
		if len(res.Rows) != 1 {
			t.Fatalf("rows %v", res.Rows)
		}
		if got := res.Rows[0][0].(string); (got == "paid/paid") != age.Tailed {
			t.Fatalf("answered %q, and the run says it read every table with its changes: %v", got, age.Tailed)
		}
		return res.Rows[0][0].(string)
	}
	settleTails(t, r.s, func() { read() })
	if got := read(); got != "paid/paid" {
		t.Fatalf("answered %q, want both tables with their tails", got)
	}
	r.s.tails.mu.Lock()
	k := r.s.tails.by[bootServerID]
	r.s.tails.mu.Unlock()
	k.pub.mu.Lock()
	asked := map[string]bool{}
	for key := range k.pub.cuts {
		asked[key] = true
	}
	k.pub.mu.Unlock()
	want := map[string]bool{tailKey("shop", names[1]): true, tailKey("shop", names[4]): true}
	if len(asked) != len(want) {
		t.Fatalf("the keeper was asked for %v, want only %v", asked, want)
	}
	for key := range want {
		if !asked[key] {
			t.Fatalf("the keeper was asked for %v, want %v", asked, want)
		}
	}
}

// A name the engine can resolve to a table of the copy without it matching
// a view by its schema (memory.<table> is the catalog and a table of the
// schema in use) keeps the whole statement from tails: laying changes over
// the table named in full and not over that one would join rows of two
// moments, under an answer that says every table was read with its changes.
func TestIntegrationFreshNeverTailsHalfAStatement_3043(t *testing.T) {
	t.Setenv("BINTRAIL_SQL_TAILS", "on")
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	r, _ := newTailRig(t)
	r.s.sqlRunner = sandboxRunner{sqlsandbox.New(sqlsandbox.Config{Exe: exe, Args: []string{}, Limits: sqlsandbox.Limits{Timeout: 60 * time.Second}})}
	r.s.sqlLimits = resolveSQLLimits(sqlsandbox.Limits{})
	r.b.baselineConfigured = true
	r.s.tailCapturesHook = func(string) (func(string, string) bool, bool) { return nil, true }
	at := time.Now().UTC().Add(-time.Hour).Truncate(time.Second)
	a, b := r.table(nil), r.table(nil)
	r.change(a.Table, event.EventUpdate, "1", "paid", "binlog.000008", 100, at)
	r.change(b.Table, event.EventUpdate, "1", "paid", "binlog.000008", 200, at)
	q := &SQLOnCopy{s: r.s, b: r.b, user: "u", id: bootServerID}
	run := func(stmt string) (string, CopyRunAge, error) {
		res, err := q.Run(context.Background(), stmt, "shop", sqlsandbox.Session{})
		if err != nil {
			return "", CopyRunAge{}, err
		}
		age := q.LastRunAge()
		q.NoteRoute("copy")
		if len(res.Rows) != 1 {
			t.Fatalf("%s: rows %v", stmt, res.Rows)
		}
		return res.Rows[0][0].(string), age, nil
	}
	both := "SELECT x.status || '/' || y.status FROM shop." + a.Table + " x JOIN shop." + b.Table + " y ON y.id = x.id"
	settleTails(t, r.s, func() {
		if _, _, err := run(both); err != nil {
			t.Fatal(err)
		}
	})
	if got, age, err := run(both); err != nil || got != "paid/paid" || !age.Tailed {
		t.Fatalf("both tables named in full: %q, tailed %v, %v", got, age.Tailed, err)
	}
	for _, other := range []string{"memory." + b.Table, "memory.shop." + b.Table, b.Table} {
		stmt := "SELECT x.status || '/' || y.status FROM shop." + a.Table + " x JOIN " + other + " y ON y.id = x.id"
		got, age, err := run(stmt)
		if err != nil {
			t.Logf("%s: not answered (%v)", stmt, err)
			continue
		}
		t.Logf("%s: %q, tailed %v", stmt, got, age.Tailed)
		switch {
		case got == "paid/paid" && age.Tailed:
		case got == "new/new" && !age.Tailed:
		default:
			t.Errorf("%s\n\tanswered %q and says every table was read with its changes: %v", stmt, got, age.Tailed)
		}
	}
}
