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

// #3043: in fresh mode, a statement that reads a table of the copy either
// reads the table's recent changes or says it answered from the snapshot
// alone. It never answers from the snapshot in silence. The shapes below are
// the ways a WITH can stand between the statement and the table.
//
// withChainShapes are statements over two tables: {T} has a recent change
// (its row 1 went from the snapshot's value to "paid") and can have a tail;
// {N} has one too, and capture does not record it, so it cannot. {V} is the
// expression that prints a row as id=status. Every statement returns the
// rows of one of the two tables in that form.
var withChainShapes = []struct {
	name, stmt string
	// noTail: the statement reads the table that cannot have a tail.
	noTail bool
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
	{"a table that cannot have a tail", "SELECT {V} FROM {N} ORDER BY id", true},
	{"a block reading a block over a table that cannot have a tail", "WITH x AS (SELECT * FROM {N}), y AS (SELECT * FROM x) SELECT {V} FROM y ORDER BY id", true},
	{"a block over each table", "WITH x AS (SELECT * FROM {T}), n AS (SELECT id AS nid FROM {N}), y AS (SELECT x.* FROM x JOIN n ON x.id = n.nid) SELECT {V} FROM y ORDER BY id", true},
}

// checkWithChainShapes runs every shape and holds each answer to the rule:
// the recent change is in it, or the answer carries a note. run returns the
// rows as text and the note.
func checkWithChainShapes(t *testing.T, value, table, noTail string, run func(stmt string) (rows []string, note string, err error)) {
	bare := table[strings.LastIndex(table, ".")+1:]
	for _, c := range withChainShapes {
		stmt := strings.NewReplacer("{V}", value, "{T}", table, "{N}", noTail, "{t}", bare).Replace(c.stmt)
		t.Run(c.name, func(t *testing.T) {
			rows, note, err := run(stmt)
			if err != nil {
				t.Fatalf("%s\n\tfailed: %v", stmt, err)
			}
			t.Logf("rows %v, note %q", rows, note)
			if len(rows) != 1 {
				t.Fatalf("%s\n\tanswered %v, want the table's one row", stmt, rows)
			}
			switch {
			case rows[0] == "1=paid" && !c.noTail:
			case rows[0] == "1=paid":
				t.Errorf("%s\n\tread a recent change of a table capture does not record", stmt)
			case note != "":
			default:
				t.Errorf("%s\n\tanswered %v from the snapshot alone, with no note", stmt, rows)
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
	checkWithChainShapes(t, "id || '=' || status", "public."+tbl.Table, "public."+none.Table, run)
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
	checkWithChainShapes(t, "CAST(id AS VARCHAR) || '=' || status", "shop."+tbl.Table, "shop."+none.Table, run)
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
