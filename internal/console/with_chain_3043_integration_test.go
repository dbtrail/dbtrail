//go:build integration

package console

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/metadata"
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
// statement whose tables are not known: a table function, which can read a
// table it does not name, and a qualified name that is no view of the copy,
// which the engine may still resolve to one of its tables (sqlTailTables).
//
// pg marks the ones also run by PostgreSQL's rules. The rule is one piece of
// code for every family, and each run starts a worker: the whole list runs
// once, and the package has ten minutes for all its tests.
var withChainShapes = []struct {
	name, stmt   string
	snapshot, pg bool
}{
	{"a block reading a block", "WITH x AS (SELECT * FROM {T}), y AS (SELECT x.* FROM x) SELECT {V} FROM y ORDER BY id", false, true},
	{"three levels", "WITH x AS (SELECT * FROM {T}), y AS (SELECT * FROM x), z AS (SELECT * FROM y) SELECT {V} FROM z ORDER BY id", false, false},
	{"a block read by two blocks", "WITH x AS (SELECT * FROM {T}), y AS (SELECT * FROM x), z AS (SELECT id AS zid FROM x) SELECT {V} FROM y JOIN z ON y.id = z.zid ORDER BY id", false, false},
	{"two independent blocks", "WITH x AS (SELECT * FROM {T}), y AS (SELECT 1 AS one) SELECT {V} FROM x, y ORDER BY id", false, false},
	{"a recursive WITH over the table", "WITH RECURSIVE r AS (SELECT id, status, 1 AS depth FROM {T} UNION ALL SELECT id, status, depth + 1 FROM r WHERE depth < 1) SELECT {V} FROM r ORDER BY id", false, true},
	{"a block named as the table, read by a block", "WITH {t} AS (SELECT * FROM {T}), y AS (SELECT * FROM {t}) SELECT {V} FROM y ORDER BY id", false, false},
	{"a block with a column list", "WITH x(id, status) AS (SELECT id, status FROM {T}), y AS (SELECT * FROM x) SELECT {V} FROM y ORDER BY id", false, false},
	{"blocks inside a subquery", "SELECT {V} FROM (WITH x AS (SELECT * FROM {T}), y AS (SELECT * FROM x) SELECT * FROM y) q ORDER BY id", false, false},
	{"MATERIALIZED and NOT MATERIALIZED", "WITH x AS MATERIALIZED (SELECT * FROM {T}), y AS NOT MATERIALIZED (SELECT * FROM x) SELECT {V} FROM y ORDER BY id", false, false},
	{"a block that reads no table", "WITH c AS (SELECT 1 AS one), y AS (SELECT one FROM c) SELECT {V} FROM {T}, y ORDER BY id", false, false},
	{"quoted and upper-case block names", `WITH "X" AS (SELECT * FROM {T}), Y AS (SELECT * FROM x) SELECT {V} FROM "y" ORDER BY id`, false, true},
	{"a system table beside the table", "SELECT {V} FROM {T} WHERE (SELECT count(*) FROM duckdb_views) >= 0 ORDER BY id", false, false},
	// By decision, as before #3043:
	{"a block reading a block over a table that cannot have a tail", "WITH x AS (SELECT * FROM {N}), y AS (SELECT * FROM x) SELECT {V} FROM y ORDER BY id", true, true},
	{"a block over each table", "WITH x AS (SELECT * FROM {T}), n AS (SELECT id AS nid FROM {N}), y AS (SELECT x.* FROM x JOIN n ON x.id = n.nid) SELECT {V} FROM y ORDER BY id", true, false},
	{"a table function beside a block reading a block", "WITH x AS (SELECT * FROM {T}), y AS (SELECT * FROM x) SELECT {V} FROM y, range(1) ORDER BY id", true, true},
	{"a qualified name that is no view of the copy", "SELECT {V} FROM {T} WHERE (SELECT count(*) FROM information_schema.schemata) >= 0 ORDER BY id", true, false},
}

// checkWithChainShapes runs the shapes (those marked pg alone when pgOnly)
// and holds each answer to what the shape says: the recent change, or the
// snapshot's row, and no note either way. stale is the snapshot's row. run
// runs a statement on a connection of its own and returns the rows as text
// and the note: a connection whose last answer read the recent changes is
// told once when the next one does not (snapshotAloneNote), whatever the
// statement, and that would be the connection's note, not the statement's.
func checkWithChainShapes(t *testing.T, pgOnly bool, value, table, noTail, stale string, run func(stmt string) (rows []string, note string, err error)) {
	bare := table[strings.LastIndex(table, ".")+1:]
	for _, c := range withChainShapes {
		if pgOnly && !c.pg {
			continue
		}
		stmt := strings.NewReplacer("{V}", value, "{T}", table, "{N}", noTail, "{t}", bare).Replace(c.stmt)
		t.Run(c.name, func(t *testing.T) {
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

	run := func(stmt string) ([]string, string, error) {
		q := &SQLOnCopy{s: r.s, b: r.b, user: "u", id: bootServerID}
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
	// Both tables, so the keeper has said of each whether it has a tail.
	settleTails(t, r.s, func() { run("SELECT a.status FROM public." + tbl.Table + " a, public." + none.Table + " b") })
	checkWithChainShapes(t, true, "id || '=' || status", "public."+tbl.Table, "public."+none.Table, "1=one", run)
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

	run := func(stmt string) ([]string, string, error) {
		q := &SQLOnCopy{s: r.s, b: r.b, user: "u", id: bootServerID}
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
	// Both tables, so the keeper has said of each whether it has a tail.
	settleTails(t, r.s, func() { run("SELECT a.status FROM shop." + tbl.Table + " a, shop." + none.Table + " b") })
	checkWithChainShapes(t, false, "CAST(id AS VARCHAR) || '=' || status", "shop."+tbl.Table, "shop."+none.Table, "1=new", run)

	// A connection that routes (the port with a forwarding user): the copy
	// answers the chained statement with the table's tail, and says the run
	// read every table with its changes. No star here: a routing session
	// refuses one over this rig's tables, which carry no definition.
	t.Run("a block reading a block, on a connection that routes", func(t *testing.T) {
		stmt := "WITH x AS (SELECT id, status FROM shop." + tbl.Table + "), y AS (SELECT id, status FROM x) SELECT CAST(id AS VARCHAR) || '=' || status FROM y ORDER BY id"
		q := &SQLOnCopy{s: r.s, b: r.b, user: "u", id: bootServerID}
		res, err := q.Run(context.Background(), stmt, "", sqlsandbox.Session{StrictStar: true})
		if err != nil {
			t.Fatalf("not answered by the copy: %v", err)
		}
		if age := q.LastRunAge(); len(res.Rows) != 1 || res.Rows[0][0].(string) != "1=paid" || res.Note != "" || !age.Tailed {
			t.Fatalf("answered %v with note %q, read with its changes: %v", res.Rows, res.Note, age.Tailed)
		}
	})
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
	stmt := "WITH x AS (SELECT * FROM shop." + names[1] + "), y AS (SELECT x.id, x.status || '/' || o.status AS pair FROM x JOIN shop." + names[4] + " o ON o.id = x.id) SELECT pair FROM y"
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
// a view (memory.<table> is the catalog and a table of the schema in use; a
// quoted path is the table's file) keeps the whole statement from tails: laying changes over
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
	// Each way of naming the second table, and what the statement is then
	// answered with: both tables with their tails, or both from the snapshot
	// and the run saying so. Never one of each.
	for _, c := range []struct {
		other  string
		want   string // "" when the engine does not resolve the name
		tailed bool
	}{
		{b.Table, "paid/paid", true},                  // bare: the view, in the schema in use
		{"memory.shop." + b.Table, "paid/paid", true}, // the engine's catalog written out: the view
		{"memory." + b.Table, "", false},              // no view of that name: the engine has none either
		// The table's own file, by its path: the parse reports a relation
		// of that name, which is no view and is the table all the same.
		{"'" + b.Path + "'", "new/new", false},
	} {
		stmt := "SELECT x.status || '/' || y.status FROM shop." + a.Table + " x JOIN " + c.other + " y ON y.id = x.id"
		got, age, err := run(stmt)
		switch {
		case c.want == "" && err != nil:
		case err != nil:
			t.Errorf("%s\n\tnot answered: %v", stmt, err)
		case got != c.want || age.Tailed != c.tailed:
			t.Errorf("%s\n\tanswered %q, read with its changes: %v; want %q and %v", stmt, got, age.Tailed, c.want, c.tailed)
		}
	}
}

// sqlTailTables leaves a bare, plain name aside when it matched no view, on
// the ground that it is then no table of the copy. That holds only while
// the match here (names compared with A-Z folded) sees every name the
// ENGINE takes for a view. This asks the engine, over real tables with
// tails, under both kinds of session: for every spelling of a table's name,
// the statement is answered with both tables' recent changes, or with
// neither and the run saying so, or not at all. Never one of each.
func TestIntegrationFreshNamesTheEngineResolves_3043(t *testing.T) {
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

	const (
		cafe   = "caf\u00e9"   // é as one code point
		kelvin = "\u212aelvin" // the Kelvin sign, which lower-cases to k
		longS  = "\u017ftats"  // the long s, which upper-cases to S
	)
	names := []string{cafe, "Orders", kelvin, longS}
	metas := map[string]*metadata.TableMeta{}
	meta := func(name string) {
		metas["shop."+name] = &metadata.TableMeta{Schema: "shop", Table: name, PKColumns: []string{"id"}, Columns: []metadata.ColumnMeta{
			{Name: "id", OrdinalPosition: 1, IsPK: true, DataType: "int", ColumnType: "int"},
			{Name: "status", OrdinalPosition: 2, DataType: "varchar", ColumnType: "varchar(32)"},
		}}
	}
	first := r.table(nil)
	meta(first.Table)
	r.change(first.Table, event.EventUpdate, "1", "paid", "binlog.000008", 100, at)
	stamp := r.stamp.Format(time.RFC3339)
	for i, name := range names {
		ddl := "CREATE TABLE `" + name + "` (\n  `id` int NOT NULL,\n  `status` varchar(32) DEFAULT NULL,\n  PRIMARY KEY (`id`)\n);\n"
		r.write(filepath.Join(r.root, unchangedSnapshotDir, "shop", name+".parquet"), ddl, map[string]string{
			baseline.MetaKeyBinlogFile: r.anchor.File, baseline.MetaKeyBinlogPos: strconv.FormatUint(r.anchor.Pos, 10),
			baseline.MetaKeySnapshotTimestamp: stamp, baseline.MetaKeyLastDumpAt: stamp, baseline.MetaKeyFoldGeneration: "0",
			baseline.MetaKeySnapshotProducer: baseline.ProducerDump, baseline.MetaKeyLockMode: string(baseline.LockModeFTWRL),
			baseline.MetaKeyCreateTableSQL: ddl,
		})
		meta(name)
		r.change(name, event.EventUpdate, "1", "paid", "binlog.000008", uint64(200+100*i), at)
	}
	r.b.resolver = metadata.NewResolverFromTables(1, metas)

	run := func(sess sqlsandbox.Session, stmt string) (string, CopyRunAge, error) {
		q := &SQLOnCopy{s: r.s, b: r.b, user: "u", id: bootServerID}
		res, err := q.Run(context.Background(), stmt, "shop", sess)
		if err != nil {
			return "", CopyRunAge{}, err
		}
		if len(res.Rows) != 1 {
			t.Fatalf("%s: rows %v", stmt, res.Rows)
		}
		return res.Rows[0][0].(string), q.LastRunAge(), nil
	}
	join := func(spelling string) string {
		return "SELECT x.status || '/' || y.status FROM shop." + first.Table + " x JOIN " + spelling + " y ON y.id = x.id"
	}
	// Each table's tail is built and published: named exactly, every one is
	// read with its changes.
	for _, name := range names {
		settleTails(t, r.s, func() {
			if _, _, err := run(sqlsandbox.Session{}, join(`shop."`+name+`"`)); err != nil {
				t.Fatalf("%s: %v", name, err)
			}
		})
		if got, age, err := run(sqlsandbox.Session{}, join(`shop."`+name+`"`)); err != nil || got != "paid/paid" || !age.Tailed {
			t.Fatalf("shop.%q named exactly: %q, read with its changes %v, %v", name, got, age.Tailed, err)
		}
	}

	spellings := []string{
		// 1. A letter outside ASCII, in another case, bare.
		"caf\u00e9", "CAF\u00c9", "Caf\u00c9",
		// 2. The same in quotes, in backticks, and with the accent as a
		// code point of its own.
		`"caf\u00e9"`, `"CAF\u00c9"`, "`caf\u00e9`", "`CAF\u00c9`", "cafe\u0301", `"cafe\u0301"`, "CAFE\u0301",
		// 3. ASCII, every quoting, mixed case.
		"orders", "ORDERS", "oRdErS", `"Orders"`, `"ORDERS"`, `"orders"`, "`Orders`", "`orders`", "[orders]", "[Orders]",
		// 4. A space or a quote inside the quotes.
		`"Orders "`, `" Orders"`, `"orders "`, `"Or""ders"`, `"Orders"""`,
		// A view whose name has a letter that folds into ASCII, named by
		// the ASCII letter: plain names, which the match here does not take
		// for the view.
		"kelvin", "KELVIN", "Kelvin", `"kelvin"`, "stats", "STATS", "Stats", `"STATS"`,
		// And each named as it is.
		"\u212aelvin", "\u017ftats",
	}
	for _, sess := range []struct {
		name string
		s    sqlsandbox.Session
	}{{"a MySQL session", sqlsandbox.Session{}}, {"a MySQL session that routes", sqlsandbox.Session{StrictStar: true}}, {"PostgreSQL's rules", sqlsandbox.Session{PostgresRules: true, PostgresNames: true}}} {
		for _, sp := range spellings {
			got, age, err := run(sess.s, join(sp))
			switch {
			case err != nil:
				t.Logf("%s, %s: not answered", sess.name, sp)
			case got == "paid/paid" && age.Tailed:
				t.Logf("%s, %s: both tables with their changes", sess.name, sp)
			case got == "new/new" && !age.Tailed:
				t.Logf("%s, %s: the snapshot, and the run says so", sess.name, sp)
			default:
				t.Errorf("%s, %s: answered %q, and the run says every table was read with its changes: %v", sess.name, sp, got, age.Tailed)
			}
		}
	}
}
