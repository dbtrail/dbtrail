//go:build integration

package consoleapp

import (
	"context"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/pgshim"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

const (
	pgApartA    = "pgapart_a"
	pgApartB    = "pgapart_b"
	pgApartSlot = "bintrail_pgapart_slot"
	pgApartPub  = "bintrail_pgapart_pub"
	pgApartRole = "dbtrail_fwd_apart"
	pgApartPass = "fwd-pw-apart"
)

// #2985, against a real PostgreSQL and a real copy: after the source
// answered a connection about one table, a statement for the copy about
// another is answered by the copy, and one about the same table is not.
//
// The server is in fast mode, where the copy can never show it caught up
// with the source: a statement held back goes to the source (copy_too_old
// in the port's status) and one that is not is answered by the copy
// (answers_the_same), so the status says which of the two each was.
//
// It is the names' test: what PostgreSQL says a statement read (its own
// catalog's schema and table) must compare equal to what the copy's catalog
// calls the same table, in capitals, with a schema, through a WITH. And it
// is the test of what PostgreSQL refuses to confirm: a view, an inheritance
// parent, a partitioned table, row-level security, and code of the user's
// that runs where the text shows none.
func TestIntegrationPGRouterOnlyTheSourcesTablesAreHeldBack_2985(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	t.Setenv("BINTRAIL_SQL_TAILS", "")

	baseDSN, source, exec1 := pgTestSource(t, ctx, pgApartA, pgApartSlot, pgApartPub, "id int PRIMARY KEY, counter int NOT NULL")
	extras := []string{
		"DROP FUNCTION IF EXISTS pgapart_eq(" + pgApartA + ")",
		"DROP OPERATOR IF EXISTS === (int, text)",
		"DROP AGGREGATE IF EXISTS sum(text)",
		"DROP FUNCTION IF EXISTS pgapart_sf(int, text)",
		"DROP TYPE IF EXISTS pgapart_enum CASCADE",
		"DROP FUNCTION IF EXISTS pgapart_eq(int, text)",
		"DROP FUNCTION IF EXISTS abs(text)",
		"DROP DOMAIN IF EXISTS pgapart_dom",
		"DROP VIEW IF EXISTS pgapart_view",
		"DROP TABLE IF EXISTS pgapart_child",
		"DROP TABLE IF EXISTS pgapart_parent",
		"DROP TABLE IF EXISTS pgapart_part",
		"DROP TABLE IF EXISTS pgapart_rls",
		`DROP TABLE IF EXISTS "PgApart_Mixed"`,
		"DROP TABLE IF EXISTS " + pgApartB + " CASCADE",
	}
	dropExtras := func() {
		for _, s := range extras {
			_, _ = source.Exec(context.Background(), s).ReadAll()
		}
	}
	dropExtras()
	t.Cleanup(dropExtras)
	for _, s := range []string{
		"CREATE TABLE " + pgApartB + " (id int PRIMARY KEY, counter int NOT NULL)",
		"ALTER TABLE " + pgApartB + " REPLICA IDENTITY FULL",
		`CREATE TABLE "PgApart_Mixed" (id int PRIMARY KEY, counter int NOT NULL)`,
		`ALTER TABLE "PgApart_Mixed" REPLICA IDENTITY FULL`,
		"ALTER PUBLICATION " + pgApartPub + " ADD TABLE " + pgApartB + `, "PgApart_Mixed"`,
		"INSERT INTO " + pgApartA + " VALUES (1, 10)",
		"INSERT INTO " + pgApartB + " VALUES (1, 20)",
		`INSERT INTO "PgApart_Mixed" VALUES (1, 30)`,
		// On the source only: none is published, none is on the copy.
		"CREATE VIEW pgapart_view AS SELECT id, counter FROM " + pgApartB,
		"CREATE TABLE pgapart_parent (id int, counter int)",
		"CREATE TABLE pgapart_child () INHERITS (pgapart_parent)",
		"CREATE TABLE pgapart_part (id int, counter int) PARTITION BY RANGE (id)",
		"CREATE TABLE pgapart_rls (id int, counter int)",
		"ALTER TABLE pgapart_rls ENABLE ROW LEVEL SECURITY",
		"CREATE POLICY pgapart_all ON pgapart_rls USING (true)",
	} {
		exec1(s)
	}
	exec1(`DO $$ BEGIN IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '` + pgApartRole + `') THEN EXECUTE 'DROP OWNED BY ` + pgApartRole + `'; END IF; END $$`)
	exec1("DROP ROLE IF EXISTS " + pgApartRole)
	exec1("CREATE ROLE " + pgApartRole + " LOGIN PASSWORD '" + pgApartPass + "'")
	exec1("GRANT SELECT ON " + pgApartA + ", " + pgApartB + `, "PgApart_Mixed", pgapart_view, pgapart_parent, pgapart_child, pgapart_part, pgapart_rls TO ` + pgApartRole)
	t.Cleanup(func() {
		bg := context.Background()
		for _, s := range []string{"DROP OWNED BY " + pgApartRole, "DROP ROLE IF EXISTS " + pgApartRole} {
			_, _ = source.Exec(bg, s).ReadAll()
		}
	})

	mon, bootDSN := pgTestMonitor(t, ctx, 200*time.Millisecond)
	_, _, sup := newScheduleFixture(t, true)
	f := newRouterFiles(t)
	e, err := f.owner.Add(console.ServerEntry{
		Name: "pg-apart", Flavor: console.FlavorPostgres,
		SourceDSN: baseDSN, SourceSlot: pgApartSlot, SourcePublication: pgApartPub,
		Schemas: "public", BaselineDir: t.TempDir(),
		RouteDSN: withAccount(t, baseDSN, pgApartRole, pgApartPass),
	})
	if err != nil {
		t.Fatalf("add the server: %v", err)
	}
	if e.DSN, err = mon.DeriveIndexDSN(e.ID); err != nil {
		t.Fatal(err)
	}
	if err := f.owner.Update(e); err != nil {
		t.Fatal(err)
	}
	pgTestStart(t, ctx, mon, bootDSN, source, e)
	if err := sup.Trigger(console.BaselineRequestFor(e)); err != nil {
		t.Fatalf("start the full read: %v", err)
	}
	pgTestWait(t, "the snapshot", 2*time.Minute, func() (bool, string) {
		st := sup.Status(e.ID)
		if st.State == "failed" {
			t.Fatalf("the full read failed: %s", st.LastError)
		}
		return st.State == "succeeded", st.State
	})
	fast := false
	if _, err := f.owner.SetSQLMode(e.ID, &fast, nil, nil); err != nil {
		t.Fatal(err)
	}

	f.savePortFile(t, true, "tok")
	srv, runner, err := newPGRouterConsole(f.settings(""), console.SQLPool{Workers: 2, Threads: 2})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(runner.Close)
	srv.RefreshFollowedFiles()
	routes := newPGRouteStats(time.Now(), true)
	// Routing by plan off: the screen alone decides, so a read goes to the
	// copy and the same read with now() to the source.
	router := &pgRouterPort{srv: srv, addr: freeAddr(t), maxConns: 8, poll: 20 * time.Millisecond, out: io.Discard, routes: routes, cost: &pgshim.CostPolicy{}}
	portCtx, stopPort := context.WithCancel(context.Background())
	served := make(chan struct{})
	go func() { _ = router.serve(portCtx); close(served) }()
	defer func() { stopPort(); <-served }()
	waitUntil(t, "the port to open", func() bool { return router.listening() != "" })
	host, portNum, _ := net.SplitHostPort(router.listening())
	dsn := fmt.Sprintf("host=%s port=%s sslmode=disable user=anyone password=tok dbname=pg-apart", host, portNum)
	reasons := func() map[string]uint64 {
		out := map[string]uint64{}
		for k, v := range routes.doc("").Servers[e.ID].Reasons {
			out[k] = v
		}
		return out
	}

	// answeredBy runs stmt, a statement for the copy, and says who answered
	// it: "copy", or "held" when it was held back and the source answered.
	answeredBy := func(c *pgconn.PgConn, stmt string) string {
		t.Helper()
		before := reasons()
		got := one(ctx, c, stmt)
		for range 100 {
			after := reasons()
			switch {
			case after[pgshim.ReasonAnswersTheSame] == before[pgshim.ReasonAnswersTheSame]+1:
				return "copy"
			case after[pgshim.ReasonCopyTooOld] == before[pgshim.ReasonCopyTooOld]+1:
				return "held"
			}
			time.Sleep(20 * time.Millisecond)
		}
		t.Fatalf("%s: answered %q, and the status counted neither the copy nor copy_too_old: %v then %v", stmt, got, before, reasons())
		return ""
	}
	readA := "SELECT counter FROM " + pgApartA + " WHERE id = 1"
	readB := "SELECT counter FROM " + pgApartB + " WHERE id = 1"
	const bySource = " AND now() IS NOT NULL"
	fresh := func() *pgconn.PgConn {
		t.Helper()
		return pgClient(t, ctx, dsn, nil)
	}

	// The copy holds the three tables: a connection the source never
	// answered reads each from it.
	pgTestWait(t, "the copy to answer", 90*time.Second, func() (bool, string) {
		c := fresh()
		defer c.Close(context.Background())
		a, b := one(ctx, c, readA), one(ctx, c, readB)
		return a == "10" && b == "20", a + " " + b
	})
	c := fresh()
	for _, stmt := range []string{readA, readB, "SELECT counter FROM pgapart_mixed WHERE id = 1"} {
		if who := answeredBy(c, stmt); who != "copy" {
			t.Fatalf("before the source answered, %s: %s", stmt, who)
		}
	}
	c.Close(context.Background())

	// What the source answers about table a, written every way: table b is
	// then answered by the copy, and table a is held back.
	for _, tc := range []struct{ name, stmt string }{
		{"a bare name", readA + bySource},
		{"capitals", "SELECT counter FROM PGAPART_A WHERE id = 1" + bySource},
		{"quoted", `SELECT counter FROM "pgapart_a" WHERE id = 1` + bySource},
		{"with its schema", "SELECT counter FROM public.pgapart_a WHERE id = 1" + bySource},
		{"schema and table quoted", `SELECT counter FROM "public"."pgapart_a" x WHERE x.id = 1` + bySource},
		{"in a WITH", "WITH w AS (SELECT counter FROM pgapart_a WHERE id = 1) SELECT counter FROM w WHERE now() IS NOT NULL"},
		{"in a WITH of the table's own name", "WITH pgapart_a AS (SELECT counter FROM pgapart_a WHERE id = 1) SELECT counter FROM pgapart_a WHERE now() IS NOT NULL"},
		{"in a subquery", "SELECT (SELECT counter FROM pgapart_a WHERE id = 1) WHERE now() IS NOT NULL"},
		{"joined to itself", "SELECT x.counter FROM pgapart_a x JOIN pgapart_a y ON y.id = x.id WHERE x.id = 1" + bySource},
		{"with functions that read no table", "SELECT coalesce(max(counter), 0) FROM pgapart_a WHERE abs(id) = 1 AND length('x') = 1" + bySource},
	} {
		func() {
			c := fresh()
			defer c.Close(context.Background())
			if got := one(ctx, c, tc.stmt); got != "10" {
				t.Fatalf("%s: the source's answer: %q", tc.name, got)
			}
			if who := answeredBy(c, readB); who != "copy" {
				t.Errorf("%s: table b after the source answered about a: %s, want the copy", tc.name, who)
			}
			if who := answeredBy(c, readA); who != "held" {
				t.Errorf("%s: table a after the source answered about it: %s, want held back", tc.name, who)
			}
			// The source answered a again (copy_too_old): b is still apart.
			if who := answeredBy(c, readB); who != "copy" {
				t.Errorf("%s: table b after a was held back: %s, want the copy", tc.name, who)
			}
		}()
	}

	func() {
		c := fresh()
		defer c.Close(context.Background())
		for range 2 {
			res := c.ExecParams(ctx, "SELECT counter FROM pgapart_a WHERE id = $1"+bySource, [][]byte{[]byte("1")}, nil, nil, nil).Read()
			if res.Err != nil || len(res.Rows) != 1 || string(res.Rows[0][0]) != "10" {
				t.Fatalf("the extended protocol: the source's answer: %v %q", res.Err, res.Rows)
			}
			if who := answeredBy(c, readB); who != "copy" {
				t.Errorf("the extended protocol: table b: %s, want the copy", who)
			}
			if who := answeredBy(c, readA); who != "held" {
				t.Errorf("the extended protocol: table a: %s, want held back", who)
			}
		}
	}()

	func() {
		c := fresh()
		defer c.Close(context.Background())
		for _, stmt := range []string{"BEGIN", readA, "COMMIT"} {
			if got := one(ctx, c, stmt); strings.HasPrefix(got, "error") {
				t.Fatalf("%s: %s", stmt, got)
			}
		}
		if who := answeredBy(c, readB); who != "copy" {
			t.Errorf("table b after the block: %s, want the copy", who)
		}
		if who := answeredBy(c, readA); who != "held" {
			t.Errorf("table a after the block: %s, want held back", who)
		}
	}()

	// PostgreSQL tells "PgApart_Mixed" from pgapart_mixed; the copy does
	// not, and answers the second with the first's rows. After the source
	// answered about the first, the second is held back.
	func() {
		c := fresh()
		defer c.Close(context.Background())
		if got := one(ctx, c, `SELECT counter FROM "PgApart_Mixed" WHERE id = 1`); got != "30" {
			t.Fatalf("the source's answer: %q", got)
		}
		if who := answeredBy(c, readB); who != "copy" {
			t.Errorf("a table named in capitals: table b: %s, want the copy", who)
		}
		if who := answeredBy(c, "SELECT counter FROM pgapart_mixed WHERE id = 1"); who != "held" {
			t.Errorf("the copy's pgapart_mixed after the source answered about \"PgApart_Mixed\": %s, want held back", who)
		}
	}()

	// What PostgreSQL does not confirm: each of these, answered by the
	// source, marks every table, and table b is held back.
	everyTable := func(name, stmt, want string) {
		t.Helper()
		c := fresh()
		defer c.Close(context.Background())
		if got := one(ctx, c, stmt); got != want {
			t.Fatalf("%s: the source's answer: %q, want %q", name, got, want)
		}
		if who := answeredBy(c, readB); who != "held" {
			t.Errorf("%s: table b after it: %s, want held back", name, who)
		}
	}
	everyTable("a view, which reads table b", "SELECT counter FROM pgapart_view WHERE id = 1"+bySource, "20")
	everyTable("an inheritance parent", "SELECT count(*) FROM pgapart_parent WHERE now() IS NOT NULL", "0")
	everyTable("a partitioned table", "SELECT count(*) FROM pgapart_part WHERE now() IS NOT NULL", "0")
	everyTable("a table with row-level security", "SELECT count(*) FROM pgapart_rls WHERE now() IS NOT NULL", "0")
	everyTable("a function in FROM", "SELECT g FROM generate_series(1, 1) g WHERE now() IS NOT NULL", "1")
	everyTable("a built-in function that reads a table by its text",
		"SELECT length(query_to_xml('select counter from "+pgApartB+"', true, false, '')::text) > 0 FROM "+pgApartA+" WHERE id = 1", "t")

	// Code of the user's that runs where the text shows none: while it is
	// in the database nothing is confirmed, and every table is marked. Each
	// is made, seen to mark every table, and dropped.
	apartAgain := func(when string) {
		t.Helper()
		c := fresh()
		defer c.Close(context.Background())
		if got := one(ctx, c, readA+bySource); got != "10" {
			t.Fatalf("%s, the source's answer: %q", when, got)
		}
		if who := answeredBy(c, readB); who != "copy" {
			t.Fatalf("%s, table b after the source answered about a: %s, want the copy", when, who)
		}
	}
	for _, tc := range []struct {
		name         string
		make, unmake []string
		stmt         string
	}{
		{"an operator written in SQL",
			[]string{"CREATE FUNCTION pgapart_eq(int, text) RETURNS bool LANGUAGE sql AS 'SELECT EXISTS (SELECT 1 FROM " + pgApartB + ")'",
				"CREATE OPERATOR === (LEFTARG = int, RIGHTARG = text, FUNCTION = pgapart_eq)"},
			[]string{"DROP OPERATOR === (int, text)", "DROP FUNCTION pgapart_eq(int, text)"},
			readA + bySource},
		{"a domain with a constraint",
			[]string{"CREATE DOMAIN pgapart_dom AS int CHECK (VALUE > 0)"},
			[]string{"DROP DOMAIN pgapart_dom"},
			readA + bySource},
		{"a function of the user's named as a built-in one the statement calls",
			[]string{"CREATE FUNCTION abs(text) RETURNS int LANGUAGE sql AS 'SELECT counter FROM " + pgApartB + " LIMIT 1'"},
			[]string{"DROP FUNCTION abs(text)"},
			"SELECT counter FROM " + pgApartA + " WHERE abs(id) = 1" + bySource},
		{"a cast written in SQL",
			[]string{"CREATE TYPE pgapart_enum AS ENUM ('x')",
				"CREATE FUNCTION pgapart_c(pgapart_enum) RETURNS int LANGUAGE sql AS 'SELECT counter FROM " + pgApartB + " LIMIT 1'",
				"CREATE CAST (pgapart_enum AS int) WITH FUNCTION pgapart_c(pgapart_enum)"},
			[]string{"DROP TYPE pgapart_enum CASCADE"},
			readA + bySource},
		{"an aggregate of the user's named as a built-in one the statement calls",
			[]string{"CREATE FUNCTION pgapart_sf(int, text) RETURNS int LANGUAGE sql AS 'SELECT counter FROM " + pgApartB + " LIMIT 1'",
				"CREATE AGGREGATE sum(text) (SFUNC = pgapart_sf, STYPE = int)"},
			[]string{"DROP AGGREGATE sum(text)", "DROP FUNCTION pgapart_sf(int, text)"},
			"SELECT sum(counter) FROM " + pgApartA + " WHERE id = 1" + bySource},
		{"a function of one row, which a statement can call as a column",
			[]string{"CREATE FUNCTION pgapart_eq(" + pgApartA + ") RETURNS int LANGUAGE sql AS 'SELECT counter FROM " + pgApartB + " LIMIT 1'"},
			[]string{"DROP FUNCTION pgapart_eq(" + pgApartA + ")"},
			"SELECT x.counter FROM " + pgApartA + " x WHERE x.id = 1 AND x.pgapart_eq = 20" + bySource},
	} {
		apartAgain("before " + tc.name)
		for _, s := range tc.make {
			exec1(s)
		}
		everyTable(tc.name, tc.stmt, "10")
		for _, s := range tc.unmake {
			exec1(s)
		}
	}
	apartAgain("at the end")
}
