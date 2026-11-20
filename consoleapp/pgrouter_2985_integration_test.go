//go:build integration

package consoleapp

import (
	"context"
	"fmt"
	"io"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/pgshim"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

const (
	pgMonoTable = "pgmono_counters"
	pgMonoSlot  = "bintrail_pgmono_slot"
	pgMonoPub   = "bintrail_pgmono_pub"
	pgMonoRole  = "dbtrail_fwd_mono"
	pgMonoPass  = "fwd-pw-mono"
)

// #2985, in fresh mode against a real PostgreSQL and its capture: a
// connection the source answered never reads an older counter from the copy
// afterwards, while the source keeps writing. What a quiet source does after
// that is logged: the copy's data reaches the last change captured, which no
// later answer of the source is before.
func TestIntegrationPGRouterNoReadGoesBack_2985(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	t.Setenv("BINTRAIL_SQL_TAILS", "")
	t.Cleanup(func() { console.EnablePostgresTails(false) })

	baseDSN, source, exec1 := pgTestSource(t, ctx, pgMonoTable, pgMonoSlot, pgMonoPub, "id int PRIMARY KEY, counter int NOT NULL")
	exec1("INSERT INTO " + pgMonoTable + " VALUES (1, 0)")
	exec1(`DO $$ BEGIN IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '` + pgMonoRole + `') THEN EXECUTE 'DROP OWNED BY ` + pgMonoRole + `'; END IF; END $$`)
	exec1("DROP ROLE IF EXISTS " + pgMonoRole)
	exec1("CREATE ROLE " + pgMonoRole + " LOGIN PASSWORD '" + pgMonoPass + "'")
	exec1("GRANT SELECT ON " + pgMonoTable + " TO " + pgMonoRole)
	t.Cleanup(func() {
		bg := context.Background()
		for _, s := range []string{"DROP OWNED BY " + pgMonoRole, "DROP ROLE IF EXISTS " + pgMonoRole} {
			_, _ = source.Exec(bg, s).ReadAll()
		}
	})

	mon, bootDSN := pgTestMonitor(t, ctx, 200*time.Millisecond)
	_, _, sup := newScheduleFixture(t, true)
	f := newRouterFiles(t)
	e, err := f.owner.Add(console.ServerEntry{
		Name: "pg-mono", Flavor: console.FlavorPostgres,
		SourceDSN: baseDSN, SourceSlot: pgMonoSlot, SourcePublication: pgMonoPub,
		Schemas: "public", BaselineDir: t.TempDir(),
		RouteDSN: withAccount(t, baseDSN, pgMonoRole, pgMonoPass),
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
	fresh := true
	if _, err := f.owner.SetSQLMode(e.ID, &fresh, nil, nil); err != nil {
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
	// Routing by plan off: the screen alone decides, so the counter's read
	// goes to the copy and its read with now() to the source. The rule under
	// test does not depend on the plan.
	router := &pgRouterPort{srv: srv, addr: freeAddr(t), maxConns: 8, poll: 20 * time.Millisecond, out: io.Discard, routes: routes, cost: &pgshim.CostPolicy{}}
	portCtx, stopPort := context.WithCancel(context.Background())
	served := make(chan struct{})
	go func() { _ = router.serve(portCtx); close(served) }()
	defer func() { stopPort(); <-served }()
	waitUntil(t, "the port to open", func() bool { return router.listening() != "" })
	host, portNum, _ := net.SplitHostPort(router.listening())
	dsn := fmt.Sprintf("host=%s port=%s sslmode=disable user=anyone password=tok dbname=pg-mono", host, portNum)
	reasons := func() map[string]uint64 { return routes.doc("").Servers[e.ID].Reasons }

	copyRead := "SELECT counter FROM " + pgMonoTable + " WHERE id = 1"
	sourceRead := "SELECT counter FROM " + pgMonoTable + " WHERE id = 1 AND now() IS NOT NULL"
	counter := func(c *pgconn.PgConn, stmt string) int {
		t.Helper()
		got := one(ctx, c, stmt)
		n, err := strconv.Atoi(got)
		if err != nil {
			t.Fatalf("%s: %q", stmt, got)
		}
		return n
	}

	// The copy's tail is up: a connection the source never answered reads a
	// change made after the snapshot from the copy.
	exec1("UPDATE " + pgMonoTable + " SET counter = 1 WHERE id = 1")
	pgTestWait(t, "the copy's tail to show the change", 90*time.Second, func() (bool, string) {
		c := pgClient(t, ctx, dsn, nil)
		defer c.Close(context.Background())
		n := counter(c, copyRead)
		return n == 1, strconv.Itoa(n)
	})

	c := pgClient(t, ctx, dsn, nil)
	if n := counter(c, copyRead); n != 1 {
		t.Fatalf("before the source answered: %d, want the copy's 1", n)
	}
	for i := 2; i <= 21; i++ {
		exec1(fmt.Sprintf("UPDATE %s SET counter = %d WHERE id = 1", pgMonoTable, i))
		if n := counter(c, sourceRead); n != i {
			t.Fatalf("the source: %d, want %d", n, i)
		}
		// The copy is a moment behind: whoever answers, never the older value.
		if n := counter(c, copyRead); n < i {
			t.Fatalf("after the source showed %d, the copy's read said %d: it went back", i, n)
		}
	}
	r := reasons()
	t.Logf("while the source writes: copy_too_old %d, answers_the_same %d", r[pgshim.ReasonCopyTooOld], r[pgshim.ReasonAnswersTheSame])
	if r[pgshim.ReasonCopyTooOld] == 0 {
		t.Logf("the copy kept up with every answer of the source; the rule was not needed this run")
	}

	// A quiet source: nothing written after the last change. Once capture
	// has it, the copy holds the source's state, but its data reaches the
	// last change's commit, which no later answer of the source is before.
	time.Sleep(3 * time.Second)
	if n := counter(c, sourceRead); n != 21 {
		t.Fatalf("the source, quiet: %d", n)
	}
	before := reasons()
	copied := false
	for range 10 {
		if n := counter(c, copyRead); n != 21 {
			t.Fatalf("a quiet source: %d, want 21", n)
		}
		if reasons()[pgshim.ReasonAnswersTheSame] > before[pgshim.ReasonAnswersTheSame] {
			copied = true
			break
		}
		time.Sleep(time.Second)
	}
	t.Logf("a quiet source, 10 s after the source's last answer: the copy answered this connection again: %v", copied)

	// A connection the source never answered reads the copy, fresh.
	d := pgClient(t, ctx, dsn, nil)
	pgTestWait(t, "another connection to read the copy's newest value", 60*time.Second, func() (bool, string) {
		n := counter(d, copyRead)
		return n == 21, strconv.Itoa(n)
	})
}
