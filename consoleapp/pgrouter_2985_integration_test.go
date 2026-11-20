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
// afterwards, while the source keeps writing, and the copy, which waits for
// what the source showed, answers most of those reads. How many it answers
// on a quiet source is logged.
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
	// Constant writes: the counter goes up every 20 ms on the source while
	// the connection reads it from the source and then from the copy, a
	// hundred times. Every read is at least the last one; the copy waits
	// for what the source showed and answers most of them.
	wctx, stopWriter := context.WithCancel(ctx)
	writer := pgClient(t, ctx, baseDSN, nil)
	wrote := make(chan struct{})
	go func() {
		defer close(wrote)
		for wctx.Err() == nil {
			if _, err := writer.Exec(wctx, "UPDATE "+pgMonoTable+" SET counter = counter + 1 WHERE id = 1").ReadAll(); err != nil && wctx.Err() == nil {
				t.Errorf("the writer: %v", err)
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
	}()
	before := reasons()
	last := 0
	const pairs = 100
	for i := range pairs {
		s := counter(c, sourceRead)
		if s < last {
			t.Fatalf("pair %d: the source said %d after %d", i, s, last)
		}
		n := counter(c, copyRead)
		if n < s {
			t.Fatalf("pair %d: after the source showed %d, the read for the copy said %d: it went back", i, s, n)
		}
		last = n
	}
	stopWriter()
	<-wrote
	after := reasons()
	byCopy := after[pgshim.ReasonAnswersTheSame] - before[pgshim.ReasonAnswersTheSame]
	tooOld := after[pgshim.ReasonCopyTooOld] - before[pgshim.ReasonCopyTooOld]
	t.Logf("constant writes: of %d reads for the copy after the source answered, the copy answered %d (%.0f%%), the source %d (copy_too_old)",
		pairs, byCopy, 100*float64(byCopy)/pairs, tooOld)
	if byCopy*2 < pairs {
		t.Errorf("the copy answered %d of %d reads for it: most should wait for it and be answered there", byCopy, pairs)
	}

	// A quiet source: nothing written. The copy waits for capture's saved
	// position, which on a quiet source reaches the source's through the
	// walsender's own position, and answers.
	time.Sleep(3 * time.Second)
	final := counter(c, sourceRead)
	quietBefore := reasons()
	copied := 0
	for range 10 {
		if n := counter(c, copyRead); n != final {
			t.Fatalf("a quiet source: %d, want %d", n, final)
		}
		_ = counter(c, sourceRead)
	}
	copied = int(reasons()[pgshim.ReasonAnswersTheSame] - quietBefore[pgshim.ReasonAnswersTheSame])
	t.Logf("a quiet source: the copy answered %d of 10 reads for it after the source answered", copied)

	// A connection the source never answered reads the copy, fresh.
	d := pgClient(t, ctx, dsn, nil)
	pgTestWait(t, "another connection to read the copy's newest value", 60*time.Second, func() (bool, string) {
		n := counter(d, copyRead)
		return n == final, strconv.Itoa(n)
	})
}
