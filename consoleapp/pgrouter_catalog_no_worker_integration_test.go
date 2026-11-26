//go:build integration

package consoleapp

import (
	"context"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/pgshim"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// A statement's routing must not wait for a worker of the copy: with the one
// worker held by a long report and the connection's list of the copy's
// tables past its age, a read by key the source answers comes back at once.
func TestIntegrationPGRouterCheapLookupDoesNotWaitForAWorker(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	const tbl, slot, pub, role, pass = "pgnoworker_t", "bintrail_pgnoworker_slot", "bintrail_pgnoworker_pub", "dbtrail_fwd_noworker", "fwd-pw-noworker"
	baseDSN, source, exec1 := pgTestSource(t, ctx, tbl, slot, pub, "id int PRIMARY KEY, v int NOT NULL")
	exec1("INSERT INTO " + tbl + " SELECT g, g FROM generate_series(1, 5000) g")
	exec1("ANALYZE " + tbl)
	exec1(`DO $$ BEGIN IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '` + role + `') THEN EXECUTE 'DROP OWNED BY ` + role + `'; END IF; END $$`)
	exec1("DROP ROLE IF EXISTS " + role)
	exec1("CREATE ROLE " + role + " LOGIN PASSWORD '" + pass + "'")
	exec1("GRANT SELECT ON " + tbl + " TO " + role)
	t.Cleanup(func() {
		for _, s := range []string{"DROP OWNED BY " + role, "DROP ROLE IF EXISTS " + role} {
			_, _ = source.Exec(context.Background(), s).ReadAll()
		}
	})
	mon, bootDSN := pgTestMonitor(t, ctx, 200*time.Millisecond)
	_, _, sup := newScheduleFixture(t, true)
	f := newRouterFiles(t)
	e, err := f.owner.Add(console.ServerEntry{Name: "pg-noworker", Flavor: console.FlavorPostgres,
		SourceDSN: baseDSN, SourceSlot: slot, SourcePublication: pub, Schemas: "public", BaselineDir: t.TempDir(),
		RouteDSN: withAccount(t, baseDSN, role, pass)})
	if err != nil {
		t.Fatal(err)
	}
	if e.DSN, err = mon.DeriveIndexDSN(e.ID); err != nil {
		t.Fatal(err)
	}
	if err := f.owner.Update(e); err != nil {
		t.Fatal(err)
	}
	pgTestStart(t, ctx, mon, bootDSN, source, e)
	if err := sup.Trigger(console.BaselineRequestFor(e)); err != nil {
		t.Fatal(err)
	}
	pgTestWait(t, "the snapshot", 2*time.Minute, func() (bool, string) {
		st := sup.Status(e.ID)
		if st.State == "failed" {
			t.Fatalf("the snapshot failed: %s", st.LastError)
		}
		return st.State == "succeeded", st.State
	})
	f.savePortFile(t, true, "tok")
	srv, runner, err := newPGRouterConsole(f.settings(""), console.SQLPool{Workers: 1, Threads: 2})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(runner.Close)
	srv.RefreshFollowedFiles()
	cost := pgshim.DefaultCostPolicy()
	router := &pgRouterPort{srv: srv, addr: freeAddr(t), maxConns: 8, poll: 20 * time.Millisecond, out: io.Discard,
		routes: newPGRouteStats(time.Now(), true), cost: &cost, sources: newPGSourceSlots(4)}
	portCtx, stopPort := context.WithCancel(context.Background())
	served := make(chan struct{})
	go func() { _ = router.serve(portCtx); close(served) }()
	defer func() { stopPort(); <-served }()
	waitUntil(t, "the port", func() bool { return router.listening() != "" })
	host, portNum, _ := net.SplitHostPort(router.listening())
	dsn := fmt.Sprintf("host=%s port=%s sslmode=disable user=anyone password=tok dbname=pg-noworker", host, portNum)

	lookup := "SELECT v FROM " + tbl + " WHERE id = 7"
	c := pgClient(t, ctx, dsn, nil)
	defer c.Close(context.Background())
	if got := one(ctx, c, lookup); got != "7" {
		t.Fatalf("the lookup: %q", got)
	}
	// The report holds the one worker for far longer than this test.
	report := pgClient(t, ctx, dsn, nil)
	reportCtx, stopReport := context.WithCancel(ctx)
	reportDone := make(chan error, 1)
	go func() {
		_, err := report.Exec(reportCtx, "SELECT count(*) FROM "+tbl+" a, "+tbl+" b, "+tbl+" c, "+tbl+" d WHERE a.v + b.v + c.v + d.v = -1").ReadAll()
		reportDone <- err
	}()
	defer func() { stopReport(); <-reportDone; report.Close(context.Background()) }()
	// Past the age of the list c read (catalogCacheFor), the report running.
	time.Sleep(11 * time.Second)
	select {
	case err := <-reportDone:
		reportDone <- err
		t.Fatalf("the report ended before the lookup was timed: %v", err)
	default:
	}
	worst := timedLookup(t, ctx, c, lookup)
	t.Logf("the read by key, the worker held and the list past its age: %v", worst)
	select {
	case err := <-reportDone:
		reportDone <- err
		t.Fatalf("the report ended during the lookup: %v", err)
	default:
	}
	if worst > 2*time.Second {
		t.Errorf("the read by key waited %v behind the copy's worker", worst)
	}
}

func timedLookup(t *testing.T, ctx context.Context, c *pgconn.PgConn, stmt string) time.Duration {
	t.Helper()
	start := time.Now()
	if got := one(ctx, c, stmt); got != "7" {
		t.Errorf("the lookup answered %q after %v", got, time.Since(start))
	}
	return time.Since(start)
}
