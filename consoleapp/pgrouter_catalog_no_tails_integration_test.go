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

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/pgshim"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// Reading the copy's list of columns needs no table's recent changes: with
// an update of the tails held, a statement the plan sends to the copy and
// the typed screen sends to the source gets there at once.
func TestIntegrationPGRouterVetoedStatementDoesNotWaitForTheTails(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	t.Setenv("BINTRAIL_SQL_TAILS", "")
	t.Cleanup(func() { console.EnablePostgresTails(false) })
	const tbl, slot, pub, role, pass = "pgnotails_t", "bintrail_pgnotails_slot", "bintrail_pgnotails_pub", "dbtrail_fwd_notails", "fwd-pw-notails"
	baseDSN, source, exec1 := pgTestSource(t, ctx, tbl, slot, pub, "id int PRIMARY KEY, d date NOT NULL, v int NOT NULL")
	exec1("INSERT INTO " + tbl + " SELECT g, DATE '2026-01-01' + (g % 300), g FROM generate_series(1, 2000) g")
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
	e, err := f.owner.Add(console.ServerEntry{Name: "pg-notails", Flavor: console.FlavorPostgres,
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
	fresh := true
	if _, err := f.owner.SetSQLMode(e.ID, &fresh, nil, nil); err != nil {
		t.Fatal(err)
	}
	f.savePortFile(t, true, "tok")
	srv, runner, err := newPGRouterConsole(f.settings(""), console.SQLPool{Workers: 3, Threads: 2})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(runner.Close)
	srv.RefreshFollowedFiles()
	// Every plan is expensive here: each statement heads for the copy.
	cost := pgshim.CostPolicy{CostThreshold: 1}
	router := &pgRouterPort{srv: srv, addr: freeAddr(t), maxConns: 8, poll: 20 * time.Millisecond, out: io.Discard,
		routes: newPGRouteStats(time.Now(), true), cost: &cost, sources: newPGSourceSlots(4)}
	portCtx, stopPort := context.WithCancel(context.Background())
	served := make(chan struct{})
	go func() { _ = router.serve(portCtx); close(served) }()
	defer func() { stopPort(); <-served }()
	waitUntil(t, "the port", func() bool { return router.listening() != "" })
	host, portNum, _ := net.SplitHostPort(router.listening())
	dsn := fmt.Sprintf("host=%s port=%s sslmode=disable user=anyone password=tok dbname=pg-notails", host, portNum)

	// The table's tail is up: a change made after the snapshot is read.
	exec1("UPDATE " + tbl + " SET v = -1 WHERE id = 1")
	pgTestWait(t, "the tail to show the change", 60*time.Second, func() (bool, string) {
		c := pgClient(t, ctx, dsn, nil)
		defer c.Close(context.Background())
		got := one(ctx, c, "SELECT v FROM "+tbl+" WHERE id = 1")
		return got == "-1", got
	})

	// An update of the tails running: the keeper's lock is in its hands.
	release, ok := srv.HoldTailKeeperForTest(e.ID)
	if !ok {
		t.Fatal("the server has no tail keeper")
	}
	defer release()
	held := time.AfterFunc(20*time.Second, release)
	defer held.Stop()

	// date_trunc of a date column: the plan sends it to the copy, the typed
	// screen to the source.
	c := pgClient(t, ctx, dsn, nil)
	defer c.Close(context.Background())
	begin := time.Now()
	got := one(ctx, c, "SELECT count(*) FROM (SELECT date_trunc('month', d) FROM "+tbl+") x")
	took := time.Since(begin)
	t.Logf("the vetoed statement, a tail update held: %v, answered %q", took, got)
	if got != "2000" {
		t.Errorf("answered %q", got)
	}
	if took > 2*time.Second {
		t.Errorf("the vetoed statement waited %v behind the update of the tails", took)
	}
	if strings.HasPrefix(got, "error") {
		t.Errorf("%s", got)
	}
	// The check of a SET reads no table either.
	begin = time.Now()
	if got := one(ctx, c, "SET TimeZone = 'Asia/Tokyo'"); strings.HasPrefix(got, "error") {
		t.Errorf("the SET: %s", got)
	}
	if took := time.Since(begin); took > 2*time.Second {
		t.Errorf("a SET waited %v behind the update of the tails", took)
	}
}
