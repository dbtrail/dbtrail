//go:build integration

package consoleapp

import (
	"context"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/pgshim"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

const (
	pgAgeTable = "pgsnapage_orders"
	pgAgeSlot  = "bintrail_pgsnapage_slot"
	pgAgePub   = "bintrail_pgsnapage_pub"
	pgAgeRole  = "dbtrail_fwd_snapage"
	pgAgePass  = "fwd-pw-snapage"
)

// #3125, against a real PostgreSQL and a real copy: a server in fast mode
// with a lag limit. Every table read is answered from the snapshot alone
// there, and the snapshot's files say how far they reach, so the copy
// answers within the limit and the source answers past it. Before, the age
// of such an answer could never be told, and with any limit every table read
// went to the source (copy_age_unknown).
//
// The row is changed on the source after the snapshot, so the value says
// who answered: 10 is the copy's, 11 the source's.
func TestIntegrationPGRouterSnapshotAgeAndLagLimit_3125(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	t.Setenv("BINTRAIL_SQL_TAILS", "")

	baseDSN, source, exec1 := pgTestSource(t, ctx, pgAgeTable, pgAgeSlot, pgAgePub, "id int PRIMARY KEY, counter int NOT NULL")
	exec1("INSERT INTO " + pgAgeTable + " VALUES (1, 10)")
	exec1(`DO $$ BEGIN IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '` + pgAgeRole + `') THEN EXECUTE 'DROP OWNED BY ` + pgAgeRole + `'; END IF; END $$`)
	exec1("DROP ROLE IF EXISTS " + pgAgeRole)
	exec1("CREATE ROLE " + pgAgeRole + " LOGIN PASSWORD '" + pgAgePass + "'")
	exec1("GRANT SELECT ON " + pgAgeTable + " TO " + pgAgeRole)
	t.Cleanup(func() {
		bg := context.Background()
		for _, s := range []string{"DROP OWNED BY " + pgAgeRole, "DROP ROLE IF EXISTS " + pgAgeRole} {
			_, _ = source.Exec(bg, s).ReadAll()
		}
	})

	mon, bootDSN := pgTestMonitor(t, ctx, 200*time.Millisecond)
	_, _, sup := newScheduleFixture(t, true)
	f := newRouterFiles(t)
	e, err := f.owner.Add(console.ServerEntry{
		Name: "pg-snapage", Flavor: console.FlavorPostgres,
		SourceDSN: baseDSN, SourceSlot: pgAgeSlot, SourcePublication: pgAgePub,
		Schemas: "public", BaselineDir: t.TempDir(),
		RouteDSN: withAccount(t, baseDSN, pgAgeRole, pgAgePass),
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
	exec1("UPDATE " + pgAgeTable + " SET counter = 11 WHERE id = 1")
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
	routes := newPGRouteStats(time.Now(), false)
	// Routing by plan off: the screen alone decides, so a plain read is the
	// copy's unless the lag limit says otherwise.
	router := &pgRouterPort{srv: srv, addr: freeAddr(t), maxConns: 8, poll: 20 * time.Millisecond, out: io.Discard, routes: routes, cost: &pgshim.CostPolicy{}}
	portCtx, stopPort := context.WithCancel(context.Background())
	served := make(chan struct{})
	go func() { _ = router.serve(portCtx); close(served) }()
	defer func() { stopPort(); <-served }()
	waitUntil(t, "the port to open", func() bool { return router.listening() != "" })
	host, portNum, _ := net.SplitHostPort(router.listening())
	dsn := fmt.Sprintf("host=%s port=%s sslmode=disable user=anyone password=tok dbname=pg-snapage", host, portNum)
	read := "SELECT counter FROM " + pgAgeTable + " WHERE id = 1"
	// A new connection each time: one the source already answered about the
	// table is not given the copy's older answer (#2985), which is another
	// rule than the one under test.
	ask := func() string {
		t.Helper()
		c := pgClient(t, ctx, dsn, nil)
		defer c.Close(context.Background())
		return one(ctx, c, read)
	}
	limit := func(seconds int64) {
		t.Helper()
		if _, err := f.owner.SetSQLMode(e.ID, nil, nil, &seconds); err != nil {
			t.Fatal(err)
		}
		srv.RefreshFollowedFiles()
	}
	numbers := func() pgRouterServerNumbers { return routes.doc("").Servers[e.ID] }

	// No limit: the copy answers, as it always did.
	pgTestWait(t, "the copy to answer", 90*time.Second, func() (bool, string) {
		got := ask()
		return got == "10", got
	})

	// A limit the snapshot is well within. The full read was a moment ago,
	// and its bound sits a minute before it.
	limit(3600)
	before := numbers()
	if got := ask(); got != "10" {
		t.Errorf("fast mode with a limit of an hour: answered %s, want the copy's 10; reasons %v", got, numbers().Reasons)
	}
	after := numbers()
	if n := after.Reasons[pgshim.ReasonCopyAgeUnknown]; n != 0 {
		t.Errorf("copy_age_unknown counted %d times: the snapshot's age could not be told", n)
	}
	if after.Reasons[pgshim.ReasonAnswersTheSame] != before.Reasons[pgshim.ReasonAnswersTheSame]+1 || after.Source != before.Source {
		t.Errorf("the statement was not counted as the copy's: before %+v, after %+v", before.Reasons, after.Reasons)
	}
	if after.DataAsOf == "" || after.DataAgeSeconds == nil || *after.DataAgeSeconds < 60 || *after.DataAgeSeconds > 600 ||
		after.DataAgeUnknown != 0 || after.DataAgeUnknownReason != "" {
		t.Errorf("the status after a snapshot answer: as_of=%q age=%v unknown=%d why=%q, want an age from 60 to 600 s",
			after.DataAsOf, after.DataAgeSeconds, after.DataAgeUnknown, after.DataAgeUnknownReason)
	}

	// A limit the snapshot is past (it is at least a minute old by its own
	// bound): the source answers, and the reason is the lag, not that it
	// cannot be told.
	limit(30)
	before = numbers()
	if got := ask(); got != "11" {
		t.Errorf("a limit of 30 s: answered %s, want the source's 11", got)
	}
	after = numbers()
	if after.Reasons[pgshim.ReasonCopyBehindSource] != before.Reasons[pgshim.ReasonCopyBehindSource]+1 || after.Reasons[pgshim.ReasonCopyAgeUnknown] != 0 {
		t.Errorf("a limit of 30 s: reasons before %v, after %v, want one more copy_behind_source and no copy_age_unknown", before.Reasons, after.Reasons)
	}

	// The limit off again: the copy.
	limit(0)
	if got := ask(); got != "10" {
		t.Errorf("the limit off: answered %s, want the copy's 10", got)
	}
}
