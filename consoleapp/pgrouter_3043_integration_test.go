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

const (
	pgChainTable = "pgchain_orders"
	pgChainSlot  = "bintrail_pgchain_slot"
	pgChainPub   = "bintrail_pgchain_pub"
	pgChainRole  = "dbtrail_fwd_chain"
	pgChainPass  = "fwd-pw-chain"
)

// #3043 through the PostgreSQL port, in fresh mode against a real PostgreSQL
// and its capture, on a connection that forwards: a statement in which one
// WITH reads another reads the table's recent changes, so after the source
// answered the connection about that table the copy still answers it. It
// used to read the snapshot alone, which the port cannot show to be as new
// as what the source showed, so every such statement went to the source
// (copy_too_old in the port's status).
func TestIntegrationPGRouterWithReadingAWithIsAnsweredByTheCopy_3043(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	t.Setenv("BINTRAIL_SQL_TAILS", "")
	t.Cleanup(func() { console.EnablePostgresTails(false) })

	baseDSN, source, exec1 := pgTestSource(t, ctx, pgChainTable, pgChainSlot, pgChainPub, "id int PRIMARY KEY, counter int NOT NULL")
	exec1("INSERT INTO " + pgChainTable + " VALUES (1, 0)")
	exec1(`DO $$ BEGIN IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '` + pgChainRole + `') THEN EXECUTE 'DROP OWNED BY ` + pgChainRole + `'; END IF; END $$`)
	exec1("DROP ROLE IF EXISTS " + pgChainRole)
	exec1("CREATE ROLE " + pgChainRole + " LOGIN PASSWORD '" + pgChainPass + "'")
	exec1("GRANT SELECT ON " + pgChainTable + " TO " + pgChainRole)
	t.Cleanup(func() {
		bg := context.Background()
		for _, s := range []string{"DROP OWNED BY " + pgChainRole, "DROP ROLE IF EXISTS " + pgChainRole} {
			_, _ = source.Exec(bg, s).ReadAll()
		}
	})

	mon, bootDSN := pgTestMonitor(t, ctx, 200*time.Millisecond)
	_, _, sup := newScheduleFixture(t, true)
	f := newRouterFiles(t)
	e, err := f.owner.Add(console.ServerEntry{
		Name: "pg-chain", Flavor: console.FlavorPostgres,
		SourceDSN: baseDSN, SourceSlot: pgChainSlot, SourcePublication: pgChainPub,
		Schemas: "public", BaselineDir: t.TempDir(),
		RouteDSN: withAccount(t, baseDSN, pgChainRole, pgChainPass),
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
	// Routing by plan off: the screen alone decides, so a read goes to the
	// copy and the same read with now() to the source.
	router := &pgRouterPort{srv: srv, addr: freeAddr(t), maxConns: 8, poll: 20 * time.Millisecond, out: io.Discard, routes: routes, cost: &pgshim.CostPolicy{}}
	portCtx, stopPort := context.WithCancel(context.Background())
	served := make(chan struct{})
	go func() { _ = router.serve(portCtx); close(served) }()
	defer func() { stopPort(); <-served }()
	waitUntil(t, "the port to open", func() bool { return router.listening() != "" })
	host, portNum, _ := net.SplitHostPort(router.listening())
	dsn := fmt.Sprintf("host=%s port=%s sslmode=disable user=anyone password=tok dbname=pg-chain", host, portNum)
	reasons := func() map[string]uint64 { return routes.doc("").Servers[e.ID].Reasons }

	// answeredBy runs stmt, a statement for the copy, and returns its answer
	// and who gave it: "copy", or "held" when it was held back and the
	// source answered (copy_too_old).
	answeredBy := func(c *pgconn.PgConn, stmt string) (string, string) {
		t.Helper()
		before := map[string]uint64{}
		for k, v := range reasons() {
			before[k] = v
		}
		got := one(ctx, c, stmt)
		for range 100 {
			after := reasons()
			switch {
			case after[pgshim.ReasonAnswersTheSame] == before[pgshim.ReasonAnswersTheSame]+1:
				return got, "copy"
			case after[pgshim.ReasonCopyTooOld] == before[pgshim.ReasonCopyTooOld]+1:
				return got, "held"
			}
			time.Sleep(20 * time.Millisecond)
		}
		t.Fatalf("%s: answered %q, and the status counted neither the copy nor copy_too_old: %v then %v", stmt, got, before, reasons())
		return "", ""
	}
	plain := "SELECT counter FROM " + pgChainTable + " WHERE id = 1"
	bySource := plain + " AND now() IS NOT NULL"
	chained := []struct{ name, stmt, want string }{
		{"a WITH reading a WITH", "WITH x AS (SELECT id, counter FROM " + pgChainTable + "), y AS (SELECT id, counter FROM x) SELECT counter FROM y WHERE id = 1", "1"},
		{"the statement of the issue", "WITH x AS (SELECT * FROM " + pgChainTable + "), y AS (SELECT x.* FROM x) SELECT count(*) FROM y", "2"},
		{"three levels", "WITH x AS (SELECT id, counter FROM " + pgChainTable + "), y AS (SELECT id, counter FROM x), z AS (SELECT counter FROM y WHERE id = 1) SELECT counter FROM z", "1"},
	}

	// Two changes after the snapshot: row 1's counter, and a second row.
	exec1("UPDATE " + pgChainTable + " SET counter = 1 WHERE id = 1")
	exec1("INSERT INTO " + pgChainTable + " VALUES (2, 0)")
	// Waited for on the copy's own answers: since #3116 a light statement
	// whose tails are not ready yet is answered by the source, with the
	// changes in it, so the answer alone no longer says the copy has them.
	pgTestWait(t, "the copy's tail to show the changes", 90*time.Second, func() (bool, string) {
		c := pgClient(t, ctx, dsn, nil)
		defer c.Close(context.Background())
		counter, byA := answeredBy(c, plain)
		rows, byB := answeredBy(c, "SELECT count(*) FROM "+pgChainTable)
		return counter == "1" && rows == "2" && byA == "copy" && byB == "copy", counter + " by " + byA + ", " + rows + " by " + byB
	})

	// A connection the source never answered: the copy answers, with the
	// changes made after the snapshot.
	c := pgClient(t, ctx, dsn, nil)
	for _, tc := range chained {
		if got, who := answeredBy(c, tc.stmt); got != tc.want || who != "copy" {
			t.Errorf("%s, before the source answered: %q by %s, want %q by the copy", tc.name, got, who, tc.want)
		}
	}

	// The source answers the connection about the table. On a quiet source
	// the copy catches up with what it showed, and answers the statements
	// that read the table's recent changes: the plain read, and the chained
	// ones like it.
	if got := one(ctx, c, bySource); got != "1" {
		t.Fatalf("the source's answer: %q", got)
	}
	pgTestWait(t, "the copy to answer the plain read after the source answered", 60*time.Second, func() (bool, string) {
		got, who := answeredBy(c, plain)
		return got == "1" && who == "copy", got + " by " + who
	})
	for _, tc := range chained {
		held := 0
		for range 5 {
			got, who := answeredBy(c, tc.stmt)
			if got != tc.want {
				t.Fatalf("%s, after the source answered: %q, want %q", tc.name, got, tc.want)
			}
			if who == "held" {
				held++
			}
		}
		t.Logf("%s: after the source answered, the copy answered %d of 5", tc.name, 5-held)
		if held > 0 {
			t.Errorf("%s: after the source answered about the table, %d of 5 were sent to the source (copy_too_old); the plain read of the same table is answered by the copy", tc.name, held)
		}
	}
}
