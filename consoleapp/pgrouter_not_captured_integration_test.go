//go:build integration

package consoleapp

import (
	"context"
	"database/sql"
	"errors"
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

const (
	pgLostA    = "pgnotcapt_a"
	pgLostB    = "pgnotcapt_b"
	pgLostSlot = "bintrail_pgnotcapt_slot"
	pgLostPub  = "bintrail_pgnotcapt_pub"
	pgLostRole = "dbtrail_fwd_notcapt"
	pgLostPass = "fwd-pw-notcapt"
)

// A table whose capture drops its changes, read through the PostgreSQL port
// by a NEW connection, in fresh mode and then in fast mode, against a real PostgreSQL 16 and a
// real index. Capture keeps running and keeps the other table, records the
// drops in its ledger, and the copy holds the dropped table as of its
// snapshot alone. Before, the port answered it from there: no answer of the
// source on the connection held it back, and the server has no lag limit.
// Now the source answers it, with its current rows, as table_not_captured.
//
// The drops are made with a deferrable primary key, which capture skips on
// every PostgreSQL version this branch captures; a table below REPLICA
// IDENTITY FULL is skipped the same way, under its own reason, once that
// rule is in. Nothing here depends on the reason.
func TestIntegrationPGRouterTableNotCapturedGoesToTheSource(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	t.Setenv("BINTRAIL_SQL_TAILS", "")
	t.Cleanup(func() { console.EnablePostgresTails(false) })

	baseDSN, source, exec1 := pgTestSource(t, ctx, pgLostA, pgLostSlot, pgLostPub, "id int PRIMARY KEY, counter int NOT NULL")
	dropB := func() { _, _ = source.Exec(context.Background(), "DROP TABLE IF EXISTS "+pgLostB).ReadAll() }
	dropB()
	t.Cleanup(dropB)
	for _, s := range []string{
		"CREATE TABLE " + pgLostB + " (id int PRIMARY KEY, counter int NOT NULL)",
		"ALTER TABLE " + pgLostB + " REPLICA IDENTITY FULL",
		"ALTER PUBLICATION " + pgLostPub + " ADD TABLE " + pgLostB,
		"INSERT INTO " + pgLostA + " VALUES (1, 10)",
		"INSERT INTO " + pgLostB + " VALUES (1, 20)",
	} {
		exec1(s)
	}
	exec1(`DO $$ BEGIN IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '` + pgLostRole + `') THEN EXECUTE 'DROP OWNED BY ` + pgLostRole + `'; END IF; END $$`)
	exec1("DROP ROLE IF EXISTS " + pgLostRole)
	exec1("CREATE ROLE " + pgLostRole + " LOGIN PASSWORD '" + pgLostPass + "'")
	exec1("GRANT SELECT ON " + pgLostA + ", " + pgLostB + " TO " + pgLostRole)
	t.Cleanup(func() {
		bg := context.Background()
		for _, s := range []string{"DROP OWNED BY " + pgLostRole, "DROP ROLE IF EXISTS " + pgLostRole} {
			_, _ = source.Exec(bg, s).ReadAll()
		}
	})

	mon, bootDSN := pgTestMonitor(t, ctx, 200*time.Millisecond)
	_, _, sup := newScheduleFixture(t, true)
	f := newRouterFiles(t)
	e, err := f.owner.Add(console.ServerEntry{
		Name: "pg-notcapt", Flavor: console.FlavorPostgres,
		SourceDSN: baseDSN, SourceSlot: pgLostSlot, SourcePublication: pgLostPub,
		Schemas: "public", BaselineDir: t.TempDir(),
		RouteDSN: withAccount(t, baseDSN, pgLostRole, pgLostPass),
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

	// After the snapshot, while capture runs: a's key made deferrable, which
	// capture skips from its next change on, and a change of each table.
	exec1("ALTER TABLE " + pgLostA + " DROP CONSTRAINT " + pgLostA + "_pkey, ADD PRIMARY KEY (id) DEFERRABLE INITIALLY IMMEDIATE")
	exec1("UPDATE " + pgLostA + " SET counter = 11 WHERE id = 1")
	exec1("UPDATE " + pgLostB + " SET counter = 21 WHERE id = 1")
	idx, err := sql.Open("mysql", e.DSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = idx.Close() })
	pgTestWait(t, "the drop of a's change in the ledger, b's change indexed", 90*time.Second, func() (bool, string) {
		var skips sql.NullString
		var bEvents int
		err := idx.QueryRowContext(ctx, `SELECT (SELECT capture_skips FROM stream_state WHERE id = 1),
			(SELECT COUNT(*) FROM binlog_events WHERE table_name = ?)`, pgLostB).Scan(&skips, &bEvents)
		if err != nil {
			return false, err.Error()
		}
		return strings.Contains(skips.String, "public."+pgLostA) && bEvents >= 1, fmt.Sprintf("skips=%q b_events=%d", skips.String, bEvents)
	})

	f.savePortFile(t, true, "tok")
	srv, runner, err := newPGRouterConsole(f.settings(""), console.SQLPool{Workers: 2, Threads: 2})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(runner.Close)
	srv.RefreshFollowedFiles()
	routes := newPGRouteStats(time.Now(), true)
	// Routing by plan off: every statement the screen lets through is for
	// the copy, as an expensive report is under the default thresholds.
	router := &pgRouterPort{srv: srv, addr: freeAddr(t), maxConns: 8, poll: 20 * time.Millisecond, out: io.Discard, routes: routes, cost: &pgshim.CostPolicy{}}
	portCtx, stopPort := context.WithCancel(context.Background())
	served := make(chan struct{})
	go func() { _ = router.serve(portCtx); close(served) }()
	defer func() { stopPort(); <-served }()
	waitUntil(t, "the port to open", func() bool { return router.listening() != "" })
	host, portNum, _ := net.SplitHostPort(router.listening())
	dsn := fmt.Sprintf("host=%s port=%s sslmode=disable user=anyone password=tok dbname=pg-notcapt", host, portNum)
	reasons := func() map[string]uint64 {
		out := map[string]uint64{}
		for k, v := range routes.doc("").Servers[e.ID].Reasons {
			out[k] = v
		}
		return out
	}
	// counted waits for the status to count reason once more than before.
	counted := func(what string, before map[string]uint64, reason string) {
		t.Helper()
		pgTestWait(t, what+": counted as "+reason, 5*time.Second, func() (bool, string) {
			after := reasons()
			return after[reason] == before[reason]+1, fmt.Sprintf("%v then %v", before, after)
		})
	}

	// oneOn runs stmt on a new connection, closed after it.
	oneOn := func(stmt string) string {
		t.Helper()
		c := pgClient(t, ctx, dsn, nil)
		defer c.Close(context.Background())
		return one(ctx, c, stmt)
	}

	// The captured table, in fresh mode: the copy, with its change, once its
	// tail is built.
	readB := "SELECT counter FROM " + pgLostB + " WHERE id = 1"
	// Waited for on the copy's own answer (pgAnsweredByCopy): the source
	// holds the change too.
	pgTestWait(t, "b's change from the copy", 90*time.Second, func() (bool, string) {
		got, byCopy := pgAnsweredByCopy(reasons, func() string { return oneOn(readB) })
		return got == "21" && byCopy, fmt.Sprintf("%s by the copy=%v", got, byCopy)
	})
	before := reasons()
	if got := oneOn(readB); got != "21" {
		t.Fatalf("b: %s", got)
	}
	counted("b", before, pgshim.ReasonAnswersTheSame)

	// The defect: a new connection reads the dropped table. The copy holds
	// 10; the source holds 11.
	readA := "SELECT counter FROM " + pgLostA + " WHERE id = 1"
	c := pgClient(t, ctx, dsn, nil)
	before = reasons()
	if got := one(ctx, c, readA); got != "11" {
		t.Fatalf("a new connection reading the table capture drops: %q, want the source's current 11 (10 is the copy's snapshot)", got)
	}
	counted("a", before, pgshim.ReasonTableNotCaptured)

	// A join of the dropped table with the captured one: the source.
	before = reasons()
	if got := oneOn("SELECT a.counter, b.counter FROM "+pgLostA+" a JOIN "+pgLostB+" b ON b.id = a.id"); got != "11|21" {
		t.Fatalf("the join: %q, want 11|21", got)
	}
	counted("the join", before, pgshim.ReasonTableNotCaptured)

	// A prepared statement (Parse, Bind, Execute): the source too.
	p := pgClient(t, ctx, dsn, nil)
	if _, err := p.Prepare(ctx, "readA", "SELECT counter FROM "+pgLostA+" WHERE id = $1", nil); err != nil {
		t.Fatal(err)
	}
	before = reasons()
	res := p.ExecPrepared(ctx, "readA", [][]byte{[]byte("1")}, nil, nil).Read()
	if res.Err != nil || len(res.Rows) != 1 || string(res.Rows[0][0]) != "11" {
		t.Fatalf("prepared: %v %q, want the source's 11", res.Err, res.Rows)
	}
	counted("prepared", before, pgshim.ReasonTableNotCaptured)

	// Fast mode: every table is read from its snapshot, and the copy's age
	// of a PostgreSQL server is not computed, so nothing else would hold the
	// dropped table back. It goes to the source all the same; the captured
	// one is the snapshot's, as fast mode answers it.
	fast := false
	if _, err := f.owner.SetSQLMode(e.ID, &fast, nil, nil); err != nil {
		t.Fatal(err)
	}
	srv.RefreshFollowedFiles()
	pgTestWait(t, "fast mode: b from the snapshot", 30*time.Second, func() (bool, string) {
		got := oneOn(readB)
		return got == "20", got
	})
	before = reasons()
	if got := oneOn(readA); got != "11" {
		t.Fatalf("fast mode, a new connection reading the table capture drops: %q, want the source's current 11", got)
	}
	counted("fast mode, a", before, pgshim.ReasonTableNotCaptured)

	// The connection's own status says so.
	_, rows, err := pgText(t, c, "SHOW dbtrail.router_status")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(fmt.Sprint(rows), "last_answer_reason "+pgshim.ReasonTableNotCaptured) {
		t.Errorf("SHOW dbtrail.router_status after the read of a: %v", rows)
	}
}

// Capture saves its state as soon as replication starts, so a quiet source
// right after a full read is answered by the copy. A capture that cannot
// save its state (here a trigger of the index refuses every write to
// stream_state, and the row is removed) keeps its ledger of drops in memory
// only. A snapshot just read is still answered by the copy;
// once it is older than the bound, the PostgreSQL port takes such a capture
// for one that cannot save, and the source answers the table with its
// current rows. Fast mode, where nothing else would hold the copy back.
func TestIntegrationPGRouterNoCaptureStateGoesToTheSource(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	t.Setenv("BINTRAIL_SQL_TAILS", "")
	t.Cleanup(func() { console.EnablePostgresTails(false) })
	const (
		table = "pgnostate_t"
		slot  = "bintrail_pgnostate_slot"
		pub   = "bintrail_pgnostate_pub"
		role  = "dbtrail_fwd_nostate"
		pass  = "fwd-pw-nostate"
	)
	baseDSN, source, exec1 := pgTestSource(t, ctx, table, slot, pub, "id int PRIMARY KEY, counter int NOT NULL")
	exec1("INSERT INTO " + table + " VALUES (1, 10)")
	exec1(`DO $$ BEGIN IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '` + role + `') THEN EXECUTE 'DROP OWNED BY ` + role + `'; END IF; END $$`)
	exec1("DROP ROLE IF EXISTS " + role)
	exec1("CREATE ROLE " + role + " LOGIN PASSWORD '" + pass + "'")
	exec1("GRANT SELECT ON " + table + " TO " + role)
	t.Cleanup(func() {
		bg := context.Background()
		for _, s := range []string{"DROP OWNED BY " + role, "DROP ROLE IF EXISTS " + role} {
			_, _ = source.Exec(bg, s).ReadAll()
		}
	})

	mon, bootDSN := pgTestMonitor(t, ctx, 200*time.Millisecond)
	_, _, sup := newScheduleFixture(t, true)
	f := newRouterFiles(t)
	e, err := f.owner.Add(console.ServerEntry{
		Name: "pg-nostate", Flavor: console.FlavorPostgres,
		SourceDSN: baseDSN, SourceSlot: slot, SourcePublication: pub,
		Schemas: "public", BaselineDir: t.TempDir(),
		RouteDSN: withAccount(t, baseDSN, role, pass),
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
	snapshotAt := time.Now()
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
	router := &pgRouterPort{srv: srv, addr: freeAddr(t), maxConns: 8, poll: 20 * time.Millisecond, out: io.Discard, routes: routes, cost: &pgshim.CostPolicy{}}
	portCtx, stopPort := context.WithCancel(context.Background())
	served := make(chan struct{})
	go func() { _ = router.serve(portCtx); close(served) }()
	defer func() { stopPort(); <-served }()
	waitUntil(t, "the port to open", func() bool { return router.listening() != "" })
	host, portNum, _ := net.SplitHostPort(router.listening())
	dsn := fmt.Sprintf("host=%s port=%s sslmode=disable user=anyone password=tok dbname=pg-nostate", host, portNum)
	oneOn := func(stmt string) string {
		t.Helper()
		c := pgClient(t, ctx, dsn, nil)
		defer c.Close(context.Background())
		return one(ctx, c, stmt)
	}
	reasonCount := func(reason string) uint64 { return routes.doc("").Servers[e.ID].Reasons[reason] }
	read := "SELECT counter FROM " + table + " WHERE id = 1"

	// Healthy capture on a quiet source: it saved its state as soon as
	// replication started (an empty record of drops), and the copy answers.
	idx, err := sql.Open("mysql", e.DSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = idx.Close() })
	// The full read can end before capture has started replicating, and the
	// row is written when it has: wait for the row, not for the read.
	pgTestWait(t, "capture's state after replication started", 30*time.Second, func() (bool, string) {
		var skips sql.NullString
		err := idx.QueryRowContext(ctx, "SELECT capture_skips FROM stream_state WHERE id = 1").Scan(&skips)
		if errors.Is(err, sql.ErrNoRows) {
			return false, "no row yet"
		}
		if err != nil {
			t.Fatalf("capture's state after replication started: %v", err)
		}
		return true, "saved"
	})
	if got := oneOn(read); got != "10" {
		t.Fatalf("a quiet source, capture healthy: %q, want the copy's 10", got)
	}
	if n := reasonCount(pgshim.ReasonAnswersTheSame); n != 1 {
		t.Errorf("answered by the copy %d times, want 1", n)
	}

	// From here capture cannot save: every write to its state is refused,
	// and the row it may have saved is gone.
	for _, s := range []string{
		"CREATE TRIGGER pgnostate_no_insert BEFORE INSERT ON stream_state FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'refused by the test'",
		"CREATE TRIGGER pgnostate_no_update BEFORE UPDATE ON stream_state FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'refused by the test'",
		"DELETE FROM stream_state",
	} {
		if _, err := idx.ExecContext(ctx, s); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}

	// Within the bound: the copy, as for a capture that has not saved yet.
	if got := oneOn(read); got != "10" {
		t.Fatalf("a snapshot just read, no state saved yet: %q, want the copy's 10", got)
	}
	if time.Since(snapshotAt) >= time.Minute {
		t.Fatalf("the young read came %s after the snapshot, past the bound: the test is too slow to show it", time.Since(snapshotAt))
	}
	if n := reasonCount(pgshim.ReasonAnswersTheSame); n != 2 {
		t.Errorf("answered by the copy %d times, want 2", n)
	}

	// Past the bound: the source, with the change capture could not save.
	exec1("UPDATE " + table + " SET counter = 13 WHERE id = 1")
	pgTestWait(t, "the source's 13 past the bound", 3*time.Minute, func() (bool, string) {
		got := oneOn(read)
		return got == "13", fmt.Sprintf("%s after %s", got, time.Since(snapshotAt).Round(time.Second))
	})
	if n := reasonCount(pgshim.ReasonTableNotCaptured); n == 0 {
		t.Errorf("table_not_captured counted %d times, want at least once", n)
	}
	if since := time.Since(snapshotAt); since < 55*time.Second {
		t.Errorf("the source answered %s after the snapshot, before the bound of a minute", since)
	}
}
