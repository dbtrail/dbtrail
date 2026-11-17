//go:build integration

package consoleapp

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

const (
	pgFreshTable = "pgfresh_orders"
	pgFreshSlot  = "bintrail_pgfresh_slot"
	pgFreshPub   = "bintrail_pgfresh_pub"
)

// #2756 on the PostgreSQL port, end to end on a real PostgreSQL and a real
// MySQL index: the router's console (newPGRouterConsole, as the command
// builds it) answers a server in fresh mode with the changes captured after
// its snapshot, without a new snapshot, and in fast mode with the snapshot
// alone. BINTRAIL_SQL_TAILS=off puts it in fast mode whatever is saved.
// Nothing is stubbed: capture is the daemon's, the snapshot the daemon's
// full read, and the answers come over the port.
func TestIntegrationPGRouterFreshMode_2756(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	t.Setenv("BINTRAIL_SQL_TAILS", "")
	// The switch is the process's: set back for the tests after this one.
	t.Cleanup(func() { console.EnablePostgresTails(false) })

	baseDSN, source, exec1 := pgTestSource(t, ctx, pgFreshTable, pgFreshSlot, pgFreshPub, "id text PRIMARY KEY, status text")
	// Two keys that differ only in case: under PostgreSQL's rules they are
	// two rows, and a change of one must leave the other. A tail laid over
	// the copy by a comparison without case would take one for the other.
	exec1("INSERT INTO " + pgFreshTable + " VALUES ('abc', 'new'), ('ABC', 'paid')")

	mon, bootDSN := pgTestMonitor(t, ctx, 200*time.Millisecond)
	_, _, sup := newScheduleFixture(t, true)
	f := newRouterFiles(t)
	snapshots := t.TempDir()
	e, err := f.owner.Add(console.ServerEntry{
		Name: "pg-fresh", Flavor: console.FlavorPostgres,
		SourceDSN: baseDSN, SourceSlot: pgFreshSlot, SourcePublication: pgFreshPub,
		Schemas: "public", BaselineDir: snapshots,
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

	// Written in PostgreSQL after the snapshot, and indexed with the
	// capture's position saved past them: nothing but a tail shows them.
	exec1("UPDATE " + pgFreshTable + " SET status = 'shipped' WHERE id = 'abc'")
	exec1("INSERT INTO " + pgFreshTable + " VALUES ('x3', 'after the snapshot')")
	idx, err := sql.Open("mysql", e.DSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = idx.Close() })
	pgTestWait(t, "the changes indexed and checkpointed", 90*time.Second, func() (bool, string) {
		var events, covered int
		err := idx.QueryRowContext(ctx, `SELECT
			(SELECT COUNT(*) FROM binlog_events WHERE table_name = ?),
			COALESCE((SELECT binlog_position FROM stream_state WHERE id = 1) >= (SELECT MAX(end_pos) FROM binlog_events), 0)`,
			pgFreshTable).Scan(&events, &covered)
		if err != nil {
			return false, err.Error()
		}
		return events >= 2 && covered == 1, fmt.Sprintf("events=%d covered=%d", events, covered)
	})

	f.savePortFile(t, true, "tok")
	srv, runner, err := newPGRouterConsole(f.settings(""), console.SQLPool{Workers: 2, Threads: 2})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(runner.Close)
	srv.RefreshFollowedFiles()
	ages := newPGRouteStats(time.Now(), false)
	router := &pgRouterPort{srv: srv, addr: freeAddr(t), maxConns: 8, poll: 20 * time.Millisecond, out: io.Discard, routes: ages}
	portCtx, stopPort := context.WithCancel(context.Background())
	served := make(chan struct{})
	go func() { _ = router.serve(portCtx); close(served) }()
	defer func() { stopPort(); <-served }()
	waitUntil(t, "the port to open", func() bool { return router.listening() != "" })
	host, portNum, _ := net.SplitHostPort(router.listening())
	cfg, err := pgconn.ParseConfig(fmt.Sprintf("host=%s port=%s sslmode=disable", host, portNum))
	if err != nil {
		t.Fatal(err)
	}
	cfg.User, cfg.Password, cfg.Database = "anyone", "tok", "pg-fresh"
	var noticeMu sync.Mutex
	var notices []string
	cfg.OnNotice = func(_ *pgconn.PgConn, n *pgconn.Notice) {
		noticeMu.Lock()
		notices = append(notices, n.Message)
		noticeMu.Unlock()
	}
	takeNotices := func() []string {
		noticeMu.Lock()
		defer noticeMu.Unlock()
		out := notices
		notices = nil
		return out
	}
	port, err := pgconn.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("connect to the port: %v", err)
	}
	defer port.Close(context.Background())

	read := func() string {
		t.Helper()
		_, rows, err := pgText(t, port, "SELECT id, status FROM "+pgFreshTable+" ORDER BY id")
		if err != nil {
			t.Fatalf("port: %v", err)
		}
		return fmt.Sprint(rows)
	}
	const (
		snapshot = "[[ABC paid] [abc new]]"
		current  = "[[ABC paid] [abc shipped] [x3 after the snapshot]]"
	)
	mode := func(fresh bool) {
		t.Helper()
		if _, err := f.owner.SetSQLMode(e.ID, &fresh, nil, nil); err != nil {
			t.Fatal(err)
		}
		srv.RefreshFollowedFiles()
	}

	// Fast mode: the snapshot alone. It is also what proves the snapshot
	// was taken before the changes.
	mode(false)
	if got := read(); got != snapshot {
		t.Fatalf("fast mode: %s, want the snapshot %s", got, snapshot)
	}

	// Fresh mode: the changes show without a new snapshot. The first
	// statement starts the table's tail, so an answer or two may still be
	// the snapshot's.
	mode(true)
	var last string
	pgTestWait(t, "the changes after the snapshot in fresh mode", 90*time.Second, func() (bool, string) {
		last = read()
		return last == current, last
	})
	if snaps := pgUpdSnapshots(t, snapshots); len(snaps) != 1 {
		t.Fatalf("snapshots = %v: the answer must come from the tail, not a new snapshot", snaps)
	}

	// The status says what the port does, from the console of this process.
	status := newPGRouteStats(time.Now(), false)
	status.modes = srv
	says := func(mode, from string) {
		t.Helper()
		n, ok := status.doc("").Servers[e.ID]
		if !ok || n.SQLMode != mode || n.SQLModeFrom != from {
			t.Fatalf("status: listed=%v mode=%q from=%q, want %s from %s", ok, n.SQLMode, n.SQLModeFrom, mode, from)
		}
	}
	says("fresh", "saved")

	// The copy's age (#2756): the fresh answer read changes committed a
	// moment ago, by this machine's clock; the test's own source and
	// DBTrail share it.
	if n := ages.doc("").Servers[e.ID]; n.DataAsOf == "" || n.DataAgeSeconds == nil || *n.DataAgeSeconds > 600 || n.DataAgeUnknownReason != "" || n.DataAgeMaxSeconds == nil {
		t.Fatalf("the age after a fresh answer: as_of=%q age=%v max=%v why=%q", n.DataAsOf, n.DataAgeSeconds, n.DataAgeMaxSeconds, n.DataAgeUnknownReason)
	}

	// Back to fast: the snapshot again, at once, and this connection, which
	// was just shown the newer rows, is told so once.
	takeNotices()
	mode(false)
	says("fast", "saved")
	if got := read(); got != snapshot {
		t.Fatalf("fast mode again: %s, want the snapshot %s", got, snapshot)
	}
	if n := ages.doc("").Servers[e.ID]; n.DataAgeUnknownReason == "" || n.DataAgeSeconds != nil || n.DataAgeMaxSeconds == nil {
		t.Fatalf("the age after a fast answer: age=%v max=%v why=%q", n.DataAgeSeconds, n.DataAgeMaxSeconds, n.DataAgeUnknownReason)
	}
	if n := takeNotices(); len(n) != 1 || !strings.Contains(n[0], "can be older than an earlier answer of the copy on this connection") {
		t.Fatalf("notices after the switch to fast: %q, want the one that says the answer can be older", n)
	}
	if got := read(); got != snapshot {
		t.Fatalf("fast mode, the next statement: %s", got)
	}
	if _, _, err := pgText(t, port, "SELECT 1"); err != nil {
		t.Fatal(err)
	}
	if n := takeNotices(); len(n) != 0 {
		t.Fatalf("notices on the next statements: %q, want none (told once, and SELECT 1 reads no table)", n)
	}

	// Fresh saved, and BINTRAIL_SQL_TAILS=off where the router runs: fast.
	mode(true)
	pgTestWait(t, "fresh mode again", 90*time.Second, func() (bool, string) {
		last = read()
		return last == current, last
	})
	t.Setenv("BINTRAIL_SQL_TAILS", "off")
	srv.RefreshFollowedFiles()
	if got := read(); got != snapshot {
		t.Fatalf("BINTRAIL_SQL_TAILS=off with fresh saved: %s, want the snapshot %s", got, snapshot)
	}
	says("fast", "environment")
}
