//go:build integration

package consoleapp

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/pgshim"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// A capture that saved its state and then stops saving it (#3202), against a
// real PostgreSQL 16 and a real index, in fast mode, where nothing else would
// hold the copy back. Capture is stopped after it saved (its last save on the
// way out), and its row is then set as a capture that cannot save leaves it:
// the same position, saved long ago by the index's clock.
//
//   - The source wrote nothing past the saved position: the copy answers.
//   - The source writes: the source answers, under capture_state_stale, over
//     the simple and the extended protocol, and /status says why.
//   - Capture saves again: the copy answers again, by itself.
func TestIntegrationPGRouterCaptureStateStaleGoesToTheSource(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	t.Setenv("BINTRAIL_SQL_TAILS", "")
	t.Cleanup(func() { console.EnablePostgresTails(false) })
	bound := pgCaptureStaleBound
	pgCaptureStaleBound = 2 * time.Second
	t.Cleanup(func() { pgCaptureStaleBound = bound })
	const (
		table = "pgstale_t"
		slot  = "bintrail_pgstale_slot"
		pub   = "bintrail_pgstale_pub"
		role  = "dbtrail_fwd_pgstale"
		pass  = "fwd-pw-pgstale"
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
		Name: "pg-stale", Flavor: console.FlavorPostgres,
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
	fast := false
	if _, err := f.owner.SetSQLMode(e.ID, &fast, nil, nil); err != nil {
		t.Fatal(err)
	}
	idx, err := sql.Open("mysql", e.DSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = idx.Close() })
	// A save with a trusted sent-through position, then capture stopped: no
	// save from here on but the test's. A first run saves a position once a
	// transaction ends: one that changes nothing the copy shows.
	exec1("UPDATE " + table + " SET counter = 10 WHERE id = 1")
	pgTestWait(t, "a checkpoint with its sent-through position", time.Minute, func() (bool, string) {
		var sent sql.NullInt64
		err := idx.QueryRowContext(ctx, "SELECT IF(pg_sent_through_at <=> last_checkpoint, pg_sent_through_lsn, NULL) FROM stream_state WHERE id = 1 AND binlog_position > 0").Scan(&sent)
		if err != nil {
			return false, err.Error()
		}
		return sent.Valid && sent.Int64 > 0, fmt.Sprint(sent)
	})
	if err := mon.Stop(ctx, e.ID); err != nil {
		t.Fatal(err)
	}
	pgTestWait(t, "capture stopped", 30*time.Second, func() (bool, string) {
		st := mon.Status(e.ID)
		return st.State == "stopped", st.State
	})
	// sourceLSN is where the source's WAL is flushed now.
	sourceLSN := func() uint64 {
		t.Helper()
		_, rows, err := pgText(t, source, "SELECT pg_current_wal_flush_lsn()::text")
		if err != nil || len(rows) != 1 {
			t.Fatalf("the source's position: %v %v", err, rows)
		}
		lsn, err := pglogrepl.ParseLSN(rows[0][0])
		if err != nil {
			t.Fatal(err)
		}
		return uint64(lsn)
	}
	// saved sets the row as a capture leaves it: position sent, saved ago
	// seconds before the index's now (last_checkpoint and the stamp the
	// position is trusted by are one value).
	saved := func(sent uint64, ago int) {
		t.Helper()
		if _, err := idx.ExecContext(ctx, `UPDATE stream_state SET pg_sent_through_lsn = ?,
			last_checkpoint = UTC_TIMESTAMP() - INTERVAL ? SECOND, pg_sent_through_at = UTC_TIMESTAMP() - INTERVAL ? SECOND WHERE id = 1`, sent, ago, ago); err != nil {
			t.Fatal(err)
		}
		// The port keeps a verdict for a second.
		time.Sleep(pgCaptureStateTTL + 100*time.Millisecond)
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
	dsn := fmt.Sprintf("host=%s port=%s sslmode=disable user=anyone password=tok dbname=pg-stale", host, portNum)
	oneOn := func(stmt string) string {
		t.Helper()
		c := pgClient(t, ctx, dsn, nil)
		defer c.Close(context.Background())
		return one(ctx, c, stmt)
	}
	reasonCount := func(reason string) uint64 { return routes.doc("").Servers[e.ID].Reasons[reason] }
	read := "SELECT counter FROM " + table + " WHERE id = 1"

	// Saves stopped long ago, the source wrote nothing past the saved
	// position (it is ahead of the source's, so that the cluster's own
	// housekeeping cannot pass it during the test): the copy.
	saved(sourceLSN()+1<<24, 600)
	before := reasonCount(pgshim.ReasonAnswersTheSame)
	if got := oneOn(read); got != "10" {
		t.Fatalf("saves stopped on a quiet source: %q, want the copy's 10", got)
	}
	if n := reasonCount(pgshim.ReasonAnswersTheSame); n != before+1 {
		t.Errorf("answered by the copy %d more times, want 1", n-before)
	}
	if d := routes.doc("").Servers[e.ID].CaptureState; d != nil {
		t.Errorf("/status on a quiet source: %+v", d)
	}

	// The source writes past the saved position: the source answers, with
	// its current rows.
	saved(sourceLSN(), 600)
	exec1("UPDATE " + table + " SET counter = 13 WHERE id = 1")
	time.Sleep(pgCaptureStateTTL + 100*time.Millisecond)
	before = reasonCount(pgshim.ReasonCaptureStateStale)
	if got := oneOn(read); got != "13" {
		t.Fatalf("saves stopped while the source wrote: %q, want the source's 13 (10 is the copy's snapshot)", got)
	}
	p := pgClient(t, ctx, dsn, nil)
	if _, err := p.Prepare(ctx, "readT", "SELECT counter FROM "+table+" WHERE id = $1", nil); err != nil {
		t.Fatal(err)
	}
	res := p.ExecPrepared(ctx, "readT", [][]byte{[]byte("1")}, nil, nil).Read()
	if res.Err != nil || len(res.Rows) != 1 || string(res.Rows[0][0]) != "13" {
		t.Fatalf("prepared, saves stopped while the source wrote: %v %q, want the source's 13", res.Err, res.Rows)
	}
	p.Close(ctx)
	if n := reasonCount(pgshim.ReasonCaptureStateStale); n != before+2 {
		t.Errorf("capture_state_stale counted %d more times, want 2", n-before)
	}
	if d := routes.doc("").Servers[e.ID].CaptureState; d == nil || d.State != pgshim.CaptureStale || !strings.Contains(d.Why, "seconds while your PostgreSQL wrote") {
		t.Errorf("/status while stale: %+v", d)
	}

	// Capture saves again (the same position, saved now): the copy, by
	// itself.
	saved(sourceLSN()-1, 0)
	before = reasonCount(pgshim.ReasonAnswersTheSame)
	if got := oneOn(read); got != "10" {
		t.Fatalf("capture saved again: %q, want the copy's 10", got)
	}
	if n := reasonCount(pgshim.ReasonAnswersTheSame); n != before+1 {
		t.Errorf("answered by the copy %d more times after capture saved again, want 1", n-before)
	}
	if d := routes.doc("").Servers[e.ID].CaptureState; d != nil {
		t.Errorf("/status after capture saved again: %+v", d)
	}
}
