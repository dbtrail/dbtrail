//go:build integration

package consoleapp

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/ext"
	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/pgstreamrun"
	"github.com/dbtrail/dbtrail/internal/streamrun"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// probeSourceJob registers an ext source job that captures the SourceJobInfo and
// the bound context of every firing whose IndexDSN contains sentinel — the gate
// keeps a Start elsewhere in the suite from misattributing a firing to this
// test. The ext registry is process-global with no public deregister, so
// ext.ResetForTest clears it both before registering (no prior bleed) and in
// t.Cleanup (no bleed into later tests). Buffered so a fired job never blocks on
// its own goroutine.
func probeSourceJob(t *testing.T, sentinel string) (<-chan ext.SourceJobInfo, <-chan context.Context) {
	t.Helper()
	ext.ResetForTest()
	t.Cleanup(ext.ResetForTest)
	infoCh := make(chan ext.SourceJobInfo, 4)
	ctxCh := make(chan context.Context, 4)
	ext.RegisterSourceJob(func(jobCtx context.Context, src ext.SourceJobInfo) {
		if !strings.Contains(src.IndexDSN, sentinel) {
			return
		}
		infoCh <- src
		ctxCh <- jobCtx
	})
	return infoCh, ctxCh
}

// TestIntegrationMonitorRunsExtSourceJob proves the monitor supervisor runs
// registered ext source jobs alongside a supervised stream, bound to the
// per-source lifecycle context. Start reaches the ext wiring (monitor.go, after
// index-DB provisioning + the advisory lock) only against real MySQL, but the
// stream itself is stubbed to block on its context, so no reachable source is
// needed. Asserts: (a) the job fires ONCE with the entry's DSNs and resolved
// flavor, (c) an idempotent Start for an already-running entry does not fire a
// duplicate, and (b) THE LEAK CONTRACT — after Stop, the job's bound context is
// Done, proving the job is torn down with the source and not merely with the
// outer daemon context.
func TestIntegrationMonitorRunsExtSourceJob(t *testing.T) {
	testutil.SkipIfNoMySQL(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, bootName := testutil.CreateTestDB(t)
	bootDSN := testutil.IntegrationDSN(bootName)

	sup := newMonitorSupervisor(ctx, bootDSN, nil, 0)
	// Block until the job's lifecycle context is cancelled: this lets Start reach
	// the ext wiring with only provisioning + the advisory lock done (real
	// MySQL), never a reachable source, and keeps the stored state at "pending"
	// so the idempotency assertion below has a running entry to collide with.
	// The stub reports a resolved flavor the way the real stream does after
	// asking the source: the MySQL-family jobs start from that hook.
	sup.streamFn = func(c context.Context, cfg streamrun.Config) error {
		cfg.Hooks.OnFlavorResolved(console.FlavorMySQL)
		<-c.Done()
		return c.Err()
	}

	sentinel := fmt.Sprintf("extjob%d", time.Now().UnixNano()%1e9)
	infoCh, ctxCh := probeSourceJob(t, sentinel)

	entry := console.ServerEntry{
		ID:        sentinel,
		Name:      "ext-source-job",
		SourceDSN: testutil.BaseDSN() + "/",
	}
	derived, err := sup.DeriveIndexDSN(entry.ID)
	if err != nil {
		t.Fatal(err)
	}
	entry.DSN = derived
	t.Cleanup(func() {
		if db, err := sql.Open("mysql", bootDSN); err == nil {
			_, _ = db.Exec("DROP DATABASE IF EXISTS bintrail_idx_" + entry.ID)
			_ = db.Close()
		}
	})

	if err := sup.Start(ctx, entry); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = sup.Stop(context.Background(), entry.ID) })

	// (a) One firing, carrying the entry's source/index DSNs and the flavor
	// the stream resolved.
	var jobCtx context.Context
	select {
	case got := <-infoCh:
		want := ext.SourceJobInfo{SourceDSN: entry.SourceDSN, IndexDSN: entry.DSN, Flavor: console.FlavorMySQL, SourceTLS: ext.SourceTLS{Mode: "preferred"}}
		if got != want {
			t.Fatalf("source job info = %+v, want %+v", got, want)
		}
		jobCtx = <-ctxCh
	case <-time.After(15 * time.Second):
		t.Fatal("ext source job never fired for the started source")
	}

	// (c) An idempotent Start (the entry is still "pending" — the stub never
	// progressed it to running, and both are running-variants that early-return)
	// must NOT re-provision or fire a second job.
	if err := sup.Start(ctx, entry); err != nil {
		t.Fatalf("idempotent Start: %v", err)
	}
	select {
	case got := <-infoCh:
		t.Fatalf("idempotent Start fired a duplicate source job: %+v", got)
	case <-time.After(2 * time.Second):
	}

	// (b) The leak contract: Stop tears down the source; the job's bound context
	// must become Done. The outer daemon ctx is still live (only deferred), so
	// this proves per-source jobCtx binding, not daemon-ctx binding.
	if err := sup.Stop(ctx, entry.ID); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	select {
	case <-jobCtx.Done():
	case <-time.After(10 * time.Second):
		t.Fatal("source job context still live after Stop — job not bound to the source lifecycle")
	}
}

// TestIntegrationMonitorExtSourceJobTornDownOnStreamExit is the terminal-exit
// half of THE LEAK CONTRACT that the Stop path above does NOT cover: when the
// supervised stream ends on its own — here a clean return; the crash-loop
// give-up shares the identical terminal path in run — the ext source job's
// bound context must become Done with NO Stop call. run defers job.cancel(), so
// jobCtx dies together with the advisory lock the deferred lockDB.Close()
// releases. Without that defer the stream could exit and free the lock while the
// source job kept running, and a second daemon re-acquiring the freed lock would
// double-run it. The outer daemon ctx stays live throughout, so a Done here can
// only come from the per-source cancel, not daemon teardown.
func TestIntegrationMonitorExtSourceJobTornDownOnStreamExit(t *testing.T) {
	testutil.SkipIfNoMySQL(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, bootName := testutil.CreateTestDB(t)
	bootDSN := testutil.IntegrationDSN(bootName)

	sup := newMonitorSupervisor(ctx, bootDSN, nil, 0)
	// A stream that returns nil immediately: run takes the clean-return terminal
	// branch (err == nil) and, with the fix, cancels jobCtx via its defer.
	sup.streamFn = func(_ context.Context, cfg streamrun.Config) error {
		cfg.Hooks.OnFlavorResolved(console.FlavorMySQL)
		return nil
	}

	sentinel := fmt.Sprintf("extjobexit%d", time.Now().UnixNano()%1e9)
	infoCh, ctxCh := probeSourceJob(t, sentinel)

	entry := console.ServerEntry{
		ID:        sentinel,
		Name:      "ext-source-job-exit",
		SourceDSN: testutil.BaseDSN() + "/",
	}
	derived, err := sup.DeriveIndexDSN(entry.ID)
	if err != nil {
		t.Fatal(err)
	}
	entry.DSN = derived
	t.Cleanup(func() {
		if db, err := sql.Open("mysql", bootDSN); err == nil {
			_, _ = db.Exec("DROP DATABASE IF EXISTS bintrail_idx_" + entry.ID)
			_ = db.Close()
		}
	})

	if err := sup.Start(ctx, entry); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = sup.Stop(context.Background(), entry.ID) })

	// The job fires once (FlavorOnce: once per Start, not per stream
	// attempt). Capture its bound context — even if the stream already returned
	// and cancelled jobCtx, the job still fired (RunSourceJobs launches
	// unconditionally) and a cancelled context read here still reports Done.
	var jobCtx context.Context
	select {
	case <-infoCh:
		jobCtx = <-ctxCh
	case <-time.After(15 * time.Second):
		t.Fatal("ext source job never fired for the started source")
	}

	// THE CONTRACT: the stream exited on its own and no Stop was called, yet the
	// job's bound context must be Done. The daemon ctx is still live, so this is
	// the per-source cancel firing with the released advisory lock — not a leak.
	select {
	case <-jobCtx.Done():
	case <-time.After(10 * time.Second):
		t.Fatal("source job context still live after the stream exited on its own — job outlived the released advisory lock (leak)")
	}
}

// TestIntegrationMonitorExtSourceJobPGFlavor proves a PostgreSQL source carries
// Flavor="postgres" into the ext source job. The PG stream is stubbed (no
// reachable PostgreSQL needed); provisioning still runs against the real MySQL
// index (the shared index schema for every source family).
func TestIntegrationMonitorExtSourceJobPGFlavor(t *testing.T) {
	testutil.SkipIfNoMySQL(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, bootName := testutil.CreateTestDB(t)
	bootDSN := testutil.IntegrationDSN(bootName)

	sup := newMonitorSupervisor(ctx, bootDSN, nil, 0)
	sup.pgStreamFn = func(c context.Context, _ pgstreamrun.Config) error { <-c.Done(); return c.Err() }

	sentinel := fmt.Sprintf("extjobpg%d", time.Now().UnixNano()%1e9)
	infoCh, _ := probeSourceJob(t, sentinel)

	entry := console.ServerEntry{
		ID:                sentinel,
		Name:              "ext-source-job-pg",
		SourceDSN:         "postgres://repl:secret@pg:5432/appdb",
		SourceSlot:        "bintrail_slot",
		SourcePublication: "bintrail_pub",
		Flavor:            console.FlavorPostgres,
	}
	derived, err := sup.DeriveIndexDSN(entry.ID)
	if err != nil {
		t.Fatal(err)
	}
	entry.DSN = derived
	t.Cleanup(func() {
		if db, err := sql.Open("mysql", bootDSN); err == nil {
			_, _ = db.Exec("DROP DATABASE IF EXISTS bintrail_idx_" + entry.ID)
			_ = db.Close()
		}
	})

	if err := sup.Start(ctx, entry); err != nil {
		t.Fatalf("Start (postgres): %v", err)
	}
	t.Cleanup(func() { _ = sup.Stop(context.Background(), entry.ID) })

	select {
	case got := <-infoCh:
		want := ext.SourceJobInfo{SourceDSN: entry.SourceDSN, IndexDSN: entry.DSN, Flavor: console.FlavorPostgres, SourceTLS: ext.SourceTLS{Mode: "preferred"}}
		if got != want {
			t.Fatalf("pg source job info = %+v, want %+v", got, want)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("ext source job never fired for the started PostgreSQL source")
	}
}

// waitStreamLive proves the supervised stream is attached and past its
// auto-discovered start position before the test writes the rows it intends
// to assert on. The supervisor reports "running" while streamrun.One is still
// connecting → snapshotting → discovering its start position; rows written in
// that window land BEFORE the stream's start and are legitimately never
// indexed. On a fast laptop the window is <1s; on a cold CI runner it spans
// seconds (the #407 CI flake: "indexed 0 of 4"). Sentinel writes are retried
// until one is observed in the index — once any sentinel lands, every later
// write must land too (the binlog is sequential).
func waitStreamLive(t *testing.T, srcDB, idxDB *sql.DB, schema, table, pkCol string) {
	t.Helper()
	for attempt := range 8 {
		id := 900000 + attempt
		if _, err := srcDB.Exec(fmt.Sprintf("INSERT INTO %s.%s (%s) VALUES (%d)", schema, table, pkCol, id)); err != nil {
			t.Fatalf("sentinel insert: %v", err)
		}
		deadline := time.Now().Add(8 * time.Second)
		for time.Now().Before(deadline) {
			var n int
			_ = idxDB.QueryRow(
				"SELECT COUNT(*) FROM binlog_events WHERE schema_name = ? AND table_name = ? AND pk_values = ?",
				schema, table, fmt.Sprint(id)).Scan(&n)
			if n > 0 {
				return
			}
			time.Sleep(300 * time.Millisecond)
		}
	}
	t.Fatal("stream never went live: no sentinel write was indexed")
}

// TestIntegrationMonitorSupervisor exercises the control plane end to end
// against real MySQL: provision a per-source index DB, take the capture lock,
// stream real binlog events into it, make a second daemon wait (#2105), stop
// cleanly releasing the lock, and see the second daemon take over.
func TestIntegrationMonitorSupervisor(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	shortCaptureLockTimings(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// The daemon's boot index: any database on the test server (per-source
	// DBs are derived siblings on the same server).
	_, bootName := testutil.CreateTestDB(t)
	bootDSN := testutil.IntegrationDSN(bootName)

	// A dedicated source schema with a table that exists BEFORE the stream
	// starts (so the startup snapshot covers it — see #396 for the
	// DDL-after-start race).
	srcDB, err := sql.Open("mysql", testutil.BaseDSN()+"/?parseTime=true")
	if err != nil {
		t.Fatal(err)
	}
	defer srcDB.Close()
	srcSchema := fmt.Sprintf("cp_src_%d", time.Now().UnixNano()%1e9)
	mustExec := func(q string) {
		t.Helper()
		if _, err := srcDB.Exec(q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	mustExec("CREATE DATABASE " + srcSchema)
	t.Cleanup(func() { _, _ = srcDB.Exec("DROP DATABASE IF EXISTS " + srcSchema) })
	mustExec("CREATE TABLE " + srcSchema + ".items (id INT PRIMARY KEY, qty INT)")

	sup := newMonitorSupervisor(ctx, bootDSN, nil, 0)
	entry := console.ServerEntry{
		ID:        fmt.Sprintf("itest%d", time.Now().UnixNano()%1e9),
		Name:      "integration",
		SourceDSN: testutil.BaseDSN() + "/",
		Schemas:   srcSchema,
	}

	// Derive + provision + start.
	derived, err := sup.DeriveIndexDSN(entry.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(derived, "bintrail_idx_"+entry.ID) {
		t.Fatalf("derived DSN missing per-source db: %q", derived)
	}
	entry.DSN = derived
	t.Cleanup(func() { _, _ = srcDB.Exec("DROP DATABASE IF EXISTS bintrail_idx_" + entry.ID) })

	// Doctor against the real source must not fail (warnings allowed —
	// retention on the test container is a warn).
	report, err := sup.Doctor(ctx, entry)
	if err != nil {
		t.Fatal(err)
	}
	if report.Failed > 0 {
		t.Fatalf("doctor failed on the test source: %+v", report.Checks)
	}

	if err := sup.Start(ctx, entry); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = sup.Stop(context.Background(), entry.ID) })

	waitState := func(want string, timeout time.Duration) console.MonitorStatus {
		t.Helper()
		deadline := time.Now().Add(timeout)
		for time.Now().Before(deadline) {
			st := sup.Status(entry.ID)
			if st.State == want {
				return st
			}
			time.Sleep(200 * time.Millisecond)
		}
		st := sup.Status(entry.ID)
		t.Fatalf("state = %+v, want %s", st, want)
		return st
	}
	waitState("running", 30*time.Second)

	idxDB, err := sql.Open("mysql", derived)
	if err != nil {
		t.Fatal(err)
	}
	defer idxDB.Close()
	// "running" means the supervisor launched the stream, not that it finished
	// attaching — gate on real liveness before writing the rows under test.
	waitStreamLive(t, srcDB, idxDB, srcSchema, "items", "id")

	// The capture lock: a second supervisor (second daemon) must wait for it,
	// not capture beside the first and not fail for good (#2105).
	sup2 := newMonitorSupervisor(ctx, bootDSN, nil, 0)
	if err := sup2.Start(ctx, entry); err != nil {
		t.Fatalf("second daemon Start: %v, want it to wait for the lock", err)
	}
	t.Cleanup(func() { _ = sup2.Stop(context.Background(), entry.ID) })
	time.Sleep(time.Second) // several lock polls
	if st := sup2.Status(entry.ID); st.State != "pending" || st.Phase != monitorPhaseLockWaiting {
		t.Fatalf("second daemon while the first captures: %+v, want pending/%s", st, monitorPhaseLockWaiting)
	}
	// A schema reload on the waiting daemon reloads nothing: the stream it
	// would claim to restart is the first daemon's.
	if reloaded, err := sup2.ReloadSchema(ctx, entry); err != nil || reloaded {
		t.Fatalf("ReloadSchema on a waiting daemon: reloaded=%v err=%v, want false, nil", reloaded, err)
	}
	if st := sup2.Status(entry.ID); st.State != "pending" || st.Phase != monitorPhaseLockWaiting {
		t.Fatalf("waiting daemon after ReloadSchema: %+v, want still waiting", st)
	}

	// Real events flow into the per-source index.
	mustExec("INSERT INTO " + srcSchema + ".items VALUES (1, 10), (2, 5)")
	mustExec("UPDATE " + srcSchema + ".items SET qty = 99 WHERE id = 1")

	var count int
	deadline := time.Now().Add(45 * time.Second)
	for time.Now().Before(deadline) {
		_ = idxDB.QueryRow("SELECT COUNT(*) FROM binlog_events WHERE schema_name = ?", srcSchema).Scan(&count)
		if count >= 3 {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if count < 3 {
		t.Fatalf("expected >=3 events in the per-source index, got %d", count)
	}

	// Stop: drains the stream and releases the capture lock.
	if err := sup.Stop(ctx, entry.ID); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if st := sup.Status(entry.ID); st.State != "stopped" {
		t.Errorf("after Stop: state=%+v, want stopped", st)
	}

	// The waiting daemon takes over on its own and captures from the
	// checkpoint the first one left.
	deadline = time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) && sup2.Status(entry.ID).State != "running" {
		time.Sleep(200 * time.Millisecond)
	}
	if st := sup2.Status(entry.ID); st.State != "running" {
		t.Fatalf("second daemon after the first stopped: %+v, want running", st)
	}
	mustExec("INSERT INTO " + srcSchema + ".items VALUES (3, 1)")
	deadline = time.Now().Add(45 * time.Second)
	var taken int
	for time.Now().Before(deadline) {
		_ = idxDB.QueryRow("SELECT COUNT(*) FROM binlog_events WHERE schema_name = ? AND pk_values = '3'", srcSchema).Scan(&taken)
		if taken > 0 {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if taken != 1 {
		t.Fatalf("row written after the handover indexed %d times, want 1", taken)
	}
	var dups int
	if err := idxDB.QueryRow(`SELECT COUNT(*) FROM (SELECT 1 FROM binlog_events WHERE schema_name = ?
		GROUP BY pk_values, event_type, binlog_file, start_pos HAVING COUNT(*) > 1) d`, srcSchema).Scan(&dups); err != nil {
		t.Fatal(err)
	}
	if dups != 0 {
		t.Fatalf("%d events indexed twice across the handover", dups)
	}
	if err := sup2.Stop(ctx, entry.ID); err != nil {
		t.Fatalf("second daemon Stop: %v", err)
	}
	// Stop returns once the run goroutine has closed lockDB on the client
	// side, but mysqld releases a GET_LOCK held by a closing session only
	// when it finishes tearing the session down — asynchronously. A
	// zero-timeout probe raced that teardown and flaked under CI load
	// (#1164, twice on main with this exact assertion). GET_LOCK's own
	// timeout is the event-driven wait: it returns 1 the moment the lock
	// frees and 0 only if the lock is genuinely still held after 10s.
	var got int
	if err := idxDB.QueryRow("SELECT GET_LOCK(?, 10)", "bintrail_monitor_"+entry.ID).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != 1 {
		t.Error("advisory lock must be free after Stop")
	}
	_, _ = idxDB.Exec("SELECT RELEASE_LOCK(?)", "bintrail_monitor_"+entry.ID)
}

// TestIntegrationDDLThenImmediateInserts is the #396 regression: a
// `CREATE TABLE …; INSERT …; UPDATE …; DELETE …;` burst against a LIVE stream
// must index every trailing row event. Before the synchronous parser-side DDL
// hook, the consumer-side auto-snapshot raced the parser and the rows that
// followed the DDL in the binlog were silently skipped ("table not in
// snapshot") and permanently lost.
func TestIntegrationDDLThenImmediateInserts(t *testing.T) {
	testutil.SkipIfNoMySQL(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, bootName := testutil.CreateTestDB(t)
	bootDSN := testutil.IntegrationDSN(bootName)

	srcDB, err := sql.Open("mysql", testutil.BaseDSN()+"/?parseTime=true")
	if err != nil {
		t.Fatal(err)
	}
	defer srcDB.Close()
	srcSchema := fmt.Sprintf("ddlrace_%d", time.Now().UnixNano()%1e9)
	if _, err := srcDB.Exec("CREATE DATABASE " + srcSchema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = srcDB.Exec("DROP DATABASE IF EXISTS " + srcSchema) })
	// A pre-existing table so the STARTUP snapshot has something to capture —
	// the bug under test is about a table created LATER, mid-stream.
	if _, err := srcDB.Exec("CREATE TABLE " + srcSchema + ".seed (id INT PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}

	sup := newMonitorSupervisor(ctx, bootDSN, nil, 0)
	entry := console.ServerEntry{
		ID:        fmt.Sprintf("ddlrace%d", time.Now().UnixNano()%1e9),
		Name:      "ddl-race",
		SourceDSN: testutil.BaseDSN() + "/",
		Schemas:   srcSchema,
	}
	derived, err := sup.DeriveIndexDSN(entry.ID)
	if err != nil {
		t.Fatal(err)
	}
	entry.DSN = derived
	t.Cleanup(func() { _, _ = srcDB.Exec("DROP DATABASE IF EXISTS bintrail_idx_" + entry.ID) })

	if err := sup.Start(ctx, entry); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = sup.Stop(context.Background(), entry.ID) })

	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) && sup.Status(entry.ID).State != "running" {
		time.Sleep(200 * time.Millisecond)
	}
	if st := sup.Status(entry.ID); st.State != "running" {
		t.Fatalf("stream not running: %+v", st)
	}

	idxDB, err := sql.Open("mysql", derived)
	if err != nil {
		t.Fatal(err)
	}
	defer idxDB.Close()
	// "running" precedes actual attachment; prove liveness via the seed table
	// before the burst, or a slow runner discovers its start position AFTER
	// the burst and legitimately never sees it (the #407 CI flake).
	waitStreamLive(t, srcDB, idxDB, srcSchema, "seed", "id")

	// THE repro: the table is created WHILE the stream is live, with the row
	// events immediately behind the DDL in the binlog — one multi-statement
	// burst, no pause for any snapshot to win a race.
	if _, err := srcDB.Exec("CREATE TABLE " + srcSchema + ".burst (id INT PRIMARY KEY, v VARCHAR(20))"); err != nil {
		t.Fatal(err)
	}
	if _, err := srcDB.Exec("INSERT INTO " + srcSchema + ".burst VALUES (1,'a'),(2,'b')"); err != nil {
		t.Fatal(err)
	}
	if _, err := srcDB.Exec("UPDATE " + srcSchema + ".burst SET v='z' WHERE id=1"); err != nil {
		t.Fatal(err)
	}
	if _, err := srcDB.Exec("DELETE FROM " + srcSchema + ".burst WHERE id=2"); err != nil {
		t.Fatal(err)
	}

	// 3 row events (INSERT batch counts once per row? InsertEvent granularity:
	// the indexer writes one row per affected row — INSERT(2 rows) + UPDATE(1)
	// + DELETE(1) = 4) must ALL land; before the fix this was 0.
	var count int
	deadline = time.Now().Add(45 * time.Second)
	for time.Now().Before(deadline) {
		_ = idxDB.QueryRow(
			"SELECT COUNT(*) FROM binlog_events WHERE schema_name = ? AND table_name = 'burst'",
			srcSchema).Scan(&count)
		if count >= 4 {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if count < 4 {
		t.Fatalf("rows immediately after the DDL were lost: indexed %d of 4 (#396)", count)
	}

	// And the DDL itself was recorded with a fresh snapshot.
	var changes int
	if err := idxDB.QueryRow(
		"SELECT COUNT(*) FROM schema_changes WHERE table_name = 'burst'").Scan(&changes); err != nil {
		t.Fatal(err)
	}
	if changes < 1 {
		t.Errorf("schema_changes must record the CREATE TABLE, got %d rows", changes)
	}
}

// TestIntegrationLostPositionDurable verifies the #402 lost-position cycle
// end to end: a recorded gap loss survives a supervisor restart (re-hydrated
// from stream_state at Start), is presented as the derived "lost_position"
// state, and is cleared only by an explicit Stop — the operator's
// acknowledgment. The gap record itself is simulated (real binlog purging is
// not reproducible on the shared test container); the streamrun.One write path
// for it is the same UPDATE this test issues.
func TestIntegrationLostPositionDurable(t *testing.T) {
	testutil.SkipIfNoMySQL(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, bootName := testutil.CreateTestDB(t)
	bootDSN := testutil.IntegrationDSN(bootName)

	srcDB, err := sql.Open("mysql", testutil.BaseDSN()+"/?parseTime=true")
	if err != nil {
		t.Fatal(err)
	}
	defer srcDB.Close()
	srcSchema := fmt.Sprintf("cp_lp_%d", time.Now().UnixNano()%1e9)
	if _, err := srcDB.Exec("CREATE DATABASE " + srcSchema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = srcDB.Exec("DROP DATABASE IF EXISTS " + srcSchema) })
	if _, err := srcDB.Exec("CREATE TABLE " + srcSchema + ".items (id INT PRIMARY KEY, qty INT)"); err != nil {
		t.Fatal(err)
	}

	sup := newMonitorSupervisor(ctx, bootDSN, nil, 0)
	entry := console.ServerEntry{
		ID:        fmt.Sprintf("lptest%d", time.Now().UnixNano()%1e9),
		Name:      "lost-position",
		SourceDSN: testutil.BaseDSN() + "/",
		Schemas:   srcSchema,
	}
	derived, err := sup.DeriveIndexDSN(entry.ID)
	if err != nil {
		t.Fatal(err)
	}
	entry.DSN = derived
	t.Cleanup(func() { _, _ = srcDB.Exec("DROP DATABASE IF EXISTS bintrail_idx_" + entry.ID) })

	waitState := func(want string, timeout time.Duration) {
		t.Helper()
		deadline := time.Now().Add(timeout)
		for time.Now().Before(deadline) {
			if sup.Status(entry.ID).State == want {
				return
			}
			time.Sleep(200 * time.Millisecond)
		}
		t.Fatalf("state = %+v, want %s", sup.Status(entry.ID), want)
	}

	// Run 1: healthy start, then stop (creates the per-source DB and its
	// stream_state checkpoint row).
	if err := sup.Start(ctx, entry); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = sup.Stop(context.Background(), entry.ID) })
	waitState("running", 30*time.Second)

	idxDB, err := sql.Open("mysql", derived)
	if err != nil {
		t.Fatal(err)
	}
	defer idxDB.Close()
	// The flip to running can come from an indexed batch before the first
	// checkpoint write — wait for the stream_state row itself.
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		var n int
		if err := idxDB.QueryRow("SELECT COUNT(*) FROM stream_state WHERE id = 1").Scan(&n); err == nil && n == 1 {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if err := sup.Stop(ctx, entry.ID); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	// Simulate what streamrun.One persists on an unfillable-gap auto-advance.
	const gapDetail = "simulated: binlogs purged past saved position; events lost"
	if _, err := idxDB.Exec(`UPDATE stream_state
		SET gap_lost_at = UTC_TIMESTAMP(), gap_lost_detail = ? WHERE id = 1`, gapDetail); err != nil {
		t.Fatal(err)
	}

	// Run 2 (the "daemon restart"): Start must re-hydrate the record and the
	// derived state must surface it instead of a clean RUNNING badge.
	if err := sup.Start(ctx, entry); err != nil {
		t.Fatalf("Start (run 2): %v", err)
	}
	waitState("lost_position", 30*time.Second)
	if st := sup.Status(entry.ID); st.LastError != gapDetail {
		t.Errorf("LastError = %q, want the hydrated gap detail", st.LastError)
	}

	// Explicit Stop acknowledges: the durable record must be cleared.
	if err := sup.Stop(ctx, entry.ID); err != nil {
		t.Fatalf("Stop (ack): %v", err)
	}
	var at, detail sql.NullString
	if err := idxDB.QueryRow("SELECT gap_lost_at, gap_lost_detail FROM stream_state WHERE id = 1").Scan(&at, &detail); err != nil {
		t.Fatal(err)
	}
	if at.Valid || detail.Valid {
		t.Errorf("gap record not cleared on explicit Stop: at=%v detail=%v", at, detail)
	}

	// Run 3: a fresh start after acknowledgment is plain running again.
	if err := sup.Start(ctx, entry); err != nil {
		t.Fatalf("Start (run 3): %v", err)
	}
	waitState("running", 30*time.Second)
	if st := sup.Status(entry.ID); st.State != "running" || st.LastError != "" {
		t.Errorf("post-ack status = %+v, want clean running", st)
	}
}

// TestIntegrationReplicaOverlapSQL exercises the replica-detection SQL
// against real MySQL — the unit tests cover the pure GTID helpers and the
// card assembly (evaluateReplicaOverlap). The shared test container runs
// gtid_mode=OFF, so the full warn path is not reproducible here; what this
// test proves is (a) the supervisor-side flow up to and including the
// gtid_mode gate (registry peers → source connect → skip card), (b)
// loadPeerIdentity's queries against a provisioned per-source index DB
// (bintrail_servers + stream_state scan shapes), and (c)
// loadCandidateIdentity's query shapes, unreachable in (a) past the gate.
func TestIntegrationReplicaOverlapSQL(t *testing.T) {
	testutil.SkipIfNoMySQL(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, bootName := testutil.CreateTestDB(t)
	bootDSN := testutil.IntegrationDSN(bootName)

	// A registry with TWO source-bearing entries so Doctor(A) has peer B.
	regPath := t.TempDir() + "/servers.yaml"
	reg, err := console.LoadRegistry(regPath)
	if err != nil {
		t.Fatal(err)
	}
	entryA, err := reg.Add(console.ServerEntry{Name: "cand", SourceDSN: testutil.BaseDSN() + "/", DSN: bootDSN})
	if err != nil {
		t.Fatal(err)
	}
	sup := newMonitorSupervisor(ctx, bootDSN, reg, 0)

	// Peer B's per-source index DB, provisioned with the real tables and
	// seeded with an identity + an accumulated GTID set.
	peerDB, peerName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, peerDB)
	const peerUUID = "3e11fa47-71ca-11e1-9e33-c80aa9429562"
	if _, err := peerDB.Exec(`INSERT INTO bintrail_servers
		(bintrail_id, server_uuid, host, port, username)
		VALUES (UUID(), ?, 'peer-host', 3306, 'rep')`, peerUUID); err != nil {
		t.Fatal(err)
	}
	if _, err := peerDB.Exec(`INSERT INTO stream_state
		(id, mode, gtid_set, last_checkpoint, server_id)
		VALUES (1, 'gtid', ?, UTC_TIMESTAMP(), 12345)`, peerUUID+":1-100"); err != nil {
		t.Fatal(err)
	}
	if _, err := reg.Add(console.ServerEntry{
		Name: "peer", SourceDSN: testutil.BaseDSN() + "/", DSN: testutil.IntegrationDSN(peerName),
	}); err != nil {
		t.Fatal(err)
	}

	// (a) Doctor on the candidate: container runs gtid_mode=OFF, so the
	// check must surface as an explicit skip card — not vanish, not warn.
	report, err := sup.Doctor(ctx, entryA)
	if err != nil {
		t.Fatal(err)
	}
	var card *console.DoctorCheck
	for i := range report.Checks {
		if report.Checks[i].Name == "Replica / duplicate detection" {
			card = &report.Checks[i]
		}
	}
	if card == nil {
		t.Fatalf("replica check card missing from doctor report: %+v", report.Checks)
	}
	if card.Status != "skip" || !strings.Contains(card.Detail, "gtid_mode") {
		t.Fatalf("card = %+v, want skip mentioning gtid_mode (container runs GTID off)", *card)
	}

	// (b) loadPeerIdentity's SQL against the seeded per-source DB.
	gotUUID, gotSet, err := loadPeerIdentity(ctx, testutil.IntegrationDSN(peerName))
	if err != nil {
		t.Fatalf("loadPeerIdentity: %v", err)
	}
	if gotUUID != peerUUID {
		t.Errorf("peer uuid = %q, want %q", gotUUID, peerUUID)
	}
	if gotSet != peerUUID+":1-100" {
		t.Errorf("peer gtid set = %q, want %q", gotSet, peerUUID+":1-100")
	}

	// (c) The candidate-side identity queries (unreachable above because of
	// the gtid_mode gate) — exercise the production helper directly.
	srcConn, err := config.Connect(testutil.BaseDSN() + "/")
	if err != nil {
		t.Fatal(err)
	}
	defer srcConn.Close()
	gtidMode, candUUID, _, err := loadCandidateIdentity(ctx, srcConn)
	if err != nil {
		t.Fatalf("loadCandidateIdentity: %v", err)
	}
	if !strings.EqualFold(gtidMode, "OFF") {
		t.Errorf("gtid_mode = %q, want OFF on the shared test container", gtidMode)
	}
	if candUUID == "" {
		t.Error("candidate server_uuid came back empty")
	}
}

// TestIntegrationMonitorLockLossStopsThenResumes: a supervised stream whose
// capture lock vanishes (here its session is killed, as wait_timeout or a cut
// connection would) must stop writing and start again holding a lock, on a
// new session, with no row indexed twice (#2105).
func TestIntegrationMonitorLockLossStopsThenResumes(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	shortCaptureLockTimings(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, bootName := testutil.CreateTestDB(t)
	bootDSN := testutil.IntegrationDSN(bootName)

	srcDB, err := sql.Open("mysql", testutil.BaseDSN()+"/?parseTime=true")
	if err != nil {
		t.Fatal(err)
	}
	defer srcDB.Close()
	srcSchema := fmt.Sprintf("lockloss_%d", time.Now().UnixNano()%1e9)
	mustExec := func(q string) {
		t.Helper()
		if _, err := srcDB.Exec(q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	mustExec("CREATE DATABASE " + srcSchema)
	t.Cleanup(func() { _, _ = srcDB.Exec("DROP DATABASE IF EXISTS " + srcSchema) })
	mustExec("CREATE TABLE " + srcSchema + ".items (id INT PRIMARY KEY, qty INT)")

	sup := newMonitorSupervisor(ctx, bootDSN, nil, 0)
	entry := console.ServerEntry{
		ID:        fmt.Sprintf("lockloss%d", time.Now().UnixNano()%1e9),
		Name:      "lockloss",
		SourceDSN: testutil.BaseDSN() + "/",
		Schemas:   srcSchema,
	}
	derived, err := sup.DeriveIndexDSN(entry.ID)
	if err != nil {
		t.Fatal(err)
	}
	entry.DSN = derived
	t.Cleanup(func() { _, _ = srcDB.Exec("DROP DATABASE IF EXISTS bintrail_idx_" + entry.ID) })
	if err := sup.Start(ctx, entry); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = sup.Stop(context.Background(), entry.ID) })
	waitRunning := func(what string) {
		t.Helper()
		deadline := time.Now().Add(30 * time.Second)
		for time.Now().Before(deadline) && sup.Status(entry.ID).State != "running" {
			time.Sleep(100 * time.Millisecond)
		}
		if st := sup.Status(entry.ID); st.State != "running" {
			t.Fatalf("%s: %+v, want running", what, st)
		}
	}
	waitRunning("first start")
	idxDB, err := sql.Open("mysql", derived)
	if err != nil {
		t.Fatal(err)
	}
	defer idxDB.Close()
	waitStreamLive(t, srcDB, idxDB, srcSchema, "items", "id")

	lockName := monitorLockName(entry.ID)
	var holder int64
	if err := idxDB.QueryRow("SELECT IS_USED_LOCK(?)", lockName).Scan(&holder); err != nil || holder == 0 {
		t.Fatalf("lock holder: %d %v", holder, err)
	}
	if _, err := idxDB.Exec(fmt.Sprintf("KILL %d", holder)); err != nil {
		t.Fatal(err)
	}
	// The job must leave "running" (the stream was told to stop writing) ...
	deadline := time.Now().Add(10 * time.Second)
	left := false
	for time.Now().Before(deadline) {
		if sup.Status(entry.ID).State != "running" {
			left = true
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !left {
		t.Fatal("the lock's session was killed and the stream kept running")
	}
	// ... and come back holding the lock again, on another session.
	waitRunning("after the lock was lost")
	var again int64
	if err := idxDB.QueryRow("SELECT IS_USED_LOCK(?)", lockName).Scan(&again); err != nil || again == 0 || again == holder {
		t.Fatalf("lock holder after resuming: %d (before %d) %v, want a new session", again, holder, err)
	}
	mustExec("INSERT INTO " + srcSchema + ".items VALUES (7, 7)")
	var n int
	deadline = time.Now().Add(45 * time.Second)
	for time.Now().Before(deadline) {
		_ = idxDB.QueryRow("SELECT COUNT(*) FROM binlog_events WHERE schema_name = ? AND pk_values = '7'", srcSchema).Scan(&n)
		if n > 0 {
			break
		}
		time.Sleep(300 * time.Millisecond)
	}
	if n != 1 {
		t.Fatalf("row written after resuming indexed %d times, want 1", n)
	}
}
