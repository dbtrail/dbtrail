//go:build integration

package pgstreamrun_test

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/pgcapture"
	"github.com/dbtrail/dbtrail/internal/pgstreamrun"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/recovery"
	"github.com/dbtrail/dbtrail/internal/status"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestOne_EndToEnd_PostgresToIndexToRecovery is the permanent form of the capture
// spike's Part C: a live PostgreSQL change stream flows through pgstreamrun.One into
// the real MySQL index (binlog_events), is read back by the query engine, and
// produces reversal SQL — with an out-of-line TOAST value round-tripping all the way.
// This is what closes #530's end-to-end acceptance.
//
// Needs BOTH a live MySQL index (BINTRAIL_TEST_DSN, the CI default) and a live
// PostgreSQL source (BINTRAIL_TEST_PG_DSN; not set on the MySQL CI jobs → skips).
func TestOne_EndToEnd_PostgresToIndexToRecovery(t *testing.T) {
	pgDSN := testutil.SkipIfNoPostgres(t)
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()

	indexDB, dbName := testutil.CreateTestDB(t)
	indexDSN := testutil.BaseDSN() + "/" + dbName

	const slot = "bintrail_pgsr_it"
	const pub = "bintrail_pgsr_it_pub"
	const tbl = "pgsr_it_t"
	const bigSize = 6000

	pg, err := pgx.Connect(ctx, pgDSN)
	if err != nil {
		t.Fatalf("connect PG: %v", err)
	}
	t.Cleanup(func() { pg.Close(context.Background()) })

	dropAll := func() {
		bg := context.Background()
		_, _ = pg.Exec(bg, "DROP PUBLICATION IF EXISTS "+pub)
		_, _ = pg.Exec(bg, "DROP TABLE IF EXISTS "+tbl)
		_, _ = pg.Exec(bg, "SELECT pg_drop_replication_slot($1) WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name=$1)", slot)
	}
	dropAll()
	t.Cleanup(dropAll)

	mustExec := func(sql string, args ...any) {
		t.Helper()
		if _, err := pg.Exec(ctx, sql, args...); err != nil {
			t.Fatalf("exec %q: %v", sql, err)
		}
	}
	mustExec(fmt.Sprintf("CREATE TABLE %s (id int PRIMARY KEY, small_col text, big_col text)", tbl))
	mustExec(fmt.Sprintf("ALTER TABLE %s ALTER COLUMN big_col SET STORAGE EXTERNAL", tbl))
	mustExec(fmt.Sprintf("ALTER TABLE %s REPLICA IDENTITY FULL", tbl))
	mustExec(fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s", pub, tbl))

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	cfg := pgstreamrun.Config{
		IndexDSN:    indexDSN,
		ReplDSN:     replDSN(pgDSN),
		QueryDSN:    pgDSN,
		SlotName:    slot,
		Publication: pub,
		ServerID:    42,
		Tables:      "public." + tbl,
		BatchSize:   100,
		Checkpoint:  200 * time.Millisecond,
	}
	var connected atomic.Bool
	cfg.Hooks = &pgstreamrun.Hooks{OnSourceConnected: func() { connected.Store(true) }}
	runErr := make(chan error, 1)
	go func() { runErr <- pgstreamrun.One(runCtx, cfg) }()

	// Wait until the consumer's capturer has the slot active, then do DML so every
	// change is captured.
	waitFor(t, 15*time.Second, func() bool {
		var active bool
		if err := pg.QueryRow(ctx, "SELECT active FROM pg_replication_slots WHERE slot_name=$1", slot).Scan(&active); err != nil {
			return false
		}
		return active
	}, "replication slot active")
	// The first-run list's connection step (#1606) follows replication start.
	waitFor(t, 15*time.Second, connected.Load, "OnSourceConnected fired")

	bigVal := strings.Repeat("X", bigSize)
	mustExec(fmt.Sprintf("INSERT INTO %s VALUES (1, 'orig', $1)", tbl), bigVal)
	mustExec(fmt.Sprintf("UPDATE %s SET small_col='changed' WHERE id=1", tbl)) // big_col untouched

	// Wait until both row events are indexed (flushed on the 200ms ticker).
	waitFor(t, 15*time.Second, func() bool {
		var n int
		if err := indexDB.QueryRow("SELECT COUNT(*) FROM binlog_events WHERE table_name = ?", tbl).Scan(&n); err != nil {
			return false
		}
		return n >= 2
	}, "INSERT+UPDATE indexed into binlog_events")

	cancel()
	if err := <-runErr; err != nil {
		t.Fatalf("One returned error: %v", err)
	}

	// Read back through the real query engine.
	rows, err := query.New(indexDB).Fetch(ctx, query.Options{Schema: "public", Table: tbl, Order: "ASC"})
	if err != nil {
		t.Fatalf("query Fetch: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("indexed %d events, want 2 (INSERT + UPDATE)", len(rows))
	}

	var upd *query.ResultRow
	for i := range rows {
		if rows[i].EventType == event.EventUpdate {
			upd = &rows[i]
		}
	}
	if upd == nil {
		t.Fatal("no UPDATE event read back from the index")
	}
	// The #530 headline: the out-of-line TOAST value round-trips into the index.
	if got := upd.RowBefore["big_col"]; got != bigVal {
		t.Errorf("UPDATE RowBefore[big_col] is %d bytes (%T), want the real %d-byte value", lenOf(got), got, bigSize)
	}
	// Option B: the after-image carries the real (unchanged) value, not a sentinel.
	if got := upd.RowAfter["big_col"]; got != bigVal {
		t.Errorf("UPDATE RowAfter[big_col] not resolved to the real value (Option B): %d bytes (%T)", lenOf(got), got)
	}

	// Recovery SQL: the TOAST value round-trips all the way into reversal SQL.
	var buf bytes.Buffer
	n, err := recovery.New(indexDB, nil).GenerateSQLFromRows(rows, &buf)
	if err != nil {
		t.Fatalf("GenerateSQLFromRows: %v", err)
	}
	if n == 0 {
		t.Fatal("no reversal statements generated")
	}
	if !strings.Contains(buf.String(), bigVal) {
		t.Error("reversal SQL does not contain the TOAST value — it did not round-trip into recovery")
	}
}

// TestOne_DDLDrift_MidStreamAlter is the integration half of the #591 DDL-drift gate:
// an ALTER TABLE ... ADD COLUMN mid-stream must (1) decode the post-ALTER row against
// the NEW shape (the added column is captured), (2) stamp it with a NEW schema_version
// than the pre-ALTER row (the Relation-message-driven re-snapshot — the PG analog of
// the MySQL #396 auto-snapshot-on-DDL), and (3) still produce valid reversal SQL.
// Logical decoding streams no DDL, so this proves the RelationMessage shape-swap
// (decoder.cacheRelation) end-to-end on live PostgreSQL — the behavior the decoder unit
// test TestDecode_RelationShapeChange_MidStream pins in isolation.
//
// CI-only: needs a live PostgreSQL source (BINTRAIL_TEST_PG_DSN) + MySQL index; the
// MySQL-only CI jobs skip it (a green local `go test` without PG does NOT exercise it).
func TestOne_DDLDrift_MidStreamAlter(t *testing.T) {
	pgDSN := testutil.SkipIfNoPostgres(t)
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()

	indexDB, dbName := testutil.CreateTestDB(t)
	indexDSN := testutil.BaseDSN() + "/" + dbName

	const slot = "bintrail_pgsr_ddl"
	const pub = "bintrail_pgsr_ddl_pub"
	const tbl = "pgsr_ddl_t"

	pg, err := pgx.Connect(ctx, pgDSN)
	if err != nil {
		t.Fatalf("connect PG: %v", err)
	}
	t.Cleanup(func() { pg.Close(context.Background()) })

	dropAll := func() {
		bg := context.Background()
		_, _ = pg.Exec(bg, "DROP PUBLICATION IF EXISTS "+pub)
		_, _ = pg.Exec(bg, "DROP TABLE IF EXISTS "+tbl)
		_, _ = pg.Exec(bg, "SELECT pg_drop_replication_slot($1) WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name=$1)", slot)
	}
	dropAll()
	t.Cleanup(dropAll)

	mustExec := func(sql string, args ...any) {
		t.Helper()
		if _, err := pg.Exec(ctx, sql, args...); err != nil {
			t.Fatalf("exec %q: %v", sql, err)
		}
	}
	mustExec(fmt.Sprintf("CREATE TABLE %s (id int PRIMARY KEY, a text)", tbl))
	mustExec(fmt.Sprintf("ALTER TABLE %s REPLICA IDENTITY FULL", tbl))
	mustExec(fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s", pub, tbl))

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	cfg := pgstreamrun.Config{
		IndexDSN:    indexDSN,
		ReplDSN:     replDSN(pgDSN),
		QueryDSN:    pgDSN,
		SlotName:    slot,
		Publication: pub,
		ServerID:    43,
		Tables:      "public." + tbl,
		BatchSize:   100,
		Checkpoint:  200 * time.Millisecond,
	}
	runErr := make(chan error, 1)
	go func() { runErr <- pgstreamrun.One(runCtx, cfg) }()

	waitFor(t, 15*time.Second, func() bool {
		var active bool
		if err := pg.QueryRow(ctx, "SELECT active FROM pg_replication_slots WHERE slot_name=$1", slot).Scan(&active); err != nil {
			return false
		}
		return active
	}, "replication slot active")

	// Pre-ALTER row against the (id, a) shape; wait until it is indexed so its
	// RelationMessage (the first snapshot) is durably recorded before the ALTER.
	mustExec(fmt.Sprintf("INSERT INTO %s VALUES (1, 'x')", tbl))
	waitFor(t, 15*time.Second, func() bool {
		var n int
		_ = indexDB.QueryRow("SELECT COUNT(*) FROM binlog_events WHERE table_name = ? AND pk_values = '1'", tbl).Scan(&n)
		return n >= 1
	}, "pre-ALTER INSERT indexed")

	// Mid-stream DDL: add a column. pgoutput emits no DDL, but it DOES re-emit a fresh
	// RelationMessage carrying the new shape before the next row event.
	mustExec(fmt.Sprintf("ALTER TABLE %s ADD COLUMN b text", tbl))
	mustExec(fmt.Sprintf("INSERT INTO %s VALUES (2, 'y', 'z')", tbl))
	waitFor(t, 15*time.Second, func() bool {
		var n int
		_ = indexDB.QueryRow("SELECT COUNT(*) FROM binlog_events WHERE table_name = ? AND pk_values = '2'", tbl).Scan(&n)
		return n >= 1
	}, "post-ALTER INSERT indexed")

	cancel()
	if err := <-runErr; err != nil {
		t.Fatalf("One returned error: %v", err)
	}

	// (1) The post-ALTER row decoded against the NEW shape: the added column is captured.
	rows, err := query.New(indexDB).Fetch(ctx, query.Options{Schema: "public", Table: tbl, Order: "ASC"})
	if err != nil {
		t.Fatalf("query Fetch: %v", err)
	}
	var post *query.ResultRow
	for i := range rows {
		if rows[i].PKValues == "2" {
			post = &rows[i]
		}
	}
	if post == nil {
		t.Fatal("post-ALTER row (pk=2) not indexed")
	}
	if got := post.RowAfter["b"]; got != "z" {
		t.Errorf("post-ALTER RowAfter[b] = %v, want %q (added column captured against the swapped shape)", got, "z")
	}

	// (2) The re-snapshot: the post-ALTER row carries a DIFFERENT schema_version than the
	// pre-ALTER row. Equal versions would mean the mid-stream ALTER was not re-snapshotted.
	var svPre, svPost int
	if err := indexDB.QueryRow("SELECT schema_version FROM binlog_events WHERE table_name=? AND pk_values='1'", tbl).Scan(&svPre); err != nil {
		t.Fatalf("read pre-ALTER schema_version: %v", err)
	}
	if err := indexDB.QueryRow("SELECT schema_version FROM binlog_events WHERE table_name=? AND pk_values='2'", tbl).Scan(&svPost); err != nil {
		t.Fatalf("read post-ALTER schema_version: %v", err)
	}
	if svPost == svPre {
		t.Errorf("post-ALTER schema_version (%d) equals pre-ALTER (%d) — the mid-stream ALTER did not trigger a re-snapshot", svPost, svPre)
	}

	// (3) Recovery still produces valid reversal SQL across the mixed-shape rows.
	var buf bytes.Buffer
	n, err := recovery.New(indexDB, nil).GenerateSQLFromRows(rows, &buf)
	if err != nil {
		t.Fatalf("GenerateSQLFromRows: %v", err)
	}
	if n == 0 {
		t.Fatal("no reversal statements generated for the mixed-shape rows")
	}
}

// TestOne_LostSlotResume_PersistsLossBadge is the #532 slice-4 proof: when a resume
// finds the replication slot gone (WAL behind the checkpoint is irrecoverable), One
// returns the ErrSlotMissingOnResume sentinel AND durably records the permanent loss
// in stream_state.gap_lost_at, so index-only `status` shows the loud badge after the
// process has exited. (Dropping the slot is the deterministic trigger; a wal_status=lost
// invalidation takes the same ErrSlotLost → persist path.)
func TestOne_LostSlotResume_PersistsLossBadge(t *testing.T) {
	pgDSN := testutil.SkipIfNoPostgres(t)
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()

	indexDB, dbName := testutil.CreateTestDB(t)
	indexDSN := testutil.BaseDSN() + "/" + dbName

	const slot = "bintrail_lostresume_it"
	const pub = "bintrail_lostresume_it_pub"
	const tbl = "lostresume_it_t"

	pg, err := pgx.Connect(ctx, pgDSN)
	if err != nil {
		t.Fatalf("connect PG: %v", err)
	}
	t.Cleanup(func() { pg.Close(context.Background()) })

	dropAll := func() {
		bg := context.Background()
		_, _ = pg.Exec(bg, "DROP PUBLICATION IF EXISTS "+pub)
		_, _ = pg.Exec(bg, "DROP TABLE IF EXISTS "+tbl)
		_, _ = pg.Exec(bg, "SELECT pg_drop_replication_slot($1) WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name=$1)", slot)
	}
	dropAll()
	t.Cleanup(dropAll)

	mustExec := func(sql string) {
		t.Helper()
		if _, err := pg.Exec(ctx, sql); err != nil {
			t.Fatalf("exec %q: %v", sql, err)
		}
	}
	mustExec(fmt.Sprintf("CREATE TABLE %s (id int PRIMARY KEY, v text)", tbl))
	mustExec(fmt.Sprintf("ALTER TABLE %s REPLICA IDENTITY FULL", tbl))
	mustExec(fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s", pub, tbl))

	cfg := pgstreamrun.Config{
		IndexDSN: indexDSN, ReplDSN: replDSN(pgDSN), QueryDSN: pgDSN,
		SlotName: slot, Publication: pub, ServerID: 51,
		Tables: "public." + tbl, BatchSize: 100, Checkpoint: 200 * time.Millisecond,
	}

	// ── First run: create the slot, index a row, save a checkpoint, then stop. ──
	runCtx, cancel := context.WithCancel(ctx)
	runErr := make(chan error, 1)
	go func() { runErr <- pgstreamrun.One(runCtx, cfg) }()
	waitFor(t, 15*time.Second, func() bool {
		var active bool
		if err := pg.QueryRow(ctx, "SELECT active FROM pg_replication_slots WHERE slot_name=$1", slot).Scan(&active); err != nil {
			return false
		}
		return active
	}, "replication slot active")
	mustExec(fmt.Sprintf("INSERT INTO %s VALUES (1, 'a')", tbl))
	waitFor(t, 15*time.Second, func() bool {
		// The resume path needs a DURABLE checkpoint, not just a flushed row. Polling
		// binlog_events was the #607 race: checkpoint() runs flush() (row → binlog_events)
		// but only calls saveCheckpointPG when lastCommitLSN != 0, so a ticker flush can
		// land the row while the COMMIT is still undrained (lastCommitLSN == 0) — the row
		// appears before any checkpoint exists. A resume then finds no prior cursor and
		// never returns ErrSlotMissingOnResume. binlog_position (the commit LSN) is written
		// ONLY by saveCheckpointPG, so a non-zero value is the real "resumable cursor
		// durable" signal, and it is deterministic across PG versions.
		var pos uint64
		if err := indexDB.QueryRow("SELECT binlog_position FROM stream_state WHERE id = 1").Scan(&pos); err != nil {
			return false // no row yet → keep waiting
		}
		return pos > 0
	}, "first checkpoint persisted (resumable cursor durable)")
	cancel()
	if err := <-runErr; err != nil {
		t.Fatalf("first run returned error: %v", err)
	}

	// One has returned, but PostgreSQL clearing the slot's active_pid (the walsender
	// backend exiting) lags the client's clean Terminate — and that lag varies by PG
	// version. Without this gate, the pg_drop_replication_slot below races the backend
	// exit and errors 55006 "replication slot is active for PID" on PG 15/16 (it passed
	// on 14/17): the #607 flake. Wait for the slot to go inactive before dropping it.
	waitFor(t, 15*time.Second, func() bool {
		var active bool
		if err := pg.QueryRow(ctx, "SELECT active FROM pg_replication_slots WHERE slot_name=$1", slot).Scan(&active); err != nil {
			return false // slot row must still exist here (not dropped yet); transient → keep waiting
		}
		return !active
	}, "replication slot inactive (walsender backend exited)")

	// ── Drop the slot: the WAL behind the saved checkpoint is now gone. ──
	mustExec(fmt.Sprintf("SELECT pg_drop_replication_slot('%s')", slot))

	// ── Resume: One must fail loud with ErrSlotMissingOnResume (not silently create a
	// fresh slot that skips data) AND record the permanent loss. ──
	resumeCtx, resumeCancel := context.WithTimeout(ctx, 20*time.Second)
	defer resumeCancel()
	err = pgstreamrun.One(resumeCtx, cfg)
	if !errors.Is(err, pgcapture.ErrSlotMissingOnResume) {
		t.Fatalf("resume error = %v, want ErrSlotMissingOnResume", err)
	}

	// The permanent-loss record is durable in the index.
	st, err := status.LoadStreamState(ctx, indexDB)
	if err != nil {
		t.Fatalf("LoadStreamState: %v", err)
	}
	if st == nil || !st.GapLostAt.Valid {
		t.Fatalf("gap_lost_at not set after a lost-slot resume: %+v", st)
	}

	// And index-only status (the same data the process would show after exiting)
	// renders the loud badge.
	var buf bytes.Buffer
	status.WriteStatus(&buf, nil, nil, nil, nil, nil, st)
	if !strings.Contains(buf.String(), "EVENTS PERMANENTLY LOST") {
		t.Errorf("status does not show the permanent-loss badge:\n%s", buf.String())
	}
}

// TestOne_LostSlot_FirstRun_SeedsLossBadge is the CRITICAL-review repro: a slot detected
// lost (wal_status=lost) on a run with NO prior checkpoint row must still record the
// permanent loss — persistSlotLostPG UPSERTs, seeding a complete stream_state row, so the
// badge appears (a bare UPDATE would match zero rows and be silent). It also covers the
// ErrSlotLost arm end-to-end through One (the ErrSlotMissingOnResume arm is the resume
// test above). max_slot_wal_keep_size is server-wide; CI runs PG packages -p 1 and this
// test is non-parallel, and t.Cleanup resets it even on failure.
func TestOne_LostSlot_FirstRun_SeedsLossBadge(t *testing.T) {
	pgDSN := testutil.SkipIfNoPostgres(t)
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()

	indexDB, dbName := testutil.CreateTestDB(t)
	indexDSN := testutil.BaseDSN() + "/" + dbName

	const slot = "bintrail_lostfirst_it"
	const pub = "bintrail_lostfirst_it_pub"
	const tbl = "lostfirst_it_t"

	pg, err := pgx.Connect(ctx, pgDSN)
	if err != nil {
		t.Fatalf("connect PG: %v", err)
	}
	t.Cleanup(func() { pg.Close(context.Background()) })

	mustExec := func(sql string) {
		t.Helper()
		if _, err := pg.Exec(ctx, sql); err != nil {
			t.Fatalf("exec %q: %v", sql, err)
		}
	}
	t.Cleanup(func() {
		bg := context.Background()
		_, _ = pg.Exec(bg, "ALTER SYSTEM RESET max_slot_wal_keep_size")
		_, _ = pg.Exec(bg, "SELECT pg_reload_conf()")
		_, _ = pg.Exec(bg, "DROP PUBLICATION IF EXISTS "+pub)
		_, _ = pg.Exec(bg, "DROP TABLE IF EXISTS "+tbl)
		_, _ = pg.Exec(bg, "SELECT pg_drop_replication_slot($1) WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name=$1)", slot)
	})
	// Start clean.
	_, _ = pg.Exec(ctx, "DROP PUBLICATION IF EXISTS "+pub)
	_, _ = pg.Exec(ctx, "DROP TABLE IF EXISTS "+tbl)
	_, _ = pg.Exec(ctx, "SELECT pg_drop_replication_slot($1) WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name=$1)", slot)

	mustExec(fmt.Sprintf("CREATE TABLE %s (id serial PRIMARY KEY, pad text)", tbl))
	mustExec(fmt.Sprintf("ALTER TABLE %s REPLICA IDENTITY FULL", tbl))
	mustExec(fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s", pub, tbl))

	// Bound retention, create an idle slot, churn WAL past the bound → next checkpoint
	// invalidates it (wal_status=lost).
	mustExec("ALTER SYSTEM SET max_slot_wal_keep_size = '1MB'")
	mustExec("SELECT pg_reload_conf()")
	mustExec(fmt.Sprintf("SELECT pg_create_logical_replication_slot('%s', 'pgoutput')", slot))
	lost := false
	for i := 0; i < 10 && !lost; i++ {
		mustExec(fmt.Sprintf("INSERT INTO %s (pad) SELECT repeat('x', 900) FROM generate_series(1, 4000)", tbl))
		mustExec("SELECT pg_switch_wal()")
		mustExec("CHECKPOINT")
		var status string
		if err := pg.QueryRow(ctx, "SELECT wal_status FROM pg_replication_slots WHERE slot_name=$1", slot).Scan(&status); err != nil {
			t.Fatalf("read wal_status: %v", err)
		}
		lost = status == "lost"
	}
	if !lost {
		t.Skip("could not drive the slot to wal_status=lost on this server; skipping")
	}

	// First run with NO prior checkpoint row: ensureSlot detects the lost slot and One
	// must fail loud AND seed the loss record (the upsert, not a no-op UPDATE).
	runCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	err = pgstreamrun.One(runCtx, pgstreamrun.Config{
		IndexDSN: indexDSN, ReplDSN: replDSN(pgDSN), QueryDSN: pgDSN,
		SlotName: slot, Publication: pub, ServerID: 52,
		Tables: "public." + tbl, BatchSize: 100, Checkpoint: 200 * time.Millisecond,
	})
	if !errors.Is(err, pgcapture.ErrSlotLost) {
		t.Fatalf("first-run lost slot: One returned %v, want ErrSlotLost", err)
	}

	st, err := status.LoadStreamState(ctx, indexDB)
	if err != nil {
		t.Fatalf("LoadStreamState: %v", err)
	}
	if st == nil || !st.GapLostAt.Valid {
		t.Fatalf("gap_lost_at not seeded after a first-run lost slot (the CRITICAL no-op): %+v", st)
	}
	var buf bytes.Buffer
	status.WriteStatus(&buf, nil, nil, nil, nil, nil, st)
	if !strings.Contains(buf.String(), "EVENTS PERMANENTLY LOST") {
		t.Errorf("status does not show the permanent-loss badge:\n%s", buf.String())
	}
}

// TestDropSlot_ActiveSlotErrors covers DropSlot's safety branch: while a capture is
// running and holds the slot, DropSlot must fail loud ("replication slot is active for
// PID N") rather than silently no-op — so `bintrail-pg reset` tells the operator to
// stop the stream first. A running One provides the live consumer.
func TestDropSlot_ActiveSlotErrors(t *testing.T) {
	pgDSN := testutil.SkipIfNoPostgres(t)
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()

	_, dbName := testutil.CreateTestDB(t) // One bootstraps the index tables itself
	indexDSN := testutil.BaseDSN() + "/" + dbName

	const slot = "bintrail_activedrop_it"
	const pub = "bintrail_activedrop_it_pub"
	const tbl = "activedrop_it_t"

	pg, err := pgx.Connect(ctx, pgDSN)
	if err != nil {
		t.Fatalf("connect PG: %v", err)
	}
	t.Cleanup(func() { pg.Close(context.Background()) })
	dropAll := func() {
		bg := context.Background()
		_, _ = pg.Exec(bg, "DROP PUBLICATION IF EXISTS "+pub)
		_, _ = pg.Exec(bg, "DROP TABLE IF EXISTS "+tbl)
		_, _ = pg.Exec(bg, "SELECT pg_drop_replication_slot($1) WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name=$1)", slot)
	}
	dropAll()
	t.Cleanup(dropAll)
	for _, ddl := range []string{
		fmt.Sprintf("CREATE TABLE %s (id int PRIMARY KEY, v text)", tbl),
		fmt.Sprintf("ALTER TABLE %s REPLICA IDENTITY FULL", tbl),
		fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s", pub, tbl),
	} {
		if _, err := pg.Exec(ctx, ddl); err != nil {
			t.Fatalf("exec %q: %v", ddl, err)
		}
	}

	runCtx, cancel := context.WithCancel(ctx)
	runErr := make(chan error, 1)
	go func() {
		runErr <- pgstreamrun.One(runCtx, pgstreamrun.Config{
			IndexDSN: indexDSN, ReplDSN: replDSN(pgDSN), QueryDSN: pgDSN,
			SlotName: slot, Publication: pub, ServerID: 53,
			Tables: "public." + tbl, BatchSize: 100, Checkpoint: 200 * time.Millisecond,
		})
	}()
	waitFor(t, 15*time.Second, func() bool {
		var active bool
		if err := pg.QueryRow(ctx, "SELECT active FROM pg_replication_slots WHERE slot_name=$1", slot).Scan(&active); err != nil {
			return false
		}
		return active
	}, "replication slot active")

	// A separate connection tries to drop the in-use slot — PostgreSQL refuses.
	conn, err := pgx.Connect(ctx, pgDSN)
	if err != nil {
		t.Fatalf("connect drop conn: %v", err)
	}
	_, dropErr := pgcapture.DropSlot(ctx, conn, slot)
	conn.Close(ctx)
	if dropErr == nil || !strings.Contains(dropErr.Error(), "active") {
		t.Errorf("DropSlot on an active slot = %v, want an 'active' error", dropErr)
	}

	cancel()
	<-runErr
}

// TestOne_CapturerFailureSurfaces proves the cancellation bridge in One: when the
// capturer fails on its own (here: a non-existent publication makes cap.Run return
// before streaming), One must surface that error PROMPTLY rather than hang forever
// on a never-closed events channel under a never-cancelled parent ctx (the silent
// hung-stream class). The 15s timeout is the hang detector.
func TestOne_CapturerFailureSurfaces(t *testing.T) {
	pgDSN := testutil.SkipIfNoPostgres(t)
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()

	_, dbName := testutil.CreateTestDB(t)
	indexDSN := testutil.BaseDSN() + "/" + dbName

	cfg := pgstreamrun.Config{
		IndexDSN:    indexDSN,
		ReplDSN:     replDSN(pgDSN),
		QueryDSN:    pgDSN,
		SlotName:    "bintrail_pgsr_fail_slot",
		Publication: "bintrail_pgsr_nonexistent_pub", // does not exist → cap.Run fails
		ServerID:    7,
		Checkpoint:  200 * time.Millisecond,
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- pgstreamrun.One(runCtx, cfg) }()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("One returned nil; expected the capturer's publication failure to surface")
		}
		if !strings.Contains(err.Error(), "does not exist") {
			t.Errorf("unexpected error (want publication failure): %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("One hung — a capturer failure was not surfaced (cancellation bridge missing)")
	}
}

// TestOne_MultiTable_PKScopedRecovery is the #533/#531-closure discriminator: with
// TWO published tables, a row of the FIRST table that arrives AFTER the second
// table's RelationMessage must still recover with a PK-SCOPED WHERE. A single scalar
// "current snapshot id" would stamp it with the second table's snapshot, silently
// degrading recovery to an all-columns WHERE — the multi-table blocker a single-table
// test cannot catch (pgoutput sends a RelationMessage once per relation per session,
// so the UPDATE of the first table has no fresh Relation preceding it). Also asserts
// schema_snapshots carries column_key='PRI' and a non-NULL pg_type_oid per table, and
// that no unchanged-TOAST sentinel is persisted under RI FULL.
func TestOne_MultiTable_PKScopedRecovery(t *testing.T) {
	pgDSN := testutil.SkipIfNoPostgres(t)
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()

	indexDB, dbName := testutil.CreateTestDB(t)
	indexDSN := testutil.BaseDSN() + "/" + dbName

	const slot = "bintrail_pgsr_mt"
	const pub = "bintrail_pgsr_mt_pub"
	const t1 = "pgsr_mt_t1"
	const t2 = "pgsr_mt_t2"

	pg, err := pgx.Connect(ctx, pgDSN)
	if err != nil {
		t.Fatalf("connect PG: %v", err)
	}
	t.Cleanup(func() { pg.Close(context.Background()) })

	dropAll := func() {
		bg := context.Background()
		_, _ = pg.Exec(bg, "DROP PUBLICATION IF EXISTS "+pub)
		_, _ = pg.Exec(bg, "DROP TABLE IF EXISTS "+t1)
		_, _ = pg.Exec(bg, "DROP TABLE IF EXISTS "+t2)
		_, _ = pg.Exec(bg, "SELECT pg_drop_replication_slot($1) WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name=$1)", slot)
	}
	dropAll()
	t.Cleanup(dropAll)

	mustExec := func(sqlStr string, args ...any) {
		t.Helper()
		if _, err := pg.Exec(ctx, sqlStr, args...); err != nil {
			t.Fatalf("exec %q: %v", sqlStr, err)
		}
	}
	mustExec(fmt.Sprintf("CREATE TABLE %s (id int PRIMARY KEY, label text)", t1))
	mustExec(fmt.Sprintf("CREATE TABLE %s (id int PRIMARY KEY, note text)", t2))
	mustExec(fmt.Sprintf("ALTER TABLE %s REPLICA IDENTITY FULL", t1))
	mustExec(fmt.Sprintf("ALTER TABLE %s REPLICA IDENTITY FULL", t2))
	mustExec(fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s, %s", pub, t1, t2))

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	cfg := pgstreamrun.Config{
		IndexDSN:    indexDSN,
		ReplDSN:     replDSN(pgDSN),
		QueryDSN:    pgDSN,
		SlotName:    slot,
		Publication: pub,
		ServerID:    43,
		BatchSize:   100,
		Checkpoint:  200 * time.Millisecond,
	}
	runErr := make(chan error, 1)
	go func() { runErr <- pgstreamrun.One(runCtx, cfg) }()

	waitFor(t, 15*time.Second, func() bool {
		var active bool
		if err := pg.QueryRow(ctx, "SELECT active FROM pg_replication_slots WHERE slot_name=$1", slot).Scan(&active); err != nil {
			return false
		}
		return active
	}, "replication slot active")

	// The interleave: INSERT t1, INSERT t2, then UPDATE t1 — the UPDATE t1 arrives
	// AFTER t2's RelationMessage (and with no fresh Relation t1), so a single-scalar
	// snapshot id would mis-stamp it with t2's snapshot. t2's UPDATE is the control.
	mustExec(fmt.Sprintf("INSERT INTO %s VALUES (1, 'one-before')", t1))
	mustExec(fmt.Sprintf("INSERT INTO %s VALUES (1, 'two-before')", t2))
	mustExec(fmt.Sprintf("UPDATE %s SET label='one-after' WHERE id=1", t1))
	mustExec(fmt.Sprintf("UPDATE %s SET note='two-after' WHERE id=1", t2))

	waitFor(t, 15*time.Second, func() bool {
		var n int
		if err := indexDB.QueryRow("SELECT COUNT(*) FROM binlog_events WHERE table_name IN (?, ?)", t1, t2).Scan(&n); err != nil {
			return false
		}
		return n >= 4
	}, "2 INSERT + 2 UPDATE indexed")

	cancel()
	if err := <-runErr; err != nil {
		t.Fatalf("One returned error: %v", err)
	}

	// Build the recovery generator EXACTLY as the `bintrail-pg recover` command does
	// (internal/cli/recover.go): the index db + the latest-snapshot resolver. This is
	// the load-bearing path — PK-scoped WHERE depends on resolverForRow lazy-loading
	// each row's own snapshot from the db (a nil-db or pre-empting top-level resolver
	// would silently emit all-columns WHERE). For a PG index the latest snapshot is a
	// SINGLE table, so this also proves the top-level resolver does not pre-empt the
	// other table's per-row resolution.
	latestResolver, err := metadata.NewResolver(indexDB, 0)
	if err != nil {
		t.Fatalf("NewResolver(latest): %v", err)
	}

	// For each table, the UPDATE reversal must use a PK-SCOPED WHERE (names the PK
	// column, NOT the non-PK column). t1's UPDATE is the discriminator; t2's the control.
	assertPKScoped := func(table, pkCol, nonPKCol string) {
		t.Helper()
		rows, err := query.New(indexDB).Fetch(ctx, query.Options{Schema: "public", Table: table, Order: "ASC"})
		if err != nil {
			t.Fatalf("query %s: %v", table, err)
		}
		var upd *query.ResultRow
		for i := range rows {
			if rows[i].EventType == event.EventUpdate {
				upd = &rows[i]
			}
		}
		if upd == nil {
			t.Fatalf("%s: no UPDATE event indexed", table)
		}
		if upd.SchemaVersion == 0 {
			t.Errorf("%s UPDATE has SchemaVersion 0 — not stamped with its table's snapshot id", table)
		}
		var buf bytes.Buffer
		if _, err := recovery.New(indexDB, latestResolver).GenerateSQLFromRows([]query.ResultRow{*upd}, &buf); err != nil {
			t.Fatalf("%s recovery: %v", table, err)
		}
		_, where, ok := strings.Cut(buf.String(), " WHERE ")
		if !ok {
			t.Fatalf("%s reversal has no WHERE clause: %s", table, buf.String())
		}
		if !strings.Contains(where, pkCol) {
			t.Errorf("%s reversal WHERE does not name PK %q (PK-scoped expected): %s", table, pkCol, where)
		}
		if strings.Contains(where, nonPKCol) {
			t.Errorf("%s reversal WHERE references non-PK %q — all-columns fallback, NOT PK-scoped (snapshot mis-stamped?): %s", table, nonPKCol, where)
		}
	}
	assertPKScoped(t1, "id", "label")
	assertPKScoped(t2, "id", "note")

	// EventRelation must never be persisted as a binlog_events row (the consumer
	// handles it out-of-band). A bare count of indexed rows would pass even if it
	// leaked in, so assert event_type=8 (EventRelation) has zero rows.
	var relRows int
	if err := indexDB.QueryRow(
		"SELECT COUNT(*) FROM binlog_events WHERE event_type = ?", uint8(event.EventRelation),
	).Scan(&relRows); err != nil {
		t.Fatalf("count EventRelation rows: %v", err)
	}
	if relRows != 0 {
		t.Errorf("found %d EventRelation rows persisted to binlog_events, want 0 (it must be consumed out-of-band)", relRows)
	}

	// The oracle: schema_snapshots carries the PK flag and the captured PG type OID.
	for _, table := range []string{t1, t2} {
		var columnKey string
		var oid sql.NullInt64
		if err := indexDB.QueryRow(
			`SELECT column_key, pg_type_oid FROM schema_snapshots
			 WHERE schema_name='public' AND table_name=? AND column_name='id'`, table,
		).Scan(&columnKey, &oid); err != nil {
			t.Fatalf("%s snapshot row: %v", table, err)
		}
		if columnKey != "PRI" {
			t.Errorf("%s.id column_key=%q, want PRI", table, columnKey)
		}
		if !oid.Valid || oid.Int64 == 0 {
			t.Errorf("%s.id pg_type_oid is NULL/0, want the captured int4 OID", table)
		}
	}

	// No unchanged-TOAST sentinel persisted under RI FULL.
	var sentinels int
	const marker = "%__bintrail_unchanged_toast__%"
	if err := indexDB.QueryRow(
		`SELECT COUNT(*) FROM binlog_events
		 WHERE table_name IN (?, ?) AND (row_before LIKE ? OR row_after LIKE ?)`,
		t1, t2, marker, marker,
	).Scan(&sentinels); err != nil {
		t.Fatalf("sentinel scan: %v", err)
	}
	if sentinels != 0 {
		t.Errorf("found %d rows carrying the unchanged-TOAST sentinel, want 0 under RI FULL", sentinels)
	}
}

// TestOne_PGDialect_ReversalExecutesAgainstPostgres is the #533 PG-dialect proof:
// the reversal SQL bintrail-pg generates for PG-origin rows is valid PostgreSQL — it
// EXECUTES against the live source and round-trips the values, including the escaping
// cases the MySQL dialect would break: a single quote (MySQL `\'` → PG syntax error)
// and a backslash (MySQL `\\` → silently stored doubled). It covers all three reversal
// shapes — reverse INSERT (VALUES), reverse UPDATE (SET, escaping in the SET clause),
// reverse DELETE (PK-scoped WHERE) — each executed against PostgreSQL, plus the
// dialect-selection path (recovery.DialectForIndex on a PG-flavored index).
//
// Scope asterisk: this proves validity for tables WITHOUT identity / STORED generated
// columns. A reverse INSERT into a `GENERATED ALWAYS AS IDENTITY` PK needs OVERRIDING
// SYSTEM VALUE, and STORED generated columns must be omitted — both capture-side,
// tracked in #557.
func TestOne_PGDialect_ReversalExecutesAgainstPostgres(t *testing.T) {
	pgDSN := testutil.SkipIfNoPostgres(t)
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()

	indexDB, dbName := testutil.CreateTestDB(t)
	indexDSN := testutil.BaseDSN() + "/" + dbName

	const slot = "bintrail_pgdialect"
	const pub = "bintrail_pgdialect_pub"
	const tbl = "pgdialect_t"

	pg, err := pgx.Connect(ctx, pgDSN)
	if err != nil {
		t.Fatalf("connect PG: %v", err)
	}
	t.Cleanup(func() { pg.Close(context.Background()) })

	dropAll := func() {
		bg := context.Background()
		_, _ = pg.Exec(bg, "DROP PUBLICATION IF EXISTS "+pub)
		_, _ = pg.Exec(bg, "DROP TABLE IF EXISTS "+tbl)
		_, _ = pg.Exec(bg, "SELECT pg_drop_replication_slot($1) WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name=$1)", slot)
	}
	dropAll()
	t.Cleanup(dropAll)

	mustExec := func(sqlStr string, args ...any) {
		t.Helper()
		if _, err := pg.Exec(ctx, sqlStr, args...); err != nil {
			t.Fatalf("exec %q: %v", sqlStr, err)
		}
	}
	mustExec(fmt.Sprintf(`CREATE TABLE %s (
		id int PRIMARY KEY, name text, num numeric, frac numeric, bin bytea,
		uid uuid, doc jsonb, flag boolean, ts timestamptz)`, tbl))
	mustExec(fmt.Sprintf("ALTER TABLE %s REPLICA IDENTITY FULL", tbl))
	mustExec(fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s", pub, tbl))

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	cfg := pgstreamrun.Config{
		IndexDSN: indexDSN, ReplDSN: replDSN(pgDSN), QueryDSN: pgDSN,
		SlotName: slot, Publication: pub, ServerID: 44,
		BatchSize: 100, Checkpoint: 200 * time.Millisecond,
	}
	runErr := make(chan error, 1)
	go func() { runErr <- pgstreamrun.One(runCtx, cfg) }()
	waitFor(t, 15*time.Second, func() bool {
		var active bool
		if err := pg.QueryRow(ctx, "SELECT active FROM pg_replication_slots WHERE slot_name=$1", slot).Scan(&active); err != nil {
			return false
		}
		return active
	}, "replication slot active")

	// The escaping-critical value: a single quote AND a backslash. With MySQL escaping
	// the quote would make a statement a syntax error and the backslash would be
	// silently doubled. numeric > 2^53 guards precision, frac=1.50 guards scale, jsonb
	// carries an embedded quote too.
	const trickyName = `O'Brien \ C:\back`
	const bigNum = "18446744073709551615"
	selCanonical := fmt.Sprintf("SELECT name, num::text, frac::text, encode(bin,'hex'), uid::text, doc::text, flag::text, ts::text FROM %s WHERE id=1", tbl)
	type rowText struct{ name, num, frac, bin, uid, doc, flag, ts string }
	mustExec(fmt.Sprintf("INSERT INTO %s VALUES (1, $1, $2, 1.50, '\\xdeadbeef', '11111111-1111-1111-1111-111111111111', $3, true, '2026-06-22 12:00:00+00')", tbl),
		trickyName, bigNum, `{"k": "v's"}`)
	// Capture PostgreSQL's canonical text rendering of the original row (V1, flag=true).
	var orig rowText
	if err := pg.QueryRow(ctx, selCanonical).Scan(&orig.name, &orig.num, &orig.frac, &orig.bin, &orig.uid, &orig.doc, &orig.flag, &orig.ts); err != nil {
		t.Fatalf("capture original canonical text: %v", err)
	}
	mustExec(fmt.Sprintf("UPDATE %s SET flag=false WHERE id=1", tbl)) // → V2; reverse-UPDATE must restore the full V1 before-image
	mustExec(fmt.Sprintf("DELETE FROM %s WHERE id=1", tbl))

	waitFor(t, 15*time.Second, func() bool {
		var n int
		if err := indexDB.QueryRow("SELECT COUNT(*) FROM binlog_events WHERE table_name = ?", tbl).Scan(&n); err != nil {
			return false
		}
		return n >= 3 // INSERT + UPDATE + DELETE
	}, "INSERT+UPDATE+DELETE indexed")

	cancel()
	if err := <-runErr; err != nil {
		t.Fatalf("One returned error: %v", err)
	}

	// The dialect SELECTION path `bintrail-pg recover` actually runs: a PG-flavored
	// index (pgstreamrun stamped stream_state.flavor='postgres') must resolve to
	// PostgresDialect — not the tautological pure-mapper test.
	if d := recovery.DialectForIndex(indexDB); d != recovery.PostgresDialect {
		t.Fatalf("DialectForIndex on a PG-flavored index = %v, want PostgresDialect", d)
	}

	rows, err := query.New(indexDB).Fetch(ctx, query.Options{Schema: "public", Table: tbl, Order: "ASC"})
	if err != nil {
		t.Fatalf("query Fetch: %v", err)
	}
	var insRow, updRow, delRow *query.ResultRow
	for i := range rows {
		switch rows[i].EventType {
		case event.EventInsert:
			insRow = &rows[i]
		case event.EventUpdate:
			updRow = &rows[i]
		case event.EventDelete:
			delRow = &rows[i]
		}
	}
	if insRow == nil || updRow == nil || delRow == nil {
		t.Fatalf("expected INSERT+UPDATE+DELETE events, got %d rows", len(rows))
	}

	resolver, err := metadata.NewResolver(indexDB, 0)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}
	genPG := func(r query.ResultRow) string {
		var buf bytes.Buffer
		if _, err := recovery.NewForDialect(indexDB, resolver, recovery.PostgresDialect).
			GenerateSQLFromRows([]query.ResultRow{r}, &buf); err != nil {
			t.Fatalf("GenerateSQLFromRows: %v", err)
		}
		return buf.String()
	}

	// ── Reverse the DELETE → a reverse INSERT (VALUES, all scary types). The row is
	// gone; executing the reversal against live PostgreSQL must reconstruct the
	// pre-DELETE row (V2: flag=false). The escaping-critical name comes through VALUES.
	// A MySQL-dialect reversal would NOT execute here. ──
	reverseInsert := genPG(*delRow)
	if strings.Contains(reverseInsert, "`") {
		t.Errorf("PG reversal contains MySQL backticks:\n%s", reverseInsert)
	}
	if !strings.Contains(reverseInsert, "SET LOCAL standard_conforming_strings = on;") {
		t.Errorf("PG script missing the standard_conforming_strings guard:\n%s", reverseInsert)
	}
	if _, err := pg.Exec(ctx, reverseInsert); err != nil {
		t.Fatalf("PG-dialect reverse INSERT failed to execute against PostgreSQL: %v\nSQL:\n%s", err, reverseInsert)
	}
	var afterIns rowText
	if err := pg.QueryRow(ctx, selCanonical).Scan(&afterIns.name, &afterIns.num, &afterIns.frac, &afterIns.bin, &afterIns.uid, &afterIns.doc, &afterIns.flag, &afterIns.ts); err != nil {
		t.Fatalf("re-select after reverse INSERT: %v", err)
	}
	if afterIns.name != trickyName {
		t.Errorf("reverse INSERT name = %q, want exactly %q (quote/backslash escaping bug in VALUES)", afterIns.name, trickyName)
	}
	if afterIns.flag != "false" {
		t.Errorf("reverse INSERT reconstructed flag = %q, want \"false\" (the pre-DELETE before-image V2)", afterIns.flag)
	}

	// ── Reverse the UPDATE → a reverse UPDATE (SET = full V1 before-image, PK-scoped
	// WHERE). The SET clause carries the escaping-critical name, the >2^53 numeric and
	// the 1.50 scale; executing it must restore the original row exactly. ──
	reverseUpdate := genPG(*updRow)
	if _, err := pg.Exec(ctx, reverseUpdate); err != nil {
		t.Fatalf("PG-dialect reverse UPDATE failed to execute against PostgreSQL: %v\nSQL:\n%s", err, reverseUpdate)
	}
	var afterUpd rowText
	if err := pg.QueryRow(ctx, selCanonical).Scan(&afterUpd.name, &afterUpd.num, &afterUpd.frac, &afterUpd.bin, &afterUpd.uid, &afterUpd.doc, &afterUpd.flag, &afterUpd.ts); err != nil {
		t.Fatalf("re-select after reverse UPDATE: %v", err)
	}
	if afterUpd != orig {
		t.Errorf("reverse UPDATE round-trip mismatch:\n got  %+v\n want %+v", afterUpd, orig)
	}
	if afterUpd.name != trickyName {
		t.Errorf("reverse UPDATE SET name = %q, want exactly %q (escaping in the SET clause)", afterUpd.name, trickyName)
	}
	if afterUpd.num != bigNum || afterUpd.frac != "1.50" {
		t.Errorf("reverse UPDATE numeric/scale = (%q,%q), want (%q,\"1.50\")", afterUpd.num, afterUpd.frac, bigNum)
	}
	// Typed boolean read: the canonical SELECT renders bool as 't' text on both sides,
	// so a typed read is what actually proves the restored value (not the text).
	var flagBool bool
	if err := pg.QueryRow(ctx, fmt.Sprintf("SELECT flag FROM %s WHERE id=1", tbl)).Scan(&flagBool); err != nil {
		t.Fatalf("typed bool read: %v", err)
	}
	if !flagBool {
		t.Error("reverse UPDATE restored flag=false, want true (typed read)")
	}

	// ── Reverse the INSERT → a reverse DELETE (PK-scoped double-quoted WHERE).
	// Executing it removes the row again — proving the PG WHERE dialect runs. ──
	reverseDelete := genPG(*insRow)
	if _, err := pg.Exec(ctx, reverseDelete); err != nil {
		t.Fatalf("PG-dialect reverse DELETE failed to execute against PostgreSQL: %v\nSQL:\n%s", err, reverseDelete)
	}
	var cnt int
	if err := pg.QueryRow(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE id=1", tbl)).Scan(&cnt); err != nil {
		t.Fatalf("count after reverse DELETE: %v", err)
	}
	if cnt != 0 {
		t.Errorf("reverse DELETE left %d rows, want 0 (PK-scoped WHERE did not match)", cnt)
	}
}

// TestOne_PGTypeRoundTripMatrix is the #533 type-fidelity audit: for a broad set of
// PostgreSQL types, a value flows PG → pgoutput → index → recover → EXECUTE the
// reverse-INSERT against live PostgreSQL, and the column's canonical ::text rendering
// must round-trip byte-for-byte (no silent precision/encoding loss). Each type is its
// own table (id PK + val) so a per-type failure is isolated; one pgstreamrun session
// captures them all. The documented type-support matrix (docs/postgres.md) is derived
// from what this test proves — repro > cita.
func TestOne_PGTypeRoundTripMatrix(t *testing.T) {
	pgDSN := testutil.SkipIfNoPostgres(t)
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()

	indexDB, dbName := testutil.CreateTestDB(t)
	indexDSN := testutil.BaseDSN() + "/" + dbName

	const slot = "bintrail_pgtypes"
	const pub = "bintrail_pgtypes_pub"

	pg, err := pgx.Connect(ctx, pgDSN)
	if err != nil {
		t.Fatalf("connect PG: %v", err)
	}
	t.Cleanup(func() { pg.Close(context.Background()) })

	// The shared type matrix (pgTypeMatrixCases, typematrix_fold_integration_
	// test.go): name → (column type DDL, value SQL as it appears in VALUES),
	// scary-first. This test uses valSQL only; the fold test also exercises
	// each case's second literal (updSQL).
	cases := pgTypeMatrixCases

	tblOf := func(name string) string { return "pgtype_" + name }
	dropAll := func() {
		bg := context.Background()
		_, _ = pg.Exec(bg, "DROP PUBLICATION IF EXISTS "+pub)
		for _, c := range cases {
			_, _ = pg.Exec(bg, "DROP TABLE IF EXISTS "+tblOf(c.name))
		}
		_, _ = pg.Exec(bg, "DROP TYPE IF EXISTS mood")
		_, _ = pg.Exec(bg, "DROP TYPE IF EXISTS pgt_pair")
		_, _ = pg.Exec(bg, "SELECT pg_drop_replication_slot($1) WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name=$1)", slot)
	}
	dropAll()
	t.Cleanup(dropAll)

	mustExec := func(sqlStr string) {
		t.Helper()
		if _, err := pg.Exec(ctx, sqlStr); err != nil {
			t.Fatalf("exec %q: %v", sqlStr, err)
		}
	}
	mustExec("CREATE TYPE mood AS ENUM ('happy','sad')")
	mustExec("CREATE TYPE pgt_pair AS (a int, b text)")
	tbls := make([]string, len(cases))
	for i, c := range cases {
		tbl := tblOf(c.name)
		tbls[i] = tbl
		mustExec(fmt.Sprintf("CREATE TABLE %s (id int PRIMARY KEY, val %s)", tbl, c.typeDDL))
		mustExec(fmt.Sprintf("ALTER TABLE %s REPLICA IDENTITY FULL", tbl))
	}
	mustExec("CREATE PUBLICATION " + pub + " FOR TABLE " + strings.Join(tbls, ", "))

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	cfg := pgstreamrun.Config{
		IndexDSN: indexDSN, ReplDSN: replDSN(pgDSN), QueryDSN: pgDSN,
		SlotName: slot, Publication: pub, ServerID: 45,
		BatchSize: 200, Checkpoint: 200 * time.Millisecond,
	}
	runErr := make(chan error, 1)
	go func() { runErr <- pgstreamrun.One(runCtx, cfg) }()
	waitFor(t, 15*time.Second, func() bool {
		var active bool
		if err := pg.QueryRow(ctx, "SELECT active FROM pg_replication_slots WHERE slot_name=$1", slot).Scan(&active); err != nil {
			return false
		}
		return active
	}, "replication slot active")

	// INSERT then DELETE one row per type; capture the original canonical ::text first.
	orig := make(map[string]string, len(cases))
	for _, c := range cases {
		tbl := tblOf(c.name)
		mustExec(fmt.Sprintf("INSERT INTO %s (id, val) VALUES (1, %s)", tbl, c.valSQL))
		var got sql.NullString
		if err := pg.QueryRow(ctx, fmt.Sprintf("SELECT val::text FROM %s WHERE id=1", tbl)).Scan(&got); err != nil {
			t.Fatalf("%s: capture original: %v", c.name, err)
		}
		orig[c.name] = got.String
		mustExec(fmt.Sprintf("DELETE FROM %s WHERE id=1", tbl))
	}

	wantEvents := 2 * len(cases) // INSERT + DELETE per table
	waitFor(t, 30*time.Second, func() bool {
		var n int
		if err := indexDB.QueryRow("SELECT COUNT(*) FROM binlog_events WHERE schema_name='public'").Scan(&n); err != nil {
			return false
		}
		return n >= wantEvents
	}, "all type events indexed")

	cancel()
	if err := <-runErr; err != nil {
		t.Fatalf("One returned error: %v", err)
	}

	resolver, err := metadata.NewResolver(indexDB, 0)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			tbl := tblOf(c.name)
			rows, err := query.New(indexDB).Fetch(ctx, query.Options{Schema: "public", Table: tbl, Order: "ASC"})
			if err != nil {
				t.Fatalf("fetch: %v", err)
			}
			var del *query.ResultRow
			for i := range rows {
				if rows[i].EventType == event.EventDelete {
					del = &rows[i]
				}
			}
			if del == nil {
				t.Fatalf("no DELETE event indexed for %s", tbl)
			}
			var buf bytes.Buffer
			if _, err := recovery.NewForDialect(indexDB, resolver, recovery.PostgresDialect).
				GenerateSQLFromRows([]query.ResultRow{*del}, &buf); err != nil {
				t.Fatalf("generate: %v", err)
			}
			if _, err := pg.Exec(ctx, buf.String()); err != nil {
				t.Fatalf("reverse INSERT did not execute against PostgreSQL: %v\nSQL:\n%s", err, buf.String())
			}
			var got sql.NullString
			if err := pg.QueryRow(ctx, fmt.Sprintf("SELECT val::text FROM %s WHERE id=1", tbl)).Scan(&got); err != nil {
				t.Fatalf("re-select: %v", err)
			}
			if got.String != orig[c.name] {
				t.Errorf("round-trip mismatch (%s):\n got  %q\n want %q", c.typeDDL, got.String, orig[c.name])
			}
		})
	}
}

// TestOne_PGIdentityGenerated_Recovery is the #557 proof: recovery for a table with a
// GENERATED ALWAYS AS IDENTITY primary key and for one with a STORED generated column
// EXECUTES against live PostgreSQL through all three reversal shapes. Without the fix,
// the reverse-INSERT errors ("cannot insert a non-DEFAULT value" / "cannot insert into
// generated column") and the reverse-UPDATE errors ("can only be updated to DEFAULT").
// It also empirically settles whether pgoutput carries the STORED generated column.
func TestOne_PGIdentityGenerated_Recovery(t *testing.T) {
	pgDSN := testutil.SkipIfNoPostgres(t)
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()

	indexDB, dbName := testutil.CreateTestDB(t)
	indexDSN := testutil.BaseDSN() + "/" + dbName

	const slot = "bintrail_pgidgen"
	const pub = "bintrail_pgidgen_pub"
	const tIda = "pgidgen_ida" // GENERATED ALWAYS AS IDENTITY PK
	const tBd = "pgidgen_bd"   // GENERATED BY DEFAULT AS IDENTITY PK
	const tGen = "pgidgen_gen" // STORED generated column

	pg, err := pgx.Connect(ctx, pgDSN)
	if err != nil {
		t.Fatalf("connect PG: %v", err)
	}
	t.Cleanup(func() { pg.Close(context.Background()) })

	dropAll := func() {
		bg := context.Background()
		_, _ = pg.Exec(bg, "DROP PUBLICATION IF EXISTS "+pub)
		_, _ = pg.Exec(bg, "DROP TABLE IF EXISTS "+tIda)
		_, _ = pg.Exec(bg, "DROP TABLE IF EXISTS "+tBd)
		_, _ = pg.Exec(bg, "DROP TABLE IF EXISTS "+tGen)
		_, _ = pg.Exec(bg, "SELECT pg_drop_replication_slot($1) WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name=$1)", slot)
	}
	dropAll()
	t.Cleanup(dropAll)

	mustExec := func(s string) {
		t.Helper()
		if _, err := pg.Exec(ctx, s); err != nil {
			t.Fatalf("exec %q: %v", s, err)
		}
	}
	mustExec(fmt.Sprintf("CREATE TABLE %s (id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY, v text)", tIda))
	mustExec(fmt.Sprintf("CREATE TABLE %s (id int GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, v text)", tBd))
	mustExec(fmt.Sprintf("CREATE TABLE %s (id int PRIMARY KEY, v text, vlen int GENERATED ALWAYS AS (length(v)) STORED)", tGen))
	mustExec(fmt.Sprintf("ALTER TABLE %s REPLICA IDENTITY FULL", tIda))
	mustExec(fmt.Sprintf("ALTER TABLE %s REPLICA IDENTITY FULL", tBd))
	mustExec(fmt.Sprintf("ALTER TABLE %s REPLICA IDENTITY FULL", tGen))
	mustExec(fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s, %s, %s", pub, tIda, tBd, tGen))

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	cfg := pgstreamrun.Config{
		IndexDSN: indexDSN, ReplDSN: replDSN(pgDSN), QueryDSN: pgDSN,
		SlotName: slot, Publication: pub, ServerID: 46,
		BatchSize: 100, Checkpoint: 200 * time.Millisecond,
	}
	runErr := make(chan error, 1)
	go func() { runErr <- pgstreamrun.One(runCtx, cfg) }()
	waitFor(t, 15*time.Second, func() bool {
		var a bool
		if err := pg.QueryRow(ctx, "SELECT active FROM pg_replication_slots WHERE slot_name=$1", slot).Scan(&a); err != nil {
			return false
		}
		return a
	}, "replication slot active")

	// ida: identity-ALWAYS PK. INSERT (id auto-assigned), UPDATE v, DELETE.
	var idaID int
	if err := pg.QueryRow(ctx, fmt.Sprintf("INSERT INTO %s (v) VALUES ('orig') RETURNING id", tIda)).Scan(&idaID); err != nil {
		t.Fatalf("insert ida: %v", err)
	}
	mustExec(fmt.Sprintf("UPDATE %s SET v='changed' WHERE id=%d", tIda, idaID))
	mustExec(fmt.Sprintf("DELETE FROM %s WHERE id=%d", tIda, idaID))

	// bd: identity BY DEFAULT PK — the more common variant (explicit inserts allowed).
	// INSERT (id auto-assigned), UPDATE v, DELETE.
	var bdID int
	if err := pg.QueryRow(ctx, fmt.Sprintf("INSERT INTO %s (v) VALUES ('orig') RETURNING id", tBd)).Scan(&bdID); err != nil {
		t.Fatalf("insert bd: %v", err)
	}
	mustExec(fmt.Sprintf("UPDATE %s SET v='changed' WHERE id=%d", tBd, bdID))
	mustExec(fmt.Sprintf("DELETE FROM %s WHERE id=%d", tBd, bdID))

	// gen: STORED generated col (vlen = length(v)). INSERT, UPDATE v, DELETE.
	mustExec(fmt.Sprintf("INSERT INTO %s (id, v) VALUES (1, 'hello')", tGen))
	mustExec(fmt.Sprintf("UPDATE %s SET v='hi' WHERE id=1", tGen))
	mustExec(fmt.Sprintf("DELETE FROM %s WHERE id=1", tGen))

	waitFor(t, 20*time.Second, func() bool {
		var n int
		if err := indexDB.QueryRow("SELECT COUNT(*) FROM binlog_events WHERE schema_name='public'").Scan(&n); err != nil {
			return false
		}
		return n >= 9 // 3 events per table × 3 tables
	}, "all identity/generated events indexed")

	cancel()
	if err := <-runErr; err != nil {
		t.Fatalf("One returned error: %v", err)
	}

	resolver, err := metadata.NewResolver(indexDB, 0)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}
	// reverseAndExec generates the PG-dialect reversal for one event and executes it
	// against live PostgreSQL — a #557 regression (missing OVERRIDING SYSTEM VALUE, or
	// a SET on an identity/generated column) makes pg.Exec fail here.
	reverseAndExec := func(table string, typ event.EventType) {
		t.Helper()
		rows, err := query.New(indexDB).Fetch(ctx, query.Options{Schema: "public", Table: table, Order: "ASC"})
		if err != nil {
			t.Fatalf("%s fetch: %v", table, err)
		}
		var ev *query.ResultRow
		for i := range rows {
			if rows[i].EventType == typ {
				ev = &rows[i]
			}
		}
		if ev == nil {
			t.Fatalf("%s: no %v event indexed", table, typ)
		}
		var buf bytes.Buffer
		if _, err := recovery.NewForDialect(indexDB, resolver, recovery.PostgresDialect).
			GenerateSQLFromRows([]query.ResultRow{*ev}, &buf); err != nil {
			t.Fatalf("%s generate: %v", table, err)
		}
		if _, err := pg.Exec(ctx, buf.String()); err != nil {
			t.Fatalf("%s reverse %v failed to execute against PostgreSQL: %v\nSQL:\n%s", table, typ, err, buf.String())
		}
	}

	// ── ida: reverse DELETE → INSERT must emit OVERRIDING SYSTEM VALUE + restore id ──
	reverseAndExec(tIda, event.EventDelete)
	var gotID int
	var gotV string
	if err := pg.QueryRow(ctx, fmt.Sprintf("SELECT id, v FROM %s WHERE id=%d", tIda, idaID)).Scan(&gotID, &gotV); err != nil {
		t.Fatalf("ida after reverse INSERT: %v", err)
	}
	if gotID != idaID || gotV != "changed" {
		t.Errorf("ida reverse INSERT: got (id=%d, v=%q), want (id=%d, v=changed)", gotID, gotV, idaID)
	}
	// reverse UPDATE → SET must OMIT the identity-ALWAYS id, restoring v.
	reverseAndExec(tIda, event.EventUpdate)
	if err := pg.QueryRow(ctx, fmt.Sprintf("SELECT v FROM %s WHERE id=%d", tIda, idaID)).Scan(&gotV); err != nil {
		t.Fatalf("ida after reverse UPDATE: %v", err)
	}
	if gotV != "orig" {
		t.Errorf("ida reverse UPDATE: v=%q, want orig", gotV)
	}

	// ── bd: reverse DELETE → INSERT (OVERRIDING is a no-op on BY DEFAULT) restores id ──
	reverseAndExec(tBd, event.EventDelete)
	if err := pg.QueryRow(ctx, fmt.Sprintf("SELECT id, v FROM %s WHERE id=%d", tBd, bdID)).Scan(&gotID, &gotV); err != nil {
		t.Fatalf("bd after reverse INSERT: %v", err)
	}
	if gotID != bdID || gotV != "changed" {
		t.Errorf("bd reverse INSERT: got (id=%d, v=%q), want (id=%d, v=changed)", gotID, gotV, bdID)
	}
	// reverse UPDATE → SET KEEPS the BY DEFAULT id (allowed, and needed for PK changes).
	reverseAndExec(tBd, event.EventUpdate)
	if err := pg.QueryRow(ctx, fmt.Sprintf("SELECT v FROM %s WHERE id=%d", tBd, bdID)).Scan(&gotV); err != nil {
		t.Fatalf("bd after reverse UPDATE: %v", err)
	}
	if gotV != "orig" {
		t.Errorf("bd reverse UPDATE: v=%q, want orig", gotV)
	}

	// ── gen: reverse DELETE → INSERT must OMIT the generated col; vlen recomputes ──
	reverseAndExec(tGen, event.EventDelete)
	var gv string
	var gvlen int
	if err := pg.QueryRow(ctx, fmt.Sprintf("SELECT v, vlen FROM %s WHERE id=1", tGen)).Scan(&gv, &gvlen); err != nil {
		t.Fatalf("gen after reverse INSERT: %v", err)
	}
	if gv != "hi" || gvlen != 2 {
		t.Errorf("gen reverse INSERT: got (v=%q, vlen=%d), want (hi, 2)", gv, gvlen)
	}
	// reverse UPDATE → SET must OMIT the generated col; v restored, vlen recomputes.
	reverseAndExec(tGen, event.EventUpdate)
	if err := pg.QueryRow(ctx, fmt.Sprintf("SELECT v, vlen FROM %s WHERE id=1", tGen)).Scan(&gv, &gvlen); err != nil {
		t.Fatalf("gen after reverse UPDATE: %v", err)
	}
	if gv != "hello" || gvlen != 5 {
		t.Errorf("gen reverse UPDATE: got (v=%q, vlen=%d), want (hello, 5)", gv, gvlen)
	}

	// Empirically settle whether pgoutput carries the STORED generated column (pre-PG18
	// it does not). Either way recovery is correct — the omit handles a present column,
	// and an absent one is naturally omitted.
	var rb sql.NullString
	if err := indexDB.QueryRow("SELECT row_before FROM binlog_events WHERE table_name=? AND event_type=? ORDER BY event_id LIMIT 1",
		tGen, uint8(event.EventDelete)).Scan(&rb); err == nil {
		t.Logf("STORED generated column 'vlen' present in captured stream: %v", strings.Contains(rb.String, `"vlen"`))
	}
}

// ── helpers ──

func replDSN(base string) string {
	if strings.Contains(base, "?") {
		return base + "&replication=database"
	}
	return base + "?replication=database"
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func lenOf(v any) int {
	if s, ok := v.(string); ok {
		return len(s)
	}
	return -1
}
