//go:build integration

package pgcapture_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/pgcapture"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestCapturer_Integration drives the real Capturer against a live PostgreSQL,
// reproducing the spike end-to-end: an UPDATE that does not touch an out-of-line
// TOASTed column under REPLICA IDENTITY FULL must carry the real value in BOTH
// images (Option B), and confirmed_flush_lsn must advance ONLY after AckCommitted.
//
// Requires BINTRAIL_TEST_PG_DSN pointing at a PostgreSQL with wal_level=logical and
// a REPLICATION role (e.g. postgres://postgres:testpg@localhost:15533/pgtest). The
// MySQL CI jobs do not set it, so this skips cleanly there (Postgres CI is #534).
func TestCapturer_Integration(t *testing.T) {
	baseDSN := testutil.SkipIfNoPostgres(t)
	ctx := context.Background()

	const slot = "bintrail_pgcap_it"
	const pub = "bintrail_pgcap_it_pub"
	const tbl = "pgcap_it_t"
	const bigSize = 6000

	setup, err := pgx.Connect(ctx, baseDSN)
	if err != nil {
		t.Fatalf("connect setup conn: %v", err)
	}
	t.Cleanup(func() { setup.Close(context.Background()) })

	dropAll := func(c *pgx.Conn) {
		bg := context.Background()
		_, _ = c.Exec(bg, fmt.Sprintf("DROP PUBLICATION IF EXISTS %s", pub))
		_, _ = c.Exec(bg, fmt.Sprintf("DROP TABLE IF EXISTS %s", tbl))
		// Drop the slot last (it must be inactive — the Run goroutine is cancelled by
		// the time Cleanup runs). A leaked permanent slot retains WAL and burns a
		// max_replication_slots entry on every rerun.
		_, _ = c.Exec(bg, "SELECT pg_drop_replication_slot($1) WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name=$1)", slot)
	}
	dropAll(setup)
	t.Cleanup(func() { dropAll(setup) })

	mustExec := func(sql string, args ...any) {
		t.Helper()
		if _, err := setup.Exec(ctx, sql, args...); err != nil {
			t.Fatalf("exec %q: %v", sql, err)
		}
	}
	mustExec(fmt.Sprintf("CREATE TABLE %s (id int PRIMARY KEY, small_col text, big_col text)", tbl))
	mustExec(fmt.Sprintf("ALTER TABLE %s ALTER COLUMN big_col SET STORAGE EXTERNAL", tbl)) // no compression → forced out-of-line
	mustExec(fmt.Sprintf("ALTER TABLE %s REPLICA IDENTITY FULL", tbl))
	mustExec(fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s", pub, tbl))

	bigVal := strings.Repeat("X", bigSize)
	mustExec(fmt.Sprintf("INSERT INTO %s VALUES (1, 'orig', $1)", tbl), bigVal) // baseline (pre-slot, not streamed)

	// Prove big_col is genuinely out-of-line, else the 'u' path is never exercised
	// and the whole test could pass green without testing anything (verify finding).
	var colSize int
	if err := setup.QueryRow(ctx, fmt.Sprintf("SELECT pg_column_size(big_col) FROM %s WHERE id=1", tbl)).Scan(&colSize); err != nil {
		t.Fatalf("pg_column_size: %v", err)
	}
	if colSize < 2000 {
		t.Fatalf("big_col size %d is below the TOAST threshold — not out-of-line, TOAST path not exercised", colSize)
	}
	var toastSize int64
	if err := setup.QueryRow(ctx, "SELECT pg_relation_size(reltoastrelid) FROM pg_class WHERE oid = $1::regclass", "public."+tbl).Scan(&toastSize); err != nil {
		t.Fatalf("toast relation size: %v", err)
	}
	if toastSize == 0 {
		t.Fatal("TOAST relation is empty — big_col stored inline, TOAST path not exercised")
	}

	// Start the capturer (it creates the slot from its ConsistentPoint).
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	cap := pgcapture.New(pgcapture.Config{
		ReplDSN:         replDSN(baseDSN),
		QueryDSN:        baseDSN,
		SlotName:        slot,
		Publication:     pub,
		Filters:         event.Filters{Tables: map[string]bool{"public." + tbl: true}},
		StandbyInterval: 200 * time.Millisecond, // low so the never-ahead poll is fast, not flaky
	})
	events := make(chan event.Event, 64)
	runErr := make(chan error, 1)
	go func() { runErr <- cap.Run(runCtx, events) }()

	// Wait until the slot is active (Run has started replication) before any DML, so
	// every change below is within the captured range. `active` alone is not enough:
	// a slot can be active=true while confirmed_flush_lsn is still NULL (the slot is
	// acquired but confirmed_flush_lsn is not yet visible — it is populated from the
	// consumer's standby feedback, initially the slot's consistent point), and
	// confirmedFlush below
	// scans confirmed_flush_lsn::text into a string — NULL then errors "cannot scan
	// NULL into *string". That NULL window's timing is PG-version-sensitive (it bit
	// PG 15). Gate on a non-NULL confirmed_flush_lsn so the baseline is the real
	// consistent point.
	waitFor(t, 10*time.Second, func() bool {
		var active, hasFlush bool
		if err := setup.QueryRow(ctx,
			"SELECT active, confirmed_flush_lsn IS NOT NULL FROM pg_replication_slots WHERE slot_name=$1",
			slot).Scan(&active, &hasFlush); err != nil {
			return false
		}
		return active && hasFlush
	}, "replication slot active with a confirmed_flush_lsn")

	baseline := confirmedFlush(t, setup, slot) // ≈ the slot's consistent point

	// The discriminating DML: UPDATE leaves big_col untouched.
	mustExec(fmt.Sprintf("UPDATE %s SET small_col='changed' WHERE id=1", tbl))
	mustExec(fmt.Sprintf("INSERT INTO %s VALUES (2, 'second', $1)", tbl), strings.Repeat("Y", bigSize))
	mustExec(fmt.Sprintf("DELETE FROM %s WHERE id=2", tbl))

	// Collect until we have all three row events (commits interleaved).
	rows, commits := collect(t, events, 3, 15*time.Second)

	// ── UPDATE: the spike's headline TOAST round-trip ──
	upd := pick(t, rows, event.EventUpdate)
	if got := upd.RowBefore["big_col"]; got != bigVal {
		t.Errorf("UPDATE RowBefore[big_col]: got %d-byte %T, want the real %d-byte value", lenOf(got), got, bigSize)
	}
	if got := upd.RowAfter["big_col"]; got != bigVal {
		t.Errorf("UPDATE RowAfter[big_col]: got %v (%T), want the real value resolved from the before-image (Option B)", trunc(got), got)
	}
	if changed := event.ChangedColumns(upd.RowBefore, upd.RowAfter); contains(changed, "big_col") {
		t.Errorf("changed_columns wrongly includes the untouched TOAST column: %v", changed)
	}
	if upd.PKValues != "1" {
		t.Errorf("UPDATE PKValues=%q, want 1", upd.PKValues)
	}
	if strings.Contains(upd.PKValues, pgcapture.UnchangedToastKey) {
		t.Errorf("UPDATE PKValues leaked the unchanged-TOAST marker: %q", upd.PKValues)
	}

	// ── INSERT / DELETE ──
	ins := pick(t, rows, event.EventInsert)
	if ins.PKValues != "2" || ins.RowAfter["small_col"] != "second" {
		t.Errorf("INSERT event wrong: pk=%q after=%v", ins.PKValues, ins.RowAfter)
	}
	del := pick(t, rows, event.EventDelete)
	if del.PKValues != "2" || del.RowBefore["small_col"] != "second" {
		t.Errorf("DELETE event wrong: pk=%q before=%v", del.PKValues, del.RowBefore)
	}

	// ── never-ahead: confirmed_flush must NOT advance without an ack ──
	// Give the capturer time to send several standby updates (interval 200ms).
	time.Sleep(700 * time.Millisecond)
	if cur := confirmedFlush(t, setup, slot); cur > baseline {
		t.Errorf("confirmed_flush_lsn advanced (%s → %s) WITHOUT AckCommitted — never-ahead violated", baseline, cur)
	}

	// ── ack the last commit → confirmed_flush advances to it ──
	if len(commits) == 0 {
		t.Fatal("no EventCommit collected")
	}
	lastCommit, err := pgcapture.DecodeLSN(commits[len(commits)-1].GTID)
	if err != nil {
		t.Fatalf("decode commit LSN %q: %v", commits[len(commits)-1].GTID, err)
	}
	cap.AckCommitted(uint64(lastCommit))
	waitFor(t, 5*time.Second, func() bool {
		return confirmedFlush(t, setup, slot) >= lastCommit
	}, "confirmed_flush_lsn to advance to the acked commit LSN")

	// Clean shutdown returns nil.
	cancel()
	select {
	case err := <-runErr:
		if err != nil {
			t.Errorf("Run returned non-nil on graceful cancel: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Error("Run did not return promptly after ctx cancel")
	}
}

// TestCapturer_PublicationCoverageFailsLoud proves the validate-don't-create gate:
// a publication that omits a requested table fails loud rather than silently
// streaming zero events for it.
// TestCapturer_NonFullReplicaIdentityFailsLoud proves the #531-A RI-FULL validator:
// a publication table left at the default replica identity (not FULL) makes Run fail
// loud — its before-images would be partial (an unchanged out-of-line TOAST value
// lost), which is unrecoverable.
func TestCapturer_NonFullReplicaIdentityFailsLoud(t *testing.T) {
	baseDSN := testutil.SkipIfNoPostgres(t)
	ctx := context.Background()
	const pub = "bintrail_pgcap_ri_pub"
	const tbl = "pgcap_ri_t"

	setup, err := pgx.Connect(ctx, baseDSN)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { setup.Close(context.Background()) })
	cleanup := func() {
		bg := context.Background()
		_, _ = setup.Exec(bg, "DROP PUBLICATION IF EXISTS "+pub)
		_, _ = setup.Exec(bg, "DROP TABLE IF EXISTS "+tbl)
	}
	cleanup()
	t.Cleanup(cleanup)

	// Default replica identity (NOT FULL) on purpose.
	if _, err := setup.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (id int PRIMARY KEY, body text)", tbl)); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := setup.Exec(ctx, fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s", pub, tbl)); err != nil {
		t.Fatalf("create publication: %v", err)
	}

	cap := pgcapture.New(pgcapture.Config{
		ReplDSN:     replDSN(baseDSN),
		QueryDSN:    baseDSN,
		SlotName:    "bintrail_pgcap_ri_slot",
		Publication: pub,
	})
	err = cap.Run(ctx, make(chan event.Event, 1))
	if err == nil {
		t.Fatal("expected Run to fail loud on a table that is not at REPLICA IDENTITY FULL")
	}
	if !strings.Contains(err.Error(), "REPLICA IDENTITY FULL") {
		t.Errorf("unexpected error (want RI-FULL failure): %v", err)
	}
}

func TestCapturer_PublicationCoverageFailsLoud(t *testing.T) {
	baseDSN := testutil.SkipIfNoPostgres(t)
	ctx := context.Background()
	const pub = "bintrail_pgcap_cov_pub"
	const tblA = "pgcap_cov_a"

	setup, err := pgx.Connect(ctx, baseDSN)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { setup.Close(context.Background()) })
	cleanup := func() {
		bg := context.Background()
		_, _ = setup.Exec(bg, "DROP PUBLICATION IF EXISTS "+pub)
		_, _ = setup.Exec(bg, "DROP TABLE IF EXISTS "+tblA)
	}
	cleanup()
	t.Cleanup(cleanup)

	if _, err := setup.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (id int PRIMARY KEY)", tblA)); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := setup.Exec(ctx, fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s", pub, tblA)); err != nil {
		t.Fatalf("create publication: %v", err)
	}

	cap := pgcapture.New(pgcapture.Config{
		ReplDSN:     replDSN(baseDSN),
		QueryDSN:    baseDSN,
		SlotName:    "bintrail_pgcap_cov_slot",
		Publication: pub,
		// Request a table the publication does NOT cover.
		Filters: event.Filters{Tables: map[string]bool{"public.not_published": true}},
	})
	err = cap.Run(ctx, make(chan event.Event, 1))
	if err == nil {
		t.Fatal("expected Run to fail loud on a publication that omits a requested table")
	}
	if !strings.Contains(err.Error(), "does not cover") {
		t.Errorf("unexpected error (want coverage failure): %v", err)
	}
}

// TestCapturer_ResumeMissingSlotFailsLoud proves the resume guard (#534): when the
// consumer resumes from a saved checkpoint (ExpectExistingSlot) but the slot is gone
// (here: never created), Run fails loud rather than silently creating a fresh slot
// from a new ConsistentPoint — which would skip the WAL since the checkpoint.
func TestCapturer_ResumeMissingSlotFailsLoud(t *testing.T) {
	baseDSN := testutil.SkipIfNoPostgres(t)
	ctx := context.Background()
	const pub = "bintrail_pgcap_resume_pub"
	const tbl = "pgcap_resume_t"

	setup, err := pgx.Connect(ctx, baseDSN)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { setup.Close(context.Background()) })
	cleanup := func() {
		bg := context.Background()
		_, _ = setup.Exec(bg, "DROP PUBLICATION IF EXISTS "+pub)
		_, _ = setup.Exec(bg, "DROP TABLE IF EXISTS "+tbl)
	}
	cleanup()
	t.Cleanup(cleanup)

	if _, err := setup.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (id int PRIMARY KEY)", tbl)); err != nil {
		t.Fatalf("create table: %v", err)
	}
	// RI FULL so the validator passes and Run reaches the slot-resume guard (the
	// behavior under test here), not the RI check.
	if _, err := setup.Exec(ctx, fmt.Sprintf("ALTER TABLE %s REPLICA IDENTITY FULL", tbl)); err != nil {
		t.Fatalf("alter replica identity: %v", err)
	}
	if _, err := setup.Exec(ctx, fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s", pub, tbl)); err != nil {
		t.Fatalf("create publication: %v", err)
	}

	cap := pgcapture.New(pgcapture.Config{
		ReplDSN:            replDSN(baseDSN),
		QueryDSN:           baseDSN,
		SlotName:           "bintrail_pgcap_resume_absent_slot", // never created
		Publication:        pub,
		StartLSN:           0x1000, // a non-zero saved checkpoint
		ExpectExistingSlot: true,   // resuming
	})
	err = cap.Run(ctx, make(chan event.Event, 1))
	if err == nil {
		t.Fatal("expected Run to fail loud when resuming but the slot is gone")
	}
	if !strings.Contains(err.Error(), "no longer exists") {
		t.Errorf("unexpected error (want missing-slot resume failure): %v", err)
	}
}

// TestCapturer_CompositePKOrder proves PKValues uses TABLE-ORDINAL (column
// declaration) order, matching the offline resolver's metadata.PKColumnMetas — NOT
// PostgreSQL's primary-key KEY order. The table's columns are (a, b, c) but its
// PRIMARY KEY is (b, a); the catalog returns the key in (b, a) order, so the decoder
// REORDERS it to (a, b) ordinal order (#533). This is the cross-source invariant:
// pk_values == BuildPKValues(resolver PKColumnMetas, row), which MySQL satisfies by
// construction and reconstruct/fulltable.go relies on POSITIONALLY — a key-order
// pk_values would silently corrupt the baseline+delta merge (and diverge pk_hash
// from a MySQL source) for a composite PK declared out of column order. With
// a=10, b=20 the ordinal order (a, b) yields "10|20".
func TestCapturer_CompositePKOrder(t *testing.T) {
	baseDSN := testutil.SkipIfNoPostgres(t)
	ctx := context.Background()

	const slot = "bintrail_pgcap_ck"
	const pub = "bintrail_pgcap_ck_pub"
	const tbl = "pgcap_ck_t"

	setup, err := pgx.Connect(ctx, baseDSN)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { setup.Close(context.Background()) })
	cleanup := func() {
		bg := context.Background()
		_, _ = setup.Exec(bg, "DROP PUBLICATION IF EXISTS "+pub)
		_, _ = setup.Exec(bg, "DROP TABLE IF EXISTS "+tbl)
		_, _ = setup.Exec(bg, "SELECT pg_drop_replication_slot($1) WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name=$1)", slot)
	}
	cleanup()
	t.Cleanup(cleanup)

	mustExec := func(sql string, args ...any) {
		t.Helper()
		if _, err := setup.Exec(ctx, sql, args...); err != nil {
			t.Fatalf("exec %q: %v", sql, err)
		}
	}
	// Columns declared a, b, c (attnums 1, 2, 3); key is (b, a) = attnums (2, 1).
	// The decoder reorders the catalog's key-order PK to table-ordinal order (a, b).
	mustExec(fmt.Sprintf("CREATE TABLE %s (a int, b int, c text, PRIMARY KEY (b, a))", tbl))
	mustExec(fmt.Sprintf("ALTER TABLE %s REPLICA IDENTITY FULL", tbl))
	mustExec(fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s", pub, tbl))

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	cap := pgcapture.New(pgcapture.Config{
		ReplDSN:         replDSN(baseDSN),
		QueryDSN:        baseDSN,
		SlotName:        slot,
		Publication:     pub,
		Filters:         event.Filters{Tables: map[string]bool{"public." + tbl: true}},
		StandbyInterval: 200 * time.Millisecond,
	})
	events := make(chan event.Event, 16)
	runErr := make(chan error, 1)
	go func() { runErr <- cap.Run(runCtx, events) }()
	waitFor(t, 10*time.Second, func() bool {
		var active bool
		if err := setup.QueryRow(ctx, "SELECT active FROM pg_replication_slots WHERE slot_name=$1", slot).Scan(&active); err != nil {
			return false
		}
		return active
	}, "replication slot to become active")

	mustExec(fmt.Sprintf("INSERT INTO %s VALUES (10, 20, 'x')", tbl)) // a=10, b=20

	rows, _ := collect(t, events, 1, 10*time.Second)
	ins := pick(t, rows, event.EventInsert)
	if ins.PKValues != "10|20" {
		t.Errorf("composite PKValues = %q, want %q (table-ordinal order (a, b), matching the offline resolver — NOT key order (b, a))", ins.PKValues, "10|20")
	}

	cancel()
	<-runErr
}

// ── helpers ──

func replDSN(base string) string {
	if strings.Contains(base, "?") {
		return base + "&replication=database"
	}
	return base + "?replication=database"
}

func confirmedFlush(t *testing.T, conn *pgx.Conn, slot string) pglogrepl.LSN {
	t.Helper()
	var s string
	if err := conn.QueryRow(context.Background(), "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name=$1", slot).Scan(&s); err != nil {
		t.Fatalf("read confirmed_flush_lsn: %v", err)
	}
	lsn, err := pglogrepl.ParseLSN(s)
	if err != nil {
		t.Fatalf("parse confirmed_flush_lsn %q: %v", s, err)
	}
	return lsn
}

// collect reads from ch until it has wantRows non-commit row events or times out,
// returning the row events and the EventCommit events separately.
func collect(t *testing.T, ch <-chan event.Event, wantRows int, timeout time.Duration) (rows, commits []event.Event) {
	t.Helper()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for len(rows) < wantRows {
		select {
		case ev := <-ch:
			switch ev.EventType {
			case event.EventCommit:
				commits = append(commits, ev)
			case event.EventRelation:
				// Schema/shape event (#533) — not a row; ignore for row collection.
			case event.EventSentThrough:
				// A keepalive's position (#2746) — not a row.
			default:
				rows = append(rows, ev)
			}
		case <-deadline.C:
			t.Fatalf("timed out: collected %d/%d row events (%d commits)", len(rows), wantRows, len(commits))
		}
	}
	// Drain any trailing commit already queued (best-effort, non-blocking).
	for {
		select {
		case ev := <-ch:
			switch ev.EventType {
			case event.EventCommit:
				commits = append(commits, ev)
			case event.EventRelation:
				// Schema/shape event (#533) — not a row; ignore for row collection.
			case event.EventSentThrough:
				// A keepalive's position (#2746) — not a row.
			default:
				rows = append(rows, ev)
			}
		default:
			return rows, commits
		}
	}
}

func pick(t *testing.T, rows []event.Event, typ event.EventType) event.Event {
	t.Helper()
	for _, ev := range rows {
		if ev.EventType == typ {
			return ev
		}
	}
	t.Fatalf("no event of type %d among %d collected row events", typ, len(rows))
	return event.Event{}
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

func contains(ss []string, s string) bool {
	for _, x := range ss {
		if x == s {
			return true
		}
	}
	return false
}

func lenOf(v any) int {
	if s, ok := v.(string); ok {
		return len(s)
	}
	return -1
}

func trunc(v any) string {
	s := fmt.Sprintf("%v", v)
	if len(s) > 40 {
		return s[:40] + "…"
	}
	return s
}
