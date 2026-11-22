//go:build integration

package pgcapture_test

import (
	"context"
	"testing"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/dbtrail/dbtrail/internal/pgcapture"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestSlotFloorLSN_Integration pins the #771 fix's core value contract:
// SlotFloorLSN reports a floor that (a) matches the slot's own
// confirmed_flush_lsn, (b) does NOT move when the slot is never consumed
// even though unrelated WAL activity advances pg_current_wal_lsn(), and
// (c) stays <= any later live LSN read on the same connection — the
// invariant pgbaseline.Run leans on to make "replay deltas from this floor
// forward" safe regardless of the snapshot-vs-concurrent-commit race
// described on SlotFloorLSN's doc comment.
func TestSlotFloorLSN_Integration(t *testing.T) {
	baseDSN := testutil.SkipIfNoPostgres(t)
	ctx := context.Background()

	const slot = "bintrail_pgcap_it_slotfloor_issue_771"
	const tbl = "pgcap_it_slotfloor_issue_771"

	conn, err := pgx.Connect(ctx, baseDSN)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { conn.Close(context.Background()) })
	drop := func() {
		bg := context.Background()
		_, _ = conn.Exec(bg, "DROP TABLE IF EXISTS "+tbl)
		_, _ = conn.Exec(bg, "SELECT pg_drop_replication_slot($1) WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name=$1)", slot)
	}
	drop()
	t.Cleanup(drop)

	if _, err := conn.Exec(ctx, "CREATE TABLE "+tbl+" (id int PRIMARY KEY)"); err != nil {
		t.Fatalf("create table: %v", err)
	}

	// Missing slot: actionable error, not a zero value.
	if _, err := pgcapture.SlotFloorLSN(ctx, conn, slot); err == nil {
		t.Fatal("SlotFloorLSN on a missing slot succeeded, want an error")
	}

	replConnect := func(ctx context.Context) (*pgconn.PgConn, error) {
		return pgconn.Connect(ctx, replDSN(baseDSN))
	}
	if _, err := pgcapture.AnchorOnSlot(ctx, conn, slot, replConnect, 0, nil); err != nil {
		t.Fatalf("AnchorOnSlot: %v", err)
	}

	floor1, err := pgcapture.SlotFloorLSN(ctx, conn, slot)
	if err != nil {
		t.Fatalf("SlotFloorLSN (fresh slot): %v", err)
	}
	if floor1 == 0 {
		t.Error("SlotFloorLSN on a freshly created slot = 0, want a real LSN")
	}

	// The fresh slot's floor must equal its own confirmed_flush_lsn (verified
	// independently of SlotFloorLSN's internal query).
	var flushText string
	if err := conn.QueryRow(ctx, "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name=$1", slot).Scan(&flushText); err != nil {
		t.Fatalf("read confirmed_flush_lsn: %v", err)
	}
	flush, err := pglogrepl.ParseLSN(flushText)
	if err != nil {
		t.Fatalf("parse confirmed_flush_lsn %q: %v", flushText, err)
	}
	if floor1 != flush {
		t.Errorf("SlotFloorLSN = %d, want confirmed_flush_lsn %d", uint64(floor1), uint64(flush))
	}

	// Unrelated WAL activity (no consumer ever reads from the slot) must
	// advance pg_current_wal_lsn() while leaving the slot's floor untouched —
	// this is the exact gap the #771 fix relies on: a live "now" LSN and the
	// slot's own floor are NOT the same thing, and can diverge arbitrarily.
	if _, err := conn.Exec(ctx, "INSERT INTO "+tbl+" VALUES (1), (2), (3)"); err != nil {
		t.Fatalf("insert: %v", err)
	}

	var curText string
	if err := conn.QueryRow(ctx, "SELECT pg_current_wal_lsn()::text").Scan(&curText); err != nil {
		t.Fatalf("pg_current_wal_lsn: %v", err)
	}
	cur, err := pglogrepl.ParseLSN(curText)
	if err != nil {
		t.Fatalf("parse current LSN %q: %v", curText, err)
	}
	if uint64(cur) <= uint64(floor1) {
		t.Fatalf("test setup: current WAL LSN %d did not advance past the slot floor %d after committing inserts", uint64(cur), uint64(floor1))
	}

	floor2, err := pgcapture.SlotFloorLSN(ctx, conn, slot)
	if err != nil {
		t.Fatalf("SlotFloorLSN (after unrelated writes): %v", err)
	}
	if floor2 != floor1 {
		t.Errorf("SlotFloorLSN moved from %d to %d after unrelated WAL activity with no slot consumption — the floor must stay fixed until something actually consumes the slot", uint64(floor1), uint64(floor2))
	}
	if uint64(floor2) > uint64(cur) {
		t.Errorf("SlotFloorLSN %d > live pg_current_wal_lsn() %d — safety invariant violated", uint64(floor2), uint64(cur))
	}
}
