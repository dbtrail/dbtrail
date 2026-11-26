//go:build integration

package pgcapture

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"

	"github.com/dbtrail/dbtrail/internal/testutil"
)

// A capture that resumes is held to ITS slot, not to a slot of that name: a
// physical slot, or a logical one of another plugin, called what the capture
// calls its own is refused, where it was taken for the capture's and the
// stream resumed on it.
func TestEnsureSlot_refusesAnotherSlotOfItsName(t *testing.T) {
	dsn := testutil.SkipIfNoPostgres(t)
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { conn.Close(context.Background()) })
	const physical, other, own = "bintrail_it_identity_physical", "bintrail_it_identity_other", "bintrail_it_identity_own"
	drop := func() {
		for _, s := range []string{physical, other, own} {
			_, _ = conn.Exec(context.Background(), "SELECT pg_drop_replication_slot($1) WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name=$1)", s)
		}
	}
	drop()
	t.Cleanup(drop)
	for _, q := range []string{
		"SELECT pg_create_physical_replication_slot('" + physical + "')",
		"SELECT pg_create_logical_replication_slot('" + other + "', 'test_decoding')",
		"SELECT pg_create_logical_replication_slot('" + own + "', 'pgoutput')",
	} {
		if _, err := conn.Exec(ctx, q); err != nil {
			if strings.Contains(q, "test_decoding") {
				t.Logf("no test_decoding plugin on this server (%v): that case is skipped", err)
				continue
			}
			t.Fatalf("%s: %v", q, err)
		}
	}
	const saved = pglogrepl.LSN(0x1000)
	// expectExisting returns before the replication connection is used.
	for _, s := range []string{physical, other} {
		var exists bool
		if err := conn.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name=$1)", s).Scan(&exists); err != nil || !exists {
			continue
		}
		if lsn, err := ensureSlot(ctx, nil, conn, s, saved, true, nil); err == nil || !strings.Contains(err.Error(), "not a pgoutput logical slot") {
			t.Errorf("resuming on %s: LSN %s, %v; want a refusal that says what the slot is", s, lsn, err)
		}
	}
	if lsn, err := ensureSlot(ctx, nil, conn, own, saved, true, nil); err != nil || lsn != saved {
		t.Errorf("resuming on the capture's own slot: %s, %v; want %s", lsn, err, saved)
	}
	if _, err := ensureSlot(ctx, nil, conn, "bintrail_it_identity_none", saved, true, nil); err == nil || !strings.Contains(err.Error(), "no longer exists") {
		t.Errorf("resuming with no slot: %v", err)
	}
}
