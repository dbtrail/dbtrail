//go:build integration

package pgcapture_test

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/pgcapture"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestProbeHealth_Integration drives ProbeHealth (slot health + REPLICA IDENTITY
// coverage, connect-per-call) against a live PostgreSQL — the source-side read the
// streaming daemon persists for the console health panel (#599). It also exercises the
// lists it builds over QueryTableIdentities end to end: a publication with one FULL and one default-identity table must
// report exactly the non-FULL one.
//
// Requires BINTRAIL_TEST_PG_DSN (e.g. postgres://postgres:testpg@localhost:15533/pgtest).
func TestProbeHealth_Integration(t *testing.T) {
	dsn := testutil.SkipIfNoPostgres(t)
	ctx := context.Background()
	const slot = "bintrail_probe_it"
	const pub = "bintrail_probe_pub"

	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { conn.Close(context.Background()) })

	cleanup := func() {
		bg := context.Background()
		_, _ = conn.Exec(bg, "SELECT pg_drop_replication_slot($1) WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name=$1)", slot)
		_, _ = conn.Exec(bg, "DROP PUBLICATION IF EXISTS "+pub)
		_, _ = conn.Exec(bg, "DROP TABLE IF EXISTS probe_full, probe_nofull")
	}
	cleanup()
	t.Cleanup(cleanup)

	mustExec := func(sql string) {
		t.Helper()
		if _, err := conn.Exec(ctx, sql); err != nil {
			t.Fatalf("exec %q: %v", sql, err)
		}
	}
	mustExec("CREATE TABLE probe_full (id int PRIMARY KEY)")
	mustExec("CREATE TABLE probe_nofull (id int PRIMARY KEY)")
	mustExec("ALTER TABLE probe_full REPLICA IDENTITY FULL")
	// probe_nofull keeps the default identity (relreplident='d') → reported not captured.
	mustExec("CREATE PUBLICATION " + pub + " FOR TABLE probe_full, probe_nofull")
	if _, err := conn.Exec(ctx, "SELECT pg_create_logical_replication_slot($1, 'pgoutput')", slot); err != nil {
		t.Fatalf("create slot: %v", err)
	}

	snap, err := pgcapture.ProbeHealth(ctx, dsn, slot, pub, event.Filters{})
	if err != nil {
		t.Fatalf("ProbeHealth: %v", err)
	}
	if !snap.Slot.Exists {
		t.Errorf("ProbeHealth slot Exists=false for a created slot: %+v", snap.Slot)
	}
	// The default-identity table is not captured; the list of tables without
	// undo (what 0.138.0 captured below FULL) stays empty.
	if len(snap.ReplicaIdentityNotFull) != 0 {
		t.Errorf("ReplicaIdentityNotFull = %v, want none", snap.ReplicaIdentityNotFull)
	}
	if len(snap.NotCaptured) != 1 || snap.NotCaptured[0] != "public.probe_nofull (not at REPLICA IDENTITY FULL)" {
		t.Errorf("NotCaptured = %v, want exactly the one default-identity table", snap.NotCaptured)
	}

	// The publication's tables, by OID: a rename keeps a table's, and a
	// table taken out of the publication, or dropped, is no longer listed.
	names := func(ts []pgcapture.PublishedTable) string {
		var out []string
		for _, pt := range ts {
			out = append(out, pt.Schema+"."+pt.Table)
		}
		return strings.Join(out, " ")
	}
	oidOf := func(ts []pgcapture.PublishedTable, table string) uint32 {
		for _, pt := range ts {
			if pt.Table == table {
				return pt.OID
			}
		}
		return 0
	}
	if snap.PublishedErr != nil || len(snap.Published) != 2 || oidOf(snap.Published, "probe_full") == 0 || oidOf(snap.Published, "probe_nofull") == 0 {
		t.Fatalf("Published = %q, %v; want the publication's two tables with their OIDs", names(snap.Published), snap.PublishedErr)
	}
	was := oidOf(snap.Published, "probe_nofull")
	mustExec("ALTER TABLE probe_nofull RENAME TO probe_renamed")
	t.Cleanup(func() { _, _ = conn.Exec(context.Background(), "DROP TABLE IF EXISTS probe_renamed") })
	renamed, err := pgcapture.QueryPublishedTables(ctx, conn, pub)
	if err != nil || len(renamed) != 2 || oidOf(renamed, "probe_renamed") != was {
		t.Fatalf("after a rename: %q, %v; want the same OID %d under the new name", names(renamed), err, was)
	}
	mustExec("ALTER PUBLICATION " + pub + " DROP TABLE probe_renamed")
	out, err := pgcapture.QueryPublishedTables(ctx, conn, pub)
	if err != nil || len(out) != 1 || out[0].Table != "probe_full" {
		t.Fatalf("after the table was taken out: %q, %v; want probe_full alone", names(out), err)
	}
	mustExec("DROP TABLE probe_full")
	if out, err = pgcapture.QueryPublishedTables(ctx, conn, pub); err != nil || out == nil || len(out) != 0 {
		t.Fatalf("after the last table was dropped: %q, %v; want none, and not an unread list", names(out), err)
	}
}
