//go:build integration

package status_test

import (
	"context"
	"strings"
	"testing"

	_ "github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/status"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestLoadStreamState_LegacyIndexMissingGapColumns pins the #586 graceful fallback: a
// pre-cascade index lacking gap_lost_at/gap_lost_detail — read before any migrating
// command ran EnsureSchema (the console never migrates registry DSNs) — must NOT error
// `status`. LoadStreamState falls back to the base columns and reports no loss record.
func TestLoadStreamState_LegacyIndexMissingGapColumns(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()
	db, _ := testutil.CreateTestDB(t)
	if err := indexer.CreateIndexTables(ctx, db, 4, false, nil); err != nil {
		t.Fatalf("CreateIndexTables: %v", err)
	}
	// Simulate a legacy index: drop the columns the cascade-recovery work added.
	if _, err := db.ExecContext(ctx, "ALTER TABLE stream_state DROP COLUMN gap_lost_at, DROP COLUMN gap_lost_detail"); err != nil {
		t.Fatalf("drop gap columns: %v", err)
	}
	if _, err := db.ExecContext(ctx,
		"INSERT INTO stream_state (id, mode, server_id, last_checkpoint) VALUES (1, 'gtid', 7, UTC_TIMESTAMP())"); err != nil {
		t.Fatalf("seed base row: %v", err)
	}

	st, err := status.LoadStreamState(ctx, db)
	if err != nil {
		t.Fatalf("LoadStreamState must not error on a legacy index, got: %v", err)
	}
	if st == nil {
		t.Fatal("expected the base row, got nil")
	}
	if st.ServerID != 7 || st.Mode != "gtid" {
		t.Errorf("base columns not loaded after fallback: %+v", st)
	}
	if st.GapLostAt.Valid {
		t.Errorf("a legacy index has no gap-loss record; GapLostAt should be invalid, got %v", st.GapLostAt)
	}
}

// TestLoadStreamState_SourceHealth pins the #599 read path: a row with a source_health
// JSON snapshot is returned verbatim, and an index missing the column (legacy / not yet
// migrated — the console never migrates registry DSNs) must NOT error. The separate
// best-effort query degrades to "no health" only on the unknown-column error.
func TestLoadStreamState_SourceHealth(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()
	db, _ := testutil.CreateTestDB(t)
	if err := indexer.CreateIndexTables(ctx, db, 4, false, nil); err != nil {
		t.Fatalf("CreateIndexTables: %v", err)
	}
	if _, err := db.ExecContext(ctx,
		`INSERT INTO stream_state (id, mode, flavor, server_id, last_checkpoint, source_health)
		 VALUES (1, 'gtid', 'postgres', 9, UTC_TIMESTAMP(), '{"wal_status":"reserved"}')`); err != nil {
		t.Fatalf("seed row: %v", err)
	}

	st, err := status.LoadStreamState(ctx, db)
	if err != nil {
		t.Fatalf("LoadStreamState: %v", err)
	}
	if st == nil || !st.SourceHealth.Valid {
		t.Fatalf("expected a source_health snapshot, got %+v", st)
	}
	if !strings.Contains(st.SourceHealth.String, `"wal_status"`) {
		t.Errorf("source_health not loaded verbatim: %q", st.SourceHealth.String)
	}

	// Legacy index without the column → the best-effort second query tolerates the
	// unknown-column error, leaving SourceHealth invalid (no health, no error).
	if _, err := db.ExecContext(ctx, "ALTER TABLE stream_state DROP COLUMN source_health"); err != nil {
		t.Fatalf("drop source_health: %v", err)
	}
	st, err = status.LoadStreamState(ctx, db)
	if err != nil {
		t.Fatalf("LoadStreamState must not error on an index missing source_health, got: %v", err)
	}
	if st == nil || st.SourceHealth.Valid {
		t.Errorf("a legacy index has no source_health; want invalid, got %+v", st)
	}
}

// TestLoadStreamState_GapAckedAt pins the acknowledgement read path against a
// real index: a loss on record with gap_acked_at set reads as acknowledged
// (and stays loaded); a second loss stamped the way the writers stamp it
// (gap_acked_at = NULL in the same statement) is unacknowledged again; and an
// index that has gap_lost_* but not yet gap_acked_at still loads as EVALUABLE
// and unacknowledged, instead of falling back to the base columns.
func TestLoadStreamState_GapAckedAt(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()
	db, _ := testutil.CreateTestDB(t)
	if err := indexer.CreateIndexTables(ctx, db, 4, false, nil); err != nil {
		t.Fatalf("CreateIndexTables: %v", err)
	}
	testutil.MustExec(t, db, `INSERT INTO stream_state (id, mode, server_id, last_checkpoint, gap_lost_at, gap_lost_detail)
		VALUES (1, 'gtid', 7, UTC_TIMESTAMP(), UTC_TIMESTAMP(), 'purged')`)
	load := func() *status.StreamStateInfo {
		t.Helper()
		st, err := status.LoadStreamState(ctx, db)
		if err != nil || st == nil {
			t.Fatalf("LoadStreamState = (%v, %v)", st, err)
		}
		return st
	}

	if st := load(); !st.GapUnacknowledged() || st.GapAcknowledged() {
		t.Errorf("a fresh loss must be unacknowledged: %+v", st)
	}
	// No-op acknowledgement on a row with a loss, in the same second.
	testutil.MustExec(t, db, `UPDATE stream_state SET gap_acked_at = UTC_TIMESTAMP() WHERE id = 1 AND gap_lost_at IS NOT NULL`)
	if st := load(); !st.GapAcknowledged() || st.GapUnacknowledged() || !st.GapLostAt.Valid || !st.GapColumnsPresent {
		t.Errorf("an acknowledged loss must stay on record and read as acknowledged: %+v", st)
	}
	if got := status.ContinuityStatus(load(), nil); got != status.ContinuityOK {
		t.Errorf("continuity after the acknowledgement = %q, want ok", got)
	}
	// A second loss in the same second as the acknowledgement.
	testutil.MustExec(t, db, `UPDATE stream_state SET gap_lost_at = UTC_TIMESTAMP(), gap_lost_detail = 'second', gap_acked_at = NULL WHERE id = 1`)
	if st := load(); !st.GapUnacknowledged() {
		t.Errorf("a loss stamped after an acknowledgement must be unacknowledged: %+v", st)
	}

	// An index with the loss record and no acknowledgement column.
	testutil.MustExec(t, db, `ALTER TABLE stream_state DROP COLUMN gap_acked_at`)
	st := load()
	if !st.GapColumnsPresent {
		t.Error("a missing gap_acked_at must not make the gap state unevaluable")
	}
	if !st.GapLostAt.Valid || !st.GapUnacknowledged() {
		t.Errorf("want the loss loaded and unacknowledged, got %+v", st)
	}
}
