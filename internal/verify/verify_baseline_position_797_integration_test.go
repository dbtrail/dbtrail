//go:build integration

package verify

import (
	"context"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestVerifyBaselinePair_PositionAnchoredLowerBound_797 is the end-to-end #797
// repro via a real consumer: a transaction whose statement executed just
// BEFORE the previous baseline's wall-clock snapshot instant, but which
// committed (and so was durably logged, gaining a binlog position at or after
// the previous baseline's own recorded anchor) just after it.
//
// Before #797, VerifyBaselinePair anchored the delta fetch's lower bound on
// PrevSnapshot alone (a DATETIME). That transaction's event_timestamp falls
// in the HOUR BEFORE PrevSnapshot's own hour — an earlier binlog_events
// partition — so it would be silently dropped from the reconstruction: absent
// from both the previous baseline's MVCC snapshot (not yet committed when the
// dump's consistent read was established) and from the Since-only fetch. The
// reconstructed row would stay "a" while the new (truth) baseline already
// shows "zzz", surfacing as a causeless MISMATCH.
//
// #797 anchors the fetch on the previous baseline's own recorded binlog
// position (BaselinePair.PrevAnchor) instead, so the event is now correctly
// picked up as a delta and the reconstruction matches the new baseline.
func TestVerifyBaselinePair_PositionAnchoredLowerBound_797(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	for _, c := range []struct {
		name, key, dt, colType string
		ord                    int
	}{
		{"id", "PRI", "int", "int", 1},
		{"status", "", "varchar", "varchar(64)", 2},
	} {
		testutil.MustExec(t, db, `INSERT INTO schema_snapshots
			(snapshot_id, snapshot_time, schema_name, table_name, column_name,
			 ordinal_position, column_key, data_type, column_type, is_nullable, is_generated)
			VALUES (1, UTC_TIMESTAMP(), ?, 'orders', ?, ?, ?, ?, ?, 'YES', 0)`,
			dbName, c.name, c.ord, c.key, c.dt, c.colType)
	}

	baseDir := t.TempDir()
	// prevTS is hour-aligned so subtracting 10 minutes lands the skewed event
	// in the PRECEDING hourly partition — exactly the #797 scenario (a
	// transaction whose statement executed at, e.g., 09:59:50 against a dump
	// "Started at" 10:00:00).
	prevTS := time.Now().UTC().Truncate(time.Hour).Add(-2 * time.Hour)
	newTS := prevTS.Add(time.Hour)
	createSQL := "CREATE TABLE `orders` (\n  `id` INT NOT NULL,\n  `status` VARCHAR(64),\n  PRIMARY KEY (`id`)\n);\n"
	cols := []baseline.Column{
		{Name: "id", MySQLType: "int", ParquetType: baseline.MysqlToParquetNode("int")},
		{Name: "status", MySQLType: "varchar", ParquetType: baseline.MysqlToParquetNode("varchar")},
	}

	writeTestBaseline(t, baseDir, prevTS, dbName, "orders", createSQL, cols, [][]string{
		{"1", "a"},
	}, "binlog.000001", 200)
	writeTestBaseline(t, baseDir, newTS, dbName, "orders", createSQL, cols, [][]string{
		{"1", "zzz"},
	}, "binlog.000001", 500)

	// Partitions: the hour BEFORE prevTS (holds the skewed event), prevTS's
	// own hour, and newTS's hour.
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{prevTS.Add(-time.Hour), prevTS, newTS})

	// The skewed event: executed 10 minutes before prevTS's hour, but its
	// binlog position (200) is exactly the previous baseline's recorded
	// anchor — a genuine post-snapshot delta by position.
	skewedTS := prevTS.Add(-10 * time.Minute).Format("2006-01-02 15:04:05")
	testutil.InsertEvent(t, db, "binlog.000001", 200, 300, skewedTS, nil, dbName, "orders", 2 /*UPDATE*/, "1", nil,
		[]byte(`{"id":1,"status":"a"}`), []byte(`{"id":1,"status":"zzz"}`))

	resolver, err := metadata.NewResolver(db, 1)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}
	cfg := BaselineConfig{IndexDB: db, Resolver: resolver, IndexDBName: dbName, NoArchive: true}
	ctx := context.Background()

	pairs, _, err := FindBaselinePair(ctx, baseDir)
	if err != nil {
		t.Fatalf("FindBaselinePair: %v", err)
	}
	if len(pairs) != 1 {
		t.Fatalf("expected 1 pair (orders), got %d", len(pairs))
	}
	if pairs[0].PrevAnchor.File != "binlog.000001" || pairs[0].PrevAnchor.Pos != 200 {
		t.Fatalf("PrevAnchor = %+v, want {binlog.000001 200} (FindBaselinePair must read the prev baseline's own recorded position)", pairs[0].PrevAnchor)
	}

	got, err := VerifyBaselinePair(ctx, cfg, pairs[0])
	if err != nil {
		t.Fatalf("VerifyBaselinePair: %v", err)
	}
	if got.Status != StatusMatch {
		t.Fatalf("status = %q (%s); want match — the skewed-timestamp event must be folded into the reconstruction "+
			"(a %q status pre-#797 would indicate the event was silently dropped, reconstructing to \"a\" instead of \"zzz\")",
			got.Status, got.Detail, StatusMismatch)
	}
}

// TestVerifyBaselinePair_NoPrevAnchor_FallsBackToTimestamp_797 confirms the
// documented fallback: a previous baseline that never recorded a binlog
// position (an older baseline, predating that metadata) leaves
// BaselinePair.PrevAnchor at its zero value, VerifyBaselinePair does not set
// SincePos, and an ordinary (non-skewed) in-window event is still picked up
// correctly via the plain PrevSnapshot DATETIME filter — the pre-#797
// behavior, unbroken by the new code path.
func TestVerifyBaselinePair_NoPrevAnchor_FallsBackToTimestamp_797(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	for _, c := range []struct {
		name, key, dt, colType string
		ord                    int
	}{
		{"id", "PRI", "int", "int", 1},
		{"status", "", "varchar", "varchar(64)", 2},
	} {
		testutil.MustExec(t, db, `INSERT INTO schema_snapshots
			(snapshot_id, snapshot_time, schema_name, table_name, column_name,
			 ordinal_position, column_key, data_type, column_type, is_nullable, is_generated)
			VALUES (1, UTC_TIMESTAMP(), ?, 'orders', ?, ?, ?, ?, ?, 'YES', 0)`,
			dbName, c.name, c.ord, c.key, c.dt, c.colType)
	}

	baseDir := t.TempDir()
	prevTS := time.Now().UTC().Truncate(time.Hour).Add(-2 * time.Hour)
	newTS := prevTS.Add(time.Hour)
	createSQL := "CREATE TABLE `orders` (\n  `id` INT NOT NULL,\n  `status` VARCHAR(64),\n  PRIMARY KEY (`id`)\n);\n"
	cols := []baseline.Column{
		{Name: "id", MySQLType: "int", ParquetType: baseline.MysqlToParquetNode("int")},
		{Name: "status", MySQLType: "varchar", ParquetType: baseline.MysqlToParquetNode("varchar")},
	}

	// PREV baseline deliberately has NO recorded binlog anchor (anchorFile ""),
	// simulating a pre-#633 baseline.
	writeTestBaseline(t, baseDir, prevTS, dbName, "orders", createSQL, cols, [][]string{
		{"1", "a"},
	}, "", 0)
	writeTestBaseline(t, baseDir, newTS, dbName, "orders", createSQL, cols, [][]string{
		{"1", "zzz"},
	}, "binlog.000001", 500)

	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{prevTS, newTS})

	// A normal (non-skewed) in-window event, well after prevTS.
	ets := prevTS.Add(30 * time.Minute).Format("2006-01-02 15:04:05")
	testutil.InsertEvent(t, db, "binlog.000001", 200, 300, ets, nil, dbName, "orders", 2 /*UPDATE*/, "1", nil,
		[]byte(`{"id":1,"status":"a"}`), []byte(`{"id":1,"status":"zzz"}`))

	resolver, err := metadata.NewResolver(db, 1)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}
	cfg := BaselineConfig{IndexDB: db, Resolver: resolver, IndexDBName: dbName, NoArchive: true}
	ctx := context.Background()

	pairs, _, err := FindBaselinePair(ctx, baseDir)
	if err != nil {
		t.Fatalf("FindBaselinePair: %v", err)
	}
	if len(pairs) != 1 {
		t.Fatalf("expected 1 pair (orders), got %d", len(pairs))
	}
	if pairs[0].PrevAnchor.File != "" || pairs[0].PrevAnchor.Pos != 0 {
		t.Fatalf("PrevAnchor = %+v, want the zero value (no position recorded on the prev baseline)", pairs[0].PrevAnchor)
	}

	got, err := VerifyBaselinePair(ctx, cfg, pairs[0])
	if err != nil {
		t.Fatalf("VerifyBaselinePair: %v", err)
	}
	if got.Status != StatusMatch {
		t.Fatalf("status = %q (%s); want match — the fallback (timestamp-only Since) must still pick up an ordinary in-window event", got.Status, got.Detail)
	}
}
