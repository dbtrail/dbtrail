//go:build integration

package rotation

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/parquet-go/parquet-go"

	"github.com/dbtrail/dbtrail/internal/archive"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/parquetquery"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// ─── hasPendingS3Upload ──────────────────────────────────────────────────────

func TestHasPendingS3Upload_noRow(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	pending, err := hasPendingS3Upload(context.Background(), db, "p_2026030100", "test-uuid")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pending {
		t.Error("expected false when no archive_state row exists")
	}
}

func TestHasPendingS3Upload_localOnly(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	// Row exists but s3_bucket is NULL — no S3 intent recorded.
	testutil.MustExec(t, db, `INSERT INTO archive_state
		(partition_name, bintrail_id, local_path, row_count)
		VALUES ('p_2026030100', 'test-uuid', '/data/test.parquet', 42)`)

	pending, err := hasPendingS3Upload(context.Background(), db, "p_2026030100", "test-uuid")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pending {
		t.Error("expected false when s3_bucket is NULL (no S3 intent)")
	}
}

func TestHasPendingS3Upload_pendingUpload(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	// Row with s3_bucket set but s3_uploaded_at NULL — pending upload.
	testutil.MustExec(t, db, `INSERT INTO archive_state
		(partition_name, bintrail_id, local_path, row_count, s3_bucket, s3_key)
		VALUES ('p_2026030100', 'test-uuid', '/data/test.parquet', 42, 'my-bucket', 'archives/test.parquet')`)

	pending, err := hasPendingS3Upload(context.Background(), db, "p_2026030100", "test-uuid")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !pending {
		t.Error("expected true when s3_bucket is set but s3_uploaded_at is NULL")
	}
}

func TestHasPendingS3Upload_completed(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	// Row with s3_uploaded_at set — upload complete.
	testutil.MustExec(t, db, `INSERT INTO archive_state
		(partition_name, bintrail_id, local_path, row_count, s3_bucket, s3_key, s3_uploaded_at)
		VALUES ('p_2026030100', 'test-uuid', '/data/test.parquet', 42, 'my-bucket', 'archives/test.parquet', UTC_TIMESTAMP())`)

	pending, err := hasPendingS3Upload(context.Background(), db, "p_2026030100", "test-uuid")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pending {
		t.Error("expected false when s3_uploaded_at is set")
	}
}

func TestHasPendingS3Upload_emptyBintrailID(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	// Row with a specific bintrail_id but query uses empty string — should still detect.
	testutil.MustExec(t, db, `INSERT INTO archive_state
		(partition_name, bintrail_id, local_path, row_count, s3_bucket, s3_key)
		VALUES ('p_2026030100', 'some-uuid', '/data/test.parquet', 42, 'my-bucket', 'archives/test.parquet')`)

	pending, err := hasPendingS3Upload(context.Background(), db, "p_2026030100", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !pending {
		t.Error("expected true when bintrailID is empty and any row has pending S3")
	}
}

// ─── Perform S3 safety ───────────────────────────────────────────────────────

// TestPerformRotation_PendingS3BlocksDrop verifies that when a previous
// rotation run recorded S3 upload intent (s3_bucket set) but the upload
// did not complete (s3_uploaded_at NULL), a subsequent rotation run — even
// without --archive-s3 — refuses to drop that partition.
//
// Note: the S3-upload-failure continue path (uploadFileFunc returning an error)
// is not directly integration-tested here because Perform creates its own S3
// client internally from the archive Options. The safety check tested here is
// the defense-in-depth layer that catches any scenario where the partition
// reaches the drop step with a pending upload.
func TestPerformRotation_PendingS3BlocksDrop(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	// Create two old partitions.
	h1 := time.Now().UTC().Add(-48 * time.Hour).Truncate(time.Hour)
	h2 := h1.Add(time.Hour)
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{h1, h2})

	// Insert a row into each partition so archiving has data.
	ts1 := h1.Add(30 * time.Minute).Format("2006-01-02 15:04:05")
	ts2 := h2.Add(30 * time.Minute).Format("2006-01-02 15:04:05")
	testutil.InsertEvent(t, db, "binlog.000001", 100, 200, ts1, nil, "testdb", "users", 1, "1", nil, nil, []byte(`{"id":1}`))
	testutil.InsertEvent(t, db, "binlog.000001", 200, 300, ts2, nil, "testdb", "users", 1, "2", nil, nil, []byte(`{"id":2}`))

	// Pre-archive: create local Parquet files and archive_state rows
	// simulating a previous run where S3 upload failed for h1 but
	// succeeded for h2.
	archiveDir := t.TempDir()
	bintrailID := "test-uuid-167"

	outPath1, _ := HiveArchivePath(archiveDir, bintrailID, indexer.PartitionName(h1))
	outPath2, _ := HiveArchivePath(archiveDir, bintrailID, indexer.PartitionName(h2))
	os.MkdirAll(filepath.Dir(outPath1), 0o755)
	os.MkdirAll(filepath.Dir(outPath2), 0o755)
	os.WriteFile(outPath1, []byte("parquet-data"), 0o644)
	os.WriteFile(outPath2, []byte("parquet-data"), 0o644)

	// Insert archive_state: first partition has pending S3, second is complete.
	testutil.MustExec(t, db, `INSERT INTO archive_state
		(partition_name, bintrail_id, local_path, row_count, s3_bucket, s3_key)
		VALUES (?, ?, ?, 1, 'my-bucket', 'archives/p1.parquet')`,
		indexer.PartitionName(h1), bintrailID, outPath1)
	testutil.MustExec(t, db, `INSERT INTO archive_state
		(partition_name, bintrail_id, local_path, row_count, s3_bucket, s3_key, s3_uploaded_at)
		VALUES (?, ?, ?, 1, 'my-bucket', 'archives/p2.parquet', UTC_TIMESTAMP())`,
		indexer.PartitionName(h2), bintrailID, outPath2)

	// Run rotation WITHOUT --archive-s3, WITH --retry (so it skips re-archiving).
	logs := captureSlog(t)
	res, err := Perform(context.Background(), db, dbName, Options{
		RetainDur:          24 * time.Hour,
		ArchiveDir:         archiveDir,
		BintrailID:         bintrailID,
		ArchiveCompression: "none",
		Format:             "text",
		NoReplace:          true,
		Retry:              true,
	})
	if err != nil {
		t.Fatalf("Perform failed: %v", err)
	}
	// The archive-then-drop site names the database too (#1715).
	if !logs.hasAttr("dropped partition", "db", dbName) {
		t.Error("the archive path's drop line does not name the database")
	}

	// First partition should NOT be dropped (pending S3 upload).
	// Second partition should be dropped (S3 upload complete).
	if res.Dropped != 1 {
		t.Errorf("expected 1 partition dropped, got %d", res.Dropped)
	}
	// h1's still-pending upload must register as Deferred (the archive-branch
	// pending-skip increment). Asserting only Dropped would let that count
	// regress silently — a stalled archive would then read as a healthy cycle
	// and the built-in loop's escalation streak would never fire.
	if res.Deferred != 1 {
		t.Errorf("expected 1 partition deferred (h1 pending S3 upload), got %d", res.Deferred)
	}

	// Verify partition h1 still exists.
	partitions, err := listPartitions(context.Background(), db, dbName)
	if err != nil {
		t.Fatalf("listPartitions: %v", err)
	}
	p1Name := indexer.PartitionName(h1)
	p2Name := indexer.PartitionName(h2)
	var foundP1, foundP2 bool
	for _, p := range partitions {
		if p.Name == p1Name {
			foundP1 = true
		}
		if p.Name == p2Name {
			foundP2 = true
		}
	}
	if !foundP1 {
		t.Errorf("partition %s should NOT have been dropped (pending S3 upload)", p1Name)
	}
	if foundP2 {
		t.Errorf("partition %s should have been dropped (S3 upload complete)", p2Name)
	}
}

func TestPerformRotation_NoPendingS3DropsAll(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	// Create two old partitions.
	h1 := time.Now().UTC().Add(-48 * time.Hour).Truncate(time.Hour)
	h2 := h1.Add(time.Hour)
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{h1, h2})

	ts1 := h1.Add(30 * time.Minute).Format("2006-01-02 15:04:05")
	ts2 := h2.Add(30 * time.Minute).Format("2006-01-02 15:04:05")
	testutil.InsertEvent(t, db, "binlog.000001", 100, 200, ts1, nil, "testdb", "users", 1, "1", nil, nil, []byte(`{"id":1}`))
	testutil.InsertEvent(t, db, "binlog.000001", 200, 300, ts2, nil, "testdb", "users", 1, "2", nil, nil, []byte(`{"id":2}`))

	// No archive_state rows at all — partitions should be dropped freely.
	res, err := Perform(context.Background(), db, dbName, Options{
		RetainDur: 24 * time.Hour,
		Format:    "text",
		NoReplace: true,
	})
	if err != nil {
		t.Fatalf("Perform failed: %v", err)
	}
	if res.Dropped != 2 {
		t.Errorf("expected 2 partitions dropped, got %d", res.Dropped)
	}
}

func TestPerformRotation_BulkDropSkipsPendingS3(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	// Create two old partitions.
	h1 := time.Now().UTC().Add(-48 * time.Hour).Truncate(time.Hour)
	h2 := h1.Add(time.Hour)
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{h1, h2})

	ts1 := h1.Add(30 * time.Minute).Format("2006-01-02 15:04:05")
	ts2 := h2.Add(30 * time.Minute).Format("2006-01-02 15:04:05")
	testutil.InsertEvent(t, db, "binlog.000001", 100, 200, ts1, nil, "testdb", "users", 1, "1", nil, nil, []byte(`{"id":1}`))
	testutil.InsertEvent(t, db, "binlog.000001", 200, 300, ts2, nil, "testdb", "users", 1, "2", nil, nil, []byte(`{"id":2}`))

	// Insert archive_state with pending S3 for h1 only.
	testutil.MustExec(t, db, `INSERT INTO archive_state
		(partition_name, bintrail_id, local_path, row_count, s3_bucket, s3_key)
		VALUES (?, 'prev-uuid', '/data/test.parquet', 1, 'my-bucket', 'archives/p1.parquet')`,
		indexer.PartitionName(h1))

	// No --archive-dir on this run (bulk-drop path).
	res, err := Perform(context.Background(), db, dbName, Options{
		RetainDur: 24 * time.Hour,
		Format:    "text",
		NoReplace: true,
	})
	if err != nil {
		t.Fatalf("Perform failed: %v", err)
	}

	// h1 should be skipped (pending S3), h2 dropped.
	if res.Dropped != 1 {
		t.Errorf("expected 1 partition dropped (h2 only), got %d", res.Dropped)
	}

	partitions, err := listPartitions(context.Background(), db, dbName)
	if err != nil {
		t.Fatalf("listPartitions: %v", err)
	}
	p1Name := indexer.PartitionName(h1)
	var foundP1 bool
	for _, p := range partitions {
		if p.Name == p1Name {
			foundP1 = true
		}
	}
	if !foundP1 {
		t.Errorf("partition %s should NOT have been dropped (pending S3 from previous run)", p1Name)
	}
}

// ─── protect-unarchived guard (built-in `up` rotation, #420) ─────────────────

// TestPerformRotation_ProtectUnarchivedDefers verifies the full guard matrix
// in one rotation cycle when ProtectUnarchived is set (the built-in `up`
// rotation) and the index has archiving history:
//   - h1: past retention, NOT archived          → deferred (guard)
//   - h2: archived but S3 upload still pending  → skipped (pending-S3 filter)
//   - h3: archived and uploaded                 → dropped
func TestPerformRotation_ProtectUnarchivedDefers(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	h1 := time.Now().UTC().Add(-48 * time.Hour).Truncate(time.Hour)
	h2 := h1.Add(time.Hour)
	h3 := h2.Add(time.Hour)
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{h1, h2, h3})

	for i, h := range []time.Time{h1, h2, h3} {
		ts := h.Add(30 * time.Minute).Format("2006-01-02 15:04:05")
		testutil.InsertEvent(t, db, "binlog.000001", uint64(100*(i+1)), uint64(100*(i+2)), ts, nil,
			"testdb", "users", 1, fmt.Sprintf("%d", i+1), nil, nil, []byte(`{"id":1}`))
	}

	// Archiving history exists: h2 archived with a pending S3 upload
	// (s3_bucket set, s3_uploaded_at NULL); h3 archived and uploaded;
	// h1 not archived at all.
	testutil.MustExec(t, db, `INSERT INTO archive_state
		(partition_name, bintrail_id, local_path, row_count, s3_bucket, s3_key)
		VALUES (?, 'cron-uuid', '/archives/p2.parquet', 1, 'my-bucket', 'archives/p2.parquet')`,
		indexer.PartitionName(h2))
	testutil.MustExec(t, db, `INSERT INTO archive_state
		(partition_name, bintrail_id, local_path, row_count, s3_bucket, s3_key, s3_uploaded_at)
		VALUES (?, 'cron-uuid', '/archives/p3.parquet', 1, 'my-bucket', 'archives/p3.parquet', UTC_TIMESTAMP())`,
		indexer.PartitionName(h3))

	// Built-in rotation profile: no archive flags, protection on.
	res, err := Perform(context.Background(), db, dbName, Options{
		RetainDur:         24 * time.Hour,
		Format:            "text",
		NoReplace:         true,
		ProtectUnarchived: true,
	})
	if err != nil {
		t.Fatalf("Perform failed: %v", err)
	}
	if res.Dropped != 1 {
		t.Errorf("expected 1 partition dropped (archived+uploaded h3 only), got %d", res.Dropped)
	}
	if res.Deferred != 1 {
		t.Errorf("expected 1 partition deferred (unarchived h1; pending-S3 h2 is a skip, not a guard deferral), got %d", res.Deferred)
	}

	partitions, err := listPartitions(context.Background(), db, dbName)
	if err != nil {
		t.Fatalf("listPartitions: %v", err)
	}
	remaining := map[string]bool{}
	for _, p := range partitions {
		remaining[p.Name] = true
	}
	if !remaining[indexer.PartitionName(h1)] {
		t.Errorf("partition %s should NOT have been dropped (past retention but unarchived)", indexer.PartitionName(h1))
	}
	if !remaining[indexer.PartitionName(h2)] {
		t.Errorf("partition %s should NOT have been dropped (archived but S3 upload pending)", indexer.PartitionName(h2))
	}
	if remaining[indexer.PartitionName(h3)] {
		t.Errorf("partition %s should have been dropped (archived and uploaded)", indexer.PartitionName(h3))
	}
}

// TestPerformRotation_ProtectUnarchivedNoHistoryDropsAll verifies the guard's
// other half: an index with NO archiving history at all (empty archive_state —
// the quickstart world) rotates freely even under ProtectUnarchived, which is
// what keeps an unattended bundled volume bounded.
func TestPerformRotation_ProtectUnarchivedNoHistoryDropsAll(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	h1 := time.Now().UTC().Add(-48 * time.Hour).Truncate(time.Hour)
	h2 := h1.Add(time.Hour)
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{h1, h2})

	ts1 := h1.Add(30 * time.Minute).Format("2006-01-02 15:04:05")
	ts2 := h2.Add(30 * time.Minute).Format("2006-01-02 15:04:05")
	testutil.InsertEvent(t, db, "binlog.000001", 100, 200, ts1, nil, "testdb", "users", 1, "1", nil, nil, []byte(`{"id":1}`))
	testutil.InsertEvent(t, db, "binlog.000001", 200, 300, ts2, nil, "testdb", "users", 1, "2", nil, nil, []byte(`{"id":2}`))

	res, err := Perform(context.Background(), db, dbName, Options{
		RetainDur:         24 * time.Hour,
		Format:            "text",
		NoReplace:         true,
		ProtectUnarchived: true,
	})
	if err != nil {
		t.Fatalf("Perform failed: %v", err)
	}
	if res.Dropped != 2 {
		t.Errorf("expected 2 partitions dropped (no archiving history), got %d", res.Dropped)
	}
}

// ─── listPartitions ──────────────────────────────────────────────────────────────────

func TestListPartitions(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)

	if err := indexer.CreateIndexTables(context.Background(), db, 3, false, nil); err != nil {
		t.Fatalf("indexer.CreateIndexTables failed: %v", err)
	}

	parts, err := listPartitions(context.Background(), db, dbName)
	if err != nil {
		t.Fatalf("listPartitions failed: %v", err)
	}

	if len(parts) != 4 {
		t.Fatalf("expected 4 partitions, got %d", len(parts))
	}

	// Verify ordinals are sequential.
	for i, p := range parts {
		if p.Ordinal != i+1 {
			t.Errorf("partition %d: expected ordinal %d, got %d", i, i+1, p.Ordinal)
		}
	}

	// Verify the last is p_future.
	if parts[3].Name != "p_future" {
		t.Errorf("expected last partition p_future, got %s", parts[3].Name)
	}
}

// ─── dropPartitions ──────────────────────────────────────────────────────────────────

func TestDropPartitions(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)

	if err := indexer.CreateIndexTables(context.Background(), db, 5, false, nil); err != nil {
		t.Fatalf("indexer.CreateIndexTables failed: %v", err)
	}

	// List the first partition to drop.
	parts, _ := listPartitions(context.Background(), db, dbName)
	if len(parts) < 2 {
		t.Fatal("need at least 2 partitions to test drop")
	}

	toDrop := parts[0].Name // drop the first daily partition
	if err := dropPartitions(context.Background(), db, dbName, []string{toDrop}); err != nil {
		t.Fatalf("dropPartitions failed: %v", err)
	}

	// Verify count decreased.
	partsAfter, _ := listPartitions(context.Background(), db, dbName)
	if len(partsAfter) != len(parts)-1 {
		t.Errorf("expected %d partitions after drop, got %d", len(parts)-1, len(partsAfter))
	}

	// Verify the dropped partition is gone.
	for _, p := range partsAfter {
		if p.Name == toDrop {
			t.Errorf("partition %s should have been dropped", toDrop)
		}
	}
}

// ─── partitionHasData ────────────────────────────────────────────────────────────────

func TestPartitionHasData_empty(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db) // creates binlog_events with only p_future

	has, err := partitionHasData(context.Background(), db, dbName)
	if err != nil {
		t.Fatalf("partitionHasData failed: %v", err)
	}
	if has {
		t.Error("expected false for empty p_future partition")
	}
}

func TestPartitionHasData_withData(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	// Insert a row into binlog_events. It will land in p_future since
	// InitIndexTables creates only p_future.
	testutil.InsertEvent(t, db,
		"binlog.000001", 100, 200,
		time.Now().UTC().Format("2006-01-02 15:04:05"),
		nil, "testdb", "orders", 1, "1",
		nil, nil, []byte(`{"id": 1}`),
	)

	has, err := partitionHasData(context.Background(), db, dbName)
	if err != nil {
		t.Fatalf("partitionHasData failed: %v", err)
	}
	if !has {
		t.Error("expected true when p_future has data")
	}
}

// ─── addFuturePartitions ─────────────────────────────────────────────────────────────

func TestAddFuturePartitions(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db) // only p_future

	startDate := time.Now().UTC().Truncate(time.Hour)

	if err := addFuturePartitions(context.Background(), db, dbName, startDate, 3); err != nil {
		t.Fatalf("addFuturePartitions failed: %v", err)
	}

	parts, _ := listPartitions(context.Background(), db, dbName)
	// Should be 3 hourly + p_future = 4.
	if len(parts) != 4 {
		t.Fatalf("expected 4 partitions, got %d", len(parts))
	}

	// The last one must still be p_future.
	if parts[len(parts)-1].Name != "p_future" {
		t.Errorf("expected last partition p_future, got %s", parts[len(parts)-1].Name)
	}

	// First 3 should be hourly partitions.
	for i := range 3 {
		expected := indexer.PartitionName(startDate.Add(time.Duration(i) * time.Hour))
		if parts[i].Name != expected {
			t.Errorf("partition %d: expected %s, got %s", i, expected, parts[i].Name)
		}
	}
}

// TestPerformRotation_S3UploadFailureDefers covers the path the review flagged:
// when archiving to S3 and the upload persistently fails, the partition is NOT
// dropped (data safe) AND the failure is counted into Result.Deferred so the
// built-in loop's unhealthy-streak escalation fires — instead of reporting a
// healthy cycle while the index grows unbounded.
func TestPerformRotation_S3UploadFailureDefers(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	h1 := time.Now().UTC().Add(-48 * time.Hour).Truncate(time.Hour)
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{h1})
	ts1 := h1.Add(30 * time.Minute).Format("2006-01-02 15:04:05")
	testutil.InsertEvent(t, db, "binlog.000001", 100, 200, ts1, nil, "testdb", "users", 1, "1", nil, nil, []byte(`{"id":1}`))

	// Stub the uploader to always fail, simulating a bad bucket / missing creds.
	prev := uploadFileFunc
	uploadFileFunc = func(ctx context.Context, client *s3.Client, path, bucket, key string) error {
		return fmt.Errorf("simulated S3 upload failure")
	}
	t.Cleanup(func() { uploadFileFunc = prev })

	res, err := Perform(context.Background(), db, dbName, Options{
		RetainDur:          24 * time.Hour,
		ArchiveDir:         t.TempDir(),
		ArchiveS3:          "s3://fake-bucket/prefix/",
		ArchiveS3Region:    "us-east-1",
		BintrailID:         "test-uuid-upload-fail",
		ArchiveCompression: "zstd",
		Format:             "json",
		NoReplace:          true,
	})
	if err != nil {
		t.Fatalf("Perform must not error on an upload failure (it defers): %v", err)
	}
	if res.Dropped != 0 {
		t.Errorf("Dropped = %d, want 0 (an un-uploaded partition must never be dropped)", res.Dropped)
	}
	if res.Deferred < 1 {
		t.Errorf("Deferred = %d, want >=1 — a failed upload must count as deferred so the loop escalates", res.Deferred)
	}
	// The partition must still be present.
	partitions, err := listPartitions(context.Background(), db, dbName)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, p := range partitions {
		if p.Name == indexer.PartitionName(h1) {
			found = true
		}
	}
	if !found {
		t.Error("partition was dropped despite a failed S3 upload — data loss")
	}
}

// TestPerformRotation_S3UploadSuccessPrunesAndDrops covers the success path that
// no other test exercises: a confirmed S3 upload must stamp s3_uploaded_at, drop
// the partition, and (with PruneLocalAfterUpload) remove the local staging copy.
// A regression that stamped before the upload returned, or pruned the durable
// copy on the wrong branch, would be data loss this test catches.
func TestPerformRotation_S3UploadSuccessPrunesAndDrops(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	h1 := time.Now().UTC().Add(-48 * time.Hour).Truncate(time.Hour)
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{h1})
	ts1 := h1.Add(30 * time.Minute).Format("2006-01-02 15:04:05")
	testutil.InsertEvent(t, db, "binlog.000001", 100, 200, ts1, nil, "testdb", "users", 1, "1", nil, nil, []byte(`{"id":1}`))

	// Stub the uploader to succeed (no real S3); Perform then stamps
	// s3_uploaded_at, drops the partition, and prunes the local staging copy.
	prev := uploadFileFunc
	uploadFileFunc = func(ctx context.Context, client *s3.Client, path, bucket, key string) error {
		return nil
	}
	t.Cleanup(func() { uploadFileFunc = prev })

	archiveDir := t.TempDir()
	bintrailID := "test-uuid-upload-ok"
	outPath, _ := HiveArchivePath(archiveDir, bintrailID, indexer.PartitionName(h1))

	res, err := Perform(context.Background(), db, dbName, Options{
		RetainDur:             24 * time.Hour,
		ArchiveDir:            archiveDir,
		ArchiveS3:             "s3://fake-bucket/prefix/",
		ArchiveS3Region:       "us-east-1",
		BintrailID:            bintrailID,
		ArchiveCompression:    "zstd",
		Format:                "json",
		NoReplace:             true,
		PruneLocalAfterUpload: true,
	})
	if err != nil {
		t.Fatalf("Perform: %v", err)
	}
	if res.Dropped != 1 {
		t.Errorf("Dropped = %d, want 1 (a successfully uploaded partition is dropped)", res.Dropped)
	}
	if res.Deferred != 0 {
		t.Errorf("Deferred = %d, want 0", res.Deferred)
	}

	// s3_uploaded_at must be stamped so a later drop-only cycle sees the S3 copy
	// as durable (hasPendingS3Upload → false).
	var uploadedAt sql.NullTime
	if err := db.QueryRow(
		`SELECT s3_uploaded_at FROM archive_state WHERE partition_name = ? AND bintrail_id = ?`,
		indexer.PartitionName(h1), bintrailID,
	).Scan(&uploadedAt); err != nil {
		t.Fatalf("read s3_uploaded_at: %v", err)
	}
	if !uploadedAt.Valid {
		t.Error("s3_uploaded_at must be set after a successful upload")
	}

	// PruneLocalAfterUpload removed the local staging Parquet (reads fall back to S3).
	if _, err := os.Stat(outPath); !os.IsNotExist(err) {
		t.Errorf("local archive %s must be pruned after a confirmed upload; stat err = %v", outPath, err)
	}

	// The partition itself is gone.
	partitions, err := listPartitions(context.Background(), db, dbName)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range partitions {
		if p.Name == indexer.PartitionName(h1) {
			t.Error("partition should have been dropped after a successful upload")
		}
	}
}

// TestPerformRotation_TruncatedArchiveRetryReArchives reproduces the OLD
// buggy precondition from issue #802: a truncated (no valid Parquet footer)
// file already sits at the Hive archive path with size>0 — as a crash
// mid-write used to leave behind — but there is NO archive_state row for the
// partition (that INSERT only ever happened after a completed write).
// Before the fix, --retry's `fileExists(outPath)` trusted the truncated file,
// skipped re-archiving AND skipped the archive_state INSERT, then uploaded
// the corrupt bytes to S3 as-is; since no archive_state row ever existed, the
// pending-upload guard saw nothing pending and rotation dropped the partition
// and (with PruneLocalAfterUpload) deleted the local copy too — the hour of
// data then existed only as an unreadable Parquet file in S3. The fixed code
// must recognize the file is unverifiable (no archive_state row confirms its
// row count) and re-archive it before ever uploading or dropping anything.
func TestPerformRotation_TruncatedArchiveRetryReArchives(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	h1 := time.Now().UTC().Add(-48 * time.Hour).Truncate(time.Hour)
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{h1})
	ts1 := h1.Add(30 * time.Minute).Format("2006-01-02 15:04:05")
	testutil.InsertEvent(t, db, "binlog.000001", 100, 200, ts1, nil, "testdb", "users", 1, "1", nil, nil, []byte(`{"id":1}`))

	archiveDir := t.TempDir()
	bintrailID := "test-uuid-truncated"
	outPath, _ := HiveArchivePath(archiveDir, bintrailID, indexer.PartitionName(h1))
	if err := os.MkdirAll(filepath.Dir(outPath), 0o755); err != nil {
		t.Fatal(err)
	}
	// Simulate the crash-mid-write leftover: a "parquet" file with content
	// that has no valid footer, size>0, and (deliberately) no archive_state
	// row inserted for it yet.
	const garbage = "truncated-not-a-real-parquet-footer"
	if err := os.WriteFile(outPath, []byte(garbage), 0o644); err != nil {
		t.Fatal(err)
	}

	var uploadedBytes []byte
	prev := uploadFileFunc
	uploadFileFunc = func(ctx context.Context, client *s3.Client, path, bucket, key string) error {
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		uploadedBytes = b
		return nil
	}
	t.Cleanup(func() { uploadFileFunc = prev })

	res, err := Perform(context.Background(), db, dbName, Options{
		RetainDur:          24 * time.Hour,
		ArchiveDir:         archiveDir,
		ArchiveS3:          "s3://fake-bucket/prefix/",
		ArchiveS3Region:    "us-east-1",
		BintrailID:         bintrailID,
		ArchiveCompression: "none",
		Format:             "json",
		NoReplace:          true,
		Retry:              true,
	})
	if err != nil {
		t.Fatalf("Perform: %v", err)
	}
	if res.Dropped != 1 {
		t.Errorf("Dropped = %d, want 1 (re-archived successfully, then uploaded and dropped)", res.Dropped)
	}

	if uploadedBytes == nil {
		t.Fatal("expected an S3 upload to occur")
	}
	if string(uploadedBytes) == garbage {
		t.Fatal("uploaded the truncated garbage file instead of re-archiving it first")
	}
	pf, err := parquet.OpenFile(bytes.NewReader(uploadedBytes), int64(len(uploadedBytes)))
	if err != nil {
		t.Fatalf("uploaded bytes are not a valid parquet file (footer): %v", err)
	}
	if pf.NumRows() != 1 {
		t.Errorf("uploaded parquet NumRows = %d, want 1", pf.NumRows())
	}

	// archive_state must now record the real row count — the row a crash
	// mid-write never got to insert.
	var rowCount sql.NullInt64
	if err := db.QueryRow(
		`SELECT row_count FROM archive_state WHERE partition_name = ? AND bintrail_id = ?`,
		indexer.PartitionName(h1), bintrailID,
	).Scan(&rowCount); err != nil {
		t.Fatalf("read archive_state: %v", err)
	}
	if !rowCount.Valid || rowCount.Int64 != 1 {
		t.Errorf("archive_state.row_count = %v, want 1", rowCount)
	}
}

// TestPerformRotation_GrownAfterArchiveDefersDropAndReArchives is the archive→
// drop TOCTOU regression (issue #779), and simultaneously pins the #802 side
// that must NOT regress: a genuinely complete prior archive — a Parquet file
// whose footer row count matches what archive_state recorded — is still trusted
// and skipped under --retry, never needlessly re-archived.
//
// Scenario: a valid 1-row archive already exists; then a second live row lands
// in the same partition after the archive was taken (a backfilled gap replayed
// with original binlog timestamps into the oldest RANGE partition). Cycle 1
// under --retry correctly SKIPS re-archiving the still-valid file (#802), but
// the drop must be DEFERRED, not taken: dropping now would erase the second row
// from BOTH the index and the archive (#779). The now-incomplete staged archive
// is discarded so cycle 2 re-archives the full (2-row) partition and only then
// drops it — proving the "leave for the next cycle" path converges.
func TestPerformRotation_GrownAfterArchiveDefersDropAndReArchives(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	h1 := time.Now().UTC().Add(-48 * time.Hour).Truncate(time.Hour)
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{h1})
	ts1 := h1.Add(30 * time.Minute).Format("2006-01-02 15:04:05")
	testutil.InsertEvent(t, db, "binlog.000001", 100, 200, ts1, nil, "testdb", "users", 1, "1", nil, nil, []byte(`{"id":1}`))

	archiveDir := t.TempDir()
	bintrailID := "test-uuid-toctou"
	partName := indexer.PartitionName(h1)
	outPath, _ := HiveArchivePath(archiveDir, bintrailID, partName)
	if err := os.MkdirAll(filepath.Dir(outPath), 0o755); err != nil {
		t.Fatal(err)
	}
	// Seed a genuinely complete, valid 1-row archive up front (as a prior
	// successful run would have left it) and record it in archive_state —
	// the state a legitimate --retry skip depends on.
	if _, err := archive.ArchivePartition(context.Background(), db, dbName, partName, outPath, "none"); err != nil {
		t.Fatalf("seed ArchivePartition: %v", err)
	}
	origBytes, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatal(err)
	}
	testutil.MustExec(t, db, `INSERT INTO archive_state
		(partition_name, bintrail_id, local_path, row_count)
		VALUES (?, ?, ?, 1)`, partName, bintrailID, outPath)

	// The second live row lands in the partition AFTER the archive was taken.
	ts2 := h1.Add(40 * time.Minute).Format("2006-01-02 15:04:05")
	testutil.InsertEvent(t, db, "binlog.000001", 300, 400, ts2, nil, "testdb", "users", 1, "2", nil, nil, []byte(`{"id":2}`))

	var uploadedBytes []byte
	prev := uploadFileFunc
	uploadFileFunc = func(ctx context.Context, client *s3.Client, path, bucket, key string) error {
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		uploadedBytes = b
		return nil
	}
	t.Cleanup(func() { uploadFileFunc = prev })

	opts := Options{
		RetainDur:          24 * time.Hour,
		ArchiveDir:         archiveDir,
		ArchiveS3:          "s3://fake-bucket/prefix/",
		ArchiveS3Region:    "us-east-1",
		BintrailID:         bintrailID,
		ArchiveCompression: "none",
		Format:             "json",
		NoReplace:          true,
		Retry:              true,
	}

	// ── Cycle 1: the partition grew since it was archived → defer the drop. ──
	res, err := Perform(context.Background(), db, dbName, opts)
	if err != nil {
		t.Fatalf("Perform cycle 1: %v", err)
	}
	if res.Dropped != 0 {
		t.Errorf("cycle 1 Dropped = %d, want 0 (partition grew since archive; must not be dropped)", res.Dropped)
	}
	if res.Deferred != 1 {
		t.Errorf("cycle 1 Deferred = %d, want 1 (TOCTOU guard)", res.Deferred)
	}
	// #802 half: the still-valid 1-row file was skipped, not re-archived —
	// the bytes uploaded are the original 1-row archive, unchanged.
	if !bytes.Equal(uploadedBytes, origBytes) {
		t.Error("a valid, already-recorded archive was re-archived (bytes changed) instead of being skipped")
	}
	// #779 half: the partition (and both rows) must survive the deferral.
	if !partitionExists(t, db, dbName, partName) {
		t.Errorf("partition %s must NOT have been dropped after the guard deferred it", partName)
	}
	if got := livePartitionCount(t, db, dbName, partName); got != 2 {
		t.Errorf("live partition row count = %d, want 2 (both rows must survive the deferral)", got)
	}
	// The incomplete staged archive is discarded: no archive_state row and no
	// local file, so cycle 2 re-archives from scratch rather than trusting it.
	var stillRecorded bool
	if err := db.QueryRow(
		`SELECT EXISTS(SELECT 1 FROM archive_state WHERE partition_name = ? AND bintrail_id = ?)`,
		partName, bintrailID,
	).Scan(&stillRecorded); err != nil {
		t.Fatalf("check archive_state: %v", err)
	}
	if stillRecorded {
		t.Error("stale archive_state row must be discarded on deferral so --retry re-archives next cycle")
	}
	if _, statErr := os.Stat(outPath); !os.IsNotExist(statErr) {
		t.Errorf("stale local archive %s must be removed on deferral; stat err = %v", outPath, statErr)
	}

	// ── Cycle 2: re-archive the full partition, then drop it (convergence). ──
	uploadedBytes = nil
	res, err = Perform(context.Background(), db, dbName, opts)
	if err != nil {
		t.Fatalf("Perform cycle 2: %v", err)
	}
	if res.Dropped != 1 {
		t.Errorf("cycle 2 Dropped = %d, want 1 (re-archived the full partition, then dropped)", res.Dropped)
	}
	if partitionExists(t, db, dbName, partName) {
		t.Errorf("partition %s should have been dropped in cycle 2 after a complete re-archive", partName)
	}
	if uploadedBytes == nil {
		t.Fatal("cycle 2 must re-upload the re-archived partition")
	}
	pf, err := parquet.OpenFile(bytes.NewReader(uploadedBytes), int64(len(uploadedBytes)))
	if err != nil {
		t.Fatalf("cycle 2 uploaded bytes are not a valid parquet file: %v", err)
	}
	if pf.NumRows() != 2 {
		t.Errorf("cycle 2 uploaded parquet NumRows = %d, want 2 (the full grown partition)", pf.NumRows())
	}
}

// partitionExists reports whether binlog_events still has the named partition.
func partitionExists(t *testing.T, db *sql.DB, dbName, name string) bool {
	t.Helper()
	parts, err := listPartitions(context.Background(), db, dbName)
	if err != nil {
		t.Fatalf("listPartitions: %v", err)
	}
	for _, p := range parts {
		if p.Name == name {
			return true
		}
	}
	return false
}

// livePartitionCount returns the live row count of a single partition.
func livePartitionCount(t *testing.T, db *sql.DB, dbName, name string) int64 {
	t.Helper()
	var c int64
	if err := db.QueryRow(
		fmt.Sprintf("SELECT COUNT(*) FROM `%s`.`binlog_events` PARTITION (`%s`)", dbName, name),
	).Scan(&c); err != nil {
		t.Fatalf("count partition %s: %v", name, err)
	}
	return c
}

// TestPerformRotation_LocalOnlyArchiveNotPruned pins the inverse invariant: with
// no ArchiveS3 the local Parquet IS the durable copy, so PruneLocalAfterUpload
// must be a no-op even when set. Deleting it would be silent data loss.
func TestPerformRotation_LocalOnlyArchiveNotPruned(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	h1 := time.Now().UTC().Add(-48 * time.Hour).Truncate(time.Hour)
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{h1})
	ts1 := h1.Add(30 * time.Minute).Format("2006-01-02 15:04:05")
	testutil.InsertEvent(t, db, "binlog.000001", 100, 200, ts1, nil, "testdb", "users", 1, "1", nil, nil, []byte(`{"id":1}`))

	archiveDir := t.TempDir()
	bintrailID := "test-uuid-local-only"
	outPath, _ := HiveArchivePath(archiveDir, bintrailID, indexer.PartitionName(h1))

	res, err := Perform(context.Background(), db, dbName, Options{
		RetainDur:             24 * time.Hour,
		ArchiveDir:            archiveDir, // local archive, no S3
		BintrailID:            bintrailID,
		ArchiveCompression:    "zstd",
		Format:                "json",
		NoReplace:             true,
		PruneLocalAfterUpload: true, // must be ignored without ArchiveS3
	})
	if err != nil {
		t.Fatalf("Perform: %v", err)
	}
	if res.Dropped != 1 {
		t.Errorf("Dropped = %d, want 1", res.Dropped)
	}
	if _, err := os.Stat(outPath); err != nil {
		t.Errorf("local-only archive %s must survive (it is the durable copy); stat err = %v", outPath, err)
	}
}

// ─── backfill archived under the wrong hour label (#1037) ────────────────────

// TestPerformRotation_BackfilledPartitionStaysTimeQueryable reproduces #1037
// end-to-end: a capture stall is recovered by replaying two-day-old events,
// which RANGE partitioning files into the OLDEST live partition; rotation then
// archives that partition under its own hour label. Before the fix any
// time-scoped read for the backfilled window missed those rows — the planner
// classified the hours as gaps (label-only archive coverage → strict mode
// aborted with a GapError) and label-pruned S3 listings skipped the file.
//
// With content-derived pruning, rotation records the archive's true
// MIN/MAX(event_timestamp) in archive_state; the planner counts the spanned
// hours as covered and reports the misfiled label so the fetch opens the file.
func TestPerformRotation_BackfilledPartitionStaysTimeQueryable(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	// Oldest (and only named) live partition, already past retention.
	h1 := time.Now().UTC().Add(-48 * time.Hour).Truncate(time.Hour)
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{h1})

	// Backfilled events carry their ORIGINAL binlog timestamps, two days and
	// one day before h1 — far older than the partition's hour label. The
	// native event belongs to h1's own hour.
	backfillOld := h1.Add(-48 * time.Hour).Add(10 * time.Minute)
	backfillMid := h1.Add(-24 * time.Hour).Add(20 * time.Minute)
	native := h1.Add(30 * time.Minute)
	const tsFmt = "2006-01-02 15:04:05"
	testutil.InsertEvent(t, db, "binlog.000001", 100, 200, backfillOld.Format(tsFmt), nil, "testdb", "users", 1, "1", nil, nil, []byte(`{"id":1}`))
	testutil.InsertEvent(t, db, "binlog.000001", 200, 300, backfillMid.Format(tsFmt), nil, "testdb", "users", 1, "2", nil, nil, []byte(`{"id":2}`))
	testutil.InsertEvent(t, db, "binlog.000001", 300, 400, native.Format(tsFmt), nil, "testdb", "users", 1, "3", nil, nil, []byte(`{"id":3}`))

	archiveDir := t.TempDir()
	bintrailID := "test-uuid-1037"
	res, err := Perform(context.Background(), db, dbName, Options{
		RetainDur:          24 * time.Hour,
		ArchiveDir:         archiveDir,
		BintrailID:         bintrailID,
		ArchiveCompression: "none",
		Format:             "text",
		NoReplace:          true,
	})
	if err != nil {
		t.Fatalf("Perform: %v", err)
	}
	if res.Dropped != 1 {
		t.Fatalf("expected 1 partition dropped, got %d", res.Dropped)
	}

	// archive_state must record the CONTENT time range, not the label hour.
	var minTS, maxTS sql.NullTime
	if err := db.QueryRow(
		`SELECT min_event_ts, max_event_ts FROM archive_state WHERE partition_name = ? AND bintrail_id = ?`,
		indexer.PartitionName(h1), bintrailID,
	).Scan(&minTS, &maxTS); err != nil {
		t.Fatalf("read archive_state content range: %v", err)
	}
	if !minTS.Valid || !minTS.Time.UTC().Equal(backfillOld) {
		t.Errorf("min_event_ts = %v (valid=%v), want %v", minTS.Time, minTS.Valid, backfillOld)
	}
	if !maxTS.Valid || !maxTS.Time.UTC().Equal(native) {
		t.Errorf("max_event_ts = %v (valid=%v), want %v", maxTS.Time, maxTS.Valid, native)
	}

	// The mutation-killer: a STRICT time-scoped read of the backfilled
	// window must find the replayed event. On pre-#1037 main this failed
	// twice over — FetchMerged returned a GapError (the hours counted as
	// uncovered) and label-based file pruning skipped the archive.
	since := backfillOld.Add(-time.Minute)
	until := backfillOld.Add(time.Hour)
	rows, plan, err := query.FetchMerged(context.Background(), db, query.New(db), query.FetchMergedOptions{
		Opts:           query.Options{Schema: "testdb", Table: "users", Since: &since, Until: &until},
		DBName:         dbName,
		AllowGaps:      false,
		ArchiveFetcher: parquetquery.Fetch,
	})
	if err != nil {
		t.Fatalf("strict FetchMerged over the backfilled window: %v", err)
	}
	if len(rows) != 1 || rows[0].PKValues != "1" {
		t.Fatalf("expected exactly the backfilled event (pk=1), got %d rows: %+v", len(rows), rows)
	}
	if plan == nil {
		t.Fatal("expected a query plan")
	}
	foundLabel := false
	for _, h := range plan.MisfiledArchiveHours {
		if h.Equal(h1) {
			foundLabel = true
		}
	}
	if !foundLabel {
		t.Errorf("plan.MisfiledArchiveHours = %v, want to include the misfiled label hour %v", plan.MisfiledArchiveHours, h1)
	}
}
