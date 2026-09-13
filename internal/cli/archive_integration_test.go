//go:build integration

package cli

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/archive"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/rotation"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestArchiveReconcileRebuild covers #392's headline scenario end-to-end
// against real MySQL + a real Parquet archive: the registry is lost (index
// rebuild), reconcile --repair re-registers the file, and the rebuilt row
// is indistinguishable from one rotate would have written — row_count and
// file_size filled from the real footer/stat, partition name preserved.
func TestArchiveReconcileRebuild(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	ctx := context.Background()

	// One partition with one real event, archived to a real Parquet file
	// in the production Hive layout.
	h1 := time.Now().UTC().Add(-48 * time.Hour).Truncate(time.Hour)
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{h1})
	ts1 := h1.Add(30 * time.Minute).Format("2006-01-02 15:04:05")
	testutil.InsertEvent(t, db, "binlog.000001", 100, 200, ts1, nil,
		"testdb", "orders", 1, "42", nil, nil, []byte(`{"id":42}`))

	archiveDir := t.TempDir()
	const bintrailID = "deadbeef-dead-beef-dead-beefdeadbeef" // 36 chars, matches archivePathRe
	outPath, err := rotation.HiveArchivePath(archiveDir, bintrailID, indexer.PartitionName(h1))
	if err != nil {
		t.Fatalf("hiveArchivePath: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(outPath), 0o755); err != nil {
		t.Fatal(err)
	}
	n, err := archive.ArchivePartition(ctx, db, dbName, indexer.PartitionName(h1), outPath, "zstd")
	if err != nil {
		t.Fatalf("ArchivePartition: %v", err)
	}
	if n.Rows != 1 {
		t.Fatalf("expected 1 archived row, got %d", n.Rows)
	}

	// The registry is LOST (index rebuilt): archive_state is empty.
	// Scan + diff + repair must restore it.
	files, err := scanLocalArchive(archiveDir)
	if err != nil {
		t.Fatalf("scanLocalArchive: %v", err)
	}
	if len(files) != 1 {
		t.Fatalf("expected 1 scanned file, got %d: %+v", len(files), files)
	}
	if files[0].BintrailID != bintrailID || files[0].PartitionName != indexer.PartitionName(h1) {
		t.Fatalf("scan derivation wrong: %+v", files[0])
	}
	if !files[0].RowCount.Valid || files[0].RowCount.Int64 != 1 {
		t.Fatalf("local scan must read row_count from the footer, got %+v", files[0].RowCount)
	}

	rows, err := loadArchiveStateRows(ctx, db)
	if err != nil {
		t.Fatalf("loadArchiveStateRows: %v", err)
	}
	rep := archive.Diff(files, rows, archive.DiffOptions{
		ScannedLocal: true, PruneMinAge: time.Hour, Now: time.Now().UTC(),
	})
	if rep.Inserts != 1 {
		t.Fatalf("want 1 insert, got %+v", rep)
	}
	if rep.Err() == nil {
		t.Fatal("dry-run drift must be a non-nil Err (cron exit contract)")
	}

	executed, errs := executeReconcileActions(ctx, db, rep.Actions, true /*repair*/, false)
	if len(errs) != 0 || executed != 1 {
		t.Fatalf("repair: executed=%d errs=%v", executed, errs)
	}

	// The rebuilt row must look exactly like rotate's: same key, local
	// path, real size, footer row_count, and NO phantom S3-pending state.
	var (
		localPath          sql.NullString
		fileSize, rowCount sql.NullInt64
		s3Bucket           sql.NullString
		s3UploadedAt       sql.NullTime
	)
	if err := db.QueryRowContext(ctx, `
		SELECT local_path, file_size_bytes, row_count, s3_bucket, s3_uploaded_at
		FROM archive_state WHERE partition_name = ? AND bintrail_id = ?`,
		indexer.PartitionName(h1), bintrailID).
		Scan(&localPath, &fileSize, &rowCount, &s3Bucket, &s3UploadedAt); err != nil {
		t.Fatalf("rebuilt row missing: %v", err)
	}
	if !localPath.Valid || localPath.String != outPath {
		t.Errorf("local_path = %+v, want %s", localPath, outPath)
	}
	if !rowCount.Valid || rowCount.Int64 != 1 {
		t.Errorf("row_count = %+v, want 1", rowCount)
	}
	fi, _ := os.Stat(outPath)
	if !fileSize.Valid || fileSize.Int64 != fi.Size() {
		t.Errorf("file_size_bytes = %+v, want %d", fileSize, fi.Size())
	}
	if s3Bucket.Valid {
		t.Errorf("local-only repair must not invent S3 columns: %+v", s3Bucket)
	}

	// Idempotency: a second diff over the repaired state is clean.
	rows2, _ := loadArchiveStateRows(ctx, db)
	rep2 := archive.Diff(files, rows2, archive.DiffOptions{
		ScannedLocal: true, PruneMinAge: time.Hour, Now: time.Now().UTC(),
	})
	if len(rep2.Actions) != 0 || rep2.Err() != nil {
		t.Fatalf("post-repair diff must be clean, got %+v err=%v", rep2, rep2.Err())
	}
}

// TestArchiveReconcilePruneGates covers the registry-safety rules against
// real MySQL: a partial scan never prunes a row referencing an unscanned
// backend, and --prune deletes only fully-verified orphans past the margin.
func TestArchiveReconcilePruneGates(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	ctx := context.Background()

	old := time.Now().UTC().Add(-2 * time.Hour).Format("2006-01-02 15:04:05")
	// Row 1: S3-only registration (healthy bucket we are NOT scanning).
	testutil.MustExec(t, db, `INSERT INTO archive_state
		(partition_name, bintrail_id, s3_bucket, s3_key, s3_uploaded_at, row_count, archived_at)
		VALUES ('p_2026060410', 's3-only-row-aaaa-aaaa-aaaaaaaaaaaa', 'bkt', 'k/events.parquet', ?, 5, ?)`, old, old)
	// Row 2: local-only registration whose file never existed (orphan).
	testutil.MustExec(t, db, `INSERT INTO archive_state
		(partition_name, bintrail_id, local_path, row_count, archived_at)
		VALUES ('p_2026060411', 'local-orphan-aaaa-aaaa-aaaaaaaaaaaa', '/nonexistent/bintrail_id=x/events.parquet', 5, ?)`, old)
	// Row 3: local-only orphan but RECENT (inside the margin).
	testutil.MustExec(t, db, `INSERT INTO archive_state
		(partition_name, bintrail_id, local_path, row_count, archived_at)
		VALUES ('p_2026060412', 'recent-orphan-aaa-aaaa-aaaaaaaaaaaa', '/nonexistent2/bintrail_id=y/events.parquet', 5, UTC_TIMESTAMP())`)

	// The scan dir holds ONE unrelated layout file: testimony that the
	// scanner can see the layout (an empty scan refuses to prune — the
	// blind-scanner gate).
	scanDir := t.TempDir()
	witness := filepath.Join(scanDir, "bintrail_id=witness-id-0000-0000-000000000000",
		"event_date=2026-06-04", "event_hour=09", "events.parquet")
	if err := os.MkdirAll(filepath.Dir(witness), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(witness, []byte("not-a-real-parquet"), 0o644); err != nil {
		t.Fatal(err)
	}
	files, err := scanLocalArchive(scanDir)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := loadArchiveStateRows(ctx, db)
	if err != nil {
		t.Fatal(err)
	}

	rep := archive.Diff(files, rows, archive.DiffOptions{
		ScannedLocal: true, ScannedS3: false, PruneMinAge: time.Hour, Now: time.Now().UTC(),
	})
	if rep.Inserts != 1 { // the witness file has no row — expected
		t.Errorf("witness should be an insert candidate, got %+v", rep)
	}
	if rep.SkippedUnverified != 1 {
		t.Errorf("S3-only row under a local-only scan must be skip-unverified, got %+v", rep)
	}
	if rep.SkippedRecent != 1 {
		t.Errorf("recent orphan must be skip-recent, got %+v", rep)
	}
	if rep.Prunes != 1 {
		t.Fatalf("exactly the old local orphan must be prunable, got %+v", rep)
	}

	executed, errs := executeReconcileActions(ctx, db, rep.Actions, false, true /*prune*/)
	if len(errs) != 0 || executed != 1 {
		t.Fatalf("prune: executed=%d errs=%v", executed, errs)
	}

	var remaining int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM archive_state`).Scan(&remaining); err != nil {
		t.Fatal(err)
	}
	if remaining != 2 {
		t.Fatalf("prune must delete ONLY the verified old orphan: %d rows remain, want 2", remaining)
	}
	var gone int
	if err := db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM archive_state WHERE partition_name = 'p_2026060411'`).Scan(&gone); err != nil {
		t.Fatal(err)
	}
	if gone != 0 {
		t.Fatal("the verified orphan row should be the one pruned")
	}
}

// TestParseArchivePathRoundTrip guards the layout coupling: every path
// hiveArchivePath writes must be parsed back to the same identity by
// parseArchivePath (the scan-side derivation reconcile depends on).
func TestParseArchivePathRoundTrip(t *testing.T) {
	const id = "97adaf56-fe9e-4c1b-9794-b042f7faf197"
	hours := []time.Time{
		time.Date(2026, 6, 5, 0, 0, 0, 0, time.UTC),
		time.Date(2026, 6, 5, 9, 0, 0, 0, time.UTC),
		time.Date(2026, 12, 31, 23, 0, 0, 0, time.UTC),
	}
	for _, h := range hours {
		p, err := rotation.HiveArchivePath("/var/archives", id, indexer.PartitionName(h))
		if err != nil {
			t.Fatalf("rotation.HiveArchivePath(%v): %v", h, err)
		}
		gotID, gotPart := archive.ParseArchivePath(p)
		if gotID != id || gotPart != indexer.PartitionName(h) {
			t.Errorf("round-trip(%s): got (%s, %s), want (%s, %s)", p, gotID, gotPart, id, indexer.PartitionName(h))
		}
	}
}

// TestArchiveReconcileRecordsTheColumnSet is #1535's backfill end to end: a real
// Parquet archive, a registry with no recorded column set, and a repair that
// fills it from the footer it already reads — after which query.ArchiveGroups
// can group the layout and the views bind one footer per group instead of one
// per file.
//
// Against real MySQL and a real footer on purpose. The unit tests drive Diff
// with a hand-set ColumnSet, so they would still pass if the scan never read
// the names, if the value rotation records disagreed with the file, or if the
// column could not be written to archive_state at all.
func TestArchiveReconcileRecordsTheColumnSet(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	ctx := context.Background()

	h1 := time.Now().UTC().Add(-48 * time.Hour).Truncate(time.Hour)
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{h1})
	testutil.InsertEvent(t, db, "binlog.000001", 100, 200,
		h1.Add(30*time.Minute).Format("2006-01-02 15:04:05"), nil,
		"testdb", "orders", 1, "42", nil, nil, []byte(`{"id":42}`))

	archiveDir := t.TempDir()
	const bintrailID = "deadbeef-dead-beef-dead-beefdeadbeef"
	part := indexer.PartitionName(h1)
	outPath, err := rotation.HiveArchivePath(archiveDir, bintrailID, part)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(outPath), 0o755); err != nil {
		t.Fatal(err)
	}
	stats, err := archive.ArchivePartition(ctx, db, dbName, part, outPath, "zstd")
	if err != nil {
		t.Fatalf("ArchivePartition: %v", err)
	}
	// The writer reports the set it wrote, which is what rotation records.
	if stats.Columns != archive.ColumnSet(archive.BinlogEventColumns) {
		t.Fatalf("ArchivePartition reported column set %q, want the archive's own", stats.Columns)
	}

	// A registry row that predates the column: everything else recorded, no
	// column set. This is every deployment on the day it upgrades.
	if _, err := db.ExecContext(ctx,
		`INSERT INTO archive_state (partition_name, bintrail_id, local_path, file_size_bytes, row_count)
		 VALUES (?, ?, ?, ?, ?)`, part, bintrailID, outPath, 1, 1); err != nil {
		t.Fatal(err)
	}

	files, err := scanLocalArchive(archiveDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 || files[0].ColumnSet != stats.Columns {
		t.Fatalf("the local scan read column set %q from the real footer, want %q",
			files[0].ColumnSet, stats.Columns)
	}
	rows, err := loadArchiveStateRows(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	// Deep is FALSE: the backfill must not need it. A local footer read has
	// already happened for row_count, so requiring --deep here would leave
	// every plain reconcile unable to fix the thing it just measured.
	rep := archive.Diff(files, rows, archive.DiffOptions{
		ScannedLocal: true, PruneMinAge: time.Hour, Now: time.Now().UTC(),
	})
	if executed, errs := executeReconcileActions(ctx, db, rep.Actions, true, false); len(errs) != 0 || executed != 1 {
		t.Fatalf("repair: executed=%d errs=%v", executed, errs)
	}

	var recorded sql.NullString
	if err := db.QueryRowContext(ctx,
		`SELECT column_set FROM archive_state WHERE partition_name = ? AND bintrail_id = ?`,
		part, bintrailID).Scan(&recorded); err != nil {
		t.Fatalf("read back column_set: %v", err)
	}
	if !recorded.Valid || recorded.String != stats.Columns {
		t.Fatalf("column_set = %+v, want %q", recorded, stats.Columns)
	}

	// And the payoff: the registry can now group the layout.
	groups, ungrouped, err := query.ArchiveGroups(ctx, db,
		[]string{filepath.Join(archiveDir, "bintrail_id="+bintrailID)})
	if err != nil {
		t.Fatal(err)
	}
	if ungrouped != 0 {
		t.Fatalf("ungrouped = %d after a repair; the views would still bind every footer", ungrouped)
	}
	if len(groups) != 1 || len(groups[0].Files) != 1 || groups[0].Files[0] != outPath {
		t.Fatalf("groups = %+v, want the archived file under its own source", groups)
	}

	// A second reconcile finds nothing to do. Without this the repair reports
	// drift forever and a cron never goes green again.
	rows, err = loadArchiveStateRows(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	rep = archive.Diff(files, rows, archive.DiffOptions{
		ScannedLocal: true, PruneMinAge: time.Hour, Now: time.Now().UTC(),
	})
	if rep.Err() != nil {
		t.Errorf("a repaired registry still reports drift: %v", rep.Err())
	}
}
