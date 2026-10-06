//go:build integration

package rotation_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/parquetquery"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/rotation"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// #2152 end to end, across the real seam: rotation writes the archive_state
// row, the snapshot update's fetch reads it.
//
// A snapshot was taken at T, at binlog position 1000. Afterwards capture
// indexes a LATE change (position 1500) that ran on the source two days
// earlier: it lands in an old partition. Rotation archives that partition and
// drops it before the next update runs. The update continues from position
// 1000 and must still apply the change: before #2152 nothing live showed it,
// the fetch kept T as its floor, pruned the archive file, and the change was
// left out with no error.
func TestIntegrationLateEventArchivedBeforeTheNextUpdateIsStillFetched(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	ctx := context.Background()

	now := time.Now().UTC()
	old := now.Add(-48 * time.Hour).Truncate(time.Hour) // rotated out below
	recent := now.Add(-2 * time.Hour).Truncate(time.Hour)
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{old, recent, recent.Add(time.Hour), recent.Add(2 * time.Hour)})
	ts := func(t time.Time) string { return t.Format("2006-01-02 15:04:05") }
	ins := func(pos uint64, at time.Time, table, pk string) {
		testutil.InsertEvent(t, db, "binlog.000001", pos, pos+50, ts(at), nil,
			"shop", table, 1, pk, nil, nil, []byte(fmt.Sprintf(`{"id":%s}`, pk)))
	}
	// Before the snapshot, in binlog order.
	ins(100, old.Add(10*time.Minute), "orders", "1")
	ins(800, recent.Add(10*time.Minute), "orders", "2")
	snapTime := now.Add(-30 * time.Minute)
	anchor := &query.BinlogPos{File: "binlog.000001", Pos: 1000}
	// After the snapshot: the late change, in the old hour.
	ins(1500, old.Add(40*time.Minute), "orders", "7")

	const bintrailID = "2152beef-dead-beef-dead-beefdeadbeef"
	archiveDir := t.TempDir()
	if _, err := rotation.Perform(ctx, db, dbName, rotation.Options{
		RetainDur: 24 * time.Hour, RetainRaw: "24h",
		ArchiveDir: archiveDir, ArchiveCompression: "zstd",
		BintrailID: bintrailID, Format: "json",
	}); err != nil {
		t.Fatalf("rotation.Perform: %v", err)
	}
	var still int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM information_schema.PARTITIONS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'binlog_events' AND PARTITION_NAME = ?`, dbName, indexer.PartitionName(old)).Scan(&still); err != nil {
		t.Fatal(err)
	}
	if still != 0 {
		t.Fatal("fixture: the old partition must be archived AND dropped before the update")
	}

	// What rotation recorded about the file.
	var maxID sql.Null[uint64]
	var maxFile sql.NullString
	var maxPos sql.Null[uint64]
	if err := db.QueryRowContext(ctx, `SELECT max_event_id, max_binlog_file, max_start_pos FROM archive_state
		WHERE partition_name = ? AND bintrail_id = ?`, indexer.PartitionName(old), bintrailID).Scan(&maxID, &maxFile, &maxPos); err != nil {
		t.Fatalf("read archive_state: %v", err)
	}
	if !maxID.Valid || maxID.V == 0 || maxFile.String != "binlog.000001" || maxPos.V != 1500 {
		t.Fatalf("archive_state record = id %+v, %+v:%+v; want the late change's binlog.000001:1500", maxID, maxFile, maxPos)
	}

	rows, _, err := query.FetchMerged(ctx, db, query.New(db), query.FetchMergedOptions{
		Opts:           query.Options{Schema: "shop", Table: "orders", Since: &snapTime, SincePos: anchor},
		DBName:         dbName,
		ArchiveFetcher: parquetquery.Fetch,
	})
	if err != nil {
		t.Fatalf("FetchMerged: %v", err)
	}
	var got []string
	for _, r := range rows {
		got = append(got, fmt.Sprintf("%s:%d", r.PKValues, r.StartPos))
	}
	if len(rows) != 1 || rows[0].PKValues != "7" || rows[0].StartPos != 1500 {
		t.Fatalf("the update's fetch returned %v; want exactly the late change 7:1500, read from the archive", got)
	}

	// The steady state: an update anchored after everything the archive holds
	// keeps its own time. The archive was written after the snapshot (it is a
	// routine rotation), and its record is what keeps it from moving the start.
	h, err := query.LoadPartitionHeads(ctx, db)
	if err != nil {
		t.Fatalf("LoadPartitionHeads: %v", err)
	}
	later := &query.BinlogPos{File: "binlog.000001", Pos: 1501}
	since, err := h.SinceFor(ctx, db, query.Options{Schema: "shop", Table: "orders", Since: &snapTime, SincePos: later})
	if err != nil || !since.Equal(snapTime) {
		t.Fatalf("steady state: SinceFor = %v, err=%v; want the snapshot's own time %v", since, err, snapTime)
	}
}

// The floor reads archive_state whatever backend holds the file, how an
// unrecorded row counts, and what loading the picture costs on an index that
// keeps a year of hourly archives.
func TestIntegrationArchiveFloorShapesAndCost(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	ctx := context.Background()
	now := time.Now().UTC()
	recent := now.Add(-2 * time.Hour).Truncate(time.Hour)
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{recent, recent.Add(time.Hour), recent.Add(2 * time.Hour)})
	testutil.InsertEvent(t, db, "binlog.000009", 100, 150, recent.Add(time.Minute).Format("2006-01-02 15:04:05"), nil,
		"shop", "orders", 1, "1", nil, nil, []byte(`{"id":1}`))

	snapTime := now.Add(-30 * time.Minute)
	anchor := &query.BinlogPos{File: "binlog.000009", Pos: 1000}
	opts := query.Options{Schema: "shop", Table: "orders", Since: &snapTime, SincePos: anchor}
	sinceFor := func(t *testing.T) time.Time {
		t.Helper()
		h, err := query.LoadPartitionHeads(ctx, db)
		if err != nil {
			t.Fatalf("LoadPartitionHeads: %v", err)
		}
		got, err := h.SinceFor(ctx, db, opts)
		if err != nil {
			t.Fatalf("SinceFor: %v", err)
		}
		return *got
	}
	hourAgo := func(n int) time.Time { return now.Add(-time.Duration(n) * time.Hour).Truncate(time.Hour) }
	// A year of routine archives: recorded, every newest change before the
	// anchor, written by rotation long before the snapshot. 8,760 rows.
	const year = 365 * 24
	stmt := `INSERT INTO archive_state (partition_name, bintrail_id, local_path, row_count, max_event_id, max_binlog_file, max_start_pos, archived_at) VALUES `
	for lo := 0; lo < year; lo += 1000 {
		q, args := stmt, []any{}
		for i := lo; i < min(lo+1000, year); i++ {
			if i > lo {
				q += ","
			}
			h := hourAgo(30*24 + year - i)
			q += "(?, 'feedface-0000-0000-0000-000000000000', '/a/x.parquet', 10, ?, 'binlog.000001', ?, NOW() - INTERVAL ? HOUR)"
			args = append(args, indexer.PartitionName(h), i+1, 4+i, year-i)
		}
		testutil.MustExec(t, db, q, args...)
	}
	start := time.Now()
	got := sinceFor(t)
	took := time.Since(start)
	t.Logf("picture load + SinceFor with %d archive_state rows: %s", year, took)
	if !got.Equal(snapTime) {
		t.Fatalf("steady state with a year of archives: SinceFor = %v, want the snapshot's own time", got)
	}

	// An archive held only in S3 (no local path), newest change after the
	// anchor: the start moves to its hour.
	s3Hour := hourAgo(40)
	testutil.MustExec(t, db, `INSERT INTO archive_state (partition_name, bintrail_id, s3_bucket, s3_key, row_count, max_event_id, max_binlog_file, max_start_pos)
		VALUES (?, 's3only-0000', 'bucket', 'k.parquet', 1, 99, 'binlog.000010', 4)`, indexer.PartitionName(s3Hour))
	if got := sinceFor(t); !got.Equal(s3Hour) {
		t.Fatalf("S3-only archive after the anchor: SinceFor = %v, want its hour %v", got, s3Hour)
	}
	testutil.MustExec(t, db, `DELETE FROM archive_state WHERE bintrail_id = 's3only-0000'`)

	// Not recorded and written before the snapshot (a row from an older
	// build): cannot hold a change the snapshot missed.
	testutil.MustExec(t, db, `INSERT INTO archive_state (partition_name, bintrail_id, local_path, row_count, archived_at)
		VALUES (?, 'legacy-0000', '/a/y.parquet', 1, NOW() - INTERVAL 3 HOUR)`, indexer.PartitionName(hourAgo(41)))
	if got := sinceFor(t); !got.Equal(snapTime) {
		t.Fatalf("unrecorded archive written before the snapshot: SinceFor = %v, want the snapshot's own time", got)
	}
	// Not recorded and written after it: looked at.
	unrec := hourAgo(42)
	testutil.MustExec(t, db, `INSERT INTO archive_state (partition_name, bintrail_id, local_path, row_count)
		VALUES (?, 'legacy-0001', '/a/z.parquet', 1)`, indexer.PartitionName(unrec))
	if got := sinceFor(t); !got.Equal(unrec) {
		t.Fatalf("unrecorded archive written after the snapshot: SinceFor = %v, want its hour %v", got, unrec)
	}
}

// The built-in rotation loop rotates indexes nothing else in its process
// migrated. Its archive_state INSERT names the #2152 columns, so on an
// archive_state from an older build it must migrate first: an INSERT that
// fails leaves every old partition in place until the disk fills.
func TestIntegrationRotateMigratesArchiveStateBeforeArchiving(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	ctx := context.Background()
	testutil.MustExec(t, db, `ALTER TABLE archive_state DROP COLUMN max_event_id, DROP COLUMN max_binlog_file, DROP COLUMN max_start_pos`)

	h := time.Now().UTC().Add(-48 * time.Hour).Truncate(time.Hour)
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{h})
	testutil.InsertEvent(t, db, "binlog.000004", 300, 350, h.Add(5*time.Minute).Format("2006-01-02 15:04:05"), nil,
		"shop", "orders", 1, "1", nil, nil, []byte(`{"id":1}`))
	if _, err := rotation.Perform(ctx, db, dbName, rotation.Options{
		RetainDur: 24 * time.Hour, RetainRaw: "24h",
		ArchiveDir: t.TempDir(), ArchiveCompression: "zstd",
		BintrailID: "2152cafe-dead-beef-dead-beefdeadbeef", Format: "json",
	}); err != nil {
		t.Fatalf("rotation.Perform on an archive_state without the #2152 columns: %v", err)
	}
	var file sql.NullString
	var pos sql.Null[uint64]
	if err := db.QueryRowContext(ctx, `SELECT max_binlog_file, max_start_pos FROM archive_state`).Scan(&file, &pos); err != nil {
		t.Fatalf("read archive_state: %v", err)
	}
	if file.String != "binlog.000004" || pos.V != 300 {
		t.Fatalf("recorded %+v:%+v, want binlog.000004:300", file, pos)
	}
}
