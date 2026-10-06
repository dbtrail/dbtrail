//go:build integration

package rotation

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// A partition whose S3 upload failed is archived again by the next cycle,
// with whatever was indexed into it since (#2152). That archive is a new file:
// its archive_state row must say it was written NOW. A snapshot taken between
// the two attempts reads an archive with no cut only when it was written after
// the snapshot, so keeping the first attempt's archived_at would skip the
// archive that holds a change the snapshot never saw, with no error.
func TestPerformRotation_reArchiveAfterUploadFailureIsStampedNow_2152(t *testing.T) {
	ctx := context.Background()
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	h1 := time.Now().UTC().Add(-48 * time.Hour).Truncate(time.Hour)
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{h1})
	testutil.InsertEvent(t, db, "binlog.000001", 100, 200, h1.Add(10*time.Minute).Format("2006-01-02 15:04:05"), nil,
		"shop", "orders", 1, "1", nil, nil, []byte(`{"id":1}`))

	fail := true
	prev := uploadFileFunc
	uploadFileFunc = func(ctx context.Context, client *s3.Client, path, bucket, key string) error {
		if fail {
			return fmt.Errorf("simulated S3 upload failure")
		}
		return nil
	}
	t.Cleanup(func() { uploadFileFunc = prev })
	const bintrailID = "2152aaaa-dead-beef-dead-beefdeadbeef"
	opts := Options{
		RetainDur: 24 * time.Hour, ArchiveDir: t.TempDir(),
		ArchiveS3: "s3://fake-bucket/prefix/", ArchiveS3Region: "us-east-1",
		BintrailID: bintrailID, ArchiveCompression: "zstd", Format: "json", NoReplace: true,
	}

	// First cycle: archived, upload fails, partition kept.
	if _, err := Perform(ctx, db, dbName, opts); err != nil {
		t.Fatalf("first Perform: %v", err)
	}
	// That cycle ran hours ago.
	testutil.MustExec(t, db, `UPDATE archive_state SET archived_at = NOW() - INTERVAL 5 HOUR WHERE bintrail_id = ?`, bintrailID)

	// A snapshot is taken between the two attempts, at position 300.
	snapTime := time.Now().UTC().Add(-2 * time.Hour)
	anchor := &query.BinlogPos{File: "binlog.000001", Pos: 300}
	// Then a change after it is indexed late into the old hour.
	testutil.InsertEvent(t, db, "binlog.000001", 1500, 1600, h1.Add(40*time.Minute).Format("2006-01-02 15:04:05"), nil,
		"shop", "orders", 2, "1", nil, nil, []byte(`{"id":1,"v":2}`))

	// Second cycle: archived again with the late change, uploaded, dropped.
	fail = false
	res, err := Perform(ctx, db, dbName, opts)
	if err != nil {
		t.Fatalf("second Perform: %v", err)
	}
	if res.Dropped != 1 {
		t.Fatalf("fixture: second cycle dropped %d partitions, want 1", res.Dropped)
	}

	var age int64
	if err := db.QueryRowContext(ctx, `SELECT TIMESTAMPDIFF(SECOND, archived_at, NOW()) FROM archive_state WHERE bintrail_id = ?`, bintrailID).Scan(&age); err != nil {
		t.Fatalf("read archived_at: %v", err)
	}
	if age > 600 {
		t.Fatalf("archived_at is %d s old after the re-archive: it still names the first attempt", age)
	}

	// The update of that snapshot, with no cut known, reaches the archive.
	h, err := query.LoadPartitionHeads(ctx, db)
	if err != nil {
		t.Fatalf("LoadPartitionHeads: %v", err)
	}
	got, err := h.SinceFor(ctx, db, query.Options{Schema: "shop", Table: "orders", Since: &snapTime, SincePos: anchor})
	if err != nil || got == nil || got.After(h1) {
		t.Fatalf("SinceFor = %v, err=%v; want at or before %v: the archive holds a change after the anchor", got, err, h1)
	}
}
