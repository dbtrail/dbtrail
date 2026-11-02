//go:build integration

package rotation

import (
	"context"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// #2606: rotation records a drop in the partition history before it drops,
// and a drop it cannot record does not run.
func TestIntegrationDropPartitions_recordsTheDropFirst_2606(t *testing.T) {
	ctx := context.Background()
	db, dbName := testutil.CreateTestDB(t)
	if err := indexer.CreateIndexTables(ctx, db, 4, false, nil); err != nil {
		t.Fatal(err)
	}
	exists := func(name string) bool {
		t.Helper()
		var n int
		if err := db.QueryRow(`SELECT COUNT(*) FROM information_schema.PARTITIONS
			WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'binlog_events' AND PARTITION_NAME = ?`, name).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n == 1
	}
	hour := time.Now().UTC().Truncate(time.Hour)
	second := indexer.PartitionName(hour.Add(time.Hour))
	third := indexer.PartitionName(hour.Add(2 * time.Hour))

	if err := dropPartitions(ctx, db, dbName, []string{third}, nil); err != nil {
		t.Fatal(err)
	}
	if exists(third) {
		t.Fatalf("%s still exists after its drop", third)
	}
	h, err := indexer.ReadPartitionHistory(ctx, db, dbName)
	if err != nil {
		t.Fatal(err)
	}
	if want := hour.Add(3 * time.Hour); !h.DroppedBelow.Equal(want) {
		t.Errorf("recorded dropped below %v, want %v", h.DroppedBelow, want)
	}

	// The record cannot be written: the drop does not run.
	testutil.MustExec(t, db, "ALTER TABLE partition_history DROP COLUMN dropped_below_unix")
	if err := dropPartitions(ctx, db, dbName, []string{second}, nil); err == nil {
		t.Error("a drop whose record failed returned no error")
	}
	if !exists(second) {
		t.Errorf("%s was dropped although its drop could not be recorded", second)
	}
}
