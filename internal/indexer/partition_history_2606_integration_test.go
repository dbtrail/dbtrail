//go:build integration

package indexer

import (
	"context"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/testutil"
)

// #2606 against a real index: the partition history is recorded once, by
// the call that creates binlog_events, from the instant its partitions are
// built from; an index that existed before gets none; a restore marks it
// unknown; a drop moves the bound up, never down.
func TestIntegrationPartitionHistory_2606(t *testing.T) {
	ctx := context.Background()

	t.Run("created with the index, once", func(t *testing.T) {
		db, dbName := testutil.CreateTestDB(t)
		before := time.Now().UTC().Truncate(time.Second)
		if err := CreateIndexTables(ctx, db, 3, false, nil); err != nil {
			t.Fatal(err)
		}
		h, err := ReadPartitionHistory(ctx, db, dbName)
		if err != nil {
			t.Fatal(err)
		}
		if !h.Known || h.CreatedAt.Before(before) || h.CreatedAt.After(time.Now().UTC()) || !h.DroppedBelow.IsZero() {
			t.Fatalf("history = %+v, want known, created now, nothing dropped", h)
		}
		var first string
		if err := db.QueryRow(`SELECT PARTITION_NAME FROM information_schema.PARTITIONS
			WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'binlog_events' ORDER BY PARTITION_ORDINAL_POSITION LIMIT 1`).Scan(&first); err != nil {
			t.Fatal(err)
		}
		if first != PartitionName(h.CreatedAt) {
			t.Errorf("first partition %s, recorded creation %v: not the same hour", first, h.CreatedAt)
		}
		// A later boot (watch calls this every start) keeps the record.
		testutil.MustExec(t, db, "UPDATE partition_history SET created_unix = created_unix - 7200 WHERE id = 1")
		if err := CreateIndexTables(ctx, db, 3, false, nil); err != nil {
			t.Fatal(err)
		}
		again, err := ReadPartitionHistory(ctx, db, dbName)
		if err != nil {
			t.Fatal(err)
		}
		if !again.CreatedAt.Equal(h.CreatedAt.Add(-2 * time.Hour)) {
			t.Errorf("a second CreateIndexTables rewrote the record: %v, want %v", again.CreatedAt, h.CreatedAt.Add(-2*time.Hour))
		}
	})

	t.Run("an index an older build created gets none", func(t *testing.T) {
		db, dbName := testutil.CreateTestDB(t)
		testutil.InitIndexTables(t, db) // binlog_events without the record
		if err := CreateIndexTables(ctx, db, 3, false, nil); err != nil {
			t.Fatal(err)
		}
		h, err := ReadPartitionHistory(ctx, db, dbName)
		if err != nil {
			t.Fatal(err)
		}
		if h.Known {
			t.Errorf("history = %+v on an index that existed before the record, want unknown", h)
		}
		// A drop on it records nothing, and does not fail.
		if err := MarkPartitionsDropped(ctx, db, dbName, []string{"p_2026110105"}); err != nil {
			t.Errorf("mark on an index without a record: %v", err)
		}
	})

	t.Run("no table: an older build's index", func(t *testing.T) {
		db, dbName := testutil.CreateTestDB(t)
		testutil.InitIndexTables(t, db)
		if h, err := ReadPartitionHistory(ctx, db, dbName); err != nil || h.Known {
			t.Errorf("history = %+v, %v; want unknown, no error", h, err)
		}
		if err := MarkPartitionsDropped(ctx, db, dbName, []string{"p_2026110105"}); err != nil {
			t.Errorf("mark with no table: %v", err)
		}
	})

	t.Run("restore marks it unknown, before and after creation", func(t *testing.T) {
		db, dbName := testutil.CreateTestDB(t)
		if err := MarkPartitionHistoryUnknown(ctx, db); err != nil {
			t.Fatal(err)
		}
		if err := CreateIndexTables(ctx, db, 3, false, nil); err != nil {
			t.Fatal(err)
		}
		if h, _ := ReadPartitionHistory(ctx, db, dbName); h.Known {
			t.Errorf("history = %+v after a restore's mark, want unknown", h)
		}
		db2, dbName2 := testutil.CreateTestDB(t)
		if err := CreateIndexTables(ctx, db2, 3, false, nil); err != nil {
			t.Fatal(err)
		}
		if err := MarkPartitionHistoryUnknown(ctx, db2); err != nil {
			t.Fatal(err)
		}
		if h, _ := ReadPartitionHistory(ctx, db2, dbName2); h.Known {
			t.Errorf("history = %+v after marking a created index, want unknown", h)
		}
	})

	t.Run("drops move the bound up only", func(t *testing.T) {
		db, dbName := testutil.CreateTestDB(t)
		if err := CreateIndexTables(ctx, db, 3, false, nil); err != nil {
			t.Fatal(err)
		}
		if err := MarkPartitionsDropped(ctx, db, dbName, []string{"p_2026110107"}); err != nil {
			t.Fatal(err)
		}
		if err := MarkPartitionsDropped(ctx, db, dbName, []string{"p_2026110103"}); err != nil {
			t.Fatal(err)
		}
		h, err := ReadPartitionHistory(ctx, db, dbName)
		if err != nil {
			t.Fatal(err)
		}
		if want := time.Date(2026, 11, 1, 8, 0, 0, 0, time.UTC); !h.DroppedBelow.Equal(want) {
			t.Errorf("dropped below %v, want %v (the highest drop)", h.DroppedBelow, want)
		}
	})
}
