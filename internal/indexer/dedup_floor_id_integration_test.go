//go:build integration

package indexer

import (
	"database/sql"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/parser"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestFirstIDOfLastBatchIsTheFIRSTIDAndNeverGoesDown pins the contract the
// resume-dedup floor is built on (#1690).
//
// FIRST, not last, and the difference is the whole point: LastInsertId returns
// the first auto-increment value of a multi-row INSERT, and deriving the last
// from it (first + rowcount - 1) would smuggle in an assumption that the ids
// inside one statement are consecutive. They are here, but the floor does not
// need them to be, and the safe direction for a lower bound is DOWN.
func TestFirstIDOfLastBatchIsTheFIRSTIDAndNeverGoesDown(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	idx := New(db, 100)

	if got := idx.FirstIDOfLastBatch(); got != 0 {
		t.Fatalf("a fresh indexer reports %d, want 0 — 'no batch has landed' must read as 'no floor'", got)
	}

	batch := func(n int, startPos uint64) []parser.Event {
		ts := time.Date(2026, 2, 19, 12, 0, 0, 0, time.UTC)
		var out []parser.Event
		for i := range n {
			out = append(out, parser.Event{
				BinlogFile: "binlog.000001", StartPos: startPos + uint64(i*100), EndPos: startPos + uint64(i*100+100),
				Timestamp: ts, Schema: "shop", Table: "orders", EventType: parser.EventInsert,
				PKValues: string(rune('a' + i)), RowAfter: map[string]any{"id": i},
			})
		}
		return out
	}

	minOfLastBatch := func(sincePos uint64) int64 {
		var id sql.NullInt64
		if err := db.QueryRow("SELECT MIN(event_id) FROM binlog_events WHERE start_pos >= ?", sincePos).Scan(&id); err != nil {
			t.Fatalf("min id: %v", err)
		}
		if !id.Valid {
			t.Fatal("the batch wrote no rows")
		}
		return id.Int64
	}

	for _, n := range []int{1, 5, 20} {
		startPos := uint64(n * 100000)
		if _, err := idx.InsertBatch(batch(n, startPos)); err != nil {
			t.Fatalf("InsertBatch(%d): %v", n, err)
		}
		if got, want := idx.FirstIDOfLastBatch(), minOfLastBatch(startPos); got != want {
			t.Errorf("batch of %d: FirstIDOfLastBatch = %d, want %d (the LOWEST id in that batch)", n, got, want)
		}
	}

	// A statement that inserts nothing reports LastInsertId 0. Letting that
	// through would drop the floor below every row it must bound — slow is
	// fine, wrong is not.
	before := idx.FirstIDOfLastBatch()
	if _, err := db.Exec("UPDATE binlog_events SET schema_name = schema_name WHERE event_id = 1"); err != nil {
		t.Fatalf("no-op update: %v", err)
	}
	if _, err := idx.InsertBatch(nil); err == nil {
		// An empty batch is not a valid INSERT; if the implementation ever
		// starts accepting it, the floor must still not move.
		if got := idx.FirstIDOfLastBatch(); got != before {
			t.Errorf("an empty batch moved the floor from %d to %d", before, got)
		}
	}
	if got := idx.FirstIDOfLastBatch(); got != before {
		t.Errorf("the floor moved to %d without a successful batch (was %d)", got, before)
	}
}
