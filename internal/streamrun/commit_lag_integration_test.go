//go:build integration

package streamrun

import (
	"context"
	"database/sql"
	"strconv"
	"testing"
	"time"

	promtest "github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/observe"
	"github.com/dbtrail/dbtrail/internal/parser"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// lagEvents builds n synthetic INSERTs for testdb.orders, each stamped as read
// readAgo ago and committed at the source commitAgo ago.
func lagEvents(n int, readAgo, commitAgo time.Duration) []parser.Event {
	now := time.Now().UTC()
	evs := make([]parser.Event, 0, n)
	for i := range n {
		evs = append(evs, parser.Event{
			BinlogFile: "binlog.000001",
			StartPos:   uint64(i * 100),
			EndPos:     uint64((i + 1) * 100),
			Timestamp:  now.Add(-commitAgo),
			ReadAt:     now.Add(-readAgo),
			Schema:     "testdb",
			Table:      "orders",
			EventType:  parser.EventInsert,
			PKValues:   strconv.Itoa(i + 1),
			RowAfter:   map[string]any{"id": int64(i + 1), "amount": 9.99},
		})
	}
	return evs
}

// lagTestIndex stands up the same minimal index TestStreamLoop_flushAndCheckpoint uses.
func lagTestIndex(t *testing.T) (*indexer.Indexer, *sql.DB) {
	t.Helper()
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	testutil.InsertSnapshot(t, db, 1, "2026-01-01 00:00:00",
		"testdb", "orders", "id", 1, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, db, 1, "2026-01-01 00:00:00",
		"testdb", "orders", "amount", 2, "", "decimal", "YES")
	return indexer.New(db, 10), db
}

// committed appends the end of the transaction evs belong to, as the stream
// parser does: rows of a transaction whose end never arrives are discarded
// when the stream stops, not written (#2391).
func committed(evs []parser.Event) []parser.Event {
	last := evs[len(evs)-1]
	return append(evs, parser.Event{BinlogFile: last.BinlogFile, EndPos: last.EndPos + 31, GTID: last.GTID, EventType: parser.EventCommit})
}

func feedClosed(evs []parser.Event) chan parser.Event {
	ch := make(chan parser.Event, len(evs)+1)
	for _, ev := range evs {
		ch <- ev
	}
	close(ch)
	return ch
}

// TestStreamLoop_commitLagMetricsWired covers the WIRING, which the pure
// observeCommitLag unit tests cannot: that flush() actually calls it after a
// successful InsertBatch. Deleting that call leaves every unit test in
// commit_lag_test.go green while shipping a build that publishes no
// availability signal at all.
func TestStreamLoop_commitLagMetricsWired(t *testing.T) {
	idx, db := lagTestIndex(t)
	m := observe.ForSource("wired-ok")

	before := histCount(t, m.IndexCommitLatency)
	err := streamLoop(context.Background(), feedClosed(committed(lagEvents(3, 2*time.Second, 30*time.Second))),
		idx, db, time.Hour, &streamState{mode: "position", serverID: 1}, m, nil)
	if err != nil {
		t.Fatalf("streamLoop: %v", err)
	}

	if got := histCount(t, m.IndexCommitLatency) - before; got != 3 {
		t.Errorf("index_commit_latency observations = %d, want 3 — flush() never called observeCommitLag", got)
	}
	if got := promtest.ToFloat64(m.LastFlushTimestamp); got == 0 {
		t.Error("last_flush_timestamp is 0 after a successful flush")
	}
	// ~30s behind at the source; a clamp or a wrong subtraction shows up here.
	if got := promtest.ToFloat64(m.AvailabilityLag); got < 25 || got > 60 {
		t.Errorf("availability_lag = %v, want ≈30", got)
	}
}

// TestStreamLoop_commitLagSilentOnFlushFailure pins the other half: a FAILED
// InsertBatch must publish nothing. A flush timestamp advanced by a failed write
// is the worst possible reading — it claims data became queryable at exactly the
// moment data did not.
func TestStreamLoop_commitLagSilentOnFlushFailure(t *testing.T) {
	idx, db := lagTestIndex(t)
	m := observe.ForSource("wired-fail")

	// Force the INSERT to fail, the same way TestStreamLoop_flushFailurePropagates does.
	if _, err := db.Exec("DROP TABLE binlog_events"); err != nil {
		t.Fatalf("DROP TABLE: %v", err)
	}

	before := histCount(t, m.IndexCommitLatency)
	err := streamLoop(context.Background(), feedClosed(committed(lagEvents(2, time.Second, 10*time.Second))),
		idx, db, time.Hour, &streamState{mode: "position", serverID: 1}, m, nil)
	if err == nil {
		t.Fatal("expected streamLoop to propagate the flush failure")
	}

	if got := histCount(t, m.IndexCommitLatency) - before; got != 0 {
		t.Errorf("index_commit_latency observations = %d after a FAILED flush, want 0", got)
	}
	if got := promtest.ToFloat64(m.LastFlushTimestamp); got != 0 {
		t.Errorf("last_flush_timestamp = %v after a FAILED flush, want 0 — nothing became queryable", got)
	}
}
