//go:build integration

package pgstreamrun

import (
	"context"
	"database/sql"
	"log/slog"
	"strconv"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// #2391 for a PostgreSQL source: a reader of the index sees a source
// transaction whole or not at all. No live PostgreSQL is needed: the loop is
// fed events, and a nil capturer is skipped at the acknowledgement.

func pgRow(n int) event.Event {
	return event.Event{
		BinlogFile: "0/" + strconv.Itoa(n), EndPos: uint64(1000 + n),
		Timestamp: time.Date(2026, 2, 19, 14, 0, 0, 0, time.UTC),
		Schema:    "public", Table: "orders", EventType: event.EventInsert,
		PKValues: strconv.Itoa(n), RowAfter: map[string]any{"id": int64(n)},
	}
}

func pgCount(t *testing.T, db *sql.DB) int {
	t.Helper()
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM binlog_events`).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	return n
}

// runPGLoop starts streamLoopPG on an unbuffered channel: once send(ev[k])
// returns, ev[k-1] is fully handled, so a probe there sees what a reader
// between those two events would.
func runPGLoop(t *testing.T) (send func(event.Event), finish func(), db *sql.DB) {
	t.Helper()
	db, _ = testutil.CreateTestDB(t)
	if err := indexer.CreateIndexTables(context.Background(), db, 4, false, nil); err != nil {
		t.Fatalf("CreateIndexTables: %v", err)
	}
	idx := indexer.New(db, 1000)
	events := make(chan event.Event)
	done := make(chan error, 1)
	go func() {
		done <- streamLoopPG(context.Background(), events, idx, db, nil, time.Hour, nil, time.Hour,
			&pgStreamState{serverID: 91}, slog.New(slog.DiscardHandler), nil)
	}()
	send = func(ev event.Event) {
		select {
		case events <- ev:
		case err := <-done:
			t.Fatalf("streamLoopPG ended early: %v", err)
		}
	}
	finish = func() {
		close(events)
		if err := <-done; err != nil {
			t.Fatalf("streamLoopPG: %v", err)
		}
	}
	return send, finish, db
}

func TestIntegrationPGReaderSeesAWholeTransactionOrNone_2391(t *testing.T) {
	send, finish, db := runPGLoop(t)
	for n := 1; n <= 2500; n++ {
		send(pgRow(n))
		if n%100 == 1 && n > 1 {
			if seen := pgCount(t, db); seen != 0 {
				t.Fatalf("after %d of the transaction's 2500 rows were read, a reader sees %d; want 0", n-1, seen)
			}
		}
	}
	send(event.Event{EventType: event.EventCommit, EndPos: 9000})
	send(event.Event{EventType: event.EventGTID}) // taken only once the commit is handled
	if seen := pgCount(t, db); seen != 2500 {
		t.Fatalf("after the commit a reader sees %d rows, want 2500", seen)
	}
	finish()
	var lsn uint64
	if err := db.QueryRow(`SELECT binlog_position FROM stream_state WHERE id = 1`).Scan(&lsn); err != nil || lsn != 9000 {
		t.Fatalf("checkpoint LSN = %d (%v), want 9000", lsn, err)
	}
}

func TestIntegrationPGStopInsideATransactionKeepsOnlyCompleteOnes_2391(t *testing.T) {
	send, finish, db := runPGLoop(t)
	send(pgRow(1))
	send(event.Event{EventType: event.EventCommit, EndPos: 5000})
	for n := 2; n <= 1501; n++ { // one batch of it reaches an index transaction
		send(pgRow(n))
	}
	finish()
	if got := pgCount(t, db); got != 1 {
		t.Fatalf("want only the complete transaction's row, got %d", got)
	}
	var lsn uint64
	if err := db.QueryRow(`SELECT binlog_position FROM stream_state WHERE id = 1`).Scan(&lsn); err != nil || lsn != 5000 {
		t.Fatalf("checkpoint LSN = %d (%v), want 5000", lsn, err)
	}
}
