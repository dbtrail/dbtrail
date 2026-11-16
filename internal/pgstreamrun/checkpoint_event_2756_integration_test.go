//go:build integration

package pgstreamrun

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// checkpointEvent reads pg_checkpoint_event_id and the saved position.
func checkpointEvent(t *testing.T, db *sql.DB) (id sql.NullInt64, lsn sql.NullInt64) {
	t.Helper()
	err := db.QueryRow(`SELECT pg_checkpoint_event_id, binlog_position FROM stream_state WHERE id = 1`).Scan(&id, &lsn)
	if err != nil && err != sql.ErrNoRows {
		t.Fatalf("read the checkpoint: %v", err)
	}
	return id, lsn
}

// rowAt is one row of the transaction committed at lsn, as the decoder hands
// it over: its position is its transaction's commit LSN.
func rowAt(lsn uint64, key int) event.Event {
	return event.Event{
		BinlogFile: fmt.Sprintf("%X/%X", lsn>>32, uint32(lsn)), StartPos: lsn, EndPos: lsn,
		Timestamp: time.Date(2026, 2, 19, 14, 0, 0, 0, time.UTC),
		Schema:    "public", Table: "orders", EventType: event.EventInsert,
		PKValues: fmt.Sprint(key), RowAfter: map[string]any{"id": fmt.Sprint(key)},
	}
}

// leftAt indexes a row at lsn without the capture: what a capture that
// stopped left visible, or what one that started again has indexed so far of
// a transaction it is receiving a second time. It returns the row's id.
func leftAt(t *testing.T, db *sql.DB, lsn uint64, key int) int64 {
	t.Helper()
	testutil.InsertEvent(t, db, fmt.Sprintf("%X/%X", lsn>>32, uint32(lsn)), lsn, lsn, "2026-02-19 14:00:00", nil,
		"public", "orders", 1, fmt.Sprint(key), nil, nil, []byte(fmt.Sprintf(`{"id":"%d"}`, key)))
	var id int64
	if err := db.QueryRow(`SELECT MAX(event_id) FROM binlog_events`).Scan(&id); err != nil {
		t.Fatal(err)
	}
	return id
}

func newestID(t *testing.T, db *sql.DB) int64 {
	t.Helper()
	var id sql.NullInt64
	if err := db.QueryRow(`SELECT MAX(event_id) FROM binlog_events`).Scan(&id); err != nil {
		t.Fatal(err)
	}
	return id.Int64
}

// The capture records which event its saved position reaches (#2756): the
// newest one at or before the position, written when a transaction ended,
// and left alone by a save with none ended, so the rows of a transaction
// arriving a second time stay above it until that delivery is whole.
func TestIntegrationPGCheckpointEvent_2756(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	if err := indexer.CreateIndexTables(context.Background(), db, 4, false, nil); err != nil {
		t.Fatalf("CreateIndexTables: %v", err)
	}
	idx := indexer.New(db, 1000)
	events := make(chan event.Event)
	done := make(chan error, 1)
	go func() {
		done <- streamLoopPG(context.Background(), events, idx, db, nil, 10*time.Millisecond, nil, time.Hour,
			&pgStreamState{serverID: 92756}, slog.New(slog.DiscardHandler), nil)
	}()
	send := func(ev event.Event) {
		t.Helper()
		select {
		case events <- ev:
		case err := <-done:
			t.Fatalf("streamLoopPG ended early: %v", err)
		}
	}
	defer func() {
		close(events)
		if err := <-done; err != nil {
			t.Fatalf("streamLoopPG: %v", err)
		}
	}()
	stamped := func(want int64, wantLSN int64) bool {
		id, lsn := checkpointEvent(t, db)
		return id.Valid && id.Int64 == want && lsn.Valid && lsn.Int64 == wantLSN
	}

	// A transaction of two rows at 0x100.
	send(rowAt(0x100, 1))
	send(rowAt(0x100, 2))
	send(event.Event{EventType: event.EventCommit, EndPos: 0x100})
	waitFor(t, 5*time.Second, func() bool { return newestID(t, db) > 0 && stamped(newestID(t, db), 0x100) }, "the event of the first transaction's position")
	first := newestID(t, db)

	// The capture stopped and started again: the transaction at the saved
	// position arrives a second time, and one of its two rows is in so far.
	// Saves go on (every 10 ms) and none moves the event.
	half := leftAt(t, db, 0x100, 1)
	time.Sleep(200 * time.Millisecond)
	if !stamped(first, 0x100) {
		id, lsn := checkpointEvent(t, db)
		t.Fatalf("with a second delivery half in (row %d): event %v at position %v, want %d kept", half, id, lsn, first)
	}

	// A row left visible past the saved position, and then the second
	// delivery ends: the event is the newest one AT OR BEFORE the position.
	left := leftAt(t, db, 0x300, 9)
	send(rowAt(0x100, 2))
	send(event.Event{EventType: event.EventCommit, EndPos: 0x100})
	waitFor(t, 5*time.Second, func() bool { return newestID(t, db) > left && stamped(newestID(t, db), 0x100) }, "the event once the second delivery is whole")
	second := newestID(t, db)
	if second <= left {
		t.Fatalf("the redelivered row has id %d, not above the row left at 0x300 (%d)", second, left)
	}

	// A transaction with no row of a captured table, at 0x200: the position
	// moves, and the event is still the newest at or before it.
	send(event.Event{EventType: event.EventCommit, EndPos: 0x200})
	waitFor(t, 5*time.Second, func() bool { return stamped(second, 0x200) }, "the same event under the next position")

	// Past the row left behind: now it is at or before the position, and it
	// is not the newest such row.
	send(rowAt(0x400, 5))
	send(event.Event{EventType: event.EventCommit, EndPos: 0x400})
	waitFor(t, 5*time.Second, func() bool { return newestID(t, db) > second && stamped(newestID(t, db), 0x400) }, "the event of the last transaction")
}

// An index with no event at or before the position records none.
func TestIntegrationPGCheckpointEvent_none_2756(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	if err := indexer.CreateIndexTables(context.Background(), db, 4, false, nil); err != nil {
		t.Fatalf("CreateIndexTables: %v", err)
	}
	leftAt(t, db, 0x300, 9)
	if id, err := newestEventThrough(db, 0x200); err != nil || id != 0 {
		t.Fatalf("newestEventThrough(0x200) = %d, %v; want none", id, err)
	}
	st := &pgStreamState{serverID: 92756, stampEvent: true}
	if err := saveCheckpointPG(db, st, 0x200); err != nil {
		t.Fatal(err)
	}
	if id, _ := checkpointEvent(t, db); id.Valid {
		t.Fatalf("event %d recorded with none at or before the position", id.Int64)
	}
	// One recorded, then a save that stamps none: removed, not kept.
	st.checkpointEvent, st.stampEvent = 7, true
	if err := saveCheckpointPG(db, st, 0x200); err != nil {
		t.Fatal(err)
	}
	if id, _ := checkpointEvent(t, db); !id.Valid || id.Int64 != 7 {
		t.Fatalf("event %v, want 7", id)
	}
	if st.stampEvent {
		t.Fatal("the save left the stamp pending")
	}
	if err := saveCheckpointPG(db, st, 0x250); err != nil { // no transaction ended since
		t.Fatal(err)
	}
	if id, _ := checkpointEvent(t, db); !id.Valid || id.Int64 != 7 {
		t.Fatalf("a save with no transaction ended changed the event to %v", id)
	}
	st.checkpointEvent, st.stampEvent = 0, true
	if err := saveCheckpointPG(db, st, 0x260); err != nil {
		t.Fatal(err)
	}
	if id, _ := checkpointEvent(t, db); id.Valid {
		t.Fatalf("event %d kept by a save that found none", id.Int64)
	}
}
