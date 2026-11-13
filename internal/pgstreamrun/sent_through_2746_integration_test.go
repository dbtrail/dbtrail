//go:build integration

package pgstreamrun

import (
	"context"
	"database/sql"
	"log/slog"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// #2746: the loop saves the walsender's sent position (EventSentThrough) in
// pg_sent_through_lsn with the checkpoint, only after everything received
// before the marker is committed, and never indexes the marker as a row. No
// live PostgreSQL is needed: the loop is fed events.

type sentRow struct {
	pos, sent sql.NullInt64
	lastEvent sql.NullTime
	tied      bool // pg_sent_through_at is the save's last_checkpoint
}

func readSentRow(t *testing.T, db *sql.DB) (sentRow, bool) {
	t.Helper()
	var r sentRow
	err := db.QueryRow("SELECT binlog_position, pg_sent_through_lsn, last_event_time, pg_sent_through_at <=> last_checkpoint FROM stream_state WHERE id = 1").Scan(&r.pos, &r.sent, &r.lastEvent, &r.tied)
	if err == sql.ErrNoRows {
		return r, false
	}
	if err != nil {
		t.Fatalf("read stream_state: %v", err)
	}
	return r, true
}

func waitSentRow(t *testing.T, db *sql.DB, what string, ok func(sentRow) bool) sentRow {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		r, found := readSentRow(t, db)
		if found && ok(r) {
			return r
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s; last stream_state %+v (found %v)", what, r, found)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// startSentLoop runs streamLoopPG with a 10 ms checkpoint on an unbuffered
// channel, so once send(ev[k]) returns, ev[k-1] is handled.
func startSentLoop(t *testing.T, state *pgStreamState) (send func(event.Event), finish func(), db *sql.DB) {
	t.Helper()
	db, _ = testutil.CreateTestDB(t)
	if err := indexer.CreateIndexTables(context.Background(), db, 4, false, nil); err != nil {
		t.Fatalf("CreateIndexTables: %v", err)
	}
	idx := indexer.New(db, 1000)
	events := make(chan event.Event)
	done := make(chan error, 1)
	go func() {
		done <- streamLoopPG(context.Background(), events, idx, db, nil, 10*time.Millisecond, nil, time.Hour,
			state, slog.New(slog.DiscardHandler), nil)
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

func sentMarkerAt(pos uint64) event.Event {
	return event.Event{EventType: event.EventSentThrough, EndPos: pos}
}

func TestIntegrationPGSentThroughSavedAfterItsChanges_2746(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	send, finish, db := startSentLoop(t, &pgStreamState{serverID: 92746})

	// A committed transaction, then the marker past its commit.
	send(pgRow(1))
	send(pgRow(2))
	send(event.Event{EventType: event.EventCommit, EndPos: 1500})
	send(sentMarkerAt(2000))
	r := waitSentRow(t, db, "the sent position saved", func(r sentRow) bool { return r.sent.Valid && r.sent.Int64 == 2000 })
	if r.pos.Int64 != 1500 || !r.tied {
		t.Fatalf("binlog_position %d, tied to the save %v; want the commit, 1500, in the save that wrote the position", r.pos.Int64, r.tied)
	}
	if n := pgCount(t, db); n != 2 {
		t.Fatalf("%d rows indexed when the sent position was saved; want the transaction's 2, and the marker never a row", n)
	}

	// A marker in the middle of a transaction: saved, while the rows read so
	// far stay invisible (their commit is past the marker's position).
	send(pgRow(3))
	send(sentMarkerAt(2500))
	send(pgRow(4))
	waitSentRow(t, db, "the mid-transaction sent position saved", func(r sentRow) bool { return r.sent.Int64 == 2500 })
	if n := pgCount(t, db); n != 2 {
		t.Fatalf("%d rows visible with a transaction half read; want 2", n)
	}
	// An older position never moves it back.
	send(sentMarkerAt(2100))
	send(event.Event{EventType: event.EventCommit, EndPos: 2600})
	r = waitSentRow(t, db, "the second commit saved", func(r sentRow) bool { return r.pos.Int64 == 2600 })
	if r.sent.Int64 != 2500 {
		t.Fatalf("pg_sent_through_lsn %d after an older marker; want 2500", r.sent.Int64)
	}
	finish()
	if n := pgCount(t, db); n != 4 {
		t.Fatalf("%d rows indexed; want 4 and no marker row", n)
	}
}

// A first run that has committed nothing saves nothing, marker or not: there
// is no durable position to save it with.
func TestIntegrationPGSentThroughNotSavedWithoutACheckpoint_2746(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	send, finish, db := startSentLoop(t, &pgStreamState{serverID: 92746})
	send(sentMarkerAt(2000))
	time.Sleep(100 * time.Millisecond) // ten checkpoint intervals
	if r, found := readSentRow(t, db); found {
		t.Fatalf("stream_state saved with no commit: %+v", r)
	}
	finish()
}

// A resumed run on a quiet database: nothing is committed in this run, and
// its checkpoint is still saved every interval, at the position it resumed
// from, with the sent position, and without clearing the newest change's time.
func TestIntegrationPGResumedQuietRunSavesItsCheckpoint_2746(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	at := time.Date(2026, 11, 13, 9, 30, 0, 0, time.UTC)
	state := &pgStreamState{lsn: 1500, serverID: 92746, resumed: true, eventsIndexed: 7,
		lastEventTime: sql.NullTime{Time: at, Valid: true}}
	send, finish, db := startSentLoop(t, state)
	r := waitSentRow(t, db, "the resumed checkpoint saved", func(r sentRow) bool { return r.pos.Int64 == 1500 })
	if !r.sent.Valid || r.sent.Int64 != 0 {
		t.Fatalf("pg_sent_through_lsn %+v before any marker; want 0, not NULL", r.sent)
	}
	if !r.lastEvent.Valid || !r.lastEvent.Time.Equal(at) {
		t.Fatalf("last_event_time %+v; want the resumed %v kept", r.lastEvent, at)
	}
	send(sentMarkerAt(3000))
	waitSentRow(t, db, "the sent position saved", func(r sentRow) bool { return r.sent.Int64 == 3000 && r.pos.Int64 == 1500 })
	finish()
}

// A flag LSN on a first run is not durable: it is never saved as a checkpoint.
func TestIntegrationPGFlagStartIsNotSavedAsACheckpoint_2746(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	send, finish, db := startSentLoop(t, &pgStreamState{lsn: 1500, serverID: 92746})
	send(sentMarkerAt(3000))
	time.Sleep(100 * time.Millisecond)
	if r, found := readSentRow(t, db); found {
		t.Fatalf("a first run's start position was saved: %+v", r)
	}
	finish()
}
