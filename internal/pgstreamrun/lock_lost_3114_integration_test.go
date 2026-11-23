//go:build integration

package pgstreamrun

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/parser"
	"github.com/dbtrail/dbtrail/internal/pgcapture"
	"github.com/dbtrail/dbtrail/internal/streamrun"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// #3114: a process that lost its capture lock writes nothing more to the
// index: no row, no position, no schema record, and it acknowledges nothing to
// the slot. The supervisor says so by cancelling the run with
// streamrun.ErrStopWithoutFlush. A plain cancellation is a clean stop and
// still saves.

func lockIndex(t *testing.T) *sql.DB {
	t.Helper()
	db, _ := testutil.CreateTestDB(t)
	if err := indexer.CreateIndexTables(context.Background(), db, 4, false, nil); err != nil {
		t.Fatalf("CreateIndexTables: %v", err)
	}
	return db
}

// lockLoop runs streamLoopPG on an unbuffered channel for a test that decides
// when and how it is cancelled.
func lockLoop(t *testing.T, ctx context.Context, db *sql.DB) (send func(event.Event), done <-chan error) {
	t.Helper()
	prev := commitWithin
	commitWithin = func() time.Duration { return time.Hour } // only the stop writes
	t.Cleanup(func() { commitWithin = prev })
	events := make(chan event.Event)
	ended := make(chan error, 1)
	idx := indexer.New(db, 1000)
	go func() {
		ended <- streamLoopPG(ctx, events, idx, db, nil, time.Hour, nil, time.Hour,
			&pgStreamState{serverID: 91}, slog.New(slog.DiscardHandler), nil)
	}()
	return func(ev event.Event) {
		t.Helper()
		select {
		case events <- ev:
		case err := <-ended:
			t.Fatalf("streamLoopPG ended early: %v", err)
		}
	}, ended
}

func waitLoop(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(15 * time.Second):
		t.Fatal("streamLoopPG did not stop")
		return nil
	}
}

func savedPosition(t *testing.T, db *sql.DB) uint64 {
	t.Helper()
	var lsn uint64
	err := db.QueryRow(`SELECT binlog_position FROM stream_state WHERE id = 1`).Scan(&lsn)
	if errors.Is(err, sql.ErrNoRows) {
		return 0
	}
	if err != nil {
		t.Fatalf("read stream_state: %v", err)
	}
	return lsn
}

func wantNothingWritten(t *testing.T, err error, db *sql.DB) {
	t.Helper()
	if !errors.Is(err, streamrun.ErrStopWithoutFlush) {
		t.Errorf("streamLoopPG returned %v, want ErrStopWithoutFlush", err)
	}
	if n := pgCount(t, db); n != 0 {
		t.Errorf("the index holds %d row(s) written after the lock was lost, want 0", n)
	}
	if lsn := savedPosition(t, db); lsn != 0 {
		t.Errorf("position %d was saved after the lock was lost, want none", lsn)
	}
}

// taken is an event the loop receives only once the one before is handled.
var taken = event.Event{EventType: event.EventGTID}

func TestIntegrationPGLockLostMidTransactionWritesNothing_3114(t *testing.T) {
	db := lockIndex(t)
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	send, done := lockLoop(t, ctx, db)
	send(pgRow(1))
	send(taken)
	cancel(streamrun.ErrStopWithoutFlush)
	wantNothingWritten(t, waitLoop(t, done), db)
}

func TestIntegrationPGLockLostBetweenBatchesWritesNothing_3114(t *testing.T) {
	db := lockIndex(t)
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	send, done := lockLoop(t, ctx, db)
	send(pgRow(1))
	send(event.Event{EventType: event.EventCommit, EndPos: 5000})
	send(taken)
	cancel(streamrun.ErrStopWithoutFlush)
	wantNothingWritten(t, waitLoop(t, done), db)
}

// Past the batch size the transaction being read already has rows in an open
// index transaction. A lost lock rolls them back; it does not commit them.
func TestIntegrationPGLockLostWithAnOpenIndexTransaction_3114(t *testing.T) {
	db := lockIndex(t)
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	send, done := lockLoop(t, ctx, db)
	for n := 1; n <= 1501; n++ {
		send(pgRow(n))
	}
	send(taken)
	cancel(streamrun.ErrStopWithoutFlush)
	wantNothingWritten(t, waitLoop(t, done), db)
}

// The lock goes first and the daemon's shutdown comes after: the first cause
// is the one the context keeps, so the shutdown does not turn the stop back
// into one that saves.
func TestIntegrationPGLockLostThenShutdownWritesNothing_3114(t *testing.T) {
	db := lockIndex(t)
	parent, shutdown := context.WithCancel(context.Background())
	defer shutdown()
	ctx, cancel := context.WithCancelCause(parent)
	defer cancel(nil)
	send, done := lockLoop(t, ctx, db)
	send(pgRow(1))
	send(event.Event{EventType: event.EventCommit, EndPos: 5000})
	send(taken)
	cancel(streamrun.ErrStopWithoutFlush)
	shutdown()
	wantNothingWritten(t, waitLoop(t, done), db)
}

// The control: a clean stop keeps what is complete and saves its position.
func TestIntegrationPGCleanStopStillSaves_3114(t *testing.T) {
	db := lockIndex(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	send, done := lockLoop(t, ctx, db)
	send(pgRow(1))
	send(event.Event{EventType: event.EventCommit, EndPos: 5000})
	send(taken)
	cancel()
	if err := waitLoop(t, done); err != nil {
		t.Fatalf("a clean stop returned %v", err)
	}
	if n, lsn := pgCount(t, db), savedPosition(t, db); n != 1 || lsn != 5000 {
		t.Fatalf("after a clean stop: %d row(s), position %d; want 1 and 5000", n, lsn)
	}
}

// The new owner has already saved a newer position: the old one neither moves
// it back nor adds its rows.
func TestIntegrationPGLockLostLeavesTheNewOwnersPosition_3114(t *testing.T) {
	db := lockIndex(t)
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	send, done := lockLoop(t, ctx, db)
	send(pgRow(1))
	send(event.Event{EventType: event.EventCommit, EndPos: 5000})
	send(taken)
	testutil.MustExec(t, db, `INSERT INTO stream_state (id, mode, binlog_file, binlog_position, flavor, events_indexed, last_checkpoint, server_id)
		VALUES (1, 'gtid', '0/2328', 9000, 'postgres', 7, UTC_TIMESTAMP(), 92)`)
	cancel(streamrun.ErrStopWithoutFlush)
	if err := waitLoop(t, done); !errors.Is(err, streamrun.ErrStopWithoutFlush) {
		t.Errorf("streamLoopPG returned %v, want ErrStopWithoutFlush", err)
	}
	if lsn := savedPosition(t, db); lsn != 9000 {
		t.Errorf("the new owner's position became %d, want 9000", lsn)
	}
	if n := pgCount(t, db); n != 0 {
		t.Errorf("the old owner added %d row(s), want 0", n)
	}
}

// The capturer closes the channel when it is cancelled, so that branch can win
// the select against the cancellation; and events already received can too.
// Neither may write. A schema record does not go through the row writer, so it
// has a guard of its own.
func TestIntegrationPGLockLostEventsStillInTheChannelWriteNothing_3114(t *testing.T) {
	db := lockIndex(t)
	prev := commitWithin
	commitWithin = func() time.Duration { return time.Hour }
	t.Cleanup(func() { commitWithin = prev })
	rel := &metadata.PGRelationSchema{OID: 16400, Schema: "public", Table: "orders", Columns: []metadata.PGRelationColumn{{Name: "id", Ordinal: 1, TypeOID: 23, TypeMod: -1, IsPK: true}}}
	for round := range 40 {
		events := make(chan event.Event, 6)
		events <- event.Event{EventType: event.EventRelation, Schema: "public", Table: "orders", Relation: rel}
		events <- event.Event{BinlogFile: "0/2", EndPos: 2, Schema: "public", Table: "orders", EventType: event.EventDDL, DDLType: event.DDLTruncateTable,
			DDLQuery: "TRUNCATE public.orders", Timestamp: time.Date(2026, 2, 19, 14, 0, 0, 0, time.UTC)}
		events <- pgRow(1)
		events <- event.Event{EventType: event.EventCommit, EndPos: 5000}
		close(events)
		ctx, cancel := context.WithCancelCause(context.Background())
		cancel(streamrun.ErrStopWithoutFlush)
		err := streamLoopPG(ctx, events, indexer.New(db, 1000), db, nil, time.Hour, nil, time.Hour,
			&pgStreamState{serverID: 91}, slog.New(slog.DiscardHandler), nil)
		if !errors.Is(err, streamrun.ErrStopWithoutFlush) {
			t.Fatalf("round %d: streamLoopPG returned %v, want ErrStopWithoutFlush", round, err)
		}
	}
	wantNothingWritten(t, streamrun.ErrStopWithoutFlush, db)
	for _, table := range []string{"schema_snapshots", "schema_changes"} {
		var n int
		if err := db.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n != 0 {
			t.Errorf("%s holds %d row(s) written after the lock was lost, want 0", table, n)
		}
	}
}

// A health reading in flight when the lock goes: its result is neither saved
// nor turned into a record that a table left the publication.
func TestIntegrationPGLockLostDuringHealthReadWritesNothing_3114(t *testing.T) {
	db := lockIndex(t)
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	table := pgcapture.PublishedTable{OID: 16400, Schema: "public", Table: "orders"}
	reads := 0
	var before string
	probe := func(context.Context) (pgcapture.HealthSnapshot, error) {
		reads++
		if reads == 1 {
			return pgcapture.HealthSnapshot{Published: []pgcapture.PublishedTable{table}}, nil
		}
		if reads == 2 {
			// The first reading is saved by now.
			if err := db.QueryRow(`SELECT COALESCE(source_health, '') FROM stream_state WHERE id = 1`).Scan(&before); err != nil {
				t.Errorf("read the saved health: %v", err)
			}
			cancel(streamrun.ErrStopWithoutFlush)
		}
		return pgcapture.HealthSnapshot{}, nil // the table is gone from the publication
	}
	events := make(chan event.Event)
	done := make(chan error, 1)
	go func() {
		done <- streamLoopPG(ctx, events, indexer.New(db, 1000), db, nil, time.Hour, probe, 20*time.Millisecond,
			&pgStreamState{serverID: 91}, slog.New(slog.DiscardHandler), nil)
	}()
	if err := waitLoop(t, done); !errors.Is(err, streamrun.ErrStopWithoutFlush) {
		t.Errorf("streamLoopPG returned %v, want ErrStopWithoutFlush", err)
	}
	var after string
	if err := db.QueryRow(`SELECT COALESCE(source_health, '') FROM stream_state WHERE id = 1`).Scan(&after); err != nil {
		t.Fatal(err)
	}
	if before == "" || after != before {
		t.Errorf("the saved health changed after the lock was lost:\nbefore %s\nafter  %s", before, after)
	}
	var left int
	if err := db.QueryRow(`SELECT COUNT(*) FROM schema_changes`).Scan(&left); err != nil {
		t.Fatal(err)
	}
	if left != 0 {
		t.Errorf("%d schema record(s) written after the lock was lost, want 0", left)
	}
}

// Replication starting just after the lock went does not write the capture's
// row; the caller's hook still runs, since the source was reached.
func TestIntegrationPGLockLostBeforeStartedRowWritesNothing_3114(t *testing.T) {
	db := lockIndex(t)
	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(streamrun.ErrStopWithoutFlush)
	connected := false
	startedHook(ctx, db, 91, parser.NewSkipCounters(slog.New(slog.DiscardHandler)),
		&Hooks{OnSourceConnected: func() { connected = true }}, slog.New(slog.DiscardHandler))()
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM stream_state`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Errorf("the capture's row was written after the lock was lost")
	}
	if !connected {
		t.Error("the caller's hook did not run")
	}
	// The control: with the lock held, the row is written.
	startedHook(context.Background(), db, 91, parser.NewSkipCounters(slog.New(slog.DiscardHandler)), nil, slog.New(slog.DiscardHandler))()
	if err := db.QueryRow(`SELECT COUNT(*) FROM stream_state`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("with the lock held the capture's row was not written")
	}
}

// The lock goes at each step of a checkpoint that is under way: right before
// the position is written, and between that write and the acknowledgement.
func TestIntegrationPGLockLostInsideACheckpoint_3114(t *testing.T) {
	for _, tc := range []struct {
		step     string
		wantRows int
		wantSave uint64
	}{
		{"commit", 0, 0}, // nothing is written
		{"save", 1, 0},   // the rows were in before the loss, the position is not written
		{"ack", 1, 5000}, // both were in before the loss, and it is not acknowledged
	} {
		t.Run(tc.step, func(t *testing.T) {
			db := lockIndex(t)
			ctx, cancel := context.WithCancelCause(context.Background())
			defer cancel(nil)
			prevStep, prevAck, prevWithin := beforeWrite, ackCheckpoint, commitWithin
			t.Cleanup(func() { beforeWrite, ackCheckpoint, commitWithin = prevStep, prevAck, prevWithin })
			commitWithin = func() time.Duration { return time.Hour }
			beforeWrite = func(step string) {
				if step == tc.step {
					cancel(streamrun.ErrStopWithoutFlush)
				}
			}
			var acked []uint64
			ackCheckpoint = func(_ *pgcapture.Capturer, lsn uint64) { acked = append(acked, lsn) }

			events := make(chan event.Event, 3)
			events <- pgRow(1)
			events <- event.Event{EventType: event.EventCommit, EndPos: 5000}
			done := make(chan error, 1)
			go func() {
				done <- streamLoopPG(ctx, events, indexer.New(db, 1000), db, pgcapture.New(pgcapture.Config{}), 30*time.Millisecond, nil, time.Hour,
					&pgStreamState{serverID: 91}, slog.New(slog.DiscardHandler), nil)
			}()
			if err := waitLoop(t, done); !errors.Is(err, streamrun.ErrStopWithoutFlush) {
				t.Errorf("streamLoopPG returned %v, want ErrStopWithoutFlush", err)
			}
			if got := pgCount(t, db); got != tc.wantRows {
				t.Errorf("rows in the index = %d, want %d", got, tc.wantRows)
			}
			if got := savedPosition(t, db); got != tc.wantSave {
				t.Errorf("saved position = %d, want %d", got, tc.wantSave)
			}
			if len(acked) != 0 {
				t.Errorf("acknowledged %v to the slot after the lock was lost, want nothing", acked)
			}
		})
	}
}

// The control of the test above: with the lock held the same checkpoint saves
// and acknowledges.
func TestIntegrationPGCheckpointAcknowledgesWithTheLockHeld_3114(t *testing.T) {
	db := lockIndex(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	prevAck := ackCheckpoint
	t.Cleanup(func() { ackCheckpoint = prevAck })
	acked := make(chan uint64, 8)
	ackCheckpoint = func(_ *pgcapture.Capturer, lsn uint64) { acked <- lsn }
	events := make(chan event.Event, 3)
	events <- pgRow(1)
	events <- event.Event{EventType: event.EventCommit, EndPos: 5000}
	done := make(chan error, 1)
	go func() {
		done <- streamLoopPG(ctx, events, indexer.New(db, 1000), db, pgcapture.New(pgcapture.Config{}), 30*time.Millisecond, nil, time.Hour,
			&pgStreamState{serverID: 91}, slog.New(slog.DiscardHandler), nil)
	}()
	select {
	case lsn := <-acked:
		if lsn != 5000 {
			t.Errorf("acknowledged %d, want 5000", lsn)
		}
	case <-time.After(10 * time.Second):
		t.Error("the checkpoint was not acknowledged")
	}
	cancel()
	if err := waitLoop(t, done); err != nil {
		t.Errorf("a clean stop returned %v", err)
	}
}

// A process that lost the lock does not stamp a permanent loss on the index:
// the slot it could not read may be the new owner's to judge.
func TestIntegrationPGLockLostDoesNotRecordASlotLoss_3114(t *testing.T) {
	db := lockIndex(t)
	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(streamrun.ErrStopWithoutFlush)
	recordSlotLoss(ctx, db, 91, "slot lost", slog.New(slog.DiscardHandler))
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM stream_state`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Error("a slot loss was recorded after the lock was lost")
	}
	recordSlotLoss(context.Background(), db, 91, "slot lost", slog.New(slog.DiscardHandler))
	if err := db.QueryRow(`SELECT COUNT(*) FROM stream_state WHERE gap_lost_at IS NOT NULL`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Error("with the lock held the slot loss was not recorded")
	}
}

// A schema record whose duplicate check was waiting on the index when the lock
// went is not written.
func TestIntegrationPGLockLostBeforeASchemaRecord_3114(t *testing.T) {
	db := lockIndex(t)
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	prev := beforeWrite
	t.Cleanup(func() { beforeWrite = prev })
	beforeWrite = func(step string) {
		if step == "ddl" {
			cancel(streamrun.ErrStopWithoutFlush)
		}
	}
	send, done := lockLoop(t, ctx, db)
	send(event.Event{BinlogFile: "0/2", EndPos: 2, Schema: "public", Table: "orders", EventType: event.EventDDL, DDLType: event.DDLTruncateTable,
		DDLQuery: "TRUNCATE public.orders", Timestamp: time.Date(2026, 2, 19, 14, 0, 0, 0, time.UTC)})
	if err := waitLoop(t, done); !errors.Is(err, streamrun.ErrStopWithoutFlush) {
		t.Errorf("streamLoopPG returned %v, want ErrStopWithoutFlush", err)
	}
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM schema_changes`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Errorf("%d schema record(s) written after the lock was lost, want 0", n)
	}
}

// The lock goes partway through a health reading's writes: after the first of
// two tables that left the publication, and right before the reading is saved.
func TestIntegrationPGLockLostPartwayThroughHealthWrites_3114(t *testing.T) {
	for _, tc := range []struct {
		step     string
		at       int
		wantLeft int
	}{
		{"left", 2, 1},
		{"health", 1, 2},
	} {
		t.Run(tc.step, func(t *testing.T) {
			db := lockIndex(t)
			ctx, cancel := context.WithCancelCause(context.Background())
			defer cancel(nil)
			tables := []pgcapture.PublishedTable{
				{OID: 16400, Schema: "public", Table: "a"},
				{OID: 16401, Schema: "public", Table: "b"},
			}
			var armed atomic.Bool
			var before atomic.Value
			reads := 0
			probe := func(context.Context) (pgcapture.HealthSnapshot, error) {
				reads++
				if reads == 1 {
					return pgcapture.HealthSnapshot{Published: tables}, nil
				}
				if reads == 2 {
					var saved string
					if err := db.QueryRow(`SELECT COALESCE(source_health, '') FROM stream_state WHERE id = 1`).Scan(&saved); err != nil {
						t.Errorf("read the saved health: %v", err)
					}
					before.Store(saved)
					armed.Store(true)
				}
				return pgcapture.HealthSnapshot{}, nil
			}
			seen := 0
			prev := beforeWrite
			t.Cleanup(func() { beforeWrite = prev })
			beforeWrite = func(step string) {
				if !armed.Load() || step != tc.step {
					return
				}
				if seen++; seen == tc.at {
					cancel(streamrun.ErrStopWithoutFlush)
				}
			}
			events := make(chan event.Event)
			done := make(chan error, 1)
			go func() {
				done <- streamLoopPG(ctx, events, indexer.New(db, 1000), db, nil, time.Hour, probe, 20*time.Millisecond,
					&pgStreamState{serverID: 91}, slog.New(slog.DiscardHandler), nil)
			}()
			if err := waitLoop(t, done); !errors.Is(err, streamrun.ErrStopWithoutFlush) {
				t.Errorf("streamLoopPG returned %v, want ErrStopWithoutFlush", err)
			}
			var left int
			if err := db.QueryRow(`SELECT COUNT(*) FROM schema_changes`).Scan(&left); err != nil {
				t.Fatal(err)
			}
			if left != tc.wantLeft {
				t.Errorf("%d table-left record(s), want %d", left, tc.wantLeft)
			}
			var after string
			if err := db.QueryRow(`SELECT COALESCE(source_health, '') FROM stream_state WHERE id = 1`).Scan(&after); err != nil {
				t.Fatal(err)
			}
			if saved, _ := before.Load().(string); saved == "" || after != saved {
				t.Errorf("the saved health changed after the lock was lost:\nbefore %s\nafter  %s", saved, after)
			}
		})
	}
}

// The start's cleanup, reached with the lock already gone, removes nothing:
// the rows past the saved position may be the new owner's.
func TestIntegrationPGLockLostResumeCleanupRemovesNothing_3114(t *testing.T) {
	db := lockIndex(t)
	leftAt(t, db, 0x100, 1)
	leftAt(t, db, 0x200, 2)
	testutil.MustExec(t, db, `INSERT INTO stream_state (id, mode, binlog_file, binlog_position, flavor, events_indexed, last_checkpoint, server_id)
		VALUES (1, 'gtid', '0/100', 256, 'postgres', 2, UTC_TIMESTAMP(), 91)`)
	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(streamrun.ErrStopWithoutFlush)
	if _, _, err := deleteEventsPastCheckpointPG(ctx, db, 0x100); !errors.Is(err, context.Canceled) {
		t.Errorf("the cleanup returned %v, want it refused by the cancellation", err)
	}
	if n := pgCount(t, db); n != 2 {
		t.Errorf("the cleanup left %d row(s), want 2", n)
	}
}

// A delayed save from an older owner changes nothing: not the position, and
// not the columns that go with it. A newer or equal one saves as always, and a
// position cleared by a reset can start again from any value.
func TestIntegrationPGSavedPositionNeverMovesBack_3114(t *testing.T) {
	db := lockIndex(t)
	save := func(lsn uint64, indexed int64) {
		t.Helper()
		if err := saveCheckpointPG(db, &pgStreamState{serverID: 91, eventsIndexed: indexed}, lsn); err != nil {
			t.Fatal(err)
		}
	}
	read := func() (pos uint64, file string, indexed int64) {
		t.Helper()
		if err := db.QueryRow(`SELECT binlog_position, binlog_file, events_indexed FROM stream_state WHERE id = 1`).Scan(&pos, &file, &indexed); err != nil {
			t.Fatal(err)
		}
		return
	}
	save(9000, 9)
	save(5000, 5)
	if pos, file, indexed := read(); pos != 9000 || file != "0/2328" || indexed != 9 {
		t.Errorf("after a delayed older save: position %d, file %s, events %d; want 9000, 0/2328, 9", pos, file, indexed)
	}
	save(9000, 10)
	if pos, _, indexed := read(); pos != 9000 || indexed != 10 {
		t.Errorf("a save at the same position: position %d, events %d; want 9000 and 10", pos, indexed)
	}
	save(9500, 11)
	if pos, file, indexed := read(); pos != 9500 || file != "0/251C" || indexed != 11 {
		t.Errorf("a newer save: position %d, file %s, events %d; want 9500, 0/251C, 11", pos, file, indexed)
	}
	testutil.MustExec(t, db, `UPDATE stream_state SET binlog_position = 0 WHERE id = 1`) // what reset does
	save(100, 1)
	if pos, _, _ := read(); pos != 100 {
		t.Errorf("after a reset the position is %d, want 100", pos)
	}
}

// A stop during the check of a row against the index: the check answered
// nothing, so the row is not indexed and its commit is not saved.
func TestIntegrationPGFirstRunStopDuringTheCheckIndexesNothing_3114(t *testing.T) {
	db := lockIndex(t)
	prevWithin, prevStart, prevStep := commitWithin, capturerStart, beforeWrite
	t.Cleanup(func() { commitWithin, capturerStart, beforeWrite = prevWithin, prevStart, prevStep })
	commitWithin = func() time.Duration { return time.Hour }
	capturerStart = func(*pgcapture.Capturer) uint64 { return 100 }
	leftAt(t, db, 0x500, 1)
	at := time.Date(2026, 2, 19, 14, 0, 0, 0, time.UTC)
	for round := range 25 {
		ctx, cancel := context.WithCancel(context.Background())
		beforeWrite = func(step string) {
			if step == "recognize" {
				cancel()
			}
		}
		events := make(chan event.Event, 3)
		events <- event.Event{BinlogFile: "0/500", StartPos: 0x500, EndPos: 0x500, Timestamp: at, Schema: "public", Table: "orders",
			EventType: event.EventInsert, PKValues: "1", RowAfter: map[string]any{"id": int64(1)}}
		events <- event.Event{EventType: event.EventCommit, EndPos: 0x500}
		err := streamLoopPG(ctx, events, indexer.New(db, 1000), db, pgcapture.New(pgcapture.Config{}), time.Hour, nil, time.Hour,
			&pgStreamState{serverID: 91}, slog.New(slog.DiscardHandler), nil)
		cancel()
		if err != nil {
			t.Fatalf("round %d: %v", round, err)
		}
		if n := pgCount(t, db); n != 1 {
			t.Fatalf("round %d: the index holds %d row(s), want the 1 it had", round, n)
		}
		if lsn := savedPosition(t, db); lsn != 0 {
			t.Fatalf("round %d: position %X saved for a transaction not indexed", round, lsn)
		}
	}
}

// A check that cannot read the index keeps the change: a duplicate, never a
// loss. Past the newest row the window closes and the index is not asked.
func TestIntegrationPGFirstRunCheckFailsTowardsKeeping_3114(t *testing.T) {
	db := lockIndex(t)
	leftAt(t, db, 0x500, 1)
	ev := event.Event{StartPos: 0x500, EndPos: 0x500, Timestamp: time.Date(2026, 2, 19, 14, 0, 0, 0, time.UTC),
		Schema: "public", Table: "orders", EventType: event.EventInsert, PKValues: "1"}
	logger := slog.New(slog.DiscardHandler)

	var f firstRunResend
	f.load(context.Background(), db, 0x100, logger)
	if !f.inWindow(0x500) || f.inWindow(0xFF) || f.inWindow(0x501) {
		t.Fatalf("window [%X, %X], want [100, 500]", f.from, f.through)
	}
	if !f.heldRow(context.Background(), db, ev, logger) {
		t.Error("the row in the index was not recognized")
	}
	if f.heldRow(context.Background(), db, ev, logger) {
		t.Error("a second arrival was taken for the single copy in the index")
	}
	f.endTxn(0x500)
	if f.inWindow(0x500) {
		t.Error("the window stayed open past the newest row")
	}

	// Another transaction changing the same key in the same second, inside the
	// window, is a different change even with a copy of the key in the index.
	other := firstRunResend{from: 0x100, through: 0x700, loaded: true}
	later := ev
	later.StartPos, later.EndPos = 0x600, 0x600
	if other.heldRow(context.Background(), db, later, logger) {
		t.Error("a change of another transaction was taken for the one in the index")
	}

	broken, _ := testutil.CreateTestDB(t)
	broken.Close()
	failing := firstRunResend{from: 0x100, through: 0x500, loaded: true}
	if failing.heldRow(context.Background(), broken, ev, logger) {
		t.Error("a check that could not read the index dropped the change")
	}

	// A new slot: everything in the index is before it, so nothing matches.
	var fresh firstRunResend
	fresh.load(context.Background(), db, 0x600, logger)
	if fresh.inWindow(0x500) || fresh.through != 0 {
		t.Errorf("a slot past the index opened a window [%X, %X]", fresh.from, fresh.through)
	}
}

// The same stop during the check of a TRUNCATE marker: it is not recorded on an
// answer the check never gave.
func TestIntegrationPGFirstRunStopDuringTheMarkerCheckRecordsNothing_3114(t *testing.T) {
	db := lockIndex(t)
	prevStart, prevStep := capturerStart, beforeWrite
	t.Cleanup(func() { capturerStart, beforeWrite = prevStart, prevStep })
	capturerStart = func(*pgcapture.Capturer) uint64 { return 100 }
	leftAt(t, db, 0x500, 1)
	marker := event.Event{BinlogFile: "0/500", StartPos: 0x500, EndPos: 0x500, Schema: "public", Table: "orders", EventType: event.EventDDL,
		DDLType: event.DDLTruncateTable, DDLQuery: "TRUNCATE public.orders", Timestamp: time.Date(2026, 2, 19, 14, 0, 0, 0, time.UTC)}
	if err := indexer.InsertSchemaChange(db, marker, nil); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	beforeWrite = func(step string) {
		if step == "recognize" {
			cancel()
		}
	}
	events := make(chan event.Event, 1)
	events <- marker
	if err := streamLoopPG(ctx, events, indexer.New(db, 1000), db, pgcapture.New(pgcapture.Config{}), time.Hour, nil, time.Hour,
		&pgStreamState{serverID: 91}, slog.New(slog.DiscardHandler), nil); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM schema_changes`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("schema_changes holds %d record(s), want the 1 it had", n)
	}
}
