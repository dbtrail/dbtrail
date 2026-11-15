//go:build integration

package pgstreamrun

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/pgcapture"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// stream_state.caught_up_at for a PostgreSQL capture (#2845): when it last
// held everything its source had.

// caughtUpAgo reads the stamp as its age in microseconds on the index's
// clock (the way its readers do), and whether there is one.
func caughtUpAgo(t *testing.T, db *sql.DB) (time.Duration, bool) {
	t.Helper()
	var us sql.NullInt64
	err := db.QueryRow(`SELECT TIMESTAMPDIFF(MICROSECOND, caught_up_at, UTC_TIMESTAMP(6)) FROM stream_state WHERE id = 1`).Scan(&us)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false
	}
	if err != nil {
		t.Fatalf("read caught_up_at: %v", err)
	}
	return time.Duration(us.Int64) * time.Microsecond, us.Valid
}

// caughtUpStamp reads the stamp itself, as text, to tell two apart.
func caughtUpStamp(t *testing.T, db *sql.DB) string {
	t.Helper()
	var s sql.NullString
	if err := db.QueryRow(`SELECT CAST(caught_up_at AS CHAR) FROM stream_state WHERE id = 1`).Scan(&s); err != nil && !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("read caught_up_at: %v", err)
	}
	return s.String
}

func waitCaughtUp(t *testing.T, what string, limit time.Duration, done func() bool) {
	t.Helper()
	deadline := time.Now().Add(limit)
	for !done() {
		if time.Now().After(deadline) {
			t.Fatalf("%s: not within %s", what, limit)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// The loop, with the source's readings fed by the test: the stamp is written
// only for a reading the received sent position has reached, at the moment
// the source was asked, and is kept when there is nothing newer. (That it
// must be reached within caughtUpWithin is TestCaughtUpProof's: here a
// reading is taken every 10 ms, so the one waiting is always recent.)
func TestIntegrationPGCaughtUpStampFollowsWhatWasReached_2845(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, _ := testutil.CreateTestDB(t)
	if err := indexer.CreateIndexTables(context.Background(), db, 4, false, nil); err != nil {
		t.Fatalf("CreateIndexTables: %v", err)
	}
	var flushed atomic.Uint64
	var failing atomic.Bool
	probe := func(context.Context) (pgcapture.HealthSnapshot, error) {
		if failing.Load() {
			return pgcapture.HealthSnapshot{FlushedLSNErr: errors.New("permission denied")}, nil
		}
		return pgcapture.HealthSnapshot{FlushedLSN: flushed.Load()}, nil
	}
	idx := indexer.New(db, 1000)
	events := make(chan event.Event)
	done := make(chan error, 1)
	go func() {
		done <- streamLoopPG(context.Background(), events, idx, db, nil, 10*time.Millisecond, probe, 10*time.Millisecond,
			&pgStreamState{serverID: 92845}, slog.New(slog.DiscardHandler), nil)
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

	// A committed transaction and a sent-through position of 2000, with the
	// source flushed to 5000: the capture has not reached what the source
	// had, and nothing is stamped however many checkpoints pass.
	flushed.Store(5000)
	send(pgRow(1))
	send(event.Event{EventType: event.EventCommit, EndPos: 1500})
	send(sentMarkerAt(2000))
	waitSentRow(t, db, "the sent position saved", func(r sentRow) bool { return r.sent.Valid && r.sent.Int64 == 2000 })
	time.Sleep(150 * time.Millisecond)
	if ago, ok := caughtUpAgo(t, db); ok {
		t.Fatalf("stamped %s ago with the capture at 2000 and the source at 5000", ago)
	}

	// The capture reaches it.
	before := time.Now()
	send(sentMarkerAt(5000))
	waitCaughtUp(t, "a stamp once the capture reached the source", 5*time.Second, func() bool { _, ok := caughtUpAgo(t, db); return ok })
	if ago, _ := caughtUpAgo(t, db); ago < 0 || ago > time.Since(before)+2*time.Second {
		t.Fatalf("the stamp is %s old, %s after the capture reached the source", ago, time.Since(before))
	}

	// The source moves on and the capture does not: the stamp stays where it
	// was, and grows old.
	flushed.Store(9000)
	time.Sleep(100 * time.Millisecond)
	held := caughtUpStamp(t, db)
	if held == "" {
		t.Fatal("the checkpoints with nothing new to stamp erased the stamp")
	}
	time.Sleep(200 * time.Millisecond)
	if now := caughtUpStamp(t, db); now != held {
		t.Fatalf("the stamp moved from %s to %s with the capture at 5000 and the source at 9000", held, now)
	}

	// A reading that fails is not one: reaching 9000 while the source cannot
	// be read stamps nothing.
	failing.Store(true)
	time.Sleep(50 * time.Millisecond)
	send(sentMarkerAt(9000))
	time.Sleep(200 * time.Millisecond)
	if now := caughtUpStamp(t, db); now != held {
		t.Fatalf("the stamp moved from %s to %s while the source's position could not be read", held, now)
	}
	// It can be read again, and it is where the capture already is: a quiet
	// source, stamped at the next reading with no new position to receive.
	failing.Store(false)
	waitCaughtUp(t, "a stamp once the source could be read again", 5*time.Second, func() bool { return caughtUpStamp(t, db) != held })

	// A lost slot removes the stamp: capture is not caught up with anything.
	persistSlotLostPG(db, 92845, "test: slot lost", slog.New(slog.DiscardHandler))
	if ago, ok := caughtUpAgo(t, db); ok {
		t.Fatalf("a stamp %s old survived a lost slot", ago)
	}
}

// A source that keeps writing: each reading is past every position the loop
// has received, so nothing a reading finds already there can answer it. Only
// the position that arrives after the asking can, and it does when it
// arrives in time.
func TestIntegrationPGCaughtUpStampOnABusySource_2845(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, _ := testutil.CreateTestDB(t)
	if err := indexer.CreateIndexTables(context.Background(), db, 4, false, nil); err != nil {
		t.Fatalf("CreateIndexTables: %v", err)
	}
	// Reading n answers n*1000: always ahead of what was received before it.
	var readings atomic.Uint64
	probe := func(context.Context) (pgcapture.HealthSnapshot, error) {
		return pgcapture.HealthSnapshot{FlushedLSN: readings.Add(1) * 1000}, nil
	}
	idx := indexer.New(db, 1000)
	events := make(chan event.Event)
	done := make(chan error, 1)
	const poll = 400 * time.Millisecond
	go func() {
		done <- streamLoopPG(context.Background(), events, idx, db, nil, 10*time.Millisecond, probe, poll,
			&pgStreamState{serverID: 92846}, slog.New(slog.DiscardHandler), nil)
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
	send(pgRow(1))
	send(event.Event{EventType: event.EventCommit, EndPos: 500})

	// The capture stays one reading behind: it receives each position only
	// after the next reading was taken. Never in time for the one waiting.
	for range 3 {
		n := readings.Load()
		waitCaughtUp(t, "the next reading", 5*time.Second, func() bool { return readings.Load() > n })
		send(sentMarkerAt(n * 1000))
	}
	time.Sleep(100 * time.Millisecond)
	if ago, ok := caughtUpAgo(t, db); ok {
		t.Fatalf("stamped %s ago with the capture always one reading behind", ago)
	}

	// It receives the position of the reading that is waiting, right after
	// that reading: caught up at the moment the source was asked.
	n := readings.Load()
	waitCaughtUp(t, "the next reading", 5*time.Second, func() bool { return readings.Load() > n })
	asked := time.Now()
	send(sentMarkerAt(readings.Load() * 1000))
	waitCaughtUp(t, "a stamp for the reading reached in time", 5*time.Second, func() bool { _, ok := caughtUpAgo(t, db); return ok })
	if ago, _ := caughtUpAgo(t, db); ago < 0 || ago > time.Since(asked)+poll {
		t.Fatalf("the stamp is %s old, %s after the reading it answers", ago, time.Since(asked))
	}
}

// Against a real PostgreSQL: a capture that keeps up is stamped after a
// change it indexes and after WAL it has nothing to index, and a capture
// that is stopped is stamped no more.
func TestIntegrationPGCaughtUpStampRealSource_2845(t *testing.T) {
	pgDSN := testutil.SkipIfNoPostgres(t)
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()
	indexDB, dbName := testutil.CreateTestDB(t)

	const slot, pub, tbl, other = "bintrail_pgcu_it", "bintrail_pgcu_it_pub", "pgcu_it_t", "pgcu_it_unpublished"
	pg, err := pgx.Connect(ctx, pgDSN)
	if err != nil {
		t.Fatalf("connect PG: %v", err)
	}
	t.Cleanup(func() { pg.Close(context.Background()) })
	dropAll := func() {
		bg := context.Background()
		_, _ = pg.Exec(bg, "DROP PUBLICATION IF EXISTS "+pub)
		_, _ = pg.Exec(bg, "DROP TABLE IF EXISTS "+tbl)
		_, _ = pg.Exec(bg, "DROP TABLE IF EXISTS "+other)
		for range 50 {
			_, _ = pg.Exec(bg, "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1 AND NOT active", slot)
			var n int
			if err := pg.QueryRow(bg, "SELECT COUNT(*) FROM pg_replication_slots WHERE slot_name = $1", slot).Scan(&n); err != nil || n == 0 {
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
	dropAll()
	t.Cleanup(dropAll)
	mustExec := func(q string) {
		t.Helper()
		if _, err := pg.Exec(ctx, q); err != nil {
			t.Fatalf("exec %q: %v", q, err)
		}
	}
	mustExec(fmt.Sprintf("CREATE TABLE %s (id int PRIMARY KEY, v text)", tbl))
	mustExec(fmt.Sprintf("ALTER TABLE %s REPLICA IDENTITY FULL", tbl))
	mustExec(fmt.Sprintf("CREATE TABLE %s (id int PRIMARY KEY, v text)", other))
	mustExec(fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s", pub, tbl))

	prevPoll := healthPollInterval
	healthPollInterval = 300 * time.Millisecond
	t.Cleanup(func() { healthPollInterval = prevPoll })

	repl := pgDSN + "?replication=database"
	if strings.Contains(pgDSN, "?") {
		repl = pgDSN + "&replication=database"
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	runErr := make(chan error, 1)
	go func() {
		runErr <- One(runCtx, Config{
			IndexDSN: testutil.BaseDSN() + "/" + dbName, ReplDSN: repl, QueryDSN: pgDSN,
			SlotName: slot, Publication: pub, ServerID: 2845, Tables: "public." + tbl,
			BatchSize: 100, Checkpoint: 200 * time.Millisecond,
		})
	}()
	waitCaughtUp(t, "the slot active", 30*time.Second, func() bool {
		var active bool
		return pg.QueryRow(ctx, "SELECT active FROM pg_replication_slots WHERE slot_name=$1", slot).Scan(&active) == nil && active
	})

	// fresh waits for a stamp newer than held, and answers with it.
	fresh := func(what, held string) string {
		t.Helper()
		start := time.Now()
		waitCaughtUp(t, what, 30*time.Second, func() bool { s := caughtUpStamp(t, indexDB); return s != "" && s != held })
		ago, _ := caughtUpAgo(t, indexDB)
		t.Logf("%s: stamped after %s, the stamp %s old (poll %s, checkpoint 200ms)", what, time.Since(start).Round(10*time.Millisecond), ago.Round(time.Millisecond), healthPollInterval)
		if ago < 0 || ago > 10*time.Second {
			t.Fatalf("%s: the stamp is %s old", what, ago)
		}
		return caughtUpStamp(t, indexDB)
	}
	mustExec(fmt.Sprintf("INSERT INTO %s VALUES (1, 'a')", tbl))
	held := fresh("after a captured change", "")
	// A capture that keeps up on a source nobody writes to is stamped again.
	held = fresh("on a quiet source", held)
	// WAL with nothing to index moves the source; the capture follows.
	mustExec(fmt.Sprintf("INSERT INTO %s SELECT g, repeat('x', 200) FROM generate_series(1, 2000) g", other))
	held = fresh("after changes outside the publication", held)

	// Capture stops; the source keeps changing; the stamp is the last one.
	cancel()
	if err := <-runErr; err != nil {
		t.Fatalf("One: %v", err)
	}
	held = caughtUpStamp(t, indexDB)
	mustExec(fmt.Sprintf("INSERT INTO %s VALUES (2, 'missed')", tbl))
	time.Sleep(time.Second)
	if now := caughtUpStamp(t, indexDB); now != held {
		t.Fatalf("the stamp moved from %s to %s with capture stopped", held, now)
	}
}
