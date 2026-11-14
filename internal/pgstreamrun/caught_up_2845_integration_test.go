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

// Where a capture started reading from (#2843): a first run writes it, a
// resumed run leaves it, and a later first run (one with no checkpoint to
// resume: the capture before it ended) writes its own over it, an unknown
// start included.
func TestIntegrationPGCaptureFromIsWrittenOnce_2843(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, _ := testutil.CreateTestDB(t)
	if err := indexer.CreateIndexTables(context.Background(), db, 4, false, nil); err != nil {
		t.Fatalf("CreateIndexTables: %v", err)
	}
	captureFrom := func() (int64, bool) {
		t.Helper()
		var v sql.NullInt64
		if err := db.QueryRow("SELECT pg_capture_from_lsn FROM stream_state WHERE id = 1").Scan(&v); err != nil && !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("read pg_capture_from_lsn: %v", err)
		}
		return v.Int64, v.Valid
	}
	// run drives one life of the loop over a commit at pos.
	run := func(state *pgStreamState, n int, pos uint64) {
		t.Helper()
		idx := indexer.New(db, 1000)
		events := make(chan event.Event)
		done := make(chan error, 1)
		go func() {
			done <- streamLoopPG(context.Background(), events, idx, db, nil, 10*time.Millisecond, nil, time.Hour,
				state, slog.New(slog.DiscardHandler), nil)
		}()
		events <- pgRow(n)
		events <- event.Event{EventType: event.EventCommit, EndPos: pos}
		waitSentRow(t, db, "the commit checkpointed", func(r sentRow) bool { return r.pos.Int64 == int64(pos) })
		close(events)
		if err := <-done; err != nil {
			t.Fatalf("streamLoopPG: %v", err)
		}
	}

	// A first run that does not know where it started records nothing.
	run(&pgStreamState{serverID: 92843}, 1, 1500)
	if v, ok := captureFrom(); ok {
		t.Fatalf("a start of %d on record with none known", v)
	}
	// A first run that knows.
	run(&pgStreamState{serverID: 92843, captureFrom: 1200}, 2, 2500)
	if v, ok := captureFrom(); !ok || v != 1200 {
		t.Fatalf("the start on record is %d (%v), want 1200", v, ok)
	}
	// A resumed run saves checkpoints and leaves it alone.
	run(&pgStreamState{serverID: 92843, lsn: 2500, resumed: true}, 3, 3500)
	if v, ok := captureFrom(); !ok || v != 1200 {
		t.Fatalf("after a resumed run the start on record is %d (%v), want 1200", v, ok)
	}
	// And a resumed run that somehow carries a start does not write it.
	run(&pgStreamState{serverID: 92843, lsn: 3500, resumed: true, captureFrom: 7000}, 4, 4500)
	if v, ok := captureFrom(); !ok || v != 1200 {
		t.Fatalf("a resumed run wrote a start: %d (%v), want 1200", v, ok)
	}
	// Another first run is another capture: its start replaces the old one.
	run(&pgStreamState{serverID: 92843, captureFrom: 9000}, 5, 9500)
	if v, ok := captureFrom(); !ok || v != 9000 {
		t.Fatalf("a new capture's start on record is %d (%v), want its own, 9000", v, ok)
	}
	// And one that does not know where it started removes the old one: the
	// start of a capture that ended must not vouch for this one.
	run(&pgStreamState{serverID: 92843}, 6, 9900)
	if v, ok := captureFrom(); ok {
		t.Fatalf("the start of the capture before, %d, survived a first run with none known", v)
	}
}

// Against a real PostgreSQL, the two orders a new server can be set up in:
// capture first (it creates the slot, and starts at its consistent point), or
// a snapshot first (the slot is already there, and capture starts where the
// slot was left, which is the floor that snapshot recorded).
func TestIntegrationPGCaptureFromRealSource_2843(t *testing.T) {
	pgDSN := testutil.SkipIfNoPostgres(t)
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()
	repl := pgDSN + "?replication=database"
	if strings.Contains(pgDSN, "?") {
		repl = pgDSN + "&replication=database"
	}
	for _, tc := range []struct {
		name       string
		slotFirst  bool
		askEarlier bool
	}{
		{name: "capture creates the slot"},
		{name: "the slot is already there", slotFirst: true},
		// An LSN asked for that is before the slot: PostgreSQL starts at the
		// slot, and that is what must be on record.
		{name: "the slot is already there and an earlier LSN is asked for", slotFirst: true, askEarlier: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			indexDB, dbName := testutil.CreateTestDB(t)
			const slot, pub, tbl = "bintrail_pgfrom_it", "bintrail_pgfrom_it_pub", "pgfrom_it_t"
			pg, err := pgx.Connect(ctx, pgDSN)
			if err != nil {
				t.Fatalf("connect PG: %v", err)
			}
			t.Cleanup(func() { pg.Close(context.Background()) })
			dropAll := func() {
				bg := context.Background()
				_, _ = pg.Exec(bg, "DROP PUBLICATION IF EXISTS "+pub)
				_, _ = pg.Exec(bg, "DROP TABLE IF EXISTS "+tbl)
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
			mustExec(fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s", pub, tbl))

			var floor uint64
			if tc.slotFirst {
				mustExec(fmt.Sprintf("SELECT pg_create_logical_replication_slot('%s', 'pgoutput')", slot))
				f, err := pgcapture.SlotFloorLSN(ctx, pg, slot)
				if err != nil {
					t.Fatalf("the slot's floor: %v", err)
				}
				floor = uint64(f)
				// WAL written between the slot and the start of capture, as a
				// snapshot's own reads and any other session would.
				mustExec(fmt.Sprintf("INSERT INTO %s VALUES (100, 'before capture')", tbl))
			}

			var startLSN uint64
			if tc.askEarlier {
				startLSN = floor - 64
			}
			life := func() (stop func()) {
				runCtx, cancel := context.WithCancel(ctx)
				runErr := make(chan error, 1)
				go func() {
					runErr <- One(runCtx, Config{
						IndexDSN: testutil.BaseDSN() + "/" + dbName, ReplDSN: repl, QueryDSN: pgDSN,
						SlotName: slot, Publication: pub, ServerID: 2843, Tables: "public." + tbl,
						BatchSize: 100, Checkpoint: 200 * time.Millisecond, StartLSN: startLSN,
					})
				}()
				waitCaughtUp(t, "the slot active", 30*time.Second, func() bool {
					var active bool
					return pg.QueryRow(ctx, "SELECT active FROM pg_replication_slots WHERE slot_name=$1", slot).Scan(&active) == nil && active
				})
				return func() {
					cancel()
					if err := <-runErr; err != nil {
						t.Fatalf("One: %v", err)
					}
				}
			}
			recorded := func() (uint64, bool) {
				var v sql.NullInt64
				if err := indexDB.QueryRow("SELECT pg_capture_from_lsn FROM stream_state WHERE id = 1").Scan(&v); err != nil {
					return 0, false
				}
				return uint64(v.Int64), v.Valid
			}

			stop := life()
			mustExec(fmt.Sprintf("INSERT INTO %s VALUES (1, 'a')", tbl))
			waitCaughtUp(t, "where capture started on record", 30*time.Second, func() bool { _, ok := recorded(); return ok })
			from, _ := recorded()
			var oldest uint64
			if err := indexDB.QueryRow("SELECT MIN(start_pos) FROM binlog_events").Scan(&oldest); err != nil {
				t.Fatalf("the oldest event: %v", err)
			}
			if from == 0 || from > oldest {
				t.Fatalf("capture started at %#x on record, past its own oldest event at %#x", from, oldest)
			}
			if tc.slotFirst && from != floor {
				t.Fatalf("capture started at %#x on record, want the floor the slot had before it, %#x", from, floor)
			}
			stop()

			// A resume leaves the record alone, however far the slot has moved.
			stop = life()
			mustExec(fmt.Sprintf("INSERT INTO %s VALUES (2, 'b')", tbl))
			waitCaughtUp(t, "the second change indexed", 30*time.Second, func() bool {
				var n int
				return indexDB.QueryRow("SELECT COUNT(*) FROM binlog_events WHERE pk_values = '2'").Scan(&n) == nil && n == 1
			})
			stop()
			if now, ok := recorded(); !ok || now != from {
				t.Fatalf("after a resume the start on record is %#x (%v), want %#x", now, ok, from)
			}
		})
	}
}
