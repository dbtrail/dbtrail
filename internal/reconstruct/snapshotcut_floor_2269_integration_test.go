//go:build integration

package reconstruct

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// #2269: a search for the snapshot's cut starts where the search before it
// ended, when it can prove that is safe. These run against a real MySQL: what
// is under test is which rows a statement reads and returns.

// cutEvent is one indexed event of these fixtures: start = 1000*id and
// end = start+100 in file, so a cut names the event it is the start of.
type cutEvent struct {
	id   uint64
	ts   time.Time
	file string
}

func insertCutEvent(t *testing.T, db *sql.DB, e cutEvent) {
	t.Helper()
	file := e.file
	if file == "" {
		file = "binlog.000001"
	}
	if _, err := db.Exec(`INSERT INTO binlog_events
		(event_id, binlog_file, start_pos, end_pos, event_timestamp, schema_name, table_name, event_type, pk_values, row_after)
		VALUES (?, ?, ?, ?, ?, 'shop', 'orders', 1, ?, '{}')`,
		e.id, file, e.id*1000, e.id*1000+100, e.ts.UTC().Format("2006-01-02 15:04:05"), fmt.Sprint(e.id)); err != nil {
		t.Fatalf("insert event %d: %v", e.id, err)
	}
}

// wantCut is ResolveSnapshotCut's rule worked out over the fixture: the start
// of the lowest-id event stamped past at, else the end of the newest event.
func wantCut(events []cutEvent, at time.Time) *query.BinlogPos {
	var newest *cutEvent
	for i := range events {
		e := &events[i]
		if e.ts.After(at) {
			return &query.BinlogPos{File: "binlog.000001", Pos: e.id * 1000}
		}
		newest = e
	}
	if newest == nil {
		return nil
	}
	return &query.BinlogPos{File: "binlog.000001", Pos: newest.id*1000 + 100}
}

func cutIndex(t *testing.T) *sql.DB {
	t.Helper()
	testutil.SkipIfNoMySQL(t)
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	return db
}

func mustCut(t *testing.T, db *sql.DB, at time.Time) *query.BinlogPos {
	t.Helper()
	cut, err := ResolveSnapshotCut(context.Background(), db, at)
	if err != nil {
		t.Fatalf("ResolveSnapshotCut(%s): %v", at.Format(time.RFC3339), err)
	}
	return cut
}

func sameCut(a, b *query.BinlogPos) bool {
	return (a == nil && b == nil) || (a != nil && b != nil && *a == *b)
}

// TestCutFloor_sameCutAsASearchFromTheStart: capture indexes a batch, a
// refresh resolves its cut, again and again. The stamps do not follow the
// ids: some events are stamped minutes before or after their neighbours (a
// long transaction, a session that set its own timestamp, a source clock
// ahead). After every batch the cut is resolved
// for a time that mostly moves forward and sometimes back, and must be the
// one the rule gives over all the events so far, which is also what a search
// with nothing remembered returns.
func TestCutFloor_sameCutAsASearchFromTheStart(t *testing.T) {
	for seed := int64(1); seed <= 6; seed++ {
		t.Run(fmt.Sprint("seed ", seed), func(t *testing.T) {
			db := cutIndex(t)
			r := rand.New(rand.NewSource(seed))
			base := time.Now().UTC().Truncate(time.Second).Add(-50 * time.Minute)
			var events []cutEvent
			id := uint64(0)
			clock := base
			for round := 0; round < 25; round++ {
				for n := r.Intn(12); n > 0; n-- {
					id += uint64(1 + r.Intn(3))
					clock = clock.Add(time.Duration(r.Intn(4)) * time.Second)
					ts := clock
					switch r.Intn(10) {
					case 0:
						ts = clock.Add(-time.Duration(r.Intn(600)) * time.Second) // stamped long before
					case 1:
						ts = clock.Add(time.Duration(1+r.Intn(300)) * time.Second) // stamped ahead
					}
					e := cutEvent{id: id, ts: ts}
					insertCutEvent(t, db, e)
					events = append(events, e)
				}
				at := clock.Add(time.Duration(r.Intn(5)-1) * time.Second)
				if r.Intn(6) == 0 {
					at = clock.Add(-time.Duration(r.Intn(900)) * time.Second) // a time in the past
				}
				want := wantCut(events, at)
				if got := mustCut(t, db, at); !sameCut(got, want) {
					t.Fatalf("round %d, at %s: cut = %+v, want %+v (with what earlier searches left remembered)", round, at.Format("15:04:05"), got, want)
				}
				cutFloors.reset()
				if got := mustCut(t, db, at); !sameCut(got, want) {
					t.Fatalf("round %d, at %s: cut = %+v, want %+v (a first search)", round, at.Format("15:04:05"), got, want)
				}
			}
		})
	}
}

// rowsRead runs f on one connection and returns how many rows the server
// read to answer it (the Handler_read counters of that session).
func rowsRead(t *testing.T, db *sql.DB, f func()) int64 {
	t.Helper()
	db.SetMaxOpenConns(1)
	if _, err := db.Exec("FLUSH STATUS"); err != nil {
		t.Fatalf("FLUSH STATUS: %v", err)
	}
	f()
	rows, err := db.Query("SHOW SESSION STATUS WHERE Variable_name IN ('Handler_read_first','Handler_read_key','Handler_read_next','Handler_read_prev','Handler_read_rnd_next')")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var total int64
	for rows.Next() {
		var name string
		var n int64
		if err := rows.Scan(&name, &n); err != nil {
			t.Fatal(err)
		}
		total += n
	}
	return total
}

// TestCutFloor_aLaterSearchReadsWhatWasIndexedSince is the cost #2269 is
// about, counted in rows: with 4,000 events in the hour and 20 indexed since
// the last refresh, the search reads those 20 and not the hour's 4,000.
func TestCutFloor_aLaterSearchReadsWhatWasIndexedSince(t *testing.T) {
	db := cutIndex(t)
	cutFloors.reset()
	// All inside one hour, and that hour's partition: the bound #1692 put on
	// the search does not help here.
	hour := time.Now().UTC().Truncate(time.Hour)
	if time.Since(hour) < 2*time.Minute {
		hour = hour.Add(-time.Hour)
	}
	const before = 4000
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= before; i++ {
		if _, err := tx.Exec(`INSERT INTO binlog_events (event_id, binlog_file, start_pos, end_pos, event_timestamp, schema_name, table_name, event_type, pk_values, row_after)
			VALUES (?, 'binlog.000001', ?, ?, ?, 'shop', 'orders', 1, ?, '{}')`, i, i*1000, i*1000+100, hour.Add(time.Duration(i)*10*time.Millisecond).Format("2006-01-02 15:04:05"), fmt.Sprint(i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	at1 := hour.Add(50 * time.Second)
	var first, second int64
	first = rowsRead(t, db, func() { mustCut(t, db, at1) })
	for i := before + 1; i <= before+20; i++ {
		insertCutEvent(t, db, cutEvent{id: uint64(i), ts: hour.Add(55 * time.Second)})
	}
	at2 := hour.Add(60 * time.Second)
	var got *query.BinlogPos
	second = rowsRead(t, db, func() { got = mustCut(t, db, at2) })
	if want := (&query.BinlogPos{File: "binlog.000001", Pos: (before+20)*1000 + 100}); !sameCut(got, want) {
		t.Fatalf("cut = %+v, want %+v", got, want)
	}
	t.Logf("rows read: first search %d, the one after it %d", first, second)
	if first < before {
		t.Fatalf("the first search read %d rows; the fixture is meant to make it read the hour's %d", first, before)
	}
	if second > 200 {
		t.Fatalf("the search after 20 new events read %d rows, want about 20: it started over from the hour's first event", second)
	}
}

// TestCutFloor_isDroppedWhenItCannotBeTrusted: each case leaves something
// remembered, changes the index so that it no longer holds, and puts an event
// stamped past the time BELOW the remembered floor. A search that still
// started at the floor would miss it and anchor the snapshot past it.
func TestCutFloor_isDroppedWhenItCannotBeTrusted(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second).Add(-20 * time.Minute)
	old := func(id uint64) cutEvent { return cutEvent{id: id, ts: now.Add(-time.Minute)} }
	prime := func(t *testing.T) *sql.DB {
		db := cutIndex(t)
		cutFloors.reset()
		for id := uint64(10); id <= 50; id += 10 {
			insertCutEvent(t, db, old(id))
		}
		if got, want := mustCut(t, db, now), (&query.BinlogPos{File: "binlog.000001", Pos: 50*1000 + 100}); !sameCut(got, want) {
			t.Fatalf("priming search: cut = %+v, want %+v", got, want)
		}
		return db
	}
	exec := func(t *testing.T, db *sql.DB, q string, args ...any) {
		t.Helper()
		if _, err := db.Exec(q, args...); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	past := cutEvent{id: 25, ts: now.Add(30 * time.Second)} // below the floor (51), past every time asked
	wantPast := &query.BinlogPos{File: "binlog.000001", Pos: 25 * 1000}

	t.Run("the event it was read with is gone", func(t *testing.T) {
		db := prime(t)
		exec(t, db, "DELETE FROM binlog_events WHERE event_id = 50")
		insertCutEvent(t, db, past)
		if got := mustCut(t, db, now.Add(time.Second)); !sameCut(got, wantPast) {
			t.Fatalf("cut = %+v, want %+v", got, wantPast)
		}
	})
	t.Run("its id names another event now", func(t *testing.T) {
		db := prime(t)
		exec(t, db, "UPDATE binlog_events SET end_pos = end_pos + 7 WHERE event_id = 50")
		insertCutEvent(t, db, past)
		if got := mustCut(t, db, now.Add(time.Second)); !sameCut(got, wantPast) {
			t.Fatalf("cut = %+v, want %+v", got, wantPast)
		}
	})
	t.Run("its id names an event of another file now", func(t *testing.T) {
		db := prime(t)
		exec(t, db, "UPDATE binlog_events SET binlog_file = 'binlog.000002' WHERE event_id = 50")
		insertCutEvent(t, db, past)
		got := mustCut(t, db, now.Add(time.Second))
		if !sameCut(got, wantPast) {
			t.Fatalf("cut = %+v, want %+v", got, wantPast)
		}
	})
	t.Run("the time asked for is before the one remembered", func(t *testing.T) {
		db := prime(t)
		// Stamped between the two times, below the floor: past the earlier
		// time, not past the remembered one. Indexed with a low id as a
		// rebuilt index would.
		insertCutEvent(t, db, cutEvent{id: 25, ts: now.Add(-30 * time.Second)})
		if got := mustCut(t, db, now.Add(-45*time.Second)); !sameCut(got, wantPast) {
			t.Fatalf("cut = %+v, want %+v", got, wantPast)
		}
		// And the search for the earlier time did not replace what the
		// later one left: asked for the later time again, the floor holds
		// and the cut is the newest event's end.
		if got, want := mustCut(t, db, now), (&query.BinlogPos{File: "binlog.000001", Pos: 50*1000 + 100}); !sameCut(got, want) {
			t.Fatalf("the later time again: cut = %+v, want %+v", got, want)
		}
	})
	t.Run("another index on the same server", func(t *testing.T) {
		prime(t) // leaves a floor for ITS database
		other := cutIndex(t)
		for id := uint64(10); id <= 50; id += 10 {
			insertCutEvent(t, other, old(id))
		}
		insertCutEvent(t, other, past)
		if got := mustCut(t, other, now.Add(time.Second)); !sameCut(got, wantPast) {
			t.Fatalf("cut on the other index = %+v, want %+v", got, wantPast)
		}
	})
}

// TestCutFloor_theEventFoundIsTheNextFloor: with an event stamped past the
// time (a source clock ahead), the cut is that event's start, and the next
// search starts AT it, not after it: it is still the cut while it stays past
// the time asked for, and stops being it once the time passes its stamp.
func TestCutFloor_theEventFoundIsTheNextFloor(t *testing.T) {
	db := cutIndex(t)
	cutFloors.reset()
	now := time.Now().UTC().Truncate(time.Second).Add(-20 * time.Minute)
	insertCutEvent(t, db, cutEvent{id: 10, ts: now.Add(-time.Minute)})
	insertCutEvent(t, db, cutEvent{id: 20, ts: now.Add(40 * time.Second)}) // ahead
	insertCutEvent(t, db, cutEvent{id: 30, ts: now.Add(-time.Second)})
	ahead := &query.BinlogPos{File: "binlog.000001", Pos: 20 * 1000}
	for _, d := range []time.Duration{0, 10 * time.Second, 39 * time.Second} {
		if got := mustCut(t, db, now.Add(d)); !sameCut(got, ahead) {
			t.Fatalf("at +%s: cut = %+v, want the start of the event stamped ahead, %+v", d, got, ahead)
		}
	}
	insertCutEvent(t, db, cutEvent{id: 40, ts: now.Add(50 * time.Second)})
	if got, want := mustCut(t, db, now.Add(45*time.Second)), (&query.BinlogPos{File: "binlog.000001", Pos: 40 * 1000}); !sameCut(got, want) {
		t.Fatalf("at +45s: cut = %+v, want %+v", got, want)
	}
	if got, want := mustCut(t, db, now.Add(55*time.Second)), (&query.BinlogPos{File: "binlog.000001", Pos: 40*1000 + 100}); !sameCut(got, want) {
		t.Fatalf("at +55s: cut = %+v, want the newest event's end, %+v", got, want)
	}
}
