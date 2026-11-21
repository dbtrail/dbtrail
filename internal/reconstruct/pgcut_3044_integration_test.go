//go:build integration

package reconstruct

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// pgIndex3044 is a real index and a way to write a PostgreSQL capture's rows
// into it: commit order is event_id order, each row carries its commit LSN
// as both positions.
func pgIndex3044(t *testing.T) (db *sql.DB, first time.Time, add func(lsn uint64, at time.Time)) {
	t.Helper()
	testutil.SkipIfNoMySQL(t)
	db, _ = testutil.CreateTestDB(t)
	if err := indexer.CreateIndexTables(context.Background(), db, 12, false, nil); err != nil {
		t.Fatal(err)
	}
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatal(err)
	}
	first = time.Now().UTC().Truncate(time.Hour).Add(time.Hour)
	add = func(lsn uint64, at time.Time) {
		if _, err := db.Exec(fmt.Sprintf(`INSERT INTO binlog_events
			(binlog_file, start_pos, end_pos, event_timestamp, schema_name, table_name, event_type, pk_values, row_after)
			VALUES ('%s', %d, %d, '%s', 'public', 'orders', 1, '1', '{"id":"1"}')`,
			lsnText(lsn), lsn, lsn, at.Format("2006-01-02 15:04:05"))); err != nil {
			t.Error(err)
		}
	}
	return db, first, add
}

// betweenTheReads runs write once, after the newest event was read and
// before the real search for the first event past the point: what a capture
// that is writing does to a fold. It returns how many searches were made.
func betweenTheReads(t *testing.T, write func()) *int {
	t.Helper()
	old := pgFirstEventPast
	calls := 0
	pgFirstEventPast = func(ctx context.Context, db *sql.DB, at time.Time, floor uint64) (*query.BinlogPos, uint64, error) {
		if calls++; calls == 1 {
			write()
		}
		return firstEventPast(ctx, db, at, floor)
	}
	t.Cleanup(func() { pgFirstEventPast = old })
	return &calls
}

const otherCluster3044 = "the index now follows another PostgreSQL cluster than the one the snapshot was taken from; take a new snapshot (`bintrail-pg baseline`)"

// #3044, as it happened: an update whose point is "now", on a source that
// commits all the time. The newest change the index held was read, capture
// wrote the next transactions, and the search for the first change past the
// point found one of those: later in the WAL than the "newest", on the one
// cluster the index has ever followed.
func TestResolvePGRunEnd_captureWritesBetweenTheReads_integration(t *testing.T) {
	db, first, add := pgIndex3044(t)
	ctx := context.Background()
	at := first.Add(10 * time.Minute)
	add(0xF32FC19C0, at.Add(-time.Second))
	add(0xF32FC19D0, at)
	calls := betweenTheReads(t, func() {
		add(0xF3336CE28, at.Add(time.Second))
		add(0xF3336CE28, at.Add(time.Second)) // a second row of the same transaction
		add(0xF3336CE90, at.Add(time.Second))
	})
	run, err := resolvePGRunEnd(ctx, db, at)
	if err != nil {
		t.Fatalf("a healthy index with capture writing between the two reads: %v", err)
	}
	if *calls != 2 {
		t.Errorf("%d searches, want 2: the newest change read again once", *calls)
	}
	if run.until != 0xF3336CE27 || run.newestEnd != 0xF3336CE90 || run.allIn {
		t.Fatalf("run = until %X newest %X allIn %v, want until F3336CE27 newest F3336CE90, not all in", run.until, run.newestEnd, run.allIn)
	}
	// The table folded from a snapshot taken before: through the change at
	// the point, the next fold from the first one after it.
	b, err := run.boundsFor(0xF00000000, first, first)
	if err != nil {
		t.Fatal(err)
	}
	if b.Until == nil || b.Until.Pos != 0xF3336CE27 || b.Next != 0xF3336CE28 {
		t.Fatalf("bounds = until %+v next %X", b.Until, b.Next)
	}
}

// The same with nothing staged between the reads: a writer that never stops,
// and folds asked for at the time of the newest change the index holds,
// which is what a scheduled update of a busy source asks. Every change is
// one cluster's, in WAL order: no fold may be refused.
func TestResolvePGRunEnd_busyCapture_integration(t *testing.T) {
	db, first, add := pgIndex3044(t)
	ctx := context.Background()
	const rows = 3000
	add(0x1000, first)
	var wg sync.WaitGroup
	var done atomic.Bool
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer done.Store(true)
		for i := 1; i <= rows; i++ {
			add(0x1000+uint64(i)*0x10, first.Add(time.Duration(i)*time.Second))
			time.Sleep(500 * time.Microsecond)
		}
	}()
	old := pgFirstEventPast
	searches := 0
	pgFirstEventPast = func(ctx context.Context, db *sql.DB, at time.Time, floor uint64) (*query.BinlogPos, uint64, error) {
		searches++
		return firstEventPast(ctx, db, at, floor)
	}
	t.Cleanup(func() { pgFirstEventPast = old })
	folds := 0
	for !done.Load() {
		var at time.Time
		if err := db.QueryRowContext(ctx, `SELECT MAX(event_timestamp) FROM binlog_events`).Scan(&at); err != nil {
			t.Fatal(err)
		}
		run, err := resolvePGRunEnd(ctx, db, at)
		if err != nil {
			wg.Wait()
			t.Fatalf("fold %d at %s of a healthy index while capture writes: %v", folds+1, at.Format(time.RFC3339), err)
		}
		// Where it stops is a change's own LSN (nothing past the point) or
		// one below a change's: never beyond what the index holds.
		if run.until == 0 || run.until > run.newestEnd {
			wg.Wait()
			t.Fatalf("fold %d: until %X past the newest change %X", folds+1, run.until, run.newestEnd)
		}
		folds++
	}
	wg.Wait()
	t.Logf("%d folds, %d searches while %d changes were written", folds, searches, rows)
	if searches == folds {
		t.Fatalf("%d folds and no change was ever written between the two reads: the test did not exercise what it is for", folds)
	}
}

// A capture that starts again removes what it indexed past its saved
// position and receives it again, under new and higher ids
// (deleteEventsPastCheckpointPG). One cluster, and LSNs still in order: not
// refused, whether it happened before the fold or between its two reads.
func TestResolvePGRunEnd_captureStartedAgain_integration(t *testing.T) {
	for _, between := range []bool{false, true} {
		t.Run(fmt.Sprintf("between the reads=%v", between), func(t *testing.T) {
			db, first, add := pgIndex3044(t)
			ctx := context.Background()
			at := first.Add(10 * time.Minute)
			add(0x1000, at) // the saved position
			add(0x1010, at.Add(time.Second))
			add(0x1020, at.Add(2*time.Second))
			again := func() {
				testutil.MustExec(t, db, `DELETE FROM binlog_events WHERE start_pos > 4096`)
				add(0x1000, at) // the transaction at the saved position comes again too
				add(0x1010, at.Add(time.Second))
				add(0x1020, at.Add(2*time.Second))
			}
			calls := betweenTheReads(t, func() {
				if between {
					again()
				}
			})
			if !between {
				again()
			}
			run, err := resolvePGRunEnd(ctx, db, at)
			if err != nil {
				t.Fatal(err)
			}
			if want := map[bool]int{false: 1, true: 2}[between]; *calls != want {
				t.Errorf("%d searches, want %d", *calls, want)
			}
			if run.until != 0x100F || run.newestEnd != 0x1020 {
				t.Fatalf("run = until %X newest %X, want until 100F newest 1020", run.until, run.newestEnd)
			}
		})
	}
}

// What the check is for, against a real index: the index holds the changes
// of a cluster, and then those of another one whose LSNs started over (a
// restore, a failover to another history). Refused, in the same words, in
// every order the two clusters' changes can reach the two reads.
func TestResolvePGRunEnd_anotherClusterStillRefused_integration(t *testing.T) {
	refused := func(t *testing.T, err error) {
		t.Helper()
		if err == nil || !strings.Contains(err.Error(), otherCluster3044) {
			t.Fatalf("err = %v, want the refusal: %s", err, otherCluster3044)
		}
	}
	t.Run("both clusters in the index before the fold", func(t *testing.T) {
		db, first, add := pgIndex3044(t)
		at := first.Add(10 * time.Minute)
		add(0xF32FC19D0, at)                  // the old cluster, at the point
		add(0xF3336CE28, at.Add(time.Second)) // the old cluster, past it
		add(0x3000028, at.Add(time.Minute))   // the other cluster
		add(0x3000090, at.Add(2*time.Minute))
		calls := betweenTheReads(t, func() {})
		_, err := resolvePGRunEnd(context.Background(), db, at)
		refused(t, err)
		if !strings.Contains(err.Error(), "(LSN F/3336CE28) is later in the WAL than the newest change (LSN 0/3000090)") {
			t.Errorf("the refusal names other LSNs: %v", err)
		}
		if *calls != 1 {
			t.Errorf("%d searches, want 1", *calls)
		}
	})
	t.Run("both clusters, and capture writing between the reads", func(t *testing.T) {
		db, first, add := pgIndex3044(t)
		at := first.Add(10 * time.Minute)
		add(0xF32FC19D0, at)
		add(0xF3336CE28, at.Add(time.Second))
		add(0x3000028, at.Add(time.Minute))
		betweenTheReads(t, func() { add(0x3000090, at.Add(2*time.Minute)) })
		_, err := resolvePGRunEnd(context.Background(), db, at)
		refused(t, err)
	})
	t.Run("the source replaced between the two reads", func(t *testing.T) {
		db, first, add := pgIndex3044(t)
		at := first.Add(10 * time.Minute)
		add(0xF32FC19D0, at)
		calls := betweenTheReads(t, func() {
			add(0xF3336CE28, at.Add(time.Second)) // the old cluster's last change
			add(0x3000028, at.Add(time.Minute))   // the other cluster's first
		})
		_, err := resolvePGRunEnd(context.Background(), db, at)
		refused(t, err)
		if !strings.Contains(err.Error(), "(LSN F/3336CE28) is later in the WAL than the newest change (LSN 0/3000028)") {
			t.Errorf("the refusal names other LSNs: %v", err)
		}
		if *calls != 2 {
			t.Errorf("%d searches, want 2", *calls)
		}
	})
	// The other cluster's LSNs have not reached the old ones, and none of
	// the old cluster's changes is past the point: the two reads show
	// nothing wrong, and the snapshot's own floor does (boundsFor).
	t.Run("the other cluster's changes below the snapshot's floor", func(t *testing.T) {
		db, first, add := pgIndex3044(t)
		at := first.Add(10 * time.Minute)
		add(0xF32FC19D0, first.Add(-time.Minute)) // before the snapshot
		add(0x3000028, first.Add(time.Minute))
		add(0x3000090, first.Add(2*time.Minute))
		_, err := resolvePGFoldBounds(context.Background(), db, at, 0xF32FC19D1, first, first)
		refused(t, err)
		if !strings.Contains(err.Error(), "is after the snapshot") {
			t.Errorf("want the floor's refusal: %v", err)
		}
	})
}
