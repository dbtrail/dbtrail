//go:build integration

package streamrun

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	promtest "github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/observe"
	"github.com/dbtrail/dbtrail/internal/parser"
)

// #2666: complete transactions that do not fill a batch are committed after
// commitWithin, not at the checkpoint.

func setCommitWithin(t *testing.T, d time.Duration) {
	t.Helper()
	prev := commitWithin
	commitWithin = func() time.Duration { return d }
	t.Cleanup(func() { commitWithin = prev })
}

// waitRows polls until a reader of the index sees want rows, and fails after
// limit with what it saw last.
func waitRows(t *testing.T, db *sql.DB, want int, limit time.Duration) time.Duration {
	t.Helper()
	start := time.Now()
	for {
		got := countRows(t, db)
		if got == want {
			return time.Since(start)
		}
		if time.Since(start) > limit {
			t.Fatalf("a reader sees %d rows after %v, want %d", got, limit, want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// One transaction, then silence, with a checkpoint an hour away: the row is
// visible after commitWithin. The position is not saved by that commit.
func TestIntegrationAQuietSourcesTransactionIsVisibleWithinTheWait_2666(t *testing.T) {
	setCommitWithin(t, 200*time.Millisecond)
	idx, db := floorIndex(t, 1000)
	l := startLoop(t, idx, db, positionState(), time.Hour, "within-quiet")
	l.send(t, floorEvent(1, ""))
	l.send(t, floorCommit(2, ""))
	l.settle(t)
	waitRows(t, db, 1, 10*time.Second)
	st, err := loadStreamState(db)
	if err != nil {
		t.Fatalf("loadStreamState: %v", err)
	}
	if st != nil {
		t.Fatalf("the commit after the wait saved a position (%s:%d); only the checkpoint saves one", st.binlogFile, st.binlogPos)
	}

	// The wait starts again for what ends after that commit.
	l.send(t, floorEvent(3, ""))
	l.send(t, floorCommit(4, ""))
	l.settle(t)
	waitRows(t, db, 2, 10*time.Second)
	l.finish(t)
}

// The wait turned off: a transaction that does not fill a batch stays out of
// the index until the checkpoint, as before #2666.
func TestIntegrationWithTheWaitOffAQuietTransactionWaitsForTheCheckpoint_2666(t *testing.T) {
	setCommitWithin(t, 0)
	idx, db := floorIndex(t, 1000)
	l := startLoop(t, idx, db, positionState(), time.Hour, "within-off")
	l.send(t, floorEvent(1, ""))
	l.send(t, floorCommit(2, ""))
	l.settle(t)
	// Longer than the default wait: a zero taken for "use the default"
	// would show here.
	time.Sleep(indexer.DefaultCommitWithin + 500*time.Millisecond)
	if got := countRows(t, db); got != 0 {
		t.Fatalf("with the wait off and the checkpoint an hour away a reader sees %d rows, want 0", got)
	}
	l.finish(t)
	if got := countRows(t, db); got != 1 {
		t.Fatalf("after the loop ended a reader sees %d rows, want 1", got)
	}
}

// A transaction every 50 ms, a wait of 300 ms: the wait counts from the
// oldest transaction not committed, so rows appear while the trickle goes on.
// Counted from the newest, every transaction would postpone the commit and
// nothing would be visible until the trickle stopped.
func TestIntegrationASteadyTrickleDoesNotPostponeTheCommit_2666(t *testing.T) {
	setCommitWithin(t, 300*time.Millisecond)
	idx, db := floorIndex(t, 1000)
	l := startLoop(t, idx, db, positionState(), time.Hour, "within-trickle")
	before := promtest.ToFloat64(l.m.BatchFlushes)
	seenDuring := 0
	const txns = 40 // two seconds
	for n := 1; n <= txns; n++ {
		l.send(t, floorEvent(2*n, ""))
		l.send(t, floorCommit(2*n+1, ""))
		time.Sleep(50 * time.Millisecond)
		if n == txns-1 {
			seenDuring = countRows(t, db)
		}
	}
	if seenDuring == 0 {
		t.Fatal("after two seconds of one transaction every 50 ms a reader sees no row; each transaction postponed the commit")
	}
	// About one commit per wait, not one per transaction.
	if inserts := promtest.ToFloat64(l.m.BatchFlushes) - before; inserts > 12 {
		t.Fatalf("%d transactions over two seconds took %v INSERTs with a wait of 300 ms; want about 7", txns, inserts)
	}
	l.finish(t)
	if got := countRows(t, db); got != txns {
		t.Fatalf("want %d rows indexed, got %d", txns, got)
	}
}

// A busy source fills its batches before the wait ends: the wait adds no
// write to the index.
func TestIntegrationABusySourceCommitsByFullBatchesNotByTheWait_2666(t *testing.T) {
	setCommitWithin(t, time.Minute)
	idx, db := floorIndex(t, 1000)
	l := startLoop(t, idx, db, positionState(), time.Hour, "within-busy")
	before := promtest.ToFloat64(l.m.BatchFlushes)
	for n := 1; n <= 3000; n++ {
		l.send(t, floorEvent(2*n, ""))
		l.send(t, floorCommit(2*n+1, ""))
	}
	l.settle(t)
	if got := countRows(t, db); got != 3000 {
		t.Fatalf("three full batches read, a reader sees %d rows, want 3000", got)
	}
	if inserts := promtest.ToFloat64(l.m.BatchFlushes) - before; inserts != 3 {
		t.Fatalf("3000 one-row transactions took %v INSERTs with batches of 1000, want 3", inserts)
	}
	l.finish(t)
}

// The commit after the wait takes complete transactions only. The one still
// being read stays out of the index until its own end.
func TestIntegrationTheWaitLeavesTheTransactionBeingReadAlone_2666(t *testing.T) {
	for _, tc := range []struct {
		name  string
		gtid  string
		state func(*testing.T) *streamState
	}{
		{"position mode", "", func(*testing.T) *streamState { return positionState() }},
		{"gtid mode", atomicGTID, gtidState},
	} {
		t.Run(tc.name, func(t *testing.T) {
			setCommitWithin(t, 200*time.Millisecond)
			idx, db := floorIndex(t, 1000)
			l := startLoop(t, idx, db, tc.state(t), time.Hour, "within-open-"+tc.name)
			l.send(t, floorEvent(1, tc.gtid))
			l.send(t, floorCommit(2, tc.gtid))
			// A second transaction begins and does not end.
			l.send(t, floorEvent(3, ""))
			l.send(t, floorEvent(4, ""))
			l.settle(t)
			waitRows(t, db, 1, 10*time.Second)
			time.Sleep(500 * time.Millisecond) // more than two waits
			if got := countRows(t, db); got != 1 {
				t.Fatalf("with a transaction of two rows still being read a reader sees %d rows, want only the complete one", got)
			}
			l.send(t, floorCommit(5, ""))
			l.settle(t)
			waitRows(t, db, 3, 10*time.Second)
			l.finish(t)
		})
	}
}

// A capture that lost its lock writes nothing, the commit after the wait
// included. The wait is one nanosecond, so it has ended by the time the loop
// looks again and the stop is the only thing between the row and the index:
// whichever the loop takes first, it ends with ErrStopWithoutFlush and the row
// is not written.
func TestIntegrationTheWaitWritesNothingAfterAStopWithoutFlush_2666(t *testing.T) {
	setCommitWithin(t, time.Nanosecond)
	for round := 0; round < 20; round++ {
		idx, db := floorIndex(t, 1000)
		ctx, cancel := context.WithCancelCause(context.Background())
		// Cancelled before the transaction ends: the wait starts at its end,
		// already under the stop.
		cancel(ErrStopWithoutFlush)
		ch := make(chan parser.Event, 2)
		ch <- floorEvent(1, "")
		ch <- floorCommit(2, "")
		errc := make(chan error, 1)
		go func() {
			errc <- streamLoop(ctx, ch, idx, db, time.Hour, positionState(), observe.ForSource("within-stop"), nil)
		}()
		select {
		case err := <-errc:
			if !errors.Is(err, ErrStopWithoutFlush) {
				t.Fatalf("round %d: streamLoop ended with %v, want ErrStopWithoutFlush", round, err)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("round %d: streamLoop did not end after the stop", round)
		}
		if got := countRows(t, db); got != 0 {
			t.Fatalf("round %d: after a stop without flush a reader sees %d rows, want 0", round, got)
		}
	}
}
