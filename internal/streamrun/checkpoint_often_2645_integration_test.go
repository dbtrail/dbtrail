//go:build integration

package streamrun

import (
	"database/sql"
	"strings"
	"testing"
	"time"

	promtest "github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/dbtrail/dbtrail/internal/parser"
)

// #2645: capture saves its position soon after the rows it names are
// committed, not only at the checkpoint interval, so a reader that compares
// the source's position with the saved one is held back by about
// checkpointSoonest. Every test runs with the interval an hour away: any save
// seen here is the early one.

func setCheckpointSoonest(t *testing.T, d time.Duration) {
	t.Helper()
	prev := checkpointSoonest
	checkpointSoonest = d
	t.Cleanup(func() { checkpointSoonest = prev })
}

// savedPosition is the saved position-mode coordinate, 0 with none saved.
func savedPosition(t *testing.T, db *sql.DB) uint64 {
	t.Helper()
	st, err := loadStreamState(db)
	if err != nil {
		t.Fatalf("loadStreamState: %v", err)
	}
	if st == nil {
		return 0
	}
	return st.binlogPos
}

// waitSaved polls until the saved position is want, and fails after limit
// with what it saw last.
func waitSaved(t *testing.T, db *sql.DB, want uint64, limit time.Duration) time.Duration {
	t.Helper()
	start := time.Now()
	for {
		got := savedPosition(t, db)
		if got == want {
			return time.Since(start)
		}
		if got > want {
			t.Fatalf("the saved position is %d, past %d, the end of the last transaction read", got, want)
		}
		if time.Since(start) > limit {
			t.Fatalf("the saved position is %d after %v, want %d", got, limit, want)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// A quiet source: one transaction, committed after commitWithin, and its
// position saved right after, an hour before the interval.
func TestIntegrationAQuietSourcesPositionIsSavedAfterItsCommit_2645(t *testing.T) {
	setCommitWithin(t, 200*time.Millisecond)
	idx, db := floorIndex(t, 1000)
	l := startLoop(t, idx, db, positionState(), time.Hour, "often-quiet")
	l.send(t, floorEvent(1, ""))
	l.send(t, floorCommit(2, ""))
	l.settle(t)
	took := waitSaved(t, db, floorCommit(2, "").EndPos, 5*time.Second)
	if got := countRows(t, db); got != 1 {
		t.Fatalf("the position is saved and a reader sees %d rows, want 1", got)
	}
	t.Logf("saved %v after the transaction was read", took)
	l.finish(t)
}

// Complete transactions waiting uncommitted (commitWithin a minute away) keep
// the position where it was: a save now would name rows the index does not
// hold, which a crash would lose for good (the resume reads from the saved
// position on).
func TestIntegrationThePositionIsNeverSavedAheadOfTheCommittedRows_2645(t *testing.T) {
	setCommitWithin(t, time.Minute)
	setCheckpointSoonest(t, 10*time.Millisecond)
	idx, db := floorIndex(t, 1000)
	l := startLoop(t, idx, db, positionState(), time.Hour, "often-ahead")
	for n := 1; n <= 5; n++ {
		l.send(t, floorEvent(2*n, ""))
		l.send(t, floorCommit(2*n+1, ""))
	}
	l.settle(t)
	time.Sleep(500 * time.Millisecond) // fifty times checkpointSoonest
	if got := countRows(t, db); got != 0 {
		t.Fatalf("with the commit a minute away a reader sees %d rows, want 0", got)
	}
	if got := savedPosition(t, db); got != 0 {
		t.Fatalf("five transactions read, none committed, and the position %d was saved", got)
	}
	l.finish(t)
}

// A busy source fills its batches: the early save adds no INSERT, and the
// saved position ends at the last transaction of the last full batch.
func TestIntegrationABusySourceKeepsItsFullBatches_2645(t *testing.T) {
	setCommitWithin(t, time.Minute)
	idx, db := floorIndex(t, 1000)
	l := startLoop(t, idx, db, positionState(), time.Hour, "often-busy")
	before := promtest.ToFloat64(l.m.BatchFlushes)
	for n := 1; n <= 3000; n++ {
		l.send(t, floorEvent(2*n, ""))
		l.send(t, floorCommit(2*n+1, ""))
	}
	l.settle(t)
	waitSaved(t, db, floorCommit(6001, "").EndPos, 5*time.Second)
	if inserts := promtest.ToFloat64(l.m.BatchFlushes) - before; inserts != 3 {
		t.Fatalf("3000 one-row transactions took %v INSERTs with batches of 1000, want 3", inserts)
	}
	l.finish(t)
}

// One transaction every 20 ms, each committed after 10 ms: the position is
// saved at most every checkpointSoonest, not once per commit.
func TestIntegrationEarlySavesAreSpacedByCheckpointSoonest_2645(t *testing.T) {
	setCommitWithin(t, 10*time.Millisecond)
	setCheckpointSoonest(t, 300*time.Millisecond)
	idx, db := floorIndex(t, 1000)
	l := startLoop(t, idx, db, positionState(), time.Hour, "often-spaced")
	before := promtest.ToFloat64(l.m.CheckpointSaves)
	start := time.Now()
	const txns = 100 // two seconds
	for n := 1; n <= txns; n++ {
		l.send(t, floorEvent(2*n, ""))
		l.send(t, floorCommit(2*n+1, ""))
		time.Sleep(20 * time.Millisecond)
	}
	l.settle(t)
	// The last one, held back by the spacing, is saved when it ends.
	waitSaved(t, db, floorCommit(2*txns+1, "").EndPos, 5*time.Second)
	elapsed := time.Since(start)
	saves := promtest.ToFloat64(l.m.CheckpointSaves) - before
	if most := float64(elapsed/(300*time.Millisecond)) + 2; saves > most {
		t.Fatalf("%v saves over %v with saves spaced by 300 ms; want at most %v", saves, elapsed, most)
	}
	if saves < 3 {
		t.Fatalf("%v saves over %v of steady commits; the position is not saved while they go on", saves, elapsed)
	}
	l.finish(t)
}

// A transaction with no row capture keeps (another table's, written by the
// source like any other) still moves the position, which the source reports
// as its own: it is saved too, or a reader would wait for the interval.
func TestIntegrationATransactionWithNoCapturedRowMovesTheSavedPosition_2645(t *testing.T) {
	idx, db := floorIndex(t, 1000)
	l := startLoop(t, idx, db, positionState(), time.Hour, "often-norows")
	l.send(t, floorCommit(1, ""))
	l.settle(t)
	waitSaved(t, db, floorCommit(1, "").EndPos, 5*time.Second)
	if got := countRows(t, db); got != 0 {
		t.Fatalf("a reader sees %d rows, want 0", got)
	}
	l.finish(t)
}

// Rows of the transaction still being read move no early save, and none
// names them: before its end the saved position stays at the last commit.
func TestIntegrationTheTransactionBeingReadMovesNoSave_2645(t *testing.T) {
	setCommitWithin(t, 50*time.Millisecond)
	idx, db := floorIndex(t, 1000)
	l := startLoop(t, idx, db, positionState(), time.Hour, "often-open")
	l.send(t, floorEvent(1, ""))
	l.send(t, floorCommit(2, ""))
	l.settle(t)
	committed := floorCommit(2, "").EndPos
	waitSaved(t, db, committed, 5*time.Second)
	for n := 3; n <= 6; n++ {
		l.send(t, floorEvent(n, ""))
	}
	l.settle(t)
	time.Sleep(500 * time.Millisecond)
	if got := savedPosition(t, db); got != committed {
		t.Fatalf("with a transaction being read the saved position moved to %d, want %d", got, committed)
	}
	l.send(t, floorCommit(7, ""))
	l.settle(t)
	waitSaved(t, db, floorCommit(7, "").EndPos, 5*time.Second)
	if got := countRows(t, db); got != 5 {
		t.Fatalf("a reader sees %d rows, want 5", got)
	}
	l.finish(t)
}

// GTID mode: the saved set takes a transaction once its rows are committed,
// and not one still being read.
func TestIntegrationGTIDModeSavesTheSetAfterTheCommit_2645(t *testing.T) {
	const g2 = "3f2a1c88-0000-0000-0000-000000002391:2"
	setCommitWithin(t, 50*time.Millisecond)
	idx, db := floorIndex(t, 1000)
	l := startLoop(t, idx, db, gtidState(t), time.Hour, "often-gtid")
	l.send(t, floorEvent(1, atomicGTID))
	l.send(t, floorCommit(2, atomicGTID))
	l.settle(t)
	set := func() string {
		st, err := loadStreamState(db)
		if err != nil {
			t.Fatalf("loadStreamState: %v", err)
		}
		if st == nil {
			return ""
		}
		return st.gtidSet
	}
	deadline := time.Now().Add(5 * time.Second)
	for set() != atomicGTID {
		if time.Now().After(deadline) {
			t.Fatalf("the saved set is %q after 5 s, want %q", set(), atomicGTID)
		}
		time.Sleep(5 * time.Millisecond)
	}
	l.send(t, parser.Event{EventType: parser.EventGTID, GTID: g2, BinlogFile: "binlog.000007"})
	l.send(t, floorEvent(3, g2))
	l.send(t, floorEvent(4, g2))
	l.settle(t)
	time.Sleep(500 * time.Millisecond)
	if got := set(); got != atomicGTID {
		t.Fatalf("with %s being read the saved set is %q, want %q", g2, got, atomicGTID)
	}
	l.finish(t)
}

// GTID mode, CREATE TABLE ... SELECT: its GTID enters the set at the DDL,
// before its rows. Neither an early save nor the interval's may save that set
// until its end: a crash in between would resume past rows never indexed.
func TestIntegrationNoSaveInsideCreateTableSelect_2645(t *testing.T) {
	for _, tc := range []struct {
		name     string
		interval time.Duration
	}{
		{"early save", time.Hour},
		{"interval", 50 * time.Millisecond},
	} {
		t.Run(tc.name, func(t *testing.T) {
			setCommitWithin(t, 20*time.Millisecond)
			setCheckpointSoonest(t, 10*time.Millisecond)
			idx, db := floorIndex(t, 1000)
			l := startLoop(t, idx, db, gtidState(t), tc.interval, "often-ctas-"+tc.name)
			l.send(t, parser.Event{EventType: parser.EventGTID, BinlogFile: "binlog.000007", EndPos: 150, GTID: atomicGTID})
			l.send(t, parser.Event{EventType: parser.EventDDL, BinlogFile: "binlog.000007", EndPos: 250, GTID: atomicGTID,
				Schema: "testdb", Table: "orders"})
			l.send(t, floorEvent(3, ""))
			l.send(t, floorEvent(4, ""))
			time.Sleep(400 * time.Millisecond)
			if st, err := loadStreamState(db); err != nil {
				t.Fatalf("loadStreamState: %v", err)
			} else if st != nil && strings.Contains(st.gtidSet, "3f2a1c88-0000-0000-0000-000000002391") {
				t.Fatalf("inside the CREATE TABLE ... SELECT the saved set already holds its GTID (%q), with %d of its rows indexed",
					st.gtidSet, countRows(t, db))
			}
			l.send(t, floorCommit(5, ""))
			l.settle(t)
			deadline := time.Now().Add(5 * time.Second)
			for {
				st, err := loadStreamState(db)
				if err != nil {
					t.Fatalf("loadStreamState: %v", err)
				}
				if st != nil && st.gtidSet == atomicGTID {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("after the end of the CREATE TABLE ... SELECT the set was not saved")
				}
				time.Sleep(5 * time.Millisecond)
			}
			if got := countRows(t, db); got != 2 {
				t.Fatalf("a reader sees %d rows, want 2", got)
			}
			l.finish(t)
		})
	}
}
