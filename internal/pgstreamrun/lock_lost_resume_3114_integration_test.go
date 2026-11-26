//go:build integration

package pgstreamrun_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"

	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/pgstreamrun"
	"github.com/dbtrail/dbtrail/internal/streamrun"
)

func (r *resumeRig) confirmedFlush() pglogrepl.LSN {
	r.t.Helper()
	var s string
	if err := r.pg.QueryRow(r.ctx, "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name=$1", r.slot).Scan(&s); err != nil {
		r.t.Fatalf("read confirmed_flush_lsn: %v", err)
	}
	lsn, err := pglogrepl.ParseLSN(s)
	if err != nil {
		r.t.Fatal(err)
	}
	return lsn
}

// #3114 over real replication: the second run loses its lock with a committed
// transaction received and not saved. It writes neither that row nor its
// position and the slot does not move past the saved position. The next run
// receives the transaction again and the index holds every row once.
func TestIntegrationPGLockLostResumesWithoutLossOrRepeat_3114(t *testing.T) {
	r := newResumeRig(t, "locklost", 1000)

	stop := r.start()
	r.insert(1)
	at := r.waitSavedAt(1)
	stop()

	// Nothing but the stop would save in this run.
	t.Setenv(indexer.CommitWithinEnv, "1h")
	second := r.cfg
	second.Checkpoint = time.Hour
	ctx, lose := context.WithCancelCause(r.ctx)
	defer lose(nil)
	// A commit handled by the loop: proof the transaction reached this
	// process, so "not written" below is about a row it had. The first one is
	// the saved transaction, which PostgreSQL sends again.
	var commits atomic.Int64
	restore := pgstreamrun.SetBeforeWriteForTest(func(step string) {
		if step == "received" {
			commits.Add(1)
		}
	})
	ended := make(chan error, 1)
	go func() { ended <- pgstreamrun.One(ctx, second) }()
	waitFor(t, 20*time.Second, r.slotActive, "replication slot active")
	r.insert(2)
	waitFor(t, 20*time.Second, func() bool { return commits.Load() >= 2 }, "the second transaction received")
	if got := r.copies(); got != once(1) {
		t.Fatalf("before the lock was lost the index holds %q, want %q", got, once(1))
	}

	lose(streamrun.ErrStopWithoutFlush)
	select {
	case err := <-ended:
		if !errors.Is(err, streamrun.ErrStopWithoutFlush) {
			t.Errorf("One returned %v, want ErrStopWithoutFlush", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("One did not stop")
	}
	restore()
	waitFor(t, 15*time.Second, func() bool { return !r.slotActive() }, "replication slot inactive")

	if got := r.copies(); got != once(1) {
		t.Errorf("after the lock was lost the index holds %q, want %q", got, once(1))
	}
	if got := r.saved(); got != at {
		t.Errorf("the saved position moved to %X after the lock was lost, want %X", got, at)
	}
	if got := r.confirmedFlush(); uint64(got) > at {
		t.Errorf("the slot was confirmed through %s, past the saved position %s", got, pglogrepl.LSN(at))
	}

	t.Setenv(indexer.CommitWithinEnv, "")
	stop = r.start()
	r.insert(3)
	r.waitSavedAt(3)
	stop()
	if got := r.copies(); got != once(1, 2, 3) {
		t.Errorf("after resuming the index holds %q, want %q", got, once(1, 2, 3))
	}
}

// firstRunLosesLock runs a first capture on the rig and loses the lock between
// its rows reaching the index and its first position save.
func (r *resumeRig) firstRunLosesLock(cfg pgstreamrun.Config, write func()) {
	r.t.Helper()
	ctx, lose := context.WithCancelCause(r.ctx)
	defer lose(nil)
	restore := pgstreamrun.SetBeforeWriteForTest(func(step string) {
		if step == "save" {
			lose(streamrun.ErrStopWithoutFlush)
		}
	})
	ended := make(chan error, 1)
	go func() { ended <- pgstreamrun.One(ctx, cfg) }()
	waitFor(r.t, 20*time.Second, r.slotActive, "replication slot active")
	write()
	select {
	case err := <-ended:
		if !errors.Is(err, streamrun.ErrStopWithoutFlush) {
			r.t.Fatalf("One returned %v, want ErrStopWithoutFlush", err)
		}
	case <-time.After(30 * time.Second):
		r.t.Fatal("the first run did not stop")
	}
	restore()
	waitFor(r.t, 15*time.Second, func() bool { return !r.slotActive() }, "replication slot inactive")
	if got := r.saved(); got != 0 {
		r.t.Fatalf("the first run saved position %X; the test needs it to stop before", got)
	}
}

// The lock goes between the first rows and the first save: no position, the
// slot did not move, and PostgreSQL sends everything again. Each row ends up in
// the index once, and a later change of the same key is kept.
func TestIntegrationPGLockLostBeforeTheFirstSave_3114(t *testing.T) {
	r := newResumeRig(t, "lockfirst", 1000)
	r.firstRunLosesLock(r.cfg, func() {
		r.txn(
			fmt.Sprintf("INSERT INTO %s VALUES (1, 'a'), (2, 'b')", r.tbl),
			fmt.Sprintf("UPDATE %s SET v = 'c' WHERE id = 1", r.tbl),
			fmt.Sprintf("UPDATE %s SET v = 'd' WHERE id = 1", r.tbl),
		)
	})
	if got := r.copies(); got != "1:3 2:1" {
		t.Fatalf("the first run left %q, want the whole transaction", got)
	}
	r.exec(fmt.Sprintf("UPDATE %s SET v = 'e' WHERE id = 1", r.tbl)) // while capture is down
	stop := r.start()
	r.insert(3)
	r.waitSavedAt(3)
	stop()
	if got := r.copies(); got != "1:4 2:1 3:1" {
		t.Errorf("after resuming the index holds %q, want 1:4 2:1 3:1", got)
	}
}

// Part of a transaction in the index (past the writer's caps): what is there
// is not repeated and what is missing arrives, kind by kind of the same key.
func TestIntegrationPGFirstRunCompletesAPartialTransaction_3114(t *testing.T) {
	r := newResumeRig(t, "lockpart", 1000)
	r.firstRunLosesLock(r.cfg, func() {
		r.txn(
			fmt.Sprintf("INSERT INTO %s VALUES (1, 'a')", r.tbl),
			fmt.Sprintf("UPDATE %s SET v = 'b' WHERE id = 1", r.tbl),
			fmt.Sprintf("UPDATE %s SET v = 'c' WHERE id = 1", r.tbl),
		)
	})
	// Keep the INSERT and one UPDATE, as a transaction cut in parts leaves it.
	if _, err := r.index.Exec("DELETE FROM binlog_events WHERE table_name = ? AND event_type = 2 ORDER BY event_id DESC LIMIT 1", r.tbl); err != nil {
		t.Fatal(err)
	}
	stop := r.start()
	r.insert(2)
	r.waitSavedAt(2)
	stop()
	if got := r.copies(); got != "1:3 2:1" {
		t.Errorf("after resuming the index holds %q, want 1:3 2:1", got)
	}
}

// A table that left the filter between the runs is not sent again. Its rows are
// still in the index: nothing was deleted on the guess that they would return.
func TestIntegrationPGFirstRunKeepsATableThatLeftTheFilter_3114(t *testing.T) {
	r := newResumeRig(t, "lockfilter", 1000)
	other := r.addUncaptured()
	both := r.cfg
	both.Tables = "public." + r.tbl + ",public." + other
	r.firstRunLosesLock(both, func() {
		r.txn(
			fmt.Sprintf("INSERT INTO %s VALUES (1, 'a')", r.tbl),
			fmt.Sprintf("INSERT INTO %s VALUES (1, 'a')", other),
		)
	})
	stop := r.start() // captures r.tbl alone
	r.insert(2)
	r.waitSavedAt(2)
	stop()
	if got := r.copies(); got != once(1, 2) {
		t.Errorf("the captured table holds %q, want %q", got, once(1, 2))
	}
	if got := r.copiesOf(other); got != once(1) {
		t.Errorf("the table that left the filter holds %q, want %q", got, once(1))
	}
}

// A TRUNCATE recorded by the run that stopped before its first save is not
// recorded a second time when PostgreSQL sends it again.
func TestIntegrationPGFirstRunRecordsATruncateOnce_3114(t *testing.T) {
	r := newResumeRig(t, "locktrunc", 1000)
	r.insert(1)
	r.firstRunLosesLock(r.cfg, func() {
		r.txn(
			fmt.Sprintf("TRUNCATE %s", r.tbl),
			fmt.Sprintf("INSERT INTO %s VALUES (2, 'b')", r.tbl),
		)
	})
	stop := r.start()
	r.insert(3)
	r.waitSavedAt(3)
	stop()
	var n int
	if err := r.index.QueryRow("SELECT COUNT(*) FROM schema_changes WHERE table_name = ? AND ddl_type = 'TRUNCATE TABLE'", r.tbl).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("the TRUNCATE is recorded %d times, want 1", n)
	}
	if got := r.copies(); got != once(2, 3) {
		t.Errorf("the index holds %q, want %q", got, once(2, 3))
	}
}

// #3114 over real replication: another run claims the capture while this one
// runs, as a run that started after it would. This one's next save is refused,
// so it stops with that error, the position stays where it was, the slot is not
// confirmed past it, and the run after indexes every row once.
func TestIntegrationPGCaptureTakenOverSavesNothing_3114(t *testing.T) {
	r := newResumeRig(t, "takeover", 1000)
	_, ended := r.startRun()
	r.insert(1)
	at := r.waitSavedAt(1)
	if _, err := r.index.Exec(`UPDATE pg_capture_owner SET owner = REPEAT('f', 32) WHERE id = 1`); err != nil {
		t.Fatal(err)
	}
	r.insert(2)
	select {
	case err := <-ended:
		if !errors.Is(err, pgstreamrun.ErrCaptureOwnerChangedForTest) {
			t.Errorf("One returned %v, want the owner error", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("One kept running after another run claimed the capture")
	}
	waitFor(t, 15*time.Second, func() bool { return !r.slotActive() }, "replication slot inactive")
	if got := r.saved(); got != at {
		t.Errorf("the saved position moved to %X after another run claimed the capture, want %X", got, at)
	}
	if got := r.confirmedFlush(); uint64(got) > at {
		t.Errorf("the slot was confirmed through %s, past the saved position %s", got, pglogrepl.LSN(at))
	}
	stop := r.start()
	r.insert(3)
	r.waitSavedAt(3)
	stop()
	if got := r.copies(); got != once(1, 2, 3) {
		t.Errorf("after resuming the index holds %q, want %q", got, once(1, 2, 3))
	}
}
