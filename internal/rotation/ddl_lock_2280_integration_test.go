//go:build integration

package rotation

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/testutil"
)

// shortDDLLock makes the lock attempts quick: 1 s waits (lock_wait_timeout
// is whole seconds), the given number of attempts, a short pause.
func shortDDLLock(t *testing.T, attempts int) {
	t.Helper()
	w, a, p := ddlLockWait, ddlLockAttempts, ddlLockPause
	ddlLockWait, ddlLockAttempts, ddlLockPause = time.Second, attempts, 300*time.Millisecond
	t.Cleanup(func() { ddlLockWait, ddlLockAttempts, ddlLockPause = w, a, p })
}

// holdRead opens a transaction that has read binlog_events and keeps it open:
// what a long statement over the table is to an ALTER. The returned func ends it.
func holdRead(t *testing.T, db *sql.DB, dbName string) func() {
	t.Helper()
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	var n int
	if err := tx.QueryRow("SELECT COUNT(*) FROM `" + dbName + "`.binlog_events").Scan(&n); err != nil {
		t.Fatal(err)
	}
	var once sync.Once
	end := func() { once.Do(func() { _ = tx.Rollback() }) }
	t.Cleanup(end)
	return end
}

func partitionList(t *testing.T, db *sql.DB, dbName string) string {
	t.Helper()
	parts, err := listPartitions(context.Background(), db, dbName)
	if err != nil {
		t.Fatal(err)
	}
	names := make([]string, len(parts))
	for i, p := range parts {
		names[i] = p.Name
	}
	return strings.Join(names, ",")
}

// waitForALTERWaiting returns once an ALTER is waiting for the table, so what
// the caller does next arrives behind it. It only reports: it runs in
// goroutines.
func waitForALTERWaiting(t *testing.T, db *sql.DB) {
	for range 200 {
		var n int
		err := db.QueryRow("SELECT COUNT(*) FROM information_schema.PROCESSLIST WHERE STATE = 'Waiting for table metadata lock' AND INFO LIKE 'ALTER TABLE%'").Scan(&n)
		if err == nil && n > 0 {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Error("no ALTER came to wait for the table within 5 s")
}

// rotationTable is an index whose binlog_events has one hour two days old
// and the current hour.
func rotationTable(t *testing.T) (*sql.DB, string, time.Time) {
	t.Helper()
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	now := time.Now().UTC().Truncate(time.Hour)
	old := now.Add(-72 * time.Hour)
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{old, now})
	return db, dbName, old
}

// #2280: while a read of binlog_events is running, a rotation ALTER must not
// wait in front of capture. It gives up within its lock wait, a write to the
// table that arrives meanwhile goes through in about that long, and nothing
// is changed.
func TestAlterBinlogEvents_givesUpInsteadOfHoldingUpWrites(t *testing.T) {
	db, dbName, old := rotationTable(t)
	shortDDLLock(t, 2)
	end := holdRead(t, db, dbName)
	defer end()
	before := partitionList(t, db, dbName)

	// A write that arrives while the ALTER is waiting: capture's INSERT.
	wrote := make(chan time.Duration, 1)
	writeErr := make(chan error, 1)
	go func() {
		waitForALTERWaiting(t, db)
		start := time.Now()
		_, err := db.Exec("INSERT INTO `"+dbName+"`.binlog_events (binlog_file, start_pos, end_pos, event_timestamp, schema_name, table_name, event_type, pk_values) VALUES ('binlog.000001', 1, 2, ?, 's', 't', 1, '1')",
			time.Now().UTC().Format("2006-01-02 15:04:05"))
		writeErr <- err
		wrote <- time.Since(start)
	}()

	start := time.Now()
	err := dropPartitions(context.Background(), db, dbName, []string{old.Format("p_2006010215")}, nil)
	took := time.Since(start)
	if !errors.Is(err, errTableBusy) {
		t.Fatalf("dropPartitions under a running read: err = %v, want errTableBusy", err)
	}
	// Two attempts of 1 s and one pause; the ceiling leaves room for a slow host.
	if took < 2*time.Second || took > 6*time.Second {
		t.Errorf("gave up after %s, want about 2.3 s (two 1 s attempts and a pause)", took)
	}
	select {
	case d := <-wrote:
		if err := <-writeErr; err != nil {
			t.Errorf("the write failed: %v", err)
		}
		if d > 2500*time.Millisecond {
			t.Errorf("a write that arrived behind the ALTER waited %s, want no more than one lock wait (1 s) and some margin", d)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the write never went through")
	}
	if after := partitionList(t, db, dbName); after != before {
		t.Errorf("partitions changed although the statement gave up: %s -> %s", before, after)
	}
}

// A table that frees up between attempts is altered on the next one.
func TestAlterBinlogEvents_retriesUntilTheReadEnds(t *testing.T) {
	db, dbName, old := rotationTable(t)
	shortDDLLock(t, 5)
	end := holdRead(t, db, dbName)
	time.AfterFunc(1500*time.Millisecond, end) // during the second attempt
	name := old.Format("p_2006010215")
	if err := dropPartitions(context.Background(), db, dbName, []string{name}, nil); err != nil {
		t.Fatalf("dropPartitions: %v, want the drop to go through once the read ended", err)
	}
	if got := partitionList(t, db, dbName); strings.Contains(got, name) {
		t.Errorf("partitions = %s, want %s dropped", got, name)
	}
}

// The short lock wait is the ALTER's own: no connection of the pool keeps it.
func TestAlterBinlogEvents_doesNotLeaveItsLockWaitInThePool(t *testing.T) {
	db, dbName, old := rotationTable(t)
	db.SetMaxOpenConns(1) // the next statement gets the same connection if it was kept
	shortDDLLock(t, 1)
	var global int
	if err := db.QueryRow("SELECT @@GLOBAL.lock_wait_timeout").Scan(&global); err != nil {
		t.Fatal(err)
	}
	if err := dropPartitions(context.Background(), db, dbName, []string{old.Format("p_2006010215")}, nil); err != nil {
		t.Fatal(err)
	}
	var session int
	if err := db.QueryRow("SELECT @@SESSION.lock_wait_timeout").Scan(&session); err != nil {
		t.Fatal(err)
	}
	if session != global {
		t.Errorf("a pooled connection has lock_wait_timeout = %d after the ALTER, want the server's %d", session, global)
	}
}

// An error that is not a lock wait is returned as it is, at once.
func TestAlterBinlogEvents_otherErrorsAreNotRetried(t *testing.T) {
	db, dbName, _ := rotationTable(t)
	shortDDLLock(t, 5)
	start := time.Now()
	err := dropPartitions(context.Background(), db, dbName, []string{"p_1999010100"}, nil)
	if err == nil || errors.Is(err, errTableBusy) {
		t.Fatalf("dropping a partition that does not exist: err = %v, want MySQL's own error", err)
	}
	if took := time.Since(start); took > time.Second {
		t.Errorf("took %s: an error that is not a lock wait was retried", took)
	}
}

// A context that ends between attempts stops the retries.
func TestAlterBinlogEvents_stopsWhenTheContextEnds(t *testing.T) {
	db, dbName, old := rotationTable(t)
	shortDDLLock(t, 50)
	ddlLockPause = 5 * time.Second
	end := holdRead(t, db, dbName)
	defer end()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	start := time.Now()
	err := dropPartitions(ctx, db, dbName, []string{old.Format("p_2006010215")}, nil)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want the context's", err)
	}
	if took := time.Since(start); took > 4*time.Second {
		t.Errorf("returned after %s, want it at the context's 2 s", took)
	}
}

// A whole cycle over a table that stays busy: no error, the expired
// partition is counted as deferred and still there, no partition is added,
// and the next cycle, with the table free, does both.
func TestPerform_busyTableWaitsForTheNextCycle(t *testing.T) {
	db, dbName, old := rotationTable(t)
	shortDDLLock(t, 2)
	end := holdRead(t, db, dbName)
	opts := Options{RetainDur: 48 * time.Hour, RetainRaw: "48h", AddFuture: 3, Format: "json"}
	name := old.Format("p_2006010215")

	start := time.Now()
	res, err := Perform(context.Background(), db, dbName, opts)
	if err != nil {
		t.Fatalf("Perform over a busy table: %v, want no error", err)
	}
	// Deferred counts the partition and the add, so a table busy cycle
	// after cycle reaches the loop's escalation either way.
	if res.Dropped != 0 || res.Added != 0 || res.Deferred != 2 || res.AddSkipped < 3 {
		t.Errorf("busy cycle = %+v, want nothing dropped or added, 2 deferred and the add reported skipped", res)
	}
	// The add does not ask for the table again after the drop gave up:
	// one round of two 1 s attempts, not two.
	if took := time.Since(start); took > 3500*time.Millisecond {
		t.Errorf("busy cycle took %s, want one round of attempts (about 2.3 s)", took)
	}
	if got := partitionList(t, db, dbName); !strings.Contains(got, name) {
		t.Fatalf("partitions = %s, want %s still there", got, name)
	}

	end()
	res, err = Perform(context.Background(), db, dbName, opts)
	if err != nil {
		t.Fatal(err)
	}
	if res.Dropped != 1 || res.Added < 3 || res.Deferred != 0 {
		t.Errorf("free cycle = %+v, want the partition dropped and at least 3 added", res)
	}
	if got := partitionList(t, db, dbName); strings.Contains(got, name) {
		t.Errorf("partitions = %s, want %s dropped", got, fmt.Sprint(name))
	}
}

// The same in the branch that archives a partition and then drops it: the
// drop that finds the table busy is left for the next cycle, which drops the
// partition it finds already archived.
func TestPerform_busyTableAfterArchivingWaitsForTheNextCycle(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	old := time.Now().UTC().Add(-48 * time.Hour).Truncate(time.Hour)
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{old, old.Add(time.Hour)})
	for i, at := range []time.Time{old.Add(30 * time.Minute), old.Add(90 * time.Minute)} {
		testutil.InsertEvent(t, db, "binlog.000001", uint64(100+i*100), uint64(200+i*100), at.Format("2006-01-02 15:04:05"),
			nil, "testdb", "users", 1, fmt.Sprint(i), nil, nil, []byte(`{"id":1}`))
	}
	shortDDLLock(t, 2)
	end := holdRead(t, db, dbName)
	opts := Options{RetainDur: 24 * time.Hour, ArchiveDir: t.TempDir(), BintrailID: "test-uuid-2280",
		ArchiveCompression: "zstd", Format: "json", NoReplace: true}
	name := old.Format("p_2006010215")

	start := time.Now()
	res, err := Perform(context.Background(), db, dbName, opts)
	if err != nil {
		t.Fatalf("Perform over a busy table: %v, want no error", err)
	}
	// Both partitions wait, and the second is not tried: one round of
	// attempts for the cycle, however many partitions are due.
	if res.Dropped != 0 || res.Deferred != 2 {
		t.Errorf("busy cycle = %+v, want nothing dropped and 2 deferred", res)
	}
	if took := time.Since(start); took > 3500*time.Millisecond {
		t.Errorf("busy cycle took %s, want one round of attempts (about 2.3 s), not one per partition", took)
	}
	if got := partitionList(t, db, dbName); !strings.Contains(got, name) {
		t.Fatalf("partitions = %s, want %s still there", got, name)
	}

	end()
	res, err = Perform(context.Background(), db, dbName, opts)
	if err != nil {
		t.Fatal(err)
	}
	if res.Dropped != 2 || res.Deferred != 0 {
		t.Errorf("free cycle = %+v, want both partitions dropped", res)
	}
	if got := partitionList(t, db, dbName); strings.Contains(got, name) {
		t.Errorf("partitions = %s, want %s dropped", got, name)
	}
}

// A caller's check runs again before every attempt after the first, and its
// refusal ends the retries: the pause between attempts lets other statements
// change what the caller checked.
func TestAlterBinlogEvents_checksAgainBeforeEachRetry(t *testing.T) {
	db, dbName, old := rotationTable(t)
	shortDDLLock(t, 5)
	name := old.Format("p_2006010215")
	refused := errors.New("no longer safe")
	var calls atomic.Int32
	check := func(context.Context) error { calls.Add(1); return refused }

	end := holdRead(t, db, dbName)
	start := time.Now()
	err := dropPartitions(context.Background(), db, dbName, []string{name}, check)
	if !errors.Is(err, refused) {
		t.Fatalf("err = %v, want the check's own error", err)
	}
	if took := time.Since(start); calls.Load() != 1 || took > 3*time.Second {
		t.Errorf("check ran %d time(s) and the call took %s, want it to stop at the first retry (about 1.3 s)", calls.Load(), took)
	}
	if got := partitionList(t, db, dbName); !strings.Contains(got, name) {
		t.Fatalf("partitions = %s, want %s still there", got, name)
	}

	// The first attempt follows the caller's own check: it is not repeated.
	end()
	calls.Store(0)
	if err := dropPartitions(context.Background(), db, dbName, []string{name}, check); err != nil || calls.Load() != 0 {
		t.Errorf("free table: err = %v after %d check(s), want the drop with no check", err, calls.Load())
	}
}

// A row that lands in an archived partition while its drop waits for the
// table is not in the archive: the drop is left, not retried into deleting it.
func TestPerform_rowArrivingWhileTheDropWaitsStopsTheDrop(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	old := time.Now().UTC().Add(-48 * time.Hour).Truncate(time.Hour)
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{old})
	at := old.Add(30 * time.Minute).Format("2006-01-02 15:04:05")
	testutil.InsertEvent(t, db, "binlog.000001", 100, 200, at, nil, "testdb", "users", 1, "1", nil, nil, []byte(`{"id":1}`))
	shortDDLLock(t, 5)
	end := holdRead(t, db, dbName)
	name := old.Format("p_2006010215")
	opts := Options{RetainDur: 24 * time.Hour, ArchiveDir: t.TempDir(), BintrailID: "test-uuid-2280b",
		ArchiveCompression: "zstd", Format: "json", NoReplace: true}

	inserted := make(chan error, 1)
	go func() {
		waitForALTERWaiting(t, db)
		_, err := db.Exec("INSERT INTO `"+dbName+"`.binlog_events (binlog_file, start_pos, end_pos, event_timestamp, schema_name, table_name, event_type, pk_values) VALUES ('binlog.000001', 300, 400, ?, 'testdb', 'users', 1, '2')", at)
		inserted <- err
	}()

	start := time.Now()
	res, err := Perform(context.Background(), db, dbName, opts)
	if err != nil {
		t.Fatalf("Perform: %v, want no error", err)
	}
	if err := <-inserted; err != nil {
		t.Fatalf("the late row was not written: %v", err)
	}
	// Five attempts on a table that stays busy would take over 6 s.
	if took := time.Since(start); took > 4*time.Second {
		t.Errorf("cycle took %s, want it to stop at the first retry", took)
	}
	if res.Dropped != 0 || res.Deferred != 1 {
		t.Errorf("cycle = %+v, want nothing dropped and 1 deferred", res)
	}
	end()
	var rows int
	if got := partitionList(t, db, dbName); !strings.Contains(got, name) {
		t.Fatalf("partitions = %s, want %s still there", got, name)
	}
	if err := db.QueryRow("SELECT COUNT(*) FROM `" + dbName + "`.binlog_events").Scan(&rows); err != nil || rows != 2 {
		t.Fatalf("rows in the index = %d (%v), want both: the late one is in no archive", rows, err)
	}
}

// Nothing to drop and the add finds the table busy: counted as deferred and
// reported, so it cannot repeat unnoticed until rows land in p_future.
func TestPerform_busyAddIsCountedAndReported(t *testing.T) {
	db, dbName, _ := rotationTable(t)
	shortDDLLock(t, 2)
	end := holdRead(t, db, dbName)
	opts := Options{AddFuture: 3, Format: "json"}

	res, err := Perform(context.Background(), db, dbName, opts)
	if err != nil {
		t.Fatalf("Perform over a busy table: %v, want no error", err)
	}
	if res.Added != 0 || res.Deferred != 1 || res.AddSkipped != 3 || res.AddFutureTarget != 3 {
		t.Errorf("busy cycle = %+v, want 0 added, 1 deferred, 3 skipped with target 3", res)
	}
	end()
	res, err = Perform(context.Background(), db, dbName, opts)
	if err != nil {
		t.Fatal(err)
	}
	if res.Added != 3 || res.Deferred != 0 || res.AddSkipped != 0 {
		t.Errorf("free cycle = %+v, want 3 added and nothing skipped", res)
	}
}
