//go:build integration

package rotation

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
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
	done := false
	end := func() {
		if !done {
			done = true
			_ = tx.Rollback()
		}
	}
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
	go func() {
		time.Sleep(300 * time.Millisecond) // the ALTER is waiting by now
		start := time.Now()
		_, err := db.Exec("INSERT INTO `"+dbName+"`.binlog_events (binlog_file, start_pos, end_pos, event_timestamp, schema_name, table_name, event_type, pk_values) VALUES ('binlog.000001', 1, 2, ?, 's', 't', 1, '1')",
			time.Now().UTC().Format("2006-01-02 15:04:05"))
		if err != nil {
			t.Errorf("the write failed: %v", err)
		}
		wrote <- time.Since(start)
	}()

	start := time.Now()
	err := dropPartitions(context.Background(), db, dbName, []string{old.Format("p_2006010215")})
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
	if err := dropPartitions(context.Background(), db, dbName, []string{name}); err != nil {
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
	if err := dropPartitions(context.Background(), db, dbName, []string{old.Format("p_2006010215")}); err != nil {
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
	err := dropPartitions(context.Background(), db, dbName, []string{"p_1999010100"})
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
	err := dropPartitions(ctx, db, dbName, []string{old.Format("p_2006010215")})
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

	res, err := Perform(context.Background(), db, dbName, opts)
	if err != nil {
		t.Fatalf("Perform over a busy table: %v, want no error", err)
	}
	if res.Dropped != 0 || res.Added != 0 || res.Deferred != 1 {
		t.Errorf("busy cycle = %+v, want nothing dropped or added and 1 deferred", res)
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
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{old})
	testutil.InsertEvent(t, db, "binlog.000001", 100, 200, old.Add(30*time.Minute).Format("2006-01-02 15:04:05"),
		nil, "testdb", "users", 1, "1", nil, nil, []byte(`{"id":1}`))
	shortDDLLock(t, 2)
	end := holdRead(t, db, dbName)
	opts := Options{RetainDur: 24 * time.Hour, ArchiveDir: t.TempDir(), BintrailID: "test-uuid-2280",
		ArchiveCompression: "zstd", Format: "json", NoReplace: true}
	name := old.Format("p_2006010215")

	res, err := Perform(context.Background(), db, dbName, opts)
	if err != nil {
		t.Fatalf("Perform over a busy table: %v, want no error", err)
	}
	if res.Dropped != 0 || res.Deferred != 1 {
		t.Errorf("busy cycle = %+v, want nothing dropped and 1 deferred", res)
	}
	if got := partitionList(t, db, dbName); !strings.Contains(got, name) {
		t.Fatalf("partitions = %s, want %s still there", got, name)
	}

	end()
	res, err = Perform(context.Background(), db, dbName, opts)
	if err != nil {
		t.Fatal(err)
	}
	if res.Dropped != 1 || res.Deferred != 0 {
		t.Errorf("free cycle = %+v, want the partition dropped", res)
	}
	if got := partitionList(t, db, dbName); strings.Contains(got, name) {
		t.Errorf("partitions = %s, want %s dropped", got, name)
	}
}
