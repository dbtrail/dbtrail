//go:build integration

package console

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/testutil"
)

// oneAggregate is the pre-partition statement over the same window: the
// reference the partitioned count must equal.
func oneAggregate(t *testing.T, db *sql.DB, since, until time.Time) (total, deletes int64) {
	t.Helper()
	q, args := buildActivitySQL(since, until, nil, nil)
	rows, err := db.Query(q, args...)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var s, tb string
		var et uint8
		var n int64
		if err := rows.Scan(&s, &tb, &et, &n); err != nil {
			t.Fatal(err)
		}
		total += n
		if et == 3 {
			deletes += n
		}
	}
	return total, deletes
}

// TestIntegrationActivityPartitionedMatchesOneAggregate runs the partitioned,
// kept-counts scan against real MySQL through three changes — a first count,
// a backlog write into an older hour, and a resume-dedup-shaped delete of the
// newest rows — and checks each result against the single statement it
// replaced.
func TestIntegrationActivityPartitionedMatchesOneAggregate(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	h := time.Now().UTC().Truncate(time.Hour)
	hours := []time.Time{h.Add(-3 * time.Hour), h.Add(-2 * time.Hour), h.Add(-time.Hour), h}
	testutil.SetupPartitionedTable(t, db, dbName, hours)

	ts := func(d time.Duration) string { return h.Add(d).Format("2006-01-02 15:04:05") }
	pos := uint64(4)
	ins := func(at string, table string, et uint8) {
		testutil.InsertEvent(t, db, "bin.000001", pos, pos+10, at, nil, "app", table, et, "1", nil, nil, nil)
		pos += 10
	}
	ins(ts(-3*time.Hour+time.Minute), "users", 1)
	ins(ts(-2*time.Hour+time.Minute), "users", 3)
	ins(ts(-2*time.Hour+2*time.Minute), "orders", 2)
	ins(ts(-time.Hour+time.Minute), "orders", 3)
	ins(ts(0), "orders", 1) // the open hour (if now is past its first second)

	store := newActivityPartStore()
	check := func(step string) activityResponse {
		t.Helper()
		got, err := computeActivity(context.Background(), db, activityScan{dbName: dbName, store: store, storeBudget: -1})
		if err != nil {
			t.Fatalf("%s: %v", step, err)
		}
		since, _ := time.Parse(consoleTSFormat, got.Since)
		until, _ := time.Parse(consoleTSFormat, got.Until)
		wantTotal, wantDeletes := oneAggregate(t, db, since, until)
		if got.Total != wantTotal || got.Deletes != wantDeletes {
			t.Errorf("%s: partitioned total %d deletes %d, one aggregate %d / %d", step, got.Total, got.Deletes, wantTotal, wantDeletes)
		}
		return got
	}
	first := check("first count")
	if first.Since != hours[0].Format(consoleTSFormat) {
		t.Errorf("since = %s, want %s", first.Since, hours[0].Format(consoleTSFormat))
	}
	if len(store.parts) != 3 {
		t.Errorf("kept %d partitions, want the 3 closed hours", len(store.parts))
	}

	// Backlog catch-up writes into an hour that was already counted.
	ins(ts(-2*time.Hour+30*time.Minute), "users", 3)
	if got := check("after a backlog write"); got.Deletes != first.Deletes+1 {
		t.Errorf("deletes = %d, want %d: the backlog row in the older hour was missed", got.Deletes, first.Deletes+1)
	}

	// The resume-time dedup deletes the rows written after the checkpoint:
	// the newest ids, here the backlog row just written.
	testutil.MustExec(t, db, "DELETE FROM binlog_events WHERE event_id >= (SELECT m FROM (SELECT MAX(event_id) AS m FROM binlog_events) x)")
	check("after a dedup delete")
}

// TestIntegrationActivityFingerprintIsAnIndexLookup pins that the per-refresh
// fingerprint costs an index lookup, not a scan of the partition.
func TestIntegrationActivityFingerprintIsAnIndexLookup(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	h := time.Now().UTC().Truncate(time.Hour)
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{h.Add(-time.Hour)})
	testutil.InsertEvent(t, db, "bin.000001", 4, 14, h.Add(-30*time.Minute).Format("2006-01-02 15:04:05"), nil,
		"app", "users", 1, "1", nil, nil, nil)

	rows, err := db.Query("EXPLAIN SELECT 0 AS i, MAX(event_id) AS m FROM binlog_events PARTITION (" +
		quotePartition(h.Add(-time.Hour).Format("p_2006010215")) + ")")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	cols, _ := rows.Columns()
	found := false
	for rows.Next() {
		vals := make([]sql.NullString, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatal(err)
		}
		for i, c := range cols {
			if c == "Extra" && strings.Contains(vals[i].String, "Select tables optimized away") {
				found = true
			}
		}
	}
	if !found {
		t.Error("the fingerprint is not answered from the index (no \"Select tables optimized away\")")
	}
}

// TestIntegrationActivityLockOutlivesCancelledStatement is the double scan
// seen live: the client gives up on a statement (go-sql-driver closes the
// socket, no KILL QUERY), the server keeps running it, and the next count must
// not start beside it. The lock is the server session's, so it is still held,
// and a count reports busy; the statement's own MAX_EXECUTION_TIME then ends
// it and frees the lock.
func TestIntegrationActivityLockOutlivesCancelledStatement(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	release, err := acquireActivityLock(context.Background(), conn, dbName, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer release()

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	// A real statement, not SLEEP: a CPU loop the server stops itself at 4 s.
	_, qerr := conn.QueryContext(ctx, "SELECT /*+ MAX_EXECUTION_TIME(4000) */ BENCHMARK(5000000000, SHA2('x', 256))")
	if qerr == nil {
		t.Fatal("the statement finished before the client gave up; make it longer")
	}

	name := activityLockName(dbName)
	var holder sql.NullInt64
	if err := db.QueryRow("SELECT IS_USED_LOCK(?)", name).Scan(&holder); err != nil {
		t.Fatal(err)
	}
	if !holder.Valid {
		t.Fatal("the lock was freed when the client gave up, while the statement still runs on the server")
	}
	_, err = computeActivity(context.Background(), db, activityScan{dbName: dbName})
	if !errors.Is(err, errActivityBusy) {
		t.Fatalf("a second count started beside the running statement: err = %v", err)
	}
	if strings.Contains(err.Error(), "KILL") {
		t.Errorf("KILL advised for a session that is running a statement: %v", err)
	}

	deadline := time.Now().Add(20 * time.Second)
	for {
		if err := db.QueryRow("SELECT IS_USED_LOCK(?)", name).Scan(&holder); err != nil {
			t.Fatal(err)
		}
		if !holder.Valid {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the orphaned statement still holds the lock 20 s later; MAX_EXECUTION_TIME did not end it")
		}
		time.Sleep(200 * time.Millisecond)
	}
	if _, err := computeActivity(context.Background(), db, activityScan{dbName: dbName}); err != nil {
		t.Errorf("count after the orphan ended: %v", err)
	}
}

// TestIntegrationActivityLockIdleHolderAndWait: an IDLE session holding the
// lock (a console host that vanished) gets the KILL advice; and a count that
// waits for the lock runs once a healthy holder lets go, instead of failing.
func TestIntegrationActivityLockIdleHolderAndWait(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	release, err := acquireActivityLock(context.Background(), conn, dbName, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, err = computeActivity(context.Background(), db, activityScan{dbName: dbName})
	if !errors.Is(err, errActivityBusy) || !strings.Contains(err.Error(), "KILL ") {
		t.Fatalf("idle holder: err = %v, want busy with the KILL advice", err)
	}
	go func() { time.Sleep(time.Second); release() }()
	start := time.Now()
	if _, err := computeActivity(context.Background(), db, activityScan{dbName: dbName, lockWait: 5 * time.Second}); err != nil {
		t.Fatalf("waiting count: %v", err)
	}
	if d := time.Since(start); d < 500*time.Millisecond {
		t.Errorf("the count ran after %v, before the holder let go", d)
	}
}
