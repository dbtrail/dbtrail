package console

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

// scanNow is the fixed "now" of the partition tests: half past 19:00, so
// p_2026101819 is the open hour and 16..18 are closed.
var scanNow = time.Date(2026, 10, 18, 19, 30, 0, 0, time.UTC)

func hourAt(h int) time.Time { return time.Date(2026, 10, 18, h, 0, 0, 0, time.UTC) }

// countExp is one expected per-partition COUNT: its range and its rows.
type countExp struct {
	lo, hi time.Time
	rows   []aRow
	err    error
}

// expectPartitionRun is one whole partitioned count: lock, listing,
// fingerprints of the closed partitions (fps, in listing order; nil = no
// fingerprint statement), the counts in the order given, then release unless a
// count fails (the release still runs; it is expected after the failure).
func expectPartitionRun(mock sqlmock.Sqlmock, parts []string, fps []any, counts []countExp) {
	expectActivityLock(mock)
	mock.ExpectQuery("PARTITION_NAME, PARTITION_DESCRIPTION FROM information_schema.PARTITIONS").
		WillReturnRows(partitionsResult(parts...))
	if fps != nil {
		mock.ExpectQuery(`MAX\(event_id\)`).WillReturnRows(fingerprintResult(fps...))
	}
	for _, c := range counts {
		q := mock.ExpectQuery(`COUNT\(\*\) AS n FROM binlog_events PARTITION`).WithArgs(c.lo, c.hi)
		if c.err != nil {
			q.WillReturnError(c.err)
			break
		}
		q.WillReturnRows(activityResult(c.rows...))
	}
	expectActivityRelease(mock)
}

var fourHours = []string{"p_2026101816", "p_2026101817", "p_2026101818", "p_2026101819", "p_future"}

func scanAt(store *activityPartStore, now time.Time) activityScan {
	return activityScan{dbName: "idx", store: store, storeBudget: -1, now: now}
}

// TestActivityClosedPartitionCountsAreKept is the cost bound: the second
// refresh re-reads only the fingerprints and the open hour. Before this, every
// refresh scanned the whole live retention.
func TestActivityClosedPartitionCountsAreKept(t *testing.T) {
	db, mock, closeDB := newSQLMock(t)
	defer closeDB()
	store := newActivityPartStore()
	expectPartitionRun(mock, fourHours, []any{10, 20, 30}, []countExp{
		{hourAt(19), scanNow, []aRow{{"shop", "orders", 1, 5}}, nil},
		{hourAt(18), hourAt(19), []aRow{{"shop", "orders", 3, 3}}, nil},
		{hourAt(17), hourAt(18), []aRow{{"shop", "users", 2, 2}}, nil},
		{hourAt(16), hourAt(17), []aRow{{"shop", "users", 3, 1}}, nil},
	})
	later := scanNow.Add(10 * time.Minute)
	// Same fingerprints: only the open hour is counted again.
	expectPartitionRun(mock, fourHours, []any{10, 20, 30}, []countExp{
		{hourAt(19), later, []aRow{{"shop", "orders", 1, 9}}, nil},
	})

	first, err := computeActivity(context.Background(), db, scanAt(store, scanNow))
	if err != nil {
		t.Fatal(err)
	}
	if first.Total != 11 || first.Deletes != 4 || first.Tables != 2 {
		t.Fatalf("first = total %d deletes %d tables %d, want 11/4/2", first.Total, first.Deletes, first.Tables)
	}
	second, err := computeActivity(context.Background(), db, scanAt(store, later))
	if err != nil {
		t.Fatal(err)
	}
	if second.Total != 15 || second.Deletes != 4 || second.Inserts != 9 {
		t.Errorf("second = total %d deletes %d inserts %d, want 15/4/9 (kept hours + recounted open hour)",
			second.Total, second.Deletes, second.Inserts)
	}
	if second.Since != hourAt(16).Format(consoleTSFormat) {
		t.Errorf("since = %s, want the oldest partition hour", second.Since)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

// TestActivityFingerprintIsReadBeforeCounts pins the order the safety of the
// kept counts rests on: a row committed between a COUNT and a later MAX would
// be fingerprinted but never counted. The ordered mock fails if the
// fingerprint statement moves after any count.
func TestActivityFingerprintIsReadBeforeCounts(t *testing.T) {
	db, mock, closeDB := newSQLMock(t)
	defer closeDB()
	expectPartitionRun(mock, []string{"p_2026101818", "p_2026101819"}, []any{30}, []countExp{
		{hourAt(19), scanNow, nil, nil},
		{hourAt(18), hourAt(19), nil, nil},
	})
	if _, err := computeActivity(context.Background(), db, scanAt(newActivityPartStore(), scanNow)); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

// TestActivityBacklogWriteIntoOlderHourRecounts: a stream catching up writes
// into an hour that was already counted. Its MAX(event_id) moves, so that hour
// — and only that hour — is counted again.
func TestActivityBacklogWriteIntoOlderHourRecounts(t *testing.T) {
	db, mock, closeDB := newSQLMock(t)
	defer closeDB()
	store := newActivityPartStore()
	expectPartitionRun(mock, fourHours, []any{10, 20, 30}, []countExp{
		{hourAt(19), scanNow, nil, nil},
		{hourAt(18), hourAt(19), nil, nil},
		{hourAt(17), hourAt(18), []aRow{{"shop", "users", 3, 1}}, nil},
		{hourAt(16), hourAt(17), nil, nil},
	})
	expectPartitionRun(mock, fourHours, []any{10, 41, 30}, []countExp{
		{hourAt(19), scanNow, nil, nil},
		{hourAt(17), hourAt(18), []aRow{{"shop", "users", 3, 6}}, nil},
	})
	if _, err := computeActivity(context.Background(), db, scanAt(store, scanNow)); err != nil {
		t.Fatal(err)
	}
	got, err := computeActivity(context.Background(), db, scanAt(store, scanNow))
	if err != nil {
		t.Fatal(err)
	}
	if got.Deletes != 6 {
		t.Errorf("deletes = %d, want the recounted 6 — the backlog rows in the older hour were missed", got.Deletes)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

// TestActivityDedupDeleteRecounts: the resume-time dedup deletes the newest
// rows, so the partition's MAX goes DOWN (here to NULL: the hour emptied). A
// fingerprint compared only for growth would keep the deleted rows.
func TestActivityDedupDeleteRecounts(t *testing.T) {
	db, mock, closeDB := newSQLMock(t)
	defer closeDB()
	store := newActivityPartStore()
	parts := []string{"p_2026101818", "p_2026101819"}
	expectPartitionRun(mock, parts, []any{30}, []countExp{
		{hourAt(19), scanNow, nil, nil},
		{hourAt(18), hourAt(19), []aRow{{"shop", "users", 3, 4}}, nil},
	})
	expectPartitionRun(mock, parts, []any{nil}, []countExp{
		{hourAt(19), scanNow, nil, nil},
		{hourAt(18), hourAt(19), nil, nil},
	})
	if _, err := computeActivity(context.Background(), db, scanAt(store, scanNow)); err != nil {
		t.Fatal(err)
	}
	got, err := computeActivity(context.Background(), db, scanAt(store, scanNow))
	if err != nil {
		t.Fatal(err)
	}
	if got.Deletes != 0 {
		t.Errorf("deletes = %d, want 0 after the hour's rows were deleted", got.Deletes)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

// TestActivityRotationDropsOldestPartitionEvictsItsCounts: rotation drops the
// oldest hour; its kept counts must leave the total, and the window moves.
func TestActivityRotationDropsOldestPartitionEvictsItsCounts(t *testing.T) {
	db, mock, closeDB := newSQLMock(t)
	defer closeDB()
	store := newActivityPartStore()
	expectPartitionRun(mock, fourHours, []any{10, 20, 30}, []countExp{
		{hourAt(19), scanNow, nil, nil},
		{hourAt(18), hourAt(19), nil, nil},
		{hourAt(17), hourAt(18), nil, nil},
		{hourAt(16), hourAt(17), []aRow{{"shop", "old", 3, 7}}, nil},
	})
	// p_2026101816 is gone. p_2026101817 is now the first partition, so its
	// counted range is [since, 18:00) with since = 17:00 — the same range.
	expectPartitionRun(mock, fourHours[1:], []any{20, 30}, []countExp{
		{hourAt(19), scanNow, nil, nil},
	})
	if _, err := computeActivity(context.Background(), db, scanAt(store, scanNow)); err != nil {
		t.Fatal(err)
	}
	got, err := computeActivity(context.Background(), db, scanAt(store, scanNow))
	if err != nil {
		t.Fatal(err)
	}
	if got.Deletes != 0 || got.Tables != 0 {
		t.Errorf("deletes = %d tables = %d, want 0/0: the dropped hour's counts stayed in the total", got.Deletes, got.Tables)
	}
	if got.Since != hourAt(17).Format(consoleTSFormat) {
		t.Errorf("since = %s, want 17:00 after the drop", got.Since)
	}
	if _, kept := store.parts["p_2026101816"]; kept {
		t.Error("the dropped partition's counts are still kept")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

// TestActivityPartitionRecreatedAfterDropRecounts: a name that left the
// listing and came back is counted again even if its MAX looks the same.
func TestActivityPartitionRecreatedAfterDropRecounts(t *testing.T) {
	db, mock, closeDB := newSQLMock(t)
	defer closeDB()
	store := newActivityPartStore()
	two := []string{"p_2026101818", "p_2026101819"}
	expectPartitionRun(mock, two, []any{30}, []countExp{
		{hourAt(19), scanNow, nil, nil},
		{hourAt(18), hourAt(19), []aRow{{"a", "t", 3, 1}}, nil},
	})
	expectPartitionRun(mock, two[1:], nil, []countExp{
		{hourAt(19), scanNow, nil, nil},
	})
	expectPartitionRun(mock, two, []any{30}, []countExp{
		{hourAt(19), scanNow, nil, nil},
		{hourAt(18), hourAt(19), []aRow{{"a", "t", 3, 2}}, nil},
	})
	for range 3 {
		if _, err := computeActivity(context.Background(), db, scanAt(store, scanNow)); err != nil {
			t.Fatal(err)
		}
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

// TestActivityPFutureIsNeverKept: when capture outran partition creation,
// p_future holds rows of the window. It is counted (up to now) on every
// refresh and never kept: REORGANIZE moves its rows into new partitions.
func TestActivityPFutureIsNeverKept(t *testing.T) {
	db, mock, closeDB := newSQLMock(t)
	defer closeDB()
	store := newActivityPartStore()
	parts := []string{"p_2026101817", "p_future"} // p_future starts at 18:00 < now
	expectPartitionRun(mock, parts, []any{20}, []countExp{
		{hourAt(18), scanNow, []aRow{{"a", "t", 1, 3}}, nil},
		{hourAt(17), hourAt(18), nil, nil},
	})
	expectPartitionRun(mock, parts, []any{20}, []countExp{
		{hourAt(18), scanNow, []aRow{{"a", "t", 1, 4}}, nil},
	})
	if _, err := computeActivity(context.Background(), db, scanAt(store, scanNow)); err != nil {
		t.Fatal(err)
	}
	got, err := computeActivity(context.Background(), db, scanAt(store, scanNow))
	if err != nil {
		t.Fatal(err)
	}
	if got.Inserts != 4 {
		t.Errorf("inserts = %d, want p_future's recount 4", got.Inserts)
	}
	if _, kept := store.parts["p_future"]; kept {
		t.Error("p_future's counts were kept")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

// TestActivityEmptyIndex: every partition empty (MAX NULL). Zeros are real
// here — measured — and an empty hour stays kept (NULL equals NULL).
func TestActivityEmptyIndex(t *testing.T) {
	db, mock, closeDB := newSQLMock(t)
	defer closeDB()
	store := newActivityPartStore()
	parts := []string{"p_2026101818", "p_2026101819", "p_future"}
	expectPartitionRun(mock, parts, []any{nil}, []countExp{
		{hourAt(19), scanNow, nil, nil},
		{hourAt(18), hourAt(19), nil, nil},
	})
	expectPartitionRun(mock, parts, []any{nil}, []countExp{
		{hourAt(19), scanNow, nil, nil},
	})
	for range 2 {
		got, err := computeActivity(context.Background(), db, scanAt(store, scanNow))
		if err != nil {
			t.Fatal(err)
		}
		if got.Total != 0 || !got.Complete {
			t.Errorf("total = %d complete = %v, want a complete zero", got.Total, got.Complete)
		}
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

// TestActivityCountFailureKeepsCountedPartitions: a statement fails partway
// (the server's MAX_EXECUTION_TIME, say). The hours already counted are kept,
// so the next attempt resumes instead of starting over — the first count of a
// large index used to time out the same way forever.
func TestActivityCountFailureKeepsCountedPartitions(t *testing.T) {
	db, mock, closeDB := newSQLMock(t)
	defer closeDB()
	store := newActivityPartStore()
	boom := errors.New("Query execution was interrupted, maximum statement execution time exceeded")
	expectPartitionRun(mock, fourHours, []any{10, 20, 30}, []countExp{
		{hourAt(19), scanNow, nil, nil},
		{hourAt(18), hourAt(19), []aRow{{"a", "t", 3, 1}}, nil},
		{hourAt(17), hourAt(18), nil, boom},
	})
	expectPartitionRun(mock, fourHours, []any{10, 20, 30}, []countExp{
		{hourAt(19), scanNow, nil, nil},
		{hourAt(17), hourAt(18), []aRow{{"a", "t", 3, 2}}, nil},
		{hourAt(16), hourAt(17), []aRow{{"a", "t", 3, 4}}, nil},
	})
	if _, err := computeActivity(context.Background(), db, scanAt(store, scanNow)); err == nil || !strings.Contains(err.Error(), "p_2026101817") {
		t.Fatalf("err = %v, want the failure naming the partition", err)
	}
	got, err := computeActivity(context.Background(), db, scanAt(store, scanNow))
	if err != nil {
		t.Fatal(err)
	}
	if got.Deletes != 7 {
		t.Errorf("deletes = %d, want 1 (kept) + 2 + 4", got.Deletes)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

// TestActivityReverifyRecountsOldestKeptPartition is the backstop for what the
// fingerprint cannot see: kept counts older than activityReverifyAge are
// recounted, one partition per refresh, oldest first.
func TestActivityReverifyRecountsOldestKeptPartition(t *testing.T) {
	db, mock, closeDB := newSQLMock(t)
	defer closeDB()
	store := newActivityPartStore()
	parts := []string{"p_2026101817", "p_2026101818", "p_2026101819"}
	expectPartitionRun(mock, parts, []any{20, 30}, []countExp{
		{hourAt(19), scanNow, nil, nil},
		{hourAt(18), hourAt(19), nil, nil},
		{hourAt(17), hourAt(18), nil, nil},
	})
	if _, err := computeActivity(context.Background(), db, scanAt(store, scanNow)); err != nil {
		t.Fatal(err)
	}
	// 18:00's counts were taken a minute later than 17:00's, so 17:00 is the
	// oldest and the one recounted.
	store.parts["p_2026101818"].countedAt = scanNow.Add(time.Minute)
	later := scanNow.Add(activityReverifyAge + 2*time.Minute)
	// later is in the next hour: 19:00 is now closed too, and 20:00 does not
	// exist yet; keep the same listing, so 19:00 counts [19:00, 20:00).
	expectPartitionRun(mock, parts, []any{20, 30, 40}, []countExp{
		{hourAt(19), hourAt(20), nil, nil},
		{hourAt(17), hourAt(18), nil, nil},
	})
	if _, err := computeActivity(context.Background(), db, scanAt(store, later)); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

// TestActivityStoreBudgetIsRespected: with no budget left nothing is kept,
// and the counts are still right.
func TestActivityStoreBudgetIsRespected(t *testing.T) {
	db, mock, closeDB := newSQLMock(t)
	defer closeDB()
	store := newActivityPartStore()
	parts := []string{"p_2026101818", "p_2026101819"}
	expectPartitionRun(mock, parts, []any{30}, []countExp{
		{hourAt(19), scanNow, nil, nil},
		{hourAt(18), hourAt(19), []aRow{{"a", "t", 3, 1}}, nil},
	})
	sc := scanAt(store, scanNow)
	sc.storeBudget = 0
	got, err := computeActivity(context.Background(), db, sc)
	if err != nil {
		t.Fatal(err)
	}
	if got.Deletes != 1 || len(store.parts) != 0 {
		t.Errorf("deletes = %d kept = %d, want 1 and nothing kept", got.Deletes, len(store.parts))
	}
}

// TestActivityLockHeldElsewhereIsBusy: GET_LOCK answered 0 — another session
// (another console, or this one before a restart, whose statement still runs)
// holds the index's lock. Nothing else is read.
func TestActivityLockHeldElsewhereIsBusy(t *testing.T) {
	db, mock, closeDB := newSQLMock(t)
	defer closeDB()
	mock.ExpectQuery(`SELECT GET_LOCK\(\?, 0\)`).WithArgs(activityLockName("idx")).
		WillReturnRows(sqlmock.NewRows([]string{"l"}).AddRow(0))
	_, err := computeActivity(context.Background(), db, scanAt(nil, scanNow))
	if !errors.Is(err, errActivityBusy) {
		t.Fatalf("err = %v, want errActivityBusy", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

// TestActivityLockNameFitsMySQLLimit: MySQL refuses lock names over 64
// characters (error 3057), and a database name alone may be 64.
func TestActivityLockNameFitsMySQLLimit(t *testing.T) {
	long := strings.Repeat("d", 64)
	for _, name := range []string{"", "idx", long} {
		if n := len(activityLockName(name)); n > 64 {
			t.Errorf("lock name for %q is %d characters", name, n)
		}
	}
	if activityLockName("a") == activityLockName("b") || activityLockName(long) == activityLockName(long[:63]) {
		t.Error("two databases share one activity lock")
	}
}

// TestActivityStatementsCarryServerDeadline: the server, not the client, ends
// a statement that runs too long; and the partition name is quoted.
func TestActivityStatementsCarryServerDeadline(t *testing.T) {
	q, args := buildActivitySQLFor("p_x`y", 5*time.Minute, hourAt(18), hourAt(19), nil, nil)
	if !strings.HasPrefix(q, "SELECT /*+ MAX_EXECUTION_TIME(300000) */ schema_name") {
		t.Errorf("no server-side deadline: %s", q)
	}
	if !strings.Contains(q, "FROM binlog_events PARTITION (`p_x``y`) WHERE event_timestamp >= ? AND event_timestamp < ?") {
		t.Errorf("partition clause / time predicate wrong: %s", q)
	}
	if len(args) != 2 {
		t.Errorf("args = %v", args)
	}
	if q, _ := buildActivitySQL(hourAt(18), hourAt(19), nil, nil); strings.Contains(q, "MAX_EXECUTION_TIME") || strings.Contains(q, "PARTITION (") {
		t.Errorf("the plain builder grew a hint or partition clause: %s", q)
	}
}

// TestActivityPlanSkipsPartitionsOutsideTheWindow: hours after now are not
// read at all, and an open hour is counted up to now.
func TestActivityPlanSkipsPartitionsOutsideTheWindow(t *testing.T) {
	parts := []activityPartition{
		{name: "p_2026101818", loOpen: true, hi: hourAt(19)},
		{name: "p_2026101819", lo: hourAt(19), hi: hourAt(20)},
		{name: "p_2026101820", lo: hourAt(20), hi: hourAt(21)},
		{name: "p_future", lo: hourAt(21), hiOpen: true},
	}
	w := planActivity(parts, hourAt(18), scanNow)
	if len(w) != 2 {
		t.Fatalf("plan = %+v, want 18 (closed) and 19 (open)", w)
	}
	if !w[0].closed || !w[0].lo.Equal(hourAt(18)) || !w[0].hi.Equal(hourAt(19)) {
		t.Errorf("18:00 = %+v", w[0])
	}
	if w[1].closed || !w[1].hi.Equal(scanNow) {
		t.Errorf("19:00 = %+v, want open up to now", w[1])
	}
}

// --- the cache: one count at a time, and what a request gets meanwhile ---

// TestActivityTwoScopesNeverCountAtOnce: two RBAC scopes arriving together
// used to run two aggregates side by side on the index. They now queue.
func TestActivityTwoScopesNeverCountAtOnce(t *testing.T) {
	c := newActivityCache()
	var running, peak atomic.Int32
	compute := func(context.Context) (activityResponse, error) {
		n := running.Add(1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		time.Sleep(50 * time.Millisecond)
		running.Add(-1)
		return activityResponse{Deletes: 1}, nil
	}
	var wg sync.WaitGroup
	for _, key := range []string{"", "a", "b"} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := c.get(context.Background(), key, compute); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if peak.Load() != 1 {
		t.Errorf("%d counts ran at once on one server, want 1", peak.Load())
	}
}

// TestActivityRefreshStillRunningWhenTTLExpiresAgain: a slow refresh outlives
// the TTL; later requests keep getting the old value and start nothing new.
func TestActivityRefreshStillRunningWhenTTLExpiresAgain(t *testing.T) {
	c := newActivityCache()
	c.entries[""] = activityResponse{Deletes: 4, RefreshedAt: "old"}
	c.stamps[""] = time.Now().Add(-time.Hour)
	c.costs[""] = time.Minute
	release := make(chan struct{})
	var calls atomic.Int32
	compute := func(context.Context) (activityResponse, error) {
		calls.Add(1)
		<-release
		return activityResponse{Deletes: 9}, nil
	}
	for range 5 {
		got, err := c.get(context.Background(), "", compute)
		if err != nil || got.RefreshedAt != "old" {
			t.Fatalf("got %+v, %v; want the old value while the refresh runs", got, err)
		}
	}
	close(release)
	deadline := time.Now().Add(2 * time.Second)
	for {
		c.mu.Lock()
		f := c.flights[""]
		c.mu.Unlock()
		if f == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("flight never finished")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if calls.Load() != 1 {
		t.Errorf("%d refreshes started, want 1", calls.Load())
	}
}

// TestActivityFailedCountBacksOff: a failing count is not retried on every
// request (the page asks every few seconds), only after retryBackoff.
func TestActivityFailedCountBacksOff(t *testing.T) {
	c := newActivityCache()
	c.retryBackoff = 100 * time.Millisecond
	var calls atomic.Int32
	compute := func(context.Context) (activityResponse, error) {
		calls.Add(1)
		return activityResponse{}, errors.New("index went away")
	}
	_, err := c.get(context.Background(), "", compute)
	var unavail *activityUnavailableError
	if !errors.As(err, &unavail) || unavail.running || !strings.Contains(err.Error(), "index went away") {
		t.Fatalf("err = %v, want the failure with its cause", err)
	}
	for range 3 {
		if _, err := c.get(context.Background(), "", compute); !strings.Contains(fmtErr(err), "index went away") {
			t.Fatalf("err during backoff = %v", err)
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("%d counts during the backoff, want 1", calls.Load())
	}
	time.Sleep(120 * time.Millisecond)
	_, _ = c.get(context.Background(), "", compute)
	if calls.Load() != 2 {
		t.Errorf("%d counts after the backoff, want 2", calls.Load())
	}
}

func fmtErr(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// TestActivityFailedRefreshKeepsServingTheOldValue: warm cache, the refresh
// fails; the old value stays, with its refreshed_at.
func TestActivityFailedRefreshKeepsServingTheOldValue(t *testing.T) {
	c := newActivityCache()
	c.entries[""] = activityResponse{Deletes: 4, RefreshedAt: "old"}
	c.stamps[""] = time.Now().Add(-time.Hour)
	c.costs[""] = time.Minute
	compute := func(context.Context) (activityResponse, error) { return activityResponse{}, errors.New("boom") }
	for range 3 {
		got, err := c.get(context.Background(), "", compute)
		if err != nil || got.RefreshedAt != "old" {
			t.Fatalf("got %+v, %v", got, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestActivityEvictionDropsPartStore: a scope evicted from the cache takes its
// kept partition counts with it.
func TestActivityEvictionDropsPartStore(t *testing.T) {
	c := newActivityCache()
	for i := range activityMaxProfiles + 1 {
		k := strings.Repeat("k", i+1)
		c.entries[k] = activityResponse{}
		c.stamps[k] = time.Now().Add(time.Duration(i) * time.Second)
		c.parts[k] = newActivityPartStore()
	}
	c.evictOverCapLocked()
	if _, ok := c.parts["k"]; ok {
		t.Error("the evicted scope's partition counts are still kept")
	}
}

// --- through the handler: what the browser gets ---

// TestActivityColdRequestPastColdWaitAnswers503 is the first request on an
// empty cache whose count is slow: an answer that says the count is still
// running, instead of holding the request until the flight's deadline turns
// it into a bare 500.
func TestActivityColdRequestPastColdWaitAnswers503(t *testing.T) {
	db, mock, closeDB := newSQLMock(t)
	defer closeDB()
	mock.ExpectQuery(`SELECT GET_LOCK\(\?, 0\)`).WillDelayFor(300 * time.Millisecond).
		WillReturnRows(sqlmock.NewRows([]string{"l"}).AddRow(1))
	mock.ExpectQuery(`COUNT\(\*\) AS n FROM binlog_events`).WillReturnRows(activityResult())
	expectActivityRelease(mock)
	srv := newBootServer(db)
	srv.cm.boot.activity.coldWait = 20 * time.Millisecond

	code, _, body := getActivity(t, srv, "/api/activity")
	if code != http.StatusServiceUnavailable || !strings.Contains(body, "still being computed") {
		t.Fatalf("code = %d body = %s, want 503 saying the count is running", code, body)
	}
	// The flight finishes behind the answer; the next request gets the count.
	deadline := time.Now().Add(3 * time.Second)
	for {
		if code, _, _ := getActivity(t, srv, "/api/activity"); code == 200 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the count never landed")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestActivityColdFailureAnswers503WithCause: nothing cached and the count
// failed — the body names the cause and when the next attempt starts.
func TestActivityColdFailureAnswers503WithCause(t *testing.T) {
	db, mock, closeDB := newSQLMock(t)
	defer closeDB()
	expectActivityLock(mock)
	mock.ExpectQuery(`COUNT\(\*\) AS n FROM binlog_events`).WillReturnError(errors.New("disk full"))
	expectActivityRelease(mock)
	srv := newBootServer(db)

	rec, raw := doServersReq(t, srv, "GET", "/api/activity", "")
	body := string(raw)
	if rec.Code != http.StatusServiceUnavailable || !strings.Contains(body, "could not be computed") || !strings.Contains(body, "disk full") {
		t.Fatalf("code = %d body = %s", rec.Code, body)
	}
	if rec.Header().Get("Retry-After") == "" {
		t.Error("no Retry-After on a failed count")
	}
}

// TestActivityLockHeldElsewhere covers a restart (the old process's statement
// still running on the index) from the browser's side: cold → 503 saying so;
// warm → the old value, refreshed_at and all.
func TestActivityLockHeldElsewhere(t *testing.T) {
	db, mock, closeDB := newSQLMock(t)
	defer closeDB()
	busy := func() {
		mock.ExpectQuery(`SELECT GET_LOCK\(\?, 0\)`).WillReturnRows(sqlmock.NewRows([]string{"l"}).AddRow(0))
	}
	busy()
	srv := newBootServer(db)
	code, _, body := getActivity(t, srv, "/api/activity")
	if code != http.StatusServiceUnavailable || !strings.Contains(body, "another activity count is still running") {
		t.Fatalf("cold: code = %d body = %s", code, body)
	}

	c := srv.cm.boot.activity
	c.mu.Lock()
	c.entries[""] = activityResponse{Deletes: 4, RefreshedAt: "2026-10-18 19:00:00"}
	c.stamps[""] = time.Now().Add(-time.Hour)
	c.costs[""] = time.Minute
	delete(c.retryAt, "")
	c.mu.Unlock()
	busy()
	code, got, body := getActivity(t, srv, "/api/activity")
	if code != 200 || got.Deletes != 4 || got.RefreshedAt != "2026-10-18 19:00:00" {
		t.Fatalf("warm: code = %d got = %+v body = %s, want the old value", code, got, body)
	}
	// That refresh failed (busy), so the next request falls in the retry
	// backoff: still the old value, and no statement at all.
	deadline := time.Now().Add(2 * time.Second)
	for {
		c.mu.Lock()
		_, backoff := c.retryAt[""]
		c.mu.Unlock()
		if backoff {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the busy refresh never finished")
		}
		time.Sleep(5 * time.Millisecond)
	}
	code, got, body = getActivity(t, srv, "/api/activity")
	if code != 200 || got.Deletes != 4 {
		t.Fatalf("warm, in backoff: code = %d got = %+v body = %s, want the old value", code, got, body)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}
