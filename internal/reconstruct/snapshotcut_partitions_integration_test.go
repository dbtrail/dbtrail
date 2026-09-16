//go:build integration

package reconstruct

import (
	"context"
	"database/sql"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/testutil"
)

// explainPartitions returns the `partitions` column of EXPLAIN for one
// statement, as MySQL prints it (comma-separated partition names).
func explainPartitions(t *testing.T, db *sql.DB, stmt string, args ...any) string {
	t.Helper()
	rows, err := db.Query("EXPLAIN "+stmt, args...)
	if err != nil {
		t.Fatalf("EXPLAIN: %v", err)
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		t.Fatalf("EXPLAIN columns: %v", err)
	}
	idx := slices.Index(cols, "partitions")
	if idx < 0 {
		t.Fatalf("EXPLAIN has no partitions column: %v", cols)
	}
	if !rows.Next() {
		t.Fatalf("EXPLAIN returned no rows: %v", rows.Err())
	}
	vals := make([]sql.NullString, len(cols))
	ptrs := make([]any, len(cols))
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	if err := rows.Scan(ptrs...); err != nil {
		t.Fatalf("EXPLAIN scan: %v", err)
	}
	return vals[idx].String
}

// TestResolveSnapshotCut_hourlyLayoutLeavesTheOldestPartitionOut is #1692
// against a real server: binlog_events partitioned by the hour, rows in the
// oldest partition (which also holds everything before its hour), and `at`
// inside the newest hour. The bound derived from a real
// information_schema listing names only that hour and p_future, MySQL's plan
// for the bounded statement does not open the oldest partition, and the cut
// is the first event past `at`.
func TestResolveSnapshotCut_hourlyLayoutLeavesTheOldestPartitionOut(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	ctx := context.Background()
	warns := captureWarns(t)

	h := time.Date(2026, 9, 16, 20, 0, 0, 0, time.UTC)
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{h.Add(-2 * time.Hour), h.Add(-time.Hour), h})
	oldest := "p_2026091618"

	ins := func(pos uint64, ts time.Time) {
		testutil.InsertEvent(t, db, "mysql-bin.000001", pos, pos+100,
			ts.Format("2006-01-02 15:04:05"), nil, "shop", "orders", 1, "1", nil, nil, []byte(`{"id":1}`))
	}
	ins(10, h.Add(-5*time.Hour))                // before the oldest partition's hour: lands in it all the same
	ins(20, h.Add(-2*time.Hour+10*time.Minute)) // inside the oldest partition's own hour
	ins(30, h.Add(-time.Hour+10*time.Minute))   // the middle partition
	ins(40, h.Add(3*time.Minute))               // at's hour, before at
	ins(50, h.Add(7*time.Minute))               // at's hour, after at: the cut
	at := h.Add(5*time.Minute + 27*time.Second)

	bound, err := listCutBound(ctx, db, at)
	if err != nil {
		t.Fatalf("listCutBound: %v", err)
	}
	if want := []string{"p_2026091620", "p_future"}; !slices.Equal(bound.keep, want) {
		t.Fatalf("bound.keep = %v, want %v", bound.keep, want)
	}

	bounded := explainPartitions(t, db, firstEventPastSQL(at, bound.clause()), at)
	if strings.Contains(bounded, oldest) || !strings.Contains(bounded, "p_2026091620") {
		t.Errorf("bounded plan opens partitions %q; want at's hour without %s", bounded, oldest)
	}
	// Documented, not asserted: MySQL's own pruning of the unbounded statement
	// kept the oldest partition on 8.4, which is what this bound exists for.
	t.Logf("unbounded plan opens partitions %q; bounded plan opens %q",
		explainPartitions(t, db, firstEventPastSQL(at, ""), at), bounded)

	cut, err := ResolveSnapshotCut(ctx, db, at)
	if err != nil {
		t.Fatalf("ResolveSnapshotCut: %v", err)
	}
	if cut == nil || cut.File != "mysql-bin.000001" || cut.Pos != 50 {
		t.Errorf("cut = %+v, want mysql-bin.000001:50, the first event past at", cut)
	}
	if warns.Len() != 0 {
		t.Errorf("the bounded path must not fall back on a real hourly layout, got: %s", warns.String())
	}

	// And the ordinary refresh case: nothing past at, so the cut is the newest
	// event's end, found without the fallback either.
	late := h.Add(30 * time.Minute)
	cut, err = ResolveSnapshotCut(ctx, db, late)
	if err != nil {
		t.Fatalf("ResolveSnapshotCut(nothing past at): %v", err)
	}
	if cut == nil || cut.Pos != 150 {
		t.Errorf("cut = %+v, want the newest event's end_pos 150", cut)
	}
	if warns.Len() != 0 {
		t.Errorf("unexpected warnings: %s", warns.String())
	}
}
