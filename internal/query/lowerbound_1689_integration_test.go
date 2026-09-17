//go:build integration

package query

import (
	"context"
	"database/sql"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/testutil"
)

// #1689 against a real optimizer: with a baseline anchor set, the partitions
// below the window's floor must not be opened at all.
//
// The old lower bound, TO_SECONDS(event_timestamp) >= n, wraps the column, and a
// predicate on a FUNCTION of the partitioning column prunes nothing — measured
// identical on MySQL 8.0.46 and 8.4.9, four partitions opened where two can hold
// a row. The plain comparison that replaces it prunes correctly on both. That is
// a property of the PREDICATE, so it holds whichever index the optimizer picks,
// which is why it is the assertion here.
//
// The issue's other criterion — the index range bounded on both sides — is
// deliberately NOT pinned here. It only appears when the optimizer chooses
// idx_row_lookup, and on a fixture of this size it prefers idx_pk_hash, while
// the production EXPLAIN in #1689 shows idx_row_lookup. Pinning it would be
// pinning a cost estimate, and it would go red on an unrelated optimizer change.
// The predicate's form is pinned with no server at all by
// TestBuildQuery_sincePosLowerBoundIsIndexUsable, and the two-sided range was
// measured directly on both versions.
func TestBuildQuery_sincePosPruningHoldsWhicheverIndexIsChosen(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	ctx := context.Background()

	// Four consecutive hours, each needing its OWN partition for the pruning
	// half of this test to mean anything. The hours run forward from the current
	// one only so the fixture reads like a live index: SetupPartitionedTable
	// takes whatever hours it is handed, past included, so nothing here depends
	// on that direction.
	//
	// 40k rows across 8 tables, bulk-loaded: on a table of a few hundred rows the
	// optimizer reads everything and reports no index scan at all, and a plan
	// with no scan says nothing about the predicate under test.
	base := time.Now().UTC().Truncate(time.Hour)
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("conn: %v", err)
	}
	defer conn.Close()
	// testutil.InitIndexTables builds binlog_events with p_future ALONE, so the
	// hourly layout the real schema has is built here first: with one partition
	// there is nothing to prune, and this test could not fail however wrong the
	// predicate was.
	hours := make([]time.Time, 4)
	for h := range hours {
		hours[h] = base.Add(time.Duration(h) * time.Hour)
	}
	testutil.SetupPartitionedTable(t, db, dbName, hours)
	// Seeded through one dedicated connection because the recursion limit is a
	// SESSION variable and the pool would not carry it to the INSERT. The
	// assertions below deliberately go back through the pool: nothing they do
	// needs that setting, and pinning them to this connection would hide a
	// dependency on it if one ever crept in.
	if _, err := conn.ExecContext(ctx, "SET SESSION cte_max_recursion_depth = 200000"); err != nil {
		t.Fatalf("raise the CTE recursion limit: %v", err)
	}
	if _, err := conn.ExecContext(ctx, `INSERT INTO binlog_events
		  (binlog_file,start_pos,end_pos,event_timestamp,schema_name,table_name,event_type,pk_values,row_after)
		WITH RECURSIVE n(i) AS (SELECT 0 UNION ALL SELECT i+1 FROM n WHERE i < 39999)
		SELECT 'binlog.000001', 100+i*4, 104+i*4,
		       TIMESTAMPADD(SECOND, i MOD 14400, ?),
		       'shop', CONCAT('t', LPAD(i MOD 8, 2, '0')), 2, CONCAT(i),
		       JSON_OBJECT('id', i)
		FROM n`, base.Format("2006-01-02 15:04:05")); err != nil {
		t.Fatalf("seed events: %v", err)
	}
	if _, err := conn.ExecContext(ctx, "ANALYZE TABLE binlog_events"); err != nil {
		t.Fatalf("ANALYZE: %v", err)
	}

	since := base.Add(3 * time.Hour).Add(20 * time.Minute)
	until := since.Add(20 * time.Minute)
	// #797's coarse floor: the hour of since, then one more hour back.
	floor := since.Truncate(time.Hour).Add(-time.Hour)
	q, args := buildQuery(Options{
		Schema: "shop", Table: "t03",
		Since: &since, Until: &until,
		// Inside the seeded range (start_pos runs 100..160096) on purpose: with an
		// anchor above every row the result set is empty, and a control that says
		// "the partitions that CAN hold an admitted row" would be false.
		SincePos: &BinlogPos{File: "binlog.000001", Pos: 100},
		Limit:    1000,
	})

	// Run it, before asking what its plan looks like: neither guard in this file
	// otherwise executes what buildQuery produced. What this catches is rows
	// leaking OUTSIDE the window. It cannot catch rows missing FROM it — a floor
	// that over-excluded would still report a MIN inside the range — and
	// over-exclusion is the direction that breaks #797, so the both-directions
	// check is TestLowerBoundFormsAdmitTheSameRows.
	// Wrapped in an aggregate rather than scanned column by column: the SELECT
	// list is 18 columns wide and a hand-written scan would break on the next
	// column added, for a reason unrelated to this test.
	var n int
	var lo, hi sql.NullTime
	if err := db.QueryRowContext(ctx,
		"SELECT COUNT(*), MIN(event_timestamp), MAX(event_timestamp) FROM ("+q+") t", args...,
	).Scan(&n, &lo, &hi); err != nil {
		t.Fatalf("run the generated query: %v", err)
	}
	if n == 0 {
		t.Fatal("the generated query returned nothing, so the assertions below are vacuous")
	}
	if lo.Time.Before(floor) || hi.Time.After(until) {
		t.Errorf("rows outside the window: [%s, %s] is not within [%s, %s]",
			lo.Time.Format(time.RFC3339), hi.Time.Format(time.RFC3339),
			floor.Format(time.RFC3339), until.Format(time.RFC3339))
	}

	parts := explainScanPartitions(t, db, q, args...)
	// since is in hour 3, so #797's floor is hour 2: hours 0 and 1 must go.
	for h := range 2 {
		name := "p_" + base.Add(time.Duration(h)*time.Hour).Format("2006010215")
		if strings.Contains(parts, name) {
			t.Errorf("partition %s is below the window's floor and must be pruned away, "+
				"but the plan opens it (#1689). partitions=%s", name, parts)
		}
	}
	// The control: the partitions that CAN hold an admitted row are still read,
	// or the assertion above would pass on a plan that reads nothing at all.
	for _, h := range []int{2, 3} {
		name := "p_" + base.Add(time.Duration(h)*time.Hour).Format("2006010215")
		if !strings.Contains(parts, name) {
			t.Errorf("partition %s holds part of the window and must be read; partitions=%s", name, parts)
		}
	}
}

// explainScanPartitions returns the partitions column of every EXPLAIN row that
// is NOT the deferred join's primary-key lookup.
//
// The fetch reads in two steps: an inner scan that finds the events, and a
// primary-key lookup that reads each matched row back. Only the first carries
// the window's predicates — the second joins on a key whose value is unknown at
// plan time, so its partitions column in EXPLAIN lists them all (execution still
// prunes per row from the actual key, which is why buildQuery joins on both key
// parts), and folding the two together would make any pruning assertion
// unsatisfiable. Selecting by "not
// PRIMARY" rather than by index name keeps this working whichever secondary
// index the optimizer picks. The reconstruct package has a single-row sibling;
// this one reads several because the fetch under test is a join.
func explainScanPartitions(t *testing.T, db *sql.DB, stmt string, args ...any) string {
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
	keyCol := slices.Index(cols, "key")
	if idx < 0 || keyCol < 0 {
		t.Fatalf("EXPLAIN is missing the partitions or key column: %v", cols)
	}
	var out []string
	for rows.Next() {
		vals := make([]sql.NullString, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatalf("EXPLAIN scan: %v", err)
		}
		if vals[keyCol].String != "PRIMARY" && vals[idx].Valid {
			out = append(out, vals[idx].String)
		}
	}
	// Checked whether or not anything was collected: the assertions this feeds
	// are NEGATIVE (a partition must be absent), so a truncated iteration would
	// satisfy them silently.
	if err := rows.Err(); err != nil {
		t.Fatalf("EXPLAIN iteration: %v", err)
	}
	if len(out) == 0 {
		// Not "no partitions": the plan has no scan row at all, which makes
		// every assertion above vacuous rather than satisfied.
		t.Fatal("EXPLAIN shows no scan row, so nothing here measures pruning")
	}
	return strings.Join(out, " | ")
}

// The claim this change rests on, checked against a real server instead of
// argued: the two spellings of the floor admit the same rows.
//
// Be precise about what that buys. With buildQuery's hour-aligned floor and
// event_timestamp DATETIME(0) the two are identical BY CONSTRUCTION, so this
// cannot fail on the alignment invariant — no aligned floor reaches the
// fractional divergence buildQuery's comment describes. What it does pin is
// what the paper argument ASSUMES and does not establish: that Go's
// mysqlToSeconds agrees with the server's own TO_SECONDS at this instant, and
// that the driver renders the time.Time parameter into the same wall clock the
// column stores. The len(now) == 3 check is the non-vacuity guard — without it
// the equality would also hold if both forms admitted everything, or nothing.
func TestLowerBoundFormsAdmitTheSameRows(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	ctx := context.Background()

	base := time.Now().UTC().Truncate(time.Hour)
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{base, base.Add(time.Hour)})

	// buildQuery's floor is always hour-aligned, so the boundary that matters is
	// an hour boundary: one second below it, exactly on it, and above.
	floor := base.Add(time.Hour)
	for i, ts := range []time.Time{
		floor.Add(-time.Second), floor, floor.Add(time.Second), floor.Add(30 * time.Minute),
	} {
		testutil.InsertEvent(t, db, "binlog.000001", uint64(100+i*10), uint64(104+i*10),
			ts.UTC().Format("2006-01-02 15:04:05"), nil,
			"shop", "orders", 2, strconv.Itoa(i), nil, nil, []byte(`{"id":1}`))
	}

	collect := func(pred string, arg any) []int64 {
		t.Helper()
		rows, err := db.QueryContext(ctx,
			"SELECT event_id FROM binlog_events WHERE schema_name='shop' AND table_name='orders' AND "+
				pred+" ORDER BY event_id", arg)
		if err != nil {
			t.Fatalf("select with %q: %v", pred, err)
		}
		defer rows.Close()
		var out []int64
		for rows.Next() {
			var id int64
			if err := rows.Scan(&id); err != nil {
				t.Fatalf("scan: %v", err)
			}
			out = append(out, id)
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("iterate with %q: %v", pred, err)
		}
		return out
	}

	was := collect("TO_SECONDS(event_timestamp) >= ?", mysqlToSeconds(floor))
	now := collect("event_timestamp >= ?", floor)
	if !slices.Equal(was, now) {
		t.Errorf("the two spellings of the floor admit different rows: function form %v, column form %v", was, now)
	}
	// Three of the four: the row one second BELOW the floor is excluded, the row
	// exactly ON it is included. Without this the equality above would also hold
	// if both forms admitted everything, or nothing.
	if len(now) != 3 {
		t.Errorf("got %d rows (%v), want the 3 at or after the floor — the boundary is the "+
			"whole point of this test", len(now), now)
	}
}
