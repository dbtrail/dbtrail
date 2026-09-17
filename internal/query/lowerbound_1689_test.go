package query

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// A baseline-anchored fetch needs a lower bound the INDEX can use. With
// SincePos set, buildQuery emitted its only lower bound as
// TO_SECONDS(event_timestamp) >= <literal>, which wraps the column in a
// function: the optimizer cannot bound the idx_row_lookup range with it, and it
// prunes no partitions either. Measured on MySQL 8.0.46 and 8.4.9, same result
// on both: that form opens every partition in the window and leaves the range
// bounded on ONE side, so each table's scan starts at its first indexed entry
// and walks forward through one to two hours of entries it then discards
// (#1689).
//
// The replacement admits EXACTLY the same rows, and the reason is the FLOOR,
// not the function: with an integer-second-aligned floor,
// `TO_SECONDS(col) >= mysqlToSeconds(F)` and `col >= F` select the same set.
// See buildQuery for the fractional case that breaks that, and
// TestLowerBoundFormsAdmitTheSameRows for the check against a real server.
//
// What THIS test pins is the FORM plus the floor's VALUE: no TO_SECONDS wrapper
// at the emitted floor, a plain `event_timestamp >= ?`, and an argument
// carrying #797's unchanged one-hour-back instant. It does not prove the window
// is unmoved in general — a stray second predicate, say an un-widened
// TO_SECONDS literal, would AND in a tighter floor and pass all three checks.
// What DOES catch that is TestFetch_sincePos_timestampSkewNotLost in this
// package (#797): it plants an event ten minutes before the anchor's hour and
// insists it is still found, and a review measured that mutation dropping 67%
// of the window's rows while every assertion here stayed green. Do not delete
// that test as redundant with these; it covers the case these cannot.
func TestBuildQuery_sincePosLowerBoundIsIndexUsable(t *testing.T) {
	since := time.Date(2026, 9, 16, 15, 45, 36, 0, time.UTC)
	opts := Options{
		Schema:   "tpcc",
		Table:    "district1",
		Since:    &since,
		SincePos: &BinlogPos{File: "mysql-bin.000004", Pos: 900000},
	}
	q, args := buildQuery(opts)

	// The margin is #797's, unchanged: the hour of Since, then one more back.
	floor := since.Truncate(time.Hour).Add(-time.Hour)

	if hint := fmt.Sprintf("TO_SECONDS(event_timestamp) >= %d", mysqlToSeconds(floor)); strings.Contains(q, hint) {
		t.Errorf("the lower bound is still wrapped in TO_SECONDS(): the optimizer can neither seek "+
			"the index range nor prune partitions with it, so the scan starts at the table's first "+
			"entry and walks (#1689). Query:\n%s", q)
	}
	if !strings.Contains(q, "event_timestamp >= ?") {
		t.Errorf("want a plain column lower bound the index can seek to, got:\n%s", q)
	}

	// Same instant, or the rewrite silently moved the window instead of
	// restating it. This is the assertion that makes the change a refactor.
	var carried bool
	for _, a := range args {
		if tv, ok := a.(time.Time); ok && tv.Equal(floor) {
			carried = true
		}
	}
	if !carried {
		t.Errorf("no argument carries the unchanged floor %s, so the window moved; args=%v",
			floor.Format(time.RFC3339), args)
	}
}

// The position predicate is what governs correctness (#797), and it must stay
// exactly as it was: the time bound above is a coarse, deliberately
// over-inclusive pre-filter, and if this rewrite had turned it into the exact
// cut, a transaction that executed before the anchor's hour and committed after
// it would be dropped with nothing to say so.
func TestBuildQuery_sincePosKeepsThePositionPredicate(t *testing.T) {
	since := time.Date(2026, 9, 16, 15, 45, 36, 0, time.UTC)
	q, args := buildQuery(Options{
		Since:    &since,
		SincePos: &BinlogPos{File: "mysql-bin.000004", Pos: 900000},
	})
	if !strings.Contains(q, "binlog_file = ? AND start_pos >= ?") {
		t.Errorf("the exact position lower bound is gone; the time bound alone is NOT the cut:\n%s", q)
	}
	// And the exact Since instant must still be ABSENT. Asserted on the ARGS,
	// because that is the shape the regression would actually take: buildQuery
	// never inlines a quoted datetime — every time bound it emits is either an
	// integer literal or a parameter — so a check for the instant in the SQL
	// TEXT can never fire, however wrong the builder gets. The sibling `else`
	// branch shows the exact form to watch for: a SECOND `event_timestamp >= ?`
	// carrying Since itself.
	for _, a := range args {
		if tv, ok := a.(time.Time); ok && tv.Equal(since) {
			t.Errorf("the exact Since instant is bound as a parameter: the coarse floor was "+
				"tightened into the exact cut, so a transaction that executed before the anchor's "+
				"hour and committed after it is silently dropped (#797); args=%v", args)
		}
	}
	if n := strings.Count(q, "event_timestamp >= ?"); n != 1 {
		t.Errorf("want exactly one lower bound on event_timestamp, got %d:\n%s", n, q)
	}
}
