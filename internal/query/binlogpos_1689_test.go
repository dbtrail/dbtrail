package query

import (
	"context"
	"testing"
	"time"
)

// BinlogPos.AtOrBefore is the Go spelling of the ordering the SincePos/UntilPos
// predicates use in SQL. The rollover cases are the reason it is not a plain
// string compare (#840).
func TestBinlogPosAtOrBefore(t *testing.T) {
	cases := []struct {
		name string
		p, q BinlogPos
		want bool
	}{
		{"same file, p before q", BinlogPos{"mysql-bin.000004", 100}, BinlogPos{"mysql-bin.000004", 200}, true},
		{"same file, p after q", BinlogPos{"mysql-bin.000004", 200}, BinlogPos{"mysql-bin.000004", 100}, false},
		{"same file, same position", BinlogPos{"mysql-bin.000004", 100}, BinlogPos{"mysql-bin.000004", 100}, true},
		{"earlier file wins over a larger position",
			BinlogPos{"mysql-bin.000004", 9999}, BinlogPos{"mysql-bin.000005", 4}, true},
		{"later file loses despite a smaller position",
			BinlogPos{"mysql-bin.000005", 4}, BinlogPos{"mysql-bin.000004", 9999}, false},
		// The rollover: .1000000 FOLLOWS .999999, and a plain string compare
		// would say the opposite because '1' sorts before '9'.
		{"across the rollover, longer name is later",
			BinlogPos{"mysql-bin.999999", 900}, BinlogPos{"mysql-bin.1000000", 4}, true},
		{"across the rollover, inverted",
			BinlogPos{"mysql-bin.1000000", 4}, BinlogPos{"mysql-bin.999999", 900}, false},
		{"zero value is at or before anything", BinlogPos{}, BinlogPos{"mysql-bin.000001", 4}, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := c.p.AtOrBefore(c.q); got != c.want {
				t.Errorf("%+v.AtOrBefore(%+v) = %v, want %v", c.p, c.q, got, c.want)
			}
		})
	}
}

// A total order: for any two coordinates, at least one side holds, and both
// hold only when they are equal. A comparison that fails this would make the
// empty-window proof in reconstruct unsound in one direction or the other.
func TestBinlogPosAtOrBefore_isATotalOrder(t *testing.T) {
	all := []BinlogPos{
		{"mysql-bin.000004", 4}, {"mysql-bin.000004", 100},
		{"mysql-bin.000005", 4}, {"mysql-bin.999999", 900},
		{"mysql-bin.1000000", 4}, {},
	}
	for _, a := range all {
		for _, b := range all {
			ab, ba := a.AtOrBefore(b), b.AtOrBefore(a)
			if !ab && !ba {
				t.Errorf("%+v and %+v are unordered", a, b)
			}
			if ab && ba && a != b {
				t.Errorf("%+v and %+v compare equal but are different", a, b)
			}
		}
	}
}

// VerifyMergedCoverage is what a caller runs INSTEAD of a fetch it has proven
// empty (#1689), so it has to refuse the same configurations the fetch refuses.
// The one that matters is a strict caller with no DBName: the planner cannot
// run without it, so gap detection would be silently off — and a coverage check
// that quietly checks nothing is worse than no check, because it reads as proof.
//
// nil *sql.DB: the two refusals are decided by FetchMergedOptions.validate()
// before any database work. The control is NOT — it reaches query.Plan, which
// returns (nil, nil) for a nil db, so it proves only that validate() let it
// through. That the planner actually RUNS is pinned by the integration test in
// internal/reconstruct (TestRefresh_emptyWindowStillRefusesACoverageGap).
func TestVerifyMergedCoverage_refusesWhatItCannotCheck(t *testing.T) {
	ctx := context.Background()
	since := time.Now().Add(-time.Hour)

	t.Run("strict with no DBName", func(t *testing.T) {
		err := VerifyMergedCoverage(ctx, nil, FetchMergedOptions{
			Opts:      Options{Since: &since},
			NoArchive: true,
			AllowGaps: false,
		})
		if err == nil {
			t.Fatal("want a refusal: without a DBName the planner cannot run, so this " +
				"would report full coverage having checked none of it")
		}
	})

	t.Run("archives included but no fetcher", func(t *testing.T) {
		err := VerifyMergedCoverage(ctx, nil, FetchMergedOptions{
			Opts:      Options{Since: &since},
			DBName:    "idx",
			NoArchive: false,
			AllowGaps: false,
		})
		if err == nil {
			t.Fatal("want a refusal: archives are in scope with no way to read them")
		}
	})

	// The control: the same call with the coverage inputs present must NOT
	// refuse, or the two cases above would pass for the wrong reason.
	t.Run("everything the check needs", func(t *testing.T) {
		if err := VerifyMergedCoverage(ctx, nil, FetchMergedOptions{
			Opts:      Options{Since: &since},
			DBName:    "idx",
			NoArchive: true,
			AllowGaps: false,
		}); err != nil {
			t.Fatalf("VerifyMergedCoverage = %v, want no refusal", err)
		}
	})
}
