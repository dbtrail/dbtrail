package consoleapp

import (
	"errors"
	"fmt"
	"maps"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/pgshim"
	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

// What a run says of its retry in the order written, as join_checks names.
func TestPGOOMRetryOutcome(t *testing.T) {
	failed := &sqlsandbox.QueryError{Message: "Out of Memory Error: x", WrittenOrderRetry: sqlsandbox.WrittenOrderRetryFailed}
	for _, tc := range []struct {
		res  sqlsandbox.Result
		err  error
		want string
	}{
		{sqlsandbox.Result{WrittenOrderRetry: sqlsandbox.WrittenOrderRetryOK}, nil, pgOOMRetryOK},
		{sqlsandbox.Result{}, failed, pgOOMRetryFailed},
		{sqlsandbox.Result{}, fmt.Errorf("x: %w", failed), pgOOMRetryFailed},
		{sqlsandbox.Result{}, nil, ""},
		{sqlsandbox.Result{}, &sqlsandbox.QueryError{Message: "Out of Memory Error: x"}, ""},
		{sqlsandbox.Result{}, errors.New("x"), ""},
	} {
		if got := pgOOMRetryOutcome(tc.res, tc.err); got != tc.want {
			t.Errorf("%+v %v: %q, want %q", tc.res, tc.err, got, tc.want)
		}
	}
}

// The retry is counted under join_checks whichever side answered, once per
// statement, beside the join check's own outcome; a statement with none
// counts nothing, and a stale outcome is not counted twice.
func TestPGConnAges_countOOMRetry(t *testing.T) {
	r := newPGRouteStats(time.Now(), false)
	c := r.agesFor("p")
	routed := c.routed()

	c.stashOOMRetry(pgOOMRetryOK)
	routed(pgshim.RouteNote{Side: pgshim.RouteCopy, Reason: pgshim.ReasonAnswersTheSame, JoinCheck: pgshim.JoinCheckPlanned})
	// A statement with no run of the copy (the port's own answer): the last
	// outcome was taken already.
	routed(pgshim.RouteNote{Side: pgshim.RouteCopy, Reason: pgshim.ReasonCopyCommand})
	c.stashOOMRetry(pgOOMRetryFailed)
	routed(pgshim.RouteNote{Side: pgshim.RouteSource, Reason: pgshim.ReasonCopyRefusedAfterWrittenOrder})
	c.stashOOMRetry("")
	routed(pgshim.RouteNote{Side: pgshim.RouteSource, Reason: pgshim.ReasonCopyRefused})
	routed(pgshim.RouteNote{Side: pgshim.RouteCopy, Reason: pgshim.ReasonAnswersTheSame})

	n := r.counts("").Servers["p"]
	want := map[string]uint64{pgshim.JoinCheckPlanned: 1, pgOOMRetryOK: 1, pgOOMRetryFailed: 1}
	if !maps.Equal(n.JoinChecks, want) {
		t.Errorf("join_checks %v, want %v", n.JoinChecks, want)
	}
	if n.Reasons[pgshim.ReasonCopyRefusedAfterWrittenOrder] != 1 || n.Copy != 3 || n.Reasons[pgshim.ReasonCopyRefused] != 1 {
		t.Errorf("reasons %v", n.Reasons)
	}

	var none *pgRouteStats
	none.agesFor("p").stashOOMRetry(pgOOMRetryOK) // no panic
}
