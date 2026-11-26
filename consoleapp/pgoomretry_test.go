package consoleapp

import (
	"maps"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/pgshim"
	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

// What the copy's retry in the order written did is counted under
// join_checks from the statement's own RouteNote, whichever side answered,
// beside the join check's outcome; a note with none counts nothing.
func TestPGRouteStats_countsOOMRetry(t *testing.T) {
	r := newPGRouteStats(time.Now(), false)
	for _, n := range []pgshim.RouteNote{
		{Side: pgshim.RouteCopy, Reason: pgshim.ReasonAnswersTheSame, JoinCheck: pgshim.JoinCheckPlanned, OOMRetry: sqlsandbox.WrittenOrderRetryOK},
		{Side: pgshim.RouteSource, Reason: pgshim.ReasonCopyRefusedAfterWrittenOrder, OOMRetry: sqlsandbox.WrittenOrderRetryFailed},
		{Side: pgshim.RouteSource, Reason: pgshim.ReasonCopyRefused, OOMRetry: sqlsandbox.WrittenOrderRetrySkippedNoTime},
		{Side: pgshim.RouteCopy, Failed: true, Reason: pgshim.ReasonAnswersTheSame, OOMRetry: sqlsandbox.WrittenOrderRetryOtherError},
		{Side: pgshim.RouteSource, Reason: pgshim.ReasonCopyRefused},
		{Side: pgshim.RouteCopy, Reason: pgshim.ReasonAnswersTheSame},
	} {
		r.note("p", n)
	}
	n := r.counts("").Servers["p"]
	want := map[string]uint64{pgshim.JoinCheckPlanned: 1, "oom_retry_written_order_ok": 1, "oom_retry_written_order_failed": 1,
		"oom_retry_written_order_skipped_no_time": 1, "oom_retry_written_order_other_error": 1}
	if !maps.Equal(n.JoinChecks, want) {
		t.Errorf("join_checks %v, want %v", n.JoinChecks, want)
	}
	if n.Reasons[pgshim.ReasonCopyRefusedAfterWrittenOrder] != 1 || n.Reasons[pgshim.ReasonCopyRefused] != 2 {
		t.Errorf("reasons %v", n.Reasons)
	}
}
