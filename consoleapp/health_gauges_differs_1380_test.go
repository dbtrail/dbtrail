package consoleapp

import (
	"testing"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/observe"
)

// #1380: tables that differ from a snapshot read with no locks count in the
// mismatch series as well as in their own, so an alert rule on mismatch does
// not read zero while one stands, including a run where they are all there is.
func TestVerifyGauges_aTornDifferenceIsNotZeroProblems(t *testing.T) {
	for _, tc := range []struct {
		name                     string
		sum                      console.VerifySummary
		mismatch, differs, incon float64
	}{
		{"beside matches", console.VerifySummary{Match: 3, Inconclusive: 1, InconclusiveDiffers: 1, Total: 4}, 1, 1, 1},
		{"every table", console.VerifySummary{Inconclusive: 2, InconclusiveDiffers: 2, Total: 2}, 2, 2, 2},
		{"beside a mismatch", console.VerifySummary{Match: 1, Mismatch: 1, Inconclusive: 1, InconclusiveDiffers: 1, Total: 3}, 2, 1, 1},
		{"none", console.VerifySummary{Match: 3, Inconclusive: 1, Total: 4}, 0, 0, 1},
	} {
		server := "gauge-1380-" + tc.name
		verifyFinishObservers(nil)(console.VerifyRunRecord{ServerID: "s", ServerName: server,
			VerifyStatus: console.VerifyStatus{State: "succeeded", FinishedAt: "2026-09-28T12:00:00Z", Summary: tc.sum}})
		for status, want := range map[string]float64{"mismatch": tc.mismatch, "differs": tc.differs, "inconclusive": tc.incon} {
			if got, ok := gatherGauge(t, "bintrail_verify_tables", map[string]string{"server": server, "status": status}); !ok || got != want {
				t.Errorf("%s: %s = %v (found %v), want %v", tc.name, status, got, ok, want)
			}
		}
		observe.DeleteVerifyOutcome(server)
	}
	// An all-inconclusive run with no difference still publishes nothing.
	if verifyRunPublishable(console.VerifyRunRecord{VerifyStatus: console.VerifyStatus{State: "succeeded",
		Summary: console.VerifySummary{Inconclusive: 2, Total: 2}}}) {
		t.Error("an all-inconclusive run with no difference publishes")
	}
}
