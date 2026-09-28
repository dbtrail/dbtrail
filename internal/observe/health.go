package observe

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Watch-daemon health gauges (#1203) — the pull-based siblings of the webhook
// notification channel (#1192): the same three "your safety net has a hole"
// conditions, exported for Prometheus/Alertmanager.
//
// All are *Vec collectors — including the label-less rotation pair — so
// nothing is published until the daemon actually has a verdict: an absent
// series means "no verdict" — never evaluated, or (for the verify pair)
// never a conclusive run — and NEVER a misleading healthy zero (the same
// rule the index timestamp gauges follow for an empty index).
// The server label is the display name — the same identity the webhook's
// `server` field and the console UI use.
var (
	continuityGapLost = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "bintrail",
		Subsystem: "continuity",
		Name:      "gap_lost",
		Help:      "1 when the stream stamped a permanent capture gap (events in it are unrecoverable); 0 when continuity is intact. Absent while the index cannot be evaluated.",
	}, []string{"server"})

	verifyLastRun = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "bintrail",
		Subsystem: "verify",
		Name:      "last_run_timestamp_seconds",
		Help:      "Unix time of the newest verify run that succeeded and conclusively verified at least one table. Failed and all-inconclusive runs do not refresh it - staleness means verification is broken.",
	}, []string{"server"})

	verifyTables = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "bintrail",
		Subsystem: "verify",
		Name:      "tables",
		Help:      "Per-status table counts of the newest conclusive verify run (see last_run_timestamp_seconds for what counts as one).",
	}, []string{"server", "status"})

	rotationHealthy = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "bintrail",
		Subsystem: "rotation",
		Name:      "healthy",
		Help:      "1 when the last built-in rotation cycle neither failed nor deferred unarchived partitions; 0 otherwise.",
	}, nil)

	rotationDeferred = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "bintrail",
		Subsystem: "rotation",
		Name:      "deferred_partitions",
		Help:      "Unarchived partitions the last built-in rotation cycle declined to drop.",
	}, nil)
)

// SetContinuityGapLost publishes one index's continuity verdict.
func SetContinuityGapLost(server string, lost bool) {
	continuityGapLost.WithLabelValues(server).Set(boolGauge(lost))
}

// ClearContinuity unpublishes a server's continuity gauge — the unknowable
// case (unreachable index, legacy schema without the gap columns). Unknown is
// never "no gap", so it must never read as 0.
func ClearContinuity(server string) {
	continuityGapLost.DeleteLabelValues(server)
}

// DeleteVerifyOutcome unpublishes a server's verify gauges — called when the
// server leaves the registry or is renamed, so a frozen series can never keep
// alerting (or keep reading healthy) for something nobody evaluates anymore.
func DeleteVerifyOutcome(server string) {
	verifyLastRun.DeleteLabelValues(server)
	for _, status := range []string{"match", "mismatch", "inconclusive", "error", VerifyStatusDiffers} {
		verifyTables.DeleteLabelValues(server, status)
	}
}

// SetVerifyOutcome publishes the newest finished verify run for a server.
func SetVerifyOutcome(server string, finishedAt time.Time, match, mismatch, inconclusive, errorCount int) {
	verifyLastRun.WithLabelValues(server).Set(float64(finishedAt.Unix()))
	verifyTables.WithLabelValues(server, "match").Set(float64(match))
	verifyTables.WithLabelValues(server, "mismatch").Set(float64(mismatch))
	verifyTables.WithLabelValues(server, "inconclusive").Set(float64(inconclusive))
	verifyTables.WithLabelValues(server, "error").Set(float64(errorCount))
}

// VerifyStatusDiffers is the status label of the tables where a difference
// was found over a snapshot read with no locks (#1380). They are reported
// inconclusive, so they are counted under "inconclusive" too; this series is
// that slice on its own, so that an alert on differences does not read zero
// while one is standing. "mismatch" is left meaning what it meant.
const VerifyStatusDiffers = "differs"

// SetVerifyDiffers publishes that slice for the run SetVerifyOutcome just
// published.
func SetVerifyDiffers(server string, differs int) {
	verifyTables.WithLabelValues(server, VerifyStatusDiffers).Set(float64(differs))
}

// SetRotationHealth publishes the last rotation cycle's health — the same
// (failed, deferred) pair rotation.StartLoop's onCycle callbacks observe.
func SetRotationHealth(failed bool, deferred int) {
	rotationHealthy.WithLabelValues().Set(boolGauge(!failed && deferred == 0))
	rotationDeferred.WithLabelValues().Set(float64(deferred))
}

func boolGauge(b bool) float64 {
	if b {
		return 1
	}
	return 0
}
