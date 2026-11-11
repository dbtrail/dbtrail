package consoleapp

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/notify"
	"github.com/dbtrail/dbtrail/internal/observe"
)

// gatherGauge reads one series from the process-global default registry —
// the same registry /metrics serves, so these tests assert what an operator
// actually scrapes.
func gatherGauge(t *testing.T, metric string, labels map[string]string) (float64, bool) {
	t.Helper()
	mfs, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range mfs {
		if mf.GetName() != metric {
			continue
		}
	metrics:
		for _, m := range mf.GetMetric() {
			got := make(map[string]string)
			for _, lp := range m.GetLabel() {
				got[lp.GetName()] = lp.GetValue()
			}
			for k, v := range labels {
				if got[k] != v {
					continue metrics
				}
			}
			return m.GetGauge().GetValue(), true
		}
	}
	return 0, false
}

// TestVerifyFinishObservers_publishesGauges pins the wiring the feature IS:
// a publishable run lands on /metrics, and a later failed run does NOT
// overwrite it (zeroed counts would auto-resolve a live mismatch alert;
// a refreshed timestamp would silence the staleness alert).
func TestVerifyFinishObservers_publishesGauges(t *testing.T) {
	const server = "gauge-wp-1205"
	rec := console.VerifyRunRecord{ServerID: "sg1", ServerName: server,
		VerifyStatus: console.VerifyStatus{State: "succeeded", FinishedAt: "2026-08-02T12:00:00Z",
			Summary: console.VerifySummary{Match: 3, Mismatch: 2, Total: 5}}}
	verifyFinishObservers(nil)(rec)

	if got, ok := gatherGauge(t, "bintrail_verify_tables", map[string]string{"server": server, "status": "mismatch"}); !ok || got != 2 {
		t.Fatalf("mismatch gauge = %v (found=%v), want 2", got, ok)
	}
	ts, ok := gatherGauge(t, "bintrail_verify_last_run_timestamp_seconds", map[string]string{"server": server})
	if !ok || ts == 0 {
		t.Fatalf("last_run gauge missing after a publishable run (found=%v, ts=%v)", ok, ts)
	}

	// A failed run must leave every series untouched.
	verifyFinishObservers(nil)(console.VerifyRunRecord{ServerID: "sg1", ServerName: server,
		VerifyStatus: console.VerifyStatus{State: "failed", FinishedAt: "2026-08-02T13:00:00Z", LastError: "boom"}})
	if got, _ := gatherGauge(t, "bintrail_verify_tables", map[string]string{"server": server, "status": "mismatch"}); got != 2 {
		t.Fatalf("failed run overwrote the mismatch gauge: %v", got)
	}
	if got, _ := gatherGauge(t, "bintrail_verify_last_run_timestamp_seconds", map[string]string{"server": server}); got != ts {
		t.Fatalf("failed run refreshed the timestamp: %v -> %v (staleness alert silenced)", ts, got)
	}
	observe.DeleteVerifyOutcome(server)
}

// TestSeedVerifyGauges: seeding selects the newest PUBLISHABLE record and
// labels it with the server's CURRENT name — never the name captured in the
// historical record (a pre-rename label would join with nothing).
func TestSeedVerifyGauges(t *testing.T) {
	reg := testRegistryWithEntries(t, console.ServerEntry{Name: "new-name-1205", DSN: "dsn-x"})
	id := reg.List()[0].ID
	hist, err := console.OpenVerifyHistory(filepath.Join(t.TempDir(), "h.json"))
	if err != nil {
		t.Fatal(err)
	}
	older := console.VerifyRunRecord{ServerID: id, ServerName: "old-name-1205",
		VerifyStatus: console.VerifyStatus{State: "succeeded", FinishedAt: "2026-08-01T10:00:00Z",
			Summary: console.VerifySummary{Match: 7, Total: 7}}}
	newestButFailed := console.VerifyRunRecord{ServerID: id, ServerName: "old-name-1205",
		VerifyStatus: console.VerifyStatus{State: "failed", FinishedAt: "2026-08-02T10:00:00Z", LastError: "boom"}}
	if err := hist.Append(older); err != nil {
		t.Fatal(err)
	}
	if err := hist.Append(newestButFailed); err != nil {
		t.Fatal(err)
	}

	seedVerifyGauges(reg, hist)
	if got, ok := gatherGauge(t, "bintrail_verify_tables", map[string]string{"server": "new-name-1205", "status": "match"}); !ok || got != 7 {
		t.Fatalf("seed must publish the newest PUBLISHABLE run under the CURRENT name: got %v found=%v", got, ok)
	}
	if _, ok := gatherGauge(t, "bintrail_verify_last_run_timestamp_seconds", map[string]string{"server": "old-name-1205"}); ok {
		t.Fatal("seed resurrected the pre-rename label from the historical record")
	}
	observe.DeleteVerifyOutcome("new-name-1205")
}

// TestContinuityWatcher_runCycle drives the poller cycle with an injected
// readGap — pinning the gauge publication, the nil-notifier path, the
// unknown-unpublishes rule, and the departed-server cleanup (which also
// unpublishes the departed server's VERIFY series).
func TestContinuityWatcher_runCycle(t *testing.T) {
	reg := testRegistryWithEntries(t,
		console.ServerEntry{Name: "cw-a", DSN: "dsn-cw-a"},
		console.ServerEntry{Name: "cw-b", DSN: "dsn-cw-b"},
	)
	w := &continuityWatcher{
		registry:    reg, // n is nil: the Prometheus-only deployment must not panic
		unknownEdge: notify.NewEdge(0),
		prevNames:   make(map[string]bool),
		readGap: func(_ context.Context, dsn string) (gapReading, error) {
			if dsn == "dsn-cw-b" {
				return gapReading{}, errors.New("unknown column 'gap_lost_at'")
			}
			return gapReading{lost: true, detail: "binlog gap"}, nil
		},
	}
	w.runCycle(context.Background())
	if got, ok := gatherGauge(t, "bintrail_continuity_gap_lost", map[string]string{"server": "cw-a"}); !ok || got != 1 {
		t.Fatalf("gap_lost for cw-a = %v (found=%v), want 1", got, ok)
	}
	if _, ok := gatherGauge(t, "bintrail_continuity_gap_lost", map[string]string{"server": "cw-b"}); ok {
		t.Fatal("unknowable index published a gauge — unknown must never read as a verdict")
	}

	// Delete cw-a and seed a verify series under its name: the next cycle
	// must unpublish BOTH families, not freeze them at their last value.
	observe.SetVerifyOutcome("cw-a", timeMustParse(t, "2026-08-02T10:00:00Z"), 1, 0, 0, 0)
	if err := reg.Delete(reg.List()[0].ID); err != nil {
		t.Fatal(err)
	}
	w.runCycle(context.Background())
	if _, ok := gatherGauge(t, "bintrail_continuity_gap_lost", map[string]string{"server": "cw-a"}); ok {
		t.Fatal("departed server's continuity series froze instead of unpublishing")
	}
	if _, ok := gatherGauge(t, "bintrail_verify_last_run_timestamp_seconds", map[string]string{"server": "cw-a"}); ok {
		t.Fatal("departed server's verify series froze instead of unpublishing")
	}
}

func timeMustParse(t *testing.T, s string) time.Time {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339, s)
	if err != nil {
		t.Fatal(err)
	}
	return parsed
}
