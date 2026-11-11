package consoleapp

import (
	"context"
	"testing"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/notify"
)

type fakeSender struct{ events []notify.Event }

func (f *fakeSender) Notify(ev notify.Event) { f.events = append(f.events, ev) }

func testNotifier() (*watchNotifier, *fakeSender) {
	f := &fakeSender{}
	return &watchNotifier{send: f, edge: notify.NewEdge(0)}, f
}

func TestWatchNotifier_VerifyFinished(t *testing.T) {
	n, f := testNotifier()
	bad := console.VerifyRunRecord{
		ServerID: "s1", ServerName: "wp", Trigger: "scheduled",
		VerifyStatus: console.VerifyStatus{State: "succeeded", Mode: console.VerifyModeBaselineAnchored,
			Summary: console.VerifySummary{Match: 3, Mismatch: 2, Total: 5}},
	}
	n.VerifyFinished(bad)
	if len(f.events) != 1 || f.events[0].Severity != "critical" || f.events[0].Event != "verify_problem" {
		t.Fatalf("mismatch run: want one critical verify_problem, got %+v", f.events)
	}
	if f.events[0].Details["mismatch"] != "2" || f.events[0].Server != "wp" {
		t.Fatalf("event details incomplete: %+v", f.events[0])
	}

	// Same condition again inside the repeat window: edge-suppressed.
	n.VerifyFinished(bad)
	if len(f.events) != 1 {
		t.Fatalf("repeat inside window must be suppressed, got %+v", f.events)
	}

	// Clean run: exactly one recovery event, then silence while it stays clean.
	clean := console.VerifyRunRecord{
		ServerID: "s1", ServerName: "wp",
		VerifyStatus: console.VerifyStatus{State: "succeeded", Summary: console.VerifySummary{Match: 5, Total: 5}},
	}
	n.VerifyFinished(clean)
	if len(f.events) != 2 || !f.events[1].Resolved || f.events[1].Severity != "info" {
		t.Fatalf("clean run after a problem must send one resolved event, got %+v", f.events)
	}
	n.VerifyFinished(clean)
	if len(f.events) != 2 {
		t.Fatalf("clean run with no prior problem must stay silent, got %+v", f.events)
	}

	// Error-only (no mismatch) is warning; a failed run reports its error.
	n2, f2 := testNotifier()
	n2.VerifyFinished(console.VerifyRunRecord{ServerID: "s2",
		VerifyStatus: console.VerifyStatus{State: "succeeded", Summary: console.VerifySummary{Error: 1, Total: 1}}})
	if len(f2.events) != 1 || f2.events[0].Severity != "warning" {
		t.Fatalf("error-only run: want warning, got %+v", f2.events)
	}
	// Escalation: a later CRITICAL mismatch must not be suppressed by the
	// warning tier's repeat window — the severity is part of the edge key.
	n2.VerifyFinished(console.VerifyRunRecord{ServerID: "s2",
		VerifyStatus: console.VerifyStatus{State: "succeeded", Summary: console.VerifySummary{Mismatch: 4, Total: 4}}})
	if len(f2.events) != 2 || f2.events[1].Severity != "critical" {
		t.Fatalf("warning must not mask a subsequent critical mismatch, got %+v", f2.events)
	}
	// A clean run resolves BOTH tiers with one event.
	n2.VerifyFinished(console.VerifyRunRecord{ServerID: "s2",
		VerifyStatus: console.VerifyStatus{State: "succeeded", Summary: console.VerifySummary{Match: 4, Total: 4}}})
	if len(f2.events) != 3 || !f2.events[2].Resolved {
		t.Fatalf("clean run must resolve both severity tiers once, got %+v", f2.events)
	}

	n3, f3 := testNotifier()
	n3.VerifyFinished(console.VerifyRunRecord{ServerID: "s3",
		VerifyStatus: console.VerifyStatus{State: "failed", LastError: "connect index: boom"}})
	if len(f3.events) != 1 || f3.events[0].Severity != "warning" || f3.events[0].Summary != "verification run failed: connect index: boom" {
		t.Fatalf("failed run: %+v", f3.events)
	}

	// Skip records are bookkeeping, never notifications.
	n4, f4 := testNotifier()
	n4.VerifyFinished(console.VerifyRunRecord{ServerID: "s4", VerifyStatus: console.VerifyStatus{State: "skipped"}})
	if len(f4.events) != 0 {
		t.Fatalf("skip record must not notify: %+v", f4.events)
	}
}

// TestWatchNotifier_VerifyRunsThatVerifiedNothing: a run with zero verified
// tables must neither reassure (auto-close a real mismatch alert) nor stay
// entirely invisible when it means "verification is not verifying".
func TestWatchNotifier_VerifyRunsThatVerifiedNothing(t *testing.T) {
	n, f := testNotifier()
	mismatch := console.VerifyRunRecord{ServerID: "s1", ServerName: "wp",
		VerifyStatus: console.VerifyStatus{State: "succeeded", Summary: console.VerifySummary{Mismatch: 2, Match: 3, Total: 5}}}
	n.VerifyFinished(mismatch)
	if len(f.events) != 1 {
		t.Fatalf("setup: %+v", f.events)
	}

	// Zero-table run ("only one baseline yet" / empty tables filter): the
	// prior critical alert must NOT be resolved.
	n.VerifyFinished(console.VerifyRunRecord{ServerID: "s1",
		VerifyStatus: console.VerifyStatus{State: "succeeded"}})
	if len(f.events) != 1 {
		t.Fatalf("zero-table run must neither fire nor resolve, got %+v", f.events)
	}

	// All-inconclusive (baseline/archive unreachable): fires as warning —
	// Report.ExitError's rule — and must NOT resolve the mismatch alert.
	n.VerifyFinished(console.VerifyRunRecord{ServerID: "s1", ServerName: "wp",
		VerifyStatus: console.VerifyStatus{State: "succeeded", Summary: console.VerifySummary{Inconclusive: 5, Total: 5}}})
	if len(f.events) != 2 || f.events[1].Severity != "warning" || f.events[1].Resolved {
		t.Fatalf("all-inconclusive: want a non-resolved warning, got %+v", f.events)
	}
	if f.events[1].Summary != "verification could not verify any table: all 5 inconclusive" {
		t.Fatalf("all-inconclusive summary: %q", f.events[1].Summary)
	}
}

func TestWatchNotifier_RotationCycle(t *testing.T) {
	n, f := testNotifier()
	n.RotationCycle(true, 0)
	n.RotationCycle(false, 3) // still unhealthy (deferring) — suppressed, same edge key
	if len(f.events) != 1 || f.events[0].Event != "rotation_unhealthy" || f.events[0].Severity != "warning" {
		t.Fatalf("want one rotation_unhealthy warning, got %+v", f.events)
	}
	n.RotationCycle(false, 0)
	if len(f.events) != 2 || !f.events[1].Resolved {
		t.Fatalf("healthy cycle after unhealthy must resolve once, got %+v", f.events)
	}
	n.RotationCycle(false, 0)
	if len(f.events) != 2 {
		t.Fatalf("healthy with no prior problem must stay silent, got %+v", f.events)
	}
}

func TestWatchNotifier_Continuity(t *testing.T) {
	n, f := testNotifier()
	n.Continuity("wp", "dsn-wp", gapReading{lost: true, detail: "binlog gap at file 42"})
	n.Continuity("wp", "dsn-wp", gapReading{lost: true, detail: "binlog gap at file 42"})
	if len(f.events) != 1 || f.events[0].Severity != "critical" || f.events[0].Details["detail"] != "binlog gap at file 42" {
		t.Fatalf("want one critical continuity event, got %+v", f.events)
	}
	// A DIFFERENT gap while the first is active is a new data-loss event —
	// the changed detail bypasses the repeat window.
	n.Continuity("wp", "dsn-wp", gapReading{lost: true, detail: "binlog gap at file 99"})
	if len(f.events) != 2 || f.events[1].Details["detail"] != "binlog gap at file 99" {
		t.Fatalf("changed gap detail must re-notify, got %+v", f.events)
	}
	// A different index is its own edge key.
	n.Continuity("other", "dsn-other", gapReading{lost: true, detail: ""})
	if len(f.events) != 3 {
		t.Fatalf("second index must fire independently, got %+v", f.events)
	}
	n.Continuity("wp", "dsn-wp", gapReading{})
	if len(f.events) != 4 || !f.events[3].Resolved {
		t.Fatalf("recovery must resolve once, got %+v", f.events)
	}
}

func TestContinuityTargets(t *testing.T) {
	reg := testRegistryWithEntries(t,
		console.ServerEntry{Name: "a", DSN: "dsn-a"},
		console.ServerEntry{Name: "same-as-boot", DSN: "dsn-boot"},
	)
	got := continuityTargets(reg, "dsn-boot")
	if len(got) != 2 || got[0].dsn != "dsn-boot" || got[1].dsn != "dsn-a" {
		t.Fatalf("want boot + a (registry entry sharing the boot DSN deduped), got %+v", got)
	}
	if got := continuityTargets(reg, ""); len(got) != 2 {
		t.Fatalf("source-less watch: want the 2 registry entries, got %+v", got)
	}
	if got := continuityTargets(nil, ""); got != nil {
		t.Fatalf("no registry, no boot: want nil, got %+v", got)
	}
}

func testRegistryWithEntries(t *testing.T, entries ...console.ServerEntry) *console.Registry {
	t.Helper()
	reg, err := console.LoadRegistry(t.TempDir() + "/console-servers.yaml")
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if _, err := reg.Add(e); err != nil {
			t.Fatal(err)
		}
	}
	return reg
}

func TestRotationCycleHooks(t *testing.T) {
	// The gauge hook is always present (#1203) — it publishes only when a
	// cycle actually runs; the notifier hook joins it when configured.
	if hooks := rotationCycleHooks(nil); len(hooks) != 1 {
		t.Fatalf("nil notifier must still yield the gauge hook, got %d", len(hooks))
	}
	n, f := testNotifier()
	hooks := rotationCycleHooks(n)
	if len(hooks) != 2 {
		t.Fatalf("want gauge + notifier hooks, got %d", len(hooks))
	}
	hooks[1](true, 0)
	if len(f.events) != 1 {
		t.Fatalf("notifier hook is not wired: %+v", f.events)
	}
}

// TestVerifyRunPublishable: only a succeeded run that conclusively verified
// at least one table may reach the gauges — a failed, zero-table, or
// all-inconclusive run overwriting the counts would auto-resolve a live
// mismatch alert and keep the staleness alert quiet while verification is
// broken (the gauge-path twin of the webhook's clean/problem split).
func TestVerifyRunPublishable(t *testing.T) {
	rec := func(state string, match, mismatch, inconclusive, total int) console.VerifyRunRecord {
		return console.VerifyRunRecord{VerifyStatus: console.VerifyStatus{State: state,
			Summary: console.VerifySummary{Match: match, Mismatch: mismatch, Inconclusive: inconclusive, Total: total}}}
	}
	cases := []struct {
		name string
		rec  console.VerifyRunRecord
		want bool
	}{
		{"clean run", rec("succeeded", 5, 0, 0, 5), true},
		{"mismatch run still publishes (the alert must fire)", rec("succeeded", 3, 2, 0, 5), true},
		{"partially inconclusive publishes", rec("succeeded", 3, 0, 2, 5), true},
		{"failed run must not publish", rec("failed", 0, 0, 0, 0), false},
		{"zero-table run must not publish", rec("succeeded", 0, 0, 0, 0), false},
		{"all-inconclusive must not publish", rec("succeeded", 0, 0, 5, 5), false},
		{"skip record must not publish", rec(console.VerifyStateSkipped, 0, 0, 0, 0), false},
	}
	for _, tc := range cases {
		if got := verifyRunPublishable(tc.rec); got != tc.want {
			t.Errorf("%s: publishable = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// TestVerifyFinishObservers: gauges never panic without a notifier, and the
// notifier still receives the record when configured.
func TestVerifyFinishObservers(t *testing.T) {
	rec := console.VerifyRunRecord{ServerID: "s1", ServerName: "wp",
		VerifyStatus: console.VerifyStatus{State: "succeeded", FinishedAt: "2026-08-02T12:00:00Z",
			Summary: console.VerifySummary{Mismatch: 1, Total: 1}}}
	verifyFinishObservers(nil)(rec) // must not panic
	n, f := testNotifier()
	verifyFinishObservers(n)(rec)
	if len(f.events) != 1 {
		t.Fatalf("notifier must observe the record: %+v", f.events)
	}
}

// TestNewWatchNotifierFromFlags_validatesURL: a webhook URL typo must refuse
// startup, not surface weeks later as a buried delivery warning during the
// first real incident.
func TestNewWatchNotifierFromFlags_validatesURL(t *testing.T) {
	orig := upNotifyWebhook
	defer func() { upNotifyWebhook = orig }()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	upNotifyWebhook = ""
	if n, err := newWatchNotifierFromFlags(ctx); n != nil || err != nil {
		t.Fatalf("empty flag: want nil,nil got %v,%v", n, err)
	}
	for _, bad := range []string{"://bad", "ftp://host/hook", "not-a-url", "http://"} {
		upNotifyWebhook = bad
		if _, err := newWatchNotifierFromFlags(ctx); err == nil {
			t.Errorf("URL %q accepted; a typo would be silent until the first incident", bad)
		}
	}
	upNotifyWebhook = "https://hooks.example.com/T123"
	n, err := newWatchNotifierFromFlags(ctx)
	if err != nil || n == nil {
		t.Fatalf("valid URL rejected: %v", err)
	}
}
