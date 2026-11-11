package consoleapp

import (
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/notify"
)

// captureHarness drives captureWatcher cycles on a fake clock, a fake entry
// list and a fake status per entry — no supervisor, no ticker.
type captureHarness struct {
	w       *captureWatcher
	f       *fakeSender
	now     time.Time
	entries []console.ServerEntry
	status  map[string]console.MonitorStatus
}

func newCaptureHarness() *captureHarness {
	n, f := testNotifier()
	h := &captureHarness{f: f, now: time.Date(2026, 11, 11, 12, 0, 0, 0, time.UTC), status: map[string]console.MonitorStatus{}}
	h.w = &captureWatcher{
		n:       n,
		entries: func() []console.ServerEntry { return h.entries },
		status:  func(id string) console.MonitorStatus { return h.status[id] },
		now:     func() time.Time { return h.now },
	}
	n.edge = notify.NewEdgeWithClock(0, func() time.Time { return h.now })
	return h
}

func (h *captureHarness) add(id, name string) {
	h.entries = append(h.entries, console.ServerEntry{ID: id, Name: name, SourceDSN: "u:p@tcp(src:3306)/", DSN: "u:p@tcp(idx:3306)/x", MonitorDesired: true})
}

// cycle sets one entry's status, advances the clock, and runs one cycle. A
// status with no Since gets one: a running stream an hour old (long past
// captureHealthyAfter), any other state entered just now.
func (h *captureHarness) cycle(advance time.Duration, id string, st console.MonitorStatus) {
	h.now = h.now.Add(advance)
	if st.Since == "" {
		at := h.now
		if st.State == "running" || st.State == "lost_position" {
			at = h.now.Add(-time.Hour)
		}
		st.Since = at.Format(time.RFC3339)
	}
	h.status[id] = st
	h.w.runCycle()
}

var (
	stRunning  = console.MonitorStatus{State: "running"}
	stRetrying = console.MonitorStatus{State: "failed", Retrying: true, LastError: "dial tcp src:3306: connection refused (retrying)"}
	stPending  = console.MonitorStatus{State: "pending"}
	stGaveUp   = console.MonitorStatus{State: "failed", LastError: "Access denied for user 'u' (gave up after 6h0m0s of crash-looping; fix the issue, then press Start to retry)"}
	stStalled  = console.MonitorStatus{State: "stalled", LastError: "no progress for 5m1s"}
	stStopped  = console.MonitorStatus{State: "stopped"}
	stLostPos  = console.MonitorStatus{State: "lost_position", LastError: "gap"}
	stLockWait = console.MonitorStatus{State: "pending", Phase: monitorPhaseLockWaiting}
)

func TestCaptureWatcher_shortOutageIsSilent(t *testing.T) {
	h := newCaptureHarness()
	h.add("e1", "shop")
	h.cycle(0, "e1", stRetrying)
	h.cycle(time.Minute, "e1", stPending)
	h.cycle(5*time.Minute, "e1", stRetrying)
	h.cycle(time.Minute, "e1", stRunning)
	h.cycle(9*time.Minute, "e1", stRetrying)
	if len(h.f.events) != 0 {
		t.Fatalf("outages shorter than %s must not notify, got %+v", captureStoppedAfter, h.f.events)
	}
}

func TestCaptureWatcher_crashLoopFiresWarningOnceThenResolves(t *testing.T) {
	h := newCaptureHarness()
	h.add("e1", "shop")
	// failed → pending → failed: a crash loop never shows "running", so the
	// pending in between must not restart the clock.
	h.cycle(0, "e1", stRetrying)
	h.cycle(4*time.Minute, "e1", stPending)
	h.cycle(4*time.Minute, "e1", stRetrying)
	if len(h.f.events) != 0 {
		t.Fatalf("8 minutes down must not notify yet, got %+v", h.f.events)
	}
	h.cycle(2*time.Minute, "e1", stRetrying)
	if len(h.f.events) != 1 {
		t.Fatalf("10 minutes down: want one event, got %+v", h.f.events)
	}
	ev := h.f.events[0]
	if ev.Event != notify.EventCaptureStopped || ev.Severity != notify.SeverityWarning || ev.Server != "shop" || ev.Resolved {
		t.Fatalf("want warning capture_stopped for shop, got %+v", ev)
	}
	if !strings.Contains(ev.Details["error"], "connection refused") || ev.Details["down_for"] != "10m0s" {
		t.Fatalf("details must carry the error and how long: %+v", ev.Details)
	}
	if !strings.HasPrefix(ev.Summary, "capture has not run for 10 minutes;") {
		t.Fatalf("summary words the outage for a person: %q", ev.Summary)
	}
	h.cycle(time.Minute, "e1", stRetrying)
	h.cycle(time.Hour, "e1", stRetrying)
	if len(h.f.events) != 1 {
		t.Fatalf("a standing outage repeats once a day, not per cycle: %+v", h.f.events)
	}
	h.cycle(23*time.Hour, "e1", stRetrying)
	if len(h.f.events) != 2 || h.f.events[1].Severity != notify.SeverityWarning || !strings.Contains(h.f.events[1].Summary, "24h11m") {
		t.Fatalf("the daily reminder, saying how long: %+v", h.f.events)
	}
	h.f.events = h.f.events[:1]
	h.cycle(time.Minute, "e1", stRunning)
	if len(h.f.events) != 2 || !h.f.events[1].Resolved || h.f.events[1].Severity != notify.SeverityInfo {
		t.Fatalf("recovery: want one resolved info event, got %+v", h.f.events)
	}
	h.cycle(time.Minute, "e1", stRunning)
	if len(h.f.events) != 2 {
		t.Fatalf("resolved is sent once: %+v", h.f.events)
	}
}

func TestCaptureWatcher_gaveUpEscalatesToCritical(t *testing.T) {
	h := newCaptureHarness()
	h.add("e1", "shop")
	h.cycle(0, "e1", stRetrying)
	h.cycle(captureStoppedAfter, "e1", stRetrying)
	h.cycle(6*time.Hour, "e1", stGaveUp)
	if len(h.f.events) != 2 {
		t.Fatalf("want warning then critical, got %+v", h.f.events)
	}
	ev := h.f.events[1]
	if ev.Severity != notify.SeverityCritical || !strings.Contains(ev.Summary, "Start") {
		t.Fatalf("give-up: want critical saying to press Start, got %+v", ev)
	}
	// A transient retrying reading while the critical alert stands must not
	// downgrade it into a fresh warning page.
	h.cycle(time.Minute, "e1", stRetrying)
	if len(h.f.events) != 2 {
		t.Fatalf("no downgrade page while critical stands: %+v", h.f.events)
	}
	h.cycle(time.Minute, "e1", stRunning)
	if len(h.f.events) != 3 || !h.f.events[2].Resolved {
		t.Fatalf("one resolve closes both tiers: %+v", h.f.events)
	}
	h.cycle(time.Minute, "e1", stRunning)
	if len(h.f.events) != 3 {
		t.Fatalf("no second resolve for the other tier: %+v", h.f.events)
	}
}

func TestCaptureWatcher_setupFailureIsCriticalAfterTheWait(t *testing.T) {
	h := newCaptureHarness()
	h.add("e1", "shop")
	// Start's own setup failure (provisioning refused) is not retried: the
	// first page is already critical.
	h.cycle(0, "e1", stGaveUp)
	if len(h.f.events) != 0 {
		t.Fatalf("even a final failure waits out the window (a reload/boot can read as stopped): %+v", h.f.events)
	}
	h.cycle(captureStoppedAfter, "e1", stGaveUp)
	if len(h.f.events) != 1 || h.f.events[0].Severity != notify.SeverityCritical {
		t.Fatalf("want one critical, got %+v", h.f.events)
	}
}

func TestCaptureWatcher_stalledAndDesiredButStoppedAreCritical(t *testing.T) {
	// Nothing restarts either one by itself, so the page is critical and
	// says what to press, never "retrying by itself".
	for name, c := range map[string]struct {
		st   console.MonitorStatus
		says string
	}{
		"stalled":             {stStalled, "press Stop, then Start"},
		"desired but stopped": {stStopped, "press Start"},
	} {
		t.Run(name, func(t *testing.T) {
			h := newCaptureHarness()
			h.add("e1", "shop")
			h.cycle(0, "e1", c.st)
			h.cycle(captureStoppedAfter-time.Second, "e1", c.st)
			if len(h.f.events) != 0 {
				t.Fatalf("inside the window (boot, reload): silent, got %+v", h.f.events)
			}
			h.cycle(time.Second, "e1", c.st)
			if len(h.f.events) != 1 || h.f.events[0].Severity != notify.SeverityCritical {
				t.Fatalf("want one critical, got %+v", h.f.events)
			}
			if s := h.f.events[0].Summary; !strings.Contains(s, c.says) || strings.Contains(s, "retrying by itself") {
				t.Fatalf("summary must say %q and not claim a retry: %q", c.says, s)
			}
		})
	}
}

func TestCaptureWatcher_briefRunsInACrashLoopDoNotResetTheClock(t *testing.T) {
	h := newCaptureHarness()
	h.add("e1", "shop")
	young := func() console.MonitorStatus {
		return console.MonitorStatus{State: "running", Since: h.now.Add(-30 * time.Second).Format(time.RFC3339)}
	}
	// Each run reaches its first checkpoint, reads "running" for under a
	// minute, and dies.
	h.cycle(0, "e1", stRetrying)
	for range 4 {
		h.now = h.now.Add(time.Minute)
		h.cycle(0, "e1", young())
		h.cycle(time.Minute, "e1", stRetrying)
	}
	h.cycle(2*time.Minute, "e1", stRetrying)
	if len(h.f.events) != 1 || h.f.events[0].Severity != notify.SeverityWarning {
		t.Fatalf("a crash loop with brief runs still pages after the window: %+v", h.f.events)
	}
	h.cycle(time.Minute, "e1", young())
	if len(h.f.events) != 1 {
		t.Fatalf("a run younger than %s does not resolve: %+v", captureHealthyAfter, h.f.events)
	}
	h.cycle(captureHealthyAfter, "e1", stRunning)
	if len(h.f.events) != 2 || !h.f.events[1].Resolved {
		t.Fatalf("a run past %s resolves: %+v", captureHealthyAfter, h.f.events)
	}
}

func TestCaptureWatcher_stuckStartingPages(t *testing.T) {
	h := newCaptureHarness()
	h.add("e1", "shop")
	started := h.now
	pend := func() console.MonitorStatus {
		return console.MonitorStatus{State: "pending", Phase: "cleanup", Since: started.Format(time.RFC3339)}
	}
	h.cycle(0, "e1", pend())
	h.cycle(captureStartingLimit-time.Minute, "e1", pend())
	if len(h.f.events) != 0 {
		t.Fatalf("starting under %s is not an outage: %+v", captureStartingLimit, h.f.events)
	}
	h.cycle(time.Minute, "e1", pend())
	h.cycle(captureStoppedAfter, "e1", pend())
	if len(h.f.events) != 1 || h.f.events[0].Severity != notify.SeverityWarning || h.f.events[0].Details["phase"] != "cleanup" {
		t.Fatalf("stuck starting: want a warning naming the step, got %+v", h.f.events)
	}
	if !strings.Contains(h.f.events[0].Summary, "starting for over") {
		t.Fatalf("summary: %q", h.f.events[0].Summary)
	}
}

func TestCaptureWatcher_neutralAndHealthyStates(t *testing.T) {
	// lost_position is capture running (its loss has its own event), and a
	// lock wait means another process holds the source: neither is "stopped".
	for name, st := range map[string]console.MonitorStatus{"lost position": stLostPos, "lock wait": stLockWait, "pending": stPending} {
		t.Run(name, func(t *testing.T) {
			h := newCaptureHarness()
			h.add("e1", "shop")
			h.cycle(0, "e1", st)
			h.cycle(time.Hour, "e1", st)
			if len(h.f.events) != 0 {
				t.Fatalf("%s must never page, got %+v", name, h.f.events)
			}
		})
	}
	// lost_position counts as capturing: it ends the outage clock.
	h := newCaptureHarness()
	h.add("e1", "shop")
	h.cycle(0, "e1", stRetrying)
	h.cycle(9*time.Minute, "e1", stLostPos)
	h.cycle(2*time.Minute, "e1", stRetrying)
	if len(h.f.events) != 0 {
		t.Fatalf("lost_position is a running stream and resets the clock: %+v", h.f.events)
	}
}

func TestCaptureWatcher_stoppedOnPurposeOrRemovedResolves(t *testing.T) {
	down := func(h *captureHarness) {
		h.cycle(0, "e1", stRetrying)
		h.cycle(captureStoppedAfter, "e1", stRetrying)
		if len(h.f.events) != 1 {
			t.Fatalf("setup: want one warning, got %+v", h.f.events)
		}
	}
	t.Run("stopped from the console", func(t *testing.T) {
		h := newCaptureHarness()
		h.add("e1", "shop")
		down(h)
		h.entries[0].MonitorDesired = false
		h.cycle(time.Minute, "e1", stStopped)
		if len(h.f.events) != 2 || !h.f.events[1].Resolved || !strings.Contains(h.f.events[1].Summary, "stopped") {
			t.Fatalf("want a resolve that says it was stopped, got %+v", h.f.events)
		}
		h.entries[0].MonitorDesired = true
		h.cycle(time.Minute, "e1", stRetrying)
		h.cycle(9*time.Minute, "e1", stRetrying)
		if len(h.f.events) != 2 {
			t.Fatalf("Start again restarts the clock from zero: %+v", h.f.events)
		}
	})
	t.Run("removed", func(t *testing.T) {
		h := newCaptureHarness()
		h.add("e1", "shop")
		down(h)
		h.entries = nil
		h.cycle(time.Minute, "e1", stStopped)
		if len(h.f.events) != 2 || !h.f.events[1].Resolved || h.f.events[1].Server != "shop" || !strings.Contains(h.f.events[1].Summary, "removed") {
			t.Fatalf("want a resolve naming the removed server, got %+v", h.f.events)
		}
	})
	t.Run("never alerted: nothing to say", func(t *testing.T) {
		h := newCaptureHarness()
		h.add("e1", "shop")
		h.cycle(0, "e1", stRetrying)
		h.entries = nil
		h.cycle(time.Minute, "e1", stStopped)
		if len(h.f.events) != 0 {
			t.Fatalf("no alert stood, so no resolve: %+v", h.f.events)
		}
	})
}

func TestCaptureWatcher_keyedByIDNotName(t *testing.T) {
	h := newCaptureHarness()
	h.add("e1", "shop")
	h.add("e2", "blog")
	h.status["e2"] = stRunning
	h.cycle(0, "e1", stRetrying)
	h.cycle(captureStoppedAfter, "e1", stRetrying)
	// A rename mid-outage is the same outage: no second page.
	h.entries[0].Name = "shop-eu"
	h.cycle(time.Minute, "e1", stRetrying)
	if len(h.f.events) != 1 || h.f.events[0].Server != "shop" {
		t.Fatalf("want one page for e1 only, got %+v", h.f.events)
	}
	h.cycle(time.Minute, "e1", stRunning)
	if len(h.f.events) != 2 || h.f.events[1].Server != "shop-eu" {
		t.Fatalf("the resolve names the server as it is called now: %+v", h.f.events)
	}
}

func TestHumanDown(t *testing.T) {
	for d, want := range map[time.Duration]string{
		time.Minute: "1 minute", 10 * time.Minute: "10 minutes", 59*time.Minute + 20*time.Second: "59 minutes",
		59*time.Minute + 40*time.Second: "1h00m", 6*time.Hour + 10*time.Minute: "6h10m", 30 * time.Hour: "30h00m",
	} {
		if got := humanDown(d); got != want {
			t.Errorf("humanDown(%s) = %q, want %q", d, got, want)
		}
	}
}

func TestCaptureWatcher_entryWithoutSourceIsSkipped(t *testing.T) {
	h := newCaptureHarness()
	// A server registered to read an index only has no capture to stop.
	h.entries = []console.ServerEntry{{ID: "e1", Name: "ro", DSN: "u:p@tcp(idx:3306)/x", MonitorDesired: true}}
	h.cycle(0, "e1", stStopped)
	h.cycle(time.Hour, "e1", stStopped)
	if len(h.f.events) != 0 {
		t.Fatalf("no source, no capture to alert on: %+v", h.f.events)
	}
}
