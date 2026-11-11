package consoleapp

import (
	"context"
	"fmt"
	"log/slog"
	"runtime/debug"
	"time"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/notify"
)

// captureStoppedAfter is how long a supervised source must go without
// capturing before capture_stopped fires (#2635). Every down reading waits it
// out, a final failure included: a source reads "stopped" for a few seconds at
// boot (before Reconcile starts it) and during a schema reload, and a page for
// either would be a false alarm. Short next to how long a source keeps its
// binlogs, which is what the warning is racing.
var captureStoppedAfter = 10 * time.Minute

// capturePollInterval is how often the watcher reads each source's state.
// The read is in-memory (the supervisor's job table), so it is cheap.
const capturePollInterval = time.Minute

// captureWatcher turns the supervisor's per-source states into the
// capture_stopped webhook event. It covers the sources added from the console
// (the registry). The --source-dsn main stream is not covered: when it gives
// up the whole daemon exits, which the container or service manager sees.
type captureWatcher struct {
	n       *watchNotifier
	entries func() []console.ServerEntry
	status  func(entryID string) console.MonitorStatus
	now     func() time.Time

	// downSince is, per entry ID, the first reading of the current outage.
	// Only a reading that shows capture running clears it: a crash loop goes
	// failed → pending → failed and never shows running, so pending must not
	// restart the clock.
	downSince map[string]time.Time
	// names is the display name of every entry with an alert standing, for
	// the resolve sent after the entry is gone from the registry.
	names map[string]string
}

func startCaptureWatch(ctx context.Context, n *watchNotifier, registry *console.Registry, sup *monitorSupervisor) {
	if n == nil || registry == nil || sup == nil {
		return
	}
	w := &captureWatcher{n: n, entries: registry.List, status: sup.Status, now: time.Now}
	go func() {
		tick := time.NewTicker(capturePollInterval)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
				w.runCycle()
			}
		}
	}()
}

// captureHealthyAfter is how long a stream must have been running before a
// reading of it ends an outage. A crash loop whose runs reach their first
// checkpoint and then die reads "running" for seconds at a time; counting
// each such reading as recovery would reset the clock on every one, so the
// alert would never fire (and a standing one would resolve and re-fire).
var captureHealthyAfter = 5 * time.Minute

// captureStartingLimit is how long a stream may stay "pending" (connecting,
// or in a startup step such as the resume cleanup) before it counts as down.
// Generous on purpose: a large resume cleanup is legitimately slow.
var captureStartingLimit = 30 * time.Minute

// captureReading is what one status reading means for the outage clock.
type captureReading int

const (
	captureNeutral  captureReading = iota // starting, a run too young to count, or waiting for another process's lock
	captureUp                             // running for captureHealthyAfter (lost_position included: the stream runs; its loss has its own event)
	captureRetrying                       // failing, and the supervisor will try again by itself
	captureStarting                       // pending past captureStartingLimit
	captureStalled                        // connected but not advancing; nothing restarts it
	captureGaveUp                         // failed, and nothing will retry until someone presses Start
	captureStopped                        // not running although the operator wants it capturing; nothing will start it
)

// final reports whether nothing will bring capture back by itself, which is
// what makes the alert critical.
func (r captureReading) final() bool {
	return r == captureStalled || r == captureGaveUp || r == captureStopped
}

func readCapture(st console.MonitorStatus, now time.Time) captureReading {
	since, sinceErr := time.Parse(time.RFC3339, st.Since)
	switch st.State {
	case "running", "lost_position":
		if sinceErr == nil && now.Sub(since) < captureHealthyAfter {
			return captureNeutral
		}
		return captureUp
	case "failed":
		if st.Retrying {
			return captureRetrying
		}
		return captureGaveUp
	case "stalled":
		return captureStalled
	case "stopped":
		// The operator wants it capturing (the caller checked): a schema
		// reload that did not restart it, a run that ended without an error,
		// or an entry Start refused before it reserved a job. Also every
		// source for the few seconds before boot starts it, which the window
		// absorbs.
		return captureStopped
	case "pending":
		if st.Phase == monitorPhaseLockWaiting {
			// Another process holds the source and captures it; its own
			// watcher is the one to page.
			return captureNeutral
		}
		if sinceErr == nil && now.Sub(since) >= captureStartingLimit {
			return captureStarting
		}
	}
	return captureNeutral
}

// captureSummary is the message for one kind of outage: what is happening,
// whether it fixes itself, and what to press when it does not.
func captureSummary(r captureReading, down time.Duration) string {
	keep := "; the source must keep its binlogs until capture catches up"
	switch r {
	case captureGaveUp:
		return "capture has stopped and will NOT retry by itself: fix the cause, then press Start for this server in the console (or restart DBTrail)" + keep
	case captureStopped:
		return "capture is not running for this server and nothing will start it: press Start for it in the console" + keep
	case captureStalled:
		return "capture is connected but has not advanced for " + humanDown(down) + ", and DBTrail does not restart a stuck stream by itself: press Stop, then Start for this server in the console" + keep
	case captureStarting:
		return "capture has been starting for over " + humanDown(down) + " without capturing anything (see the server's row in the console for the step it is in)" + keep
	}
	return "capture has not run for " + humanDown(down) + "; it is retrying by itself" + keep
}

func (w *captureWatcher) runCycle() {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("capture watch cycle panicked; watching continues next tick", "panic", r, "stack", string(debug.Stack()))
		}
	}()
	if w.downSince == nil {
		w.downSince = map[string]time.Time{}
		w.names = map[string]string{}
	}
	now := w.now()
	seen := map[string]bool{}
	for _, e := range w.entries() {
		if e.SourceDSN == "" {
			continue // reads an index only: there is no capture to stop
		}
		seen[e.ID] = true
		if !e.MonitorDesired {
			w.forget(e.ID, e.Name, "capture was stopped from the console; this server is no longer watched for stopped capture")
			continue
		}
		st := w.status(e.ID)
		reading := readCapture(st, now)
		switch reading {
		case captureUp:
			delete(w.downSince, e.ID)
			if w.n.resolveCapture(e.ID) {
				delete(w.names, e.ID)
				w.n.send.Notify(notify.Event{
					Event: notify.EventCaptureStopped, Severity: notify.SeverityInfo, Server: e.Name, Resolved: true,
					Summary: "capture is running again",
				})
			}
			continue
		case captureNeutral:
			continue
		}
		since, ok := w.downSince[e.ID]
		if !ok {
			w.downSince[e.ID] = now
			since = now
		}
		down := now.Sub(since)
		if down < captureStoppedAfter {
			continue
		}
		if w.n.CaptureStopped(e.Name, e.ID, reading, st, down) {
			w.names[e.ID] = e.Name
		}
	}
	for id := range w.downSince {
		if !seen[id] {
			delete(w.downSince, id)
		}
	}
	for id, name := range w.names {
		if !seen[id] {
			w.forget(id, name, "the server was removed from the console; it is no longer watched for stopped capture")
		}
	}
}

// forget ends the watch of one entry, sending a resolve when an alert stood:
// an incident opened by capture_stopped must be closed by something, and the
// summary says it was closed by a decision, not by capture coming back.
func (w *captureWatcher) forget(id, name, summary string) {
	delete(w.downSince, id)
	if w.n.resolveCapture(id) {
		w.n.send.Notify(notify.Event{
			Event: notify.EventCaptureStopped, Severity: notify.SeverityInfo, Server: name, Resolved: true,
			Summary: summary,
		})
	}
	delete(w.names, id)
}

// captureKey is the edge key of one entry's capture_stopped tier. Keyed on the
// entry ID, which survives renames; the severity is part of the key so the
// escalation to critical is a new transition, as verify does.
func captureKey(sev, entryID string) string { return "capture:" + sev + ":" + entryID }

// resolveCapture clears both tiers of one entry, reporting whether either was
// standing.
func (n *watchNotifier) resolveCapture(entryID string) bool {
	resolved := n.edge.Resolve(captureKey(notify.SeverityCritical, entryID))
	if n.edge.Resolve(captureKey(notify.SeverityWarning, entryID)) {
		resolved = true
	}
	return resolved
}

// CaptureStopped notifies that one source has not captured for down, and
// reports whether an alert now stands. A reading after which nothing brings
// capture back by itself is critical; one the supervisor still retries is a
// warning. Severity only goes up while an outage lasts: once critical stands,
// a retrying reading keeps the critical tier (its daily reminder included)
// rather than paging again as a fresh warning, and its text says it is
// retrying now.
func (n *watchNotifier) CaptureStopped(server, entryID string, r captureReading, st console.MonitorStatus, down time.Duration) bool {
	critKey := captureKey(notify.SeverityCritical, entryID)
	sev := notify.SeverityWarning
	if r.final() || n.edge.Active(critKey) {
		sev = notify.SeverityCritical
	}
	if !n.edge.Fire(captureKey(sev, entryID), "") {
		return true
	}
	if sev == notify.SeverityCritical {
		n.edge.Resolve(captureKey(notify.SeverityWarning, entryID))
	}
	details := map[string]string{"state": st.State, "down_for": down.Round(time.Second).String()}
	if st.LastError != "" {
		details["error"] = st.LastError
	}
	if st.Phase != "" {
		details["phase"] = st.Phase
	}
	n.send.Notify(notify.Event{
		Event: notify.EventCaptureStopped, Severity: sev, Server: server, Summary: captureSummary(r, down), Details: details,
	})
	return true
}

// humanDown words an outage for a person: "10 minutes", or "6h10m" past an
// hour. down_for in the details keeps Go's format for machines.
func humanDown(d time.Duration) string {
	m := int(d.Round(time.Minute) / time.Minute)
	if m < 60 {
		if m == 1 {
			return "1 minute"
		}
		return fmt.Sprintf("%d minutes", m)
	}
	return fmt.Sprintf("%dh%02dm", m/60, m%60)
}
