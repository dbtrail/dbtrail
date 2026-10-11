package consoleapp

import (
	"bytes"
	"context"
	"log/slog"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/metadata"
)

// waitForSnapshotJob polls a server's schema-snapshot status until it leaves
// "running".
//
// Polling rather than a channel because the goroutine under test dies by
// panic: it never reaches a clean signal. The read goes through the same mutex
// the guard writes under, so observing a terminal state also orders the test's
// later reads after the goroutine's last write.
func waitForSnapshotJob(t *testing.T, s *schemaSnapshotSupervisor, serverID string) console.SchemaSnapshotStatus {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		st := s.Status(serverID)
		if st.State != "running" {
			return st
		}
		if time.Now().After(deadline) {
			t.Fatalf("the job never left the running state: %+v; the goroutine vanished and Trigger will "+
				"refuse every later snapshot for this server until the daemon restarts", st)
		}
		time.Sleep(time.Millisecond)
	}
}

// TestSchemaSnapshotGoroutines_survivePanicAndReportFailure drives each of the
// two schema-snapshot goroutines to a panic INSIDE the goroutine, not on the
// dispatch side of the `go` that starts it, and asserts the daemon survives,
// the failure is reported, and the endpoint is usable again.
//
// Without the guards each subtest kills the whole test binary, which is what
// the panic does to a `watch` daemon that is also capturing.
//
// The assertion is on the reported failure, not merely on the absence of a
// crash: a subtest cannot pass by the goroutine simply never having run, since
// that leaves the slot "running" with an empty LastError and waitForSnapshotJob
// fails.
func TestSchemaSnapshotGoroutines_survivePanicAndReportFailure(t *testing.T) {
	for _, tc := range []struct {
		name string
		// setup makes the named goroutine panic and returns the substring the
		// reported error must carry.
		setup func(s *schemaSnapshotSupervisor) string
		// forbid must NOT appear in the reported error.
		forbid string
	}{
		{
			name: "snapshot goroutine",
			setup: func(s *schemaSnapshotSupervisor) string {
				const sentinel = "induced panic while taking the snapshot"
				s.snapshotFn = func(console.SchemaSnapshotRequest) (metadata.SnapshotStats, error) {
					panic(sentinel)
				}
				return sentinel
			},
			// A panic that is only recovered, without being routed into the
			// channel run selects on, would leave run waiting out s.timeout
			// and then reporting the source as unresponsive: the daemon's own
			// internal error, blamed on a lock the source is not holding.
			forbid: "did not answer within",
		},
		{
			name: "run goroutine",
			setup: func(s *schemaSnapshotSupervisor) string {
				// An arbitrary panic site inside run's OWN frames: a select
				// evaluates every channel operand on entry, so a nil context
				// makes s.ctx.Done() panic there. The guard must not care
				// where the panic came from; this is simply the cheapest site
				// a unit test can reach without adding a seam for publish.
				s.ctx = nil
				return "internal error:"
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := testSnapshotSupervisor(t, okSnapshot, func(context.Context, string) (bool, error) {
				return true, nil
			})
			// Short, so a panic that is recovered but never reported fails
			// here in seconds instead of hanging for the production timeout.
			s.timeout = 2 * time.Second
			want := tc.setup(s)

			req := console.SchemaSnapshotRequest{ServerID: "srv-1", ServerName: "srv"}
			if err := s.Trigger(req); err != nil {
				t.Fatalf("trigger: %v", err)
			}
			st := waitForSnapshotJob(t, s, "srv-1")

			if st.State != "failed" {
				t.Errorf("state = %q, want failed: a job whose goroutine died must not report anything else", st.State)
			}
			if !strings.Contains(st.LastError, want) {
				t.Errorf("last_error = %q, want it to carry %q; the operator has to be able to see that the "+
					"job died rather than watch it never finish", st.LastError, want)
			}
			if tc.forbid != "" && strings.Contains(st.LastError, tc.forbid) {
				t.Errorf("last_error = %q; it blames the source for an internal error", st.LastError)
			}
			if st.FinishedAt == "" {
				t.Error("finished_at is empty; the run is reported as still open")
			}

			// Trigger refuses while this server's slot reads "running". A
			// guard that logged and left it there would refuse this server's
			// schema refresh until the daemon restarts.
			if err := s.Trigger(req); err != nil {
				t.Fatalf("a second snapshot was refused after the first one panicked (%v); the endpoint is "+
					"wedged for this server until the daemon restarts", err)
			}
			waitForSnapshotJob(t, s, "srv-1")
		})
	}
}

// guardedPanic runs the guard the run goroutine registers, over a body that
// panics.
//
// It calls s.runPanicReporter, the SAME closure run defers, rather than
// rebuilding one that looks like it. A local copy is a twin, not the wiring:
// with one, swapping failIfRunning for publish at run's defer passed this
// whole file.
func guardedPanic(s *schemaSnapshotSupervisor, req console.SchemaSnapshotRequest, gen uint64, body func()) {
	defer recoverSnapshotJob(req, s.runPanicReporter(req, gen))
	body()
}

// TestRecoverSnapshotJob_releasesTheLockAPanicWasHoldingIt: publish writes the
// job's outcome inside `s.mu.Lock(); defer s.mu.Unlock()`, so a panic can be
// raised while the supervisor mutex is held. Deferred functions run
// last-in-first-out, so that unlock fires before the guard and the guard can
// take the mutex it needs. Pinned here because registering the guard AFTER the
// lock instead would deadlock the supervisor for good, and a deadlock is a
// worse outage than the crash this replaces.
func TestRecoverSnapshotJob_releasesTheLockAPanicWasHoldingIt(t *testing.T) {
	s := testSnapshotSupervisor(t, okSnapshot, nil)
	req := console.SchemaSnapshotRequest{ServerID: "a", ServerName: "srv"}
	s.jobs["a"] = &console.SchemaSnapshotStatus{State: "running", Since: nowStamp()}
	s.gens["a"] = 1

	guardedPanic(s, req, 1, func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		panic("raised while holding the supervisor mutex")
	})

	// Hangs here instead of failing if the guard ever deadlocks.
	if got := s.Status("a"); got.State != "failed" {
		t.Fatalf("state = %q, want failed", got.State)
	}
}

// TestRecoverSnapshotJob_doesNotRewriteAFinishedRun pins the still-running
// check, which is DEFENSIVE here rather than live: publish in this file logs
// first and writes the map last, and its log sits inside `if err != nil`, so
// no reachable panic site leaves the slot terminal with a matching generation.
// The sibling guard in baseline_job_guard.go has the live version of the
// hazard, because those jobs write and THEN log. Pinned anyway, cheaply, so
// that adding a step after publish cannot silently turn a run whose snapshot is
// already recorded into a failure and send the operator to redo it.
func TestRecoverSnapshotJob_doesNotRewriteAFinishedRun(t *testing.T) {
	s := testSnapshotSupervisor(t, okSnapshot, nil)
	req := console.SchemaSnapshotRequest{ServerID: "a", ServerName: "srv"}
	s.jobs["a"] = &console.SchemaSnapshotStatus{
		State: "succeeded", SnapshotID: 7, Tables: 12, StreamReloaded: true, FinishedAt: nowStamp(),
	}
	s.gens["a"] = 1

	guardedPanic(s, req, 1, func() { panic("raised after the run published") })

	got := s.Status("a")
	if got.State != "succeeded" || got.SnapshotID != 7 || got.Tables != 12 || !got.StreamReloaded {
		t.Fatalf("status = %+v; the guard overwrote a run that had already published its snapshot", got)
	}
	if got.LastError != "" {
		t.Fatalf("last_error = %q on a succeeded run", got.LastError)
	}
}

// TestRecoverSnapshotJob_dropsASupersededRun: a run that timed out and was
// retried no longer owns this server's slot. The newer run has reset it to
// "running", so a still-running check alone would let the stale guard fail the
// run that owns it now, and the operator would watch a snapshot they just
// started report an error from the previous one.
func TestRecoverSnapshotJob_dropsASupersededRun(t *testing.T) {
	s := testSnapshotSupervisor(t, okSnapshot, nil)
	req := console.SchemaSnapshotRequest{ServerID: "a", ServerName: "srv"}
	s.jobs["a"] = &console.SchemaSnapshotStatus{State: "running", Since: nowStamp()}
	s.gens["a"] = 2 // a newer run owns the slot

	guardedPanic(s, req, 1, func() { panic("raised by a superseded run") })

	if got := s.Status("a"); got.State != "running" || got.LastError != "" {
		t.Fatalf("status = %+v; a superseded run failed the newer one that owns the slot", got)
	}
}

// TestRecoverSnapshotJob_passesThroughWithoutAPanic: the guard runs on the
// success path of both goroutines too, and must be inert there.
func TestRecoverSnapshotJob_passesThroughWithoutAPanic(t *testing.T) {
	s := testSnapshotSupervisor(t, okSnapshot, nil)
	req := console.SchemaSnapshotRequest{ServerID: "a", ServerName: "srv"}
	s.jobs["a"] = &console.SchemaSnapshotStatus{State: "running", Since: nowStamp()}
	s.gens["a"] = 1

	reported := false
	func() {
		defer recoverSnapshotJob(req, func(error) { reported = true })
	}()

	if reported {
		t.Error("the guard reported a failure for a goroutine that never panicked")
	}
	if got := s.Status("a"); got.State != "running" {
		t.Fatalf("state = %q, want running: the guard touched a job that never panicked", got.State)
	}
}

// TestSchemaSnapshotPanic_scrubsTheDSN: a panic value can carry a driver error,
// and a driver error commonly embeds the whole connection string. LastError is
// served over HTTP.
func TestSchemaSnapshotPanic_scrubsTheDSN(t *testing.T) {
	const dsn = "root:hunter2@tcp(127.0.0.1:3306)/app"
	s := testSnapshotSupervisor(t, okSnapshot, nil)
	req := console.SchemaSnapshotRequest{ServerID: "a", ServerName: "srv", SourceDSN: dsn}
	s.jobs["a"] = &console.SchemaSnapshotStatus{State: "running", Since: nowStamp()}
	s.gens["a"] = 1

	guardedPanic(s, req, 1, func() { panic("dial " + dsn + ": refused") })

	got := s.Status("a")
	if strings.Contains(got.LastError, "hunter2") || strings.Contains(got.LastError, dsn) {
		t.Fatalf("last_error = %q; it carries the source DSN to an HTTP client", got.LastError)
	}
}

// TestRecoverSnapshotJob_logsTheStackAtErrorLevel: half the standard this guard
// is held to lives in the log. Once the process stops dying on the panic, that
// error line is the ONLY record of where it came from, so a guard that reported
// the failure and logged nothing would still be a regression. Nothing else in
// the file fails if the slog.Error is deleted.
func TestRecoverSnapshotJob_logsTheStackAtErrorLevel(t *testing.T) {
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(prev) })

	req := console.SchemaSnapshotRequest{ServerID: "a", ServerName: "srv"}
	func() {
		defer recoverSnapshotJob(req, func(error) {})
		panic("induced panic that must reach the log")
	}()

	out := buf.String()
	if !strings.Contains(out, "level=ERROR") {
		t.Errorf("the panic was not logged at error level: %s", out)
	}
	if !strings.Contains(out, "induced panic that must reach the log") {
		t.Errorf("the panic value is missing from the log: %s", out)
	}
	// The stack is the point: the panic site is recorded nowhere else now.
	if !strings.Contains(out, "stack=") || !strings.Contains(out, "recoverSnapshotJob") {
		t.Errorf("the log carries no stack trace: %s", out)
	}
}

// TestSchemaSnapshotPanic_doesNotCryWolfAboutCapture: the capture warning must
// fire only for a panic that could actually have stopped capture. A panic while
// READING the source's columns cannot have: execute reaches the restart only
// after the snapshot returns. Warning anyway would send an operator to check a
// stream nothing touched, on the one surface that has to be trustworthy during
// an incident.
func TestSchemaSnapshotPanic_doesNotCryWolfAboutCapture(t *testing.T) {
	reloadCalled := false
	s := testSnapshotSupervisor(t, func(console.SchemaSnapshotRequest) (metadata.SnapshotStats, error) {
		panic("induced panic while reading the source columns")
	}, func(context.Context, string) (bool, error) {
		reloadCalled = true
		return true, nil
	})
	s.timeout = 2 * time.Second

	req := console.SchemaSnapshotRequest{ServerID: "srv-1", ServerName: "srv"}
	if err := s.Trigger(req); err != nil {
		t.Fatalf("trigger: %v", err)
	}
	st := waitForSnapshotJob(t, s, "srv-1")

	if reloadCalled {
		t.Fatal("the premise is broken: the restart ran, so this panic COULD have stopped capture")
	}
	if strings.Contains(st.LastError, "press Start") || strings.Contains(st.LastError, "restarting capture") {
		t.Errorf("last_error = %q; it warns about capture for a panic raised before the restart was reached", st.LastError)
	}
	if !strings.Contains(st.LastError, "Capture for this server was not touched") {
		t.Errorf("last_error = %q; it should say what IS known about capture", st.LastError)
	}
}

// TestSchemaSnapshotPanic_reportsTheSnapshotItTook: a panic during the restart
// arrives with the snapshot already written to the index. Reporting a bare
// failure would have the operator hunt for a snapshot that is already there,
// and re-run a step that worked.
func TestSchemaSnapshotPanic_reportsTheSnapshotItTook(t *testing.T) {
	s := testSnapshotSupervisor(t, okSnapshot, func(context.Context, string) (bool, error) {
		panic("induced panic while restarting capture")
	})
	s.timeout = 2 * time.Second

	req := console.SchemaSnapshotRequest{ServerID: "srv-1", ServerName: "srv"}
	if err := s.Trigger(req); err != nil {
		t.Fatalf("trigger: %v", err)
	}
	st := waitForSnapshotJob(t, s, "srv-1")

	if st.State != "failed" {
		t.Errorf("state = %q, want failed: an internal error is not a success", st.State)
	}
	if st.SnapshotID != 7 || st.Tables != 12 {
		t.Errorf("status = %+v; the snapshot that WAS taken is not reported", st)
	}
	if !strings.Contains(st.LastError, "taken and recorded") {
		t.Errorf("last_error = %q; it does not say the snapshot itself succeeded", st.LastError)
	}
	if !strings.Contains(st.LastError, "Manage servers") || !strings.Contains(st.LastError, "press Start") {
		t.Errorf("last_error = %q; the remedy must name the page that actually has the control", st.LastError)
	}
}

// TestSchemaSnapshotPanic_afterTheTimeoutReplacesTheSourceBlame: a snapshot that
// outlives s.timeout and THEN panics leaves run's timeout text standing, which
// tells the operator the source is holding a metadata lock when the daemon is
// what broke, and drops the capture warning entirely. The reachable ordering,
// not a race: the slot went terminal long before the panic.
func TestSchemaSnapshotPanic_afterTheTimeoutReplacesTheSourceBlame(t *testing.T) {
	release := make(chan struct{})
	s := testSnapshotSupervisor(t, func(console.SchemaSnapshotRequest) (metadata.SnapshotStats, error) {
		<-release
		panic("induced panic after the job had already timed out")
	}, nil)
	s.timeout = 50 * time.Millisecond

	req := console.SchemaSnapshotRequest{ServerID: "srv-1", ServerName: "srv"}
	if err := s.Trigger(req); err != nil {
		t.Fatalf("trigger: %v", err)
	}
	timedOut := waitForSnapshotJob(t, s, "srv-1")
	if !strings.Contains(timedOut.LastError, "did not answer within") {
		t.Fatalf("the premise is broken: the run did not report a timeout first, got %q", timedOut.LastError)
	}

	close(release)
	deadline := time.Now().Add(10 * time.Second)
	for {
		st := s.Status("srv-1")
		if strings.Contains(st.LastError, "induced panic after the job had already timed out") {
			if st.State != "failed" {
				t.Errorf("state = %q, want failed", st.State)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("last_error = %q; the timeout text still blames the source for this daemon's own "+
				"internal error, and the panic exists only in the log", st.LastError)
		}
		time.Sleep(time.Millisecond)
	}
}

// TestMonitorStart_panicLeavesTheSlotRetryable: Start reserves the entry as
// "pending" before provisioning, and #1497 made a panic during provisioning
// SURVIVABLE for the first time (the schema-snapshot refresh reaches Start
// through ReloadSchema, and its goroutines now recover). Nothing heals a stuck
// "pending": Start is idempotent on it, snapshot() ages out only "running", and
// Reconcile goes through Start. The servers list even counts "pending" as live,
// so it offers Stop where the remedy this PR reports says Start. So the
// reserved slot has to go terminal before the panic travels on.
//
// The re-panic is asserted too: for every caller that does NOT guard, a panic
// in Start must stay exactly as loud as it is today.
//
// dbNameRE is the panic site because it is the first thing Start touches after
// reserving the slot and before any I/O. The site is arbitrary; the guard must
// not care where the panic came from. Mutating a package var is this package's
// established pattern (monitorReloadDrainTimeout) and safe because nothing here
// runs in parallel.
func TestMonitorStart_panicLeavesTheSlotRetryable(t *testing.T) {
	prev := dbNameRE
	t.Cleanup(func() { dbNameRE = prev })

	m := newMonitorSupervisor(context.Background(), "", nil, nil)
	e := console.ServerEntry{ID: "srv-1", Name: "srv",
		SourceDSN: "u:p@tcp(127.0.0.1:3306)/app", DSN: "u:p@tcp(127.0.0.1:3306)/idx"}

	dbNameRE = nil // nil receiver: MatchString dereferences it
	panicked := false
	func() {
		defer func() {
			if r := recover(); r != nil {
				panicked = true
			}
		}()
		_ = m.Start(context.Background(), e)
	}()

	if !panicked {
		t.Fatal("the panic was swallowed inside Start; an unguarded caller must still see it")
	}
	st := m.Status("srv-1")
	if st.State == "pending" {
		t.Fatalf("state = pending after a panic: nothing ever heals that, so this server's capture is off "+
			"for the life of the daemon and the servers list offers Stop rather than Start (%+v)", st)
	}
	if st.State != "failed" {
		t.Errorf("state = %q, want failed", st.State)
	}
	if !strings.Contains(st.LastError, "internal error") {
		t.Errorf("last_error = %q; it does not say an internal error ended the start", st.LastError)
	}

	// The remedy has to work. A regexp that refuses every name makes the retry
	// fail at the name check, with no I/O: what matters is that Start got PAST
	// the idempotent switch at all. On a "pending" slot it returns nil instead.
	dbNameRE = regexp.MustCompile(`^$`)
	if err := m.Start(context.Background(), e); err == nil {
		t.Error("Start reported success without doing anything: a slot left non-terminal makes every later " +
			"Start a silent no-op, which is exactly the wedge this guards")
	}
}
