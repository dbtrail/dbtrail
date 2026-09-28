package consoleapp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/streamrun"
)

// earlierCleanupErr is the failure as streamrun.One returns it.
func earlierCleanupErr() error {
	return &streamrun.EarlierCleanupError{ConnectionID: 812, Running: 14 * time.Minute, Waited: time.Hour, Count: 1}
}

// lockWaitErr is what a second cleanup returned before #1708, wrapped the way
// One wraps it.
func lockWaitErr() error {
	raw := &mysql.MySQLError{Number: 1205, Message: "Lock wait timeout exceeded; try restarting transaction"}
	return fmt.Errorf("failed to dedup events since checkpoint: %w",
		fmt.Errorf("delete events since checkpoint binlog.000042:5000: %w", raw))
}

// TestMonitorWaitingPhaseCarriesItsDetail: while a start waits for an earlier
// cleanup the row stays "pending", names the phase, and says which connection
// it waits on. The detail goes when the phase goes.
func TestMonitorWaitingPhaseCarriesItsDetail(t *testing.T) {
	job := &monitorJob{}
	job.set("pending", "")
	hooks := job.streamHooks()

	hooks.OnPhase(streamrun.PhaseResumeCleanupWaiting)
	hooks.OnPhaseDetail("connection 812, running for 14m0s")
	st := job.snapshot()
	if st.State != "pending" || st.Phase != "resume_cleanup_waiting" || st.PhaseDetail != "connection 812, running for 14m0s" {
		t.Fatalf("while waiting: %+v", st)
	}
	// The wire keys the page reads.
	raw, _ := json.Marshal(st)
	for _, want := range []string{`"phase":"resume_cleanup_waiting"`, `"phase_detail":"connection 812, running for 14m0s"`} {
		if !strings.Contains(string(raw), want) {
			t.Errorf("the status document lacks %s: %s", want, raw)
		}
	}

	// The wait ended and this run's own cleanup began: a new phase, and the
	// old one's detail must not describe it.
	hooks.OnPhase("")
	hooks.OnPhaseDetail("")
	hooks.OnPhase(streamrun.PhaseResumeCleanup)
	if st := job.snapshot(); st.Phase != streamrun.PhaseResumeCleanup || st.PhaseDetail != "" {
		t.Fatalf("after the wait: %+v, want the cleanup phase with no detail", st)
	}

	for _, tc := range []struct {
		name string
		move func()
	}{
		{"a failure the supervisor will retry", func() { job.fail("boom (retrying)", "", true) }},
		{"a stop", func() { job.set("stopped", "") }},
		{"the next run starting", func() { job.set("pending", "") }},
		{"capture producing", func() {
			job.set("pending", "")
			job.setPhase(streamrun.PhaseResumeCleanupWaiting)
			job.setPhaseDetail("connection 812, running for 14m0s")
			job.progress()
		}},
	} {
		job.set("pending", "")
		job.setPhase(streamrun.PhaseResumeCleanupWaiting)
		job.setPhaseDetail("connection 812, running for 14m0s")
		tc.move()
		if st := job.snapshot(); st.Phase != "" || st.PhaseDetail != "" {
			t.Errorf("%s left a stale phase: %+v", tc.name, st)
		}
	}

	// A detail that arrives with no phase set belongs to a phase a state
	// change already ended.
	job.set("stopped", "")
	job.setPhaseDetail("connection 812, running for 14m0s")
	if st := job.snapshot(); st.PhaseDetail != "" {
		t.Errorf("a detail without a phase was kept: %+v", st)
	}
}

func TestMonitorErrorCode(t *testing.T) {
	for _, c := range []struct {
		name string
		err  error
		want string
	}{
		{"the ceiling failure", earlierCleanupErr(), console.MonitorErrEarlierCleanup},
		{"the ceiling failure, wrapped", fmt.Errorf("stream: %w", earlierCleanupErr()), console.MonitorErrEarlierCleanup},
		{"the raw lock timeout", lockWaitErr(), ""},
		{"a text that only reads like it", errors.New("an earlier resume cleanup is still running on the index"), ""},
		{"a write deadline", fmt.Errorf("flush: %w", indexer.ErrWriteDeadline), ""},
		{"nil", nil, ""},
	} {
		if got := monitorErrorCode(c.err); got != c.want {
			t.Errorf("%s: code %q, want %q", c.name, got, c.want)
		}
	}
}

// TestMonitorRun_earlierCleanupFailureCarriesItsCode: a supervised source
// whose start gave up waiting reports the code with the failure, retries on
// its own, and drops the code when the state moves on.
func TestMonitorRun_earlierCleanupFailureCarriesItsCode(t *testing.T) {
	oldBase, oldCap := monitorBackoffBase, monitorBackoffCap
	monitorBackoffBase, monitorBackoffCap = time.Hour, time.Hour
	defer func() { monitorBackoffBase, monitorBackoffCap = oldBase, oldCap }()

	for _, c := range []struct {
		name string
		err  error
		want string
	}{
		{"the ceiling failure", earlierCleanupErr(), console.MonitorErrEarlierCleanup},
		{"the raw lock timeout", lockWaitErr(), ""},
	} {
		ctx, cancel := context.WithCancel(context.Background())
		m := &monitorSupervisor{baseCtx: ctx, jobs: map[string]*monitorJob{}}
		job := &monitorJob{cancel: cancel, done: make(chan struct{})}
		job.set("pending", "")
		m.wg.Add(1)
		go m.run(ctx, job, console.ServerEntry{ID: "e1708", Name: "big"}, console.FlavorMySQL, func(context.Context) error { return c.err })

		deadline := time.Now().Add(5 * time.Second)
		for job.snapshot().State != "failed" && time.Now().Before(deadline) {
			time.Sleep(5 * time.Millisecond)
		}
		st := job.snapshot()
		if st.State != "failed" || !st.Retrying || st.ErrorCode != c.want {
			t.Errorf("%s: %+v, want failed, retrying, code %q", c.name, st, c.want)
		}
		raw, _ := json.Marshal(st)
		if has := strings.Contains(string(raw), `"error_code":"earlier_cleanup_running"`); has != (c.want != "") {
			t.Errorf("%s: status document = %s", c.name, raw)
		}
		cancel()
		<-job.done
		if st := job.snapshot(); st.State != "stopped" || st.ErrorCode != "" {
			t.Errorf("%s: after stop: %+v, want stopped with no code", c.name, st)
		}
	}
}

// TestMonitorRun_gaveUpKeepsTheCode: the breaker's terminal failure is still
// this cause, and says so.
func TestMonitorRun_gaveUpKeepsTheCode(t *testing.T) {
	old := monitorGiveUpAfter
	monitorGiveUpAfter = 0
	defer func() { monitorGiveUpAfter = old }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m := &monitorSupervisor{baseCtx: ctx, jobs: map[string]*monitorJob{}}
	job := &monitorJob{cancel: cancel, done: make(chan struct{})}
	job.set("pending", "")
	m.wg.Add(1)
	m.run(ctx, job, console.ServerEntry{ID: "e1708", Name: "big"}, console.FlavorMySQL, func(context.Context) error { return earlierCleanupErr() })
	if st := job.snapshot(); st.State != "failed" || st.Retrying || st.ErrorCode != console.MonitorErrEarlierCleanup {
		t.Errorf("%+v, want failed, not retrying, with the code", st)
	}
}

// TestRestartPolicyForAFailedCleanup pins what each kind of source does today
// when the resume cleanup fails, because what a screen may promise depends on
// it (#1708). Nothing here changes the policy; it records it.
//
//	a source added from the interface : retried with backoff, either error
//	the source given on the command line : NOT retried, either error. Its loop
//	    restarts on indexer.ErrWriteDeadline only, and neither the lock
//	    timeout nor the ceiling failure is one. The daemon exits.
func TestRestartPolicyForAFailedCleanup(t *testing.T) {
	oldBase, oldCap := monitorBackoffBase, monitorBackoffCap
	monitorBackoffBase, monitorBackoffCap = time.Millisecond, time.Millisecond
	oldMain := mainStreamFn
	defer func() { monitorBackoffBase, monitorBackoffCap, mainStreamFn = oldBase, oldCap, oldMain }()

	for _, c := range []struct {
		name string
		err  error
	}{
		{"lock wait timeout", lockWaitErr()},
		{"ceiling failure", earlierCleanupErr()},
	} {
		if errors.Is(c.err, indexer.ErrWriteDeadline) {
			t.Errorf("%s is tagged as a write deadline", c.name)
		}

		var mainRuns atomic.Int32
		mainStreamFn = func(context.Context, streamrun.Config) error { mainRuns.Add(1); return c.err }
		got := runMainStreamWithWriteDeadlineRetry(context.Background(), streamrun.Config{})
		if !errors.Is(got, c.err) || mainRuns.Load() != 1 {
			t.Errorf("%s, main source: ran %d times and returned %v; today it runs once and the daemon exits with the error",
				c.name, mainRuns.Load(), got)
		}

		var runs atomic.Int32
		ctx, cancel := context.WithCancel(context.Background())
		m := &monitorSupervisor{baseCtx: ctx, jobs: map[string]*monitorJob{}}
		job := &monitorJob{cancel: cancel, done: make(chan struct{})}
		job.set("pending", "")
		m.wg.Add(1)
		go m.run(ctx, job, console.ServerEntry{ID: "e1708", Name: "big"}, console.FlavorMySQL, func(context.Context) error { runs.Add(1); return c.err })
		deadline := time.Now().Add(5 * time.Second)
		for runs.Load() < 3 && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}
		cancel()
		<-job.done
		if runs.Load() < 3 {
			t.Errorf("%s, supervised source: ran %d times; today it is retried with backoff", c.name, runs.Load())
		}
	}
}
