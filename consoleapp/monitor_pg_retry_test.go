package consoleapp

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/pgcapture"
)

func unreachableErr() error {
	return &pgcapture.SourceUnreachableError{Err: syscall.ECONNREFUSED, Resumed: true}
}

// At the real constants, past the six hours: the delay stays at the cap and
// there is nothing that stops the retries.
func TestPGOutageBackoff_PastSixHours(t *testing.T) {
	var b pgOutageBackoff
	start := time.Date(2026, 11, 22, 0, 0, 0, 0, time.UTC)
	now := start
	var delays []time.Duration
	for now.Sub(start) < 3*monitorGiveUpAfter {
		// Each attempt fails within a second of starting.
		delay, down := b.failed(now.Add(-time.Second), now)
		if down != now.Sub(start) {
			t.Fatalf("outage measured as %s at %s", down, now.Sub(start))
		}
		if delay <= 0 || delay > monitorBackoffCap {
			t.Fatalf("delay %s at %s", delay, now.Sub(start))
		}
		delays = append(delays, delay)
		now = now.Add(delay)
	}
	want := []time.Duration{15 * time.Second, 30 * time.Second, time.Minute, 2 * time.Minute, 4 * time.Minute, 5 * time.Minute, 5 * time.Minute}
	for i, d := range want {
		if delays[i] != d {
			t.Fatalf("delay %d = %s, want %s", i, delays[i], d)
		}
	}
	if last := delays[len(delays)-1]; last != monitorBackoffCap {
		t.Fatalf("after 18h the delay is %s, want the cap", last)
	}
}

// A run that streamed for a healthy stretch ended the previous outage.
func TestPGOutageBackoff_HealthyRunStartsAgain(t *testing.T) {
	var b pgOutageBackoff
	t0 := time.Date(2026, 11, 22, 0, 0, 0, 0, time.UTC)
	for i := range 8 {
		now := t0.Add(time.Duration(i) * time.Minute)
		b.failed(now.Add(-time.Second), now)
	}
	now := t0.Add(2 * time.Hour)
	delay, down := b.failed(now.Add(-monitorHealthyReset-time.Second), now)
	if delay != monitorBackoffBase || down != 0 {
		t.Fatalf("after a healthy run: delay %s, down %s; want the base and a new outage", delay, down)
	}
}

func TestPGOutageMessage(t *testing.T) {
	short := pgOutageMessage("connection refused", 20*time.Second)
	long := pgOutageMessage("connection refused", 7*time.Hour+5*time.Minute)
	t.Logf("short: %s", short)
	t.Logf("long:  %s", long)
	for _, m := range []string{short, long} {
		if !strings.Contains(m, "cannot be reached") || !strings.Contains(m, "connection refused") || !strings.HasSuffix(m, "(retrying)") {
			t.Errorf("message %q", m)
		}
		// The words of the web interface's own controls (app.js).
		for _, want := range []string{"If its address changed", "press Edit", "Servers", "Source host", "press Save", "press Stop first"} {
			if !strings.Contains(m, want) {
				t.Errorf("message %q lacks %q", m, want)
			}
		}
	}
	if strings.Contains(short, "reached for ") || !strings.Contains(long, "reached for 7h05m") {
		t.Errorf("how long it lasted: short %q, long %q", short, long)
	}
}

func outageJob(cancel context.CancelFunc) *monitorJob {
	job := &monitorJob{cancel: cancel, done: make(chan struct{})}
	job.set("pending", "")
	return job
}

// The supervisor, past its give-up threshold, with a source that stays
// unreachable: still retrying, and showing it; then the source comes back.
func TestPGRunThroughOutages_NotGivenUpThenRecovers(t *testing.T) {
	shrinkMonitorBackoff(t)
	monitorGiveUpAfter = 30 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m := &monitorSupervisor{baseCtx: ctx, jobs: map[string]*monitorJob{}}
	job := outageJob(cancel)
	e := console.ServerEntry{ID: "e1", Name: "pg", SourceDSN: "postgres://u:s3cret@h/db"}

	var runs atomic.Int32
	var back atomic.Bool
	recovered := make(chan struct{})
	once := pgRunThroughOutages(job, e, func(c context.Context) error {
		runs.Add(1)
		if !back.Load() {
			return &pgcapture.SourceUnreachableError{Err: errors.New("dial postgres://u:s3cret@h/db: refused"), Resumed: true}
		}
		job.progress()
		close(recovered)
		<-c.Done()
		return nil
	})
	m.wg.Add(1)
	go m.run(ctx, job, e, console.FlavorPostgres, once)

	time.Sleep(5 * monitorGiveUpAfter)
	select {
	case <-job.done:
		t.Fatalf("gave up: %+v", job.snapshot())
	default:
	}
	if runs.Load() < 3 {
		t.Fatalf("%d attempts; want it to keep trying", runs.Load())
	}
	var st console.MonitorStatus
	for range 500 {
		if st = job.snapshot(); st.State == "failed" {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if st.State != "failed" || !st.Retrying || !strings.Contains(st.LastError, "cannot be reached") {
		t.Fatalf("state during the outage: %+v", st)
	}
	if strings.Contains(st.LastError, "s3cret") {
		t.Fatalf("the password is in the state: %q", st.LastError)
	}
	if got := readCapture(st, time.Now()); got != captureRetrying {
		t.Fatalf("capture_stopped reads it as %v, want captureRetrying", got)
	}

	back.Store(true)
	select {
	case <-recovered:
	case <-time.After(5 * time.Second):
		t.Fatal("capture did not come back")
	}
	if st := job.snapshot(); st.State != "running" || st.LastError != "" {
		t.Fatalf("after the outage: %+v", st)
	}
	cancel()
	<-job.done
}

// The controls the outage message names exist in the web interface.
func TestPGOutageMessage_NamesRealControls(t *testing.T) {
	src, err := os.ReadFile("../internal/console/assets/app.js")
	if err != nil {
		t.Fatal(err)
	}
	for _, control := range []string{`text: "Edit"`, `text: "Save"`, `"Source host"`, `? "Stop" : "Start"`} {
		if !strings.Contains(string(src), control) {
			t.Errorf("app.js no longer has %s, which the outage message tells the operator to use", control)
		}
	}
}

// A failure that is not an outage reaches the supervisor at once and keeps
// the give-up.
func TestPGRunThroughOutages_OtherErrorsKeepTheGiveUp(t *testing.T) {
	shrinkMonitorBackoff(t)
	monitorGiveUpAfter = 0
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m := &monitorSupervisor{baseCtx: ctx, jobs: map[string]*monitorJob{}}
	job := outageJob(cancel)
	var runs atomic.Int32
	once := pgRunThroughOutages(job, console.ServerEntry{ID: "e2"}, func(context.Context) error {
		runs.Add(1)
		return errors.New("password authentication failed")
	})
	m.wg.Add(1)
	go m.run(ctx, job, console.ServerEntry{ID: "e2"}, console.FlavorPostgres, once)
	select {
	case <-job.done:
	case <-time.After(5 * time.Second):
		t.Fatal("a failure that is not an outage was retried past the give-up")
	}
	st := job.snapshot()
	if st.State != "failed" || st.Retrying || !strings.Contains(st.LastError, "gave up") || runs.Load() != 1 {
		t.Fatalf("state %+v after %d runs", st, runs.Load())
	}
}

// A source never captured may have a wrong address: it keeps the give-up.
func TestPGRunThroughOutages_NeverCapturedKeepsTheGiveUp(t *testing.T) {
	shrinkMonitorBackoff(t)
	monitorGiveUpAfter = 0
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m := &monitorSupervisor{baseCtx: ctx, jobs: map[string]*monitorJob{}}
	job := outageJob(cancel)
	once := pgRunThroughOutages(job, console.ServerEntry{ID: "e4"}, func(context.Context) error {
		return &pgcapture.SourceUnreachableError{Err: syscall.ECONNREFUSED}
	})
	m.wg.Add(1)
	go m.run(ctx, job, console.ServerEntry{ID: "e4"}, console.FlavorPostgres, once)
	select {
	case <-job.done:
	case <-time.After(5 * time.Second):
		t.Fatal("a source never captured was retried past the give-up")
	}
	if st := job.snapshot(); st.State != "failed" || st.Retrying || !strings.Contains(st.LastError, "gave up") {
		t.Fatalf("state %+v", st)
	}
}

// A first capture that reached the source and then lost it, before it saved
// any position, is an outage.
func TestPGRunThroughOutages_ReachedInThisJobIsAnOutage(t *testing.T) {
	shrinkMonitorBackoff(t)
	monitorGiveUpAfter = 0
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	job := outageJob(cancel)
	var runs atomic.Int32
	once := pgRunThroughOutages(job, console.ServerEntry{ID: "e5"}, func(context.Context) error {
		if runs.Add(1) == 1 {
			job.markSourceConnected()
		}
		if runs.Load() == 4 {
			cancel()
			return context.Canceled
		}
		return &pgcapture.SourceUnreachableError{Err: syscall.ECONNRESET}
	})
	if err := once(ctx); !errors.Is(err, context.Canceled) || runs.Load() != 4 {
		t.Fatalf("returned %v after %d runs; want it to keep retrying", err, runs.Load())
	}
}

// The checkpoint tick fires while the capturer is still connecting: it must
// not show a source that is down as running.
func TestPGStreamHooks_TickBeforeConnectingIsNotProgress(t *testing.T) {
	job := outageJob(func() {})
	hooks := job.pgStreamHooks()
	hooks.OnCheckpoint()
	if st := job.snapshot(); st.State != "pending" {
		t.Fatalf("after a tick with no connection: %q, want pending", st.State)
	}
	hooks.OnSourceConnected()
	hooks.OnCheckpoint()
	if st := job.snapshot(); st.State != "running" {
		t.Fatalf("after a tick on a connected stream: %q, want running", st.State)
	}
}

// Stop during the wait between attempts ends it at once.
func TestPGRunThroughOutages_CancelDuringWait(t *testing.T) {
	shrinkMonitorBackoff(t)
	monitorBackoffBase, monitorBackoffCap = time.Hour, time.Hour
	ctx, cancel := context.WithCancel(context.Background())
	job := outageJob(cancel)
	entered := make(chan struct{}, 1)
	once := pgRunThroughOutages(job, console.ServerEntry{ID: "e3"}, func(context.Context) error {
		entered <- struct{}{}
		return unreachableErr()
	})
	done := make(chan error, 1)
	go func() { done <- once(ctx) }()
	<-entered
	for job.snapshot().State != "failed" {
		time.Sleep(time.Millisecond)
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("returned %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Stop did not end the wait")
	}
}
