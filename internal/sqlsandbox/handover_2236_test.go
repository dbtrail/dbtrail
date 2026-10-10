//go:build unix

package sqlsandbox

import (
	"context"
	"encoding/json"
	"errors"
	"syscall"
	"testing"
	"time"
)

// #2236, first step: a worker is started, and then handed its job. A cold
// run does both at once; these pin what must hold when time passes between
// the two, which is what a worker started ahead of its statement needs.

func handover(t *testing.T, r *Runner, w *worker, job Job, limits Limits) (Result, error) {
	t.Helper()
	in, err := marshalJob(job, limits, spillSpec{})
	if err != nil {
		t.Fatal(err)
	}
	return r.run(context.Background(), w, time.Now(), in, job.ViewsFor, limits, spillSpec{}, nil)
}

func alive(pid int) bool { return syscall.Kill(pid, 0) == nil }

// The statement's clock starts when the job is handed over. A worker that
// waited longer than the whole timeout before its job still runs it, and the
// worker does not stop itself while it waits.
func TestHandover_theClockStartsWithTheJob(t *testing.T) {
	f := newCopyFixture(t)
	limits := testLimits()
	limits.Timeout = 2 * time.Second
	r := newTestRunner(t, limits)
	w, err := r.startWorker()
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(limits.Timeout + selfDeadlineGrace + 500*time.Millisecond)
	select {
	case <-w.done:
		t.Fatalf("the worker exited while it waited for a job: %v\n%s", w.waitErr, w.stderr.text())
	default:
	}
	res, err := handover(t, r, w, f.job("SELECT count(*) FROM shop.orders"), limits)
	if err != nil {
		t.Fatalf("a job handed over after the wait: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("rows = %v", res.Rows)
	}
	// Spawn is this statement's cost, not the worker's age.
	if res.Phases.Spawn >= limits.Timeout {
		t.Fatalf("Spawn = %v counts the wait before the job", res.Phases.Spawn)
	}
}

// And the timeout still ends a statement that runs past it, counted from the
// handover.
func TestHandover_theTimeoutCountsFromTheJob(t *testing.T) {
	f := newCopyFixture(t)
	limits := testLimits()
	limits.Timeout = 1500 * time.Millisecond
	r := newTestRunner(t, limits)
	w, err := r.startWorker()
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second)
	pid := w.cmd.Process.Pid
	start := time.Now()
	_, err = handover(t, r, w, f.job("SELECT count(*) FROM range(100000000) a, range(100000000) b"), limits)
	var te *TimeoutError
	if !errors.As(err, &te) {
		t.Fatalf("err = %v, want a timeout", err)
	}
	if took := time.Since(start); took < limits.Timeout {
		t.Fatalf("timed out after %v, before the %v the statement was given", took, limits.Timeout)
	}
	if alive(pid) {
		t.Fatalf("worker %d survived its timeout", pid)
	}
}

// A worker whose parent closes its stdin without a job (the parent is going
// away, or does not want it any more) exits on its own, at once, and is
// reaped. Nothing is left waiting for a deadline it does not have.
func TestHandover_aWorkerWithNoJobExitsWhenStdinCloses(t *testing.T) {
	r := newTestRunner(t, testLimits())
	w, err := r.startWorker()
	if err != nil {
		t.Fatal(err)
	}
	pid := w.cmd.Process.Pid
	_ = w.stdin.Close()
	select {
	case <-w.done:
	case <-time.After(15 * time.Second):
		w.kill()
		t.Fatal("the worker did not exit when its stdin closed")
	}
	if alive(pid) {
		t.Fatalf("worker %d is still there", pid)
	}
}

// A worker that died before its job is a worker failure with its pid, said at
// once: never a hang, and never a result.
func TestHandover_aWorkerThatDiedFirstIsAFailureNotAHang(t *testing.T) {
	f := newCopyFixture(t)
	limits := testLimits()
	r := newTestRunner(t, limits)
	w, err := r.startWorker()
	if err != nil {
		t.Fatal(err)
	}
	w.kill()
	<-w.done
	done := make(chan error, 1)
	go func() {
		_, err := handover(t, r, w, f.job("SELECT 1"), limits)
		done <- err
	}()
	select {
	case err := <-done:
		var we *WorkerError
		if !errors.As(err, &we) || we.PID == 0 {
			t.Fatalf("err = %v, want a worker failure with its pid", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("handing a job to a dead worker never returned")
	}
}

// A worker serves one job. Handed a second, it has exited: a failure, never
// a second result from a session the first statement used.
func TestHandover_aWorkerServesOneJob(t *testing.T) {
	f := newCopyFixture(t)
	limits := testLimits()
	r := newTestRunner(t, limits)
	w, err := r.startWorker()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := handover(t, r, w, f.job("SELECT 1"), limits); err != nil {
		t.Fatal(err)
	}
	res, err := handover(t, r, w, f.job("SELECT 2"), limits)
	var we *WorkerError
	if !errors.As(err, &we) {
		t.Fatalf("a worker answered a second job: %v, err = %v", res.Rows, err)
	}
}

// The limits a job runs under are the ones in the job, whatever the runner's
// were when the worker started: a memory limit changed in between applies.
func TestHandover_theJobsLimitsApplyNotTheOnesAtStart(t *testing.T) {
	f := newCopyFixture(t)
	r := newTestRunner(t, testLimits())
	w, err := r.startWorker()
	if err != nil {
		t.Fatal(err)
	}
	limits := testLimits()
	limits.MemoryLimit = "300MiB"
	limits.Threads = 1
	res, err := handover(t, r, w, f.job("SELECT current_setting('memory_limit'), current_setting('threads')"), limits)
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := json.Marshal(res.Rows); string(got) != `[["286.1 MiB",1]]` && string(got) != `[["300.0 MiB",1]]` {
		t.Fatalf("the session ran under %s, want the job's 300MiB and 1 thread", got)
	}
}

// The views a statement asks for are answered on a worker started earlier
// too.
func TestHandover_askingForViewsWorksAfterAWait(t *testing.T) {
	f := newCopyFixture(t)
	limits := testLimits()
	r := newTestRunner(t, limits)
	w, err := r.startWorker()
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)
	job := f.job("SELECT count(*) FROM shop.orders")
	script := job.ViewsSQL
	asked := 0
	job.ViewsSQL, job.ViewsFor = "", func(Refs) (string, error) { asked++; return script, nil }
	res, err := handover(t, r, w, job, limits)
	if err != nil || asked != 1 || len(res.Rows) != 1 {
		t.Fatalf("err = %v, asked %d time(s), rows = %v", err, asked, res.Rows)
	}
}
