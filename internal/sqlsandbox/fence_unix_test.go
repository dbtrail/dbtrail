//go:build unix

package sqlsandbox

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

// fakeFence gives the Runners of one test a fence over a plain directory:
// the kernel's part (the control files, the counters a dead worker leaves)
// is stood in for, and the worker's move into it is recorded, not made.
func fakeFence(t *testing.T, events string) (parent string) {
	t.Helper()
	fakeKernel(t, events)
	_, parent = fakeCgroupTree(t, "memory\n", "memory\n")
	prevFor, prevInto := fenceFor, intoFence
	fenceFor = func() fence { return fence{root: parent} }
	startedIn = nil
	intoFence = func(wf *workerFence, pid int) error {
		// Read now: the cgroup is gone when the statement returns.
		ceiling, _ := os.ReadFile(filepath.Join(wf.dir, "memory.max"))
		startedIn = append(startedIn, fenceStart{dir: wf.dir, ceiling: string(ceiling), pid: pid})
		return nil
	}
	t.Cleanup(func() { fenceFor, intoFence = prevFor, prevInto })
	return parent
}

// fenceStart is one worker start as fakeFence's stand-in saw it: the cgroup
// the worker was to be started in and the ceiling it had at that moment.
type fenceStart struct {
	dir, ceiling string
	pid          int
}

var startedIn []fenceStart

func fencesLeft(t *testing.T, parent string) []string {
	t.Helper()
	entries, err := os.ReadDir(parent)
	if err != nil {
		t.Fatal(err)
	}
	var left []string
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), fencePrefix) {
			left = append(left, e.Name())
		}
	}
	return left
}

// A worker the kernel stopped at its cgroup's ceiling is a statement that
// ran out of memory, in the words every caller already recognises, not a
// worker that failed.
func TestRun_aWorkerKilledAtItsCeilingIsOutOfMemory(t *testing.T) {
	parent := fakeFence(t, "max 12\noom 1\noom_kill 1\n")
	f := newCopyFixture(t)
	r := New(Config{Exe: "/bin/sh", Args: []string{"-c", "kill -9 $$"}, Limits: testLimits()})
	_, err := r.Run(context.Background(), f.job("SELECT 1"))
	var qe *QueryError
	if !errors.As(err, &qe) || !strings.HasPrefix(qe.Message, "Out of Memory Error") {
		t.Fatalf("err = %v (%T), want a QueryError that starts with Out of Memory Error", err, err)
	}
	if left := fencesLeft(t, parent); len(left) != 0 {
		t.Errorf("cgroups left after the statement: %v", left)
	}
}

// The same death with no kill counted in the cgroup stays what it was: a
// worker failure.
func TestRun_aWorkerThatDiesUnderItsCeilingIsAWorkerError(t *testing.T) {
	parent := fakeFence(t, "max 0\noom 0\noom_kill 0\n")
	f := newCopyFixture(t)
	r := New(Config{Exe: "/bin/sh", Args: []string{"-c", "kill -9 $$"}, Limits: testLimits()})
	_, err := r.Run(context.Background(), f.job("SELECT 1"))
	var we *WorkerError
	if !errors.As(err, &we) {
		t.Fatalf("err = %v (%T), want a WorkerError", err, err)
	}
	if left := fencesLeft(t, parent); len(left) != 0 {
		t.Errorf("cgroups left after the statement: %v", left)
	}
}

// A statement in its cgroup answers as any other, the Runner says it is
// fenced, and the cgroup is gone when it returns.
func TestRun_aFencedStatementAnswers(t *testing.T) {
	parent := fakeFence(t, "oom_kill 0\n")
	f := newCopyFixture(t)
	r := newTestRunner(t, testLimits())
	res, err := r.Run(context.Background(), f.job("SELECT 41 + 1"))
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("rows = %v", res.Rows)
	}
	if fenced, why := r.FenceState(); !fenced || why != "" {
		t.Errorf("FenceState = %v, %q; want fenced", fenced, why)
	}
	// The worker was started into a cgroup of its own under the delegated
	// one, with the ceiling its memory limit asks for already set.
	want, _ := fenceBytes(testLimits().MemoryLimit)
	if len(startedIn) != 1 || filepath.Dir(startedIn[0].dir) != parent || startedIn[0].ceiling != fmt.Sprint(want) || startedIn[0].pid <= 0 {
		t.Errorf("worker moves = %+v, want one process, into a cgroup under %s, with memory.max %d", startedIn, parent, want)
	}
	if left := fencesLeft(t, parent); len(left) != 0 {
		t.Errorf("cgroups left after the statement: %v", left)
	}
}

// A cgroup that cannot be made does not fail the statement: it runs as it
// did before, and the Runner says why it is not fenced.
func TestRun_aStatementRunsWithoutItsCgroupWhenNoneCanBeMade(t *testing.T) {
	_, parent := fakeCgroupTree(t, "memory\n", "memory\n")
	prev := fenceFor
	// No fakeKernel: the new cgroup has no memory.max, so make fails.
	fenceFor = func() fence { return fence{root: parent} }
	t.Cleanup(func() { fenceFor = prev })
	f := newCopyFixture(t)
	r := newTestRunner(t, testLimits())
	if fenced, _ := r.FenceState(); !fenced {
		t.Fatal("a Runner with a fence reports none before any statement")
	}
	res, err := r.Run(context.Background(), f.job("SELECT 1"))
	if err != nil || len(res.Rows) != 1 {
		t.Fatalf("res = %v, err = %v; want the statement's answer", res.Rows, err)
	}
	fenced, why := r.FenceState()
	if fenced || !strings.Contains(why, "memory ceiling") {
		t.Errorf("FenceState = %v, %q; want unfenced, naming the ceiling that could not be set", fenced, why)
	}
	// The same failure on the next statement is not a change: the cgroup's
	// name differs every time, and the reason does not.
	before := r.fenceLastError
	if _, err := r.Run(context.Background(), f.job("SELECT 1")); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(before, parent) || r.fenceLastError != before {
		t.Errorf("the reason kept for comparison changed between two statements that failed alike: %q then %q", before, r.fenceLastError)
	}
	// What the settings panel shows names no path on the host.
	if strings.Contains(why, parent) {
		t.Errorf("the reason names a path on the host: %q", why)
	}
	if left := fencesLeft(t, parent); len(left) != 0 {
		t.Errorf("cgroups left after the statement: %v", left)
	}
}

// A host with no fence at all: statements run, and the reason is the one
// found at start.
func TestRun_noFenceOnTheHost(t *testing.T) {
	prev := fenceFor
	fenceFor = func() fence { return fence{why: "this system does not offer it (it needs cgroup v2)"} }
	t.Cleanup(func() { fenceFor = prev })
	f := newCopyFixture(t)
	r := newTestRunner(t, testLimits())
	if res, err := r.Run(context.Background(), f.job("SELECT 1")); err != nil || len(res.Rows) != 1 {
		t.Fatalf("res = %v, err = %v", res.Rows, err)
	}
	if fenced, why := r.FenceState(); fenced || !strings.Contains(why, "cgroup v2") {
		t.Errorf("FenceState = %v, %q", fenced, why)
	}
}

// A worker that cannot start at all is not a fence that stopped working.
func TestRun_aWorkerThatCannotStartDoesNotBlameItsCgroup(t *testing.T) {
	fakeFence(t, "oom_kill 0\n")
	f := newCopyFixture(t)
	r := New(Config{Exe: "/nonexistent/worker", Args: []string{}, Limits: testLimits()})
	if _, err := r.Run(context.Background(), f.job("SELECT 1")); err == nil {
		t.Fatal("a worker that does not exist ran")
	}
	if fenced, why := r.FenceState(); !fenced {
		t.Errorf("FenceState = false, %q; the cgroup was not what failed", why)
	}
}

// A worker that cannot be moved into its cgroup is stopped, never handed a
// job outside its ceiling as if it were inside: the statement runs in a
// second worker with no cgroup, and the Runner says so.
func TestRun_aWorkerThatCannotBeMovedIsReplaced(t *testing.T) {
	parent := fakeFence(t, "oom_kill 0\n")
	var refused []int
	intoFence = func(_ *workerFence, pid int) error {
		refused = append(refused, pid)
		return errors.New("no")
	}
	f := newCopyFixture(t)
	r := newTestRunner(t, testLimits())
	var ran int
	r.onStart = func(pid int) { ran = pid }
	res, err := r.Run(context.Background(), f.job("SELECT 1"))
	if err != nil || len(res.Rows) != 1 {
		t.Fatalf("res = %v, err = %v; want the statement's answer", res.Rows, err)
	}
	if len(refused) != 1 || ran == refused[0] {
		t.Fatalf("moves refused for %v, statement ran in %d: want it to run in another worker than the one refused", refused, ran)
	}
	if alive(refused[0]) {
		t.Errorf("worker %d, which could not be moved, is still running", refused[0])
	}
	if fenced, why := r.FenceState(); fenced || !strings.Contains(why, "move it into its cgroup") {
		t.Errorf("FenceState = %v, %q; want unfenced, naming the move", fenced, why)
	}
	if left := fencesLeft(t, parent); len(left) != 0 {
		t.Errorf("cgroups left after the statement: %v", left)
	}
}

// A worker that died before it could be moved is a worker that failed, as
// it was before: no second worker is started for it, and its cgroup is not
// what gets the blame.
func TestRun_aWorkerGoneBeforeTheMoveIsNotReplaced(t *testing.T) {
	fakeFence(t, "oom_kill 0\n")
	moves := 0
	intoFence = func(*workerFence, int) error {
		moves++
		return &os.PathError{Op: "write", Path: "cgroup.procs", Err: syscall.ESRCH}
	}
	f := newCopyFixture(t)
	r := New(Config{Exe: "/bin/sh", Args: []string{"-c", "exit 1"}, Limits: testLimits()})
	starts := 0
	r.onStart = func(int) { starts++ }
	_, err := r.Run(context.Background(), f.job("SELECT 1"))
	var we *WorkerError
	if !errors.As(err, &we) {
		t.Fatalf("err = %v (%T), want a WorkerError", err, err)
	}
	if moves != 1 || starts != 1 {
		t.Errorf("moves = %d, workers run = %d; want one of each", moves, starts)
	}
	if fenced, why := r.FenceState(); !fenced {
		t.Errorf("FenceState = false, %q; the cgroup was not what failed", why)
	}
}

// A worker started ahead of its statement (#2236) does not know the
// statement's memory yet: it waits under the Runner's default ceiling, and
// gets the statement's own when the job is handed over.
func TestRun_aWorkerStartedAheadGetsItsStatementsCeiling(t *testing.T) {
	fakeFence(t, "oom_kill 0\n")
	f := newCopyFixture(t)
	r := newTestRunner(t, testLimits())
	w, err := r.startWorker(r.limits.MemoryLimit)
	if err != nil {
		t.Fatal(err)
	}
	waiting, _ := fenceBytes(r.limits.MemoryLimit)
	if len(startedIn) != 1 || startedIn[0].ceiling != fmt.Sprint(waiting) {
		t.Fatalf("worker moves = %+v, want one under the default ceiling %d", startedIn, waiting)
	}
	limits := testLimits()
	limits.MemoryLimit = "512MiB"
	var atHandover string
	r.onStart = func(int) {
		b, _ := os.ReadFile(filepath.Join(startedIn[0].dir, "memory.max"))
		atHandover = string(b)
	}
	res, err := handover(t, r, w, f.job("SELECT 1"), limits)
	if err != nil || len(res.Rows) != 1 {
		t.Fatalf("res = %v, err = %v", res.Rows, err)
	}
	if want, _ := fenceBytes("512MiB"); atHandover != fmt.Sprint(want) {
		t.Errorf("memory.max when the job was handed over = %q, want %d", atHandover, want)
	}
	if fenced, why := r.FenceState(); !fenced {
		t.Errorf("FenceState = false, %q", why)
	}
}
