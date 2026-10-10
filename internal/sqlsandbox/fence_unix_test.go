//go:build unix

package sqlsandbox

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"strings"
	"testing"
)

// fakeFence gives the Runners of one test a fence over a plain directory:
// the kernel's part (the control files, the counters a dead worker leaves)
// is stood in for, and the worker is started outside it.
func fakeFence(t *testing.T, events string) (parent string) {
	t.Helper()
	fakeKernel(t, events)
	_, parent = fakeCgroupTree(t, "memory\n", "memory\n")
	prevFor, prevInto := fenceFor, intoFence
	fenceFor = func() fence { return fence{root: parent} }
	intoFence = func(*exec.Cmd, *workerFence) {}
	t.Cleanup(func() { fenceFor, intoFence = prevFor, prevInto })
	return parent
}

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
	if left := fencesLeft(t, parent); len(left) != 0 {
		t.Errorf("cgroups left after the statement: %v", left)
	}
}

// A host with no fence at all: statements run, and the reason is the one
// found at start.
func TestRun_noFenceOnTheHost(t *testing.T) {
	prev := fenceFor
	fenceFor = func() fence { return fence{why: "this host does not run DBTrail under cgroup v2"} }
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
