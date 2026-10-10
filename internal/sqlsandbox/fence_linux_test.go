//go:build linux

package sqlsandbox

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
)

// realFence skips unless this test process was handed a cgroup to manage:
// under a unit with Delegate=yes and DelegateSubgroup=, for instance
//
//	systemd-run --wait --pipe -p Delegate=yes -p DelegateSubgroup=daemon go test ./internal/sqlsandbox/
//
// BINTRAIL_REQUIRE_FENCE=1 turns the skip into a failure, for the run that
// is meant to exercise the kernel.
func realFence(t *testing.T) fence {
	t.Helper()
	f := fenceFor()
	if f.root == "" {
		if os.Getenv("BINTRAIL_REQUIRE_FENCE") == "1" {
			t.Fatalf("no cgroup to fence workers in: %s", f.why)
		}
		t.Skipf("no cgroup to fence workers in: %s", f.why)
	}
	return f
}

// The worker runs inside a cgroup of its own, with the ceiling its memory
// limit asks for, from its first instruction.
func TestFence_theWorkerRunsInItsOwnCgroup(t *testing.T) {
	realFence(t)
	f := newCopyFixture(t)
	limits := testLimits()
	limits.MemoryLimit = "512MiB"
	r := newTestRunner(t, limits)
	var membership, ceiling string
	r.onStart = func(pid int) {
		b, _ := os.ReadFile(fmt.Sprintf("/proc/%d/cgroup", pid))
		membership = strings.TrimSpace(string(b))
		if p, ok := selfCgroupPath(b); ok {
			m, _ := os.ReadFile(cgroupMount + p + "/memory.max")
			ceiling = strings.TrimSpace(string(m))
		}
	}
	res, err := r.Run(context.Background(), f.job("SELECT count(*) FROM shop.orders"))
	if err != nil || len(res.Rows) != 1 {
		t.Fatalf("res = %v, err = %v", res.Rows, err)
	}
	if !strings.Contains(membership, "/"+fencePrefix) {
		t.Errorf("the worker's cgroup is %q, want one named %s*", membership, fencePrefix)
	}
	if want := fmt.Sprint(512<<20 + fenceMinHeadroom); ceiling != want {
		t.Errorf("the worker's memory.max = %q, want %s", ceiling, want)
	}
	if fenced, why := r.FenceState(); !fenced {
		t.Errorf("FenceState = false, %q", why)
	}
	if left := fencesLeft(t, r.fence.root); len(left) != 0 {
		t.Errorf("cgroups left after the statement: %v", left)
	}
}

// A statement that grows past its ceiling outside DuckDB's own limit (one
// huge string is built outside the buffer manager) is stopped by the kernel
// inside its cgroup, and reads as out of memory.
func TestFence_aStatementPastItsCeilingIsOutOfMemory(t *testing.T) {
	realFence(t)
	f := newCopyFixture(t)
	limits := testLimits()
	limits.MemoryLimit = "256MiB"
	r := newTestRunner(t, limits)
	_, err := r.Run(context.Background(), f.job("SELECT length(repeat('x', 1500000000))"))
	var qe *QueryError
	if !errors.As(err, &qe) || qe.Message != oomFenceMessage {
		t.Fatalf("err = %v (%T), want the cgroup's out-of-memory error", err, err)
	}
	// The next statement is unaffected.
	if res, err := r.Run(context.Background(), f.job("SELECT 1")); err != nil || len(res.Rows) != 1 {
		t.Fatalf("the statement after it: res = %v, err = %v", res.Rows, err)
	}
	if left := fencesLeft(t, r.fence.root); len(left) != 0 {
		t.Errorf("cgroups left after the statements: %v", left)
	}
}
