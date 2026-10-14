//go:build unix

package sqlsandbox

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"syscall"
	"testing"
)

// shChild makes RunChild start /bin/sh with script, and its fence the one
// fenceFor gives (fakeFence sets that), for the length of one test.
func shChild(t *testing.T) {
	t.Helper()
	prevExe, prevFence := childExe, childFence
	childExe = func() (string, error) { return "/bin/sh", nil }
	childFence = func() fence { return fenceFor() }
	t.Cleanup(func() { childExe, childFence = prevExe, prevFence })
}

func shJob(script string) ChildJob {
	return ChildJob{Args: []string{"-c", script}, MemoryLimit: "512MiB", Input: []byte("job\n"), MaxOutput: 1 << 10}
}

// TestRunChild_Answers: the job reaches stdin and stdout comes back.
func TestRunChild_Answers(t *testing.T) {
	shChild(t)
	noFence(t)
	out, err := RunChild(context.Background(), shJob(`read j; echo "got $j"`))
	if err != nil || string(out) != "got job\n" {
		t.Fatalf("RunChild = %q, %v; want %q", out, err, "got job\n")
	}
}

// TestRunChild_ExitCarriesStderr: a child that fails says why, in its own
// words; a bare "exit status 1" sends an operator hunting.
func TestRunChild_ExitCarriesStderr(t *testing.T) {
	shChild(t)
	noFence(t)
	_, err := RunChild(context.Background(), shJob(`read j; echo "index refused" >&2; exit 3`))
	var ce *ChildExitError
	if !errors.As(err, &ce) || !strings.Contains(err.Error(), "index refused") || !strings.Contains(err.Error(), "exit status 3") {
		t.Fatalf("err = %v, want a ChildExitError with the exit status and the child's stderr", err)
	}
}

// TestRunChild_OutputCap: past MaxOutput the child is stopped, not read on.
func TestRunChild_OutputCap(t *testing.T) {
	shChild(t)
	noFence(t)
	_, err := RunChild(context.Background(), shJob(`read j; while :; do echo xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx; done`))
	if !errors.Is(err, ErrResultTooLarge) {
		t.Fatalf("err = %v, want ErrResultTooLarge", err)
	}
}

// TestRunChild_CanceledBeforeStart: nothing is started for a caller that
// already gave up.
func TestRunChild_CanceledBeforeStart(t *testing.T) {
	shChild(t)
	childExe = func() (string, error) { t.Fatal("a child was started for a canceled context"); return "", nil }
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := RunChild(ctx, shJob("true")); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}

// TestRunChild_InItsCgroup: where the host gives cgroups, the child is moved
// into one with the job's ceiling BEFORE it is handed the job, and the
// cgroup is gone afterwards.
func TestRunChild_InItsCgroup(t *testing.T) {
	shChild(t)
	parent := fakeFence(t, "")
	out, err := RunChild(context.Background(), shJob(`read j; echo "$j"`))
	if err != nil || string(out) != "job\n" {
		t.Fatalf("RunChild = %q, %v", out, err)
	}
	want, _ := fenceBytes("512MiB")
	if len(startedIn) != 1 || startedIn[0].ceiling != itoa(want) {
		t.Fatalf("started in %+v, want one cgroup with ceiling %d", startedIn, want)
	}
	if left := fencesLeft(t, parent); len(left) != 0 {
		t.Errorf("cgroups left behind: %v", left)
	}
}

// TestRunChild_StoppedAtItsCeiling: a child the kernel stopped at its own
// ceiling is reported as that, with the ceiling, never as a generic exit.
func TestRunChild_StoppedAtItsCeiling(t *testing.T) {
	shChild(t)
	fakeFence(t, "oom 1\noom_kill 1\n")
	_, err := RunChild(context.Background(), shJob(`read j; kill -9 $$`))
	var oom *ChildOOMError
	if !errors.As(err, &oom) {
		t.Fatalf("err = %v, want a ChildOOMError", err)
	}
	if want, _ := fenceBytes("512MiB"); oom.CeilingBytes != want {
		t.Errorf("ceiling = %d, want %d", oom.CeilingBytes, want)
	}
	if !strings.Contains(err.Error(), "768 MiB") {
		t.Errorf("message %q does not name the ceiling in MiB", err)
	}
}

// TestRunChild_CgroupRefusedRunsWithout: a child that cannot be moved into
// its cgroup is stopped before it has its job, and the job runs in one
// started without a cgroup, as a statement's worker would.
func TestRunChild_CgroupRefusedRunsWithout(t *testing.T) {
	shChild(t)
	parent := fakeFence(t, "")
	intoFence = func(*workerFence, int) error { return syscall.EACCES }
	out, err := RunChild(context.Background(), shJob(`read j; echo "$j"`))
	if err != nil || string(out) != "job\n" {
		t.Fatalf("RunChild = %q, %v; want the answer from a child started without a cgroup", out, err)
	}
	if left := fencesLeft(t, parent); len(left) != 0 {
		t.Errorf("the refused cgroup was left behind: %v", left)
	}
}

func noFence(t *testing.T) {
	t.Helper()
	prev := fenceFor
	fenceFor = func() fence { return fence{} }
	t.Cleanup(func() { fenceFor = prev })
}

func itoa(n int64) string { return strconv.FormatInt(n, 10) }
