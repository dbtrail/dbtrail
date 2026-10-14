//go:build unix

package sqlsandbox

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
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

// TestRunChild_KilledFromOutside: without a ceiling of its own, a child the
// kernel killed reads as that, not as a bare "signal: killed".
func TestRunChild_KilledFromOutside(t *testing.T) {
	shChild(t)
	noFence(t)
	_, err := RunChild(context.Background(), shJob(`read j; kill -9 $$`))
	var ce *ChildExitError
	if !errors.As(err, &ce) || !ce.Killed || !strings.Contains(err.Error(), "ran out of memory") {
		t.Fatalf("err = %v, want a ChildExitError that says it was killed from outside", err)
	}
}

// TestRunChild_StopGraceLetsItClean: a child whose caller gave up gets its
// stdin closed and time to run its cleanup before any kill.
func TestRunChild_StopGraceLetsItClean(t *testing.T) {
	shChild(t)
	noFence(t)
	cleaned := filepath.Join(t.TempDir(), "cleaned")
	ctx, cancel := context.WithCancel(context.Background())
	job := shJob(`read j; cat >/dev/null; echo yes > ` + cleaned)
	job.StopGrace = 10 * time.Second
	go func() { time.Sleep(200 * time.Millisecond); cancel() }()
	if _, err := RunChild(ctx, job); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if _, err := os.Stat(cleaned); err != nil {
		t.Errorf("the child was not given the chance to clean up: %v", err)
	}
}

// TestRunChild_StopGraceThenKill: one that does not stop within its grace is
// killed.
func TestRunChild_StopGraceThenKill(t *testing.T) {
	shChild(t)
	noFence(t)
	ctx, cancel := context.WithCancel(context.Background())
	job := shJob(`read j; trap '' TERM; while :; do sleep 1; done`)
	job.StopGrace = 300 * time.Millisecond
	go func() { time.Sleep(200 * time.Millisecond); cancel() }()
	start := time.Now()
	if _, err := RunChild(ctx, job); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if d := time.Since(start); d > 5*time.Second {
		t.Errorf("RunChild took %v to give up on a child that ignored its stdin", d)
	}
}

// TestRunChild_StderrReachesTheLog: what a child warns about is logged here
// even when it succeeds, at the level its line names; and a failure's
// summary is the panic line, not the goroutine dump after it.
func TestRunChild_StderrReachesTheLog(t *testing.T) {
	shChild(t)
	noFence(t)
	var buf syncBuffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(prev) })
	job := shJob(`read j; echo 'level=WARN msg="hours missing"' >&2; echo 'level=ERROR msg=bad' >&2; echo ok`)
	job.LogMsg, job.LogAttrs = "drill-down", []any{"table", "wp.posts"}
	if _, err := RunChild(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	log := buf.String()
	for _, want := range []string{`level=WARN msg=drill-down table=wp.posts line="level=WARN msg=\"hours missing\""`, `level=ERROR msg=drill-down table=wp.posts`} {
		if !strings.Contains(log, want) {
			t.Errorf("log lacks %s:\n%s", want, log)
		}
	}
	_, err := RunChild(context.Background(), shJob(`read j; echo 'panic: boom' >&2; echo; echo 'goroutine 1 [running]:' >&2; echo 'main.main()' >&2; exit 2`))
	var ce *ChildExitError
	if !errors.As(err, &ce) || ce.Stderr != "panic: boom" {
		t.Fatalf("err = %v, want the panic line as its summary", err)
	}
	_, err = RunChild(context.Background(), shJob(`read j; echo 'fatal error: concurrent map writes' >&2; echo 'goroutine 1 [running]:' >&2; exit 2`))
	if !errors.As(err, &ce) || ce.Stderr != "fatal error: concurrent map writes" {
		t.Fatalf("err = %v, want the fatal error line as its summary", err)
	}
}

type syncBuffer struct {
	mu sync.Mutex
	b  strings.Builder
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// TestChildFailure_OwnKillIsNotTheKernel: a SIGKILL this process sent (a
// child it gave up on) must not read as the kernel's out-of-memory kill.
func TestChildFailure_OwnKillIsNotTheKernel(t *testing.T) {
	waitErr := exec.Command("/bin/sh", "-c", "kill -9 $$").Run()
	c := &child{stderr: &stderrLog{}, waitErr: waitErr}
	if ce, ok := c.failure(0).(*ChildExitError); !ok || !ce.Killed {
		t.Fatalf("a SIGKILL from outside: %v, want Killed", c.failure(0))
	}
	c.killedByUs.Store(true)
	if ce, ok := c.failure(0).(*ChildExitError); !ok || ce.Killed {
		t.Errorf("our own kill read as one from outside: %v", c.failure(0))
	}
}
