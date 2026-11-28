package sqlsandbox

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// taskNices reads the nice value of every thread of process pid, from
// /proc/<pid>/task/<tid>/stat (field 19, counted from 1; the command name in
// parentheses may hold spaces, so fields are counted after its closing one).
func taskNices(t *testing.T, pid int) map[int]int {
	t.Helper()
	dir := filepath.Join("/proc", strconv.Itoa(pid), "task")
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	out := map[int]int{}
	for _, e := range entries {
		tid, err := strconv.Atoi(e.Name())
		if err != nil {
			continue
		}
		b, err := os.ReadFile(filepath.Join(dir, e.Name(), "stat"))
		if err != nil {
			// A thread that ended between the listing and the read.
			continue
		}
		s := string(b)
		fields := strings.Fields(s[strings.LastIndexByte(s, ')')+1:])
		if len(fields) < 17 {
			t.Fatalf("thread %d: short stat %q", tid, s)
		}
		// fields[0] is field 3 (state), so field 19 is fields[16].
		n, err := strconv.Atoi(fields[16])
		if err != nil {
			t.Fatalf("thread %d: nice field %q", tid, fields[16])
		}
		out[tid] = n
	}
	if len(out) == 0 {
		t.Fatalf("no threads read for process %d", pid)
	}
	return out
}

// oneNice is the nice value every thread of pid has; it fails the test when
// they still differ after two seconds. The wait is for a lowered thread of
// this process that has done its work and is about to end (startLowered
// returns a moment before the runtime ends it), never for one that stays.
func oneNice(t *testing.T, pid int) int {
	t.Helper()
	var nices map[int]int
	for range 100 {
		nices = taskNices(t, pid)
		v, same, first := 0, true, true
		for _, n := range nices {
			if first {
				v, first = n, false
			} else if n != v {
				same = false
			}
		}
		if same {
			return v
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("process %d: threads at different nice values: %v", pid, nices)
	return 0
}

// #3321: the worker, and every thread it has, DuckDB's included, runs step
// nice values below this process, while this process keeps its own priority
// on every thread. DuckDB starts its threads for the statement, so they are
// read while a long one runs, then the statement is stopped.
func TestWorkerNice_theWorkerAndAllItsThreadsRunLowerAndThisProcessDoesNot(t *testing.T) {
	before := oneNice(t, os.Getpid())
	if before+3 > maxNice {
		t.Skipf("this process already runs at nice %d", before)
	}
	f := newCopyFixture(t)
	limits := testLimits()
	limits.Threads = 4
	r := newTestRunner(t, limits)
	r.workerNice = 3
	pids := make(chan int, 1)
	r.onStart = func(p int) { pids <- p }
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	ran := make(chan error, 1)
	go func() {
		_, err := r.Run(ctx, f.job("SELECT count(*) FROM range(100000000) a, range(100000000) b"))
		ran <- err
	}()
	var pid int
	select {
	case pid = <-pids:
	case err := <-ran:
		t.Fatalf("the statement ended before its worker was seen: %v", err)
	case <-time.After(30 * time.Second):
		t.Fatal("no worker started")
	}
	// Until DuckDB's threads are there: the Go runtime alone has a few.
	most := 0
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(50 * time.Millisecond) {
		nices := taskNices(t, pid)
		for tid, n := range nices {
			if n != before+3 {
				t.Fatalf("worker %d thread %d at nice %d, want %d (all: %v)", pid, tid, n, before+3, nices)
			}
		}
		most = max(most, len(nices))
		if most >= 8 {
			break
		}
	}
	// How many threads the worker has depends on the machine (the Go
	// runtime's own grow with the cores), so it is said, not checked: what
	// is checked is that every one of them was lowered.
	t.Logf("worker %d: up to %d threads seen, all at nice %d", pid, most, before+3)
	if after := oneNice(t, os.Getpid()); after != before {
		t.Errorf("this process went from nice %d to %d", before, after)
	}
	cancel()
	<-ran
	r.spillMu.Lock()
	last := r.niceLastError
	r.spillMu.Unlock()
	if last != "" {
		t.Errorf("a reason was noted for a worker that was lowered: %q", last)
	}
}

// The console's own setup: standby workers, which open DuckDB before their
// statement comes. Every thread of every standby is lowered.
func TestWorkerNice_standbysRunLowerToo(t *testing.T) {
	before := oneNice(t, os.Getpid())
	if before+3 > maxNice {
		t.Skipf("this process already runs at nice %d", before)
	}
	f := newCopyFixture(t)
	r := newStandbyRunner(t, Config{WorkerNice: 3})
	mustRun(t, r, f.job("SELECT 1"))
	for _, pid := range waitStandbys(t, r, DefaultMaxInFlight) {
		for tid, n := range taskNices(t, pid) {
			if n != before+3 {
				t.Errorf("standby %d thread %d at nice %d, want %d", pid, tid, n, before+3)
			}
		}
	}
	if after := oneNice(t, os.Getpid()); after != before {
		t.Errorf("this process went from nice %d to %d", before, after)
	}
}

// Off: the worker runs at this process's priority.
func TestWorkerNice_offKeepsTheWorkerAtThisProcesssPriority(t *testing.T) {
	before := oneNice(t, os.Getpid())
	f := newCopyFixture(t)
	r := newStandbyRunner(t, Config{WorkerNice: 0})
	mustRun(t, r, f.job("SELECT 1"))
	standby := waitStandbys(t, r, DefaultMaxInFlight)[0]
	if n := oneNice(t, standby); n != before {
		t.Errorf("with the step off the standby runs at nice %d, want this process's %d", n, before)
	}
}

// A refused lowering (a seccomp profile, say) is not a failed statement: the
// worker starts at this process's priority and the statement answers; the
// reason is noted once, and cleared when lowering works again.
func TestWorkerNice_aRefusedLoweringStillRunsTheStatement(t *testing.T) {
	orig := setThreadNice
	t.Cleanup(func() { setThreadNice = orig })
	setThreadNice = func(int, int) error { return syscall.EPERM }

	before := oneNice(t, os.Getpid())
	f := newCopyFixture(t)
	r := newTestRunner(t, testLimits())
	r.workerNice = 5
	res, err := r.Run(t.Context(), f.job("SELECT count(*) FROM shop.orders"))
	if err != nil || len(res.Rows) != 1 {
		t.Fatalf("the statement: %v, %v", res.Rows, err)
	}
	r.spillMu.Lock()
	last := r.niceLastError
	r.spillMu.Unlock()
	if !strings.Contains(last, "lower the CPU priority") || !strings.Contains(last, syscall.EPERM.Error()) {
		t.Errorf("noted reason %q, want what was refused", last)
	}
	if after := oneNice(t, os.Getpid()); after != before {
		t.Errorf("this process went from nice %d to %d", before, after)
	}

	setThreadNice = orig
	if _, err := r.Run(t.Context(), f.job("SELECT 1")); err != nil {
		t.Fatal(err)
	}
	r.spillMu.Lock()
	last = r.niceLastError
	r.spillMu.Unlock()
	if last != "" {
		t.Errorf("after a lowering that worked the reason is still %q", last)
	}
}

// A priority that cannot even be read is said the same way, and the worker
// starts.
func TestStartLowered_anUnreadablePriority(t *testing.T) {
	orig := threadNice
	t.Cleanup(func() { threadNice = orig })
	threadNice = func(int) (int, error) { return 0, syscall.EINVAL }
	started := false
	niceErr, err := startLowered(10, func() error { started = true; return nil })
	if err != nil || !started {
		t.Fatalf("start: %v, started %v", err, started)
	}
	if !errors.Is(niceErr, syscall.EINVAL) || !strings.Contains(niceErr.Error(), "read this thread's CPU priority") {
		t.Errorf("niceErr = %v", niceErr)
	}
}

// start's own error comes back as it is, apart from the priority's.
func TestStartLowered_theStartsErrorIsItsOwn(t *testing.T) {
	boom := errors.New("boom")
	niceErr, err := startLowered(2, func() error { return boom })
	if niceErr != nil || !errors.Is(err, boom) {
		t.Fatalf("niceErr %v, err %v", niceErr, err)
	}
	niceErr, err = startLowered(0, func() error { return boom })
	if niceErr != nil || !errors.Is(err, boom) {
		t.Fatalf("step 0: niceErr %v, err %v", niceErr, err)
	}
}

// At the bottom already, nothing is set (no call that could fail), and it is
// said: the workers run at this process's priority.
func TestStartLowered_alreadyAtTheBottom(t *testing.T) {
	origRead, origSet := threadNice, setThreadNice
	t.Cleanup(func() { threadNice, setThreadNice = origRead, origSet })
	threadNice = func(int) (int, error) { return maxNice, nil }
	setThreadNice = func(int, int) error { t.Error("set called at the bottom"); return nil }
	niceErr, err := startLowered(10, func() error { return nil })
	if err != nil || niceErr == nil || !strings.Contains(niceErr.Error(), "already runs at nice 19") {
		t.Fatalf("niceErr %v, err %v", niceErr, err)
	}
}

// A panic in the start comes back as its error instead of ending the
// process: it runs in a goroutine no caller can recover.
func TestStartLowered_aPanicInTheStartIsAnError(t *testing.T) {
	niceErr, err := startLowered(3, func() error { panic("boom") })
	if niceErr != nil || err == nil || !strings.Contains(err.Error(), "panic: boom") {
		t.Fatalf("niceErr %v, err %v", niceErr, err)
	}
}

// The process's main thread is never the one lowered: the runtime parks that
// thread instead of ending it. The main thread's path is taken here from the
// test's own locked thread, which must come out of it untouched while the
// start runs, lowered, on another one.
func TestStartLowered_theMainThreadsPathLowersAnotherThread(t *testing.T) {
	orig := onMainThread
	t.Cleanup(func() { onMainThread = orig })
	outer := make(chan int, 1)
	onMainThread = func() bool { outer <- syscall.Gettid(); return true }

	var startTid, startNice int
	niceErr, err := startLowered(2, func() error {
		startTid = syscall.Gettid()
		startNice, _ = threadNice(startTid)
		return nil
	})
	if niceErr != nil || err != nil {
		t.Fatalf("niceErr %v, err %v", niceErr, err)
	}
	held := <-outer
	if startTid == held {
		t.Fatalf("the start ran on the thread that took the main thread's path (%d)", held)
	}
	before := oneNice(t, os.Getpid())
	if startNice != min(before+2, maxNice) {
		t.Errorf("the start ran at nice %d, want %d", startNice, min(before+2, maxNice))
	}
	if n, err := threadNice(held); err == nil && n != before {
		t.Errorf("the held thread %d is at nice %d after its path, want %d", held, n, before)
	}
	if after := oneNice(t, os.Getpid()); after != before {
		t.Errorf("this process's threads moved from nice %d to %d", before, after)
	}
}
