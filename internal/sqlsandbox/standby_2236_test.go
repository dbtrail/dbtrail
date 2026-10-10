//go:build unix

package sqlsandbox

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// #2236: workers started ahead of the statements that use them. What must not
// change is everything a statement can observe except its time: one process
// per statement, never reused, the same limits, the same refusals.

func newStandbyRunner(t *testing.T, cfg Config) *Runner {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cfg.Exe, cfg.Args, cfg.Standby = exe, []string{}, true
	if cfg.Limits == (Limits{}) {
		cfg.Limits = testLimits()
	}
	if cfg.MaxWait == 0 {
		cfg.MaxWait = -1
	}
	r := New(cfg)
	t.Cleanup(r.Close)
	return r
}

func standbyPids(r *Runner) []int {
	r.mu.Lock()
	defer r.mu.Unlock()
	var pids []int
	for _, w := range r.standbys {
		pids = append(pids, w.cmd.Process.Pid)
	}
	return pids
}

// waitStandbys waits for the top-up, which runs off the statement.
func waitStandbys(t *testing.T, r *Runner, n int) []int {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		pids := standbyPids(r)
		if len(pids) == n {
			return pids
		}
		if time.Now().After(deadline) {
			t.Fatalf("standbys = %v, want %d", pids, n)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func mustRun(t *testing.T, r *Runner, job Job) (Result, int) {
	t.Helper()
	pid := 0
	r.onStart = func(p int) { pid = p }
	res, err := r.Run(context.Background(), job)
	if err != nil {
		t.Fatal(err)
	}
	return res, pid
}

// Nothing is started until a statement has run. After it there is one standby
// per free slot, the next statement runs on one of them, and that worker is
// gone afterwards, replaced by another process.
func TestStandby_theNextStatementRunsOnOneAndItIsNeverReused(t *testing.T) {
	f := newCopyFixture(t)
	r := newStandbyRunner(t, Config{})
	if pids := standbyPids(r); len(pids) != 0 {
		t.Fatalf("standbys before any statement: %v", pids)
	}
	res, first := mustRun(t, r, f.job("SELECT 1"))
	if res.Phases.Standby || res.Phases.Open == 0 {
		t.Fatalf("the first statement: %+v, want a worker started for it", res.Phases)
	}
	waiting := waitStandbys(t, r, DefaultMaxInFlight)
	res, second := mustRun(t, r, f.job("SELECT count(*) FROM shop.orders"))
	if !res.Phases.Standby || res.Phases.Open != 0 || r.standbyUsed.Load() != 1 {
		t.Fatalf("the second statement: %+v, used %d, want it on a standby", res.Phases, r.standbyUsed.Load())
	}
	// The one that waited longest: it has finished opening.
	if second != waiting[0] {
		t.Fatalf("the second statement ran in %d; the standbys were %v, oldest first, and the first worker %d", second, waiting, first)
	}
	if alive(second) {
		t.Fatalf("worker %d is still there after its statement", second)
	}
	after := waitStandbys(t, r, DefaultMaxInFlight)
	for _, p := range after {
		if p == second || p == first {
			t.Fatalf("a used worker %d is waiting again: %v", p, after)
		}
	}
}

// Off unless asked for: a Runner without Standby never keeps a process.
func TestStandby_offByDefault(t *testing.T) {
	f := newCopyFixture(t)
	r := newTestRunner(t, testLimits())
	for range 2 {
		if res, _ := mustRun(t, r, f.job("SELECT 1")); res.Phases.Standby {
			t.Fatal("a statement ran on a standby with Standby off")
		}
	}
	time.Sleep(300 * time.Millisecond)
	if pids := standbyPids(r); len(pids) != 0 {
		t.Fatalf("standbys with Standby off: %v", pids)
	}
}

// A standby killed while it waits is not a failed statement: the next one
// runs, on another standby or on a worker of its own.
func TestStandby_oneThatDiedWaitingCostsNothing(t *testing.T) {
	f := newCopyFixture(t)
	r := newStandbyRunner(t, Config{MaxInFlight: 1})
	mustRun(t, r, f.job("SELECT 1"))
	pids := waitStandbys(t, r, 1)
	_ = killProcessGroup(r.standbys[0].cmd.Process)
	<-r.standbys[0].done
	res, pid := mustRun(t, r, f.job("SELECT count(*) FROM shop.orders"))
	if pid == pids[0] || res.Phases.Standby || len(res.Rows) != 1 {
		t.Fatalf("ran in %d (the dead standby was %d), phases %+v", pid, pids[0], res.Phases)
	}
}

// The narrow case: the standby is alive when it is taken and gone before it
// reads its job. The statement runs once, on a worker started for it.
func TestStandby_goneBetweenTakenAndHandedOverRunsOnce(t *testing.T) {
	f := newCopyFixture(t)
	r := newStandbyRunner(t, Config{MaxInFlight: 1})
	mustRun(t, r, f.job("SELECT 1"))
	waitStandbys(t, r, 1)
	r.beforeHandOver = func(w *worker) {
		_ = killProcessGroup(w.cmd.Process)
		<-w.done
	}
	starts := 0
	r.onStart = func(int) { starts++ }
	job := f.job("SELECT count(*) FROM shop.orders")
	script, asked := job.ViewsSQL, 0
	job.ViewsSQL, job.ViewsFor = "", func(Refs) (string, error) { asked++; return script, nil }
	res, err := r.Run(context.Background(), job)
	if err != nil || len(res.Rows) != 1 || res.Phases.Standby {
		t.Fatalf("err = %v, rows = %v, phases %+v", err, res.Rows, res.Phases)
	}
	// Handed to the dead standby, then to its own worker; the statement's
	// views were asked for once, by the worker that ran it.
	if starts != 2 || asked != 1 {
		t.Fatalf("handed over %d time(s), views asked %d time(s)", starts, asked)
	}
}

// A worker that dies AFTER it has the job is a failed statement, as it is
// without standbys: the statement is not given to another worker.
func TestStandby_aStatementThatKillsItsWorkerIsNotRunAgain(t *testing.T) {
	f := newCopyFixture(t)
	r := newStandbyRunner(t, Config{MaxInFlight: 1})
	mustRun(t, r, f.job("SELECT 1"))
	waitStandbys(t, r, 1)
	starts := 0
	r.onStart = func(pid int) {
		starts++
		go func() {
			time.Sleep(400 * time.Millisecond)
			_ = killProcessGroup(&os.Process{Pid: pid})
		}()
	}
	_, err := r.Run(context.Background(), f.job("SELECT count(*) FROM range(100000000) a, range(100000000) b"))
	var we *WorkerError
	if !errors.As(err, &we) || starts != 1 {
		t.Fatalf("err = %v after %d hand-over(s), want one worker failure", err, starts)
	}
}

// Statements running at once stay at the slot count, a statement above it is
// refused exactly as without standbys, and there is never more than one
// standby per slot, however many statements come and go.
func TestStandby_theSlotCountStillBoundsStatementsAndStandbys(t *testing.T) {
	f := newCopyFixture(t)
	r := newStandbyRunner(t, Config{MaxInFlight: 2})
	mustRun(t, r, f.job("SELECT 1"))
	waitStandbys(t, r, 2)
	var handed atomic.Int32
	r.onStart = func(int) { handed.Add(1) }

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = r.Run(ctx, f.job("SELECT count(*) FROM range(100000000) a, range(100000000) b"))
		}()
	}
	deadline := time.Now().Add(10 * time.Second)
	for handed.Load() < 2 {
		if n := len(standbyPids(r)); n > 2 {
			t.Fatalf("%d standbys with 2 slots", n)
		}
		if time.Now().After(deadline) {
			t.Fatalf("the two statements never got their workers (%d)", handed.Load())
		}
		time.Sleep(5 * time.Millisecond)
	}
	if _, err := r.Run(context.Background(), f.job("SELECT 1")); !errors.Is(err, ErrBusy) {
		t.Fatalf("a third statement: %v, want busy", err)
	}
	if handed.Load() != 2 {
		t.Fatalf("%d workers were handed a job with 2 slots", handed.Load())
	}
	// The replacements started while the two statements run.
	waitStandbys(t, r, 2)
	cancel()
	wg.Wait()
	for range 6 {
		if _, err := r.Run(context.Background(), f.job("SELECT 1")); err != nil {
			t.Fatal(err)
		}
		if n := len(standbyPids(r)); n > 2 {
			t.Fatalf("%d standbys with 2 slots", n)
		}
	}
	waitStandbys(t, r, 2)
}

// With no statement for StandbyIdle the standbys are stopped, and the next
// statement starts its own worker and brings them back.
func TestStandby_stoppedAfterTheIdleTime(t *testing.T) {
	f := newCopyFixture(t)
	r := newStandbyRunner(t, Config{MaxInFlight: 1, StandbyIdle: 400 * time.Millisecond})
	mustRun(t, r, f.job("SELECT 1"))
	pids := waitStandbys(t, r, 1)
	deadline := time.Now().Add(10 * time.Second)
	for alive(pids[0]) || len(standbyPids(r)) != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("standby %d outlived the idle time (waiting: %v)", pids[0], standbyPids(r))
		}
		time.Sleep(20 * time.Millisecond)
	}
	if res, _ := mustRun(t, r, f.job("SELECT 1")); res.Phases.Standby {
		t.Fatal("a statement after the idle time ran on a standby that should be gone")
	}
	waitStandbys(t, r, 1)
}

// Close leaves no process behind and starts no more; statements still run.
func TestStandby_closeLeavesNoProcess(t *testing.T) {
	f := newCopyFixture(t)
	r := newStandbyRunner(t, Config{})
	mustRun(t, r, f.job("SELECT 1"))
	pids := waitStandbys(t, r, DefaultMaxInFlight)
	r.Close()
	for _, p := range pids {
		if alive(p) {
			t.Fatalf("standby %d survived Close", p)
		}
	}
	if res, _ := mustRun(t, r, f.job("SELECT 1")); res.Phases.Standby {
		t.Fatal("a statement after Close ran on a standby")
	}
	time.Sleep(300 * time.Millisecond)
	if pids := standbyPids(r); len(pids) != 0 {
		t.Fatalf("standbys after Close: %v", pids)
	}
}

// The limits are the statement's, not the ones in force when the standby
// started, and a session setting does not carry from one statement to the
// next: each ran in its own process.
func TestStandby_eachStatementItsOwnLimitsAndSession(t *testing.T) {
	f := newCopyFixture(t)
	r := newStandbyRunner(t, Config{MaxInFlight: 1})
	mustRun(t, r, f.job("SELECT 1"))
	waitStandbys(t, r, 1)
	job := f.job("SELECT current_setting('threads'), current_setting('TimeZone')")
	job.Limits = Limits{Threads: 1}
	job.Session.TimeZone = "America/Bogota"
	res, _ := mustRun(t, r, job)
	if got, _ := json.Marshal(res.Rows); !res.Phases.Standby || string(got) != `[[1,"America/Bogota"]]` {
		t.Fatalf("on a standby = %v: %s", res.Phases.Standby, got)
	}
	waitStandbys(t, r, 1)
	res, _ = mustRun(t, r, f.job("SELECT current_setting('threads'), current_setting('TimeZone')"))
	if got, _ := json.Marshal(res.Rows); !res.Phases.Standby || string(got) != `[[2,"UTC"]]` {
		t.Fatalf("the next statement saw the one before it: %s", got)
	}
}

// Standbys that keep dying before they are used are not started for a while.
func TestStandby_tooManyDeathsInARowPauseThem(t *testing.T) {
	f := newCopyFixture(t)
	r := newStandbyRunner(t, Config{MaxInFlight: 1})
	for i := range standbyMaxDeaths {
		mustRun(t, r, f.job("SELECT 1"))
		if i == 0 {
			waitStandbys(t, r, 1)
		} else if len(waitStandbysOrNone(r, 1)) == 0 {
			t.Fatalf("no standby after statement %d, before the pause", i+1)
		}
		_ = killProcessGroup(r.standbys[0].cmd.Process)
		<-r.standbys[0].done
	}
	mustRun(t, r, f.job("SELECT 1"))
	time.Sleep(500 * time.Millisecond)
	if pids := standbyPids(r); len(pids) != 0 {
		t.Fatalf("a standby was started after %d died in a row: %v", standbyMaxDeaths, pids)
	}
	r.mu.Lock()
	off := time.Until(r.standbyOffUntil)
	r.mu.Unlock()
	if off <= 0 || off > standbyOffFor {
		t.Fatalf("paused for %v, want up to %v", off, standbyOffFor)
	}
}

func waitStandbysOrNone(r *Runner, n int) []int {
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if pids := standbyPids(r); len(pids) == n {
			return pids
		}
		time.Sleep(10 * time.Millisecond)
	}
	return nil
}
