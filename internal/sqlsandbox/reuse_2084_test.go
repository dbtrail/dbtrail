//go:build unix

package sqlsandbox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// #2084: a worker that answers statements one after another. What a statement
// can observe must not change: its own limits, its own session, its own
// directories, nothing of the statement before it. Only the process is kept.

func newReuseRunner(t *testing.T, cfg Config) *Runner {
	t.Helper()
	if cfg.WorkerStatements == 0 {
		cfg.WorkerStatements = 100
	}
	return newStandbyRunner(t, cfg)
}

func rowsJSON(t *testing.T, res Result) string {
	t.Helper()
	b, err := json.Marshal(res.Rows)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// One slot: the worker that answered is the one that answers next, and its
// DuckDB was opened ahead of every statement but the first.
func TestReuse_oneWorkerAnswersStatementAfterStatement(t *testing.T) {
	f := newCopyFixture(t)
	r := newReuseRunner(t, Config{MaxInFlight: 1})
	_, first := mustRun(t, r, f.job("SELECT 1"))
	for i := range 5 {
		res, pid := mustRun(t, r, f.job(fmt.Sprintf("SELECT %d + count(*) FROM shop.orders", i)))
		if pid != first {
			t.Fatalf("statement %d ran in %d, want the first worker %d again", i, pid, first)
		}
		if !res.Phases.Standby || res.Phases.Open != 0 {
			t.Fatalf("statement %d: %+v, want a worker that was already open", i, res.Phases)
		}
		if len(res.Rows) != 1 {
			t.Fatalf("statement %d: rows = %v", i, res.Rows)
		}
	}
	if !alive(first) {
		t.Fatalf("worker %d is gone after its statements", first)
	}
}

// Off unless asked for: with standbys alone, a worker still serves one.
func TestReuse_offUnlessAskedFor(t *testing.T) {
	f := newCopyFixture(t)
	r := newStandbyRunner(t, Config{MaxInFlight: 1})
	_, first := mustRun(t, r, f.job("SELECT 1"))
	waitStandbys(t, r, 1)
	_, second := mustRun(t, r, f.job("SELECT 1"))
	if first == second || alive(first) || alive(second) {
		t.Fatalf("workers %d and %d: want two processes, both gone", first, second)
	}
}

// Nothing of a statement is there for the next one on the same worker: not
// its views, not its tables' schema, not its variables, its time zone, its
// threads, its memory, nor the directories it could read.
func TestReuse_theNextStatementSeesNothingOfTheOneBefore(t *testing.T) {
	f := newCopyFixture(t)
	other := writeOutsideParquet(t)
	r := newReuseRunner(t, Config{MaxInFlight: 1})

	job := f.job("SELECT current_setting('threads'), current_setting('TimeZone'), current_setting('memory_limit'), getvariable('left_behind'), (SELECT count(*) FROM left_behind.t)")
	job.ViewsSQL += "\nCREATE SCHEMA left_behind; CREATE VIEW left_behind.t AS SELECT 1 AS a; SET VARIABLE left_behind = 7;"
	job.Limits = Limits{Threads: 1, MemoryLimit: "600MiB"}
	job.Session.TimeZone = "America/Bogota"
	job.Schema = "shop"
	res, first := mustRun(t, r, job)
	if got := rowsJSON(t, res); got != `[[1,"America/Bogota","600.0 MiB",7,1]]` {
		t.Fatalf("the first statement: %s", got)
	}

	res, second := mustRun(t, r, f.job("SELECT current_setting('threads'), current_setting('TimeZone'), current_setting('memory_limit'), getvariable('left_behind'), current_schema()"))
	if second != first {
		t.Fatalf("the second statement ran in %d, not in the first worker %d", second, first)
	}
	if got := rowsJSON(t, res); got != `[[2,"UTC","2.0 GiB",null,"main"]]` {
		t.Fatalf("the next statement saw the one before it: %s", got)
	}
	_, err := r.Run(context.Background(), f.job("SELECT * FROM left_behind.t"))
	var qe *QueryError
	if !errors.As(err, &qe) {
		t.Fatalf("a view of the statement before is still there: %v", err)
	}

	// The directories: a statement over another copy, then one over this
	// copy that reaches for the other's file.
	elsewhere := Job{CopyDirs: []string{strings.TrimSuffix(other, "/outside.parquet")}, SQL: "SELECT count(*) FROM read_parquet('" + other + "')"}
	if strings.TrimSuffix(other, "/outside.parquet") == other {
		t.Fatalf("fixture: %s", other)
	}
	if _, third := mustRun(t, r, elsewhere); third != first {
		t.Fatalf("the third statement ran in %d, not in %d", third, first)
	}
	_, err = r.Run(context.Background(), f.job("SELECT count(*) FROM read_parquet('"+other+"')"))
	if !errors.As(err, &qe) || !strings.Contains(qe.Message, "Permission Error") {
		t.Fatalf("a directory the statement before could read is still readable: %v", err)
	}
}

// A statement that fails by its own doing (refused, an engine error, a
// result past its cap) leaves the worker fit for the next, which gets ITS
// answer and not what was left of the failure.
func TestReuse_aFailedStatementDoesNotSpoilTheNext(t *testing.T) {
	f := newCopyFixture(t)
	r := newReuseRunner(t, Config{MaxInFlight: 1})
	_, first := mustRun(t, r, f.job("SELECT 1"))

	var re *RefusedError
	if _, err := r.Run(context.Background(), f.job("DROP VIEW shop.orders")); !errors.As(err, &re) {
		t.Fatalf("a DROP: %v", err)
	}
	var qe *QueryError
	if _, err := r.Run(context.Background(), f.job("SELECT no_such_column FROM shop.orders")); !errors.As(err, &qe) {
		t.Fatalf("a bad column: %v", err)
	}
	big := f.job("SELECT repeat('x', 1000) FROM range(1000)")
	big.Limits = Limits{MaxResultBytes: 10_000}
	if _, err := r.Run(context.Background(), big); !errors.Is(err, ErrResultTooLarge) {
		t.Fatalf("a result past its cap: %v", err)
	}
	res, pid := mustRun(t, r, f.job("SELECT 41 + 1"))
	if got := rowsJSON(t, res); got != `[[42]]` {
		t.Fatalf("after three failures: %s", got)
	}
	if pid != first {
		t.Fatalf("the worker was replaced (%d, then %d) by statements that only failed themselves", first, pid)
	}
}

// A statement stopped at its timeout, or by its caller, takes its worker
// with it: that process is gone, and the next statement runs on another.
func TestReuse_aStatementStoppedTakesItsWorkerWithIt(t *testing.T) {
	f := newCopyFixture(t)
	r := newReuseRunner(t, Config{MaxInFlight: 1})
	_, first := mustRun(t, r, f.job("SELECT 1"))

	slow := f.job("SELECT count(*) FROM range(100000000000) a, range(1000) b")
	slow.Limits = Limits{Timeout: 500 * time.Millisecond}
	var te *TimeoutError
	if _, err := r.Run(context.Background(), slow); !errors.As(err, &te) {
		t.Fatalf("a statement past its timeout: %v", err)
	}
	if alive(first) {
		t.Fatalf("worker %d outlived the statement that timed out in it", first)
	}
	res, second := mustRun(t, r, f.job("SELECT 2"))
	if second == first || rowsJSON(t, res) != `[[2]]` {
		t.Fatalf("after a timeout: worker %d (was %d), rows %s", second, first, rowsJSON(t, res))
	}
	// And the one that replaced it stays, like the first did: the slot did
	// not lose its worker for good with the statement that was stopped.
	if _, again := mustRun(t, r, f.job("SELECT 2")); again != second {
		t.Fatalf("after a timeout the slot's worker is not kept: %d, then %d", second, again)
	}

	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan int, 1)
	r.onStart = func(pid int) { started <- pid }
	done := make(chan error, 1)
	go func() {
		_, err := r.Run(ctx, f.job("SELECT count(*) FROM range(100000000000) a, range(1000) b"))
		done <- err
	}()
	pid := <-started
	time.Sleep(200 * time.Millisecond)
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("a cancelled statement: %v", err)
	}
	if alive(pid) {
		t.Fatalf("worker %d outlived the statement its caller left", pid)
	}
	r.onStart = nil
	res, third := mustRun(t, r, f.job("SELECT 3"))
	if third == pid || rowsJSON(t, res) != `[[3]]` {
		t.Fatalf("after a cancel: worker %d (was %d), rows %s", third, pid, rowsJSON(t, res))
	}
	if _, again := mustRun(t, r, f.job("SELECT 3")); again != third {
		t.Fatalf("after a cancel the slot's worker is not kept: %d, then %d", third, again)
	}
	r.mu.Lock()
	lent := r.lent
	r.mu.Unlock()
	if lent != 0 {
		t.Fatalf("%d workers counted as running with no statement", lent)
	}
}

// A statement whose caller leaves in the instant its answer arrives still
// gets the answer, and the worker, which the cancel set out to kill, is not
// put back for the next statement.
func TestReuse_aWorkerBeingKilledIsNotKept(t *testing.T) {
	f := newCopyFixture(t)
	r := newReuseRunner(t, Config{MaxInFlight: 1})
	ctx, cancel := context.WithCancel(context.Background())
	// No wait after it: the kill has been set off and has not happened, which
	// is the instant in question.
	r.beforeKeep = func(*worker) { cancel() }
	pid := 0
	r.onStart = func(p int) { pid = p }
	res, err := r.Run(ctx, f.job("SELECT 7"))
	if err != nil || rowsJSON(t, res) != `[[7]]` {
		t.Fatalf("the statement that had answered: %s, %v", rowsJSON(t, res), err)
	}
	r.beforeKeep = nil
	if alive(pid) {
		t.Fatalf("worker %d was neither kept nor stopped", pid)
	}
	// It never waited for a statement: one that had would be found dead
	// among the waiting ones, and counted.
	r.topUps.Wait()
	r.mu.Lock()
	r.pruneStandbysLocked()
	deaths := r.standbyDeaths
	r.mu.Unlock()
	if deaths != 0 {
		t.Fatalf("worker %d was put back to wait while it was being killed", pid)
	}
}

// A worker whose session failed (it could not install what it was given, or
// it panicked) is not given another statement.
func TestReuse_aWorkerWhoseSessionFailedIsReplaced(t *testing.T) {
	f := newCopyFixture(t)
	r := newReuseRunner(t, Config{MaxInFlight: 1})
	_, first := mustRun(t, r, f.job("SELECT 1"))
	broken := f.job("SELECT 1")
	broken.ViewsSQL = "THIS IS NOT A VIEWS SCRIPT"
	var we *WorkerError
	if _, err := r.Run(context.Background(), broken); !errors.As(err, &we) {
		t.Fatalf("a views script that does not install: %v", err)
	}
	if alive(first) {
		t.Fatalf("worker %d is kept after its session failed", first)
	}
	res, second := mustRun(t, r, f.job("SELECT 2"))
	if second == first || rowsJSON(t, res) != `[[2]]` {
		t.Fatalf("after a failed session: worker %d (was %d), rows %s", second, first, rowsJSON(t, res))
	}
}

// The deadline of one statement is not the worker's: a worker that waits
// longer than its last statement's timeout is still there for the next.
func TestReuse_theLastStatementsDeadlineDoesNotStopTheWorker(t *testing.T) {
	f := newCopyFixture(t)
	r := newReuseRunner(t, Config{MaxInFlight: 1})
	quick := f.job("SELECT 1")
	quick.Limits = Limits{Timeout: time.Second}
	_, first := mustRun(t, r, quick)
	time.Sleep(time.Second + selfDeadlineGrace + 500*time.Millisecond)
	if _, second := mustRun(t, r, f.job("SELECT 2")); second != first {
		t.Fatalf("worker %d was gone after its statement's deadline; the next ran in %d", first, second)
	}
}

// After WorkerStatements a worker is replaced, and the one it replaces is
// gone.
func TestReuse_aWorkerIsReplacedAfterItsStatements(t *testing.T) {
	f := newCopyFixture(t)
	r := newReuseRunner(t, Config{MaxInFlight: 1, WorkerStatements: 3})
	var pids []int
	for range 7 {
		_, pid := mustRun(t, r, f.job("SELECT 1"))
		pids = append(pids, pid)
		// The replacement of a worker that served its last starts off the
		// statement; wait so the next one finds it.
		waitStandbys(t, r, 1)
	}
	if pids[0] != pids[1] || pids[1] != pids[2] || pids[3] == pids[2] || pids[3] != pids[4] || pids[4] != pids[5] || pids[6] == pids[5] {
		t.Fatalf("workers, want three statements each: %v", pids)
	}
	if alive(pids[0]) || alive(pids[3]) {
		t.Fatalf("a replaced worker is still there: %v", pids)
	}
}

// retire is the rule for replacing a worker: its count of statements, or the
// memory it still holds with no statement in it.
func TestReuse_retireRule(t *testing.T) {
	const mib = int64(1 << 20)
	for _, c := range []struct {
		name          string
		served, most  int
		resident      int64
		memoryLimit   string
		wantRetire    bool
		wantThreshold int64
	}{
		{"fresh", 1, 100, 40 * mib, "2048MiB", false, 256 * mib},
		{"its last statement", 100, 100, 40 * mib, "2048MiB", true, 256 * mib},
		{"past its last", 101, 100, 0, "2048MiB", true, 256 * mib},
		{"holds an eighth of the limit", 1, 100, 256 * mib, "2048MiB", false, 256 * mib},
		{"holds more than an eighth", 1, 100, 256*mib + 1, "2048MiB", true, 256 * mib},
		{"a small limit has a floor", 1, 100, 100 * mib, "512MiB", false, retireResidentFloor},
		{"over the floor", 1, 100, retireResidentFloor + 1, "512MiB", true, retireResidentFloor},
		{"resident unknown", 1, 100, 0, "2048MiB", false, 256 * mib},
		{"a limit that does not parse uses the floor", 1, 100, retireResidentFloor + 1, "lots", true, retireResidentFloor},
	} {
		if got := retireResident(c.memoryLimit); got != c.wantThreshold {
			t.Errorf("%s: threshold = %d, want %d", c.name, got, c.wantThreshold)
		}
		if got := retire(c.served, c.most, c.resident, c.memoryLimit); got != c.wantRetire {
			t.Errorf("%s: retire = %v, want %v", c.name, got, c.wantRetire)
		}
	}
}

// The views a statement asks for (#2029) work on every statement of a
// worker, and a statement whose views are refused costs the worker nothing.
func TestReuse_askingForViewsOnEveryStatement(t *testing.T) {
	f := newCopyFixture(t)
	r := newReuseRunner(t, Config{MaxInFlight: 1})
	asked := 0
	ask := func(sqlText string) Job {
		job := f.job(sqlText)
		views := job.ViewsSQL
		job.ViewsSQL = ""
		job.ViewsFor = func(Refs) (string, error) {
			asked++
			return views, nil
		}
		return job
	}
	_, first := mustRun(t, r, ask("SELECT count(*) FROM shop.orders"))
	for i := range 3 {
		res, pid := mustRun(t, r, ask("SELECT count(*) FROM shop.orders"))
		if pid != first || len(res.Rows) != 1 {
			t.Fatalf("statement %d: worker %d (first %d), rows %v", i, pid, first, res.Rows)
		}
	}
	if asked != 4 {
		t.Fatalf("the views were asked for %d times, want once per statement (4)", asked)
	}

	// A statement whose views are refused does not run at all: this one
	// would run until its timeout if the worker went on without them.
	refused := ask("SELECT count(*) FROM range(100000000000) a, range(1000) b")
	refused.Limits = Limits{Timeout: 5 * time.Second}
	why := &MayHaveChangedError{Reason: "shop.orders may have changed"}
	refused.ViewsFor = func(Refs) (string, error) { return "", why }
	_, err := r.Run(context.Background(), refused)
	var got *MayHaveChangedError
	if !errors.As(err, &got) || got != why {
		t.Fatalf("views refused: %v, want the refusal itself", err)
	}
	// A statement the worker refuses before it asks: its one line is the
	// result, not a question.
	var re *RefusedError
	if _, err := r.Run(context.Background(), ask("DELETE FROM shop.orders")); !errors.As(err, &re) {
		t.Fatalf("a DELETE: %v", err)
	}
	res, pid := mustRun(t, r, ask("SELECT 5"))
	if pid != first || rowsJSON(t, res) != `[[5]]` {
		t.Fatalf("after refused views: worker %d (first %d), rows %s", pid, first, rowsJSON(t, res))
	}
}

// The slot count bounds the processes too: with statements arriving one after
// another and at once, there are never more workers than slots, waiting and
// running together, and the same ones keep answering.
func TestReuse_noMoreWorkersThanSlots(t *testing.T) {
	f := newCopyFixture(t)
	const slots = 3
	r := newReuseRunner(t, Config{MaxInFlight: slots, MaxWait: 30 * time.Second, MaxWaiters: 100})
	var mu sync.Mutex
	seen := map[int]int{}
	r.onStart = func(pid int) {
		mu.Lock()
		seen[pid]++
		mu.Unlock()
	}
	if _, err := r.Run(context.Background(), f.job("SELECT 1")); err != nil {
		t.Fatal(err)
	}
	waitStandbys(t, r, slots)
	var wg sync.WaitGroup
	errs := make(chan error, 60)
	for i := range 60 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			job := f.job(fmt.Sprintf("SELECT %d", i))
			if i%2 == 1 {
				// Half of them ask for their views, as the console's do.
				views := job.ViewsSQL
				job.ViewsSQL, job.ViewsFor = "", func(Refs) (string, error) { return views, nil }
			}
			res, err := r.Run(context.Background(), job)
			if err == nil && fmt.Sprint(res.Rows) != fmt.Sprintf("[[%d]]", i) {
				err = fmt.Errorf("statement %d answered %v", i, res.Rows)
			}
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	waitStandbys(t, r, slots)
	if len(seen) > slots {
		t.Fatalf("%d workers answered 61 statements on %d slots: %v", len(seen), slots, seen)
	}
	// And none was started only to be stopped: a worker that is out with a
	// statement still holds its slot.
	if n := r.started.Load(); n != slots {
		t.Fatalf("%d worker processes were started for %d slots", n, slots)
	}
	r.mu.Lock()
	lent := r.lent
	r.mu.Unlock()
	if lent != 0 {
		t.Fatalf("%d workers counted as running with no statement", lent)
	}
}

// A worker that died while it waited is not handed a statement.
func TestReuse_aWorkerThatDiedWaitingCostsNothing(t *testing.T) {
	f := newCopyFixture(t)
	r := newReuseRunner(t, Config{MaxInFlight: 1})
	_, first := mustRun(t, r, f.job("SELECT 1"))
	if got := killStandby(t, r); got != first {
		t.Fatalf("the waiting worker is %d, want the one that answered, %d", got, first)
	}
	res, second := mustRun(t, r, f.job("SELECT 2"))
	if second == first || rowsJSON(t, res) != `[[2]]` {
		t.Fatalf("after the worker died waiting: worker %d, rows %s", second, rowsJSON(t, res))
	}
}

// Close leaves no process, and a statement after it runs on a worker of its
// own that is not kept.
func TestReuse_closeLeavesNoProcess(t *testing.T) {
	f := newCopyFixture(t)
	r := newReuseRunner(t, Config{MaxInFlight: 2})
	mustRun(t, r, f.job("SELECT 1"))
	pids := waitStandbys(t, r, 2)
	r.Close()
	for _, p := range pids {
		if alive(p) {
			t.Fatalf("worker %d survived Close", p)
		}
	}
	_, pid := mustRun(t, r, f.job("SELECT 1"))
	time.Sleep(200 * time.Millisecond)
	if alive(pid) || len(standbyPids(r)) != 0 {
		t.Fatalf("after Close worker %d is kept: %v", pid, standbyPids(r))
	}
}

// The worker says what it holds between statements only where the platform
// tells it; elsewhere the count of statements alone replaces it.
func TestReuse_residentIsReportedWhereKnown(t *testing.T) {
	got := residentBytes()
	if _, err := os.Stat("/proc/self/statm"); err == nil {
		if got < 1<<20 || got > retireResidentFloor {
			t.Fatalf("resident = %d on a host with /proc, want this process's own memory, under what replaces a worker", got)
		}
		return
	}
	if got != 0 {
		t.Fatalf("resident = %d on a host without /proc, want 0 (unknown)", got)
	}
}

// A worker that dies with a statement in it fails that statement, which is
// not run again, and the next statement has a worker.
func TestReuse_aWorkerKilledMidStatementFailsItOnce(t *testing.T) {
	f := newCopyFixture(t)
	r := newReuseRunner(t, Config{MaxInFlight: 1})
	mustRun(t, r, f.job("SELECT 1"))
	before := r.started.Load()
	r.onStart = func(pid int) {
		go func() {
			time.Sleep(200 * time.Millisecond)
			_ = syscall.Kill(-pid, syscall.SIGKILL)
		}()
	}
	_, err := r.Run(context.Background(), f.job("SELECT count(*) FROM range(100000000000) a, range(1000) b"))
	var we *WorkerError
	if !errors.As(err, &we) {
		t.Fatalf("a statement whose worker was killed: %v", err)
	}
	r.onStart = nil
	r.topUps.Wait()
	if n := r.started.Load() - before; n > 1 {
		t.Fatalf("%d workers were started after the kill; the statement was run again", n)
	}
	if res, _ := mustRun(t, r, f.job("SELECT 2")); rowsJSON(t, res) != `[[2]]` {
		t.Fatalf("after a killed worker: %s", rowsJSON(t, res))
	}
}

// What a worker wrote to stderr during one statement is not reported with the
// failure of a later one, which may be another person's.
func TestReuse_aFailureDoesNotCarryAnEarlierStatementsStderr(t *testing.T) {
	f := newCopyFixture(t)
	r := newReuseRunner(t, Config{MaxInFlight: 1})
	// As the worker's own stderr would have it, had the statement made it
	// write there (a USE that was not applied, a setting refused ahead of
	// the job).
	r.beforeKeep = func(w *worker) { _, _ = w.stderr.Write([]byte("sql worker: somebody_elses_schema\n")) }
	_, pid := mustRun(t, r, f.job("SELECT 1"))
	r.beforeKeep = nil
	broken := f.job("SELECT 1")
	broken.ViewsSQL = "THIS IS NOT A VIEWS SCRIPT"
	r.onStart = func(p int) {
		if p != pid {
			t.Errorf("the failing statement ran in %d, not in the worker %d that ran the first", p, pid)
		}
	}
	_, err := r.Run(context.Background(), broken)
	var we *WorkerError
	if !errors.As(err, &we) {
		t.Fatalf("a views script that does not install: %v", err)
	}
	if strings.Contains(we.Stderr, "somebody_elses_schema") || strings.Contains(we.Error(), "somebody_elses_schema") {
		t.Fatalf("the failure carries the statement before it: %s", we.Error())
	}
}

// A kept worker that holds too much for the NEXT statement's memory limit is
// not handed that statement: its memory cgroup is sized from the limit, and
// the worker would be over it before running anything.
func TestReuse_aWorkerTooHeavyForTheNextStatementIsNotHandedIt(t *testing.T) {
	f := newCopyFixture(t)
	r := newReuseRunner(t, Config{MaxInFlight: 1})
	_, first := mustRun(t, r, f.job("SELECT 1"))
	r.mu.Lock()
	// As after a statement with a large limit: under an eighth of 8 GiB,
	// over an eighth of 2 GiB.
	r.standbys[0].resident = 600 << 20
	r.mu.Unlock()
	large := f.job("SELECT 1")
	large.Limits = Limits{MemoryLimit: "8192MiB"}
	if _, pid := mustRun(t, r, large); pid != first {
		t.Fatalf("a statement with room for it ran in %d, not in the kept worker %d", pid, first)
	}
	r.mu.Lock()
	r.standbys[0].resident = 600 << 20
	r.mu.Unlock()
	res, pid := mustRun(t, r, f.job("SELECT 2"))
	if pid == first || rowsJSON(t, res) != `[[2]]` {
		t.Fatalf("a statement with a 2 GiB limit ran in worker %d, which held 600 MiB (first %d): %s", pid, first, rowsJSON(t, res))
	}
	deadline := time.Now().Add(10 * time.Second)
	for alive(first) {
		if time.Now().After(deadline) {
			t.Fatalf("worker %d was passed over and left running", first)
		}
		time.Sleep(10 * time.Millisecond)
	}
	r.mu.Lock()
	deaths, lent := r.standbyDeaths, r.lent
	r.mu.Unlock()
	if deaths != 0 || lent != 0 {
		t.Fatalf("passing a worker over counted %d deaths and left %d lent", deaths, lent)
	}
}

// What the worker reports it holds reaches the Runner (where the platform
// tells the worker).
func TestReuse_theWorkersResidentReachesTheRunner(t *testing.T) {
	f := newCopyFixture(t)
	r := newReuseRunner(t, Config{MaxInFlight: 1})
	mustRun(t, r, f.job("SELECT 1"))
	r.mu.Lock()
	got := r.standbys[0].resident
	r.mu.Unlock()
	_, err := os.Stat("/proc/self/statm")
	if known := err == nil; known != (got > 1<<20) {
		t.Fatalf("resident = %d, /proc readable = %v", got, known)
	}
	// And after one short statement it is far from what replaces a worker:
	// a worker that crossed it at once would never answer a second.
	if got > retireResidentFloor/2 {
		t.Fatalf("a worker holds %d bytes of its own after SELECT 1, over half the %d that replace it", got, int64(retireResidentFloor))
	}
}

// A statement larger than the pipe's buffer reaches a kept worker whole.
func TestReuse_aStatementLargerThanThePipe(t *testing.T) {
	f := newCopyFixture(t)
	r := newReuseRunner(t, Config{MaxInFlight: 1})
	_, first := mustRun(t, r, f.job("SELECT 1"))
	long := "SELECT length('" + strings.Repeat("x", 300_000) + "')"
	for range 2 {
		res, pid := mustRun(t, r, f.job(long))
		if pid != first || rowsJSON(t, res) != `[[300000]]` {
			t.Fatalf("worker %d (first %d): %s", pid, first, rowsJSON(t, res))
		}
	}
}

// Kept workers stop after the idle time like the ones started ahead, and a
// Runner closed while a statement runs does not keep that statement's worker.
func TestReuse_idleAndCloseStopKeptWorkers(t *testing.T) {
	f := newCopyFixture(t)
	r := newReuseRunner(t, Config{MaxInFlight: 1, StandbyIdle: 300 * time.Millisecond})
	_, first := mustRun(t, r, f.job("SELECT 1"))
	deadline := time.Now().Add(10 * time.Second)
	for alive(first) {
		if time.Now().After(deadline) {
			t.Fatalf("worker %d still waits after the idle time", first)
		}
		time.Sleep(20 * time.Millisecond)
	}

	started := make(chan int, 1)
	r.onStart = func(pid int) { started <- pid }
	done := make(chan error, 1)
	go func() {
		_, err := r.Run(context.Background(), f.job("SELECT count(*) FROM range(300000000)"))
		done <- err
	}()
	pid := <-started
	r.Close()
	if err := <-done; err != nil {
		t.Fatalf("a statement running when the Runner was closed: %v", err)
	}
	time.Sleep(100 * time.Millisecond)
	r.mu.Lock()
	lent, waiting := r.lent, len(r.standbys)
	r.mu.Unlock()
	if alive(pid) || lent != 0 || waiting != 0 {
		t.Fatalf("after Close: worker %d alive = %v, lent %d, waiting %d", pid, alive(pid), lent, waiting)
	}
}

// A kept worker that is gone between being taken and being handed its job
// costs the statement nothing: it runs once, on a worker started for it,
// which takes the dead one's place and is kept in turn.
func TestReuse_goneBetweenTakenAndHandedOverRunsOnce(t *testing.T) {
	f := newCopyFixture(t)
	r := newReuseRunner(t, Config{MaxInFlight: 1})
	_, first := mustRun(t, r, f.job("SELECT 1"))
	r.beforeHandOver = func(w *worker) {
		_ = killProcessGroup(w.cmd.Process)
		<-w.done
	}
	job := f.job("SELECT count(*) FROM shop.orders")
	script, asked := job.ViewsSQL, 0
	job.ViewsSQL, job.ViewsFor = "", func(Refs) (string, error) { asked++; return script, nil }
	res, second := mustRun(t, r, job)
	r.beforeHandOver = nil
	if second == first || len(res.Rows) != 1 || asked != 1 {
		t.Fatalf("worker %d (the dead one was %d), rows %v, views asked %d time(s)", second, first, res.Rows, asked)
	}
	r.topUps.Wait()
	r.mu.Lock()
	lent, waiting := r.lent, len(r.standbys)
	r.mu.Unlock()
	if lent != 0 || waiting != 1 {
		t.Fatalf("after the statement: %d lent, %d waiting, want none and the one worker", lent, waiting)
	}
	if _, third := mustRun(t, r, f.job("SELECT 3")); third != second {
		t.Fatalf("the worker that took the dead one's place was not kept: %d, then %d", second, third)
	}
}

// A worker that cannot be started fails the statement and is not counted as
// one that is out with it.
func TestReuse_aWorkerThatDoesNotStartIsNotCounted(t *testing.T) {
	f := newCopyFixture(t)
	r := newReuseRunner(t, Config{MaxInFlight: 1})
	exe := r.exe
	r.exe = filepath.Join(t.TempDir(), "no-such-worker")
	var we *WorkerError
	if _, err := r.Run(context.Background(), f.job("SELECT 1")); !errors.As(err, &we) {
		t.Fatalf("a worker that cannot start: %v", err)
	}
	r.topUps.Wait()
	r.mu.Lock()
	lent := r.lent
	r.mu.Unlock()
	if lent != 0 {
		t.Fatalf("%d workers counted as running after one that never started", lent)
	}
	r.exe = exe
	_, first := mustRun(t, r, f.job("SELECT 1"))
	if _, second := mustRun(t, r, f.job("SELECT 1")); second != first {
		t.Fatalf("after a failed start the slot's worker is not kept: %d, then %d", first, second)
	}
}

// What a worker holds of its own is its resident set less the file-backed
// pages; a line that does not read is "not known", never a number.
func TestReuse_statmPrivate(t *testing.T) {
	for _, c := range []struct {
		line string
		want int64
	}{
		{"52000 9000 6000 100 0 30000 0\n", 3000 * 4096},
		{"52000 9000 9000 100 0 30000 0", 0},
		{"52000 9000 9500 100 0 30000 0", 0},
		{"52000 9000", 0},
		{"52000 nine 6000", 0},
		{"52000 9000 -1", 0},
		{"", 0},
	} {
		if got := statmPrivate(c.line, 4096); got != c.want {
			t.Errorf("statmPrivate(%q) = %d, want %d", c.line, got, c.want)
		}
	}
}
