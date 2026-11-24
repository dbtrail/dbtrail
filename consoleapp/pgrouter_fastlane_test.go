package consoleapp

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/pgshim"
	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
	"github.com/dbtrail/dbtrail/internal/stmtclock"
)

// testLanes is a lane gate over a pool of workers the test sets, and the
// runner's wait and line.
type testLanes struct {
	*pgCopyLanes
	workers atomic.Int32
}

func newTestLanes(workers int, maxWait time.Duration, maxWaiters int) *testLanes {
	l := &testLanes{}
	l.workers.Store(int32(workers))
	l.pgCopyLanes = &pgCopyLanes{
		workers:    func() int { return int(l.workers.Load()) },
		maxWait:    func() time.Duration { return maxWait },
		maxWaiters: func() int { return maxWaiters },
		recheck:    10 * time.Millisecond,
	}
	return l
}

var longRun = pgshim.RunOptions{}

// laneEnterAsync enters the gate on a goroutine and reports what it got.
type laneEntered struct {
	release func()
	err     error
}

func laneEnterAsync(ctx context.Context, l *pgCopyLanes, opts pgshim.RunOptions) chan laneEntered {
	ch := make(chan laneEntered, 1)
	go func() {
		r, err := l.enter(ctx, opts)
		ch <- laneEntered{r, err}
	}()
	return ch
}

func laneEnter(t *testing.T, l *pgCopyLanes, opts pgshim.RunOptions) func() {
	t.Helper()
	r, err := l.enter(context.Background(), opts)
	if err != nil {
		t.Fatalf("enter: %v", err)
	}
	return r
}

func laneWaitFor(t *testing.T, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !ok() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

func laneNotYet(t *testing.T, what string, ch chan laneEntered) {
	t.Helper()
	select {
	case e := <-ch:
		t.Fatalf("%s got in: %+v", what, e)
	case <-time.After(50 * time.Millisecond):
	}
}

func laneGotIn(t *testing.T, what string, ch chan laneEntered) func() {
	t.Helper()
	select {
	case e := <-ch:
		if e.err != nil {
			t.Fatalf("%s: %v", what, e.err)
		}
		return e.release
	case <-time.After(5 * time.Second):
		t.Fatalf("%s did not get in", what)
	}
	return nil
}

// Four workers: three long statements at once, the fourth waits, and a
// short one or a probe never does.
func TestPGCopyLanes_oneKeptForShortStatements(t *testing.T) {
	l := newTestLanes(4, sqlsandbox.NoMaxWait, 16)
	var held []func()
	for range 3 {
		held = append(held, laneEnter(t, l.pgCopyLanes, longRun))
	}
	if n := l.numbers(); n.LongLimit != 3 || n.LongAdmitted != 3 || n.LongWaiting != 0 || n.KeptForShort != 1 {
		t.Fatalf("numbers %+v", n)
	}
	fourth := laneEnterAsync(context.Background(), l.pgCopyLanes, longRun)
	laneNotYet(t, "a fourth long statement", fourth)
	laneWaitFor(t, "the fourth to be counted waiting", func() bool { return l.numbers().LongWaiting == 1 })
	for _, opts := range []pgshim.RunOptions{{FastLane: true}, {Probe: true}, {FastLane: true, Probe: true}} {
		r := laneEnter(t, l.pgCopyLanes, opts)
		r()
	}
	held[0]()
	laneGotIn(t, "the fourth, once one ended", fourth)()
	held[1]()
	held[2]()
	if n := l.numbers(); n.LongAdmitted != 0 || n.LongWaiting != 0 {
		t.Errorf("every place given back: %+v", n)
	}
}

// Those that wait get in in the order they came.
func TestPGCopyLanes_firstComeFirstServed(t *testing.T) {
	l := newTestLanes(2, sqlsandbox.NoMaxWait, 16)
	first := laneEnter(t, l.pgCopyLanes, longRun)
	var order []int
	var mu sync.Mutex
	var wg sync.WaitGroup
	for i := range 5 {
		ch := laneEnterAsync(context.Background(), l.pgCopyLanes, longRun)
		laneWaitFor(t, "the waiter to be in line", func() bool { return l.numbers().LongWaiting == i+1 })
		wg.Add(1)
		go func() {
			defer wg.Done()
			e := <-ch
			mu.Lock()
			order = append(order, i)
			mu.Unlock()
			e.release()
		}()
	}
	first()
	wg.Wait()
	for i, got := range order {
		if got != i {
			t.Fatalf("they got in in the order %v, want the order they came", order)
		}
	}
}

// One worker: there is no place to keep, and every statement takes its turn
// as before (the runner's own line).
func TestPGCopyLanes_oneWorkerNoLane(t *testing.T) {
	l := newTestLanes(1, sqlsandbox.NoMaxWait, 16)
	var held []func()
	for range 5 {
		held = append(held, laneEnter(t, l.pgCopyLanes, longRun))
	}
	if n := l.numbers(); n.LongLimit != 0 || n.KeptForShort != 0 || n.LongWaiting != 0 {
		t.Errorf("one worker: %+v", n)
	}
	for _, r := range held {
		r()
	}
	if n := l.numbers(); n.LongAdmitted != 0 {
		t.Errorf("after: %+v", n)
	}
}

// The workers changed while statements wait (a server turned to fresh mode,
// #2546): raised, the waiters get in; lowered to one, every waiter gets in.
func TestPGCopyLanes_workersChangeWhileWaiting(t *testing.T) {
	l := newTestLanes(2, sqlsandbox.NoMaxWait, 16)
	a := laneEnter(t, l.pgCopyLanes, longRun)
	b := laneEnterAsync(context.Background(), l.pgCopyLanes, longRun)
	laneNotYet(t, "a second long statement on two workers", b)
	l.workers.Store(3)
	rb := laneGotIn(t, "the second, with three workers", b)
	c := laneEnterAsync(context.Background(), l.pgCopyLanes, longRun)
	d := laneEnterAsync(context.Background(), l.pgCopyLanes, longRun)
	laneNotYet(t, "a third", c)
	l.workers.Store(1)
	rc := laneGotIn(t, "the third, with one worker", c)
	rd := laneGotIn(t, "the fourth, with one worker", d)
	for _, r := range []func(){a, rb, rc, rd} {
		r()
	}
	if n := l.numbers(); n.LongAdmitted != 0 || n.LongWaiting != 0 {
		t.Errorf("after: %+v", n)
	}
	// Lowered below what holds a place: no one new gets in until enough
	// have ended.
	l.workers.Store(4)
	held := []func(){laneEnter(t, l.pgCopyLanes, longRun), laneEnter(t, l.pgCopyLanes, longRun), laneEnter(t, l.pgCopyLanes, longRun)}
	l.workers.Store(3)
	e := laneEnterAsync(context.Background(), l.pgCopyLanes, longRun)
	held[0]()
	laneNotYet(t, "a long statement with two of a limit of two still running", e)
	held[1]()
	laneGotIn(t, "it, once one more ended", e)()
	held[2]()
}

// The wait is the runner's: past it the statement is refused as busy
// (copy_wait_timeout, 53000), and its place in line is given up.
func TestPGCopyLanes_waitRunsOut(t *testing.T) {
	l := newTestLanes(2, 80*time.Millisecond, 16)
	held := laneEnter(t, l.pgCopyLanes, longRun)
	defer held()
	start := time.Now()
	_, err := l.enter(context.Background(), longRun)
	var busy *sqlsandbox.BusyError
	if !errors.As(err, &busy) || !errors.Is(err, sqlsandbox.ErrBusy) {
		t.Fatalf("%v, want a BusyError", err)
	}
	if took := time.Since(start); took < 80*time.Millisecond {
		t.Errorf("refused after %v, before its wait ran out", took)
	}
	if busy.Waited != 80*time.Millisecond || busy.MaxInFlight != 2 {
		t.Errorf("%+v", busy)
	}
	// Said as the wait it was: short statements may have finished meanwhile,
	// which the runner's own text would deny.
	if msg := err.Error(); strings.Contains(msg, "no query finished") || strings.Contains(msg, "MySQL") ||
		!strings.Contains(msg, "for a turn among the long statements (1 run at once, and one more worker is kept") {
		t.Errorf("the refusal says %q", msg)
	}
	if n := l.numbers(); n.LongWaiting != 0 || n.LongAdmitted != 1 || n.RefusedWaitTimeout != 1 || n.RefusedQueueFull != 0 {
		t.Errorf("after the refusal: %+v", n)
	}
}

// The line is the runner's length: one more is refused at once, as a full
// line (copy_queue_full); so is any wait under a negative MaxWait.
func TestPGCopyLanes_fullLine(t *testing.T) {
	l := newTestLanes(2, sqlsandbox.NoMaxWait, 1)
	held := laneEnter(t, l.pgCopyLanes, longRun)
	ctx, cancel := context.WithCancel(context.Background())
	waiting := laneEnterAsync(ctx, l.pgCopyLanes, longRun)
	laneWaitFor(t, "one in line", func() bool { return l.numbers().LongWaiting == 1 })
	_, err := l.enter(context.Background(), longRun)
	var busy *sqlsandbox.BusyError
	if !errors.Is(err, sqlsandbox.ErrBusy) || errors.As(err, &busy) {
		t.Errorf("the line full: %v, want ErrBusy for a full line", err)
	}
	if n := l.numbers(); n.RefusedQueueFull != 1 || n.RefusedWaitTimeout != 0 {
		t.Errorf("the line full, counted: %+v", n)
	}
	cancel()
	if e := <-waiting; !errors.Is(e.err, context.Canceled) {
		t.Errorf("the waiter, cancelled: %v", e.err)
	}
	held()

	never := newTestLanes(2, -1, 16)
	h := laneEnter(t, never.pgCopyLanes, longRun)
	defer h()
	if _, err := never.enter(context.Background(), longRun); !errors.Is(err, sqlsandbox.ErrBusy) {
		t.Errorf("a negative wait: %v, want ErrBusy at once", err)
	}
}

// A client that leaves while it waits gives its place in line up, and holds
// no place after.
func TestPGCopyLanes_clientLeaves(t *testing.T) {
	l := newTestLanes(2, sqlsandbox.NoMaxWait, 16)
	held := laneEnter(t, l.pgCopyLanes, longRun)
	ctx, cancel := context.WithCancel(context.Background())
	ch := laneEnterAsync(ctx, l.pgCopyLanes, longRun)
	laneWaitFor(t, "it in line", func() bool { return l.numbers().LongWaiting == 1 })
	cancel()
	if e := <-ch; !errors.Is(e.err, context.Canceled) || e.release != nil {
		t.Errorf("%+v, want the context's error and no place", e)
	}
	if n := l.numbers(); n.LongWaiting != 0 || n.LongAdmitted != 1 {
		t.Errorf("after it left: %+v", n)
	}
	held()
	// The place is free for the next one.
	laneEnter(t, l.pgCopyLanes, longRun)()
	// Already gone before it asked: nothing taken.
	gone, stop := context.WithCancel(context.Background())
	stop()
	full := laneEnter(t, l.pgCopyLanes, longRun)
	if _, err := l.enter(gone, longRun); !errors.Is(err, context.Canceled) {
		t.Errorf("a client gone before it asked: %v", err)
	}
	full()
	if n := l.numbers(); n.LongAdmitted != 0 || n.LongWaiting != 0 {
		t.Errorf("at the end: %+v", n)
	}
}

// Handed a place in the instant it leaves: the place goes to the next in
// line, not lost. Run many times, so the race is hit.
func TestPGCopyLanes_grantAsItLeaves(t *testing.T) {
	for range 200 {
		l := newTestLanes(2, sqlsandbox.NoMaxWait, 16)
		held := laneEnter(t, l.pgCopyLanes, longRun)
		ctx, cancel := context.WithCancel(context.Background())
		ch := laneEnterAsync(ctx, l.pgCopyLanes, longRun)
		laneWaitFor(t, "it in line", func() bool { return l.numbers().LongWaiting == 1 })
		go cancel()
		held()
		if e := <-ch; e.err == nil {
			e.release()
		}
		laneWaitFor(t, "every place given back", func() bool { n := l.numbers(); return n.LongAdmitted == 0 && n.LongWaiting == 0 })
	}
}

// Giving a place back twice gives it back once.
func TestPGCopyLanes_releaseTwice(t *testing.T) {
	l := newTestLanes(3, sqlsandbox.NoMaxWait, 16)
	a := laneEnter(t, l.pgCopyLanes, longRun)
	b := laneEnter(t, l.pgCopyLanes, longRun)
	a()
	a()
	if n := l.numbers(); n.LongAdmitted != 1 {
		t.Errorf("%+v, want b still counted", n)
	}
	b()
}

// No lanes (a port served without them): nothing waits.
func TestPGCopyLanes_nil(t *testing.T) {
	var l *pgCopyLanes
	for range 10 {
		r, err := l.enter(context.Background(), longRun)
		if err != nil || r == nil {
			t.Fatalf("%v", err)
		}
		r()
	}
	if n := l.numbers(); n != (pgCopyLanesDoc{}) {
		t.Errorf("%+v", n)
	}
}

// The wait does not count against a statement's time cap, as the runner's
// line does not (stmtclock).
func TestPGCopyLanes_waitIsNotTheStatementRunning(t *testing.T) {
	l := newTestLanes(2, sqlsandbox.NoMaxWait, 16)
	held := laneEnter(t, l.pgCopyLanes, longRun)
	var g stmtclock.Group
	ctx, cancel := g.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	ch := laneEnterAsync(ctx, l.pgCopyLanes, longRun)
	time.Sleep(250 * time.Millisecond)
	held()
	r := laneGotIn(t, "the waiter", ch)
	defer r()
	if err := ctx.Err(); err != nil {
		t.Errorf("the statement's clock ran while it waited: %v", err)
	}
}

// run: a short statement runs while the long ones hold every place they
// may, and a long one's place is given back when it returns, error or not.
func TestPGCopyLanes_run(t *testing.T) {
	l := newTestLanes(2, sqlsandbox.NoMaxWait, 16)
	block := make(chan struct{})
	started := make(chan struct{})
	go func() {
		_, _ = l.run(context.Background(), longRun, func(context.Context) (sqlsandbox.Result, error) {
			close(started)
			<-block
			return sqlsandbox.Result{}, errors.New("the copy failed")
		})
	}()
	<-started
	done := make(chan error, 1)
	go func() {
		_, err := l.run(context.Background(), pgshim.RunOptions{FastLane: true}, func(context.Context) (sqlsandbox.Result, error) {
			return sqlsandbox.Result{}, nil
		})
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a short statement waited behind a long one")
	}
	// Two workers: the long limit is 1, held by the first, so a second long
	// one waits, here until its client gives up.
	ran := false
	wait, stop := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer stop()
	_, err := l.run(wait, longRun, func(context.Context) (sqlsandbox.Result, error) { ran = true; return sqlsandbox.Result{}, nil })
	if ran || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("a second long statement beside the first on two workers: ran %v, %v", ran, err)
	}
	close(block)
	// The first returned an error: its place is given back all the same.
	laneWaitFor(t, "the failed long statement's place", func() bool { return l.numbers().LongAdmitted == 0 })
	if _, err := l.run(context.Background(), longRun, func(context.Context) (sqlsandbox.Result, error) { ran = true; return sqlsandbox.Result{}, nil }); err != nil || !ran {
		t.Errorf("a long statement once the first ended: ran %v, %v", ran, err)
	}
}

// A connection whose copy refuses its statements as unavailable (at once,
// before any worker) does not wait its turn among the long statements to be
// refused; once the copy answers again, it does.
func TestPGConnLane_unavailableDoesNotWait(t *testing.T) {
	l := newTestLanes(2, sqlsandbox.NoMaxWait, 16)
	c := &pgConnLane{lanes: l.pgCopyLanes}
	unavailable := func(context.Context) (sqlsandbox.Result, error) {
		return sqlsandbox.Result{}, &sqlsandbox.UnavailableError{Reason: "the copy lives on S3"}
	}
	answered := func(context.Context) (sqlsandbox.Result, error) { return sqlsandbox.Result{}, nil }
	// The first one, the lane free: refused as unavailable.
	var refused *sqlsandbox.UnavailableError
	if _, err := c.run(context.Background(), longRun, unavailable); !errors.As(err, &refused) {
		t.Fatalf("%v", err)
	}
	held := laneEnter(t, l.pgCopyLanes, longRun)
	defer held()
	// The lane full: the next one is refused at once, as unavailable, not
	// as busy after a wait.
	done := make(chan error, 1)
	go func() { _, err := c.run(context.Background(), longRun, unavailable); done <- err }()
	select {
	case err := <-done:
		if !errors.As(err, &refused) {
			t.Errorf("%v, want the copy's refusal", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a statement the copy refuses at once waited its turn")
	}
	// Answered: the connection's next long statement waits its turn again.
	if _, err := c.run(context.Background(), longRun, answered); err != nil {
		t.Fatal(err)
	}
	wait, stop := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer stop()
	ran := false
	if _, err := c.run(wait, longRun, func(context.Context) (sqlsandbox.Result, error) { ran = true; return sqlsandbox.Result{}, nil }); ran || !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("after an answer, a long statement on a full lane: ran %v, %v; want it to wait", ran, err)
	}
	// Another connection never refused is not let past by this one.
	other := &pgConnLane{lanes: l.pgCopyLanes}
	wait2, stop2 := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer stop2()
	if _, err := other.run(wait2, longRun, answered); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("another connection: %v, want it to wait", err)
	}
}

// The status document carries the lane's numbers; without lanes it says
// nothing of them.
func TestPGRouterStatus_copyLanes(t *testing.T) {
	routes := newPGRouteStats(time.Now(), false)
	if d := routes.doc(""); d.CopyLanes != nil {
		t.Errorf("no lanes: %+v", d.CopyLanes)
	}
	l := newTestLanes(4, sqlsandbox.NoMaxWait, 16)
	routes.lanes = l.pgCopyLanes
	held := laneEnter(t, l.pgCopyLanes, longRun)
	defer held()
	rec := httptest.NewRecorder()
	pgRouterStatusHandler(routes, "127.0.0.1:5433").ServeHTTP(rec, httptest.NewRequest("GET", "/status", nil))
	var doc map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &doc); err != nil {
		t.Fatal(err)
	}
	lanes, _ := doc["copy_lanes"].(map[string]any)
	want := map[string]float64{"kept_for_short": 1, "long_limit": 3, "long_admitted": 1, "long_waiting": 0, "refused_wait_timeout": 0, "refused_queue_full": 0}
	for k, v := range want {
		if got, ok := lanes[k].(float64); !ok || got != v {
			t.Errorf("copy_lanes.%s = %v, want %v (%s)", k, lanes[k], v, rec.Body.String())
		}
	}
}

// The start-up line says a worker is kept, and how many long statements run
// at once; with one worker, that none is.
func TestPGRouterStartupLines_fastLane(t *testing.T) {
	st := pgRouterSettings{Listen: "127.0.0.1:5433", Workers: 4, Threads: 2, MaxConns: 16}
	out := pgRouterStartupLines(st, "2GB", "the default", nil, 1, true)
	if !strings.Contains(out, "one of those 4 workers is kept for statements whose plan on the source reads one table and joins nothing") || !strings.Contains(out, "the other statements run at most 3 at once") {
		t.Errorf("four workers:\n%s", out)
	}
	st.Workers = 2
	if out = pgRouterStartupLines(st, "2GB", "the default", nil, 1, true); !strings.Contains(out, "one of those 2 workers is kept") || !strings.Contains(out, "the other statements run at most 1 at once") {
		t.Errorf("two workers:\n%s", out)
	}
	st.Workers = 1
	if out = pgRouterStartupLines(st, "2GB", "the default", nil, 1, true); !strings.Contains(out, "with 1 worker on the copy none is kept for short statements") || strings.Contains(out, "is kept for statements") {
		t.Errorf("one worker:\n%s", out)
	}
}
