package pgshim

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

// A statement shape learned from slowness (writtenorder_slow.go).

// lockedClock is a WrittenOrders clock a test moves by hand, read from the
// connection's goroutine too.
type lockedClock struct {
	mu sync.Mutex
	at time.Time
}

func (c *lockedClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.at
}

func (c *lockedClock) add(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.at = c.at.Add(d)
}

func TestWrittenOrders_slowMarkTrialVerdicts(t *testing.T) {
	clock := &lockedClock{at: time.Date(2026, 11, 27, 10, 0, 0, 0, time.UTC)}
	w := &WrittenOrders{now: clock.now}
	if w.markSlow("a", DefaultWrittenOrderSlowAfter-time.Millisecond) {
		t.Error("marked under the threshold")
	}
	if _, ok := w.trial("a"); ok {
		t.Error("on trial before it was marked")
	}
	if !w.markSlow("a", 31*time.Second) {
		t.Fatal("a slow shape was not marked")
	}
	if w.markSlow("a", 40*time.Second) {
		t.Error("marked twice: it would be logged per statement, and its slow time replaced")
	}
	if took, ok := w.trial("a"); !ok || took != 31*time.Second {
		t.Errorf("trial %v %v, want 31s", took, ok)
	}
	if w.has("a") {
		t.Error("a shape on trial is said to be remembered")
	}
	// Faster: remembered, as an out-of-memory one is, and no longer on trial.
	if !w.learnFromTrial("a") || !w.has("a") {
		t.Fatal("a faster trial was not remembered")
	}
	if _, ok := w.trial("a"); ok {
		t.Error("still on trial after it was learned")
	}
	if w.learnFromTrial("a") || w.keepOwnOrder("a") {
		t.Error("a second verdict was applied: two connections' trials must settle once")
	}
	if w.markSlow("a", time.Minute) {
		t.Error("a remembered shape was marked slow")
	}
	// The learned shape keeps the remembered shape's rules: forgotten after
	// its day from when it was learned.
	clock.add(DefaultWrittenOrderRemember)
	if w.has("a") {
		t.Error("a shape learned from slowness was kept past its day")
	}

	// Not faster: the copy's own order is kept, no trial for a day.
	if !w.markSlow("b", 31*time.Second) || !w.keepOwnOrder("b") {
		t.Fatal("own order not kept")
	}
	if _, ok := w.trial("b"); ok || w.has("b") {
		t.Error("a shape whose own order was kept is on trial or remembered")
	}
	if w.markSlow("b", time.Minute) || w.learnFromTrial("b") {
		t.Error("a shape whose own order was kept was marked or learned again within its day")
	}
	clock.add(DefaultWrittenOrderRemember - time.Second)
	if w.markSlow("b", time.Minute) {
		t.Error("own order forgotten before its day")
	}
	clock.add(time.Second)
	if !w.markSlow("b", time.Minute) {
		t.Error("own order kept past its day")
	}

	// A trial that never gets its next statement expires too.
	w.markSlow("c", time.Minute)
	clock.add(DefaultWrittenOrderRemember)
	if _, ok := w.trial("c"); ok {
		t.Error("a trial kept past its day")
	}
	if w.learnFromTrial("c") || w.keepOwnOrder("c") {
		t.Error("an expired trial took a verdict")
	}

	// Out of memory wins: a shape learned by the retry is not on trial.
	w.markSlow("d", time.Minute)
	w.note("d")
	if _, ok := w.trial("d"); ok || !w.has("d") {
		t.Error("a shape learned by the retry in the order written is still on trial")
	}
	w.markSlow("e", time.Minute)
	w.keepOwnOrder("e")
	w.note("e")
	if !w.has("e") {
		t.Error("the retry's lesson lost to a kept own order")
	}
}

func TestWrittenOrders_slowOffAndNil(t *testing.T) {
	var nilW *WrittenOrders
	if nilW.markSlow("a", time.Hour) || nilW.learnFromTrial("a") || nilW.keepOwnOrder("a") {
		t.Error("a nil memory marked something")
	}
	if _, ok := nilW.trial("a"); ok {
		t.Error("a nil memory has a trial")
	}
	if (&WrittenOrders{SlowAfter: -1}).markSlow("a", time.Hour) {
		t.Error("a negative SlowAfter marked a shape")
	}
	if (&WrittenOrders{Remember: -1}).markSlow("a", time.Hour) {
		t.Error("a memory that remembers nothing marked a shape")
	}
	short := &WrittenOrders{SlowAfter: time.Second}
	if !short.markSlow("a", time.Second) {
		t.Error("SlowAfter not used")
	}
}

// Bounded as the remembered shapes are, apart from them: past the bound the
// marks are forgotten, never a shape that out of memory taught.
func TestWrittenOrders_slowBounded(t *testing.T) {
	w := &WrittenOrders{}
	w.note("oom")
	for i := range writtenOrdersMax {
		w.markSlow(fmt.Sprint(i), time.Minute)
	}
	if len(w.slow) != writtenOrdersMax {
		t.Fatalf("%d marks", len(w.slow))
	}
	w.markSlow("one more", time.Minute)
	if len(w.slow) > writtenOrdersMax {
		t.Errorf("%d marks past the cap", len(w.slow))
	}
	if _, ok := w.trial("one more"); !ok {
		t.Error("the new mark was not kept")
	}
	if !w.has("oom") {
		t.Error("the marks' bound forgot a remembered shape")
	}
}

// Each server has its own marks.
func TestWrittenOrders_slowPerServer(t *testing.T) {
	a, b := &WrittenOrders{}, &WrittenOrders{}
	a.markSlow("k", time.Minute)
	if _, ok := b.trial("k"); ok {
		t.Error("one server's mark is another's")
	}
	b.markSlow("k", time.Minute)
	b.keepOwnOrder("k")
	if _, ok := a.trial("k"); !ok {
		t.Error("another server's verdict reached this one")
	}
}

// slowCopy is a copy whose statements take the time their text says: in
// the copy's own order "own=<s>" seconds, in the order written "wr=<s>"
// seconds, or fail there as the text says.
type slowCopy struct {
	mu   sync.Mutex
	runs []RunOptions
	// deadlines are each run's time left when it started (0: none).
	deadlines []time.Duration
	// cut, when set, is called by a run in the order written that says
	// wr=cut: the client's statement_timeout ending it.
	cut func()
}

func secondsIn(st, key string) time.Duration {
	_, rest, ok := strings.Cut(st, key+"=")
	if !ok {
		return 0
	}
	var n int
	fmt.Sscanf(rest, "%d", &n)
	return time.Duration(n) * time.Second
}

func (c *slowCopy) run(ctx context.Context, st string, opts RunOptions) (sqlsandbox.Result, error) {
	c.mu.Lock()
	c.runs = append(c.runs, opts)
	var left time.Duration
	if dl, ok := ctx.Deadline(); ok {
		left = time.Until(dl)
	}
	c.deadlines = append(c.deadlines, left)
	c.mu.Unlock()
	res := sqlsandbox.Result{Columns: []sqlsandbox.Column{{Name: "n", Type: "INTEGER"}}, Rows: [][]any{{int32(7)}}}
	if !opts.KeepJoinOrder {
		res.Elapsed = secondsIn(st, "own")
		switch {
		case strings.Contains(st, "own=err"):
			return sqlsandbox.Result{Elapsed: 40 * time.Second}, &sqlsandbox.QueryError{Message: "Binder Error: x"}
		case strings.Contains(st, "own=oomok"):
			res.WrittenOrderRetry = sqlsandbox.WrittenOrderRetryOK
		case strings.Contains(st, "own=checkwritten"):
			res.JoinOrderWritten = true
		}
		return res, nil
	}
	switch {
	case strings.Contains(st, "wr=oom"):
		return sqlsandbox.Result{}, &sqlsandbox.QueryError{Message: "Out of Memory Error: x", WrittenOrderRetry: sqlsandbox.WrittenOrderRetrySkippedForcedOrder}
	case strings.Contains(st, "wr=timeout"):
		return sqlsandbox.Result{}, &sqlsandbox.TimeoutError{Limit: time.Minute}
	case strings.Contains(st, "wr=toobig"):
		return sqlsandbox.Result{}, &sqlsandbox.JoinTooBigError{Reason: "a join in the order written"}
	case strings.Contains(st, "wr=hang"):
		<-ctx.Done()
		return sqlsandbox.Result{}, ctx.Err()
	case strings.Contains(st, "wr=cut"):
		c.cut()
		<-ctx.Done()
		return sqlsandbox.Result{}, ctx.Err()
	case strings.Contains(st, "wr=err"):
		return sqlsandbox.Result{}, &sqlsandbox.QueryError{Message: "Binder Error: y"}
	}
	res.Elapsed = secondsIn(st, "wr")
	return res, nil
}

func (c *slowCopy) took() (n int, last RunOptions, lastLeft time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	n = len(c.runs)
	if n > 0 {
		last, lastLeft = c.runs[n-1], c.deadlines[n-1]
	}
	c.runs, c.deadlines = nil, nil
	return n, last, lastLeft
}

func slowSession(forward bool, clock *lockedClock) (*session, *slowCopy) {
	s := routeSession(forward)
	c := &slowCopy{}
	s.bound.Run = c.run
	s.bound.WrittenOrders = &WrittenOrders{now: clock.now}
	return s, c
}

// The statement's text, with its shape: the values differ, the shape does
// not.
func slowStmt(table, how string, n int) string {
	return fmt.Sprintf("SELECT n FROM %s WHERE note = '%s' AND k = %d", table, how, n)
}

func TestRunRemembering_slowness(t *testing.T) {
	clock := &lockedClock{at: time.Now()}
	type outcome struct {
		written   string
		runs      int
		keepOrder bool
		err       bool
	}
	s, c := slowSession(true, clock)
	do := func(stmt string, ctx context.Context) outcome {
		t.Helper()
		if ctx == nil {
			ctx = context.Background()
		}
		s.joinCheck, s.oomRetry, s.writtenOrder = "", "", ""
		_, release, err := s.runRemembering(ctx, stmt, stmt, s.runOptions(), "")
		release()
		n, last, _ := c.took()
		return outcome{written: s.writtenOrder, runs: n, keepOrder: last.KeepJoinOrder, err: err != nil}
	}
	check := func(what string, got, want outcome) {
		t.Helper()
		if got != want {
			t.Errorf("%s:\n got %+v\nwant %+v", what, got, want)
		}
	}

	// Q7's case: slow in the copy's own order, fast in the order written.
	check("slow in its own order: marked", do(slowStmt("t", "own=31 wr=6", 1), nil), outcome{written: WrittenOrderSlowMarked, runs: 1})
	check("its trial, clearly faster: learned", do(slowStmt("t", "own=31 wr=6", 2), nil), outcome{written: WrittenOrderLearnedSlow, runs: 1, keepOrder: true})
	check("then remembered", do(slowStmt("t", "own=31 wr=6", 3), nil), outcome{written: WrittenOrderRemembered, runs: 1, keepOrder: true})

	// Not clearly faster: own order kept for a day.
	check("slow: marked", do(slowStmt("u", "own=31 wr=20", 1), nil), outcome{written: WrittenOrderSlowMarked, runs: 1})
	check("a trial a little faster: own order kept", do(slowStmt("u", "own=31 wr=20", 2), nil), outcome{written: WrittenOrderOwnOrderKept, runs: 1, keepOrder: true})
	check("then its own order, not marked again", do(slowStmt("u", "own=31 wr=20", 3), nil), outcome{runs: 1})
	check("slower in the order written: marked", do(slowStmt("u2", "own=20 wr=40", 1), nil), outcome{written: WrittenOrderSlowMarked, runs: 1})
	check("a slower trial: own order kept", do(slowStmt("u2", "own=20 wr=40", 2), nil), outcome{written: WrittenOrderOwnOrderKept, runs: 1, keepOrder: true})

	// Failing in the order written: run again in its own order, at once,
	// and own order kept; the copy answers.
	for _, how := range []string{"wr=oom", "wr=timeout", "wr=toobig"} {
		table := "v_" + strings.TrimPrefix(how, "wr=")
		check(how+": marked", do(slowStmt(table, "own=31 "+how, 1), nil), outcome{written: WrittenOrderSlowMarked, runs: 1})
		check(how+": its trial fails, run again in its own order", do(slowStmt(table, "own=31 "+how, 2), nil), outcome{written: WrittenOrderOwnOrderKept, runs: 2})
		check(how+": then its own order", do(slowStmt(table, "own=31 "+how, 3), nil), outcome{runs: 1})
	}

	// Another error in the trial: that error, the trial stays.
	check("marked", do(slowStmt("w", "own=31 wr=err", 1), nil), outcome{written: WrittenOrderSlowMarked, runs: 1})
	check("another error in the trial: that error, one run", do(slowStmt("w", "own=31 wr=err", 2), nil), outcome{written: WrittenOrderTrial, runs: 1, keepOrder: true, err: true})
	if _, ok := s.bound.WrittenOrders.trial(s.writtenOrderKey(slowStmt("w", "", 0))); !ok {
		t.Error("another error ended the trial")
	}

	// What is never marked.
	check("fast", do(slowStmt("x", "own=2", 1), nil), outcome{runs: 1})
	check("fast, again in its own order", do(slowStmt("x", "own=2", 2), nil), outcome{runs: 1})
	check("slow but an error", do(slowStmt("y", "own=err", 1), nil), outcome{err: true, runs: 1})
	check("not marked after an error", do(slowStmt("y", "own=31", 2), nil), outcome{written: WrittenOrderSlowMarked, runs: 1})
	check("slow after the retry in the order written: that retry's lesson", do(slowStmt("z", "own=31 own=oomok", 1), nil), outcome{runs: 1})
	check("remembered by the retry, not by slowness", do(slowStmt("z", "own=31", 2), nil), outcome{written: WrittenOrderRemembered, runs: 1, keepOrder: true})
	check("slow, run in the order written by the join check", do(slowStmt("jc", "own=31 own=checkwritten", 1), nil), outcome{runs: 1})
	check("not marked by the check's run", do(slowStmt("jc", "own=31 own=checkwritten", 2), nil), outcome{runs: 1})
	// A statement read in part has no shape: never marked.
	part := "SELECT n FROM t WHERE note = 'own=31"
	check("read in part, slow", do(part, nil), outcome{runs: 1})
	check("read in part, again", do(part, nil), outcome{runs: 1})

	// Past its day the copy's own order is tried again, and marked again.
	clock.add(DefaultWrittenOrderRemember)
	check("past its day, the learned one: its own order again, marked", do(slowStmt("t", "own=31 wr=6", 4), nil), outcome{written: WrittenOrderSlowMarked, runs: 1})
	check("past its day, the kept one: marked again", do(slowStmt("u", "own=31 wr=20", 4), nil), outcome{written: WrittenOrderSlowMarked, runs: 1})

	// A learned shape that runs out of memory in the order written is
	// forgotten and goes to the source, as one the retry taught.
	check("trial of t", do(slowStmt("t", "own=31 wr=6", 5), nil), outcome{written: WrittenOrderLearnedSlow, runs: 1, keepOrder: true})
	_, release, err := s.runRemembering(context.Background(), slowStmt("t", "wr=oom", 6), slowStmt("t", "wr=oom", 6), s.runOptions(), "")
	release()
	c.took()
	if s.writtenOrder != WrittenOrderForgotten || s.copyFallback(err, freeError(err, s.logger)) != ReasonCopyRefusedAfterWrittenOrder {
		t.Errorf("a learned shape out of memory in the order written: %q, %v", s.writtenOrder, err)
	}
}

// The trial and its run again in the copy's own order, under the client's
// statement_timeout.
func TestRunRemembering_trialUnderStatementTimeout(t *testing.T) {
	clock := &lockedClock{at: time.Now()}
	mark := func(s *session, c *slowCopy, table string) string {
		stmt := slowStmt(table, "own=31", 1)
		_, release, _ := s.runRemembering(context.Background(), stmt, stmt, s.runOptions(), "")
		release()
		c.took()
		if s.writtenOrder != WrittenOrderSlowMarked {
			t.Fatalf("not marked: %q", s.writtenOrder)
		}
		return s.writtenOrderKey(stmt)
	}

	// Its half of what is left: cut there, run again in its own order with
	// half of what is left then, and answered.
	s, c := slowSession(true, clock)
	mark(s, c, "h")
	ctx := sqlsandbox.WithClientDeadline(context.Background(), time.Now().Add(4*time.Second))
	stmt := slowStmt("h", "wr=hang", 2)
	start := time.Now()
	_, release, err := s.runRemembering(ctx, stmt, stmt, s.runOptions(), "")
	release()
	n, last, left := c.took()
	if err != nil || n != 2 || last.KeepJoinOrder || s.writtenOrder != WrittenOrderOwnOrderKept {
		t.Errorf("cut by its half: %v, %d runs, last in the order written %v, %q", err, n, last.KeepJoinOrder, s.writtenOrder)
	}
	if took := time.Since(start); took < 1500*time.Millisecond || took > 3*time.Second {
		t.Errorf("the trial took %v of a 4 s statement_timeout, want about half", took)
	}
	if left <= 0 || left > 1200*time.Millisecond {
		t.Errorf("the run again in its own order had %v, want at most half of the about 2 s left", left)
	}

	// The client's statement_timeout itself (no source: the trial had all of
	// it): answered as that, no run again, own order kept.
	s, c = slowSession(false, clock)
	key := mark(s, c, "st")
	stmtCtx, cut := context.WithCancelCause(context.Background())
	defer cut(nil)
	c.cut = func() { cut(errStatementTimeout) }
	stmt = slowStmt("st", "wr=cut", 2)
	_, release, err = s.runRemembering(stmtCtx, stmt, stmt, s.runOptions(), "")
	release()
	n, _, _ = c.took()
	if n != 1 || err == nil || s.writtenOrder != WrittenOrderOwnOrderKept {
		t.Errorf("statement_timeout in the trial: %v, %d runs, %q", err, n, s.writtenOrder)
	} else if code := s.statementError(stmtCtx, err).code; code != "57014" {
		t.Errorf("statement_timeout in the trial: code %s", code)
	}
	if _, ok := s.bound.WrittenOrders.trial(key); ok || s.bound.WrittenOrders.markSlow(key, time.Minute) {
		t.Error("statement_timeout in the trial: own order not kept")
	}

	// A cancel request: nothing learned, the trial stays.
	s, c = slowSession(true, clock)
	key = mark(s, c, "cr")
	stmtCtx, cancelReq := context.WithCancelCause(context.Background())
	c.cut = func() { cancelReq(errCanceledByUser) }
	stmt = slowStmt("cr", "wr=cut", 2)
	_, release, err = s.runRemembering(stmtCtx, stmt, stmt, s.runOptions(), "")
	release()
	n, _, _ = c.took()
	if n != 1 || err == nil || s.writtenOrder != WrittenOrderTrial {
		t.Errorf("a cancel request in the trial: %v, %d runs, %q", err, n, s.writtenOrder)
	}
	if _, ok := s.bound.WrittenOrders.trial(key); !ok {
		t.Error("a cancel request ended the trial")
	}
}

// Through the port, on both protocols: marked, learned, then remembered;
// the client sees one answer each time, and the note says what happened.
func TestRoute_writtenOrderLearnedFromSlowness(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for _, protocol := range []string{"simple", "extended"} {
		clock := &lockedClock{at: time.Now()}
		cp := &slowCopy{}
		var mu sync.Mutex
		var notes []RouteNote
		orders := &WrittenOrders{now: clock.now}
		login := func(_ context.Context, _, _ string, password *Password, _ func()) (*Binding, *Error) {
			if !password.Matches([]string{portPassword}) {
				return nil, &Error{Code: "28P01", Message: "no"}
			}
			b := &Binding{Run: cp.run, Actor: "server:orders", Schema: "public", WrittenOrders: orders, Routed: func(n RouteNote) {
				mu.Lock()
				defer mu.Unlock()
				notes = append(notes, n)
			}}
			b.Forward = &Forward{DSN: "postgres://fwd:pw@127.0.0.1:1/db?sslmode=disable&connect_timeout=2", Cost: &CostPolicy{}}
			return b, nil
		}
		addr := serveAddrCfg(t, Config{Login: login})
		c, err := pgconn.Connect(ctx, fmt.Sprintf("host=%s port=%s user=orders password=%s sslmode=disable", hostOf(addr), portOf(addr), portPassword))
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close(context.Background())
		do := func(stmt string) (RouteNote, RunOptions) {
			t.Helper()
			if protocol == "simple" {
				_, err = c.Exec(ctx, stmt).ReadAll()
			} else {
				err = c.ExecParams(ctx, stmt, nil, nil, nil, nil).Read().Err
			}
			if err != nil {
				t.Fatalf("%s: %v", protocol, err)
			}
			_, last, _ := cp.took()
			mu.Lock()
			defer mu.Unlock()
			return notes[len(notes)-1], last
		}
		for i, want := range []struct {
			written   string
			keepOrder bool
		}{
			{WrittenOrderSlowMarked, false},
			{WrittenOrderLearnedSlow, true},
			{WrittenOrderRemembered, true},
		} {
			n, last := do(slowStmt("t", "own=31 wr=6", i))
			if n.Side != RouteCopy || n.WrittenOrder != want.written || last.KeepJoinOrder != want.keepOrder {
				t.Errorf("%s, statement %d: %+v, in the order written %v; want %q, %v", protocol, i, n, last.KeepJoinOrder, want.written, want.keepOrder)
			}
		}
		// A trial that runs out of memory in the order written: the copy
		// answers in its own order, not the source.
		do(slowStmt("v", "own=31 wr=oom", 1))
		if n, last := do(slowStmt("v", "own=31 wr=oom", 2)); n.Side != RouteCopy || n.Failed || n.WrittenOrder != WrittenOrderOwnOrderKept || last.KeepJoinOrder {
			t.Errorf("%s, a trial out of memory: %+v", protocol, n)
		}
	}
}

// The trial's verdict is settled once, whichever connection gets there
// first.
func TestSettleTrial_once(t *testing.T) {
	clock := &lockedClock{at: time.Now()}
	s, _ := slowSession(true, clock)
	key := s.writtenOrderKey(slowStmt("t", "", 0))
	s.bound.WrittenOrders.markSlow(key, 31*time.Second)
	if again := s.settleTrial(context.Background(), context.Background(), "SELECT 1", key, 31*time.Second, sqlsandbox.Result{Elapsed: 30 * time.Second}, nil); again || s.writtenOrder != WrittenOrderOwnOrderKept {
		t.Fatalf("first verdict: %v %q", again, s.writtenOrder)
	}
	// A second connection's trial, faster, settles after it: nothing changes.
	s.settleTrial(context.Background(), context.Background(), "SELECT 1", key, 31*time.Second, sqlsandbox.Result{Elapsed: time.Second}, nil)
	if s.bound.WrittenOrders.has(key) {
		t.Error("a second verdict replaced the first")
	}
	// An answer that says nothing of its time is not faster.
	key = s.writtenOrderKey(slowStmt("u", "", 0))
	s.bound.WrittenOrders.markSlow(key, 31*time.Second)
	s.settleTrial(context.Background(), context.Background(), "SELECT 1", key, 31*time.Second, sqlsandbox.Result{}, nil)
	if s.bound.WrittenOrders.has(key) {
		t.Error("an answer without its time was taken as faster")
	}
}
