package pgshim

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

// A statement shape that needed the order written (writtenorder.go).

// fakeClock is a WrittenOrders clock a test moves by hand.
type fakeClock struct{ at time.Time }

func (c *fakeClock) now() time.Time { return c.at }

func TestWrittenOrders_rememberExpireForget(t *testing.T) {
	clock := &fakeClock{at: time.Date(2026, 11, 26, 10, 0, 0, 0, time.UTC)}
	w := &WrittenOrders{now: clock.now}
	if w.has("a") {
		t.Fatal("remembered before it was noted")
	}
	if !w.note("a") {
		t.Fatal("a new shape was not said to be new")
	}
	if w.note("a") {
		t.Error("a shape noted twice was said to be new twice: it would be logged per statement")
	}
	if !w.has("a") || w.has("b") {
		t.Errorf("has a %v, has b %v", w.has("a"), w.has("b"))
	}
	// Used often, it still expires from when it was learned: a shape run
	// every minute must still be tried in the copy's own order again.
	for range 9 {
		clock.at = clock.at.Add(DefaultWrittenOrderRemember / 10)
		if !w.has("a") || w.note("a") {
			t.Fatal("forgotten, or learned again, within its day")
		}
	}
	clock.at = clock.at.Add(DefaultWrittenOrderRemember / 10)
	if w.has("a") {
		t.Error("still remembered a full day after it was learned: use renewed it")
	}
	// Expired, it is learned again from that moment.
	if !w.note("a") || !w.has("a") {
		t.Error("an expired shape could not be learned again")
	}
	clock.at = clock.at.Add(DefaultWrittenOrderRemember - time.Second)
	if !w.has("a") {
		t.Error("forgotten before its day")
	}
	if !w.forget("a") || w.has("a") {
		t.Error("forget did not forget")
	}
	if w.forget("a") {
		t.Error("forgetting a shape that is not remembered said it was")
	}
}

func TestWrittenOrders_nilShortAndNegative(t *testing.T) {
	var nilW *WrittenOrders
	if nilW.note("a") || nilW.has("a") || nilW.forget("a") {
		t.Error("a nil memory remembered something")
	}
	off := &WrittenOrders{Remember: -1}
	if off.note("a") || off.has("a") {
		t.Error("a negative Remember remembered something")
	}
	clock := &fakeClock{at: time.Now()}
	short := &WrittenOrders{Remember: time.Minute, now: clock.now}
	short.note("a")
	clock.at = clock.at.Add(time.Minute)
	if short.has("a") {
		t.Error("Remember not used: still remembered past it")
	}
}

func TestWrittenOrders_bounded(t *testing.T) {
	w := &WrittenOrders{}
	for i := range writtenOrdersMax {
		w.note(fmt.Sprint(i))
	}
	if !w.has("0") || len(w.at) != writtenOrdersMax {
		t.Fatalf("%d shapes", len(w.at))
	}
	w.note("one more")
	if len(w.at) > writtenOrdersMax || !w.has("one more") {
		t.Errorf("%d shapes past the cap, the new one remembered %v", len(w.at), w.has("one more"))
	}
}

// The key is the routing's: the shape with every value as ?, the schema and
// the search_path; a statement read only in part has none.
func TestWrittenOrderKey(t *testing.T) {
	s := routeSession(true)
	if s.writtenOrderKey("SELECT 1") != "" {
		t.Error("a key without a memory on the binding")
	}
	s.bound.WrittenOrders = &WrittenOrders{}
	base := s.writtenOrderKey("SELECT n FROM t WHERE k = 1 AND name = 'a'")
	for _, same := range []string{
		"SELECT n FROM t WHERE k = 2 AND name = 'other'",
		"select   n from t where k = 3 and name = 'x'",
		"SELECT n FROM t WHERE k = $1 AND name = $2",
		"SELECT n\nFROM t\nWHERE k = 1\nAND name = 'a'",
	} {
		if got := s.writtenOrderKey(same); got != base {
			t.Errorf("%q: another key than its shape's", same)
		}
	}
	for _, other := range []string{
		"SELECT n FROM u WHERE k = 1 AND name = 'a'",
		"SELECT n FROM t WHERE k = 1",
		"SELECT n FROM t WHERE k > 1 AND name = 'a'",
	} {
		if s.writtenOrderKey(other) == base {
			t.Errorf("%q: the same key as another shape", other)
		}
	}
	if got := s.writtenOrderKey("SELECT 'unterminated"); got != "" {
		t.Errorf("a statement read in part has key %q", got)
	}
	s.set.searchPath = []string{"other"}
	if s.writtenOrderKey("SELECT n FROM t WHERE k = 1 AND name = 'a'") == base {
		t.Error("another search_path, where t can be another table, has the same key")
	}
}

// What is remembered after a run: only an answer of the retry in the order
// written. A remembered shape that runs out of memory in the order written
// is forgotten and marked as one that ran out in both orders; any other
// error of it leaves it remembered.
func TestSettleWrittenOrder(t *testing.T) {
	oom := "Out of Memory Error: could not allocate block"
	ok := sqlsandbox.Result{WrittenOrderRetry: sqlsandbox.WrittenOrderRetryOK}
	for _, tc := range []struct {
		what   string
		res    sqlsandbox.Result
		err    error
		learnt bool
	}{
		{"answered after the retry", ok, nil, true},
		{"answered without one", sqlsandbox.Result{}, nil, false},
		{"out of memory in both orders", sqlsandbox.Result{}, &sqlsandbox.QueryError{Message: oom, WrittenOrderRetry: sqlsandbox.WrittenOrderRetryFailed}, false},
		{"the second run's own error", sqlsandbox.Result{}, &sqlsandbox.WrittenOrderRetryError{Outcome: sqlsandbox.WrittenOrderRetryOtherError, Err: errors.New("x")}, false},
		{"not run again: forced", sqlsandbox.Result{}, &sqlsandbox.QueryError{Message: oom, WrittenOrderRetry: sqlsandbox.WrittenOrderRetrySkippedForcedOrder}, false},
		{"not run again: the check", sqlsandbox.Result{}, &sqlsandbox.QueryError{Message: oom, WrittenOrderRetry: sqlsandbox.WrittenOrderRetrySkippedCheckWritten}, false},
		{"not run again: no time", sqlsandbox.Result{}, &sqlsandbox.QueryError{Message: oom, WrittenOrderRetry: sqlsandbox.WrittenOrderRetrySkippedNoTime}, false},
		{"cancelled", sqlsandbox.Result{}, context.Canceled, false},
		{"timed out", sqlsandbox.Result{}, &sqlsandbox.TimeoutError{}, false},
		{"another error", sqlsandbox.Result{}, &sqlsandbox.QueryError{Message: "Binder Error: x"}, false},
		// An error that carries an answer's mark is still an error.
		{"an error with an answer's mark", ok, &sqlsandbox.QueryError{Message: oom}, false},
	} {
		s := routeSession(true)
		s.bound.WrittenOrders = &WrittenOrders{}
		key := s.writtenOrderKey("SELECT n FROM t")
		s.oomRetry = sqlsandbox.WrittenOrderRetryOf(tc.res, tc.err)
		if got := s.settleWrittenOrder(context.Background(), context.Background(), "SELECT n FROM t", key, false, tc.res, tc.err); got != tc.err {
			t.Errorf("%s: the error changed to %v", tc.what, got)
		}
		if s.bound.WrittenOrders.has(key) != tc.learnt {
			t.Errorf("%s: remembered %v, want %v", tc.what, !tc.learnt, tc.learnt)
		}
		if s.writtenOrder != "" {
			t.Errorf("%s: a shape not remembered noted %q", tc.what, s.writtenOrder)
		}
	}

	remembered := func() (*session, string) {
		s := routeSession(true)
		s.bound.WrittenOrders = &WrittenOrders{}
		key := s.writtenOrderKey("SELECT n FROM t")
		s.bound.WrittenOrders.note(key)
		return s, key
	}
	// Answered in the order written from the start.
	s, key := remembered()
	if err := s.settleWrittenOrder(context.Background(), context.Background(), "SELECT n FROM t", key, true, sqlsandbox.Result{}, nil); err != nil || s.writtenOrder != WrittenOrderRemembered || !s.bound.WrittenOrders.has(key) {
		t.Errorf("answered: %v, %q, remembered %v", err, s.writtenOrder, s.bound.WrittenOrders.has(key))
	}
	// Out of memory in the order written: forgotten, and the error says it
	// ran out in both orders, so the source answers it under that reason.
	s, key = remembered()
	s.oomRetry = sqlsandbox.WrittenOrderRetrySkippedForcedOrder
	first := &sqlsandbox.QueryError{Message: oom, WrittenOrderRetry: sqlsandbox.WrittenOrderRetrySkippedForcedOrder}
	err := s.settleWrittenOrder(context.Background(), context.Background(), "SELECT n FROM t", key, true, sqlsandbox.Result{}, first)
	var qe *sqlsandbox.QueryError
	if !errors.As(err, &qe) || qe.WrittenOrderRetry != sqlsandbox.WrittenOrderRetryFailed || !strings.HasPrefix(qe.Message, oom+". It ran") || !strings.Contains(qe.Message, "order written") {
		t.Errorf("out of memory in the order written: %#v", err)
	}
	if first.WrittenOrderRetry != sqlsandbox.WrittenOrderRetrySkippedForcedOrder {
		t.Error("the copy's own error was changed in place")
	}
	if s.bound.WrittenOrders.has(key) || s.writtenOrder != WrittenOrderForgotten || s.oomRetry != "" {
		t.Errorf("forgotten: remembered %v, note %q, retry %q", s.bound.WrittenOrders.has(key), s.writtenOrder, s.oomRetry)
	}
	if got := s.copyFallback(err, freeError(err, s.logger)); got != ReasonCopyRefusedAfterWrittenOrder {
		t.Errorf("sent to the source as %q", got)
	}
	// Out of the copy's time in the order written: forgotten too, and sent
	// to the source as the retry's time-out was before this memory, in a
	// message whose sentences are apart.
	s, key = remembered()
	err = s.settleWrittenOrder(context.Background(), context.Background(), "SELECT n FROM t", key, true, sqlsandbox.Result{}, &sqlsandbox.TimeoutError{Limit: time.Minute})
	if !errors.As(err, &qe) || qe.WrittenOrderRetry != sqlsandbox.WrittenOrderRetryFailed || !strings.Contains(qe.Message, "ran out of time") || !strings.Contains(qe.Message, "stopped. It ran") {
		t.Errorf("out of time in the order written: %#v", err)
	}
	if s.bound.WrittenOrders.has(key) || s.writtenOrder != WrittenOrderForgotten {
		t.Errorf("out of time: remembered %v, note %q", s.bound.WrittenOrders.has(key), s.writtenOrder)
	}
	if got := s.copyFallback(err, freeError(err, s.logger)); got != ReasonCopyRefusedAfterWrittenOrder {
		t.Errorf("out of time: sent to the source as %q", got)
	}
	// Out of time with nowhere else to send it: forgotten, and the time-out
	// is the answer, 57014.
	s, key = remembered()
	s.bound.Forward = nil
	timeout := &sqlsandbox.TimeoutError{Limit: time.Minute}
	if got := s.settleWrittenOrder(context.Background(), context.Background(), "SELECT n FROM t", key, true, sqlsandbox.Result{}, timeout); got != timeout || s.bound.WrittenOrders.has(key) || s.writtenOrder != WrittenOrderForgotten {
		t.Errorf("out of time without a source: %v, remembered %v, note %q", got, s.bound.WrittenOrders.has(key), s.writtenOrder)
	} else if code := s.statementError(context.Background(), got).code; code != "57014" {
		t.Errorf("out of time without a source: code %s", code)
	}
	// Its half of the client's time over (writtenOrderContext), the
	// statement's own context alive: forgotten, and to the source.
	s, key = remembered()
	run, stop := context.WithCancel(context.Background())
	stop()
	err = s.settleWrittenOrder(context.Background(), run, "SELECT n FROM t", key, true, sqlsandbox.Result{}, context.Canceled)
	if !errors.As(err, &qe) || qe.WrittenOrderRetry != sqlsandbox.WrittenOrderRetryFailed || !strings.Contains(qe.Message, "statement_timeout") {
		t.Errorf("its half of the client's time over: %#v", err)
	}
	if s.bound.WrittenOrders.has(key) || s.copyFallback(err, freeError(err, s.logger)) != ReasonCopyRefusedAfterWrittenOrder {
		t.Errorf("its half of the client's time over: remembered %v", s.bound.WrittenOrders.has(key))
	}
	// The client's statement_timeout itself: forgotten, answered as that.
	s, key = remembered()
	stmt, cut := context.WithCancelCause(context.Background())
	cut(errStatementTimeout)
	if got := s.settleWrittenOrder(stmt, stmt, "SELECT n FROM t", key, true, sqlsandbox.Result{}, context.Canceled); got != context.Canceled || s.bound.WrittenOrders.has(key) || s.writtenOrder != WrittenOrderForgotten {
		t.Errorf("statement_timeout: %v, remembered %v, note %q", got, s.bound.WrittenOrders.has(key), s.writtenOrder)
	} else if code := s.statementError(stmt, got).code; code != "57014" {
		t.Errorf("statement_timeout: code %s", code)
	}
	// A cancel request: kept, as any error that is not the shape's.
	s, key = remembered()
	stmt, cut = context.WithCancelCause(context.Background())
	cut(errCanceledByUser)
	if got := s.settleWrittenOrder(stmt, stmt, "SELECT n FROM t", key, true, sqlsandbox.Result{}, context.Canceled); got != context.Canceled || !s.bound.WrittenOrders.has(key) {
		t.Errorf("a cancel request: %v, remembered %v", got, s.bound.WrittenOrders.has(key))
	}
	// Any other error, a cancel among them: kept, and that error.
	for _, other := range []error{&sqlsandbox.QueryError{Message: "Binder Error: x"}, context.Canceled, &sqlsandbox.QueryError{Message: "Out of Memory Error: x"},
		&sqlsandbox.JoinTooBigError{Reason: "x"}} {
		s, key = remembered()
		if got := s.settleWrittenOrder(context.Background(), context.Background(), "SELECT n FROM t", key, true, sqlsandbox.Result{}, other); got != other || !s.bound.WrittenOrders.has(key) || s.writtenOrder != WrittenOrderRemembered {
			t.Errorf("%v: %v, remembered %v, note %q", other, got, s.bound.WrittenOrders.has(key), s.writtenOrder)
		}
	}
	// No key, no memory: nothing.
	s = routeSession(true)
	if got := s.settleWrittenOrder(context.Background(), context.Background(), "SELECT n FROM t", "", false, ok, nil); got != nil || s.writtenOrder != "" {
		t.Errorf("without a key: %v, %q", got, s.writtenOrder)
	}
}

// A remembered shape runs in the order written, and keeps the join check:
// the copy then checks the order written alone, and one value of a shape
// can make it far larger than the one it was learned with.
func TestStartInWrittenOrder(t *testing.T) {
	opts := RunOptions{Schema: "public", JoinNDV: map[string]float64{"a": 1}}
	startInWrittenOrder(&opts)
	if !opts.KeepJoinOrder || opts.JoinNDV == nil || opts.Schema != "public" {
		t.Errorf("%+v", opts)
	}
}

// A remembered shape gets half of what is left of the client's limit where
// the source can have the rest; with no source, or no limit, all of it.
func TestWrittenOrderContext(t *testing.T) {
	at := time.Now().Add(10 * time.Second)
	ctx := sqlsandbox.WithClientDeadline(context.Background(), at)
	run, cancel := routeSession(true).writtenOrderContext(ctx)
	defer cancel()
	dl, ok := run.Deadline()
	if left := time.Until(dl); !ok || left > 5*time.Second || left < 4*time.Second {
		t.Errorf("with a source: deadline in %v (%v), want about 5 s", left, ok)
	}
	for what, c := range map[string]context.Context{"without a source": ctx, "without a limit": context.Background()} {
		s := routeSession(true)
		if what == "without a source" {
			s = routeSession(false)
		}
		run, cancel := s.writtenOrderContext(c)
		defer cancel()
		if _, ok := run.Deadline(); ok || run != c {
			t.Errorf("%s: a context of its own", what)
		}
	}
}

// A remembered shape's join check that passed is not counted as planned;
// the check's other outcomes are counted as they are.
func TestRememberedJoinCheck(t *testing.T) {
	for _, tc := range []struct {
		remembered bool
		in, want   string
	}{
		{true, JoinCheckPlanned, ""},
		{true, JoinCheckCouldNotRun, JoinCheckCouldNotRun},
		{true, JoinCheckNoStatistics, JoinCheckNoStatistics},
		{true, "", ""},
		{false, JoinCheckPlanned, JoinCheckPlanned},
		{false, JoinCheckWrittenOrder, JoinCheckWrittenOrder},
	} {
		if got := rememberedJoinCheck(tc.remembered, tc.in); got != tc.want {
			t.Errorf("remembered %v, %q: %q, want %q", tc.remembered, tc.in, got, tc.want)
		}
	}
}

// rememberLogin is a port whose copy behaves as the sandbox does with a
// statement whose shape runs out of memory in the copy's join order: in that
// order it answers after its own retry (WrittenOrderRetryOK), and run in the
// order written (RunOptions.KeepJoinOrder) it answers at once, except LIMIT
// 9, which runs out of memory in both orders, LIMIT 8, which fails with
// another error when run in the order written, and LIMIT 7, which runs out
// of time in the order written. Its source cannot be reached
// (a statement that gets 08001 was sent there).
type rememberLogin struct {
	orders *WrittenOrders
	mu     sync.Mutex
	notes  []RouteNote
	runs   []RunOptions
	// deadline is the deadline of the last run of LIMIT 66.
	deadline time.Time
}

func (l *rememberLogin) run(ctx context.Context, st string, opts RunOptions) (sqlsandbox.Result, error) {
	l.mu.Lock()
	l.runs = append(l.runs, opts)
	l.mu.Unlock()
	oom := "Out of Memory Error: x"
	res := sqlsandbox.Result{Columns: []sqlsandbox.Column{{Name: "n", Type: "INTEGER"}}, Rows: [][]any{{int32(7)}}}
	switch {
	case strings.Contains(st, "dbtrail_describe"):
		// A Describe's run under LIMIT 0: its columns, nothing remembered.
		return sqlsandbox.Result{Columns: res.Columns}, nil
	case strings.Contains(st, "LIMIT 9") && opts.KeepJoinOrder:
		return sqlsandbox.Result{}, &sqlsandbox.QueryError{Message: oom, WrittenOrderRetry: sqlsandbox.WrittenOrderRetrySkippedForcedOrder}
	case strings.Contains(st, "LIMIT 9"):
		return sqlsandbox.Result{}, &sqlsandbox.QueryError{Message: oom + " It ran out of memory in the order written too.", WrittenOrderRetry: sqlsandbox.WrittenOrderRetryFailed}
	case strings.Contains(st, "LIMIT 77") && opts.KeepJoinOrder:
		return sqlsandbox.Result{}, &sqlsandbox.JoinTooBigError{Reason: "a join in the order written"}
	case strings.Contains(st, "LIMIT 66") && opts.KeepJoinOrder:
		// Slow in the order written: it runs until its context ends.
		dl, _ := ctx.Deadline()
		l.mu.Lock()
		l.deadline = dl
		l.mu.Unlock()
		<-ctx.Done()
		return sqlsandbox.Result{}, ctx.Err()
	case strings.Contains(st, "LIMIT 7") && opts.KeepJoinOrder:
		return sqlsandbox.Result{}, &sqlsandbox.TimeoutError{Limit: time.Minute}
	case strings.Contains(st, "LIMIT 8") && opts.KeepJoinOrder:
		return sqlsandbox.Result{}, &sqlsandbox.QueryError{Message: "Binder Error: x"}
	}
	if !opts.KeepJoinOrder {
		res.WrittenOrderRetry = sqlsandbox.WrittenOrderRetryOK
	}
	return res, nil
}

func (l *rememberLogin) login(_ context.Context, _, _ string, password *Password, _ func()) (*Binding, *Error) {
	if !password.Matches([]string{portPassword}) {
		return nil, &Error{Code: "28P01", Message: "no"}
	}
	b := &Binding{Run: l.run, Actor: "server:orders", Schema: "public", WrittenOrders: l.orders, Routed: func(n RouteNote) {
		l.mu.Lock()
		defer l.mu.Unlock()
		l.notes = append(l.notes, n)
	}}
	b.Forward = &Forward{DSN: "postgres://fwd:pw@127.0.0.1:1/db?sslmode=disable&connect_timeout=2", Cost: &CostPolicy{}}
	return b, nil
}

// On both protocols: a shape answered after the retry starts in the order
// written from its next statement, with other values too, in one run and
// without the join check; one that then runs out of memory in the order
// written is forgotten and goes to the source as one that ran out in both
// orders, with no third run; another error keeps it; past its time it runs
// in the copy's own order again.
func TestRoute_rememberedWrittenOrder(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for _, protocol := range []string{"simple", "extended"} {
		clock := &fakeClock{at: time.Now()}
		l := &rememberLogin{orders: &WrittenOrders{now: clock.now}}
		addr := serveAddrCfg(t, Config{Login: l.login})
		c, err := pgconn.Connect(ctx, fmt.Sprintf("host=%s port=%s user=orders password=%s sslmode=disable", hostOf(addr), portOf(addr), portPassword))
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close(context.Background())
		type outcome struct {
			code, side, reason, retry, written string
			keepOrder                          bool
			runs                               int
		}
		var before int
		do := func(stmt string) outcome {
			if protocol == "simple" {
				_, err = c.Exec(ctx, stmt).ReadAll()
			} else {
				err = c.ExecParams(ctx, stmt, nil, nil, nil, nil).Read().Err
			}
			code := ""
			var pe *pgconn.PgError
			if errors.As(err, &pe) {
				code = pe.Code
			} else if err != nil {
				code = err.Error()
			}
			l.mu.Lock()
			defer l.mu.Unlock()
			n := l.notes[len(l.notes)-1]
			runs := l.runs[before:]
			before = len(l.runs)
			o := outcome{code: code, side: n.Side, reason: n.Reason, retry: n.OOMRetry, written: n.WrittenOrder, runs: len(runs)}
			if len(runs) > 0 {
				// The Execute's run, after a Describe's on the extended
				// protocol.
				last := runs[len(runs)-1]
				o.keepOrder = last.KeepJoinOrder
			}
			if protocol == "extended" {
				o.runs = 0
			}
			return o
		}
		runs := func(n int) int {
			if protocol == "extended" {
				return 0
			}
			return n
		}
		check := func(what string, got, want outcome) {
			t.Helper()
			if got != want {
				t.Errorf("%s, %s:\n got %+v\nwant %+v", protocol, what, got, want)
			}
		}
		check("first: out of memory, answered after the retry", do("SELECT n FROM t LIMIT 1"),
			outcome{side: RouteCopy, reason: ReasonAnswersTheSame, retry: sqlsandbox.WrittenOrderRetryOK, runs: runs(1)})
		check("same shape, other value: the order written from the start", do("SELECT n FROM t LIMIT 2"),
			outcome{side: RouteCopy, reason: ReasonAnswersTheSame, written: WrittenOrderRemembered, keepOrder: true, runs: runs(1)})
		check("another shape is not remembered", do("SELECT n FROM u LIMIT 2"),
			outcome{side: RouteCopy, reason: ReasonAnswersTheSame, retry: sqlsandbox.WrittenOrderRetryOK, runs: runs(1)})
		check("another error in the order written: that error", do("SELECT n FROM t LIMIT 8"),
			outcome{code: "08001", side: RouteSource, reason: ReasonCopyRefused, written: WrittenOrderRemembered, keepOrder: true, runs: runs(1)})
		check("still remembered after it", do("SELECT n FROM t LIMIT 3"),
			outcome{side: RouteCopy, reason: ReasonAnswersTheSame, written: WrittenOrderRemembered, keepOrder: true, runs: runs(1)})
		check("out of memory in the order written: forgotten, to the source, no third run", do("SELECT n FROM t LIMIT 9"),
			outcome{code: "08001", side: RouteSource, reason: ReasonCopyRefusedAfterWrittenOrder, written: WrittenOrderForgotten, keepOrder: true, runs: runs(1)})
		check("forgotten: the copy's own order again, and learned again", do("SELECT n FROM t LIMIT 4"),
			outcome{side: RouteCopy, reason: ReasonAnswersTheSame, retry: sqlsandbox.WrittenOrderRetryOK, runs: runs(1)})
		check("learned again", do("SELECT n FROM t LIMIT 5"),
			outcome{side: RouteCopy, reason: ReasonAnswersTheSame, written: WrittenOrderRemembered, keepOrder: true, runs: runs(1)})
		clock.at = clock.at.Add(DefaultWrittenOrderRemember)
		check("past its time: the copy's own order again", do("SELECT n FROM t LIMIT 6"),
			outcome{side: RouteCopy, reason: ReasonAnswersTheSame, retry: sqlsandbox.WrittenOrderRetryOK, runs: runs(1)})
		check("out of time in the order written: forgotten, to the source", do("SELECT n FROM t LIMIT 7"),
			outcome{code: "08001", side: RouteSource, reason: ReasonCopyRefusedAfterWrittenOrder, written: WrittenOrderForgotten, keepOrder: true, runs: runs(1)})
		check("after a time-out: the copy's own order again", do("SELECT n FROM t LIMIT 1"),
			outcome{side: RouteCopy, reason: ReasonAnswersTheSame, retry: sqlsandbox.WrittenOrderRetryOK, runs: runs(1)})
		check("a join too big in the order written: to the source, nothing forgotten", do("SELECT n FROM t LIMIT 77"),
			outcome{code: "08001", side: RouteSource, reason: ReasonJoinTooBig, written: WrittenOrderRemembered, keepOrder: true, runs: runs(1)})
		// Under statement_timeout: half of what is left for the order
		// written, then the source.
		if _, err := c.Exec(ctx, "SET statement_timeout = '4s'").ReadAll(); err != nil {
			t.Fatal(err)
		}
		start := time.Now()
		check("slow in the order written under statement_timeout: its half, then forgotten and to the source", do("SELECT n FROM t LIMIT 66"),
			outcome{code: "08001", side: RouteSource, reason: ReasonCopyRefusedAfterWrittenOrder, written: WrittenOrderForgotten, keepOrder: true, runs: runs(1)})
		l.mu.Lock()
		left := l.deadline.Sub(start)
		l.mu.Unlock()
		if left < time.Second || left > 2100*time.Millisecond {
			t.Errorf("%s: the order written was given %v of a 4 s statement_timeout, want about half", protocol, left)
		}
		if _, err := c.Exec(ctx, "SET statement_timeout = 0").ReadAll(); err != nil {
			t.Fatal(err)
		}
		check("forgotten after its half: the copy's own order again", do("SELECT n FROM t LIMIT 2"),
			outcome{side: RouteCopy, reason: ReasonAnswersTheSame, retry: sqlsandbox.WrittenOrderRetryOK, runs: runs(1)})
	}
}
