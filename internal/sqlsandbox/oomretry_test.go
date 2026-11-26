package sqlsandbox

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// What the retry in the order written is asked for, case by case: only an
// out-of-memory error of the engine, only when the job asks for it, and only
// when the first attempt ran in the copy's own join order.
func TestWrittenOrderRetryWanted(t *testing.T) {
	oom := &QueryError{Message: "Out of Memory Error: could not allocate block of size 256.0 KiB (1.8 GiB/1.8 GiB used)"}
	on := Session{RetryOutOfMemoryInWrittenOrder: true}
	withCheck := on
	withCheck.JoinNDV = &JoinCounts{NDV: map[string]float64{"a": 1}}
	forced := on
	forced.KeepJoinOrder = true
	for _, tc := range []struct {
		name string
		sess Session
		err  error
		want bool
	}{
		{"an out-of-memory error, asked for", on, oom, true},
		{"the same with the join check on, which kept the copy's order", withCheck, oom, true},
		{"not asked for (the MySQL port, the SQL page)", Session{}, oom, false},
		{"the order written forced by the statement", forced, oom, false},
		{"the join check already ran it in the order written", withCheck, &QueryError{Message: oom.Message, ranWrittenOrder: true}, false},
		{"stopped by the kernel, no check: the copy's order", on, &QueryError{Message: oomFenceMessage, orderUnknown: true}, true},
		{"stopped by the kernel, the check on: the order is not known", withCheck, &QueryError{Message: oomFenceMessage, orderUnknown: true}, false},
		{"the disk past the memory full", on, &QueryError{Message: "Out of Memory Error: the disk this statement spills to past its memory is full."}, true},
		{"no error", on, nil, false},
		{"another engine error", on, &QueryError{Message: "Binder Error: column nope not found"}, false},
		{"the words inside another error", on, &QueryError{Message: "Invalid Input Error: Out of Memory Error"}, false},
		{"lower case", on, &QueryError{Message: "out of memory error: x"}, false},
		{"already retried", on, &QueryError{Message: oom.Message, WrittenOrderRetry: WrittenOrderRetryFailed}, false},
		{"a time out", on, &TimeoutError{Limit: time.Second}, false},
		{"a worker that failed", on, &WorkerError{Err: errors.New("Out of Memory Error")}, false},
		{"a result too large", on, ErrResultTooLarge, false},
		{"a refusal", on, &RefusedError{Reason: "Out of Memory Error"}, false},
		{"the join check's refusal", on, &JoinTooBigError{Reason: "x"}, false},
		{"a cancel", on, context.Canceled, false},
	} {
		if got := writtenOrderRetryWanted(tc.sess, tc.err); got != tc.want {
			t.Errorf("%s: %v, want %v", tc.name, got, tc.want)
		}
	}
}

// The retry's time comes out of the statement's own: what is left of the
// worker's wall clock and of the caller's deadline, less a margin, and none
// below the floor.
func TestWrittenOrderRetryBudget(t *testing.T) {
	now := time.Now()
	for _, tc := range []struct {
		name     string
		limit    time.Duration
		elapsed  time.Duration
		deadline time.Time
		want     time.Duration
	}{
		{"the worker's clock, no deadline", 60 * time.Second, 21 * time.Second, time.Time{}, 39 * time.Second},
		{"a nearer deadline rules, less its margin", 60 * time.Second, 10 * time.Second, now.Add(20 * time.Second), 20*time.Second - writtenOrderRetryMargin},
		{"a farther deadline: the worker's clock", 60 * time.Second, 50 * time.Second, now.Add(time.Hour), 10 * time.Second},
		{"used up", 60 * time.Second, 60 * time.Second, time.Time{}, 0},
		{"past it", 60 * time.Second, 61 * time.Second, time.Time{}, 0},
		{"under the floor", 60 * time.Second, 60*time.Second - writtenOrderRetryFloor + time.Millisecond, time.Time{}, 0},
		{"at the floor", 60 * time.Second, 60*time.Second - writtenOrderRetryFloor, time.Time{}, writtenOrderRetryFloor},
		{"a deadline gone", 60 * time.Second, time.Second, now.Add(-time.Second), 0},
		{"a deadline under the floor once its margin is taken", 60 * time.Second, time.Second, now.Add(writtenOrderRetryMargin + writtenOrderRetryFloor/2), 0},
	} {
		if got := writtenOrderRetryBudget(tc.limit, tc.elapsed, now, tc.deadline); got != tc.want {
			t.Errorf("%s: %v, want %v", tc.name, got, tc.want)
		}
	}
}

// oomWithoutWrittenOrder runs out of 16 MB in the copy's own join order and
// reads nothing in the order written: its work depends on the setting the
// order written is made with, so the test does not hang on how DuckDB plans
// a join.
const oomWithoutWrittenOrder = "SELECT count(*) AS n FROM (SELECT DISTINCT i FROM range(30000000) t(i) " +
	"WHERE current_setting('disabled_optimizers') NOT LIKE '%join_order%') s"

// oomBothOrders runs out of 16 MB in either order.
const oomBothOrders = "SELECT count(*) AS n FROM (SELECT DISTINCT i FROM range(30000000) t(i)) s"

// oomJob is f's job of q under the PostgreSQL port's session with the retry
// asked for, its views counted: one ViewsFor call per worker started for it.
func oomJob(f copyFixture, q string, asked *atomic.Int32) Job {
	job := f.job(q)
	views := job.ViewsSQL
	job.ViewsSQL, job.ViewsFor = "", func(Refs) (string, error) { asked.Add(1); return views, nil }
	job.Session = Session{PostgresRules: true, SkipCommonSubplan: true, RetryOutOfMemoryInWrittenOrder: true}
	return job
}

func oomLimits() Limits {
	l := testLimits()
	l.MemoryLimit = "16MB"
	return l
}

// Through the real worker process: a statement that runs out of memory in
// the copy's join order is answered in the order written, in the same slot.
func TestRunner_outOfMemoryRetriedInWrittenOrder(t *testing.T) {
	f := newCopyFixture(t)
	r := newTestRunner(t, oomLimits())
	var asked atomic.Int32
	res, err := r.Run(context.Background(), oomJob(f, oomWithoutWrittenOrder, &asked))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.WrittenOrderRetry != WrittenOrderRetryOK || asked.Load() != 2 {
		t.Fatalf("retry %q, %d workers asked for views; want ok, 2", res.WrittenOrderRetry, asked.Load())
	}
	if len(res.Rows) != 1 || res.JoinOrderWritten {
		t.Errorf("rows %v, join order written by the check %v", res.Rows, res.JoinOrderWritten)
	}
	// The slot was given back once: the next statement runs.
	if _, err := r.Run(context.Background(), f.job("SELECT 1")); err != nil {
		t.Fatalf("the next statement: %v", err)
	}
}

// Out of memory in both orders: the first attempt's error, marked as
// retried, so the PostgreSQL port can say why it goes to the source.
func TestRunner_outOfMemoryInBothOrders(t *testing.T) {
	f := newCopyFixture(t)
	r := newTestRunner(t, oomLimits())
	var asked atomic.Int32
	_, err := r.Run(context.Background(), oomJob(f, oomBothOrders, &asked))
	var qe *QueryError
	if !errors.As(err, &qe) {
		t.Fatalf("err = %T %v, want a QueryError", err, err)
	}
	if qe.WrittenOrderRetry != WrittenOrderRetryFailed || !strings.HasPrefix(qe.Message, "Out of Memory Error") || asked.Load() != 2 {
		t.Fatalf("retry %q, %d asked, message %q", qe.WrittenOrderRetry, asked.Load(), qe.Message)
	}
	if !strings.Contains(qe.Message, "order written") {
		t.Errorf("the message does not say it was run again: %q", qe.Message)
	}
}

// Not asked for: the MySQL port and the SQL page are as before, one attempt
// and DuckDB's own error.
func TestRunner_outOfMemoryNotRetriedUnlessAsked(t *testing.T) {
	f := newCopyFixture(t)
	r := newTestRunner(t, oomLimits())
	var asked atomic.Int32
	job := oomJob(f, oomWithoutWrittenOrder, &asked)
	job.Session = Session{MySQLClock: true, MySQLText: true}
	_, err := r.Run(context.Background(), job)
	var qe *QueryError
	if !errors.As(err, &qe) || qe.WrittenOrderRetry != "" || asked.Load() != 1 {
		t.Fatalf("err = %v, asked %d", err, asked.Load())
	}
	if strings.Contains(qe.Message, "order written") {
		t.Errorf("the MySQL port's message changed: %q", qe.Message)
	}
}

// The statement already ran in the order written (forced): the same plan
// twice is not tried.
func TestRunner_outOfMemoryInForcedOrderNotRetried(t *testing.T) {
	f := newCopyFixture(t)
	r := newTestRunner(t, oomLimits())
	var asked atomic.Int32
	job := oomJob(f, oomBothOrders, &asked)
	job.Session.KeepJoinOrder = true
	_, err := r.Run(context.Background(), job)
	var qe *QueryError
	if !errors.As(err, &qe) || qe.WrittenOrderRetry != "" || asked.Load() != 1 {
		t.Fatalf("err = %v, asked %d", err, asked.Load())
	}
}

// The join check ran the statement in the order written and it ran out of
// memory there: the worker's error says which order it ran in, so the parent
// does not run the same plan again.
func TestWorker_engineErrorSaysTheOrderTheCheckChose(t *testing.T) {
	f := newCopyFixture(t)
	was := joinDanger
	t.Cleanup(func() { joinDanger = was })
	job := wireJob{CopyDirs: []string{f.archiveRoot}, SQL: "SELECT count(*) AS n FROM (SELECT DISTINCT i FROM range(30000000) t(i)) a JOIN range(10) b ON a.i = b.range",
		Threads: 1, MemoryLimit: "16MB", MaxRows: 10, MaxResultBytes: 1 << 20, JoinNDV: map[string]float64{"range": 10}}
	for _, tc := range []struct {
		verdicts []string
		want     string
	}{
		{[]string{"HASH_JOIN on a = b", ""}, joinOrderWritten},
		{[]string{""}, ""},
	} {
		n := 0
		joinDanger = func([]byte, map[string]float64) (string, error) {
			n++
			if n > len(tc.verdicts) {
				return "", nil
			}
			return tc.verdicts[n-1], nil
		}
		res := runJob(job, nil, os.Stderr)
		if res.Error == nil || res.Error.Kind != errQuery || !strings.Contains(res.Error.Message, "Out of Memory") {
			t.Fatalf("%v: %+v", tc.verdicts, res.Error)
		}
		if res.JoinOrder != tc.want {
			t.Errorf("%v: join order %q, want %q", tc.verdicts, res.JoinOrder, tc.want)
		}
	}
}

// A client that left, or a statement cancelled, while the first attempt
// ended: no second one, and the first's error as before.
func TestRunner_outOfMemoryNotRetriedAfterACancel(t *testing.T) {
	f := newCopyFixture(t)
	r := newTestRunner(t, oomLimits())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r.beforeWrittenOrderRetry = cancel
	var asked atomic.Int32
	_, err := r.Run(ctx, oomJob(f, oomWithoutWrittenOrder, &asked))
	var qe *QueryError
	if !errors.As(err, &qe) || qe.WrittenOrderRetry != "" || asked.Load() != 1 {
		t.Fatalf("err = %v, asked %d", err, asked.Load())
	}
}

// The retry's time is what the statement has left: none left, no retry, and
// the first attempt's error unmarked (the port sends it to the source as
// copy_refused, as before).
func TestRunner_outOfMemoryNotRetriedWithoutTimeLeft(t *testing.T) {
	f := newCopyFixture(t)
	t.Run("the worker's clock", func(t *testing.T) {
		l := oomLimits()
		l.Timeout = 3 * time.Second
		r := newTestRunner(t, l)
		r.beforeWrittenOrderRetry = func() { time.Sleep(l.Timeout - writtenOrderRetryFloor/2) }
		var asked atomic.Int32
		_, err := r.Run(context.Background(), oomJob(f, oomWithoutWrittenOrder, &asked))
		var qe *QueryError
		if !errors.As(err, &qe) || qe.WrittenOrderRetry != "" || asked.Load() != 1 {
			t.Fatalf("err = %v, asked %d", err, asked.Load())
		}
	})
	t.Run("the caller's deadline", func(t *testing.T) {
		r := newTestRunner(t, oomLimits())
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		deadline, _ := ctx.Deadline()
		r.beforeWrittenOrderRetry = func() { time.Sleep(time.Until(deadline) - writtenOrderRetryMargin - writtenOrderRetryFloor/2) }
		var asked atomic.Int32
		_, err := r.Run(ctx, oomJob(f, oomWithoutWrittenOrder, &asked))
		var qe *QueryError
		if !errors.As(err, &qe) || qe.WrittenOrderRetry != "" || asked.Load() != 1 {
			t.Fatalf("err = %v, asked %d", err, asked.Load())
		}
	})
}

// The retry gets what is left of the worker's clock, not a fresh one: a
// retry that needs more than that is stopped at it, and the statement's
// error is still the first attempt's out-of-memory, marked, never a time out
// (which the PostgreSQL port would not send to the source).
func TestRunner_writtenOrderRetryStoppedAtTheStatementsClock(t *testing.T) {
	f := newCopyFixture(t)
	l := oomLimits()
	l.Timeout = 5 * time.Second
	r := newTestRunner(t, l)
	// The first attempt plus this leaves about 2 s for the second, which
	// takes far longer in the order written.
	r.beforeWrittenOrderRetry = func() { time.Sleep(2500 * time.Millisecond) }
	var asked atomic.Int32
	q := "SELECT sum(i % 7) AS n FROM range(20000000000) t(i) WHERE current_setting('disabled_optimizers') LIKE '%join_order%' UNION ALL " + oomWithoutWrittenOrder
	started := time.Now()
	_, err := r.Run(context.Background(), oomJob(f, q, &asked))
	var qe *QueryError
	if !errors.As(err, &qe) || qe.WrittenOrderRetry != WrittenOrderRetryFailed || !strings.HasPrefix(qe.Message, "Out of Memory Error") {
		t.Fatalf("err = %T %v", err, err)
	}
	if took := time.Since(started); took > l.Timeout+2*time.Second {
		t.Errorf("took %v, past the statement's clock of %v", took, l.Timeout)
	}
}

// hideHostPathsInError keeps what the parent knows of the join order: the
// retry is decided after it.
func TestHideHostPathsInError_keepsTheJoinOrder(t *testing.T) {
	err := hideHostPathsInError(&QueryError{Message: "Out of Memory Error: /srv/spill/x full", ranWrittenOrder: true}, []string{"/srv/spill"})
	var qe *QueryError
	if !errors.As(err, &qe) || !qe.ranWrittenOrder || strings.Contains(qe.Message, "/srv/spill") {
		t.Fatalf("%+v", qe)
	}
}
