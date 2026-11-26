package sqlsandbox

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"time"
)

// Out of memory in the copy's join order, run again in the order written
// (Session.RetryOutOfMemoryInWrittenOrder). Without statistics of the
// copy's columns DuckDB can pick a join order whose hash table does not fit:
// the CH-benCHmark's Q9 over fresh-mode views ran out of a 2 GB worker in 21
// to 59 s in DuckDB's order and answered in 5.5 to 7.9 s in the order
// written. The join check (joinguard.go) catches a join whose output grows
// far past its inputs; Q9's bad join is on a key, so its output does not
// grow and the check passes it. So a statement that fails with DuckDB's out
// of memory error in the copy's own order is run once more in the order
// written, in the same slot, before the caller is told.
//
// Not retried: an error that is not the engine's out of memory, a statement
// that already ran in the order written (Session.KeepJoinOrder, or the join
// check chose it), one whose order is not known (the kernel stopped the
// worker while the join check was on), a caller that left, and a statement
// with less than writtenOrderRetryFloor of its time left. The retry's time
// comes out of the statement's: what is left of the worker's wall clock
// (Limits.Timeout) and of the caller's deadline.

// WrittenOrderRetryOK and WrittenOrderRetryFailed are what the retry did:
// Result.WrittenOrderRetry on an answer, QueryError.WrittenOrderRetry on an
// error.
const (
	WrittenOrderRetryOK     = "ok"
	WrittenOrderRetryFailed = "failed"
)

// writtenOrderRetryFloor is the least time a retry is started with; with
// less the first attempt's error is the answer. writtenOrderRetryMargin is
// kept off the caller's deadline, so the retry is stopped by the worker's
// own clock (an out-of-memory error marked as retried, which the PostgreSQL
// port sends to the source) and not by the caller's (an error it does not).
const (
	writtenOrderRetryFloor  = time.Second
	writtenOrderRetryMargin = 500 * time.Millisecond
)

// outOfMemoryPrefix starts DuckDB's out of memory errors, and the ones this
// package writes in their place (oomFenceMessage, spillCapMessage).
const outOfMemoryPrefix = "Out of Memory Error"

// writtenOrderRetryWanted reports whether err, the first attempt's error of
// a statement run under sess, is one to run again in the order written.
func writtenOrderRetryWanted(sess Session, err error) bool {
	if !sess.RetryOutOfMemoryInWrittenOrder || sess.KeepJoinOrder {
		return false
	}
	var qe *QueryError
	if !errors.As(err, &qe) || !strings.HasPrefix(qe.Message, outOfMemoryPrefix) || qe.WrittenOrderRetry != "" {
		return false
	}
	if qe.ranWrittenOrder {
		return false
	}
	// The kernel stopped the worker (oomFenceMessage): no answer said which
	// order the join check chose. Without the check it was the copy's own.
	return !qe.orderUnknown || sess.JoinNDV == nil
}

// writtenOrderRetryBudget is the wall clock a retry gets: what is left of
// limit after elapsed, and of deadline (zero: none) less its margin; zero
// when that is under the floor.
func writtenOrderRetryBudget(limit, elapsed time.Duration, now, deadline time.Time) time.Duration {
	left := limit - elapsed
	if !deadline.IsZero() {
		left = min(left, deadline.Sub(now)-writtenOrderRetryMargin)
	}
	if left < writtenOrderRetryFloor {
		return 0
	}
	return left
}

// runWithWrittenOrderRetry runs job, and runs it once more in the order
// written when its first attempt ran out of memory in the copy's own order
// (writtenOrderRetryWanted). It is called with the job's slot held, so the
// retry takes no other.
func (r *Runner) runWithWrittenOrderRetry(ctx context.Context, job Job, limits Limits) (Result, error) {
	start := time.Now()
	res, err := r.spawn(ctx, job, limits)
	if err == nil || !job.Session.RetryOutOfMemoryInWrittenOrder {
		return res, err
	}
	if r.beforeWrittenOrderRetry != nil {
		r.beforeWrittenOrderRetry()
	}
	if ctx.Err() != nil || !writtenOrderRetryWanted(job.Session, err) {
		return res, err
	}
	first := time.Since(start)
	deadline, _ := ctx.Deadline()
	budget := writtenOrderRetryBudget(limits.Timeout, first, time.Now(), deadline)
	if budget == 0 {
		slog.Info("sql on the copy: a statement ran out of memory in the copy's join order, with too little of its time left to run it again in the order written",
			"first_ms", first.Milliseconds(), "time_limit_ms", limits.Timeout.Milliseconds())
		return res, err
	}
	again := job
	// The order written, and nothing to check: the check plans it in the
	// copy's order first, which is the one that ran out.
	again.Session.KeepJoinOrder = true
	again.Session.JoinNDV = nil
	retryLimits := limits
	retryLimits.Timeout = budget
	secondStart := time.Now()
	res2, err2 := r.spawn(ctx, again, retryLimits)
	second := time.Since(secondStart)
	if err2 == nil {
		slog.Info("sql on the copy: a statement ran out of memory in the copy's join order; run again in the order written, it answered",
			"outcome", WrittenOrderRetryOK, "first_ms", first.Milliseconds(), "second_ms", second.Milliseconds())
		res2.WrittenOrderRetry = WrittenOrderRetryOK
		return res2, nil
	}
	slog.Info("sql on the copy: a statement ran out of memory in the copy's join order; run again in the order written, it failed too",
		"outcome", WrittenOrderRetryFailed, "first_ms", first.Milliseconds(), "second_ms", second.Milliseconds(), "second_error", firstLine(err2.Error()))
	if errors.Is(ctx.Err(), context.Canceled) {
		// The client left or cancelled during the retry: that, as before.
		return Result{}, err2
	}
	var qe *QueryError
	errors.As(err, &qe)
	marked := *qe
	marked.WrittenOrderRetry = WrittenOrderRetryFailed
	var qe2 *QueryError
	if errors.As(err2, &qe2) && strings.HasPrefix(qe2.Message, outOfMemoryPrefix) {
		marked.Message += " It ran out of memory in the order written too."
	} else {
		marked.Message += " Run again with its joins in the order written, it did not answer either."
	}
	return Result{}, &marked
}
