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
// written, in the same slot and with the views of its first run, before the
// caller is told.
//
// Not run again: an error that is not the engine's out of memory, a caller
// that left, and, each said in the log and in the error
// (QueryError.WrittenOrderRetry), a statement that already ran in the order
// written (Session.KeepJoinOrder, or the join check chose it) and one with
// too little of its time left. The retry's time comes out of the
// statement's: what is left of the worker's wall clock (Limits.Timeout), of
// the caller's deadline less a margin, and half of what is left of the
// client's own limit (WithClientDeadline), so the source still has the other
// half when the retry fails too.
//
// What the second run gives: an answer (Result.WrittenOrderRetry ok); out of
// memory or out of its time again, the first run's error marked failed
// (QueryError.WrittenOrderRetry), which the PostgreSQL port sends to the
// source under its own reason; any other error is returned as it is, inside
// a WrittenOrderRetryError, so the caller treats it as that error (a result
// too large is not sent to the source, a refusal is the refusal); a cancel
// is returned as it is.

// What the retry did: Result.WrittenOrderRetry on an answer,
// QueryError.WrittenOrderRetry on an out-of-memory error,
// WrittenOrderRetryError.Outcome on another (WrittenOrderRetryOf reads all
// three).
const (
	WrittenOrderRetryOK     = "ok"
	WrittenOrderRetryFailed = "failed"
	// WrittenOrderRetryOtherError: the second run failed with another error
	// than running out of memory or time.
	WrittenOrderRetryOtherError = "other_error"
	// Not run again: the statement asked for the order written
	// (Session.KeepJoinOrder), the join check had already chosen it, or
	// less than writtenOrderRetryFloor of its time was left.
	WrittenOrderRetrySkippedForcedOrder  = "skipped_forced_order"
	WrittenOrderRetrySkippedCheckWritten = "skipped_check_chose_written_order"
	WrittenOrderRetrySkippedNoTime       = "skipped_no_time"
)

// WrittenOrderRetryError is the second run's error when it was neither out
// of memory nor out of time: the caller reads it as that error (Unwrap).
type WrittenOrderRetryError struct {
	Outcome string
	Err     error
}

func (e *WrittenOrderRetryError) Error() string { return e.Err.Error() }
func (e *WrittenOrderRetryError) Unwrap() error { return e.Err }

// WrittenOrderRetryOf is what the retry in the order written did with a
// statement that answered res or failed with err, "" when it did nothing.
func WrittenOrderRetryOf(res Result, err error) string {
	if err == nil {
		return res.WrittenOrderRetry
	}
	var other *WrittenOrderRetryError
	if errors.As(err, &other) {
		return other.Outcome
	}
	var qe *QueryError
	if errors.As(err, &qe) {
		return qe.WrittenOrderRetry
	}
	return ""
}

// clientDeadlineKey carries WithClientDeadline's instant.
type clientDeadlineKey struct{}

// WithClientDeadline says when the client's own limit on the statement ends
// it (PostgreSQL's statement_timeout), where that limit is not ctx's
// deadline: the retry in the order written takes at most half of what is
// left of it, so the source still has the rest.
func WithClientDeadline(ctx context.Context, at time.Time) context.Context {
	return context.WithValue(ctx, clientDeadlineKey{}, at)
}

// ClientDeadlineOf is WithClientDeadline's instant on ctx, ok false when it
// has none.
func ClientDeadlineOf(ctx context.Context) (at time.Time, ok bool) {
	at, ok = ctx.Value(clientDeadlineKey{}).(time.Time)
	return at, ok
}

func clientDeadline(ctx context.Context) time.Time {
	at, _ := ClientDeadlineOf(ctx)
	return at
}

// writtenOrderRetryFloor is the least time a retry is started with; with
// less the first run's error is the answer. writtenOrderRetryMargin is kept
// off the caller's deadline, so a slow retry is stopped by the worker's own
// clock (TimeoutError, which is marked as failed and goes to the source) and
// not by the caller's context.
const (
	writtenOrderRetryFloor  = time.Second
	writtenOrderRetryMargin = 500 * time.Millisecond
)

// joinOrderWrittenLine is what a worker writes on stderr when the join check
// has it run the statement in the order written, before it runs: a worker
// the kernel stops leaves no answer that says so.
const joinOrderWrittenLine = "sql worker: the join check runs this statement with its joins in the order written"

// outOfMemoryPrefix starts DuckDB's out of memory errors, and the ones this
// package writes in their place (oomFenceMessage, spillCapMessage).
const outOfMemoryPrefix = "Out of Memory Error"

// isOutOfMemory reports whether err is the engine's out-of-memory error.
func isOutOfMemory(err error) (*QueryError, bool) {
	var qe *QueryError
	if !errors.As(err, &qe) || !strings.HasPrefix(qe.Message, outOfMemoryPrefix) {
		return nil, false
	}
	return qe, true
}

// writtenOrderRetryWanted reports whether err, the first run's error of a
// statement run under sess, is an out-of-memory error of the copy's own
// join order, and so one to run again in the order written. skip names why
// one that ran out of memory is not, "" otherwise.
func writtenOrderRetryWanted(sess Session, err error) (want bool, skip string) {
	if !sess.RetryOutOfMemoryInWrittenOrder {
		return false, ""
	}
	qe, oom := isOutOfMemory(err)
	if !oom || qe.WrittenOrderRetry != "" {
		return false, ""
	}
	switch {
	case sess.KeepJoinOrder:
		return false, WrittenOrderRetrySkippedForcedOrder
	case qe.ranWrittenOrder:
		return false, WrittenOrderRetrySkippedCheckWritten
	}
	return true, ""
}

// writtenOrderRetryBudget is the wall clock a retry gets: what is left of
// limit after elapsed, of deadline (zero: none) less its margin, and half of
// what is left of client (zero: none); zero when that is under the floor.
func writtenOrderRetryBudget(limit, elapsed time.Duration, now, deadline, client time.Time) time.Duration {
	left := limit - elapsed
	if !deadline.IsZero() {
		left = min(left, deadline.Sub(now)-writtenOrderRetryMargin)
	}
	if !client.IsZero() {
		left = min(left, client.Sub(now)/2)
	}
	if left < writtenOrderRetryFloor {
		return 0
	}
	return left
}

// runWithWrittenOrderRetry runs job, and runs it once more in the order
// written when its first run ran out of memory in the copy's own order
// (writtenOrderRetryWanted). It is called with the job's slot held, so the
// retry takes no other.
func (r *Runner) runWithWrittenOrderRetry(ctx context.Context, job Job, limits Limits) (Result, error) {
	if !job.Session.RetryOutOfMemoryInWrittenOrder {
		return r.spawn(ctx, job, limits)
	}
	// The views the first run was given, for the second: the same answer
	// of the caller's, its waits and refusals not made twice, and what it
	// noted of them true of the run that answered.
	var views string
	var viewsGiven bool
	first := job
	if job.ViewsFor != nil {
		ask := job.ViewsFor
		first.ViewsFor = func(refs Refs) (string, error) {
			script, err := ask(refs)
			views, viewsGiven = script, err == nil
			return script, err
		}
	}
	start := time.Now()
	res, err := r.spawn(ctx, first, limits)
	if err == nil {
		return res, nil
	}
	if r.beforeWrittenOrderRetry != nil {
		r.beforeWrittenOrderRetry()
	}
	if ctx.Err() != nil {
		return res, err
	}
	want, skip := writtenOrderRetryWanted(job.Session, err)
	took := time.Since(start)
	deadline, _ := ctx.Deadline()
	budget := writtenOrderRetryBudget(limits.Timeout, took, time.Now(), deadline, clientDeadline(ctx))
	if want && budget == 0 {
		skip = WrittenOrderRetrySkippedNoTime
	}
	if skip != "" {
		slog.Info("sql on the copy: a statement ran out of memory in the copy's join order and was not run again in the order written",
			"outcome", skip, "first_ms", took.Milliseconds(), "time_limit_ms", limits.Timeout.Milliseconds())
		qe, _ := isOutOfMemory(err)
		marked := *qe
		marked.WrittenOrderRetry = skip
		return res, &marked
	}
	if !want {
		return res, err
	}
	again := job
	// The order written, and nothing to check: the check plans it in the
	// copy's order first, which is the one that ran out.
	again.Session.KeepJoinOrder = true
	again.Session.JoinNDV = nil
	if viewsGiven {
		again.ViewsFor = func(Refs) (string, error) { return views, nil }
	}
	retryLimits := limits
	retryLimits.Timeout = budget
	secondStart := time.Now()
	res2, err2 := r.spawn(ctx, again, retryLimits)
	second := time.Since(secondStart)
	log := func(outcome string, extra ...any) {
		slog.Info("sql on the copy: a statement ran out of memory in the copy's join order and was run again in the order written",
			append([]any{"outcome", outcome, "first_ms", took.Milliseconds(), "second_ms", second.Milliseconds(), "first_error", firstLine(err.Error())}, extra...)...)
	}
	if err2 == nil {
		log(WrittenOrderRetryOK)
		res2.WrittenOrderRetry = WrittenOrderRetryOK
		return res2, nil
	}
	if errors.Is(ctx.Err(), context.Canceled) {
		// The client left or cancelled during the retry: that, as before.
		log("cancelled")
		return Result{}, err2
	}
	_, oomAgain := isOutOfMemory(err2)
	var timedOut *TimeoutError
	if !oomAgain && !errors.As(err2, &timedOut) {
		// Its own error, which the caller answers as that error.
		log(WrittenOrderRetryOtherError, "second_error", firstLine(err2.Error()))
		return Result{}, &WrittenOrderRetryError{Outcome: WrittenOrderRetryOtherError, Err: err2}
	}
	log(WrittenOrderRetryFailed, "second_error", firstLine(err2.Error()))
	qe, _ := isOutOfMemory(err)
	marked := *qe
	marked.WrittenOrderRetry = WrittenOrderRetryFailed
	if oomAgain {
		marked.Message += " It ran out of memory in the order written too."
	} else {
		marked.Message += " Run again with its joins in the order written, it ran out of time."
	}
	return Result{}, &marked
}
