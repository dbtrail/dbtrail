package pgshim

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

// A statement shape learned from slowness. Out of memory is not the only
// sign of a bad join order: the CH-benCHmark's Q7 over fresh-mode views on a
// live copy (DuckDB 1.5.6, 200 warehouses, 4 threads) flips between two plans
// in the engine's own join order, one that answers in about 7 s and one that
// takes a steady 31 to 33 s. In the order written the same statement over
// the same snapshot answers in 5.7 to 6.6 s. The join check (joinguard.go)
// does not flag it, since no join of it passes 3e9 rows, and its estimates
// flag it on one copy and not on another, so which plan a server gets
// depends on the day.
//
// So a statement that answered in the copy's own order and took
// SlowAfter or more (DefaultWrittenOrderSlowAfter) marks its shape for a
// trial. Only a run in the copy's own order counts: not one in the order
// written (a remembered shape, the join check's choice), not one the retry
// in the order written answered (that is the out-of-memory memory's), and
// not an error. The time is the engine's own (sqlsandbox.Result.Elapsed):
// not the wait for a worker or a lane, nor the building of the views, so a
// statement queued behind a report is not taken for a slow plan. A run that
// says nothing of its time (0) marks nothing. The shape and its rules are
// the remembered shapes' (writtenOrderKey): one read in part is never
// marked.
//
// The next statement of a marked shape runs as a remembered one does: in
// the order written, with the join check of that order alone, and within
// half of what is left of the client's statement_timeout where the source
// can have the rest (writtenOrderContext). Then:
//
//   - It answered in at most 1/writtenOrderTrialFaster of the slow run's
//     time: the shape is remembered, as one the retry taught, with the same
//     day from that moment (DefaultWrittenOrderRemember) and the same
//     forgetting: one that then runs out of memory or time in the order
//     written is forgotten and goes to the source
//     (copy_refused_after_written_order).
//   - It answered, not that much faster: the copy's own order is kept for
//     the shape, and it is not tried again for a day.
//   - It ran out of memory, of the copy's time, of its half of the client's
//     time, or the join check refused the order written (join_too_big): the
//     copy's own order is kept for a day, and the statement runs once more,
//     at once, in that order, the one it answered in before. Not sent to the
//     source: the copy can answer it. That run gets half of what is left of
//     the client's statement_timeout where the source can have the rest
//     (writtenOrderContext again), no time limit of its own past the copy's
//     otherwise, and is the statement's answer, its error or the source's as
//     for any statement in the copy's own order: it is not run a third time
//     in the order written by this memory, though the retry in the order
//     written still follows its own out of memory, as for any statement.
//   - The client's statement_timeout ended it: answered as that, and the
//     copy's own order is kept for a day: the order written did not answer
//     within the client's time.
//   - A cancel request, a client that left, any other error: answered as
//     that, and the shape stays on trial for its next statement.
//
// The comparison is conservative on purpose. A shape is the statement with
// every value as ?, so INTERVAL '1 day' and INTERVAL '10 years' are one
// shape, and a trial measured with one value can be wrong for another; it
// must be clearly faster, not a little, and the day it is kept corrects a
// wrong one. Two connections can try one shape at once: the first verdict
// counts, the later ones change nothing. The answer is the same in either
// order: KeepJoinOrder turns off the engine's join order optimizer and
// nothing else, only the plan changes.
//
// The marks are kept, per server, beside the remembered shapes and bounded
// as they are (writtenOrdersMax), apart from them, so filling one never
// forgets the other. Each, a trial or a kept own order, lasts a day from
// when it was set: a trial whose next statement never comes is forgotten
// then too. In memory only: a restart forgets them.

// DefaultWrittenOrderSlowAfter is how long a statement must take in the
// copy's own order for its shape to be tried in the order written: far
// above the 5.7 to 7 s Q7 takes in a good plan, well under the 31 s of its
// bad one, and long enough that a trial, at most once a day per shape, costs
// little beside the run it may save.
const DefaultWrittenOrderSlowAfter = 15 * time.Second

// writtenOrderTrialFaster is how many times faster than the slow run a trial
// must answer for its shape to be remembered: Q7's 6 s against 31 s passes
// with room; a plan that is only somewhat faster with one value may be
// slower with another.
const writtenOrderTrialFaster = 2

// What slowness did with the statement (RouteNote.WrittenOrder).
const (
	// WrittenOrderSlowMarked: it answered slowly in the copy's own order;
	// the next statement of its shape is tried in the order written.
	WrittenOrderSlowMarked = "slow_marked"
	// WrittenOrderTrial: run in the order written on trial, and nothing was
	// decided (a cancel or an error that is not the order's).
	WrittenOrderTrial = "trial"
	// WrittenOrderLearnedSlow: run in the order written on trial, clearly
	// faster; the shape is remembered.
	WrittenOrderLearnedSlow = "learned_slow"
	// WrittenOrderOwnOrderKept: run in the order written on trial, not
	// clearly faster, or failed there and run again in the copy's own order;
	// that order is kept for a day.
	WrittenOrderOwnOrderKept = "own_order_kept"
)

// slowShape is what slowness marked of a shape: on trial, the slow run
// taking took, or its own order kept (own), from at.
type slowShape struct {
	at   time.Time
	took time.Duration
	own  bool
}

func (w *WrittenOrders) slowAfter() time.Duration {
	switch {
	case w.SlowAfter < 0:
		return 0
	case w.SlowAfter == 0:
		return DefaultWrittenOrderSlowAfter
	}
	return w.SlowAfter
}

// slowLocked is key's mark now, with w.mu held; one past its day is
// forgotten.
func (w *WrittenOrders) slowLocked(key string, now time.Time) (slowShape, bool) {
	e, ok := w.slow[key]
	if !ok {
		return slowShape{}, false
	}
	if now.Sub(e.at) >= w.keepFor() {
		delete(w.slow, key)
		return slowShape{}, false
	}
	return e, true
}

// markSlow marks key for a trial, its run in the copy's own order having
// taken took, and reports whether it did: not under the threshold, nor a
// shape remembered or marked already.
func (w *WrittenOrders) markSlow(key string, took time.Duration) bool {
	if w == nil || w.keepFor() == 0 || w.slowAfter() == 0 || took < w.slowAfter() {
		return false
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	now := w.clock()
	if at, ok := w.at[key]; ok && now.Sub(at) < w.keepFor() {
		return false
	}
	if _, ok := w.slowLocked(key, now); ok {
		return false
	}
	if w.slow == nil || len(w.slow) >= writtenOrdersMax {
		w.slow = map[string]slowShape{}
	}
	w.slow[key] = slowShape{at: now, took: took}
	return true
}

// trial is the slow run's time of key when it is on trial now.
func (w *WrittenOrders) trial(key string) (time.Duration, bool) {
	if w == nil || w.keepFor() == 0 {
		return 0, false
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	e, ok := w.slowLocked(key, w.clock())
	if !ok || e.own {
		return 0, false
	}
	return e.took, true
}

// learnFromTrial remembers key, on trial, from now, and reports whether it
// did: a shape no longer on trial (another connection's verdict came first,
// or its day is over) is left as it is.
func (w *WrittenOrders) learnFromTrial(key string) bool {
	if w == nil || w.keepFor() == 0 {
		return false
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	now := w.clock()
	if e, ok := w.slowLocked(key, now); !ok || e.own {
		return false
	}
	delete(w.slow, key)
	w.rememberLocked(key, now)
	return true
}

// keepOwnOrder keeps the copy's own order for key, on trial, from now, and
// reports whether it did, as learnFromTrial does.
func (w *WrittenOrders) keepOwnOrder(key string) bool {
	if w == nil || w.keepFor() == 0 {
		return false
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	now := w.clock()
	if e, ok := w.slowLocked(key, now); !ok || e.own {
		return false
	}
	w.slow[key] = slowShape{at: now, own: true}
	return true
}

// runRemembering runs text, the copy's form of qstr, on the copy in opts,
// in the order its shape's memory says: the order written for a remembered
// shape or one on trial, the copy's own otherwise; and settles what the run
// teaches (settleWrittenOrder, settleTrial, markIfSlow). A trial that fails
// in the order written runs once more in the copy's own order. want is what
// joinNDVFor said of opts.JoinNDV. The caller calls release once it is done
// with the result.
func (s *session) runRemembering(ctx context.Context, qstr, text string, opts RunOptions, want string) (res sqlsandbox.Result, release func(), err error) {
	shape := s.writtenOrderKey(qstr)
	w := s.bound.WrittenOrders
	remembered := shape != "" && w.has(shape)
	var slow time.Duration
	trial := false
	if shape != "" && !remembered {
		slow, trial = w.trial(shape)
	}
	own := opts
	run, release := ctx, func() {}
	if remembered || trial {
		startInWrittenOrder(&opts)
		run, release = s.writtenOrderContext(ctx)
	}
	res, err = s.bound.Run(run, text, opts)
	s.joinCheck = rememberedJoinCheck(remembered || trial, s.noteJoinCheck(res, err, opts.JoinNDV != nil, want))
	s.oomRetry = sqlsandbox.WrittenOrderRetryOf(res, err)
	if !trial {
		err = s.settleWrittenOrder(ctx, run, qstr, shape, remembered, res, err)
		if !remembered {
			s.markIfSlow(qstr, shape, opts, res, err)
		}
		return res, release, err
	}
	if !s.settleTrial(ctx, run, qstr, shape, slow, res, err) {
		return res, release, err
	}
	// Once more in the copy's own order, the one it answered in, with the
	// join check of both orders as any statement in that order has.
	release()
	run, release = s.writtenOrderContext(ctx)
	res, err = s.bound.Run(run, text, own)
	s.joinCheck = s.noteJoinCheck(res, err, own.JoinNDV != nil, want)
	s.oomRetry = sqlsandbox.WrittenOrderRetryOf(res, err)
	// Not remembered: what its own retry in the order written teaches is
	// learned as for any statement.
	err = s.settleWrittenOrder(ctx, run, qstr, shape, false, res, err)
	return res, release, err
}

// markIfSlow marks shape for a trial when the run of qstr in opts answered
// in the copy's own order and took the threshold or more.
func (s *session) markIfSlow(qstr, shape string, opts RunOptions, res sqlsandbox.Result, err error) {
	if shape == "" || err != nil || opts.KeepJoinOrder || res.WrittenOrderRetry != "" || res.JoinOrderWritten {
		return
	}
	w := s.bound.WrittenOrders
	if !w.markSlow(shape, res.Elapsed) {
		return
	}
	s.writtenOrder = WrittenOrderSlowMarked
	s.logger.Debug("pgshim: a statement answered slowly in the copy's join order; the next statement of its shape is tried with its joins in the order written",
		"shape", Fingerprint(qstr), "took", res.Elapsed.String(), "slow_after", w.slowAfter().String())
}

// settleTrial is told what the trial of key, a shape whose run in the
// copy's own order took slow, did in the order written, in run, a context
// of ctx's (writtenOrderContext), and reports whether the statement is to
// run once more in the copy's own order. The error to answer with is err
// when it is not.
func (s *session) settleTrial(ctx, run context.Context, qstr, key string, slow time.Duration, res sqlsandbox.Result, err error) (again bool) {
	w := s.bound.WrittenOrders
	// The sandbox did not run it again: it ran in the order written, and
	// that is this memory's doing.
	s.oomRetry = ""
	s.writtenOrder = WrittenOrderTrial
	keepOwn := func(why string) {
		s.writtenOrder = WrittenOrderOwnOrderKept
		if w.keepOwnOrder(key) {
			s.logger.Info("pgshim: a statement shape slow in the copy's join order was tried with its joins in the order written; the copy's own order is kept for it",
				"shape", Fingerprint(qstr), "why", why, "for", w.keepFor().String())
		}
	}
	var qe *sqlsandbox.QueryError
	var timedOut *sqlsandbox.TimeoutError
	var tooBig *sqlsandbox.JoinTooBigError
	switch {
	case err == nil:
		if res.Elapsed > 0 && res.Elapsed*writtenOrderTrialFaster <= slow {
			if w.learnFromTrial(key) {
				s.writtenOrder = WrittenOrderLearnedSlow
				s.logger.Info("pgshim: a statement shape slow in the copy's join order answered clearly faster with its joins in the order written; its next statements run in the order written from the start",
					"shape", Fingerprint(qstr), "own_order", slow.String(), "written_order", res.Elapsed.String(), "for", w.keepFor().String())
			}
			return false
		}
		keepOwn("not clearly faster: " + res.Elapsed.String() + " against " + slow.String())
		return false
	case errors.Is(context.Cause(ctx), errStatementTimeout):
		// The client's own statement_timeout: answered as that, nothing left
		// to run anything in.
		keepOwn("the client's statement_timeout ended it")
		return false
	case ctx.Err() != nil:
		// A cancel request, a client that left: nothing learned.
		return false
	case run.Err() != nil,
		errors.As(err, &qe) && qe.WrittenOrderRetry == sqlsandbox.WrittenOrderRetrySkippedForcedOrder,
		errors.As(err, &timedOut),
		errors.As(err, &tooBig):
		line, _, _ := strings.Cut(err.Error(), "\n")
		keepOwn(line)
		return true
	}
	// Its own error, answered as that: the trial stays for the next one.
	return false
}
