package consoleapp

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dbtrail/dbtrail/internal/pgshim"
	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
	"github.com/dbtrail/dbtrail/internal/stmtclock"
)

// The copy's fast lane on the PostgreSQL port. The copy runs its statements
// in one line, first come first served (sqlsandbox's Runner, which the MySQL
// router shares and this does not change). With reports of many seconds
// holding every worker, a short statement waited behind them: a sum of one
// warehouse, a second alone, took 13 to 31 seconds under 30 connections.
//
// So the copy is given one worker more than --workers (pgCopySlots), and that
// one is kept for short statements: a statement whose plan on the source
// reads one table and joins nothing (pgshim fastlane.go,
// RunOptions.FastLane). Every other statement for the copy, a report, a
// statement with no plan, takes a place here first, and at most --workers of
// them (the runner's count less the one kept) hold one at once, as many as
// ran at once before. Short statements take their worker from the runner
// directly, any free one: the kept one, or one of --workers no report holds.
// The port's own probes (RunOptions.Probe) and the catalog's reads do not
// come here either.
//
// What the extra worker costs, said where the router starts: one statement
// more at once, with its own --threads and --sql-memory (they are given per
// statement, not shared among the workers), one more warm worker process
// kept between statements, and each statement's share of the spill disk
// (half the free disk, split between the statements that can run at once)
// a little smaller.
//
// The wait here is bounded as the runner's is (--max-wait, its line's
// length), and refused with the runner's own errors, so the port refuses it
// with 53000 and does not send it to the source (#3109). A long statement
// that got its place then takes a worker from the runner: at most --workers
// long statements hold one, so it waits there only behind short ones. The
// two waits are each bounded by --max-wait.

// pgCopyLanes is the place long statements take before the copy's runner.
// A nil one keeps nothing: every statement goes to the runner directly.
type pgCopyLanes struct {
	// workers is how many statements the copy runs at once now
	// (Runner.MaxInFlight, which a server turned to fresh mode changes,
	// #2546); maxWait and maxWaiters bound the wait here as the runner
	// bounds its own (Runner.MaxWait, Runner.MaxWaiters).
	workers    func() int
	maxWait    func() time.Duration
	maxWaiters func() int
	// recheck is how often a statement that waits looks again at workers,
	// which can change without a place being given back.
	recheck time.Duration

	mu      sync.Mutex
	long    int
	waiters []*pgLaneWaiter
	// refusedWait and refusedFull count the long statements refused here
	// since the router started: their wait ran out, or the line was full.
	refusedWait, refusedFull uint64
}

// pgLaneRecheck is pgCopyLanes.recheck in the router.
const pgLaneRecheck = 250 * time.Millisecond

// newPGCopyLanes keeps one of r's workers for short statements, r being
// sized one over --workers (pgCopySlots); nil for no runner.
func newPGCopyLanes(r *sqlsandbox.Runner) *pgCopyLanes {
	if r == nil {
		return nil
	}
	return &pgCopyLanes{workers: r.MaxInFlight, maxWait: r.MaxWait, maxWaiters: r.MaxWaiters, recheck: pgLaneRecheck}
}

// pgLaneWaiter is one long statement in line. granted is closed, with got
// set under the lock, when it is handed a place.
type pgLaneWaiter struct {
	granted chan struct{}
	got     bool
}

// limitLocked is how many long statements may hold a place at once: the
// runner's workers less the one kept, which is --workers. Below 1 (a runner
// of one worker, which the router never makes) there is no lane, and none
// waits.
func (l *pgCopyLanes) limitLocked() int { return l.workers() - 1 }

// enter takes a place for a statement the copy is about to run, waiting for
// one when it is a long statement and every place is taken. The release
// gives it back and may be called more than once. The error is the
// runner's own kind (a *sqlsandbox.BusyError past the wait,
// sqlsandbox.ErrBusy for a full line), or ctx's when the client left.
func (l *pgCopyLanes) enter(ctx context.Context, opts pgshim.RunOptions) (release func(), err error) {
	if l == nil || opts.FastLane || opts.Probe {
		return func() {}, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	l.mu.Lock()
	limit := l.limitLocked()
	if limit < 1 {
		l.mu.Unlock()
		return func() {}, nil
	}
	if len(l.waiters) == 0 && l.long < limit {
		l.long++
		l.mu.Unlock()
		return l.releaser(), nil
	}
	wait := l.maxWait()
	if wait < 0 || len(l.waiters) >= l.maxWaiters() {
		l.refusedFull++
		waiting := len(l.waiters)
		l.mu.Unlock()
		slog.Debug("PostgreSQL router: a long statement was refused, the line among the long statements full", "long_limit", limit, "waiting", waiting)
		return nil, sqlsandbox.ErrBusy
	}
	w := &pgLaneWaiter{granted: make(chan struct{})}
	l.waiters = append(l.waiters, w)
	l.mu.Unlock()

	// The wait is not the statement running, as in the runner's line.
	defer stmtclock.Hold(ctx)()
	var timeUp <-chan time.Time
	if wait != sqlsandbox.NoMaxWait {
		timer := time.NewTimer(wait)
		defer timer.Stop()
		timeUp = timer.C
	}
	tick := time.NewTicker(l.recheck)
	defer tick.Stop()
	var gaveUp error
	for gaveUp == nil {
		select {
		case <-w.granted:
			if ctx.Err() != nil {
				// Handed a place as it left: given back, for the next.
				l.releaser()()
				return nil, ctx.Err()
			}
			return l.releaser(), nil
		case <-ctx.Done():
			gaveUp = ctx.Err()
		case <-timeUp:
			gaveUp = &pgLaneBusyError{BusyError: &sqlsandbox.BusyError{Waited: wait, MaxInFlight: l.workers()}}
		case <-tick.C:
			// The workers may have changed (raised, or down to one).
			l.mu.Lock()
			l.grantLocked()
			l.mu.Unlock()
		}
	}
	l.mu.Lock()
	if w.got {
		// Handed a place in the same instant the wait ended: it is this
		// statement's, and nobody else was offered it. One whose wait ran
		// out uses it, one whose client left gives it back.
		l.mu.Unlock()
		if ctx.Err() != nil {
			l.releaser()()
			return nil, ctx.Err()
		}
		return l.releaser(), nil
	}
	l.waiters = slices.DeleteFunc(l.waiters, func(o *pgLaneWaiter) bool { return o == w })
	var lane *pgLaneBusyError
	timedOut := errors.As(gaveUp, &lane)
	if timedOut {
		l.refusedWait++
		lane.limit = l.limitLocked()
	}
	waiting := len(l.waiters)
	l.mu.Unlock()
	if timedOut {
		slog.Debug("PostgreSQL router: a long statement was refused, its wait among the long statements over", "waited", wait, "long_limit", lane.limit, "waiting", waiting)
	}
	return nil, gaveUp
}

// pgLaneBusyError is a long statement whose wait for its place ran out: the
// runner's own BusyError (so the port refuses it as copy_wait_timeout, and
// never sends it to the source), said as the wait it was. The runner's text
// would say no statement finished, which short ones may well have.
type pgLaneBusyError struct {
	*sqlsandbox.BusyError
	// limit is how many long statements ran at once.
	limit int
}

func (e *pgLaneBusyError) Error() string {
	return fmt.Sprintf("SQL on the copy is busy: waited %s for a turn among the long statements (%d run at once, and one more worker is kept for statements that read one table and join nothing); try again in a moment",
		e.Waited.Round(time.Second), e.limit)
}

func (e *pgLaneBusyError) Unwrap() error { return e.BusyError }

// releaser is the release of one place taken.
func (l *pgCopyLanes) releaser() func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			l.mu.Lock()
			l.long--
			l.grantLocked()
			l.mu.Unlock()
		})
	}
}

// grantLocked hands free places to those in line, the longest waiting
// first; with no lane (one worker) every one of them gets in.
func (l *pgCopyLanes) grantLocked() {
	limit := l.limitLocked()
	n := 0
	for n < len(l.waiters) && (limit < 1 || l.long < limit) {
		w := l.waiters[n]
		l.long++
		w.got = true
		close(w.granted)
		n++
	}
	if n > 0 {
		clear(l.waiters[:n])
		l.waiters = l.waiters[n:]
	}
}

// run runs f in its lane: a long statement first takes its place, given back
// when f returns.
func (l *pgCopyLanes) run(ctx context.Context, opts pgshim.RunOptions, f func(context.Context) (sqlsandbox.Result, error)) (sqlsandbox.Result, error) {
	release, err := l.enter(ctx, opts)
	if err != nil {
		return sqlsandbox.Result{}, err
	}
	defer release()
	return f(ctx)
}

// pgConnLane is one connection's way into the lanes. The copy refuses some
// statements at once, before it takes a worker, for what does not depend on
// the load (a copy that lives only on S3, among others: a
// *sqlsandbox.UnavailableError, which the port answers from the source). A
// statement refused that way must not first wait its turn among the long
// statements, and be refused as busy instead. So once the copy answered a
// statement of the connection that way, the next ones go to the copy without
// a place, until one is answered: the first such statement of a connection
// still waits its turn.
type pgConnLane struct {
	lanes       *pgCopyLanes
	unavailable atomic.Bool
}

// run is pgCopyLanes.run for this connection.
func (c *pgConnLane) run(ctx context.Context, opts pgshim.RunOptions, f func(context.Context) (sqlsandbox.Result, error)) (sqlsandbox.Result, error) {
	on := c.lanes
	if c.unavailable.Load() {
		on = nil
	}
	res, err := on.run(ctx, opts, f)
	var refused *sqlsandbox.UnavailableError
	switch {
	case errors.As(err, &refused):
		c.unavailable.Store(true)
	case err == nil:
		c.unavailable.Store(false)
	}
	return res, err
}

// pgCopyLanesDoc is the lane's numbers in the router's status document
// (copy_lanes).
type pgCopyLanesDoc struct {
	// KeptForShort is how many workers are kept for short statements, beyond
	// --workers: 1.
	KeptForShort int `json:"kept_for_short"`
	// LongLimit is how many long statements run at once at most: --workers.
	// The copy runs up to LongLimit + KeptForShort statements at once.
	LongLimit int `json:"long_limit"`
	// LongAdmitted are the long statements that hold a place now: running
	// on the copy, or about to, waiting for a worker behind short
	// statements (the runner's line counts those too). LongWaiting are those
	// that wait for a place, here and not in the runner's line.
	LongAdmitted int `json:"long_admitted"`
	LongWaiting  int `json:"long_waiting"`
	// RefusedWaitTimeout and RefusedQueueFull count the long statements
	// refused for want of a place since the router started (53000, counted
	// in the reasons as copy_wait_timeout and copy_queue_full with the
	// runner's own refusals).
	RefusedWaitTimeout uint64 `json:"refused_wait_timeout"`
	RefusedQueueFull   uint64 `json:"refused_queue_full"`
}

// numbers are the lane's numbers now.
func (l *pgCopyLanes) numbers() pgCopyLanesDoc {
	if l == nil {
		return pgCopyLanesDoc{}
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	d := pgCopyLanesDoc{LongAdmitted: l.long, LongWaiting: len(l.waiters), RefusedWaitTimeout: l.refusedWait, RefusedQueueFull: l.refusedFull}
	if limit := l.limitLocked(); limit >= 1 {
		d.KeptForShort, d.LongLimit = 1, limit
	}
	return d
}
