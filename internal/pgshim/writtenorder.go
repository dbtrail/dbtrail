package pgshim

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

// A statement shape that needed the order written. The copy runs a
// statement that ran out of memory in its own join order once more with its
// joins in the order written (sqlsandbox oomretry.go). On the CH-benCHmark's
// Q9 over fresh-mode views that first run failed after 22 to 34 s every time,
// and the second answered in about 5.5 s. So when the second run answers, the
// port remembers the statement's shape for its server, and the next
// statements of that shape run in the order written from the start: one run
// of about 6 s instead of two of about 30.
//
// The shape is the routing's (costVerdictKey, route_cost.go): the statement
// with every value as ?, so another value of the same report is the same
// shape, with the schema and search_path, under which a name can mean
// another table. A statement read only in part (its shape ends with "…") is
// not remembered: two statements can share that shape. A prepared
// statement's arguments are written into its text before it runs, so the
// two protocols share a shape unless an argument is written with a cast the
// simple statement does not have.
//
// It is remembered only when the retry in the order written answered
// (sqlsandbox.WrittenOrderRetryOK): never from a retry that failed, one that
// was skipped, a cancel, a time-out or any other error. What it learned is
// about the plan, so an answer the port then does not give (copy_too_old, a
// result too large) still counts.
//
// A remembered shape runs with sqlsandbox.Session.KeepJoinOrder, with the
// join check (joinstats.go) of that order alone: a join that would explode
// there refuses it before it runs (join_too_big), since one shape covers
// every value of it. KeepJoinOrder turns off the engine's join order
// optimizer and nothing else, so the answer is the same, only its plan
// changes. It gets at most half of what is left of the client's
// statement_timeout, as the retry does. When it runs out of memory or of
// time in the order written (the copy's cap, or that half), the shape is
// forgotten and the statement goes to the source as one that ran out of
// memory in both orders (copy_refused_after_written_order), as the retry's
// did: it is not run a third time. A cut by the client's statement_timeout
// forgets it too. Any other error leaves it remembered, and is answered as
// that error, as before.
//
// Each shape is kept for WrittenOrderRemember from when it was learned. Using
// it does not make that longer, so a shape is tried in the copy's own order
// again at least once a day: as the data changes, the engine's order may fit
// again, and when it fits it is usually the faster one. A new snapshot of the
// copy does not forget it: the engine picks the bad order because it has no
// statistics of the copy's columns, which a new snapshot does not give it; a
// change in the data's size is what the expiry and the forgetting on running
// out of memory are for. The memory is per server (Binding.WrittenOrders,
// shared by its connections), in memory only, like the routing's (CostSides):
// a restart forgets it.

// DefaultWrittenOrderRemember is how long a shape that needed the order
// written is run that way: forgetting it costs one failed first run per shape
// and day (22 to 34 s for Q9); keeping it too long costs at most a day of a
// plan that may be slower than the engine's own, with the same answer.
const DefaultWrittenOrderRemember = 24 * time.Hour

// writtenOrdersMax bounds the shapes a server remembers; past it, they are
// forgotten and learned again, as CostSides does.
const writtenOrdersMax = 4096

// What a remembered shape did with the statement (RouteNote.WrittenOrder).
const (
	// WrittenOrderRemembered: run in the order written from the start.
	WrittenOrderRemembered = "remembered"
	// WrittenOrderForgotten: run in the order written from the start, it ran
	// out of memory there too, and the shape was forgotten.
	WrittenOrderForgotten = "forgotten"
)

// WrittenOrders is the statement shapes of a server that needed the order
// written, shared by its connections. The zero value is ready; nil
// remembers nothing.
type WrittenOrders struct {
	// Remember is how long a shape is kept from when it was learned: 0 is
	// DefaultWrittenOrderRemember, a negative value remembers nothing.
	Remember time.Duration

	mu sync.Mutex
	at map[string]time.Time
	// now is the clock, time.Now when nil.
	now func() time.Time
}

func (w *WrittenOrders) keepFor() time.Duration {
	switch {
	case w.Remember < 0:
		return 0
	case w.Remember == 0:
		return DefaultWrittenOrderRemember
	}
	return w.Remember
}

func (w *WrittenOrders) clock() time.Time {
	if w.now != nil {
		return w.now()
	}
	return time.Now()
}

// has reports whether key is remembered now; one past its time is forgotten.
func (w *WrittenOrders) has(key string) bool {
	if w == nil || w.keepFor() == 0 {
		return false
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	at, ok := w.at[key]
	if !ok {
		return false
	}
	if w.clock().Sub(at) >= w.keepFor() {
		delete(w.at, key)
		return false
	}
	return true
}

// note remembers key from now, and reports whether it was not remembered
// already: one that is keeps the time it was learned.
func (w *WrittenOrders) note(key string) bool {
	if w == nil || w.keepFor() == 0 {
		return false
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	now := w.clock()
	if at, ok := w.at[key]; ok && now.Sub(at) < w.keepFor() {
		return false
	}
	if w.at == nil || len(w.at) >= writtenOrdersMax {
		w.at = map[string]time.Time{}
	}
	w.at[key] = now
	return true
}

// forget forgets key, and reports whether it was remembered.
func (w *WrittenOrders) forget(key string) bool {
	if w == nil {
		return false
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	_, ok := w.at[key]
	delete(w.at, key)
	return ok
}

// writtenOrderKey is the key text's shape is remembered by for this
// connection's server, "" when the server remembers none or text was read
// only in part.
func (s *session) writtenOrderKey(text string) string {
	if s.bound == nil || s.bound.WrittenOrders == nil || strings.HasSuffix(Fingerprint(text), "…") {
		return ""
	}
	return s.costVerdictKey(text)
}

// startInWrittenOrder is opts for a remembered shape: the order written.
// The join check stays (RunOptions.JoinNDV): with the order written asked
// for, the copy checks that order alone, and refuses the statement when a
// join there would explode (sqlsandbox checkJoinsWritten). A shape keeps
// its key whatever its values, and one value can make it far larger than
// the one it was learned with.
func startInWrittenOrder(opts *RunOptions) {
	opts.KeepJoinOrder = true
}

// writtenOrderContext is the context a remembered shape runs on the copy
// in: on a connection that can send it to its source, at most half of what
// is left of the client's statement_timeout, as the retry in the order
// written gets (sqlsandbox writtenOrderRetryBudget), so the source still has
// the other half when the order written runs out of time. ctx itself
// otherwise.
func (s *session) writtenOrderContext(ctx context.Context) (context.Context, context.CancelFunc) {
	at, ok := sqlsandbox.ClientDeadlineOf(ctx)
	if !ok || !s.canFallBack() {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, time.Until(at)/2)
}

// sentence is text ending in a full stop, so another can follow it.
func sentence(text string) string {
	text = strings.TrimRight(text, " ")
	if strings.HasSuffix(text, ".") {
		return text
	}
	return text + "."
}

// settleWrittenOrder is told what the copy's run of text did, key its shape's
// key ("" for none) and remembered whether it ran in the order written for
// it, in run, a context of ctx's (writtenOrderContext), and returns the
// error to answer with. It remembers a shape whose retry in the order
// written answered, and forgets a remembered one that ran out of memory or
// time in the order written. Where the statement can go to the source, that
// error is then marked as one that ran out of memory in both orders, which
// sends it there; a cut by the client's statement_timeout is that, as it
// always is, and on a connection with no source a time-out stays one.
func (s *session) settleWrittenOrder(ctx, run context.Context, text, key string, remembered bool, res sqlsandbox.Result, err error) error {
	if key == "" {
		return err
	}
	w := s.bound.WrittenOrders
	if !remembered {
		if err == nil && res.WrittenOrderRetry == sqlsandbox.WrittenOrderRetryOK && w.note(key) {
			s.logger.Info("pgshim: a statement shape ran out of memory on the copy in its join order and answered with its joins in the order written; its next statements run in the order written from the start",
				"shape", Fingerprint(text), "for", w.keepFor().String())
		}
		return err
	}
	// The sandbox did not run it again: it ran in the order written, and
	// that is this memory's doing, not a statement that asked for it.
	s.oomRetry = ""
	s.writtenOrder = WrittenOrderRemembered
	if err == nil {
		return nil
	}
	var qe *sqlsandbox.QueryError
	var timedOut *sqlsandbox.TimeoutError
	var marked *sqlsandbox.QueryError
	switch {
	case errors.As(err, &qe) && qe.WrittenOrderRetry == sqlsandbox.WrittenOrderRetrySkippedForcedOrder:
		m := *qe
		m.Message = sentence(m.Message) + " It ran with its joins in the order written, which this statement shape needed before, and ran out of memory in that order too."
		marked = &m
	case errors.Is(context.Cause(ctx), errStatementTimeout):
		// The client's own statement_timeout ended it: answered as that, as
		// always, nothing more to send anywhere.
	case ctx.Err() == nil && run.Err() != nil:
		// Its half of the client's time (writtenOrderContext) is over: the
		// source has the rest.
		marked = &sqlsandbox.QueryError{Message: "The statement ran on the copy for half of what was left of statement_timeout, the most it may take there so that the source has the rest, and was stopped." +
			" It ran with its joins in the order written, which this statement shape needed before because it ran out of memory in the copy's own order."}
	case errors.As(err, &timedOut):
		// Out of the copy's time in the order written: what the retry's own
		// time-out was before this memory (oomretry.go).
		if s.canFallBack() {
			marked = &sqlsandbox.QueryError{Message: sentence(timedOut.Error()) +
				" It ran with its joins in the order written, which this statement shape needed before because it ran out of memory in the copy's own order, and ran out of time in that order."}
		}
	default:
		return err
	}
	s.writtenOrder = WrittenOrderForgotten
	if w.forget(key) {
		line, _, _ := strings.Cut(err.Error(), "\n")
		s.logger.Info("pgshim: a statement shape remembered as needing the order written ran out of memory or time in that order; it is forgotten",
			"shape", Fingerprint(text), "error", line)
	}
	if marked == nil {
		// Nowhere else to send it, or the client's own limit: the time-out
		// is the answer (57014).
		return err
	}
	marked.WrittenOrderRetry = sqlsandbox.WrittenOrderRetryFailed
	return marked
}
