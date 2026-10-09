package shim

import (
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"

	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

// ResultBudget bounds the bytes of SQL-on-the-copy results every connection
// of one port holds while it sends them (#2241). A result is built whole in
// this process, which on the embedded port is the one that captures, and
// stays there until the client has read it; the worker's slot
// (--sql-max-in-flight) is given back before that, so without this bound the
// results held at once are as many as the connections receiving one.
//
// A statement is refused before it runs when the port already holds the
// budget or more, and again when its result is ready if the budget was
// taken meanwhile; a result is counted only while the port holds less than
// the budget, so what it holds is under the budget plus one result.
//
// A nil *ResultBudget admits everything: every method is nil-receiver safe.
type ResultBudget struct {
	max     int64
	held    atomic.Int64
	charged atomic.Int64
	refused atomic.Int64
	// lastWarn is when the last refusal was logged, in Unix seconds: one
	// line a minute, however many statements are refused.
	lastWarn atomic.Int64
}

// NewResultBudget returns a budget of max bytes; max <= 0 returns nil
// (no bound).
func NewResultBudget(max int64) *ResultBudget {
	if max <= 0 {
		return nil
	}
	return &ResultBudget{max: max}
}

// Max is the budget in bytes, 0 for none.
func (b *ResultBudget) Max() int64 {
	if b == nil {
		return 0
	}
	return b.max
}

// Held is the bytes of results being sent now.
func (b *ResultBudget) Held() int64 {
	if b == nil {
		return 0
	}
	return b.held.Load()
}

// Charged is every byte ever counted, released or not.
func (b *ResultBudget) Charged() int64 {
	if b == nil {
		return 0
	}
	return b.charged.Load()
}

// Refused is how many statements the budget turned away.
func (b *ResultBudget) Refused() int64 {
	if b == nil {
		return 0
	}
	return b.refused.Load()
}

// ResultsHeldError is the refusal of a statement that found the budget
// taken. It is a busy copy (sqlsandbox.ErrBusy), so a client gets MySQL's
// "try again" code (1203); read routing sends the statement to MySQL under
// a reason of its own (RouteReasonCopyResultsHeld).
type ResultsHeldError struct{ Held, Max int64 }

func (e *ResultsHeldError) Error() string {
	return fmt.Sprintf("this port is still sending %d MB of results to other connections, and keeps at most %d MB of them in memory at once; run the statement again in a moment",
		e.Held>>20, e.Max>>20)
}

func (e *ResultsHeldError) Is(target error) bool { return target == sqlsandbox.ErrBusy }

// refuse counts a refusal, logs one line a minute, and returns the error.
func (b *ResultBudget) refuse(logger *slog.Logger) error {
	n := b.refused.Add(1)
	held := b.held.Load()
	now := time.Now().Unix()
	if last := b.lastWarn.Load(); now-last >= 60 && b.lastWarn.CompareAndSwap(last, now) && logger != nil {
		logger.Warn("free sql: statement refused, the port is still sending as many results as it keeps in memory at once",
			"held_mb", held>>20, "budget_mb", b.max>>20, "refused_so_far", n)
	}
	return &ResultsHeldError{Held: held, Max: b.max}
}

// admit is nil when a statement may run, a *ResultsHeldError when the port
// already holds its budget: asked before the statement runs, so one that
// will be refused costs nothing.
func (b *ResultBudget) admit(logger *slog.Logger) error {
	if b == nil || b.held.Load() < b.max {
		return nil
	}
	return b.refuse(logger)
}

// resultBytes is what a result built for the wire holds: its row data.
func resultBytes(r *mysql.Result) int64 {
	if r == nil || r.Resultset == nil {
		return 0
	}
	var n int64
	for _, row := range r.RowDatas {
		n += int64(len(row))
	}
	return n
}

// holdResult counts r against the budget as the one result this connection
// is sending, or refuses it when the budget was taken while the statement
// ran (many statements can pass admit and then wait for a worker). The port
// therefore never holds more than its budget plus one result. A prepared
// statement's binary result is counted as the text one it is built from:
// the same rows, within a few bytes each.
func (h *Handler) holdResult(r *mysql.Result) error {
	b := h.cfg.ResultBudget
	if b == nil {
		return nil
	}
	h.ReleaseResult()
	n := resultBytes(r)
	for {
		cur := b.held.Load()
		if cur >= b.max {
			return b.refuse(h.logger)
		}
		if b.held.CompareAndSwap(cur, cur+n) {
			break
		}
	}
	h.heldResult.Store(n)
	b.charged.Add(n)
	return nil
}

// ReleaseResult gives back what this connection's last result counted. The
// serving loop calls it once a command's answer has been written
// (Session.HandleCommand), and Close does when the connection ends. Safe to
// call when nothing is held, and more than once.
func (h *Handler) ReleaseResult() {
	if b := h.cfg.ResultBudget; b != nil {
		b.held.Add(-h.heldResult.Swap(0))
	}
}

// resultReleaser is the handler that counts the result it is sending:
// *Handler, and a handler that wraps one.
type resultReleaser interface{ ReleaseResult() }
