package pgshim

import (
	"errors"
	"log/slog"
	"net"
	"sync/atomic"
	"time"

	"github.com/dbtrail/dbtrail/internal/shim"
)

// ResultBudget bounds the bytes of results every connection of one port
// holds while it sends them (#2804). It means what the MySQL-protocol port's
// budget means (shim.ResultBudget, #2241), kept by this port for itself: the
// two ports are separate processes and share no memory to bound together.
//
// A result is built whole in this process and stays there until the client
// has read it. Each statement is bounded on its own (the row cap, the cap on
// one result's size) and the copy runs a bounded number at once, but its
// worker is given back before the result is written, so without this bound
// the results held at once are as many as the connections receiving one.
//
// A statement is refused before it runs when the port already holds the
// budget or more, and again when its result is ready if the budget was taken
// meanwhile; a result is counted only while the port holds less than the
// budget, so what it holds is under the budget plus one result.
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

// NewResultBudget returns a budget of max bytes; max <= 0 returns nil (no
// bound).
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

// resultsHeldCode is the SQLSTATE of a statement the budget refused:
// insufficient_resources, the class the port's connection limit answers in
// (53300) and the code a busy copy already gets here (freeError). The MySQL
// port calls the same refusal a busy copy too. What clears it is clients
// reading their answers, so the statement can be sent again as it is.
const resultsHeldCode = "53000"

// refuse counts a refusal, logs one line a minute, and returns the client's
// error. Its sentence is the MySQL port's (shim.ResultsHeldError).
func (b *ResultBudget) refuse(logger *slog.Logger) *pgErr {
	n := b.refused.Add(1)
	held := b.held.Load()
	now := time.Now().Unix()
	if last := b.lastWarn.Load(); now-last >= 60 && b.lastWarn.CompareAndSwap(last, now) && logger != nil {
		logger.Warn("pgshim: statement refused, the port is still sending as many results as it keeps in memory at once",
			"held_mb", held>>20, "budget_mb", b.max>>20, "refused_so_far", n)
	}
	return &pgErr{resultsHeldCode, (&shim.ResultsHeldError{Held: held, Max: b.max}).Error()}
}

// admit is nil when a statement may run, the refusal when the port already
// holds its budget: asked before the statement runs, so one that will be
// refused costs nothing.
func (b *ResultBudget) admit(logger *slog.Logger) *pgErr {
	if b == nil || b.held.Load() < b.max {
		return nil
	}
	return b.refuse(logger)
}

// What a held result costs beside the bytes of its cells. A row is a slice of
// cells (its header, 24 bytes) and goes on the wire as a DataRow message (a
// type byte, a length, a count of cells: 7 bytes); a cell is a slice of bytes
// (its header, 24 bytes) and goes on the wire behind its length (4 bytes). A
// NULL or an empty cell holds no bytes of its own and costs these all the
// same, so a result of many of them is not counted as nothing.
//
// The MySQL port counts each row as it is written for the wire
// (shim.resultBytes): the cell's bytes and its length prefix, one byte for a
// NULL. This counts the same things as they are on this wire, and the
// headers this port keeps for each row and cell while it sends them.
const (
	sliceHeaderBytes  = 24
	rowOverheadBytes  = sliceHeaderBytes + 7
	cellOverheadBytes = sliceHeaderBytes + 4
)

// resultBytes is what a result built for the wire holds: its cells' bytes,
// and the fixed cost of every row and every cell (rowOverheadBytes,
// cellOverheadBytes).
func resultBytes(rows [][][]byte) int64 {
	var n int64
	for _, row := range rows {
		n += rowOverheadBytes + int64(len(row))*cellOverheadBytes
		for _, cell := range row {
			n += int64(len(cell))
		}
	}
	return n
}

// admitResult asks the port's budget whether this connection may run a
// statement that returns rows.
func (s *session) admitResult() *pgErr {
	return s.budget.admit(s.logger)
}

// holdResult counts rows against the budget as the one result this
// connection is sending, or refuses it when the budget was taken while the
// statement ran (many statements can pass admitResult and then wait for a
// worker). The port therefore never holds more than its budget plus one
// result.
func (s *session) holdResult(rows [][][]byte) *pgErr {
	b := s.budget
	if b == nil {
		return nil
	}
	s.releaseResult()
	n := resultBytes(rows)
	for {
		cur := b.held.Load()
		if cur >= b.max {
			return b.refuse(s.logger)
		}
		if b.held.CompareAndSwap(cur, cur+n) {
			break
		}
	}
	s.heldResult = n
	b.charged.Add(n)
	return nil
}

// releaseResult gives back what this connection's last result counted. It is
// called once a statement's answer has been written or never will be
// (handleSimpleQuery), and when the connection ends. Safe to call when
// nothing is held, and more than once. A session belongs to one goroutine,
// its connection's, so what it holds is a plain number.
func (s *session) releaseResult() {
	if b := s.budget; b != nil && s.heldResult != 0 {
		b.held.Add(-s.heldResult)
		s.heldResult = 0
	}
}

// portWriteTimeout is how long one write to a client may block, in
// nanoseconds: the 60 seconds the MySQL port gives (shim's write timeout). A
// write blocks only while the client reads nothing, so a client that takes
// long over a large answer is not cut as long as it keeps reading; one that
// asked and stopped reading is, and its connection ends, which gives back
// the result the port held for it (ResultBudget). Without it such a client
// would hold its part of the budget for as long as it stayed connected. An
// atomic so a test can lower it.
var portWriteTimeout atomic.Int64

func init() { portWriteTimeout.Store(int64(60 * time.Second)) }

// writeChunk is how much of an answer is handed to the socket under one
// deadline. An answer is written whole (pgproto3 flushes its buffer in one
// Write), and one deadline over all of it would cut a client that reads a
// large result slowly; a deadline for each piece cuts only one that reads
// next to nothing for the whole timeout.
const writeChunk = 64 << 10

// deadlineWriter writes to a client in pieces, each under portWriteTimeout.
type deadlineWriter struct {
	c      net.Conn
	logger *slog.Logger
	// stalled says the write timeout was already logged for this connection.
	stalled bool
}

func (w *deadlineWriter) Write(p []byte) (int, error) {
	d := time.Duration(portWriteTimeout.Load())
	written := 0
	for len(p) > 0 {
		piece := p
		if len(piece) > writeChunk {
			piece = p[:writeChunk]
		}
		if d > 0 {
			_ = w.c.SetWriteDeadline(time.Now().Add(d))
		}
		n, err := w.c.Write(piece)
		written += n
		if err != nil {
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() && !w.stalled {
				w.stalled = true
				w.logger.Warn("pgshim: a client read nothing of its answer for the whole write timeout; its connection is closed",
					"remote", w.c.RemoteAddr(), "timeout", d)
			}
			return written, err
		}
		p = p[n:]
	}
	return written, nil
}
