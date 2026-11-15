package pgshim

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/binary"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// Query cancel (#2769). Every connection is told a process number and a
// secret key (BackendKeyData) when it logs in. To stop the statement a
// connection is running, a client opens a second connection and sends only a
// CancelRequest with those two numbers; the port stops that statement, which
// answers 57014, and closes the second connection without a word, as
// PostgreSQL does. The connection whose statement stopped stays open.
//
// The key is compared in constant time, so the time an answer takes says
// nothing about how much of a guessed key was right. A request that names no
// connection, or the wrong key, changes nothing.

var (
	// errCanceledByUser is the cause of a statement a CancelRequest stopped.
	errCanceledByUser = errors.New("canceling statement due to user request")
	// errStatementTimeout is the cause of a statement that ran past the
	// session's statement_timeout.
	errStatementTimeout = errors.New("canceling statement due to statement timeout")
)

// cancelRegistry is the port's list of logged-in connections that a
// CancelRequest can name: one per Serve, shared by its connections.
type cancelRegistry struct {
	mu    sync.Mutex
	conns map[uint32]*cancelTarget
}

func newCancelRegistry() *cancelRegistry {
	return &cancelRegistry{conns: map[uint32]*cancelTarget{}}
}

// cancelTarget is one connection a CancelRequest can stop the statement of.
type cancelTarget struct {
	pid uint32
	key [4]byte

	mu sync.Mutex
	// stop ends the statement running now; nil between statements, when
	// there is nothing to cancel.
	stop context.CancelCauseFunc
}

// register gives a connection its process number and secret key. release
// takes it off the list, at the end of the connection.
func (r *cancelRegistry) register() (t *cancelTarget, release func()) {
	if r == nil {
		return nil, func() {}
	}
	t = &cancelTarget{}
	// Four bytes: the key a protocol 3.0 client (libpq before 18, pgx, JDBC)
	// reads and sends back. crypto/rand never fails on the platforms Go
	// supports.
	_, _ = rand.Read(t.key[:])
	r.mu.Lock()
	for {
		var b [4]byte
		_, _ = rand.Read(b[:])
		// Positive and never 0, as a process number is: some clients read
		// it as a signed integer, and 0 reads as none.
		pid := binary.BigEndian.Uint32(b[:]) & 0x7fffffff
		if _, taken := r.conns[pid]; pid != 0 && !taken {
			t.pid = pid
			r.conns[pid] = t
			break
		}
	}
	r.mu.Unlock()
	return t, func() {
		r.mu.Lock()
		delete(r.conns, t.pid)
		r.mu.Unlock()
	}
}

// cancel stops the statement the connection pid runs now, when key is its
// secret key. It says whether a connection of that number was found and the
// key matched, for the log.
func (r *cancelRegistry) cancel(pid uint32, key []byte) (found, matched bool) {
	r.mu.Lock()
	t := r.conns[pid]
	r.mu.Unlock()
	if t == nil {
		return false, false
	}
	// ConstantTimeCompare answers 0 at once for a key of another length,
	// which says only the length, never which bytes were right.
	if subtle.ConstantTimeCompare(t.key[:], key) != 1 {
		return true, false
	}
	t.mu.Lock()
	if t.stop != nil {
		t.stop(errCanceledByUser)
	}
	t.mu.Unlock()
	return true, true
}

// running makes stop what a cancel request for this connection calls, until
// the returned function is called at the end of the statement. A nil target
// (a port that keeps no list, in a test) cancels nothing.
func (t *cancelTarget) running(stop context.CancelCauseFunc) (done func()) {
	if t == nil {
		return func() {}
	}
	t.mu.Lock()
	t.stop = stop
	t.mu.Unlock()
	return func() {
		t.mu.Lock()
		t.stop = nil
		t.mu.Unlock()
	}
}

// handleCancel answers a CancelRequest read in place of a start-up message:
// it stops the statement it names, if any, and says nothing to the client.
func handleCancel(r *cancelRegistry, pid uint32, key []byte, logger *slog.Logger) {
	if r == nil {
		return
	}
	found, matched := r.cancel(pid, key)
	switch {
	case matched:
		logger.Debug("pgshim: a cancel request stopped the connection's statement, if it was running one", "pid", pid)
	case found:
		// The right number with the wrong key: someone who does not hold
		// the connection's key. Said once in a while, not for every guess.
		now := time.Now().UnixNano()
		last := cancelWarnedAt.Load()
		if now-last >= int64(tlsWarnEvery) && cancelWarnedAt.CompareAndSwap(last, now) {
			logger.Warn("pgshim: a cancel request named a connection with the wrong key, and was ignored (others in the next 10 s are logged at debug level)", "pid", pid)
			return
		}
		logger.Debug("pgshim: a cancel request named a connection with the wrong key", "pid", pid)
	default:
		logger.Debug("pgshim: a cancel request named no connection of this port", "pid", pid)
	}
}

// cancelWarnedAt is when a cancel request with a wrong key was last logged as
// a warning.
var cancelWarnedAt atomic.Int64
