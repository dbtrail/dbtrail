package shim

import (
	"context"
	"io"
	"net"
	"sync"
)

// watchedConn is the net.Conn handed to go-mysql/server by a shim host:
// writes, deadlines, addresses and Close delegate to the real TCP conn;
// reads come from the pump goroutine's pipe (see WatchConn).
type watchedConn struct {
	net.Conn
	r *io.PipeReader
}

func (w *watchedConn) Read(p []byte) (int, error) { return w.r.Read(p) }

// WatchConn wires a client disconnect to context cancellation (#823). Both
// MySQL-protocol hosts use it: the standalone `bintrail shim` and the
// console's embedded port (watch --flashback-listen, #2033).
//
// The MySQL protocol is strictly request/response: while HandleQuery is
// resolving a time-travel query nothing reads the socket, so a client
// that timed out and closed (FIN/RST) went unnoticed until the query
// finished and the result write failed — the expensive FetchMerged
// (index scan + S3/DuckDB archive fetch) kept running to completion for
// a client that was gone, and a retrying ORM stacked those orphans
// unbounded. WatchConn moves ALL socket reads to a dedicated pump
// goroutine that relays bytes into a synchronous pipe; the protocol
// layer reads the pipe instead. The pump is therefore always parked in
// Read on the real socket and observes EOF/RST the moment the client
// disconnects — even mid-query — and cancels the returned context,
// which the host binds to the Handler so in-flight fetches abort.
//
// The pump reads a little ahead of the protocol layer (watchReadAhead),
// so it sees the close of a client that sent something first, and no
// further: a client pipelining bytes ahead of the protocol reads parks a
// bounded amount here, no unbounded queue. Read deadlines set on the wrapped conn would
// land on the real socket and error the pump, but nothing in the shim
// or go-mysql/server arms one (go-mysql only sets read deadlines when a
// readTimeout option is configured, which the shim never does). TLS
// upgrades (caching_sha2/sha256 auth) layer transparently over the
// wrapped conn: TLS reads flow through the pipe, writes go straight to
// the socket.
//
// The returned stop func must be deferred: it cancels the context and
// closes the pipe's read side so a pump parked in a pipe write unblocks
// once the protocol side stops reading (the host's deferred c.Close
// unblocks a pump parked in the socket read).
func WatchConn(parent context.Context, c net.Conn) (net.Conn, context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(parent)
	pr, pw := io.Pipe()
	ahead := newReadAhead(watchReadAhead)
	// The relay hands the protocol layer what the socket reader took in,
	// in order, and then the error the socket ended with.
	go func() {
		for {
			b, err := ahead.next()
			if b == nil {
				pw.CloseWithError(err)
				return
			}
			if _, werr := pw.Write(b); werr != nil {
				ahead.abandon() // the protocol side stopped reading (stop)
				return
			}
		}
	}()
	go func() {
		buf := make([]byte, 32<<10)
		for {
			n, err := c.Read(buf)
			if n > 0 && !ahead.add(buf[:n]) {
				return
			}
			if err != nil {
				// The client is gone: said first, whatever it sent before
				// leaving is still handed over after.
				cancel()
				ahead.end(err)
				return
			}
		}
	}()
	stop := func() {
		cancel()
		pr.CloseWithError(net.ErrClosed)
	}
	return &watchedConn{Conn: c, r: pr}, ctx, stop
}

// watchReadAhead is how many bytes the socket reader takes in ahead of the
// protocol layer. It is what lets a client that says goodbye before it
// closes be seen leaving: a driver's close sends COM_QUIT and then closes
// the socket, and while a statement is in hand nothing reads those five
// bytes. A reader that stopped at them never reached the close behind them,
// so the statement went on (or went on waiting its turn for the copy, for
// as long as the line lets it) for a client that had left. Past this many
// unread bytes the reader does wait, so a client that sends without reading
// its answers still parks a bounded amount here.
const watchReadAhead = 64 << 10

// readAhead is the bytes the socket reader took in and the protocol layer
// has not read yet.
type readAhead struct {
	mu        sync.Mutex
	changed   *sync.Cond
	max       int
	chunks    [][]byte
	held      int
	err       error // what the socket ended with, once it has
	abandoned bool
}

func newReadAhead(max int) *readAhead {
	a := &readAhead{max: max}
	a.changed = sync.NewCond(&a.mu)
	return a
}

// add keeps a copy of b for the protocol layer, waiting while as much as is
// allowed is already held. False: nobody will read it (abandon).
func (a *readAhead) add(b []byte) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	for a.held >= a.max && !a.abandoned {
		a.changed.Wait()
	}
	if a.abandoned {
		return false
	}
	a.chunks = append(a.chunks, append([]byte(nil), b...))
	a.held += len(b)
	a.changed.Broadcast()
	return true
}

// end says the socket is finished, with the error it ended with.
func (a *readAhead) end(err error) {
	a.mu.Lock()
	a.err = err
	a.changed.Broadcast()
	a.mu.Unlock()
}

// abandon says nobody will read what is held or added.
func (a *readAhead) abandon() {
	a.mu.Lock()
	a.abandoned = true
	a.changed.Broadcast()
	a.mu.Unlock()
}

// next is the oldest bytes held, waiting for some. nil with the socket's
// error once everything it took in was handed over.
func (a *readAhead) next() ([]byte, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	for len(a.chunks) == 0 && a.err == nil {
		a.changed.Wait()
	}
	if len(a.chunks) == 0 {
		return nil, a.err
	}
	b := a.chunks[0]
	a.chunks = a.chunks[1:]
	a.held -= len(b)
	a.changed.Broadcast()
	return b, nil
}
