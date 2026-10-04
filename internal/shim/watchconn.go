package shim

import (
	"context"
	"io"
	"net"
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
// io.Pipe is synchronous, so a client pipelining bytes ahead of the
// protocol reads parks at most one io.Copy buffer (32 KiB) in the pump
// — no unbounded queue. Read deadlines set on the wrapped conn would
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
	go func() {
		_, err := io.Copy(pw, c)
		if err == nil {
			err = io.EOF // clean FIN: surface as EOF to the protocol reads
		}
		pw.CloseWithError(err)
		cancel()
	}()
	stop := func() {
		cancel()
		pr.CloseWithError(net.ErrClosed)
	}
	return &watchedConn{Conn: c, r: pr}, ctx, stop
}
