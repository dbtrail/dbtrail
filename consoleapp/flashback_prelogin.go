package consoleapp

import (
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// When the port is full and one more client arrives, the connection that
// has gone longest without finishing its login is closed to make room, if
// it has gone more than flashbackEvictAfter (#3162). A client that logged in
// is never closed for this, so a port full of logged-in clients still
// answers error 1040. The PostgreSQL port follows the same rule.
//
// flashbackEvictWait is how long the accept loop waits for the closed
// connection's goroutine to end and give its place back: its socket is
// closed, so the handshake it was in fails at once. Variables so a test can
// change them.
var (
	flashbackEvictAfter = time.Second
	flashbackEvictWait  = time.Second
)

const (
	preLoginWaiting = iota
	preLoginDone
	preLoginEvicted
)

// preLoginConn is a client's socket while the port counts its clients: it
// notes whether the client has sent anything, and holds the one decision
// between "it logged in" and "it was closed to make room".
type preLoginConn struct {
	net.Conn
	set   *preLoginSet
	since time.Time
	sent  atomic.Bool
	// state is one of the preLogin constants, under set.mu.
	state int
	// gone is closed when the connection's goroutine has ended and its
	// place has been given back.
	gone chan struct{}
}

func (p *preLoginConn) Read(b []byte) (int, error) {
	n, err := p.Conn.Read(b)
	if n > 0 && !p.sent.Load() {
		p.sent.Store(true)
	}
	return n, err
}

// loggedIn records that the handshake succeeded. False: the connection was
// closed to make room first, and must not be served. From then on it is
// never a candidate.
func (p *preLoginConn) loggedIn() bool {
	p.set.mu.Lock()
	defer p.set.mu.Unlock()
	if p.state != preLoginWaiting {
		return false
	}
	p.state = preLoginDone
	delete(p.set.waiting, p)
	return true
}

// evicted reports whether the connection was closed to make room.
func (p *preLoginConn) evicted() bool {
	p.set.mu.Lock()
	defer p.set.mu.Unlock()
	return p.state == preLoginEvicted
}

// preLoginSet is the connections of one port that have not logged in yet.
type preLoginSet struct {
	mu      sync.Mutex
	waiting map[*preLoginConn]struct{}
}

func newPreLoginSet() *preLoginSet {
	return &preLoginSet{waiting: map[*preLoginConn]struct{}{}}
}

func (s *preLoginSet) add(c net.Conn, now time.Time) *preLoginConn {
	p := &preLoginConn{Conn: c, set: s, since: now, gone: make(chan struct{})}
	s.mu.Lock()
	s.waiting[p] = struct{}{}
	s.mu.Unlock()
	return p
}

// ended is called once, when the connection's goroutine has returned and
// its place is back.
func (s *preLoginSet) ended(p *preLoginConn) {
	s.mu.Lock()
	delete(s.waiting, p)
	s.mu.Unlock()
	close(p.gone)
}

// evictOne closes the socket of the connection to make room with, and
// returns it; nil when there is none. Among those that have not logged in
// for more than flashbackEvictAfter, one that has sent nothing goes before
// one that has, and the oldest goes first. The choice and loggedIn are
// under one lock: a connection is either served or closed here, never both,
// and it is chosen at most once.
func (s *preLoginSet) evictOne(now time.Time) *preLoginConn {
	s.mu.Lock()
	var pick *preLoginConn
	for p := range s.waiting {
		if now.Sub(p.since) <= flashbackEvictAfter {
			continue
		}
		if pick == nil {
			pick = p
			continue
		}
		pSent, pickSent := p.sent.Load(), pick.sent.Load()
		if pSent != pickSent {
			if !pSent {
				pick = p
			}
			continue
		}
		if p.since.Before(pick.since) {
			pick = p
		}
	}
	if pick != nil {
		pick.state = preLoginEvicted
		delete(s.waiting, pick)
	}
	s.mu.Unlock()
	if pick != nil {
		// Ends the handshake it is in: its goroutine returns and gives the
		// place back (preLoginConn.gone).
		_ = pick.Conn.Close()
	}
	return pick
}
