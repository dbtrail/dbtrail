package pgshim

import (
	"container/list"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// A connection holds one of the port's places (Config.MaxConns) from the
// moment it is accepted, before anyone has logged in on it. A port whose
// places are all held by connections that sent nothing would keep every real
// client out for as long as those took to time out (loginTimeout), and they
// can be opened again as fast as they close.
//
// So when the port is full and a new connection arrives, the place of the
// connection that has gone longest without logging in is given to the
// newcomer, if that one has had more than preLoginGrace to log in (#3097):
// its socket is closed, and nothing is said to it. Only a connection that
// has not done its part loses its place: one whose proof of the password
// reached the port keeps it from then on (while the proof is checked and the
// server is opened, however long that takes), and among the others one that
// sent nothing goes before one that sent its start-up message. A connection
// that has logged in never loses its place: with every place held by a
// client, or by one whose proof is being checked, the newcomer is told the
// port is full (53300), as before.

// preLoginGrace is how long a connection that has not logged in keeps its
// place whatever happens. A variable so a test can shorten it.
var preLoginGrace = time.Second

// The states of a place.
const (
	placeWaiting  int32 = iota // accepted, not logged in
	placeLoggedIn              // logged in: never taken
	placeTaken                 // given to a newer connection
)

// connPlace is one connection's place in the port.
type connPlace struct {
	state atomic.Int32
	// spoke: the client sent its start-up message. One that sent nothing
	// loses its place before one that did.
	spoke atomic.Bool
	since time.Time
	conn  net.Conn
	list  *preLogins
	// at is where it stands among the connections that have not logged in;
	// nil once it logged in, left, or lost its place. Under list.mu.
	at *list.Element
}

// preLogins are the connections that hold a place and have not logged in,
// the one accepted first in front.
type preLogins struct {
	mu      sync.Mutex
	waiting *list.List
	// lastWarn is when a taken place was last logged as a warning, in Unix
	// seconds: one line a minute, the rest at debug.
	lastWarn atomic.Int64
}

func newPreLogins() *preLogins { return &preLogins{waiting: list.New()} }

// add gives c a place, as a connection that has not logged in.
func (p *preLogins) add(c net.Conn) *connPlace {
	pl := &connPlace{since: time.Now(), conn: c, list: p}
	p.mu.Lock()
	pl.at = p.waiting.PushBack(pl)
	p.mu.Unlock()
	return pl
}

// forget takes pl out of the connections that have not logged in.
func (p *preLogins) forget(pl *connPlace) {
	p.mu.Lock()
	if pl.at != nil {
		p.waiting.Remove(pl.at)
		pl.at = nil
	}
	p.mu.Unlock()
}

// takeOldest takes the place of the connection that has gone longest
// without logging in, if it has had more than grace, and closes it; nil when
// there is none. One that logs in at the same moment either keeps its place
// (and the next oldest is tried) or loses it, never both: the two change the
// place's state from the same value, and one of them finds it changed.
func (p *preLogins) takeOldest(grace time.Duration) *connPlace {
	p.mu.Lock()
	defer p.mu.Unlock()
	// The oldest that sent nothing first, then the oldest of the rest.
	for _, spoke := range []bool{false, true} {
		for e := p.waiting.Front(); e != nil; e = e.Next() {
			pl := e.Value.(*connPlace)
			if time.Since(pl.since) <= grace {
				// The ones behind it are newer still.
				break
			}
			if pl.spoke.Load() != spoke {
				continue
			}
			if pl.state.CompareAndSwap(placeWaiting, placeTaken) {
				p.waiting.Remove(e)
				pl.at = nil
				_ = pl.conn.Close()
				return pl
			}
		}
	}
	return nil
}

// warnTaken says that old's place went to c: a warning at most once a
// minute, since a port kept full on purpose would say it for every arrival.
func (p *preLogins) warnTaken(logger *slog.Logger, old *connPlace, c net.Conn, max int) {
	now := time.Now().Unix()
	args := []any{"closed", old.conn.RemoteAddr(), "waited", time.Since(old.since).Round(time.Millisecond), "for", c.RemoteAddr(), "max_connections", max}
	if last := p.lastWarn.Load(); now-last >= 60 && p.lastWarn.CompareAndSwap(last, now) {
		logger.Warn("pgshim: the connection limit is reached; the connection that waited longest without logging in was closed to make room for a new one (said once a minute)", args...)
		return
	}
	logger.Debug("pgshim: a connection that had not logged in was closed to make room for a new one", args...)
}

// loggedIn marks the connection as having done its part of the login, which
// keeps its place for as long as it lasts: it is asked when the client's
// proof is about to be checked (Password.gate), and again when the login is
// over. False when the place was taken first: the connection is closed, and
// must not be served, nor its proof checked.
func (pl *connPlace) loggedIn() bool {
	if pl == nil {
		return true
	}
	if pl.state.CompareAndSwap(placeWaiting, placeLoggedIn) {
		pl.list.forget(pl)
		return true
	}
	return pl.state.Load() == placeLoggedIn
}

// leave is the connection ending. It reports whether the place is still its
// own to give back to the port: false when it was given to a newer
// connection, which gives it back when it ends.
func (pl *connPlace) leave() bool {
	if pl == nil {
		return true
	}
	pl.list.forget(pl)
	return pl.state.Load() != placeTaken
}
