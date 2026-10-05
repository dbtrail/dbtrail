package console

import (
	"log/slog"
	"sync"
)

// routedConns tracks the client connections on the MySQL-protocol port that
// were bound to a read router, per server, so that a change to the account
// the port forwards with takes effect at once (#2079).
//
// A router opens its one upstream connection with the account the server had
// when the client connected and keeps it for the life of that client
// connection. Without this, saving, changing or removing a forwarding account
// would leave every open connection on the previous account for as long as
// the client stayed connected, while the page already described the new one.
// When the forwarding DSN of a server changes (or the server is deleted) its
// tracked connections are closed: the client sees a lost connection,
// reconnects, and is bound to the account in force.
type routedConns struct {
	mu   sync.Mutex
	next uint64
	// gen counts, per server, how many times its connections were dropped.
	// A connection being bound reads it BEFORE it reads the server's entry
	// and hands it back to track: a drop in between changes the number, and
	// the connection, which may have read the old entry, is refused.
	gen   map[string]uint64
	conns map[string]map[uint64]func()
}

func newRoutedConns() *routedConns {
	return &routedConns{gen: map[string]uint64{}, conns: map[string]map[uint64]func(){}}
}

func (r *routedConns) generation(serverID string) uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.gen[serverID]
}

// track registers one bound connection; drop is how to close it. ok is false
// when the server's connections were dropped since gen was read: the caller
// must close the connection itself. untrack is called when the connection
// ends on its own.
func (r *routedConns) track(serverID string, gen uint64, drop func()) (untrack func(), ok bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.gen[serverID] != gen {
		return func() {}, false
	}
	r.next++
	id := r.next
	if r.conns[serverID] == nil {
		r.conns[serverID] = map[uint64]func(){}
	}
	r.conns[serverID][id] = drop
	return func() {
		r.mu.Lock()
		delete(r.conns[serverID], id)
		r.mu.Unlock()
	}, true
}

// drop closes every tracked connection of a server and returns how many.
// The closers run outside the lock: closing a socket must not hold up a
// connection that is registering.
func (r *routedConns) drop(serverID string) int {
	r.mu.Lock()
	r.gen[serverID]++
	closers := make([]func(), 0, len(r.conns[serverID]))
	for _, c := range r.conns[serverID] {
		closers = append(closers, c)
	}
	delete(r.conns, serverID)
	r.mu.Unlock()
	for _, c := range closers {
		c()
	}
	return len(closers)
}

// TrackRoutedConn registers a client connection of the MySQL-protocol port
// that was bound to a read router for serverID, with the generation its
// FlashbackTarget carried (ForwardGen). See routedConns.track.
func (s *Server) TrackRoutedConn(serverID string, gen uint64, drop func()) (untrack func(), ok bool) {
	return s.routed.track(serverID, gen, drop)
}

// dropRoutedConns closes the routed connections of a server whose forwarding
// DSN just changed, and says so in the log. It also forgets a refusal noted
// for the previous account.
func (s *Server) dropRoutedConns(serverID, why string) {
	// What the source said about the previous account is not about this one.
	s.routing.setAccountRefused(serverID, "")
	if n := s.routed.drop(serverID); n > 0 {
		slog.Info("console: closed this server's open connections on the MySQL-protocol port; they reconnect with the account now in force",
			"server", serverID, "connections", n, "reason", why)
	}
}
