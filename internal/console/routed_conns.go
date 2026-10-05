package console

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/dbtrail/dbtrail/internal/config"
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
	// the connection, which may have read the old entry, is refused. What a
	// connection reports later about its account (whileCurrent) is likewise
	// taken only while its generation is the server's.
	gen   map[string]uint64
	conns map[string]map[uint64]routedConn
}

// routedConn is one tracked connection: how to close it, and the id its
// connection to the source has there (0 while it has none), which is what a
// KILL names.
type routedConn struct {
	drop     func()
	threadID func() uint32
}

func newRoutedConns() *routedConns {
	return &routedConns{gen: map[string]uint64{}, conns: map[string]map[uint64]routedConn{}}
}

func (r *routedConns) generation(serverID string) uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.gen[serverID]
}

// track registers one bound connection; drop is how to close it and
// threadID (may be nil) its thread on the source. ok is false when the
// server's connections were dropped since gen was read: the caller must
// close the connection itself. untrack is called when the connection ends on
// its own.
func (r *routedConns) track(serverID string, gen uint64, drop func(), threadID func() uint32) (untrack func(), ok bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.gen[serverID] != gen {
		return func() {}, false
	}
	r.next++
	id := r.next
	if r.conns[serverID] == nil {
		r.conns[serverID] = map[uint64]routedConn{}
	}
	r.conns[serverID][id] = routedConn{drop: drop, threadID: threadID}
	return func() {
		r.mu.Lock()
		delete(r.conns[serverID], id)
		r.mu.Unlock()
	}, true
}

// whileCurrent runs fn, under the lock, only if gen is still the server's
// generation: no drop happened since the caller's connection was bound.
func (r *routedConns) whileCurrent(serverID string, gen uint64, fn func()) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.gen[serverID] == gen {
		fn()
	}
}

// drop closes every tracked connection of a server and returns the source
// thread of each (to be read after the close; see routedConn). underLock,
// when not nil, runs with the generation already advanced and the lock
// still held, so nothing recorded for the previous generation can land
// after it. The closers run outside the lock: closing a socket must not
// hold up a connection that is registering.
func (r *routedConns) drop(serverID string, underLock func()) []func() uint32 {
	r.mu.Lock()
	r.gen[serverID]++
	dropped := make([]routedConn, 0, len(r.conns[serverID]))
	for _, c := range r.conns[serverID] {
		dropped = append(dropped, c)
	}
	delete(r.conns, serverID)
	if underLock != nil {
		underLock()
	}
	r.mu.Unlock()
	threads := make([]func() uint32, 0, len(dropped))
	for _, c := range dropped {
		c.drop()
		threads = append(threads, c.threadID)
	}
	return threads
}

// TrackRoutedConn registers a client connection of the MySQL-protocol port
// that was bound to a read router for serverID, with the generation its
// FlashbackTarget carried (ForwardGen). drop closes the client connection;
// threadID returns the id of its connection to the source there, or 0 while
// it has not opened one. See routedConns.track.
func (s *Server) TrackRoutedConn(serverID string, gen uint64, drop func(), threadID func() uint32) (untrack func(), ok bool) {
	return s.routed.track(serverID, gen, drop, threadID)
}

// routedKillTimeout bounds the one connection that ends, on the source, the
// statements of the connections just dropped.
const routedKillTimeout = 10 * time.Second

// dropRoutedConns closes the routed connections of a server whose forwarding
// DSN just changed (oldDSN and oldSSL are the ones those connections were
// opened with) or that was deleted, and says so in the log. It also forgets
// a refusal noted for the previous account.
//
// Closing the client's socket ends the port's connection to the source, but
// the source only notices when the statement it is running next touches the
// network: a long statement would run to its end, as the previous account,
// and a write would complete. So the statements are ended there too: one
// short-lived connection with the PREVIOUS account, which may kill its own
// threads, sends KILL for each dropped connection. That runs in the
// background and never holds up or fails the edit; what it could not do is
// logged at Warn.
func (s *Server) dropRoutedConns(serverID, why, oldDSN string, oldSSL config.SSL) {
	threads := s.routed.drop(serverID, func() {
		// What the source said about the previous account is not about
		// this one. Under the lock, so a connection bound before this
		// cannot record it again afterwards (RecordRouteAccountRefused).
		s.routing.setAccountRefused(serverID, "")
	})
	if len(threads) == 0 {
		return
	}
	slog.Info("console: closed this server's open connections on the MySQL-protocol port; they reconnect with the account now in force",
		"server", serverID, "connections", len(threads), "reason", why)
	if s.killSourceThreads == nil || oldDSN == "" {
		return
	}
	s.routedKills.Add(1)
	go func() {
		defer s.routedKills.Done()
		var ids []uint32
		for _, threadID := range threads {
			if threadID == nil {
				continue
			}
			// Read after the close: a connection that was still logging
			// in to the source has its id by now, or never will.
			if id := threadID(); id != 0 {
				ids = append(ids, id)
			}
		}
		if len(ids) == 0 {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), routedKillTimeout)
		defer cancel()
		if err := s.killSourceThreads(ctx, oldDSN, oldSSL, ids); err != nil {
			slog.Warn("console: could not end, on the source, the statements of the connections just closed; one that was running keeps running there as the previous account until it finishes or the source notices the closed connection",
				"server", serverID, "connections", len(ids), "error", scrubDSNError(err, oldDSN))
			return
		}
		slog.Info("console: ended on the source the statements of the connections just closed", "server", serverID, "connections", len(ids))
	}()
}
