package console

import (
	"maps"
	"sync"
	"time"
)

// routingStats is the in-memory tally of the read router's decisions on the
// embedded time-travel port (#2038), per server, since the daemon started. It
// is what GET /api/flashback reports under "routing" so the Connect page can
// say who answered; the Prometheus counter (observe.ObserveRouteDecision) is
// the same tally for an operator's dashboards. Decisions, not successes: a
// statement MySQL then fails was still MySQL's. Nothing is persisted — a
// restart starts the count over, and Since says from when.
type routingStats struct {
	mu    sync.Mutex
	since time.Time
	// perServer is keyed by the registry id the port resolved the connection
	// to (the canonical id, never the display name a client typed), so a
	// renamed server keeps its tally. An entry outlives its server: a deleted
	// server's counts stay until restart, and the page simply has no server
	// to show them under.
	perServer map[string]*routingTally
	// unavailable holds, per server id, why the LAST connection to that
	// server could not route (no source DSN, no SQL on the copy, a DSN the
	// forwarder does not speak) — so the page can say the copy answers
	// everything there instead of showing a rule that does not apply.
	// Cleared when a connection binds a router.
	unavailable map[string]string
	// accountRefused holds, per server id, what the source answered the
	// LAST time a connection of the port tried to log in and was turned
	// away (a wrong password, a host not allowed, a locked account), in the
	// words the page shows. Cleared when a connection logs in. Clients of
	// such a connection get error 2006 on every statement; without this the
	// only trace would be the upstream_lost count.
	accountRefused map[string]string
	// sessionUntracked holds, per server id, why the source does not report
	// session changes to the port's connections (#2127), as the last
	// connection that asked found it. Cleared when a connection is tracked.
	sessionUntracked map[string]string
}

// routingTally is one server's counts.
type routingTally struct {
	copy, mysql, refused uint64
	reasons              map[string]uint64
}

func newRoutingStats(now time.Time) *routingStats {
	return &routingStats{since: now, perServer: map[string]*routingTally{}, unavailable: map[string]string{}, accountRefused: map[string]string{}, sessionUntracked: map[string]string{}}
}

// record tallies one decision. route is "copy", "mysql" or "refused" (the
// shim's RouteCopy/RouteMySQL/RouteRefused); any other value is counted under its reason only, so
// a vocabulary drift shows up as a reason with no side rather than vanishing.
func (r *routingStats) record(serverID, route, reason string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	t := r.perServer[serverID]
	if t == nil {
		t = &routingTally{reasons: map[string]uint64{}}
		r.perServer[serverID] = t
	}
	switch route {
	case "copy":
		t.copy++
	case "mysql":
		t.mysql++
	case "refused":
		t.refused++
	}
	t.reasons[reason]++
}

// setUnavailable records why connections to a server cannot route; an
// empty reason clears the note (a connection just bound a router).
func (r *routingStats) setUnavailable(serverID, reason string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if reason == "" {
		delete(r.unavailable, serverID)
		return
	}
	r.unavailable[serverID] = reason
}

// setAccountRefused records what the source said when it turned the port's
// account away; an empty text clears the note (a connection just logged in).
func (r *routingStats) setAccountRefused(serverID, text string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if text == "" {
		delete(r.accountRefused, serverID)
		return
	}
	r.accountRefused[serverID] = text
}

// setSessionUntracked records why a server's source does not report session
// changes; an empty reason clears the note (a connection is tracked).
func (r *routingStats) setSessionUntracked(serverID, why string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if why == "" {
		delete(r.sessionUntracked, serverID)
		return
	}
	r.sessionUntracked[serverID] = why
}

// snapshot copies the tally for the wire; the caller owns the result. A
// server with only an unavailable note gets an entry with zero counts.
func (r *routingStats) snapshot() map[string]routingServerDTO {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(map[string]routingServerDTO, len(r.perServer))
	for id, t := range r.perServer {
		out[id] = routingServerDTO{Copy: t.copy, MySQL: t.mysql, Refused: t.refused, Reasons: maps.Clone(t.reasons)}
	}
	// The notes: a server with a note and no decision yet still gets an
	// entry, with zero counts.
	note := func(id string, set func(*routingServerDTO)) {
		d, ok := out[id]
		if !ok {
			d = routingServerDTO{Reasons: map[string]uint64{}}
		}
		set(&d)
		out[id] = d
	}
	for id, why := range r.unavailable {
		note(id, func(d *routingServerDTO) { d.Unavailable = why })
	}
	for id, text := range r.accountRefused {
		note(id, func(d *routingServerDTO) { d.AccountRefused = text })
	}
	for id, why := range r.sessionUntracked {
		note(id, func(d *routingServerDTO) { d.SessionUntracked = why })
	}
	return out
}

// RecordRouteDecision tallies one read-routing decision for a server, for
// GET /api/flashback. Called by the port's serving layer (consoleapp) from
// the shim's RouterConfig.Observe hook; safe from any goroutine.
func (s *Server) RecordRouteDecision(serverID, route, reason string) {
	s.routing.record(serverID, route, reason)
}

// RecordRouteUnavailable notes that a connection to serverID could not route
// and why (in the words the page shows); RecordRouteAvailable clears the
// note. Called by the serving layer at bind time, so the page says "the copy
// answers everything here" for a server with no source instead of a rule
// that does not apply to it.
func (s *Server) RecordRouteUnavailable(serverID, reason string) {
	s.routing.setUnavailable(serverID, reason)
}

// RecordRouteAccountRefused notes that the source turned away the account a
// connection of the port tried to log in with, in the words the page shows
// (which account, the user name, MySQL's error number and message; never a
// password). RecordRouteAccountOK clears the note: a connection logged in.
//
// gen is the generation the reporting connection was bound at
// (FlashbackTarget.ForwardGen). A connection bound before the server's
// account changed, or before the server was deleted, speaks of an account
// that is no longer the server's: what it reports is dropped.
func (s *Server) RecordRouteAccountRefused(serverID string, gen uint64, text string) {
	s.routed.whileCurrent(serverID, gen, func() { s.routing.setAccountRefused(serverID, text) })
}

// RecordRouteAccountOK: see RecordRouteAccountRefused.
func (s *Server) RecordRouteAccountOK(serverID string, gen uint64) {
	s.routed.whileCurrent(serverID, gen, func() { s.routing.setAccountRefused(serverID, "") })
}

// RecordRouteSessionUntracked notes that the source of serverID does not
// tell the port's connections when a statement changes a session setting,
// and why, so a setting changed inside a stored function is not seen there
// (#2127). RecordRouteSessionTracked clears the note: a connection is told.
func (s *Server) RecordRouteSessionUntracked(serverID, why string) {
	s.routing.setSessionUntracked(serverID, why)
}

// RecordRouteSessionTracked: see RecordRouteSessionUntracked.
func (s *Server) RecordRouteSessionTracked(serverID string) {
	s.routing.setSessionUntracked(serverID, "")
}

// RecordRouteAvailable: see RecordRouteUnavailable.
func (s *Server) RecordRouteAvailable(serverID string) { s.routing.setUnavailable(serverID, "") }
