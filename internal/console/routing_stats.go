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
}

// routingTally is one server's counts.
type routingTally struct {
	copy, mysql uint64
	reasons     map[string]uint64
}

func newRoutingStats(now time.Time) *routingStats {
	return &routingStats{since: now, perServer: map[string]*routingTally{}, unavailable: map[string]string{}}
}

// record tallies one decision. route is "copy" or "mysql" (the shim's
// RouteCopy/RouteMySQL); any other value is counted under its reason only, so
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

// snapshot copies the tally for the wire; the caller owns the result. A
// server with only an unavailable note gets an entry with zero counts.
func (r *routingStats) snapshot() map[string]routingServerDTO {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(map[string]routingServerDTO, len(r.perServer))
	for id, t := range r.perServer {
		out[id] = routingServerDTO{Copy: t.copy, MySQL: t.mysql, Reasons: maps.Clone(t.reasons), Unavailable: r.unavailable[id]}
	}
	for id, why := range r.unavailable {
		if _, ok := out[id]; !ok {
			out[id] = routingServerDTO{Reasons: map[string]uint64{}, Unavailable: why}
		}
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

// RecordRouteAvailable: see RecordRouteUnavailable.
func (s *Server) RecordRouteAvailable(serverID string) { s.routing.setUnavailable(serverID, "") }
