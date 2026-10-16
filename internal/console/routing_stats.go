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
	// statementsOn: the per-statement table is kept, under the shapes
	// shape gives (Config.StatementShape).
	statementsOn bool
	shape        func(string) string
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
	// answers is keyed by side ("copy", "mysql"): what each side finished,
	// apart from the decisions above. A decision is counted when it is
	// made; an answer when the statement is over, with how long it took.
	answers map[string]*routingAnswers
	// statements is the per-statement table (#2360), nil while it is off
	// or nothing has finished.
	statements *statementTally
}

// routingAnswers is one side's finished statements for one server.
type routingAnswers struct {
	count, failed, timedOut uint64
	total, slowest          time.Duration
}

// RouteAnswers is routingAnswers on the wire: counts and times only.
type RouteAnswers struct {
	// Count is the statements this side finished, failed ones included.
	Count uint64 `json:"count"`
	// Errors is how many of them the client got an error for, and Timeouts
	// how many of those were the statement's time running out.
	Errors   uint64 `json:"errors"`
	Timeouts uint64 `json:"timeouts"`
	// Seconds is the time all of them took, added up, and SlowestSeconds
	// the longest one. For MySQL the time includes sending the rows to the
	// client; for the copy it ends when the answer is ready to send, and
	// includes asking the source for the column names where the port does.
	Seconds        float64 `json:"seconds"`
	SlowestSeconds float64 `json:"slowest_seconds"`
}

func newRoutingStats(now time.Time, shape func(string) string) *routingStats {
	return &routingStats{since: now, statementsOn: shape != nil, shape: shape, perServer: map[string]*routingTally{}, unavailable: map[string]string{}, accountRefused: map[string]string{}, sessionUntracked: map[string]string{}}
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

// answered tallies one finished statement. side is "copy" or "mysql"; any
// other value is dropped, since no column of the page is its.
func (r *routingStats) answered(serverID, side, shape string, took time.Duration, failed, timedOut bool) {
	if side != "copy" && side != "mysql" {
		return
	}
	if took < 0 {
		took = 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	t := r.perServer[serverID]
	if t == nil {
		t = &routingTally{reasons: map[string]uint64{}}
		r.perServer[serverID] = t
	}
	if t.answers == nil {
		t.answers = map[string]*routingAnswers{}
	}
	a := t.answers[side]
	if a == nil {
		a = &routingAnswers{}
		t.answers[side] = a
	}
	a.add(took, failed, timedOut)
	if r.statementsOn {
		t.answeredStatement(shape, side, took, failed, timedOut)
	}
}

// add counts one finished statement.
func (a *routingAnswers) add(took time.Duration, failed, timedOut bool) {
	a.count++
	if failed {
		a.failed++
		if timedOut {
			a.timedOut++
		}
	}
	a.total += took
	if took > a.slowest {
		a.slowest = took
	}
}

// dto is the wire form.
func (a *routingAnswers) dto() RouteAnswers {
	return RouteAnswers{Count: a.count, Errors: a.failed, Timeouts: a.timedOut,
		Seconds: a.total.Seconds(), SlowestSeconds: a.slowest.Seconds()}
}

// answersDTO copies a tally's answers for the wire, nil when it has none.
func (t *routingTally) answersDTO() map[string]RouteAnswers {
	if len(t.answers) == 0 {
		return nil
	}
	out := make(map[string]RouteAnswers, len(t.answers))
	for side, a := range t.answers {
		out[side] = a.dto()
	}
	return out
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
	return r.snapshotLocked()
}

// snapshotWithStatements is snapshot and the statement tables (nil while
// off) read at one instant, so a side's rows add up to its answers.
func (r *routingStats) snapshotWithStatements() (map[string]routingServerDTO, map[string]*RouterStatementTable) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var tables map[string]*RouterStatementTable
	if r.statementsOn {
		tables = r.statementTablesLocked()
	}
	return r.snapshotLocked(), tables
}

func (r *routingStats) snapshotLocked() map[string]routingServerDTO {
	out := make(map[string]routingServerDTO, len(r.perServer))
	for id, t := range r.perServer {
		out[id] = routingServerDTO{Copy: t.copy, MySQL: t.mysql, Refused: t.refused, Reasons: maps.Clone(t.reasons), Answers: t.answersDTO()}
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

// RecordRouteAnswer tallies one statement a side finished: how long it took
// and whether the client got an error for it. Called by the port's serving
// layer from the shim's RouterConfig.Answered hook; safe from any goroutine.
func (s *Server) RecordRouteAnswer(serverID, side, stmt string, took time.Duration, failed, timedOut bool) {
	shape := ""
	if s.routing.statementsOn {
		shape = statementShape(s.routing.shape, stmt)
	}
	s.routing.answered(serverID, side, shape, took, failed, timedOut)
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
