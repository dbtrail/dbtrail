package shim

import "time"

// What is expensive on the source goes to the copy (product decision,
// 2026-11-26, the MySQL side of what the PostgreSQL port does since
// 0.140.0). A statement sent as text whose plan on the source is expensive
// is answered by the copy even where the port would otherwise send it to
// MySQL for how the copy computes, sorts or prints a value: a veto of
// readrouter that is only that (readrouter.HardVetoOpts says which are
// not), and the copy's own refusals of the same kind
// (sqlsandbox.Session.HeavyOnCopy). The copy may then answer otherwise than
// MySQL; that is accepted, counted under RouteReasonHeavyOnCopy, and said
// in docs/time-travel-sql.md.
//
// Decided as before, whatever the plan: a statement in a transaction, on a
// pinned connection, whose session the copy does not reproduce or whose
// time zone is not UTC, one whose plan is cheap or could not be had (the
// veto stands, under its own reason), and a veto under which the copy would
// read another statement or needs the live server. A statement the copy
// fails on still goes to MySQL (copy_refused), and nothing of the copy's
// answer has reached the client by then: a result in a file is sent once
// the ladder is done (sendSpooled).
//
// Not for a prepared statement: a client may decode the rows of every
// EXECUTE with the column types its PREPARE was answered with (MariaDB's
// metadata cache, MySQL's optional result set metadata), and a copy that
// types a column otherwise than MySQL (a quotient, an average, a date read
// as a number) would leave it reading those rows wrong.

// heavyState is what the routing ladder decided of the statement it is on.
type heavyState struct {
	// on: the copy is asked for an answer that may differ from MySQL's.
	on bool
	// relaxed is the veto the statement was let past, "" for none.
	relaxed string
	// waived is the first refusal the copy let the statement past
	// (sqlsandbox.Result.Waived), "" for none.
	waived string
}

// why is what the answer may differ for: the veto, or the copy's refusal.
func (s heavyState) why() string {
	if s.relaxed != "" {
		return s.relaxed
	}
	return s.waived
}

// copyReason is the reason the copy's answer to the statement is counted
// under: heavy_on_copy when it was let past a veto or a refusal, whatever
// else holds; tables_unchanged past the copy's age limit; expensive_plan
// otherwise.
func (h *Handler) copyReason(unchangedWithin time.Duration) RouteReason {
	switch {
	case h.heavy.on && h.heavy.why() != "":
		return RouteReasonHeavyOnCopy
	case unchangedWithin > 0:
		return RouteReasonTablesUnchanged
	}
	return RouteReasonExpensivePlan
}

// sessionInUTC says whether the session the statement runs under has no
// time zone but UTC.
func (h *Handler) sessionInUTC() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.sessVars.duckZone == ""
}
