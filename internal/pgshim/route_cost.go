package pgshim

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
)

// Routing by the source's plan. The screen (routeFree) says which statements
// the copy answers as PostgreSQL does; this says which of those are worth
// sending there. The copy has no index: a statement that reads a few rows by
// a key is answered by the source in a millisecond and by the copy only after
// reading the whole table, behind every other statement waiting for a worker.
// So a statement the screen lets through is asked about on the source first,
// with EXPLAIN (FORMAT JSON) over the connection the port forwards on, and
// goes to the copy only when its plan is expensive there: a total cost of at
// least CostThreshold, or a sequential scan estimated at ScanRows rows or
// more. Anything else is answered by the source (ReasonCheapPlan). A plan
// that cannot be had (EXPLAIN refused, the connection lost, a statement it
// would not be safe to ask about) sends the statement to the source too
// (ReasonExplainFailed), which then answers it, its own error included. This
// is the MySQL port's rule (internal/readrouter Policy.Decide), with
// PostgreSQL's plan.
//
// Nothing is asked when the screen already sent the statement to the source,
// on a connection that cannot reach its source (the copy answers everything
// there), or inside a transaction block (the source answers everything
// there).

// The thresholds the pg-router starts with. Measured on a TPC-C source of 97
// million rows (PostgreSQL 16): reads by a primary key cost 1 to 12, an
// order's lines by an index 12, a join of the stock level transaction 712,
// count(*) of one warehouse's orders by an index 15,059; the analytics
// reports from 98,367 to 13,351,258, with sequential scans over 2.4 to 61
// million rows. Two cheap reports (177 and 699) are cheap on PostgreSQL too.
const (
	DefaultCostThreshold = 50000
	DefaultScanRows      = 1000000
)

// CostPolicy is when a statement the copy answers as PostgreSQL does is sent
// there: when the top of its plan on the source costs at least CostThreshold,
// OR a sequential scan in it is estimated to return at least ScanRows rows.
// 0 turns that rule off. Both 0 turns routing by plan off: every statement
// the screen lets through goes to the copy without asking the source, as
// before this rule existed.
type CostPolicy struct {
	CostThreshold float64
	ScanRows      int64
	// Remember is how long a connection keeps the verdict of a plan it was
	// given for a statement shape, and sends the shape's next statements by
	// it without asking the source again (rememberedVerdict); 0 is
	// DefaultCostRemember, a negative value remembers nothing.
	Remember time.Duration
}

// DefaultCostRemember is how long a connection keeps a plan's verdict: past
// it, the source's statistics may have changed the plan (an ANALYZE, a table
// that grew), and the shape is asked about again.
const DefaultCostRemember = time.Minute

// remember is how long a verdict is kept, 0 for not at all.
func (p CostPolicy) remember() time.Duration {
	switch {
	case p.Remember < 0:
		return 0
	case p.Remember == 0:
		return DefaultCostRemember
	}
	return p.Remember
}

// DefaultCostPolicy is the policy a Forward without one gets.
func DefaultCostPolicy() CostPolicy {
	return CostPolicy{CostThreshold: DefaultCostThreshold, ScanRows: DefaultScanRows}
}

func (p CostPolicy) off() bool { return p.CostThreshold <= 0 && p.ScanRows <= 0 }

// toCopy says whether a plan is expensive enough for the copy, and why in
// words for the log.
func (p CostPolicy) toCopy(pl pgPlan) (bool, string) {
	if p.CostThreshold > 0 && pl.cost >= p.CostThreshold {
		return true, fmt.Sprintf("plan cost %.0f >= %.0f", pl.cost, p.CostThreshold)
	}
	if p.ScanRows > 0 && pl.maxSeqScanRows >= float64(p.ScanRows) {
		return true, fmt.Sprintf("sequential scan over %.0f rows >= %d", pl.maxSeqScanRows, p.ScanRows)
	}
	return false, fmt.Sprintf("plan cost %.0f below %.0f, no sequential scan over %d rows", pl.cost, p.CostThreshold, p.ScanRows)
}

// pgPlan is what the policy reads of a plan: the top node's Total Cost, and
// the most rows any Seq Scan node, at any depth, is estimated to return.
type pgPlan struct {
	cost           float64
	maxSeqScanRows float64
}

// parsePGPlan reads EXPLAIN (FORMAT JSON)'s answer: an array with one object
// whose "Plan" is the top node, every node's children in "Plans" (subplans,
// init plans and CTEs among them).
//
// A Seq Scan run by parallel workers ("Parallel Aware") gives the rows of one
// process; they are multiplied back by the divisor PostgreSQL split them by
// (get_parallel_divisor, with the leader taking part, as it does by
// default), from the Workers Planned of the Gather above it.
func parsePGPlan(raw []byte) (pgPlan, error) {
	var doc []struct {
		Plan map[string]any `json:"Plan"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		return pgPlan{}, fmt.Errorf("the plan is not EXPLAIN's JSON: %w", err)
	}
	if len(doc) != 1 || doc[0].Plan == nil {
		return pgPlan{}, errors.New("the plan is not one EXPLAIN plan")
	}
	top := doc[0].Plan
	cost, ok := top["Total Cost"].(float64)
	if !ok {
		return pgPlan{}, errors.New("the plan has no Total Cost")
	}
	pl := pgPlan{cost: cost}
	if err := walkPGPlan(top, 1, &pl, 0); err != nil {
		return pgPlan{}, err
	}
	return pl, nil
}

// maxPlanDepth bounds the walk: a plan nested deeper is not one PostgreSQL
// writes.
const maxPlanDepth = 1000

func walkPGPlan(node map[string]any, divisor float64, pl *pgPlan, depth int) error {
	if depth > maxPlanDepth {
		return errors.New("the plan is nested too deep")
	}
	switch node["Node Type"] {
	case "Seq Scan":
		rows, ok := node["Plan Rows"].(float64)
		if !ok {
			return errors.New("a Seq Scan of the plan has no Plan Rows")
		}
		if parallel, _ := node["Parallel Aware"].(bool); parallel {
			rows *= divisor
		}
		pl.maxSeqScanRows = max(pl.maxSeqScanRows, rows)
	case "Gather", "Gather Merge":
		if w, ok := node["Workers Planned"].(float64); ok {
			divisor = parallelDivisor(w)
		}
	}
	children, ok := node["Plans"]
	if !ok {
		return nil
	}
	list, ok := children.([]any)
	if !ok {
		return errors.New("the plan's Plans is not a list")
	}
	for _, c := range list {
		child, ok := c.(map[string]any)
		if !ok {
			return errors.New("a node of the plan is not an object")
		}
		if err := walkPGPlan(child, divisor, pl, depth+1); err != nil {
			return err
		}
	}
	return nil
}

// parallelDivisor is what PostgreSQL divides a parallel scan's rows by, for
// workers planned and the leader taking part (costsize.c
// get_parallel_divisor).
func parallelDivisor(workers float64) float64 {
	d := workers
	if lead := 1 - 0.3*workers; lead > 0 {
		d += lead
	}
	return max(d, 1)
}

// costPolicy is the policy this connection's server routes by.
func (s *session) costPolicy() CostPolicy {
	if s.bound != nil && s.bound.Forward != nil && s.bound.Forward.Cost != nil {
		return *s.bound.Forward.Cost
	}
	return DefaultCostPolicy()
}

// byCostApplies reports whether a decision is one the plan may still change:
// the screen gave it to the copy, on a connection that can reach its source.
func byCostApplies(d routeDecision) bool {
	return d.side == RouteCopy && d.reason == ReasonAnswersTheSame
}

// multiStatement reports whether a text holds more than one statement: EXPLAIN
// of the first would run the others on the source.
func multiStatement(q string) bool {
	toks, why := lexSQL(q)
	if why != "" {
		// Not read whole: a ; past what was read may be missed.
		return true
	}
	for i, t := range toks {
		if t.kind == rtOp && t.text == ";" {
			for _, u := range toks[i+1:] {
				if u.kind != rtOp || u.text != ";" {
					return true
				}
			}
			return false
		}
	}
	return false
}

// routeByCost decides, by the source's plan, a statement the screen gave to
// the copy. generic is set for a prepared statement with parameters, whose
// plan is asked for without values (EXPLAIN GENERIC_PLAN, PostgreSQL 16 and
// later; earlier ones refuse it, and the statement goes to the source).
//
// keep is false when the decision is the moment's (the source could not be
// reached, an extended batch was open on it), which a prepared statement must
// not keep. stop, when not nil, is an error the client gets in place of an
// answer: the statement was cancelled or timed out while its plan was asked
// for, or the source could not be reached, which sending the statement there
// would only try again.
func (s *session) routeByCost(text string, generic bool) (d routeDecision, keep bool, stop *pgErr) {
	d, keep, stop, _ = s.routeByCostPlan(text, generic)
	return d, keep, stop
}

// routeByCostPlan is routeByCost with the plan it decided by, nil when it
// decided without one.
func (s *session) routeByCostPlan(text string, generic bool) (d routeDecision, keep bool, stop *pgErr, plan *pgPlan) {
	pol := s.costPolicy()
	if pol.off() {
		return routeDecision{RouteCopy, ReasonAnswersTheSame}, true, nil, nil
	}
	failed := routeDecision{RouteSource, ReasonExplainFailed}
	if multiStatement(text) {
		s.logger.Debug("pgshim: sent to the source", "reason", ReasonExplainFailed, "why", "more than one statement")
		return failed, true, nil, nil
	}
	if s.up != nil && s.up.inCycle {
		// An extended batch is open there: a simple query cannot be sent in
		// the middle of it, and an error of the port's own in it would
		// discard the client's batch on the source.
		s.logger.Debug("pgshim: sent to the source", "reason", ReasonExplainFailed, "why", "an extended batch is open on the source")
		return failed, false, nil, nil
	}
	up, perr := s.prepareSource()
	if perr != nil {
		if sourceUnreachable(perr) {
			return s.copyWhileUnreachable(perr.code + ": " + perr.msg), false, nil, nil
		}
		return failed, false, perr, nil
	}
	opts := "FORMAT JSON"
	if generic {
		opts = "GENERIC_PLAN, FORMAT JSON"
	}
	raw, srcErr, lost := s.explainOnSource(up, "EXPLAIN ("+opts+") "+text)
	if lost != nil {
		s.loseSource(lost.why)
		return s.copyWhileUnreachable("the connection to the source was lost: " + lost.why), false, nil, nil
	}
	if srcErr != nil {
		if srcErr.code == "57014" {
			return failed, false, srcErr, nil
		}
		if strings.HasPrefix(srcErr.code, "42") {
			// The statement's own (a name the source does not know, a syntax
			// it refuses, GENERIC_PLAN before PostgreSQL 16): it goes there,
			// and the source answers it.
			s.logger.Debug("pgshim: sent to the source", "reason", ReasonExplainFailed, "code", srcErr.code, "err", srcErr.msg)
			return failed, true, nil, nil
		}
		// Anything else (a privilege, a lock, the source busy) may be the
		// moment's: said, and not kept by a prepared statement.
		s.logger.Warn("pgshim: the source did not give a statement's plan; it answers the statement", "code", srcErr.code, "err", srcErr.msg)
		return failed, false, nil, nil
	}
	pl, err := parsePGPlan(raw)
	if err != nil {
		s.logger.Warn("pgshim: the source's plan could not be read; the statement goes to the source", "err", err)
		return failed, true, nil, nil
	}
	toCopy, why := pol.toCopy(pl)
	if !toCopy {
		s.logger.Debug("pgshim: sent to the source", "reason", ReasonCheapPlan, "plan", why)
		return routeDecision{RouteSource, ReasonCheapPlan}, true, nil, &pl
	}
	s.logger.Debug("pgshim: sent to the copy", "plan", why)
	if s.sourceRead && !s.knownValid && s.bound != nil && s.bound.CopyWait != nil {
		// The copy will wait for the source's position (waitForCopy): read
		// on this connection while it is open, not on one opened again.
		if cell, perr, lost := s.cellOnSource(up, sourceWALPositionSQL); lost != nil {
			s.loseSource(lost.why)
		} else if lsn, ok := parseLSN(string(cell)); perr == nil && ok {
			s.copyWant, s.copyWantAt = lsn, time.Now()
		}
	}
	// Nothing of the client's lives on the source: its slot is given back
	// now, not when the copy has answered, which can be minutes.
	s.releaseIdleSource()
	return routeDecision{RouteCopy, ReasonAnswersTheSame}, true, nil, &pl
}

// sourceUnreachable reports whether a failure to reach the source is the
// source's or the port's for the moment (it cannot be reached, it refused
// the forwarding user, its connection was lost, every connection to it is
// taken), not something of the statement's.
func sourceUnreachable(perr *pgErr) bool {
	switch perr.code {
	case "08001", "08004", "08006", "53300":
		return true
	}
	return false
}

// copyWhileUnreachable is the decision for a statement the copy answers as
// PostgreSQL does when its plan cannot be asked because the source cannot be
// reached: the copy answers it, as before routing by plan existed, and the
// client is told so (#2985). A connection the source answered still gets
// the copy only after it waited for the position known after that answer,
// which needs no source (waitForCopy).
func (s *session) copyWhileUnreachable(why string) routeDecision {
	s.logger.Debug("pgshim: the source cannot be asked for a plan; the copy answers", "why", why)
	s.notice("Answered by the copy: the source could not be asked whether this statement is cheaper there (" + why + "), so routing by its plan is off for the moment.")
	return routeDecision{RouteCopy, ReasonSourceUnreachable}
}

// A connection remembers the source's verdict on a statement shape for a
// while (CostPolicy.Remember), so the next statement of that shape, the same
// read by key with another key, is sent where the last one went without
// asking the source for a plan again (#2985). Asking costs a round trip to
// the source for every statement, and a connection there for it: under 50
// clients and a limit of 16 connections it was most of the port's work.
//
// The MySQL port remembers nothing: it asks for every statement's plan, a
// prepared one with its arguments, so a value that changes the plan changes
// the side. A verdict is kept here only where a value cannot do that by much:
//   - the plan was read (cheap or expensive), never a failure to have it: a
//     plan refused for the moment, or a statement the source refused, is
//     asked about again, so those paths stay as they are;
//   - every value in the statement is compared with = (valuesOnlyEqualed):
//     id = 5 and id = 6 read as many rows, while id > 5 and id > 5000000,
//     or a LIMIT of 10 and of a million, do not, and are asked every time;
//   - the plan is far from both thresholds (CostPolicy.clear): a cost of 12
//     under a threshold of 50,000 stays cheap whatever key is read, while
//     one of 30,000 might cross it for another value, and is asked again;
//   - the key is the shape (Fingerprint) and the search_path, under which a
//     name can mean another table.
//
// Prepared statements keep their own decision as before (stmtRoute).

// costVerdict is one remembered verdict and when it was given.
type costVerdict struct {
	d  routeDecision
	at time.Time
}

// costVerdictMargin is how far from a threshold a plan must be for its
// verdict to be kept: ten times under it, or ten times over.
const costVerdictMargin = 10

// costVerdictsMax bounds what a connection remembers; past it, it forgets
// everything and starts again.
const costVerdictsMax = 512

// clear reports whether a plan is far enough from the thresholds that
// another value of the same shape is not expected to change its verdict.
func (p CostPolicy) clear(pl pgPlan) bool {
	toCopy, _ := p.toCopy(pl)
	if toCopy {
		return p.CostThreshold > 0 && pl.cost >= costVerdictMargin*p.CostThreshold ||
			p.ScanRows > 0 && pl.maxSeqScanRows >= costVerdictMargin*float64(p.ScanRows)
	}
	return (p.CostThreshold <= 0 || pl.cost*costVerdictMargin < p.CostThreshold) &&
		(p.ScanRows <= 0 || pl.maxSeqScanRows*costVerdictMargin < float64(p.ScanRows))
}

// valuesOnlyEqualed reports whether every value of a statement (a string, a
// number, a parameter) stands right after an =, and the statement was read
// whole.
func valuesOnlyEqualed(q string) bool {
	toks, why := lexSQL(q)
	if why != "" {
		return false
	}
	for i, t := range toks {
		switch t.kind {
		case rtString, rtNumber, rtParam:
			if i == 0 || toks[i-1].kind != rtOp || toks[i-1].text != "=" {
				return false
			}
		}
	}
	return true
}

// costVerdictKey is what a verdict is remembered by.
func (s *session) costVerdictKey(text string) string {
	return Fingerprint(text) + "\x00" + s.set.schema + "\x00" + strings.Join(s.set.searchPath, ",")
}

// routeByCostRemembered is routeByCost for a simple query, with the verdicts
// the connection remembers: a remembered one is used without asking, and a
// new one is kept when it may be.
func (s *session) routeByCostRemembered(text string) (routeDecision, *pgErr) {
	pol := s.costPolicy()
	keepFor := pol.remember()
	key := ""
	if !pol.off() && keepFor > 0 {
		key = s.costVerdictKey(text)
		if v, ok := s.costVerdicts[key]; ok {
			if time.Since(v.at) < keepFor {
				s.logger.Debug("pgshim: routed by a remembered plan", "side", v.d.side, "reason", v.d.reason)
				return v.d, nil
			}
			delete(s.costVerdicts, key)
		}
	}
	d, keep, stop, pl := s.routeByCostPlan(text, false)
	// Only a verdict for the source is kept: the source answers by its own
	// tables whatever happened to them since, while a kept verdict for the
	// copy would send the shape there after its table was dropped and made
	// again on the source, its old rows still on the copy (#2985); asking
	// costs little next to a statement expensive enough for the copy.
	if key != "" && stop == nil && keep && pl != nil && d.side == RouteSource && pol.clear(*pl) && valuesOnlyEqualed(text) {
		if s.costVerdicts == nil || len(s.costVerdicts) >= costVerdictsMax {
			s.costVerdicts = map[string]costVerdict{}
		}
		s.costVerdicts[key] = costVerdict{d: d, at: time.Now()}
	}
	return d, stop
}

// explainOnSource runs one EXPLAIN as a simple query on the source, outside
// any extended batch, and returns its one cell. It is bounded as every read
// of the source is (relay): the statement's context, statement_timeout and
// the port's cap.
func (s *session) explainOnSource(up *upstream, sql string) (plan []byte, perr *pgErr, lost *errSourceLost) {
	plan, perr, lost = s.cellOnSource(up, sql)
	if perr != nil && perr.code == "XX000" && perr.msg == noCellAnswered {
		perr = &pgErr{"XX000", "EXPLAIN answered no plan"}
	}
	return plan, perr, lost
}

// noCellAnswered is cellOnSource's error for a statement that returned no
// row.
const noCellAnswered = "the source answered no row"

// cellOnSource runs one statement of the port's own as a simple query on the
// source, outside any extended batch, and returns the first cell of its
// first row, bounded as explainOnSource is.
func (s *session) cellOnSource(up *upstream, sql string) (plan []byte, perr *pgErr, lost *errSourceLost) {
	if err := up.send(&pgproto3.Query{String: sql}); err != nil {
		return nil, nil, &errSourceLost{"it could not be written to: " + err.Error()}
	}
	return s.readCell(up)
}

// readCell reads the answer of one simple query of the port's own already
// sent, as cellOnSource does.
func (s *session) readCell(up *upstream) (plan []byte, perr *pgErr, lost *errSourceLost) {
	// A simple query ends the source's unnamed statement.
	delete(up.stmts, "")
	k := &statusKeeper{}
	perr, lost = s.relay(up, relayCapture, func(m pgproto3.BackendMessage) bool {
		if r, ok := m.(*pgproto3.DataRow); ok {
			if plan == nil && len(r.Values) == 1 {
				// The message's buffer is reused by the next one.
				plan = append([]byte{}, r.Values[0]...)
			}
			return false
		}
		return k.done(m)
	})
	if lost != nil {
		return nil, perr, lost
	}
	if k.tx == 'I' {
		clear(up.portals)
	}
	if perr == nil && plan == nil {
		return nil, &pgErr{"XX000", noCellAnswered}, nil
	}
	return plan, perr, nil
}
