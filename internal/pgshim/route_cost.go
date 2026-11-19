package pgshim

import (
	"encoding/json"
	"errors"
	"fmt"

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
	toks, _ := lexSQL(q)
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
	pol := s.costPolicy()
	if pol.off() {
		return routeDecision{RouteCopy, ReasonAnswersTheSame}, true, nil
	}
	failed := routeDecision{RouteSource, ReasonExplainFailed}
	if multiStatement(text) {
		s.logger.Debug("pgshim: sent to the source", "reason", ReasonExplainFailed, "why", "more than one statement")
		return failed, true, nil
	}
	if s.up != nil && s.up.inCycle {
		// An extended batch is open there: a simple query cannot be sent in
		// the middle of it, and an error of the port's own in it would
		// discard the client's batch on the source.
		s.logger.Debug("pgshim: sent to the source", "reason", ReasonExplainFailed, "why", "an extended batch is open on the source")
		return failed, false, nil
	}
	up, perr := s.prepareSource()
	if perr != nil {
		return failed, false, perr
	}
	opts := "FORMAT JSON"
	if generic {
		opts = "GENERIC_PLAN, FORMAT JSON"
	}
	raw, srcErr, lost := s.explainOnSource(up, "EXPLAIN ("+opts+") "+text)
	if lost != nil {
		s.loseSource(lost.why)
		return failed, false, s.lostError(lost)
	}
	if srcErr != nil {
		if srcErr.code == "57014" {
			return failed, false, srcErr
		}
		// The statement's own (a name the source does not know, a syntax it
		// refuses): it goes there, and the source answers it.
		s.logger.Debug("pgshim: sent to the source", "reason", ReasonExplainFailed, "code", srcErr.code, "err", srcErr.msg)
		return failed, true, nil
	}
	pl, err := parsePGPlan(raw)
	if err != nil {
		s.logger.Warn("pgshim: the source's plan could not be read; the statement goes to the source", "err", err)
		return failed, true, nil
	}
	toCopy, why := pol.toCopy(pl)
	if !toCopy {
		s.logger.Debug("pgshim: sent to the source", "reason", ReasonCheapPlan, "plan", why)
		return routeDecision{RouteSource, ReasonCheapPlan}, true, nil
	}
	s.logger.Debug("pgshim: sent to the copy", "plan", why)
	// Nothing of the client's lives on the source: its slot is given back
	// now, not when the copy has answered, which can be minutes.
	s.releaseIdleSource()
	return routeDecision{RouteCopy, ReasonAnswersTheSame}, true, nil
}

// explainOnSource runs one EXPLAIN as a simple query on the source, outside
// any extended batch, and returns its one cell. It is bounded as every read
// of the source is (relay): the statement's context, statement_timeout and
// the port's cap.
func (s *session) explainOnSource(up *upstream, sql string) (plan []byte, perr *pgErr, lost *errSourceLost) {
	if err := up.send(&pgproto3.Query{String: sql}); err != nil {
		return nil, nil, &errSourceLost{"it could not be written to: " + err.Error()}
	}
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
		return nil, &pgErr{"XX000", "EXPLAIN answered no plan"}, nil
	}
	return plan, perr, nil
}
