package sqlsandbox

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
)

// The join check (Session.JoinNDV). DuckDB's join-order optimizer reads the
// copy's views with no statistics of their columns, and on some multi-table
// reports it picks an order whose intermediate result does not fit anywhere:
// a CH-benCHmark Q5 joined stock (20M rows) with the filtered orders (240k)
// on the warehouse alone, 200 values, about 2.4e10 rows, and ran out of
// memory after minutes; in the order written it answered in 5.5 s. Turning
// the optimizer off for every statement is worse elsewhere (Q2 went from 0.9
// to 6.9 s). So the statement's own plan is read first, in milliseconds,
// before anything runs:
//
//  1. EXPLAIN (FORMAT JSON) in the statement's own session and views.
//  2. Each join of two inputs is given an estimate of its output: left x
//     right (DuckDB's estimates of its inputs, raised where the copy's views
//     make them fall short: correctRows) over the product of
//     min(ndv(a), ndv(b)) for each a = b of its
//     conditions, that product capped at the larger input. ndv is the
//     source's own count of distinct values of the column (PostgreSQL's
//     pg_stats.n_distinct), which the copy does not have.
//  3. A join is dangerous when that estimate passes joinBlowup times its
//     larger input; a cross product (no condition at all) when it passes
//     both that and crossProductFloor rows.
//  4. A dangerous plan is planned again in the order written
//     (disabled_optimizers = 'join_order'); the statement runs in that order
//     when that plan has no dangerous join, and is refused before it runs
//     (JoinTooBigError) when it has one too.
//
// What the check cannot read it leaves alone: a condition that is not a
// plain column = column (an expression, a range) is not counted, and a join
// with no plain equality is not judged; a column with no
// statistics or one whose name more than one table holds (the plan names
// columns without their table), a join that never gives more rows than one
// side (semi, anti, mark, single, as-of), a missing or zero estimate. Each
// is taken as selective: the check only ever stops what it can show. In the
// order written DuckDB estimates less: a bare table function or file read
// gets 0 (its estimator is part of the optimizer turned off) while the
// projection over it, which every view of the copy is, keeps its number, and
// a cross product writes no estimate of its own, so a join over one is not
// judged.
//
// The two bounds were set on the 22 CH-benCHmark reports. Over plain cast
// views (testdata) the check flags exactly Q5, Q8 and Q21 in DuckDB's order;
// Q5 and Q8 pass in the order written, and Q21 does not (a cross product of
// order_line and supplier, 6e11 rows). Over the copy's own views, with and
// without fresh mode's tails, it flags Q5 and Q21: Q5 gave no answer in 210 s
// in DuckDB's order and answered in 7.5 s in the order written, and Q21 ran
// out of memory in both. Not caught: Q9 over fresh-mode views, whose order
// keeps each join's output in proportion but builds a hash table from a
// side DuckDB takes for small. A cross product needs the floor because a ratio
// alone cannot tell Q21's from Q8's in the order written (1e5 x 1e4, which
// answers): both are 1e4 times their larger input.

const (
	// joinBlowup is how many times its larger input a join's estimated
	// output may be before it is dangerous.
	joinBlowup = 100
	// crossProductFloor is the fewest rows a dangerous cross product gives.
	crossProductFloor = 1e10
)

// joinOrderWritten is wireResult.JoinOrder for a statement the check ran in
// the order written.
const joinOrderWritten = "written"

// joinDanger is the plan's verdict: planJoinDanger, a variable so a test can
// stand in for it.
var joinDanger = planJoinDanger

// planNode is one node of DuckDB's EXPLAIN (FORMAT JSON), with what the
// check reads of it.
type planNode struct {
	Name      string         `json:"name"`
	ExtraInfo map[string]any `json:"extra_info"`
	Children  []planNode     `json:"children"`
	// rows is the node's estimate as the check takes it (correctRows).
	rows float64
}

// correctRows sets the rows of n and of every node under it: DuckDB's own
// estimate, raised where DuckDB's is known to fall far short over the copy's
// views. A view read with its recent changes (fresh mode), and a snapshot
// with rows deleted since it was written, is a table's file anti-joined with
// what replaced or deleted its rows, in a UNION with the newer ones, and
// DuckDB takes each anti join to keep a fifth of its input: over the
// CH-benCHmark's tails it estimated stock, 20 million rows, at 844,417, and
// order_line, 60 million, at 2.5 million, which hides a join of the two from
// the check. So here an anti join keeps the larger of its inputs (it never
// keeps more), a UNION is the sum of its inputs (DuckDB writes none of its
// own), and any other node keeps DuckDB's estimate times the most any of its
// inputs was raised, so a filter's selectivity stands; a node over an input
// that has no estimate of DuckDB's (a UNION) is raised to that input's.
// Without anti joins nothing changes.
func correctRows(n *planNode) float64 {
	own := estimatedRows(n)
	if len(n.Children) == 0 {
		n.rows = own
		return own
	}
	in := make([]float64, len(n.Children))
	for i := range n.Children {
		in[i] = correctRows(&n.Children[i])
	}
	t, _ := n.ExtraInfo["Join Type"].(string)
	switch t = strings.ToUpper(t); {
	case (t == "ANTI" || t == "RIGHT_ANTI") && len(in) == 2:
		n.rows = math.Max(in[0], in[1])
	case n.Name == "UNION":
		n.rows = 0
		for _, r := range in {
			n.rows += r
		}
	default:
		raise := 0.0
		for i := range n.Children {
			switch theirs := estimatedRows(&n.Children[i]); {
			case in[i] <= 0:
			case theirs > 0:
				raise = math.Max(raise, in[i]/theirs)
			case own > 0:
				raise = math.Max(raise, in[i]/own)
			}
		}
		n.rows = own
		if own > 0 && raise > 0 {
			n.rows = own * raise
		}
	}
	return n.rows
}

// planJoinDanger says which join of plan, the JSON array EXPLAIN (FORMAT
// JSON) returns, is dangerous by the rule above, "" when none is. ndv is
// the distinct values of each column by lowercased name; without any, no
// join is.
func planJoinDanger(plan []byte, ndv map[string]float64) (string, error) {
	var roots []planNode
	if err := json.Unmarshal(plan, &roots); err != nil {
		return "", fmt.Errorf("read the copy's plan: %w", err)
	}
	if len(ndv) == 0 {
		return "", nil
	}
	worst := dangerJoin{}
	for i := range roots {
		correctRows(&roots[i])
		worst = worstJoin(&roots[i], ndv, worst)
	}
	return worst.why, nil
}

// dangerJoin is a dangerous join: why, and the rows it is estimated to give.
type dangerJoin struct {
	why  string
	rows float64
}

// worstJoin is the dangerous join at or under n estimated to give the most
// rows, worst when none gives more.
func worstJoin(n *planNode, ndv map[string]float64, worst dangerJoin) dangerJoin {
	if d := judgeJoin(n, ndv); d.why != "" && d.rows > worst.rows {
		worst = d
	}
	for i := range n.Children {
		worst = worstJoin(&n.Children[i], ndv, worst)
	}
	return worst
}

// joinTypesBounded are the join types whose output is never larger than the
// side they probe.
var joinTypesBounded = map[string]bool{
	"SEMI": true, "ANTI": true, "MARK": true, "SINGLE": true, "RIGHT_SEMI": true, "RIGHT_ANTI": true,
}

// judgeJoin is n alone as a dangerous join, the zero value when it is not
// one or not dangerous.
func judgeJoin(n *planNode, ndv map[string]float64) dangerJoin {
	cross := n.Name == "CROSS_PRODUCT"
	if !cross && (!strings.Contains(n.Name, "JOIN") || strings.Contains(n.Name, "ASOF")) {
		return dangerJoin{}
	}
	if len(n.Children) != 2 {
		return dangerJoin{}
	}
	if t, _ := n.ExtraInfo["Join Type"].(string); joinTypesBounded[strings.ToUpper(t)] {
		return dangerJoin{}
	}
	left, right := n.Children[0].rows, n.Children[1].rows
	if left <= 0 || right <= 0 {
		return dangerJoin{}
	}
	larger := math.Max(left, right)
	conds := conditionsOf(n.ExtraInfo["Conditions"])
	if len(conds) == 0 {
		if out := left * right; cross && out > joinBlowup*larger && out > crossProductFloor {
			return dangerJoin{fmt.Sprintf("%s of %.0f x %.0f estimated rows gives %.1e rows", n.Name, left, right, out), out}
		}
		return dangerJoin{}
	}
	combo, pairs := 1.0, 0
	for _, c := range conds {
		a, b, ok := columnEquality(c)
		if !ok {
			continue
		}
		pairs++
		na, nb := ndv[a], ndv[b]
		if na <= 0 || nb <= 0 {
			// A column the check knows nothing of: taken as a selective
			// key, which caps the join at its larger input below.
			combo = math.Inf(1)
			break
		}
		combo *= math.Max(math.Min(na, nb), 1)
	}
	if pairs == 0 {
		return dangerJoin{}
	}
	combo = math.Min(combo, larger)
	if out := left * right / combo; out > joinBlowup*larger {
		return dangerJoin{fmt.Sprintf("%s on %s of %.0f x %.0f estimated rows over %.0f distinct values gives %.1e rows",
			n.Name, strings.Join(conds, " AND "), left, right, combo, out), out}
	}
	return dangerJoin{}
}

// estimatedRows is a node's Estimated Cardinality, 0 when it has none or it
// cannot be read. DuckDB writes it as a string.
func estimatedRows(n *planNode) float64 {
	var s string
	switch v := n.ExtraInfo["Estimated Cardinality"].(type) {
	case string:
		s = v
	case float64:
		return v
	default:
		return 0
	}
	s = strings.TrimPrefix(strings.TrimSpace(strings.ReplaceAll(s, ",", "")), "~")
	f, err := strconv.ParseFloat(s, 64)
	if err != nil || math.IsNaN(f) || math.IsInf(f, 0) {
		return 0
	}
	return f
}

// conditionsOf is a join's Conditions, which DuckDB writes as one string for
// one condition and as a list for several.
func conditionsOf(v any) []string {
	var out []string
	switch c := v.(type) {
	case string:
		for _, part := range strings.Split(c, " AND ") {
			if part = strings.TrimSpace(part); part != "" {
				out = append(out, part)
			}
		}
	case []any:
		for _, e := range c {
			if s, ok := e.(string); ok && strings.TrimSpace(s) != "" {
				out = append(out, strings.TrimSpace(s))
			}
		}
	}
	return out
}

// planIdent is one name of a column in a plan's condition, bare or quoted;
// planEquality a condition that is exactly one column, perhaps qualified,
// equal to another.
const planIdent = `(?:"(?:[^"]|"")+"|[\p{L}_][\p{L}\p{N}_$]*)`

var (
	planIdentRe  = regexp.MustCompile(planIdent)
	planEquality = regexp.MustCompile(`^\s*(` + planIdent + `(?:\.` + planIdent + `)*)\s*=\s*(` + planIdent + `(?:\.` + planIdent + `)*)\s*$`)
)

// columnEquality reads cond as a = b of two columns, each by its own
// lowercased name (the last part of a qualified one, unquoted).
func columnEquality(cond string) (a, b string, ok bool) {
	m := planEquality.FindStringSubmatch(cond)
	if m == nil {
		return "", "", false
	}
	return lastName(m[1]), lastName(m[2]), true
}

func lastName(qualified string) string {
	parts := planIdentRe.FindAllString(qualified, -1)
	p := parts[len(parts)-1]
	if strings.HasPrefix(p, `"`) {
		p = strings.ReplaceAll(p[1:len(p)-1], `""`, `"`)
	}
	return strings.ToLower(p)
}

// joinCheck is what the check decided for a statement in the worker.
type joinCheck struct {
	// written: run it in the order written.
	written bool
	// refusal: do not run it, and why.
	refusal string
	// skipped: the check could not be made, and why; the statement runs as
	// it would without one.
	skipped string
}

// checkJoins runs the check on conn, the statement's session with its views
// and settings in place and not yet locked (the order written is a SET).
// skipCommonSubplan is the session's own (Session.SkipCommonSubplan), kept in
// the list the order written replaces.
func checkJoins(ctx context.Context, conn *sql.Conn, statement string, ndv map[string]float64, skipCommonSubplan bool) joinCheck {
	plan, err := explainJSON(ctx, conn, statement)
	if err != nil {
		return joinCheck{skipped: "the copy's plan could not be had: " + firstLine(err.Error())}
	}
	first, err := joinDanger(plan, ndv)
	if err != nil {
		return joinCheck{skipped: err.Error()}
	}
	if first == "" {
		return joinCheck{}
	}
	// The order DuckDB picked is dangerous. Whatever happens next, the
	// statement does not run in it.
	refuse := func(written string) joinCheck {
		return joinCheck{refusal: "a join of the statement on the copy is estimated far past its inputs in the copy's join order (" + first +
			") and in the order written (" + written + "), so it was not run there"}
	}
	if _, err := conn.ExecContext(ctx, disabledOptimizersSQL(true, skipCommonSubplan)); err != nil {
		return refuse("it could not be planned: " + firstLine(err.Error()))
	}
	plan, err = explainJSON(ctx, conn, statement)
	if err != nil {
		return refuse("it could not be planned: " + firstLine(err.Error()))
	}
	second, err := joinDanger(plan, ndv)
	switch {
	case err != nil:
		return refuse(err.Error())
	case second != "":
		return refuse(second)
	}
	return joinCheck{written: true}
}

// disabledOptimizersSQL is the session's one SET of DuckDB's
// disabled_optimizers, "" when it turns none off: join_order for the joins
// in the order written (Session.KeepJoinOrder, or the join check), and
// common_subplan for the PostgreSQL port (Session.SkipCommonSubplan). A SET
// replaces the whole list, so every writer of it goes through here.
func disabledOptimizersSQL(keepJoinOrder, skipCommonSubplan bool) string {
	var off []string
	if keepJoinOrder {
		off = append(off, "join_order")
	}
	if skipCommonSubplan {
		off = append(off, "common_subplan")
	}
	if len(off) == 0 {
		return ""
	}
	return "SET disabled_optimizers = '" + strings.Join(off, ",") + "'"
}

// explainJSON is DuckDB's plan of statement as EXPLAIN (FORMAT JSON) writes
// it: the value of its one row.
func explainJSON(ctx context.Context, conn *sql.Conn, statement string) ([]byte, error) {
	rows, err := conn.QueryContext(ctx, "EXPLAIN (FORMAT JSON) "+statement)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return nil, err
		}
		return nil, errors.New("EXPLAIN answered no row")
	}
	cells := make([]sql.NullString, len(cols))
	ptrs := make([]any, len(cols))
	for i := range cells {
		ptrs[i] = &cells[i]
	}
	if err := rows.Scan(ptrs...); err != nil {
		return nil, err
	}
	// explain_key, explain_value: the plan is the value, the last column.
	for i, c := range cols {
		if c == "explain_value" {
			return []byte(cells[i].String), nil
		}
	}
	return []byte(cells[len(cells)-1].String), nil
}
