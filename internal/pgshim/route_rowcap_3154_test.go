package pgshim

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
)

// A statement the plan sends to the copy whose result the plan estimates
// above the copy's row cap is not tried there (#3154): the copy would run
// it, refuse its result for its size, and the client would get that error.
// The twins of the MySQL port's TestDecideStatement_limitsAndResultRows_2115
// and TestResultOverRowCap, with PostgreSQL's plan.

// TestResultEstimate holds the plan's estimate of a statement's result to
// the one shape it is trusted for: one table read alone with nothing checked
// row by row, under a LIMIT, a sort or a Gather at most.
func TestResultEstimate(t *testing.T) {
	for _, tc := range []struct {
		name string
		top  string
		want float64
	}{
		// Trusted: every row read is a row of the result.
		{"a table read whole", `{"Node Type": "Seq Scan", "Total Cost": 90000, "Plan Rows": 2400000}`, 2400000},
		{"an index read by range", `{"Node Type": "Index Scan", "Total Cost": 60000, "Plan Rows": 246857, "Index Cond": "(placed_at >= '2026-01-01')"}`, 246857},
		{"an index alone read by range", `{"Node Type": "Index Only Scan", "Total Cost": 60000, "Plan Rows": 30000, "Index Cond": "(id > 5)"}`, 30000},
		{"a bitmap read of a range", `{"Node Type": "Bitmap Heap Scan", "Total Cost": 70000, "Plan Rows": 30000, "Recheck Cond": "(w_id = 5)", "Plans": [
			{"Node Type": "Bitmap Index Scan", "Total Cost": 300, "Plan Rows": 30000, "Index Cond": "(w_id = 5)"}]}`, 30000},
		// A LIMIT has cut the top node's rows, whatever is behind it.
		{"a LIMIT over a table read whole", `{"Node Type": "Limit", "Total Cost": 90000, "Plan Rows": 500, "Plans": [
			{"Node Type": "Seq Scan", "Total Cost": 90000, "Plan Rows": 2400000}]}`, 500},
		{"a LIMIT above the cap", `{"Node Type": "Limit", "Total Cost": 90000, "Plan Rows": 50000, "Plans": [
			{"Node Type": "Seq Scan", "Total Cost": 90000, "Plan Rows": 2400000}]}`, 50000},
		{"a sort of a table read whole", `{"Node Type": "Sort", "Total Cost": 400000, "Plan Rows": 2400000, "Plans": [
			{"Node Type": "Seq Scan", "Total Cost": 90000, "Plan Rows": 2400000}]}`, 2400000},
		{"a LIMIT over a sort", `{"Node Type": "Limit", "Total Cost": 400000, "Plan Rows": 20, "Plans": [
			{"Node Type": "Incremental Sort", "Total Cost": 400000, "Plan Rows": 2400000, "Plans": [
				{"Node Type": "Index Scan", "Total Cost": 90000, "Plan Rows": 2400000}]}]}`, 20},
		// The Gather counts every process; the scan under it one of them.
		{"a parallel read of a table", `{"Node Type": "Gather", "Total Cost": 90000, "Plan Rows": 1500000, "Workers Planned": 2, "Plans": [
			{"Node Type": "Seq Scan", "Parallel Aware": true, "Total Cost": 23296, "Plan Rows": 625000}]}`, 1500000},
		{"a parallel read, sorted", `{"Node Type": "Gather Merge", "Total Cost": 90000, "Plan Rows": 1500000, "Workers Planned": 2, "Plans": [
			{"Node Type": "Sort", "Total Cost": 80000, "Plan Rows": 625000, "Plans": [
				{"Node Type": "Seq Scan", "Parallel Aware": true, "Total Cost": 23296, "Plan Rows": 625000}]}]}`, 1500000},

		// Not trusted: a filter checked row by row is a guess (221,203 rows
		// estimated, 1 returned, on the MySQL port's measured b05).
		{"a table read with a filter", `{"Node Type": "Seq Scan", "Total Cost": 90000, "Plan Rows": 221203, "Filter": "(note = 'rare')"}`, 0},
		{"an index range with a filter left over", `{"Node Type": "Index Scan", "Total Cost": 60000, "Plan Rows": 246857, "Index Cond": "(placed_at >= '2026-01-01')", "Filter": "(qty > 5)"}`, 0},
		{"a bitmap read with a filter", `{"Node Type": "Bitmap Heap Scan", "Total Cost": 70000, "Plan Rows": 30000, "Recheck Cond": "(w_id = 5)", "Filter": "(qty > 5)", "Plans": [
			{"Node Type": "Bitmap Index Scan", "Total Cost": 300, "Plan Rows": 30000}]}`, 0},
		{"a LIMIT over a filtered read", `{"Node Type": "Limit", "Total Cost": 90000, "Plan Rows": 50000, "Plans": [
			{"Node Type": "Seq Scan", "Total Cost": 90000, "Plan Rows": 221203, "Filter": "(note = 'rare')"}]}`, 0},
		{"a parallel read with a filter", `{"Node Type": "Gather", "Total Cost": 90000, "Plan Rows": 221203, "Workers Planned": 2, "Plans": [
			{"Node Type": "Seq Scan", "Parallel Aware": true, "Total Cost": 23296, "Plan Rows": 92168, "Filter": "(note = 'rare')"}]}`, 0},
		// Not trusted: what returns something else than the rows it reads.
		{"an aggregate", `{"Node Type": "Aggregate", "Total Cost": 98367, "Plan Rows": 1, "Plans": [
			{"Node Type": "Seq Scan", "Total Cost": 90000, "Plan Rows": 2400000}]}`, 0},
		{"a GROUP BY", `{"Node Type": "Aggregate", "Strategy": "Hashed", "Total Cost": 98367, "Plan Rows": 200000, "Plans": [
			{"Node Type": "Seq Scan", "Total Cost": 90000, "Plan Rows": 2400000}]}`, 0},
		{"a DISTINCT", `{"Node Type": "Unique", "Total Cost": 98367, "Plan Rows": 200000, "Plans": [
			{"Node Type": "Sort", "Total Cost": 98000, "Plan Rows": 2400000, "Plans": [
				{"Node Type": "Seq Scan", "Total Cost": 90000, "Plan Rows": 2400000}]}]}`, 0},
		{"a window", `{"Node Type": "WindowAgg", "Total Cost": 98367, "Plan Rows": 2400000, "Plans": [
			{"Node Type": "Seq Scan", "Total Cost": 90000, "Plan Rows": 2400000}]}`, 0},
		{"a join", `{"Node Type": "Hash Join", "Total Cost": 98367, "Plan Rows": 2400000, "Plans": [
			{"Node Type": "Seq Scan", "Total Cost": 90000, "Plan Rows": 2400000},
			{"Node Type": "Hash", "Total Cost": 700, "Plan Rows": 10, "Plans": [{"Node Type": "Seq Scan", "Total Cost": 600, "Plan Rows": 10}]}]}`, 0},
		{"a LIMIT over a join", `{"Node Type": "Limit", "Total Cost": 98367, "Plan Rows": 50000, "Plans": [
			{"Node Type": "Nested Loop", "Total Cost": 98367, "Plan Rows": 2400000, "Plans": [
				{"Node Type": "Seq Scan", "Total Cost": 90000, "Plan Rows": 2400000},
				{"Node Type": "Index Scan", "Total Cost": 1, "Plan Rows": 1}]}]}`, 0},
		{"a UNION ALL or a partitioned table", `{"Node Type": "Append", "Total Cost": 98367, "Plan Rows": 2400000, "Plans": [
			{"Node Type": "Seq Scan", "Total Cost": 45000, "Plan Rows": 1200000},
			{"Node Type": "Seq Scan", "Total Cost": 45000, "Plan Rows": 1200000}]}`, 0},
		{"a subquery read", `{"Node Type": "Subquery Scan", "Total Cost": 98367, "Plan Rows": 2400000, "Plans": [
			{"Node Type": "Seq Scan", "Total Cost": 90000, "Plan Rows": 2400000}]}`, 0},
		{"a WITH read", `{"Node Type": "CTE Scan", "Total Cost": 98367, "Plan Rows": 2400000, "Plans": [
			{"Node Type": "Seq Scan", "Parent Relationship": "InitPlan", "Total Cost": 90000, "Plan Rows": 2400000}]}`, 0},
		{"a scan with a subplan of its own", `{"Node Type": "Seq Scan", "Total Cost": 98367, "Plan Rows": 2400000, "Plans": [
			{"Node Type": "Result", "Parent Relationship": "InitPlan", "Total Cost": 1, "Plan Rows": 1}]}`, 0},
		{"a bitmap read with an init plan among its children", `{"Node Type": "Bitmap Heap Scan", "Total Cost": 70000, "Plan Rows": 30000, "Recheck Cond": "(w_id = $0)", "Plans": [
			{"Node Type": "Result", "Parent Relationship": "InitPlan", "Total Cost": 1, "Plan Rows": 1},
			{"Node Type": "Bitmap Index Scan", "Parent Relationship": "Outer", "Total Cost": 300, "Plan Rows": 30000}]}`, 0},
		{"a bitmap read with a subplan among its children", `{"Node Type": "Bitmap Heap Scan", "Total Cost": 70000, "Plan Rows": 30000, "Plans": [
			{"Node Type": "Bitmap Index Scan", "Parent Relationship": "Outer", "Total Cost": 300, "Plan Rows": 30000},
			{"Node Type": "Seq Scan", "Parent Relationship": "SubPlan", "Total Cost": 1, "Plan Rows": 1}]}`, 0},
		{"a bitmap read whose child is not a node", `{"Node Type": "Bitmap Heap Scan", "Total Cost": 70000, "Plan Rows": 30000, "Plans": [3]}`, 0},
		{"no table", `{"Node Type": "Result", "Total Cost": 1, "Plan Rows": 1}`, 0},
		{"a function in FROM", `{"Node Type": "Function Scan", "Total Cost": 98367, "Plan Rows": 2400000}`, 0},
		// A plan this cannot read gives no estimate, and no error: the cost
		// and the scans still decide.
		{"a top node without rows", `{"Node Type": "Index Scan", "Total Cost": 90000}`, 0},
		{"rows written as text", `{"Node Type": "Index Scan", "Total Cost": 90000, "Plan Rows": "2400000"}`, 0},
		{"negative rows", `{"Node Type": "Index Scan", "Total Cost": 90000, "Plan Rows": -1}`, 0},
		{"a LIMIT with nothing under it", `{"Node Type": "Limit", "Total Cost": 90000, "Plan Rows": 50000}`, 0},
		{"a LIMIT over two nodes", `{"Node Type": "Limit", "Total Cost": 90000, "Plan Rows": 50000, "Plans": [
			{"Node Type": "Index Scan", "Total Cost": 1, "Plan Rows": 1}, {"Node Type": "Index Scan", "Total Cost": 1, "Plan Rows": 1}]}`, 0},
		{"a node with no type", `{"Total Cost": 90000, "Plan Rows": 2400000}`, 0},
	} {
		pl, err := parsePGPlan([]byte(plan(tc.top)))
		if err != nil {
			t.Errorf("%s: %v", tc.name, err)
			continue
		}
		if pl.resultRows != tc.want {
			t.Errorf("%s: the result is estimated at %v rows, want %v", tc.name, pl.resultRows, tc.want)
		}
	}
	// A chain of sorts deeper than any plan ends without an estimate.
	deep := strings.Repeat(`{"Node Type": "Sort", "Total Cost": 1, "Plan Rows": 5, "Plans": [`, maxPlanDepth+2) +
		`{"Node Type": "Index Scan", "Total Cost": 1, "Plan Rows": 5}` + strings.Repeat("]}", maxPlanDepth+2)
	var top map[string]any
	if err := json.Unmarshal([]byte(deep), &top); err != nil {
		t.Fatal(err)
	}
	if got := resultEstimate(top); got != 0 {
		t.Errorf("a plan nested too deep: estimated at %v rows", got)
	}
}

func TestCostPolicy_overRowCap(t *testing.T) {
	capped := CostPolicy{CostThreshold: 50000, ScanRows: 1000000, RowCap: 10000}
	for _, tc := range []struct {
		name string
		pol  CostPolicy
		rows float64
		over bool
	}{
		{"far over the cap", capped, 2400000, true},
		{"one row over", capped, 10001, true},
		{"exactly the cap: the copy returns it", capped, 10000, false},
		{"under the cap", capped, 500, false},
		{"no estimate to trust", capped, 0, false},
		{"a fraction of a row over", capped, 10000.5, true},
		{"no cap given by the host", CostPolicy{CostThreshold: 50000}, 2400000, false},
		{"a cap that is not one", CostPolicy{CostThreshold: 50000, RowCap: -1}, 2400000, false},
	} {
		why := tc.pol.overRowCap(pgPlan{cost: 90000, resultRows: tc.rows})
		if (why != "") != tc.over {
			t.Errorf("%s: %q, want over=%v", tc.name, why, tc.over)
		}
		if tc.over && !(strings.Contains(why, "row cap of 10000") && strings.Contains(why, "not tried on the copy")) {
			t.Errorf("%s: the reason does not say the cap and what was done: %q", tc.name, why)
		}
	}
	if def := DefaultCostPolicy(); def.RowCap != 0 {
		t.Errorf("the default policy has a row cap of %d: only the host knows the copy's", def.RowCap)
	}
}

// rowCapPlans are the source's plans for the statements below. Each
// expensive one costs 90,000, over the cost threshold.
var rowCapPlans = map[string]string{
	"FROM whole":    plan(`{"Node Type": "Seq Scan", "Total Cost": 90000, "Plan Rows": 2400000}`),
	"FROM atcap":    plan(`{"Node Type": "Seq Scan", "Total Cost": 90000, "Plan Rows": 1000}`),
	"FROM bounded":  plan(`{"Node Type": "Limit", "Total Cost": 90000, "Plan Rows": 500, "Plans": [{"Node Type": "Seq Scan", "Total Cost": 90000, "Plan Rows": 2400000}]}`),
	"FROM filtered": plan(`{"Node Type": "Seq Scan", "Total Cost": 90000, "Plan Rows": 221203, "Filter": "(note = 'rare')"}`),
	"FROM summed":   plan(`{"Node Type": "Aggregate", "Total Cost": 98367, "Plan Rows": 1, "Plans": [{"Node Type": "Seq Scan", "Total Cost": 90000, "Plan Rows": 2400000}]}`),
	"FROM grouped":  plan(`{"Node Type": "Aggregate", "Total Cost": 98367, "Plan Rows": 200000, "Plans": [{"Node Type": "Seq Scan", "Total Cost": 90000, "Plan Rows": 2400000}]}`),
	"FROM joined":   plan(`{"Node Type": "Hash Join", "Total Cost": 98367, "Plan Rows": 2400000, "Plans": [{"Node Type": "Seq Scan", "Total Cost": 90000, "Plan Rows": 2400000}, {"Node Type": "Seq Scan", "Total Cost": 1, "Plan Rows": 10}]}`),
	"FROM small":    plan(`{"Node Type": "Seq Scan", "Total Cost": 40, "Plan Rows": 2000}`),
	"FROM ranged":   plan(`{"Node Type": "Index Scan", "Total Cost": 60000, "Plan Rows": 246857, "Index Cond": "(id > $1)"}`),
	"FROM scanwide": plan(`{"Node Type": "Seq Scan", "Total Cost": 30000, "Plan Rows": 1200000}`),
}

func TestRouteByCost_resultOverRowCap(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	src := startFakeSource(t, rowCapPlans)
	l := &costLogin{src: src, cost: &CostPolicy{CostThreshold: DefaultCostThreshold, ScanRows: DefaultScanRows, RowCap: 1000}}
	c := costPortConn(t, ctx, l)
	toSource := costOutcome{"42", "", RouteSource, ReasonResultOverRowCap}
	toCopy := costOutcome{"7", "", RouteCopy, ReasonAnswersTheSame}
	cheap := costOutcome{"42", "", RouteSource, ReasonCheapPlan}
	for _, tc := range []struct {
		name, stmt string
		want       costOutcome
	}{
		// The statement of the issue: an expensive read of 2,400,000 rows,
		// which the copy would run and refuse at its 1,001st row.
		{"a whole table over the cap", "SELECT n FROM whole", toSource},
		{"over the cap by the scan rule alone", "SELECT n FROM scanwide", toSource},
		// What the copy can return, it answers as before.
		{"exactly the cap", "SELECT n FROM atcap", toCopy},
		{"a LIMIT under the cap", "SELECT n FROM bounded LIMIT 500", toCopy},
		// No estimate to trust: tried on the copy, as before.
		{"a filter checked row by row", "SELECT n FROM filtered WHERE note = 'rare'", toCopy},
		{"an aggregate", "SELECT sum(n) FROM summed", toCopy},
		{"a GROUP BY of many groups", "SELECT n, count(*) FROM grouped GROUP BY n", toCopy},
		{"a join", "SELECT a.n FROM joined a JOIN t b ON b.id = a.id", toCopy},
		// A cheap plan is the source's for being cheap, whatever it returns.
		{"a cheap read over the cap", "SELECT n FROM small", cheap},
	} {
		explained, ran, runs := len(src.explained()), len(src.ran()), l.runs.Load()
		got := costSimple(ctx, c, l, tc.stmt)
		if got != tc.want {
			t.Errorf("%s: %+v, want %+v", tc.name, got, tc.want)
		}
		if asked := src.explained()[explained:]; len(asked) != 1 || asked[0] != "EXPLAIN (FORMAT JSON) "+tc.stmt {
			t.Errorf("%s: the source was asked %q, want the statement's plan once", tc.name, asked)
		}
		onSource, onCopy := len(src.ran())-ran, int(l.runs.Load()-runs)
		if tc.want.side == RouteSource && (onSource != 1 || onCopy != 0) {
			t.Errorf("%s: ran %d time(s) on the source and %d on the copy, want once on the source and never on the copy", tc.name, onSource, onCopy)
		}
		if tc.want.side == RouteCopy && (onSource != 0 || onCopy != 1) {
			t.Errorf("%s: ran %d time(s) on the source and %d on the copy, want once on the copy", tc.name, onSource, onCopy)
		}
	}
	if c.TxStatus() != 'I' {
		t.Errorf("status %c", c.TxStatus())
	}

	// A host that gives no cap (the policy's zero): nothing changes, the
	// copy is tried.
	none := &costLogin{src: startFakeSource(t, rowCapPlans), cost: &CostPolicy{CostThreshold: DefaultCostThreshold, ScanRows: DefaultScanRows}}
	d := costPortConn(t, ctx, none)
	if got := costSimple(ctx, d, none, "SELECT n FROM whole"); got != toCopy {
		t.Errorf("without a row cap: %+v, want the copy", got)
	}
}

// A prepared statement with parameters is asked about without its values
// (GENERIC_PLAN), and there PostgreSQL's rows are fixed guesses: LIMIT $1 is
// taken to let a tenth of the rows through, an index condition on $1 a
// fixed share of the table. Such a plan never sends the statement to the
// source for its size, and the copy answers every execution; the verdict is
// kept for as long as the statement lives, so a guess would send an ORM's
// paging (ORDER BY ... LIMIT $1) to the source for good.
func TestRouteByCost_resultOverRowCap_prepared(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	plans := map[string]string{
		// 2,400,000 rows sorted, and a tenth of them "returned".
		"FROM paged": plan(`{"Node Type": "Limit", "Total Cost": 400000, "Plan Rows": 240000, "Plans": [
			{"Node Type": "Sort", "Total Cost": 400000, "Plan Rows": 2400000, "Plans": [
				{"Node Type": "Seq Scan", "Total Cost": 90000, "Plan Rows": 2400000}]}]}`),
		// A third of the table for id > $1.
		"FROM ranged": plan(`{"Node Type": "Index Scan", "Total Cost": 60000, "Plan Rows": 800000, "Index Cond": "(id > $1)"}`),
		// No parameter in what is read: still not judged.
		"FROM listed": plan(`{"Node Type": "Seq Scan", "Total Cost": 90000, "Plan Rows": 2400000}`),
		"FROM whole":  plan(`{"Node Type": "Seq Scan", "Total Cost": 90000, "Plan Rows": 2400000}`),
	}
	src := startFakeSource(t, plans)
	l := &costLogin{src: src, cost: &CostPolicy{CostThreshold: DefaultCostThreshold, ScanRows: DefaultScanRows, RowCap: 1000}}
	c := costPortConn(t, ctx, l)
	for i, tc := range []struct {
		name, stmt string
		args       [][]byte
		answer     string
		side       string
		reason     string
		explain    string
	}{
		{"paging with LIMIT $1", "SELECT n FROM paged ORDER BY n LIMIT $1", [][]byte{[]byte("20")}, "7", RouteCopy, ReasonAnswersTheSame,
			"EXPLAIN (GENERIC_PLAN, FORMAT JSON) SELECT n FROM paged ORDER BY n LIMIT $1"},
		{"an index range on $1", "SELECT n FROM ranged WHERE id > $1", [][]byte{[]byte("5")}, "7", RouteCopy, ReasonAnswersTheSame,
			"EXPLAIN (GENERIC_PLAN, FORMAT JSON) SELECT n FROM ranged WHERE id > $1"},
		{"a parameter that filters nothing the plan shows", "SELECT n FROM listed WHERE n = $1", [][]byte{[]byte("5")}, "7", RouteCopy, ReasonAnswersTheSame,
			"EXPLAIN (GENERIC_PLAN, FORMAT JSON) SELECT n FROM listed WHERE n = $1"},
		// Without parameters the plan is the statement's own, values and
		// all: over the cap, the source answers every execution.
		{"prepared without parameters", "SELECT n FROM whole", nil, "42", RouteSource, ReasonResultOverRowCap,
			"EXPLAIN (FORMAT JSON) SELECT n FROM whole"},
	} {
		name := fmt.Sprintf("s%d", i)
		before, runs := len(src.explained()), l.runs.Load()
		if _, err := c.Prepare(ctx, name, tc.stmt, nil); err != nil {
			t.Fatalf("%s: prepare: %v", tc.name, err)
		}
		for run := range 3 {
			res := c.ExecPrepared(ctx, name, tc.args, nil, nil).Read()
			if res.Err != nil || len(res.Rows) != 1 || string(res.Rows[0][0]) != tc.answer {
				t.Errorf("%s, run %d: %v %q, want %s", tc.name, run, res.Err, res.Rows, tc.answer)
			}
			if n := l.last(); n.Side != tc.side || n.Reason != tc.reason {
				t.Errorf("%s, run %d: noted %s/%s, want %s/%s", tc.name, run, n.Side, n.Reason, tc.side, tc.reason)
			}
		}
		if asked := src.explained()[before:]; len(asked) != 1 || asked[0] != tc.explain {
			t.Errorf("%s: the source was asked %q, want %q once", tc.name, asked, tc.explain)
		}
		if onCopy := l.runs.Load() - runs; tc.side == RouteSource && onCopy != 0 {
			t.Errorf("%s: the copy ran %d statement(s)", tc.name, onCopy)
		}
	}
	// The same paging written with its number is read from the plan: a
	// LIMIT of 20 is 20 rows, and stays on the copy for that.
	lit := &costLogin{src: startFakeSource(t, map[string]string{
		"FROM paged": plan(`{"Node Type": "Limit", "Total Cost": 400000, "Plan Rows": 20, "Plans": [
			{"Node Type": "Sort", "Total Cost": 400000, "Plan Rows": 2400000, "Plans": [
				{"Node Type": "Seq Scan", "Total Cost": 90000, "Plan Rows": 2400000}]}]}`)}),
		cost: &CostPolicy{CostThreshold: DefaultCostThreshold, ScanRows: DefaultScanRows, RowCap: 1000}}
	d := costPortConn(t, ctx, lit)
	if got := costSimple(ctx, d, lit, "SELECT n FROM paged ORDER BY n LIMIT 20"); got.side != RouteCopy {
		t.Errorf("LIMIT 20 written out: %+v, want the copy", got)
	}
}

// A shape the copy answered keeps no side from a statement sent to the
// source for its size: the next statement of that shape under the cap goes
// to the copy by its plan, as it would have.
func TestRouteByCost_resultOverRowCap_leavesNoSide(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	sides := &CostSides{}
	l := &costLogin{src: startFakeSource(t, rowCapPlans), sides: sides,
		cost: &CostPolicy{CostThreshold: DefaultCostThreshold, ScanRows: DefaultScanRows, RowCap: 1000}}
	c := costPortConn(t, ctx, l)
	if got := costSimple(ctx, c, l, "SELECT n FROM whole"); got.reason != ReasonResultOverRowCap {
		t.Fatalf("%+v", got)
	}
	sides.mu.Lock()
	n := len(sides.sides)
	sides.mu.Unlock()
	if n != 0 {
		t.Errorf("a statement sent to the source for its size left %d side(s) remembered", n)
	}
	if got := costSimple(ctx, c, l, "SELECT n FROM atcap"); got.side != RouteCopy {
		t.Errorf("the next statement under the cap: %+v, want the copy", got)
	}
}

// Every reason the port notes has words in its status.
func TestResultOverRowCap_statusWords(t *testing.T) {
	if w := reasonMeans[ReasonResultOverRowCap]; !strings.Contains(w, "more rows than the copy returns") {
		t.Errorf("status words for %s: %q", ReasonResultOverRowCap, w)
	}
}

// A verdict for the source is remembered for a statement shape only when
// its plan was cheap. One sent there for the size of its result is an
// expensive statement: the same shape with another value is asked about
// again, and goes to the copy when its plan says so.
func TestRouteByCost_resultOverRowCap_notRemembered(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	// Both plans are far over the thresholds, where a verdict is kept.
	src := startFakeSource(t, map[string]string{
		"FROM shaped WHERE k = 5": plan(`{"Node Type": "Index Scan", "Total Cost": 900000, "Plan Rows": 2400000, "Index Cond": "(k = 5)"}`),
		"FROM shaped WHERE k = 6": plan(`{"Node Type": "Seq Scan", "Total Cost": 900000, "Plan Rows": 12000000, "Filter": "(k = 6)"}`),
	})
	l := &costLogin{src: src, cost: &CostPolicy{CostThreshold: DefaultCostThreshold, ScanRows: DefaultScanRows, RowCap: 1000}}
	c := costPortConn(t, ctx, l)
	if got := costSimple(ctx, c, l, "SELECT n FROM shaped WHERE k = 5"); got != (costOutcome{"42", "", RouteSource, ReasonResultOverRowCap}) {
		t.Fatalf("the first value: %+v", got)
	}
	if got := costSimple(ctx, c, l, "SELECT n FROM shaped WHERE k = 6"); got != (costOutcome{"7", "", RouteCopy, ReasonAnswersTheSame}) {
		t.Errorf("the same shape with a value whose plan scans and filters: %+v, want the copy", got)
	}
	if asked := src.explained(); len(asked) != 2 {
		t.Errorf("the source was asked for %d plan(s), want one for each statement: %q", len(asked), asked)
	}
	// A cheap plan's verdict is still kept: asked once for two values.
	cheapSrc := startFakeSource(t, map[string]string{"FROM keyed": plan(`{"Node Type": "Index Scan", "Total Cost": 8.45, "Plan Rows": 1}`)})
	m := &costLogin{src: cheapSrc, cost: &CostPolicy{CostThreshold: DefaultCostThreshold, ScanRows: DefaultScanRows, RowCap: 1000}}
	d := costPortConn(t, ctx, m)
	for _, q := range []string{"SELECT n FROM keyed WHERE id = 1", "SELECT n FROM keyed WHERE id = 2"} {
		if got := costSimple(ctx, d, m, q); got.reason != ReasonCheapPlan {
			t.Errorf("%s: %+v", q, got)
		}
	}
	if asked := cheapSrc.explained(); len(asked) != 1 {
		t.Errorf("a cheap shape was asked about %d time(s), want once", len(asked))
	}
}

func TestRelayedBytes(t *testing.T) {
	for _, tc := range []struct {
		name string
		row  [][]byte
		want int
	}{
		{"no cells", nil, 7},
		{"one NULL", [][]byte{nil}, 11},
		{"one empty cell", [][]byte{{}}, 11},
		{"three NULLs", [][]byte{nil, nil, nil}, 19},
		{"a cell of two bytes", [][]byte{[]byte("42")}, 13},
		{"a NULL beside five bytes", [][]byte{nil, []byte("hello")}, 20},
	} {
		if got := relayedBytes(&pgproto3.DataRow{Values: tc.row}); got != tc.want {
			t.Errorf("%s: %d bytes, want %d", tc.name, got, tc.want)
		}
		// What it counts is the message as encoded.
		if enc, err := (&pgproto3.DataRow{Values: tc.row}).Encode(nil); err != nil || len(enc) != tc.want {
			t.Errorf("%s: encoded in %d bytes (%v), counted as %d", tc.name, len(enc), err, tc.want)
		}
	}
}

// An answer of the source made of rows that hold no bytes (NULLs) reaches
// the client as it comes: the port does not keep it whole until the source
// is done. The source here sends its rows and ends its answer only once the
// client has read one.
func TestRelay_rowsOfNullsAreNotHeldWhole(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	// 120,000 rows of one NULL are 1.3 MB on the wire, past the size the
	// port flushes at, and 0 bytes of cells.
	const rows = 120000
	gate := make(chan struct{})
	src := startFakeSourceWith(t, &fakeSource{emptyRows: rows, emptyGate: gate})
	l := &costLogin{src: src}
	c := costPortConn(t, ctx, l)
	mrr := c.Exec(ctx, "SELECT n FROM manyempty")
	if !mrr.NextResult() {
		close(gate)
		t.Fatalf("no result: %v", mrr.Close())
	}
	rr := mrr.ResultReader()
	if !rr.NextRow() {
		close(gate)
		_, err := rr.Close()
		t.Fatalf("no row reached the client while the source was still answering: %v", err)
	}
	// A row arrived before the source ended its answer.
	close(gate)
	n := 1
	for rr.NextRow() {
		n++
	}
	if _, err := rr.Close(); err != nil {
		t.Fatal(err)
	}
	if err := mrr.Close(); err != nil {
		t.Fatal(err)
	}
	if n != rows {
		t.Errorf("%d rows reached the client, want %d", n, rows)
	}
	if got := l.last(); got.Side != RouteSource {
		t.Errorf("answered by %s/%s, want the source", got.Side, got.Reason)
	}
}
