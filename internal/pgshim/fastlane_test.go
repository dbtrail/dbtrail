package pgshim

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

// readsOneTable over the plans PostgreSQL writes: what reads one table and
// joins nothing is short; anything else, and anything this does not know,
// is not.
func TestReadsOneTable(t *testing.T) {
	for _, tc := range []struct {
		name string
		plan string
		want bool
	}{
		{"a sequential scan", `{"Node Type": "Seq Scan", "Relation Name": "orders", "Plan Rows": 30000000}`, true},
		{"a sum of one warehouse by an index", `{"Node Type": "Aggregate", "Plans": [{"Node Type": "Index Scan", "Parent Relationship": "Outer", "Relation Name": "order_line"}]}`, true},
		{"an index only scan under a limit", `{"Node Type": "Limit", "Plans": [{"Node Type": "Index Only Scan", "Parent Relationship": "Outer", "Relation Name": "t"}]}`, true},
		{"a parallel count", `{"Node Type": "Aggregate", "Partial Mode": "Finalize", "Plans": [{"Node Type": "Gather", "Parent Relationship": "Outer", "Workers Planned": 2, "Plans": [{"Node Type": "Aggregate", "Partial Mode": "Partial", "Parent Relationship": "Outer", "Plans": [{"Node Type": "Seq Scan", "Parallel Aware": true, "Parent Relationship": "Outer", "Relation Name": "orders"}]}]}]}`, true},
		{"a grouped and sorted report of one table", `{"Node Type": "Sort", "Plans": [{"Node Type": "Aggregate", "Strategy": "Hashed", "Parent Relationship": "Outer", "Plans": [{"Node Type": "Seq Scan", "Parent Relationship": "Outer", "Relation Name": "order_line"}]}]}`, true},
		{"gather merge, incremental sort, unique, group, window", `{"Node Type": "Unique", "Plans": [{"Node Type": "WindowAgg", "Parent Relationship": "Outer", "Plans": [{"Node Type": "Group", "Parent Relationship": "Outer", "Plans": [{"Node Type": "Incremental Sort", "Parent Relationship": "Outer", "Plans": [{"Node Type": "Gather Merge", "Parent Relationship": "Outer", "Plans": [{"Node Type": "Index Scan", "Parent Relationship": "Outer", "Relation Name": "t"}]}]}]}]}]}`, true},
		{"a bitmap scan of two indexes of the table", `{"Node Type": "Aggregate", "Plans": [{"Node Type": "Bitmap Heap Scan", "Parent Relationship": "Outer", "Relation Name": "t", "Plans": [{"Node Type": "BitmapAnd", "Parent Relationship": "Outer", "Plans": [{"Node Type": "Bitmap Index Scan", "Parent Relationship": "Member", "Index Name": "a"}, {"Node Type": "Bitmap Index Scan", "Parent Relationship": "Member", "Index Name": "b"}]}]}]}`, true},

		{"a hash join", `{"Node Type": "Aggregate", "Plans": [{"Node Type": "Hash Join", "Parent Relationship": "Outer", "Plans": [{"Node Type": "Seq Scan", "Parent Relationship": "Outer", "Relation Name": "a"}, {"Node Type": "Hash", "Parent Relationship": "Inner", "Plans": [{"Node Type": "Seq Scan", "Parent Relationship": "Outer", "Relation Name": "b"}]}]}]}`, false},
		{"a nested loop", `{"Node Type": "Nested Loop", "Plans": [{"Node Type": "Seq Scan", "Parent Relationship": "Outer", "Relation Name": "a"}, {"Node Type": "Index Scan", "Parent Relationship": "Inner", "Relation Name": "b"}]}`, false},
		{"a merge join", `{"Node Type": "Merge Join", "Plans": [{"Node Type": "Index Scan", "Parent Relationship": "Outer", "Relation Name": "a"}, {"Node Type": "Index Scan", "Parent Relationship": "Inner", "Relation Name": "b"}]}`, false},
		{"a self-join reads the table twice", `{"Node Type": "Nested Loop", "Plans": [{"Node Type": "Seq Scan", "Parent Relationship": "Outer", "Relation Name": "t"}, {"Node Type": "Seq Scan", "Parent Relationship": "Inner", "Relation Name": "t"}]}`, false},
		// WHERE x IN (SELECT ...) or a correlated subquery: no Join node,
		// another table read under the scan.
		{"a subquery in the filter (SubPlan)", `{"Node Type": "Seq Scan", "Relation Name": "a", "Filter": "(hashed SubPlan 1)", "Plans": [{"Node Type": "Seq Scan", "Parent Relationship": "SubPlan", "Subplan Name": "SubPlan 1", "Relation Name": "b"}]}`, false},
		{"a subquery of the same table (SubPlan)", `{"Node Type": "Seq Scan", "Relation Name": "a", "Plans": [{"Node Type": "Seq Scan", "Parent Relationship": "SubPlan", "Subplan Name": "SubPlan 1", "Relation Name": "a"}]}`, false},
		{"a scalar subquery computed once (InitPlan)", `{"Node Type": "Seq Scan", "Relation Name": "a", "Plans": [{"Node Type": "Aggregate", "Parent Relationship": "InitPlan", "Subplan Name": "InitPlan 1 (returns $0)", "Plans": [{"Node Type": "Seq Scan", "Parent Relationship": "Outer", "Relation Name": "b"}]}]}`, false},
		{"an InitPlan under a Result", `{"Node Type": "Result", "Plans": [{"Node Type": "Limit", "Parent Relationship": "InitPlan", "Subplan Name": "InitPlan 1", "Plans": [{"Node Type": "Index Only Scan", "Parent Relationship": "Outer", "Relation Name": "t"}]}]}`, false},
		{"a WITH read as a CTE Scan", `{"Node Type": "CTE Scan", "CTE Name": "w", "Plans": [{"Node Type": "Seq Scan", "Parent Relationship": "InitPlan", "Subplan Name": "CTE w", "Relation Name": "t"}]}`, false},
		{"a WITH that names no InitPlan", `{"Node Type": "Aggregate", "Plans": [{"Node Type": "CTE Scan", "Parent Relationship": "Outer", "CTE Name": "w"}]}`, false},
		{"a partitioned table (Append)", `{"Node Type": "Aggregate", "Plans": [{"Node Type": "Append", "Parent Relationship": "Outer", "Plans": [{"Node Type": "Seq Scan", "Parent Relationship": "Member", "Relation Name": "t_2025"}, {"Node Type": "Seq Scan", "Parent Relationship": "Member", "Relation Name": "t_2026"}]}]}`, false},
		{"a partitioned table pruned to one partition (Append of one)", `{"Node Type": "Append", "Plans": [{"Node Type": "Seq Scan", "Parent Relationship": "Member", "Relation Name": "t_2026"}]}`, false},
		{"a sorted UNION (Merge Append)", `{"Node Type": "Merge Append", "Plans": [{"Node Type": "Index Scan", "Parent Relationship": "Member", "Relation Name": "a"}, {"Node Type": "Index Scan", "Parent Relationship": "Member", "Relation Name": "b"}]}`, false},
		{"a subquery in FROM (Subquery Scan)", `{"Node Type": "Subquery Scan", "Plans": [{"Node Type": "Seq Scan", "Parent Relationship": "Subquery", "Relation Name": "t"}]}`, false},
		{"a function scan", `{"Node Type": "Function Scan", "Function Name": "generate_series"}`, false},
		{"a set-returning function over a table (ProjectSet)", `{"Node Type": "ProjectSet", "Plans": [{"Node Type": "Seq Scan", "Parent Relationship": "Outer", "Relation Name": "t"}]}`, false},
		{"no table at all", `{"Node Type": "Result"}`, false},
		{"a node this does not know", `{"Node Type": "Custom Scan", "Plans": [{"Node Type": "Seq Scan", "Parent Relationship": "Outer", "Relation Name": "t"}]}`, false},
		{"a materialized scan", `{"Node Type": "Materialize", "Plans": [{"Node Type": "Seq Scan", "Parent Relationship": "Outer", "Relation Name": "t"}]}`, false},
		{"an aggregate over two children", `{"Node Type": "Aggregate", "Plans": [{"Node Type": "Seq Scan", "Parent Relationship": "Outer", "Relation Name": "a"}, {"Node Type": "Seq Scan", "Parent Relationship": "Outer", "Relation Name": "b"}]}`, false},
		{"a scan with a child that is not a subplan", `{"Node Type": "Seq Scan", "Relation Name": "a", "Plans": [{"Node Type": "Seq Scan", "Parent Relationship": "Outer", "Relation Name": "b"}]}`, false},
		{"a bitmap index scan outside a bitmap heap scan", `{"Node Type": "Bitmap Index Scan", "Index Name": "a"}`, false},
		{"a bitmap heap scan over a scan of a table", `{"Node Type": "Bitmap Heap Scan", "Relation Name": "t", "Plans": [{"Node Type": "Seq Scan", "Parent Relationship": "Outer", "Relation Name": "b"}]}`, false},
		{"Plans not a list", `{"Node Type": "Aggregate", "Plans": "x"}`, false},
		{"a child not an object", `{"Node Type": "Aggregate", "Plans": [1]}`, false},
		{"no node type", `{"Plans": [{"Node Type": "Seq Scan", "Relation Name": "t"}]}`, false},
	} {
		var top map[string]any
		if err := json.Unmarshal([]byte(tc.plan), &top); err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if got := readsOneTable(top); got != tc.want {
			t.Errorf("%s: readsOneTable = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// A plan nested past what PostgreSQL writes is not short.
func TestReadsOneTable_tooDeep(t *testing.T) {
	plan := `{"Node Type": "Seq Scan", "Relation Name": "t"}`
	for range maxPlanDepth + 1 {
		plan = `{"Node Type": "Limit", "Plans": [` + plan + `]}`
	}
	var top map[string]any
	if err := json.Unmarshal([]byte(plan), &top); err != nil {
		t.Fatal(err)
	}
	if readsOneTable(top) {
		t.Error("a plan nested too deep was read as one table")
	}
}

// parsePGPlan carries the verdict, read from the same EXPLAIN the route was
// decided by.
func TestParsePGPlan_oneTable(t *testing.T) {
	one, err := parsePGPlan([]byte(fastLanePlans["FROM lone"]))
	if err != nil || !one.oneTable {
		t.Errorf("one table: %+v %v", one, err)
	}
	joined, err := parsePGPlan([]byte(fastLanePlans["FROM joined"]))
	if err != nil || joined.oneTable {
		t.Errorf("a join: %+v %v", joined, err)
	}
}

// fastLanePlans are the source's plans of the lane tests: both expensive
// enough for the copy, one reading a table alone, one joining two.
var fastLanePlans = map[string]string{
	"WHERE id = ": plan(`{"Node Type": "Index Scan", "Relation Name": "t", "Total Cost": 8.45, "Plan Rows": 1}`),
	"FROM lone":   plan(`{"Node Type": "Aggregate", "Total Cost": 506064, "Plan Rows": 1, "Plans": [{"Node Type": "Seq Scan", "Parent Relationship": "Outer", "Relation Name": "lone", "Total Cost": 400000, "Plan Rows": 30000000}]}`),
	"FROM joined": plan(`{"Node Type": "Aggregate", "Total Cost": 1046199, "Plan Rows": 1, "Plans": [{"Node Type": "Hash Join", "Parent Relationship": "Outer", "Total Cost": 900000, "Plans": [{"Node Type": "Seq Scan", "Parent Relationship": "Outer", "Relation Name": "joined", "Total Cost": 400000, "Plan Rows": 30000000}, {"Node Type": "Hash", "Parent Relationship": "Inner", "Plans": [{"Node Type": "Seq Scan", "Parent Relationship": "Outer", "Relation Name": "other", "Plan Rows": 3000000}]}]}]}`),
	"FROM sub":    plan(`{"Node Type": "Aggregate", "Total Cost": 506064, "Plan Rows": 1, "Plans": [{"Node Type": "Seq Scan", "Parent Relationship": "Outer", "Relation Name": "sub", "Plan Rows": 30000000, "Plans": [{"Node Type": "Seq Scan", "Parent Relationship": "SubPlan", "Subplan Name": "SubPlan 1", "Relation Name": "other", "Plan Rows": 10}]}]}`),
}

// lanes is what the copy was told of each statement it ran, in order.
func (l *costLogin) lanes() []bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]bool(nil), l.fastLane...)
}

// The simple protocol: a statement the plan sends to the copy is marked for
// its lane when the plan reads one table alone, and only then; a statement
// after it is not marked by what came before.
func TestFastLane_simple(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	l := &costLogin{src: startFakeSource(t, fastLanePlans)}
	c := costPortConn(t, ctx, l)
	for _, tc := range []struct {
		name, stmt string
		side       string
		lane       bool
	}{
		{"one table", "SELECT sum(n) FROM lone", RouteCopy, true},
		{"a join, right after", "SELECT sum(n) FROM joined", RouteCopy, false},
		{"one table again", "SELECT count(*) FROM lone", RouteCopy, true},
		{"a subquery of another table", "SELECT count(*) FROM sub", RouteCopy, false},
		{"one table, a third time", "SELECT max(n) FROM lone", RouteCopy, true},
	} {
		before := len(l.lanes())
		got := costSimple(ctx, c, l, tc.stmt)
		if got.side != tc.side {
			t.Fatalf("%s: %+v, want the %s", tc.name, got, tc.side)
		}
		lanes := l.lanes()[before:]
		if len(lanes) != 1 || lanes[0] != tc.lane {
			t.Errorf("%s: the copy was told %v, want [%v]", tc.name, lanes, tc.lane)
		}
	}
	// The source answers a read by key: the copy runs nothing, and the next
	// statement for the copy is not marked by it.
	before := len(l.lanes())
	if got := costSimple(ctx, c, l, "SELECT n FROM t WHERE id = 1"); got.side != RouteSource {
		t.Fatalf("a read by key: %+v", got)
	}
	if n := len(l.lanes()) - before; n != 0 {
		t.Errorf("the copy ran %d statements for a read the source answers", n)
	}
	if got := costSimple(ctx, c, l, "SELECT sum(n) FROM joined"); got.side != RouteCopy || l.lanes()[len(l.lanes())-1] {
		t.Errorf("a join after a read by key was marked for the lane: %+v", got)
	}
}

// With routing by plan off there is no plan: nothing is marked, and every
// statement takes its turn as before.
func TestFastLane_noPlanNoLane(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	l := &costLogin{src: startFakeSource(t, fastLanePlans), cost: &CostPolicy{}}
	c := costPortConn(t, ctx, l)
	if got := costSimple(ctx, c, l, "SELECT sum(n) FROM lone"); got.side != RouteCopy {
		t.Fatalf("%+v", got)
	}
	if lanes := l.lanes(); len(lanes) != 1 || lanes[0] {
		t.Errorf("routing by plan off: the copy was told %v, want [false]", lanes)
	}
	// No source at all: no plan either.
	l2 := &costLogin{}
	c2 := costPortConn(t, ctx, l2)
	if got := costSimple(ctx, c2, l2, "SELECT sum(n) FROM lone"); got.side != RouteCopy {
		t.Fatalf("no source: %+v", got)
	}
	if lanes := l2.lanes(); len(lanes) != 1 || lanes[0] {
		t.Errorf("no source: the copy was told %v, want [false]", lanes)
	}
}

// The extended protocol: the plan asked once for a prepared statement marks
// its describe and every execution, with parameters and without, and a
// statement that joins is not marked.
func TestFastLane_extended(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	// The copy's columns, so the screen can read $1 against n.
	l := &costLogin{src: startFakeSource(t, fastLanePlans), catalog: func() []CatalogTable { return catalogOf("lone", "joined", "other") }}
	c := costPortConn(t, ctx, l)
	run := func(name, stmt string, args [][]byte) []bool {
		t.Helper()
		before := len(l.lanes())
		if _, err := c.Prepare(ctx, name, stmt, nil); err != nil {
			t.Fatalf("prepare %s: %v", stmt, err)
		}
		for range 2 {
			if res := c.ExecPrepared(ctx, name, args, nil, nil).Read(); res.Err != nil {
				t.Fatalf("%s: %v", stmt, res.Err)
			}
		}
		if n := l.last(); n.Side != RouteCopy {
			t.Fatalf("%s: answered by the %s (%s)", stmt, n.Side, n.Reason)
		}
		return l.lanes()[before:]
	}
	allAre := func(what string, got []bool, want bool) {
		t.Helper()
		if len(got) == 0 {
			t.Errorf("%s: the copy ran nothing", what)
		}
		for _, g := range got {
			if g != want {
				t.Errorf("%s: the copy was told %v, want every one %v", what, got, want)
				return
			}
		}
	}
	allAre("prepared, one table", run("lone", "SELECT sum(n) FROM lone", nil), true)
	allAre("prepared, a join", run("joined", "SELECT sum(n) FROM joined", nil), false)
	allAre("prepared with a parameter, one table", run("loneArg", "SELECT sum(n) FROM lone WHERE n > $1", [][]byte{[]byte("0")}), true)
	allAre("prepared with a parameter, a join", run("joinedArg", "SELECT sum(n) FROM joined WHERE n > $1", [][]byte{[]byte("0")}), false)
	// And on the unnamed statement, as pgx's ExecParams sends it.
	before := len(l.lanes())
	if res := c.ExecParams(ctx, "SELECT sum(n) FROM lone WHERE n > $1", [][]byte{[]byte("0")}, nil, nil, nil).Read(); res.Err != nil {
		t.Fatal(res.Err)
	}
	allAre("unnamed, one table", l.lanes()[before:], true)
	before = len(l.lanes())
	if res := c.ExecParams(ctx, "SELECT sum(n) FROM joined WHERE n > $1", [][]byte{[]byte("0")}, nil, nil, nil).Read(); res.Err != nil {
		t.Fatal(res.Err)
	}
	allAre("unnamed, a join", l.lanes()[before:], false)
}
