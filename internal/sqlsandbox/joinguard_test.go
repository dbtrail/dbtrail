package sqlsandbox

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// The CH-benCHmark's 22 reports, planned by DuckDB 1.5.6 over a 200-warehouse
// copy, in its own join order and in the order written, with the source's
// n_distinct for every column (read with the PostgreSQL port's statistics
// query). Only the fields the check reads are kept of each plan node.
type chBenchmarkPlans struct {
	NDV     map[string]float64 `json:"ndv"`
	Queries map[string]struct {
		SQL          string          `json:"sql"`
		Plan         json.RawMessage `json:"plan"`
		WrittenOrder json.RawMessage `json:"plan_written_order"`
	} `json:"queries"`
}

func loadCHBenchmarkPlans(t *testing.T) chBenchmarkPlans {
	t.Helper()
	raw, err := os.ReadFile("testdata/joinguard_chbenchmark.json")
	if err != nil {
		t.Fatal(err)
	}
	var p chBenchmarkPlans
	if err := json.Unmarshal(raw, &p); err != nil {
		t.Fatal(err)
	}
	if len(p.Queries) != 22 || len(p.NDV) == 0 {
		t.Fatalf("%d queries, %d columns with statistics", len(p.Queries), len(p.NDV))
	}
	return p
}

// On the 22 reports, the check flags exactly the three whose DuckDB order
// joins two large inputs on a column with few values (Q5 joins stock with the
// filtered orders on the warehouse alone, 20M x 240k over 200 warehouses);
// in the order written Q5 and Q8 are not flagged, Q21 still is (a cross
// product of order_line and supplier, 60M x 10k). The written order flags
// three reports DuckDB orders well (Q3, Q17, Q20): why the order written is
// tried only for a statement whose own plan is flagged.
func TestPlanJoinDanger_CHBenchmark(t *testing.T) {
	p := loadCHBenchmarkPlans(t)
	var planner, written []string
	decision := map[string]string{}
	for name, q := range p.Queries {
		d, err := planJoinDanger(q.Plan, p.NDV)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		w, err := planJoinDanger(q.WrittenOrder, p.NDV)
		if err != nil {
			t.Fatalf("%s, in the order written: %v", name, err)
		}
		if d != "" {
			planner = append(planner, name)
		}
		if w != "" {
			written = append(written, name)
		}
		switch {
		case d == "":
			decision[name] = "as planned"
		case w == "":
			decision[name] = "written order"
		default:
			decision[name] = "refused"
		}
	}
	slices.Sort(planner)
	slices.Sort(written)
	if want := []string{"Q21", "Q5", "Q8"}; !slices.Equal(planner, want) {
		t.Errorf("flagged in DuckDB's order: %v, want %v", planner, want)
	}
	if want := []string{"Q17", "Q20", "Q21", "Q3"}; !slices.Equal(written, want) {
		t.Errorf("flagged in the order written: %v, want %v", written, want)
	}
	for name, got := range decision {
		want := "as planned"
		switch name {
		case "Q5", "Q8":
			want = "written order"
		case "Q21":
			want = "refused"
		}
		if got != want {
			t.Errorf("%s: %s, want %s", name, got, want)
		}
	}
}

// What the check says names the join and its numbers, for the log and the
// client's error.
func TestPlanJoinDanger_saysWhichJoin(t *testing.T) {
	p := loadCHBenchmarkPlans(t)
	got, err := planJoinDanger(p.Queries["Q5"].Plan, p.NDV)
	if err != nil {
		t.Fatal(err)
	}
	for _, part := range []string{"HASH_JOIN", "s_w_id = c_w_id", "20000000", "240698"} {
		if !strings.Contains(got, part) {
			t.Errorf("%q does not say %q", got, part)
		}
	}
	got, _ = planJoinDanger(p.Queries["Q21"].WrittenOrder, p.NDV)
	if !strings.Contains(got, "CROSS_PRODUCT") || !strings.Contains(got, "60162442") {
		t.Errorf("Q21 in the order written: %q", got)
	}
}

// A join node as DuckDB's EXPLAIN (FORMAT JSON) writes one, with two children
// of the estimates given; conds is the Conditions value as is (a string, a
// list, or nil for none).
func joinNode(name, joinType string, conds any, left, right any) map[string]any {
	extra := map[string]any{}
	if joinType != "" {
		extra["Join Type"] = joinType
	}
	if conds != nil {
		extra["Conditions"] = conds
	}
	return map[string]any{"name": name, "extra_info": extra, "children": []any{scanNode(left), scanNode(right)}}
}

// scanNode is a leaf whose Estimated Cardinality is est as is (a string, a
// number, or nil for none).
func scanNode(est any) map[string]any {
	extra := map[string]any{}
	if est != nil {
		extra["Estimated Cardinality"] = est
	}
	return map[string]any{"name": "READ_PARQUET", "extra_info": extra, "children": []any{}}
}

func planOf(t *testing.T, roots ...any) []byte {
	t.Helper()
	raw, err := json.Marshal(roots)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestPlanJoinDanger_edges(t *testing.T) {
	ndv := map[string]float64{
		"w_id": 200, "o_w_id": 200, "s_w_id": 200, "c_w_id": 200,
		"i_id": 100000, "s_i_id": 100000, "id": 20000000, "o_id": 3000,
		"mixed_case": 200, "quoted col": 200, "amount": 1,
	}
	big, mid := "20000000", "240698"
	for _, tc := range []struct {
		name string
		plan []byte
		want bool // flagged
	}{
		{"one equality on a column with few values", planOf(t, joinNode("HASH_JOIN", "INNER", "s_w_id = c_w_id", big, mid)), true},
		{"the same as a list", planOf(t, joinNode("HASH_JOIN", "INNER", []any{"s_w_id = c_w_id"}, big, mid)), true},
		{"a selective key", planOf(t, joinNode("HASH_JOIN", "INNER", "s_i_id = i_id", big, mid)), false},
		// Every amount is 1, joined to a key: each row meets one, whatever
		// few values the column has.
		{"a column of one value joined to a key", planOf(t, joinNode("HASH_JOIN", "INNER", "amount = id", "20000", "20000")), false},
		{"a column of few values joined to a column of few values", planOf(t, joinNode("HASH_JOIN", "INNER", "amount = s_w_id", "50000", "50000")), true},
		{"two equalities whose values multiply past the larger side", planOf(t, joinNode("HASH_JOIN", "INNER", []any{"s_w_id = c_w_id", "s_i_id = i_id"}, big, mid)), false},
		{"no equality, only a range", planOf(t, joinNode("NESTED_LOOP_JOIN", "INNER", "s_w_id < c_w_id", big, mid)), false},
		{"an equality of an expression: the copy's MOD", planOf(t, joinNode("HASH_JOIN", "INNER", "mod((s_w_id * s_i_id), 10000) = c_w_id", big, mid)), false},
		{"an equality of an expression: ascii(substring)", planOf(t, joinNode("HASH_JOIN", "INNER", `ascii("substring"(c_state, 1, 1)) = s_w_id`, big, mid)), false},
		{"an expression beside a plain equality: the plain one counts", planOf(t, joinNode("HASH_JOIN", "INNER", []any{"mod((s_w_id * s_i_id), 10000) = i_id", "s_w_id = c_w_id"}, big, mid)), true},
		{"a column the source has no statistics for", planOf(t, joinNode("HASH_JOIN", "INNER", "s_w_id = nope", big, mid)), false},
		{"both columns unknown", planOf(t, joinNode("HASH_JOIN", "INNER", "a = b", big, mid)), false},
		{"one unknown pair beside a dangerous one: taken as very selective", planOf(t, joinNode("HASH_JOIN", "INNER", []any{"s_w_id = c_w_id", "x = y"}, big, mid)), false},
		{"a tiny side", planOf(t, joinNode("HASH_JOIN", "INNER", "s_w_id = c_w_id", big, "1")), false},
		{"a zero estimate", planOf(t, joinNode("HASH_JOIN", "INNER", "s_w_id = c_w_id", big, "0")), false},
		{"no estimate", planOf(t, joinNode("HASH_JOIN", "INNER", "s_w_id = c_w_id", big, nil)), false},
		{"an estimate that is not a number", planOf(t, joinNode("HASH_JOIN", "INNER", "s_w_id = c_w_id", big, "lots")), false},
		{"estimates as numbers", planOf(t, joinNode("HASH_JOIN", "INNER", "s_w_id = c_w_id", 20000000, 240698)), true},
		{"estimates with separators", planOf(t, joinNode("HASH_JOIN", "INNER", "s_w_id = c_w_id", "~20,000,000", "240,698")), true},
		{"qualified and quoted names", planOf(t, joinNode("HASH_JOIN", "INNER", `"stock"."S_W_ID" = public.c_w_id`, big, mid)), true},
		{"a quoted name with a blank", planOf(t, joinNode("HASH_JOIN", "INNER", `"quoted col" = Mixed_Case`, big, mid)), true},
		{"a self join on its own key", planOf(t, joinNode("HASH_JOIN", "INNER", "id = id", big, big)), false},
		{"a left join counts as an inner one", planOf(t, joinNode("HASH_JOIN", "LEFT", "s_w_id = c_w_id", big, mid)), true},
		{"a semi join never gives more rows than it probes", planOf(t, joinNode("HASH_JOIN", "SEMI", "s_w_id = c_w_id", big, mid)), false},
		{"an anti join", planOf(t, joinNode("HASH_JOIN", "ANTI", "s_w_id = c_w_id", big, mid)), false},
		{"a right semi join", planOf(t, joinNode("HASH_JOIN", "RIGHT_SEMI", "s_w_id = c_w_id", big, mid)), false},
		{"a right anti join", planOf(t, joinNode("HASH_JOIN", "RIGHT_ANTI", "s_w_id = c_w_id", big, mid)), false},
		{"a mark join", planOf(t, joinNode("HASH_JOIN", "MARK", "s_w_id = c_w_id", big, mid)), false},
		{"a single join", planOf(t, joinNode("HASH_JOIN", "SINGLE", "s_w_id = c_w_id", big, mid)), false},
		{"an as-of join gives at most a row per row probed", planOf(t, joinNode("ASOF_JOIN", "INNER", []any{"s_w_id = c_w_id", "a >= b"}, big, mid)), false},
		{"a cross product past both bounds", planOf(t, joinNode("CROSS_PRODUCT", "", nil, "60162442", "10000")), true},
		{"a cross product of a billion rows: under the floor", planOf(t, joinNode("CROSS_PRODUCT", "", nil, "100000", "10000")), false},
		{"a cross product with a one-row side", planOf(t, joinNode("CROSS_PRODUCT", "", nil, "99999999999", "1")), false},
		{"a join with no condition that is not a cross product", planOf(t, joinNode("NESTED_LOOP_JOIN", "INNER", nil, big, mid)), false},
		{"an empty plan", []byte("[]"), false},
		{"no join at all", planOf(t, map[string]any{"name": "PROJECTION", "children": []any{scanNode(big)}}), false},
	} {
		got, err := planJoinDanger(tc.plan, ndv)
		if err != nil {
			t.Errorf("%s: %v", tc.name, err)
			continue
		}
		if (got != "") != tc.want {
			t.Errorf("%s: %q, want flagged %v", tc.name, got, tc.want)
		}
	}
}

// A dangerous join found deep in the tree: under projections and an
// aggregate, as the right child of a harmless join, and in the second of two
// roots.
func TestPlanJoinDanger_nestedChildren(t *testing.T) {
	ndv := map[string]float64{"s_w_id": 200, "c_w_id": 200, "a": 1e6, "b": 1e6}
	inner := joinNode("HASH_JOIN", "INNER", "s_w_id = c_w_id", "20000000", "240698")
	outer := map[string]any{"name": "HASH_JOIN", "extra_info": map[string]any{"Join Type": "INNER", "Conditions": "a = b"},
		"children": []any{scanNode("1000"), inner}}
	wrapped := map[string]any{"name": "ORDER_BY", "children": []any{
		map[string]any{"name": "HASH_GROUP_BY", "extra_info": map[string]any{"Estimated Cardinality": "10"}, "children": []any{
			map[string]any{"name": "PROJECTION", "children": []any{outer}},
		}},
	}}
	if got, err := planJoinDanger(planOf(t, wrapped), ndv); err != nil || got == "" {
		t.Errorf("nested: %q %v", got, err)
	}
	if got, err := planJoinDanger(planOf(t, scanNode("5"), wrapped), ndv); err != nil || got == "" {
		t.Errorf("in the second root: %q %v", got, err)
	}
	// The outer join's right child is the inner join: its estimate is the
	// child's own, not the scans under it.
	harmless := map[string]any{"name": "HASH_JOIN", "extra_info": map[string]any{"Join Type": "INNER", "Conditions": "s_w_id = c_w_id"},
		"children": []any{scanNode("1000"), map[string]any{"name": "PROJECTION", "extra_info": map[string]any{"Estimated Cardinality": "1"},
			"children": []any{scanNode("99999999")}}}}
	if got, err := planJoinDanger(planOf(t, harmless), ndv); err != nil || got != "" {
		t.Errorf("the children's own estimates: %q %v", got, err)
	}
	// A delim join has three children: not judged, but what is under it is.
	delim := map[string]any{"name": "LEFT_DELIM_JOIN", "extra_info": map[string]any{"Join Type": "INNER", "Conditions": "s_w_id = c_w_id"},
		"children": []any{scanNode("20000000"), inner, scanNode("1")}}
	if got, err := planJoinDanger(planOf(t, delim), ndv); err != nil || !strings.Contains(got, "240698") {
		t.Errorf("under a delim join: %q %v", got, err)
	}
	three := map[string]any{"name": "LEFT_DELIM_JOIN", "extra_info": map[string]any{"Join Type": "INNER", "Conditions": "s_w_id = c_w_id"},
		"children": []any{scanNode("20000000"), scanNode("240698"), scanNode("1")}}
	if got, err := planJoinDanger(planOf(t, three), ndv); err != nil || got != "" {
		t.Errorf("a join of three children: %q %v", got, err)
	}
}

// tailView is a view of the copy as fresh mode reads it, shaped as DuckDB
// plans one: the table's file anti-joined with its dead rows, in a UNION with
// the newer rows, anti-joined with the tail, in a UNION with the tail's own
// files. DuckDB takes each anti join to keep a fifth of its input, so its
// estimate at the top (duckTop) is far below base, the file's rows.
func tailView(base float64, duckTop string) map[string]any {
	est := func(n float64) string { return strconv.FormatFloat(n, 'f', 0, 64) }
	node := func(name, joinType, est string, children ...any) map[string]any {
		extra := map[string]any{}
		if joinType != "" {
			extra["Join Type"] = joinType
		}
		if est != "" {
			extra["Estimated Cardinality"] = est
		}
		if children == nil {
			children = []any{}
		}
		return map[string]any{"name": name, "extra_info": extra, "children": children}
	}
	deadRows := node("HASH_JOIN", "ANTI", est(base/5), node("READ_PARQUET", "", est(base)), node("FILTER", "", est(base/50), node("READ_PARQUET", "", "0")))
	snapshot := node("UNION", "", "", deadRows, node("PROJECTION", "", est(base/200), node("READ_PARQUET", "", est(base/200))))
	tail := node("HASH_JOIN", "ANTI", est(base/25), snapshot, node("READ_PARQUET", "", est(base/20)))
	newest := node("HASH_JOIN", "RIGHT_ANTI", "50", node("READ_PARQUET", "", "200"), node("READ_PARQUET", "", "40"))
	return node("PROJECTION", "", duckTop, node("UNION", "", "", tail, newest, node("READ_PARQUET", "", "10")))
}

// Over views with their recent changes, DuckDB's estimates fall to a
// twenty-fifth of the tables (stock, 20 million rows, at 844,417 in the
// CH-benCHmark's Q5 with tails): the join of two such views is only seen
// with the estimates raised back to the tables'.
func TestPlanJoinDanger_tailViewsAreTakenAtTheirTablesRows(t *testing.T) {
	ndv := map[string]float64{"s_w_id": 200, "c_w_id": 200, "s_d_id": 10, "c_d_id": 10}
	stock, customer := tailView(20000000, "800000"), tailView(6000000, "150000")
	join := map[string]any{"name": "HASH_JOIN", "extra_info": map[string]any{"Join Type": "INNER", "Conditions": []any{"s_w_id = c_w_id", "s_d_id = c_d_id"}},
		"children": []any{stock, customer}}
	got, err := planJoinDanger(planOf(t, join), ndv)
	if err != nil {
		t.Fatal(err)
	}
	// By DuckDB's own estimates 800,000 x 150,000 over 2,000 values is under
	// 100 times 800,000; by the tables' about 6e10, far past 100 times 20
	// million.
	if got == "" || !strings.Contains(got, "20100210 x 6030210") {
		t.Fatalf("not flagged: %q", got)
	}
	var roots []planNode
	if err := json.Unmarshal(planOf(t, stock), &roots); err != nil {
		t.Fatal(err)
	}
	if rows := correctRows(&roots[0]); rows < 20000000 || rows > 20200000 {
		t.Errorf("the stock view: %.0f rows, want about its table's 20 million", rows)
	}
	// A filter over such a view keeps its selectivity.
	filtered := map[string]any{"name": "FILTER", "extra_info": map[string]any{"Estimated Cardinality": "80000"}, "children": []any{stock}}
	if err := json.Unmarshal(planOf(t, filtered), &roots); err != nil {
		t.Fatal(err)
	}
	if rows := correctRows(&roots[0]); rows < 2000000 || rows > 2020000 {
		t.Errorf("a tenth of the stock view: %.0f rows", rows)
	}
	// Without anti joins nothing is raised.
	plain := planOf(t, map[string]any{"name": "PROJECTION", "extra_info": map[string]any{"Estimated Cardinality": "5"}, "children": []any{scanNode("5")}})
	if err := json.Unmarshal(plain, &roots); err != nil {
		t.Fatal(err)
	}
	if rows := correctRows(&roots[0]); rows != 5 {
		t.Errorf("a plain projection: %.0f", rows)
	}
}

// Nothing to compare against: no statistics, no check.
func TestPlanJoinDanger_noStatistics(t *testing.T) {
	plan := planOf(t, joinNode("CROSS_PRODUCT", "", nil, "60162442", "10000"))
	for _, ndv := range []map[string]float64{nil, {}} {
		if got, err := planJoinDanger(plan, ndv); err != nil || got != "" {
			t.Errorf("ndv %v: %q %v", ndv, got, err)
		}
	}
}

func TestPlanJoinDanger_unreadablePlan(t *testing.T) {
	for _, raw := range []string{"", "not json", `{"name": "HASH_JOIN"}`, `[{"name": 7}]`} {
		if _, err := planJoinDanger([]byte(raw), map[string]float64{"a": 1}); err == nil {
			t.Errorf("%q: no error", raw)
		}
	}
}

// explodingCross is a cross product of 1e5 x 1e6 rows whose two sides are
// projections, as the copy's views are: DuckDB estimates a projection in the
// order written too, where a bare table function's estimate is 0 (its
// estimator is part of the join-order optimizer turned off).
const explodingCross = "SELECT count(*) AS n FROM (SELECT range * 2 AS x FROM range(100000)) a, (SELECT range * 2 AS y FROM range(1000000)) b"

// The check in the worker, through DuckDB's own EXPLAIN: a cross product
// estimated past both bounds in either order is refused before it runs.
func TestWorker_joinCheckRefusesWhatExplodesInBothOrders(t *testing.T) {
	f := newCopyFixture(t)
	res := runJob(wireJob{
		CopyDirs: []string{f.archiveRoot}, SQL: explodingCross,
		Threads: 1, MemoryLimit: "256MB", MaxRows: 10, MaxResultBytes: 1 << 20, JoinNDV: map[string]float64{"range": 1},
	}, nil, os.Stderr)
	if res.Error == nil || res.Error.Kind != errJoinTooBig {
		t.Fatalf("error = %+v, want kind %s", res.Error, errJoinTooBig)
	}
	if !strings.Contains(res.Error.Message, "CROSS_PRODUCT") {
		t.Errorf("the refusal does not name the join: %q", res.Error.Message)
	}
	// An equality join, the case the check is for, through DuckDB's own
	// Conditions and estimates: 1e6 x 1e5 rows on a column of two values.
	res = runJob(wireJob{
		CopyDirs: []string{f.archiveRoot}, SQL: "SELECT count(*) AS n FROM (SELECT range % 2 AS x FROM range(1000000)) a JOIN (SELECT range % 2 AS y FROM range(100000)) b ON a.x = b.y",
		Threads: 1, MemoryLimit: "256MB", MaxRows: 10, MaxResultBytes: 1 << 20, JoinNDV: map[string]float64{"x": 2, "y": 2},
	}, nil, os.Stderr)
	if res.Error == nil || res.Error.Kind != errJoinTooBig || !strings.Contains(res.Error.Message, "x = y") {
		t.Fatalf("an equality join: %+v", res.Error)
	}
	// Without statistics the same statement runs as it always did.
	res = runJob(wireJob{
		CopyDirs: []string{f.archiveRoot}, SQL: "SELECT count(*) AS n FROM range(1000) a, range(1000) b",
		Threads: 1, MemoryLimit: "256MB", MaxRows: 10, MaxResultBytes: 1 << 20,
	}, nil, os.Stderr)
	if res.Error != nil || len(res.Rows) != 1 || res.JoinOrder != "" {
		t.Errorf("without statistics: %+v %v %q", res.Error, res.Rows, res.JoinOrder)
	}
}

// fakeJoinDanger replaces the plan's verdict for one test: verdicts[i] is
// the i-th plan's (the planner's, then the order written's), err every call's.
func fakeJoinDanger(t *testing.T, err error, verdicts ...string) *[][]byte {
	t.Helper()
	var plans [][]byte
	was := joinDanger
	t.Cleanup(func() { joinDanger = was })
	joinDanger = func(plan []byte, _ map[string]float64) (string, error) {
		plans = append(plans, plan)
		if err != nil {
			return "", err
		}
		if len(plans) > len(verdicts) {
			t.Errorf("plan %d asked for, %d expected", len(plans), len(verdicts))
			return "", nil
		}
		return verdicts[len(plans)-1], nil
	}
	return &plans
}

func TestWorker_joinCheckOrders(t *testing.T) {
	f := newCopyFixture(t)
	job := func() wireJob {
		return wireJob{CopyDirs: []string{f.archiveRoot}, SQL: "SELECT count(*) AS n FROM range(10) a JOIN range(10) b ON a.range = b.range",
			Threads: 1, MemoryLimit: "256MB", MaxRows: 10, MaxResultBytes: 1 << 20, JoinNDV: map[string]float64{"range": 10}}
	}
	t.Run("a plan the check passes runs as planned", func(t *testing.T) {
		plans := fakeJoinDanger(t, nil, "")
		res := runJob(job(), nil, os.Stderr)
		if res.Error != nil || res.JoinOrder != "" || len(*plans) != 1 {
			t.Fatalf("%+v %q, %d plans", res.Error, res.JoinOrder, len(*plans))
		}
		if !strings.Contains(string((*plans)[0]), "HASH_JOIN") {
			t.Errorf("the plan given to the check: %s", (*plans)[0])
		}
	})
	t.Run("a plan flagged whose order written passes runs in that order", func(t *testing.T) {
		plans := fakeJoinDanger(t, nil, "HASH_JOIN on a = b", "")
		res := runJob(job(), nil, os.Stderr)
		if res.Error != nil || res.JoinOrder != joinOrderWritten || len(*plans) != 2 {
			t.Fatalf("%+v %q, %d plans", res.Error, res.JoinOrder, len(*plans))
		}
		if len(res.Rows) != 1 || res.Rows[0][0] != int64(10) {
			t.Errorf("rows %v", res.Rows)
		}
		// The second plan was taken with the join order off: DuckDB then
		// estimates the bare table functions at 0, so the two differ.
		if bytes.Equal((*plans)[0], (*plans)[1]) {
			t.Errorf("the plan in the order written is the planner's: %s", (*plans)[1])
		}
	})
	t.Run("both orders flagged: refused, both named", func(t *testing.T) {
		fakeJoinDanger(t, nil, "HASH_JOIN on a = b", "CROSS_PRODUCT of 1 x 2")
		res := runJob(job(), nil, os.Stderr)
		if res.Error == nil || res.Error.Kind != errJoinTooBig || len(res.Rows) != 0 {
			t.Fatalf("%+v rows %v", res.Error, res.Rows)
		}
		for _, part := range []string{"HASH_JOIN on a = b", "CROSS_PRODUCT of 1 x 2"} {
			if !strings.Contains(res.Error.Message, part) {
				t.Errorf("%q does not say %q", res.Error.Message, part)
			}
		}
	})
	t.Run("a plan that cannot be read: the statement runs as before, and says so", func(t *testing.T) {
		fakeJoinDanger(t, errors.New("garbled"))
		res := runJob(job(), nil, os.Stderr)
		if res.Error != nil || res.JoinOrder != "" || !strings.Contains(res.JoinCheckSkipped, "garbled") {
			t.Fatalf("%+v %q %q", res.Error, res.JoinOrder, res.JoinCheckSkipped)
		}
	})
	t.Run("a statement EXPLAIN cannot plan: runs as before, its own error", func(t *testing.T) {
		plans := fakeJoinDanger(t, nil)
		j := job()
		j.SQL = "SELECT nope FROM range(3)"
		res := runJob(j, nil, os.Stderr)
		if res.Error == nil || res.Error.Kind != errQuery || !strings.Contains(res.Error.Message, "nope") || len(*plans) != 0 {
			t.Fatalf("%+v, %d plans", res.Error, len(*plans))
		}
	})
	t.Run("no statistics: no plan asked", func(t *testing.T) {
		plans := fakeJoinDanger(t, nil)
		j := job()
		j.JoinNDV = nil
		if res := runJob(j, nil, os.Stderr); res.Error != nil || len(*plans) != 0 {
			t.Fatalf("%+v, %d plans", res.Error, len(*plans))
		}
	})
	t.Run("the order written already asked for: no check", func(t *testing.T) {
		plans := fakeJoinDanger(t, nil)
		j := job()
		j.KeepJoinOrder = true
		if res := runJob(j, nil, os.Stderr); res.Error != nil || len(*plans) != 0 || res.JoinOrder != "" {
			t.Fatalf("%+v, %d plans %q", res.Error, len(*plans), res.JoinOrder)
		}
	})
}

// The one SET of disabled_optimizers: a SET replaces the whole list, so the
// order written and the PostgreSQL port's common_subplan go in one.
func TestDisabledOptimizersSQL(t *testing.T) {
	for _, tc := range []struct {
		keep, skip bool
		want       string
	}{
		{false, false, ""},
		{true, false, "SET disabled_optimizers = 'join_order'"},
		{false, true, "SET disabled_optimizers = 'common_subplan'"},
		{true, true, "SET disabled_optimizers = 'join_order,common_subplan'"},
	} {
		if got := disabledOptimizersSQL(tc.keep, tc.skip); got != tc.want {
			t.Errorf("keep %v skip %v: %q, want %q", tc.keep, tc.skip, got, tc.want)
		}
	}
}

// What the session's list holds when the statement runs, read through the
// statement itself (current_setting is the copy's own setting read).
func TestWorker_disabledOptimizersAsTheStatementRuns(t *testing.T) {
	f := newCopyFixture(t)
	read := func(j wireJob) string {
		t.Helper()
		j.CopyDirs, j.Threads, j.MemoryLimit, j.MaxRows, j.MaxResultBytes = []string{f.archiveRoot}, 1, "256MB", 10, 1<<20
		res := runJob(j, nil, os.Stderr)
		if res.Error != nil {
			t.Fatalf("%+v", res.Error)
		}
		s, _ := res.Rows[0][0].(string)
		return s
	}
	const q = "SELECT current_setting('disabled_optimizers') AS d FROM range(1)"
	// The MySQL port and the SQL page: nothing turned off, or join_order
	// alone for a STRAIGHT_JOIN, as before.
	if got := read(wireJob{SQL: q}); got != "" {
		t.Errorf("MySQL: %q", got)
	}
	if got := read(wireJob{SQL: q, KeepJoinOrder: true}); got != "join_order" {
		t.Errorf("MySQL STRAIGHT_JOIN: %q", got)
	}
	// The PostgreSQL port.
	if got := read(wireJob{SQL: q, SkipCommonSubplan: true}); got != "common_subplan" {
		t.Errorf("PostgreSQL: %q", got)
	}
	// The join check moving the statement to the order written keeps
	// common_subplan off.
	fakeJoinDanger(t, nil, "HASH_JOIN on a = b", "")
	if got := read(wireJob{SQL: q, SkipCommonSubplan: true, JoinNDV: map[string]float64{"a": 1}}); got != "join_order,common_subplan" {
		t.Errorf("PostgreSQL in the order written: %q", got)
	}
}

// Through the real worker process: the statistics travel to it, and its
// refusal comes back as a JoinTooBigError, not a worker failure.
func TestRunner_joinTooBigIsTyped(t *testing.T) {
	f := newCopyFixture(t)
	r := newTestRunner(t, Limits{})
	job := f.job(explodingCross)
	job.Session.JoinNDV = map[string]float64{"range": 1}
	_, err := r.Run(context.Background(), job)
	var tooBig *JoinTooBigError
	if !errors.As(err, &tooBig) || !strings.Contains(tooBig.Reason, "CROSS_PRODUCT") {
		t.Fatalf("err = %T %v", err, err)
	}
	job = f.job("SELECT count(*) AS n FROM range(1000) a, range(1000) b")
	job.Session.JoinNDV = map[string]float64{"range": 1}
	res, err := r.Run(context.Background(), job)
	if err != nil || len(res.Rows) != 1 || res.JoinOrderWritten {
		t.Fatalf("a small cross product: %v %v %v", err, res.Rows, res.JoinOrderWritten)
	}
}
