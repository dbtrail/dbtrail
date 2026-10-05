package readrouter

import (
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// joinPlan reads one plan of testdata/joins: EXPLAIN FORMAT=JSON captured
// from a real server over the dataset of testdata/joins/setup.sql.
func joinPlan(t *testing.T, server, name string) Plan {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", "joins", server, name+".json"))
	if err != nil {
		t.Fatal(err)
	}
	p, err := ParsePlan(raw)
	if err != nil {
		t.Fatalf("%s/%s: %v", server, name, err)
	}
	return p
}

var mariaServers = []string{"mariadb114", "mariadb1011"}

// The three joins of the issue: on MariaDB no table of theirs shows a scan
// over the threshold, and each reads one to four million rows.
func TestDecideStatement_heavyJoinsOnMariaDB(t *testing.T) {
	for _, tc := range []struct {
		name, stmt string
		rows       map[string]int64 // the estimate, per server
	}{
		{"join_group_by_country", "SELECT c.country_code, count(*), sum(o.amount) FROM orders o JOIN customers c ON c.id = o.customer_id GROUP BY c.country_code",
			map[string]int64{"mariadb114": 99615 + 99615*20, "mariadb1011": 97208 + 97208*10}},
		{"join_items_products_group_by_category", "SELECT p.category_id, count(*), sum(oi.qty * oi.price) FROM order_items oi JOIN products p ON p.id = oi.product_id GROUP BY p.category_id",
			map[string]int64{"mariadb114": 2000 + 2000*1993, "mariadb1011": 2000 + 2000*996}},
		{"join_three_tables_group_by", "SELECT o.status, co.region, count(*) FROM orders o JOIN customers c ON c.id = o.customer_id JOIN countries co ON co.code = c.country_code GROUP BY o.status, co.region",
			map[string]int64{"mariadb114": 50 + 50*2075 + 50*2075*20, "mariadb1011": 50 + 50*1012 + 50*1012*10}},
	} {
		for _, server := range mariaServers {
			p := joinPlan(t, server, tc.name)
			if p.RowsRead != tc.rows[server] || !p.Joined || p.RowsReadUnknown != "" {
				t.Errorf("%s/%s: estimate %d (joined %v, unknown %q), want %d", server, tc.name, p.RowsRead, p.Joined, p.RowsReadUnknown, tc.rows[server])
			}
			d := DefaultPolicy().DecideStatement(tc.stmt, p)
			if !d.ToCopy || d.Rule != RuleJoinRows {
				t.Errorf("%s/%s: decided toCopy=%v by %s (%s), want the copy by %s", server, tc.name, d.ToCopy, d.Rule, d.Reason, RuleJoinRows)
			}
		}
	}
	// The reason names the estimate and the threshold in plain words.
	d := DefaultPolicy().Decide(joinPlan(t, "mariadb114", "join_group_by_country"))
	if want := "plan reads about 2,090,000 rows across a join (threshold 100,000)"; d.Reason != want {
		t.Errorf("reason = %q, want %q", d.Reason, want)
	}
}

// The cheap side: joins that read few rows stay on the source, whatever
// the size of the tables behind them.
func TestDecideStatement_cheapJoinsStayOnMariaDB(t *testing.T) {
	for _, tc := range []struct {
		name, stmt string
		rule       Rule
		maxRows    int64 // the estimate is at most this, on every server
	}{
		// A point lookup joined to a small table, and to a big one.
		{"join_point_lookup_small_table", "SELECT c.name, co.name FROM customers c JOIN countries co ON co.code = c.country_code WHERE c.id = 42", RuleCheap, 2},
		{"join_point_lookup", "SELECT o.id, o.amount, c.name FROM orders o JOIN customers c ON c.id = o.customer_id WHERE o.id = 42", RuleCheap, 2},
		{"join_const_customer", "SELECT c.name, o.id FROM customers c JOIN orders o ON o.customer_id = c.id WHERE c.id = 7", RuleCheap, 21},
		// A primary key range of a few hundred rows.
		{"join_pk_range", "SELECT o.id, c.name FROM orders o JOIN customers c ON c.id = o.customer_id WHERE o.id BETWEEN 1000 AND 1300", RuleCheap, 602},
		{"in_list_join", "SELECT o.id, c.name FROM orders o JOIN customers c ON c.id = o.customer_id WHERE o.id IN (5, 50, 500, 5000)", RuleCheap, 8},
		// The outer table cut to a handful of rows by an index, over the
		// 2,000,000-row and 4,000,000-row tables.
		{"join_selective_outer_by_index", "SELECT c.name, o.id, o.amount FROM customers c JOIN orders o ON o.customer_id = c.id WHERE c.email = 'c777@example.com'", RuleCheap, 21},
		{"join_selective_outer_three_levels", "SELECT c.name, o.id, oi.product_id FROM customers c JOIN orders o ON o.customer_id = c.id JOIN order_items oi ON oi.order_id = o.id WHERE c.email = 'c777@example.com'", RuleCheap, 41},
		{"join_outer_pk_range_three_levels", "SELECT c.name, o.id, oi.product_id FROM customers c JOIN orders o ON o.customer_id = c.id JOIN order_items oi ON oi.order_id = o.id WHERE c.id BETWEEN 100 AND 110", RuleCheap, 11 + 220 + 220},
		{"left_join_small", "SELECT c.id, o.id FROM customers c LEFT JOIN orders o ON o.customer_id = c.id AND o.amount > 400 WHERE c.id BETWEEN 1 AND 20", RuleCheap, 420},
		{"semi_join_small_outer", "SELECT c.id, c.name FROM customers c WHERE c.id IN (SELECT customer_id FROM orders) AND c.id < 30", RuleCheap, 29 + 29*20},
		{"union_all_joins", "SELECT c.country_code, o.id FROM customers c JOIN orders o ON o.customer_id = c.id WHERE c.id = 5 UNION ALL SELECT c.country_code, o.id FROM customers c JOIN orders o ON o.customer_id = c.id WHERE c.id = 6", RuleCheap, 42},
		// One country of fifty: 2,000 customers, 40,000 orders.
		{"join_one_country", "SELECT count(*), sum(o.amount) FROM customers c JOIN orders o ON o.customer_id = c.id WHERE c.country_code = 'AR'", RuleCheap, 42000},
		// A subquery run per row of a small outer table.
		{"scalar_subquery_per_row_small", "SELECT c.id, (SELECT count(*) FROM orders o WHERE o.customer_id = c.id) FROM customers c WHERE c.id BETWEEN 100 AND 120", RuleCheap, 21 + 21*20},
		// LIMIT 10 over an index-ordered outer table: the bounded-limit
		// rule, which the estimate does not override.
		{"join_limit_outer_index_order", "SELECT c.id, o.id FROM customers c JOIN orders o ON o.customer_id = c.id ORDER BY c.id LIMIT 10", RuleBoundedLimit, 21},
		{"join_limit_pk_order_straight", "SELECT STRAIGHT_JOIN o.id, c.name FROM orders o JOIN customers c ON c.id = o.customer_id ORDER BY o.id DESC LIMIT 10", RuleBoundedLimit, 20},
	} {
		for _, server := range mariaServers {
			p := joinPlan(t, server, tc.name)
			if p.RowsReadUnknown != "" || p.RowsRead > tc.maxRows || p.RowsRead <= 0 {
				t.Errorf("%s/%s: estimate %d (unknown %q), want 1..%d", server, tc.name, p.RowsRead, p.RowsReadUnknown, tc.maxRows)
			}
			if d := DefaultPolicy().DecideStatement(tc.stmt, p); d.ToCopy || d.Rule != tc.rule {
				t.Errorf("%s/%s: decided toCopy=%v by %s (%s), want the source by %s", server, tc.name, d.ToCopy, d.Rule, d.Reason, tc.rule)
			}
		}
	}
}

// A LIMIT the server can stop at keeps the rule it had: the plan's rows
// are what the join reads to its end, and the LIMIT ends it early. The
// plans here read 100,000 to 1,100,000 rows by the estimate and their
// statements are answered after a few hundred.
func TestDecideStatement_limitKeepsEstimateOut(t *testing.T) {
	for _, tc := range []struct {
		server, name, stmt string
	}{
		{"mariadb114", "join_range_limit500", "SELECT o.id, c.name FROM orders o JOIN customers c ON c.id = o.customer_id WHERE o.created_at >= '2024-03-01' AND o.created_at < '2024-06-01' LIMIT 500"},
		{"mariadb1011", "join_range_limit500", "SELECT o.id, c.name FROM orders o JOIN customers c ON c.id = o.customer_id WHERE o.created_at >= '2024-03-01' AND o.created_at < '2024-06-01' LIMIT 500"},
		{"mariadb114", "join_limit_no_order", "SELECT o.id, c.name FROM orders o JOIN customers c ON c.id = o.customer_id LIMIT 10"},
		{"mariadb1011", "join_limit_no_order", "SELECT o.id, c.name FROM orders o JOIN customers c ON c.id = o.customer_id LIMIT 10"},
	} {
		p := joinPlan(t, tc.server, tc.name)
		if p.RowsRead < 100000 || !p.Joined {
			t.Fatalf("%s/%s: estimate %d, joined %v: the fixture no longer reads past the threshold", tc.server, tc.name, p.RowsRead, p.Joined)
		}
		d := DefaultPolicy().DecideStatement(tc.stmt, p)
		if d.ToCopy || d.Rule != RuleCheap || !strings.Contains(d.Reason, "LIMIT") {
			t.Errorf("%s/%s: decided toCopy=%v by %s (%s), want the source, cheap, with the LIMIT named", tc.server, tc.name, d.ToCopy, d.Rule, d.Reason)
		}
		// The same plan without the statement's LIMIT is the copy's.
		if d := DefaultPolicy().Decide(p); !d.ToCopy || d.Rule != RuleJoinRows {
			t.Errorf("%s/%s without the LIMIT: toCopy=%v by %s", tc.server, tc.name, d.ToCopy, d.Rule)
		}
	}
	// A LIMIT over a sort is no early stop: the join is read whole first.
	p := joinPlan(t, "mariadb114", "join_limit_index_order")
	if !p.Filesort {
		t.Fatal("join_limit_index_order no longer sorts")
	}
	if d := DefaultPolicy().DecideStatement("SELECT o.id, c.name FROM orders o JOIN customers c ON c.id = o.customer_id ORDER BY o.id DESC LIMIT 10", p); !d.ToCopy || d.Rule != RuleJoinRows {
		t.Errorf("LIMIT over a filesort: toCopy=%v by %s (%s), want the copy by %s", d.ToCopy, d.Rule, d.Reason, RuleJoinRows)
	}
}

// One table alone, however many rows it reads through an index, is not
// this rule's: the scan rules decide it, as before.
func TestDecide_singleTableIsNotAJoin(t *testing.T) {
	for _, name := range []string{"single_range_year", "single_ref_status", "single_range_limit500", "union_single_ranges"} {
		for _, server := range mariaServers {
			p := joinPlan(t, server, name)
			if p.Joined || p.RowsReadUnknown != "" || p.RowsRead < 100000 {
				t.Errorf("%s/%s: joined=%v unknown=%q rows=%d, want a counted read over the threshold that is no join", server, name, p.Joined, p.RowsReadUnknown, p.RowsRead)
			}
			if d := DefaultPolicy().Decide(p); d.ToCopy || d.Rule != RuleCheap {
				t.Errorf("%s/%s: toCopy=%v by %s (%s), want the source", server, name, d.ToCopy, d.Rule, d.Reason)
			}
		}
	}
}

// MySQL plans carry a cost: no estimate is made and the new rule never
// decides one, whatever the plan reads.
func TestDecide_mySQLPlansUntouched(t *testing.T) {
	for _, dir := range []string{filepath.Join("testdata", "joins", "mysql84"), filepath.Join("testdata", "servers", "mysql84"), "testdata"} {
		files, err := filepath.Glob(filepath.Join(dir, "*.json"))
		if err != nil || len(files) == 0 {
			t.Fatalf("%s: %v, %d plans", dir, err, len(files))
		}
		for _, f := range files {
			raw, err := os.ReadFile(f)
			if err != nil {
				t.Fatal(err)
			}
			p, err := ParsePlan(raw)
			if err != nil {
				t.Fatalf("%s: %v", f, err)
			}
			if p.CostUnknown && p.Message == "" {
				t.Errorf("%s: a MySQL plan read as one with no cost", f)
			}
			if p.RowsRead != 0 || p.Joined || p.RowsReadUnknown != "" {
				t.Errorf("%s: a MySQL plan was estimated: %+v", f, p)
			}
			if d := DefaultPolicy().Decide(p); d.Rule == RuleJoinRows {
				t.Errorf("%s: the rows rule decided a MySQL plan: %s", f, d.Reason)
			}
		}
	}
	// Even a plan struct that claims an estimate: the cost decides.
	withCost := Plan{Cost: 5, Tables: 2, RowsRead: 5000000, Joined: true}
	if d := DefaultPolicy().Decide(withCost); d.ToCopy || d.Rule != RuleCheap {
		t.Errorf("a plan with a cost went by its rows: %+v", d)
	}
}

// A shape the estimate does not know is not guessed: no estimate, the
// reason kept, and the scan rules decide as before.
func TestParsePlan_unknownShapeIsNotEstimated(t *testing.T) {
	table := func(name, access string, rows int) string {
		return `{"table":{"table_name":"` + name + `","access_type":"` + access + `","rows":` + itoa(rows) + `,"filtered":100}}`
	}
	big := table("c", "index", 90000) + "," + table("o", "ref", 50)
	for _, tc := range []struct {
		name, plan, why string
	}{
		{"a join step of an unknown kind",
			`{"query_block":{"select_id":1,"nested_loop":[` + table("c", "index", 90000) + `,{"future_join":` + table("o", "ref", 50) + `}]}}`, `unknown join step "future_join"`},
		{"a table under a wrapper the estimate does not descend into",
			`{"query_block":{"select_id":1,"nested_loop":[` + big + `],"future_wrapper":{"nested_loop":[` + table("x", "ALL", 500) + `]}}}`, "the plan has 3 table reads, 2 of them where a join step is expected"},
		{"a table with no rows",
			`{"query_block":{"select_id":1,"nested_loop":[` + table("c", "index", 90000) + `,{"table":{"table_name":"o","access_type":"ref","filtered":100}}]}}`, "table o has no usable rows estimate"},
		{"rows as a string",
			`{"query_block":{"select_id":1,"nested_loop":[` + table("c", "index", 90000) + `,{"table":{"table_name":"o","access_type":"ref","rows":"50"}}]}}`, "table o has no usable rows estimate"},
		{"negative rows",
			`{"query_block":{"select_id":1,"nested_loop":[` + table("c", "index", 90000) + `,{"table":{"table_name":"o","access_type":"ref","rows":-5}}]}}`, "table o has no usable rows estimate"},
		{"filtered over 100",
			`{"query_block":{"select_id":1,"nested_loop":[{"table":{"table_name":"c","access_type":"index","rows":90000,"filtered":250}},` + table("o", "ref", 50) + `]}}`, "table c has no usable filtered percentage"},
		{"an unknown subquery wrapper",
			`{"query_block":{"select_id":1,"nested_loop":[` + big + `],"subqueries":[{"future_cache":{"query_block":{"select_id":2,"nested_loop":[` + table("x", "ref", 5) + `]}}}]}}`, `unknown subquery node "future_cache"`},
		{"a block with no list of tables",
			`{"query_block":{"select_id":1,"future_plan":{"nested_loop":[` + big + `]}}}`, "no list of tables found in a query block"},
		{"a derived table with no block",
			`{"query_block":{"select_id":1,"nested_loop":[{"table":{"table_name":"<derived2>","access_type":"ALL","rows":90000,"filtered":100,"materialized":{}}},` + table("o", "ref", 50) + `]}}`, "table <derived2> is materialized from no query_block"},
		{"a rowid filter with no rows",
			`{"query_block":{"select_id":1,"nested_loop":[` + table("c", "index", 90000) + `,{"table":{"table_name":"o","access_type":"ref","rows":50,"rowid_filter":{"selectivity_pct":1}}}]}}`, "table o has a rowid_filter with no rows estimate"},
	} {
		p, err := ParsePlan([]byte(tc.plan))
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if p.RowsRead != 0 || p.Joined || p.RowsReadUnknown != tc.why {
			t.Errorf("%s: rows=%d joined=%v unknown=%q, want no estimate and %q", tc.name, p.RowsRead, p.Joined, p.RowsReadUnknown, tc.why)
		}
		d := DefaultPolicy().Decide(p)
		if d.ToCopy || d.Rule != RuleCheap || !strings.Contains(d.Reason, tc.why) {
			t.Errorf("%s: decided toCopy=%v by %s (%s), want the source with the reason kept", tc.name, d.ToCopy, d.Rule, d.Reason)
		}
	}
	// The one real plan of the fixtures with such a shape: a recursive CTE.
	for _, server := range mariaServers {
		p := joinPlan(t, server, "recursive_cte_join")
		if p.RowsRead != 0 || !strings.Contains(p.RowsReadUnknown, "recursive CTE") {
			t.Errorf("%s/recursive_cte_join: rows=%d unknown=%q", server, p.RowsRead, p.RowsReadUnknown)
		}
	}
	// An unknown shape does not switch the scan rules off.
	scan := `{"query_block":{"select_id":1,"nested_loop":[` + table("c", "ALL", 150000) + `,{"future_join":` + table("o", "ref", 50) + `}]}}`
	p, err := ParsePlan([]byte(scan))
	if err != nil {
		t.Fatal(err)
	}
	if d := DefaultPolicy().Decide(p); !d.ToCopy || d.Rule != RuleScan {
		t.Errorf("a full scan under an unknown shape: toCopy=%v by %s", d.ToCopy, d.Rule)
	}
}

func itoa(n int) string { return groupDigitsPlain(int64(n)) }

func groupDigitsPlain(n int64) string { return strings.ReplaceAll(groupDigits(n), ",", "") }

// Each way a block is combined with the one around it, on a plan small
// enough to check by hand.
func TestParsePlan_rowsReadArithmetic(t *testing.T) {
	tbl := func(name, access string, rows int, more string) string {
		return `{"table":{"table_name":"` + name + `","access_type":"` + access + `","rows":` + itoa(rows) + more + `}}`
	}
	block := func(steps string) string { return `{"select_id":2,"nested_loop":[` + steps + `]}` }
	top := func(steps, rest string) string {
		return `{"query_block":{"select_id":1,"nested_loop":[` + steps + `]` + rest + `}}`
	}
	for _, tc := range []struct {
		name, plan string
		rows       int64
		joined     bool
	}{
		{"one table", top(tbl("a", "range", 300, `,"filtered":100`), ""), 300, false},
		{"two tables: rows of the first times rows per entry of the second",
			top(tbl("a", "range", 300, "")+","+tbl("b", "ref", 20, ""), ""), 300 + 300*20, true},
		{"filtered of the first cuts the entries into the second",
			top(tbl("a", "range", 1000, `,"filtered":10`)+","+tbl("b", "ref", 20, ""), ""), 1000 + 100*20, true},
		{"filtered of the last table changes nothing read",
			top(tbl("a", "range", 300, "")+","+tbl("b", "ref", 20, `,"filtered":1`), ""), 300 + 300*20, true},
		{"a table is entered at least once, however few rows come before it",
			top(tbl("a", "ref", 1, `,"filtered":0.05`)+","+tbl("b", "ref", 2000, ""), ""), 1 + 2000, true},
		{"three tables", top(tbl("a", "ALL", 50, "")+","+tbl("b", "ref", 2000, "")+","+tbl("c", "ref", 20, ""), ""), 50 + 50*2000 + 50*2000*20, true},
		{"a buffered join with no hash compares every pair",
			top(tbl("a", "ALL", 2000, "")+`,{"block-nl-join":`+tbl("b", "ALL", 5000, "")+`}`, ""), 2000 + 2000*5000, true},
		{"a hash join reads its table once",
			top(tbl("a", "ALL", 2000, "")+`,{"block-nl-join":`+tbl("b", "hash_ALL", 5000, `,"filtered":10`)+`}`, ""), 2000 + 5000, true},
		{"duplicates_removal is part of the same list",
			top(tbl("a", "ref", 100, "")+`,{"duplicates_removal":[`+tbl("b", "ref", 10, "")+","+tbl("c", "eq_ref", 1, "")+`]}`, ""), 100 + 1000 + 1000, true},
		{"a derived table is built once, then probed per row",
			top(tbl("a", "range", 100, "")+","+tbl("<derived2>", "ref", 4, `,"materialized":{"query_block":`+block(tbl("x", "index", 7000, ""))+`}`), ""), 100 + 100*4 + 7000, true},
		{"a lateral derived table is built for each row before it",
			top(tbl("a", "range", 100, "")+","+tbl("<derived2>", "ref", 4, `,"materialized":{"lateral":1,"query_block":`+block(tbl("x", "ref", 30, ""))+`}`), ""), 100 + 100*4 + 100*30, true},
		{"a subquery with no reference outward runs once",
			top(tbl("a", "range", 100, ""), `,"subqueries":[{"query_block":`+block(tbl("x", "ref", 3000, `,"ref":["const"]`))+`}]`), 100 + 3000, false},
		{"a materialized IN subquery runs once",
			top(tbl("a", "range", 100, ""), `,"subqueries":[{"materialization":{"query_block":`+block(tbl("x", "ALL", 3000, ""))+`}}]`), 100 + 3000, false},
		{"a cached subquery runs for each row of its block (11.x spelling)",
			top(tbl("a", "range", 100, ""), `,"subqueries":[{"subquery_cache":{"state":"uninitialized","query_block":`+block(tbl("x", "ref", 30, ""))+`}}]`), 100 + 100*30, true},
		{"a cached subquery runs for each row of its block (10.x spelling)",
			top(tbl("a", "range", 100, ""), `,"subqueries":[{"expression_cache":{"state":"uninitialized","query_block":`+block(tbl("x", "ref", 30, ""))+`}}]`), 100 + 100*30, true},
		{"a per-row subquery runs at least once, however few rows its block produces",
			top(tbl("a", "ref", 1, `,"filtered":10`), `,"subqueries":[{"subquery_cache":{"query_block":`+block(tbl("x", "ref", 30, ""))+`}}]`), 1 + 30, true},
		{"a UNION branch the optimizer answered without a table reads nothing",
			`{"query_block":{"union_result":{"query_specifications":[{"query_block":{"select_id":1,"table":{"message":"Impossible WHERE"}}},{"query_block":` + block(tbl("a", "range", 300, "")+","+tbl("b", "ref", 20, "")) + `}]}}}`, 300 + 300*20, true},
		{"a subquery reading by an outer table's column runs per row",
			top(tbl("a", "range", 100, ""), `,"subqueries":[{"query_block":`+block(tbl("x", "ref", 30, `,"ref":["shop.a.id"]`))+`}]`), 100 + 100*30, true},
		{"a reference to a table of the subquery itself is not outward",
			top(tbl("a", "range", 100, ""), `,"subqueries":[{"query_block":`+block(tbl("x", "range", 30, "")+","+tbl("y", "eq_ref", 1, `,"ref":["shop.x.id"]`))+`}]`), 100 + 30 + 30, true},
		{"an IN probe runs per row",
			top(tbl("a", "range", 100, ""), `,"subqueries":[{"query_block":`+block(tbl("x", "index_subquery", 30, `,"ref":["func"]`))+`}]`), 100 + 100*30, true},
		{"the branches of a UNION add up",
			`{"query_block":{"union_result":{"query_specifications":[{"query_block":` + block(tbl("a", "range", 300, "")) + `},{"query_block":` + block(tbl("b", "range", 500, "")) + `}]}}}`, 800, false},
		{"wrappers around the list read nothing",
			`{"query_block":{"select_id":1,"filesort":{"sort_key":"x","temporary_table":{"nested_loop":[` + tbl("a", "range", 300, "") + "," + tbl("b", "ref", 20, "") + `]}}}}`, 300 + 300*20, true},
		{"a table read through its own sort",
			top(`{"read_sorted_file":{"filesort":{"sort_key":"x",`+strings.TrimSuffix(strings.TrimPrefix(tbl("a", "range", 300, ""), "{"), "}")+`}}},`+tbl("b", "ref", 20, ""), ""), 300 + 300*20, true},
		{"a rowid filter is built once",
			top(tbl("a", "range", 100, "")+","+tbl("b", "ref", 20, `,"rowid_filter":{"range":{"key":"k"},"rows":700,"selectivity_pct":5}`), ""), 100 + 100*20 + 700, true},
	} {
		p, err := ParsePlan([]byte(tc.plan))
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if p.RowsReadUnknown != "" || p.RowsRead != tc.rows || p.Joined != tc.joined {
			t.Errorf("%s: rows=%d joined=%v unknown=%q, want rows=%d joined=%v", tc.name, p.RowsRead, p.Joined, p.RowsReadUnknown, tc.rows, tc.joined)
		}
	}
}

// Products of large row counts stay a number: capped, never negative, +Inf
// or NaN, and still over any threshold.
func TestParsePlan_rowsReadDoesNotOverflow(t *testing.T) {
	var steps []string
	for i := 0; i < 40; i++ {
		steps = append(steps, `{"table":{"table_name":"t`+itoa(i)+`","access_type":"ALL","rows":9000000000000000000,"filtered":100}}`)
	}
	p, err := ParsePlan([]byte(`{"query_block":{"select_id":1,"nested_loop":[` + strings.Join(steps, ",") + `],"subqueries":[{"subquery_cache":{"query_block":{"select_id":2,"nested_loop":[` + steps[0] + `]}}}]}}`))
	if err != nil {
		t.Fatal(err)
	}
	if p.RowsReadUnknown != "" || p.RowsRead != int64(maxRowsRead) || p.RowsRead <= 0 {
		t.Fatalf("rows=%d unknown=%q, want the cap %d", p.RowsRead, p.RowsReadUnknown, int64(maxRowsRead))
	}
	if maxRowsRead >= math.MaxInt64 {
		t.Fatal("the cap does not fit an int64")
	}
	// Small tables: a full scan of 9e18 rows is not this test's subject.
	p.FullScans, p.MaxScanRows = 0, 0
	d := DefaultPolicy().Decide(p)
	if !d.ToCopy || d.Rule != RuleJoinRows || !strings.Contains(d.Reason, "1,000,000,000,000,000,000 rows") {
		t.Errorf("decided toCopy=%v by %s (%s)", d.ToCopy, d.Rule, d.Reason)
	}
	// A filtered of 0 on every table: nothing multiplies to NaN.
	zero, err := ParsePlan([]byte(`{"query_block":{"select_id":1,"nested_loop":[{"table":{"table_name":"a","access_type":"ALL","rows":0,"filtered":0}},{"table":{"table_name":"b","access_type":"ref","rows":0,"filtered":0}}]}}`))
	if err != nil {
		t.Fatal(err)
	}
	if zero.RowsRead != 0 || zero.RowsReadUnknown != "" {
		t.Errorf("empty tables: rows=%d unknown=%q", zero.RowsRead, zero.RowsReadUnknown)
	}
}

// The threshold is the scan threshold, inclusive, and 0 switches the rule
// off with the scan rules.
func TestDecide_rowsReadThreshold(t *testing.T) {
	at := Plan{CostUnknown: true, Tables: 2, RowsRead: 100000, Joined: true}
	if d := DefaultPolicy().Decide(at); !d.ToCopy || d.Rule != RuleJoinRows {
		t.Errorf("at the threshold: %+v", d)
	}
	below := at
	below.RowsRead = 99999
	d := DefaultPolicy().Decide(below)
	if d.ToCopy || d.Rule != RuleCheap || !strings.Contains(d.Reason, "about 99,999 rows read across a join") {
		t.Errorf("below the threshold: %+v", d)
	}
	if d := (Policy{CostThreshold: 10000}).Decide(at); d.ToCopy {
		t.Errorf("ScanRows 0 still sent a join to the copy: %+v", d)
	}
	if d := (Policy{ScanRows: 5000}).Decide(below); !d.ToCopy || d.Rule != RuleJoinRows {
		t.Errorf("a lower threshold: %+v", d)
	}
	// A trivial plan stays trivial.
	trivial := at
	trivial.Message = "Impossible WHERE"
	if d := DefaultPolicy().Decide(trivial); d.ToCopy || d.Rule != RuleTrivial {
		t.Errorf("a trivial plan: %+v", d)
	}
}

func TestGroupDigitsAndRound(t *testing.T) {
	for n, want := range map[int64]string{0: "0", 999: "999", 1000: "1,000", 2091915: "2,091,915", 1000000000000000000: "1,000,000,000,000,000,000"} {
		if got := groupDigits(n); got != want {
			t.Errorf("groupDigits(%d) = %q, want %q", n, got, want)
		}
	}
	for n, want := range map[int64]int64{0: 0, 999: 999, 1004: 1000, 1005: 1010, 2091915: 2090000, 99999: 100000, 3988000: 3990000, 1000000000000000000: 1000000000000000000} {
		if got := roundRows(n); got != want {
			t.Errorf("roundRows(%d) = %d, want %d", n, got, want)
		}
	}
}

// joinStatements reads testdata/joins/statements.tsv: the statement each
// plan of that directory was captured for.
func joinStatements(t *testing.T) (names []string, stmts map[string]string) {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", "joins", "statements.tsv"))
	if err != nil {
		t.Fatal(err)
	}
	stmts = map[string]string{}
	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		name, stmt, ok := strings.Cut(line, "\t")
		if !ok {
			t.Fatalf("statements.tsv: no tab in %q", line)
		}
		if strings.HasPrefix(stmt, "SET ") {
			// A session setting the plan was captured under.
			_, stmt, _ = strings.Cut(stmt, "; ")
		}
		names = append(names, name)
		stmts[name] = stmt
	}
	return names, stmts
}

// Every statement of testdata/joins on both MariaDB versions: which go to
// the copy, and by which rule. The times are one warm run on the MariaDB
// 11.4 the plans came from (2 vCPU, 400 MB buffer pool).
func TestDecideStatement_joinFixturesOnMariaDB(t *testing.T) {
	type sides struct{ v114, v1011 Rule } // "" = stays on the source
	const stays = Rule("")
	toCopy := map[string]sides{
		// The three joins of the issue: 6.9 s, 12.5 s and 8.2 s.
		"join_group_by_country":                 {RuleJoinRows, RuleJoinRows},
		"join_items_products_group_by_category": {RuleJoinRows, RuleJoinRows},
		"join_three_tables_group_by":            {RuleJoinRows, RuleJoinRows},
		// The same walk-and-probe shape under other clauses (0.2 s to 7.3 s).
		"join_distinct":              {RuleJoinRows, RuleJoinRows},
		"join_group_order_limit":     {RuleJoinRows, RuleJoinRows},
		"join_limit_index_order":     {RuleJoinRows, RuleJoinRows}, // sorts the whole join for its LIMIT
		"join_one_region":            {RuleJoinRows, RuleJoinRows},
		"join_range_month_items":     {RuleJoinRows, RuleJoinRows},
		"join_items_orders_status":   {RuleJoinRows, RuleJoinRows},
		"straight_join_big_outer":    {RuleJoinRows, RuleJoinRows},
		"anti_join_left":             {RuleJoinRows, RuleJoinRows},
		"union_distinct_heavy_joins": {RuleJoinRows, RuleJoinRows},
		// A subquery run for each of 100,000 rows (1.1 s).
		"scalar_subquery_per_row_all": {RuleJoinRows, RuleJoinRows},
		// A join with no index: 2,000 x 100,000 pairs compared (16.6 s), and
		// 50 x 2,000 (9 ms: at the threshold, and MySQL's cost sends it to
		// the copy too).
		"join_no_index_block": {RuleJoinRows, RuleJoinRows},
		"join_no_index_small": {RuleJoinRows, RuleJoinRows},
		// The first table is scanned whole under a filter no index serves;
		// the plan cannot know one row passes it (25 ms). 385 rows short of
		// the full-scan rule, which would send it to the copy as well.
		"join_outer_filter_no_index": {RuleJoinRows, RuleJoinRows},
		// At the threshold: MariaDB 10.11 reports about half the rows per
		// key that 11.4 does for the same index, and stays under it
		// (40 ms to 90 ms on the source).
		"scalar_subquery_per_row_big":       {RuleJoinRows, stays},
		"scalar_subquery_per_row_cache_off": {RuleJoinRows, stays},
		"derived_small_join_big":            {RuleJoinRows, stays},
		"join_no_index_hash":                {RuleJoinRows, stays},
		"order_by_pk_limit_join_small":      {RuleJoinRows, stays},
		// 10.11 builds the derived table per customer (lateral); 11.4 scans
		// the whole index for it.
		"derived_group_all_join": {RuleScan, RuleJoinRows},
		// Decided by the full-scan rule before and after.
		"semi_join_big_inner": {RuleScan, RuleScan},
		"not_in_subquery":     {RuleScan, RuleScan},
		"single_full_scan":    {RuleScan, RuleScan},
		"exists_uncorrelated": {RuleScan, RuleScan},
	}
	names, stmts := joinStatements(t)
	if len(names) < 60 {
		t.Fatalf("only %d statements read", len(names))
	}
	seen := map[string]bool{}
	for _, name := range names {
		seen[name] = true
		for i, server := range mariaServers {
			want := []Rule{toCopy[name].v114, toCopy[name].v1011}[i]
			p := joinPlan(t, server, name)
			if !p.CostUnknown {
				t.Errorf("%s/%s: a MariaDB plan read as one with a cost", server, name)
			}
			d := DefaultPolicy().DecideStatement(stmts[name], p)
			switch {
			case want == stays && d.ToCopy:
				t.Errorf("%s/%s: to the copy by %s (%s), want the source", server, name, d.Rule, d.Reason)
			case want != stays && (!d.ToCopy || d.Rule != want):
				t.Errorf("%s/%s: toCopy=%v by %s (%s), want the copy by %s", server, name, d.ToCopy, d.Rule, d.Reason, want)
			}
			// Only the recursive CTE has a shape the estimate does not know.
			if (p.RowsReadUnknown != "") != (name == "recursive_cte_join") {
				t.Errorf("%s/%s: RowsReadUnknown = %q", server, name, p.RowsReadUnknown)
			}
		}
	}
	for name := range toCopy {
		if !seen[name] {
			t.Errorf("%s is expected on the copy and is not in statements.tsv", name)
		}
	}
}
