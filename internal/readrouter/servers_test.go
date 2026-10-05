package readrouter

import (
	"os"
	"path/filepath"
	"testing"
)

// The plans under testdata/servers are EXPLAIN FORMAT=JSON as three servers
// print it for the same statements over the same data (testdata/servers/
// setup.sql: 200,000 orders, 5,000 customers): MySQL 8.4 and MariaDB 10.11,
// 11.4, 11.8 and 12.3. MariaDB spells the plan differently (rows, a filesort node,
// the shortcut message on a table) and carries no cost comparable to
// MySQL's, so the cost rule does not apply to it (#2073).
//
// Each statement is decided under the default policy. want is who answers on
// MySQL; wantMaria on every MariaDB version. Where they differ, MariaDB
// keeps the statement on the source: MySQL's cost is what sends it to the
// copy, and MariaDB's row estimates alone do not say the work is large.
func TestDecideStatement_acrossServers(t *testing.T) {
	const (
		toMySQL = false
		toCopy  = true
	)
	cases := []struct {
		name, stmt string
		want       bool
		rule       Rule
		wantMaria  bool
		ruleMaria  Rule
	}{
		{"point_lookup", "SELECT * FROM orders WHERE id = 42", toMySQL, RuleCheap, toMySQL, RuleCheap},
		{"ref_limit", "SELECT * FROM orders WHERE customer_id = 7 LIMIT 10", toMySQL, RuleBoundedLimit, toMySQL, RuleBoundedLimit},
		{"group_by_full_scan", "SELECT status, count(*), sum(total) FROM orders GROUP BY status", toCopy, RuleCost, toCopy, RuleScan},
		{"group_by_filter_sort", "SELECT status, count(*) FROM orders WHERE note = 'n7' GROUP BY status ORDER BY 2 DESC", toCopy, RuleCost, toCopy, RuleScan},
		// A derived table built by walking a whole index: no full scan in
		// the plan, so on MariaDB it is the index-scan rule that catches it.
		{"join_derived", "SELECT c.tier, t.n FROM customers c JOIN (SELECT customer_id, count(*) n FROM orders GROUP BY customer_id) t ON t.customer_id = c.id WHERE c.tier = 'gold'", toCopy, RuleCost, toCopy, RuleScan},
		{"order_by_pk_limit2", "SELECT id FROM orders ORDER BY id DESC LIMIT 2", toMySQL, RuleBoundedLimit, toMySQL, RuleBoundedLimit},
		{"order_by_filesort_limit20", "SELECT id, total FROM orders ORDER BY total DESC LIMIT 20", toCopy, RuleCost, toCopy, RuleScan},
		{"order_by_pk_limit2_filtered", "SELECT id FROM orders WHERE note = 'nope' ORDER BY id LIMIT 2", toCopy, RuleCost, toMySQL, RuleCheap},
		{"order_by_pk_limit2_join_filtered", "SELECT o.id FROM orders o JOIN customers c ON c.id = o.customer_id WHERE c.name = 'nope' ORDER BY o.id LIMIT 2", toCopy, RuleCost, toMySQL, RuleCheap},
		{"impossible_where", "SELECT * FROM orders WHERE 1 = 0", toMySQL, RuleTrivial, toMySQL, RuleTrivial},
		{"no_matching_const", "SELECT * FROM orders WHERE id = -1", toMySQL, RuleTrivial, toMySQL, RuleTrivial},
		{"no_tables", "SELECT 1 + 1", toMySQL, RuleTrivial, toMySQL, RuleTrivial},
		{"range_scan", "SELECT count(*) FROM orders WHERE created_at >= '2026-03-01' AND created_at < '2026-03-02'", toMySQL, RuleCheap, toMySQL, RuleCheap},
		{"union_scan", "SELECT id FROM orders WHERE status = 'new' UNION SELECT id FROM orders WHERE status = 'void'", toCopy, RuleScan, toCopy, RuleScan},
		{"subquery_in", "SELECT count(*) FROM orders WHERE customer_id IN (SELECT id FROM customers WHERE tier = 'gold')", toMySQL, RuleCheap, toMySQL, RuleCheap},
		{"full_scan_plain", "SELECT * FROM orders WHERE note = 'n7'", toCopy, RuleCost, toCopy, RuleScan},
	}
	// Seven more shapes, captured on MySQL 8.4 and MariaDB 11.4, 11.8, 12.3.
	more := []struct {
		name, stmt string
		want       bool
		rule       Rule
		wantMaria  bool
		ruleMaria  Rule
	}{
		// One row, plus an EXISTS over a big table: the subquery stops at
		// the first index entry. MySQL's cost counts the whole index and
		// sends it to the copy; on MariaDB an index scan inside a subquery
		// is not counted, and it stays on the source.
		{"exists_subquery", "SELECT * FROM customers WHERE id = 5 AND EXISTS (SELECT 1 FROM orders)", toCopy, RuleCost, toMySQL, RuleCheap},
		{"index_limit_no_order", "SELECT concat(customer_id) FROM orders LIMIT 10", toCopy, RuleCost, toCopy, RuleScan},
		{"scalar_subquery_no_tables", "SELECT (SELECT count(*) FROM orders WHERE note = 'x')", toMySQL, RuleTrivial, toMySQL, RuleTrivial},
		{"semi_join_in", "SELECT * FROM customers c WHERE c.id IN (SELECT customer_id FROM orders) AND c.id < 3", toMySQL, RuleCheap, toMySQL, RuleCheap},
		// MariaDB reports the few groups the LIMIT needs; MySQL's cost is
		// the whole index's.
		{"group_by_index_limit", "SELECT customer_id, count(*) FROM orders GROUP BY customer_id LIMIT 5", toCopy, RuleCost, toMySQL, RuleCheap},
		{"count_star", "SELECT count(*) FROM orders", toCopy, RuleCost, toCopy, RuleScan},
		{"covering_index_scan", "SELECT customer_id FROM orders", toCopy, RuleCost, toCopy, RuleScan},
	}
	pol := DefaultPolicy()
	for _, server := range []string{"mysql84", "mariadb1011", "mariadb114", "mariadb118", "mariadb123"} {
		all := cases
		if server != "mariadb1011" {
			all = append(append(all[:0:0], cases...), more...)
		}
		for _, tc := range all {
			raw, err := os.ReadFile(filepath.Join("testdata", "servers", server, tc.name+".json"))
			if err != nil {
				t.Fatal(err)
			}
			p, err := ParsePlan(raw)
			if err != nil {
				t.Fatalf("%s/%s: %v", server, tc.name, err)
			}
			want, rule := tc.want, tc.rule
			if server != "mysql84" {
				want, rule = tc.wantMaria, tc.ruleMaria
			}
			if p.CostUnknown != (server != "mysql84") && p.Message == "" {
				t.Errorf("%s/%s: CostUnknown = %v", server, tc.name, p.CostUnknown)
			}
			if d := pol.DecideStatement(tc.stmt, p); d.ToCopy != want || d.Rule != rule {
				t.Errorf("%s/%s: decided toCopy=%v by %s (%s); want toCopy=%v by %s. Plan: %+v", server, tc.name, d.ToCopy, d.Rule, d.Reason, want, rule, p)
			}
		}
	}
}

// What the MariaDB spelling must be read as, field by field.
func TestParsePlan_mariaDB(t *testing.T) {
	for _, tc := range []struct {
		name, plan string
		want       Plan
	}{
		{"rows and a full scan",
			`{"query_block":{"select_id":1,"cost":32.5,"nested_loop":[{"table":{"table_name":"orders","access_type":"ALL","loops":1,"rows":195573,"cost":32.5,"filtered":100,"attached_condition":"orders.note = 'n7'"}}]}}`,
			Plan{CostUnknown: true, Tables: 1, FullScans: 1, MaxScanRows: 195573, ScanFilter: true}},
		{"a filesort node",
			`{"query_block":{"select_id":1,"nested_loop":[{"read_sorted_file":{"filesort":{"sort_key":"orders.total desc","table":{"table_name":"orders","access_type":"ALL","rows":200246,"filtered":100}}}}]}}`,
			Plan{CostUnknown: true, Tables: 1, FullScans: 1, MaxScanRows: 200246, Filesort: true}},
		{"a whole index",
			`{"query_block":{"select_id":1,"nested_loop":[{"table":{"table_name":"orders","access_type":"index","key":"customer_id","rows":200246,"filtered":100,"using_index":true}}]}}`,
			Plan{CostUnknown: true, Tables: 1, MaxScanRows: 200246, MaxIndexScanRows: 200246}},
		{"the shortcut message on the top block's table",
			`{"query_block":{"select_id":1,"table":{"message":"Impossible WHERE"}}}`,
			Plan{CostUnknown: true, Message: "Impossible WHERE"}},
		// One impossible branch of a UNION is not a trivial statement.
		{"a message below the top block",
			`{"query_block":{"union_result":{"query_specifications":[{"query_block":{"select_id":1,"table":{"message":"Impossible WHERE"}}},{"query_block":{"select_id":2,"nested_loop":[{"table":{"table_name":"orders","access_type":"ALL","rows":200246}}]}}]}}}`,
			Plan{CostUnknown: true, Tables: 1, FullScans: 1, MaxScanRows: 200246}},
	} {
		got, err := ParsePlan([]byte(tc.plan))
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		got.scans, got.conditions, got.costInfo = false, false, false
		got.RowsRead, got.Joined, got.RowsReadUnknown = 0, false, ""
		got.topSort, got.sortedFirstRows = 0, 0 // joinrows_test.go
		if got != tc.want {
			t.Errorf("%s:\n  got  %+v\n  want %+v", tc.name, got, tc.want)
		}
	}
	// The index-scan rule is for plans with no cost only: on MySQL the cost
	// already says what an index walk costs.
	mysqlIndex := Plan{Cost: 5, Tables: 1, MaxScanRows: 500000, MaxIndexScanRows: 500000}
	if d := DefaultPolicy().Decide(mysqlIndex); d.ToCopy {
		t.Errorf("a cheap MySQL plan with a large index walk went to the copy: %+v", d)
	}
	// And a cost in MariaDB's unit never meets a MySQL threshold.
	maria := Plan{CostUnknown: true, Cost: 99999, Tables: 1, MaxScanRows: 10}
	if d := DefaultPolicy().Decide(maria); d.ToCopy {
		t.Errorf("the cost rule fired on a plan with no MySQL cost: %+v", d)
	}
}
