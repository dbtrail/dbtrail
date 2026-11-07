package readrouter

import (
	"fmt"
	"strings"
	"testing"
)

// Range reads on a plan with no cost (MariaDB). The plans are EXPLAIN
// FORMAT=JSON as MariaDB 10.11.14 printed them over sysbench's sbtest2
// (10,000,000 rows, primary key id, secondary index k), 2026-11-07, shrunk
// to one line. Before range reads were counted, every one of them stayed on
// the source: WHERE id BETWEEN 1 AND 8000000 GROUP BY ... read 4.9 million
// rows on MariaDB at 5 s to 33 s under load, called a cheap plan.

// mariaRange is a one-table nested_loop with that table read as given.
func mariaRange(access, key string, rows int, cond string, usingIndex bool) string {
	extra := ""
	if usingIndex {
		extra = `,"using_index":true`
	}
	return fmt.Sprintf(`{"query_block":{"select_id":1,"nested_loop":[{"table":{"table_name":"sbtest2","access_type":%q,"possible_keys":[%q],"key":%q,"key_length":"4","used_key_parts":["id"],"rows":%d,"filtered":100,"attached_condition":%q%s}}]}}`,
		access, key, key, rows, cond, extra)
}

const (
	rangeCond = "sbtest2.`id` between 1 and 8000000"
	// The bench report: a range of the primary key, grouped and sorted.
	mariaGroupedRange = `{"query_block":{"select_id":1,"filesort":{"sort_key":"left(sbtest2.c,2)","temporary_table":{"nested_loop":[{"table":{"table_name":"sbtest2","access_type":"range","possible_keys":["PRIMARY"],"key":"PRIMARY","key_length":"4","used_key_parts":["id"],"rows":4931532,"filtered":100,"attached_condition":"sbtest2.` + "`id`" + ` between 1 and 8000000"}}]}}}}`
	// A const table, then an EXISTS whose subquery reads the same range.
	mariaExistsRange = `{"query_block":{"select_id":1,"const_condition":"<in_optimizer>(1,exists(subquery#2))","nested_loop":[{"table":{"table_name":"sbtest1","access_type":"const","possible_keys":["PRIMARY"],"key":"PRIMARY","key_length":"4","used_key_parts":["id"],"ref":["const"],"rows":1,"filtered":100,"using_index":true}}],"subqueries":[{"query_block":{"select_id":2,"nested_loop":[{"table":{"table_name":"sbtest2","access_type":"range","possible_keys":["PRIMARY"],"key":"PRIMARY","key_length":"4","used_key_parts":["id"],"rows":4931532,"filtered":100,"attached_condition":"sbtest2.` + "`id`" + ` between 1 and 8000000","using_index":true}}]}}]}}`
)

func TestDecideStatement_mariaDBRangeReads(t *testing.T) {
	bigRange := mariaRange("range", "PRIMARY", 4931532, rangeCond, false)
	for _, tc := range []struct {
		name, stmt, plan string
		toCopy           bool
		reason           string // a part of the reason
	}{
		// What the bench sent to the source: the whole range is read.
		{"grouped range", "SELECT LEFT(c, 2) AS p, COUNT(*), SUM(k) FROM sbtest2 WHERE id BETWEEN 1 AND 8000000 GROUP BY p ORDER BY p", mariaGroupedRange, true, "range read over 4931532 rows >= 100000"},
		{"count over a range", "SELECT COUNT(*) FROM sbtest2 WHERE id > 100", mariaRange("range", "PRIMARY", 4931532, "sbtest2.`id` > 100", true), true, "range read"},
		{"secondary index range", "SELECT COUNT(*), AVG(id) FROM sbtest2 WHERE k BETWEEN 1 AND 6000000", mariaRange("range", "k_2", 4931532, "sbtest2.k between 1 and 6000000", true), true, "range read"},
		{"no LIMIT", "SELECT id, k FROM sbtest2 WHERE id BETWEEN 1 AND 8000000", bigRange, true, "range read"},
		// A LIMIT ends the read: MariaDB's estimate does not shrink for it.
		{"page one", "SELECT id, k FROM sbtest2 WHERE id BETWEEN 1 AND 8000000 LIMIT 20", bigRange, false, "LIMIT 20 with no sort ends the range read early"},
		{"page one in key order", "SELECT id, k FROM sbtest2 WHERE id BETWEEN 1 AND 8000000 ORDER BY id LIMIT 20", bigRange, false, "LIMIT 20"},
		{"page one, offset form", "SELECT id, k FROM sbtest2 WHERE id BETWEEN 1 AND 8000000 LIMIT 40, 20", bigRange, false, "LIMIT 60"},
		{"prepared page", "SELECT id, k FROM sbtest2 WHERE id > ? ORDER BY id LIMIT ?", bigRange, false, "LIMIT ?"},
		{"5,000 rows", "SELECT id, k FROM sbtest2 WHERE id BETWEEN 1 AND 8000000 LIMIT 5000", bigRange, false, "LIMIT 5000"},
		// Known and kept: a filter the index does not serve, under a LIMIT,
		// may still walk the range; the page stays where it was.
		{"page one with a residual filter", "SELECT id FROM sbtest2 WHERE id BETWEEN 1 AND 8000000 AND c LIKE '%zz%' LIMIT 20", mariaRange("range", "PRIMARY", 4931532, rangeCond+" and sbtest2.c like '%zz%'", false), false, "LIMIT 20"},
		// The LIMIT does not end it.
		{"deep page", "SELECT id, k FROM sbtest2 WHERE id BETWEEN 1 AND 8000000 LIMIT 20 OFFSET 5000000", bigRange, true, "range read"},
		{"LIMIT at the threshold", "SELECT id, k FROM sbtest2 WHERE id BETWEEN 1 AND 8000000 LIMIT 99990, 10", bigRange, true, "range read"},
		{"LIMIT under the threshold", "SELECT id, k FROM sbtest2 WHERE id BETWEEN 1 AND 8000000 LIMIT 99989, 10", bigRange, false, "LIMIT 99999"},
		{"sort over the range", "SELECT id, k FROM sbtest2 WHERE id BETWEEN 1 AND 8000000 ORDER BY c LIMIT 20", mariaGroupedRange, true, "range read"},
		{"aggregate under a LIMIT", "SELECT COUNT(*) FROM sbtest2 WHERE id BETWEEN 1 AND 8000000 LIMIT 20", bigRange, true, "range read"},
		{"UNION under a LIMIT", "SELECT id FROM sbtest2 WHERE id BETWEEN 1 AND 8000000 UNION SELECT id FROM sbtest2 WHERE id < 5 LIMIT 20", bigRange, true, "range read"},
		{"UNION ALL under a LIMIT", "SELECT id FROM sbtest2 WHERE id BETWEEN 1 AND 8000000 UNION ALL SELECT id FROM sbtest2 WHERE id < 5 LIMIT 20", bigRange, false, "LIMIT 20"},
		{"LIMIT in a derived table too", "SELECT * FROM (SELECT id FROM sbtest2 WHERE id BETWEEN 1 AND 8000000 LIMIT 20) d LIMIT 10", bigRange, true, "range read"},
		{"executable comment", "SELECT /*!99999 SQL_NO_CACHE */ id FROM sbtest2 WHERE id BETWEEN 1 AND 8000000 LIMIT 20", bigRange, true, "range read"},
		{"LIMIT in a string only", "SELECT id FROM sbtest2 WHERE id BETWEEN 1 AND 8000000 AND c = 'LIMIT 20'", bigRange, true, "range read"},
		// Small, or not the outer read.
		{"small range", "SELECT SUM(k) FROM sbtest2 WHERE id BETWEEN 1 AND 1000", mariaRange("range", "PRIMARY", 2028, "sbtest2.`id` between 1 and 1000", false), false, "no full scan"},
		{"IN list", "SELECT * FROM sbtest2 WHERE id IN (1,2,3)", mariaRange("range", "PRIMARY", 3, "sbtest2.`id` in (1,2,3)", false), false, "no full scan"},
		{"one row under the threshold", "SELECT SUM(k) FROM sbtest2 WHERE id < 100000", mariaRange("range", "PRIMARY", 99999, "sbtest2.`id` < 100000", false), false, "no full scan"},
		{"at the threshold", "SELECT SUM(k) FROM sbtest2 WHERE id <= 100000", mariaRange("range", "PRIMARY", 100000, "sbtest2.`id` <= 100000", false), true, "range read over 100000 rows >= 100000"},
		{"range inside an EXISTS", "SELECT id FROM sbtest1 WHERE id = 5 AND EXISTS (SELECT 1 FROM sbtest2 WHERE sbtest2.id BETWEEN 1 AND 8000000)", mariaExistsRange, false, "no full scan"},
	} {
		p, err := ParsePlan([]byte(tc.plan))
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if !p.CostUnknown {
			t.Fatalf("%s: a MariaDB plan read as one with a cost", tc.name)
		}
		d := DefaultPolicy().DecideStatement(tc.stmt, p)
		if d.ToCopy != tc.toCopy || !strings.Contains(d.Reason, tc.reason) {
			t.Errorf("%s: toCopy=%v by %s (%s), want toCopy=%v with %q", tc.name, d.ToCopy, d.Rule, d.Reason, tc.toCopy, tc.reason)
		}
		if tc.toCopy && d.Rule != RuleScan {
			t.Errorf("%s: to the copy by %s, want %s", tc.name, d.Rule, RuleScan)
		}
	}
}

// A MySQL plan carries a cost, which already counts a range read: the
// range rule is not consulted, and a cheap one stays cheap however many
// rows its range estimate says.
func TestDecide_rangeRuleIsForPlansWithNoCost(t *testing.T) {
	plan := `{"query_block":{"select_id":1,"cost_info":{"query_cost":"900.00"},"table":{"table_name":"t","access_type":"range","key":"PRIMARY","rows_examined_per_scan":500000,"rows_produced_per_join":500000,"filtered":"100.00","cost_info":{"read_cost":"800.00"}}}}`
	p, err := ParsePlan([]byte(plan))
	if err != nil {
		t.Fatal(err)
	}
	if p.CostUnknown || p.MaxRangeScanRows != 500000 {
		t.Fatalf("costUnknown=%v range rows=%d", p.CostUnknown, p.MaxRangeScanRows)
	}
	if d := DefaultPolicy().Decide(p); d.ToCopy || strings.Contains(d.Reason, "range read") {
		t.Errorf("toCopy=%v (%s), want the cost rule's verdict, cheap", d.ToCopy, d.Reason)
	}
}
