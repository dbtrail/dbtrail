package readrouter

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The plans under testdata/limits are EXPLAIN FORMAT=JSON as MySQL 8.4.9,
// MariaDB 11.4.13 and MariaDB 10.11.19 print it over the same data
// (testdata/limits/setup.sql: 2,000,000 orders over two years, 100,000
// customers), for the statements in testdata/limits/statements.tsv (#2115).
// Each statement was also run on each server; the rows it returned, the
// rows the server read and the time it took are what the verdicts below
// were written from, and the comments carry the ones that matter.
//
// A verdict is who answers under the default policy with a row cap of 1,000
// on the copy:
//
//	source:<rule>  the source, by that rule
//	copy           the copy is tried
//	over-cap       the plan is the copy's, but it returns more rows than the
//	               copy's cap: the source answers, the copy is not tried
const limitsRowCap = 1000

func limitsVerdict(d Decision) string {
	switch {
	case !d.ToCopy:
		return "source:" + string(d.Rule)
	case d.ResultRows > limitsRowCap:
		return "over-cap"
	}
	return "copy"
}

func TestDecideStatement_limitsAndResultRows_2115(t *testing.T) {
	const (
		bounded = "source:bounded_limit"
		cheap   = "source:cheap"
		trivial = "source:trivial"
		toCopy  = "copy"
		overCap = "over-cap"
	)
	// MySQL 8.4, MariaDB 11.4, then MariaDB 10.11 where it differs from 11.4.
	want := map[string][3]string{
		// The statement of the issue: 500 rows of a three-month range (246,857
		// rows). MySQL's cost (544,453) ignores the LIMIT; the source read it
		// in 24 ms on MySQL and 1 ms on MariaDB, the copy in 65 ms.
		"a01_range_limit500":                    {bounded, cheap},
		"a02_range_between_limit500":            {bounded, cheap},
		"a03_range_order_indexed_limit500":      {bounded, cheap}, // 1.6 ms on MySQL
		"a04_range_order_indexed_desc_limit500": {bounded, cheap},
		// The sort has no index: the source reads the whole range (615 ms on
		// MySQL) before the first row.
		"a05_range_order_unindexed_limit500": {toCopy, cheap},
		// A second condition no index serves, matching nothing: the source
		// reads the whole range looking for 500 rows (400 ms). MySQL reports
		// filtered 10 for the first, and 100 for the function, the arithmetic
		// and the column compared with a column: only the condition's own text
		// tells those.
		"a06_range_residual_absent_limit500":  {toCopy, cheap},
		"a07_range_residual_func_limit500":    {toCopy, cheap},
		"a08_range_residual_arith_limit500":   {toCopy, cheap},
		"a28_range_col_eq_col_limit500":       {toCopy, cheap},
		"a30_range_estimated_filter_limit500": {toCopy, cheap},
		"a09_range_covering_limit500":         {bounded, cheap}, // the range sits in attached_condition: the index covers the statement
		"a10_pk_range_limit500":               {bounded, cheap}, // and here: a primary key range
		"a11_pk_range_residual_limit500":      {toCopy, cheap},  // 294 ms: one million rows read for none
		"a12_ref_status_limit500":             {bounded, cheap},
		"a13_ref_status_order_pk_limit500":    {bounded, cheap},
		"a14_wide_range_limit500":             {bounded, cheap},
		// The order comes from the primary key and the filter from another
		// column: 1,668,158 rows read before the 500 (438 ms on MySQL).
		"a15_tail_range_order_pk_limit500": {toCopy, cheap},
		// The source reads offset plus limit rows (163 ms): not a few.
		"a16_range_limit500_offset100000": {toCopy, cheap},
		// The LIMIT applies after the grouping, the DISTINCT, the aggregate.
		"a17_group_by_limit500":  {toCopy, cheap},
		"a18_distinct_limit500":  {toCopy, cheap, toCopy}, // 10.11 walks a whole index for it
		"a19_aggregate_limit500": {toCopy, cheap},
		"a20_limit_in_derived":   {toCopy, toCopy}, // the scan rule reads the derived table's 500 rows as the range behind them: as before
		"a21_union_limit500":     {cheap, cheap},
		// A join: the rule is for one table read alone. (MariaDB: the full
		// scan of customers, 100,422 rows, is over the scan threshold.)
		"a22_join_limit500":                        {toCopy, toCopy},
		"a23_range_limit0":                         {trivial, trivial},
		"a24_range_limit5000000":                   {toCopy, toCopy},   // a full scan with a filter: no estimate of the result is trusted
		"a25_composite_all_key_parts_limit500":     {bounded, bounded}, // 52 rows estimated: the rule that was there
		"a26_composite_residual_key_part_limit500": {cheap, cheap},
		"a27_range_or_limit500":                    {toCopy, cheap}, // an OR is not read: as before
		"a29_order_indexed_no_where_limit500":      {bounded, bounded},

		"b01_pk_range_10k": {cheap, cheap},
		// A full scan with a filter. Its estimate of the result is a guess when
		// no index serves the filter (b05: 221,203 rows estimated, 1 returned),
		// and the plan does not say which it is: tried on the copy as before.
		"b02_range_3m":               {toCopy, toCopy},
		"b05_unindexed_eq_rare":      {toCopy, toCopy},
		"b06_unindexed_eq_2000":      {toCopy, toCopy},
		"b16_range_estimated_filter": {toCopy, toCopy},
		"b22_unindexed_range":        {toCopy, toCopy}, // 737,270 estimated, 980 returned: the copy answers it
		// The whole table: 2,000,000 rows, whatever the estimate (2,212,033).
		"b03_full_table":                 {overCap, overCap},
		"b04_full_table_order_unindexed": {overCap, overCap},
		"b07_ref_refunded":               {cheap, cheap},
		// By an index, no other filter: 1,106,016 estimated for 1,798,000
		// returned, 632,032 for 200,000. (MariaDB reads the table whole with
		// the filter attached.)
		"b08_ref_paid":    {overCap, toCopy},
		"b09_ref_pending": {overCap, toCopy},
		// One row, three rows: an aggregate's result is not the rows it reads.
		"b10_aggregate_range":       {toCopy, toCopy},
		"b11_group_by_status":       {toCopy, toCopy},
		"b12_group_by_customer":     {toCopy, toCopy}, // 100,000 groups: the plan does not count groups
		"b13_join_country":          {toCopy, toCopy},
		"b14_join_country_rare":     {toCopy, toCopy}, // 1 row out of a join estimated at 27,913
		"b15_range_limit5000":       {overCap, cheap},
		"b17_customers_ref_us":      {overCap, toCopy},
		"b18_range_order_unindexed": {toCopy, toCopy},
		"b19_point":                 {cheap, cheap},
		"b20_range_900":             {cheap, cheap},
		"b21_range_1500":            {cheap, cheap},

		// MySQL only (the p* plans).
		// The index is on (customer_id, created_at) and the range on its
		// first column: the condition on the second is checked entry by
		// entry (158 ms for no row). The plan estimates one row, and the
		// rule that was there before this one keeps it on the source.
		"p01_composite_between_then_eq":  {bounded},
		"p02_composite_range_then_range": {bounded},
		"p03_composite_eq_then_range":    {bounded},
		// An index on the first two characters of note: every row with the
		// prefix is read and compared (222,000 rows for none, 349 ms).
		"p04_prefix_ref":              {toCopy},
		"p05_prefix_ref_absent":       {toCopy},
		"p06_prefix_range_in":         {toCopy},
		"p07_string_in":               {bounded},
		"p08_string_range":            {bounded},
		"p09_ref_datetime":            {bounded},
		"p10_ref_plus_pk_range":       {toCopy},
		"p11_pk_in":                   {bounded},
		"p12_pk_between":              {bounded},
		"p13_pk_const":                {bounded},
		"p14_ne":                      {toCopy}, // <> is not read: as before
		"p15_is_null":                 {bounded},
		"p16_range_order_other_index": {toCopy},
		"p17_ref_order_unindexed":     {toCopy}, // 200,000 rows sorted for 500: 418 ms
		"p18_select_expr_no_where":    {bounded},
		"p19_sql_buffer_result":       {toCopy},
		"p20_window":                  {toCopy},
		"p21_scalar_subquery_select":  {toCopy},
		"p22_in_subquery":             {toCopy},
		"p23_range_date_literal":      {bounded},
		"p24_negative":                {toCopy}, // printed as <cache>(-(5)): not read, as before
		"p25_const_on_left":           {toCopy}, // the value before the column: not read, as before
		"p26_view":                    {toCopy}, // the view's own filter is a second condition
		"p27_partitioned":             {toCopy}, // a partitioned table: not read, as before
	}
	raw, err := os.ReadFile(filepath.Join("testdata", "limits", "statements.tsv"))
	if err != nil {
		t.Fatal(err)
	}
	pol := DefaultPolicy()
	seen := 0
	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		name, stmt, ok := strings.Cut(line, "\t")
		if !ok {
			t.Fatalf("statements.tsv: %q", line)
		}
		w, ok := want[name]
		if !ok {
			t.Errorf("%s: no verdict written for it", name)
			continue
		}
		seen++
		for i, server := range []string{"mysql84", "mariadb114", "mariadb1011"} {
			wantV := w[i]
			if i == 2 && wantV == "" {
				wantV = w[1]
			}
			if wantV == "" {
				continue // a p* statement: captured on MySQL only
			}
			plan, err := os.ReadFile(filepath.Join("testdata", "limits", server, name+".json"))
			if err != nil {
				t.Fatal(err)
			}
			p, err := ParsePlan(plan)
			if err != nil {
				t.Fatalf("%s/%s: %v", server, name, err)
			}
			d := pol.DecideStatement(stmt, p)
			if got := limitsVerdict(d); got != wantV {
				t.Errorf("%s/%s: %s, want %s (%s; reads only its result %v, result rows %d)\n  %s", server, name, got, wantV, d.Reason, p.ReadIsResult, d.ResultRows, stmt)
			}
			if !d.ToCopy && d.ResultRows != 0 {
				t.Errorf("%s/%s: a statement kept on the source carries result rows %d", server, name, d.ResultRows)
			}
		}
	}
	if seen != len(want) {
		t.Errorf("%d verdicts for %d statements: one names a statement that is not in statements.tsv", len(want), seen)
	}
}

// The condition reader takes MySQL's spelling of a condition and nothing
// else: what it cannot read keeps the statement where it was.
func TestReadCondition_2115(t *testing.T) {
	type pred = condPred
	cases := []struct {
		cond string
		want []pred
		ok   bool
	}{
		{"(`d`.`t`.`id` > 5)", []pred{{col: "id", num: true}}, true},
		{"((`d`.`t`.`a` >= TIMESTAMP'2026-01-01 00:00:00') and (`d`.`t`.`a` < DATE'2026-04-01'))", []pred{{col: "a"}, {col: "a"}}, true},
		{"(`d`.`t`.`a` between '2026-01-01' and '2026-03-31')", []pred{{col: "a", str: true}}, true},
		{"((`d`.`t`.`a` in (1,2,3)) and (`d`.`t`.`b` = 'x''y\\'z'))", []pred{{col: "a", point: true, num: true}, {col: "b", point: true, str: true}}, true},
		{"(`d`.`t`.`a` <=> 1.5e3)", []pred{{col: "a", point: true, num: true}}, true},
		{"(`d`.`t`.`we``ird` = -7)", []pred{{col: "we`ird", point: true, num: true}}, true},
		{"(`t`.`a` = 1) and (`t`.`b` <= 2)", []pred{{col: "a", point: true, num: true}, {col: "b", num: true}}, true},
		{"(`d`.`t`.`a` between 1 and '9')", []pred{{col: "a", str: true, num: true}}, true},
		// Not read.
		{"(length(`d`.`t`.`note`) > 100)", nil, false},
		{"((`d`.`t`.`amount` + 0) > 99999)", nil, false},
		{"(`d`.`t`.`a` = `d`.`t`.`b`)", nil, false},
		{"((`d`.`t`.`a` = 1) or (`d`.`t`.`a` = 2))", nil, false},
		{"(`d`.`t`.`a` <> 1)", nil, false},
		{"(`d`.`t`.`a` is null)", nil, false},
		{"(`d`.`t`.`a` not in (1,2))", nil, false},
		{"(`d`.`t`.`a` like 'x%')", nil, false},
		{"(`d`.`t`.`id` > <cache>(-(5)))", nil, false},
		{"(TIMESTAMP'2026-01-01 00:00:00' <= `d`.`t`.`a`)", nil, false},
		{"(`d`.`t`.`a` in (select 1))", nil, false},
		{"(`d`.`t`.`a` between 1 and `d`.`t`.`b`)", nil, false},
		{"(`d`.`t`.`a` = 1) and", nil, false},
		{"(`d`.`t`.`a` = 1", nil, false},
		{"(`d`.`t`.`a` = 'open)", nil, false},
		{"(`d`.`t`.`a = 1)", nil, false},
		{"`d`.`t`.`a` = 1)", nil, false},
		{"(`d`.`t`.`a` in ())", nil, false},
		{"(`d`.`t`.`a` = 1 2)", nil, false},
		{"", nil, false},
		// MariaDB's spelling: not read (its range reads never were the copy's).
		{"orders.created_at >= '2026-01-01' and orders.created_at < '2026-04-01'", nil, false},
		{"orders.`id` > 1000000", nil, false},
	}
	for _, tc := range cases {
		got, ok := readCondition(tc.cond)
		if ok != tc.ok || (ok && fmt.Sprint(got) != fmt.Sprint(tc.want)) {
			t.Errorf("readCondition(%q) = %v, %v; want %v, %v", tc.cond, got, ok, tc.want, tc.ok)
		}
	}
}

// Plan shapes the captured plans do not hold, written from them by changing
// one thing: each must stop the plan from being called "reads only its
// result".
func TestReadIsResult_refusals_2115(t *testing.T) {
	base, err := os.ReadFile(filepath.Join("testdata", "limits", "mysql84", "a01_range_limit500.json"))
	if err != nil {
		t.Fatal(err)
	}
	edit := func(f func(qb, table map[string]any)) Plan {
		t.Helper()
		var root map[string]any
		if err := json.Unmarshal(base, &root); err != nil {
			t.Fatal(err)
		}
		qb := root["query_block"].(map[string]any)
		f(qb, qb["table"].(map[string]any))
		out, _ := json.Marshal(root)
		p, err := ParsePlan(out)
		if err != nil {
			t.Fatal(err)
		}
		return p
	}
	if p := edit(func(_, _ map[string]any) {}); !p.ReadIsResult || p.ResultRows != 599380 {
		t.Fatalf("the plan as captured: reads only its result %v, %d rows; want true, 599380", p.ReadIsResult, p.ResultRows)
	}
	for name, f := range map[string]func(qb, table map[string]any){
		"filtered under 100":          func(_, tb map[string]any) { tb["filtered"] = "99.99" },
		"no filtered":                 func(_, tb map[string]any) { delete(tb, "filtered") },
		"an access type not known":    func(_, tb map[string]any) { tb["access_type"] = "index_merge" },
		"a full scan with a filter":   func(_, tb map[string]any) { tb["access_type"] = "ALL" },
		"an index walk with a filter": func(_, tb map[string]any) { tb["access_type"] = "index" },
		"a column outside the key":    func(_, tb map[string]any) { tb["used_key_parts"] = []any{"id"} },
		"no key parts":                func(_, tb map[string]any) { delete(tb, "used_key_parts") },
		"a temporary table":           func(qb, _ map[string]any) { qb["using_temporary_table"] = true },
		"a node not known":            func(qb, _ map[string]any) { qb["something_new"] = map[string]any{"x": 1} },
		"a list not known":            func(_, tb map[string]any) { tb["partitions"] = []any{"p0"} },
		"a skip scan":                 func(_, tb map[string]any) { tb["using_index_for_skip_scan"] = true },
		"a second table": func(qb, tb map[string]any) {
			delete(qb, "table")
			qb["nested_loop"] = []any{map[string]any{"table": tb}, map[string]any{"table": tb}}
		},
		// A secondary index, not covering, its condition attached and not
		// pushed to the index: what a prefix index looks like (p06).
		"a condition attached under a secondary index": func(_, tb map[string]any) {
			tb["attached_condition"] = tb["index_condition"]
			delete(tb, "index_condition")
		},
		// A range on the first column of a two-column key, then a range on
		// the second: the second is checked entry by entry (p02), should a
		// server ever list both columns as used.
		"a range before the last key part": func(_, tb map[string]any) {
			tb["used_key_parts"] = []any{"created_at", "id"}
			tb["index_condition"] = "((`r`.`orders`.`created_at` >= TIMESTAMP'2026-01-01 00:00:00') and (`r`.`orders`.`id` > 5))"
		},
		// A condition on the block and not on the table (MariaDB's HAVING)
		// filters after the read, and so does any table-level condition
		// other than the two that are read.
		"a condition on the block":                 func(qb, _ map[string]any) { qb["having_condition"] = "t.v > 1" },
		"a condition of another kind on the table": func(_, tb map[string]any) { tb["pushed_condition"] = "(`r`.`orders`.`note` = 'x')" },
		// A plan with no cost is MariaDB's: its range and key reads were
		// not measured under this rule, and never needed it.
		"a range read in a plan with no cost": func(qb, tb map[string]any) {
			delete(qb, "cost_info")
			delete(tb, "cost_info")
		},
		// A column held against a text and against a number: one of the two
		// is not its type, and is no bound of the index.
		"a text and a number for one column": func(_, tb map[string]any) {
			tb["index_condition"] = "((`r`.`orders`.`created_at` > 'a') and (`r`.`orders`.`created_at` = 5))"
		},
		"a text and a number in one BETWEEN": func(_, tb map[string]any) {
			tb["index_condition"] = "(`r`.`orders`.`created_at` between 5 and 'a')"
		},
		// A text compared under the primary key, with no index condition
		// pushdown to tell a prefix key from a whole one.
		"a text under the primary key": func(_, tb map[string]any) {
			tb["key"] = "PRIMARY"
			tb["attached_condition"] = "(`r`.`orders`.`created_at` > 'x')"
			delete(tb, "index_condition")
		},
	} {
		if p := edit(f); p.ReadIsResult {
			t.Errorf("%s: the plan still reads only its result", name)
		}
	}
	// And the ones that must not stop it.
	for name, f := range map[string]func(qb, table map[string]any){
		"points before the last key part": func(_, tb map[string]any) {
			tb["used_key_parts"] = []any{"customer_id", "created_at"}
			tb["index_condition"] = "((`r`.`orders`.`customer_id` in (1,2)) and (`r`.`orders`.`created_at` >= TIMESTAMP'2026-01-01 00:00:00'))"
		},
		"a number under the primary key": func(_, tb map[string]any) {
			tb["key"] = "PRIMARY"
			tb["used_key_parts"] = []any{"id"}
			tb["attached_condition"] = "(`r`.`orders`.`id` > 5)"
			delete(tb, "index_condition")
		},
		"a scalar not known": func(qb, tb map[string]any) { qb["new_scalar"] = 1.0; tb["other"] = "x" },
	} {
		if p := edit(f); !p.ReadIsResult {
			t.Errorf("%s: the plan no longer reads only its result", name)
		}
	}
}

// The result's row count: the plan's estimate, cut by the statement's own
// LIMIT, and nothing when the statement returns something else than the
// rows it reads.
func TestResultRows_2115(t *testing.T) {
	exact := Plan{ReadIsResult: true, ResultRows: 599380}
	cases := []struct {
		stmt string
		p    Plan
		want int64
	}{
		{"SELECT * FROM orders WHERE created_at >= '2026-01-01'", exact, 599380},
		{"SELECT * FROM orders WHERE created_at >= '2026-01-01';", exact, 599380},
		{"SELECT * FROM orders LIMIT 5000", exact, 5000},
		{"SELECT * FROM orders LIMIT 5000;", exact, 5000},
		{"SELECT * FROM orders LIMIT 900", exact, 900},
		{"SELECT * FROM orders LIMIT 10, 5000", exact, 5000},
		{"SELECT * FROM orders LIMIT 5000 OFFSET 10", exact, 5000},
		{"SELECT * FROM orders LIMIT 5000 OFFSET 599000", exact, 380},
		{"SELECT * FROM orders LIMIT 599000, 5000", exact, 380},
		{"SELECT * FROM orders LIMIT 5000 OFFSET 700000", exact, 0},
		{"SELECT * FROM orders LIMIT 18446744073709551615", exact, 599380},
		{"SELECT * FROM orders LIMIT 99999999999999999999", exact, 0}, // not a number MySQL takes: no estimate
		{"SELECT * FROM orders LIMIT 0", exact, 0},
		{"select * from orders limit 5000", exact, 5000},
		{"SELECT 'limit 5' FROM orders", exact, 599380},
		{"SELECT * FROM orders /* limit 5 */", exact, 599380},
		// The standard's spelling of a LIMIT (MariaDB, and the copy): the
		// bound is not read, so nothing is said about the result.
		{"SELECT * FROM orders ORDER BY amount FETCH FIRST 5 ROWS ONLY", exact, 0},
		{"SELECT * FROM orders OFFSET 10 ROWS FETCH NEXT 5 ROWS ONLY", exact, 0},
		{"SELECT fetch, first FROM orders", exact, 599380},
		// A bound not known here.
		{"SELECT * FROM orders LIMIT ?", exact, 0},
		{"SELECT * FROM orders LIMIT ?, 5000", exact, 0},
		{"SELECT * FROM orders LIMIT 5000 OFFSET ?", exact, 0},
		{"SELECT `limit` FROM orders", exact, 0}, // a column named limit is not told from the keyword
		// The result is not the rows read.
		{"SELECT count(*) FROM orders", exact, 0},
		{"SELECT status, count(*) FROM orders GROUP BY status", exact, 0},
		{"SELECT DISTINCT status FROM orders", exact, 0},
		{"SELECT max(id) FROM orders", exact, 0},
		{"SELECT id, row_number() OVER (ORDER BY id) FROM orders", exact, 0},
		{"SELECT /*+ MAX_EXECUTION_TIME(1) */ * FROM orders", exact, 0},
		{"SELECT /*!50000 DISTINCT */ status FROM orders", exact, 0},
		// The plan does not read only its result.
		{"SELECT * FROM orders", Plan{ResultRows: 599380}, 0},
	}
	for _, tc := range cases {
		if got := resultRows(tc.stmt, tc.p); got != tc.want {
			t.Errorf("resultRows(%q) = %d, want %d", tc.stmt, got, tc.want)
		}
	}
}

// MariaDB 11.4.13's own plan for `SELECT * FROM t HAVING v > 1 LIMIT 5`:
// one table read whole with no condition on it, and the filter on the
// block. Not a plan that reads only its result.
func TestReadIsResult_mariaDBHaving_2115(t *testing.T) {
	const plan = `{"query_block":{"select_id":1,"cost":0.0113438,"having_condition":"h2115.t.v > 1","nested_loop":[{"table":{"table_name":"t","access_type":"ALL","loops":1,"rows":3,"cost":0.0113438,"filtered":100}}]}}`
	p, err := ParsePlan([]byte(plan))
	if err != nil {
		t.Fatal(err)
	}
	if p.ReadIsResult {
		t.Error("a plan with a having_condition is said to read only its result")
	}
	// The same plan without it is one: the table read whole.
	p, err = ParsePlan([]byte(strings.Replace(plan, `"having_condition":"h2115.t.v > 1",`, "", 1)))
	if err != nil || !p.ReadIsResult {
		t.Errorf("the table read whole with no condition: reads only its result %v, %v; want true", p.ReadIsResult, err)
	}
}

// A function call the router does not know may be an aggregate: one row out
// of the whole table, which the plan does not show (an aggregate with no
// GROUP BY is one table read like any other). Only the calls on a short
// list of functions that return one value per row keep a statement under
// the two rules.
func TestCallsOnlyRowFunctions_2115(t *testing.T) {
	for stmt, want := range map[string]bool{
		"SELECT * FROM orders WHERE id > 5 LIMIT 500":                       true,
		"SELECT id, upper(note) FROM orders LIMIT 500":                      true,
		"SELECT concat(customer_id) FROM orders LIMIT 10":                   true,
		"SELECT CONCAT (a, b), coalesce(c, 0) FROM orders LIMIT 10":         true,
		"SELECT * FROM orders WHERE id IN (1, 2) AND (a = 1) LIMIT 10":      true,
		"SELECT * FROM orders FORCE INDEX (k_created) WHERE a > 1 LIMIT 10": true,
		"SELECT * FROM orders USE INDEX (k) IGNORE KEY (j) WHERE a > 1":     true,
		"SELECT 'myagg(x)' FROM orders /* myagg(x) */ LIMIT 10":             true,
		"SELECT `myagg`, a FROM orders LIMIT 10":                            true,
		"SELECT myagg(amount) FROM orders LIMIT 1":                          false,
		"SELECT myagg (amount) FROM orders LIMIT 1":                         false,
		"SELECT st_collect(g) FROM orders LIMIT 1":                          false,
		"SELECT db.myagg(amount) FROM orders LIMIT 1":                       false,
		"SELECT upper(myagg(amount)) FROM orders LIMIT 1":                   false,
		"SELECT id FROM orders WHERE a > 1 ORDER BY somefn(a) LIMIT 1":      false,
		"SELECT MYAGG(amount) FROM orders":                                  false,
		"SELECT `db`.`myagg`(amount) FROM orders":                           false,
	} {
		if got := callsOnlyRowFunctions(Scrub(stmt)); got != want {
			t.Errorf("callsOnlyRowFunctions(%q) = %v, want %v", stmt, got, want)
		}
	}
	// Through the policy, on the real plan of a table read whole (p18).
	raw, err := os.ReadFile(filepath.Join("testdata", "limits", "mysql84", "p18_select_expr_no_where.json"))
	if err != nil {
		t.Fatal(err)
	}
	p, err := ParsePlan(raw)
	if err != nil {
		t.Fatal(err)
	}
	pol := DefaultPolicy()
	if d := pol.DecideStatement("SELECT id, upper(note) FROM orders LIMIT 500", p); d.ToCopy || d.Rule != RuleBoundedLimit {
		t.Errorf("a row function in the select list: %+v, want the source by bounded_limit", d)
	}
	for _, stmt := range []string{"SELECT myagg(amount) FROM orders LIMIT 1", "SELECT st_collect(note) FROM orders LIMIT 1"} {
		if d := pol.DecideStatement(stmt, p); !d.ToCopy || d.ResultRows != 0 {
			t.Errorf("%s: %+v, want the copy tried as before, with no estimate of the result", stmt, d)
		}
	}
	if d := pol.DecideStatement("SELECT myagg(amount) FROM orders", p); !d.ToCopy || d.ResultRows != 0 {
		t.Errorf("an unknown function with no LIMIT: %+v, want no estimate of the result", d)
	}
	if d := pol.DecideStatement("SELECT id, upper(note) FROM orders", p); !d.ToCopy || d.ResultRows < 2000000 {
		t.Errorf("a row function with no LIMIT: %+v, want the table's rows as the estimate", d)
	}
}
