package readrouter

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/config"
)

// TestLimitBounded: a top-level LIMIT of a few rows with nothing that forces
// MySQL to read everything first (aggregate, GROUP BY, HAVING, DISTINCT,
// window, UNION, a second LIMIT in a derived table) bounds the rows
// produced; offsets count; a LIMIT inside a literal is not one; absurd
// numbers are never "a few".
func TestLimitBounded(t *testing.T) {
	cases := []struct {
		stmt string
		rows int64
		ok   bool
	}{
		{"SELECT id FROM orders ORDER BY id DESC LIMIT 2", 2, true},
		{"SELECT * FROM t LIMIT 10", 10, true},
		{"SELECT * FROM t WHERE a = 1 LIMIT 10", 10, true}, // bounded rows, not bounded work: the plan decides
		{"select * from t limit 5;", 5, true},
		{"SELECT *\nFROM t\nLIMIT\n  5", 5, true},
		{"SELECT * FROM t LIMIT 5 -- the first page", 5, true},
		{"SELECT max_col, min_col FROM t LIMIT 5", 5, true}, // columns named like aggregates
		{"SELECT a FROM t WHERE b = 1 ORDER BY c LIMIT 999 OFFSET 1", 1000, true},
		{"SELECT a FROM t LIMIT 10, 20", 30, true},
		{"SELECT a FROM t LIMIT 0", 0, true},
		{"SELECT a FROM t LIMIT 1000, 50", 0, false}, // offset counts: 1050 rows
		{"SELECT a FROM t LIMIT 50 OFFSET 1000", 0, false},
		{"SELECT a FROM t LIMIT 5000", 0, false},
		{"SELECT * FROM t LIMIT 999 OFFSET 99999999999999999999", 0, false}, // overflow is not a few
		{"SELECT * FROM t LIMIT 99999999999999999999, 5", 0, false},
		{"SELECT * FROM t LIMIT 9223372036854775807, 5", 0, false},
		{"SELECT count(*) FROM t LIMIT 1", 0, false},
		{"SELECT SQL_CALC_FOUND_ROWS * FROM t LIMIT 5", 0, false}, // reads the whole table past the LIMIT
		{"SELECT ANY_VALUE(a) FROM t LIMIT 5", 0, false},
		{"SELECT status, COUNT(*) FROM t GROUP BY status LIMIT 5", 0, false},
		{"SELECT status, COUNT(*) FROM t GROUP\n BY status LIMIT 5", 0, false},
		{"SELECT a FROM t HAVING a > 1 LIMIT 5", 0, false},
		{"SELECT DISTINCT a FROM t LIMIT 5", 0, false},
		{"SELECT DISTINCTROW a FROM t LIMIT 5", 0, false},
		{"SELECT a, row_number() OVER (ORDER BY b) FROM t LIMIT 5", 0, false},
		{"SELECT a, row_number() OVER w FROM t WINDOW w AS () LIMIT 5", 0, false}, // a named window
		{"SELECT a FROM t UNION SELECT a FROM u LIMIT 5", 0, false},
		{"SELECT a FROM t WHERE b IN (SELECT MIN(b) FROM t) LIMIT 5", 0, false}, // aggregate anywhere: conservative
		{"SELECT * FROM (SELECT a FROM t LIMIT 500000) x LIMIT 5", 0, false},    // the inner LIMIT is materialized whole
		{"WITH x AS (SELECT a FROM t LIMIT 500000) SELECT * FROM x LIMIT 5", 0, false},
		{"SELECT * FROM (SELECT a FROM t LIMIT 5) x", 0, false},      // not top-level
		{"SELECT * FROM t WHERE note = 'LIMIT 5'", 0, false},         // a literal
		{"SELECT a FROM t /*!50000 GROUP BY a */ LIMIT 5", 0, false}, // an executable comment may hide a GROUP BY
		{"SELECT * FROM t LIMIT 5 FOR UPDATE", 0, false},             // LIMIT does not end it
		{"SELECT * FROM t", 0, false},
	}
	for _, tc := range cases {
		rows, ok := limitBounded(tc.stmt)
		if ok != tc.ok || rows != tc.rows {
			t.Errorf("limitBounded(%q) = %d, %v; want %d, %v", tc.stmt, rows, ok, tc.rows, tc.ok)
		}
	}
}

// TestPrejudge: the one shape decided with no plan is a select list without
// parentheses FROM one table LIMIT a few, nothing else. Anything with a
// filter, a join, an order, a subquery or a function needs the plan.
func TestPrejudge(t *testing.T) {
	pol := DefaultPolicy()
	for stmt, want := range map[string]bool{
		"SELECT * FROM t LIMIT 10":                        true,
		"select a, b from shop.orders o limit 5;":         true,
		"SELECT a FROM `t` AS x LIMIT 10 OFFSET 20":       true,
		"SELECT *\n  FROM t\n  LIMIT 5 -- page":           true,
		"SELECT * FROM t WHERE a = 1 LIMIT 10":            false,
		"SELECT * FROM t JOIN u ON u.id = t.u_id LIMIT 5": false,
		"SELECT * FROM t, u LIMIT 5":                      false,
		"SELECT * FROM t ORDER BY id LIMIT 5":             false,
		"SELECT UPPER(a) FROM t LIMIT 5":                  false, // a function: parentheses
		"SELECT * FROM (SELECT a FROM t) x LIMIT 5":       false,
		"SELECT DISTINCT a FROM t LIMIT 5":                false,
		"SELECT count(*) FROM t LIMIT 1":                  false,
		"SELECT a FROM t /*!50000 WHERE a = 1 */ LIMIT 5": false,
		"SELECT * FROM t LIMIT 5000":                      false,
		"SELECT * FROM t LIMIT 5 FOR UPDATE":              false,
		"SELECT * FROM t":                                 false,
	} {
		d, ok := pol.Prejudge(stmt)
		if ok != want {
			t.Errorf("Prejudge(%q) = %v (%+v), want %v", stmt, ok, d, want)
		}
		if ok && (d.ToCopy || d.Rule != RuleBoundedLimit || !strings.Contains(d.Reason, "LIMIT")) {
			t.Errorf("Prejudge(%q) decided %+v, want MySQL under the bounded-limit rule", stmt, d)
		}
	}
}

// TestDecideStatement_limit pins the misroute sql-compare found on a real
// workload: `SELECT id FROM orders ORDER BY id DESC LIMIT 2` carries a plan
// cost of 272,233 (MySQL's cost ignores LIMIT) yet MySQL walks the primary
// key backwards for two rows in 0.2 ms, while the copy took 340 ms. The plan
// still tells: no filesort, rows_examined_per_scan 2. An ORDER BY that sorts
// the whole table, a filter MySQL must scan for, and any aggregation keep
// the cost rule and go to the copy.
func TestDecideStatement_limit(t *testing.T) {
	load := func(name string) Plan {
		t.Helper()
		raw, err := os.ReadFile(filepath.Join("testdata", name))
		if err != nil {
			t.Fatal(err)
		}
		p, err := ParsePlan(raw)
		if err != nil {
			t.Fatal(err)
		}
		return p
	}
	pol := DefaultPolicy()
	byPK := load("order_by_pk_limit2.json")
	if byPK.Filesort || byPK.Cost != 272233.65 || byPK.MaxScanRows != 2 {
		t.Fatalf("index-ordered plan parsed as %+v", byPK)
	}
	if d := pol.Decide(byPK); !d.ToCopy || d.Rule != RuleCost {
		t.Fatalf("plan alone decides %+v; the cost rule must still fire, the statement text is what saves it", d)
	}
	stmt := "SELECT id FROM orders ORDER BY id DESC LIMIT 2"
	if d := pol.DecideStatement(stmt, byPK); d.ToCopy || d.Rule != RuleBoundedLimit || !strings.Contains(d.Reason, "LIMIT 2") {
		t.Errorf("index-ordered LIMIT 2: DecideStatement = %+v, want MySQL under the bounded-limit rule", d)
	}
	if _, ok := pol.Prejudge(stmt); ok {
		t.Error("an ORDER BY was prejudged without a plan")
	}
	sorted := load("order_by_filesort_limit20.json")
	if !sorted.Filesort || sorted.FullScans != 1 {
		t.Fatalf("filesort plan parsed as %+v", sorted)
	}
	if d := pol.DecideStatement("SELECT * FROM orders ORDER BY created_at DESC LIMIT 20", sorted); !d.ToCopy {
		t.Errorf("ORDER BY with a filesort over 2.6M rows: DecideStatement = %+v, want the copy", d)
	}
	// A filter no index serves: MySQL scans until it has 10 matches, and
	// the plan says so (ALL over the table). Bounded rows, unbounded work.
	scanned := Plan{Cost: 272233, Tables: 1, FullScans: 1, MaxScanRows: 2654806, ScanFilter: true}
	if d := pol.DecideStatement("SELECT * FROM orders WHERE note = 'x' LIMIT 10", scanned); !d.ToCopy {
		t.Errorf("unindexed filter with LIMIT 10: DecideStatement = %+v, want the copy", d)
	}
	// The trap: the same filter under an index-served ORDER BY. MySQL walks
	// the primary key backwards and GUESSES rows_examined_per_scan as the
	// LIMIT over the filter's selectivity (20 here); a rare or absent
	// value makes it walk all 2.6M rows. The attached_condition on the
	// scanned table is what disqualifies it, not the row estimate.
	filtered := load("order_by_pk_limit2_filtered.json")
	if !filtered.ScanFilter || filtered.Filesort || filtered.MaxScanRows != 20 {
		t.Fatalf("filtered index-order plan parsed as %+v", filtered)
	}
	if d := pol.DecideStatement("SELECT * FROM orders WHERE note = 'x' ORDER BY id DESC LIMIT 2", filtered); !d.ToCopy || d.Rule != RuleCost {
		t.Errorf("unindexed filter under an index order: DecideStatement = %+v, want the copy under the cost rule", d)
	}
	if byPK.ScanFilter {
		t.Error("the unfiltered index-order plan reports a scan filter")
	}
	// The join form of the trap: the filter sits on the JOINED table (an
	// eq_ref with the attached_condition) while the scanned one has none,
	// and MySQL's guess for the driving table is still the LIMIT over the
	// join's assumed selectivity. The fixture follows MySQL 8.4's
	// nested_loop shape; it was not captured live.
	joined := load("order_by_pk_limit2_join_filtered.json")
	if !joined.ScanFilter || joined.Filesort || joined.Tables != 2 || joined.MaxScanRows != 20 {
		t.Fatalf("join-filtered index-order plan parsed as %+v", joined)
	}
	if d := pol.DecideStatement("SELECT o.* FROM orders o JOIN customers c ON c.id = o.customer_id WHERE c.country = 'XX' ORDER BY o.id DESC LIMIT 2", joined); !d.ToCopy || d.Rule != RuleCost {
		t.Errorf("filter on the joined table under an index order: DecideStatement = %+v, want the copy under the cost rule", d)
	}
	// A filter an index serves, on a small estimate: MySQL's.
	indexed := Plan{Cost: 808.75, Tables: 1, MaxScanRows: 400}
	if d := pol.DecideStatement("SELECT * FROM orders WHERE customer_id = 7 LIMIT 10", indexed); d.ToCopy || d.Rule != RuleBoundedLimit {
		t.Errorf("indexed filter with LIMIT 10: DecideStatement = %+v, want MySQL under the bounded-limit rule", d)
	}
	// The same rows sorted: MySQL's too (400 rows are cheap), but under the
	// plan's own rule, since "served without a sort" would be false.
	sortedFew := Plan{Cost: 808.75, Tables: 1, MaxScanRows: 400, Filesort: true}
	if d := pol.DecideStatement("SELECT * FROM orders WHERE customer_id = 7 ORDER BY created_at LIMIT 10", sortedFew); d.ToCopy || d.Rule != RuleCheap {
		t.Errorf("sorted LIMIT over 400 rows: DecideStatement = %+v, want MySQL under the cheap rule", d)
	}
	grouped := load("group_by_full_scan.json")
	if d := pol.DecideStatement("SELECT status, count(*) FROM orders GROUP BY status LIMIT 5", grouped); !d.ToCopy {
		t.Errorf("GROUP BY with a LIMIT: DecideStatement = %+v, want the copy (the LIMIT bounds nothing)", d)
	}
	trivial := Plan{Message: "no matching row in const table"}
	if d := pol.DecideStatement("SELECT * FROM orders WHERE id = 1 LIMIT 1", trivial); d.ToCopy || d.Rule != RuleTrivial {
		t.Errorf("trivial plan with a LIMIT: DecideStatement = %+v, want the trivial rule", d)
	}
}

// The bare shape is decided before any connection to the source: no EXPLAIN
// round trip, and no upstream needed. A filter makes the plan necessary, so
// the source is dialled (and, here, lost).
func TestForwarder_bareLimitDecidedWithoutExplain(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	var accepted atomic.Int32
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			accepted.Add(1)
			c.Close()
		}
	}()
	f, err := NewForwarder("u:p@tcp("+ln.Addr().String()+")/db?tls=false", config.SSL{Mode: "disabled"}, DefaultPolicy(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	f.connectTimeout = time.Second
	d, err := f.Decide(context.Background(), "SELECT * FROM t LIMIT 10")
	if err != nil || d.ToCopy || d.Rule != RuleBoundedLimit {
		t.Fatalf("Decide = %+v, %v; want MySQL under the bounded-limit rule and no error", d, err)
	}
	if n := accepted.Load(); n != 0 {
		t.Errorf("the source was dialled %d times for a statement decided on its text", n)
	}
	if _, err := f.Decide(context.Background(), "SELECT * FROM t WHERE a = 1 LIMIT 10"); !IsLost(err) {
		t.Errorf("filtered LIMIT without a reachable source: err = %v, want the lost error (the plan was needed)", err)
	}
}
