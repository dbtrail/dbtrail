package sqlcompare

import (
	"strings"
	"testing"
)

func s(v string) *string { return &v }

func rows(cols []string, r ...[]*string) Rows { return Rows{Columns: cols, Rows: r} }

func TestParseStatements(t *testing.T) {
	in := `-- a comment
SELECT 1;

# another
SELECT a,
       b
  FROM t
 WHERE x = ';';
SELECT 1;
SELECT 2
`
	got, err := ParseStatements(strings.NewReader(in))
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"SELECT 1", "SELECT a,\n       b\n  FROM t\n WHERE x = ';'", "SELECT 2"}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Errorf("got %q, want %q", got, want)
	}
	// A comment after the `;` on the same line, a `;` inside a literal that
	// spans lines, a `;` inside a backtick name, and MySQL's executable and
	// hint comments kept while ordinary ones go.
	got, err = ParseStatements(strings.NewReader("SELECT 1; -- note\nSELECT 'a;\nb'; SELECT `x;y` FROM t; SELECT /*+ NO_INDEX(t) */ /* drop me */ 2; /*!50001 SELECT 3 */;"))
	if err != nil {
		t.Fatal(err)
	}
	want = []string{"SELECT 1", "SELECT 'a;\nb'", "SELECT `x;y` FROM t", "SELECT /*+ NO_INDEX(t) */   2", "/*!50001 SELECT 3 */"}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Errorf("got %q, want %q", got, want)
	}
	if _, err := ParseStatements(strings.NewReader("-- only comments\n\n")); err == nil {
		t.Error("an empty file was accepted")
	}
}

func TestCompare(t *testing.T) {
	cols := []string{"a", "b"}
	cases := []struct {
		name     string
		src, cp  Rows
		ordered  bool
		verdict  Verdict
		kind     string
		contains string
	}{
		{"equal ordered", rows(cols, []*string{s("1"), s("x")}), rows(cols, []*string{s("1"), s("x")}), true, Equal, "", ""},
		{"equal unordered", rows(cols, []*string{s("1"), s("x")}, []*string{s("2"), s("y")}), rows(cols, []*string{s("2"), s("y")}, []*string{s("1"), s("x")}), false, Equal, "", ""},
		{"order matters", rows(cols, []*string{s("1"), s("x")}, []*string{s("2"), s("y")}), rows(cols, []*string{s("2"), s("y")}, []*string{s("1"), s("x")}), true, Different, "order", "different order"},
		{"null order", rows(cols, []*string{nil, s("n")}, []*string{s("1"), s("x")}), rows(cols, []*string{s("1"), s("x")}, []*string{nil, s("n")}), true, Different, "order", "NULL first"},
		{"row count", rows(cols, []*string{s("1"), s("x")}), rows(cols), false, Different, "rows", "1 rows, copy 0"},
		{"columns", rows(cols, []*string{s("1"), s("x")}), rows([]string{"a"}, []*string{s("1")}), false, Different, "columns", "2 columns, copy 1"},
		{"case", rows(cols, []*string{s("1"), s("Paid")}), rows(cols, []*string{s("1"), s("paid")}), true, Different, "case", "collation"},
		{"precision", rows(cols, []*string{s("1"), s("1.6667")}), rows(cols, []*string{s("1"), s("1.6666666666666667")}), true, Different, "precision", "4 decimals"},
		{"null vs value", rows(cols, []*string{s("1"), nil}), rows(cols, []*string{s("1"), s("")}), true, Different, "null", "NULL"},
		{"text", rows(cols, []*string{s("1"), s("2026-01-01 10:00:00")}), rows(cols, []*string{s("1"), s("2026-01-01 12:00:00")}), true, Different, "text", "source"},
		{"null is not empty string (unordered)", rows(cols, []*string{s("1"), nil}), rows(cols, []*string{s("1"), s("")}), false, Different, "rows", "NULL"},
		{"a separator inside a cell cannot alias two rows", rows(cols, []*string{s("x3:y"), s("z")}), rows(cols, []*string{s("x"), s("y1:z")}), false, Different, "rows", ""},
		{"numbers that merely look close are text when not numeric", rows(cols, []*string{s("1"), s("1e")}), rows(cols, []*string{s("1"), s("1f")}), true, Different, "text", ""},
		{"numbers apart beyond 4 decimals are a value difference, not precision", rows(cols, []*string{s("1"), s("1.5")}), rows(cols, []*string{s("1"), s("1.6")}), true, Different, "text", ""},
		{"a 5th-decimal difference is precision", rows(cols, []*string{s("1"), s("100.00001")}), rows(cols, []*string{s("1"), s("100.00002")}), true, Different, "precision", ""},
		{"small numbers twice each other are a value difference", rows(cols, []*string{s("1"), s("0.00001")}), rows(cols, []*string{s("1"), s("0.00002")}), true, Different, "text", ""},
		{"4 units apart on big numbers is a value difference", rows(cols, []*string{s("1"), s("100000")}), rows(cols, []*string{s("1"), s("100004")}), true, Different, "text", ""},
		// Unordered: the pair named is the source-only row and the copy-only
		// row that differ in one cell, not the pair at the same sorted position.
		{"unordered case pair", rows(cols, []*string{s("B"), s("1")}, []*string{s("a"), s("2")}), rows(cols, []*string{s("b"), s("1")}, []*string{s("a"), s("2")}), false, Different, "case", `source "B", copy "b"`},
		{"unordered missing and extra", rows(cols, []*string{s("1"), s("x")}, []*string{s("2"), s("x")}, []*string{s("3"), s("x")}), rows(cols, []*string{s("1"), s("x")}, []*string{s("3"), s("x")}, []*string{s("4"), s("x")}), false, Different, "rows", `only in the source (first: ("2", "x")); 1 only in the copy (first: ("4", "x"))`},
	}
	// #2111: the same columns in another order is a difference of its own,
	// whatever the cells hold. Before, only the COUNT was compared: with equal
	// cells in the swapped columns, or no rows, the two answers read as equal.
	one, two := s("1"), s("1")
	cases = append(cases, []struct {
		name     string
		src, cp  Rows
		ordered  bool
		verdict  Verdict
		kind     string
		contains string
	}{
		{"same names, another order, cells that happen to agree", rows([]string{"id", "amount"}, []*string{one, two}), rows([]string{"amount", "id"}, []*string{one, two}), false, Different, "columns", "id is column 1 on the source and column 2 on the copy"},
		{"same names, another order, no rows", rows([]string{"id", "amount"}), rows([]string{"amount", "id"}), true, Different, "columns", "on the source and column"},
		{"same names, another order, different cells", rows([]string{"id", "amount"}, []*string{s("1"), s("9.50")}), rows([]string{"amount", "id"}, []*string{s("9.50"), s("1")}), true, Different, "columns", "on the source and column"},
		{"names differ only in case", rows([]string{"ID", "Amount"}, []*string{one, two}), rows([]string{"id", "amount"}, []*string{one, two}), true, Equal, "", ""},
		{"a name twice, as a join returns it", rows([]string{"id", "id", "x"}, []*string{one, two, one}), rows([]string{"id", "x", "id"}, []*string{one, two, one}), true, Different, "columns", "on the source and column"},
		// An expression is named differently by each side: that is not an
		// order, and the cells decide.
		{"different names are not an order", rows([]string{"count(*)"}, []*string{one}), rows([]string{"count_star()"}, []*string{one}), true, Equal, "", ""},
		// One expression named differently by each side must not switch the
		// check off for the columns both sides do name.
		{"reordered columns beside an expression, no rows", rows([]string{"id", "zeta", "alpha", "count(*) OVER ()"}), rows([]string{"alpha", "id", "zeta", "count_star() OVER ()"}), false, Different, "columns", "id is column 1 on the source and column 2 on the copy"},
		{"swapped columns whose cells agree, beside an expression", rows([]string{"b", "a", "n+1"}, []*string{one, two, one}), rows([]string{"a", "b", "(n + 1)"}, []*string{one, two, one}), true, Different, "columns", "b is column 1 on the source and column 2 on the copy"},
		{"another set of the same size, no rows", rows([]string{"id", "twice", "a"}), rows([]string{"id", "a", "secret"}), false, Different, "columns", "a is column 3 on the source and column 2 on the copy"},
		{"a column each side names differently, no rows", rows([]string{"id", "twice"}), rows([]string{"id", "secret"}), false, Different, "columns", "column 2 is twice on the source and secret on the copy"},
		{"a name on one side twice", rows([]string{"id", "id"}, []*string{one, two}), rows([]string{"id", "x"}, []*string{one, two}), true, Different, "columns", "id is 2 column(s) on the source and 1 on the copy"},
		// What stays EQUAL: each side names an expression its own way, and
		// MySQL names a literal by its value where DuckDB quotes it.
		{"an aggregate named by each engine", rows([]string{"id", "COUNT(*)"}, []*string{one, two}), rows([]string{"id", "count_star()"}, []*string{one, two}), true, Equal, "", ""},
		{"a string literal: abc on MySQL, 'abc' on DuckDB", rows([]string{"id", "abc", "TRUE"}, []*string{one, two, one}), rows([]string{"id", "'abc'", "CAST('t' AS BOOLEAN)"}, []*string{one, two, one}), true, Equal, "", ""},
		{"an expression against an alias-less column on the other side", rows([]string{"id", "SUM(a)"}, []*string{one, two}), rows([]string{"id", "sum(a)"}, []*string{one, two}), true, Equal, "", ""},
	}...)
	for _, tc := range cases {
		v, kind, detail := Compare(tc.src, tc.cp, tc.ordered)
		if v != tc.verdict || kind != tc.kind || !strings.Contains(detail, tc.contains) {
			t.Errorf("%s: got %s/%s %q, want %s/%s containing %q", tc.name, v, kind, detail, tc.verdict, tc.kind, tc.contains)
		}
	}
}

func TestReportText(t *testing.T) {
	rep := &Report{Counts: map[Verdict]int{Different: 1, Equal: 1}, CopyDiffers: 1, Results: []Result{
		{Statement: "SELECT status, count(*) FROM orders GROUP BY status", Verdict: Different, Kind: "rows", Detail: "source returned 1 rows, copy 2", Route: "copy", RouteReason: "plan cost 20146 >= 10000", SourceRows: 1, CopyRows: 2},
		{Statement: "SELECT 1", Verdict: Equal, Route: "mysql", RouteReason: "trivial plan"},
	}}
	var b strings.Builder
	WriteText(&b, rep)
	out := b.String()
	for _, want := range []string{"DIFFERENT    router=copy", "rows: source returned 1 rows, copy 2", "2 statements: EQUAL 1 DIFFERENT 1", "1 statement(s) the router would send to the copy answer DIFFERENTLY"} {
		if !strings.Contains(out, want) {
			t.Errorf("report lacks %q:\n%s", want, out)
		}
	}
	if !rep.Failed() {
		t.Error("a report with a copy-routed difference did not fail")
	}
}

func TestTopLevelOrderBy(t *testing.T) {
	cases := map[string]bool{
		"SELECT * FROM t ORDER BY id":                                      true,
		"select * from t order\n by id limit 3":                            true,
		"SELECT id, ROW_NUMBER() OVER (PARTITION BY g ORDER BY id) FROM t": false,
		"SELECT * FROM (SELECT * FROM t ORDER BY id) x":                    false,
		"SELECT GROUP_CONCAT(a ORDER BY a) FROM t":                         false,
		"SELECT * FROM t WHERE note = 'order by me'":                       false,
		"SELECT * FROM t -- order by id\n":                                 false,
		"SELECT * FROM (SELECT 1) x ORDER BY 1":                            true,
		"SELECT border_by FROM t":                                          false,
	}
	for stmt, want := range cases {
		if got := TopLevelOrderBy(stmt); got != want {
			t.Errorf("TopLevelOrderBy(%q) = %v, want %v", stmt, got, want)
		}
	}
}

func TestReadOnlyVeto(t *testing.T) {
	vetoed := map[string]string{
		"WITH c AS (SELECT id FROM orders) DELETE FROM orders WHERE id IN (SELECT id FROM c)": "a WITH that writes",
		"WITH c AS (SELECT 1) UPDATE t SET a = 1":                                             "a WITH that writes",
		"SELECT * FROM t INTO OUTFILE '/tmp/x'":                                               "SELECT ... INTO",
		"SELECT a INTO @v FROM t":                                                             "SELECT ... INTO",
		"SELECT GET_LOCK('x', 10)":                                                            "side effects",
		"SELECT SLEEP(5)":                                                                     "side effects",
		"SELECT * FROM t FOR UPDATE":                                                          "a locking read",
		"SELECT 1; DROP TABLE t":                                                              "more than one statement",
	}
	for stmt, want := range vetoed {
		if got := ReadOnlyVeto(stmt); !strings.Contains(got, want) {
			t.Errorf("ReadOnlyVeto(%q) = %q, want %q", stmt, got, want)
		}
	}
	for _, stmt := range []string{
		"WITH c AS (SELECT id FROM orders WHERE note = 'delete me') SELECT * FROM c",
		"SELECT * FROM t WHERE note = 'a; b'",
		"SELECT sleeping, benchmark_id FROM t", // not the functions
		"SELECT (SELECT count(*) FROM u) AS n FROM t",
	} {
		if got := ReadOnlyVeto(stmt); got != "" {
			t.Errorf("ReadOnlyVeto(%q) = %q, want none", stmt, got)
		}
	}
}

// The copy is sent what the router would send it: backtick-quoted names in
// double quotes (#2081), and the client's text when the rewrite refuses it.
func TestCopyStatement(t *testing.T) {
	for stmt, want := range map[string]string{
		"SELECT `status`, count(*) FROM `orders` GROUP BY `status`": `SELECT "status", count(*) FROM "orders" GROUP BY "status"`,
		"SELECT status FROM orders WHERE note = 'a `b`'":            "SELECT status FROM orders WHERE note = 'a `b`'",
		"SELECT `a``b` FROM `orders`":                               "SELECT `a``b` FROM `orders`",
	} {
		if got := copyStatement(stmt); got != want {
			t.Errorf("copyStatement(%q) = %q, want %q", stmt, got, want)
		}
	}
}
