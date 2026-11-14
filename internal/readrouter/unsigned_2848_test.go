package readrouter

import (
	"strings"
	"testing"
)

// #2848: arithmetic on an UNSIGNED whole-number column is kept on MySQL.
// MySQL computes it as a BIGINT UNSIGNED and refuses a negative result with
// error 1690; the copy answers another type and, for a negative result, a
// number. Comparisons are not arithmetic and stay on the copy.
func TestUnsignedVeto_2848(t *testing.T) {
	cols := []string{"u", "ui", "ti"}
	cases := []struct {
		stmt string
		cols []string
		star bool
		veto bool
		why  string
	}{
		{"SELECT b - u FROM ev", cols, false, true, "the issue's case"},
		{"SELECT u - b FROM ev", cols, false, true, "the column first"},
		{"SELECT b + u, u * b FROM ev", cols, false, true, "+ and *"},
		{"SELECT 2*ui FROM ev", cols, false, true, "a product, no spaces"},
		{"SELECT ui % 2 FROM ev", cols, false, true, "%"},
		{"SELECT ui MOD 2 FROM ev", cols, false, true, "MOD as an operator"},
		{"SELECT 7 mod ui FROM ev", cols, false, true, "MOD before the column"},
		{"SELECT MOD(ui, 2) FROM ev", cols, false, true, "MOD called"},
		{"SELECT ui DIV 2, ui / 2 FROM ev", cols, false, true, "DIV and /"},
		{"SELECT -u FROM ev", cols, false, true, "unary minus: wraps around on the copy"},
		{"SELECT - ui FROM ev", cols, false, true, "unary minus, a space"},
		{"SELECT (-ti) FROM ev", cols, false, true, "unary minus in parentheses"},
		{"SELECT u + ? FROM ev", cols, false, true, "a placeholder"},
		{"SELECT u * -1, u + -5 FROM ev", cols, false, true, "a negative literal"},
		{"SELECT ui\t-\n4 FROM ev", cols, false, true, "tab and newline"},
		{"SELECT ui\v- 4 FROM ev", cols, false, true, "a vertical tab"},
		{"SELECT UI - 4 FROM ev", cols, false, true, "upper case in the statement"},
		{"SELECT ui - 4 FROM ev", []string{"UI"}, false, true, "upper case in the table"},
		{"SELECT `ui` - 4 FROM ev", cols, false, true, "quoted"},
		{"SELECT ev.ui - 4 FROM ev", cols, false, true, "qualified"},
		{"SELECT 4 - db.ev.`ui` FROM db.ev", cols, false, true, "qualified by schema and table, quoted, after the minus"},
		{"SELECT (ui) - 4 FROM ev", cols, false, true, "in parentheses"},
		{"SELECT ABS(ui) - 4 FROM ev", cols, false, true, "under a call"},
		{"SELECT COALESCE(ui, 0) * 2 FROM ev", cols, false, true, "under COALESCE"},
		{"SELECT CASE WHEN k > 0 THEN ui END - 4 FROM ev", cols, false, true, "under CASE"},
		{"SELECT MAX(ui) OVER () - 4 FROM ev", cols, false, true, "under a window"},
		{"SELECT MAX(ui) OVER w - 4 FROM ev WINDOW w AS ()", cols, false, true, "under a named window"},
		{"SELECT SUM(ui - 1) FROM ev", cols, false, true, "arithmetic inside SUM"},
		{"SELECT ui + INTERVAL 1 DAY FROM ev", cols, false, true, "INTERVAL after a column"},
		{"SELECT ui AS x FROM ev ORDER BY x - 4", cols, false, true, "an alias in ORDER BY"},
		{"SELECT MAX(ui) AS x FROM ev GROUP BY k HAVING x - 100 > 0", cols, false, true, "an alias in HAVING"},
		{"SELECT x - 1 FROM (SELECT u AS x FROM ev) q", cols, false, true, "an alias in a derived table"},
		{"WITH q AS (SELECT u AS x FROM ev) SELECT x * 2 FROM q", cols, false, true, "an alias in a WITH"},
		{"SELECT a - 1 FROM (SELECT * FROM ev) q (a)", cols, false, true, "a star in a derived table"},
		{"SELECT a - 1 FROM (SELECT id FROM ev) q", cols, true, true, "a star the parse saw, in a derived table"},

		{"SELECT u, ui, ti FROM ev", cols, false, false, "the columns alone"},
		{"SELECT u FROM ev WHERE u > -1", cols, false, false, "compared with a negative literal"},
		{"SELECT u FROM ev WHERE u BETWEEN -5 AND 5", cols, false, false, "BETWEEN a negative"},
		{"SELECT u FROM ev WHERE u IN (-1, 3) AND u > b", cols, false, false, "IN, and a signed column"},
		{"SELECT u FROM ev WHERE u = 1e-5", cols, false, false, "an exponent"},
		{"SELECT IF(k > 4, u, b), GREATEST(u, b) FROM ev", cols, false, false, "merged, no arithmetic"},
		{"SELECT k - 1, b + 1 FROM ev WHERE u > 2", cols, false, false, "signed arithmetic beside an unsigned filter"},
		{"SELECT SUM(ui) - 100, COUNT(ui) - 100 FROM ev", cols, false, false, "SUM and COUNT: measured equal"},
		{"SELECT SUM(CASE WHEN k > 0 THEN ui END) - 1 FROM ev", cols, false, false, "a CASE under SUM"},
		{"SELECT *, k - 1 FROM ev", cols, false, false, "a star with no subquery"},
		{"SELECT COUNT(*) * 2 FROM ev", cols, false, false, "a product of COUNT, no column named"},
		{"SELECT uix - 1, u_2 * 2, xu + 1 FROM ev", cols, false, false, "longer names that hold the name"},
		{"SELECT name FROM ev WHERE name = 'u - 1'", cols, false, false, "inside a string"},
		{"SELECT ui /* - 1 */ FROM ev", cols, false, false, "inside a comment"},
		{"SELECT `a-b`, ui FROM ev", cols, false, false, "a minus inside a quoted name"},
		{"SELECT ui.k - 1 FROM ev ui", cols, false, false, "a table alias called the same, its own column signed"},
		{"SELECT u - 1 FROM ev", nil, false, false, "no UNSIGNED column"},
		{"SELECT año - 1 FROM t", []string{"año"}, false, true, "a name outside ASCII"},
		{"SELECT ano - 1 FROM t", []string{"año"}, false, false, "an accent is not dropped"},
		{"SELECT x FROM t", []string{"my col"}, false, true, "a name that cannot be looked for"},
	}
	for _, c := range cases {
		got := ShapeOf(c.stmt).UnsignedVeto(c.cols, c.star)
		if (got != "") != c.veto {
			t.Errorf("%q over %q (%s): %q, want veto=%v", c.stmt, c.cols, c.why, got, c.veto)
		}
	}
	if got := UnsignedVeto("", cols, false); got == "" {
		t.Error("a statement with no shape: no veto, want one (not known is never nothing to find)")
	}
	if got := UnsignedVeto("SELECT \xff FROM ev", cols, false); got == "" {
		t.Error("a statement that is not UTF-8: no veto, want one")
	}
	if got := UnsignedVeto("", nil, false); got != "" {
		t.Errorf("no UNSIGNED column and no shape: %q, want none", got)
	}
	if got := ShapeOf("SELECT b - `ui` FROM ev").UnsignedVeto(cols, false); !strings.Contains(got, "ui") || !strings.Contains(got, "UNSIGNED") || !strings.Contains(got, "1690") {
		t.Errorf("reason %q, want it to name ui, UNSIGNED and error 1690", got)
	}
}

// ROW_NUMBER, RANK, DENSE_RANK and NTILE are BIGINT UNSIGNED on MySQL:
// arithmetic on one is kept there, the call alone is not.
func TestWindowNumberVeto_2848(t *testing.T) {
	cases := []struct {
		stmt string
		veto bool
		why  string
	}{
		{"SELECT ROW_NUMBER() OVER (ORDER BY k) - 10 FROM ev", true, "the measured case"},
		{"SELECT row_number ()over() + 1 FROM ev", true, "lower case, spaces"},
		{"SELECT RANK() OVER w * 2 FROM ev WINDOW w AS (ORDER BY k)", true, "RANK, a named window"},
		{"SELECT DENSE_RANK() OVER () % 2 FROM ev", true, "DENSE_RANK"},
		{"SELECT 10 - NTILE(3) OVER () FROM ev", true, "NTILE, after the minus"},
		{"SELECT ROW_NUMBER\v() OVER () - 1 FROM ev", true, "a vertical tab before the parenthesis"},
		{"SELECT (ROW_NUMBER() OVER ()) - 1 FROM ev", true, "in parentheses"},
		{"SELECT CASE WHEN k > 0 THEN RANK() OVER () END - 1 FROM ev", true, "under CASE"},
		{"SELECT ROW_NUMBER() OVER () AS rn FROM ev ORDER BY rn - 1", true, "an alias in ORDER BY"},
		{"SELECT rn - 1 FROM (SELECT ROW_NUMBER() OVER () AS rn FROM ev) q", true, "an alias in a derived table"},

		{"SELECT ROW_NUMBER() OVER (ORDER BY k) FROM ev", false, "alone"},
		{"SELECT ROW_NUMBER() OVER (PARTITION BY k - 1) FROM ev", false, "arithmetic inside the window, on a signed column"},
		{"SELECT ROW_NUMBER() OVER (ORDER BY k - 1) FROM ev", true, "the same after ORDER BY, where an alias could stand: costs a statement, never a wrong answer"},
		{"SELECT rn FROM (SELECT ROW_NUMBER() OVER () AS rn FROM ev) q WHERE rn BETWEEN -1 AND 5", false, "compared, in a derived table"},
		{"SELECT k - 1, RANK() OVER () FROM ev", false, "arithmetic elsewhere"},
		{"SELECT \"rank\" - 1 FROM ev", false, "a quoted name called rank"},
		{"SELECT my_rank(k) - 1 FROM ev", false, "a function whose name holds rank"},
		{"SELECT 'rank() - 1' FROM ev", false, "inside a string"},
	}
	for _, c := range cases {
		got := windowNumberVeto(Shape(c.stmt))
		if (got != "") != c.veto {
			t.Errorf("%q (%s): %q, want veto=%v", c.stmt, c.why, got, c.veto)
		}
	}
	// VetoOpts asks it, last.
	if got := VetoOpts("SELECT ROW_NUMBER() OVER (ORDER BY k) - 10 FROM ev", CopyOptions{}); got != vetoWindowNumber {
		t.Errorf("VetoOpts: %q, want %q", got, vetoWindowNumber)
	}
	if got := VetoOpts("SELECT ROW_NUMBER() OVER (ORDER BY k) FROM ev", CopyOptions{}); got != "" {
		t.Errorf("VetoOpts, the call alone: %q, want none", got)
	}
}

// The checks read the text in work that grows with its length and no faster.
func TestUnsignedVetoLinear_2848(t *testing.T) {
	cols := []string{"u", "ui"}
	shapes := map[string]func(n int) string{
		"many names":   func(n int) string { return "SELECT " + strings.Repeat("u, `ui`, t.u, ", n) + "1 FROM t ORDER BY 1" },
		"many groups":  func(n int) string { return "SELECT " + strings.Repeat("f(u), (ui), ", n) + "1 FROM t" },
		"many windows": func(n int) string { return "SELECT " + strings.Repeat("row_number() over (), ", n) + "1 FROM t" },
	}
	for name, build := range shapes {
		_, small := unsignedVetoWork(Shape(build(2000)), cols, false)
		_, large := unsignedVetoWork(Shape(build(8000)), cols, false)
		_, wsmall := windowNumberVetoWork(Shape(build(2000)))
		_, wlarge := windowNumberVetoWork(Shape(build(8000)))
		for _, w := range [][2]int{{small, large}, {wsmall, wlarge}} {
			ratio := float64(w[1]) / float64(w[0])
			t.Logf("%-12s %9d steps at 2,000, %9d at 8,000: x%.2f", name, w[0], w[1], ratio)
			if w[0] == 0 || ratio > 4.5 {
				t.Errorf("%s: %d steps at 2,000 and %d at 8,000 (x%.2f): the work grows faster than the text", name, w[0], w[1], ratio)
			}
		}
	}
	const depth = 2000
	for name, stmt := range map[string]string{
		"nested":  "SELECT " + strings.Repeat("f(", depth) + "u" + strings.Repeat(")", depth) + " - 1 FROM t",
		"windows": "SELECT " + strings.Repeat("max(", depth) + "u" + strings.Repeat(") over ()", depth) + " * 2 FROM t",
		"case":    "SELECT 1 + " + strings.Repeat("case when a then ", depth) + "ui" + strings.Repeat(" end", depth) + " FROM t",
	} {
		if got := UnsignedVeto(Shape(stmt), cols, false); got == "" {
			t.Errorf("%s with arithmetic at the far end: not kept back", name)
		}
	}
	big := "SELECT " + strings.Repeat("x, ", 300000) + "u FROM t"
	if got := UnsignedVeto(Shape(big), cols, false); got != "" {
		t.Errorf("a million-byte statement with no arithmetic: %q", got)
	}
}
