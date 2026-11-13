package sqlsandbox

import (
	"reflect"
	"testing"
)

// #2611: under routing MySQL's DATE_FORMAT reaches the copy as strftime, and
// a report sorts or groups by what it writes. A format whose every text has
// the same length and the same characters outside its specifiers sorts as
// MySQL sorts it, so its literal is not read as a string the statement puts
// in order. Any other format, and every other literal, is read as before.
func TestCollectRefs_textUseStrftime_2611(t *testing.T) {
	c := func(n string) ColumnRef { return ColumnRef{Name: n} }
	inExpr := []OrderedLiteral{{Expression: true}}
	cases := []struct {
		sql  string
		want TextUse
	}{
		// The issue's report: an ORDER BY of the alias of a fixed format.
		{"SELECT strftime(created_at, '%Y-%m') AS month, count(*) FROM orders GROUP BY month ORDER BY month",
			TextUse{Ordered: []ColumnRef{c("created_at")}, Sorted: []ColumnRef{c("created_at")}, Aliases: []string{"month"}, Wrapped: []ColumnRef{c("created_at"), c("created_at")}}},
		{"SELECT strftime(d, '%Y-%m-%d %H:%M:%S') FROM t ORDER BY 1",
			TextUse{Ordered: []ColumnRef{c("d")}, Sorted: []ColumnRef{c("d")}, Wrapped: []ColumnRef{c("d"), c("d")}}},
		{"SELECT strftime(d, '%a %b %p, week %U/%V of %G, day %j %w, %f, 100%%') FROM t ORDER BY 1",
			TextUse{Ordered: []ColumnRef{c("d")}, Sorted: []ColumnRef{c("d")}, Wrapped: []ColumnRef{c("d"), c("d")}}},
		// Compared with anything but its own texts, the format is a string
		// with a sign again: a literal, another format, a CASE of two, a
		// set operation, an aggregate that picks by order.
		{"SELECT max(strftime(d, '%Y/%m')) FROM t", TextUse{Ordered: []ColumnRef{c("d")}, Wrapped: []ColumnRef{c("d")}, OrderedLiterals: inExpr}},
		{"SELECT id FROM t WHERE strftime(dt, '%H:%M') < '1045'",
			TextUse{Ordered: []ColumnRef{c("dt")}, Wrapped: []ColumnRef{c("dt")}, OrderedLiterals: inExpr}},
		{"SELECT id FROM t WHERE strftime(dt, '%H:%M') < strftime(d, '%Y')",
			TextUse{Ordered: []ColumnRef{c("dt"), c("d")}, Wrapped: []ColumnRef{c("dt"), c("d")}, OrderedLiterals: inExpr}},
		{"SELECT CASE WHEN k THEN strftime(a, '%H:%M') ELSE strftime(b, '%Y') END AS x FROM t ORDER BY x",
			TextUse{Ordered: []ColumnRef{c("a"), c("k"), c("b")}, Sorted: []ColumnRef{c("a"), c("k"), c("b")}, Aliases: []string{"x"},
				Wrapped: []ColumnRef{c("a"), c("b"), c("a"), c("b")}, OrderedLiterals: inExpr}},
		{"SELECT strftime(a, '%H:%M') AS x FROM t UNION ALL SELECT strftime(b, '%Y') FROM u ORDER BY x",
			TextUse{Ordered: []ColumnRef{c("x")}, Sorted: []ColumnRef{c("x")}, Aliases: []string{"x"}, SetOperation: true,
				Wrapped: []ColumnRef{c("a"), c("b")}, LooseLiteral: true}},
		// Not fixed: an unpadded number, a full name, a trailing space, an
		// unknown or unfinished specifier, a character outside ASCII.
		{"SELECT strftime(d, '%Y-%-m') AS m FROM t ORDER BY m",
			TextUse{Ordered: []ColumnRef{c("d")}, Sorted: []ColumnRef{c("d")}, Aliases: []string{"m"}, Wrapped: []ColumnRef{c("d"), c("d")}, OrderedLiterals: inExpr, LooseLiteral: true}},
		{"SELECT strftime(d, '%B %Y') AS m FROM t ORDER BY m",
			TextUse{Ordered: []ColumnRef{c("d")}, Sorted: []ColumnRef{c("d")}, Aliases: []string{"m"}, Wrapped: []ColumnRef{c("d"), c("d")}, OrderedLiterals: inExpr, LooseLiteral: true}},
		{"SELECT strftime(d, '%Y ') FROM t", TextUse{Wrapped: []ColumnRef{c("d")}, OddLiteral: true, LooseLiteral: true}},
		{"SELECT strftime(d, '%Y%') FROM t", TextUse{Wrapped: []ColumnRef{c("d")}, LooseLiteral: true}},
		{"SELECT strftime(d, '%Q') FROM t", TextUse{Wrapped: []ColumnRef{c("d")}, LooseLiteral: true}},
		{"SELECT strftime(d, 'año %Y') FROM t", TextUse{Wrapped: []ColumnRef{c("d")}, OddLiteral: true, LooseLiteral: true}},
		// The literal the format is compared with is still a literal.
		{"SELECT 1 FROM t WHERE strftime(d, '%Y-%m') >= '2026-01'",
			TextUse{Ordered: []ColumnRef{c("d")}, Wrapped: []ColumnRef{c("d")}, OrderedLiterals: inExpr, LooseLiteral: true}},
		// Another function with the same literal is read as before.
		{"SELECT concat(d, '%Y-%m') AS m FROM t ORDER BY m",
			TextUse{Ordered: []ColumnRef{c("d")}, Sorted: []ColumnRef{c("d")}, Aliases: []string{"m"}, OrderedLiterals: inExpr, LooseLiteral: true}},
	}
	// The select item is walked once where it stands and once more as what
	// the ORDER BY sorts, so a column under a call that sorts is listed in
	// Wrapped twice, as the #2533 cases list theirs.
	for _, tc := range cases {
		got := parseForRefs(t, tc.sql).Text
		if !reflect.DeepEqual(got, tc.want) {
			t.Errorf("%s\n  got  %+v\n  want %+v", tc.sql, got, tc.want)
		}
	}
}

func TestFixedFormat_2611(t *testing.T) {
	for f, want := range map[string]bool{
		"%Y-%m": true, "%Y-%m-%d %H:%M:%S": true, "%I:%M:%S %p": true, "%a %b": true, "%%": true, "x": true, "%f": true,
		"": false, "%Y ": false, "%": false, "%Y%": false, "%-m": false, "%-d": false, "%B": false, "%A": false, "%e": false, "%Q": false,
		"%Y\t": false, "é%Y": false,
	} {
		if got := fixedFormat(f); got != want {
			t.Errorf("fixedFormat(%q) = %v, want %v", f, got, want)
		}
	}
}
