package sqlsandbox

import (
	"reflect"
	"testing"
)

// How a statement uses text, from the parse (#2533): which columns it puts in
// order, which it sorts with an ORDER BY, what it puts in order that has no
// column's name, the names it gives, whether it compares whole rows, and its
// string literals. Read routing turns this into what the copy's values must
// be before it answers; this pins the reading, not the decision.
func TestCollectRefs_textUse(t *testing.T) {
	c := func(q, n string) ColumnRef { return ColumnRef{Qualifier: q, Name: n} }
	beside := func(cols ...ColumnRef) []OrderedLiteral { return []OrderedLiteral{{Columns: cols}} }
	inExpr := []OrderedLiteral{{Expression: true}}
	cases := []struct {
		sql  string
		want TextUse
	}{
		{"SELECT name FROM t WHERE status = 'paid' AND id IN (1, 2)", TextUse{}},
		{"SELECT status, count(*) FROM t GROUP BY status", TextUse{}},
		{"SELECT name FROM t WHERE name LIKE 'jo%' OR name IN ('a', 'b') OR name <> 'c'", TextUse{LooseLiteral: true}},
		{"SELECT name FROM t ORDER BY name", TextUse{Ordered: []ColumnRef{c("", "name")}, Sorted: []ColumnRef{c("", "name")}}},
		{"SELECT t.name FROM t ORDER BY lower(t.name) DESC NULLS LAST",
			TextUse{Ordered: []ColumnRef{c("t", "name")}, Sorted: []ColumnRef{c("t", "name")}}},
		{"SELECT min(city), max(t.city), greatest(a, b), least(a, 'x') FROM t",
			TextUse{Ordered: []ColumnRef{c("", "city"), c("t", "city"), c("", "a"), c("", "b"), c("", "a")},
				ByBytes: []ColumnRef{c("", "a"), c("", "b"), c("", "a")}, ByBytesLiteral: true}},
		{"SELECT arg_max(name, score), max_by(name, score), median(name), quantile_disc(name, 0.5) FROM t",
			TextUse{Ordered: []ColumnRef{c("", "name"), c("", "score"), c("", "name"), c("", "score"), c("", "name"), c("", "name")}}},
		{"SELECT name FROM t WHERE code < other AND a >= b AND x <= y AND p > q",
			TextUse{Ordered: []ColumnRef{c("", "code"), c("", "other"), c("", "a"), c("", "b"), c("", "x"), c("", "y"), c("", "p"), c("", "q")}}},
		{"SELECT name FROM t WHERE name BETWEEN 'a' AND 'm' AND d NOT BETWEEN 1 AND 2",
			TextUse{Ordered: []ColumnRef{c("", "name"), c("", "d")}}},
		{"SELECT row_number() OVER (PARTITION BY region ORDER BY name) FROM t",
			TextUse{Ordered: []ColumnRef{c("", "name")}, Sorted: []ColumnRef{c("", "name")}}},
		{"SELECT max(name) OVER (PARTITION BY k), min(code) OVER () FROM t",
			// The PARTITION BY counts too: more than needed, on the safe side.
			TextUse{Ordered: []ColumnRef{c("", "name"), c("", "k"), c("", "code")}}},
		{"SELECT string_agg(name, ',' ORDER BY code) FROM t",
			TextUse{Ordered: []ColumnRef{c("", "code")}, Sorted: []ColumnRef{c("", "code")}, LooseLiteral: true}},
		// Ordered with no column's name: an ordinal, a star.
		{"SELECT name, code FROM t ORDER BY 2", TextUse{OrderedUnknown: true}},
		{"SELECT * FROM t ORDER BY *", TextUse{OrderedUnknown: true}},
		{"SELECT name FROM t WHERE code > ANY (SELECT code FROM u)", TextUse{OrderedUnknown: true}},
		// An ORDER BY name that a select item, a WITH or a derived table
		// defines: the names are reported, the caller matches them.
		{"SELECT city AS name FROM t ORDER BY name",
			TextUse{Ordered: []ColumnRef{c("", "name")}, Sorted: []ColumnRef{c("", "name")}, Aliases: []string{"name"}}},
		{"WITH w(k) AS (SELECT a FROM t) SELECT k FROM w ORDER BY k",
			TextUse{Ordered: []ColumnRef{c("", "k")}, Sorted: []ColumnRef{c("", "k")}, Aliases: []string{"k"}}},
		{"SELECT x FROM (SELECT a FROM t) AS s(x) ORDER BY x",
			TextUse{Ordered: []ColumnRef{c("", "x")}, Sorted: []ColumnRef{c("", "x")}, Aliases: []string{"x"}}},
		// Set operations: the parts share the first part's names.
		{"SELECT DISTINCT name FROM t", TextUse{}},
		{"SELECT name FROM t UNION SELECT name FROM u", TextUse{SetOperation: true}},
		{"SELECT name FROM t UNION ALL SELECT name FROM u", TextUse{SetOperation: true}},
		{"SELECT name FROM t EXCEPT SELECT name FROM u", TextUse{SetOperation: true}},
		{"SELECT name FROM t UNION ALL SELECT code FROM u ORDER BY name",
			TextUse{Ordered: []ColumnRef{c("", "name")}, Sorted: []ColumnRef{c("", "name")}, SetOperation: true}},
		{"SELECT max(name) FROM (SELECT name FROM t UNION ALL SELECT 'a_b') x",
			TextUse{Ordered: []ColumnRef{c("", "name")}, SetOperation: true, LooseLiteral: true}},
		// String literals: one outside printable ASCII, or ending in a space,
		// anywhere; one outside letters, digits and inner spaces where it is
		// put in order, with what it is compared with.
		{"SELECT name FROM t WHERE name = 'José'", TextUse{OddLiteral: true, LooseLiteral: true}},
		{"SELECT name FROM t WHERE name = 'Jose '", TextUse{OddLiteral: true, LooseLiteral: true}},
		{"SELECT name FROM t WHERE name = 'a\tb'", TextUse{OddLiteral: true, LooseLiteral: true}},
		{"SELECT 'Año' AS label FROM t", TextUse{OddLiteral: true, LooseLiteral: true, Aliases: []string{"label"}}},
		{"SELECT name FROM t WHERE name = '' AND code = 'a_b-c.d'", TextUse{LooseLiteral: true}},
		{"SELECT name FROM t WHERE name < 'a_b'",
			TextUse{Ordered: []ColumnRef{c("", "name")}, OrderedLiterals: beside(c("", "name")), LooseLiteral: true}},
		{"SELECT name FROM t WHERE created_at >= '2026-01-01' AND name > 'Jo Ann'",
			TextUse{Ordered: []ColumnRef{c("", "created_at"), c("", "name")}, OrderedLiterals: beside(c("", "created_at")), LooseLiteral: true}},
		{"SELECT name FROM t WHERE upper(name) BETWEEN 'A' AND 'M_'",
			TextUse{Ordered: []ColumnRef{c("", "name")}, OrderedLiterals: inExpr, LooseLiteral: true}},
		{"SELECT greatest(name, 'z-z') FROM t",
			TextUse{Ordered: []ColumnRef{c("", "name")}, OrderedLiterals: beside(c("", "name")), LooseLiteral: true,
				ByBytes: []ColumnRef{c("", "name")}, ByBytesLiteral: true}},
		{"SELECT 1 FROM t WHERE 'a_b' < 'ab'", TextUse{OrderedLiterals: beside(), LooseLiteral: true}},
		// A string in an ORDER BY sorts nothing.
		{"SELECT name FROM t ORDER BY 'x-y'", TextUse{LooseLiteral: true}},
		// A string with a sign inside an ORDER BY expression is put in order.
		{"SELECT name FROM t ORDER BY COALESCE(name, '~')",
			TextUse{Ordered: []ColumnRef{c("", "name")}, Sorted: []ColumnRef{c("", "name")}, OrderedLiterals: inExpr, LooseLiteral: true}},
		// Beside a number only as a direct operand: CONCAT(k, '') is text.
		{"SELECT 1 FROM t WHERE CONCAT(k, '') > '1_'",
			TextUse{Ordered: []ColumnRef{c("", "k")}, OrderedLiterals: inExpr, LooseLiteral: true}},
		// Compared by bytes, case included: GREATEST, LEAST, a row.
		{"SELECT greatest(a, b) FROM t", TextUse{Ordered: []ColumnRef{c("", "a"), c("", "b")}, ByBytes: []ColumnRef{c("", "a"), c("", "b")}}},
		{"SELECT 1 FROM t WHERE (name, id) > ('bob', 2)",
			TextUse{Ordered: []ColumnRef{c("", "name"), c("", "id")}, ByBytes: []ColumnRef{c("", "name"), c("", "id")}, ByBytesLiteral: true}},
		{"SELECT 1 FROM t WHERE (id, k) = (1, 2)", TextUse{ByBytes: []ColumnRef{c("", "id"), c("", "k")}}},
		// A comment is not a literal.
		{"SELECT name FROM t /* José */ WHERE id = 1", TextUse{}},
	}
	for _, tc := range cases {
		got := parseForRefs(t, tc.sql).Text
		if !reflect.DeepEqual(got, tc.want) {
			t.Errorf("%s\n  got  %+v\n  want %+v", tc.sql, got, tc.want)
		}
	}
}
