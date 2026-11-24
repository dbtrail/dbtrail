package pgshim

import (
	"strings"
	"testing"
)

// The screen's name for "known not to be text" is none of the families it
// tells apart: not text, not a whole number, not a float.
func TestNotTextType(t *testing.T) {
	if typeIsText(notTextType) || typeIsInteger(notTextType) || typeIsFloat(notTextType) {
		t.Errorf("%q is read as a family of its own", notTextType)
	}
}

func TestExprNotText(t *testing.T) {
	types := map[string][]string{"qty": {"INTEGER"}, "amount": {"DECIMAL(12,2)"}, "note": {"VARCHAR"}, "both": {"INTEGER", "VARCHAR"}, "attrs": {"VARCHAR"}}
	for expr, want := range map[string]bool{
		"7":            true,
		"qty":          true,
		"o.qty":        true,
		"(qty)":        true,
		"note":         false,
		"o.note":       false,
		"both":         false, // a text column of that name somewhere
		"unknown_name": false,
		"count(*)":     true,
		"sum(qty)":     true,
		"sum(note)":    true, // never text, whatever it is given (PostgreSQL refuses it)
		"avg(amount)":  true,
		"row_number() OVER (PARTITION BY qty ORDER BY amount)": true,
		"sum(amount) OVER ()":                           true,
		"count(*) FILTER (WHERE qty > 1)":               true,
		"(SELECT note FROM t WHERE t.qty = qty + 1)":    false,
		"(SELECT * FROM t)":                             false,
		"attrs - 1":                                     false,
		"amount - qty":                                  true,
		"(qty - 1) * 2":                                 true,
		"count(*) + 1":                                  true,
		"qty * 2 - 1":                                   true,
		"min(qty)":                                      true,
		"max(o.amount)":                                 true,
		"min(note)":                                     false,
		"lag(qty, 1, 0) OVER (ORDER BY qty)":            true,
		"lag(note) OVER (ORDER BY qty)":                 false,
		"coalesce(qty, 0)":                              true,
		"coalesce(qty, note)":                           false,
		"nullif(amount, 0)":                             true,
		"CASE WHEN qty > 1 THEN 1 ELSE 0 END":           true,
		"CASE WHEN qty > 1 THEN qty END":                true,
		"CASE WHEN qty > 1 THEN 1 ELSE NULL END":        true,
		"CASE WHEN qty > 1 THEN 'big' ELSE 'small' END": false,
		"CASE WHEN qty > 1 THEN CASE WHEN qty > 2 THEN 2 ELSE 1 END ELSE 0 END":   true,
		"CASE WHEN qty > 1 THEN CASE WHEN qty > 2 THEN 'x' ELSE 1 END ELSE 0 END": false,
		"note || 'x'":                        false,
		"qty || 1":                           false,
		"'5'":                                false,
		"$1":                                 false,
		"$1 + 1":                             false,
		"upper(note)":                        false,
		"min(DISTINCT qty)":                  false,
		"some_function(qty)":                 false,
		"count(*) over (ORDER BY qty) extra": false,
		"":                                   false,
	} {
		toks, _ := lexSQL(expr)
		if got := exprNotText(toks, types); got != want {
			t.Errorf("%q: %v, want %v", expr, got, want)
		}
	}
}

// A name a WITH or a subquery defines as a number is not text to the
// screen; one defined as text, or as anything it cannot read, still is.
func TestCopyScreen_namesTheStatementDefines(t *testing.T) {
	for _, q := range []string{
		`WITH s AS (SELECT customer_id AS c, sum(qty) AS units FROM pgcmp_orders GROUP BY customer_id)
		 SELECT s.c, s.units FROM s WHERE s.units >= 10 ORDER BY s.units DESC, s.c`,
		// With a text column in the statement, so a name of unknown type
		// would count as text: p.qty is read as its column.
		`SELECT o.id, o.note FROM pgcmp_orders o JOIN pgcmp_orders p ON o.qty >= p.qty - 5`,
		`WITH r AS (SELECT id, row_number() OVER (ORDER BY id) AS rnk FROM pgcmp_orders)
		 SELECT r.id, r.rnk FROM r WHERE r.rnk <= 3 ORDER BY r.id, r.rnk`,
		// ) OVER ( is no column list (AS b(n)): the names are still read.
		`WITH r AS (SELECT id, note, row_number() OVER (ORDER BY id) AS rnk, sum(qty) FILTER (WHERE qty > 0) AS s FROM pgcmp_orders)
		 SELECT r.id, r.note FROM r WHERE r.rnk <= 3 ORDER BY r.rnk, r.s`,
		`SELECT max(o.qty) FROM pgcmp_orders o`,
		`SELECT customer_id FROM pgcmp_orders WHERE note IS NOT NULL GROUP BY customer_id HAVING count(*) >= 2`,
		`SELECT customer_id FROM pgcmp_orders WHERE note IS NOT NULL GROUP BY customer_id HAVING 2 <= count(*)`,
		`SELECT customer_id FROM pgcmp_orders WHERE note IS NOT NULL GROUP BY customer_id HAVING sum(qty) BETWEEN 1 AND 9`,
		`WITH f AS (SELECT id, CASE WHEN qty > 5 THEN 1 ELSE 0 END AS big FROM pgcmp_orders)
		 SELECT f.id FROM f WHERE f.big >= 1`,
		`WITH a AS (SELECT customer_id AS c, count(*) AS n FROM pgcmp_orders GROUP BY customer_id),
		      b AS (SELECT a.c, a.n * 2 AS n2 FROM a)
		 SELECT b.c FROM b WHERE b.n2 > 2 ORDER BY b.n2`,
	} {
		if veto := CopyScreen(q, routeTypes); veto != "" {
			t.Errorf("sent to the source for %s:\n%s", veto, q)
		}
	}
	for q, says := range map[string]string{
		`SELECT note AS id FROM pgcmp_orders ORDER BY id`:                                           "text sorted",
		`WITH s AS (SELECT note AS label FROM pgcmp_orders) SELECT s.label FROM s ORDER BY s.label`: "text sorted",
		`WITH s AS (SELECT id, CASE WHEN qty > 5 THEN 'big' ELSE 'small' END AS seg FROM pgcmp_orders)
		 SELECT s.id FROM s WHERE s.seg > 'm'`: "text compared",
		`WITH s AS (SELECT id, CASE WHEN qty > 5 THEN 'big' ELSE 'small' END AS seg FROM pgcmp_orders)
		 SELECT s.id FROM s WHERE 'm' < s.seg`: "text compared",
		`WITH a AS (SELECT count(*) AS v FROM pgcmp_orders), b AS (SELECT note AS v FROM pgcmp_orders)
		 SELECT b.v FROM b ORDER BY b.v`: "text sorted",
		`SELECT count(*) AS n FROM pgcmp_orders WHERE n > 1`:                                 "used in WHERE",
		`WITH s AS (SELECT min(note) AS m FROM pgcmp_orders) SELECT s.m FROM s ORDER BY s.m`: "min() of text",
		`SELECT o.id FROM pgcmp_orders o WHERE 'a' < o.note`:                                 "text compared",
		`SELECT o.id FROM pgcmp_orders o WHERE o.note > 'a'`:                                 "text compared",
		// A name defined as a number that is also a text column: PostgreSQL
		// reads the column in WHERE.
		`WITH s AS (SELECT count(*) AS note FROM pgcmp_orders) SELECT o.id FROM pgcmp_orders o WHERE o.note > 'a'`: "text compared",
		`SELECT max(o.note) FROM pgcmp_orders o`: "max() of text",
		// A scalar subquery is what it selects, not what its WHERE adds.
		`SELECT (SELECT name FROM pgcmp_customers c WHERE c.id = o.customer_id + 1) AS x FROM pgcmp_orders o ORDER BY x`: "text sorted",
		// A name a column list gives (AS b(n)) is not read as a definition:
		// nothing the statement defines is known then.
		`WITH d AS (SELECT count(*) AS n FROM pgcmp_orders) SELECT b.n FROM d, (SELECT note FROM pgcmp_orders) AS b(n) ORDER BY b.n`:    "the function b()",
		`WITH c(n) AS (SELECT note FROM pgcmp_orders), d AS (SELECT count(*) AS n FROM pgcmp_orders) SELECT c.n FROM c, d ORDER BY c.n`: "the function c()",
		// Only comparisons read a qualified name as its column.
		`SELECT id::pg_catalog.date FROM pgcmp_orders`: "a cast with ::",
		// A name of three parts is its last.
		`SELECT id FROM public.pgcmp_orders WHERE 'm' < public.pgcmp_orders.note`: "text compared",
		// A call that may answer text is still read as one.
		`SELECT customer_id FROM pgcmp_orders GROUP BY customer_id HAVING max(note) >= 'a'`: "text",
		`SELECT id FROM pgcmp_orders WHERE upper(note) > 'A'`:                               "upper",
	} {
		// With the main ORDER BY's text kept from the copy: that one is the
		// copy's since #3219, and these still say what reads as text.
		if veto := CopyScreenSortingLikeSource(q, routeTypes); !strings.Contains(veto, says) {
			t.Errorf("veto %q, want one that says %q:\n%s", veto, says, q)
		}
	}
}
