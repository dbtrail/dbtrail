package pgshim

import (
	"strings"
	"testing"
)

// A division on its own is written as the copy's macro (#3219): the left
// side is the product before the slash, the right side the one value after
// it, as PostgreSQL reads them.
func TestRewriteDivisions(t *testing.T) {
	const d = "_bintrail_pg_div"
	for _, c := range []struct{ in, want string }{
		{"SELECT a / b", "SELECT " + d + "(a, b)"},
		{"SELECT a/b FROM t", "SELECT " + d + "(a, b) FROM t"},
		{"SELECT sum(x) / 2.0 AS y FROM t", "SELECT " + d + "(sum(x), 2.0) AS y FROM t"},
		{"SELECT 100.00 * sum(x) / (1 + sum(y)) AS p FROM t", "SELECT " + d + "(100.00 * sum(x), (1 + sum(y))) AS p FROM t"},
		{"SELECT t.a / t.b FROM t", "SELECT " + d + "(t.a, t.b) FROM t"},
		{`SELECT "A" / "B"`, "SELECT " + d + `("A", "B")`},
		// PostgreSQL divides from the left: (a / b) / c, (a / b) * c.
		{"SELECT a / b / c", "SELECT " + d + "(" + d + "(a, b), c)"},
		{"SELECT a / b * c", "SELECT " + d + "(a, b) * c"},
		// A third division of one chain is left as written: the macro's text
		// holds each side several times over.
		{"SELECT a / b / c / e", "SELECT " + d + "(" + d + "(a, b), c) / e"},
		{"SELECT (a / b) / (c / e)", "SELECT " + d + "((" + d + "(a, b)), (" + d + "(c, e)))"},
		{"SELECT (a / b / c) / e", "SELECT (" + d + "(" + d + "(a, b), c)) / e"},
		// Nested on the right too: two deep, and the third is left.
		{"SELECT a / (b / (c / e))", "SELECT " + d + "(a, (" + d + "(b, (c / e))))"},
		{"SELECT a / (b / (c / (e / (f / g))))", "SELECT " + d + "(a, (" + d + "(b, (c / (e / (f / g))))))"},
		{"SELECT (a / b) / c / e", "SELECT " + d + "((" + d + "(a, b)), c) / e"},
		{"SELECT sum(a / b) / sum(c / e)", "SELECT " + d + "(sum(" + d + "(a, b)), sum(" + d + "(c, e)))"},
		// A string that is not a typed literal's does not stop the read.
		{"SELECT sum(CASE WHEN s LIKE 'PR%' THEN a ELSE 0 END) / b", "SELECT " + d + "(sum(CASE WHEN s LIKE 'PR%' THEN a ELSE 0 END), b)"},
		// What stands before the first value is not part of it.
		{"SELECT DISTINCT ON (customer_id) amount / qty FROM orders", "SELECT DISTINCT ON (customer_id) " + d + "(amount, qty) FROM orders"},
		{"SELECT DISTINCT ON (a, b) c * e / f FROM t", "SELECT DISTINCT ON (a, b) " + d + "(c * e, f) FROM t"},
		{"SELECT s.t.a / s.t.b FROM s.t", "SELECT " + d + "(s.t.a, s.t.b) FROM s.t"},
		{"SELECT pg_catalog.abs(a) / b", "SELECT " + d + "(pg_catalog.abs(a), b)"},
		{"SELECT (a + b) / c, x", "SELECT " + d + "((a + b), c), x"},
		{"SELECT a * (b + c) / e", "SELECT " + d + "(a * (b + c), e)"},
		{"SELECT a % b / c", "SELECT " + d + "(a % b, c)"},
		{"SELECT a * b / c * e", "SELECT " + d + "(a * b, c) * e"},
		{"SELECT (a / b) / c", "SELECT " + d + "((" + d + "(a, b)), c)"},
		{"SELECT a / (b / c)", "SELECT " + d + "(a, (" + d + "(b, c)))"},
		// A sum or a difference is not part of either side.
		{"SELECT a + b / c", "SELECT a + " + d + "(b, c)"},
		{"SELECT a - b / c - e", "SELECT a - " + d + "(b, c) - e"},
		{"SELECT a / b + c / e", "SELECT " + d + "(a, b) + " + d + "(c, e)"},
		// A sign is part of its value.
		{"SELECT -a / b", "SELECT " + d + "(-a, b)"},
		{"SELECT a / -b", "SELECT " + d + "(a, -b)"},
		{"SELECT x, -a / b", "SELECT x, " + d + "(-a, b)"},
		{"SELECT x - -a / b", "SELECT x - " + d + "(-a, b)"},
		{"SELECT (-a / b)", "SELECT (" + d + "(-a, b))"},
		// Wherever a value stands.
		{"SELECT x FROM t WHERE a / b > 1 AND c / e < 2", "SELECT x FROM t WHERE " + d + "(a, b) > 1 AND " + d + "(c, e) < 2"},
		{"SELECT CASE WHEN a > 0 THEN b / a ELSE c / a END", "SELECT CASE WHEN a > 0 THEN " + d + "(b, a) ELSE " + d + "(c, a) END"},
		{"SELECT count(DISTINCT a / b)", "SELECT count(DISTINCT " + d + "(a, b))"},
		{"SELECT sum(a / b) FROM t", "SELECT sum(" + d + "(a, b)) FROM t"},
		{"SELECT g FROM t GROUP BY g HAVING sum(a) / count(*) > 2", "SELECT g FROM t GROUP BY g HAVING " + d + "(sum(a), count(*)) > 2"},
		{"SELECT x FROM t ORDER BY a / b DESC", "SELECT x FROM t ORDER BY " + d + "(a, b) DESC"},
		{"SELECT x FROM t WHERE x BETWEEN a / b AND c / e", "SELECT x FROM t WHERE x BETWEEN " + d + "(a, b) AND " + d + "(c, e)"},
		{"SELECT x FROM t WHERE x IN (a / b, c)", "SELECT x FROM t WHERE x IN (" + d + "(a, b), c)"},
		{"SELECT coalesce(a / b, 0)", "SELECT coalesce(" + d + "(a, b), 0)"},
		{"SELECT a / nullif(b, 0)", "SELECT " + d + "(a, nullif(b, 0))"},
		{"SELECT a / pg_catalog.abs(b)", "SELECT " + d + "(a, pg_catalog.abs(b))"},
		// A whole number over a column, a column over a number with a point,
		// and a column over zero are not the form the screen measures.
		{"SELECT 1 / qty", "SELECT " + d + "(1, qty)"},
		{"SELECT qty / 2.5", "SELECT " + d + "(qty, 2.5)"},
		{"SELECT qty / 0", "SELECT " + d + "(qty, 0)"},
		{"SELECT qty / 00", "SELECT " + d + "(qty, 00)"},
		{"SELECT sum(qty) / 2", "SELECT " + d + "(sum(qty), 2)"},
		// The text around it is kept byte for byte.
		{"SELECT /* c */ x,  a/b -- t\nFROM t", "SELECT /* c */ x,  " + d + "(a, b) -- t\nFROM t"},
		{"SELECT 'a / b', a / b", "SELECT 'a / b', " + d + "(a, b)"},
	} {
		if got := rewriteDivisions(c.in, nil); got != c.want {
			t.Errorf("rewriteDivisions(%q)\n   got %q\n  want %q", c.in, got, c.want)
		}
		// Written once: a second pass finds nothing to write.
		if again := rewriteDivisions(c.want, nil); again != c.want {
			t.Errorf("rewriteDivisions is not stable on %q: %q", c.want, again)
		}
	}
}

// What rewriteDivisions does not read stays as written, and the screen goes
// on sending it to the source.
func TestRewriteDivisionsLeavesAsWritten(t *testing.T) {
	for _, q := range []string{
		"SELECT 1",
		"SELECT '/'",
		"",
		"SELECT /",
		"SELECT a /",
		"SELECT / b",
		"SELECT * / 2 FROM t",
		// A name over a whole number other than zero: the engine's division
		// for a column of whole numbers or of floats, the macro's for a
		// numeric column, which the columns' types decide (retypeForCopy).
		"SELECT qty / 2 FROM t",
		"SELECT o.qty / 10 FROM t o",
		`SELECT "Qty" / 7`,
		"SELECT -qty / 2 FROM t",
		"SELECT price_cents * qty / 100 FROM t",
		// A subquery on a side: the macro's text holds each side several
		// times over.
		"SELECT a / (SELECT sum(b) FROM t)",
		"SELECT (SELECT sum(b) FROM t) / a",
		// Two whole numbers written in the text: 3 on PostgreSQL and on the
		// engine alike.
		"SELECT 7 / 2 AS cut, -7 / 2 AS cut_negative",
		"SELECT 10 / -3",
		// A cast, a parameter, a string, a typed literal.
		"SELECT a::numeric / b",
		"SELECT a / b::numeric",
		"SELECT cast(a AS numeric) / b",
		"SELECT a / cast(b AS numeric)",
		"SELECT (a::numeric) / b",
		"SELECT a / $1",
		"SELECT $1 / a",
		"SELECT 'x' / a",
		"SELECT a / 'x'",
		"SELECT interval '1 day' / 2",
		// The value of a CASE, and what the divisor binds to first.
		"SELECT CASE WHEN a > 0 THEN a END / b",
		"SELECT a / CASE WHEN b > 0 THEN b END",
		"SELECT a / b ^ 2",
		"SELECT a ^ 2 / b",
		"SELECT arr[1] / b",
		"SELECT a / arr[1]",
		"SELECT a / (b).c",
		"SELECT a / sum(b) OVER ()",
		"SELECT a / count(*) FILTER (WHERE b > 0)",
		"SELECT a / NULL",
		"SELECT a / b COLLATE \"C\"",
		"SELECT x FROM t WHERE a IS NULL / 2",
		// Inside one of the port's own macros, which take the division
		// themselves or refuse it.
		"SELECT _bintrail_pg_round_div(a, b, 2, 0.01)",
		"SELECT _bintrail_pg_round_k(a / b + c, 2, 0.01)",
		"SELECT _bintrail_pg_floor_div(a, (b / c))",
		// Inside a function that cuts its value, of a shape the copy does not
		// answer: the quotient could fall on the other side of the cut.
		"SELECT round(a / b + c, 2)",
		"SELECT round(sum(q) / 2 * 3)",
		"SELECT floor(a / b * c)",
		"SELECT pg_catalog.floor(a / b)",
		`SELECT "round"(a / b, 2)`,
		"SELECT ceil(a / b), ceiling(a / b), trunc(a / b)",
		"SELECT trunc(coalesce(a / b, 0))",
		// A parameter or a typed literal anywhere on a side: a bound value is
		// written as a cast, and a side with a cast is not read.
		"SELECT a / (b + $1)",
		"SELECT (a + $1) / b",
		"SELECT a / abs($2)",
		"SELECT a / numeric '2.5'",
		"SELECT a / (b + date '2026-01-01' - c)",
		// Where PostgreSQL takes a whole number of rows.
		"SELECT x FROM t ORDER BY x LIMIT 10 / 4.0",
		"SELECT x FROM t ORDER BY x OFFSET n / 2.0",
		"SELECT sum(x) OVER (ORDER BY y ROWS n / 2.0 PRECEDING) FROM t",
		// Words that are neither a value nor an operator before the side.
		"SELECT substring(s FOR len / 2.0) FROM t",
		"SELECT x FROM t WHERE x BETWEEN SYMMETRIC a / b AND c",
		"SELECT a.b. / c",
		// A statement the port does not read.
		"SELECT a / b, 'unterminated",
	} {
		if got := rewriteDivisions(q, nil); got != q {
			t.Errorf("rewriteDivisions(%q) = %q, want it left as written", q, got)
		}
	}
}

// RewriteForCopy writes rounds first and divisions after: a division a round
// took is not written twice, and the one left beside it is. A connection the
// copy answers alone keeps its divisions as written: the macro refuses a
// divisor of zero, and there a refusal has no source to go to.
func TestRewriteForCopyDivisions(t *testing.T) {
	for _, c := range []struct{ in, want, alone string }{
		{"SELECT round(a / b, 2), c / e", "SELECT _bintrail_pg_round_div(a, b, 2, 0.01), _bintrail_pg_div(c, e)",
			"SELECT _bintrail_pg_round_div(a, b, 2, 0.01), c / e"},
		{"SELECT avg(x) / 2.0", "SELECT _bintrail_pg_div(_bintrail_pg_avg(x), 2.0)", "SELECT _bintrail_pg_avg(x) / 2.0"},
		{"SELECT avg(a / b), sum(a / b)", "SELECT _bintrail_pg_avg(_bintrail_pg_div(a, b)), sum(_bintrail_pg_div(a, b))",
			"SELECT _bintrail_pg_avg(a / b), sum(a / b)"},
		{"SELECT 100.00 * sum(case when i_data like 'PR%' then ol_amount else 0 end) / (1 + sum(ol_amount)) as promo_revenue from order_line, item",
			"SELECT _bintrail_pg_div(100.00 * sum(case when i_data like 'PR%' then ol_amount else 0 end), (1 + sum(ol_amount))) as promo_revenue from order_line, item",
			"SELECT 100.00 * sum(case when i_data like 'PR%' then ol_amount else 0 end) / (1 + sum(ol_amount)) as promo_revenue from order_line, item"},
		{"SELECT -qty / 2, count(*) / 2, (a + b) / 2", "SELECT -qty / 2, _bintrail_pg_div(count(*), 2), _bintrail_pg_div((a + b), 2)",
			"SELECT -qty / 2, count(*) / 2, (a + b) / 2"},
	} {
		if got := RewriteForCopy(c.in); got != c.want {
			t.Errorf("RewriteForCopy(%q)\n   got %q\n  want %q", c.in, got, c.want)
		}
		if alone, err := RewriteForCopyAlone(c.in); err != nil || alone != c.alone {
			t.Errorf("RewriteForCopyAlone(%q) = %q, %v\n  want %q", c.in, alone, err, c.alone)
		}
	}
	// More divisions than the rewrite reads: every one stays as written.
	many := "SELECT " + strings.Repeat("a / b + ", maxDivisions) + "a / b"
	if got := rewriteDivisions(many, nil); got != many {
		t.Errorf("a statement with %d divisions was rewritten", maxDivisions+1)
	}
	few := "SELECT " + strings.Repeat("a / b + ", maxDivisions-1) + "a / b"
	if got := rewriteDivisions(few, nil); strings.Count(got, pgDiv) != maxDivisions || strings.Contains(got, "/") {
		t.Errorf("a statement with %d divisions: %d written", maxDivisions, strings.Count(got, pgDiv))
	}
}

// The column types of the tests below: orders, as the copy has it.
var divTypes = map[string][]string{
	"id": {"BIGINT"}, "customer_id": {"INTEGER"}, "qty": {"INTEGER"}, "amount": {"DECIMAL(12,2)"}, "ratio": {"DOUBLE"},
	"wide": {"DECIMAL(38,18)"}, "fine": {"DECIMAL(20,8)"}, "note": {"VARCHAR"}, "pack": {"SMALLINT"},
	// A name two tables have, a numeric in one and a whole number in the
	// other: not known to be either.
	"mixed": {"DECIMAL(10,2)", "INTEGER"},
}

// What a column's type decides is written after RewriteForCopy: a numeric
// column over a whole number is the macro's division, and avg of numerics or
// whole numbers the numeric avg.
func TestRewriteForCopyTyped(t *testing.T) {
	const d, an, af = "_bintrail_pg_div", "_bintrail_pg_avgn", "_bintrail_pg_avg"
	for _, c := range []struct{ in, want string }{
		{"SELECT amount / 100 FROM orders", "SELECT " + d + "(amount, 100) FROM orders"},
		{"SELECT o.amount / 12 FROM orders o", "SELECT " + d + "(o.amount, 12) FROM orders o"},
		{"SELECT qty * amount / 100 FROM orders", "SELECT " + d + "(qty * amount, 100) FROM orders"},
		{"SELECT -amount / 2 FROM orders", "SELECT " + d + "(-amount, 2) FROM orders"},
		{"SELECT fine / 3 FROM orders", "SELECT " + d + "(fine, 3) FROM orders"},
		// A numeric without precision is not one the macros divide for every
		// value: it keeps the engine's text, and the screen sends it away.
		{"SELECT wide / 3 FROM orders", "SELECT wide / 3 FROM orders"},
		{"SELECT avg(amount) * qty / 2 FROM orders", "SELECT " + d + "(" + an + "(amount) * qty, 2) FROM orders"},
		// A column of whole numbers or of floats, and a name no column has,
		// keep the engine's division.
		{"SELECT qty / 2, ratio / 4, id / 10, total / 2, mixed / 2, note / 2 FROM orders", "SELECT qty / 2, ratio / 4, id / 10, total / 2, mixed / 2, note / 2 FROM orders"},
		{"SELECT amount * qty / 100 FROM orders", "SELECT " + d + "(amount * qty, 100) FROM orders"},
		{"SELECT pack * qty / 100, ratio * qty / 100 FROM orders", "SELECT pack * qty / 100, ratio * qty / 100 FROM orders"},
		// avg.
		{"SELECT avg(amount), avg(qty), avg(id) FROM orders", "SELECT " + an + "(amount), " + an + "(qty), " + an + "(id) FROM orders"},
		{"SELECT avg(o.amount * 2 + qty) FROM orders o", "SELECT " + an + "(o.amount * 2 + qty) FROM orders o"},
		{"SELECT avg(amount / qty) FROM orders", "SELECT " + an + "(" + d + "(amount, qty)) FROM orders"},
		{"SELECT avg(amount / 100) FROM orders", "SELECT " + an + "(" + d + "(amount, 100)) FROM orders"},
		{"SELECT avg(coalesce(amount, 0)), avg(abs(qty)), avg(length(note)) FROM orders", "SELECT " + an + "(coalesce(amount, 0)), " + an + "(abs(qty)), " + an + "(length(note)) FROM orders"},
		{"SELECT 3 * avg(amount) FROM orders", "SELECT 3 * " + an + "(amount) FROM orders"},
		{"SELECT avg(ratio), avg(note), avg(total), avg(mixed), avg(ratio * amount) FROM orders", "SELECT " + af + "(ratio), " + af + "(note), " + af + "(total), " + af + "(mixed), " + af + "(ratio * amount) FROM orders"},
		// A CASE is what its values are (caseClass, #3219).
		{"SELECT avg(CASE WHEN qty > 1 THEN amount END) FROM orders", "SELECT " + an + "(CASE WHEN qty > 1 THEN amount END) FROM orders"},
		{"SELECT avg(CASE WHEN qty > 1 THEN ratio END) FROM orders", "SELECT " + af + "(CASE WHEN qty > 1 THEN ratio END) FROM orders"},
		{"SELECT avg(qty / 2) FROM orders", "SELECT " + af + "(qty / 2) FROM orders"},
		// A round took its avg and its division first.
		{"SELECT round(avg(amount), 2), round(amount / 100, 2) FROM orders", "SELECT _bintrail_pg_round_avg(amount, 2, 0.01), _bintrail_pg_round_div(amount, 100, 2, 0.01) FROM orders"},
	} {
		got := RewriteForCopyTyped(c.in, divTypes)
		if got != c.want {
			t.Errorf("RewriteForCopyTyped(%q)\n   got %q\n  want %q", c.in, got, c.want)
		}
		// What is written is not read again.
		if again := retypeForCopy(got, divTypes); again != got {
			t.Errorf("retypeForCopy is not stable on %q: %q", got, again)
		}
		// Without the columns' types nothing of it is decided.
		if got := RewriteForCopyTyped(c.in, nil); got != RewriteForCopy(c.in) {
			t.Errorf("RewriteForCopyTyped(%q, nil) = %q, want RewriteForCopy's", c.in, got)
		}
	}
}

// The screen lets a division or an avg through only where the statement's
// text shows every side to be a numeric or a whole number, and not two whole
// numbers: the macros then refuse nothing by type when the statement runs.
func TestCopyScreen_divisionAndAvg(t *testing.T) {
	for _, q := range []string{
		"SELECT sum(amount) / 2.0 FROM orders",
		"SELECT sum(amount) / 100 FROM orders",
		"SELECT 100.00 * sum(amount) / (1 + sum(amount)) FROM orders",
		"SELECT amount / qty FROM orders",
		"SELECT o.amount / o.qty FROM orders o",
		"SELECT amount / 100, amount / 2 FROM orders",
		"SELECT qty / 2.0, 1 / amount, id / 2.5 FROM orders",
		"SELECT id FROM orders WHERE amount / qty > 2 ORDER BY id",
		"SELECT sum(amount) / count(*), count(*) / sum(amount), sum(qty) / sum(amount) FROM orders",
		"SELECT coalesce(amount, 0) / nullif(qty, 0), length(note) / 2.0, abs(amount) / greatest(qty, 1) FROM orders",
		"SELECT amount / qty / 3.0, amount * 2 / qty, -amount / qty, amount / -qty FROM orders",
		"SELECT fine / amount FROM orders",
		"SELECT avg(amount) * qty / 2 FROM orders",
		"SELECT avg(amount) FROM orders",
		"SELECT customer_id, avg(amount), avg(qty), avg(id) FROM orders GROUP BY customer_id ORDER BY customer_id",
		"SELECT customer_id FROM orders GROUP BY customer_id HAVING avg(amount) > 5 ORDER BY customer_id",
		"SELECT customer_id FROM orders GROUP BY customer_id ORDER BY avg(amount), customer_id",
		"SELECT avg(amount / qty), avg(amount) / 2.0, sum(amount) / avg(qty), 3 * avg(amount) FROM orders",
		"SELECT round(avg(amount) * 100, 2) FROM orders",
		// As before: a column of whole numbers or of floats over a whole
		// number written in the text is the engine's division.
		"SELECT qty / 2, ratio / 4, -qty / 2, pack * qty / 100 FROM orders",
	} {
		if veto := CopyScreen(q, divTypes); veto != "" {
			t.Errorf("%s: sent to the source for %s", q, veto)
		}
	}
	for q, says := range map[string]string{
		// PostgreSQL's error, known before the copy is asked.
		"SELECT amount / 0 FROM orders":       "a division by zero",
		"SELECT amount / 0.00 FROM orders":    "a division by zero",
		"SELECT sum(amount) / -0 FROM orders": "a division by zero",
		// Two whole numbers: PostgreSQL divides them as whole numbers.
		"SELECT 7 / 2":                                        "a division",
		"SELECT qty / customer_id FROM orders":                "two whole numbers",
		"SELECT sum(qty) / count(*) FROM orders":              "two whole numbers",
		"SELECT count(*) / -3 FROM orders":                    "two whole numbers",
		"SELECT max(qty) / count(DISTINCT qty) FROM orders":   "two whole numbers",
		"SELECT id, (0 - qty) / 2 FROM orders":                "two whole numbers",
		"SELECT sum(qty * 2) / (count(*) + 1) FROM orders":    "two whole numbers",
		"SELECT o.qty * o.pack / o.customer_id FROM orders o": "two whole numbers",
		"SELECT length(note) / 2 FROM orders":                 "a division",
		"SELECT qty / nullif(pack, 0) FROM orders":            "two whole numbers",
		// A side the text does not show to be a numeric or a whole number.
		"SELECT amount / ratio FROM orders":                                                "cannot show",
		"SELECT ratio / amount FROM orders":                                                "cannot show",
		"SELECT sum(id) / count(*) FROM orders":                                            "cannot show",
		"SELECT total / n FROM (SELECT sum(amount) AS total, count(*) AS n FROM orders) s": "cannot show",
		"SELECT amount / mixed FROM orders":                                                "cannot show",
		"SELECT amount / note FROM orders":                                                 "cannot show",
		"SELECT amount / random() FROM orders":                                             "",
		"SELECT amount / (qty % 3 + 1) FROM orders":                                        "",
		"SELECT p / 2 FROM (SELECT sum(amount) AS p FROM orders) s":                        "a division",
		"SELECT wide / 2 FROM (SELECT qty AS wide FROM orders) s":                          "",
		// An avg that is not of numerics or whole numbers.
		"SELECT wide / amount FROM orders":                  "cannot show",
		"SELECT wide / 3 FROM orders":                       "a division",
		"SELECT avg(wide) FROM orders":                      "_bintrail_pg_avg",
		"SELECT sum(id) * qty / 2 FROM orders GROUP BY qty": "a division",
		"SELECT avg(ratio) FROM orders":                     "_bintrail_pg_avg",
		"SELECT avg(note) FROM orders":                      "_bintrail_pg_avg",
		"SELECT avg(DISTINCT amount) FROM orders":           "avg",
		"SELECT avg(amount) OVER () FROM orders":            "avg",
		// What the rewrite does not read.
		"SELECT amount::numeric / qty FROM orders":         "",
		"SELECT interval '1 day' / 2":                      "",
		"SELECT amount / qty ^ 2 FROM orders":              "",
		"SELECT id FROM orders ORDER BY id LIMIT 10 / 4.0": "a division",
		"SELECT amount / (qty + $1) FROM orders":           "a division",
		// The macros named by the client: PostgreSQL has no such function.
		"SELECT _bintrail_pg_div(1.0, 2) FROM orders":  "_bintrail_pg_div",
		"SELECT _bintrail_pg_avgn(amount) FROM orders": "_bintrail_pg_avgn",
	} {
		if veto := CopyScreen(q, divTypes); veto == "" || !strings.Contains(veto, says) {
			t.Errorf("%s: veto %q, want it sent to the source (%q)", q, veto, says)
		}
	}
	// Without the columns' types nothing is shown: every division and every
	// avg goes to the source.
	for _, q := range []string{"SELECT amount / qty FROM orders", "SELECT avg(amount) FROM orders", "SELECT sum(amount) / 2.0 FROM orders"} {
		if veto := CopyScreen(q, nil); veto == "" {
			t.Errorf("without types, %s: left to the copy", q)
		}
	}
}

func TestExactClass(t *testing.T) {
	of := func(name string) []string { return divTypes[name] }
	for expr, want := range map[string]exactCls{
		"amount": clsDecimal, "qty": clsWhole32, "id": clsWhole64, "pack": clsWhole32, "o.amount": clsDecimal, "s.o.qty": clsWhole32,
		"1": clsWhole32, "1.0": clsDecimal, "12345678901": clsWhole64, "-3": clsWhole32, "- 2.5": clsDecimal,
		"qty * 2": clsWhole32, "qty * id": clsWhole64, "qty * amount": clsDecimal, "(qty + 1) * (pack - 2)": clsWhole32, "amount - 1": clsDecimal,
		"count(*)": clsWhole64, "count(DISTINCT note)": clsWhole64, "length(note)": clsWhole32, "position('a' IN note)": clsWhole32,
		"sum(qty)": clsWhole64, "sum(amount)": clsDecimal, "sum(qty * amount)": clsDecimal, "min(qty)": clsWhole32, "max(amount)": clsDecimal, "abs(id)": clsWhole64,
		"coalesce(amount, 0)": clsDecimal, "coalesce(qty, 0)": clsWhole32, "nullif(qty, 0)": clsWhole32, "greatest(qty, id, 1)": clsWhole64, "least(amount, 1)": clsDecimal,
		"_bintrail_pg_div(amount, qty)": clsDecimal, "_bintrail_pg_avgn(qty)": clsDecimal, "_bintrail_pg_round_k(amount, 2, 0.01)": clsDecimal,
		"CASE WHEN qty > 0 THEN amount END": clsDecimal,
		// Not known.
		"": clsUnknown, "ratio": clsUnknown, "note": clsUnknown, "mixed": clsUnknown, "nosuch": clsUnknown, "amount * ratio": clsUnknown,
		"sum(id)": clsUnknown, "sum(ratio)": clsUnknown, "avg(amount)": clsUnknown, "random()": clsUnknown, "amount / qty": clsUnknown, "qty % 2": clsUnknown,
		"'1'": clsUnknown, "$1": clsUnknown, "amount::int": clsUnknown, "CASE WHEN qty > 0 THEN ratio END": clsUnknown,
		"(SELECT sum(amount) FROM orders)": clsUnknown, "sum(amount) OVER ()": clsUnknown, "count(*) FILTER (WHERE qty > 0)": clsUnknown,
		"coalesce(amount, ratio)": clsUnknown, "coalesce()": clsUnknown, "amount.": clsUnknown, "o.": clsUnknown, "(": clsUnknown, "sum(": clsUnknown,
		"+": clsUnknown,
	} {
		toks, _ := lexSQL(expr)
		if got := exactClass(toks, of); got != want {
			t.Errorf("exactClass(%q) = %d, want %d", expr, got, want)
		}
	}
	// A name the statement defines carries a mark beside the column's type:
	// it is not known to be the column.
	marked := func(string) []string { return []string{"DECIMAL(12,2)", notTextType} }
	if toks, _ := lexSQL("amount"); exactClass(toks, marked) != clsUnknown {
		t.Errorf("a name with the mark of a definition is taken for a numeric")
	}
	// A side too long to read is not known.
	long, _ := lexSQL(strings.Repeat("qty + ", maxClassTokens) + "qty")
	if exactClass(long, of) != clsUnknown {
		t.Errorf("a side of %d tokens is taken for a whole number", len(long))
	}
}

// However the divisions are nested, the macro's calls never stand more than
// maxDivDepth deep: each holds its sides several times over in its text.
func TestRewriteDivisionsDepth(t *testing.T) {
	for _, q := range []string{
		"SELECT a / b / c / e / f / g",
		"SELECT a / (b / (c / (e / (f / g))))",
		"SELECT ((((a / b) / c) / e) / f) / g",
		"SELECT (a / b) / ((c / e) / (f / (g / h)))",
		"SELECT sum(a / sum(b / sum(c / sum(e / f))))",
		"SELECT a / b / (c / e / (f / g / (h / i)))",
		"SELECT " + strings.Repeat("(a / ", 30) + "b" + strings.Repeat(")", 30),
		"SELECT " + strings.Repeat("a / (", 30) + "b" + strings.Repeat(")", 30),
	} {
		got := RewriteForCopy(q)
		toks, why := lexSQL(got)
		if why != "" {
			t.Errorf("%q was written as a text the port does not read: %s", q, why)
			continue
		}
		if deep := divDepth(toks); deep > maxDivDepth || deep == 0 {
			t.Errorf("%q: the macro's calls stand %d deep in %q, want 1 to %d", q, deep, got, maxDivDepth)
		}
		if again := rewriteDivisions(got, nil); again != got {
			t.Errorf("rewriteDivisions is not stable on %q: %q", got, again)
		}
	}
}

// The screen's first pass has no column types and reads every name as a
// whole number (screenUntyped): it must not take a numeric over a column for
// two whole numbers, nor refuse an avg the typed pass is still to name. The
// divisor of zero needs no type.
func TestScreenUntyped_divisionAndAvg(t *testing.T) {
	untyped := func(q string) string {
		toks, _ := lexSQL(RewriteForCopy(q))
		return screenUntyped(toks)
	}
	for _, q := range []string{
		"SELECT amount / qty FROM orders",
		"SELECT sum(amount) / count(*) FROM orders",
		"SELECT avg(amount) FROM orders",
		"SELECT qty / 2 FROM orders",
	} {
		if veto := untyped(q); veto != "" {
			t.Errorf("%s: the untyped screen sends it to the source for %s", q, veto)
		}
	}
	if veto := untyped("SELECT amount / 0 FROM orders"); !strings.Contains(veto, "division by zero") {
		t.Errorf("a divisor of zero: untyped veto %q", veto)
	}
}

func TestRewriteDivisionsLongText(t *testing.T) {
	q := "SELECT a / b FROM t WHERE note <> '" + strings.Repeat("x", maxDivisionText) + "'"
	if got := rewriteDivisions(q, nil); got != q {
		t.Errorf("a text past maxDivisionText was rewritten")
	}
	short := "SELECT a / b FROM t WHERE note <> '" + strings.Repeat("x", 1000) + "'"
	if got := rewriteDivisions(short, nil); !strings.Contains(got, pgDiv) {
		t.Errorf("a short text was not rewritten")
	}
}

func TestTypeIsDividable(t *testing.T) {
	for ty, want := range map[string]bool{
		"DECIMAL(12,2)": true, "DECIMAL(20,8)": true, "DECIMAL(1,0)": true, "DECIMAL(9, 3)": true,
		"DECIMAL(21,2)": false, "DECIMAL(20,9)": false, "DECIMAL(38,18)": false,
		"DECIMAL": false, "DECIMAL(12)": false, "DECIMAL(x,2)": false, "DECIMAL(12,2": false, "INTEGER": false, "": false,
	} {
		if got := typeIsDividable(ty); got != want {
			t.Errorf("typeIsDividable(%q) = %v, want %v", ty, got, want)
		}
	}
}
