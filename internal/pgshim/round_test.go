package pgshim

import (
	"errors"
	"strings"
	"testing"
)

// RewriteForCopy writes the round shapes the copy rounds as PostgreSQL does
// as its macros, and leaves everything else as written.
func TestRewriteForCopy_rounds(t *testing.T) {
	for _, c := range []struct{ in, want string }{
		// The HTAP bench's reports.
		{"SELECT ROUND(SUM(d_ytd) / NULLIF(SUM(d_next_o_id - 1), 0), 2) AS p FROM district",
			"SELECT _bintrail_pg_round_div(SUM(d_ytd), NULLIF(SUM(d_next_o_id - 1), 0), 2, 0.01) AS p FROM district"},
		{"SELECT ROUND(100 * AVG(d_tax), 2) FROM district", "SELECT _bintrail_pg_round_avg(d_tax, 4, 0.01) FROM district"},
		{"SELECT ROUND(AVG(ol_quantity), 2)", "SELECT _bintrail_pg_round_avg(ol_quantity, 2, 0.01)"},
		{"SELECT ROUND(100.0 * SUM(CASE WHEN a <> b THEN 1 ELSE 0 END) / COUNT(*), 3)",
			"SELECT _bintrail_pg_round_div(100.0 * SUM(CASE WHEN a <> b THEN 1 ELSE 0 END), COUNT(*), 3, 0.001)"},
		{"SELECT ROUND(1.0 * p.together * t.n / (ia.n * ib.n), 1)", "SELECT _bintrail_pg_round_div(1.0 * p.together * t.n, (ia.n * ib.n), 1, 0.1)"},
		{"SELECT ROUND(100.0 * SUM(total) / NULLIF(SUM(SUM(total)) OVER (), 0), 2)",
			"SELECT _bintrail_pg_round_div(100.0 * SUM(total), NULLIF(SUM(SUM(total)) OVER (), 0), 2, 0.01)"},
		// The multiplier of avg moves the decimal point.
		{"SELECT round(100.0 * avg(x), 2)", "SELECT _bintrail_pg_round_avg(x, 4, 0.01)"},
		{"SELECT round(10.00 * avg(x), 1)", "SELECT _bintrail_pg_round_avg(x, 2, 0.1)"},
		{"SELECT round(1 * avg(x), 0)", "SELECT _bintrail_pg_round_avg(x, 0, 1)"},
		{"SELECT round(avg(CASE WHEN f = 1 THEN o END), 2)", "SELECT _bintrail_pg_round_avg(CASE WHEN f = 1 THEN o END, 2, 0.01)"},
		{"SELECT round(a / b, 15)", "SELECT _bintrail_pg_round_div(a, b, 15, 0.000000000000001)"},
		{"SELECT round(100 * avg(x), 13)", "SELECT _bintrail_pg_round_avg(x, 15, 0.0000000000001)"},
		{"SELECT round(a / b, 0)", "SELECT _bintrail_pg_round_div(a, b, 0, 1)"},
		// The rest of the text is kept byte for byte, comments, case and
		// spacing included.
		{"SELECT /* c */ x,  Round( a/b ,2 ) -- t\nFROM t", "SELECT /* c */ x,  _bintrail_pg_round_div(a, b, 2, 0.01) -- t\nFROM t"},
		{"SELECT round(a / b, 2), 'round(a / b, 2)', round(c / d, 1)",
			"SELECT _bintrail_pg_round_div(a, b, 2, 0.01), 'round(a / b, 2)', _bintrail_pg_round_div(c, d, 1, 0.1)"},
		// Nested: each round is read.
		{"SELECT round(round(a / b, 2) / c, 1)", "SELECT _bintrail_pg_round_div(_bintrail_pg_round_div(a, b, 2, 0.01), c, 1, 0.1)"},
		{"SELECT round(round(a / b, 2), 1)", "SELECT _bintrail_pg_round_k(_bintrail_pg_round_div(a, b, 2, 0.01), 1, 0.1)"},
		// round(x, k) of anything else: exactly k decimals, as round(numeric,
		// k) writes them (500 is 500.00), or refused by the macro.
		{"SELECT round(amount, 2)", "SELECT _bintrail_pg_round_k(amount, 2, 0.01)"},
		{"SELECT ROUND( SUM(amount) , 3 )", "SELECT _bintrail_pg_round_k(SUM(amount), 3, 0.001)"},
		{"SELECT round(amount * qty, 0)", "SELECT _bintrail_pg_round_k(amount * qty, 0, 1)"},
		{"SELECT round(x, 15)", "SELECT _bintrail_pg_round_k(x, 15, 0.000000000000001)"},
		{"SELECT round(round(x, 3), 1)", "SELECT _bintrail_pg_round_k(_bintrail_pg_round_k(x, 3, 0.001), 1, 0.1)"},
		// A negative k: no decimals, rounded to tens, hundreds...
		{"SELECT round(x, -1)", "SELECT _bintrail_pg_round_neg(x, -1)"},
		{"SELECT round(x, - 2)", "SELECT _bintrail_pg_round_neg(x, -2)"},
		{"SELECT round(x, -38)", "SELECT _bintrail_pg_round_neg(x, -38)"},
		// round(x) with no digits: round(x, 0) of a numeric, and the avg and
		// division macros at k = 0.
		{"SELECT round(x)", "SELECT _bintrail_pg_round_whole(x)"},
		{"SELECT round(avg(x))", "SELECT _bintrail_pg_round_avg(x, 0, 1)"},
		{"SELECT round(100 * avg(x))", "SELECT _bintrail_pg_round_avg(x, 2, 1)"},
		{"SELECT round(a / b)", "SELECT _bintrail_pg_round_div(a, b, 0, 1)"},
		// A multiplier of avg other than 10^j: the avg is read on its own.
		{"SELECT round(3 * avg(x), 2)", "SELECT _bintrail_pg_round_k(3 * _bintrail_pg_avg(x), 2, 0.01)"},
		// avg(x) outside a round: refused for a numeric or a whole number
		// by the macro (PostgreSQL writes it with as many decimals as each
		// value needs), the engine's own for a float.
		{"SELECT avg(x) FROM t GROUP BY g", "SELECT _bintrail_pg_avg(x) FROM t GROUP BY g"},
		{"SELECT AVG( x + 1 )", "SELECT _bintrail_pg_avg(x + 1)"},
		// A CASE or a COALESCE that holds a rewritten round: each value keeps
		// its own decimals on PostgreSQL (0 stays 0 beside 500.00), so each
		// is checked against the copy's one type.
		{"SELECT CASE WHEN g = 1 THEN round(avg(a), 2) ELSE 0 END FROM t",
			"SELECT _bintrail_pg_unpack(CASE WHEN g = 1 THEN _bintrail_pg_pack(_bintrail_pg_round_avg(a, 2, 0.01)) ELSE _bintrail_pg_pack(0) END) FROM t"},
		{"SELECT CASE g WHEN 1 THEN round(x, 2) WHEN 2 THEN 1.5 END",
			"SELECT _bintrail_pg_unpack(CASE g WHEN 1 THEN _bintrail_pg_pack(_bintrail_pg_round_k(x, 2, 0.01)) WHEN 2 THEN _bintrail_pg_pack(1.5) END)"},
		{"SELECT CASE WHEN c THEN round(x, 2) ELSE NULL END",
			"SELECT _bintrail_pg_unpack(CASE WHEN c THEN _bintrail_pg_pack(_bintrail_pg_round_k(x, 2, 0.01)) ELSE NULL END)"},
		{"SELECT CASE WHEN a THEN CASE WHEN b THEN round(x, 2) ELSE 1 END ELSE 0 END",
			"SELECT _bintrail_pg_unpack(CASE WHEN a THEN _bintrail_pg_pack(_bintrail_pg_unpack(CASE WHEN b THEN _bintrail_pg_pack(_bintrail_pg_round_k(x, 2, 0.01)) ELSE _bintrail_pg_pack(1) END)) ELSE _bintrail_pg_pack(0) END)"},
		{"SELECT coalesce(round(avg(a), 2), 0)",
			"SELECT _bintrail_pg_unpack(coalesce(_bintrail_pg_packn(_bintrail_pg_round_avg(a, 2, 0.01)), _bintrail_pg_packn(0)))"},
		// An avg the macros do not take, inside a round: the round is the
		// macro's, which refuses the engine's float; the avg is left, and
		// refused too.
		{"SELECT round(avg(x) OVER (), 2)", "SELECT _bintrail_pg_round_k(avg(x) OVER (), 2, 0.01)"},
		// An avg rounded otherwise is read on its own, and the round as any
		// other: both refused for what the copy cannot answer.
		{"SELECT round(avg(x), -1)", "SELECT _bintrail_pg_round_neg(_bintrail_pg_avg(x), -1)"},
		{"SELECT round(100 * avg(x), 14)", "SELECT round(100 * _bintrail_pg_avg(x), 14)"},
		{"SELECT round(avg(date_part('month', d)), 2)", "SELECT round(_bintrail_pg_avg(date_part('month', d)), 2)"},
		// Inside the round, a division or an epoch a macro took.
		{"SELECT round(round(a / b, 2) * 3, 1)", "SELECT _bintrail_pg_round_k(_bintrail_pg_round_div(a, b, 2, 0.01) * 3, 1, 0.1)"},
		{"SELECT round(avg(x) * 100, 2)", "SELECT _bintrail_pg_round_k(_bintrail_pg_avg(x) * 100, 2, 0.01)"},
		// A name that stands for a round, from a subquery or a WITH, mixes
		// its decimals as the round does.
		{"SELECT coalesce(r, 0) FROM (SELECT round(a, 2) AS r FROM t) s",
			"SELECT _bintrail_pg_unpack(coalesce(_bintrail_pg_packn(r), _bintrail_pg_packn(0))) FROM (SELECT _bintrail_pg_round_k(a, 2, 0.01) AS r FROM t) s"},
		{"WITH s AS (SELECT round(a, 2) * 2 r, b FROM t) SELECT CASE WHEN c THEN s.r ELSE 0 END FROM s",
			"WITH s AS (SELECT _bintrail_pg_round_k(a, 2, 0.01) * 2 r, b FROM t) SELECT _bintrail_pg_unpack(CASE WHEN c THEN _bintrail_pg_pack(s.r) ELSE _bintrail_pg_pack(0) END) FROM s"},
		{"WITH s(r) AS (SELECT round(a, 2) FROM t) SELECT coalesce(r, 0) FROM s",
			"WITH s(r) AS (SELECT _bintrail_pg_round_k(a, 2, 0.01) FROM t) SELECT _bintrail_pg_unpack(coalesce(_bintrail_pg_packn(r), _bintrail_pg_packn(0))) FROM s"},
		{"SELECT coalesce(s.round, 0) FROM (SELECT round(a, 2) FROM t) s",
			"SELECT _bintrail_pg_unpack(coalesce(_bintrail_pg_packn(s.round), _bintrail_pg_packn(0))) FROM (SELECT _bintrail_pg_round_k(a, 2, 0.01) FROM t) s"},
		// A name of something else is left alone.
		{"SELECT coalesce(b, 0) FROM (SELECT round(a, 2) AS r, b FROM t) s",
			"SELECT coalesce(b, 0) FROM (SELECT _bintrail_pg_round_k(a, 2, 0.01) AS r, b FROM t) s"},
		// The round in a condition decides a branch, it is not one.
		{"SELECT CASE WHEN round(x, 2) > 1 THEN 1 ELSE 0 END", "SELECT CASE WHEN _bintrail_pg_round_k(x, 2, 0.01) > 1 THEN 1 ELSE 0 END"},
		{"SELECT round(avg(round(a / b, 1)), 2)", "SELECT _bintrail_pg_round_avg(_bintrail_pg_round_div(a, b, 1, 0.1), 2, 0.01)"},
		// floor(a / b), the HTAP bench's price bands.
		{"SELECT FLOOR(i_price / 5) * 5 AS price_from", "SELECT _bintrail_pg_floor_div(i_price, 5) * 5 AS price_from"},
		{"SELECT floor(100.0 * a / (b * c))", "SELECT _bintrail_pg_floor_div(100.0 * a, (b * c))"},
		{"SELECT round(floor(a / b) / c, 2)", "SELECT _bintrail_pg_round_div(_bintrail_pg_floor_div(a, b), c, 2, 0.01)"},
		// Ten-minute buckets, the HTAP bench's Q04 and Q13.
		{"SELECT to_timestamp(FLOOR(EXTRACT(EPOCH FROM o.o_entry_d) / 600) * 600) AS period_start",
			"SELECT _bintrail_pg_to_timestamp(_bintrail_pg_floor_epoch(o.o_entry_d, 600) * 600) AS period_start"},
		{"SELECT extract(epoch from t)", "SELECT _bintrail_pg_epoch(t)"},
		// floor(extract(epoch from x) / N), N whole seconds: from the
		// timestamp's microseconds. N is written as the copy reads it.
		{"SELECT floor(extract(epoch from t) / 1)", "SELECT _bintrail_pg_floor_epoch(t, 1)"},
		{"SELECT floor(extract(epoch from t) / 0600)", "SELECT _bintrail_pg_floor_epoch(t, 600)"},
		{"SELECT floor(extract(epoch from t) / 999999999)", "SELECT _bintrail_pg_floor_epoch(t, 999999999)"},
		{"SELECT floor( Extract( Epoch From (t + interval '1 hour') ) / 60 )", "SELECT _bintrail_pg_floor_epoch((t + interval '1 hour'), 60)"},
		{"SELECT floor(extract(epoch from max(t)) / 3600)", "SELECT _bintrail_pg_floor_epoch(max(t), 3600)"},
		// Any other division of an epoch goes the general way.
		{"SELECT floor(extract(epoch from t) / 0)", "SELECT _bintrail_pg_floor_div(_bintrail_pg_epoch(t), 0)"},
		{"SELECT floor(extract(epoch from t) / 1000000000)", "SELECT _bintrail_pg_floor_div(_bintrail_pg_epoch(t), 1000000000)"},
		{"SELECT floor(extract(epoch from t) / 600.0)", "SELECT _bintrail_pg_floor_div(_bintrail_pg_epoch(t), 600.0)"},
		{"SELECT floor(extract(epoch from t) / (600))", "SELECT _bintrail_pg_floor_div(_bintrail_pg_epoch(t), (600))"},
		{"SELECT floor(extract(epoch from t) / n)", "SELECT _bintrail_pg_floor_div(_bintrail_pg_epoch(t), n)"},
		{"SELECT floor(2 * extract(epoch from t) / 600)", "SELECT _bintrail_pg_floor_div(2 * _bintrail_pg_epoch(t), 600)"},
		{"SELECT floor(extract(epoch from t) * 2 / 600)", "SELECT _bintrail_pg_floor_div(_bintrail_pg_epoch(t) * 2, 600)"},
		{"SELECT floor(extract(hour from t) / 6)", "SELECT _bintrail_pg_floor_div(extract(hour from t), 6)"},
	} {
		if got := RewriteForCopy(c.in); got != c.want {
			t.Errorf("%q\n got %q\nwant %q", c.in, got, c.want)
		}
	}
}

// Every other round is left as written, and the screen sends it to the
// source.
func TestRewriteForCopy_leavesOtherShapes(t *testing.T) {
	for _, in := range []string{
		"SELECT round(x, $1)",                         // k must be written
		"SELECT round(x, 2.0)",                        // k must be a whole number
		"SELECT round(x, 16)",                         // past what the copy rounds
		"SELECT round(x, -39)",                        // past 38 digits
		"SELECT round(avg(DISTINCT x), 2)",            // a macro takes no DISTINCT
		"SELECT round(a / b + c, 2)",                  // a division inside, not last
		"SELECT round(x::numeric, 2)",                 // the copy's numeric has three decimals
		"SELECT round(CAST(x AS numeric(10,4)), 2)",   //
		"SELECT avg(x::numeric)",                      //
		"SELECT round(ceil(q), 2)",                    // a double precision on PostgreSQL
		"SELECT round(trunc(q))",                      //
		"SELECT round(sum(q) / 2 * 3)",                // a division of whole numbers may be a numeric's there
		"SELECT round(extract(second from t), 2)",     // a whole number here, a fraction there
		"SELECT round(date_part('day', d), 2)",        // a double precision on PostgreSQL
		"SELECT round()",                              // nothing to round
		"SELECT avg(DISTINCT x)",                      // a macro takes no DISTINCT
		"SELECT avg(ALL x)",                           //
		"SELECT avg(x) OVER (PARTITION BY g)",         // a window avg
		"SELECT avg(x) FILTER (WHERE y)",              //
		"SELECT avg(x ORDER BY y)",                    //
		"SELECT avg(x, y)",                            //
		"SELECT avg()",                                //
		"SELECT t.avg, avg FROM t",                    // a column named avg
		"SELECT pg_catalog.avg(x)",                    // qualified
		"SELECT CASE WHEN c THEN amount ELSE 0 END",   // no round in it
		"SELECT coalesce(amount, 0)",                  //
		"SELECT round(a / b, $1)",                     // k must be written
		"SELECT round(a / b, -1)",                     // a negative k
		"SELECT round(a / b, 2.0)",                    // k must be a whole number
		"SELECT round(a / b, 16)",                     // past what the copy can round
		"SELECT round(a / b, 2, 3)",                   // three arguments
		"SELECT round(a / b * c, 2)",                  // the division is not last
		"SELECT round(a + b / c, 2)",                  // nor here
		"SELECT round(a / b / c, 2)",                  // two divisions
		"SELECT round(-a / b, 2)",                     // a sign in front
		"SELECT round(a % b / c, 2)",                  // another operator
		"SELECT round(a / b::numeric, 2)",             // a cast
		"SELECT round(CASE WHEN x THEN a END / b, 2)", // CASE at the top of a side
		"SELECT round(a / 'x', 2)",                    // a string
		"SELECT round(a / $1, 2)",                     // a parameter
		"SELECT round(numeric '1.5' / b, 2)",          // a typed literal
		"SELECT round(date_part('day', d) / 7.0, 2)",  // a double precision on PostgreSQL
		"SELECT pg_catalog.round(a / b, 2)",
		"SELECT floor(x)",
		"SELECT floor(a / b * c)",
		"SELECT floor(a, b)",
		"SELECT floor()",
		"SELECT floor(date_part('day', d) / 7.0)",
		"SELECT pg_catalog.floor(a / b)",      // qualified
		"SELECT extract(year from t)",         // the screen reads year, month and day as they are
		"SELECT extract(epoch)",               // no FROM
		"SELECT to_timestamp('2026', 'YYYY')", // another function, of text
		"SELECT to_timestamp()",
		`SELECT "round"(a / b, 2)`, // quoted
		"SELECT round(1e2 / 3, 2)", // the lexer refuses the statement
		"SELECT x FROM t",
		"",
	} {
		if got := RewriteForCopy(in); got != in {
			t.Errorf("%q was rewritten: %q", in, got)
		}
	}
}

// Each span is where its token is written.
func TestLexSQLSpans(t *testing.T) {
	q := `SELECT a.b, 'it''s', "Q", $1, 1.5, x::date, (y) -- c
	/* d */ FROM t WHERE z >= 2`
	toks, spans, why := lexSQLSpans(q)
	if why != "" || len(spans) != len(toks) {
		t.Fatalf("why %q, %d spans for %d tokens", why, len(spans), len(toks))
	}
	prev := 0
	for i, tk := range toks {
		s := q[spans[i][0]:spans[i][1]]
		if spans[i][0] < prev {
			t.Errorf("token %d %q starts at %d, before the last one ended (%d)", i, tk.text, spans[i][0], prev)
		}
		prev = spans[i][1]
		switch tk.kind {
		case rtString:
			if s != `'it''s'` {
				t.Errorf("string span %q", s)
			}
		case rtQuoted:
			if s != `"Q"` {
				t.Errorf("quoted span %q", s)
			}
		default:
			if strings.ToLower(s) != tk.text {
				t.Errorf("token %d %q has span %q", i, tk.text, s)
			}
		}
	}
	// A statement the lexer refuses still has a span for each token it read.
	toks, spans, why = lexSQLSpans("SELECT a, $$x$$")
	if why == "" || len(spans) != len(toks) {
		t.Errorf("why %q, %d spans for %d tokens", why, len(spans), len(toks))
	}
}

// The reports of the HTAP bench that only round, avg and a division kept
// from the copy: rewritten, the screen lets the copy answer them.
func TestCopyScreen_benchReportsWithRound(t *testing.T) {
	types := map[string][]string{
		"d_w_id": {"INTEGER"}, "d_next_o_id": {"INTEGER"}, "d_ytd": {"DECIMAL(12,2)"}, "d_tax": {"DECIMAL(4,4)"},
		"ol_w_id": {"INTEGER"}, "ol_supply_w_id": {"INTEGER"}, "ol_quantity": {"SMALLINT"}, "ol_amount": {"DECIMAL(6,2)"},
		"ol_delivery_d": {"TIMESTAMP WITH TIME ZONE"}, "ol_number": {"SMALLINT"}, "ol_i_id": {"INTEGER"},
		"i_id": {"INTEGER"}, "i_price": {"DECIMAL(5,2)"}, "i_data": {"VARCHAR"}, "o_w_id": {"INTEGER"}, "o_d_id": {"SMALLINT"},
		"o_id": {"INTEGER"}, "o_entry_d": {"TIMESTAMP WITH TIME ZONE"}, "o_ol_cnt": {"SMALLINT"}, "o_all_local": {"SMALLINT"}, "ol_d_id": {"SMALLINT"}, "ol_o_id": {"INTEGER"},
	}
	for name, q := range map[string]string{
		"Q01": `SELECT d_w_id AS w_id, SUM(d_next_o_id - 1) AS orders_taken, SUM(d_ytd) AS ytd_payments,
       ROUND(SUM(d_ytd) / NULLIF(SUM(d_next_o_id - 1), 0), 2) AS payment_per_order,
       MAX(d_next_o_id) - MIN(d_next_o_id) AS district_spread, ROUND(100 * AVG(d_tax), 2) AS avg_tax_pct
FROM district WHERE d_w_id BETWEEN 1 AND 20 GROUP BY d_w_id ORDER BY orders_taken DESC, w_id LIMIT 10`,
		"Q03": `SELECT ol_w_id AS w_id, COUNT(*) AS order_lines, ROUND(AVG(ol_quantity), 2) AS avg_quantity,
       ROUND(100.0 * SUM(CASE WHEN ol_supply_w_id <> ol_w_id THEN 1 ELSE 0 END) / COUNT(*), 3) AS pct_remote
FROM order_line WHERE ol_w_id BETWEEN 1 AND 20 GROUP BY ol_w_id ORDER BY ol_w_id`,
		"Q02": `SELECT price_from, price_from + 5 AS price_to, COUNT(*) AS items, ROUND(AVG(i_price), 2) AS avg_price,
       SUM(is_original) AS original_items, ROUND(100.0 * SUM(is_original) / COUNT(*), 2) AS pct_original
FROM (SELECT i_price, FLOOR(i_price / 5) * 5 AS price_from, CASE WHEN i_data LIKE '%ORIGINAL%' THEN 1 ELSE 0 END AS is_original
      FROM item WHERE i_id BETWEEN 1 AND 5000) t
GROUP BY price_from ORDER BY price_from`,
		"Q09": `WITH order_totals AS (SELECT o.o_w_id, o.o_d_id, o.o_id, o.o_ol_cnt, o.o_all_local, SUM(ol.ol_amount) AS total
  FROM orders o JOIN order_line ol ON ol.ol_w_id = o.o_w_id AND ol.ol_d_id = o.o_d_id AND ol.ol_o_id = o.o_id
  WHERE o.o_w_id BETWEEN 1 AND 20 GROUP BY o.o_w_id, o.o_d_id, o.o_id, o.o_ol_cnt, o.o_all_local),
deciles AS (SELECT t.*, NTILE(10) OVER (ORDER BY t.total) AS decile FROM order_totals t)
SELECT decile, COUNT(*) AS orders, MIN(total) AS min_value, MAX(total) AS max_value, ROUND(AVG(total), 2) AS avg_value,
       ROUND(AVG(o_ol_cnt), 2) AS avg_lines, ROUND(100.0 * SUM(total) / NULLIF(SUM(SUM(total)) OVER (), 0), 2) AS pct_of_revenue
FROM deciles GROUP BY decile ORDER BY decile`,
		"Q04": `SELECT to_timestamp(FLOOR(EXTRACT(EPOCH FROM o.o_entry_d) / 600) * 600) AS period_start,
       SUM(CASE WHEN ol.ol_number = 1 THEN 1 ELSE 0 END) AS orders, COUNT(*) AS order_lines, SUM(ol.ol_amount) AS revenue,
       ROUND(1.0 * COUNT(*) / NULLIF(SUM(CASE WHEN ol.ol_number = 1 THEN 1 ELSE 0 END), 0), 2) AS lines_per_order
FROM orders o JOIN order_line ol ON ol.ol_w_id = o.o_w_id AND ol.ol_d_id = o.o_d_id AND ol.ol_o_id = o.o_id
WHERE o.o_w_id BETWEEN 1 AND 20 AND o.o_entry_d >= TIMESTAMP '2026-11-18 10:00:00' AND o.o_entry_d < TIMESTAMP '2026-11-18 18:00:00'
GROUP BY 1 ORDER BY 1`,
		"Q12": `SELECT ol_supply_w_id AS w_id, COUNT(*) AS lines,
       ROUND(100.0 * SUM(CASE WHEN ol_delivery_d IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_pending
FROM order_line GROUP BY ol_supply_w_id ORDER BY ol_supply_w_id LIMIT 10`,
	} {
		if veto := CopyScreen(q, types); veto != "" {
			t.Errorf("%s: sent to the source for %s", name, veto)
		}
	}
	// round(x, k), round(x) and a CASE or COALESCE around a round are the
	// copy's too, as the macros write them.
	for _, q := range []string{
		"SELECT round(d_ytd, 1), round(d_ytd, 3), round(d_ytd), round(d_ytd, -2) FROM district",
		"SELECT d_w_id, CASE WHEN d_w_id = 1 THEN round(avg(d_tax), 2) ELSE 0 END, coalesce(round(sum(d_ytd), 1), 0) FROM district GROUP BY d_w_id",
	} {
		if veto := CopyScreen(q, types); veto != "" {
			t.Errorf("%s: sent to the source for %s", q, veto)
		}
	}
	// A round of another shape still goes to the source, and so does a
	// statement that names the copy's macros itself: PostgreSQL has no such
	// function.
	for q, says := range map[string]string{
		"SELECT round(d_ytd, 16) FROM district":                          "round",
		"SELECT round(avg(d_tax) OVER (), 2) FROM district":              "avg",
		"SELECT avg(d_tax) FROM district":                                "_bintrail_pg_avg",
		"SELECT round(d_ytd, 2) FROM district UNION SELECT 0":            "UNION",
		"SELECT greatest(round(d_ytd, 2), 0) FROM district":              "greatest",
		"SELECT _bintrail_pg_round_div(d_ytd, 2, 2, 0.01) FROM district": "_bintrail_pg_round_div",
		`SELECT "_bintrail_pg_round_avg"(d_tax, 2, 0.01) FROM district`:  "_bintrail_pg_round_avg",
	} {
		if veto := CopyScreen(q, types); !strings.Contains(veto, says) {
			t.Errorf("%s: veto %q, want one naming %s", q, veto, says)
		}
	}
}

// Every connection hands the copy its round and avg rewritten: without a
// forwarding account the copy answers alone, and a round it writes with
// other decimals than PostgreSQL's (500 for 500.00) would reach the client.
// floor, extract(epoch) and to_timestamp are rewritten only where a refusal
// can go to the source.
func TestCopyText_roundsOnEveryConnection(t *testing.T) {
	for _, q := range []string{
		"SELECT round(sum(qty) / count(*), 2) FROM pgcmp_orders",
		"SELECT ROUND(AVG(amount), 2) AS avg_amount FROM pgcmp_orders",
		"SELECT round(amount, 3), avg(ratio) FROM pgcmp_orders",
	} {
		for _, forward := range []bool{false, true} {
			got, err := routeSession(forward).copyText(q)
			if err != nil || got != RewriteForCopy(q) || got == q {
				t.Errorf("forwarding %v: %q -> %q, %v", forward, q, got, err)
			}
		}
	}
	q := "SELECT floor(amount / 5), extract(epoch from placed_at) FROM pgcmp_orders"
	if got, err := routeSession(false).copyText(q); err != nil || got != q {
		t.Errorf("without a forwarding account: %q, %v", got, err)
	}
	if got, err := routeSession(true).copyText(q); err != nil || got != RewriteForCopy(q) || got == q {
		t.Errorf("with a forwarding account: %q, %v", got, err)
	}
}

// Without a forwarding account, what is left of a round or an avg the copy
// cannot answer as PostgreSQL does is refused with an error the client
// sees, never answered otherwise; with one, the screen sends it to the
// source, and copyText refuses nothing.
func TestCopyText_refusesWhatTheCopyWouldAnswerOtherwise(t *testing.T) {
	for q, says := range map[string]string{
		"SELECT avg(DISTINCT qty) FROM pgcmp_orders":                                    "avg(",
		"SELECT avg(qty) OVER (ORDER BY id) FROM pgcmp_orders":                          "avg(",
		"SELECT round(amount, 16) FROM pgcmp_orders":                                    "round(",
		"SELECT round(amount, $1) FROM pgcmp_orders":                                    "round(",
		"SELECT round(amount / qty + 1, 2) FROM pgcmp_orders":                           "round(",
		"SELECT pg_catalog.round(amount, 2) FROM pgcmp_orders":                          "round(",
		`SELECT "round"(amount, 2) FROM pgcmp_orders`:                                   "round(",
		`SELECT "AVG"(amount) FROM pgcmp_orders`:                                        "",
		"SELECT round(1e2, 2)":                                                          "round(",
		"SELECT E'\\n', avg(qty) FROM pgcmp_orders":                                     "avg(",
		"SELECT round(amount, 2) FROM pgcmp_orders UNION ALL SELECT 0":                  "UNION",
		"SELECT 0 INTERSECT SELECT round(amount, 2) FROM pgcmp_orders":                  "INTERSECT",
		"SELECT greatest(round(amount, 2), 0) FROM pgcmp_orders":                        "greatest",
		"SELECT least(0, round(avg(amount), 2)) FROM pgcmp_orders":                      "least",
		"SELECT nullif(round(amount, 0), 0.5) FROM pgcmp_orders":                        "nullif",
		"SELECT ARRAY[round(amount, 2), 0] FROM pgcmp_orders":                           "ARRAY",
		"SELECT * FROM (VALUES (round(1.5, 2)), (0)) v(x)":                              "VALUES",
		"SELECT round(amount, 2) FROM pgcmp_orders EXCEPT SELECT 1":                     "EXCEPT",
		"SELECT round(ratio::numeric, 2) FROM pgcmp_orders":                             "round(",
		"SELECT round(ceil(qty), 2) FROM pgcmp_orders":                                  "round(",
		"SELECT round/**/(qty, 2) FROM pgcmp_orders WHERE note <> $$x$$":                "round(",
		"SELECT greatest(r, 0) FROM (SELECT round(amount, 2) AS r FROM pgcmp_orders) s": "greatest",
		"SELECT lag(round(amount, 2), 1, 0) OVER (ORDER BY id) FROM pgcmp_orders":       "lag",
	} {
		_, err := routeSession(false).copyText(q)
		var own *Error
		if !errors.As(err, &own) || own.Code != "0A000" || !strings.Contains(own.Message, says) {
			t.Errorf("%s: %v, want a 0A000 refusal naming %q", q, err, says)
		}
		if got, err := routeSession(true).copyText(q); err != nil || got != RewriteForCopy(q) {
			t.Errorf("with a forwarding account %s: %q, %v", q, got, err)
		}
	}
	for _, q := range []string{
		"SELECT x FROM t",
		"SELECT avg, t.avg FROM t",
		"SELECT 'round(x, 2)', 'avg(' FROM t",
		"SELECT round(amount, 2), round(amount), round(amount, -1), avg(ratio) FROM pgcmp_orders",
		"SELECT round(a / nullif(b, 0), 2) FROM t",
		"SELECT round(greatest(a, 0), 2) FROM t",
		"SELECT nullif(x, 0) FROM t UNION SELECT 1",
		"SELECT coalesce(round(avg(amount), 2), 0), CASE WHEN qty > 1 THEN round(amount, 1) ELSE 0 END FROM pgcmp_orders",
		"SELECT floor(amount / 5) FROM pgcmp_orders",
		"SELECT lag(round(amount, 2)) OVER (ORDER BY id), lag(qty, 1, 0) OVER (ORDER BY id) FROM pgcmp_orders",
		"",
	} {
		if _, err := routeSession(false).copyText(q); err != nil {
			t.Errorf("%q refused: %v", q, err)
		}
	}
}

// A prepared statement is screened with $1 and run with each argument
// written in its place: the portal's text must be written for the copy with
// the same macros as the statement's, or the copy is not handed it.
func TestCopyText_portalWrittenAsItsStatement(t *testing.T) {
	const long = "1234567890123456789"
	for _, c := range []struct {
		name, screened, bound string
		refused               bool
	}{
		{"an argument outside the round", "SELECT round(amount / 3, 2) FROM t WHERE id < $1", "SELECT round(amount / 3, 2) FROM t WHERE id < 9", false},
		{"a long argument, as the port writes it", "SELECT round(amount / 3, 2) FROM t WHERE id < $1", "SELECT round(amount / 3, 2) FROM t WHERE id < CAST('" + long + "' AS BIGINT)", false},
		{"a text the port does not read", "SELECT round(amount / 3, 2) FROM t WHERE id < $1", "SELECT round(amount / 3, 2) FROM t WHERE id < " + long, true},
		{"unread, with nothing rewritten", "SELECT amount FROM t WHERE id < $1", "SELECT amount FROM t WHERE id < " + long, false},
		{"a typed NULL inside a round", "SELECT round(coalesce($1, amount), 4) FROM t", "SELECT round(coalesce(CAST(NULL AS BIGINT), amount), 4) FROM t", true},
		{"a float inside a round", "SELECT round(amount * $1, 2) FROM t", "SELECT round(amount * CAST('1.5' AS DOUBLE), 2) FROM t", true},
		{"a number inside a round", "SELECT round(amount * $1, 2) FROM t", "SELECT round(amount * 3, 2) FROM t", false},
		{"a typed NULL inside an avg", "SELECT round(avg(coalesce(qty, $1)), 2) FROM t", "SELECT round(avg(coalesce(qty, CAST(NULL AS BIGINT))), 2) FROM t", true},
		{"a date inside a floor", "SELECT floor(amount / $1) FROM t", "SELECT floor(amount / CAST('2026-01-01' AS DATE)) FROM t", true},
		{"one round of two", "SELECT round(amount, 2), round(coalesce($1, amount), 1) FROM t", "SELECT round(amount, 2), round(coalesce(CAST(NULL AS INTEGER), amount), 1) FROM t", true},
		// A string that spells a macro's name is not one: it neither breaks
		// a statement written alike nor stands in for a round left as written.
		{"a string that names a macro", "SELECT round(amount, 2) FROM t WHERE note = $1", "SELECT round(amount, 2) FROM t WHERE note = '_bintrail_pg_round_k'", false},
		{"a string in place of the macro", "SELECT round(coalesce($1, amount), 4) FROM t WHERE note = $2", "SELECT round(coalesce(CAST(NULL AS BIGINT), amount), 4) FROM t WHERE note = '_bintrail_pg_round_k'", true},
		// A text the port does not read has no words to tell: a string in it
		// that spells the macro's name does not make it the statement's.
		{"unread, with a string in place of the macro", "SELECT round(amount, 2) FROM t WHERE note = $1 AND id < $2", "SELECT round(amount, 2) FROM t WHERE note = '_bintrail_pg_round_k' AND id < " + long, true},
		{"a quoted name that spells a macro", "SELECT round(coalesce($1, amount), 4) FROM t WHERE note = $2", `SELECT round(coalesce(CAST(NULL AS BIGINT), amount), 4) AS "_bintrail_pg_round_k" FROM t WHERE note = 'x'`, true},
	} {
		s := routeSession(true)
		s.screened = c.screened
		got, err := s.copyText(c.bound)
		var own *Error
		switch {
		case c.refused && (!errors.As(err, &own) || own.Code != "0A000"):
			t.Errorf("%s: copyText(%q) = %q, %v; want a 0A000 refusal", c.name, c.bound, got, err)
		case !c.refused && (err != nil || got != RewriteForCopy(c.bound)):
			t.Errorf("%s: copyText(%q) = %q, %v; want it written for the copy", c.name, c.bound, got, err)
		}
		// A statement of the simple protocol is the text the screen read:
		// nothing to hold it to.
		if got, err := routeSession(true).copyText(c.bound); err != nil || got != RewriteForCopy(c.bound) {
			t.Errorf("%s, with no statement behind it: copyText(%q) = %q, %v", c.name, c.bound, got, err)
		}
	}
}
