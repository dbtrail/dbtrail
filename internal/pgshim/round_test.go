package pgshim

import (
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
		{"SELECT round(round(a / b, 2), 1)", "SELECT round(_bintrail_pg_round_div(a, b, 2, 0.01), 1)"},
		{"SELECT round(avg(round(a / b, 1)), 2)", "SELECT _bintrail_pg_round_avg(_bintrail_pg_round_div(a, b, 1, 0.1), 2, 0.01)"},
		// floor(a / b), the HTAP bench's price bands.
		{"SELECT FLOOR(i_price / 5) * 5 AS price_from", "SELECT _bintrail_pg_floor_div(i_price, 5) * 5 AS price_from"},
		{"SELECT floor(100.0 * a / (b * c))", "SELECT _bintrail_pg_floor_div(100.0 * a, (b * c))"},
		{"SELECT round(floor(a / b) / c, 2)", "SELECT _bintrail_pg_round_div(_bintrail_pg_floor_div(a, b), c, 2, 0.01)"},
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
		"SELECT round(x, 2)",                          // no division: another rounding
		"SELECT round(a / b)",                         // round(numeric) is another rounding
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
		"SELECT round(avg(x) OVER (), 2)",             // a window avg
		"SELECT round(avg(x) FILTER (WHERE y), 2)",
		"SELECT round(avg(DISTINCT x), 2)", // a macro takes no DISTINCT
		"SELECT round(avg(x ORDER BY y), 2)",
		"SELECT round(avg(x, y), 2)",
		"SELECT round(3 * avg(x), 2)", // a multiplier other than 10^j
		"SELECT round(0.1 * avg(x), 2)",
		"SELECT round(avg(x) * 100, 2)",
		"SELECT round(100 * avg(x), 14)",             // 16 decimals of the average
		"SELECT round(date_part('day', d) / 7.0, 2)", // a double precision on PostgreSQL
		"SELECT round(avg(date_part('month', d)), 2)",
		"SELECT pg_catalog.round(a / b, 2)",
		"SELECT floor(x)",
		"SELECT floor(a / b * c)",
		"SELECT floor(a, b)",
		"SELECT floor()",
		"SELECT floor(date_part('day', d) / 7.0)",
		"SELECT pg_catalog.floor(a / b)", // qualified
		`SELECT "round"(a / b, 2)`,          // quoted
		"SELECT round(1e2 / 3, 2)",          // the lexer refuses the statement
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
		"o_id": {"INTEGER"}, "o_ol_cnt": {"SMALLINT"}, "o_all_local": {"SMALLINT"}, "ol_d_id": {"SMALLINT"}, "ol_o_id": {"INTEGER"},
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
		"Q12": `SELECT ol_supply_w_id AS w_id, COUNT(*) AS lines,
       ROUND(100.0 * SUM(CASE WHEN ol_delivery_d IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_pending
FROM order_line GROUP BY ol_supply_w_id ORDER BY ol_supply_w_id LIMIT 10`,
	} {
		if veto := CopyScreen(q, types); veto != "" {
			t.Errorf("%s: sent to the source for %s", name, veto)
		}
	}
	// A round of another shape still goes to the source, and so does a
	// statement that names the copy's macros itself: PostgreSQL has no such
	// function.
	for q, says := range map[string]string{
		"SELECT round(d_ytd, 1) FROM district":                           "round",
		"SELECT round(avg(d_tax) OVER (), 2) FROM district":              "round",
		"SELECT _bintrail_pg_round_div(d_ytd, 2, 2, 0.01) FROM district": "_bintrail_pg_round_div",
		`SELECT "_bintrail_pg_round_avg"(d_tax, 2, 0.01) FROM district`:  "_bintrail_pg_round_avg",
	} {
		if veto := CopyScreen(q, types); !strings.Contains(veto, says) {
			t.Errorf("%s: veto %q, want one naming %s", q, veto, says)
		}
	}
}

// Only a connection that can send a statement to its source hands the copy
// the rewritten text: without one the copy answers every statement, and a
// refusal would be an error the client sees.
func TestCopyText_onlyWithAForwardingAccount(t *testing.T) {
	q := "SELECT round(sum(qty) / count(*), 2) FROM pgcmp_orders"
	if got := routeSession(false).copyText(q); got != q {
		t.Errorf("without a forwarding account: %q", got)
	}
	if got := routeSession(true).copyText(q); got != RewriteForCopy(q) || got == q {
		t.Errorf("with a forwarding account: %q", got)
	}
}
