package pgshim

import (
	"strings"
	"testing"
)

// The report shapes of #3219 over a TPC-C schema. What the copy answers the
// same was measured by internal/pgcompare (reportShapes, charCases).

// tpccTypes are the copy's types for the columns the statements read; the
// char(2) column carries the mark of a column the source pads.
func tpccTypes(t *testing.T) map[string][]string {
	t.Helper()
	// col is a column with the type the snapshot recorded for it on the
	// source (pg_attribute.atttypid, and atttypmod: a char(2) is 6).
	col := func(name, typ string, oid uint32, mod int32) CatalogColumn {
		return CatalogColumn{Name: name, Type: typ, SourceType: oid, SourceTypeMod: mod}
	}
	const (
		int4, varchar, numeric, timestamptz = 23, 1043, 1700, 1184
	)
	tables := []CatalogTable{
		{Schema: "public", Name: "customer", Columns: []CatalogColumn{
			col("c_w_id", "INTEGER", int4, -1), col("c_d_id", "INTEGER", int4, -1), col("c_id", "INTEGER", int4, -1),
			col("c_credit", "VARCHAR", oidBpchar, 6), col("c_last", "VARCHAR", varchar, 20)}},
		{Schema: "public", Name: "orders", Columns: []CatalogColumn{
			col("o_w_id", "INTEGER", int4, -1), col("o_d_id", "INTEGER", int4, -1), col("o_id", "INTEGER", int4, -1),
			col("o_c_id", "INTEGER", int4, -1), col("o_entry_d", "TIMESTAMP WITH TIME ZONE", timestamptz, -1)}},
		{Schema: "public", Name: "order_line", Columns: []CatalogColumn{
			col("ol_w_id", "INTEGER", int4, -1), col("ol_d_id", "INTEGER", int4, -1), col("ol_o_id", "INTEGER", int4, -1),
			col("ol_i_id", "INTEGER", int4, -1), col("ol_amount", "DECIMAL(6,2)", numeric, 393222)}},
		{Schema: "public", Name: "item", Columns: []CatalogColumn{
			col("i_id", "INTEGER", int4, -1), col("i_name", "VARCHAR", varchar, 28)}},
	}
	return screenTypes(tables, nil)
}

const (
	tpccGroupByChar = `SELECT c.c_credit, count(*) AS lines, sum(ol.ol_amount) AS revenue
FROM customer c
JOIN orders o ON o.o_w_id = c.c_w_id AND o.o_d_id = c.c_d_id AND o.o_c_id = c.c_id
JOIN order_line ol ON ol.ol_w_id = o.o_w_id AND ol.ol_d_id = o.o_d_id AND ol.ol_o_id = o.o_id
WHERE c.c_w_id BETWEEN 61 AND 100
GROUP BY c.c_credit`
	tpccDaily = `SELECT date_trunc('day', o.o_entry_d) AS day, count(*) AS lines, sum(ol.ol_amount) AS revenue
FROM orders o JOIN order_line ol
  ON ol.ol_w_id = o.o_w_id AND ol.ol_d_id = o.o_d_id AND ol.ol_o_id = o.o_id
WHERE o.o_w_id BETWEEN 61 AND 100
GROUP BY 1 ORDER BY 1`
	tpccRanking = `SELECT t.ol_w_id, t.r, t.ol_i_id, i.i_name, t.revenue
FROM (SELECT ol_w_id, ol_i_id, sum(ol_amount) AS revenue,
             rank() OVER (PARTITION BY ol_w_id ORDER BY sum(ol_amount) DESC, ol_i_id) AS r
      FROM order_line WHERE ol_w_id BETWEEN 61 AND 100
      GROUP BY ol_w_id, ol_i_id) t
JOIN item i ON i.i_id = t.ol_i_id
WHERE t.r <= 3
ORDER BY t.ol_w_id, t.r`
	tpccAverage = `WITH per AS (
  SELECT o.o_w_id AS w, o.o_d_id AS d, o.o_c_id AS c, sum(ol.ol_amount) AS spent
  FROM orders o JOIN order_line ol
    ON ol.ol_w_id = o.o_w_id AND ol.ol_d_id = o.o_d_id AND ol.ol_o_id = o.o_id
  WHERE o.o_w_id BETWEEN 61 AND 100
  GROUP BY o.o_w_id, o.o_d_id, o.o_c_id),
avgd AS (SELECT w, d, avg(spent) AS a FROM per GROUP BY w, d)
SELECT per.w, per.d, count(*) FILTER (WHERE per.spent > 2 * avgd.a) AS big_spenders,
       max(per.spent) AS top_spend
FROM per JOIN avgd ON avgd.w = per.w AND avgd.d = per.d
GROUP BY per.w, per.d
ORDER BY big_spenders DESC, per.w, per.d LIMIT 20`
)

func TestCopyScreen_reportShapes3219(t *testing.T) {
	types := tpccTypes(t)
	stays := map[string]string{
		"a char(n) column as the key of GROUP BY":              tpccGroupByChar,
		"the same, sorted by a count":                          tpccGroupByChar + " ORDER BY count(*) DESC, sum(ol.ol_amount)",
		"a series by day over a timestamp with time zone":      tpccDaily,
		"by month, the column without its table":               `SELECT date_trunc('month', o_entry_d), count(*) FROM orders GROUP BY 1 ORDER BY 1`,
		"sorted by the date_trunc itself":                      `SELECT date_trunc('year', o.o_entry_d), count(*) FROM orders o GROUP BY date_trunc('year', o.o_entry_d) ORDER BY date_trunc('year', o.o_entry_d)`,
		"date_part of a day as a key":                          `SELECT date_part('day', o_entry_d), count(*) FROM orders GROUP BY 1 ORDER BY 1`,
		"a ranking read from a subquery, beside a text column": tpccRanking,
		"sorted by a sum, a text column selected":              `SELECT i.i_name, sum(ol.ol_amount) FROM order_line ol JOIN item i ON i.i_id = ol.ol_i_id GROUP BY i.i_name ORDER BY sum(ol.ol_amount) DESC, count(*)`,
		"a char(n) key beside another key":                     `SELECT c_credit, c_w_id, count(*) FROM customer GROUP BY c_credit, c_w_id`,
		"a char(n) key that is not selected":                   `SELECT count(*) FROM customer GROUP BY customer.c_credit HAVING count(*) > 1`,
	}
	for what, q := range stays {
		if why := CopyScreen(q, types); why != "" {
			t.Errorf("%s: sent to the source (%s), want the copy", what, why)
		}
		if toks, lexWhy := lexSQL(RewriteForCopy(q)); lexWhy != "" {
			t.Errorf("%s: not read: %s", what, lexWhy)
		} else if why := screenUntyped(toks); why != "" {
			t.Errorf("%s: vetoed without its types (%s), so it never reaches the plan", what, why)
		}
	}
	toSource := map[string][2]string{
		// The statement of the issue sorts its char(n) key, and text is
		// sorted by the source's collation.
		"a char(n) key, sorted":                                    {tpccGroupByChar + " ORDER BY c.c_credit", "char(n) column"},
		"a char(n) key compared in HAVING":                         {`SELECT c_credit, count(*) FROM customer GROUP BY c_credit HAVING c_credit = 'GC'`, "char(n) column"},
		"a char(n) inside a key":                                   {`SELECT count(*) FROM customer GROUP BY c_credit || 'x'`, "char(n) column"},
		"a char(n) in a function key":                              {`SELECT count(*) FROM customer GROUP BY coalesce(c_credit, 'x')`, "char(n) column"},
		"a char(n) key in a subquery":                              {`SELECT n FROM (SELECT c_credit, count(*) AS n FROM customer GROUP BY c_credit) s`, "char(n) column"},
		"a char(n) key beside a UNION":                             {`SELECT c_credit FROM customer GROUP BY c_credit UNION ALL SELECT c_credit FROM customer`, "char(n) column"},
		"a char(n) partition":                                      {`SELECT c_id, row_number() OVER (PARTITION BY c_credit) FROM customer`, "char(n) column"},
		"an average of a numeric, compared":                        {tpccAverage, "_bintrail_pg_avg"},
		"date_trunc of an hour":                                    {`SELECT date_trunc('hour', o_entry_d) FROM orders`, "date_trunc"},
		"date_trunc of an expression":                              {`SELECT date_trunc('day', o_entry_d + INTERVAL '1 day') FROM orders`, "date_trunc"},
		"date_trunc of a text column":                              {`SELECT date_trunc('day', i_name) FROM item`, "date_trunc"},
		"date_trunc of a number column":                            {`SELECT date_trunc('day', o.o_id) FROM orders o`, "date_trunc"},
		"date_trunc of a call":                                     {`SELECT date_trunc('day', max(o_entry_d)) FROM orders`, "date_trunc"},
		"date_trunc with a third argument":                         {`SELECT date_trunc('day', o_entry_d, 'UTC') FROM orders`, "date_trunc"},
		"date_trunc of a name after two dots that is not a column": {`SELECT date_trunc('day', public.orders.nothing) FROM orders`, "date_trunc"},
		"text as a sort key":                                       {`SELECT i_name FROM item ORDER BY i_name`, "ORDER BY"},
		"a function of text as a sort key":                         {`SELECT i_id FROM item ORDER BY coalesce(i_name, 'x')`, "ORDER BY"},
		"min of text as a sort key":                                {`SELECT i_id FROM item GROUP BY i_id ORDER BY min(i_name)`, "ORDER BY"},
		"a series by day beside a text column, by position":        {`SELECT date_trunc('day', o_entry_d), i_name, count(*) FROM orders, item GROUP BY 1, 2 ORDER BY 1`, "ORDER BY"},
		"a field name that is a text value elsewhere":              {`SELECT 'day', i_id FROM item ORDER BY 1`, "ORDER BY"},
		"a sum beside text in one key":                             {`SELECT i_name FROM item GROUP BY i_name ORDER BY sum(i_id) || i_name`, "ORDER BY"},
		"a parenthesis that is not a call":                         {`SELECT i_id, i_name FROM item ORDER BY (i_id)`, "ORDER BY"},
		"a call with something after it":                           {`SELECT i_name, i_id FROM item GROUP BY i_name, i_id ORDER BY sum(i_id) + (i_id)`, "ORDER BY"},
	}
	for what, c := range toSource {
		if why := CopyScreen(c[0], types); !strings.Contains(why, c[1]) {
			t.Errorf("%s: %q, want it sent to the source for %q", what, why, c[1])
		}
	}
	// A bpchar without a length keeps each value's own blanks: its groups
	// differ, and it stays the source's as a key.
	free := screenTypes([]CatalogTable{{Schema: "public", Name: "codes", Columns: []CatalogColumn{
		{Name: "id", Type: "INTEGER", SourceType: 23, SourceTypeMod: -1}, {Name: "u", Type: "VARCHAR", SourceType: oidBpchar, SourceTypeMod: -1}}}}, nil)
	if why := CopyScreen(`SELECT u, count(*) FROM codes GROUP BY u`, free); why == "" {
		t.Error("a bpchar without a length as the key of GROUP BY: left to the copy")
	}
}
