package pgshim

import (
	"strings"
	"testing"
	"time"
)

// A table with a char(n) column as a later item of a FROM list written with
// commas, and under the star of an EXISTS (#3219). What the copy answers the
// same was measured by internal/pgcompare (charCases).

func TestPaddedLists_leftToTheCopy(t *testing.T) {
	for _, q := range []string{
		// The second table of a list, its char(n) column not used.
		`SELECT p.id FROM plain p, codes c WHERE c.id = p.id`,
		`SELECT plain.qty, codes.k FROM plain, codes WHERE codes.id = plain.id`,
		`SELECT p.id FROM plain AS p, codes AS c WHERE c.id = p.id AND c.k > 1`,
		`SELECT p.id FROM plain p, public.codes c WHERE c.id = p.id`,
		`SELECT p.qty, count(*) FROM plain p, codes c, plain q WHERE c.id = p.id AND q.id = p.id GROUP BY p.qty`,
		`SELECT p.id FROM codes c, plain p WHERE c.id = p.id`,
		`SELECT s.id FROM (SELECT id, qty FROM plain) s, codes c WHERE c.id = s.id`,
		`SELECT p.id FROM plain p, "codes" c WHERE c.id = p.id`,
		// The column returned whole, as from a join written with JOIN.
		`SELECT c.id, c.code FROM plain p, codes c WHERE c.id = p.id`,
		// In a subquery's list too.
		`SELECT id FROM plain WHERE id IN (SELECT c.id FROM plain q, codes c WHERE c.id = q.id)`,

		// EXISTS and NOT EXISTS with a star: the select list is not read.
		// (The first one was held to the source until #3219, as every star
		// outside a plain select list was.)
		`SELECT id FROM plain WHERE EXISTS (SELECT * FROM codes)`,
		`SELECT p.id FROM plain p WHERE EXISTS (SELECT * FROM codes c WHERE c.id = p.id)`,
		`SELECT p.id FROM plain p WHERE NOT EXISTS (SELECT * FROM codes c WHERE c.id = p.id AND c.k > 2)`,
		`SELECT p.qty, count(*) FROM plain p WHERE p.qty > 1 AND exists (select * from codes where codes.id = p.id) GROUP BY p.qty`,
		`SELECT p.id FROM plain p WHERE EXISTS (SELECT * FROM codes c, plain q WHERE c.id = p.id AND q.id = c.id)`,
		`SELECT p.id FROM plain p WHERE EXISTS (SELECT * FROM codes) AND NOT EXISTS (SELECT * FROM codes c WHERE c.k = p.qty)`,
	} {
		if veto := paddedScreen(q); veto != "" {
			t.Errorf("%s: sent to the source (%s), want the copy", q, veto)
		}
	}
}

func TestPaddedLists_toTheSource(t *testing.T) {
	for _, q := range []string{
		// The join compares the char(n) column: its padding counts, with
		// commas as with JOIN ... ON.
		`SELECT p.id FROM plain p, codes c WHERE c.code = p.code`,
		`SELECT p.id FROM plain p, codes c WHERE p.code = c.code AND c.id = p.id`,
		`SELECT p.id FROM plain p, codes c WHERE c.id = p.id AND c.code = 'ab'`,
		`SELECT p.id FROM plain p, codes c WHERE c.id = p.id ORDER BY c.code`,
		`SELECT c.code, count(*) FROM plain p, codes c WHERE c.id = p.id GROUP BY c.code HAVING c.code > 'a'`,
		// NATURAL and USING compare columns without naming them, or name it.
		`SELECT p.id FROM plain p NATURAL JOIN codes`,
		`SELECT p.id FROM plain p, codes c NATURAL JOIN plain q`,
		`SELECT p.id FROM plain p JOIN codes c USING (code)`,
		// The table's name as a value, after a comma: the whole row.
		`SELECT k, codes FROM codes`,
		`SELECT id, c FROM codes c`,
		`SELECT count(DISTINCT c) FROM plain p, codes c WHERE c.id = p.id`,
		`SELECT p.id FROM plain p, codes c WHERE c = c`,
		`SELECT p.id FROM plain p, codes c WHERE (p.id, c) IS NOT NULL`,
		`SELECT greatest(id, codes) FROM codes`,
		`SELECT id FROM codes ORDER BY id, codes`,
		`SELECT id FROM codes GROUP BY id, codes`,
		`SELECT id IS DISTINCT FROM k, codes FROM codes`,
		// A list that mixes JOIN ... ON with commas is not followed.
		`SELECT p.id FROM plain p JOIN plain q ON q.id = p.id, codes c WHERE c.id = p.id`,
		// A star that is read: anything but EXISTS.
		`SELECT p.id FROM plain p WHERE p.id IN (SELECT * FROM codes)`,
		`SELECT p.id FROM plain p WHERE p.id = ANY (SELECT * FROM codes)`,
		`SELECT p.id FROM plain p WHERE (SELECT * FROM codes LIMIT 1) IS NOT NULL`,
		`SELECT p.id FROM plain p WHERE (p.id, p.code) IS DISTINCT FROM (SELECT * FROM codes LIMIT 1)`,
		`SELECT p.id, (SELECT * FROM codes LIMIT 1) FROM plain p`,
		`SELECT p.id FROM plain p WHERE p.id > ALL (SELECT * FROM codes)`,
		// EXISTS whose subquery compares the star's rows, or the column.
		`SELECT p.id FROM plain p WHERE EXISTS (SELECT * FROM codes EXCEPT SELECT 1, 'ab', 'x', NULL, 'y', 'z', 'k', 1)`,
		`SELECT p.id FROM plain p WHERE EXISTS (SELECT * FROM codes UNION SELECT * FROM codes)`,
		`SELECT p.id FROM plain p WHERE EXISTS (SELECT * FROM codes INTERSECT SELECT * FROM codes)`,
		`SELECT p.id FROM plain p WHERE EXISTS (SELECT * FROM codes c WHERE c.code = p.code)`,
		`SELECT p.id FROM plain p WHERE EXISTS (SELECT * FROM codes c WHERE c.code = 'ab')`,
		`SELECT p.id FROM plain p WHERE EXISTS (SELECT * FROM codes c WHERE c = c)`,
		`SELECT p.id FROM plain p WHERE EXISTS (SELECT DISTINCT * FROM codes)`,
		`SELECT p.id FROM plain p WHERE EXISTS (SELECT *, code FROM codes)`,
		`SELECT p.id FROM plain p WHERE EXISTS (SELECT c.* FROM codes c)`,
		`SELECT p.id FROM plain p WHERE EXISTS (SELECT * FROM codes NATURAL JOIN plain)`,
		`SELECT p.id FROM plain p WHERE EXISTS ((SELECT * FROM codes))`,
		`SELECT p.id FROM plain p WHERE s.exists (SELECT * FROM codes)`,
		// EXISTS whose subquery puts rows of equal values together: for the
		// bpchar without a length (free) PostgreSQL finds 'ab' and 'ab '
		// equal and the copy does not, so the groups differ and with them
		// whether a row exists.
		`SELECT p.id FROM plain p WHERE EXISTS (SELECT * FROM codes GROUP BY 1,2,3,4,5,6,7,8 HAVING count(*) > 1)`,
		`SELECT p.id FROM plain p WHERE EXISTS (SELECT * FROM codes GROUP BY 1 OFFSET 1)`,
		`SELECT EXISTS (SELECT * FROM codes GROUP BY 5 HAVING count(*) > 1)`,
		`SELECT p.id FROM plain p WHERE EXISTS (SELECT * FROM codes c WHERE c.k IN (SELECT DISTINCT qty FROM plain))`,
		`SELECT p.id FROM plain p WHERE EXISTS (SELECT * FROM codes c WHERE c.k = (SELECT count(*) OVER () FROM plain LIMIT 1))`,
		`SELECT p.id FROM plain p WHERE NOT EXISTS (SELECT * FROM codes WINDOW w AS (ORDER BY id))`,
		// A table's alias that the port reads as a keyword: its use as the
		// whole row could not be told.
		`SELECT p.id FROM plain p, codes last WHERE last = last`,
		`SELECT p.id FROM plain p JOIN codes AS first ON first.id = p.id`,
		`SELECT id FROM codes filter WHERE filter IS NULL`,
		`SELECT id FROM codes AS where`,
		// A bpchar without a length compared, as ever.
		`SELECT p.id FROM plain p, codes c WHERE c.free = p.code`,
	} {
		if veto := paddedScreen(q); !isPaddedVeto(veto) {
			t.Errorf("%s: %q, want it sent to the source for its padded column", q, veto)
		}
	}
}

func TestFromListItem(t *testing.T) {
	at := func(q, name string, nth int) (toks []rtToken, i int) {
		toks, _ = lexSQL(q)
		for j, tk := range toks {
			if tk.kind == rtWord && tk.text == name {
				if nth == 0 {
					return toks, j
				}
				nth--
			}
		}
		t.Fatalf("%s: no %q", q, name)
		return nil, 0
	}
	for _, tc := range []struct {
		q    string
		nth  int
		want bool
	}{
		{`SELECT 1 FROM a, codes`, 0, true},
		{`SELECT 1 FROM a x, b AS y, codes z`, 0, true},
		{`SELECT trim(both FROM a, codes) FROM t`, 0, false},
		{`SELECT f((SELECT 1 FROM a, codes)) FROM t`, 0, true},
		{`SELECT 1 FROM (SELECT 1 FROM t WHERE u > 1) s, codes`, 0, true},
		{`SELECT 1 FROM t WHERE EXISTS (SELECT 1 FROM a, codes)`, 0, true},
		{`SELECT 1 FROM codes`, 0, false},    // after FROM, not after a comma: the other rule's
		{`SELECT a, codes FROM t`, 0, false}, // a value
		{`SELECT 1 FROM t WHERE x IN (a, codes)`, 0, false},
		{`SELECT 1 FROM t ORDER BY a, codes`, 0, false},
		{`SELECT 1 FROM t GROUP BY a, codes`, 0, false},
		{`SELECT a IS DISTINCT FROM b, codes FROM t`, 0, false},
		{`SELECT f(a, codes) FROM t`, 0, false},
		{`SELECT 1 FROM a JOIN b ON b.i = a.i, codes`, 0, false},
		{`SELECT 1 FROM a, b WHERE a.i = b.i AND (a.i, codes) IS NULL`, 0, false},
		{`SELECT extract(year FROM d), codes FROM t`, 0, false},
		{`SELECT 1 FROM a, 2, codes`, 0, false},
		{`, codes`, 0, false},
		{`codes`, 0, false},
	} {
		toks, i := at(tc.q, "codes", tc.nth)
		if got := fromListItem(toks, i); got != tc.want {
			t.Errorf("%s: fromListItem = %v, want %v", tc.q, got, tc.want)
		}
	}
}

// The two statements of the issue, over the TPC-C tables: order_line holds a
// char(24) column, ol_dist_info, which neither statement uses.
func TestPaddedLists_reportsOfTheIssue(t *testing.T) {
	col := func(name, typ string, oid uint32, mod int32) CatalogColumn {
		return CatalogColumn{Name: name, Type: typ, SourceType: oid, SourceTypeMod: mod}
	}
	const timestamptz = 1184
	catalog := []CatalogTable{
		{Schema: "public", Name: "orders", Columns: []CatalogColumn{
			col("o_id", "INTEGER", oidInt4, -1), col("o_w_id", "INTEGER", oidInt4, -1), col("o_d_id", "INTEGER", oidInt4, -1),
			col("o_carrier_id", "INTEGER", oidInt4, -1), col("o_ol_cnt", "INTEGER", oidInt4, -1),
			col("o_entry_d", "TIMESTAMP WITH TIME ZONE", timestamptz, -1)}},
		{Schema: "public", Name: "order_line", Columns: []CatalogColumn{
			col("ol_o_id", "INTEGER", oidInt4, -1), col("ol_w_id", "INTEGER", oidInt4, -1), col("ol_d_id", "INTEGER", oidInt4, -1),
			col("ol_delivery_d", "TIMESTAMP WITH TIME ZONE", timestamptz, -1), col("ol_dist_info", "VARCHAR", oidBpchar, 28)}},
	}
	for name, q := range map[string]string{
		"Q12, tables listed with commas": `select o_ol_cnt,
       sum(case when o_carrier_id = 1 or o_carrier_id = 2 then 1 else 0 end) as high_line_count,
       sum(case when o_carrier_id <> 1 and o_carrier_id <> 2 then 1 else 0 end) as low_line_count
from orders, order_line
where ol_w_id = o_w_id and ol_d_id = o_d_id and ol_o_id = o_id
  and o_entry_d <= ol_delivery_d and ol_delivery_d < '2032-01-01 00:00:00'
group by o_ol_cnt order by o_ol_cnt`,
		"Q4, EXISTS with a star": `select o_ol_cnt, count(*) as order_count
from orders
where o_entry_d >= '2007-01-02 00:00:00' and o_entry_d < '2032-01-02 00:00:00'
  and exists (select * from order_line
              where o_id = ol_o_id and o_w_id = ol_w_id and o_d_id = ol_d_id
                and ol_delivery_d >= o_entry_d)
group by o_ol_cnt order by o_ol_cnt`,
	} {
		if veto := CopyScreenFor(q, catalog); veto != "" {
			t.Errorf("%s: sent to the source (%s), want the copy", name, veto)
		}
	}
	// The same two with the char(n) column compared: the source's.
	for name, q := range map[string]string{
		"the list, the column compared": `select count(*) from orders, order_line where ol_o_id = o_id and ol_dist_info = 'x'`,
		"EXISTS, the column compared":   `select count(*) from orders where exists (select * from order_line where ol_o_id = o_id and ol_dist_info = 'x')`,
	} {
		if veto := CopyScreenFor(q, catalog); !isPaddedVeto(veto) {
			t.Errorf("%s: %q, want it sent to the source for its padded column", name, veto)
		}
	}
}

// A table followed by a clause's keyword has no alias, and is read as before.
func TestPaddedLists_noAliasBeforeAClause(t *testing.T) {
	for _, q := range []string{
		`SELECT id FROM codes WHERE id = 1`,
		`SELECT id FROM codes ORDER BY id`,
		`SELECT id FROM codes LIMIT 5`,
		`SELECT id, count(*) FROM codes GROUP BY id HAVING count(*) > 1`,
		`SELECT codes.id FROM codes LEFT JOIN plain p ON p.id = codes.id`,
		`SELECT codes.id FROM codes JOIN plain p ON p.id = codes.id`,
		`SELECT codes.id FROM codes CROSS JOIN plain p`,
		`SELECT c.id FROM codes AS c WHERE c.k > 1`,
		`SELECT c.id FROM codes c OFFSET 2`,
	} {
		if veto := paddedScreen(q); veto != "" {
			t.Errorf("%s: sent to the source (%s), want the copy", q, veto)
		}
	}
}

// WITH x AS [NOT] MATERIALIZED (...) is a WITH like any other.
func TestCopyScreen_materialized(t *testing.T) {
	for _, q := range []string{
		`WITH y AS MATERIALIZED (SELECT id, qty FROM plain) SELECT id FROM y WHERE qty > 1`,
		`WITH y AS NOT MATERIALIZED (SELECT id, qty FROM plain) SELECT id, qty FROM y`,
	} {
		if veto := CopyScreenFor(q, paddedCatalog()); veto != "" {
			t.Errorf("%s: sent to the source (%s), want the copy", q, veto)
		}
	}
	if veto := CopyScreenFor(`SELECT materialized(id) FROM plain`, paddedCatalog()); veto == "" {
		t.Error("a function named materialized: left to the copy")
	}
}

// A megabyte of names after commas, and of EXISTS nested in EXISTS: the
// screen's rules for char(n) answer at once.
func TestPaddedLists_bounded(t *testing.T) {
	types := screenTypes(paddedCatalog(), nil)
	for name, q := range map[string]string{
		"names after commas": "SELECT 1 FROM plain" + strings.Repeat(", codes", (1<<20)/7),
		"nested EXISTS":      "SELECT 1 FROM plain WHERE " + strings.Repeat("EXISTS (SELECT * FROM codes WHERE ", (1<<20)/34) + "true",
		"open EXISTS":        "SELECT 1 FROM plain WHERE " + strings.Repeat("EXISTS (SELECT * FROM codes WHERE EXISTS ( ", (1<<20)/42),
	} {
		toks, why := lexSQL(q)
		if why != "" {
			t.Fatalf("%s: not read: %s", name, why)
		}
		done := make(chan struct{})
		start := time.Now()
		go func() { paddedUse(toks, types); close(done) }()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Fatalf("a megabyte of %s: no answer in 10 s", name)
		}
		if took := time.Since(start); took > 3*time.Second {
			t.Errorf("a megabyte of %s took %v", name, took)
		}
	}
}
