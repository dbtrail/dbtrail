package pgshim

import (
	"strings"
	"testing"
)

// A star that stands for a char(n) column vetoes only if the statement can
// read that column; anything the analysis cannot follow keeps the veto.

// starCatalog is paddedCatalog with every column of codes of a built-in type:
// its enum column alone would keep every star over codes on the source.
func starCatalog() []CatalogTable {
	tables := paddedCatalog()
	var cols []CatalogColumn
	for _, c := range tables[0].Columns {
		if c.Name != "kind" {
			cols = append(cols, c)
		}
	}
	tables[0].Columns = cols
	return tables
}

func starScreen(stmt string) string { return CopyScreenFor(stmt, starCatalog()) }

func TestPaddedStar_leftToTheCopy(t *testing.T) {
	for _, q := range []string{
		// A star over a char(n) table, read only by other columns.
		`WITH x AS (SELECT * FROM codes) SELECT id FROM x WHERE id = 1`,
		`WITH x AS (SELECT * FROM codes) SELECT x.id, x.k FROM x`,
		`WITH x AS (SELECT c.* FROM codes c WHERE c.k > 1) SELECT id FROM x`,
		`WITH x AS (SELECT * FROM public.codes) SELECT id FROM x`,
		`SELECT count(*) FROM (SELECT * FROM codes) s`,
		`SELECT id FROM (SELECT c.* FROM codes c) s WHERE id = 1`,
		`SELECT s.id FROM (SELECT * FROM codes) AS s WHERE s.k > 1 ORDER BY s.id`,
		// Through more than one level.
		`WITH x AS (SELECT * FROM codes), y AS (SELECT * FROM x) SELECT id, k FROM y`,
		`SELECT s.id FROM (SELECT * FROM (SELECT * FROM codes) a) s`,
		`WITH x AS (SELECT * FROM codes) SELECT s.id FROM (SELECT y.* FROM x y) s`,
		// A star over columns none of which is a char(n).
		`WITH x AS (SELECT id, k FROM codes) SELECT * FROM x`,
		`WITH x AS (SELECT id, k FROM codes), y AS (SELECT x.*, row_number() OVER (ORDER BY x.k) AS r FROM x) SELECT y.id, y.r FROM y`,
		`SELECT * FROM (SELECT id, k FROM codes) s ORDER BY 1`,
		`SELECT DISTINCT s.* FROM (SELECT id, k FROM codes) s`,
		`SELECT * FROM plain WHERE id IN (SELECT id FROM codes)`,
	} {
		if veto := starScreen(q); veto != "" {
			t.Errorf("%s: sent to the source (%s), want the copy", q, veto)
		}
	}
}

func TestPaddedStar_toTheSource(t *testing.T) {
	for _, q := range []string{
		// The column read after the star.
		`WITH x AS (SELECT * FROM codes) SELECT id FROM x WHERE code = 'ab'`,
		`WITH x AS (SELECT * FROM codes) SELECT id FROM x WHERE x.code = 'ab'`,
		`WITH x AS (SELECT * FROM codes) SELECT code FROM x ORDER BY code`,
		`WITH x AS (SELECT * FROM codes) SELECT code, count(*) FROM x GROUP BY code`,
		`WITH x AS (SELECT * FROM codes) SELECT length(code) FROM x`,
		`WITH x AS (SELECT * FROM codes) SELECT code || 'x' FROM x`,
		`WITH x AS (SELECT * FROM codes) SELECT code::text FROM x`,
		`WITH x AS (SELECT * FROM codes) SELECT id, row_number() OVER (PARTITION BY code) FROM x`,
		`WITH x AS (SELECT * FROM codes) SELECT a.id FROM x a JOIN plain p ON a.code = p.name`,
		`SELECT s.id FROM (SELECT * FROM codes) s WHERE s.code = 'ab'`,
		`WITH x AS (SELECT * FROM codes), y AS (SELECT * FROM x) SELECT id FROM y WHERE code = 'ab'`,
		// A star that reaches the client, or a set of rows, through another.
		`WITH x AS (SELECT * FROM codes) SELECT * FROM x`,
		`SELECT * FROM (SELECT * FROM codes) s`,
		`SELECT s.* FROM (SELECT * FROM codes) s`,
		`WITH x AS (SELECT * FROM codes) SELECT x.* FROM x`,
		`WITH x AS (SELECT id FROM plain) SELECT * FROM x JOIN codes c ON c.id = x.id`,
		`WITH x AS (SELECT * FROM codes) SELECT DISTINCT * FROM x`,
		// The whole row as a value.
		`WITH x AS (SELECT * FROM codes) SELECT x FROM x`,
		`WITH x AS (SELECT * FROM codes) SELECT to_json(x) FROM x`,
		`WITH x AS (SELECT * FROM codes) SELECT count(DISTINCT x) FROM x`,
		`WITH x AS (SELECT * FROM codes) SELECT row(x.*) FROM x`,
		`WITH x AS (SELECT * FROM codes) SELECT count(x.*) FROM x`,
		`WITH x AS (SELECT * FROM codes) SELECT r.id FROM x r WHERE r IS NOT NULL`,
		`SELECT s.id FROM (SELECT * FROM codes) s WHERE s IS NOT NULL`,
		`WITH x AS (SELECT * FROM codes) SELECT 1 FROM plain WHERE EXISTS (SELECT 1 FROM x y WHERE y IS NULL)`,
		// The block with the star compares or orders its rows.
		`WITH x AS (SELECT DISTINCT * FROM codes) SELECT id FROM x`,
		`WITH x AS (SELECT DISTINCT ON (id) * FROM codes) SELECT id FROM x`,
		`WITH x AS (SELECT * FROM codes GROUP BY id) SELECT id FROM x`,
		`WITH x AS (SELECT * FROM codes ORDER BY 2 LIMIT 1) SELECT id FROM x`,
		`WITH x AS (SELECT * FROM codes ORDER BY code LIMIT 1) SELECT id FROM x`,
		`WITH x AS (SELECT *, row_number() OVER () AS r FROM codes) SELECT id FROM x`,
		`WITH x AS (SELECT * FROM codes UNION SELECT * FROM codes) SELECT id FROM x`,
		`WITH x AS (SELECT * FROM codes INTERSECT SELECT * FROM codes) SELECT id FROM x`,
		`WITH x AS (SELECT * FROM codes EXCEPT SELECT * FROM codes) SELECT id FROM x`,
		`WITH x AS (SELECT * FROM codes) SELECT id FROM x UNION SELECT id FROM plain`,
		`SELECT id FROM (SELECT * FROM codes) s WHERE id = 1 UNION ALL SELECT id FROM plain`,
		`SELECT id FROM (SELECT * FROM codes) s WHERE id = 1 INTERSECT SELECT id FROM plain`,
		`SELECT id FROM (SELECT * FROM codes) s WHERE id = 1 EXCEPT SELECT id FROM plain`,
		`WITH x AS (SELECT * FROM codes WHERE id IN (SELECT row_number() OVER () FROM plain)) SELECT id FROM x`,
		`SELECT id FROM (SELECT * FROM codes) s WHERE id IN (WITH z AS (SELECT 1 AS a) SELECT a FROM z)`,
		// A star a subquery hands to an expression.
		`SELECT id FROM plain WHERE name IN (SELECT * FROM codes)`,
		`SELECT id FROM plain WHERE EXISTS (SELECT * FROM codes)`,
		`SELECT (SELECT * FROM codes LIMIT 1) FROM plain`,
		`SELECT ARRAY(SELECT * FROM codes) FROM plain`,
		// IS DISTINCT FROM is not the FROM of its SELECT.
		`SELECT 'ab' IS DISTINCT FROM (SELECT * FROM codes LIMIT 1) AS differs`,
		`SELECT 'ab' IS NOT DISTINCT FROM (SELECT * FROM codes LIMIT 1) AS same`,
		`SELECT $1 IS DISTINCT FROM (SELECT * FROM codes LIMIT 1) AS d`,
		`SELECT * FROM (SELECT 'ab' IS DISTINCT FROM (SELECT * FROM codes LIMIT 1) AS d) s`,
		`WITH x AS (SELECT id FROM plain) SELECT 'ab' IS DISTINCT FROM (SELECT * FROM codes LIMIT 1) AS d`,
		// A literal cast to a padded type compares without its blanks.
		`SELECT 'a '::char(3) = 'a' FROM (SELECT * FROM codes) s`,
		`SELECT 'a '::character(3) = 'a' FROM (SELECT * FROM codes) s`,
		`SELECT 'a '::bpchar = 'a' FROM (SELECT * FROM codes) s`,
		`SELECT 'a '::"bpchar" = 'a' FROM (SELECT * FROM codes) s`,
		`SELECT 'a '::pg_catalog.bpchar = 'a' FROM (SELECT * FROM codes) s`,
		`SELECT 'a '::nchar(3) = 'a' FROM (SELECT * FROM codes) s`,
		`SELECT 'a '::nchar = 'a' FROM (SELECT * FROM codes) s`,
		`SELECT CAST('a ' AS char(3)) = 'a' FROM (SELECT * FROM codes) s`,
		`SELECT char 'a ' = 'a' FROM (SELECT * FROM codes) s`,
		`SELECT 'a '::national character(3) = 'a' FROM (SELECT * FROM codes) s`,
		`SELECT * FROM (SELECT id, k FROM codes) s WHERE 'a '::char(3) = 'a'`,
		// What the analysis does not follow.
		`WITH RECURSIVE x AS (SELECT * FROM codes) SELECT id FROM x`,
		`WITH x(a, b) AS (SELECT * FROM codes) SELECT a FROM x`,
		`WITH x AS (WITH y AS (SELECT * FROM codes) SELECT * FROM y) SELECT id FROM x`,
		`SELECT s.q FROM (SELECT * FROM codes) AS s(p, q)`,
		`SELECT id FROM plain p, LATERAL (SELECT * FROM codes c WHERE c.id = p.id) s`,
		`SELECT id FROM generate_series(1, 2) g JOIN (SELECT * FROM codes) s ON true`,
		`SELECT a.id FROM (SELECT * FROM codes) a JOIN plain p USING (id)`,
		`SELECT a.id FROM (SELECT * FROM codes) a NATURAL JOIN plain p`,
		`SELECT id FROM (SELECT * FROM codes a JOIN plain b ON a.id = b.id) s`,
		`SELECT id FROM (SELECT a.*, b.* FROM codes a JOIN plain b ON a.id = b.id) s`,
		`SELECT id FROM (SELECT *, 1 AS more FROM codes) s`,
		`SELECT id FROM (SELECT c.*, c.id AS again FROM codes c) s`,
		`WITH x AS (SELECT * FROM nosuch, codes) SELECT id FROM x`,
		`WITH x AS (SELECT * FROM other.codes) SELECT id FROM x`,
		`WITH "X" AS (SELECT * FROM codes) SELECT id FROM "X"`,
		`SELECT id FROM (SELECT * FROM "codes") s`,
		`SELECT id FROM (SELECT * FROM ONLY codes) s`,
		`SELECT id FROM (SELECT * FROM codes TABLESAMPLE SYSTEM (10)) s`,
		`SELECT id FROM (SELECT * FROM (codes)) s`,
		`SELECT id FROM (SELECT * FROM codes) s, (SELECT * FROM codes) s`,
		// A char(n) in the last SELECT, even returned whole: its bytes match,
		// but the port announces text where PostgreSQL announces bpchar(n).
		`WITH s AS (SELECT id, sum(qty) AS q FROM plain GROUP BY id), r AS (SELECT s.*, rank() OVER (PARTITION BY s.id ORDER BY s.q DESC) AS rn FROM s) SELECT r.id, c.code, r.q FROM r JOIN codes c ON c.id = r.id WHERE r.rn <= 3 ORDER BY r.id LIMIT 5`,
		`WITH x AS (SELECT id FROM plain) SELECT c.code AS z, x.id FROM x JOIN codes c ON c.id = x.id ORDER BY x.id`,
		`WITH x AS (SELECT * FROM codes) SELECT x.code, x.id FROM x`,
		`WITH x AS (SELECT id FROM plain) SELECT c.code, x.id FROM x JOIN codes c ON c.id = x.id ORDER BY 1`,
		`WITH x AS (SELECT id FROM plain) SELECT DISTINCT c.code FROM x JOIN codes c ON c.id = x.id`,
		`WITH x AS (SELECT id FROM plain) SELECT c.code, count(*) FROM x JOIN codes c ON c.id = x.id GROUP BY 1`,
		`WITH x AS (SELECT id FROM plain) SELECT c.code AS z FROM x JOIN codes c ON c.id = x.id ORDER BY z`,
		`WITH x AS (SELECT id FROM plain) SELECT c.code || 'x' FROM x JOIN codes c ON c.id = x.id`,
		`WITH x AS (SELECT id, code FROM codes) SELECT id FROM x WHERE id = 1`,
		// Pinned by #3068, without a WITH.
		`SELECT code FROM codes WHERE id IN (SELECT id FROM plain)`,
	} {
		if veto := starScreen(q); !isPaddedVeto(veto) {
			t.Errorf("%s: %q, want it sent to the source for its padded column", q, veto)
		}
	}
}

// A keyword after a dot is a column: read as a clause, it would end the FROM
// early and hide the table joined after it.
func TestPaddedStar_aDottedKeyword(t *testing.T) {
	for _, w := range []string{"where", "group", "having", "order", "limit", "offset", "window", "fetch", "for", "from", "distinct", "on", "join", "left", "cross"} {
		q := `WITH x AS (SELECT * FROM plain u JOIN plain v ON u.` + w + ` = v.id JOIN codes t ON t.k = u.id) SELECT count(*) FROM x`
		if veto := starScreen(q); !isPaddedVeto(veto) {
			t.Errorf("%s: %q, want it sent to the source", q, veto)
		}
	}
}

// A keyword after AS is a label: read as FROM, it would make the real table
// the alias of a table named "from".
func TestPaddedStar_aKeywordAsLabel(t *testing.T) {
	// Such a statement is not read whole, so every table counts as read:
	// without the table of unrecorded types, only the label's rule refuses it.
	var tables []CatalogTable
	for _, tb := range starCatalog() {
		if tb.Name != "legacy" {
			tables = append(tables, tb)
		}
	}
	tables = append(tables, CatalogTable{Schema: "public", Name: "from", Columns: []CatalogColumn{
		{Name: "id", Type: "INTEGER", SourceType: oidInt4, SourceTypeMod: -1},
	}})
	for _, q := range []string{
		`WITH x AS (SELECT DISTINCT *, 1 AS from FROM codes) SELECT count(*) FROM x`,
		`SELECT * FROM (SELECT *, 1 AS from FROM codes) s`,
	} {
		if veto := CopyScreenFor(q, tables); !isPaddedVeto(veto) {
			t.Errorf("%s: %q, want it sent to the source", q, veto)
		}
	}
}

// An argument's declared type is not in the text.
func TestPaddedStar_anArgument(t *testing.T) {
	for _, q := range []string{
		`SELECT s.id FROM (SELECT * FROM codes) s WHERE s.label = $1`,
		`WITH x AS (SELECT * FROM codes) SELECT id FROM x WHERE id = $1`,
	} {
		if veto := starScreen(q); !isPaddedVeto(veto) {
			t.Errorf("%s: %q, want it sent to the source", q, veto)
		}
	}
}

// A table with a column of a type not known to be free of padding gets no
// exemption: a domain over char(n) is recorded under its own type and carries
// no mark, so the veto of the star is what keeps it from the copy.
func TestPaddedStar_aTypeNotProven(t *testing.T) {
	const domain = 16500
	base := func(extra CatalogColumn) []CatalogTable {
		return append(starCatalog(), CatalogTable{Schema: "public", Name: "dom", Columns: []CatalogColumn{
			{Name: "id", Type: "INTEGER", SourceType: oidInt4, SourceTypeMod: -1},
			{Name: "other", Type: "VARCHAR", SourceType: oidBpchar, SourceTypeMod: 6},
			extra,
		}})
	}
	for name, extra := range map[string]CatalogColumn{
		"a domain over char(n)": {Name: "dc", Type: "VARCHAR", SourceType: domain, SourceTypeMod: -1},
		"an enum":               {Name: "dc", Type: "VARCHAR", SourceType: domain + 1, SourceTypeMod: -1},
		"a domain over int":     {Name: "dc", Type: "INTEGER", SourceType: domain + 2, SourceTypeMod: -1},
		"no recorded type":      {Name: "dc", Type: "INTEGER"},
	} {
		tables := base(extra)
		for _, q := range []string{
			`SELECT count(*) FROM (SELECT * FROM dom) s WHERE dc = 'a'`,
			`SELECT sum(length(dc)) FROM (SELECT * FROM dom) s`,
			`SELECT count(*) FROM (SELECT * FROM dom) s`,
			`WITH x AS (SELECT * FROM dom) SELECT id FROM x`,
			`SELECT * FROM (SELECT id, dc FROM dom) s WHERE dc = 'a'`,
			`SELECT * FROM plain WHERE code IN (SELECT dc FROM dom)`,
		} {
			if veto := CopyScreenFor(q, tables); !isPaddedVeto(veto) {
				t.Errorf("%s: %s: %q, want it sent to the source", name, q, veto)
			}
		}
		// A table the statement does not read says nothing of it.
		if q := `WITH x AS (SELECT * FROM codes) SELECT id FROM x`; CopyScreenFor(q, tables) != "" {
			t.Errorf("%s: %s: sent to the source for a table it does not read", name, q)
		}
	}
	// The plain paths do not change: an enum stays on the copy.
	if q := `SELECT id FROM codes WHERE kind = 'new'`; paddedScreen(q) != "" {
		t.Errorf("%s: sent to the source", q)
	}
}

// The three reports of the TPROC-C report set, on HammerDB's schema, whose
// customer and order_line tables hold char(n) columns (HammerDB 6.0's DDL).
var tprocReports = map[string]string{
	"Q07": `WITH spend AS (
  SELECT o.o_w_id                                              AS w_id,
         o.o_d_id                                              AS d_id,
         o.o_c_id                                              AS c_id,
         SUM(CASE WHEN ol.ol_number = 1 THEN 1 ELSE 0 END)     AS orders,
         SUM(ol.ol_amount)                                     AS amount
  FROM   orders o
  JOIN   order_line ol ON ol.ol_w_id = o.o_w_id AND ol.ol_d_id = o.o_d_id AND ol.ol_o_id = o.o_id
  WHERE  o.o_w_id BETWEEN 1 AND 40
    AND  o.o_entry_d >= TIMESTAMP '2026-11-01 00:00:00' AND o.o_entry_d < TIMESTAMP '2026-11-20 00:00:00'
  GROUP  BY o.o_w_id, o.o_d_id, o.o_c_id
), ranked AS (
  SELECT s.*,
         ROW_NUMBER() OVER (PARTITION BY s.w_id, s.d_id ORDER BY s.amount DESC, s.c_id) AS rnk,
         SUM(s.amount) OVER (PARTITION BY s.w_id, s.d_id)                                AS district_amount
  FROM   spend s
)
SELECT r.w_id, r.d_id, r.rnk,
       r.c_id,
       c.c_first, c.c_last, c.c_credit,
       r.orders,
       r.amount,
       ROUND(100.0 * r.amount / NULLIF(r.district_amount, 0), 2)   AS pct_of_district
FROM   ranked r
JOIN   customer c ON c.c_w_id = r.w_id AND c.c_d_id = r.d_id AND c.c_id = r.c_id
WHERE  r.rnk <= 3
ORDER  BY r.w_id, r.d_id, r.rnk
LIMIT  500`,
	"Q08": `WITH early AS (
  SELECT o_w_id AS w_id, o_d_id AS d_id, o_c_id AS c_id, COUNT(*) AS orders
  FROM   orders
  WHERE  o_w_id BETWEEN 1 AND 40
    AND  o_entry_d >= TIMESTAMP '2026-11-01 00:00:00' AND o_entry_d < TIMESTAMP '2026-11-10 00:00:00'
  GROUP  BY o_w_id, o_d_id, o_c_id
), flagged AS (
  SELECT e.*,
         CASE WHEN EXISTS (SELECT 1 FROM orders o2
                           WHERE  o2.o_w_id = e.w_id AND o2.o_d_id = e.d_id AND o2.o_c_id = e.c_id
                             AND  o2.o_entry_d >= TIMESTAMP '2026-11-10 00:00:00' AND o2.o_entry_d < TIMESTAMP '2026-11-20 00:00:00')
              THEN 0 ELSE 1 END AS churned
  FROM   early e
)
SELECT f.w_id,
       COUNT(*)                                                        AS early_customers,
       SUM(f.churned)                                                  AS churned,
       ROUND(100.0 * SUM(f.churned) / COUNT(*), 2)                     AS churn_pct,
       ROUND(AVG(CASE WHEN f.churned = 1 THEN f.orders END), 2)        AS early_orders_churned,
       ROUND(AVG(CASE WHEN f.churned = 0 THEN f.orders END), 2)        AS early_orders_retained,
       ROUND(AVG(CASE WHEN f.churned = 1 THEN c.c_balance END), 2)     AS avg_balance_churned,
       ROUND(AVG(CASE WHEN f.churned = 0 THEN c.c_balance END), 2)     AS avg_balance_retained
FROM   flagged f
JOIN   customer c ON c.c_w_id = f.w_id AND c.c_d_id = f.d_id AND c.c_id = f.c_id
GROUP  BY f.w_id
ORDER  BY churn_pct DESC, f.w_id
LIMIT  3`,
	"Q09": `WITH order_totals AS (
  SELECT o.o_w_id, o.o_d_id, o.o_id, o.o_ol_cnt, o.o_all_local,
         SUM(ol.ol_amount) AS total
  FROM   orders o
  JOIN   order_line ol ON ol.ol_w_id = o.o_w_id AND ol.ol_d_id = o.o_d_id AND ol.ol_o_id = o.o_id
  WHERE  o.o_w_id BETWEEN 1 AND 40
    AND  o.o_entry_d >= TIMESTAMP '2026-11-01 00:00:00' AND o.o_entry_d < TIMESTAMP '2026-11-20 00:00:00'
  GROUP  BY o.o_w_id, o.o_d_id, o.o_id, o.o_ol_cnt, o.o_all_local
), deciles AS (
  SELECT t.*, NTILE(10) OVER (ORDER BY t.total) AS decile
  FROM   order_totals t
)
SELECT decile,
       COUNT(*)                                                         AS orders,
       MIN(total)                                                       AS min_value,
       MAX(total)                                                       AS max_value,
       ROUND(AVG(total), 2)                                             AS avg_value,
       ROUND(AVG(o_ol_cnt), 2)                                          AS avg_lines,
       ROUND(100.0 * SUM(total) / NULLIF(SUM(SUM(total)) OVER (), 0), 2) AS pct_of_revenue,
       ROUND(100.0 * SUM(CASE WHEN o_all_local = 0 THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_with_remote
FROM   deciles
GROUP  BY decile
ORDER  BY decile`,
}

func tprocCatalog() []CatalogTable {
	col := func(name, typ string, oid uint32, mod int32) CatalogColumn {
		return CatalogColumn{Name: name, Type: typ, SourceType: oid, SourceTypeMod: mod}
	}
	whole := func(oid uint32, typ string, names ...string) (out []CatalogColumn) {
		for _, n := range names {
			out = append(out, col(n, typ, oid, -1))
		}
		return out
	}
	const zoned = "TIMESTAMP WITH TIME ZONE"
	customer := append(whole(oidInt4, "INTEGER", "c_id", "c_w_id"), whole(oidInt2, "SMALLINT", "c_d_id", "c_payment_cnt", "c_delivery_cnt")...)
	customer = append(customer, col("c_since", zoned, oidTimestamptz, -1),
		col("c_first", "VARCHAR", oidVarchar, 20), col("c_middle", "VARCHAR", oidBpchar, 6),
		col("c_last", "VARCHAR", oidVarchar, 20), col("c_street_1", "VARCHAR", oidVarchar, 24),
		col("c_street_2", "VARCHAR", oidVarchar, 24), col("c_city", "VARCHAR", oidVarchar, 24),
		col("c_state", "VARCHAR", oidBpchar, 6), col("c_zip", "VARCHAR", oidBpchar, 13),
		col("c_phone", "VARCHAR", oidBpchar, 20), col("c_credit", "VARCHAR", oidBpchar, 6),
		col("c_balance", "DECIMAL(12,2)", oidNumeric, -1))
	orders := append(whole(oidInt4, "INTEGER", "o_id", "o_w_id", "o_c_id"), whole(oidInt2, "SMALLINT", "o_d_id", "o_carrier_id", "o_ol_cnt", "o_all_local")...)
	orders = append(orders, col("o_entry_d", zoned, oidTimestamptz, -1))
	lines := append(whole(oidInt4, "INTEGER", "ol_o_id", "ol_w_id", "ol_i_id", "ol_supply_w_id"), whole(oidInt2, "SMALLINT", "ol_d_id", "ol_number", "ol_quantity")...)
	lines = append(lines, col("ol_delivery_d", zoned, oidTimestamptz, -1),
		col("ol_amount", "DECIMAL(6,2)", oidNumeric, -1), col("ol_dist_info", "VARCHAR", oidBpchar, 28))
	return []CatalogTable{
		{Schema: "public", Name: "customer", Columns: customer},
		{Schema: "public", Name: "orders", Columns: orders},
		{Schema: "public", Name: "order_line", Columns: lines},
	}
}

// Q08 and Q09 use their stars over columns that are no char(n). Q07 also
// returns c_credit, a char(2), so it stays on the source.
func TestPaddedStar_theThreeReports(t *testing.T) {
	for name, q := range tprocReports {
		toks, why := lexSQL(RewriteForCopy(q))
		if why != "" {
			t.Fatalf("%s: %s", name, why)
		}
		tables := tprocCatalog()
		types := screenTypes(tables, tablesRead(RewriteForCopy(q), tables))
		veto := paddedUse(toks, types)
		if name == "Q07" {
			if !strings.Contains(veto, `"c_credit"`) {
				t.Errorf("Q07: %q, want the veto of its char(2) column c_credit", veto)
			}
			continue
		}
		if veto != "" {
			t.Errorf("%s: sent to the source: %s", name, veto)
		}
	}
}
