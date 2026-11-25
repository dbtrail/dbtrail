package pgshim

import (
	"context"
	"testing"
)

// chCatalog is the tables CH-benCHmark's Q2 and Q15 read, as the live
// board's snapshot types them: TPROC-C's numbers integer and smallint, its
// names varchar, and CH-benCHmark's extra tables' names char(n), which the
// copy's view trims.
func chCatalog() []CatalogTable {
	return []CatalogTable{
		{Schema: "public", Name: "item", Columns: []CatalogColumn{{Name: "i_id", Type: "INTEGER", SourceType: 23, SourceTypeMod: -1}, {Name: "i_im_id", Type: "INTEGER", SourceType: 23, SourceTypeMod: -1}, {Name: "i_name", Type: "VARCHAR", SourceType: 1043, SourceTypeMod: 28}, {Name: "i_price", Type: "DECIMAL(5,2)", SourceType: 1700, SourceTypeMod: 327686}, {Name: "i_data", Type: "VARCHAR", SourceType: 1043, SourceTypeMod: 54}}},
		{Schema: "public", Name: "stock", Columns: []CatalogColumn{{Name: "s_i_id", Type: "INTEGER", SourceType: 23, SourceTypeMod: -1}, {Name: "s_w_id", Type: "INTEGER", SourceType: 23, SourceTypeMod: -1}, {Name: "s_ytd", Type: "INTEGER", SourceType: 23, SourceTypeMod: -1}, {Name: "s_quantity", Type: "SMALLINT", SourceType: 21, SourceTypeMod: -1}, {Name: "s_order_cnt", Type: "SMALLINT", SourceType: 21, SourceTypeMod: -1}, {Name: "s_remote_cnt", Type: "SMALLINT", SourceType: 21, SourceTypeMod: -1}, {Name: "s_dist_01", Type: "VARCHAR", SourceType: 1042, SourceTypeMod: 28, TrimmedOnCopy: true}, {Name: "s_dist_02", Type: "VARCHAR", SourceType: 1042, SourceTypeMod: 28, TrimmedOnCopy: true}, {Name: "s_dist_03", Type: "VARCHAR", SourceType: 1042, SourceTypeMod: 28, TrimmedOnCopy: true}, {Name: "s_dist_04", Type: "VARCHAR", SourceType: 1042, SourceTypeMod: 28, TrimmedOnCopy: true}, {Name: "s_dist_05", Type: "VARCHAR", SourceType: 1042, SourceTypeMod: 28, TrimmedOnCopy: true}, {Name: "s_dist_06", Type: "VARCHAR", SourceType: 1042, SourceTypeMod: 28, TrimmedOnCopy: true}, {Name: "s_dist_07", Type: "VARCHAR", SourceType: 1042, SourceTypeMod: 28, TrimmedOnCopy: true}, {Name: "s_dist_08", Type: "VARCHAR", SourceType: 1042, SourceTypeMod: 28, TrimmedOnCopy: true}, {Name: "s_dist_09", Type: "VARCHAR", SourceType: 1042, SourceTypeMod: 28, TrimmedOnCopy: true}, {Name: "s_dist_10", Type: "VARCHAR", SourceType: 1042, SourceTypeMod: 28, TrimmedOnCopy: true}, {Name: "s_data", Type: "VARCHAR", SourceType: 1043, SourceTypeMod: 54}}},
		{Schema: "public", Name: "order_line", Columns: []CatalogColumn{{Name: "ol_delivery_d", Type: "TIMESTAMP WITH TIME ZONE", SourceType: 1184, SourceTypeMod: -1}, {Name: "ol_o_id", Type: "INTEGER", SourceType: 23, SourceTypeMod: -1}, {Name: "ol_w_id", Type: "INTEGER", SourceType: 23, SourceTypeMod: -1}, {Name: "ol_i_id", Type: "INTEGER", SourceType: 23, SourceTypeMod: -1}, {Name: "ol_supply_w_id", Type: "INTEGER", SourceType: 23, SourceTypeMod: -1}, {Name: "ol_d_id", Type: "SMALLINT", SourceType: 21, SourceTypeMod: -1}, {Name: "ol_number", Type: "SMALLINT", SourceType: 21, SourceTypeMod: -1}, {Name: "ol_quantity", Type: "SMALLINT", SourceType: 21, SourceTypeMod: -1}, {Name: "ol_amount", Type: "DECIMAL(6,2)", SourceType: 1700, SourceTypeMod: 393222}, {Name: "ol_dist_info", Type: "VARCHAR", SourceType: 1042, SourceTypeMod: 28, TrimmedOnCopy: true}}},
		{Schema: "public", Name: "region", Columns: []CatalogColumn{{Name: "r_regionkey", Type: "INTEGER", SourceType: 23, SourceTypeMod: -1}, {Name: "r_name", Type: "VARCHAR", SourceType: 1042, SourceTypeMod: 59, TrimmedOnCopy: true}, {Name: "r_comment", Type: "VARCHAR", SourceType: 1042, SourceTypeMod: 156, TrimmedOnCopy: true}}},
		{Schema: "public", Name: "nation", Columns: []CatalogColumn{{Name: "n_nationkey", Type: "INTEGER", SourceType: 23, SourceTypeMod: -1}, {Name: "n_name", Type: "VARCHAR", SourceType: 1042, SourceTypeMod: 29, TrimmedOnCopy: true}, {Name: "n_regionkey", Type: "INTEGER", SourceType: 23, SourceTypeMod: -1}, {Name: "n_comment", Type: "VARCHAR", SourceType: 1042, SourceTypeMod: 156, TrimmedOnCopy: true}}},
		{Schema: "public", Name: "supplier", Columns: []CatalogColumn{{Name: "su_suppkey", Type: "INTEGER", SourceType: 23, SourceTypeMod: -1}, {Name: "su_name", Type: "VARCHAR", SourceType: 1042, SourceTypeMod: 29, TrimmedOnCopy: true}, {Name: "su_address", Type: "VARCHAR", SourceType: 1043, SourceTypeMod: 44}, {Name: "su_nationkey", Type: "INTEGER", SourceType: 23, SourceTypeMod: -1}, {Name: "su_phone", Type: "VARCHAR", SourceType: 1042, SourceTypeMod: 19, TrimmedOnCopy: true}, {Name: "su_acctbal", Type: "DECIMAL(12,2)", SourceType: 1700, SourceTypeMod: 786438}, {Name: "su_comment", Type: "VARCHAR", SourceType: 1042, SourceTypeMod: 105, TrimmedOnCopy: true}}},
	}
}

const (
	chQ2  = "SELECT su_suppkey, su_name, n_name, i_id, i_name, su_address, su_phone, su_comment FROM item, supplier, stock, nation, region, (SELECT s_i_id AS m_i_id, MIN(s_quantity) AS m_s_quantity FROM stock, supplier, nation, region WHERE MOD((s_w_id*s_i_id), 10000)=su_suppkey AND su_nationkey=n_nationkey AND n_regionkey=r_regionkey AND r_name LIKE 'Europ%' GROUP BY s_i_id) m WHERE i_id = s_i_id AND MOD((s_w_id * s_i_id), 10000) = su_suppkey AND su_nationkey = n_nationkey AND n_regionkey = r_regionkey AND i_data LIKE '%b' AND r_name LIKE 'Europ%' AND i_id=m_i_id AND s_quantity = m_s_quantity ORDER BY n_name, su_name, i_id LIMIT 100"
	chQ15 = "WITH revenue0 (supplier_no, total_revenue) AS (SELECT mod((s_w_id * s_i_id),10000) as supplier_no, sum(ol_amount) as total_revenue FROM order_line, stock WHERE ol_i_id = s_i_id AND ol_supply_w_id = s_w_id AND ol_delivery_d >= '2007-01-02 00:00:00.000000' GROUP BY supplier_no) SELECT su_suppkey, su_name, su_address, su_phone, total_revenue FROM supplier, revenue0 WHERE su_suppkey = supplier_no AND total_revenue = (select max(total_revenue) from revenue0) ORDER BY su_suppkey"
)

// CH-benCHmark's Q2 reads the minimum its table in FROM names m_s_quantity:
// the copy answers it.
func TestAliasScope_CH(t *testing.T) {
	if why := CopyScreenFor(chQ2, chCatalog()); why != "" {
		t.Errorf("Q2 sent to the source: %s", why)
	}
}

// A name given in the select list of a table in FROM or of a WITH is a
// column of that table to the statement around it, which reads it in its
// WHERE on PostgreSQL too.
func TestAliasScope_outerReadsInnerName(t *testing.T) {
	for _, q := range []string{
		"SELECT y FROM (SELECT k AS y FROM a) s WHERE y = 1",
		"SELECT y FROM (SELECT k y FROM a) s WHERE y = 1",
		"WITH w AS (SELECT k AS y FROM a) SELECT y FROM w WHERE y > 1",
		"SELECT y FROM (SELECT y FROM (SELECT k AS y FROM a) i) o WHERE y = 1",
		"SELECT y FROM (SELECT k AS y FROM a) s WHERE mod(y, 2) = 0",
		"SELECT y FROM ((SELECT k AS y FROM a)) s WHERE y = 1",
		"SELECT n FROM (SELECT k AS y, count(*) AS n FROM a GROUP BY k) s WHERE y = 1 AND n > 2",
		"SELECT y, count(*) FROM (SELECT k AS y FROM a) s GROUP BY y HAVING y > 1",
	} {
		if why := aliasUse(lexOrFail(t, q), nil, false); why != "" {
			t.Errorf("%s\n    vetoed: %s", q, why)
		}
	}
}

// Every other use of a select list's name in WHERE or HAVING stays the
// source's: PostgreSQL refuses it, or reads a column of that name, where the
// copy may read the alias.
func TestAliasScope_otherUsesVetoed(t *testing.T) {
	for _, q := range []string{
		// The same statement's.
		"SELECT k AS y FROM a WHERE y = 1",
		"SELECT k y FROM a WHERE y = 1",
		"SELECT k AS y FROM a WHERE mod(y, 2) = 0",
		"SELECT k AS y, count(*) FROM a GROUP BY k HAVING y > 1",
		"SELECT k AS y FROM a WHERE id IN (1, 2) AND y = 1",
		// After a subquery of WHERE, the WHERE goes on.
		"SELECT k AS y FROM a WHERE id IN (SELECT id FROM b) AND y = 1",
		// Inside a subquery of the statement that names it.
		"SELECT k AS y FROM a WHERE id IN (SELECT id FROM b WHERE y = 1)",
		"SELECT k AS y FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.id = a.id AND y = 1)",
		// A table in FROM beside the one that names it.
		"SELECT * FROM (SELECT k AS y FROM a) s, (SELECT id FROM b WHERE y = 1) t",
		// The outer statement gives the name too.
		"SELECT k AS y FROM (SELECT k AS y FROM a) s WHERE y = 1",
		// Each side of a UNION is one statement.
		"SELECT k AS y FROM a UNION SELECT id FROM b WHERE y = 1",
		// A bare name after a quoted column is a name of the select list.
		`SELECT "k" y FROM a WHERE y = 1`,
		`SELECT "k" y FROM a WHERE EXISTS (SELECT 1 AS y FROM b) AND y = 1`,
		"SELECT * FROM (SELECT k AS y FROM a WHERE y = 1) s",
	} {
		if why := aliasUse(lexOrFail(t, q), nil, false); why == "" {
			t.Errorf("%s\n    left to the copy", q)
		}
	}
}

// A padded char(n) read through a name stays the source's too: the battery
// of #3219 measured SELECT ... FROM (SELECT id, c AS r FROM cn_a) s WHERE
// r <> 'a  ' answered otherwise by the copy.
func TestAliasScope_paddedThroughName(t *testing.T) {
	for _, q := range []string{
		"SELECT id FROM (SELECT id, c AS r FROM d) s WHERE r <> 'a  '",
		"SELECT id FROM (SELECT id, c r FROM d) s WHERE r = 'a'",
		"SELECT id FROM (SELECT id, r AS z FROM (SELECT id, c AS r FROM d) i) s WHERE z = 'a'",
	} {
		if why := CopyScreenFor(q, trimmedCatalog()); why == "" {
			t.Errorf("%s\n    left to the copy", q)
		}
		if why := aliasUse(lexOrFail(t, q), screenTypes(trimmedCatalog(), nil), false); why == "" {
			t.Errorf("%s\n    let through by aliasUse", q)
		}
	}
}

// A trimmed char(n) read again through a name stays the source's, whatever
// statement gives the name (#3219): the screen of trimmed.go sends it there.
func TestAliasScope_trimmedThroughName(t *testing.T) {
	for _, q := range []string{
		"SELECT y FROM (SELECT c AS y FROM a) s WHERE y = 'ab  '",
		"SELECT y FROM (SELECT c AS y FROM a) s WHERE y = 'ab'",
		"WITH w AS (SELECT c AS y FROM a) SELECT y FROM w WHERE y = 'ab'",
		"SELECT y FROM (SELECT c AS y FROM a) s WHERE y LIKE 'a%'",
		// Through two names.
		"SELECT * FROM (SELECT y AS z FROM (SELECT c AS y FROM a) i) m WHERE z = 'ab  '",
		"SELECT * FROM (SELECT y z FROM (SELECT c y FROM a) i) m WHERE z = 'ab'",
	} {
		if why := CopyScreenFor(q, trimmedCatalog()); why == "" {
			t.Errorf("%s\n    left to the copy", q)
		}
	}
}

func lexOrFail(t *testing.T, q string) []rtToken {
	t.Helper()
	toks, why := lexSQL(q)
	if why != "" {
		t.Fatalf("%s: not lexed: %s", q, why)
	}
	return toks
}

// Without the columns' own types the screen may not veto what the typed one
// lets through (screenUntyped): Q2 and Q15 pass it, and the typed screen,
// asked before the copy answers, decides.
func TestAliasScope_untypedLetsThrough(t *testing.T) {
	for _, q := range []string{chQ2, chQ15,
		"SELECT y FROM (SELECT k AS y FROM a) s WHERE y = 1",
		"WITH w (y) AS (SELECT k FROM a) SELECT y FROM w WHERE y = 1",
	} {
		toks := lexOrFail(t, q)
		if why := screenWith(toks, untypedAsInteger(toks), true, false); why != "" {
			t.Errorf("%s\n    vetoed without types: %s", q, why)
		}
	}
}

// Q2 and Q15 as a session routes them: the copy's, with the screen of their
// types asked too. The same shapes over a trimmed char(n) or a timestamp are
// the source's.
func TestAliasScope_route(t *testing.T) {
	cat := append(chCatalog(), trimmedCatalog()...)
	b := &Binding{Schema: "public", Catalog: func(context.Context) ([]CatalogTable, error) { return cat, nil },
		Forward: &Forward{DSN: "postgres://fwd:pw@127.0.0.1:1/db"}}
	s := &session{bound: b, logger: discardLogger(), currentDB: "public"}
	s.initSettings("public", "app")
	for stmt, want := range map[string]routeDecision{
		chQ2:  {RouteCopy, ReasonAnswersTheSame},
		chQ15: {RouteCopy, ReasonAnswersTheSame},
		"WITH w (y) AS (SELECT ol_delivery_d FROM order_line) SELECT count(*) FROM w WHERE y = '2026-03-02 10:00:00+02'": {RouteSource, ReasonVeto},
		"SELECT y FROM (SELECT c AS y FROM a) s WHERE y = 'ab  '":                                                       {RouteSource, ReasonVeto},
		"WITH w(y) AS (SELECT c FROM a) SELECT y FROM w WHERE y = 'ab  '":                                               {RouteSource, ReasonVeto},
	} {
		if got := s.screenTyped(stmt, s.route(s.classify(stmt), stmt)); got != want {
			t.Errorf("%s: %+v, want %+v", stmt, got, want)
		}
	}
}

// Q2 and Q15 are let through by this screen; what the rest of the screen
// says of them is pinned by TestAliasScope_CH.
func TestAliasScope_CHNames(t *testing.T) {
	for _, q := range []string{chQ2, chQ15} {
		if why := aliasUse(lexOrFail(t, q), screenTypes(chCatalog(), nil), false); why != "" {
			t.Errorf("%.60s...\n    vetoed: %s", q, why)
		}
	}
}

// WITH name (a, b) AS (...) names the WITH's columns: the copy reads them
// as PostgreSQL does (CH-benCHmark Q15).
func TestCTEColumnList(t *testing.T) {
	for _, q := range []string{
		"WITH w (y) AS (SELECT k FROM a) SELECT y FROM w WHERE y = 1",
		"WITH w (y, n) AS (SELECT k, count(*) FROM a GROUP BY k) SELECT y, n FROM w WHERE n > 1 ORDER BY y",
		"WITH v AS (SELECT id FROM a), w (y) AS (SELECT k FROM a) SELECT y FROM w, v WHERE y = v.id",
		"WITH w (y) AS MATERIALIZED (SELECT k FROM a) SELECT y FROM w WHERE y = 1",
		"WITH w (y) AS NOT MATERIALIZED (SELECT k FROM a) SELECT y FROM w WHERE y = 1",
	} {
		if why := CopyScreenFor(q, trimmedCatalog()); why != "" {
			t.Errorf("%s\n    sent to the source: %s", q, why)
		}
	}
	for _, q := range []string{
		// A list over a trimmed char(n): its names hide the column.
		"WITH w (y) AS (SELECT c FROM a) SELECT y FROM w WHERE y = 'ab'",
		"WITH w (y) AS (SELECT c FROM a) SELECT y = 'ab  ' FROM w",
		"WITH w (i, y) AS (SELECT id, trim(c) FROM a) SELECT i FROM w",
		"WITH w (y) AS (SELECT a.c FROM a) SELECT 1 FROM w",
		// A * in the body: the list's names would land on other columns.
		"WITH w (a1, b1) AS (SELECT *, 1 FROM a) SELECT max(b1) FROM w",
		"WITH w (a1, b1) AS (SELECT *, 1 FROM a) SELECT a1 FROM w WHERE b1 > 'M'",
		"WITH w (a1, b1, c1) AS (SELECT b.id, a.*, 0 FROM a, b) SELECT max(c1) FROM w",
		"WITH w (a1) AS (SELECT DISTINCT * FROM b) SELECT a1 FROM w",
		// A name twice.
		"WITH w (y, y) AS (SELECT 1, 2) SELECT * FROM w",
		// A function of FROM with a list of its columns is no WITH.
		"SELECT * FROM a, foo(zz) AS (p int, q text)",
		"WITH v AS (SELECT 1) SELECT * FROM v, foo(zz) AS (p int, q text)",
		// A function is still a function.
		"SELECT foo(k) FROM a",
		"WITH w AS (SELECT foo(k) AS y FROM a) SELECT y FROM w",
		// A name of the list read inside the body is no column of it.
		"WITH w (y) AS (SELECT k FROM a WHERE y = 1) SELECT y FROM w",
	} {
		if why := CopyScreenFor(q, trimmedCatalog()); why == "" {
			t.Errorf("%s\n    left to the copy", q)
		}
	}
}

func TestCTEColumnListAt(t *testing.T) {
	for _, c := range []struct {
		q    string
		want bool
	}{
		{"WITH w (y) AS (SELECT 1)", true},
		{"WITH RECURSIVE w (y) AS (SELECT 1)", true},
		{"WITH v AS (SELECT 1), w (y, z) AS (SELECT 1, 2)", true},
		{"WITH v AS MATERIALIZED (SELECT 1), w (y) AS (SELECT 1)", true},
		{"SELECT * FROM a, f(x) AS (SELECT 1)", false},
		{"SELECT * FROM (SELECT 1) s, f(x) AS (SELECT 1)", false},
		{"WITH w () AS (SELECT 1)", false},
		{"WITH w (y z) AS (SELECT 1)", false},
		{"WITH w (y) (SELECT 1)", false},
		{"SELECT count(y) AS n FROM a", false},
		{"SELECT a, f(y) AS z FROM a", false},
	} {
		q, want := c.q, c.want
		toks := lexOrFail(t, q)
		got := false
		for i := range toks {
			if cteColumnList(toks, i) >= 0 {
				got = true
			}
		}
		if got != want {
			t.Errorf("%s: a WITH column list = %v, want %v", q, got, want)
		}
	}
}

// CH-benCHmark's Q15 reads its WITH's columns by the names of the list.
func TestCTEColumnList_CH(t *testing.T) {
	if why := CopyScreenFor(chQ15, chCatalog()); why != "" {
		t.Errorf("Q15 sent to the source: %s", why)
	}
}

// mod() of a name of a WITH's column list, whose type the screen does not
// know, stays the source's; mod() of the table's own columns does not.
func TestCTEColumnList_mod(t *testing.T) {
	cat := chCatalog()
	if why := CopyScreenFor("WITH w (x) AS (SELECT s_w_id FROM stock) SELECT mod(x, 3) FROM w", cat); why == "" {
		t.Error("mod() of a name of the list left to the copy")
	}
	if why := CopyScreenFor("WITH w (x) AS (SELECT mod(s_w_id, 3) FROM stock) SELECT x FROM w", cat); why != "" {
		t.Errorf("mod() of the table's column sent to the source: %s", why)
	}
	if why := CopyScreenFor("SELECT mod(s_w_id, 3) FROM (SELECT s_w_id FROM stock) m (s_w_id)", cat); why == "" {
		t.Error("a column list of a table in FROM left to the copy")
	}
}

func TestSelectItems(t *testing.T) {
	for _, c := range []struct {
		q    string
		want int
	}{
		{"SELECT a, b FROM t", 2},
		{"SELECT mod((x*y),10) AS n, sum(z) total FROM t", 2},
		{"SELECT DISTINCT a, f(b, c) FROM t", 2},
		{"SELECT DISTINCT ON (a) a FROM t", 0},
		{"SELECT CASE WHEN a THEN 1 END, b.c FROM t", 2},
		{"SELECT 1", 1},
	} {
		q, want := c.q, c.want
		toks := lexOrFail(t, q)
		if got := len(selectItems(toks, 0)); got != want {
			t.Errorf("%s: %d items, want %d", q, got, want)
		}
	}
	items := selectItems(lexOrFail(t, "SELECT mod(x, 10) AS n, sum(z) total, c FROM t"), 0)
	if len(items) != 3 || len(items[0]) != 6 || len(items[1]) != 4 || len(items[2]) != 1 {
		t.Errorf("items without their names: %v", items)
	}
}

// A name that stands for a column that is not a number hides the column's
// type from the rules of a comparison (a timestamp against a string with a
// time zone, a loose date, text against a number): every use of it is the
// source's. So is a name a table read beside it has as a column.
func TestAliasScope_typedThroughName(t *testing.T) {
	cat := []CatalogTable{
		{Schema: "public", Name: "o", Columns: []CatalogColumn{{Name: "id", Type: "INTEGER", SourceType: 23, SourceTypeMod: -1}, {Name: "placed_at", Type: "TIMESTAMP", SourceType: 1114, SourceTypeMod: -1}, {Name: "day", Type: "DATE", SourceType: 1082, SourceTypeMod: -1}, {Name: "note", Type: "VARCHAR", SourceType: 25, SourceTypeMod: -1}, {Name: "amount", Type: "DECIMAL(6,2)", SourceType: 1700, SourceTypeMod: 393222}}},
		{Schema: "public", Name: "t", Columns: []CatalogColumn{{Name: "id", Type: "INTEGER", SourceType: 23, SourceTypeMod: -1}, {Name: "y", Type: "TIMESTAMP", SourceType: 1114, SourceTypeMod: -1}}},
	}
	for _, q := range []string{
		"SELECT id FROM (SELECT id, placed_at AS p FROM o) s WHERE p = '2026-03-02 10:00:00+02'",
		"WITH w (p) AS (SELECT placed_at FROM o) SELECT count(*) FROM w WHERE p = '2026-03-02 10:00:00+02'",
		"SELECT id FROM (SELECT id, placed_at AS p FROM o) s WHERE p = '2026-3-2'",
		"SELECT id FROM (SELECT id, day AS p FROM o) s WHERE p IN ('2026-3-2')",
		"SELECT id FROM (SELECT id, note AS p FROM o) s WHERE p = 5",
		"SELECT id FROM (SELECT id, coalesce(placed_at, now()) AS p FROM o) s WHERE p = '2026-03-02 10:00:00+02'",
		"SELECT id FROM (SELECT id, p AS q FROM (SELECT id, placed_at AS p FROM o) i) s WHERE q = '2026-3-2'",
		"SELECT * FROM t WHERE y = '2026-03-02 10:00:00+02' AND id IN (SELECT id AS y FROM o)",
	} {
		if why := aliasUse(lexOrFail(t, q), screenTypes(cat, nil), false); why == "" {
			t.Errorf("%s\n    left to the copy", q)
		}
	}
	// A number renamed keeps reading as before.
	for _, q := range []string{
		"SELECT id FROM (SELECT id, amount AS p FROM o) s WHERE p > 5",
		"WITH w (k, total) AS (SELECT id, sum(amount) FROM o GROUP BY id) SELECT k FROM w WHERE total > 5",
		"SELECT n FROM (SELECT id, count(*) AS n FROM o GROUP BY id) s WHERE n > 2",
	} {
		if why := aliasUse(lexOrFail(t, q), screenTypes(cat, nil), false); why != "" {
			t.Errorf("%s\n    vetoed: %s", q, why)
		}
	}
}
