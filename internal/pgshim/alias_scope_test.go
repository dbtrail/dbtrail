package pgshim

import "testing"

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

// Without the columns' own types no name is let through: whether one hides
// a trimmed char(n) is not known.
func TestAliasScope_untypedVetoed(t *testing.T) {
	q := "SELECT y FROM (SELECT k AS y FROM a) s WHERE y = 1"
	if why := aliasUse(lexOrFail(t, q), nil, true); why == "" {
		t.Errorf("%s\n    left to the copy without types", q)
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
