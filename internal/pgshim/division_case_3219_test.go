package pgshim

import (
	"strconv"
	"testing"
)

// chTables are the columns CH-benCHmark's Q8 and Q14 read, with the types
// PostgreSQL 16 has for them in HammerDB's TPROC-C schema and in the three
// tables CH-benCHmark adds, and the copy's type for each.
func chTables() []CatalogTable {
	const (
		int2, int4, numeric, varchar, timestamptz = 21, 23, 1700, 1043, 1184
	)
	in := func(n string) CatalogColumn { return CatalogColumn{Name: n, Type: "INTEGER", SourceType: int4, SourceTypeMod: -1} }
	small := func(n string) CatalogColumn {
		return CatalogColumn{Name: n, Type: "SMALLINT", SourceType: int2, SourceTypeMod: -1}
	}
	num := func(n string, p, s int32) CatalogColumn {
		return CatalogColumn{Name: n, Type: "DECIMAL(" + strconv.Itoa(int(p)) + "," + strconv.Itoa(int(s)) + ")", SourceType: numeric, SourceTypeMod: p<<16 | s + 4}
	}
	vc := func(n string, l int32) CatalogColumn {
		return CatalogColumn{Name: n, Type: "VARCHAR", SourceType: varchar, SourceTypeMod: l + 4}
	}
	ch := func(n string, l int32) CatalogColumn {
		return CatalogColumn{Name: n, Type: "VARCHAR", SourceType: oidBpchar, SourceTypeMod: l + 4}
	}
	ts := func(n string) CatalogColumn {
		return CatalogColumn{Name: n, Type: "TIMESTAMP WITH TIME ZONE", SourceType: timestamptz, SourceTypeMod: -1}
	}
	return []CatalogTable{
		{Schema: "public", Name: "item", Columns: []CatalogColumn{in("i_id"), in("i_im_id"), vc("i_name", 24), num("i_price", 5, 2), vc("i_data", 50)}},
		{Schema: "public", Name: "supplier", Columns: []CatalogColumn{in("su_suppkey"), ch("su_name", 25), in("su_nationkey"), num("su_acctbal", 12, 2)}},
		{Schema: "public", Name: "stock", Columns: []CatalogColumn{in("s_i_id"), in("s_w_id"), small("s_quantity"), vc("s_data", 50)}},
		{Schema: "public", Name: "order_line", Columns: []CatalogColumn{in("ol_o_id"), small("ol_d_id"), in("ol_w_id"), small("ol_number"),
			in("ol_i_id"), in("ol_supply_w_id"), ts("ol_delivery_d"), small("ol_quantity"), num("ol_amount", 6, 2)}},
		{Schema: "public", Name: "orders", Columns: []CatalogColumn{in("o_id"), small("o_d_id"), in("o_w_id"), in("o_c_id"), ts("o_entry_d"), small("o_ol_cnt")}},
		{Schema: "public", Name: "customer", Columns: []CatalogColumn{in("c_id"), small("c_d_id"), in("c_w_id"), ch("c_state", 2), vc("c_last", 16)}},
		{Schema: "public", Name: "nation", Columns: []CatalogColumn{in("n_nationkey"), ch("n_name", 25), in("n_regionkey")}},
		{Schema: "public", Name: "region", Columns: []CatalogColumn{in("r_regionkey"), ch("r_name", 55)}},
	}
}

// CH-benCHmark's Q8 and Q14, as BenchBase publishes them (the rig's copy,
// bench queries.json).
const (
	chQ8 = `SELECT extract(YEAR FROM o_entry_d) AS l_year, sum(CASE WHEN n2.n_name = 'Germany' THEN ol_amount ELSE 0 END) / sum(ol_amount) AS mkt_share FROM item, supplier, stock, order_line, orders, customer, nation n1, nation n2, region WHERE i_id = s_i_id AND ol_i_id = s_i_id AND ol_supply_w_id = s_w_id AND MOD ((s_w_id * s_i_id), 10000) = su_suppkey AND ol_w_id = o_w_id AND ol_d_id = o_d_id AND ol_o_id = o_id AND c_id = o_c_id AND c_w_id = o_w_id AND c_d_id = o_d_id AND n1.n_nationkey = ascii(substring(c_state from 1 for 1)) AND n1.n_regionkey = r_regionkey AND ol_i_id < 1000 AND r_name = 'Europe' AND su_nationkey = n2.n_nationkey AND i_data LIKE '%b' AND i_id = ol_i_id GROUP BY l_year ORDER BY l_year`
	chQ14 = `SELECT (100.00 * sum(CASE WHEN i_data LIKE 'PR%' THEN ol_amount ELSE 0 END) / (1 + sum(ol_amount))) AS promo_revenue FROM order_line, item WHERE ol_i_id = i_id AND ol_delivery_d >= '2007-01-02 00:00:00.000000' AND ol_delivery_d < '2032-01-02 00:00:00.000000'`
)

func TestProbeCHQ8Q14(t *testing.T) {
	for name, q := range map[string]string{"Q8": chQ8, "Q14": chQ14} {
		t.Logf("%s rewritten: %s", name, RewriteForCopyTyped(q, screenTypes(chTables(), nil)))
		t.Logf("%s veto: %q", name, CopyScreenFor(q, chTables()))
	}
}
