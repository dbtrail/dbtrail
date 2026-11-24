package pgshim

import (
	"strconv"
	"strings"
	"testing"
)

// caseTypes are the copy's column types for the CASE cases: whole numbers of
// each width, a numeric the macros divide, one declared without precision, a
// float and text.
var caseTypes = map[string][]string{
	"qty": {"INTEGER"}, "small": {"SMALLINT"}, "big": {"BIGINT"}, "amount": {"DECIMAL(12,2)"},
	"free": {"DECIMAL(38,18)"}, "ratio": {"DOUBLE"}, "note": {"VARCHAR"}, "id": {"BIGINT"},
}

// The class of a CASE is what PostgreSQL makes of its values: the values
// after THEN and ELSE joined as a sum's are, a NULL among them taking the
// others' type. Its conditions, and the value a simple CASE compares, are
// no part of it.
func TestExactClass_case(t *testing.T) {
	for _, c := range []struct {
		expr string
		want exactCls
	}{
		{`CASE WHEN qty > 1 THEN amount ELSE 0 END`, clsDecimal},
		{`CASE WHEN qty > 1 THEN qty ELSE 0 END`, clsWhole32},
		{`CASE WHEN note = 'x' THEN small ELSE qty END`, clsWhole32},
		{`CASE WHEN qty > 1 THEN big ELSE 0 END`, clsWhole64},
		{`CASE WHEN qty > 1 THEN qty ELSE amount END`, clsDecimal},
		{`CASE WHEN qty > 1 THEN 1 ELSE 0 END`, clsWhole32},
		{`CASE WHEN qty > 1 THEN 1.5 ELSE 2 END`, clsDecimal},
		{`CASE WHEN qty > 1 THEN -amount ELSE -1 END`, clsDecimal},
		{`CASE WHEN qty > 1 THEN amount END`, clsDecimal},
		{`CASE WHEN qty > 1 THEN amount ELSE NULL END`, clsDecimal},
		{`CASE WHEN qty > 1 THEN NULL ELSE qty END`, clsWhole32},
		{`CASE WHEN qty > 1 THEN amount WHEN qty > 2 THEN qty WHEN qty > 3 THEN NULL ELSE 0 END`, clsDecimal},
		// A simple CASE: the value it compares is no part of its type.
		{`CASE qty WHEN 1 THEN amount ELSE 0 END`, clsDecimal},
		{`CASE ratio WHEN 1.5 THEN qty ELSE 0 END`, clsWhole32},
		// Its conditions are no part of it either: a float, text, a
		// subquery or a division there decide a branch.
		{`CASE WHEN ratio > 0.5 AND note LIKE 'P%' THEN amount ELSE 0 END`, clsDecimal},
		{`CASE WHEN qty IN (SELECT qty FROM t) THEN amount ELSE 0 END`, clsDecimal},
		{`CASE WHEN _bintrail_pg_div(amount, qty) > 1 THEN qty ELSE 0 END`, clsWhole32},
		// One inside another.
		{`CASE WHEN qty > 1 THEN CASE WHEN qty > 2 THEN amount ELSE 1 END ELSE 0 END`, clsDecimal},
		{`CASE WHEN qty > 1 THEN (CASE WHEN qty > 2 THEN qty END) ELSE 0 END`, clsWhole32},
		{`CASE WHEN CASE WHEN qty > 1 THEN true END THEN amount ELSE 0 END`, clsDecimal},
		// Inside arithmetic, a call or parentheses.
		{`CASE WHEN qty > 1 THEN amount ELSE 0 END * 2`, clsDecimal},
		{`-CASE WHEN qty > 1 THEN qty ELSE 0 END`, clsWhole32},
		{`1 + (CASE WHEN qty > 1 THEN amount ELSE 0 END)`, clsDecimal},
		{`sum(CASE WHEN qty > 1 THEN amount ELSE 0 END)`, clsDecimal},
		{`sum(CASE WHEN qty > 1 THEN qty ELSE 0 END)`, clsWhole64},
		{`coalesce(CASE WHEN qty > 1 THEN amount END, 0)`, clsDecimal},
		{`nullif(CASE WHEN qty > 1 THEN qty ELSE 0 END, 0)`, clsWhole32},

		// Not known: a value that is not, and a CASE the port does not read.
		{`sum(CASE WHEN qty > 1 THEN big ELSE 0 END)`, clsUnknown},
		{`CASE WHEN qty > 1 THEN ratio ELSE 0 END`, clsUnknown},
		{`CASE WHEN qty > 1 THEN amount ELSE ratio END`, clsUnknown},
		{`CASE WHEN qty > 1 THEN free ELSE 0 END`, clsUnknown},
		{`CASE WHEN qty > 1 THEN 'a' ELSE 0 END`, clsUnknown},
		{`CASE WHEN qty > 1 THEN note ELSE 0 END`, clsUnknown},
		{`CASE WHEN qty > 1 THEN (SELECT max(amount) FROM t) ELSE 0 END`, clsUnknown},
		{`CASE WHEN qty > 1 THEN CASE WHEN qty > 2 THEN ratio ELSE 1 END ELSE 0 END`, clsUnknown},
		{`CASE WHEN qty > 1 THEN nothing ELSE 0 END`, clsUnknown},
		// All its values NULL: PostgreSQL makes it text.
		{`CASE WHEN qty > 1 THEN NULL END`, clsUnknown},
		{`CASE WHEN qty > 1 THEN NULL ELSE NULL END`, clsUnknown},
		// What follows the END binds to it otherwise.
		{`CASE WHEN qty > 1 THEN amount ELSE 0 END::numeric`, clsUnknown},
		{`CASE WHEN qty > 1 THEN amount ELSE 0 END || 'x'`, clsUnknown},
		{`CASE WHEN qty > 1 THEN amount ELSE 0 END . x`, clsUnknown},
		// Written wrong: no END, a value missing, no WHEN.
		{`CASE WHEN qty > 1 THEN amount ELSE 0`, clsUnknown},
		{`CASE WHEN qty > 1 THEN ELSE 0 END`, clsUnknown},
		{`CASE WHEN qty > 1 THEN amount ELSE END`, clsUnknown},
		{`CASE amount END`, clsUnknown},
		{`CASE WHEN qty > 1 THEN amount END END`, clsUnknown},
		{`CASE END`, clsUnknown},
		// A word of a CASE after a dot is a column's name, read as one: a
		// value it stands in is still read whole.
		{`t.case`, clsUnknown},
		{`CASE WHEN qty > 1 THEN amount ELSE t.case END`, clsUnknown},
		{`CASE WHEN qty > 1 THEN t.ratio * t.then * amount END`, clsUnknown},
		{`CASE WHEN qty > 1 THEN t.qty * t.end * amount END`, clsUnknown},
		{`CASE WHEN t.case > 1 THEN amount ELSE t.qty END`, clsDecimal},
	} {
		toks, why := lexSQL(c.expr)
		if why != "" {
			t.Fatalf("%s: %s", c.expr, why)
		}
		if got := exactClass(toks, func(n string) []string { return caseTypes[n] }); got != c.want {
			t.Errorf("exactClass(%s) = %d, want %d", c.expr, got, c.want)
		}
	}
}

// A division or an avg of a CASE goes to the copy when PostgreSQL's answer
// is a numeric on both sides, and to the source when it is not: two whole
// numbers (PostgreSQL divides them as whole numbers), a float, a sum of a
// bigint, a numeric without precision.
func TestCopyScreen_caseInDivision(t *testing.T) {
	copy := []string{
		`SELECT sum(CASE WHEN note LIKE 'PR%' THEN amount ELSE 0 END) / sum(amount) FROM t`,
		`SELECT 100.00 * sum(CASE WHEN note LIKE 'PR%' THEN amount ELSE 0 END) / (1 + sum(amount)) AS promo FROM t`,
		`SELECT (100.00 * sum(CASE WHEN note LIKE 'PR%' THEN amount ELSE 0 END) / (1 + sum(amount))) AS promo FROM t`,
		`SELECT sum(CASE WHEN qty > 1 THEN amount END) / count(*) FROM t`,
		`SELECT 100.0 * sum(CASE WHEN qty > 1 THEN 1 ELSE 0 END) / count(*) FROM t`,
		`SELECT sum(CASE WHEN qty > 1 THEN qty ELSE 0 END) / sum(amount) FROM t`,
		`SELECT sum(CASE WHEN qty > 1 THEN small ELSE amount END) / sum(amount) FROM t`,
		`SELECT -sum(CASE WHEN qty > 1 THEN amount ELSE 0 END) / sum(amount), sum(amount) / -sum(CASE WHEN qty > 1 THEN amount ELSE 0 END) FROM t`,
		`SELECT sum(CASE qty WHEN 1 THEN amount WHEN 2 THEN qty ELSE 0 END) / sum(amount) FROM t`,
		`SELECT amount / nullif(CASE WHEN qty > 1 THEN qty ELSE 0 END, 0) FROM t`,
		// A divisor of zero in some row: the copy's macro refuses it when it
		// runs, and the source answers PostgreSQL's error.
		`SELECT amount / (CASE WHEN qty > 1 THEN qty ELSE 0 END) FROM t`,
		`SELECT avg(CASE WHEN qty > 1 THEN amount ELSE 0 END), avg(CASE WHEN qty > 1 THEN qty END) FROM t`,
		// Used to choose and to sort the rows.
		`SELECT qty FROM t GROUP BY qty HAVING sum(CASE WHEN note LIKE 'P%' THEN amount ELSE 0 END) / nullif(sum(amount), 0) > 0.5`,
		`SELECT qty, sum(CASE WHEN note LIKE 'P%' THEN amount ELSE 0 END) / nullif(sum(amount), 0) AS share FROM t GROUP BY qty ORDER BY share DESC NULLS LAST, qty`,
	}
	for _, q := range copy {
		if veto := CopyScreen(q, caseTypes); veto != "" {
			t.Errorf("sent to the source (%s), want the copy: %s", veto, q)
		}
		if toks, why := lexSQL(RewriteForCopy(q)); why != "" {
			t.Errorf("not read: %s: %s", why, q)
		} else if veto := screenUntyped(toks); veto != "" {
			t.Errorf("vetoed without its types (%s): %s", veto, q)
		}
	}
	const unknown, whole = "cannot show to be a numeric or a whole number", "two whole numbers"
	for q, want := range map[string]string{
		// PostgreSQL: bigint / bigint, a whole number (3 for 7 / 2).
		`SELECT sum(CASE WHEN qty > 1 THEN qty ELSE 0 END) / count(*) FROM t`:                        whole,
		`SELECT sum(CASE WHEN qty > 1 THEN 1 ELSE 0 END) / count(*) FROM t`:                          whole,
		`SELECT sum(CASE WHEN qty > 1 THEN 1 ELSE 0 END) / sum(CASE WHEN qty > 0 THEN 1 END) FROM t`: whole,
		`SELECT qty / (CASE WHEN qty > 1 THEN qty ELSE 1 END) FROM t`:                                whole,
		`SELECT CASE WHEN qty > 1 THEN qty ELSE 1 END / 2 FROM t`:                                    "a division",
		`SELECT amount / CASE WHEN qty > 1 THEN qty ELSE 1 END FROM t`:                               "a division",
		// A sum of a bigint is a numeric on PostgreSQL and a whole number on
		// the copy.
		`SELECT sum(CASE WHEN qty > 1 THEN big ELSE 0 END) / sum(amount) FROM t`:        unknown,
		`SELECT sum(CASE WHEN qty > 1 THEN ratio ELSE 0 END) / sum(amount) FROM t`:      unknown,
		`SELECT sum(CASE WHEN qty > 1 THEN amount ELSE ratio END) / sum(amount) FROM t`: unknown,
		`SELECT sum(CASE WHEN qty > 1 THEN free ELSE 0 END) / sum(amount) FROM t`:       unknown,
		`SELECT sum(CASE WHEN qty > 1 THEN NULL END) / sum(amount) FROM t`:              unknown,
		`SELECT sum(CASE WHEN qty > 1 THEN amount END) / 0 FROM t`:                      "a division by zero",
		`SELECT avg(CASE WHEN qty > 1 THEN ratio ELSE 0 END) FROM t`:                    "avg()",
	} {
		veto := CopyScreen(q, caseTypes)
		if want == "a division" {
			// A CASE just before or after the slash is a division left as
			// written: the screen's own rule for one decides it, as before.
			if veto != want {
				t.Errorf("%q, want %q: %s", veto, want, q)
			}
			continue
		}
		if !strings.Contains(veto, want) {
			t.Errorf("%q, want it sent to the source for %q: %s", veto, want, q)
		}
	}
}

// chTables are the columns CH-benCHmark's Q8 and Q14 read, with the types
// PostgreSQL 16 has for them in HammerDB's TPROC-C schema and in the three
// tables CH-benCHmark adds (read from the rig's source), and the copy's
// type for each. plainNames gives the char(n) columns a varchar's type, to
// tell what the division alone decides.
func chTables(plainNames bool) []CatalogTable {
	const (
		int2, int4, numeric, varchar, timestamptz = 21, 23, 1700, 1043, 1184
	)
	in := func(n string) CatalogColumn {
		return CatalogColumn{Name: n, Type: "INTEGER", SourceType: int4, SourceTypeMod: -1}
	}
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
		if plainNames {
			return vc(n, l)
		}
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

// CH-benCHmark's Q8 and Q14, as BenchBase publishes them (the rig's
// queries.json).
const (
	chQ8  = `SELECT extract(YEAR FROM o_entry_d) AS l_year, sum(CASE WHEN n2.n_name = 'Germany' THEN ol_amount ELSE 0 END) / sum(ol_amount) AS mkt_share FROM item, supplier, stock, order_line, orders, customer, nation n1, nation n2, region WHERE i_id = s_i_id AND ol_i_id = s_i_id AND ol_supply_w_id = s_w_id AND MOD ((s_w_id * s_i_id), 10000) = su_suppkey AND ol_w_id = o_w_id AND ol_d_id = o_d_id AND ol_o_id = o_id AND c_id = o_c_id AND c_w_id = o_w_id AND c_d_id = o_d_id AND n1.n_nationkey = ascii(substring(c_state from 1 for 1)) AND n1.n_regionkey = r_regionkey AND ol_i_id < 1000 AND r_name = 'Europe' AND su_nationkey = n2.n_nationkey AND i_data LIKE '%b' AND i_id = ol_i_id GROUP BY l_year ORDER BY l_year`
	chQ14 = `SELECT (100.00 * sum(CASE WHEN i_data LIKE 'PR%' THEN ol_amount ELSE 0 END) / (1 + sum(ol_amount))) AS promo_revenue FROM order_line, item WHERE ol_i_id = i_id AND ol_delivery_d >= '2007-01-02 00:00:00.000000' AND ol_delivery_d < '2032-01-02 00:00:00.000000'`
)

// Q14 goes to the copy. Q8's division does too; what still sends Q8 to the
// source is its char(n) columns, a rule of its own.
func TestCopyScreen_chQ8Q14(t *testing.T) {
	if veto := CopyScreenFor(chQ14, chTables(false)); veto != "" {
		t.Errorf("Q14 sent to the source: %s", veto)
	}
	if veto := CopyScreenFor(chQ8, chTables(false)); veto == "" || strings.Contains(veto, "division") {
		t.Errorf("Q8: %q, want it sent to the source for its char(n) columns alone", veto)
	}
	if veto := CopyScreenFor(chQ8, chTables(true)); veto != "" {
		t.Errorf("Q8 with its char(n) columns typed as varchar, sent to the source: %s", veto)
	}
	for name, q := range map[string]string{"Q8": chQ8, "Q14": chQ14} {
		if toks, why := lexSQL(RewriteForCopy(q)); why != "" {
			t.Errorf("%s not read: %s", name, why)
		} else if veto := screenUntyped(toks); veto != "" {
			t.Errorf("%s vetoed without its types: %s", name, veto)
		}
	}
	// Q8's division itself, as the screen reads it: both sides numerics.
	types := screenTypes(chTables(false), nil)
	of := func(n string) []string { return types[n] }
	toks, why := lexSQL(RewriteForCopyTyped(chQ8, types))
	if why != "" {
		t.Fatal(why)
	}
	found := false
	for i, tk := range toks {
		if tk.kind == rtWord && tk.text == pgDiv && isOpTok(toks, i+1, "(") {
			found = true
			end := closingParen(toks, i+1)
			args := splitTopCommas(toks[i+2 : end])
			if veto := divisionVeto(args, of, false); veto != "" {
				t.Errorf("Q8's division: %s", veto)
			}
		}
	}
	if !found {
		t.Errorf("Q8's division is not written as the macro: %s", RewriteForCopyTyped(chQ8, types))
	}
}

// splitTopCommas is toks split at the commas outside parentheses.
func splitTopCommas(toks []rtToken) [][]rtToken {
	var out [][]rtToken
	depth, from := 0, 0
	for i := range toks {
		switch {
		case isOpTok(toks, i, "("):
			depth++
		case isOpTok(toks, i, ")"):
			depth--
		case depth == 0 && isOpTok(toks, i, ","):
			out = append(out, toks[from:i])
			from = i + 1
		}
	}
	return append(out, toks[from:])
}
