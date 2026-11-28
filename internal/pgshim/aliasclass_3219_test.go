package pgshim

import (
	"fmt"
	"strings"
	"testing"
)

// A name a WITH or a select list defines is read as what it stands for, when
// every definition agrees (#3219).
func TestAliasClassOf(t *testing.T) {
	types := map[string][]string{"amount": {"DECIMAL(12,2)"}, "qty": {"INTEGER"}, "ratio": {"DOUBLE"}, "id": {"BIGINT"}}
	of := func(n string) []string { return types[n] }
	for _, tc := range []struct {
		q, name string
		want    exactCls
	}{
		{"WITH p AS (SELECT sum(amount) AS spent FROM t) SELECT spent FROM p", "spent", clsDecimal},
		{"WITH p AS (SELECT sum(qty) AS n FROM t) SELECT n FROM p", "n", clsWhole64},
		{"WITH p AS (SELECT qty AS n FROM t), q AS (SELECT n + 1 AS m FROM p) SELECT m FROM q", "m", clsWhole32},
		// Not known: a float, two definitions that disagree, a column list
		// beside a table, a name that is also a column of another class.
		{"WITH p AS (SELECT sum(ratio) AS s FROM t) SELECT s FROM p", "s", clsUnknown},
		{"SELECT x FROM (SELECT amount AS x FROM t UNION ALL SELECT qty AS x FROM t) u", "x", clsUnknown},
		{"SELECT s FROM (SELECT sum(amount) FROM t) AS u(s)", "s", clsUnknown},
		{"SELECT qty FROM (SELECT sum(amount) AS qty FROM t) u", "qty", clsUnknown},
	} {
		toks, why := lexSQL(tc.q)
		if why != "" {
			t.Fatal(why)
		}
		if got := classOfTypes(aliasClassOf(toks, of)(tc.name)); got != tc.want {
			t.Errorf("%s: %s is %v, want %v", tc.q, tc.name, got, tc.want)
		}
	}
}

// sum of smallints or integers is written for the copy as the macro that
// answers PostgreSQL's bigint; any other sum is left as it is.
func TestRetypeForCopy_sum32(t *testing.T) {
	types := map[string][]string{"amount": {"DECIMAL(12,2)"}, "qty": {"INTEGER"}, "small": {"SMALLINT"}, "id": {"BIGINT"}, "ratio": {"DOUBLE"}}
	for q, want := range map[string]string{
		"SELECT sum(qty) FROM t":                                            "SELECT " + pgSum32 + "(qty) FROM t",
		"SELECT sum(small), SUM(t.qty) FROM t":                              "SELECT " + pgSum32 + "(small), " + pgSum32 + "(t.qty) FROM t",
		"SELECT sum(CASE WHEN qty > 1 THEN 1 ELSE 0 END) FROM t":            "SELECT " + pgSum32 + "(CASE WHEN qty > 1 THEN 1 ELSE 0 END) FROM t",
		"SELECT x, sum(qty) + 1 FROM t GROUP BY x HAVING sum(qty) > 2":      "SELECT x, " + pgSum32 + "(qty) + 1 FROM t GROUP BY x HAVING " + pgSum32 + "(qty) > 2",
		"SELECT sum(id) FROM t":                                             "SELECT sum(id) FROM t",
		"SELECT sum(amount) FROM t":                                         "SELECT sum(amount) FROM t",
		"SELECT sum(ratio) FROM t":                                          "SELECT sum(ratio) FROM t",
		"SELECT sum(DISTINCT qty) FROM t":                                   "SELECT sum(DISTINCT qty) FROM t",
		"SELECT sum(qty) FILTER (WHERE qty > 1) FROM t":                     "SELECT sum(qty) FILTER (WHERE qty > 1) FROM t",
		"SELECT sum(qty) OVER () FROM t":                                    "SELECT sum(qty) OVER () FROM t",
		"SELECT sum(qty ORDER BY qty) FROM t":                               "SELECT sum(qty ORDER BY qty) FROM t",
		"SELECT sum(qty + id) FROM t":                                       "SELECT sum(qty + id) FROM t",
		"SELECT sum(nothere) FROM t":                                        "SELECT sum(nothere) FROM t",
		"SELECT t.sum(qty) FROM t":                                          "SELECT t.sum(qty) FROM t",
	} {
		if got := retypeForCopy(q, types); got != want {
			t.Errorf("%s:\n got  %s\n want %s", q, got, want)
		}
	}
	// Inside one of the copy's own macros the sum is left as the engine's.
	if got := RewriteForCopyTyped("SELECT round(sum(qty) / count(*), 2) FROM t", types); strings.Contains(got, pgSum32) {
		t.Errorf("inside a round: %s", got)
	}
	// The macro is the copy's, and a statement that names it is the source's.
	if veto := CopyScreen("SELECT "+pgSum32+"(qty) FROM t", types); veto == "" {
		t.Error("a statement naming the macro is answered by the copy")
	}
	if veto := CopyScreen("SELECT sum(qty), sum(CASE WHEN qty > 1 THEN 1 ELSE 0 END) FROM t", types); veto != "" {
		t.Errorf("sum of integers: %s", veto)
	}
}

// A varchar or char(n) column returned as it is is announced with its source
// type; anything else keeps the copy's.
func TestTextItemTypes(t *testing.T) {
	col := func(name, typ string, oid uint32, mod int32) CatalogColumn {
		return CatalogColumn{Name: name, Type: typ, SourceType: oid, SourceTypeMod: mod}
	}
	tables := []CatalogTable{
		{Schema: "public", Name: "customer", Columns: []CatalogColumn{
			col("c_id", "INTEGER", 23, -1), col("c_last", "VARCHAR", oidVarchar, 20), col("c_credit", "VARCHAR", oidBpchar, 6), col("note", "VARCHAR", oidText, -1)}},
		{Schema: "public", Name: "item", Columns: []CatalogColumn{
			col("i_id", "INTEGER", 23, -1), col("i_name", "VARCHAR", oidVarchar, 28), col("c_last", "VARCHAR", oidVarchar, 40)}},
	}
	vc := func(mod int32) colType { return colType{oid: oidVarchar, size: -1, typmod: mod} }
	bp := colType{oid: oidBpchar, size: -1, typmod: 6}
	for _, tc := range []struct {
		q    string
		n    int
		want []colType
	}{
		{"SELECT c_last, c.c_credit, c_id, note FROM customer c", 4, []colType{vc(20), bp, {}, {}}},
		{"SELECT c_last AS name, public.customer.c_credit FROM customer", 2, []colType{vc(20), bp}},
		{"SELECT c_credit, count(*) FROM customer GROUP BY c_credit ORDER BY 1", 2, []colType{bp, {}}},
		{"SELECT t.r, i.i_name FROM (SELECT i_id AS r FROM item) t JOIN item i ON i.i_id = t.r", 2, []colType{{}, vc(28)}},
		{"WITH w AS (SELECT c_last FROM customer) SELECT c_last FROM w", 1, []colType{vc(20)}},
		// c_last of two lengths among the tables read: neither.
		{"SELECT c_last FROM customer, item", 1, []colType{{}}},
		// A name the statement makes: not the column's.
		{"WITH w AS (SELECT upper(c_last) AS c_last FROM customer) SELECT c_last FROM w", 1, []colType{{}}},
		{"SELECT upper(c_last), c_last || 'x' FROM customer", 2, []colType{{}, {}}},
	} {
		if got := textItemTypes(tc.q, tables, tc.n); fmtTypes(got) != fmtTypes(tc.want) {
			t.Errorf("%s: %s, want %s", tc.q, fmtTypes(got), fmtTypes(tc.want))
		}
	}
	for _, q := range []string{
		"SELECT * FROM customer",
		"SELECT c_last FROM customer UNION SELECT i_name FROM item",
		"SELECT s FROM (SELECT c_last FROM customer) AS t(s)",
	} {
		if got := textItemTypes(q, tables, 4); got != nil {
			t.Errorf("%s: %v, want nil", q, got)
		}
	}
	if got := textItemTypes("SELECT c_last FROM customer", tables, 2); got != nil {
		t.Errorf("another number of columns: %v", got)
	}
}

func fmtTypes(ts []colType) string { return fmt.Sprint(ts) }
