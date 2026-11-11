package console

import (
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/readrouter"
	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
	"github.com/dbtrail/dbtrail/internal/views"
)

// #2641: ROUND over a whole number or an exact DECIMAL is the copy's to
// answer; over a FLOAT, a DOUBLE, text, an expression or a column whose
// type is not known, MySQL's (a half rounds to even there).
func TestSQLRoundRefusalFor(t *testing.T) {
	table := func(name string, safe, other []string) views.BaselineTable {
		return views.BaselineTable{Schema: "shop", Table: name, Path: "/c/shop/" + name + ".parquet", SchemaKnown: true,
			Columns: append(append([]string{}, safe...), other...), DistinctSafe: safe, DistinctOther: other}
	}
	in := views.Input{Baselines: []views.BaselineTable{
		table("orders", []string{"id", "customer_id", "total"}, []string{"status", "weight"}),
		table("customers", []string{"id"}, []string{"tier", "score"}),
		table("readings", []string{"r_id"}, []string{"total"}), // total is a DOUBLE here
	}}
	only := func(names ...string) views.Input {
		var refs sqlsandbox.Refs
		for _, n := range names {
			refs.Tables = append(refs.Tables, sqlsandbox.TableRef{Schema: "shop", Name: n})
		}
		narrowed := in
		narrowed.OnlyViews = sqlWantedViews(in, refs)
		return narrowed
	}
	readme := "SELECT c.tier, count(*) AS orders, round(sum(o.total), 2) AS revenue\n" +
		"FROM orders o JOIN customers c ON c.id = o.customer_id\n" +
		"GROUP BY c.tier ORDER BY revenue DESC;"
	for _, c := range []struct {
		stmt    string
		tables  []string
		refused bool
	}{
		{readme, []string{"orders", "customers"}, false},
		{"SELECT ROUND(total, 2) FROM orders", []string{"orders"}, false},
		{"SELECT ROUND(weight, 2) FROM orders", []string{"orders"}, true},
		{"SELECT ROUND(AVG(score)) FROM customers", []string{"customers"}, true},
		// The same name is a DOUBLE in a table the statement reads.
		{"SELECT ROUND(SUM(total), 2) FROM orders JOIN readings ON r_id = id", []string{"orders", "readings"}, true},
		// A walk not certain of the tables asks about every table: readings too.
		{"SELECT ROUND(total, 2) FROM orders", nil, true},
		{"SELECT tier FROM customers", []string{"customers"}, false},
	} {
		narrowed := in
		if c.tables != nil {
			narrowed = only(c.tables...)
		}
		got := sqlRoundRefusalFor(narrowed, c.stmt, readrouter.ShapeOf(c.stmt))
		if (got != "") != c.refused {
			t.Errorf("%s: refusal %q, want refused %v", c.stmt, got, c.refused)
		}
		if got != "" && !strings.Contains(got, "ROUND") {
			t.Errorf("%s: refusal %q does not say why", c.stmt, got)
		}
	}
	// No reading of the statement: refused when it may hold a ROUND, not otherwise.
	if got := sqlRoundRefusalFor(in, "SELECT round(total) FROM orders", nil); got == "" {
		t.Error("no reading of a statement with ROUND, yet the copy answers")
	}
	if got := sqlRoundRefusalFor(in, "SELECT total FROM orders", nil); got != "" {
		t.Errorf("no ROUND in the statement: %q", got)
	}
	// sqlStrictRefusalFor asks it.
	stmt := "SELECT ROUND(weight) FROM orders"
	if got := sqlStrictRefusalFor(in, only("orders"), sqlsandbox.Refs{}, stmt, readrouter.ShapeOf(stmt)); !strings.Contains(got, "ROUND") {
		t.Errorf("sqlStrictRefusalFor does not ask about ROUND: %q", got)
	}
	// A table whose snapshot carries no table definition: its total may be
	// a DOUBLE, though orders' is exact. Not known is never exact.
	legacy := views.BaselineTable{Schema: "shop", Table: "legacy", Path: "/c/shop/legacy.parquet"}
	withLegacy := in
	withLegacy.Baselines = append(append([]views.BaselineTable{}, in.Baselines...), legacy)
	narrowed := withLegacy
	var refs sqlsandbox.Refs
	for _, n := range []string{"orders", "legacy"} {
		refs.Tables = append(refs.Tables, sqlsandbox.TableRef{Schema: "shop", Name: n})
	}
	narrowed.OnlyViews = sqlWantedViews(withLegacy, refs)
	stmt = "SELECT ROUND(l.total, 2) FROM orders o JOIN legacy l ON l.id = o.id"
	if got := sqlRoundRefusalFor(narrowed, stmt, readrouter.ShapeOf(stmt)); got == "" {
		t.Error("ROUND of a column of a table with no definition, yet the copy answers")
	}
	// A number written in the statement is exact whatever the tables are.
	stmt = "SELECT ROUND(2.5), l.id FROM orders o JOIN legacy l ON l.id = o.id"
	if got := sqlRoundRefusalFor(narrowed, stmt, readrouter.ShapeOf(stmt)); got != "" {
		t.Errorf("ROUND(2.5) beside a table with no definition: %q", got)
	}
}
