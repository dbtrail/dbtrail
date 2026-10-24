package console

import (
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/readrouter"
	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
	"github.com/dbtrail/dbtrail/internal/views"
)

// #2532: an aggregate over DISTINCT numbers or dates is the copy's to
// answer; over text, or a column whose type is not known, MySQL's.
func TestSQLDistinctRefusalFor(t *testing.T) {
	table := func(name string, safe, other []string) views.BaselineTable {
		return views.BaselineTable{Schema: "shop", Table: name, Path: "/c/shop/" + name + ".parquet", SchemaKnown: true,
			Columns: append(append([]string{}, safe...), other...), DistinctSafe: safe, DistinctOther: other}
	}
	in := views.Input{Baselines: []views.BaselineTable{
		table("order_line", []string{"ol_w_id", "ol_i_id", "ol_amount"}, []string{"ol_dist_info"}),
		table("item", []string{"i_id", "i_price"}, []string{"i_name", "i_data"}),
		table("notes", []string{"n_id"}, []string{"ol_w_id"}), // ol_w_id is text here
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
	for _, c := range []struct {
		stmt    string
		tables  []string
		refused bool
	}{
		{"SELECT COUNT(DISTINCT ol_w_id) FROM order_line", []string{"order_line"}, false},
		{"SELECT i.i_id, COUNT(DISTINCT ol.ol_w_id) FROM order_line ol JOIN item i ON i.i_id = ol.ol_i_id GROUP BY i.i_id", []string{"order_line", "item"}, false},
		{"SELECT COUNT(DISTINCT i_name) FROM item", []string{"item"}, true},
		{"SELECT COUNT(DISTINCT ol_dist_info) FROM order_line", []string{"order_line"}, true},
		// The same name is text in a table the statement reads.
		{"SELECT COUNT(DISTINCT ol_w_id) FROM order_line JOIN notes ON n_id = ol_i_id", []string{"order_line", "notes"}, true},
		// A walk not certain of the tables asks about every table: notes too.
		{"SELECT COUNT(DISTINCT ol_w_id) FROM order_line", nil, true},
		{"SELECT i_name FROM item", []string{"item"}, false},
	} {
		narrowed := in
		if c.tables != nil {
			narrowed = only(c.tables...)
		}
		got := sqlDistinctRefusalFor(narrowed, c.stmt, readrouter.ShapeOf(c.stmt))
		if (got != "") != c.refused {
			t.Errorf("%s: refusal %q, want refused %v", c.stmt, got, c.refused)
		}
		if got != "" && !strings.Contains(got, "DISTINCT") {
			t.Errorf("%s: refusal %q does not say why", c.stmt, got)
		}
	}
	// No reading of the statement: refused when it may hold one, not otherwise.
	if got := sqlDistinctRefusalFor(in, "SELECT count(distinct ol_w_id) FROM order_line", nil); got == "" {
		t.Error("no reading of a statement with DISTINCT, yet the copy answers")
	}
	if got := sqlDistinctRefusalFor(in, "SELECT ol_w_id FROM order_line", nil); got != "" {
		t.Errorf("no DISTINCT in the statement: %q", got)
	}
	// sqlStrictRefusalFor asks it.
	stmt := "SELECT COUNT(DISTINCT i_name) FROM item"
	if got := sqlStrictRefusalFor(in, only("item"), sqlsandbox.Refs{}, stmt, readrouter.ShapeOf(stmt)); !strings.Contains(got, "DISTINCT") {
		t.Errorf("sqlStrictRefusalFor does not ask about DISTINCT: %q", got)
	}
}
