package views

import (
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// #2123: a statement that names a column the copy does not hold is not the
// copy's to answer, and neither is any statement over a table whose missing
// columns are not known by name.
func TestNamesUnlikeMySQL(t *testing.T) {
	known := BaselineTable{Schema: "shop", Table: "gen", SchemaKnown: true, Columns: []string{"id", "a"}}
	with := func(names ...string) BaselineTable {
		t := known
		t.NotHeld = names
		return t
	}
	cases := []struct {
		name  string
		table BaselineTable
		stmt  string
		want  string // "" = the copy may answer; else a fragment of the reason
	}{
		{"nothing missing", known, "SELECT twice FROM gen", ""},
		{"the statement does not name it", with("twice"), "SELECT id, a FROM gen WHERE a > 1 ORDER BY id", ""},
		{"named bare", with("twice"), "SELECT twice FROM gen", "twice"},
		{"named in another case", with("twice"), "select TWICE from gen", "twice"},
		{"the definition in another case", with("Twice"), "select twice from gen", "Twice"},
		{"named in backticks", with("twice"), "SELECT `twice` FROM gen", "twice"},
		{"named in double quotes, as the port rewrites backticks", with("twice"), `SELECT "twice" FROM gen`, "twice"},
		{"named with its table", with("twice"), "SELECT g.twice FROM gen g", "twice"},
		{"named only in WHERE", with("twice"), "SELECT id FROM gen WHERE twice = 2", "twice"},
		{"named only in ORDER BY", with("twice"), "SELECT id FROM gen ORDER BY twice", "twice"},
		{"named only in GROUP BY and HAVING", with("twice"), "SELECT count(*) FROM gen GROUP BY twice HAVING twice > 1", "twice"},
		{"named only in a window", with("twice"), "SELECT row_number() OVER (ORDER BY twice) FROM gen", "twice"},
		{"named only in USING", with("twice"), "SELECT g.id FROM gen g JOIN g2 USING (twice)", "twice"},
		{"named as an alias", with("twice"), "SELECT a AS twice FROM gen", "twice"},
		{"named on another line", with("twice"), "SELECT id\nFROM gen\nWHERE\n\ttwice = 2", "twice"},
		{"in a prepared statement", with("twice"), "SELECT id FROM gen WHERE twice = ?", "twice"},
		{"the second of two", with("v", "twice"), "SELECT twice FROM gen", "twice"},
		// The text is searched, not parsed: a longer word, a string and a
		// comment that hold the name keep the statement on the source too.
		{"inside a longer word", with("twice"), "SELECT twice_as_much FROM gen", "twice"},
		{"inside a string", with("twice"), "SELECT id FROM gen WHERE a = 'twice'", "twice"},
		{"inside a comment", with("twice"), "SELECT id /* not twice */ FROM gen", "twice"},
		// A statement with any character outside ASCII is not searched at
		// all: which letters a server folds onto ASCII ones when it compares
		// names depends on the server and its version.
		{"a Kelvin sign for k", with("mark"), "SELECT marK FROM gen", "outside ASCII"},
		{"a long s for s", with("st"), "SELECT ſt FROM gen", "outside ASCII"},
		{"a dotless i for i", with("twice"), "SELECT twıce FROM gen", "outside ASCII"},
		{"a dotted capital I for i", with("twice"), "SELECT twİce FROM gen", "outside ASCII"},
		{"an accented letter, precomposed", with("twice"), "SELECT a AS twíce FROM gen WHERE twíce = 2", "outside ASCII"},
		{"an accent as a combining mark", with("twice"), "SELECT a AS twíce FROM gen WHERE twíce = 2", "outside ASCII"},
		{"the reason names the columns", with("v", "twice"), "SELECT twíce FROM gen", "v, twice"},
		// The cost: a string outside ASCII keeps the statement on the source.
		{"a string outside ASCII", with("twice"), "SELECT id FROM gen WHERE a = 'señor'", "outside ASCII"},
		{"a byte that is not UTF-8", with("twice"), "SELECT id FROM gen WHERE a = '\xff'", "outside ASCII"},
		// Only for a table that lacks a column: nothing to mistake otherwise.
		{"outside ASCII over a table that lacks nothing", known, "SELECT twíce, 'señor' FROM gen", ""},
		// A name that is not plain ASCII letters, digits, _ and $ could be
		// written in the statement in a way a search does not find.
		{"a name with a letter outside ASCII", with("año"), "SELECT id FROM gen", "año"},
		{"a name with a space", with("two words"), "SELECT id FROM gen", "two words"},
		{"a name with a quote", with(`tw"ice`), "SELECT id FROM gen", `tw"ice`},
		{"an empty name", with(""), "SELECT id FROM gen", "cannot be looked for"},
		{"a name with a dollar and a digit is plain", with("t$1"), "SELECT id FROM gen", ""},
		{"a name with a dollar and a digit, named", with("t$1"), "SELECT T$1 FROM gen", "t$1"},
		{"an empty statement names nothing", with("twice"), "", ""},
		// _rowid is MySQL's other name for a single integer key column. No
		// definition lists it and no snapshot holds it, for any table.
		{"_rowid, over a table that lacks nothing else", known, "SELECT a AS _rowid FROM gen WHERE _rowid = 1", "_rowid"},
		{"_rowid in another case", known, "SELECT _RowID FROM gen", "_rowid"},
		{"_rowid with a dotless i", known, "SELECT _rowıd FROM gen", "_rowid"},
		{"_rowid over a table with a generated column", with("twice"), "SELECT _rowid FROM gen", "_rowid"},
		{"rowid without the underscore is a name like any other", known, "SELECT rowid, row_id FROM gen", ""},
		// my_row_id is the key MySQL generates for a table created without
		// one. A server told not to show it lists it in no definition, and a
		// dump of it holds no such column.
		{"my_row_id, which the table does not list", known, "SELECT a AS my_row_id FROM gen WHERE my_row_id = 3", "my_row_id"},
		{"my_row_id in another case", known, "SELECT MY_ROW_ID FROM gen", "my_row_id"},
		{"my_row_id with a dotless i", known, "SELECT my_row_ıd FROM gen", "my_row_id"},
		{"my_row_id, which the table lists and the file holds", func() BaselineTable {
			t := known
			t.Columns = []string{"My_Row_ID", "a"}
			return t
		}(), "SELECT a AS my_row_id FROM gen WHERE my_row_id = 3", ""},
		// Not known is not "none".
		{"no table definition", BaselineTable{Schema: "shop", Table: "old"}, "SELECT id FROM old", "no table definition"},
		{"no table definition, though a list is set", BaselineTable{Schema: "shop", Table: "old", Columns: []string{"id"}}, "SELECT id FROM old", "no table definition"},
		{"the file's columns were not checked", BaselineTable{Schema: "shop", Table: "t", SchemaKnown: true}, "SELECT id FROM t", "could not be checked"},
		{"a definition that could not be read", func() BaselineTable { t := known; t.NotHeldUnread = true; return t }(), "SELECT id FROM gen", "could not be read"},
		{"unread wins over a name that is not in the statement", func() BaselineTable { t := with("twice"); t.NotHeldUnread = true; return t }(), "SELECT id FROM gen", "could not be read"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := c.table.NamesUnlikeMySQL(c.stmt)
			if c.want == "" {
				if got != "" {
					t.Errorf("NamesUnlikeMySQL(%q) = %q, want the copy to answer", c.stmt, got)
				}
				return
			}
			if !strings.Contains(got, c.want) {
				t.Errorf("NamesUnlikeMySQL(%q) = %q, want a reason holding %q", c.stmt, got, c.want)
			}
		})
	}
}

// Two tables whose names differ only by letter case (a source that tells them
// apart) share one name on the copy: one keeps it, the other's view is
// renamed. A statement that names either is then read against the one that
// kept the name, whichever the source would read.
func TestSelectedCaseTwin(t *testing.T) {
	in := Input{Baselines: []BaselineTable{
		{Schema: "shop", Table: "gen", Path: "/c/shop/gen.parquet"},
		{Schema: "shop", Table: "Gen", Path: "/c/shop/Gen.parquet"},
		{Schema: "shop", Table: "lines", Path: "/c/shop/lines.parquet"},
		{Schema: "Shop", Table: "other", Path: "/c/Shop/other.parquet"},
		{Schema: "shop", Table: "other", Path: "/c/shop/other.parquet"},
	}}
	keys := map[string]string{}
	for _, n := range in.ViewNames() {
		keys[n.Schema+"."+n.View] = n.Key
	}
	only := func(views ...string) Input {
		out := in
		out.OnlyViews = ViewSet{}
		for _, v := range views {
			k, ok := keys[v]
			if !ok {
				t.Fatalf("no view %s among %v", v, keys)
			}
			out.OnlyViews[k] = true
		}
		return out
	}
	for _, c := range []struct {
		name string
		in   Input
		want []string // both spellings, or none
	}{
		{"the table that kept the name", only("shop.gen"), []string{"shop.gen", "shop.Gen"}},
		{"a table with no twin", only("shop.lines"), nil},
		{"a twin by its schema's case", only("shop.other"), []string{"shop.other", "Shop.other"}},
		{"every view", in, []string{"shop.other", "Shop.other"}}, // the first in the plan's order
		{"no view", only(), nil},
	} {
		table, twin := c.in.SelectedCaseTwin()
		if len(c.want) == 0 {
			if table != "" || twin != "" {
				t.Errorf("%s: twin %q / %q, want none", c.name, table, twin)
			}
			continue
		}
		got := map[string]bool{table: true, twin: true}
		if !got[c.want[0]] || !got[c.want[1]] {
			t.Errorf("%s: twin %q / %q, want %v", c.name, table, twin, c.want)
		}
	}
}

// ApplyFooters carries the names from the footer read.
func TestApplyFooters_notHeld(t *testing.T) {
	in := Input{Baselines: []BaselineTable{{Schema: "s", Table: "gen", Path: "/x/gen.parquet"}, {Schema: "s", Table: "old", Path: "/x/old.parquet"}}}
	in.ApplyFooters(map[string]baseline.TableFooter{"/x/gen.parquet": {Columns: []string{"id"}, NotHeld: []string{"twice"}, NotHeldUnread: true}})
	if got := in.Baselines[0]; len(got.NotHeld) != 1 || got.NotHeld[0] != "twice" || !got.NotHeldUnread {
		t.Errorf("gen: NotHeld = %q, unread %v", got.NotHeld, got.NotHeldUnread)
	}
	if in.Baselines[1].NamesUnlikeMySQL("SELECT 1") == "" {
		t.Error("a table with no footer must not be one with nothing missing")
	}
}
