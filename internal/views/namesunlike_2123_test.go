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
		// Letters MySQL folds onto ASCII ones when it compares names.
		{"a Kelvin sign for k", with("mark"), "SELECT marK FROM gen", "mark"},
		{"a long s for s", with("st"), "SELECT ſt FROM gen", "st"},
		{"a dotless i for i", with("twice"), "SELECT twıce FROM gen", "twice"},
		{"a dotted capital I for i", with("twice"), "SELECT twİce FROM gen", "twice"},
		// A name that is not plain ASCII letters, digits, _ and $ could be
		// written in the statement in a way a search does not find.
		{"a name with a letter outside ASCII", with("año"), "SELECT id FROM gen", "año"},
		{"a name with a space", with("two words"), "SELECT id FROM gen", "two words"},
		{"a name with a quote", with(`tw"ice`), "SELECT id FROM gen", `tw"ice`},
		{"an empty name", with(""), "SELECT id FROM gen", "cannot be looked for"},
		{"a name with a dollar and a digit is plain", with("t$1"), "SELECT id FROM gen", ""},
		{"a name with a dollar and a digit, named", with("t$1"), "SELECT T$1 FROM gen", "t$1"},
		{"an empty statement names nothing", with("twice"), "", ""},
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
