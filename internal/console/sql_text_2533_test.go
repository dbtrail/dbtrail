package console

import (
	"context"
	"database/sql"
	"slices"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/hiderules"
	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
	"github.com/dbtrail/dbtrail/internal/views"
)

// #2533: which text columns a routed statement holds to which values before
// the copy answers it, and which statements are MySQL's whatever the values.
func TestSQLTextCheck(t *testing.T) {
	text := func(name string, kind views.TextKind) views.TextColumn {
		return views.TextColumn{Name: name, Kind: kind}
	}
	people := views.BaselineTable{Schema: "shop", Table: "people", Path: "/c/shop/people.parquet", SchemaKnown: true,
		DistinctSafe: []string{"id"}, DistinctOther: []string{"name", "email", "code", "tag", "status", "born"},
		Text: []views.TextColumn{text("name", views.TextFolded), text("email", views.TextFolded),
			text("code", views.TextBytes), text("tag", views.TextUnlike),
			{Name: "status", Kind: views.TextFolded, Positional: true}}}
	legacy := views.BaselineTable{Schema: "shop", Table: "legacy", Path: "/c/shop/legacy.parquet", SchemaKnown: true,
		DistinctSafe: []string{"id"}, DistinctOther: []string{"ref", "secret"},
		Text: []views.TextColumn{text("ref", views.TextBytesASCII), text("secret", views.TextFolded)}}
	in := views.Input{Baselines: []views.BaselineTable{people, legacy}}
	only := func(names ...string) views.Input {
		var refs sqlsandbox.Refs
		for _, n := range names {
			refs.Tables = append(refs.Tables, sqlsandbox.TableRef{Schema: "shop", Name: n})
		}
		narrowed := in
		narrowed.OnlyViews = sqlWantedViews(in, refs)
		return narrowed
	}
	col := func(names ...string) []sqlsandbox.ColumnRef {
		var out []sqlsandbox.ColumnRef
		for _, n := range names {
			q, name, ok := strings.Cut(n, ".")
			if !ok {
				q, name = "", n
			}
			out = append(out, sqlsandbox.ColumnRef{Qualifier: q, Name: name})
		}
		return out
	}
	g := func(view, column, pattern string) string { return view + " " + column + " " + pattern }
	const (
		pv = `"shop"."people"`
		lv = `"shop"."legacy"`
	)
	cases := []struct {
		name    string
		tables  []string
		refs    sqlsandbox.Refs
		refused string // a word the refusal holds, "" for none
		guards  []string
	}{
		{"a shown text column: printable ASCII", []string{"people"},
			sqlsandbox.Refs{Columns: col("name", "id")}, "", []string{g(pv, "name", sqlTextPrintable)}},
		{"names fold case, qualifiers are not columns", []string{"people"},
			sqlsandbox.Refs{Columns: col("p.NAME")}, "", []string{g(pv, "name", sqlTextPrintable)}},
		{"no text column named: nothing to check", []string{"people"},
			sqlsandbox.Refs{Columns: col("id", "born")}, "", nil},
		{"a sorted text column: letters, digits and inner spaces", []string{"people"},
			sqlsandbox.Refs{Columns: col("name", "email"), Text: sqlsandbox.TextUse{Ordered: col("name"), Sorted: col("name")}}, "",
			[]string{g(pv, "name", sqlTextSortable), g(pv, "email", sqlTextPrintable)}},
		{"ordered by something unnamed: every named one sorts", []string{"people"},
			sqlsandbox.Refs{Columns: col("name", "email"), Text: sqlsandbox.TextUse{OrderedUnknown: true}}, "",
			[]string{g(pv, "name", sqlTextSortable), g(pv, "email", sqlTextSortable)}},
		{"ordered by a name that is no column here (an alias): every named one sorts", []string{"people"},
			sqlsandbox.Refs{Columns: col("name", "n"), Text: sqlsandbox.TextUse{Ordered: col("n"), Sorted: col("n"), Aliases: []string{"n"}}}, "",
			[]string{g(pv, "name", sqlTextSortable)}},
		{"ordered by an alias that shadows a column", []string{"people"},
			sqlsandbox.Refs{Columns: col("email", "name"), Text: sqlsandbox.TextUse{Ordered: col("name"), Sorted: col("name"), Aliases: []string{"NAME"}}}, "",
			[]string{g(pv, "name", sqlTextSortable), g(pv, "email", sqlTextSortable)}},
		{"a _bin column: control characters and a trailing space", []string{"people"},
			sqlsandbox.Refs{Columns: col("code"), Text: sqlsandbox.TextUse{Ordered: col("code")}}, "", []string{g(pv, "code", sqlTextPadded)}},
		{"a latin1_bin column: printable ASCII, sorted or not", []string{"legacy"},
			sqlsandbox.Refs{Columns: col("ref"), Text: sqlsandbox.TextUse{Ordered: col("ref")}}, "", []string{g(lv, "ref", sqlTextPrintable)}},
		{"a column named in two tables is checked in both", []string{"people", "legacy"},
			sqlsandbox.Refs{Columns: col("name", "secret")}, "", []string{g(lv, "secret", sqlTextPrintable), g(pv, "name", sqlTextPrintable)}},
		{"a _cs or language collation named: MySQL's", []string{"people"},
			sqlsandbox.Refs{Columns: col("tag")}, "tag", nil},
		{"a _cs collation not named: nothing", []string{"people"},
			sqlsandbox.Refs{Columns: col("name")}, "", []string{g(pv, "name", sqlTextPrintable)}},
		{"an ENUM in an ORDER BY: MySQL's", []string{"people"},
			sqlsandbox.Refs{Columns: col("status"), Text: sqlsandbox.TextUse{Ordered: col("status"), Sorted: col("status")}}, "position", nil},
		{"an ENUM ordered by an ordinal: MySQL's", []string{"people"},
			sqlsandbox.Refs{Columns: col("status"), Text: sqlsandbox.TextUse{OrderedUnknown: true}}, "position", nil},
		{"an ENUM in a MIN: MySQL compares its text", []string{"people"},
			sqlsandbox.Refs{Columns: col("status"), Text: sqlsandbox.TextUse{Ordered: col("status")}}, "", []string{g(pv, "status", sqlTextSortable)}},
		{"an ENUM compared for equality", []string{"people"},
			sqlsandbox.Refs{Columns: col("status")}, "", []string{g(pv, "status", sqlTextPrintable)}},
		{"a literal with an accent or a trailing space", []string{"people"},
			sqlsandbox.Refs{Columns: col("id"), Text: sqlsandbox.TextUse{OddLiteral: true}}, "string", nil},
		{"a sign put in order against a date: not text", []string{"people"},
			sqlsandbox.Refs{Columns: col("born"), Text: sqlsandbox.TextUse{Ordered: col("born"), OrderedLiterals: []sqlsandbox.OrderedLiteral{{Columns: col("born")}}}}, "", nil},
		{"a sign put in order against text: MySQL's", []string{"people"},
			sqlsandbox.Refs{Columns: col("name"), Text: sqlsandbox.TextUse{Ordered: col("name"), OrderedLiterals: []sqlsandbox.OrderedLiteral{{Columns: col("name")}}}}, "sorts", nil},
		{"a sign put in order against a name that is no column: MySQL's", []string{"people"},
			sqlsandbox.Refs{Columns: col("x"), Text: sqlsandbox.TextUse{Ordered: col("x"), OrderedLiterals: []sqlsandbox.OrderedLiteral{{Columns: col("x")}}}}, "sorts", nil},
		{"literals put in order against each other: MySQL's", []string{"people"},
			sqlsandbox.Refs{Text: sqlsandbox.TextUse{OrderedLiterals: []sqlsandbox.OrderedLiteral{{}}}}, "sorts", nil},
		{"a star brings its text columns in, compared or not (a subquery's star feeds an IN)", []string{"people"},
			sqlsandbox.Refs{Columns: col("id"), StarTables: []sqlsandbox.TableRef{{Schema: "shop", Name: "people"}}}, "tag", nil},
		{"a star over one table of two", []string{"people", "legacy"},
			sqlsandbox.Refs{Columns: col("id"), StarTables: []sqlsandbox.TableRef{{Schema: "shop", Name: "legacy"}}}, "",
			[]string{g(lv, "ref", sqlTextPrintable), g(lv, "secret", sqlTextPrintable)}},
		{"a sign put in order inside an expression: MySQL's", []string{"people"},
			sqlsandbox.Refs{Columns: col("born"), Text: sqlsandbox.TextUse{Ordered: col("born"), OrderedLiterals: []sqlsandbox.OrderedLiteral{{Columns: col("born"), Expression: true}}}}, "sorts", nil},
		{"sorted by an ordinal with a sign in some string: MySQL's", []string{"people"},
			sqlsandbox.Refs{Columns: col("id"), Text: sqlsandbox.TextUse{OrderedUnknown: true, LooseLiteral: true}}, "sorts", nil},
		{"sorted by an alias with a sign in some string: MySQL's", []string{"people"},
			sqlsandbox.Refs{Columns: col("id", "k"), Text: sqlsandbox.TextUse{Ordered: col("k"), Sorted: col("k"), Aliases: []string{"k"}, LooseLiteral: true}}, "sorts", nil},
		{"a sign in a string, sorted by a column: only that column's values decide", []string{"people"},
			sqlsandbox.Refs{Columns: col("email"), Text: sqlsandbox.TextUse{Ordered: col("email"), Sorted: col("email"), LooseLiteral: true}}, "",
			[]string{g(pv, "email", sqlTextSortable)}},
		{"GREATEST over text: by bytes on the copy", []string{"people"},
			sqlsandbox.Refs{Columns: col("name"), Text: sqlsandbox.TextUse{Ordered: col("name"), ByBytes: col("name")}}, "case", nil},
		{"a row compared with a string in it", []string{"people"},
			sqlsandbox.Refs{Columns: col("id"), Text: sqlsandbox.TextUse{ByBytes: col("id"), ByBytesLiteral: true}}, "case", nil},
		{"a row of numbers and dates", []string{"people"},
			sqlsandbox.Refs{Columns: col("id", "born"), Text: sqlsandbox.TextUse{ByBytes: col("id", "born")}}, "", nil},
		{"a row with a name that is no column here", []string{"people"},
			sqlsandbox.Refs{Columns: col("q"), Text: sqlsandbox.TextUse{ByBytes: col("q")}}, "case", nil},
		{"a star under DISTINCT compares every column it brings", []string{"people"},
			sqlsandbox.Refs{StarTables: []sqlsandbox.TableRef{{Schema: "shop", Name: "people"}}, Text: sqlsandbox.TextUse{RowsCompared: true}}, "tag", nil},
		{"a star the walk could not place, over a set operation: every table read", []string{"legacy"},
			sqlsandbox.Refs{Star: true, Text: sqlsandbox.TextUse{RowsCompared: true}}, "",
			[]string{g(lv, "ref", sqlTextPrintable), g(lv, "secret", sqlTextPrintable)}},
		{"a NATURAL or USING join compares columns it does not name", []string{"legacy"},
			sqlsandbox.Refs{NamedJoin: true}, "", []string{g(lv, "ref", sqlTextPrintable), g(lv, "secret", sqlTextPrintable)}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			refused, guards := sqlTextCheck(only(c.tables...), c.refs)
			if (refused != "") != (c.refused != "") || !strings.Contains(refused, c.refused) {
				t.Errorf("refusal %q, want one holding %q", refused, c.refused)
			}
			var got []string
			for _, gd := range guards {
				got = append(got, g(gd.view, gd.column, gd.pattern))
			}
			if !slices.Equal(got, c.guards) {
				t.Errorf("guards\n  got  %q\n  want %q", got, c.guards)
			}
		})
	}
}

// A column a hidden rule takes out of its view (#2529) is not there to
// check: the statement fails on its name, or another table's column of that
// name answers, which sqlHiddenNameRefusal has decided already.
func TestSQLTextCheck_hiddenColumn(t *testing.T) {
	set, err := hiderules.Parse(hiderules.Rules{Columns: []string{"shop.people.email"}})
	if err != nil {
		t.Fatal(err)
	}
	in := views.Input{Hidden: set, Baselines: []views.BaselineTable{{Schema: "shop", Table: "people", Path: "/c/p.parquet", SchemaKnown: true,
		DistinctOther: []string{"name"}, Text: []views.TextColumn{{Name: "name", Kind: views.TextFolded}, {Name: "email", Kind: views.TextFolded}}}}}
	_, guards := sqlTextCheck(in, sqlsandbox.Refs{Columns: []sqlsandbox.ColumnRef{{Name: "name"}, {Name: "email"}}})
	if len(guards) != 1 || guards[0].column != "name" {
		t.Errorf("guards = %+v, want name alone", guards)
	}
}

// The patterns, run where they run: DuckDB's regexp_matches over the text the
// copy holds. A match is a value the copy would not compare as MySQL does.
func TestSQLTextPatterns(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	matches := func(pattern, value string) bool {
		t.Helper()
		var m bool
		if err := db.QueryRowContext(context.Background(), "SELECT regexp_matches(?, ?)", value, pattern).Scan(&m); err != nil {
			t.Fatal(err)
		}
		return m
	}
	for _, c := range []struct {
		pattern, value string
		want           bool
	}{
		{sqlTextPrintable, "Jose Maria", false},
		{sqlTextPrintable, "a_b-c.d@e{f}~", false},
		{sqlTextPrintable, "", false},
		{sqlTextPrintable, "José", true},
		{sqlTextPrintable, "Ｊose", true},
		{sqlTextPrintable, "Jose ", true},
		{sqlTextPrintable, "a\tb", true},
		{sqlTextPrintable, "a\nb", true},
		{sqlTextSortable, "Jose Maria 2", false},
		{sqlTextSortable, " lead", false},
		{sqlTextSortable, "a_b", true},
		{sqlTextSortable, "a.b", true},
		{sqlTextSortable, "José", true},
		{sqlTextSortable, "end ", true},
		{sqlTextPadded, "José_ü.€", false},
		{sqlTextPadded, "a ", true},
		{sqlTextPadded, "a\tb", true},
		{sqlTextPadded, "a\x00", true},
	} {
		if got := matches(c.pattern, c.value); got != c.want {
			t.Errorf("regexp_matches(%q, %q) = %v, want %v", c.value, c.pattern, got, c.want)
		}
	}
}

// The check as the script ends with it: one statement per view, over the
// view's own name, each column cast to text.
func TestSQLTextDeclineSQL(t *testing.T) {
	got := sqlTextDeclineSQL([]sqlTextGuard{
		{view: `"shop"."people"`, label: "shop.people", column: "name", pattern: sqlTextSortable},
		{view: `"shop"."people"`, label: "shop.people", column: `we"ird`, pattern: sqlTextPrintable},
		{view: `"shop"."legacy"`, label: "shop.legacy", column: "ref", pattern: sqlTextPrintable},
	})
	if n := strings.Count(got, "SELECT CASE WHEN"); n != 2 {
		t.Errorf("%d statements, want one per view:\n%s", n, got)
	}
	for _, want := range []string{
		`FROM "shop"."people" WHERE regexp_matches(CAST("name" AS VARCHAR), '` + sqlTextSortable + `') OR regexp_matches(CAST("we""ird" AS VARCHAR), '`,
		"shop.people (name, we\"ird)",
		`FROM "shop"."legacy" WHERE regexp_matches(CAST("ref" AS VARCHAR)`,
	} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %q in:\n%s", want, got)
		}
	}
	if sqlTextDeclineSQL(nil) != "" {
		t.Error("no guard should add nothing")
	}
}

// A table whose definition was not read has no known text columns, which is
// not "no text": the check refuses it itself, not only behind
// sqlNamesRefusalFor.
func TestSQLTextCheck_schemaUnknown(t *testing.T) {
	in := views.Input{Baselines: []views.BaselineTable{{Schema: "shop", Table: "old", Path: "/c/o.parquet"}}}
	if why, _ := sqlTextCheck(in, sqlsandbox.Refs{Columns: []sqlsandbox.ColumnRef{{Name: "id"}}}); !strings.Contains(why, "shop.old") {
		t.Errorf("refusal %q, want one naming shop.old", why)
	}
}
