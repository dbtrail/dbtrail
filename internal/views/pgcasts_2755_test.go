package views

import (
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

func pgTable() BaselineTable {
	t := BaselineTable{Schema: "public", Table: "orders", Path: "/snap/public/orders.parquet", Rel: "public/orders.parquet"}
	in := Input{Baselines: []BaselineTable{t}}
	in.ApplyFooters(map[string]baseline.TableFooter{t.Path: {
		Columns: []string{"id", "note", "amount", "free", "payload", "labels", "it's"},
		PG: []baseline.PGCast{
			{Name: "id", As: "BIGINT"}, {Name: "note", As: baseline.PGAsText}, {Name: "amount", As: "DECIMAL(12,2)"},
			{Name: "free", As: baseline.PGAsNumeric}, {Name: "payload", As: baseline.PGAsBytes}, {Name: "labels"},
			{Name: "it's", As: baseline.PGAsNumeric},
		},
	}})
	return in.Baselines[0]
}

// A PostgreSQL table's pinned view lists its columns in the table's order,
// each read as its type (#2755).
func TestPGSelectList(t *testing.T) {
	tbl := pgTable()
	if tbl.SchemaKnown {
		t.Fatal("a PostgreSQL table set SchemaKnown, which every MySQL rule reads")
	}
	got := selectList(tbl)
	for _, want := range []string{
		`CAST("id" AS BIGINT) AS "id", "note" COLLATE C AS "note", CAST("amount" AS DECIMAL(12,2)) AS "amount", CASE WHEN strpos("free", '.') > 0 AND length("free") - strpos("free", '.') > 18 THEN error('public.orders: column free holds a numeric with more than 18 decimals, which the copy does not read exactly') ELSE CAST("free" AS DECIMAL(38,18)) END AS "free"`,
		`unhex(substr("payload", 3)) AS "payload", "labels", CASE`,
		// A quote in a column name is doubled in the message's literal.
		`error('public.orders: column it''s holds`,
	} {
		if !strings.Contains(got, want) {
			t.Errorf("select list lacks %s\n got: %s", want, got)
		}
	}
	if notes := decimalComments(tbl, true); len(notes) != 1 || !strings.HasPrefix(notes[0], "labels (left as text") {
		t.Errorf("notes = %q, want the one column left as text", notes)
	}
}

// The whole script: the note above the views, the list in the view, and no
// "carries no column types" line for a table that has them.
func TestPGViewsScript(t *testing.T) {
	in := SnapshotScopedInput(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC), []BaselineTable{pgTable()})
	in.Baselines[0] = pgTable()
	out := Generate(in)
	for _, want := range []string{
		"-- A PostgreSQL source's snapshot files hold every value as the text PostgreSQL",
		`SELECT CAST("id" AS BIGINT) AS "id", "note" COLLATE C AS "note"`,
		"labels (left as text",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("script lacks %q\n%s", want, out)
		}
	}
	for _, not := range []string{"carries no column types", "Some files carry no column types"} {
		if strings.Contains(out, not) {
			t.Errorf("script says %q about a table whose types are read\n%s", not, out)
		}
	}
}

// A file that follows later snapshots keeps the casts and drops the column
// list and the text collation, as it does for a MySQL table.
func TestPGFollowingViewKeepsCasts(t *testing.T) {
	casts := withoutPGCollation(pgTable().PGCasts)
	tbl := pgTable()
	tbl.PGCasts, tbl.Columns = casts, nil
	if got := selectList(tbl); got != "" {
		t.Errorf("select list = %q, want none without a column order", got)
	}
	got := replaceClause(tbl)
	if !strings.Contains(got, `CAST("id" AS BIGINT) AS "id"`) || strings.Contains(got, "COLLATE") {
		t.Errorf("replace list = %q, want the casts without the collation", got)
	}
	if notes := decimalComments(tbl, false); len(notes) != 1 || !strings.HasPrefix(notes[0], "labels (left as text") {
		t.Errorf("notes = %q: a text column without its collation is not a column left for want of a type", notes)
	}
	if pgTable().PGCasts[1].As != baseline.PGAsText {
		t.Error("withoutPGCollation changed the table it was given")
	}
}
