package console

import (
	"testing"

	"github.com/dbtrail/dbtrail/internal/views"
)

func TestPGCopyStamp(t *testing.T) {
	base := func() views.Input {
		return views.Input{Baselines: []views.BaselineTable{
			{Schema: "public", Table: "orders", Path: "/s/2026-11-24T10/public/orders.parquet"},
			{Schema: "public", Table: "items", Path: "/s/2026-11-24T10/public/items.parquet", HiddenColumns: []string{"price"}},
		}}
	}
	want := pgCopyStamp(base())
	if want == "" {
		t.Fatal("empty stamp")
	}

	reordered := base()
	reordered.Baselines[0], reordered.Baselines[1] = reordered.Baselines[1], reordered.Baselines[0]
	if got := pgCopyStamp(reordered); got != want {
		t.Error("the order of the tables changed the stamp")
	}

	cases := map[string]func(*views.Input){
		"new snapshot of one table": func(in *views.Input) { in.Baselines[0].Path = "/s/2026-11-24T11/public/orders.parquet" },
		"a column hidden":           func(in *views.Input) { in.Baselines[0].HiddenColumns = []string{"amount"} },
		"a column no longer hidden": func(in *views.Input) { in.Baselines[1].HiddenColumns = nil },
		"a table added": func(in *views.Input) {
			in.Baselines = append(in.Baselines, views.BaselineTable{Schema: "public", Table: "x", Path: "/s/x.parquet"})
		},
		"a table gone": func(in *views.Input) { in.Baselines = in.Baselines[:1] },
		// Names that run together into the same text.
		"name boundary": func(in *views.Input) { in.Baselines[0].Schema, in.Baselines[0].Table = "publico", "rders" },
		"hidden boundary": func(in *views.Input) {
			in.Baselines[1].HiddenColumns = []string{"pri", "ce"}
		},
	}
	for name, change := range cases {
		in := base()
		change(&in)
		if pgCopyStamp(in) == want {
			t.Errorf("%s: same stamp", name)
		}
	}
	if pgCopyStamp(views.Input{}) == want {
		t.Error("no tables: same stamp")
	}
}
