package console

import (
	"errors"
	"testing"
	"time"

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

func TestPGStampMemo(t *testing.T) {
	var m pgStampMemo
	t0 := time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC)
	reads := 0
	read := func(v string, err error) func() (string, error) {
		return func() (string, error) { reads++; return v, err }
	}

	// Nothing kept: read.
	if v, _ := m.get(t0, read("a", nil)); v != "a" || reads != 1 {
		t.Fatalf("first: %q after %d reads", v, reads)
	}
	// What a read keeps (PGCopyStampFresh does put) is shared...
	m.put(t0, "a")
	if v, _ := m.get(t0.Add(pgStampFor-time.Millisecond), read("b", nil)); v != "a" || reads != 1 {
		t.Fatalf("within pgStampFor: %q after %d reads", v, reads)
	}
	// ...and read again once it is pgStampFor old.
	if v, _ := m.get(t0.Add(pgStampFor), read("b", nil)); v != "b" || reads != 2 {
		t.Fatalf("after pgStampFor: %q after %d reads", v, reads)
	}
	// An error is passed on and not kept.
	if _, err := m.get(t0.Add(time.Hour), read("", errors.New("listing failed"))); err == nil {
		t.Fatal("the error was lost")
	}
	// A stamp read earlier does not replace one read later.
	m.put(t0.Add(2*time.Second), "new")
	m.put(t0.Add(time.Second), "old")
	if v, _ := m.get(t0.Add(2*time.Second), read("x", nil)); v != "new" {
		t.Fatalf("an older stamp replaced a newer one: %q", v)
	}
	// A clock that went back is not "within pgStampFor".
	if v, _ := m.get(t0, read("y", nil)); v != "y" {
		t.Fatalf("clock back: %q", v)
	}
}
