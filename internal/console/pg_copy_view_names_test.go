package console

import (
	"slices"
	"testing"

	"github.com/dbtrail/dbtrail/internal/views"
)

// The capture gate's list is the copy's views, not the snapshot's files: a
// table with no view there, or under another name, is not one the copy holds.
func TestPGViewNames(t *testing.T) {
	in := views.Input{Baselines: []views.BaselineTable{
		{Schema: "public", Table: "orders", Path: "/s/public/orders.parquet"},
		{Schema: "temp", Table: "t", Path: "/s/temp/t.parquet"},
		{Schema: "main", Table: "m", Path: "/s/main/m.parquet"},
		{Schema: "public", Table: "bad", Path: `/s/public/b\ad.parquet`},
		{Schema: "public", Table: "Twin", Path: "/s/public/Twin.parquet"},
		{Schema: "public", Table: "twin", Path: "/s/public/twin.parquet"},
	}}
	got := pgViewNames(in)
	t.Logf("views: %v", got)
	has := func(s, n string) bool { return slices.Contains(got, [2]string{s, n}) }
	if !has("public", "orders") {
		t.Error("a plain table is missing")
	}
	for _, n := range [][2]string{{"temp", "t"}, {"main", "m"}, {"public", "bad"}} {
		if has(n[0], n[1]) {
			t.Errorf("%v has no view on the copy, and is listed", n)
		}
	}
	if has("public", "Twin") && has("public", "twin") {
		t.Error("both twins are listed by their own names; the copy cannot hold both under them")
	}
}
