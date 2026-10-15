package views

import (
	"strings"
	"testing"
)

// #2369: a PINNED state view lays the table's tail over its published state;
// a following one never does (the file is one statement's moment).
func TestStateView_tail(t *testing.T) {
	tbl := BaselineTable{Schema: "shop", Table: "orders", Path: "/snap/2026-10-14T10-00-00Z/shop/orders.parquet",
		Rel: "shop/orders.parquet", Tail: &TableTail{Path: "/tails/1-shop.or*ders.parquet", PK: []string{"id"}}}
	pinned := Generate(Input{Baselines: []BaselineTable{tbl}})
	for _, want := range []string{"ANTI JOIN", `"id"`, `1-shop.or[*]ders.parquet`} {
		if !strings.Contains(pinned, want) {
			t.Errorf("the pinned view lacks %q:\n%s", want, pinned)
		}
	}
	following := Generate(Input{Baselines: []BaselineTable{tbl}, Follow: FollowNewest})
	if strings.Contains(following, "1-shop.or") {
		t.Errorf("a following view reads a tail:\n%s", following)
	}
}
