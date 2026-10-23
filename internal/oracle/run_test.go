package oracle

import (
	"strings"
	"testing"
)

// TestSourceFor: a table with a tail is compared with the cut; one the
// port answers as of the published cut with the rows read there, and the
// context says which cut; one answered as of a published cut that no step
// published is an error, not a pass against nothing.
func TestSourceFor(t *testing.T) {
	cut := &Cut{Step: 5, Rows: map[string]TableRows{"t_int": rowsOf("t_int", []string{"1", "now", ""})}}
	pub := map[string]published{"t_int": {step: 3, rows: rowsOf("t_int", []string{"1", "then", ""})}}
	cx := Context{Seed: 1, Step: 5}

	got, gcx, err := sourceFor(cx, "t_int", cut, Read{}, pub)
	if err != nil || got.Rows[0][1] != "now" || gcx.AsOf != "" {
		t.Fatalf("with a tail: rows=%v asOf=%q err=%v", got.Rows, gcx.AsOf, err)
	}
	got, gcx, err = sourceFor(cx, "t_int", cut, Read{AsPublished: map[string]bool{"t_int": true}}, pub)
	if err != nil || got.Rows[0][1] != "then" || gcx.AsOf != "the published cut (step 3)" {
		t.Fatalf("as published: rows=%v asOf=%q err=%v", got.Rows, gcx.AsOf, err)
	}
	if _, _, err = sourceFor(cx, "t_int", cut, Read{AsPublished: map[string]bool{"t_int": true}}, nil); err == nil || !strings.Contains(err.Error(), "no step published one") {
		t.Fatalf("as published with nothing published: err=%v", err)
	}
}
