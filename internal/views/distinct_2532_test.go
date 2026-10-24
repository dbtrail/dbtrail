package views

import (
	"slices"
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

func TestApplyFooters_distinct(t *testing.T) {
	in := Input{Baselines: []BaselineTable{{Schema: "s", Table: "t", Path: "/p/t.parquet"}}}
	in.ApplyFooters(map[string]baseline.TableFooter{"/p/t.parquet": {DistinctSafe: []string{"id"}, DistinctOther: []string{"name"}}})
	if b := in.Baselines[0]; !slices.Equal(b.DistinctSafe, []string{"id"}) || !slices.Equal(b.DistinctOther, []string{"name"}) {
		t.Errorf("DistinctSafe %v, DistinctOther %v", b.DistinctSafe, b.DistinctOther)
	}
}
