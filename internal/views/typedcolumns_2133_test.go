package views

import (
	"reflect"
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// #2133: a table's date, time and year columns reach the view input with the
// rest of its footer, and are sorted for the question read routing asks. A
// column of a type that is not known goes with the dates.
func TestApplyFooters_temporal(t *testing.T) {
	in := Input{Baselines: []BaselineTable{{Schema: "shop", Table: "ev", Path: "/x/ev.parquet"}, {Schema: "shop", Table: "plain", Path: "/x/plain.parquet"}}}
	in.ApplyFooters(map[string]baseline.TableFooter{
		"/x/ev.parquet": {Columns: []string{"id"}, Temporal: []baseline.TemporalColumn{
			{Name: "created_on", Type: "date"}, {Name: "tm", Type: "time"}, {Name: "seen", Type: "datetime"},
			{Name: "yr", Type: "year"}, {Name: "ts", Type: "timestamp"}, {Name: "odd"}}},
		"/x/plain.parquet": {Columns: []string{"id"}},
	})
	dates, whole := in.Baselines[0].TypedColumns()
	if want := []string{"created_on", "seen", "ts", "odd"}; !reflect.DeepEqual(dates, want) {
		t.Errorf("dates = %q, want %q", dates, want)
	}
	if want := []string{"tm", "yr"}; !reflect.DeepEqual(whole, want) {
		t.Errorf("whole = %q, want %q", whole, want)
	}
	if dates, whole := in.Baselines[1].TypedColumns(); dates != nil || whole != nil {
		t.Errorf("a table with none: %q, %q", dates, whole)
	}
}
