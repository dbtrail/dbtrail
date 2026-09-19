package cli

import (
	"strings"
	"testing"
)

// The line names the fullest databases with their estimates and stops: a
// server with many index databases must still produce something readable in
// the middle of an incident.
func TestDescribeSiblings(t *testing.T) {
	many := make([]siblingIndex, 8)
	for i := range many {
		many[i] = siblingIndex{Name: "bintrail_idx_" + string(rune('a'+i)), Rows: int64(100 - i)}
	}
	got := describeSiblings(many)
	if !strings.Contains(got, "and 3 more") {
		t.Errorf("eight siblings did not fold into a countable remainder: %s", got)
	}
	if strings.Contains(got, "bintrail_idx_f") {
		t.Errorf("more than five names were spelled out: %s", got)
	}
	if !strings.Contains(got, "(~100 events)") {
		t.Errorf("the estimate is missing, so an empty sibling reads like a full one: %s", got)
	}

	// A sibling the catalogue has no estimate for is named WITHOUT a count
	// rather than as "0 events", which would read as "this one is empty too"
	// and send the operator away from the database that may hold everything.
	one := describeSiblings([]siblingIndex{{Name: "bintrail_idx_x"}})
	if strings.Contains(one, "events") {
		t.Errorf("a sibling with no estimate was given one: %s", one)
	}
	if !strings.Contains(one, "bintrail_idx_x") {
		t.Errorf("the sibling is not named: %s", one)
	}
}
