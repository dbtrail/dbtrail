package pgtext

import (
	"testing"
	"time"
)

// A timestamp with time zone is written in the session's zone with that
// zone's offset at the instant, as PostgreSQL 16 writes it (each want below
// is what it printed under SET TimeZone); a timestamp without one, and text
// that is not an instant, are written as they always were (#2769).
func TestCellIn_writesAnInstantInTheSessionZone_2769(t *testing.T) {
	zone := func(name string) *time.Location {
		loc, err := time.LoadLocation(name)
		if err != nil {
			t.Skipf("no zone data for %s here: %v", name, err)
		}
		return loc
	}
	for _, c := range []struct {
		typ  string
		v    any
		loc  *time.Location
		want string
	}{
		{"TIMESTAMP WITH TIME ZONE", "2026-01-01T10:00:00Z", nil, "2026-01-01 10:00:00+00"},
		{"TIMESTAMPTZ", "2026-01-01T10:00:00Z", zone("Asia/Kolkata"), "2026-01-01 15:30:00+05:30"},
		{"TIMESTAMPTZ", "2026-01-01T10:00:00.5Z", zone("Asia/Kolkata"), "2026-01-01 15:30:00.5+05:30"},
		{"TIMESTAMPTZ", "2026-01-01T10:00:00Z", zone("Asia/Kathmandu"), "2026-01-01 15:45:00+05:45"},
		{"TIMESTAMPTZ", "1900-01-01T10:00:00Z", zone("Asia/Kathmandu"), "1900-01-01 15:41:16+05:41:16"},
		{"TIMESTAMPTZ", "1900-01-01T10:00:00Z", zone("Europe/Amsterdam"), "1900-01-01 10:19:32+00:19:32"},
		{"TIMESTAMPTZ", "2026-03-29T01:30:00Z", zone("Europe/Amsterdam"), "2026-03-29 03:30:00+02"},
		{"TIMESTAMPTZ", "2026-01-01T10:00:00Z", zone("America/Bogota"), "2026-01-01 05:00:00-05"},
		// The same instant written with another offset by the engine.
		{"TIMESTAMPTZ", "2026-01-01T05:00:00-05:00", nil, "2026-01-01 10:00:00+00"},
		// A wall clock stays a wall clock.
		{"TIMESTAMP", "2026-01-01T10:00:00Z", zone("America/Bogota"), "2026-01-01 10:00:00"},
		// What is not an instant passes as it came.
		{"TIMESTAMPTZ", "infinity", zone("America/Bogota"), "infinity"},
		{"TIMESTAMPTZ", "-infinity", nil, "-infinity"},
		{"TIMESTAMP", "infinity", nil, "infinity"},
		{"DATE", "-infinity", nil, "-infinity"},
		{"TIMESTAMPTZ[]", []any{"2026-01-01T10:00:00Z", nil, "infinity"}, zone("America/Bogota"), `{"2026-01-01 05:00:00-05",NULL,infinity}`},
		{"DATE[]", []any{"infinity", "-infinity"}, nil, "{infinity,-infinity}"},
	} {
		got := CellIn(c.typ, c.v, c.loc)
		if got == nil || *got != c.want {
			t.Errorf("CellIn(%s, %v, %v) = %v, want %q", c.typ, c.v, c.loc, got, c.want)
		}
	}
	// Cell is CellIn in UTC.
	if got := Cell("TIMESTAMPTZ", "2026-01-01T05:00:00-05:00"); got == nil || *got != "2026-01-01 10:00:00+00" {
		t.Errorf("Cell = %v", got)
	}
}
