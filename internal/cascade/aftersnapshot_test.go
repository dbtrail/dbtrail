package cascade

import (
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/query"
)

// TestAfterSnapshot pins the skip-mode membership filter's comparator: by
// recorded binlog position when the baseline has one (file order first, then
// AT-OR-AFTER the position — a baseline stamped from SHOW MASTER STATUS
// records exactly the next event's start), else by execution timestamp.
func TestAfterSnapshot(t *testing.T) {
	snap := time.Date(2026, 9, 11, 14, 10, 0, 0, time.UTC)
	pos := &query.BinlogPos{File: "b.000002", Pos: 500}
	ev := func(file string, start uint64, at time.Time) query.ResultRow {
		return query.ResultRow{BinlogFile: file, StartPos: start, EventTimestamp: at}
	}
	before, after := snap.Add(-time.Minute), snap.Add(time.Minute)
	cases := []struct {
		name string
		ev   query.ResultRow
		pos  *query.BinlogPos
		want bool
	}{
		{"no position, before by time", ev("b.000002", 900, before), nil, false},
		{"no position, at the snapshot instant", ev("b.000002", 900, snap), nil, true},
		{"no position, after by time", ev("b.000002", 1, after), nil, true},
		{"earlier file, later time", ev("b.000001", 9000, after), pos, false},
		{"later file, earlier time", ev("b.000003", 1, before), pos, true},
		{"same file, before the position", ev("b.000002", 499, after), pos, false},
		{"same file, exactly the position", ev("b.000002", 500, before), pos, true},
		{"same file, past the position", ev("b.000002", 900, before), pos, true},
		{"position recorded but event carries no file: time decides", ev("", 0, before), pos, false},
		// #840 rollover: the six-digit suffix overflows to seven; length orders first.
		{"rollover: shorter name is older even though it sorts later", ev("b.999999", 9000, after), &query.BinlogPos{File: "b.1000000", Pos: 4}, false},
		{"rollover: longer name is newer even though it sorts earlier", ev("b.1000001", 4, before), &query.BinlogPos{File: "b.999999", Pos: 9000}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := afterSnapshot(tc.ev, snap, tc.pos); got != tc.want {
				t.Errorf("afterSnapshot = %v, want %v", got, tc.want)
			}
		})
	}
}
