package reconstruct

import (
	"testing"

	"github.com/dbtrail/dbtrail/internal/query"
)

// windowEmptyByPosition is the whole of #1689's decision: it must say "empty"
// only when no event can possibly be in the window, and it must say it whenever
// a cut was resolved and that cut is at-or-before the anchor — the shape a
// refresh takes when capture is behind, where the fold used to walk the index
// to apply nothing. With no cut or no anchor nothing is provable and it must
// stay silent, whatever the index has reached: the function never claims a
// window is NON-empty.
func TestWindowEmptyByPosition(t *testing.T) {
	pos := func(f string, p uint64) *query.BinlogPos { return &query.BinlogPos{File: f, Pos: p} }

	cases := []struct {
		name         string
		since, until *query.BinlogPos
		want         bool
	}{
		{"no bounds at all", nil, nil, false},
		{"only a lower bound (mydumper output never sets a cut)", pos("binlog.000001", 100), nil, false},
		{"only an upper bound (a baseline that recorded no coordinate)", nil, pos("binlog.000001", 100), false},
		{"cut past the anchor: the ordinary window", pos("binlog.000001", 100), pos("binlog.000001", 900), false},
		{"cut exactly at the anchor", pos("binlog.000001", 100), pos("binlog.000001", 100), true},
		{"cut one byte before the anchor", pos("binlog.000001", 100), pos("binlog.000001", 99), true},
		{"cut in an earlier file: capture behind the baseline",
			pos("binlog.000009", 4), pos("binlog.000008", 999999), true},
		{"cut in a later file", pos("binlog.000008", 999999), pos("binlog.000009", 4), false},
		{"across the rollover, cut is later", pos("binlog.999999", 900), pos("binlog.1000000", 4), false},
		{"across the rollover, cut is earlier", pos("binlog.1000000", 4), pos("binlog.999999", 900), true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := windowEmptyByPosition(c.since, c.until); got != c.want {
				t.Errorf("windowEmptyByPosition(%v, %v) = %v, want %v", c.since, c.until, got, c.want)
			}
		})
	}
}

// The boundary the proof rests on, stated on its own so a future "optimisation"
// to `until.Pos < since.Pos` has a test in its way: an event whose start_pos is
// exactly the anchor still needs an end_pos STRICTLY greater, so a cut sitting
// on the anchor admits nothing either.
func TestWindowEmptyByPosition_cutOnTheAnchorAdmitsNothing(t *testing.T) {
	since := &query.BinlogPos{File: "binlog.000001", Pos: 4}
	if !windowEmptyByPosition(since, &query.BinlogPos{File: "binlog.000001", Pos: 4}) {
		t.Error("a cut exactly at the anchor admits no event: every event ends after it starts")
	}
	if windowEmptyByPosition(since, &query.BinlogPos{File: "binlog.000001", Pos: 5}) {
		t.Error("a cut past the anchor leaves room by the ordering, which is all this function " +
			"reasons about; it must not be called empty (that a real event is at least a 19-byte " +
			"header is not something the proof uses)")
	}
}
