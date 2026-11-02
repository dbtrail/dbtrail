package indexer

import (
	"testing"
	"time"
)

// #2606: how far back the index may have lost events, from its partition
// history and its oldest live partition. A zero bound with ok means nothing
// was ever lost: the oldest partition has no lower bound, so it still holds
// every event older than its hour.
func TestPartitionHistory_LossBound(t *testing.T) {
	h := func(s string) time.Time {
		t.Helper()
		v, err := time.Parse("2006-01-02 15:04", s)
		if err != nil {
			t.Fatal(err)
		}
		return v.UTC()
	}
	born := h("2026-11-01 23:35")
	since := h("2026-11-01 23:36")
	for _, c := range []struct {
		name   string
		hist   PartitionHistory
		oldest time.Time
		since  time.Time
		want   time.Time
		ok     bool
		// notCaptured: capture began after the snapshot's coordinate, or
		// where it began is not known.
		notCaptured bool
	}{
		{"young, nothing dropped", PartitionHistory{Known: true, CreatedAt: born}, h("2026-11-01 23:00"), since, time.Time{}, true, false},
		{"a drop recorded", PartitionHistory{Known: true, CreatedAt: born, DroppedBelow: h("2026-11-02 05:00")}, h("2026-11-01 23:00"), since, h("2026-11-02 05:00"), true, false},
		{"the bottom moved with nothing recorded: an untracked drop", PartitionHistory{Known: true, CreatedAt: born}, h("2026-11-02 03:00"), since, h("2026-11-02 03:00"), true, false},
		{"a recorded drop below an untracked one", PartitionHistory{Known: true, CreatedAt: born, DroppedBelow: h("2026-11-02 01:00")}, h("2026-11-02 03:00"), since, h("2026-11-02 03:00"), true, false},
		{"a partition added below the first: not the history recorded", PartitionHistory{Known: true, CreatedAt: born}, h("2026-11-01 20:00"), since, h("2026-11-01 20:00"), true, false},
		{"no record: an older index", PartitionHistory{}, h("2026-11-01 23:00"), since, h("2026-11-01 23:00"), true, false},
		{"history unknown: a restored index", PartitionHistory{Known: false, CreatedAt: born}, h("2026-11-01 23:00"), since, h("2026-11-01 23:00"), true, false},
		{"created after the lookup start: an index emptied and created again", PartitionHistory{Known: true, CreatedAt: h("2026-11-01 23:40")}, h("2026-11-01 23:00"), since, h("2026-11-01 23:00"), true, false},
		{"no lookup start", PartitionHistory{Known: true, CreatedAt: born}, h("2026-11-01 23:00"), time.Time{}, h("2026-11-01 23:00"), true, false},
		{"no live partition", PartitionHistory{Known: true, CreatedAt: born}, time.Time{}, since, time.Time{}, false, false},
		{"capture began after the snapshot: what came between was never read", PartitionHistory{Known: true, CreatedAt: born}, h("2026-11-01 23:00"), since, h("2026-11-01 23:00"), true, true},
	} {
		got, ok := c.hist.LossBound(c.oldest, c.since, !c.notCaptured)
		if ok != c.ok || !got.Equal(c.want) {
			t.Errorf("%s: LossBound = %v, %v; want %v, %v", c.name, got, ok, c.want, c.ok)
		}
	}
}

// The bound a drop records is the upper bound of the highest partition it
// drops: partition p_YYYYMMDDHH holds every event before the next hour.
func TestDroppedBound(t *testing.T) {
	got, err := droppedBound([]string{"p_2026110105", "p_2026110107", "p_2026110106"})
	if err != nil {
		t.Fatal(err)
	}
	if want := time.Date(2026, 11, 1, 8, 0, 0, 0, time.UTC); !got.Equal(want) {
		t.Errorf("droppedBound = %v, want %v", got, want)
	}
	for _, bad := range [][]string{nil, {"p_future"}, {"p_2026110105", "pX"}} {
		if _, err := droppedBound(bad); err == nil {
			t.Errorf("droppedBound(%q): no error; a drop it cannot record must not run", bad)
		}
	}
}
