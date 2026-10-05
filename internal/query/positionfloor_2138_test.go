package query

import (
	"slices"
	"testing"
	"time"
)

// #2138, the rule on its own: which partitions a fetch anchored on a position
// must still reach although its time floor leaves them out.

// headsAt builds hourly partitions p0, p1, ... starting at h0. heads[i] is the
// start position of partition i's newest row; a negative value is an empty
// partition.
func headsAt(h0 time.Time, heads ...int64) *PartitionHeads {
	h := &PartitionHeads{}
	for i, pos := range heads {
		p := partitionHead{
			name:  "p" + string(rune('0'+i)),
			lower: h0.Add(time.Duration(i) * time.Hour),
			open:  i == 0,
		}
		if pos < 0 {
			p.empty = true
		} else {
			p.pos = BinlogPos{File: "binlog.000001", Pos: uint64(pos)}
		}
		h.parts = append(h.parts, p)
	}
	return h
}

// The issue's own statement: the previous snapshot is stamped T, the index
// then receives an event that ran at T-3h, after the previous cut. The window
// must reach the partition that event is in.
func TestPartitionHeads_anEventAfterTheCutThatRanHoursBeforeTheStampIsInside(t *testing.T) {
	h0 := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	T := h0.Add(10*time.Hour + 30*time.Minute)
	cut := BinlogPos{File: "binlog.000001", Pos: 500}
	// Hours 0..10. The event at T-3h (hour 7) starts at 600, after the cut;
	// every other partition's newest row is before the cut.
	h := headsAt(h0, 100, 110, 120, 130, 140, 150, 160, 600, 480, 490, 500-1)
	if floor := CoarseSinceFloor(T); !h0.Add(7 * time.Hour).Before(floor) {
		t.Fatalf("the fixture must put the event below the floor %s the stamp gives", floor)
	}
	if got := h.below(T, cut); !slices.Equal(got, []string{"p7"}) {
		t.Fatalf("partitions the fetch must reach = %v, want [p7]: the event at T-3h is after the cut", got)
	}
}

func TestPartitionHeads_below(t *testing.T) {
	h0 := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	since := h0.Add(5*time.Hour + 30*time.Minute) // floor: h0+4h
	anchor := BinlogPos{File: "binlog.000001", Pos: 500}
	for _, tc := range []struct {
		name string
		h    *PartitionHeads
		at   time.Time
		want []string
	}{
		{"nothing after the anchor below the floor", headsAt(h0, 100, 200, 300, 400, 900, 950), since, nil},
		{"a newest row exactly at the anchor counts: the gate is start_pos >= anchor", headsAt(h0, 100, 500, 300, 400), since, []string{"p1"}},
		{"one position before the anchor does not", headsAt(h0, 100, 499, 300, 400), since, nil},
		{"the first partition has no lower bound and is below any floor", headsAt(h0, 700), h0.Add(-48 * time.Hour), []string{"p0"}},
		{"empty partitions are skipped", headsAt(h0, -1, 700, -1, 400), since, []string{"p1"}},
		// p3 is [h0+3h, h0+4h): entirely below the floor. p4 starts AT the
		// floor: the fetch reads it anyway.
		{"the partition that ends at the floor is in, the one that starts at it is not", headsAt(h0, 100, 200, 300, 800, 900), since, []string{"p3"}},
		{"a later binlog file is after the anchor whatever its offset", func() *PartitionHeads {
			h := headsAt(h0, 100, 200)
			h.parts[1].pos = BinlogPos{File: "binlog.000002", Pos: 4}
			return h
		}(), since, []string{"p1"}},
		{"a newest row with no coordinate cannot be placed, so its partition is reached", func() *PartitionHeads {
			h := headsAt(h0, 100, 200)
			h.parts[0].unknown = true
			return h
		}(), since, []string{"p0"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.h.below(tc.at, anchor); !slices.Equal(got, tc.want) {
				t.Fatalf("below = %v, want %v", got, tc.want)
			}
		})
	}
}

// `bintrail index` adding files to an index a stream writes puts old
// positions on top of a partition. A fetch whose snapshot is older than that
// run cannot trust the newest row, so it reaches every partition below its
// floor that holds anything.
func TestPartitionHeads_filesIndexedSinceTheSnapshotVoidTheShortcut(t *testing.T) {
	h0 := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	since := h0.Add(5*time.Hour + 30*time.Minute)
	anchor := BinlogPos{File: "binlog.000001", Pos: 500}
	mk := func(stream bool, lastFile time.Time) *PartitionHeads {
		h := headsAt(h0, 100, -1, 300, 400, 900)
		h.streamCaptured, h.lastFileIndexed = stream, lastFile
		return h
	}
	all := []string{"p0", "p2", "p3"}
	for _, tc := range []struct {
		name string
		h    *PartitionHeads
		want []string
	}{
		{"a stream alone", mk(true, time.Time{}), nil},
		{"files alone, however recent", mk(false, since.Add(time.Hour)), nil},
		{"stream, files indexed after the snapshot", mk(true, since.Add(time.Minute)), all},
		{"stream, files indexed inside the clock margin before it", mk(true, since.Add(-fileIndexingMargin)), all},
		{"stream, files indexed well before it", mk(true, since.Add(-fileIndexingMargin-time.Second)), nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.h.below(since, anchor); !slices.Equal(got, tc.want) {
				t.Fatalf("below = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestPartitionClause(t *testing.T) {
	for _, tc := range []struct {
		names []string
		want  string
	}{
		{nil, ""},
		{[]string{""}, ""}, // a table that is not partitioned
		{[]string{"p_2026030100", "p_future"}, " PARTITION (`p_2026030100`, `p_future`)"},
		{[]string{"a`b"}, " PARTITION (`a``b`)"},
	} {
		if got := partitionClause(tc.names); got != tc.want {
			t.Errorf("partitionClause(%q) = %q, want %q", tc.names, got, tc.want)
		}
	}
}

// buildQuery and PartitionHeads must agree on the floor: a partition the
// index bound leaves out and the picture does not look at is the loss again.
func TestCoarseSinceFloor_isTheBoundBuildQueryApplies(t *testing.T) {
	since := time.Date(2026, 3, 1, 14, 37, 12, 0, time.UTC)
	want := time.Date(2026, 3, 1, 13, 0, 0, 0, time.UTC)
	if got := CoarseSinceFloor(since); !got.Equal(want) {
		t.Fatalf("CoarseSinceFloor = %s, want %s", got, want)
	}
	_, args := buildQuery(Options{Schema: "s", Table: "t", Since: &since, SincePos: &BinlogPos{File: "binlog.000001", Pos: 4}})
	found := false
	for _, a := range args {
		if ts, ok := a.(time.Time); ok && ts.Equal(want) {
			found = true
		}
	}
	if !found {
		t.Fatalf("buildQuery did not bind %s as its time floor; args = %v", want, args)
	}
}
