package console

import (
	"testing"
	"time"
)

// #1738: on a quiet server every relative guard is passed by one update
// that applied a few more events and took a noisy while longer, because the
// server's own scale is that small. The per-event cost needs one run in the
// sample that applied at least measuredFoldMinEvents more than the shortest.

// The issue's sample and the slot it describes.
func TestUpdateModel_quietServerOneSlightlyLargerRun(t *testing.T) {
	var recs []BaselineRunRecord
	for range 4 {
		recs = append(recs, BaselineRunRecord{Kind: BaselineRunRefresh, Events: 100, UpdateSeconds: 90})
	}
	recs = append(recs, BaselineRunRecord{Kind: BaselineRunRefresh, Events: 200, UpdateSeconds: 140})
	// Every other guard lets it through: 50 s beyond is exactly a tenth of
	// the 500 s total, 100 events beyond is over a tenth of the 120 mean,
	// and 2 events/s is over a tenth of the shortest run's 1.1/s.
	fixed, rate := historyWith(t, recs).UpdateModel("s")
	if rate != 0 || fixed != 90*time.Second {
		t.Fatalf("four runs of 100 in 90 s and one of 200 in 140 s: fixed=%s rate=%v, want 90s and no rate (was 2 events/s)", fixed, rate)
	}
	// The slot: a 60,000-event burst on a fresh anchor, the last full
	// backup 40 minutes. At 2 events/s it read as 8h 21m and cut over; with
	// no rate it is an update, which takes about the fixed cost.
	now := time.Date(2026, 9, 18, 10, 30, 0, 0, time.UTC)
	w := BackupWindow{Anchor: now.Add(-5 * time.Minute), Events: 60_000, FoldFixed: fixed, FoldRate: rate, Proven: 200, LastFull: 40 * time.Minute}
	if why := CutoverToFull(w, 5*time.Minute, now); why != "" {
		t.Fatalf("a burst on a quiet server cut over: %q", why)
	}
	// Two slightly larger runs instead of one: 200 events beyond in all,
	// still no single run anywhere near the floor.
	recs = recs[:3]
	for range 2 {
		recs = append(recs, BaselineRunRecord{Kind: BaselineRunRefresh, Events: 200, UpdateSeconds: 140})
	}
	if _, rate := historyWith(t, recs).UpdateModel("s"); rate != 0 {
		t.Fatalf("three quiet runs and two slightly larger: rate=%v, want none", rate)
	}
}

// The floor from both sides, with every other guard passing: four runs of
// 1,000 events in 60 s and one that applied delta more in 100 s (40 s
// beyond against a 340 s total, a spread far over a tenth of the mean, a
// slope far over a tenth of the shortest run's 16.7/s).
func TestUpdateModel_minEventsFloorBoundary(t *testing.T) {
	at := func(delta int64) float64 {
		var recs []BaselineRunRecord
		for range 4 {
			recs = append(recs, BaselineRunRecord{Kind: BaselineRunRefresh, Events: 1000, UpdateSeconds: 60})
		}
		recs = append(recs, BaselineRunRecord{Kind: BaselineRunRefresh, Events: 1000 + delta, UpdateSeconds: 100})
		_, rate := historyWith(t, recs).UpdateModel("s")
		return rate
	}
	if rate := at(measuredFoldMinEvents); rate != float64(measuredFoldMinEvents)/40 {
		t.Fatalf("one run exactly the floor beyond the shortest: rate=%v, want %v", rate, float64(measuredFoldMinEvents)/40)
	}
	if rate := at(measuredFoldMinEvents - 1); rate != 0 {
		t.Fatalf("one run an event short of the floor: rate=%v, want none", rate)
	}
}

// Per run, and over the runs the slope is read from: neither two runs that
// only reach the floor together, nor a wide run that was no longer than the
// shortest (so it is not in the slope), make a rate.
func TestUpdateModel_minEventsFloorIsPerRun(t *testing.T) {
	// Oldest first: a run of 21,000 events in 60 s, as short as the
	// shortest, then the shortest (1,000 in 60 s), then two of 7,000 in
	// 100 s. The shortest is the newer 60 s run, so the wide one is not
	// beyond it in time and adds nothing to the slope; the two slower runs
	// are 6,000 beyond each, 12,000 together.
	recs := []BaselineRunRecord{
		{Kind: BaselineRunRefresh, Events: 21_000, UpdateSeconds: 60},
		{Kind: BaselineRunRefresh, Events: 1000, UpdateSeconds: 60},
		{Kind: BaselineRunRefresh, Events: 7000, UpdateSeconds: 100},
		{Kind: BaselineRunRefresh, Events: 7000, UpdateSeconds: 100},
	}
	if _, rate := historyWith(t, recs).UpdateModel("s"); rate != 0 {
		t.Fatalf("no run in the slope 10,000 beyond the shortest: rate=%v, want none", rate)
	}
	// Control: one of the slower runs wide enough, and the rate is back
	// (16,000 + 6,000 events over 80 s).
	recs[3].Events = 17_000
	if _, rate := historyWith(t, recs).UpdateModel("s"); rate != 22_000/80.0 {
		t.Fatalf("one run 16,000 beyond: rate=%v, want 22000/80", rate)
	}
}
