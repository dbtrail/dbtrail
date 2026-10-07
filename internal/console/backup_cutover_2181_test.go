package console

import (
	"strings"
	"testing"
	"time"
)

// #2181: on a small source the cost rule took a full read every other slot
// because the cheapest update (15 s) took longer than the last full read
// (6 s). A full read loads the production source and an update does not, so
// when the two cost about the same the update is preferred: a full read wins
// on cost only when the update is clearly dearer, more than
// BackupFullCostFactor times the full read AND more than BackupFullCostFloor
// longer. Both the measured fixed cost and the estimate are judged this way.
func TestCutoverToFull_fullReadOnlyWhenTheUpdateIsClearlyDearer_2181(t *testing.T) {
	now := time.Date(2026, 10, 7, 10, 30, 0, 0, time.UTC)
	fresh := now.Add(-5 * time.Minute)
	fixed := func(update, full time.Duration) BackupWindow {
		return BackupWindow{Anchor: fresh, Events: 40, FoldFixed: update, LastFull: full}
	}
	cases := []struct {
		name string
		w    BackupWindow
		want string
	}{
		// The issue's numbers: 2.5 times the full read, but only 9 s more
		// on a five-minute slot. Today a full read; now an update.
		{"issue: 15 s update vs 6 s full read", fixed(15*time.Second, 6*time.Second), ""},
		// After the upload fix (#2203), measured with S3 at 190 ms a request.
		{"5.8 s update vs 3.9 s full read", fixed(5800*time.Millisecond, 3900*time.Millisecond), ""},
		{"really dear: 120 s update vs 6 s full read", fixed(120*time.Second, 6*time.Second), "window_measured"},
		// Each half of the margin alone is not enough.
		{"factor without floor: 50 s vs 24 s (2.08x, 26 s more)", fixed(50*time.Second, 24*time.Second), ""},
		{"floor without factor: 15 m vs 10 m (1.5x, 5 m more)", fixed(15*time.Minute, 10*time.Minute), ""},
		{"both: 6 m vs 2 m", fixed(6*time.Minute, 2*time.Minute), "window_measured"},
		// The edges are exclusive: equal to the margin is not clearly dearer.
		{"exactly 2x and exactly 30 s more: 60 s vs 30 s", fixed(60*time.Second, 30*time.Second), ""},
		{"just past both: 61 s vs 30 s", fixed(61*time.Second, 30*time.Second), "window_measured"},
		// The estimate (fixed cost plus events at the measured rate).
		{"estimate 1.5x the full read: update", BackupWindow{Anchor: fresh, Events: 720_000, FoldRate: 1000, LastFull: 8 * time.Minute}, ""},
		{"estimate 3x and minutes more: full", BackupWindow{Anchor: fresh, Events: 1_440_000, FoldRate: 1000, LastFull: 8 * time.Minute}, "window_measured"},
		{"estimate 2.5x but 9 s more: update", BackupWindow{Anchor: fresh, Events: 15_000, FoldRate: 1000, LastFull: 6 * time.Second}, ""},
	}
	for _, c := range cases {
		why := CutoverToFull(c.w, 5*time.Minute, now)
		if got := BackupWhyCode(why); got != c.want {
			t.Errorf("%s: got %q (%s), want %q", c.name, got, why, c.want)
		}
	}

	// The reason says the margin, so a reader sees why this was not close.
	for _, w := range []BackupWindow{fixed(120*time.Second, 6*time.Second),
		{Anchor: fresh, Events: 1_440_000, FoldRate: 1000, LastFull: 8 * time.Minute}} {
		why := CutoverToFull(w, 5*time.Minute, now)
		if !strings.Contains(why, "more than twice") || !strings.Contains(why, "30s longer") {
			t.Errorf("reason does not state the margin: %q", why)
		}
	}
}

// The slot after a full read is unchanged: no update has been measured since
// it, so the model may not choose a full read however dear it says updates
// are (#1737); the update runs and measures again.
func TestCutoverToFull_slotAfterAFullReadUpdates_2181(t *testing.T) {
	now := time.Date(2026, 10, 7, 10, 30, 0, 0, time.UTC)
	w := BackupWindow{Anchor: now.Add(-5 * time.Minute), Events: 40, FoldFixed: 120 * time.Second,
		LastFull: 6 * time.Second, UnmeasuredSinceFull: true}
	if why := CutoverToFull(w, 5*time.Minute, now); why != "" {
		t.Fatalf("the slot after a full read chose a full read again: %q", why)
	}
}

// Within the margin the estimate does not decide, so an old anchor still
// cuts over on age, and the reason says the estimate was close, not that no
// update was measured.
func TestCutoverToFull_withinTheMarginAnOldAnchorStillCutsOverOnAge_2181(t *testing.T) {
	now := time.Date(2026, 10, 7, 10, 30, 0, 0, time.UTC)
	w := BackupWindow{Anchor: now.Add(-5 * time.Hour), Events: 720_000, FoldRate: 1000, LastFull: 8 * time.Minute}
	why := CutoverToFull(w, 5*time.Minute, now)
	if BackupWhyCode(why) != "window_age" || !strings.Contains(why, "about the cost of a full read") {
		t.Fatalf("got %q, want a full read on age that says the estimate was within the margin", why)
	}
}
