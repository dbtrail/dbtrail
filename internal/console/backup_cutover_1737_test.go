package console

import (
	"strings"
	"testing"
	"time"
)

// #1737: once the model chose a full backup, no update after it was ever
// measured, so the next slot chose a full backup on the same numbers. The
// history now answers whether an update was measured since the last full
// backup, the full backup records the mark the next update counts from,
// and CutoverToFull lets the model decide only on evidence newer than it.

func TestBaselineHistory_measuredSinceFull(t *testing.T) {
	measured := BaselineRunRecord{Kind: BaselineRunRefresh, Events: 1000, UpdateSeconds: 60}
	full := BaselineRunRecord{Kind: BaselineRunDump, StartedAt: "2026-09-18T05:00:00Z", FinishedAt: "2026-09-18T05:08:00Z"}
	failedFull := full
	failedFull.Error = "mydumper: exit 2"
	skippedFull := BaselineRunRecord{Kind: BaselineRunDump, SkipReason: "another backup job was running"}
	unmeasured := BaselineRunRecord{Kind: BaselineRunRefresh, UpdateSeconds: 60} // the update after a full backup, before #1737
	failedUpdate := BaselineRunRecord{Kind: BaselineRunRefresh, Events: 1000, UpdateSeconds: 60, Error: "capture gap"}
	restore := BaselineRunRecord{Kind: BaselineRunRestore, Events: 1000, UpdateSeconds: 60}
	compact := BaselineRunRecord{Kind: BaselineRunCompact}
	cases := []struct {
		name string
		recs []BaselineRunRecord
		want bool
	}{
		{"empty history: nothing to be older than", nil, true},
		{"updates only, no full backup", []BaselineRunRecord{measured, measured}, true},
		{"a full backup is the newest record", []BaselineRunRecord{measured, full}, false},
		{"a measured update after the full backup", []BaselineRunRecord{measured, full, measured}, true},
		{"only an unmeasured update after it", []BaselineRunRecord{measured, full, unmeasured}, false},
		{"only a failed update after it", []BaselineRunRecord{measured, full, failedUpdate}, false},
		{"a restore and a compaction after it are not updates", []BaselineRunRecord{measured, full, restore, compact}, false},
		{"a failed full backup is not a full backup", []BaselineRunRecord{measured, failedFull}, true},
		{"a skipped full backup is not a full backup", []BaselineRunRecord{measured, skippedFull}, true},
		{"a failed full backup after a real one does not hide it", []BaselineRunRecord{measured, full, failedFull}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := historyWith(t, c.recs).MeasuredSinceFull("s"); got != c.want {
				t.Fatalf("MeasuredSinceFull = %v, want %v", got, c.want)
			}
		})
	}
	if !historyWith(t, []BaselineRunRecord{measured, full}).MeasuredSinceFull("another server") {
		t.Fatal("another server's full backup made this one's model stale")
	}
}

func TestBaselineHistory_updateSample(t *testing.T) {
	if n, newest := historyWith(t, nil).UpdateSample("s"); n != 0 || !newest.IsZero() {
		t.Fatalf("empty history: n=%d newest=%s", n, newest)
	}
	var recs []BaselineRunRecord
	for i := range 7 {
		recs = append(recs, BaselineRunRecord{Kind: BaselineRunRefresh, Events: 1000, UpdateSeconds: 60,
			FinishedAt: time.Date(2026, 9, 18, 10+i, 0, 0, 0, time.UTC).Format(time.RFC3339)})
	}
	// Newer records that are not measured updates do not count or date it.
	recs = append(recs,
		BaselineRunRecord{Kind: BaselineRunRefresh, UpdateSeconds: 60, FinishedAt: "2026-09-18T20:00:00Z"},
		BaselineRunRecord{Kind: BaselineRunDump, StartedAt: "2026-09-18T20:00:00Z", FinishedAt: "2026-09-18T21:00:00Z"})
	n, newest := historyWith(t, recs).UpdateSample("s")
	if n != measuredFoldRuns || !newest.Equal(time.Date(2026, 9, 18, 16, 0, 0, 0, time.UTC)) {
		t.Fatalf("n=%d newest=%s, want the model's five and the 16:00 update", n, newest)
	}
	recs[6].FinishedAt = "not a time"
	if n, newest := historyWith(t, recs).UpdateSample("s"); n != measuredFoldRuns || !newest.IsZero() {
		t.Fatalf("unparsable newest stamp: n=%d newest=%s, want five and no time", n, newest)
	}
}

// A successful full backup's mark is the base the next update counts from;
// anything that is not a successful update or full backup is not.
func TestBaselineHistory_indexMarkForAFullBackup(t *testing.T) {
	const snap = "2026-09-18T07:00:00Z"
	for _, c := range []struct {
		name string
		rec  BaselineRunRecord
		ok   bool
	}{
		{"successful full backup", BaselineRunRecord{Kind: BaselineRunDump, SnapshotTime: snap, IndexMark: 900}, true},
		{"successful update", BaselineRunRecord{Kind: BaselineRunRefresh, SnapshotTime: snap, IndexMark: 900}, true},
		{"failed full backup", BaselineRunRecord{Kind: BaselineRunDump, SnapshotTime: snap, IndexMark: 900, Error: "upload: denied"}, false},
		{"full backup with no mark (the index did not answer)", BaselineRunRecord{Kind: BaselineRunDump, SnapshotTime: snap}, false},
		{"restore", BaselineRunRecord{Kind: BaselineRunRestore, SnapshotTime: snap, IndexMark: 900}, false},
		{"compaction", BaselineRunRecord{Kind: BaselineRunCompact, SnapshotTime: snap, IndexMark: 900}, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			mark, ok := historyWith(t, []BaselineRunRecord{c.rec}).IndexMarkFor("s", snap)
			if ok != c.ok || (ok && mark != 900) {
				t.Fatalf("IndexMarkFor = %d,%v, want ok=%v", mark, ok, c.ok)
			}
		})
	}
}

// With no update measured since the last full backup, neither the rate nor
// the fixed cost decides; the age rule still does, and says what is missing.
func TestCutoverToFull_abstainsUntilAnUpdateIsMeasuredSinceTheFullBackup(t *testing.T) {
	now := time.Date(2026, 9, 18, 10, 30, 0, 0, time.UTC)
	fresh := now.Add(-5 * time.Minute)
	old := now.Add(-5*time.Hour - 30*time.Minute)
	cases := []struct {
		name string
		w    BackupWindow
		want string
	}{
		{"dear by the rate: update", BackupWindow{Anchor: fresh, Events: 17_000_000, FoldRate: 4000, LastFull: 8 * time.Minute}, ""},
		{"fixed cost over the full backup: update", BackupWindow{Anchor: fresh, Events: 1_000_000, FoldFixed: 6 * time.Minute, LastFull: 2 * time.Minute}, ""},
		{"fixed cost over it, count unknown: update", BackupWindow{Anchor: fresh, Events: -1, FoldFixed: 6 * time.Minute, LastFull: 2 * time.Minute}, ""},
		{"cheap by the rate, old anchor: full on age", BackupWindow{Anchor: old, Events: 1000, FoldRate: 1000, LastFull: 8 * time.Minute}, "window_age"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			w := c.w
			w.UnmeasuredSinceFull = true
			if got := BackupWhyCode(CutoverToFull(w, 5*time.Minute, now)); got != c.want {
				t.Fatalf("got %q, want %q", got, c.want)
			}
		})
	}
	// Controls: the same windows with an update measured since decide as
	// the model says (the old anchor is cheap by the estimate, so updated).
	for i, want := range []string{"window_measured", "window_measured", "window_measured", ""} {
		if got := BackupWhyCode(CutoverToFull(cases[i].w, 5*time.Minute, now)); got != want {
			t.Errorf("%s, model fresh: got %q, want %q", cases[i].name, got, want)
		}
	}
	// The age reason names the stale model as what is missing, and only
	// that when everything else is known.
	w := BackupWindow{Anchor: old, Events: 1000, FoldRate: 1000, LastFull: 8 * time.Minute, UnmeasuredSinceFull: true}
	why := CutoverToFull(w, 5*time.Minute, now)
	if !strings.Contains(why, "(no update measured since the last full backup, so the update could not be estimated)") {
		t.Fatalf("age reason %q does not name the stale model alone", why)
	}
}
