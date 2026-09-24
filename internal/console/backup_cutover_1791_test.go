package console

import (
	"strings"
	"testing"
	"time"
)

// #1791: a quiet server with no measured rate used to take a full backup at
// every slot once its anchor passed the cut-over age, with nothing indexed
// since. The daemon now asks the source whether it wrote anything the
// capture has not recorded; only a confirmed "no" skips the full backup.
func TestCutoverToFull_nothingIndexedAndTheSourceConfirmsIt(t *testing.T) {
	now := time.Date(2026, 9, 22, 10, 30, 0, 0, time.UTC)
	old := now.Add(-5*time.Hour - 30*time.Minute)
	fresh := now.Add(-5 * time.Minute)
	quiet := func(events int64, capture string) BackupWindow {
		return BackupWindow{Anchor: old, Events: events, FoldFixed: 90 * time.Second, LastFull: 40 * time.Minute, Capture: capture}
	}
	cases := []struct {
		name string
		w    BackupWindow
		want string
	}{
		{"nothing indexed, the source confirms: update", quiet(0, CaptureCaughtUp), ""},
		{"nothing indexed, the source is ahead: full on age", quiet(0, CaptureBehind), "window_age"},
		{"nothing indexed, the source could not be asked: full on age, as before", quiet(0, ""), "window_age"},
		// Only a zero count is "nothing to fold": an unknown one (-1) and a
		// real one keep the age rule whatever the source says.
		{"count unknown, the source confirms: full on age", quiet(-1, CaptureCaughtUp), "window_age"},
		{"five events, the source confirms: full on age", quiet(5, CaptureCaughtUp), "window_age"},
		// A fresh anchor never reaches the question.
		{"fresh anchor, the source ahead: update", BackupWindow{Anchor: fresh, Events: 0, Capture: CaptureBehind}, ""},
		// The measured rule decides first, as before: with a rate, nothing
		// to fold is an update whatever the source says.
		{"a rate and nothing to fold, the source ahead: update", BackupWindow{Anchor: old, Events: 0, FoldRate: 4000, LastFull: 8 * time.Minute, Capture: CaptureBehind}, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := BackupWhyCode(CutoverToFull(c.w, 5*time.Minute, now)); got != c.want {
				t.Fatalf("got %q, want %q", got, c.want)
			}
		})
	}
	// The reasons talk about the capture, not about a rate nobody needs to
	// fold nothing.
	// Behind says it once: the detail would only repeat it.
	w := quiet(0, CaptureBehind)
	w.CaptureDetail = "the source reports transactions the capture's checkpoint does not include"
	why := CutoverToFull(w, 5*time.Minute, now)
	if want := "it is 5h 30m old and the cut-over is 2h; nothing new was indexed since it, but the source reports transactions the capture's checkpoint does not include, so the capture may have stopped or fallen behind"; !strings.HasSuffix(why, want) {
		t.Errorf("behind reason %q, want it to end %q", why, want)
	}
	w = quiet(0, "")
	w.CaptureDetail = "the source did not answer"
	why = CutoverToFull(w, 5*time.Minute, now)
	if !strings.Contains(why, "nothing new was indexed since it, and whether the source changed could not be checked (the source did not answer)") ||
		strings.Contains(why, "update rate") {
		t.Errorf("unknown reason %q, want the capture question and no rate talk", why)
	}
}

// LastSourceRead: when the newest full backup at or before an anchor read
// the source, the instant the capture's dropped rows are dated against.
func TestLastSourceRead(t *testing.T) {
	anchor := time.Date(2026, 9, 22, 6, 0, 0, 0, time.UTC)
	at := func(d time.Duration) string { return anchor.Add(d).Format(time.RFC3339) }
	dump := func(snap, started string) BaselineRunRecord {
		return BaselineRunRecord{ServerID: "a", Kind: BaselineRunDump, SnapshotTime: snap, StartedAt: started, FinishedAt: snap}
	}
	cases := []struct {
		name string
		recs []BaselineRunRecord
		want time.Time
	}{
		{"nothing on record", nil, time.Time{}},
		{"the newest full read at or before the anchor: its start", []BaselineRunRecord{
			dump(at(-5*time.Hour), at(-6*time.Hour)),
			dump(at(-2*time.Hour), at(-3*time.Hour)),
			{ServerID: "a", Kind: BaselineRunRefresh, SnapshotTime: at(0), StartedAt: at(-time.Minute)},
		}, anchor.Add(-3 * time.Hour)},
		{"the anchor itself a full read", []BaselineRunRecord{dump(at(0), at(-20*time.Minute))}, anchor.Add(-20 * time.Minute)},
		{"a full read newer than the anchor is not its chain", []BaselineRunRecord{
			dump(at(-2*time.Hour), at(-3*time.Hour)),
			dump(at(time.Hour), at(30*time.Minute)),
		}, anchor.Add(-3 * time.Hour)},
		{"one that published nothing does not count", []BaselineRunRecord{
			dump(at(-2*time.Hour), at(-3*time.Hour)),
			{ServerID: "a", Kind: BaselineRunDump, StartedAt: at(-time.Hour), Error: "mydumper exited 2"},
		}, anchor.Add(-3 * time.Hour)},
		// Published here, refused by the bucket: a fold reading the bucket
		// descends from the older full backup, which never read the rows.
		{"one that failed after publishing does not count", []BaselineRunRecord{
			dump(at(-2*time.Hour), at(-3*time.Hour)),
			{ServerID: "a", Kind: BaselineRunDump, SnapshotTime: at(-time.Hour), StartedAt: at(-80 * time.Minute), Error: "upload failed"},
		}, anchor.Add(-3 * time.Hour)},
		{"a slot that did not start is not a read", []BaselineRunRecord{
			dump(at(-2*time.Hour), at(-3*time.Hour)),
			{ServerID: "a", Kind: BaselineRunDump, SkipReason: "busy", SnapshotTime: at(-time.Hour), StartedAt: at(-time.Hour)},
		}, anchor.Add(-3 * time.Hour)},
		{"another server's full read", []BaselineRunRecord{{ServerID: "b", Kind: BaselineRunDump, SnapshotTime: at(-time.Hour), StartedAt: at(-2 * time.Hour)}}, time.Time{}},
		{"the newest one's start does not parse: never, not an older one", []BaselineRunRecord{
			dump(at(-2*time.Hour), at(-3*time.Hour)),
			dump(at(-time.Hour), "yesterday"),
		}, time.Time{}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			h, err := OpenBaselineHistory(t.TempDir() + "/h.json")
			if err != nil {
				t.Fatal(err)
			}
			for _, r := range c.recs {
				if err := h.Append(r); err != nil {
					t.Fatal(err)
				}
			}
			if got := h.LastSourceRead("a", anchor); !got.Equal(c.want) {
				t.Fatalf("got %s, want %s", got, c.want)
			}
		})
	}
}
