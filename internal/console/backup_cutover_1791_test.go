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
		{"nothing indexed, the source is ahead: full", quiet(0, CaptureBehind), "window_age"},
		{"nothing indexed, the source could not be asked: full, as before", quiet(0, ""), "window_age"},
		// Only a zero count puts the index in doubt: with an unknown one
		// (-1) or a real one the update runs whatever the source says, since
		// the anchor's age alone chooses nothing (#2659).
		{"count unknown, the source confirms: update", quiet(-1, CaptureCaughtUp), ""},
		{"five events, the source confirms: update", quiet(5, CaptureCaughtUp), ""},
		{"count unknown, the source is ahead: update", quiet(-1, CaptureBehind), ""},
		{"five events, the source is ahead: update", quiet(5, CaptureBehind), ""},
		{"five events, the source could not be asked: update", quiet(5, ""), ""},
		// A fresh anchor never reaches the question.
		{"fresh anchor, the source ahead: update", BackupWindow{Anchor: fresh, Events: 0, Capture: CaptureBehind}, ""},
		// The measured rule decides first, as before: with a rate, nothing
		// to fold is an update whatever the source says.
		{"a rate and nothing to fold, the source ahead: update", BackupWindow{Anchor: old, Events: 0, FoldRate: 4000, LastFull: 8 * time.Minute, Capture: CaptureBehind}, ""},
		// The same with the fixed cost of an update on record, under the
		// last full read: the usual shape of a server with a rate.
		{"a rate, a fixed cost under the full read, the source ahead: update", BackupWindow{Anchor: old, Events: 0, FoldFixed: 90 * time.Second, FoldRate: 4000, LastFull: 8 * time.Minute, Capture: CaptureBehind}, ""},
		{"a rate, a fixed cost clearly over the full read, the source ahead: update", BackupWindow{Anchor: old, Events: 0, FoldFixed: 20 * time.Minute, FoldRate: 4000, LastFull: 8 * time.Minute, Capture: CaptureBehind}, ""},
		// The one shape with a rate that reaches the question: a fixed cost
		// over the last full read but within the margin, where the estimate
		// settles nothing.
		{"a rate, a fixed cost a little over the full read, the source ahead: full", BackupWindow{Anchor: old, Events: 0, FoldFixed: 9 * time.Minute, FoldRate: 4000, LastFull: 8 * time.Minute, Capture: CaptureBehind}, "window_age"},
		{"a rate and no full read on record, the source ahead: full", BackupWindow{Anchor: old, Events: 0, FoldRate: 4000, Capture: CaptureBehind}, "window_age"},
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
	if want := "the index may be missing changes made since the previous snapshot: nothing new was indexed since it was taken 5h 30m ago, but the source reports transactions the capture's checkpoint does not include, so the capture may have stopped or fallen behind"; why != want {
		t.Errorf("behind reason %q, want %q", why, want)
	}
	w = quiet(0, "")
	w.CaptureDetail = "the source did not answer"
	why = CutoverToFull(w, 5*time.Minute, now)
	if !strings.Contains(why, "nothing new was indexed since it was taken 5h 30m ago, and whether the source changed could not be checked (the source did not answer)") ||
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
