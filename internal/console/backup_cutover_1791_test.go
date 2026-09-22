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
	w := quiet(0, CaptureBehind)
	w.CaptureDetail = "the capture's GTID set does not contain the source's"
	why := CutoverToFull(w, 5*time.Minute, now)
	for _, want := range []string{"it is 5h 30m old and the cut-over is 2h; nothing new was indexed since it, but the source has written past what the capture has recorded, so the capture may have stopped or fallen behind (the capture's GTID set does not contain the source's)"} {
		if !strings.Contains(why, want) {
			t.Errorf("behind reason %q lacks %q", why, want)
		}
	}
	w = quiet(0, "")
	w.CaptureDetail = "the source did not answer"
	why = CutoverToFull(w, 5*time.Minute, now)
	if !strings.Contains(why, "nothing new was indexed since it, and whether the source changed could not be checked (the source did not answer)") ||
		strings.Contains(why, "update rate") {
		t.Errorf("unknown reason %q, want the capture question and no rate talk", why)
	}
}
