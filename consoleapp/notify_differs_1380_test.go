package consoleapp

import (
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/console"
)

// #1380: a table that differs from a snapshot read with no locks is reported
// inconclusive, and the run is still a problem: it closes no open alert, sends
// no "clean again", and warns.
func TestWatchNotifier_aTornDifferenceIsNotClean(t *testing.T) {
	n, f := testNotifier()
	n.VerifyFinished(console.VerifyRunRecord{ServerID: "s1", ServerName: "wp",
		VerifyStatus: console.VerifyStatus{State: "succeeded", Summary: console.VerifySummary{Match: 3, Mismatch: 1, Total: 4}}})
	if len(f.events) != 1 {
		t.Fatalf("setup: %+v", f.events)
	}
	torn := console.VerifyRunRecord{ServerID: "s1", ServerName: "wp",
		VerifyStatus: console.VerifyStatus{State: "succeeded",
			Summary: console.VerifySummary{Match: 3, Inconclusive: 1, InconclusiveDiffers: 1, Total: 4}}}
	n.VerifyFinished(torn)
	if len(f.events) != 2 {
		t.Fatalf("a torn difference sent %d events after the mismatch, want one warning: %+v", len(f.events)-1, f.events)
	}
	ev := f.events[1]
	if ev.Resolved || ev.Severity != "warning" {
		t.Fatalf("a torn difference: %+v, want an open warning", ev)
	}
	if strings.Contains(ev.Summary, "clean") {
		t.Fatalf("a torn difference says clean: %q", ev.Summary)
	}
	want := "verification found 1 table(s) that differ from a snapshot read at different points in time (3 match); " +
		"the difference may come from that read, and a point-in-time database read tells"
	if ev.Summary != want || ev.Details["differs"] != "1" {
		t.Fatalf("summary %q details %v", ev.Summary, ev.Details)
	}

	// Alone, with no alert open, it still warns.
	n2, f2 := testNotifier()
	n2.VerifyFinished(torn)
	if len(f2.events) != 1 || f2.events[0].Resolved || f2.events[0].Severity != "warning" {
		t.Fatalf("a torn difference on its own: %+v", f2.events)
	}

	// A torn snapshot that MATCHES is not a problem, and any other
	// inconclusive beside matches stays as quiet as it was.
	for name, s := range map[string]console.VerifySummary{
		"other inconclusive beside matches": {Match: 3, Inconclusive: 1, Total: 4},
		"all match":                         {Match: 4, Total: 4},
	} {
		n3, f3 := testNotifier()
		n3.VerifyFinished(console.VerifyRunRecord{ServerID: "s3", VerifyStatus: console.VerifyStatus{State: "succeeded", Summary: s}})
		if len(f3.events) != 0 {
			t.Errorf("%s: %+v", name, f3.events)
		}
	}
}
