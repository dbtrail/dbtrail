package consoleapp

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/console"
)

// The PostgreSQL router's status says how old the data of each server's last
// answer was (#2756): the last, rounded up, and the worst since the start;
// an answer from the snapshot alone says why it has none, and one that read
// no table changes nothing.
func TestPGRouterStatusCopyAge(t *testing.T) {
	r := newPGRouteStats(time.Now(), false)
	ran := time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC)
	r.age("p", console.CopyRunAge{Ran: ran, Through: ran.Add(-1200 * time.Millisecond), Tailed: true})
	n := r.doc("").Servers["p"]
	if n.DataAsOf != "2026-11-17T11:59:58Z" || n.DataAgeSeconds == nil || *n.DataAgeSeconds != 2 || *n.DataAgeMaxSeconds != 2 || n.DataAgeUnknownReason != "" {
		t.Fatalf("after a fresh answer 1.2 s behind: %+v", n)
	}
	r.age("p", console.CopyRunAge{Ran: ran.Add(time.Second), Through: ran.Add(-4 * time.Second), Tailed: true})
	r.age("p", console.CopyRunAge{Ran: ran.Add(2 * time.Second), Through: ran.Add(1500 * time.Millisecond), Tailed: true})
	n = r.doc("").Servers["p"]
	if *n.DataAgeSeconds != 1 || *n.DataAgeMaxSeconds != 5 {
		t.Fatalf("last and worst: age=%d max=%d, want 1 and 5", *n.DataAgeSeconds, *n.DataAgeMaxSeconds)
	}
	r.age("p", console.CopyRunAge{})
	if n = r.doc("").Servers["p"]; *n.DataAgeSeconds != 1 {
		t.Fatalf("an answer that read no table changed the age: %+v", n)
	}
	r.age("p", console.CopyRunAge{Ran: ran.Add(3 * time.Second), Snapshot: true})
	n = r.doc("").Servers["p"]
	if n.DataAgeSeconds != nil || n.DataAsOf != "" || !strings.Contains(n.DataAgeUnknownReason, "snapshot alone") || *n.DataAgeMaxSeconds != 5 {
		t.Fatalf("after the snapshot alone: %+v", n)
	}
	// A clock behind the source's never gives a negative age.
	r.age("q", console.CopyRunAge{Ran: ran, Through: ran.Add(time.Second), Tailed: true})
	if n = r.doc("").Servers["q"]; *n.DataAgeSeconds != 0 {
		t.Fatalf("a through after the run: %+v", n)
	}
	b, _ := json.Marshal(r.doc(""))
	for _, want := range []string{`"data_as_of":`, `"data_age_seconds":`, `"data_age_max_seconds":5`, `"data_age_unknown_reason":`} {
		if !strings.Contains(string(b), want) {
			t.Errorf("the JSON lacks %s: %s", want, b)
		}
	}
	var nilStats *pgRouteStats
	nilStats.age("p", console.CopyRunAge{Tailed: true}) // no panic
}
