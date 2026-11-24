package consoleapp

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/pgshim"
)

var ageRan = time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC)

func fresh(ranOff, throughOff time.Duration) console.CopyRunAge {
	return console.CopyRunAge{Ran: ageRan.Add(ranOff), Through: ageRan.Add(throughOff), Tailed: true}
}

// The PostgreSQL router's status says how old the data of each server's last
// answer was (#2756): the last, rounded up, and the worst since the start;
// an answer with no age says why and is counted, and one that read no table
// changes nothing.
func TestPGRouterStatusCopyAge(t *testing.T) {
	r := newPGRouteStats(time.Now(), false)
	r.age("p", fresh(0, -1200*time.Millisecond))
	n := r.doc("").Servers["p"]
	if n.DataAsOf != "2026-11-17T11:59:58Z" || n.DataAgeSeconds == nil || *n.DataAgeSeconds != 2 || *n.DataAgeMaxSeconds != 2 || n.DataAgeUnknownReason != "" || n.DataAgeUnknown != 0 {
		t.Fatalf("after a fresh answer 1.2 s behind: %+v", n)
	}
	r.age("p", fresh(time.Second, -4*time.Second))
	r.age("p", fresh(2*time.Second, 1500*time.Millisecond))
	if n = r.doc("").Servers["p"]; *n.DataAgeSeconds != 1 || *n.DataAgeMaxSeconds != 5 {
		t.Fatalf("last and worst: age=%d max=%d, want 1 and 5", *n.DataAgeSeconds, *n.DataAgeMaxSeconds)
	}
	r.age("p", console.CopyRunAge{})
	if n = r.doc("").Servers["p"]; *n.DataAgeSeconds != 1 {
		t.Fatalf("an answer that read no table changed the age: %+v", n)
	}
	// A run that finished before the last one, said later by another
	// connection, is not the last.
	r.age("p", console.CopyRunAge{Ran: ageRan, Snapshot: true})
	if n = r.doc("").Servers["p"]; n.DataAgeSeconds == nil || *n.DataAgeSeconds != 1 || n.DataAgeUnknown != 1 {
		t.Fatalf("an older run took the last place: %+v", n)
	}
	r.age("p", console.CopyRunAge{Ran: ageRan.Add(3 * time.Second), Snapshot: true})
	n = r.doc("").Servers["p"]
	if n.DataAgeSeconds != nil || n.DataAsOf != "" || !strings.Contains(n.DataAgeUnknownReason, "newest captured changes") || *n.DataAgeMaxSeconds != 5 || n.DataAgeUnknown != 2 {
		t.Fatalf("after the snapshot alone: %+v", n)
	}
	r.age("p", console.CopyRunAge{Ran: ageRan.Add(4 * time.Second), Unknown: true})
	if n = r.doc("").Servers["p"]; !strings.Contains(n.DataAgeUnknownReason, "could not be told") || n.DataAgeUnknown != 3 {
		t.Fatalf("after files that cannot be told: %+v", n)
	}
	// A clock behind the source's never gives a negative age.
	r.age("q", fresh(0, time.Second))
	if n = r.doc("").Servers["q"]; *n.DataAgeSeconds != 0 {
		t.Fatalf("a through after the run: %+v", n)
	}
	b, _ := json.Marshal(r.doc(""))
	for _, want := range []string{`"data_as_of":`, `"data_age_seconds":`, `"data_age_max_seconds":5`, `"data_age_unknown":3`, `"data_age_unknown_reason":`} {
		if !strings.Contains(string(b), want) {
			t.Errorf("the JSON lacks %s: %s", want, b)
		}
	}
	var nilStats *pgRouteStats
	nilStats.age("p", fresh(0, 0)) // no panic
}

// A connection's age is kept only when the port says the copy's answer is
// what the client got: not when the source answered after all, not when the
// client got an error, and a probe's run is never stashed by the caller.
func TestPGConnAgesKeepOnlyTheCopysAnswers(t *testing.T) {
	r := newPGRouteStats(time.Now(), false)
	c := r.agesFor("p")
	routed := c.routed()

	c.stash(fresh(0, -time.Second))
	routed(pgshim.RouteNote{Statement: "SELECT n FROM t", Side: pgshim.RouteSource, Reason: pgshim.ReasonCopyColumnsDiffer})
	if n := r.doc("").Servers["p"]; n.DataAgeSeconds != nil || n.Source != 1 {
		t.Fatalf("the source answered and the copy's age was kept: %+v", n)
	}
	c.stash(fresh(0, -time.Second))
	routed(pgshim.RouteNote{Statement: "SELECT * FROM big", Side: pgshim.RouteCopy, Failed: true})
	if n := r.doc("").Servers["p"]; n.DataAgeSeconds != nil {
		t.Fatalf("the client got an error and the age was kept: %+v", n)
	}
	c.stash(fresh(0, -2*time.Second))
	routed(pgshim.RouteNote{Statement: "SELECT * FROM t", Side: pgshim.RouteCopy, Reason: pgshim.ReasonAnswersTheSame})
	if n := r.doc("").Servers["p"]; n.DataAgeSeconds == nil || *n.DataAgeSeconds != 2 || n.Copy != 2 {
		t.Fatalf("the copy's answer: %+v", n)
	}
	// What was stashed is used once: a later statement with no run of the
	// copy (answered by the source, or by the port itself) keeps nothing.
	routed(pgshim.RouteNote{Statement: "SELECT 1", Side: pgshim.RouteCopy, Reason: pgshim.ReasonCopyCommand})
	if n := r.doc("").Servers["p"]; *n.DataAgeSeconds != 2 || n.DataAgeUnknown != 0 {
		t.Fatalf("a stale stash was used twice: %+v", n)
	}

	var none *pgRouteStats
	if none.agesFor("p").routed() != nil {
		t.Fatal("no tally, and still a Routed hook")
	}
	none.agesFor("p").stash(fresh(0, 0)) // no panic
}

// An answer read from the snapshot alone has an age when its files say how
// far they reach (#3125): it is the last answer's age and counts toward the
// worst, and it is not counted among the answers with none. One whose files
// do not say stays unknown, with the reason.
func TestPGRouterStatusSnapshotAge(t *testing.T) {
	r := newPGRouteStats(time.Now(), false)
	placed := console.CopyRunAge{Ran: ageRan, Through: ageRan.Add(-90 * time.Second), Snapshot: true}
	r.age("p", placed)
	n := r.doc("").Servers["p"]
	if n.DataAsOf != "2026-11-17T11:58:30Z" || n.DataAgeSeconds == nil || *n.DataAgeSeconds != 90 || n.DataAgeMaxSeconds == nil || *n.DataAgeMaxSeconds != 90 ||
		n.DataAgeUnknown != 0 || n.DataAgeUnknownReason != "" {
		t.Fatalf("a snapshot answer whose files say how far they reach: %+v", n)
	}
	r.age("p", console.CopyRunAge{Ran: ageRan.Add(time.Second), Snapshot: true})
	n = r.doc("").Servers["p"]
	if n.DataAgeSeconds != nil || n.DataAgeUnknown != 1 || !strings.Contains(n.DataAgeUnknownReason, "does not record how far it reaches") || *n.DataAgeMaxSeconds != 90 {
		t.Fatalf("a snapshot answer whose files do not say: %+v", n)
	}
	// A through on an answer whose files cannot be told is not an age.
	if pgAgeKnown(console.CopyRunAge{Ran: ageRan, Through: ageRan, Unknown: true}) || pgAgeKnown(console.CopyRunAge{}) {
		t.Fatal("an answer with no table, or with files that cannot be told, has an age")
	}

	rows := pgLastAgeRows(placed)
	if len(rows) != 2 || rows[0] != [2]string{"last_answer_indexed_through", "2026-11-17T11:58:30Z"} || rows[1] != [2]string{"last_answer_age_seconds", "90"} {
		t.Fatalf("SHOW dbtrail.router_status after a placed snapshot answer: %v", rows)
	}
	rows = pgLastAgeRows(console.CopyRunAge{Ran: ageRan, Snapshot: true})
	if len(rows) != 3 || rows[1] != [2]string{"last_answer_age_seconds", "unknown"} || !strings.Contains(rows[2][1], "does not record how far it reaches") {
		t.Fatalf("SHOW dbtrail.router_status after a snapshot answer with no bound: %v", rows)
	}
}

// The port's own probes report no age, so they do not pay for one (#3125):
// measuring it reads the index.
func TestPGSnapshotAgeForProbe(t *testing.T) {
	if got := pgSnapshotAgeFor(pgshim.RunOptions{Probe: true}); got != nil {
		t.Errorf("a probe asked for the snapshot's age: %v", got)
	}
	if got := pgSnapshotAgeFor(pgshim.RunOptions{}); got == nil {
		t.Error("a client's statement did not ask for the snapshot's age")
	}
}
