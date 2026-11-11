package console

import (
	"testing"
	"time"
)

// What the copy answers on a connection that does not route is counted per
// server, apart from the routing decisions, and reaches the numbers a router
// publishes, for a server with routed statements and for one with none.
func TestRouterNumbersCarryUnroutedAnswers(t *testing.T) {
	s := &Server{routing: newRoutingStats(time.Now(), nil)}
	q := &SQLOnCopy{s: s, id: "id-a"}
	q.NoteAnswered(200*time.Millisecond, false)
	q.NoteAnswered(400*time.Millisecond, true)
	(&SQLOnCopy{s: s, id: "id-b"}).NoteAnswered(time.Second, false)
	s.RecordRouteDecision("id-b", "mysql", "cheap")
	// A copy with no server behind it, and no copy at all, count nothing
	// and do not panic.
	(&SQLOnCopy{}).NoteAnswered(time.Second, false)
	var none *SQLOnCopy
	none.NoteAnswered(time.Second, false)

	got := s.routing.aloneSnapshot()
	a, b := got["id-a"], got["id-b"]
	if a.Count != 2 || a.Errors != 1 || a.Seconds < 0.59 || a.Seconds > 0.61 || a.SlowestSeconds < 0.39 || a.SlowestSeconds > 0.41 {
		t.Errorf("id-a = %+v", a)
	}
	if b.Count != 1 || b.Errors != 0 {
		t.Errorf("id-b = %+v", b)
	}
	if len(got) != 2 {
		t.Errorf("counted for %d servers, want 2: %v", len(got), got)
	}
	// Not a routing decision: the decision counts stay what they were.
	snap := s.routing.snapshot()
	if _, ok := snap["id-a"]; ok {
		t.Errorf("an unrouted answer made a routing entry for id-a: %+v", snap["id-a"])
	}
	if snap["id-b"].MySQL != 1 || snap["id-b"].Copy != 0 {
		t.Errorf("id-b decisions = %+v", snap["id-b"])
	}
}
