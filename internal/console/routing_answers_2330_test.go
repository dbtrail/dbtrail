package console

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestRoutingStats_answersPerSide(t *testing.T) {
	r := newRoutingStats(time.Now(), nil)
	r.answered("a", "mysql", "", 2*time.Second, false, false)
	r.answered("a", "mysql", "", 4*time.Second, true, false)
	r.answered("a", "mysql", "", time.Second, true, true)
	r.answered("a", "copy", "", 500*time.Millisecond, false, false)
	// Not counted as a timeout unless it failed, a side nobody knows is
	// dropped, and a negative time (the clock stepped back) counts as zero.
	r.answered("a", "copy", "", -time.Second, false, true)
	r.answered("a", "refused", "", time.Second, false, false)
	r.answered("b", "copy", "", time.Second, false, false)

	got := r.snapshot()
	my, cp := got["a"].Answers["mysql"], got["a"].Answers["copy"]
	if my != (RouteAnswers{Count: 3, Errors: 2, Timeouts: 1, Seconds: 7, SlowestSeconds: 4}) {
		t.Errorf("mysql = %+v", my)
	}
	if cp != (RouteAnswers{Count: 2, Seconds: 0.5, SlowestSeconds: 0.5}) {
		t.Errorf("copy = %+v", cp)
	}
	if len(got["a"].Answers) != 2 || got["b"].Answers["copy"].Count != 1 {
		t.Errorf("got %+v", got)
	}
}

// A server with decisions and no finished statement has no answers key at
// all: zero counts would read as "measured, and nothing ran".
func TestRoutingStats_noAnswersIsAbsent(t *testing.T) {
	r := newRoutingStats(time.Now(), nil)
	r.record("a", "mysql", "cheap_plan")
	b, err := json.Marshal(r.snapshot()["a"])
	if err != nil || strings.Contains(string(b), "answers") {
		t.Fatalf("json = %s, err %v", b, err)
	}
}

// The router's document carries the answers, and nothing in them but counts
// and times.
func TestRouterNumbers_carryAnswers(t *testing.T) {
	srv, err := New(Config{Listen: "127.0.0.1:8090", Token: "t"})
	if err != nil {
		t.Fatal(err)
	}
	if r := srv.SQLSandbox(); r != nil {
		t.Cleanup(r.Close)
	}
	srv.RecordRouteDecision("a", "copy", "expensive_plan")
	srv.RecordRouteAnswer("a", "copy", "SELECT 1", 1500*time.Millisecond, false, false)
	n := srv.RouterNumbers()
	if got := n.Servers["a"].Answers["copy"]; got != (RouteAnswers{Count: 1, Seconds: 1.5, SlowestSeconds: 1.5}) {
		t.Fatalf("answers = %+v", got)
	}
	b, _ := json.Marshal(n.Servers["a"].Answers)
	if string(b) != `{"copy":{"count":1,"errors":0,"timeouts":0,"seconds":1.5,"slowest_seconds":1.5}}` {
		t.Fatalf("json = %s", b)
	}
}
