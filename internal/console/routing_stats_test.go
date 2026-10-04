package console

import (
	"encoding/json"
	"testing"
	"time"
)

// TestRoutingStats_tallyPerServer: counts land under the server they were
// recorded for, the snapshot is a copy (editing it changes nothing), and a
// route outside copy|mysql is kept under its reason so a vocabulary drift
// stays visible instead of vanishing.
func TestRoutingStats_tallyPerServer(t *testing.T) {
	r := newRoutingStats(time.Now())
	r.record("a", "copy", "expensive_plan")
	r.record("a", "mysql", "veto")
	r.record("a", "mysql", "veto")
	r.record("b", "mysql", "write")
	r.record("b", "sideways", "odd")

	snap := r.snapshot()
	if got := snap["a"]; got.Copy != 1 || got.MySQL != 2 || got.Reasons["veto"] != 2 || got.Reasons["expensive_plan"] != 1 {
		t.Errorf("a = %+v, want copy 1, mysql 2, veto 2, expensive_plan 1", got)
	}
	if got := snap["b"]; got.Copy != 0 || got.MySQL != 1 || got.Reasons["write"] != 1 || got.Reasons["odd"] != 1 {
		t.Errorf("b = %+v, want mysql 1 with reasons write 1 and odd 1 (unknown side counted under its reason only)", got)
	}
	if _, present := snap["c"]; present {
		t.Error("a server with no decision has an entry; the page must read absence as zero")
	}
	snap["a"].Reasons["veto"] = 99
	if again := r.snapshot(); again["a"].Reasons["veto"] != 2 {
		t.Errorf("editing a snapshot reached the tally: veto = %d, want 2", again["a"].Reasons["veto"])
	}
}

// TestFlashbackAPI_RoutingOff: port on, routing not configured → the routing
// block is present and says off, with none of the on-only fields, so the
// page can name the flag instead of guessing.
func TestFlashbackAPI_RoutingOff(t *testing.T) {
	s := newFlashbackStatusServer(t, "127.0.0.1:3308")
	rec := doJSON(t, s, "GET", "/api/flashback", "secret-tok")
	var got struct {
		Routing map[string]any `json:"routing"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v (%s)", err, rec.Body.String())
	}
	if got.Routing == nil || got.Routing["enabled"] != false {
		t.Fatalf("routing = %v, want {enabled:false}", got.Routing)
	}
	if len(got.Routing) != 1 {
		t.Errorf("routing off carries extra fields: %v", got.Routing)
	}
}

// TestFlashbackAPI_RoutingOn: the policy as configured, the tally since
// start, and only servers with a decision.
func TestFlashbackAPI_RoutingOn(t *testing.T) {
	s, err := New(Config{Listen: "127.0.0.1:8090", Token: "secret-tok", FlashbackListen: "127.0.0.1:3308",
		ReadRouting: ReadRoutingConfig{MaxCopyAge: 15 * time.Minute, CostThreshold: 10000, ScanRows: 100000}})
	if err != nil {
		t.Fatal(err)
	}
	s.RecordRouteDecision("srv-1", "copy", "expensive_plan")
	s.RecordRouteDecision("srv-1", "mysql", "cheap_plan")
	s.RecordRouteDecision("srv-1", "mysql", "cheap_plan")

	rec := doJSON(t, s, "GET", "/api/flashback", "secret-tok")
	if rec.Code != 200 {
		t.Fatalf("code = %d: %s", rec.Code, rec.Body.String())
	}
	var got flashbackStatusDTO
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v (%s)", err, rec.Body.String())
	}
	r := got.Routing
	if r == nil || !r.Enabled {
		t.Fatalf("routing = %+v, want enabled", r)
	}
	if r.MaxCopyAge != "15m0s" || r.CostThreshold != 10000 || r.ScanRows != 100000 {
		t.Errorf("policy = %s/%v/%d, want 15m0s/10000/100000", r.MaxCopyAge, r.CostThreshold, r.ScanRows)
	}
	since, err := time.Parse(time.RFC3339, r.Since)
	if err != nil || time.Since(since) > time.Minute || since.After(time.Now()) {
		t.Errorf("since = %q (%v), want RFC 3339 about now", r.Since, err)
	}
	if tally := r.Servers["srv-1"]; tally.Copy != 1 || tally.MySQL != 2 || tally.Reasons["cheap_plan"] != 2 || tally.Reasons["expensive_plan"] != 1 {
		t.Errorf("srv-1 = %+v, want copy 1, mysql 2, cheap_plan 2, expensive_plan 1", tally)
	}
	if len(r.Servers) != 1 {
		t.Errorf("servers = %v, want srv-1 alone", r.Servers)
	}
}

// TestFlashbackAPI_RoutingReadOnly: the port's mode travels with the policy
// (#2079), and a refused statement is counted on its own, under neither
// side, since nobody ran it.
func TestFlashbackAPI_RoutingReadOnly(t *testing.T) {
	for _, readOnly := range []bool{true, false} {
		s, err := New(Config{Listen: "127.0.0.1:8090", Token: "secret-tok", FlashbackListen: "127.0.0.1:3308",
			ReadRouting: ReadRoutingConfig{MaxCopyAge: 15 * time.Minute, CostThreshold: 10000, ScanRows: 100000, ReadOnly: readOnly}})
		if err != nil {
			t.Fatal(err)
		}
		s.RecordRouteDecision("srv-1", "mysql", "cheap_plan")
		s.RecordRouteDecision("srv-1", "refused", "read_only")
		s.RecordRouteDecision("srv-1", "refused", "read_only")
		rec := doJSON(t, s, "GET", "/api/flashback", "secret-tok")
		var got flashbackStatusDTO
		if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
			t.Fatalf("decode: %v (%s)", err, rec.Body.String())
		}
		if got.Routing == nil || got.Routing.ReadOnly != readOnly {
			t.Fatalf("read_only = %+v, want %v", got.Routing, readOnly)
		}
		if tally := got.Routing.Servers["srv-1"]; tally.MySQL != 1 || tally.Copy != 0 || tally.Refused != 2 || tally.Reasons["read_only"] != 2 {
			t.Errorf("srv-1 = %+v, want mysql 1, copy 0, refused 2, read_only 2", tally)
		}
	}
}
