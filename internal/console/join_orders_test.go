package console

import (
	"encoding/json"
	"strings"
	"testing"
)

// The copy's answers in a join order picked by the shape of their FROM are
// counted per server under join_orders, apart from the routing reasons, and
// a server with none has no such key.
func TestRouterNumbers_joinOrders(t *testing.T) {
	srv, err := New(Config{Listen: "127.0.0.1:8090", Token: "t"})
	if err != nil {
		t.Fatal(err)
	}
	if r := srv.SQLSandbox(); r != nil {
		t.Cleanup(r.Close)
	}
	srv.RecordRouteDecision("a", "copy", "expensive_plan")
	srv.RecordRouteDecision("b", "copy", "expensive_plan")
	for _, order := range []string{"written", "written", "connected", ""} {
		srv.routing.joinOrderByShape("a", order)
	}
	got := srv.RouterNumbers().Servers
	if want := map[string]uint64{"written": 2, "connected": 1}; len(got["a"].JoinOrders) != 2 ||
		got["a"].JoinOrders["written"] != want["written"] || got["a"].JoinOrders["connected"] != want["connected"] {
		t.Errorf("a: %v", got["a"].JoinOrders)
	}
	if got["a"].Reasons["expensive_plan"] != 1 || len(got["a"].Reasons) != 1 {
		t.Errorf("the reasons changed: %v", got["a"].Reasons)
	}
	raw, err := json.Marshal(got["b"])
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "join_orders") {
		t.Errorf("b has join_orders: %s", raw)
	}
	raw, _ = json.Marshal(got["a"])
	if !strings.Contains(string(raw), `"join_orders":{"connected":1,"written":2}`) {
		t.Errorf("a on the wire: %s", raw)
	}
}
