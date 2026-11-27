package console

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

// The copy's answers in a join order picked by the shape of their FROM are
// counted per server under join_orders once the port commits them as the
// answer (NoteRoute "copy"), apart from the routing reasons; a run the port
// threw away (NoteRoute "mysql") is not, and a server with none has no such
// key.
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
	q := &SQLOnCopy{s: srv, id: "a"}
	answer := func(order, side string) {
		q.notePendingRun(sqlOutcome{Result: sqlsandbox.Result{JoinOrderByShape: order}}, time.Now())
		q.NoteRoute(side)
	}
	answer(sqlsandbox.JoinOrderByShapeWritten, "copy")
	answer(sqlsandbox.JoinOrderByShapeWritten, "copy")
	answer(sqlsandbox.JoinOrderByShapeConnected, "copy")
	answer(sqlsandbox.JoinOrderByShapeFailed, "copy")
	answer("", "copy")
	answer(sqlsandbox.JoinOrderByShapeConnected, "mysql") // thrown away
	answer(sqlsandbox.JoinOrderByShapeConnected, "refused")
	got := srv.RouterNumbers().Servers
	if j := got["a"].JoinOrders; len(j) != 3 || j["written"] != 2 || j["connected"] != 1 || j["failed"] != 1 {
		t.Errorf("a: %v", j)
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
	if !strings.Contains(string(raw), `"join_orders":{"connected":1,"failed":1,"written":2}`) {
		t.Errorf("a on the wire: %s", raw)
	}
}
