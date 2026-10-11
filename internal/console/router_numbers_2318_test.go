package console

import (
	"encoding/json"
	"strings"
	"testing"
)

// #2318: the numbers a router hands over HTTP. They are served with no
// password, where the metrics are, so the texts the web interface shows a
// signed-in user (which account a source turned away, and MySQL's words)
// must not travel with them.
func TestRouterNumbers_countsAndPoolAndNoTexts(t *testing.T) {
	srv, err := New(Config{Listen: "127.0.0.1:8090", Token: "t", SQLMaxInFlight: 6, SQLWorkerStatements: 500,
		SQLLimits: resolveSQLLimitsForTest(3, "1024MiB")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.SQLSandbox().Close)
	for range 3 {
		srv.RecordRouteDecision("srv-a", "copy", "expensive_plan")
	}
	srv.RecordRouteDecision("srv-a", "mysql", "cheap_plan")
	srv.RecordRouteDecision("srv-a", "refused", "copy_queue_full")
	srv.routing.setAccountRefused("srv-a", "the forwarding account app_reader: Error 1045 Access denied")
	srv.routing.setSessionUntracked("srv-a", "the account lacks a privilege")
	// A server with a note and no statement yet.
	srv.RecordRouteUnavailable("srv-b", "it has no source address for hunter2-host")

	got := srv.RouterNumbers()
	a := got.Servers["srv-a"]
	if a.Copy != 3 || a.MySQL != 1 || a.Refused != 1 || a.Reasons["expensive_plan"] != 3 || a.Reasons["copy_queue_full"] != 1 || a.CannotRoute || !a.AccountRefused || !a.SessionUntracked {
		t.Errorf("srv-a = %+v", a)
	}
	b, ok := got.Servers["srv-b"]
	if !ok || !b.CannotRoute || b.AccountRefused || b.SessionUntracked || b.Copy+b.MySQL+b.Refused != 0 || b.Reasons == nil {
		t.Errorf("srv-b = %+v (present %v): want an entry of zeros that cannot route, with reasons not null", b, ok)
	}
	if got.Pool == nil || got.Pool.Workers != 6 || got.Pool.ThreadsEach != 3 || got.Pool.MemoryEach != "1024MiB" || got.Pool.StatementsPerWorker != 500 || got.Pool.Running != 0 || got.Pool.Waiting != 0 {
		t.Errorf("pool = %+v", got.Pool)
	}
	if got.CountingSince == "" {
		t.Error("counting_since is empty")
	}
	raw, err := json.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	for _, leak := range []string{"app_reader", "1045", "Access denied", "privilege", "hunter2-host"} {
		if strings.Contains(string(raw), leak) {
			t.Errorf("the document carries %q, a text only a signed-in user is shown:\n%s", leak, raw)
		}
	}
}

// With nothing sent and no pool, the document still has a servers object: a
// reader must be able to tell "no server yet" from a field that is missing.
func TestRouterNumbers_emptyIsAnObjectNotNull(t *testing.T) {
	srv, err := New(Config{Listen: "127.0.0.1:8090", Token: "t"})
	if err != nil {
		t.Fatal(err)
	}
	if r := srv.SQLSandbox(); r != nil {
		t.Cleanup(r.Close)
	}
	raw, err := json.Marshal(srv.RouterNumbers())
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), `"servers":{}`) {
		t.Errorf("want \"servers\":{} in %s", raw)
	}
}
