package console

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

type sqlLimitsForTest = sqlsandbox.Limits

// #2084: what SHOW ROUTER STATUS answers, read off a console that counted a
// few decisions for one server and none for another.
func TestRouterStatus_numbersOfThisServerAndThePool(t *testing.T) {
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
	srv.RecordRouteDecision("srv-a", "mysql", "write")
	srv.RecordRouteDecision("srv-a", "refused", "copy_queue_full")
	srv.RecordRouteDecision("srv-b", "copy", "expensive_plan")

	got := map[string]string{}
	var order []string
	for _, p := range srv.RouterStatus(context.Background(), "srv-a", nil) {
		if _, dup := got[p[0]]; dup {
			t.Errorf("%s is reported twice", p[0])
		}
		got[p[0]] = p[1]
		order = append(order, p[0])
	}
	for name, want := range map[string]string{
		"server": "srv-a", "copy_snapshot": "unknown", "copy_age_seconds": "unknown",
		"statements_copy": "3", "statements_mysql": "2", "statements_refused": "1",
		"reason_expensive_plan": "3", "reason_cheap_plan": "1", "reason_write": "1", "reason_copy_queue_full": "1",
		"pool_workers": "6", "pool_running": "0", "pool_waiting": "0", "pool_threads_each": "3", "pool_memory_each": "1024MiB",
		"pool_worker_processes_started": "0", "pool_statements_per_worker": "500",
	} {
		if got[name] != want {
			t.Errorf("%s = %q, want %q", name, got[name], want)
		}
	}
	if since, err := time.Parse(time.RFC3339, got["counting_since"]); err != nil || time.Since(since) > time.Minute {
		t.Errorf("counting_since = %q", got["counting_since"])
	}
	// The copy's age comes first after the server's name, and the reasons
	// in a fixed order.
	if order[1] != "copy_snapshot" || order[2] != "copy_age_seconds" {
		t.Errorf("order: %v", order)
	}
	reasons := strings.Join(order, ",")
	if !strings.Contains(reasons, "reason_cheap_plan,reason_copy_queue_full,reason_expensive_plan,reason_write") {
		t.Errorf("the reasons are not sorted: %v", order)
	}
	// A server nothing was counted for reports zeros, not another's.
	for _, p := range srv.RouterStatus(context.Background(), "srv-c", nil) {
		if strings.HasPrefix(p[0], "statements_") && p[1] != "0" || strings.HasPrefix(p[0], "reason_") {
			t.Errorf("a server with no statements reports %s = %s", p[0], p[1])
		}
	}
}

func resolveSQLLimitsForTest(threads int, memory string) (l sqlLimitsForTest) {
	l.Threads, l.MemoryLimit = threads, memory
	return l
}
