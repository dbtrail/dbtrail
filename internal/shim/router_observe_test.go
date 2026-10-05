package shim

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"

	"github.com/dbtrail/dbtrail/internal/readrouter"
)

// observedHandler is routingHandler with a recording Observe: every decision
// lands in *got as "route/reason".
func observedHandler(t *testing.T, r *fakeRouter, f *fakeFreeSQL, maxAge time.Duration, got *[]string) *Handler {
	t.Helper()
	h := NewHandler(nil, nil)
	h.BindFreeSQL(f)
	h.BindRouter(r, RouterConfig{MaxCopyAge: maxAge, Observe: func(route RouteSide, reason RouteReason) {
		*got = append(*got, string(route)+"/"+string(reason))
	}})
	return h
}

// TestRouter_observesEveryRungOnce: each rung of the ladder reports exactly
// one decision, under its closed-vocabulary reason — the contract the metric
// and the console tally are built on. A rung that forgot to observe, or
// observed twice, would undercount or double-count that side.
func TestRouter_observesEveryRungOnce(t *testing.T) {
	fresh := time.Now()
	cases := []struct {
		name    string
		stmt    string
		router  fakeRouter
		copyErr error
		copyAge time.Time
		maxAge  time.Duration
		want    string
	}{
		{"not a select", "SHOW TABLES", fakeRouter{toCopy: true}, nil, fresh, time.Minute, "mysql/not_a_select"},
		{"begin", "BEGIN", fakeRouter{toCopy: true}, nil, fresh, time.Minute, "mysql/not_a_select"},
		{"use as a statement", "USE shop", fakeRouter{toCopy: true}, nil, fresh, time.Minute, "mysql/not_a_select"},
		{"a write", "UPDATE t SET a = 1", fakeRouter{toCopy: true}, nil, fresh, time.Minute, "mysql/write"},
		{"a set", "SET time_zone = '+00:00'", fakeRouter{toCopy: true}, nil, fresh, time.Minute, "mysql/session_setting"},
		{"a harmless set", "SET NAMES utf8mb4", fakeRouter{toCopy: true}, nil, fresh, time.Minute, "mysql/session_setting"},
		{"vetoed", "SELECT NOW()", fakeRouter{toCopy: true}, nil, fresh, time.Minute, "mysql/veto"},
		{"in transaction", "SELECT count(*) FROM t", fakeRouter{toCopy: true, inTxn: true}, nil, fresh, time.Minute, "mysql/in_transaction"},
		{"routing off", "SELECT count(*) FROM t", fakeRouter{toCopy: true}, nil, fresh, 0, "mysql/routing_off"},
		{"explain failed", "SELECT * FROM t WHERE id = 1", fakeRouter{decideErr: errors.New("boom")}, nil, fresh, time.Minute, "mysql/explain_failed"},
		{"cheap plan", "SELECT * FROM t WHERE id = 1", fakeRouter{toCopy: false, reason: "cheap"}, nil, fresh, time.Minute, "mysql/cheap_plan"},
		{"bounded limit", "SELECT id FROM t ORDER BY id DESC LIMIT 2", fakeRouter{toCopy: false, reason: "LIMIT 2", rule: readrouter.RuleBoundedLimit}, nil, fresh, time.Minute, "mysql/bounded_limit"},
		{"copy age unknown", "SELECT count(*) FROM t", fakeRouter{toCopy: true}, nil, time.Time{}, time.Minute, "mysql/copy_age_unknown"},
		{"copy too old", "SELECT count(*) FROM t", fakeRouter{toCopy: true}, nil, time.Now().Add(-time.Hour), time.Minute, "mysql/copy_too_old"},
		{"copy refused", "SELECT a, count(*) FROM t GROUP BY a", fakeRouter{toCopy: true}, errors.New("Binder Error"), fresh, time.Minute, "mysql/copy_refused"},
		{"expensive plan", "SELECT a, count(*) FROM t GROUP BY a", fakeRouter{toCopy: true, reason: "plan cost 20146 >= 10000"}, nil, fresh, time.Minute, "copy/expensive_plan"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := tc.router
			f := &fakeFreeSQL{res: oneCell("side", "VARCHAR", "copy"), err: tc.copyErr, updatedAt: tc.copyAge}
			var got []string
			h := observedHandler(t, &r, f, tc.maxAge, &got)
			if _, err := h.HandleQuery(tc.stmt); err != nil {
				t.Fatalf("HandleQuery: %v", err)
			}
			if len(got) != 1 || got[0] != tc.want {
				t.Errorf("observed %v, want exactly [%s]", got, tc.want)
			}
		})
	}
}

// The two decisions that depend on what came BEFORE on the connection: a
// SELECT after a LOCK TABLES (connection_pinned), and SHOW WARNINGS after a
// forwarded statement (show_warnings); SHOW WARNINGS after a copy-served
// statement is answered locally and is no routing decision at all.
func TestRouter_observesConnectionStateReasons(t *testing.T) {
	r := &fakeRouter{toCopy: true, reason: "expensive"}
	f := &fakeFreeSQL{res: oneCell("side", "VARCHAR", "copy"), updatedAt: time.Now()}
	var got []string
	h := observedHandler(t, r, f, time.Minute, &got)
	for _, q := range []string{
		"SELECT a, count(*) FROM t GROUP BY a", // copy
		"SHOW WARNINGS",                        // ours: not observed
		"SELECT * FROM t WHERE id = 1",         // cheap → mysql (fake says toCopy, but the ladder is what we test)
		"SHOW WARNINGS",                        // after a MySQL statement → forwarded
		"SET time_zone = '+00:00'",
		"SELECT a, count(*) FROM t GROUP BY a", // a SET does not pin: the copy again
		"LOCK TABLES t READ",
		"SELECT a, count(*) FROM t GROUP BY a", // pinned by the lock
	} {
		r.toCopy = !strings.HasPrefix(q, "SELECT * FROM t WHERE")
		if _, err := h.HandleQuery(q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	want := []string{"copy/expensive_plan", "mysql/cheap_plan", "mysql/show_warnings", "mysql/session_setting", "copy/expensive_plan", "mysql/session_setting", "mysql/connection_pinned"}
	if strings.Join(got, " ") != strings.Join(want, " ") {
		t.Errorf("observed\n  %v\nwant\n  %v", got, want)
	}
}

// A forwarded statement MySQL then fails is still a MySQL decision: the
// tally counts decisions, and the client got MySQL's error. A statement
// NOBODY answered — the port's upstream connection is lost, every forward
// fails with 2006 — is its own reason, whatever rung sent it there:
// counting it under the rung's reason would read as "MySQL answered".
func TestRouter_observesForwardFailuresAsMySQL(t *testing.T) {
	r := &fakeRouter{toCopy: false, reason: "cheap"}
	f := &fakeFreeSQL{res: oneCell("side", "VARCHAR", "copy"), updatedAt: time.Now()}
	var got []string
	h := observedHandler(t, r, f, time.Minute, &got)
	r.forwardErr = errors.New("ERROR 1146: table nope doesn't exist")
	if _, err := h.HandleQuery("SELECT * FROM nope WHERE id = 1"); err == nil {
		t.Fatal("MySQL's error did not reach the client")
	}
	if len(got) != 1 || got[0] != "mysql/cheap_plan" {
		t.Errorf("observed %v, want [mysql/cheap_plan]", got)
	}

	got = nil
	r.forwardErr = mysql.NewError(readrouter.CodeUpstreamLost, "the connection to the source was lost")
	for _, q := range []string{"SELECT * FROM t WHERE id = 1", "UPDATE t SET a = 1", "SHOW TABLES"} {
		if _, err := h.HandleQuery(q); err == nil {
			t.Fatalf("%s: the 2006 did not reach the client", q)
		}
	}
	want := []string{"mysql/upstream_lost", "mysql/upstream_lost", "mysql/upstream_lost"}
	if strings.Join(got, " ") != strings.Join(want, " ") {
		t.Errorf("observed %v, want %v", got, want)
	}
}
