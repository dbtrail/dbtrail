package shim

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"

	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

// #2084: the two things a port that runs as a service of its own does
// differently from the one inside watch. Both are off unless asked for.

func serviceHandler(t *testing.T, r *fakeRouter, f *fakeFreeSQL, cfg RouterConfig, got *[]string) *Handler {
	t.Helper()
	h := NewHandler(nil, nil)
	h.BindFreeSQL(f)
	cfg.Observe = func(route RouteSide, reason RouteReason) {
		*got = append(*got, string(route)+"/"+string(reason))
	}
	h.BindRouter(r, cfg)
	return h
}

const expensive2084 = "SELECT a, count(*) FROM t GROUP BY a"

// With AnyCopyAge the copy is an asynchronous replica: a statement the plan
// sends to it runs there whatever the age of its snapshot, with no maximum
// set, and without the copy being asked to vouch for anything.
func TestRouter_anyCopyAge(t *testing.T) {
	for _, tc := range []struct {
		name      string
		updatedAt time.Time
		maxAge    time.Duration
	}{
		{"a fresh snapshot, no maximum set", time.Now(), 0},
		{"a snapshot a month old", time.Now().Add(-30 * 24 * time.Hour), 0},
		{"a month old, and a maximum that it passes", time.Now().Add(-30 * 24 * time.Hour), time.Minute},
		{"a snapshot time nobody could read", time.Time{}, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := &fakeRouter{toCopy: true, reason: "expensive"}
			f := &fakeFreeSQL{updatedAt: tc.updatedAt}
			var got []string
			h := serviceHandler(t, r, f, RouterConfig{AnyCopyAge: true, MaxCopyAge: tc.maxAge}, &got)
			if _, err := h.HandleQuery(expensive2084); err != nil {
				t.Fatal(err)
			}
			if len(got) != 1 || got[0] != "copy/expensive_plan" {
				t.Errorf("observed %v, want [copy/expensive_plan]", got)
			}
			if len(r.forwarded) != 0 {
				t.Errorf("forwarded to MySQL: %v", r.forwarded)
			}
			if f.calls != 1 || f.gotSess.UnchangedWithin != 0 || f.unchangedAsks != 0 {
				t.Errorf("the copy was asked %d time(s), to vouch within %v (%d asks); want once, for nothing",
					f.calls, f.gotSess.UnchangedWithin, f.unchangedAsks)
			}
			// The age is not read at all: it decides nothing, and reading
			// it lists the copy's snapshots.
			if f.ageCalls != 0 {
				t.Errorf("the copy's age was read %d time(s)", f.ageCalls)
			}
		})
	}
}

// AnyCopyAge changes the age rule and nothing above it on the ladder: a
// cheap plan, a write, a transaction and a veto are MySQL's as before.
func TestRouter_anyCopyAgeLeavesTheOtherRungs(t *testing.T) {
	for _, tc := range []struct {
		name, stmt, want string
		router           fakeRouter
	}{
		{"a cheap plan", "SELECT a FROM t WHERE id = 1", "mysql/cheap_plan", fakeRouter{toCopy: false}},
		{"a write", "UPDATE t SET a = 1", "mysql/write", fakeRouter{toCopy: true}},
		{"in a transaction", expensive2084, "mysql/in_transaction", fakeRouter{toCopy: true, inTxn: true}},
		{"a veto", "SELECT a, NOW() FROM t GROUP BY a", "mysql/veto", fakeRouter{toCopy: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := &fakeFreeSQL{updatedAt: time.Now()}
			var got []string
			h := serviceHandler(t, &tc.router, f, RouterConfig{AnyCopyAge: true}, &got)
			if _, err := h.HandleQuery(tc.stmt); err != nil {
				t.Fatal(err)
			}
			if len(got) != 1 || got[0] != tc.want || f.calls != 0 {
				t.Errorf("observed %v with %d copy runs, want [%s] and none", got, f.calls, tc.want)
			}
		})
	}
}

// Without AnyCopyAge, no maximum still means routing to the copy is off.
func TestRouter_noMaxAgeAloneIsStillRoutingOff(t *testing.T) {
	r := &fakeRouter{toCopy: true}
	f := &fakeFreeSQL{updatedAt: time.Now()}
	var got []string
	h := serviceHandler(t, r, f, RouterConfig{}, &got)
	if _, err := h.HandleQuery(expensive2084); err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != "mysql/routing_off" || f.calls != 0 {
		t.Errorf("observed %v with %d copy runs, want [mysql/routing_off] and none", got, f.calls)
	}
}

// With BusyRefuses a copy that is asked for more than it serves answers the
// client MySQL's error 1040, the one a full server gives and every driver
// retries, and the source is not handed the statement.
func TestRouter_busyRefusesWith1040(t *testing.T) {
	for _, tc := range []struct {
		name    string
		copyErr error
		want    string
	}{
		{"the line was full on arrival", sqlsandbox.ErrBusy, "refused/copy_queue_full"},
		{"waited the whole wait", &sqlsandbox.BusyError{Waited: 30 * time.Second, MaxInFlight: 8}, "refused/copy_wait_timeout"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := &fakeRouter{toCopy: true, reason: "expensive"}
			f := &fakeFreeSQL{err: tc.copyErr, updatedAt: time.Now()}
			var got []string
			h := serviceHandler(t, r, f, RouterConfig{AnyCopyAge: true, BusyRefuses: true}, &got)
			_, err := h.HandleQuery(expensive2084)
			var me *mysql.MyError
			if !errors.As(err, &me) || me.Code != mysql.ER_CON_COUNT_ERROR || me.State != "08004" {
				t.Fatalf("err = %v, want MySQL error 1040 with SQLSTATE 08004", err)
			}
			if !strings.Contains(me.Message, "busy") {
				t.Errorf("the message does not say why: %q", me.Message)
			}
			if len(got) != 1 || got[0] != tc.want {
				t.Errorf("observed %v, want exactly [%s]", got, tc.want)
			}
			if len(r.forwarded) != 0 {
				t.Errorf("the statement the copy had no room for went to MySQL: %v", r.forwarded)
			}
		})
	}
}

// BusyRefuses is about a busy copy only. A statement the copy cannot run, a
// caller that left and a port that holds its results are what they were.
func TestRouter_busyRefusesOnlyABusyCopy(t *testing.T) {
	for _, tc := range []struct {
		name    string
		copyErr error
		want    string
	}{
		{"a fault of the copy", errors.New("Binder Error"), "mysql/copy_refused"},
		{"cancelled while waiting", context.Canceled, "mysql/copy_refused"},
		{"the port holds its results", &ResultsHeldError{}, "mysql/copy_results_held"},
		{"the copy's columns differ", &sqlsandbox.ColumnsDifferError{Reason: "a star"}, "mysql/copy_columns_differ"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := &fakeRouter{toCopy: true, reason: "expensive"}
			f := &fakeFreeSQL{err: tc.copyErr, updatedAt: time.Now()}
			var got []string
			h := serviceHandler(t, r, f, RouterConfig{AnyCopyAge: true, BusyRefuses: true}, &got)
			if _, err := h.HandleQuery(expensive2084); err != nil {
				t.Fatalf("reached the client as an error: %v", err)
			}
			if len(got) != 1 || got[0] != tc.want || len(r.forwarded) != 1 {
				t.Errorf("observed %v, forwarded %v; want [%s] and the statement once", got, r.forwarded, tc.want)
			}
		})
	}
}

// The same ladder serves a prepared statement's executions.
func TestPreparedRouted_serviceOptions(t *testing.T) {
	const stmt = "SELECT a, count(*) FROM t WHERE b > ? GROUP BY a"
	r := &fakeRouter{toCopy: true}
	f := &fakeFreeSQL{updatedAt: time.Now().Add(-30 * 24 * time.Hour)}
	var got []string
	h := serviceHandler(t, r, f, RouterConfig{AnyCopyAge: true, BusyRefuses: true}, &got)
	_, _, ctx, err := h.HandleStmtPrepare(stmt)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.HandleStmtExecute(ctx, "", []any{int64(1)}); err != nil {
		t.Fatalf("execute on a month-old copy: %v", err)
	}
	if got[len(got)-1] != "copy/expensive_plan" {
		t.Errorf("observed %v, want the execution under copy/expensive_plan", got)
	}
	f.err = sqlsandbox.ErrBusy
	_, err = h.HandleStmtExecute(ctx, "", []any{int64(1)})
	var me *mysql.MyError
	if !errors.As(err, &me) || me.Code != mysql.ER_CON_COUNT_ERROR {
		t.Fatalf("execute on a busy copy: %v, want MySQL error 1040", err)
	}
	if got[len(got)-1] != "refused/copy_queue_full" {
		t.Errorf("observed %v, want the execution under refused/copy_queue_full", got)
	}
}

// SHOW ROUTER STATUS is the port's own statement where a Status was bound:
// answered from it, never sent to the source, and not a routing decision.
func TestRouter_showRouterStatus(t *testing.T) {
	asked := 0
	status := func(context.Context) [][2]string {
		asked++
		return [][2]string{{"copy_age_seconds", "42"}, {"statements_copy", "7"}}
	}
	for _, stmt := range []string{"SHOW ROUTER STATUS", "show router status", "  Show   Router\tStatus ; "} {
		r := &fakeRouter{toCopy: true}
		f := &fakeFreeSQL{updatedAt: time.Now()}
		var got []string
		h := serviceHandler(t, r, f, RouterConfig{AnyCopyAge: true, Status: status}, &got)
		res, err := h.HandleQuery(stmt)
		if err != nil {
			t.Fatalf("%q: %v", stmt, err)
		}
		if res.Resultset == nil || len(res.Resultset.Fields) != 2 || string(res.Resultset.Fields[0].Name) != "Variable_name" || string(res.Resultset.Fields[1].Name) != "Value" {
			t.Fatalf("%q: columns %+v", stmt, res.Resultset)
		}
		var rows []string
		for _, row := range textRows(t, res.Resultset) {
			rows = append(rows, row[0]+"="+row[1])
		}
		if strings.Join(rows, ",") != "copy_age_seconds=42,statements_copy=7" {
			t.Errorf("%q: rows %v", stmt, rows)
		}
		if len(r.forwarded) != 0 || len(got) != 0 || f.calls != 0 {
			t.Errorf("%q: forwarded %v, observed %v, copy runs %d; want none of each", stmt, r.forwarded, got, f.calls)
		}
	}
	if asked != 3 {
		t.Errorf("the status was asked for %d times, want once per statement", asked)
	}
}

// Anything that is not exactly that statement, and that statement on a
// connection with no Status, goes on as any other: to MySQL, which answers it.
func TestRouter_showRouterStatusOnlyWhereBoundAndOnlyThatStatement(t *testing.T) {
	status := func(context.Context) [][2]string { return [][2]string{{"a", "b"}} }
	for _, tc := range []struct {
		name, stmt string
		cfg        RouterConfig
	}{
		{"no Status bound", "SHOW ROUTER STATUS", RouterConfig{AnyCopyAge: true}},
		{"another SHOW", "SHOW ROUTER STATUSES", RouterConfig{AnyCopyAge: true, Status: status}},
		{"with a filter", "SHOW ROUTER STATUS LIKE 'copy%'", RouterConfig{AnyCopyAge: true, Status: status}},
		{"inside something else", "SELECT 'SHOW ROUTER STATUS'", RouterConfig{AnyCopyAge: true, Status: status}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := &fakeRouter{}
			f := &fakeFreeSQL{updatedAt: time.Now()}
			var got []string
			h := serviceHandler(t, r, f, tc.cfg, &got)
			if _, err := h.HandleQuery(tc.stmt); err != nil {
				t.Fatal(err)
			}
			if len(r.forwarded) != 1 || r.forwarded[0] != tc.stmt {
				t.Errorf("forwarded %v, want the statement, to MySQL", r.forwarded)
			}
		})
	}
	// With no rows to give, the answer is still a result with one row that
	// says so, not a malformed one.
	h := serviceHandler(t, &fakeRouter{}, &fakeFreeSQL{}, RouterConfig{AnyCopyAge: true, Status: func(context.Context) [][2]string { return nil }}, new([]string))
	res, err := h.HandleQuery("SHOW ROUTER STATUS")
	if err != nil || len(textRows(t, res.Resultset)) != 1 {
		t.Errorf("a Status with nothing: %v, %v", res, err)
	}
}
