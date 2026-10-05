package shim

import (
	"errors"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

// A statement whose plan says it returns more rows than the copy's row cap
// is not tried on the copy (#2115): the copy would run it, refuse the
// result for its size, and MySQL would run it again. The plan's estimate
// comes with the decision (readrouter.Decision.ResultRows), the cap from
// the copy (FreeSQL.RowCap), and the connection's own sql_select_limit from
// the source's session: at or under the cap, the copy cuts the result there
// as the client asked, so it is the copy's as before.
func TestRouter_resultOverTheRowCap_2115(t *testing.T) {
	type obs struct {
		side   RouteSide
		reason RouteReason
	}
	const stmt = "SELECT id FROM orders WHERE id > 1900000"
	for _, tc := range []struct {
		name       string
		resultRows int64
		rowCap     int
		source     func(*fakeRouter)
		want       obs
		// the copy's snapshot time is read only for a statement that may
		// run there: the over-cap rule comes before it.
		ageCalls int
	}{
		{"over the cap", 5000, 1000, nil, obs{RouteMySQL, RouteReasonResultOverCap}, 0},
		{"one row over", 1001, 1000, nil, obs{RouteMySQL, RouteReasonResultOverCap}, 0},
		{"exactly the cap", 1000, 1000, nil, obs{RouteCopy, RouteReasonExpensivePlan}, 1},
		{"under the cap", 999, 1000, nil, obs{RouteCopy, RouteReasonExpensivePlan}, 1},
		{"no estimate", 0, 1000, nil, obs{RouteCopy, RouteReasonExpensivePlan}, 1},
		{"a copy that names no cap", 5000, 0, nil, obs{RouteCopy, RouteReasonExpensivePlan}, 1},
		{"a lower cap on this server", 500, 200, nil, obs{RouteMySQL, RouteReasonResultOverCap}, 0},
		{"sql_select_limit under the cap: the copy cuts there", 5000, 1000,
			func(r *fakeRouter) { r.src = stockSource(); r.src.limit = "100" }, obs{RouteCopy, RouteReasonExpensivePlan}, 1},
		{"sql_select_limit at the cap", 5000, 1000,
			func(r *fakeRouter) { r.src = stockSource(); r.src.limit = "1000" }, obs{RouteCopy, RouteReasonExpensivePlan}, 1},
		{"sql_select_limit over the cap", 5000, 1000,
			func(r *fakeRouter) { r.src = stockSource(); r.src.limit = "1001" }, obs{RouteMySQL, RouteReasonResultOverCap}, 0},
		// The session rungs keep their own reasons: this rule only acts on
		// a session it could read and the copy runs under.
		{"a session the copy does not reproduce", 5000, 1000,
			func(r *fakeRouter) { r.src = stockSource(); r.src.zone = "+05:30" }, obs{RouteMySQL, RouteReasonSessionDiffers}, 1},
		{"a session that cannot be read", 5000, 1000,
			func(r *fakeRouter) { r.sessionErr = errors.New("boom") }, obs{RouteMySQL, RouteReasonSessionDiffers}, 1},
	} {
		for _, prepared := range []bool{false, true} {
			r := &fakeRouter{toCopy: true, reason: "plan cost 30000 >= 10000", resultRows: tc.resultRows}
			if tc.source != nil {
				tc.source(r)
			}
			f := &fakeFreeSQL{res: oneCell("side", "VARCHAR", "copy"), updatedAt: time.Now(), rowCap: tc.rowCap}
			h := NewHandler(nil, nil)
			h.BindFreeSQL(f)
			var seen []obs
			h.BindRouter(r, RouterConfig{MaxCopyAge: time.Minute, Observe: func(side RouteSide, reason RouteReason) {
				seen = append(seen, obs{side, reason})
			}})
			name := tc.name
			var err error
			if prepared {
				name += " (prepared)"
				var ctx any
				if _, _, ctx, err = h.HandleStmtPrepare(stmt); err == nil {
					_, err = h.HandleStmtExecute(ctx, "", nil)
				}
			} else {
				_, err = h.HandleQuery(stmt)
			}
			if err != nil {
				t.Errorf("%s: %v", name, err)
				continue
			}
			if len(seen) != 1 || seen[0] != tc.want {
				t.Errorf("%s: observed %v, want one %v", name, seen, tc.want)
			}
			wantCopy := 0
			if tc.want.side == RouteCopy {
				wantCopy = 1
			}
			if f.calls != wantCopy {
				t.Errorf("%s: the copy ran %d statements, want %d", name, f.calls, wantCopy)
			}
			if f.ageCalls != tc.ageCalls {
				t.Errorf("%s: the copy's snapshot time was read %d times, want %d", name, f.ageCalls, tc.ageCalls)
			}
			// MySQL ran it once, or not at all: never after a try on the copy.
			wantForwarded := 1 - wantCopy
			if got := len(r.forwarded) + executions(r); got != wantForwarded {
				t.Errorf("%s: MySQL ran %d statements, want %d", name, got, wantForwarded)
			}
		}
	}
	// The copy that cut its result is still a refusal, as before: the rule
	// above is about not getting there.
	r := &fakeRouter{toCopy: true, reason: "expensive"}
	f := &fakeFreeSQL{res: sqlsandbox.Result{Columns: []sqlsandbox.Column{{Name: "side", Type: "VARCHAR"}}, Rows: [][]any{{"copy"}}, Truncated: true}, updatedAt: time.Now(), rowCap: 1000}
	h := routingHandler(t, r, f, time.Minute)
	if res, err := h.HandleQuery(stmt); err != nil || firstCell(t, res) != "mysql" || f.calls != 1 {
		t.Errorf("a result the copy cut: err %v, the copy ran %d statements; want MySQL's answer after one try", err, f.calls)
	}
}

// executions is how many times the fake source executed a prepared statement.
func executions(r *fakeRouter) int {
	n := 0
	for _, s := range r.prepared {
		n += len(s.executed)
	}
	return n
}
