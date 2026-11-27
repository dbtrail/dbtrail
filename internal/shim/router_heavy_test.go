package shim

import (
	"errors"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/readrouter"
	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

// heavyRun is one statement through a routing handler, what each side was
// asked and what was counted.
type heavyRun struct {
	err       error
	observed  []string
	answered  []string
	copyCalls int
	explained int
	mysqlRan  int
	sess      sqlsandbox.Session
}

func runHeavy(t *testing.T, stmt string, prepared bool, r *fakeRouter, f *fakeFreeSQL) heavyRun {
	t.Helper()
	h := NewHandler(nil, nil)
	h.BindFreeSQL(f)
	var run heavyRun
	h.BindRouter(r, RouterConfig{MaxCopyAge: time.Minute,
		Observe: func(side RouteSide, reason RouteReason) {
			run.observed = append(run.observed, string(side)+"/"+string(reason))
		},
		Answered: func(side RouteSide, reason RouteReason, _ string, _ time.Duration, _, _ bool) {
			run.answered = append(run.answered, string(side)+"/"+string(reason))
		},
	})
	if prepared {
		var ctx any
		if _, _, ctx, run.err = h.HandleStmtPrepare(stmt); run.err == nil {
			_, run.err = h.HandleStmtExecute(ctx, "", nil)
		}
	} else {
		_, run.err = h.HandleQuery(stmt)
	}
	run.copyCalls, run.sess = f.calls, f.gotSess
	run.explained = len(r.explained)
	for _, s := range r.prepared {
		run.explained += len(s.decided)
	}
	run.mysqlRan = len(r.forwarded) + executions(r)
	return run
}

func heavyPlan() *fakeRouter { return &fakeRouter{toCopy: true, reason: "plan cost 30000 >= 10000"} }

func copyAnswer() *fakeFreeSQL {
	return &fakeFreeSQL{res: oneCell("side", "VARCHAR", "copy"), updatedAt: time.Now()}
}

// An expensive statement sent as text with a veto that is only how the
// copy computes is the copy's, counted under heavy_on_copy by both hooks;
// prepared, it is MySQL's under the veto, and its plan is not asked for.
func TestRouter_heavyOnCopy_vetoRelaxed(t *testing.T) {
	const stmt = "SELECT a FROM t WHERE b LIKE 'x%'"
	run := runHeavy(t, stmt, false, heavyPlan(), copyAnswer())
	if run.err != nil || run.copyCalls != 1 || run.mysqlRan != 0 || run.explained != 1 {
		t.Fatalf("text: err %v, copy %d, mysql %d, plans %d; want the copy's answer after one plan", run.err, run.copyCalls, run.mysqlRan, run.explained)
	}
	if len(run.observed) != 1 || run.observed[0] != "copy/heavy_on_copy" || len(run.answered) != 1 || run.answered[0] != "copy/heavy_on_copy" {
		t.Errorf("text: observed %v, answered %v; want copy/heavy_on_copy in both", run.observed, run.answered)
	}
	if !run.sess.HeavyOnCopy || !run.sess.StrictStar {
		t.Errorf("text: the copy was asked with %+v; want HeavyOnCopy and StrictStar", run.sess)
	}

	run = runHeavy(t, stmt, true, heavyPlan(), copyAnswer())
	if run.err != nil || run.copyCalls != 0 || run.mysqlRan != 1 || run.explained != 0 {
		t.Fatalf("prepared: err %v, copy %d, mysql %d, plans %d; want MySQL's answer, no plan", run.err, run.copyCalls, run.mysqlRan, run.explained)
	}
	if len(run.observed) != 1 || run.observed[0] != "mysql/veto" {
		t.Errorf("prepared: observed %v, want mysql/veto", run.observed)
	}
}

// Whatever keeps the relaxed veto's statement off the copy, it is MySQL's
// under the veto: a cheap plan, a small LIMIT, a plan that could not be had,
// a session time zone other than UTC.
func TestRouter_heavyOnCopy_vetoStands(t *testing.T) {
	const stmt = "SELECT a FROM t WHERE b LIKE 'x%'"
	for _, c := range []struct {
		name   string
		router func() *fakeRouter
		plans  int
	}{
		{"cheap", func() *fakeRouter { return &fakeRouter{reason: "plan cost 10 < 10000"} }, 1},
		{"a small LIMIT", func() *fakeRouter { return &fakeRouter{reason: "limit 5", rule: readrouter.RuleBoundedLimit} }, 1},
		{"no plan", func() *fakeRouter { return &fakeRouter{toCopy: true, decideErr: errors.New("explain: 1064")} }, 1},
		{"a session zone", func() *fakeRouter { r := heavyPlan(); r.src = stockSource(); r.src.zone = "+03:00"; return r }, 1},
	} {
		run := runHeavy(t, stmt, false, c.router(), copyAnswer())
		if run.err != nil || run.copyCalls != 0 || run.mysqlRan != 1 || run.explained != c.plans {
			t.Errorf("%s: err %v, copy %d, mysql %d, plans %d; want MySQL's answer after %d plan(s)", c.name, run.err, run.copyCalls, run.mysqlRan, run.explained, c.plans)
		}
		if len(run.observed) != 1 || run.observed[0] != "mysql/veto" {
			t.Errorf("%s: observed %v, want mysql/veto", c.name, run.observed)
		}
	}
	// A zone the copy reproduces is UTC: +00:00.
	r := heavyPlan()
	r.src = stockSource()
	r.src.zone = "+00:00"
	if run := runHeavy(t, stmt, false, r, copyAnswer()); len(run.observed) != 1 || run.observed[0] != "copy/heavy_on_copy" {
		t.Errorf("+00:00: observed %v, want copy/heavy_on_copy", run.observed)
	}
}

// A veto that is not only how the copy computes keeps the statement on
// MySQL without its plan, whichever veto the list names first.
func TestRouter_heavyOnCopy_hardVeto(t *testing.T) {
	for _, stmt := range []string{
		"SELECT a || b FROM t WHERE c LIKE 'x%'",
		"SELECT CONCAT(a, b) FROM t WHERE c = @v",
		"SELECT a FROM t WHERE b LIKE 'x%' FOR UPDATE",
	} {
		run := runHeavy(t, stmt, false, heavyPlan(), copyAnswer())
		if run.copyCalls != 0 || run.mysqlRan != 1 || run.explained != 0 {
			t.Errorf("%s: copy %d, mysql %d, plans %d; want MySQL's answer, no plan", stmt, run.copyCalls, run.mysqlRan, run.explained)
		}
		if len(run.observed) != 1 || run.observed[0] != "mysql/veto" {
			t.Errorf("%s: observed %v, want mysql/veto", stmt, run.observed)
		}
	}
}

// The copy that fails on a relaxed statement hands it to MySQL, as any
// expensive one; a transaction keeps it on MySQL before its plan.
func TestRouter_heavyOnCopy_copyFailsAndTransaction(t *testing.T) {
	const stmt = "SELECT a FROM t WHERE b LIKE 'x%'"
	f := copyAnswer()
	f.err = errors.New("Binder Error: no such function")
	run := runHeavy(t, stmt, false, heavyPlan(), f)
	if run.err != nil || run.copyCalls != 1 || run.mysqlRan != 1 {
		t.Errorf("copy fails: err %v, copy %d, mysql %d; want MySQL's answer after one try", run.err, run.copyCalls, run.mysqlRan)
	}
	if len(run.observed) != 1 || run.observed[0] != "mysql/copy_refused" || len(run.answered) != 1 || run.answered[0] != "mysql/copy_refused" {
		t.Errorf("copy fails: observed %v, answered %v; want mysql/copy_refused", run.observed, run.answered)
	}
	r := heavyPlan()
	r.inTxn = true
	if run := runHeavy(t, stmt, false, r, copyAnswer()); len(run.observed) != 1 || run.observed[0] != "mysql/in_transaction" || run.explained != 0 {
		t.Errorf("in a transaction: observed %v, plans %d; want mysql/in_transaction with no plan", run.observed, run.explained)
	}
}

// With no veto, an expensive statement the copy answered past one of its
// own refusals (Result.Waived) is heavy_on_copy; one it answered with none
// is expensive_plan, as before. HeavyOnCopy reaches the copy for text only.
func TestRouter_heavyOnCopy_copyWaived(t *testing.T) {
	const stmt = "SELECT a, SUM(n) FROM t GROUP BY a"
	f := copyAnswer()
	f.res.Waived = "the copy does not answer this statement: it puts in order a string that holds a sign"
	run := runHeavy(t, stmt, false, heavyPlan(), f)
	if len(run.observed) != 1 || run.observed[0] != "copy/heavy_on_copy" || len(run.answered) != 1 || run.answered[0] != "copy/heavy_on_copy" {
		t.Errorf("waived: observed %v, answered %v; want copy/heavy_on_copy", run.observed, run.answered)
	}
	run = runHeavy(t, stmt, false, heavyPlan(), copyAnswer())
	if len(run.observed) != 1 || run.observed[0] != "copy/expensive_plan" || !run.sess.HeavyOnCopy {
		t.Errorf("nothing waived: observed %v, HeavyOnCopy %v; want copy/expensive_plan, asked with HeavyOnCopy", run.observed, run.sess.HeavyOnCopy)
	}
	run = runHeavy(t, stmt, true, heavyPlan(), copyAnswer())
	if len(run.observed) != 1 || run.observed[0] != "copy/expensive_plan" || run.sess.HeavyOnCopy || !run.sess.StrictStar {
		t.Errorf("prepared: observed %v, session %+v; want copy/expensive_plan, StrictStar without HeavyOnCopy", run.observed, run.sess)
	}
}

// What one statement was let past is not the next one's: the state is the
// statement's.
func TestRouter_heavyOnCopy_notCarriedOver(t *testing.T) {
	f := copyAnswer()
	h := NewHandler(nil, nil)
	h.BindFreeSQL(f)
	var seen []string
	h.BindRouter(heavyPlan(), RouterConfig{MaxCopyAge: time.Minute, Observe: func(side RouteSide, reason RouteReason) {
		seen = append(seen, string(side)+"/"+string(reason))
	}})
	for _, stmt := range []string{"SELECT a FROM t WHERE b LIKE 'x%'", "SELECT a FROM t"} {
		if _, err := h.HandleQuery(stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	if len(seen) != 2 || seen[0] != "copy/heavy_on_copy" || seen[1] != "copy/expensive_plan" {
		t.Errorf("observed %v, want copy/heavy_on_copy then copy/expensive_plan", seen)
	}
	if h.heavy != (heavyState{}) {
		t.Errorf("after the statements the handler holds %+v, want nothing", h.heavy)
	}
}
