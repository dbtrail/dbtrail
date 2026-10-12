package shim

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

// answeredHandler records every decision as "route/reason" and every answer
// as "side/ok", "side/error" or "side/timeout".
func answeredHandler(r *fakeRouter, f *fakeFreeSQL, cfg RouterConfig, decided, answered *[]string) *Handler {
	h := NewHandler(nil, nil)
	h.BindFreeSQL(f)
	cfg.Observe = func(route RouteSide, reason RouteReason) {
		*decided = append(*decided, string(route)+"/"+string(reason))
	}
	cfg.Answered = func(side RouteSide, took time.Duration, failed, timedOut bool) {
		how := "ok"
		switch {
		case timedOut && !failed:
			how = "timeout-without-failure"
		case timedOut:
			how = "timeout"
		case failed:
			how = "error"
		}
		if took < 0 {
			how += "/negative"
		}
		*answered = append(*answered, string(side)+"/"+how)
	}
	h.BindRouter(r, cfg)
	return h
}

// Each statement the ladder runs is reported once, on the side that ran it.
// The copy is reported only when IT answered: a statement it declines is
// MySQL's, and is reported once, as MySQL's.
func TestRouterAnswered_onceOnTheSideThatRan(t *testing.T) {
	fresh := time.Now()
	cases := []struct {
		name    string
		stmt    string
		router  fakeRouter
		copyErr error
		want    string
	}{
		{"cheap read", "SELECT * FROM t WHERE id = 1", fakeRouter{toCopy: false, reason: "cheap"}, nil, "mysql/ok"},
		{"a write", "UPDATE t SET a = 1", fakeRouter{toCopy: true}, nil, "mysql/ok"},
		{"not a select", "SHOW TABLES", fakeRouter{toCopy: true}, nil, "mysql/ok"},
		{"vetoed", "SELECT NOW()", fakeRouter{toCopy: true}, nil, "mysql/ok"},
		{"explain failed", "SELECT * FROM t WHERE id = 1", fakeRouter{decideErr: errors.New("boom")}, nil, "mysql/ok"},
		{"the copy answers", "SELECT a, count(*) FROM t GROUP BY a", fakeRouter{toCopy: true, reason: "expensive"}, nil, "copy/ok"},
		{"the copy declines", "SELECT a, count(*) FROM t GROUP BY a", fakeRouter{toCopy: true}, errors.New("Binder Error"), "mysql/ok"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := tc.router
			f := &fakeFreeSQL{res: oneCell("side", "VARCHAR", "copy"), err: tc.copyErr, updatedAt: fresh}
			var decided, answered []string
			h := answeredHandler(&r, f, RouterConfig{MaxCopyAge: time.Minute}, &decided, &answered)
			if _, err := h.HandleQuery(tc.stmt); err != nil {
				t.Fatalf("HandleQuery: %v", err)
			}
			if len(answered) != 1 || answered[0] != tc.want {
				t.Errorf("answered %v, want exactly [%s] (decided %v)", answered, tc.want, decided)
			}
			// The answer is on the side the decision named.
			if len(decided) != 1 || strings.SplitN(decided[0], "/", 2)[0] != strings.SplitN(tc.want, "/", 2)[0] {
				t.Errorf("decided %v, answered %v: not the same side", decided, answered)
			}
		})
	}
}

// What the client got an error for is reported as failed, and a statement
// whose time ran out as a timeout, on MySQL's side: it is what ran it.
func TestRouterAnswered_failuresAndTimeouts(t *testing.T) {
	for _, tc := range []struct {
		err  error
		want string
	}{
		{errors.New("ERROR 1146: table nope doesn't exist"), "mysql/error"},
		{fmt.Errorf("forward: %w", context.DeadlineExceeded), "mysql/timeout"},
	} {
		r := &fakeRouter{toCopy: false, reason: "cheap", forwardErr: tc.err}
		f := &fakeFreeSQL{res: oneCell("side", "VARCHAR", "copy"), updatedAt: time.Now()}
		var decided, answered []string
		h := answeredHandler(r, f, RouterConfig{MaxCopyAge: time.Minute}, &decided, &answered)
		if _, err := h.HandleQuery("SELECT * FROM nope WHERE id = 1"); err == nil {
			t.Fatal("the error did not reach the client")
		}
		if len(answered) != 1 || answered[0] != tc.want {
			t.Errorf("answered %v, want [%s]", answered, tc.want)
		}
	}
}

// Nothing ran, so nothing is reported: a statement the read-only port
// refuses never reaches a side.
func TestRouterAnswered_refusalsAreNotAnswers(t *testing.T) {
	r := &fakeRouter{toCopy: true}
	f := &fakeFreeSQL{res: oneCell("side", "VARCHAR", "copy"), updatedAt: time.Now()}
	var decided, answered []string
	h := answeredHandler(r, f, RouterConfig{MaxCopyAge: time.Minute, ReadOnly: true}, &decided, &answered)
	if _, err := h.HandleQuery("UPDATE t SET a = 1"); err == nil {
		t.Fatal("a write was not refused on a read-only port")
	}
	if len(answered) != 0 || len(decided) != 1 || !strings.HasPrefix(decided[0], "refused/") {
		t.Errorf("answered %v, decided %v", answered, decided)
	}
}

// With no one listening the ladder is as it was.
func TestRouterAnswered_nilHookIsFine(t *testing.T) {
	r := &fakeRouter{toCopy: true, reason: "expensive"}
	f := &fakeFreeSQL{res: oneCell("side", "VARCHAR", "copy"), updatedAt: time.Now()}
	h := NewHandler(nil, nil)
	h.BindFreeSQL(f)
	h.BindRouter(r, RouterConfig{MaxCopyAge: time.Minute})
	for _, q := range []string{"SELECT a, count(*) FROM t GROUP BY a", "UPDATE t SET a = 1"} {
		if _, err := h.HandleQuery(q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
}

// A prepared statement runs the same ladder, so it is reported the same way.
func TestRouterAnswered_preparedStatement(t *testing.T) {
	r := &fakeRouter{toCopy: false, reason: "cheap"}
	f := &fakeFreeSQL{res: oneCell("side", "VARCHAR", "copy"), updatedAt: time.Now()}
	var decided, answered []string
	h := answeredHandler(r, f, RouterConfig{MaxCopyAge: time.Minute}, &decided, &answered)
	_, _, ctx, err := h.HandleStmtPrepare("SELECT * FROM t WHERE id = ?")
	if err != nil {
		t.Fatal(err)
	}
	if len(answered) != 0 {
		t.Fatalf("preparing is not answering: %v", answered)
	}
	if _, err := h.HandleStmtExecute(ctx, "", []any{int64(1)}); err != nil {
		t.Fatal(err)
	}
	if len(answered) != 1 || answered[0] != "mysql/ok" {
		t.Errorf("answered %v, want [mysql/ok] (decided %v)", answered, decided)
	}
}
