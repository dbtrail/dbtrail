package shim

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"

	"github.com/dbtrail/dbtrail/internal/readrouter"
)

// answeredHandler records every decision as "route/reason" and every answer
// as "side/ok", "side/error" or "side/timeout".
func answeredHandler(r *fakeRouter, f *fakeFreeSQL, cfg RouterConfig, decided, answered *[]string) *Handler {
	h := NewHandler(nil, nil)
	h.BindFreeSQL(f)
	h.BindRouter(r, answeredConfig(cfg, decided, answered))
	return h
}

func answeredConfig(cfg RouterConfig, decided, answered *[]string) RouterConfig {
	cfg.Observe = func(route RouteSide, reason RouteReason) {
		*decided = append(*decided, string(route)+"/"+string(reason))
	}
	cfg.Answered = func(side RouteSide, _ string, took time.Duration, failed, timedOut bool) {
		how := "ok"
		switch {
		case timedOut && !failed:
			how = "timeout-without-failure"
		case timedOut:
			how = "timeout"
		case failed:
			how = "error"
		}
		*answered = append(*answered, string(side)+"/"+how)
	}
	return cfg
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

// MySQL's own error is a failed answer: it ran the statement and said no.
func TestRouterAnswered_mysqlErrorIsAFailedAnswer(t *testing.T) {
	r := &fakeRouter{toCopy: false, reason: "cheap", forwardErr: errors.New("ERROR 1146: table nope doesn't exist")}
	f := &fakeFreeSQL{res: oneCell("side", "VARCHAR", "copy"), updatedAt: time.Now()}
	var decided, answered []string
	h := answeredHandler(r, f, RouterConfig{MaxCopyAge: time.Minute}, &decided, &answered)
	if _, err := h.HandleQuery("SELECT * FROM nope WHERE id = 1"); err == nil {
		t.Fatal("the error did not reach the client")
	}
	if len(answered) != 1 || answered[0] != "mysql/error" {
		t.Errorf("answered %v, want [mysql/error]", answered)
	}
}

// A statement whose time ran out reaches the ladder the way the real
// forwarder hands it over: as a lost connection (error 2006), with the
// statement's own context expired. That is a timeout. The same 2006 with
// the context still good is a source nobody reached: not an answer at all.
func TestRouterAnswered_timeoutAndLostConnection(t *testing.T) {
	lost := mysql.NewError(readrouter.CodeUpstreamLost, "the connection to the source was lost")

	r := &fakeRouter{toCopy: false, reason: "cheap", forwardErr: lost, forwardWaits: true}
	f := &fakeFreeSQL{res: oneCell("side", "VARCHAR", "copy"), updatedAt: time.Now()}
	var decided, answered []string
	h := NewHandlerWithConfig(nil, Config{QueryTimeout: 30 * time.Millisecond}, nil)
	h.BindFreeSQL(f)
	h.BindRouter(r, answeredConfig(RouterConfig{MaxCopyAge: time.Minute}, &decided, &answered))
	if _, err := h.HandleQuery("SELECT * FROM t WHERE id = 1"); err == nil {
		t.Fatal("the timeout did not reach the client")
	}
	if len(answered) != 1 || answered[0] != "mysql/timeout" {
		t.Errorf("out of time: answered %v, want [mysql/timeout] (decided %v)", answered, decided)
	}

	r = &fakeRouter{toCopy: false, reason: "cheap", forwardErr: lost}
	decided, answered = nil, nil
	h = answeredHandler(r, f, RouterConfig{MaxCopyAge: time.Minute}, &decided, &answered)
	if _, err := h.HandleQuery("SELECT * FROM t WHERE id = 1"); err == nil {
		t.Fatal("the lost connection did not reach the client")
	}
	if len(answered) != 0 || len(decided) != 1 || decided[0] != "mysql/upstream_lost" {
		t.Errorf("source not reached: answered %v, decided %v", answered, decided)
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

// The statement is reported with the answer, as the client sent it: for a
// prepared one its template, never the values it ran with (#2360).
func TestRouterAnswered_carriesTheStatement(t *testing.T) {
	var got []string
	cfg := RouterConfig{MaxCopyAge: time.Minute}
	cfg.Answered = func(side RouteSide, stmt string, _ time.Duration, _, _ bool) {
		got = append(got, string(side)+": "+stmt)
	}
	r := &fakeRouter{toCopy: false, reason: "cheap"}
	f := &fakeFreeSQL{res: oneCell("side", "VARCHAR", "copy"), updatedAt: time.Now()}
	h := NewHandler(nil, nil)
	h.BindFreeSQL(f)
	h.BindRouter(r, cfg)
	if _, err := h.HandleQuery("SELECT * FROM t WHERE id = 1"); err != nil {
		t.Fatal(err)
	}
	_, _, ctx, err := h.HandleStmtPrepare("SELECT * FROM t WHERE id = ?")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.HandleStmtExecute(ctx, "", []any{int64(424242)}); err != nil {
		t.Fatal(err)
	}
	want := []string{"mysql: SELECT * FROM t WHERE id = 1", "mysql: SELECT * FROM t WHERE id = ?"}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("answered %q, want %q", got, want)
	}
}
