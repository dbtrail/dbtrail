package shim

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"

	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

// The server's lag limit (#2777) is the copy's to apply, since only the copy
// knows the tables a statement reads and their tails: it answers with a
// BehindSourceError. A routed statement then goes to MySQL under its own
// reason, an age that cannot be told under copy_age_unknown, with no
// warning in the log (the rule the operator set, not a fault), and the same
// on a prepared statement's execution.
func TestRouter_behindSourceLimitForwards_2777(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want string
	}{
		{"past the limit", &sqlsandbox.BehindSourceError{Reason: "the copy is 7200 s behind the source, more than this server's limit of 300 s"}, "mysql/copy_behind_source"},
		{"lag unknown", &sqlsandbox.BehindSourceError{Reason: "how far behind the source the copy is cannot be told", Unknown: true}, "mysql/copy_age_unknown"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := &fakeRouter{toCopy: true, reason: "expensive"}
			f := &fakeFreeSQL{err: tc.err, updatedAt: time.Now()}
			var got []string
			h := serviceHandler(t, r, f, RouterConfig{AnyCopyAge: true, BusyRefuses: true}, &got)
			if _, err := h.HandleQuery(expensive2084); err != nil {
				t.Fatalf("reached the client as an error: %v", err)
			}
			if len(got) != 1 || got[0] != tc.want || len(r.forwarded) != 1 {
				t.Errorf("observed %v, forwarded %v; want [%s] and the statement once", got, r.forwarded, tc.want)
			}
			if len(h.routeWarned) != 0 {
				t.Errorf("warned %v: the limit is the operator's rule, not a fault", h.routeWarned)
			}

			const stmt = "SELECT a, count(*) FROM t WHERE b > ? GROUP BY a"
			_, _, ctx, err := h.HandleStmtPrepare(stmt)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := h.HandleStmtExecute(ctx, "", []any{int64(1)}); err != nil {
				t.Fatalf("execute: %v", err)
			}
			if got[len(got)-1] != tc.want {
				t.Errorf("prepared: observed %v, want the execution under %s", got, tc.want)
			}
		})
	}
}

// On a connection that does not route (<server>@copy, Engine Routing off)
// nobody else can answer: the client gets the copy's reason, which says the
// lag and the limit, as a MySQL error.
func TestCopyOnly_behindSourceLimitRefuses_2777(t *testing.T) {
	const reason = "the copy is 7200 s behind the source, more than this server's limit of 300 s (max_behind_source_seconds), so it does not answer this statement"
	f := &fakeFreeSQL{res: oneCell("x", "INTEGER", json.Number("1")), err: &sqlsandbox.BehindSourceError{Reason: reason}}
	h := NewHandler(nil, nil)
	h.BindFreeSQL(f)
	_, err := h.HandleQuery("SELECT count(*) FROM orders")
	var me *mysql.MyError
	if !errors.As(err, &me) || me.Code != mysql.ER_UNKNOWN_ERROR || me.Message != reason {
		t.Fatalf("err = %v, want MySQL error 1105 carrying the copy's reason", err)
	}
	if !strings.Contains(me.Message, "7200 s") || !strings.Contains(me.Message, "300 s") {
		t.Errorf("the message does not say the lag and the limit: %q", me.Message)
	}
}
