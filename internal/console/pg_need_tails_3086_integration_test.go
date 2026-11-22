//go:build integration

package console

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

// #3086: a statement whose tables' recent changes are not ready within fresh
// mode's wait. A PostgreSQL statement marked WithPGNeedTails is refused
// before it runs, as a routed MySQL statement (StrictStar) is; a statement
// of a connection that does not route is still answered from the snapshot,
// with the warning that says so. The one shared line (sql_api.go) changes
// nothing for the two MySQL cases.
func TestIntegrationPGNeedTailsRefusesBeforeRunning_3086(t *testing.T) {
	t.Setenv("BINTRAIL_SQL_TAILS", "on")
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	r, _ := newTailRig(t)
	r.s.sqlRunner = sandboxRunner{sqlsandbox.New(sqlsandbox.Config{Exe: exe, Args: []string{}, Limits: sqlsandbox.Limits{Timeout: 60 * time.Second}})}
	r.s.sqlLimits = resolveSQLLimits(sqlsandbox.Limits{})
	r.b.baselineConfigured = true
	r.s.tailCapturesHook = func(string) (func(string, string) bool, bool) {
		return func(string, string) bool { return true }, true
	}
	// The tails never get ready in time: the index's events are locked
	// while the statement runs, so its table's tail is still being built
	// when fresh mode's wait ends, as a large table's is.
	lock := func(t *testing.T) func() {
		t.Helper()
		c, err := r.db.Conn(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if _, err := c.ExecContext(context.Background(), "LOCK TABLES binlog_events WRITE"); err != nil {
			t.Fatal(err)
		}
		return func() {
			_, _ = c.ExecContext(context.Background(), "UNLOCK TABLES")
			_ = c.Close()
		}
	}
	// views_for runs before the statement: counted to show where it stopped.
	var viewsFor atomic.Int32
	r.s.sqlViewsForHook = func() { viewsFor.Add(1) }
	q := &SQLOnCopy{s: r.s, b: r.b, user: "u", id: bootServerID}

	for _, tc := range []struct {
		name    string
		ctx     func(context.Context) context.Context
		sess    sqlsandbox.Session
		refused bool
	}{
		{"a MySQL connection that does not route: the snapshot, said", func(c context.Context) context.Context { return c }, sqlsandbox.Session{}, false},
		{"a routed MySQL connection: refused", func(c context.Context) context.Context { return c }, sqlsandbox.Session{StrictStar: true}, true},
		{"a PostgreSQL statement that needs its tails: refused", WithPGNeedTails, sqlsandbox.Session{PostgresRules: true, PostgresNames: true}, true},
		{"a PostgreSQL statement that does not: the snapshot, said", func(c context.Context) context.Context { return c }, sqlsandbox.Session{PostgresRules: true, PostgresNames: true}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var ran time.Time
			t0 := time.Now()
			tbl := r.table(nil)
			r.change(tbl.Table, event.EventUpdate, "1", "paid", "binlog.000008", 100, time.Now().UTC().Add(-time.Hour).Truncate(time.Second))
			stmt := "SELECT CAST(id AS VARCHAR) || '=' || status FROM shop." + tbl.Table + " ORDER BY id"
			start := time.Now()
			t.Logf("table and change %v", start.Sub(t0))
			unlock := lock(t)
			defer func() { u := time.Now(); unlock(); t.Logf("setup %v, run %v, unlock %v", ran.Sub(start), u.Sub(ran), time.Since(u)) }()
			before := viewsFor.Load()
			res, err := q.Run(tc.ctx(context.Background()), stmt, "", tc.sess)
			ran = time.Now()
			if viewsFor.Load() == before {
				t.Fatal("the statement never reached the tails step")
			}
			var changed *sqlsandbox.MayHaveChangedError
			if tc.refused {
				if !errors.As(err, &changed) || !strings.Contains(changed.Reason, "not ready yet") {
					t.Fatalf("got %v %v, want a MayHaveChangedError naming the tails not ready", res.Rows, err)
				}
				if len(res.Rows) != 0 {
					t.Errorf("rows %v with the refusal", res.Rows)
				}
				return
			}
			if err != nil {
				t.Fatalf("got %v, want the snapshot's answer", err)
			}
			if len(res.Rows) != 1 || res.Rows[0][0].(string) != "1=new" {
				t.Errorf("rows %v, want the snapshot's 1=new", res.Rows)
			}
			if !strings.Contains(res.Note, "not ready yet") {
				t.Errorf("note %q, want the warning that the snapshot alone answered", res.Note)
			}
		})
	}
}
