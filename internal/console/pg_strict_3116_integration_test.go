//go:build integration

package console

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

// #3116, case 1: the recent changes a statement reads are older than what an
// earlier answer of the copy on the same connection read. A routed MySQL
// statement (StrictStar) is refused before it runs; so is a PostgreSQL
// statement marked WithPGNeedTails (its source can answer it). A MySQL
// connection that does not route, and a PostgreSQL statement with nowhere
// else to go, are answered with the note that says so. The memory is the
// connection's (one SQLOnCopy each): a new one starts clean. The two MySQL
// rows pin that the shared line (sql_api.go) changes nothing for MySQL.
func TestIntegrationOlderThanAnEarlierAnswer_3116(t *testing.T) {
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
	tbl := r.table(nil)
	r.change(tbl.Table, event.EventUpdate, "1", "paid", "binlog.000008", 100, time.Now().UTC().Add(-time.Hour).Truncate(time.Second))
	stmt := "SELECT CAST(id AS VARCHAR) || '=' || status FROM shop." + tbl.Table + " ORDER BY id"
	const current = "[[1=paid]]"
	const older = "older than an earlier answer of the copy on this connection"
	pg := sqlsandbox.Session{PostgresRules: true, PostgresNames: true}
	plain := func(c context.Context) context.Context { return c }

	for _, tc := range []struct {
		name    string
		ctx     func(context.Context) context.Context
		sess    sqlsandbox.Session
		refused bool
	}{
		{"a MySQL connection that does not route: answered, said", plain, sqlsandbox.Session{}, false},
		{"a routed MySQL connection: refused", plain, sqlsandbox.Session{StrictStar: true}, true},
		{"a PostgreSQL statement its source can answer: refused", WithPGNeedTails, pg, true},
		{"a PostgreSQL statement with nowhere else to go: answered, said", plain, pg, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// A connection of its own: its first answer of the copy, with the
			// recent change in it, sets the floor.
			q := &SQLOnCopy{s: r.s, b: r.b, user: "u", id: bootServerID}
			limit := time.Now().Add(30 * time.Second)
			for {
				res, err := q.Run(tc.ctx(context.Background()), stmt, "", tc.sess)
				if err == nil && fmt.Sprint(res.Rows) == current {
					break
				}
				if time.Now().After(limit) {
					t.Fatalf("the recent change never showed: %v %v", res.Rows, err)
				}
				time.Sleep(50 * time.Millisecond)
			}
			floor := q.seen.Load()
			if floor == 0 {
				t.Fatal("an answer of the copy that read the recent changes set no floor on its connection")
			}
			// An earlier answer that read the index further than the tails
			// hold now: tails only move forward within one process, so the
			// floor is raised by hand to stand for it (a tail rebuilt from
			// an older position, a keeper made again).
			q.seen.Store(floor + 1<<20)
			res, err := q.Run(tc.ctx(context.Background()), stmt, "", tc.sess)
			if tc.refused {
				var changed *sqlsandbox.MayHaveChangedError
				if !errors.As(err, &changed) || !strings.Contains(changed.Reason, older) {
					t.Fatalf("got %v %v, want a MayHaveChangedError naming the earlier answer", res.Rows, err)
				}
				if len(res.Rows) != 0 {
					t.Errorf("rows %v with the refusal", res.Rows)
				}
			} else {
				if err != nil {
					t.Fatalf("got %v, want the copy's answer", err)
				}
				if fmt.Sprint(res.Rows) != current {
					t.Errorf("rows %v, want %s", res.Rows, current)
				}
				if !strings.Contains(res.Note, older) {
					t.Errorf("note %q, want the one that says the answer is older than an earlier one", res.Note)
				}
			}
			// A new connection starts clean: nothing it was shown is newer.
			fresh := &SQLOnCopy{s: r.s, b: r.b, user: "u", id: bootServerID}
			res, err = fresh.Run(tc.ctx(context.Background()), stmt, "", tc.sess)
			if err != nil || fmt.Sprint(res.Rows) != current {
				t.Fatalf("a new connection: %v %v, want %s", res.Rows, err, current)
			}
			if strings.Contains(res.Note, older) {
				t.Errorf("a new connection was told %q", res.Note)
			}
		})
	}
}
