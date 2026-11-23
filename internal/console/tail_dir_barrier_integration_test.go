//go:build integration

package console

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

// An update longer than a statement may wait for it: the statement is not
// killed for its time, and says it read the snapshot alone.
func TestIntegrationTailDirAnUpdateLongerThanTheWait(t *testing.T) {
	t.Setenv("BINTRAIL_SQL_TAILS", "on")
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	r, _ := newTailRig(t)
	const clock = 10 * time.Second
	r.s.sqlRunner = sandboxRunner{sqlsandbox.New(sqlsandbox.Config{Exe: exe, Args: []string{}, Limits: sqlsandbox.Limits{Timeout: clock}})}
	r.s.sqlLimits = resolveSQLLimits(sqlsandbox.Limits{Timeout: clock})
	r.b.baselineConfigured = true
	r.s.tailCapturesHook = func(string) (func(string, string) bool, bool) {
		return func(string, string) bool { return true }, true
	}
	first := r.table(nil)
	r.change(first.Table, event.EventUpdate, "1", "paid", "binlog.000008", 100, time.Now().UTC().Add(-time.Hour).Truncate(time.Second))
	q := &SQLOnCopy{s: r.s, b: r.b, user: "u", id: bootServerID}
	read := func(table string) string {
		return "SELECT CAST(id AS VARCHAR) || '=' || status FROM shop." + table + " ORDER BY id"
	}
	limit := time.Now().Add(30 * time.Second)
	for {
		res, err := q.Run(context.Background(), read(first.Table), "", sqlsandbox.Session{})
		if err == nil && fmt.Sprint(res.Rows) == "[[1=paid]]" {
			break
		}
		if time.Now().After(limit) {
			t.Fatalf("the first table's change never showed: %v %v", res.Rows, err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	k := r.s.tailKeeperFor(bootServerID, r.b)
	tbl := r.table(nil)
	r.change(tbl.Table, event.EventUpdate, "1", "paid", "binlog.000008", 900, time.Now().UTC().Add(-time.Hour).Truncate(time.Second))
	k.mu.Lock()
	k.wakeFollower()
	for deadline := time.Now().Add(10 * time.Second); ; {
		k.pub.mu.Lock()
		running := k.pub.updating
		k.pub.mu.Unlock()
		if running {
			break
		}
		if time.Now().After(deadline) {
			k.mu.Unlock()
			t.Fatal("no update started")
		}
		k.wakeFollower()
		time.Sleep(10 * time.Millisecond)
	}
	release := time.AfterFunc(40*time.Second, k.mu.Unlock)
	start := time.Now()
	res, err := q.Run(context.Background(), read(tbl.Table), "", sqlsandbox.Session{})
	took := time.Since(start)
	if release.Stop() {
		k.mu.Unlock()
	}
	if err != nil {
		t.Fatalf("after %v: %v", took, err)
	}
	if took >= clock {
		t.Errorf("took %v, the statement's whole time", took)
	}
	if fmt.Sprint(res.Rows) != "[[1=new]]" || res.Note == "" {
		t.Errorf("answered %v, note %q: want the snapshot, said", res.Rows, res.Note)
	}
}

// A table with no version published yet, during an update that runs longer
// than a statement waits for a tail: the statement still gets the table's
// recent changes. Not waiting for the update is only for a table that has a
// version to read.
func TestIntegrationTailDirATableNotPublishedWaitsForTheUpdate(t *testing.T) {
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
	first := r.table(nil)
	r.change(first.Table, event.EventUpdate, "1", "paid", "binlog.000008", 100, time.Now().UTC().Add(-time.Hour).Truncate(time.Second))
	q := &SQLOnCopy{s: r.s, b: r.b, user: "u", id: bootServerID}
	read := func(table string) string {
		return "SELECT CAST(id AS VARCHAR) || '=' || status FROM shop." + table + " ORDER BY id"
	}
	limit := time.Now().Add(30 * time.Second)
	for {
		res, err := q.Run(context.Background(), read(first.Table), "", sqlsandbox.Session{})
		if err == nil && fmt.Sprint(res.Rows) == "[[1=paid]]" {
			break
		}
		if time.Now().After(limit) {
			t.Fatalf("the first table's change never showed: %v %v", res.Rows, err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	k := r.s.tailKeeperFor(bootServerID, r.b)
	for i, sess := range []sqlsandbox.Session{{}, {StrictStar: true}, {PostgresRules: true, PostgresNames: true}} {
		t.Run(fmt.Sprintf("session %d", i), func(t *testing.T) {
			tbl := r.table(nil)
			r.change(tbl.Table, event.EventUpdate, "1", "paid", "binlog.000008", uint64(200+i*100), time.Now().UTC().Add(-time.Hour).Truncate(time.Second))
			// An update of the follower, held past a statement's 3 s.
			k.mu.Lock()
			k.wakeFollower()
			deadline := time.Now().Add(10 * time.Second)
			for {
				k.pub.mu.Lock()
				running := k.pub.updating
				k.pub.mu.Unlock()
				if running {
					break
				}
				if time.Now().After(deadline) {
					k.mu.Unlock()
					t.Fatal("no update started")
				}
				k.wakeFollower()
				time.Sleep(10 * time.Millisecond)
			}
			release := time.AfterFunc(freshTailsWait+time.Second, k.mu.Unlock)
			res, err := q.Run(context.Background(), read(tbl.Table), "", sess)
			if release.Stop() {
				k.mu.Unlock()
			}
			var differ *sqlsandbox.ColumnsDifferError
			if sess.StrictStar && errors.As(err, &differ) {
				// This rig's table right after it is written: the refusal of a
				// routed statement that comes after the wait for its tail,
				// which is what is asked here.
				return
			}
			if err != nil {
				t.Fatalf("%v", err)
			}
			if fmt.Sprint(res.Rows) != "[[1=paid]]" || res.Note != "" {
				t.Errorf("answered %v, note %q: want the table's recent change", res.Rows, res.Note)
			}
		})
	}
}
