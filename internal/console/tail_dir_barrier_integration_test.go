//go:build integration

package console

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
	"github.com/dbtrail/dbtrail/internal/views"
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

// With BINTRAIL_SQL_FRESH_WAIT on, a client that just wrote reads its write,
// though an update that began before the write is running: the statement
// waits for that update, and only then for the source.
func TestIntegrationTailDirFreshWaitReadsYourWriteDuringAnUpdate(t *testing.T) {
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
	reg, err := LoadRegistry(t.TempDir() + "/console-servers.yaml")
	if err != nil {
		t.Fatal(err)
	}
	e, err := reg.Add(ServerEntry{Name: "prod", DSN: "u:p@tcp(idx:3306)/i", SourceDSN: "repl:pw@tcp(src:3306)/"})
	if err != nil {
		t.Fatal(err)
	}
	r.s.cm.reg = reg
	src := &fakePositions{pos: SourcePosition{File: "binlog.000008", Pos: 100}}
	var savedMu sync.Mutex
	saved := SourcePosition{File: "binlog.000008", Pos: 100}
	r.s.sourcePos, r.s.freshWait = src, 200*time.Millisecond
	r.s.freshSavedHook = func(context.Context) (SourcePosition, bool, error) {
		savedMu.Lock()
		defer savedMu.Unlock()
		return saved, false, nil
	}
	q := &SQLOnCopy{s: r.s, b: r.b, user: "u", id: e.ID}
	// Written before the copy is first read: a routed statement is vouched
	// for by what the snapshot records of each table.
	tables := []views.BaselineTable{r.table(nil), r.table(nil)}
	for i, tc := range []struct {
		name string
		sess sqlsandbox.Session
	}{
		{"a connection that does not route", sqlsandbox.Session{}},
		{"a routed connection", sqlsandbox.Session{StrictStar: true}},
	} {
		sess := tc.sess
		t.Run(tc.name, func(t *testing.T) {
			tbl := tables[i]
			at := time.Now().UTC().Add(-time.Hour).Truncate(time.Second)
			base := uint64(1000 * (i + 1))
			r.change(tbl.Table, event.EventUpdate, "1", "old", "binlog.000008", base, at)
			savedMu.Lock()
			saved.Pos = base + 50
			savedMu.Unlock()
			src.mu.Lock()
			src.pos.Pos = base + 50
			src.mu.Unlock()
			stmt := "SELECT CAST(id AS VARCHAR) || '=' || status FROM shop." + tbl.Table + " ORDER BY id"
			limit := time.Now().Add(30 * time.Second)
			for {
				res, err := q.Run(context.Background(), stmt, "", sess)
				if err == nil && fmt.Sprint(res.Rows) == "[[1=old]]" {
					break
				}
				if time.Now().After(limit) {
					t.Fatalf("the first change never showed: %v %v", res.Rows, err)
				}
				time.Sleep(50 * time.Millisecond)
			}
			k := r.s.tailKeeperFor(e.ID, r.b)
			// An update starts, then the client writes.
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
			r.change(tbl.Table, event.EventUpdate, "1", "new", "binlog.000008", base+100, at.Add(time.Second))
			savedMu.Lock()
			saved.Pos = base + 150
			savedMu.Unlock()
			src.mu.Lock()
			src.pos.Pos = base + 150
			src.mu.Unlock()
			before := r.s.freshWaits.of(e.ID).numbers(r.s.freshWait, time.Now())
			release := time.AfterFunc(time.Second, k.mu.Unlock)
			res, err := q.Run(context.Background(), stmt, "", sess)
			if release.Stop() {
				k.mu.Unlock()
			}
			after := r.s.freshWaits.of(e.ID).numbers(r.s.freshWait, time.Now())
			if after.Reached != before.Reached+1 || after.RanOut != before.RanOut {
				t.Errorf("the wait for the source: reached %d to %d, ran out %d to %d: want it reached", before.Reached, after.Reached, before.RanOut, after.RanOut)
			}
			if err != nil {
				t.Fatal(err)
			}
			if fmt.Sprint(res.Rows) != "[[1=new]]" {
				t.Errorf("answered %v, note %q: want the client's own write", res.Rows, res.Note)
			}
		})
	}
}
