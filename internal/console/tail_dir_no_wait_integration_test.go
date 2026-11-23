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

// With the real engine, in each kind of session the two ports run: a
// statement reads a table's recent changes during an update of the tails,
// without waiting for it, and gets the rows it gets with no update running.
// With the directory swapped during the update it waits, and never answers
// old rows as if they were current.
func TestIntegrationTailDirDuringAnUpdate(t *testing.T) {
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
	q := &SQLOnCopy{s: r.s, b: r.b, user: "u", id: bootServerID}
	stmt := "SELECT CAST(id AS VARCHAR) || '=' || status FROM shop." + tbl.Table + " ORDER BY id"
	const current = "[[1=paid]]"
	settled := func(t *testing.T, sess sqlsandbox.Session) {
		t.Helper()
		limit := time.Now().Add(30 * time.Second)
		for {
			res, err := q.Run(context.Background(), stmt, "", sess)
			if err == nil && fmt.Sprint(res.Rows) == current {
				return
			}
			if time.Now().After(limit) {
				t.Fatalf("the recent change never showed: %v %v", res.Rows, err)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	sessions := []struct {
		name string
		sess sqlsandbox.Session
	}{
		{"MySQL port, routed", sqlsandbox.Session{StrictStar: true}},
		{"MySQL port, not routed", sqlsandbox.Session{}},
		{"PostgreSQL port", sqlsandbox.Session{PostgresRules: true, PostgresNames: true}},
	}
	settled(t, sqlsandbox.Session{})
	k := r.s.tailKeeperFor(bootServerID, r.b)
	if k == nil {
		t.Fatal("no keeper")
	}
	for _, s := range sessions {
		t.Run(s.name+": during an update", func(t *testing.T) {
			settled(t, s.sess)
			free, err := q.Run(context.Background(), stmt, "", s.sess)
			if err != nil {
				t.Fatal(err)
			}
			k.mu.Lock()
			release := time.AfterFunc(15*time.Second, k.mu.Unlock)
			start := time.Now()
			held, err := q.Run(context.Background(), stmt, "", s.sess)
			took := time.Since(start)
			if release.Stop() {
				k.mu.Unlock()
			}
			if err != nil {
				t.Fatal(err)
			}
			if took > 5*time.Second {
				t.Errorf("waited %v for the update", took)
			}
			if fmt.Sprint(held.Rows) != current || fmt.Sprint(held.Rows) != fmt.Sprint(free.Rows) {
				t.Errorf("during the update %v, with none %v, want the recent change %s", held.Rows, free.Rows, current)
			}
		})
	}
	swaps := []struct {
		name string
		swap func(dir string) error
	}{
		{"removed", os.RemoveAll},
		{"replaced by another directory", func(dir string) error {
			if err := os.Rename(dir, dir+".old"); err != nil {
				return err
			}
			return os.Mkdir(dir, 0o700)
		}},
		{"a link swapped in", func(dir string) error {
			if err := os.Rename(dir, dir+".old"); err != nil {
				return err
			}
			if err := os.Mkdir(dir+".other", 0o700); err != nil {
				return err
			}
			return os.Symlink(dir+".other", dir)
		}},
	}
	for _, s := range sessions {
		for _, sw := range swaps {
			t.Run(s.name+": directory "+sw.name, func(t *testing.T) {
				settled(t, s.sess)
				dir, err := k.ensureDir()
				if err != nil {
					t.Fatal(err)
				}
				k.mu.Lock()
				if err := sw.swap(dir); err != nil {
					k.mu.Unlock()
					t.Fatal(err)
				}
				type answer struct {
					res sqlsandbox.Result
					err error
				}
				ch := make(chan answer, 1)
				go func() {
					res, err := q.Run(context.Background(), stmt, "", s.sess)
					ch <- answer{res, err}
				}()
				select {
				case a := <-ch:
					k.mu.Unlock()
					t.Fatalf("answered during the update, with its directory swapped: %v %v", a.res.Rows, a.err)
				case <-time.After(400 * time.Millisecond):
				}
				k.mu.Unlock()
				a := <-ch
				var changed *sqlsandbox.MayHaveChangedError
				switch {
				case errors.As(a.err, &changed):
					// Refused rather than answered from the snapshot alone.
				case a.err != nil && a.err.Error() == sqlWorkerFailedMessage:
					// The views published before the swap name files that are
					// gone: an error, as before this change, and never old
					// rows. The next update publishes them again.
				case a.err != nil:
					t.Fatalf("after the update: %v", a.err)
				case fmt.Sprint(a.res.Rows) != current && a.res.Note == "":
					t.Errorf("answered %v with nothing to say it is not current", a.res.Rows)
				}
				settled(t, s.sess)
			})
		}
	}
}
