//go:build integration

package console

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

// A statement that read every table with its tail has its age from the
// tails, so the snapshot's age is not measured for it: that costs reads of
// the index on the statement's turn, in the default mode, for a number
// nobody uses (#3125). The same statement in fast mode is the control: the
// measure works on this very file.
func TestIntegrationPGAllTailedStatementMeasuresNoSnapshotAge_3125(t *testing.T) {
	t.Setenv("BINTRAIL_SQL_TAILS", "on")
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	r := newPGTailRig(t)
	r.b.pgSource = true
	r.s.sqlRunner = sandboxRunner{sqlsandbox.New(sqlsandbox.Config{Exe: exe, Args: []string{}, Limits: sqlsandbox.Limits{Timeout: 60 * time.Second}})}
	r.s.sqlLimits = resolveSQLLimits(sqlsandbox.Limits{})
	r.b.baselineConfigured = true
	r.s.tailCapturesHook = func(string) (func(string, string) bool, bool) { return nil, true }
	EnablePostgresTails(true)
	t.Cleanup(func() { EnablePostgresTails(false) })

	tbl := r.table(nil, [2]string{"1", "one"})
	r.change(tbl.Table, event.EventUpdate, "1", "paid", 0x610, 20*time.Minute)
	r.checkpoint(0x610)

	q := &SQLOnCopy{s: r.s, b: r.b, user: "u", id: bootServerID}
	statement := "SELECT status FROM public." + tbl.Table
	run := func(ctx context.Context) {
		t.Helper()
		if _, err := q.Run(ctx, statement, "", sqlsandbox.Session{PostgresRules: true}); err != nil {
			t.Fatal(err)
		}
	}
	run(context.Background())
	r.s.tails.mu.Lock()
	k := r.s.tails.by[bootServerID]
	r.s.tails.mu.Unlock()
	if k == nil {
		t.Fatal("no keeper for the server")
	}
	k.followOnce(context.Background(), time.Minute)
	k.settle()
	k.followOnce(context.Background(), time.Minute)

	var tailed PGSnapshotAge
	run(WithPGSnapshotAge(context.Background(), &tailed))
	if a := q.LastRunAge(); !a.Tailed {
		t.Fatalf("the statement did not read its table with its tail: %+v", a)
	}
	if got := tailed.get(); !got.IsZero() {
		t.Errorf("a statement with every tail measured the snapshot's age (%v)", got)
	}

	t.Setenv("BINTRAIL_SQL_TAILS", "off")
	var fast PGSnapshotAge
	run(WithPGSnapshotAge(context.Background(), &fast))
	if a := q.LastRunAge(); !a.Snapshot {
		t.Fatalf("fast mode did not read the snapshot alone: %+v", a)
	}
	if fast.get().IsZero() {
		t.Error("the control: fast mode has no age on this file, so the check above proves nothing")
	}
}
