package consoleapp

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/pgbaseline"
)

// Review of #1991: what the failure card needs beyond the kind.

// The card may only blame a setting when an operator set one. An automatic
// mode is the daemon's own choice: there is nothing to undo.
func TestExecute_failureSaysWhetherTheModeWasChosen(t *testing.T) {
	for _, c := range []struct {
		name   string
		chosen bool
		dsn    string
		first  error
		want   bool
	}{
		{"operator's ftwrl", true, ownHostDSN, missingErr(baseline.LockModeFTWRL, "`u`@`%`", "BACKUP_ADMIN"), true},
		{"automatic lock-all on RDS", false, rdsDSN, missingErr(baseline.LockModeLockAll, "`u`@`%`", "LOCK TABLES"), false},
		{"automatic, after the retry", false, ownHostDSN, missingErr(baseline.LockModeFTWRL, "`u`@`%`", "RELOAD"), false},
		{"operator's mode, mydumper too old", true, ownHostDSN, &mydumperTooOldError{err: errors.New("old"), min: "0.18.1"}, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			stubRetryDump(t, func(n int, mode baseline.LockMode, _ string) error {
				if n == 1 {
					return c.first
				}
				return missingErr(baseline.LockModeLockAll, "`u`@`%`", "LOCK TABLES")
			})
			s := newBaselineSupervisor(context.Background(), t.TempDir(), baseline.DefaultLockMode)
			s.lockModeChosen = c.chosen
			req := console.BaselineRequest{ServerID: "s1", SourceDSN: c.dsn}
			_, err := s.execute(req)
			if err == nil {
				t.Fatal("execute succeeded over a failed dump")
			}
			f := snapshotFailureOf(err, req)
			if f == nil || f.ModeChosen != c.want {
				t.Fatalf("failure = %+v, want mode_chosen %v (err %v)", f, c.want, err)
			}
		})
	}
}

// Every failure that published nothing carries what the card shows for it:
// a short first line (no mydumper output), the server's name, whether the
// schedule ran it, and whether it was a Postgres source.
func TestSnapshotFailureOf_carriesTheCardsContext(t *testing.T) {
	long := strings.Repeat("x", 400)
	for _, c := range []struct {
		name string
		err  error
		req  console.BaselineRequest
		want console.SnapshotFailure
	}{
		{"no kind: the first line, without mydumper's output",
			errors.New("dump: mydumper failed: exit status 1; output: ** (mydumper:7): CRITICAL **: Access denied\nmore"),
			console.BaselineRequest{ServerName: "shop-db", SourceDSN: ownHostDSN},
			console.SnapshotFailure{Summary: "dump: mydumper failed: exit status 1", Server: "shop-db"}},
		{"a long first line is capped", errors.New(long),
			console.BaselineRequest{ServerName: "a", SourceDSN: ownHostDSN},
			console.SnapshotFailure{Summary: strings.Repeat("x", 300) + "...", Server: "a"}},
		{"the schedule ran it", errors.New("disk full"),
			console.BaselineRequest{ServerName: "a", SourceDSN: ownHostDSN, Trigger: console.BaselineRunTriggerScheduled},
			console.SnapshotFailure{Summary: "disk full", Server: "a", Scheduled: true}},
		{"Postgres: no kind, ever", missingErr(baseline.LockModeLockAll, "`u`@`%`", "LOCK TABLES"),
			console.BaselineRequest{ServerName: "pg", SourceDSN: ownHostDSN, Flavor: console.FlavorPostgres},
			console.SnapshotFailure{Summary: "", Server: "pg", Postgres: true}},
	} {
		t.Run(c.name, func(t *testing.T) {
			got := snapshotFailureOf(c.err, c.req)
			if got == nil {
				t.Fatal("no failure for a failed run")
			}
			if *got != c.want {
				t.Errorf("failure = %+v, want %+v", *got, c.want)
			}
		})
	}
	if snapshotFailureOf(nil, console.BaselineRequest{}) != nil {
		t.Error("a success carries a failure")
	}
}

// A Postgres snapshot written to the server's own folder, whose upload then
// failed, exists: the status says published and the record names its time,
// so no place calls it unfinished.
func TestExecutePG_uploadFailureAfterTheSnapshotIsPublished(t *testing.T) {
	prevRun, prevUp, prevW := pgBaselineRun, uploadSnapshot, snapshotWriterIDFunc
	t.Cleanup(func() { pgBaselineRun, uploadSnapshot, snapshotWriterIDFunc = prevRun, prevUp, prevW })
	at := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)
	pgBaselineRun = func(context.Context, pgbaseline.Config) (pgbaseline.Stats, error) {
		return pgbaseline.Stats{TablesProcessed: 2, SnapshotTime: at}, nil
	}
	uploadSnapshot = func(context.Context, string, string, string, bool) (int, error) { return 0, errors.New("s3: access denied") }
	snapshotWriterIDFunc = func(string) (string, error) { return "w", nil }

	s := newBaselineSupervisor(context.Background(), t.TempDir(), baseline.DefaultLockMode)
	path := filepath.Join(t.TempDir(), "runs.json")
	hist, err := console.OpenBaselineHistory(path)
	if err != nil {
		t.Fatal(err)
	}
	s.history = hist
	req := console.BaselineRequest{ServerID: "pg1", ServerName: "pg", Flavor: console.FlavorPostgres,
		SourceDSN: "postgres://<redacted>/appdb", Slot: "s", Publication: "p",
		LocalDir: t.TempDir(), S3: "s3://bucket/x", Trigger: console.BaselineRunTriggerScheduled}
	s.jobs["pg1"] = &console.BaselineStatus{State: "running"}
	s.run(req)
	st := s.Status("pg1")
	if st.State != "failed" || !st.Published {
		t.Fatalf("status = %+v, want failed and published", st)
	}
	if st.Failure != nil {
		t.Errorf("a published snapshot carries the did-not-finish failure: %+v", st.Failure)
	}
	run, _ := hist.LastScheduled("pg1")
	if run == nil || run.SnapshotTime == "" || run.Failure != nil {
		t.Fatalf("history record = %+v, want the snapshot's time and no failure", run)
	}
}
