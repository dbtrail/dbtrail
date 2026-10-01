package consoleapp

import (
	"context"
	"errors"
	"fmt"
	"github.com/dbtrail/dbtrail/internal/config"
	"path/filepath"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/mydumperlock"
)

// #1986 part 2: a failed snapshot carries, as data, the two causes the page
// explains in plain words. These run the REAL producers of each error
// (planMydumper, runMydumper, execute's retry) wherever one exists, so a kind
// is pinned to the error that makes it, not to a hand-built copy of it.

const rdsDSN = "admin:p@tcp(db1.abcdefghij.us-east-1.rds.amazonaws.com:1)/"

func missingErr(mode baseline.LockMode, account string, privs ...string) error {
	return &mydumperlock.MissingPrivilegesError{Mode: mode, Missing: privs, Account: account}
}

// kindOnly keeps what names the fix, nil when there is none.
func kindOnly(f *console.SnapshotFailure) *console.SnapshotFailure {
	if f == nil || f.Kind == "" {
		return nil
	}
	return &console.SnapshotFailure{Kind: f.Kind, Grant: f.Grant, Privileges: f.Privileges, MinVersion: f.MinVersion}
}

func TestSnapshotFailureOf_kinds(t *testing.T) {
	for _, c := range []struct {
		name   string
		err    error
		dsn    string
		want   *console.SnapshotFailure
		flavor string
	}{
		{"nil", nil, ownHostDSN, nil, ""},
		{"anything else", errors.New("mydumper failed: exit status 2"), ownHostDSN, nil, ""},
		{"ftwrl privileges, operator's ftwrl on a server they run",
			fmt.Errorf("dump: %w", missingErr(baseline.LockModeFTWRL, "`u`@`%`", "RELOAD", "BACKUP_ADMIN")), ownHostDSN,
			&console.SnapshotFailure{Kind: console.SnapshotFailureMissingPermission, Grant: "GRANT RELOAD, BACKUP_ADMIN ON *.* TO `u`@`%`;", Privileges: 2}, ""},
		// RDS refuses BACKUP_ADMIN outright: a GRANT for it would send the
		// operator to run a statement that cannot work.
		{"ftwrl privileges on an RDS host", missingErr(baseline.LockModeFTWRL, "`admin`@`%`", "BACKUP_ADMIN"), rdsDSN, nil, ""},
		{"lock-all privileges on an RDS host",
			missingErr(baseline.LockModeLockAll, "`admin`@`%`", "LOCK TABLES"), rdsDSN,
			&console.SnapshotFailure{Kind: console.SnapshotFailureMissingPermission, Grant: "GRANT LOCK TABLES ON *.* TO `admin`@`%`;", Privileges: 1}, ""},
		// No account read from SHOW GRANTS: the DSN's user at '%' would be a
		// guess at the account's host, so no statement at all.
		{"no account read", missingErr(baseline.LockModeLockAll, "", "LOCK TABLES", "SHOW VIEW"), "o'b\\x:p@tcp(127.0.0.1:1)/", nil, ""},
		// The global read lock refused at run time: RELOAD is already held
		// (the check passed), so no GRANT fixes it.
		{"mydumper refused the global read lock", errDeniedFTWRL, ownHostDSN, nil, ""},
		// A Postgres snapshot never runs mydumper: whatever it returns, the
		// page draws the generic card.
		{"a Postgres snapshot", missingErr(baseline.LockModeLockAll, "`u`@`%`", "LOCK TABLES"), ownHostDSN, nil, console.FlavorPostgres},
		{"too old", fmt.Errorf("dump: %w", &mydumperTooOldError{err: errors.New("old"), min: "0.18.1"}), ownHostDSN,
			&console.SnapshotFailure{Kind: console.SnapshotFailureMydumperTooOld, MinVersion: "0.18.1"}, ""},
	} {
		t.Run(c.name, func(t *testing.T) {
			// Since the #1991 review every failure carries the card's context
			// (summary, server...); this table is about the kind alone, and
			// want nil means "no kind".
			got := kindOnly(snapshotFailureOf(c.err, console.BaselineRequest{SourceDSN: c.dsn, Flavor: c.flavor}))
			if fmt.Sprintf("%+v", got) != fmt.Sprintf("%+v", c.want) {
				t.Errorf("snapshotFailureOf = %+v, want %+v", got, c.want)
			}
		})
	}
}

// The too-old kind comes from planMydumper's own refusals, and only from the
// ones where installing a newer mydumper is the fix. A version that cannot be
// read, a binary that cannot run, and no binary at all name no version.
func TestPlanMydumper_tooOldIsTyped(t *testing.T) {
	for _, c := range []struct {
		name    string
		version string // "" = no mydumper on PATH
		mode    baseline.LockMode
		src     lockModeSource
		tooOld  bool
	}{
		{"distribution build, automatic lock-all", printsVersion(versionDistro), baseline.LockModeLockAll, lockModeAutomatic, true},
		{"distribution build, operator's lock-all", printsVersion(versionDistro), baseline.LockModeLockAll, lockModeFromEnv, true},
		{"0.16, safe-no-lock", printsVersion("mydumper v0.16.3-6, built against MySQL 8.4.1 with SSL support"), baseline.LockModeSafeNoLock, lockModeFromEnv, true},
		{"version unreadable", printsVersion("something else entirely"), baseline.LockModeLockAll, lockModeAutomatic, false},
		{"binary cannot run", loaderFailure, baseline.LockModeLockAll, lockModeAutomatic, false},
		{"not installed", "", baseline.LockModeLockAll, lockModeAutomatic, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			if c.version == "" {
				t.Setenv("PATH", t.TempDir())
			} else {
				fakeConsoleMydumper(t, c.version)
			}
			_, err := planMydumper(c.mode, c.src)
			if err == nil {
				t.Fatal("planMydumper accepted the run")
			}
			f := snapshotFailureOf(err, console.BaselineRequest{SourceDSN: ownHostDSN})
			if got := f != nil && f.Kind == console.SnapshotFailureMydumperTooOld && f.MinVersion == mydumperlock.LockModeFloor; got != c.tooOld {
				t.Errorf("too old = %v (%+v), want %v: %v", got, f, c.tooOld, err)
			}
		})
	}
}

// A build older than 0.16.3 against MySQL 8.4 is refused before it dumps; the
// fix it names is installing mydumper 0.18.1 or newer.
func TestRunMydumper_positionFloorRefusalIsTooOld(t *testing.T) {
	fakeConsoleMydumper(t, printsVersion(versionDistro))
	stubPreflight(t, nil)
	stubSourceVersion(t, "8.4.9", nil)
	err := runMydumper(context.Background(), "u:p@tcp(127.0.0.1:1)/", config.SSL{Mode: "disabled"}, nil, filepath.Join(t.TempDir(), "out"), baseline.LockModeFTWRL, lockModeFromEnv)
	if err == nil {
		t.Fatal("refusal expected")
	}
	f := snapshotFailureOf(err, console.BaselineRequest{SourceDSN: ownHostDSN})
	if f == nil || f.Kind != console.SnapshotFailureMydumperTooOld || f.MinVersion != mydumperlock.LockModeFloor {
		t.Fatalf("snapshotFailureOf = %+v, want mydumper_too_old %s: %v", f, mydumperlock.LockModeFloor, err)
	}
}

// After the automatic retry, the statement is for what the LAST attempt
// (lock-all) needs: LOCK TABLES, never ftwrl's RELOAD.
func TestExecute_retryFailureNamesLockAllsPermission(t *testing.T) {
	stubRetryDump(t, func(n int, mode baseline.LockMode, _ string) error {
		if mode == baseline.LockModeFTWRL {
			return missingErr(baseline.LockModeFTWRL, "`u`@`%`", "RELOAD")
		}
		return missingErr(baseline.LockModeLockAll, "`u`@`%`", "LOCK TABLES")
	})
	s := newBaselineSupervisor(context.Background(), t.TempDir(), baseline.DefaultLockMode)
	_, err := s.execute(console.BaselineRequest{ServerID: "s1", SourceDSN: ownHostDSN})
	if err == nil {
		t.Fatal("execute succeeded over a failed dump")
	}
	f := snapshotFailureOf(err, console.BaselineRequest{SourceDSN: ownHostDSN})
	if f == nil || f.Grant != "GRANT LOCK TABLES ON *.* TO `u`@`%`;" {
		t.Fatalf("snapshotFailureOf = %+v, want the lock-all GRANT: %v", f, err)
	}
}

// The kind reaches the status the toast and the Overview read, and the run
// record the scheduled line reads; a later success clears it from the status.
func TestFinishDump_carriesTheFailureToStatusAndHistory(t *testing.T) {
	s := newBaselineSupervisor(context.Background(), t.TempDir(), baseline.DefaultLockMode)
	path := filepath.Join(t.TempDir(), "runs.json")
	hist, err := console.OpenBaselineHistory(path)
	if err != nil {
		t.Fatal(err)
	}
	s.history = hist
	req := console.BaselineRequest{ServerID: "s1", ServerName: "one", SourceDSN: ownHostDSN, Trigger: console.BaselineRunTriggerScheduled}
	s.jobs["s1"] = &console.BaselineStatus{State: "running"}
	s.finishDump(req, time.Now(), dumpOutcome{}, nil, 0, 0, fmt.Errorf("dump: %w", missingErr(baseline.LockModeLockAll, "`u`@`%`", "LOCK TABLES")))
	st := s.Status("s1")
	if st.Failure == nil || st.Failure.Kind != console.SnapshotFailureMissingPermission {
		t.Fatalf("status failure = %+v, want missing_permission", st.Failure)
	}
	if st.LastError == "" {
		t.Error("the error text is gone; the Technical details fold has nothing to show")
	}
	// Read back from disk: the scheduled line reads the record after a
	// daemon restart too.
	reopened, err := console.OpenBaselineHistory(path)
	if err != nil {
		t.Fatal(err)
	}
	run, _ := reopened.LastScheduled("s1")
	if run == nil || run.Failure == nil || run.Failure.Grant != "GRANT LOCK TABLES ON *.* TO `u`@`%`;" {
		t.Fatalf("history record = %+v, want the failure with its GRANT", run)
	}
	s.jobs["s1"].State = "running"
	s.finishDump(req, time.Now(), dumpOutcome{}, nil, 0, 0, nil)
	if st := s.Status("s1"); st.Failure != nil {
		t.Errorf("a success kept the old failure: %+v", st.Failure)
	}
}
