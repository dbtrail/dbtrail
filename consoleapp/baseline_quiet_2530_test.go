package consoleapp

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/mydumperlock"
)

// stubQuietWait replaces the wait for a quiet source and returns what it saw.
func stubQuietWait(t *testing.T, fn func(baseline.LockMode, mydumperlock.Remedy) (time.Duration, error)) {
	t.Helper()
	prev := waitForQuietSource
	t.Cleanup(func() { waitForQuietSource = prev })
	waitForQuietSource = func(_ context.Context, _ string, _ config.SSL, mode baseline.LockMode, remedy mydumperlock.Remedy) (time.Duration, error) {
		return fn(mode, remedy)
	}
}

// #2530: the instant that names the snapshot, and both marks that date it,
// are taken AFTER the wait for a quiet source. Taken before, a wait of ten
// minutes would label a snapshot ten minutes before its data, and a
// point-in-time read inside that window would get later data with no warning.
func TestDumpAttempt_stampsTheSnapshotAfterTheQuietWait(t *testing.T) {
	var waitEnded time.Time
	stubQuietWait(t, func(baseline.LockMode, mydumperlock.Remedy) (time.Duration, error) {
		time.Sleep(30 * time.Millisecond)
		waitEnded = time.Now()
		return 30 * time.Millisecond, nil
	})
	var ddlAt, eventAt, dumpAt time.Time
	prevDDL, prevEvent, prevRun := dumpDDLMarkFunc, dumpEventMarkFunc, runMydumperFunc
	t.Cleanup(func() { dumpDDLMarkFunc, dumpEventMarkFunc, runMydumperFunc = prevDDL, prevEvent, prevRun })
	dumpDDLMarkFunc = func(console.BaselineRequest) string { ddlAt = time.Now(); return "" }
	dumpEventMarkFunc = func(console.BaselineRequest) string { eventAt = time.Now(); return "" }
	runMydumperFunc = func(ctx context.Context, _ string, _ config.SSL, _ []string, _ string, _ baseline.LockMode, _ lockModeSource) error {
		// What runMydumper does after its own checks, right before mydumper.
		if err := runBeforeMydumper(ctx); err != nil {
			return err
		}
		dumpAt = time.Now()
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	sup := newBaselineSupervisor(ctx, t.TempDir(), baseline.DefaultLockMode)
	a, err := sup.dumpAttempt(console.BaselineRequest{ServerID: "s", ServerName: "s", SourceDSN: "u:p@tcp(127.0.0.1:1)/"},
		baseline.LockModeFTWRL, lockModeFromEnv)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(a.dir)
	if waitEnded.IsZero() {
		t.Fatal("the dump never waited for a quiet source")
	}
	for name, at := range map[string]time.Time{"snapshot instant": a.startedAt, "DDL mark": ddlAt, "event mark": eventAt, "mydumper": dumpAt} {
		if at.Before(waitEnded) {
			t.Errorf("%s taken at %v, before the wait ended at %v", name, at, waitEnded)
		}
	}
}

// A source never quiet enough: no folder, no mydumper, and the card says why
// in words that fit it, remedy included.
func TestDumpAttempt_aSourceNeverQuietIsRefusedBeforeAnything(t *testing.T) {
	var gotRemedy mydumperlock.Remedy
	stubQuietWait(t, func(_ baseline.LockMode, remedy mydumperlock.Remedy) (time.Duration, error) {
		gotRemedy = remedy
		return 10 * time.Minute, &mydumperlock.QuietWaitError{Waited: 10 * time.Minute, Busy: []mydumperlock.Busy{{ID: 7, User: "app", Command: "Query", Seconds: 900}}}
	})
	prevRun := runMydumperFunc
	t.Cleanup(func() { runMydumperFunc = prevRun })
	ran := false
	runMydumperFunc = func(ctx context.Context, _ string, _ config.SSL, _ []string, _ string, _ baseline.LockMode, _ lockModeSource) error {
		if err := runBeforeMydumper(ctx); err != nil {
			return err
		}
		ran = true
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	staging := t.TempDir()
	sup := newBaselineSupervisor(ctx, staging, baseline.DefaultLockMode)
	req := console.BaselineRequest{ServerID: "s", ServerName: "s", SourceDSN: "u:p@tcp(127.0.0.1:1)/"}
	_, err := sup.dumpAttempt(req, baseline.LockModeFTWRL, lockModeSaved)
	var qe *mydumperlock.QuietWaitError
	if !errors.As(err, &qe) {
		t.Fatalf("err = %v, want a QuietWaitError", err)
	}
	if ran {
		t.Error("mydumper ran after the source was never quiet")
	}
	if left, _ := os.ReadDir(staging); len(left) != 0 {
		t.Errorf("the refused read left %d entries in the working folder", len(left))
	}
	if gotRemedy != mydumperlock.RemedyConsoleSaved {
		t.Errorf("remedy = %q, want the saved setting's", gotRemedy)
	}
	f := snapshotFailureOf(err, req)
	if f == nil || !strings.Contains(f.Summary, "Nothing was locked") || len([]rune(f.Summary)) > 300 {
		t.Fatalf("card summary = %+v", f)
	}
	t.Logf("card: %s", f.Summary)
}

// The wait comes after runMydumper's own checks. On a managed source whose
// host name does not say so, the default ftwrl is refused by the privilege
// check in seconds, and that refusal is what execute retries with lock-all
// (#1986); waiting first would spend ten minutes on a lock that could never be
// taken, then fail without the retry.
func TestRunMydumper_privilegeRefusalComesBeforeTheQuietWait(t *testing.T) {
	waited := false
	stubQuietWait(t, func(baseline.LockMode, mydumperlock.Remedy) (time.Duration, error) { waited = true; return 0, nil })
	dir := t.TempDir()
	script := "#!/bin/bash\n" +
		"if [ \"$1\" = \"--version\" ]; then printf '%s\\n' '" + versionModern + "'; exit 0; fi\n" +
		"exit 0\n"
	if err := os.WriteFile(filepath.Join(dir, "mydumper"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir)
	stubPreflight(t, mydumperlock.ErrFTWRLPrivilegesMissing)
	ctx := withBeforeMydumper(context.Background(), func() error {
		_, err := waitForQuietSource(context.Background(), "", config.SSL{}, baseline.LockModeFTWRL, mydumperlock.RemedyConsole)
		return err
	})
	err := runMydumper(ctx, "admin:p@tcp(127.0.0.1:1)/", config.SSL{Mode: "disabled"}, []string{"appdb"},
		filepath.Join(t.TempDir(), "out"), baseline.LockModeFTWRL, lockModeFromEnv)
	if !errors.Is(err, mydumperlock.ErrFTWRLPrivilegesMissing) {
		t.Fatalf("err = %v, want the privilege refusal", err)
	}
	if waited {
		t.Error("the wait for a quiet source ran before the privilege check refused")
	}
}
