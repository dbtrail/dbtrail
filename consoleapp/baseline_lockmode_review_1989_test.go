package consoleapp

import (
	"bytes"
	"context"
	"errors"
	"github.com/dbtrail/dbtrail/internal/config"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/mydumperlock"
)

// Review of #1989. Where a lock mode came from decides what a refusal tells
// the operator: a mode the automatic choice made names no variable nobody
// set, and a SAVED mode is changed where it is saved, since it wins over the
// environment variable.

const envVar = "BINTRAIL_CONSOLE_BASELINE_LOCK_MODE"

// 1. An old or unreadable mydumper, with lock-all picked automatically.
func TestPlanMydumper_automaticLockAllNamesNoVariable(t *testing.T) {
	for _, c := range []struct {
		name, version string
	}{
		{"old build", printsVersion(versionDistro)},
		{"unreadable version", "printf 'garbage\\n'; exit 0"},
	} {
		t.Run(c.name, func(t *testing.T) {
			fakeConsoleMydumper(t, c.version)
			_, err := planMydumper(baseline.LockModeLockAll, lockModeAutomatic)
			if err == nil {
				t.Fatal("lock-all accepted on a build that cannot be told to use it")
			}
			t.Logf("automatic: %v", err)
			for _, want := range []string{"Amazon RDS or Aurora", "refused ftwrl", mydumperlock.LockModeFloor} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("refusal lacks %q: %v", want, err)
				}
			}
			if strings.Contains(err.Error(), envVar) {
				t.Errorf("an automatic choice names a variable nobody set: %v", err)
			}
			// Saved: the remedy is the saved setting, not the variable.
			_, err = planMydumper(baseline.LockModeLockAll, lockModeSaved)
			if err == nil || strings.Contains(err.Error(), envVar) || !strings.Contains(err.Error(), mydumperlock.SavedLockModeEndpoint) {
				t.Errorf("saved: %v; want the saved setting named, not the variable", err)
			}
			// The variable: unchanged.
			if _, err = planMydumper(baseline.LockModeLockAll, lockModeFromEnv); err == nil || !strings.Contains(err.Error(), envVar) {
				t.Errorf("env: %v; want the variable named", err)
			}
		})
	}
}

// 1. The startup line: with nothing chosen, a mydumper that cannot do
// lock-all is announced, since every RDS/Aurora source will fail on it.
func TestMydumperBootWarning_automaticWarnsAboutRDS(t *testing.T) {
	fakeConsoleMydumper(t, printsVersion(versionDistro))
	got := mydumperBootWarning(baseline.LockModeFTWRL, lockModeAutomatic)
	t.Logf("boot line: %s", got)
	if !strings.Contains(got, "Amazon RDS and Aurora") || !strings.Contains(got, mydumperlock.LockModeFloor) {
		t.Errorf("boot line %q does not warn that RDS/Aurora snapshots will fail", got)
	}
	if strings.Contains(got, envVar) {
		t.Errorf("boot line names the variable: %q", got)
	}
	// An operator who chose ftwrl gets no RDS sentence.
	if got := mydumperBootWarning(baseline.LockModeFTWRL, lockModeFromEnv); strings.Contains(got, "Amazon RDS and Aurora") {
		t.Errorf("chosen ftwrl: %q", got)
	}
	// A modern build: nothing to say.
	fakeConsoleMydumper(t, printsVersion(versionModern))
	if got := mydumperBootWarning(baseline.LockModeFTWRL, lockModeAutomatic); got != "" {
		t.Errorf("modern build: %q, want no boot line", got)
	}
}

// 2. A saved mode: the privilege check and the global-lock hint name the
// saved setting, and execute says where it is saved.
func TestRunMydumper_savedModeUsesTheSavedRemedy(t *testing.T) {
	fakeConsoleMydumper(t, printsVersion(versionModern))
	var got mydumperlock.Remedy
	checkMydumperPrivileges = func(_ context.Context, _ string, _ config.SSL, _ baseline.LockMode, r mydumperlock.Remedy, _ []string) error {
		got = r
		return errors.New("stop")
	}
	t.Cleanup(func() { checkMydumperPrivileges = realCheckMydumperPrivileges })
	for src, want := range map[lockModeSource]mydumperlock.Remedy{
		lockModeSaved:     mydumperlock.RemedyConsoleSaved,
		lockModeFromEnv:   mydumperlock.RemedyConsole,
		lockModeAutomatic: mydumperlock.RemedyConsole,
	} {
		_ = runMydumper(context.Background(), "u:p@tcp(127.0.0.1:1)/", config.SSL{Mode: "disabled"}, nil, t.TempDir(), baseline.LockModeFTWRL, src)
		if got != want {
			t.Errorf("source %d: privilege check told remedy %q, want %q", src, got, want)
		}
	}
}

func TestRunMydumper_savedModeGlobalLockHint(t *testing.T) {
	installFake(t, "#!/bin/bash\nif [ \"$1\" = \"--version\" ]; then "+printsVersion(versionModern)+"; fi\n"+
		"printf '%s\\n' \"Couldn't acquire global lock, snapshots will not be consistent: Access denied for user 'admin'@'%'\" >&2\nexit 1\n")
	stubPreflight(t, nil)
	err := runMydumper(context.Background(), "admin:p@tcp(127.0.0.1:1)/", config.SSL{Mode: "disabled"}, nil, t.TempDir(), baseline.LockModeFTWRL, lockModeSaved)
	if err == nil {
		t.Fatal("runMydumper succeeded")
	}
	t.Logf("saved: %v", err)
	if strings.Contains(err.Error(), envVar) || !strings.Contains(err.Error(), mydumperlock.SavedLockModeEndpoint) ||
		!strings.Contains(err.Error(), `{"use_startup":true}`) {
		t.Errorf("a saved ftwrl is told to change the variable, which it wins over: %v", err)
	}
}

func TestExecute_savedModeSaysWhereItIsSaved(t *testing.T) {
	stubRetryDump(t, func(int, baseline.LockMode, string) error { return errDeniedFTWRL })
	s := newBaselineSupervisor(context.Background(), t.TempDir(), baseline.DefaultLockMode)
	s.reg = liveRegistry(t)
	save(t, s.reg, console.BackupSettingLockMode, "ftwrl")
	_, err := s.execute(console.BaselineRequest{ServerID: "s1", SourceDSN: ownHostDSN})
	if err == nil || !strings.Contains(err.Error(), s.reg.Path()) {
		t.Fatalf("err = %v, want the settings file %s named", err, s.reg.Path())
	}
	var d *ftwrlDeniedError
	if !errors.As(err, &d) {
		t.Errorf("the note hid the typed error: %v", err)
	}
}

// 3. The settings API's startup value for the lock mode: "" (automatic)
// when the variable is unset, never the internal ftwrl default.
func TestBootLockModeReport(t *testing.T) {
	prevMode, prevSet := upConsoleBaselineLockMode, upConsoleBaselineLockModeSet
	t.Cleanup(func() { upConsoleBaselineLockMode, upConsoleBaselineLockModeSet = prevMode, prevSet })
	upConsoleBaselineLockMode, upConsoleBaselineLockModeSet = baseline.DefaultLockMode, false
	if got := bootLockModeReport(); got != "" {
		t.Errorf("unset: %q, want \"\" (automatic)", got)
	}
	cfg, err := upConsoleConfig(nil, "user:pass@tcp(127.0.0.1:3306)/idx", consoleOpts{Listen: "127.0.0.1:8090", Token: "tok"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.BackupSettingsDefaults.LockMode != "" {
		t.Errorf("the console is told lock mode %q at startup with the variable unset", cfg.BackupSettingsDefaults.LockMode)
	}
	upConsoleBaselineLockMode, upConsoleBaselineLockModeSet = baseline.LockModeFTWRL, true
	if got := bootLockModeReport(); got != "ftwrl" {
		t.Errorf("set: %q, want ftwrl", got)
	}
}

// 4. A failed retry: both causes in the error, and a Warn line.
func TestExecute_failedRetryKeepsBothCausesAndWarns(t *testing.T) {
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })
	lockAll := errors.New("lock-all baseline mode requires the LOCK TABLES privilege")
	stubRetryDump(t, func(n int, _ baseline.LockMode, _ string) error {
		if n == 1 {
			return &ftwrlDeniedError{err: errors.New("mydumper failed: exit status 1: mydumper could not take the global read lock; output: line one\nline two")}
		}
		return lockAll
	})
	s := newBaselineSupervisor(context.Background(), t.TempDir(), baseline.DefaultLockMode)
	_, err := s.execute(console.BaselineRequest{ServerID: "s1", SourceDSN: ownHostDSN})
	if !errors.Is(err, lockAll) {
		t.Fatalf("err = %v, want the lock-all failure", err)
	}
	t.Logf("error: %v", err)
	if !strings.Contains(err.Error(), "could not take the global read lock") || strings.Contains(err.Error(), "line two") {
		t.Errorf("error %q lacks the ftwrl refusal's first line (without mydumper's output)", err)
	}
	warned := false
	for _, l := range strings.Split(buf.String(), "\n") {
		if strings.Contains(l, "level=WARN") && strings.Contains(l, "automatic retry") && strings.Contains(l, "global read lock") {
			warned = true
		}
	}
	if !warned {
		t.Errorf("no Warn line for the failed retry naming the ftwrl refusal:\n%s", buf.String())
	}
}

// 5. Unset is automatic now: no text calls ftwrl "the default".
func TestNoTextCallsFTWRLTheConsoleDefault(t *testing.T) {
	raw, err := os.ReadFile("baseline.go")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "ftwrl default") {
		t.Error("baseline.go still says unsetting the variable gives the ftwrl default; unset is automatic")
	}
}

// 6. A failed attempt's folder that cannot be removed is logged with its path.
func TestDumpAttempt_logsAFolderItCannotRemove(t *testing.T) {
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })
	var locked string
	stubRetryDump(t, func(_ int, _ baseline.LockMode, dir string) error {
		locked = filepath.Join(dir, "locked")
		if err := os.MkdirAll(locked, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(locked, "f"), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(locked, 0o500); err != nil {
			t.Fatal(err)
		}
		return errors.New("fake mydumper stopped")
	})
	s := newBaselineSupervisor(context.Background(), t.TempDir(), baseline.DefaultLockMode)
	// After TempDir: cleanups run last-in first-out, and the folder must be
	// writable again before TempDir removes it.
	t.Cleanup(func() { _ = os.Chmod(locked, 0o755) })
	if _, err := s.execute(console.BaselineRequest{ServerID: "s1", SourceDSN: ownHostDSN}); err == nil {
		t.Fatal("execute succeeded")
	}
	if os.Geteuid() == 0 {
		t.Skip("root removes the folder anyway")
	}
	if !strings.Contains(buf.String(), "could not remove the folder") || !strings.Contains(buf.String(), filepath.Dir(locked)) {
		t.Errorf("no Warn line with the folder's path:\n%s", buf.String())
	}
}
