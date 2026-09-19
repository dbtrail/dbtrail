package consoleapp

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/mydumperlock"
)

// TestConsoleBaselineDefaultsToConsistent: the console's Create-baseline button
// and the `bintrail dump` CLI must not disagree about what a baseline is. This
// pins the console half of that — a torn snapshot is not something an operator
// should get without asking (#1377).
func TestConsoleBaselineDefaultsToConsistent(t *testing.T) {
	if !upConsoleBaselineLockMode.PointConsistent() {
		t.Fatalf("console baseline defaults to %s, which can emit a torn snapshot", upConsoleBaselineLockMode)
	}
	if upConsoleBaselineLockMode != baseline.DefaultLockMode {
		t.Errorf("console default %s disagrees with baseline.DefaultLockMode %s; the two baseline surfaces must not drift",
			upConsoleBaselineLockMode, baseline.DefaultLockMode)
	}
}

// TestBuildConsoleMydumperArgsCarriesLockMode drives the real argv builder, so
// a builder that ignored its argument and hardcoded a mode fails here.
func TestBuildConsoleMydumperArgsCarriesLockMode(t *testing.T) {
	for _, tc := range []struct {
		mode baseline.LockMode
		want string
	}{
		{baseline.LockModeFTWRL, "FTWRL"},
		{baseline.LockModeLockAll, "LOCK_ALL"},
		{baseline.LockModeSafeNoLock, "SAFE_NO_LOCK"},
		{baseline.LockModeNoLock, "NO_LOCK"},
	} {
		args := buildConsoleMydumperArgs("127.0.0.1", 3306, "root", []string{"demo"}, "/tmp/d", tc.mode, true)
		i := slices.Index(args, "--sync-thread-lock-mode")
		if i < 0 || i+1 >= len(args) {
			t.Fatalf("mode %s: no --sync-thread-lock-mode in argv", tc.mode)
		}
		if args[i+1] != tc.want {
			t.Errorf("mode %s produced %q, want %q", tc.mode, args[i+1], tc.want)
		}
	}
}

// TestRunMydumperForwardsTheSelectedModeToThePreflight is the console's half of
// the CLI's identically-named test. The console had NO preflight test at all,
// so hardcoding a mode at this call site passed the whole package.
//
// This is the surface where it matters most: the console is where the
// **Create baseline** button lives, and BINTRAIL_CONSOLE_BASELINE_LOCK_MODE is
// the only way its operator can select lock-all. Judging their choice against
// ftwrl's requirements is #1381 — a refusal demanding BACKUP_ADMIN, which
// managed MySQL will not grant under any circumstances.
func TestRunMydumperForwardsTheSelectedModeToThePreflight(t *testing.T) {
	var got baseline.LockMode
	var gotRemedy mydumperlock.Remedy
	var gotSchemas []string
	sentinel := errors.New("stop after preflight")
	checkMydumperPrivileges = func(_ context.Context, _ string, m baseline.LockMode, r mydumperlock.Remedy, sch []string) error {
		got, gotRemedy, gotSchemas = m, r, sch
		return sentinel
	}
	t.Cleanup(func() { checkMydumperPrivileges = mydumperlock.CheckPrivileges })
	// A modern mydumper on PATH: the version probe (#1688) runs before the
	// preflight, and with no binary at all the run would stop there instead.
	fakeConsoleMydumper(t, printsVersion(versionModern))

	err := runMydumper(context.Background(), "u:p@tcp(127.0.0.1:1)/db", []string{"appdb"},
		t.TempDir(), baseline.LockModeLockAll)
	if !errors.Is(err, sentinel) {
		t.Fatalf("runMydumper err = %v, want the preflight's own error to propagate unchanged", err)
	}
	if got != baseline.LockModeLockAll {
		t.Errorf("preflight judged %q, but the console was configured for lock-all — on RDS this refuses a working config", got)
	}
	if gotRemedy != mydumperlock.RemedyConsole {
		t.Errorf("remedy = %q, want the console's own knob named in the refusal", gotRemedy)
	}
	if !slices.Equal(gotSchemas, []string{"appdb"}) {
		t.Errorf("schemas = %v, want the request's own schema filter", gotSchemas)
	}
}
