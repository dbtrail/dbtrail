package cliapp

import (
	"slices"
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// syncModeArg returns the value passed to mydumper's --sync-thread-lock-mode,
// or "" when the flag is absent.
func syncModeArg(t *testing.T, args []string) string {
	t.Helper()
	i := slices.Index(args, "--sync-thread-lock-mode")
	if i < 0 || i+1 >= len(args) {
		return ""
	}
	return args[i+1]
}

// TestBuildMydumperArgsCarriesLockMode tests the WIRING, not the mapping:
// baseline.LockMode.MydumperValue is unit-tested on its own, but a builder
// that computed the right string and then passed a hardcoded one would still
// pass that test. This drives the real argv.
func TestBuildMydumperArgsCarriesLockMode(t *testing.T) {
	for _, tc := range []struct {
		mode baseline.LockMode
		want string
	}{
		{baseline.LockModeFTWRL, "FTWRL"},
		{baseline.LockModeLockAll, "LOCK_ALL"},
		{baseline.LockModeSafeNoLock, "SAFE_NO_LOCK"},
		{baseline.LockModeNoLock, "NO_LOCK"},
	} {
		args := buildMydumperArgs("127.0.0.1", 3306, "root", "", "", "/tmp/dump", 4, nil, nil, "", true, tc.mode)
		if got := syncModeArg(t, args); got != tc.want {
			t.Errorf("lock mode %s produced --sync-thread-lock-mode %q, want %q", tc.mode, got, tc.want)
		}
	}
}

// TestDumpLockModeFlagDefaultsToConsistent guards the operator-facing surface.
// The library default is pinned in internal/baseline; this pins that the CLI
// actually offers it, so a flag registered with the old "NO_LOCK" string would
// fail here even though the library default was untouched.
func TestDumpLockModeFlagDefaultsToConsistent(t *testing.T) {
	f := dumpCmd.Flags().Lookup("lock-mode")
	if f == nil {
		t.Fatal("dump has no --lock-mode flag; the lock mode would be unreachable from the CLI")
	}
	mode, err := baseline.ParseLockMode(f.DefValue)
	if err != nil {
		t.Fatalf("--lock-mode default %q does not parse: %v", f.DefValue, err)
	}
	if !mode.PointConsistent() {
		t.Errorf("--lock-mode defaults to %s, which can emit a torn snapshot", mode)
	}
	if mode != baseline.DefaultLockMode {
		t.Errorf("--lock-mode default %s disagrees with baseline.DefaultLockMode %s; the CLI and the library must not drift", mode, baseline.DefaultLockMode)
	}
}

// TestBuildMydumperArgsOmitsLockModeOnOldMydumper: pre-0.18 builds reject the
// flag outright (#219, #460), so it must be dropped rather than passed. The
// run path refuses when the operator asked for a specific mode — this only
// pins that the builder itself stays silent.
func TestBuildMydumperArgsOmitsLockModeOnOldMydumper(t *testing.T) {
	args := buildMydumperArgs("127.0.0.1", 3306, "root", "", "", "/tmp/dump", 4, nil, nil, "", false, baseline.LockModeFTWRL)
	if got := syncModeArg(t, args); got != "" {
		t.Errorf("old mydumper got --sync-thread-lock-mode %q; the flag does not exist there and the dump would fail", got)
	}
	if slices.Contains(args, "--trx-tables") {
		t.Error("old mydumper got --trx-tables, which it also rejects")
	}
}
