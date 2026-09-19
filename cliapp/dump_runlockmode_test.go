package cliapp

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/mydumperlock"
)

// fakeMydumperModern writes a fake mydumper that reports a version NEW ENOUGH to
// accept --sync-thread-lock-mode, and records the argv it was called with.
//
// The version string is the whole point, twice over.
//
// It must be new enough: the pre-existing runDump tests use a fake reporting
// 0.15.0, which sets supportsLockMode=false and makes the entire lock-mode
// region of runDump structurally unreachable — so none of it was covered,
// including the privilege preflight that keeps mydumper from segfaulting.
//
// And it must be a REAL shape (#1686). It used to read "mydumper 0.18.0 (built
// with foo)", impossible twice over: no 0.18.0 was ever released (see
// mydumperlock.Version.SupportsLockMode) and every build measured from 0.16.3 up carries the
// "v" prefix. With a string a binary actually prints, the three tests below
// become regression tests for the parser half — revert the "v" strip and they
// go red, because the version reads as "very old mydumper" again.
func fakeMydumperModern(t *testing.T, dir string) (bin, record string) {
	t.Helper()
	return fakeMydumperVersion(t, dir, "mydumper v0.18.1, built against MySQL 8.0.36 with SSL support")
}

// newDumpCmdForTest returns a command carrying a context. runDump passes
// cmd.Context() to the privilege preflight, and a cobra command built without
// one returns nil — the repo's recorded gotcha (a nil context hangs or panics
// rather than failing cleanly).
func newDumpCmdForTest(t *testing.T) *cobra.Command {
	t.Helper()
	cmd := &cobra.Command{RunE: runDump}
	cmd.Flags().StringVar(&dmpLockMode, "lock-mode", "ftwrl", "")
	cmd.Flags().StringVar(&dmpMydumperPath, "mydumper-path", "mydumper", "")
	cmd.SetContext(context.Background())
	return cmd
}

// TestRunDumpDefaultModeChecksPrivilegesBeforeDumping asserts the ABSENCE of a
// side effect, not just an error: with the point-consistent default, mydumper
// must never be launched against a source whose privileges were not verified.
// Granting BACKUP_ADMIN without RELOAD makes the pinned build SEGFAULT rather
// than fail cleanly, so this guard is what keeps that crash unreachable — and
// #1377 made this the DEFAULT path of the surface most operators use.
func TestRunDumpDefaultModeChecksPrivilegesBeforeDumping(t *testing.T) {
	dir := t.TempDir()
	bin, record := fakeMydumperModern(t, dir)

	stubPingSource(t)
	dumpLockDir = func() string { return dir }
	t.Cleanup(func() { dumpLockDir = os.TempDir })

	dmpSourceDSN = "u:p@tcp(127.0.0.1:1)/" // nothing listens: the probe cannot pass
	dmpOutputDir = filepath.Join(dir, "out")
	dmpMydumperPath = bin
	dmpLockMode = "ftwrl"
	dmpFormat = "text"
	t.Cleanup(func() { dmpLockMode = "ftwrl"; dmpSourceDSN = ""; dmpOutputDir = "" })

	cmd := newDumpCmdForTest(t)
	if err := cmd.Flags().Set("mydumper-path", bin); err != nil {
		t.Fatal(err)
	}
	err := runDump(cmd, nil)
	if err == nil {
		t.Fatal("the default lock mode dumped without verifying privileges; the segfault path is reachable again")
	}
	if _, statErr := os.Stat(record); statErr == nil {
		t.Error("mydumper ran even though the privilege probe failed — the preflight must gate the launch, not just report")
	}
}

// TestRunDumpSafeNoLockSkipsPreflightAndCarriesTheMode covers the other half:
// a mode that needs no elevated privilege must not be gated by the probe, and
// the mode the operator chose must reach mydumper's argv. Passing no-lock here
// instead would prove nothing — a builder that ignored its argument and
// hardcoded NO_LOCK would produce identical argv.
func TestRunDumpSafeNoLockSkipsPreflightAndCarriesTheMode(t *testing.T) {
	dir := t.TempDir()
	bin, record := fakeMydumperModern(t, dir)

	stubPingSource(t)
	dumpLockDir = func() string { return dir }
	t.Cleanup(func() { dumpLockDir = os.TempDir })

	dmpSourceDSN = "u:p@tcp(127.0.0.1:1)/"
	dmpOutputDir = filepath.Join(dir, "out")
	dmpMydumperPath = bin
	dmpLockMode = "safe-no-lock"
	dmpFormat = "text"
	t.Cleanup(func() { dmpLockMode = "ftwrl"; dmpSourceDSN = ""; dmpOutputDir = "" })

	cmd := newDumpCmdForTest(t)
	if err := cmd.Flags().Set("mydumper-path", bin); err != nil {
		t.Fatal(err)
	}
	if err := cmd.Flags().Set("lock-mode", "safe-no-lock"); err != nil {
		t.Fatal(err)
	}
	if err := runDump(cmd, nil); err != nil {
		t.Fatalf("safe-no-lock was blocked by the privilege preflight it does not need: %v", err)
	}

	argv, readErr := os.ReadFile(record)
	if readErr != nil {
		t.Fatalf("mydumper never ran: %v", readErr)
	}
	if !strings.Contains(string(argv), "SAFE_NO_LOCK") {
		t.Errorf("argv = %q; the operator asked for safe-no-lock and mydumper was told something else", argv)
	}
}

// TestRunDumpForwardsTheSelectedModeToThePreflight closes a gap that a green
// suite hid: nothing verified that the mode the OPERATOR chose is the mode the
// privilege check judges. Hardcoding baseline.LockModeFTWRL at the call site
// passed every other test in this package, because an unreachable source fails
// identically for every mode.
//
// The consequence of that mutation is exactly #1381: an RDS operator passes
// --lock-mode lock-all, is judged against ftwrl's requirements, and is told to
// grant BACKUP_ADMIN — which RDS refuses to grant at all.
func TestRunDumpForwardsTheSelectedModeToThePreflight(t *testing.T) {
	dir := t.TempDir()
	bin, _ := fakeMydumperModern(t, dir)

	stubPingSource(t)
	dumpLockDir = func() string { return dir }
	t.Cleanup(func() { dumpLockDir = os.TempDir })

	var got baseline.LockMode
	var gotRemedy mydumperlock.Remedy
	var gotSchemas []string
	checkMydumperPrivileges = func(_ context.Context, _ string, m baseline.LockMode, r mydumperlock.Remedy, sch []string) error {
		got, gotRemedy, gotSchemas = m, r, sch
		return errStopAfterPreflight
	}
	t.Cleanup(func() { checkMydumperPrivileges = mydumperlock.CheckPrivileges })

	dmpSourceDSN = "u:p@tcp(127.0.0.1:1)/"
	dmpOutputDir = filepath.Join(dir, "out")
	dmpMydumperPath = bin
	// NOTE: dmpLockMode is deliberately NOT set here — newDumpCmdForTest's
	// StringVar registration resets it, so only the Flags().Set below carries
	// the mode. Assigning it here would read as load-bearing and be inert.
	dmpSchemas = "appdb"
	dmpFormat = "text"
	t.Cleanup(func() { dmpLockMode = "ftwrl"; dmpSchemas = ""; dmpSourceDSN = ""; dmpOutputDir = "" })

	cmd := newDumpCmdForTest(t)
	if err := cmd.Flags().Set("mydumper-path", bin); err != nil {
		t.Fatal(err)
	}
	if err := cmd.Flags().Set("lock-mode", "lock-all"); err != nil {
		t.Fatal(err)
	}
	if err := runDump(cmd, nil); !errors.Is(err, errStopAfterPreflight) {
		t.Fatalf("runDump err = %v, want the preflight's own error to propagate unchanged", err)
	}
	if got != baseline.LockModeLockAll {
		t.Errorf("preflight judged %q, but the operator asked for lock-all — on RDS this refuses a working config", got)
	}
	if gotRemedy != mydumperlock.RemedyCLI {
		t.Errorf("remedy = %q, want the CLI's own knob named in the refusal", gotRemedy)
	}
	// The schema list decides whether a partial REVOKE applies to THIS dump.
	// Forwarding the wrong one turns a provable refusal into a guess.
	if !slices.Equal(gotSchemas, []string{"appdb"}) {
		t.Errorf("schemas = %v, want the dump's own --schemas filter", gotSchemas)
	}
}

var errStopAfterPreflight = errors.New("stop after preflight")

// fakeMydumperVersion writes a fake mydumper that answers --version with the
// given line and records the argv of any real invocation, so a test can assert
// that mydumper was NEVER launched.
func fakeMydumperVersion(t *testing.T, dir, versionLine string) (bin, record string) {
	t.Helper()
	bin = filepath.Join(dir, "mydumper")
	record = filepath.Join(dir, "argv.txt")
	// SINGLE-quoted, not strconv.Quote into a double-quoted string. Go escaping
	// is not bash escaping: inside "..." a $, a backtick or a backslash stays
	// live, so a pasted --version line containing one would make the fake print
	// something the test never asked for — a false green in the one helper
	// whose entire job is fidelity to real mydumper output. Single quotes are
	// inert in bash; the only character needing care is the quote itself.
	script := "#!/bin/bash\n" +
		"if [ \"$1\" = \"--version\" ]; then printf '%s\\n' '" +
		strings.ReplaceAll(versionLine, "'", `'\''`) + "'; exit 0; fi\n" +
		"echo \"$@\" > " + record + "\n" +
		"exit 0\n"
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake mydumper: %v", err)
	}
	return bin, record
}

// TestFakeMydumperVersionReportsVerbatim guards the fixture itself. Every
// version string in this file is now pasted from a real `mydumper --version`,
// so if the fake alters what it was handed, the tests below assert against
// output no binary ever produced — and they still pass, which is the worst
// shape a test can have. The metacharacters are the point: under the previous
// double-quoted form bash would have expanded them.
func TestFakeMydumperVersionReportsVerbatim(t *testing.T) {
	for _, want := range []string{
		"mydumper v1.0.5-1, built against MariaDB 10.8.8 with SSL support",
		`mydumper v1.0.5-1, built from $HOME/src with "quotes" and a \backslash`,
		"mydumper v1.0.5-1, built in `pwd` with 100% coverage",
		"mydumper v1.0.5-1, O'Brien build",
	} {
		dir := t.TempDir()
		bin, _ := fakeMydumperVersion(t, dir, want)
		out, err := exec.Command(bin, "--version").Output()
		if err != nil {
			t.Fatalf("run fake mydumper: %v", err)
		}
		if got := strings.TrimRight(string(out), "\n"); got != want {
			t.Errorf("fake printed %q, want %q — the fixture is not faithful to what it was given", got, want)
		}
	}
}

// TestRunDumpUnreadableVersionStillChecksPrivileges pins the half of #1686 that
// outlives the parser fix: a version we could not READ must not switch off the
// privilege preflight.
//
// This is the state every stock 1.x install was in — the "v" prefix made the
// parse fail — so the guard that keeps the #800 segfault unreachable was off on
// the DEFAULT path, for the newest mydumper available. Stripping the "v" fixes
// the shape we have SEEN; this fixes what the next unrecognised shape does.
//
// The mode is left at the ftwrl default deliberately: setting --lock-mode would
// trip the "needs mydumper 0.18.1 or newer" refusal earlier in runDump and the
// test would pass without ever reaching the preflight.
func TestRunDumpUnreadableVersionStillChecksPrivileges(t *testing.T) {
	dir := t.TempDir()
	bin, record := fakeMydumperVersion(t, dir, "mydumper built from source")

	stubPingSource(t)
	dumpLockDir = func() string { return dir }
	t.Cleanup(func() { dumpLockDir = os.TempDir })

	called := false
	checkMydumperPrivileges = func(_ context.Context, _ string, _ baseline.LockMode, _ mydumperlock.Remedy, _ []string) error {
		called = true
		return errStopAfterPreflight
	}
	t.Cleanup(func() { checkMydumperPrivileges = mydumperlock.CheckPrivileges })

	dmpSourceDSN = "u:p@tcp(127.0.0.1:1)/"
	dmpOutputDir = filepath.Join(dir, "out")
	dmpMydumperPath = bin
	dmpFormat = "text"
	t.Cleanup(func() { dmpLockMode = "ftwrl"; dmpSourceDSN = ""; dmpOutputDir = "" })

	cmd := newDumpCmdForTest(t)
	if err := cmd.Flags().Set("mydumper-path", bin); err != nil {
		t.Fatal(err)
	}
	err := runDump(cmd, nil)
	// `called` is checked BEFORE the error: the error check is a Fatalf, so
	// asserting it first would abort the test and this diagnostic — the one
	// that names what actually broke — could never print.
	if !called {
		t.Fatal("the privilege preflight was skipped because the mydumper version could not be read — " +
			"the #800 segfault guard is off on the default path for exactly the builds we know least about")
	}
	if !errors.Is(err, errStopAfterPreflight) {
		t.Fatalf("runDump err = %v, want the preflight's own error to stop the dump", err)
	}
	if _, statErr := os.Stat(record); statErr == nil {
		t.Error("mydumper ran even though the preflight refused — the check must gate the launch, not just report")
	}
}

// TestRunDumpLockModeRefusalNamesTheRealReason pins the OTHER half of the
// message split (#1686). Both reviewers reached this by following the product's
// own advice: every refusal the privilege preflight prints offers
// `--lock-mode <mode>` as the way out, and on the unreadable path taking that
// advice lands on the refusal asserted here. When both states shared one
// message, that second message told an operator their mydumper was older than
// 0.18 — about a binary whose version was never read, and which in the
// originally reported case was NEWER than the floor.
//
// The two subtests must not be merged: asserting only that each errors passes
// against a single shared message, which is the bug.
func TestRunDumpLockModeRefusalNamesTheRealReason(t *testing.T) {
	cases := []struct {
		name        string
		version     string
		wantContain string
		wantAbsent  string
	}{
		{
			name:        "unreadable_version_does_not_claim_the_build_is_old",
			version:     "mydumper built from source",
			wantContain: "version could not be read",
			// The false assertion this split exists to remove.
			wantAbsent: "0.18.1 or newer",
		},
		{
			name:    "positively_old_build_still_says_upgrade",
			version: "mydumper 0.15.0 (built with foo)",
			// Unchanged for a build we actually read: upgrading IS the remedy.
			wantContain: "0.18.1 or newer",
			wantAbsent:  "could not be read",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			bin, _ := fakeMydumperVersion(t, dir, tc.version)

			stubPingSource(t)
			dumpLockDir = func() string { return dir }
			t.Cleanup(func() { dumpLockDir = os.TempDir })

			dmpSourceDSN = "u:p@tcp(127.0.0.1:1)/"
			dmpOutputDir = filepath.Join(dir, "out")
			dmpMydumperPath = bin
			dmpFormat = "text"
			t.Cleanup(func() { dmpLockMode = "ftwrl"; dmpSourceDSN = ""; dmpOutputDir = "" })

			cmd := newDumpCmdForTest(t)
			if err := cmd.Flags().Set("mydumper-path", bin); err != nil {
				t.Fatal(err)
			}
			// lock-all is what the preflight's own refusal tells the operator
			// to pass, so this is the exact command that advice produces.
			if err := cmd.Flags().Set("lock-mode", "lock-all"); err != nil {
				t.Fatal(err)
			}
			err := runDump(cmd, nil)
			if err == nil {
				t.Fatal("an explicit --lock-mode was accepted although the flag is not being sent to mydumper")
			}
			if !strings.Contains(err.Error(), tc.wantContain) {
				t.Errorf("refusal = %q, want it to say %q", err, tc.wantContain)
			}
			if strings.Contains(err.Error(), tc.wantAbsent) {
				t.Errorf("refusal = %q, must not claim %q — it is not what the probe established", err, tc.wantAbsent)
			}
		})
	}
}

// TestRunDumpKnownOldMydumperStillSkipsPreflight is the companion that pins the
// OTHER half of the asymmetry, and it is green both before and after #1686 on
// purpose: it is not a regression test for the bug, it is what stops the fix
// from being widened later into "always check".
//
// Skipping for a build we positively read as pre-0.18 is deliberate.
// requiresBackupAdmin decides from the SERVER's version, so demanding
// BACKUP_ADMIN from an old mydumper that may never issue LOCK INSTANCE FOR
// BACKUP would refuse a dump that works today — and 0.10.1 is what Ubuntu 24.04
// and Debian bookworm package. Without this test nothing distinguishes
// "unreadable" from "old", and the next refactor collapses them back into one
// flag with no suite going red.
func TestRunDumpKnownOldMydumperStillSkipsPreflight(t *testing.T) {
	dir := t.TempDir()
	// 0.10 is the shape Ubuntu 24.04 and Debian bookworm package, and the one
	// MEASURED to issue no LOCK INSTANCE FOR BACKUP at all.
	bin, record := fakeMydumperVersion(t, dir, "mydumper 0.10.1 (built with foo)")

	stubPingSource(t)
	dumpLockDir = func() string { return dir }
	t.Cleanup(func() { dumpLockDir = os.TempDir })

	called := false
	checkMydumperPrivileges = func(_ context.Context, _ string, _ baseline.LockMode, _ mydumperlock.Remedy, _ []string) error {
		called = true
		return errStopAfterPreflight
	}
	t.Cleanup(func() { checkMydumperPrivileges = mydumperlock.CheckPrivileges })

	dmpSourceDSN = "u:p@tcp(127.0.0.1:1)/"
	dmpOutputDir = filepath.Join(dir, "out")
	dmpMydumperPath = bin
	dmpFormat = "text"
	// NOTE: dmpLockMode is deliberately left at the ftwrl default that
	// newDumpCmdForTest registers. NeedsElevatedPrivileges() is true for it, so
	// the version verdict is the ONLY thing that can skip the preflight — a
	// low-privilege mode would skip for its own reason and prove nothing.
	t.Cleanup(func() { dmpLockMode = "ftwrl"; dmpSourceDSN = ""; dmpOutputDir = "" })

	cmd := newDumpCmdForTest(t)
	if err := cmd.Flags().Set("mydumper-path", bin); err != nil {
		t.Fatal(err)
	}
	err := runDump(cmd, nil)
	// Checked before the error for the same reason as its sibling: the error
	// assertion is fatal, and if the preflight ran it fails with the stub's own
	// error, hiding the real diagnostic behind an unrelated message.
	if called {
		t.Fatal("the privilege preflight ran for a build measured to take no backup lock; it is judged against " +
			"BACKUP_ADMIN, which such a build never uses — that refuses a dump that works today")
	}
	if err != nil {
		t.Fatalf("a 0.10 mydumper was blocked by a preflight it does not need: %v", err)
	}
	if _, statErr := os.Stat(record); statErr != nil {
		t.Errorf("mydumper never ran: %v", statErr)
	}
}

// TestRunDumpBrokenBinaryIsNamedNotAPrivilegeGap pins #1699: a mydumper that
// does not run at all used to fall into the "version unknown" branch, which
// runs the privilege preflight, so the first hard error named BACKUP_ADMIN for a
// binary that never executed. It must now be refused as what it is, before the
// preflight and before anything is launched.
func TestRunDumpBrokenBinaryIsNamedNotAPrivilegeGap(t *testing.T) {
	dir := t.TempDir()
	bin := filepath.Join(dir, "mydumper")
	record := filepath.Join(dir, "argv.txt")
	// What a build whose dynamic linker cannot resolve a library does: the
	// loader complains on stderr and the process exits 127 before main runs.
	script := "#!/bin/bash\n" +
		"if [ \"$1\" = \"--version\" ]; then printf 'mydumper: error while loading shared libraries: libmysqlclient.so.21: cannot open shared object file\\n' >&2; exit 127; fi\n" +
		"echo \"$@\" > " + record + "\nexit 0\n"
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}

	stubPingSource(t)
	dumpLockDir = func() string { return dir }
	t.Cleanup(func() { dumpLockDir = os.TempDir })

	called := false
	checkMydumperPrivileges = func(_ context.Context, _ string, _ baseline.LockMode, _ mydumperlock.Remedy, _ []string) error {
		called = true
		return errors.New("missing BACKUP_ADMIN")
	}
	t.Cleanup(func() { checkMydumperPrivileges = mydumperlock.CheckPrivileges })

	dmpSourceDSN = "u:p@tcp(127.0.0.1:1)/"
	dmpOutputDir = filepath.Join(dir, "out")
	dmpMydumperPath = bin
	dmpFormat = "text"
	t.Cleanup(func() { dmpLockMode = "ftwrl"; dmpSourceDSN = ""; dmpOutputDir = "" })

	cmd := newDumpCmdForTest(t)
	if err := cmd.Flags().Set("mydumper-path", bin); err != nil {
		t.Fatal(err)
	}
	err := runDump(cmd, nil)
	if called {
		t.Fatal("a mydumper that does not run reached the privilege preflight; the operator is told about BACKUP_ADMIN instead of the broken binary")
	}
	if !errors.Is(err, mydumperlock.ErrNotRunnable) || !strings.Contains(err.Error(), "libmysqlclient.so.21") {
		t.Fatalf("runDump err = %v, want a refusal carrying the loader's own complaint", err)
	}
	if _, statErr := os.Stat(record); statErr == nil {
		t.Error("mydumper was launched after its --version failed to run")
	}
}

// TestRunDumpRefusesADumpWithNoPositionAndKeepsThePreviousOne (#1688): mydumper
// older than 0.16.3 exits 0 against MySQL 8.4 with no binlog position in its
// metadata (measured). That dump cannot seed a baseline anything is folded onto,
// so `bintrail dump` must fail, and must not replace the previous good dump.
func TestRunDumpRefusesADumpWithNoPositionAndKeepsThePreviousOne(t *testing.T) {
	dir := t.TempDir()
	bin := filepath.Join(dir, "mydumper")
	script := "#!/bin/bash\n" +
		"if [ \"$1\" = \"--version\" ]; then printf 'mydumper 0.10.0, built against MySQL 8.0.36\\n'; exit 0; fi\n" +
		"out=\"\"; prev=\"\"; for a in \"$@\"; do if [ \"$prev\" = \"--outputdir\" ]; then out=\"$a\"; fi; prev=\"$a\"; done\n" +
		"mkdir -p \"$out\"\n" +
		"printf 'Started dump at: 2026-09-19 18:14:40\\nFinished dump at: 2026-09-19 18:14:40\\n' > \"$out/metadata\"\n" +
		"exit 0\n"
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}

	// The previous, good dump, which must survive the refusal.
	out := filepath.Join(dir, "out")
	if err := os.MkdirAll(out, 0o755); err != nil {
		t.Fatal(err)
	}
	const good = "Started dump at: 2026-09-18 03:00:00\nSHOW MASTER STATUS:\n\tLog: binlog.000001\n\tPos: 4\n\nFinished dump at: 2026-09-18 03:00:01\n"
	if err := os.WriteFile(filepath.Join(out, "metadata"), []byte(good), 0o644); err != nil {
		t.Fatal(err)
	}

	stubPingSource(t)
	dumpLockDir = func() string { return dir }
	t.Cleanup(func() { dumpLockDir = os.TempDir })

	dmpSourceDSN = "u:p@tcp(127.0.0.1:1)/"
	dmpOutputDir = out
	dmpMydumperPath = bin
	dmpFormat = "text"
	t.Cleanup(func() { dmpLockMode = "ftwrl"; dmpSourceDSN = ""; dmpOutputDir = "" })

	cmd := newDumpCmdForTest(t)
	if err := cmd.Flags().Set("mydumper-path", bin); err != nil {
		t.Fatal(err)
	}
	err := runDump(cmd, nil)
	if !errors.Is(err, baseline.ErrDumpNotAnchored) {
		t.Fatalf("runDump err = %v, want ErrDumpNotAnchored", err)
	}
	got, readErr := os.ReadFile(filepath.Join(out, "metadata"))
	if readErr != nil || string(got) != good {
		t.Errorf("the previous dump was not restored after the refusal (read err %v): metadata now %q", readErr, got)
	}
}

// TestRunDumpMarksARefusedFirstDump (#1744): with no previous dump to restore,
// the refused one stays in --output-dir. It is marked, so `bintrail baseline`
// refuses it instead of publishing a baseline with no position.
func TestRunDumpMarksARefusedFirstDump(t *testing.T) {
	dir := t.TempDir()
	bin := filepath.Join(dir, "mydumper")
	script := "#!/bin/bash\n" +
		"if [ \"$1\" = \"--version\" ]; then printf 'mydumper 0.10.0, built against MySQL 8.0.36\\n'; exit 0; fi\n" +
		"out=\"\"; prev=\"\"; for a in \"$@\"; do if [ \"$prev\" = \"--outputdir\" ]; then out=\"$a\"; fi; prev=\"$a\"; done\n" +
		"mkdir -p \"$out\"\n" +
		"printf 'Started dump at: 2026-09-19 18:14:40\\nFinished dump at: 2026-09-19 18:14:40\\n' > \"$out/metadata\"\n" +
		"printf 'CREATE TABLE `t` (\\n  `id` int NOT NULL,\\n  PRIMARY KEY (`id`)\\n) ENGINE=InnoDB;\\n' > \"$out/appdb.t-schema.sql\"\n" +
		"printf 'INSERT INTO `t` VALUES(1);\\n' > \"$out/appdb.t.00000.sql\"\n" +
		"exit 0\n"
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(dir, "out") // nothing here yet: a first dump

	stubPingSource(t)
	dumpLockDir = func() string { return dir }
	t.Cleanup(func() { dumpLockDir = os.TempDir })
	dmpSourceDSN = "u:p@tcp(127.0.0.1:1)/"
	dmpOutputDir = out
	dmpMydumperPath = bin
	dmpFormat = "text"
	t.Cleanup(func() { dmpLockMode = "ftwrl"; dmpSourceDSN = ""; dmpOutputDir = "" })

	cmd := newDumpCmdForTest(t)
	if err := cmd.Flags().Set("mydumper-path", bin); err != nil {
		t.Fatal(err)
	}
	if err := runDump(cmd, nil); !errors.Is(err, baseline.ErrDumpNotAnchored) {
		t.Fatalf("runDump err = %v, want ErrDumpNotAnchored", err)
	}
	if _, refused := baseline.ReadRefusedDumpMarker(out); !refused {
		t.Fatal("the refused first dump was left unmarked, so bintrail baseline would convert it")
	}
	if _, err := baseline.Run(context.Background(), baseline.Config{InputDir: out, OutputDir: t.TempDir(), Compression: "none"}); err == nil {
		t.Error("bintrail baseline converted a dump bintrail dump refused")
	}
}

// TestRunDumpOldBuildThatTakesTheBackupLockStillChecksPrivileges is the other
// half of the skip above. The exemption is about the BACKUP LOCK, not about
// being old: 0.16.3 is below the --sync-thread-lock-mode floor and still
// issues LOCK INSTANCE FOR BACKUP (measured 2026-09-19 against MySQL 8.0 with
// the general log on), so the #800 check — which exists because granting
// BACKUP_ADMIN without RELOAD SEGFAULTS mydumper — must run for it.
func TestRunDumpOldBuildThatTakesTheBackupLockStillChecksPrivileges(t *testing.T) {
	dir := t.TempDir()
	bin, _ := fakeMydumperVersion(t, dir, "mydumper v0.16.3-6, built against MySQL 8.4.1 with SSL support")

	stubPingSource(t)
	dumpLockDir = func() string { return dir }
	t.Cleanup(func() { dumpLockDir = os.TempDir })

	called := false
	checkMydumperPrivileges = func(_ context.Context, _ string, _ baseline.LockMode, _ mydumperlock.Remedy, _ []string) error {
		called = true
		return errStopAfterPreflight
	}
	t.Cleanup(func() { checkMydumperPrivileges = mydumperlock.CheckPrivileges })

	dmpSourceDSN = "u:p@tcp(127.0.0.1:1)/"
	dmpOutputDir = filepath.Join(dir, "out")
	dmpMydumperPath = bin
	dmpFormat = "text"
	t.Cleanup(func() { dmpLockMode = "ftwrl"; dmpSourceDSN = ""; dmpOutputDir = "" })

	cmd := newDumpCmdForTest(t)
	if err := cmd.Flags().Set("mydumper-path", bin); err != nil {
		t.Fatal(err)
	}
	err := runDump(cmd, nil)
	if !called {
		t.Fatal("the privilege preflight was skipped for a build that takes the backup lock: the check that " +
			"prevents a segfault is off for every 0.16/0.17 install")
	}
	if !errors.Is(err, errStopAfterPreflight) {
		t.Fatalf("err = %v, want the preflight stub's error (the dump must not start)", err)
	}
}
