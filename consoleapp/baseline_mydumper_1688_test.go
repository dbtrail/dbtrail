package consoleapp

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/mydumperlock"
)

// Version lines a real binary prints. The first is Ubuntu 24.04's package
// (0.10.1-1ubuntu3), measured with `docker run ubuntu:24.04` + apt; the second is
// the build the console image bundles.
const (
	versionDistro = "mydumper 0.10.0, built against MySQL 8.0.36"
	versionModern = "mydumper v1.0.3-1, built against MySQL 8.4.9 with SSL support"
)

// fakeConsoleMydumper puts a fake `mydumper` first on PATH. versionBody is the
// bash that answers --version (it must exit); any other invocation records its
// argv to the returned file, so a test can assert the dump ran, or never did.
func fakeConsoleMydumper(t *testing.T, versionBody string) (record string) {
	t.Helper()
	dir := t.TempDir()
	record = filepath.Join(dir, "argv.txt")
	script := "#!/bin/bash\n" +
		"if [ \"$1\" = \"--version\" ]; then\n" + versionBody + "\nfi\n" +
		"printf '%s\\n' \"$@\" > '" + record + "'\n" +
		"exit 0\n"
	if err := os.WriteFile(filepath.Join(dir, "mydumper"), []byte(script), 0o755); err != nil {
		t.Fatalf("write fake mydumper: %v", err)
	}
	t.Setenv("PATH", dir)
	return record
}

// printsVersion is a versionBody that prints line verbatim and exits 0.
func printsVersion(line string) string {
	return "printf '%s\\n' '" + strings.ReplaceAll(line, "'", `'\''`) + "'; exit 0"
}

// A mydumper whose dynamic linker cannot resolve a library: the loader writes
// to stderr and the process exits 127 before main runs (#1699).
const loaderFailure = "printf 'mydumper: error while loading shared libraries: libmysqlclient.so.21: cannot open shared object file\\n' >&2; exit 127"

// stubPreflight replaces the privilege preflight with one that records it ran
// and returns err.
func stubPreflight(t *testing.T, err error) *int {
	t.Helper()
	calls := new(int)
	checkMydumperPrivileges = func(context.Context, string, baseline.LockMode, mydumperlock.Remedy, []string) error {
		*calls++
		return err
	}
	t.Cleanup(func() { checkMydumperPrivileges = mydumperlock.CheckPrivileges })
	return calls
}

func recordedArgs(t *testing.T, record string) []string {
	t.Helper()
	data, err := os.ReadFile(record)
	if err != nil {
		t.Fatalf("mydumper was never launched: %v", err)
	}
	return strings.Split(strings.TrimRight(string(data), "\n"), "\n")
}

func assertNeverLaunched(t *testing.T, record string) {
	t.Helper()
	if _, err := os.Stat(record); !os.IsNotExist(err) {
		t.Fatalf("mydumper was launched (record present, stat err: %v); the run had to stop before it", err)
	}
}

// TestPlanMydumperByVersionAndMode is the decision table for #1688: every probe
// outcome against every lock mode.
func TestPlanMydumperByVersionAndMode(t *testing.T) {
	type want struct {
		refuse    []string // substrings the refusal must carry; nil = no refusal
		flags     bool
		preflight bool
		fallback  bool
	}
	modes := []baseline.LockMode{baseline.LockModeFTWRL, baseline.LockModeLockAll, baseline.LockModeSafeNoLock, baseline.LockModeNoLock}
	cases := []struct {
		name    string
		version string
		want    map[baseline.LockMode]want
	}{
		{
			name:    "modern build",
			version: printsVersion(versionModern),
			want: map[baseline.LockMode]want{
				baseline.LockModeFTWRL:      {flags: true, preflight: true},
				baseline.LockModeLockAll:    {flags: true, preflight: true},
				baseline.LockModeSafeNoLock: {flags: true},
				baseline.LockModeNoLock:     {flags: true},
			},
		},
		{
			name:    "distribution 0.10",
			version: printsVersion(versionDistro),
			want: map[baseline.LockMode]want{
				baseline.LockModeFTWRL:      {fallback: true},
				baseline.LockModeLockAll:    {refuse: []string{"lock-all", "0.18.1", "0.10.0", "BINTRAIL_CONSOLE_BASELINE_LOCK_MODE"}},
				baseline.LockModeSafeNoLock: {refuse: []string{"safe-no-lock", "0.18.1", "0.10.0"}},
				baseline.LockModeNoLock:     {refuse: []string{"no-lock", "0.18.1", "0.10.0"}},
			},
		},
		{
			// v-prefixed and still below the floor: read, and old. It takes
			// MySQL's backup lock all the same (measured), so the #800
			// privilege check RUNS for it — unlike the 0.10 row above, which
			// takes no lock and would be refused for a privilege it never uses.
			name:    "0.16 with the v prefix",
			version: printsVersion("mydumper v0.16.3-6, built against MySQL 8.4.1 with SSL support"),
			want: map[baseline.LockMode]want{
				baseline.LockModeFTWRL:      {preflight: true, fallback: true},
				baseline.LockModeLockAll:    {refuse: []string{"0.16.3"}},
				baseline.LockModeSafeNoLock: {refuse: []string{"0.16.3"}},
				baseline.LockModeNoLock:     {refuse: []string{"0.16.3"}},
			},
		},
		{
			name:    "unreadable version",
			version: printsVersion("mydumper built from source"),
			want: map[baseline.LockMode]want{
				baseline.LockModeFTWRL:      {preflight: true, fallback: true},
				baseline.LockModeLockAll:    {refuse: []string{"lock-all", "could not be read", "built from source"}},
				baseline.LockModeSafeNoLock: {refuse: []string{"could not be read"}},
				baseline.LockModeNoLock:     {refuse: []string{"could not be read"}},
			},
		},
		{
			name:    "binary does not run",
			version: loaderFailure,
			want: map[baseline.LockMode]want{
				baseline.LockModeFTWRL:      {refuse: []string{"did not run", "libmysqlclient.so.21", "fix or replace"}},
				baseline.LockModeLockAll:    {refuse: []string{"did not run"}},
				baseline.LockModeSafeNoLock: {refuse: []string{"did not run"}},
				baseline.LockModeNoLock:     {refuse: []string{"did not run"}},
			},
		},
	}
	for _, tc := range cases {
		for _, mode := range modes {
			t.Run(tc.name+"/"+string(mode), func(t *testing.T) {
				fakeConsoleMydumper(t, tc.version)
				w := tc.want[mode]
				plan, err := planMydumper(mode)
				if w.refuse != nil {
					if err == nil {
						t.Fatalf("want a refusal, got plan %+v", plan)
					}
					for _, sub := range w.refuse {
						if !strings.Contains(err.Error(), sub) {
							t.Errorf("refusal %q does not say %q", err, sub)
						}
					}
					return
				}
				if err != nil {
					t.Fatalf("unexpected refusal: %v", err)
				}
				if plan.sendLockFlags != w.flags || plan.preflight != w.preflight || (plan.fallback != "") != w.fallback {
					t.Errorf("plan = {flags:%v preflight:%v fallback:%q}, want {flags:%v preflight:%v fallback:%v}",
						plan.sendLockFlags, plan.preflight, plan.fallback, w.flags, w.preflight, w.fallback)
				}
				if !strings.HasSuffix(plan.path, "/mydumper") {
					t.Errorf("plan.path = %q, want the resolved binary", plan.path)
				}
			})
		}
	}
}

// TestPlanMydumperNotInstalled: no mydumper on PATH at all.
func TestPlanMydumperNotInstalled(t *testing.T) {
	t.Setenv("PATH", t.TempDir())
	_, err := planMydumper(baseline.LockModeFTWRL)
	if err == nil || !strings.Contains(err.Error(), "not installed") || !strings.Contains(err.Error(), "0.18.1") {
		t.Fatalf("err = %v, want a refusal saying mydumper is not installed and naming the version needed", err)
	}
}

// TestRunMydumperDistributionBuildDumpsWithDefaultMode is the #1688 repro with
// the console's default lock mode: the dump must RUN, without the two flags a
// 0.10 build rejects, and without the BACKUP_ADMIN preflight the CLI also skips
// for a build read as old.
func TestRunMydumperDistributionBuildDumpsWithDefaultMode(t *testing.T) {
	record := fakeConsoleMydumper(t, printsVersion(versionDistro))
	calls := stubPreflight(t, errors.New("preflight must not run for a build read as pre-0.18"))

	out := filepath.Join(t.TempDir(), "out")
	if err := runMydumper(context.Background(), "u:p@tcp(127.0.0.1:1)/", []string{"appdb"}, out, baseline.LockModeFTWRL); err != nil {
		t.Fatalf("runMydumper: %v", err)
	}
	if *calls != 0 {
		t.Errorf("privilege preflight ran %d time(s) for a 0.10 build", *calls)
	}
	args := recordedArgs(t, record)
	for _, flag := range []string{"--sync-thread-lock-mode", "--trx-tables"} {
		if slices.Contains(args, flag) {
			t.Errorf("args carry %s, which mydumper 0.10 rejects with \"Unknown option\": %v", flag, args)
		}
	}
	assertOutputdirLast(t, args, out)
}

// TestRunMydumperDistributionBuildRefusesAnotherMode: lock-all on a 0.10 build
// (the issue's own configuration, on RDS) stops before mydumper starts and before
// the preflight's network round trip, with a message naming both versions.
func TestRunMydumperDistributionBuildRefusesAnotherMode(t *testing.T) {
	record := fakeConsoleMydumper(t, printsVersion(versionDistro))
	calls := stubPreflight(t, nil)

	err := runMydumper(context.Background(), "u:p@tcp(127.0.0.1:1)/", nil, filepath.Join(t.TempDir(), "out"), baseline.LockModeLockAll)
	if err == nil || !strings.Contains(err.Error(), "0.10.0") || !strings.Contains(err.Error(), "0.18.1") {
		t.Fatalf("err = %v, want a refusal naming the installed 0.10.0 and the 0.18.1 floor", err)
	}
	if *calls != 0 {
		t.Errorf("preflight ran %d time(s) before a refusal that needs no server", *calls)
	}
	assertNeverLaunched(t, record)
}

// TestRunMydumperBrokenBinaryNeverReachesThePreflight is #1699 on the console:
// a binary that does not run is named as such, not reported as a privilege gap.
func TestRunMydumperBrokenBinaryNeverReachesThePreflight(t *testing.T) {
	record := fakeConsoleMydumper(t, loaderFailure)
	calls := stubPreflight(t, errors.New("BACKUP_ADMIN missing"))

	err := runMydumper(context.Background(), "u:p@tcp(127.0.0.1:1)/", nil, filepath.Join(t.TempDir(), "out"), baseline.LockModeFTWRL)
	if err == nil || !strings.Contains(err.Error(), "libmysqlclient.so.21") {
		t.Fatalf("err = %v, want the loader's own complaint", err)
	}
	if strings.Contains(err.Error(), "BACKUP_ADMIN") || *calls != 0 {
		t.Errorf("a binary that does not run reached the privilege preflight (%d call(s)): %v", *calls, err)
	}
	assertNeverLaunched(t, record)
}

// TestRunMydumperUnreadableVersionKeepsThePreflight: the CLI's #1686 rule on the
// console. Unknown is not old, so the segfault guard stays on.
func TestRunMydumperUnreadableVersionKeepsThePreflight(t *testing.T) {
	record := fakeConsoleMydumper(t, printsVersion("mydumper built from source"))
	stop := errors.New("stop after preflight")
	calls := stubPreflight(t, stop)

	err := runMydumper(context.Background(), "u:p@tcp(127.0.0.1:1)/", nil, filepath.Join(t.TempDir(), "out"), baseline.LockModeFTWRL)
	if !errors.Is(err, stop) || *calls != 1 {
		t.Fatalf("err = %v, preflight calls = %d; want the preflight to run once and stop the dump", err, *calls)
	}
	assertNeverLaunched(t, record)
}

// TestRunMydumperModernBuildKeepsTheFlags: nothing changes for the build the
// image bundles.
func TestRunMydumperModernBuildKeepsTheFlags(t *testing.T) {
	record := fakeConsoleMydumper(t, printsVersion(versionModern))
	stubPreflight(t, nil)

	if err := runMydumper(context.Background(), "u:p@tcp(127.0.0.1:1)/", nil, filepath.Join(t.TempDir(), "out"), baseline.LockModeSafeNoLock); err != nil {
		t.Fatalf("runMydumper: %v", err)
	}
	args := recordedArgs(t, record)
	if valueAfter(args, "--sync-thread-lock-mode") != baseline.LockModeSafeNoLock.MydumperValue() || !slices.Contains(args, "--trx-tables") {
		t.Errorf("args = %v, want --sync-thread-lock-mode %s and --trx-tables", args, baseline.LockModeSafeNoLock.MydumperValue())
	}
}

// TestMydumperBootWarning pins the startup line: said once, before the first
// slot, in the same words the run would use.
func TestMydumperBootWarning(t *testing.T) {
	fakeConsoleMydumper(t, printsVersion(versionModern))
	if msg := mydumperBootWarning(baseline.LockModeLockAll); msg != "" {
		t.Errorf("modern build warned at boot: %q", msg)
	}

	fakeConsoleMydumper(t, printsVersion(versionDistro))
	fail := mydumperBootWarning(baseline.LockModeLockAll)
	if !strings.HasPrefix(fail, "full backups of MySQL and MariaDB servers will fail") || !strings.Contains(fail, "0.10.0") {
		t.Errorf("0.10 + lock-all: %q", fail)
	}
	run := mydumperBootWarning(baseline.LockModeFTWRL)
	if !strings.HasPrefix(run, "full backups of MySQL and MariaDB servers will run, but") || !strings.Contains(run, "0.10.0") {
		t.Errorf("0.10 + ftwrl: %q", run)
	}
	for _, msg := range []string{fail, run} {
		if strings.ContainsRune(msg, '—') {
			t.Errorf("operator-facing text carries an em dash: %q", msg)
		}
		t.Log(msg)
	}
}

// TestBaselineWiringWarnsAboutAnUnusableMydumperAtBoot is the wiring half of the
// boot line: mydumperBootWarning proves nothing if the constructor the daemon
// uses never calls it. A refresh-only daemon never runs mydumper, so it must
// stay quiet about it.
func TestBaselineWiringWarnsAboutAnUnusableMydumperAtBoot(t *testing.T) {
	fakeConsoleMydumper(t, printsVersion(versionDistro))
	prevTrigger, prevMode, prevErr := upConsoleBaselineTrigger, upConsoleBaselineLockMode, upConsoleBaselineLockModeErr
	prevLog := slog.Default()
	t.Cleanup(func() {
		upConsoleBaselineTrigger, upConsoleBaselineLockMode, upConsoleBaselineLockModeErr = prevTrigger, prevMode, prevErr
		slog.SetDefault(prevLog)
	})
	upConsoleBaselineLockMode, upConsoleBaselineLockModeErr = baseline.LockModeLockAll, nil

	// A lock-mode typo already refuses every run with its own error; a boot
	// line about the default mode would describe a daemon that does not exist.
	t.Run("invalid lock mode config", func(t *testing.T) {
		var buf bytes.Buffer
		slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
		upConsoleBaselineTrigger = true
		upConsoleBaselineLockModeErr = errors.New("BINTRAIL_CONSOLE_BASELINE_LOCK_MODE: bad value")
		ctx, cancel := context.WithCancel(context.Background())
		newBaselineSupervisorFromConfig(ctx, t.TempDir(), nil)
		cancel()
		upConsoleBaselineLockModeErr = nil
		if strings.Contains(buf.String(), "full backups of MySQL and MariaDB servers") {
			t.Errorf("a mydumper boot line was printed over an invalid lock-mode setting:\n%s", buf.String())
		}
	})
	for _, trigger := range []bool{true, false} {
		var buf bytes.Buffer
		slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
		upConsoleBaselineTrigger = trigger
		ctx, cancel := context.WithCancel(context.Background())
		newBaselineSupervisorFromConfig(ctx, t.TempDir(), nil)
		cancel() // stops the reaper goroutine the constructor starts
		warned := strings.Contains(buf.String(), "full backups of MySQL and MariaDB servers will fail") &&
			strings.Contains(buf.String(), "0.10.0")
		if warned != trigger {
			t.Errorf("trigger=%v: boot warning present = %v, want %v; log:\n%s", trigger, warned, trigger, buf.String())
		}
	}
}

// TestExecuteRefusesADumpWithNoBinlogPosition is the half the pre-push review
// found (#1688): with the old-build fallback, mydumper 0.10 against MySQL 8.4
// exits 0 and writes metadata with no position (measured). Published, that
// snapshot has no anchor. The run must fail, name why, and publish nothing.
func TestExecuteRefusesADumpWithNoBinlogPosition(t *testing.T) {
	dir := t.TempDir()
	// The metadata 0.10 wrote against MySQL 8.4, verbatim: no position lines.
	script := fakeDumpScript(versionDistro, "Started dump at: 2026-09-19 18:14:40\\nFinished dump at: 2026-09-19 18:14:40\\n")
	if err := os.WriteFile(filepath.Join(dir, "mydumper"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir)
	stubPreflight(t, nil)
	stubSourceVersion(t, "8.0.36", nil)

	sup := newBaselineSupervisor(context.Background(), t.TempDir(), baseline.LockModeFTWRL)
	local := t.TempDir()
	_, err := sup.execute(console.BaselineRequest{ServerID: "s1", SourceDSN: "u:p@tcp(127.0.0.1:1)/", LocalDir: local})
	if !errors.Is(err, baseline.ErrDumpNotAnchored) {
		t.Fatalf("execute err = %v, want ErrDumpNotAnchored", err)
	}
	for _, want := range []string{"no binlog position", "binary logging", "REPLICATION CLIENT"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the run's error %q does not say %q", err, want)
		}
	}
	entries, _ := os.ReadDir(local)
	if len(entries) != 0 {
		t.Errorf("an unanchored dump was converted anyway: %d entries in the backup directory", len(entries))
	}
}

// fakeDumpScript is a fake mydumper that answers --version with versionLine and
// otherwise writes a dump that converts: metadata (printf-escaped) plus one
// real table, appdb.t, so a check that fails to refuse lets a snapshot through.
func fakeDumpScript(versionLine, metadata string) string {
	return "#!/bin/bash\n" +
		"if [ \"$1\" = \"--version\" ]; then printf '%s\\n' '" + versionLine + "'; exit 0; fi\n" +
		"out=\"${@: -1}\"\n" +
		"printf '" + metadata + "' > \"$out/metadata\"\n" +
		"printf 'CREATE TABLE `t` (\\n  `id` int NOT NULL,\\n  PRIMARY KEY (`id`)\\n) ENGINE=InnoDB;\\n' > \"$out/appdb.t-schema.sql\"\n" +
		"printf 'INSERT INTO `t` VALUES(1),(2);\\n' > \"$out/appdb.t.00000.sql\"\n" +
		"exit 0\n"
}

// stubSourceVersion replaces the SELECT VERSION() probe.
func stubSourceVersion(t *testing.T, version string, err error) *int {
	t.Helper()
	calls := new(int)
	prev := sourceServerVersion
	sourceServerVersion = func(context.Context, string) (string, error) {
		*calls++
		return version, err
	}
	t.Cleanup(func() { sourceServerVersion = prev })
	return calls
}

// installWorkingMydumper puts a mydumper on PATH that answers --version and
// exits 0. For tests whose subject is the scheduler or the job guard, not
// mydumper: since #1688 the plan (find the binary, read its version) runs
// BEFORE the privilege check, so a host with no mydumper at all is refused
// there — which on a CI runner without mydumper made those tests fail while
// passing on a laptop that happens to have one installed.
func installWorkingMydumper(t *testing.T) {
	t.Helper()
	installFake(t, "#!/bin/bash\nif [ \"$1\" = \"--version\" ]; then "+printsVersion(versionModern)+"; fi\nexit 0\n")
}

func installFake(t *testing.T, script string) (record string) {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "mydumper"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir)
	return filepath.Join(dir, "never-written")
}

// TestExecuteConvertsAHealthyDumpFromAnOldBuild is the other side of the
// refusal: 0.10 against MySQL 8.0 records its position, so the fallback's dump
// is converted and published. Without this, a check that refused everything
// would pass the refusal tests.
func TestExecuteConvertsAHealthyDumpFromAnOldBuild(t *testing.T) {
	installFake(t, fakeDumpScript(versionDistro,
		"Started dump at: 2026-09-19 18:14:40\\nSHOW MASTER STATUS:\\n\\tLog: binlog.000002\\n\\tPos: 1374\\n\\tGTID:\\n\\nFinished dump at: 2026-09-19 18:14:40\\n"))
	stubPreflight(t, nil)
	stubSourceVersion(t, "8.0.36", nil)

	sup := newBaselineSupervisor(context.Background(), t.TempDir(), baseline.LockModeFTWRL)
	local := t.TempDir()
	out, err := sup.execute(console.BaselineRequest{ServerID: "s1", SourceDSN: "u:p@tcp(127.0.0.1:1)/", LocalDir: local})
	if err != nil {
		t.Fatalf("execute refused a dump that records its position: %v", err)
	}
	defer out.cleanup()
	if _, statErr := os.Stat(filepath.Join(out.snapDir, "appdb", "t.parquet")); statErr != nil {
		t.Errorf("no converted table in %s: %v", out.snapDir, statErr)
	}
}

// TestExecuteRefusesADumpWhoseMetadataCannotBeRead: the console refuses what it
// cannot anchor, including a dump with no readable metadata, where the
// conversion would only log at Info (the CLI only warns there, on purpose).
func TestExecuteRefusesADumpWhoseMetadataCannotBeRead(t *testing.T) {
	installFake(t, fakeDumpScript(versionModern, "not mydumper metadata\\n"))
	stubPreflight(t, nil)

	sup := newBaselineSupervisor(context.Background(), t.TempDir(), baseline.LockModeSafeNoLock)
	local := t.TempDir()
	_, err := sup.execute(console.BaselineRequest{ServerID: "s1", SourceDSN: "u:p@tcp(127.0.0.1:1)/", LocalDir: local})
	if err == nil || !strings.Contains(err.Error(), "cannot read mydumper's metadata") {
		t.Fatalf("execute err = %v, want a refusal naming the unreadable metadata", err)
	}
	if entries, _ := os.ReadDir(local); len(entries) != 0 {
		t.Errorf("a dump with unreadable metadata was converted anyway: %d entries", len(entries))
	}
}

// TestRunMydumperOldBuildIsRefusedBeforeDumpingMySQL84: a build older than
// 0.16.3 against MySQL 8.4 would dump everything under FTWRL and then be
// refused for having no position, so the run stops before mydumper starts.
// MariaDB, 8.0, and a source whose version cannot be read go ahead.
func TestRunMydumperOldBuildIsRefusedBeforeDumpingMySQL84(t *testing.T) {
	for _, tc := range []struct {
		name    string
		server  string
		err     error
		refused bool
	}{
		{"mysql 8.4", "8.4.9", nil, true},
		{"mysql 8.0", "8.0.36", nil, false},
		{"mariadb", "11.4.12-MariaDB-ubu2404-log", nil, false},
		{"version unreadable", "", errors.New("connection refused"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			record := fakeConsoleMydumper(t, printsVersion(versionDistro))
			stubPreflight(t, nil)
			calls := stubSourceVersion(t, tc.server, tc.err)
			err := runMydumper(context.Background(), "u:p@tcp(127.0.0.1:1)/", nil, filepath.Join(t.TempDir(), "out"), baseline.LockModeFTWRL)
			if *calls != 1 {
				t.Errorf("source version read %d time(s), want once for a build older than 0.16.3", *calls)
			}
			if tc.refused {
				if err == nil || !strings.Contains(err.Error(), "8.4.9") || !strings.Contains(err.Error(), "0.16.3") {
					t.Fatalf("err = %v, want a refusal naming the server version and 0.16.3", err)
				}
				assertNeverLaunched(t, record)
				return
			}
			if err != nil {
				t.Fatalf("runMydumper: %v", err)
			}
			recordedArgs(t, record)
		})
	}

	t.Run("a build from 0.16.3 on is not asked", func(t *testing.T) {
		fakeConsoleMydumper(t, printsVersion(versionModern))
		stubPreflight(t, nil)
		calls := stubSourceVersion(t, "8.4.9", nil)
		if err := runMydumper(context.Background(), "u:p@tcp(127.0.0.1:1)/", nil, filepath.Join(t.TempDir(), "out"), baseline.LockModeSafeNoLock); err != nil {
			t.Fatalf("runMydumper: %v", err)
		}
		if *calls != 0 {
			t.Errorf("source version read for a build that records the position on every server")
		}
	})
}
