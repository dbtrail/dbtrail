package cliapp

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// ─── Cobra command wiring ─────────────────────────────────────────────────────

func TestDumpCmd_registered(t *testing.T) {
	found := false
	for _, cmd := range rootCmd.Commands() {
		if cmd.Use == "dump" {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected 'dump' command to be registered under rootCmd")
	}
}

func TestDumpCmd_requiredFlags(t *testing.T) {
	for _, name := range []string{"source-dsn", "output-dir"} {
		flag := dumpCmd.Flag(name)
		if flag == nil {
			t.Fatalf("flag --%s not registered", name)
		}
		if flag.Annotations["cobra_annotation_bash_completion_one_required_flag"] == nil {
			t.Errorf("flag --%s is not marked required", name)
		}
	}
}

func TestDumpCmd_defaults(t *testing.T) {
	cases := []struct {
		flag string
		want string
	}{
		{"threads", "4"},
		{"mydumper-path", "mydumper"},
	}
	for _, tc := range cases {
		f := dumpCmd.Flag(tc.flag)
		if f == nil {
			t.Errorf("flag --%s not registered", tc.flag)
			continue
		}
		if f.DefValue != tc.want {
			t.Errorf("flag --%s: expected default %q, got %q", tc.flag, tc.want, f.DefValue)
		}
	}
}

func TestDumpCmd_allFlagsRegistered(t *testing.T) {
	for _, name := range []string{
		"source-dsn", "output-dir", "schemas", "tables", "mydumper-path", "mydumper-image", "threads",
	} {
		if dumpCmd.Flag(name) == nil {
			t.Errorf("flag --%s not registered on dumpCmd", name)
		}
	}
}

// ─── buildMydumperArgs ────────────────────────────────────────────────────────

func TestBuildMydumperArgs_basic(t *testing.T) {
	args := buildMydumperArgs("127.0.0.1", 3306, "root", "", "/tmp/dump", 4, nil, nil, "", true, baseline.DefaultLockMode)
	assertArgsContainPair(t, args, "--host", "127.0.0.1")
	assertArgsContainPair(t, args, "--port", "3306")
	assertArgsContainPair(t, args, "--user", "root")
	assertArgsContainPair(t, args, "--outputdir", "/tmp/dump")
	assertArgsContainPair(t, args, "--threads", "4")
}

func TestBuildMydumperArgs_compressAndComplete(t *testing.T) {
	args := buildMydumperArgs("127.0.0.1", 3306, "root", "", "/tmp/dump", 4, nil, nil, "", true, baseline.DefaultLockMode)
	if !argsContain(args, "--compress-protocol") {
		t.Error("expected --compress-protocol in args")
	}
	if !argsContain(args, "--complete-insert") {
		t.Error("expected --complete-insert in args")
	}
}

func TestBuildMydumperArgs_lockAndTrx_supported(t *testing.T) {
	// Was NO_LOCK until #1377. NO_LOCK is mydumper's "if you don't need a
	// consistent backup" mode — deprecated upstream in 0.18.1 — and it can
	// hand back a snapshot stitched from several instants with nothing saying
	// so. A baseline is the seed state reconstruct merges deltas onto, so that
	// is not a default anyone should land on by not choosing.
	args := buildMydumperArgs("127.0.0.1", 3306, "root", "", "/tmp/dump", 4, nil, nil, "", true, baseline.DefaultLockMode)
	assertArgsContainPair(t, args, "--sync-thread-lock-mode", "FTWRL")
	if !argsContain(args, "--trx-tables") {
		t.Error("expected --trx-tables in args when supportsLockMode=true")
	}
}

func TestBuildMydumperArgs_lockAndTrx_unsupported(t *testing.T) {
	args := buildMydumperArgs("127.0.0.1", 3306, "root", "", "/tmp/dump", 4, nil, nil, "", false, baseline.DefaultLockMode)
	if argsContain(args, "--sync-thread-lock-mode") {
		t.Error("expected --sync-thread-lock-mode to be absent when supportsLockMode=false")
	}
	if argsContain(args, "--trx-tables") {
		t.Error("expected --trx-tables to be absent when supportsLockMode=false")
	}
	// Other flags must still be present.
	if !argsContain(args, "--compress-protocol") {
		t.Error("--compress-protocol should still be present")
	}
	if !argsContain(args, "--complete-insert") {
		t.Error("--complete-insert should still be present")
	}
}

// TestBuildMydumperArgs_neverPassword asserts the source password is NEVER put
// on argv, regardless of whether a defaults-file is used — the whole point of
// #811 (a password on argv is world-readable via ps aux / /proc/cmdline). The
// function no longer even accepts a password, so it cannot leak one.
func TestBuildMydumperArgs_neverPassword(t *testing.T) {
	for _, defaultsFile := range []string{"", "/tmp/bintrail-mydumper-xyz.cnf"} {
		args := buildMydumperArgs("127.0.0.1", 3306, "root", defaultsFile, "/tmp/dump", 4, nil, nil, "", true, baseline.DefaultLockMode)
		if argsContain(args, "--password") {
			t.Errorf("defaultsFile=%q: --password must never appear on argv: %v", defaultsFile, args)
		}
	}
}

// TestBuildMydumperArgs_defaultsFile verifies that a non-empty defaults-file
// path is passed via --defaults-file (Docker mode delivers the secret this way),
// and omitted when empty (local mode delivers it via MYSQL_PWD env).
func TestBuildMydumperArgs_defaultsFile(t *testing.T) {
	const path = "/tmp/bintrail-mydumper-abc.cnf"
	args := buildMydumperArgs("127.0.0.1", 3306, "root", path, "/tmp/dump", 4, nil, nil, "", true, baseline.DefaultLockMode)
	assertArgsContainPair(t, args, "--defaults-file", path)

	none := buildMydumperArgs("127.0.0.1", 3306, "root", "", "/tmp/dump", 4, nil, nil, "", true, baseline.DefaultLockMode)
	if argsContain(none, "--defaults-file") {
		t.Errorf("expected --defaults-file to be absent when no defaults file given: %v", none)
	}
}

func TestBuildMydumperArgs_singleSchema(t *testing.T) {
	args := buildMydumperArgs("127.0.0.1", 3306, "root", "", "/tmp/dump", 4, []string{"mydb"}, nil, "", true, baseline.DefaultLockMode)
	assertArgsContainPair(t, args, "--database", "mydb")
	if argsContain(args, "--regex") {
		t.Error("expected --regex to be absent for single schema")
	}
}

func TestBuildMydumperArgs_multipleSchemas(t *testing.T) {
	args := buildMydumperArgs("127.0.0.1", 3306, "root", "", "/tmp/dump", 4, []string{"db1", "db2"}, nil, "", true, baseline.DefaultLockMode)
	if argsContain(args, "--database") {
		t.Error("expected --database to be absent for multiple schemas")
	}
	idx := argsIndex(args, "--regex")
	if idx < 0 {
		t.Fatal("expected --regex in args")
	}
	if idx+1 >= len(args) {
		t.Fatal("--regex has no value")
	}
	regex := args[idx+1]
	if !strings.Contains(regex, "db1") || !strings.Contains(regex, "db2") {
		t.Errorf("regex %q does not contain both schema names", regex)
	}
}

// TestBuildMydumperArgs_passwordNeverLeaksToArgv is a regression guard for #811:
// even a password-shaped value must never appear anywhere in the argv (the
// function delivers credentials out of band, so it takes no password at all).
func TestBuildMydumperArgs_passwordNeverLeaksToArgv(t *testing.T) {
	args := buildMydumperArgs("127.0.0.1", 3306, "root", "", "/tmp/dump", 4, []string{"mydb"}, nil, "", true, baseline.DefaultLockMode)
	for _, a := range args {
		if a == "s3cr3t" || a == "--password" {
			t.Errorf("password-related token leaked to argv: %q in %v", a, args)
		}
	}
}

func TestBuildMydumperArgs_noSchemasOrTables(t *testing.T) {
	args := buildMydumperArgs("127.0.0.1", 3306, "root", "", "/tmp/dump", 4, nil, nil, "", true, baseline.DefaultLockMode)
	for _, flag := range []string{"--database", "--regex", "--tables-list"} {
		if argsContain(args, flag) {
			t.Errorf("expected %s to be absent when no schemas or tables given", flag)
		}
	}
}

func TestBuildMydumperArgs_regexAnchoredFormat(t *testing.T) {
	args := buildMydumperArgs("127.0.0.1", 3306, "root", "", "/tmp/dump", 4, []string{"db1", "db2"}, nil, "", true, baseline.DefaultLockMode)
	idx := argsIndex(args, "--regex")
	if idx < 0 || idx+1 >= len(args) {
		t.Fatal("expected --regex in args")
	}
	regex := args[idx+1]
	// Must be anchored at start and dot must be escaped.
	if !strings.HasPrefix(regex, "^(") {
		t.Errorf("regex should start with ^(, got %q", regex)
	}
	if !strings.HasSuffix(regex, `\.`) {
		t.Errorf("regex should end with \\., got %q", regex)
	}
}

func TestBuildMydumperArgs_schemaAndTables(t *testing.T) {
	args := buildMydumperArgs("127.0.0.1", 3306, "root", "", "/tmp/dump", 4,
		[]string{"mydb"}, []string{"mydb.orders", "mydb.items"}, "", true, baseline.DefaultLockMode)
	assertArgsContainPair(t, args, "--database", "mydb")
	idx := argsIndex(args, "--tables-list")
	if idx < 0 || idx+1 >= len(args) {
		t.Fatal("expected --tables-list in args")
	}
	if !strings.Contains(args[idx+1], "mydb.orders") {
		t.Errorf("--tables-list missing mydb.orders: %q", args[idx+1])
	}
}

func TestBuildMydumperArgs_tables(t *testing.T) {
	args := buildMydumperArgs("127.0.0.1", 3306, "root", "", "/tmp/dump", 4, nil, []string{"mydb.orders", "mydb.items"}, "", true, baseline.DefaultLockMode)
	idx := argsIndex(args, "--tables-list")
	if idx < 0 {
		t.Fatal("expected --tables-list in args")
	}
	if idx+1 >= len(args) {
		t.Fatal("--tables-list has no value")
	}
	val := args[idx+1]
	if !strings.Contains(val, "mydb.orders") || !strings.Contains(val, "mydb.items") {
		t.Errorf("--tables-list value %q missing expected table names", val)
	}
}

func TestBuildMydumperArgs_outputDirIsLast(t *testing.T) {
	// Docker wrapper scripts use ${@: -1} for the volume mount, so
	// --outputdir must always be the last flag pair.
	cases := []struct {
		name    string
		schemas []string
		tables  []string
		encrypt string
	}{
		{"no filters", nil, nil, ""},
		{"single schema", []string{"demo"}, nil, ""},
		{"multiple schemas", []string{"db1", "db2"}, nil, ""},
		{"tables", nil, []string{"db.t1", "db.t2"}, ""},
		{"schema and tables", []string{"mydb"}, []string{"mydb.t1"}, ""},
		{"encryption", nil, nil, "/path/to/key"},
		{"all filters plus encryption", []string{"demo"}, []string{"demo.t1"}, "/path/to/key"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			args := buildMydumperArgs("127.0.0.1", 3306, "root", "", "/data/backup", 4, tc.schemas, tc.tables, tc.encrypt, true, baseline.DefaultLockMode)
			n := len(args)
			if n < 2 {
				t.Fatalf("args too short: %v", args)
			}
			if args[n-2] != "--outputdir" || args[n-1] != "/data/backup" {
				t.Errorf("expected last two args to be [--outputdir /data/backup], got %v", args[n-2:])
			}
		})
	}
}

// ─── Lock mechanism ───────────────────────────────────────────────────────────

func TestAcquireReleaseDumpLock(t *testing.T) {
	dir := t.TempDir()
	old := dumpLockDir
	dumpLockDir = func() string { return dir }
	t.Cleanup(func() { dumpLockDir = old })

	f, err := acquireDumpLock()
	if err != nil {
		t.Fatalf("acquireDumpLock: unexpected error: %v", err)
	}

	// Lock file should exist and contain our PID.
	lockPath := filepath.Join(dir, dumpLockFilename)
	data, readErr := os.ReadFile(lockPath)
	if readErr != nil {
		t.Fatalf("failed to read lock file: %v", readErr)
	}
	pid, parseErr := strconv.Atoi(strings.TrimSpace(string(data)))
	if parseErr != nil {
		t.Fatalf("invalid PID in lock file %q: %v", string(data), parseErr)
	}
	if pid != os.Getpid() {
		t.Errorf("lock PID: expected %d, got %d", os.Getpid(), pid)
	}

	// Release and verify the file is gone.
	releaseDumpLock(f)
	if _, statErr := os.Stat(lockPath); !os.IsNotExist(statErr) {
		t.Error("lock file should not exist after release")
	}
}

func TestAcquireDumpLock_alreadyHeld(t *testing.T) {
	dir := t.TempDir()
	old := dumpLockDir
	dumpLockDir = func() string { return dir }
	t.Cleanup(func() { dumpLockDir = old })

	f, err := acquireDumpLock()
	if err != nil {
		t.Fatalf("first acquireDumpLock: unexpected error: %v", err)
	}
	defer releaseDumpLock(f)

	_, err = acquireDumpLock()
	if err == nil {
		t.Error("expected error on second acquireDumpLock, got nil")
	}
}

func TestAcquireDumpLock_staleLock(t *testing.T) {
	dir := t.TempDir()
	old := dumpLockDir
	dumpLockDir = func() string { return dir }
	t.Cleanup(func() { dumpLockDir = old })

	// Write a lockfile with a PID that almost certainly does not exist.
	lockPath := filepath.Join(dir, dumpLockFilename)
	if err := os.WriteFile(lockPath, []byte("999999999"), 0o600); err != nil {
		t.Fatalf("failed to write stale lock file: %v", err)
	}

	f, err := acquireDumpLock()
	if err != nil {
		t.Fatalf("acquireDumpLock with stale lock: unexpected error: %v", err)
	}
	releaseDumpLock(f)
}

// ─── extractSchemasFromTables ─────────────────────────────────────────────────

// TestBuildMydumperArgs_threeSchemas verifies that 3+ schemas all appear in the
// --regex value, not just the first two.
func TestBuildMydumperArgs_threeSchemas(t *testing.T) {
	args := buildMydumperArgs("127.0.0.1", 3306, "root", "", "/tmp/dump", 4, []string{"db1", "db2", "db3"}, nil, "", true, baseline.DefaultLockMode)
	idx := argsIndex(args, "--regex")
	if idx < 0 || idx+1 >= len(args) {
		t.Fatal("expected --regex in args for 3 schemas")
	}
	regex := args[idx+1]
	for _, s := range []string{"db1", "db2", "db3"} {
		if !strings.Contains(regex, s) {
			t.Errorf("regex %q missing schema %q", regex, s)
		}
	}
}

// TestAcquireDumpLock_invalidPIDContent verifies that a lockfile containing
// non-numeric text (parseErr != nil) is treated as stale — the signal check
// is skipped and the file is removed so a fresh lock can be acquired.
func TestAcquireDumpLock_invalidPIDContent(t *testing.T) {
	dir := t.TempDir()
	old := dumpLockDir
	dumpLockDir = func() string { return dir }
	t.Cleanup(func() { dumpLockDir = old })

	lockPath := filepath.Join(dir, dumpLockFilename)
	if err := os.WriteFile(lockPath, []byte("not-a-pid"), 0o600); err != nil {
		t.Fatalf("failed to write stale lock file: %v", err)
	}

	f, err := acquireDumpLock()
	if err != nil {
		t.Fatalf("acquireDumpLock with invalid PID content: unexpected error: %v", err)
	}
	releaseDumpLock(f)
}

func TestExtractSchemasFromTables_basic(t *testing.T) {
	schemas := extractSchemasFromTables([]string{"mydb.t1", "mydb.t2", "other.t3"})
	if len(schemas) != 2 {
		t.Fatalf("expected 2 schemas, got %d: %v", len(schemas), schemas)
	}
	if schemas[0] != "mydb" {
		t.Errorf("schemas[0]: expected mydb, got %q", schemas[0])
	}
	if schemas[1] != "other" {
		t.Errorf("schemas[1]: expected other, got %q", schemas[1])
	}
}

func TestExtractSchemasFromTables_noDot(t *testing.T) {
	schemas := extractSchemasFromTables([]string{"nodot", "mydb.t1"})
	if len(schemas) != 1 || schemas[0] != "mydb" {
		t.Errorf("expected [mydb], got %v", schemas)
	}
}

func TestExtractSchemasFromTables_allNoDot(t *testing.T) {
	// All entries lack a dot — none contribute a schema, so result must be nil.
	if schemas := extractSchemasFromTables([]string{"nodot1", "nodot2", "nodot3"}); schemas != nil {
		t.Errorf("expected nil when all entries have no dot, got %v", schemas)
	}
}

// TestExtractSchemasFromTables_deduplicationOrder verifies that insertion order
// is preserved and a schema that appears in multiple entries is counted once.
func TestExtractSchemasFromTables_deduplicationOrder(t *testing.T) {
	got := extractSchemasFromTables([]string{"other.t1", "mydb.t1", "other.t2"})
	want := []string{"other", "mydb"}
	if len(got) != len(want) {
		t.Fatalf("expected %v, got %v", want, got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("[%d] expected %q, got %q", i, want[i], got[i])
		}
	}
}

func TestExtractSchemasFromTables_empty(t *testing.T) {
	if schemas := extractSchemasFromTables(nil); schemas != nil {
		t.Errorf("expected nil for nil input, got %v", schemas)
	}
	if schemas := extractSchemasFromTables([]string{}); schemas != nil {
		t.Errorf("expected nil for empty slice, got %v", schemas)
	}
}

// ─── RunE validation ──────────────────────────────────────────────────────────

func TestRunDump_mydumperNotFound(t *testing.T) {
	// Use Flags().Set to mark --mydumper-path as Changed so resolveMydumper
	// tries the explicit path branch.
	savedPath := dmpMydumperPath
	t.Cleanup(func() {
		dmpMydumperPath = savedPath
		dumpCmd.Flags().Set("mydumper-path", savedPath)
	})
	dumpCmd.Flags().Set("mydumper-path", "/nonexistent/path/to/mydumper")

	savedDSN := dmpSourceDSN
	t.Cleanup(func() { dmpSourceDSN = savedDSN })
	dmpSourceDSN = "root@tcp(127.0.0.1:3306)/"

	err := runDump(dumpCmd, nil)
	if err == nil {
		t.Fatal("expected error for missing mydumper, got nil")
	}
	if !strings.Contains(err.Error(), "mydumper not found") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestRunDump_invalidSourceDSN(t *testing.T) {
	// Create a fake mydumper binary (LookPath only needs it to exist and be executable).
	dir := t.TempDir()
	fakeBin := filepath.Join(dir, "mydumper")
	if err := os.WriteFile(fakeBin, []byte(""), 0o755); err != nil {
		t.Fatalf("failed to create fake mydumper: %v", err)
	}

	savedPath, savedDSN := dmpMydumperPath, dmpSourceDSN
	t.Cleanup(func() {
		dmpMydumperPath = savedPath
		dumpCmd.Flags().Set("mydumper-path", savedPath)
		dmpSourceDSN = savedDSN
	})

	dumpCmd.Flags().Set("mydumper-path", fakeBin)
	dmpSourceDSN = "root@unix(/var/run/mysqld.sock)/" // unix socket → rejected by config.ParseSourceDSN

	err := runDump(dumpCmd, nil)
	if err == nil {
		t.Fatal("expected error for unix socket DSN, got nil")
	}
	if !strings.Contains(err.Error(), "unix socket") {
		t.Errorf("unexpected error: %v", err)
	}
}

// TestRunDump_capturesStderrOnFailure verifies the fix for the issue where
// every mydumper failure surfaced as the opaque "mydumper failed: exit
// status 1" message, hiding the real diagnostic (auth plugin mismatch,
// missing privilege, full disk, etc.) emitted by mydumper on stderr.
func TestRunDump_capturesStderrOnFailure(t *testing.T) {
	dir := t.TempDir()
	fakeBin := filepath.Join(dir, "mydumper")
	// Fake mydumper that prints a recognizable diagnostic to stderr and
	// exits non-zero. The --version probe returns a valid version so the
	// caller doesn't bail out before invoking the dump.
	script := `#!/bin/bash
if [ "$1" = "--version" ]; then
  echo "mydumper 0.15.0 (built with foo)"
  exit 0
fi
echo "CRITICAL: simulated mydumper failure (auth plugin caching_sha2_password)" >&2
exit 1
`
	if err := os.WriteFile(fakeBin, []byte(script), 0o755); err != nil {
		t.Fatalf("failed to create fake mydumper: %v", err)
	}

	saved := struct {
		path, dsn, outDir, format string
	}{dmpMydumperPath, dmpSourceDSN, dmpOutputDir, dmpFormat}
	t.Cleanup(func() {
		dmpMydumperPath = saved.path
		dumpCmd.Flags().Set("mydumper-path", saved.path)
		dmpSourceDSN = saved.dsn
		dmpOutputDir = saved.outDir
		dmpFormat = saved.format
	})

	dumpCmd.Flags().Set("mydumper-path", fakeBin)
	dmpSourceDSN = "root:rootpw@tcp(127.0.0.1:3306)/"
	dmpOutputDir = filepath.Join(dir, "out")
	dmpFormat = "text"
	stubPingSource(t)

	// runDump reaches acquireDumpLock, which without this override uses the
	// REAL os.TempDir() lockfile — machine-global, shared with every other
	// bintrail test process. A second suite running concurrently on the same
	// machine turns this test's assertions into "another dump is already
	// running" failures (#1164). Same pattern as
	// TestRunDump_localDeliversPasswordViaEnvNotArgv.
	savedLock := dumpLockDir
	dumpLockDir = func() string { return dir }
	t.Cleanup(func() { dumpLockDir = savedLock })

	dumpCmd.SetContext(context.Background())
	err := runDump(dumpCmd, nil)
	if err == nil {
		t.Fatal("expected error from failing mydumper, got nil")
	}
	if !strings.Contains(err.Error(), "mydumper failed") {
		t.Errorf("expected error to mention 'mydumper failed', got: %v", err)
	}
	// The whole point of the fix: stderr from mydumper must be in the
	// returned error so the operator can debug without re-running by hand.
	if !strings.Contains(err.Error(), "CRITICAL") || !strings.Contains(err.Error(), "caching_sha2_password") {
		t.Errorf("expected error to include mydumper stderr ('CRITICAL ... caching_sha2_password'), got: %v", err)
	}
}

// ─── resolveMydumper ──────────────────────────────────────────────────────────

func TestResolveMydumper_explicitPathTakesPrecedence(t *testing.T) {
	dir := t.TempDir()
	fakeBin := filepath.Join(dir, "mydumper")
	if err := os.WriteFile(fakeBin, []byte(""), 0o755); err != nil {
		t.Fatalf("failed to create fake mydumper: %v", err)
	}

	saved := dmpMydumperPath
	t.Cleanup(func() {
		dmpMydumperPath = saved
		dumpCmd.Flags().Set("mydumper-path", saved)
	})
	dumpCmd.Flags().Set("mydumper-path", fakeBin)

	res, err := resolveMydumper(dumpCmd)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.mode != dumpModeLocal {
		t.Errorf("expected dumpModeLocal, got %d", res.mode)
	}
	if res.path != fakeBin {
		t.Errorf("expected path %q, got %q", fakeBin, res.path)
	}
}

func TestResolveMydumper_explicitPathNotFound(t *testing.T) {
	saved := dmpMydumperPath
	t.Cleanup(func() {
		dmpMydumperPath = saved
		dumpCmd.Flags().Set("mydumper-path", saved)
	})
	dumpCmd.Flags().Set("mydumper-path", "/nonexistent/mydumper")

	_, err := resolveMydumper(dumpCmd)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "mydumper not found") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestResolveMydumper_nothingAvailable(t *testing.T) {
	// Ensure --mydumper-path is not Changed by resetting the flag.
	saved := dmpMydumperPath
	t.Cleanup(func() { dmpMydumperPath = saved })

	// Create a fresh command with the same flags to avoid Changed state.
	cmd := &cobra.Command{Use: "test"}
	cmd.Flags().String("mydumper-path", "mydumper", "")

	// Override PATH to exclude both mydumper and docker.
	t.Setenv("PATH", t.TempDir())

	_, err := resolveMydumper(cmd)
	if err == nil {
		t.Fatal("expected error when neither mydumper nor docker is available")
	}
	if !strings.Contains(err.Error(), "mydumper not found on $PATH") {
		t.Errorf("unexpected error: %v", err)
	}
	if !strings.Contains(err.Error(), "Docker is not available") {
		t.Errorf("expected Docker mentioned in error: %v", err)
	}
}

func TestResolveMydumper_dockerFallback(t *testing.T) {
	// Create a fake docker binary but no mydumper.
	dir := t.TempDir()
	fakeDocker := filepath.Join(dir, "docker")
	if err := os.WriteFile(fakeDocker, []byte(""), 0o755); err != nil {
		t.Fatalf("failed to create fake docker: %v", err)
	}

	// Override PATH to contain only the fake docker.
	t.Setenv("PATH", dir)

	saved := dmpMydumperImage
	t.Cleanup(func() { dmpMydumperImage = saved })
	dmpMydumperImage = "mydumper/mydumper:v0.16"

	// Use a fresh command so --mydumper-path is not Changed.
	cmd := &cobra.Command{Use: "test"}
	cmd.Flags().String("mydumper-path", "mydumper", "")

	res, err := resolveMydumper(cmd)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.mode != dumpModeDocker {
		t.Errorf("expected dumpModeDocker, got %d", res.mode)
	}
	if res.path != fakeDocker {
		t.Errorf("expected docker path %q, got %q", fakeDocker, res.path)
	}
	if res.image != "mydumper/mydumper:v0.16" {
		t.Errorf("expected image %q, got %q", "mydumper/mydumper:v0.16", res.image)
	}
}

// ─── buildDockerArgs ──────────────────────────────────────────────────────────

func TestBuildDockerArgs_basic(t *testing.T) {
	mydumperArgs := []string{"--host", "db.example.com", "--port", "3306", "--user", "root", "--outputdir", "/tmp/dump"}
	args := buildDockerArgs("mydumper/mydumper:latest", "/tmp/dump", "db.example.com", mydumperArgs, "", "")

	// Should start with docker run --rm
	if len(args) < 3 || args[0] != "run" || args[1] != "--rm" {
		t.Fatalf("expected args to start with [run --rm], got %v", args[:min(3, len(args))])
	}

	// Volume mount
	assertArgsContainPair(t, args, "-v", "/tmp/dump:/tmp/dump")

	// Should NOT have --network host for non-localhost
	if argsContain(args, "--network") {
		t.Error("expected no --network flag for non-localhost host")
	}

	// Image and mydumper command
	imgIdx := argsIndex(args, "mydumper/mydumper:latest")
	if imgIdx < 0 {
		t.Fatal("expected image name in args")
	}
	if imgIdx+1 >= len(args) || args[imgIdx+1] != "mydumper" {
		t.Error("expected 'mydumper' command after image name")
	}

	// mydumper args follow
	if !argsContain(args, "--host") || !argsContain(args, "db.example.com") {
		t.Error("expected mydumper args to be present after image + command")
	}
}

func TestBuildDockerArgs_localhostNetworkHost(t *testing.T) {
	for _, host := range []string{"localhost", "127.0.0.1", "::1"} {
		args := buildDockerArgs("mydumper/mydumper:latest", "/tmp/dump", host, nil, "", "")

		// On Linux, --network host should be added; on macOS it should not.
		hasNetwork := argsContain(args, "--network")
		if hasNetwork {
			idx := argsIndex(args, "--network")
			if idx+1 >= len(args) || args[idx+1] != "host" {
				t.Errorf("host=%s: --network should be followed by 'host'", host)
			}
		}
		// We can't assert the exact behavior since it depends on runtime.GOOS,
		// but we can verify it doesn't panic and the structure is valid.
	}
}

func TestIsLocalhost(t *testing.T) {
	for _, tc := range []struct {
		host string
		want bool
	}{
		{"localhost", true},
		{"127.0.0.1", true},
		{"::1", true},
		{"db.example.com", false},
		{"192.168.1.1", false},
		{"", false},
	} {
		if got := isLocalhost(tc.host); got != tc.want {
			t.Errorf("isLocalhost(%q) = %v, want %v", tc.host, got, tc.want)
		}
	}
}

func TestDumpCmd_mydumperImageFlag(t *testing.T) {
	f := dumpCmd.Flag("mydumper-image")
	if f == nil {
		t.Fatal("flag --mydumper-image not registered")
	}
	if f.DefValue != "mydumper/mydumper:latest" {
		t.Errorf("expected default %q, got %q", "mydumper/mydumper:latest", f.DefValue)
	}
}

// ─── encryption ───────────────────────────────────────────────────────────────

func TestDumpCmd_encryptFlagsRegistered(t *testing.T) {
	for _, name := range []string{"encrypt", "encrypt-key"} {
		if dumpCmd.Flag(name) == nil {
			t.Errorf("flag --%s not registered on dumpCmd", name)
		}
	}
}

func TestBuildMydumperArgs_encrypt(t *testing.T) {
	args := buildMydumperArgs("127.0.0.1", 3306, "root", "", "/tmp/dump", 4, nil, nil, "/path/to/key", true, baseline.DefaultLockMode)
	idx := argsIndex(args, "--exec-per-thread")
	if idx < 0 {
		t.Fatal("expected --exec-per-thread in args when encryption enabled")
	}
	if idx+1 >= len(args) {
		t.Fatal("--exec-per-thread has no value")
	}
	val := args[idx+1]
	if !strings.Contains(val, "openssl enc -aes-256-cbc -pbkdf2") {
		t.Errorf("expected openssl command in --exec-per-thread value, got %q", val)
	}
	if !strings.Contains(val, "file:") {
		t.Errorf("expected file: passphrase source in --exec-per-thread value, got %q", val)
	}
	assertArgsContainPair(t, args, "--exec-per-thread-extension", ".enc")
}

func TestBuildMydumperArgs_noEncrypt(t *testing.T) {
	args := buildMydumperArgs("127.0.0.1", 3306, "root", "", "/tmp/dump", 4, nil, nil, "", true, baseline.DefaultLockMode)
	if argsContain(args, "--exec-per-thread") {
		t.Error("expected --exec-per-thread to be absent when encryption disabled")
	}
	if argsContain(args, "--exec-per-thread-extension") {
		t.Error("expected --exec-per-thread-extension to be absent when encryption disabled")
	}
}

func TestBuildDockerArgs_encryptKeyMount(t *testing.T) {
	args := buildDockerArgs("mydumper/mydumper:latest", "/tmp/dump", "db.example.com", nil, "/path/to/key", "")
	found := false
	for i, a := range args {
		if a == "-v" && i+1 < len(args) && strings.Contains(args[i+1], "/key") && strings.HasSuffix(args[i+1], ":ro") {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected key file bind mount with :ro in docker args")
	}
}

func TestBuildDockerArgs_userFlag(t *testing.T) {
	args := buildDockerArgs("mydumper/mydumper:latest", "/tmp/dump", "db.example.com", nil, "", "")
	want := fmt.Sprintf("%d:%d", os.Getuid(), os.Getgid())
	assertArgsContainPair(t, args, "--user", want)
}

func TestBuildDockerArgs_noEncryptKeyMount(t *testing.T) {
	args := buildDockerArgs("mydumper/mydumper:latest", "/tmp/dump", "db.example.com", nil, "", "")
	// Should only have one -v (for output dir)
	count := 0
	for _, a := range args {
		if a == "-v" {
			count++
		}
	}
	if count != 1 {
		t.Errorf("expected 1 -v flag without encryption, got %d", count)
	}
}

func TestResolveEncryptKey_missingFile(t *testing.T) {
	_, err := resolveEncryptKey("/nonexistent/path/to/key")
	if err == nil {
		t.Fatal("expected error for missing key file")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Errorf("expected 'not found' in error, got: %v", err)
	}
}

func TestResolveEncryptKey_defaultPath(t *testing.T) {
	// When key path is empty, resolveEncryptKey uses defaultKeyPath().
	// This will fail because the file doesn't exist, but we can verify
	// the error message references the default path.
	_, err := resolveEncryptKey("")
	if err == nil {
		t.Fatal("expected error for missing default key file")
	}
	if !strings.Contains(err.Error(), "generate-key") {
		t.Errorf("expected 'generate-key' hint in error, got: %v", err)
	}
}

// ─── password out-of-band delivery (#811) ────────────────────────────────────

func TestBuildDockerArgs_defaultsFileMount(t *testing.T) {
	const path = "/tmp/bintrail-mydumper-x.cnf"
	args := buildDockerArgs("mydumper/mydumper:latest", "/tmp/dump", "db.example.com",
		[]string{"--defaults-file", path}, "", path)
	found := false
	for i, a := range args {
		if a == "-v" && i+1 < len(args) && strings.HasPrefix(args[i+1], path+":"+path) && strings.HasSuffix(args[i+1], ":ro") {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected read-only bind mount for the defaults file, got %v", args)
	}
}

func TestBuildDockerArgs_noDefaultsFileNoExtraMount(t *testing.T) {
	// Without a defaults file, only the output-dir -v mount is present.
	args := buildDockerArgs("mydumper/mydumper:latest", "/tmp/dump", "db.example.com", nil, "", "")
	count := 0
	for _, a := range args {
		if a == "-v" {
			count++
		}
	}
	if count != 1 {
		t.Errorf("expected exactly 1 -v mount without a defaults file, got %d: %v", count, args)
	}
}

func TestEscapeMyCnfValue(t *testing.T) {
	cases := []struct{ in, want string }{
		{"simple", `"simple"`},
		{"", `""`},
		{`pa#ss;word`, `"pa#ss;word"`}, // # / ; safe inside quotes
		{`a b`, `"a b"`},               // whitespace safe inside quotes
		{`ba\ck`, `"ba\\ck"`},          // backslash escaped
		{`quo"te`, `"quo\"te"`},        // double-quote escaped
		{`x\y"z`, `"x\\y\"z"`},         // both, in order
	}
	for _, tc := range cases {
		if got := escapeMyCnfValue(tc.in); got != tc.want {
			t.Errorf("escapeMyCnfValue(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestWriteMydumperDefaultsFile(t *testing.T) {
	const pw = `s3cr3t#p"w`
	path, cleanup, err := writeMydumperDefaultsFile(pw)
	if err != nil {
		t.Fatalf("writeMydumperDefaultsFile: %v", err)
	}
	t.Cleanup(cleanup)

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat defaults file: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Errorf("defaults file mode = %o, want 0600", perm)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read defaults file: %v", err)
	}
	s := string(data)
	for _, group := range []string{"[client]", "[mydumper]"} {
		if !strings.Contains(s, group) {
			t.Errorf("defaults file missing %s group: %q", group, s)
		}
	}
	want := "password=" + escapeMyCnfValue(pw)
	if !strings.Contains(s, want) {
		t.Errorf("defaults file missing escaped password %q: %q", want, s)
	}
	// The raw (unescaped) password must not be written verbatim if it needs
	// escaping — the escaped form differs, guarding against a naive write.
	if strings.Contains(s, "password="+pw+"\n") {
		t.Errorf("defaults file wrote unescaped password: %q", s)
	}

	// cleanup removes the file.
	cleanup()
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Errorf("expected defaults file removed by cleanup, stat err = %v", err)
	}
}

// TestRunDump_localDeliversPasswordViaEnvNotArgv is the end-to-end regression
// guard for #811: in local mode the password reaches the child as MYSQL_PWD and
// never appears on argv (world-readable via ps aux / /proc/cmdline).
func TestRunDump_localDeliversPasswordViaEnvNotArgv(t *testing.T) {
	dir := t.TempDir()
	record := filepath.Join(dir, "record.txt")
	t.Setenv("BINTRAIL_TEST_RECORD", record)

	fakeBin := filepath.Join(dir, "mydumper")
	script := `#!/bin/bash
if [ "$1" = "--version" ]; then
  echo "mydumper 0.15.0"
  exit 0
fi
{ echo "ARGS: $@"; echo "MYSQL_PWD=${MYSQL_PWD}"; } > "$BINTRAIL_TEST_RECORD"
mkdir -p "${@: -1}"
exit 0
`
	if err := os.WriteFile(fakeBin, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake mydumper: %v", err)
	}

	lockDir := t.TempDir()
	savedLock := dumpLockDir
	dumpLockDir = func() string { return lockDir }
	t.Cleanup(func() { dumpLockDir = savedLock })

	saved := struct{ path, dsn, outDir, format string }{dmpMydumperPath, dmpSourceDSN, dmpOutputDir, dmpFormat}
	t.Cleanup(func() {
		dmpMydumperPath = saved.path
		dumpCmd.Flags().Set("mydumper-path", saved.path)
		dmpSourceDSN = saved.dsn
		dmpOutputDir = saved.outDir
		dmpFormat = saved.format
	})
	dumpCmd.Flags().Set("mydumper-path", fakeBin)
	const pw = "supersecretpw"
	dmpSourceDSN = "root:" + pw + "@tcp(127.0.0.1:3306)/"
	dmpOutputDir = filepath.Join(dir, "out")
	dmpFormat = "text"
	stubPingSource(t)

	dumpCmd.SetContext(context.Background())
	if err := runDump(dumpCmd, nil); err != nil {
		t.Fatalf("runDump: %v", err)
	}

	data, err := os.ReadFile(record)
	if err != nil {
		t.Fatalf("read record: %v", err)
	}
	var argsLine, pwdLine string
	for _, line := range strings.Split(string(data), "\n") {
		switch {
		case strings.HasPrefix(line, "ARGS: "):
			argsLine = line
		case strings.HasPrefix(line, "MYSQL_PWD="):
			pwdLine = line
		}
	}
	if strings.Contains(argsLine, pw) {
		t.Errorf("password leaked onto argv: %q", argsLine)
	}
	if strings.Contains(argsLine, "--password") {
		t.Errorf("--password must not appear on argv: %q", argsLine)
	}
	if pwdLine != "MYSQL_PWD="+pw {
		t.Errorf("password not delivered via MYSQL_PWD env: got %q", pwdLine)
	}
}

// ─── #809: destructive-op guard on --output-dir ───────────────────────────────

// stubPingSource replaces the source connectivity check with a no-op for the
// duration of the test, so end-to-end runDump tests using a fake mydumper do
// not require a live MySQL on 127.0.0.1:3306.
func stubPingSource(t *testing.T) {
	t.Helper()
	saved := pingSource
	pingSource = func(string) error { return nil }
	t.Cleanup(func() { pingSource = saved })
}

func TestLooksLikeDumpDir(t *testing.T) {
	cases := []struct {
		name  string
		files []string
		want  bool
	}{
		{"metadata", []string{"metadata", "db.t.00000.sql"}, true},
		{"metadata_partial", []string{"metadata.partial", "db.t-schema.sql"}, true},
		{"bintrail_marker", []string{baseline.StartedAtMarkerFile}, true},
		{"only_data_no_marker", []string{"db.t.00000.sql", "db-schema-create.sql"}, false},
		{"unrelated_tree", []string{"ibdata1", "mysql", "important.txt"}, false},
		{"empty", nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			for _, f := range tc.files {
				if err := os.WriteFile(filepath.Join(dir, f), []byte("x"), 0o644); err != nil {
					t.Fatalf("write %s: %v", f, err)
				}
			}
			entries, err := os.ReadDir(dir)
			if err != nil {
				t.Fatalf("ReadDir: %v", err)
			}
			if got := looksLikeDumpDir(entries); got != tc.want {
				t.Errorf("looksLikeDumpDir(%v) = %v, want %v", tc.files, got, tc.want)
			}
		})
	}
}

// TestPrepareDumpOutputDir_refusesNonDumpDir is the core #809 guard: a non-empty
// directory that is not a recognizable prior dump must be REFUSED with nothing
// deleted — protecting an arbitrary tree hit by a typo'd --output-dir.
func TestPrepareDumpOutputDir_refusesNonDumpDir(t *testing.T) {
	dir := t.TempDir()
	precious := filepath.Join(dir, "important.txt")
	if err := os.WriteFile(precious, []byte("do not delete"), 0o644); err != nil {
		t.Fatalf("seed: %v", err)
	}

	prep, err := prepareDumpOutputDir(dir)
	if err == nil {
		t.Fatal("expected refusal for a non-dump non-empty directory, got nil")
	}
	if prep != nil {
		t.Errorf("expected nil prep on refusal, got %+v", prep)
	}
	if !strings.Contains(err.Error(), "refusing to delete") {
		t.Errorf("error should tell the user we refuse to delete, got: %v", err)
	}
	// Nothing was deleted.
	if _, statErr := os.Stat(precious); statErr != nil {
		t.Errorf("guard deleted a non-dump directory's contents: %v", statErr)
	}
}

// TestPrepareDumpOutputDir_movesRecognizedDump verifies a recognizable prior
// dump is moved aside (not deleted), and that commit deletes the backup while
// rollback restores it.
func TestPrepareDumpOutputDir_movesRecognizedDump(t *testing.T) {
	t.Run("commit_deletes_backup", func(t *testing.T) {
		dir := t.TempDir()
		out := filepath.Join(dir, "out")
		if err := os.MkdirAll(out, 0o755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		if err := os.WriteFile(filepath.Join(out, "metadata"), []byte("old"), 0o644); err != nil {
			t.Fatalf("seed metadata: %v", err)
		}

		prep, err := prepareDumpOutputDir(out)
		if err != nil {
			t.Fatalf("prepareDumpOutputDir: %v", err)
		}
		if prep.backup == "" {
			t.Fatal("expected a backup path for a recognized prior dump")
		}
		// The prior dump was moved aside, not deleted.
		if _, statErr := os.Stat(filepath.Join(prep.backup, "metadata")); statErr != nil {
			t.Errorf("prior dump not preserved in backup: %v", statErr)
		}
		if _, statErr := os.Stat(out); !os.IsNotExist(statErr) {
			t.Errorf("expected original dir moved away, stat err = %v", statErr)
		}

		// Simulate a fresh successful dump, then commit.
		if err := os.MkdirAll(out, 0o755); err != nil {
			t.Fatalf("recreate out: %v", err)
		}
		prep.commit()
		if _, statErr := os.Stat(prep.backup); !os.IsNotExist(statErr) {
			t.Errorf("commit should have removed the backup, stat err = %v", statErr)
		}
	})

	t.Run("rollback_restores_previous_dump", func(t *testing.T) {
		dir := t.TempDir()
		out := filepath.Join(dir, "out")
		if err := os.MkdirAll(out, 0o755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		if err := os.WriteFile(filepath.Join(out, "metadata"), []byte("old-good"), 0o644); err != nil {
			t.Fatalf("seed metadata: %v", err)
		}

		prep, err := prepareDumpOutputDir(out)
		if err != nil {
			t.Fatalf("prepareDumpOutputDir: %v", err)
		}
		// Simulate a failed dump that left partial output.
		if err := os.MkdirAll(out, 0o755); err != nil {
			t.Fatalf("recreate out: %v", err)
		}
		if err := os.WriteFile(filepath.Join(out, "metadata.partial"), []byte("junk"), 0o644); err != nil {
			t.Fatalf("seed partial: %v", err)
		}
		prep.rollback()

		// The previous good dump is back, and the backup is gone.
		got, readErr := os.ReadFile(filepath.Join(out, "metadata"))
		if readErr != nil {
			t.Fatalf("previous dump not restored: %v", readErr)
		}
		if string(got) != "old-good" {
			t.Errorf("restored metadata = %q, want %q", got, "old-good")
		}
		if _, statErr := os.Stat(filepath.Join(out, "metadata.partial")); !os.IsNotExist(statErr) {
			t.Errorf("partial output should have been discarded, stat err = %v", statErr)
		}
		if _, statErr := os.Stat(prep.backup); !os.IsNotExist(statErr) {
			t.Errorf("backup should be consumed by rollback, stat err = %v", statErr)
		}
	})
}

// TestPrepareDumpOutputDir_happyPaths verifies the non-destructive fast paths:
// an absent or empty directory needs no preparation and yields no backup.
func TestPrepareDumpOutputDir_happyPaths(t *testing.T) {
	t.Run("absent", func(t *testing.T) {
		out := filepath.Join(t.TempDir(), "does-not-exist")
		prep, err := prepareDumpOutputDir(out)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if prep.backup != "" {
			t.Errorf("expected no backup for an absent dir, got %q", prep.backup)
		}
	})
	t.Run("empty", func(t *testing.T) {
		out := t.TempDir() // exists and empty
		prep, err := prepareDumpOutputDir(out)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if prep.backup != "" {
			t.Errorf("expected no backup for an empty dir, got %q", prep.backup)
		}
	})
}

// TestPrepareDumpOutputDir_preservesFixedDotOldBackup guards the #809 review
// finding: moving a recognized prior dump aside must NOT delete a pre-existing
// <dir>.old. The earlier fixed backup name did an unconditional
// os.RemoveAll(<dir>.old), which would destroy either an operator's own backup
// kept at that human-conventional suffix, or — in a failed-rollback chain — the
// only good copy of the previous dump stranded there.
func TestPrepareDumpOutputDir_preservesFixedDotOldBackup(t *testing.T) {
	dir := t.TempDir()
	out := filepath.Join(dir, "out")
	if err := os.MkdirAll(out, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(out, "metadata"), []byte("recognized dump"), 0o644); err != nil {
		t.Fatalf("seed metadata: %v", err)
	}
	// A precious tree the user independently keeps at the conventional .old suffix.
	precious := out + ".old"
	if err := os.MkdirAll(precious, 0o755); err != nil {
		t.Fatalf("mkdir precious: %v", err)
	}
	preciousFile := filepath.Join(precious, "do-not-delete.txt")
	if err := os.WriteFile(preciousFile, []byte("keep me"), 0o644); err != nil {
		t.Fatalf("seed precious: %v", err)
	}

	prep, err := prepareDumpOutputDir(out)
	if err != nil {
		t.Fatalf("prepareDumpOutputDir: %v", err)
	}
	// The fixed <dir>.old must be untouched.
	if got, readErr := os.ReadFile(preciousFile); readErr != nil {
		t.Fatalf("precious <dir>.old was deleted or altered: %v", readErr)
	} else if string(got) != "keep me" {
		t.Errorf("precious <dir>.old content changed = %q, want %q", got, "keep me")
	}
	// The backup must be a distinct unique sibling, never the fixed <dir>.old.
	if prep.backup == precious {
		t.Fatalf("backup collided with the pre-existing fixed <dir>.old: %q", prep.backup)
	}
	if prep.backup == "" {
		t.Fatal("expected a backup path for a recognized prior dump")
	}
	if _, statErr := os.Stat(filepath.Join(prep.backup, "metadata")); statErr != nil {
		t.Errorf("prior dump not preserved in unique backup: %v", statErr)
	}
}

// ─── helpers ─────────────────────────────────────────────────────────────────

// argsContain reports whether args contains the string s.
func argsContain(args []string, s string) bool {
	return argsIndex(args, s) >= 0
}

// argsIndex returns the index of s in args, or -1 if not present.
func argsIndex(args []string, s string) int {
	for i, a := range args {
		if a == s {
			return i
		}
	}
	return -1
}

// assertArgsContainPair checks that key appears in args immediately followed by val.
func assertArgsContainPair(t *testing.T, args []string, key, val string) {
	t.Helper()
	idx := argsIndex(args, key)
	if idx < 0 {
		t.Errorf("expected %q in args %v", key, args)
		return
	}
	if idx+1 >= len(args) || args[idx+1] != val {
		var got string
		if idx+1 < len(args) {
			got = args[idx+1]
		}
		t.Errorf("expected %q after %q, got %q", val, key, got)
	}
}
