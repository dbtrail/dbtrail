package consoleapp

import (
	"context"
	"github.com/dbtrail/dbtrail/internal/baseline"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

// TestBuildConsoleMydumperArgs covers the schema-filter branches and the
// invariants the dump relies on: --outputdir last (docker wrappers read the
// final arg as the mount path) and — the #811 guard — that the password never
// appears on argv (runMydumper delivers it via MYSQL_PWD in the child env).
func TestBuildConsoleMydumperArgs(t *testing.T) {
	t.Run("shared flags present in no-lock mode", func(t *testing.T) {
		// These let a least-privilege replication user (no BACKUP_ADMIN/RELOAD)
		// dump consistently — verified against a real Percona 8.0 source. Their
		// absence is the bug that produced a schema-only dump.
		args := buildConsoleMydumperArgs("h", 3306, "u", []string{"x"}, "/out", baseline.LockModeNoLock, true)
		if valueAfter(args, "--sync-thread-lock-mode") != "NO_LOCK" {
			t.Errorf("missing --sync-thread-lock-mode NO_LOCK: %v", args)
		}
		if !has(args, "--trx-tables") {
			t.Errorf("missing --trx-tables: %v", args)
		}
	})

	// #800/#1377: the point-consistent DEFAULT uses mydumper's FTWRL
	// sync mode, which barriers every worker's snapshot open at the same instant
	// — a single point-in-time snapshot across every transactional table, at the
	// cost of requiring RELOAD/FLUSH_TABLES on every flavor plus BACKUP_ADMIN on
	// MySQL/Percona 8.0+ (see checkPointConsistentPrivilegesDB and
	// sourceRequiresBackupAdmin for the flavor-conditional gate). --trx-tables
	// stays present: it shortens the FTWRL hold for transactional tables and is
	// documented as compatible with any --sync-thread-lock-mode value.
	t.Run("point-consistent mode uses FTWRL", func(t *testing.T) {
		args := buildConsoleMydumperArgs("h", 3306, "u", []string{"x"}, "/out", baseline.LockModeFTWRL, true)
		if valueAfter(args, "--sync-thread-lock-mode") != "FTWRL" {
			t.Errorf("missing --sync-thread-lock-mode FTWRL: %v", args)
		}
		if !has(args, "--trx-tables") {
			t.Errorf("missing --trx-tables: %v", args)
		}
		if has(args, "NO_LOCK") {
			t.Errorf("point-consistent mode must not pass NO_LOCK: %v", args)
		}
	})

	t.Run("no schema filter excludes system schemas", func(t *testing.T) {
		args := buildConsoleMydumperArgs("h", 3306, "u", nil, "/out", baseline.LockModeNoLock, true)
		if has(args, "--database") {
			t.Errorf("no schema filter must not use --database: %v", args)
		}
		// A least-privilege user can't read the sys views, so an unfiltered dump
		// dies; the no-filter case must exclude the system schemas instead.
		if v := valueAfter(args, "--regex"); v != systemSchemaExcludeRegex {
			t.Errorf("--regex = %q, want the system-schema exclusion: %v", v, args)
		}
		assertOutputdirLast(t, args, "/out")
	})

	t.Run("single schema uses --database", func(t *testing.T) {
		args := buildConsoleMydumperArgs("h", 3306, "u", []string{"wordpress"}, "/out", baseline.LockModeNoLock, true)
		if v := valueAfter(args, "--database"); v != "wordpress" {
			t.Errorf("--database = %q, want wordpress: %v", v, args)
		}
		if has(args, "--regex") {
			t.Errorf("single schema must not use --regex: %v", args)
		}
	})

	t.Run("multiple schemas use anchored --regex", func(t *testing.T) {
		args := buildConsoleMydumperArgs("h", 3306, "u", []string{"a", "b"}, "/out", baseline.LockModeNoLock, true)
		if v := valueAfter(args, "--regex"); v != "^(a|b)\\." {
			t.Errorf("--regex = %q, want ^(a|b)\\. : %v", v, args)
		}
		if has(args, "--database") {
			t.Errorf("multiple schemas must not use --database: %v", args)
		}
	})

	t.Run("password never appears on argv (#811)", func(t *testing.T) {
		for _, schemas := range [][]string{nil, {"wordpress"}, {"a", "b"}} {
			for _, lockMode := range baseline.LockModeValues {
				args := buildConsoleMydumperArgs("h", 3306, "u", schemas, "/out", lockMode, true)
				if has(args, "--password") {
					t.Errorf("schemas=%v lockMode=%v: --password must never appear on argv: %v", schemas, lockMode, args)
				}
			}
		}
	})
}

// TestDumpableTableCountQuery covers the pure query-building logic behind the
// NO_LOCK cross-table skew warning (#800): it must mirror
// buildConsoleMydumperArgs' own schema-selection branches so the advisory count
// approximates what mydumper will actually dump.
func TestDumpableTableCountQuery(t *testing.T) {
	t.Run("no schema filter excludes system schemas", func(t *testing.T) {
		query, args := dumpableTableCountQuery(nil)
		if args != nil {
			t.Errorf("args = %v, want nil", args)
		}
		if !strings.Contains(query, "NOT IN ('mysql','sys','performance_schema','information_schema')") {
			t.Errorf("query missing system-schema exclusion: %q", query)
		}
	})

	t.Run("single schema", func(t *testing.T) {
		query, args := dumpableTableCountQuery([]string{"wordpress"})
		if !slices.Equal(args, []any{"wordpress"}) {
			t.Errorf("args = %v, want [wordpress]", args)
		}
		if !strings.Contains(query, "TABLE_SCHEMA IN (?)") {
			t.Errorf("query missing single placeholder: %q", query)
		}
	})

	t.Run("multiple schemas", func(t *testing.T) {
		query, args := dumpableTableCountQuery([]string{"a", "b"})
		if !slices.Equal(args, []any{"a", "b"}) {
			t.Errorf("args = %v, want [a b]", args)
		}
		if !strings.Contains(query, "TABLE_SCHEMA IN (?,?)") {
			t.Errorf("query missing two placeholders: %q", query)
		}
	})
}

// TestCountDumpableTables verifies countDumpableTables runs the query built by
// dumpableTableCountQuery and scans the result, using sqlmock so no live MySQL is
// needed.
func TestCountDumpableTables(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM information_schema.TABLES").
		WithArgs("mydb").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(3))

	count, err := countDumpableTables(context.Background(), db, []string{"mydb"})
	if err != nil {
		t.Fatalf("countDumpableTables: %v", err)
	}
	if count != 3 {
		t.Errorf("count = %d, want 3", count)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet expectations: %v", err)
	}
}

// TestRunMydumper_deliversPasswordViaEnvNotArgv is the end-to-end regression
// guard for #811: the console's in-process dump passes the source password to
// mydumper as MYSQL_PWD, never on argv (world-readable via ps aux / cmdline).
func TestRunMydumper_deliversPasswordViaEnvNotArgv(t *testing.T) {
	dir := t.TempDir()
	record := filepath.Join(dir, "record.txt")
	t.Setenv("BINTRAIL_TEST_RECORD", record)

	// Fake `mydumper` resolved via PATH. Uses only bash builtins (printf,
	// redirection) so it needs nothing else on PATH.
	fakeBin := filepath.Join(dir, "mydumper")
	// It answers --version like the build the console image bundles: since
	// #1688 runMydumper reads the version before it dumps.
	script := `#!/bin/bash
if [ "$1" = "--version" ]; then printf 'mydumper v1.0.3-1, built against MySQL 8.4.9 with SSL support\n'; exit 0; fi
printf 'ARGS: %s\n' "$*" > "$BINTRAIL_TEST_RECORD"
printf 'MYSQL_PWD=%s\n' "$MYSQL_PWD" >> "$BINTRAIL_TEST_RECORD"
exit 0
`
	if err := os.WriteFile(fakeBin, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake mydumper: %v", err)
	}
	t.Setenv("PATH", dir)

	const pw = "consolesecretpw"
	// pointConsistent=false: true would hit checkPointConsistentPrivileges' HARD
	// gate, which needs a real, successful DB connection+query and would fail
	// against this fake setup (nothing listens on 127.0.0.1:3306), aborting
	// before the fake mydumper ever runs. no-lock only triggers the best-effort
	// skew warning, whose connection failure is swallowed (Debug-logged).
	err := runMydumper(context.Background(), "root:"+pw+"@tcp(127.0.0.1:3306)/", nil, filepath.Join(dir, "out"), baseline.LockModeNoLock)
	if err != nil {
		t.Fatalf("runMydumper: %v", err)
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

// TestRunMydumper_pointConsistentPreflightBlocksExecution is the highest-value
// missing test flagged by the #800 review: the thing checkPointConsistentPrivileges
// guards against is a SEGFAULT, so it must be structurally impossible for
// mydumper to run when the preflight fails — not just that runMydumper returns
// an error, but that the mydumper subprocess is never even started. Reuses the
// fake-mydumper-on-PATH harness from TestRunMydumper_deliversPasswordViaEnvNotArgv.
func TestRunMydumper_pointConsistentPreflightBlocksExecution(t *testing.T) {
	dir := t.TempDir()
	record := filepath.Join(dir, "record.txt")
	t.Setenv("BINTRAIL_TEST_RECORD", record)

	// If this fake mydumper ever runs, it proves the preflight gate was
	// bypassed — the record file's mere existence is the failure signal.
	fakeBin := filepath.Join(dir, "mydumper")
	// Answers --version (modern build) without touching the record: only a
	// real launch may create it.
	script := "#!/bin/bash\nif [ \"$1\" = \"--version\" ]; then printf 'mydumper v1.0.3-1, built against MySQL 8.4.9 with SSL support\\n'; exit 0; fi\nprintf 'RAN\\n' > \"$BINTRAIL_TEST_RECORD\"\nexit 0\n"
	if err := os.WriteFile(fakeBin, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake mydumper: %v", err)
	}
	t.Setenv("PATH", dir)

	// 127.0.0.1:1 refuses the connection immediately (closed port) so the
	// preflight's config.Connect fails fast rather than waiting out a timeout.
	err := runMydumper(context.Background(), "root:pw@tcp(127.0.0.1:1)/", nil, filepath.Join(dir, "out"), baseline.LockModeFTWRL)
	if err == nil {
		t.Fatal("expected an error from the point-consistent preflight, got nil")
	}
	if _, statErr := os.Stat(record); !os.IsNotExist(statErr) {
		t.Errorf("fake mydumper ran despite the preflight failing — the segfault-prevention gate was bypassed (record file present, stat err: %v)", statErr)
	}
}

func has(args []string, flag string) bool { return slices.Contains(args, flag) }

func valueAfter(args []string, flag string) string {
	i := slices.Index(args, flag)
	if i < 0 || i+1 >= len(args) {
		return ""
	}
	return args[i+1]
}

func assertOutputdirLast(t *testing.T, args []string, want string) {
	t.Helper()
	n := len(args)
	if n < 2 || args[n-2] != "--outputdir" || args[n-1] != want {
		t.Errorf("--outputdir %q must be the last arg pair: ...%v", want, strings.Join(args[max(0, n-3):], " "))
	}
}
