package sqlsandbox

import (
	"database/sql"
	"slices"
	"sort"
	"strings"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
)

// Every DuckDB setting the worker relies on exists in the pinned engine. The
// lock-down is a list of SET statements; a renamed or removed setting would
// make one of them fail, and the worker treats that as fatal, so this test is
// what turns an engine bump into an explicit edit here instead of a sandbox
// that refuses every query (or, worse, one that silently lost a belt because
// someone made the SET best-effort).
//
// Verified on DuckDB v1.4.5 (the 1.4 LTS line, see TestDuckDBEngineOnLTSLine
// in internal/duckdbutil).
func TestSandboxSettingsExistInPinnedEngine(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, s := range append(append([]string{}, sandboxSettings...), "threads", "memory_limit") {
		var n int
		if err := db.QueryRow("SELECT count(*) FROM duckdb_settings() WHERE name = ?", s).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n != 1 {
			t.Errorf("setting %q is not in duckdb_settings() of this engine", s)
		}
	}
	// And the lock-down script names each of them, in the order that matters:
	// the lock last.
	script := strings.Join(lockdownStatements([]string{"/copy"}), "\n")
	for _, s := range sandboxSettings {
		if !strings.Contains(script, "SET "+s+" ") {
			t.Errorf("lockdown does not SET %s:\n%s", s, script)
		}
	}
	last := lockdownStatements([]string{"/copy"})
	if !strings.HasPrefix(last[len(last)-1], "SET lock_configuration = true") {
		t.Errorf("lock_configuration must be the last statement, got %q", last[len(last)-1])
	}
}

// A copy directory with a quote in its name cannot break out of the literal.
func TestLockdownQuotesDirectories(t *testing.T) {
	stmts := lockdownStatements([]string{"/co'py", "/other"})
	var got string
	for _, s := range stmts {
		if strings.HasPrefix(s, "SET allowed_directories") {
			got = s
		}
	}
	if got != "SET allowed_directories = ['/co''py', '/other']" {
		t.Errorf("allowed_directories = %q", got)
	}
}

// The lock-down runs, in its order, on the pinned engine, and afterwards
// nothing in it can be changed. Order is what this pins: DuckDB v1.4.5
// refuses to modify temp_directory once external access is off, so a
// reordering that looks harmless fails every query in the worker.
func TestLockdownRunsInOrderOnPinnedEngine(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	stmts := lockdownStatements([]string{t.TempDir()})
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
	for _, s := range append(stmts, "SET threads = 64", "RESET memory_limit", "PRAGMA memory_limit = '100GB'") {
		_, err := db.Exec(s)
		if err == nil || !strings.Contains(err.Error(), "locked") {
			t.Errorf("after the lock, %q: err = %v, want the configuration-locked refusal", s, err)
		}
	}
}

// The engine's own belt, after the lock on the real engine: DuckDB itself
// refuses an INSTALL and a read outside the copy, independently of the
// statement-shape gate in front of it.
func TestLockdownEngineRefusesInstallAndOutsideReads(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, s := range lockdownStatements([]string{t.TempDir()}) {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
	cases := map[string]string{
		"INSTALL httpfs":                            "Permission Error",
		"LOAD httpfs":                               "Permission Error",
		"SELECT * FROM read_csv('/etc/passwd')":     "Permission Error",
		"SELECT * FROM read_parquet('/etc/passwd')": "Permission Error",
		"ATTACH '/tmp/x.duckdb'":                    "Permission Error",
	}
	for q, want := range cases {
		_, err := db.Exec(q)
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("after the lock, %q: err = %v, want %s from DuckDB itself", q, err, want)
		}
	}
}

// knownDuckDBTableFunctions is every duckdb_* table function the pinned
// engine (v1.4.5) exposes. The allowlist admits that prefix as a whole
// because every one of these is a catalog or introspection read; a future
// engine that adds a duckdb_* function with a side effect would be admitted
// by the prefix rule silently, so this test turns the addition into a loud
// review: check what the new function does, then extend this list.
var knownDuckDBTableFunctions = []string{
	"duckdb_approx_database_count", "duckdb_columns", "duckdb_connection_count",
	"duckdb_constraints", "duckdb_databases", "duckdb_dependencies", "duckdb_extensions",
	"duckdb_external_file_cache", "duckdb_functions", "duckdb_indexes", "duckdb_keywords",
	"duckdb_log_contexts", "duckdb_logs", "duckdb_memory", "duckdb_optimizers",
	"duckdb_prepared_statements", "duckdb_schemas", "duckdb_secret_types", "duckdb_secrets",
	"duckdb_sequences", "duckdb_settings", "duckdb_table_sample", "duckdb_tables",
	"duckdb_temporary_files", "duckdb_types", "duckdb_variables", "duckdb_views",
}

func TestDuckDBPrefixedTableFunctionsArePinned(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rows, err := db.Query(`SELECT DISTINCT function_name FROM duckdb_functions()
		WHERE function_type = 'table' AND function_name LIKE 'duckdb\_%' ESCAPE '\' ORDER BY 1`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var got []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			t.Fatal(err)
		}
		got = append(got, n)
	}
	want := append([]string{}, knownDuckDBTableFunctions...)
	sort.Strings(want)
	if !slices.Equal(got, want) {
		t.Errorf("the engine's duckdb_* table functions changed; the allowlist admits the whole prefix, "+
			"so review each new one for side effects before extending knownDuckDBTableFunctions.\n got %v\nwant %v", got, want)
	}
}
