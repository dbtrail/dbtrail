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
	script := strings.Join(lockdownStatements([]string{"/copy"}, spillSpec{Dir: "/spill", MaxBytes: 1 << 30}), "\n")
	for _, s := range sandboxSettings {
		if !strings.Contains(script, "SET "+s+" ") {
			t.Errorf("lockdown does not SET %s:\n%s", s, script)
		}
	}
	last := lockdownStatements([]string{"/copy"}, spillSpec{})
	if !strings.HasPrefix(last[len(last)-1], "SET lock_configuration = true") {
		t.Errorf("lock_configuration must be the last statement, got %q", last[len(last)-1])
	}
}

// A copy directory with a quote in its name cannot break out of the literal.
func TestLockdownQuotesDirectories(t *testing.T) {
	stmts := lockdownStatements([]string{"/co'py", "/other"}, spillSpec{})
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
	stmts := lockdownStatements([]string{t.TempDir()}, spillSpec{})
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
	for _, s := range lockdownStatements([]string{t.TempDir()}, spillSpec{}) {
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

// The copy's default collation is an ICU one, and ICU must come from the
// binary itself: the product runs air-gapped, and the lock-down turns
// extension loading off. Three things hold that together, each pinned here on
// the engine this build links:
//
//   - extension auto-install and auto-load go off BEFORE the collation is set,
//     so setting it can never reach for the network;
//   - ICU is statically linked and loaded in that engine;
//   - a collation the engine does not have fails at the SET, by name. The
//     worker treats a failed lock-down statement as fatal for the statement,
//     so a build without ICU refuses every query loudly instead of answering
//     under some other collation.
func TestLockdown_collationComesFromTheBinary(t *testing.T) {
	stmts := lockdownStatements([]string{t.TempDir()}, spillSpec{})
	index := func(prefix string) int {
		for i, s := range stmts {
			if strings.HasPrefix(s, prefix) {
				return i
			}
		}
		t.Fatalf("lockdown has no %q:\n%s", prefix, strings.Join(stmts, "\n"))
		return -1
	}
	collation := index("SET default_collation = 'nocase.icu_noaccent'")
	for _, off := range []string{"SET autoinstall_known_extensions = false", "SET autoload_known_extensions = false"} {
		if index(off) > collation {
			t.Errorf("%q runs after the collation is set: an engine without ICU could try to download it", off)
		}
	}

	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	var mode string
	var loaded bool
	if err := db.QueryRow("SELECT install_mode, loaded FROM duckdb_extensions() WHERE extension_name = 'icu'").Scan(&mode, &loaded); err != nil {
		t.Fatalf("icu is not among this engine's extensions: %v", err)
	}
	if mode != "STATICALLY_LINKED" || !loaded {
		t.Errorf("icu install_mode = %s, loaded = %v; want STATICALLY_LINKED and loaded, or the copy's collation depends on a download", mode, loaded)
	}
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
	// After the lock, with external access and extension loading off, the
	// collation is in force and does what plain nocase.noaccent does not.
	var eszett, fullWidth, folds bool
	var name string
	if err := db.QueryRow("SELECT 'ß' = 'ss', 'Ａ' = 'a', 'Café' = 'cafe', current_setting('default_collation')").Scan(&eszett, &fullWidth, &folds, &name); err != nil {
		t.Fatal(err)
	}
	if !eszett || !fullWidth || !folds || name != "nocase.icu_noaccent" {
		t.Errorf("under the lock-down: 'ß' = 'ss' %v, full-width %v, case and accent %v, default_collation %q", eszett, fullWidth, folds, name)
	}

	other, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	if _, err := other.Exec("SET default_collation = 'nocase.icu_nosuch'"); err == nil || !strings.Contains(err.Error(), "does not exist") {
		t.Errorf("a collation the engine lacks was accepted at SET (err = %v): a build without ICU would start and answer under another collation", err)
	}
}
