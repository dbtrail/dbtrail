package sqlsandbox

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"testing"
)

// #3096: a statement on the copy must not learn the host's folders, the
// copy's own file paths, the engine's settings or secrets, from a result or
// from an error.

// hostLeaks returns the host folders named in s.
func hostLeaks(f copyFixture, s string) []string {
	home, _ := os.UserHomeDir()
	var out []string
	for _, p := range []string{filepath.Join(home, ".duckdb"), f.archiveRoot, f.baselineRoot, spillPrefix, spoolPrefix} {
		if p != "" && p != "/" && strings.Contains(s, p) {
			out = append(out, p)
		}
	}
	return out
}

// The engine's settings, secrets, extensions, file cache, temporary files,
// session variables and logs are refused, in every spelling and at any
// depth, and the refusal names no folder.
func TestHostPaths_introspectionRefused(t *testing.T) {
	f := newCopyFixture(t)
	r := newTestRunner(t, testLimits())
	cases := map[string]string{
		"settings":                      "SELECT * FROM duckdb_settings()",
		"settings qualified":            "SELECT * FROM system.main.duckdb_settings()",
		"settings main":                 "SELECT * FROM main.duckdb_settings()",
		"settings quoted upper":         `SELECT * FROM "DUCKDB_SETTINGS"()`,
		"settings mixed case":           "SELECT * FROM DuckDB_Settings()",
		"settings in a subquery":        "SELECT (SELECT max(value) FROM duckdb_settings() WHERE name = 'temp_directory')",
		"settings in a CTE":             "WITH s AS (SELECT * FROM duckdb_settings()) SELECT * FROM s",
		"settings in a set operation":   "SELECT name FROM shop.orders, (SELECT 'x' AS name) q UNION ALL SELECT name FROM duckdb_settings()",
		"settings in a LATERAL":         "SELECT * FROM shop.orders o, LATERAL (SELECT value FROM duckdb_settings()) s",
		"profiling settings":            "SELECT * FROM duckdb_profiling_settings()",
		"secrets":                       "SELECT * FROM duckdb_secrets()",
		"secret types":                  "SELECT * FROM duckdb_secret_types()",
		"which secret":                  "SELECT * FROM which_secret('s3://b/k', 's3')",
		"extensions":                    "SELECT extension_name, install_path FROM duckdb_extensions()",
		"file cache":                    "SELECT * FROM duckdb_external_file_cache()",
		"file cache after a read":       "SELECT (SELECT count(*) FROM shop.orders), * FROM duckdb_external_file_cache()",
		"temporary files":               "SELECT * FROM duckdb_temporary_files()",
		"variables":                     "SELECT * FROM duckdb_variables()",
		"logs":                          "SELECT * FROM duckdb_logs()",
		"parsed logs":                   "SELECT * FROM duckdb_logs_parsed('QueryLog')",
		"log contexts":                  "SELECT * FROM duckdb_log_contexts()",
		"current_setting":               "SELECT current_setting('temp_directory')",
		"current_setting upper":         "SELECT CURRENT_SETTING('secret_directory')",
		"current_setting quoted":        `SELECT "current_setting"('allowed_directories')`,
		"current_setting qualified":     "SELECT pg_catalog.current_setting('temp_directory') FROM shop.orders",
		"current_setting in WHERE":      "SELECT id FROM shop.orders WHERE current_setting('temp_directory') LIKE '/%'",
		"current_setting in a CTE":      "WITH s AS (SELECT current_setting('home_directory') AS v) SELECT v FROM s",
		"getvariable":                   "SELECT getvariable('x')",
		"getvariable upper":             "SELECT GETVARIABLE('x')",
		"pg_settings":                   "SELECT * FROM pg_settings",
		"pg_settings qualified":         "SELECT * FROM pg_catalog.pg_settings",
		"pg_settings fully qualified":   "SELECT * FROM system.pg_catalog.pg_settings",
		"pg_settings quoted upper":      `SELECT * FROM "PG_SETTINGS"`,
		"pg_settings in a subquery":     "SELECT id FROM shop.orders WHERE EXISTS (SELECT 1 FROM pg_settings)",
		"pg_settings joined":            "SELECT * FROM shop.orders JOIN pg_catalog.pg_settings ON true",
		"not listed: connection count":  "SELECT * FROM duckdb_connection_count()",
		"not listed: prepared":          "SELECT * FROM duckdb_prepared_statements()",
		"not listed: memory":            "SELECT * FROM duckdb_memory()",
		"not listed: optimizers":        "SELECT * FROM duckdb_optimizers()",
		"not listed: approx db count":   "SELECT * FROM duckdb_approx_database_count()",
		"not listed: table sample":      "SELECT * FROM duckdb_table_sample('shop.orders')",
	}
	for name, q := range cases {
		t.Run(name, func(t *testing.T) {
			res, err := r.Run(context.Background(), f.job(q))
			var re *RefusedError
			if !errors.As(err, &re) {
				t.Fatalf("%s: err = %v (rows %v), want RefusedError", q, err, res.Rows)
			}
			if !strings.Contains(re.Reason, "not allowed") {
				t.Errorf("%s: reason %q is not the copy's refusal", q, re.Reason)
			}
			if l := hostLeaks(f, re.Reason); len(l) > 0 {
				t.Errorf("%s: the refusal names %v: %s", q, l, re.Reason)
			}
		})
	}
}

// The engine's settings view is refused by what it is, however the reference
// is qualified or spelled; a relation of the copy that only shares its name
// stays readable when it is named in its own schema.
func TestHostPaths_settingsViewEveryQualification(t *testing.T) {
	f := newCopyFixture(t)
	f.views += "\nCREATE VIEW shop.pg_settings AS SELECT 1 AS x;\n"
	r := newTestRunner(t, testLimits())
	for _, q := range []string{
		"SELECT * FROM pg_settings",
		`SELECT * FROM "pg_settings"`,
		"SELECT * FROM PG_SETTINGS",
		`SELECT * FROM "Pg_Settings"`,
		"SELECT * FROM pg_catalog.pg_settings",
		`SELECT * FROM "PG_CATALOG"."PG_SETTINGS"`,
		"SELECT * FROM system.pg_catalog.pg_settings",
		`SELECT * FROM "system"."pg_catalog"."pg_settings"`,
		"SELECT * FROM SYSTEM.PG_CATALOG.PG_SETTINGS",
		"SELECT * FROM system.main.pg_settings",
		"SELECT * FROM main.pg_settings",
		"SELECT * FROM memory.pg_catalog.pg_settings",
		"SELECT * FROM temp.pg_catalog.pg_settings",
		"SELECT * FROM information_schema.pg_settings",
		"SELECT * FROM system.shop.pg_settings",
		"SELECT * FROM nosuchschema.pg_settings",
		"SELECT x FROM shop.orders o, LATERAL (SELECT 1 AS x FROM system.pg_catalog.pg_settings) s",
		"SELECT 1 FROM shop.pg_settings, pg_catalog.pg_settings",
	} {
		_, err := r.Run(context.Background(), f.job(q))
		var re *RefusedError
		if !errors.As(err, &re) {
			t.Errorf("%s: err = %v, want RefusedError", q, err)
		}
	}
	for _, q := range []string{
		"SELECT x FROM shop.pg_settings",
		"SELECT x FROM memory.shop.pg_settings",
		`SELECT x FROM "SHOP"."PG_SETTINGS"`,
	} {
		if _, err := r.Run(context.Background(), f.job(q)); err != nil {
			t.Errorf("%s: %v, want the copy's own view read", q, err)
		}
	}
}

// What a client reads the catalog with still works, by name, qualified, and
// in upper case: tools and the ports list schemas, tables and columns this
// way, and plain reads of the copy are untouched.
func TestHostPaths_catalogStillAnswers(t *testing.T) {
	f := newCopyFixture(t)
	r := newTestRunner(t, testLimits())
	for _, q := range []string{
		"SELECT id, status FROM shop.orders ORDER BY id",
		"SELECT count(*) FROM duckdb_tables()",
		"SELECT count(*) FROM SYSTEM.MAIN.DUCKDB_TABLES()",
		`SELECT count(*) FROM "duckdb_columns"()`,
		"SELECT count(*) FROM duckdb_columns() WHERE schema_name = 'shop'",
		"SELECT count(*) FROM duckdb_schemas()",
		"SELECT count(*) FROM duckdb_views()",
		"SELECT count(*) FROM duckdb_databases()",
		"SELECT count(*) FROM duckdb_types()",
		"SELECT count(*) FROM duckdb_functions()",
		"SELECT count(*) FROM duckdb_keywords()",
		"SELECT count(*) FROM duckdb_constraints()",
		"SELECT count(*) FROM duckdb_indexes()",
		"SELECT count(*) FROM duckdb_sequences()",
		"SELECT count(*) FROM duckdb_dependencies()",
		"SELECT count(*) FROM duckdb_coordinate_systems()",
		"SELECT count(*) FROM information_schema.tables",
		"SELECT count(*) FROM information_schema.columns",
		"SELECT count(*) FROM pg_catalog.pg_class",
		"SELECT count(*) FROM pg_catalog.pg_namespace",
		"SELECT format_type(23, -1)",
		"SHOW TABLES",
		"DESCRIBE shop.orders",
		"SELECT * FROM pragma_table_info('shop.orders')",
		"SELECT current_database(), current_schema(), version()",
		"SELECT count(*) FROM read_parquet('" + filepath.Join(f.archiveRoot, "**", "*.parquet") + "')",
		// A column or alias that is only named like a refused function.
		"SELECT 1 AS current_setting, 2 AS getvariable, 3 AS pg_settings",
		"WITH duckdb_settings AS (SELECT 1 AS x) SELECT x FROM duckdb_settings",
	} {
		if _, err := r.Run(context.Background(), f.job(q)); err != nil {
			t.Errorf("%s: %v", q, err)
		}
	}
}

// The same on a server with hidden rules (#2529), which also refuses the
// views' text; pg_get_viewdef prints it too.
func TestHostPaths_hiddenRulesServer(t *testing.T) {
	f := newCopyFixture(t)
	r := newTestRunner(t, testLimits())
	hidden := func(q string) Job {
		j := f.job(q)
		j.NoFileReaders = true
		return j
	}
	for _, q := range []string{
		"SELECT * FROM duckdb_external_file_cache()",
		"SELECT * FROM duckdb_settings()",
		"SELECT current_setting('allowed_directories')",
		"SELECT * FROM pg_settings",
		"SELECT pg_get_viewdef(oid) FROM pg_class",
		"SELECT PG_CATALOG.PG_GET_VIEWDEF(oid) FROM pg_class",
	} {
		_, err := r.Run(context.Background(), hidden(q))
		var re *RefusedError
		if !errors.As(err, &re) {
			t.Errorf("%s on a hidden-rules server: err = %v, want RefusedError", q, err)
			continue
		}
		if l := hostLeaks(f, re.Reason); len(l) > 0 {
			t.Errorf("%s: the refusal names %v", q, l)
		}
	}
	for _, q := range []string{"SELECT id FROM shop.orders ORDER BY id", "SELECT count(*) FROM duckdb_tables()"} {
		if _, err := r.Run(context.Background(), hidden(q)); err != nil {
			t.Errorf("%s on a hidden-rules server: %v", q, err)
		}
	}
	// On a server without hidden rules the views' text stays readable, as
	// duckdb_views() and the views file already show it.
	if _, err := r.Run(context.Background(), f.job("SELECT count(pg_get_viewdef(oid)) FROM pg_class")); err != nil {
		t.Errorf("pg_get_viewdef without hidden rules: %v", err)
	}
}

// An engine error that would name a folder of the copy reaches the client
// with the folder replaced.
func TestHostPaths_engineErrorsHideFolders(t *testing.T) {
	f := newCopyFixture(t)
	r := newTestRunner(t, testLimits())
	for _, q := range []string{
		"SELECT * FROM read_parquet('" + filepath.Join(f.archiveRoot, "nosuch.parquet") + "')",
		"SELECT * FROM read_parquet('" + filepath.Join(f.baselineRoot, "x", "nosuch.parquet") + "')",
		"SELECT * FROM read_csv('" + f.archiveRoot + "/')",
	} {
		_, err := r.Run(context.Background(), f.job(q))
		if err == nil {
			t.Errorf("%s: no error", q)
			continue
		}
		if l := hostLeaks(f, err.Error()); len(l) > 0 {
			t.Errorf("%s: the error names %v: %v", q, l, err)
		}
		if !strings.Contains(err.Error(), hiddenPathWord) {
			t.Errorf("%s: the error does not say a folder was left out: %v", q, err)
		}
	}
}

func TestHideHostPaths(t *testing.T) {
	home := "/home/svc"
	roots := []string{"/data/copy/archive/", "/data/copy", "/tmp", home + "/.duckdb", home, "/", ""}
	h := hiddenPathWord
	cases := []struct{ in, want string }{
		{`No files found that match the pattern "/data/copy/archive/x.parquet"`, `No files found that match the pattern "` + h + `/x.parquet"`},
		{`Cannot access directory "/home/svc/.duckdb/stored_secrets" - disabled`, `Cannot access directory "` + h + `/stored_secrets" - disabled`},
		{`allowed: [/data/copy/, /tmp/bintrail-sql-spill-1/]`, `allowed: [` + h + `/, ` + h + `/bintrail-sql-spill-1/]`},
		{"at /data/copy", "at " + h},
		{"two /tmp/a and /tmp/b", "two " + h + "/a and " + h + "/b"},
		// Not a known folder: a longer name, a folder inside another one, a
		// URL, a date, a division.
		{"/tmpfiles/x", "/tmpfiles/x"},
		{"/mnt/data/copy/x", "/mnt/data/copy/x"},
		{"/home/svc2/x", "/home/svc2/x"},
		{"s3://bucket/tmp/x", "s3://bucket/tmp/x"},
		{"2026/11/21", "2026/11/21"},
		{"SELECT 1/2 FROM t", "SELECT 1/2 FROM t"},
		{"Binder Error: column x not found", "Binder Error: column x not found"},
		{"", ""},
	}
	for _, c := range cases {
		if got := hideHostPaths(c.in, roots); got != c.want {
			t.Errorf("hideHostPaths(%q)\n got %q\nwant %q", c.in, got, c.want)
		}
	}
	if got := hideHostPaths("at /data/copy", nil); got != "at /data/copy" {
		t.Errorf("no roots: %q", got)
	}
}

// A folder is hidden in the form it was configured in and in the form its
// links resolve to, after a colon, in quotes and at the end of the text; a
// URL stays as written.
func TestHideHostPaths_resolvedFormsAndColons(t *testing.T) {
	real := t.TempDir()
	link := filepath.Join(t.TempDir(), "copy-link")
	if err := os.Symlink(real, link); err != nil {
		t.Skipf("cannot make a symbolic link here: %v", err)
	}
	resolved, err := filepath.EvalSymlinks(real)
	if err != nil {
		t.Fatal(err)
	}
	roots := []string{link}
	h := hiddenPathWord
	for _, form := range []string{link, resolved} {
		cases := []struct{ in, want string }{
			{"open " + form + "/a.parquet: no such file", "open " + h + "/a.parquet: no such file"},
			{"open: " + form + "/a.parquet", "open: " + h + "/a.parquet"},
			{"open:" + form + "/a.parquet", "open:" + h + "/a.parquet"},
			{`pattern "` + form + `/x"`, `pattern "` + h + `/x"`},
			{`pattern '` + form + `'`, `pattern '` + h + `'`},
			{"looked in " + form, "looked in " + h},
		}
		for _, c := range cases {
			if got := hideHostPaths(c.in, roots); got != c.want {
				t.Errorf("hideHostPaths(%q)\n got %q\nwant %q", c.in, got, c.want)
			}
		}
	}
	for _, u := range []string{
		"s3://bucket" + resolved + "/x", "https://example.com" + resolved, "file://" + resolved + "x",
	} {
		if got := hideHostPaths(u, roots); got != u {
			t.Errorf("hideHostPaths(%q) = %q, want the URL as written", u, got)
		}
	}
}

// Every table function the pinned engine has is either allowed by name or
// known to be refused, so a function a new engine version adds is refused
// until it is reviewed and listed, and this test says which one it is.
var knownRefusedTableFunctions = []string{
	"arrow_scan", "arrow_scan_dumb", "check_peg_parser", "checkpoint", "disable_logging",
	"disable_peg_parser", "disable_profiling", "duckdb_approx_database_count", "duckdb_connection_count",
	"duckdb_extensions", "duckdb_external_file_cache", "duckdb_log_contexts", "duckdb_logs",
	"duckdb_logs_parsed", "duckdb_memory", "duckdb_optimizers", "duckdb_prepared_statements",
	"duckdb_profiling_settings", "duckdb_secret_types", "duckdb_secrets", "duckdb_settings",
	"duckdb_table_sample", "duckdb_temporary_files", "duckdb_variables", "enable_logging",
	"enable_peg_parser", "enable_profiling", "force_checkpoint", "histogram", "histogram_values",
	"icu_calendar_names", "json_execute_serialized_sql", "parquet_full_metadata", "pg_timezone_names",
	"query", "query_table", "read_duckdb", "repeat_row", "seq_scan", "sql_auto_complete", "summary",
	"test_all_types", "test_vector_types", "truncate_duckdb_logs", "which_secret",
}

func TestTableFunctionsAreAllClassified(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	got := engineFunctionNames(t, db, `function_type IN ('table', 'table_macro')`)
	var unclassified, both []string
	for _, n := range got {
		allowed, refused := allowedTableFunctions[n], slices.Contains(knownRefusedTableFunctions, n)
		switch {
		case allowed && refused:
			both = append(both, n)
		case !allowed && !refused:
			unclassified = append(unclassified, n)
		}
	}
	if len(unclassified) > 0 {
		t.Errorf("table functions of this engine in neither list (refused on the copy until reviewed): %v; "+
			"add each to allowedTableFunctions only if it reads nothing of the host (no path, directory, setting, "+
			"secret, extension or file cache) and changes nothing, else to knownRefusedTableFunctions", unclassified)
	}
	if len(both) > 0 {
		t.Errorf("in both lists: %v", both)
	}
	for n := range allowedTableFunctions {
		if !slices.Contains(got, n) {
			t.Errorf("allowedTableFunctions lists %q, which this engine does not have", n)
		}
	}
}

// Scalar functions are refused by name, not allowed by name (there are
// hundreds, and a statement needs them). This test is the watch over that
// list: any scalar or macro whose name speaks of settings, secrets, paths,
// folders, files, caches, variables, the environment or the logs must be
// reviewed and listed here, refused or found harmless.
var reviewedScalarFunctions = map[string]bool{
	// Refused (refusedScalarFunctions).
	"current_setting": true, "getvariable": true,
	// Harmless: they work on the text or value they are given, or name the
	// in-memory database and schema.
	"current_database": true, "current_catalog": true, "has_database_privilege": true,
	"pg_is_other_temp_schema": true, "pg_my_temp_schema": true, "pg_ts_config_is_visible": true,
	"pg_ts_template_is_visible": true, "in_search_path": true, "json_extract_path": true,
	"json_extract_path_text": true, "log": true, "log10": true, "log2": true, "parse_dirname": true,
	"parse_dirpath": true, "parse_filename": true, "parse_path": true, "parse_duckdb_log_message": true,
	"write_log": true,
}

func TestScalarFunctionsNamedLikeTheHostAreReviewed(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	got := engineFunctionNames(t, db, `function_type IN ('scalar', 'macro')
		AND regexp_matches(function_name, '(setting|secret|path|dir|file|cache|variable|env|home|temp|config|extension|log|profil|database|catalog|attach|storage)')`)
	var unreviewed []string
	for _, n := range got {
		if !reviewedScalarFunctions[n] {
			unreviewed = append(unreviewed, n)
		}
	}
	if len(unreviewed) > 0 {
		t.Errorf("scalar functions of this engine not reviewed for #3096: %v; refuse each that returns a path, "+
			"a directory, a setting, a secret or a variable (refusedScalarFunctions), and list it here", unreviewed)
	}
	for n := range refusedScalarFunctions {
		if !reviewedScalarFunctions[n] {
			t.Errorf("refusedScalarFunctions has %q, not listed as reviewed here", n)
		}
	}
}

func engineFunctionNames(t *testing.T, db *sql.DB, where string) []string {
	t.Helper()
	rows, err := db.Query(fmt.Sprintf(`SELECT DISTINCT function_name FROM duckdb_functions() WHERE %s ORDER BY 1`, where))
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
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	sort.Strings(got)
	return got
}
