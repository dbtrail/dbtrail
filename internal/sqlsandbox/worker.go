package sqlsandbox

import (
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/big"
	"os"
	"runtime/debug"
	"slices"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/duckdb/duckdb-go/v2"

	"github.com/dbtrail/dbtrail/internal/duckdbutil"
)

// IsWorkerProcess reports whether this process was started as a worker by a
// Runner (the marker is in the environment). The console's hidden command is
// selected by argument; the package's own test binary uses this.
func IsWorkerProcess() bool { return os.Getenv(workerEnv) == "1" }

// WorkerMain is the child's whole life: open its DuckDB, read one job from
// stdin, run it, write one result to stdout, exit. The exit code is 0
// whenever a result (including a structured error) was written; anything
// else is a protocol failure the parent reports as a WorkerError with stderr
// attached.
//
// DuckDB is opened BEFORE the job is read (#2236), so a worker started ahead
// of its statement has that part done when the statement arrives. Nothing
// about a job is known then and nothing of one is applied: the threads, the
// memory limit, the spill directory and the directories it may read are all
// set from the job, in runJobOn.
func WorkerMain(stdin io.Reader, stdout, stderr io.Writer) int {
	// First thing: on Linux, be the kernel's first choice if memory runs
	// out on the host, ahead of the console that is also the capture plane.
	lowerOOMPriority(stderr)
	sess := openSession(stderr)
	defer sess.close()
	var job wireJob
	dec := json.NewDecoder(stdin)
	if err := dec.Decode(&job); err != nil {
		if errors.Is(err, io.EOF) {
			// Stdin closed with no job: a worker started ahead that its
			// parent stopped, or whose parent is gone. Not a failure.
			return 0
		}
		fmt.Fprintf(stderr, "sql worker: read job: %v\n", err)
		return 2
	}
	// The parent kills this process at its deadline. If the parent is gone
	// (crashed, OOM-killed) nobody will, so the child stops on its own a
	// little after: the query is not worth 2 threads and 2 GB for hours.
	if job.TimeoutNS > 0 {
		stop := time.AfterFunc(time.Duration(job.TimeoutNS)+selfDeadlineGrace, func() {
			fmt.Fprintln(stderr, "sql worker: past its deadline with no parent kill; exiting")
			os.Exit(selfDeadlineExit)
		})
		defer stop.Stop()
	}
	// The views a statement needs (#2029): the worker says what the statement
	// names, on stdout ahead of the result, and the parent answers on stdin
	// with the script to install.
	ask := func(refs Refs) (string, error) {
		if err := json.NewEncoder(stdout).Encode(wireResult{Ask: &refs}); err != nil {
			return "", fmt.Errorf("send the statement's names: %w", err)
		}
		var v wireViews
		if err := dec.Decode(&v); err != nil {
			return "", fmt.Errorf("read the views script: %w", err)
		}
		return v.ViewsSQL, nil
	}
	res := runJobOn(sess, job, ask, stderr)
	if err := json.NewEncoder(stdout).Encode(res); err != nil {
		fmt.Fprintf(stderr, "sql worker: write result: %v\n", err)
		return 2
	}
	return 0
}

// selfDeadlineGrace is how long past the parent's timeout the child waits for
// the parent's kill before exiting on its own; selfDeadlineExit is its exit
// status then. Only an orphan ever reaches it: a live parent kills the
// process group at the timeout itself.
const (
	selfDeadlineGrace = 2 * time.Second
	selfDeadlineExit  = 3
)

// session is the worker's in-memory DuckDB and its one connection, opened
// before the job is known. A failure to open is kept, not returned: the
// worker still reads its job and answers it with that failure, so the parent
// gets a typed answer either way.
type session struct {
	db     *sql.DB
	conn   *sql.Conn
	openNS int64
	err    error
}

// openSession opens the DuckDB a job will run on. It never panics out: a
// panic becomes the session's error, with the stack on stderr.
func openSession(stderr io.Writer) (s *session) {
	s = &session{}
	start := time.Now()
	defer func() {
		if r := recover(); r != nil {
			fmt.Fprintf(stderr, "sql worker: panic: %v\n%s", r, debug.Stack())
			s.err = fmt.Errorf("sql worker panicked: %v", r)
		}
		s.openNS = int64(time.Since(start))
	}()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		s.err = fmt.Errorf("open DuckDB: %v", err)
		return s
	}
	s.db = db
	// ONE connection for everything. DuckDB scopes some settings (TimeZone)
	// and every SET VARIABLE to the connection, and the views script uses
	// both; a pooled second connection would not see them.
	db.SetMaxOpenConns(1)
	conn, err := db.Conn(context.Background())
	if err != nil {
		s.err = fmt.Errorf("open DuckDB connection: %v", err)
		return s
	}
	s.conn = conn
	// Paid here so a worker started ahead of its statement has it done: the
	// settings every job gets, and one parse, which is what first loads the
	// statement parser. The job's lockdown runs them all again, so nothing
	// depends on this having worked, and a failure here is the job's to
	// report, in its own words.
	for _, stmt := range fixedSettings {
		if _, err := conn.ExecContext(context.Background(), stmt); err != nil {
			fmt.Fprintf(stderr, "sql worker: ahead of the job, %s: %v\n", stmt, err)
			break
		}
	}
	var parsed string
	if err := conn.QueryRowContext(context.Background(), "SELECT json_serialize_sql('SELECT 1')::VARCHAR").Scan(&parsed); err != nil {
		fmt.Fprintf(stderr, "sql worker: ahead of the job, the parser: %v\n", err)
	}
	return s
}

func (s *session) close() {
	if s.conn != nil {
		_ = s.conn.Close()
	}
	if s.db != nil {
		_ = s.db.Close()
	}
}

// runJob opens a session and runs job on it.
func runJob(job wireJob, ask func(Refs) (string, error), stderr io.Writer) wireResult {
	sess := openSession(stderr)
	defer sess.close()
	return runJobOn(sess, job, ask, stderr)
}

// runJobOn never panics out: a panic anywhere below becomes a session error
// with the stack on stderr, so the parent gets a typed answer.
func runJobOn(sess *session, job wireJob, ask func(Refs) (string, error), stderr io.Writer) (res wireResult) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Fprintf(stderr, "sql worker: panic: %v\n%s", r, debug.Stack())
			res = wireResult{Error: &wireError{Kind: errSession, Message: fmt.Sprintf("sql worker panicked: %v", r)}}
		}
	}()
	if sess.err != nil {
		return sessionErr("%v", sess.err)
	}
	ctx := context.Background()
	conn := sess.conn
	res.OpenNS = sess.openNS
	phase := time.Now()
	mark := func(dst *int64) {
		now := time.Now()
		*dst = int64(now.Sub(phase))
		phase = now
	}

	stmt, reason := parseStatement(ctx, conn, job.SQL)
	if reason != "" {
		return wireResult{Error: &wireError{Kind: errRefused, Message: reason}}
	}

	// Caps first, so the views install already runs under them.
	caps := duckdbutil.Tuning{Threads: job.Threads, MemoryLimit: job.MemoryLimit}
	for _, stmt := range caps.Statements() {
		if _, err := conn.ExecContext(ctx, stmt); err != nil {
			return sessionErr("%s: %v", stmt, err)
		}
	}
	// Then the restrictions, then the views, then the lock. The views are
	// installed AFTER external access is restricted so an installed view can
	// only ever reach the copy directories, and BEFORE the lock because the
	// script sets the session time zone.
	lock := lockdownStatements(job.CopyDirs, spillSpec{Dir: job.SpillDir, MaxBytes: job.SpillMaxBytes})
	restrict, lockLast := lock[:len(lock)-1], lock[len(lock)-1]
	for _, stmt := range restrict {
		if _, err := conn.ExecContext(ctx, stmt); err != nil {
			return sessionErr("%s: %v", stmt, err)
		}
	}
	mark(&res.LockdownNS)
	viewsSQL := job.ViewsSQL
	if job.AskViews {
		var err error
		if viewsSQL, err = ask(collectRefs(stmt)); err != nil {
			return sessionErr("%v", err)
		}
	}
	if strings.TrimSpace(viewsSQL) != "" {
		if _, err := conn.ExecContext(ctx, viewsSQL); err != nil {
			return sessionErr("install the copy's views: %v", err)
		}
	}
	// The port's USE, before the lock like every other SET. A schema the
	// views did not create (a default seeded from the source DSN, a typo in
	// -D) is NOT applied: SET search_path refuses it, and refusing every
	// statement on the connection would take down SHOW DATABASES, the one
	// statement that shows the way out. The default stays, and a name that
	// then fails to resolve carries the hint.
	//
	// The path is set to the name as the CATALOG spells it, found by the
	// rule DuckDB itself resolves names by (catalogSchema), so the probe and
	// the SET cannot disagree; and if the SET is refused all the same, that
	// is one more schema that was not applied, not a failed session.
	missingSchema := ""
	if job.Schema != "" {
		name, ok := catalogSchema(ctx, conn, job.Schema)
		if ok {
			if _, err := conn.ExecContext(ctx, "SET search_path = "+searchPathLiteral(name)); err != nil {
				fmt.Fprintf(stderr, "sql worker: USE %q was not applied: SET search_path: %v\n", job.Schema, err)
				ok = false
			}
		}
		if !ok {
			missingSchema = job.Schema
		}
	}
	// The client's time zone (the port's SET time_zone), after the views
	// script, which pins UTC, and before the lock, after which no SET runs. A
	// zone the engine does not know refuses the statement by name: running it
	// under UTC instead would be the silent wrong answer this exists to end.
	if job.TimeZone != "" {
		if _, err := conn.ExecContext(ctx, "SET TimeZone = '"+strings.ReplaceAll(job.TimeZone, "'", "''")+"'"); err != nil {
			return wireResult{Error: &wireError{Kind: errRefused, Message: fmt.Sprintf(
				"the session time zone %q (SET time_zone) cannot be applied on the copy: %v", job.TimeZone, firstLine(err.Error()))}}
		}
	}
	if _, err := conn.ExecContext(ctx, lockLast); err != nil {
		return sessionErr("%s: %v", lockLast, err)
	}
	mark(&res.ViewsNS)

	// The client's sql_select_limit: the cut it asked for, when the statement
	// is a SELECT with no LIMIT of its own and the limit is within the cap.
	rowLimit, askedFor := job.MaxRows, false
	if job.SelectLimit > 0 {
		applies, known := selectLimitApplies(stmt)
		if !known {
			return wireResult{Error: &wireError{Kind: errRefused, Message: "sql_select_limit is set on this connection and " +
				"cannot be applied to a statement of this shape; give the statement its own LIMIT, or SET sql_select_limit = DEFAULT"}}
		}
		if applies && job.SelectLimit <= job.MaxRows {
			rowLimit, askedFor = job.SelectLimit, true
		}
	}

	start := time.Now()
	rows, err := conn.QueryContext(ctx, job.SQL)
	if err != nil {
		msg := err.Error()
		if missingSchema != "" && strings.Contains(msg, "does not exist") {
			msg += fmt.Sprintf("; the current database %q is not in the copy, so unqualified names resolve in main: "+
				"SHOW DATABASES lists the schemas that are, USE one of them", missingSchema)
		}
		return wireResult{Error: &wireError{Kind: errQuery, Message: msg}}
	}
	defer rows.Close()
	types, err := rows.ColumnTypes()
	if err != nil {
		return wireResult{Error: &wireError{Kind: errQuery, Message: err.Error()}}
	}
	res.Columns = make([]Column, len(types))
	typeNames := make([]string, len(types))
	for i, ct := range types {
		typeNames[i] = ct.DatabaseTypeName()
		res.Columns[i] = Column{Name: ct.Name(), Type: typeNames[i]}
	}
	res.Rows = [][]any{}
	var rd renderer
	var bytesSoFar int64
	for rows.Next() {
		if len(res.Rows) == rowLimit {
			// One past the limit: the result was cut. Do not keep it. A cut
			// at the client's own sql_select_limit is what it asked for, not
			// a truncation.
			res.Truncated = !askedFor
			break
		}
		raw := make([]any, len(types))
		ptrs := make([]any, len(types))
		for i := range raw {
			ptrs[i] = &raw[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return wireResult{Error: &wireError{Kind: errQuery, Message: err.Error()}}
		}
		row := make([]any, len(raw))
		for i, v := range raw {
			row[i] = rd.cell(v, typeNames[i])
		}
		// Count the result as it grows, in the bytes the parent will read,
		// and stop before building one it would refuse anyway.
		if job.MaxResultBytes > 0 {
			enc, err := json.Marshal(row)
			if err != nil {
				return wireResult{Error: &wireError{Kind: errQuery, Message: "encode row: " + err.Error()}}
			}
			bytesSoFar += int64(len(enc)) + 2
			if bytesSoFar > job.MaxResultBytes {
				return wireResult{Error: &wireError{Kind: errTooLarge,
					Message: fmt.Sprintf("the result passed %d bytes at row %d", job.MaxResultBytes, len(res.Rows)+1)}}
			}
		}
		res.Rows = append(res.Rows, row)
	}
	res.TruncatedCells = rd.truncated
	if err := rows.Err(); err != nil {
		return wireResult{Error: &wireError{Kind: errQuery, Message: err.Error()}}
	}
	res.ElapsedNS = int64(time.Since(start))
	return res
}

// selectLimitApplies reports whether MySQL's sql_select_limit governs the
// parsed statement: a SELECT (or a set operation) with no LIMIT of its own at
// the top level. A LIMIT there takes precedence, as on MySQL, an OFFSET alone
// does not count as one, and SHOW, DESCRIBE and SUMMARIZE (a SELECT over a
// SHOW_REF in DuckDB's tree) are not SELECTs on MySQL. known is false for a
// tree this cannot read: the caller refuses instead of guessing, because both
// guesses are silent (a cut below the statement's own LIMIT, or more rows
// than the client's limit).
func selectLimitApplies(stmt any) (applies, known bool) {
	top, _ := stmt.(map[string]any)
	node, _ := top["node"].(map[string]any)
	switch node["type"] {
	case "SELECT_NODE":
		if from, _ := node["from_table"].(map[string]any); from["type"] == "SHOW_REF" {
			return false, true
		}
	case "SET_OPERATION_NODE":
	default:
		return false, false
	}
	mods, ok := node["modifiers"].([]any)
	if !ok && node["modifiers"] != nil {
		return false, false
	}
	for _, m := range mods {
		mod, ok := m.(map[string]any)
		if !ok {
			return false, false
		}
		switch mod["type"] {
		case "LIMIT_PERCENT_MODIFIER":
			return false, true
		case "LIMIT_MODIFIER":
			if mod["limit"] != nil {
				return false, true
			}
		}
	}
	return true, true
}

// firstLine keeps an engine message's first line: DuckDB appends a list of
// candidates on the following ones.
func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return strings.TrimSpace(s[:i])
	}
	return s
}

func sessionErr(format string, args ...any) wireResult {
	return wireResult{Error: &wireError{Kind: errSession, Message: fmt.Sprintf(format, args...)}}
}

// sandboxSettings are the DuckDB settings the lock-down sets, verified to
// exist on DuckDB v1.4.5 (TestSandboxSettingsExistInPinnedEngine keeps that
// true across engine bumps). allowed_paths (single files) also exists there
// and is not needed: the copy is whole directories.
var sandboxSettings = []string{
	"default_collation",
	"default_null_order",
	"ieee_floating_point_ops",
	"allowed_directories",
	"enable_external_access",
	"autoinstall_known_extensions",
	"autoload_known_extensions",
	"temp_directory",
	"max_temp_directory_size",
	"lock_configuration",
}

// spillSpec is where a worker may spill past its memory limit (#2210): Dir is
// the private directory the parent made for this statement, MaxBytes what it
// may hold. A zero spec is no spill.
type spillSpec struct {
	Dir      string
	MaxBytes int64
}

// lockdownStatements is the session lock-down, in order, lock LAST. What
// each one does, as observed on DuckDB v1.4.5:
//
//   - autoinstall_known_extensions / autoload_known_extensions = false,
//     FIRST: a function or a collation that lives in a not-yet-loaded
//     extension does not trigger a download or a load; it is simply not
//     there. First, because the next statement names an ICU collation, and
//     the product runs air-gapped: with these off, an engine that lacked ICU
//     fails that SET by name instead of reaching for the network.
//   - default_collation = 'nocase.icu_noaccent' and default_null_order =
//     'nulls_first_on_asc_last_on_desc': MySQL semantics for the two things
//     no statement-level check can catch (#2038). MySQL's default collation
//     (utf8mb4_0900_ai_ci) treats 'Paid' and 'paid', 'café' and 'cafe' as
//     EQUAL in WHERE, GROUP BY, SELECT DISTINCT, IN and ORDER BY, and sorts
//     NULL first on ASC and last on DESC (NULL is its smallest value);
//     DuckDB's defaults do neither, so a copy-served statement answered
//     differently from MySQL without any error. The collation is nocase
//     over ICU's accent-insensitive one (#2083): like MySQL's it also
//     equates 'ß' with 'ss', full-width forms, ligatures and kana, and sorts
//     punctuation before digits, where the built-in nocase.noaccent this
//     used to be folds case and accents and nothing else (57 measured pairs:
//     1 disagreement with MySQL against 26; collations_2083_test.go). It
//     costs about twice as much on every comparison of text, which is the
//     price accepted for the same answers. ICU is statically linked into
//     the engine (TestLockdown_collationComesFromTheBinary), never loaded
//     at run time; a collation the engine does not have fails this SET, and
//     runJob fails the statement on any lock-down error, so no worker can
//     end up answering under another collation. Close to _ai_ci, not
//     identical: LIKE/REGEXP, count(DISTINCT ...) and the string-search
//     functions (instr, position, contains) do NOT fold under either
//     collation (DuckDB #10416 for LIKE), which the read router vetoes; a
//     column MySQL declares _cs becomes case-insensitive here. A _bin
//     column does not: its state view gives it COLLATE C
//     (views.BaselineTable.BinaryText, #2083), which outranks this default.
//     The copy's OWN views are immune on purpose:
//     the delta chain compares "bintrail_pk" as its bytes
//     (baseline.TableDeltaLatestSQL), or two keys differing only in case
//     would fold into one row. Locked with the rest so a statement cannot
//     undo them.
//   - ieee_floating_point_ops = false: division and modulo by zero are NULL,
//     as on MySQL (#2083). DuckDB's default answers +Infinity, -Infinity or
//     NaN without an error, so `amount / qty` held a value on the copy for
//     the rows where MySQL holds NULL, and every COUNT, SUM, AVG and WHERE
//     over it answered differently. Nothing else changes: an overflow is
//     still Infinity and a cast from 'nan' still NaN.
//   - allowed_directories = [copy dirs]: the directories reads may touch
//     while external access is off. A path outside them, including one that
//     traverses out with "..", is a Permission Error. Note it admits WRITES
//     into these directories too (COPY ... TO succeeds); the single-SELECT
//     check in checkStatement is what keeps the copy read-only.
//   - enable_external_access = false: no file system operations outside
//     allowed_directories (read_csv('/etc/passwd'), read_parquet elsewhere,
//     glob, ATTACH a file, COPY TO elsewhere), no http:// or s3:// URLs,
//     no INSTALL (it cannot reach the extension directory), no LOAD.
//   - temp_directory = the statement's spill directory, and
//     max_temp_directory_size = its cap (#2210). Past memory_limit DuckDB
//     moves what its operators can offload (the joins and aggregates the
//     views read the delta chain through) to that directory, and the
//     statement slows down instead of failing; past the cap it fails with
//     an out-of-memory error that names the cap. The directory is the
//     parent's (Runner.spawn makes it per statement and removes it when the
//     worker exits). DuckDB admits its temp directory to reads, so the
//     statement CAN list and read this one (observed on v1.4.5: glob and
//     read_csv over it succeed): that is why it is one statement's own and
//     lives as long as the statement, holding nothing but that statement's
//     own spill. Its parent and another statement's directory stay out of
//     reach, and nothing can be written there but by the engine, since the
//     only statement admitted is one SELECT.
//     With no spill spec, temp_directory is empty and nothing is written:
//     a statement past memory_limit fails, as before #2210.
//   - lock_configuration = true: from here on every SET, RESET and config
//     PRAGMA is "Cannot change configuration option ... the configuration
//     has been locked", including this one and every setting above.
//
// Three orderings are load-bearing. TestLockdownRunsInOrderOnPinnedEngine
// pins two on the real engine: temp_directory must be set BEFORE external
// access goes off (afterwards DuckDB answers "Modifying the temp_directory
// has been disabled by configuration"), and lock_configuration must be LAST.
// TestLockdown_collationComesFromTheBinary pins the third: extension
// loading goes off BEFORE the collation is set.
// fixedSettings are the session settings that are the same for every job:
// they name no directory, no limit and nothing a statement brings. They are
// the first statements of the lockdown, and openSession also runs them before
// the job is read (#2236), because the first of each kind is slow: setting
// the collation loads ICU, about 7 ms of the 12 a short statement spent
// between its job and its first row.
var fixedSettings = []string{
	"SET autoinstall_known_extensions = false",
	"SET autoload_known_extensions = false",
	"SET default_collation = 'nocase.icu_noaccent'",
	"SET default_null_order = 'nulls_first_on_asc_last_on_desc'",
	"SET ieee_floating_point_ops = false",
}

func lockdownStatements(copyDirs []string, spill spillSpec) []string {
	quoted := make([]string, len(copyDirs))
	for i, d := range copyDirs {
		quoted[i] = "'" + strings.ReplaceAll(d, "'", "''") + "'"
	}
	temp := []string{"SET temp_directory = ''"}
	if spill.Dir != "" && spill.MaxBytes > 0 {
		temp = []string{
			"SET temp_directory = '" + strings.ReplaceAll(spill.Dir, "'", "''") + "'",
			fmt.Sprintf("SET max_temp_directory_size = '%dMiB'", max(spill.MaxBytes>>20, 1)),
		}
	}
	return slices.Concat(fixedSettings, []string{
		"SET allowed_directories = [" + strings.Join(quoted, ", ") + "]",
	}, temp, []string{
		"SET enable_external_access = false",
		"SET lock_configuration = true",
	})
}

// allowedTableFunctions are the table functions a statement may call, at any
// depth: readers of files (DuckDB's own allowed_directories decides WHICH
// files), Parquet metadata, generators, JSON walkers, and the read-only
// pragma_* views. duckdb_* catalog functions are allowed by prefix below.
//
// Everything else is refused by name, because a SELECT-shaped statement can
// still change state through a table function after the lock (verified on
// v1.4.5): enable_logging(storage='file', ...) turns on a logger that later
// writes CSV files into the copy, disable_logging, checkpoint,
// force_checkpoint and truncate_duckdb_logs all run, query('...') and
// json_execute_serialized_sql('...') run arbitrary text hidden in a string,
// and query_table names a table by string.
var allowedTableFunctions = map[string]bool{
	"read_parquet": true, "parquet_scan": true,
	"read_csv": true, "read_csv_auto": true, "sniff_csv": true,
	"read_json": true, "read_json_auto": true, "read_json_objects": true, "read_json_objects_auto": true,
	"read_ndjson": true, "read_ndjson_auto": true, "read_ndjson_objects": true,
	"read_text": true, "read_blob": true, "glob": true,
	"parquet_metadata": true, "parquet_schema": true, "parquet_file_metadata": true,
	"parquet_kv_metadata": true, "parquet_bloom_probe": true,
	"range": true, "generate_series": true, "unnest": true, "repeat": true,
	"json_each": true, "json_tree": true,
	"pragma_version": true, "pragma_platform": true, "pragma_database_size": true,
	"pragma_storage_info": true, "pragma_table_info": true, "pragma_metadata_info": true,
	"pragma_collations": true, "pragma_show": true, "pragma_user_agent": true,
}

func tableFunctionAllowed(name string) bool {
	return allowedTableFunctions[name] || strings.HasPrefix(name, "duckdb_")
}

// refusedTableFunction walks the serialized statement tree and returns the
// first table function that is not allowed, or "" when every one is. A
// table function is the node DuckDB serializes as type TABLE_FUNCTION with
// its call under "function"; it can sit in FROM, a join side, a CTE, a
// subquery anywhere (select list, WHERE, LIMIT), a LATERAL, or a set
// operation side, so the walk is over every key of every object. A
// TABLE_FUNCTION node whose call cannot be read is refused too: unknown is
// not allowed.
func refusedTableFunction(node any) string {
	switch x := node.(type) {
	case map[string]any:
		if x["type"] == "TABLE_FUNCTION" {
			fn, _ := x["function"].(map[string]any)
			name, _ := fn["function_name"].(string)
			if name == "" {
				return "(unrecognized table function)"
			}
			if !tableFunctionAllowed(strings.ToLower(name)) {
				return name
			}
		}
		keys := make([]string, 0, len(x))
		for k := range x {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			if r := refusedTableFunction(x[k]); r != "" {
				return r
			}
		}
	case []any:
		for _, e := range x {
			if r := refusedTableFunction(e); r != "" {
				return r
			}
		}
	}
	return ""
}

// parseStatement asks DuckDB's own parser what the text is, through
// json_serialize_sql (built into the engine; verified on v1.4.5), and returns
// a refusal reason, or "" when the text is exactly one SELECT-shaped
// statement whose table functions are all allowed. The serializer accepts
// SELECT, FROM-first, VALUES, WITH, set operations, DESCRIBE, SHOW and
// SUMMARIZE, and reports "Only SELECT statements can be serialized" for
// everything else (COPY, CREATE, ATTACH, INSTALL, LOAD, SET, RESET, PRAGMA,
// CALL, EXPLAIN, INSERT, a top-level PIVOT, ...). A comment or
// whitespace-only text parses to zero statements.
//
// The parsed statement comes back with the verdict so the caller can read
// what it names (collectRefs) from the same parse that allowed it.
func parseStatement(ctx context.Context, conn *sql.Conn, text string) (any, string) {
	var raw string
	if err := conn.QueryRowContext(ctx, "SELECT json_serialize_sql(?::VARCHAR)::VARCHAR", text).Scan(&raw); err != nil {
		return nil, "could not parse the statement: " + err.Error()
	}
	var parsed struct {
		Error        bool   `json:"error"`
		ErrorType    string `json:"error_type"`
		ErrorMessage string `json:"error_message"`
		Statements   []any  `json:"statements"`
	}
	if err := json.Unmarshal([]byte(raw), &parsed); err != nil {
		return nil, "could not parse the statement: " + err.Error()
	}
	switch {
	case parsed.Error && strings.Contains(parsed.ErrorMessage, "Only SELECT statements"):
		return nil, "only a single SELECT statement can run here (no COPY, CREATE, ATTACH, INSTALL, LOAD, SET, PRAGMA or CALL)"
	case parsed.Error && parsed.ErrorType == "parser":
		return nil, "syntax error: " + parsed.ErrorMessage
	case parsed.Error:
		return nil, parsed.ErrorType + " error: " + parsed.ErrorMessage
	case len(parsed.Statements) == 0:
		return nil, "the query is empty"
	case len(parsed.Statements) > 1:
		return nil, fmt.Sprintf("one statement at a time: %d statements were given", len(parsed.Statements))
	}
	if name := refusedTableFunction(parsed.Statements[0]); name != "" {
		return nil, fmt.Sprintf("the table function %s is not allowed here; only readers (read_parquet, glob, the parquet_* metadata functions, range, unnest, json_each and the duckdb_* catalog) can run on the copy", name)
	}
	return parsed.Statements[0], ""
}

// renderer turns driver values into cells and counts the ones it cut.
type renderer struct{ truncated int }

// cell renders one value as a JSON-shaped cell (see Result). typ is DuckDB's
// type name for the column, used to tell a DATE from a TIMESTAMP, both of
// which the driver hands over as time.Time. Text longer than MaxCellBytes is
// cut (on a rune boundary) and marked.
func (r *renderer) cell(v any, typ string) any {
	switch x := v.(type) {
	case nil, bool, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return x
	case string:
		return r.text(x)
	case float32:
		return floatCell(float64(x))
	case float64:
		return floatCell(x)
	case time.Time:
		switch {
		case typ == "DATE":
			return x.Format("2006-01-02")
		case typ == "TIME" || strings.HasPrefix(typ, "TIME "):
			return x.Format("15:04:05.999999")
		default:
			return x.UTC().Format(time.RFC3339Nano)
		}
	case []byte:
		if utf8.Valid(x) {
			return r.text(string(x))
		}
		return r.text("0x" + strings.ToUpper(hex.EncodeToString(x)))
	case *big.Int:
		return x.String()
	case duckdb.Decimal:
		return decimalText(x)
	case duckdb.UUID:
		return x.String()
	case []any:
		out := make([]any, len(x))
		for i, e := range x {
			out[i] = r.cell(e, "")
		}
		return out
	case map[string]any:
		out := make(map[string]any, len(x))
		for k, e := range x {
			out[k] = r.cell(e, "")
		}
		return out
	case duckdb.Map:
		// A MAP is the driver's own map type, keyed by any value. Rendered
		// like a STRUCT, each key by its cell text, so the values inside go
		// through the same rules (a DECIMAL keeps its scale) and the cell is
		// the JSON object its column is declared as.
		out := make(map[string]any, len(x))
		for k, e := range x {
			key, ok := r.cell(k, "").(string)
			if !ok {
				key = fmt.Sprint(r.cell(k, ""))
			}
			out[key] = r.cell(e, "")
		}
		return out
	case duckdb.Union:
		// A UNION holds one member: the cell is that member's value.
		return r.cell(x.Value, "")
	case fmt.Stringer:
		return r.text(x.String())
	default:
		return r.text(fmt.Sprint(x))
	}
}

func (r *renderer) text(s string) string {
	if len(s) <= MaxCellBytes {
		return s
	}
	cut := MaxCellBytes
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	r.truncated++
	return s[:cut] + cellTruncatedMarker
}

// decimalText prints a DECIMAL with exactly its scale, trailing zeros kept:
// 10.00 for a DECIMAL(10,2), as MySQL prints one (#2083). The driver's own
// Decimal.String trims them ("10"), which reads as a different answer to a
// program that compares the returned text. The digits come from the unscaled
// integer, so nothing is rounded whatever the width.
func decimalText(d duckdb.Decimal) string {
	if d.Value == nil {
		// The driver fills Value for every storage width it knows. A cell
		// without one has no number to print, and a made-up "0" would read as
		// data: fail the statement (runJob reports the panic as its error).
		panic("a DECIMAL cell arrived from the driver without a value")
	}
	digits := d.Value.String()
	sign := ""
	if strings.HasPrefix(digits, "-") {
		sign, digits = "-", digits[1:]
	}
	scale := int(d.Scale)
	if scale == 0 {
		return sign + digits
	}
	if len(digits) <= scale {
		digits = strings.Repeat("0", scale-len(digits)+1) + digits
	}
	return sign + digits[:len(digits)-scale] + "." + digits[len(digits)-scale:]
}

// floatCell keeps a finite float as a number and renders the values JSON
// cannot carry as text.
func floatCell(f float64) any {
	switch {
	case math.IsNaN(f):
		return "NaN"
	case math.IsInf(f, 1):
		return "Infinity"
	case math.IsInf(f, -1):
		return "-Infinity"
	}
	return f
}

// catalogSchema finds the schema the views created under that name and
// returns it as the catalog spells it. Names match the way DuckDB's own
// lookup matches them: ASCII letters without regard to case, every other
// byte exactly. So "SHOP" finds shop, and "été" does not find Été.
//
// The comparison is done here and not in SQL on purpose. In SQL it would run
// under the session's default collation, which equates far more than that
// ('ß' with 'ss', accents, full-width forms), and lower() folds all of
// Unicode: both said a schema existed under a name SET search_path then
// refused, which failed every statement on the connection. A lookup failure
// reads as absent, which is the safe side (the default stays).
func catalogSchema(ctx context.Context, conn *sql.Conn, schema string) (string, bool) {
	rows, err := conn.QueryContext(ctx, "SELECT schema_name FROM information_schema.schemata")
	if err != nil {
		return "", false
	}
	defer rows.Close()
	found, ok := "", false
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return "", false
		}
		switch {
		case name == schema:
			return name, true
		case !ok && asciiEqualFold(name, schema):
			found, ok = name, true
		}
	}
	if rows.Err() != nil {
		return "", false
	}
	return found, ok
}

// asciiEqualFold reports whether two names are equal once ASCII letters are
// folded, comparing every other byte as it is.
func asciiEqualFold(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := 0; i < len(a); i++ {
		x, y := a[i], b[i]
		if 'A' <= x && x <= 'Z' {
			x += 'a' - 'A'
		}
		if 'A' <= y && y <= 'Z' {
			y += 'a' - 'A'
		}
		if x != y {
			return false
		}
	}
	return true
}

// searchPathLiteral renders one schema name as the string literal SET
// search_path takes: the name as a quoted identifier (so a space, a comma
// or a double quote inside it is part of the name, not a separator), inside
// a single-quoted string.
func searchPathLiteral(schema string) string {
	ident := `"` + strings.ReplaceAll(schema, `"`, `""`) + `"`
	return "'" + strings.ReplaceAll(ident, "'", "''") + "'"
}
