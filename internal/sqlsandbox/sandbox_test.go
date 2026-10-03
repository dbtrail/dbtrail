package sqlsandbox

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/views"
)

// TestMain doubles as the worker: the parent re-executes THIS test binary with
// the worker marker in its environment, exactly the way the console binary is
// re-executed in production, so every test below drives the real child
// process boundary and not an in-process shortcut.
func TestMain(m *testing.M) {
	if IsWorkerProcess() {
		os.Exit(WorkerMain(os.Stdin, os.Stdout, os.Stderr))
	}
	os.Exit(m.Run())
}

// newTestRunner re-executes the test binary. Args is the empty, non-nil slice:
// nil would mean "the console's hidden command", which a test binary does not
// have (TestMain never parses its arguments in worker mode anyway).
func newTestRunner(t *testing.T, limits Limits) *Runner {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	return New(Config{Exe: exe, Args: []string{}, Limits: limits})
}

func testLimits() Limits {
	l := DefaultLimits()
	l.Timeout = 30 * time.Second
	return l
}

// A query over the views the console generates, inside the copy, works: the
// happy path every refusal below is measured against.
func TestRun_queryOverCopyViewsSucceeds(t *testing.T) {
	f := newCopyFixture(t)
	r := newTestRunner(t, testLimits())
	res, err := r.Run(context.Background(), f.job("SELECT id, status FROM shop.orders ORDER BY id"))
	if err != nil {
		t.Fatalf("Run: %v\n--- views ---\n%s", err, f.views)
	}
	wantCols := []Column{{Name: "id", Type: "INTEGER"}, {Name: "status", Type: "VARCHAR"}}
	if !reflect.DeepEqual(res.Columns, wantCols) {
		t.Errorf("columns = %+v, want %+v", res.Columns, wantCols)
	}
	// The fixture baseline carries no chain anchor, so the state view is the
	// snapshot as written (the same rows the generator's own execute test
	// expects); the events query below is what proves the archive half.
	want := [][]any{{json.Number("1"), "new"}, {json.Number("2"), "paid"}}
	if !reflect.DeepEqual(res.Rows, want) {
		t.Errorf("rows = %#v, want %#v", res.Rows, want)
	}
	if res.Truncated {
		t.Error("two rows under a 1000-row cap reported truncated")
	}
	if res.Elapsed <= 0 {
		t.Errorf("elapsed = %v, want > 0", res.Elapsed)
	}
	// The events view reads the archives under the OTHER allowed directory.
	res, err = r.Run(context.Background(), f.job("SELECT count(*) AS n FROM events"))
	if err != nil {
		t.Fatalf("events view: %v", err)
	}
	if got := res.Rows[0][0]; got != json.Number("1") {
		t.Errorf("events count = %#v, want 1", got)
	}
}

// The worker cannot read outside the copy directories. Each case is a real
// file that exists, so the refusal is the sandbox's and not a missing path.
func TestRun_cannotReadOutsideCopy(t *testing.T) {
	f := newCopyFixture(t)
	outside := writeOutsideParquet(t)
	r := newTestRunner(t, testLimits())
	// The traversal case builds "<archive root>/../<sibling>/outside.parquet"
	// without cleaning it, so DuckDB sees the ".." itself; t.TempDir gives
	// siblings under one parent, and the built path is asserted to exist.
	traversal := f.archiveRoot + "/../" + filepath.Base(filepath.Dir(outside)) + "/outside.parquet"
	if _, err := os.Stat(traversal); err != nil {
		t.Fatalf("traversal path does not reach the outside file: %v", err)
	}
	cases := map[string]struct{ sql, want string }{
		"read_csv /etc/passwd":  {"SELECT * FROM read_csv('/etc/passwd')", "Permission Error"},
		"read_text /etc/passwd": {"SELECT * FROM read_text('/etc/passwd')", "Permission Error"},
		"read_blob /etc/passwd": {"SELECT * FROM read_blob('/etc/passwd')", "Permission Error"},
		"read_parquet outside":  {"SELECT * FROM read_parquet('" + outside + "')", "Permission Error"},
		"glob outside":          {"SELECT * FROM glob('/etc/*')", "Permission Error"},
		"traversal from inside": {"SELECT * FROM read_parquet('" + traversal + "')", "Permission Error"},
		"http":                  {"SELECT * FROM read_parquet('https://example.com/x.parquet')", "Permission Error"},
		"s3":                    {"SELECT * FROM read_parquet('s3://bucket/x.parquet')", "Permission Error"},
		// A quoted path as a table is a replacement scan, and those are off
		// with external access off: the path is just an unknown table name.
		"bare path": {"SELECT * FROM '/etc/passwd'", "does not exist"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := r.Run(context.Background(), f.job(c.sql))
			var qe *QueryError
			if !errors.As(err, &qe) {
				t.Fatalf("%s: err = %v, want a QueryError from the sandboxed session", c.sql, err)
			}
			if !strings.Contains(qe.Message, c.want) {
				t.Errorf("%s: %q, want %q", c.sql, qe.Message, c.want)
			}
		})
	}
	// The parent is unharmed: the next query runs.
	if _, err := r.Run(context.Background(), f.job("SELECT 1")); err != nil {
		t.Fatalf("query after the refusals: %v", err)
	}
}

// Anything that is not a single SELECT is refused BEFORE it runs. This is the
// only belt against writes: allowed_directories admits writes into the copy
// (verified on DuckDB v1.4.5, COPY ... TO a path under it succeeds), so the
// statement-shape check is what keeps the copy read-only. The COPY cases
// assert on the disk, not only on the error.
func TestRun_nonSelectStatementsRefused(t *testing.T) {
	f := newCopyFixture(t)
	r := newTestRunner(t, testLimits())
	before := listFiles(t, f.archiveRoot)
	cases := map[string]string{
		"COPY TO inside copy": "COPY (SELECT 1) TO '" + filepath.Join(f.archiveRoot, "x.csv") + "'",
		"COPY TO outside":     "COPY (SELECT 1) TO '" + filepath.Join(t.TempDir(), "x.csv") + "'",
		"INSTALL":             "INSTALL httpfs",
		"LOAD":                "LOAD httpfs",
		"ATTACH file":         "ATTACH '" + filepath.Join(t.TempDir(), "x.duckdb") + "'",
		"ATTACH memory":       "ATTACH ':memory:' AS m2",
		"SET threads":         "SET threads = 64",
		"SET memory":          "SET memory_limit = '100GB'",
		"SET external access": "SET enable_external_access = true",
		"SET lock off":        "SET lock_configuration = false",
		"SET allowed dirs":    "SET allowed_directories = ['/']",
		"RESET":               "RESET enable_external_access",
		"PRAGMA config":       "PRAGMA threads = 64",
		"CREATE TABLE":        "CREATE TABLE t AS SELECT 1",
		"CREATE VIEW":         "CREATE VIEW q AS SELECT 1",
		"INSERT":              "INSERT INTO events VALUES (1)",
		"DELETE":              "DELETE FROM events",
		"EXPORT DATABASE":     "EXPORT DATABASE '" + t.TempDir() + "'",
		"SET VARIABLE":        "SET VARIABLE x = 1",
		"CALL":                "CALL pragma_version()",
		"BEGIN":               "BEGIN",
		"EXPLAIN":             "EXPLAIN SELECT 1",
		"top-level PIVOT":     "PIVOT shop.orders ON status",
		"SELECT then SET":     "SELECT 1; SET threads = 64",
		"SET then SELECT":     "SET threads = 64; SELECT 1",
		"two SELECTs":         "SELECT 1; SELECT 2",
		"two SELECTs newline": "SELECT 1;\nSELECT 2;",
		"empty":               "",
		"whitespace":          "   \n\t",
		"comment only":        "-- nothing here",
		"block comment only":  "/* nothing */",
		"syntax error":        "SELECT nonsense syntax here",
	}
	for name, q := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := r.Run(context.Background(), f.job(q))
			var re *RefusedError
			if !errors.As(err, &re) {
				t.Fatalf("%q: err = %v, want a RefusedError", q, err)
			}
			if re.Reason == "" {
				t.Errorf("%q: refusal carries no reason", q)
			}
		})
	}
	if after := listFiles(t, f.archiveRoot); !reflect.DeepEqual(before, after) {
		t.Errorf("the copy directory changed:\nbefore %v\nafter  %v", before, after)
	}
	if _, err := os.Stat(filepath.Join(f.archiveRoot, "x.csv")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("COPY ... TO wrote into the copy: stat err = %v", err)
	}
	if _, err := r.Run(context.Background(), f.job("SELECT 1")); err != nil {
		t.Fatalf("query after the refusals: %v", err)
	}
}

// The refusal reasons are specific enough for a person to act on.
func TestRun_refusalReasons(t *testing.T) {
	f := newCopyFixture(t)
	r := newTestRunner(t, testLimits())
	cases := []struct{ sql, want string }{
		{"", "empty"},
		{"-- x", "empty"},
		{"SELECT 1; SELECT 2", "one statement"},
		{"COPY (SELECT 1) TO '/tmp/x'", "SELECT"},
		{"SELECT nonsense syntax here", "syntax error"},
	}
	for _, c := range cases {
		_, err := r.Run(context.Background(), f.job(c.sql))
		var re *RefusedError
		if !errors.As(err, &re) {
			t.Fatalf("%q: err = %v, want RefusedError", c.sql, err)
		}
		if !strings.Contains(strings.ToLower(re.Reason), strings.ToLower(c.want)) {
			t.Errorf("%q: reason %q does not mention %q", c.sql, re.Reason, c.want)
		}
	}
}

// Empty input never spawns a process.
func TestRun_emptySQLRefusedWithoutSpawning(t *testing.T) {
	f := newCopyFixture(t)
	r := New(Config{Exe: "/nonexistent/worker", Args: []string{}, Limits: testLimits()})
	for _, q := range []string{"", "  \n"} {
		_, err := r.Run(context.Background(), f.job(q))
		var re *RefusedError
		if !errors.As(err, &re) {
			t.Errorf("%q: err = %v, want RefusedError without a spawn", q, err)
		}
	}
}

// Statement shapes DuckDB parses as a SELECT are allowed: DESCRIBE, SHOW,
// SUMMARIZE, FROM-first, VALUES and a CTE.
func TestRun_selectShapedStatementsAllowed(t *testing.T) {
	f := newCopyFixture(t)
	r := newTestRunner(t, testLimits())
	for _, q := range []string{
		"DESCRIBE shop.orders",
		"SHOW TABLES",
		"SUMMARIZE shop.orders",
		"FROM shop.orders",
		"VALUES (1), (2)",
		"WITH x AS (SELECT 1 AS a) SELECT a FROM x",
		"SELECT 1 UNION ALL SELECT 2",
		"SELECT 1 -- ; SELECT 2",
		"SELECT ';' AS semi",
		"SELECT 1;",
	} {
		if _, err := r.Run(context.Background(), f.job(q)); err != nil {
			t.Errorf("%q: %v", q, err)
		}
	}
}

// The row cap: cap+1 rows produce exactly cap rows and truncated=true; cap
// rows are not truncated.
func TestRun_rowCap(t *testing.T) {
	f := newCopyFixture(t)
	l := testLimits()
	l.MaxRows = 5
	r := newTestRunner(t, l)
	res, err := r.Run(context.Background(), f.job("SELECT range AS i FROM range(6) ORDER BY i"))
	if err != nil {
		t.Fatal(err)
	}
	if !res.Truncated || len(res.Rows) != 5 {
		t.Errorf("6 rows under cap 5: truncated=%v rows=%d, want true/5", res.Truncated, len(res.Rows))
	}
	if got := res.Rows[4][0]; got != json.Number("4") {
		t.Errorf("last kept row = %#v, want the 5th in order", got)
	}
	res, err = r.Run(context.Background(), f.job("SELECT range AS i FROM range(5)"))
	if err != nil {
		t.Fatal(err)
	}
	if res.Truncated || len(res.Rows) != 5 {
		t.Errorf("5 rows under cap 5: truncated=%v rows=%d, want false/5", res.Truncated, len(res.Rows))
	}
	// A per-job cap overrides the runner's.
	job := f.job("SELECT range AS i FROM range(3)")
	job.Limits.MaxRows = 2
	res, err = r.Run(context.Background(), job)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Truncated || len(res.Rows) != 2 {
		t.Errorf("per-job cap 2: truncated=%v rows=%d", res.Truncated, len(res.Rows))
	}
}

// Timeout: the worker is killed, the parent gets a TimeoutError within about
// a second of the cap, the child is gone, and the NEXT query works. The last
// point is the package-level form of "killing it leaves capture running":
// the parent process, which in production is also the capture plane, keeps
// serving.
func TestRun_timeoutKillsWorkerAndParentKeepsServing(t *testing.T) {
	f := newCopyFixture(t)
	l := testLimits()
	l.Timeout = 1 * time.Second
	r := newTestRunner(t, l)
	start := time.Now()
	_, err := r.Run(context.Background(), f.job("SELECT count(*) FROM range(100000000) a, range(100000000) b"))
	took := time.Since(start)
	var te *TimeoutError
	if !errors.As(err, &te) {
		t.Fatalf("err = %v, want TimeoutError", err)
	}
	// A real SIGKILL of the group returns in milliseconds. The bound sits
	// BELOW the child's own deadline (Timeout + selfDeadlineGrace), so a
	// parent that failed to kill and merely waited for the orphan to exit on
	// its own cannot pass here.
	if bound := l.Timeout + 1500*time.Millisecond; took > bound {
		t.Errorf("timeout took %v to come back, want under %v (the parent must kill, not wait for the child's own deadline)", took, bound)
	}
	if te.Limit != l.Timeout {
		t.Errorf("TimeoutError.Limit = %v, want %v", te.Limit, l.Timeout)
	}
	if te.PID == 0 {
		t.Error("TimeoutError carries no pid")
	}
	// The follow-up query gets a normal deadline: the point is that the
	// parent still works, not that a slow (race-instrumented) child starts
	// within the tight cap above.
	after := f.job("SELECT count(*) AS n FROM shop.orders")
	after.Limits.Timeout = 30 * time.Second
	res, err := r.Run(context.Background(), after)
	if err != nil {
		t.Fatalf("query after a timeout: %v", err)
	}
	if res.Rows[0][0] != json.Number("2") {
		t.Errorf("after the timeout the parent returned %#v, want 2", res.Rows[0][0])
	}
}

// Cancelling the caller's context kills the worker the same way.
func TestRun_cancelKillsWorker(t *testing.T) {
	f := newCopyFixture(t)
	r := newTestRunner(t, testLimits())
	started := make(chan int, 1)
	r.onStart = func(pid int) { started <- pid }
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := r.Run(ctx, f.job("SELECT count(*) FROM range(100000000) a, range(100000000) b"))
		done <- err
	}()
	<-started
	time.Sleep(300 * time.Millisecond)
	cancel()
	// Under exec's 5s WaitDelay, so a kill that did not happen (the pipes
	// closing late) cannot pass as a fast return.
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return within 2s of cancel")
	}
}

// The memory cap fails INSIDE the worker: DuckDB reports out of memory, the
// parent gets a QueryError, and the next query works. Spilling is off in the
// worker (temp_directory is empty), so the cap is a cap and not a slowdown.
func TestRun_memoryCapFailsInsideWorker(t *testing.T) {
	f := newCopyFixture(t)
	l := testLimits()
	l.MemoryLimit = "16MB"
	r := newTestRunner(t, l)
	_, err := r.Run(context.Background(), f.job("SELECT count(*) FROM (SELECT DISTINCT i FROM range(30000000) t(i))"))
	var qe *QueryError
	if !errors.As(err, &qe) {
		t.Fatalf("err = %v, want QueryError", err)
	}
	if !strings.Contains(qe.Message, "Out of Memory") {
		t.Errorf("message = %q, want DuckDB's Out of Memory error", qe.Message)
	}
	if _, err := r.Run(context.Background(), f.job("SELECT 1")); err != nil {
		t.Fatalf("query after the OOM: %v", err)
	}
}

// What the user's SQL sees of the session: the caps applied, external access
// off, spilling off, extension autoload off, and the configuration locked.
func TestRun_sessionIsLockedAsSeenByTheQuery(t *testing.T) {
	f := newCopyFixture(t)
	l := testLimits()
	// Not 2: that is DuckDB's own default on a two-core runner, where the
	// assertion would pass without the SET.
	l.Threads = 3
	r := newTestRunner(t, l)
	res, err := r.Run(context.Background(), f.job(`SELECT
		current_setting('threads')::VARCHAR,
		current_setting('enable_external_access')::VARCHAR,
		current_setting('lock_configuration')::VARCHAR,
		current_setting('temp_directory')::VARCHAR,
		current_setting('autoload_known_extensions')::VARCHAR,
		current_setting('autoinstall_known_extensions')::VARCHAR,
		current_setting('allowed_directories')::VARCHAR`))
	if err != nil {
		t.Fatal(err)
	}
	row := res.Rows[0]
	want := []any{"3", "false", "true", "", "false", "false"}
	for i, w := range want {
		if row[i] != w {
			t.Errorf("setting %d = %#v, want %#v", i, row[i], w)
		}
	}
	allowed, _ := row[6].(string)
	for _, dir := range []string{f.archiveRoot, f.baselineRoot} {
		if !strings.Contains(allowed, dir) {
			t.Errorf("allowed_directories %q lacks %q", allowed, dir)
		}
	}
	if strings.Contains(allowed, os.TempDir()+"]") || strings.Count(allowed, "/") == 0 {
		t.Errorf("allowed_directories %q", allowed)
	}
}

// A copy that lives only on S3 is refused up front, typed, without a spawn.
func TestRun_s3OnlyCopyRefused(t *testing.T) {
	f := newCopyFixture(t)
	r := New(Config{Exe: "/nonexistent/worker", Args: []string{}, Limits: testLimits()})
	for _, dirs := range [][]string{nil, {}, {"s3://bucket/prefix"}, {f.archiveRoot, "s3://bucket/prefix"}} {
		job := f.job("SELECT 1")
		job.CopyDirs = dirs
		if _, err := r.Run(context.Background(), job); !errors.Is(err, ErrCopyNotLocal) {
			t.Errorf("CopyDirs %v: err = %v, want ErrCopyNotLocal", dirs, err)
		}
	}
	// A relative directory is not a copy location either.
	job := f.job("SELECT 1")
	job.CopyDirs = []string{"relative/dir"}
	if _, err := r.Run(context.Background(), job); !errors.Is(err, ErrCopyNotLocal) {
		t.Errorf("relative dir: err = %v, want ErrCopyNotLocal", err)
	}
}

// One query at a time per user: a second query from the same user while one
// runs is refused at once with ErrBusy; another user is not affected; and
// the slot frees when the first query ends.
func TestRun_oneQueryAtATimePerUser(t *testing.T) {
	f := newCopyFixture(t)
	r := newTestRunner(t, testLimits())
	started := make(chan int, 1)
	r.onStart = func(pid int) {
		select {
		case started <- pid:
		default:
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	slow := f.job("SELECT count(*) FROM range(100000000) a, range(100000000) b")
	slow.User = "alice"
	done := make(chan error, 1)
	go func() {
		_, err := r.Run(ctx, slow)
		done <- err
	}()
	<-started
	quick := f.job("SELECT 1")
	quick.User = "alice"
	if _, err := r.Run(context.Background(), quick); !errors.Is(err, ErrBusy) {
		t.Errorf("second query for alice: err = %v, want ErrBusy", err)
	}
	quick.User = "bob"
	if _, err := r.Run(context.Background(), quick); err != nil {
		t.Errorf("bob while alice runs: %v", err)
	}
	cancel()
	<-done
	quick.User = "alice"
	if _, err := r.Run(context.Background(), quick); err != nil {
		t.Errorf("alice after her query ended: %v", err)
	}
	// No user key: no gate.
	quick.User = ""
	if _, err := r.Run(context.Background(), quick); err != nil {
		t.Errorf("ungated: %v", err)
	}
}

// Cells come back as JSON scalars a browser can show: numbers keep every
// digit (json.Number), dates and timestamps are ISO text, wide integers and
// decimals are text, NaN is text, nested values stay nested.
func TestRun_cellsAreJSONScalars(t *testing.T) {
	f := newCopyFixture(t)
	r := newTestRunner(t, testLimits())
	res, err := r.Run(context.Background(), f.job(`SELECT
		1::HUGEINT AS h,
		1.50::DECIMAL(10,2) AS d,
		DATE '2026-01-01' AS dt,
		TIMESTAMP '2026-01-01 10:00:00' AS ts,
		'x'::BLOB AS b,
		'\xff'::BLOB AS bin,
		[1, 2] AS l,
		{'a': 1} AS s,
		NULL AS n,
		'nan'::DOUBLE AS f,
		9223372036854775807::BIGINT AS big,
		'é'::VARCHAR AS v,
		true AS t`))
	if err != nil {
		t.Fatal(err)
	}
	row := res.Rows[0]
	want := []any{
		"1", "1.5", "2026-01-01", "2026-01-01T10:00:00Z", "x", "0xFF",
		[]any{json.Number("1"), json.Number("2")}, map[string]any{"a": json.Number("1")},
		nil, "NaN", json.Number("9223372036854775807"), "é", true,
	}
	for i, w := range want {
		if !reflect.DeepEqual(row[i], w) {
			t.Errorf("%s = %#v (%T), want %#v", res.Columns[i].Name, row[i], row[i], w)
		}
	}
	if res.Columns[1].Type != "DECIMAL(10,2)" || res.Columns[3].Type != "TIMESTAMP" {
		t.Errorf("column types = %+v", res.Columns)
	}
}

// A worker that dies without a result (here: it cannot even start) is a
// WorkerError, never a silent empty result.
func TestRun_workerThatCannotStartIsAnError(t *testing.T) {
	f := newCopyFixture(t)
	r := New(Config{Exe: "/nonexistent/worker", Args: []string{}, Limits: testLimits()})
	_, err := r.Run(context.Background(), f.job("SELECT 1"))
	var we *WorkerError
	if !errors.As(err, &we) {
		t.Fatalf("err = %v, want WorkerError", err)
	}
}

// DefaultLimits are the ones the issue asks for.
func TestDefaultLimits(t *testing.T) {
	l := DefaultLimits()
	if l.Threads != 2 || l.MemoryLimit != "2GB" || l.Timeout != 60*time.Second || l.MaxRows != 1000 {
		t.Errorf("DefaultLimits = %+v", l)
	}
}

// The result byte cap: a result past it is ErrResultTooLarge, the child is
// gone, and the next query works.
func TestRun_resultTooLargeIsTypedAndParentKeepsServing(t *testing.T) {
	f := newCopyFixture(t)
	l := testLimits()
	l.MaxResultBytes = 4096
	r := newTestRunner(t, l)
	// The worker counts too, so with this small a cap it reports the
	// overflow itself; the parent-side kill is exercised in-process by
	// TestCappedBuffer_overflowKillsAndErrors.
	_, err := r.Run(context.Background(), f.job("SELECT repeat('x', 1000) AS s FROM range(100)"))
	if !errors.Is(err, ErrResultTooLarge) {
		t.Fatalf("err = %v, want ErrResultTooLarge", err)
	}
	if _, err := r.Run(context.Background(), f.job("SELECT 1")); err != nil {
		t.Fatalf("query after the overflow: %v", err)
	}
}

// A copy directory that is itself a symbolic link (a linked backup directory
// is an ordinary setup) admits its RESOLVED directory too: the link is the
// only CopyDir, and the views name the files by their resolved path, which
// DuckDB compares as text. Without the resolved entry this is a Permission
// Error.
func TestRun_copyDirThatIsASymlinkAdmitsTheResolvedDir(t *testing.T) {
	realRoot := t.TempDir()
	writeFixtureBaseline(t, realRoot)
	resolved, err := filepath.EvalSymlinks(realRoot)
	if err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(t.TempDir(), "backups")
	if err := os.Symlink(realRoot, link); err != nil {
		t.Skipf("symlink: %v", err)
	}
	viewsSQL := views.Generate(views.Input{
		GeneratedAt:      time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC),
		Version:          "test",
		BaselineSource:   resolved,
		BaselineSnapshot: time.Date(2026, 4, 30, 3, 0, 0, 0, time.UTC),
		Baselines:        []views.BaselineTable{{Schema: "shop", Table: "orders", Path: filepath.Join(resolved, "2026-04-30T03-00-00Z", "shop", "orders.parquet")}},
		OmitEvents:       true,
	})
	r := newTestRunner(t, testLimits())
	res, err := r.Run(context.Background(), Job{CopyDirs: []string{link}, ViewsSQL: viewsSQL, SQL: "SELECT count(*) AS n FROM shop.orders"})
	if err != nil {
		t.Fatalf("through the link with resolved paths: %v", err)
	}
	if res.Rows[0][0] != json.Number("2") {
		t.Errorf("rows = %#v", res.Rows)
	}
	if got := allowedDirs([]string{link}); len(got) != 2 || got[0] != link || got[1] != resolved {
		t.Errorf("allowedDirs(link) = %v, want [link, resolved]", got)
	}
}

// Canary for the documented limit: a symbolic link planted INSIDE the copy
// that points outside is readable through it, because DuckDB compares the
// path text. This test pins the current engine's behavior so a DuckDB bump
// that starts resolving links fails loudly here, and the package comment
// and the PR record get updated instead of silently going stale.
func TestRun_symlinkInsideCopyIsReadable_knownLimit(t *testing.T) {
	f := newCopyFixture(t)
	outsideDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(outsideDir, "secret.txt"), []byte("outside"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outsideDir, filepath.Join(f.archiveRoot, "leak")); err != nil {
		t.Skipf("symlink: %v", err)
	}
	r := newTestRunner(t, testLimits())
	res, err := r.Run(context.Background(), f.job("SELECT content FROM read_text('"+filepath.Join(f.archiveRoot, "leak", "secret.txt")+"')"))
	if err != nil {
		t.Fatalf("DuckDB now refuses a symbolic link inside the copy (%v): the limit documented in the package comment and in PR #1955 no longer holds, update both and turn this test into the refusal assertion", err)
	}
	if res.Rows[0][0] != "outside" {
		t.Errorf("rows = %#v", res.Rows)
	}
}

// A context that is already cancelled comes back as the cancel, not as a
// worker that failed to start.
func TestRun_preCancelledContextIsTheCancel(t *testing.T) {
	f := newCopyFixture(t)
	r := newTestRunner(t, testLimits())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := r.Run(ctx, f.job("SELECT 1"))
	var we *WorkerError
	if !errors.Is(err, context.Canceled) || errors.As(err, &we) {
		t.Errorf("err = %v (%T), want context.Canceled and not a WorkerError", err, err)
	}
}

// An orphaned worker stops on its own: started directly (no parent deadline,
// no kill), with a 1s job timeout and a query that never ends, it exits
// with selfDeadlineExit shortly after the timeout plus its grace.
func TestWorker_stopsOnItsOwnWhenNobodyKillsIt(t *testing.T) {
	f := newCopyFixture(t)
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	in, _ := json.Marshal(wireJob{
		CopyDirs: []string{f.archiveRoot}, SQL: "SELECT count(*) FROM range(100000000) a, range(100000000) b",
		Threads: 1, MemoryLimit: DefaultLimits().MemoryLimit, MaxRows: 10, TimeoutNS: int64(time.Second),
	})
	// Bounded from outside: a worker with no self-deadline would run the
	// cross join for hours, and this test must fail, not hang.
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, exe)
	cmd.Env = childEnv()
	cmd.Stdin = bytes.NewReader(in)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	start := time.Now()
	err = cmd.Run()
	took := time.Since(start)
	if ctx.Err() != nil {
		t.Fatalf("the orphaned worker did not stop on its own within 15s")
	}
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != selfDeadlineExit {
		t.Fatalf("err = %v, want exit status %d\n%s", err, selfDeadlineExit, stderr.String())
	}
	if took > time.Second+selfDeadlineGrace+5*time.Second {
		t.Errorf("the orphan took %v to stop", took)
	}
	if !strings.Contains(stderr.String(), "deadline") {
		t.Errorf("stderr = %q, want the self-deadline line", stderr.String())
	}
}

// SELECT-shaped table functions that change state or run hidden text are
// refused by name, at any depth, before anything runs; the copy is
// byte-for-byte unchanged afterwards. enable_logging with a path INSIDE the
// copy is the case that would otherwise write CSV files into it later in
// the session (verified on DuckDB v1.4.5 after the full lock).
func TestRun_stateChangingTableFunctionsRefused(t *testing.T) {
	f := newCopyFixture(t)
	r := newTestRunner(t, testLimits())
	before := dirDigest(t, f.archiveRoot)
	logs := filepath.Join(f.archiveRoot, "logs")
	cases := map[string]struct{ sql, name string }{
		"enable_logging into the copy": {"SELECT * FROM enable_logging(storage='file', storage_config={'path':'" + logs + "'})", "enable_logging"},
		"enable_logging plain":         {"SELECT * FROM enable_logging()", "enable_logging"},
		"hidden in query()":            {"SELECT * FROM query('SELECT * FROM enable_logging(storage=''file'', storage_config={''path'':''" + logs + "''})')", "query"},
		"query_table":                  {"SELECT * FROM query_table('shop.orders')", "query_table"},
		"disable_logging":              {"SELECT * FROM disable_logging()", "disable_logging"},
		"checkpoint":                   {"SELECT * FROM checkpoint()", "checkpoint"},
		"force_checkpoint":             {"SELECT * FROM force_checkpoint()", "force_checkpoint"},
		"truncate_duckdb_logs":         {"SELECT * FROM truncate_duckdb_logs()", "truncate_duckdb_logs"},
		"json_execute_serialized_sql":  {"SELECT * FROM json_execute_serialized_sql('{}')", "json_execute_serialized_sql"},
		"in a scalar subquery":         {"SELECT (SELECT count(*) FROM checkpoint()) AS c", "checkpoint"},
		"in a CTE":                     {"WITH x AS (SELECT * FROM force_checkpoint()) SELECT * FROM x", "force_checkpoint"},
		"in a set operation":           {"SELECT 1 UNION ALL SELECT * FROM truncate_duckdb_logs()", "truncate_duckdb_logs"},
		"in a LATERAL":                 {"SELECT * FROM range(3) t, LATERAL (SELECT * FROM checkpoint()) l", "checkpoint"},
		"in WHERE EXISTS":              {"SELECT * FROM shop.orders WHERE EXISTS (SELECT 1 FROM disable_logging())", "disable_logging"},
		"in LIMIT":                     {"SELECT * FROM shop.orders LIMIT (SELECT count(*) FROM checkpoint())", "checkpoint"},
		"in a JOIN":                    {"SELECT * FROM shop.orders JOIN checkpoint() ON true", "checkpoint"},
		"mixed case":                   {"SELECT * FROM Enable_Logging()", "enable_logging"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := r.Run(context.Background(), f.job(c.sql))
			var re *RefusedError
			if !errors.As(err, &re) {
				t.Fatalf("%s: err = %v, want RefusedError", c.sql, err)
			}
			if !strings.Contains(re.Reason, c.name) {
				t.Errorf("%s: reason %q does not name %s", c.sql, re.Reason, c.name)
			}
		})
	}
	if after := dirDigest(t, f.archiveRoot); after != before {
		t.Errorf("the copy directory changed")
	}
	if _, err := os.Stat(logs); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("a logs directory appeared inside the copy: stat err = %v", err)
	}
	// The readers stay allowed, including inside the copy and at depth.
	for _, q := range []string{
		"SELECT count(*) FROM read_parquet('" + filepath.Join(f.archiveRoot, "**", "*.parquet") + "')",
		"SELECT count(*) FROM glob('" + filepath.Join(f.archiveRoot, "**") + "')",
		"SELECT count(*) FROM parquet_metadata('" + filepath.Join(f.archiveRoot, "**", "*.parquet") + "')",
		"WITH r AS (SELECT * FROM range(3)) SELECT (SELECT count(*) FROM generate_series(1, 2)) FROM r, LATERAL (SELECT unnest([1])) u",
		"SELECT count(*) FROM duckdb_views()",
		"SELECT * FROM pragma_version()",
		"SELECT * FROM shop.orders PIVOT (count(*) FOR status IN ('new', 'paid'))",
	} {
		if _, err := r.Run(context.Background(), f.job(q)); err != nil {
			t.Errorf("%s: %v", q, err)
		}
	}
}

// A session that cannot be prepared (here: a memory limit DuckDB cannot
// parse) is a WorkerError, never an uncapped run.
func TestRun_unpreparableSessionIsAWorkerError(t *testing.T) {
	f := newCopyFixture(t)
	l := testLimits()
	l.MemoryLimit = "banana"
	r := newTestRunner(t, l)
	_, err := r.Run(context.Background(), f.job("SELECT 1"))
	var we *WorkerError
	if !errors.As(err, &we) {
		t.Fatalf("err = %v, want WorkerError", err)
	}
	if !strings.Contains(we.Error(), "memory_limit") {
		t.Errorf("error does not name the failed cap: %v", we)
	}
}

// A query with no rows comes back as an empty, non-nil row list.
func TestRun_zeroRowsIsAnEmptyList(t *testing.T) {
	f := newCopyFixture(t)
	r := newTestRunner(t, testLimits())
	res, err := r.Run(context.Background(), f.job("SELECT 1 AS a WHERE false"))
	if err != nil {
		t.Fatal(err)
	}
	if res.Rows == nil || len(res.Rows) != 0 || len(res.Columns) != 1 {
		t.Errorf("rows = %#v cols = %+v", res.Rows, res.Columns)
	}
}

// The global cap: with MaxInFlight 1, a second user's query while one runs
// is ErrBusy, and works once the first ends.
func TestRun_globalInFlightCap(t *testing.T) {
	f := newCopyFixture(t)
	exe, _ := os.Executable()
	r := New(Config{Exe: exe, Args: []string{}, Limits: testLimits(), MaxInFlight: 1})
	started := make(chan int, 1)
	r.onStart = func(pid int) {
		select {
		case started <- pid:
		default:
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	slow := f.job("SELECT count(*) FROM range(100000000) a, range(100000000) b")
	slow.User = "alice"
	done := make(chan error, 1)
	go func() {
		_, err := r.Run(ctx, slow)
		done <- err
	}()
	<-started
	quick := f.job("SELECT 1")
	quick.User = "bob"
	if _, err := r.Run(context.Background(), quick); !errors.Is(err, ErrBusy) {
		t.Errorf("bob at the global cap: err = %v, want ErrBusy", err)
	}
	quick.User = ""
	if _, err := r.Run(context.Background(), quick); !errors.Is(err, ErrBusy) {
		t.Errorf("ungated query at the global cap: err = %v, want ErrBusy", err)
	}
	cancel()
	<-done
	quick.User = "bob"
	if _, err := r.Run(context.Background(), quick); err != nil {
		t.Errorf("bob after the slot freed: %v", err)
	}
}

// A cell longer than MaxCellBytes is cut with the marker and counted; the
// rest of the row is intact. In-process through runJob (the worker's own
// code path), which keeps the 2 MiB string out of the pipe.
func TestWorker_cutsOversizedCells(t *testing.T) {
	f := newCopyFixture(t)
	res := runJob(wireJob{
		CopyDirs: []string{f.archiveRoot}, SQL: "SELECT repeat('x', 2 * 1024 * 1024) AS big, 'small' AS s",
		Threads: 1, MemoryLimit: "256MB", MaxRows: 10, MaxResultBytes: 8 << 20,
	}, os.Stderr)
	if res.Error != nil {
		t.Fatalf("error: %+v", res.Error)
	}
	big, _ := res.Rows[0][0].(string)
	if len(big) != MaxCellBytes+len(cellTruncatedMarker) || !strings.HasSuffix(big, cellTruncatedMarker) {
		t.Errorf("big cell: len %d, suffix %q", len(big), big[max(0, len(big)-30):])
	}
	if res.Rows[0][1] != "small" || res.TruncatedCells != 1 {
		t.Errorf("row = %v truncated_cells = %d", res.Rows[0][1], res.TruncatedCells)
	}
}

// The worker counts the result as it collects rows and stops at
// MaxResultBytes on its own side, before building a result the parent would
// refuse; the parent maps that to ErrResultTooLarge.
func TestWorker_stopsAtMaxResultBytesItself(t *testing.T) {
	f := newCopyFixture(t)
	res := runJob(wireJob{
		CopyDirs: []string{f.archiveRoot}, SQL: "SELECT repeat('x', 1000) AS s FROM range(100)",
		Threads: 1, MemoryLimit: "256MB", MaxRows: 1000, MaxResultBytes: 4096,
	}, os.Stderr)
	if res.Error == nil || res.Error.Kind != errTooLarge {
		t.Fatalf("error = %+v, want kind %s", res.Error, errTooLarge)
	}
	if len(res.Rows) != 0 {
		t.Errorf("a refused result still carries %d rows", len(res.Rows))
	}
}

// The parent's own byte cap: past it the writer errors, fires the kill, and
// later writes are refused.
func TestCappedBuffer_overflowKillsAndErrors(t *testing.T) {
	killed := 0
	c := &cappedBuffer{max: 10, onOverflow: func() { killed++ }}
	if n, err := c.Write([]byte("12345")); err != nil || n != 5 {
		t.Fatalf("first write: %d %v", n, err)
	}
	if _, err := c.Write([]byte("678901")); !errors.Is(err, ErrResultTooLarge) || killed != 1 || !c.overflowed {
		t.Errorf("overflow: err=%v killed=%d overflowed=%v", err, killed, c.overflowed)
	}
	if _, err := c.Write([]byte("x")); !errors.Is(err, ErrResultTooLarge) || killed != 1 {
		t.Errorf("after overflow: err=%v killed=%d", err, killed)
	}
	// The dropping variant keeps what fits and never errors.
	d := &cappedBuffer{max: 4, drop: true}
	if n, err := d.Write([]byte("abcdef")); err != nil || n != 6 || d.buf.String() != "abcd" {
		t.Errorf("drop: n=%d err=%v kept=%q", n, err, d.buf.String())
	}
	if n, err := d.Write([]byte("gh")); err != nil || n != 2 || d.buf.String() != "abcd" {
		t.Errorf("drop after cap: n=%d err=%v kept=%q", n, err, d.buf.String())
	}
}

func TestIsWorkerInvocation(t *testing.T) {
	if !IsWorkerInvocation([]string{"bintrail-console", WorkerCommand}) {
		t.Error("worker args not recognized")
	}
	for _, a := range [][]string{nil, {"bintrail-console"}, {"bintrail-console", "serve"}, {"bintrail-console", "serve", WorkerCommand}} {
		if IsWorkerInvocation(a) {
			t.Errorf("%v recognized as a worker", a)
		}
	}
}

// Job.Schema is the MySQL-protocol port's USE: unqualified names resolve in
// that schema, events (in main) stays reachable, and a schema the views did
// not create is the user's own error, naming the schema, not a worker
// failure. The odd names prove the literal quoting: an unbalanced quote
// would surface as a parser error, not as the catalog error asserted here.
func TestRun_schemaResolvesUnqualifiedNames(t *testing.T) {
	f := newCopyFixture(t)
	r := newTestRunner(t, testLimits())
	withSchema := func(schema, sqlText string) Job {
		j := f.job(sqlText)
		j.Schema = schema
		return j
	}
	res, err := r.Run(context.Background(), withSchema("shop", "SELECT id FROM orders ORDER BY id"))
	if err != nil {
		t.Fatalf("unqualified orders under schema shop: %v", err)
	}
	if want := [][]any{{json.Number("1")}, {json.Number("2")}}; !reflect.DeepEqual(res.Rows, want) {
		t.Errorf("rows = %#v, want %#v", res.Rows, want)
	}
	res, err = r.Run(context.Background(), withSchema("shop", "SELECT count(*) AS n FROM events"))
	if err != nil {
		t.Fatalf("events under schema shop: %v", err)
	}
	if got := res.Rows[0][0]; got != json.Number("1") {
		t.Errorf("events count = %#v, want 1", got)
	}
	// Without a schema the default stays main: orders is not there.
	if _, err := r.Run(context.Background(), f.job("SELECT id FROM orders")); err == nil {
		t.Error("unqualified orders with no schema ran; want a QueryError")
	}
	// A schema the copy does not have, odd spellings included: the default
	// stays, so a statement that needs no schema still runs...
	for _, schema := range []string{"nope", `we"ird`, "a,b", "it's"} {
		if _, err := r.Run(context.Background(), withSchema(schema, "SELECT 1 AS one")); err != nil {
			t.Fatalf("schema %q: SELECT 1 = %v, want it to run on the default", schema, err)
		}
		res, err := r.Run(context.Background(), withSchema(schema, "SELECT count(*) AS n FROM events"))
		if err != nil || res.Rows[0][0] != json.Number("1") {
			t.Fatalf("schema %q: events = (%v, %v), want 1 row counted", schema, res.Rows, err)
		}
	}
	// ...and a name that then fails to resolve says why, naming the schema
	// and the way out.
	_, err = r.Run(context.Background(), withSchema("shopp", "SELECT id FROM orders"))
	var qerr *QueryError
	if !errors.As(err, &qerr) {
		t.Fatalf("typo schema: err = %v (%T), want *QueryError", err, err)
	}
	for _, want := range []string{"does not exist", `the current database "shopp" is not in the copy`, "SHOW DATABASES"} {
		if !strings.Contains(qerr.Message, want) {
			t.Errorf("typo schema: message %q lacks %q", qerr.Message, want)
		}
	}
	// A DuckDB error that is not about a missing name carries no hint.
	_, err = r.Run(context.Background(), withSchema("shopp", "SELECT 'x'::INTEGER AS boom"))
	if !errors.As(err, &qerr) {
		t.Fatalf("conversion: err = %v, want a QueryError", err)
	}
	if strings.Contains(qerr.Message, "current database") {
		t.Errorf("conversion error carried the schema hint: %q", qerr.Message)
	}
}

// Reserve takes the slot before any job exists (#2026: the caller prepares
// the job AFTER the slot, so a busy refusal costs no preparation). The slot
// serves one job and is released by Run; Release gives an unused one back;
// the per-user and global gates are the same ones Run applies.
func TestReserve_slotBeforeTheJob(t *testing.T) {
	f := newCopyFixture(t)
	r := New(Config{Exe: testExe(t), Args: []string{}, Limits: testLimits(), MaxInFlight: 1})
	slot, err := r.Reserve("alice")
	if err != nil {
		t.Fatal(err)
	}
	// The global slot is taken: a second reservation, any user, is busy now.
	if _, err := r.Reserve("bob"); !errors.Is(err, ErrBusy) {
		t.Errorf("second Reserve with MaxInFlight 1 = %v, want ErrBusy", err)
	}
	// An unused slot given back frees it.
	slot.Release()
	slot.Release() // idempotent
	slot2, err := r.Reserve("alice")
	if err != nil {
		t.Fatalf("Reserve after Release: %v", err)
	}
	// A job for another user cannot ride this slot.
	wrong := f.job("SELECT 1")
	wrong.User = "bob"
	var werr *WorkerError
	if _, err := slot2.Run(context.Background(), wrong); !errors.As(err, &werr) {
		t.Fatalf("job for another user in the slot: err = %v, want WorkerError", err)
	}
	// Run released it even on that refusal.
	slot3, err := r.Reserve("alice")
	if err != nil {
		t.Fatalf("Reserve after a refused Run: %v", err)
	}
	job := f.job("SELECT id FROM shop.orders ORDER BY id")
	job.User = "alice"
	res, err := slot3.Run(context.Background(), job)
	if err != nil {
		t.Fatalf("Slot.Run: %v", err)
	}
	if len(res.Rows) != 2 {
		t.Errorf("rows = %d, want 2", len(res.Rows))
	}
	// Phases: the child reports its own, the parent its spawn and total.
	ph := res.Phases
	if ph.Open <= 0 || ph.Lockdown <= 0 || ph.Views <= 0 || ph.Query <= 0 || ph.Spawn <= 0 || ph.Total <= 0 {
		t.Errorf("phases not all measured: %+v", ph)
	}
	if ph.Total < ph.Spawn || ph.Spawn < ph.Open+ph.Lockdown+ph.Views+ph.Query {
		t.Errorf("phases do not nest (total >= spawn >= child phases): %+v", ph)
	}
	if ph.Query != res.Elapsed {
		t.Errorf("Phases.Query = %v, Elapsed = %v, want equal", ph.Query, res.Elapsed)
	}
	// Released after the run: the slot is free again.
	if s, err := r.Reserve("alice"); err != nil {
		t.Errorf("Reserve after a completed Run: %v", err)
	} else {
		s.Release()
	}
}

func testExe(t *testing.T) string {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	return exe
}
