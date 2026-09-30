package console

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/dbtrail/dbtrail/ext"
	"github.com/dbtrail/dbtrail/internal/archive"
	"github.com/dbtrail/dbtrail/internal/audittest"
	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
	"github.com/dbtrail/dbtrail/internal/views"
)

// fakeSQLRunner stands in for sqlsandbox.Runner: it records the job the
// handler built and answers with a canned result or error.
type fakeSQLRunner struct {
	mu   sync.Mutex
	jobs []sqlsandbox.Job
	res  sqlsandbox.Result
	err  error
}

func (f *fakeSQLRunner) Run(_ context.Context, job sqlsandbox.Job) (sqlsandbox.Result, error) {
	f.mu.Lock()
	f.jobs = append(f.jobs, job)
	f.mu.Unlock()
	return f.res, f.err
}

func (f *fakeSQLRunner) last(t *testing.T) sqlsandbox.Job {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.jobs) == 0 {
		t.Fatal("the runner was never called")
	}
	return f.jobs[len(f.jobs)-1]
}

func (f *fakeSQLRunner) calls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.jobs)
}

// sqlSnapshotAt is the fixture snapshot's time, what copy_updated_at reports.
var sqlSnapshotAt = time.Date(2026, 4, 30, 3, 0, 0, 0, time.UTC)

const (
	sqlSnapshotDirName = "2026-04-30T03-00-00Z"
	sqlArchiveID       = "11111111-2222-3333-4444-555555555555"
)

// writeSQLBaselineFixture writes one real baseline Parquet under dir, in the
// layout the daemon writes, and returns the table's schema directory (the
// directory the sandbox must be handed).
func writeSQLBaselineFixture(t *testing.T, dir string) string {
	t.Helper()
	schemaDir := filepath.Join(dir, sqlSnapshotDirName, "shop")
	path := filepath.Join(schemaDir, "orders.parquet")
	schemaFile := filepath.Join(t.TempDir(), "shop.orders-schema.sql")
	ddl := "CREATE TABLE `orders` (\n  `id` int NOT NULL,\n  `status` varchar(32) DEFAULT NULL,\n  PRIMARY KEY (`id`)\n);\n"
	if err := os.WriteFile(schemaFile, []byte(ddl), 0o644); err != nil {
		t.Fatal(err)
	}
	cols, err := baseline.ParseSchema(schemaFile)
	if err != nil {
		t.Fatal(err)
	}
	w, err := baseline.NewWriter(path, cols, baseline.WriterConfig{Compression: "none", RowGroupSize: 100})
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range [][]string{{"1", "new"}, {"2", "paid"}} {
		if err := w.WriteRow(r, []bool{false, false}); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return schemaDir
}

// writeSQLArchiveFixture writes one archived partition under root in the
// Hive layout rotation produces, through the real archive column set, and
// returns the archive base (the bintrail_id=<id> directory) and the file.
func writeSQLArchiveFixture(t *testing.T, root string) (base, file string) {
	t.Helper()
	base = filepath.Join(root, "bintrail_id="+sqlArchiveID)
	file = filepath.Join(base, "event_date=2026-05-01", "event_hour=03", "events.parquet")
	w, err := baseline.NewWriter(file, archive.BinlogEventColumns, baseline.WriterConfig{Compression: "none", RowGroupSize: 100})
	if err != nil {
		t.Fatal(err)
	}
	values := []string{
		"1", "binlog.000001", "100", "200", "2026-05-01 03:00:00", "",
		"42", "shop", "orders", "2", "1",
		`["status"]`, `{"id":1,"status":"new"}`, `{"id":1,"status":"paid"}`,
		"1", "", "", "1777000000000000",
	}
	nulls := make([]bool, len(archive.BinlogEventColumns))
	nulls[5], nulls[15], nulls[16] = true, true, true
	if len(values) != len(archive.BinlogEventColumns) {
		t.Fatalf("fixture has %d values for %d columns", len(values), len(archive.BinlogEventColumns))
	}
	if err := w.WriteRow(values, nulls); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return base, file
}

// sqlFixture is a boot-only server whose copy is a local snapshot
// directory with one table, and, when withArchive is set, an index database
// (sqlmock) whose archive_state names one local archived partition, plus a
// reachable DSN so the views' live leg is OFFERABLE (and its absence in the
// job therefore a real assertion).
type sqlFixture struct {
	s          *Server
	runner     *fakeSQLRunner
	root       string // the snapshot root: what bundle.baselineSrc is
	schemaDir  string // the pinned table's directory: what the sandbox gets
	archiveDir string // the archive base, or "" without an archive
	mock       sqlmock.Sqlmock
}

func newSQLFixture(t *testing.T, runner sqlRunner, withArchive bool) *sqlFixture {
	t.Helper()
	f := &sqlFixture{root: t.TempDir()}
	if r, ok := runner.(*fakeSQLRunner); ok {
		f.runner = r
	}
	f.schemaDir = writeSQLBaselineFixture(t, f.root)
	f.s = &Server{token: "t", cm: newConnManager(nil, false), sessionProfiles: newProfileRuleCache()}
	f.s.cm.boot = &bundle{baselineSrc: f.root, baselineConfigured: true, activity: newActivityCache()}
	if withArchive {
		db, mock, closeDB := newSQLMock(t)
		t.Cleanup(closeDB)
		f.mock = mock
		f.archiveDir, _ = writeSQLArchiveFixture(t, t.TempDir())
		f.s.cm.boot.db = db
		f.s.cm.boot.dsn = "u:p@tcp(127.0.0.1:3306)/bintrail_index"
	}
	f.s.sqlRunner = runner
	f.s.sqlLimits = resolveSQLLimits(sqlsandbox.Limits{})
	f.s.mux = f.s.buildHandler()
	return f
}

// expectArchive queues the archive_state answer one POST consumes (the
// column-set grouping query that follows is left unexpected on purpose: it
// is best-effort in the resolver, and its failure only loses the grouping).
func (f *sqlFixture) expectArchive() {
	if f.mock == nil {
		return
	}
	localFile := filepath.Join(f.archiveDir, "event_date=2026-05-01", "event_hour=03", "events.parquet")
	// Two answers per call, matched on the archive-sources query alone: one
	// request can read it twice (capabilities asks for views and for sql;
	// a change-log-only copy resolves its views twice), and the looser
	// "FROM archive_state" would let the grouping query eat one.
	for range 2 {
		f.mock.ExpectQuery(`MIN\(local_path\)`).WillReturnRows(
			sqlmock.NewRows([]string{"bintrail_id", "sample_local", "sample_bucket", "sample_key"}).
				AddRow(sqlArchiveID, localFile, nil, nil))
	}
}

// expectArchiveS3 queues the archive_state answer for a change log that
// lives in S3 only: a bucket and a key, no local path.
func (f *sqlFixture) expectArchiveS3() {
	for range 2 {
		f.mock.ExpectQuery(`MIN\(local_path\)`).WillReturnRows(
			sqlmock.NewRows([]string{"bintrail_id", "sample_local", "sample_bucket", "sample_key"}).
				AddRow(sqlArchiveID, nil, "e2e-bucket", "arch/bintrail_id="+sqlArchiveID+"/event_date=2026-05-01/event_hour=03/events.parquet"))
	}
}

func (f *sqlFixture) post(t *testing.T, body string, hdr ...string) *httptest.ResponseRecorder {
	t.Helper()
	f.expectArchive()
	return postSQL(t, f.s, body, hdr...)
}

func postSQL(t *testing.T, s *Server, body string, hdr ...string) *httptest.ResponseRecorder {
	t.Helper()
	path := "/api/sql"
	if strings.HasPrefix(body, "?") {
		// A query string riding in front of the body: "?format=csv{...}".
		i := strings.Index(body, "{")
		path, body = path+body[:i], body[i:]
	}
	req := httptest.NewRequest("POST", path, strings.NewReader(body))
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Set(hdr[i], hdr[i+1])
	}
	w := httptest.NewRecorder()
	s.handleSQL(w, req)
	return w
}

// newSQLServer keeps the simplest shape for the audit coverage test.
func newSQLServer(t *testing.T, runner sqlRunner) (*Server, string) {
	t.Helper()
	f := newSQLFixture(t, runner, false)
	return f.s, f.root
}

// sqlCodeOnly drops the comment lines of a generated views file: its header
// names locations in prose (an S3 change log it left out, for one), and
// only what executes matters to the locked worker.
func sqlCodeOnly(text string) string {
	var b strings.Builder
	for _, line := range strings.Split(text, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "--") {
			continue
		}
		b.WriteString(line + "\n")
	}
	return b.String()
}

func oneRowResult() sqlsandbox.Result {
	return sqlsandbox.Result{
		Columns: []sqlsandbox.Column{{Name: "id", Type: "INTEGER"}, {Name: "status", Type: "VARCHAR"}},
		Rows:    [][]any{{json.Number("1"), "new"}},
		Elapsed: 12 * time.Millisecond,
	}
}

// The happy path: the job carries the session identity, EXACTLY the
// directories the views read (the archive base and the pinned table's
// directory, never the snapshot root), views without the live leg, and the
// statement; the response carries the result and the pinned snapshot.
func TestSQLAPI_runsTheStatementOnTheCopy(t *testing.T) {
	f := newSQLFixture(t, &fakeSQLRunner{res: oneRowResult()}, true)
	var seen *views.Input
	f.s.sqlViewsObserver = func(in views.Input) { seen = &in }
	const stmt = "SELECT o.id, o.status FROM state_shop_orders o WHERE o.id IN (SELECT 1 FROM events)"
	w := f.post(t, `{"sql":"`+stmt+`","max_rows":50}`)
	if w.Code != http.StatusOK {
		t.Fatalf("code=%d body=%s", w.Code, w.Body.String())
	}
	var resp sqlResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if len(resp.Columns) != 2 || len(resp.Rows) != 1 || resp.ElapsedMS != 12 || resp.Truncated {
		t.Errorf("response = %+v", resp)
	}
	if resp.CopyUpdatedAt == nil || !resp.CopyUpdatedAt.Equal(sqlSnapshotAt) {
		t.Errorf("copy_updated_at = %v, want %v", resp.CopyUpdatedAt, sqlSnapshotAt)
	}
	job := f.runner.last(t)
	if job.User != tokenActor {
		t.Errorf("job.User = %q, want the request's actor %q", job.User, tokenActor)
	}
	want := []string{f.archiveDir, f.schemaDir}
	if strings.Join(job.CopyDirs, "\n") != strings.Join(want, "\n") {
		t.Errorf("job.CopyDirs = %v, want exactly %v", job.CopyDirs, want)
	}
	for _, d := range job.CopyDirs {
		if d == f.root || d == filepath.Join(f.root, sqlSnapshotDirName) {
			t.Errorf("job.CopyDirs hands the sandbox the snapshot root or the snapshot directory: %v", job.CopyDirs)
		}
	}
	if job.SQL != stmt || job.Limits.MaxRows != 50 {
		t.Errorf("job SQL/MaxRows = %q/%d", job.SQL, job.Limits.MaxRows)
	}
	// The views input, positively: no live leg although the DSN makes it
	// offerable, pinned (no following mode), local spellings.
	if seen == nil {
		t.Fatal("the views observer never ran")
	}
	if seen.LiveIndex != nil || seen.LiveLegUnavailable || seen.Follow != views.FollowNone || seen.PortableRouting || seen.OmitEvents {
		t.Errorf("views input: LiveIndex=%v LiveLegUnavailable=%v Follow=%v PortableRouting=%v",
			seen.LiveIndex != nil, seen.LiveLegUnavailable, seen.Follow, seen.PortableRouting)
	}
	if len(seen.ArchiveSources) != 1 || seen.ArchiveSources[0] != f.archiveDir || len(seen.Baselines) != 1 {
		t.Errorf("views input sources = %v / %d baselines", seen.ArchiveSources, len(seen.Baselines))
	}
	// And the rendered text, as a second belt.
	if !strings.Contains(job.ViewsSQL, "state_shop_orders") || !strings.Contains(job.ViewsSQL, "VIEW events") && !strings.Contains(job.ViewsSQL, "VIEW \"events\"") {
		t.Errorf("views do not define the state view and the events view:\n%s", job.ViewsSQL)
	}
	for _, forbidden := range []string{"ATTACH", "INSTALL", "LOAD ", "s3://"} {
		if strings.Contains(sqlCodeOnly(job.ViewsSQL), forbidden) {
			t.Errorf("views carry %q, which the locked worker refuses:\n%s", forbidden, job.ViewsSQL)
		}
	}
	// A statement that does not name events gets neither the events view nor
	// the archive directory: defining that view is the expensive half.
	w = f.post(t, `{"sql":"SELECT id FROM state_shop_orders"}`)
	if w.Code != http.StatusOK {
		t.Fatalf("state-only: code=%d body=%s", w.Code, w.Body.String())
	}
	job = f.runner.last(t)
	if len(job.CopyDirs) != 1 || job.CopyDirs[0] != f.schemaDir {
		t.Errorf("state-only job.CopyDirs = %v, want only %s", job.CopyDirs, f.schemaDir)
	}
	if !seen.OmitEvents || strings.Contains(job.ViewsSQL, "VIEW events") || strings.Contains(job.ViewsSQL, "VIEW \"events\"") {
		t.Errorf("state-only statement still installs the events view (OmitEvents=%v)", seen.OmitEvents)
	}
	// The body may not raise the cap.
	w = f.post(t, `{"sql":"SELECT 1","max_rows":100000}`)
	if w.Code != http.StatusBadRequest || !strings.Contains(w.Body.String(), "1000") {
		t.Errorf("max_rows above the cap: code=%d body=%s", w.Code, w.Body.String())
	}
}

// The REAL worker over the narrowed directories, without Docker: the state
// view and the events view answer from exactly the directories the handler
// hands over, and a file at the snapshot ROOT (where an operator-chosen
// baseline_dir could put the console's own configuration) is out of reach
// even though the views resolver started from that root.
func TestSQLAPI_realWorkerReadsOnlyTheNarrowedDirs(t *testing.T) {
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	runner := sqlsandbox.New(sqlsandbox.Config{Exe: exe, Args: []string{}, Limits: sqlsandbox.Limits{Timeout: 60 * time.Second}})
	f := newSQLFixture(t, runner, true)
	planted := filepath.Join(f.root, "console-servers.yaml")
	if err := os.WriteFile(planted, []byte("password: s3cret\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	w := f.post(t, `{"sql":"SELECT id, status FROM state_shop_orders ORDER BY id"}`)
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `[1,"new"]`) || !strings.Contains(w.Body.String(), `[2,"paid"]`) {
		t.Fatalf("state view: code=%d body=%s", w.Code, w.Body.String())
	}
	w = f.post(t, `{"sql":"SELECT count(*) AS n FROM events"}`)
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"rows":[[1]]`) {
		t.Fatalf("events view: code=%d body=%s", w.Code, w.Body.String())
	}
	w = f.post(t, `{"sql":"SELECT content FROM read_text('`+planted+`')"}`)
	if w.Code != http.StatusUnprocessableEntity || !strings.Contains(w.Body.String(), "Permission Error") || strings.Contains(w.Body.String(), "s3cret") {
		t.Fatalf("a file at the snapshot root was readable: code=%d body=%s", w.Code, w.Body.String())
	}
}

// The second belt: a copy directory that holds, or is an ancestor of the
// directory holding, the servers registry, the auth file or the MCP token
// is refused with a 409 before the runner runs. And the case the first belt
// alone answers: baseline_dir set to the directory the registry lives in
// runs, because the sandbox is handed the table's directory and not that
// root.
func TestSQLAPI_copyDirCoveringConsoleConfigIsRefused(t *testing.T) {
	// Registry file inside the pinned table's directory: equal.
	f := newSQLFixture(t, &fakeSQLRunner{res: oneRowResult()}, false)
	reg, err := LoadRegistry(filepath.Join(f.schemaDir, "console-servers.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	f.s.cm.reg = reg
	w := f.post(t, `{"sql":"SELECT 1"}`)
	if w.Code != http.StatusConflict || !strings.Contains(w.Body.String(), "console-servers.yaml") {
		t.Errorf("registry inside the copy dir: code=%d body=%s", w.Code, w.Body.String())
	}
	// MCP token below the pinned directory: ancestor.
	f = newSQLFixture(t, &fakeSQLRunner{res: oneRowResult()}, false)
	f.s.mcpTokenPath = filepath.Join(f.schemaDir, "deeper", "console-mcp-token.yaml")
	w = f.post(t, `{"sql":"SELECT 1"}`)
	if w.Code != http.StatusConflict || !strings.Contains(w.Body.String(), "console-mcp-token.yaml") {
		t.Errorf("token below the copy dir: code=%d body=%s", w.Code, w.Body.String())
	}
	// Auth file in the copy dir, reached through a symlink to it.
	f = newSQLFixture(t, &fakeSQLRunner{res: oneRowResult()}, false)
	link := filepath.Join(t.TempDir(), "link")
	if err := os.Symlink(f.schemaDir, link); err == nil {
		f.s.authPath = filepath.Join(link, "console-auth.yaml")
		w = f.post(t, `{"sql":"SELECT 1"}`)
		if w.Code != http.StatusConflict || !strings.Contains(w.Body.String(), "console-auth.yaml") {
			t.Errorf("auth file through a symlink: code=%d body=%s", w.Code, w.Body.String())
		}
	}
	if f.runner.calls() != 0 {
		t.Errorf("a refused copy reached the runner: %+v", f.runner.jobs)
	}
	// baseline_dir = the registry's directory: allowed, and the root is not
	// handed over.
	f = newSQLFixture(t, &fakeSQLRunner{res: oneRowResult()}, false)
	reg, err = LoadRegistry(filepath.Join(f.root, "console-servers.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	f.s.cm.reg = reg
	w = f.post(t, `{"sql":"SELECT 1"}`)
	if w.Code != http.StatusOK {
		t.Fatalf("registry beside the snapshots: code=%d body=%s", w.Code, w.Body.String())
	}
	if job := f.runner.last(t); len(job.CopyDirs) != 1 || job.CopyDirs[0] != f.schemaDir {
		t.Errorf("job.CopyDirs = %v, want only %s", job.CopyDirs, f.schemaDir)
	}
	// The helper itself, lexically.
	if _, _, c := dirCoversConfig([]string{"/data/snap"}, []string{"/data/snapshots/console-auth.yaml"}); c {
		t.Error("/data/snap is not an ancestor of /data/snapshots")
	}
	if _, _, c := dirCoversConfig([]string{"/data"}, []string{"/data/x/y/console-auth.yaml"}); !c {
		t.Error("/data is an ancestor of /data/x/y")
	}
}

// Every runner error maps to a status and a message a person can act on,
// and a worker failure never leaks the child's stderr.
func TestSQLAPI_errorMapping(t *testing.T) {
	cases := []struct {
		name     string
		err      error
		code     int
		want     string
		mustLack string
	}{
		{"refused", &sqlsandbox.RefusedError{Reason: "one statement at a time: 2 statements were given"}, 422, "one statement at a time", ""},
		{"query", &sqlsandbox.QueryError{Message: "Binder Error: column nope not found"}, 422, "Binder Error", ""},
		{"timeout", &sqlsandbox.TimeoutError{Limit: 60 * time.Second, PID: 7}, 504, "60 s", ""},
		{"busy", sqlsandbox.ErrBusy, 429, "already running", ""},
		{"not local", sqlsandbox.ErrCopyNotLocal, 409, "only on S3", ""},
		{"too large", sqlsandbox.ErrResultTooLarge, 422, "too large", ""},
		{"worker", &sqlsandbox.WorkerError{Err: errors.New("exit status 2"), Stderr: "panic at /var/lib/secret/path"}, 500, "DBTrail's log", "/var/lib/secret"},
		{"unknown", errors.New("something else"), 500, "DBTrail's log", "something else"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s, _ := newSQLServer(t, &fakeSQLRunner{err: c.err})
			w := postSQL(t, s, `{"sql":"SELECT 1"}`)
			if w.Code != c.code {
				t.Fatalf("code=%d body=%s, want %d", w.Code, w.Body.String(), c.code)
			}
			var body map[string]string
			if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
				t.Fatalf("not a JSON error: %s", w.Body.String())
			}
			if !strings.Contains(body["error"], c.want) {
				t.Errorf("error %q does not mention %q", body["error"], c.want)
			}
			if c.mustLack != "" && strings.Contains(w.Body.String(), c.mustLack) {
				t.Errorf("error leaks %q: %s", c.mustLack, w.Body.String())
			}
		})
	}
	// A client that went away gets nothing written.
	s, _ := newSQLServer(t, &fakeSQLRunner{err: context.Canceled})
	w := postSQL(t, s, `{"sql":"SELECT 1"}`)
	if w.Body.Len() != 0 {
		t.Errorf("a cancelled request still got a body: %s", w.Body.String())
	}
}

// Malformed requests are 400s before any runner call, including a body past
// the 1 MiB bound.
func TestSQLAPI_badRequests(t *testing.T) {
	runner := &fakeSQLRunner{res: oneRowResult()}
	s, _ := newSQLServer(t, runner)
	huge := `{"sql":"SELECT '` + strings.Repeat("x", sqlRequestMaxBytes) + `'"}`
	cases := map[string]struct{ body, query string }{
		"no body":        {"", ""},
		"not json":       {"SELECT 1", ""},
		"empty sql":      {`{"sql":"  "}`, ""},
		"negative rows":  {`{"sql":"SELECT 1","max_rows":-1}`, ""},
		"unknown format": {`{"sql":"SELECT 1"}`, "?format=xml"},
		"over 1 MiB":     {huge, ""},
	}
	for name, c := range cases {
		req := httptest.NewRequest("POST", "/api/sql"+c.query, strings.NewReader(c.body))
		w := httptest.NewRecorder()
		s.handleSQL(w, req)
		if w.Code != http.StatusBadRequest {
			t.Errorf("%s: code=%d body=%s, want 400", name, w.Code, w.Body.String())
		}
	}
	if runner.calls() != 0 {
		t.Errorf("a malformed request reached the runner: %+v", runner.jobs)
	}
}

// Condition 3 of #1952: a data-restricted session is refused with 403 and a
// reason, audited as profile.denied with surface_gate sql, EVEN WITH the
// permission; the process-wide profile (--profile) refuses the same way,
// with the same audit and no runner call; a session with no data
// restriction runs.
func TestSQLAPI_profileGateRefusesRestrictedSessions(t *testing.T) {
	rec := audittest.Install(t)
	runner := &fakeSQLRunner{res: oneRowResult()}
	s, _ := newSQLServer(t, runner)
	restricted := []*ext.AccessPolicy{
		{Permissions: []ext.Permission{ext.PermSQLExecute}, Profile: "auditors"},
		{Permissions: []ext.Permission{ext.PermSQLExecute}, Restrictions: &ext.SessionRestrictions{DenyTables: []ext.TableRef{{Schema: "shop", Table: "orders"}}}},
	}
	for i, pol := range restricted {
		req := httptest.NewRequest("POST", "/api/sql", strings.NewReader(`{"sql":"SELECT 1"}`))
		req = req.WithContext(context.WithValue(req.Context(), policyCtxKey{}, pol))
		w := httptest.NewRecorder()
		s.handleSQL(w, req)
		if w.Code != http.StatusForbidden || !strings.Contains(w.Body.String(), "raw") {
			t.Errorf("policy %d: code=%d body=%s, want 403 explaining raw files", i, w.Code, w.Body.String())
		}
	}
	// The process-wide profile refuses too.
	s.profileActive = true
	if w := postSQL(t, s, `{"sql":"SELECT 1"}`); w.Code != http.StatusForbidden {
		t.Errorf("process profile: code=%d body=%s, want 403", w.Code, w.Body.String())
	}
	s.profileActive = false
	if runner.calls() != 0 {
		t.Fatalf("a restricted session reached the runner: %+v", runner.jobs)
	}
	events := rec.Events()
	if len(events) != 3 {
		t.Fatalf("audit events = %d, want one profile.denied per refusal: %+v", len(events), events)
	}
	for _, ev := range events {
		if ev.Surface != "console" || ev.Action != "profile.denied" || ev.Detail["surface_gate"] != "sql" || ev.Detail["reason"] != "unredactable_surface" {
			t.Errorf("refusal audit = %+v", ev)
		}
	}
	// A policy with the permission and no data restriction runs.
	req := httptest.NewRequest("POST", "/api/sql", strings.NewReader(`{"sql":"SELECT 1"}`))
	req = req.WithContext(context.WithValue(req.Context(), policyCtxKey{}, &ext.AccessPolicy{Permissions: []ext.Permission{ext.PermSQLExecute}}))
	w := httptest.NewRecorder()
	s.handleSQL(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("unrestricted scoped session: code=%d body=%s", w.Code, w.Body.String())
	}
}

// The route needs sql:execute and nothing else grants it: through the real
// middleware chain, a scoped session without it is refused by authz (403,
// audited authz.denied naming the permission) and one with it reaches the
// handler; the static token, being policy-less, is never blocked by authz.
func TestSQLAPI_permissionEnforcedEndToEnd(t *testing.T) {
	if p, ok := permForRoute("POST", "/api/sql"); !ok || p != ext.PermSQLExecute {
		t.Fatalf("POST /api/sql classified as %q (%v), want %s", p, ok, ext.PermSQLExecute)
	}
	rec := audittest.Install(t)
	srv, err := New(Config{Listen: "127.0.0.1:8090", Token: "static-tok", AuthPath: filepath.Join(t.TempDir(), "auth.yaml")})
	if err != nil {
		t.Fatal(err)
	}
	runner := &fakeSQLRunner{res: oneRowResult()}
	srv.sqlRunner = runner
	dir := t.TempDir()
	writeSQLBaselineFixture(t, dir)
	srv.cm.boot = &bundle{baselineSrc: dir, baselineConfigured: true, activity: newActivityCache()}

	post := func(cred string) *httptest.ResponseRecorder {
		req := httptest.NewRequest("POST", "http://127.0.0.1:8090/api/sql", strings.NewReader(`{"sql":"SELECT 1"}`))
		req.Host = "127.0.0.1:8090"
		req.Header.Set("Authorization", "Bearer "+cred)
		w := httptest.NewRecorder()
		srv.Handler().ServeHTTP(w, req)
		return w
	}
	without, _, err := srv.sessions.IssueWithPolicy("reader", &ext.AccessPolicy{Permissions: []ext.Permission{ext.PermQueryExecute, ext.PermSettingsRead}})
	if err != nil {
		t.Fatal(err)
	}
	if w := post(without); w.Code != http.StatusForbidden || !strings.Contains(w.Body.String(), string(ext.PermSQLExecute)) {
		t.Errorf("query:execute alone: code=%d body=%s, want 403 naming sql:execute", w.Code, w.Body.String())
	}
	with, _, err := srv.sessions.IssueWithPolicy("analyst", &ext.AccessPolicy{Permissions: []ext.Permission{ext.PermSQLExecute}})
	if err != nil {
		t.Fatal(err)
	}
	if w := post(with); w.Code != http.StatusOK {
		t.Errorf("sql:execute: code=%d body=%s, want 200", w.Code, w.Body.String())
	}
	if job := runner.last(t); job.User != "analyst" {
		t.Errorf("job.User = %q, want the login identity", job.User)
	}
	if w := post("static-tok"); w.Code != http.StatusOK {
		t.Errorf("static token: code=%d body=%s, want 200", w.Code, w.Body.String())
	}
	var denied, ran int
	for _, ev := range rec.Events() {
		switch ev.Action {
		case "authz.denied":
			denied++
			if ev.Detail["missing_permission"] != string(ext.PermSQLExecute) {
				t.Errorf("authz.denied names %q", ev.Detail["missing_permission"])
			}
		case "sql.run":
			ran++
		}
	}
	if denied != 1 || ran != 2 {
		t.Errorf("audit: denied=%d ran=%d, want 1 and 2", denied, ran)
	}
}

// The success audit carries who, which server, the statement and what came
// back; never the rows. The truncation flags reach the JSON body.
func TestSQLAPI_auditsTheRunAndReportsTruncation(t *testing.T) {
	rec := audittest.Install(t)
	res := oneRowResult()
	res.Truncated = true
	res.TruncatedCells = 3
	s, _ := newSQLServer(t, &fakeSQLRunner{res: res})
	w := postSQL(t, s, `{"sql":"SELECT id FROM state_shop_orders"}`, serverHeader, bootServerID)
	if w.Code != http.StatusOK {
		t.Fatalf("code=%d body=%s", w.Code, w.Body.String())
	}
	var resp sqlResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if !resp.Truncated || resp.TruncatedCells != 3 {
		t.Errorf("truncation in the body: truncated=%v cells=%d", resp.Truncated, resp.TruncatedCells)
	}
	events := rec.Events()
	if len(events) != 1 {
		t.Fatalf("events = %+v", events)
	}
	ev := events[0]
	want := map[string]string{"sql": "SELECT id FROM state_shop_orders", "rows": "1", "truncated": "true", "elapsed_ms": "12", "server": bootServerID}
	for k, v := range want {
		if ev.Detail[k] != v {
			t.Errorf("detail[%s] = %q, want %q", k, ev.Detail[k], v)
		}
	}
	if ev.Surface != "console" || ev.Action != "sql.run" || ev.Actor != tokenActor {
		t.Errorf("event = %+v", ev)
	}
	for k, v := range ev.Detail {
		if strings.Contains(v, "new") && k != "sql" {
			t.Errorf("detail[%s] carries row data: %q", k, v)
		}
	}
	// A refused run audits nothing on this pair.
	rec.Reset()
	s2, _ := newSQLServer(t, &fakeSQLRunner{err: &sqlsandbox.RefusedError{Reason: "x"}})
	postSQL(t, s2, `{"sql":"COPY x TO y"}`)
	if got := rec.Events(); len(got) != 0 {
		t.Errorf("a refused statement was audited as a run: %+v", got)
	}
}

// A copy with a change log but no snapshot: the query runs over the events
// view alone, the sandbox gets the archive base only, and copy_updated_at
// is null.
func TestSQLAPI_changeLogWithoutSnapshotHasNoCopyTime(t *testing.T) {
	f := newSQLFixture(t, &fakeSQLRunner{res: oneRowResult()}, true)
	f.s.cm.boot.baselineSrc = ""
	f.s.cm.boot.baselineConfigured = false
	w := f.post(t, `{"sql":"SELECT count(*) FROM events"}`)
	if w.Code != http.StatusOK {
		t.Fatalf("code=%d body=%s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), `"copy_updated_at":null`) {
		t.Errorf("copy_updated_at should be null: %s", w.Body.String())
	}
	if job := f.runner.last(t); len(job.CopyDirs) != 1 || job.CopyDirs[0] != f.archiveDir {
		t.Errorf("job.CopyDirs = %v, want only the archive base", job.CopyDirs)
	}
	// On a change-log-only copy the events view is all there is to install,
	// so a statement that does not name it still runs (two resolver reads).
	f.expectArchive()
	w = f.post(t, `{"sql":"SELECT 1"}`)
	if w.Code != http.StatusOK {
		t.Fatalf("SELECT 1 on a change-log-only copy: code=%d body=%s", w.Code, w.Body.String())
	}
	if job := f.runner.last(t); len(job.CopyDirs) != 1 || job.CopyDirs[0] != f.archiveDir {
		t.Errorf("fallback job.CopyDirs = %v, want the archive base", job.CopyDirs)
	}
}

// The CSV form: by query parameter or Accept header, an attachment with a
// header line and CRLF rows, no-store, cells in the client dialect; audited
// with format=csv. format=json wins over an Accept header.
func TestSQLAPI_csvForm(t *testing.T) {
	rec := audittest.Install(t)
	res := sqlsandbox.Result{
		Columns: []sqlsandbox.Column{{Name: "id", Type: "INTEGER"}, {Name: "note", Type: "VARCHAR"}, {Name: "tags", Type: "VARCHAR[]"}},
		Rows: [][]any{
			{json.Number("1"), `say "hi", now`, []any{"a", "b<c&d"}},
			{json.Number("-2"), nil, map[string]any{"k": json.Number("1")}},
			{json.Number("3"), "=SUM(A1)", true},
		},
	}
	s, _ := newSQLServer(t, &fakeSQLRunner{res: res})
	for name, hdr := range map[string][]string{"query": nil, "accept": {"Accept", "text/csv"}} {
		path := "/api/sql"
		if name == "query" {
			path += "?format=csv"
		}
		req := httptest.NewRequest("POST", path, strings.NewReader(`{"sql":"SELECT 1"}`))
		for i := 0; i+1 < len(hdr); i += 2 {
			req.Header.Set(hdr[i], hdr[i+1])
		}
		w := httptest.NewRecorder()
		s.handleSQL(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("%s: code=%d body=%s", name, w.Code, w.Body.String())
		}
		if ct := w.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/csv") {
			t.Errorf("%s: Content-Type = %q", name, ct)
		}
		if cd := w.Header().Get("Content-Disposition"); !strings.Contains(cd, `attachment; filename="dbtrail-sql-`) || !strings.HasSuffix(cd, `.csv"`) {
			t.Errorf("%s: Content-Disposition = %q", name, cd)
		}
		if cc := w.Header().Get("Cache-Control"); cc != "no-store" {
			t.Errorf("%s: Cache-Control = %q, want no-store", name, cc)
		}
		want := "id,note,tags\r\n" +
			`1,"say ""hi"", now","[""a"",""b<c&d""]"` + "\r\n" +
			`'-2,,"{""k"":1}"` + "\r\n" +
			`3,'=SUM(A1),true`
		if got := w.Body.String(); got != want {
			t.Errorf("%s: csv =\n%q\nwant\n%q", name, got, want)
		}
	}
	events := rec.Events()
	if len(events) != 2 {
		t.Fatalf("csv audit = %+v", events)
	}
	for _, ev := range events {
		if ev.Action != "sql.run" || ev.Detail["format"] != "csv" {
			t.Errorf("csv audit event = %+v, want sql.run with format=csv", ev)
		}
	}
	// format=json beats the Accept header.
	req := httptest.NewRequest("POST", "/api/sql?format=json", strings.NewReader(`{"sql":"SELECT 1"}`))
	req.Header.Set("Accept", "text/csv")
	w := httptest.NewRecorder()
	s.handleSQL(w, req)
	if w.Code != http.StatusOK || !strings.HasPrefix(w.Header().Get("Content-Type"), "application/json") {
		t.Errorf("format=json with Accept csv: code=%d type=%q", w.Code, w.Header().Get("Content-Type"))
	}
}

// clientCSVCellJS is the client's csvCell in assets/app.js, whole. Any edit
// to the JS rules fails this pin, so the Go twin below is re-checked.
const clientCSVCellJS = `function csvCell(v) {
  if (v === null || v === undefined) return "";
  let s = typeof v === "object" ? JSON.stringify(v) : String(v);
  // Formula-injection guard (OWASP): a leading =, +, -, @, tab or CR would be
  // interpreted as a formula by Excel/Sheets — prefix a quote to neutralize.
  if (/^[=+\-@\t\r]/.test(s)) s = "'" + s;
  return /[",\r\n]/.test(s) ? '"' + s.replace(/"/g, '""') + '"' : s;
}`

// csvCell is the Go twin of csvCell in assets/app.js: same formula guard,
// same quoting, same nested rendering (no HTML escaping). The JS body is
// pinned whole so the two cannot drift apart silently; the Go rules by value.
func TestCSVCellMatchesTheClientDialect(t *testing.T) {
	js, err := os.ReadFile(filepath.Join("assets", "app.js"))
	if err != nil {
		t.Fatal(err)
	}
	start := strings.Index(string(js), "function csvCell(v) {")
	if start < 0 {
		t.Fatal("assets/app.js has no csvCell")
	}
	end := strings.Index(string(js)[start:], "\n}")
	if body := string(js)[start : start+end+2]; body != clientCSVCellJS {
		t.Errorf("assets/app.js csvCell changed; re-check the Go twin and update the pin:\n%s", body)
	}
	cases := []struct {
		in   any
		want string
	}{
		{nil, ""},
		{"plain", "plain"},
		{"a,b", `"a,b"`},
		{`q"q`, `"q""q"`},
		{"line\nbreak", "\"line\nbreak\""},
		{"=1+1", "'=1+1"},
		{"+1", "'+1"},
		{"-5", "'-5"},
		{"@x", "'@x"},
		{"\tx", "'\tx"},
		{"\rx", "\"'\rx\""},
		{json.Number("42"), "42"},
		{json.Number("-1.5"), "'-1.5"},
		{true, "true"},
		{[]any{json.Number("1"), "x"}, `"[1,""x""]"`},
		{[]any{"a<b&c"}, `"[""a<b&c""]"`},
		{map[string]any{"a": "b"}, `"{""a"":""b""}"`},
	}
	for _, c := range cases {
		if got := csvCell(c.in); got != c.want {
			t.Errorf("csvCell(%#v) = %q, want %q", c.in, got, c.want)
		}
	}
}

// A copy the worker cannot reach: no snapshot and no archive is a 409 that
// says there is no copy yet; archive access disabled is a 409 too. The
// S3-only mapping is covered by the error table above.
func TestSQLAPI_noCopyIs409(t *testing.T) {
	runner := &fakeSQLRunner{res: oneRowResult()}
	s := &Server{token: "t", cm: newConnManager(nil, false), sessionProfiles: newProfileRuleCache()}
	s.cm.boot = &bundle{activity: newActivityCache()}
	s.sqlRunner = runner
	s.sqlLimits = resolveSQLLimits(sqlsandbox.Limits{})
	s.mux = s.buildHandler()
	if w := postSQL(t, s, `{"sql":"SELECT 1"}`); w.Code != http.StatusConflict || !strings.Contains(w.Body.String(), "no copy") {
		t.Errorf("no sources: code=%d body=%s", w.Code, w.Body.String())
	}
	s.cm.boot.noArchive = true
	if w := postSQL(t, s, `{"sql":"SELECT 1"}`); w.Code != http.StatusConflict || !strings.Contains(w.Body.String(), "disabled") {
		t.Errorf("noArchive: code=%d body=%s", w.Code, w.Body.String())
	}
	if runner.calls() != 0 {
		t.Errorf("the runner was called with no copy: %+v", runner.jobs)
	}
}

// The runner the server builds is one per process with the resolved caps.
func TestSQLAPI_serverBuildsOneRunnerWithResolvedLimits(t *testing.T) {
	srv, err := New(Config{Listen: "127.0.0.1:8090", Token: "t", SQLLimits: sqlsandbox.Limits{MaxRows: 25}})
	if err != nil {
		t.Fatal(err)
	}
	if srv.sqlRunner == nil {
		t.Fatal("no runner")
	}
	d := sqlsandbox.DefaultLimits()
	if srv.sqlLimits.MaxRows != 25 || srv.sqlLimits.Timeout != d.Timeout || srv.sqlLimits.MemoryLimit != d.MemoryLimit || srv.sqlLimits.Threads != d.Threads {
		t.Errorf("limits = %+v", srv.sqlLimits)
	}
}

// A relative copy directory (a relative --baseline-dir or --archive-dir) is
// still a local copy: sqlCopyDirs makes it absolute so the sandbox does
// not refuse it as "not local", which the user would read as "only on S3".
func TestSQLCopyDirs_relativePathsBecomeAbsolute(t *testing.T) {
	got := sqlCopyDirs(views.Input{
		ArchiveSources: []string{"rel/archives/bintrail_id=x"},
		Baselines:      []views.BaselineTable{{Schema: "shop", Table: "orders", Path: "rel/snap/2026-04-30T03-00-00Z/shop/orders.parquet"}},
	})
	if len(got) != 2 {
		t.Fatalf("dirs = %v", got)
	}
	for _, d := range got {
		if !filepath.IsAbs(d) || !sqlsandboxLocal(d) {
			t.Errorf("dir %q is not absolute", d)
		}
	}
	if !strings.HasSuffix(got[0], filepath.Join("rel", "archives", "bintrail_id=x")) || !strings.HasSuffix(got[1], filepath.Join("rel", "snap", "2026-04-30T03-00-00Z", "shop")) {
		t.Errorf("dirs = %v", got)
	}
}

// sqlsandboxLocal mirrors the sandbox's own "is this a local copy dir" rule
// (absolute, not s3://), so the test above fails the way the runner would.
func sqlsandboxLocal(d string) bool { return filepath.IsAbs(d) && !strings.HasPrefix(d, "s3://") }

func TestSQLMentionsEvents(t *testing.T) {
	for stmt, want := range map[string]bool{
		"SELECT * FROM events":                   true,
		"select count(*) from EVENTS e":          true,
		"SELECT * FROM \"events\" LIMIT 1":       true,
		"SELECT * FROM state_shop_orders":        false,
		"SELECT * FROM state_shop_events_log":    false,
		"SELECT * FROM state_x WHERE eventsx=1":  false,
		"SELECT * FROM main.events":              true,
		"SELECT * FROM memory.main.events e":     true,
		"FROM Events":                            true,
		"SELECT count(*) FROM events;":           true,
		"SELECT * FROM (events)":                 true,
		"SELECT count(*) AS events FROM state_x": true,
	} {
		if got := sqlMentionsEvents(stmt); got != want {
			t.Errorf("sqlMentionsEvents(%q) = %v, want %v", stmt, got, want)
		}
	}
}

// GET /api/sql: the names a statement can use, the pinned snapshot's time
// and the real limits, read without a worker; same gates as the POST.
func TestSQLAPI_infoListsViewsCopyTimeAndLimits(t *testing.T) {
	f := newSQLFixture(t, &fakeSQLRunner{res: oneRowResult()}, true)
	f.s.sqlLimits = resolveSQLLimits(sqlsandbox.Limits{Timeout: 45 * time.Second, MaxRows: 250})
	get := func(s *Server, ctx context.Context) *httptest.ResponseRecorder {
		req := httptest.NewRequest("GET", "/api/sql", nil)
		if ctx != nil {
			req = req.WithContext(ctx)
		}
		w := httptest.NewRecorder()
		s.handleSQLInfo(w, req)
		return w
	}
	f.expectArchive()
	w := get(f.s, nil)
	if w.Code != http.StatusOK {
		t.Fatalf("code=%d body=%s", w.Code, w.Body.String())
	}
	var info sqlInfoResponse
	if err := json.Unmarshal(w.Body.Bytes(), &info); err != nil {
		t.Fatal(err)
	}
	if strings.Join(info.Views, ",") != "events,state_shop_orders" {
		t.Errorf("views = %v, want [events state_shop_orders]", info.Views)
	}
	if info.CopyUpdatedAt == nil || !info.CopyUpdatedAt.Equal(sqlSnapshotAt) {
		t.Errorf("copy_updated_at = %v", info.CopyUpdatedAt)
	}
	if info.Limits.TimeoutSeconds != 45 || info.Limits.MaxRows != 250 || info.Limits.MaxCellBytes != sqlsandbox.MaxCellBytes {
		t.Errorf("limits = %+v", info.Limits)
	}
	if f.runner.calls() != 0 {
		t.Errorf("the metadata read spawned a worker: %+v", f.runner.jobs)
	}
	// Without a change log the list is the state views alone.
	g := newSQLFixture(t, &fakeSQLRunner{}, false)
	w = get(g.s, nil)
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"views":["state_shop_orders"]`) {
		t.Errorf("no archive: code=%d body=%s", w.Code, w.Body.String())
	}
	// The profile gate refuses the listing too: view names are table names.
	pol := &ext.AccessPolicy{Permissions: []ext.Permission{ext.PermSQLExecute}, Profile: "auditors"}
	w = get(g.s, context.WithValue(context.Background(), policyCtxKey{}, pol))
	if w.Code != http.StatusForbidden {
		t.Errorf("restricted session: code=%d body=%s, want 403", w.Code, w.Body.String())
	}
	if p, ok := permForRoute("GET", "/api/sql"); !ok || p != ext.PermSQLExecute {
		t.Errorf("GET /api/sql classified as %q (%v)", p, ok)
	}
}

// The `sql` capability is true exactly where the route can work: a session
// holding sql:execute, no data profile, archive access on, a local copy.
func TestCapabilities_sql(t *testing.T) {
	caps := func(s *Server, pol *ext.AccessPolicy) bool {
		req := httptest.NewRequest("GET", "/api/capabilities", nil)
		if pol != nil {
			req = req.WithContext(context.WithValue(req.Context(), policyCtxKey{}, pol))
		}
		w := httptest.NewRecorder()
		s.handleCapabilities(w, req)
		var resp capabilitiesResponse
		if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
			t.Fatalf("capabilities: %v: %s", err, w.Body.String())
		}
		if !strings.Contains(w.Body.String(), `"sql":`) {
			t.Fatalf("capabilities carries no sql field: %s", w.Body.String())
		}
		return resp.SQL
	}
	f := newSQLFixture(t, &fakeSQLRunner{}, false)
	if !caps(f.s, nil) {
		t.Error("local snapshot directory, policy-less session: sql should be true")
	}
	if !caps(f.s, &ext.AccessPolicy{Permissions: []ext.Permission{ext.PermSQLExecute}}) {
		t.Error("session holding sql:execute: sql should be true")
	}
	if caps(f.s, &ext.AccessPolicy{Permissions: []ext.Permission{ext.PermQueryExecute, ext.PermSettingsRead}}) {
		t.Error("session without sql:execute: sql should be false")
	}
	if caps(f.s, &ext.AccessPolicy{Permissions: []ext.Permission{ext.PermSQLExecute}, Profile: "auditors"}) {
		t.Error("data-restricted session: sql should be false")
	}
	f.s.profileActive = true
	if caps(f.s, nil) {
		t.Error("process-wide profile: sql should be false")
	}
	f.s.profileActive = false
	f.s.cm.boot.noArchive = true
	if caps(f.s, nil) {
		t.Error("archive access disabled: sql should be false")
	}
	f.s.cm.boot.noArchive = false
	f.s.cm.boot.baselineSrc = "s3://bucket/baselines"
	if caps(f.s, nil) {
		t.Error("S3-only snapshot root and no change log: sql should be false")
	}
	// An S3 snapshot root with a LOCAL change log is one of the two
	// documented conservative corners: the route would have to list S3
	// before it could answer, which a capability probe must not do.
	a := newSQLFixture(t, &fakeSQLRunner{}, true)
	a.s.cm.boot.baselineSrc = "s3://bucket/baselines"
	a.expectArchive()
	if caps(a.s, nil) {
		t.Error("S3 snapshot root: sql should be false (conservative, documented on sqlAvailable)")
	}
	a = newSQLFixture(t, &fakeSQLRunner{}, true)
	a.s.cm.boot.baselineSrc = ""
	a.expectArchive()
	if !caps(a.s, nil) {
		t.Error("no snapshot root, local change log: sql should be true")
	}
	f.s.cm.boot.baselineSrc = ""
	if caps(f.s, nil) {
		t.Error("no copy at all: sql should be false")
	}
}

// The mixed layout: the tables on local disk, the change log in S3. A
// statement over the tables runs; one that merely CONTAINS the word events
// runs too (the word match is loose on purpose, and must not turn a valid
// query into "the copy is only on S3"); one that really reads events gets
// a sentence that says what is where; the listing offers the tables only.
func TestSQLAPI_localTablesWithChangeLogInS3(t *testing.T) {
	f := newSQLFixture(t, &fakeSQLRunner{res: oneRowResult()}, true)
	postS3 := func(body string) *httptest.ResponseRecorder {
		f.expectArchiveS3()
		return postSQL(t, f.s, body)
	}
	var seen *views.Input
	f.s.sqlViewsObserver = func(in views.Input) { seen = &in }
	for _, stmt := range []string{
		"SELECT id FROM state_shop_orders",
		"SELECT count(*) AS events FROM state_shop_orders",
		"SELECT id FROM state_shop_orders WHERE status LIKE '%events%'",
	} {
		w := postS3(`{"sql":"` + stmt + `"}`)
		if w.Code != http.StatusOK {
			t.Fatalf("%s: code=%d body=%s", stmt, w.Code, w.Body.String())
		}
		job := f.runner.last(t)
		if len(job.CopyDirs) != 1 || job.CopyDirs[0] != f.schemaDir {
			t.Errorf("%s: job.CopyDirs = %v, want only %s", stmt, job.CopyDirs, f.schemaDir)
		}
		if code := sqlCodeOnly(job.ViewsSQL); !seen.OmitEvents || strings.Contains(code, "s3://") || strings.Contains(code, "INSTALL") || strings.Contains(code, "ATTACH") {
			t.Errorf("%s: the views reach for S3 (OmitEvents=%v):\n%s", stmt, seen.OmitEvents, code)
		}
	}
	// A statement that really reads events: DuckDB says the table does not
	// exist (it was not installed); the person reads why.
	f.runner.err = &sqlsandbox.QueryError{Message: "Catalog Error: Table with name events does not exist!\nDid you mean \"state_shop_orders\"?"}
	w := postS3(`{"sql":"SELECT count(*) FROM events"}`)
	if w.Code != http.StatusUnprocessableEntity || !strings.Contains(w.Body.String(), "on S3, so events cannot be read here; the tables can") {
		t.Errorf("reading events with the change log in S3: code=%d body=%s", w.Code, w.Body.String())
	}
	if strings.Contains(w.Body.String(), "only on S3") || strings.Contains(w.Body.String(), "Catalog Error") {
		t.Errorf("the answer blames the whole copy or echoes the catalog error: %s", w.Body.String())
	}
	// Any other DuckDB error there stays DuckDB's.
	f.runner.err = &sqlsandbox.QueryError{Message: "Binder Error: column nope not found"}
	if w := postS3(`{"sql":"SELECT nope FROM events"}`); w.Code != http.StatusUnprocessableEntity || !strings.Contains(w.Body.String(), "Binder Error") {
		t.Errorf("another error: code=%d body=%s", w.Code, w.Body.String())
	}
	f.runner.err = nil
	// The listing offers the tables only.
	f.expectArchiveS3()
	g := httptest.NewRecorder()
	f.s.handleSQLInfo(g, httptest.NewRequest("GET", "/api/sql", nil))
	if g.Code != http.StatusOK || !strings.Contains(g.Body.String(), `"views":["state_shop_orders"]`) {
		t.Errorf("listing: code=%d body=%s", g.Code, g.Body.String())
	}
	// On a fully local copy the same catalog error is NOT rewritten.
	l := newSQLFixture(t, &fakeSQLRunner{err: &sqlsandbox.QueryError{Message: "Catalog Error: Table with name events does not exist!"}}, false)
	if w := l.post(t, `{"sql":"SELECT * FROM events"}`); !strings.Contains(w.Body.String(), "Catalog Error") {
		t.Errorf("no change log at all: the catalog error should stand: %s", w.Body.String())
	}
	// A change log in S3 and nothing else: that copy really is only on S3.
	o := newSQLFixture(t, &fakeSQLRunner{res: oneRowResult()}, true)
	o.s.cm.boot.baselineSrc = ""
	o.expectArchiveS3()
	if w := postSQL(t, o.s, `{"sql":"SELECT 1"}`); w.Code != http.StatusConflict || !strings.Contains(w.Body.String(), sqlCopyNotLocalMessage) {
		t.Errorf("S3-only change log, no snapshot: code=%d body=%s", w.Code, w.Body.String())
	}
	if o.runner.calls() != 0 {
		t.Errorf("an S3-only copy reached the runner")
	}
}

// GET /api/sql refuses what the POST refuses, with the same words.
func TestSQLAPI_infoRefusals(t *testing.T) {
	get := func(s *Server) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		s.handleSQLInfo(w, httptest.NewRequest("GET", "/api/sql", nil))
		return w
	}
	// No copy at all.
	n := newSQLFixture(t, &fakeSQLRunner{}, false)
	n.s.cm.boot.baselineSrc = ""
	if w := get(n.s); w.Code != http.StatusConflict || !strings.Contains(w.Body.String(), errNoViewSources.Error()) {
		t.Errorf("no copy: code=%d body=%s", w.Code, w.Body.String())
	}
	// Archive access disabled.
	n.s.cm.boot.noArchive = true
	if w := get(n.s); w.Code != http.StatusConflict || !strings.Contains(w.Body.String(), "disabled") {
		t.Errorf("archive disabled: code=%d body=%s", w.Code, w.Body.String())
	}
	// A change log in S3 and nothing else.
	o := newSQLFixture(t, &fakeSQLRunner{}, true)
	o.s.cm.boot.baselineSrc = ""
	o.expectArchiveS3()
	if w := get(o.s); w.Code != http.StatusConflict || !strings.Contains(w.Body.String(), sqlCopyNotLocalMessage) {
		t.Errorf("S3-only: code=%d body=%s", w.Code, w.Body.String())
	}
}

// The capability never promises what the route refuses: over every state
// a unit fixture can stand up, `sql` is true exactly when GET /api/sql is
// 200. (Where the capability is conservative, the route needs S3 to answer
// at all; those two corners are documented on sqlAvailable.)
func TestCapabilities_sqlAgreesWithTheRoute(t *testing.T) {
	states := map[string]func(t *testing.T) (*sqlFixture, func()){
		"local snapshot": func(t *testing.T) (*sqlFixture, func()) {
			return newSQLFixture(t, &fakeSQLRunner{}, false), func() {}
		},
		"local directory, zero snapshots": func(t *testing.T) (*sqlFixture, func()) {
			f := newSQLFixture(t, &fakeSQLRunner{}, false)
			f.s.cm.boot.baselineSrc = t.TempDir()
			return f, func() {}
		},
		"local directory with only an incomplete snapshot": func(t *testing.T) (*sqlFixture, func()) {
			f := newSQLFixture(t, &fakeSQLRunner{}, false)
			if err := baseline.WriteIncompleteMarker(filepath.Join(f.root, sqlSnapshotDirName)); err != nil {
				t.Fatal(err)
			}
			return f, func() {}
		},
		"no snapshot root, local change log": func(t *testing.T) (*sqlFixture, func()) {
			f := newSQLFixture(t, &fakeSQLRunner{}, true)
			f.s.cm.boot.baselineSrc = ""
			return f, f.expectArchive
		},
		"no snapshot root, change log in S3": func(t *testing.T) (*sqlFixture, func()) {
			f := newSQLFixture(t, &fakeSQLRunner{}, true)
			f.s.cm.boot.baselineSrc = ""
			return f, f.expectArchiveS3
		},
		"local snapshot, change log in S3": func(t *testing.T) (*sqlFixture, func()) {
			f := newSQLFixture(t, &fakeSQLRunner{}, true)
			return f, f.expectArchiveS3
		},
		"no copy at all": func(t *testing.T) (*sqlFixture, func()) {
			f := newSQLFixture(t, &fakeSQLRunner{}, false)
			f.s.cm.boot.baselineSrc = ""
			return f, func() {}
		},
		"archive access disabled": func(t *testing.T) (*sqlFixture, func()) {
			f := newSQLFixture(t, &fakeSQLRunner{}, false)
			f.s.cm.boot.noArchive = true
			return f, func() {}
		},
	}
	want := map[string]bool{
		"local snapshot": true, "local directory, zero snapshots": false, "local directory with only an incomplete snapshot": false,
		"no snapshot root, local change log": true, "no snapshot root, change log in S3": false,
		"local snapshot, change log in S3": true, "no copy at all": false, "archive access disabled": false,
	}
	for name, build := range states {
		t.Run(name, func(t *testing.T) {
			f, expect := build(t)
			expect()
			cw := httptest.NewRecorder()
			f.s.handleCapabilities(cw, httptest.NewRequest("GET", "/api/capabilities", nil))
			var caps capabilitiesResponse
			if err := json.Unmarshal(cw.Body.Bytes(), &caps); err != nil {
				t.Fatal(err)
			}
			// A fresh set of answers for the route's own read.
			g, expect2 := build(t)
			expect2()
			gw := httptest.NewRecorder()
			g.s.handleSQLInfo(gw, httptest.NewRequest("GET", "/api/sql", nil))
			if caps.SQL != (gw.Code == http.StatusOK) {
				t.Errorf("capability sql=%v but GET /api/sql is %d: %s", caps.SQL, gw.Code, gw.Body.String())
			}
			if caps.SQL != want[name] {
				t.Errorf("capability sql=%v, want %v", caps.SQL, want[name])
			}
		})
	}
	// The S3-only fixture the review asked for: an S3 snapshot root AND a
	// change log registered in S3 only. False, without a network call.
	f := newSQLFixture(t, &fakeSQLRunner{}, true)
	f.s.cm.boot.baselineSrc = "s3://bucket/baselines"
	f.expectArchiveS3()
	cw := httptest.NewRecorder()
	f.s.handleCapabilities(cw, httptest.NewRequest("GET", "/api/capabilities", nil))
	if strings.Contains(cw.Body.String(), `"sql":true`) {
		t.Errorf("an S3-only copy reports sql:true: %s", cw.Body.String())
	}
}
