package console

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/audittest"
	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

// mcpSQLSession serves f over HTTP and connects an MCP client with the
// fixture's static token.
func mcpSQLSession(t *testing.T, f *sqlFixture) func(args map[string]any) (bool, string) {
	t.Helper()
	ts := httptest.NewServer(f.s.mux)
	t.Cleanup(ts.Close)
	session := mcpGrantConnect(t, ts.URL+"/mcp", "t")
	return func(args map[string]any) (bool, string) {
		res, texts := routedCall(t, session, "sql", args)
		return res.IsError, strings.Join(texts, "\n")
	}
}

// The sql tool runs the statement on the copy through the same path as
// POST /api/sql: the job carries the statement and the copy's directory,
// the slot is keyed by the token, and the answer echoes the statement with
// the copy's time.
func TestMCPSQL_runsOnTheCopyLikeTheSQLPage(t *testing.T) {
	rec := audittest.Install(t)
	runner := &fakeSQLRunner{res: oneRowResult()}
	f := newSQLFixture(t, runner, false)
	call := mcpSQLSession(t, f)

	const stmt = "SELECT id, status FROM shop.orders\nWHERE id = 1"
	isErr, text := call(map[string]any{"statement": stmt, "max_rows": 10})
	if isErr {
		t.Fatalf("sql tool failed: %s", text)
	}
	var out struct {
		Statement     string     `json:"statement"`
		Rows          [][]any    `json:"rows"`
		RowCount      int        `json:"row_count"`
		CopyUpdatedAt *time.Time `json:"copy_updated_at"`
	}
	if err := json.Unmarshal([]byte(text), &out); err != nil {
		t.Fatalf("answer is not JSON: %v\n%s", err, text)
	}
	if out.Statement != stmt || out.RowCount != 1 || len(out.Rows) != 1 {
		t.Errorf("statement=%q row_count=%d rows=%v", out.Statement, out.RowCount, out.Rows)
	}
	if out.CopyUpdatedAt == nil || !out.CopyUpdatedAt.Equal(sqlSnapshotAt) {
		t.Errorf("copy_updated_at = %v, want %v", out.CopyUpdatedAt, sqlSnapshotAt)
	}
	job := runner.last(t)
	if job.SQL != stmt || job.Limits.MaxRows != 10 {
		t.Errorf("job SQL=%q MaxRows=%d", job.SQL, job.Limits.MaxRows)
	}
	if len(job.CopyDirs) == 0 || !strings.HasPrefix(job.CopyDirs[0], f.root) {
		t.Errorf("job CopyDirs = %v, want the fixture's copy under %s", job.CopyDirs, f.root)
	}
	if len(runner.users) != 1 || runner.users[0] != "mcp:static" {
		t.Errorf("slot keys = %v, want [mcp:static]: the slot is the token's", runner.users)
	}
	// The record a real console emission makes: tagged console-mcp, with
	// the statement as asked.
	evs := rec.Events()
	if len(evs) != 1 || evs[0].Surface != "console-mcp" || evs[0].Action != "sql.run" || evs[0].Detail["sql"] != stmt {
		t.Errorf("audit = %+v, want one console-mcp sql.run carrying the statement", evs)
	}
}

// The REAL worker behind the tool: a locked DuckDB over the fixture copy.
// The answer a model reads, the error DuckDB gives for a wrong name, and the
// sandbox's refusal of a file outside the copy all reach the client.
func TestMCPSQL_realWorker(t *testing.T) {
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	runner := sandboxRunner{sqlsandbox.New(sqlsandbox.Config{Exe: exe, Args: []string{}, Limits: sqlsandbox.Limits{Timeout: 60 * time.Second}})}
	f := newSQLFixture(t, runner, false)
	call := mcpSQLSession(t, f)

	isErr, text := call(map[string]any{"statement": "SELECT status, count(*) AS n FROM shop.orders GROUP BY status ORDER BY status"})
	t.Logf("answer:\n%s", text)
	if isErr || !strings.Contains(text, `"new"`) || !strings.Contains(text, `"paid"`) || !strings.Contains(text, `"row_count":2`) {
		t.Fatalf("aggregate: isErr=%v\n%s", isErr, text)
	}

	isErr, text = call(map[string]any{"statement": "SELECT * FROM shop.nope"})
	t.Logf("wrong table:\n%s", text)
	if !isErr || !strings.Contains(text, "nope") {
		t.Errorf("wrong table: isErr=%v %q", isErr, text)
	}

	planted := filepath.Join(f.root, "console-servers.yaml")
	if err := os.WriteFile(planted, []byte("leak-marker-2456"), 0o600); err != nil {
		t.Fatal(err)
	}
	isErr, text = call(map[string]any{"statement": "SELECT content FROM read_text('" + planted + "')"})
	if !isErr || !strings.Contains(text, "Permission Error") || strings.Contains(text, "leak-marker-2456") {
		t.Errorf("a file outside the copy: isErr=%v %q", isErr, text)
	}
}

func TestMCPSQL_refusals(t *testing.T) {
	t.Run("data profile", func(t *testing.T) {
		runner := &fakeSQLRunner{res: oneRowResult()}
		f := newSQLFixture(t, runner, false)
		f.s.profileActive = true
		isErr, text := mcpSQLSession(t, f)(map[string]any{"statement": "SELECT 1"})
		if !isErr || !strings.Contains(text, "access rules are active") {
			t.Fatalf("got %q, want the access-rules refusal", text)
		}
		if runner.calls() != 0 || len(runner.users) != 0 {
			t.Error("a refused statement took a slot or ran")
		}
	})
	t.Run("archive access off", func(t *testing.T) {
		runner := &fakeSQLRunner{res: oneRowResult()}
		f := newSQLFixture(t, runner, false)
		f.s.cm.boot.noArchive = true
		isErr, text := mcpSQLSession(t, f)(map[string]any{"statement": "SELECT 1"})
		if !isErr || !strings.Contains(text, "archive access is disabled") {
			t.Fatalf("got %q", text)
		}
		if len(runner.users) != 0 {
			t.Error("a refused statement took a slot")
		}
	})
	t.Run("busy", func(t *testing.T) {
		f := newSQLFixture(t, &fakeSQLRunner{busy: true}, false)
		isErr, text := mcpSQLSession(t, f)(map[string]any{"statement": "SELECT 1"})
		if !isErr || text != sqlsandbox.ErrBusy.Error() {
			t.Fatalf("got %q, want %q", text, sqlsandbox.ErrBusy.Error())
		}
	})
	t.Run("no sandbox, no tool", func(t *testing.T) {
		f := newSQLFixture(t, nil, false)
		ts := httptest.NewServer(f.s.mux)
		t.Cleanup(ts.Close)
		tools, err := mcpGrantConnect(t, ts.URL+"/mcp", "t").ListTools(context.Background(), nil)
		if err != nil {
			t.Fatal(err)
		}
		for _, tool := range tools.Tools {
			if tool.Name == "sql" {
				t.Fatal("sql listed by a console with no SQL sandbox: every call would fail")
			}
		}
	})
}

func TestMCPSQLError(t *testing.T) {
	cases := []struct {
		name    string
		err     error
		want    string
		notWant string
	}{
		{"copy only on S3", &sqlRefusal{409, sqlCopyNotLocalMessage}, sqlCopyNotLocalPortMessage, "browser"},
		{"copy not local (runner)", sqlsandbox.ErrCopyNotLocal, sqlCopyNotLocalPortMessage, ""},
		{"events in S3", &sqlRefusal{409, sqlEventsInS3Message}, sqlEventsInS3MCPMessage, "Events page"},
		{"other refusal", &sqlRefusal{409, "no snapshot yet"}, "no snapshot yet", ""},
		{"timeout", &sqlsandbox.TimeoutError{Limit: time.Minute}, "narrow it", ""},
		{"refused shape", &sqlsandbox.RefusedError{Reason: "not a SELECT"}, "query refused: not a SELECT", ""},
		{"query error", &sqlsandbox.QueryError{Message: "Table shop.x does not exist"}, "Table shop.x does not exist", ""},
		{"too large", sqlsandbox.ErrResultTooLarge, sqlsandbox.ErrResultTooLarge.Error(), ""},
		{"worker failed", &sqlsandbox.WorkerError{Err: errors.New("exec /opt/secret/path: denied")}, sqlWorkerFailedMessage, "/opt/secret"},
		{"unknown", errors.New("dial tcp 10.0.0.5:3306: refused"), "DBTrail's log has the details", "10.0.0.5"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := mcpSQLError(tc.err).Error()
			if !strings.Contains(got, tc.want) {
				t.Errorf("got %q, want it to contain %q", got, tc.want)
			}
			if tc.notWant != "" && strings.Contains(got, tc.notWant) {
				t.Errorf("got %q, must not contain %q", got, tc.notWant)
			}
		})
	}
}
