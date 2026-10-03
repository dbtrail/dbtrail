//go:build integration

package console

import (
	"encoding/json"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// The route end to end with the REAL runner: a real child process (this test
// binary, see TestMain), a real locked DuckDB, a real snapshot on disk, and
// a real index database behind the bundle (empty archive_state, so the
// views carry the state view alone). JSON and CSV forms, and a refusal the
// sandbox itself makes.
func TestIntegrationSQLRoute_realRunnerOnAFixtureCopy(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	baseDir := t.TempDir()
	writeSQLBaselineFixture(t, baseDir)
	srv, err := New(Config{DB: db, DBName: dbName, Listen: "127.0.0.1:8090", Token: intToken, BaselineDir: baseDir})
	if err != nil {
		t.Fatal(err)
	}
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	srv.sqlRunner = sandboxRunner{sqlsandbox.New(sqlsandbox.Config{Exe: exe, Args: []string{}, Limits: sqlsandbox.Limits{Timeout: 60 * time.Second}})}

	rec, body := doReq(t, srv, "POST", "/api/sql", `{"sql":"SELECT id, status FROM shop.orders ORDER BY id"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("code=%d body=%s", rec.Code, body)
	}
	var resp sqlResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatal(err)
	}
	if len(resp.Rows) != 2 || len(resp.Columns) != 2 || resp.Columns[0].Name != "id" {
		t.Errorf("response = %+v", resp)
	}
	if got := string(body); !strings.Contains(got, `[1,"new"]`) || !strings.Contains(got, `[2,"paid"]`) {
		t.Errorf("rows in body: %s", got)
	}
	if resp.CopyUpdatedAt == nil || !resp.CopyUpdatedAt.Equal(sqlSnapshotAt) {
		t.Errorf("copy_updated_at = %v, want %v", resp.CopyUpdatedAt, sqlSnapshotAt)
	}

	rec, body = doReq(t, srv, "POST", "/api/sql?format=csv", `{"sql":"SELECT id, status FROM shop.orders ORDER BY id"}`)
	if rec.Code != http.StatusOK || !strings.HasPrefix(rec.Header().Get("Content-Type"), "text/csv") {
		t.Fatalf("csv: code=%d type=%s body=%s", rec.Code, rec.Header().Get("Content-Type"), body)
	}
	if got := string(body); got != "id,status\r\n1,new\r\n2,paid" {
		t.Errorf("csv = %q", got)
	}

	// The sandbox's own refusal reaches the client as a 422 with DuckDB's
	// words, and the daemon is fine afterwards.
	rec, body = doReq(t, srv, "POST", "/api/sql", `{"sql":"SELECT * FROM read_csv('/etc/passwd')"}`)
	if rec.Code != http.StatusUnprocessableEntity || !strings.Contains(string(body), "Permission Error") {
		t.Errorf("outside read: code=%d body=%s", rec.Code, body)
	}
	rec, body = doReq(t, srv, "POST", "/api/sql", `{"sql":"SELECT 1; SELECT 2"}`)
	if rec.Code != http.StatusUnprocessableEntity || !strings.Contains(string(body), "one statement") {
		t.Errorf("two statements: code=%d body=%s", rec.Code, body)
	}
	if rec, _ := doReq(t, srv, "POST", "/api/sql", `{"sql":"SELECT count(*) AS n FROM shop.orders"}`); rec.Code != http.StatusOK {
		t.Errorf("after the refusals: code=%d", rec.Code)
	}
}
