//go:build integration

package console

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/ext"
	"github.com/dbtrail/dbtrail/internal/audittest"
	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// #1916: Time-travel runs the destructive-DDL check (#764, anchored by #1912)
// and the capture-gap check (#765) that the command line, the MCP tool and
// the shim's _snapshot already run.
//
// Every window in these fixtures is ALSO a coverage gap (InitIndexTables
// creates only p_future), so a request without allow_gaps is refused by the
// fetch whatever these checks do. Each refusal below is therefore attributed
// by words only its own check produces, and never by the status code alone.

// secretToken sits in every seeded statement's text. The refusal names the
// statement's type, table, time and position, never its text: under a data
// profile that text could name tables the profile hides.
const secretToken = "zq_secret_tbl"

func execDDLRow(t *testing.T, srv *Server, detectedAt, file string, pos uint64, schema, table, ddlType string) {
	t.Helper()
	if _, err := srv.cm.boot.db.Exec(`INSERT INTO schema_changes
		(detected_at, binlog_file, binlog_pos, schema_name, table_name, ddl_type, ddl_query)
		VALUES (?, ?, ?, ?, ?, ?, ?)`,
		detectedAt, file, pos, schema, table, ddlType, ddlType+" "+table+" /* "+secretToken+" */"); err != nil {
		t.Fatal(err)
	}
}

func setStreamState(t *testing.T, srv *Server, gapAt, detail string) {
	t.Helper()
	var at any
	if gapAt != "" {
		at = gapAt
	}
	if _, err := srv.cm.boot.db.Exec(`INSERT INTO stream_state
		(id, mode, binlog_file, binlog_position, last_checkpoint, server_id, gap_lost_at, gap_lost_detail)
		VALUES (1, 'position', 'bin.000001', 400, NOW(), 7, ?, ?)`, at, detail); err != nil {
		t.Fatal(err)
	}
}

var reconstructModes = []string{"", "&history=true"}

// reconstructRefused asserts a 422 whose body carries every want and none of
// the coverage-gap refusal's words, and returns the message for reading.
func reconstructRefused(t *testing.T, srv *Server, qs string, want ...string) string {
	t.Helper()
	rec, body := doReq(t, srv, "GET", "/api/reconstruct?"+qs, "")
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("%s: code=%d, want 422 (body=%s)", qs, rec.Code, body)
	}
	var e struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal(body, &e); err != nil {
		t.Fatalf("%s: decode: %v (body=%s)", qs, err, body)
	}
	for _, w := range want {
		if !strings.Contains(e.Error, w) {
			t.Errorf("%s: refusal missing %q: %s", qs, w, e.Error)
		}
	}
	if strings.Contains(e.Error, "archive reconcile") {
		t.Errorf("%s: refused by the coverage-gap check, not the one under test: %s", qs, e.Error)
	}
	if strings.Contains(e.Error, secretToken) {
		t.Errorf("%s: the refusal carries the statement's text: %s", qs, e.Error)
	}
	if strings.Contains(e.Error, "\u2014") {
		t.Errorf("%s: the refusal carries an em dash: %s", qs, e.Error)
	}
	return e.Error
}

func hasWarningPrefix(ws []string, prefix string) bool {
	for _, w := range ws {
		if strings.HasPrefix(w, prefix) {
			return true
		}
	}
	return false
}

// A TRUNCATE between the snapshot and the requested time refuses in both
// modes, and allow_gaps does not cover it: the CLI's --allow-gaps and the MCP
// tool's allow_gaps do not either. A fold across it is not incomplete, it is
// wrong (the rows the statement removed read as present), and the way out is
// a newer snapshot, not an override.
func TestIntegrationReconstruct1916TruncateInWindowRefuses(t *testing.T) {
	srv := seedReconstruct(t)
	execDDLRow(t, srv, "2026-06-01 12:30:00", "bin.000001", 130, "app", "users", "TRUNCATE TABLE")
	for _, mode := range reconstructModes {
		for _, gaps := range []string{"", "&allow_gaps=true"} {
			msg := reconstructRefused(t, srv, "schema=app&table=users&pk=1&at=2026-06-01%2013:30:00"+mode+gaps,
				"TRUNCATE TABLE", "app.users", "2026-06-01T12:30:00Z", "bin.000001:130", "new snapshot")
			t.Logf("refusal (mode=%q gaps=%q): %s", mode, gaps, msg)
		}
	}
	// At the very second of the statement: the window's end is inclusive.
	reconstructRefused(t, srv, "schema=app&table=users&pk=1&at=2026-06-01%2012:30:00&allow_gaps=true", "TRUNCATE TABLE")
	// Before the statement: nothing in the window, the answer is as before.
	r := reconstructAt(t, srv, "schema=app&table=users&pk=1&at=2026-06-01%2012:10:00&allow_gaps=true")
	if r.State["name"] != "alicia" {
		t.Errorf("before the TRUNCATE: name=%v, want alicia", r.State["name"])
	}
}

// Every destructive kind the shared check names, not only TRUNCATE.
func TestIntegrationReconstruct1916DropInWindowRefuses(t *testing.T) {
	srv := seedReconstruct(t)
	execDDLRow(t, srv, "2026-06-01 12:30:00", "bin.000001", 130, "app", "users", "DROP TABLE")
	reconstructRefused(t, srv, "schema=app&table=users&pk=1&at=2026-06-01%2013:30:00&allow_gaps=true", "DROP TABLE", "app.users")
}

// A TRUNCATE of ANOTHER table is not this row's business.
func TestIntegrationReconstruct1916OtherTableDoesNotRefuse(t *testing.T) {
	srv := seedReconstruct(t)
	execDDLRow(t, srv, "2026-06-01 12:30:00", "bin.000001", 130, "app", "orders", "TRUNCATE TABLE")
	execDDLRow(t, srv, "2026-06-01 12:31:00", "bin.000001", 131, "other", "users", "TRUNCATE TABLE")
	for _, mode := range reconstructModes {
		reconstructAt(t, srv, "schema=app&table=users&pk=1&at=2026-06-01%2013:30:00&allow_gaps=true"+mode)
	}
}

// A time before the only snapshot still answers "no snapshot" (404), as it
// did: the checks run after a snapshot is found, never instead of that answer.
func TestIntegrationReconstruct1916BeforeSnapshotStillNotFound(t *testing.T) {
	srv := seedReconstruct(t)
	execDDLRow(t, srv, "2026-05-31 12:00:00", "bin.000001", 2, "app", "users", "TRUNCATE TABLE")
	rec, body := doReq(t, srv, "GET", "/api/reconstruct?schema=app&table=users&pk=1&at=2026-05-31%2013:00:00&allow_gaps=true", "")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("before the snapshot: code=%d, want 404 (body=%s)", rec.Code, body)
	}
}

// #1912 through this route: a TRUNCATE whose TIME is before the snapshot but
// whose binlog POSITION is after the snapshot's anchor was indexed late, and
// still belongs to the window. Only the anchor from the footer catches it;
// a check by time alone lets it pass.
func TestIntegrationReconstruct1916LateTruncatePlacedByAnchor(t *testing.T) {
	srv := seedReconstructMeta(t, map[string]string{
		baseline.MetaKeyBinlogFile: "bin.000001",
		baseline.MetaKeyBinlogPos:  "1000",
	})
	execDDLRow(t, srv, "2026-05-31 23:59:00", "bin.000001", 1500, "app", "users", "TRUNCATE TABLE")
	msg := reconstructRefused(t, srv, "schema=app&table=users&pk=1&at=2026-06-01%2013:30:00&allow_gaps=true",
		"TRUNCATE TABLE", "binlog position", "bin.000001:1000")
	t.Logf("late TRUNCATE refusal: %s", msg)

	// Ending exactly at the anchor is before the snapshot: no refusal.
	srv2 := seedReconstructMeta(t, map[string]string{
		baseline.MetaKeyBinlogFile: "bin.000001",
		baseline.MetaKeyBinlogPos:  "1000",
	})
	execDDLRow(t, srv2, "2026-05-31 23:59:00", "bin.000001", 1000, "app", "users", "TRUNCATE TABLE")
	reconstructAt(t, srv2, "schema=app&table=users&pk=1&at=2026-06-01%2013:30:00&allow_gaps=true")
}

// The footer's DDL mark (#1912): a row at or below it was already in the index
// when the snapshot was taken, so it is not placed by position alone.
func TestIntegrationReconstruct1916DDLMarkClearsPositionalRow(t *testing.T) {
	mark := reconstruct.DDLMark{ID: 1, File: "bin.000001", Pos: 1500,
		DetectedAt: time.Date(2026, 5, 31, 23, 59, 0, 0, time.UTC), Type: "TRUNCATE TABLE"}
	srv := seedReconstructMeta(t, map[string]string{
		baseline.MetaKeyBinlogFile: "bin.000001",
		baseline.MetaKeyBinlogPos:  "1000",
		baseline.MetaKeyDDLMark:    mark.Encode(),
	})
	execDDLRow(t, srv, "2026-05-31 23:59:00", "bin.000001", 1500, "app", "users", "TRUNCATE TABLE")
	r := reconstructAt(t, srv, "schema=app&table=users&pk=1&at=2026-06-01%2013:30:00&allow_gaps=true")
	if r.State["name"] != "alex" {
		t.Errorf("with the mark: name=%v, want alex", r.State["name"])
	}
}

// A stamped capture loss inside the window refuses without the override, in
// both modes; with it the answer is given and the finding travels as a
// capture_gap warning. An override never silences the finding.
func TestIntegrationReconstruct1916CaptureGap(t *testing.T) {
	srv := seedReconstruct(t)
	setStreamState(t, srv, "2026-06-01 12:30:00", "binlog.000042 was purged")
	for _, mode := range reconstructModes {
		msg := reconstructRefused(t, srv, "schema=app&table=users&pk=1&at=2026-06-01%2013:30:00"+mode,
			"2026-06-01 12:30:00", "binlog.000042 was purged", "Continue even if some history is missing")
		t.Logf("capture gap refusal (mode=%q): %s", mode, msg)
		r := reconstructAt(t, srv, "schema=app&table=users&pk=1&at=2026-06-01%2013:30:00&allow_gaps=true"+mode)
		if !hasWarningPrefix(r.Warnings, "capture_gap: ") {
			t.Errorf("mode=%q: allow_gaps must carry the finding as a capture_gap warning, got %v", mode, r.Warnings)
		}
		for _, w := range r.Warnings {
			if strings.HasPrefix(w, "capture_gap: ") {
				t.Logf("capture gap warning (mode=%q): %s", mode, w)
				if strings.Contains(w, "\u2014") || strings.Contains(w, "--") {
					t.Errorf("warning carries an em dash or a flag: %s", w)
				}
			}
		}
	}
	// A loss after the requested time is outside the window.
	r := reconstructAt(t, srv, "schema=app&table=users&pk=1&at=2026-06-01%2012:10:00&allow_gaps=true")
	if hasWarningPrefix(r.Warnings, "capture_gap: ") {
		t.Errorf("a loss after the requested time must not be reported: %v", r.Warnings)
	}
}

// An empty stream_state (an index built from files, no capture ever ran) is no
// loss; a row with no stamp is no loss either.
func TestIntegrationReconstruct1916NoCaptureGap(t *testing.T) {
	srv := seedReconstruct(t)
	r := reconstructAt(t, srv, "schema=app&table=users&pk=1&at=2026-06-01%2013:30:00&allow_gaps=true")
	if hasWarningPrefix(r.Warnings, "capture_gap: ") {
		t.Errorf("empty stream_state must not report a capture gap: %v", r.Warnings)
	}
	setStreamState(t, srv, "", "")
	r = reconstructAt(t, srv, "schema=app&table=users&pk=1&at=2026-06-01%2013:30:00&allow_gaps=true")
	if hasWarningPrefix(r.Warnings, "capture_gap: ") {
		t.Errorf("an unstamped stream_state must not report a capture gap: %v", r.Warnings)
	}
}

// An index older than the gap columns cannot say whether capture lost events:
// that is treated as a gap, as the MCP tool treats it.
func TestIntegrationReconstruct1916LegacyIndexCannotRuleOutLoss(t *testing.T) {
	srv := seedReconstruct(t)
	if _, err := srv.cm.boot.db.Exec(`ALTER TABLE stream_state DROP COLUMN gap_lost_at, DROP COLUMN gap_lost_detail`); err != nil {
		t.Fatal(err)
	}
	if _, err := srv.cm.boot.db.Exec(`INSERT INTO stream_state
		(id, mode, binlog_file, binlog_position, last_checkpoint, server_id)
		VALUES (1, 'position', 'bin.000001', 400, NOW(), 7)`); err != nil {
		t.Fatal(err)
	}
	msg := reconstructRefused(t, srv, "schema=app&table=users&pk=1&at=2026-06-01%2013:30:00", "cannot be ruled out")
	t.Logf("legacy index refusal: %s", msg)
	if strings.Contains(msg, "before the loss") {
		t.Errorf("no loss time is known on a legacy index, so the advice must not name one: %s", msg)
	}
	r := reconstructAt(t, srv, "schema=app&table=users&pk=1&at=2026-06-01%2013:30:00&allow_gaps=true")
	if !hasWarningPrefix(r.Warnings, "capture_gap: ") {
		t.Errorf("legacy index under allow_gaps must warn: %v", r.Warnings)
	}
}

// A snapshot whose footer cannot be read refuses, as the MCP tool does:
// without the anchor a TRUNCATE indexed late would pass as outside the window.
// The row read of the same file fails too, so this is attributed by the
// footer's own words, not by the 500.
func TestIntegrationReconstruct1916UnreadableFooterRefuses(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	testutil.InsertSnapshot(t, db, 1, "2026-06-01 00:00:00", "app", "users", "id", 1, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, db, 1, "2026-06-01 00:00:00", "app", "users", "name", 2, "", "varchar", "YES")
	baseDir := t.TempDir()
	dir := filepath.Join(baseDir, "2026-06-01T00-00-00Z", "app")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "users.parquet"), []byte("not a parquet file"), 0o644); err != nil {
		t.Fatal(err)
	}
	srv, err := New(Config{DB: db, DBName: dbName, Listen: "127.0.0.1:8090", Token: intToken, BaselineDir: baseDir})
	if err != nil {
		t.Fatal(err)
	}
	rec, body := doReq(t, srv, "GET", "/api/reconstruct?schema=app&table=users&pk=1&at=2026-06-01%2013:30:00&allow_gaps=true", "")
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("unreadable footer: code=%d, want 500 (body=%s)", rec.Code, body)
	}
	if !strings.Contains(string(body), "TRUNCATE") || !strings.Contains(string(body), "footer") {
		t.Errorf("unreadable footer must say why it stops: %s", body)
	}
	t.Logf("unreadable footer refusal: %s", body)
}

// A session with a data profile is refused before either check runs, so the
// finding (and anything about the statement) never reaches it.
func TestIntegrationReconstruct1916ProfiledSessionSeesNoFinding(t *testing.T) {
	srv := seedReconstruct(t)
	execDDLRow(t, srv, "2026-06-01 12:30:00", "bin.000001", 130, "app", "users", "TRUNCATE TABLE")
	setStreamState(t, srv, "2026-06-01 12:30:00", "binlog.000042 was purged")
	req := httptest.NewRequest("GET", "/api/reconstruct?schema=app&table=users&pk=1&at=2026-06-01%2013:30:00&allow_gaps=true", nil)
	req = req.WithContext(context.WithValue(req.Context(), policyCtxKey{}, &ext.AccessPolicy{Profile: "analyst"}))
	w := httptest.NewRecorder()
	srv.handleReconstruct(w, req)
	body := w.Body.String()
	if w.Code != http.StatusForbidden {
		t.Fatalf("profiled session: code=%d, want 403 (body=%s)", w.Code, body)
	}
	for _, leak := range []string{"TRUNCATE", secretToken, "bin.000001", "purged"} {
		if strings.Contains(body, leak) {
			t.Errorf("profiled session saw %q: %s", leak, body)
		}
	}
}

// The check runs against the SELECTED server's index: a TRUNCATE on the second
// server does not refuse the first, and does refuse the second.
func TestIntegrationReconstruct1916PerServer(t *testing.T) {
	srv := seedReconstruct(t)
	t.Cleanup(srv.cm.CloseAll)

	db2, dbName2 := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db2)
	testutil.InsertSnapshot(t, db2, 1, "2026-06-01 00:00:00", "app", "users", "id", 1, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, db2, 1, "2026-06-01 00:00:00", "app", "users", "name", 2, "", "varchar", "YES")
	if _, err := db2.Exec(`INSERT INTO schema_changes
		(detected_at, binlog_file, binlog_pos, schema_name, table_name, ddl_type, ddl_query)
		VALUES ('2026-06-01 12:30:00', 'bin.000001', 130, 'app', 'users', 'TRUNCATE TABLE', 'TRUNCATE TABLE users')`); err != nil {
		t.Fatal(err)
	}
	baseDir2 := t.TempDir()
	writeBaselineParquet(t, baseDir2, "app", "users", time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC), "1", "alice")

	rec, body := doReq(t, srv, "POST", "/api/servers",
		`{"name":"second","dsn":"`+testutil.IntegrationDSN(dbName2)+`","baseline_dir":"`+baseDir2+`"}`)
	if rec.Code != 201 {
		t.Fatalf("create server: code=%d body=%s", rec.Code, body)
	}
	var created serverDTO
	if err := json.Unmarshal(body, &created); err != nil {
		t.Fatal(err)
	}

	qs := "/api/reconstruct?schema=app&table=users&pk=1&at=2026-06-01%2013:30:00&allow_gaps=true"
	if rec, body := doReqOn(t, srv, "", "GET", qs, ""); rec.Code != 200 {
		t.Errorf("boot server (clean): code=%d, want 200 (body=%s)", rec.Code, body)
	}
	rec, body = doReqOn(t, srv, created.ID, "GET", qs, "")
	if rec.Code != http.StatusUnprocessableEntity || !strings.Contains(string(body), "TRUNCATE TABLE") {
		t.Errorf("second server (TRUNCATE): code=%d, want 422 naming the TRUNCATE (body=%s)", rec.Code, body)
	}
}

// Audit follows the other data refusals of this route (coverage gap, event
// cap): a refusal served no row data, so it records no reconstruct.run. An
// answer given under the override still does.
func TestIntegrationReconstruct1916Audit(t *testing.T) {
	srv := seedReconstruct(t)
	rec := audittest.Install(t)
	setStreamState(t, srv, "2026-06-01 12:30:00", "binlog.000042 was purged")
	reconstructRefused(t, srv, "schema=app&table=users&pk=1&at=2026-06-01%2013:30:00", "binlog.000042 was purged")
	for _, ev := range rec.Events() {
		if ev.Action == "reconstruct.run" {
			t.Errorf("a refusal must not record reconstruct.run: %+v", ev)
		}
	}
	reconstructAt(t, srv, "schema=app&table=users&pk=1&at=2026-06-01%2013:30:00&allow_gaps=true")
	n := 0
	for _, ev := range rec.Events() {
		if ev.Action == "reconstruct.run" {
			n++
		}
	}
	if n != 1 {
		t.Errorf("an answer under allow_gaps: %d reconstruct.run events, want 1", n)
	}
}
