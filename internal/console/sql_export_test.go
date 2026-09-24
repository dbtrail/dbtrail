package console

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/ext"
	"github.com/dbtrail/dbtrail/internal/audittest"
)

// stubSQLExporter records the last request and serves scripted state.
type stubSQLExporter struct {
	err   error
	last  *SQLExportRequest
	st    BaselineStatus
	dir   string
	ready bool
	// delivered records every SQLExportDelivered call as "<server>|<dir>".
	delivered []string
	staged    SQLExportStagingInfo
	// holds is the number of open holds; heldOnce counts every hold taken.
	holds, heldOnce int
	holdRefused     bool
}

func (s *stubSQLExporter) SQLExportDelivered(serverID, dir string) {
	s.delivered = append(s.delivered, serverID+"|"+dir)
}

// holds counts open holds; holdRefused scripts SQLExportHold to say no.
func (s *stubSQLExporter) SQLExportHold(string, string) (func(), bool) {
	if s.holdRefused {
		return func() {}, false
	}
	s.holds++
	s.heldOnce++
	released := false
	return func() {
		if !released {
			released = true
			s.holds--
		}
	}, true
}
func (s *stubSQLExporter) SQLExportStaged() SQLExportStagingInfo { return s.staged }

func (s *stubSQLExporter) TriggerSQLExport(req SQLExportRequest) error {
	s.last = &req
	return s.err
}
func (s *stubSQLExporter) SQLExportStatus(string) BaselineStatus {
	if s.st.State == "" {
		return BaselineStatus{State: "idle"}
	}
	return s.st
}
func (s *stubSQLExporter) SQLExportDir(string) (string, BaselineStatus, bool) {
	return s.dir, s.SQLExportStatus(""), s.ready
}

func newSQLExportServerWithDefault(t *testing.T, exp SQLExporter, baselineDir string) *Server {
	t.Helper()
	reg, err := LoadRegistry(t.TempDir() + "/console-servers.yaml")
	if err != nil {
		t.Fatal(err)
	}
	srv, err := New(Config{
		Listen: "127.0.0.1:8090", Token: "t", Registry: reg,
		MonitorCtrl: &stubMonitorCtrl{}, SQLExport: exp, BaselineDir: baselineDir,
	})
	if err != nil {
		t.Fatal(err)
	}
	return srv
}

func newSQLExportServer(t *testing.T, exp SQLExporter) *Server {
	t.Helper()
	reg, err := LoadRegistry(t.TempDir() + "/console-servers.yaml")
	if err != nil {
		t.Fatal(err)
	}
	srv, err := New(Config{
		Listen: "127.0.0.1:8090", Token: "t", Registry: reg,
		MonitorCtrl: &stubMonitorCtrl{}, SQLExport: exp,
	})
	if err != nil {
		t.Fatal(err)
	}
	return srv
}

func TestSQLExport_gates(t *testing.T) {
	// No exporter wired: all three verbs refuse (the standalone console).
	srvOff := newSQLExportServer(t, nil)
	idOff := addRestoreEntry(t, srvOff, t.TempDir())
	for _, probe := range []struct{ method, path string }{
		{"POST", "/api/servers/" + idOff + "/sql-export"},
		{"GET", "/api/servers/" + idOff + "/sql-export"},
		{"GET", "/api/servers/" + idOff + "/sql-export/download"},
	} {
		rec, body := doServersReq(t, srvOff, probe.method, probe.path, `{"at":"2026-06-10 12:00:00"}`)
		if rec.Code != 403 {
			t.Fatalf("%s %s with no exporter: code=%d body=%s, want 403", probe.method, probe.path, rec.Code, body)
		}
	}

	stub := &stubSQLExporter{}
	srv := newSQLExportServer(t, stub)

	// No baseline source at all: nothing to fold from.
	bare, err := srv.cm.reg.Add(ServerEntry{Name: "bare", DSN: "i:p@tcp(h:3306)/idx"})
	if err != nil {
		t.Fatal(err)
	}
	rec, body := doServersReq(t, srv, "POST", "/api/servers/"+bare.ID+"/sql-export", `{"at":"2026-06-10 12:00:00"}`)
	if rec.Code != 400 || !strings.Contains(string(body), "snapshot location") {
		t.Fatalf("no baseline source: code=%d body=%s, want 400 naming the missing snapshot location", rec.Code, body)
	}

	id := addRestoreEntry(t, srv, "/var/lib/dbtrail/baselines")

	rec, body = doServersReq(t, srv, "POST", "/api/servers/"+id+"/sql-export", `{"at":"lunes"}`)
	if rec.Code != 400 {
		t.Fatalf("bad at: code=%d body=%s, want 400", rec.Code, body)
	}
	rec, body = doServersReq(t, srv, "POST", "/api/servers/"+id+"/sql-export", `{"at":"2999-01-01 00:00:00"}`)
	if rec.Code != 400 || !strings.Contains(string(body), "future") {
		t.Fatalf("future at: code=%d body=%s, want 400 naming the future", rec.Code, body)
	}

	stub.err = ErrBaselineRunning
	rec, body = doServersReq(t, srv, "POST", "/api/servers/"+id+"/sql-export", `{"at":"2026-06-10 12:00:00"}`)
	if rec.Code != 409 {
		t.Fatalf("busy: code=%d body=%s, want 409", rec.Code, body)
	}
	stub.err = errors.New("staging disk full")
	rec, body = doServersReq(t, srv, "POST", "/api/servers/"+id+"/sql-export", `{"at":"2026-06-10 12:00:00"}`)
	if rec.Code != 500 {
		t.Fatalf("other trigger error: code=%d body=%s, want 500", rec.Code, body)
	}
	stub.err = nil

	// Happy path: the request carries the entry's own connection facts, the
	// local directory wins when both sources are set, and At arrives parsed.
	rec, body = doServersReq(t, srv, "POST", "/api/servers/"+id+"/sql-export", `{"at":"2026-06-10 12:00:00"}`)
	if rec.Code != 202 {
		t.Fatalf("happy: code=%d body=%s, want 202", rec.Code, body)
	}
	if stub.last == nil {
		t.Fatal("trigger never reached the exporter")
	}
	want := time.Date(2026, 6, 10, 12, 0, 0, 0, time.UTC)
	if stub.last.ServerID != id || stub.last.IndexDSN == "" || !stub.last.At.Equal(want) {
		t.Fatalf("request = %+v, want server %s with its DSN and at=%s", stub.last, id, want)
	}
	if stub.last.BaselineSrc != "/var/lib/dbtrail/baselines" {
		t.Fatalf("BaselineSrc = %q, want the local directory", stub.last.BaselineSrc)
	}
	var resp struct {
		Status BaselineStatus `json:"sql_export"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatal(err)
	}

	// An entry with no baseline of its own inherits the process-wide
	// default (#1010): the Backups listing the card gates on applies the
	// same fallback, so the trigger must accept it — refusing here made
	// every Build click 400 on the shipped compose deployment.
	srvDef := newSQLExportServerWithDefault(t, stub, "/proc-wide/baselines")
	bareDef, err := srvDef.cm.reg.Add(ServerEntry{Name: "bare2", DSN: "i:p@tcp(h:3306)/idx"})
	if err != nil {
		t.Fatal(err)
	}
	rec, body = doServersReq(t, srvDef, "POST", "/api/servers/"+bareDef.ID+"/sql-export", `{"at":"2026-06-10 12:00:00"}`)
	if rec.Code != 202 || stub.last.BaselineSrc != "/proc-wide/baselines" {
		t.Fatalf("inherited default: code=%d body=%s src=%q, want 202 with the process-wide dir", rec.Code, body, stub.last.BaselineSrc)
	}

	// Unlike the point-in-time restore, an S3-only backup store qualifies:
	// the fold engine reads s3:// sources directly.
	s3only, err := srv.cm.reg.Add(ServerEntry{Name: "s3only", DSN: "i:p@tcp(h:3306)/idx",
		BaselineS3: "s3://bkt/baselines"})
	if err != nil {
		t.Fatal(err)
	}
	rec, body = doServersReq(t, srv, "POST", "/api/servers/"+s3only.ID+"/sql-export", `{"at":"2026-06-10 12:00:00"}`)
	if rec.Code != 202 || stub.last.BaselineSrc != "s3://bkt/baselines" {
		t.Fatalf("s3-only: code=%d body=%s src=%q, want 202 with the s3 source", rec.Code, body, stub.last.BaselineSrc)
	}
}

func TestSQLExportStatus_reportsExporterState(t *testing.T) {
	stub := &stubSQLExporter{st: BaselineStatus{State: "running", At: "2026-06-10T12:00:00Z"}}
	srv := newSQLExportServer(t, stub)
	id := addRestoreEntry(t, srv, t.TempDir())
	rec, body := doServersReq(t, srv, "GET", "/api/servers/"+id+"/sql-export", "")
	if rec.Code != 200 {
		t.Fatalf("code=%d body=%s", rec.Code, body)
	}
	var resp struct {
		Status BaselineStatus `json:"sql_export"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Status.State != "running" || resp.Status.At != "2026-06-10T12:00:00Z" {
		t.Fatalf("status = %+v", resp.Status)
	}
}

func TestSQLExportDownload_notReady(t *testing.T) {
	stub := &stubSQLExporter{ready: false, st: BaselineStatus{State: "running"}}
	srv := newSQLExportServer(t, stub)
	id := addRestoreEntry(t, srv, t.TempDir())
	rec, body := doServersReq(t, srv, "GET", "/api/servers/"+id+"/sql-export/download", "")
	if rec.Code != 409 || !strings.Contains(string(body), "build one first") {
		t.Fatalf("code=%d body=%s, want 409 telling the operator to build first", rec.Code, body)
	}
}

// newSQLDumpFixture lays out a finished mydumper-format build with BOTH
// completeness markers present, so the round trip proves the per-name skip
// for each. (They can coexist live: WriteSuccessMarker's removal of a stale
// _INCOMPLETE is best-effort.)
func newSQLDumpFixture(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	files := map[string]string{
		"metadata":               "Started dump at: 2026-06-10 12:00:00",
		"shop.orders-schema.sql": "CREATE TABLE `orders` (id INT PRIMARY KEY);",
		"shop.orders.00000.sql":  "INSERT INTO `orders` VALUES (1);",
		"_SUCCESS":               "",
		"_INCOMPLETE":            "",
	}
	for name, content := range files {
		if err := os.WriteFile(dir+"/"+name, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

func TestSQLExportDownload_roundTrip(t *testing.T) {
	rec := audittest.Install(t)
	dir := newSQLDumpFixture(t)
	stub := &stubSQLExporter{dir: dir, ready: true,
		st: BaselineStatus{State: "succeeded", At: "2026-06-10T12:00:00Z"}}
	srv := newSQLExportServer(t, stub)
	id := addRestoreEntry(t, srv, t.TempDir())

	w, body := doServersReq(t, srv, "GET", "/api/servers/"+id+"/sql-export/download", "")
	if w.Code != 200 {
		t.Fatalf("code=%d body=%s", w.Code, body)
	}
	if ct := w.Header().Get("Content-Type"); ct != "application/gzip" {
		t.Fatalf("content-type = %q", ct)
	}
	if cd := w.Header().Get("Content-Disposition"); !strings.Contains(cd, `dbtrail-sql-2026-06-10T12-00-00Z.tar.gz`) {
		t.Fatalf("content-disposition = %q", cd)
	}
	got := untarAll(t, body)
	// The markers describe the BUILD, not the dump: myloader must never see
	// them, so the archive holds exactly the loadable files.
	want := map[string]string{
		"dbtrail-sql-2026-06-10T12-00-00Z/metadata":               "Started dump at: 2026-06-10 12:00:00",
		"dbtrail-sql-2026-06-10T12-00-00Z/shop.orders-schema.sql": "CREATE TABLE `orders` (id INT PRIMARY KEY);",
		"dbtrail-sql-2026-06-10T12-00-00Z/shop.orders.00000.sql":  "INSERT INTO `orders` VALUES (1);",
	}
	if len(got) != len(want) {
		t.Fatalf("entries = %v, want %v (markers excluded)", keysOf(got), keysOf(want))
	}
	for name, content := range want {
		if got[name] != content {
			t.Fatalf("entry %s = %q, want %q", name, got[name], content)
		}
	}
	var ev *ext.AuditEvent
	for _, e := range rec.Events() {
		if e.Action == "baseline.download" {
			c := e
			ev = &c
		}
	}
	if ev == nil {
		t.Fatal("a completed download must be audited")
	}
	if ev.Detail["format"] != "sql" || ev.Detail["files"] != "3" || ev.Detail["aborted"] != "" ||
		ev.Detail["at"] != "2026-06-10T12:00:00Z" {
		t.Fatalf("audit detail = %v, want format=sql, 3 files, the instant, and no aborted flag", ev.Detail)
	}
	// The whole archive left: the staged copy is consumed (#1448), and the
	// exporter is told WHICH build so a replacement is never removed for it.
	if len(stub.delivered) != 1 || stub.delivered[0] != id+"|"+dir {
		t.Fatalf("delivered = %v, want exactly [%s|%s]", stub.delivered, id, dir)
	}
	// The stream held the build (so no deadline could remove it underneath)
	// and let go before reporting the delivery.
	if stub.heldOnce != 1 || stub.holds != 0 {
		t.Fatalf("holds: taken %d, open %d; want one taken and released", stub.heldOnce, stub.holds)
	}
}

func TestSQLExportDownload_emptyBuild(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(dir+"/_SUCCESS", nil, 0o644); err != nil {
		t.Fatal(err)
	}
	stub := &stubSQLExporter{dir: dir, ready: true, st: BaselineStatus{State: "succeeded"}}
	srv := newSQLExportServer(t, stub)
	id := addRestoreEntry(t, srv, t.TempDir())
	rec, body := doServersReq(t, srv, "GET", "/api/servers/"+id+"/sql-export/download", "")
	if rec.Code != 409 {
		t.Fatalf("marker-only build: code=%d body=%s, want 409", rec.Code, body)
	}
}

func TestSQLExportTrigger_profileRefusal(t *testing.T) {
	stub := &stubSQLExporter{}
	srv := newSQLExportServer(t, stub)
	id := addRestoreEntry(t, srv, t.TempDir())
	pol := &ext.AccessPolicy{Profile: "sensitive"}
	req := httptest.NewRequest("POST", "/api/servers/"+id+"/sql-export",
		strings.NewReader(`{"at":"2026-06-10 12:00:00"}`))
	req.SetPathValue("id", id)
	req = req.WithContext(context.WithValue(req.Context(), policyCtxKey{}, pol))
	w := httptest.NewRecorder()
	srv.handleSQLExportTrigger(w, req)
	if w.Code != 403 {
		t.Fatalf("code = %d body = %s, want 403 under a data profile (the build writes unredacted rows)", w.Code, w.Body.String())
	}
	if stub.last != nil {
		t.Fatal("a refused trigger must never reach the exporter")
	}
}

func TestSQLExportDownload_profileRefusal(t *testing.T) {
	stub := &stubSQLExporter{dir: newSQLDumpFixture(t), ready: true,
		st: BaselineStatus{State: "succeeded"}}
	srv := newSQLExportServer(t, stub)
	id := addRestoreEntry(t, srv, t.TempDir())
	pol := &ext.AccessPolicy{Profile: "sensitive"}
	req := httptest.NewRequest("GET", "/api/servers/"+id+"/sql-export/download", nil)
	req.SetPathValue("id", id)
	req = req.WithContext(context.WithValue(req.Context(), policyCtxKey{}, pol))
	w := httptest.NewRecorder()
	srv.handleSQLExportDownload(w, req)
	if w.Code != 403 {
		t.Fatalf("code = %d body = %s, want 403 under a data profile (the dump bypasses redaction)", w.Code, w.Body.String())
	}
}

// TestSQLExportDownload_midStreamAbort pins the abort contract on the sql
// download: an unreadable file mid-archive panics with http.ErrAbortHandler
// (a truncated tar.gz must never end as an apparent success) and the audit
// still records the egress that already happened.
func TestSQLExportDownload_midStreamAbort(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("mode-000 files stay readable to root")
	}
	rec := audittest.Install(t)
	dir := t.TempDir()
	if err := os.WriteFile(dir+"/a.sql", []byte("aaaa"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dir+"/b.sql", []byte("bbbb"), 0o000); err != nil {
		t.Fatal(err)
	}
	stub := &stubSQLExporter{dir: dir, ready: true,
		st: BaselineStatus{State: "succeeded", At: "2026-06-10T12:00:00Z"}}
	srv := newSQLExportServer(t, stub)
	id := addRestoreEntry(t, srv, t.TempDir())

	req := httptest.NewRequest("GET", "/api/servers/"+id+"/sql-export/download", nil)
	req.SetPathValue("id", id)
	w := httptest.NewRecorder()
	panicked := false
	func() {
		defer func() {
			if r := recover(); r != nil {
				panicked = true
				if r != http.ErrAbortHandler {
					t.Fatalf("panic = %v, want http.ErrAbortHandler", r)
				}
			}
		}()
		srv.handleSQLExportDownload(w, req)
	}()
	if !panicked {
		t.Fatal("an unreadable file mid-archive must abort the connection, not end the body cleanly")
	}
	var ev *ext.AuditEvent
	for _, e := range rec.Events() {
		if e.Action == "baseline.download" {
			c := e
			ev = &c
		}
	}
	if ev == nil {
		t.Fatal("an aborted download must still be audited")
	}
	if ev.Detail["aborted"] != "true" || ev.Detail["bytes"] != "4" || ev.Detail["files"] != "1" {
		t.Fatalf("audit detail = %v, want aborted=true with the 4 bytes and 1 file that left", ev.Detail)
	}
	// An aborted stream delivered nothing: the build must stay for a retry.
	if len(stub.delivered) != 0 {
		t.Fatalf("delivered = %v after an aborted stream, want none", stub.delivered)
	}
}

// TestSQLExportDownload_unexpectedSubdir: the engine writes a flat dump; a
// subdirectory means the layout changed under this handler, and silently
// skipping it would ship an archive missing whatever it holds.
func TestSQLExportDownload_unexpectedSubdir(t *testing.T) {
	dir := newSQLDumpFixture(t)
	if err := os.Mkdir(dir+"/extra", 0o755); err != nil {
		t.Fatal(err)
	}
	stub := &stubSQLExporter{dir: dir, ready: true,
		st: BaselineStatus{State: "succeeded", At: "2026-06-10T12:00:00Z"}}
	srv := newSQLExportServer(t, stub)
	id := addRestoreEntry(t, srv, t.TempDir())
	rec, body := doServersReq(t, srv, "GET", "/api/servers/"+id+"/sql-export/download", "")
	if rec.Code != 500 || !strings.Contains(string(body), "unexpected subdirectory") {
		t.Fatalf("code=%d body=%s, want a loud 500 naming the subdirectory", rec.Code, body)
	}
}

// TestSQLExportDownload_markerVanishedMidStream pins the post-stream belt:
// a rebuild's teardown racing the stream can hand ReadDir a subset whose
// every surviving file streams cleanly — the per-file guards cannot see it,
// so the handler re-checks _SUCCESS after the last byte and aborts if the
// build was replaced under it.
func TestSQLExportDownload_markerVanishedMidStream(t *testing.T) {
	rec := audittest.Install(t)
	dir := t.TempDir()
	if err := os.WriteFile(dir+"/a.sql", []byte("aaaa"), 0o644); err != nil {
		t.Fatal(err)
	}
	// No _SUCCESS in the dir: from the handler's viewpoint this is exactly
	// the mid-stream shape (the supervisor said ready; the marker is gone by
	// the time the stream ends).
	stub := &stubSQLExporter{dir: dir, ready: true,
		st: BaselineStatus{State: "succeeded", At: "2026-06-10T12:00:00Z"}}
	srv := newSQLExportServer(t, stub)
	id := addRestoreEntry(t, srv, t.TempDir())

	req := httptest.NewRequest("GET", "/api/servers/"+id+"/sql-export/download", nil)
	req.SetPathValue("id", id)
	w := httptest.NewRecorder()
	panicked := false
	func() {
		defer func() {
			if r := recover(); r != nil {
				panicked = true
				if r != http.ErrAbortHandler {
					t.Fatalf("panic = %v, want http.ErrAbortHandler", r)
				}
			}
		}()
		srv.handleSQLExportDownload(w, req)
	}()
	if !panicked {
		t.Fatal("a vanished _SUCCESS after streaming must abort, not declare the archive whole")
	}
	var ev *ext.AuditEvent
	for _, e := range rec.Events() {
		if e.Action == "baseline.download" {
			c := e
			ev = &c
		}
	}
	if ev == nil || ev.Detail["aborted"] != "true" {
		t.Fatalf("audit = %v, want the aborted handover recorded", ev)
	}
	if len(stub.delivered) != 0 {
		t.Fatalf("delivered = %v after a replaced build, want none", stub.delivered)
	}
}

// TestCapabilitySQLExportGate: only a daemon with the exporter wired
// advertises sql_export.
func TestCapabilitySQLExportGate(t *testing.T) {
	for _, enabled := range []bool{true, false} {
		reg, _ := LoadRegistry("")
		cfg := Config{Listen: "127.0.0.1:8090", Token: "t", Registry: reg}
		if enabled {
			cfg.SQLExport = &stubSQLExporter{}
		}
		srv, err := New(cfg)
		if err != nil {
			t.Fatal(err)
		}
		srv.cm.boot = &bundle{}
		rec, body := doServersReq(t, srv, "GET", "/api/capabilities", "")
		if rec.Code != 200 {
			t.Fatalf("capabilities: code=%d body=%s", rec.Code, body)
		}
		var caps capabilitiesResponse
		if err := json.Unmarshal(body, &caps); err != nil {
			t.Fatal(err)
		}
		if caps.SQLExport != enabled {
			t.Errorf("sql_export capability = %v, want %v", caps.SQLExport, enabled)
		}
	}
}

// flushErrWriter is a ResponseWriter whose flush reports a broken
// connection, the shape net/http shows when the client vanished during the
// buffered tail of the body.
type flushErrWriter struct {
	*httptest.ResponseRecorder
	err error
}

func (w *flushErrWriter) FlushError() error { return w.err }

// TestSQLExportDownload_flushFailureAborts: the build is consumed only once
// the last bytes were FLUSHED to the socket. A flush that fails after every
// byte was written must abort, audit as aborted, deliver nothing and drop
// the hold, so the build stays for a retry.
func TestSQLExportDownload_flushFailureAborts(t *testing.T) {
	rec := audittest.Install(t)
	dir := newSQLDumpFixture(t)
	stub := &stubSQLExporter{dir: dir, ready: true,
		st: BaselineStatus{State: "succeeded", At: "2026-06-10T12:00:00Z"}}
	srv := newSQLExportServer(t, stub)
	id := addRestoreEntry(t, srv, t.TempDir())

	req := httptest.NewRequest("GET", "/api/servers/"+id+"/sql-export/download", nil)
	req.SetPathValue("id", id)
	w := &flushErrWriter{ResponseRecorder: httptest.NewRecorder(), err: errors.New("write: broken pipe")}
	panicked := false
	func() {
		defer func() {
			if r := recover(); r != nil {
				panicked = true
				if r != http.ErrAbortHandler {
					t.Fatalf("panic = %v, want http.ErrAbortHandler", r)
				}
			}
		}()
		srv.handleSQLExportDownload(w, req)
	}()
	if !panicked {
		t.Fatal("a failed flush after the last byte must abort, not declare the archive delivered")
	}
	if len(stub.delivered) != 0 {
		t.Fatalf("delivered = %v after a failed flush, want none", stub.delivered)
	}
	if stub.heldOnce != 1 || stub.holds != 0 {
		t.Fatalf("holds: taken %d, open %d; want one taken and released on abort", stub.heldOnce, stub.holds)
	}
	var ev *ext.AuditEvent
	for _, e := range rec.Events() {
		if e.Action == "baseline.download" {
			c := e
			ev = &c
		}
	}
	if ev == nil || ev.Detail["aborted"] != "true" {
		t.Fatalf("audit = %v, want the aborted handover recorded", ev)
	}
}

// bareWriter is a ResponseWriter with neither Flush nor Unwrap: the shape a
// middleware wrapper takes when it forwards neither, which leaves
// http.ResponseController unable to reach the connection.
type bareWriter struct{ rec *httptest.ResponseRecorder }

func (w *bareWriter) Header() http.Header         { return w.rec.Header() }
func (w *bareWriter) Write(b []byte) (int, error) { return w.rec.Write(b) }
func (w *bareWriter) WriteHeader(code int)        { w.rec.WriteHeader(code) }

// TestSQLExportDownload_unflushableWriterKeepsTheBuild: a writer that
// cannot flush leaves "did the last bytes reach the socket" unanswerable,
// and an unanswerable delivery must not consume the build. The handler
// aborts, says which writer type broke the chain (a wiring bug, not a
// client that left), delivers nothing, and the build stays succeeded.
func TestSQLExportDownload_unflushableWriterKeepsTheBuild(t *testing.T) {
	rec := audittest.Install(t)
	var logs bytes.Buffer
	orig := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelError})))
	t.Cleanup(func() { slog.SetDefault(orig) })
	dir := newSQLDumpFixture(t)
	stub := &stubSQLExporter{dir: dir, ready: true,
		st: BaselineStatus{State: "succeeded", At: "2026-06-10T12:00:00Z"}}
	srv := newSQLExportServer(t, stub)
	id := addRestoreEntry(t, srv, t.TempDir())

	req := httptest.NewRequest("GET", "/api/servers/"+id+"/sql-export/download", nil)
	req.SetPathValue("id", id)
	w := &bareWriter{rec: httptest.NewRecorder()}
	panicked := false
	func() {
		defer func() {
			if r := recover(); r != nil {
				panicked = true
				if r != http.ErrAbortHandler {
					t.Fatalf("panic = %v, want http.ErrAbortHandler", r)
				}
			}
		}()
		srv.handleSQLExportDownload(w, req)
	}()
	if !panicked {
		t.Fatal("a delivery that could not be flushed must abort, not declare the archive delivered")
	}
	if len(stub.delivered) != 0 {
		t.Fatalf("delivered = %v over a writer that cannot flush, want none (the build must not be removed)", stub.delivered)
	}
	if st := stub.SQLExportStatus(""); st.State != "succeeded" {
		t.Fatalf("state = %s, want succeeded (never downloaded)", st.State)
	}
	if stub.heldOnce != 1 || stub.holds != 0 {
		t.Fatalf("holds: taken %d, open %d; want one taken and released on abort", stub.heldOnce, stub.holds)
	}
	if got := logs.String(); !strings.Contains(got, "level=ERROR") || !strings.Contains(got, "*console.bareWriter") {
		t.Fatalf("log = %q, want an ERROR naming the writer type", got)
	}
	var ev *ext.AuditEvent
	for _, e := range rec.Events() {
		if e.Action == "baseline.download" {
			c := e
			ev = &c
		}
	}
	if ev == nil || ev.Detail["aborted"] != "true" {
		t.Fatalf("audit = %v, want the aborted handover recorded", ev)
	}
}

// TestSQLExportDownload_holdRefused: a build that expired or was replaced
// between the readiness read and the hold is refused, not streamed.
func TestSQLExportDownload_holdRefused(t *testing.T) {
	dir := newSQLDumpFixture(t)
	stub := &stubSQLExporter{dir: dir, ready: true, holdRefused: true,
		st: BaselineStatus{State: "succeeded", At: "2026-06-10T12:00:00Z"}}
	srv := newSQLExportServer(t, stub)
	id := addRestoreEntry(t, srv, t.TempDir())
	w, body := doServersReq(t, srv, "GET", "/api/servers/"+id+"/sql-export/download", "")
	if w.Code != 409 {
		t.Fatalf("code=%d body=%s, want 409", w.Code, body)
	}
	if len(stub.delivered) != 0 {
		t.Fatalf("delivered = %v, want none", stub.delivered)
	}
}
