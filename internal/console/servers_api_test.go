package console

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/storage"
)

const secretPW = "s3cr3t-hunter2"

// newRegistryServer builds a Server over a file-backed registry (no boot
// entry) with the full middleware chain, for exercising the /api/servers CRUD
// surface end to end.
func newRegistryServer(t *testing.T) *Server {
	t.Helper()
	clearStores(t)
	reg, err := LoadRegistry(t.TempDir() + "/console-servers.yaml")
	if err != nil {
		t.Fatal(err)
	}
	srv, err := New(Config{Listen: "127.0.0.1:8090", Token: "t", Registry: reg})
	if err != nil {
		t.Fatal(err)
	}
	return srv
}

// TestIsUnknownDatabase: the probe distinguishes "server reachable but the
// named DB doesn't exist" (MySQL 1049) from any other failure, through wraps —
// this is what reclassifies a monitored source's pre-Start probe as pending
// rather than a hard connection error.
func TestIsUnknownDatabase(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"direct 1049", &mysql.MySQLError{Number: 1049, Message: "Unknown database 'x'"}, true},
		{"wrapped 1049", errors.New("failed to ping MySQL: "), false}, // placeholder, replaced below
		{"other mysql error", &mysql.MySQLError{Number: 1045, Message: "Access denied"}, false},
		{"plain error", errors.New("dial tcp: connection refused"), false},
		{"nil", nil, false},
	}
	// The wrapped case mirrors config.Connect's fmt.Errorf("...: %w", err).
	cases[1].err = errwrap(&mysql.MySQLError{Number: 1049, Message: "Unknown database 'bintrail_idx_x'"})
	cases[1].want = true
	for _, tc := range cases {
		if got := isUnknownDatabase(tc.err); got != tc.want {
			t.Errorf("%s: isUnknownDatabase = %v, want %v", tc.name, got, tc.want)
		}
	}
}

func errwrap(err error) error { return fmt.Errorf("failed to ping MySQL: %w", err) }

func doServersReq(t *testing.T, srv *Server, method, path, body string) (*httptest.ResponseRecorder, []byte) {
	t.Helper()
	var rdr *strings.Reader
	if body == "" {
		rdr = strings.NewReader("")
	} else {
		rdr = strings.NewReader(body)
	}
	req := httptest.NewRequest(method, "http://127.0.0.1:8090"+path, rdr)
	req.Host = "127.0.0.1:8090"
	req.Header.Set("Authorization", "Bearer t")
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	return rec, rec.Body.Bytes()
}

// TestServersAPINeverLeaksSecrets: list and get responses must not contain the
// password or any DSN string — the core masking invariant.
func TestServersAPINeverLeaksSecrets(t *testing.T) {
	srv := newRegistryServer(t)

	rec, body := doServersReq(t, srv, "POST", "/api/servers",
		`{"name":"prod","host":"10.0.0.5","port":"3306","user":"forensics","password":"`+secretPW+`","dbname":"binlog_index"}`)
	if rec.Code != 201 {
		t.Fatalf("create code = %d, body = %s", rec.Code, body)
	}
	if strings.Contains(string(body), secretPW) {
		t.Fatalf("create response leaked the password: %s", body)
	}
	var created serverDTO
	if err := json.Unmarshal(body, &created); err != nil {
		t.Fatal(err)
	}
	if !created.HasPassword {
		t.Error("has_password should be true")
	}
	if created.Host != "10.0.0.5" || created.User != "forensics" || created.DBName != "binlog_index" {
		t.Errorf("masked parts wrong: %+v", created)
	}

	for _, path := range []string{"/api/servers", "/api/servers/" + created.ID} {
		rec, body := doServersReq(t, srv, "GET", path, "")
		if rec.Code != 200 {
			t.Fatalf("%s code = %d", path, rec.Code)
		}
		if strings.Contains(string(body), secretPW) || strings.Contains(string(body), "@tcp(") {
			t.Errorf("%s leaked a secret or DSN string: %s", path, body)
		}
	}
}

// TestServersAPIKeepPassword: PUT with password omitted keeps the stored
// secret; "" clears it; a value replaces it. Asserted by re-parsing the STORED
// DSN, never via an echoed string (nothing echoes it).
func TestServersAPIKeepPassword(t *testing.T) {
	srv := newRegistryServer(t)
	_, body := doServersReq(t, srv, "POST", "/api/servers",
		`{"name":"prod","host":"h","user":"u","password":"`+secretPW+`","dbname":"db"}`)
	var created serverDTO
	if err := json.Unmarshal(body, &created); err != nil {
		t.Fatal(err)
	}
	storedPW := func() string {
		e, ok := srv.cm.reg.Get(created.ID)
		if !ok {
			t.Fatal("entry vanished")
		}
		cfg, err := mysql.ParseDSN(e.DSN)
		if err != nil {
			t.Fatal(err)
		}
		return cfg.Passwd
	}

	// Omitted password → keep.
	if rec, b := doServersReq(t, srv, "PUT", "/api/servers/"+created.ID,
		`{"name":"prod-renamed","host":"h","user":"u","dbname":"db"}`); rec.Code != 200 {
		t.Fatalf("rename code = %d, body = %s", rec.Code, b)
	}
	if pw := storedPW(); pw != secretPW {
		t.Errorf("omitted password must keep the stored one; got %q", pw)
	}

	// Explicit new value → replace.
	doServersReq(t, srv, "PUT", "/api/servers/"+created.ID,
		`{"name":"prod-renamed","host":"h","user":"u","password":"new-pw","dbname":"db"}`)
	if pw := storedPW(); pw != "new-pw" {
		t.Errorf("explicit password must replace; got %q", pw)
	}

	// Explicit "" → clear.
	doServersReq(t, srv, "PUT", "/api/servers/"+created.ID,
		`{"name":"prod-renamed","host":"h","user":"u","password":"","dbname":"db"}`)
	if pw := storedPW(); pw != "" {
		t.Errorf("empty-string password must clear; got %q", pw)
	}
}

func TestServersAPIValidation(t *testing.T) {
	srv := newRegistryServer(t)

	cases := []struct {
		name, body string
		wantCode   int
	}{
		{"missing dbname", `{"name":"x","host":"h","user":"u"}`, 400},
		{"missing host", `{"name":"x","user":"u","dbname":"db"}`, 400},
		{"missing user", `{"name":"x","host":"h","dbname":"db"}`, 400},
		{"missing name", `{"host":"h","user":"u","dbname":"db"}`, 400},
		{"dsn without dbname", `{"name":"x","dsn":"u:p@tcp(h:3306)/"}`, 400},
		{"invalid json", `{nope`, 400},
	}
	for _, tc := range cases {
		rec, body := doServersReq(t, srv, "POST", "/api/servers", tc.body)
		if rec.Code != tc.wantCode {
			t.Errorf("%s: code = %d, want %d (body=%s)", tc.name, rec.Code, tc.wantCode, body)
		}
	}

	// Duplicate name → 409.
	doServersReq(t, srv, "POST", "/api/servers", `{"name":"dup","host":"h","user":"u","dbname":"db"}`)
	rec, _ := doServersReq(t, srv, "POST", "/api/servers", `{"name":"dup","host":"h2","user":"u","dbname":"db"}`)
	if rec.Code != 409 {
		t.Errorf("duplicate name: code = %d, want 409", rec.Code)
	}

	// Unknown id → 404 on get/put/delete/test.
	for _, m := range []struct{ method, path string }{
		{"GET", "/api/servers/ffffffffffffffff"},
		{"PUT", "/api/servers/ffffffffffffffff"},
		{"DELETE", "/api/servers/ffffffffffffffff"},
		{"POST", "/api/servers/ffffffffffffffff/test"},
	} {
		rec, _ := doServersReq(t, srv, m.method, m.path, `{"name":"x","host":"h","user":"u","dbname":"db"}`)
		if rec.Code != 404 {
			t.Errorf("%s %s: code = %d, want 404", m.method, m.path, rec.Code)
		}
	}
}

// TestServersAPIEphemeralImmutable: the command-line (boot) entry shows up in
// the list but can never be edited or deleted.
func TestServersAPIEphemeralImmutable(t *testing.T) {
	db, mock, closeFn := newSQLMock(t)
	defer closeFn()
	_ = mock

	reg, err := LoadRegistry(t.TempDir() + "/console-servers.yaml")
	if err != nil {
		t.Fatal(err)
	}
	srv, err := New(Config{
		Listen: "127.0.0.1:8090", Token: "t", Registry: reg,
		DB: db, DBName: "binlog_index", BootDSN: "cli:" + secretPW + "@tcp(127.0.0.1:3306)/binlog_index",
	})
	if err != nil {
		t.Fatal(err)
	}

	rec, body := doServersReq(t, srv, "GET", "/api/servers", "")
	if rec.Code != 200 {
		t.Fatalf("list code = %d", rec.Code)
	}
	if strings.Contains(string(body), secretPW) {
		t.Fatalf("boot DSN password leaked: %s", body)
	}
	var resp serversResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatal(err)
	}
	if len(resp.Servers) != 1 || resp.Servers[0].Kind != "ephemeral" || resp.Servers[0].ID != bootServerID {
		t.Fatalf("expected exactly the ephemeral boot entry, got %+v", resp.Servers)
	}
	if resp.DefaultID != bootServerID {
		t.Errorf("default_id = %q, want %q", resp.DefaultID, bootServerID)
	}
	if resp.Servers[0].Editable || resp.Servers[0].Deletable {
		t.Error("the boot entry must not be editable/deletable")
	}

	if rec, _ := doServersReq(t, srv, "PUT", "/api/servers/default",
		`{"name":"x","host":"h","user":"u","dbname":"db"}`); rec.Code != 409 {
		t.Errorf("edit boot entry: code = %d, want 409", rec.Code)
	}
	if rec, _ := doServersReq(t, srv, "DELETE", "/api/servers/default", ""); rec.Code != 409 {
		t.Errorf("delete boot entry: code = %d, want 409", rec.Code)
	}
}

// TestResolveHeader: the X-Bintrail-Server header picks the bundle; empty
// falls back to the default; an unknown id 404s; a configured-but-unreachable
// registry server fails on selection (502), not at boot.
func TestResolveHeader(t *testing.T) {
	db, _, closeFn := newSQLMock(t)
	defer closeFn()

	srv := newRegistryServer(t)
	srv.cm.boot = &bundle{db: db, dbName: "bootdb", noArchive: true}
	srv.cm.boot.engine = nil // never queried below; resolution is the subject

	// Empty header → boot.
	req := httptest.NewRequest("GET", "/api/events", nil)
	b, err := srv.resolve(req)
	if err != nil || b.dbName != "bootdb" {
		t.Fatalf("empty header: bundle=%v err=%v, want boot", b, err)
	}
	// Explicit boot id → boot.
	req.Header.Set(serverHeader, bootServerID)
	if b, err = srv.resolve(req); err != nil || b.dbName != "bootdb" {
		t.Fatalf("default id: bundle=%v err=%v, want boot", b, err)
	}
	// Unknown id → ErrUnknownServer (404 at the HTTP layer).
	req.Header.Set(serverHeader, "ffffffffffffffff")
	if _, err = srv.resolve(req); !errors.Is(err, ErrUnknownServer) {
		t.Fatalf("unknown id: err=%v, want ErrUnknownServer", err)
	}
	// A registry entry pointing at a dead host fails on SELECTION with 502 and
	// a scrubbed error — on the DATA endpoints (status/events/recover). NOTE:
	// /api/capabilities deliberately does NOT 502 here; Monitor/Auth are
	// process-level and must survive a broken selection (see
	// TestHandleCapabilitiesMonitorSurvivesUnresolvableServer), so the
	// dead-server feedback comes from the data queries, exercised below.
	added, err := srv.cm.reg.Add(ServerEntry{Name: "dead", DSN: "u:" + secretPW + "@tcp(127.0.0.1:1)/db?timeout=200ms"})
	if err != nil {
		t.Fatal(err)
	}
	req2 := httptest.NewRequest("GET", "http://127.0.0.1:8090/api/status", nil)
	req2.Host = "127.0.0.1:8090"
	req2.Header.Set("Authorization", "Bearer t")
	req2.Header.Set(serverHeader, added.ID)
	rec2 := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec2, req2)
	if rec2.Code != 502 {
		t.Fatalf("dead server selection: code = %d, want 502 (body=%s)", rec2.Code, rec2.Body.String())
	}
	if strings.Contains(rec2.Body.String(), secretPW) {
		t.Errorf("dead-server error leaked the password: %s", rec2.Body.String())
	}
}

// TestServersAPIRequiresToken: every server-management endpoint sits behind
// the bearer token — the write surface (a local file) must not be reachable
// by a cross-site request.
func TestServersAPIRequiresToken(t *testing.T) {
	srv := newRegistryServer(t)
	for _, m := range []struct{ method, path string }{
		{"GET", "/api/servers"},
		{"POST", "/api/servers"},
		{"PUT", "/api/servers/abc"},
		{"DELETE", "/api/servers/abc"},
		{"POST", "/api/servers/abc/test"},
		{"POST", "/api/servers/test"},
	} {
		req := httptest.NewRequest(m.method, "http://127.0.0.1:8090"+m.path, strings.NewReader("{}"))
		req.Host = "127.0.0.1:8090"
		rec := httptest.NewRecorder()
		srv.Handler().ServeHTTP(rec, req)
		if rec.Code != 401 {
			t.Errorf("%s %s without token: code = %d, want 401", m.method, m.path, rec.Code)
		}
	}
}

func TestScrubDSNError(t *testing.T) {
	dsn := "user:" + secretPW + "@tcp(10.0.0.5:3306)/binlog_index"
	err := errors.New("failed to ping MySQL: dial tcp: connect refused for " + dsn + " (password " + secretPW + ")")
	got := scrubDSNError(err, dsn)
	if strings.Contains(got, secretPW) {
		t.Errorf("scrubbed error still contains the password: %s", got)
	}
	if strings.Contains(got, dsn) {
		t.Errorf("scrubbed error still contains the DSN: %s", got)
	}
}

func TestBuildDSNMerge(t *testing.T) {
	stored := "olduser:" + secretPW + "@tcp(oldhost:3306)/olddb?tls=preferred"

	// Structured edit, password omitted: host/user/db change, password and
	// params survive.
	got, err := buildDSN(serverRequest{Name: "x", Host: "newhost", Port: "3307", User: "newuser", DBName: "newdb"}, stored)
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := mysql.ParseDSN(got)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Addr != "newhost:3307" || cfg.User != "newuser" || cfg.DBName != "newdb" {
		t.Errorf("merge wrong: %+v", cfg)
	}
	if cfg.Passwd != secretPW {
		t.Errorf("password lost in merge: %q", cfg.Passwd)
	}
	if cfg.TLSConfig != "preferred" {
		t.Errorf("DSN params lost in merge: tls=%q", cfg.TLSConfig)
	}

	// Default port.
	got, err = buildDSN(serverRequest{Name: "x", Host: "h", User: "u", DBName: "db"}, "")
	if err != nil {
		t.Fatal(err)
	}
	if cfg, _ := mysql.ParseDSN(got); cfg.Addr != "h:3306" {
		t.Errorf("default port: addr = %q, want h:3306", cfg.Addr)
	}

	// Host-only edit keeps a stored NON-default port (the merge is symmetric:
	// a port-only edit keeps the host, a host-only edit keeps the port —
	// defaulting to 3306 here would silently rewrite :3307 connections).
	got, err = buildDSN(serverRequest{Name: "x", Host: "newhost"}, "u:p@tcp(db.internal:3307)/binlog_index")
	if err != nil {
		t.Fatal(err)
	}
	if cfg, _ := mysql.ParseDSN(got); cfg.Addr != "newhost:3307" {
		t.Errorf("host-only edit: addr = %q, want newhost:3307 (stored port preserved)", cfg.Addr)
	}

	// Port-only edit keeps the stored host (the pre-existing symmetric case).
	got, err = buildDSN(serverRequest{Name: "x", Port: "3308"}, "u:p@tcp(db.internal:3307)/binlog_index")
	if err != nil {
		t.Fatal(err)
	}
	if cfg, _ := mysql.ParseDSN(got); cfg.Addr != "db.internal:3308" {
		t.Errorf("port-only edit: addr = %q, want db.internal:3308 (stored host preserved)", cfg.Addr)
	}

	// Raw DSN wins over structured fields and must name a database.
	if _, err := buildDSN(serverRequest{DSN: "u:p@tcp(h:3306)/"}, ""); err == nil {
		t.Error("raw DSN without dbname must be rejected")
	}
	if got, err := buildDSN(serverRequest{DSN: "u:p@tcp(h:3306)/db", Host: "ignored"}, ""); err != nil || !strings.Contains(got, "@tcp(h:3306)/db") {
		t.Errorf("raw DSN should be used verbatim: %q err=%v", got, err)
	}
}

// TestServersAPISourceSecrecy: the source DSN carries REPLICATION credentials
// — the masking discipline must hold for it exactly as for the index DSN, on
// create, get, list, and across keep/clear/replace edits.
func TestServersAPISourceSecrecy(t *testing.T) {
	srv := newRegistryServer(t)

	rec, body := doServersReq(t, srv, "POST", "/api/servers",
		`{"name":"prod","host":"h","user":"u","password":"idxpw","dbname":"db",`+
			`"source_host":"db.prod","source_port":"3307","source_user":"repl","source_password":"`+secretPW+`","schemas":"shop"}`)
	if rec.Code != 201 {
		t.Fatalf("create: code=%d body=%s", rec.Code, body)
	}
	if strings.Contains(string(body), secretPW) || strings.Contains(string(body), "@tcp(") {
		t.Fatalf("create response leaked a source secret: %s", body)
	}
	var created serverDTO
	if err := json.Unmarshal(body, &created); err != nil {
		t.Fatal(err)
	}
	if !created.HasSource || created.SourceHost != "db.prod" || created.SourcePort != "3307" ||
		created.SourceUser != "repl" || !created.HasSourcePassword || created.Schemas != "shop" {
		t.Errorf("masked source parts wrong: %+v", created)
	}
	if created.MonitorDesired {
		t.Error("a plain create must not set monitor_desired (the verbs arrive in phase 3)")
	}

	storedSource := func() string {
		e, _ := srv.cm.reg.Get(created.ID)
		return e.SourceDSN
	}

	// Keep semantics: an edit that omits every source field keeps the config.
	doServersReq(t, srv, "PUT", "/api/servers/"+created.ID,
		`{"name":"prod-2","host":"h","user":"u","dbname":"db","schemas":"shop"}`)
	if got := storedSource(); !strings.Contains(got, "repl:"+secretPW+"@tcp(db.prod:3307)/") {
		t.Errorf("source config must survive an unrelated edit; got %q", got)
	}

	// Source password keep: structured source edit without source_password.
	doServersReq(t, srv, "PUT", "/api/servers/"+created.ID,
		`{"name":"prod-2","host":"h","user":"u","dbname":"db","source_host":"db2.prod","schemas":"shop"}`)
	got := storedSource()
	cfg, err := mysql.ParseDSN(got)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Passwd != secretPW {
		t.Errorf("source password must be kept on a structured edit; got %q", cfg.Passwd)
	}
	if cfg.Addr != "db2.prod:3307" {
		t.Errorf("host-only source edit must keep the stored port; addr=%q", cfg.Addr)
	}

	// Explicit clear: source_dsn "" returns the entry to view-only.
	rec, body = doServersReq(t, srv, "PUT", "/api/servers/"+created.ID,
		`{"name":"prod-2","host":"h","user":"u","dbname":"db","source_dsn":""}`)
	if rec.Code != 200 {
		t.Fatalf("clear: code=%d body=%s", rec.Code, body)
	}
	if got := storedSource(); got != "" {
		t.Errorf("source_dsn:\"\" must clear the source config; got %q", got)
	}
	var cleared serverDTO
	if err := json.Unmarshal(body, &cleared); err != nil {
		t.Fatal(err)
	}
	if cleared.HasSource {
		t.Error("cleared entry must report has_source=false")
	}
}

// TestServersAPIUpdatePreservesSSL guards the regression the #879 schema change
// could introduce: source-TLS is hand-edited into the registry YAML (no request
// field yet), so once it is a TYPED field it no longer round-trips through the
// Extra catch-all. handleServersUpdate must carry it over from the stored entry
// or a plain UI edit silently wipes a configured verify-ca / mutual-TLS source.
func TestServersAPIUpdatePreservesSSL(t *testing.T) {
	srv := newRegistryServer(t)
	created, err := srv.cm.reg.Add(ServerEntry{
		Name:      "prod",
		DSN:       "u:p@tcp(h:3306)/binlog_index",
		SourceDSN: "repl:" + secretPW + "@tcp(db.prod:3306)/",
		SSLMode:   "verify-ca",
		SSLCA:     "/etc/ssl/ca.pem",
		SSLCert:   "/etc/ssl/client-cert.pem",
		SSLKey:    "/etc/ssl/client-key.pem",
	})
	if err != nil {
		t.Fatal(err)
	}

	// An unrelated edit (rename) that carries no ssl_* fields must keep them.
	rec, body := doServersReq(t, srv, "PUT", "/api/servers/"+created.ID,
		`{"name":"prod-renamed","host":"h","user":"u","password":"p","dbname":"binlog_index"}`)
	if rec.Code != 200 {
		t.Fatalf("update: code=%d body=%s", rec.Code, body)
	}
	got, ok := srv.cm.reg.Get(created.ID)
	if !ok {
		t.Fatal("entry lost after update")
	}
	if got.SSLMode != "verify-ca" || got.SSLCA != "/etc/ssl/ca.pem" ||
		got.SSLCert != "/etc/ssl/client-cert.pem" || got.SSLKey != "/etc/ssl/client-key.pem" {
		t.Errorf("source TLS must survive an unrelated edit; got %+v", got)
	}
}

func TestBuildSourceDSNValidation(t *testing.T) {
	pw := "x"
	cases := []struct {
		name string
		req  serverRequest
	}{
		{"raw + structured password", serverRequest{SourceDSN: strPtr("u:p@tcp(h:3306)/"), SourcePassword: &pw}},
		{"unix socket", serverRequest{SourceDSN: strPtr("u:p@unix(/var/run/mysqld.sock)/")}},
		{"structured without host", serverRequest{SourceUser: "repl"}},
		{"structured without user", serverRequest{SourceHost: "h"}},
	}
	for _, tc := range cases {
		if _, err := buildSourceDSN(tc.req, "", FlavorMySQL); err == nil {
			t.Errorf("%s: expected error", tc.name)
		}
	}

	// A source DSN needs NO database name (server-level), unlike the index DSN.
	got, err := buildSourceDSN(serverRequest{SourceHost: "h", SourceUser: "repl"}, "", FlavorMySQL)
	if err != nil {
		t.Fatalf("dbname-less source must be valid: %v", err)
	}
	if cfg, _ := mysql.ParseDSN(got); cfg.DBName != "" || cfg.Addr != "h:3306" {
		t.Errorf("structured source build wrong: %q", got)
	}
}

func strPtr(s string) *string { return &s }

// stubMonitorCtrl is a recording MonitorController for unit tests.
type stubMonitorCtrl struct {
	derived   string
	deriveErr error
	report    *DoctorReport
	status    MonitorStatus
	started   []string
	stopped   []string
	startErr  error
	unsaved   []ServerEntry // entries DoctorUnsaved was asked about
}

func (c *stubMonitorCtrl) DeriveIndexDSN(entryID string) (string, error) {
	if c.deriveErr != nil {
		return "", c.deriveErr
	}
	if c.derived != "" {
		return c.derived, nil
	}
	return "mon:pw@tcp(idx:3306)/bintrail_idx_" + entryID, nil
}
func (c *stubMonitorCtrl) Doctor(_ context.Context, _ ServerEntry) (*DoctorReport, error) {
	if c.report != nil {
		return c.report, nil
	}
	return &DoctorReport{Passed: 1, Checks: []DoctorCheck{{Name: "ok", Status: "pass"}}}, nil
}
func (c *stubMonitorCtrl) DoctorUnsaved(_ context.Context, e ServerEntry) (*DoctorReport, error) {
	c.unsaved = append(c.unsaved, e)
	if c.report != nil {
		return c.report, nil
	}
	return &DoctorReport{Passed: 1, Checks: []DoctorCheck{{Name: "ok", Status: "pass"}}}, nil
}
func (c *stubMonitorCtrl) Start(_ context.Context, e ServerEntry) error {
	if c.startErr != nil {
		return c.startErr
	}
	c.started = append(c.started, e.ID)
	c.status = MonitorStatus{State: "running"}
	return nil
}
func (c *stubMonitorCtrl) Stop(_ context.Context, id string) error {
	c.stopped = append(c.stopped, id)
	c.status = MonitorStatus{State: "stopped"}
	return nil
}
func (c *stubMonitorCtrl) Status(string) MonitorStatus {
	if c.status.State == "" {
		return MonitorStatus{State: "stopped"}
	}
	return c.status
}

// newSupervisorServer builds a Server wired with a stub controller over a
// file-backed registry — the unit harness for the monitor verbs.
func newSupervisorServer(t *testing.T) (*Server, *stubMonitorCtrl) {
	t.Helper()
	reg, err := LoadRegistry(t.TempDir() + "/console-servers.yaml")
	if err != nil {
		t.Fatal(err)
	}
	ctrl := &stubMonitorCtrl{}
	srv, err := New(Config{Listen: "127.0.0.1:8090", Token: "t", Registry: reg, MonitorCtrl: ctrl})
	if err != nil {
		t.Fatal(err)
	}
	return srv, ctrl
}

// TestCapabilityMonitorGate: only a supervisor process (Config.MonitorCtrl,
// wired by `bintrail-console watch`) advertises the monitor capability; the standalone
// read-only console never does.
func TestCapabilityMonitorGate(t *testing.T) {
	for _, monitor := range []bool{true, false} {
		reg, _ := LoadRegistry("")
		cfg := Config{Listen: "127.0.0.1:8090", Token: "t", Registry: reg}
		if monitor {
			cfg.MonitorCtrl = &stubMonitorCtrl{}
		}
		srv, err := New(cfg)
		if err != nil {
			t.Fatal(err)
		}
		srv.cm.boot = &bundle{} // a resolvable default so capabilities answers
		rec, body := doServersReq(t, srv, "GET", "/api/capabilities", "")
		if rec.Code != 200 {
			t.Fatalf("capabilities: code=%d body=%s", rec.Code, body)
		}
		var caps capabilitiesResponse
		if err := json.Unmarshal(body, &caps); err != nil {
			t.Fatal(err)
		}
		if caps.Monitor != monitor {
			t.Errorf("monitor capability = %v, want %v", caps.Monitor, monitor)
		}
	}
}

// TestMonitorVerbsRefuseOnStandaloneConsole: without a supervisor wired in,
// every monitor verb is 403 — the standalone console stays powerless even if
// a client crafts the requests by hand.
func TestMonitorVerbsRefuseOnStandaloneConsole(t *testing.T) {
	srv := newRegistryServer(t) // no MonitorCtrl
	for _, m := range []struct{ method, path string }{
		{"POST", "/api/servers/abc/monitor/start"},
		{"POST", "/api/servers/abc/monitor/stop"},
		{"GET", "/api/servers/abc/monitor"},
	} {
		rec, body := doServersReq(t, srv, m.method, m.path, "{}")
		if rec.Code != 403 {
			t.Errorf("%s %s on standalone console: code=%d body=%s, want 403", m.method, m.path, rec.Code, body)
		}
	}
}

// TestMonitorStartFlow: the zero-terminal path — create with source only
// (index DSN derived), doctor green → desired recorded → stream started; stop
// → desired cleared.
func TestMonitorStartFlow(t *testing.T) {
	srv, ctrl := newSupervisorServer(t)

	// Source-only create: index DSN must be derived via the controller.
	rec, body := doServersReq(t, srv, "POST", "/api/servers",
		`{"name":"prod","source_host":"db.prod","source_user":"repl","source_password":"`+secretPW+`"}`)
	if rec.Code != 201 {
		t.Fatalf("source-only create: code=%d body=%s", rec.Code, body)
	}
	if strings.Contains(string(body), secretPW) {
		t.Fatalf("create leaked the source password: %s", body)
	}
	var created serverDTO
	if err := json.Unmarshal(body, &created); err != nil {
		t.Fatal(err)
	}
	e, _ := srv.cm.reg.Get(created.ID)
	if e.DSN != "mon:pw@tcp(idx:3306)/bintrail_idx_"+created.ID {
		t.Fatalf("index DSN not derived: %q", e.DSN)
	}
	if created.DBName != "bintrail_idx_"+created.ID {
		t.Errorf("DTO should show the derived index db: %+v", created)
	}

	// Start: doctor pass (stub default) → started, desired persisted.
	rec, body = doServersReq(t, srv, "POST", "/api/servers/"+created.ID+"/monitor/start", "")
	if rec.Code != 200 {
		t.Fatalf("start: code=%d body=%s", rec.Code, body)
	}
	var startResp monitorStartResponse
	if err := json.Unmarshal(body, &startResp); err != nil {
		t.Fatal(err)
	}
	if !startResp.Started || startResp.Monitor.State != "running" || startResp.Doctor == nil {
		t.Errorf("start response wrong: %+v", startResp)
	}
	if len(ctrl.started) != 1 || ctrl.started[0] != created.ID {
		t.Errorf("controller Start not invoked correctly: %v", ctrl.started)
	}
	if e, _ := srv.cm.reg.Get(created.ID); !e.MonitorDesired {
		t.Error("monitor_desired must be persisted before the stream starts (boot reconcile depends on it)")
	}

	// The list DTO carries the live state.
	_, body = doServersReq(t, srv, "GET", "/api/servers/"+created.ID, "")
	var dto serverDTO
	if err := json.Unmarshal(body, &dto); err != nil {
		t.Fatal(err)
	}
	if dto.MonitorState != "running" || !dto.MonitorDesired {
		t.Errorf("DTO monitor view wrong: %+v", dto)
	}

	// Stop: desired cleared FIRST (crash-safety), controller stopped.
	rec, body = doServersReq(t, srv, "POST", "/api/servers/"+created.ID+"/monitor/stop", "")
	if rec.Code != 200 {
		t.Fatalf("stop: code=%d body=%s", rec.Code, body)
	}
	if len(ctrl.stopped) != 1 || ctrl.stopped[0] != created.ID {
		t.Errorf("controller Stop not invoked: %v", ctrl.stopped)
	}
	if e, _ := srv.cm.reg.Get(created.ID); e.MonitorDesired {
		t.Error("stop must clear monitor_desired")
	}
}

// TestMonitorStartDoctorFailure: a failed required check means NOTHING starts
// — no desired flag, no controller call — and the remediation cards return.
func TestMonitorStartDoctorFailure(t *testing.T) {
	srv, ctrl := newSupervisorServer(t)
	ctrl.report = &DoctorReport{
		Failed: 1,
		Checks: []DoctorCheck{{Name: "Source MySQL connection", Status: "fail", Detail: "refused", Remediation: "open the firewall"}},
	}
	_, body := doServersReq(t, srv, "POST", "/api/servers",
		`{"name":"prod","source_host":"db.prod","source_user":"repl"}`)
	var created serverDTO
	if err := json.Unmarshal(body, &created); err != nil {
		t.Fatal(err)
	}
	rec, body := doServersReq(t, srv, "POST", "/api/servers/"+created.ID+"/monitor/start", "")
	if rec.Code != 200 {
		t.Fatalf("start with failing doctor: code=%d body=%s", rec.Code, body)
	}
	var resp monitorStartResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Started || len(ctrl.started) != 0 {
		t.Error("a failed doctor must not start anything")
	}
	if len(resp.Doctor.Checks) == 0 || resp.Doctor.Checks[0].Remediation == "" {
		t.Errorf("remediation cards must come back: %+v", resp.Doctor)
	}
	if e, _ := srv.cm.reg.Get(created.ID); e.MonitorDesired {
		t.Error("a failed doctor must not record monitoring intent")
	}
}

// TestFailedCheckSummary: the log line joins only the failed checks, with
// their (already-scrubbed) details, skipping passes.
func TestFailedCheckSummary(t *testing.T) {
	r := &DoctorReport{Checks: []DoctorCheck{
		{Name: "Source MySQL connection", Status: "pass", Detail: "MySQL 8.4"},
		{Name: "binlog_row_image", Status: "fail", Detail: "is MINIMAL, need FULL"},
		{Name: "Replication grants", Status: "fail", Detail: ""},
	}}
	got := failedCheckSummary(r)
	want := "binlog_row_image: is MINIMAL, need FULL; Replication grants"
	if got != want {
		t.Errorf("failedCheckSummary = %q, want %q", got, want)
	}
}

// TestMonitorStartLogsPreflightFailure locks in the silent-failure fix: a
// preflight failure must be visible from the host (docker logs), not only in
// the browser that fired the request. Without the slog.Warn, an operator
// running --log-level debug sees NOTHING when Start fails.
func TestMonitorStartLogsPreflightFailure(t *testing.T) {
	var buf bytes.Buffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	defer slog.SetDefault(old)

	srv, ctrl := newSupervisorServer(t)
	ctrl.report = &DoctorReport{
		Failed: 1,
		Checks: []DoctorCheck{{Name: "Source MySQL connection", Status: "fail",
			Detail: "failed to ping MySQL: dial tcp 172.18.0.1:3306: connect: connection refused", Remediation: "x"}},
	}
	_, body := doServersReq(t, srv, "POST", "/api/servers",
		`{"name":"wp","source_host":"db","source_user":"repl"}`)
	var created serverDTO
	if err := json.Unmarshal(body, &created); err != nil {
		t.Fatal(err)
	}
	doServersReq(t, srv, "POST", "/api/servers/"+created.ID+"/monitor/start", "")

	out := buf.String()
	if !strings.Contains(out, "preflight failed") {
		t.Errorf("a preflight failure must be logged (silent-failure fix); log was:\n%s", out)
	}
	if !strings.Contains(out, "172.18.0.1:3306") {
		t.Errorf("the failure log must carry the actionable detail; log was:\n%s", out)
	}
}

// TestMonitorStartRequiresSource: starting a view-only entry is a 400.
func TestMonitorStartRequiresSource(t *testing.T) {
	srv, _ := newSupervisorServer(t)
	_, body := doServersReq(t, srv, "POST", "/api/servers",
		`{"name":"viewonly","host":"h","user":"u","dbname":"db"}`)
	var created serverDTO
	if err := json.Unmarshal(body, &created); err != nil {
		t.Fatal(err)
	}
	rec, _ := doServersReq(t, srv, "POST", "/api/servers/"+created.ID+"/monitor/start", "")
	if rec.Code != 400 {
		t.Errorf("start without source: code=%d, want 400", rec.Code)
	}
}

// TestMonitorGuardsOnEditAndDelete: a live stream blocks source changes and
// deletion until an explicit stop.
func TestMonitorGuardsOnEditAndDelete(t *testing.T) {
	srv, ctrl := newSupervisorServer(t)
	_, body := doServersReq(t, srv, "POST", "/api/servers",
		`{"name":"prod","source_host":"db.prod","source_user":"repl"}`)
	var created serverDTO
	if err := json.Unmarshal(body, &created); err != nil {
		t.Fatal(err)
	}
	ctrl.status = MonitorStatus{State: "running"}

	rec, _ := doServersReq(t, srv, "PUT", "/api/servers/"+created.ID,
		`{"name":"prod","source_host":"OTHER.host","source_user":"repl"}`)
	if rec.Code != 409 {
		t.Errorf("source edit while running: code=%d, want 409", rec.Code)
	}
	rec, _ = doServersReq(t, srv, "DELETE", "/api/servers/"+created.ID, "")
	if rec.Code != 409 {
		t.Errorf("delete while running: code=%d, want 409", rec.Code)
	}

	// A non-source edit (rename) stays allowed while running.
	rec, b := doServersReq(t, srv, "PUT", "/api/servers/"+created.ID,
		`{"name":"prod-renamed"}`)
	if rec.Code != 200 {
		t.Errorf("rename while running: code=%d body=%s, want 200", rec.Code, b)
	}
}

// TestCreateDeriveFailureRollsBack: when the index DSN cannot be derived the
// half-created entry must not survive.
func TestCreateDeriveFailureRollsBack(t *testing.T) {
	srv, ctrl := newSupervisorServer(t)
	ctrl.deriveErr = errors.New("boom")
	rec, _ := doServersReq(t, srv, "POST", "/api/servers",
		`{"name":"prod","source_host":"db.prod","source_user":"repl"}`)
	if rec.Code != 500 {
		t.Fatalf("derive failure: code=%d, want 500", rec.Code)
	}
	if srv.cm.reg.Len() != 0 {
		t.Error("half-created entry must be rolled back")
	}
}

// TestResolveNoServers: an empty console (no boot entry, empty registry) must
// return the user-facing errNoServers, which the HTTP layer maps to 404.
func TestResolveNoServers(t *testing.T) {
	cm := newConnManager(nil, false)
	if _, err := cm.Resolve(t.Context(), ""); !errors.Is(err, errNoServers) {
		t.Fatalf("Resolve on empty console: err = %v, want errNoServers", err)
	}
}

// TestRegistryErrStatus locks the registry-error → HTTP status contract,
// including the string-matched 400 branches a refactor could silently break.
func TestRegistryErrStatus(t *testing.T) {
	cases := []struct {
		err  error
		want int
	}{
		{ErrDuplicateName, 409},
		{ErrRegistryReadOnly, 409},
		{ErrUnknownServer, 404},
		{errors.New("server name is required"), 400},
		{errors.New(`"default" is reserved for the command-line server`), 400},
		{errors.New("disk exploded"), 500},
		{fmt.Errorf("%w: endpoint: bad", storage.ErrBucketStoreConfig), 400},
		{fmt.Errorf("%w: bucket x", ErrS3StoreConflict), 422},
	}
	for _, tc := range cases {
		if got := registryErrStatus(tc.err); got != tc.want {
			t.Errorf("registryErrStatus(%v) = %d, want %d", tc.err, got, tc.want)
		}
	}
}

// TestBuildDSNRejectsRawDSNPlusPassword: a raw dsn carries its own password;
// accepting a structured password alongside would silently drop one of them.
func TestBuildDSNRejectsRawDSNPlusPassword(t *testing.T) {
	pw := "x"
	if _, err := buildDSN(serverRequest{DSN: "u:p@tcp(h:3306)/db", Password: &pw}, ""); err == nil {
		t.Fatal("dsn + structured password must be rejected, not silently merged")
	}
}

// TestCapabilityMatrix: the pure-config reconstruct gate over
// baseline × no_archive × profile × process-wide baseline fallback (#1010).
func TestCapabilityMatrix(t *testing.T) {
	cases := []struct {
		name       string
		entry      ServerEntry
		profile    bool
		defaultDir string
		want       bool
	}{
		{"baseline dir", ServerEntry{BaselineDir: "/b"}, false, "", true},
		{"baseline s3", ServerEntry{BaselineS3: "s3://b/"}, false, "", true},
		{"no baseline", ServerEntry{}, false, "", false},
		{"no_archive kills it", ServerEntry{BaselineDir: "/b", NoArchive: true}, false, "", false},
		{"profile kills it", ServerEntry{BaselineDir: "/b"}, true, "", false},
		// #1010: an entry with no baseline of its own inherits the process-wide
		// --baseline-dir; no_archive and an active profile still gate it off.
		{"process fallback", ServerEntry{}, false, "/proc/b", true},
		{"fallback + no_archive", ServerEntry{NoArchive: true}, false, "/proc/b", false},
		{"fallback + profile", ServerEntry{}, true, "/proc/b", false},
	}
	for _, tc := range cases {
		cm := newConnManager(nil, tc.profile)
		cm.defaultBaselineDir = tc.defaultDir
		if got := cm.capability(tc.entry); got != tc.want {
			t.Errorf("%s: capability = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// TestRegistryBaselineFallbackAPI (#1010): a server added through the real
// POST /api/servers path — which has no baseline field — must inherit the
// process-wide --baseline-dir: its DTO reports reconstruct:true, and the
// derived bundle (rebuildDerived shares newBundleDerived with the lazy open)
// turns on both Reconstruct and Verify in /api/capabilities. The DTO's
// baseline_dir must stay the entry's OWN (empty) value — echoing the default
// into the edit form would persist it as per-server config on the next save.
//
// Without a process baseline dir the server gets a folder of its own since
// #1681 (<state dir>/baselines/<id>), so both capabilities are on and the DTO
// names that folder; with one, a create that does not ask for a local copy
// keeps the fallback, unchanged.
func TestRegistryBaselineFallbackAPI(t *testing.T) {
	for _, procBaseline := range []bool{true, false} {
		state := t.TempDir()
		reg, err := LoadRegistry(state + "/console-servers.yaml")
		if err != nil {
			t.Fatal(err)
		}
		cfg := Config{
			Listen: "127.0.0.1:8090", Token: "t", Registry: reg,
			MonitorCtrl: &stubMonitorCtrl{}, VerifyCtrl: &stubVerifyCtrl{},
		}
		if procBaseline {
			cfg.BaselineDir = "/var/bintrail/baselines"
		}
		srv, err := New(cfg)
		if err != nil {
			t.Fatal(err)
		}

		rec, body := doServersReq(t, srv, "POST", "/api/servers",
			`{"name":"orders","dsn":"u:p@tcp(10.0.0.5:3306)/idx"}`)
		if rec.Code/100 != 2 {
			t.Fatalf("procBaseline=%v: create code=%d body=%s", procBaseline, rec.Code, body)
		}
		var created serverDTO
		if err := json.Unmarshal(body, &created); err != nil {
			t.Fatal(err)
		}
		if !created.Reconstruct {
			t.Errorf("procBaseline=%v: created DTO reconstruct=false, want true", procBaseline)
		}
		wantOwn := ""
		if !procBaseline {
			wantOwn = state + "/baselines/" + created.ID
		}
		if created.BaselineDir != wantOwn || created.BaselineS3 != "" {
			t.Errorf("procBaseline=%v: DTO must report the entry's OWN baseline (%q), got dir=%q s3=%q",
				procBaseline, wantOwn, created.BaselineDir, created.BaselineS3)
		}

		// Publish the entry's derived bundle the way the manager does (the
		// unit suite can't dial the fake DSN a lazy open would need).
		entry, ok := srv.cm.reg.Get(created.ID)
		if !ok {
			t.Fatal("created entry missing from registry")
		}
		srv.cm.bundles[created.ID] = &bundle{}
		srv.cm.rebuildDerived(entry)

		rec, body = doServersReqHeader(t, srv, "GET", "/api/capabilities", "", created.ID)
		if rec.Code != 200 {
			t.Fatalf("procBaseline=%v: capabilities code=%d body=%s", procBaseline, rec.Code, body)
		}
		var caps capabilitiesResponse
		if err := json.Unmarshal(body, &caps); err != nil {
			t.Fatal(err)
		}
		if !caps.Reconstruct {
			t.Errorf("procBaseline=%v: capabilities reconstruct=false, want true", procBaseline)
		}
		if !caps.Verify {
			t.Errorf("procBaseline=%v: capabilities verify=false, want true", procBaseline)
		}
		if b := srv.cm.bundles[created.ID]; procBaseline && b.baselineSrc != "/var/bintrail/baselines" {
			t.Errorf("bundle baselineSrc=%q, want the process --baseline-dir", b.baselineSrc)
		}
	}
}

// TestWithBaselineDefaultsAllOrNothing (#1010): the process fallback applies
// only when the entry carries NO baseline of its own — an entry with its own
// dir or S3 chose its location explicitly, and mixing in the process default
// would make findBaseline read a location never associated with that server.
func TestWithBaselineDefaultsAllOrNothing(t *testing.T) {
	cm := newConnManager(nil, false)
	cm.defaultBaselineDir = "/proc/b"
	cm.defaultBaselineS3 = "s3://proc/"

	got := cm.withBaselineDefaults(ServerEntry{})
	if got.BaselineDir != "/proc/b" || got.BaselineS3 != "s3://proc/" {
		t.Errorf("empty entry must inherit both defaults, got dir=%q s3=%q", got.BaselineDir, got.BaselineS3)
	}
	got = cm.withBaselineDefaults(ServerEntry{BaselineS3: "s3://own/"})
	if got.BaselineDir != "" || got.BaselineS3 != "s3://own/" {
		t.Errorf("own S3 must suppress BOTH defaults, got dir=%q s3=%q", got.BaselineDir, got.BaselineS3)
	}
	got = cm.withBaselineDefaults(ServerEntry{BaselineDir: "/own"})
	if got.BaselineDir != "/own" || got.BaselineS3 != "" {
		t.Errorf("own dir must suppress BOTH defaults, got dir=%q s3=%q", got.BaselineDir, got.BaselineS3)
	}
}
