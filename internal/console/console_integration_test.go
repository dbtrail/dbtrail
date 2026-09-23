//go:build integration

package console

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/recovery"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

const intToken = "integration-token"

func seedConsoleData(t *testing.T) (*Server, string) {
	t.Helper()
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	// INSERT then UPDATE on app.users pk=1.
	testutil.InsertEvent(t, db, "bin.000001", 4, 40, "2026-06-01 12:00:00", nil,
		"app", "users", 1 /*INSERT*/, "1",
		nil, nil, []byte(`{"id":1,"name":"alice"}`))
	testutil.InsertEvent(t, db, "bin.000001", 40, 80, "2026-06-01 12:05:00", nil,
		"app", "users", 2 /*UPDATE*/, "1",
		[]byte(`["name"]`), []byte(`{"id":1,"name":"alice"}`), []byte(`{"id":1,"name":"alicia"}`))

	srv, err := New(Config{
		DB:        db,
		DBName:    dbName,
		Listen:    "127.0.0.1:8090",
		Token:     intToken,
		NoArchive: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	return srv, dbName
}

func doReq(t *testing.T, srv *Server, method, path, body string) (*httptest.ResponseRecorder, []byte) {
	t.Helper()
	var r io.Reader
	if body != "" {
		r = strings.NewReader(body)
	}
	req := httptest.NewRequest(method, "http://127.0.0.1:8090"+path, r)
	req.Host = "127.0.0.1:8090"
	req.Header.Set("Authorization", "Bearer "+intToken)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	return rec, rec.Body.Bytes()
}

func TestIntegrationEventsAPI(t *testing.T) {
	srv, _ := seedConsoleData(t)

	rec, body := doReq(t, srv, "GET", "/api/events?schema=app&table=users", "")
	if rec.Code != 200 {
		t.Fatalf("events code = %d, body = %s", rec.Code, body)
	}
	// connection_id is a regular indexed column on the events API (#701 D1).
	if !strings.Contains(string(body), "connection_id") {
		t.Errorf("events response must contain connection_id (#701 D1): %s", body)
	}
	var resp eventsResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Count != 2 {
		t.Errorf("event count = %d, want 2", resp.Count)
	}
}

func TestIntegrationSchemasAPI(t *testing.T) {
	srv, _ := seedConsoleData(t)

	_, body := doReq(t, srv, "GET", "/api/schemas", "")
	var sr schemasResponse
	if err := json.Unmarshal(body, &sr); err != nil {
		t.Fatal(err)
	}
	if !contains(sr.Schemas, "app") {
		t.Errorf("schemas = %v, want to include app", sr.Schemas)
	}

	_, body = doReq(t, srv, "GET", "/api/schemas?schema=app", "")
	var tr tablesResponse
	if err := json.Unmarshal(body, &tr); err != nil {
		t.Fatal(err)
	}
	if !contains(tr.Tables, "users") {
		t.Errorf("tables = %v, want to include users", tr.Tables)
	}
}

func TestIntegrationStatusAPI(t *testing.T) {
	srv, _ := seedConsoleData(t)
	rec, body := doReq(t, srv, "GET", "/api/status", "")
	if rec.Code != 200 {
		t.Fatalf("status code = %d, body = %s", rec.Code, body)
	}
	var generic map[string]any
	if err := json.Unmarshal(body, &generic); err != nil {
		t.Fatalf("status is not valid JSON: %v", err)
	}
}

// TestIntegrationRecoverMatchesGenerator is the issue's manual check, automated:
// the console's undo SQL must match what the recovery generator produces for
// the same filters.
func TestIntegrationRecoverMatchesGenerator(t *testing.T) {
	srv, _ := seedConsoleData(t)

	rec, body := doReq(t, srv, "POST", "/api/recover", `{"schema":"app","table":"users"}`)
	if rec.Code != 200 {
		t.Fatalf("recover code = %d, body = %s", rec.Code, body)
	}
	var resp recoverResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatal(err)
	}
	if resp.StatementCount != 2 {
		t.Errorf("statement_count = %d, want 2", resp.StatementCount)
	}
	// Order correctness: the newest event (the UPDATE) must be undone first,
	// then the INSERT. This anchors the fix for the DESC-vs-ASC ordering bug —
	// a buggy DESC fetch would invert these two statements.
	stmts := statements(resp.SQL)
	if len(stmts) != 2 {
		t.Fatalf("expected 2 undo statements, got %d: %v", len(stmts), stmts)
	}
	if !strings.HasPrefix(stmts[0], "UPDATE") {
		t.Errorf("first undo statement should reverse the newest (UPDATE) event, got: %s", stmts[0])
	}
	if !strings.HasPrefix(stmts[1], "DELETE") {
		t.Errorf("second undo statement should reverse the INSERT event, got: %s", stmts[1])
	}

	// Equivalence with the generator. The console now fetches DESC (newest
	// first) so a LIMIT truncation keeps the most recent events (#981), then
	// re-sorts ASC before generation — whereas the generator call below fetches
	// ASC (oldest-first, the zero value) under the same limit. Those two
	// orderings diverge once the match set exceeds the limit: ASC-then-cap
	// keeps the OLDEST rows, DESC-then-cap keeps the NEWEST. They agree here
	// only because this fixture's 2 events sit far under recoverDefaultLimit,
	// so nothing is actually truncated on either side — this is a same-input
	// sanity check, not a general proof of equivalence. Guard that assumption
	// explicitly so a future larger fixture doesn't silently start comparing
	// two different truncation windows.
	if len(stmts) >= recoverDefaultLimit {
		t.Fatalf("fixture has grown to %d events, at/above recoverDefaultLimit (%d); the ASC/DESC equivalence below no longer holds — rewrite this check against the console's actual DESC-then-resort behavior", len(stmts), recoverDefaultLimit)
	}
	var buf bytes.Buffer
	opts := query.Options{Schema: "app", Table: "users", Order: "", Limit: recoverDefaultLimit}
	if _, err := recovery.New(srv.cm.boot.db, srv.cm.boot.resolver).GenerateSQL(context.Background(), opts, &buf); err != nil {
		t.Fatal(err)
	}
	if got, want := stmts, statements(buf.String()); !equalStrings(got, want) {
		t.Errorf("console recover SQL differs from generator:\nconsole: %v\ngenerator: %v", got, want)
	}
}

// TestIntegrationRecoverPGDialect proves the console recover surface emits
// PostgreSQL-dialect SQL when the selected server's index is PG-flavored (#573):
// the console is the reachable-today surface for PG customers (capture with
// bintrail-pg, view/recover with the shared console). DialectForIndex reads the
// per-bundle stream_state.flavor, so a 'postgres' flavor → double-quoted identifiers
// + the standard_conforming_strings guard, NOT MySQL backticks.
func TestIntegrationRecoverPGDialect(t *testing.T) {
	srv, _ := seedConsoleData(t)
	// Stamp the boot bundle's index as PostgreSQL-sourced (single-row stream_state).
	if _, err := srv.cm.boot.db.Exec(
		`INSERT INTO stream_state (id, mode, flavor, last_checkpoint, server_id)
		 VALUES (1, 'gtid', 'postgres', UTC_TIMESTAMP(), 1)`); err != nil {
		t.Fatalf("stamp stream_state flavor=postgres: %v", err)
	}

	rec, body := doReq(t, srv, "POST", "/api/recover", `{"schema":"app","table":"users"}`)
	if rec.Code != 200 {
		t.Fatalf("recover code = %d, body = %s", rec.Code, body)
	}
	var resp recoverResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(resp.SQL, `"app"."users"`) {
		t.Errorf("console PG recover must use double-quoted identifiers, got:\n%s", resp.SQL)
	}
	if !strings.Contains(resp.SQL, "SET LOCAL standard_conforming_strings = on;") {
		t.Errorf("console PG recover must emit the standard_conforming_strings guard, got:\n%s", resp.SQL)
	}
	if strings.Contains(resp.SQL, "`") {
		t.Errorf("console PG recover must NOT contain MySQL backticks, got:\n%s", resp.SQL)
	}
}

// TestIntegrationCapabilitiesSourcePostgres proves /api/capabilities reports the
// source family per-server from stream_state.flavor (#595). The shared console
// reads only the index, so this is the signal the frontend uses to present PG
// vocabulary (LSN vs binlog file/pos/GTID) and the connection-id availability note —
// without ever probing the source database. A fresh/legacy index (no flavor row)
// reads as "mysql" (the safe default); a PG-flavored index as "postgresql".
func TestIntegrationCapabilitiesSourcePostgres(t *testing.T) {
	srv, _ := seedConsoleData(t)

	// Default: seedConsoleData writes no stream_state row, so DialectForIndex
	// falls back to MySQL — capabilities must report the common case, never blank.
	_, body := doReq(t, srv, "GET", "/api/capabilities", "")
	var caps capabilitiesResponse
	if err := json.Unmarshal(body, &caps); err != nil {
		t.Fatal(err)
	}
	if caps.Source != "mysql" {
		t.Errorf("a MySQL/legacy index must report source=mysql, got %q", caps.Source)
	}

	// Stamp the boot bundle's index as PostgreSQL-sourced (single-row stream_state).
	if _, err := srv.cm.boot.db.Exec(
		`INSERT INTO stream_state (id, mode, flavor, last_checkpoint, server_id)
		 VALUES (1, 'gtid', 'postgres', UTC_TIMESTAMP(), 1)`); err != nil {
		t.Fatalf("stamp stream_state flavor=postgres: %v", err)
	}

	_, body = doReq(t, srv, "GET", "/api/capabilities", "")
	if err := json.Unmarshal(body, &caps); err != nil {
		t.Fatal(err)
	}
	if caps.Source != "postgresql" {
		t.Errorf("a PG-flavored index must report source=postgresql, got %q", caps.Source)
	}
}

// TestIntegrationRecoverWithTimeRangeSurfacesGap exercises the planner-active
// recover path. testutil.InitIndexTables creates only the p_future partition,
// so loadLivePartitionHours is empty and every hour in a bounded query range is
// classified as a gap. Recover must still SUCCEED (200) with the undo
// statements and surface the gap as a warning — never hard-fail. This locks in
// AllowGaps=true for recover: a former AllowGaps=false would have returned 422
// here, diverging from the CLI `recover` the issue requires it to match.
func TestIntegrationRecoverWithTimeRangeSurfacesGap(t *testing.T) {
	srv, _ := seedConsoleData(t)

	body := `{"schema":"app","table":"users","since":"2026-06-01 00:00:00","until":"2026-06-02 00:00:00"}`
	rec, raw := doReq(t, srv, "POST", "/api/recover", body)
	if rec.Code != 200 {
		t.Fatalf("recover with time range code = %d, body = %s", rec.Code, raw)
	}
	var resp recoverResponse
	if err := json.Unmarshal(raw, &resp); err != nil {
		t.Fatal(err)
	}
	if resp.StatementCount != 2 {
		t.Errorf("statement_count = %d, want 2 (undo still generated despite gap)", resp.StatementCount)
	}
	if len(resp.Warnings) == 0 {
		t.Error("expected a coverage-gap warning (only p_future exists, so the bounded range is all gap)")
	}
}

// TestIntegrationProfileRBAC verifies end-to-end that profile RBAC rules are
// enforced on the console's events surface: a denied table never appears, and a
// redacted column's value is nulled while its siblings remain. It also confirms
// New forces NoArchive when RBAC rules are present (archives don't apply RBAC).
func TestIntegrationProfileRBAC(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	testutil.InsertEvent(t, db, "bin.000001", 4, 40, "2026-06-01 12:00:00", nil,
		"app", "users", 1 /*INSERT*/, "1",
		nil, nil, []byte(`{"id":1,"email":"alice@x","ssn":"111-22-3333"}`))
	testutil.InsertEvent(t, db, "bin.000001", 40, 80, "2026-06-01 12:01:00", nil,
		"app", "secrets", 1 /*INSERT*/, "1",
		nil, nil, []byte(`{"id":1,"value":"topsecret"}`))

	srv, err := New(Config{
		DB:            db,
		DBName:        dbName,
		Listen:        "127.0.0.1:8090",
		Token:         intToken,
		DenyTables:    []query.SchemaTable{{Schema: "app", Table: "secrets"}},
		RedactColumns: []query.SchemaTableColumn{{Schema: "app", Table: "users", Column: "ssn"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !srv.cm.boot.noArchive {
		t.Error("New must force noArchive when RBAC rules are present (archives don't enforce RBAC)")
	}

	// Denied table: querying it returns nothing and never leaks its data.
	_, body := doReq(t, srv, "GET", "/api/events?schema=app&table=secrets", "")
	if strings.Contains(string(body), "topsecret") {
		t.Errorf("denied table app.secrets leaked into the events response: %s", body)
	}

	// Redacted column nulled; non-redacted sibling still present.
	_, body = doReq(t, srv, "GET", "/api/events?schema=app&table=users", "")
	if strings.Contains(string(body), "111-22-3333") {
		t.Errorf("redacted column ssn leaked: %s", body)
	}
	if !strings.Contains(string(body), "alice@x") {
		t.Errorf("non-redacted column email should remain present: %s", body)
	}
}

// doReqOn is doReq with an explicit X-Bintrail-Server selection header.
func doReqOn(t *testing.T, srv *Server, serverID, method, path, body string) (*httptest.ResponseRecorder, []byte) {
	t.Helper()
	var r io.Reader
	if body != "" {
		r = strings.NewReader(body)
	}
	req := httptest.NewRequest(method, "http://127.0.0.1:8090"+path, r)
	req.Host = "127.0.0.1:8090"
	req.Header.Set("Authorization", "Bearer "+intToken)
	if serverID != "" {
		req.Header.Set("X-Bintrail-Server", serverID)
	}
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	return rec, rec.Body.Bytes()
}

// seedSecondIndex creates another test index seeded with one shop.orders event
// and returns its database name (its DSN comes from testutil.IntegrationDSN).
func seedSecondIndex(t *testing.T) string {
	t.Helper()
	db2, dbName2 := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db2)
	testutil.InsertEvent(t, db2, "bin.000009", 4, 40, "2026-06-02 09:00:00", nil,
		"shop", "orders", 1 /*INSERT*/, "1001",
		nil, nil, []byte(`{"id":1001,"status":"pending"}`))
	return dbName2
}

// TestIntegrationServerSwitching is the feature's end-to-end check: a second
// index registered through the API, every data surface re-scoped by the
// selection header, and per-server capability gating.
func TestIntegrationServerSwitching(t *testing.T) {
	srv, _ := seedConsoleData(t) // boot: app.users events
	t.Cleanup(srv.cm.CloseAll)
	dbName2 := seedSecondIndex(t) // registry: shop.orders events

	// Register the second index via the API (lazy: no connection yet).
	rec, body := doReq(t, srv, "POST", "/api/servers",
		`{"name":"second","dsn":"`+testutil.IntegrationDSN(dbName2)+`"}`)
	if rec.Code != 201 {
		t.Fatalf("create server: code=%d body=%s", rec.Code, body)
	}
	var created serverDTO
	if err := json.Unmarshal(body, &created); err != nil {
		t.Fatal(err)
	}

	// Default (no header) → boot index: app schema, no shop events.
	_, body = doReq(t, srv, "GET", "/api/schemas", "")
	var sr schemasResponse
	if err := json.Unmarshal(body, &sr); err != nil {
		t.Fatal(err)
	}
	if !contains(sr.Schemas, "app") || contains(sr.Schemas, "shop") {
		t.Errorf("boot schemas = %v, want app without shop", sr.Schemas)
	}

	// Selection header → second index: shop schema and its event, scoped.
	_, body = doReqOn(t, srv, created.ID, "GET", "/api/schemas", "")
	if err := json.Unmarshal(body, &sr); err != nil {
		t.Fatal(err)
	}
	if !contains(sr.Schemas, "shop") || contains(sr.Schemas, "app") {
		t.Errorf("registry-server schemas = %v, want shop without app", sr.Schemas)
	}
	rec, body = doReqOn(t, srv, created.ID, "GET", "/api/events?schema=shop&table=orders", "")
	if rec.Code != 200 {
		t.Fatalf("events on second server: code=%d body=%s", rec.Code, body)
	}
	var er eventsResponse
	if err := json.Unmarshal(body, &er); err != nil {
		t.Fatal(err)
	}
	if er.Count != 1 {
		t.Errorf("second-server event count = %d, want 1", er.Count)
	}
	// The same query against the boot index must see nothing — scoping holds.
	_, body = doReq(t, srv, "GET", "/api/events?schema=shop&table=orders", "")
	if err := json.Unmarshal(body, &er); err != nil {
		t.Fatal(err)
	}
	if er.Count != 0 {
		t.Errorf("boot index leaked %d shop events", er.Count)
	}

	// Per-server capabilities: boot has no baseline (false); give the registry
	// entry a baseline dir (keep-password PUT, structured fields) → true.
	_, body = doReq(t, srv, "GET", "/api/capabilities", "")
	var caps capabilitiesResponse
	if err := json.Unmarshal(body, &caps); err != nil {
		t.Fatal(err)
	}
	if caps.Reconstruct {
		t.Error("boot entry must report reconstruct=false (no baseline)")
	}
	// A folder of the test's own: since #1681 a saved folder must exist (this
	// console only reads, so it creates none), and a fixed /tmp path passed
	// only on machines where some earlier run had left it behind.
	baselineDir := t.TempDir()
	rec, body = doReqOn(t, srv, "", "PUT", "/api/servers/"+created.ID,
		`{"name":"second","host":"`+created.Host+`","port":"`+created.Port+`","user":"`+created.User+`","dbname":"`+created.DBName+`","baseline_dir":"`+baselineDir+`"}`)
	if rec.Code != 200 {
		t.Fatalf("baseline edit: code=%d body=%s", rec.Code, body)
	}
	rec, body = doReqOn(t, srv, created.ID, "GET", "/api/capabilities", "")
	if rec.Code != 200 {
		t.Fatalf("capabilities on second server: code=%d body=%s (keep-password merge must keep the DSN working)", rec.Code, body)
	}
	if err := json.Unmarshal(body, &caps); err != nil {
		t.Fatal(err)
	}
	if !caps.Reconstruct {
		t.Error("registry entry with baseline_dir must report reconstruct=true")
	}

	// CloseAll closes registry connections but must NOT touch the boot db —
	// the cmd layer opened it and owns its deferred Close.
	srv.cm.CloseAll()
	if err := srv.cm.boot.db.Ping(); err != nil {
		t.Errorf("CloseAll must not close the boot db (cmd layer owns it): %v", err)
	}
}

// TestIntegrationRegistryServerNeverMigrated locks the read-only boundary: a
// registry index missing the connection_id column is NEVER ALTERed by the
// console — the query fails with an actionable 422 and the schema stays put.
func TestIntegrationRegistryServerNeverMigrated(t *testing.T) {
	srv, _ := seedConsoleData(t)
	t.Cleanup(srv.cm.CloseAll)

	db2, dbName2 := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db2)
	if _, err := db2.Exec("ALTER TABLE binlog_events DROP COLUMN connection_id"); err != nil {
		t.Fatalf("simulate legacy index: %v", err)
	}

	rec, body := doReq(t, srv, "POST", "/api/servers",
		`{"name":"legacy","dsn":"`+testutil.IntegrationDSN(dbName2)+`"}`)
	if rec.Code != 201 {
		t.Fatalf("create: code=%d body=%s", rec.Code, body)
	}
	var created serverDTO
	if err := json.Unmarshal(body, &created); err != nil {
		t.Fatal(err)
	}

	// The probe is write-free and reports the stale schema.
	rec, body = doReq(t, srv, "POST", "/api/servers/"+created.ID+"/test", "")
	if rec.Code != 200 {
		t.Fatalf("test probe: code=%d body=%s", rec.Code, body)
	}
	var probe testResponse
	if err := json.Unmarshal(body, &probe); err != nil {
		t.Fatal(err)
	}
	if !probe.OK || probe.HasIndex == nil || !*probe.HasIndex {
		t.Errorf("probe = %+v, want ok with has_index=true", probe)
	}
	// Tri-state matters here: a legacy index must report an EXPLICIT
	// schema_current=false (the actionable claim), not a nil/unknown.
	if probe.SchemaCurrent == nil || *probe.SchemaCurrent {
		t.Errorf("probe.SchemaCurrent = %v, want explicit false on a legacy index", probe.SchemaCurrent)
	}

	// Querying it fails with the actionable 422, not a silent migration.
	rec, body = doReqOn(t, srv, created.ID, "GET", "/api/events?schema=shop", "")
	if rec.Code != 422 {
		t.Fatalf("legacy index query: code=%d, want 422 (body=%s)", rec.Code, body)
	}
	if !strings.Contains(string(body), "writer command") {
		t.Errorf("422 must name the fix (a writer command): %s", body)
	}

	// The console must not have ALTERed the table back.
	var n int
	if err := db2.QueryRow(
		"SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'binlog_events' AND COLUMN_NAME = 'connection_id'",
		dbName2).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Error("console ran EnsureSchema on a registry server — the read-only boundary is broken")
	}
}

// TestIntegrationProbeProvisionPending: a monitored source whose per-source
// index DB does not exist yet probes as ProvisionPending (reachable server,
// pre-Start state) rather than a hard failure — but only when monitored=true.
// An unmonitored entry pointing at a missing DB is a genuine error.
func TestIntegrationProbeProvisionPending(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	// A reachable server, but a database name that does not exist.
	missingDSN := testutil.DefaultDSN + "/bintrail_idx_doesnotexist00"
	req := httptest.NewRequest("POST", "http://127.0.0.1:8090/api/servers/x/test", strings.NewReader(""))

	pending := probeServer(req, missingDSN, true)
	if !pending.ProvisionPending {
		t.Errorf("monitored probe of a missing index DB: ProvisionPending=false, want true (resp=%+v)", pending)
	}
	if pending.OK {
		t.Errorf("a not-yet-provisioned index is not OK: %+v", pending)
	}
	if !strings.Contains(pending.Error, "not provisioned yet") {
		t.Errorf("pending error must be actionable, got %q", pending.Error)
	}

	hard := probeServer(req, missingDSN, false)
	if hard.ProvisionPending {
		t.Errorf("unmonitored probe must NOT be ProvisionPending: %+v", hard)
	}
	if hard.OK || hard.Error == "" {
		t.Errorf("unmonitored probe of a missing DB must be a hard error: %+v", hard)
	}
}

// TestIntegrationEvictOnDSNEdit: editing an entry's DSN closes the cached
// connection; a baseline-only edit keeps it open.
func TestIntegrationEvictOnDSNEdit(t *testing.T) {
	srv, _ := seedConsoleData(t)
	t.Cleanup(srv.cm.CloseAll)
	dbName2 := seedSecondIndex(t)

	_, body := doReq(t, srv, "POST", "/api/servers",
		`{"name":"second","dsn":"`+testutil.IntegrationDSN(dbName2)+`"}`)
	var created serverDTO
	if err := json.Unmarshal(body, &created); err != nil {
		t.Fatal(err)
	}

	// First selection opens and caches the bundle.
	if rec, b := doReqOn(t, srv, created.ID, "GET", "/api/capabilities", ""); rec.Code != 200 {
		t.Fatalf("warm-up: code=%d body=%s", rec.Code, b)
	}
	srv.cm.mu.Lock()
	oldBundle := srv.cm.bundles[created.ID]
	srv.cm.mu.Unlock()
	if oldBundle == nil {
		t.Fatal("bundle not cached after first selection")
	}

	// Baseline-only edit (structured fields unchanged) keeps the same db.
	doReqOn(t, srv, "", "PUT", "/api/servers/"+created.ID,
		`{"name":"second","host":"`+created.Host+`","port":"`+created.Port+`","user":"`+created.User+`","dbname":"`+created.DBName+`","baseline_s3":"s3://b/"}`)
	srv.cm.mu.Lock()
	rebuilt := srv.cm.bundles[created.ID]
	srv.cm.mu.Unlock()
	if rebuilt == nil || rebuilt.db != oldBundle.db {
		t.Error("baseline-only edit must keep the open *sql.DB (no re-Ping)")
	}
	if rebuilt != nil && !rebuilt.baselineConfigured {
		t.Error("baseline-only edit must flip baselineConfigured on the cached bundle")
	}
	if err := oldBundle.db.Ping(); err != nil {
		t.Errorf("db must still be open after a baseline-only edit: %v", err)
	}

	// DSN change (raw dsn with an extra param) evicts and closes.
	rec, b := doReqOn(t, srv, "", "PUT", "/api/servers/"+created.ID,
		`{"name":"second","dsn":"`+testutil.IntegrationDSN(dbName2)+`&timeout=30s"}`)
	if rec.Code != 200 {
		t.Fatalf("dsn edit: code=%d body=%s", rec.Code, b)
	}
	srv.cm.mu.Lock()
	_, stillCached := srv.cm.bundles[created.ID]
	srv.cm.mu.Unlock()
	if stillCached {
		t.Error("DSN edit must evict the cached bundle")
	}
	if err := oldBundle.db.Ping(); err == nil {
		t.Error("DSN edit must Close the evicted connection")
	}

	// And the next selection lazily reopens against the new DSN.
	if rec, b := doReqOn(t, srv, created.ID, "GET", "/api/capabilities", ""); rec.Code != 200 {
		t.Errorf("reopen after DSN edit: code=%d body=%s", rec.Code, b)
	}
}

// TestIntegrationRegistryServerInheritsProcessBaseline (#1010): a server
// added through POST /api/servers (no baseline field) under a daemon started
// with --baseline-dir must come up Time-travel-enabled through the REAL
// lazy-open path: /api/capabilities on the new server reports
// reconstruct:true. The same add under a daemon without a process baseline
// stays gated off.
func TestIntegrationRegistryServerInheritsProcessBaseline(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	dbName2 := seedSecondIndex(t)

	for _, procBaseline := range []bool{true, false} {
		cfg := Config{DB: db, DBName: dbName, Listen: "127.0.0.1:8090", Token: intToken}
		if procBaseline {
			cfg.BaselineDir = t.TempDir()
		}
		srv, err := New(cfg)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(srv.cm.CloseAll)

		rec, body := doReq(t, srv, "POST", "/api/servers",
			`{"name":"second","dsn":"`+testutil.IntegrationDSN(dbName2)+`"}`)
		if rec.Code/100 != 2 {
			t.Fatalf("procBaseline=%v: create code=%d body=%s", procBaseline, rec.Code, body)
		}
		var created serverDTO
		if err := json.Unmarshal(body, &created); err != nil {
			t.Fatal(err)
		}
		if created.Reconstruct != procBaseline {
			t.Errorf("procBaseline=%v: created DTO reconstruct=%v, want %v",
				procBaseline, created.Reconstruct, procBaseline)
		}

		rec, body = doReqOn(t, srv, created.ID, "GET", "/api/capabilities", "")
		if rec.Code != 200 {
			t.Fatalf("procBaseline=%v: capabilities code=%d body=%s", procBaseline, rec.Code, body)
		}
		var caps capabilitiesResponse
		if err := json.Unmarshal(body, &caps); err != nil {
			t.Fatal(err)
		}
		if caps.Reconstruct != procBaseline {
			t.Errorf("procBaseline=%v: lazy-opened registry server reconstruct=%v, want %v",
				procBaseline, caps.Reconstruct, procBaseline)
		}
	}
}

// TestIntegrationRegistryOnlyConsole: no --index-dsn at all — the first saved
// server is the default and serves every surface.
func TestIntegrationRegistryOnlyConsole(t *testing.T) {
	dbName2 := seedSecondIndex(t)

	reg, err := LoadRegistry(t.TempDir() + "/console-servers.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reg.Add(ServerEntry{Name: "only", DSN: testutil.IntegrationDSN(dbName2)}); err != nil {
		t.Fatal(err)
	}
	srv, err := New(Config{Listen: "127.0.0.1:8090", Token: intToken, Registry: reg})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.cm.CloseAll)

	rec, body := doReq(t, srv, "GET", "/api/events?schema=shop&table=orders", "")
	if rec.Code != 200 {
		t.Fatalf("registry-only events: code=%d body=%s", rec.Code, body)
	}
	var er eventsResponse
	if err := json.Unmarshal(body, &er); err != nil {
		t.Fatal(err)
	}
	if er.Count != 1 {
		t.Errorf("registry-only count = %d, want 1", er.Count)
	}
	var resp serversResponse
	_, body = doReq(t, srv, "GET", "/api/servers", "")
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatal(err)
	}
	if len(resp.Servers) != 1 || resp.DefaultID != resp.Servers[0].ID {
		t.Errorf("registry-only list = %+v, want the single entry as default", resp)
	}
}

// statements extracts the executable SQL lines (ignoring comments, blanks, and
// the BEGIN/COMMIT wrapper) so two scripts can be compared without their
// timestamped header comment.
func statements(sql string) []string {
	var out []string
	for _, line := range strings.Split(sql, "\n") {
		l := strings.TrimSpace(line)
		// Skip blanks, comments, the BEGIN/COMMIT wrapper, and any preamble SET
		// (time_zone, sql_mode, …) — undo statements are only UPDATE/INSERT/DELETE,
		// never a session pin, so stripping all SETs keeps this robust as the
		// generator's preamble grows.
		if l == "" || strings.HasPrefix(l, "--") || l == "BEGIN;" || l == "COMMIT;" || strings.HasPrefix(l, "SET ") {
			continue
		}
		out = append(out, l)
	}
	return out
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func contains(s []string, v string) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}

// TestIntegrationSchemasAPIArchiveOnly is the #1065 repro against a real index:
// rotate has archived every partition, so binlog_events is empty while the
// schema snapshot remains. /api/schemas must still list the schema — the UI's
// schema picker is a <select> with no free-text fallback, so returning
// {"schemas":[]} here left the recover page unable to target archived data.
func TestIntegrationSchemasAPIArchiveOnly(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	// A snapshot exists (taken at stream start) but every event has since been
	// rotated out to Parquet/S3 — binlog_events is left empty on purpose.
	testutil.InsertSnapshot(t, db, 1, "2026-06-01 11:00:00",
		"app", "users", "id", 1, "PRI", "int", "NO")

	// NoArchive stays FALSE: this is the reported configuration, where the
	// archives are exactly what still answers /api/events and /api/recover.
	// (handleSchemas never calls FetchMerged, so this opens the gate without
	// touching archive I/O.)
	srv, err := New(Config{
		DB:     db,
		DBName: dbName,
		Listen: "127.0.0.1:8090",
		Token:  intToken,
	})
	if err != nil {
		t.Fatal(err)
	}

	rec, body := doReq(t, srv, "GET", "/api/schemas", "")
	if rec.Code != 200 {
		t.Fatalf("schemas code = %d, body = %s", rec.Code, body)
	}
	var sr schemasResponse
	if err := json.Unmarshal(body, &sr); err != nil {
		t.Fatal(err)
	}
	if !contains(sr.Schemas, "app") {
		t.Fatalf("archive-only schemas = %v, want to include app (#1065)", sr.Schemas)
	}

	// The tables cascade must resolve from the snapshot too, or the picker
	// dead-ends one level down.
	_, body = doReq(t, srv, "GET", "/api/schemas?schema=app", "")
	var tr tablesResponse
	if err := json.Unmarshal(body, &tr); err != nil {
		t.Fatal(err)
	}
	if !contains(tr.Tables, "users") {
		t.Errorf("archive-only tables = %v, want to include users", tr.Tables)
	}
}

// TestIntegrationSchemasAPINoArchiveKeepsLiveOnly pins the other half of the
// #1065 gate: when archives are NOT reachable for this server (--no-archive, or
// any active RBAC profile) a snapshot-only schema is unreachable by
// construction, so it must NOT be advertised. binlog_events is left empty to
// isolate the snapshot path — a schema with live events would be listed either
// way and would not exercise the gate.
func TestIntegrationSchemasAPINoArchiveKeepsLiveOnly(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	testutil.InsertSnapshot(t, db, 1, "2026-06-01 11:00:00",
		"app", "users", "id", 1, "PRI", "int", "NO")

	srv, err := New(Config{
		DB:        db,
		DBName:    dbName,
		Listen:    "127.0.0.1:8090",
		Token:     intToken,
		NoArchive: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	rec, body := doReq(t, srv, "GET", "/api/schemas", "")
	if rec.Code != 200 {
		t.Fatalf("schemas code = %d, body = %s", rec.Code, body)
	}
	var sr schemasResponse
	if err := json.Unmarshal(body, &sr); err != nil {
		t.Fatal(err)
	}
	if len(sr.Schemas) != 0 {
		t.Errorf("--no-archive schemas = %v, want none: archives are unreachable, so a snapshot-only schema must not be offered", sr.Schemas)
	}
}
