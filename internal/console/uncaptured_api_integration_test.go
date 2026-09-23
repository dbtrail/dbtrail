//go:build integration

package console

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/ext"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/status"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// ─── GET /api/uncaptured-tables (#1802) ──────────────────────────────────────
//
// The Overview names every table the current schema snapshot left out. The
// list names tables, so it is scoped exactly like the capture-health names on
// /api/status (#1452): a table the session may not read is counted, never
// named, and its fix (which names it) goes with it. These assert on the whole
// SERIALIZED body, which catches the fix and the sentence as well as the
// name fields.

// seedUncaptured writes an older snapshot that left app.stale_log out and the
// current one, which captures app.users and leaves out app.audit_log and
// app.secrets. The older row must never show: a table fixed and re-read
// drops out of the list.
func seedUncaptured(t *testing.T, srv *Server) {
	t.Helper()
	// These tests are about the RBAC scoping, so the server is one that runs
	// its own boot capture and therefore may claim a count. The cases where
	// the scope is NOT known have their own tests below.
	srv.bootCaptureFilter = &status.CaptureFilter{Known: true}
	db := srv.cm.boot.db
	testutil.MustExec(t, db, metadata.DDLSnapshotExclusions)
	for _, row := range []struct {
		id    int
		table string
	}{{1, "users"}, {1, "stale_log"}, {2, "users"}} {
		testutil.MustExec(t, db, `INSERT INTO schema_snapshots
			(snapshot_id, snapshot_time, schema_name, table_name, column_name, ordinal_position, column_key, data_type, is_nullable)
			VALUES (?, UTC_TIMESTAMP(), 'app', ?, 'id', 1, 'PRI', 'int', 'NO')`, row.id, row.table)
	}
	testutil.MustExec(t, db, `INSERT INTO snapshot_exclusions (snapshot_id, schema_name, table_name, reason, pk_column) VALUES
		(1, 'app', 'stale_log', 'no primary key', 'id'),
		(2, 'app', 'audit_log', 'no primary key', 'id'),
		(2, 'app', 'secrets', 'not InnoDB', NULL)`)
}

func uncapturedFor(t *testing.T, srv *Server, bearer string) (string, status.TableCaptureView) {
	t.Helper()
	rec := getPath(t, srv, "127.0.0.1:8090", "/api/uncaptured-tables", bearer)
	if rec.Code != 200 {
		t.Fatalf("GET /api/uncaptured-tables = %d: %s", rec.Code, rec.Body.String())
	}
	var v status.TableCaptureView
	if err := json.Unmarshal(rec.Body.Bytes(), &v); err != nil {
		t.Fatalf("decode: %v\n%s", err, rec.Body.String())
	}
	return rec.Body.String(), v
}

func assertScopedUncaptured(t *testing.T, body string, v status.TableCaptureView) {
	t.Helper()
	if strings.Contains(body, "secrets") {
		t.Errorf("a table the session may not read is named:\n%s", body)
	}
	if !strings.Contains(body, "app.audit_log is not captured: no primary key.") ||
		!strings.Contains(body, "ALTER TABLE `app`.`audit_log`") {
		t.Errorf("the table the session MAY read must keep its sentence and fix:\n%s", body)
	}
	if v.UncapturedWithheld != 1 || len(v.Uncaptured) != 1 {
		t.Errorf("uncaptured = %+v withheld = %d, want app.audit_log and 1 withheld", v.Uncaptured, v.UncapturedWithheld)
	}
	if v.TablesCaptured == nil || *v.TablesCaptured != 1 || v.TablesTotal == nil || *v.TablesTotal != 3 {
		t.Errorf("counts = %v of %v, want 1 of 3: scoping never changes a count", v.TablesCaptured, v.TablesTotal)
	}
}

// TestIntegrationUncapturedTablesUnrestricted: the current snapshot's list,
// verbatim, and nothing from the older snapshot.
func TestIntegrationUncapturedTablesUnrestricted(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	srv := newProfileIndexServer(t)
	seedUncaptured(t, srv)
	body, v := uncapturedFor(t, srv, "static-tok")
	if v.State != status.TableCaptureChecked || v.SnapshotID != 2 {
		t.Fatalf("view = %+v, want checked on snapshot 2", v)
	}
	if len(v.Uncaptured) != 2 || v.Uncaptured[0].Table != "audit_log" || v.Uncaptured[1].Table != "secrets" {
		t.Errorf("uncaptured = %+v, want audit_log then secrets", v.Uncaptured)
	}
	if strings.Contains(body, "stale_log") {
		t.Errorf("a table only an OLDER snapshot left out is listed:\n%s", body)
	}
	if strings.Contains(body, "uncaptured_withheld") {
		t.Errorf("nothing is withheld from an unrestricted session:\n%s", body)
	}
	if *v.TablesCaptured != 1 || *v.TablesTotal != 3 {
		t.Errorf("counts = %d of %d, want 1 of 3", *v.TablesCaptured, *v.TablesTotal)
	}
}

func TestIntegrationUncapturedTablesPolicyDenyScopesNames(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	srv := newProfileIndexServer(t)
	seedUncaptured(t, srv)
	scoped := restrictedBearer(t, srv, &ext.SessionRestrictions{
		DenyTables: []ext.TableRef{{Schema: "APP", Table: "Secrets"}}, // deny matches case-insensitively
	})
	body, v := uncapturedFor(t, srv, scoped)
	assertScopedUncaptured(t, body, v)
	if body, _ := uncapturedFor(t, srv, "static-tok"); !strings.Contains(body, "app.secrets") {
		t.Errorf("the restriction leaked from the session to the process:\n%s", body)
	}
}

func TestIntegrationUncapturedTablesAllowListScopesNames(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	srv := newProfileIndexServer(t)
	seedUncaptured(t, srv)
	scoped := restrictedBearer(t, srv, &ext.SessionRestrictions{
		AllowTables: []ext.TableRef{{Schema: "app", Table: "users"}, {Schema: "app", Table: "audit_log"}},
	})
	body, v := uncapturedFor(t, srv, scoped)
	assertScopedUncaptured(t, body, v)
}

func TestIntegrationUncapturedTablesProfileScopesNames(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	srv := newProfileIndexServer(t)
	seedSensitiveProfile(t, srv)
	seedUncaptured(t, srv)
	body, v := uncapturedFor(t, srv, scopedBearer(t, srv, "sensitive"))
	assertScopedUncaptured(t, body, v)
	if rec := getPath(t, srv, "127.0.0.1:8090", "/api/uncaptured-tables", scopedBearer(t, srv, "ghost")); rec.Code != 403 {
		t.Errorf("a session with an undefined profile must be refused (403), got %d: %s", rec.Code, rec.Body.String())
	}
}

// TestIntegrationUncapturedTablesStartupFloorScopesNames: the startup
// --profile floor withholds the name from a STATIC token too.
func TestIntegrationUncapturedTablesStartupFloorScopesNames(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	srv, err := New(Config{
		DB: db, DBName: dbName, Listen: "127.0.0.1:8090", Token: "static-tok",
		DenyTables: []query.SchemaTable{{Schema: "app", Table: "secrets"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	seedUncaptured(t, srv)
	body, v := uncapturedFor(t, srv, "static-tok")
	assertScopedUncaptured(t, body, v)
}

// TestIntegrationUncapturedTablesLegacyIndexNotChecked: an index without
// snapshot_exclusions answers "not checked", never an empty checked list.
func TestIntegrationUncapturedTablesLegacyIndexNotChecked(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	srv := newProfileIndexServer(t)
	srv.bootCaptureFilter = &status.CaptureFilter{Known: true}
	testutil.MustExec(t, srv.cm.boot.db, `INSERT INTO schema_snapshots
		(snapshot_id, snapshot_time, schema_name, table_name, column_name, ordinal_position, column_key, data_type, is_nullable)
		VALUES (1, UTC_TIMESTAMP(), 'app', 'users', 'id', 1, 'PRI', 'int', 'NO')`)
	_, v := uncapturedFor(t, srv, "static-tok")
	if v.State != status.TableCaptureNotChecked || v.TablesTotal != nil {
		t.Errorf("legacy index: %+v, want not_checked with no total", v)
	}
}

// registryIndexWith builds a second index holding one current snapshot with
// tables in shop and crm, and exclusions in both, and registers it.
func registryIndexWith(t *testing.T, srv *Server, entry ServerEntry) ServerEntry {
	t.Helper()
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	testutil.MustExec(t, db, metadata.DDLSnapshotExclusions)
	for _, st := range []struct{ schema, table string }{{"shop", "orders"}, {"shop", "customers"}, {"crm", "leads"}} {
		testutil.MustExec(t, db, `INSERT INTO schema_snapshots
			(snapshot_id, snapshot_time, schema_name, table_name, column_name, ordinal_position, column_key, data_type, is_nullable)
			VALUES (4, UTC_TIMESTAMP(), ?, ?, 'id', 1, 'PRI', 'int', 'NO')`, st.schema, st.table)
	}
	testutil.MustExec(t, db, `INSERT INTO snapshot_exclusions (snapshot_id, schema_name, table_name, reason, pk_column) VALUES
		(4, 'shop', 'audit_log', 'no primary key', 'id'),
		(4, 'crm', 'call_log', 'no primary key', 'dbtrail_id')`)
	entry.DSN = testutil.IntegrationDSN(dbName)
	if entry.SourceDSN == "" {
		// A server this console captures FOR. Without a source connection an
		// entry is index-only, nothing here captures it, and the scope is
		// not this process's to claim (firstrun.go and servers_api.go read
		// the same signal).
		entry.SourceDSN = "root:testroot@tcp(127.0.0.1:13306)/"
	}
	added, err := srv.cm.reg.Add(entry)
	if err != nil {
		t.Fatal(err)
	}
	return added
}

// TestIntegrationUncapturedTablesKeepToTheCaptureFilter: a server whose
// capture filter names only shop lists and counts only shop's tables, even
// though its last schema read still holds crm (the filter changed since). The
// filter's case follows what the operator typed.
func TestIntegrationUncapturedTablesKeepToTheCaptureFilter(t *testing.T) {
	srv, _ := seedConsoleData(t)
	t.Cleanup(srv.cm.CloseAll)
	// The scope is known because this process supervises the server's capture.
	srv.monitorCtrl = &stubMonitorCtrl{}
	// Padding is benign (both sides trim); the case must match what the
	// snapshot recorded, which the mis-cased leg below pins.
	entry := registryIndexWith(t, srv, ServerEntry{Name: "shop-db", Schemas: " shop "})

	rec, body := doReqOn(t, srv, entry.ID, "GET", "/api/uncaptured-tables", "")
	if rec.Code != 200 {
		t.Fatalf("code=%d body=%s", rec.Code, body)
	}
	var v status.TableCaptureView
	if err := json.Unmarshal(body, &v); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(body), "crm") {
		t.Errorf("a schema outside the capture filter is listed:\n%s", body)
	}
	if len(v.Uncaptured) != 1 || v.Uncaptured[0].Table != "audit_log" {
		t.Errorf("uncaptured = %+v, want shop.audit_log only", v.Uncaptured)
	}
	if *v.TablesCaptured != 2 || *v.TablesTotal != 3 {
		t.Errorf("counts = %d of %d, want 2 of 3 (shop only)", *v.TablesCaptured, *v.TablesTotal)
	}

	// A registry entry whose schema is typed in the wrong case is the same
	// hazard as a mis-cased --tables entry: capture matches byte for byte, so
	// it would watch nothing while the folded list still showed tables. The
	// list keeps folding (it must not hide an uncaptured table), the count
	// goes.
	miscased := registryIndexWith(t, srv, ServerEntry{Name: "shouty", Schemas: "SHOP"})
	_, body = doReqOn(t, srv, miscased.ID, "GET", "/api/uncaptured-tables", "")
	var wrongCase status.TableCaptureView
	if err := json.Unmarshal(body, &wrongCase); err != nil {
		t.Fatal(err)
	}
	if wrongCase.TablesCaptured != nil || wrongCase.TablesTotal != nil || wrongCase.CoverageNote == "" {
		t.Errorf("a mis-cased schema filter claimed a count: %+v", wrongCase)
	}
	if len(wrongCase.Uncaptured) != 1 || wrongCase.Uncaptured[0].Table != "audit_log" {
		t.Errorf("the list stopped folding schema case: %+v", wrongCase.Uncaptured)
	}

	// Without a filter every schema counts.
	all := registryIndexWith(t, srv, ServerEntry{Name: "all-db"})
	_, body = doReqOn(t, srv, all.ID, "GET", "/api/uncaptured-tables", "")
	if !strings.Contains(string(body), "crm.call_log") || !strings.Contains(string(body), "shop.audit_log") {
		t.Errorf("a server with no filter must list every schema:\n%s", body)
	}
}

// TestIntegrationUncapturedTablesPostgresServerNotApplicable: a PostgreSQL
// server's list is not applicable, whatever its index holds.
func TestIntegrationUncapturedTablesPostgresServerNotApplicable(t *testing.T) {
	srv, _ := seedConsoleData(t)
	t.Cleanup(srv.cm.CloseAll)
	entry := registryIndexWith(t, srv, ServerEntry{Name: "pg", Flavor: FlavorPostgres})
	rec, body := doReqOn(t, srv, entry.ID, "GET", "/api/uncaptured-tables", "")
	if rec.Code != 200 {
		t.Fatalf("code=%d body=%s", rec.Code, body)
	}
	var v status.TableCaptureView
	if err := json.Unmarshal(body, &v); err != nil {
		t.Fatal(err)
	}
	if v.State != status.TableCaptureNotApplicable || len(v.Uncaptured) != 0 || v.TablesCaptured != nil {
		t.Errorf("PostgreSQL server: %s, want not_applicable with no counts and no list", body)
	}
}

// ─── A count is claimed only where the capture scope is known (#1802) ────────
//
// The snapshot records the SCHEMAS capture reads, never the per-table filter
// `watch --tables` / `stream --tables` may also run with, and a table that
// filter drops sits in the snapshot looking captured: the parser drops its
// events with no counter, no log line and nothing in capture_skips. So a
// count taken from the snapshot alone would state full coverage over tables
// nobody is watching. These pin who may claim one.

// TestIntegrationUncapturedTablesBootTableFilterNarrowsTheCount: this process
// runs the boot capture, so it knows its own --tables filter. A table that
// filter leaves out is the operator's own choice: it is neither counted nor
// drawn as something to fix.
func TestIntegrationUncapturedTablesBootTableFilterNarrowsTheCount(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	srv, err := New(Config{
		DB: db, DBName: dbName, Listen: "127.0.0.1:8090", Token: "static-tok",
		BootCaptureFilter: &status.CaptureFilter{Known: true, Schemas: []string{"app"}, Tables: []string{"app.users", "app.audit_log"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	seedUncaptured(t, srv)
	// seedUncaptured widens it; this test is about the narrow one the Config carried.
	srv.bootCaptureFilter = &status.CaptureFilter{Known: true, Schemas: []string{"app"}, Tables: []string{"app.users", "app.audit_log"}}
	body, v := uncapturedFor(t, srv, "static-tok")
	if v.TablesCaptured == nil || *v.TablesCaptured != 1 || v.TablesTotal == nil || *v.TablesTotal != 2 {
		t.Errorf("counts = %v of %v, want 1 of 2 (only the two filtered tables)", v.TablesCaptured, v.TablesTotal)
	}
	if len(v.Uncaptured) != 1 || v.Uncaptured[0].Table != "audit_log" {
		t.Errorf("uncaptured = %+v, want app.audit_log only", v.Uncaptured)
	}
	if strings.Contains(body, "secrets") {
		t.Errorf("a table the operator's own filter leaves out is drawn as a problem:\n%s", body)
	}
	if v.CoverageNote != "" {
		t.Errorf("a known filter must claim its count, note = %q", v.CoverageNote)
	}
}

// TestIntegrationUncapturedTablesUnknownScopeClaimsNoCount: a console that
// starts no capture (read-only `serve`) cannot know what the capturing
// process watches. It still names what the schema read left out — true under
// any filter — and claims no coverage.
func TestIntegrationUncapturedTablesUnknownScopeClaimsNoCount(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	srv := newProfileIndexServer(t)
	seedUncaptured(t, srv)
	srv.bootCaptureFilter = nil // this console starts no capture
	_, v := uncapturedFor(t, srv, "static-tok")
	if v.TablesCaptured != nil || v.TablesTotal != nil || v.Headline != "" {
		t.Errorf("a console that started no capture counted anyway: %+v", v)
	}
	if v.CoverageNote == "" {
		t.Error("the missing count must say why")
	}
	if len(v.Uncaptured) != 2 {
		t.Errorf("uncaptured = %+v, want both still named", v.Uncaptured)
	}
}

// TestIntegrationUncapturedTablesSupervisedServerCounts: a registry server
// this process supervises is captured with the entry's schemas and NO table
// filter (ServerEntry has none), so its count is claimable.
func TestIntegrationUncapturedTablesSupervisedServerCounts(t *testing.T) {
	srv, _ := seedConsoleData(t)
	t.Cleanup(srv.cm.CloseAll)
	srv.monitorCtrl = &stubMonitorCtrl{}
	entry := registryIndexWith(t, srv, ServerEntry{Name: "shop-db", Schemas: "shop",
		SourceDSN: "root:testroot@tcp(127.0.0.1:13306)/"})
	rec, body := doReqOn(t, srv, entry.ID, "GET", "/api/uncaptured-tables", "")
	if rec.Code != 200 {
		t.Fatalf("code=%d body=%s", rec.Code, body)
	}
	var v status.TableCaptureView
	if err := json.Unmarshal(body, &v); err != nil {
		t.Fatal(err)
	}
	if v.TablesCaptured == nil || *v.TablesCaptured != 2 || v.TablesTotal == nil || *v.TablesTotal != 3 {
		t.Errorf("counts = %v of %v, want 2 of 3 for a supervised server", v.TablesCaptured, v.TablesTotal)
	}
	// An entry with NO source connection is not captured by this console
	// either, whatever the control plane is doing for other servers.
	sourceless := registryIndexWith(t, srv, ServerEntry{Name: "index-only"})
	sourceless.SourceDSN = ""
	if err := srv.cm.reg.Update(sourceless); err != nil {
		t.Fatal(err)
	}
	_, body = doReqOn(t, srv, sourceless.ID, "GET", "/api/uncaptured-tables", "")
	var indexOnly status.TableCaptureView
	if err := json.Unmarshal(body, &indexOnly); err != nil {
		t.Fatal(err)
	}
	if indexOnly.TablesCaptured != nil || indexOnly.TablesTotal != nil || indexOnly.CoverageNote == "" {
		t.Errorf("a server with no source connection was counted as captured by this console: %+v", indexOnly)
	}
	if len(indexOnly.Uncaptured) != 2 {
		t.Errorf("its tables left out stay named: %+v", indexOnly.Uncaptured)
	}

	// The same server on a console with no control plane claims nothing. A
	// FRESH value, never the one above: unmarshalling over it would keep the
	// old pointers and the assertion could not fail.
	srv.monitorCtrl = nil
	_, body = doReqOn(t, srv, entry.ID, "GET", "/api/uncaptured-tables", "")
	var unsupervised status.TableCaptureView
	if err := json.Unmarshal(body, &unsupervised); err != nil {
		t.Fatal(err)
	}
	if unsupervised.TablesTotal != nil || unsupervised.TablesCaptured != nil || unsupervised.CoverageNote == "" {
		t.Errorf("without the control plane the count must go: %+v", unsupervised)
	}
	if len(unsupervised.Uncaptured) != 2 {
		t.Errorf("the tables left out stay named: %+v", unsupervised.Uncaptured)
	}
}
