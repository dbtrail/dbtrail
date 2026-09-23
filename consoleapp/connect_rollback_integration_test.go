//go:build integration

package consoleapp

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/testutil"
	"github.com/go-sql-driver/mysql"
)

// The promise of the Connect check (#1803), asserted directly against a real
// MySQL and the real supervisor rather than inferred from a stub: a check that
// fails leaves NOTHING behind — no server in the list, and no per-server
// database on the index server.

// perServerDatabases lists the databases the supervisor provisions, one per
// server (bintrail_idx_<id>).
func perServerDatabases(t *testing.T) []string {
	t.Helper()
	db, err := config.Connect(testutil.BaseDSN() + "/")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rows, err := db.Query(`SHOW DATABASES LIKE 'bintrail\_idx\_%'`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			t.Fatal(err)
		}
		out = append(out, n)
	}
	return out
}

const rollbackListen = "127.0.0.1:18093"

func connectConsole(t *testing.T, sup *monitorSupervisor) (*console.Server, *console.Registry) {
	t.Helper()
	reg, err := console.LoadRegistry(filepath.Join(t.TempDir(), "console-servers.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	srv, err := console.New(console.Config{Listen: rollbackListen, Token: "t", Registry: reg, MonitorCtrl: sup})
	if err != nil {
		t.Fatal(err)
	}
	return srv, reg
}

func postCheck(t *testing.T, srv *console.Server, body string) (int, map[string]any) {
	t.Helper()
	req := httptest.NewRequest("POST", "http://"+rollbackListen+"/api/servers/check", strings.NewReader(body))
	req.Host = rollbackListen
	req.Header.Set("Authorization", "Bearer t")
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	var out map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &out)
	return rec.Code, out
}

func TestIntegrationConnectCheck_aFailedCheckLeavesNothingBehind(t *testing.T) {
	db, name := testutil.CreateTestDB(t)
	testutil.MustExec(t, db, "CREATE TABLE loose (v INT)")

	sup := newMonitorSupervisor(context.Background(), testutil.IntegrationDSN(name), nil, 0)
	srv, reg := connectConsole(t, sup)
	before := perServerDatabases(t)

	cfg, err := mysql.ParseDSN(testutil.BaseDSN() + "/")
	if err != nil {
		t.Fatal(err)
	}
	host, port, _ := strings.Cut(cfg.Addr, ":")
	body, _ := json.Marshal(map[string]string{
		"source_host": host, "source_port": port,
		"source_user": cfg.User, "source_password": cfg.Passwd, "schemas": name,
	})
	code, out := postCheck(t, srv, string(body))
	if code != 200 {
		t.Fatalf("code=%d body=%v", code, out)
	}
	if out["ok"] == true || out["started"] == true {
		// Errorf, not Fatalf: the assertions below are the promise itself and
		// must each report even when this one already failed.
		t.Errorf("a table without a key must refuse the whole server at setup (ok=%v started=%v)", out["ok"], out["started"])
	}
	// It refused for the reason the fixture is built around, not for some
	// unrelated check this test did not mean to exercise.
	doc, _ := out["doctor"].(map[string]any)
	checks, _ := doc["checks"].([]any)
	refusedForKey := false
	for _, c := range checks {
		m, _ := c.(map[string]any)
		if m["kind"] == "no_primary_key" && m["status"] == "fail" {
			refusedForKey = true
		}
	}
	if !refusedForKey {
		t.Errorf("the check did not refuse for the table without a key: %v", checks)
	}
	if n := reg.Len(); n != 0 {
		t.Errorf("the failed check left %d server(s) in the list", n)
	}
	if after := perServerDatabases(t); !slices.Equal(after, before) {
		t.Errorf("the failed check created a per-server database: before %v, after %v", before, after)
	}
	if len(sup.jobs) != 0 {
		t.Errorf("the failed check left %d job slot(s) in the supervisor", len(sup.jobs))
	}
}

// DiscardNew is what the rollback calls when the checks passed and Start then
// failed — possibly after it had already created the per-server database.
func TestIntegrationDiscardNew_dropsOnlyTheDatabaseItDerived(t *testing.T) {
	_, name := testutil.CreateTestDB(t)
	sup := newMonitorSupervisor(context.Background(), testutil.IntegrationDSN(name), nil, 0)

	const id = "0123456789abcdef"
	derived, err := sup.DeriveIndexDSN(id)
	if err != nil {
		t.Fatal(err)
	}
	dcfg, err := mysql.ParseDSN(derived)
	if err != nil {
		t.Fatal(err)
	}
	if err := indexer.EnsureDatabase(dcfg, dcfg.DBName, nil); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { dropIfExists(t, dcfg.DBName) })

	// A server that brought its OWN index is never touched, even with the id
	// whose derived database exists.
	byo := testutil.IntegrationDSN(name)
	if err := sup.DiscardNew(context.Background(), console.ServerEntry{ID: id, DSN: byo}); err != nil {
		t.Fatalf("DiscardNew on a bring-your-own index: %v", err)
	}
	if !slices.Contains(perServerDatabases(t), dcfg.DBName) {
		t.Fatal("DiscardNew dropped the derived database for an entry whose index is its own")
	}
	if !slices.Contains(allDatabases(t), name) {
		t.Fatal("DiscardNew dropped a bring-your-own index")
	}

	// A running job is not a failed first start.
	sup.mu.Lock()
	j := &monitorJob{cancel: func() {}, done: make(chan struct{})}
	j.set("running", "")
	sup.jobs[id] = j
	sup.mu.Unlock()
	if err := sup.DiscardNew(context.Background(), console.ServerEntry{ID: id, DSN: derived}); err == nil {
		t.Fatal("DiscardNew accepted a running server")
	}
	if !slices.Contains(perServerDatabases(t), dcfg.DBName) {
		t.Fatal("DiscardNew dropped the database of a running server")
	}

	// The failed first start it exists for: slot gone, database gone.
	j.set("failed", "boom")
	if err := sup.DiscardNew(context.Background(), console.ServerEntry{ID: id, DSN: derived}); err != nil {
		t.Fatalf("DiscardNew: %v", err)
	}
	if slices.Contains(perServerDatabases(t), dcfg.DBName) {
		t.Error("the derived database is still there")
	}
	if _, ok := sup.jobs[id]; ok {
		t.Error("the failed job slot is still there")
	}
}

func allDatabases(t *testing.T) []string {
	t.Helper()
	db, err := config.Connect(testutil.BaseDSN() + "/")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	return queryStrings(t, db, "SHOW DATABASES")
}

func queryStrings(t *testing.T, db *sql.DB, q string) []string {
	t.Helper()
	rows, err := db.Query(q)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			t.Fatal(err)
		}
		out = append(out, s)
	}
	return out
}

func dropIfExists(t *testing.T, name string) {
	db, err := config.Connect(testutil.BaseDSN() + "/")
	if err != nil {
		return
	}
	defer db.Close()
	_, _ = db.Exec("DROP DATABASE IF EXISTS `" + name + "`")
}
