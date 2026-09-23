//go:build integration

package consoleapp

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync"
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

func connectConsole(t *testing.T, sup console.MonitorController) (*console.Server, *console.Registry) {
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

	sup := &recordingSupervisor{monitorSupervisor: newMonitorSupervisor(context.Background(), testutil.IntegrationDSN(name), nil, 0)}
	srv, reg := connectConsole(t, sup)
	code, out := postCheck(t, srv, sourceBody(t, name))
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
	// Only the ids THIS test's supervisor handed out are looked at: other
	// packages create bintrail_idx_ databases concurrently, so comparing the
	// whole list before and after was flaky. A refused check must not reach
	// the point of choosing a database at all.
	for _, id := range sup.recorded() {
		t.Errorf("the failed check chose a per-server database for id %s", id)
		if databaseExists(t, "bintrail_idx_"+id) {
			t.Errorf("the failed check created bintrail_idx_%s", id)
			dropIfExists(t, "bintrail_idx_"+id)
		}
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

// recordingSupervisor is the real supervisor, noting every id it derives a
// database for, so a test can look at exactly the databases IT caused.
type recordingSupervisor struct {
	*monitorSupervisor
	mu  sync.Mutex
	ids []string
}

func (r *recordingSupervisor) DeriveIndexDSN(id string) (string, error) {
	r.mu.Lock()
	r.ids = append(r.ids, id)
	r.mu.Unlock()
	return r.monitorSupervisor.DeriveIndexDSN(id)
}

func (r *recordingSupervisor) recorded() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.ids...)
}

var hexID = regexp.MustCompile(`^[0-9a-f]{16}$`)

func databaseExists(t *testing.T, name string) bool {
	t.Helper()
	db, err := config.Connect(testutil.BaseDSN() + "/")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var n int
	if err := db.QueryRow("SELECT COUNT(*) FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = ?", name).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n > 0
}

// sourceBody is a Connect form for the test MySQL, scoped to schema.
func sourceBody(t *testing.T, schema string) string {
	t.Helper()
	cfg, err := mysql.ParseDSN(testutil.BaseDSN() + "/")
	if err != nil {
		t.Fatal(err)
	}
	host, port, _ := strings.Cut(cfg.Addr, ":")
	b, _ := json.Marshal(map[string]string{
		"source_host": host, "source_port": port,
		"source_user": cfg.User, "source_password": cfg.Passwd, "schemas": schema,
	})
	return string(b)
}

// limitedIndexUser creates a MySQL account for the INDEX side that can create
// a database and its tables but cannot write a row, so a real start fails
// right AFTER it has created the per-server database: at the first INSERT.
// withDrop decides whether the rollback may drop that database again. The
// account is created and removed by exact name.
func limitedIndexUser(t *testing.T, name string, withDrop bool) string {
	t.Helper()
	root, err := config.Connect(testutil.BaseDSN() + "/")
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	privs := "CREATE, SELECT"
	if withDrop {
		privs = "CREATE, DROP, SELECT"
	}
	for _, q := range []string{
		"DROP USER IF EXISTS '" + name + "'@'%'",
		"CREATE USER '" + name + "'@'%' IDENTIFIED BY 'Ct1803-limited'",
		"GRANT " + privs + " ON *.* TO '" + name + "'@'%'",
	} {
		if _, err := root.Exec(q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	t.Cleanup(func() {
		if db, err := config.Connect(testutil.BaseDSN() + "/"); err == nil {
			_, _ = db.Exec("DROP USER IF EXISTS '" + name + "'@'%'")
			db.Close()
		}
	})
	cfg, err := mysql.ParseDSN(testutil.BaseDSN() + "/ct1803_boot")
	if err != nil {
		t.Fatal(err)
	}
	cfg.User, cfg.Passwd = name, "Ct1803-limited"
	return cfg.FormatDSN()
}

// The whole rollback, end to end, through the real /api/servers/check: every
// check passes, the real start creates the per-server database and then dies
// at its first INSERT. Nothing may remain — no server, no job slot, and the
// database dropped again — and the answer must not claim anything was kept.
func TestIntegrationConnectCheck_aStartThatFailsAfterCreatingItsDatabaseLeavesNothing(t *testing.T) {
	db, schema := testutil.CreateTestDB(t)
	testutil.MustExec(t, db, "CREATE TABLE keyed (id INT PRIMARY KEY)")
	sup := &recordingSupervisor{monitorSupervisor: newMonitorSupervisor(context.Background(),
		limitedIndexUser(t, "ct1803_noinsert", true), nil, 0)}
	srv, reg := connectConsole(t, sup)

	code, out := postCheck(t, srv, sourceBody(t, schema))
	if code != 200 {
		t.Fatalf("code=%d body=%v", code, out)
	}
	if out["started"] == true {
		t.Fatalf("the start succeeded with an index account that cannot INSERT: %v", out)
	}
	if doc, _ := out["doctor"].(map[string]any); doc["failed"] != float64(0) {
		t.Fatalf("a check failed, so this never reached the start it is about: %v", doc)
	}
	ids := sup.recorded()
	if len(ids) != 1 || !hexID.MatchString(ids[0]) {
		t.Fatalf("derived ids = %v, want exactly one fresh id", ids)
	}
	if !strings.Contains(fmt.Sprint(out["error"]), "INSERT") && !strings.Contains(fmt.Sprint(out["error"]), "denied") {
		t.Errorf("the start did not fail where this test means it to (at the first INSERT): %v", out["error"])
	}
	if reg.Len() != 0 {
		t.Errorf("the failed start left %d server(s) in the list", reg.Len())
	}
	if len(sup.jobs) != 0 {
		t.Errorf("the failed start left %d job slot(s)", len(sup.jobs))
	}
	if databaseExists(t, "bintrail_idx_"+ids[0]) {
		t.Errorf("bintrail_idx_%s outlived the rollback", ids[0])
		dropIfExists(t, "bintrail_idx_"+ids[0])
	}
	if out["kept"] == true {
		t.Errorf("kept=true although everything was taken back: %v", out)
	}
}

// The same, with an account that cannot drop the database either: something
// IS left behind, and the answer has to say so.
func TestIntegrationConnectCheck_aDatabaseThatCannotBeDroppedIsReportedKept(t *testing.T) {
	db, schema := testutil.CreateTestDB(t)
	testutil.MustExec(t, db, "CREATE TABLE keyed (id INT PRIMARY KEY)")
	sup := &recordingSupervisor{monitorSupervisor: newMonitorSupervisor(context.Background(),
		limitedIndexUser(t, "ct1803_nodrop", false), nil, 0)}
	srv, reg := connectConsole(t, sup)

	_, out := postCheck(t, srv, sourceBody(t, schema))
	ids := sup.recorded()
	for _, id := range ids {
		if hexID.MatchString(id) {
			t.Cleanup(func() { dropIfExists(t, "bintrail_idx_"+id) })
		}
	}
	if out["started"] == true {
		t.Fatalf("the start succeeded with an index account that cannot INSERT: %v", out)
	}
	if len(ids) != 1 || !databaseExists(t, "bintrail_idx_"+ids[0]) {
		t.Fatalf("the fixture did not leave the database it is about (ids %v)", ids)
	}
	if out["kept"] != true {
		t.Errorf("the per-server database is still there and the answer does not say so: %v", out)
	}
	if reg.Len() != 0 {
		t.Errorf("%d server(s) left in the list", reg.Len())
	}
}
