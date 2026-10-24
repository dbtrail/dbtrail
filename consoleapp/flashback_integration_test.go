//go:build integration

package consoleapp

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/audittest"
	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/hiderules"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// seedFlashbackIndex builds an index DB holding one myapp.users row (id=1) whose
// name column carries `name`, and returns its DSN. The PK, the AS OF instant,
// and the schema are shared across servers so a wire query distinguishes routing
// purely by the returned name.
func seedFlashbackIndex(t *testing.T, name string, now time.Time) string {
	t.Helper()
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{now})

	// PK snapshot so the wire parser can verify `WHERE id = 1` names the PK
	// (the failure the multi-source footgun surfaced: an empty schema_snapshots
	// fails closed with "cannot verify WHERE column is PK").
	snapTS := now.Add(-time.Hour).Format("2006-01-02 15:04:05")
	testutil.InsertSnapshot(t, db, 1, snapTS, "myapp", "users", "id", 1, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, db, 1, snapTS, "myapp", "users", "name", 2, "", "varchar", "YES")

	eventTS := now.Add(5 * time.Minute).Format("2006-01-02 15:04:05")
	testutil.InsertEvent(t, db, "mysql-bin.000001", 100, 200, eventTS, nil,
		"myapp", "users", 1, "1", nil, nil,
		[]byte(fmt.Sprintf(`{"id":1,"name":%q}`, name)))
	return testutil.IntegrationDSN(dbName)
}

// TestIntegrationFlashbackRoutesByServer proves the issue's acceptance criterion
// 2: one embedded time-travel port (serveFlashback), the same _flashback query,
// routed to two monitored servers by the connection username, returns each
// server's own per-source index data (#996). Auth is the shared console token;
// selection is the username (registry id OR display name).
func TestIntegrationFlashbackRoutesByServer(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	now := time.Now().UTC().Truncate(time.Hour)

	dsnA := seedFlashbackIndex(t, "alice", now)
	dsnB := seedFlashbackIndex(t, "bob", now)

	reg, err := console.LoadRegistry(t.TempDir() + "/servers.yaml")
	if err != nil {
		t.Fatal(err)
	}
	// NoArchive keeps _flashback index-only (no archive_state planner path);
	// SourceDSN seeds the default schema so the wire client needs no USE.
	entA, err := reg.Add(console.ServerEntry{Name: "srva", DSN: dsnA, SourceDSN: "r:p@tcp(x:3306)/myapp", NoArchive: true})
	if err != nil {
		t.Fatal(err)
	}
	entB, err := reg.Add(console.ServerEntry{Name: "srvb", DSN: dsnB, SourceDSN: "r:p@tcp(x:3306)/myapp", NoArchive: true})
	if err != nil {
		t.Fatal(err)
	}

	srv, err := console.New(console.Config{Listen: "127.0.0.1:0", Token: "tok", Registry: reg})
	if err != nil {
		t.Fatal(err)
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	served := make(chan struct{})
	go func() { _ = serveFlashback(ctx, srv, ln, flashbackConfig{}); close(served) }()
	defer func() { cancel(); <-served }()

	asOf := now.Add(10 * time.Minute).Format("2006-01-02 15:04:05")
	q := fmt.Sprintf("SELECT * FROM _flashback.users AS OF '%s' WHERE id = 1", asOf)

	// Recording audit sink: the flashback port authenticates on the shared
	// console token and its username is a server-ROUTING key, so the audit
	// Actor must be the prefixed "server:<name>" sentinel — never the bare
	// username, which a sink would misread as a person (#1123).
	rec := audittest.Install(t)

	routeCases := []struct{ user, want string }{
		{entA.ID, "alice"},
		{entB.ID, "bob"},
		{"srva", "alice"}, // by display name too
		{"srvb", "bob"},
	}
	for _, tc := range routeCases {
		if got := queryFlashbackName(t, ln.Addr().String(), tc.user, "tok", "", q); got != tc.want {
			t.Errorf("user %q: name = %q, want %q (routed to the wrong index?)", tc.user, got, tc.want)
		}
	}
	auditEvs := rec.Events()
	if len(auditEvs) != len(routeCases) {
		t.Fatalf("recorded %d audit events, want %d (one per served query): %+v", len(auditEvs), len(routeCases), auditEvs)
	}
	for i, tc := range routeCases {
		if want := "server:" + tc.user; auditEvs[i].Actor != want {
			t.Errorf("audit actor for user %q = %q, want %q", tc.user, auditEvs[i].Actor, want)
		}
	}

	// A client that sends a default database in the handshake
	// (CLIENT_CONNECT_WITH_DB — mysql -D, a DSN /db path, JDBC) must connect AND
	// its schema must WIN over the SourceDSN seed. entC points at server A's
	// index but declares a DECOY source schema; connecting with handshake DB
	// "myapp" must still resolve `_flashback.users` against myapp (where the
	// data is). This fails two ways if the code regresses: a rejected handshake
	// (pre-bind UseDB not stashed) or the decoy schema winning (replay dropped),
	// either of which returns no rows instead of "alice".
	entC, err := reg.Add(console.ServerEntry{Name: "srvc", DSN: dsnA, SourceDSN: "r:p@tcp(x:3306)/decoyschema", NoArchive: true})
	if err != nil {
		t.Fatal(err)
	}
	if got := queryFlashbackName(t, ln.Addr().String(), entC.ID, "tok", "myapp", q); got != "alice" {
		t.Errorf("handshake DB must win over the SourceDSN seed: name = %q, want alice", got)
	}

	// A server with hidden rules refuses time travel on the port (#2529):
	// it reads the index directly, past the views that apply the rules.
	entD, err := reg.Add(console.ServerEntry{Name: "srvd", DSN: dsnA, SourceDSN: "r:p@tcp(x:3306)/myapp", NoArchive: true})
	if err != nil {
		t.Fatal(err)
	}
	set, err := hiderules.Parse(hiderules.Rules{Columns: []string{"myapp.users.email"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reg.SetHidden(entD.ID, set); err != nil {
		t.Fatal(err)
	}
	func() {
		db := openFlashback(t, ln.Addr().String(), entD.ID, "tok", "")
		defer db.Close()
		var name string
		err := db.QueryRow(q).Scan(&name)
		if err == nil || !strings.Contains(err.Error(), "hides some tables and columns") || name == "alice" {
			t.Errorf("time travel on a server with rules: name=%q err=%v, want refused", name, err)
		}
	}()

	// A wrong password is rejected (auth is the console token).
	if db := openFlashback(t, ln.Addr().String(), entA.ID, "wrong", ""); func() bool {
		defer db.Close()
		_, err := db.Query(q)
		return err == nil
	}() {
		t.Error("a wrong password must be rejected")
	}
	// An unknown server username authenticates on the token but fails on the
	// first query with "no such server" (validity is checked post-handshake).
	if db := openFlashback(t, ln.Addr().String(), "ghost", "tok", ""); func() bool {
		defer db.Close()
		_, err := db.Query(q)
		return err == nil
	}() {
		t.Error("an unknown server username must fail the query")
	}
}

func openFlashback(t *testing.T, addr, user, pass, db string) *sql.DB {
	t.Helper()
	conn, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s)/%s?parseTime=true&timeout=5s", user, pass, addr, db))
	if err != nil {
		t.Fatal(err)
	}
	return conn
}

// queryFlashbackName runs q over a fresh flashback connection (optionally with a
// handshake default database) and returns the `name` column of the row.
func queryFlashbackName(t *testing.T, addr, user, pass, db, q string) string {
	t.Helper()
	conn := openFlashback(t, addr, user, pass, db)
	defer conn.Close()
	rows, err := conn.Query(q)
	if err != nil {
		t.Fatalf("user %q query: %v", user, err)
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}
	if !rows.Next() {
		t.Fatalf("user %q: no rows (routing or seed problem)", user)
	}
	vals := make([]sql.RawBytes, len(cols))
	ptrs := make([]any, len(cols))
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	if err := rows.Scan(ptrs...); err != nil {
		t.Fatal(err)
	}
	for i, c := range cols {
		if c == "name" {
			return string(vals[i])
		}
	}
	t.Fatalf("user %q: no name column in %v", user, cols)
	return ""
}

// writeConsoleBaseline writes a baseline Parquet in the FindBaseline directory
// layout and returns the root dir to set as ServerEntry.BaselineDir (mirrors the
// shim package's writeBaselineSnapshot, which isn't importable from here).
func writeConsoleBaseline(t *testing.T, snapTime time.Time, schema, table string, cols []baseline.Column, rows [][]string) string {
	t.Helper()
	root := t.TempDir()
	dirName := snapTime.UTC().Format("2006-01-02T15-04-05") + "Z"
	tableDir := filepath.Join(root, dirName, schema)
	if err := os.MkdirAll(tableDir, 0o755); err != nil {
		t.Fatalf("mkdir baseline layout: %v", err)
	}
	w, err := baseline.NewWriter(filepath.Join(tableDir, table+".parquet"), cols,
		baseline.WriterConfig{Compression: "none", RowGroupSize: 100})
	if err != nil {
		t.Fatalf("baseline.NewWriter: %v", err)
	}
	nulls := make([]bool, len(cols))
	for _, r := range rows {
		if err := w.WriteRow(r, nulls); err != nil {
			t.Fatalf("WriteRow: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("baseline writer close: %v", err)
	}
	return root
}

// TestIntegrationFlashbackStreamsSnapshotFullTable proves the #998 streaming
// full-table path survives the console's routingHandler proxy: a full-table
// _snapshot AS OF query (no LIMIT) over the embedded flashback port returns the
// whole merged table, streamed row-by-row through the BindConn(mysqlConn) seam
// bindFlashbackHandler wires onto the inner handler. A framing/packet-sequence
// bug in the proxied stream surfaces as a go-sql-driver error, not a silent pass
// — the standalone shim tests would stay green while `bintrail-console watch`
// shipped broken.
func TestIntegrationFlashbackStreamsSnapshotFullTable(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	now := time.Now().UTC().Truncate(time.Hour)

	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{now})
	snapTS := now.Add(-time.Hour).Format("2006-01-02 15:04:05")
	testutil.InsertSnapshot(t, db, 1, snapTS, "myapp", "users", "id", 1, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, db, 1, snapTS, "myapp", "users", "name", 2, "", "varchar", "YES")

	snapTime := now.Add(1 * time.Minute)
	eventTS := now.Add(5 * time.Minute).Format("2006-01-02 15:04:05")
	// Baseline id=1 alice (never touched), id=2 bob (updated), id=3 carol (deleted).
	baselineDir := writeConsoleBaseline(t, snapTime, "myapp", "users",
		[]baseline.Column{
			{Name: "id", MySQLType: "int", ParquetType: baseline.MysqlToParquetNode("int")},
			{Name: "name", MySQLType: "varchar", ParquetType: baseline.MysqlToParquetNode("varchar")},
		},
		[][]string{{"1", "alice"}, {"2", "bob"}, {"3", "carol"}})
	testutil.InsertEvent(t, db, "mysql-bin.000001", 100, 200, eventTS, nil,
		"myapp", "users", 2 /*update*/, "2", nil,
		[]byte(`{"id":2,"name":"bob"}`), []byte(`{"id":2,"name":"bob2"}`))
	testutil.InsertEvent(t, db, "mysql-bin.000001", 200, 300, eventTS, nil,
		"myapp", "users", 3 /*delete*/, "3", nil,
		[]byte(`{"id":3,"name":"carol"}`), nil)
	testutil.InsertEvent(t, db, "mysql-bin.000001", 300, 400, eventTS, nil,
		"myapp", "users", 1 /*insert*/, "4", nil, nil,
		[]byte(`{"id":4,"name":"dave"}`))

	reg, err := console.LoadRegistry(t.TempDir() + "/servers.yaml")
	if err != nil {
		t.Fatal(err)
	}
	ent, err := reg.Add(console.ServerEntry{
		Name: "srv", DSN: testutil.IntegrationDSN(dbName),
		SourceDSN: "r:p@tcp(x:3306)/myapp", NoArchive: true, BaselineDir: baselineDir,
	})
	if err != nil {
		t.Fatal(err)
	}

	srv, err := console.New(console.Config{Listen: "127.0.0.1:0", Token: "tok", Registry: reg})
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	served := make(chan struct{})
	go func() { _ = serveFlashback(ctx, srv, ln, flashbackConfig{}); close(served) }()
	defer func() { cancel(); <-served }()

	asOf := now.Add(10 * time.Minute).Format("2006-01-02 15:04:05")
	conn := openFlashback(t, ln.Addr().String(), ent.ID, "tok", "")
	defer conn.Close()
	rows, err := conn.Query("SELECT * FROM _snapshot.users AS OF '" + asOf + "'")
	if err != nil {
		t.Fatalf("streamed full-table _snapshot over the console proxy failed: %v", err)
	}
	defer rows.Close()

	got := map[string]string{}
	for rows.Next() {
		var id, name sql.NullString
		if err := rows.Scan(&id, &name); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got[id.String] = name.String
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("row iteration (a mid-stream framing error surfaces here): %v", err)
	}
	want := map[string]string{"1": "alice", "2": "bob2", "4": "dave"}
	if len(got) != len(want) {
		t.Fatalf("streamed %d rows %v through the proxy, want %d %v", len(got), got, len(want), want)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("row id=%s = %q, want %q (full: %v)", k, got[k], v, got)
		}
	}
}
