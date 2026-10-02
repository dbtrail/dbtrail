//go:build integration

package consoleapp

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	drivermysql "github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/audittest"
	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// writeFreeSQLBaseline writes one snapshot the way the daemon lays one out,
// shop.orders with two rows, through the real Parquet writer. Mirrors the
// console package's own SQL fixture.
func writeFreeSQLBaseline(t *testing.T, dir string) {
	t.Helper()
	path := filepath.Join(dir, "2026-04-30T03-00-00Z", "shop", "orders.parquet")
	schemaFile := filepath.Join(t.TempDir(), "shop.orders-schema.sql")
	ddl := "CREATE TABLE `orders` (\n  `id` int NOT NULL,\n  `status` varchar(32) DEFAULT NULL,\n  PRIMARY KEY (`id`)\n);\n"
	if err := os.WriteFile(schemaFile, []byte(ddl), 0o644); err != nil {
		t.Fatal(err)
	}
	cols, err := baseline.ParseSchema(schemaFile)
	if err != nil {
		t.Fatal(err)
	}
	w, err := baseline.NewWriter(path, cols, baseline.WriterConfig{Compression: "none", RowGroupSize: 100})
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range [][]string{{"1", "new"}, {"2", "paid"}} {
		if err := w.WriteRow(r, []bool{false, false}); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

// The embedded port runs free SQL on the copy end to end: a real index
// behind the registry entry, a real snapshot on disk, the console's REAL
// sandbox runner (this test binary re-executed as the worker, see
// TestMain), and a MySQL client on the wire. One connection does both
// jobs: an ordinary SELECT over the state view, and a time-travel query
// over the index, and each is audited under its own action.
func TestIntegrationFlashbackFreeSQLOnTheCopy(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	now := time.Now().UTC().Truncate(time.Hour)
	dsn := seedFlashbackIndex(t, "alice", now)
	baseDir := t.TempDir()
	writeFreeSQLBaseline(t, baseDir)

	reg, err := console.LoadRegistry(t.TempDir() + "/servers.yaml")
	if err != nil {
		t.Fatal(err)
	}
	// SourceDSN names shop, so the connection's default schema is the one
	// the snapshot's table lives in: no USE needed for `orders`.
	ent, err := reg.Add(console.ServerEntry{Name: "srva", DSN: dsn, SourceDSN: "r:p@tcp(x:3306)/shop", BaselineDir: baseDir})
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
	addr := ln.Addr().String()
	rec := audittest.Install(t)

	conn := openFlashback(t, addr, ent.ID, "tok", "")
	defer conn.Close()

	// An ordinary SELECT, unqualified: the state view in the default schema.
	rows, err := conn.Query("SELECT id, status FROM orders ORDER BY id")
	if err != nil {
		t.Fatalf("free SQL: %v", err)
	}
	cts, err := rows.ColumnTypes()
	if err != nil {
		t.Fatal(err)
	}
	if got := cts[0].DatabaseTypeName() + "/" + cts[1].DatabaseTypeName(); got != "BIGINT/VARCHAR" {
		t.Errorf("wire column types = %s, want BIGINT/VARCHAR (DuckDB INTEGER/VARCHAR mapped)", got)
	}
	var got []string
	for rows.Next() {
		var id int64
		var status string
		if err := rows.Scan(&id, &status); err != nil {
			t.Fatal(err)
		}
		got = append(got, fmt.Sprintf("%d:%s", id, status))
	}
	rows.Close()
	if want := "1:new 2:paid"; strings.Join(got, " ") != want {
		t.Errorf("rows = %v, want %s", got, want)
	}

	// MySQL-shaped metadata over the DuckDB catalog.
	if dbs := scanStrings(t, conn, "SHOW DATABASES"); !contains(dbs, "shop") || !contains(dbs, "main") || contains(dbs, "pg_catalog") {
		t.Errorf("SHOW DATABASES = %v, want shop and main, never pg_catalog", dbs)
	}
	if tbls := scanStrings(t, conn, "SHOW TABLES"); !contains(tbls, "orders") {
		t.Errorf("SHOW TABLES = %v, want orders (the default schema's view)", tbls)
	}
	if cols := scanStrings(t, conn, "SHOW COLUMNS FROM orders"); strings.Join(cols, ",") != "id,status" {
		t.Errorf("SHOW COLUMNS FROM orders = %v, want id,status", cols)
	}

	// Refusals keep their codes: a statement the sandbox will not run is
	// 1064, a DuckDB error is 1105 with DuckDB's words.
	if _, err := conn.Exec("DELETE FROM orders"); mysqlCode(err) != 1064 {
		t.Errorf("DELETE: err = %v, want 1064", err)
	}
	if _, err := conn.Query("SELECT * FROM nope"); mysqlCode(err) != 1105 || !strings.Contains(err.Error(), "does not exist") {
		t.Errorf("SELECT * FROM nope: err = %v, want 1105 naming the missing table", err)
	}

	// The same connection still time-travels over the index: USE moves both
	// the virtual schema's default and free SQL's search path.
	if _, err := conn.Exec("USE myapp"); err != nil {
		t.Fatal(err)
	}
	asOf := now.Add(10 * time.Minute).Format("2006-01-02 15:04:05")
	var id int64
	var name string
	if err := conn.QueryRow(fmt.Sprintf("SELECT * FROM _flashback.users AS OF '%s' WHERE id = 1", asOf)).Scan(&id, &name); err != nil {
		t.Fatalf("time travel after free SQL: %v", err)
	}
	if name != "alice" {
		t.Errorf("time-travel name = %q, want alice", name)
	}
	if _, err := conn.Query("SELECT id FROM orders"); mysqlCode(err) != 1105 {
		t.Errorf("after USE myapp, unqualified orders: err = %v, want 1105 (the view is in shop, not myapp)", err)
	}

	// Audit: each served free-SQL statement is shim/sql.run by the routing
	// actor, with the statement; the time-travel read keeps its own action.
	var sqlRuns, timeTravel int
	for _, ev := range rec.Events() {
		switch ev.Surface + "/" + ev.Action {
		case "shim/sql.run":
			sqlRuns++
			if ev.Actor != "server:"+ent.ID || ev.Detail["sql"] == "" {
				t.Errorf("sql.run event = %+v, want actor server:%s and the statement", ev, ent.ID)
			}
		case "shim/timetravel.query":
			timeTravel++
		}
	}
	if sqlRuns != 4 || timeTravel != 1 {
		t.Errorf("audited %d sql.run and %d timetravel.query, want 4 (the SELECT and three SHOWs) and 1", sqlRuns, timeTravel)
	}

	// The real mysql client, when the machine has one: the CLI's connection
	// chatter and its rendering are what a person sees.
	mysqlCLI, err := exec.LookPath("mysql")
	if err != nil {
		t.Log("no mysql client on PATH; the CLI leg is skipped")
		return
	}
	host, port, _ := net.SplitHostPort(addr)
	out, err := exec.Command(mysqlCLI, "-h", host, "-P", port, "-u", ent.ID, "-ptok", "--batch",
		"-e", "SELECT id, status FROM orders ORDER BY id; SHOW WARNINGS").CombinedOutput()
	if err != nil {
		t.Fatalf("mysql CLI: %v\n%s", err, out)
	}
	if !strings.Contains(string(out), "1\tnew") || !strings.Contains(string(out), "2\tpaid") {
		t.Errorf("mysql CLI output:\n%s\nwant the two rows", out)
	}
	// The table rendering, for the eye (-v shows it): a wrong column type
	// or charset shows up here as hex or misalignment before anywhere else.
	table, err := exec.Command(mysqlCLI, "-h", host, "-P", port, "-u", ent.ID, "-ptok", "--table",
		"-e", "SELECT id, status, id * 2.5 AS x, id > 1 AS big FROM orders ORDER BY id; SHOW DATABASES; SHOW COLUMNS FROM orders").CombinedOutput()
	if err != nil {
		t.Fatalf("mysql CLI (table): %v\n%s", err, table)
	}
	t.Logf("mysql CLI:\n%s", table)
}

func scanStrings(t *testing.T, conn *sql.DB, q string) []string {
	t.Helper()
	rows, err := conn.Query(q)
	if err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for rows.Next() {
		vals := make([]sql.RawBytes, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatal(err)
		}
		out = append(out, string(vals[0]))
	}
	return out
}

func contains(ss []string, s string) bool {
	for _, x := range ss {
		if x == s {
			return true
		}
	}
	return false
}

func mysqlCode(err error) uint16 {
	var me *drivermysql.MySQLError
	if errors.As(err, &me) {
		return me.Number
	}
	return 0
}
