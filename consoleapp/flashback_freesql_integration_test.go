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
	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
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
	// myapp is not a schema of the copy: the default stays, so the copy is
	// still browsable from here, and the failing name says what is missing.
	_, err = conn.Query("SELECT id FROM orders")
	if mysqlCode(err) != 1105 || !strings.Contains(err.Error(), `the current database "myapp" is not in the copy`) {
		t.Errorf("after USE myapp, unqualified orders: err = %v, want 1105 naming the missing database", err)
	}
	if dbs := scanStrings(t, conn, "SHOW DATABASES"); !contains(dbs, "shop") {
		t.Errorf("SHOW DATABASES under a database the copy lacks = %v, want it to still list shop", dbs)
	}
	if n := scanStrings(t, conn, "SELECT count(*) FROM shop.orders"); len(n) != 1 || n[0] != "2" {
		t.Errorf("qualified shop.orders under a database the copy lacks = %v, want [2]", n)
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
	if sqlRuns != 6 || timeTravel != 1 {
		t.Errorf("audited %d sql.run and %d timetravel.query, want 6 (the SELECT, three SHOWs, SHOW DATABASES and the qualified count) and 1", sqlRuns, timeTravel)
	}

	// The real mysql client, when the machine has one: the CLI's connection
	// chatter and its rendering are what a person sees.
	mysqlCLI, err := exec.LookPath("mysql")
	if err != nil {
		t.Skip("no mysql client on PATH; the CLI leg (the client's chatter and rendering) did not run")
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
	rendered := string(table)
	for _, want := range []string{"|  2.5 |", "|    0 |", "|    1 |", "| new    |", "| shop     |", "| status      | VARCHAR"} {
		if !strings.Contains(rendered, want) {
			t.Errorf("mysql CLI table output lacks %q (a wrong column type or a binary charset shows as hex or misaligned)", want)
		}
	}
	if strings.Contains(rendered, "0x") {
		t.Error("mysql CLI rendered a text column as hex: a column was declared with the binary charset")
	}

	// A result with more rows than the row cap (#2037): an ERROR the CLI
	// shows, naming the cap and the way out, never the first rows with a
	// warning a client may not read. Pinned with a 1-row cap.
	capped, err := console.New(console.Config{Listen: "127.0.0.1:0", Token: "tok", Registry: reg,
		SQLLimits: sqlsandbox.Limits{MaxRows: 1}})
	if err != nil {
		t.Fatal(err)
	}
	ln2, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx2, cancel2 := context.WithCancel(context.Background())
	served2 := make(chan struct{})
	go func() { _ = serveFlashback(ctx2, capped, ln2, flashbackConfig{}); close(served2) }()
	defer func() { cancel2(); <-served2 }()
	_, port2, _ := net.SplitHostPort(ln2.Addr().String())
	cappedCLI := func(statements string) (string, error) {
		out, err := exec.Command(mysqlCLI, "-h", host, "-P", port2, "-u", ent.ID, "-ptok", "--table", "--show-warnings",
			"-e", statements).CombinedOutput()
		t.Logf("mysql CLI, 1-row cap, %q:\n%s", statements, out)
		return string(out), err
	}
	cut, err := cappedCLI("SELECT id FROM orders ORDER BY id")
	if err == nil || !strings.Contains(cut, "ERROR 1104") || !strings.Contains(cut, "more than 1 rows") || !strings.Contains(cut, "LIMIT of 1 or less") {
		t.Errorf("two rows under a 1-row cap: err = %v, output:\n%s\nwant ERROR 1104 naming the cap and the LIMIT that avoids it", err, cut)
	}
	if strings.Contains(cut, "|    1 |") {
		t.Errorf("the refused result still delivered rows:\n%s", cut)
	}
	// The way out the error names: the statement's own LIMIT.
	if out, err := cappedCLI("SELECT id FROM orders ORDER BY id LIMIT 1"); err != nil || !strings.Contains(out, "|    1 |") || strings.Contains(out, "|    2 |") {
		t.Errorf("with LIMIT 1 under a 1-row cap: err = %v, output:\n%s\nwant the first row", err, out)
	}
	// A result that fits the cap exactly is whole, not cut.
	if out, err := cappedCLI("SELECT id FROM orders WHERE id = 2"); err != nil || !strings.Contains(out, "|    2 |") {
		t.Errorf("one row under a 1-row cap: err = %v, output:\n%s\nwant the row", err, out)
	}
	// The other way out: the client asks for the cut, and gets it with no
	// error and no warning.
	asked, err := cappedCLI("SET sql_select_limit = 1; SELECT id FROM orders ORDER BY id; SHOW WARNINGS")
	if err != nil || !strings.Contains(asked, "|    1 |") || strings.Contains(asked, "|    2 |") || strings.Contains(asked, "Warning (Code") {
		t.Errorf("SET sql_select_limit = 1 under a 1-row cap: err = %v, output:\n%s\nwant the first row alone, silently", err, asked)
	}
	// The cap still bounds what a client can ask for: a limit above it does
	// not lift it, and the error says so.
	above, err := cappedCLI("SET sql_select_limit = 5; SELECT id FROM orders ORDER BY id")
	if err == nil || !strings.Contains(above, "ERROR 1104") || !strings.Contains(above, "it is 5 now, above the cap") {
		t.Errorf("SET sql_select_limit = 5 under a 1-row cap: err = %v, output:\n%s\nwant ERROR 1104 saying the limit is above the cap", err, above)
	}
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
