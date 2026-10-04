//go:build integration

package consoleapp

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	drivermysql "github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/readrouter"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestIntegrationFlashbackReadOnly: the routed port with --route-read-only
// against a real MySQL source (#2079). Every refused statement is checked on
// the SOURCE ITSELF, through a connection that does not go through the port:
// the table still holds its three untouched rows and no file, table or user
// appeared. The same statements on a read-write port do change the source,
// so the check can tell a refusal from a statement that never had an effect.
func TestIntegrationFlashbackReadOnly(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	now := time.Now().UTC().Truncate(time.Hour)
	indexDSN := seedFlashbackIndex(t, "alice", now)

	srcDB, srcName := testutil.CreateTestDB(t)
	for _, q := range []string{
		"CREATE TABLE orders (id INT NOT NULL PRIMARY KEY, status VARCHAR(32))",
		"INSERT INTO orders VALUES (1,'live'),(2,'live'),(3,'live')",
		"ANALYZE TABLE orders",
	} {
		if _, err := srcDB.Exec(q); err != nil {
			t.Fatalf("%.80s: %v", q, err)
		}
	}
	baseDir := t.TempDir()
	writeRoutingBaseline(t, baseDir, srcName, time.Minute, 2)
	reg, err := console.LoadRegistry(t.TempDir() + "/servers.yaml")
	if err != nil {
		t.Fatal(err)
	}
	ent, err := reg.Add(console.ServerEntry{Name: "srva", DSN: indexDSN, SourceDSN: testutil.IntegrationDSN(srcName), BaselineDir: baseDir})
	if err != nil {
		t.Fatal(err)
	}
	srv, err := console.New(console.Config{Listen: "127.0.0.1:0", Token: "tok", Registry: reg, FlashbackListen: "127.0.0.1:3308",
		ReadRouting: console.ReadRoutingConfig{MaxCopyAge: time.Hour, ScanRows: 2, ReadOnly: true}})
	if err != nil {
		t.Fatal(err)
	}
	serve := func(cfg flashbackConfig) string {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		served := make(chan struct{})
		go func() { _ = serveFlashback(ctx, srv, ln, cfg); close(served) }()
		t.Cleanup(func() { cancel(); <-served })
		return ln.Addr().String()
	}
	policy := readrouter.Policy{ScanRows: 2}

	// sourceState is what the source holds, read without the port.
	sourceState := func() string {
		t.Helper()
		var rows, sum, tables int
		var statuses string
		if err := srcDB.QueryRow("SELECT count(*), coalesce(sum(id), 0), coalesce(group_concat(DISTINCT status ORDER BY status), '') FROM orders").Scan(&rows, &sum, &statuses); err != nil {
			t.Fatalf("read the source: %v", err)
		}
		if err := srcDB.QueryRow("SELECT count(*) FROM information_schema.tables WHERE table_schema = ?", srcName).Scan(&tables); err != nil {
			t.Fatalf("read the source's tables: %v", err)
		}
		return fmt.Sprintf("rows %d sum %d statuses %s tables %d", rows, sum, statuses, tables)
	}
	before := sourceState()

	ro := openFlashback(t, serve(flashbackConfig{RouteMaxCopyAge: time.Hour, RoutePolicy: policy, RouteReadOnly: true}), ent.ID, "tok", srcName)
	defer ro.Close()
	ro.SetMaxOpenConns(1)

	writes := []string{
		"INSERT INTO orders VALUES (4,'written')",
		"UPDATE orders SET status = 'written'",
		"DELETE FROM orders",
		"REPLACE INTO orders VALUES (1,'written')",
		"CREATE TABLE made_by_port (a INT)",
		"ALTER TABLE orders ADD COLUMN extra INT",
		"TRUNCATE TABLE orders",
		"DROP TABLE orders",
		"WITH c AS (SELECT 1) DELETE FROM orders",
		"SELECT 1; DELETE FROM orders",
		"/*!50000 DELETE FROM orders */",
		"EXPLAIN ANALYZE DELETE FROM orders",
		"SELECT * FROM orders FOR UPDATE",
		"SELECT * FROM orders INTO OUTFILE '/tmp/dbtrail-readonly-test'",
		"SET GLOBAL max_connections = 151",
		"SET PASSWORD = 'x'",
		"GRANT SELECT ON *.* TO 'nobody'@'localhost'",
		"KILL 1",
		"CREATE TEMPORARY TABLE tmp_by_port (a INT)",
		"LOCK TABLES orders WRITE",
		"PREPARE s FROM 'DELETE FROM orders'",
		"CALL nothing()",
	}
	for _, q := range writes {
		_, err := ro.Exec(q)
		var me *drivermysql.MySQLError
		if !errors.As(err, &me) || me.Number != 1290 || !strings.Contains(me.Message, "--route-read-only") {
			t.Errorf("%q on the read-only port: got %v, want error 1290 naming --route-read-only", q, err)
		}
		if after := sourceState(); after != before {
			t.Fatalf("%q changed the source through the read-only port: %s, was %s", q, after, before)
		}
	}
	// The binary protocol: a write with an argument is a COM_STMT_PREPARE.
	if _, err := ro.Exec("UPDATE orders SET status = ? WHERE id = ?", "written", 1); err == nil || !strings.Contains(err.Error(), "--route-read-only") {
		t.Errorf("prepared UPDATE on the read-only port: got %v, want the read-only refusal", err)
	}
	if after := sourceState(); after != before {
		t.Fatalf("a prepared UPDATE changed the source through the read-only port: %s, was %s", after, before)
	}

	// Reads and the control around them still work, on both sides.
	if got := scanStrings(t, ro, "SELECT status FROM orders WHERE id = 1"); len(got) != 1 || got[0] != "live" {
		t.Errorf("point lookup on the read-only port = %v, want MySQL's row", got)
	}
	if got := scanStrings(t, ro, "SELECT status, count(*) FROM orders GROUP BY status ORDER BY status LIMIT 1"); len(got) == 0 || got[0] != "copy" {
		t.Errorf("full scan on the read-only port = %v, want the copy's row", got)
	}
	var id int
	if err := ro.QueryRow("SELECT id FROM orders WHERE id = ?", 2).Scan(&id); err != nil || id != 2 {
		t.Errorf("prepared read on the read-only port = %d, %v", id, err)
	}
	for _, q := range []string{"SET NAMES utf8mb4", "SET SESSION sql_mode = ''", "BEGIN", "SELECT 1", "SAVEPOINT a", "ROLLBACK TO SAVEPOINT a", "COMMIT", "SHOW TABLES", "DESCRIBE orders", "EXPLAIN SELECT * FROM orders", "USE " + srcName} {
		rows, err := ro.Query(q)
		if err != nil {
			t.Errorf("%q on the read-only port: %v", q, err)
			continue
		}
		rows.Close()
	}

	// The control: on a read-write port the same kind of statement lands.
	rw := openFlashback(t, serve(flashbackConfig{RouteMaxCopyAge: time.Hour, RoutePolicy: policy}), ent.ID, "tok", srcName)
	defer rw.Close()
	if _, err := rw.Exec("INSERT INTO orders VALUES (4,'written')"); err != nil {
		t.Fatalf("INSERT on the read-write port: %v", err)
	}
	if after := sourceState(); after == before {
		t.Fatal("the read-write port did not change the source: the read-only checks above prove nothing")
	}
}
