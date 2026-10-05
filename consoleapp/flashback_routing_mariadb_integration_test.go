//go:build integration

package consoleapp

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/readrouter"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// Read routing with a MariaDB source (#2073). MariaDB prints EXPLAIN
// FORMAT=JSON in its own shape; before it was read, every plan looked free
// and nothing ever went to the copy. The source's orders say "live", the
// copy's say "copy": a full scan must be the copy's, a point lookup the
// source's, as text and as a prepared statement.
func TestIntegrationFlashbackReadRoutingMariaDBSource(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	testutil.SkipIfNoMariaDB(t)
	now := time.Now().UTC().Truncate(time.Hour)
	indexDSN := seedFlashbackIndex(t, "alice", now)

	srcName := fmt.Sprintf("route_maria_%d", time.Now().UnixNano())
	root, err := sql.Open("mysql", testutil.MariaDBBaseDSN()+"/")
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	if _, err := root.Exec("CREATE DATABASE `" + srcName + "`"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = root.Exec("DROP DATABASE `" + srcName + "`") })
	srcDB, err := sql.Open("mysql", testutil.MariaDBBaseDSN()+"/"+srcName)
	if err != nil {
		t.Fatal(err)
	}
	defer srcDB.Close()
	for _, q := range []string{
		"CREATE TABLE orders (id INT NOT NULL PRIMARY KEY, status VARCHAR(32))",
		"INSERT INTO orders VALUES (1,'live'),(2,'live'),(3,'live')",
		"ANALYZE TABLE orders",
	} {
		if _, err := srcDB.Exec(q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}

	baseDir := t.TempDir()
	writeRoutingBaseline(t, baseDir, srcName, time.Minute, 2)
	reg, err := console.LoadRegistry(t.TempDir() + "/servers.yaml")
	if err != nil {
		t.Fatal(err)
	}
	ent, err := reg.Add(console.ServerEntry{Name: "srva", DSN: indexDSN, SourceDSN: testutil.MariaDBBaseDSN() + "/" + srcName + "?parseTime=true", BaselineDir: baseDir})
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
	go func() {
		_ = serveFlashback(ctx, srv, ln, flashbackConfig{RouteMaxCopyAge: time.Hour, RoutePolicy: readrouter.Policy{ScanRows: 2}})
		close(served)
	}()
	t.Cleanup(func() { cancel(); <-served })

	conn := openFlashback(t, ln.Addr().String(), ent.ID, "tok", srcName)
	defer conn.Close()
	first := func(q string, args ...any) string {
		t.Helper()
		var s sql.NullString
		rows, err := conn.Query(q, args...)
		if err != nil {
			t.Fatalf("%s %v: %v", q, args, err)
		}
		defer rows.Close()
		if !rows.Next() {
			t.Fatalf("%s %v: no rows", q, args)
		}
		cols, _ := rows.Columns()
		dest := make([]any, len(cols))
		dest[0] = &s
		for i := 1; i < len(dest); i++ {
			dest[i] = new(sql.RawBytes)
		}
		if err := rows.Scan(dest...); err != nil {
			t.Fatal(err)
		}
		return s.String
	}
	if got := first("SELECT status FROM orders WHERE id = 1"); got != "live" {
		t.Errorf("point lookup answered %q, want live (the source)", got)
	}
	if got := first("SELECT status, count(*) FROM orders GROUP BY status"); got != "copy" {
		t.Errorf("full scan answered %q, want copy: MariaDB's plan was not read", got)
	}
	// The copy's file of orders carries no table definition, so its column
	// set is not known: a NATURAL JOIN, which pairs on that set, and a star
	// are the source's (#2111). The same join with its columns named is not.
	if got := first("SELECT max(a.status) FROM orders a NATURAL JOIN orders b"); got != "live" {
		t.Errorf("NATURAL JOIN over a table with no definition answered %q, want live (the source)", got)
	}
	if got := first("SELECT max(a.status) FROM orders a JOIN orders b USING (id)"); got != "copy" {
		t.Errorf("JOIN ... USING with named columns answered %q, want copy", got)
	}
	if got := first("SELECT a.* FROM orders a ORDER BY a.status"); got != "1" {
		t.Errorf("star over a table with no definition answered %q first, want 1: the source's id column", got)
	}
	if got := first("SELECT status FROM orders WHERE id = ?", 2); got != "live" {
		t.Errorf("prepared point lookup answered %q, want live (the source)", got)
	}
	if got := first("SELECT status, count(*) FROM orders WHERE status <> ? GROUP BY status", "zz"); got != "copy" {
		t.Errorf("prepared full scan answered %q, want copy", got)
	}
}
