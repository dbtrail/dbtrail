//go:build integration

package consoleapp

import (
	"context"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/client"
	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	drivermysql "github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/readrouter"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// cellKinds renders each cell of a result as NULL or its quoted text.
func cellKinds(r *gomysql.Result) string {
	var out []string
	for _, row := range r.Values {
		for _, v := range row {
			switch {
			case v.Type == gomysql.FieldValueTypeNull:
				out = append(out, "NULL")
			default:
				out = append(out, fmt.Sprintf("%q", fmt.Sprint(string(v.AsString()))))
			}
		}
	}
	return strings.Join(out, " ")
}

// TestIntegrationFlashbackForwardedEmptyString: a forwarded result tells an
// empty string from NULL exactly as the source does, read at the wire level
// (a cell's own NULL marker, not a driver type that hides it), in the text
// protocol and through a prepared statement.
func TestIntegrationFlashbackForwardedEmptyString(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	now := time.Now().UTC().Truncate(time.Hour)
	indexDSN := seedFlashbackIndex(t, "alice", now)
	srcDB, srcName := testutil.CreateTestDB(t)
	for _, q := range []string{
		"CREATE TABLE orders (id INT NOT NULL PRIMARY KEY, status VARCHAR(32))",
		"INSERT INTO orders VALUES (1,'live')",
		"CREATE TABLE e (id INT NOT NULL PRIMARY KEY, v VARCHAR(10), b BLOB, n VARCHAR(10))",
		"INSERT INTO e VALUES (1, '', '', NULL)",
	} {
		if _, err := srcDB.Exec(q); err != nil {
			t.Fatal(err)
		}
	}
	baseDir := t.TempDir()
	writeRoutingBaseline(t, baseDir, srcName, time.Minute, 2)
	reg, _ := console.LoadRegistry(t.TempDir() + "/servers.yaml")
	ent, err := reg.Add(console.ServerEntry{Name: "srva", DSN: indexDSN, SourceDSN: testutil.IntegrationDSN(srcName), BaselineDir: baseDir})
	if err != nil {
		t.Fatal(err)
	}
	srv, err := console.New(console.Config{Listen: "127.0.0.1:0", Token: "tok", Registry: reg})
	if err != nil {
		t.Fatal(err)
	}
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	ctx, cancel := context.WithCancel(context.Background())
	served := make(chan struct{})
	go func() {
		_ = serveFlashback(ctx, srv, ln, flashbackConfig{RouteMaxCopyAge: time.Hour, RoutePolicy: readrouter.Policy{ScanRows: 1000000}})
		close(served)
	}()
	t.Cleanup(func() { cancel(); <-served })

	cfg, _ := drivermysql.ParseDSN(testutil.IntegrationDSN(srcName))
	direct, err := client.Connect(cfg.Addr, cfg.User, cfg.Passwd, srcName)
	if err != nil {
		t.Fatal(err)
	}
	defer direct.Close()
	port, err := client.Connect(ln.Addr().String(), ent.ID, "tok", srcName)
	if err != nil {
		t.Fatal(err)
	}
	defer port.Close()
	for _, q := range []string{"SELECT ''", "SELECT '' AS a, NULL AS b, 'x' AS c", "SELECT v, b, n FROM e WHERE id = 1",
		"SHOW SESSION STATUS LIKE 'Ssl_version_no_such_status'", "SHOW VARIABLES LIKE 'init_connect'",
		"SELECT CAST('' AS BINARY), CAST('' AS CHAR), CONCAT('', ''), NULLIF('', '')"} {
		d, err := direct.Execute(q)
		if err != nil {
			t.Fatal(err)
		}
		p, err := port.Execute(q)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		if cellKinds(d) != cellKinds(p) {
			t.Errorf("text %q: the source answers %s, the port %s", q, cellKinds(d), cellKinds(p))
		}
		if strings.HasPrefix(q, "SHOW") {
			continue
		}
		ds, err := direct.Prepare(q)
		if err != nil {
			t.Fatal(err)
		}
		if d, err = ds.Execute(); err != nil {
			t.Fatal(err)
		}
		ps, err := port.Prepare(q)
		if err != nil {
			t.Fatalf("prepare %s: %v", q, err)
		}
		if p, err = ps.Execute(); err != nil {
			t.Fatalf("execute %s: %v", q, err)
		}
		if cellKinds(d) != cellKinds(p) {
			t.Errorf("prepared %q: the source answers %s, the port %s", q, cellKinds(d), cellKinds(p))
		}
	}
	// The control: the statements above do hold both kinds of cell.
	d, err := direct.Execute("SELECT v, b, n FROM e WHERE id = 1")
	if err != nil || cellKinds(d) != `"" "" NULL` {
		t.Fatalf("the source answers %s (%v), want two empty strings and a NULL", cellKinds(d), err)
	}
}
