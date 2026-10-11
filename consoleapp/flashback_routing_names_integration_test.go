//go:build integration

package consoleapp

import (
	"context"
	"database/sql"
	"net"
	"reflect"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/readrouter"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// A statement the copy answers carries the column names the source itself
// gives it. The source is a real MySQL: each statement is sent to it
// directly, and through the port where the copy answers it, and the names
// must be the same. An expression with no alias (MySQL names it its text,
// the copy its own way), a column spelled in another case than declared,
// an alias, and a literal.
func TestIntegrationFlashbackReadRouting_theCopyAnswersUnderTheSourcesColumnNames(t *testing.T) {
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
	sourceDSN := testutil.IntegrationDSN(srcName)
	baseDir := t.TempDir()
	writeRoutingBaseline(t, baseDir, srcName, time.Minute, 2)

	reg, err := console.LoadRegistry(t.TempDir() + "/servers.yaml")
	if err != nil {
		t.Fatal(err)
	}
	ent, err := reg.Add(console.ServerEntry{Name: "srva", DSN: indexDSN, SourceDSN: sourceDSN, RouteDSN: routeAccountDSN(t, sourceDSN), BaselineDir: baseDir})
	if err != nil {
		t.Fatal(err)
	}
	srv, err := console.New(console.Config{Listen: "127.0.0.1:0", Token: "tok", Registry: reg,
		FlashbackListen: "127.0.0.1:3308", ReadRouting: console.ReadRoutingConfig{MaxCopyAge: time.Hour, ScanRows: 2}})
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

	port := openFlashback(t, ln.Addr().String(), ent.ID, "tok", srcName)
	defer port.Close()

	// names runs q and returns its column names and the first cell.
	names := func(db *sql.DB, q string, args ...any) ([]string, string) {
		t.Helper()
		rows, err := db.Query(q, args...)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		defer rows.Close()
		cols, err := rows.Columns()
		if err != nil {
			t.Fatal(err)
		}
		first := ""
		if rows.Next() {
			cells := make([]sql.RawBytes, len(cols))
			dest := make([]any, len(cols))
			for i := range cells {
				dest[i] = &cells[i]
			}
			if err := rows.Scan(dest...); err != nil {
				t.Fatal(err)
			}
			first = string(cells[0])
		}
		return cols, first
	}
	for _, q := range []string{
		"SELECT status, COUNT(*), count(*) + 1, MAX(id) AS biggest, STATUS, 'x' FROM orders GROUP BY status",
		"SELECT Status, Count( * ) FROM orders GROUP BY status",
		"SELECT status AS `a b`, COUNT(*) AS `COUNT(*)`, count(*) FROM orders GROUP BY status",
	} {
		want, sourceSide := names(srcDB, q)
		got, portSide := names(port, q)
		if sourceSide != "live" || portSide != "copy" {
			t.Fatalf("%s: the source answered %q and the port %q; want live and copy, or this test compares nothing", q, sourceSide, portSide)
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("%s:\n the copy's answer is named %q\n the source names it     %q", q, got, want)
		}
	}
	// A prepared statement, under the names the source answered PREPARE with.
	const tmpl = "SELECT status, COUNT(*), MAX(id) + ? FROM orders GROUP BY status"
	want, sourceSide := names(srcDB, tmpl, 1)
	got, portSide := names(port, tmpl, 1)
	if sourceSide != "live" || portSide != "copy" {
		t.Fatalf("prepared: the source answered %q and the port %q; want live and copy", sourceSide, portSide)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("prepared %s:\n the copy's answer is named %q\n the source names it     %q", tmpl, got, want)
	}
}
