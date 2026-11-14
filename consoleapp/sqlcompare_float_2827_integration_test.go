//go:build integration

package consoleapp

import (
	"context"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/readrouter"
	"github.com/dbtrail/dbtrail/internal/testutil"
	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	drivermysql "github.com/go-sql-driver/mysql"
)

// #2827 against a real MySQL: under routing, a statement that names a FLOAT
// column, or has a star over a table with one, is answered by MySQL. MySQL
// compares and computes a FLOAT as a double (3.14 stored in one is
// 3.140000104904175, so WHERE f = 3.14 finds no row), the copy as a float
// (it finds the row, and f * 2 overflows to -Infinity). A statement over the
// same table that does not name the column is still answered by the copy.
//
// Two tables with the same rows: fl's copy holds the text MySQL prints for
// each cell (what a dump holds: 6 significant digits), fx's the exact float
// (what a captured change holds). The side column says who answered.
func TestIntegrationSQLCompareFloatRouted(t *testing.T) {
	srcDB, srcName := testutil.CreateTestDB(t)
	sourceDSN := testutil.IntegrationDSN(srcName)
	indexDSN := seedFlashbackIndex(t, "alice", time.Now().UTC().Truncate(time.Hour))
	var version string
	if err := srcDB.QueryRow("SELECT VERSION()").Scan(&version); err != nil {
		t.Fatal(err)
	}
	t.Logf("source: %s", version)

	src := openRaw(t, sourceDSN)
	var tables []orderTable
	for _, tb := range []struct {
		name  string
		exact bool
	}{{"fl", false}, {"fx", true}} {
		create := "CREATE TABLE " + tb.name + " (id INT NOT NULL PRIMARY KEY, side VARCHAR(8), f FLOAT, f72 FLOAT(7,2), fu FLOAT UNSIGNED, d DOUBLE)"
		if _, err := src.Exec(create); err != nil {
			t.Fatal(err)
		}
		if _, err := src.Exec("INSERT INTO " + tb.name + " VALUES (1,'live',3.14,3.14159,3.14,3.14), (2,'live',NULL,NULL,NULL,NULL), " +
			"(3,'live',1e38,12345.67,3.4e38,1e38), (4,'live',1e-10,-0.01,1e-38,1e-10), (5,'live',123456789,99999.99,0,123456789), " +
			"(6,'live',-0.1,0.1,16777217,-0.1), (7,'live',0.3333333,1.005,2.5,0.3333333), (8,'live',-3.4e38,-99999.99,1.5,-3.4e38), " +
			"(9,'live',0.3333334,0.5,0.5,0.5)"); err != nil {
			t.Fatal(err)
		}
		if _, err := src.Exec("ANALYZE TABLE " + tb.name); err != nil {
			t.Fatal(err)
		}
		var name, ddl string
		if err := src.QueryRow("SHOW CREATE TABLE "+tb.name).Scan(&name, &ddl); err != nil {
			t.Fatal(err)
		}
		cols := "id, side, f, f72, fu, d"
		if tb.exact {
			cols = "id, side, CAST(f AS DOUBLE), CAST(f72 AS DOUBLE), CAST(fu AS DOUBLE), d"
		}
		rows := sourceText(t, sourceDSN, srcName, "SELECT "+cols+" FROM "+tb.name+" ORDER BY id")
		copyRows := make([][]string, len(rows))
		for i, r := range rows {
			copyRows[i] = append([]string{}, r...)
			copyRows[i][1] = "copy"
		}
		tables = append(tables, orderTable{name: tb.name, ddl: ddl + ";\n", rows: rows, copyRows: copyRows, footer: true})
	}
	baseDir := t.TempDir()
	writeOrderSnapshot(t, baseDir, srcName, tables)

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
	routed := openRaw(t, fmt.Sprintf("%s:tok@tcp(%s)/%s?timeout=5s", ent.ID, ln.Addr().String(), srcName))
	live := openRaw(t, sourceDSN)

	cases := []struct {
		stmt string
		args []any // run as a prepared statement when set
		who  string
		why  string
	}{
		{"SELECT side, id FROM %s WHERE f = 3.14", nil, "live", "no row on MySQL, the row on the copy"},
		{"SELECT side, id FROM %s WHERE fu = 3.14", nil, "live", "the UNSIGNED one"},
		{"SELECT side, id FROM %s WHERE `f` = -0.1", nil, "live", "quoted"},
		{"SELECT side, id FROM %s WHERE f IN (3.14, 0.1) ORDER BY id", nil, "live", "IN"},
		{"SELECT side, id FROM %s WHERE f <> 3.14 ORDER BY id", nil, "live", "eight rows on MySQL, seven on the copy"},
		{"SELECT side, id FROM %s WHERE f > 3.14 ORDER BY id", nil, "live", "3.140000104904175 is more than 3.14 on MySQL"},
		{"SELECT side, id FROM %s WHERE f = ?", []any{3.14}, "live", "a placeholder bound to a double"},
		{"SELECT side, id FROM %s WHERE f72 = 3.14", nil, "live", "a FLOAT(7,2)"},
		{"SELECT MIN(side), MIN(f * 2), MAX(f * 2) FROM %s", nil, "live", "-Infinity on the copy"},
		{"SELECT side, f * 2, -f FROM %s ORDER BY f", nil, "live", "a double on MySQL, a float on the copy"},
		{"SELECT MIN(side), SUM(f72), AVG(f72) FROM %s", nil, "live", "MySQL keeps the column's two decimals"},
		{"SELECT side, f, COUNT(*) FROM %s GROUP BY side, f ORDER BY f", nil, "live", "two values a dump prints alike"},
		{"SELECT DISTINCT side, f FROM %s ORDER BY f", nil, "live", "the same"},
		{"SELECT side, CAST(f AS CHAR) FROM %s ORDER BY f", nil, "live", "MySQL's text of a FLOAT"},
		{"SELECT a.side, a.id, b.id FROM %[1]s a JOIN %[1]s b ON a.f = b.d ORDER BY a.id", nil, "live", "a join on it"},
		{"SELECT * FROM %s ORDER BY d", nil, "live", "a star"},
		{"SELECT side, id, d FROM %s WHERE d > 0 ORDER BY d", nil, "copy", "the table's DOUBLE, a full scan: the copy"},
		{"SELECT MIN(side), COUNT(*) FROM %s WHERE id + 0 > 0", nil, "copy", "no FLOAT named"},
	}
	for _, tb := range []string{"fl", "fx"} {
		for _, c := range cases {
			stmt := fmt.Sprintf(c.stmt, tb)
			got := answerText(routed, stmt, c.args...)
			t.Logf("routed %-60s %s", got, stmt)
			if strings.HasPrefix(got, "ERROR ") || !strings.HasPrefix(got, c.who+"|") {
				t.Errorf("routed %q (%s):\n  got  %s\n  want it answered by %s", stmt, c.why, got, c.who)
				continue
			}
			want := answerText(live, stmt, c.args...)
			if strings.ReplaceAll(got, "copy|", "live|") != want {
				t.Errorf("routed %q (%s):\n  got  %s\n  want %s, the source's own answer", stmt, c.why, got, want)
			}
		}
	}
}

// sourceText is every row of a statement as the text MySQL sent for each
// cell, read with a raw client: database/sql's driver parses a FLOAT's text
// into a float32 and prints it again, which is not what a dump holds. \N is
// a NULL, as in mydumper's files.
func sourceText(t *testing.T, dsn, db, stmt string) [][]string {
	t.Helper()
	cfg, err := drivermysql.ParseDSN(dsn)
	if err != nil {
		t.Fatal(err)
	}
	c, err := client.Connect(cfg.Addr, cfg.User, cfg.Passwd, db)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	res, err := c.Execute(stmt)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Close()
	var out [][]string
	for _, row := range res.RowDatas {
		r := make([]string, len(res.Fields))
		pos := 0
		for i := range res.Fields {
			v, isNull, n, err := mysql.LengthEncodedString(row[pos:])
			if err != nil {
				t.Fatal(err)
			}
			pos += n
			r[i] = string(v)
			if isNull {
				r[i] = `\N`
			}
		}
		out = append(out, r)
	}
	return out
}
