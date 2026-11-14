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

// #2849 against a real MySQL: measure the column definition (type and
// decimals) each side declares for a date or time result.
func TestIntegrationSQLCompareDatetimeScale(t *testing.T) {
	srcDB, srcName := testutil.CreateTestDB(t)
	sourceDSN := testutil.IntegrationDSN(srcName)
	indexDSN := seedFlashbackIndex(t, "alice", time.Now().UTC().Truncate(time.Hour))
	var version string
	if err := srcDB.QueryRow("SELECT VERSION()").Scan(&version); err != nil {
		t.Fatal(err)
	}
	t.Logf("source: %s", version)

	src := openRaw(t, withZone(sourceDSN, "+00:00"))
	if _, err := src.Exec("CREATE TABLE ev (id INT NOT NULL PRIMARY KEY, side VARCHAR(8), k INT, dc DECIMAL(12,2), dt DATETIME, dt3 DATETIME(3), " +
		"ts6 TIMESTAMP(6) NULL, ts TIMESTAMP NULL, d DATE, tm TIME, tm3 TIME(3))"); err != nil {
		t.Fatal(err)
	}
	if _, err := src.Exec("INSERT INTO ev VALUES " +
		"(1, 'live', 1774998900, 1774998900.50, '2026-03-31 23:15:00', '2026-03-31 23:15:00.120', '2026-03-31 23:15:00.000000', '2026-03-31 23:15:00', '2026-03-31', '10:00:00', '10:00:00.500'), " +
		"(2, 'live', 1700000000, 1700000000.00, '2023-11-14 22:13:20', '2023-11-14 22:13:20.000', '2023-11-14 22:13:20.123456', '2023-11-14 22:13:20', '2023-11-14', '-1:00:00', '00:00:00.000'), " +
		"(3, 'live', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)"); err != nil {
		t.Fatal(err)
	}
	if _, err := src.Exec("ANALYZE TABLE ev"); err != nil {
		t.Fatal(err)
	}
	var name, ddl string
	if err := src.QueryRow("SHOW CREATE TABLE ev").Scan(&name, &ddl); err != nil {
		t.Fatal(err)
	}
	rows := sourceText(t, withZone(sourceDSN, "+00:00"), srcName, "SELECT * FROM ev ORDER BY id")
	copyRows := make([][]string, len(rows))
	for i, r := range rows {
		copyRows[i] = append([]string{}, r...)
		copyRows[i][1] = "copy"
	}
	baseDir := t.TempDir()
	writeOrderSnapshot(t, baseDir, srcName, []orderTable{{name: "ev", ddl: ddl + ";\n", rows: rows, copyRows: copyRows, footer: true}})

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
	go func() { _ = serveFlashback(ctx, srv, ln, flashbackConfig{}); close(served) }()
	t.Cleanup(func() { cancel(); <-served })
	routedAddr := serveRouting(t, srv, flashbackConfig{RouteMaxCopyAge: time.Hour, RoutePolicy: readrouter.Policy{ScanRows: 2}})

	cfg, err := drivermysql.ParseDSN(sourceDSN)
	if err != nil {
		t.Fatal(err)
	}
	dial := func(addr, user, pass string) *client.Conn {
		t.Helper()
		c, err := client.Connect(addr, user, pass, srcName)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { c.Close() })
		if _, err := c.Execute("SET time_zone = '+00:00'"); err != nil {
			t.Fatal(err)
		}
		return c
	}
	live := dial(cfg.Addr, cfg.User, cfg.Passwd)
	plain := dial(ln.Addr().String(), ent.ID, "tok")
	routed := dial(routedAddr, ent.ID, "tok")

	for _, stmt := range []string{
		"SELECT side, dt FROM ev ORDER BY id",
		"SELECT side, dt3 FROM ev ORDER BY id",
		"SELECT side, ts6 FROM ev ORDER BY id",
		"SELECT side, ts FROM ev ORDER BY id",
		"SELECT side, d FROM ev ORDER BY id",
		"SELECT side, tm, tm3 FROM ev ORDER BY id",
		"SELECT * FROM ev ORDER BY id",
		"SELECT side, e.dt, e.dt3 FROM ev e ORDER BY id",
		"SELECT side, dt AS x, dt3 AS y FROM ev ORDER BY id",
		"SELECT side, NOW() > '2000-01-01', NOW(), NOW(3), CURRENT_TIMESTAMP(6), SYSDATE(), CURDATE(), CURTIME() FROM ev ORDER BY id",
		"SELECT side, FROM_UNIXTIME(k), FROM_UNIXTIME(dc), FROM_UNIXTIME(1.5), FROM_UNIXTIME(k + 0.0) FROM ev ORDER BY id",
		"SELECT side, FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(dt) / 3600) * 3600) FROM ev ORDER BY id",
		"SELECT side, FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(dt) / 3600) * 3600) AS b, COUNT(*) FROM ev GROUP BY side, b ORDER BY b",
		"SELECT side, CAST(dt3 AS DATETIME), CAST(dt AS DATETIME(3)), CAST(d AS DATETIME), CAST(ts6 AS DATETIME) FROM ev ORDER BY id",
		"SELECT side, DATE_ADD(dt, INTERVAL 1 HOUR), dt3 + INTERVAL 1 SECOND, DATE(dt3), TIMESTAMP(d) FROM ev ORDER BY id",
		"SELECT MIN(side), MIN(dt), MAX(dt3), MAX(ts6), MIN(ts), MAX(d) FROM ev",
		"SELECT side, NOW(6) AS dt, FROM_UNIXTIME(k) AS dt3 FROM ev ORDER BY id",
		"SELECT side, COALESCE(dt, dt3), IFNULL(dt3, dt), GREATEST(dt, dt3) FROM ev ORDER BY id",
		"SELECT side, x FROM (SELECT side, dt3 AS x, id FROM ev) s ORDER BY id",
		"SELECT side, dt FROM ev UNION ALL SELECT side, dt3 FROM ev",
		"SELECT side, STR_TO_DATE('2026-01-01 10:00:00.5', '%Y-%m-%d %H:%i:%s.%f') FROM ev ORDER BY id",
		"SELECT side, TIMESTAMP '2026-01-01 10:00:00.25', DATE '2026-01-01' FROM ev ORDER BY id",
	} {
		for _, side := range []struct {
			name string
			c    *client.Conn
		}{{"plain", plain}, {"routed", routed}} {
			got, want := typedText(t, side.c, stmt), typedText(t, live, stmt)
			gotB, wantB := typedBinary(t, side.c, stmt), typedBinary(t, live, stmt)
			t.Logf("%s %s\n  text   port  %.500s\n  text   MySQL %.500s\n  binary port  %.500s\n  binary MySQL %.500s", side.name, stmt, got, want, gotB, wantB)
		}
	}
}

// typedText is a statement's answer as the text protocol carried it: every
// column's type and decimals, then each row's bytes, \N for NULL; or the
// error.
func typedText(t *testing.T, c *client.Conn, stmt string) string {
	t.Helper()
	res, err := c.Execute(stmt)
	if err != nil {
		return "ERROR " + err.Error()
	}
	// Not closed, as in rawText.
	var rows []string
	for _, row := range res.RowDatas {
		var cells []string
		pos := 0
		for range res.Fields {
			v, isNull, n, err := mysql.LengthEncodedString(row[pos:])
			if err != nil {
				t.Fatal(err)
			}
			pos += n
			if isNull {
				cells = append(cells, `\N`)
			} else {
				cells = append(cells, string(v))
			}
		}
		rows = append(rows, strings.Join(cells, "|"))
	}
	return fieldDefs(res.Fields) + " " + strings.Join(rows, " / ")
}

// typedBinary is the same as a prepared statement got it, each value as
// go-mysql decodes it by its column's decimals.
func typedBinary(t *testing.T, c *client.Conn, stmt string) string {
	t.Helper()
	st, err := c.Prepare(stmt)
	if err != nil {
		return "ERROR " + err.Error()
	}
	defer st.Close()
	res, err := st.Execute()
	if err != nil {
		return "ERROR " + err.Error()
	}
	var rows []string
	for _, row := range res.Values {
		var cells []string
		for _, v := range row {
			switch {
			case v.Type == mysql.FieldValueTypeNull:
				cells = append(cells, `\N`)
			default:
				s := fmt.Sprint(v.Value())
				if b, ok := v.Value().([]byte); ok {
					s = string(b)
				}
				cells = append(cells, s)
			}
		}
		rows = append(rows, strings.Join(cells, "|"))
	}
	return fieldDefs(res.Fields) + " " + strings.Join(rows, " / ")
}

// fieldDefs is each column's MySQL type and decimals, [t12/0 ...].
func fieldDefs(fields []*mysql.Field) string {
	var b strings.Builder
	b.WriteString("[")
	for i, f := range fields {
		if i > 0 {
			b.WriteString(" ")
		}
		fmt.Fprintf(&b, "t%d/%d", f.Type, f.Decimal)
	}
	b.WriteString("]")
	return b.String()
}
