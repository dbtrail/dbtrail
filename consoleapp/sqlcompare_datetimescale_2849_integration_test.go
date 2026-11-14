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

// #2849 against a real MySQL: the port declares a date and time column of
// the copy's answer as MySQL does, with the type's fractional seconds (0 for
// a DATETIME and for FROM_UNIXTIME of a whole number, 3 for a DATETIME(3))
// and as a TIMESTAMP where MySQL types it one, and prints each value with
// exactly those digits (12:00:00.120 in a DATETIME(3), where the copy
// printed 12:00:00.12). Compared on the column definitions and the bytes,
// with a raw client, in text and in a prepared statement.
//
// Under routing, a date and time whose digits the copy cannot tell (a cast,
// a literal, a column of a derived table) is MySQL's to answer, and so is a
// DATE stepped by a day, which MySQL answers as a DATE. Without routing the
// copy answers those as before, declared with 6.
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
	for _, stmt := range []string{
		"CREATE TABLE ev (id INT NOT NULL PRIMARY KEY, side VARCHAR(8), k INT, dc DECIMAL(12,2), dt DATETIME, dt3 DATETIME(3), " +
			"ts6 TIMESTAMP(6) NULL, ts TIMESTAMP NULL, d DATE, tm TIME, tm3 TIME(3))",
		"INSERT INTO ev VALUES " +
			"(1, 'live', 1774998900, 1774998900.50, '2026-03-31 23:15:00', '2026-03-31 23:15:00.120', '2026-03-31 23:15:00.000000', '2026-03-31 23:15:00', '2026-03-31', '10:00:00', '10:00:00.500'), " +
			"(2, 'live', 1700000000, 1700000000.00, '2023-11-14 22:13:20', '2023-11-14 22:13:20.000', '2023-11-14 22:13:20.123456', '2023-11-14 22:13:20', '2023-11-14', '-1:00:00', '00:00:00.000'), " +
			"(3, 'live', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)",
		// A star's table: no TIME column, which the copy holds as text.
		"CREATE TABLE ew (id INT NOT NULL PRIMARY KEY, side VARCHAR(8), k INT, dt DATETIME, dt3 DATETIME(3), ts6 TIMESTAMP(6) NULL, d DATE)",
		"INSERT INTO ew SELECT id, side, k, dt, dt3, ts6, d FROM ev",
		"ANALYZE TABLE ev, ew",
	} {
		if _, err := src.Exec(stmt); err != nil {
			t.Fatal(err)
		}
	}
	var tables []orderTable
	for _, name := range []string{"ev", "ew"} {
		var n, ddl string
		if err := src.QueryRow("SHOW CREATE TABLE "+name).Scan(&n, &ddl); err != nil {
			t.Fatal(err)
		}
		rows := sourceText(t, withZone(sourceDSN, "+00:00"), srcName, "SELECT * FROM "+name+" ORDER BY id")
		copyRows := make([][]string, len(rows))
		for i, r := range rows {
			copyRows[i] = append([]string{}, r...)
			copyRows[i][1] = "copy"
		}
		tables = append(tables, orderTable{name: name, ddl: ddl + ";\n", rows: rows, copyRows: copyRows, footer: true})
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

	// same compares one statement on the port with MySQL's own answer, in
	// text and prepared: the date and time columns' definitions, and the
	// rows unless defsOnly (a clock, which may tick between the two).
	same := func(port *client.Conn, name, stmt, who string, defsOnly bool, args ...any) {
		t.Helper()
		for _, binary := range []bool{false, true} {
			got, want := typedAnswer(t, port, stmt, binary, args...), typedAnswer(t, live, stmt, binary, args...)
			t.Logf("%s binary=%v %s\n  port  %.400s\n  MySQL %.400s", name, binary, stmt, got, want)
			if got.err != "" || want.err != "" {
				t.Errorf("%s binary=%v %q: port %q, MySQL %q", name, binary, stmt, got.err, want.err)
				continue
			}
			if who != "" && !strings.HasPrefix(got.rows, who+"|") {
				t.Errorf("%s binary=%v %q: %s, want it answered by %s", name, binary, stmt, got.rows, who)
				continue
			}
			if got.defs != want.defs {
				t.Errorf("%s binary=%v %q: declared %s, MySQL declares %s", name, binary, stmt, got.defs, want.defs)
			}
			if !defsOnly && strings.ReplaceAll(got.rows, "copy|", "live|") != want.rows {
				t.Errorf("%s binary=%v %q:\n  got   %s\n  MySQL %s", name, binary, stmt, got.rows, want.rows)
			}
		}
	}

	// The port without routing: the copy answers, as MySQL declares.
	for _, stmt := range []string{
		"SELECT side, dt FROM ev WHERE k > 0 ORDER BY k",
		"SELECT side, dt3 FROM ev WHERE k > 0 ORDER BY k",
		"SELECT side, ts6, ts FROM ev WHERE k > 0 ORDER BY k",
		"SELECT side, e.dt3 AS x, E.DT, ev2.ts6 FROM ev e JOIN ew ev2 ON ev2.id = e.id WHERE e.k > 0 ORDER BY e.k",
		"SELECT MIN(side), MIN(dt), MAX(dt3), MAX(ts6), MIN(ts), MAX(d) FROM ev",
		"SELECT * FROM ew WHERE k > 0 ORDER BY k",
		"SELECT side, COALESCE(dt, dt3), CASE WHEN k > 1700000000 THEN dt ELSE dt3 END FROM ev WHERE k > 0 ORDER BY k",
		"SELECT side, dt3 FROM ev WHERE k IS NULL",
	} {
		same(plain, "without routing", stmt, "copy", false)
	}
	same(plain, "without routing", "SELECT side, NOW() FROM ev WHERE k > 0 ORDER BY k", "copy", true)
	same(plain, "without routing", "SELECT side, dt3, ts6 FROM ev WHERE k > ? ORDER BY k", "copy", false, 0)
	// What the copy cannot tell the digits of, without routing: as before,
	// declared with 6, the value as it is.
	if got := typedAnswer(t, plain, "SELECT side, FROM_UNIXTIME(k) FROM ev WHERE k > 0 ORDER BY k", false); got.defs != "[t12/6]" ||
		got.rows != "copy|2023-11-14 22:13:20 / copy|2026-03-31 23:15:00" {
		t.Errorf("without routing, FROM_UNIXTIME of a column: %+v, want it declared with 6", got)
	}

	// Under routing: the copy answers what it can declare as MySQL does.
	for _, stmt := range []string{
		"SELECT side, dt FROM ev WHERE k > 0 ORDER BY k",
		"SELECT side, dt3 FROM ev WHERE k > 0 ORDER BY k",
		"SELECT side, ts6, ts FROM ev WHERE k > 0 ORDER BY k",
		"SELECT side, e.dt3 AS x FROM ev e WHERE k > 0 ORDER BY k",
		"SELECT MIN(side), MIN(dt), MAX(dt3), MAX(ts6), MIN(ts), MAX(d) FROM ev",
		"SELECT side, FROM_UNIXTIME(k) FROM ev WHERE k > 0 ORDER BY k",
		// The issue's statement.
		"SELECT side, FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(dt) / 3600) * 3600) FROM ev WHERE k > 0 ORDER BY k",
		"SELECT side, DATE_ADD(dt, INTERVAL 1 HOUR), dt3 + INTERVAL 1 DAY, d + INTERVAL 1 HOUR FROM ev WHERE k > 0 ORDER BY k",
		"SELECT side, dt + INTERVAL 1 MICROSECOND, DATE_SUB(dt3, INTERVAL '2' WEEK) FROM ev WHERE k > 0 ORDER BY k",
		"SELECT side, COALESCE(dt, dt3), CASE WHEN k > 1700000000 THEN dt ELSE dt3 END FROM ev WHERE k > 0 ORDER BY k",
		"SELECT side, MAX(dt3) OVER (ORDER BY k) FROM ev WHERE k > 0 ORDER BY k",
		"SELECT side, dt FROM ev WHERE k > 0 UNION ALL SELECT side, dt3 FROM ev WHERE k > 0 ORDER BY 2",
	} {
		same(routed, "routed", stmt, "copy", false)
	}
	same(routed, "routed", "SELECT side, NOW(), UTC_TIMESTAMP(), CURRENT_TIMESTAMP FROM ev WHERE k > 0 ORDER BY k", "copy", true)
	same(routed, "routed", "SELECT side, dt3, ts6 FROM ev WHERE k > ? ORDER BY k", "copy", false, 0)
	// A star: whoever answers, MySQL's definitions.
	same(routed, "routed", "SELECT * FROM ew WHERE k > 0 ORDER BY k", "", false)
	// And MySQL answers what the copy cannot declare as it does.
	for _, stmt := range []string{
		"SELECT side, x FROM (SELECT side, dt3 AS x, k FROM ev) s WHERE k > 0 ORDER BY k",
		"SELECT side, TIMESTAMP '2026-01-01 10:00:00.25' FROM ev WHERE k > 0 ORDER BY k",
		"SELECT side, CAST(dt3 AS DATETIME) FROM ev WHERE k > 0 ORDER BY k",
		"SELECT side, d + INTERVAL 1 DAY FROM ev WHERE k > 0 ORDER BY k",
	} {
		same(routed, "routed", stmt, "live", false)
	}
}

// dateAnswer is a statement's answer as a raw client read it: the
// definitions of its date and time columns ([t12/0 t7/6]: the type and its
// fractional seconds), and its rows, each cell's bytes (\N for NULL) or, in
// a prepared statement, the value as go-mysql reads it by those digits.
type dateAnswer struct{ defs, rows, err string }

func typedAnswer(t *testing.T, c *client.Conn, stmt string, binary bool, args ...any) dateAnswer {
	t.Helper()
	var res *mysql.Result
	var err error
	if binary {
		st, perr := c.Prepare(stmt)
		if perr != nil {
			return dateAnswer{err: perr.Error()}
		}
		defer st.Close()
		res, err = st.Execute(args...)
	} else {
		if len(args) > 0 {
			stmt = strings.Replace(stmt, "?", fmt.Sprint(args[0]), 1)
		}
		res, err = c.Execute(stmt)
	}
	if err != nil {
		return dateAnswer{err: err.Error()}
	}
	// Not closed, as in rawText.
	var defs []string
	for _, f := range res.Fields {
		switch f.Type {
		case mysql.MYSQL_TYPE_DATE, mysql.MYSQL_TYPE_DATETIME, mysql.MYSQL_TYPE_TIMESTAMP, mysql.MYSQL_TYPE_TIME:
			defs = append(defs, fmt.Sprintf("t%d/%d", f.Type, f.Decimal))
		}
	}
	var rows []string
	if binary {
		for _, row := range res.Values {
			var cells []string
			for _, v := range row {
				s := `\N`
				if v.Type != mysql.FieldValueTypeNull {
					s = fmt.Sprint(v.Value())
					if b, ok := v.Value().([]byte); ok {
						s = string(b)
					}
				}
				cells = append(cells, s)
			}
			rows = append(rows, strings.Join(cells, "|"))
		}
	} else {
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
	}
	return dateAnswer{defs: "[" + strings.Join(defs, " ") + "]", rows: strings.Join(rows, " / ")}
}
