//go:build integration

package consoleapp

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"net/url"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// writeSessionBaseline adds shop.visits to the snapshot: one row whose
// DATETIME and TIMESTAMP hold the same text, with the CREATE TABLE in the
// footer, the way `bintrail baseline` writes one.
func writeSessionBaseline(t *testing.T, dir string) {
	t.Helper()
	createSQL := "CREATE TABLE `visits` (\n  `id` int NOT NULL,\n  `seen_at` datetime DEFAULT NULL,\n" +
		"  `stamped` timestamp NULL DEFAULT NULL,\n  PRIMARY KEY (`id`)\n);\n"
	cols, err := baseline.ParseSchemaText(createSQL)
	if err != nil {
		t.Fatal(err)
	}
	w, err := baseline.NewWriter(filepath.Join(dir, "2026-04-30T03-00-00Z", "shop", "visits.parquet"), cols, baseline.WriterConfig{
		Compression: "none", RowGroupSize: 100, Metadata: map[string]string{baseline.MetaKeyCreateTableSQL: createSQL}})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.WriteRow([]string{"1", "2026-10-04 10:00:00", "2026-10-04 10:00:00"}, make([]bool, 3)); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

// sessionPort serves the embedded port over a real index, a real snapshot and
// the console's real sandbox runner, with the given row cap (0 = default).
func sessionPort(t *testing.T, maxRows int) (addr, user string) {
	t.Helper()
	now := time.Now().UTC().Truncate(time.Hour)
	dsn := seedFlashbackIndex(t, "alice", now)
	baseDir := t.TempDir()
	writeFreeSQLBaseline(t, baseDir)
	writeSessionBaseline(t, baseDir)
	reg, err := console.LoadRegistry(t.TempDir() + "/servers.yaml")
	if err != nil {
		t.Fatal(err)
	}
	ent, err := reg.Add(console.ServerEntry{Name: "srva", DSN: dsn, SourceDSN: "r:p@tcp(x:3306)/shop", BaselineDir: baseDir})
	if err != nil {
		t.Fatal(err)
	}
	srv, err := console.New(console.Config{Listen: "127.0.0.1:0", Token: "tok", Registry: reg,
		SQLLimits: sqlsandbox.Limits{MaxRows: maxRows}, SQLPortMaxRows: maxRows})
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
	return ln.Addr().String(), ent.ID
}

// oneConn is a single connection of the pool: a session setting lives on the
// connection that set it.
func oneConn(t *testing.T, addr, user, params string) *sql.Conn {
	t.Helper()
	db, err := sql.Open("mysql", fmt.Sprintf("%s:tok@tcp(%s)/?timeout=5s%s", user, addr, params))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	c, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("connect (%s): %v", params, err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func connStrings(t *testing.T, c *sql.Conn, q string) [][]string {
	t.Helper()
	rows, err := c.QueryContext(context.Background(), q)
	if err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}
	var out [][]string
	for rows.Next() {
		vals := make([]sql.RawBytes, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatal(err)
		}
		row := make([]string, len(cols))
		for i, v := range vals {
			row[i] = string(v)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	return out
}

func connExec(t *testing.T, c *sql.Conn, q string) error {
	t.Helper()
	_, err := c.ExecContext(context.Background(), q)
	return err
}

// #2035 end to end, over the wire and through the real sandbox: a client that
// sets its time zone gets NOW() and its TIMESTAMP columns in that zone, its
// DATETIME columns as the wall clock they are, and a zone or a mode the port
// cannot honour refused by name. Never an OK followed by a UTC answer.
func TestIntegrationFlashbackSessionSettings(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	addr, user := sessionPort(t, 0)
	ctx := context.Background()

	// NOW() under a zone with no daylight saving, nine hours ahead of UTC.
	tokyo, err := time.LoadLocation("Asia/Tokyo")
	if err != nil {
		t.Fatal(err)
	}
	c := oneConn(t, addr, user, "")
	if got := connStrings(t, c, "SELECT @@time_zone, @@session.sql_mode, @@sql_select_limit"); got[0][0] != "UTC" || got[0][2] != "18446744073709551615" {
		t.Errorf("a fresh connection's variables = %v, want UTC and no select limit", got)
	}
	nowUnder := func(c *sql.Conn, loc *time.Location) time.Duration {
		t.Helper()
		before := time.Now()
		text := connStrings(t, c, "SELECT NOW()")[0][0]
		got, err := time.ParseInLocation("2006-01-02 15:04:05.999999", text, loc)
		if err != nil {
			t.Fatalf("NOW() = %q: %v", text, err)
		}
		return got.Sub(before).Abs()
	}
	if off := nowUnder(c, time.UTC); off > time.Minute {
		t.Fatalf("before any SET, NOW() is %s away from the UTC wall clock", off)
	}
	if err := connExec(t, c, "SET time_zone = 'Asia/Tokyo'"); err != nil {
		t.Fatalf("SET time_zone: %v", err)
	}
	if off := nowUnder(c, tokyo); off > time.Minute {
		t.Errorf("after SET time_zone = 'Asia/Tokyo', NOW() is %s away from Tokyo's wall clock: the setting was accepted and not applied", off)
	}
	// The engine itself runs under the zone, not only the printing of its
	// answer: what a comparison with a literal, or current_date, goes by.
	if got := connStrings(t, c, "SELECT current_setting('TimeZone'), CAST(TIMESTAMPTZ '2026-01-15 03:00:00+00' AS VARCHAR)"); got[0][0] != "Asia/Tokyo" || got[0][1] != "2026-01-15 12:00:00+09" {
		t.Errorf("the copy's session after SET time_zone = 'Asia/Tokyo' = %v, want the zone and an instant cast in it", got)
	}
	if got := connStrings(t, c, "SELECT @@time_zone, @@session.time_zone"); got[0][0] != "Asia/Tokyo" || got[0][1] != "Asia/Tokyo" {
		t.Errorf("@@time_zone after the SET = %v, want what the client set", got)
	}
	// The stored row: the DATETIME is the wall clock MySQL holds, in any
	// zone; the TIMESTAMP is an instant, printed and compared in the
	// session's zone (10:00 UTC is 19:00 in Tokyo).
	got := connStrings(t, c, "SELECT seen_at, stamped FROM visits "+
		"WHERE seen_at >= '2026-10-04 09:30:00' AND seen_at < '2026-10-04 10:30:00' AND stamped = '2026-10-04 19:00:00'")
	if len(got) != 1 || got[0][0] != "2026-10-04 10:00:00" || got[0][1] != "2026-10-04 19:00:00" {
		t.Errorf("visits under Asia/Tokyo = %v, want [[2026-10-04 10:00:00 2026-10-04 19:00:00]]", got)
	}
	// A table whose column types the copy does not record cannot be read
	// under a zone: refused, naming the setting and the table.
	_, err = c.QueryContext(ctx, "SELECT id FROM orders")
	if mysqlCode(err) != 1105 || !strings.Contains(err.Error(), "time_zone") || !strings.Contains(err.Error(), "shop.orders") {
		t.Errorf("orders under a session zone: err = %v, want 1105 naming time_zone and shop.orders", err)
	}
	// The way out still answers under the zone: the schema list reads no table.
	if dbs := connStrings(t, c, "SHOW DATABASES"); len(dbs) == 0 {
		t.Error("SHOW DATABASES under a session zone listed nothing")
	}
	// Back to UTC, the same connection reads it again, and the row as before.
	if err := connExec(t, c, "SET time_zone = '+00:00'"); err != nil {
		t.Fatal(err)
	}
	if got := connStrings(t, c, "SELECT count(*) FROM orders"); got[0][0] != "2" {
		t.Errorf("orders after SET time_zone = '+00:00' = %v, want 2 rows", got)
	}
	if got := connStrings(t, c, "SELECT seen_at, stamped FROM visits"); got[0][0] != "2026-10-04 10:00:00" || got[0][1] != "2026-10-04 10:00:00" {
		t.Errorf("visits under UTC = %v, want both columns as stored", got)
	}

	// An offset, west of UTC, and the time-travel shapes on the same
	// connection keep working.
	west := oneConn(t, addr, user, "")
	if err := connExec(t, west, "SET @@session.time_zone = '-03:00', sql_mode = 'STRICT_TRANS_TABLES,NO_ZERO_DATE'"); err != nil {
		t.Fatalf("combined SET: %v", err)
	}
	if off := nowUnder(west, time.FixedZone("-03:00", -3*3600)); off > time.Minute {
		t.Errorf("after SET time_zone = '-03:00', NOW() is %s away from that zone's wall clock", off)
	}
	if got := connStrings(t, west, "SELECT stamped FROM visits"); got[0][0] != "2026-10-04 07:00:00" {
		t.Errorf("a TIMESTAMP under -03:00 = %v, want 2026-10-04 07:00:00", got)
	}
	if got := connStrings(t, west, "SELECT @@sql_mode"); got[0][0] != "STRICT_TRANS_TABLES,NO_ZERO_DATE" {
		t.Errorf("@@sql_mode = %v, want what the client set", got)
	}

	// Refused by name; the connection keeps what it had.
	for _, tc := range []struct {
		stmt string
		code uint16
		name string
	}{
		{"SET time_zone = '+05:30'", 1298, "+05:30"},
		{"SET time_zone = 'Mars/Olympus'", 1298, "Mars/Olympus"},
		{"SET sql_mode = 'ANSI_QUOTES'", 1231, "ANSI_QUOTES"},
		{"SET sql_mode = 'STRICT_TRANS_TABLES,PIPES_AS_CONCAT'", 1231, "PIPES_AS_CONCAT"},
		{"SET time_zone = 'UTC', sql_mode = 'ANSI'", 1231, "ANSI"},
		{"SET GLOBAL time_zone = 'UTC'", 1228, "time_zone"},
		{"SET sql_select_limit = 0", 1231, "sql_select_limit"},
	} {
		err := connExec(t, west, tc.stmt)
		if mysqlCode(err) != tc.code || !strings.Contains(err.Error(), tc.name) {
			t.Errorf("%s: err = %v, want %d naming %s", tc.stmt, err, tc.code, tc.name)
		}
	}
	if got := connStrings(t, west, "SELECT @@time_zone, @@sql_mode"); got[0][0] != "-03:00" || got[0][1] != "STRICT_TRANS_TABLES,NO_ZERO_DATE" {
		t.Errorf("after the refusals the connection has %v, want what it had", got)
	}

	// Time travel answers in UTC. Under the zone this connection is in it is
	// refused by name, and it is not refused for its zone once back in UTC.
	const travel = "SELECT * FROM _flashback.orders AS OF '2026-10-04 10:00:00' WHERE id = 1"
	if err := connExec(t, west, travel); mysqlCode(err) != 1235 || !strings.Contains(err.Error(), "-03:00") {
		t.Errorf("time travel under -03:00: err = %v, want 1235 naming the zone", err)
	}
	// The connect statement Rails sends: accepted whole, the mode computed.
	if err := connExec(t, west, "SET NAMES utf8mb4, @@SESSION.sql_mode = CONCAT(CONCAT(@@sql_mode, ',STRICT_ALL_TABLES'), ',NO_AUTO_VALUE_ON_ZERO'), @@SESSION.sql_auto_is_null = 0, @@SESSION.wait_timeout = 2147483, @@SESSION.time_zone = '+00:00'"); err != nil {
		t.Fatalf("a driver's connect statement: %v", err)
	}
	if got := connStrings(t, west, "SELECT @@time_zone, @@sql_mode"); got[0][0] != "+00:00" || got[0][1] != "STRICT_TRANS_TABLES,NO_ZERO_DATE,STRICT_ALL_TABLES,NO_AUTO_VALUE_ON_ZERO" {
		t.Errorf("after the connect statement the connection has %v", got)
	}
	if err := connExec(t, west, travel); mysqlCode(err) == 1235 {
		t.Errorf("time travel under UTC is still refused for its zone: %v", err)
	}

	// A real driver's own SET: go-sql-driver sends the DSN's system variables
	// as it connects.
	viaDSN := oneConn(t, addr, user, "&time_zone="+url.QueryEscape("'Asia/Tokyo'")+"&sql_mode="+url.QueryEscape("'STRICT_ALL_TABLES'"))
	if off := nowUnder(viaDSN, tokyo); off > time.Minute {
		t.Errorf("time_zone from the driver's DSN: NOW() is %s away from Tokyo's wall clock", off)
	}
	// And a driver asking for a mode the port refuses cannot connect quietly.
	db, err := sql.Open("mysql", fmt.Sprintf("%s:tok@tcp(%s)/?timeout=5s&sql_mode=%s", user, addr, url.QueryEscape("'ANSI'")))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.PingContext(ctx); err == nil || !strings.Contains(err.Error(), "ANSI") {
		t.Errorf("a connection that sets sql_mode = 'ANSI' opened: err = %v, want the refusal naming it", err)
	}

	// A prepared statement runs under the connection's settings too.
	var stamped string
	if err := c.QueryRowContext(ctx, "SELECT CAST(stamped AS VARCHAR) FROM visits WHERE id = ?", 1).Scan(&stamped); err != nil {
		t.Fatalf("prepared, UTC: %v", err)
	}
	if err := connExec(t, c, "SET time_zone = 'Asia/Tokyo'"); err != nil {
		t.Fatal(err)
	}
	var at string
	if err := c.QueryRowContext(ctx, "SELECT stamped FROM visits WHERE id = ?", 1).Scan(&at); err != nil {
		t.Fatalf("prepared, Tokyo: %v", err)
	}
	if !strings.HasPrefix(at, "2026-10-04 19:00:00") {
		t.Errorf("prepared SELECT of a TIMESTAMP under Asia/Tokyo = %q, want 2026-10-04 19:00:00", at)
	}

	// sql_select_limit: the cut the client asked for, with no warning; the
	// statement's own LIMIT wins.
	lim := oneConn(t, addr, user, "")
	if err := connExec(t, lim, "SET sql_select_limit = 1"); err != nil {
		t.Fatal(err)
	}
	if got := connStrings(t, lim, "SELECT id FROM orders ORDER BY id"); len(got) != 1 || got[0][0] != "1" {
		t.Errorf("sql_select_limit = 1: rows = %v, want the first row alone", got)
	}
	if w := connStrings(t, lim, "SHOW WARNINGS"); len(w) != 0 {
		t.Errorf("a cut the client asked for raised a warning: %v", w)
	}
	if got := connStrings(t, lim, "SELECT id FROM orders ORDER BY id LIMIT 2"); len(got) != 2 {
		t.Errorf("the statement's own LIMIT 2 under sql_select_limit = 1: %d rows, want 2", len(got))
	}
	if err := connExec(t, lim, "SET sql_select_limit = DEFAULT"); err != nil {
		t.Fatal(err)
	}
	if got := connStrings(t, lim, "SELECT id FROM orders ORDER BY id"); len(got) != 2 {
		t.Errorf("after SET sql_select_limit = DEFAULT: %d rows, want 2", len(got))
	}
}
