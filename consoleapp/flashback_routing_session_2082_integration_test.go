//go:build integration

package consoleapp

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/readrouter"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// visitRows are the three rows of <schema>.visits on the source: a DATETIME
// and a TIMESTAMP holding the same text, written under UTC. One in winter,
// one in summer, one late enough that a zone east of UTC moves the day.
var visitRows = [][2]string{
	{"1", "2026-01-15 10:00:00"},
	{"2", "2026-07-01 10:00:00"},
	{"3", "2026-07-01 23:30:00"},
}

const visitsCreate = "CREATE TABLE `visits` (\n  `id` int NOT NULL,\n  `side` varchar(8) DEFAULT NULL,\n  `code` char(6) DEFAULT NULL,\n" +
	"  `seen_at` datetime DEFAULT NULL,\n  `stamped` timestamp NULL DEFAULT NULL,\n  PRIMARY KEY (`id`)\n);\n"

// writeVisitsBaseline writes the copy of <schema>.visits with the same times
// as the source and `side` in every row, the CREATE TABLE in the footer (the
// way `bintrail baseline` writes one), so the copy can tell the DATETIME
// from the TIMESTAMP.
func writeVisitsBaseline(t *testing.T, dir, schema, side string) {
	t.Helper()
	cols, err := baseline.ParseSchemaText(visitsCreate)
	if err != nil {
		t.Fatal(err)
	}
	snap := time.Now().UTC().Add(-time.Minute).Format("2006-01-02T15-04-05Z")
	w, err := baseline.NewWriter(filepath.Join(dir, snap, schema, "visits.parquet"), cols, baseline.WriterConfig{
		Compression: "none", RowGroupSize: 100, Metadata: map[string]string{baseline.MetaKeyCreateTableSQL: visitsCreate}})
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range visitRows {
		if err := w.WriteRow([]string{r[0], side, "ab", r[1], r[1]}, make([]bool, 5)); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

// routedConn is one connection to the port: a session lives on the
// connection. No parseTime: the tests compare the text the protocol carried.
func routedConn(t *testing.T, addr, user, db, params string) *sql.Conn {
	t.Helper()
	pool, err := sql.Open("mysql", fmt.Sprintf("%s:tok@tcp(%s)/%s?timeout=5s%s", user, addr, db, params))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { pool.Close() })
	c, err := pool.Conn(context.Background())
	if err != nil {
		t.Fatalf("connect (%s): %v", params, err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

// directConn is one connection straight to the source: what MySQL itself
// answers, with no port between.
func directConn(t *testing.T, dsn string) *sql.Conn {
	t.Helper()
	pool, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { pool.Close() })
	c, err := pool.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

// A full scan (the copy's, by the scan rule at 2 rows): the times, and the
// CHAR column with a mark after it, which is where PAD_CHAR_TO_FULL_LENGTH
// shows.
const visitsScan = "SELECT side, seen_at, stamped, CONCAT(code, '|'), count(*) FROM visits GROUP BY side, seen_at, stamped, code ORDER BY seen_at"

// The same three rows as primary-key lookups, which MySQL always answers.
func visitsByLookup(t *testing.T, c *sql.Conn) [][]string {
	t.Helper()
	var out [][]string
	for _, r := range visitRows {
		rows := connStrings(t, c, "SELECT side, seen_at, stamped, CONCAT(code, '|'), 1 FROM visits WHERE id = "+r[0])
		if len(rows) != 1 {
			t.Fatalf("lookup of visit %s: %d rows", r[0], len(rows))
		}
		out = append(out, rows[0])
	}
	return out
}

// sidesOf is the first column of every row, de-duplicated: who answered.
func sidesOf(rows [][]string) string {
	seen := map[string]bool{}
	var out []string
	for _, r := range rows {
		if !seen[r[0]] {
			seen[r[0]] = true
			out = append(out, r[0])
		}
	}
	return strings.Join(out, ",")
}

// restOf drops the side column, leaving what the two sides must agree on.
func restOf(rows [][]string) [][]string {
	out := make([][]string, len(rows))
	for i, r := range rows {
		out[i] = r[1:]
	}
	return out
}

// routedSessionRig is a real source (MySQL or MariaDB) whose visits say
// "live", a copy whose visits say "copy" with the same times, and a routing
// port over both.
type routedSessionRig struct {
	srv     *console.Server
	entID   string
	deadID  string // a server whose source is unreachable
	addr    string
	srcName string
	srcDSN  string // straight to the source's schema
	src     *sql.DB
	index   *sql.DB
	now     time.Time
}

func newRoutedSessionRig(t *testing.T, baseDSN string) *routedSessionRig {
	t.Helper()
	rig := &routedSessionRig{now: time.Now().UTC().Truncate(time.Hour)}
	indexDSN := seedFlashbackIndex(t, "alice", rig.now)
	var err error
	if rig.index, err = sql.Open("mysql", indexDSN); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { rig.index.Close() })
	rig.srcName = fmt.Sprintf("routed_sess_%d", time.Now().UnixNano())
	root, err := sql.Open("mysql", baseDSN+"/")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := root.Exec("CREATE DATABASE `" + rig.srcName + "`"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = root.Exec("DROP DATABASE `" + rig.srcName + "`"); root.Close() })
	rig.srcDSN = baseDSN + "/" + rig.srcName + "?timeout=5s"
	if rig.src, err = sql.Open("mysql", rig.srcDSN); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { rig.src.Close() })
	ins := directConn(t, rig.srcDSN)
	ctx := context.Background()
	for _, q := range []string{"SET time_zone = '+00:00'", visitsCreate} {
		if _, err := ins.ExecContext(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	for _, r := range visitRows {
		if _, err := ins.ExecContext(ctx, "INSERT INTO visits VALUES (?, 'live', 'ab', ?, ?)", r[0], r[1], r[1]); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := ins.ExecContext(ctx, "ANALYZE TABLE visits"); err != nil {
		t.Fatal(err)
	}
	// A row the time-travel statements read: <source schema>.users, id 1.
	snapTS := rig.now.Add(-time.Hour).Format("2006-01-02 15:04:05")
	testutil.InsertSnapshot(t, rig.index, 1, snapTS, rig.srcName, "users", "id", 1, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, rig.index, 1, snapTS, rig.srcName, "users", "name", 2, "", "varchar", "YES")
	testutil.InsertEvent(t, rig.index, "mysql-bin.000001", 300, 400, rig.now.Add(5*time.Minute).Format("2006-01-02 15:04:05"), nil,
		rig.srcName, "users", 1, "1", nil, nil, []byte(`{"id":1,"name":"traveller"}`))

	baseDir := t.TempDir()
	writeVisitsBaseline(t, baseDir, rig.srcName, "copy")
	reg, err := console.LoadRegistry(t.TempDir() + "/servers.yaml")
	if err != nil {
		t.Fatal(err)
	}
	ent, err := reg.Add(console.ServerEntry{Name: "srva", DSN: indexDSN, SourceDSN: baseDSN + "/" + rig.srcName, BaselineDir: baseDir})
	if err != nil {
		t.Fatal(err)
	}
	rig.entID = ent.ID
	// The same index and copy behind a source nobody answers at: a
	// connection the source never lets in.
	dead, err := reg.Add(console.ServerEntry{Name: "srvdead", DSN: indexDSN, SourceDSN: "root:testroot@tcp(127.0.0.1:1)/" + rig.srcName, BaselineDir: baseDir})
	if err != nil {
		t.Fatal(err)
	}
	rig.deadID = dead.ID
	rig.srv, err = console.New(console.Config{Listen: "127.0.0.1:0", Token: "tok", Registry: reg,
		FlashbackListen: "127.0.0.1:3308", ReadRouting: console.ReadRoutingConfig{MaxCopyAge: time.Hour, ScanRows: 2}})
	if err != nil {
		t.Fatal(err)
	}
	rig.addr = serveRouting(t, rig.srv, flashbackConfig{RouteMaxCopyAge: time.Hour, RoutePolicy: readrouter.Policy{ScanRows: 2}})
	return rig
}

func (rig *routedSessionRig) conn(t *testing.T, params string) *sql.Conn {
	t.Helper()
	return routedConn(t, rig.addr, rig.entID, rig.srcName, params)
}

// scan runs the full scan on a routed connection and holds it to the one
// invariant every case shares: whoever answered, the answer is what MySQL
// gives for the same rows on the same connection (the primary-key lookups).
// wantSide is who must have answered.
func (rig *routedSessionRig) scan(t *testing.T, c *sql.Conn, wantSide string) [][]string {
	t.Helper()
	got := connStrings(t, c, visitsScan)
	if s := sidesOf(got); s != wantSide {
		t.Errorf("the full scan was answered by %q, want %s", s, wantSide)
	}
	mysqlRows := visitsByLookup(t, c)
	if s := sidesOf(mysqlRows); s != "live" {
		t.Fatalf("the primary-key lookups were answered by %q, want MySQL", s)
	}
	if !reflect.DeepEqual(restOf(got), restOf(mysqlRows)) {
		t.Errorf("the answer differs from MySQL's on the same connection:\n answered by %s: %v\n mysql:        %v", sidesOf(got), restOf(got), restOf(mysqlRows))
	}
	return restOf(got)
}

// tally reads the routing reasons the Connect page shows for the server.
func (rig *routedSessionRig) tally(t *testing.T) (copyN, mysqlN uint64, reasons map[string]uint64) {
	t.Helper()
	req := httptest.NewRequest("GET", "http://127.0.0.1/api/flashback", nil)
	req.Header.Set("Authorization", "Bearer tok")
	rec := httptest.NewRecorder()
	rig.srv.Handler().ServeHTTP(rec, req)
	var fb struct {
		Routing struct {
			Servers map[string]struct {
				Copy    uint64            `json:"copy"`
				MySQL   uint64            `json:"mysql"`
				Reasons map[string]uint64 `json:"reasons"`
			} `json:"servers"`
		} `json:"routing"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &fb); err != nil {
		t.Fatalf("decode /api/flashback: %v (%s)", err, rec.Body.String())
	}
	s := fb.Routing.Servers[rig.entID]
	return s.Copy, s.MySQL, s.Reasons
}

// inZone is what the three rows read as in a zone: the DATETIME is a wall
// clock and never moves, the TIMESTAMP is an instant printed in the zone.
func inZone(t *testing.T, zone string) [][]string {
	t.Helper()
	loc, err := time.LoadLocation(zone)
	if err != nil {
		t.Fatal(err)
	}
	var out [][]string
	for _, r := range visitRows {
		at, err := time.ParseInLocation("2006-01-02 15:04:05", r[1], time.UTC)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, []string{r[1], at.In(loc).Format("2006-01-02 15:04:05"), "ab|", "1"})
	}
	return out
}

// #2082 end to end against a real MySQL source, through a ROUTED port: the
// copy answers an expensive statement only under a session it reproduces,
// and then answers what MySQL would. Every statement is asserted by WHICH
// SIDE answered and against MySQL's own answer on the same connection.
func TestIntegrationFlashbackRoutedSessionSettings(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	rig := newRoutedSessionRig(t, testutil.BaseDSN())
	ctx := context.Background()
	must := func(t *testing.T, c *sql.Conn, stmt string) {
		t.Helper()
		if err := connExec(t, c, stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	var systemZone, globalZone, globalMode string
	if err := rig.src.QueryRow("SELECT @@global.system_time_zone, @@global.time_zone, @@global.sql_mode").Scan(&systemZone, &globalZone, &globalMode); err != nil {
		t.Fatal(err)
	}
	utcSource := globalZone == "SYSTEM" && systemZone == "UTC"
	if !utcSource {
		t.Logf("the source's default zone is %s (system %s): the cases that need a UTC source are skipped", globalZone, systemZone)
	}

	// The issue's acceptance: SET time_zone = '<zone>', then an expensive
	// SELECT over a DATETIME and a TIMESTAMP is answered by the copy and
	// equals MySQL: on the same connection, and by the same statement run
	// straight on the source under the same zone.
	t.Run("SET time_zone: the copy answers, equal to MySQL", func(t *testing.T) {
		c := rig.conn(t, "")
		must(t, c, "SET time_zone = 'Europe/Madrid'")
		got := rig.scan(t, c, "copy")
		if want := inZone(t, "Europe/Madrid"); !reflect.DeepEqual(got, want) {
			t.Errorf("under Europe/Madrid = %v, want %v", got, want)
		}
		direct := directConn(t, rig.srcDSN)
		must(t, direct, "SET time_zone = 'Europe/Madrid'")
		if onSource := connStrings(t, direct, visitsScan); sidesOf(onSource) != "live" || !reflect.DeepEqual(restOf(onSource), got) {
			t.Errorf("the same statement straight on the source: %v, the copy through the port: %v", restOf(onSource), got)
		}
		// Every change is followed, not only the first.
		must(t, c, "SET time_zone = '-05:00'")
		if got, want := rig.scan(t, c, "copy"), inZone(t, "Etc/GMT+5"); !reflect.DeepEqual(got, want) {
			t.Errorf("under -05:00 = %v, want %v", got, want)
		}
	})
	t.Run("the zone as the driver sets it when it connects", func(t *testing.T) {
		c := rig.conn(t, "&time_zone="+url.QueryEscape("'Asia/Tokyo'"))
		if got, want := rig.scan(t, c, "copy"), inZone(t, "Asia/Tokyo"); !reflect.DeepEqual(got, want) {
			t.Errorf("under Asia/Tokyo = %v, want %v", got, want)
		}
	})
	t.Run("what MySQL made of the value, not the client's text", func(t *testing.T) {
		c := rig.conn(t, "")
		must(t, c, "SET time_zone = 'europe/madrid'")
		if got, want := rig.scan(t, c, "copy"), inZone(t, "Europe/Madrid"); !reflect.DeepEqual(got, want) {
			t.Errorf("under europe/madrid = %v, want %v", got, want)
		}
		must(t, c, "SET @tz = '+09:00'")
		must(t, c, "SET time_zone = @tz, sql_select_limit = DEFAULT")
		if got, want := rig.scan(t, c, "copy"), inZone(t, "Asia/Tokyo"); !reflect.DeepEqual(got, want) {
			t.Errorf("under @tz = %v, want Tokyo's %v", got, want)
		}
	})
	if utcSource {
		t.Run("a connection that sends no SET at all, and SYSTEM", func(t *testing.T) {
			c := rig.conn(t, "")
			if got, want := rig.scan(t, c, "copy"), inZone(t, "UTC"); !reflect.DeepEqual(got, want) {
				t.Errorf("no SET, UTC source = %v, want %v", got, want)
			}
			must(t, c, "SET time_zone = '+03:00'")
			must(t, c, "SET time_zone = DEFAULT")
			if got, want := rig.scan(t, c, "copy"), inZone(t, "UTC"); !reflect.DeepEqual(got, want) {
				t.Errorf("DEFAULT on a UTC source = %v, want %v", got, want)
			}
		})
	}
	t.Run("sql_select_limit cuts the copy's answer as it cuts MySQL's", func(t *testing.T) {
		c := rig.conn(t, "")
		must(t, c, "SET sql_select_limit = 2")
		got := connStrings(t, c, visitsScan)
		if len(got) != 2 || sidesOf(got) != "copy" {
			t.Errorf("under sql_select_limit = 2: %d rows from %q, want 2 from the copy", len(got), sidesOf(got))
		}
		// 0 cuts every SELECT, the port's own read of the session included:
		// MySQL answers (no rows), and the copy comes back with the limit gone.
		must(t, c, "SET sql_select_limit = 0")
		if got := connStrings(t, c, visitsScan); len(got) != 0 {
			t.Errorf("under sql_select_limit = 0: %v, want MySQL's no rows", got)
		}
		must(t, c, "SET sql_select_limit = DEFAULT")
		rig.scan(t, c, "copy")
	})
	t.Run("sql_mode as Rails sets it, beside SET NAMES", func(t *testing.T) {
		c := rig.conn(t, "")
		must(t, c, "SET NAMES utf8mb4, @@SESSION.sql_mode = CONCAT(CONCAT(@@sql_mode, ',STRICT_ALL_TABLES'), ',NO_AUTO_VALUE_ON_ZERO'), @@SESSION.sql_auto_is_null = 0, @@SESSION.wait_timeout = 2147483")
		rig.scan(t, c, "copy")
		must(t, c, "SET sql_mode = ''")
		rig.scan(t, c, "copy")
	})

	// W1: a mode that changes MySQL's answer keeps the statement on MySQL.
	t.Run("W1 a sql_mode the copy does not follow", func(t *testing.T) {
		c := rig.conn(t, "")
		must(t, c, "SET sql_mode = CONCAT(@@sql_mode, ',PAD_CHAR_TO_FULL_LENGTH')")
		got := rig.scan(t, c, "live")
		if got[0][2] != "ab    |" {
			t.Errorf("under PAD_CHAR_TO_FULL_LENGTH MySQL answered %q for the CHAR(6), want it padded", got[0][2])
		}
		// It came with a zone the copy does reproduce: still MySQL's.
		must(t, c, "SET time_zone = 'Europe/Madrid'")
		rig.scan(t, c, "live")
		// Back to the defaults: the copy answers again, in the zone.
		must(t, c, "SET sql_mode = DEFAULT")
		if got, want := rig.scan(t, c, "copy"), inZone(t, "Europe/Madrid"); !reflect.DeepEqual(got, want) {
			t.Errorf("after SET sql_mode = DEFAULT: %v, want %v", got, want)
		}
		for _, mode := range []string{"HIGH_NOT_PRECEDENCE", "REAL_AS_FLOAT", "TIME_TRUNCATE_FRACTIONAL", "ALLOW_INVALID_DATES", "IGNORE_SPACE", "ANSI_QUOTES", "ANSI"} {
			must(t, c, "SET sql_mode = '"+mode+"'")
			rig.scan(t, c, "live")
		}
	})
	// W2: a procedure changes the session behind a statement that is not a SET.
	t.Run("W2 a procedure that sets the zone", func(t *testing.T) {
		if _, err := rig.src.Exec("CREATE PROCEDURE to_utc() SET time_zone = '+00:00'"); err != nil {
			t.Fatal(err)
		}
		if _, err := rig.src.Exec("CREATE PROCEDURE to_kolkata() SET time_zone = '+05:30'"); err != nil {
			t.Fatal(err)
		}
		c := rig.conn(t, "")
		must(t, c, "SET time_zone = '+09:00'")
		if got, want := rig.scan(t, c, "copy"), inZone(t, "Asia/Tokyo"); !reflect.DeepEqual(got, want) {
			t.Fatalf("under +09:00 = %v, want %v", got, want)
		}
		must(t, c, "CALL to_utc()")
		if got, want := rig.scan(t, c, "copy"), inZone(t, "UTC"); !reflect.DeepEqual(got, want) {
			t.Errorf("after CALL to_utc(): %v, want UTC's %v", got, want)
		}
		must(t, c, "CALL to_kolkata()")
		rig.scan(t, c, "live")
	})
	// W3: a SET the statement classifier used not to see.
	t.Run("W3 a SET behind a vertical tab or an empty comment", func(t *testing.T) {
		c := rig.conn(t, "")
		rig.scan(t, c, "copy")
		must(t, c, "\vSET time_zone = '+03:00'")
		if got, want := rig.scan(t, c, "copy"), inZone(t, "Etc/GMT-3"); !reflect.DeepEqual(got, want) {
			t.Errorf("after \\vSET: %v, want +03:00's %v", got, want)
		}
		must(t, c, "/**/SET time_zone = '+04:00' /* x */")
		if got, want := rig.scan(t, c, "copy"), inZone(t, "Etc/GMT-4"); !reflect.DeepEqual(got, want) {
			t.Errorf("after /**/SET: %v, want +04:00's %v", got, want)
		}
		must(t, c, "/*!50000 SET time_zone = '+06:00' */")
		if got, want := rig.scan(t, c, "copy"), inZone(t, "Etc/GMT-6"); !reflect.DeepEqual(got, want) {
			t.Errorf("after /*!50000 SET */: %v, want +06:00's %v", got, want)
		}
	})
	// W4: variables riding behind SET NAMES.
	t.Run("W4 a variable behind SET NAMES", func(t *testing.T) {
		const months = "SELECT side, MONTHNAME(seen_at), DAYNAME(seen_at), count(*) FROM visits GROUP BY side, seen_at ORDER BY seen_at"
		c := rig.conn(t, "")
		if got := connStrings(t, c, months); sidesOf(got) != "copy" || got[0][1] != "January" || got[0][2] != "Thursday" {
			t.Fatalf("before: %v", got)
		}
		must(t, c, "SET NAMES utf8mb4, lc_time_names = 'es_ES'")
		if got := connStrings(t, c, months); sidesOf(got) != "live" || got[0][1] != "enero" || got[0][2] != "jueves" {
			t.Errorf("under lc_time_names = es_ES: %v, want MySQL's enero / jueves", got)
		}
		for _, set := range []string{
			"SET NAMES utf8mb4, div_precision_increment = 9",
			"SET NAMES utf8mb4, sql_auto_is_null = 1",
			"SET NAMES utf8mb4, sql_big_selects = 0",
			"SET NAMES utf8mb4 COLLATE utf8mb4_bin",
			"SET NAMES utf8mb4 COLLATE utf8mb4_general_ci",
			"SET NAMES utf8mb4 COLLATE utf8mb4_0900_as_cs",
			"SET NAMES latin1",
			"SET NAMES utf8mb4, character_set_results = latin1",
		} {
			c := rig.conn(t, "")
			must(t, c, set)
			if got := connStrings(t, c, visitsScan); sidesOf(got) == "copy" {
				t.Errorf("after %s the copy answered", set)
			}
		}
		// And the ones the copy reproduces.
		for _, set := range []string{
			"SET NAMES utf8mb4",
			"SET NAMES utf8mb4 COLLATE utf8mb4_0900_ai_ci",
			// A PAD SPACE collation that differs on trailing spaces only:
			// accepted, with that documented difference.
			"SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci",
			"SET NAMES utf8mb4, character_set_results = NULL",
			"SET NAMES utf8mb4, wait_timeout = 100, group_concat_max_len = 4096",
		} {
			c := rig.conn(t, "")
			must(t, c, set)
			rig.scan(t, c, "copy")
		}
	})
	// W6: named zones. Whoever answers, the answer is MySQL's: a zone the
	// source's tables and this host's data disagree on stays on MySQL.
	t.Run("W6 named zones never answer another hour", func(t *testing.T) {
		for _, zone := range []string{"Africa/Casablanca", "Africa/El_Aaiun", "America/Vancouver", "America/Edmonton", "America/Yellowknife",
			"Canada/Pacific", "Canada/Mountain", "Europe/London", "America/Sao_Paulo", "Asia/Kolkata", "Australia/Lord_Howe"} {
			c := rig.conn(t, "")
			if err := connExec(t, c, "SET time_zone = '"+zone+"'"); err != nil {
				t.Logf("%s: the source does not have it (%v)", zone, err)
				continue
			}
			got := connStrings(t, c, visitsScan)
			if mysqlRows := visitsByLookup(t, c); !reflect.DeepEqual(restOf(got), restOf(mysqlRows)) {
				t.Errorf("%s: answered by %s: %v, MySQL: %v", zone, sidesOf(got), restOf(got), restOf(mysqlRows))
			}
			t.Logf("%s: answered by %s", zone, sidesOf(got))
		}
	})
	t.Run("week numbers and recursion stay on MySQL", func(t *testing.T) {
		c := rig.conn(t, "")
		for _, stmt := range []string{
			"SELECT side, EXTRACT(WEEK FROM seen_at), count(*) FROM visits GROUP BY side, seen_at ORDER BY seen_at",
			"SELECT side, WEEK(seen_at), count(*) FROM visits GROUP BY side, seen_at ORDER BY seen_at",
			"WITH RECURSIVE n AS (SELECT 1 AS i UNION ALL SELECT i + 1 FROM n WHERE i < 3) SELECT side, count(*) FROM visits, n GROUP BY side",
		} {
			if got := connStrings(t, c, stmt); sidesOf(got) != "live" {
				t.Errorf("%s: answered by %q, want MySQL", stmt, sidesOf(got))
			}
		}
	})
	t.Run("a value the copy does not reproduce never fails the client", func(t *testing.T) {
		c := rig.conn(t, "")
		must(t, c, "SET time_zone = '+05:30'")
		if got := rig.scan(t, c, "live"); got[0][1] != "2026-01-15 15:30:00" {
			t.Errorf("MySQL's answer under +05:30 = %v", got)
		}
		must(t, c, "SET time_zone = 'Asia/Kolkata'")
		if got := rig.scan(t, c, "copy"); got[0][1] != "2026-01-15 15:30:00" {
			t.Errorf("the copy's answer under Asia/Kolkata = %v", got)
		}
	})
	t.Run("a statement prepared before the SET runs under it", func(t *testing.T) {
		c := rig.conn(t, "")
		st, err := c.PrepareContext(ctx, "SELECT side, stamped FROM visits WHERE side <> ? GROUP BY side, stamped ORDER BY stamped")
		if err != nil {
			t.Fatal(err)
		}
		defer st.Close()
		read := func() (string, string) {
			t.Helper()
			var side, stamped sql.RawBytes
			rows, err := st.QueryContext(ctx, "zz")
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()
			if !rows.Next() {
				t.Fatal("no rows")
			}
			if err := rows.Scan(&side, &stamped); err != nil {
				t.Fatal(err)
			}
			// The binary protocol carries the copy's microseconds.
			return string(side), strings.TrimSuffix(string(stamped), ".000000")
		}
		if utcSource {
			if side, stamped := read(); side != "copy" || stamped != "2026-01-15 10:00:00" {
				t.Fatalf("before the SET: %s %s", side, stamped)
			}
		}
		// The SET itself as a prepared statement, the way a driver without
		// client-side interpolation sends everything.
		if _, err := c.ExecContext(ctx, "SET time_zone = ?", "+03:00"); err != nil {
			t.Fatal(err)
		}
		if side, stamped := read(); side != "copy" || stamped != "2026-01-15 13:00:00" {
			t.Errorf("after SET time_zone = '+03:00': %s %s, want the copy and 13:00", side, stamped)
		}
	})

	// B2: nothing follows the client's statement on the source, so what the
	// client reads next about its own statement is what MySQL itself says.
	t.Run("B2 the client's next statement reads its own diagnostics", func(t *testing.T) {
		sequences := [][]string{
			{"SELECT SQL_CALC_FOUND_ROWS id FROM visits ORDER BY id LIMIT 1", "SET time_zone = '+01:00'", "SELECT FOUND_ROWS()"},
			{"SET sql_mode = 'STRICT_ALL_TABLES,NO_ZERO_DATE'", "SELECT @@warning_count"},
			{"SET sql_mode = 'STRICT_ALL_TABLES,NO_ZERO_DATE'", "SHOW COUNT(*) WARNINGS"},
			{"SET sql_mode = 'STRICT_ALL_TABLES,NO_ZERO_DATE'", "SHOW WARNINGS LIMIT 1"},
			{"SET sql_mode = 'STRICT_ALL_TABLES,NO_ZERO_DATE'", "SHOW WARNINGS /* x */"},
			{"SET sql_mode = 'STRICT_ALL_TABLES,NO_ZERO_DATE'", "SHOW WARNINGS"},
			{"UPDATE visits SET code = 'ab' WHERE id = 0", "SET time_zone = '+02:00'", "SELECT ROW_COUNT()"},
		}
		for _, seq := range sequences {
			var answers [2][][]string
			for i, c := range []*sql.Conn{rig.conn(t, ""), directConn(t, rig.srcDSN)} {
				for _, stmt := range seq[:len(seq)-1] {
					if strings.HasPrefix(stmt, "SELECT") {
						connStrings(t, c, stmt)
					} else {
						must(t, c, stmt)
					}
				}
				answers[i] = connStrings(t, c, seq[len(seq)-1])
			}
			if !reflect.DeepEqual(answers[0], answers[1]) {
				t.Errorf("%q: through the port %v, straight on MySQL %v", seq, answers[0], answers[1])
			}
		}
	})

	// B1: time travel reads and prints UTC, so it runs only when the
	// source's session is in UTC, whatever the connection did before.
	t.Run("B1 time travel needs a UTC session, whatever came before", func(t *testing.T) {
		tt := fmt.Sprintf("SELECT * FROM _flashback.users AS OF '%s' WHERE id = 1", rig.now.Add(10*time.Minute).Format("2006-01-02 15:04:05"))
		travel := func(c *sql.Conn) (string, error) {
			rows, err := c.QueryContext(ctx, tt)
			if err != nil {
				return "", err
			}
			defer rows.Close()
			cols, _ := rows.Columns()
			vals := make([]sql.RawBytes, len(cols))
			ptrs := make([]any, len(cols))
			for i := range vals {
				ptrs[i] = &vals[i]
			}
			if !rows.Next() {
				return "", fmt.Errorf("no row: %v", rows.Err())
			}
			if err := rows.Scan(ptrs...); err != nil {
				return "", err
			}
			for i, col := range cols {
				if col == "name" {
					return string(vals[i]), nil
				}
			}
			return "", fmt.Errorf("no name column in %v", cols)
		}
		for _, history := range [][]string{
			{"SET time_zone = '+05:30'"},
			{"SET time_zone = 'Europe/Madrid'"},
			{"SET time_zone = '+09:00', group_concat_max_len = 4096"},
			{"SET time_zone = '+09:00'", "CREATE TEMPORARY TABLE tt_pin (id INT)"},
			{"SET time_zone = '+09:00'", visitsScan},
		} {
			c := rig.conn(t, "")
			for _, stmt := range history {
				if strings.HasPrefix(stmt, "SELECT") {
					connStrings(t, c, stmt)
				} else {
					must(t, c, stmt)
				}
			}
			zone := strings.Split(strings.Split(history[0], "'")[1], "'")[0]
			if _, err := travel(c); mysqlCode(err) != 1235 || !strings.Contains(err.Error(), zone) {
				t.Errorf("after %q: %v, want 1235 naming %s", history, err, zone)
			}
			must(t, c, "SET time_zone = '+00:00'")
			if name, err := travel(c); err != nil || name != "traveller" {
				t.Errorf("after %q and back to +00:00: %q, %v; want the row", history, name, err)
			}
		}
		if utcSource {
			if name, err := travel(rig.conn(t, "")); err != nil || name != "traveller" {
				t.Errorf("a connection that sent nothing, UTC source: %q, %v", name, err)
			}
		}
	})

	t.Run("what still keeps a connection on MySQL for good", func(t *testing.T) {
		c := rig.conn(t, "")
		must(t, c, "CREATE TEMPORARY TABLE pin_me (id INT)")
		rig.scan(t, c, "live")
		must(t, c, "SET time_zone = '+00:00'")
		rig.scan(t, c, "live")
	})

	copyN, mysqlN, reasons := rig.tally(t)
	var sum uint64
	for reason, n := range reasons {
		if reason != "expensive_plan" {
			sum += n
		}
	}
	if mysqlN != sum || copyN != reasons["expensive_plan"] {
		t.Errorf("tally = copy %d, mysql %d; reasons %v: the port's own read of the session was counted as a statement", copyN, mysqlN, reasons)
	}
	if reasons["session_differs"] == 0 || reasons["connection_pinned"] == 0 || reasons["expensive_plan"] == 0 {
		t.Errorf("reasons = %v, want session_differs, connection_pinned and expensive_plan all counted", reasons)
	}
	if n, ok := reasons["settings_set"]; ok {
		t.Errorf("the retired reason settings_set was counted %d time(s)", n)
	}
}

// The source's own defaults, on connections that never send a SET (W5): a
// global zone or mode set on the SERVER. It changes the server for every
// connection, so it runs only where the server is this test's own
// (BINTRAIL_TEST_ALLOW_SET_GLOBAL=1), never on a shared one.
func TestIntegrationFlashbackRoutedSessionSourceDefaults(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	if os.Getenv("BINTRAIL_TEST_ALLOW_SET_GLOBAL") != "1" {
		t.Skip("changes the source's GLOBAL time_zone and sql_mode: set BINTRAIL_TEST_ALLOW_SET_GLOBAL=1 on a server of your own")
	}
	rig := newRoutedSessionRig(t, testutil.BaseDSN())
	var zone, mode string
	if err := rig.src.QueryRow("SELECT @@global.time_zone, @@global.sql_mode").Scan(&zone, &mode); err != nil {
		t.Fatal(err)
	}
	setGlobal := func(name, value string) {
		t.Helper()
		if _, err := rig.src.Exec("SET GLOBAL "+name+" = ?", value); err != nil {
			t.Fatalf("SET GLOBAL %s = %s: %v", name, value, err)
		}
	}
	t.Cleanup(func() {
		_, _ = rig.src.Exec("SET GLOBAL time_zone = ?", zone)
		_, _ = rig.src.Exec("SET GLOBAL sql_mode = ?", mode)
	})

	setGlobal("time_zone", "Europe/Madrid")
	if got, want := rig.scan(t, rig.conn(t, ""), "copy"), inZone(t, "Europe/Madrid"); !reflect.DeepEqual(got, want) {
		t.Errorf("global time_zone Europe/Madrid, no SET: %v, want %v", got, want)
	}
	setGlobal("time_zone", "+05:30")
	c := rig.conn(t, "")
	if got := rig.scan(t, c, "live"); got[0][1] != "2026-01-15 15:30:00" {
		t.Errorf("global time_zone +05:30, no SET: %v", got)
	}
	// The same connection reaches the copy once it names a zone itself.
	if err := connExec(t, c, "SET time_zone = 'Asia/Kolkata'"); err != nil {
		t.Fatal(err)
	}
	rig.scan(t, c, "copy")
	// Time travel on a connection that sent nothing: the source's zone.
	tt := fmt.Sprintf("SELECT * FROM _flashback.users AS OF '%s' WHERE id = 1", rig.now.Add(10*time.Minute).Format("2006-01-02 15:04:05"))
	if _, err := rig.conn(t, "").QueryContext(context.Background(), tt); mysqlCode(err) != 1235 || !strings.Contains(err.Error(), "+05:30") {
		t.Errorf("time travel under a global +05:30: %v, want 1235 naming the zone", err)
	}
	setGlobal("time_zone", zone)

	setGlobal("sql_mode", mode+",PAD_CHAR_TO_FULL_LENGTH")
	c = rig.conn(t, "")
	if got := rig.scan(t, c, "live"); got[0][2] != "ab    |" {
		t.Errorf("global PAD_CHAR_TO_FULL_LENGTH, no SET: %v", got)
	}
	// A SET of something else does not make the copy answer under it.
	if err := connExec(t, c, "SET time_zone = '+00:00'"); err != nil {
		t.Fatal(err)
	}
	rig.scan(t, c, "live")
	setGlobal("sql_mode", mode)
	rig.scan(t, rig.conn(t, ""), "copy")
}

// The same on a MariaDB source: its own defaults (modes, connection
// collation) are a session the copy reproduces, a zone is followed, and the
// two forms only MariaDB runs (its executable comment, EXECUTE IMMEDIATE)
// change the session without hiding it from the port.
func TestIntegrationFlashbackRoutedSessionMariaDB(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	testutil.SkipIfNoMariaDB(t)
	rig := newRoutedSessionRig(t, testutil.MariaDBBaseDSN())
	must := func(c *sql.Conn, stmt string) {
		t.Helper()
		if err := connExec(t, c, stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	var systemZone, globalZone string
	if err := rig.src.QueryRow("SELECT @@global.system_time_zone, @@global.time_zone").Scan(&systemZone, &globalZone); err != nil {
		t.Fatal(err)
	}
	// A connection that sent nothing. The port's login asks for MySQL's
	// default collation, which MariaDB 11.4 knows and 10.11 does not: 10.11
	// would leave the session in utf8mb4_general_ci, which does not compare
	// like the copy, so the port names utf8mb4_unicode_ci on its new
	// connection. Either way the copy answers a default connection.
	if globalZone == "SYSTEM" && systemZone == "UTC" {
		d := rig.conn(t, "")
		// What the port's session has (a variable: MySQL answers).
		given := connStrings(t, d, "SELECT @@collation_connection")[0][0]
		t.Logf("a connection that names no collation is in %s", given)
		if strings.Contains(given, "general") {
			t.Errorf("the port's session is in %s, which the copy does not reproduce", given)
		}
		rig.scan(t, d, "copy")
		// A client that asks for another collation gets it, and MySQL's
		// answers with it.
		must(d, "SET NAMES utf8mb4 COLLATE utf8mb4_general_ci")
		if got := connStrings(t, d, "SELECT @@collation_connection")[0][0]; got != "utf8mb4_general_ci" {
			t.Errorf("after the client's SET NAMES the session is in %s", got)
		}
		rig.scan(t, d, "live")
	}
	c := rig.conn(t, "")
	must(c, "SET time_zone = 'Europe/Madrid'")
	if got, want := rig.scan(t, c, "copy"), inZone(t, "Europe/Madrid"); !reflect.DeepEqual(got, want) {
		t.Errorf("under Europe/Madrid = %v, want %v", got, want)
	}
	// MariaDB's executable comment: MariaDB runs it.
	must(c, "/*M! SET time_zone = '+03:00' */")
	if got, want := rig.scan(t, c, "copy"), inZone(t, "Etc/GMT-3"); !reflect.DeepEqual(got, want) {
		t.Errorf("after /*M! SET */: %v, want +03:00's %v", got, want)
	}
	must(c, "/*M!100100 SET time_zone = '+05:30' */")
	rig.scan(t, c, "live")
	must(c, "EXECUTE IMMEDIATE 'SET time_zone = ''-05:00'''")
	if got, want := rig.scan(t, c, "copy"), inZone(t, "Etc/GMT+5"); !reflect.DeepEqual(got, want) {
		t.Errorf("after EXECUTE IMMEDIATE: %v, want -05:00's %v", got, want)
	}
	must(c, "BEGIN NOT ATOMIC SET time_zone = '+09:00'; END")
	if got, want := rig.scan(t, c, "copy"), inZone(t, "Asia/Tokyo"); !reflect.DeepEqual(got, want) {
		t.Errorf("after BEGIN NOT ATOMIC ... END: %v, want +09:00's %v", got, want)
	}
	// MariaDB's own modes that change an answer.
	for _, mode := range []string{"EMPTY_STRING_IS_NULL", "TIME_ROUND_FRACTIONAL", "PAD_CHAR_TO_FULL_LENGTH", "ORACLE"} {
		must(c, "SET sql_mode = '"+mode+"'")
		rig.scan(t, c, "live")
	}
	must(c, "SET sql_mode = DEFAULT")
	rig.scan(t, c, "copy")
	// Time travel: the session's zone, as on MySQL.
	tt := fmt.Sprintf("SELECT * FROM _flashback.users AS OF '%s' WHERE id = 1", rig.now.Add(10*time.Minute).Format("2006-01-02 15:04:05"))
	if _, err := c.QueryContext(context.Background(), tt); mysqlCode(err) != 1235 || !strings.Contains(err.Error(), "+09:00") {
		t.Errorf("time travel under +09:00: %v, want 1235 naming the zone", err)
	}
	must(c, "SET time_zone = '+00:00'")
	rows, err := c.QueryContext(context.Background(), tt)
	if err != nil {
		t.Fatalf("time travel back in UTC: %v", err)
	}
	rows.Close()
}

// Time travel on a routed port when the source cannot be asked for the
// session's zone. A session that existed and is lost answers 2006, like
// every command, whether or not the port had read the session; a source that
// never let the connection in leaves time travel running under UTC.
func TestIntegrationFlashbackRoutedTimeTravelSourceGone(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	rig := newRoutedSessionRig(t, testutil.BaseDSN())
	ctx := context.Background()
	tt := fmt.Sprintf("SELECT * FROM _flashback.users AS OF '%s' WHERE id = 1", rig.now.Add(10*time.Minute).Format("2006-01-02 15:04:05"))
	travel := func(c *sql.Conn) error {
		rows, err := c.QueryContext(ctx, tt)
		if err != nil {
			return err
		}
		defer rows.Close()
		if !rows.Next() {
			return fmt.Errorf("no row: %v", rows.Err())
		}
		return nil
	}
	// killUpstream ends the port's session on the source for this client
	// connection, from the source's side.
	killUpstream := func(c *sql.Conn) {
		t.Helper()
		id := connStrings(t, c, "SELECT CONNECTION_ID()") // vetoed: MySQL answers with the upstream session's id
		if _, err := rig.src.Exec("KILL " + id[0][0]); err != nil {
			t.Fatal(err)
		}
		time.Sleep(300 * time.Millisecond)
	}
	for _, known := range []bool{false, true} {
		c := rig.conn(t, "")
		if known {
			// A copy-bound statement: the port reads and keeps the session.
			if got := connStrings(t, c, visitsScan); sidesOf(got) != "copy" {
				t.Fatalf("the scan was answered by %q", sidesOf(got))
			}
		}
		killUpstream(c)
		if err := travel(c); mysqlCode(err) != 2006 {
			t.Errorf("session lost (session read before: %v): time travel = %v, want error 2006", known, err)
		}
	}
	// The source never let the connection in.
	c := routedConn(t, rig.addr, rig.deadID, rig.srcName, "")
	for range 2 {
		if err := travel(c); err != nil {
			t.Errorf("source unreachable from the start: time travel = %v, want it to run", err)
		}
	}
	// And an ordinary statement on that connection is the 2006 it always was.
	if _, err := c.QueryContext(ctx, "SELECT 1"); mysqlCode(err) != 2006 {
		t.Errorf("SELECT 1 with the source unreachable = %v, want 2006", err)
	}
}
