//go:build integration

package consoleapp

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/readrouter"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// #2611 against a real MySQL: under routing, DATE_SUB and SUBDATE with an
// INTERVAL over a DATETIME or TIMESTAMP column, NOW() or UTC_TIMESTAMP(), or
// over a DATE by less than a day, are answered by the copy with the source's
// answer; a DATE stepped by a day or more, a fraction, text, a literal, a
// placeholder, a time before the year 1, and arithmetic on the result stay
// on the source.
func TestIntegrationSQLCompareDateSubRouted(t *testing.T) {
	srcDB, srcName := testutil.CreateTestDB(t)
	dateSubRouted(t, srcDB, srcName, testutil.IntegrationDSN(srcName))
}

// The same against a MariaDB source. A test of its own, with MariaDB in its
// name: the MariaDB job picks its tests by name.
func TestIntegrationSQLCompareDateSubRoutedMariaDB(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	srcDB, srcName := testutil.CreateTestMariaDB(t)
	dateSubRouted(t, srcDB, srcName, testutil.MariaDBBaseDSN()+"/"+srcName+"?parseTime=true")
}

func dateSubRouted(t *testing.T, srcDB *sql.DB, srcName, sourceDSN string) {
	now := time.Now().UTC().Truncate(time.Second)
	indexDSN := seedFlashbackIndex(t, "alice", now.Truncate(time.Hour))
	var version string
	if err := srcDB.QueryRow("SELECT VERSION()").Scan(&version); err != nil {
		t.Fatal(err)
	}
	t.Logf("source: %s", version)

	// Month ends, a leap day, the turn of a year, a TIMESTAMP half an hour
	// before midnight UTC (another day five hours east), rows of the last
	// days for "since", the first day of the year 1 (k = 4: a day before it
	// is the year 0), the last day of 9999, a fraction, and NULLs.
	at := func(d time.Duration) string { return now.Add(-d).Format("2006-01-02 15:04:05") }
	day := func(d time.Duration) string { return now.Add(-d).Format("2006-01-02") }
	rows := func(side string) [][]string {
		return [][]string{
			{"1", "1", side, "2026-03-31", "2026-03-31 10:00:00", "2026-03-31 23:30:00", "2026-03-31 10:00:00.250000", "2026-03-31"},
			{"2", "2", side, "2024-02-29", "2024-02-29 00:00:00", "2024-02-29 12:00:00", "2024-02-29 00:00:00.000000", "20240229"},
			{"3", "3", side, day(24 * time.Hour), at(time.Hour), at(2 * time.Hour), "2026-01-01 00:00:00.000000", "2026-01-15 02:00:00+05:00"},
			{"4", "4", side, "0001-01-01", "0001-01-01 00:00:00", `\N`, `\N`, "x"},
			{"5", "5", side, "9999-12-31", "9999-12-31 23:59:59", "2026-01-01 00:30:00", `\N`, `\N`},
			{"6", "6", side, `\N`, `\N`, `\N`, `\N`, `\N`},
			{"7", "7", side, "2026-05-31", "2026-05-31 23:59:59", "2026-05-31 19:00:00", `\N`, "y"},
		}
	}
	src := openRaw(t, withZone(sourceDSN, "+00:00"))
	const create = "CREATE TABLE ev (id INT NOT NULL PRIMARY KEY, k INT, side VARCHAR(8), d DATE, dt DATETIME, ts TIMESTAMP NULL, dt6 DATETIME(6), s VARCHAR(32))"
	if _, err := src.Exec(create); err != nil {
		t.Fatal(err)
	}
	for _, r := range rows("live") {
		vals := make([]string, len(r))
		for i, v := range r {
			vals[i] = "'" + v + "'"
			if v == `\N` {
				vals[i] = "NULL"
			}
		}
		if _, err := src.Exec("INSERT INTO ev VALUES (" + strings.Join(vals, ",") + ")"); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := src.Exec("ANALYZE TABLE ev"); err != nil {
		t.Fatal(err)
	}
	var name, ddl string
	if err := src.QueryRow("SHOW CREATE TABLE ev").Scan(&name, &ddl); err != nil {
		t.Fatal(err)
	}
	baseDir := t.TempDir()
	writeOrderSnapshot(t, baseDir, srcName, []orderTable{{name: "ev", ddl: ddl + ";\n", rows: rows("live"), copyRows: rows("copy"), footer: true}})

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
	portDSN := fmt.Sprintf("%s:tok@tcp(%s)/%s?parseTime=true&timeout=5s", ent.ID, ln.Addr().String(), srcName)

	// ORDER BY k and WHERE k: a column with no index, so the plan reads the
	// whole table and the copy is asked. k <> 4 leaves out the year 1, which
	// the copy declines.
	cases := []struct {
		stmt string
		args []any
		who  string
		why  string
	}{
		{stmt: "SELECT side, DATE_SUB(dt, INTERVAL 1 DAY), DATE_SUB(dt, INTERVAL 1 MONTH), DATE_SUB(dt, INTERVAL 1 YEAR) FROM ev WHERE k <> 4 ORDER BY k", who: "copy", why: "a DATETIME by days, months and years"},
		{stmt: "SELECT side, DATE_SUB(ts, INTERVAL 1 MONTH), SUBDATE(ts, INTERVAL 2 HOUR), DATE_SUB(ts, INTERVAL 1 QUARTER) FROM ev WHERE k <> 4 ORDER BY k", who: "copy", why: "a TIMESTAMP, in the session's zone"},
		{stmt: "SELECT side, DATE_SUB(d, INTERVAL 1 HOUR), DATE_SUB(dt, INTERVAL 90 MINUTE), date_sub(dt, interval 3601 second), DATE_SUB(dt, INTERVAL '2' WEEK) FROM ev WHERE k <> 4 ORDER BY k", who: "copy", why: "a DATE by an hour, minutes, seconds, weeks"},
		{stmt: "SELECT MIN(side), COUNT(*) FROM ev WHERE dt >= DATE_SUB(NOW(), INTERVAL 3 DAY)", who: "copy", why: "the last days"},
		{stmt: "SELECT MIN(side), COUNT(*) FROM ev WHERE ts >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 1 DAY)", who: "copy", why: "UTC_TIMESTAMP() against a TIMESTAMP"},
		{stmt: "SELECT MIN(side), COUNT(*) FROM ev WHERE dt BETWEEN DATE_SUB(CURRENT_TIMESTAMP, INTERVAL 2 DAY) AND NOW()", who: "copy", why: "the keyword, in BETWEEN"},
		{stmt: "SELECT MIN(side), COUNT(*) FROM ev WHERE d >= DATE_SUB(NOW(), INTERVAL 7 DAY)", who: "copy", why: "a DATE compared with it"},
		{stmt: "SELECT MIN(side), DATE_SUB(dt, INTERVAL 1 MONTH) AS since, COUNT(*) FROM ev WHERE k <> 4 GROUP BY since ORDER BY since", who: "copy", why: "grouped and sorted, a NULL first"},
		{stmt: "SELECT side, DATE_SUB(dt, INTERVAL 1 MONTH) FROM ev WHERE k > ? AND k <> 4 ORDER BY k", args: []any{0}, who: "copy", why: "prepared"},
		{stmt: "SELECT side, DATE_SUB(dt, INTERVAL '-1' MONTH), DATE_SUB(ts, INTERVAL '-1' DAY) FROM ev WHERE k <> 5 ORDER BY k", who: "copy", why: "a quoted amount with a sign: a step forward"},

		{stmt: "SELECT side, DATE_SUB(dt, INTERVAL '-1' DAY) FROM ev WHERE k = 5", who: "live", why: "past 9999: NULL on MySQL"},

		{stmt: "SELECT side, DATE_SUB(dt, INTERVAL 1 DAY) FROM ev ORDER BY k", who: "live", why: "the year 1 minus a day: a zero date on MySQL, the year 0 on the copy"},
		{stmt: "SELECT side, DATE_SUB(dt, INTERVAL 1 MONTH) FROM ev WHERE k = 4", who: "live", why: "the year 1 minus a month"},
		{stmt: "SELECT side, DATE_SUB(d, INTERVAL 1 MONTH) FROM ev WHERE k <> 4 ORDER BY k", who: "live", why: "a DATE by a month: a DATE on MySQL"},
		{stmt: "SELECT side, DATE_SUB(dt6, INTERVAL 1 DAY) FROM ev WHERE k <> 4 ORDER BY k", who: "live", why: "a fraction"},
		{stmt: "SELECT side, DATE_SUB(dt, INTERVAL 1 MICROSECOND) FROM ev WHERE k <> 4 ORDER BY k", who: "live", why: "MICROSECOND"},
		{stmt: "SELECT side, DATE_SUB(s, INTERVAL 1 DAY) FROM ev WHERE k <> 4 ORDER BY k", who: "live", why: "a text column"},
		{stmt: "SELECT side, DATE_SUB('2026-03-31', INTERVAL 1 DAY) FROM ev ORDER BY k", who: "live", why: "a literal"},
		{stmt: "SELECT side, DATE_SUB(dt, INTERVAL 1.5 DAY) FROM ev WHERE k <> 4 ORDER BY k", who: "live", why: "a decimal amount"},
		{stmt: "SELECT side, DATE_SUB(dt, INTERVAL ? DAY) FROM ev WHERE k <> 4 ORDER BY k", args: []any{1}, who: "live", why: "an amount bound to a placeholder"},
		{stmt: "SELECT side, DATE_SUB(?, INTERVAL 1 DAY) FROM ev ORDER BY k", args: []any{"2026-03-31"}, who: "live", why: "a date bound to a placeholder"},
		{stmt: "SELECT side, DATE_SUB(CURDATE(), INTERVAL 1 DAY) FROM ev ORDER BY k", who: "live", why: "CURDATE() stepped"},
		{stmt: "SELECT side, DATE_SUB(dt, INTERVAL 1 DAY_HOUR) FROM ev WHERE k <> 4 ORDER BY k", who: "live", why: "a unit of two parts"},
		{stmt: "SELECT side, s AS dt, DATE_SUB(dt, INTERVAL 1 DAY) FROM ev WHERE k <> 4 ORDER BY k", who: "live", why: "a name the statement gives itself"},
		{stmt: "SELECT side, DATE_SUB(dt, INTERVAL 1 DAY) + 0 FROM ev WHERE k <> 4 ORDER BY k", who: "live", why: "the digits of the result"},
		{stmt: "SELECT side, DATE_SUB(dt, INTERVAL 1 DAY) * 2 FROM ev WHERE k <> 4 ORDER BY k", who: "live", why: "a product of the result"},
		{stmt: "SELECT MIN(side), SUM(DATE_SUB(dt, INTERVAL 1 DAY)) FROM ev WHERE k <> 4", who: "live", why: "SUM of the result"},
		{stmt: "SELECT MIN(side), AVG(DATE_SUB(dt, INTERVAL 1 DAY)) FROM ev WHERE k <> 4", who: "live", why: "AVG of the result"},
		{stmt: "SELECT side, DATE_SUB(NOW(), INTERVAL 1 DAY) - DATE_SUB(NOW(), INTERVAL 2 DAY) FROM ev ORDER BY k", who: "live", why: "one result minus another"},
	}
	for _, zone := range []string{"+00:00", "+05:00"} {
		routed := openRaw(t, withZone(portDSN, zone))
		live := openRaw(t, withZone(sourceDSN, zone))
		for _, c := range cases {
			got := answerText(routed, c.stmt, c.args...)
			t.Logf("[%s] routed %-60.300s %s", zone, got, c.stmt)
			if strings.HasPrefix(got, "ERROR ") || !strings.Contains(got, c.who+"|") {
				t.Errorf("[%s] routed %q (%s):\n  got  %s\n  want it answered by %s", zone, c.stmt, c.why, got, c.who)
				continue
			}
			want := answerText(live, c.stmt, c.args...)
			if strings.ReplaceAll(got, "copy|", "live|") != want {
				t.Errorf("[%s] routed %q (%s):\n  got  %s\n  want %s, the source's own answer", zone, c.stmt, c.why, got, want)
			}
		}
		// Not this change's: DATE_ADD past 9999, logged only.
		probe := "SELECT side, DATE_ADD(dt, INTERVAL 1 DAY), dt + INTERVAL 1 DAY FROM ev WHERE k = 5"
		t.Logf("[%s] PROBE routed %s / live %s", zone, answerText(routed, probe), answerText(live, probe))
	}
}
