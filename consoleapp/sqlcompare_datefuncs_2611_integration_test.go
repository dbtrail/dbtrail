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

// #2611 against a real MySQL: under routing, DATE_FORMAT and DATEDIFF over a
// date column or a clock call are answered by the copy with the source's
// answer, the issue's own report among them; over text, a literal, a
// placeholder or a name the statement gives itself they stay on the source.
func TestIntegrationSQLCompareDateFunctionsRouted(t *testing.T) {
	srcDB, srcName := testutil.CreateTestDB(t)
	dateFunctionsRouted(t, srcDB, srcName, testutil.IntegrationDSN(srcName))
}

// The same against a MariaDB source. A test of its own, with MariaDB in its
// name: the MariaDB job picks its tests by name.
func TestIntegrationSQLCompareDateFunctionsRoutedMariaDB(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	srcDB, srcName := testutil.CreateTestMariaDB(t)
	dateFunctionsRouted(t, srcDB, srcName, testutil.MariaDBBaseDSN()+"/"+srcName+"?parseTime=true")
}

func dateFunctionsRouted(t *testing.T, srcDB *sql.DB, srcName, sourceDSN string) {
	now := time.Now().UTC().Truncate(time.Second)
	indexDSN := seedFlashbackIndex(t, "alice", now.Truncate(time.Hour))
	var version string
	if err := srcDB.QueryRow("SELECT VERSION()").Scan(&version); err != nil {
		t.Fatal(err)
	}
	t.Logf("source: %s", version)

	// The end of a month, a leap day, the turn of a year, a TIMESTAMP half an
	// hour before midnight UTC (another day five hours east), rows of the last
	// days for the issue's report, a year before 1000, and NULLs.
	at := func(d time.Duration) string { return now.Add(-d).Format("2006-01-02 15:04:05") }
	day := func(d time.Duration) string { return now.Add(-d).Format("2006-01-02") }
	rows := func(side string) [][]string {
		return [][]string{
			{"1", "1", side, "2026-01-31", "2026-01-01", "2026-01-31 23:30:00", "2026-01-31 23:30:00", "2026-01-15T10:00"},
			{"2", "2", side, "2024-02-29", "2025-03-01", "2024-02-29 00:00:00", "2024-02-29 12:00:00", "20260115"},
			{"3", "3", side, day(24 * time.Hour), day(72 * time.Hour), at(time.Hour), at(2 * time.Hour), "2026-01-15 02:00:00+05:00"},
			{"4", "4", side, "0999-12-31", "2026-12-31", "2026-12-31 00:00:00", "2026-01-04 00:00:00", "2026-1-5"},
			{"5", "5", side, day(48 * time.Hour), day(0), at(26 * time.Hour), at(30 * time.Hour), "x"},
			{"6", "6", side, `\N`, `\N`, `\N`, `\N`, `\N`},
		}
	}
	src := openRaw(t, withZone(sourceDSN, "+00:00"))
	const create = "CREATE TABLE ev (id INT NOT NULL PRIMARY KEY, k INT, side VARCHAR(8), d DATE, d2 DATE, dt DATETIME, ts TIMESTAMP NULL, s VARCHAR(32))"
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

	// ORDER BY k: a column with no index, so the plan reads the whole table
	// and the copy is asked (ORDER BY id walks the key, which MySQL keeps).
	cases := []struct {
		stmt string
		args []any
		who  string
		why  string
	}{
		{stmt: "SELECT MIN(side), DATE_FORMAT(dt, '%Y-%m') AS month, COUNT(*) FROM ev WHERE dt >= NOW() - INTERVAL 30 DAY GROUP BY month ORDER BY month", who: "copy", why: "the issue's report"},
		{stmt: "SELECT MIN(side), DATE_FORMAT(d, '%Y-%m') AS month, COUNT(*) FROM ev GROUP BY month ORDER BY month", who: "copy", why: "a NULL month sorts first on both"},
		{stmt: "SELECT MIN(side), DATE_FORMAT(dt, '%Y-%m-%d %H'), COUNT(*) FROM ev GROUP BY DATE_FORMAT(dt, '%Y-%m-%d %H') ORDER BY 2 DESC", who: "copy", why: "grouped by the call, sorted by its position"},
		{stmt: "SELECT side, DATE_FORMAT(d, '%Y-%m-%d'), DATE_FORMAT(dt, '%Y-%m-%d %H:%i:%s'), DATE_FORMAT(ts, '%H:%i') FROM ev ORDER BY k", who: "copy", why: "a DATE, a DATETIME and a TIMESTAMP"},
		{stmt: "SELECT side, DATE_FORMAT(d, '%M %b %W %a %p'), DATE_FORMAT(dt, '%U %v %x %j %e %c %y %T %r') FROM ev ORDER BY k", who: "copy", why: "names and week numbers"},
		{stmt: "SELECT side, DATE_FORMAT(d, '%Y') FROM ev ORDER BY k", who: "copy", why: "a year before 1000"},
		{stmt: "SELECT MIN(side), DATE_FORMAT(NOW(), '%Y-%m-%d %H:%i'), DATE_FORMAT(CURDATE(), '%Y-%m-%d'), DATE_FORMAT(CURRENT_DATE, '%d') FROM ev", who: "copy", why: "the clock calls"},
		{stmt: "SELECT MIN(side), COUNT(*) FROM ev WHERE DATE_FORMAT(d, '%Y-%m') = DATE_FORMAT(CURDATE(), '%Y-%m')", who: "copy", why: "this month"},
		{stmt: "SELECT side, DATEDIFF(d, d2), DATEDIFF(dt, ts), DATEDIFF(ts, d) FROM ev ORDER BY k", who: "copy", why: "dates, a TIMESTAMP in the session's zone"},
		{stmt: "SELECT side, DATEDIFF(CURDATE(), d), DATEDIFF(d2, CURRENT_DATE), DATEDIFF(NOW(), ts) FROM ev ORDER BY k", who: "copy", why: "against today"},
		{stmt: "SELECT MIN(side), COUNT(*) FROM ev WHERE DATEDIFF(CURDATE(), dt) <= 3", who: "copy", why: "the last days by DATEDIFF"},
		{stmt: "SELECT side, DATE_FORMAT(d, '%Y-%m') FROM ev WHERE k > ? ORDER BY k", args: []any{0}, who: "copy", why: "prepared"},
		{stmt: "SELECT side, DATEDIFF(d, d2) FROM ev WHERE k > ? ORDER BY k", args: []any{0}, who: "copy", why: "prepared"},

		{stmt: "SELECT side, DATE_FORMAT(s, '%Y-%m-%d %H') FROM ev ORDER BY k", who: "live", why: "a text column"},
		{stmt: "SELECT MIN(side), DATE_FORMAT('2026-01-15 02:00:00+05:00', '%Y-%m-%d %H') FROM ev", who: "live", why: "a literal with an offset"},
		{stmt: "SELECT side, DATEDIFF(s, d) FROM ev ORDER BY k", who: "live", why: "a text column"},
		{stmt: "SELECT side, DATEDIFF(d, '2026-01-01') FROM ev ORDER BY k", who: "live", why: "a literal"},
		{stmt: "SELECT side, DATEDIFF(?, d) FROM ev ORDER BY k", args: []any{"2026-02-01"}, who: "live", why: "a placeholder"},
		{stmt: "SELECT side, DATE_FORMAT(d, ?) FROM ev ORDER BY k", args: []any{"%Y"}, who: "live", why: "a format bound to a placeholder"},
		{stmt: "SELECT side, DATE_FORMAT(d, '%D') FROM ev ORDER BY k", who: "live", why: "a specifier the copy does not write"},
		{stmt: "SELECT side, s AS d2, DATEDIFF(d, d2) FROM ev ORDER BY k", who: "live", why: "a name the statement gives itself"},
		{stmt: "SELECT side, DATEDIFF(CURDATE() - INTERVAL 1 DAY, d) FROM ev ORDER BY k", who: "live", why: "CURDATE() stepped"},
		{stmt: "SELECT side, TIMESTAMPDIFF(DAY, d2, d) FROM ev ORDER BY k", who: "copy", why: "TIMESTAMPDIFF over two DATE columns (the copy's since its own slice of #2611)"},
		{stmt: "SELECT MIN(side), DATE_FORMAT(d, '%M') AS m FROM ev GROUP BY m ORDER BY m", who: "live", why: "sorted by a name of another length each"},
		{stmt: "SELECT MIN(side), COUNT(*) FROM ev WHERE DATE_FORMAT(dt, '%H:%i') < '1045'", who: "live", why: "a format compared with another text: ':' sorts before a digit on MySQL, after one on the copy"},
		{stmt: "SELECT side, s AS `d` FROM ev ORDER BY DATE_FORMAT(d, '%Y'), k", who: "live", why: "a quoted name the statement gives itself"},
	}
	// Both sides under a collation that compares like utf8mb4_0900_ai_ci,
	// the one the port forwards under whatever the client's handshake asks
	// for (#2810): under the driver's default, utf8mb4_general_ci, the
	// direct connection compared '10:47' < '1045' the other way, and a row
	// of "now minus an hour" in hour 10 UTC failed the format case below.
	// utf8mb4_unicode_ci, which every supported MySQL and MariaDB has.
	sameCollation := func(dsn string) string {
		if strings.Contains(dsn, "?") {
			return dsn + "&collation=utf8mb4_unicode_ci"
		}
		return dsn + "?collation=utf8mb4_unicode_ci"
	}
	for _, zone := range []string{"+00:00", "+05:00"} {
		routed := openRaw(t, withZone(sameCollation(portDSN), zone))
		live := openRaw(t, withZone(sameCollation(sourceDSN), zone))
		for _, c := range cases {
			// A statement that reads the clock is asked of the source before
			// and after the port: the port's answer may fall on either side
			// of a minute or of midnight (clockAnswers).
			ask := func() string { return answerText(live, c.stmt, c.args...) }
			before, start := "", time.Now()
			if readsClock.MatchString(c.stmt) {
				before = ask()
			}
			got := answerText(routed, c.stmt, c.args...)
			t.Logf("[%s] routed %-60.300s %s", zone, got, c.stmt)
			if strings.HasPrefix(got, "ERROR ") || !strings.Contains(got, c.who+"|") {
				t.Errorf("[%s] routed %q (%s):\n  got  %s\n  want it answered by %s", zone, c.stmt, c.why, got, c.who)
				continue
			}
			want := ask()
			if why := clockAnswers(strings.ReplaceAll(got, "copy|", "live|"), before, want, start); why != "" {
				t.Errorf("[%s] routed %q (%s):\n  got  %s\n  %s", zone, c.stmt, c.why, got, why)
			}
		}
	}
}
