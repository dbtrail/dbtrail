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

// #2611 against a real MySQL: under routing, TIMESTAMPDIFF over DATE columns,
// DATETIME and TIMESTAMP columns without fractional seconds, NOW(), CURDATE()
// and the keywords is answered by the copy with the source's answer, for every
// unit, both ways round, over month ends and February 29; a fraction, text, a
// literal, a placeholder, a unit spelled SQL_TSI_, a TIMESTAMP under a named
// zone and arithmetic on the result stay on the source.
func TestIntegrationSQLCompareTimestampDiffRouted(t *testing.T) {
	srcDB, srcName := testutil.CreateTestDB(t)
	timestampDiffRouted(t, srcDB, srcName, testutil.IntegrationDSN(srcName))
}

// The same against a MariaDB source. A test of its own, with MariaDB in its
// name: the MariaDB job picks its tests by name.
func TestIntegrationSQLCompareTimestampDiffRoutedMariaDB(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	srcDB, srcName := testutil.CreateTestMariaDB(t)
	timestampDiffRouted(t, srcDB, srcName, testutil.MariaDBBaseDSN()+"/"+srcName+"?parseTime=true")
}

func timestampDiffRouted(t *testing.T, srcDB *sql.DB, srcName, sourceDSN string) {
	now := time.Now().UTC().Truncate(time.Second)
	indexDSN := seedFlashbackIndex(t, "alice", now.Truncate(time.Hour))
	var version string
	if err := srcDB.QueryRow("SELECT VERSION()").Scan(&version); err != nil {
		t.Fatal(err)
	}
	t.Logf("source: %s", version)

	// Pairs of dates (dt to dt2, ts to ts2): January 31 to February 28 (no
	// month on MySQL, one boundary crossed), February 29 to the next
	// February 28 and to the next February 29, the years 1 and 9999, a
	// quarter that ends a day short, a pair that goes backwards, rows of the
	// last hours for NOW(), the clock changes of America/New_York in UTC
	// (k = 5 and 7), a fraction, text, and NULLs.
	at := func(d time.Duration) string { return now.Add(-d).Format("2006-01-02 15:04:05") }
	rows := func(side string) [][]string {
		return [][]string{
			{"1", "1", side, "2026-01-31", "2026-01-31 00:00:00", "2026-02-28 00:00:00", "2026-01-31 20:00:00", "2026-02-28 20:00:00", "2026-01-15 10:00:00.500000", "2026-01-31"},
			{"2", "2", side, "2024-02-29", "2024-02-29 00:00:00", "2025-02-28 00:00:00", "2024-02-29 12:00:00", "2028-02-29 12:00:00", "2026-03-01 00:00:00.900000", "20240229"},
			{"3", "3", side, now.Add(-24 * time.Hour).Format("2006-01-02"), at(48 * time.Hour), at(time.Hour), at(3 * time.Hour), at(30 * time.Minute), `\N`, "2026-01-15 02:00:00+05:00"},
			{"4", "4", side, "0001-01-01", "0001-01-01 00:00:00", "9999-12-31 23:59:59", `\N`, `\N`, `\N`, "x"},
			{"5", "5", side, "2026-03-31", "2026-03-31 00:00:00", "2026-06-30 00:00:00", "2026-03-08 06:30:00", "2026-03-08 07:30:00", `\N`, `\N`},
			{"6", "6", side, `\N`, `\N`, `\N`, `\N`, `\N`, `\N`, `\N`},
			{"7", "7", side, "2026-05-31", "2026-05-31 08:00:00", "2026-02-28 12:00:00", "2026-11-01 05:30:00", "2026-11-01 06:30:00", `\N`, "y"},
		}
	}
	src := openRaw(t, withZone(sourceDSN, "+00:00"))
	const create = "CREATE TABLE ev (id INT NOT NULL PRIMARY KEY, k INT, side VARCHAR(8), d DATE, dt DATETIME, dt2 DATETIME, ts TIMESTAMP NULL, ts2 TIMESTAMP NULL, dt6 DATETIME(6), s VARCHAR(32))"
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

	type answerCase struct {
		stmt string
		args []any
		who  string
		why  string
	}
	// ORDER BY k and WHERE k: a column with no index, so the plan reads the
	// whole table and the copy is asked. The answer opens with side, then a
	// comma, so who answered shows.
	cases := []answerCase{
		{stmt: "SELECT side, TIMESTAMPDIFF(MICROSECOND, dt, dt2), TIMESTAMPDIFF(SECOND, dt, dt2), TIMESTAMPDIFF(MINUTE, dt, dt2), TIMESTAMPDIFF(HOUR, dt, dt2), TIMESTAMPDIFF(DAY, dt, dt2), TIMESTAMPDIFF(WEEK, dt, dt2) FROM ev ORDER BY k", who: "copy", why: "a DATETIME, the units of a fixed length"},
		{stmt: "SELECT side, TIMESTAMPDIFF(MICROSECOND, dt2, dt), TIMESTAMPDIFF(SECOND, dt2, dt), TIMESTAMPDIFF(HOUR, dt2, dt), TIMESTAMPDIFF(WEEK, dt2, dt) FROM ev ORDER BY k", who: "copy", why: "backwards"},
		{stmt: "SELECT side, TIMESTAMPDIFF(MONTH, dt, dt2), TIMESTAMPDIFF(QUARTER, dt, dt2), TIMESTAMPDIFF(YEAR, dt, dt2), TIMESTAMPDIFF(MONTH, dt2, dt), TIMESTAMPDIFF(QUARTER, dt2, dt), TIMESTAMPDIFF(YEAR, dt2, dt) FROM ev ORDER BY k", who: "copy", why: "months, quarters and years, month ends"},
		{stmt: "SELECT side, TIMESTAMPDIFF(MINUTE, ts, ts2), TIMESTAMPDIFF(DAY, ts, ts2), TIMESTAMPDIFF(MONTH, ts, ts2), TIMESTAMPDIFF(YEAR, ts, ts2), TIMESTAMPDIFF(HOUR, ts, dt2) FROM ev ORDER BY k", who: "copy", why: "a TIMESTAMP, on the session's wall clock"},
		{stmt: "SELECT side, TIMESTAMPDIFF(HOUR, d, dt2), TIMESTAMPDIFF(MONTH, d, dt2), TIMESTAMPDIFF(DAY, dt, d) FROM ev ORDER BY k", who: "copy", why: "a DATE, at its midnight"},
		{stmt: "SELECT side, timestampdiff(/* u */ month, `dt`, ev.dt2), TimestampDiff(day, (dt), dt2) FROM ev ORDER BY k", who: "copy", why: "spelled otherwise"},
		{stmt: "SELECT MIN(side), COUNT(*) FROM ev WHERE TIMESTAMPDIFF(MINUTE, dt2, NOW()) BETWEEN 30 AND 400", who: "copy", why: "NOW(), the last hours (five more of them five hours east)"},
		{stmt: "SELECT MIN(side), COUNT(*) FROM ev WHERE TIMESTAMPDIFF(HOUR, ts2, UTC_TIMESTAMP()) < 2", who: "copy", why: "UTC_TIMESTAMP() and a TIMESTAMP"},
		{stmt: "SELECT MIN(side), COUNT(*) FROM ev WHERE TIMESTAMPDIFF(DAY, dt, CURRENT_TIMESTAMP) > 1", who: "copy", why: "the keyword"},
		{stmt: "SELECT side, TIMESTAMPDIFF(YEAR, d, CURDATE()), TIMESTAMPDIFF(MONTH, CURDATE(), dt2) FROM ev WHERE k <> 3 ORDER BY k", who: "copy", why: "CURDATE()"},
		{stmt: "SELECT side, s FROM ev WHERE TIMESTAMPDIFF(DAY, dt, dt2) > 30 ORDER BY TIMESTAMPDIFF(HOUR, dt, dt2)", who: "copy", why: "in WHERE and ORDER BY"},
		{stmt: "SELECT MIN(side), TIMESTAMPDIFF(MONTH, dt, dt2) AS m, COUNT(*) FROM ev GROUP BY m ORDER BY m", who: "copy", why: "grouped and sorted, a NULL first"},
		{stmt: "SELECT side, TIMESTAMPDIFF(DAY, dt, dt2), TIMESTAMPDIFF(MONTH, dt, dt2) FROM ev WHERE k > ? ORDER BY k", args: []any{0}, who: "copy", why: "prepared"},

		{stmt: "SELECT side, TIMESTAMPDIFF(MICROSECOND, dt6, dt2) FROM ev ORDER BY k", who: "live", why: "a fraction"},
		{stmt: "SELECT side, TIMESTAMPDIFF(DAY, s, dt2) FROM ev ORDER BY k", who: "live", why: "a text column"},
		{stmt: "SELECT side, TIMESTAMPDIFF(DAY, '2026-01-31', dt2) FROM ev ORDER BY k", who: "live", why: "a literal"},
		{stmt: "SELECT side, TIMESTAMPDIFF(DAY, ?, dt2) FROM ev ORDER BY k", args: []any{"2026-01-31"}, who: "live", why: "a date bound to a placeholder"},
		{stmt: "SELECT side, TIMESTAMPDIFF(SQL_TSI_DAY, dt, dt2) FROM ev ORDER BY k", who: "live", why: "a unit spelled SQL_TSI_"},
		{stmt: "SELECT side, TIMESTAMPDIFF(DAY, dt + INTERVAL 1 DAY, dt2) FROM ev ORDER BY k", who: "live", why: "an expression"},
		{stmt: "SELECT side, s AS dt, TIMESTAMPDIFF(DAY, dt, dt2) FROM ev ORDER BY k", who: "live", why: "a name the statement gives itself"},
		{stmt: "SELECT side, TIMESTAMPDIFF(DAY, dt, dt2) + 1 FROM ev ORDER BY k", who: "live", why: "arithmetic on the result"},
		{stmt: "SELECT MIN(side), AVG(TIMESTAMPDIFF(DAY, dt, dt2)) FROM ev", who: "live", why: "AVG of the result"},
		{stmt: "SELECT side, TIMESTAMPDIFF(DAY, dt, dt2) - TIMESTAMPDIFF(DAY, d, dt2) FROM ev ORDER BY k", who: "live", why: "one result minus another"},
		{stmt: "SELECT side, TIMESTAMPDIFF(DAY, dt, dt2), TIMEDIFF(dt2, dt) FROM ev ORDER BY k", who: "live", why: "TIMEDIFF beside it"},
	}
	// Without parseTime, and with each column's type: the text and the type
	// each side sends are compared. A prepared statement is read with
	// parseTime, in the binary protocol.
	asText := func(dsn string) string { return strings.Replace(dsn, "parseTime=true", "parseTime=false", 1) }
	compare := func(zone string, cases []answerCase) {
		routedText := openRaw(t, asText(withZone(portDSN, zone)))
		liveText := openRaw(t, asText(withZone(sourceDSN, zone)))
		routedBin := openRaw(t, withZone(portDSN, zone))
		liveBin := openRaw(t, withZone(sourceDSN, zone))
		for _, c := range cases {
			routed, live := routedText, liveText
			if len(c.args) > 0 {
				routed, live = routedBin, liveBin
			}
			got := typedAnswer(routed, c.stmt, c.args...)
			t.Logf("[%s] routed %-60.300s %s", zone, got, c.stmt)
			if strings.HasPrefix(got, "ERROR ") || !strings.Contains(got, "("+c.who+",") {
				t.Errorf("[%s] routed %q (%s):\n  got  %s\n  want it answered by %s", zone, c.stmt, c.why, got, c.who)
				continue
			}
			want := typedAnswer(live, c.stmt, c.args...)
			if strings.ReplaceAll(got, "(copy,", "(live,") != want {
				t.Errorf("[%s] routed %q (%s):\n  got  %s\n  want %s, the source's own answer", zone, c.stmt, c.why, got, want)
			}
		}
	}
	for _, zone := range []string{"+00:00", "+05:00"} {
		compare(zone, cases)
	}

	// Under a named zone a DATETIME is still the copy's to answer; a
	// TIMESTAMP is not: MySQL reads it on a wall clock that, at the clock
	// changes of k = 5 and 7, puts no minute or two hours between two
	// instants an hour apart.
	var tz sql.NullString
	if err := srcDB.QueryRow("SELECT CONVERT_TZ('2026-11-02 12:00:00', 'UTC', 'America/New_York')").Scan(&tz); err != nil || !tz.Valid {
		t.Errorf("named zone not compared: the source has no time zone tables (%v); load them (mysql_tzinfo_to_sql)", err)
	} else {
		compare("America/New_York", []answerCase{
			{stmt: "SELECT side, TIMESTAMPDIFF(MINUTE, dt, dt2), TIMESTAMPDIFF(MONTH, dt, dt2), TIMESTAMPDIFF(DAY, d, dt2) FROM ev ORDER BY k", who: "copy", why: "a DATETIME under a named zone"},
			{stmt: "SELECT side, TIMESTAMPDIFF(MINUTE, ts, ts2) FROM ev ORDER BY k", who: "live", why: "a TIMESTAMP under a named zone"},
		})
	}

	// A client that calls one of the copy's macros by name: MySQL has no
	// such function (and reads no INTERVAL in one), and the port refuses as
	// MySQL does instead of answering from the copy.
	routed := openRaw(t, asText(withZone(portDSN, "+00:00")))
	live := openRaw(t, asText(withZone(sourceDSN, "+00:00")))
	for _, stmt := range []string{"SELECT side, _bintrail_timestampdiff(dt, dt2, 1) FROM ev", "SELECT side, _bintrail_date_sub(dt, INTERVAL 1 DAY) FROM ev"} {
		got, want := typedAnswer(routed, stmt), typedAnswer(live, stmt)
		if !strings.HasPrefix(got, "ERROR ") || got != want {
			t.Errorf("routed %q:\n  got  %s\n  want %s, the source's own refusal", stmt, got, want)
		}
	}
}
