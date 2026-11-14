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
)

// #2800 against a real MySQL: under routing, a date stepped forward or back
// by an INTERVAL (DATE_ADD, ADDDATE, d + INTERVAL n unit, d - INTERVAL n
// unit) to a time after the year 9999 or before the year 1 is NULL on MySQL.
// The copy computes a year of five digits or the year 0 there, so such a
// statement is answered by MySQL; inside the years 1 to 9999 the copy still
// answers, to the last second of 9999.
func TestIntegrationSQLCompareDateAddRouted(t *testing.T) {
	srcDB, srcName := testutil.CreateTestDB(t)
	now := time.Now().UTC().Truncate(time.Second)
	indexDSN := seedFlashbackIndex(t, "alice", now.Truncate(time.Hour))
	sourceDSN := testutil.IntegrationDSN(srcName)
	var version string
	if err := srcDB.QueryRow("SELECT VERSION()").Scan(&version); err != nil {
		t.Fatal(err)
	}
	t.Logf("source: %s", version)

	// k = 3 is a second before the end of 9999 (a DATE its last day), k = 4
	// the first moment of the year 1, k = 2 a TIMESTAMP at the start of the
	// last day of 2026 in UTC.
	rows := func(side string) [][]string {
		return [][]string{
			{"1", "1", side, "2026-03-31", "2026-03-31 10:00:00", "2026-03-31 23:30:00"},
			{"2", "2", side, "2024-02-29", "2024-02-29 00:00:00", "2026-12-31 00:00:00"},
			{"3", "3", side, "9999-12-31", "9999-12-31 23:59:58", `\N`},
			{"4", "4", side, "0001-01-01", "0001-01-01 00:00:00", `\N`},
			{"5", "5", side, `\N`, `\N`, `\N`},
		}
	}
	src := openRaw(t, withZone(sourceDSN, "+00:00"))
	const create = "CREATE TABLE ev (id INT NOT NULL PRIMARY KEY, k INT, side VARCHAR(8), d DATE, dt DATETIME, ts TIMESTAMP NULL)"
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

	// who is the side that must answer, who5 the side under +05:00 when it
	// is another one. The TIMESTAMP of k = 2 is 2026-12-31 00:00:00 on the
	// wall clock of UTC and 05:00:00 five hours east: 7973 years and 86399
	// seconds later is the last second of 9999 in UTC and past it there.
	cases := []struct {
		stmt      string
		args      []any
		who, who5 string
		why       string
	}{
		{stmt: "SELECT side, DATE_ADD(dt, INTERVAL 1 DAY), DATE_ADD(dt, INTERVAL 1 MONTH), ADDDATE(dt, INTERVAL 1 YEAR), dt + INTERVAL 1 MONTH, dt - INTERVAL 1 YEAR FROM ev WHERE k NOT IN (3, 4) ORDER BY k", who: "copy", why: "a DATETIME, both spellings"},
		{stmt: "SELECT side, DATE_ADD(ts, INTERVAL 1 MONTH), ts + INTERVAL 2 HOUR, ts - INTERVAL 1 DAY FROM ev WHERE k NOT IN (3, 4) ORDER BY k", who: "copy", why: "a TIMESTAMP, in the session's zone"},
		{stmt: "SELECT side, DATE_ADD(dt, INTERVAL 1 SECOND), dt + INTERVAL 1 SECOND FROM ev WHERE k = 3", who: "copy", why: "the last second of 9999"},
		{stmt: "SELECT side, DATE_ADD(d, INTERVAL 23 HOUR), d + INTERVAL 86399 SECOND FROM ev WHERE k = 3", who: "copy", why: "a DATE to the last hour and second of 9999"},
		{stmt: "SELECT side, ts + INTERVAL 7973 YEAR + INTERVAL 86399 SECOND, DATE_ADD(DATE_ADD(ts, INTERVAL 7973 YEAR), INTERVAL 86399 SECOND) FROM ev WHERE k = 2", who: "copy", who5: "live", why: "a TIMESTAMP to the last second of 9999 in UTC"},
		{stmt: "SELECT side, ts + INTERVAL 7973 YEAR + INTERVAL 68399 SECOND FROM ev WHERE k = 2", who: "copy", why: "a TIMESTAMP to the last second of 9999 five hours east"},
		{stmt: "SELECT side, ts + INTERVAL 7973 YEAR + INTERVAL 68400 SECOND FROM ev WHERE k = 2", who: "copy", who5: "live", why: "a second past 9999 five hours east"},
		{stmt: "SELECT side, DATE_ADD(DATE_ADD(ts, INTERVAL 7973 YEAR), INTERVAL 86400 SECOND) FROM ev WHERE k = 2", who: "live", why: "a TIMESTAMP a second past 9999 in UTC"},
		{stmt: "SELECT MIN(side), COUNT(*) FROM ev WHERE dt <= NOW() + INTERVAL 1 DAY", who: "copy", why: "NOW() stepped forward, compared"},
		{stmt: "SELECT MIN(side), COUNT(*) FROM ev WHERE dt <= DATE_ADD(NOW(), INTERVAL 1 DAY) AND dt > NOW() - INTERVAL 30 YEAR", who: "copy", why: "NOW() stepped both ways, both spellings"},
		{stmt: "SELECT MIN(side), COUNT(*) FROM ev WHERE dt >= DATE_ADD(NOW(), INTERVAL -30 DAY)", who: "live", why: "a sign before the number, which the copy refuses"},
		{stmt: "SELECT side, dt + INTERVAL 1 DAY FROM ev WHERE k > ? AND k NOT IN (3, 4) ORDER BY k", args: []any{0}, who: "copy", why: "prepared, inside the range"},
		{stmt: "SELECT side, dt FROM ev WHERE (dt + INTERVAL 1 DAY) > dt AND k <> 3 ORDER BY k", who: "copy", why: "a group compared"},

		{stmt: "SELECT side, DATE_ADD(dt, INTERVAL 1 DAY) FROM ev ORDER BY k", who: "live", why: "DATE_ADD past 9999"},
		{stmt: "SELECT side, ADDDATE(dt, INTERVAL 1 DAY) FROM ev ORDER BY k", who: "live", why: "ADDDATE past 9999"},
		{stmt: "SELECT side, dt + INTERVAL 1 DAY FROM ev ORDER BY k", who: "live", why: "+ INTERVAL past 9999"},
		{stmt: "SELECT side, DATE_ADD(dt, INTERVAL 2 SECOND), dt + INTERVAL 2 SECOND FROM ev WHERE k = 3", who: "live", why: "a second past 9999"},
		{stmt: "SELECT side, DATE_ADD(d, INTERVAL 1 DAY), d + INTERVAL 24 HOUR FROM ev WHERE k = 3", who: "live", why: "a DATE past 9999"},
		{stmt: "SELECT side, INTERVAL 1 DAY + dt FROM ev WHERE k = 3", who: "live", why: "the INTERVAL first"},
		{stmt: "SELECT MIN(side), COUNT(dt + INTERVAL 1 DAY) FROM ev", who: "live", why: "a NULL MySQL does not count"},
		{stmt: "SELECT side, dt FROM ev WHERE dt + INTERVAL 1 DAY > dt ORDER BY k", who: "live", why: "a NULL MySQL does not keep"},
		{stmt: "SELECT side, dt FROM ev WHERE k = 1 OR NOW() + INTERVAL 7974 YEAR > dt ORDER BY k", who: "live", why: "NOW() past 9999, compared"},
		{stmt: "SELECT side, DATE_ADD(NOW(), INTERVAL 7974 YEAR) FROM ev WHERE k = 1", who: "live", why: "NOW() past 9999"},
		{stmt: "SELECT side, DATE_ADD(TIMESTAMP '9999-12-31 00:00:00', INTERVAL 1 DAY), TIMESTAMP '9999-12-31 00:00:00' + INTERVAL 1 DAY FROM ev WHERE k = 1", who: "live", why: "the issue's literal (MySQL has no DATETIME '...')"},
		{stmt: "SELECT side, DATE '9999-12-31' + INTERVAL 1 DAY FROM ev WHERE k = 1", who: "live", why: "a DATE literal past 9999"},
		{stmt: "SELECT side, DATE_ADD(dt, INTERVAL -1 DAY), dt + INTERVAL -1 DAY FROM ev WHERE k = 4", who: "live", why: "before the year 1, a negative amount"},
		{stmt: "SELECT side, DATE_ADD(dt, INTERVAL '-1' DAY) FROM ev WHERE k = 4", who: "live", why: "before the year 1, a quoted negative amount"},
		{stmt: "SELECT side, dt - INTERVAL 1 DAY FROM ev WHERE k = 4", who: "live", why: "- INTERVAL before the year 1"},
		{stmt: "SELECT side, dt + INTERVAL 1 DAY FROM ev WHERE k > ? ORDER BY k", args: []any{0}, who: "live", why: "prepared, past 9999"},
		{stmt: "SELECT side, ? + INTERVAL 1 DAY FROM ev WHERE k = 1", args: []any{"2026-01-01 00:00:00"}, who: "live", why: "a placeholder stepped"},
		{stmt: "SELECT side, DATE_ADD(?, INTERVAL 1 DAY) FROM ev WHERE k = 1", args: []any{"2026-01-01 00:00:00"}, who: "live", why: "DATE_ADD of a placeholder"},
	}
	asText := func(dsn string) string { return strings.Replace(dsn, "parseTime=true", "parseTime=false", 1) }
	for _, zone := range []string{"+00:00", "+05:00"} {
		routedText := openRaw(t, asText(withZone(portDSN, zone)))
		liveText := openRaw(t, asText(withZone(sourceDSN, zone)))
		routedBin := openRaw(t, withZone(portDSN, zone))
		liveBin := openRaw(t, withZone(sourceDSN, zone))
		for _, c := range cases {
			who := c.who
			if zone != "+00:00" && c.who5 != "" {
				who = c.who5
			}
			routed, live := routedText, liveText
			if len(c.args) > 0 {
				routed, live = routedBin, liveBin
			}
			got := typedAnswer(routed, c.stmt, c.args...)
			t.Logf("[%s] routed %-60.300s %s", zone, got, c.stmt)
			if strings.HasPrefix(got, "ERROR ") || !strings.Contains(got, "("+who+",") {
				t.Errorf("[%s] routed %q (%s):\n  got  %s\n  want it answered by %s", zone, c.stmt, c.why, got, who)
				continue
			}
			want := typedAnswer(live, c.stmt, c.args...)
			if strings.ReplaceAll(got, "(copy,", "(live,") != want {
				t.Errorf("[%s] routed %q (%s):\n  got  %s\n  want %s, the source's own answer", zone, c.stmt, c.why, got, want)
			}
		}
	}
}
