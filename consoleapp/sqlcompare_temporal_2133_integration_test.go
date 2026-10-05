//go:build integration

package consoleapp

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/readrouter"
	"github.com/dbtrail/dbtrail/internal/sqlcompare"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// #2133 against a real MySQL: arithmetic on a date column, bit operators and
// a two-digit year are answered by both sides, each with another value and
// no error. See temporalColumns.
func TestIntegrationSQLCompareTemporalColumns(t *testing.T) {
	srcDB, srcName := testutil.CreateTestDB(t)
	temporalColumns(t, srcDB, srcName, testutil.IntegrationDSN(srcName))
}

// The same against a MariaDB source. A test of its own, with MariaDB in its
// name: the MariaDB job picks its tests by name.
func TestIntegrationSQLCompareTemporalColumnsMariaDB(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	srcDB, srcName := testutil.CreateTestMariaDB(t)
	temporalColumns(t, srcDB, srcName, testutil.MariaDBBaseDSN()+"/"+srcName+"?parseTime=true")
}

// temporalColumns is the body of both tests. One table, created on the
// source in plain DDL; the copy's file carries what that server's own SHOW
// CREATE TABLE prints, as a dump does, so the column types the copy reads
// are the ones that server writes. The rows are the same on both sides but
// for the column side, which says who answered: "live" on the source, "copy"
// on the copy.
//
// Two parts. What the copy answers when nobody asked for MySQL's answer
// (sql-compare, a port with no routing): every statement here is DIFFERENT,
// which is the bug's shape, and the ones the text alone can tell are vetoed.
// Then the same shapes under read routing, where each must be the source's
// answer, but for the two listed as known differences.
func temporalColumns(t *testing.T, srcDB *sql.DB, srcName, sourceDSN string) {
	now := time.Now().UTC().Truncate(time.Hour)
	indexDSN := seedFlashbackIndex(t, "alice", now)

	var version string
	if err := srcDB.QueryRow("SELECT VERSION()").Scan(&version); err != nil {
		t.Fatal(err)
	}
	t.Logf("source: %s", version)

	row := func(side string) [][]string {
		return [][]string{
			{"1", "2026-01-01", "2026-01-01 10:00:00", "10:00:00", "2026", "-8", side},
			{"2", "2026-01-15", "2026-01-15 11:30:00", "11:30:00", "2026", "5", side},
			{"3", "2026-02-03", "2026-02-03 12:00:01", "12:00:01", "2025", "-1", side},
		}
	}
	if _, err := srcDB.Exec("CREATE TABLE ev (id INT NOT NULL PRIMARY KEY, created_on DATE, dt DATETIME, tm TIME, yr YEAR, n INT, side VARCHAR(8))"); err != nil {
		t.Fatal(err)
	}
	for _, r := range row("live") {
		if _, err := srcDB.Exec("INSERT INTO ev VALUES ('" + strings.Join(r, "','") + "')"); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := srcDB.Exec("ANALYZE TABLE ev"); err != nil {
		t.Fatal(err)
	}
	var name, ddl string
	if err := srcDB.QueryRow("SHOW CREATE TABLE ev").Scan(&name, &ddl); err != nil {
		t.Fatal(err)
	}
	baseDir := t.TempDir()
	writeOrderSnapshot(t, baseDir, srcName, []orderTable{{name: "ev", ddl: ddl + ";\n", rows: row("live"), copyRows: row("copy"), footer: true}})

	reg, err := console.LoadRegistry(t.TempDir() + "/servers.yaml")
	if err != nil {
		t.Fatal(err)
	}
	ent, err := reg.Add(console.ServerEntry{Name: "srva", DSN: indexDSN, SourceDSN: sourceDSN, BaselineDir: baseDir})
	if err != nil {
		t.Fatal(err)
	}
	// A second server over the same copy, for the comparison only (see
	// compareInHalves).
	entB, err := reg.Add(console.ServerEntry{Name: "srvb", DSN: indexDSN, SourceDSN: sourceDSN, BaselineDir: baseDir})
	if err != nil {
		t.Fatal(err)
	}
	srv, err := console.New(console.Config{Listen: "127.0.0.1:0", Token: "tok", Registry: reg,
		FlashbackListen: "127.0.0.1:3308", ReadRouting: console.ReadRoutingConfig{MaxCopyAge: time.Hour, ScanRows: 2}})
	if err != nil {
		t.Fatal(err)
	}
	serve := func(cfg flashbackConfig) string {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		served := make(chan struct{})
		go func() { _ = serveFlashback(ctx, srv, ln, cfg); close(served) }()
		t.Cleanup(func() { cancel(); <-served })
		return ln.Addr().String()
	}
	policy := readrouter.Policy{ScanRows: 2}

	const (
		plus     = "SELECT created_on + 1 FROM ev ORDER BY n"
		minus    = "SELECT MAX(created_on) - MIN(created_on) FROM ev"
		avg      = "SELECT AVG(dt) FROM ev"
		alias    = "SELECT d + 1 FROM (SELECT created_on AS d, n FROM ev) x ORDER BY n"
		timeText = "SELECT n FROM ev WHERE tm >= '9:00:00' ORDER BY n"
		bitOr    = "SELECT n | 0 FROM ev ORDER BY n"
		yearTwo  = "SELECT n FROM ev WHERE created_on = '26-01-15' ORDER BY n"
		castFrac = "SELECT CAST('2026-01-01 10:00:00.6' AS DATETIME)"
		// The two known differences this change leaves, each with its line in
		// docs/time-travel-sql.md.
		interval = "SELECT created_on + INTERVAL 1 DAY FROM ev ORDER BY n"
		concat   = "SELECT CONCAT(dt, '') FROM ev ORDER BY n"
	)

	// 1. The copy, asked as a port with no routing answers: another value
	// with no error.
	copyAddr := serve(flashbackConfig{})
	copies := [2]string{
		fmt.Sprintf("%s:tok@tcp(%s)/%s", ent.ID, copyAddr, srcName),
		fmt.Sprintf("%s:tok@tcp(%s)/%s", entB.ID, copyAddr, srcName),
	}
	diff := sqlcompare.Different
	fixtures := []valuesFixture{
		{plus, diff, "", "a DATE plus a number: 20260102 on the source, 2026-01-02 on the copy"},
		{minus, diff, "", "the difference of two numbers on the source (102), a count of days on the copy (33)"},
		{avg, diff, "", "a number on the source, a date and time on the copy"},
		{alias, diff, "", "the same date under an alias, from outside its subquery"},
		{timeText, diff, "rows", "a TIME is text on the copy: '10:00:00' sorts before '9:00:00' there"},
		{bitOr, diff, "", "64 unsigned bits on the source (18446744073709551608), signed on the copy (-8)"},
		{yearTwo, diff, "rows", "the year 2026 on the source (one row), the year 26 on the copy (none)"},
		{castFrac, diff, "", "the fraction is rounded by MySQL, cut by MariaDB and kept by the copy"},
		{interval, diff, "", "the same day: a DATE on the source, a date and time on the copy"},
		{concat, diff, "", "a DATETIME as text ends in +00 on the copy"},
	}
	by := compareInHalves(t, sourceDSN, copies, policy, fixtures)
	checkValuesFixtures(t, sourceDSN, copies[0], fixtures, by)
	// What the text alone can tell is kept on the source before the plan is
	// asked for; the rest is the table's to tell, which sql-compare does not
	// ask (part 2 does).
	for stmt, veto := range map[string]string{
		bitOr: "bit operator", yearTwo: "two-digit year", castFrac: "CAST to DATETIME or TIME",
		plus: "", minus: "", avg: "", alias: "", timeText: "", interval: "", concat: "",
	} {
		r := by[stmt]
		if veto == "" && r.RouteRule == "veto" {
			t.Errorf("%q: vetoed from the text (%s); it is the table's column types that tell", stmt, r.RouteReason)
		}
		if veto != "" && (r.Route != "mysql" || r.RouteRule != "veto" || !strings.Contains(r.RouteReason, veto)) {
			t.Errorf("%q: route=%s rule=%s (%s), want it kept on the source by the veto %q", stmt, r.Route, r.RouteRule, r.RouteReason, veto)
		}
	}

	// PROBE-START
	{
		psrc := openRaw(t, strings.Replace(sourceDSN, "parseTime=true", "parseTime=false", 1))
		pcp := openRaw(t, copies[0])
		for _, s := range []string{
			"SELECT LAST_DAY('2026-01-15') + 1", "SELECT MAKEDATE(2026, 1) + 1", "SELECT ADDDATE('2026-01-15', 1) + 0", "SELECT FROM_DAYS(739000) + 1",
			"SELECT created_on AS d FROM ev ORDER BY d + 0 DESC", "SELECT created_on AS d FROM ev HAVING d + 0 > 20260110", "SELECT created_on AS d, COUNT(*) FROM ev GROUP BY d + 0",
			"SELECT DATE_FORMAT(created_on, '%Y-%m') FROM ev ORDER BY n", "SELECT DATE_FORMAT(dt, '%Y-%m-%d %H:%i') FROM ev ORDER BY n", "SELECT IF(n > 0, created_on, dt) FROM ev ORDER BY n",
			"SELECT created_on FROM ev UNION ALL SELECT dt FROM ev", "SELECT n FROM ev WHERE created_on = dt - INTERVAL 10 HOUR ORDER BY n", "SELECT LEFT(created_on, 7) FROM ev ORDER BY n",
			"SELECT SUBSTRING(created_on, 1, 4) FROM ev ORDER BY n", "SELECT created_on, dt FROM ev WHERE dt > created_on ORDER BY n", "SELECT TO_DAYS(created_on) FROM ev ORDER BY n",
			"SELECT YEAR(created_on) * 100 + MONTH(created_on) FROM ev ORDER BY n", "SELECT n FROM ev WHERE yr = 2026 ORDER BY n", "SELECT UNIX_TIMESTAMP(dt) FROM ev",
			"SELECT TIMESTAMP(created_on) FROM ev ORDER BY n", "SELECT created_on + 1 FROM ev e WHERE EXISTS (SELECT 1) ORDER BY n", "SELECT STR_TO_DATE('2026-01-15', '%Y-%m-%d') + 1",
		} {
			t.Logf("PROBE\t%s\t%s\t%s\t%s", s, strings.ReplaceAll(rawAnswer(psrc, s), "\n", " "), strings.ReplaceAll(rawAnswer(pcp, s), "\n", " "), readrouter.Veto(s))
		}
	}
	// PROBE-END

	// 2. The same server with read routing on. The table has three rows and
	// the scan rule is at two, and no statement here reads by the key, so
	// each plan is the copy's unless something keeps the statement on the
	// source. Every statement selects side, which says who answered.
	src := openRaw(t, sourceDSN)
	routed := openFlashback(t, serve(flashbackConfig{RouteMaxCopyAge: time.Hour, RoutePolicy: policy}), ent.ID, "tok", srcName)
	defer routed.Close()
	for _, c := range []struct {
		stmt, who, why string
		args           []any
	}{
		{stmt: "SELECT side, created_on + 1 FROM ev ORDER BY n", who: "live", why: "a DATE column next to +"},
		{stmt: "SELECT side, `ev`.`created_on` - 1 FROM `ev` ORDER BY `n`", who: "live", why: "the same, the way an ORM quotes it"},
		{stmt: "SELECT MIN(side), MAX(created_on) - MIN(created_on) FROM ev", who: "live", why: "one date minus another, each inside a call"},
		{stmt: "SELECT MIN(side), AVG(dt) FROM ev", who: "live", why: "AVG of a DATETIME column"},
		{stmt: "SELECT side, d + 1 FROM (SELECT created_on AS d, n, side FROM ev) x ORDER BY n", who: "live", why: "an alias of the date, used outside its subquery"},
		{stmt: "SELECT side, n FROM ev WHERE tm >= '9:00:00' ORDER BY n", who: "live", why: "a TIME column named"},
		{stmt: "SELECT side, n FROM ev WHERE yr = 26 ORDER BY n", who: "live", why: "a YEAR column named"},
		{stmt: "SELECT side, n | 0 FROM ev ORDER BY n", who: "live", why: "a bit operator: kept on the source from the text"},
		{stmt: "SELECT side, created_on + ? FROM ev ORDER BY n", args: []any{1}, who: "live", why: "as a prepared statement: the number is bound, the + is in the template"},
		{stmt: "SELECT side, n FROM ev WHERE created_on = ? ORDER BY n", args: []any{"26-01-15"}, who: "live", why: "a two-digit year bound to a prepared statement"},
		// What the copy goes on answering over the same table.
		{stmt: "SELECT side, n + 1, created_on FROM ev WHERE created_on >= '2026-01-15' ORDER BY n", who: "copy", why: "a + that is not next to the date"},
		{stmt: "SELECT side, n FROM ev WHERE created_on = ? ORDER BY n", args: []any{"2026-01-15"}, who: "copy", why: "a date with its whole year bound to a prepared statement"},
		{stmt: "SELECT side, created_on + INTERVAL 1 DAY FROM ev ORDER BY n", who: "copy", why: "a date plus INTERVAL: the same day, a known difference in how it is shown"},
	} {
		got := answerText(routed, c.stmt, c.args...)
		t.Logf("routed  %-60s %s", got, c.stmt)
		if strings.HasPrefix(got, "ERROR ") || !strings.HasPrefix(got, c.who+"|") {
			t.Errorf("routed %q (%s):\n  got  %s\n  want it answered by %s", c.stmt, c.why, got, c.who)
			continue
		}
		if c.who == "live" {
			if want := answerText(src, c.stmt, c.args...); got != want {
				t.Errorf("routed %q (%s):\n  got  %s\n  want %s, the source's own answer", c.stmt, c.why, got, want)
			}
		}
	}
	// The values this is about, on every server: a number built from the
	// date's digits, where the copy would have said 2026-01-02.
	if got := answerText(routed, "SELECT created_on + 1 FROM ev ORDER BY n"); got != "20260102 / 20260204 / 20260116" {
		t.Errorf("routed created_on + 1: got %s, want 20260102 / 20260204 / 20260116", got)
	}

	// Who answered, and why the source: nine statements declined by the
	// copy for a column's type (eight above and the one just now), two kept on
	// the source from the text (the bit operator, the bound two-digit year),
	// three answered by the copy, and the copy at fault in none.
	req := httptest.NewRequest("GET", "http://127.0.0.1/api/flashback", nil)
	req.Header.Set("Authorization", "Bearer tok")
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	var fb struct {
		Routing struct {
			Servers map[string]struct {
				Reasons map[string]uint64 `json:"reasons"`
			} `json:"servers"`
		} `json:"routing"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &fb); err != nil {
		t.Fatalf("decode /api/flashback: %v (%s)", err, rec.Body.String())
	}
	reasons := fb.Routing.Servers[ent.ID].Reasons
	t.Logf("routing reasons: %v", reasons)
	if reasons["copy_columns_differ"] != 9 || reasons["veto"] != 2 || reasons["expensive_plan"] != 3 || reasons["copy_refused"] != 0 || reasons["explain_failed"] != 0 {
		t.Errorf("reasons = %v, want copy_columns_differ 9, veto 2, expensive_plan 3, copy_refused 0, explain_failed 0", reasons)
	}
}
