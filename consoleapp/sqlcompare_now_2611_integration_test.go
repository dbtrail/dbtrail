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
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// #2611 against a real MySQL: "the last N days" reports written with NOW(),
// CURDATE(), UTC_TIMESTAMP() and UTC_DATE() are answered by the copy with the
// source's answer; the forms the copy does not have, and arithmetic that
// turns them into numbers, stay on the source.
func TestIntegrationSQLCompareNow(t *testing.T) {
	srcDB, srcName := testutil.CreateTestDB(t)
	nowCompare(t, srcDB, srcName, testutil.IntegrationDSN(srcName))
}

// The same against a MariaDB source. A test of its own, with MariaDB in its
// name: the MariaDB job picks its tests by name.
func TestIntegrationSQLCompareNowMariaDB(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	srcDB, srcName := testutil.CreateTestMariaDB(t)
	nowCompare(t, srcDB, srcName, testutil.MariaDBBaseDSN()+"/"+srcName+"?parseTime=true")
}

func nowCompare(t *testing.T, srcDB *sql.DB, srcName, sourceDSN string) {
	now := time.Now().UTC().Truncate(time.Second)
	indexDSN := seedFlashbackIndex(t, "alice", now.Truncate(time.Hour))
	var version string
	if err := srcDB.QueryRow("SELECT VERSION()").Scan(&version); err != nil {
		t.Fatal(err)
	}
	t.Logf("source: %s", version)

	// Every row far from every boundary the cases draw (1, 7 and 30 days,
	// in UTC and five hours east of it).
	at := func(d time.Duration) string { return now.Add(-d).Format("2006-01-02 15:04:05") }
	day := func(d time.Duration) string { return now.Add(-d).Format("2006-01-02") }
	rows := func(side string) [][]string {
		return [][]string{
			{"1", "1", side, at(time.Hour), at(time.Hour), day(0)},
			{"2", "2", side, at(72 * time.Hour), at(72 * time.Hour), day(72 * time.Hour)},
			{"3", "3", side, at(40 * 24 * time.Hour), at(40 * 24 * time.Hour), day(40 * 24 * time.Hour)},
		}
	}
	src := openRaw(t, withZone(sourceDSN, "+00:00"))
	const create = "CREATE TABLE ev (id INT NOT NULL PRIMARY KEY, k INT, side VARCHAR(8), dt DATETIME, ts TIMESTAMP NULL, d DATE)"
	if _, err := src.Exec(create); err != nil {
		t.Fatal(err)
	}
	for _, r := range rows("live") {
		if _, err := src.Exec("INSERT INTO ev VALUES ('" + strings.Join(r, "','") + "')"); err != nil {
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

	cases := []struct {
		stmt, who, why string
	}{
		{"SELECT MIN(side), COUNT(*) FROM ev WHERE dt >= NOW() - INTERVAL 1 DAY", "copy", "a DATETIME in the last day"},
		{"SELECT MIN(side), COUNT(*) FROM ev WHERE ts >= NOW() - INTERVAL 7 DAY", "copy", "a TIMESTAMP in the last week"},
		{"SELECT MIN(side), COUNT(*) FROM ev WHERE d >= CURDATE() - INTERVAL 30 DAY", "copy", "a DATE in the last month"},
		{"SELECT MIN(side), COUNT(*) FROM ev WHERE ts BETWEEN UTC_TIMESTAMP() - INTERVAL 30 DAY AND UTC_TIMESTAMP()", "copy", "UTC_TIMESTAMP"},
		{"SELECT MIN(side), COUNT(*) FROM ev WHERE d <= UTC_DATE() + INTERVAL 1 DAY", "copy", "UTC_DATE"},
		{"SELECT MIN(side), YEAR(NOW()) >= 2026, MONTH(CURDATE()) = MONTH(NOW()) FROM ev", "copy", "NOW and CURDATE in the select list"},
		{"SELECT MIN(side), COUNT(*) FROM ev WHERE ts >= CURDATE() - INTERVAL 1 DAY", "copy", "a TIMESTAMP against a date"},
		{"SELECT MIN(side), NOW() > UTC_TIMESTAMP() + INTERVAL 4 HOUR FROM ev", "copy", "NOW() in the session's zone, UTC_TIMESTAMP() in UTC"},
		{"SELECT MIN(side), COUNT(*) FROM ev WHERE dt BETWEEN CURDATE() - INTERVAL 7 DAY AND NOW()", "copy", "a DATETIME between a date and now"},
		{"SELECT MIN(side), SUM(side <= CURDATE()) FROM ev", "live", "CURDATE compared with text"},
		{"SELECT MIN(side), DATE_ADD(CURDATE(), INTERVAL 1 DAY) FROM ev", "live", "a date stepped through DATE_ADD"},
		{"SELECT MIN(side), SUM(dt >= NOW() - INTERVAL 1 DAY * 2) FROM ev", "live", "arithmetic after the INTERVAL"},
		{"SELECT side, k FROM ev WHERE dt < NOW() ORDER BY k", "copy", "a bare comparison"},
		{"SELECT MIN(side), SUM(dt >= NOW() + 0 - 86400) FROM ev", "live", "NOW() + 0 is a number on the source"},
		{"SELECT MIN(side), CURDATE() - 1 FROM ev", "live", "CURDATE() - 1 is a number on the source, yesterday on the copy"},
		{"SELECT MIN(side), COUNT(*) FROM ev WHERE ts >= CURRENT_TIMESTAMP - INTERVAL 1 DAY", "live", "CURRENT_TIMESTAMP stays on the source"},
		{"SELECT MIN(side), COUNT(*) FROM ev WHERE dt < NOW(3)", "live", "NOW with a fraction stays on the source"},
	}
	for _, zone := range []string{"+00:00", "+05:00"} {
		routed := openRaw(t, withZone(portDSN, zone))
		live := openRaw(t, withZone(sourceDSN, zone))
		for _, c := range cases {
			got := answerText(routed, c.stmt)
			t.Logf("[%s] routed %-40s %s", zone, got, c.stmt)
			if strings.HasPrefix(got, "ERROR ") || !strings.HasPrefix(got, c.who+"|") {
				t.Errorf("[%s] routed %q (%s):\n  got  %s\n  want it answered by %s", zone, c.stmt, c.why, got, c.who)
				continue
			}
			want := answerText(live, c.stmt)
			if strings.ReplaceAll(got, "copy|", "live|") != want {
				t.Errorf("[%s] routed %q (%s):\n  got  %s\n  want %s, the source's own answer", zone, c.stmt, c.why, got, want)
			}
		}
	}

	// A session that froze its clock (SET timestamp, as replay tools do):
	// MySQL's NOW() is that moment, a day in 2023, and every row is after
	// it; the copy would read its own clock and count one.
	one, err := openRaw(t, withZone(portDSN, "+00:00")).Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer one.Close()
	if _, err := one.ExecContext(context.Background(), "SET timestamp = 1700000000"); err != nil {
		t.Fatal(err)
	}
	var who string
	var n int
	if err := one.QueryRowContext(context.Background(), "SELECT MIN(side), COUNT(*) FROM ev WHERE dt >= NOW() - INTERVAL 1 DAY").Scan(&who, &n); err != nil {
		t.Fatal(err)
	}
	if who != "live" || n != 3 {
		t.Errorf("a frozen clock: answered by %s with %d rows, want MySQL's 3", who, n)
	}

	// A named zone: the copy takes the zone (a statement without the
	// four answers there), and a statement that calls one goes to MySQL,
	// since near a clock change a wall clock there stands for two instants.
	// Needs the source's time zone tables.
	var tz sql.NullString
	if err := srcDB.QueryRowContext(context.Background(), "SELECT CONVERT_TZ('2026-11-02 12:00:00', 'UTC', 'America/New_York')").Scan(&tz); err != nil || !tz.Valid {
		t.Errorf("named zone not compared: the source has no time zone tables (%v); load them (mysql_tzinfo_to_sql)", err)
	} else {
		named := openRaw(t, withZone(portDSN, "America/New_York"))
		if got := answerText(named, "SELECT MIN(side), COUNT(*) FROM ev"); !strings.HasPrefix(got, "copy|") {
			t.Errorf("[America/New_York] a statement without NOW(): %s, want the copy to answer it", got)
		}
		if got := answerText(named, "SELECT MIN(side), COUNT(*) FROM ev WHERE ts >= NOW() - INTERVAL 7 DAY"); !strings.HasPrefix(got, "live|") {
			t.Errorf("[America/New_York] NOW() under a named zone: %s, want MySQL to answer it", got)
		}
	}

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
	if reasons["copy_refused"] != 0 {
		t.Errorf("reasons = %v, want copy_refused 0", reasons)
	}
}
