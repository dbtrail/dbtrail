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

// #2802 against a real MySQL: under routing, DATEDIFF and DATE_FORMAT with
// each white space byte MySQL takes before the parenthesis (a space and 0x09
// to 0x0D) are decided as they are with none. A vertical tab there sent
// DATEDIFF('2026-01-15 02:00:00+05:00', d) to the copy, which answered a day
// off MySQL (-45 where MySQL answers -46).
func TestIntegrationSQLCompareCallSpaceRouted(t *testing.T) {
	srcDB, srcName := testutil.CreateTestDB(t)
	callSpaceRouted(t, srcDB, srcName, testutil.IntegrationDSN(srcName))
}

// The same against a MariaDB source. A test of its own, with MariaDB in its
// name: the MariaDB job picks its tests by name.
func TestIntegrationSQLCompareCallSpaceRoutedMariaDB(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	srcDB, srcName := testutil.CreateTestMariaDB(t)
	callSpaceRouted(t, srcDB, srcName, testutil.MariaDBBaseDSN()+"/"+srcName+"?parseTime=true")
}

func callSpaceRouted(t *testing.T, srcDB *sql.DB, srcName, sourceDSN string) {
	now := time.Now().UTC().Truncate(time.Second)
	indexDSN := seedFlashbackIndex(t, "alice", now.Truncate(time.Hour))
	var version string
	if err := srcDB.QueryRow("SELECT VERSION()").Scan(&version); err != nil {
		t.Fatal(err)
	}
	t.Logf("source: %s", version)

	// The dates of the issue's table: a Sunday, a turn of the year, the last
	// days of a year and of a month.
	rows := func(side string) [][]string {
		return [][]string{
			{"1", "1", side, "2026-03-01", "2026-03-01 10:00:00", "2026-03-01"},
			{"2", "2", side, "2026-01-01", "2026-01-01 00:00:00", "2026-01-15 02:00:00+05:00"},
			{"3", "3", side, "2024-12-30", "2024-12-30 23:59:59", "20260115"},
			{"4", "4", side, "2026-03-31", "2026-03-31 23:30:00", "x"},
		}
	}
	src := openRaw(t, withZone(sourceDSN, "+00:00"))
	if _, err := src.Exec("CREATE TABLE ev (id INT NOT NULL PRIMARY KEY, k INT, side VARCHAR(8), d DATE, dt DATETIME, s VARCHAR(32))"); err != nil {
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
	portDSN := fmt.Sprintf("%s:tok@tcp(%s)/%s", ent.ID, ln.Addr().String(), srcName)
	liveDSN := strings.Replace(sourceDSN, "?parseTime=true", "", 1)

	// ORDER BY k: a column with no index, so the plan reads the whole table
	// and the copy is asked. {} is where the white space goes.
	cases := []struct{ expr, who string }{
		{"DATEDIFF{}('2026-01-15 02:00:00+05:00', d)", "live"}, // the issue's case
		{"DATEDIFF{}(d, '2026-01-15 22:00:00-05:00')", "live"},
		{"DATEDIFF{}(s, d)", "live"},
		{"DATE_FORMAT{}(dt + INTERVAL 1 DAY, '%Y-%m-%d')", "live"},
		{"DATE_FORMAT{}(s, '%Y-%m-%d')", "live"},
		{"DATEDIFF{}(dt, d)", "copy"},
		{"DATE_FORMAT{}(dt, '%Y-%m-%d %H')", "copy"},
	}
	seps := []struct{ name, sep string }{
		{"none", ""}, {"space", " "}, {"tab", "\t"}, {"line feed", "\n"},
		{"vertical tab", "\v"}, {"form feed", "\f"}, {"carriage return", "\r"},
	}
	for _, zone := range []string{"+00:00", "+05:00"} {
		routed := openRaw(t, withZone(portDSN, zone))
		live := openRaw(t, withZone(liveDSN, zone))
		for _, c := range cases {
			for _, sp := range seps {
				stmt := "SELECT side, " + strings.ReplaceAll(c.expr, "{}", sp.sep) + " FROM ev ORDER BY k"
				got := answerText(routed, stmt)
				want := answerText(live, stmt)
				t.Logf("[%s] %-16s routed %-70.200s %s", zone, sp.name, got, strings.ReplaceAll(c.expr, "{}", sp.sep))
				if strings.HasPrefix(want, "ERROR ") {
					t.Errorf("[%s] %s: the source refused %q: %s", zone, sp.name, stmt, want)
					continue
				}
				if !strings.Contains(got, c.who+"|") {
					t.Errorf("[%s] %s: routed %q:\n  got  %s\n  want it answered by %s", zone, sp.name, stmt, got, c.who)
					continue
				}
				if strings.ReplaceAll(got, "copy|", "live|") != want {
					t.Errorf("[%s] %s: routed %q:\n  got  %s\n  want %s, the source's own answer", zone, sp.name, stmt, got, want)
				}
			}
		}
	}
}
