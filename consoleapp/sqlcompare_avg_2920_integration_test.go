//go:build integration

package consoleapp

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/readrouter"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// #2920 against a real MySQL: under routing, an AVG of a whole-number column
// that is a whole column of the select list is answered by the copy with
// MySQL's text and type (a DECIMAL with 4 decimals: 2.3333, -1.5000, a half
// rounded away from zero, exact near 2^63 and 2^64, NULL over NULLs and over
// no row). Every other AVG stays on MySQL: over a DECIMAL, a DOUBLE, text, a
// date or an expression (the issue's AVG(LENGTH(c))), and one MySQL computes
// on more digits than it prints (in arithmetic, a comparison, HAVING, ORDER
// BY its alias or position, a window).
func TestIntegrationSQLCompareAvgRouted(t *testing.T) {
	srcDB, srcName := testutil.CreateTestDB(t)
	avgRouted(t, srcDB, srcName, testutil.IntegrationDSN(srcName))
}

// The same against a MariaDB source. A test of its own, with MariaDB in its
// name: the MariaDB job picks its tests by name.
func TestIntegrationSQLCompareAvgRoutedMariaDB(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	srcDB, srcName := testutil.CreateTestMariaDB(t)
	avgRouted(t, srcDB, srcName, testutil.MariaDBBaseDSN()+"/"+srcName+"?parseTime=true")
}

func avgRouted(t *testing.T, srcDB *sql.DB, srcName, sourceDSN string) {
	now := time.Now().UTC().Truncate(time.Second)
	indexDSN := seedFlashbackIndex(t, "alice", now.Truncate(time.Hour))
	var version string
	if err := srcDB.QueryRow("SELECT VERSION()").Scan(&version); err != nil {
		t.Fatal(err)
	}
	t.Logf("source: %s", version)

	// Groups: 1, 2 and 4 (2.3333); -1 and -2; NULLs only; one 1 among 32
	// rows (a half: 0.0313); one -1 among 32 (-0.0313). BIGINT and BIGINT
	// UNSIGNED at their limits in the first two groups.
	const n = `\N`
	rows := func(side string) [][]string {
		out := [][]string{
			{"1", "1", side, "1", "9223372036854775807", "18446744073709551615", "1", "1", "1.25", "0.1", "abc", "2026-01-01"},
			{"2", "1", side, "2", "9223372036854775807", "18446744073709551615", "2", "0", "2.50", "0.2", "abcd", "2026-01-03"},
			{"3", "1", side, "4", "9223372036854775806", "18446744073709551614", "3", "1", "-0.01", "0.3", "a", "2026-01-04"},
			{"4", "2", side, "-1", "-9223372036854775808", "0", "-1", n, "-1.25", n, "", n},
			{"5", "2", side, "-2", "-9223372036854775808", "0", "-2", n, "-2.50", n, n, n},
			{"6", "3", side, n, n, n, n, n, n, n, n, n},
		}
		id := 7
		for _, g := range []struct{ group, one string }{{"4", "1"}, {"5", "-1"}} {
			for i := 0; i < 32; i++ {
				k := "0"
				if i == 0 {
					k = g.one
				}
				out = append(out, []string{strconv.Itoa(id), g.group, side, k, n, n, n, n, n, n, n, n})
				id++
			}
		}
		return out
	}
	src := openRaw(t, withZone(sourceDSN, "+00:00"))
	const create = "CREATE TABLE ev (id INT NOT NULL PRIMARY KEY, g INT, side VARCHAR(8), k INT, b BIGINT, u BIGINT UNSIGNED, ti TINYINT, bo TINYINT(1), dc DECIMAL(10,2), dbl DOUBLE, s VARCHAR(20), dt DATE)"
	if _, err := src.Exec(create); err != nil {
		t.Fatal(err)
	}
	for _, r := range rows("live") {
		vals := make([]string, len(r))
		for i, v := range r {
			vals[i] = "'" + v + "'"
			if v == n {
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

	// A plain INT column such as g is typed by the copy as a BIGINT, and a CAST
	// goes to MySQL: neither is what this test is about, so g is only grouped
	// and ordered by, never selected.
	// who "" asserts the answer and not who gave it. The first column of
	// every row is side, so the answer says who gave it.
	cases := []struct {
		stmt string
		args []any
		who  string
		why  string
	}{
		{stmt: "SELECT MIN(side), AVG(k) FROM ev GROUP BY g ORDER BY g", who: "copy", why: "a third, a negative, NULLs only, a half each way"},
		{stmt: "SELECT MIN(side), AVG(b), AVG(u), AVG(ti), AVG(bo) FROM ev GROUP BY g ORDER BY g", who: "copy", why: "BIGINT and UNSIGNED at their limits, TINYINT, TINYINT(1)"},
		{stmt: "SELECT MIN(side), AVG(DISTINCT k), AVG(DISTINCT(ti)), AVG(k) FROM ev", who: "copy", why: "DISTINCT"},
		{stmt: "SELECT MIN(side), avg (k), Avg(`k`) AS `a`, AVG(ev.k) a2 FROM ev", who: "copy", why: "spellings"},
		{stmt: "SELECT MIN(side), COUNT(*), AVG(k) FROM ev GROUP BY g ORDER BY g", who: "copy", why: "the issue's shape over a column"},
		{stmt: "SELECT MIN(side), AVG(k) FROM ev GROUP BY g ORDER BY 1, g", who: "copy", why: "ORDER BY other positions"},
		{stmt: "SELECT MIN(side), AVG(k) FROM ev WHERE g = 3", who: "copy", why: "NULLs only"},
		{stmt: "SELECT MIN(side), AVG(k) FROM ev WHERE k > 100", who: "", why: "no row: nothing says who answered"},
		{stmt: "SELECT MIN(side), AVG(k) FROM ev WHERE g > ?", args: []any{0}, who: "copy", why: "prepared"},

		{stmt: "SELECT MIN(side), AVG(dc), AVG(dbl) FROM ev", who: "live", why: "a DECIMAL (scale 6) and a DOUBLE"},
		{stmt: "SELECT MIN(side), AVG(s), AVG(dt) FROM ev", who: "live", why: "text and a date"},
		{stmt: "SELECT MIN(side), AVG(LENGTH(s)) FROM ev", who: "live", why: "the issue's: an expression the router does not type"},
		{stmt: "SELECT MIN(side), g AS bucket, COUNT(*), AVG(LENGTH(s)) FROM ev GROUP BY bucket ORDER BY bucket", who: "live", why: "the issue's statement"},
		{stmt: "SELECT MIN(side), AVG(k) * 3, AVG(k) > 2.3333 FROM ev WHERE g = 1", who: "live", why: "arithmetic and a comparison on more digits than printed"},
		{stmt: "SELECT MIN(side), g, AVG(k) a FROM ev GROUP BY g HAVING a > 0.03 ORDER BY g", who: "live", why: "HAVING its alias"},
		{stmt: "SELECT MIN(side), g, AVG(k) a FROM ev GROUP BY g ORDER BY a, g", who: "live", why: "ORDER BY its alias"},
		{stmt: "SELECT MIN(side), g, AVG(k) FROM ev GROUP BY g ORDER BY 3, g", who: "live", why: "ORDER BY its position"},
		{stmt: "SELECT MIN(side), ROUND(AVG(k), 2) FROM ev", who: "live", why: "ROUND of an AVG"},
		{stmt: "SELECT side, AVG(k) OVER (PARTITION BY g) FROM ev WHERE g = 1 ORDER BY k", who: "live", why: "a window"},
		{stmt: "SELECT MIN(side), AVG(k) FROM ev WHERE k IN (SELECT k FROM ev)", who: "live", why: "a subquery"},
		{stmt: "SELECT MIN(side), AVG(?) FROM ev", args: []any{3}, who: "live", why: "a placeholder"},
	}
	asText := func(dsn string) string { return strings.Replace(dsn, "parseTime=true", "parseTime=false", 1) }
	routedText := openRaw(t, asText(withZone(portDSN, "+00:00")))
	liveText := openRaw(t, asText(withZone(sourceDSN, "+00:00")))
	for _, c := range cases {
		got := scaledAnswer(routedText, c.stmt, c.args...)
		want := scaledAnswer(liveText, c.stmt, c.args...)
		t.Logf("routed %-90.400s %s", got, c.stmt)
		t.Logf("live   %-90.400s", want)
		if strings.HasPrefix(got, "ERROR ") || (c.who != "" && !strings.Contains(got, "("+c.who+",")) {
			t.Errorf("routed %q (%s):\n  got  %s\n  want it answered by %s", c.stmt, c.why, got, c.who)
			continue
		}
		if strings.ReplaceAll(got, "(copy,", "(live,") != want {
			t.Errorf("routed %q (%s):\n  got  %s\n  want %s, the source's own answer", c.stmt, c.why, got, want)
		}
	}
}
