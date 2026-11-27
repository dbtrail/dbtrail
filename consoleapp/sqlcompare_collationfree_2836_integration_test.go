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

// #2836 against a real MySQL: a client on utf8mb4_general_ci (go-sql-driver's
// default, and what ProxySQL passes on for it) is answered by the copy on a
// statement that collation cannot change (no string literal, no function
// that returns text), with MySQL's answer, and by MySQL on the others.
func TestIntegrationSQLCompareCollationFree(t *testing.T) {
	srcDB, srcName := testutil.CreateTestDB(t)
	collationFree(t, srcDB, srcName, testutil.IntegrationDSN(srcName))
}

// The same against a MariaDB source. A test of its own, with MariaDB in its
// name: the MariaDB job picks its tests by name.
func TestIntegrationSQLCompareCollationFreeMariaDB(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	srcDB, srcName := testutil.CreateTestMariaDB(t)
	collationFree(t, srcDB, srcName, testutil.MariaDBBaseDSN()+"/"+srcName+"?parseTime=true")
}

func collationFree(t *testing.T, srcDB *sql.DB, srcName, sourceDSN string) {
	now := time.Now().UTC().Truncate(time.Second)
	indexDSN := seedFlashbackIndex(t, "alice", now.Truncate(time.Hour))
	var version string
	if err := srcDB.QueryRow("SELECT VERSION()").Scan(&version); err != nil {
		t.Fatal(err)
	}
	t.Logf("source: %s", version)

	// Text in a column of the table's collation and one of another; the
	// statements that compare literals ('10:47' < '1045' is false under
	// utf8mb4_general_ci and true under utf8mb4_0900_ai_ci) stay on MySQL.
	// ASCII letters and digits only: the copy declines a column it sorts
	// with accents or signs in it, whatever the session.
	rows := func(side string) [][]string {
		return [][]string{
			{"1", "1", side, "a", "a", "1", "2026-01-01"},
			{"2", "2", side, "A", "A", "2", "2026-01-02"},
			{"3", "3", side, "c", "c", "3", "2026-01-03"},
			{"4", "4", side, "1045", "1045", "4", "2026-01-04"},
			{"5", "5", side, "b", "b", "5", "2026-01-05"},
			{"6", "6", side, "ss", "ss", "6", "2026-01-06"},
		}
	}
	if _, err := srcDB.Exec("CREATE TABLE ev (id INT NOT NULL PRIMARY KEY, k INT, side VARCHAR(8), s VARCHAR(16), b VARCHAR(16) COLLATE utf8mb4_bin, n INT, d DATE)"); err != nil {
		t.Fatal(err)
	}
	for _, r := range rows("live") {
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
	// utf8mb4_general_ci on both sides, named so testCollation stays out.
	general := "collation=utf8mb4_general_ci"
	port := openRaw(t, fmt.Sprintf("%s:tok@tcp(%s)/%s?%s", ent.ID, ln.Addr().String(), srcName, general))
	live := openRaw(t, strings.Replace(sourceDSN, "parseTime=true", general, 1))

	// ORDER BY k and WHERE k: a column with no index, so the plan reads the
	// whole table and the copy is asked.
	cases := []struct{ stmt, who string }{
		{"SELECT side, s FROM ev ORDER BY k", "copy"},
		{"SELECT MIN(side), COUNT(*) FROM ev WHERE k > 0", "copy"},
		{"SELECT MIN(side), k, COUNT(*) FROM ev GROUP BY k ORDER BY k", "copy"},
		{"SELECT MIN(side), s, COUNT(*) FROM ev WHERE k > 0 GROUP BY s ORDER BY MIN(k)", "copy"},
		{"SELECT MIN(side), MIN(s), MAX(s), MIN(b), MAX(b) FROM ev WHERE k > 0", "copy"},
		{"SELECT side, s FROM ev WHERE k > 0 ORDER BY s, k", "copy"},
		{"SELECT side, b FROM ev WHERE k > 0 ORDER BY b, k", "copy"},
		{"SELECT side, n FROM ev WHERE n IN (1, 3) AND k > 0 ORDER BY k", "copy"},
		{"SELECT side, YEAR(d), MONTH(d) FROM ev WHERE k > 0 ORDER BY k", "copy"},

		{"SELECT side, '10:47' < '1045' FROM ev WHERE k > 0 ORDER BY k", "live"},
		{"SELECT side, s FROM ev WHERE k > 0 AND s > '1045' ORDER BY k", "live"},
		{"SELECT side, CONCAT(n) FROM ev WHERE k > 0 ORDER BY k", "live"},
		{"SELECT side, LOWER(s) AS x FROM ev WHERE k > 0 ORDER BY x, k", "live"},
		{"SELECT side, d FROM ev WHERE k > 0 AND d > '2026-01-03' ORDER BY k", "live"},
	}
	// MariaDB 11.4 and later turn a utf8mb4_general_ci handshake into
	// utf8mb4_uca1400_ai_ci, which compares as the copy does: there the
	// collation keeps none of these on MySQL (other checks still may, so who
	// answers the second group is not asserted), and each is still the
	// source's answer.
	var sessionCollation string
	if err := live.QueryRow("SELECT @@collation_connection").Scan(&sessionCollation); err != nil {
		t.Fatal(err)
	}
	t.Logf("the source's session: %s", sessionCollation)
	reproduced := sessionCollation == "utf8mb4_uca1400_ai_ci" || sessionCollation == "utf8mb4_0900_ai_ci" || sessionCollation == "utf8mb4_unicode_ci"
	for _, c := range cases {
		anyone := reproduced && c.who == "live"
		mark := heavyMark(t, srv, ent.ID)
		got, want := answerText(port, c.stmt), answerText(live, c.stmt)
		t.Logf("%-5s port %-70.200s %s", c.who, got, c.stmt)
		if heavyOnCopy(t, mark(), c.who, c.stmt, got, want) {
			continue
		}
		if strings.HasPrefix(want, "ERROR ") {
			t.Errorf("the source refused %q: %s", c.stmt, want)
			continue
		}
		if !anyone && !strings.Contains(got, c.who+"|") {
			t.Errorf("%q answered %s, want it answered by %s", c.stmt, got, c.who)
			continue
		}
		if strings.ReplaceAll(got, "copy|", "live|") != want {
			t.Errorf("%q through the port:\n  got  %s\n  want %s, the source's own answer", c.stmt, got, want)
		}
	}
}
