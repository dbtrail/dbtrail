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

// #2641 against a real MySQL: ROUND over a DECIMAL and SUBSTRING from a
// positive position are answered by the copy with the source's answer;
// ROUND over a DOUBLE or a FLOAT, ROUND of an AVG, and SUBSTRING from 0, from
// a negative position other than -1 or with a negative length stay on the
// source. The side column says who answered: live is MySQL, copy the copy.
func TestIntegrationSQLCompareRoundSubstring(t *testing.T) {
	srcDB, srcName := testutil.CreateTestDB(t)
	sourceDSN := testutil.IntegrationDSN(srcName)
	indexDSN := seedFlashbackIndex(t, "alice", time.Now().UTC().Truncate(time.Hour))
	var version string
	if err := srcDB.QueryRow("SELECT VERSION()").Scan(&version); err != nil {
		t.Fatal(err)
	}
	t.Logf("source: %s", version)

	// The halves the two engines round apart over a DOUBLE: 2.5, -2.5,
	// 0.125 and 0.5.
	rows := func(side string) [][]string {
		return [][]string{
			{"1", "1", side, "2.5", "2.5", "2.675", "abc"},
			{"2", "1", side, "-2.5", "-2.5", "-2.675", "a"},
			{"3", "2", side, "0.125", "0.125", "0.125", ""},
			{"4", "2", side, "0.5", "0.5", "0.500", "hello"},
		}
	}
	src := openRaw(t, sourceDSN)
	const create = "CREATE TABLE nums (id INT NOT NULL PRIMARY KEY, k INT, side VARCHAR(8), d DOUBLE, f FLOAT, m DECIMAL(10,3), name VARCHAR(10))"
	if _, err := src.Exec(create); err != nil {
		t.Fatal(err)
	}
	for _, r := range rows("live") {
		if _, err := src.Exec("INSERT INTO nums VALUES ('" + strings.Join(r, "','") + "')"); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := src.Exec("ANALYZE TABLE nums"); err != nil {
		t.Fatal(err)
	}
	var name, ddl string
	if err := src.QueryRow("SHOW CREATE TABLE nums").Scan(&name, &ddl); err != nil {
		t.Fatal(err)
	}
	baseDir := t.TempDir()
	writeOrderSnapshot(t, baseDir, srcName, []orderTable{{name: "nums", ddl: ddl + ";\n", rows: rows("live"), copyRows: rows("copy"), footer: true}})

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
		{"SELECT MIN(side), ROUND(SUM(m), 2) FROM nums", "copy", "ROUND of a DECIMAL sum"},
		{"SELECT MIN(side), k, round(sum(m), 2) AS revenue FROM nums GROUP BY k ORDER BY revenue DESC", "copy", "the README's shape"},
		{"SELECT side, ROUND(m, 2), ROUND(m), ROUND(m, -1) FROM nums ORDER BY m", "copy", "ROUND of a DECIMAL column"},
		{"SELECT side, ROUND(k, -1), ROUND(2.5) FROM nums ORDER BY m", "copy", "ROUND of an INT and of a decimal literal"},
		{"SELECT side, ROUND(d) FROM nums ORDER BY m", "live", "ROUND of a DOUBLE: 2 on MySQL, 3 on the copy"},
		{"SELECT side, ROUND(d, 2) FROM nums ORDER BY m", "live", "ROUND(0.125, 2) of a DOUBLE"},
		{"SELECT side, ROUND(f) FROM nums ORDER BY m", "live", "ROUND of a FLOAT"},
		{"SELECT MIN(side), ROUND(AVG(m), 2) FROM nums", "live", "AVG is a DOUBLE on the copy"},
		{"SELECT side, ROUND(m * 1.5, 1) FROM nums ORDER BY m", "live", "an expression"},
		{"SELECT side, SUBSTRING(name, 2, 2), SUBSTR(name, 1), SUBSTRING(name, 4) FROM nums ORDER BY m", "copy", "positive positions, past the end too"},
		// The copy has no MID: it refuses the statement, and MySQL answers.
		{"SELECT side, MID(name, 2, 2) FROM nums ORDER BY m", "live", "MID, which the copy does not have"},
		{"SELECT side, SUBSTRING(name, -1), SUBSTRING(name FROM 2 FOR 0) FROM nums ORDER BY m", "copy", "-1 and a length of 0"},
		{"SELECT side, SUBSTRING(name, 0, 2) FROM nums ORDER BY m", "live", "position 0"},
		{"SELECT side, SUBSTRING(name, -3, 2) FROM nums ORDER BY m", "live", "a negative position past the start of 'a'"},
		{"SELECT side, SUBSTRING(name FROM 2 FOR -1) FROM nums ORDER BY m", "live", "a negative length"},
		{"SELECT side, SUBSTRING(name, k) FROM nums ORDER BY m", "live", "a position from a column"},
	}
	routed := openRaw(t, portDSN)
	live := openRaw(t, sourceDSN)
	for _, c := range cases {
		got := answerText(routed, c.stmt)
		t.Logf("routed %-50s %s", got, c.stmt)
		if strings.HasPrefix(got, "ERROR ") || !strings.HasPrefix(got, c.who+"|") {
			t.Errorf("routed %q (%s):\n  got  %s\n  want it answered by %s", c.stmt, c.why, got, c.who)
			continue
		}
		want := answerText(live, c.stmt)
		if strings.ReplaceAll(got, "copy|", "live|") != want {
			t.Errorf("routed %q (%s):\n  got  %s\n  want %s, the source's own answer", c.stmt, c.why, got, want)
		}
	}
}
