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

// #2652 against a real MySQL: LEFT and RIGHT with a length of 0 or more
// written in the statement are answered by the copy with the source's
// answer; a negative length, or one from a column, stays on the source. So
// does a statement with a no-break space outside its strings: in a column
// named a<U+00A0>b, SELECT a<U+00A0>b is that column on MySQL and column a
// under the alias b on the copy, another value with no error on either side.
// The side column says who answered: live is MySQL, copy the copy.
func TestIntegrationSQLCompareLeftRight(t *testing.T) {
	srcDB, srcName := testutil.CreateTestDB(t)
	sourceDSN := testutil.IntegrationDSN(srcName)
	indexDSN := seedFlashbackIndex(t, "alice", time.Now().UTC().Truncate(time.Hour))
	var version string
	if err := srcDB.QueryRow("SELECT VERSION()").Scan(&version); err != nil {
		t.Fatal(err)
	}
	t.Logf("source: %s", version)

	rows := func(side string) [][]string {
		return [][]string{
			{"1", "-1", side, "abc", "10", "20", "30"},
			// Printable ASCII only: a text outside it sends every statement
			// that names the column to MySQL already (#2533).
			{"2", "2", side, "hello", "11", "21", "31"},
			{"3", "0", side, "", "12", "22", "32"},
		}
	}
	src := openRaw(t, sourceDSN)
	const create = "CREATE TABLE words (id INT NOT NULL PRIMARY KEY, n INT, side VARCHAR(8), name VARCHAR(10) CHARACTER SET utf8mb4, a INT, b INT, `a\u00a0b` INT)"
	if _, err := src.Exec(create); err != nil {
		t.Fatal(err)
	}
	for _, r := range rows("live") {
		if _, err := src.Exec("INSERT INTO words VALUES ('" + strings.Join(r, "','") + "')"); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := src.Exec("ANALYZE TABLE words"); err != nil {
		t.Fatal(err)
	}
	var name, ddl string
	if err := src.QueryRow("SHOW CREATE TABLE words").Scan(&name, &ddl); err != nil {
		t.Fatal(err)
	}
	baseDir := t.TempDir()
	writeOrderSnapshot(t, baseDir, srcName, []orderTable{{name: "words", ddl: ddl + ";\n", rows: rows("live"), copyRows: rows("copy"), footer: true}})

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
		{"SELECT side, LEFT(name, 2), RIGHT(name, 2) FROM words ORDER BY n", "copy", "a length written in the statement"},
		{"SELECT side, LEFT(name, 0), RIGHT(name, 100) FROM words ORDER BY n", "copy", "0 and a length past the end"},
		{"SELECT side, LEFT(name, -1) FROM words ORDER BY n", "live", "a negative length: '' on MySQL, 'ab' on the copy"},
		{"SELECT side, RIGHT(name, -1) FROM words ORDER BY n", "live", "a negative length: '' on MySQL, 'bc' on the copy"},
		{"SELECT side, LEFT(name, n) FROM words ORDER BY n", "live", "a length from a column, -1 on one row"},
		{"SELECT side, a, b FROM words ORDER BY n", "copy", "the two columns, apart"},
		{"SELECT side, a\u00a0b FROM words ORDER BY n", "live", "a no-break space inside a name: 30 on MySQL, 10 under the alias b on the copy"},
	}
	routed := openRaw(t, portDSN)
	live := openRaw(t, sourceDSN)
	for _, c := range cases {
		got := answerText(routed, c.stmt)
		want := answerText(live, c.stmt)
		t.Logf("routed %-50s %q", got, c.stmt)
		if strings.HasPrefix(got, "ERROR ") || !strings.HasPrefix(got, c.who+"|") {
			t.Errorf("routed %q (%s):\n  got  %s\n  want it answered by %s", c.stmt, c.why, got, c.who)
			continue
		}
		if strings.ReplaceAll(got, "copy|", "live|") != want {
			t.Errorf("routed %q (%s):\n  got  %s\n  want %s, the source's own answer", c.stmt, c.why, got, want)
		}
	}
}
