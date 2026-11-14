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

	"github.com/go-mysql-org/go-mysql/client"
	drivermysql "github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/readrouter"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// #2810 against a real MySQL: the routing port forwards under the collation
// and character set the client's handshake asked for, as a connection to
// MySQL itself gets them. It forwarded under utf8mb4_0900_ai_ci whatever was
// asked: '10:47' < '1045' was true through the port and false directly under
// go-sql-driver's default (utf8mb4_general_ci), and a latin1 client read a
// stored é as C3 A9 instead of E9. The copy answers only a session whose
// collation compares as it does (utf8mb4_0900_ai_ci or utf8mb4_unicode_ci);
// on any other MySQL answers.
func TestIntegrationSQLCompareClientCollation(t *testing.T) {
	srcDB, srcName := testutil.CreateTestDB(t)
	clientCollation(t, srcDB, srcName, testutil.IntegrationDSN(srcName), false)
}

// The same against a MariaDB source, compared with MariaDB itself: what each
// collation maps to there is logged, not assumed. A test of its own, with
// MariaDB in its name: the MariaDB job picks its tests by name.
func TestIntegrationSQLCompareClientCollationMariaDB(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	srcDB, srcName := testutil.CreateTestMariaDB(t)
	clientCollation(t, srcDB, srcName, testutil.MariaDBBaseDSN()+"/"+srcName+"?parseTime=true", true)
}

// withCollation is dsn asking for collation in its handshake ("" leaves the
// driver's default, utf8mb4_general_ci).
func withCollation(dsn, collation string) string {
	if collation == "" {
		return dsn
	}
	sep := "?"
	if strings.Contains(dsn, "?") {
		sep = "&"
	}
	return dsn + sep + "collation=" + collation
}

func clientCollation(t *testing.T, srcDB *sql.DB, srcName, sourceDSN string, mariadb bool) {
	now := time.Now().UTC().Truncate(time.Second)
	indexDSN := seedFlashbackIndex(t, "alice", now.Truncate(time.Hour))
	var version string
	if err := srcDB.QueryRow("SELECT VERSION()").Scan(&version); err != nil {
		t.Fatal(err)
	}
	t.Logf("source: %s", version)

	// A row at 10:47 (the text '10:47' compared with '1045'), an é and an ß.
	rows := func(side string) [][]string {
		return [][]string{
			{"1", "1", side, "2026-01-01 10:47:00", "é"},
			{"2", "2", side, "2026-01-01 09:00:00", "ß"},
			{"3", "3", side, "2026-01-01 11:00:00", "b"},
		}
	}
	src := openRaw(t, sourceDSN)
	if _, err := src.Exec("CREATE TABLE ev (id INT NOT NULL PRIMARY KEY, k INT, side VARCHAR(8), dt DATETIME, s VARCHAR(8)) DEFAULT CHARSET=utf8mb4"); err != nil {
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

	const session = "SELECT @@collation_connection, @@character_set_client, @@character_set_connection, @@character_set_results"
	// Answered by MySQL on every connection: a system variable.
	sameEverywhere := []string{
		session,
		"SELECT '10:47' < '1045', 'ß' = 'ss', 'a' = 'á', @@version <> ''",
		"SELECT MIN(side), COUNT(*) FROM ev WHERE DATE_FORMAT(dt, '%H:%i') < '1045' AND @@version <> ''",
		"SELECT side, HEX(s), s FROM ev WHERE @@version <> '' ORDER BY k",
	}
	// What the copy answers on a session that compares as it does: ORDER BY
	// k, a column with no index, so the plan reads the whole table.
	// Not s: a letter with an accent there keeps any statement reading it on
	// MySQL.
	const copyRead = "SELECT side, dt FROM ev ORDER BY k"
	cases := []struct {
		collation string
		copy      bool // the copy answers copyRead
	}{
		{"", false}, // go-sql-driver's default: utf8mb4_general_ci
		{"utf8mb4_general_ci", false},
		{"utf8mb4_bin", false},
		{"latin1_swedish_ci", false},
		{"binary", false},
		{"utf8mb4_unicode_ci", true},
		{"utf8mb4_0900_ai_ci", !mariadb}, // MariaDB 10.11 has no such collation
	}
	for _, c := range cases {
		label := c.collation
		if label == "" {
			label = "(driver default)"
		}
		port := openRaw(t, withCollation(portDSN, c.collation))
		live := openRaw(t, withCollation(liveDSN, c.collation))
		for _, stmt := range sameEverywhere {
			got, want := answerText(port, stmt), answerText(live, stmt)
			t.Logf("%-20s port %-70.200s %s", label, got, stmt)
			if strings.HasPrefix(want, "ERROR ") {
				t.Errorf("%s: the source refused %q: %s", label, stmt, want)
				continue
			}
			if strings.ReplaceAll(got, "copy|", "live|") != want {
				t.Errorf("%s: %q through the port:\n  got  %s\n  want %s, the source's own answer", label, stmt, got, want)
			}
		}
		got, want := answerText(port, copyRead), answerText(live, copyRead)
		t.Logf("%-20s port %-70.200s %s", label, got, copyRead)
		who := "live"
		if strings.Contains(got, "copy|") {
			who = "copy"
		}
		if wantWho := map[bool]string{true: "copy", false: "live"}[c.copy]; who != wantWho && !(mariadb && c.collation == "utf8mb4_0900_ai_ci") {
			t.Errorf("%s: %q answered by %s, want %s", label, copyRead, who, wantWho)
		}
		if strings.ReplaceAll(got, "copy|", "live|") != want {
			t.Errorf("%s: %q through the port:\n  got  %s\n  want %s, the source's own answer", label, copyRead, got, want)
		}
	}

	// A SET NAMES of the client's own, later: the copy answers once the
	// session compares as it does, on the one connection.
	ctxT := context.Background()
	pconn, err := openRaw(t, portDSN).Conn(ctxT)
	if err != nil {
		t.Fatal(err)
	}
	defer pconn.Close()
	lconn, err := openRaw(t, liveDSN).Conn(ctxT)
	if err != nil {
		t.Fatal(err)
	}
	defer lconn.Close()
	setNames := "SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci"
	for _, cn := range []*sql.Conn{pconn, lconn} {
		if _, err := cn.ExecContext(ctxT, setNames); err != nil {
			t.Fatal(err)
		}
	}
	for _, stmt := range []string{session, copyRead} {
		got, want := connAnswer(t, pconn, stmt), connAnswer(t, lconn, stmt)
		t.Logf("after SET NAMES: port %-70.200s %s", got, stmt)
		if strings.ReplaceAll(got, "copy|", "live|") != want {
			t.Errorf("after %s, %q through the port:\n  got  %s\n  want %s", setNames, stmt, got, want)
		}
		if stmt == copyRead && !strings.Contains(got, "copy|") {
			t.Errorf("after %s, %q was not answered by the copy: %s", setNames, stmt, got)
		}
	}

	// COM_RESET_CONNECTION: the session goes back to what the handshake
	// asked for, on the port as on the source.
	srcAddr, srcUser, srcPass := sourceAddr(t, liveDSN)
	ends := []struct{ addr, user, pass string }{
		{ln.Addr().String(), ent.ID, "tok"},
		{srcAddr, srcUser, srcPass},
	}
	for _, coll := range []string{"latin1_swedish_ci", "utf8mb4_general_ci"} {
		var after [2]string
		for i, addr := range ends {
			c, err := client.Connect(addr.addr, addr.user, addr.pass, srcName, func(c *client.Conn) error { return c.SetCollation(coll) })
			if err != nil {
				t.Fatalf("connect (%s): %v", coll, err)
			}
			if _, err := c.Execute("SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci"); err != nil {
				t.Fatal(err)
			}
			if r := resetConnection(t, c); r.closed || r.code != 0 {
				t.Fatalf("COM_RESET_CONNECTION (%s): %+v", coll, r)
			}
			res, err := c.Execute(session)
			if err != nil {
				t.Fatal(err)
			}
			for j := 0; j < 4; j++ {
				v, _ := res.GetString(0, j)
				after[i] += v + "|"
			}
			c.Close()
		}
		t.Logf("after a reset, asked %s: port %s, source %s", coll, after[0], after[1])
		if after[0] != after[1] {
			t.Errorf("after COM_RESET_CONNECTION on a connection that asked for %s: the port's session is %s, the source's %s", coll, after[0], after[1])
		}
	}

	// A read-only port: a client that asks for gbk does not get it (its
	// screen reads a statement's bytes as utf8mb4 or latin1 would), so the
	// bytes BF 5C, a letter in gbk and a letter and a backslash to the
	// screen, cannot end a string the screen reads as going on. The
	// statement below is one string literal to the screen; under gbk it
	// would be a string and then a second column naming the character set.
	roLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	roCtx, roCancel := context.WithCancel(context.Background())
	roServed := make(chan struct{})
	go func() {
		_ = serveFlashback(roCtx, srv, roLn, flashbackConfig{RouteMaxCopyAge: time.Hour, RoutePolicy: readrouter.Policy{ScanRows: 2}, RouteReadOnly: true})
		close(roServed)
	}()
	t.Cleanup(func() { roCancel(); <-roServed })
	ro := openRaw(t, withCollation(fmt.Sprintf("%s:tok@tcp(%s)/%s", ent.ID, roLn.Addr().String(), srcName), "gbk_chinese_ci"))
	if got := answerText(ro, session); !strings.HasPrefix(got, "utf8mb4_0900_ai_ci|utf8mb4|") {
		t.Errorf("a gbk client on a read-only port: the session is %s, want utf8mb4_0900_ai_ci and utf8mb4", got)
	}
	hidden := "SELECT '\xbf\\' , @@character_set_client -- '"
	got := answerText(ro, hidden)
	t.Logf("read-only, gbk: %q answered %q", hidden, got)
	if strings.Contains(got, "gbk") || strings.Count(got, "|") > 0 {
		t.Errorf("a gbk client on a read-only port: %q was read as two columns by the source: %q", hidden, got)
	}
}

// connAnswer is answerText on one connection.
func connAnswer(t *testing.T, c *sql.Conn, stmt string) string {
	t.Helper()
	rows, err := c.QueryContext(context.Background(), stmt)
	if err != nil {
		return "ERROR " + err.Error()
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return "ERROR " + err.Error()
	}
	var out []string
	for rows.Next() {
		cells := make([]sql.RawBytes, len(cols))
		ptrs := make([]any, len(cols))
		for i := range cells {
			ptrs[i] = &cells[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return "ERROR " + err.Error()
		}
		parts := make([]string, len(cells))
		for i, b := range cells {
			parts[i] = string(b)
		}
		out = append(out, strings.Join(parts, "|"))
	}
	if err := rows.Err(); err != nil {
		return "ERROR " + err.Error()
	}
	return strings.Join(out, " / ")
}

// sourceAddr is the address, user and password of a source DSN, for a
// go-mysql client.
func sourceAddr(t *testing.T, dsn string) (string, string, string) {
	t.Helper()
	cfg, err := drivermysql.ParseDSN(dsn)
	if err != nil {
		t.Fatal(err)
	}
	return cfg.Addr, cfg.User, cfg.Passwd
}
