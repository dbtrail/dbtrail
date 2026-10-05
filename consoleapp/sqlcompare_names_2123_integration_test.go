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

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/readrouter"
	"github.com/dbtrail/dbtrail/internal/sqlcompare"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// #2123 against a real MySQL: the copy holds no generated column, and a
// statement that names one is answered there by whatever else answers to the
// name, with no error. See generatedNames.
func TestIntegrationSQLCompareGeneratedNames(t *testing.T) {
	srcDB, srcName := testutil.CreateTestDB(t)
	generatedNames(t, srcDB, srcName, testutil.IntegrationDSN(srcName))
}

// The same against a MariaDB source. A test of its own, with MariaDB in its
// name: the MariaDB job picks its tests by name, and a subtest of the test
// above would never run there.
func TestIntegrationSQLCompareGeneratedNamesMariaDB(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	srcDB, srcName := testutil.CreateTestMariaDB(t)
	generatedNames(t, srcDB, srcName, testutil.MariaDBBaseDSN()+"/"+srcName+"?parseTime=true")
}

// answerText is every row of a statement as text, rows joined by " / " and
// cells by "|", or "ERROR <message>".
func answerText(db *sql.DB, stmt string, args ...any) string {
	rows, err := db.Query(stmt, args...)
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
		for i, c := range cells {
			parts[i] = string(c)
		}
		out = append(out, strings.Join(parts, "|"))
	}
	if err := rows.Err(); err != nil {
		return "ERROR " + err.Error()
	}
	return strings.Join(out, " / ")
}

// generatedNames is the body of both tests. The tables are created on the
// source in plain DDL and the copy's files carry what that server's own SHOW
// CREATE TABLE prints, as a dump does, so the definition each server writes
// is the one the copy reads.
//
// Three parts: what the SOURCE answers (the measurement the issue asks for,
// pinned per server because both tests run this); what the copy answers when
// nobody asked for MySQL's answer (sql-compare, a port with no routing); and
// the same statements under read routing, where each must be the source's.
func generatedNames(t *testing.T, srcDB *sql.DB, srcName, sourceDSN string) {
	now := time.Now().UTC().Truncate(time.Hour)
	indexDSN := seedFlashbackIndex(t, "alice", now)

	var version string
	if err := srcDB.QueryRow("SELECT VERSION()").Scan(&version); err != nil {
		t.Fatal(err)
	}
	t.Logf("source: %s", version)

	type table struct {
		name, create string
		insert       string     // the stored columns
		rows         [][]string // the source's rows, stored columns in declared order
		copyRows     [][]string // nil = the same
	}
	tables := []table{
		{name: "gen", create: "CREATE TABLE gen (id INT NOT NULL PRIMARY KEY, twice INT GENERATED ALWAYS AS (id * 2) STORED, a INT)",
			insert: "(id, a)", rows: [][]string{{"1", "5"}, {"2", "2"}, {"3", "7"}}},
		{name: "g2", create: "CREATE TABLE g2 (id INT NOT NULL PRIMARY KEY, twice INT, b INT)",
			insert: "(id, twice, b)", rows: [][]string{{"1", "99", "7"}, {"2", "99", "8"}, {"3", "99", "9"}}},
		// A VIRTUAL column, under a name the copy calls as a function.
		{name: "kw", create: "CREATE TABLE kw (id INT NOT NULL PRIMARY KEY, user VARCHAR(8) GENERATED ALWAYS AS (concat('u', id)) VIRTUAL, a INT)",
			insert: "(id, a)", rows: [][]string{{"1", "5"}, {"2", "6"}, {"3", "7"}}},
		// An invisible column is held by both sides, and both resolve its
		// name the same way.
		{name: "invis", create: "CREATE TABLE invis (id INT NOT NULL PRIMARY KEY, secret INT INVISIBLE DEFAULT NULL, a INT)",
			insert: "(id, secret, a)", rows: [][]string{{"1", "9", "5"}, {"2", "9", "6"}, {"3", "9", "7"}}},
		{name: "inv2", create: "CREATE TABLE inv2 (id INT NOT NULL PRIMARY KEY, secret INT, b INT)",
			insert: "(id, secret, b)", rows: [][]string{{"1", "6", "7"}, {"2", "6", "8"}, {"3", "6", "9"}}},
		// For the routed part: the source's rows say "live", the copy's "copy".
		{name: "who_gen", create: "CREATE TABLE who_gen (id INT NOT NULL PRIMARY KEY, twice INT GENERATED ALWAYS AS (id * 2) STORED, side VARCHAR(8))",
			insert: "(id, side)", rows: [][]string{{"1", "live"}, {"2", "live"}, {"3", "live"}},
			copyRows: [][]string{{"1", "copy"}, {"2", "copy"}, {"3", "copy"}}},
	}
	var order []orderTable
	for _, tb := range tables {
		if _, err := srcDB.Exec(tb.create); err != nil {
			t.Fatalf("%s: %v", tb.create, err)
		}
		for _, r := range tb.rows {
			if _, err := srcDB.Exec("INSERT INTO " + tb.name + " " + tb.insert + " VALUES ('" + strings.Join(r, "','") + "')"); err != nil {
				t.Fatalf("insert into %s: %v", tb.name, err)
			}
		}
		if _, err := srcDB.Exec("ANALYZE TABLE " + tb.name); err != nil {
			t.Fatal(err)
		}
		var name, ddl string
		if err := srcDB.QueryRow("SHOW CREATE TABLE "+tb.name).Scan(&name, &ddl); err != nil {
			t.Fatalf("SHOW CREATE TABLE %s: %v", tb.name, err)
		}
		order = append(order, orderTable{name: tb.name, ddl: ddl + ";\n", rows: tb.rows, copyRows: tb.copyRows, footer: true})
	}
	baseDir := t.TempDir()
	writeOrderSnapshot(t, baseDir, srcName, order)

	// A generation expression whose string ends in a backslash, as each
	// server prints it under both readings of the backslash: the column must
	// be read as generated either way, or the conversion counts one column
	// too many.
	for _, mode := range []string{"", "NO_BACKSLASH_ESCAPES"} {
		conn, err := srcDB.Conn(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		tbl := "bs" + fmt.Sprint(len(mode))
		// Under NO_BACKSLASH_ESCAPES one backslash is one backslash; without
		// it, two are.
		lit := `'\\'`
		if mode != "" {
			lit = `'\'`
		}
		var name, ddl string
		for _, q := range []string{
			"SET SESSION sql_mode = '" + mode + "'",
			"CREATE TABLE " + tbl + " (id INT NOT NULL PRIMARY KEY, a VARCHAR(8), p VARCHAR(9) GENERATED ALWAYS AS (concat(a, " + lit + ")) STORED, z INT)",
		} {
			if _, err := conn.ExecContext(context.Background(), q); err != nil {
				t.Fatalf("%s: %v", q, err)
			}
		}
		if err := conn.QueryRowContext(context.Background(), "SHOW CREATE TABLE "+tbl).Scan(&name, &ddl); err != nil {
			t.Fatal(err)
		}
		conn.Close()
		for _, line := range strings.Split(ddl, "\n") {
			if strings.Contains(line, "`p`") {
				t.Logf("sql_mode=%q prints: %s", mode, strings.TrimSpace(line))
			}
		}
		cols, err := baseline.ParseSchemaText(ddl + ";\n")
		if err != nil {
			t.Fatal(err)
		}
		var names []string
		for _, c := range cols {
			names = append(names, c.Name)
		}
		if got := strings.Join(names, ","); got != "id,a,z" {
			t.Errorf("sql_mode=%q: the stored columns read from\n%s\nare %s, want id,a,z (p is generated)", mode, ddl, got)
		}
	}

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
		scalar   = "SELECT o.id, (SELECT twice FROM gen g WHERE g.id = o.id) AS t FROM g2 o ORDER BY o.b"
		exists   = "SELECT o.id FROM g2 o WHERE EXISTS (SELECT 1 FROM gen g WHERE g.id = o.id AND twice = 2) ORDER BY o.b"
		join     = "SELECT twice FROM g2 JOIN gen ON g2.id = gen.id"
		alias    = "SELECT a AS twice FROM gen WHERE twice = 2 ORDER BY a"
		keyword  = "SELECT user FROM kw ORDER BY a"
		single   = "SELECT twice FROM gen ORDER BY a"
		orderBy  = "SELECT id FROM gen ORDER BY twice DESC"
		invInner = "SELECT o.id, (SELECT secret FROM invis i WHERE i.id = o.id) AS s FROM inv2 o ORDER BY o.b"
		invJoin  = "SELECT secret FROM invis i JOIN inv2 j ON j.id = i.id"
	)

	// 1. The source's answers. gen.twice is 2, 4, 6; g2.twice is 99.
	src := openRaw(t, sourceDSN)
	for _, c := range []struct{ stmt, want string }{
		{scalar, "1|2 / 2|4 / 3|6"},
		{exists, "1"},
		{join, "ERROR ambiguous"},
		{alias, "5"}, // the row whose twice is 2, not the one whose a is 2
		{keyword, "u1 / u2 / u3"},
		{single, "4 / 2 / 6"},
		{orderBy, "3 / 2 / 1"},
		{invInner, "1|9 / 2|9 / 3|9"}, // the inner table's invisible column, not the outer one's 6
		{invJoin, "ERROR ambiguous"},
	} {
		got := answerText(src, c.stmt)
		t.Logf("source  %-40s %s", got, c.stmt)
		if strings.HasPrefix(c.want, "ERROR ") {
			if !strings.HasPrefix(got, "ERROR ") || !strings.Contains(got, strings.TrimPrefix(c.want, "ERROR ")) {
				t.Errorf("source: %s\n  got  %s\n  want an error saying %q", c.stmt, got, strings.TrimPrefix(c.want, "ERROR "))
			}
			continue
		}
		if got != c.want {
			t.Errorf("source: %s\n  got  %s\n  want %s", c.stmt, got, c.want)
		}
	}

	// 2. The copy, asked as a port with no routing answers: other rows with
	// no error, which is the bug's shape.
	copyAddr := serve(flashbackConfig{})
	copies := [2]string{
		fmt.Sprintf("%s:tok@tcp(%s)/%s", ent.ID, copyAddr, srcName),
		fmt.Sprintf("%s:tok@tcp(%s)/%s", entB.ID, copyAddr, srcName),
	}
	eq, diff := sqlcompare.Equal, sqlcompare.Different
	fixtures := []valuesFixture{
		{scalar, diff, "", "the copy answers with g2.twice: 99 for each row"},
		{exists, diff, "", "the copy reads g2.twice = 2: no row"},
		{join, sqlcompare.SourceError, "", "ambiguous on the source; the copy would answer 99"},
		{alias, diff, "", "the copy reads the alias: the row whose a is 2"},
		{keyword, diff, "", "the copy calls the function user"},
		{single, sqlcompare.NotOnCopy, "", "nothing else answers to the name: the copy fails"},
		{invInner, eq, "", "an invisible column is held and resolved the same on both"},
		{invJoin, sqlcompare.SourceError, "", "ambiguous on both"},
	}
	by := compareInHalves(t, sourceDSN, copies, policy, fixtures)
	checkValuesFixtures(t, sourceDSN, copies[0], fixtures, by)

	// 3. The same server with read routing on. Every table has three rows and
	// the scan rule is at two, so each of these plans is the copy's unless
	// something keeps the statement on the source.
	routed := openFlashback(t, serve(flashbackConfig{RouteMaxCopyAge: time.Hour, RoutePolicy: policy}), ent.ID, "tok", srcName)
	defer routed.Close()
	for _, c := range []struct {
		stmt, want, why string
		args            []any
	}{
		{stmt: scalar, want: "1|2 / 2|4 / 3|6", why: "names gen's generated column: the source answers"},
		{stmt: exists, want: "1", why: "names it in a subquery's WHERE"},
		{stmt: join, want: "ERROR ambiguous", why: "the source cannot explain it, so the source answers, with its error"},
		{stmt: alias, want: "5", why: "one table: the name would be the alias on the copy"},
		{stmt: keyword, want: "u1 / u2 / u3", why: "one table: the name would be a function on the copy"},
		{stmt: single, want: "4 / 2 / 6", why: "one table, nothing else of that name: kept off the copy before it fails there"},
		{stmt: orderBy, want: "3 / 2 / 1", why: "named only in ORDER BY"},
		{stmt: "SELECT o.id, (SELECT twice FROM gen g WHERE g.id = o.id) AS t FROM g2 o WHERE o.b > ? ORDER BY o.b", args: []any{7},
			want: "2|4 / 3|6", why: "as a prepared statement"},
		// The rule is the name, not the company: these read the same tables.
		{stmt: "SELECT g.side, o.b FROM who_gen g JOIN g2 o ON o.id = g.id ORDER BY o.b", want: "copy|7 / copy|8 / copy|9",
			why: "a table with a generated column beside another, the column not named: the copy answers"},
		{stmt: "SELECT side FROM who_gen ORDER BY side, id", want: "copy / copy / copy", why: "alone, the column not named: the copy answers"},
		{stmt: "SELECT g.side FROM who_gen g JOIN g2 o ON o.id = g.id WHERE o.twice = 99 ORDER BY o.b", want: "live / live / live",
			why: "the name, qualified with the other table: still the source's, the text is searched and not parsed"},
	} {
		got := answerText(routed, c.stmt, c.args...)
		t.Logf("routed  %-40s %s", got, c.stmt)
		if strings.HasPrefix(c.want, "ERROR ") {
			if !strings.HasPrefix(got, "ERROR ") || !strings.Contains(got, strings.TrimPrefix(c.want, "ERROR ")) {
				t.Errorf("routed %q (%s):\n  got  %s\n  want an error saying %q", c.stmt, c.why, got, strings.TrimPrefix(c.want, "ERROR "))
			}
			continue
		}
		if got != c.want {
			t.Errorf("routed %q (%s):\n  got  %s\n  want %s", c.stmt, c.why, got, c.want)
		}
	}

	// Who answered, and why the source: eight statements declined by the copy
	// for their names, under the reason a star declined for its columns has;
	// the ambiguous one never got that far; two served by the copy; and the
	// copy at fault in none.
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
	if reasons["copy_columns_differ"] != 8 || reasons["copy_refused"] != 0 || reasons["explain_failed"] != 1 || reasons["expensive_plan"] != 2 {
		t.Errorf("reasons = %v, want copy_columns_differ 8, copy_refused 0, explain_failed 1, expensive_plan 2", reasons)
	}
}
