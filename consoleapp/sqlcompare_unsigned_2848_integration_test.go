//go:build integration

package consoleapp

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/readrouter"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// #2848 against a real MySQL: under routing, arithmetic over an UNSIGNED
// whole-number column, or over ROW_NUMBER, RANK, DENSE_RANK or NTILE, is
// answered by MySQL. MySQL types b - u (BIGINT minus BIGINT UNSIGNED) as a
// BIGINT UNSIGNED and refuses a row whose result is negative (error 1690);
// the copy computes it as a 128-bit integer and answers the negative number,
// answers ui - 4 over an INT UNSIGNED holding 3 as -1, and -u as
// 18446744073709551613. A comparison with a negative number, an UNSIGNED
// DECIMAL or DOUBLE in arithmetic, and SUM or COUNT of an UNSIGNED column
// stay on the copy, with MySQL's answer.
func TestIntegrationSQLCompareUnsignedMathRouted(t *testing.T) {
	srcDB, srcName := testutil.CreateTestDB(t)
	unsignedMathRouted(t, srcDB, srcName, testutil.IntegrationDSN(srcName))
}

// The same against a MariaDB source. A test of its own, with MariaDB in its
// name: the MariaDB job picks its tests by name.
func TestIntegrationSQLCompareUnsignedMathRoutedMariaDB(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	srcDB, srcName := testutil.CreateTestMariaDB(t)
	unsignedMathRouted(t, srcDB, srcName, testutil.MariaDBBaseDSN()+"/"+srcName+"?parseTime=true")
}

func unsignedMathRouted(t *testing.T, srcDB *sql.DB, srcName, sourceDSN string) {
	now := time.Now().UTC().Truncate(time.Second)
	indexDSN := seedFlashbackIndex(t, "alice", now.Truncate(time.Hour))
	var version string
	if err := srcDB.QueryRow("SELECT VERSION()").Scan(&version); err != nil {
		t.Fatal(err)
	}
	t.Logf("source: %s", version)

	// Signed and unsigned whole numbers of several widths, at their limits
	// on two rows, an unsigned DECIMAL and DOUBLE, and a row of NULLs.
	rows := func(side string) [][]string {
		return [][]string{
			{"1", "1", side, "9223372036854775807", "18446744073709551615", "4294967295", "255", "1.50", "0.5", "2026-01-01 10:00:00"},
			{"2", "2", side, "-9223372036854775808", "0", "0", "0", "0.00", "0", "2026-01-01 10:00:00"},
			{"3", "3", side, "3", "3", "3", "3", "3.00", "3", "2026-01-01 10:00:00"},
			{"4", "-4", side, "-4", "4", "4", "4", "4.25", "4", "2026-01-01 10:00:00"},
			{"5", "5", side, "5", "5", "5", "5", "5.00", "5", "2026-01-01 10:00:00"},
			{"6", "6", side, "6", "6", "6", "6", "6.00", "6", "2026-01-01 10:00:00"},
			{"7", "7", side, "7", "7", "7", "7", "7.00", "7", "2026-01-01 10:00:00"},
			{"8", `\N`, side, `\N`, `\N`, `\N`, `\N`, `\N`, `\N`, `\N`},
		}
	}
	src := openRaw(t, withZone(sourceDSN, "+00:00"))
	const create = "CREATE TABLE ev (id INT NOT NULL PRIMARY KEY, k INT, side VARCHAR(8), b BIGINT, u BIGINT UNSIGNED, ui INT UNSIGNED, " +
		"ti TINYINT UNSIGNED, dcu DECIMAL(10,2) UNSIGNED, dbu DOUBLE UNSIGNED, dt DATETIME)"
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

	// who "?" is measured and logged, not asserted; "" asserts the answer
	// and not who gave it. A statement MySQL refuses is asserted refused
	// through the port with MySQL's own error. Rows: id 4 has b = -4 and
	// u = 4, and k = -4; id 1 holds each column's largest value, id 2 the
	// smallest BIGINT and zeros.
	type sqlCase struct {
		stmt string
		args []any
		who  string
		why  string
	}
	positive := "WHERE k > 2 ORDER BY k"
	cases := []sqlCase{
		{stmt: "SELECT side, b - u FROM ev " + positive, who: "live", why: "the issue's case"},
		{stmt: "SELECT side, b + u, u + b, u * b, b * u FROM ev " + positive, who: "live", why: "signed and unsigned, + and *"},
		{stmt: "SELECT side, u - b, u - k, ui - k, k - ui FROM ev WHERE k = 7", who: "live", why: "differences that stay positive"},
		{stmt: "SELECT side, u + 1, u - 1, u * 2, 1 + u FROM ev " + positive, who: "live", why: "unsigned and a literal"},
		{stmt: "SELECT side, ui + 1, ui - 1, ui * 2, ui + k FROM ev " + positive, who: "live", why: "INT UNSIGNED"},
		{stmt: "SELECT side, ti + 1, ti - 1, ti * ti FROM ev " + positive, who: "live", why: "TINYINT UNSIGNED"},
		{stmt: "SELECT side, u % 4, ui % k, MOD(u, 4), u MOD 4 FROM ev " + positive, who: "live", why: "modulo"},
		{stmt: "SELECT side, -u, -ui, -ti FROM ev " + positive, who: "live", why: "unary minus"},
		{stmt: "SELECT side, u + ? FROM ev " + positive, args: []any{1}, who: "live", why: "prepared"},
		{stmt: "SELECT side, dcu - 1, dcu * 2, dbu - 1, dbu * 2 FROM ev " + positive, who: "copy", why: "unsigned DECIMAL and DOUBLE"},
		{stmt: "SELECT side, SUM(u - k), SUM(ui) - 1 FROM ev WHERE k > 2 GROUP BY side", who: "live", why: "under an aggregate"},
		{stmt: "SELECT side, x - 1 FROM (SELECT side, k, u AS x FROM ev) q " + positive, who: "live", why: "under an alias"},

		{stmt: "SELECT side, b - u FROM ev WHERE id = 4", who: "live", why: "negative: error 1690 on MySQL"},
		{stmt: "SELECT side, u - 4 FROM ev WHERE k = 3", who: "live", why: "negative unsigned minus a literal"},
		{stmt: "SELECT side, ui - 4 FROM ev WHERE k = 3", who: "live", why: "negative INT UNSIGNED minus a literal"},
		{stmt: "SELECT side, ti - 4 FROM ev WHERE k = 3", who: "live", why: "negative TINYINT UNSIGNED minus a literal"},
		{stmt: "SELECT side, u * -1 FROM ev WHERE k = 3", who: "live", why: "times a negative literal"},
		{stmt: "SELECT side, u + -5 FROM ev WHERE k = 3", who: "live", why: "plus a negative literal"},
		{stmt: "SELECT side, u + 1 FROM ev WHERE k = 1", who: "live", why: "overflow past 2^64-1"},
		{stmt: "SELECT side, u * 2 FROM ev WHERE k = 1", who: "live", why: "overflow by product"},
		{stmt: "SELECT side, b - u FROM ev WHERE k IS NULL", who: "live", why: "NULL"},
		{stmt: "SELECT side, u - b FROM ev WHERE k = 2", who: "live", why: "0 minus the smallest BIGINT"},

		{stmt: "SELECT side, u, ui, ti, dcu, dbu FROM ev ORDER BY k", who: "?", why: "the columns themselves: an INT and a TINYINT UNSIGNED are a signed BIGINT on the copy (#2856)"},
		{stmt: "SELECT side, u FROM ev WHERE u > -1 ORDER BY k", who: "copy", why: "compared with a negative literal"},
		{stmt: "SELECT side, u FROM ev WHERE u > b AND u IN (-1, 3, 4) ORDER BY k", who: "copy", why: "compared with a signed column, IN"},
		{stmt: "SELECT side, u FROM ev WHERE u BETWEEN -5 AND 5 ORDER BY k", who: "copy", why: "BETWEEN a negative"},
		{stmt: "SELECT side, GREATEST(u, b), LEAST(u, b), COALESCE(u, b) FROM ev " + positive, who: "", why: "signed and unsigned merged"},
		{stmt: "SELECT side, IF(k > 4, u, b), CASE WHEN k > 4 THEN u ELSE b END FROM ev " + positive, who: "copy", why: "IF and CASE"},
		{stmt: "SELECT side, ABS(ui), ABS(u) FROM ev " + positive, who: "?", why: "ABS: a signed BIGINT on the copy (#2856)"},
		{stmt: "SELECT MIN(side), SUM(u), MAX(ui), MIN(ti) FROM ev", who: "?", why: "MAX and MIN keep the column's type on MySQL (#2856)"},
		{stmt: "SELECT side, k - 1, b + 1 FROM ev WHERE u > 2 AND u < 100 ORDER BY k", who: "copy", why: "signed arithmetic beside an unsigned filter"},
		{stmt: "SELECT side, ROW_NUMBER() OVER (ORDER BY k) - 10, RANK() OVER (ORDER BY k) - 10, DENSE_RANK() OVER (ORDER BY k) - 10 FROM ev ORDER BY k", who: "live", why: "window numbers"},
		{stmt: "SELECT side, ROW_NUMBER() OVER (ORDER BY k) FROM ev ORDER BY k", who: "?", why: "ROW_NUMBER alone: a BIGINT UNSIGNED on MySQL, a BIGINT on the copy (#2856)"},
		{stmt: "SELECT side, LENGTH(side) - 10, CHAR_LENGTH(side) - 10, ASCII(side) - 200, LOCATE('i', side) - 5 FROM ev ORDER BY k", who: "", why: "text functions"},
		{stmt: "SELECT side, CRC32(side) - 1 FROM ev ORDER BY k", who: "", why: "CRC32"},
		{stmt: "SELECT MIN(side), COUNT(*) - 100 FROM ev", who: "copy", why: "COUNT"},
		{stmt: "SELECT side, UNIX_TIMESTAMP(dt) - 2000000000, UNIX_TIMESTAMP(dt) FROM ev ORDER BY k", who: "", why: "UNIX_TIMESTAMP"},
		{stmt: "SELECT side, YEAR(dt) - 3000, MONTH(dt) - 20, DAYOFYEAR(dt) - 400 FROM ev ORDER BY k", who: "live", why: "date parts: YEAR is unsigned on MySQL 8.4"},
		{stmt: "SELECT side, EXTRACT(YEAR FROM dt) - 3000, HOUR(dt) - 30 FROM ev ORDER BY k", who: "live", why: "EXTRACT, HOUR"},
		{stmt: "SELECT side, MONTH(dt) - 20 FROM ev ORDER BY k", who: "live", why: "MONTH alone, its type logged"},
		{stmt: "SELECT side, HOUR(dt) - 30 FROM ev ORDER BY k", who: "live", why: "HOUR alone, its type logged"},
		{stmt: "SELECT side, YEAR(dt) FROM ev WHERE YEAR(dt) = 2026 ORDER BY k", who: "?", why: "YEAR selected: its declared type differs on MySQL 8.4 (#2856)"},
		{stmt: "SELECT side, NTILE(3) OVER (ORDER BY k) - 10, NTILE(3) OVER (ORDER BY k) FROM ev ORDER BY k", who: "live", why: "NTILE"},
		{stmt: "SELECT side, dcu - 10, dbu - 10 FROM ev WHERE k = 3", who: "copy", why: "negative unsigned DECIMAL and DOUBLE"},
		{stmt: "SELECT side, u + ? FROM ev WHERE k = 3", args: []any{-5}, who: "live", why: "prepared, negative"},
		{stmt: "SELECT side, k - ? FROM ev WHERE k = 3", args: []any{uint64(10)}, who: "live", why: "an argument bound as unsigned: a BIGINT UNSIGNED on MySQL"},
		{stmt: "SELECT side, k FROM ev WHERE k > ? - 10 ORDER BY k", args: []any{uint64(5)}, who: "live", why: "an argument bound as unsigned, in WHERE"},
		{stmt: "SELECT side, b FROM ev WHERE k = ? ORDER BY k", args: []any{uint64(3)}, who: "copy", why: "an argument bound as unsigned, compared"},
		{stmt: "SELECT side, `u` - 4, ev.ui - 4 FROM ev WHERE k = 3", who: "live", why: "quoted and qualified"},
		{stmt: "SELECT side, ui AS x FROM ev WHERE k > 2 ORDER BY x - 4, k", who: "live", why: "alias in ORDER BY"},
		{stmt: "SELECT side, MAX(ui) AS x FROM ev GROUP BY side HAVING x - 100 > 0", who: "live", why: "alias in HAVING"},
		{stmt: "SELECT side, MAX(ui) OVER () - 4294967296 FROM ev WHERE k = 3", who: "live", why: "window MAX"},
		{stmt: "SELECT side, CASE WHEN k > 0 THEN ui ELSE 0 END - 4 FROM ev WHERE k = 3", who: "live", why: "CASE"},
		{stmt: "SELECT side, COALESCE(ui, 0) - 4, GREATEST(ti, 1) - 4 FROM ev WHERE k = 3", who: "live", why: "calls"},
		{stmt: "SELECT side, ui % k, ui MOD 2 FROM ev WHERE k = 3", who: "live", why: "modulo of INT UNSIGNED"},
		{stmt: "SELECT side, SUM(ui) - 100, COUNT(ui) - 100 FROM ev GROUP BY side", who: "copy", why: "SUM and COUNT"},
		{stmt: "SELECT side, ui + INTERVAL 1 DAY FROM ev WHERE k = 3", who: "live", why: "INTERVAL"},
		{stmt: "SELECT side, ui << 1, ti << 1 FROM ev WHERE k = 3", who: "live", why: "a shift: BIGINT UNSIGNED on MySQL"},
	}
	asText := func(dsn string) string { return strings.Replace(dsn, "parseTime=true", "parseTime=false", 1) }
	routedText := openRaw(t, asText(withZone(portDSN, "+00:00")))
	liveText := openRaw(t, asText(withZone(sourceDSN, "+00:00")))
	check := func(label string, routed, live *sql.DB, c sqlCase) {
		mark := heavyMark(t, srv, ent.ID)
		got := scaledAnswer(routed, c.stmt, c.args...)
		want := scaledAnswer(live, c.stmt, c.args...)
		t.Logf("%s routed %-90.400s %s", label, got, c.stmt)
		if heavyOnCopy(t, mark(), c.who, c.stmt, got, want) {
			return
		}
		t.Logf("%s live   %-90.400s", label, want)
		if c.who == "?" {
			return
		}
		if strings.Contains(want, "ERROR ") {
			// MySQL refused the statement, before or after its column
			// definitions: the port must send that refusal, which names
			// MySQL's own database and expression, and nothing else.
			if got != want {
				t.Errorf("%s routed %q (%s):\n  got  %s\n  want %s, MySQL's own refusal", label, c.stmt, c.why, got, want)
			}
			return
		}
		if strings.HasPrefix(got, "ERROR ") || (c.who != "" && !strings.Contains(got, "("+c.who+",")) {
			t.Errorf("%s routed %q (%s):\n  got  %s\n  want it answered by %s", label, c.stmt, c.why, got, c.who)
			return
		}
		if strings.ReplaceAll(got, "(copy,", "(live,") != want {
			t.Errorf("%s routed %q (%s):\n  got  %s\n  want %s, the source's own answer", label, c.stmt, c.why, got, want)
		}
	}
	for _, c := range cases {
		check("", routedText, liveText, c)
	}

	// NO_UNSIGNED_SUBTRACTION: MySQL then types a difference with an
	// unsigned operand as a signed BIGINT and answers a negative one.
	mode := "&sql_mode=" + url.QueryEscape("'NO_UNSIGNED_SUBTRACTION'")
	routedMode := openRaw(t, asText(withZone(portDSN, "+00:00"))+mode)
	liveMode := openRaw(t, asText(withZone(sourceDSN, "+00:00"))+mode)
	for _, c := range []sqlCase{
		{stmt: "SELECT side, b - u, u - b FROM ev " + positive, who: "live", why: "the issue's case"},
		{stmt: "SELECT side, u - 4, ui - 4, ti - 4 FROM ev WHERE k = 3", who: "live", why: "negative"},
		{stmt: "SELECT side, u + 1, ui + k FROM ev " + positive, who: "live", why: "a sum"},
		{stmt: "SELECT side, k - 1 FROM ev " + positive, who: "copy", why: "no unsigned column"},
	} {
		check("no_unsigned_subtraction", routedMode, liveMode, c)
	}
}
