//go:build integration

package consoleapp

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/readrouter"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// #2806 against a real MySQL: under routing, a division with / and an AVG
// are answered by MySQL, which returns a DECIMAL with div_precision_increment
// more digits than the operand (k / 7 is 0.1429) where the copy divides as a
// DOUBLE (0.14285714285714285). A SUM of whole numbers is answered by the
// copy, typed DECIMAL as MySQL types it.
func TestIntegrationSQLCompareDivisionRouted(t *testing.T) {
	srcDB, srcName := testutil.CreateTestDB(t)
	divisionRouted(t, srcDB, srcName, testutil.IntegrationDSN(srcName))
}

// The same against a MariaDB source. A test of its own, with MariaDB in its
// name: the MariaDB job picks its tests by name.
func TestIntegrationSQLCompareDivisionRoutedMariaDB(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	srcDB, srcName := testutil.CreateTestMariaDB(t)
	divisionRouted(t, srcDB, srcName, testutil.MariaDBBaseDSN()+"/"+srcName+"?parseTime=true")
}

// scaledAnswer is typedAnswer with each column's scale, when the driver
// reports one: a DECIMAL's scale is what a client formats it by.
func scaledAnswer(db *sql.DB, stmt string, args ...any) string {
	rows, err := db.Query(stmt, args...)
	if err != nil {
		return "ERROR " + err.Error()
	}
	defer rows.Close()
	types, err := rows.ColumnTypes()
	if err != nil {
		return "ERROR " + err.Error()
	}
	var b strings.Builder
	b.WriteString("[")
	for i, ct := range types {
		if i > 0 {
			b.WriteString(" ")
		}
		b.WriteString(ct.DatabaseTypeName())
		if _, scale, ok := ct.DecimalSize(); ok {
			fmt.Fprintf(&b, "/%d", scale)
		}
	}
	b.WriteString("]")
	for rows.Next() {
		cells := make([]sql.RawBytes, len(types))
		ptrs := make([]any, len(types))
		for i := range cells {
			ptrs[i] = &cells[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return "ERROR " + err.Error()
		}
		b.WriteString(" (")
		for i, c := range cells {
			if i > 0 {
				b.WriteString(", ")
			}
			if c == nil {
				b.WriteString("NULL")
			} else {
				b.Write(c)
			}
		}
		b.WriteString(")")
	}
	if err := rows.Err(); err != nil {
		return b.String() + " ERROR " + err.Error()
	}
	return b.String()
}

// scaleless is the scale scaledAnswer writes after a type.
var scaleless = regexp.MustCompile(`/\d+`)

func divisionRouted(t *testing.T, srcDB *sql.DB, srcName, sourceDSN string) {
	now := time.Now().UTC().Truncate(time.Second)
	indexDSN := seedFlashbackIndex(t, "alice", now.Truncate(time.Hour))
	var version string
	if err := srcDB.QueryRow("SELECT VERSION()").Scan(&version); err != nil {
		t.Fatal(err)
	}
	t.Logf("source: %s", version)

	// Whole numbers of every width at their limits, a negative, a zero to
	// divide by, a DECIMAL, a DOUBLE, two dates, and a row of NULLs.
	rows := func(side string) [][]string {
		return [][]string{
			{"1", "1", side, "9223372036854775807", "18446744073709551615", "4294967295", "1.50", "0.5", "2026-01-01 10:00:00", "2026-01-05 10:00:00", "0"},
			{"2", "2", side, "-9223372036854775808", "0", "0", "-2.25", "1.25", "2025-06-30 00:00:00", "2026-01-01 00:00:00", "0"},
			{"3", "3", side, "3", "3", "3", "0.01", "-3", "2024-02-29 12:00:00", "2024-01-01 00:00:00", "0"},
			{"4", "-4", side, "-4", "4", "4", "99999999.99", "10", "2026-03-31 23:59:59", "2026-04-01 00:00:00", "0"},
			{"5", "5", side, "5", "5", "5", "-0.05", "2", "2000-01-01 00:00:00", "2010-01-01 00:00:00", "0"},
			{"6", "6", side, "6", "6", "6", "3.33", "7", "2026-01-01 00:00:00", "2026-01-01 00:00:00", "0"},
			{"7", "7", side, "7", "7", "7", "7.00", "0.25", "2026-12-31 23:00:00", "2026-01-01 00:00:00", "0"},
			{"8", `\N`, side, `\N`, `\N`, `\N`, `\N`, `\N`, `\N`, `\N`, `\N`},
		}
	}
	src := openRaw(t, withZone(sourceDSN, "+00:00"))
	const create = "CREATE TABLE ev (id INT NOT NULL PRIMARY KEY, k INT, side VARCHAR(8), b BIGINT, u BIGINT UNSIGNED, ui INT UNSIGNED, dc DECIMAL(10,2), dbl DOUBLE, dt DATETIME, dt2 DATETIME, z INT)"
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
	// and not who gave it. noScale leaves the declared scale out: the copy
	// declares every DATETIME with six fraction digits (applyDuckType), a
	// difference of its own (#2849).
	cases := []struct {
		stmt    string
		args    []any
		who     string
		noScale bool
		why     string
	}{
		{stmt: "SELECT side, k / 7 FROM ev ORDER BY k", who: "live", why: "the issue's case"},
		{stmt: "SELECT side, k/7, -k / 7, k / -7 FROM ev ORDER BY k", who: "live", why: "signs, no spaces"},
		{stmt: "SELECT side, k / 0, k / z FROM ev ORDER BY k", who: "live", why: "by zero"},
		{stmt: "SELECT side, b / 3, u / 3, ui / 7 FROM ev ORDER BY k", who: "live", why: "BIGINT, UNSIGNED"},
		{stmt: "SELECT side, dc / 3, dc / dc FROM ev ORDER BY k", who: "live", why: "a DECIMAL"},
		{stmt: "SELECT side, dbl / 2 FROM ev ORDER BY k", who: "live", why: "a DOUBLE"},
		{stmt: "SELECT side, k / 7.0, k / 2e0 FROM ev ORDER BY k", who: "live", why: "a decimal and a double literal"},
		{stmt: "SELECT side, YEAR(dt) / 7, DATEDIFF(dt2, dt) / 7 FROM ev ORDER BY k", who: "live", why: "a function's whole number"},
		{stmt: "SELECT side, FLOOR(k / 7), CEIL(k / 3) FROM ev ORDER BY k", who: "live", why: "FLOOR of a quotient, selected"},
		{stmt: "SELECT side, b FROM ev WHERE k / 3 > 1 ORDER BY k", who: "live", why: "in WHERE"},
		{stmt: "SELECT side, b FROM ev ORDER BY k / 3, b", who: "live", why: "in ORDER BY"},
		{stmt: "SELECT side, k / ? FROM ev ORDER BY k", args: []any{7}, who: "live", why: "prepared"},
		{stmt: "SELECT MIN(side), SUM(k) / COUNT(*) FROM ev", who: "live", why: "SUM over COUNT"},
		{stmt: "SELECT MIN(side), AVG(k), AVG(b), AVG(u), AVG(dc), AVG(dbl) FROM ev", who: "live", why: "AVG"},
		{stmt: "SELECT MIN(side), AVG(z) FROM ev WHERE k IS NULL", who: "live", why: "AVG of NULLs"},
		{stmt: "SELECT MIN(side), avg (k) FROM ev", who: "live", why: "AVG with a space"},

		{stmt: "SELECT MIN(side), SUM(k) FROM ev", who: "copy", why: "the issue's SUM"},
		{stmt: "SELECT MIN(side), SUM(b), SUM(u), SUM(ui) FROM ev", who: "copy", why: "BIGINT, UNSIGNED, past 2^64"},
		{stmt: "SELECT MIN(side), SUM(dc) FROM ev", who: "copy", why: "a DECIMAL"},
		{stmt: "SELECT MIN(side), SUM(dbl) FROM ev", who: "?", why: "a DOUBLE: the same text, and the scale a DOUBLE is declared with differs (#2829)"},
		{stmt: "SELECT MIN(side), SUM(z) FROM ev WHERE k IS NULL", who: "copy", why: "NULLs only"},
		{stmt: "SELECT MIN(side), SUM(k) FROM ev WHERE k > 100", who: "", why: "no row: nothing says who answered"},
		{stmt: "SELECT side, SUM(k) OVER (ORDER BY k) FROM ev ORDER BY k", who: "copy", why: "a window"},
		{stmt: "SELECT MIN(side), SUM(k) + 1, SUM(k) * 2, -SUM(k), ABS(SUM(k)) FROM ev", who: "copy", why: "arithmetic on a SUM"},
		{stmt: "SELECT MIN(side), SUM(k) * 1.5, SUM(k) + 0.25 FROM ev", who: "copy", why: "a SUM and a decimal"},
		{stmt: "SELECT MIN(side), SUM(YEAR(dt)), SUM(DATEDIFF(dt2, dt)) FROM ev", who: "copy", why: "SUM of a function's whole number"},
		{stmt: "SELECT side, SUM(k) FROM ev GROUP BY side", who: "copy", why: "grouped"},
		{stmt: "SELECT MIN(side), SUM(k) FROM ev WHERE k > ?", args: []any{0}, who: "copy", why: "prepared"},

		{stmt: "SELECT side, k % 3, MOD(k, 3) FROM ev ORDER BY k", who: "copy", why: "modulo"},
		{stmt: "SELECT MIN(side), MAX(b), MAX(u), COUNT(k) FROM ev", who: "copy", why: "MAX, COUNT"},
		{stmt: "SELECT MIN(side), STD(k), VARIANCE(k) FROM ev", who: "live", why: "STD and VARIANCE, kept by an older rule"},
		{stmt: "SELECT side, 9223372036854775808, 99999999999999999999 FROM ev ORDER BY k", who: "live", why: "literals past BIGINT: a BIGINT UNSIGNED and a DECIMAL on MySQL"},
		{stmt: "SELECT side, -9223372036854775808 FROM ev ORDER BY k", who: "live", why: "the smallest BIGINT, written"},
		{stmt: "SELECT side, 9223372036854775807 FROM ev ORDER BY k", who: "copy", why: "the largest BIGINT, written"},
		{stmt: "SELECT side, k DIV 2 FROM ev ORDER BY k", who: "live", why: "DIV"},
		{stmt: "SELECT side, ui + k, -ui, u - 1 FROM ev WHERE k > 0 ORDER BY k", who: "?", why: "unsigned arithmetic: #2848"},
		{stmt: "SELECT side, b - u FROM ev WHERE k > 2 ORDER BY k", who: "?", why: "signed minus unsigned: #2848"},
		{stmt: "SELECT side, FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(dt) / 3600) * 3600) FROM ev WHERE k IS NOT NULL ORDER BY k", who: "copy", noScale: true, why: "the #2493 bucket"},
		{stmt: "SELECT side, FLOOR(UNIX_TIMESTAMP(dt) / 86400) FROM ev WHERE k IS NOT NULL ORDER BY k", who: "live", why: "FLOOR of a quotient outside FROM_UNIXTIME: a DECIMAL on MySQL, a DOUBLE on the copy"},
	}
	asText := func(dsn string) string { return strings.Replace(dsn, "parseTime=true", "parseTime=false", 1) }
	routedText := openRaw(t, asText(withZone(portDSN, "+00:00")))
	liveText := openRaw(t, asText(withZone(sourceDSN, "+00:00")))
	for _, c := range cases {
		got := scaledAnswer(routedText, c.stmt, c.args...)
		want := scaledAnswer(liveText, c.stmt, c.args...)
		t.Logf("routed %-90.400s %s", got, c.stmt)
		t.Logf("live   %-90.400s", want)
		if c.who == "?" {
			continue
		}
		if c.noScale {
			got, want = scaleless.ReplaceAllString(got, ""), scaleless.ReplaceAllString(want, "")
		}
		if strings.HasPrefix(got, "ERROR ") || (c.who != "" && !strings.Contains(got, "("+c.who+",")) {
			t.Errorf("routed %q (%s):\n  got  %s\n  want it answered by %s", c.stmt, c.why, got, c.who)
			continue
		}
		if strings.ReplaceAll(got, "(copy,", "(live,") != want {
			t.Errorf("routed %q (%s):\n  got  %s\n  want %s, the source's own answer", c.stmt, c.why, got, want)
		}
	}

	// A session that changed div_precision_increment is answered by MySQL
	// whatever it runs (routedsession), a SUM included.
	routedPrec := openRaw(t, asText(withZone(portDSN, "+00:00"))+"&div_precision_increment=2")
	livePrec := openRaw(t, asText(withZone(sourceDSN, "+00:00"))+"&div_precision_increment=2")
	for _, stmt := range []string{"SELECT side, k / 7 FROM ev ORDER BY k", "SELECT MIN(side), SUM(k), AVG(k) FROM ev"} {
		got, want := scaledAnswer(routedPrec, stmt), scaledAnswer(livePrec, stmt)
		t.Logf("prec=2 routed %-80.300s %s", got, stmt)
		if !strings.Contains(got, "(live,") || got != want {
			t.Errorf("div_precision_increment=2, routed %q:\n  got  %s\n  want %s, from MySQL", stmt, got, want)
		}
	}
}
