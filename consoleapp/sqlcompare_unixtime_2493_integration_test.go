//go:build integration

package consoleapp

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/readrouter"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// #2493 against a real MySQL: UNIX_TIMESTAMP and FROM_UNIXTIME, the way a
// report buckets time, answered by the copy with the source's value under
// read routing, and kept on the source where the copy's value would not be
// the source's.
func TestIntegrationSQLCompareUnixTime(t *testing.T) {
	srcDB, srcName := testutil.CreateTestDB(t)
	unixTimeCompare(t, srcDB, srcName, testutil.IntegrationDSN(srcName))
}

// The same against a MariaDB source. A test of its own, with MariaDB in its
// name: the MariaDB job picks its tests by name.
func TestIntegrationSQLCompareUnixTimeMariaDB(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	srcDB, srcName := testutil.CreateTestMariaDB(t)
	unixTimeCompare(t, srcDB, srcName, testutil.MariaDBBaseDSN()+"/"+srcName+"?parseTime=true")
}

// withZone adds a session time zone to a DSN: the driver sets it on every
// connection it opens, so a pool cannot hand out a connection without it.
func withZone(dsn, zone string) string {
	sep := "?"
	if strings.Contains(dsn, "?") {
		sep = "&"
	}
	return dsn + sep + "time_zone=" + url.QueryEscape("'"+zone+"'")
}

func unixTimeCompare(t *testing.T, srcDB *sql.DB, srcName, sourceDSN string) {
	now := time.Now().UTC().Truncate(time.Hour)
	indexDSN := seedFlashbackIndex(t, "alice", now)
	var version string
	if err := srcDB.QueryRow("SELECT VERSION()").Scan(&version); err != nil {
		t.Fatal(err)
	}
	t.Logf("source: %s", version)

	// Written under UTC on both sides: the copy's file holds every date and
	// time as a UTC instant, and the source's TIMESTAMP is read in the
	// session's zone.
	row := func(side string) [][]string {
		return [][]string{
			{"1", "2026-01-15", "2026-01-15 12:34:56", "2026-01-15 12:34:56", "2026-01-15 12:34:56.250", "1768478400", side},
			{"2", "2026-03-08", "2026-03-08 02:30:00", "2026-03-08 02:30:00", "2026-03-08 02:30:00.000", "1700000000", side},
			{"3", "1999-12-31", "1999-12-31 23:59:59", "1999-12-31 23:59:59", "1999-12-31 23:59:59.999", "0", side},
			{"4", "2026-01-15", "2026-01-15 13:05:00", "2026-01-15 13:05:00", "2026-01-15 13:05:00.000", "86399", side},
		}
	}
	// z holds what the copy cannot tell from MySQL's answer, or MySQL types
	// with a fraction: a zero date (written under an empty sql_mode, which
	// lets one in, and held as a NULL on the copy, which MySQL's
	// UNIX_TIMESTAMP reads as 0), a real NULL, a DECIMAL(12,2) and a DOUBLE
	// holding whole numbers, and milliseconds.
	zrow := func(side string) [][]string {
		return [][]string{
			{"1", "0000-00-00 00:00:00", "2026-01-15 10:00:00", "1700000000.00", "1700000000", "1700000000500", side},
			{"2", "2026-01-15 12:00:00", `\N`, "1700003600.00", "1700003600", "1700003600000", side},
			{"3", "2026-01-16 00:00:00", "2026-01-16 00:00:00", "1700007200.00", "1700007200", "1700007200999", side},
			{"4", "2026-01-17 00:00:00", "2026-01-17 00:00:00", "1700010800.00", "1700010800", "1700010800001", side},
		}
	}
	src := openRaw(t, withZone(sourceDSN, "+00:00")+"&sql_mode="+url.QueryEscape("''"))
	baseDir := t.TempDir()
	var tables []orderTable
	for _, tb := range []struct {
		name, create string
		rows         func(string) [][]string
	}{
		{"ev", "CREATE TABLE ev (id INT NOT NULL PRIMARY KEY, d DATE, dt DATETIME, ts TIMESTAMP NULL, dt3 DATETIME(3), secs BIGINT, side VARCHAR(8))", row},
		{"z", "CREATE TABLE z (id INT NOT NULL PRIMARY KEY, dtz DATETIME NOT NULL, dn DATETIME NULL, price DECIMAL(12,2), f DOUBLE, ms BIGINT, side VARCHAR(8))", zrow},
	} {
		if _, err := src.Exec(tb.create); err != nil {
			t.Fatal(err)
		}
		for _, r := range tb.rows("live") {
			vals := make([]string, len(r))
			for i, v := range r {
				vals[i] = "'" + v + "'"
				if v == `\N` {
					vals[i] = "NULL"
				}
			}
			if _, err := src.Exec("INSERT INTO " + tb.name + " VALUES (" + strings.Join(vals, ",") + ")"); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := src.Exec("ANALYZE TABLE " + tb.name); err != nil {
			t.Fatal(err)
		}
		var name, ddl string
		if err := src.QueryRow("SHOW CREATE TABLE " + tb.name).Scan(&name, &ddl); err != nil {
			t.Fatal(err)
		}
		tables = append(tables, orderTable{name: tb.name, ddl: ddl + ";\n", rows: tb.rows("live"), copyRows: tb.rows("copy"), footer: true})
	}
	writeOrderSnapshot(t, baseDir, srcName, tables)

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

	const bucket = "SELECT MIN(side), FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(dt) / 3600) * 3600) AS period_start, COUNT(*) FROM ev GROUP BY period_start ORDER BY period_start"
	cases := []struct {
		stmt, who, why string
	}{
		{bucket, "copy", "the issue's statement: orders per hour"},
		{"SELECT side, UNIX_TIMESTAMP(dt) FROM ev ORDER BY secs", "copy", "a DATETIME without fractional seconds"},
		{"SELECT side, UNIX_TIMESTAMP(ts) FROM ev ORDER BY secs", "copy", "a TIMESTAMP without fractional seconds"},
		{"SELECT side, UNIX_TIMESTAMP(d) FROM ev ORDER BY secs", "copy", "a DATE"},
		{"SELECT side, FLOOR(UNIX_TIMESTAMP(ts) / 86400) AS day, COUNT(*) FROM ev GROUP BY side, day ORDER BY day", "copy", "orders per day, over a TIMESTAMP"},
		{"SELECT side, FROM_UNIXTIME(secs) FROM ev WHERE secs > 0 ORDER BY secs", "copy", "a whole number of seconds"},
		{"SELECT side, FROM_UNIXTIME(secs) FROM ev ORDER BY secs", "live", "second 0: NULL on MariaDB 11.8 and 12.3, so the copy declines it"},
		{"SELECT side, FROM_UNIXTIME(UNIX_TIMESTAMP(dt)) = dt FROM ev ORDER BY secs", "copy", "the round trip"},
		{"SELECT side, UNIX_TIMESTAMP(dt3) FROM ev ORDER BY secs", "live", "fractional seconds: a decimal on the source"},
		{"SELECT side, FROM_UNIXTIME(secs + 0.5) FROM ev ORDER BY secs", "live", "a fraction: a decimal on the source"},
		{"SELECT side, UNIX_TIMESTAMP() > 0 FROM ev ORDER BY secs", "live", "now: vetoed from the text"},
		{"SELECT side, UNIX_TIMESTAMP('2026-01-15 00:00:00') FROM ev ORDER BY secs", "live", "a literal"},
		{"SELECT side, `UNIX_TIMESTAMP`(`dt3`) FROM `ev` ORDER BY secs", "live", "a quoted name is the function on the source"},
		{"SELECT side, `UNIX_TIMESTAMP`() > 0 FROM `ev` ORDER BY secs", "live", "now, quoted"},
		{"SELECT side, `UNIX_TIMESTAMP`(`dt`) FROM `ev` ORDER BY secs", "live", "a quoted name before a parenthesis is vetoed from the text"},
		{"SELECT side, UNIX_TIMESTAMP(dtz) FROM z ORDER BY ms", "live", "a zero date: 0 on the source, a NULL on the copy"},
		{"SELECT side, UNIX_TIMESTAMP(dn) FROM z ORDER BY ms", "live", "a real NULL, which the copy cannot tell from a zero date"},
		{"SELECT side, FROM_UNIXTIME(price) FROM z ORDER BY ms", "live", "a DECIMAL(12,2): .00 on the source"},
		{"SELECT side, FROM_UNIXTIME(f) FROM z ORDER BY ms", "live", "a DOUBLE: .000000 on the source"},
		{"SELECT side, FROM_UNIXTIME(ms / 1000) FROM z ORDER BY ms", "live", "a division: .0000 on the source"},
		{"SELECT side, FROM_UNIXTIME(FLOOR(f / 3600) * 3600) FROM z ORDER BY ms", "live", "FLOOR of a DOUBLE stays a DOUBLE"},
		{"SELECT side, FROM_UNIXTIME(FLOOR(ms / 1000)) FROM z ORDER BY ms", "live", "a column in a division: the copy's DOUBLE is exact only under 2^53"},
		{"SELECT side, FROM_UNIXTIME(CEIL(UNIX_TIMESTAMP(dt) / 60) * 60) FROM ev ORDER BY secs", "copy", "CEIL of a division is a whole number"},
		{"SELECT side, FROM_UNIXTIME(FLOOR((UNIX_TIMESTAMP(dt) + 18000) / 86400) * 86400) FROM ev ORDER BY secs", "live", "a date column next to a +: the date-arithmetic rule (#2133) keeps it on the source"},
	}
	for _, zone := range []string{"+00:00", "+05:00"} {
		routed := openRaw(t, withZone(portDSN, zone))
		live := openRaw(t, withZone(sourceDSN, zone))
		for _, c := range cases {
			got := answerText(routed, c.stmt)
			t.Logf("[%s] routed %-70s %s", zone, got, c.stmt)
			if strings.HasPrefix(got, "ERROR ") || !strings.HasPrefix(got, c.who+"|") {
				t.Errorf("[%s] routed %q (%s):\n  got  %s\n  want it answered by %s", zone, c.stmt, c.why, got, c.who)
				continue
			}
			// The copy's rows say "copy" where the source's say "live";
			// everything else must be the source's own answer.
			want := answerText(live, c.stmt)
			if strings.ReplaceAll(got, "copy|", "live|") != want {
				t.Errorf("[%s] routed %q (%s):\n  got  %s\n  want %s, the source's own answer", zone, c.stmt, c.why, got, want)
			}
		}
	}
	// #2607: a session with autocommit off is in a transaction from its
	// first statement, so the same expensive read is MySQL's, the copy's
	// again once the session turns autocommit back on.
	off := openRaw(t, withZone(portDSN, "+00:00")+"&autocommit=0")
	if got := answerText(off, bucket); !strings.HasPrefix(got, "live|") {
		t.Errorf("with autocommit off: routed %s, want it answered by MySQL (a transaction)", got)
	}
	on := openRaw(t, withZone(portDSN, "+00:00")+"&autocommit=1")
	if got := answerText(on, bucket); !strings.HasPrefix(got, "copy|") {
		t.Errorf("with autocommit on: routed %s, want it answered by the copy", got)
	}
	// And within one session, as an application runs it: a COMMIT under
	// autocommit off starts the next transaction, SET autocommit=1 ends it.
	one, err := openRaw(t, withZone(portDSN, "+00:00")).Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer one.Close()
	side := func(step string) string {
		t.Helper()
		var who string
		if err := one.QueryRowContext(context.Background(), bucket).Scan(&who, new(sql.NullString), new(sql.NullString)); err != nil {
			t.Fatalf("%s: %v", step, err)
		}
		return who
	}
	for _, step := range []struct{ stmt, want string }{
		{"SET autocommit=0", "live"},
		{"COMMIT", "live"},
		{"ROLLBACK", "live"},
		{"SET autocommit=1", "copy"},
	} {
		if _, err := one.ExecContext(context.Background(), step.stmt); err != nil {
			t.Fatalf("%s: %v", step.stmt, err)
		}
		if got := side(step.stmt); got != step.want {
			t.Errorf("one session, after %s: answered by %s, want %s", step.stmt, got, step.want)
		}
	}

	// What MySQL answered was the copy's decision every time, never its
	// fault: the NULL and the zero date the UNIX_TIMESTAMP macro declines
	// count as the copy's columns differing, not as the copy refusing.
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
	if reasons["copy_refused"] != 0 || reasons["copy_columns_differ"] < 2 {
		t.Errorf("reasons = %v, want copy_refused 0 and copy_columns_differ at least 2 (the declined fraction, in each zone)", reasons)
	}
}
