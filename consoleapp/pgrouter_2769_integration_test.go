//go:build integration

package consoleapp

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/dbtrail/dbtrail/internal/shim"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// The PostgreSQL port's session settings, query cancel and infinite dates
// (#2769), end to end: a real PostgreSQL source, a snapshot of it in a
// schema of its own, the console's real SQL sandbox, and the same statements
// sent to both. What a client is told (each SET's tag, each ParameterStatus,
// each SHOW) is the source's; a timestamp with time zone reads the same in
// every zone, in text and in binary; a cancel request stops a statement on
// the copy as on the source, over TLS too; and infinity reads as infinity.

const (
	pgSetSchema = "s2769"
	pgSetTable  = pgSetSchema + ".pg2769_t"
)

func startPGSettingsPort(t *testing.T, ctx context.Context) (pgExtEnv, testutil.TestCert) {
	t.Helper()
	cert := testutil.WriteTestCert(t, testutil.TestCertOptions{})
	pt, err := shim.LoadPortTLS(cert.CertFile, cert.KeyFile)
	if err != nil {
		t.Fatal(err)
	}
	env := startPGPortFor(t, ctx, pt.Config, pgPortFixture{table: pgSetTable, slot: "bintrail_pg2769_slot", pub: "bintrail_pg2769_pub", copyOnly: true, setup: []string{
		"CREATE SCHEMA IF NOT EXISTS " + pgSetSchema,
		"CREATE TABLE " + pgSetTable + " (id int PRIMARY KEY, tstz timestamptz, ts timestamp, day date, da date[])",
		"INSERT INTO " + pgSetTable + ` VALUES
		(1, '2026-01-01 10:00:00.5+00', '2026-01-01 10:00:00', '2026-01-01', '{2026-01-01}'),
		(2, '2026-03-29 00:30:00+00', '2026-03-29 00:30:00', '2026-03-29', NULL),
		(3, '2026-03-29 01:30:00+00', '2026-03-29 01:30:00', '2026-03-29', NULL),
		(4, '1900-01-01 10:00:00+00', '1900-01-01 10:00:00', '1900-01-01', NULL),
		(5, '2026-07-01 23:30:00+00', '2026-07-01 23:30:00', '2026-07-01', NULL),
		(6, '2026-10-25 00:59:59.999999+00', '2026-10-25 00:59:59.999999', '2026-10-25', NULL),
		(7, NULL, NULL, NULL, NULL),
		(100, 'infinity', 'infinity', 'infinity', '{infinity,-infinity}'),
		(101, '-infinity', '-infinity', '-infinity', '{-infinity,2026-01-01}')`,
	}})
	return env, cert
}

// rawPGWith is rawPG with start-up parameters of its own.
func rawPGWith(t *testing.T, ctx context.Context, dsn string, params map[string]string) rawFrontend {
	t.Helper()
	cfg, err := pgconn.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.RuntimeParams["timezone"] = "UTC"
	for k, v := range params {
		cfg.RuntimeParams[k] = v
	}
	conn, err := pgconn.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	hj, err := conn.Hijack()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { hj.Conn.Close() })
	_ = hj.Conn.SetDeadline(time.Now().Add(5 * time.Minute))
	return rawFrontend{fe: hj.Frontend}
}

// said is what a simple query was answered with, message by message, with
// what a client reads in each: the tag, each ParameterStatus, a notice's and
// an error's code, a SHOW's column and value, and the transaction status.
func said(t *testing.T, r rawFrontend, q string) []string {
	t.Helper()
	r.fe.Send(&pgproto3.Query{String: q})
	if err := r.fe.Flush(); err != nil {
		t.Fatal(err)
	}
	var out []string
	for {
		m, err := r.fe.Receive()
		if err != nil {
			t.Fatalf("%s: after %v: %v", q, out, err)
		}
		switch x := m.(type) {
		case *pgproto3.ReadyForQuery:
			return append(out, "ready "+string(x.TxStatus))
		case *pgproto3.ParameterStatus:
			out = append(out, "status "+x.Name+"="+x.Value)
		case *pgproto3.ErrorResponse:
			out = append(out, "error "+x.Code)
		case *pgproto3.NoticeResponse:
			out = append(out, "notice "+x.Code)
		case *pgproto3.CommandComplete:
			out = append(out, string(x.CommandTag))
		case *pgproto3.RowDescription:
			var f []string
			for _, d := range x.Fields {
				f = append(f, fmt.Sprintf("%s:%d", d.Name, d.DataTypeOID))
			}
			out = append(out, "columns "+strings.Join(f, ","))
		case *pgproto3.DataRow:
			var cells []string
			for _, v := range x.Values {
				cells = append(cells, string(v))
			}
			out = append(out, "row "+strings.Join(cells, "|"))
		}
	}
}

func TestIntegrationPGRouterSettingsCancelInfinity(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	env, cert := startPGSettingsPort(t, ctx)
	portDSN := env.portDSN()
	sourceApp, portApp := env.sourceDSN, portDSN
	if strings.Contains(sourceApp, "?") {
		sourceApp += "&application_name=capapp"
	} else {
		sourceApp += "?application_name=capapp"
	}
	portApp += " application_name=capapp"

	// Every statement a client sends to set up its session, answered as
	// the source answers it: the same tags, ParameterStatus, notices, error
	// codes, SHOW values, and transaction status, in the same order.
	t.Run("settings", func(t *testing.T) {
		src := rawPGWith(t, ctx, sourceApp, nil)
		port := rawPGWith(t, ctx, portApp, nil)
		// PostgreSQL 18 reports search_path to the client when it changes (a
		// ParameterStatus after the SET or RESET), which no earlier version
		// does, and the port does not yet. Against a source of 18 or later
		// that one report of the source's is left out of the comparison:
		// search_path's and no other parameter's, so any other difference
		// still fails.
		sourceReportsSearchPath := pgSourceMajor(t, ctx, env.sourceDSN) >= 18
		for _, q := range []string{
			"SET TimeZone = 'America/Bogota'", "SET TimeZone = 'america/bogota'", "SHOW TimeZone",
			"SET TimeZone = 'utc'", "SET TimeZone = 'GMT-05:00'", "SET TIME ZONE LOCAL", "SHOW TIME ZONE",
			"SET application_name = 'añoé x'", "SHOW application_name",
			"SET application_name = 'PostgreSQL JDBC Driver'",
			"SET extra_float_digits = 3", "SET extra_float_digits = 2", "SHOW extra_float_digits", "SET extra_float_digits = 4",
			"SET DateStyle = 'ISO'", "SHOW DateStyle", "SET DateStyle = 'MDY'", "SET DateStyle = ISO, MDY", "SHOW DateStyle",
			"SET statement_timeout = 5000", "SHOW statement_timeout", "SET statement_timeout = '1.5s'", "SHOW statement_timeout",
			"SET statement_timeout = 60000", "SHOW statement_timeout", "SET statement_timeout = '500us'", "SHOW statement_timeout",
			"SET statement_timeout = -1", "SET statement_timeout = 0", "SHOW statement_timeout",
			"SET lock_timeout = '2s'", "SHOW lock_timeout",
			"SET client_encoding = 'unicode'", "SHOW client_encoding", "SET client_encoding = 'sql_ascii'", "SET client_encoding = 'UTF8'",
			"SET NAMES 'utf8'",
			"SET LOCAL TimeZone = 'Asia/Tokyo'",
			"BEGIN", "SET LOCAL TimeZone = 'Asia/Tokyo'", "SET application_name = 'inblock'", "COMMIT", "SHOW TimeZone",
			"BEGIN", "SET application_name = 'rolledback'", "ROLLBACK", "SHOW application_name",
			"BEGIN", "SET TimeZone = 'Asia/Kolkata'", "SET extra_float_digits = 9", "SHOW TimeZone", "ROLLBACK", "SHOW TimeZone",
			"RESET ALL", "SHOW application_name",
			"show transaction isolation level", "show standard_conforming_strings", "show max_identifier_length", "show integer_datetimes",
			"SET TRANSACTION ISOLATION LEVEL READ COMMITTED",
			"BEGIN ISOLATION LEVEL READ UNCOMMITTED", "show transaction isolation level", "COMMIT", "show transaction isolation level",
			"SET search_path = public", "SHOW search_path", "SET search_path TO DEFAULT", "SHOW search_path",
			"SET client_min_messages = warning", "SHOW client_min_messages", "RESET client_min_messages", "SHOW client_min_messages",
			"SET nosuch_param = 1", "SET server_version = '1'", "RESET ALL",
			"SET search_path = " + pgSetSchema, "select current_schema()", "SELECT count(*) FROM pg2769_t", "RESET search_path",
		} {
			s, p := said(t, src, q), said(t, port, q)
			if sourceReportsSearchPath {
				s = slices.DeleteFunc(s, func(line string) bool { return strings.HasPrefix(line, "status search_path=") })
			}
			if strings.Join(s, "\n") != strings.Join(p, "\n") {
				t.Errorf("%s\n  the port: %q\nthe source: %q", q, p, s)
			}
		}
		// SQLAlchemy reads the server's version from version() and fails the
		// connection when it does not start with PostgreSQL and a number.
		for _, q := range []string{"select pg_catalog.version()", "SELECT version()"} {
			got := said(t, port, q)
			if len(got) != 4 || got[0] != "columns version:25" || !strings.HasPrefix(got[1], "row PostgreSQL 14.0") {
				t.Errorf("%s: %q", q, got)
			}
		}
	})

	// What the port refuses, which the source takes, said with the reason
	// rather than taken and ignored.
	t.Run("refusals", func(t *testing.T) {
		port := rawPGWith(t, ctx, portDSN, nil)
		for q, code := range map[string]string{
			"SET DateStyle = 'ISO, DMY'":          "error 0A000",
			"SET IntervalStyle = 'sql_standard'":  "error 0A000",
			"SET bytea_output = 'escape'":         "error 0A000",
			"SET extra_float_digits = 0":          "error 0A000",
			"SET client_encoding = 'LATIN1'":      "error 0A000",
			"SET my.custom = 'x'":                 "error 42704",
			"SET search_path = nowhere":           "error 3F000",
			"SET search_path = s2769, public":     "error 0A000",
			"BEGIN ISOLATION LEVEL SERIALIZABLE":  "error 0A000",
			"SET standard_conforming_strings = 0": "error 0A000",
		} {
			if got := said(t, port, q); got[0] != code {
				t.Errorf("%s: %q, want %s", q, got, code)
			}
		}
	})

	// A timestamp with time zone, and what the copy computes from one in the
	// session's zone, read the same as on the source in every zone, in text
	// and (the instant itself) in binary. A wall clock and a date do not move.
	t.Run("time zones", func(t *testing.T) {
		src := rawPG(t, ctx, env.sourceDSN)
		port := rawPG(t, ctx, portDSN)
		finite := " FROM " + pgSetTable + " WHERE id < 100 ORDER BY id"
		textStmts := []string{
			"SELECT id, tstz" + finite,
			"SELECT id, tstz::date AS d" + finite,
			"SELECT id, CAST(tstz AS timestamp) AS w" + finite,
			"SELECT id, date_trunc('day', tstz) AS dt" + finite,
			"SELECT id, ts, day" + finite,
			"SELECT count(*) AS n FROM " + pgSetTable + " WHERE tstz >= '2026-03-29 02:00:00'",
		}
		binaryStmts := []string{"SELECT id, tstz, ts, day" + finite}
		// Where the copy's engine computes in the zone (a cast, date_trunc)
		// it uses its own zone data, which for Amsterdam before 1937 has no
		// local mean time (+00:19:32) where PostgreSQL's has one: row 4, in
		// 1900, is left out of those statements there. The text of the
		// instant itself is written by the port, from this process's zone
		// data, and is compared for every row.
		engineOff := map[string]string{"Europe/Amsterdam": "4|"}
		for _, zone := range []string{"UTC", "America/Bogota", "Asia/Kolkata", "Asia/Kathmandu", "Europe/Amsterdam", "GMT-05:00"} {
			set := "SET TimeZone = '" + zone + "'"
			if s, p := said(t, src, set), said(t, port, set); strings.Join(s, " ") != strings.Join(p, " ") {
				t.Fatalf("%s\n  the port: %q\nthe source: %q", set, p, s)
			}
			for format, stmts := range map[int16][]string{0: textStmts, 1: binaryStmts} {
				for _, stmt := range stmts {
					_, sRows, sErr := pgWireAnswer(t, src, stmt, format)
					_, pRows, pErr := pgWireAnswer(t, port, stmt, format)
					if sErr != "" || pErr != "" {
						t.Errorf("%s / %s (format %d): source %q, port %q", zone, stmt, format, sErr, pErr)
						continue
					}
					if off := engineOff[zone]; off != "" && (strings.Contains(stmt, "::date") || strings.Contains(stmt, "CAST(") || strings.Contains(stmt, "date_trunc")) {
						sRows, pRows = dropRow(sRows, off), dropRow(pRows, off)
					}
					if strings.Join(sRows, "\n") != strings.Join(pRows, "\n") {
						t.Errorf("%s / %s (format %d)\n  the port: %q\nthe source: %q", zone, stmt, format, pRows, sRows)
					}
				}
			}
		}
	})

	// infinity and -infinity read as the source writes them: the words in
	// text, the ends of the count in binary, in arrays too, in any zone.
	t.Run("infinity", func(t *testing.T) {
		src := rawPG(t, ctx, env.sourceDSN)
		port := rawPG(t, ctx, portDSN)
		inf := " FROM " + pgSetTable + " WHERE id >= 100 ORDER BY id"
		for _, zone := range []string{"UTC", "America/Bogota"} {
			said(t, src, "SET TimeZone = '"+zone+"'")
			said(t, port, "SET TimeZone = '"+zone+"'")
			for _, format := range []int16{0, 1} {
				for _, stmt := range []string{"SELECT id, tstz, ts, day, da" + inf, "SELECT max(day) AS d, min(tstz) AS t FROM " + pgSetTable} {
					sFields, sRows, sErr := pgWireAnswer(t, src, stmt, format)
					pFields, pRows, pErr := pgWireAnswer(t, port, stmt, format)
					if sErr != "" || pErr != "" {
						t.Errorf("%s / %s (format %d): source %q, port %q", zone, stmt, format, sErr, pErr)
						continue
					}
					if fmt.Sprint(sFields) != fmt.Sprint(pFields) {
						t.Errorf("%s (format %d) columns\n  the port: %v\nthe source: %v", stmt, format, pFields, sFields)
					}
					if strings.Join(sRows, "\n") != strings.Join(pRows, "\n") {
						t.Errorf("%s / %s (format %d)\n  the port: %q\nthe source: %q", zone, stmt, format, pRows, sRows)
					}
				}
			}
		}
	})

	// A cancel request stops a statement the copy is running, with the
	// source's words, and the connection goes on: from pgx, from psql (which
	// sends it in clear text even on a TLS connection), from JDBC's query
	// timeout over TLS and in clear text, and from psycopg.
	const long = "SELECT count(*) AS n FROM range(3000000000) t(i) WHERE i % 7 = 3"
	const sleep = "SELECT pg_sleep(30)"
	const canceled = "canceling statement due to user request"
	t.Run("cancel pgx", func(t *testing.T) {
		for _, mode := range []string{"disable", "require"} {
			conn, err := pgx.Connect(ctx, portDSN[:strings.LastIndex(portDSN, "sslmode=")]+"sslmode="+mode)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close(context.Background())
			qctx, qcancel := context.WithCancel(ctx)
			go func() { time.Sleep(time.Second); qcancel() }()
			started := time.Now()
			_, err = conn.Exec(qctx, long)
			qcancel()
			var pe *pgconn.PgError
			// pgx answers a canceled context by sending a cancel request
			// and closing; what the port said arrives first, or the
			// connection is gone.
			if errors.As(err, &pe) && (pe.Code != "57014" || pe.Message != canceled) {
				t.Fatalf("sslmode=%s: %v", mode, err)
			}
			if took := time.Since(started); took > 6*time.Second {
				t.Fatalf("sslmode=%s: the statement ran %s after its cancel", mode, took)
			}
			// Straight through the cancel request, on a connection that stays.
			conn2, err := pgx.Connect(ctx, portDSN[:strings.LastIndex(portDSN, "sslmode=")]+"sslmode="+mode)
			if err != nil {
				t.Fatal(err)
			}
			defer conn2.Close(context.Background())
			go func() { time.Sleep(time.Second); _ = conn2.PgConn().CancelRequest(ctx) }()
			_, err = conn2.PgConn().Exec(ctx, long).ReadAll()
			if !errors.As(err, &pe) || pe.Code != "57014" || pe.Message != canceled {
				t.Fatalf("sslmode=%s: a canceled statement: %v", mode, err)
			}
			var n int
			if err := conn2.QueryRow(ctx, "SELECT count(*) FROM "+pgSetTable).Scan(&n); err != nil || n != 9 {
				t.Fatalf("sslmode=%s: the connection after the cancel: %d %v", mode, n, err)
			}
		}
	})

	t.Run("cancel psql", func(t *testing.T) {
		psql, err := exec.LookPath("psql")
		if err != nil {
			t.Skip("psql is not installed here")
		}
		libpq := fmt.Sprintf("host=localhost hostaddr=%s port=%s user=anyone password=tok dbname=pgsrv sslmode=require sslrootcert=%s", env.host, env.port, cert.CAFile)
		run := func(dsn, stmt string) string {
			cmd := exec.CommandContext(ctx, psql, dsn, "-X", "-c", stmt)
			var out strings.Builder
			cmd.Stdout, cmd.Stderr = &out, &out
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			time.Sleep(2 * time.Second)
			_ = cmd.Process.Signal(os.Interrupt)
			done := make(chan error, 1)
			go func() { done <- cmd.Wait() }()
			select {
			case <-done:
			case <-time.After(15 * time.Second):
				_ = cmd.Process.Kill()
				t.Fatalf("psql did not end after its cancel:\n%s", out.String())
			}
			return out.String()
		}
		s := run(env.sourceDSN, sleep)
		p := run(libpq, long)
		if !strings.Contains(s, "ERROR:  "+canceled) || !strings.Contains(p, "ERROR:  "+canceled) {
			t.Fatalf("psql\n  the port: %s\nthe source: %s", p, s)
		}
		// statement_timeout, in the source's words too.
		for _, c := range []struct{ dsn, stmt string }{{env.sourceDSN, sleep}, {libpq, long}} {
			out, err := exec.CommandContext(ctx, psql, c.dsn, "-X", "-c", "SET statement_timeout = 500", "-c", c.stmt).CombinedOutput()
			if err == nil || !strings.Contains(string(out), "ERROR:  canceling statement due to statement timeout") {
				t.Fatalf("statement_timeout through psql against %s: %v\n%s", c.dsn, err, out)
			}
		}
	})

	t.Run("cancel jdbc", func(t *testing.T) {
		jar := os.Getenv("BINTRAIL_PGJDBC_JAR")
		java, err := exec.LookPath("java")
		if jar == "" || err != nil {
			if os.Getenv("BINTRAIL_REQUIRE_JDBC") != "" {
				t.Fatal("java or BINTRAIL_PGJDBC_JAR is missing, and BINTRAIL_REQUIRE_JDBC says they must be there")
			}
			t.Skip("JDBC is not set up here (java and BINTRAIL_PGJDBC_JAR)")
		}
		src := filepath.Join(t.TempDir(), "Cancel.java")
		if err := os.WriteFile(src, []byte(jdbcCancelProgram), 0o600); err != nil {
			t.Fatal(err)
		}
		u, err := url.Parse(env.sourceDSN)
		if err != nil {
			t.Fatal(err)
		}
		pw, _ := u.User.Password()
		sourceURL := fmt.Sprintf("jdbc:postgresql://%s%s?user=%s&password=%s&sslmode=disable", u.Host, u.Path, url.QueryEscape(u.User.Username()), url.QueryEscape(pw))
		run := func(jdbcURL, stmt string) string {
			out, err := exec.CommandContext(ctx, java, "-Duser.timezone=America/Bogota", "-cp", jar, src, jdbcURL, stmt, pgSetTable).CombinedOutput()
			if err != nil {
				t.Fatalf("JDBC against %s: %v\n%s", jdbcURL, err, out)
			}
			return string(out)
		}
		want := run(sourceURL, sleep)
		for _, mode := range []string{"disable", "require"} {
			portURL := fmt.Sprintf("jdbc:postgresql://%s:%s/pgsrv?user=anyone&password=tok&sslmode=%s&sslfactory=org.postgresql.ssl.NonValidatingFactory", env.host, env.port, mode)
			compareLines(t, "JDBC sslmode="+mode, want, run(portURL, long))
		}
	})

	t.Run("cancel psycopg", func(t *testing.T) {
		python, err := exec.LookPath("python3")
		if err == nil {
			err = exec.CommandContext(ctx, python, "-c", "import psycopg").Run()
		}
		if err != nil {
			if os.Getenv("BINTRAIL_REQUIRE_PSYCOPG") != "" {
				t.Fatal("psycopg 3 is not installed, and BINTRAIL_REQUIRE_PSYCOPG says it must be")
			}
			t.Skip("psycopg 3 is not installed here")
		}
		script := filepath.Join(t.TempDir(), "cancel.py")
		if err := os.WriteFile(script, []byte(psycopgCancelScript), 0o600); err != nil {
			t.Fatal(err)
		}
		run := func(dsn, stmt string) string {
			out, err := exec.CommandContext(ctx, python, script, dsn, stmt).CombinedOutput()
			if err != nil {
				t.Fatalf("psycopg against %s: %v\n%s", dsn, err, out)
			}
			return string(out)
		}
		s, p := run(env.sourceDSN, sleep), run(portDSN, long)
		if s != p {
			t.Fatalf("psycopg\n  the port: %s\nthe source: %s", p, s)
		}
	})

	// SQLAlchemy (and Superset on it) connects: it reads version(),
	// current_schema() and two SHOWs before anything else.
	t.Run("sqlalchemy", func(t *testing.T) {
		python, err := exec.LookPath("python3")
		if err == nil {
			err = exec.CommandContext(ctx, python, "-c", "import sqlalchemy, psycopg2").Run()
		}
		if err != nil {
			t.Skip("SQLAlchemy with psycopg2 is not installed here")
		}
		script := filepath.Join(t.TempDir(), "sa.py")
		if err := os.WriteFile(script, []byte(sqlalchemyConnectScript), 0o600); err != nil {
			t.Fatal(err)
		}
		dsn := fmt.Sprintf("postgresql+psycopg2://anyone:tok@%s:%s/pgsrv?sslmode=disable", env.host, env.port)
		out, err := exec.CommandContext(ctx, python, script, dsn, pgSetTable).CombinedOutput()
		if err != nil || strings.TrimSpace(string(out)) != "(14, 0) 9" {
			t.Fatalf("SQLAlchemy: %v\n%s", err, out)
		}
	})
}

// jdbcCancelProgram runs a statement with a query timeout of one second,
// which JDBC carries out by sending a cancel request, and prints the
// SQLSTATE and message it got; then the connection's next statement.
const jdbcCancelProgram = `import java.sql.*;
public class Cancel {
  public static void main(String[] a) throws Exception {
    try (Connection c = DriverManager.getConnection(a[0])) {
      try (Statement s = c.createStatement()) {
        s.setQueryTimeout(1);
        s.executeQuery(a[1]);
        System.out.println("not canceled");
      } catch (SQLException e) {
        System.out.println(e.getSQLState() + " " + e.getMessage());
      }
      try (Statement s = c.createStatement(); ResultSet r = s.executeQuery("SELECT count(*) FROM " + a[2])) {
        r.next();
        System.out.println("after: " + r.getLong(1));
      }
      try (Statement s = c.createStatement(); ResultSet r = s.executeQuery("SHOW TimeZone")) {
        r.next();
        System.out.println("zone: " + r.getString(1));
      }
      try (Statement s = c.createStatement(); ResultSet r = s.executeQuery("SHOW application_name")) {
        r.next();
        System.out.println("application_name: " + r.getString(1));
      }
      try (Statement s = c.createStatement(); ResultSet r = s.executeQuery("SELECT tstz FROM " + a[2] + " WHERE id = 1")) {
        r.next();
        System.out.println("tstz: " + r.getObject(1) + " " + r.getString(1));
      }
    }
  }
}
`

// psycopgCancelScript cancels a statement from another thread after a
// second, and prints what came back, then the connection's next answer.
const psycopgCancelScript = `import sys, threading, psycopg
with psycopg.connect(sys.argv[1]) as c:
    threading.Timer(1.0, c.cancel).start()
    try:
        c.execute(sys.argv[2])
        print("not canceled")
    except psycopg.errors.QueryCanceled as e:
        print("57014", str(e).strip())
    c.rollback()
    print("after:", c.execute("SELECT 1").fetchone()[0])
`

// sqlalchemyConnectScript connects as SQLAlchemy does, and prints the
// server version it read and the table's row count.
const sqlalchemyConnectScript = `import sys, sqlalchemy as sa
e = sa.create_engine(sys.argv[1])
with e.connect() as c:
    n = c.execute(sa.text("SELECT count(*) FROM " + sys.argv[2])).scalar()
    print(e.dialect.server_version_info, n)
`

// dropRow leaves out the rows that start with prefix.
func dropRow(rows []string, prefix string) []string {
	var out []string
	for _, r := range rows {
		if !strings.HasPrefix(r, prefix) {
			out = append(out, r)
		}
	}
	return out
}

// pgSourceMajor is the major version of the PostgreSQL at dsn (18 for 18.1).
func pgSourceMajor(t *testing.T, ctx context.Context, dsn string) int {
	t.Helper()
	c, err := pgconn.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connecting to the source for its version: %v", err)
	}
	defer c.Close(ctx)
	v := c.ParameterStatus("server_version")
	major, err := strconv.Atoi(strings.SplitN(strings.SplitN(v, ".", 2)[0], " ", 2)[0])
	if err != nil {
		t.Fatalf("the source's server_version is %q", v)
	}
	return major
}
