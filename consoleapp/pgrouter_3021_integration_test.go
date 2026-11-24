//go:build integration

package consoleapp

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/pgshim"
)

// SHOW dbtrail.router_status on the PostgreSQL port (#3021), against a real
// PostgreSQL: the port says which side answered the connection's last
// statement and why, answers it itself in every state of the connection,
// and never sends it to the source.

const pgStatusStmt = "SHOW dbtrail.router_status"

// pgStatus runs the status statement and returns its rows by name, and in
// the order shown as "name=value".
func pgStatus(t *testing.T, ctx context.Context, c *pgconn.PgConn) (map[string]string, []string) {
	t.Helper()
	res, err := c.Exec(ctx, pgStatusStmt).ReadAll()
	if err != nil {
		t.Fatalf("%s: %v", pgStatusStmt, err)
	}
	if len(res) != 1 || res[0].Err != nil {
		t.Fatalf("%s: %+v", pgStatusStmt, res)
	}
	if names := fmt.Sprint(res[0].FieldDescriptions[0].Name, res[0].FieldDescriptions[1].Name); names != "namevalue" || res[0].CommandTag.String() != "SHOW" {
		t.Fatalf("%s: columns %s, tag %s", pgStatusStmt, names, res[0].CommandTag)
	}
	by, order := map[string]string{}, []string{}
	for _, r := range res[0].Rows {
		by[string(r[0])] = string(r[1])
		order = append(order, string(r[0])+"="+string(r[1]))
	}
	return by, order
}

func TestIntegrationPGRouterStatusStatement(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	env, source := pgRouteEnv(t, ctx)
	count := "SELECT count(*) FROM " + pgRouteTable
	// counted is how many statements the port's tally holds for the server.
	counted := func() uint64 {
		n := env.routes.doc("").Servers[env.serverID]
		return n.Copy + n.Source + n.Refused
	}

	t.Run("a connection with a forwarding account", func(t *testing.T) {
		c := pgClient(t, ctx, env.portDSN(), nil)
		before := counted()

		// The first statement of the connection: nothing answered yet.
		st, order := pgStatus(t, ctx, c)
		t.Logf("before any statement:\n%s", strings.Join(order, "\n"))
		if st["server"] != env.serverID || st["forwarding"] != "on" || st["sql_mode"] != "fast" || st["last_answer"] != "none" {
			t.Errorf("before any statement: %v", order)
		}
		if _, err := time.Parse(time.RFC3339, st["copy_snapshot"]); err != nil {
			t.Errorf("copy_snapshot %q is not an instant: %v", st["copy_snapshot"], order)
		}
		if n, err := strconv.Atoi(st["copy_age_seconds"]); err != nil || n < 0 {
			t.Errorf("copy_age_seconds %q", st["copy_age_seconds"])
		}
		if _, err := time.Parse(time.RFC3339, st["counting_since"]); err != nil {
			t.Errorf("counting_since %q", st["counting_since"])
		}
		for _, absent := range []string{"last_answer_reason", "last_answer_error", "last_answer_age_seconds", "forwarding_off_reason"} {
			if v, ok := st[absent]; ok {
				t.Errorf("%s=%q before any statement", absent, v)
			}
		}

		// The copy holds three rows and the source four: the answer says
		// which side gave it, and the status must say the same.
		got := one(ctx, c, count)
		st, order = pgStatus(t, ctx, c)
		t.Logf("after %s answered %s:\n%s", count, got, strings.Join(order, "\n"))
		switch got {
		case "3":
			if st["last_answer"] != "copy" || st["last_answer_error"] != "no" || st["last_answer_reason"] == "" {
				t.Errorf("the copy answered, and the status says: %v", order)
			}
			// Fast mode: the answer is as old as the snapshot it read,
			// which its file says (#3125): a minute before the full read
			// began, a moment ago.
			age, err := strconv.Atoi(st["last_answer_age_seconds"])
			if _, perr := time.Parse(time.RFC3339, st["last_answer_indexed_through"]); err != nil || perr != nil || age < 60 || age > 600 || st["last_answer_age_unknown_reason"] != "" {
				t.Errorf("the copy's answer's age: %v", order)
			}
		case "4":
			if st["last_answer"] != "source" || st["last_answer_error"] != "no" {
				t.Errorf("the source answered, and the status says: %v", order)
			}
		default:
			t.Fatalf("%s: %q", count, got)
		}

		for _, tc := range []struct{ stmt, want, side, reason, failed string }{
			{count + " WHERE now() IS NOT NULL", "4", "source", pgshim.ReasonVeto, "no"},
			{"SELECT count(*) FROM " + pgRouteSource, "1", "source", pgshim.ReasonTableNotOnCopy, "no"},
			{"INSERT INTO " + pgRouteTable + " VALUES (9)", "error 25006", "refused", pgshim.ReasonReadOnly, "yes"},
			{"SELECT 1/0 FROM " + pgRouteTable + " WHERE now() IS NOT NULL", "error 22012", "source", pgshim.ReasonVeto, "yes"},
		} {
			if got := one(ctx, c, tc.stmt); !strings.HasPrefix(got, tc.want) {
				t.Errorf("%s: %q, want %q", tc.stmt, got, tc.want)
			}
			st, order = pgStatus(t, ctx, c)
			t.Logf("after %s:\n%s", tc.stmt, strings.Join(order, "\n"))
			if st["last_answer"] != tc.side || st["last_answer_reason"] != tc.reason || st["last_answer_error"] != tc.failed || st["last_answer_reason_means"] == "" {
				t.Errorf("after %s: %v", tc.stmt, order)
			}
			if _, ok := st["last_answer_age_seconds"]; ok {
				t.Errorf("an age for an answer the copy did not give: %v", order)
			}
		}

		// Five statements reached a side; the eight status statements
		// between them are in no count, and the status shows the counts.
		if n := counted() - before; n != 5 {
			t.Errorf("the tally grew by %d statements, want 5", n)
		}
		var shown uint64
		for _, k := range []string{"statements_copy", "statements_source", "statements_refused"} {
			n, err := strconv.ParseUint(st[k], 10, 64)
			if err != nil {
				t.Errorf("%s=%q", k, st[k])
			}
			shown += n
		}
		if shown != counted() {
			t.Errorf("the status shows %d statements, the tally holds %d: %v", shown, counted(), order)
		}
		if st["reason_"+pgshim.ReasonReadOnly] == "" || st["reason_"+pgshim.ReasonVeto] == "" {
			t.Errorf("no count per reason: %v", order)
		}
	})

	t.Run("inside a block and a failed one", func(t *testing.T) {
		c := pgClient(t, ctx, env.portDSN(), nil)
		one(ctx, c, "BEGIN")
		if got := one(ctx, c, count); got != "4" {
			t.Fatalf("in a block: %q", got)
		}
		st, order := pgStatus(t, ctx, c)
		if st["last_answer"] != "source" || st["last_answer_reason"] != pgshim.ReasonInTransaction || c.TxStatus() != 'T' {
			t.Errorf("in a block (status %c): %v", c.TxStatus(), order)
		}
		if got := one(ctx, c, "SELECT 1/0"); !strings.HasPrefix(got, "error 22012") || c.TxStatus() != 'E' {
			t.Fatalf("a failed statement in a block: %q, status %c", got, c.TxStatus())
		}
		st, order = pgStatus(t, ctx, c)
		t.Logf("in a failed block:\n%s", strings.Join(order, "\n"))
		if st["last_answer"] != "source" || st["last_answer_reason"] != pgshim.ReasonInTransaction || st["last_answer_error"] != "yes" {
			t.Errorf("in a failed block: %v", order)
		}
		if c.TxStatus() != 'E' {
			t.Errorf("the status statement moved the failed block to %c", c.TxStatus())
		}
		if got := one(ctx, c, count); !strings.HasPrefix(got, "error 25P02") {
			t.Errorf("another statement in the failed block: %q", got)
		}
		if got := one(ctx, c, "ROLLBACK"); got != "ROLLBACK" || c.TxStatus() != 'I' {
			t.Errorf("ROLLBACK: %q, status %c", got, c.TxStatus())
		}
		if got := one(ctx, c, count+" WHERE now() IS NOT NULL"); got != "4" {
			t.Errorf("after the block: %q", got)
		}
	})

	t.Run("it is never sent to the source", func(t *testing.T) {
		c := pgClient(t, ctx, env.portDSN(), nil)
		before := counted()
		// The source refuses the name with 42704 (it is no setting of
		// PostgreSQL's), and runs a message of several statements whole:
		// neither answer may come back from here.
		for stmt, want := range map[string]string{
			"SELECT 1; " + pgStatusStmt:                       "error 0A000",
			pgStatusStmt + "; SELECT 1":                       "error 0A000",
			"SHOW ROUTER STATUS":                              "error 42601",
			"SHOW ROUTER STATUS; SELECT 1":                    "error 0A000",
			"SELECT current_setting('dbtrail.router_status')": "error 42704",
			"SET dbtrail.router_status = 'x'":                 "error 42704",
		} {
			if got := one(ctx, c, stmt); !strings.HasPrefix(got, want) {
				t.Errorf("%s: %q, want %s", stmt, got, want)
			}
		}
		_, err := c.Exec(ctx, "SHOW ROUTER STATUS").ReadAll()
		var pe *pgconn.PgError
		if !errors.As(err, &pe) || !strings.Contains(pe.Message, pgStatusStmt) {
			t.Errorf("the MySQL port's spelling: %v, want the statement to send named", err)
		} else {
			t.Logf("SHOW ROUTER STATUS answers: ERROR: %s", pe.Message)
		}
		st, order := pgStatus(t, ctx, c)
		if st["last_answer"] != "none" {
			t.Errorf("after refusals the port made itself: %v", order)
		}
		if n := counted() - before; n != 0 {
			t.Errorf("the tally grew by %d statements, want none", n)
		}
		// As the forwarding account, the source saw none of them.
		if n := one(ctx, source, "SELECT count(*) FROM pg_stat_activity WHERE usename = '"+pgRouteRole+"' AND query ILIKE '%router%' AND pid <> pg_backend_pid()"); n != "0" {
			t.Errorf("%s statements about the router on the source", n)
		}
	})

	t.Run("pgx, in every mode", func(t *testing.T) {
		for _, mode := range []pgx.QueryExecMode{pgx.QueryExecModeCacheStatement, pgx.QueryExecModeCacheDescribe, pgx.QueryExecModeDescribeExec, pgx.QueryExecModeExec, pgx.QueryExecModeSimpleProtocol} {
			conn := pgxConn(t, ctx, env.portDSN(), mode)
			if got := pgxAnswer(ctx, conn, pgStatusStmt); !strings.HasPrefix(got, "[name value] ") || !strings.Contains(got, `"last_answer|none"`) {
				t.Errorf("mode %v, before any statement: %s", mode, got)
			}
			if got := pgxAnswer(ctx, conn, count+" WHERE now() IS NOT NULL AND id > $1", 0); got != `[count] ["4"]` {
				t.Errorf("mode %v: the source's answer: %s", mode, got)
			}
			// Twice: a prepared status statement answers what is true now.
			for i := range 2 {
				if got := pgxAnswer(ctx, conn, pgStatusStmt); !strings.Contains(got, `"last_answer|source"`) || !strings.Contains(got, `"last_answer_reason|veto"`) {
					t.Errorf("mode %v, run %d: %s", mode, i, got)
				}
			}
			conn.Close(ctx)
		}
	})

	t.Run("psql, psycopg and JDBC", func(t *testing.T) {
		if psql, err := exec.LookPath("psql"); err == nil {
			out, err := exec.CommandContext(ctx, psql, "-X", "-v", "ON_ERROR_STOP=1",
				"-c", count+" WHERE now() IS NOT NULL", "-c", pgStatusStmt+";", env.portDSN()).CombinedOutput()
			t.Logf("psql:\n%s", out)
			if err != nil || !strings.Contains(string(out), "last_answer") || !strings.Contains(string(out), "| source") || !strings.Contains(string(out), "| veto") {
				t.Errorf("psql: %v %s", err, out)
			}
		} else {
			t.Log("psql is not installed here")
		}
		const want = "none source veto source veto"
		python, perr := exec.LookPath("python3")
		if perr == nil {
			perr = exec.CommandContext(ctx, python, "-c", "import psycopg").Run()
		}
		if perr != nil {
			t.Log("psycopg 3 is not installed here")
		} else {
			script := filepath.Join(t.TempDir(), "status.py")
			if err := os.WriteFile(script, []byte(psycopgStatusScript), 0o600); err != nil {
				t.Fatal(err)
			}
			out, err := exec.CommandContext(ctx, python, script, env.portDSN()).CombinedOutput()
			if got := strings.TrimSpace(string(out)); err != nil || got != want {
				t.Errorf("psycopg: %v %q, want %q", err, got, want)
			}
		}
		jar := os.Getenv("BINTRAIL_PGJDBC_JAR")
		java, jerr := exec.LookPath("java")
		if jar == "" || jerr != nil {
			if os.Getenv("BINTRAIL_REQUIRE_JDBC") != "" {
				t.Fatal("java or BINTRAIL_PGJDBC_JAR is missing")
			}
			t.Log("JDBC is not set up here")
			return
		}
		src := filepath.Join(t.TempDir(), "Status.java")
		if err := os.WriteFile(src, []byte(jdbcStatusProgram), 0o600); err != nil {
			t.Fatal(err)
		}
		jdbcURL := fmt.Sprintf("jdbc:postgresql://%s:%s/pgsrv?user=anyone&password=tok&sslmode=disable", env.host, env.port)
		out, err := exec.CommandContext(ctx, java, "-cp", jar, src, jdbcURL).CombinedOutput()
		if got := strings.TrimSpace(string(out)); err != nil || got != want {
			t.Errorf("JDBC: %v %q, want %q", err, got, want)
		}
	})
}

// A server with no source: refused at login, with the cause.
func TestIntegrationPGRouterStatusStatementWithoutASource(t *testing.T) {
	// Since 0.140.0 a server the port cannot forward for is not served at
	// all: there is no connection to ask the status on, and the login says
	// why instead.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := startPGPortFor(t, ctx, nil, pgPortFixture{table: pgExtTable, slot: pgExtSlot, pub: pgExtPub, noForward: true, setup: []string{
		"CREATE TABLE " + pgExtTable + " (id int PRIMARY KEY)", "INSERT INTO " + pgExtTable + " VALUES (1)"}})
	c, err := pgconn.Connect(ctx, env.portDSN())
	if err == nil {
		c.Close(ctx)
		t.Fatal("a server with no source was served")
	}
	var pe *pgconn.PgError
	if !errors.As(err, &pe) || pe.Code != "08004" || pe.Message != console.PGPortNotServed("pgsrv", console.PGRouteRefusedNoSource) {
		t.Fatalf("the login: %v", err)
	}
}

const psycopgStatusScript = `import sys
import psycopg

with psycopg.connect(sys.argv[1], autocommit=True) as conn:
    def status(**kw):
        rows = dict(conn.execute("` + pgStatusStmt + `", **kw).fetchall())
        return rows["last_answer"], rows.get("last_answer_reason")
    out = [status()[0]]
    conn.execute("SELECT count(*) FROM ` + pgRouteTable + ` WHERE now() IS NOT NULL AND id > %s", (0,)).fetchone()
    out += status()
    out += status(prepare=True)
    print(" ".join(out))
`

const jdbcStatusProgram = `import java.sql.*;
import java.util.*;

public class Status {
    static Map<String, String> status(Connection c, boolean prepared) throws SQLException {
        Map<String, String> m = new HashMap<>();
        if (prepared) {
            try (PreparedStatement ps = c.prepareStatement("` + pgStatusStmt + `"); ResultSet rs = ps.executeQuery()) {
                while (rs.next()) m.put(rs.getString("name"), rs.getString("value"));
            }
        } else {
            try (Statement s = c.createStatement(); ResultSet rs = s.executeQuery("` + pgStatusStmt + `")) {
                while (rs.next()) m.put(rs.getString(1), rs.getString(2));
            }
        }
        return m;
    }
    public static void main(String[] args) throws Exception {
        try (Connection c = DriverManager.getConnection(args[0])) {
            String first = status(c, false).get("last_answer");
            try (PreparedStatement ps = c.prepareStatement("SELECT count(*) FROM ` + pgRouteTable + ` WHERE now() IS NOT NULL AND id > ?")) {
                ps.setInt(1, 0);
                try (ResultSet rs = ps.executeQuery()) { rs.next(); }
            }
            Map<String, String> a = status(c, false);
            Map<String, String> b = new HashMap<>();
            for (int i = 0; i < 6; i++) b = status(c, true); // past prepareThreshold: a named statement too
            System.out.println(first + " " + a.get("last_answer") + " " + a.get("last_answer_reason") + " " + b.get("last_answer") + " " + b.get("last_answer_reason"));
        }
    }
}
`
