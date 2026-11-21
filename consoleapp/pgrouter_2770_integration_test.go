//go:build integration

package consoleapp

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/hiderules"
	"github.com/dbtrail/dbtrail/internal/pgshim"
)

// Per-statement routing on the PostgreSQL port (#2770), against a real
// PostgreSQL: the copy answers what it is known to answer as PostgreSQL
// does, the source the rest, and a server that hides tables or columns sends
// nothing to its source.

const (
	pgRouteTable  = "pgroute_t"
	pgRouteSource = "pgroute_src"
	pgRouteRole   = "dbtrail_fwd_2770"
	pgRoutePass   = "fwd-pw-2770"
)

// pgRouteEnv is a port whose server has a forwarding account. The copy holds
// rows 1 to 3 of pgroute_t; the source holds a fourth, and pgroute_src,
// which the copy does not: an answer says which side gave it.
func pgRouteEnv(t *testing.T, ctx context.Context) (pgExtEnv, *pgconn.PgConn) {
	t.Helper()
	env := startPGPortFor(t, ctx, nil, pgPortFixture{table: pgRouteTable, slot: "pgroute_slot", pub: "pgroute_pub",
		setup: []string{
			"CREATE TABLE " + pgRouteTable + " (id int PRIMARY KEY, name text, amount numeric(12,2), free numeric, at int, secret text)",
			"INSERT INTO " + pgRouteTable + ` VALUES (1, 'bruno', 10.50, 1.5, 1, 's3cr3t-1'), (2, 'Ana', 0.25, 2, 2, 's3cr3t-2'), (3, 'carla', 3.00, 3.25, 3, 's3cr3t-3')`,
		},
		entry: func(e *console.ServerEntry) {
			e.SourceDSN, e.SourceSlot, e.SourcePublication = pgFwdSourceDSN(t), "pgroute_slot", "pgroute_pub"
			e.RouteDSN = withAccount(t, e.SourceDSN, pgRouteRole, pgRoutePass)
		}})
	source, err := pgconn.Connect(ctx, env.sourceDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { source.Close(context.Background()) })
	exec1 := func(stmt string) {
		t.Helper()
		if _, err := source.Exec(ctx, stmt).ReadAll(); err != nil {
			t.Fatalf("source: %s: %v", stmt, err)
		}
	}
	exec1("INSERT INTO " + pgRouteTable + " VALUES (4, 'Dario', 7.75, 4, 0, 's3cr3t-4')")
	exec1("DROP TABLE IF EXISTS " + pgRouteSource)
	exec1("CREATE TABLE " + pgRouteSource + " (id int PRIMARY KEY)")
	exec1("INSERT INTO " + pgRouteSource + " VALUES (1)")
	exec1(`DO $$ BEGIN IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '` + pgRouteRole + `') THEN EXECUTE 'DROP OWNED BY ` + pgRouteRole + `'; END IF; END $$`)
	exec1("DROP ROLE IF EXISTS " + pgRouteRole)
	exec1("CREATE ROLE " + pgRouteRole + " LOGIN PASSWORD '" + pgRoutePass + "'")
	// The account reads everything, the hidden column and table included:
	// what keeps them hidden must not be its grants.
	exec1("GRANT SELECT ON " + pgRouteTable + ", " + pgRouteSource + " TO " + pgRouteRole)
	t.Cleanup(func() {
		bg := context.Background()
		for _, s := range []string{"DROP TABLE IF EXISTS " + pgRouteSource, "DROP OWNED BY " + pgRouteRole, "DROP ROLE IF EXISTS " + pgRouteRole} {
			_, _ = source.Exec(bg, s).ReadAll()
		}
	})
	return env, source
}

func TestIntegrationPGRouterRouting(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	env, source := pgRouteEnv(t, ctx)
	count := "SELECT count(*) FROM " + pgRouteTable

	t.Run("each statement goes to the side that answers it as PostgreSQL does", func(t *testing.T) {
		c := pgClient(t, ctx, env.portDSN(), nil)
		// What the copy answers first: once the source answered this
		// connection, the copy's older answers are not given to it (#2985).
		for _, tc := range []struct{ stmt, want string }{
			{count, "3"},
			// at is a reserved word on the copy, which the port quotes there
			// (#2973): the copy answers it, from its rows.
			{"SELECT min(at) FROM " + pgRouteTable, "1"},
			{count + " WHERE now() IS NOT NULL", "4"},
			{"SELECT count(*) FROM " + pgRouteSource, "1"},
			// An array written as a string: PostgreSQL reads it as the
			// column's type, the copy cannot and fails, and the source
			// answers it with its fourth row (the copy would say 2).
			{"SELECT count(*) FROM " + pgRouteTable + " WHERE id = ANY('{1,2,4}')", "3"},
			// A numeric without precision: written as PostgreSQL writes it.
			{"SELECT free FROM " + pgRouteTable + " WHERE id = 1", "1.5"},
			{"SELECT avg(amount) FROM " + pgRouteTable, one(ctx, source, "SELECT avg(amount) FROM "+pgRouteTable)},
			// Text by order: the source's collation.
			{"SELECT name FROM " + pgRouteTable + " ORDER BY name DESC LIMIT 1", one(ctx, source, "SELECT name FROM "+pgRouteTable+" ORDER BY name DESC LIMIT 1")},
			{"SELECT current_user", pgRouteRole},
		} {
			if got := one(ctx, c, tc.stmt); got != tc.want {
				t.Errorf("%s: %q, want %q", tc.stmt, got, tc.want)
			}
		}
		if got := one(ctx, c, "INSERT INTO "+pgRouteTable+" VALUES (9)"); !strings.HasPrefix(got, "error 25006") {
			t.Errorf("a write: %q", got)
		}
		if got := one(ctx, c, "EXPLAIN SELECT 1"); !strings.HasPrefix(got, "Result") {
			t.Errorf("EXPLAIN: %q, want the source's plan", got)
		}
		one(ctx, c, "SET TimeZone = 'America/Bogota'")
		if got := one(ctx, c, count); got != "4" {
			t.Errorf("under another time zone: %q, want the source's 4", got)
		}
		one(ctx, c, "RESET TimeZone")
		// Back in UTC the screen gives it to the copy again, whose 3 is older
		// than the 4 the source showed this connection: the source answers
		// (#2985).
		tooOld := env.routes.doc("").Servers[env.serverID].Reasons[pgshim.ReasonCopyTooOld]
		if got := one(ctx, c, count); got != "4" {
			t.Errorf("back in UTC: %q, want the source's 4", got)
		}
		if n := env.routes.doc("").Servers[env.serverID].Reasons[pgshim.ReasonCopyTooOld]; n != tooOld+1 {
			t.Errorf("back in UTC: copy_too_old %d, want %d: the copy was not chosen first", n, tooOld+1)
		}
		one(ctx, c, "BEGIN")
		if got := one(ctx, c, count); got != "4" {
			t.Errorf("in a block: %q", got)
		}
		one(ctx, c, "COMMIT")
		if c.TxStatus() != 'I' {
			t.Errorf("status %c", c.TxStatus())
		}
	})

	t.Run("pgx, in every mode", func(t *testing.T) {
		for _, mode := range []pgx.QueryExecMode{pgx.QueryExecModeCacheStatement, pgx.QueryExecModeCacheDescribe, pgx.QueryExecModeDescribeExec, pgx.QueryExecModeExec, pgx.QueryExecModeSimpleProtocol} {
			conn := pgxConn(t, ctx, env.portDSN(), mode)
			for _, tc := range []struct {
				stmt string
				want string
			}{
				// The copy's first (#2985).
				{"SELECT count(*) AS n FROM " + pgRouteTable + " WHERE id > $1", `[n] ["3"]`},
				{"SELECT min(at) AS m FROM " + pgRouteTable + " WHERE id > $1", `[m] ["1"]`},
				{"SELECT count(*) AS n FROM " + pgRouteTable + " WHERE now() IS NOT NULL AND id > $1", `[n] ["4"]`},
				{"SELECT count(*) AS n FROM " + pgRouteSource + " WHERE id > $1", `[n] ["1"]`},
				{"SELECT free FROM " + pgRouteTable + " WHERE id = $1", `[free] ["1.5"]`},
			} {
				arg := 0
				if strings.Contains(tc.stmt, "id = $1") {
					arg = 1
				}
				// Twice: a prepared statement keeps its side.
				for i := range 2 {
					if got := pgxAnswer(ctx, conn, tc.stmt, arg); got != tc.want {
						t.Errorf("mode %v, run %d: %s: %s, want %s", mode, i, tc.stmt, got, tc.want)
					}
				}
			}
			conn.Close(ctx)
		}
	})

	t.Run("psql, psycopg and JDBC", func(t *testing.T) {
		const want = "copy=3 source=4 missing=1 at=1 free=1.5"
		if psql, err := exec.LookPath("psql"); err == nil {
			out, err := exec.CommandContext(ctx, psql, "-X", "-At", "-v", "ON_ERROR_STOP=1",
				"-c", count, "-c", "SELECT min(at) FROM "+pgRouteTable, "-c", count+" WHERE now() IS NOT NULL",
				"-c", "SELECT count(*) FROM "+pgRouteSource, "-c", "SELECT free FROM "+pgRouteTable+" WHERE id = 1",
				env.portDSN()).CombinedOutput()
			if got := strings.Join(strings.Fields(string(out)), " "); err != nil || got != "3 1 4 1 1.5" {
				t.Errorf("psql: %v %q", err, out)
			}
		} else {
			t.Log("psql is not installed here")
		}
		python, perr := exec.LookPath("python3")
		if perr == nil {
			perr = exec.CommandContext(ctx, python, "-c", "import psycopg").Run()
		}
		if perr != nil {
			t.Log("psycopg 3 is not installed here")
		} else {
			script := filepath.Join(t.TempDir(), "route.py")
			if err := os.WriteFile(script, []byte(psycopgRouteScript), 0o600); err != nil {
				t.Fatal(err)
			}
			out, err := exec.CommandContext(ctx, python, script, env.portDSN()).CombinedOutput()
			if got := strings.TrimSpace(string(out)); err != nil || got != want {
				t.Errorf("psycopg: %v %q", err, got)
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
		src := filepath.Join(t.TempDir(), "Route.java")
		if err := os.WriteFile(src, []byte(jdbcRouteProgram), 0o600); err != nil {
			t.Fatal(err)
		}
		jdbcURL := fmt.Sprintf("jdbc:postgresql://%s:%s/pgsrv?user=anyone&password=tok&sslmode=disable", env.host, env.port)
		out, err := exec.CommandContext(ctx, java, "-cp", jar, src, jdbcURL).CombinedOutput()
		if got := strings.TrimSpace(string(out)); err != nil || got != want {
			t.Errorf("JDBC: %v %q", err, got)
		}
	})

	t.Run("the status says which side answered and why", func(t *testing.T) {
		rec := httptest.NewRecorder()
		pgRouterStatusHandler(env.routes, "127.0.0.1:5433").ServeHTTP(rec, httptest.NewRequest("GET", "/status", nil))
		var doc pgRouterStatusDoc
		if err := json.Unmarshal(rec.Body.Bytes(), &doc); err != nil {
			t.Fatalf("%v: %s", err, rec.Body.String())
		}
		n, ok := doc.Servers[env.serverID]
		if !ok {
			t.Fatalf("no entry for the server: %s", rec.Body.String())
		}
		for _, r := range []string{pgshim.ReasonAnswersTheSame, pgshim.ReasonVeto, pgshim.ReasonTableNotOnCopy, pgshim.ReasonCopyRefused,
			pgshim.ReasonCopyColumnsDiffer, pgshim.ReasonReadOnly, pgshim.ReasonSessionDiffers, pgshim.ReasonNotASelect, pgshim.ReasonInTransaction} {
			if n.Reasons[r] == 0 {
				t.Errorf("no statement counted under %s: %v", r, n.Reasons)
			}
		}
		if n.Copy == 0 || n.Source == 0 || n.Refused == 0 {
			t.Errorf("counts: copy %d, source %d, refused %d", n.Copy, n.Source, n.Refused)
		}
		found := false
		for _, row := range n.Statements.Rows {
			if row.Statement == "select count(*) from "+pgRouteTable && row.Side == pgshim.RouteCopy {
				found = row.Reasons[pgshim.ReasonAnswersTheSame] > 0
			}
			if strings.Contains(row.Statement, "s3cr3t") || strings.Contains(row.Statement, "America/Bogota") {
				t.Errorf("a value in the statement table: %q", row.Statement)
			}
		}
		if !found {
			t.Errorf("no copy row for the plain count: %+v", n.Statements.Rows)
		}
		if n.ForwardingOff != "" {
			t.Errorf("forwarding_off %q on a server that hides nothing", n.ForwardingOff)
		}
	})

	// Last: the rules change the server for the rest of the test.
	t.Run("a server that hides tables or columns sends nothing to its source", func(t *testing.T) {
		hide := func(r hiderules.Rules) {
			t.Helper()
			set, err := hiderules.Parse(r)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := env.owner.SetHidden(env.serverID, set); err != nil {
				t.Fatal(err)
			}
			env.srv.RefreshFollowedFiles()
		}
		// leaked says an answer came from the source: its fourth row, a
		// hidden value, or the forwarding account.
		leaked := func(got string) bool {
			return strings.Contains(got, "s3cr3t") || got == "4" || strings.Contains(got, "Dario") || got == pgRouteRole
		}

		// A rule saved while a block is open on the source: the block
		// fails, nothing more is read there.
		c := pgClient(t, ctx, env.portDSN(), nil)
		one(ctx, c, "BEGIN")
		if got := one(ctx, c, count); got != "4" {
			t.Fatalf("the block before the rule: %q", got)
		}
		hide(hiderules.Rules{Columns: []string{"public." + pgRouteTable + ".secret"}})
		if got := one(ctx, c, "SELECT secret FROM "+pgRouteTable+" WHERE id = 4"); leaked(got) || !strings.HasPrefix(got, "error") {
			t.Errorf("the open block after the rule: %q", got)
		}
		one(ctx, c, "ROLLBACK")

		// A hidden column: refused by the copy, never read on the source,
		// whichever way the statement takes.
		d := pgClient(t, ctx, env.portDSN(), nil)
		for _, stmt := range []string{
			"SELECT secret FROM " + pgRouteTable + " WHERE id = 1",
			"SELECT secret, now() FROM " + pgRouteTable,
			"SELECT secret FROM " + pgRouteTable + " ORDER BY secret",
			"SELECT current_user",
			"SELECT count(*) FROM " + pgRouteSource,
		} {
			if got := one(ctx, d, stmt); leaked(got) || got == "1" && strings.Contains(stmt, pgRouteSource) {
				t.Errorf("%s with the column hidden: %q", stmt, got)
			}
		}
		// A block: answered by the port and the copy, not the source.
		one(ctx, d, "BEGIN")
		for _, stmt := range []string{count, "SELECT secret FROM " + pgRouteTable + " WHERE id = 1", "SELECT current_user"} {
			if got := one(ctx, d, stmt); leaked(got) {
				t.Errorf("%s in a block with the column hidden: %q", stmt, got)
			}
		}
		one(ctx, d, "ROLLBACK")
		conn := pgxConn(t, ctx, env.portDSN(), pgx.QueryExecModeCacheStatement)
		if got := pgxAnswer(ctx, conn, "SELECT secret FROM "+pgRouteTable+" WHERE id = $1", 1); leaked(got) || !strings.HasPrefix(got, "error") {
			t.Errorf("pgx, the hidden column: %s", got)
		}
		conn.Close(ctx)

		// A hidden table.
		hide(hiderules.Rules{Tables: []string{"public." + pgRouteTable}})
		e := pgClient(t, ctx, env.portDSN(), nil)
		for _, stmt := range []string{count, count + " WHERE now() IS NOT NULL", "SELECT secret FROM " + pgRouteTable} {
			if got := one(ctx, e, stmt); leaked(got) || !strings.HasPrefix(got, "error") {
				t.Errorf("%s with the table hidden: %q", stmt, got)
			}
		}
		one(ctx, e, "BEGIN")
		if got := one(ctx, e, count); leaked(got) || !strings.HasPrefix(got, "error") {
			t.Errorf("the hidden table in a block: %q", got)
		}
		one(ctx, e, "ROLLBACK")

		// Nothing of it ran on the source as the forwarding account.
		if n := one(ctx, source, "SELECT count(*) FROM pg_stat_activity WHERE usename = '"+pgRouteRole+"' AND pid <> pg_backend_pid()"); n != "0" {
			t.Errorf("%s connections of the forwarding account on the source", n)
		}
		doc := env.routes.doc("")
		if n := doc.Servers[env.serverID]; n.ForwardingOff == "" || n.Reasons[pgshim.ReasonHiddenRules] == 0 {
			t.Errorf("the status does not say forwarding is off: %+v", n)
		}

		// Rules gone: the source answers again.
		hide(hiderules.Rules{})
		f := pgClient(t, ctx, env.portDSN(), nil)
		if got := one(ctx, f, count+" WHERE now() IS NOT NULL"); got != "4" {
			t.Errorf("with no rule: %q", got)
		}
	})
}

const psycopgRouteScript = `import sys
import psycopg

with psycopg.connect(sys.argv[1], autocommit=True) as conn:
    def one(q, *a):
        return conn.execute(q, a).fetchone()[0]
    t = "` + pgRouteTable + `"
    # The copy's first: once the source answered, the copy's older answers
    # are not given to this connection.
    copy = one("SELECT count(*) FROM " + t + " WHERE id > %s", 0)
    at = one("SELECT min(at) FROM " + t + " WHERE id > %s", 0)
    source = one("SELECT count(*) FROM " + t + " WHERE now() IS NOT NULL AND id > %s", 0)
    missing = one("SELECT count(*) FROM ` + pgRouteSource + ` WHERE id > %s", 0)
    free = one("SELECT free FROM " + t + " WHERE id = %s", 1)
    print(f"copy={copy} source={source} missing={missing} at={at} free={free}")
`

const jdbcRouteProgram = `import java.sql.*;

public class Route {
    static String one(Connection c, String q, int arg) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(q)) {
            ps.setInt(1, arg);
            try (ResultSet rs = ps.executeQuery()) { rs.next(); return rs.getString(1); }
        }
    }
    public static void main(String[] args) throws Exception {
        String t = "` + pgRouteTable + `";
        try (Connection c = DriverManager.getConnection(args[0])) {
            String copy = "", source = "", missing = "", at = "", free = "";
            // Past prepareThreshold: named statements too. The copy's first:
            // once the source answered, the copy's older answers are not
            // given to this connection.
            for (int i = 0; i < 6; i++) {
                copy = one(c, "SELECT count(*) FROM " + t + " WHERE id > ?", 0);
                at = one(c, "SELECT min(at) FROM " + t + " WHERE id > ?", 0);
            }
            for (int i = 0; i < 6; i++) {
                source = one(c, "SELECT count(*) FROM " + t + " WHERE now() IS NOT NULL AND id > ?", 0);
                missing = one(c, "SELECT count(*) FROM ` + pgRouteSource + ` WHERE id > ?", 0);
                free = one(c, "SELECT free FROM " + t + " WHERE id = ?", 1);
            }
            System.out.println("copy=" + copy + " source=" + source + " missing=" + missing + " at=" + at + " free=" + free);
        }
    }
}
`
