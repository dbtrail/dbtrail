//go:build integration

package consoleapp

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/dbtrail/dbtrail/internal/console"
)

// #3054 against a real PostgreSQL: a server with no forwarding account of
// its own and the answer yes forwards with the capture account, and only a
// SELECT whose every table is a table of the copy reaches the source.

const pgCapTable = "pgcap_t"

func TestIntegrationPGRouterCaptureAccount(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	env := startPGPortFor(t, ctx, nil, pgPortFixture{table: pgCapTable, slot: "pgcap_slot", pub: "pgcap_pub",
		setup: []string{
			"CREATE TABLE " + pgCapTable + " (id int PRIMARY KEY, v text)",
			"INSERT INTO " + pgCapTable + " VALUES (1, 'one'), (2, 'two')",
		},
		entry: func(e *console.ServerEntry) {
			e.SourceDSN, e.SourceSlot, e.SourcePublication = pgFwdSourceDSN(t), "pgcap_slot", "pgcap_pub"
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
	cleanup := []string{"DROP VIEW IF EXISTS pgcap_v", "DROP TABLE IF EXISTS pgcap_other", "DROP SCHEMA IF EXISTS pgcap_secret CASCADE",
		"DROP FUNCTION IF EXISTS pgcap_leak()"}
	for _, s := range cleanup {
		exec1(s)
	}
	t.Cleanup(func() {
		for _, s := range cleanup {
			_, _ = source.Exec(context.Background(), s).ReadAll()
		}
	})
	exec1("CREATE SCHEMA pgcap_secret")
	exec1("CREATE TABLE pgcap_secret.vip (id int PRIMARY KEY, card text)")
	exec1("INSERT INTO pgcap_secret.vip VALUES (1, '4111')")
	exec1("CREATE TABLE pgcap_other (id int)")
	exec1("CREATE VIEW pgcap_v AS SELECT * FROM pgcap_secret.vip")
	exec1(`CREATE FUNCTION pgcap_leak() RETURNS text LANGUAGE sql AS 'SELECT card FROM pgcap_secret.vip LIMIT 1'`)
	// After the snapshot: a row the copy does not have, so an answer says
	// which side gave it.
	exec1("INSERT INTO " + pgCapTable + " VALUES (3, 'three')")

	inBlock := func(c *pgconn.PgConn, stmt string) string {
		t.Helper()
		if got := one(ctx, c, "BEGIN"); got != "BEGIN" {
			t.Fatalf("BEGIN: %q", got)
		}
		got := one(ctx, c, stmt)
		one(ctx, c, "ROLLBACK")
		return got
	}

	t.Run("no answer saved: nothing is sent to the source, as before", func(t *testing.T) {
		c := pgClient(t, ctx, env.portDSN(), nil)
		// The copy answers (its snapshot holds 2 rows), not the source (3).
		if got := inBlock(c, "SELECT count(*) FROM "+pgCapTable); got != "2" {
			t.Fatalf("a block with no account and no answer: %q, want the copy's 2", got)
		}
	})

	if _, err := env.owner.SetForwardWithCaptureAccount(env.serverID, true); err != nil {
		t.Fatal(err)
	}
	env.srv.RefreshFollowedFiles()

	refused := "error 0A000 this server forwards to its source with the account DBTrail captures with, so the port sends there only a SELECT whose every table is a table of the copy, and "
	c := pgClient(t, ctx, env.portDSN(), nil)
	for _, tc := range []struct {
		name, stmt, want string
	}{
		{"a table of the copy: the source answers, as the capture account", "SELECT count(*) FROM " + pgCapTable, "3"},
		{"no table", "SELECT 1 + 1", "2"},
		{"the session is read-only", "SELECT current_setting('transaction_read_only')", "on"},
		{"the password hashes", "SELECT rolname FROM pg_authid", refused + "this statement reads a table of the system catalog; nothing was sent to the source"},
		{"the password hashes, qualified", "SELECT rolname FROM pg_catalog.pg_authid", refused + "this statement reads a table of the system catalog; nothing was sent to the source"},
		{"a schema not captured", "SELECT card FROM pgcap_secret.vip", refused + "this statement reads a table the copy does not hold; nothing was sent to the source"},
		{"a table not captured in a captured schema", "SELECT * FROM pgcap_other", refused + "this statement reads a table the copy does not hold; nothing was sent to the source"},
		{"a captured table joined with one that is not", "SELECT t.v, s.card FROM " + pgCapTable + " t JOIN pgcap_secret.vip s ON s.id = t.id", refused + "this statement reads a table the copy does not hold; nothing was sent to the source"},
		{"a view over a table that is not captured", "SELECT * FROM pgcap_v", refused + "the source did not confirm that the names in it are plain tables and that nothing of a user's runs in it; nothing was sent to the source"},
		{"a derived table reaching one", "SELECT * FROM (SELECT card FROM pgcap_secret.vip) x", refused + "this statement reads a table the copy does not hold; nothing was sent to the source"},
		{"a subquery reaching one", "SELECT v FROM " + pgCapTable + " WHERE id IN (SELECT id FROM pgcap_secret.vip)", refused + "this statement reads a table the copy does not hold; nothing was sent to the source"},
		{"a WITH reaching one", "WITH s AS (SELECT card FROM pgcap_secret.vip) SELECT * FROM s", refused + "this statement reads a table the copy does not hold; nothing was sent to the source"},
		{"a function of the user's", "SELECT pgcap_leak()", refused + "this statement is not one whose tables the port can tell from its text (the function pgcap_leak()); nothing was sent to the source"},
		{"a built-in that runs a query", "SELECT query_to_xml('select * from pgcap_secret.vip', true, true, '')", refused + "this statement is not one whose tables the port can tell from its text (the function query_to_xml()); nothing was sent to the source"},
		{"information_schema", "SELECT table_name FROM information_schema.tables", refused + "the source did not confirm that the names in it are plain tables and that nothing of a user's runs in it; nothing was sent to the source"},
		{"a name in another case, which is another table", `SELECT * FROM "PGCAP_T"`, refused + "the source did not confirm that the names in it are plain tables and that nothing of a user's runs in it; nothing was sent to the source"},
		// The port's screen refuses it first (#3057); the gate refuses it
		// too (TestCaptureTextRefusal).
		{"a locking read", "SELECT * FROM " + pgCapTable + " FOR SHARE", "error 25006 this port is read-only toward the source: a SELECT that locks its rows (FOR UPDATE, FOR SHARE) is not sent"},
	} {
		got := inBlock(c, tc.stmt)
		if got != tc.want {
			t.Errorf("%s:\n got %s\nwant %s", tc.name, got, tc.want)
		}
		t.Logf("%s: %s", tc.name, got)
	}

	t.Run("search_path toward a schema that is not captured", func(t *testing.T) {
		c := pgClient(t, ctx, env.portDSN(), nil)
		// The port itself keeps a session on the copy's schemas: a schema
		// the copy does not hold is refused, so vip does not resolve there.
		if got, want := one(ctx, c, "SET search_path = pgcap_secret"), `error 3F000 schema "pgcap_secret" does not exist on the copy`; got != want {
			t.Fatalf("SET search_path: %s, want %s", got, want)
		}
		if got, want := inBlock(c, "SELECT card FROM vip"), refused+"the source did not confirm that the names in it are plain tables and that nothing of a user's runs in it; nothing was sent to the source"; got != want {
			t.Errorf("got %s\nwant %s", got, want)
		}
		// pg_catalog is refused by the port too; the gate's own refusal of
		// it is TestCaptureTextRefusal's (pgshim).
		if got, want := one(ctx, c, "SET search_path = pg_catalog"), "error 0A000 SET search_path with no schema of the copy is not supported on this port: name the one schema unqualified names resolve in"; got != want {
			t.Errorf("SET search_path = pg_catalog: %s, want %s", got, want)
		}
	})

	t.Run("extended protocol", func(t *testing.T) {
		conn := pgxConn(t, ctx, env.portDSN(), pgx.QueryExecModeCacheStatement)
		tx, err := conn.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var v string
		if err := tx.QueryRow(ctx, "SELECT v FROM "+pgCapTable+" WHERE id = $1", 3).Scan(&v); err != nil || v != "three" {
			t.Errorf("a table of the copy, by parameter: %q, %v", v, err)
		}
		_ = tx.Rollback(ctx)
		tx, err = conn.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		err = tx.QueryRow(ctx, "SELECT card FROM pgcap_secret.vip WHERE id = $1", 1).Scan(&v)
		if err == nil || !strings.Contains(pgCode(err), "0A000") {
			t.Errorf("a table not captured, by parameter: %q, %v", v, err)
		}
		_ = tx.Rollback(ctx)
	})

	t.Run("outside a block, a table the copy does not hold", func(t *testing.T) {
		c := pgClient(t, ctx, env.portDSN(), nil)
		// The copy cannot run it, so it would go to the source: the gate
		// refuses it there.
		if got, want := one(ctx, c, "SELECT card FROM pgcap_secret.vip"), refused+"this statement reads a table the copy does not hold; nothing was sent to the source"; got != want {
			t.Errorf("got %s\nwant %s", got, want)
		}
		if got := one(ctx, c, "SELECT count(*) FROM "+pgCapTable); got != "2" {
			t.Errorf("a table of the copy outside a block: %q, want the copy's 2", got)
		}
	})

	t.Run("one batch: Parse, Describe, Bind, Execute, Sync", func(t *testing.T) {
		c := pgClient(t, ctx, env.portDSN(), nil)
		if got := one(ctx, c, "BEGIN"); got != "BEGIN" {
			t.Fatalf("BEGIN: %q", got)
		}
		pipe := c.StartPipeline(ctx)
		pipe.SendPrepare("cap_one", "SELECT v FROM "+pgCapTable+" WHERE id = $1", nil)
		pipe.SendQueryPrepared("cap_one", [][]byte{[]byte("3")}, nil, nil)
		if err := pipe.Sync(); err != nil {
			t.Fatal(err)
		}
		var got []string
		for {
			res, err := pipe.GetResults()
			if err != nil {
				got = append(got, "error "+pgCode(err))
				break
			}
			if res == nil {
				break
			}
			switch r := res.(type) {
			case *pgconn.ResultReader:
				out := r.Read()
				if out.Err != nil {
					got = append(got, "error "+pgCode(out.Err))
				}
				for _, row := range out.Rows {
					got = append(got, string(row[0]))
				}
			case *pgconn.PipelineSync:
				got = append(got, "sync")
			}
			if len(got) > 0 && got[len(got)-1] == "sync" {
				break
			}
		}
		_ = pipe.Close()
		one(ctx, c, "ROLLBACK")
		if strings.Join(got, ",") != "three,sync" {
			t.Errorf("one batch: %q, want the source's row and the Sync", got)
		}
	})

	t.Run("the status names the account", func(t *testing.T) {
		c := pgClient(t, ctx, env.portDSN(), nil)
		res, err := c.Exec(ctx, "SHOW dbtrail.router_status").ReadAll()
		if err != nil || len(res) == 0 {
			t.Fatal(err)
		}
		var lines []string
		for _, r := range res[0].Rows {
			if name := string(r[0]); strings.HasPrefix(name, "forwarding") {
				lines = append(lines, name+" | "+string(r[1]))
			}
		}
		want := "forwarding | on\nforwarding_account | capture account, read-only, tables of the copy only"
		if got := strings.Join(lines, "\n"); got != want {
			t.Errorf("got\n%s\nwant\n%s", got, want)
		}
		t.Logf("SHOW dbtrail.router_status:\n%s", strings.Join(lines, "\n"))
	})

	t.Run("taken back: nothing is sent again", func(t *testing.T) {
		if _, err := env.owner.SetForwardWithCaptureAccount(env.serverID, false); err != nil {
			t.Fatal(err)
		}
		env.srv.RefreshFollowedFiles()
		c := pgClient(t, ctx, env.portDSN(), nil)
		if got := inBlock(c, "SELECT count(*) FROM "+pgCapTable); got != "2" {
			t.Errorf("after no: %q, want the copy's 2", got)
		}
	})
}
