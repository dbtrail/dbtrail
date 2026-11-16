//go:build integration

package consoleapp

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/dbtrail/dbtrail/internal/console"
)

// Forwarding to the source on the PostgreSQL port (#2771), against a real
// PostgreSQL: a transaction block the client opens runs there, as the
// server's forwarding account, read-only, with the source's errors, notices
// and cancel passed through; everything else is answered by the copy as
// before.

const (
	pgFwdTable = "pgfwd_t"
	pgFwdRole  = "dbtrail_fwd_2771"
	pgFwdPass  = "fwd-pw-2771"
)

// withAccount is the URL u with its user and password replaced.
func withAccount(t *testing.T, raw, user, password string) string {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	u.User = url.UserPassword(user, password)
	return u.String()
}

// pgFwdEnv is a port whose one server has its source and a forwarding
// account of its own.
func pgFwdEnv(t *testing.T, ctx context.Context, sources int) (pgExtEnv, *pgconn.PgConn) {
	t.Helper()
	env := startPGPortFor(t, ctx, nil, pgPortFixture{table: pgFwdTable, slot: "pgfwd_slot", pub: "pgfwd_pub", sources: sources,
		setup: []string{
			"CREATE TABLE " + pgFwdTable + " (id int PRIMARY KEY, v text)",
			"INSERT INTO " + pgFwdTable + " VALUES (1, 'one'), (2, 'two')",
		},
		entry: func(e *console.ServerEntry) {
			e.SourceDSN, e.SourceSlot, e.SourcePublication = pgFwdSourceDSN(t), "pgfwd_slot", "pgfwd_pub"
			e.RouteDSN = withAccount(t, e.SourceDSN, pgFwdRole, pgFwdPass)
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
	// After the snapshot: a row the copy does not have, so an answer says
	// which side gave it.
	exec1("INSERT INTO " + pgFwdTable + " VALUES (3, 'three')")
	exec1(`DO $$ BEGIN IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '` + pgFwdRole + `') THEN EXECUTE 'DROP OWNED BY ` + pgFwdRole + `'; END IF; END $$`)
	exec1("DROP ROLE IF EXISTS " + pgFwdRole)
	exec1("CREATE ROLE " + pgFwdRole + " LOGIN PASSWORD '" + pgFwdPass + "'")
	// INSERT granted on purpose: the read-only connection refuses a write
	// whatever the grants say.
	exec1("GRANT SELECT, INSERT ON " + pgFwdTable + " TO " + pgFwdRole)
	exec1(`CREATE OR REPLACE FUNCTION pgfwd_notice() RETURNS int LANGUAGE plpgsql AS $$ BEGIN RAISE NOTICE 'hello from the source'; RETURN 7; END $$`)
	exec1(`CREATE OR REPLACE FUNCTION pgfwd_write() RETURNS int LANGUAGE plpgsql AS $$ BEGIN INSERT INTO ` + pgFwdTable + ` VALUES (99, 'x'); RETURN 1; END $$`)
	exec1(`CREATE OR REPLACE FUNCTION pgfwd_unguard() RETURNS int LANGUAGE plpgsql AS $$ BEGIN PERFORM set_config('default_transaction_read_only', 'off', false); RETURN 1; END $$`)
	t.Cleanup(func() {
		bg := context.Background()
		for _, s := range []string{"DROP FUNCTION IF EXISTS pgfwd_notice()", "DROP FUNCTION IF EXISTS pgfwd_write()", "DROP FUNCTION IF EXISTS pgfwd_unguard()",
			"DROP OWNED BY " + pgFwdRole, "DROP ROLE IF EXISTS " + pgFwdRole} {
			_, _ = source.Exec(bg, s).ReadAll()
		}
	})
	return env, source
}

// pgFwdSourceDSN is the test PostgreSQL as the capture account.
func pgFwdSourceDSN(t *testing.T) string {
	t.Helper()
	return os.Getenv("BINTRAIL_TEST_PG_DSN")
}

// pgClient connects pgconn to the port, keeping the notices it is sent.
func pgClient(t *testing.T, ctx context.Context, dsn string, notices *[]string) *pgconn.PgConn {
	t.Helper()
	cfg, err := pgconn.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	if notices != nil {
		cfg.OnNotice = func(_ *pgconn.PgConn, n *pgconn.Notice) { *notices = append(*notices, n.Severity+" "+n.Message) }
	}
	c, err := pgconn.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("connect %s: %v", dsn, err)
	}
	t.Cleanup(func() { c.Close(context.Background()) })
	return c
}

// one runs stmt and returns its first row's cells joined, or "error CODE
// message".
func one(ctx context.Context, c *pgconn.PgConn, stmt string) string {
	res, err := c.Exec(ctx, stmt).ReadAll()
	if err != nil {
		return "error " + pgCode(err)
	}
	last := res[len(res)-1]
	if last.Err != nil {
		return "error " + pgCode(last.Err)
	}
	if len(last.Rows) == 0 {
		return string(last.CommandTag.String())
	}
	var cells []string
	for _, v := range last.Rows[0] {
		cells = append(cells, string(v))
	}
	return strings.Join(cells, "|")
}

// sourceActive counts the statements running on the source as the
// forwarding account whose text holds like.
func sourceActive(ctx context.Context, source *pgconn.PgConn, like string) string {
	return one(ctx, source, "SELECT count(*) FROM pg_stat_activity WHERE usename = '"+pgFwdRole+"' AND state = 'active' AND query LIKE '%"+like+"%' AND pid <> pg_backend_pid()")
}

func TestIntegrationPGRouterForward(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	env, source := pgFwdEnv(t, ctx, 2)
	count := "SELECT count(*) FROM " + pgFwdTable

	t.Run("a transaction block is answered by the source, as the forwarding account", func(t *testing.T) {
		c := pgClient(t, ctx, env.portDSN(), nil)
		if got := one(ctx, c, count); got != "2" {
			t.Fatalf("outside a block: %q, want the copy's 2", got)
		}
		if got := one(ctx, c, "BEGIN"); got != "BEGIN" || c.TxStatus() != 'T' {
			t.Fatalf("BEGIN: %q, status %c", got, c.TxStatus())
		}
		if got := one(ctx, c, count); got != "3" {
			t.Errorf("in the block: %q, want the source's 3", got)
		}
		if got := one(ctx, c, "SELECT current_user"); got != pgFwdRole {
			t.Errorf("the source ran it as %q, want %s", got, pgFwdRole)
		}
		if got := one(ctx, c, "SELECT current_setting('default_transaction_read_only'), current_setting('transaction_read_only')"); got != "on|on" {
			t.Errorf("the source connection's read-only settings: %q", got)
		}
		if got := one(ctx, c, "SELECT 1 AS a; SELECT 2 AS b"); got != "2" {
			t.Errorf("two statements in one: %q", got)
		}
		if got := one(ctx, c, "COMMIT"); got != "COMMIT" || c.TxStatus() != 'I' {
			t.Errorf("COMMIT: %q, status %c", got, c.TxStatus())
		}
		if got := one(ctx, c, count); got != "2" {
			t.Errorf("after the block: %q, want the copy's 2 again", got)
		}
	})

	t.Run("the session's settings go with it", func(t *testing.T) {
		c := pgClient(t, ctx, env.portDSN(), nil)
		one(ctx, c, "SET TimeZone = 'America/Bogota'")
		one(ctx, c, "SET statement_timeout = '45s'")
		one(ctx, c, "BEGIN")
		if got := one(ctx, c, "SELECT current_setting('TimeZone'), current_setting('statement_timeout'), current_setting('DateStyle'), current_setting('search_path')"); got != `America/Bogota|45s|ISO, MDY|"$user", public` {
			t.Errorf("the source's settings: %q", got)
		}
		if got := one(ctx, c, "SELECT '2026-01-01 12:00:00+00'::timestamptz::text"); got != "2026-01-01 07:00:00-05" {
			t.Errorf("a timestamptz from the source: %q", got)
		}
		one(ctx, c, "SET LOCAL TimeZone = 'UTC'")
		if got := one(ctx, c, "SELECT current_setting('TimeZone')"); got != "UTC" {
			t.Errorf("SET LOCAL in the block: %q", got)
		}
		one(ctx, c, "COMMIT")
		one(ctx, c, "BEGIN")
		if got := one(ctx, c, "SELECT current_setting('TimeZone')"); got != "America/Bogota" {
			t.Errorf("after COMMIT the SET LOCAL is gone: %q", got)
		}
		one(ctx, c, "ROLLBACK")
	})

	t.Run("the source's errors keep their code, and fail the block", func(t *testing.T) {
		c := pgClient(t, ctx, env.portDSN(), nil)
		one(ctx, c, "BEGIN")
		if got := one(ctx, c, "SELECT 1/0"); !strings.HasPrefix(got, "error 22012") {
			t.Errorf("division by zero: %q", got)
		}
		if c.TxStatus() != 'E' {
			t.Errorf("status %c after an error in the block, want E", c.TxStatus())
		}
		if got := one(ctx, c, count); !strings.HasPrefix(got, "error 25P02") {
			t.Errorf("a statement in the failed block: %q", got)
		}
		if got := one(ctx, c, "COMMIT"); got != "ROLLBACK" || c.TxStatus() != 'I' {
			t.Errorf("COMMIT of a failed block: %q, status %c", got, c.TxStatus())
		}
		one(ctx, c, "BEGIN")
		if got := one(ctx, c, "SELECT * FROM no_such_table_2771"); !strings.HasPrefix(got, "error 42P01") {
			t.Errorf("an unknown table: %q", got)
		}
		one(ctx, c, "ROLLBACK")
	})

	t.Run("a savepoint brings a block back from an error", func(t *testing.T) {
		c := pgClient(t, ctx, env.portDSN(), nil)
		one(ctx, c, "BEGIN")
		one(ctx, c, "SAVEPOINT a")
		if got := one(ctx, c, "SELECT 1/0"); !strings.HasPrefix(got, "error 22012") || c.TxStatus() != 'E' {
			t.Fatalf("the error: %q, status %c", got, c.TxStatus())
		}
		if got := one(ctx, c, "ROLLBACK TO SAVEPOINT a"); got != "ROLLBACK" || c.TxStatus() != 'T' {
			t.Errorf("ROLLBACK TO SAVEPOINT: %q, status %c", got, c.TxStatus())
		}
		if got := one(ctx, c, count); got != "3" {
			t.Errorf("after it: %q", got)
		}
		// A statement the port refused fails the block too, and the same
		// savepoint brings it back.
		one(ctx, c, "SAVEPOINT b")
		if got := one(ctx, c, "INSERT INTO "+pgFwdTable+" VALUES (60, 'w')"); !strings.HasPrefix(got, "error 25006") || c.TxStatus() != 'E' {
			t.Errorf("a refused write: %q, status %c", got, c.TxStatus())
		}
		if got := one(ctx, c, "ROLLBACK TO b"); got != "ROLLBACK" || c.TxStatus() != 'T' {
			t.Errorf("ROLLBACK TO b: %q, status %c", got, c.TxStatus())
		}
		if got := one(ctx, c, "COMMIT"); got != "COMMIT" {
			t.Errorf("COMMIT: %q", got)
		}
		// The settings go back to the savepoint's, as on PostgreSQL: a SET
		// before it stays, one after it goes.
		one(ctx, c, "BEGIN")
		one(ctx, c, "SET TimeZone = 'Asia/Tokyo'")
		one(ctx, c, "SAVEPOINT s1")
		one(ctx, c, "SET TimeZone = 'UTC'")
		one(ctx, c, "SELECT 1/0")
		one(ctx, c, "ROLLBACK TO SAVEPOINT s1")
		if got := one(ctx, c, "SELECT current_setting('TimeZone')"); got != "Asia/Tokyo" {
			t.Errorf("TimeZone on the source after ROLLBACK TO: %q", got)
		}
		if got := one(ctx, c, "SHOW TimeZone"); got != "Asia/Tokyo" {
			t.Errorf("SHOW TimeZone after ROLLBACK TO: %q", got)
		}
		one(ctx, c, "ROLLBACK")
		if got := one(ctx, c, "SHOW TimeZone"); got != "UTC" {
			t.Errorf("SHOW TimeZone after ROLLBACK: %q", got)
		}
		for _, mode := range []pgx.QueryExecMode{pgx.QueryExecModeCacheStatement, pgx.QueryExecModeExec} {
			// A deadline of its own: a hang here must not take the rest of
			// the test's time.
			ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()
			conn := pgxConn(t, ctx, env.portDSN(), mode)
			tx, err := conn.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			nested, err := tx.Begin(ctx)
			if err != nil {
				t.Fatalf("mode %v: savepoint: %v", mode, err)
			}
			var x int
			if err := nested.QueryRow(ctx, "SELECT 1/$1::int", 0).Scan(&x); !strings.HasPrefix(pgCode(err), "22012") {
				t.Errorf("mode %v: the error: %v", mode, err)
			}
			if err := nested.Rollback(ctx); err != nil {
				t.Errorf("mode %v: rollback to the savepoint: %v", mode, err)
			}
			var n int64
			if err := tx.QueryRow(ctx, "SELECT count(*) FROM "+pgFwdTable+" WHERE id > $1", 0).Scan(&n); err != nil || n != 3 {
				t.Errorf("mode %v: after the savepoint: %d, %v", mode, n, err)
			}
			if err := tx.Commit(ctx); err != nil {
				t.Errorf("mode %v: commit: %v", mode, err)
			}
			conn.Close(ctx)
		}
	})

	t.Run("writes are refused, whatever the grants", func(t *testing.T) {
		c := pgClient(t, ctx, env.portDSN(), nil)
		one(ctx, c, "BEGIN")
		if got := one(ctx, c, "INSERT INTO "+pgFwdTable+" VALUES (50, 'w')"); !strings.HasPrefix(got, "error 25006") || !strings.Contains(got, "INSERT is not a read") {
			t.Errorf("INSERT: %q", got)
		}
		one(ctx, c, "ROLLBACK")
		one(ctx, c, "BEGIN")
		// A function that writes: the text reads as a SELECT, the source's
		// read-only transaction refuses it.
		if got := one(ctx, c, "SELECT pgfwd_write()"); !strings.HasPrefix(got, "error 25006") {
			t.Errorf("a function that writes: %q", got)
		}
		one(ctx, c, "ROLLBACK")
		one(ctx, c, "BEGIN")
		if got := one(ctx, c, "WITH d AS (DELETE FROM "+pgFwdTable+" RETURNING 1) SELECT count(*) FROM d"); !strings.HasPrefix(got, "error 25006") {
			t.Errorf("a data-modifying WITH: %q", got)
		}
		one(ctx, c, "ROLLBACK")
		for _, refused := range []struct{ stmt, code, says string }{
			{"BEGIN READ WRITE", "0A000", "read-only"},
			{"SET default_transaction_read_only = off", "0A000", "read-only"},
		} {
			if got := one(ctx, c, refused.stmt); !strings.HasPrefix(got, "error "+refused.code) || !strings.Contains(got, refused.says) {
				t.Errorf("%s: %q", refused.stmt, got)
			}
		}
		if got := one(ctx, source, "SELECT count(*) FROM "+pgFwdTable+" WHERE id IN (50, 99)"); got != "0" {
			t.Errorf("a write reached the source: %s rows", got)
		}
	})

	t.Run("COPY, LISTEN, NOTIFY and DO are refused with a reason", func(t *testing.T) {
		c := pgClient(t, ctx, env.portDSN(), nil)
		for _, r := range []struct{ stmt, code, says string }{
			{"COPY " + pgFwdTable + " TO STDOUT", "0A000", "COPY is not served"},
			{"LISTEN pgfwd_chan", "0A000", "LISTEN is not served"},
			{"NOTIFY pgfwd_chan", "25006", "NOTIFY"},
			{"SELECT pg_notify('pgfwd_chan', 'x')", "0A000", "pg_notify()"},
			{"DO $$ BEGIN END $$", "0A000", "DO runs code"},
		} {
			one(ctx, c, "BEGIN")
			if got := one(ctx, c, r.stmt); !strings.HasPrefix(got, "error "+r.code) || !strings.Contains(got, r.says) {
				t.Errorf("%s: %q", r.stmt, got)
			}
			one(ctx, c, "ROLLBACK")
		}
	})

	t.Run("the source's notices reach the client", func(t *testing.T) {
		var notices []string
		c := pgClient(t, ctx, env.portDSN(), &notices)
		one(ctx, c, "BEGIN")
		if got := one(ctx, c, "SELECT pgfwd_notice()"); got != "7" {
			t.Errorf("the function's answer: %q", got)
		}
		one(ctx, c, "COMMIT")
		if strings.Join(notices, ";") != "NOTICE hello from the source" {
			t.Errorf("notices = %q", notices)
		}
	})

	t.Run("a connection whose read-only guard is turned off is dropped", func(t *testing.T) {
		c := pgClient(t, ctx, env.portDSN(), nil)
		one(ctx, c, "BEGIN")
		// The client is told in the same answer, not left with a failed
		// block it cannot explain.
		if got := one(ctx, c, "SELECT pgfwd_unguard()"); !strings.HasPrefix(got, "error 08006") || !strings.Contains(got, "default_transaction_read_only off") {
			t.Errorf("the function: %q", got)
		}
		if c.TxStatus() != 'E' {
			t.Errorf("status %c after the guard went, want the block failed (E)", c.TxStatus())
		}
		if got := one(ctx, c, "ROLLBACK"); got != "ROLLBACK" {
			t.Errorf("ROLLBACK: %q", got)
		}
		if got := one(ctx, c, "BEGIN"); !strings.HasPrefix(got, "error 08006") || !strings.Contains(got, "default_transaction_read_only off") {
			t.Errorf("BEGIN after the connection was dropped: %q", got)
		}
		if got := one(ctx, c, count); got != "2" {
			t.Errorf("the copy still answers this connection: %q", got)
		}
	})

	t.Run("a cancel request stops the statement on the source", func(t *testing.T) {
		c := pgClient(t, ctx, env.portDSN(), nil)
		one(ctx, c, "BEGIN")
		done := make(chan string, 1)
		go func() { done <- one(ctx, c, "SELECT pg_sleep(30)") }()
		pgTestWait(t, "the sleep to run on the source", 10*time.Second, func() (bool, string) {
			n := sourceActive(ctx, source, "pg_sleep(30)")
			return n == "1", n
		})
		start := time.Now()
		if err := c.CancelRequest(ctx); err != nil {
			t.Fatal(err)
		}
		select {
		case got := <-done:
			if !strings.HasPrefix(got, "error 57014") {
				t.Errorf("the cancelled statement: %q", got)
			}
			if took := time.Since(start); took > 5*time.Second {
				t.Errorf("the cancel took %s", took)
			}
		case <-time.After(15 * time.Second):
			t.Fatal("the cancelled statement did not end")
		}
		if n := sourceActive(ctx, source, "pg_sleep(30)"); n != "0" {
			t.Errorf("%s statements still run on the source", n)
		}
		one(ctx, c, "ROLLBACK")
	})

	t.Run("a client that leaves in the middle of a statement stops it on the source", func(t *testing.T) {
		c := pgClient(t, ctx, env.portDSN(), nil)
		one(ctx, c, "BEGIN")
		go func() { _ = one(context.Background(), c, "SELECT pg_sleep(31)") }()
		pgTestWait(t, "the sleep to run on the source", 10*time.Second, func() (bool, string) {
			n := sourceActive(ctx, source, "pg_sleep(31)")
			return n == "1", n
		})
		_ = c.Conn().Close()
		pgTestWait(t, "the statement to end on the source", 15*time.Second, func() (bool, string) {
			n := one(ctx, source, "SELECT count(*) FROM pg_stat_activity WHERE usename = '"+pgFwdRole+"' AND query LIKE '%pg_sleep(31)%' AND pid <> pg_backend_pid()")
			return n == "0", n
		})
	})

	t.Run("a catalog statement the copy does not answer goes to the source", func(t *testing.T) {
		c := pgClient(t, ctx, env.portDSN(), nil)
		if got := one(ctx, c, "SELECT rolname FROM pg_catalog.pg_roles WHERE rolname = '"+pgFwdRole+"'"); got != pgFwdRole {
			t.Errorf("pg_roles: %q", got)
		}
		if c.TxStatus() != 'I' {
			t.Errorf("status %c", c.TxStatus())
		}
	})

	t.Run("pgx: prepared outside a block, run inside it, in every mode", func(t *testing.T) {
		for _, mode := range []pgx.QueryExecMode{pgx.QueryExecModeCacheStatement, pgx.QueryExecModeCacheDescribe, pgx.QueryExecModeDescribeExec, pgx.QueryExecModeExec, pgx.QueryExecModeSimpleProtocol} {
			conn := pgxConn(t, ctx, env.portDSN(), mode)
			n := func(q pgx.Tx) int64 {
				var v int64
				var err error
				if q == nil {
					err = conn.QueryRow(ctx, "SELECT count(*) FROM "+pgFwdTable+" WHERE id > $1", 0).Scan(&v)
				} else {
					err = q.QueryRow(ctx, "SELECT count(*) FROM "+pgFwdTable+" WHERE id > $1", 0).Scan(&v)
				}
				if err != nil {
					t.Errorf("mode %v: %v", mode, err)
				}
				return v
			}
			if got := n(nil); got != 2 {
				t.Errorf("mode %v outside: %d", mode, got)
			}
			tx, err := conn.Begin(ctx)
			if err != nil {
				t.Fatalf("mode %v: begin: %v", mode, err)
			}
			if got := n(tx); got != 3 {
				t.Errorf("mode %v inside: %d, want the source's 3", mode, got)
			}
			var v string
			if err := tx.QueryRow(ctx, "SELECT v FROM "+pgFwdTable+" WHERE id = $1", 3).Scan(&v); err != nil || v != "three" {
				t.Errorf("mode %v: a row only the source has: %q, %v", mode, v, err)
			}
			if err := tx.Commit(ctx); err != nil {
				t.Errorf("mode %v: commit: %v", mode, err)
			}
			if got := n(nil); got != 2 {
				t.Errorf("mode %v after: %d", mode, got)
			}
			// An error in a batch inside the block: the block fails, the
			// rest of the batch is discarded, ROLLBACK ends it.
			tx, err = conn.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			b := &pgx.Batch{}
			b.Queue("SELECT 1")
			b.Queue("SELECT 1/0")
			b.Queue("SELECT 2")
			err = tx.SendBatch(ctx, b).Close()
			if got := pgCode(err); !strings.HasPrefix(got, "22012") {
				t.Errorf("mode %v: a batch with an error: %v", mode, err)
			}
			if err := tx.Rollback(ctx); err != nil {
				t.Errorf("mode %v: rollback: %v", mode, err)
			}
			if got := n(nil); got != 2 {
				t.Errorf("mode %v after a failed batch: %d", mode, got)
			}
			conn.Close(ctx)
		}
	})

	t.Run("pgx: a column the copy types otherwise is never read as the wrong value", func(t *testing.T) {
		// sum(int) is a bigint on PostgreSQL and another type on the copy.
		// Described by the copy, run by the source: either the source's
		// value, or PostgreSQL's words for a plan whose columns changed,
		// never the source's bytes read as the copy's type.
		conn := pgxConn(t, ctx, env.portDSN(), pgx.QueryExecModeCacheStatement)
		defer conn.Close(ctx)
		const q = "SELECT sum(id) AS s FROM " + pgFwdTable + " WHERE id > $1"
		var outside any
		if err := conn.QueryRow(ctx, q, 0).Scan(&outside); err != nil {
			t.Fatalf("outside: %v", err)
		}
		tx, err := conn.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var inside int64
		err = tx.QueryRow(ctx, q, 0).Scan(&inside)
		switch {
		case err != nil && !strings.Contains(err.Error(), "cached plan must not change result type"):
			t.Errorf("inside: %v", err)
		case err == nil && inside != 6:
			t.Errorf("inside: %d, want the source's 6 (outside the copy said %v)", inside, outside)
		}
		t.Logf("outside %v (%T); inside %d, %v", outside, outside, inside, err)
		_ = tx.Rollback(ctx)
	})

	t.Run("at most the port's connections to the sources", func(t *testing.T) {
		a := pgClient(t, ctx, env.portDSN(), nil)
		b := pgClient(t, ctx, env.portDSN(), nil)
		c := pgClient(t, ctx, env.portDSN(), nil)
		one(ctx, a, "BEGIN")
		one(ctx, b, "BEGIN")
		if got := one(ctx, c, "BEGIN"); !strings.HasPrefix(got, "error 53300") || !strings.Contains(got, "--max-source-connections") {
			t.Errorf("a third connection to the source: %q", got)
		}
		if got := one(ctx, c, count); got != "2" {
			t.Errorf("the copy still answers it: %q", got)
		}
		// A block that ends gives its connection back, without the client
		// disconnecting: a pool's clients never do.
		one(ctx, a, "COMMIT")
		if got := one(ctx, c, "BEGIN"); got != "BEGIN" {
			t.Errorf("after another client's block ended: %q", got)
		}
		one(ctx, c, "ROLLBACK")
		one(ctx, b, "ROLLBACK")
		// A catalog statement outside a block holds none past its answer.
		for _, cl := range []*pgconn.PgConn{a, b, c} {
			if got := one(ctx, cl, "SELECT count(*) FROM pg_catalog.pg_roles WHERE rolname = '"+pgFwdRole+"'"); got != "1" {
				t.Errorf("a catalog statement: %q", got)
			}
		}
		one(ctx, a, "BEGIN")
		one(ctx, b, "BEGIN")
		one(ctx, a, "ROLLBACK")
		one(ctx, b, "ROLLBACK")
		pgTestWait(t, "the connections to the source to close once no block is open", 10*time.Second, func() (bool, string) {
			n := one(ctx, source, "SELECT count(*) FROM pg_stat_activity WHERE usename = '"+pgFwdRole+"' AND pid <> pg_backend_pid()")
			return n == "0", n
		})
	})

	t.Run("a message larger than the port takes is refused, and nothing reaches the source", func(t *testing.T) {
		raw := rawPG(t, ctx, env.portDSN())
		big := "SELECT '" + strings.Repeat("x", 17<<20) + "' AS pgfwd_big"
		raw.fe.Send(&pgproto3.Query{String: big})
		_ = raw.fe.Flush()
		m, err := raw.fe.Receive()
		if e, ok := m.(*pgproto3.ErrorResponse); err == nil && ok {
			if e.Code != "54000" || e.Severity != "FATAL" {
				t.Errorf("the refusal: %s %s %s", e.Severity, e.Code, e.Message)
			}
		} else if err == nil {
			t.Errorf("answered %T, want a FATAL 54000 or the connection closed", m)
		}
		if n := one(ctx, source, "SELECT count(*) FROM pg_stat_activity WHERE query LIKE '%pgfwd_big%' AND pid <> pg_backend_pid()"); n != "0" {
			t.Errorf("the statement reached the source: %s", n)
		}
		c := pgClient(t, ctx, env.portDSN(), nil)
		if got := one(ctx, c, count); got != "2" {
			t.Errorf("the port after it: %q", got)
		}
	})

	t.Run("a change of the forwarding account closes the connections that use it", func(t *testing.T) {
		c := pgClient(t, ctx, env.portDSN(), nil)
		one(ctx, c, "BEGIN")
		if got := one(ctx, c, "SELECT current_user"); got != pgFwdRole {
			t.Fatalf("current_user %q", got)
		}
		e, _ := env.owner.Get(env.serverID)
		one(ctx, source, "ALTER ROLE "+pgFwdRole+" PASSWORD 'second-pw'")
		e.RouteDSN = withAccount(t, e.SourceDSN, pgFwdRole, "second-pw")
		if err := env.owner.Update(e); err != nil {
			t.Fatal(err)
		}
		env.srv.RefreshFollowedFiles()
		got := one(ctx, c, "SELECT 1")
		if !strings.HasPrefix(got, "error") {
			t.Errorf("a connection on the previous account answered: %q", got)
		}
		d := pgClient(t, ctx, env.portDSN(), nil)
		one(ctx, d, "BEGIN")
		if got := one(ctx, d, count); got != "3" {
			t.Errorf("a new connection on the new account: %q", got)
		}
		one(ctx, d, "ROLLBACK")
	})

	t.Run("a source that is down, and an account it refuses", func(t *testing.T) {
		e, _ := env.owner.Get(env.serverID)
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		closed := ln.Addr().String()
		ln.Close()
		down := withAccount(t, e.SourceDSN, pgFwdRole, pgFwdPass)
		u, _ := url.Parse(down)
		u.Host = closed
		for _, c := range []struct{ name, route, code, says string }{
			{"pgfwd_down", u.String(), "08001", "cannot connect to the source"},
			{"pgfwd_refused", withAccount(t, e.SourceDSN, pgFwdRole, "wrong-pw"), "08004", "refused the server's forwarding account"},
		} {
			add := console.ServerEntry{Name: c.name, DSN: env.indexDSN, Flavor: console.FlavorPostgres, BaselineDir: e.BaselineDir,
				SourceDSN: e.SourceDSN, SourceSlot: "s", SourcePublication: "p", RouteDSN: c.route}
			if _, err := env.owner.Add(add); err != nil {
				t.Fatal(err)
			}
			env.srv.RefreshFollowedFiles()
			cl := pgClient(t, ctx, strings.Replace(env.portDSN(), "dbname=pgsrv", "dbname="+c.name, 1), nil)
			got := one(ctx, cl, "BEGIN")
			if !strings.HasPrefix(got, "error "+c.code) || !strings.Contains(got, c.says) || strings.Contains(got, "wrong-pw") {
				t.Errorf("%s: %q", c.name, got)
			}
			if cl.TxStatus() != 'I' {
				t.Errorf("%s: status %c after a BEGIN that failed", c.name, cl.TxStatus())
			}
			if got := one(ctx, cl, count); got != "2" {
				t.Errorf("%s: the copy still answers: %q", c.name, got)
			}
		}
	})

	t.Run("psycopg and JDBC run a transaction on the source", func(t *testing.T) {
		python, perr := exec.LookPath("python3")
		if perr == nil {
			perr = exec.CommandContext(ctx, python, "-c", "import psycopg").Run()
		}
		if perr != nil {
			t.Log("psycopg 3 is not installed here")
		} else {
			script := filepath.Join(t.TempDir(), "fwd.py")
			if err := os.WriteFile(script, []byte(psycopgFwdScript), 0o600); err != nil {
				t.Fatal(err)
			}
			out, err := exec.CommandContext(ctx, python, script, env.portDSN()).CombinedOutput()
			if got := strings.TrimSpace(string(out)); err != nil || got != "outside=2 inside=3 three after=2 write=25006" {
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
		src := filepath.Join(t.TempDir(), "Fwd.java")
		if err := os.WriteFile(src, []byte(jdbcFwdProgram), 0o600); err != nil {
			t.Fatal(err)
		}
		jdbcURL := fmt.Sprintf("jdbc:postgresql://%s:%s/pgsrv?user=anyone&password=tok&sslmode=disable", env.host, env.port)
		out, err := exec.CommandContext(ctx, java, "-cp", jar, src, jdbcURL).CombinedOutput()
		if got := strings.TrimSpace(string(out)); err != nil || got != "outside=2 inside=3 three after=2 write=25006" {
			t.Errorf("JDBC: %v %q", err, got)
		}
	})

	t.Run("psql", func(t *testing.T) {
		psql, err := exec.LookPath("psql")
		if err != nil {
			t.Skip("psql is not installed here")
		}
		out, err := exec.CommandContext(ctx, psql, "-X", "-At", "-v", "ON_ERROR_STOP=0",
			"-c", count, "-c", "BEGIN", "-c", count, "-c", "SELECT v FROM "+pgFwdTable+" WHERE id = 3", "-c", "COMMIT", "-c", count,
			env.portDSN()).CombinedOutput()
		if got := strings.Fields(strings.TrimSpace(string(out))); err != nil || strings.Join(got, " ") != "2 BEGIN 3 three COMMIT 2" {
			t.Errorf("psql: %v %q", err, out)
		}
	})
}

// TestIntegrationPGRouterForwardTLS: the connection to the source takes
// TLS as the forwarding account's URL says (sslmode). It needs a PostgreSQL
// that serves TLS: BINTRAIL_TEST_PG_TLS_DSN, as its superuser.
func TestIntegrationPGRouterForwardTLS(t *testing.T) {
	tlsDSN := os.Getenv("BINTRAIL_TEST_PG_TLS_DSN")
	if tlsDSN == "" {
		if os.Getenv("BINTRAIL_REQUIRE_PG_TLS") != "" {
			t.Fatal("BINTRAIL_TEST_PG_TLS_DSN is not set, and BINTRAIL_REQUIRE_PG_TLS says it must be")
		}
		t.Skip("BINTRAIL_TEST_PG_TLS_DSN is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env, _ := pgFwdEnv(t, ctx, 4)
	tlsSource, err := pgconn.Connect(ctx, tlsDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer tlsSource.Close(context.Background())
	for _, s := range []string{"DROP ROLE IF EXISTS " + pgFwdRole, "CREATE ROLE " + pgFwdRole + " LOGIN PASSWORD '" + pgFwdPass + "'"} {
		if _, err := tlsSource.Exec(ctx, s).ReadAll(); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() { _, _ = tlsSource.Exec(context.Background(), "DROP ROLE IF EXISTS "+pgFwdRole).ReadAll() })
	e, _ := env.owner.Get(env.serverID)
	set := func(u *url.URL, mode string) string {
		q := u.Query()
		q.Set("sslmode", mode)
		u.RawQuery = q.Encode()
		return u.String()
	}
	for _, c := range []struct{ mode, want string }{
		{"require", "t"}, {"prefer", "t"}, {"disable", "f"},
		{"verify-full", "error 08001"},
	} {
		u, _ := url.Parse(withAccount(t, tlsDSN, pgFwdRole, pgFwdPass))
		name := "pgtls_" + strings.ReplaceAll(c.mode, "-", "_")
		capture, _ := url.Parse(tlsDSN)
		if _, err := env.owner.Add(console.ServerEntry{Name: name, DSN: env.indexDSN, Flavor: console.FlavorPostgres, BaselineDir: e.BaselineDir,
			SourceDSN: capture.String(), SourceSlot: "s", SourcePublication: "p", RouteDSN: set(u, c.mode)}); err != nil {
			t.Fatal(err)
		}
		env.srv.RefreshFollowedFiles()
		cl := pgClient(t, ctx, strings.Replace(env.portDSN(), "dbname=pgsrv", "dbname="+name, 1), nil)
		got := one(ctx, cl, "BEGIN")
		if got == "BEGIN" {
			got = one(ctx, cl, "SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid()")
			one(ctx, cl, "ROLLBACK")
		}
		if !strings.HasPrefix(got, c.want) {
			t.Errorf("sslmode=%s: %q, want %s", c.mode, got, c.want)
		}
	}
}

const psycopgFwdScript = `import sys
import psycopg

with psycopg.connect(sys.argv[1]) as conn:
    q = "SELECT count(*) FROM ` + pgFwdTable + ` WHERE id > %s"
    conn.autocommit = True
    outside = conn.execute(q, (0,)).fetchone()[0]
    with conn.transaction():
        inside = conn.execute(q, (0,)).fetchone()[0]
        v = conn.execute("SELECT v FROM ` + pgFwdTable + ` WHERE id = %s", (3,)).fetchone()[0]
    after = conn.execute(q, (0,)).fetchone()[0]
    code = ""
    try:
        with conn.transaction():
            conn.execute("INSERT INTO ` + pgFwdTable + ` VALUES (%s, %s)", (77, "w"))
    except psycopg.Error as e:
        code = e.sqlstate
    print(f"outside={outside} inside={inside} {v} after={after} write={code}")
`

const jdbcFwdProgram = `import java.sql.*;

public class Fwd {
    static long count(Connection c) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("SELECT count(*) FROM ` + pgFwdTable + ` WHERE id > ?")) {
            ps.setInt(1, 0);
            try (ResultSet rs = ps.executeQuery()) { rs.next(); return rs.getLong(1); }
        }
    }
    public static void main(String[] args) throws Exception {
        try (Connection c = DriverManager.getConnection(args[0])) {
            long outside = 0;
            for (int i = 0; i < 6; i++) outside = count(c); // past prepareThreshold: a named statement
            c.setAutoCommit(false);
            long inside = count(c);
            String v;
            try (PreparedStatement ps = c.prepareStatement("SELECT v FROM ` + pgFwdTable + ` WHERE id = ?")) {
                ps.setInt(1, 3);
                try (ResultSet rs = ps.executeQuery()) { rs.next(); v = rs.getString(1); }
            }
            c.commit();
            c.setAutoCommit(true);
            long after = count(c);
            String code = "";
            c.setAutoCommit(false);
            try (Statement s = c.createStatement()) {
                s.executeUpdate("INSERT INTO ` + pgFwdTable + ` VALUES (78, 'w')");
            } catch (SQLException e) {
                code = e.getSQLState();
            }
            c.rollback();
            System.out.println("outside=" + outside + " inside=" + inside + " " + v + " after=" + after + " write=" + code);
        }
    }
}
`
