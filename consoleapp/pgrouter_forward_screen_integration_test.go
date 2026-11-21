//go:build integration

package consoleapp

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// The port sends the source only reads, over a read-only connection. This
// holds that promise against a real PostgreSQL for texts written so that one
// reader of SQL would see less than PostgreSQL runs: the forwarding role is
// granted INSERT on purpose, so a write that got through would show as a row.

// pgFwdExt runs stmt on the extended protocol (Parse, Bind, Execute) and
// answers as one does.
func pgFwdExt(ctx context.Context, c *pgconn.PgConn, stmt string) string {
	res := c.ExecParams(ctx, stmt, nil, nil, nil, nil).Read()
	if res.Err != nil {
		return "error " + pgCode(res.Err)
	}
	if len(res.Rows) == 0 {
		return res.CommandTag.String()
	}
	var cells []string
	for _, v := range res.Rows[0] {
		cells = append(cells, string(v))
	}
	return strings.Join(cells, "|")
}

func TestIntegrationPGRouterForwardScreen(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	env, source := pgFwdEnv(t, ctx, 2)
	tbl := pgFwdTable
	rows := func() string { return one(ctx, source, "SELECT count(*) FROM "+tbl) }
	if got := rows(); got != "3" {
		t.Fatalf("the source starts with %s rows, want 3", got)
	}
	ins := "INSERT INTO " + tbl + " VALUES (70, 'w')"
	// clean undoes a write that got through, so the next text is judged by
	// itself.
	clean := func() {
		one(ctx, source, "DELETE FROM "+tbl+" WHERE id IN (70, 99)")
		one(ctx, source, "DROP TABLE IF EXISTS pgfwd_made")
	}
	protocols := []struct {
		name string
		run  func(context.Context, *pgconn.PgConn, string) string
	}{{"simple", one}, {"extended", pgFwdExt}}

	t.Run("a block is opened read-only whatever is written around its BEGIN", func(t *testing.T) {
		begins := []string{
			"BEGIN", "BEGIN -- c\n", "BEGIN -- c\r", "BEGIN -- c\r\n", "/* c */ BEGIN /* c */ WORK;", "START TRANSACTION READ ONLY",
			"BEGIN ISOLATION LEVEL READ COMMITTED",
			"BEGIN --\rREAD WRITE", "BEGIN -- c\r READ WRITE", "BEGIN --\r\nREAD WRITE", "BEGIN --\nREAD WRITE", "BEGIN /* c */ READ WRITE",
			"START TRANSACTION --\rREAD WRITE", "BEGIN READ WRITE",
			"BEGIN --\rREAD WRITE; " + ins, "BEGIN --\r; " + ins + "; COMMIT", "BEGIN; " + ins + "; COMMIT",
		}
		recursive := "WITH RECURSIVE w(a) AS (SELECT 1 UNION ALL SELECT a + 1 FROM w WHERE a < 2) "
		writes := []string{
			recursive + "SEARCH DEPTH FIRST BY a SET o " + ins,
			recursive + "CYCLE a SET c USING p " + ins,
			"WITH w AS (SELECT 1) " + ins,
			"WITH x AS (WITH y AS (SELECT 1) DELETE FROM " + tbl + " WHERE id = 3 RETURNING *) SELECT * FROM x",
			"WITH w AS (" + ins + " RETURNING id) SELECT * FROM w",
			ins,
			"EXPLAIN ANALYZE " + ins,
			"SELECT * INTO pgfwd_made FROM " + tbl,
			"SELECT * INTO TEMP pgfwd_made FROM " + tbl,
			"SELECT pgfwd_write()",
			"SELECT 1 --\r; " + ins,
			"SET TRANSACTION READ WRITE",
			"SET TRANSACTION --\rREAD WRITE",
			"SAVEPOINT a; " + ins,
		}
		// Asked of the source, not of the port: the alias keeps the port from
		// answering it from its own settings.
		const readOnly = "SELECT current_setting('transaction_read_only') AS ro"
		for _, p := range protocols {
			for _, begin := range begins {
				c := pgClient(t, ctx, env.portDSN(), nil)
				for _, w := range writes {
					opened := p.run(ctx, c, begin)
					if c.TxStatus() != 'T' {
						if !strings.HasPrefix(opened, "error ") {
							t.Errorf("%s %q: answered %q and opened no block", p.name, begin, opened)
						}
						p.run(ctx, c, "ROLLBACK")
						break
					}
					if got := p.run(ctx, c, readOnly); got != "on" {
						t.Errorf("%s %q: the source says the block's transaction_read_only is %q", p.name, begin, got)
					}
					if got := p.run(ctx, c, w); !strings.HasPrefix(got, "error ") {
						t.Errorf("%s after %q: %q answered %q, want an error", p.name, begin, w, got)
					}
					p.run(ctx, c, "COMMIT -- c\r")
					if c.TxStatus() != 'I' {
						t.Fatalf("%s after %q and %q: the block did not end", p.name, begin, w)
					}
					if got := rows(); got != "3" {
						t.Errorf("%s: %q, then %q, then COMMIT: the source has %s rows, want 3: a write got through", p.name, begin, w, got)
						clean()
					}
					if got := one(ctx, source, "SELECT count(*) FROM pg_class WHERE relname = 'pgfwd_made'"); got != "0" {
						t.Errorf("%s: %q, then %q, then COMMIT: a table was made on the source", p.name, begin, w)
						clean()
					}
				}
				c.Close(ctx)
			}
		}
	})

	t.Run("what one reader would not see is read, and refused", func(t *testing.T) {
		texts := []string{
			"SELECT 1 --\r; " + ins,
			"SELECT 1 -- c\r\n; " + ins,
			"SELECT 1 AS a$t$; " + ins + "; SELECT 2 AS b$t$",
			`SELECT 1 AS mode'\'; ` + ins + ` --'`,
			"SELECT e'a'\n'\\' , ' ; " + ins + " --'",
			"SELECT 1 --\r, set_config('default_transaction_read_only', 'off', false)",
			"SELECT 1 AS a$$, set_config('default_transaction_read_only', 'off', false) AS b$$",
			"WITH w AS (" + ins + " RETURNING id) SELECT * FROM w",
			"EXPLAIN ANALYZE " + ins,
			"EXPLAIN (ANALYZE) " + ins,
			"SELECT * INTO pgfwd_made FROM " + tbl,
			"SELECT * INTO TEMP pgfwd_made FROM " + tbl,
			"SELECT * FROM " + tbl + " FOR UPDATE",
			"SELECT * FROM " + tbl + " FOR SHARE",
			"SELECT pg_terminate_backend(pg_backend_pid())",
			"SELECT pg_cancel_backend(pg_backend_pid())",
			"SELECT pg_stat_reset()",
			"SELECT lo_create(0)",
			"SELECT pg_read_file('PG_VERSION')",
			"SELECT pg_ls_dir('.')",
			"SELECT pg_switch_wal()",
			"SELECT pg_reload_conf()",
			"SELECT pg_create_physical_replication_slot('pgfwd_made')",
			"SELECT query_to_xml('" + strings.ReplaceAll(ins, "'", "''") + "', true, false, '')",
			"SELECT nextval('pgfwd_made')",
			"SELECT pg_logical_emit_message(false, 'a', 'b')",
			"SELECT txid_current()",
		}
		for _, p := range protocols {
			for _, inBlock := range []bool{false, true} {
				for _, q := range texts {
					c := pgClient(t, ctx, env.portDSN(), nil)
					if inBlock {
						p.run(ctx, c, "BEGIN")
					}
					got := p.run(ctx, c, q)
					if !strings.HasPrefix(got, "error ") {
						t.Errorf("%s, in a block %v: %q answered %q, want an error", p.name, inBlock, q, got)
					}
					if got := rows(); got != "3" {
						t.Errorf("%s, in a block %v: %q: the source has %s rows, want 3: a write got through", p.name, inBlock, q, got)
						clean()
					}
					if got := one(ctx, source, "SELECT count(*) FROM pg_class WHERE relname = 'pgfwd_made'"); got != "0" {
						t.Errorf("%s, in a block %v: %q made a table on the source", p.name, inBlock, q)
						clean()
					}
					c.Close(ctx)
				}
			}
		}
	})

	// A BEGIN that arrives while a batch of the extended protocol is open on
	// the source is opened and verified there in the middle of that batch.
	t.Run("a BEGIN inside an open batch is verified too", func(t *testing.T) {
		c := pgClient(t, ctx, env.portDSN(), nil)
		defer c.Close(ctx)
		// The first statement is one the source answers outside a block (a
		// name the copy's reader does not take), so the batch is open there
		// when the BEGIN comes; twice, so the port's own statements in one
		// batch follow one another.
		batch := []string{
			"SELECT count(*) AS a$t$ FROM " + tbl,
			"BEGIN -- c\r",
			"SELECT current_setting('transaction_read_only') AS ro",
			"WITH w AS (" + ins + " RETURNING id) SELECT * FROM w",
		}
		want := []string{"3", "BEGIN", "on", "error"}
		pl := c.StartPipeline(ctx)
		for _, q := range batch {
			pl.SendQueryParams(q, nil, nil, nil, nil)
		}
		if err := pl.Sync(); err != nil {
			t.Fatal(err)
		}
		var got []string
		for {
			res, err := pl.GetResults()
			if err != nil {
				got = append(got, "error")
				continue
			}
			if res == nil {
				break
			}
			rr, ok := res.(*pgconn.ResultReader)
			if !ok {
				continue
			}
			r := rr.Read()
			switch {
			case r.Err != nil:
				got = append(got, "error")
			case len(r.Rows) > 0:
				got = append(got, string(r.Rows[0][0]))
			default:
				got = append(got, r.CommandTag.String())
			}
		}
		if err := pl.Close(); err != nil {
			t.Logf("closing the pipeline: %v", err)
		}
		if strings.Join(got, "|") != strings.Join(want, "|") {
			t.Errorf("the batch answered %q, want %q", got, want)
		}
		one(ctx, c, "ROLLBACK")
		if got := rows(); got != "3" {
			t.Errorf("the source has %s rows after the batch, want 3: a write got through", got)
			clean()
		}
	})

	t.Run("reads still go, written with the same pieces", func(t *testing.T) {
		count := "SELECT count(*) FROM " + tbl
		for _, p := range protocols {
			c := pgClient(t, ctx, env.portDSN(), nil)
			p.run(ctx, c, "BEGIN -- c\r")
			for _, r := range [][2]string{
				{count + " --\r", "3"},
				{count + " -- c\r\n", "3"},
				{"/* a /* b */ c */ " + count, "3"},
				{"SELECT count(*) AS a$t$ FROM " + tbl, "3"},
				{"SELECT 'it''s', e'a\\'b', $$a;b$$, $t$a$t$", "it's|a'b|a;b|a"},
				{"SELECT 'a'\n'b'", "ab"},
				{"SELECT v AS mode FROM " + tbl + " WHERE id = 1", "one"},
				{"SELECT substring(v for 2) FROM " + tbl + " WHERE id = 3", "th"},
				{"WITH w AS (" + count + ") SELECT * FROM w", "3"},
			} {
				if got := p.run(ctx, c, r[0]); got != r[1] {
					t.Errorf("%s: %q answered %q, want %q", p.name, r[0], got, r[1])
				}
			}
			if got := p.run(ctx, c, "EXPLAIN ANALYZE "+count); strings.HasPrefix(got, "error ") {
				t.Errorf("%s: EXPLAIN ANALYZE of a query: %q", p.name, got)
			}
			if got := p.run(ctx, c, "COMMIT"); got != "COMMIT" {
				t.Errorf("%s: COMMIT: %q", p.name, got)
			}
			c.Close(ctx)
		}
	})

	// What PostgreSQL itself does with each of these in a read-only
	// transaction, as the forwarding role: logged, as the record of why the
	// screen refuses what it refuses rather than leave it to the transaction.
	t.Run("what a read-only transaction refuses by itself", func(t *testing.T) {
		direct, err := pgconn.Connect(ctx, withAccount(t, env.sourceDSN, pgFwdRole, pgFwdPass))
		if err != nil {
			t.Fatal(err)
		}
		defer direct.Close(ctx)
		one(ctx, source, "CREATE SEQUENCE IF NOT EXISTS pgfwd_seq")
		one(ctx, source, "GRANT USAGE ON SEQUENCE pgfwd_seq TO "+pgFwdRole)
		defer one(ctx, source, "DROP SEQUENCE IF EXISTS pgfwd_seq")
		recursive := "WITH RECURSIVE w(a) AS (SELECT 1 UNION ALL SELECT a + 1 FROM w WHERE a < 2) "
		for _, q := range []string{
			ins,
			"WITH w AS (" + ins + " RETURNING id) SELECT * FROM w",
			recursive + "SEARCH DEPTH FIRST BY a SET o " + ins,
			recursive + "CYCLE a SET c USING p " + ins,
			"EXPLAIN ANALYZE " + ins,
			"SELECT * FROM " + tbl + " FOR UPDATE",
			"SELECT * FROM " + tbl + " FOR SHARE",
			"SELECT * INTO pgfwd_made FROM " + tbl,
			"SELECT * INTO TEMP pgfwd_made_tmp FROM " + tbl,
			"SELECT nextval('pgfwd_seq')",
			"SELECT pg_cancel_backend(pg_backend_pid())",
			"SELECT lo_create(0)",
			"SELECT pg_stat_reset()",
			"SELECT pg_read_file('PG_VERSION')",
			"SELECT pg_ls_dir('.')",
			"SELECT pg_switch_wal()",
			"SELECT pg_reload_conf()",
			"SELECT pg_create_physical_replication_slot('pgfwd_made')",
			"SELECT pg_logical_emit_message(false, 'a', 'b')",
			"SELECT pg_notify('pgfwd', 'x')",
			"SELECT pg_advisory_xact_lock(1)",
			"SELECT txid_current()",
			"SELECT query_to_xml('SELECT 1 AS a', true, false, '')",
		} {
			one(ctx, direct, "BEGIN READ ONLY")
			got := one(ctx, direct, q)
			one(ctx, direct, "ROLLBACK")
			verdict := "NOT refused by PostgreSQL"
			if strings.HasPrefix(got, "error ") {
				verdict = "refused by PostgreSQL"
			}
			t.Logf("read-only transaction, ordinary role: %-70s %s (%s)", q, verdict, got)
		}
		if got := rows(); got != "3" {
			t.Errorf("the source has %s rows after the read-only transactions, want 3", got)
		}
	})
}
