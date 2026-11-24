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
	"github.com/dbtrail/dbtrail/internal/pgshim"
)

// A char(n) column on the PostgreSQL port (#3068), against a real PostgreSQL
// and the snapshot of it the port serves: the copy holds the column as text
// with its padding and compares the padding, PostgreSQL does not, so a
// statement that uses the column where the padding counts is answered by
// the source, and refused by a server that sends nothing there.

const (
	pgCharRole = "dbtrail_fwd_3068"
	pgCharPass = "fwd-pw-3068"
)

// pgCharSetup is the table: code is the char(5), label the same words as
// text.
func pgCharSetup(table string) []string {
	return []string{
		"CREATE TABLE " + table + " (id int PRIMARY KEY, code char(5), label text, k int)",
		"INSERT INTO " + table + " VALUES (1, 'ab', 'ab', 1), (2, 'ab', 'x', 1), (3, 'abcde', 'abcde', 2), (4, 'x', 'ab', 2)",
	}
}

// pgRows is every row of stmt's answer on c, cells joined by | and rows by a
// newline, or "error <code> <message>".
func pgRows(t *testing.T, c *pgconn.PgConn, stmt string) string {
	t.Helper()
	_, rows, err := pgText(t, c, stmt)
	if err != nil {
		return "error " + pgCode(err)
	}
	out := make([]string, len(rows))
	for i, r := range rows {
		out[i] = strings.Join(r, "|")
	}
	return strings.Join(out, "\n")
}

func TestIntegrationPGRouterCharPadding_3068(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	t.Run("with a forwarding account the source answers what the copy would answer otherwise", func(t *testing.T) {
		const table = "pgchr_fwd"
		env := startPGPortFor(t, ctx, nil, pgPortFixture{table: table, slot: "pgchr_fwd_slot", pub: "pgchr_fwd_pub",
			setup: pgCharSetup(table),
			entry: func(e *console.ServerEntry) {
				e.SourceDSN, e.SourceSlot, e.SourcePublication = pgFwdSourceDSN(t), "pgchr_fwd_slot", "pgchr_fwd_pub"
				e.RouteDSN = withAccount(t, e.SourceDSN, pgCharRole, pgCharPass)
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
		// A fifth row the copy's snapshot does not hold: an answer that
		// counts it is the source's.
		exec1("INSERT INTO " + table + " VALUES (5, 'ab', 'ab', 3)")
		exec1(`DO $$ BEGIN IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '` + pgCharRole + `') THEN EXECUTE 'DROP OWNED BY ` + pgCharRole + `'; END IF; END $$`)
		exec1("DROP ROLE IF EXISTS " + pgCharRole)
		exec1("CREATE ROLE " + pgCharRole + " LOGIN PASSWORD '" + pgCharPass + "'")
		exec1("GRANT SELECT ON " + table + " TO " + pgCharRole)
		t.Cleanup(func() {
			bg := context.Background()
			for _, s := range []string{"DROP OWNED BY " + pgCharRole, "DROP ROLE IF EXISTS " + pgCharRole} {
				_, _ = source.Exec(bg, s).ReadAll()
			}
		})
		reasons := func() map[string]uint64 {
			out := map[string]uint64{}
			for k, v := range env.routes.doc("").Servers[env.serverID].Reasons {
				out[k] = v
			}
			return out
		}
		// answered runs stmt on the port and says which reason the port
		// counted for it.
		answered := func(c *pgconn.PgConn, stmt string) (rows, reason string) {
			t.Helper()
			before := reasons()
			rows = pgRows(t, c, stmt)
			for range 100 {
				for k, v := range reasons() {
					if v == before[k]+1 {
						return rows, k
					}
				}
				time.Sleep(20 * time.Millisecond)
			}
			t.Fatalf("%s: answered %q, and the port's status counted nothing for it", stmt, rows)
			return "", ""
		}

		// What the copy answers, first: once the source answered a
		// connection, the copy's older answers are not given to it (#2985).
		// The copy holds four rows, and the port reads a char(n) without its
		// padding (#3219, a decision of the product): 'ab' where PostgreSQL
		// writes 'ab   '.
		c := pgClient(t, ctx, env.portDSN(), nil)
		for _, tc := range []struct{ stmt, want string }{
			{"SELECT count(*) FROM " + table, "4"},
			{"SELECT code FROM " + table + " WHERE id = 1", "ab"},
			{"SELECT id, code FROM " + table + " WHERE k = 1 ORDER BY id", "1|ab\n2|ab"},
			{"SELECT * FROM " + table + " WHERE id = 3", "3|abcde|abcde|2"},
			{"SELECT count(*) FROM " + table + " WHERE label = 'ab'", "2"},
		} {
			got, reason := answered(c, tc.stmt)
			if got != tc.want || reason != pgshim.ReasonAnswersTheSame {
				t.Errorf("%s: %q (%s), want %q from the copy", tc.stmt, got, reason, tc.want)
			}
		}
		// What the copy would answer otherwise: the source's rows, the
		// fifth among them, as a veto. On the copy the first is 0.
		for _, stmt := range []string{
			"SELECT count(*) FROM " + table + " WHERE code = 'ab'",
			"SELECT id FROM " + table + " WHERE code = 'ab' ORDER BY id",
			"SELECT id FROM " + table + " WHERE code <> 'ab' ORDER BY id",
			"SELECT id, length(code) FROM " + table + " ORDER BY id",
			"SELECT id, code || 'x' FROM " + table + " ORDER BY id",
			"SELECT id FROM " + table + " WHERE code = label ORDER BY id",
			"SELECT id FROM " + table + " WHERE code IN ('ab', 'x') ORDER BY id",
			"SELECT a.id, b.id FROM " + table + " a JOIN " + table + " b ON a.code = b.label ORDER BY a.id, b.id",
			"SELECT DISTINCT length(code) FROM " + table + " ORDER BY 1",
			"SELECT label FROM " + table + " INTERSECT SELECT code FROM " + table + " ORDER BY 1",
			"SELECT count(*) FROM (SELECT * FROM " + table + ") s WHERE s.code = 'ab'",
		} {
			want := pgRows(t, source, stmt)
			got, reason := answered(c, stmt)
			if got != want || reason != pgshim.ReasonVeto {
				t.Errorf("%s: %q (%s), want PostgreSQL's %q from the source, as a veto", stmt, got, reason, want)
			}
		}
		if got := pgRows(t, source, "SELECT count(*) FROM "+table+" WHERE code = 'ab'"); got != "3" {
			t.Fatalf("PostgreSQL itself counts %s rows whose code is 'ab', want 3: the test's premise", got)
		}

		// A prepared statement, in each of pgx's modes: an argument compared
		// with the column is the source's too.
		for _, mode := range []pgx.QueryExecMode{pgx.QueryExecModeCacheStatement, pgx.QueryExecModeCacheDescribe, pgx.QueryExecModeDescribeExec, pgx.QueryExecModeExec, pgx.QueryExecModeSimpleProtocol} {
			conn := pgxConn(t, ctx, env.portDSN(), mode)
			for i := range 2 {
				if got := pgxAnswer(ctx, conn, "SELECT count(*) AS n FROM "+table+" WHERE code = $1", "ab"); got != `[n] ["3"]` {
					t.Errorf("mode %v, run %d: a count by the char(5) column: %s, want the source's 3", mode, i, got)
				}
			}
			conn.Close(ctx)
		}
	})

	// A server that hides tables or columns sends nothing to its source, so
	// the copy answers alone and must refuse (0.140.0: the one way a served
	// server still has no source to go to).
	t.Run("a server that sends nothing to its source refuses the statement rather than answer other rows", func(t *testing.T) {
		const table = "pgchr_alone"
		env := startPGPortFor(t, ctx, nil, pgPortFixture{table: table, slot: "pgchr_alone_slot", pub: "pgchr_alone_pub", copyOnly: true, setup: pgCharSetup(table)})
		c := pgClient(t, ctx, env.portDSN(), nil)
		for _, tc := range []struct{ stmt, want string }{
			{"SELECT count(*) FROM " + table, "4"},
			{"SELECT code FROM " + table + " WHERE id = 1", "ab"},
			{"SELECT * FROM " + table + " WHERE id = 3", "3|abcde|abcde|2"},
			{"SELECT count(*) FROM " + table + " WHERE label = 'ab'", "2"},
			// A star over the char(5) column that nothing goes on to read.
			{"SELECT count(*) FROM (SELECT * FROM " + table + ") s", "4"},
			{"WITH x AS (SELECT * FROM " + table + ") SELECT id FROM x WHERE id = 3", "3"},
		} {
			if got := pgRows(t, c, tc.stmt); got != tc.want {
				t.Errorf("%s: %q, want %q", tc.stmt, got, tc.want)
			}
		}
		for _, stmt := range []string{
			"SELECT count(*) FROM " + table + " WHERE code = 'ab'",
			"SELECT id, length(code) FROM " + table,
			"SELECT id, code || 'x' FROM " + table,
			"SELECT count(*) FROM (SELECT * FROM " + table + ") s WHERE s.code = 'ab'",
		} {
			got := pgRows(t, c, stmt)
			if !strings.HasPrefix(got, "error 0A000") || !strings.Contains(got, "char(n)") || !strings.Contains(got, "refused rather than answered with other rows") {
				t.Errorf("%s: %q, want it refused (0A000) for the char(n) column", stmt, got)
			}
		}
		conn := pgxConn(t, ctx, env.portDSN(), pgx.QueryExecModeCacheStatement)
		if got := pgxAnswer(ctx, conn, "SELECT count(*) AS n FROM "+table+" WHERE code = $1", "ab"); !strings.HasPrefix(got, "error 0A000") {
			t.Errorf("a prepared count by the char(5) column: %s, want it refused (0A000)", got)
		}
		if got := pgxAnswer(ctx, conn, "SELECT count(*) AS n FROM "+table+" WHERE label = $1", "ab"); got != `[n] ["2"]` {
			t.Errorf("a prepared count by the text column: %s, want the copy's 2", got)
		}
		if c.TxStatus() != 'I' {
			t.Errorf("the connection is left in status %c", c.TxStatus())
		}
	})
}
