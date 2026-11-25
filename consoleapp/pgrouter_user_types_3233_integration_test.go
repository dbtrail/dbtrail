//go:build integration

package consoleapp

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/pgshim"
)

// A column of a type that is not built in, through the real port (#3233): a
// statement that reads one is answered by PostgreSQL, or refused where there
// is no source; one on the same table that does not read it stays on the copy.

const (
	pgUserRole   = "dbtrail_fwd_3233"
	pgUserPass   = "fwd-pw-3233"
	pgUserDomain = "pguser_code"
	pgUserOver   = "pguser_code2"
	pgUserEnum   = "pguser_level"
)

func pgUserSetup(table string) []string {
	return []string{
		"DROP TYPE IF EXISTS " + pgUserEnum + " CASCADE",
		"DROP DOMAIN IF EXISTS " + pgUserOver + " CASCADE",
		"DROP DOMAIN IF EXISTS " + pgUserDomain + " CASCADE",
		"CREATE DOMAIN " + pgUserDomain + " AS char(4)",
		"CREATE DOMAIN " + pgUserOver + " AS " + pgUserDomain,
		"CREATE TYPE " + pgUserEnum + " AS ENUM ('z', 'a')",
		"CREATE TABLE " + table + " (id int PRIMARY KEY, dc " + pgUserDomain + ", dd " + pgUserOver + ", lv " + pgUserEnum + ", n int, label text)",
		"INSERT INTO " + table + " SELECT i, 'a', 'a', CASE WHEN i % 2 = 0 THEN 'z' ELSE 'a' END::" + pgUserEnum + ", i * 10, 'w' || i FROM generate_series(1, 9) i",
	}
}

func pgUserCases(table string) (read []struct{ stmt, want string }, notRead []string) {
	read = []struct{ stmt, want string }{
		{"SELECT count(*) FROM " + table + " WHERE dc = 'a'", "9"},
		{"SELECT sum(length(dc)) FROM " + table, "9"},
		{"SELECT count(*) FROM " + table + " WHERE dd = 'a'", "9"},
		{"SELECT min(lv) FROM " + table, "z"},
		{"SELECT id FROM " + table + " ORDER BY lv, id LIMIT 1", "2"},
		{"SELECT count(*) FROM " + table + " AS \"a\"(i, x) WHERE x = 'a'", "9"},
	}
	notRead = []string{
		"SELECT id, n FROM " + table + " WHERE id > 2 ORDER BY id",
		"SELECT dc, dd, lv FROM " + table + " WHERE id = 1",
		"SELECT * FROM " + table + " WHERE id = 1",
		"SELECT count(*) FROM " + table,
		"SELECT count(*) FROM (SELECT * FROM " + table + ") s",
		"WITH x AS (SELECT * FROM " + table + ") SELECT id, label FROM x ORDER BY id",
	}
	return read, notRead
}

// pgUserRows is the answer of stmt in one protocol, cells joined by | and rows
// by a newline, or "error <code> <message>".
func pgUserRows(t *testing.T, ctx context.Context, c *pgconn.PgConn, stmt string, extended bool) string {
	t.Helper()
	if !extended {
		return pgRows(t, c, stmt)
	}
	r := c.ExecParams(ctx, stmt, nil, nil, nil, nil).Read()
	if r.Err != nil {
		return "error " + pgCode(r.Err)
	}
	out := ""
	for i, row := range r.Rows {
		if i > 0 {
			out += "\n"
		}
		for j, v := range row {
			if j > 0 {
				out += "|"
			}
			if v == nil {
				out += "NULL"
			} else {
				out += string(v)
			}
		}
	}
	return out
}

func TestIntegrationPGRouterUserTypes_3233(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	dropTypes := func(source *pgconn.PgConn) {
		for _, s := range []string{"DROP TYPE IF EXISTS " + pgUserEnum + " CASCADE", "DROP DOMAIN IF EXISTS " + pgUserOver + " CASCADE", "DROP DOMAIN IF EXISTS " + pgUserDomain + " CASCADE"} {
			_, _ = source.Exec(context.Background(), s).ReadAll()
		}
	}

	t.Run("with a forwarding account the source answers what reads the column", func(t *testing.T) {
		const table = "pguser_fwd"
		env := startPGPortFor(t, ctx, nil, pgPortFixture{table: table, slot: "pguser_fwd_slot", pub: "pguser_fwd_pub",
			setup: pgUserSetup(table),
			entry: func(e *console.ServerEntry) {
				e.SourceDSN, e.SourceSlot, e.SourcePublication = pgFwdSourceDSN(t), "pguser_fwd_slot", "pguser_fwd_pub"
				e.RouteDSN = withAccount(t, e.SourceDSN, pgUserRole, pgUserPass)
			}})
		source, err := pgconn.Connect(ctx, env.sourceDSN)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { source.Close(context.Background()) })
		t.Cleanup(func() { dropTypes(source) })
		exec1 := func(stmt string) {
			t.Helper()
			if _, err := source.Exec(ctx, stmt).ReadAll(); err != nil {
				t.Fatalf("source: %s: %v", stmt, err)
			}
		}
		exec1(`DO $$ BEGIN IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '` + pgUserRole + `') THEN EXECUTE 'DROP OWNED BY ` + pgUserRole + `'; END IF; END $$`)
		exec1("DROP ROLE IF EXISTS " + pgUserRole)
		exec1("CREATE ROLE " + pgUserRole + " LOGIN PASSWORD '" + pgUserPass + "'")
		exec1("GRANT SELECT ON " + table + " TO " + pgUserRole)
		t.Cleanup(func() {
			for _, s := range []string{"DROP OWNED BY " + pgUserRole, "DROP ROLE IF EXISTS " + pgUserRole} {
				_, _ = source.Exec(context.Background(), s).ReadAll()
			}
		})
		c := pgClient(t, ctx, env.portDSN(), nil)
		reasons := func() map[string]uint64 {
			out := map[string]uint64{}
			for k, v := range env.routes.doc("").Servers[env.serverID].Reasons {
				out[k] = v
			}
			return out
		}
		answered := func(stmt string, extended bool) (string, string) {
			t.Helper()
			before := reasons()
			rows := pgUserRows(t, ctx, c, stmt, extended)
			for range 150 {
				for k, v := range reasons() {
					if v > before[k] {
						return rows, k
					}
				}
				time.Sleep(20 * time.Millisecond)
			}
			t.Fatalf("%s: answered %q, and the port counted nothing for it", stmt, rows)
			return "", ""
		}
		read, notRead := pgUserCases(table)
		for _, extended := range []bool{false, true} {
			for _, tc := range read {
				if want := pgUserRows(t, ctx, source, tc.stmt, extended); want != tc.want {
					t.Fatalf("PostgreSQL itself: %s: %q, want %q: the test's premise", tc.stmt, want, tc.want)
				}
				if got, why := answered(tc.stmt, extended); got != tc.want || why != pgshim.ReasonVeto {
					t.Errorf("extended=%v: %s: %q (%s), want PostgreSQL's %q from the source", extended, tc.stmt, got, why, tc.want)
				}
			}
			for _, stmt := range notRead {
				want := pgUserRows(t, ctx, source, stmt, extended)
				if got, why := answered(stmt, extended); got != want || why == pgshim.ReasonVeto {
					t.Errorf("extended=%v: %s: %q (%s), want the copy's %q", extended, stmt, got, why, want)
				}
			}
		}
		// Described by the side that will run it: the source says bpchar,
		// the copy would say text.
		if sd, err := c.Prepare(ctx, "pguser_arg", "SELECT $1 AS v", []uint32{1042}); err != nil {
			t.Errorf("prepare with an argument declared bpchar: %v", err)
		} else if got := sd.Fields[0].DataTypeOID; got != 1042 {
			t.Errorf("an argument declared bpchar is described as type %d, want the source's 1042", got)
		}
		// An argument declared bpchar compares without its trailing blanks.
		if r := c.ExecParams(ctx, "SELECT $1 = 'a'", [][]byte{[]byte("a  ")}, []uint32{1042}, nil, nil).Read(); r.Err != nil || len(r.Rows) != 1 || string(r.Rows[0][0]) != "t" {
			t.Errorf("an argument declared bpchar: %v, %v; want PostgreSQL's true", r.Rows, r.Err)
		}
	})

	// A server that hides tables or columns sends nothing to its source, so
	// the copy answers alone and must refuse (0.140.0).
	t.Run("without one a statement that reads the column is refused", func(t *testing.T) {
		const table = "pguser_alone"
		env := startPGPortFor(t, ctx, nil, pgPortFixture{table: table, slot: "pguser_alone_slot", pub: "pguser_alone_pub", copyOnly: true, setup: pgUserSetup(table)})
		source, err := pgconn.Connect(ctx, env.sourceDSN)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { source.Close(context.Background()) })
		t.Cleanup(func() { dropTypes(source) })
		c := pgClient(t, ctx, env.portDSN(), nil)
		read, notRead := pgUserCases(table)
		for _, extended := range []bool{false, true} {
			for _, tc := range read {
				if got := pgUserRows(t, ctx, c, tc.stmt, extended); len(got) < 11 || got[:11] != "error 0A000" {
					t.Errorf("extended=%v: %s: %q, want it refused (0A000)", extended, tc.stmt, got)
				}
			}
			for _, stmt := range notRead {
				want := pgUserRows(t, ctx, source, stmt, extended)
				if got := pgUserRows(t, ctx, c, stmt, extended); got != want {
					t.Errorf("extended=%v: %s: %q, want the copy's %q", extended, stmt, got, want)
				}
			}
		}
		for _, oid := range []uint32{1042, 869} { // bpchar, inet
			if r := c.ExecParams(ctx, "SELECT $1 = '10.0.0.1'", [][]byte{[]byte("10.0.0.1/32")}, []uint32{oid}, nil, nil).Read(); r.Err == nil || pgCode(r.Err)[:5] != "0A000" {
				t.Errorf("an argument of type %d: %v, %v; want it refused (0A000)", oid, r.Rows, r.Err)
			}
		}
	})
}
