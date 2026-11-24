//go:build integration

package consoleapp

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/pgshim"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// A text column under a nondeterministic ICU collation, through the real port
// (#3111): PostgreSQL finds 'ab' and 'AB' equal, the copy compares bytes. A
// statement that uses the column is answered by the source, or refused with
// no source; one that only shows it, or reads another column, stays on the
// copy.

const pgCollRole, pgCollPass = "dbtrail_fwd_3111", "fwd-pw-3111"

func pgCollSetup(table string) []string {
	return []string{
		"DROP COLLATION IF EXISTS pgcoll_ci CASCADE",
		"CREATE COLLATION pgcoll_ci (provider = icu, locale = 'und-u-ks-level2', deterministic = false)",
		"CREATE TABLE " + table + " (id int PRIMARY KEY, t text COLLATE pgcoll_ci, p text, k int)",
		"INSERT INTO " + table + " VALUES (1,'ab','ab',1),(2,'AB','AB',1),(3,'Ab','Ab',2),(4,'b','b',2),(5,'B','B',3),(6,'a','a',3),(7,'c','c',3)",
	}
}

func pgCollCases(table string) (used []struct{ stmt, want string }, shown []string) {
	used = []struct{ stmt, want string }{
		{"SELECT count(*) FROM " + table + " WHERE t = 'AB'", "3"},
		{"SELECT count(*) FROM " + table + " WHERE t IN ('ab')", "3"},
		{"SELECT count(DISTINCT t) FROM " + table, "4"},
		{"SELECT count(*) FROM (SELECT DISTINCT t FROM " + table + ") s", "4"},
		{"SELECT count(*) FROM (SELECT t FROM " + table + " GROUP BY t) s", "4"},
		{"SELECT count(*) FROM " + table + " a JOIN " + table + " b ON a.t = b.t", "15"},
		{"SELECT id FROM " + table + " ORDER BY t, id", "6\n1\n2\n3\n4\n5\n7"},
	}
	shown = []string{
		"SELECT t FROM " + table + " WHERE id = 2",
		"SELECT * FROM " + table + " WHERE id = 2",
		"SELECT count(*) FROM " + table + " WHERE p = 'AB'",
		"SELECT id, k FROM " + table + " WHERE k > 1 ORDER BY id",
	}
	return used, shown
}

func TestIntegrationPGRouterCollation_3111(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	dropColl := func(dsn string) {
		if c, err := pgconn.Connect(context.Background(), dsn); err == nil {
			_, _ = c.Exec(context.Background(), "DROP COLLATION IF EXISTS pgcoll_ci CASCADE").ReadAll()
			c.Close(context.Background())
		}
	}

	t.Run("with a forwarding account the source answers what uses the column", func(t *testing.T) {
		const table = "pgcoll_fwd"
		env := startPGPortFor(t, ctx, nil, pgPortFixture{table: table, slot: "pgcoll_fwd_slot", pub: "pgcoll_fwd_pub", setup: pgCollSetup(table),
			entry: func(e *console.ServerEntry) {
				e.SourceDSN, e.SourceSlot, e.SourcePublication = pgFwdSourceDSN(t), "pgcoll_fwd_slot", "pgcoll_fwd_pub"
				e.RouteDSN = withAccount(t, e.SourceDSN, pgCollRole, pgCollPass)
			}})
		t.Cleanup(func() { dropColl(env.sourceDSN) })
		source, err := pgconn.Connect(ctx, env.sourceDSN)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { source.Close(context.Background()) })
		for _, s := range []string{
			`DO $$ BEGIN IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '` + pgCollRole + `') THEN EXECUTE 'DROP OWNED BY ` + pgCollRole + `'; END IF; END $$`,
			"DROP ROLE IF EXISTS " + pgCollRole, "CREATE ROLE " + pgCollRole + " LOGIN PASSWORD '" + pgCollPass + "'", "GRANT SELECT ON " + table + " TO " + pgCollRole,
		} {
			if _, err := source.Exec(ctx, s).ReadAll(); err != nil {
				t.Fatalf("source: %s: %v", s, err)
			}
		}
		t.Cleanup(func() {
			for _, s := range []string{"DROP OWNED BY " + pgCollRole, "DROP ROLE IF EXISTS " + pgCollRole} {
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
		used, shown := pgCollCases(table)
		for _, extended := range []bool{false, true} {
			for _, tc := range used {
				if want := pgUserRows(t, ctx, source, tc.stmt, extended); want != tc.want {
					t.Fatalf("PostgreSQL itself: %s: %q, want %q: the test's premise", tc.stmt, want, tc.want)
				}
				if got, why := answered(tc.stmt, extended); got != tc.want || why != pgshim.ReasonVeto {
					t.Errorf("extended=%v: %s: %q (%s), want PostgreSQL's %q from the source", extended, tc.stmt, got, why, tc.want)
				}
			}
			for _, stmt := range shown {
				want := pgUserRows(t, ctx, source, stmt, extended)
				if got, why := answered(stmt, extended); got != want || why == pgshim.ReasonVeto {
					t.Errorf("extended=%v: %s: %q (%s), want %q, not vetoed", extended, stmt, got, why, want)
				}
			}
		}

		// A column changed with ALTER ... COLLATE after the snapshot keeps its
		// type: the source's catalog is what says it, and what counts.
		if _, err := source.Exec(ctx, "ALTER TABLE "+table+" ALTER COLUMN p TYPE text COLLATE pgcoll_ci").ReadAll(); err != nil {
			t.Fatal(err)
		}
		pgCollationCache.mu.Lock()
		pgCollationCache.byID = nil
		pgCollationCache.mu.Unlock()
		c = pgClient(t, ctx, env.portDSN(), nil)
		const altered = "SELECT count(*) FROM " + table + " WHERE p = 'AB'"
		if got, why := answered(altered, false); got != "3" || why != pgshim.ReasonVeto {
			t.Errorf("after ALTER ... COLLATE on the source: %s: %q (%s), want PostgreSQL's 3 from the source", altered, got, why)
		}

		// The source's catalog, for a snapshot that does not record
		// collations: read through the forwarding account, metadata only.
		nondet, listed, err := pgSourceCollations(withAccount(t, pgFwdSourceDSN(t), pgCollRole, pgCollPass), "test")(ctx, [][2]string{{"public", table}, {"public", "pgcoll_absent"}})
		if err != nil {
			t.Fatalf("the source's collations: %v", err)
		}
		// p was made nondeterministic above; k holds no text.
		if !nondet[[3]string{"public", table, "t"}] || !nondet[[3]string{"public", table, "p"}] || nondet[[3]string{"public", table, "k"}] || !listed[[2]string{"public", table}] || listed[[2]string{"public", "pgcoll_absent"}] {
			t.Errorf("the source's collations: %v, listed %v", nondet, listed)
		}
	})

	t.Run("without one a statement that uses the column is refused", func(t *testing.T) {
		const table = "pgcoll_alone"
		env := startPGPortFor(t, ctx, nil, pgPortFixture{table: table, slot: "pgcoll_alone_slot", pub: "pgcoll_alone_pub", setup: pgCollSetup(table)})
		t.Cleanup(func() { dropColl(env.sourceDSN) })
		source, err := pgconn.Connect(ctx, env.sourceDSN)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { source.Close(context.Background()) })
		c := pgClient(t, ctx, env.portDSN(), nil)
		used, shown := pgCollCases(table)
		for _, extended := range []bool{false, true} {
			for _, tc := range used {
				if got := pgUserRows(t, ctx, c, tc.stmt, extended); !strings.HasPrefix(got, "error 0A000") {
					t.Errorf("extended=%v: %s: %q, want it refused (0A000)", extended, tc.stmt, got)
				}
			}
			for _, stmt := range shown {
				want := pgUserRows(t, ctx, source, stmt, extended)
				if got := pgUserRows(t, ctx, c, stmt, extended); got != want {
					t.Errorf("extended=%v: %s: %q, want the copy's %q", extended, stmt, got, want)
				}
			}
		}
	})

	t.Run("a database's default collation is deterministic", func(t *testing.T) {
		base := testutil.SkipIfNoPostgres(t)
		adm, err := pgconn.Connect(ctx, base)
		if err != nil {
			t.Fatal(err)
		}
		defer adm.Close(context.Background())
		_, _ = adm.Exec(ctx, "DROP DATABASE IF EXISTS pgcoll_db").ReadAll()
		if _, err := adm.Exec(ctx, "CREATE DATABASE pgcoll_db TEMPLATE template0 LOCALE_PROVIDER icu ICU_LOCALE 'und-u-ks-level2' LOCALE 'C'").ReadAll(); err != nil {
			t.Skipf("no ICU database here: %v", err)
		}
		t.Cleanup(func() { _, _ = adm.Exec(context.Background(), "DROP DATABASE IF EXISTS pgcoll_db").ReadAll() })
		cfg, err := pgconn.ParseConfig(base)
		if err != nil {
			t.Fatal(err)
		}
		cfg.Database = "pgcoll_db"
		db, err := pgconn.ConnectConfig(ctx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close(context.Background())
		// Even with a case-insensitive ICU locale, the default compares
		// every byte: PostgreSQL has no nondeterministic default.
		if got := pgRows(t, db, "SELECT 'a' = 'A', collisdeterministic FROM pg_collation WHERE collname = 'default'"); got != "f|t" {
			t.Errorf("a database whose default is case-insensitive ICU: %q, want f|t", got)
		}
	})
}
