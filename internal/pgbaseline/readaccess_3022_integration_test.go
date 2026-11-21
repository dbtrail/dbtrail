//go:build integration

package pgbaseline_test

import (
	"context"
	"errors"
	"net/url"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/pgbaseline"
	"github.com/dbtrail/dbtrail/internal/pgcapture"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// #3022 through the snapshot itself, against a real PostgreSQL and a role
// that may replicate and has no SELECT, which is the role of the issue.
//
// Without the check the snapshot fails as it did there: 42501 from the COPY,
// after the slot was created. With it, the snapshot is refused before the
// slot exists and before anything is written, naming the table and the
// statement to run; and once that statement is run, the same role's snapshot
// works.
func TestPGBaseline_RoleWithoutSelect_3022(t *testing.T) {
	baseDSN := testutil.SkipIfNoPostgres(t)
	ctx := context.Background()
	const (
		pub      = "bintrail_pgbaseline_ra3022_pub"
		slot     = "bintrail_pgbaseline_ra3022_slot"
		role     = "pgbaseline_ra3022_role"
		password = "ra3022 pw"
		readable = "pgbaseline_ra3022_orders"
		denied   = "pgbaseline_ra3022_customers"
	)
	admin, err := pgx.Connect(ctx, baseDSN)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { admin.Close(context.Background()) })
	dropSlot := func() {
		_, _ = admin.Exec(context.Background(), "SELECT pg_drop_replication_slot($1) WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name=$1)", slot)
	}
	drop := func() {
		bg := context.Background()
		_, _ = admin.Exec(bg, "DROP PUBLICATION IF EXISTS "+pub)
		_, _ = admin.Exec(bg, "DROP TABLE IF EXISTS "+readable+", "+denied)
		dropSlot()
		_, _ = admin.Exec(bg, "DROP ROLE IF EXISTS "+role)
	}
	drop()
	t.Cleanup(drop)
	for _, stmt := range []string{
		"CREATE TABLE " + readable + " (id int PRIMARY KEY, v text)",
		"CREATE TABLE " + denied + " (id int PRIMARY KEY, name text)",
		"INSERT INTO " + readable + " VALUES (1, 'a')",
		"INSERT INTO " + denied + " VALUES (1, 'Ada'), (2, 'Grace')",
		"CREATE PUBLICATION " + pub + " FOR TABLE " + readable + ", " + denied,
		"CREATE ROLE " + role + " LOGIN REPLICATION PASSWORD '" + password + "'",
		"GRANT SELECT ON " + readable + " TO " + role,
	} {
		if _, err := admin.Exec(ctx, stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	u, err := url.Parse(baseDSN)
	if err != nil {
		t.Fatal(err)
	}
	u.User = url.UserPassword(role, password)
	roleDSN := u.String()
	slotExists := func() bool {
		var n int
		if err := admin.QueryRow(ctx, "SELECT count(*) FROM pg_replication_slots WHERE slot_name = $1", slot).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n > 0
	}
	run := func(outDir string) error {
		_, err := pgbaseline.Run(ctx, pgbaseline.Config{
			QueryDSN: roleDSN, ReplDSN: replDSN(roleDSN), SlotName: slot, Publication: pub, OutputDir: outDir, Parallelism: 1,
		})
		return err
	}

	t.Run("without the check: 42501 from the COPY, as in the issue", func(t *testing.T) {
		restore := pgbaseline.SetCheckReadAccess(func(context.Context, *pgx.Conn, string, event.Filters) error { return nil })
		defer restore()
		err := run(t.TempDir())
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "42501" || !strings.Contains(err.Error(), "permission denied for table "+denied) {
			t.Fatalf("err = %v, want permission denied for table %s (SQLSTATE 42501)", err, denied)
		}
		if !slotExists() {
			t.Error("the failed snapshot left no slot: the issue's run had created it by then")
		}
		dropSlot()
	})

	t.Run("refused before anything is created", func(t *testing.T) {
		outDir := t.TempDir()
		err := run(outDir)
		const want = `pgbaseline: this snapshot was not started: no read access: ` +
			`the role "pgbaseline_ra3022_role" cannot read 1 of the 2 tables the copy reads. ` +
			`Run on the database, as the tables' owner or a superuser: GRANT SELECT ON TABLE "public"."pgbaseline_ra3022_customers" TO "pgbaseline_ra3022_role";`
		if err == nil || err.Error() != want {
			t.Fatalf("err:\n got %v\nwant %s", err, want)
		}
		// The web interface shows the first 300 characters of a failed
		// snapshot's error, behind the prefix its supervisor adds: the
		// statement to run has to be inside them.
		if shown := "pg baseline: " + err.Error(); len(shown) > 300 {
			t.Errorf("the refusal is %d characters long; past 300 the statement is cut on screen", len(shown))
		}
		if !errors.Is(err, pgcapture.ErrNoReadAccess) {
			t.Error("the refusal does not wrap pgcapture.ErrNoReadAccess")
		}
		if slotExists() {
			t.Error("a refused snapshot created the replication slot")
		}
		if entries, _ := os.ReadDir(outDir); len(entries) != 0 {
			t.Errorf("a refused snapshot wrote into its folder: %v", entries)
		}
	})

	t.Run("a table outside the filter does not refuse the snapshot", func(t *testing.T) {
		_, err := pgbaseline.Run(ctx, pgbaseline.Config{
			QueryDSN: roleDSN, ReplDSN: replDSN(roleDSN), SlotName: slot, Publication: pub, OutputDir: t.TempDir(), Parallelism: 1,
			Filters: event.Filters{Tables: map[string]bool{"public." + readable: true}},
		})
		if err != nil {
			t.Fatalf("a snapshot of the one table the role can read: %v", err)
		}
		dropSlot()
	})

	t.Run("after the statement shown, the same role's snapshot works", func(t *testing.T) {
		if _, err := admin.Exec(ctx, `GRANT SELECT ON TABLE "public"."pgbaseline_ra3022_customers" TO "pgbaseline_ra3022_role";`); err != nil {
			t.Fatal(err)
		}
		outDir := t.TempDir()
		stats, err := pgbaseline.Run(ctx, pgbaseline.Config{
			QueryDSN: roleDSN, ReplDSN: replDSN(roleDSN), SlotName: slot, Publication: pub, OutputDir: outDir, Parallelism: 1,
		})
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
		if stats.TablesProcessed != 2 || stats.RowsWritten != 3 {
			t.Errorf("tables %d, rows %d, want 2 and 3", stats.TablesProcessed, stats.RowsWritten)
		}
	})

	// A catalog read that fails is not a refusal: the snapshot goes ahead.
	t.Run("a check that cannot run does not stop a snapshot", func(t *testing.T) {
		dropSlot()
		restore := pgbaseline.SetCheckReadAccess(func(context.Context, *pgx.Conn, string, event.Filters) error {
			return errors.New("the catalog did not answer")
		})
		defer restore()
		if err := run(t.TempDir()); err != nil {
			t.Fatalf("Run: %v", err)
		}
	})
}
