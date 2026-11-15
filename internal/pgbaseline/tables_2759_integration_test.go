//go:build integration

package pgbaseline

import (
	"context"
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/dbtrail/dbtrail/internal/cliutil"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// The tables a snapshot taken now would hold (#2759), against a real
// PostgreSQL: the publication's, narrowed by schema as a snapshot narrows
// them, and following the publication as tables are added to it.
func TestTablesOfNextRun_2759(t *testing.T) {
	dsn := testutil.SkipIfNoPostgres(t)
	ctx := context.Background()
	pg, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { pg.Close(context.Background()) })
	const pub, schema = "bintrail_tnr_it_pub", "tnr_it_other"
	drop := func() {
		bg := context.Background()
		_, _ = pg.Exec(bg, "DROP PUBLICATION IF EXISTS "+pub)
		_, _ = pg.Exec(bg, "DROP TABLE IF EXISTS tnr_it_a, tnr_it_b, tnr_it_unpublished")
		_, _ = pg.Exec(bg, "DROP SCHEMA IF EXISTS "+schema+" CASCADE")
	}
	drop()
	t.Cleanup(drop)
	for _, q := range []string{
		"CREATE TABLE tnr_it_a (id int PRIMARY KEY)",
		"CREATE TABLE tnr_it_b (id int PRIMARY KEY)",
		"CREATE TABLE tnr_it_unpublished (id int PRIMARY KEY)",
		"CREATE SCHEMA " + schema,
		"CREATE TABLE " + schema + ".c (id int PRIMARY KEY)",
		"CREATE PUBLICATION " + pub + " FOR TABLE tnr_it_a",
	} {
		if _, err := pg.Exec(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	list := func(schemas string) []string {
		t.Helper()
		got, err := TablesOfNextRun(ctx, dsn, pub, cliutil.BuildIndexFilters(schemas, ""))
		if err != nil {
			t.Fatalf("TablesOfNextRun: %v", err)
		}
		sort.Strings(got)
		return got
	}
	if got := list(""); !slices.Equal(got, []string{"public.tnr_it_a"}) {
		t.Fatalf("tables = %q, want the one published", got)
	}
	// Tables added to the publication after that.
	if _, err := pg.Exec(ctx, "ALTER PUBLICATION "+pub+" ADD TABLE tnr_it_b, "+schema+".c"); err != nil {
		t.Fatal(err)
	}
	if got := list(""); !slices.Equal(got, []string{"public.tnr_it_a", "public.tnr_it_b", schema + ".c"}) {
		t.Fatalf("tables = %q, want the three published", got)
	}
	if got := list("public"); !slices.Equal(got, []string{"public.tnr_it_a", "public.tnr_it_b"}) {
		t.Fatalf("tables in public = %q", got)
	}
	if _, err := TablesOfNextRun(ctx, dsn, "bintrail_tnr_it_no_such_pub", cliutil.BuildIndexFilters("", "")); err == nil || !strings.Contains(err.Error(), "does not exist") {
		t.Fatalf("a publication that does not exist: %v", err)
	}
}
