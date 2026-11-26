//go:build integration

package consoleapp

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/dbtrail/dbtrail/internal/testutil"
)

// The port's own read of a source's column statistics, on a real
// PostgreSQL: per table, both forms of n_distinct.
func TestIntegrationPGReadJoinStats(t *testing.T) {
	dsn := testutil.SkipIfNoPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	direct, err := pgconn.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { direct.Close(context.Background()) })
	for _, q := range []string{
		"DROP SCHEMA IF EXISTS jg_read CASCADE", "CREATE SCHEMA jg_read",
		"CREATE TABLE jg_read.t (id int, w int)",
		"INSERT INTO jg_read.t SELECT g, g % 7 FROM generate_series(1, 3000) g",
		"ANALYZE jg_read.t",
	} {
		if _, err := direct.Exec(ctx, q).ReadAll(); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	t.Cleanup(func() { _, _ = direct.Exec(context.Background(), "DROP SCHEMA IF EXISTS jg_read CASCADE").ReadAll() })
	tables, err := pgReadJoinStats(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	got := tables["jg_read.t"]
	if got["w"] != 7 || got["id"] < 2900 || got["id"] > 3100 {
		t.Errorf("jg_read.t: %v", got)
	}
}
