//go:build integration

package reconstruct_test

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/internal/cli"
	"github.com/dbtrail/dbtrail/internal/pgdrill"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// drill rehearses a PostgreSQL table's restore into a scratch PostgreSQL end
// to end: the target picks the psql output, the loader runs it, and what
// landed is the table as of --at, row for row.
func TestDrill_PostgresEndToEnd(t *testing.T) {
	base := testutil.SkipIfNoPostgres(t)
	f := newPGFixture(t)
	snap := f.at(10 * time.Minute)
	f.event(0x600, 9*time.Minute+30*time.Second, 2, "1", s(`tab	here\.x`))
	f.event(0x700, 15*time.Minute, 1, "3", s("newbie"))
	f.event(0x700, 15*time.Minute, 3, "2", nil)
	f.checkpoint(0x700)
	root := t.TempDir()
	f.baseline(root, snap, 0x500, "", [2]string{"1", "paid"}, [2]string{"2", "open"}, [2]string{"5", "keep"})

	ctx := context.Background()
	admin, err := pgx.Connect(ctx, base)
	if err != nil {
		t.Fatal(err)
	}
	name := fmt.Sprintf("drill_e2e_%d", time.Now().UnixNano())
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = admin.Exec(context.Background(), "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
		admin.Close(context.Background())
	})
	u, _ := url.Parse(base)
	u.Path = "/" + name
	target := u.String()

	cli.SetPostgresDrillTarget(func(ctx context.Context, dsn string) (cli.DrillTarget, error) {
		return pgdrill.Open(ctx, dsn)
	})
	t.Cleanup(func() { cli.SetPostgresDrillTarget(nil) })
	run := func() error {
		root2 := &cobra.Command{Use: "bintrail-pg", SilenceErrors: true}
		cli.AddReadCommands(root2)
		root2.SetArgs([]string{"drill", "--index-dsn", f.dsn, "--baseline-dir", root, "--tables", "public.orders",
			"--at", f.at(30 * time.Minute).Format(time.RFC3339), "--target-dsn", target, "--format", "json"})
		return root2.ExecuteContext(ctx)
	}
	if err := run(); err != nil {
		t.Fatalf("drill: %v", err)
	}
	conn, err := pgx.Connect(ctx, target)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	var got string
	if err := conn.QueryRow(ctx, `SELECT string_agg(id || '=' || coalesce(status, 'NULL'), ',' ORDER BY id) FROM public.orders`).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if want := "1=tab\there\\.x,3=newbie,5=keep"; got != want {
		t.Errorf("loaded %q, want %q", got, want)
	}
	// The loaded target is no longer a scratch: a second drill is refused.
	if err := run(); err == nil || !strings.Contains(err.Error(), "non-empty target") {
		t.Errorf("second drill: %v", err)
	}
}
