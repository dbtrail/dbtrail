//go:build integration

package consoleapp

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/dbtrail/dbtrail/internal/pgshim"
)

// A statement that runs out of memory on the copy in the copy's join order
// is run again in the order written (sqlsandbox oomretry.go), through the
// real port, router and copy workers held to 16 MB each.

// pgOOMWithoutWrittenOrder runs out of 16 MB in the copy's own join order and
// reads nothing in the order written: its work depends on the setting the
// order written is made with.
const pgOOMWithoutWrittenOrder = "SELECT count(*) AS n FROM (SELECT DISTINCT i FROM range(30000000) t(i) " +
	"WHERE current_setting('disabled_optimizers') NOT LIKE '%join_order%') s"

func TestIntegrationPGRouterOOMRetryInWrittenOrder(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	t.Run("the copy answers it in the order written", func(t *testing.T) {
		// A server that hides a table: the copy answers every statement
		// alone, with no screen in the way of range() and current_setting.
		env := startPGPortFor(t, ctx, nil, pgPortFixture{table: "pgoom_small", slot: "pgoom_small_slot", pub: "pgoom_small_pub",
			setup:    []string{"CREATE TABLE pgoom_small (id int PRIMARY KEY)", "INSERT INTO pgoom_small VALUES (1)"},
			copyOnly: true, sqlMemory: "16MiB"})
		c := pgClient(t, ctx, env.portDSN(), nil)
		if got := one(ctx, c, pgOOMWithoutWrittenOrder); got != "0" {
			t.Fatalf("simple protocol: %q, want the copy's 0", got)
		}
		conn := pgxConn(t, ctx, env.portDSN(), pgx.QueryExecModeCacheStatement)
		if got := pgxAnswer(ctx, conn, pgOOMWithoutWrittenOrder); got != `[n] ["0"]` {
			t.Errorf("extended protocol: %s", got)
		}
		// Out of memory in both orders, nowhere else to send it: the first
		// run's error, which says it was run again.
		got := one(ctx, c, "SELECT count(*) AS n FROM (SELECT DISTINCT i FROM range(30000000) t(i)) s")
		if !strings.HasPrefix(got, "error XX000 Out of Memory Error") || !strings.Contains(got, "order written") {
			t.Errorf("both orders: %q", got)
		}
		n := env.routes.doc("").Servers[env.serverID]
		if n.JoinChecks[pgOOMRetryOK] < 2 || n.JoinChecks[pgOOMRetryFailed] != 1 {
			t.Errorf("join_checks %v, want %s at least 2 (a Describe may run it too) and %s 1", n.JoinChecks, pgOOMRetryOK, pgOOMRetryFailed)
		}
	})

	t.Run("out of memory in both orders goes to the source under its own reason", func(t *testing.T) {
		// 3 million rows, whose DISTINCT does not fit in 16 MB in either
		// order; the screen sends the statement to the copy.
		env := startPGPortFor(t, ctx, nil, pgPortFixture{table: "pgoom_big", slot: "pgoom_big_slot", pub: "pgoom_big_pub",
			setup:     []string{"CREATE TABLE pgoom_big (id int PRIMARY KEY)", "INSERT INTO pgoom_big SELECT g FROM generate_series(1, 3000000) g"},
			sqlMemory: "16MiB"})
		c := pgClient(t, ctx, env.portDSN(), nil)
		stmt := "SELECT count(*) AS n FROM (SELECT DISTINCT id FROM pgoom_big) d"
		if got := one(ctx, c, stmt); got != "3000000" {
			t.Fatalf("%q, want the source's 3000000", got)
		}
		n := env.routes.doc("").Servers[env.serverID]
		if n.Reasons[pgshim.ReasonCopyRefusedAfterWrittenOrder] != 1 || n.Reasons[pgshim.ReasonCopyRefused] != 0 {
			t.Errorf("reasons %v", n.Reasons)
		}
		if n.JoinChecks[pgOOMRetryFailed] != 1 {
			t.Errorf("join_checks %v", n.JoinChecks)
		}
	})
}
