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

// pgOOMShape is pgOOMWithoutWrittenOrder's shape with other values: rows
// rows, and the filter's pattern. A pattern that is not join_order holds in
// the order written too, so the statement runs out of memory in both.
func pgOOMShape(rows, pattern string) string {
	return "SELECT count(*) AS n FROM (SELECT DISTINCT i FROM range(" + rows + ") t(i) " +
		"WHERE current_setting('disabled_optimizers') NOT LIKE '" + pattern + "') s"
}

func TestIntegrationPGRouterOOMRetryInWrittenOrder(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	t.Run("the copy answers it in the order written, and remembers its shape", func(t *testing.T) {
		// A server that hides a table: the copy answers every statement
		// alone, with no screen in the way of range() and current_setting.
		env := startPGPortFor(t, ctx, nil, pgPortFixture{table: "pgoom_small", slot: "pgoom_small_slot", pub: "pgoom_small_pub",
			setup:    []string{"CREATE TABLE pgoom_small (id int PRIMARY KEY)", "INSERT INTO pgoom_small VALUES (1)"},
			copyOnly: true, sqlMemory: "16MiB"})
		c := pgClient(t, ctx, env.portDSN(), nil)
		counts := func(what string, ok, remembered, forgotten uint64) {
			t.Helper()
			n := env.routes.doc("").Servers[env.serverID]
			if n.JoinChecks[pgOOMRetryPrefix+"ok"] != ok || n.JoinChecks[pgWrittenOrderRemembered] != remembered || n.JoinChecks[pgWrittenOrderForgotten] != forgotten {
				t.Errorf("%s: join_checks %v, want ok %d, remembered %d, forgotten %d", what, n.JoinChecks, ok, remembered, forgotten)
			}
		}
		// Out of memory in the copy's order, answered in the order written.
		if got := one(ctx, c, pgOOMWithoutWrittenOrder); got != "0" {
			t.Fatalf("simple protocol: %q, want the copy's 0", got)
		}
		counts("the first", 1, 0, 0)
		// The same shape with another value: in the order written from the
		// start, one run with no retry.
		if got := one(ctx, c, pgOOMShape("30000001", "%join_order%")); got != "0" {
			t.Fatalf("same shape, another value: %q, want the copy's 0", got)
		}
		counts("same shape, another value", 1, 1, 0)
		// And on the extended protocol, which shares the shape.
		conn := pgxConn(t, ctx, env.portDSN(), pgx.QueryExecModeCacheStatement)
		if got := pgxAnswer(ctx, conn, pgOOMWithoutWrittenOrder); got != `[n] ["0"]` {
			t.Errorf("extended protocol: %s", got)
		}
		counts("extended protocol", 1, 2, 0)
		// The same shape whose filter now holds in the order written too: out
		// of memory in that order as well, so it is forgotten, with no third
		// run. Nowhere else to send it here: the client gets the error.
		got := one(ctx, c, pgOOMShape("30000000", "%no such optimizer%"))
		if !strings.HasPrefix(got, "error XX000 Out of Memory Error") || !strings.Contains(got, "needed before") {
			t.Errorf("remembered, out of memory in the order written: %q", got)
		}
		counts("forgotten", 1, 3, 1)
		// Forgotten: the copy's own order first again, then the retry.
		if got := one(ctx, c, pgOOMWithoutWrittenOrder); got != "0" {
			t.Fatalf("after it was forgotten: %q, want the copy's 0", got)
		}
		counts("learned again", 2, 3, 1)
		// Out of memory in both orders, nowhere else to send it: the first
		// run's error, which says it was run again.
		got = one(ctx, c, "SELECT count(*) AS n FROM (SELECT DISTINCT i FROM range(30000000) t(i)) s")
		if !strings.HasPrefix(got, "error XX000 Out of Memory Error") || !strings.Contains(got, "order written") {
			t.Errorf("both orders: %q", got)
		}
		n := env.routes.doc("").Servers[env.serverID]
		if n.JoinChecks[pgOOMRetryPrefix+"failed"] != 1 {
			t.Errorf("join_checks %v, want failed 1", n.JoinChecks)
		}
	})

	t.Run("a remembered shape is tried in the copy's order again past its time", func(t *testing.T) {
		const remember = 5 * time.Second
		env := startPGPortFor(t, ctx, nil, pgPortFixture{table: "pgoom_exp", slot: "pgoom_exp_slot", pub: "pgoom_exp_pub",
			setup:    []string{"CREATE TABLE pgoom_exp (id int PRIMARY KEY)", "INSERT INTO pgoom_exp VALUES (1)"},
			copyOnly: true, sqlMemory: "16MiB", writtenOrderRemember: remember})
		c := pgClient(t, ctx, env.portDSN(), nil)
		for i, want := range []struct{ ok, remembered uint64 }{{1, 0}, {1, 1}, {2, 1}} {
			if i == 2 {
				time.Sleep(remember)
			}
			if got := one(ctx, c, pgOOMWithoutWrittenOrder); got != "0" {
				t.Fatalf("run %d: %q, want the copy's 0", i+1, got)
			}
			n := env.routes.doc("").Servers[env.serverID]
			if n.JoinChecks[pgOOMRetryPrefix+"ok"] != want.ok || n.JoinChecks[pgWrittenOrderRemembered] != want.remembered {
				t.Errorf("run %d: join_checks %v, want ok %d, remembered %d", i+1, n.JoinChecks, want.ok, want.remembered)
			}
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
		if n.JoinChecks[pgOOMRetryPrefix+"failed"] != 1 {
			t.Errorf("join_checks %v", n.JoinChecks)
		}
	})
}
