//go:build integration

package pgshim

import (
	"context"
	"maps"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/dbtrail/dbtrail/internal/testutil"
)

// The statistics query on a real PostgreSQL, and through the port: n_distinct
// in both of its forms (a count, and minus a fraction of the rows), a column
// whose name two tables share left out (the copy's plan names columns
// without their table), and the counts handed to the copy with the statement.
func TestIntegrationJoinStatistics(t *testing.T) {
	dsn := testutil.SkipIfNoPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	direct, err := pgconn.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer direct.Close(context.Background())
	exec := func(q string) [][][]byte {
		t.Helper()
		res, err := direct.Exec(ctx, q).ReadAll()
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		if len(res) == 0 {
			return nil
		}
		return res[len(res)-1].Rows
	}
	exec("CREATE SCHEMA IF NOT EXISTS jg_stats")
	t.Cleanup(func() { _, _ = direct.Exec(context.Background(), "DROP SCHEMA IF EXISTS jg_stats CASCADE").ReadAll() })
	exec("CREATE TABLE jg_stats.jg_orders (jg_o_id int, jg_o_w_id int, jg_shared int)")
	exec("CREATE TABLE jg_stats.jg_stock (jg_s_w_id int, jg_shared int)")
	exec("INSERT INTO jg_stats.jg_orders SELECT g, g % 20, g FROM generate_series(1, 5000) g")
	exec("INSERT INTO jg_stats.jg_stock SELECT g % 20, g FROM generate_series(1, 2000) g")
	exec("ANALYZE jg_stats.jg_orders, jg_stats.jg_stock")

	rows := exec(joinStatsSQL)
	if len(rows) != 1 || len(rows[0]) != 1 {
		t.Fatalf("the statistics query answered %v", rows)
	}
	ndv, err := parseJoinStats(rows[0][0])
	if err != nil {
		t.Fatalf("%s: %v", rows[0][0], err)
	}
	// A unique column: n_distinct -1, so its count is the table's rows.
	if got := ndv["jg_o_id"]; got < 4900 || got > 5100 {
		t.Errorf("jg_o_id: %v", got)
	}
	for _, col := range []string{"jg_o_w_id", "jg_s_w_id"} {
		if got := ndv[col]; got != 20 {
			t.Errorf("%s: %v, want 20", col, got)
		}
	}
	if got, ok := ndv["jg_shared"]; ok {
		t.Errorf("a name two tables share: %v", got)
	}

	// Through the port, forwarding to that PostgreSQL: a statement its plan
	// sends to the copy carries the counts.
	l := &costLogin{src: &fakeSource{realDSN: dsn}, cost: &CostPolicy{CostThreshold: 0.01}, joinStats: &JoinStats{}}
	c := costPortConn(t, ctx, l)
	if got := costSimple(ctx, c, l, "SELECT count(*) FROM jg_stats.jg_orders o JOIN jg_stats.jg_stock s ON o.jg_o_w_id = s.jg_s_w_id"); got.side != RouteCopy || got.answer != "7" {
		t.Fatalf("the join: %+v", got)
	}
	sent := l.joinNDVs()
	if len(sent) != 1 || !maps.Equal(sent[0], ndv) {
		t.Errorf("the copy was given %v, want %v", sent, ndv)
	}
}
