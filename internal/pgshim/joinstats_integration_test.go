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
// in both of its forms (a count, and minus a fraction of the rows), by table,
// and the counts of the statement's own tables handed to the copy with it, a
// column whose name both hold left out.
func TestIntegrationJoinStatistics(t *testing.T) {
	dsn := testutil.SkipIfNoPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	direct, err := pgconn.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	// Closed after the schema is dropped: cleanups run last first.
	t.Cleanup(func() { direct.Close(context.Background()) })
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
	exec("DROP SCHEMA IF EXISTS jg_stats CASCADE")
	exec("CREATE SCHEMA jg_stats")
	t.Cleanup(func() { _, _ = direct.Exec(context.Background(), "DROP SCHEMA IF EXISTS jg_stats CASCADE").ReadAll() })
	exec("CREATE TABLE jg_stats.jg_orders (jg_o_id int, jg_o_w_id int, jg_shared int)")
	exec("CREATE TABLE jg_stats.jg_stock (jg_s_w_id int, jg_shared int)")
	exec("INSERT INTO jg_stats.jg_orders SELECT g, g % 20, g FROM generate_series(1, 5000) g")
	exec("INSERT INTO jg_stats.jg_stock SELECT g % 20, g FROM generate_series(1, 2000) g")
	exec("ANALYZE jg_stats.jg_orders, jg_stats.jg_stock")

	rows := exec(JoinStatsSQL)
	if len(rows) != 1 || len(rows[0]) != 1 {
		t.Fatalf("the statistics query answered %v", rows)
	}
	tables, err := ParseJoinStats(rows[0][0])
	if err != nil {
		t.Fatalf("%s: %v", rows[0][0], err)
	}
	orders, stock := tables["jg_stats.jg_orders"], tables["jg_stats.jg_stock"]
	// A unique column: n_distinct -1, so its count is the table's rows.
	if got := orders["jg_o_id"]; got < 4900 || got > 5100 {
		t.Errorf("jg_o_id: %v", got)
	}
	if orders["jg_o_w_id"] != 20 || stock["jg_s_w_id"] != 20 || stock["jg_shared"] < 1900 {
		t.Errorf("orders %v, stock %v", orders, stock)
	}

	// Through the port, forwarding to that PostgreSQL, the counts read as
	// the host reads them.
	read := func(ctx context.Context) (JoinTables, error) {
		c, err := pgconn.Connect(ctx, dsn)
		if err != nil {
			return nil, err
		}
		defer c.Close(context.Background())
		res, err := c.Exec(ctx, JoinStatsSQL).ReadAll()
		if err != nil {
			return nil, err
		}
		return ParseJoinStats(res[0].Rows[0][0])
	}
	l := &costLogin{src: &fakeSource{realDSN: dsn}, cost: &CostPolicy{CostThreshold: 0.01}, joinStats: &JoinStats{Read: read, spawn: func(f func()) { f() }}}
	l.joinStats.current()
	c := costPortConn(t, ctx, l)
	if got := costSimple(ctx, c, l, "SELECT count(*) FROM jg_stats.jg_orders o JOIN jg_stats.jg_stock s ON o.jg_o_w_id = s.jg_s_w_id"); got.side != RouteCopy || got.answer != "7" {
		t.Fatalf("the join: %+v", got)
	}
	runs := l.joinRuns()
	want := map[string]float64{"jg_o_id": orders["jg_o_id"], "jg_o_w_id": 20, "jg_s_w_id": 20}
	if len(runs) != 1 || !maps.Equal(runs[0].ndv, want) {
		t.Errorf("the copy was given %v, want %v", runs, want)
	}
}
