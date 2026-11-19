//go:build integration

package consoleapp

import (
	"context"
	"testing"
	"time"
)

func TestIntegrationZZProbeCopyRefused(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	env, _ := pgRouteEnv(t, ctx)
	T := pgRouteTable
	for _, s := range []string{
		"SELECT min(at) FROM " + T,
		"SELECT count(*) FROM " + T + " WHERE id = ANY('{1,2,4}')",
		"SELECT count(*) FROM " + T + " WHERE id = '0x1'",
		"SELECT count(*) FROM " + T + " WHERE id = '1_0'",
		"SELECT count(*) FROM " + T + " WHERE id = ' 4 '",
		"SELECT count(*) FROM " + T + " LIMIT ALL",
		"SELECT count(*) FROM " + T + " OFFSET 0 ROWS",
		"SELECT greatest(id, '5') FROM " + T + " WHERE id = 1",
		"SELECT substring(name from 1 for 2) FROM " + T + " WHERE id = 1",
		"SELECT trim(both 'b' from name) FROM " + T + " WHERE id = 1",
		"SELECT position('r' in name) FROM " + T + " WHERE id = 1",
		"SELECT lag(id, -1) OVER (ORDER BY id) FROM " + T + " WHERE id = 1",
		"SELECT ntile(0) OVER (ORDER BY id) FROM " + T + " WHERE id = 1",
		"SELECT count(*) FROM " + T + " GROUP BY ()",
		"SELECT length(id) FROM " + T + " WHERE id = 1",
		"SELECT string_agg(name, 1) FROM " + T,
		"SELECT count(*) FROM " + T + " WHERE id = '4.0'",
		"SELECT count(*) FROM " + T + " WHERE free = 'NaN'",
		"SELECT count(*) FROM " + T + " WHERE amount = '7.750'",
		"SELECT abs(id - 2147483647 - 2) FROM " + T + " WHERE id = 1",
		"SELECT sum(id) FROM " + T + " WHERE id = 1 GROUP BY id, id",
	} {
		c := pgClient(t, ctx, env.portDSN(), nil)
		t.Logf("PROBE %s => %q", s, one(ctx, c, s))
		c.Close(ctx)
	}
	n := env.routes.doc("").Servers[env.serverID]
	t.Logf("PROBE reasons %v", n.Reasons)
	if n.Statements != nil {
		for _, r := range n.Statements.Rows {
			t.Logf("PROBE row %s | %s | %v", r.Statement, r.Side, r.Reasons)
		}
	}
}
