//go:build integration

package consoleapp

import (
	"context"
	"testing"
	"time"
)

// The main ORDER BY of text is the copy's (#3219), and COLLATE "default" is
// how a client asks for PostgreSQL's order. The copy of pgroute_t holds three
// rows and the source four, so the count of rows says who answered.
func TestIntegrationPGRouterMainOrderOfText(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	env, source := pgRouteEnv(t, ctx)
	count := func(stmt string) int {
		c := pgClient(t, ctx, env.portDSN(), nil)
		res, err := c.Exec(ctx, stmt).ReadAll()
		if err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
		return len(res[len(res)-1].Rows)
	}
	if n := count("SELECT name FROM " + pgRouteTable + " ORDER BY name"); n != 3 {
		t.Errorf("the main ORDER BY of text: %d rows, want the copy's 3", n)
	}
	if n := count(`SELECT name FROM ` + pgRouteTable + ` ORDER BY name COLLATE "default"`); n != 4 {
		t.Errorf(`ORDER BY name COLLATE "default": %d rows, want the source's 4`, n)
	}
	if got := one(ctx, source, `SELECT count(*) FROM `+pgRouteTable); got != "4" {
		t.Fatalf("the source holds %s rows", got)
	}
}
