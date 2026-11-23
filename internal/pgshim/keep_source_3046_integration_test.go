//go:build integration

package pgshim

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/dbtrail/dbtrail/internal/testutil"
)

// #3046, on a real PostgreSQL: a session advisory lock a client's function
// took lives on the kept connection only until the limit, with the client
// silent.
func TestIntegrationPGRouterKeptConnectionEndsAtTheLimit_3046(t *testing.T) {
	dsn := testutil.SkipIfNoPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	shortenKeepIdle(t, 700*time.Millisecond)
	direct, err := pgconn.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer direct.Close(context.Background())
	const fn = "pgshim_keep_lock_3046"
	exec := func(q string) string {
		t.Helper()
		res, err := direct.Exec(ctx, q).ReadAll()
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		if len(res) > 0 && len(res[0].Rows) > 0 {
			return string(res[0].Rows[0][0])
		}
		return ""
	}
	exec("CREATE OR REPLACE FUNCTION " + fn + "() RETURNS text LANGUAGE plpgsql AS $f$ BEGIN PERFORM pg_advisory_lock(30461); RETURN 'locked'; END $f$")
	t.Cleanup(func() { _, _ = direct.Exec(context.Background(), "DROP FUNCTION IF EXISTS "+fn+"()").ReadAll() })
	held := func() string {
		return exec("SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND objid = 30461 AND granted")
	}
	l := &costLogin{src: &fakeSource{realDSN: dsn}, keep: &keptSources{}}
	c := costPortConn(t, ctx, l)
	if got := costSimple(ctx, c, l, "SELECT "+fn+"()"); got.answer != "locked" || got.side != RouteSource {
		t.Fatalf("the function: %+v", got)
	}
	if n := held(); n != "1" {
		t.Fatalf("right after the statement %s locks held, want the kept session's", n)
	}
	waitUntilTest(t, "the lock released at the limit, the client silent", func() bool { return held() == "0" })
}
