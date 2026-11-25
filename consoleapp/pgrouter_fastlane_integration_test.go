//go:build integration

package consoleapp

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/pgshim"
)

// The copy's fast lane, against a real PostgreSQL and the router's real
// pool: --workers 1, and the one more the router keeps for short
// statements. With a long report holding the one worker long statements
// have, a sum of one table is answered by the copy at once,
// on both protocols, while a statement that joins waits its turn and is
// refused as busy past --max-wait (53000, not sent to the source). Given
// back, the joined one is answered again.
func TestIntegrationPGRouterFastLane(t *testing.T) {
	const (
		table = "fastlane_t"
		role  = "fastlane_fwd"
		pass  = "fwd-pw-fastlane"
		rows  = 20000
	)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	cleanup := []string{
		"DROP TABLE IF EXISTS " + table,
		`DO $$ BEGIN IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '` + role + `') THEN EXECUTE 'DROP OWNED BY ` + role + `'; END IF; END $$`,
		"DROP ROLE IF EXISTS " + role,
	}
	setup := append(append([]string{}, cleanup...),
		"CREATE ROLE "+role+" LOGIN PASSWORD '"+pass+"'",
		"CREATE TABLE "+table+" (id int PRIMARY KEY, amount int NOT NULL)",
		"INSERT INTO "+table+" SELECT g, 1 FROM generate_series(1, "+strconv.Itoa(rows)+") g",
		"ANALYZE "+table,
		"GRANT SELECT ON "+table+" TO "+role,
	)
	const maxWait = 3 * time.Second
	env := startPGPortFor(t, ctx, nil, pgPortFixture{table: table, slot: "fastlane_slot", pub: "fastlane_pub",
		setup: setup,
		entry: func(e *console.ServerEntry) {
			e.SourceDSN, e.SourceSlot, e.SourcePublication = pgFwdSourceDSN(t), "fastlane_slot", "fastlane_pub"
			e.RouteDSN = withAccount(t, e.SourceDSN, role, pass)
		},
		cost:    &pgshim.CostPolicy{ScanRows: 100},
		workers: 1,
		maxWait: maxWait,
	})
	source := pgClient(t, ctx, env.sourceDSN, nil)
	t.Cleanup(func() {
		for _, s := range cleanup {
			_, _ = source.Exec(context.Background(), s).ReadAll()
		}
	})

	short := "SELECT sum(amount) FROM " + table
	// Not on the key: PostgreSQL 18 removes a join of a table to itself on
	// its primary key, and its plan then reads one table (short). Every
	// amount is 1, which matches one id: the count is the rows.
	joined := "SELECT count(*) FROM " + table + " a JOIN " + table + " b ON a.amount = b.id"
	holder := "SELECT count(*) FROM " + table + " a, " + table + " b, " + table + " c WHERE a.amount + b.amount + c.amount > 0"
	// The plans this test stands on, asked of the source first.
	planOf := func(stmt string) string {
		t.Helper()
		return one(ctx, source, "EXPLAIN (FORMAT JSON) "+stmt)
	}
	if p := planOf(short); strings.Contains(p, "Join") || strings.Contains(p, "Nested Loop") || !strings.Contains(p, `"Seq Scan"`) {
		t.Fatalf("the source plans the sum with more than one table: %s", p)
	}
	for _, stmt := range []string{joined, holder} {
		if p := planOf(stmt); !strings.Contains(p, "Join") && !strings.Contains(p, "Nested Loop") {
			t.Fatalf("the source plans %q without a join: %s", stmt, p)
		}
	}

	want := strconv.Itoa(rows)
	// Connections with the copy's list of columns read (kept 10 seconds),
	// so what waits below is the statement's own run.
	p1 := pgClient(t, ctx, env.portDSN(), nil)
	p2 := pgClient(t, ctx, env.portDSN(), nil)
	ext := pgxConn(t, ctx, env.portDSN(), pgx.QueryExecModeCacheStatement)
	h := pgClient(t, ctx, env.portDSN(), nil)
	for _, c := range []struct {
		name string
		got  func() string
		want string
	}{
		{"the sum", func() string { return one(ctx, p1, short) }, want},
		{"the join", func() string { return one(ctx, p2, joined) }, want},
		{"the holder's connection", func() string { return one(ctx, h, short) }, want},
	} {
		if got := c.got(); got != c.want {
			t.Fatalf("%s, the copy free: %q, want %q", c.name, got, c.want)
		}
	}
	if got := pgxAnswer(ctx, ext, short+" WHERE amount > $1", 0); got != `[sum] ["`+want+`"]` {
		t.Fatalf("prepared, the copy free: %s", got)
	}
	numbers := func() pgRouterServerNumbers { return env.routes.doc("").Servers[env.serverID] }
	onCopy := numbers().Reasons[pgshim.ReasonAnswersTheSame]
	if onCopy < 4 {
		t.Fatalf("reasons %v, want the copy to have answered them", numbers().Reasons)
	}

	// The report holds the one worker long statements may have.
	hctx, stopHolder := context.WithCancel(ctx)
	defer stopHolder()
	held := make(chan string, 1)
	go func() { held <- one(hctx, h, holder) }()
	waitUntil(t, "the report to hold its place", func() bool {
		d := env.routes.doc("").CopyLanes
		return d != nil && d.LongAdmitted == 1 && env.runner.Running() >= 1
	})
	if d := env.routes.doc("").CopyLanes; d.KeptForShort != 1 || d.LongLimit != 1 || env.runner.MaxInFlight() != 2 {
		t.Errorf("copy_lanes %+v and %d workers, want --workers 1 for long statements and one more kept", d, env.runner.MaxInFlight())
	}

	// The sum, by both protocols, is answered by the copy, well within the
	// wait a long statement is given.
	for _, c := range []struct {
		name string
		run  func() string
		want string
	}{
		{"simple protocol", func() string { return one(ctx, p1, short) }, want},
		{"pgx, prepared", func() string { return pgxAnswer(ctx, ext, short+" WHERE amount > $1", 0) }, `[sum] ["` + want + `"]`},
	} {
		start := time.Now()
		got := c.run()
		took := time.Since(start)
		if got != c.want {
			t.Errorf("the sum, %s, with the report running: %q, want the copy's %q", c.name, got, c.want)
		}
		if took >= maxWait {
			t.Errorf("the sum, %s, took %v: it waited behind the report", c.name, took)
		}
		t.Logf("the sum, %s, with the report running: %v", c.name, took)
	}
	if n := numbers().Reasons[pgshim.ReasonAnswersTheSame]; n != onCopy+2 {
		t.Errorf("answered by the copy %d times more, want 2 (%v)", n-onCopy, numbers().Reasons)
	}

	// The join waits for the report's place and is refused as busy, not
	// sent to the source.
	start := time.Now()
	got := one(ctx, p2, joined)
	if !strings.HasPrefix(got, "error 53000 the copy is busy (copy_wait_timeout)") {
		t.Errorf("the join with the report running: %q, want 53000 for a busy copy", got)
	}
	if took := time.Since(start); took < maxWait {
		t.Errorf("the join was refused after %v, before its wait ran out", took)
	}
	if n := numbers().Reasons[pgshim.ReasonCopyWaitTimeout]; n != 1 {
		t.Errorf("copy_wait_timeout counted %d times, want 1 (%v)", n, numbers().Reasons)
	}
	if d := env.routes.doc("").CopyLanes; d.LongWaiting != 0 || d.RefusedWaitTimeout != 1 {
		t.Errorf("after the refusal: %+v, want none waiting and one refused", d)
	}

	// The report stopped: its place is given back, and the join answered.
	if err := h.CancelRequest(ctx); err != nil {
		t.Errorf("cancel the report: %v", err)
	}
	if got := <-held; !strings.HasPrefix(got, "error 57014") {
		t.Errorf("the report, cancelled: %q", got)
	}
	waitUntil(t, "the report's place to be given back", func() bool { return env.routes.doc("").CopyLanes.LongAdmitted == 0 })
	if got := one(ctx, p2, joined); got != want {
		t.Errorf("the join once the report ended: %q, want the copy's %s", got, want)
	}
}
