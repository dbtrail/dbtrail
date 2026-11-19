//go:build integration

package consoleapp

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/pgshim"
)

// Routing by the source's plan on the PostgreSQL port, against a real
// PostgreSQL: of the statements the copy answers as PostgreSQL does, a read
// by the primary key goes to the source, whose index answers it, and a
// sequential scan over the threshold goes to the copy.

const (
	pgCostTable = "pgcost_t"
	pgCostRole  = "dbtrail_fwd_cost"
	pgCostPass  = "fwd-pw-cost"
	pgCostRows  = 5000
)

func TestIntegrationPGRouterRoutesByPlan(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	// The scan rule alone, at a threshold this table crosses: a sequential
	// scan of 1,000 rows or more goes to the copy. The table's rows are on
	// the copy; a row added on the source afterwards says which side
	// answered.
	pol := pgshim.CostPolicy{ScanRows: 1000}
	env := startPGPortFor(t, ctx, nil, pgPortFixture{table: pgCostTable, slot: "pgcost_slot", pub: "pgcost_pub",
		setup: []string{
			"CREATE TABLE " + pgCostTable + " (id int PRIMARY KEY, amount int NOT NULL)",
			"INSERT INTO " + pgCostTable + " SELECT g, 1 FROM generate_series(1, " + strconv.Itoa(pgCostRows) + ") g",
		},
		entry: func(e *console.ServerEntry) {
			e.SourceDSN, e.SourceSlot, e.SourcePublication = pgFwdSourceDSN(t), "pgcost_slot", "pgcost_pub"
			e.RouteDSN = withAccount(t, e.SourceDSN, pgCostRole, pgCostPass)
		},
		cost: &pol,
	})
	source := pgClient(t, ctx, env.sourceDSN, nil)
	exec1 := func(stmt string) {
		t.Helper()
		if _, err := source.Exec(ctx, stmt).ReadAll(); err != nil {
			t.Fatalf("source: %s: %v", stmt, err)
		}
	}
	exec1("INSERT INTO " + pgCostTable + " VALUES (" + strconv.Itoa(pgCostRows+1) + ", 1000)")
	exec1("ANALYZE " + pgCostTable)
	exec1(`DO $$ BEGIN IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '` + pgCostRole + `') THEN EXECUTE 'DROP OWNED BY ` + pgCostRole + `'; END IF; END $$`)
	exec1("DROP ROLE IF EXISTS " + pgCostRole)
	exec1("CREATE ROLE " + pgCostRole + " LOGIN PASSWORD '" + pgCostPass + "'")
	exec1("GRANT SELECT ON " + pgCostTable + " TO " + pgCostRole)
	t.Cleanup(func() {
		bg := context.Background()
		for _, s := range []string{"DROP OWNED BY " + pgCostRole, "DROP ROLE IF EXISTS " + pgCostRole} {
			_, _ = source.Exec(bg, s).ReadAll()
		}
	})
	version, _ := strconv.Atoi(one(ctx, source, "SHOW server_version_num"))

	lookup := "SELECT amount FROM " + pgCostTable + " WHERE id = " + strconv.Itoa(pgCostRows+1)
	scan := "SELECT sum(amount) FROM " + pgCostTable
	// The plans this test stands on, asked of the source first, so a
	// planner that plans otherwise fails here and says so.
	shape := func(stmt string) (top string, seqRows float64) {
		t.Helper()
		var doc []struct {
			Plan map[string]any `json:"Plan"`
		}
		if err := json.Unmarshal([]byte(one(ctx, source, "EXPLAIN (FORMAT JSON) "+stmt)), &doc); err != nil || len(doc) != 1 {
			t.Fatalf("EXPLAIN %s: %v", stmt, err)
		}
		var walk func(n map[string]any)
		walk = func(n map[string]any) {
			if n["Node Type"] == "Seq Scan" {
				seqRows = max(seqRows, n["Plan Rows"].(float64))
			}
			if c, ok := n["Plans"].([]any); ok {
				for _, x := range c {
					walk(x.(map[string]any))
				}
			}
		}
		walk(doc[0].Plan)
		top, _ = doc[0].Plan["Node Type"].(string)
		return top, seqRows
	}
	if top, rows := shape(lookup); top != "Index Scan" || rows != 0 {
		t.Fatalf("the source plans the read by key as %s with a sequential scan of %v rows, not an index scan", top, rows)
	}
	if _, rows := shape(scan); rows < float64(pol.ScanRows) {
		t.Fatalf("the source plans the sum with a sequential scan of %v rows, under the test's threshold of %d", rows, pol.ScanRows)
	}

	reasons := func() map[string]uint64 { return env.routes.doc("").Servers[env.serverID].Reasons }
	c := pgClient(t, ctx, env.portDSN(), nil)
	if got := one(ctx, c, lookup); got != "1000" {
		t.Errorf("the read by key: %q, want the source's 1000 (the copy does not hold the row)", got)
	}
	if got := one(ctx, c, scan); got != strconv.Itoa(pgCostRows) {
		t.Errorf("the sum: %q, want the copy's %d", got, pgCostRows)
	}
	if r := reasons(); r[pgshim.ReasonCheapPlan] != 1 || r[pgshim.ReasonAnswersTheSame] != 1 {
		t.Errorf("reasons %v, want one cheap_plan and one answers_the_same", r)
	}
	// A statement the source refuses to plan: it goes there, and the
	// client gets PostgreSQL's own error.
	if got := one(ctx, c, "SELECT sum(amount) FROM pgcost_missing"); !strings.HasPrefix(got, "error 42P01") {
		t.Errorf("a table neither side has: %q", got)
	}
	if r := reasons(); r[pgshim.ReasonExplainFailed] == 0 {
		t.Errorf("reasons %v, want an explain_failed", r)
	}

	// Prepared, in pgx's modes: asked once per statement, before its
	// arguments (GENERIC_PLAN, PostgreSQL 16 and later; before it the
	// source answers a statement with parameters).
	for _, mode := range []pgx.QueryExecMode{pgx.QueryExecModeCacheStatement, pgx.QueryExecModeDescribeExec, pgx.QueryExecModeExec} {
		conn := pgxConn(t, ctx, env.portDSN(), mode)
		byKey := pgxAnswer(ctx, conn, "SELECT amount FROM "+pgCostTable+" WHERE id = $1", pgCostRows+1)
		if byKey != `[amount] ["1000"]` {
			t.Errorf("mode %v, the read by key: %s, want the source's row", mode, byKey)
		}
		want := `[sum] ["` + strconv.Itoa(pgCostRows) + `"]`
		if version < 160000 {
			want = `[sum] ["` + strconv.Itoa(pgCostRows+1000) + `"]`
		}
		for i := range 2 {
			if got := pgxAnswer(ctx, conn, "SELECT sum(amount) FROM "+pgCostTable+" WHERE amount < $1", 999999); got != want {
				t.Errorf("mode %v, run %d, the sum: %s, want %s", mode, i, got, want)
			}
		}
		conn.Close(ctx)
	}
	if c.TxStatus() != 'I' {
		t.Errorf("status %c", c.TxStatus())
	}
}
