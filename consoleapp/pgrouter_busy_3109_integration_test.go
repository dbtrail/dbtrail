//go:build integration

package consoleapp

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/pgshim"
)

// #3109, against a real PostgreSQL and the router's real pool of copy
// workers: with the one worker held by a long statement, a heavy statement
// headed for the copy waits its turn (--max-wait), is refused with 53000,
// and nothing of it runs on the source; what the source answers anyway
// still goes there.
//
// What runs on the source is counted there: a row-level security policy on
// the table, for the forwarding user only, calls a function that takes a
// number of a sequence for every row it reads. EXPLAIN does not run it, the
// copy knows nothing of it, and a sequence is not rolled back.

const (
	pgBusyTable = "busy3109_t"
	pgBusyRole  = "busy3109_fwd"
	pgBusyPass  = "fwd-pw-busy3109"
	pgBusySeq   = "busy3109_hits"
	pgBusyRows  = 20000
)

func TestIntegrationPGRouterBusyCopyRefusesNotForwards(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	cleanup := []string{
		"DROP TABLE IF EXISTS " + pgBusyTable,
		"DROP FUNCTION IF EXISTS busy3109_hit()",
		"DROP SEQUENCE IF EXISTS " + pgBusySeq,
		`DO $$ BEGIN IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '` + pgBusyRole + `') THEN EXECUTE 'DROP OWNED BY ` + pgBusyRole + `'; END IF; END $$`,
		"DROP ROLE IF EXISTS " + pgBusyRole,
	}
	setup := append(append([]string{}, cleanup...),
		"CREATE ROLE "+pgBusyRole+" LOGIN PASSWORD '"+pgBusyPass+"'",
		"CREATE SEQUENCE "+pgBusySeq,
		"CREATE FUNCTION busy3109_hit() RETURNS boolean LANGUAGE sql VOLATILE SECURITY DEFINER AS $$ SELECT nextval('"+pgBusySeq+"') > 0 $$",
		"CREATE TABLE "+pgBusyTable+" (id int PRIMARY KEY, amount int NOT NULL)",
		"INSERT INTO "+pgBusyTable+" SELECT g, 1 FROM generate_series(1, "+strconv.Itoa(pgBusyRows)+") g",
		"ANALYZE "+pgBusyTable,
		"ALTER TABLE "+pgBusyTable+" ENABLE ROW LEVEL SECURITY",
		"CREATE POLICY busy3109_counted ON "+pgBusyTable+" FOR SELECT TO "+pgBusyRole+" USING (busy3109_hit())",
		"GRANT SELECT ON "+pgBusyTable+" TO "+pgBusyRole,
	)
	env := startPGPortFor(t, ctx, nil, pgPortFixture{table: pgBusyTable, slot: "busy3109_slot", pub: "busy3109_pub",
		setup: setup,
		entry: func(e *console.ServerEntry) {
			e.SourceDSN, e.SourceSlot, e.SourcePublication = pgFwdSourceDSN(t), "busy3109_slot", "busy3109_pub"
			e.RouteDSN = withAccount(t, e.SourceDSN, pgBusyRole, pgBusyPass)
		},
		cost:    &pgshim.CostPolicy{ScanRows: 100},
		workers: 1,
		maxWait: 2 * time.Second,
	})
	source := pgClient(t, ctx, env.sourceDSN, nil)
	t.Cleanup(func() {
		for _, s := range cleanup {
			_, _ = source.Exec(context.Background(), s).ReadAll()
		}
	})
	hits := func() int {
		t.Helper()
		n, err := strconv.Atoi(one(ctx, source, "SELECT CASE WHEN is_called THEN last_value ELSE 0 END FROM "+pgBusySeq))
		if err != nil {
			t.Fatalf("the source's count of rows read: %v", err)
		}
		return n
	}

	heavy := "SELECT sum(amount) FROM " + pgBusyTable
	cheap := "SELECT amount FROM " + pgBusyTable + " WHERE id = 7"
	holder := "SELECT count(*) FROM " + pgBusyTable + " a, " + pgBusyTable + " b, " + pgBusyTable + " c WHERE a.amount + b.amount + c.amount > 0"
	// The plans this test stands on, asked of the source as the forwarding
	// user (its policy is in them): a planner that plans otherwise fails
	// here and says so.
	fwd := pgClient(t, ctx, withAccount(t, env.sourceDSN, pgBusyRole, pgBusyPass), nil)
	seqRows := func(stmt string) float64 {
		t.Helper()
		var doc []struct {
			Plan map[string]any `json:"Plan"`
		}
		if err := json.Unmarshal([]byte(one(ctx, fwd, "EXPLAIN (FORMAT JSON) "+stmt)), &doc); err != nil || len(doc) != 1 {
			t.Fatalf("EXPLAIN %s: %v", stmt, err)
		}
		most := 0.0
		var walk func(n map[string]any)
		walk = func(n map[string]any) {
			if n["Node Type"] == "Seq Scan" {
				most = max(most, n["Plan Rows"].(float64))
			}
			if c, ok := n["Plans"].([]any); ok {
				for _, x := range c {
					walk(x.(map[string]any))
				}
			}
		}
		walk(doc[0].Plan)
		return most
	}
	if r := seqRows(heavy); r < 100 {
		t.Fatalf("the source plans the sum with a sequential scan of %v rows, under the test's threshold of 100", r)
	}
	if r := seqRows(holder); r < 100 {
		t.Fatalf("the source plans the long statement with a sequential scan of %v rows", r)
	}
	if r := seqRows(cheap); r != 0 {
		t.Fatalf("the source plans the read by key with a sequential scan of %v rows", r)
	}
	// The count works: the forwarding user's own read is counted.
	before := hits()
	if got := one(ctx, fwd, cheap); got != "1" {
		t.Fatalf("the read by key as the forwarding user: %q", got)
	}
	if hits() == before {
		t.Fatal("a read on the source as the forwarding user was not counted")
	}

	numbers := func() pgRouterServerNumbers { return env.routes.doc("").Servers[env.serverID] }
	want := strconv.Itoa(pgBusyRows)
	// Connections that will read the copy, each with the copy's list of
	// columns already read (kept 10 seconds), so that what is refused below
	// is the statement's own run, not that list. The source never answers
	// them, which keeps them on the copy afterwards (#2985).
	p1 := pgClient(t, ctx, env.portDSN(), nil)
	p2 := pgClient(t, ctx, env.portDSN(), nil)
	ext := pgxConn(t, ctx, env.portDSN(), pgx.QueryExecModeCacheStatement)
	h := pgClient(t, ctx, env.portDSN(), nil)
	if got := one(ctx, p1, heavy); got != want {
		t.Fatalf("the copy free: %q, want the copy's %s", got, want)
	}
	if got := one(ctx, p2, heavy); got != want {
		t.Fatalf("the copy free, another connection: %q", got)
	}
	if got := pgxAnswer(ctx, ext, heavy+" WHERE amount > $1", 0); got != `[sum] ["`+want+`"]` {
		t.Fatalf("prepared, the copy free: %s", got)
	}
	if got := one(ctx, h, heavy); got != want {
		t.Fatalf("the holder's connection: %q", got)
	}
	if r := numbers().Reasons[pgshim.ReasonAnswersTheSame]; r < 4 {
		t.Fatalf("reasons %v, want 4 answers_the_same", numbers().Reasons)
	}

	// The copy's one worker held.
	hctx, stopHolder := context.WithCancel(ctx)
	defer stopHolder()
	held := make(chan string, 1)
	go func() { held <- one(hctx, h, holder) }()
	waitUntil(t, "the long statement to hold the copy's worker", func() bool { return env.runner.Running() >= 1 })
	start := hits()
	refusals := 0
	busy := func(what, got string) {
		t.Helper()
		refusals++
		if !strings.HasPrefix(got, "error 53000 the copy is busy (copy_wait_timeout)") || !strings.Contains(got, "not sent to your PostgreSQL") {
			t.Errorf("%s: %q, want 53000 for a busy copy", what, got)
		}
		t.Logf("%s: %s", what, got)
	}
	busy("simple protocol", one(ctx, p1, heavy))
	busy("pgx, a prepared statement", pgxAnswer(ctx, ext, heavy+" WHERE amount > $1", 0))
	// The same connections are usable at once, and the source still
	// answers what it would have.
	if got := one(ctx, p2, cheap); got != "1" {
		t.Errorf("a cheap read while the copy is busy: %q, want the source's answer", got)
	}
	if n := hits(); n == start {
		t.Error("the cheap read was not run on the source")
	}
	start = hits()
	// A connection whose list of columns is not read yet: the list itself
	// waits for the worker, and the statement the source's plan sends to
	// the copy is refused all the same.
	fresh := pgClient(t, ctx, env.portDSN(), nil)
	busy("simple protocol, the list of columns not read", one(ctx, fresh, heavy))
	for name, mode := range map[string]pgx.QueryExecMode{
		"cache_statement": pgx.QueryExecModeCacheStatement, "cache_describe": pgx.QueryExecModeCacheDescribe,
		"describe_exec": pgx.QueryExecModeDescribeExec, "exec": pgx.QueryExecModeExec, "simple_protocol": pgx.QueryExecModeSimpleProtocol,
	} {
		conn := pgxConn(t, ctx, env.portDSN(), mode)
		busy("pgx "+name, pgxAnswer(ctx, conn, heavy+" WHERE amount > $1", 0))
		// Usable after the error, the prepared statement cache included.
		if got := pgxAnswer(ctx, conn, "SELECT 1 AS one"); got != `[one] ["1"]` {
			t.Errorf("pgx %s after the refusal: %s", name, got)
		}
	}
	t.Run("psql", func(t *testing.T) {
		psql, err := exec.LookPath("psql")
		if err != nil {
			if os.Getenv("BINTRAIL_REQUIRE_PSQL") != "" {
				t.Fatal("psql is not installed, and BINTRAIL_REQUIRE_PSQL says it must be")
			}
			t.Skip("psql is not installed here")
		}
		out, err := exec.CommandContext(ctx, psql, "-X", "-At", "-c", heavy, env.portDSN()).CombinedOutput()
		refusals++
		if err == nil || !strings.Contains(string(out), "the copy is busy (copy_wait_timeout)") {
			t.Errorf("psql: %v\n%s", err, out)
		}
		t.Logf("psql: %s", strings.TrimSpace(string(out)))
	})
	if n := hits(); n != start {
		t.Errorf("the source read %d rows for statements refused as busy, want none", n-start)
	}
	// A transaction block lives on the source: its statements go there as
	// before, the heavy one included.
	block := pgClient(t, ctx, env.portDSN(), nil)
	if got := one(ctx, block, "BEGIN"); strings.HasPrefix(got, "error") {
		t.Fatalf("BEGIN: %s", got)
	}
	if got := one(ctx, block, heavy); got != want {
		t.Errorf("in a block: %q, want the source's %s", got, want)
	}
	if got := one(ctx, block, "COMMIT"); strings.HasPrefix(got, "error") {
		t.Errorf("COMMIT: %s", got)
	}
	if hits() == start {
		t.Error("the block's statement was not run on the source")
	}

	// Counted under its own reason, as refused, in /status, the status
	// statement and the status line.
	n := numbers()
	if n.Reasons[pgshim.ReasonCopyWaitTimeout] != uint64(refusals) || n.Refused != uint64(refusals) {
		t.Errorf("refused %d, reasons %v; want %d copy_wait_timeout refusals", n.Refused, n.Reasons, refusals)
	}
	if c := pgPortCountsOf(n); c.refused != uint64(refusals) {
		t.Errorf("the status line counts %d refused, want %d", c.refused, refusals)
	}
	_, rows, err := pgText(t, p1, pgshim.StatusStatement)
	if err != nil {
		t.Fatal(err)
	}
	status := map[string]string{}
	for _, r := range rows {
		status[r[0]] = r[1]
	}
	if status["last_answer"] != "refused" || status["last_answer_reason"] != pgshim.ReasonCopyWaitTimeout ||
		status["statements_refused"] != strconv.Itoa(refusals) || status["reason_copy_wait_timeout"] != strconv.Itoa(refusals) {
		t.Errorf("%s: %v", pgshim.StatusStatement, status)
	}

	// The worker given back: the copy answers again, on the connections it
	// refused.
	if err := h.CancelRequest(ctx); err != nil {
		t.Errorf("cancel the long statement: %v", err)
	}
	if got := <-held; !strings.HasPrefix(got, "error 57014") {
		t.Errorf("the long statement, cancelled: %q", got)
	}
	waitUntil(t, "the copy's worker to be given back", func() bool { return env.runner.Running() == 0 })
	if got := one(ctx, p1, heavy); got != want {
		t.Errorf("the copy free again: %q, want the copy's %s", got, want)
	}
	if got := pgxAnswer(ctx, ext, heavy+" WHERE amount > $1", 0); got != `[sum] ["`+want+`"]` {
		t.Errorf("prepared, the copy free again: %s", got)
	}
	if t.Failed() {
		t.Logf("reasons at the end: %s", fmt.Sprint(numbers().Reasons))
	}
}
