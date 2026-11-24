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
// What runs on the source is read from the source's own counts of the
// table's scans (pg_stat_user_tables): a sum of the table is a sequential
// scan there, a read by its key an index scan, and an EXPLAIN neither. The
// port opens its connections there read-only, so nothing can be counted by
// a write. A backend publishes its counts when it goes idle, up to about 10
// seconds later (PGSTAT_IDLE_INTERVAL), so they are waited for.

const (
	pgBusyTable = "busy3109_t"
	pgBusyRole  = "busy3109_fwd"
	pgBusyPass  = "fwd-pw-busy3109"
	pgBusyRows  = 20000
	// pgStatsSettle is longer than a backend takes to publish its counts.
	pgStatsSettle = 12 * time.Second
)

func TestIntegrationPGRouterBusyCopyRefusesNotForwards(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	cleanup := []string{
		"DROP TABLE IF EXISTS " + pgBusyTable,
		`DO $$ BEGIN IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '` + pgBusyRole + `') THEN EXECUTE 'DROP OWNED BY ` + pgBusyRole + `'; END IF; END $$`,
		"DROP ROLE IF EXISTS " + pgBusyRole,
	}
	setup := append(append([]string{}, cleanup...),
		"CREATE ROLE "+pgBusyRole+" LOGIN PASSWORD '"+pgBusyPass+"'",
		"CREATE TABLE "+pgBusyTable+" (id int PRIMARY KEY, amount int NOT NULL)",
		"INSERT INTO "+pgBusyTable+" SELECT g, 1 FROM generate_series(1, "+strconv.Itoa(pgBusyRows)+") g",
		"ANALYZE "+pgBusyTable,
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
	// scans is how many sequential and index scans of the table the source
	// has counted.
	scans := func() (seq, idx int) {
		t.Helper()
		got := one(ctx, source, "SELECT seq_scan || ' ' || coalesce(idx_scan, 0) FROM pg_stat_user_tables WHERE relname = '"+pgBusyTable+"'")
		if _, err := fmt.Sscan(got, &seq, &idx); err != nil {
			t.Fatalf("the source's scans of %s: %q", pgBusyTable, got)
		}
		return seq, idx
	}
	scansReach := func(what string, ok func(seq, idx int) bool) {
		t.Helper()
		deadline := time.Now().Add(3 * pgStatsSettle)
		for {
			seq, idx := scans()
			if ok(seq, idx) {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s: the source counted %d sequential and %d index scans", what, seq, idx)
			}
			time.Sleep(250 * time.Millisecond)
		}
	}

	heavy := "SELECT sum(amount) FROM " + pgBusyTable
	cheap := "SELECT amount FROM " + pgBusyTable + " WHERE id = 7"
	holder := "SELECT count(*) FROM " + pgBusyTable + " a, " + pgBusyTable + " b, " + pgBusyTable + " c WHERE a.amount + b.amount + c.amount > 0"
	// The plans this test stands on, asked of the source first: a planner
	// that plans otherwise fails here and says so.
	seqRows := func(stmt string) float64 {
		t.Helper()
		var doc []struct {
			Plan map[string]any `json:"Plan"`
		}
		if err := json.Unmarshal([]byte(one(ctx, source, "EXPLAIN (FORMAT JSON) "+stmt)), &doc); err != nil || len(doc) != 1 {
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

	// What the fixture did on the source (its snapshot reads the table
	// whole) is published by then. Nothing below runs on the source until
	// the copy is held: the copy answers it.
	time.Sleep(pgStatsSettle)
	seq0, idx0 := scans()
	t.Logf("the source's scans of the table before the copy is held: %d sequential, %d by index", seq0, idx0)

	numbers := func() pgRouterServerNumbers { return env.routes.doc("").Servers[env.serverID] }
	want := strconv.Itoa(pgBusyRows)
	// Connections that will read the copy, each with the copy's list of
	// columns already read (kept 10 seconds, so read just before the copy
	// is held), so that what is refused first below is the statement's own
	// run, not that list. The source never answers them, which keeps them
	// on the copy afterwards (#2985).
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
	// The source still answers what it would have.
	if got := one(ctx, p2, cheap); got != "1" {
		t.Errorf("a cheap read while the copy is busy: %q, want the source's answer", got)
	}
	// A connection whose list of columns is not read yet: the list itself
	// waits for the worker, and the statement the source's plan sends to
	// the copy is refused all the same.
	fresh := pgClient(t, ctx, env.portDSN(), nil)
	busy("simple protocol, the list of columns not read", one(ctx, fresh, heavy))
	// A column over a number written in the text is the engine's or the
	// macro's by the column's type, and is a busy refusal like any other.
	busy("a column over a number, the list of columns not read", one(ctx, pgClient(t, ctx, env.portDSN(), nil), "SELECT amount / 3 FROM "+pgBusyTable))
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
	// The cheap read reached the source (an index scan); every statement
	// refused as busy did not (no sequential scan at all).
	scansReach("the cheap read", func(_, idx int) bool { return idx > idx0 })
	time.Sleep(pgStatsSettle)
	if seq, _ := scans(); seq != seq0 {
		t.Errorf("the source scanned the table %d times for statements refused as busy, want none", seq-seq0)
	}
	// A transaction block lives on the source: its statements go there as
	// before, the heavy one included, and the count sees it.
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
	scansReach("the block's sum", func(seq, _ int) bool { return seq > seq0 })

	// A division or an avg: whether the copy may answer it is told by the
	// columns' types (#3219), so with the list not read it is the source's,
	// as it was before the copy answered any, and not a statement refused.
	for _, stmt := range []string{
		"SELECT avg(amount) FROM " + pgBusyTable,
		"SELECT sum(amount) / count(*) FROM " + pgBusyTable,
	} {
		want := one(ctx, source, stmt)
		if got := one(ctx, pgClient(t, ctx, env.portDSN(), nil), stmt); got != want {
			t.Errorf("%s, the copy busy and its columns not read: %q, want the source's %q", stmt, got, want)
		}
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
		t.Logf("reasons at the end: %v", numbers().Reasons)
	}
}
