package consoleapp

import (
	"context"
	"database/sql"
	"testing"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/duckdbutil"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/verify"
)

// TestBudgetFor pins the arithmetic the doc comment states, and its edges:
// unknown memory changes nothing, a small host gets the floor, a large one
// never more than DefaultTuning's 4GB. The solo budgets carry the same
// memory per session.
func TestBudgetFor(t *testing.T) {
	const gib = 1 << 30
	if _, ok := budgetFor(0, 8); ok {
		t.Error("unknown host memory must leave the default in place")
	}
	for _, tc := range []struct {
		host            uint64
		snapshot, check string
	}{
		{8 * gib, "1092MiB", "546MiB"},
		{16 * gib, "2184MiB", "1092MiB"},
		{4 * gib, "546MiB", "512MiB"},
		{1 * gib, "512MiB", "512MiB"},
		{64 * gib, "4096MiB", "4096MiB"},
	} {
		b, ok := budgetFor(tc.host, 8)
		if !ok || b.snapshot.MemoryLimit != tc.snapshot || b.check.MemoryLimit != tc.check {
			t.Errorf("budgetFor(%d GiB, 8) = %+v, %v; want snapshot %s, check %s", tc.host/gib, b, ok, tc.snapshot, tc.check)
		}
		if b.snapshotSolo.MemoryLimit != tc.snapshot || b.checkSolo.MemoryLimit != tc.check {
			t.Errorf("budgetFor(%d GiB, 8) solo memory = %s/%s, want %s/%s", tc.host/gib, b.snapshotSolo.MemoryLimit, b.checkSolo.MemoryLimit, tc.snapshot, tc.check)
		}
		if b.snapshot.Threads != 2 || b.check.Threads != 2 || b.snapshotSolo.Threads != 2 || b.checkSolo.Threads != 2 {
			t.Errorf("budgetFor(%d GiB, 8) threads = %+v, want 2 everywhere: 8 cores keep today's numbers", tc.host/gib, b)
		}
	}
}

// TestBudgetFor_threads pins the thread rule (#2491): the lane's sessions
// never add up to more than half the cores, never more than DefaultTuning's
// 2 per session, never 0 (a Tuning without threads leaves DuckDB at every
// core). Both lanes run 2 sessions at once, so they get the same numbers.
func TestBudgetFor_threads(t *testing.T) {
	for _, tc := range []struct {
		cores              int
		atOnce, oneAtATime int
	}{
		{0, 1, 1}, // no count at all: the least
		{1, 1, 1},
		{2, 1, 1}, // half is one core: the two sessions still take both
		{3, 1, 1},
		{4, 1, 2}, // the fold 2 x 1, the resolved pairs 1 x 2
		{5, 1, 2},
		{6, 1, 2},
		{7, 1, 2},
		{8, 2, 2}, // today's numbers, unchanged
		{16, 2, 2},
		{64, 2, 2},
	} {
		b, ok := budgetFor(8<<30, tc.cores)
		if !ok {
			t.Fatalf("cores %d: not ok", tc.cores)
		}
		got := [4]int{b.snapshot.Threads, b.check.Threads, b.snapshotSolo.Threads, b.checkSolo.Threads}
		want := [4]int{tc.atOnce, tc.atOnce, tc.oneAtATime, tc.oneAtATime}
		if got != want {
			t.Errorf("cores %d: threads (snapshot, check, snapshot solo, check solo) = %v, want %v", tc.cores, got, want)
		}
		for i, n := range got {
			if n < 1 {
				t.Errorf("cores %d: threads[%d] = %d; 0 would hand DuckDB every core", tc.cores, i, n)
			}
		}
	}
	if duckdbutil.DefaultTuning().Threads != 2 {
		t.Errorf("DefaultTuning threads = %d; the table above assumes 2 as the ceiling", duckdbutil.DefaultTuning().Threads)
	}
}

// TestBudgetFor_duckDBTakesTheLimit: the rendered limit is one DuckDB
// accepts (a SET it refused would leave the session at 80% of RAM, warned).
func TestBudgetFor_duckDBTakesTheLimit(t *testing.T) {
	b, _ := budgetFor(8<<30, 8)
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec("SET memory_limit = '" + b.snapshot.MemoryLimit + "'"); err != nil {
		t.Fatalf("DuckDB refused %q: %v", b.snapshot.MemoryLimit, err)
	}
	var got string
	if err := db.QueryRow("SELECT current_setting('memory_limit')").Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != "1.0 GiB" && got != "1092.0 MiB" {
		t.Errorf("memory_limit after SET %s = %q", b.snapshot.MemoryLimit, got)
	}
}

func withBudget(t *testing.T, b heavyBudget) {
	t.Helper()
	prev := processBudget
	t.Cleanup(func() { processBudget = prev })
	processBudget = func() (heavyBudget, bool) { return b, true }
}

// TestLaneCtx: each lane's sessions get that lane's budget; with the host's
// memory unknown they keep the default.
func TestLaneCtx(t *testing.T) {
	prev := processBudget
	t.Cleanup(func() { processBudget = prev })
	processBudget = func() (heavyBudget, bool) { return heavyBudget{}, false }
	if got := duckdbutil.DefaultTuningFor(laneCtx(context.Background(), laneSnapshot)); got != duckdbutil.DefaultTuning() {
		t.Errorf("unknown host: %+v, want the default", got)
	}
	if got := duckdbutil.DefaultTuningFor(soloLaneCtx(context.Background(), laneCheck)); got != duckdbutil.DefaultTuning() {
		t.Errorf("unknown host, solo: %+v, want the default", got)
	}
	// 4 cores: the at-once and the solo budgets differ (1 vs 2 threads), so
	// a lane handed the wrong one is seen.
	b, _ := budgetFor(8<<30, 4)
	if b.snapshot == b.snapshotSolo || b.check == b.checkSolo {
		t.Fatalf("the test needs the two budgets to differ: %+v", b)
	}
	withBudget(t, b)
	if got := duckdbutil.DefaultTuningFor(laneCtx(context.Background(), laneSnapshot)); got != b.snapshot {
		t.Errorf("snapshot lane: %+v, want %+v", got, b.snapshot)
	}
	if got := duckdbutil.DefaultTuningFor(laneCtx(context.Background(), laneCheck)); got != b.check {
		t.Errorf("check lane: %+v, want %+v", got, b.check)
	}
	if got := duckdbutil.DefaultTuningFor(soloLaneCtx(context.Background(), laneSnapshot)); got != b.snapshotSolo {
		t.Errorf("snapshot lane, solo: %+v, want %+v", got, b.snapshotSolo)
	}
	if got := duckdbutil.DefaultTuningFor(soloLaneCtx(context.Background(), laneCheck)); got != b.checkSolo {
		t.Errorf("check lane, solo: %+v, want %+v", got, b.checkSolo)
	}
}

// TestBudgetWiring_foldAndCheck: the fold of a refresh gets the snapshot
// budget (two tables at once), a check table the check budget for one
// session at a time. On 4 cores the two differ in threads, so a job handed
// the wrong one fails here.
func TestBudgetWiring_foldAndCheck(t *testing.T) {
	b, _ := budgetFor(8<<30, 4)
	withBudget(t, b)
	s := newBaselineSupervisor(context.Background(), t.TempDir(), baseline.LockModeSafeNoLock)
	s.gate = newHeavyGate()
	prev := foldTables
	t.Cleanup(func() { foldTables = prev })
	var got duckdbutil.Tuning
	foldTables = func(ctx context.Context, _ reconstruct.FullTableConfig) ([]*reconstruct.TableReport, []reconstruct.TableFailure, error) {
		got = duckdbutil.DefaultTuningFor(ctx)
		return nil, nil, nil
	}
	s.foldSnapshot(refreshRequest{ServerID: "a", ServerName: "a"}, time.Now(), []string{"db.t"})
	if got != b.snapshot {
		t.Errorf("fold budget = %+v, want %+v", got, b.snapshot)
	}

	v := &verifySupervisor{gate: newHeavyGate(), jobs: map[string]*verifyJob{}}
	if err := v.inCheckLane(context.Background(), "a", func(ctx context.Context) { got = duckdbutil.DefaultTuningFor(ctx) }); err != nil {
		t.Fatal(err)
	}
	if got != b.checkSolo {
		t.Errorf("check table budget = %+v, want %+v (one session at a time)", got, b.checkSolo)
	}
}

// TestBudgetWiring_explain: an Explain's sessions (two at once) get the
// check budget, not the solo one.
func TestBudgetWiring_explain(t *testing.T) {
	b, _ := budgetFor(8<<30, 4)
	withBudget(t, b)
	s := newVerifySupervisor(context.Background(), nil, nil)
	s.gate = newHeavyGate()
	got := make(chan duckdbutil.Tuning, 1)
	s.explainFn = func(ctx context.Context, _ string, _ bool, _, _ string, _ verify.BaselinePair) (*console.VerifyExplanation, error) {
		got <- duckdbutil.DefaultTuningFor(ctx)
		return &console.VerifyExplanation{}, nil
	}
	if _, err := s.begin(console.VerifyRequest{ServerID: "a", Mode: console.VerifyModeBaselineAnchored}, console.VerifyTriggerManual); err != nil {
		t.Fatal(err)
	}
	s.cachePair("a", "db.t", verify.BaselinePair{Schema: "db", Table: "t"})
	_, _ = s.Explain("a", "db", "t")
	select {
	case tu := <-got:
		if tu != b.check {
			t.Errorf("Explain budget = %+v, want %+v", tu, b.check)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the Explain never ran")
	}
}

// TestBudgetWiring_compactionAndResolve: a compaction's merges and the
// resolved pairs' merges, one session at a time, get the snapshot lane's
// solo budget.
func TestBudgetWiring_compactionAndResolve(t *testing.T) {
	b, _ := budgetFor(8<<30, 4)
	withBudget(t, b)

	sup, req, _, _ := compactRig(t, compactMinPairs)
	var compacted duckdbutil.Tuning
	prevCompact := compactMinor
	t.Cleanup(func() { compactMinor = prevCompact })
	compactMinor = func(ctx context.Context, base string, c *baseline.TableDeltaChain, lo, hi int, outDir string) (*reconstruct.MinorCompaction, error) {
		compacted = duckdbutil.DefaultTuningFor(ctx)
		return prevCompact(ctx, base, c, lo, hi, outDir)
	}
	sup.maybeCompact(req)
	waitCompact(t, sup, "s")
	if compacted != b.snapshotSolo {
		t.Errorf("compaction budget = %+v, want %+v (one session at a time)", compacted, b.snapshotSolo)
	}

	rsup, rreq, _, _ := resolveRig(t, []int{1}, map[string]int{"orders": 2})
	var resolved duckdbutil.Tuning
	prevResolve := resolveTableDelta
	t.Cleanup(func() { resolveTableDelta = prevResolve })
	resolveTableDelta = func(ctx context.Context, base string, c *baseline.TableDeltaChain, prevBase string) (bool, bool, error) {
		resolved = duckdbutil.DefaultTuningFor(ctx)
		return prevResolve(ctx, base, c, prevBase)
	}
	rsup.maybeResolve(rreq)
	if resolved != b.snapshotSolo {
		t.Errorf("resolved pairs budget = %+v, want %+v (one session at a time)", resolved, b.snapshotSolo)
	}
}
