package consoleapp

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/pgshim"
)

// The PostgreSQL port's row cap is its own (#3286): 200,000 rows, where the
// MySQL port keeps 10,000. CH-benCHmark's Q16 returns about 100,000.
func TestPGRouterRowCapIsItsOwn_3286(t *testing.T) {
	if defaultPGRouterMaxRows != 200_000 || console.DefaultSQLPortMaxRows != 10_000 {
		t.Fatalf("PostgreSQL port %d, MySQL port %d; want 200,000 and 10,000", defaultPGRouterMaxRows, console.DefaultSQLPortMaxRows)
	}
	st, err := pgRouterSettingsFrom(pgRouterTestCmd(t))
	if err != nil {
		t.Fatal(err)
	}
	// The plan rule that sends an estimated big result to the source keeps
	// 10,000: what it sent there before still goes there, so a wide result
	// it sent is never run on the copy and refused for its size.
	if st.SQLPortMaxRows != 200_000 || st.Cost.RowCap != 10_000 {
		t.Errorf("row cap %d, cost rule's %d; want 200,000 and 10,000", st.SQLPortMaxRows, st.Cost.RowCap)
	}
	t.Setenv("BINTRAIL_CONSOLE_SQL_PORT_MAX_ROWS", "5000")
	if st, err = pgRouterSettingsFrom(pgRouterTestCmd(t)); err != nil || st.SQLPortMaxRows != 5000 || st.Cost.RowCap != 5000 {
		t.Errorf("the variable still sets it: %d, %d, %v", st.SQLPortMaxRows, st.Cost.RowCap, err)
	}
}

// /status counts the results refused for their size, by which cap.
func TestPGRouteStatsCountsResultTooLarge_3286(t *testing.T) {
	r := newPGRouteStats(time.Now(), false)
	r.note("s1", pgshim.RouteNote{Side: pgshim.RouteCopy, Reason: pgshim.ReasonHeavyOnCopy, Failed: true, TooLarge: pgshim.TooLargeRows})
	r.note("s1", pgshim.RouteNote{Side: pgshim.RouteCopy, Reason: pgshim.ReasonHeavyOnCopy, Failed: true, TooLarge: pgshim.TooLargeRows})
	r.note("s1", pgshim.RouteNote{Side: pgshim.RouteCopy, Reason: pgshim.ReasonHeavyOnCopy, Failed: true, TooLarge: pgshim.TooLargeBytes})
	r.note("s1", pgshim.RouteNote{Side: pgshim.RouteCopy, Reason: pgshim.ReasonHeavyOnCopy})
	n := r.doc("").Servers["s1"]
	if n.ResultTooLarge["rows"] != 2 || n.ResultTooLarge["bytes"] != 1 || len(n.ResultTooLarge) != 2 {
		t.Errorf("result_too_large %v", n.ResultTooLarge)
	}
	b, err := json.Marshal(n)
	if err != nil || !strings.Contains(string(b), `"result_too_large":{"bytes":1,"rows":2}`) {
		t.Errorf("%s %v", b, err)
	}
	// A server with none has no field.
	r.note("s2", pgshim.RouteNote{Side: pgshim.RouteCopy, Reason: pgshim.ReasonHeavyOnCopy})
	if b, _ := json.Marshal(r.doc("").Servers["s2"]); strings.Contains(string(b), "result_too_large") {
		t.Errorf("%s", b)
	}
}

// The answer's memory is in the memory warning (#3296): the calibration
// still holds with it. The 8 GB host of #3296 warns; the same setup on
// 16 GB and the two PostgreSQL boards fit.
func TestPGMemoryCountsTheAnswer_3286(t *testing.T) {
	if pgCopyCellBytes != pgshim.CellMemoryBytes || pgCopyAnswerBytes != 1_000_000*pgshim.CellMemoryBytes+pgshim.TextCopiesInMemory*64<<20 {
		t.Fatalf("answer bound %d", pgCopyAnswerBytes)
	}
	plan := func(mem uint64) pgMemoryPlan {
		return pgMemoryPlan{Servers: 1, Workers: 2, Memory: "2GB", ResultsBytes: defaultFlashbackResultBudget, AnswerBytes: pgCopyAnswerBytes,
			In: pgTestMachine(mem), Pools: []pgIndexPool{{Bytes: 3 * pgTestGiB, Place: pgPoolBundled}}}
	}
	if d := pgMemoryVerdict(plan(8 * pgTestGiB)); d.Verdict != "over" {
		t.Errorf("8 GB: %+v", d)
	}
	for _, mem := range []uint64{16 * pgTestGiB, 15988900 << 10, 16041000 << 10} {
		d := pgMemoryVerdict(plan(mem))
		if d.Verdict != "fits" {
			t.Errorf("%d bytes: %+v", mem, d)
		}
		if !strings.Contains(pgMemoryLines(d), "for its answer while it is read and sent") {
			t.Errorf("%q", pgMemoryLines(d))
		}
	}
	// The advice of one worker leaves room for its answer.
	d := pgMemoryVerdict(plan(8 * pgTestGiB))
	if !strings.Contains(d.Advice, "PG_ROUTER_WORKERS=1") {
		t.Errorf("advice %q", d.Advice)
	}
}
