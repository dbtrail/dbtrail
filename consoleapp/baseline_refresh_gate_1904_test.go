package consoleapp

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/status"
)

// #1904: with table deltas a reader fetches events from where a table's chain
// started, up to a day before the snapshot's folder. The gate watched the
// folder, and the refresh it released only added a pair and kept the start, so
// on a quiet server there were hours where the gate said covered and a restore
// refused. The fix has two halves that must agree on ONE line: the gate grades
// where readers start, and the fold writes a table in full when its chain start
// has reached the same line.

// gateVerdictBefore1904 is the gate's predicate as it stood, stated with the
// product's own grading and the retention band it applied beside it. The rule
// reproduces it exactly, plus one margin: a start within an hour and an
// interval of the floor is not covered (the floor moves in hour steps and
// nothing looks again for an interval). The aging band is the product's, and
// the gate was right about everything but the instant it graded.
func gateVerdictBefore1904(readsFrom, liveFloor, now time.Time, retain time.Duration) bool {
	ok := status.BaselineStalenessFor(readsFrom, liveFloor, now) == status.BaselineOK
	if retain > 0 {
		ok = ok && now.Sub(readsFrom) < time.Duration(float64(retain)*status.BaselineAgingFraction)
	}
	return ok
}

func TestCoverageRule_isTheGatesVerdictWithAMargin(t *testing.T) {
	now := time.Date(2026, 9, 28, 12, 0, 0, 0, time.UTC)
	floors := []time.Time{
		now.Add(-48 * time.Hour), now.Add(-12 * time.Hour), now.Add(-2 * time.Hour),
		now.Add(-30 * time.Minute), now, now.Add(time.Hour), // the last two: a clock behind the index
		now.Add(-12*time.Hour - 7), // a span whose 0.8 is not a whole nanosecond: the line rounds the way the verdict does
	}
	for _, retain := range []time.Duration{0, 48 * time.Hour, 12 * time.Hour, 6 * time.Hour} {
		rule := coverageRule{retain: retain}
		for _, floor := range floors {
			line, ok := rule.reanchorBy(floor, now)
			if !ok {
				t.Fatalf("retain %s, floor %s: no line with a known floor", retain, floor)
			}
			// Every minute of three days, plus a nanosecond each side of the
			// line itself, where rounding would show.
			var starts []time.Time
			for s := now.Add(-60 * time.Hour); !s.After(now.Add(2 * time.Hour)); s = s.Add(time.Minute) {
				starts = append(starts, s)
			}
			starts = append(starts, line.Add(-time.Nanosecond), line, line.Add(time.Nanosecond))
			for _, s := range starts {
				effective := floor
				if retain > 0 && now.Add(-retain).After(effective) {
					effective = now.Add(-retain)
				}
				want := gateVerdictBefore1904(s, floor, now, retain) && s.After(effective.Add(time.Hour))
				if got := s.After(line); got != want {
					t.Fatalf("retain %s, floor %s, start %s: covered = %v by the line %s, want %v as the gate graded it",
						retain, floor.Format(time.RFC3339), s.Format(time.RFC3339Nano), got, line.Format(time.RFC3339Nano), want)
				}
			}
		}
	}
	if _, ok := (coverageRule{retain: time.Hour}).reanchorBy(time.Time{}, now); ok {
		t.Error("an unknown floor drew a line: nothing can be graded without one, and a line made up here would force rewrites on a guess")
	}
}

// The floor moves in hour steps (partitions are hourly) and the next look is a
// whole interval away, so the line keeps at least that much above the floor even
// where the aging band is thinner. For a rule that does not know when rotation
// reaches the floor, which these are (#2121: floorHolds).
func TestCoverageRule_keepsANextCycleAboveTheFloor(t *testing.T) {
	now := time.Date(2026, 9, 28, 12, 0, 0, 0, time.UTC)
	floor := now.Add(-3 * time.Hour) // aging band: 0.2 of 3h = 36 minutes
	for _, interval := range []time.Duration{0, 5 * time.Minute, 30 * time.Minute, 2 * time.Hour} {
		line, _ := coverageRule{interval: interval}.reanchorBy(floor, now)
		if min := floor.Add(time.Hour + interval); line.Before(min) {
			t.Errorf("interval %s: line %s is under %s: a chain just above it could be past the floor by the next cycle",
				interval, line.Format(time.RFC3339), min.Format(time.RFC3339))
		}
	}
	// A configured retention shorter than what the partitions hold moves the
	// floor the margin is measured from: the policy drops before the partitions
	// show it.
	line, _ := coverageRule{retain: 2 * time.Hour, interval: 30 * time.Minute}.reanchorBy(now.Add(-48*time.Hour), now)
	if min := now.Add(-2*time.Hour + time.Hour + 30*time.Minute); line.Before(min) {
		t.Errorf("2h retention over 48h of partitions: line %s is under %s, an hour and a cycle above the configured floor",
			line.Format(time.RFC3339), min.Format(time.RFC3339))
	}
	// And where the band is the wider of the two, it is the band.
	floor = now.Add(-30 * 24 * time.Hour)
	line, _ = coverageRule{interval: 5 * time.Minute}.reanchorBy(floor, now)
	if want := now.Add(-24 * 24 * time.Hour); line.Sub(want).Abs() > time.Second {
		t.Errorf("30 days of partitions: line %s, want the aging band's %s", line, want)
	}
}

// passResult is what one simulated run of refresh cycles did to one table.
type passResult struct {
	folds, forced, byAge int
	back2back            bool
	// worst is the smallest distance, over every cycle, between where a reader
	// starts and the floor. Below zero, a restore would have refused.
	worst time.Duration
}

// simulate runs cycles every interval for horizon over a table with a chain, a
// full backup at t0. busy: every cycle has events, so every cycle folds. Quiet:
// nothing is ever indexed, so a cycle folds only when the gate opens. The fold
// follows reconstruct's rule: the table is written in full when its chain is a
// day old (tableDeltaMaxAge) or when its start is not after the line the daemon
// hands it (ChainStartFloor, the same reanchorBy the gate uses).
func simulate(retain, interval, horizon time.Duration, busy bool) passResult {
	t0 := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	rule := coverageRule{retain: retain, interval: interval}
	start := t0
	res := passResult{worst: time.Duration(1<<63 - 1)}
	rewrotePrev := false
	for at := t0.Add(interval); !at.After(t0.Add(horizon)); at = at.Add(interval) {
		floor := at.Add(-retain).Truncate(time.Hour) // hourly partitions
		if d := start.Sub(floor); d < res.worst {
			res.worst = d
		}
		line, _ := rule.reanchorBy(floor, at)
		if !busy && start.After(line) {
			rewrotePrev = false
			continue // the gate skips: still covered
		}
		res.folds++
		rewrote := false
		switch {
		case at.Sub(start) > 24*time.Hour:
			res.byAge++
			rewrote = true
		case !start.After(line):
			res.forced++
			rewrote = true
		}
		if rewrote {
			if rewrotePrev {
				res.back2back = true
			}
			start = at
		}
		rewrotePrev = rewrote
	}
	return res
}

func TestCoverageRule_replacesTheChainInTimeWithoutARewriteStorm(t *testing.T) {
	const horizon = 6 * 24 * time.Hour
	const interval = 5 * time.Minute
	cases := []struct {
		name   string
		retain time.Duration
		busy   bool
		check  func(t *testing.T, r passResult)
	}{
		{"quiet, 48h retention (the issue's example)", 48 * time.Hour, false, func(t *testing.T, r passResult) {
			// A quiet server folds only to re-anchor: about once per 0.8 of
			// the retention, and never on every cycle.
			if r.folds == 0 || r.folds > int(horizon/(30*time.Hour))+1 {
				t.Errorf("%d folds in 6 days: want one every ~38h", r.folds)
			}
		}},
		{"quiet, 24h retention", 24 * time.Hour, false, func(t *testing.T, r passResult) {
			if r.forced == 0 {
				t.Error("no fold was forced by the line: the day cap alone let the chain reach the floor")
			}
		}},
		{"quiet, 12h retention (shorter than the day cap)", 12 * time.Hour, false, func(t *testing.T, r passResult) {
			if r.forced == 0 || r.byAge != 0 {
				t.Errorf("forced=%d byAge=%d: with 12h of index a chain must end on the line, long before a day", r.forced, r.byAge)
			}
		}},
		{"busy, 48h retention: the day cap governs, nothing extra", 48 * time.Hour, true, func(t *testing.T, r passResult) {
			if r.forced != 0 {
				t.Errorf("%d rewrites forced by the line: with 48h of index the day cap always comes first", r.forced)
			}
		}},
		{"busy, 12h retention: one rewrite per ~10h, not per cycle", 12 * time.Hour, true, func(t *testing.T, r passResult) {
			if max := int(horizon/(9*time.Hour)) + 1; r.forced+r.byAge > max {
				t.Errorf("%d rewrites in 6 days, want at most %d", r.forced+r.byAge, max)
			}
		}},
		{"busy, 24h retention", 24 * time.Hour, true, func(t *testing.T, r passResult) {
			if max := int(horizon/(18*time.Hour)) + 1; r.forced+r.byAge > max {
				t.Errorf("%d rewrites in 6 days, want at most %d", r.forced+r.byAge, max)
			}
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := simulate(c.retain, interval, horizon, c.busy)
			if r.worst < 0 {
				t.Errorf("a reader's start fell %s below the floor: that is the window #1904 is about", -r.worst)
			}
			if r.back2back {
				t.Error("two cycles in a row rewrote the table: a rewrite storm")
			}
			c.check(t, r)
		})
	}
}

// The gate grades where the published snapshot's readers start. Before #1904 it
// graded the folder's instant, which is up to a day later.
func TestRefreshCanSkip_gradesWhereReadersStart(t *testing.T) {
	covers := stubCoverage(t, true, true)
	sup := newBaselineSupervisor(context.Background(), t.TempDir(), baseline.DefaultLockMode)
	sup.retainInForce = func() time.Duration { return 12 * time.Hour }
	req := refreshRequest{ServerID: "s", ServerName: "s", IndexDSN: "d"}
	mark := indexMark{events: 100}
	chainStart := refreshAt.Add(-20 * time.Hour)
	sup.foldedMarks["s"] = foldMemo{mark: mark, publishedAt: refreshAt, readsFrom: chainStart, readsFromKnown: true,
		destination: refreshDestination(req), indexDSN: "d"}
	now := refreshAt.Add(time.Hour)
	sup.refreshCanSkip(context.Background(), req, mark, now, 5*time.Minute)
	got := covers.coverage()
	if len(got) != 1 {
		t.Fatalf("coverage asked %d time(s), want 1", len(got))
	}
	if !got[0].publishedAt.Equal(chainStart) {
		t.Errorf("the gate graded %s, want where readers start (%s), not the folder (%s)",
			got[0].publishedAt.Format(time.RFC3339), chainStart.Format(time.RFC3339), refreshAt.Format(time.RFC3339))
	}
	if want := (coverageRule{retain: 12 * time.Hour, interval: 5 * time.Minute}); got[0].rule != want {
		t.Errorf("the gate graded with %+v, want %+v: the fold draws its line with the same rule, and two rules drift", got[0].rule, want)
	}
}

// A start that could not be read is not a start that is covered.
func TestRefreshCanSkip_anUnreadStartFolds(t *testing.T) {
	stubCoverage(t, true, true)
	sup := newBaselineSupervisor(context.Background(), t.TempDir(), baseline.DefaultLockMode)
	req := refreshRequest{ServerID: "s", ServerName: "s", IndexDSN: "d"}
	mark := indexMark{events: 100}
	sup.foldedMarks["s"] = foldMemo{mark: mark, publishedAt: refreshAt, readsFromKnown: false,
		destination: refreshDestination(req), indexDSN: "d"}
	if sup.refreshCanSkip(context.Background(), req, mark, refreshAt.Add(time.Minute), 0) {
		t.Error("skipped with the snapshot's reads-from instant unknown: the gate would be vouching for a chain it never read")
	}
}

// foldProbe records the configuration each fold was handed.
type foldProbe struct {
	mu   sync.Mutex
	cfgs []reconstruct.FullTableConfig
}

func (p *foldProbe) all() []reconstruct.FullTableConfig {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]reconstruct.FullTableConfig(nil), p.cfgs...)
}

func probeFolds(t *testing.T) *foldProbe {
	t.Helper()
	p := &foldProbe{}
	prev := foldTables
	t.Cleanup(func() { foldTables = prev })
	foldTables = func(_ context.Context, cfg reconstruct.FullTableConfig) ([]*reconstruct.TableReport, []reconstruct.TableFailure, error) {
		p.mu.Lock()
		p.cfgs = append(p.cfgs, cfg)
		p.mu.Unlock()
		return []*reconstruct.TableReport{{Schema: "shop", Table: "orders"}}, nil, nil
	}
	return p
}

func stubLiveFloor(t *testing.T, floor time.Time, known bool) *atomic.Int32 {
	t.Helper()
	var n atomic.Int32
	prev := readLiveFloor
	t.Cleanup(func() { readLiveFloor = prev })
	readLiveFloor = func(context.Context, string) (time.Time, bool) {
		n.Add(1)
		return floor, known
	}
	return &n
}

func stubReadsFrom(t *testing.T, at time.Time, err error) *atomic.Int32 {
	t.Helper()
	var n atomic.Int32
	prev := snapshotReadsFrom
	t.Cleanup(func() { snapshotReadsFrom = prev })
	snapshotReadsFrom = func(context.Context, string, time.Time) (time.Time, error) {
		n.Add(1)
		return at, err
	}
	return &n
}

// deltasRig is a supervisor with table deltas on, a 12h retention, and one
// server whose index never moves.
func deltasRig(t *testing.T, deltas bool) (*baselineSupervisor, refreshRequest, func(at time.Time)) {
	t.Helper()
	mark := indexMark{events: 100, schemaChanges: 7}
	stubIndexMark(t, &mark, true)
	stubBucketListing(t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	sup := newBaselineSupervisor(ctx, t.TempDir(), baseline.DefaultLockMode)
	sup.tableDeltas = deltas
	sup.retainInForce = func() time.Duration { return 12 * time.Hour }
	req := refreshRequest{ServerID: "s", ServerName: "s", IndexDSN: "d", BaselineDir: stageBaselineRoot(t)}
	run := func(at time.Time) {
		sup.refreshes["s"] = &console.BaselineStatus{State: "running"}
		sup.runRefresh(req, at, 5*time.Minute)
	}
	return sup, req, run
}

// The whole loop with deltas on: the fold is handed the line, the memo keeps
// where the published snapshot's readers start, and the next quiet cycle is
// graded on that instant.
func TestRunRefresh_deltasOn_drawsTheLineAndRemembersWhereReadersStart(t *testing.T) {
	folds := probeFolds(t)
	covers := stubCoverage(t, true, true)
	floor := refreshAt.Add(-12 * time.Hour)
	stubLiveFloor(t, floor, true)
	chainStart := refreshAt.Add(-5 * time.Hour)
	reads := stubReadsFrom(t, chainStart, nil)
	sup, _, run := deltasRig(t, true)

	run(refreshAt)
	cfgs := folds.all()
	if len(cfgs) != 1 {
		t.Fatalf("folded %d time(s), want 1", len(cfgs))
	}
	// 12h of index: the aging band (0.2 of 12h above the floor) is wider than
	// an hour and a cycle, so the line is 9h36m before the run.
	want := refreshAt.Add(-9*time.Hour - 36*time.Minute)
	if got := cfgs[0].ChainStartFloor; got.Sub(want).Abs() > time.Millisecond {
		t.Errorf("ChainStartFloor = %s, want %s: without it a quiet table's chain is only ever ended by its age",
			got.Format(time.RFC3339Nano), want.Format(time.RFC3339))
	}
	if reads.Load() != 1 {
		t.Errorf("the published snapshot's reads-from instant was read %d time(s), want once per clean fold", reads.Load())
	}
	sup.mu.Lock()
	memo := sup.foldedMarks["s"]
	sup.mu.Unlock()
	if !memo.readsFromKnown || !memo.readsFrom.Equal(chainStart) {
		t.Errorf("memo reads from %s (known=%v), want the chain start %s", memo.readsFrom, memo.readsFromKnown, chainStart)
	}
	if !memo.publishedAt.Equal(refreshAt) {
		t.Errorf("memo published at %s, want %s: the folder instant still names the snapshot", memo.publishedAt, refreshAt)
	}

	run(refreshAt.Add(10 * time.Minute)) // quiet
	if got := covers.coverage(); len(got) != 1 || !got[0].publishedAt.Equal(chainStart) {
		t.Errorf("the quiet cycle graded %+v, want the chain start %s", got, chainStart)
	}
	if len(folds.all()) != 1 {
		t.Error("the quiet, covered cycle folded")
	}
}

// An index whose floor cannot be read draws no line. Forcing a rewrite on
// doubt would rewrite every table on every cycle; the fold still runs, and the
// day cap still ends chains.
func TestRunRefresh_deltasOn_anUnreadFloorForcesNothing(t *testing.T) {
	folds := probeFolds(t)
	stubCoverage(t, true, true)
	stubLiveFloor(t, time.Time{}, false)
	stubReadsFrom(t, refreshAt, nil)
	_, _, run := deltasRig(t, true)
	run(refreshAt)
	cfgs := folds.all()
	if len(cfgs) != 1 {
		t.Fatalf("folded %d time(s), want 1: an unreadable floor must not stop the refresh", len(cfgs))
	}
	if !cfgs[0].ChainStartFloor.IsZero() {
		t.Errorf("ChainStartFloor = %s with the floor unread: a guessed line", cfgs[0].ChainStartFloor)
	}
}

// When the published snapshot's chains cannot be read, the memo says so and
// the next quiet cycle folds instead of vouching for the folder's instant.
func TestRunRefresh_deltasOn_anUnreadSnapshotFoldsNextCycle(t *testing.T) {
	folds := probeFolds(t)
	stubCoverage(t, true, true)
	stubLiveFloor(t, refreshAt.Add(-12*time.Hour), true)
	stubReadsFrom(t, time.Time{}, errors.New("half a pair"))
	sup, _, run := deltasRig(t, true)
	run(refreshAt)
	sup.mu.Lock()
	memo, seen := sup.foldedMarks["s"]
	sup.mu.Unlock()
	if !seen || memo.readsFromKnown {
		t.Fatalf("memo = %+v (seen=%v), want a memo whose reads-from instant is unknown", memo, seen)
	}
	run(refreshAt.Add(10 * time.Minute))
	if n := len(folds.all()); n != 2 {
		t.Errorf("folded %d time(s), want 2: a snapshot whose chains could not be read cannot be vouched for", n)
	}
}

// Deltas off: no chain can be in the published snapshot, so its readers start
// at its folder, no footer is read, and no line is drawn.
func TestRunRefresh_deltasOff_readsNothingExtra(t *testing.T) {
	folds := probeFolds(t)
	covers := stubCoverage(t, true, true)
	floorReads := stubLiveFloor(t, refreshAt.Add(-12*time.Hour), true)
	reads := stubReadsFrom(t, refreshAt.Add(-5*time.Hour), nil)
	sup, _, run := deltasRig(t, false)
	run(refreshAt)
	run(refreshAt.Add(10 * time.Minute))
	if n := len(folds.all()); n != 1 {
		t.Fatalf("folded %d time(s), want 1", n)
	}
	if !folds.all()[0].ChainStartFloor.IsZero() || floorReads.Load() != 0 || reads.Load() != 0 {
		t.Errorf("deltas off: line %s, %d floor read(s), %d snapshot read(s); want none of them",
			folds.all()[0].ChainStartFloor, floorReads.Load(), reads.Load())
	}
	sup.mu.Lock()
	memo := sup.foldedMarks["s"]
	sup.mu.Unlock()
	if !memo.readsFromKnown || !memo.readsFrom.Equal(refreshAt) {
		t.Errorf("memo reads from %s (known=%v), want the folder %s", memo.readsFrom, memo.readsFromKnown, refreshAt)
	}
	if got := covers.coverage(); len(got) != 1 || !got[0].publishedAt.Equal(refreshAt) {
		t.Errorf("coverage graded %+v, want the folder instant", got)
	}
}

// The fold's floor is the LIVE partitions and nothing else, like the gate's:
// one statement, no archive_state. The archive-extended floor would spare an
// archiving server a rewrite, but it jumps when a second source registers or
// archiving stops, and live partitions are shared by every source of the index,
// so they need no attribution.
func TestLiveFloorIn_readsOnlyTheLivePartitions(t *testing.T) {
	now := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	oldest := now.Add(-12 * time.Hour).Truncate(time.Hour)
	mock.ExpectQuery("information_schema.PARTITIONS").WithArgs("idx").
		WillReturnRows(sqlmock.NewRows([]string{"PARTITION_NAME", "PARTITION_DESCRIPTION",
			"PARTITION_ORDINAL_POSITION", "TABLE_ROWS"}).
			AddRow("p_"+oldest.Format("2006010215"), "", 1, 0).
			AddRow("p_"+now.Format("2006010215"), "", 2, 0).
			AddRow("p_future", "MAXVALUE", 3, 0))
	mock.ExpectQuery(".*").WillReturnError(errors.New("a second query was issued"))
	floor, known := liveFloorIn(context.Background(), db, "idx")
	if !known || !floor.Equal(oldest) {
		t.Errorf("floor = %s known=%v, want %s", floor, known, oldest)
	}
	if err := mock.ExpectationsWereMet(); err == nil {
		t.Error("a second query ran: the floor is the live partitions and nothing else")
	}
}
