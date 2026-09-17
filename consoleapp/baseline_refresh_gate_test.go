package consoleapp

import (
	"context"
	"errors"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// unchangedSince decides whether a refresh cycle runs at all, so every case is
// stated as the REASON behind its verdict rather than as a shape of the data:
// one is the skip this exists for, the rest are reasons a cycle must run.
func TestIndexMark_unchangedSince(t *testing.T) {
	base := indexMark{events: 100, schemaChanges: 7}
	cases := []struct {
		name string
		now  indexMark
		want bool
		why  string
	}{
		{"nothing indexed at all", indexMark{100, 7}, true,
			"no row event and no schema change since the fold: no table can have work"},
		{"a row event arrived", indexMark{101, 7}, false, "there is something to apply"},
		{"only a schema change arrived", indexMark{100, 8}, false,
			"a TRUNCATE writes no row event, and the cycle that refuses for it must still start"},
		{"both moved", indexMark{101, 8}, false, "obviously"},
		{"the event mark went BACKWARDS", indexMark{99, 7}, false,
			"rotation dropped every partition, or restore-index rebuilt the table: this is not " +
				"the index that was folded, so the gate cannot speak for it"},
		{"the schema-change mark went backwards", indexMark{100, 6}, false, "same reason"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := c.now.unchangedSince(base); got != c.want {
				t.Errorf("unchangedSince = %v, want %v — %s", got, c.want, c.why)
			}
		})
	}
}

// stubCoverage answers the gate's second question — is the published snapshot
// still inside the window the index can fold from — without an index.
func stubCoverage(t *testing.T, covered, known bool) *gateProbe {
	t.Helper()
	p := &gateProbe{}
	prev := snapshotStillCovered
	t.Cleanup(func() { snapshotStillCovered = prev })
	snapshotStillCovered = func(_ context.Context, dsn string, publishedAt, now time.Time) (bool, bool) {
		p.mu.Lock()
		p.coverages = append(p.coverages, coverageCall{dsn: dsn, publishedAt: publishedAt, now: now})
		p.mu.Unlock()
		return covered, known
	}
	return p
}

// Both questions have to answer yes for a cycle to be skipped, so every case
// here is stated as the REASON behind its verdict rather than as a shape of the
// data: one is the skip the feature exists for, the rest are reasons a cycle
// must run.
func TestRefreshCanSkip(t *testing.T) {
	folded := indexMark{events: 100, schemaChanges: 7}
	cases := []struct {
		name           string
		seed           bool
		now            indexMark
		covered, known bool
		wantSkip       bool
		why            string
		// wantsCoverageRead is whether this case should reach the SECOND
		// question at all. The verdict alone cannot say so: a zero-value memo
		// is disqualified by three separate comparisons at once.
		wantsCoverageRead bool
	}{
		{name: "never folded in this process", seed: false, now: folded, covered: true, known: true,
			why: "the daemon may have just restarted, and what the index holds relative to the " +
				"newest snapshot is unknown here"},
		{name: "something was indexed", seed: true, now: indexMark{101, 7}, covered: true, known: true,
			why: "there is something to apply"},
		{name: "nothing indexed and the backup is still covered", seed: true, now: folded,
			covered: true, known: true, wantSkip: true,
			wantsCoverageRead: true,
			why:               "this is the cycle the whole feature exists for"},
		{name: "nothing indexed but the backup is aging out", seed: true, now: folded,
			covered: false, known: true,
			wantsCoverageRead: true,
			why: "not republishing freezes the lower bound of the next fold's window, and the " +
				"index only keeps what rotation has not dropped: the fold that eventually runs " +
				"would refuse for a coverage gap on a server that was merely quiet"},
		// covered:true with known:false is a pair the real reader never
		// returns, and that is the point: it is the only way to prove the gate
		// consults `known` at all rather than riding on the verdict beside it.
		// With both false the case passes against a gate that ignores `known`
		// entirely, and it did.
		{name: "nothing indexed and coverage could not be read", seed: true, now: folded,
			covered: true, known: false,
			wantsCoverageRead: true,
			why: "doubt folds, the same asymmetry as an unreadable mark: a verdict that could not " +
				"be computed is not a verdict to act on"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			covers := stubCoverage(t, c.covered, c.known)
			sup := newBaselineSupervisor(context.Background(), t.TempDir(), baseline.DefaultLockMode)
			req := refreshRequest{ServerID: "s", ServerName: "s", IndexDSN: "d"}
			if c.seed {
				sup.foldedMarks["s"] = foldMemo{mark: folded, publishedAt: refreshAt,
					destination: refreshDestination(req), indexDSN: "d"}
			}
			if got := sup.refreshCanSkip(context.Background(), req, c.now, refreshAt); got != c.wantSkip {
				t.Errorf("refreshCanSkip = %v, want %v — %s", got, c.wantSkip, c.why)
			}
			// The cheap questions come first, and this is the only way to say
			// so. A memo that disqualifies on its own must not have cost an
			// index read, and the verdict alone cannot prove that: three
			// separate comparisons disqualify a zero-value memo at once.
			if asked := len(covers.coverage()); c.wantsCoverageRead != (asked > 0) {
				t.Errorf("the coverage question was asked %d time(s), want asked=%v — it opens "+
					"the index, so it is only worth asking about a cycle that would otherwise "+
					"be skipped", asked, c.wantsCoverageRead)
			}
		})
	}
}

// gateProbe records WHAT the gate asked its two oracles, not only what it did
// with the answers.
//
// Every stub in this file used to discard its arguments, and that turned out to
// be one hole seen from several angles: a mutation that replaced the coverage
// call with snapshotStillCovered(ctx, "", time.Time{}, time.Time{}) — every
// argument thrown away — kept the whole package green. Nothing pinned that the
// marks are read from THIS server's index, or that the age being graded is the
// memo's own published instant. The second one is the dangerous one: a refactor
// that re-stamped publishedAt on the skip path would leave the snapshot frozen
// on disk while its age reset every cycle, so the gate would grade ok forever
// and the backup would stop moving in silence — the exact outcome the coverage
// question exists to prevent.
type gateProbe struct {
	mu        sync.Mutex
	markDSNs  []string
	coverages []coverageCall
}

type coverageCall struct {
	dsn         string
	publishedAt time.Time
	now         time.Time
}

func (p *gateProbe) marks() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.markDSNs...)
}

func (p *gateProbe) coverage() []coverageCall {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]coverageCall(nil), p.coverages...)
}

// stubIndexMark makes the gate's two queries answer from memory, and records the
// index it was asked about.
func stubIndexMark(t *testing.T, mark *indexMark, known bool) *gateProbe {
	t.Helper()
	p := &gateProbe{}
	prev := readIndexMark
	t.Cleanup(func() { readIndexMark = prev })
	readIndexMark = func(_ context.Context, dsn string) (indexMark, bool) {
		p.mu.Lock()
		p.markDSNs = append(p.markDSNs, dsn)
		p.mu.Unlock()
		return *mark, known
	}
	return p
}

// stubIndexMarkSwitchable takes `known` by POINTER so a case can make the read
// start failing BETWEEN cycles. That ordering is what reaches the gate's `known`
// check through the REAL seeding path rather than by writing the memo by hand:
// see TestRunRefresh_anUnknownMarkAlwaysFolds.
//
// The pair it returns while `known` is false — a NON-ZERO mark that could not be
// read — is one the real reader never produces: readIndexMarkFromDB returns a
// zero mark on every failure path. It is deliberate, for the same reason as the
// impossible pair in TestRefreshCanSkip: a zero mark could match a zero memo by
// coincidence, and then the case would pass without the `known` check ever being
// consulted. This tests defence in depth through a shape production cannot reach.
func stubIndexMarkSwitchable(t *testing.T, mark *indexMark, known *bool) {
	t.Helper()
	prev := readIndexMark
	t.Cleanup(func() { readIndexMark = prev })
	readIndexMark = func(context.Context, string) (indexMark, bool) { return *mark, *known }
}

// countFolds replaces the fold with one that records how often it ran and
// writes nothing, so a cycle that reaches it is visible and a cycle that does
// not leaves no trace to confuse the next one.
func countFolds(t *testing.T, n *atomic.Int32) {
	t.Helper()
	prev := foldTables
	t.Cleanup(func() { foldTables = prev })
	foldTables = func(context.Context, reconstruct.FullTableConfig) (
		[]*reconstruct.TableReport, []reconstruct.TableFailure, error) {
		n.Add(1)
		return []*reconstruct.TableReport{{Schema: "shop", Table: "orders"}}, nil, nil
	}
}

// The gate end to end, on ONE supervisor across three cycles — the memo only
// means anything across cycles, so a per-cycle harness could not see it.
//
// This is the whole of #1689's last direction: while nothing is being indexed,
// the refresh does not run. Not "runs and withholds": does not run. Everything
// downstream of a cycle — the claimed directory, the pace sample, the run
// record, the status, the upload — is untouched precisely because none of it
// happens, which is why no consumer had to learn a third kind of outcome.
func TestRunRefresh_doesNotFoldWhileNothingIsIndexed(t *testing.T) {
	var folds atomic.Int32
	mark := indexMark{events: 100, schemaChanges: 7}
	marks := stubIndexMark(t, &mark, true)
	countFolds(t, &folds)
	stubBucketListing(t)
	covers := stubCoverage(t, true, true)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	sup := newBaselineSupervisor(ctx, t.TempDir(), baseline.DefaultLockMode)
	req := refreshRequest{ServerID: "s", ServerName: "s", IndexDSN: "d", BaselineDir: stageBaselineRoot(t)}
	run := func() {
		sup.refreshes["s"] = &console.BaselineStatus{State: "running"}
		sup.runRefresh(req, refreshAt, 0)
	}

	// 1. Nothing remembered yet, so this one folds whatever the mark says.
	run()
	if got := folds.Load(); got != 1 {
		t.Fatalf("the first cycle folded %d time(s), want 1: with no remembered mark the gate "+
			"cannot know what the index holds", got)
	}
	// What the fold left behind, and what the skipped cycle must not disturb.
	// The pace sample is seeded rather than produced, so the assertion below is
	// about the gate and not about how a stubbed fold happens to be timed.
	sup.mu.Lock()
	published := sup.foldedMarks["s"].publishedAt
	sup.refreshPaces["s"] = refreshPace{window: time.Hour, took: time.Minute, fold: time.Minute}
	sup.mu.Unlock()

	// 2. The index has not moved. This is the cycle the feature exists for.
	run()
	if got := folds.Load(); got != 1 {
		t.Errorf("folded %d time(s) in total, want still 1: nothing was indexed between the two "+
			"cycles, so the second had no table with anything to apply", got)
	}

	// The skipped cycle asked about THIS server's index, and graded THIS memo's
	// own published instant. Both were unpinned until a mutation that threw away
	// every argument to the coverage question survived the whole package.
	if got := marks.marks(); len(got) != 2 || got[0] != "d" || got[1] != "d" {
		t.Errorf("the marks were read from %q, want two reads of %q: a gate that reads a "+
			"different index than the one it is deciding about is answering about other data",
			got, req.IndexDSN)
	}
	if got := covers.coverage(); len(got) != 1 {
		t.Errorf("the coverage question was asked %d time(s), want exactly 1 — only a cycle "+
			"that would otherwise be skipped is worth an index read", len(got))
	} else if got[0].dsn != "d" || !got[0].publishedAt.Equal(published) || got[0].now.IsZero() {
		t.Errorf("coverage was asked %+v, want index %q and the memo's own published instant "+
			"%s graded against a real clock: grading anything else lets the age reset while "+
			"the snapshot on disk stays frozen", got[0], req.IndexDSN,
			published.Format(time.RFC3339))
	}

	// A skipped cycle leaves the memo exactly as it found it. Re-stamping
	// publishedAt would freeze the snapshot on disk while its age reset every
	// cycle: the gate would grade ok forever and the backup would stop moving
	// with nothing anywhere to show for it.
	sup.mu.Lock()
	after := sup.foldedMarks["s"]
	pace, hadPace := sup.refreshPaces["s"]
	sup.mu.Unlock()
	if !after.publishedAt.Equal(published) {
		t.Errorf("the skipped cycle moved the memo's published instant from %s to %s",
			published.Format(time.RFC3339), after.publishedAt.Format(time.RFC3339))
	}
	// And leaves the pace sample standing. takeRefreshPace reads and REMOVES in
	// one step, so a gate below it would consume the last real fold's sample on
	// every quiet cycle and silently disable the #1693 overrun reading on
	// exactly the servers this feature makes quiet.
	if !hadPace || pace.window != time.Hour {
		t.Errorf("pace sample after the skipped cycle = %+v (present=%v), want the seeded one "+
			"untouched", pace, hadPace)
	}

	// 3. One row event arrives. The gate must open again, or a backup that
	// stopped moving would never be noticed.
	mark.events++
	run()
	if got := folds.Load(); got != 2 {
		t.Errorf("folded %d time(s) in total, want 2: a row event was indexed after the last fold", got)
	}
}

// A refusal must NOT be memoized. A capture gap or a schema change does not
// move the index, so remembering a refused cycle would silence the retry and
// the refusal with it, and the operator's only standing signal would vanish.
func TestRunRefresh_arefusedCycleIsNotRemembered(t *testing.T) {
	var folds atomic.Int32
	mark := indexMark{events: 100, schemaChanges: 7}
	stubIndexMark(t, &mark, true)
	stubBucketListing(t)
	stubCoverage(t, true, true)
	prev := foldTables
	t.Cleanup(func() { foldTables = prev })
	foldTables = func(context.Context, reconstruct.FullTableConfig) (
		[]*reconstruct.TableReport, []reconstruct.TableFailure, error) {
		folds.Add(1)
		return nil, []reconstruct.TableFailure{{Schema: "shop", Table: "orders", Err: reconstruct.ErrCaptureGap}},
			reconstruct.ErrCaptureGap
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	sup := newBaselineSupervisor(ctx, t.TempDir(), baseline.DefaultLockMode)
	req := refreshRequest{ServerID: "s", ServerName: "s", IndexDSN: "d", BaselineDir: stageBaselineRoot(t)}
	for range 2 {
		sup.refreshes["s"] = &console.BaselineStatus{State: "running"}
		sup.runRefresh(req, refreshAt, 0)
	}
	if got := folds.Load(); got != 2 {
		t.Errorf("folded %d time(s), want 2: a refused cycle leaves the mark alone so the next one "+
			"retries and re-reports, exactly as before this gate existed", got)
	}
}

// The gate must not skip on a guess. Every way of not knowing — the index will
// not open, a query fails, the table is empty — has to fold.
//
// The memo is SEEDED FIRST, by a cycle whose read succeeded, and only then does
// the read start failing. Without that ordering this proves nothing: with no
// memo every cycle folds anyway, so a second fold says nothing about whether the
// gate consulted `known`. The first version of this test was written that way
// and a mutation dropping the `known` check SURVIVED it.
func TestRunRefresh_anUnknownMarkAlwaysFolds(t *testing.T) {
	var folds atomic.Int32
	mark := indexMark{events: 100, schemaChanges: 7}
	known := true
	stubIndexMarkSwitchable(t, &mark, &known)
	countFolds(t, &folds)
	stubBucketListing(t)
	stubCoverage(t, true, true)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	sup := newBaselineSupervisor(ctx, t.TempDir(), baseline.DefaultLockMode)
	req := refreshRequest{ServerID: "s", ServerName: "s", IndexDSN: "d", BaselineDir: stageBaselineRoot(t)}
	run := func() {
		sup.refreshes["s"] = &console.BaselineStatus{State: "running"}
		sup.runRefresh(req, refreshAt, 0)
	}

	run() // succeeds, and memoizes this exact mark
	if got := folds.Load(); got != 1 {
		t.Fatalf("the seeding cycle folded %d time(s), want 1", got)
	}
	// The same mark, now unreadable. The memo matches it exactly, which is the
	// trap: "the value I could not read happens to equal the one I stored" is
	// not evidence that nothing changed.
	known = false
	run()
	if got := folds.Load(); got != 2 {
		t.Errorf("folded %d time(s), want 2: the gate skipped on a mark it could not read, "+
			"which stops the backup silently", got)
	}
}

// The gate returns EARLY, and returning early from a refresh cycle is the one
// thing runRefresh could not do before #1689: TriggerRefresh claims the
// server's refresh slot as "running" before the cycle starts, and that claim is
// the single-flight every baseline job on that server shares. A skipped cycle
// that leaves it standing stops the periodic refresh, the manual backup, the
// point-in-time restore and the SQL export, for the life of the process.
//
// Driven through TriggerRefresh on purpose. Every other test here that drives
// the fold directly calls runRefresh and re-seeds the status slot first, which is
// convenient and hides exactly this: the seeding overwrites the wedged slot, so
// the second cycle starts clean no matter what the first one left behind. This
// one takes the daemon's own entry point and never touches the slot itself.
func TestTriggerRefresh_aSkippedCycleLeavesTheServerFree(t *testing.T) {
	var folds atomic.Int32
	mark := indexMark{events: 100, schemaChanges: 7}
	stubIndexMark(t, &mark, true)
	countFolds(t, &folds)
	stubBucketListing(t)
	stubCoverage(t, true, true)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	sup := newBaselineSupervisor(ctx, t.TempDir(), baseline.DefaultLockMode)
	req := refreshRequest{ServerID: "s", ServerName: "s", IndexDSN: "d", BaselineDir: stageBaselineRoot(t)}
	cycle := func(t *testing.T) console.BaselineStatus {
		t.Helper()
		if _, err := sup.TriggerRefresh(req, time.Minute); err != nil {
			t.Fatalf("TriggerRefresh: %v — the previous cycle left this server claimed", err)
		}
		return waitForTerminalState(t, func() console.BaselineStatus { return sup.RefreshStatus("s") })
	}

	published := cycle(t)
	if folds.Load() != 1 || published.State != "succeeded" {
		t.Fatalf("the first cycle folded %d time(s) and ended %q, want 1 and \"succeeded\"",
			folds.Load(), published.State)
	}

	// The index has not moved, so this cycle is skipped. The assertions below
	// are all about what it must NOT have left behind.
	skipped := cycle(t)
	if folds.Load() != 1 {
		t.Fatalf("folded %d time(s), want still 1", folds.Load())
	}
	if skipped != published {
		t.Errorf("the skipped cycle changed the reported status:\n got %+v\nwant %+v\n"+
			"a cycle that did not run has nothing to report, and any new value here is a "+
			"third kind of outcome for every surface that reads it", skipped, published)
	}

	// The load-bearing one. A third trigger is refused only if the second left
	// the server claimed, and THAT is the shape of the outage: the operator
	// sees "a baseline is already running" forever, which reads as a refresh
	// that is taking too long rather than one that already returned.
	if _, err := sup.TriggerRefresh(req, time.Minute); err != nil {
		t.Fatalf("a third TriggerRefresh was refused with %v: the skipped cycle never released "+
			"the single-flight, so this server's backup, restore and SQL export are wedged too "+
			"until the daemon restarts", err)
	}
	waitForTerminalState(t, func() console.BaselineStatus { return sup.RefreshStatus("s") })
	sup.mu.Lock()
	busy := sup.busyLocked("s")
	sup.mu.Unlock()
	if busy {
		t.Error("the server is still busy after every cycle finished")
	}
}

// A skipped cycle on a server whose slot was never claimed must still leave it
// unclaimed. The fallback matters because the invariant is about what is left
// standing, not about bookkeeping: any path that reaches the release must end
// with no "running" in the slot, including one that did not come through
// TriggerRefresh.
func TestReleaseRefreshSlot_withNothingRememberedClearsTheSlot(t *testing.T) {
	sup := newBaselineSupervisor(context.Background(), t.TempDir(), baseline.DefaultLockMode)
	sup.refreshes["s"] = &console.BaselineStatus{State: "running"}
	sup.releaseRefreshSlot("s")
	if got := sup.RefreshStatus("s"); got.State != "idle" {
		t.Errorf("status is %q, want \"idle\": a server with no remembered previous run reads as "+
			"one that has never refreshed", got.State)
	}
}

const (
	eventsMarkQuery  = "SELECT MAX(event_id) FROM binlog_events"
	changesMarkQuery = "SELECT MAX(id) FROM schema_changes"
)

// readIndexMarkFrom is where "I cannot tell" is decided, and that verdict has to
// mean a FAILED READ and nothing else. An empty table is not doubt: it is a
// definite answer that says the table holds no rows.
//
// The case this exists for is the second one. schema_changes stays empty for as
// long as the source runs no DDL, which for plenty of sources is forever —
// reading that as "I cannot tell" made the gate answer "I cannot tell" on every
// cycle of those installs, permanently, and the whole feature did nothing while
// logging nothing about it.
func TestReadIndexMarkFrom(t *testing.T) {
	cases := []struct {
		name      string
		setup     func(sqlmock.Sqlmock)
		wantMark  indexMark
		wantKnown bool
		why       string
		// stopsBeforeChangesQuery inverts the expectations check: the case
		// registers an answer for the SECOND query and asserts it was never
		// asked. See the note on the negative-mark case for why registering a
		// query that must not run is the point.
		stopsBeforeChangesQuery bool
	}{
		{
			name: "a source that has never run a DDL",
			setup: func(m sqlmock.Sqlmock) {
				m.ExpectQuery(regexp.QuoteMeta(eventsMarkQuery)).
					WillReturnRows(sqlmock.NewRows([]string{"m"}).AddRow(4200))
				m.ExpectQuery(regexp.QuoteMeta(changesMarkQuery)).
					WillReturnRows(sqlmock.NewRows([]string{"m"}).AddRow(nil))
			},
			wantMark: indexMark{events: 4200, schemaChanges: 0}, wantKnown: true,
			why: "an empty schema_changes is the normal, permanent state of a source that runs no " +
				"DDL, and a gate that cannot read it is a gate that never engages",
		},
		{
			name: "an index that has captured nothing yet",
			setup: func(m sqlmock.Sqlmock) {
				m.ExpectQuery(regexp.QuoteMeta(eventsMarkQuery)).
					WillReturnRows(sqlmock.NewRows([]string{"m"}).AddRow(nil))
				m.ExpectQuery(regexp.QuoteMeta(changesMarkQuery)).
					WillReturnRows(sqlmock.NewRows([]string{"m"}).AddRow(nil))
			},
			wantMark: indexMark{}, wantKnown: true,
			why: "no rows means nothing a fold could apply, and the first event ever captured " +
				"lifts the mark off zero",
		},
		{
			name: "both tables have rows",
			setup: func(m sqlmock.Sqlmock) {
				m.ExpectQuery(regexp.QuoteMeta(eventsMarkQuery)).
					WillReturnRows(sqlmock.NewRows([]string{"m"}).AddRow(4200))
				m.ExpectQuery(regexp.QuoteMeta(changesMarkQuery)).
					WillReturnRows(sqlmock.NewRows([]string{"m"}).AddRow(9))
			},
			wantMark: indexMark{events: 4200, schemaChanges: 9}, wantKnown: true,
			why: "the ordinary case",
		},
		{
			name: "the events query fails",
			setup: func(m sqlmock.Sqlmock) {
				m.ExpectQuery(regexp.QuoteMeta(eventsMarkQuery)).WillReturnError(errors.New("gone away"))
			},
			wantMark: indexMark{}, wantKnown: false,
			why: "a read that did not come back is the only real doubt, and doubt folds",
		},
		{
			name: "the schema-changes query fails",
			setup: func(m sqlmock.Sqlmock) {
				m.ExpectQuery(regexp.QuoteMeta(eventsMarkQuery)).
					WillReturnRows(sqlmock.NewRows([]string{"m"}).AddRow(4200))
				m.ExpectQuery(regexp.QuoteMeta(changesMarkQuery)).WillReturnError(errors.New("gone away"))
			},
			wantMark: indexMark{}, wantKnown: false,
			why: "half a mark is not a mark: the events half alone cannot see a TRUNCATE",
		},
		{
			name: "a negative mark",
			setup: func(m sqlmock.Sqlmock) {
				m.ExpectQuery(regexp.QuoteMeta(eventsMarkQuery)).
					WillReturnRows(sqlmock.NewRows([]string{"m"}).AddRow(-1))
				// Registered even though a negative first mark stops the read
				// before it: without it, dropping the negative check lets the
				// second query run against a mock that has no answer for it,
				// which errors and produces the SAME verdict for an unrelated
				// reason. The case passed against a mutant that removed the
				// check. sqlmock reports an unmatched expectation, so leaving
				// it registered is what makes the short-circuit an assertion
				// rather than an accident.
				m.ExpectQuery(regexp.QuoteMeta(changesMarkQuery)).
					WillReturnRows(sqlmock.NewRows([]string{"m"}).AddRow(9))
			},
			wantMark: indexMark{}, wantKnown: false, stopsBeforeChangesQuery: true,
			why: "both columns are UNSIGNED, so this is not a value that can be compared against " +
				"a later one",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("sqlmock.New: %v", err)
			}
			defer db.Close()
			c.setup(mock)
			mark, known := readIndexMarkFrom(context.Background(), db)
			if mark != c.wantMark || known != c.wantKnown {
				t.Errorf("readIndexMarkFrom = %+v, %v; want %+v, %v — %s",
					mark, known, c.wantMark, c.wantKnown, c.why)
			}
			if c.stopsBeforeChangesQuery {
				if err := mock.ExpectationsWereMet(); err == nil {
					t.Error("the schema-changes query ran: a first mark that cannot be compared " +
						"has already decided the answer, and asking again only risks deciding it " +
						"for a different reason")
				}
				return
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("unmet query expectations: %v", err)
			}
		})
	}
}

// The keepalive, end to end: a server where nothing is being indexed still gets
// folded once its published backup starts aging out of the window the index
// covers, and the fold re-anchors it.
//
// This is the half of the gate that is easy to leave out, because leaving it
// out looks like it works: the skipping is visible immediately and the cost
// only lands a retention period later, as a fold that refuses for a coverage
// gap on a server that did nothing wrong. The remedy at that point is a fresh
// read of the whole production database.
func TestRunRefresh_foldsToReanchorAnAgingBackup(t *testing.T) {
	var folds atomic.Int32
	mark := indexMark{events: 100, schemaChanges: 7}
	stubIndexMark(t, &mark, true)
	countFolds(t, &folds)
	stubBucketListing(t)
	covered := true
	prevCov := snapshotStillCovered
	t.Cleanup(func() { snapshotStillCovered = prevCov })
	snapshotStillCovered = func(context.Context, string, time.Time, time.Time) (bool, bool) {
		return covered, true
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	sup := newBaselineSupervisor(ctx, t.TempDir(), baseline.DefaultLockMode)
	req := refreshRequest{ServerID: "s", ServerName: "s", IndexDSN: "d", BaselineDir: stageBaselineRoot(t)}
	// Each cycle publishes at its OWN instant. With one shared instant the memo
	// assertion at the end cannot tell the re-anchoring cycle's memo from the
	// first cycle's, which is exactly what it claims to prove.
	run := func(at time.Time) {
		sup.refreshes["s"] = &console.BaselineStatus{State: "running"}
		sup.runRefresh(req, at, 0)
	}
	reanchoredAt := refreshAt.Add(2 * time.Hour)

	run(refreshAt)                    // the first cycle folds and publishes, nothing remembered yet
	run(refreshAt.Add(1 * time.Hour)) // quiet and still covered: skipped
	if got := folds.Load(); got != 1 {
		t.Fatalf("folded %d time(s), want 1 while the backup is still covered", got)
	}

	// Time passes on a server nobody is writing to. Nothing has been indexed —
	// the mark is untouched on purpose — but the published backup is now old
	// relative to what the index still holds.
	covered = false
	run(reanchoredAt)
	if got := folds.Load(); got != 2 {
		t.Errorf("folded %d time(s), want 2: with the index unchanged this cycle had nothing to "+
			"apply, and it still had to run — republishing is what keeps the next fold's window "+
			"inside the hours the index has not rotated away", got)
	}
	// And it must be remembered as a fold, or the following cycles re-fold
	// forever once the backup has aged once.
	sup.mu.Lock()
	memo, seen := sup.foldedMarks["s"]
	sup.mu.Unlock()
	if !seen || memo.publishedAt != reanchoredAt {
		t.Errorf("memo = %+v, seen=%v; want the instant THIS cycle published (%s): the re-anchoring "+
			"fold is a fold like any other, and a memo still holding the first cycle's instant "+
			"would re-anchor again every cycle forever", memo, seen, reanchoredAt.Format(time.RFC3339))
	}
}

// The threshold the gate keeps a quiet server's backup above. Every case is
// stated as a real window: a retention floor, and a backup of some age inside or
// outside it.
//
// The floor here is the LIVE partition floor, never the archive-extended one the
// rest of the product grades against — see snapshotStillCovered for why. The
// last two cases are the ones that choice exists for.
func TestSnapshotCoveredBy(t *testing.T) {
	now := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	floor30d := now.AddDate(0, 0, -30)
	cases := []struct {
		name        string
		liveFloor   time.Time
		publishedAt time.Time
		want        bool
		why         string
	}{
		{"published an hour ago", floor30d, now.Add(-time.Hour), true,
			"the ordinary quiet server: there is a month of slack, so skipping costs nothing"},
		{"published 20 days into a 30-day window", floor30d, now.AddDate(0, 0, -20), true,
			"still inside the product's own OK band"},
		{"published 24 days into a 30-day window", floor30d, now.AddDate(0, 0, -24), false,
			"0.8 of the span, which on the default 30-day retention is the real bound on how " +
				"long a quiet server goes without re-anchoring: 24 days, not one cycle"},
		{"published before the floor", floor30d, now.AddDate(0, 0, -31), false,
			"broken: the hours between the backup and the floor are gone"},
		{"no partitions to grade against", time.Time{}, now.Add(-time.Hour), false,
			"a zero floor grades unknown, and unknown is not permission to skip"},
		{"nothing was ever published", floor30d, time.Time{}, false,
			"a memo with no published instant cannot vouch for anything"},
		{"a young install, folded an hour ago", now.AddDate(0, 0, -2), now.Add(-time.Hour), true,
			"the span is the install's age, so the aging band is narrow — but the backup is an " +
				"hour old inside a two-day window, so the gate still engages. The bootstrap " +
				"artifact staleness.go warns about does not make this feature inert"},
		{"a long-lived install whose archives would have vouched for it", floor30d,
			now.AddDate(0, 0, -400), false,
			"graded against the live floor this is broken and folds. Against the ARCHIVE-extended " +
				"floor of a 400-day install it would grade ok and keep skipping for about four " +
				"years, and then jump straight to broken the day archiving stops or a second " +
				"server is registered — never passing through the aging verdict this waits for"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := snapshotCoveredBy(c.liveFloor, c.publishedAt, now); got != c.want {
				t.Errorf("snapshotCoveredBy = %v, want %v — %s", got, c.want, c.why)
			}
		})
	}
}

// A memo speaks only for the destination its fold reached. Two producers write
// it: the daemon-wide interval loop publishes locally and never uploads, and the
// per-server schedule uploads to that server's bucket. Without this, the local
// one satisfies the gate for the scheduled cycle whose whole job was the bucket.
func TestRefreshCanSkip_aMemoDoesNotCrossDestinations(t *testing.T) {
	stubCoverage(t, true, true)
	mark := indexMark{events: 100, schemaChanges: 7}
	local := refreshRequest{ServerID: "s", ServerName: "s", IndexDSN: "d", BaselineDir: "/var/backups"}
	sup := newBaselineSupervisor(context.Background(), t.TempDir(), baseline.DefaultLockMode)
	sup.foldedMarks["s"] = foldMemo{mark: mark, publishedAt: refreshAt,
		destination: refreshDestination(local), indexDSN: "d"}

	if !sup.refreshCanSkip(context.Background(), local, mark, refreshAt) {
		t.Fatal("the same destination and the same marks must still skip")
	}
	toBucket := local
	toBucket.BaselineS3 = "s3://acme-backups/shop"
	if sup.refreshCanSkip(context.Background(), toBucket, mark, refreshAt) {
		t.Error("skipped a cycle that uploads on the strength of a fold that only wrote local " +
			"disk: the bucket never receives a copy, and the schedule reports the backup as " +
			"up to date")
	}
	moved := local
	moved.BaselineDir = "/mnt/new-backups"
	if sup.refreshCanSkip(context.Background(), moved, mark, refreshAt) {
		t.Error("skipped after the backup directory was re-pointed: nothing is ever written to " +
			"the new location")
	}
}

// A memo speaks for the index it was read from and no other. Re-pointing a
// server at a different index in the settings panel leaves the old memo behind,
// and the marks are ordinary auto-increment ids: two unrelated indexes holding
// the same pair is a coincidence, not evidence that nothing was written.
func TestRefreshCanSkip_aMemoDoesNotCrossIndexes(t *testing.T) {
	stubCoverage(t, true, true)
	mark := indexMark{events: 100, schemaChanges: 7}
	sup := newBaselineSupervisor(context.Background(), t.TempDir(), baseline.DefaultLockMode)
	same := refreshRequest{ServerID: "s", ServerName: "s", IndexDSN: "first"}
	sup.foldedMarks["s"] = foldMemo{mark: mark, publishedAt: refreshAt,
		destination: refreshDestination(same), indexDSN: "first"}

	if !sup.refreshCanSkip(context.Background(), same, mark, refreshAt) {
		t.Fatal("the same index and the same marks must still skip")
	}
	moved := refreshRequest{ServerID: "s", ServerName: "s", IndexDSN: "second"}
	if sup.refreshCanSkip(context.Background(), moved, mark, refreshAt) {
		t.Error("skipped on a memo read from a different index: the matching marks say nothing " +
			"about what this index holds")
	}
}

// A scheduled period the gate skipped has to say why it produced nothing.
//
// The schedule dispatches a job and then reads the outcome off the refresh
// slot. A gated cycle restores that slot instead of writing an outcome — which
// is the point — so to the watcher it looks exactly like a job whose end nobody
// saw, and the skip it files blames another backup job for taking the server.
// That never happened, and on a quiet server it is what the operator would read
// every single period.
func TestBackupScheduler_aGatedPeriodNamesItsOwnReason(t *testing.T) {
	var folds atomic.Int32
	mark := indexMark{events: 100, schemaChanges: 7}
	stubIndexMark(t, &mark, true)
	stubCoverage(t, true, true)
	countFolds(t, &folds)
	stubBucketListing(t)

	b, reg, sup := newScheduleFixture(t, true)
	_ = sup
	e := addScheduled(t, reg, true)

	fireAt(b, time.Date(2026, 8, 28, 9, 0, 5, 0, time.UTC))
	waitTerminal(t, b, e.ID)
	if got := folds.Load(); got != 1 {
		t.Fatalf("the first scheduled period folded %d time(s), want 1", got)
	}

	// The stamps that tell one cycle from another have one-second resolution,
	// and both periods of this test run inside the same wall second — which a
	// daemon never does, since a schedule period is minutes at the very least.
	// Waiting for the second to turn makes the two cycles as distinguishable
	// here as they already are in production.
	for s := nowStamp(); nowStamp() == s; {
		time.Sleep(20 * time.Millisecond)
	}

	// A later slot, with nothing indexed in between.
	b.tick(context.Background(), time.Date(2026, 8, 28, 11, 0, 5, 0, time.UTC))
	deadline := time.Now().Add(10 * time.Second)
	for b.ScheduleState(e.ID).LastSkippedAt == "" && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	// Joined before asserting. noteSkip sets LastSkippedAt BEFORE the watcher
	// appends the skip to the durable history, and that history lives in a
	// TempDir this test is about to remove — so the poll above can return while
	// a goroutine is still writing into it. Observed once in twenty runs as a
	// "directory not empty" cleanup failure.
	b.watchers.Wait()
	st := b.ScheduleState(e.ID)
	if got := folds.Load(); got != 1 {
		t.Fatalf("folded %d time(s), want still 1: nothing was indexed between the two periods", got)
	}
	if st.LastFallbackAt != "" {
		t.Fatalf("a full read of production was armed for a period that had nothing to back up: %+v", st)
	}
	if !strings.Contains(st.LastSkipReason, "nothing had been indexed") {
		t.Errorf("skip reason = %q, want the gate's own reason — a period that produced nothing "+
			"because there was nothing to produce must not be reported as one another job "+
			"interfered with", st.LastSkipReason)
	}
}

// stubGateReads makes both of the gate's index reads answer without an index:
// the marks are readable and the published snapshot is covered. For tests that
// are about something else and whose fixture DSN cannot be opened.
func stubGateReads(t *testing.T) {
	t.Helper()
	mark := indexMark{events: 1, schemaChanges: 1}
	stubIndexMark(t, &mark, true)
	stubCoverage(t, true, true)
}

// The coverage question must read the LIVE partitions and nothing else.
//
// Reaching for status.OldestDeltaFromDB here is the mistake this pins: that
// floor is extended backwards over contiguous archives, which is right for "how
// far back can this index still restore" and wrong for "how long may this server
// safely do nothing". It makes the span the install's whole age, and it can jump
// forward by that whole age in one read when archiving stops or a second server
// is registered — carrying a snapshot from ok straight to broken without ever
// passing through the aging verdict this gate waits for.
//
// Pinned by registering a SECOND, catch-all expectation and asserting it stays
// UNMET. sqlmock reports unmet expectations and never a surplus query, so
// "expect only the partitions query" proves nothing on its own: a best-effort
// archive read that swallows its own error — which is how such code is naturally
// written — would keep this green while production quietly changed floors.
// Exactly one statement is the property, so exactly one is what this asserts.
func TestSnapshotCoveredIn_readsOnlyTheLivePartitions(t *testing.T) {
	now := time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	mock.ExpectQuery("information_schema.PARTITIONS").WithArgs("idx").
		WillReturnRows(sqlmock.NewRows([]string{"PARTITION_NAME", "PARTITION_DESCRIPTION",
			"PARTITION_ORDINAL_POSITION", "TABLE_ROWS"}).
			AddRow("p_"+now.AddDate(0, 0, -30).Format("2006010215"), "", 1, 0).
			AddRow("p_"+now.Format("2006010215"), "", 2, 0).
			AddRow("p_future", "MAXVALUE", 3, 0))

	mock.ExpectQuery(".*").WillReturnError(errors.New("a second query was issued"))

	covered, known := snapshotCoveredIn(context.Background(), db, "idx", now.Add(-time.Hour), now)
	if !known || !covered {
		t.Errorf("covered=%v known=%v, want true/true: the backup is an hour old inside a "+
			"30-day window", covered, known)
	}
	if err := mock.ExpectationsWereMet(); err == nil {
		t.Error("a second query ran: the coverage question must read the live partitions and " +
			"nothing else, whether or not the extra read's error would have propagated")
	}
}

// And the read fails toward folding, like every other read this gate does.
func TestSnapshotCoveredIn_anUnreadablePartitionListFolds(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	mock.ExpectQuery("information_schema.PARTITIONS").WillReturnError(errors.New("gone away"))
	if covered, known := snapshotCoveredIn(context.Background(), db, "idx",
		time.Now().Add(-time.Hour), time.Now()); known || covered {
		t.Errorf("covered=%v known=%v, want false/false", covered, known)
	}
}
