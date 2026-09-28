package consoleapp

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/notify"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// The follow-ups from the review of the refresh gate (#1705). Each test below
// pins something the gate's comments state and nothing held in place.

// seedQuietServer leaves a supervisor exactly as a clean fold leaves it: a memo
// that matches the mark, the index and the destination of req. The next cycle
// for req is one the gate skips.
func seedQuietServer(sup *baselineSupervisor, req refreshRequest, mark indexMark) {
	sup.mu.Lock()
	defer sup.mu.Unlock()
	sup.foldedMarks[req.ServerID] = foldMemo{mark: mark, publishedAt: refreshAt,
		destination: refreshDestination(req), indexDSN: req.IndexDSN}
}

// Point 1. The instant a cycle folds to is taken AFTER the index marks are
// read, never before.
//
// The marks are what the gate remembers as done. Anything they count has to be
// inside the window the fold looked at, or it is remembered as folded without
// having been. A TRUNCATE is the case that matters: it writes a schema_changes
// row and no row event, so the refusal the fold raises for it is the only
// notice the operator gets, and on a server where nothing else is indexed that
// notice would wait until the coverage question opened the gate.
//
// The statement here is recorded while the marks are being read, which is the
// sliver the old order left open: the cycle was claimed before the read, so its
// window ended before the statement and the marks still counted it.
func TestTriggerRefresh_aStatementTheMarksCountIsInsideTheFoldWindow(t *testing.T) {
	var (
		mu          sync.Mutex
		truncatedAt time.Time
		foldedTo    time.Time
	)
	mark := indexMark{events: 100, schemaChanges: 8}
	prevMark := readIndexMark
	t.Cleanup(func() { readIndexMark = prevMark })
	readIndexMark = func(context.Context, string) (indexMark, bool) {
		// The pauses keep the three instants apart on a coarse clock: the
		// claim, the statement, and the return of this read.
		time.Sleep(3 * time.Millisecond)
		mu.Lock()
		truncatedAt = time.Now().UTC()
		mu.Unlock()
		time.Sleep(3 * time.Millisecond)
		return mark, true
	}
	prevFold := foldTables
	t.Cleanup(func() { foldTables = prevFold })
	foldTables = func(_ context.Context, cfg reconstruct.FullTableConfig) (
		[]*reconstruct.TableReport, []reconstruct.TableFailure, error) {
		mu.Lock()
		defer mu.Unlock()
		foldedTo = cfg.At
		// What reconstruct.CheckDestructiveDDL decides: a destructive statement
		// detected at or before the instant the fold targets refuses the table.
		if !truncatedAt.After(cfg.At) {
			err := fmt.Errorf("%w: TRUNCATE TABLE on shop.orders", reconstruct.ErrDestructiveDDL)
			return nil, []reconstruct.TableFailure{{Schema: "shop", Table: "orders", Err: err}}, err
		}
		return []*reconstruct.TableReport{{Schema: "shop", Table: "orders"}}, nil, nil
	}
	stubBucketListing(t)
	stubCoverage(t, true, true)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	sup := newBaselineSupervisor(ctx, t.TempDir(), baseline.DefaultLockMode)
	req := refreshRequest{ServerID: "s", ServerName: "s", IndexDSN: "d", BaselineDir: stageBaselineRoot(t)}
	if _, err := sup.TriggerRefresh(req, time.Minute); err != nil {
		t.Fatalf("TriggerRefresh: %v", err)
	}
	st := waitForTerminalState(t, func() console.BaselineStatus { return sup.RefreshStatus("s") })

	mu.Lock()
	stmt, to := truncatedAt, foldedTo
	mu.Unlock()
	if to.IsZero() {
		t.Fatalf("the cycle never reached the fold: %+v", st)
	}
	if to.Before(stmt) {
		t.Errorf("the fold targeted %s and the statement the marks count was recorded at %s: the "+
			"cycle's instant was taken before the marks were read, so the statement is outside "+
			"the window and inside the memo",
			to.Format(time.RFC3339Nano), stmt.Format(time.RFC3339Nano))
	}
	if st.State != "failed" || !strings.Contains(st.LastError, reconstruct.ErrDestructiveDDL.Error()) {
		t.Errorf("the cycle ended %q (%q), want it refused for the TRUNCATE in THIS cycle: "+
			"published, the notice waits for the next fold, and on a quiet server that is "+
			"most of a retention period away", st.State, st.LastError)
	}
	sup.mu.Lock()
	_, remembered := sup.foldedMarks["s"]
	sup.mu.Unlock()
	if remembered {
		t.Error("the marks were remembered as folded, and they count a statement the fold never saw")
	}
}

// The other side of the same change. The status names the snapshot by its
// instant, the schedule reads that name off the status once a run published,
// and the directory is named from the same instant. Taking the instant later
// must not let the three drift apart.
func TestTriggerRefresh_theStatusNamesTheSnapshotItPublished(t *testing.T) {
	var (
		mu       sync.Mutex
		foldedTo time.Time
		wroteTo  string
	)
	mark := indexMark{events: 100, schemaChanges: 7}
	stubIndexMark(t, &mark, true)
	stubBucketListing(t)
	stubCoverage(t, true, true)
	prevFold := foldTables
	t.Cleanup(func() { foldTables = prevFold })
	foldTables = func(_ context.Context, cfg reconstruct.FullTableConfig) (
		[]*reconstruct.TableReport, []reconstruct.TableFailure, error) {
		mu.Lock()
		defer mu.Unlock()
		foldedTo = cfg.At
		wroteTo = filepath.Join(cfg.OutputDir, reconstruct.SnapshotDirName(cfg.At))
		return []*reconstruct.TableReport{{Schema: "shop", Table: "orders"}}, nil, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	sup := newBaselineSupervisor(ctx, t.TempDir(), baseline.DefaultLockMode)
	req := refreshRequest{ServerID: "s", ServerName: "s", IndexDSN: "d", BaselineDir: stageBaselineRoot(t)}
	before := time.Now().UTC().Truncate(time.Second)
	if _, err := sup.TriggerRefresh(req, time.Minute); err != nil {
		t.Fatalf("TriggerRefresh: %v", err)
	}
	st := waitForTerminalState(t, func() console.BaselineStatus { return sup.RefreshStatus("s") })
	if st.State != "succeeded" {
		t.Fatalf("the cycle ended %q (%q), want succeeded", st.State, st.LastError)
	}
	mu.Lock()
	to, dir := foldedTo, wroteTo
	mu.Unlock()

	if to.IsZero() || to.Before(before) || to.Location() != time.UTC {
		t.Fatalf("the fold targeted %v, want a real UTC instant at or after %v", to, before)
	}
	if want := to.Format(time.RFC3339); st.At != want {
		t.Errorf("status names the snapshot at %q and the fold targeted %q: the schedule would "+
			"report a snapshot that is not the one on disk", st.At, want)
	}
	// From the status back to the disk, which is the direction a reader goes:
	// the instant the status names has to be the directory the fold wrote.
	named, err := time.Parse(time.RFC3339, st.At)
	if err != nil {
		t.Fatalf("status at = %q: %v", st.At, err)
	}
	if want := refreshSnapshotDir(req, named); dir != want {
		t.Errorf("the fold wrote %q and the status names %q", dir, want)
	}
	sup.mu.Lock()
	memo := sup.foldedMarks["s"]
	sup.mu.Unlock()
	if !memo.publishedAt.Equal(to) {
		t.Errorf("the memo dates the snapshot %s and the fold targeted %s: the coverage question "+
			"would grade an instant no directory carries", memo.publishedAt, to)
	}
}

// Point 2, first ordering. The staging sweep runs ABOVE the gate.
//
// A staging directory a killed daemon left behind is skipped by every listing,
// so nothing else will ever mention it. On a server whose cycles all skip, a
// sweep below the gate never runs and the directory stays for good.
func TestRunRefresh_aSkippedCycleStillSweepsTheStagingDirectory(t *testing.T) {
	var folds atomic.Int32
	mark := indexMark{events: 100, schemaChanges: 7}
	stubIndexMark(t, &mark, true)
	stubCoverage(t, true, true)
	stubBucketListing(t)
	countFolds(t, &folds)

	root := stageBaselineRoot(t)
	leftover := filepath.Join(root, ".2026-08-27T10-00-00Z.discarding")
	writeSnapshotFiles(t, leftover)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	sup := newBaselineSupervisor(ctx, t.TempDir(), baseline.DefaultLockMode)
	req := refreshRequest{ServerID: "s", ServerName: "s", IndexDSN: "d", BaselineDir: root}
	seedQuietServer(sup, req, mark)
	sup.refreshes["s"] = &console.BaselineStatus{State: "running"}
	sup.runRefresh(req, refreshAt, 0)

	if got := folds.Load(); got != 0 {
		t.Fatalf("the cycle folded %d time(s), want 0: this test is about a cycle the gate skips, "+
			"and a cycle that folds sweeps wherever the sweep sits", got)
	}
	if _, err := os.Stat(leftover); !os.IsNotExist(err) {
		t.Errorf("the staging directory survived a skipped cycle (stat: %v): on a quiet server "+
			"every cycle skips, so nothing would ever reclaim it", err)
	}
}

// Point 2, second ordering. The gate lets go of the supervisor's lock BEFORE
// it asks the index how far it still reaches.
//
// That lock is the one every baseline job takes to start. The coverage read
// opens the index, and an index that hangs would hold the lock for as long as
// it hangs: no snapshot, no restore and no SQL export on ANY server of the
// process. Here the read never answers, and a job for another server has to
// start anyway.
func TestRefreshCanSkip_aHungCoverageReadBlocksNoOtherJob(t *testing.T) {
	var folds atomic.Int32
	mark := indexMark{events: 100, schemaChanges: 7}
	stubIndexMark(t, &mark, true)
	stubBucketListing(t)
	countFolds(t, &folds)

	reading := make(chan struct{})
	release := make(chan struct{})
	var once, freed sync.Once
	prevCov := snapshotStillCovered
	t.Cleanup(func() { snapshotStillCovered = prevCov })
	snapshotStillCovered = func(context.Context, string, time.Time, time.Time) (bool, bool) {
		once.Do(func() { close(reading) })
		<-release
		return true, true
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	sup := newBaselineSupervisor(ctx, t.TempDir(), baseline.DefaultLockMode)
	quiet := refreshRequest{ServerID: "quiet", ServerName: "quiet", IndexDSN: "d"}
	seedQuietServer(sup, quiet, mark)

	gateDone := make(chan struct{})
	go func() {
		defer close(gateDone)
		sup.refreshCanSkip(context.Background(), quiet, mark, refreshAt)
	}()
	// Registered after the goroutine, so it runs first at cleanup: every exit
	// from this test lets the read return and waits for the gate to leave,
	// before the seams above are put back.
	unblock := func() {
		freed.Do(func() { close(release) })
		<-gateDone
	}
	t.Cleanup(unblock)

	select {
	case <-reading:
	case <-time.After(10 * time.Second):
		t.Fatal("the gate never asked the coverage question, so this test reaches nothing")
	}

	// The read is hung. Another server's job takes the same lock to start.
	other := refreshRequest{ServerID: "other", ServerName: "other", IndexDSN: "d",
		BaselineDir: stageBaselineRoot(t)}
	started := make(chan error, 1)
	go func() {
		_, err := sup.TriggerRefresh(other, time.Minute)
		started <- err
	}()
	select {
	case err := <-started:
		if err != nil {
			t.Fatalf("TriggerRefresh for another server: %v", err)
		}
	case <-time.After(5 * time.Second):
		unblock()
		<-started // the trigger proceeds once the lock is free; its cycle is joined below
		waitForTerminalState(t, func() console.BaselineStatus { return sup.RefreshStatus("other") })
		t.Fatal("a job for ANOTHER server could not start while the gate waited on the index: " +
			"the gate holds the supervisor's lock across the coverage read, so one hung index " +
			"stops every snapshot, restore and SQL export in the process")
	}
	// Joined before the seams are restored: the cycle runs in its own goroutine.
	if st := waitForTerminalState(t, func() console.BaselineStatus { return sup.RefreshStatus("other") }); st.State != "succeeded" {
		t.Errorf("the other server's cycle ended %q (%q), want succeeded", st.State, st.LastError)
	}
	if got := folds.Load(); got != 1 {
		t.Errorf("folded %d time(s), want 1: the other server has no memo, so its cycle folds", got)
	}
}

// Point 3. What the gate says when it cannot answer one of its questions.
//
// One Warn when the condition starts, silence while it lasts, nothing when it
// ends, and a new Warn when it comes back. The two questions are separate
// conditions: a mark that has been unreadable for a week must not hide a
// coverage read that starts failing today.
func TestReportGateBlind(t *testing.T) {
	const why = "cannot tell"
	req := refreshRequest{ServerID: "s", ServerName: "shop"}
	warns := func(log *lockedBuf) int { return strings.Count(log.String(), "level=WARN") }

	t.Run("once per condition, and again after it comes back", func(t *testing.T) {
		log := captureLog(t, slog.LevelDebug)
		sup := newBaselineSupervisor(context.Background(), t.TempDir(), baseline.DefaultLockMode)
		sup.gateEdge = notify.NewEdge(time.Hour)

		sup.reportGateBlind("mark", false, req, why)
		if got := warns(log); got != 1 {
			t.Fatalf("%d Warn line(s) on the first blind cycle, want 1: %q", got, log.String())
		}
		out := log.String()
		for _, want := range []string{why, "server=shop", "id=s", "question=mark"} {
			if !strings.Contains(out, want) {
				t.Errorf("the line does not carry %q, so it cannot be acted on: %q", want, out)
			}
		}
		for range 5 {
			sup.reportGateBlind("mark", false, req, why)
		}
		if got := warns(log); got != 1 {
			t.Errorf("%d Warn line(s) after six blind cycles, want still 1: at a five-minute "+
				"interval this would be 288 lines a day", got)
		}
		sup.reportGateBlind("mark", true, req, why)
		if got := warns(log); got != 1 {
			t.Errorf("%d Warn line(s) after the read worked again, want still 1: recovering "+
				"is not a warning", got)
		}
		if sup.gateEdge.Active("gate-blind:mark:s") {
			t.Error("the condition is still standing after the read worked again")
		}
		sup.reportGateBlind("mark", false, req, why)
		if got := warns(log); got != 2 {
			t.Errorf("%d Warn line(s) after the read failed AGAIN, want 2: a condition that "+
				"ended and came back is a new one", got)
		}
	})

	t.Run("a standing condition is repeated once the quiet window has passed", func(t *testing.T) {
		log := captureLog(t, slog.LevelDebug)
		sup := newBaselineSupervisor(context.Background(), t.TempDir(), baseline.DefaultLockMode)
		sup.gateEdge = notify.NewEdge(time.Millisecond)

		sup.reportGateBlind("coverage", false, req, why)
		time.Sleep(20 * time.Millisecond)
		sup.reportGateBlind("coverage", false, req, why)
		if got := warns(log); got != 2 {
			t.Errorf("%d Warn line(s), want 2: a condition that lasts for months is said "+
				"once per window, not once ever", got)
		}
	})

	t.Run("one question does not speak for the other", func(t *testing.T) {
		log := captureLog(t, slog.LevelDebug)
		sup := newBaselineSupervisor(context.Background(), t.TempDir(), baseline.DefaultLockMode)
		sup.gateEdge = notify.NewEdge(time.Hour)

		sup.reportGateBlind("mark", false, req, why)
		sup.reportGateBlind("coverage", false, req, why)
		if got := warns(log); got != 2 {
			t.Fatalf("%d Warn line(s), want 2: the mark being unreadable hid the coverage read "+
				"failing: %q", got, log.String())
		}
		if !strings.Contains(log.String(), "question=coverage") {
			t.Errorf("no line names the coverage question: %q", log.String())
		}
		// And the recovery of one leaves the other standing.
		sup.reportGateBlind("mark", true, req, why)
		if !sup.gateEdge.Active("gate-blind:coverage:s") {
			t.Error("the mark read recovering resolved the coverage condition too")
		}
		sup.reportGateBlind("coverage", false, req, why)
		if got := warns(log); got != 2 {
			t.Errorf("%d Warn line(s), want still 2: the coverage condition never ended", got)
		}
	})

	t.Run("one server does not speak for another", func(t *testing.T) {
		log := captureLog(t, slog.LevelDebug)
		sup := newBaselineSupervisor(context.Background(), t.TempDir(), baseline.DefaultLockMode)
		sup.gateEdge = notify.NewEdge(time.Hour)

		sup.reportGateBlind("mark", false, req, why)
		sup.reportGateBlind("mark", false, refreshRequest{ServerID: "t", ServerName: "billing"}, why)
		if got := warns(log); got != 2 || !strings.Contains(log.String(), "server=billing") {
			t.Errorf("%d Warn line(s), want 2 with one naming the second server: %q", got, log.String())
		}
	})
}

// The coverage arm, through the gate itself: refreshCanSkip is the only caller
// that asks the second question, so this is where a dropped report would hide.
func TestRefreshCanSkip_saysWhenItCannotReadCoverage(t *testing.T) {
	log := captureLog(t, slog.LevelDebug)
	stubCoverage(t, false, false)
	mark := indexMark{events: 100, schemaChanges: 7}
	sup := newBaselineSupervisor(context.Background(), t.TempDir(), baseline.DefaultLockMode)
	req := refreshRequest{ServerID: "s", ServerName: "shop", IndexDSN: "d"}
	seedQuietServer(sup, req, mark)
	// The mark has been unreadable for a while, and that is already standing.
	sup.reportGateBlind("mark", false, req, "cannot tell whether anything has been indexed")

	for range 3 {
		if sup.refreshCanSkip(context.Background(), req, mark, refreshAt) {
			t.Fatal("skipped on a coverage read that did not answer")
		}
	}
	out := log.String()
	if got := strings.Count(out, "question=coverage"); got != 1 {
		t.Errorf("%d line(s) about the coverage question over three cycles, want 1: %q", got, out)
	}
	if !strings.Contains(out, "level=WARN") || !strings.Contains(out, "cannot tell how far the index still reaches") {
		t.Errorf("the line is not the Warn that explains why an idle server folds: %q", out)
	}
}

// Point 5, third item. A daemon that is shutting down is not blind.
//
// The cancelled context makes the gate's read fail, and the read failing is
// what the Warn reports. But the Warn says the server "will not be skipped
// until the index answers", which is advice about an index, and nothing is
// wrong with the index.
func TestReportGateBlind_aShutdownIsNotBlindness(t *testing.T) {
	log := captureLog(t, slog.LevelDebug)
	ctx, cancel := context.WithCancel(context.Background())
	sup := newBaselineSupervisor(ctx, t.TempDir(), baseline.DefaultLockMode)
	sup.gateEdge = notify.NewEdge(time.Hour)
	req := refreshRequest{ServerID: "s", ServerName: "shop"}

	cancel()
	sup.reportGateBlind("mark", false, req, "cannot tell")
	sup.reportGateBlind("coverage", false, req, "cannot tell")
	if out := log.String(); strings.Contains(out, "level=WARN") {
		t.Errorf("a shutdown was reported as the gate being blind: %q", out)
	}
	if sup.gateEdge.Active("gate-blind:mark:s") || sup.gateEdge.Active("gate-blind:coverage:s") {
		t.Error("a condition was left standing by a read that failed because the daemon is stopping")
	}
}

// And the shutdown does not erase a condition that was real before it.
func TestReportGateBlind_aShutdownLeavesAStandingConditionAlone(t *testing.T) {
	captureLog(t, slog.LevelDebug)
	ctx, cancel := context.WithCancel(context.Background())
	sup := newBaselineSupervisor(ctx, t.TempDir(), baseline.DefaultLockMode)
	sup.gateEdge = notify.NewEdge(time.Hour)
	req := refreshRequest{ServerID: "s", ServerName: "shop"}

	sup.reportGateBlind("mark", false, req, "cannot tell")
	cancel()
	sup.reportGateBlind("mark", false, req, "cannot tell")
	if !sup.gateEdge.Active("gate-blind:mark:s") {
		t.Error("the shutdown resolved a condition nothing had fixed")
	}
}

// Point 5, first item. A destination is a place, and two spellings of one
// place are one destination.
//
// The memo speaks only for the destination its fold reached, so two spellings
// that compare different cost a fold that applies nothing after a settings
// edit that changed nothing. The rule on each side is the one the cycle itself
// writes by: the directory goes through filepath.Join, the bucket has ONE
// trailing slash trimmed before the snapshot name is appended.
func TestRefreshDestination(t *testing.T) {
	dest := func(dir, s3 string) string {
		return refreshDestination(refreshRequest{BaselineDir: dir, BaselineS3: s3})
	}
	same := []struct {
		name         string
		dirA, s3A    string
		dirB, s3B    string
		sameLocation string
	}{
		{name: "a trailing slash on the directory", dirA: "/var/backups", dirB: "/var/backups/",
			sameLocation: "both join to /var/backups/<snapshot>"},
		{name: "several trailing slashes", dirA: "/var/backups", dirB: "/var/backups///"},
		{name: "a doubled slash in the middle", dirA: "/var/backups", dirB: "/var//backups"},
		{name: "a dot segment", dirA: "/var/backups", dirB: "/var/./backups"},
		{name: "a relative directory", dirA: "backups", dirB: "./backups/"},
		{name: "a trailing slash on the bucket", s3A: "s3://acme/shop", s3B: "s3://acme/shop/",
			sameLocation: "the upload trims it before appending the snapshot name"},
		{name: "both at once", dirA: "/var/backups", s3A: "s3://acme/shop",
			dirB: "/var/backups/", s3B: "s3://acme/shop/"},
	}
	for _, c := range same {
		t.Run("same: "+c.name, func(t *testing.T) {
			if a, b := dest(c.dirA, c.s3A), dest(c.dirB, c.s3B); a != b {
				t.Errorf("%q and %q compare different (%q vs %q): one wasted fold after a "+
					"settings edit that moved nothing", c.dirA+c.s3A, c.dirB+c.s3B, a, b)
			}
		})
	}

	// Every one of these is a different place, and calling two places one
	// destination is the dangerous direction: the gate would skip the cycle
	// whose whole job was to put a copy in the second.
	different := []struct {
		name      string
		dirA, s3A string
		dirB, s3B string
	}{
		{name: "another directory", dirA: "/var/backups", dirB: "/mnt/backups"},
		{name: "a directory that only starts the same", dirA: "/var/backups", dirB: "/var/backups2"},
		{name: "a subdirectory", dirA: "/var/backups", dirB: "/var/backups/shop"},
		{name: "upper and lower case", dirA: "/var/backups", dirB: "/var/Backups"},
		{name: "a space in front", dirA: "/var/backups", dirB: " /var/backups"},
		{name: "a space behind", dirA: "/var/backups", dirB: "/var/backups "},
		{name: "no directory against the working directory", dirA: "", dirB: "."},
		{name: "no directory against the root", dirA: "", dirB: "/"},
		{name: "a bucket added", dirA: "/var/backups", dirB: "/var/backups", s3B: "s3://acme/shop"},
		{name: "another bucket", s3A: "s3://acme/shop", s3B: "s3://acme-eu/shop"},
		{name: "another prefix", s3A: "s3://acme/shop", s3B: "s3://acme/shop2"},
		{name: "bucket case", s3A: "s3://acme/shop", s3B: "s3://acme/Shop"},
		{name: "two trailing slashes on the bucket", s3A: "s3://acme/shop", s3B: "s3://acme/shop//"},
		{name: "a bucket that is only a slash against no bucket", dirA: "/var/backups", dirB: "/var/backups", s3B: "/"},
		{name: "the same text as a directory and as a bucket", dirA: "backups", s3B: "backups"},
		{name: "text moved across the two", dirA: "a", s3A: "b", dirB: "ab", s3B: ""},
	}
	for _, c := range different {
		t.Run("different: "+c.name, func(t *testing.T) {
			if a, b := dest(c.dirA, c.s3A), dest(c.dirB, c.s3B); a == b {
				t.Errorf("(%q, %q) and (%q, %q) compare EQUAL (%q): the gate would skip a cycle "+
					"on the strength of a fold that wrote somewhere else",
					c.dirA, c.s3A, c.dirB, c.s3B, a)
			}
		})
	}
}

// The rule above is only right if it matches where the cycle writes. Pinned
// against the two functions that build those paths, so a change to either one
// shows up here instead of as a skipped upload.
func TestRefreshDestination_matchesWhereTheCycleWrites(t *testing.T) {
	var uploadedTo []string
	prevUp := uploadSnapshot
	t.Cleanup(func() { uploadSnapshot = prevUp })
	uploadSnapshot = func(_ context.Context, _, dest, _ string, _ bool) (int, error) {
		uploadedTo = append(uploadedTo, dest)
		return 1, nil
	}
	spellings := [][2]refreshRequest{
		{{BaselineDir: "/var/backups", BaselineS3: "s3://acme/shop"},
			{BaselineDir: "/var/backups/", BaselineS3: "s3://acme/shop/"}},
		{{BaselineDir: "/var/backups", BaselineS3: "s3://acme/shop"},
			{BaselineDir: "/var//backups", BaselineS3: "s3://acme/shop//"}},
	}
	for _, pair := range spellings {
		uploadedTo = nil
		var dirs []string
		for _, req := range pair {
			dirs = append(dirs, refreshSnapshotDir(req, refreshAt))
			if _, err := uploadRefreshedSnapshot(context.Background(), req, refreshAt); err != nil {
				t.Fatalf("uploadRefreshedSnapshot: %v", err)
			}
		}
		writesSame := dirs[0] == dirs[1] && uploadedTo[0] == uploadedTo[1]
		namedSame := refreshDestination(pair[0]) == refreshDestination(pair[1])
		if writesSame != namedSame {
			t.Errorf("(%q, %q) and (%q, %q): the cycle writes to the same place = %v, the memo "+
				"calls them the same destination = %v", pair[0].BaselineDir, pair[0].BaselineS3,
				pair[1].BaselineDir, pair[1].BaselineS3, writesSame, namedSame)
		}
	}
}

// Point 4. A quiet server and a refresh loop that stopped must not look the
// same.
//
// A skipped cycle writes no run record and no status, so the last run on the
// wire is the same before and after it. The one thing that moves is when the
// loop last looked, and it moves without touching anything the last run said.
func TestRefreshStatus_aSkippedCycleSaysTheLoopLooked(t *testing.T) {
	var folds atomic.Int32
	mark := indexMark{events: 100, schemaChanges: 7}
	stubIndexMark(t, &mark, true)
	countFolds(t, &folds)
	stubBucketListing(t)
	stubCoverage(t, true, true)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	sup := newBaselineSupervisor(ctx, t.TempDir(), baseline.DefaultLockMode)
	if got := sup.RefreshStatus("s"); got.State != "idle" || got.CheckedAt != "" {
		t.Fatalf("before any cycle: %+v, want idle and never checked", got)
	}
	req := refreshRequest{ServerID: "s", ServerName: "s", IndexDSN: "d", BaselineDir: stageBaselineRoot(t)}
	cycle := func() console.BaselineStatus {
		t.Helper()
		if _, err := sup.TriggerRefresh(req, time.Minute); err != nil {
			t.Fatalf("TriggerRefresh: %v", err)
		}
		return waitForTerminalState(t, func() console.BaselineStatus { return sup.RefreshStatus("s") })
	}

	folded := cycle()
	if folds.Load() != 1 || folded.State != "succeeded" {
		t.Fatalf("the first cycle folded %d time(s) and ended %q", folds.Load(), folded.State)
	}
	if _, err := time.Parse(time.RFC3339, folded.CheckedAt); err != nil {
		t.Fatalf("after a fold, checked_at = %q, want an RFC3339 instant: %v", folded.CheckedAt, err)
	}

	// The stamp has one-second resolution. A schedule period is minutes; here
	// the second has to be waited for.
	for s := nowStamp(); nowStamp() == s; {
		time.Sleep(20 * time.Millisecond)
	}
	skipped := cycle()
	if folds.Load() != 1 {
		t.Fatalf("folded %d time(s), want still 1: the second cycle had to be skipped", folds.Load())
	}
	if skipped.CheckedAt <= folded.CheckedAt {
		t.Errorf("checked_at went from %q to %q over a skipped cycle, want it later: the loop "+
			"looked, and nothing else on the wire can say so", folded.CheckedAt, skipped.CheckedAt)
	}
	if skipped.FinishedAt != folded.FinishedAt {
		t.Errorf("finished_at moved from %q to %q: no run finished", folded.FinishedAt, skipped.FinishedAt)
	}
	// And the slot itself never carries it: what a skipped cycle restores is
	// the last run's status, not a copy the loop has written on.
	sup.mu.Lock()
	slot := *sup.refreshes["s"]
	sup.mu.Unlock()
	if slot.CheckedAt != "" {
		t.Errorf("the status slot carries checked_at = %q: it belongs to the loop, and the slot "+
			"is the last run's", slot.CheckedAt)
	}
	skipped.CheckedAt, folded.CheckedAt = "", ""
	if !reflect.DeepEqual(skipped, folded) {
		t.Errorf("the skipped cycle changed more than checked_at:\n got %+v\nwant %+v", skipped, folded)
	}
}

// The name on the wire, and that a status without one stays as it was.
func TestBaselineStatus_checkedAtOnTheWire(t *testing.T) {
	out, err := json.Marshal(console.BaselineStatus{State: "succeeded", CheckedAt: "2026-08-28T10:05:00Z"})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !strings.Contains(string(out), `"checked_at":"2026-08-28T10:05:00Z"`) {
		t.Errorf("wire = %s, want checked_at", out)
	}
	out, err = json.Marshal(console.BaselineStatus{State: "succeeded"})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if strings.Contains(string(out), "checked_at") {
		t.Errorf("wire = %s: a dump, a restore and an export have no loop to report on", out)
	}
}

// A cycle that stops on an internal error ended too. It leaves by the panic
// guard and not by the cycle's own last lines, so the guard has to say the loop
// looked: a loop that crashes every cycle must not read as one that stopped.
func TestRefreshStatus_aCycleThatPanickedSaysTheLoopLooked(t *testing.T) {
	captureLog(t, slog.LevelError)
	mark := indexMark{events: 100, schemaChanges: 7}
	stubIndexMark(t, &mark, true)
	stubBucketListing(t)
	stubCoverage(t, true, true)
	prevFold := foldTables
	t.Cleanup(func() { foldTables = prevFold })
	foldTables = func(context.Context, reconstruct.FullTableConfig) (
		[]*reconstruct.TableReport, []reconstruct.TableFailure, error) {
		panic("staged")
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	sup := newBaselineSupervisor(ctx, t.TempDir(), baseline.DefaultLockMode)
	req := refreshRequest{ServerID: "s", ServerName: "s", IndexDSN: "d", BaselineDir: stageBaselineRoot(t)}
	if _, err := sup.TriggerRefresh(req, time.Minute); err != nil {
		t.Fatalf("TriggerRefresh: %v", err)
	}
	st := waitForTerminalState(t, func() console.BaselineStatus { return sup.RefreshStatus("s") })
	if st.State != "failed" {
		t.Fatalf("the cycle ended %q, want failed: the fold panicked", st.State)
	}
	if st.CheckedAt == "" || st.CheckedAt != st.FinishedAt {
		t.Errorf("checked_at = %q and finished_at = %q, want the same instant", st.CheckedAt, st.FinishedAt)
	}
}
