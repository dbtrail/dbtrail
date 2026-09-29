package consoleapp

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/duckdbutil"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// #1735: the same job folds a chain INTO its table (the major compaction)
// when the chain is old enough, large enough, or close to the line the
// refresh ends chains on. One per run: the job holds the refresh's slot, and
// a fold is a full pass over the table.

type majorStub struct {
	mu    sync.Mutex
	calls []string // table names
	err   error
}

func (ms *majorStub) Calls() []string {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	return append([]string(nil), ms.calls...)
}

// stubMajor replaces the fold, and makes every chain's newest pair one that
// moved past its base, started at starts[table] (chainStart when absent).
func stubMajor(t *testing.T, starts map[string]time.Time) *majorStub {
	t.Helper()
	ms := &majorStub{}
	prevMajor, prevFooter := compactMajor, readChainFooter
	t.Cleanup(func() { compactMajor, readChainFooter = prevMajor, prevFooter })
	readChainFooter = func(path string) (baseline.DumpMetadata, error) {
		start := chainStart
		for table, s := range starts {
			if strings.HasPrefix(filepath.Base(path), table+".") {
				start = s
			}
		}
		return baseline.DumpMetadata{DeltaChainStart: start, BinlogFile: "binlog.000009", BinlogPos: 900, DeltaBaseAnchor: "binlog.000009:4"}, nil
	}
	compactMajor = func(_ context.Context, base, outDir string, _ func(string, int64) error, _ duckdbutil.Tuning) (*reconstruct.MajorCompaction, error) {
		table := strings.TrimSuffix(filepath.Base(base), ".parquet")
		ms.mu.Lock()
		ms.calls = append(ms.calls, table)
		ms.mu.Unlock()
		if err := os.MkdirAll(outDir, 0o755); err != nil {
			return nil, err
		}
		if ms.err != nil {
			// A fold that dies part way leaves its temporary behind.
			os.WriteFile(filepath.Join(outDir, filepath.Base(base)+".tmp"), []byte("half"), 0o644)
			return nil, ms.err
		}
		out := filepath.Join(outDir, filepath.Base(base))
		if err := os.WriteFile(out, []byte("folded"), 0o644); err != nil {
			return nil, err
		}
		return &reconstruct.MajorCompaction{Base: out, Seq: 3}, nil
	}
	return ms
}

// addChain gives the rig's snapshot a second table with n pairs.
func addChain(t *testing.T, req refreshRequest, table string, n int) {
	t.Helper()
	snapDir := filepath.Join(req.BaselineDir, reconstruct.SnapshotDirName(chainStart.Add(time.Hour)))
	chains, err := listSnapshotChains(context.Background(), snapDir)
	if err != nil {
		t.Fatal(err)
	}
	all := map[string]*baseline.TableDeltaChain{}
	for b, c := range chains {
		all[b] = c
	}
	base := filepath.Join(snapDir, "shop", table+".parquet")
	if err := os.WriteFile(base, []byte("rows"), 0o644); err != nil {
		t.Fatal(err)
	}
	c := &baseline.TableDeltaChain{}
	for i := range n {
		p, u := baseline.TableDeltaPaths(base, i)
		for _, x := range []string{p, u} {
			if err := os.WriteFile(x, []byte("x"), 0o644); err != nil {
				t.Fatal(err)
			}
		}
		c.Files = append(c.Files, baseline.TableDeltaFile{Seq: i, SeqLo: i, Posdel: p, Upserts: u})
	}
	all[base] = c
	listSnapshotChains = func(context.Context, string) (map[string]*baseline.TableDeltaChain, error) { return all, nil }
}

// A day-old chain is folded into its table and the result staged complete;
// a second look finds it staged and starts nothing.
func TestMaybeCompact_foldsADayOldChainIntoItsTable(t *testing.T) {
	sup, req, cs, stage := compactRig(t, 3) // short: no minor is due
	ms := stubMajor(t, nil)                 // chainStart is days before now
	sup.maybeCompact(req)
	st := waitCompact(t, sup, "s")
	if st.State != "succeeded" || st.Tables != 1 || len(cs.Calls()) != 0 {
		t.Fatalf("status = %+v minors = %v", st, cs.Calls())
	}
	if calls := ms.Calls(); len(calls) != 1 || calls[0] != "orders" {
		t.Fatalf("folds = %v", calls)
	}
	for _, f := range []string{"orders.parquet", baseline.SuccessMarker} {
		if _, err := os.Stat(filepath.Join(stage, f)); err != nil {
			t.Fatalf("%s not staged: %v", f, err)
		}
	}
	sup.maybeCompact(req)
	waitCompact(t, sup, "s")
	if calls := ms.Calls(); len(calls) != 1 {
		t.Fatalf("folds = %v, want no second fold while the result waits", calls)
	}
}

// Two chains due: the one that started first is folded, the other waits for
// the next run and still gets its minor merge now.
func TestMaybeCompact_foldsOneChainPerRunTheOldestFirst(t *testing.T) {
	sup, req, cs, _ := compactRig(t, compactMinPairs)
	addChain(t, req, "items", compactMinPairs)
	ms := stubMajor(t, map[string]time.Time{"orders": chainStart, "items": chainStart.Add(-time.Hour)})
	sup.maybeCompact(req)
	st := waitCompact(t, sup, "s")
	if st.State != "succeeded" || st.Tables != 2 {
		t.Fatalf("status = %+v", st)
	}
	if calls := ms.Calls(); len(calls) != 1 || calls[0] != "items" {
		t.Fatalf("folds = %v, want only items, whose chain started first", calls)
	}
	if calls := cs.Calls(); len(calls) != 1 || calls[0] != "orders 0-14" {
		t.Fatalf("minors = %v, want orders merged while it waits", calls)
	}
}

// A fold replaces a minor result waiting for the same chain: the fold takes
// every pair the minor merged, and the refresh adopts one result per chain.
func TestMaybeCompact_aFoldReplacesAStagedMinor(t *testing.T) {
	sup, req, _, stage := compactRig(t, compactMinPairs)
	if err := os.MkdirAll(stage, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, f := range []string{"orders.000000-000014.posdel", "orders.000000-000014.upserts", baseline.SuccessMarker} {
		if err := os.WriteFile(filepath.Join(stage, f), []byte("r"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	ms := stubMajor(t, nil)
	sup.maybeCompact(req)
	if st := waitCompact(t, sup, "s"); st.State != "succeeded" || len(ms.Calls()) != 1 {
		t.Fatalf("status = %+v folds = %v", st, ms.Calls())
	}
	left, _ := os.ReadDir(stage)
	var names []string
	for _, e := range left {
		names = append(names, e.Name())
	}
	if strings.Join(names, ",") != "_SUCCESS,orders.parquet" {
		t.Fatalf("staged = %v, want the fold alone", names)
	}
}

// A young chain is never folded, even at the line: a chain starts at a fold,
// so without this a short retention would fold every chain every cycle.
// Near the line recorded by the last refresh, a chain an hour old is.
func TestMaybeCompact_foldsNearTheLineButNotAYoungChain(t *testing.T) {
	now := time.Now().UTC()
	sup, req, _, _ := compactRig(t, 3)
	ms := stubMajor(t, map[string]time.Time{"orders": now.Add(-10 * time.Minute)})
	sup.recordChainFloor("s", now, 5*time.Minute)
	sup.maybeCompact(req)
	if len(ms.Calls()) != 0 || sup.compactStatusFor("s").State != "idle" {
		t.Fatalf("a ten-minute-old chain was folded: %v", ms.Calls())
	}
	// Two hours old, not a day, small: due only because the line the last
	// refresh drew is within the lead of its start.
	start := now.Add(-2 * time.Hour)
	ms2 := stubMajor(t, map[string]time.Time{"orders": start})
	sup.recordChainFloor("s", start.Add(-30*time.Minute), 5*time.Minute)
	sup.maybeCompact(req)
	if st := waitCompact(t, sup, "s"); st.State != "succeeded" || len(ms2.Calls()) != 1 {
		t.Fatalf("status = %+v folds = %v, want the chain folded before the line reaches it", st, ms2.Calls())
	}
	// The same chain with the line far below: not due.
	sup2, req2, _, _ := compactRig(t, 3)
	ms3 := stubMajor(t, map[string]time.Time{"orders": start})
	sup2.recordChainFloor("s", start.Add(-10*time.Hour), 5*time.Minute)
	sup2.maybeCompact(req2)
	if len(ms3.Calls()) != 0 {
		t.Fatalf("folded with the line far away: %v", ms3.Calls())
	}
}

// A fold alone is enough to consult who wrote the snapshot (#1684): another
// writer's is left alone.
func TestMaybeCompact_foldOfAForeignSnapshotIsRefused(t *testing.T) {
	sup, req, _, _ := compactRig(t, 3)
	ms := stubMajor(t, nil)
	snapDir := filepath.Join(req.BaselineDir, reconstruct.SnapshotDirName(chainStart.Add(time.Hour)))
	if err := os.WriteFile(filepath.Join(snapDir, baseline.WriterMarkerPrefix+otherWriter), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	req.IndexDSN = "idx"
	ownIdentity(t, ownWriter, nil)
	sup.maybeCompact(req)
	st := waitCompact(t, sup, "s")
	if st.State != "failed" || !strings.Contains(st.LastError, otherWriter) || len(ms.Calls()) != 0 {
		t.Fatalf("status = %+v folds = %v", st, ms.Calls())
	}
}

// A failed fold frees the slot, stages nothing, and is a failed run.
func TestCompactJob_failedFoldStagesNothing(t *testing.T) {
	sup, req, _, stage := compactRig(t, 3)
	ms := stubMajor(t, nil)
	ms.err = errors.New("no space left on device")
	sup.maybeCompact(req)
	st := waitCompact(t, sup, "s")
	if st.State != "failed" || !strings.Contains(st.LastError, "no space left") {
		t.Fatalf("status = %+v", st)
	}
	if _, err := os.Stat(stage); !os.IsNotExist(err) {
		t.Fatalf("a failed fold left %s: %v", stage, err)
	}
	sup.mu.Lock()
	busy := sup.busyLocked("s")
	sup.mu.Unlock()
	if busy {
		t.Fatal("the failed job still holds the slot")
	}
	if runs := sup.history.List("s"); len(runs) != 1 || !strings.Contains(runs[0].Error, "no space left") {
		t.Fatalf("history = %+v", runs)
	}
}

// The refresh the job serves is told that the job exists, so it stops
// rewriting on age and size itself; a server with deltas off is not.
func TestRefreshFoldConfig_tellsTheFoldAboutTheJob(t *testing.T) {
	req := refreshRequest{BaselineDir: "/b", TableDeltas: true}
	if cfg := refreshFoldConfig(req, time.Now(), nil); !cfg.CompactionJob || cfg.CompactDir == "" {
		t.Fatalf("deltas on: %+v", cfg)
	}
	req.TableDeltas = false
	if cfg := refreshFoldConfig(req, time.Now(), nil); cfg.CompactionJob {
		t.Fatal("deltas off: the fold was told a job folds its chains")
	}
}

// The line runRefresh draws is what the job reads next, with the cycle's
// interval; an unread floor leaves no line, not the previous one.
func TestRunRefresh_recordsTheLineForTheJob(t *testing.T) {
	folds := probeFolds(t)
	stubCoverage(t, true, true)
	stubReadsFrom(t, refreshAt, nil)
	floor := refreshAt.Add(-12 * time.Hour)
	stubLiveFloor(t, floor, true)
	sup, _, run := deltasRig(t, true)
	if f, _ := sup.chainFloorFor("s"); !f.IsZero() {
		t.Fatal("a line before any refresh")
	}
	run(refreshAt)
	want := refreshAt.Add(-9*time.Hour - 36*time.Minute)
	if f, iv := sup.chainFloorFor("s"); f.Sub(want).Abs() > time.Millisecond || iv != 5*time.Minute {
		t.Fatalf("line = %s interval = %s, want %s and 5m", f, iv, want)
	}
	stubLiveFloor(t, time.Time{}, false)
	stubCoverage(t, false, true) // not covered: the cycle folds
	sup.setFoldJobBlocked("s", true)
	run(refreshAt.Add(time.Hour))
	if f, _ := sup.chainFloorFor("s"); !f.IsZero() {
		t.Fatalf("line = %s after an unread floor, want none", f)
	}
	// The job's state reaches the fold: blocked, the fold keeps its rules.
	cfgs := folds.all()
	if len(cfgs) != 2 || !cfgs[0].CompactionJob || cfgs[1].CompactionJob {
		t.Fatalf("CompactionJob per fold = %v, want on then off (blocked)", func() []bool {
			var out []bool
			for _, c := range cfgs {
				out = append(out, c.CompactionJob)
			}
			return out
		}())
	}
}

// A fold stopped before its _SUCCESS (a killed process) leaves a table file,
// perhaps a temporary: swept at the next cycle, never adopted. A complete
// one is kept for the refresh.
func TestSweepCompactStaging_removesAnUnfinishedFold(t *testing.T) {
	local := t.TempDir()
	req := refreshRequest{BaselineDir: local, TableDeltas: true}
	half := reconstruct.CompactionDir(compactDirFor(local), "shop", "orders", chainStart)
	done := reconstruct.CompactionDir(compactDirFor(local), "shop", "items", chainStart)
	for _, d := range []string{half, done} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	for _, f := range []string{filepath.Join(half, "orders.parquet"), filepath.Join(half, "orders.parquet.tmp"),
		filepath.Join(done, "items.parquet"), filepath.Join(done, baseline.SuccessMarker)} {
		if err := os.WriteFile(f, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	sweepCompactStaging(req)
	if _, err := os.Stat(half); !os.IsNotExist(err) {
		t.Fatal("an unfinished fold survived the sweep")
	}
	if kind, _, _ := stagedResult(done, "items"); kind != "major" {
		t.Fatal("a complete fold was swept")
	}
}

// A chain whose fold a refresh refused is not folded again (the refusal
// would repeat, one full pass per cycle), but its first pairs are still
// merged, and the record of the refusal outlives that merge and the sweep.
// Said once.
func TestMaybeCompact_aRefusedChainIsNotFoldedAgain(t *testing.T) {
	sup, req, cs, stage := compactRig(t, compactMinPairs)
	ms := stubMajor(t, nil)
	if err := os.MkdirAll(stage, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(stage, reconstruct.CompactionRefusedMarker), []byte("a row lost"), 0o644); err != nil {
		t.Fatal(err)
	}
	logs := captureSlogFor(t)
	sweepCompactStaging(req)
	sup.maybeCompact(req)
	if st := waitCompact(t, sup, "s"); st.State != "succeeded" {
		t.Fatalf("status = %+v", st)
	}
	sweepCompactStaging(req)
	sup.maybeCompact(req) // the minor is staged now: nothing to do
	if len(ms.Calls()) != 0 || len(cs.Calls()) != 1 {
		t.Fatalf("folds = %v minors = %v, want no fold and one merge", ms.Calls(), cs.Calls())
	}
	if n := strings.Count(logs.String(), "refused this chain's fold"); n != 1 {
		t.Fatalf("said %d times: %q", n, logs.String())
	}
	for _, f := range []string{reconstruct.CompactionRefusedMarker, baseline.SuccessMarker} {
		if _, err := os.Stat(filepath.Join(stage, f)); err != nil {
			t.Fatalf("%s gone: %v", f, err)
		}
	}
}

// A staging folder the job cannot look at is a failed run, with the reason,
// not a table skipped in silence.
func TestMaybeCompact_anUnreadableStagingFolderIsAFailedRun(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root reads any folder")
	}
	sup, req, _, stage := compactRig(t, 3)
	ms := stubMajor(t, nil)
	if err := os.MkdirAll(stage, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(stage, 0); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(stage, 0o755) })
	sup.maybeCompact(req)
	st := waitCompact(t, sup, "s")
	if st.State != "failed" || !strings.Contains(st.LastError, "cannot be looked at") || len(ms.Calls()) != 0 {
		t.Fatalf("status = %+v folds = %v", st, ms.Calls())
	}
	if runs := sup.history.List("s"); len(runs) != 1 || !strings.Contains(runs[0].Error, "cannot be looked at") {
		t.Fatalf("history = %+v", runs)
	}
}

// A job that never gets the slot is not folding either: after a few busy
// looks in a row it says so, and the refresh takes its rules back until the
// job runs.
func TestMaybeCompact_aJobAlwaysBusyHandsTheRulesBack(t *testing.T) {
	sup, req, _, _ := compactRig(t, 3)
	stubMajor(t, nil)
	sup.jobs["s"] = &console.BaselineStatus{State: "running"}
	logs := captureSlogFor(t)
	for i := range compactBusyTries {
		if sup.foldJobBlocked("s") {
			t.Fatalf("blocked after %d busy look(s)", i)
		}
		sup.maybeCompact(req)
	}
	if !sup.foldJobBlocked("s") || !strings.Contains(logs.String(), "has not had the slot") {
		t.Fatalf("blocked=%v log=%q", sup.foldJobBlocked("s"), logs.String())
	}
	sup.jobs["s"] = &console.BaselineStatus{State: "succeeded"}
	sup.maybeCompact(req)
	waitCompact(t, sup, "s")
	if sup.foldJobBlocked("s") {
		t.Fatal("still blocked after the job ran")
	}
}

// A chain whose fold failed leaves its turn to the others for a while: the
// oldest chain failing every time would otherwise stop every other fold.
func TestMaybeCompact_aFailedFoldLetsTheNextChainGo(t *testing.T) {
	sup, req, _, _ := compactRig(t, 3)
	addChain(t, req, "items", 3)
	ms := stubMajor(t, map[string]time.Time{"orders": chainStart.Add(-time.Hour), "items": chainStart})
	ms.err = errors.New("no space left on device")
	sup.maybeCompact(req)
	waitCompact(t, sup, "s")
	ms.err = nil
	sup.maybeCompact(req)
	if st := waitCompact(t, sup, "s"); st.State != "succeeded" {
		t.Fatalf("status = %+v", st)
	}
	if calls := ms.Calls(); strings.Join(calls, ",") != "orders,items" {
		t.Fatalf("folds = %v, want orders (failed) then items", calls)
	}
	// Past the wait, the failed chain is tried again.
	prev := majorRetryAfter
	majorRetryAfter = 0
	t.Cleanup(func() { majorRetryAfter = prev })
	sup.maybeCompact(req)
	waitCompact(t, sup, "s")
	if calls := ms.Calls(); len(calls) != 3 || calls[2] != "orders" {
		t.Fatalf("folds = %v, want orders tried again", calls)
	}
}

// A chain that cannot be sized is not folded on a guess of "small".
func TestMaybeCompact_aChainThatCannotBeSizedIsNotFolded(t *testing.T) {
	sup, req, _, _ := compactRig(t, 3)
	ms := stubMajor(t, nil)
	snapDir := filepath.Join(req.BaselineDir, reconstruct.SnapshotDirName(chainStart.Add(time.Hour)))
	// A pair listed a moment ago and gone now.
	posdel, _ := baseline.TableDeltaPaths(filepath.Join(snapDir, "shop", "orders.parquet"), 1)
	if err := os.Remove(posdel); err != nil {
		t.Fatal(err)
	}
	logs := captureSlogFor(t)
	sup.maybeCompact(req)
	if !strings.Contains(logs.String(), "could not size a chain") {
		t.Fatalf("not said: %q", logs.String())
	}
	if len(ms.Calls()) != 0 || sup.compactStatusFor("s").State != "idle" {
		t.Fatalf("folded: %v", ms.Calls())
	}
}

// When the job cannot run (here: another writer's snapshot), the refresh is
// told, and keeps its own day and quarter rules; once it runs again, not.
func TestMaybeCompact_aBlockedJobHandsTheRulesBack(t *testing.T) {
	sup, req, _, _ := compactRig(t, 3)
	stubMajor(t, nil)
	snapDir := filepath.Join(req.BaselineDir, reconstruct.SnapshotDirName(chainStart.Add(time.Hour)))
	marker := filepath.Join(snapDir, baseline.WriterMarkerPrefix+otherWriter)
	if err := os.WriteFile(marker, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	req.IndexDSN = "idx"
	ownIdentity(t, ownWriter, nil)
	sup.maybeCompact(req)
	if !sup.foldJobBlocked("s") {
		t.Fatal("a refused job is not marked blocked")
	}
	blocked := req
	blocked.FoldJobBlocked = true
	if cfg := refreshFoldConfig(blocked, time.Now(), nil); cfg.CompactionJob {
		t.Fatal("the fold was told a blocked job folds its chains")
	}
	os.Remove(marker)
	sup.compacts["s"] = &console.BaselineStatus{State: "idle"}
	sup.maybeCompact(req)
	waitCompact(t, sup, "s")
	if sup.foldJobBlocked("s") {
		t.Fatal("still blocked after the job ran")
	}
}
