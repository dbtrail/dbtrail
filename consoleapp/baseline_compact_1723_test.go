package consoleapp

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// #1723: the compaction job. Driven through its seams (the chain listing,
// the last pair's footer, the merge) so no real chain is needed; what is
// tested is the daemon's side: when it starts, that it shares the slot, what
// it stages, what it records, and what a crash leaves.

var chainStart = time.Date(2026, 9, 18, 8, 0, 0, 0, time.UTC)

// fakeChain lists n plain pairs beside <snap>/shop/orders.parquet, with
// real (empty) files so the manifest check has paths to look at.
func fakeChain(t *testing.T, snapDir string, n int) map[string]*baseline.TableDeltaChain {
	t.Helper()
	dir := filepath.Join(snapDir, "shop")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	base := filepath.Join(dir, "orders.parquet")
	c := &baseline.TableDeltaChain{}
	for i := range n {
		posdel, upserts := baseline.TableDeltaPaths(base, i)
		for _, p := range []string{posdel, upserts} {
			if err := os.WriteFile(p, []byte("x"), 0o644); err != nil {
				t.Fatal(err)
			}
		}
		c.Files = append(c.Files, baseline.TableDeltaFile{Seq: i, SeqLo: i, Posdel: posdel, Upserts: upserts})
	}
	return map[string]*baseline.TableDeltaChain{base: c}
}

type compactStub struct {
	mu        sync.Mutex
	calls     []string // "<table> <lo>-<hi>"
	err       error
	failTable string // err applies to this table only when set
	panic     bool
}

// Calls is the merges asked so far. Read after waitCompact, which observes
// the job's terminal state under the supervisor's mutex; the stub records
// under its own so a test never reads the slice while the job appends.
func (cs *compactStub) Calls() []string {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	return append([]string(nil), cs.calls...)
}

func stubCompaction(t *testing.T, chains map[string]*baseline.TableDeltaChain) *compactStub {
	t.Helper()
	cs := &compactStub{}
	prevList, prevMinor, prevFooter := listSnapshotChains, compactMinor, readChainFooter
	t.Cleanup(func() { listSnapshotChains, compactMinor, readChainFooter = prevList, prevMinor, prevFooter })
	listSnapshotChains = func(context.Context, string) (map[string]*baseline.TableDeltaChain, error) { return chains, nil }
	readChainFooter = func(string) (baseline.DumpMetadata, error) {
		return baseline.DumpMetadata{DeltaChainStart: chainStart}, nil
	}
	compactMinor = func(_ context.Context, base string, c *baseline.TableDeltaChain, lo, hi int, outDir string) (*reconstruct.MinorCompaction, error) {
		cs.mu.Lock()
		cs.calls = append(cs.calls, strings.TrimSuffix(filepath.Base(base), ".parquet")+" "+strconv.Itoa(lo)+"-"+strconv.Itoa(hi))
		cs.mu.Unlock()
		if cs.panic {
			panic("boom in the merge")
		}
		if cs.err != nil && (cs.failTable == "" || strings.HasSuffix(base, cs.failTable+".parquet")) {
			return nil, cs.err
		}
		if err := os.MkdirAll(outDir, 0o755); err != nil {
			return nil, err
		}
		p, u := baseline.TableDeltaRangePaths(filepath.Join(outDir, filepath.Base(base)), lo, hi)
		for _, f := range []string{p, u} {
			if err := os.WriteFile(f, []byte("r"), 0o644); err != nil {
				return nil, err
			}
		}
		return &reconstruct.MinorCompaction{Lo: lo, Hi: hi, Posdel: p, Upserts: u, Merged: hi - lo + 1}, nil
	}
	return cs
}

// compactRig: a supervisor with a history, a local backup directory holding
// one snapshot, and the request a refresh would leave behind.
func compactRig(t *testing.T, pairs int) (*baselineSupervisor, refreshRequest, *compactStub, string) {
	t.Helper()
	local := t.TempDir()
	snapDir := filepath.Join(local, reconstruct.SnapshotDirName(chainStart.Add(time.Hour)))
	writeSnapshotFiles(t, snapDir, baseline.SuccessMarker)
	cs := stubCompaction(t, fakeChain(t, snapDir, pairs))
	sup := newBaselineSupervisor(context.Background(), t.TempDir(), baseline.DefaultLockMode)
	h, err := console.OpenBaselineHistory(filepath.Join(t.TempDir(), "h.json"))
	if err != nil {
		t.Fatal(err)
	}
	sup.history = h
	req := refreshRequest{ServerID: "s", ServerName: "s", BaselineDir: local, TableDeltas: true, Trigger: console.BaselineRunTriggerScheduled}
	return sup, req, cs, reconstruct.CompactionDir(compactDirFor(local), "shop", "orders", chainStart)
}

func waitCompact(t *testing.T, sup *baselineSupervisor, id string) console.BaselineStatus {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		st := sup.compactStatusFor(id)
		if st.State != "running" && st.State != "idle" {
			return st
		}
		if time.Now().After(deadline) {
			t.Fatalf("compaction never finished: %+v", st)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestMaybeCompact_mergesTheChainPrefixAfterALongChain(t *testing.T) {
	sup, req, cs, stage := compactRig(t, compactMinPairs)
	sup.maybeCompact(req)
	st := waitCompact(t, sup, "s")
	if st.State != "succeeded" || st.Tables != 1 || st.Refused != 0 {
		t.Fatalf("status = %+v", st)
	}
	if calls := cs.Calls(); len(calls) != 1 || calls[0] != "orders 0-14" { // all but the last pair
		t.Fatalf("merges = %v, want the chain's first 15 pairs", calls)
	}
	if _, err := os.Stat(filepath.Join(stage, baseline.SuccessMarker)); err != nil {
		t.Fatalf("no complete result staged under %s: %v", stage, err)
	}
	runs := sup.history.List("s")
	// Recorded with NO trigger: the schedule's "last run" must stay the
	// refresh, not this housekeeping (it would read as a full backup that
	// produced nothing).
	if len(runs) != 1 || runs[0].Kind != console.BaselineRunCompact || runs[0].Tables != 1 || runs[0].Trigger != "" {
		t.Fatalf("history = %+v", runs)
	}
	if run, _ := sup.history.LastScheduled("s"); run != nil {
		t.Fatalf("the compaction became the last scheduled run: %+v", run)
	}
	sup.mu.Lock()
	busy := sup.busyLocked("s")
	sup.mu.Unlock()
	if busy {
		t.Fatal("the finished job still holds the slot")
	}
	// Staged already: a second look starts nothing. TriggerCompact marks
	// the slot before maybeCompact returns, so waitCompact would wait out
	// a second run if one had started; the count is read after it.
	sup.maybeCompact(req)
	waitCompact(t, sup, "s")
	if calls := cs.Calls(); len(calls) != 1 {
		t.Fatalf("merges = %v, want no second merge while the result waits", calls)
	}
}

func TestMaybeCompact_leavesShortChainsAndBusyServersAlone(t *testing.T) {
	// Every "nothing started" below is synchronous: TriggerCompact marks
	// the slot "running" before maybeCompact returns, so an idle status
	// after the call is proof, with no sleep to false-pass on a slow box.
	sup, req, cs, _ := compactRig(t, compactMinPairs-1)
	sup.maybeCompact(req)
	if calls := cs.Calls(); len(calls) != 0 || sup.compactStatusFor("s").State != "idle" {
		t.Fatalf("a chain one pair short was compacted: %v", calls)
	}
	// Long enough, but the server is busy (a full backup running): not
	// started, and said at Info; the next refresh tries again.
	sup2, req2, cs2, _ := compactRig(t, compactMinPairs)
	sup2.jobs["s"] = &console.BaselineStatus{State: "running"}
	logs := captureSlogFor(t)
	sup2.maybeCompact(req2)
	if calls := cs2.Calls(); len(calls) != 0 || sup2.compactStatusFor("s").State != "idle" || !strings.Contains(logs.String(), "the server is busy") {
		t.Fatalf("calls=%v log=%q", calls, logs.String())
	}
	// Deltas off, or no local directory: nothing is even listed.
	off := req2
	off.TableDeltas = false
	sup2.jobs["s"] = &console.BaselineStatus{State: "succeeded"}
	sup2.maybeCompact(off)
	if calls := cs2.Calls(); len(calls) != 0 || sup2.compactStatusFor("s").State != "idle" {
		t.Fatal("a server with deltas off was compacted")
	}
}

func TestCompactJob_failureAndPanicFreeTheSlotAndStageNothing(t *testing.T) {
	sup, req, cs, stage := compactRig(t, compactMinPairs)
	cs.err = errors.New("duckdb: out of memory")
	sup.maybeCompact(req)
	st := waitCompact(t, sup, "s")
	if st.State != "failed" || !strings.Contains(st.LastError, "out of memory") || st.Refused != 1 {
		t.Fatalf("status = %+v", st)
	}
	if _, err := os.Stat(stage); !os.IsNotExist(err) {
		t.Fatalf("a failed merge left its staging: %v", err)
	}
	if runs := sup.history.List("s"); len(runs) != 1 || runs[0].Error == "" {
		t.Fatalf("history = %+v, want the failure recorded", runs)
	}

	sup2, req2, cs2, stage2 := compactRig(t, compactMinPairs)
	cs2.panic = true
	captureSlogFor(t)
	sup2.maybeCompact(req2)
	st = waitCompact(t, sup2, "s")
	if st.State != "failed" || !strings.Contains(st.LastError, "internal error") {
		t.Fatalf("status after a panic = %+v", st)
	}
	sup2.mu.Lock()
	busy := sup2.busyLocked("s")
	sup2.mu.Unlock()
	if busy {
		t.Fatal("a panicked compaction wedged the server's slot")
	}
	// The stub panics before it writes anything, so this says only that
	// the guard did not certify an empty staging on its way out; what a
	// panic MID-WRITE leaves behind is the sweep's job, tested below.
	if _, err := os.Stat(filepath.Join(stage2, baseline.SuccessMarker)); err == nil {
		t.Fatal("a panicked merge was staged as complete")
	}
}

// TestCompactJob_partialFailureIsAFailedRunThatNamesTheReason: one chain
// merged and one not is recorded as failed with the count and the first
// reason; a silent "succeeded" would hide a chain retried at every refresh.
func TestCompactJob_partialFailureIsAFailedRunThatNamesTheReason(t *testing.T) {
	sup, req, cs, _ := compactRig(t, compactMinPairs)
	// A second table, "items", with its own long chain beside "orders".
	snapDir := filepath.Join(req.BaselineDir, reconstruct.SnapshotDirName(chainStart.Add(time.Hour)))
	both := map[string]*baseline.TableDeltaChain{}
	for base, c := range fakeChain(t, snapDir, compactMinPairs) {
		both[base] = c
		itemsBase := filepath.Join(filepath.Dir(base), "items.parquet")
		ic := &baseline.TableDeltaChain{}
		for _, f := range c.Files {
			p, u := baseline.TableDeltaPaths(itemsBase, f.Seq)
			for _, x := range []string{p, u} {
				if err := os.WriteFile(x, []byte("x"), 0o644); err != nil {
					t.Fatal(err)
				}
			}
			ic.Files = append(ic.Files, baseline.TableDeltaFile{Seq: f.Seq, SeqLo: f.SeqLo, Posdel: p, Upserts: u})
		}
		both[itemsBase] = ic
	}
	listSnapshotChains = func(context.Context, string) (map[string]*baseline.TableDeltaChain, error) { return both, nil }
	cs.err, cs.failTable = errors.New("duckdb: out of memory"), "orders"
	sup.maybeCompact(req)
	st := waitCompact(t, sup, "s")
	if st.State != "failed" || st.Tables != 1 || st.Refused != 1 || !strings.Contains(st.LastError, "1 of 2 chain(s) not merged") || !strings.Contains(st.LastError, "out of memory") {
		t.Fatalf("status = %+v", st)
	}
	if runs := sup.history.List("s"); len(runs) != 1 || runs[0].Tables != 1 || !strings.Contains(runs[0].Error, "not merged") {
		t.Fatalf("history = %+v", runs)
	}
	stagedItems := reconstruct.CompactionDir(compactDirFor(req.BaselineDir), "shop", "items", chainStart)
	if _, err := os.Stat(filepath.Join(stagedItems, baseline.SuccessMarker)); err != nil {
		t.Fatal("the chain that did merge was not staged")
	}
}

func TestBusyLocked_countsARunningCompaction(t *testing.T) {
	sup := newBaselineSupervisor(context.Background(), t.TempDir(), baseline.DefaultLockMode)
	sup.compacts["s"] = &console.BaselineStatus{State: "running"}
	sup.mu.Lock()
	defer sup.mu.Unlock()
	if !sup.busyLocked("s") {
		t.Fatal("a running compaction does not hold the slot: a refresh could extend the chain it is merging")
	}
}

func TestSweepCompactStaging_removesUnfinishedKeepsComplete(t *testing.T) {
	local := t.TempDir()
	req := refreshRequest{BaselineDir: local, TableDeltas: true}
	done := reconstruct.CompactionDir(compactDirFor(local), "shop", "orders", chainStart)
	half := reconstruct.CompactionDir(compactDirFor(local), "shop", "items", chainStart)
	for _, d := range []string{done, half} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(d, "x.000000-000003.posdel"), []byte("r"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(done, baseline.SuccessMarker), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	sweepCompactStaging(req)
	if _, err := os.Stat(half); !os.IsNotExist(err) {
		t.Fatal("the unfinished compaction survived the sweep")
	}
	if _, err := os.Stat(filepath.Join(done, baseline.SuccessMarker)); err != nil {
		t.Fatal("the complete compaction was swept")
	}
	sweepCompactStaging(refreshRequest{}) // no directory: nothing to do, no panic
}
