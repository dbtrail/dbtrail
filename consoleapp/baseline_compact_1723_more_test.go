package consoleapp

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/baselineintegrity"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// #1723, the daemon side after the test review: the manifest check with a
// REAL manifest, the wiring from a refresh to the job, a shutdown mid-run,
// the sweep with table deltas off, and the two skips in the candidate scan.

// TestCompactJob_refusesACorruptInputPair: with a manifest beside the
// snapshot, a pair whose bytes no longer match it stops the merge before it
// starts (the merge is the one path by which corrupt bytes could reach a
// file the next snapshot certifies as its own). The control half first: the
// same manifest over untouched files lets the merge run.
func TestCompactJob_refusesACorruptInputPair(t *testing.T) {
	sup, req, cs, stage := compactRig(t, compactMinPairs)
	snapDir := filepath.Join(req.BaselineDir, reconstruct.SnapshotDirName(chainStart.Add(time.Hour)))
	if err := baselineintegrity.WriteManifest(snapDir); err != nil {
		t.Fatal(err)
	}
	sup.maybeCompact(req)
	if st := waitCompact(t, sup, "s"); st.State != "succeeded" || len(cs.Calls()) != 1 {
		t.Fatalf("with a manifest over good files: status = %+v calls = %v", st, cs.Calls())
	}
	// The staged result gone (as if adopted) and one input pair rotten.
	if err := os.RemoveAll(stage); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(snapDir, "shop", "orders.000003.posdel"), []byte("y"), 0o644); err != nil {
		t.Fatal(err)
	}
	sup.maybeCompact(req)
	st := waitCompact(t, sup, "s")
	if st.State != "failed" || !strings.Contains(st.LastError, "pair 3") || !strings.Contains(st.LastError, baselineintegrity.ErrIntegrity.Error()) {
		t.Fatalf("status = %+v, want the run failed on pair 3's integrity", st)
	}
	if calls := cs.Calls(); len(calls) != 1 {
		t.Fatalf("merges = %v, want none over a corrupt input", calls)
	}
	if _, err := os.Stat(stage); !os.IsNotExist(err) {
		t.Fatalf("a refused merge left a staging directory: %v", err)
	}
}

// TestTriggerRefresh_runsTheCompactionAfterTheRefresh pins the wiring the
// unit tests bypass: the daemon's own entry point sweeps the unfinished
// staging at the top of the cycle and, once the refresh has released the
// slot, merges a long chain. Without this test the call in TriggerRefresh
// could be dropped and every other test here would stay green.
func TestTriggerRefresh_runsTheCompactionAfterTheRefresh(t *testing.T) {
	var folds atomic.Int32
	mark := indexMark{events: 100, schemaChanges: 7}
	stubIndexMark(t, &mark, true)
	countFolds(t, &folds)
	stubBucketListing(t)
	stubCoverage(t, true, true)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	root := stageBaselineRoot(t)
	cs := stubCompaction(t, fakeChain(t, filepath.Join(root, "2026-08-28T09-00-00Z"), compactMinPairs))
	sup := newBaselineSupervisor(ctx, t.TempDir(), baseline.DefaultLockMode)
	h, err := console.OpenBaselineHistory(filepath.Join(t.TempDir(), "h.json"))
	if err != nil {
		t.Fatal(err)
	}
	sup.history = h
	half := reconstruct.CompactionDir(compactDirFor(root), "shop", "items", chainStart)
	if err := os.MkdirAll(half, 0o755); err != nil {
		t.Fatal(err)
	}
	req := refreshRequest{ServerID: "s", ServerName: "s", IndexDSN: "d", BaselineDir: root, TableDeltas: true}
	if _, err := sup.TriggerRefresh(req, time.Minute); err != nil {
		t.Fatal(err)
	}
	ref := waitForTerminalState(t, func() console.BaselineStatus { return sup.RefreshStatus("s") })
	if ref.State != "succeeded" || folds.Load() != 1 {
		t.Fatalf("refresh = %+v folds = %d", ref, folds.Load())
	}
	st := waitCompact(t, sup, "s")
	if st.State != "succeeded" || st.Tables != 1 {
		t.Fatalf("compaction after the refresh = %+v", st)
	}
	if calls := cs.Calls(); len(calls) != 1 || calls[0] != "orders 0-14" {
		t.Fatalf("merges = %v", calls)
	}
	if _, err := os.Stat(half); !os.IsNotExist(err) {
		t.Fatal("the refresh cycle did not sweep the unfinished staging")
	}
	// After, not inside: the job claimed the slot once the refresh had
	// released it.
	since, _ := time.Parse(time.RFC3339, st.Since)
	finished, _ := time.Parse(time.RFC3339, ref.FinishedAt)
	if since.IsZero() || finished.IsZero() || since.Before(finished) {
		t.Fatalf("compaction since %s, refresh finished %s: the job ran inside the refresh", st.Since, ref.FinishedAt)
	}
}

// TestCompactJob_shutdownStopsAndSaysSo: the daemon stopping INSIDE a merge
// (the common shape: one chain due) ends the run with nothing staged, the
// status naming the shutdown, and NO run in the history: a red "not merged"
// entry for a clean restart would be a false alarm, and the next refresh
// looks at the chains again.
func TestCompactJob_shutdownStopsAndSaysSo(t *testing.T) {
	local := t.TempDir()
	snapDir := filepath.Join(local, reconstruct.SnapshotDirName(chainStart.Add(time.Hour)))
	writeSnapshotFiles(t, snapDir, baseline.SuccessMarker)
	chains := fakeChain(t, snapDir, compactMinPairs)
	for base, c := range chains {
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
		chains[itemsBase] = ic
	}
	stubCompaction(t, chains)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	var calls atomic.Int32
	// The first merge is where the daemon is told to stop.
	compactMinor = func(ctx context.Context, _ string, _ *baseline.TableDeltaChain, _, _ int, _ string) (*reconstruct.MinorCompaction, error) {
		calls.Add(1)
		cancel()
		return nil, ctx.Err()
	}
	sup := newBaselineSupervisor(ctx, t.TempDir(), baseline.DefaultLockMode)
	h, err := console.OpenBaselineHistory(filepath.Join(t.TempDir(), "h.json"))
	if err != nil {
		t.Fatal(err)
	}
	sup.history = h
	req := refreshRequest{ServerID: "s", ServerName: "s", BaselineDir: local, TableDeltas: true}
	sup.maybeCompact(req)
	st := waitCompact(t, sup, "s")
	if st.State != "failed" || st.Tables != 0 || st.Refused != 2 || !strings.Contains(st.LastError, "stopped by daemon shutdown after 0 of 2") {
		t.Fatalf("status = %+v", st)
	}
	if calls.Load() != 1 {
		t.Fatalf("merges tried = %d, want 1: the loop went on after the stop", calls.Load())
	}
	for _, table := range []string{"items", "orders"} {
		if _, err := os.Stat(reconstruct.CompactionDir(compactDirFor(local), "shop", table, chainStart)); !os.IsNotExist(err) {
			t.Fatalf("%s: a stopped run left a staging directory", table)
		}
	}
	if runs := sup.history.List("s"); len(runs) != 0 {
		t.Fatalf("history = %+v, want a shutdown recorded nowhere but the log", runs)
	}
}

// TestCompactJob_repeatedFailureIsOneRecord: the same failure at two
// refreshes is one history record whose end moves; a success after it is
// a new record. (A 40-entry history at a 5-minute interval would otherwise
// be all compaction failures within four hours.)
func TestCompactJob_repeatedFailureIsOneRecord(t *testing.T) {
	sup, req, cs, _ := compactRig(t, compactMinPairs)
	cs.err = errors.New("duckdb: out of memory")
	run := func() console.BaselineStatus {
		t.Helper()
		sup.maybeCompact(req)
		return waitCompact(t, sup, "s")
	}
	run()
	first := sup.history.List("s")
	if len(first) != 1 || first[0].Error == "" {
		t.Fatalf("history after one failure = %+v", first)
	}
	run()
	second := sup.history.List("s")
	if len(second) != 1 || second[0].Error != first[0].Error {
		t.Fatalf("history after the same failure twice = %+v, want one record (its end moving is pinned by TestAppendCompact)", second)
	}
	cs.err = nil
	if st := run(); st.State != "succeeded" {
		t.Fatalf("status = %+v", st)
	}
	if runs := sup.history.List("s"); len(runs) != 2 || runs[1].Error != "" || runs[1].Tables != 1 {
		t.Fatalf("history after a success = %+v, want the failure then the success", runs)
	}
}

// TestSweepCompactStaging_removesEverythingWhenDeltasAreOff: a complete
// result is the refresh's to adopt only on the delta path; with deltas off no
// refresh will ever look at it, so it goes with the rest.
func TestSweepCompactStaging_removesEverythingWhenDeltasAreOff(t *testing.T) {
	local := t.TempDir()
	done := reconstruct.CompactionDir(compactDirFor(local), "shop", "orders", chainStart)
	if err := os.MkdirAll(done, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(done, baseline.SuccessMarker), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	sweepCompactStaging(refreshRequest{BaselineDir: local, TableDeltas: false})
	if _, err := os.Stat(compactDirFor(local)); !os.IsNotExist(err) {
		t.Fatalf("with table deltas off the staging root survived: %v", err)
	}
}

// TestMaybeCompact_skipsLegacyChainsAndEmptySnapshots: a legacy pair has no
// sequences to merge, and a snapshot with no chains starts nothing. Both are
// synchronous: TriggerCompact marks the slot before maybeCompact returns.
func TestMaybeCompact_skipsLegacyChainsAndEmptySnapshots(t *testing.T) {
	sup, req, cs, _ := compactRig(t, compactMinPairs)
	legacy := map[string]*baseline.TableDeltaChain{}
	for base, c := range fakeChain(t, filepath.Join(req.BaselineDir, reconstruct.SnapshotDirName(chainStart.Add(time.Hour))), compactMinPairs) {
		legacy[base] = &baseline.TableDeltaChain{Legacy: true, Files: c.Files}
	}
	for name, chains := range map[string]map[string]*baseline.TableDeltaChain{"legacy": legacy, "none": {}} {
		listSnapshotChains = func(context.Context, string) (map[string]*baseline.TableDeltaChain, error) { return chains, nil }
		sup.maybeCompact(req)
		if st := sup.compactStatusFor("s"); st.State != "idle" || len(cs.Calls()) != 0 {
			t.Fatalf("%s: status = %+v calls = %v, want nothing started", name, st, cs.Calls())
		}
	}
}
