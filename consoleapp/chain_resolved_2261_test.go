package consoleapp

import (
	"context"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/rotation"
)

func stubResolvedCurrent(t *testing.T, current bool) *atomic.Int32 {
	t.Helper()
	var asked atomic.Int32
	prev := resolvedCurrent
	t.Cleanup(func() { resolvedCurrent = prev })
	resolvedCurrent = func(string, *baseline.TableDeltaChain) bool {
		asked.Add(1)
		return current
	}
	return &asked
}

func chainOf(pairs int) *baseline.TableDeltaChain {
	c := &baseline.TableDeltaChain{}
	for i := range pairs {
		c.Files = append(c.Files, baseline.TableDeltaFile{Seq: i, SeqLo: i})
	}
	return c
}

// Where no resolved pair is ever written, no table is answered for (#2261):
// the refresh keeps every chain at the ordinary line.
func TestChainResolved_nilWhereNoPairIsWritten(t *testing.T) {
	sup := newBaselineSupervisor(context.Background(), t.TempDir(), baseline.DefaultLockMode)
	local := t.TempDir()
	for name, req := range map[string]refreshRequest{
		"no folder":       {ServerID: "s"},
		"snapshots on S3": {ServerID: "s", BaselineDir: "s3://bucket/prefix"},
	} {
		if sup.chainResolved(req) != nil {
			t.Errorf("%s: a table would be let past the line", name)
		}
	}
	if sup.chainResolved(refreshRequest{ServerID: "s", BaselineDir: local}) == nil {
		t.Error("a local folder with table deltas on: nobody answers for its tables")
	}
	// A folder another server writes too: maybeResolve writes nothing there.
	reg := testRegistryWithEntries(t,
		console.ServerEntry{Name: "s", DSN: "dsn-s", BaselineDir: local},
		console.ServerEntry{Name: "t", DSN: "dsn-t", BaselineDir: local},
	)
	sup.reg = reg
	if sup.chainResolved(refreshRequest{ServerID: reg.List()[0].ID, BaselineDir: local}) != nil {
		t.Error("a shared folder: a table would be let past the line")
	}
}

// A table counts when its chain's pair is current, or while a pass is
// writing this folder's pairs; a chain no pair is written for never does.
func TestChainResolved_pairOrPassInFlight(t *testing.T) {
	sup := newBaselineSupervisor(context.Background(), t.TempDir(), baseline.DefaultLockMode)
	local := t.TempDir()
	base := filepath.Join(local, "snap", "shop", "orders.parquet")
	ask := sup.chainResolved(refreshRequest{ServerID: "s", BaselineDir: local + string(filepath.Separator) + "."})

	asked := stubResolvedCurrent(t, true)
	if !ask(base, chainOf(2)) {
		t.Error("a current pair: not counted")
	}
	for name, c := range map[string]*baseline.TableDeltaChain{
		"no chain": nil, "one pair": chainOf(1), "a legacy pair": {Legacy: true, Files: chainOf(2).Files},
	} {
		n := asked.Load()
		if ask(base, c) {
			t.Errorf("%s: counted, and no pair is written for it", name)
		}
		if asked.Load() != n {
			t.Errorf("%s: the disk was read for a chain that has no pair", name)
		}
	}

	stubResolvedCurrent(t, false)
	if ask(base, chainOf(2)) {
		t.Error("no pair and no pass: counted")
	}
	sup.mu.Lock()
	sup.resolving[resolveFolder(local)] = true
	sup.mu.Unlock()
	if !ask(base, chainOf(2)) {
		t.Error("a pass is writing this folder's pairs: the table was held to the ordinary line")
	}
	if ask(base, chainOf(1)) {
		t.Error("a pass in flight writes no pair for a chain of one")
	}
	sup.mu.Lock()
	delete(sup.resolving, resolveFolder(local))
	sup.resolving[resolveFolder(t.TempDir())] = true // another folder's pass
	sup.mu.Unlock()
	if ask(base, chainOf(2)) {
		t.Error("another folder's pass counted for this one")
	}
}

// What watch wires reaches the fold: half the console's refusal line for a
// table read through its pair, read live, and someone to ask (#2261). With
// no console wired there is neither.
func TestRunRefresh_foldIsHandedTheResolvedLine(t *testing.T) {
	folds := probeFolds(t)
	stubCoverage(t, false, true)
	stubLiveFloor(t, refreshAt.Add(-40*time.Minute), true)
	stubReadsFrom(t, refreshAt.Add(-10*time.Minute), nil)
	stubIndexRetention(t, rotation.Effective{Retain: 48 * time.Hour, Raw: "48h", Source: rotation.RetainRecorded})
	sup, _, run := deltasRig(t, true)

	run(refreshAt)
	srv := &console.Server{}
	wireSQLChainLine(sup, srv)
	run(refreshAt.Add(5 * time.Minute))

	cfgs := folds.all()
	if len(cfgs) != 2 {
		t.Fatalf("folded %d time(s), want 2", len(cfgs))
	}
	if cfgs[0].MaxResolvedChainUpserts != 0 || cfgs[0].ChainResolved != nil {
		t.Errorf("no console wired: resolved line %d, someone to ask %v", cfgs[0].MaxResolvedChainUpserts, cfgs[0].ChainResolved != nil)
	}
	if got, want := cfgs[1].MaxResolvedChainUpserts, srv.SQLChainLimit()/2; got != want || want != 192<<20 {
		t.Errorf("MaxResolvedChainUpserts = %d MiB, want %d (half the refusal line, 192 at the default memory)", got>>20, want>>20)
	}
	if cfgs[1].MaxChainUpserts != 24<<20 {
		t.Errorf("MaxChainUpserts = %d MiB, want 24: the ordinary line is unchanged", cfgs[1].MaxChainUpserts>>20)
	}
	if cfgs[1].ChainResolved == nil {
		t.Error("nobody to ask whether a table has its pair")
	}
}

// With table deltas off the fold is asked nothing about pairs.
func TestRunRefresh_deltasOff_noResolvedLine(t *testing.T) {
	folds := probeFolds(t)
	stubCoverage(t, false, true)
	stubLiveFloor(t, refreshAt.Add(-40*time.Minute), true)
	stubReadsFrom(t, refreshAt.Add(-10*time.Minute), nil)
	stubIndexRetention(t, rotation.Effective{Retain: 48 * time.Hour, Raw: "48h", Source: rotation.RetainRecorded})
	sup, _, run := deltasRig(t, false)
	wireSQLChainLine(sup, &console.Server{})
	run(refreshAt)
	cfgs := folds.all()
	if len(cfgs) != 1 {
		t.Fatalf("folded %d time(s), want 1", len(cfgs))
	}
	if cfgs[0].MaxResolvedChainUpserts != 0 || cfgs[0].ChainResolved != nil {
		t.Errorf("deltas off: resolved line %d, someone to ask %v", cfgs[0].MaxResolvedChainUpserts, cfgs[0].ChainResolved != nil)
	}
}

// A compaction made the refresh's own pass stand aside; when it ends, the
// pairs of the newest snapshot are written, not left to the next refresh
// (#2261: that refresh would find no pair and hold every table to the
// ordinary line).
func TestTriggerCompact_writesThePairsWhenItEnds(t *testing.T) {
	sup, req, _, _ := compactRig(t, compactMinPairs)
	t.Cleanup(sup.postRefresh.Wait)
	var merges atomic.Int32
	var compactingAtMerge atomic.Bool
	resolveTableDelta = func(context.Context, string, *baseline.TableDeltaChain, string) (bool, bool, error) {
		merges.Add(1)
		sup.mu.Lock()
		compactingAtMerge.Store(sup.compacts[req.ServerID].State == "running")
		sup.mu.Unlock()
		return true, false, nil
	}
	sup.maybeCompact(req)
	if st := waitCompact(t, sup, req.ServerID); st.State != "succeeded" {
		t.Fatalf("compaction: %+v", st)
	}
	sup.postRefresh.Wait()
	if merges.Load() == 0 {
		t.Fatal("no pair was written after the compaction ended")
	}
	if compactingAtMerge.Load() {
		t.Fatal("a pair was merged while the compaction was still running")
	}
}
