package consoleapp

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// The resolved pairs job (#2231): after a refresh, every chain of the newest
// snapshot is merged into one pair beside the snapshots, and the pairs of
// snapshots that are no longer among the newest are removed.

type resolveCall struct{ table, prevBase string }

type resolveStub struct {
	mu        sync.Mutex
	calls     []resolveCall
	failTable string
	panic     bool
	// entered, when set, receives each call as it starts, and the call then
	// waits for release: a test looks at the supervisor while a merge runs.
	entered, release chan struct{}
}

func (rs *resolveStub) Calls() []resolveCall {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	return append([]resolveCall(nil), rs.calls...)
}

// resolveRig is a snapshots directory with the complete snapshots named by
// hours after chainStart, oldest first, the newest holding chains (table name
// to number of pairs), and the job's two seams replaced: the listing returns
// those chains, and the merge writes the pair's two files.
func resolveRig(t *testing.T, hours []int, chains map[string]int) (*baselineSupervisor, refreshRequest, *resolveStub, []string) {
	t.Helper()
	local := t.TempDir()
	var snaps []string
	for _, h := range hours {
		dir := filepath.Join(local, reconstruct.SnapshotDirName(chainStart.Add(time.Duration(h)*time.Hour)))
		writeSnapshotFiles(t, dir, baseline.SuccessMarker)
		snaps = append(snaps, dir)
	}
	newest := snaps[len(snaps)-1]
	listed := map[string]*baseline.TableDeltaChain{}
	for table, pairs := range chains {
		base := filepath.Join(newest, "shop", table+".parquet")
		c := &baseline.TableDeltaChain{}
		for i := range pairs {
			posdel, upserts := baseline.TableDeltaPaths(base, i)
			c.Files = append(c.Files, baseline.TableDeltaFile{Seq: i, SeqLo: i, Posdel: posdel, Upserts: upserts})
		}
		listed[base] = c
	}
	rs := &resolveStub{}
	prevList, prevResolve, prevCurrent := listSnapshotChains, resolveTableDelta, resolvedCurrent
	t.Cleanup(func() { listSnapshotChains, resolveTableDelta, resolvedCurrent = prevList, prevResolve, prevCurrent })
	listSnapshotChains = func(_ context.Context, snapDir string) (map[string]*baseline.TableDeltaChain, error) {
		if snapDir != newest {
			t.Errorf("chains listed for %s, want the newest snapshot %s", snapDir, newest)
		}
		return listed, nil
	}
	// The stub's pairs are not Parquet: whole by name is current here.
	resolvedCurrent = func(base string, c *baseline.TableDeltaChain) bool {
		_, ok := baseline.FindResolvedTableDelta(base, c.Files)
		return ok
	}
	resolveTableDelta = func(_ context.Context, base string, c *baseline.TableDeltaChain, prevBase string) (bool, bool, error) {
		table := strings.TrimSuffix(filepath.Base(base), ".parquet")
		rs.mu.Lock()
		rs.calls = append(rs.calls, resolveCall{table, prevBase})
		fail, boom := table == rs.failTable, rs.panic
		rs.mu.Unlock()
		if rs.entered != nil {
			rs.entered <- struct{}{}
			<-rs.release
		}
		if boom {
			panic("boom in the merge")
		}
		if fail {
			return false, false, errors.New("no room")
		}
		f, ok := baseline.ResolvedTableDeltaPaths(base, c.Files)
		if !ok {
			return false, false, nil
		}
		if err := os.MkdirAll(filepath.Dir(f.Upserts), 0o755); err != nil {
			return false, false, err
		}
		for _, p := range []string{f.Posdel, f.Upserts} {
			if err := os.WriteFile(p, []byte("r"), 0o644); err != nil {
				return false, false, err
			}
		}
		return true, false, nil
	}
	sup := newBaselineSupervisor(context.Background(), t.TempDir(), baseline.DefaultLockMode)
	return sup, refreshRequest{ServerID: "s", ServerName: "s", BaselineDir: local, TableDeltas: true}, rs, snaps
}

// resolvedSnapshots is the names under the resolved directory, sorted.
func resolvedSnapshots(t *testing.T, baselineDir string) []string {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(baselineDir, baseline.ResolvedDirName))
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	sort.Strings(names)
	return names
}

func TestMaybeResolve_mergesTheChainsOfTheNewestSnapshot(t *testing.T) {
	sup, req, rs, snaps := resolveRig(t, []int{1, 2}, map[string]int{"orders": 3, "items": 2, "quiet": 1})
	sup.maybeResolve(req)
	// A chain of one pair is read as it is. The others, in name order, each
	// with the same table's file in the snapshot before.
	want := []resolveCall{
		{"items", filepath.Join(snaps[0], "shop", "items.parquet")},
		{"orders", filepath.Join(snaps[0], "shop", "orders.parquet")},
	}
	if calls := rs.Calls(); !reflect.DeepEqual(calls, want) {
		t.Fatalf("merges = %+v, want %+v", calls, want)
	}
	if got := resolvedSnapshots(t, req.BaselineDir); !reflect.DeepEqual(got, []string{filepath.Base(snaps[1])}) {
		t.Fatalf("resolved pairs kept for %v, want the newest snapshot only", got)
	}
	// In no job slot: nothing a refresh, a full backup or a compaction would
	// have to wait for.
	sup.mu.Lock()
	busy, running := sup.busyLocked("s"), len(sup.resolving)
	sup.mu.Unlock()
	if busy || running != 0 {
		t.Fatalf("after the run: slot busy = %v, servers still marked as resolving = %d", busy, running)
	}
	// Written already: a second look merges nothing.
	sup.maybeResolve(req)
	if calls := rs.Calls(); len(calls) != 2 {
		t.Fatalf("a second look merged again: %+v", calls)
	}
}

// The first snapshot of a folder has none before it.
func TestMaybeResolve_theOnlySnapshotHasNoPrevious(t *testing.T) {
	sup, req, rs, _ := resolveRig(t, []int{1}, map[string]int{"orders": 2})
	sup.maybeResolve(req)
	if calls := rs.Calls(); !reflect.DeepEqual(calls, []resolveCall{{"orders", ""}}) {
		t.Fatalf("merges = %+v", calls)
	}
}

// The pairs of the two newest snapshots stay (a statement that started on
// the one before the newest is still reading its pair). Older ones, one
// whose snapshot retention removed, and one of a snapshot that is not
// complete are marked first and removed only once the mark is older than the
// wait: a statement names a pair when its views are generated and opens it
// later, and a pair removed in between fails that statement.
func TestMaybeResolve_keepsThePairsOfTheTwoNewestSnapshots(t *testing.T) {
	sup, req, _, snaps := resolveRig(t, []int{1, 2, 3}, map[string]int{"orders": 2})
	root := filepath.Join(req.BaselineDir, baseline.ResolvedDirName)
	incomplete := filepath.Join(req.BaselineDir, reconstruct.SnapshotDirName(chainStart.Add(4*time.Hour)))
	writeSnapshotFiles(t, incomplete, baseline.IncompleteMarker)
	gone := reconstruct.SnapshotDirName(chainStart.Add(-time.Hour))
	old := []string{filepath.Base(snaps[0]), gone, filepath.Base(incomplete)}
	for _, name := range append([]string{filepath.Base(snaps[1])}, old...) {
		if err := os.MkdirAll(filepath.Join(root, name, "shop"), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, name, "shop", "orders.000000-000001.upserts"), []byte("old"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	// A mark left on the pairs of a snapshot that is among the newest again.
	if err := os.WriteFile(filepath.Join(root, filepath.Base(snaps[1]), baseline.ResolvedLeavingMark), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	kept := []string{filepath.Base(snaps[1]), filepath.Base(snaps[2])}
	all := append(append([]string{}, kept...), old...)
	sort.Strings(all)

	sup.maybeResolve(req)
	if got := resolvedSnapshots(t, req.BaselineDir); !reflect.DeepEqual(got, all) {
		t.Fatalf("after the first look: pairs kept for %v, want all of %v still there", got, all)
	}
	for _, name := range old {
		if _, err := os.Stat(filepath.Join(root, name, baseline.ResolvedLeavingMark)); err != nil {
			t.Errorf("the pairs of %s are not marked as leaving: %v", name, err)
		}
		if _, err := os.Stat(filepath.Join(root, name, "shop", "orders.000000-000001.upserts")); err != nil {
			t.Errorf("the pair of %s went at once: %v", name, err)
		}
	}
	for _, name := range kept {
		if _, err := os.Stat(filepath.Join(root, name, baseline.ResolvedLeavingMark)); !os.IsNotExist(err) {
			t.Errorf("the pairs of %s, one of the two newest, are marked as leaving (stat err=%v)", name, err)
		}
	}
	// Within the wait nothing goes.
	sup.maybeResolve(req)
	if got := resolvedSnapshots(t, req.BaselineDir); !reflect.DeepEqual(got, all) {
		t.Fatalf("within the wait: pairs kept for %v, want %v", got, all)
	}
	// Past it, they do.
	past := time.Now().Add(-resolveSweepGrace - time.Minute)
	for _, name := range old {
		if err := os.Chtimes(filepath.Join(root, name, baseline.ResolvedLeavingMark), past, past); err != nil {
			t.Fatal(err)
		}
	}
	sup.maybeResolve(req)
	if got := resolvedSnapshots(t, req.BaselineDir); !reflect.DeepEqual(got, kept) {
		t.Fatalf("past the wait: pairs kept for %v, want %v", got, kept)
	}
}

// One table that cannot be merged does not stop the others. It is not tried
// again at the next refresh (each try checks every file of its chain and
// runs DuckDB), but once its wait is over, and a success clears the wait.
func TestMaybeResolve_aFailedTableWaitsBeforeItIsTriedAgain(t *testing.T) {
	sup, req, rs, _ := resolveRig(t, []int{1}, map[string]int{"a": 2, "b": 2, "c": 2})
	rs.failTable = "b"
	sup.maybeResolve(req)
	if calls := rs.Calls(); len(calls) != 3 {
		t.Fatalf("merges = %+v, want all three tables tried", calls)
	}
	rs.mu.Lock()
	rs.failTable = ""
	rs.mu.Unlock()
	sup.maybeResolve(req)
	if calls := rs.Calls(); len(calls) != 3 {
		t.Fatalf("the failed table was tried again at once: %+v", calls)
	}
	sup.mu.Lock()
	if len(sup.resolveRetry) != 1 {
		t.Fatalf("tables waiting = %v, want the failed one only", sup.resolveRetry)
	}
	for k, until := range sup.resolveRetry {
		if time.Until(until) < resolveRetryEvery/2 {
			t.Errorf("the wait ends at %s, want about %s from now", until, resolveRetryEvery)
		}
		sup.resolveRetry[k] = time.Now().Add(-time.Second)
	}
	sup.mu.Unlock()
	sup.maybeResolve(req)
	calls := rs.Calls()
	if len(calls) != 4 || calls[3].table != "b" {
		t.Fatalf("after the wait: merges = %+v, want only b tried again", calls)
	}
	sup.mu.Lock()
	waiting := len(sup.resolveRetry)
	sup.mu.Unlock()
	if waiting != 0 {
		t.Error("a merge that succeeded left the wait in place")
	}
}

// The pair is a second copy of the chain. Without room for it, on top of the
// margin every snapshot job keeps free, the merge does not start, and the
// table waits like any other that failed. With room for the margin and not
// for the chain it does not start either: the chain's size is what is asked.
func TestMaybeResolve_checksTheDiskBeforeItMerges(t *testing.T) {
	sup, req, rs, snaps := resolveRig(t, []int{1}, map[string]int{"orders": 2})
	// The chain's four files, 1 MiB each.
	base := filepath.Join(snaps[0], "shop", "orders.parquet")
	for i := range 2 {
		posdel, upserts := baseline.TableDeltaPaths(base, i)
		for _, p := range []string{posdel, upserts} {
			if err := os.WriteFile(p, make([]byte, 1<<20), 0o644); err != nil {
				t.Fatal(err)
			}
		}
	}
	prev := diskSpaceFn
	t.Cleanup(func() { diskSpaceFn = prev })
	free := uint64(foldDiskMargin + 2<<20) // the margin and half the chain
	diskSpaceFn = func(string) (uint64, uint64, error) { return free, 1 << 40, nil }
	sup.maybeResolve(req)
	if calls := rs.Calls(); len(calls) != 0 {
		t.Fatalf("a merge started with no room for its result: %+v", calls)
	}
	sup.mu.Lock()
	waiting := len(sup.resolveRetry)
	sup.resolveRetry = map[string]time.Time{}
	sup.mu.Unlock()
	if waiting != 1 {
		t.Fatalf("tables waiting after a refused merge = %d, want 1", waiting)
	}
	free = uint64(foldDiskMargin + 8<<20)
	sup.maybeResolve(req)
	if calls := rs.Calls(); len(calls) != 1 {
		t.Fatalf("with room for the chain: merges = %+v, want one", calls)
	}
}

// While a merge runs: the server's job slot is free, so a refresh, a full
// backup or a compaction that is due starts (each of them refuses on
// busyLocked and on nothing this job holds); a second look for the same
// folder, from this server or from another that shares it, returns at once
// instead of merging the same chain beside the first; and a server whose
// compaction is running stands aside.
func TestMaybeResolve_holdsNoSlotWhileItMerges(t *testing.T) {
	sup, req, rs, _ := resolveRig(t, []int{1}, map[string]int{"orders": 2})
	rs.entered, rs.release = make(chan struct{}), make(chan struct{})
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		sup.maybeResolve(req)
	}()
	<-rs.entered
	sup.mu.Lock()
	busy := sup.busyLocked("s")
	sup.mu.Unlock()
	if busy {
		t.Error("the server's slot is taken while its resolved pairs are written: a refresh due now would skip")
	}
	// The same folder, spelled another way, by another server.
	other := req
	other.ServerID, other.ServerName, other.BaselineDir = "t", "t", req.BaselineDir+string(filepath.Separator)+"."
	returned := make(chan struct{})
	go func() {
		defer close(returned)
		sup.maybeResolve(req)
		sup.maybeResolve(other)
	}()
	select {
	case <-returned:
	case <-rs.entered:
		t.Fatal("a second merge of the same folder started beside the first")
	case <-time.After(5 * time.Second):
		t.Fatal("a second look waited for the first instead of returning")
	}
	close(rs.release)
	rs.entered = nil
	<-finished
	if calls := rs.Calls(); len(calls) != 1 {
		t.Fatalf("merges = %+v, want one", calls)
	}
	sup.mu.Lock()
	running := len(sup.resolving)
	sup.mu.Unlock()
	if running != 0 {
		t.Fatal("the folder is still marked as being resolved after the run")
	}
}

func TestMaybeResolve_standsAsideForACompactionOfTheServer(t *testing.T) {
	sup, req, rs, _ := resolveRig(t, []int{1}, map[string]int{"orders": 2})
	sup.mu.Lock()
	sup.compacts["s"] = &console.BaselineStatus{State: "running"}
	sup.mu.Unlock()
	sup.maybeResolve(req)
	if calls := rs.Calls(); len(calls) != 0 {
		t.Fatalf("a merge ran beside the server's compaction: %+v", calls)
	}
	sup.mu.Lock()
	sup.compacts["s"].State = "succeeded"
	sup.mu.Unlock()
	sup.maybeResolve(req)
	if calls := rs.Calls(); len(calls) != 1 {
		t.Fatalf("after the compaction: merges = %+v, want one", calls)
	}
}

// The other direction: while the folder's pairs are being written, a
// compaction of the server does not start. It is not lost: the next refresh
// tries again, as for a busy server.
func TestTriggerCompact_standsAsideWhileTheFoldersPairsAreWritten(t *testing.T) {
	sup, req, cs, _ := compactRig(t, compactMinPairs)
	sup.mu.Lock()
	sup.resolving[resolveFolder(req.BaselineDir+string(filepath.Separator)+".")] = true
	sup.mu.Unlock()
	if err := sup.TriggerCompact(req, nil); !errors.Is(err, console.ErrBaselineRunning) {
		t.Fatalf("TriggerCompact beside a resolve = %v, want the busy refusal", err)
	}
	sup.maybeCompact(req)
	time.Sleep(50 * time.Millisecond) // a start would have launched a goroutine
	if calls := cs.Calls(); len(calls) != 0 {
		t.Fatalf("a compaction merged beside the resolved pairs job: %v", calls)
	}
	sup.mu.Lock()
	delete(sup.resolving, resolveFolder(req.BaselineDir))
	sup.mu.Unlock()
	sup.maybeCompact(req)
	if st := waitCompact(t, sup, "s"); st.State != "succeeded" {
		t.Fatalf("after the pairs were written: %+v", st)
	}
}

// The pairs are written into the folder, and a folder another server writes
// too may hold its chains (#1684): no pair is written there, as no
// compaction is. Given its own folder, the server's pairs are written.
func TestMaybeResolve_refusesASharedLocation(t *testing.T) {
	sup, req, rs, _ := resolveRig(t, []int{1}, map[string]int{"orders": 2})
	reg := testRegistryWithEntries(t,
		console.ServerEntry{Name: "s", DSN: "dsn-s", BaselineDir: req.BaselineDir},
		console.ServerEntry{Name: "t", DSN: "dsn-t", BaselineDir: req.BaselineDir},
	)
	sup.reg = reg
	req.ServerID = reg.List()[0].ID
	sup.maybeResolve(req)
	if calls := rs.Calls(); len(calls) != 0 {
		t.Fatalf("pairs written into a shared folder: %+v", calls)
	}
	other := reg.List()[1]
	other.BaselineDir = t.TempDir()
	if err := reg.Update(other); err != nil {
		t.Fatal(err)
	}
	sup.maybeResolve(req)
	if calls := rs.Calls(); len(calls) != 1 {
		t.Fatalf("alone in its folder: merges = %+v, want one", calls)
	}
}

// What is in the pairs' directory and is not a snapshot's directory was not
// put there by this job: it is left alone, without a warning at every
// refresh. And a mark dated in the future (the clock was set back) does not
// keep its pairs for as long as the clock was off: it is dated again, and
// the wait counts from there.
func TestSweepResolved_strayFilesAndAMarkFromTheFuture(t *testing.T) {
	root := filepath.Join(t.TempDir(), baseline.ResolvedDirName)
	old := filepath.Join(root, "2026-01-01T00-00-00Z")
	if err := os.MkdirAll(filepath.Join(old, "shop"), 0o755); err != nil {
		t.Fatal(err)
	}
	stray := filepath.Join(root, ".DS_Store")
	mark := filepath.Join(old, baseline.ResolvedLeavingMark)
	for _, f := range []string{stray, mark} {
		if err := os.WriteFile(f, nil, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	future := time.Now().Add(48 * time.Hour)
	if err := os.Chtimes(mark, future, future); err != nil {
		t.Fatal(err)
	}
	logs := captureSlogFor(t)
	sweepResolved(root, nil)
	if strings.Contains(logs.String(), "could not") {
		t.Errorf("the sweep warned about what it found: %s", logs.String())
	}
	if _, err := os.Stat(stray); err != nil {
		t.Errorf("a file the job did not write was removed: %v", err)
	}
	fi, err := os.Stat(mark)
	if err != nil {
		t.Fatalf("the pairs under a mark from the future went at once: %v", err)
	}
	if fi.ModTime().After(time.Now().Add(time.Minute)) {
		t.Fatalf("the mark is still dated %s: its pairs stay until the clock catches up", fi.ModTime())
	}
	past := time.Now().Add(-resolveSweepGrace - time.Minute)
	if err := os.Chtimes(mark, past, past); err != nil {
		t.Fatal(err)
	}
	sweepResolved(root, nil)
	if _, err := os.Stat(old); !os.IsNotExist(err) {
		t.Fatalf("the pairs are still there past the wait (stat err=%v)", err)
	}
	if _, err := os.Stat(stray); err != nil {
		t.Errorf("the stray file went with the last pairs: %v", err)
	}
}

// A refresh that lands while a run is writing returns at once and leaves its
// snapshot to that run: the run looks again when its pass is done and goes on
// with the snapshot that is the newest by then.
func TestMaybeResolve_goesOnWithASnapshotThatAppearedMeanwhile(t *testing.T) {
	sup, req, rs, snaps := resolveRig(t, []int{1}, map[string]int{"orders": 2})
	later := filepath.Join(req.BaselineDir, reconstruct.SnapshotDirName(chainStart.Add(2*time.Hour)))
	var listedFor []string
	var mu sync.Mutex
	listSnapshotChains = func(_ context.Context, snapDir string) (map[string]*baseline.TableDeltaChain, error) {
		mu.Lock()
		listedFor = append(listedFor, filepath.Base(snapDir))
		first := len(listedFor) == 1
		mu.Unlock()
		if first {
			// The next refresh publishes while this pass is at work.
			writeSnapshotFiles(t, later, baseline.SuccessMarker)
		}
		base := filepath.Join(snapDir, "shop", "orders.parquet")
		c := &baseline.TableDeltaChain{}
		for i := range 2 {
			posdel, upserts := baseline.TableDeltaPaths(base, i)
			c.Files = append(c.Files, baseline.TableDeltaFile{Seq: i, SeqLo: i, Posdel: posdel, Upserts: upserts})
		}
		return map[string]*baseline.TableDeltaChain{base: c}, nil
	}
	sup.maybeResolve(req)
	want := []string{filepath.Base(snaps[0]), filepath.Base(later)}
	if !reflect.DeepEqual(listedFor, want) {
		t.Fatalf("snapshots looked at = %v, want %v", listedFor, want)
	}
	if got := resolvedSnapshots(t, req.BaselineDir); !reflect.DeepEqual(got, want) {
		t.Fatalf("resolved pairs written for %v, want %v", got, want)
	}
	if calls := rs.Calls(); len(calls) != 2 || calls[1].prevBase != filepath.Join(snaps[0], "shop", "orders.parquet") {
		t.Fatalf("merges = %+v, want the second with the first snapshot as its previous", calls)
	}
}

// maybeResolve runs on the refresh's goroutine after the refresh's own guard
// has returned: a panic in it would end the process, which is also the
// capture. And the server must not stay marked as running.
func TestMaybeResolve_aPanicStaysInTheJob(t *testing.T) {
	sup, req, rs, _ := resolveRig(t, []int{1}, map[string]int{"orders": 2})
	rs.panic = true
	sup.maybeResolve(req) // must return
	sup.mu.Lock()
	running := len(sup.resolving)
	sup.mu.Unlock()
	if running != 0 {
		t.Fatal("the folder is still marked as being resolved after a panic: no later run would start")
	}
	rs.mu.Lock()
	rs.panic = false
	rs.mu.Unlock()
	sup.maybeResolve(req)
	if got := resolvedSnapshots(t, req.BaselineDir); len(got) != 1 {
		t.Fatalf("the run after the panic wrote nothing: %v", got)
	}
}

// With table deltas off no chain is extended, so no pair is written again
// and those there go, after the same wait as any other. And a server with no
// local snapshots, or snapshots on S3, has nothing to do.
func TestMaybeResolve_deltasOffRemovesThePairs(t *testing.T) {
	sup, req, rs, snaps := resolveRig(t, []int{1}, map[string]int{"orders": 2})
	sup.maybeResolve(req)
	if got := resolvedSnapshots(t, req.BaselineDir); len(got) != 1 {
		t.Fatalf("no pair written: %v", got)
	}
	root := filepath.Join(req.BaselineDir, baseline.ResolvedDirName)
	off := req
	off.TableDeltas = false
	sup.maybeResolve(off)
	mark := filepath.Join(root, filepath.Base(snaps[0]), baseline.ResolvedLeavingMark)
	if _, err := os.Stat(mark); err != nil {
		t.Fatalf("with table deltas off the pairs are not marked as leaving: %v", err)
	}
	past := time.Now().Add(-resolveSweepGrace - time.Minute)
	if err := os.Chtimes(mark, past, past); err != nil {
		t.Fatal(err)
	}
	sup.maybeResolve(off)
	if _, err := os.Stat(root); !os.IsNotExist(err) {
		t.Fatalf("the directory is still there with table deltas off and the wait over (stat err=%v)", err)
	}
	before := len(rs.Calls())
	for _, dir := range []string{"", "s3://bucket/snapshots"} {
		r := req
		r.BaselineDir = dir
		sup.maybeResolve(r)
	}
	if after := len(rs.Calls()); after != before {
		t.Fatalf("a server with no local snapshot directory ran %d merges", after-before)
	}
}

// The job with nothing replaced: the real listing and the real merge over a
// real chain (a snapshot with no manifest, so its files are not checked
// against one), and the state read through the pair it leaves is the state
// read through the chain.
func TestMaybeResolve_writesAPairAStatementCanRead(t *testing.T) {
	local := t.TempDir()
	snap := filepath.Join(local, reconstruct.SnapshotDirName(chainStart.Add(time.Hour)))
	base := filepath.Join(snap, "shop", "orders.parquet")
	cols, err := baseline.ParseSchemaText("CREATE TABLE `orders` (\n  `id` int NOT NULL,\n  `status` varchar(32) DEFAULT NULL,\n  PRIMARY KEY (`id`)\n);\n")
	if err != nil {
		t.Fatal(err)
	}
	w, err := baseline.NewWriter(base, cols, baseline.WriterConfig{Compression: "none", RowGroupSize: 100})
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range [][]string{{"1", "new"}, {"2", "paid"}, {"3", "held"}} {
		if err := w.WriteRow(r, []bool{false, false}); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	pair := func(seq int, dead []int64, rows ...[3]string) {
		t.Helper()
		md := map[string]string{
			baseline.MetaKeyBinlogFile: "b.1", baseline.MetaKeyBinlogPos: "9",
			baseline.MetaKeyDeltaChainStart: "2026-04-30T03:00:00Z",
			baseline.MetaKeyDeltaBaseAnchor: "b.1:1", baseline.MetaKeyDeltaBaseSize: "1",
		}
		err := baseline.WriteTableDeltaPair(base, seq, cols, md, dead, func(emit func([]string, []bool) error) error {
			for _, r := range rows {
				tomb := r[2] == baseline.TableDeltaOpDelete
				if err := emit([]string{r[0], r[1], r[0], r[2]}, []bool{tomb, tomb, false, false}); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	pair(0, []int64{0, 1}, [3]string{"1", "shipped", "u"}, [3]string{"2", "", "d"}, [3]string{"4", "new", "u"})
	pair(1, []int64{0}, [3]string{"1", "returned", "u"}, [3]string{"4", "", "d"}, [3]string{"5", "new", "u"})
	if err := os.WriteFile(filepath.Join(snap, baseline.SuccessMarker), nil, 0o644); err != nil {
		t.Fatal(err)
	}

	sup := newBaselineSupervisor(context.Background(), t.TempDir(), baseline.DefaultLockMode)
	sup.maybeResolve(refreshRequest{ServerID: "s", ServerName: "s", BaselineDir: local, TableDeltas: true})

	chain, err := baseline.ListTableDelta(context.Background(), base)
	if err != nil || chain == nil || len(chain.Files) != 2 {
		t.Fatalf("chain = %v, err = %v", chain, err)
	}
	r, ok := baseline.FindResolvedTableDelta(base, chain.Files)
	if !ok {
		t.Fatalf("the job left no resolved pair under %s", filepath.Join(local, baseline.ResolvedDirName))
	}
	lit := func(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }
	read := func(stateSQL string) []string {
		t.Helper()
		ddb, err := sql.Open("duckdb", "")
		if err != nil {
			t.Fatal(err)
		}
		defer ddb.Close()
		rows, err := ddb.Query("SELECT id::VARCHAR || '=' || status FROM (" + stateSQL + ") ORDER BY id")
		if err != nil {
			t.Fatalf("%v\n%s", err, stateSQL)
		}
		defer rows.Close()
		var out []string
		for rows.Next() {
			var s string
			if err := rows.Scan(&s); err != nil {
				t.Fatal(err)
			}
			out = append(out, s)
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		return out
	}
	want := []string{"1=returned", "3=held", "5=new"}
	if got := read(baseline.TableDeltaOnePairStateSQL(lit(base), lit(r.Posdel), lit(r.Upserts), "")); !reflect.DeepEqual(got, want) {
		t.Fatalf("state through the resolved pair = %v, want %v", got, want)
	}
	through := baseline.TableDeltaStateSQL(lit(base), "["+lit(chain.Files[0].Posdel)+", "+lit(chain.Files[1].Posdel)+"]",
		"["+lit(chain.Files[0].Upserts)+", "+lit(chain.Files[1].Upserts)+"]", base, "")
	if got := read(through); !reflect.DeepEqual(got, want) {
		t.Fatalf("state through the chain = %v, want %v: the fixture is not what the test says", got, want)
	}
}
