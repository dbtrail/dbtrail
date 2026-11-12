package consoleapp

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// The count applied after each snapshot (#2678), on real folders: a job that
// publishes prunes its server's folder to the count when it ends; the prune
// stands still while a job reads older snapshots; a restore's result stays.

// countedServer registers one server with its own folder and a count, puts n
// old snapshots in the folder, and arms the after-publish prune the way
// startBaselinePruneLoop does, against the real PruneLocal.
func countedServer(t *testing.T, keep, n int) (reg *console.Registry, id, dir string, old []string) {
	t.Helper()
	reg = liveRegistry(t)
	dir = t.TempDir()
	e, err := reg.Add(console.ServerEntry{Name: "shop", DSN: "u:p@tcp(h:3306)/a", BaselineDir: dir, LocalKeepNewest: keep})
	if err != nil {
		t.Fatal(err)
	}
	old = makeLocalSnapshots(t, dir, n)
	prev := pruneAfterPublishFn
	t.Cleanup(func() { pruneAfterPublishFn = prev })
	pruneAfterPublishFn = func(d string) {
		pruneFolderToCount(context.Background(), reg, "", "", "", d, baseline.PruneLocal)
	}
	return reg, e.ID, dir, old
}

func waitSnapshots(t *testing.T, dir string, want int) []string {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		got := snapshotsIn(t, dir)
		if len(got) == want {
			return got
		}
		if time.Now().After(deadline) {
			t.Fatalf("the folder holds %v, want %d snapshots", got, want)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// The daemon arms the prune exactly where its cleanup loop runs.
func TestCountAfterPublish_armedWithTheCleanupLoop(t *testing.T) {
	prev, prevCut := pruneAfterPublishFn, s3PruneCutFn
	t.Cleanup(func() { pruneAfterPublishFn, s3PruneCutFn = prev, prevCut })
	done, cancel := context.WithCancel(context.Background())
	cancel() // no startup sweep, no ticker: only the wiring
	pruneAfterPublishFn = nil
	if err := startBaselinePruneLoop(done, nil, "", "", "", time.Hour); err != nil {
		t.Fatal(err)
	}
	if pruneAfterPublishFn != nil {
		t.Fatal("armed on a daemon whose cleanup loop does not run (no registry, no age)")
	}
	if err := startBaselinePruneLoop(done, liveRegistry(t), "", "", "", time.Hour); err != nil {
		t.Fatal(err)
	}
	if pruneAfterPublishFn == nil {
		t.Fatal("a daemon with a registry runs the cleanup loop and did not arm the prune after a publish")
	}
}

// An update publishes a snapshot: the folder is at its count when the job's
// goroutine ends, with no hourly cleanup in between.
func TestCountAfterPublish_anUpdate(t *testing.T) {
	_, id, dir, old := countedServer(t, 2, 4)
	stubGateReads(t)
	injectFold(t, 0, nil)
	sup := newBaselineSupervisor(context.Background(), t.TempDir(), baseline.DefaultLockMode)
	sup.history = openHistoryForTest(t)
	if _, err := sup.TriggerRefresh(refreshRequest{ServerID: id, ServerName: "shop", IndexDSN: "d", BaselineDir: dir}, time.Hour); err != nil {
		t.Fatal(err)
	}
	sup.postRefresh.Wait()
	got := snapshotsIn(t, dir)
	if len(got) != 2 || got[0] != old[3] {
		t.Fatalf("after the update the folder holds %v, want the newest old one (%s) and the new one", got, old[3])
	}
	if rec, ok, err := baseline.ReadLastPrune(dir); err != nil || !ok || rec.Removed != 3 {
		t.Fatalf("the record the page reads: %+v ok=%v err=%v, want 3 removed", rec, ok, err)
	}
}

// A full read publishes a snapshot: same.
func TestCountAfterPublish_aFullRead(t *testing.T) {
	_, id, dir, old := countedServer(t, 2, 4)
	sup := newBaselineSupervisor(context.Background(), t.TempDir(), baseline.DefaultLockMode)
	sup.history = openHistoryForTest(t)
	at := time.Now().UTC().Truncate(time.Second)
	var during string
	sup.produce = func(req console.BaselineRequest) (dumpOutcome, error) {
		during = baseline.Reading(req.LocalDir)
		return dumpOutcomeAt(t, req.LocalDir, at), nil
	}
	if err := sup.Trigger(console.BaselineRequest{ServerID: id, ServerName: "shop", LocalDir: dir, IndexDSN: "d"}); err != nil {
		t.Fatal(err)
	}
	got := waitSnapshots(t, dir, 2)
	if got[0] != old[3] || got[1] != reconstruct.SnapshotDirName(at) {
		t.Fatalf("after the full read the folder holds %v", got)
	}
	if during == "" {
		t.Fatal("the cleanup was not stopped while the full read ran")
	}
}

// A folder with no count of its own (the daemon's --baseline-dir, a shared
// one, a server without a count) is not pruned after a publish.
func TestCountAfterPublish_onlyAFolderWithACount(t *testing.T) {
	reg := liveRegistry(t)
	plain := t.TempDir()
	if _, err := reg.Add(console.ServerEntry{Name: "plain", DSN: "u:p@tcp(h:3306)/a", BaselineDir: plain}); err != nil {
		t.Fatal(err)
	}
	names := makeLocalSnapshots(t, plain, 4)
	pruneFolderToCount(context.Background(), reg, "", "", "", plain, baseline.PruneLocal)
	if got := snapshotsIn(t, plain); !slices.Equal(got, names) {
		t.Fatalf("a server with no count lost snapshots: %v", got)
	}
	global := t.TempDir()
	if _, err := reg.Add(console.ServerEntry{Name: "own", DSN: "u:p@tcp(h:3306)/b", BaselineDir: global, LocalKeepNewest: 1}); err != nil {
		t.Fatal(err)
	}
	gnames := makeLocalSnapshots(t, global, 3)
	pruneFolderToCount(context.Background(), reg, global, "", "", global, baseline.PruneLocal)
	if got := snapshotsIn(t, global); !slices.Equal(got, gnames) {
		t.Fatalf("the daemon's own folder was pruned to a server's count: %v", got)
	}
}

// With an S3 destination the prune after a publish is the cleanup's own:
// one question per candidate, the newest never asked about, and a bucket
// that does not answer removes nothing.
func TestCountAfterPublish_withS3AsksOncePerCandidate(t *testing.T) {
	reg := liveRegistry(t)
	dir := t.TempDir()
	if _, err := reg.Add(console.ServerEntry{Name: "shop", DSN: "u:p@tcp(h:3306)/a", BaselineDir: dir, BaselineS3: "s3://bucket/shop/", LocalKeepNewest: 2}); err != nil {
		t.Fatal(err)
	}
	names := makeLocalSnapshots(t, dir, 4)
	asked := map[string]int{}
	var seen []baseline.PruneOptions
	silent := false
	pruneFn := func(ctx context.Context, o baseline.PruneOptions) (baseline.PruneResult, error) {
		seen = append(seen, o)
		return baseline.PruneLocalAsking(ctx, o, func(_ context.Context, name string) (bool, error) {
			asked[name]++
			if silent {
				return false, os.ErrDeadlineExceeded
			}
			return true, nil
		})
	}
	silent = true
	pruneFolderToCount(context.Background(), reg, "", "", "", dir, pruneFn)
	if got := snapshotsIn(t, dir); !slices.Equal(got, names) {
		t.Fatalf("a silent bucket: the folder holds %v, want all of %v", got, names)
	}
	clear(asked)
	silent = false
	pruneFolderToCount(context.Background(), reg, "", "", "", dir, pruneFn)
	if got := snapshotsIn(t, dir); !slices.Equal(got, names[2:]) {
		t.Fatalf("the folder holds %v, want %v", got, names[2:])
	}
	if len(asked) != 2 || asked[names[0]] != 1 || asked[names[1]] != 1 {
		t.Fatalf("the bucket was asked %v: want the two past the count, once each", asked)
	}
	for _, o := range seen {
		if o.S3URL != "s3://bucket/shop/" || o.KeepNewest != 2 {
			t.Fatalf("pruned with %+v: the destination and the count must be the server's", o)
		}
	}
}

// restoreInto runs a restore to `at` on a supervisor whose fold writes the
// snapshot and, when hold is given, waits on it first.
func restoreInto(t *testing.T, id, dir string, at time.Time, hold chan struct{}, foldErr error) *baselineSupervisor {
	t.Helper()
	realList, realFold := snapshotAt, foldTables
	t.Cleanup(func() { snapshotAt, foldTables = realList, realFold })
	snapshotAt = func(context.Context, string, time.Time) ([]string, time.Time, error) {
		return []string{"shop.orders"}, at.Add(-time.Hour), nil
	}
	foldTables = func(_ context.Context, cfg reconstruct.FullTableConfig) ([]*reconstruct.TableReport, []reconstruct.TableFailure, error) {
		if hold != nil {
			<-hold
		}
		if foldErr != nil {
			return nil, nil, foldErr
		}
		snap := filepath.Join(cfg.OutputDir, reconstruct.SnapshotDirName(cfg.At))
		if err := os.MkdirAll(filepath.Join(snap, "shop"), 0o700); err != nil {
			return nil, nil, err
		}
		if err := os.WriteFile(filepath.Join(snap, "shop", "orders.parquet"), []byte("rows"), 0o600); err != nil {
			return nil, nil, err
		}
		return nil, nil, baseline.WriteSuccessMarker(snap)
	}
	sup := newBaselineSupervisor(context.Background(), t.TempDir(), baseline.DefaultLockMode)
	sup.history = openHistoryForTest(t)
	if err := sup.TriggerRestore(console.BaselineRestoreRequest{ServerID: id, ServerName: "shop", IndexDSN: "d", BaselineDir: dir, At: at}); err != nil {
		t.Fatal(err)
	}
	return sup
}

// The defect, and the fix. A restore publishes a snapshot named for a past
// moment into a folder with a count and no S3. Without the mark the next
// cleanup removes it; with the mark the restore leaves, it stays, the
// folder's own newest stay too, and the next restore's result replaces it.
func TestCountAfterPublish_aRestoreKeepsItsResult(t *testing.T) {
	reg, id, dir, old := countedServer(t, 2, 4)
	// Between the oldest two: past the count by its name.
	at := time.Now().UTC().Add(-7 * 24 * time.Hour).Truncate(time.Second)
	result := reconstruct.SnapshotDirName(at)

	sup := restoreInto(t, id, dir, at, nil, nil)
	waitForTerminalState(t, func() console.BaselineStatus { return sup.RestoreStatus(id) })
	got := waitSnapshots(t, dir, 3)
	want := []string{result, old[2], old[3]}
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Fatalf("after the restore the folder holds %v, want the newest two and the restore's result %v", got, want)
	}
	t.Logf("with the mark: the folder holds %v after the restore and its cleanup (count 2, restore's result %s)", got, result)

	// The same folder with the mark taken away is the defect: the cleanup
	// removes the restore's result.
	twin := t.TempDir()
	if _, err := reg.Add(console.ServerEntry{Name: "twin", DSN: "u:p@tcp(h:3306)/b", BaselineDir: twin, LocalKeepNewest: 2}); err != nil {
		t.Fatal(err)
	}
	for _, n := range got {
		writeSnapshotFiles(t, filepath.Join(twin, n), baseline.SuccessMarker)
	}
	pruneFolderToCount(context.Background(), reg, "", "", "", twin, baseline.PruneLocal)
	left := snapshotsIn(t, twin)
	t.Logf("without the mark: the folder holds %v after the cleanup", left)
	if slices.Contains(left, result) {
		t.Fatalf("the defect did not reproduce: an unmarked snapshot named for a past moment survived the count: %v", left)
	}

	// The hourly cleanup keeps it as well.
	baselinePruneSweep(context.Background(), reg, "", "", "", baseline.PruneLocal)
	if got := snapshotsIn(t, dir); !slices.Contains(got, result) {
		t.Fatalf("the hourly cleanup removed the restore's result: %v", got)
	}

	// A second restore replaces the first one's result.
	at2 := at.Add(-24 * time.Hour)
	// The mark's time has one-second steps on some filesystems.
	time.Sleep(1100 * time.Millisecond)
	sup2 := restoreInto(t, id, dir, at2, nil, nil)
	waitForTerminalState(t, func() console.BaselineStatus { return sup2.RestoreStatus(id) })
	deadline := time.Now().Add(10 * time.Second)
	for slices.Contains(snapshotsIn(t, dir), result) {
		if time.Now().After(deadline) {
			t.Fatalf("the first restore's result is still there after a second restore: %v", snapshotsIn(t, dir))
		}
		time.Sleep(20 * time.Millisecond)
	}
	if got := snapshotsIn(t, dir); !slices.Contains(got, reconstruct.SnapshotDirName(at2)) || len(got) != 3 {
		t.Fatalf("after the second restore the folder holds %v", got)
	}
}

// While a restore runs, the cleanup of its folder removes nothing; when it
// ends, the count is applied. The same when the restore fails.
func TestCountAfterPublish_theCleanupWaitsForARestore(t *testing.T) {
	for _, outcome := range []string{"ends", "fails"} {
		t.Run(outcome, func(t *testing.T) {
			reg, id, dir, old := countedServer(t, 1, 4)
			at := time.Now().UTC().Add(-7 * 24 * time.Hour).Truncate(time.Second)
			hold := make(chan struct{})
			var once sync.Once
			release := func() { once.Do(func() { close(hold) }) }
			t.Cleanup(release) // a failed assertion must not leave the fold waiting
			var foldErr error
			if outcome == "fails" {
				foldErr = os.ErrInvalid
			}
			sup := restoreInto(t, id, dir, at, hold, foldErr)
			// Both cleanups, while the restore is on.
			pruneFolderToCount(context.Background(), reg, "", "", "", dir, baseline.PruneLocal)
			baselinePruneSweep(context.Background(), reg, "", "", "", baseline.PruneLocal)
			if got := snapshotsIn(t, dir); !slices.Equal(got, old) {
				t.Fatalf("the cleanup removed snapshots under a running restore: %v, want %v", got, old)
			}
			if what := baseline.Reading(dir); !strings.Contains(what, "restore") {
				t.Fatalf("the folder's reader is %q, want the restore", what)
			}
			release()
			waitForTerminalState(t, func() console.BaselineStatus { return sup.RestoreStatus(id) })
			want := 2 // the newest, and the restore's result
			if outcome == "fails" {
				want = 1
			}
			got := waitSnapshots(t, dir, want)
			if !slices.Contains(got, old[3]) {
				t.Fatalf("after the restore the folder holds %v, want the newest old one in it", got)
			}
			if what := baseline.Reading(dir); what != "" {
				t.Fatalf("the restore is over and %q still stops the cleanup", what)
			}
		})
	}
}

// A .sql build stops the cleanup of the folder it reads from the moment it
// is asked for until it ends, and no longer.
func TestCountAfterPublish_theCleanupWaitsForASQLBuild(t *testing.T) {
	at := time.Date(2026, 6, 10, 11, 0, 0, 0, time.UTC)
	src := writeSQLExportBaseline(t, at.Add(-time.Hour))
	sup := newBaselineSupervisor(context.Background(), t.TempDir(), "")
	if err := sup.TriggerSQLExport(console.SQLExportRequest{ServerID: "srv1", ServerName: "wp",
		IndexDSN: "i:p@tcp(127.0.0.1:1)/idx", BaselineSrc: src, At: at}); err != nil {
		t.Fatal(err)
	}
	// The build cannot write its outcome while this is held, so it is still
	// running here whatever it did so far.
	sup.mu.Lock()
	what := baseline.Reading(src)
	state := sup.exports["srv1"].State
	sup.mu.Unlock()
	if state != "running" || !strings.Contains(what, ".sql build") {
		t.Fatalf("while the build ran (state %q) the folder's reader was %q", state, what)
	}
	waitForTerminalState(t, func() console.BaselineStatus { return sup.SQLExportStatus("srv1") })
	deadline := time.Now().Add(10 * time.Second)
	for baseline.Reading(src) != "" {
		if time.Now().After(deadline) {
			t.Fatalf("the build is over and %q still stops the cleanup", baseline.Reading(src))
		}
		time.Sleep(20 * time.Millisecond)
	}
}
