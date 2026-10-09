package consoleapp

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// #2212: a server whose snapshots go only to S3 (a bucket, no Local folder)
// gets scheduled UPDATES. The update is written into a folder of its own
// under the staging folder (<staging>/refresh-*/<snapshot>), uploaded to the
// bucket, and the run folder is deleted, whatever the outcome.
//
// The edge cases, written down before the code:
//   - two S3-only servers folding at once: distinct run folders, both gone after;
//   - a crash mid-run: the run folder is journaled, and the boot reclaim removes it;
//   - an upload failure: folder deleted, run failed, message promises no local copy,
//     and no full read stands in for it (it would hit the same bucket);
//   - the staging disk full: a disk refusal, not a crash, folder deleted;
//   - a server with BOTH a Local folder and S3: unchanged, the local path;
//   - S3-only with no previous snapshot: still a full read (internal/console tests);
//   - staging unusable: a refusal naming the folder, never a silent full read.

// stagedFoldStubs answers the bucket listing with one table and records what
// the loop uploaded. Safe for concurrent runs, unlike stubS3Fold.
type stagedFoldStubs struct {
	mu      sync.Mutex
	uploads []uploadCall
	// seenAtUpload: whether the snapshot directory existed when the upload
	// was asked for, per call.
	seenAtUpload []bool
}

func stubStagedFold(t *testing.T, uploadErr error) *stagedFoldStubs {
	t.Helper()
	stubGateReads(t)
	stubBucketListing(t)
	realList, realUpload := newestSnapshotTables, uploadSnapshot
	t.Cleanup(func() { newestSnapshotTables, uploadSnapshot = realList, realUpload })
	newestSnapshotTables = func(ctx context.Context, src string) (time.Time, []string, error) {
		if !strings.HasPrefix(src, "s3://") {
			return realList(ctx, src)
		}
		return time.Date(2026, 8, 28, 9, 0, 0, 0, time.UTC), []string{"shop.orders"}, nil
	}
	// The bucket the partial-upload cleanup sees: empty unless a case
	// stubs its own, and never the network.
	stubPartialStore(t, &fakePartialStore{})
	s := &stagedFoldStubs{}
	uploadSnapshot = func(_ context.Context, outputDir, dest, _ string, _ bool) (int, error) {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.uploads = append(s.uploads, uploadCall{outputDir, dest})
		s.seenAtUpload = append(s.seenAtUpload, dirExists(outputDir))
		if uploadErr != nil {
			return 0, uploadErr
		}
		return 1, nil
	}
	return s
}

func s3OnlyRequest(id string) refreshRequest {
	return refreshRequest{ServerID: id, ServerName: id, IndexDSN: "d", BaselineS3: "s3://bucket/" + id + "/"}
}

func stagingEntries(t *testing.T, staging string) []string {
	t.Helper()
	entries, err := os.ReadDir(staging)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return names
}

// The happy path, and the shape every other case starts from.
func TestStagedFold_S3OnlyServerUpdatesThroughTheStagingFolder(t *testing.T) {
	stubs := stubStagedFold(t, nil)
	f := newJobsFixture(t)
	sup := f.supervisor(t)
	var cfgSeen reconstruct.FullTableConfig
	prev := foldTables
	t.Cleanup(func() { foldTables = prev })
	foldTables = func(_ context.Context, cfg reconstruct.FullTableConfig) ([]*reconstruct.TableReport, []reconstruct.TableFailure, error) {
		cfgSeen = cfg
		writeSnapshotFiles(t, filepath.Join(cfg.OutputDir, reconstruct.SnapshotDirName(cfg.At)), baseline.SuccessMarker)
		return nil, nil, nil
	}
	req := s3OnlyRequest("s")
	sup.refreshes["s"] = &console.BaselineStatus{State: "running"}
	sup.runRefresh(req, refreshAt, time.Minute)

	st := sup.RefreshStatus("s")
	if st.State != "succeeded" || !st.Published {
		t.Fatalf("status = %+v, want a published success", st)
	}
	if cfgSeen.BaselineSrc != "s3://bucket/s/" {
		t.Errorf("the fold read %q, want the bucket", cfgSeen.BaselineSrc)
	}
	if filepath.Dir(cfgSeen.OutputDir) != f.staging || !isStagingName(filepath.Base(cfgSeen.OutputDir)) ||
		!strings.HasPrefix(filepath.Base(cfgSeen.OutputDir), stagedRunPrefix) {
		t.Fatalf("the fold wrote into %q, want a %s* folder directly under the staging folder %s", cfgSeen.OutputDir, stagedRunPrefix, f.staging)
	}
	// The download lands on the run folder's disk, beside the snapshot and
	// never inside it: everything in the snapshot directory is uploaded.
	snapDir := filepath.Join(cfgSeen.OutputDir, reconstruct.SnapshotDirName(refreshAt))
	if cfgSeen.DownloadDir != cfgSeen.OutputDir {
		t.Errorf("DownloadDir = %q, want the run folder %q", cfgSeen.DownloadDir, cfgSeen.OutputDir)
	}
	if rel, err := filepath.Rel(snapDir, cfgSeen.DownloadDir); err == nil && !strings.HasPrefix(rel, "..") {
		t.Errorf("DownloadDir %q is inside the snapshot directory %q, so it would be uploaded", cfgSeen.DownloadDir, snapDir)
	}
	if cfgSeen.SpaceCheck == nil {
		t.Error("the staged fold runs without a disk check")
	}
	stamp := reconstruct.SnapshotDirName(refreshAt)
	if len(stubs.uploads) != 1 || stubs.uploads[0] != (uploadCall{snapDir, "s3://bucket/s/" + stamp}) || !stubs.seenAtUpload[0] {
		t.Fatalf("uploads = %+v (present %v), want exactly %s -> s3://bucket/s/%s", stubs.uploads, stubs.seenAtUpload, snapDir, stamp)
	}
	if left := stagingEntries(t, f.staging); len(left) != 0 {
		t.Fatalf("the staging folder still holds %v after the run", left)
	}
	recs := sup.history.List("s")
	if len(recs) != 1 || recs[0].Error != "" || recs[0].SnapshotTime == "" || recs[0].Uploaded != 1 {
		t.Fatalf("records = %+v, want one published, uploaded run", recs)
	}
}

// Two S3-only servers share one staging folder and fold at the same time:
// each writes into its own run folder, and both are gone afterwards.
func TestStagedFold_twoServersAtOnceUseDistinctRunFolders(t *testing.T) {
	stubs := stubStagedFold(t, nil)
	f := newJobsFixture(t)
	sup := f.supervisor(t)
	var mu sync.Mutex
	outs := map[string]string{}
	both := make(chan struct{})
	var arrived sync.WaitGroup
	arrived.Add(2)
	go func() { arrived.Wait(); close(both) }()
	prev := foldTables
	t.Cleanup(func() { foldTables = prev })
	foldTables = func(_ context.Context, cfg reconstruct.FullTableConfig) ([]*reconstruct.TableReport, []reconstruct.TableFailure, error) {
		mu.Lock()
		outs[cfg.BaselineSrc] = cfg.OutputDir
		mu.Unlock()
		writeSnapshotFiles(t, filepath.Join(cfg.OutputDir, reconstruct.SnapshotDirName(cfg.At)), baseline.SuccessMarker)
		arrived.Done()
		select { // both folds are inside their run folders at once
		case <-both:
		case <-time.After(10 * time.Second):
			return nil, nil, errors.New("the other fold never started")
		}
		return nil, nil, nil
	}
	var done sync.WaitGroup
	for _, id := range []string{"a", "b"} {
		sup.refreshes[id] = &console.BaselineStatus{State: "running"}
	}
	for _, id := range []string{"a", "b"} {
		done.Add(1)
		go func() {
			defer done.Done()
			sup.runRefresh(s3OnlyRequest(id), refreshAt, time.Minute)
		}()
	}
	done.Wait()

	a, b := outs["s3://bucket/a/"], outs["s3://bucket/b/"]
	if a == "" || b == "" || a == b {
		t.Fatalf("run folders = %q and %q, want two distinct ones", a, b)
	}
	for _, d := range []string{a, b} {
		if filepath.Dir(d) != f.staging || !isStagingName(filepath.Base(d)) {
			t.Errorf("run folder %q is not a staging-named folder under %s", d, f.staging)
		}
	}
	for _, id := range []string{"a", "b"} {
		if st := sup.RefreshStatus(id); st.State != "succeeded" {
			t.Errorf("%s: status = %+v", id, st)
		}
	}
	if len(stubs.uploads) != 2 {
		t.Errorf("uploads = %+v, want one per server", stubs.uploads)
	}
	if left := stagingEntries(t, f.staging); len(left) != 0 {
		t.Fatalf("the staging folder still holds %v", left)
	}
}

// A crash mid-run: while the fold runs, the journal names the run folder
// (and not the snapshot inside it, which goes with it). A killed run's folder
// is removed by the boot reclaim.
func TestStagedFold_aKilledRunsFolderIsReclaimedAtBoot(t *testing.T) {
	stubStagedFold(t, nil)
	f := newJobsFixture(t)
	sup := f.supervisor(t)
	var during []console.BaselineJob
	var out string
	prev := foldTables
	t.Cleanup(func() { foldTables = prev })
	foldTables = func(_ context.Context, cfg reconstruct.FullTableConfig) ([]*reconstruct.TableReport, []reconstruct.TableFailure, error) {
		during = sup.history.Jobs()
		out = cfg.OutputDir
		return nil, nil, errors.New("refused")
	}
	sup.refreshes["s"] = &console.BaselineStatus{State: "running"}
	sup.runRefresh(s3OnlyRequest("s"), refreshAt, time.Minute)
	if len(during) != 1 || len(during[0].Dirs) != 1 || during[0].Dirs[0].Root != f.staging || during[0].Dirs[0].Name != filepath.Base(out) {
		t.Fatalf("journal during the fold = %+v, want exactly the run folder %s", during, out)
	}

	// The kill itself: a journaled run folder with a partial snapshot in it,
	// and the lock released by the kernel.
	j := sup.beginJob(console.BaselineRunRefresh, "s", "s", console.BaselineRunTriggerScheduled, "", time.Now())
	run := filepath.Join(f.staging, stagedRunPrefix+"123")
	mkdirs(t, run)
	j.Created(f.staging, filepath.Base(run))
	writeSnapshotFiles(t, filepath.Join(run, reconstruct.SnapshotDirName(refreshAt)), baseline.IncompleteMarker)
	abandon(t, j)

	f.supervisor(t).reclaimInterruptedJobs()
	if exists(run) {
		t.Fatalf("the killed run's folder %s survived the boot reclaim", run)
	}
}

// The upload fails: the run is FAILED, nothing is kept (there is no local
// folder to keep it in), and the message promises nothing about a local copy.
func TestStagedFold_uploadFailureDeletesTheRunAndPromisesNoLocalCopy(t *testing.T) {
	stubs := stubStagedFold(t, errors.New("AccessDenied: s3:PutObject"))
	f := newJobsFixture(t)
	sup := f.supervisor(t)
	injectFold(t, 0, nil)
	sup.refreshes["s"] = &console.BaselineStatus{State: "running"}
	sup.runRefresh(s3OnlyRequest("s"), refreshAt, time.Minute)

	st := sup.RefreshStatus("s")
	t.Logf("LastError: %s", st.LastError)
	if st.State != "failed" || st.Published || !st.UploadFailed {
		t.Fatalf("status = %+v, want failed, not published, upload failed", st)
	}
	if len(stubs.uploads) != 1 || !stubs.seenAtUpload[0] {
		t.Fatalf("uploads = %+v, want one attempt of an existing snapshot", stubs.uploads)
	}
	for _, promise := range []string{"written to", "next full read", "sweeps", "local snapshot"} {
		if strings.Contains(st.LastError, promise) {
			t.Errorf("the message promises a kept copy (%q): %s", promise, st.LastError)
		}
	}
	for _, want := range []string{"AccessDenied", "s3://bucket/s/" + reconstruct.SnapshotDirName(refreshAt), "deleted", "newest snapshot in the bucket"} {
		if !strings.Contains(st.LastError, want) {
			t.Errorf("the message does not say %q: %s", want, st.LastError)
		}
	}
	if left := stagingEntries(t, f.staging); len(left) != 0 {
		t.Fatalf("the staging folder still holds %v", left)
	}
	recs := sup.history.List("s")
	if len(recs) != 1 || recs[0].SnapshotTime != "" || recs[0].Error == "" {
		t.Fatalf("records = %+v, want one failed run naming no snapshot", recs)
	}
	if fullReadStandsIn(st) {
		t.Error("a full read would stand in for this upload failure, and it uploads to the same bucket")
	}
}

// The staging disk is full: the fold's own disk check refuses (a disk
// refusal, which the schedule never answers with a full read), and the run
// folder is still deleted. The check measures the staging folder's disk.
func TestStagedFold_aFullStagingDiskIsARefusalNotACrash(t *testing.T) {
	stubs := stubStagedFold(t, nil)
	f := newJobsFixture(t)
	sup := f.supervisor(t)
	var measured []string
	prevDisk := diskSpaceFn
	t.Cleanup(func() { diskSpaceFn = prevDisk })
	diskSpaceFn = func(dir string) (uint64, uint64, error) {
		measured = append(measured, dir)
		return 0, 100 << 30, nil
	}
	prev := foldTables
	t.Cleanup(func() { foldTables = prev })
	foldTables = func(_ context.Context, cfg reconstruct.FullTableConfig) ([]*reconstruct.TableReport, []reconstruct.TableFailure, error) {
		snap := filepath.Join(cfg.OutputDir, reconstruct.SnapshotDirName(cfg.At))
		writeSnapshotFiles(t, snap, baseline.IncompleteMarker)
		err := cfg.SpaceCheck(snap, 10<<20)
		return nil, []reconstruct.TableFailure{{Schema: "shop", Table: "orders", Err: err}}, err
	}
	sup.refreshes["s"] = &console.BaselineStatus{State: "running"}
	sup.runRefresh(s3OnlyRequest("s"), refreshAt, time.Minute)

	st := sup.RefreshStatus("s")
	t.Logf("LastError: %s", st.LastError)
	if st.State != "failed" || !st.DiskRefused || st.Published {
		t.Fatalf("status = %+v, want a failed disk refusal", st)
	}
	if len(measured) == 0 || !strings.HasPrefix(measured[0], f.staging) {
		t.Fatalf("the disk check measured %v, want a folder under the staging folder %s", measured, f.staging)
	}
	if len(stubs.uploads) != 0 {
		t.Errorf("uploaded %+v after a refusal", stubs.uploads)
	}
	if left := stagingEntries(t, f.staging); len(left) != 0 {
		t.Fatalf("the staging folder still holds %v", left)
	}
	if fullReadStandsIn(st) {
		t.Error("a full read would stand in for a disk refusal")
	}
}

// A server with BOTH a Local folder and a bucket is untouched: it folds into
// its own folder, keeps the snapshot there, and the staging folder is not used.
func TestStagedFold_aServerWithALocalFolderKeepsTheLocalPath(t *testing.T) {
	stubs := stubStagedFold(t, nil)
	f := newJobsFixture(t)
	sup := f.supervisor(t)
	local := t.TempDir()
	var cfgSeen reconstruct.FullTableConfig
	prev := foldTables
	t.Cleanup(func() { foldTables = prev })
	foldTables = func(_ context.Context, cfg reconstruct.FullTableConfig) ([]*reconstruct.TableReport, []reconstruct.TableFailure, error) {
		cfgSeen = cfg
		writeSnapshotFiles(t, filepath.Join(cfg.OutputDir, reconstruct.SnapshotDirName(cfg.At)), baseline.SuccessMarker)
		return nil, nil, nil
	}
	req := s3OnlyRequest("s")
	req.BaselineDir = local
	sup.refreshes["s"] = &console.BaselineStatus{State: "running"}
	sup.runRefresh(req, refreshAt, time.Minute)

	if cfgSeen.OutputDir != local || cfgSeen.DownloadDir != "" {
		t.Fatalf("OutputDir = %q, DownloadDir = %q, want %q and the system temporary directory", cfgSeen.OutputDir, cfgSeen.DownloadDir, local)
	}
	snap := filepath.Join(local, reconstruct.SnapshotDirName(refreshAt))
	if !dirExists(snap) {
		t.Error("the local snapshot was not kept")
	}
	if len(stubs.uploads) != 1 || stubs.uploads[0].outputDir != snap {
		t.Errorf("uploads = %+v, want the local snapshot", stubs.uploads)
	}
	if left := stagingEntries(t, f.staging); len(left) != 0 {
		t.Fatalf("the staging folder was used: %v", left)
	}
}

// The staging folder cannot be used: the run fails with a reason naming it,
// uploads nothing, and writes nothing anywhere else (no relative path under
// the working directory, which is what an empty output folder would mean).
func TestStagedFold_anUnusableStagingFolderFailsTheRunAndSaysWhy(t *testing.T) {
	stubs := stubStagedFold(t, nil)
	f := newJobsFixture(t)
	sup := f.supervisor(t)
	blocker := filepath.Join(t.TempDir(), "staging-is-a-file")
	writeFile(t, blocker)
	sup.stagingDir = blocker
	folded := false
	prev := foldTables
	t.Cleanup(func() { foldTables = prev })
	foldTables = func(context.Context, reconstruct.FullTableConfig) ([]*reconstruct.TableReport, []reconstruct.TableFailure, error) {
		folded = true
		return nil, nil, nil
	}
	wd, _ := os.Getwd()
	before, _ := os.ReadDir(wd)
	sup.refreshes["s"] = &console.BaselineStatus{State: "running"}
	sup.runRefresh(s3OnlyRequest("s"), refreshAt, time.Minute)

	st := sup.RefreshStatus("s")
	t.Logf("LastError: %s", st.LastError)
	if st.State != "failed" || st.Published || !strings.Contains(st.LastError, blocker) {
		t.Fatalf("status = %+v, want a failure naming %s", st, blocker)
	}
	// #1938: the folder goes by the name the settings row shows.
	if !strings.Contains(st.LastError, "working folder") || strings.Contains(st.LastError, "staging folder") {
		t.Fatalf("LastError = %q, want it to call the folder the working folder", st.LastError)
	}
	if folded || len(stubs.uploads) != 0 {
		t.Fatalf("folded=%v uploads=%+v, want neither", folded, stubs.uploads)
	}
	if after, _ := os.ReadDir(wd); len(after) != len(before) {
		t.Fatalf("the run wrote into the working directory %s", wd)
	}

	// The gate says the same thing before any run, so the schedule reports it.
	if err := sup.stagedUpdatesRefusal(); err == nil || !strings.Contains(err.Error(), blocker) ||
		!strings.Contains(err.Error(), "the working folder") || strings.Contains(err.Error(), "staging folder") {
		t.Fatalf("stagedUpdatesRefusal = %v, want a refusal naming the working folder %s", err, blocker)
	}
	sup.stagingDir = ""
	sup.stagingChecked = time.Time{}
	if err := sup.stagedUpdatesRefusal(); err == nil {
		t.Fatal("no staging folder at all was accepted")
	} else if !strings.Contains(err.Error(), "no working folder is set") || strings.Contains(err.Error(), "staging folder") {
		t.Fatalf("stagedUpdatesRefusal with no folder = %v, want it to say no working folder is set", err)
	}
	sup.stagingDir = f.staging
	sup.stagingChecked = time.Time{}
	if err := sup.stagedUpdatesRefusal(); err != nil {
		t.Fatalf("a writable staging folder was refused: %v", err)
	}
	if left := stagingEntries(t, f.staging); len(left) != 0 {
		t.Fatalf("the probe left %v in the staging folder", left)
	}
}

// Both gate builders carry the staging verdict: the loop's own, and (through
// StagedUpdates) the console's.
func TestBackupScheduler_gatesCarryTheStagingVerdict(t *testing.T) {
	b, _, sup := newScheduleFixture(t, true)
	if g := b.gates(); g.StagingRefusal != "" {
		t.Fatalf("StagingRefusal = %q with a writable staging folder", g.StagingRefusal)
	}
	if err := b.StagedUpdates(); err != nil {
		t.Fatalf("StagedUpdates = %v with a writable staging folder", err)
	}
	blocker := filepath.Join(t.TempDir(), "file")
	writeFile(t, blocker)
	sup.stagingDir = blocker
	sup.stagingChecked = time.Time{}
	if g := b.gates(); !strings.Contains(g.StagingRefusal, blocker) {
		t.Fatalf("StagingRefusal = %q, want it to name %s", g.StagingRefusal, blocker)
	}
}

// fullReadStandsIn is the watcher's rule; pinned on the shapes it must refuse.
func TestFullReadStandsIn(t *testing.T) {
	for _, tc := range []struct {
		name string
		st   console.BaselineStatus
		want bool
	}{
		{"a refused update", console.BaselineStatus{State: "failed"}, true},
		{"published, only the upload failed (local folder)", console.BaselineStatus{State: "failed", Published: true}, false},
		{"staged upload failed (S3 only)", console.BaselineStatus{State: "failed", UploadFailed: true}, false},
		{"disk refused", console.BaselineStatus{State: "failed", DiskRefused: true}, false},
		{"succeeded", console.BaselineStatus{State: "succeeded", Published: true}, false},
	} {
		if got := fullReadStandsIn(tc.st); got != tc.want {
			t.Errorf("%s: fullReadStandsIn = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// The daemon-wide --baseline-refresh-interval loop is NOT part of #2212: it
// names no destination, so turning S3-only servers into targets would start
// uploading a full snapshot to their bucket every interval, with no cost
// check and no retention, for an operator who never asked for it. Such a
// server stays skipped, with the warning; only the per-server schedule
// updates it through the staging folder.
func TestBaselineRefreshTargets_stillSkipsS3OnlyServers_2212(t *testing.T) {
	entries := []console.ServerEntry{
		{ID: "b", Name: "s3only", DSN: "dsn-b", BaselineS3: "s3://bucket/b/"},
	}
	got, skipped, _ := baselineRefreshTargets(entries, "", "")
	if len(got) != 0 || len(skipped) != 1 || skipped[0] != "s3only" {
		t.Fatalf("targets = %+v, skipped = %v, want the S3-only server skipped", got, skipped)
	}
}

// The run folder cannot be deleted: its journal entry is KEPT, so the next
// start removes it, instead of a whole snapshot's worth of staging disk that
// nothing would ever look at again.
func TestStagedFold_aFailedDeleteIsReclaimedAtTheNextStart(t *testing.T) {
	stubStagedFold(t, nil)
	f := newJobsFixture(t)
	sup := f.supervisor(t)
	injectFold(t, 0, nil)
	prevRemove := removeAllDir
	t.Cleanup(func() { removeAllDir = prevRemove })
	removeAllDir = func(string) error { return errors.New("EBUSY") }
	sup.refreshes["s"] = &console.BaselineStatus{State: "running"}
	sup.runRefresh(s3OnlyRequest("s"), refreshAt, time.Minute)

	left := stagingEntries(t, f.staging)
	if len(left) != 1 || !isStagingName(left[0]) {
		t.Fatalf("staging = %v, want the one run folder the delete could not remove", left)
	}
	if st := sup.RefreshStatus("s"); st.State != "succeeded" {
		t.Fatalf("status = %+v: a failed cleanup must not fail an uploaded run", st)
	}
	if jobs := sup.history.Jobs(); len(jobs) != 1 || len(jobs[0].Dirs) != 1 || jobs[0].Dirs[0].Name != left[0] {
		t.Fatalf("journal = %+v, want the run kept with its folder", jobs)
	}
	removeAllDir = prevRemove
	next := f.supervisor(t)
	next.reclaimInterruptedJobs()
	if exists(filepath.Join(f.staging, left[0])) {
		t.Fatal("the next start did not remove the run folder")
	}
	if recs := next.history.List("s"); len(recs) != 1 {
		t.Fatalf("records = %+v, want the run's own record only, not an interrupted one", recs)
	}
}

// A download that fills the disk is a disk refusal, so no full read (which
// stages in the same folder) stands in for it. Classified in reconstruct,
// where the error is known to be a local write (ErrLocalDiskFull), never by
// the words: the index MySQL says the same "No space left on device" about
// ITS tmp disk, and that failure a full read would cure.
func TestFoldDiskRefused_onlyALocalWriteIsADiskRefusal(t *testing.T) {
	download := fmt.Errorf("shop.orders: materialize baseline: %w", reconstruct.ErrLocalDiskFull)
	if !foldDiskRefused(download) {
		t.Fatal("a full disk under the download is not a disk refusal")
	}
	st := console.BaselineStatus{}
	applyFoldStatus(&st, 1, 1, reuseTally{}, download)
	if !st.DiskRefused || fullReadStandsIn(st) {
		t.Fatalf("status = %+v, want a disk refusal no full read stands in for", st)
	}
	index := errors.New("shop.orders: fetch events: Error 3 (HY000): Error writing file '/tmp/MYfd=58' (OS errno 28 - No space left on device)")
	if foldDiskRefused(index) {
		t.Fatal("the index MySQL's own full tmp disk reads as a local disk refusal")
	}
	st = console.BaselineStatus{}
	applyFoldStatus(&st, 1, 1, reuseTally{}, index)
	if st.DiskRefused || !fullReadStandsIn(st) {
		t.Fatalf("status = %+v, want a refusal a full read stands in for", st)
	}
}

// The same failed delete with NO journal (a supervisor without a run history,
// or a journal write that failed): nothing will remove the folder at the next
// start, so the log must name it and say to remove it by hand, and say it
// once, although the cleanup runs twice (the explicit call and the panic net).
func TestStagedFold_aFailedDeleteWithoutAJournalSaysRemoveByHand(t *testing.T) {
	stubStagedFold(t, nil)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	staging := t.TempDir()
	sup := newBaselineSupervisor(ctx, staging, baseline.DefaultLockMode) // no history: no journal
	injectFold(t, 0, nil)
	prevRemove := removeAllDir
	t.Cleanup(func() { removeAllDir = prevRemove })
	removeAllDir = func(string) error { return errors.New("EBUSY") }
	logs := captureWarnings(t)
	sup.refreshes["s"] = &console.BaselineStatus{State: "running"}
	sup.runRefresh(s3OnlyRequest("s"), refreshAt, time.Minute)
	removeAllDir = prevRemove

	left := stagingEntries(t, staging)
	if len(left) != 1 {
		t.Fatalf("staging = %v, want the run folder left behind", left)
	}
	out := logs.String()
	t.Log(out)
	if n := strings.Count(out, "level=ERROR"); n != 1 {
		t.Fatalf("%d Error lines, want exactly one", n)
	}
	if !strings.Contains(out, filepath.Join(staging, left[0])) || !strings.Contains(out, "remove it by hand") ||
		strings.Contains(out, "next start") {
		t.Fatalf("the log does not name the folder and say to remove it by hand: %s", out)
	}
}

// fakePartialStore is the bucket as the partial-upload cleanup sees it.
type fakePartialStore struct {
	mu      sync.Mutex
	keys    []string
	listed  []string
	deleted []string
	delErr  error
	failOn  map[string]error // a delete of this key fails with this
}

func (f *fakePartialStore) List(_ context.Context, prefix string) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.listed = append(f.listed, prefix)
	var out []string
	for _, k := range f.keys {
		if strings.HasPrefix(k, prefix) {
			out = append(out, k)
		}
	}
	return out, nil
}

func (f *fakePartialStore) Delete(_ context.Context, key string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.delErr != nil {
		return f.delErr
	}
	if err := f.failOn[key]; err != nil {
		return err
	}
	f.deleted = append(f.deleted, key)
	return nil
}

func stubPartialStore(t *testing.T, f *fakePartialStore) *string {
	t.Helper()
	prev := newPartialUploadStore
	t.Cleanup(func() { newPartialUploadStore = prev })
	var bucket string
	newPartialUploadStore = func(_ context.Context, b string) (partialUploadStore, error) {
		bucket = b
		return f, nil
	}
	return &bucket
}

// A staged update whose upload failed part way: what reached the bucket under
// THIS run's snapshot prefix is deleted (it has no _SUCCESS), and nothing
// else; the message says it was removed.
func TestStagedFold_aPartialUploadIsRemovedFromTheBucket(t *testing.T) {
	stubStagedFold(t, errors.New("connection reset"))
	stamp := reconstruct.SnapshotDirName(refreshAt)
	store := &fakePartialStore{keys: []string{
		"s/2026-08-28T09-00-00Z/_SUCCESS", "s/2026-08-28T09-00-00Z/shop/orders.parquet", // the previous snapshot
		"s/" + stamp + "/_INCOMPLETE", "s/" + stamp + "/shop/orders.parquet", // this run's partial copy
		"s/" + stamp + "x/shop/orders.parquet", // a sibling sharing the name as a prefix
	}}
	bucket := stubPartialStore(t, store)
	f := newJobsFixture(t)
	sup := f.supervisor(t)
	injectFold(t, 0, nil)
	sup.refreshes["s"] = &console.BaselineStatus{State: "running"}
	sup.runRefresh(s3OnlyRequest("s"), refreshAt, time.Minute)

	st := sup.RefreshStatus("s")
	t.Logf("LastError: %s", st.LastError)
	if *bucket != "bucket" || len(store.listed) != 1 || store.listed[0] != "s/"+stamp+"/" {
		t.Fatalf("listed %v in bucket %q, want only this run's prefix s/%s/", store.listed, *bucket, stamp)
	}
	want := []string{"s/" + stamp + "/shop/orders.parquet", "s/" + stamp + "/_INCOMPLETE"} // the marker last
	if strings.Join(store.deleted, ",") != strings.Join(want, ",") {
		t.Fatalf("deleted %v, want %v", store.deleted, want)
	}
	if !st.UploadFailed || !strings.Contains(st.LastError, "removed from the bucket") {
		t.Fatalf("status = %+v, want an upload failure saying the partial copy was removed", st)
	}
}

// A copy that carries _SUCCESS is complete and is never deleted; a delete that
// fails is said at Warn with the prefix, and the message says the files stay.
func TestStagedFold_partialUploadCleanupKeepsACompleteCopyAndSaysAFailure(t *testing.T) {
	stamp := reconstruct.SnapshotDirName(refreshAt)
	t.Run("complete copy kept", func(t *testing.T) {
		stubStagedFold(t, errors.New("timeout after the last file"))
		store := &fakePartialStore{keys: []string{"s/" + stamp + "/_SUCCESS", "s/" + stamp + "/shop/orders.parquet"}}
		stubPartialStore(t, store)
		sup := newJobsFixture(t).supervisor(t)
		injectFold(t, 0, nil)
		sup.refreshes["s"] = &console.BaselineStatus{State: "running"}
		sup.runRefresh(s3OnlyRequest("s"), refreshAt, time.Minute)
		if len(store.deleted) != 0 {
			t.Fatalf("deleted %v from a copy that carries _SUCCESS", store.deleted)
		}
	})
	t.Run("delete fails", func(t *testing.T) {
		stubStagedFold(t, errors.New("connection reset"))
		store := &fakePartialStore{keys: []string{"s/" + stamp + "/_INCOMPLETE"}, delErr: errors.New("AccessDenied: s3:DeleteObject")}
		stubPartialStore(t, store)
		sup := newJobsFixture(t).supervisor(t)
		injectFold(t, 0, nil)
		logs := captureWarnings(t)
		sup.refreshes["s"] = &console.BaselineStatus{State: "running"}
		sup.runRefresh(s3OnlyRequest("s"), refreshAt, time.Minute)
		st := sup.RefreshStatus("s")
		t.Logf("LastError: %s", st.LastError)
		if !strings.Contains(logs.String(), "level=WARN") || !strings.Contains(logs.String(), "s3://bucket/s/"+stamp+"/") ||
			!strings.Contains(logs.String(), "AccessDenied: s3:DeleteObject") {
			t.Fatalf("the failed delete is not said at Warn with the prefix: %s", logs)
		}
		if !strings.Contains(st.LastError, "marked incomplete") {
			t.Fatalf("message = %q, want it to say the sent files stay", st.LastError)
		}
	})
}

// The run folder is gone BEFORE the status turns terminal, which is what every
// observer waits on (the schedule's watcher, the page's poll): the deferred
// delete alone would run after it.
func TestStagedFold_theRunFolderIsGoneWhenTheStatusTurnsTerminal(t *testing.T) {
	for _, tc := range []struct {
		name      string
		uploadErr error
	}{{"published", nil}, {"upload failed", errors.New("connection reset")}} {
		t.Run(tc.name, func(t *testing.T) {
			stubStagedFold(t, tc.uploadErr)
			f := newJobsFixture(t)
			sup := f.supervisor(t)
			injectFold(t, 0, nil)
			var atTerminal []string
			var state string
			prev := refreshStatusWritten
			t.Cleanup(func() { refreshStatusWritten = prev })
			refreshStatusWritten = func(req refreshRequest) {
				state = sup.refreshes[req.ServerID].State // under s.mu, as the hook runs
				entries, _ := os.ReadDir(f.staging)
				for _, e := range entries {
					atTerminal = append(atTerminal, e.Name())
				}
			}
			sup.refreshes["s"] = &console.BaselineStatus{State: "running"}
			sup.runRefresh(s3OnlyRequest("s"), refreshAt, time.Minute)
			if state != "succeeded" && state != "failed" {
				t.Fatalf("the hook saw state %q, want a terminal one", state)
			}
			if len(atTerminal) != 0 {
				t.Fatalf("the staging folder held %v when the status turned %s", atTerminal, state)
			}
		})
	}
}

// The _INCOMPLETE marker is what keeps a partial copy out of every S3 listing
// (a folder with no marker at all reads as a complete pre-marker snapshot,
// #467). So it is deleted LAST, and only when every other delete went
// through: a delete that fails half way must leave the marker standing, or the
// leftover tables would become the bucket's newest "complete" snapshot. S3
// lists "_INCOMPLETE" before any table folder ('_' sorts before letters), so
// deleting in listing order would remove it first.
func TestRemovePartialUpload_theMarkerGoesLastAndOnlyIfEverythingElseWent(t *testing.T) {
	stamp := reconstruct.SnapshotDirName(refreshAt)
	pre := "s/" + stamp + "/"
	keys := []string{pre + "_INCOMPLETE", pre + "shop/a.parquet", pre + "shop/b.parquet"}
	req := s3OnlyRequest("s")
	dest := "s3://bucket/s/" + stamp
	for _, tc := range []struct {
		name       string
		failOn     map[string]error
		markerKept bool
	}{
		{"every delete works", nil, false},
		{"a table delete fails half way", map[string]error{pre + "shop/b.parquet": errors.New("SlowDown")}, true},
		{"the marker delete fails", map[string]error{pre + "_INCOMPLETE": errors.New("SlowDown")}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := &fakePartialStore{keys: keys, failOn: tc.failOn}
			stubPartialStore(t, store)
			words := removePartialUpload(context.Background(), req, dest, "")
			t.Logf("words: %s; deleted %v", words, store.deleted)
			markerGone := false
			for i, k := range store.deleted {
				if k == pre+"_INCOMPLETE" {
					markerGone = true
					if i != len(store.deleted)-1 {
						t.Fatalf("the marker was deleted before %v", store.deleted[i+1:])
					}
				}
			}
			if markerGone == tc.markerKept {
				t.Fatalf("marker deleted = %v, want kept = %v (deleted %v)", markerGone, tc.markerKept, store.deleted)
			}
			says := strings.Contains(words, "marked incomplete")
			if says != tc.markerKept {
				t.Fatalf("words %q: say the files stay marked incomplete exactly when the marker does", words)
			}
		})
	}
}

// A copy with NO marker at all (another writer's, or an older one) is never
// deleted piecemeal: a failed delete there would leave tables that read as
// complete. It is left alone and said.
func TestRemovePartialUpload_aCopyWithoutAMarkerIsLeftAlone(t *testing.T) {
	stamp := reconstruct.SnapshotDirName(refreshAt)
	pre := "s/" + stamp + "/"
	store := &fakePartialStore{keys: []string{pre + "shop/a.parquet"}}
	stubPartialStore(t, store)
	logs := captureWarnings(t)
	words := removePartialUpload(context.Background(), s3OnlyRequest("s"), "s3://bucket/s/"+stamp, "")
	t.Logf("words: %s", words)
	if len(store.deleted) != 0 {
		t.Fatalf("deleted %v from a folder with no marker", store.deleted)
	}
	if strings.Contains(words, "marked incomplete") || !strings.Contains(logs.String(), pre) {
		t.Fatalf("words %q / log %q", words, logs)
	}
}
