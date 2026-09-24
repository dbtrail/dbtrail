package consoleapp

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
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

// #1725: a full backup with a local directory is PUBLISHED the moment its
// snapshot is complete on disk; the upload to the backup destination follows
// with the server's job slot already free, so a scheduled refresh runs
// alongside it. The upload sends the NEW snapshot only (the old code handed
// the baselines root to the uploader, which re-sent every snapshot on disk on
// every full backup: 3,822 objects for 13 new files), and then sweeps up any
// local snapshot the destination lacks — the refresh's failed-upload message
// promises exactly that.

// dumpStub is what stubDumpUpload records: every upload (directory,
// destination, skip-existing flag) and every destination probe, in order.
type dumpStub struct {
	mu      sync.Mutex
	calls   []uploadCall
	retries []bool
	probes  []string
	hold    chan struct{} // every upload waits on it until the test closes it
	entered chan struct{} // closed once the first upload has started
	fail    func(dest string, retry bool) error
}

// stubDumpUpload stubs the upload and the destination probe: present decides
// what the destination already has, uploadErr fails EVERY upload (see
// stubDumpUploadFn to fail a chosen one).
func stubDumpUpload(t *testing.T, present map[string]bool, uploadErr error) *dumpStub {
	t.Helper()
	return stubDumpUploadFn(t, present, func(string, bool) error { return uploadErr })
}

func stubDumpUploadFn(t *testing.T, present map[string]bool, fail func(dest string, retry bool) error) *dumpStub {
	t.Helper()
	realUp, realPresent := uploadSnapshot, s3ObjectPresent
	t.Cleanup(func() { uploadSnapshot, s3ObjectPresent = realUp, realPresent })
	ds := &dumpStub{hold: make(chan struct{}), entered: make(chan struct{}), fail: fail}
	var once sync.Once
	uploadSnapshot = func(ctx context.Context, outputDir, dest, _ string, retry bool) (int, error) {
		once.Do(func() { close(ds.entered) })
		<-ds.hold
		ds.mu.Lock()
		ds.calls = append(ds.calls, uploadCall{outputDir, dest})
		ds.retries = append(ds.retries, retry)
		ds.mu.Unlock()
		if err := ds.fail(dest, retry); err != nil {
			return 0, err
		}
		return 3, nil
	}
	s3ObjectPresent = func(_ context.Context, url string) (bool, error) {
		ds.mu.Lock()
		ds.probes = append(ds.probes, url)
		ds.mu.Unlock()
		p, ok := present[url]
		if !ok {
			// A snapshot stamped probeErrorStamp answers a probe error; any
			// other unlisted URL is a probe the test did not expect (the run's
			// own snapshot, an incomplete one, one already being uploaded).
			if strings.Contains(url, probeErrorStamp) {
				return false, errors.New("probe failed")
			}
			t.Errorf("unexpected destination probe: %s", url)
			return false, errors.New("unexpected probe")
		}
		return p, nil
	}
	return ds
}

// lockedBuf is a goroutine-safe log sink for runs that log from another
// goroutine.
type lockedBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuf) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuf) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

// captureLog routes slog at level and above into a buffer for the test.
func captureLog(t *testing.T, level slog.Level) *lockedBuf {
	t.Helper()
	prev := slog.Default()
	t.Cleanup(func() { slog.SetDefault(prev) })
	lb := &lockedBuf{}
	slog.SetDefault(slog.New(slog.NewTextHandler(lb, &slog.HandlerOptions{Level: level})))
	return lb
}

// waitDump polls a server's dump status until cond holds, with a deadline
// so a regression fails with the status seen rather than hanging the package.
func waitDump(t *testing.T, sup *baselineSupervisor, id string, cond func(console.BaselineStatus) bool, want string) console.BaselineStatus {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		st := sup.Status(id)
		if cond(st) {
			return st
		}
		if time.Now().After(deadline) {
			t.Fatalf("status = %+v, want %s", st, want)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// probeErrorStamp is the directory-name stamp of the one snapshot whose
// destination probe the stub fails (10:23:49 in the sweep test).
const probeErrorStamp = "T10-23-49Z"

// completeDumpOwn runs completeDump with a throwaway ownership pointer, for
// tests that do not exercise the panic guard.
func completeDumpOwn(sup *baselineSupervisor, req console.BaselineRequest, out dumpOutcome, err error) {
	var own dumpOwn
	sup.completeDump(req, time.Now(), out, err, &own)
}

// assertNothingInFlight: no upload mark survived the run. A leaked one makes
// every later sweep skip that snapshot with no log line for the daemon's
// lifetime.
func assertNothingInFlight(t *testing.T, sup *baselineSupervisor) {
	t.Helper()
	sup.mu.Lock()
	n := len(sup.uploading)
	sup.mu.Unlock()
	if n != 0 {
		t.Fatalf("%d snapshot(s) left marked in flight forever", n)
	}
}

func dumpOutcomeAt(t *testing.T, root string, at time.Time) dumpOutcome {
	t.Helper()
	dir := filepath.Join(root, reconstruct.SnapshotDirName(at))
	writeSnapshotFiles(t, dir, baseline.SuccessMarker)
	return dumpOutcome{stats: baseline.Stats{TablesProcessed: 1, RowsWritten: 5}, snapDir: dir, at: at, cleanup: func() {}}
}

func TestDump_publishesLocallyBeforeTheUploadAndFreesTheSlot(t *testing.T) {
	local := t.TempDir()
	at := time.Date(2026, 9, 18, 11, 23, 49, 0, time.UTC)
	ds := stubDumpUpload(t, map[string]bool{}, nil)
	sup := newBaselineSupervisor(context.Background(), t.TempDir(), baseline.DefaultLockMode)
	req := console.BaselineRequest{ServerID: "a", ServerName: "a", LocalDir: local, S3: "s3://bucket/backups/"}
	sup.jobs["a"] = &console.BaselineStatus{State: "running"}

	done := make(chan struct{})
	go func() { defer close(done); completeDumpOwn(sup, req, dumpOutcomeAt(t, local, at), nil) }()

	// While the upload is held: published, not busy, and honest about the upload.
	waitDump(t, sup, "a", func(st console.BaselineStatus) bool { return st.State == "succeeded" && st.Published && st.Uploading },
		"succeeded, published, uploading")
	sup.mu.Lock()
	busy := sup.busyLocked("a")
	sup.mu.Unlock()
	if busy {
		t.Fatal("the server is still busy after the snapshot was published: the upload holds the slot")
	}
	if st := sup.Status("a"); st.Uploaded != 0 || st.Tables != 1 || st.Rows != 5 {
		t.Fatalf("published status = %+v", st)
	}

	close(ds.hold)
	<-done
	st := sup.Status("a")
	if st.State != "succeeded" || !st.Published || st.Uploading || st.Uploaded != 3 || st.LastError != "" {
		t.Fatalf("status after the upload = %+v", st)
	}
	// Exactly the new snapshot, to its own key under the destination.
	want := uploadCall{filepath.Join(local, reconstruct.SnapshotDirName(at)), "s3://bucket/backups/" + reconstruct.SnapshotDirName(at)}
	if len(ds.calls) != 1 || ds.calls[0] != want || ds.retries[0] {
		t.Fatalf("uploads = %+v retries=%v, want exactly %+v without skip-existing", ds.calls, ds.retries, want)
	}
}

func TestDump_uploadFailureAfterPublishIsAFailedRunThatKeepsTheSnapshot(t *testing.T) {
	local := t.TempDir()
	at := time.Date(2026, 9, 18, 11, 23, 49, 0, time.UTC)
	ds := stubDumpUpload(t, map[string]bool{}, errors.New("AccessDenied"))
	close(ds.hold)
	sup := newBaselineSupervisor(context.Background(), t.TempDir(), baseline.DefaultLockMode)
	req := console.BaselineRequest{ServerID: "a", ServerName: "a", LocalDir: local, S3: "s3://bucket/backups/"}
	sup.jobs["a"] = &console.BaselineStatus{State: "running"}
	out := dumpOutcomeAt(t, local, at)
	completeDumpOwn(sup, req, out, nil)
	st := sup.Status("a")
	if st.State != "failed" || !st.Published || st.Uploading || st.Uploaded != 0 {
		t.Fatalf("status = %+v, want failed but published", st)
	}
	for _, want := range []string{out.snapDir, "s3://bucket/backups/", "AccessDenied"} {
		if !strings.Contains(st.LastError, want) {
			t.Fatalf("LastError %q does not name %q", st.LastError, want)
		}
	}
	if _, err := os.Stat(filepath.Join(out.snapDir, baseline.SuccessMarker)); err != nil {
		t.Fatalf("the published snapshot was removed after the upload failed: %v", err)
	}
}

// TestDump_s3OnlyKeepsTheSlotThroughTheUpload: with no local directory the
// staging is temporary and nothing is published until the destination has
// it, so the slot stays held and a failed upload is a failed run.
func TestDump_s3OnlyKeepsTheSlotThroughTheUpload(t *testing.T) {
	at := time.Date(2026, 9, 18, 11, 23, 49, 0, time.UTC)
	ds := stubDumpUpload(t, map[string]bool{}, nil)
	sup := newBaselineSupervisor(context.Background(), t.TempDir(), baseline.DefaultLockMode)
	req := console.BaselineRequest{ServerID: "a", ServerName: "a", S3: "s3://bucket/backups/"}
	sup.jobs["a"] = &console.BaselineStatus{State: "running"}
	staging := t.TempDir()
	out := dumpOutcomeAt(t, staging, at)
	out.staged = true

	done := make(chan struct{})
	go func() { defer close(done); completeDumpOwn(sup, req, out, nil) }()
	select {
	case <-ds.entered: // the run is inside the upload, not still on its way there
	case <-time.After(5 * time.Second):
		t.Fatal("the upload never started")
	}
	sup.mu.Lock()
	busy := sup.busyLocked("a")
	sup.mu.Unlock()
	if st := sup.Status("a"); !busy || st.State != "running" || st.Published {
		t.Fatalf("S3-only dump released the slot before the upload: busy=%v status=%+v", busy, st)
	}
	close(ds.hold)
	<-done
	if st := sup.Status("a"); st.State != "succeeded" || st.Uploaded != 3 {
		t.Fatalf("status = %+v", st)
	}
	if len(ds.calls) != 1 || ds.calls[0].dest != "s3://bucket/backups/"+reconstruct.SnapshotDirName(at) {
		t.Fatalf("uploads = %+v", ds.calls)
	}
}

// TestDump_sweepsLocalSnapshotsTheDestinationLacks: after its own upload a
// full backup with a local directory sends every complete local snapshot
// whose _SUCCESS the destination does not have, skipping objects already
// there; one the destination has is left alone; a probe that fails is a
// warning, not a failed run.
func TestDump_sweepsLocalSnapshotsTheDestinationLacks(t *testing.T) {
	local := t.TempDir()
	at := time.Date(2026, 9, 18, 11, 23, 49, 0, time.UTC)
	older := at.Add(-3 * time.Hour)   // present in the bucket
	missing := at.Add(-2 * time.Hour) // a refresh whose upload failed
	unknown := at.Add(-1 * time.Hour) // 10:23:49, probeErrorStamp: the probe fails
	for _, ts := range []time.Time{older, missing, unknown} {
		writeSnapshotFiles(t, filepath.Join(local, reconstruct.SnapshotDirName(ts)), baseline.SuccessMarker)
	}
	// A second table under the missing snapshot: the listing is per TABLE,
	// the sweep is per snapshot, so it must probe and send it once.
	if err := os.WriteFile(filepath.Join(local, reconstruct.SnapshotDirName(missing), "shop", "items.parquet"), []byte("rows"), 0o644); err != nil {
		t.Fatal(err)
	}
	// An incomplete one is never swept: it is not a snapshot.
	writeSnapshotFiles(t, filepath.Join(local, reconstruct.SnapshotDirName(at.Add(-4*time.Hour))), baseline.IncompleteMarker)
	root := "s3://bucket/backups/"
	ds := stubDumpUpload(t, map[string]bool{
		root + reconstruct.SnapshotDirName(older) + "/" + baseline.SuccessMarker:   true,
		root + reconstruct.SnapshotDirName(missing) + "/" + baseline.SuccessMarker: false,
	}, nil)
	close(ds.hold)
	sup := newBaselineSupervisor(context.Background(), t.TempDir(), baseline.DefaultLockMode)
	req := console.BaselineRequest{ServerID: "a", ServerName: "a", LocalDir: local, S3: root}
	sup.jobs["a"] = &console.BaselineStatus{State: "running"}
	completeDumpOwn(sup, req, dumpOutcomeAt(t, local, at), nil)

	if st := sup.Status("a"); st.State != "succeeded" || st.Swept != 1 {
		t.Fatalf("status = %+v, want one swept snapshot", st)
	}
	if len(ds.calls) != 2 {
		t.Fatalf("uploads = %+v, want the new snapshot and the one the bucket lacks", ds.calls)
	}
	if ds.calls[1].dest != root+reconstruct.SnapshotDirName(missing) || !ds.retries[1] {
		t.Fatalf("sweep upload = %+v retry=%v, want the missing snapshot with skip-existing", ds.calls[1], ds.retries[1])
	}
	if len(ds.probes) != 3 {
		t.Fatalf("probes = %v, want one per complete snapshot other than the run's own (the two-table one once)", ds.probes)
	}
}

// TestDump_completionNeverFreesASlotALaterBackupHolds: the slot is free
// during the upload, so a second full backup can claim the status entry;
// the first run's completion must not write "succeeded" over the second's
// "running", which would free the slot under a job still running.
func TestDump_completionNeverFreesASlotALaterBackupHolds(t *testing.T) {
	local := t.TempDir()
	at := time.Date(2026, 9, 18, 11, 23, 49, 0, time.UTC)
	ds := stubDumpUpload(t, map[string]bool{}, nil)
	sup := newBaselineSupervisor(context.Background(), t.TempDir(), baseline.DefaultLockMode)
	sup.history = openTestHistory(t)
	req := console.BaselineRequest{ServerID: "a", ServerName: "a", LocalDir: local, S3: "s3://bucket/backups/"}
	sup.jobs["a"] = &console.BaselineStatus{State: "running"}
	done := make(chan struct{})
	var own dumpOwn
	go func() { defer close(done); sup.completeDump(req, time.Now(), dumpOutcomeAt(t, local, at), nil, &own) }()
	waitDump(t, sup, "a", func(st console.BaselineStatus) bool { return st.State == "succeeded" }, "published")
	// A second full backup claims the slot while the first uploads (what
	// Trigger does once busyLocked says the server is free; claimed by hand
	// so no second run actually starts).
	sup.mu.Lock()
	if sup.busyLocked("a") {
		sup.mu.Unlock()
		t.Fatal("the server is busy during the upload")
	}
	sup.jobs["a"] = &console.BaselineStatus{State: "running", Since: nowStamp()}
	sup.mu.Unlock()
	close(ds.hold)
	<-done
	sup.mu.Lock()
	busy := sup.busyLocked("a")
	sup.mu.Unlock()
	if st := sup.Status("a"); !busy || st.State != "running" {
		t.Fatalf("the first run's completion clobbered the second's slot: busy=%v status=%+v", busy, st)
	}
	// The first run's outcome lives on its own entry and in the history,
	// which is what the log line promises.
	if own.st == nil || own.st.State != "succeeded" || own.st.Uploading || own.st.Uploaded != 3 {
		t.Fatalf("own = %+v, want succeeded with the upload counted", own.st)
	}
	runs := sup.history.List("a")
	if len(runs) != 1 || runs[0].Uploaded != 3 || runs[0].SnapshotTime != at.Format(time.RFC3339) || runs[0].Error != "" {
		t.Fatalf("history = %+v, want the first run recorded with its snapshot", runs)
	}
}

func openTestHistory(t *testing.T) *console.BaselineRunHistory {
	t.Helper()
	h, err := console.OpenBaselineHistory(filepath.Join(t.TempDir(), "h.json"))
	if err != nil {
		t.Fatal(err)
	}
	return h
}

// TestDump_sweepSkipsItsOwnSnapshotByNameAndInFlightUploads: the run's own
// snapshot is never probed or re-sent (compared by DIRECTORY NAME: the instant
// read back from disk has second resolution, the run's own has nanoseconds),
// and neither is a snapshot another job in this process is sending right now.
func TestDump_sweepSkipsItsOwnSnapshotByNameAndInFlightUploads(t *testing.T) {
	local := t.TempDir()
	at := time.Date(2026, 9, 18, 11, 23, 49, 123456789, time.UTC) // nanoseconds, as time.Now gives
	inFlight := at.Add(-2 * time.Hour)
	writeSnapshotFiles(t, filepath.Join(local, reconstruct.SnapshotDirName(inFlight)), baseline.SuccessMarker)
	ds := stubDumpUpload(t, map[string]bool{}, nil) // every probe is unexpected
	close(ds.hold)
	sup := newBaselineSupervisor(context.Background(), t.TempDir(), baseline.DefaultLockMode)
	release := sup.markUploading("a", reconstruct.SnapshotDirName(inFlight)) // a refresh mid-upload
	defer release()
	req := console.BaselineRequest{ServerID: "a", ServerName: "a", LocalDir: local, S3: "s3://bucket/backups/"}
	sup.jobs["a"] = &console.BaselineStatus{State: "running"}
	completeDumpOwn(sup, req, dumpOutcomeAt(t, local, at), nil)
	if st := sup.Status("a"); st.State != "succeeded" || st.Swept != 0 || st.LastError != "" {
		t.Fatalf("status = %+v, want succeeded with nothing swept", st)
	}
	if len(ds.calls) != 1 || ds.calls[0].dest != "s3://bucket/backups/"+reconstruct.SnapshotDirName(at) {
		t.Fatalf("uploads = %+v, want only the run's own snapshot", ds.calls)
	}
}

// claimLaterRunOnUpload swaps the server's status entry for a later backup's
// "running" one from inside the upload, the shape a takeover has while the
// first run is still sending its snapshot.
func claimLaterRunOnUpload(t *testing.T, sup *baselineSupervisor, then func() (int, error)) {
	t.Helper()
	realUp := uploadSnapshot
	t.Cleanup(func() { uploadSnapshot = realUp })
	uploadSnapshot = func(context.Context, string, string, string, bool) (int, error) {
		sup.mu.Lock()
		sup.jobs["a"] = &console.BaselineStatus{State: "running", Since: "later"}
		sup.mu.Unlock()
		return then()
	}
}

func assertLaterRunUntouched(t *testing.T, sup *baselineSupervisor) {
	t.Helper()
	sup.mu.Lock()
	busy := sup.busyLocked("a")
	sup.mu.Unlock()
	if st := sup.Status("a"); !busy || st.State != "running" || st.Since != "later" {
		t.Fatalf("the later snapshot's entry was overwritten: busy=%v status=%+v", busy, st)
	}
}

// TestDump_panicDuringTheUploadFailsThisRunOnly: a panic after the publish
// lands on THIS run's entry (failed, Uploading cleared, the panic named) and
// never on the entry of a later backup that claimed the free slot.
func TestDump_panicDuringTheUploadFailsThisRunOnly(t *testing.T) {
	t.Run("no takeover", func(t *testing.T) {
		local := t.TempDir()
		at := time.Date(2026, 9, 18, 11, 23, 49, 0, time.UTC)
		realUp := uploadSnapshot
		t.Cleanup(func() { uploadSnapshot = realUp })
		uploadSnapshot = func(context.Context, string, string, string, bool) (int, error) { panic("boom") }
		sup := newBaselineSupervisor(context.Background(), t.TempDir(), baseline.DefaultLockMode)
		req := console.BaselineRequest{ServerID: "a", ServerName: "a", LocalDir: local, S3: "s3://bucket/backups/"}
		sup.jobs["a"] = &console.BaselineStatus{State: "running"}
		var own dumpOwn
		func() {
			defer func() { sup.recoverDumpJob(req, &own, recover()) }()
			sup.completeDump(req, time.Now(), dumpOutcomeAt(t, local, at), nil, &own)
		}()
		st := sup.Status("a")
		if st.State != "failed" || st.Uploading || !st.Published || !strings.Contains(st.LastError, "internal error during the upload") || !strings.Contains(st.LastError, "boom") {
			t.Fatalf("status = %+v, want failed, published, the panic named", st)
		}
		sup.mu.Lock()
		busy := sup.busyLocked("a")
		sup.mu.Unlock()
		if busy {
			t.Fatal("a panicked upload left the server busy")
		}
		assertNothingInFlight(t, sup)
	})
	t.Run("in the sweep, after the own upload succeeded", func(t *testing.T) {
		// The message must not claim the snapshot is not in the bucket.
		local := t.TempDir()
		at := time.Date(2026, 9, 18, 11, 23, 49, 0, time.UTC)
		writeSnapshotFiles(t, filepath.Join(local, reconstruct.SnapshotDirName(at.Add(-time.Hour))), baseline.SuccessMarker)
		ds := stubDumpUploadFn(t, map[string]bool{}, func(_ string, retry bool) error {
			if retry {
				panic("boom in the sweep")
			}
			return nil
		})
		close(ds.hold)
		realPresent := s3ObjectPresent
		t.Cleanup(func() { s3ObjectPresent = realPresent })
		s3ObjectPresent = func(context.Context, string) (bool, error) { return false, nil }
		logs := captureLog(t, slog.LevelError)
		sup := newBaselineSupervisor(context.Background(), t.TempDir(), baseline.DefaultLockMode)
		req := console.BaselineRequest{ServerID: "a", ServerName: "a", LocalDir: local, S3: "s3://bucket/backups/"}
		sup.jobs["a"] = &console.BaselineStatus{State: "running"}
		var own dumpOwn
		func() {
			defer func() { sup.recoverDumpJob(req, &own, recover()) }()
			sup.completeDump(req, time.Now(), dumpOutcomeAt(t, local, at), nil, &own)
		}()
		st := sup.Status("a")
		if st.State != "failed" || !strings.Contains(st.LastError, "during the sweep of older snapshots") {
			t.Fatalf("status = %+v, want failed with the sweep named", st)
		}
		if out := logs.String(); !strings.Contains(out, "had already reached the destination") || strings.Contains(out, "the next full read sends it") {
			t.Fatalf("log = %q, want the destination's copy acknowledged", out)
		}
		assertNothingInFlight(t, sup)
	})
	t.Run("takeover", func(t *testing.T) {
		local := t.TempDir()
		at := time.Date(2026, 9, 18, 11, 23, 49, 0, time.UTC)
		sup := newBaselineSupervisor(context.Background(), t.TempDir(), baseline.DefaultLockMode)
		claimLaterRunOnUpload(t, sup, func() (int, error) { panic("boom") })
		req := console.BaselineRequest{ServerID: "a", ServerName: "a", LocalDir: local, S3: "s3://bucket/backups/"}
		sup.jobs["a"] = &console.BaselineStatus{State: "running"}
		var own dumpOwn
		func() {
			defer func() { sup.recoverDumpJob(req, &own, recover()) }()
			sup.completeDump(req, time.Now(), dumpOutcomeAt(t, local, at), nil, &own)
		}()
		assertLaterRunUntouched(t, sup)
		if own.st == nil || own.st.State != "failed" || own.st.Uploading || !strings.Contains(own.st.LastError, "boom") {
			t.Fatalf("own = %+v, want this run failed on its own entry", own.st)
		}
		assertNothingInFlight(t, sup)
	})
	t.Run("before the publish", func(t *testing.T) {
		// A panic before anything was published is the old shape: the map
		// entry (this run's) fails; no detached entry exists.
		sup := newBaselineSupervisor(context.Background(), t.TempDir(), baseline.DefaultLockMode)
		req := console.BaselineRequest{ServerID: "a", ServerName: "a", S3: "s3://bucket/backups/"}
		sup.jobs["a"] = &console.BaselineStatus{State: "running"}
		func() {
			defer func() { sup.recoverDumpJob(req, nil, recover()) }()
			panic("early")
		}()
		if st := sup.Status("a"); st.State != "failed" || st.Published || !strings.Contains(st.LastError, "early") {
			t.Fatalf("status = %+v", st)
		}
	})
}

// TestDump_uploadFailureAfterATakeoverStaysOnItsOwnEntry: the ordinary
// (non-panic) upload failure follows the same ownership rule.
func TestDump_uploadFailureAfterATakeoverStaysOnItsOwnEntry(t *testing.T) {
	local := t.TempDir()
	at := time.Date(2026, 9, 18, 11, 23, 49, 0, time.UTC)
	sup := newBaselineSupervisor(context.Background(), t.TempDir(), baseline.DefaultLockMode)
	claimLaterRunOnUpload(t, sup, func() (int, error) { return 0, errors.New("AccessDenied") })
	req := console.BaselineRequest{ServerID: "a", ServerName: "a", LocalDir: local, S3: "s3://bucket/backups/"}
	sup.jobs["a"] = &console.BaselineStatus{State: "running"}
	var own dumpOwn
	sup.completeDump(req, time.Now(), dumpOutcomeAt(t, local, at), nil, &own)
	assertLaterRunUntouched(t, sup)
	if own.st == nil || own.st.State != "failed" || !own.st.Published || own.st.Uploading || !strings.Contains(own.st.LastError, "AccessDenied") {
		t.Fatalf("own = %+v", own.st)
	}
}

// TestDump_guardLeavesASuccessfulPublishedRunAlone: the guard runs on EVERY
// exit, so with own set and no panic it must change nothing. Drop the nil
// check and every successful full backup ends "failed: internal error
// while uploading: <nil>".
func TestDump_guardLeavesASuccessfulPublishedRunAlone(t *testing.T) {
	local := t.TempDir()
	at := time.Date(2026, 9, 18, 11, 23, 49, 0, time.UTC)
	ds := stubDumpUpload(t, map[string]bool{}, nil)
	close(ds.hold)
	sup := newBaselineSupervisor(context.Background(), t.TempDir(), baseline.DefaultLockMode)
	req := console.BaselineRequest{ServerID: "a", ServerName: "a", LocalDir: local, S3: "s3://bucket/backups/"}
	sup.jobs["a"] = &console.BaselineStatus{State: "running"}
	var own dumpOwn
	sup.completeDump(req, time.Now(), dumpOutcomeAt(t, local, at), nil, &own)
	sup.recoverDumpJob(req, &own, nil)
	if own.st == nil || own.st.State != "succeeded" || own.st.Uploading || own.st.LastError != "" || own.st.Uploaded != 3 {
		t.Fatalf("own after a no-panic guard = %+v", own.st)
	}
	if st := sup.Status("a"); st != *own.st {
		t.Fatalf("status = %+v, want the run's own entry %+v", st, *own.st)
	}
	// A panic AFTER finishDump wrote the terminal status (its closing log
	// line, say) is logged and changes nothing: the backup is complete and
	// reporting it failed would be a false report about durable data.
	captureLog(t, slog.LevelError)
	sup.recoverDumpJob(req, &own, "late")
	if st := sup.Status("a"); st.State != "succeeded" || st.LastError != "" || st.Uploaded != 3 {
		t.Fatalf("a late panic rewrote a finished run: %+v", st)
	}
}

// TestDump_localOnlyIsPublishedWithoutAnyUpload: no destination, nothing to
// send or probe; the snapshot on disk is the published backup.
func TestDump_localOnlyIsPublishedWithoutAnyUpload(t *testing.T) {
	local := t.TempDir()
	at := time.Date(2026, 9, 18, 11, 23, 49, 0, time.UTC)
	ds := stubDumpUpload(t, map[string]bool{}, nil)
	close(ds.hold)
	sup := newBaselineSupervisor(context.Background(), t.TempDir(), baseline.DefaultLockMode)
	req := console.BaselineRequest{ServerID: "a", ServerName: "a", LocalDir: local}
	sup.jobs["a"] = &console.BaselineStatus{State: "running"}
	completeDumpOwn(sup, req, dumpOutcomeAt(t, local, at), nil)
	if st := sup.Status("a"); st.State != "succeeded" || !st.Published || st.Uploading || st.Uploaded != 0 || st.Swept != 0 {
		t.Fatalf("status = %+v", st)
	}
	if len(ds.calls) != 0 || len(ds.probes) != 0 {
		t.Fatalf("uploads=%v probes=%v, want none without a destination", ds.calls, ds.probes)
	}
}

// TestDump_historyNamesTheSnapshotOnlyWhenOneWasPublished: the run record
// carries a snapshot instant when a snapshot exists somewhere the console can
// join to (the Backups page finds a run by it): a success, or a local publish
// whose upload failed. An S3-only run whose upload failed published nothing.
func TestDump_historyNamesTheSnapshotOnlyWhenOneWasPublished(t *testing.T) {
	at := time.Date(2026, 9, 18, 11, 23, 49, 0, time.UTC)
	cases := []struct {
		name         string
		localDir     bool
		uploadErr    error
		wantSnapshot bool
		wantUploaded int
		wantErr      string
	}{
		{"local and S3, upload ok", true, nil, true, 3, ""},
		{"local and S3, upload failed", true, errors.New("AccessDenied"), true, 0, "AccessDenied"},
		{"S3 only, upload failed", false, errors.New("AccessDenied"), false, 0, "AccessDenied"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ds := stubDumpUpload(t, map[string]bool{}, c.uploadErr)
			close(ds.hold)
			sup := newBaselineSupervisor(context.Background(), t.TempDir(), baseline.DefaultLockMode)
			sup.history = openTestHistory(t)
			req := console.BaselineRequest{ServerID: "a", ServerName: "a", S3: "s3://bucket/backups/"}
			dir := t.TempDir()
			if c.localDir {
				req.LocalDir = dir
			}
			out := dumpOutcomeAt(t, dir, at)
			out.staged = !c.localDir
			sup.jobs["a"] = &console.BaselineStatus{State: "running"}
			completeDumpOwn(sup, req, out, nil)
			runs := sup.history.List("a")
			if len(runs) != 1 {
				t.Fatalf("runs = %+v, want one record", runs)
			}
			r := runs[0]
			wantSnap := ""
			if c.wantSnapshot {
				wantSnap = at.Format(time.RFC3339)
			}
			if r.SnapshotTime != wantSnap || r.Uploaded != c.wantUploaded || !strings.Contains(r.Error, c.wantErr) {
				t.Fatalf("record = %+v, want snapshot %q uploaded %d error containing %q", r, wantSnap, c.wantUploaded, c.wantErr)
			}
		})
	}
}

// TestDump_s3OnlyFailedUploadPublishesNothingAndNamesNoStagingPath: the
// staging directory is gone once the run ends, so the error must not send
// the operator to it; the upload reads the staging BEFORE cleanup runs.
func TestDump_s3OnlyFailedUploadPublishesNothingAndNamesNoStagingPath(t *testing.T) {
	at := time.Date(2026, 9, 18, 11, 23, 49, 0, time.UTC)
	staging := t.TempDir()
	out := dumpOutcomeAt(t, staging, at)
	out.staged = true
	cleaned := false
	out.cleanup = func() { cleaned = true; os.RemoveAll(staging) }
	ds := stubDumpUploadFn(t, map[string]bool{}, func(string, bool) error {
		if cleaned {
			t.Error("the staging was removed before the upload read it")
		}
		if _, err := os.Stat(out.snapDir); err != nil {
			t.Errorf("the staged snapshot is not readable during the upload: %v", err)
		}
		return errors.New("AccessDenied")
	})
	close(ds.hold)
	sup := newBaselineSupervisor(context.Background(), t.TempDir(), baseline.DefaultLockMode)
	req := console.BaselineRequest{ServerID: "a", ServerName: "a", S3: "s3://bucket/backups/"}
	sup.jobs["a"] = &console.BaselineStatus{State: "running"}
	completeDumpOwn(sup, req, out, nil)
	st := sup.Status("a")
	if st.State != "failed" || st.Published || st.Uploading || !strings.Contains(st.LastError, "AccessDenied") {
		t.Fatalf("status = %+v, want failed, nothing published", st)
	}
	if strings.Contains(st.LastError, staging) {
		t.Fatalf("LastError names the staging directory, which no longer exists: %q", st.LastError)
	}
	if !cleaned {
		t.Fatal("the staging was not cleaned up after the failed upload")
	}
	sup.mu.Lock()
	busy := sup.busyLocked("a")
	sup.mu.Unlock()
	if busy {
		t.Fatal("a failed S3-only run left the server busy")
	}
}

// TestDump_aFailedSweepUploadIsAWarningNotAFailedRun: the run's own upload
// succeeded; a snapshot the sweep could not send is the next full backup's
// business, and the others are still sent.
func TestDump_aFailedSweepUploadIsAWarningNotAFailedRun(t *testing.T) {
	local := t.TempDir()
	at := time.Date(2026, 9, 18, 11, 23, 49, 0, time.UTC)
	first, second := at.Add(-2*time.Hour), at.Add(-1*time.Hour)
	for _, ts := range []time.Time{first, second} {
		writeSnapshotFiles(t, filepath.Join(local, reconstruct.SnapshotDirName(ts)), baseline.SuccessMarker)
	}
	root := "s3://bucket/backups/"
	ds := stubDumpUploadFn(t, map[string]bool{
		root + reconstruct.SnapshotDirName(first) + "/" + baseline.SuccessMarker:  false,
		root + reconstruct.SnapshotDirName(second) + "/" + baseline.SuccessMarker: false,
	}, func(dest string, retry bool) error {
		if retry && dest == root+reconstruct.SnapshotDirName(first) {
			return errors.New("AccessDenied")
		}
		return nil
	})
	close(ds.hold)
	logs := captureLog(t, slog.LevelWarn)
	sup := newBaselineSupervisor(context.Background(), t.TempDir(), baseline.DefaultLockMode)
	req := console.BaselineRequest{ServerID: "a", ServerName: "a", LocalDir: local, S3: root}
	sup.jobs["a"] = &console.BaselineStatus{State: "running"}
	completeDumpOwn(sup, req, dumpOutcomeAt(t, local, at), nil)
	st := sup.Status("a")
	if st.State != "succeeded" || st.LastError != "" || st.Uploaded != 3 || st.Swept != 1 {
		t.Fatalf("status = %+v, want succeeded with the one sendable snapshot swept", st)
	}
	if len(ds.calls) != 3 {
		t.Fatalf("uploads = %+v, want own + both sweep attempts", ds.calls)
	}
	if out := logs.String(); strings.Count(out, "could not be sent") != 1 || !strings.Contains(out, reconstruct.SnapshotDirName(first)) {
		t.Fatalf("log = %q, want one warning naming the snapshot that could not be sent", out)
	}
}

// TestDump_sweepStopsAtShutdownAndKeepsWhatItSent: a shutdown mid-sweep ends
// the sweep with one line, keeps the count of what was sent, and probes
// nothing further; the run is still a success.
func TestDump_sweepStopsAtShutdownAndKeepsWhatItSent(t *testing.T) {
	local := t.TempDir()
	at := time.Date(2026, 9, 18, 11, 23, 49, 0, time.UTC)
	root := "s3://bucket/backups/"
	present := map[string]bool{}
	for _, ts := range []time.Time{at.Add(-2 * time.Hour), at.Add(-1 * time.Hour)} {
		writeSnapshotFiles(t, filepath.Join(local, reconstruct.SnapshotDirName(ts)), baseline.SuccessMarker)
		present[root+reconstruct.SnapshotDirName(ts)+"/"+baseline.SuccessMarker] = false
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	ds := stubDumpUploadFn(t, present, func(_ string, retry bool) error {
		if retry {
			cancel() // the daemon shuts down while the first sweep upload is in flight
		}
		return nil
	})
	close(ds.hold)
	logs := captureLog(t, slog.LevelInfo)
	sup := newBaselineSupervisor(ctx, t.TempDir(), baseline.DefaultLockMode)
	req := console.BaselineRequest{ServerID: "a", ServerName: "a", LocalDir: local, S3: root}
	sup.jobs["a"] = &console.BaselineStatus{State: "running"}
	completeDumpOwn(sup, req, dumpOutcomeAt(t, local, at), nil)
	st := sup.Status("a")
	if st.State != "succeeded" || st.Swept != 1 || st.LastError != "" {
		t.Fatalf("status = %+v, want succeeded with the one sent snapshot counted", st)
	}
	if len(ds.probes) != 1 || len(ds.calls) != 2 {
		t.Fatalf("probes=%v uploads=%v, want the sweep to stop after the first snapshot", ds.probes, ds.calls)
	}
	if out := logs.String(); strings.Count(out, "interrupted by shutdown") != 1 || strings.Contains(out, "level=WARN") {
		t.Fatalf("log = %q, want exactly one shutdown line and no warnings", out)
	}
}

// TestDump_shutdownDuringTheUploadIsAWarningNotALostBackup: the run's own
// upload cut by daemon shutdown is a failed run with a complete local
// snapshot, logged as a warning, and no sweep is attempted afterwards.
func TestDump_shutdownDuringTheUploadIsAWarningNotALostBackup(t *testing.T) {
	local := t.TempDir()
	at := time.Date(2026, 9, 18, 11, 23, 49, 0, time.UTC)
	writeSnapshotFiles(t, filepath.Join(local, reconstruct.SnapshotDirName(at.Add(-time.Hour))), baseline.SuccessMarker)
	ds := stubDumpUpload(t, map[string]bool{}, fmt.Errorf("put: %w", context.Canceled)) // any probe trips the trap
	close(ds.hold)
	logs := captureLog(t, slog.LevelWarn)
	sup := newBaselineSupervisor(context.Background(), t.TempDir(), baseline.DefaultLockMode)
	req := console.BaselineRequest{ServerID: "a", ServerName: "a", LocalDir: local, S3: "s3://bucket/backups/"}
	sup.jobs["a"] = &console.BaselineStatus{State: "running"}
	completeDumpOwn(sup, req, dumpOutcomeAt(t, local, at), nil)
	st := sup.Status("a")
	if st.State != "failed" || !st.Published || st.Uploading || st.Swept != 0 {
		t.Fatalf("status = %+v, want failed but published, nothing swept", st)
	}
	if out := logs.String(); !strings.Contains(out, "daemon shutdown") || strings.Contains(out, "level=ERROR") {
		t.Fatalf("log = %q, want a warning about the shutdown and no error", out)
	}
	if len(ds.probes) != 0 {
		t.Fatalf("probes = %v, want no sweep after a failed own upload", ds.probes)
	}
}

// TestDump_realRunFailsItsOwnEntryWhenTheUploadPanics drives the real
// Trigger → run with a fake producer, so the wiring of own into the deferred
// guard is what is tested (a guard that captured own at defer time, or a
// completeDump handed another variable, would fail the MAP entry: a later
// backup's, freeing the slot under a running job).
func TestDump_realRunFailsItsOwnEntryWhenTheUploadPanics(t *testing.T) {
	at := time.Date(2026, 9, 18, 11, 23, 49, 0, time.UTC)
	t.Run("takeover", func(t *testing.T) {
		local := t.TempDir()
		logs := captureLog(t, slog.LevelError)
		sup := newBaselineSupervisor(context.Background(), t.TempDir(), baseline.DefaultLockMode)
		sup.produce = func(console.BaselineRequest) (dumpOutcome, error) { return dumpOutcomeAt(t, local, at), nil }
		claimLaterRunOnUpload(t, sup, func() (int, error) { panic("boom") })
		req := console.BaselineRequest{ServerID: "a", ServerName: "a", LocalDir: local, S3: "s3://bucket/backups/"}
		if err := sup.Trigger(req); err != nil {
			t.Fatal(err)
		}
		deadline := time.Now().Add(5 * time.Second)
		for !strings.Contains(logs.String(), "stopped after the snapshot was published") {
			if time.Now().After(deadline) {
				t.Fatalf("the guard never logged the panic; log = %q", logs.String())
			}
			time.Sleep(5 * time.Millisecond)
		}
		assertLaterRunUntouched(t, sup)
		assertNothingInFlight(t, sup)
	})
	t.Run("no takeover", func(t *testing.T) {
		local := t.TempDir()
		captureLog(t, slog.LevelError)
		sup := newBaselineSupervisor(context.Background(), t.TempDir(), baseline.DefaultLockMode)
		sup.produce = func(console.BaselineRequest) (dumpOutcome, error) { return dumpOutcomeAt(t, local, at), nil }
		realUp := uploadSnapshot
		t.Cleanup(func() { uploadSnapshot = realUp })
		uploadSnapshot = func(context.Context, string, string, string, bool) (int, error) { panic("boom") }
		req := console.BaselineRequest{ServerID: "a", ServerName: "a", LocalDir: local, S3: "s3://bucket/backups/"}
		if err := sup.Trigger(req); err != nil {
			t.Fatal(err)
		}
		st := waitDump(t, sup, "a", func(st console.BaselineStatus) bool { return st.State == "failed" }, "failed")
		if !st.Published || st.Uploading || !strings.Contains(st.LastError, "boom") {
			t.Fatalf("status = %+v, want failed, published, the panic named", st)
		}
		assertNothingInFlight(t, sup)
	})
}

// TestDump_sweepSkipsAMarkerlessLegacySnapshotWithoutProbingIt: a snapshot
// written before the markers existed lists as complete but can never be
// uploaded (the uploader needs _SUCCESS); the sweep says so once and moves
// on instead of a doomed probe + upload on every full backup.
func TestDump_sweepSkipsAMarkerlessLegacySnapshotWithoutProbingIt(t *testing.T) {
	local := t.TempDir()
	at := time.Date(2026, 9, 18, 11, 23, 49, 0, time.UTC)
	legacy := reconstruct.SnapshotDirName(at.Add(-time.Hour))
	writeSnapshotFiles(t, filepath.Join(local, legacy)) // no marker at all
	ds := stubDumpUpload(t, map[string]bool{}, nil)     // any probe trips the trap
	close(ds.hold)
	logs := captureLog(t, slog.LevelInfo)
	sup := newBaselineSupervisor(context.Background(), t.TempDir(), baseline.DefaultLockMode)
	req := console.BaselineRequest{ServerID: "a", ServerName: "a", LocalDir: local, S3: "s3://bucket/backups/"}
	sup.jobs["a"] = &console.BaselineStatus{State: "running"}
	completeDumpOwn(sup, req, dumpOutcomeAt(t, local, at), nil)
	if st := sup.Status("a"); st.State != "succeeded" || st.Swept != 0 || st.LastError != "" {
		t.Fatalf("status = %+v", st)
	}
	if len(ds.calls) != 1 || len(ds.probes) != 0 {
		t.Fatalf("uploads=%v probes=%v, want only the run's own upload", ds.calls, ds.probes)
	}
	if out := logs.String(); !strings.Contains(out, "no _SUCCESS marker") || !strings.Contains(out, legacy) || strings.Contains(out, "level=WARN") {
		t.Fatalf("log = %q, want one info line naming the legacy snapshot", out)
	}
}

// TestDump_sweepSendsTheSnapshotARefreshCouldNotUpload is the refresh's
// failed-upload message end to end: the snapshot a refresh published locally
// but could not send is what the next full backup's sweep sends, and the
// refresh released its in-flight mark so the sweep can.
func TestDump_sweepSendsTheSnapshotARefreshCouldNotUpload(t *testing.T) {
	stubGateReads(t)
	local := t.TempDir()
	root := "s3://bucket/backups/"
	stubS3Fold(t, []string{"shop.orders"}, errors.New("AccessDenied"))
	injectFold(t, 0, nil)
	sup := newBaselineSupervisor(context.Background(), t.TempDir(), baseline.DefaultLockMode)
	sup.refreshes["s"] = &console.BaselineStatus{State: "running"}
	sup.runRefresh(refreshRequest{ServerID: "s", ServerName: "s", IndexDSN: "d", BaselineDir: local, BaselineS3: root}, refreshAt, time.Minute)
	if rs := sup.RefreshStatus("s"); rs.State != "failed" || !rs.Published {
		t.Fatalf("refresh status = %+v, want failed but published", rs)
	}
	name := reconstruct.SnapshotDirName(refreshAt)
	if sup.isUploading("s", name) {
		t.Fatal("the refresh left its in-flight mark behind after the failed upload")
	}

	at := refreshAt.Add(time.Hour)
	ds := stubDumpUpload(t, map[string]bool{root + name + "/" + baseline.SuccessMarker: false}, nil)
	close(ds.hold)
	req := console.BaselineRequest{ServerID: "s", ServerName: "s", LocalDir: local, S3: root}
	sup.jobs["s"] = &console.BaselineStatus{State: "running"}
	completeDumpOwn(sup, req, dumpOutcomeAt(t, local, at), nil)
	if st := sup.Status("s"); st.State != "succeeded" || st.Swept != 1 {
		t.Fatalf("status = %+v, want the refresh's snapshot swept", st)
	}
	if len(ds.calls) != 2 || ds.calls[1].dest != root+name || !ds.retries[1] {
		t.Fatalf("uploads = %+v retries=%v, want the refresh's snapshot sent second with skip-existing", ds.calls, ds.retries)
	}
}

// TestRunRefresh_marksItsSnapshotUploadingForTheDurationOfTheUpload: the
// mark is up while the refresh's upload runs and gone when it returns.
func TestRunRefresh_marksItsSnapshotUploadingForTheDurationOfTheUpload(t *testing.T) {
	stubGateReads(t)
	local := t.TempDir()
	stubS3Fold(t, []string{"shop.orders"}, nil)
	injectFold(t, 0, nil)
	sup := newBaselineSupervisor(context.Background(), t.TempDir(), baseline.DefaultLockMode)
	name := reconstruct.SnapshotDirName(refreshAt)
	marked := false
	realUp := uploadSnapshot
	t.Cleanup(func() { uploadSnapshot = realUp })
	uploadSnapshot = func(context.Context, string, string, string, bool) (int, error) {
		marked = sup.isUploading("s", name)
		return 1, nil
	}
	sup.refreshes["s"] = &console.BaselineStatus{State: "running"}
	sup.runRefresh(refreshRequest{ServerID: "s", ServerName: "s", IndexDSN: "d", BaselineDir: local, BaselineS3: "s3://bucket/backups/"}, refreshAt, time.Minute)
	if rs := sup.RefreshStatus("s"); rs.State != "succeeded" {
		t.Fatalf("refresh status = %+v", rs)
	}
	if !marked {
		t.Fatal("the refresh's snapshot was not marked in flight during its upload")
	}
	if sup.isUploading("s", name) {
		t.Fatal("the refresh's in-flight mark outlived its upload")
	}
}

// TestScheduleState_aPublishedBackupStillUploadingIsInFlight: the schedule
// takes ONE copy of a job's terminal status; a full backup published with
// its upload still running is not terminal for it, or "uploaded: 0" would
// be the run's record and the fallback alarm would end early.
func TestScheduleState_aPublishedBackupStillUploadingIsInFlight(t *testing.T) {
	b, _, sup := newScheduleFixture(t, true)
	b.mu.Lock()
	b.started["a"] = scheduledStart{method: console.BackupMethodFull, at: "2026-09-18T11:00:00Z", since: "s1"}
	b.mu.Unlock()
	sup.mu.Lock()
	sup.jobs["a"] = &console.BaselineStatus{State: "succeeded", Since: "s1", Published: true, Uploading: true, Tables: 4}
	sup.mu.Unlock()
	if st := b.ScheduleState("a"); !st.Running || st.Last == nil || !st.Last.Uploading {
		t.Fatalf("schedule state during the upload = %+v, want running with the live status", st)
	}
	b.mu.Lock()
	copied := b.started["a"].last
	b.mu.Unlock()
	if copied != nil {
		t.Fatalf("the schedule copied a status still uploading as the run's record: %+v", copied)
	}
	sup.mu.Lock()
	sup.jobs["a"].Uploading, sup.jobs["a"].Uploaded = false, 3
	sup.mu.Unlock()
	if st := b.ScheduleState("a"); st.Running || st.Last == nil || st.Last.Uploaded != 3 {
		t.Fatalf("schedule state after the upload = %+v, want settled with the upload counted", st)
	}
	b.mu.Lock()
	copied = b.started["a"].last
	b.mu.Unlock()
	if copied == nil || copied.Uploaded != 3 {
		t.Fatalf("copied record = %+v, want the settled status", copied)
	}
}
