package consoleapp

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// #2212, part 2: on an S3-only server's update, the fold lists the unchanged
// tables it did not write (TableReport.S3Copies), and the run hands that list
// to the upload, which copies them inside S3. Edge cases, written before the
// code:
//   - the fold is told where the snapshot goes ONLY on a staged run, so a
//     server with a Local folder, the daemon-wide loop's local folds and a
//     restore never leave tables out of what they publish;
//   - the copies reach the upload (a run that dropped them would publish a
//     snapshot missing every unchanged table, with nothing failing);
//   - a run with no copies uploads as before;
//   - a copy that fails fails the run, with the staged-run message;
//   - the read bound of the published snapshot counts the copied chains;
//   - the reuse tally counts a copied table as reused, never as disk saved.

type copyUploadCall struct {
	dir, dest string
	copies    []baseline.RemoteCopy
}

func stubCopyUpload(t *testing.T, err error) *[]copyUploadCall {
	t.Helper()
	var mu sync.Mutex
	var calls []copyUploadCall
	prev := uploadSnapshotCopies
	t.Cleanup(func() { uploadSnapshotCopies = prev })
	uploadSnapshotCopies = func(_ context.Context, dir, dest string, copies []baseline.RemoteCopy) (int, error) {
		mu.Lock()
		defer mu.Unlock()
		calls = append(calls, copyUploadCall{dir, dest, copies})
		if err != nil {
			return 0, err
		}
		return 1 + len(copies), nil
	}
	return &calls
}

func copiedReport() *reconstruct.TableReport {
	src := "s3://bucket/s/2026-08-28T09-00-00Z/shop/"
	return &reconstruct.TableReport{Schema: "shop", Table: "customers", CarriedForward: true,
		S3CopyChainStart: time.Date(2026, 8, 28, 9, 0, 0, 0, time.UTC),
		S3Copies: []baseline.RemoteCopy{
			{Rel: "shop/customers.parquet", Src: src + "customers.parquet", CRC32C: "00000001"},
			{Rel: "shop/customers.000000.posdel", Src: src + "customers.000000.posdel", CRC32C: "00000002"},
			{Rel: "shop/customers.000000.upserts", Src: src + "customers.000000.upserts", CRC32C: "00000003"},
		}}
}

func foldWithReports(t *testing.T, cfgSeen *reconstruct.FullTableConfig, reps ...*reconstruct.TableReport) {
	t.Helper()
	prev := foldTables
	t.Cleanup(func() { foldTables = prev })
	foldTables = func(_ context.Context, cfg reconstruct.FullTableConfig) ([]*reconstruct.TableReport, []reconstruct.TableFailure, error) {
		*cfgSeen = cfg
		writeSnapshotFiles(t, filepath.Join(cfg.OutputDir, reconstruct.SnapshotDirName(cfg.At)), baseline.SuccessMarker)
		return reps, nil, nil
	}
}

func TestStagedFold_unchangedTablesAreCopiedInsideS3(t *testing.T) {
	stubs := stubStagedFold(t, nil)
	copyCalls := stubCopyUpload(t, nil)
	f := newJobsFixture(t)
	sup := f.supervisor(t)
	sup.tableDeltas = true
	var startsSeen []time.Time
	prevRF := snapshotReadsFrom
	t.Cleanup(func() { snapshotReadsFrom = prevRF })
	snapshotReadsFrom = func(_ context.Context, _ string, at time.Time, copied []time.Time) (time.Time, error) {
		startsSeen = copied
		return at, nil
	}
	var cfgSeen reconstruct.FullTableConfig
	changed := &reconstruct.TableReport{Schema: "shop", Table: "orders", Files: []string{"shop/orders.parquet"}}
	foldWithReports(t, &cfgSeen, changed, copiedReport())

	sup.refreshes["s"] = &console.BaselineStatus{State: "running"}
	sup.runRefresh(s3OnlyRequest("s"), refreshAt, time.Minute)

	if cfgSeen.S3CopyUnchangedTo != "s3://bucket/s/" {
		t.Fatalf("S3CopyUnchangedTo = %q, want the server's bucket", cfgSeen.S3CopyUnchangedTo)
	}
	st := sup.RefreshStatus("s")
	if st.State != "succeeded" || !st.Published {
		t.Fatalf("status = %+v", st)
	}
	if len(stubs.uploads) != 0 {
		t.Fatalf("the upload without copies ran: %+v", stubs.uploads)
	}
	stamp := reconstruct.SnapshotDirName(refreshAt)
	if len(*copyCalls) != 1 {
		t.Fatalf("copy uploads = %+v, want one", *copyCalls)
	}
	call := (*copyCalls)[0]
	if call.dest != "s3://bucket/s/"+stamp || filepath.Base(call.dir) != stamp || len(call.copies) != 3 {
		t.Fatalf("upload = %+v", call)
	}
	if len(startsSeen) != 1 || !startsSeen[0].Equal(copiedReport().S3CopyChainStart) {
		t.Fatalf("the read bound saw %v, want the copied chain's start", startsSeen)
	}
	if recs := sup.history.List("s"); len(recs) != 1 || recs[0].Uploaded != 4 {
		t.Fatalf("records = %+v, want Uploaded to count the copies", recs)
	}
}

func TestStagedFold_aFailedCopyFailsTheRun(t *testing.T) {
	stubStagedFold(t, nil)
	stubCopyUpload(t, errors.New("copy shop/customers.parquet into the new snapshot inside S3: AccessDenied"))
	f := newJobsFixture(t)
	sup := f.supervisor(t)
	var cfgSeen reconstruct.FullTableConfig
	foldWithReports(t, &cfgSeen, copiedReport())
	sup.refreshes["s"] = &console.BaselineStatus{State: "running"}
	sup.runRefresh(s3OnlyRequest("s"), refreshAt, time.Minute)

	st := sup.RefreshStatus("s")
	t.Log(st.LastError)
	if st.State != "failed" || st.Published || !st.UploadFailed {
		t.Fatalf("status = %+v", st)
	}
	for _, want := range []string{"AccessDenied", "customers.parquet", "deleted"} {
		if !strings.Contains(st.LastError, want) {
			t.Errorf("message lacks %q: %s", want, st.LastError)
		}
	}
	if left := stagingEntries(t, f.staging); len(left) != 0 {
		t.Fatalf("the staging folder still holds %v", left)
	}
}

func TestStagedFold_noCopiesUploadsAsBefore(t *testing.T) {
	stubs := stubStagedFold(t, nil)
	copyCalls := stubCopyUpload(t, nil)
	f := newJobsFixture(t)
	sup := f.supervisor(t)
	var cfgSeen reconstruct.FullTableConfig
	foldWithReports(t, &cfgSeen, &reconstruct.TableReport{Schema: "shop", Table: "orders", Files: []string{"shop/orders.parquet"}})
	sup.refreshes["s"] = &console.BaselineStatus{State: "running"}
	sup.runRefresh(s3OnlyRequest("s"), refreshAt, time.Minute)
	if len(stubs.uploads) != 1 || len(*copyCalls) != 0 {
		t.Fatalf("uploads=%+v copy uploads=%+v", stubs.uploads, *copyCalls)
	}
}

// Only a staged run may leave tables out of what it writes: everywhere else
// the snapshot directory is what gets kept or uploaded whole.
func TestS3CopyIsOnlyForStagedRuns(t *testing.T) {
	stubStagedFold(t, nil)
	stubCopyUpload(t, nil)
	f := newJobsFixture(t)
	sup := f.supervisor(t)
	var cfgSeen reconstruct.FullTableConfig
	foldWithReports(t, &cfgSeen)
	req := s3OnlyRequest("s")
	req.BaselineDir = t.TempDir()
	sup.refreshes["s"] = &console.BaselineStatus{State: "running"}
	sup.runRefresh(req, refreshAt, time.Minute)
	if cfgSeen.S3CopyUnchangedTo != "" {
		t.Fatalf("a server with a Local folder folds with S3CopyUnchangedTo = %q", cfgSeen.S3CopyUnchangedTo)
	}
	restore := refreshFoldConfig(restoreFoldRequest(console.BaselineRestoreRequest{
		ServerID: "s", BaselineS3: "s3://bucket/s/", BaselineDir: t.TempDir(),
	}), refreshAt, nil)
	if restore.S3CopyUnchangedTo != "" {
		t.Fatalf("a restore folds with S3CopyUnchangedTo = %q", restore.S3CopyUnchangedTo)
	}
}

func TestCountReuse_aCopyInsideS3IsAReuseThatSavesNoDisk(t *testing.T) {
	got := countReuse([]*reconstruct.TableReport{copiedReport(), {CarriedForward: true, CarriedByLink: true}})
	if got.reused != 2 || got.copied != 1 || got.s3Copied != 1 {
		t.Fatalf("tally = %+v, want reused 2, copied 1 (no disk saved), s3Copied 1", got)
	}
}
