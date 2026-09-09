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
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// uploadCall records one upload the loop asked for.
type uploadCall struct{ outputDir, dest string }

// stubS3Fold points the fold's listing at a bucket and records what the loop
// asked to upload, so a whole refresh can run for an S3-backed server without
// a bucket. Only s3:// sources are answered here; a local source still runs
// the real listing, which is what the on-disk cases depend on.
// stubBucketListing keeps resolveFoldSource (#1626) off the network: a bucket
// listing answers empty, so the fold keeps the bucket as its source exactly as
// it did before the seam existed, and a local directory is listed for real.
// Without it every request carrying both a directory and a bucket opened
// DuckDB with httpfs and globbed a bucket literally named "bucket".
func stubBucketListing(t *testing.T) {
	t.Helper()
	real := listBaselines
	t.Cleanup(func() { listBaselines = real })
	listBaselines = func(ctx context.Context, src string) ([]reconstruct.BaselineFile, error) {
		if !strings.HasPrefix(src, "s3://") {
			return real(ctx, src)
		}
		return nil, nil
	}
}

func stubS3Fold(t *testing.T, bucketTables []string, uploadErr error) (*[]uploadCall, *[]string) {
	t.Helper()
	realList, realUpload := newestSnapshotTables, uploadSnapshot
	t.Cleanup(func() { newestSnapshotTables, uploadSnapshot = realList, realUpload })
	stubBucketListing(t)

	var listed []string
	newestSnapshotTables = func(ctx context.Context, src string) ([]string, error) {
		if !strings.HasPrefix(src, "s3://") {
			return realList(ctx, src)
		}
		listed = append(listed, src)
		return bucketTables, nil
	}
	var uploads []uploadCall
	uploadSnapshot = func(_ context.Context, outputDir, dest, _ string, _ bool) (int, error) {
		uploads = append(uploads, uploadCall{outputDir, dest})
		return 1, uploadErr
	}
	return &uploads, &listed
}

// runRefreshFor drives one whole refresh for a request and returns what it
// logged at Warn, plus the status the console would poll.
func runRefreshFor(t *testing.T, req refreshRequest) (string, console.BaselineStatus) {
	t.Helper()
	prev := slog.Default()
	t.Cleanup(func() { slog.SetDefault(prev) })
	var buf bytes.Buffer
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	sup := newBaselineSupervisor(ctx, t.TempDir(), baseline.DefaultLockMode)
	sup.refreshes[req.ServerID] = &console.BaselineStatus{State: "running"}
	sup.runRefresh(req, refreshAt, time.Minute)
	return buf.String(), sup.RefreshStatus(req.ServerID)
}

// The whole of #1539 end to end: on a server whose backups go to S3, one
// scheduled refresh reads its previous snapshot from the BUCKET, folds into
// the local directory, and sends the result to the destination a full backup
// would have written to.
//
// The local directory is staged EMPTY on purpose. That is the real shape of an
// S3-backed server the daemon has not folded for yet — every backup it has was
// uploaded — and it is the shape that used to make this refuse with "no
// baseline snapshot to refresh" while the console listed dozens.
func TestRunRefresh_S3BackedServerFoldsFromTheBucketAndUploads(t *testing.T) {
	local := t.TempDir()
	uploads, listed := stubS3Fold(t, []string{"shop.orders"}, nil)
	injectFold(t, 0, nil)

	out, st := runRefreshFor(t, refreshRequest{
		ServerID: "s", ServerName: "s", IndexDSN: "d",
		BaselineDir: local, BaselineS3: "s3://bucket/backups/",
	})

	if out != "" {
		t.Fatalf("the refresh warned: %s", out)
	}
	if !st.Published || st.State != "succeeded" {
		t.Fatalf("status = %+v, want a published success", st)
	}
	if len(*listed) != 1 || (*listed)[0] != "s3://bucket/backups/" {
		t.Fatalf("looked for the previous snapshot in %v, want the bucket once", *listed)
	}
	stamp := reconstruct.SnapshotDirName(refreshAt)
	want := uploadCall{filepath.Join(local, stamp), "s3://bucket/backups/" + stamp}
	if len(*uploads) != 1 || (*uploads)[0] != want {
		t.Fatalf("uploads = %+v, want exactly %+v", *uploads, want)
	}
}

// The daemon-wide --baseline-refresh-interval names no destination, so its
// requests carry no S3 and nothing is uploaded. Keeping that untouched is the
// point of gating on the request's own field rather than on a mode: the flag
// documents that its snapshots stay local.
func TestRunRefresh_withoutAnS3DestinationUploadsNothing(t *testing.T) {
	uploads, _ := stubS3Fold(t, nil, nil)
	injectFold(t, 0, nil)

	out, st := runRefreshFor(t, refreshRequest{
		ServerID: "s", ServerName: "s", IndexDSN: "d", BaselineDir: stageBaselineRoot(t),
	})

	if out != "" {
		t.Fatalf("the refresh warned: %s", out)
	}
	if !st.Published {
		t.Fatalf("status = %+v, want a published run: a local-only refresh publishes on disk", st)
	}
	if len(*uploads) != 0 {
		t.Fatalf("uploaded %+v, want nothing", *uploads)
	}
}

// A refresh is not published until the snapshot is where this server's backups
// live, so a failed upload fails the run. Two things must survive that: the
// local snapshot (it is finished, and it is the operator's whole remaining
// result), and a report that says the fold worked and the sending did not.
func TestRunRefresh_failedUploadFailsTheRunAndKeepsTheSnapshot(t *testing.T) {
	local := t.TempDir()
	_, _ = stubS3Fold(t, []string{"shop.orders"}, errors.New("AccessDenied"))
	injectFold(t, 0, nil)

	out, st := runRefreshFor(t, refreshRequest{
		ServerID: "s", ServerName: "s", IndexDSN: "d",
		BaselineDir: local, BaselineS3: "s3://bucket/backups/",
	})

	// Published is what stops the scheduled watcher answering this with a full
	// read of production, so the link from THIS failure to that flag is pinned
	// here: the watcher's own test stages the status directly.
	if st.State != "failed" {
		t.Fatalf("state = %q, want the run reported failed: the backup did not reach the destination", st.State)
	}
	if !st.Published {
		t.Fatalf("status = %+v, want Published: the fold finished and marked the snapshot", st)
	}
	if !strings.Contains(out, "published_snapshot=") {
		t.Fatalf("the finished snapshot is logged under the partial-snapshot key, which cleanup scripts delete: %s", out)
	}
	if strings.Contains(out, "published nothing") {
		t.Fatalf("the headline says nothing was published over a finished snapshot: %s", out)
	}
	if !strings.Contains(out, "AccessDenied") {
		t.Fatalf("the failed upload was not reported: %s", out)
	}
	if !strings.Contains(out, "only sending it to the backup destination failed") {
		t.Fatalf("the report does not say the fold itself worked: %s", out)
	}
	snap := filepath.Join(local, reconstruct.SnapshotDirName(refreshAt))
	if _, err := os.Stat(filepath.Join(snap, baseline.SuccessMarker)); err != nil {
		t.Fatalf("the finished snapshot was reclaimed after the upload failed: %v", err)
	}
}

// The run history has to name the snapshot an upload failure left behind.
//
// The report tells the operator that snapshot is their remaining result, so a
// history row carrying no time would describe the same run as having produced
// nothing, and the two surfaces would contradict each other about a backup
// that is on disk.
func TestPublishedSnapshotTime_namesTheSnapshotAnUploadFailureLeftBehind(t *testing.T) {
	at := time.Date(2026, 8, 28, 10, 0, 0, 0, time.UTC)
	want := "2026-08-28T10:00:00Z"
	for _, tc := range []struct {
		name string
		err  error
		want string
	}{
		{"a clean run", nil, want},
		{"only the upload failed", fmt.Errorf("%w: AccessDenied", errSnapshotNotUploaded), want},
		{"the fold refused", errors.New("capture gap in the reconstruction window"), ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := publishedSnapshotTime(at, tc.err); got != tc.want {
				t.Fatalf("publishedSnapshotTime = %q, want %q", got, tc.want)
			}
		})
	}
}

// A successful upload has to leave positive evidence. Without the count the
// only sign the snapshot reached the destination is the absence of a failure
// line, and the page's "N file(s) uploaded" clause never fires for this
// producer.
func TestRunRefresh_recordsHowManyFilesReachedTheDestination(t *testing.T) {
	realList, realUpload := newestSnapshotTables, uploadSnapshot
	t.Cleanup(func() { newestSnapshotTables, uploadSnapshot = realList, realUpload })
	stubBucketListing(t)
	newestSnapshotTables = func(ctx context.Context, src string) ([]string, error) {
		if !strings.HasPrefix(src, "s3://") {
			return realList(ctx, src)
		}
		return []string{"shop.orders"}, nil
	}
	uploadSnapshot = func(context.Context, string, string, string, bool) (int, error) { return 7, nil }
	injectFold(t, 0, nil)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	sup := newBaselineSupervisor(ctx, t.TempDir(), baseline.DefaultLockMode)
	hist, err := console.OpenBaselineHistory(filepath.Join(t.TempDir(), "h.json"))
	if err != nil {
		t.Fatal(err)
	}
	sup.history = hist
	sup.refreshes["s"] = &console.BaselineStatus{State: "running"}
	sup.runRefresh(refreshRequest{ServerID: "s", ServerName: "s", IndexDSN: "d",
		BaselineDir: t.TempDir(), BaselineS3: "s3://bucket/backups/"}, refreshAt, time.Minute)

	runs := hist.List("s")
	if len(runs) != 1 {
		t.Fatalf("runs = %d, want the refresh recorded", len(runs))
	}
	if runs[0].Uploaded != 7 {
		t.Fatalf("Uploaded = %d, want 7: a successful upload must be visible", runs[0].Uploaded)
	}
}

// The upload gate is `err == nil && req.BaselineS3 != ""`, and the `err == nil`
// half is the only thing standing between a refused fold and a false success.
//
// The assignment OVERWRITES the fold's error, so without it a refusal whose
// upload happened to succeed reports state "succeeded", Published true, an
// empty error and a history row naming a snapshot that was never marked. Every
// surface would show a good backup over a refused one.
func TestRunRefresh_arefusedFoldUploadsNothingAndStaysRefused(t *testing.T) {
	uploads, _ := stubS3Fold(t, []string{"shop.orders"}, nil)
	injectFold(t, 1, errors.New("capture gap in the reconstruction window"))

	out, st := runRefreshFor(t, refreshRequest{
		ServerID: "s", ServerName: "s", IndexDSN: "d",
		BaselineDir: t.TempDir(), BaselineS3: "s3://bucket/backups/",
	})

	if len(*uploads) != 0 {
		t.Fatalf("a refused fold uploaded %+v; an incomplete snapshot must never reach the destination", *uploads)
	}
	if st.State != "failed" || st.Published {
		t.Fatalf("status = %+v, want a failed run that published nothing", st)
	}
	if st.LastError == "" || !strings.Contains(st.LastError, "capture gap") {
		t.Fatalf("last error = %q, want the fold's own refusal, not an upload verdict over it", st.LastError)
	}
	if !strings.Contains(out, "published nothing") {
		t.Fatalf("the refusal was not reported as publishing nothing: %s", out)
	}
}
