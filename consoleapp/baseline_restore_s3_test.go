package consoleapp

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// restoreAt is the instant the restores below fold to; one hour after the
// staged snapshot so the listing has something at-or-before it.
var restoreAt = time.Date(2026, 8, 28, 10, 0, 0, 0, time.UTC)

// stubS3Restore points the restore's listing at a bucket, records what the
// fold was configured with, and records what was asked to be uploaded, so a
// whole restore can run for an S3-backed server without a bucket. Only s3://
// sources are answered from the stub; a local source still runs the real
// listing, which is what the local-only case depends on.
func stubS3Restore(t *testing.T, bucketTables []string, uploadErr error) (listed *[]string, folded *[]reconstruct.FullTableConfig, uploads *[]uploadCall) {
	t.Helper()
	realList, realFold, realUpload := snapshotAt, foldTables, uploadSnapshot
	t.Cleanup(func() { snapshotAt, foldTables, uploadSnapshot = realList, realFold, realUpload })

	var srcs []string
	snapshotAt = func(ctx context.Context, src string, at time.Time) ([]string, time.Time, error) {
		if !strings.HasPrefix(src, "s3://") {
			return realList(ctx, src, at)
		}
		srcs = append(srcs, src)
		// The bucket's newest snapshot at-or-before at sits an hour earlier,
		// unless a case asks for a collision (see bucketAnchor).
		return bucketTables, bucketAnchor(at), nil
	}
	var cfgs []reconstruct.FullTableConfig
	foldTables = func(_ context.Context, cfg reconstruct.FullTableConfig) ([]*reconstruct.TableReport, []reconstruct.TableFailure, error) {
		cfgs = append(cfgs, cfg)
		writeSnapshotFiles(t, filepath.Join(cfg.OutputDir, reconstruct.SnapshotDirName(cfg.At)), baseline.SuccessMarker)
		return nil, nil, nil
	}
	var ups []uploadCall
	uploadSnapshot = func(_ context.Context, outputDir, dest, _ string, _ bool) (int, error) {
		ups = append(ups, uploadCall{outputDir, dest})
		return 1, uploadErr
	}
	return &srcs, &cfgs, &ups
}

// bucketAnchor is the snapshot time the stubbed bucket reports for a restore
// to at: an hour earlier by default. A case sets bucketCollides so the stub
// answers with at itself, the shape of an operator typing the exact second
// of a backup the bucket already holds.
var bucketCollides bool

func bucketAnchor(at time.Time) time.Time {
	if bucketCollides {
		return at
	}
	return at.Add(-time.Hour)
}

// runRestoreFor drives one whole restore synchronously and returns what it
// logged at Warn, the status the console would poll, and the history record
// the run left (nil when none).
func runRestoreFor(t *testing.T, req console.BaselineRestoreRequest) (string, console.BaselineStatus, *console.BaselineRunRecord) {
	t.Helper()
	prev := slog.Default()
	t.Cleanup(func() { slog.SetDefault(prev) })
	var buf bytes.Buffer
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	sup := newBaselineSupervisor(ctx, t.TempDir(), baseline.DefaultLockMode)
	h, err := console.OpenBaselineHistory(filepath.Join(t.TempDir(), "history.json"))
	if err != nil {
		t.Fatal(err)
	}
	sup.history = h
	sup.restores[req.ServerID] = &console.BaselineStatus{State: "running"}
	sup.runRestore(req)
	// A refused run records no snapshot time, so it is found by position, not
	// by snapshot: the run is the only one this history holds.
	var rec *console.BaselineRunRecord
	if runs := h.List(req.ServerID); len(runs) == 1 {
		rec = &runs[0]
	}
	return buf.String(), sup.RestoreStatus(req.ServerID), rec
}

// The whole of #1541 end to end: on a server whose backups go to S3, a
// point-in-time restore looks for the backup to fold from in the BUCKET,
// folds into the local directory, and sends the result to the destination the
// scheduled update would have written to.
//
// The local directory is staged EMPTY on purpose. That is the real shape of an
// S3-backed server: every backup it has was uploaded and pruned, or made by
// another host, and it is the shape that used to refuse with "no backup exists
// at or before" while the bucket held dozens.
func TestRunRestore_S3BackedServerFoldsFromTheBucketAndUploads(t *testing.T) {
	local := t.TempDir()
	listed, folded, uploads := stubS3Restore(t, []string{"shop.orders"}, nil)

	out, st, rec := runRestoreFor(t, console.BaselineRestoreRequest{
		ServerID: "s", ServerName: "s", IndexDSN: "d",
		BaselineDir: local, BaselineS3: "s3://bucket/backups/", At: restoreAt,
	})

	if out != "" {
		t.Fatalf("the restore warned: %s", out)
	}
	if !st.Published || st.State != "succeeded" {
		t.Fatalf("status = %+v, want a published success", st)
	}
	if len(*listed) != 1 || (*listed)[0] != "s3://bucket/backups/" {
		t.Fatalf("looked for the backup to fold from in %v, want the bucket once", *listed)
	}
	// Read from the bucket, write to the local directory: the two halves of
	// the fold config that the stub above is the only thing to observe.
	if len(*folded) != 1 || (*folded)[0].BaselineSrc != "s3://bucket/backups/" || (*folded)[0].OutputDir != local {
		t.Fatalf("fold config = %+v, want BaselineSrc=the bucket and OutputDir=%s", *folded, local)
	}
	stamp := reconstruct.SnapshotDirName(restoreAt)
	want := uploadCall{filepath.Join(local, stamp), "s3://bucket/backups/" + stamp}
	if len(*uploads) != 1 || (*uploads)[0] != want {
		t.Fatalf("uploads = %+v, want exactly %+v", *uploads, want)
	}
	// The history is how an operator later learns the snapshot reached the
	// bucket at all: without the count the only evidence is the absence of a
	// failure line.
	if rec == nil || rec.Kind != console.BaselineRunRestore || rec.Uploaded != 1 || rec.Error != "" {
		t.Fatalf("history record = %+v, want a restore with uploaded=1 and no error", rec)
	}
}

// A server with no S3 destination restores from and into its local directory,
// and nothing is uploaded: the gate is the request's own field, not a mode.
func TestRunRestore_withoutAnS3DestinationReadsLocalAndUploadsNothing(t *testing.T) {
	listed, folded, uploads := stubS3Restore(t, nil, nil)
	local := t.TempDir()
	writeSnapshotFiles(t, filepath.Join(local, "2026-08-28T09-00-00Z"), baseline.SuccessMarker)

	out, st, rec := runRestoreFor(t, console.BaselineRestoreRequest{
		ServerID: "s", ServerName: "s", IndexDSN: "d", BaselineDir: local, At: restoreAt,
	})

	if out != "" {
		t.Fatalf("the restore warned: %s", out)
	}
	if !st.Published || st.State != "succeeded" {
		t.Fatalf("status = %+v, want a published success", st)
	}
	if len(*listed) != 0 {
		t.Fatalf("a local-only server asked the bucket stub: %v", *listed)
	}
	if len(*folded) != 1 || (*folded)[0].BaselineSrc != local || (*folded)[0].OutputDir != local {
		t.Fatalf("fold config = %+v, want both BaselineSrc and OutputDir = %s", *folded, local)
	}
	if len(*uploads) != 0 {
		t.Fatalf("uploaded %+v, want nothing", *uploads)
	}
	if rec == nil || rec.Uploaded != 0 {
		t.Fatalf("history record = %+v, want uploaded=0", rec)
	}
}

// A restore is not done until the snapshot is where this server's backups
// live, so a failed upload fails the run. Two things must survive that: the
// local snapshot (it is finished, and it is the operator's whole remaining
// result), and a report that says the fold worked and the sending did not,
// rather than "published nothing".
func TestRunRestore_failedUploadFailsTheRunAndKeepsTheSnapshot(t *testing.T) {
	local := t.TempDir()
	stubS3Restore(t, []string{"shop.orders"}, errors.New("AccessDenied"))

	out, st, rec := runRestoreFor(t, console.BaselineRestoreRequest{
		ServerID: "s", ServerName: "s", IndexDSN: "d",
		BaselineDir: local, BaselineS3: "s3://bucket/backups/", At: restoreAt,
	})

	if st.State != "failed" || !st.Published {
		t.Fatalf("status = %+v, want failed AND published: the snapshot is on disk", st)
	}
	if !strings.Contains(st.LastError, "AccessDenied") || !strings.Contains(st.LastError, local) {
		t.Fatalf("last error = %q, want the S3 error and the local path the snapshot was kept at", st.LastError)
	}
	if !baseline.SnapshotComplete(filepath.Join(local, reconstruct.SnapshotDirName(restoreAt))) {
		t.Fatal("the local snapshot was not kept complete after the upload failed")
	}
	if !strings.Contains(out, "published locally, not uploaded") || strings.Contains(out, "published nothing") {
		t.Fatalf("warned: %q, want 'published locally, not uploaded' and NOT 'published nothing'", out)
	}
	// The record keeps the snapshot time: the operator can still restore
	// from it, so a record naming none would describe a run that produced
	// nothing.
	if rec == nil || rec.SnapshotTime == "" || rec.Uploaded != 0 || !strings.Contains(rec.Error, "AccessDenied") {
		t.Fatalf("history record = %+v, want the snapshot time, uploaded=0 and the error", rec)
	}
}

// TestRestoreFoldRequest_carriesTheS3Destination: dropping BaselineS3 from the
// translation compiles and passes every test that stubs the fold on
// OutputDir alone, and makes the restore the one fold that reads and writes
// the local directory alone on an S3-backed server (#1541).
func TestRestoreFoldRequest_carriesTheS3Destination(t *testing.T) {
	for _, want := range []string{"", "s3://bucket/backups/"} {
		got := restoreFoldRequest(console.BaselineRestoreRequest{
			ServerID: "srv1", ServerName: "wp", IndexDSN: "dsn", BaselineDir: "/b", BaselineS3: want,
		})
		if got.BaselineS3 != want {
			t.Errorf("BaselineS3 = %q, want %q", got.BaselineS3, want)
		}
		if got.ServerID != "srv1" || got.ServerName != "wp" || got.IndexDSN != "dsn" || got.BaselineDir != "/b" {
			t.Errorf("the restore request did not translate: %+v", got)
		}
	}
}

// A restore at the EXACT second of a backup the bucket already holds is
// refused before anything runs. The handler's own 409 covers a complete local
// snapshot at that instant; it never opens the bucket, and without this check
// the fold would rebuild that snapshot and the upload would overwrite it in
// place, _INCOMPLETE marker first, so a failure midway hides a complete
// remote backup from every listing (#1541).
func TestRunRestore_refusesTheExactInstantOfABackupTheBucketHolds(t *testing.T) {
	local := t.TempDir()
	listed, folded, uploads := stubS3Restore(t, []string{"shop.orders"}, nil)
	bucketCollides = true
	t.Cleanup(func() { bucketCollides = false })

	_, st, rec := runRestoreFor(t, console.BaselineRestoreRequest{
		ServerID: "s", ServerName: "s", IndexDSN: "d",
		BaselineDir: local, BaselineS3: "s3://bucket/backups/", At: restoreAt,
	})

	if st.State != "failed" || st.Published {
		t.Fatalf("status = %+v, want failed and NOT published: nothing may be written", st)
	}
	if !strings.Contains(st.LastError, "already exists at exactly") || !strings.Contains(st.LastError, "s3://bucket/backups/") {
		t.Fatalf("last error = %q, want the collision named with its location", st.LastError)
	}
	if len(*listed) != 1 || len(*folded) != 0 || len(*uploads) != 0 {
		t.Fatalf("listed=%d folded=%d uploads=%d, want the listing only: the fold and the upload must not run", len(*listed), len(*folded), len(*uploads))
	}
	if rec == nil || rec.SnapshotTime != "" || !strings.Contains(rec.Error, "already exists") {
		t.Fatalf("history record = %+v, want no snapshot time and the refusal", rec)
	}
}

// The collision is decided by the directory NAME, not the instant: a caller
// that hands the job a fractional second (the console truncates, another
// caller might not) must still be refused when the whole second is taken.
func TestRunRestore_refusesTheCollisionByDirectoryNameNotInstant(t *testing.T) {
	local := t.TempDir()
	_, folded, uploads := stubS3Restore(t, []string{"shop.orders"}, nil)
	bucketCollides = true
	t.Cleanup(func() { bucketCollides = false })
	realList := snapshotAt
	// The bucket answers with the WHOLE second while the request carries a
	// fraction of it: Equal would say "different", the directory says "same".
	snapshotAt = func(ctx context.Context, src string, at time.Time) ([]string, time.Time, error) {
		tables, anchor, err := realList(ctx, src, at)
		return tables, anchor.Truncate(time.Second), err
	}
	t.Cleanup(func() { snapshotAt = realList })

	_, st, _ := runRestoreFor(t, console.BaselineRestoreRequest{
		ServerID: "s", ServerName: "s", IndexDSN: "d",
		BaselineDir: local, BaselineS3: "s3://bucket/backups/", At: restoreAt.Add(500 * time.Millisecond),
	})

	if st.State != "failed" || st.Published || !strings.Contains(st.LastError, "already exists at exactly") {
		t.Fatalf("status = %+v, want the collision refusal", st)
	}
	if len(*folded) != 0 || len(*uploads) != 0 {
		t.Fatalf("folded=%d uploads=%d, want neither: the bucket's 10:00:00 snapshot would be overwritten", len(*folded), len(*uploads))
	}
}
