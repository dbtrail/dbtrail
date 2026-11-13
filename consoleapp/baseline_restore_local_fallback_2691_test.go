package consoleapp

import (
	"context"
	"os"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// A restore on a server with an S3 destination reads the bucket (#1541). When
// the bucket holds no snapshot at or before the moment, or cannot be read,
// and the local folder holds one, the restore starts from the local copy and
// says so (#2691). It never starts from a snapshot newer than the moment.

const fallbackBucket = "s3://bucket/backups/"

// stubBucket answers the restore's listing of the bucket with tables/anchor
// or err, and runs the REAL listing on the local folder. It records the fold
// configs and the uploads like stubS3Restore.
func stubBucket(t *testing.T, tables []string, anchor time.Time, err error) (folded *[]reconstruct.FullTableConfig, uploads *[]uploadCall, uploadErr *error) {
	t.Helper()
	_, folded, uploads = stubS3Restore(t, nil, nil)
	realList := reconstruct.SnapshotAt
	snapshotAt = func(ctx context.Context, src string, at time.Time) ([]string, time.Time, error) {
		if strings.HasPrefix(src, "s3://") {
			return tables, anchor, err
		}
		return realList(ctx, src, at)
	}
	var upErr error
	uploadSnapshot = func(_ context.Context, outputDir, dest, _ string, _ bool) (int, error) {
		*uploads = append(*uploads, uploadCall{outputDir, dest})
		if upErr != nil {
			return 0, upErr
		}
		return 1, nil
	}
	return folded, uploads, &upErr
}

func restoreS3Server(t *testing.T, local string) (string, console.BaselineStatus, *console.BaselineRunRecord) {
	t.Helper()
	return runRestoreFor(t, console.BaselineRestoreRequest{
		ServerID: "s", ServerName: "s", IndexDSN: "d",
		BaselineDir: local, BaselineS3: fallbackBucket, At: restoreAt,
	})
}

var errBucketDown = errors.New("operation error S3: ListObjectsV2, dial tcp: i/o timeout")

func TestRestoreFallback_anEmptyBucketRestoresFromTheLocalCopy(t *testing.T) {
	local := t.TempDir()
	older := "2026-08-28T09-00-00Z"
	writeSnapshotFiles(t, filepath.Join(local, older), baseline.SuccessMarker)
	folded, uploads, _ := stubBucket(t, nil, time.Time{}, nil)

	out, st, _ := restoreS3Server(t, local)
	if st.State != "succeeded" || !st.Published {
		t.Fatalf("status = %+v, want a published success from the local copy", st)
	}
	if len(*folded) != 1 || (*folded)[0].BaselineSrc != local || (*folded)[0].OutputDir != local {
		t.Fatalf("fold config = %+v, want it to read and write the local folder %s", *folded, local)
	}
	stamp := reconstruct.SnapshotDirName(restoreAt)
	if len(*uploads) != 1 || (*uploads)[0] != (uploadCall{filepath.Join(local, stamp), fallbackBucket + stamp}) {
		t.Fatalf("uploads = %+v, want the result sent to the bucket as usual", *uploads)
	}
	want := "The S3 location " + fallbackBucket + " holds no snapshot at or before 2026-08-28 10:00:00, so this restore started from the snapshot at 2026-08-28 09:00:00 in this server's local folder."
	if st.SourceNote != want {
		t.Fatalf("source note = %q, want %q", st.SourceNote, want)
	}
	if !strings.Contains(out, "restoring from the local folder") {
		t.Fatalf("the log does not say where the restore read: %q", out)
	}
	t.Logf("source note: %s", st.SourceNote)
}

// A bucket that cannot be listed may still hold a complete snapshot of this
// very second, which the collision check could not see: the result is
// published locally and NOT uploaded in this run, and the status says so.
// The cleanup asks the bucket about it later and sends it only if missing.
func TestRestoreFallback_anUnreachableBucketRestoresFromTheLocalCopyAndSaysSo(t *testing.T) {
	local := t.TempDir()
	writeSnapshotFiles(t, filepath.Join(local, "2026-08-28T09-00-00Z"), baseline.SuccessMarker)
	folded, uploads, _ := stubBucket(t, nil, time.Time{}, errBucketDown)
	_, st, rec := restoreS3Server(t, local)
	if !st.Published || st.State != "failed" {
		t.Fatalf("status = %+v, want published here and not sent", st)
	}
	if len(*folded) != 1 || (*folded)[0].BaselineSrc != local {
		t.Fatalf("fold config = %+v, want the local folder", *folded)
	}
	if len(*uploads) != 0 {
		t.Fatalf("uploaded %+v to a bucket that could not be listed: a complete snapshot of that second there would be overwritten", *uploads)
	}
	stamp := reconstruct.SnapshotDirName(restoreAt)
	wantErr := "the snapshot was not sent to the snapshot destination: it was written to " + filepath.Join(local, stamp) + " and not sent to " + fallbackBucket + ", because " + fallbackBucket + " could not be read, so it is not known whether it already holds a snapshot of that second; the cleanup sends it later, skipping anything S3 already has, so a complete copy there is never replaced"
	if st.LastError != wantErr {
		t.Fatalf("last error = %q, want %q", st.LastError, wantErr)
	}
	want := "The S3 location " + fallbackBucket + " could not be read (" + errBucketDown.Error() + "), so this restore started from the snapshot at 2026-08-28 09:00:00 in this server's local folder. A newer snapshot may be in S3."
	if st.SourceNote != want {
		t.Fatalf("source note = %q, want %q", st.SourceNote, want)
	}
	if rec == nil || rec.SnapshotTime == "" {
		t.Fatalf("history record = %+v, want the snapshot time of the local result", rec)
	}
	t.Logf("source note: %s", st.SourceNote)
	t.Logf("last error: %s", st.LastError)
}

// Never from a snapshot newer than the moment: a local folder whose only
// snapshot is after it is no fallback, and the refusal stays.
func TestRestoreFallback_neverFromASnapshotNewerThanTheMoment(t *testing.T) {
	for _, bucket := range []string{"empty", "unreachable"} {
		t.Run(bucket, func(t *testing.T) {
			local := t.TempDir()
			writeSnapshotFiles(t, filepath.Join(local, "2026-08-28T11-00-00Z"), baseline.SuccessMarker)
			var berr error
			if bucket == "unreachable" {
				berr = errBucketDown
			}
			folded, uploads, _ := stubBucket(t, nil, time.Time{}, berr)
			_, st, _ := restoreS3Server(t, local)
			if st.State != "failed" || st.Published || len(*folded) != 0 || len(*uploads) != 0 {
				t.Fatalf("status = %+v folded=%d uploads=%d: a snapshot after the moment was folded from", st, len(*folded), len(*uploads))
			}
			if st.SourceNote != "" {
				t.Fatalf("a refused restore names a source: %q", st.SourceNote)
			}
			var want string
			if bucket == "empty" {
				want = "no snapshot exists at or before 2026-08-28 10:00:00; a restore folds an existing snapshot forward, so pick a moment after your oldest snapshot"
			} else {
				want = "list the snapshot to restore from: " + errBucketDown.Error() + "; the local folder " + local + " holds no snapshot at or before 2026-08-28 10:00:00 either"
			}
			if st.LastError != want {
				t.Fatalf("last error = %q, want %q", st.LastError, want)
			}
			t.Logf("refusal (%s bucket): %s", bucket, st.LastError)
		})
	}
}

// A local folder that cannot be listed is no fallback either: both failures
// are named.
func TestRestoreFallback_bothLocationsFail(t *testing.T) {
	// A path that is not a directory: it cannot be listed, and the mark the
	// restore writes first cannot create it either. (A folder that does not
	// exist yet is created by that mark, and then holds no snapshot.)
	local := filepath.Join(t.TempDir(), "not-a-folder")
	if err := os.WriteFile(local, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	folded, _, _ := stubBucket(t, nil, time.Time{}, errBucketDown)
	_, st, _ := restoreS3Server(t, local)
	if st.State != "failed" || len(*folded) != 0 {
		t.Fatalf("status = %+v folded=%d", st, len(*folded))
	}
	if !strings.HasPrefix(st.LastError, "list the snapshot to restore from: "+errBucketDown.Error()+"; the local folder "+local+" could not be read either: ") {
		t.Fatalf("last error = %q, want both failures named", st.LastError)
	}
	t.Logf("refusal (both down): %s", st.LastError)
}

// A bucket that answers with a snapshot is read as before, even when the
// local folder holds a newer one at or before the moment (#1539: the local
// folder may be the one that is behind), and the status says nothing.
func TestRestoreFallback_aBucketThatAnswersIsStillTheSource(t *testing.T) {
	local := t.TempDir()
	writeSnapshotFiles(t, filepath.Join(local, "2026-08-28T09-30-00Z"), baseline.SuccessMarker)
	folded, _, _ := stubBucket(t, []string{"shop.orders"}, restoreAt.Add(-time.Hour), nil)
	_, st, _ := restoreS3Server(t, local)
	if st.State != "succeeded" || len(*folded) != 1 || (*folded)[0].BaselineSrc != fallbackBucket {
		t.Fatalf("status = %+v folded=%+v, want the bucket read", st, *folded)
	}
	if st.SourceNote != "" {
		t.Fatalf("source note = %q on a restore that read the bucket", st.SourceNote)
	}
}

// The status slot is reused: a restore that read the bucket clears the note
// a fallback before it left.
func TestRestoreFallback_theNoteIsClearedByTheNextRun(t *testing.T) {
	local := t.TempDir()
	stubBucket(t, []string{"shop.orders"}, restoreAt.Add(-time.Hour), nil)
	sup := newBaselineSupervisor(context.Background(), t.TempDir(), baseline.DefaultLockMode)
	sup.restores["s"] = &console.BaselineStatus{State: "running", SourceNote: "from the run before"}
	sup.runRestore(console.BaselineRestoreRequest{
		ServerID: "s", ServerName: "s", IndexDSN: "d",
		BaselineDir: local, BaselineS3: fallbackBucket, At: restoreAt,
	})
	if st := sup.RestoreStatus("s"); st.State != "succeeded" || st.SourceNote != "" {
		t.Fatalf("status = %+v, want a success with the previous note cleared", st)
	}
}
