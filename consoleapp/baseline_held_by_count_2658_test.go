package consoleapp

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// #2658: with a count and an S3 destination, a snapshot held from upload on
// purpose goes by the count alone. These tests are the two things that must
// survive that: the newest held snapshot, whose upload the interval still
// owes the bucket, and a snapshot whose upload was TRIED and failed, which
// is not "held on purpose" and must wait for the bucket.

// heldFolder writes n complete snapshots an hour apart, the newest ending
// two hours before now, every one of them held, and returns their names
// oldest first with the instant of the newest.
func heldFolder(t *testing.T, n int, now time.Time) (dir string, names []string, newest time.Time) {
	t.Helper()
	dir = t.TempDir()
	for i := n; i >= 1; i-- {
		at := now.Add(-time.Duration(i+1) * time.Hour)
		name := reconstruct.SnapshotDirName(at)
		writeSnapshotFiles(t, filepath.Join(dir, name), baseline.SuccessMarker)
		if err := baseline.MarkUploadHeld(dir, name); err != nil {
			t.Fatal(err)
		}
		names = append(names, name)
		newest = at
	}
	return dir, names, newest
}

func pruneByCount(t *testing.T, dir string, keep int, now time.Time, inBucket ...string) baseline.PruneResult {
	t.Helper()
	res, err := baseline.PruneLocalAsking(context.Background(), baseline.PruneOptions{LocalDir: dir, S3URL: "s3://bkt/p/", KeepNewest: keep, Now: now},
		func(_ context.Context, name string) (bool, error) { return slices.Contains(inBucket, name), nil })
	if err != nil {
		t.Fatal(err)
	}
	return res
}

// The cleanup removes the held snapshots past the count and never the newest
// one, whose mark is what tells the next quiet cycle that S3 is owed an
// upload. That cycle then sends it and clears the mark; no mark is left
// pointing at a folder that is gone, and nothing is sent twice.
func TestHeldByCount_theOwedUploadSurvivesTheCleanup(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	dir, names, newestAt := heldFolder(t, 6, now)
	newest := names[len(names)-1]

	res := pruneByCount(t, dir, 3, now)
	slices.Sort(res.Pruned)
	if !slices.Equal(res.Pruned, names[:3]) {
		t.Fatalf("pruned %v, want the three held ones past the count %v", res.Pruned, names[:3])
	}
	if got := snapshotsIn(t, dir); !slices.Equal(got, names[3:]) {
		t.Fatalf("on disk %v, want the newest three %v", got, names[3:])
	}
	if held, _ := baseline.UploadHeld(dir, newest); !held {
		t.Fatal("the newest snapshot lost its mark: the owed upload would never be sent")
	}
	for _, gone := range names[:3] {
		if _, err := os.Stat(filepath.Join(dir, ".upload-held."+gone)); !os.IsNotExist(err) {
			t.Errorf("a mark points at %s, which is gone (err %v)", gone, err)
		}
	}

	// The upload slot moved on since the bucket's newest: the quiet cycle
	// sends the newest snapshot, that one and nothing else.
	stubBucketNewest(t, newestAt.Add(-26*time.Hour), nil)
	var sent []string
	fail := errors.New("AccessDenied")
	failing := true
	prev := uploadSnapshot
	t.Cleanup(func() { uploadSnapshot = prev })
	uploadSnapshot = func(_ context.Context, _, dest, _ string, _ bool) (int, error) {
		sent = append(sent, dest)
		if failing {
			return 0, fail
		}
		return 1, nil
	}
	s := newBaselineSupervisor(context.Background(), t.TempDir(), "")
	req := refreshRequest{ServerID: "s", ServerName: "s", BaselineDir: dir, BaselineS3: "s3://bkt/p/", UploadEvery: 24 * time.Hour}

	// First the upload fails. The snapshot keeps its mark, so the next quiet
	// cycle tries again, and being the newest it is inside the count: a
	// cleanup in between does not touch it.
	s.sendOwedHeldUpload(req, now)
	if held, _ := baseline.UploadHeld(dir, newest); !held || len(sent) != 1 {
		t.Fatalf("after a failed owed upload: held %v, attempts %v; want it still owed", held, sent)
	}
	if res := pruneByCount(t, dir, 3, now); len(res.Pruned) != 0 {
		t.Fatalf("a cleanup after the failed upload removed %v", res.Pruned)
	}
	if _, err := os.Stat(filepath.Join(dir, newest, baseline.SuccessMarker)); err != nil {
		t.Fatalf("the snapshot whose upload is owed is gone: %v", err)
	}

	// Then it works, at the first try after the hour a failed send waits
	// (#2675): sent once more, to its own key, and the mark goes.
	failing = false
	now = now.Add(owedRetryWait)
	s.sendOwedHeldUpload(req, now)
	if want := "s3://bkt/p/" + newest; len(sent) != 2 || sent[1] != want {
		t.Fatalf("uploads %v, want a second one to %s", sent, want)
	}
	if held, _ := baseline.UploadHeld(dir, newest); held {
		t.Fatal("the mark outlived the upload")
	}
	// Sent now, and still one of the newest three: it stays, and the next
	// cycle has nothing left to send.
	if res := pruneByCount(t, dir, 3, now, newest); len(res.Pruned) != 0 {
		t.Fatalf("a cleanup after the upload removed %v", res.Pruned)
	}
	stubBucketNewest(t, newestAt, nil)
	s.sendOwedHeldUpload(req, now)
	if len(sent) != 2 {
		t.Fatalf("sent again after it reached S3: %v", sent)
	}
}

// A run whose upload slot came and whose upload FAILED leaves a snapshot with
// no held mark: it was meant for the bucket. The count never removes it while
// the bucket does not have it, however many held ones go around it, and it
// goes once the bucket confirms it.
func TestHeldByCount_aFailedUploadIsNotHeld(t *testing.T) {
	stubGateReads(t)
	local := stageBaselineRoot(t)
	uploads, _ := stubS3Fold(t, []string{"shop.orders"}, errors.New("AccessDenied"))
	// The bucket's newest is from before the slot: the upload is due.
	stubBucketNewest(t, refreshAt.Add(-55*time.Minute), nil)
	injectFold(t, 0, nil)
	sup := newBaselineSupervisor(context.Background(), t.TempDir(), baseline.DefaultLockMode)
	sup.history = openHistoryForTest(t)
	sup.refreshes["s"] = &console.BaselineStatus{State: "running"}
	sup.runRefresh(refreshRequest{ServerID: "s", ServerName: "s", IndexDSN: "d", BaselineDir: local, BaselineS3: "s3://bucket/backups/",
		UploadEvery: time.Hour, UploadAt: 10 * time.Minute}, refreshAt, 5*time.Minute)
	if len(*uploads) != 1 {
		t.Fatalf("uploads = %+v, want one attempt", *uploads)
	}
	failed := reconstruct.SnapshotDirName(refreshAt)
	if held, err := baseline.UploadHeld(local, failed); err != nil || held {
		t.Fatalf("a snapshot whose upload was tried and failed is marked held (held %v, err %v)", held, err)
	}

	// The same shape on disk, with held updates after it, pruned by count.
	dir := t.TempDir()
	now := refreshAt.Add(10 * time.Hour)
	writeSnapshotFiles(t, filepath.Join(dir, failed), baseline.SuccessMarker)
	var later []string
	for h := 1; h <= 5; h++ {
		name := reconstruct.SnapshotDirName(refreshAt.Add(time.Duration(h) * time.Hour))
		writeSnapshotFiles(t, filepath.Join(dir, name), baseline.SuccessMarker)
		if err := baseline.MarkUploadHeld(dir, name); err != nil {
			t.Fatal(err)
		}
		later = append(later, name)
	}
	res := pruneByCount(t, dir, 3, now)
	slices.Sort(res.Pruned)
	if !slices.Equal(res.Pruned, later[:2]) {
		t.Fatalf("pruned %v, want only the two held ones past the count %v", res.Pruned, later[:2])
	}
	if !slices.Equal(res.NotAtDestination, []string{failed}) {
		t.Fatalf("named for a resend: %v, want %s", res.NotAtDestination, failed)
	}
	if got := snapshotsIn(t, dir); !slices.Equal(got, append([]string{failed}, later[2:]...)) {
		t.Fatalf("on disk %v, want the failed upload and the newest three", got)
	}
	// Once the bucket has it, it goes like any other past the count.
	if res := pruneByCount(t, dir, 3, now, failed); !slices.Equal(res.Pruned, []string{failed}) {
		t.Fatalf("with the copy confirmed: pruned %v, want %s", res.Pruned, failed)
	}
}

// A bucket that answers for some snapshots and times out for others leaves
// the record of what S3 lacks as the last full answer left it. The prune
// does not touch it, and neither does the resend that follows: the one
// snapshot the bucket did answer for is sent again, and the count on the
// page stays at 10, where 10 - 9 unanswered - 1 resent would have removed
// the record and read as "S3 has them all".
func TestPruneCycle_aHalfSilentBucketKeepsTheRecordOfWhatS3Lacks(t *testing.T) {
	for _, resendFails := range []bool{false, true} {
		dir := t.TempDir()
		name := "2026-01-01T00-00-00Z"
		writeSnapshotFiles(t, filepath.Join(dir, name), baseline.SuccessMarker)
		before := baseline.NotInDestination{At: time.Date(2026, 1, 2, 3, 0, 0, 0, time.UTC), Count: 10, Names: []string{name}}
		if err := baseline.RewriteNotInDestination(dir, before); err != nil {
			t.Fatal(err)
		}
		var resent []string
		prev := resendSnapshotFn
		t.Cleanup(func() { resendSnapshotFn = prev })
		resendSnapshotFn = func(_ context.Context, _, _, n string) error {
			resent = append(resent, n)
			if resendFails {
				return errors.New("network")
			}
			return nil
		}
		prune := func(context.Context, baseline.PruneOptions) (baseline.PruneResult, error) {
			return baseline.PruneResult{DestinationSilent: true, KeptNotDurable: 10, ProbeErrors: 9, NotAtDestination: []string{name}}, nil
		}
		runBaselinePruneCycle(context.Background(), []baselinePruneTarget{{dir: dir, s3: "s3://b/p", keepNewest: 3}}, 0, prune)
		if !slices.Equal(resent, []string{name}) {
			t.Fatalf("resend fails=%v: resent %v, want the one snapshot the bucket said it lacks", resendFails, resent)
		}
		rec, ok, err := baseline.ReadNotInDestination(dir)
		if err != nil || !ok || rec.Count != 10 || !rec.At.Equal(before.At) {
			t.Fatalf("resend fails=%v: record %+v ok=%v err=%v, want the 10 of the last full answer, untouched", resendFails, rec, ok, err)
		}
	}
}
