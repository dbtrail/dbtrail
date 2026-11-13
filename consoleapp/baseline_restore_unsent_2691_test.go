package consoleapp

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// A restore whose bucket could not be listed publishes its result locally
// and does not send it (#2691). Three things hold after it: the result keeps
// its finished mark (the mark is decided on the fold's outcome, before the
// upload's), the cleanup sends it again later even while S3 is still down,
// and that send skips every file S3 already has, so a complete copy there is
// never replaced.

func unsentRestore(t *testing.T) (local, name string) {
	t.Helper()
	local = t.TempDir()
	writeSnapshotFiles(t, filepath.Join(local, "2026-08-28T09-00-00Z"), baseline.SuccessMarker)
	stubBucket(t, nil, time.Time{}, errBucketDown)
	_, st, _ := restoreS3Server(t, local)
	if st.State != "failed" || !st.Published {
		t.Fatalf("status = %+v, want published and not sent", st)
	}
	return local, reconstruct.SnapshotDirName(restoreAt)
}

func TestRestoreUnsent_theResultKeepsItsFinishedMark(t *testing.T) {
	local, name := unsentRestore(t)
	b, err := os.ReadFile(filepath.Join(local, ".restored."+name))
	if err != nil {
		t.Fatalf("a result published but not sent lost its mark, so the count removes it: %v", err)
	}
	if strings.HasPrefix(string(b), "restoring") {
		t.Fatalf("the mark of a published result is still pending: %q", b)
	}
	if got, _ := baseline.Restored(local); got != name {
		t.Fatalf("Restored = %q, want the result %q", got, name)
	}
}

func TestRestoreUnsent_theCleanupSendsItAgainWhileS3IsStillDown(t *testing.T) {
	local, name := unsentRestore(t)
	prev := resendSnapshotFn
	t.Cleanup(func() { resendSnapshotFn = prev })
	var tried []string
	resendSnapshotFn = func(_ context.Context, _, _, n string) error {
		tried = append(tried, n)
		return errBucketDown
	}
	down := func(ctx context.Context, o baseline.PruneOptions) (baseline.PruneResult, error) {
		return baseline.PruneLocalAsking(ctx, o, func(context.Context, string) (bool, error) { return false, errBucketDown })
	}
	runBaselinePruneCycle(context.Background(), []baselinePruneTarget{{dir: local, s3: fallbackBucket, keepNewest: 1}}, 0, down)
	if !slices.Contains(tried, name) {
		t.Fatalf("the cleanup tried %v, want the unsent restore's result %s", tried, name)
	}
	if _, err := os.Stat(filepath.Join(local, name, baseline.SuccessMarker)); err != nil {
		t.Fatalf("the result is gone from this machine: %v", err)
	}
}

func TestRestoreUnsent_theSendSkipsWhatS3AlreadyHas(t *testing.T) {
	local, name := unsentRestore(t)
	prevCut := s3PruneCutFn
	t.Cleanup(func() { s3PruneCutFn = prevCut })
	s3PruneCutFn = nil // no S3 age: nothing asks whether the folder is being removed
	realUpload := uploadSnapshot
	t.Cleanup(func() { uploadSnapshot = realUpload })
	var skipExisting []bool
	uploadSnapshot = func(_ context.Context, _, _, _ string, skip bool) (int, error) {
		skipExisting = append(skipExisting, skip)
		return 0, nil
	}
	sup := newBaselineSupervisor(context.Background(), t.TempDir(), baseline.DefaultLockMode)
	if err := sup.resendLacking(context.Background(), local, fallbackBucket, name); err != nil && !errors.Is(err, errNotResent) {
		t.Fatal(err)
	}
	if !slices.Equal(skipExisting, []bool{true}) {
		t.Fatalf("the cleanup's send ran with skip-existing %v, want one send that skips what S3 already has", skipExisting)
	}
}
