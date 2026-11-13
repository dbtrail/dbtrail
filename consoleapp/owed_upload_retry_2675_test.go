package consoleapp

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// #2675 point 6: the upload the interval owes S3 is the newest snapshot and
// keeps its hold mark when it fails, so it is tried again. It was tried at
// every quiet cycle (every 5 minutes by default), each try sending the whole
// snapshot and logging a warning. After a failure it now waits an hour, the
// failed re-sends' pace; a success clears the wait, and the mark keeps the
// snapshot on disk meanwhile.
func TestSendOwedHeldUpload_aFailedSendWaitsAnHour(t *testing.T) {
	newestLocal := time.Date(2026, 11, 1, 10, 5, 0, 0, time.UTC)
	local := t.TempDir()
	name := reconstruct.SnapshotDirName(newestLocal)
	writeSnapshotFiles(t, filepath.Join(local, reconstruct.SnapshotDirName(newestLocal.Add(-time.Hour))), baseline.SuccessMarker)
	writeSnapshotFiles(t, filepath.Join(local, name), baseline.SuccessMarker)
	if err := baseline.MarkUploadHeld(local, name); err != nil {
		t.Fatal(err)
	}
	stubBucketNewest(t, newestLocal.Add(-5*time.Minute), nil)
	tries, fail := 0, true
	prev := uploadSnapshot
	t.Cleanup(func() { uploadSnapshot = prev })
	uploadSnapshot = func(context.Context, string, string, string, bool) (int, error) {
		tries++
		if fail {
			return 0, errors.New("put object: connection reset")
		}
		return 1, nil
	}
	logs := captureSlogFor(t)
	s := newBaselineSupervisor(context.Background(), t.TempDir(), "")
	req := refreshRequest{ServerID: "s", ServerName: "s", BaselineDir: local, BaselineS3: "s3://bkt/p/", UploadEvery: time.Hour}
	start := newestLocal.Add(time.Hour)
	var perCycle []int
	for i := 0; i < 12; i++ { // an hour of 5-minute quiet cycles
		s.sendOwedHeldUpload(req, start.Add(time.Duration(i)*5*time.Minute))
		perCycle = append(perCycle, tries)
	}
	if tries != 1 {
		t.Fatalf("tries over the first hour %v, want 1: a failed send is not tried again within the hour", perCycle)
	}
	if held, _ := baseline.UploadHeld(local, name); !held {
		t.Fatal("the failed send lost the hold mark")
	}
	t.Logf("log: %s", logs.String())
	if strings.Count(logs.String(), "it stays here, and sending it is tried again in an hour") != 1 {
		t.Errorf("the failure is not said once: %s", logs.String())
	}
	// After the hour it is tried again, and once it goes the wait is gone.
	fail = false
	s.sendOwedHeldUpload(req, start.Add(time.Hour))
	if tries != 2 {
		t.Fatalf("after the hour: %d tries, want 2", tries)
	}
	if held, _ := baseline.UploadHeld(local, name); held {
		t.Fatal("the mark outlived the upload")
	}
	s.mu.Lock()
	_, waiting := s.owedRetryAt["s"]
	s.mu.Unlock()
	if waiting {
		t.Error("a send that worked left a wait behind")
	}
}
