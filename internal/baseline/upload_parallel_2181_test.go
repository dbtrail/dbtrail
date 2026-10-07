package baseline

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

	"github.com/dbtrail/dbtrail/internal/storage"
)

// lockedOps serializes the bodies of a fake's functions. The upload sends a
// snapshot's files several at a time (#2181), so a fake that appends to a
// slice or writes a map from them would race; the lock makes the RECORDING
// safe without making the uploads sequential.
func lockedOps(ops s3UploadOps) s3UploadOps {
	var mu sync.Mutex
	wrap := func(f func(context.Context, string) error) func(context.Context, string) error {
		if f == nil {
			return nil
		}
		return func(ctx context.Context, k string) error { mu.Lock(); defer mu.Unlock(); return f(ctx, k) }
	}
	out := ops
	out.putEmpty = wrap(ops.putEmpty)
	out.deleteObject = wrap(ops.deleteObject)
	if f := ops.uploadFile; f != nil {
		out.uploadFile = func(ctx context.Context, p, k string) error { mu.Lock(); defer mu.Unlock(); return f(ctx, p, k) }
	}
	if f := ops.copyObject; f != nil {
		out.copyObject = func(ctx context.Context, src, k string) error { mu.Lock(); defer mu.Unlock(); return f(ctx, src, k) }
	}
	if f := ops.objectExists; f != nil {
		out.objectExists = func(ctx context.Context, k string) (bool, error) { mu.Lock(); defer mu.Unlock(); return f(ctx, k) }
	}
	if f := ops.putObject; f != nil {
		out.putObject = func(ctx context.Context, k string, b []byte) error { mu.Lock(); defer mu.Unlock(); return f(ctx, k, b) }
	}
	if f := ops.getObject; f != nil {
		out.getObject = func(ctx context.Context, k string) ([]byte, bool, error) {
			mu.Lock()
			defer mu.Unlock()
			return f(ctx, k)
		}
	}
	return out
}

// parallelFixture writes one complete snapshot holding n data files.
func parallelFixture(t *testing.T, n int) string {
	t.Helper()
	out := t.TempDir()
	snap := filepath.Join(out, "2026-10-06T12-00-00Z")
	if err := os.MkdirAll(filepath.Join(snap, "wp"), 0o755); err != nil {
		t.Fatal(err)
	}
	for i := range n {
		if err := os.WriteFile(filepath.Join(snap, "wp", fmt.Sprintf("t%02d.parquet", i)), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(snap, SuccessMarker), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	return out
}

// #2181: a snapshot update with an S3 destination spent most of its time
// sending its files one at a time, about 40 requests for twelve small tables
// at a round trip each. The data files now go several at a time, and the
// crash-safety bracket around them must hold exactly as before: _INCOMPLETE
// before the first data file starts, _SUCCESS only after the last one has
// FINISHED (not merely started), then the cleanup.
func TestUploadWithOps_sendsDataFilesConcurrently_2181(t *testing.T) {
	const files = 3 * uploadConcurrency
	out := parallelFixture(t, files)

	var mu sync.Mutex
	var events []string
	inFlight, peak, finished := 0, 0, 0
	// Every data upload waits until a second one is in flight (or a
	// deadline passes). A sequential walk never has two, so it fails here
	// instead of passing slowly.
	twoInFlight := make(chan struct{})
	var once sync.Once
	ops := s3UploadOps{
		putEmpty: func(_ context.Context, k string) error {
			mu.Lock()
			defer mu.Unlock()
			events = append(events, "put "+k)
			return nil
		},
		uploadFile: func(_ context.Context, _, k string) error {
			mu.Lock()
			if strings.HasSuffix(k, SuccessMarker) {
				events = append(events, fmt.Sprintf("success after %d finished, %d in flight", finished, inFlight))
				mu.Unlock()
				return nil
			}
			inFlight++
			peak = max(peak, inFlight)
			if inFlight >= 2 {
				once.Do(func() { close(twoInFlight) })
			}
			mu.Unlock()
			select {
			case <-twoInFlight:
			case <-time.After(5 * time.Second):
				return errors.New("no second upload started while this one was in flight: the files are sent one at a time")
			}
			time.Sleep(time.Millisecond)
			mu.Lock()
			inFlight--
			finished++
			events = append(events, "data "+k)
			mu.Unlock()
			return nil
		},
		objectExists: func(_ context.Context, _ string) (bool, error) { return false, nil },
		deleteObject: func(_ context.Context, k string) error {
			mu.Lock()
			defer mu.Unlock()
			events = append(events, "delete "+k)
			return nil
		},
	}
	n, err := uploadWithOps(context.Background(), out, "p", false, ops)
	if err != nil {
		t.Fatalf("uploadWithOps: %v", err)
	}
	if n != files+1 {
		t.Errorf("uploaded %d objects, want %d (every data file and _SUCCESS)", n, files+1)
	}
	if peak < 2 || peak > uploadConcurrency {
		t.Errorf("peak concurrent uploads = %d, want between 2 and %d", peak, uploadConcurrency)
	}
	want := fmt.Sprintf("success after %d finished, 0 in flight", files)
	var sawSuccess, sawDelete bool
	for i, e := range events {
		switch {
		case i == 0 && !strings.HasSuffix(e, IncompleteMarker):
			t.Errorf("first call = %q, want the _INCOMPLETE marker before any data", e)
		case strings.HasPrefix(e, "success"):
			sawSuccess = true
			if e != want {
				t.Errorf("_SUCCESS sent at %q, want %q: it must follow every data file", e, want)
			}
		case strings.HasPrefix(e, "delete"):
			sawDelete = true
			if !sawSuccess {
				t.Errorf("_INCOMPLETE removed before _SUCCESS was sent: %v", events)
			}
		case strings.HasPrefix(e, "data") && sawSuccess:
			t.Errorf("data file %q finished after _SUCCESS", e)
		}
	}
	if !sawSuccess || !sawDelete {
		t.Errorf("calls %v lack _SUCCESS or the _INCOMPLETE cleanup", events)
	}
}

// A failed data file stops the upload before _SUCCESS, so the remote copy
// stays marked incomplete, and the files not yet started are not sent.
func TestUploadWithOps_failedFileWithholdsSuccess_2181(t *testing.T) {
	const files = 4 * uploadConcurrency
	out := parallelFixture(t, files)
	var mu sync.Mutex
	started, success := 0, false
	// The other files wait until t00 has failed, so how the scheduler
	// orders the goroutines cannot let every file finish first.
	failed := make(chan struct{})
	ops := s3UploadOps{
		putEmpty: func(context.Context, string) error { return nil },
		uploadFile: func(ctx context.Context, _, k string) error {
			mu.Lock()
			if strings.HasSuffix(k, SuccessMarker) {
				success = true
				mu.Unlock()
				return nil
			}
			started++
			mu.Unlock()
			if strings.HasSuffix(k, "t00.parquet") {
				close(failed)
				return errors.New("boom")
			}
			select {
			case <-failed:
			case <-time.After(5 * time.Second):
			}
			select {
			case <-ctx.Done():
			case <-time.After(5 * time.Second):
			}
			return ctx.Err()
		},
		objectExists: func(context.Context, string) (bool, error) { return false, nil },
		deleteObject: func(context.Context, string) error { return nil },
	}
	_, err := uploadWithOps(context.Background(), out, "p", false, ops)
	if err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("err = %v, want the failed file's error", err)
	}
	if success {
		t.Error("_SUCCESS was sent although a data file failed")
	}
	mu.Lock()
	defer mu.Unlock()
	if started == files {
		t.Errorf("all %d files were attempted after the first failure; the rest should stop", files)
	}
}

// A large file is bandwidth-bound and the SDK already sends it in concurrent
// parts, so it goes alone, after the small files: nothing else is in flight
// while it uploads, and it still finishes before _SUCCESS.
func TestUploadWithOps_largeFilesGoAlone_2181(t *testing.T) {
	out := parallelFixture(t, 2*uploadConcurrency)
	snap := filepath.Join(out, "2026-10-06T12-00-00Z", "wp")
	for _, name := range []string{"big1.parquet", "big2.parquet"} {
		f, err := os.Create(filepath.Join(snap, name))
		if err != nil {
			t.Fatal(err)
		}
		if err := f.Truncate(uploadParallelMaxSize); err != nil { // sparse: no real 16 MiB written
			t.Fatal(err)
		}
		f.Close()
	}
	var mu sync.Mutex
	inFlight, bigSeen := 0, 0
	var problems []string
	ops := s3UploadOps{
		putEmpty: func(context.Context, string) error { return nil },
		uploadFile: func(_ context.Context, _, k string) error {
			mu.Lock()
			if strings.HasSuffix(k, SuccessMarker) {
				if bigSeen != 2 {
					problems = append(problems, fmt.Sprintf("_SUCCESS sent after %d of 2 large files", bigSeen))
				}
				mu.Unlock()
				return nil
			}
			inFlight++
			mu.Unlock()
			time.Sleep(20 * time.Millisecond)
			mu.Lock()
			if strings.Contains(k, "big") {
				bigSeen++
				if inFlight != 1 {
					problems = append(problems, fmt.Sprintf("%s uploaded with %d uploads in flight", k, inFlight))
				}
			}
			inFlight--
			mu.Unlock()
			return nil
		},
		objectExists: func(context.Context, string) (bool, error) { return false, nil },
		deleteObject: func(context.Context, string) error { return nil },
	}
	n, err := uploadWithOps(context.Background(), out, "p", false, ops)
	if err != nil {
		t.Fatal(err)
	}
	if want := 2*uploadConcurrency + 2 + 1; n != want {
		t.Errorf("uploaded %d objects, want %d", n, want)
	}
	for _, p := range problems {
		t.Error(p)
	}
}

// The caller's context cancelled while the data files are going up: the
// upload reports it, says what it was doing, and _SUCCESS is not sent, even
// when every job that started returned without an error of its own.
func TestUploadWithOps_cancelledMidwayWithholdsSuccess_2181(t *testing.T) {
	out := parallelFixture(t, 3*uploadConcurrency)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var mu sync.Mutex
	success := false
	ops := s3UploadOps{
		putEmpty: func(context.Context, string) error { return nil },
		uploadFile: func(_ context.Context, _, k string) error {
			mu.Lock()
			defer mu.Unlock()
			if strings.HasSuffix(k, SuccessMarker) {
				success = true
			}
			cancel() // the daemon is shutting down; this file itself went up fine
			return nil
		},
		objectExists: func(context.Context, string) (bool, error) { return false, nil },
		deleteObject: func(context.Context, string) error { return nil },
	}
	_, err := uploadWithOps(ctx, out, "p", false, ops)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want the cancellation", err)
	}
	if !strings.Contains(err.Error(), "before every data file was sent") {
		t.Errorf("err = %q, want it to say the upload stopped partway", err)
	}
	if success {
		t.Error("_SUCCESS was sent after the caller cancelled the upload")
	}
}

// The files sent eight at a time must each be ONE request: a multipart
// upload cancelled by a sibling's failure leaves billed parts behind.
func TestUploadParallelFilesAreSinglePuts_2181(t *testing.T) {
	if uploadParallelMaxSize >= storage.UploadFileSinglePutMax {
		t.Fatalf("files up to %d bytes go up concurrently, but a file from %d bytes is a multipart upload",
			uploadParallelMaxSize, storage.UploadFileSinglePutMax)
	}
}

// A panic in one upload goroutine must not take the process down: the
// daemon uploading is also the one capturing. It becomes the upload's error,
// and _SUCCESS is not sent.
func TestUploadWithOps_panicInAnUploadIsAnError_2181(t *testing.T) {
	out := parallelFixture(t, 2*uploadConcurrency)
	var mu sync.Mutex
	success := false
	ops := s3UploadOps{
		putEmpty: func(context.Context, string) error { return nil },
		uploadFile: func(_ context.Context, _, k string) error {
			if strings.HasSuffix(k, "t03.parquet") {
				panic("boom in the uploader")
			}
			mu.Lock()
			defer mu.Unlock()
			if strings.HasSuffix(k, SuccessMarker) {
				success = true
			}
			return nil
		},
		objectExists: func(context.Context, string) (bool, error) { return false, nil },
		deleteObject: func(context.Context, string) error { return nil },
	}
	_, err := uploadWithOps(context.Background(), out, "p", false, ops)
	if err == nil || !strings.Contains(err.Error(), "boom in the uploader") {
		t.Fatalf("err = %v, want the panic reported as the upload's error", err)
	}
	if success {
		t.Error("_SUCCESS was sent after an upload panicked")
	}
}
