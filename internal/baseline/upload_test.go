package baseline

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestUploadWithOps_ordering pins the crash-safe S3 upload ordering (#524
// review): _INCOMPLETE is published before any data, _SUCCESS only after all
// data, and _INCOMPLETE is deleted last. The s3UploadOps seam records the call
// order without a live client, so a future refactor that reorders the steps (the
// original #467-on-upload bug) fails this test instead of silently shipping.
func TestUploadWithOps_ordering(t *testing.T) {
	outputDir := t.TempDir()
	snap := filepath.Join(outputDir, "2025-01-01T00-00-00Z")
	if err := os.MkdirAll(filepath.Join(snap, "shop"), 0o755); err != nil {
		t.Fatal(err)
	}
	// Two data files plus the local _SUCCESS marker (Run already removed the
	// local _INCOMPLETE before upload).
	for _, f := range []string{
		filepath.Join(snap, "shop", "orders.parquet"),
		filepath.Join(snap, "shop", "customers.parquet"),
		filepath.Join(snap, SuccessMarker),
	} {
		if err := os.WriteFile(f, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	var calls []string
	ops := s3UploadOps{
		putEmpty:     func(_ context.Context, k string) error { calls = append(calls, "put "+k); return nil },
		uploadFile:   func(_ context.Context, _, k string) error { calls = append(calls, "upload "+k); return nil },
		objectExists: func(_ context.Context, _ string) (bool, error) { return false, nil },
		deleteObject: func(_ context.Context, k string) error { calls = append(calls, "delete "+k); return nil },
	}

	n, err := uploadWithOps(context.Background(), outputDir, "p", false, lockedOps(ops))
	if err != nil {
		t.Fatalf("uploadWithOps: %v", err)
	}
	if n != 3 { // 2 data files + 1 _SUCCESS
		t.Fatalf("uploaded %d objects, want 3", n)
	}

	first := func(pred func(string) bool) int {
		for i, c := range calls {
			if pred(c) {
				return i
			}
		}
		return -1
	}
	last := func(pred func(string) bool) int {
		idx := -1
		for i, c := range calls {
			if pred(c) {
				idx = i
			}
		}
		return idx
	}
	isPutIncomplete := func(c string) bool {
		return strings.HasPrefix(c, "put ") && strings.Contains(c, IncompleteMarker)
	}
	isUploadSuccess := func(c string) bool {
		return strings.HasPrefix(c, "upload ") && strings.Contains(c, SuccessMarker)
	}
	isData := func(c string) bool {
		return strings.HasPrefix(c, "upload ") && !strings.Contains(c, SuccessMarker)
	}
	isDelIncomplete := func(c string) bool {
		return strings.HasPrefix(c, "delete ") && strings.Contains(c, IncompleteMarker)
	}

	putInc := first(isPutIncomplete)
	firstData := first(isData)
	lastData := last(isData)
	upSuccess := first(isUploadSuccess)
	delInc := first(isDelIncomplete)

	if putInc < 0 || firstData < 0 || upSuccess < 0 || delInc < 0 {
		t.Fatalf("missing expected calls in %v", calls)
	}
	if putInc > firstData {
		t.Errorf("_INCOMPLETE put (%d) must precede first data upload (%d): %v", putInc, firstData, calls)
	}
	if upSuccess < lastData {
		t.Errorf("_SUCCESS upload (%d) must follow all data uploads (last %d): %v", upSuccess, lastData, calls)
	}
	if delInc < upSuccess {
		t.Errorf("_INCOMPLETE delete (%d) must follow _SUCCESS upload (%d): %v", delInc, upSuccess, calls)
	}
}

// TestUploadWithOps_retrySkipsExisting: under --retry, a data object already
// present in S3 is not re-uploaded.
func TestUploadWithOps_retrySkipsExisting(t *testing.T) {
	outputDir := t.TempDir()
	snap := filepath.Join(outputDir, "2025-01-01T00-00-00Z")
	if err := os.MkdirAll(filepath.Join(snap, "shop"), 0o755); err != nil {
		t.Fatal(err)
	}
	for _, f := range []string{
		filepath.Join(snap, "shop", "orders.parquet"),
		filepath.Join(snap, SuccessMarker),
	} {
		if err := os.WriteFile(f, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	var uploaded []string
	ops := s3UploadOps{
		putEmpty:     func(_ context.Context, _ string) error { return nil },
		uploadFile:   func(_ context.Context, _, k string) error { uploaded = append(uploaded, k); return nil },
		objectExists: func(_ context.Context, _ string) (bool, error) { return true, nil }, // everything already present
		deleteObject: func(_ context.Context, _ string) error { return nil },
	}

	// count is "objects processed" (incremented even when skipped); the
	// load-bearing assertion is that uploadFile was never actually invoked.
	if _, err := uploadWithOps(context.Background(), outputDir, "p", true, lockedOps(ops)); err != nil {
		t.Fatalf("uploadWithOps: %v", err)
	}
	if len(uploaded) != 0 {
		t.Fatalf("--retry re-uploaded existing objects %v; want none", uploaded)
	}
}

// TestSnapshotDirsWithSuccess verifies the helper that drives the S3 upload's
// _INCOMPLETE-first ordering: it returns exactly the immediate child snapshot
// directories carrying a local _SUCCESS marker (completed snapshots), ignoring
// loose files, marker-less dirs, and nested dirs.
func TestSnapshotDirsWithSuccess(t *testing.T) {
	out := t.TempDir()

	mkSnap := func(name string, success bool) {
		t.Helper()
		dir := filepath.Join(out, name, "shop")
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "orders.parquet"), nil, 0o644); err != nil {
			t.Fatal(err)
		}
		if success {
			if err := os.WriteFile(filepath.Join(out, name, SuccessMarker), nil, 0o644); err != nil {
				t.Fatal(err)
			}
		}
	}
	mkSnap("2026-01-01T00-00-00Z", true)  // complete → included
	mkSnap("2026-02-01T00-00-00Z", true)  // complete → included
	mkSnap("2026-03-01T00-00-00Z", false) // no _SUCCESS → excluded
	// A loose file at the top level must not be mistaken for a snapshot dir.
	if err := os.WriteFile(filepath.Join(out, "stray.txt"), nil, 0o644); err != nil {
		t.Fatal(err)
	}

	got, err := snapshotDirsWithSuccess(out)
	if err != nil {
		t.Fatalf("snapshotDirsWithSuccess: %v", err)
	}
	want := map[string]bool{
		filepath.Join(out, "2026-01-01T00-00-00Z"): true,
		filepath.Join(out, "2026-02-01T00-00-00Z"): true,
	}
	if len(got) != len(want) {
		t.Fatalf("got %v, want the two _SUCCESS-marked snapshots %v", got, want)
	}
	for _, d := range got {
		if !want[d] {
			t.Errorf("unexpected snapshot dir %q (only _SUCCESS-marked dirs should be returned)", d)
		}
	}
}

// TestSnapshotDirsWithSuccess_missingDir verifies the helper surfaces a read
// error rather than silently returning an empty list (which would skip the
// _INCOMPLETE-first publish entirely).
func TestSnapshotDirsWithSuccess_missingDir(t *testing.T) {
	if _, err := snapshotDirsWithSuccess("/nonexistent/path-does-not-exist"); err == nil {
		t.Fatal("expected error for nonexistent output directory, got nil")
	}
}

// TestUploadWithOps_singleSnapshotDir pins the shape the scheduled refresh
// uploads with (#1539): outputDir IS one snapshot directory and the
// destination URL already names that snapshot, rather than outputDir being the
// baselines root.
//
// Two things must hold, and only one of them is visible in the resulting
// listing. The keys must be byte-identical to what a full backup of the same
// snapshot writes, or the fold publishes snapshots discovery cannot find. And
// _INCOMPLETE must still bracket the data — that one is INVISIBLE when it
// breaks: the data and _SUCCESS land correctly either way, and an upload
// interrupted without the marker reads as a COMPLETE snapshot, because a
// snapshot carrying neither marker is complete-by-default (#467). That is why
// this test asserts the marker calls and not just the data keys.
func TestUploadWithOps_singleSnapshotDir(t *testing.T) {
	const stamp = "2025-01-01T00-00-00Z"
	snap := filepath.Join(t.TempDir(), stamp)
	if err := os.MkdirAll(filepath.Join(snap, "shop"), 0o755); err != nil {
		t.Fatal(err)
	}
	for _, f := range []string{
		filepath.Join(snap, "shop", "orders.parquet"),
		filepath.Join(snap, SuccessMarker),
	} {
		if err := os.WriteFile(f, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	var calls []string
	ops := s3UploadOps{
		putEmpty:     func(_ context.Context, k string) error { calls = append(calls, "put "+k); return nil },
		uploadFile:   func(_ context.Context, _, k string) error { calls = append(calls, "upload "+k); return nil },
		objectExists: func(_ context.Context, _ string) (bool, error) { return false, nil },
		deleteObject: func(_ context.Context, k string) error { calls = append(calls, "delete "+k); return nil },
	}

	// The prefix ParseS3URL yields for s3://bucket/backups/<stamp>, which is
	// how the refresh addresses the snapshot it just folded.
	n, err := uploadWithOps(context.Background(), snap, "backups/"+stamp, false, lockedOps(ops))
	if err != nil {
		t.Fatalf("uploadWithOps: %v", err)
	}
	if n != 2 { // 1 data file + _SUCCESS
		t.Fatalf("uploaded %d objects, want 2", n)
	}
	want := []string{
		"put backups/" + stamp + "/_INCOMPLETE",
		"upload backups/" + stamp + "/shop/orders.parquet",
		"upload backups/" + stamp + "/_SUCCESS",
		"delete backups/" + stamp + "/_INCOMPLETE",
	}
	if len(calls) != len(want) {
		t.Fatalf("calls = %v, want %v", calls, want)
	}
	for i := range want {
		if calls[i] != want[i] {
			t.Fatalf("call %d = %q, want %q (all: %v)", i, calls[i], want[i], calls)
		}
	}
}

// A directory with no completed snapshot in it or under it must be REFUSED,
// not uploaded.
//
// Steps 1 and 4 are the only readers of the snapshot list; the walk that
// uploads the data and the deferred _SUCCESS are not gated on it. So an empty
// list does not upload nothing, it uploads everything except the crash-safety
// bracket — and a remote snapshot carrying neither marker is complete by
// default (#467), so an upload interrupted partway would be discoverable,
// readable and wrong. This is the shape a stat failure on the marker used to
// fall through to.
func TestUploadWithOps_refusesWhenNoSnapshotIsComplete(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "2025-01-01T00-00-00Z", "shop"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "2025-01-01T00-00-00Z", "shop", "orders.parquet"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}

	var calls []string
	ops := s3UploadOps{
		putEmpty:     func(_ context.Context, k string) error { calls = append(calls, "put "+k); return nil },
		uploadFile:   func(_ context.Context, _, k string) error { calls = append(calls, "upload "+k); return nil },
		objectExists: func(_ context.Context, _ string) (bool, error) { return false, nil },
		deleteObject: func(_ context.Context, k string) error { calls = append(calls, "delete "+k); return nil },
	}

	n, err := uploadWithOps(context.Background(), dir, "p", false, lockedOps(ops))
	if err == nil {
		t.Fatal("the upload was allowed with no completed snapshot, so nothing would have written _INCOMPLETE")
	}
	if !strings.Contains(err.Error(), IncompleteMarker) {
		t.Errorf("err = %v, want it to name the marker that cannot be written", err)
	}
	if n != 0 || len(calls) != 0 {
		t.Errorf("uploaded %d objects via %v, want the refusal to happen before any S3 call", n, calls)
	}
}
