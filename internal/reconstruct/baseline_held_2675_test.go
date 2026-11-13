//go:build unix

package reconstruct

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// #2675 point 2: FindHeld holds what find names, and looks once more when
// the cleanup moved it aside between the lookup and the hold.

func heldSnapshot(t *testing.T, root, name string) string {
	t.Helper()
	dir := filepath.Join(root, name)
	if err := os.MkdirAll(filepath.Join(dir, "shop"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "shop", "orders.parquet"), []byte("rows"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := baseline.WriteSuccessMarker(dir); err != nil {
		t.Fatal(err)
	}
	return filepath.Join(dir, "shop", "orders.parquet")
}

// lockedBySomeone reports whether a reader holds the snapshot of a table
// path: its marker cannot be taken exclusively.
func lockedBySomeone(t *testing.T, tablePath string) bool {
	t.Helper()
	f, err := os.Open(filepath.Join(filepath.Dir(filepath.Dir(tablePath)), baseline.SuccessMarker))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return true
	}
	_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
	return false
}

func TestFindHeld_holdsWhatItFound(t *testing.T) {
	root := t.TempDir()
	p := heldSnapshot(t, root, "2026-11-12T10-00-00Z")
	find := func(context.Context, string, string, time.Time) (string, time.Time, StaleWarning, error) {
		return p, time.Time{}, StaleWarning{}, nil
	}
	path, _, _, release, err := FindHeld(context.Background(), find, "shop", "orders", time.Now())
	if err != nil || path != p {
		t.Fatalf("FindHeld = %q, %v", path, err)
	}
	if !lockedBySomeone(t, p) {
		t.Error("the snapshot found is not held")
	}
	release()
	if lockedBySomeone(t, p) {
		t.Error("the snapshot is still held after release")
	}
}

// The first answer names a snapshot the cleanup removed in between: the
// second answer is the one held.
func TestFindHeld_looksAgainWhenTheSnapshotWentInBetween(t *testing.T) {
	root := t.TempDir()
	gonePath := filepath.Join(root, "2026-11-12T09-00-00Z", "shop", "orders.parquet")
	p := heldSnapshot(t, root, "2026-11-12T10-00-00Z")
	calls := 0
	find := func(context.Context, string, string, time.Time) (string, time.Time, StaleWarning, error) {
		calls++
		if calls == 1 {
			return gonePath, time.Time{}, StaleWarning{}, nil
		}
		return p, time.Time{}, StaleWarning{}, nil
	}
	path, _, _, release, err := FindHeld(context.Background(), find, "shop", "orders", time.Now())
	defer release()
	if err != nil || path != p || calls != 2 {
		t.Fatalf("FindHeld = %q, %v after %d lookups; want %q after 2", path, err, calls, p)
	}
	if !lockedBySomeone(t, p) {
		t.Error("the second answer is not held")
	}
}

// Gone twice is an error, never a path to a snapshot that is not there; a
// lookup error comes back as it was, with a release that can be called.
func TestFindHeld_goneTwiceAndLookupErrors(t *testing.T) {
	root := t.TempDir()
	gonePath := filepath.Join(root, "2026-11-12T09-00-00Z", "shop", "orders.parquet")
	find := func(context.Context, string, string, time.Time) (string, time.Time, StaleWarning, error) {
		return gonePath, time.Time{}, StaleWarning{}, nil
	}
	path, _, _, release, err := FindHeld(context.Background(), find, "shop", "orders", time.Now())
	release()
	t.Logf("gone twice: %v", err)
	if err == nil || path != "" || !strings.Contains(err.Error(), "the snapshot of shop.orders was removed by the cleanup twice while this read started; run it again") {
		t.Errorf("gone twice = %q, %v", path, err)
	}
	notFound := fmt.Errorf("wrapped: %w", ErrNoBaseline)
	fail := func(context.Context, string, string, time.Time) (string, time.Time, StaleWarning, error) {
		return "", time.Time{}, StaleWarning{}, notFound
	}
	_, _, _, release, err = FindHeld(context.Background(), fail, "shop", "orders", time.Now())
	release()
	if !errors.Is(err, ErrNoBaseline) {
		t.Errorf("a lookup error came back as %v", err)
	}
}

// An s3:// answer is returned unheld, as before.
func TestFindHeld_anS3PathIsNotHeld(t *testing.T) {
	find := func(context.Context, string, string, time.Time) (string, time.Time, StaleWarning, error) {
		return "s3://b/p/2026-11-12T10-00-00Z/shop/orders.parquet", time.Time{}, StaleWarning{}, nil
	}
	path, _, _, release, err := FindHeld(context.Background(), find, "shop", "orders", time.Now())
	release()
	if err != nil || !strings.HasPrefix(path, "s3://") {
		t.Errorf("FindHeld over S3 = %q, %v", path, err)
	}
}
