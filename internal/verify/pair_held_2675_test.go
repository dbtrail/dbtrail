//go:build unix

package verify

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// #2675 point 2: every local snapshot a pair reads is held for the run, each
// once; an s3:// side and an empty one are skipped.
func TestHoldPairSnapshots(t *testing.T) {
	root := t.TempDir()
	mk := func(name string) string {
		dir := filepath.Join(root, name)
		if err := os.MkdirAll(filepath.Join(dir, "shop"), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := baseline.WriteSuccessMarker(dir); err != nil {
			t.Fatal(err)
		}
		return filepath.Join(dir, "shop", "t.parquet")
	}
	older, newer, other := mk("2026-11-12T09-00-00Z"), mk("2026-11-12T10-00-00Z"), mk("2026-11-12T08-00-00Z")
	pairs := []BaselinePair{
		{Schema: "shop", Table: "a", PrevPath: older, NewPath: newer},
		{Schema: "shop", Table: "b", PrevPath: older, NewPath: newer},
		{Schema: "shop", Table: "c", PrevPath: "s3://b/p/2026-11-12T07-00-00Z/shop/c.parquet", NewPath: newer},
		{Schema: "shop", Table: "d"},
	}
	release, gone := holdPairSnapshots(pairs)
	if gone {
		t.Fatal("snapshots on disk read as gone")
	}
	locked := func(tablePath string) bool {
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
	if !locked(older) || !locked(newer) || locked(other) {
		t.Errorf("held: older %v, newer %v, a snapshot no pair reads %v", locked(older), locked(newer), locked(other))
	}
	release()
	if locked(older) || locked(newer) {
		t.Error("still held after release")
	}
	if err := os.RemoveAll(filepath.Dir(filepath.Dir(older))); err != nil {
		t.Fatal(err)
	}
	release, gone = holdPairSnapshots(pairs)
	release()
	if !gone {
		t.Error("a pair whose snapshot was removed does not read as gone")
	}
}
