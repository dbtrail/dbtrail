package reconstruct

import (
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

func TestSnapshotSigners(t *testing.T) {
	at := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	dir := t.TempDir()
	snap := filepath.Join(dir, SnapshotDirName(at))
	if err := os.MkdirAll(snap, 0o755); err != nil {
		t.Fatal(err)
	}
	if w, known, err := SnapshotSigners(dir, at); err != nil || !known || len(w) != 0 {
		t.Fatalf("unsigned: %v %v %v", w, known, err)
	}
	for _, m := range []string{baseline.WriterMarkerPrefix + "BBBB-1", baseline.WriterMarkerPrefix + "aaaa-2"} {
		if err := os.WriteFile(filepath.Join(snap, m), nil, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if w, known, err := SnapshotSigners(dir, at); err != nil || !known || !slices.Equal(w, []string{"aaaa-2", "bbbb-1"}) {
		t.Fatalf("signed: %v %v %v", w, known, err)
	}
	if w, known, err := SnapshotSigners(dir, at.Add(time.Hour)); err != nil || known || w != nil {
		t.Fatalf("a snapshot that is not there: %v %v %v", w, known, err)
	}
	if err := os.Chmod(snap, 0o000); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(snap, 0o755) })
	if os.Geteuid() != 0 {
		if _, _, err := SnapshotSigners(dir, at); err == nil {
			t.Fatal("an unreadable snapshot read as unsigned")
		}
	}
	if w, known, err := SnapshotSigners("s3://never-listed/p/", at); err != nil || known || w != nil {
		t.Fatalf("an S3 snapshot never listed here: %v %v %v", w, known, err)
	}
}

// An S3 snapshot: answered from what the listing stored, keyed by the
// directory name the listing reads (a bare timestamp), whichever way the
// source is spelled.
func TestSnapshotSigners_s3FromTheInventory(t *testing.T) {
	at := time.Date(2026, 6, 2, 3, 4, 5, 0, time.UTC)
	inv := s3InventoryFor("s3://signers-1684/p")
	inv.mu.Lock()
	inv.writers[SnapshotDirName(at)] = []string{"bbbb-1"}
	inv.writers[SnapshotDirName(at.Add(time.Hour))] = nil
	inv.mu.Unlock()
	t.Cleanup(func() {
		s3InventoriesMu.Lock()
		delete(s3Inventories, "s3://signers-1684/p")
		s3InventoriesMu.Unlock()
	})
	for _, src := range []string{"s3://signers-1684/p", "s3://signers-1684/p/"} {
		if w, known, err := SnapshotSigners(src, at); err != nil || !known || !slices.Equal(w, []string{"bbbb-1"}) {
			t.Fatalf("%s: %v %v %v", src, w, known, err)
		}
	}
	if w, known, err := SnapshotSigners("s3://signers-1684/p", at.Add(time.Hour)); err != nil || !known || len(w) != 0 {
		t.Fatalf("read and unsigned: %v %v %v", w, known, err)
	}
	if _, known, _ := SnapshotSigners("s3://signers-1684/p", at.Add(2*time.Hour)); known {
		t.Fatal("a directory never read reads as known")
	}
}
