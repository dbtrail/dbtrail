package baselineintegrity

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// #2517 round 3: a file modified less than racyMargin ago is not remembered,
// because file timestamps are coarse (Linux ticks of a few ms): a rewrite in
// place within the same tick keeps device, inode, size AND mtime.

// (a) The deterministic same-tick case: same size, same mtime, mtime = now.
// The first check must not have remembered the file, so the second reads
// and refuses the rewritten bytes.
func TestOnce_fileModifiedJustNowIsNotRemembered(t *testing.T) {
	needIdentity(t)
	forgetOnExit(t)
	reads := countReads(t)
	snap := t.TempDir()
	path := filepath.Join(snap, "db", "t.parquet")
	writeFileT(t, path, []byte("the bytes"))
	if err := WriteManifest(snap); err != nil {
		t.Fatal(err)
	}
	ForgetVerified()
	now := time.Now()
	if err := os.Chtimes(path, now, now); err != nil {
		t.Fatal(err)
	}
	if err := ValidateLocalFileOnce(path); err != nil {
		t.Fatal(err)
	}
	if n := memoLen(); n != 0 {
		t.Fatalf("memo holds %d entries for a file modified just now", n)
	}
	writeFileT(t, path, []byte("THE BYTES"))
	if err := os.Chtimes(path, now, now); err != nil {
		t.Fatal(err)
	}
	if err := ValidateLocalFileOnce(path); !errors.Is(err, ErrIntegrity) {
		t.Fatalf("a same-tick rewrite was skipped: want ErrIntegrity, got %v", err)
	}
	if n := reads.Load(); n != 2 {
		t.Errorf("read %d times, want 2", n)
	}
}

// (b) The same shape with an OLD mtime on both writes is the pinned
// limitation, now stated as "outside the margin": remembered, so skipped.
func TestOnce_sameIdentityOutsideTheMarginIsSkipped_limitation(t *testing.T) {
	needIdentity(t)
	forgetOnExit(t)
	reads := countReads(t)
	snap := t.TempDir()
	path := filepath.Join(snap, "db", "t.parquet")
	writeFileT(t, path, []byte("the bytes"))
	if err := WriteManifest(snap); err != nil {
		t.Fatal(err)
	}
	ForgetVerified()
	old := time.Now().Add(-time.Minute)
	if err := os.Chtimes(path, old, old); err != nil {
		t.Fatal(err)
	}
	if err := ValidateLocalFileOnce(path); err != nil {
		t.Fatal(err)
	}
	writeFileT(t, path, []byte("THE BYTES"))
	if err := os.Chtimes(path, old, old); err != nil {
		t.Fatal(err)
	}
	if err := ValidateLocalFileOnce(path); err != nil {
		t.Fatalf("outside the margin the identity is trusted; got %v", err)
	}
	if n := reads.Load(); n != 1 {
		t.Errorf("read %d times, want 1", n)
	}
}

// (c) The writer's seed obeys the margin too: a file whose mtime is old is
// seeded, one written just now is not (and is read at its first check).
func TestWriteManifest_seedsOnlyFilesOutsideTheMargin(t *testing.T) {
	needIdentity(t)
	forgetOnExit(t)
	reads := countReads(t)
	snap := t.TempDir()
	oldFile := filepath.Join(snap, "db", "old.parquet")
	newFile := filepath.Join(snap, "db", "new.parquet")
	writeFileT(t, oldFile, []byte("old bytes"))
	writeFileT(t, newFile, []byte("new bytes"))
	old := time.Now().Add(-time.Minute)
	if err := os.Chtimes(oldFile, old, old); err != nil {
		t.Fatal(err)
	}
	if err := WriteManifest(snap); err != nil {
		t.Fatal(err)
	}
	if n := memoLen(); n != 1 {
		t.Fatalf("the writer remembered %d files, want 1 (the old one)", n)
	}
	if err := ValidateLocalFileOnce(oldFile); err != nil {
		t.Fatal(err)
	}
	if err := ValidateLocalFileOnce(newFile); err != nil {
		t.Fatal(err)
	}
	if n := reads.Load(); n != 1 {
		t.Errorf("read %d files, want 1 (the one written just now)", n)
	}
}
