//go:build darwin || linux

package console

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

func flatFolder(t *testing.T, files int) string {
	t.Helper()
	dir := t.TempDir()
	for i := range files {
		if err := os.WriteFile(filepath.Join(dir, fmt.Sprintf("f%05d", i)), []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

// A flat folder over the limit stops without being read whole: the reason this
// walk does not use filepath.WalkDir (#2475).
func TestWalkFolder_stopsAtTheLimitWithoutReadingEverything(t *testing.T) {
	dir := flatFolder(t, 3000)
	const limit = 10
	read := 0
	counting := func(f *os.File, n int) ([]os.DirEntry, error) {
		batch, err := f.ReadDir(n)
		read += len(batch)
		return batch, err
	}
	if n, ok := walkFolder(context.Background(), dir, limit, counting); ok {
		t.Fatalf("a folder over the limit got a size: %d", n)
	}
	if read > limit {
		t.Fatalf("read %d entries with a limit of %d", read, limit)
	}
}

func TestWalkFolder_exactlyAtTheLimitIsMeasured(t *testing.T) {
	dir := flatFolder(t, 9) // nine files and the folder itself
	if _, ok := measureFolder(context.Background(), dir, 10); !ok {
		t.Fatal("a folder exactly at the limit has no size")
	}
	if _, ok := measureFolder(context.Background(), dir, 9); ok {
		t.Fatal("a folder one past the limit got a size")
	}
}

// More files than one batch: every batch is counted.
func TestWalkFolder_readsAcrossBatches(t *testing.T) {
	dir := flatFolder(t, folderReadBatch+5)
	n, ok := measureFolder(context.Background(), dir, folderReadBatch+6)
	if !ok || n == 0 {
		t.Fatalf("size = %d, ok = %v", n, ok)
	}
}

func TestWalkFolder_cancelStopsAfterTheBatch(t *testing.T) {
	dir := flatFolder(t, folderReadBatch*2+10)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	read := 0
	cancelling := func(f *os.File, n int) ([]os.DirEntry, error) {
		batch, err := f.ReadDir(n)
		read += len(batch)
		cancel()
		return batch, err
	}
	if n, ok := walkFolder(ctx, dir, 1<<20, cancelling); ok {
		t.Fatalf("a cancelled walk got a size: %d", n)
	}
	if read > folderReadBatch {
		t.Fatalf("read %d entries after the cancel, one batch is %d", read, folderReadBatch)
	}
}

// Directories waiting to be read count against the limit too.
func TestWalkFolder_pendingDirectoriesCount(t *testing.T) {
	dir := t.TempDir()
	for i := range 20 {
		if err := os.Mkdir(filepath.Join(dir, fmt.Sprintf("d%02d", i)), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	if n, ok := measureFolder(context.Background(), dir, 5); ok {
		t.Fatalf("a folder of 20 subfolders passed a limit of 5: %d", n)
	}
}

// A subfolder on another disk makes the size unknown.
func TestWalkFolder_anotherDiskHasNoSize(t *testing.T) {
	dir := flatFolder(t, 1)
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	info, err := entries[0].Info()
	if err != nil {
		t.Fatal(err)
	}
	st := info.Sys().(*syscall.Stat_t)
	w := folderWalk{dev: uint64(st.Dev) + 1, linked: map[uint64]struct{}{}}
	if w.count(dir, entries[0]) {
		t.Fatal("a file on another device was counted")
	}
}

// A directory replaced after it was listed (os.Open would follow a link put in
// its place) is not read.
func TestWalkFolder_aSwappedDirectoryIsNotRead(t *testing.T) {
	parent := t.TempDir()
	listed := filepath.Join(parent, "d")
	if err := os.Mkdir(listed, 0o700); err != nil {
		t.Fatal(err)
	}
	info, err := os.Lstat(listed)
	if err != nil {
		t.Fatal(err)
	}
	outside := flatFolder(t, 3)
	if err := os.Remove(listed); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, listed); err != nil {
		t.Fatal(err)
	}
	read := 0
	w := folderWalk{ctx: context.Background(), left: 100, linked: map[uint64]struct{}{},
		read: func(f *os.File, n int) ([]os.DirEntry, error) {
			batch, err := f.ReadDir(n)
			read += len(batch)
			return batch, err
		}}
	if !w.readDir(pendingFolder{path: listed, info: info}, false) {
		t.Fatal("a swapped directory stopped the walk; it is treated as gone")
	}
	if read != 0 || w.total != 0 {
		t.Fatalf("read %d entries, %d bytes through the link", read, w.total)
	}
}
