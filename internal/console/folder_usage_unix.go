//go:build darwin || linux

package console

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"syscall"
)

// folderReadBatch is how many directory entries one read asks for.
// filepath.WalkDir is not used because it reads and sorts a whole directory
// before the first callback, so neither the entry limit nor the context could
// stop a flat folder with millions of files.
const folderReadBatch = 1024

// folderDirReader reads up to n entries of an open directory; a seam so a test
// can count what a walk read.
type folderDirReader func(f *os.File, n int) ([]os.DirEntry, error)

func readFolderEntries(f *os.File, n int) ([]os.DirEntry, error) { return f.ReadDir(n) }

// measureFolder sums the disk blocks of the regular files under root.
//
//   - A file with several hard links counts once: a new snapshot links the
//     files of the tables that did not change, so summing names would report
//     many times what the disk holds.
//   - Symbolic links are not followed: what they point at is not in the folder.
//   - A file that disappears during the walk is skipped, since a cleanup may be
//     running. Any other error, a subfolder on another disk, or a walk past
//     maxEntries or ctx answers false: a partial sum is not the folder's size.
func measureFolder(ctx context.Context, root string, maxEntries int) (uint64, bool) {
	return walkFolder(ctx, root, maxEntries, readFolderEntries)
}

// pendingFolder is a directory still to read, with the identity it had when
// it was listed.
type pendingFolder struct {
	path string
	info os.FileInfo
}

func walkFolder(ctx context.Context, root string, maxEntries int, read folderDirReader) (uint64, bool) {
	info, err := os.Stat(root)
	if err != nil || !info.IsDir() {
		return 0, false
	}
	rootStat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, false
	}
	w := folderWalk{ctx: ctx, read: read, dev: uint64(rootStat.Dev), left: maxEntries - 1, linked: map[uint64]struct{}{}}
	if w.left < 0 {
		return 0, false
	}
	// Every directory here was counted against the limit when it was listed,
	// so the limit bounds this stack too.
	w.pending = []pendingFolder{{path: root, info: info}}
	for len(w.pending) > 0 {
		next := w.pending[len(w.pending)-1]
		w.pending = w.pending[:len(w.pending)-1]
		if !w.readDir(next, next.path == root) {
			return 0, false
		}
	}
	return w.total, true
}

// folderWalk is the state of one measurement.
type folderWalk struct {
	ctx     context.Context
	read    folderDirReader
	dev     uint64
	left    int
	total   uint64
	linked  map[uint64]struct{}
	pending []pendingFolder
}

// readDir reads one directory in batches, and answers false as soon as the
// measurement cannot finish whole.
func (w *folderWalk) readDir(dir pendingFolder, isRoot bool) bool {
	if w.ctx.Err() != nil {
		return false
	}
	f, err := os.Open(dir.path)
	if err != nil {
		return !isRoot && errors.Is(err, fs.ErrNotExist)
	}
	defer f.Close()
	// os.Open follows a link: a directory swapped for one after it was listed
	// is no longer the directory that was counted, so it is not read.
	opened, err := f.Stat()
	if err != nil {
		return false
	}
	if !os.SameFile(opened, dir.info) {
		return !isRoot
	}
	for {
		// One past what is left, so a folder over the limit is told from one
		// exactly at it without reading further.
		batch, err := w.read(f, min(folderReadBatch, w.left+1))
		if w.ctx.Err() != nil {
			return false
		}
		if w.left -= len(batch); w.left < 0 {
			return false
		}
		for _, e := range batch {
			if !w.count(dir.path, e) {
				return false
			}
		}
		if errors.Is(err, io.EOF) {
			return true
		}
		if err != nil {
			return false
		}
		if len(batch) == 0 {
			return true
		}
	}
}

// count adds one entry: a file's blocks, or a directory to the pending stack.
func (w *folderWalk) count(parent string, e os.DirEntry) bool {
	if !e.IsDir() && !e.Type().IsRegular() {
		return true
	}
	// DirEntry.Info is an lstat: a link is never followed here.
	fi, err := e.Info()
	if err != nil {
		return errors.Is(err, fs.ErrNotExist)
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok || uint64(st.Dev) != w.dev {
		return false
	}
	if fi.IsDir() {
		w.pending = append(w.pending, pendingFolder{path: filepath.Join(parent, e.Name()), info: fi})
		return true
	}
	if !fi.Mode().IsRegular() {
		return true
	}
	if st.Nlink > 1 {
		if _, seen := w.linked[uint64(st.Ino)]; seen {
			return true
		}
		w.linked[uint64(st.Ino)] = struct{}{}
	}
	w.total += uint64(st.Blocks) * 512
	return true
}

// folderDisk is the id of the filesystem holding path. An unreadable path
// answers false, so nothing is attributed to a disk it may not be on.
func folderDisk(path string) (uint64, bool) {
	info, err := os.Stat(path)
	if err != nil {
		return 0, false
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, false
	}
	return uint64(st.Dev), true
}
