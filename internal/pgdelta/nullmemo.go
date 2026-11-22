package pgdelta

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// What BasePositionsOf remembers between calls: that a base file's key
// columns hold no NULL. Finding that out reads the key columns of every row
// of the file, a pass as long as the join that finds the keys, and a base
// file is asked again and again while it is unchanged: by every update that
// writes a pair beside it (each one reads it through a hard link in the
// newer snapshot's folder), and by every read of a table's copy for the
// values a change did not send.
//
// A file is known by what the file system says of it: device, inode, size
// and time of last write, the identity the rest of the code gives a base
// file (baselineintegrity's memo, keyidx's binding, the console's
// baseFileID). Two names of one file (a hard link) share it; a file written
// again, or another file renamed over the name, does not. The path is NOT
// part of it, on purpose: a base carried forward is a new path at every
// update, so a memo keyed by the path would never be asked twice, and the
// path adds no safety the inode and the time do not already give.
//
// The rules, taken from baselineintegrity's memo of verified files:
//
//   - Only the answer "no NULL" is remembered. A file with a NULL in a key
//     column, a pass that failed, or one cut short, is read again at every
//     call, so such a file is refused every time.
//   - The file is looked at before the pass and after it, and remembered
//     only when the two agree: what is remembered is what was read.
//   - Not a file written less than noNullMargin ago. Times of last write are
//     coarse, so a file written again within the same tick can keep all four
//     of device, inode, size and time; past the margin, a new write shows
//     another time.
//   - A call that skips the pass looks at the file again after the join. If
//     it is not the file remembered any more, the answer is dropped, the
//     entry too, and the call is made again with both passes.
//   - Not an S3 or other URL, not a relative path, not something that is
//     not a regular file, and nothing on a platform that gives no device
//     and inode (fileIDOf): those run both passes at every call, as before.
//   - The key columns are part of what is remembered, in the order given and
//     as their exact bytes: "no NULL in (a, b)" says nothing of (c), nor of a
//     column "A" beside "a".
//
// Bounded: two generations of noNullCap entries, as baselineintegrity keeps
// its own (a hit in the older one moves to the newer; when the newer is full
// the older is dropped). It lives in the process's memory only: a restart
// starts with nothing remembered, and the first call for each file runs both
// passes again.

// noNullKey is one remembered answer: a file's identity and the key columns.
type noNullKey struct {
	id   fileID
	cols string
}

var (
	noNullMu       sync.Mutex
	noNullCurrent  = map[noNullKey]struct{}{}
	noNullPrevious = map[noNullKey]struct{}{}

	// noNullCap is the size of one generation; a var so a test can set it
	// low. An entry is about a hundred bytes; a daemon has one per table
	// file it pairs against, or reads a copy's values from.
	noNullCap = 4096

	// noNullMargin: a file written less than this before it is looked at is
	// not remembered (see above). A var so a test can see it.
	noNullMargin = 2 * time.Second

	// nullPasses counts the passes over a whole file's key columns made to
	// look for a NULL (KeyNullPasses).
	nullPasses atomic.Uint64
)

// KeyNullPasses is how many times, since the process started, BasePositionsOf
// read the key columns of a whole base file to look for a NULL: one per call
// that did not remember the file. A caller reports a span of work as the
// difference of two calls.
func KeyNullPasses() uint64 { return nullPasses.Load() }

// noNullKeyOf is what basePath and keyColumns are remembered under, and false
// when they cannot be (see above).
func noNullKeyOf(basePath string, keyColumns []string) (noNullKey, os.FileInfo, bool) {
	// A relative path, or one the engine expands (~), may name another file
	// for the engine than for os.Stat: only an absolute local path is
	// remembered.
	if strings.Contains(basePath, "://") || !filepath.IsAbs(basePath) {
		return noNullKey{}, nil, false
	}
	fi, err := os.Stat(basePath)
	if err != nil || !fi.Mode().IsRegular() {
		return noNullKey{}, nil, false
	}
	id, ok := fileIDOf(fi)
	if !ok {
		return noNullKey{}, nil, false
	}
	return noNullKey{id: id, cols: fmt.Sprintf("%q", keyColumns)}, fi, true
}

// noNullRecalled reports whether k is remembered. A hit in the older
// generation moves to the newer one.
func noNullRecalled(k noNullKey) bool {
	noNullMu.Lock()
	defer noNullMu.Unlock()
	if _, ok := noNullCurrent[k]; ok {
		return true
	}
	if _, ok := noNullPrevious[k]; !ok {
		return false
	}
	delete(noNullPrevious, k)
	noNullStore(k)
	return true
}

// noNullRemember remembers k, the file at basePath as it was before a pass
// that found no NULL, if the file is still that file and old enough.
func noNullRemember(basePath string, keyColumns []string, k noNullKey, now time.Time) {
	after, fi, ok := noNullKeyOf(basePath, keyColumns)
	if !ok || after != k || now.Sub(fi.ModTime()) < noNullMargin {
		return
	}
	noNullMu.Lock()
	defer noNullMu.Unlock()
	noNullStore(k)
}

// noNullForget drops k from both generations.
func noNullForget(k noNullKey) {
	noNullMu.Lock()
	defer noNullMu.Unlock()
	delete(noNullCurrent, k)
	delete(noNullPrevious, k)
}

// noNullStore puts k in the newer generation, turning the generations over
// when it is full. noNullMu is held.
func noNullStore(k noNullKey) {
	if _, ok := noNullCurrent[k]; ok {
		return
	}
	if len(noNullCurrent) >= noNullCap {
		noNullPrevious = noNullCurrent
		noNullCurrent = make(map[noNullKey]struct{}, noNullCap)
	}
	noNullCurrent[k] = struct{}{}
}
