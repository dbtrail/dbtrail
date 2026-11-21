package reconstruct

import (
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// The working directories a fold makes under the system's temporary directory,
// and how a later process removes the ones a killed process left (#2513).
//
// A fold that ends removes its own (changeSpill.remove, the cleanup of
// materializeBaseWithDelta). A process killed in the middle (SIGKILL, the
// kernel's out-of-memory killer) cannot, and nothing listed these directories
// afterwards: each one stayed for good, holding as much as the fold had
// written, which is the changed rows of a whole window or a whole table.
//
// The sweep answers one question before it removes anything: is the process
// that made this directory gone? Several processes fold on one machine (the
// daemon, a `bintrail reconstruct` run by hand, a second installation sharing
// the temporary directory), so a rule that guesses would remove the changes a
// live fold is about to read back. The proof is the same one the tails'
// directories use (internal/console/tail_lock_unix.go, #2819):
//
//   - The process that makes a directory takes an flock on the directory
//     itself and holds it until it removes the directory. The kernel drops the
//     lock when the process dies, however it dies, and only then. Not a pid: a
//     restarted container's process usually has the pid of the one before.
//   - The sweep removes a directory only while it holds that lock itself. A
//     lock it cannot take, or cannot ask for, keeps the directory.
//   - Between making a directory and locking it there is an instant in which
//     a sweep could take the lock first. No rule of age covers it: the maker
//     waits for the lock, then checks that its path still names the directory
//     it locked, and makes another if the sweep removed it (newWorkDir). The
//     sweep makes the same check before removing (lockWorkDir).
//
// The directory's name carries the host, and a sweep leaves another host's
// alone: a temporary directory shared over a network filesystem may not carry
// locks between machines, and there a free lock proves nothing.
//
// Directories of versions from before the lock are named without the mark and
// were never locked, so a free lock does not prove them unused either: a
// process of that version may still be folding. Those go by age alone, a long
// one (workDirLegacyIdle), counted from the newest write in them. A directory
// this version could not lock is made under that older name, so it gets the
// same rule instead of looking like one whose owner died.

// The kinds of working directory: the middle of the name.
const (
	// workDirFold holds a fold's changed rows (changeSpill).
	workDirFold = "fold"
	// workDirCompact holds a table's base file with its deltas applied, what
	// a compaction merges from (materializeBaseWithDelta and its PostgreSQL
	// sibling). Only the ones made in the system's temporary directory are
	// swept here; one made inside a snapshot being written goes with that
	// snapshot.
	workDirCompact = "compact"
)

var workDirKinds = []string{workDirFold, workDirCompact}

// workDirMark follows the kind in the name of a directory its maker locks.
// os.MkdirTemp's random part is digits, so a name of a version from before
// the lock ("bintrail-fold-123456") never reads as a marked one.
const workDirMark = "v2-"

// workDirLegacyIdle is how long nothing may have been written in a directory
// that is not locked by design (an older version's, or one that could not be
// locked) before a sweep removes it. A fold writes its changes while it reads
// the window and then only reads them back, so this has to outlast the
// longest merge, not the longest pause between writes.
const workDirLegacyIdle = 24 * time.Hour

// errWorkDirHeld is lockWorkDir's answer for a directory a live process
// holds; errWorkDirGone for a path that no longer names the directory that
// was opened (removed, or replaced, in between).
var (
	errWorkDirHeld = errors.New("another process holds the working directory")
	errWorkDirGone = errors.New("the working directory is no longer there")
)

// Test seams. Production never reassigns them.
var (
	// workDirLock is lockWorkDir.
	workDirLock = lockWorkDir
	// afterWorkDirLocked runs between taking a directory's lock and checking
	// that its path still names it.
	afterWorkDirLocked = func(string) {}
	// beforeWorkDirSwept runs on each directory a sweep is about to lock.
	beforeWorkDirSwept = func(string) {}
)

// workDir is one working directory and the lock that keeps it.
type workDir struct {
	path string
	lock *os.File // nil when the directory could not be locked
}

// workDirLockWarn says once per process that a directory could not be locked:
// every one the process makes would fail the same way.
var workDirLockWarn sync.Once

// newWorkDir makes a working directory of kind under parent ("" is the
// system's temporary directory) and locks it. What killed processes left in
// the system's temporary directory is removed first, once per process.
func newWorkDir(parent, kind string) (*workDir, error) {
	SweepDeadWorkDirs()
	pattern := "bintrail-" + kind + "-" + workDirMark + workDirHost() + "-*"
	var lockErr error
	for range 3 {
		dir, err := os.MkdirTemp(parent, pattern)
		if err != nil {
			return nil, err
		}
		lock, err := workDirLock(dir, true)
		if err == nil {
			return &workDir{path: dir, lock: lock}, nil
		}
		lockErr = err
		if errors.Is(err, errWorkDirGone) {
			// A sweep took the lock first and removed it, empty.
			continue
		}
		// Empty, and ours: nothing was written yet.
		_ = os.Remove(dir)
		break
	}
	// Not locked, so not under the name whose free lock means a dead owner.
	dir, err := os.MkdirTemp(parent, "bintrail-"+kind+"-*")
	if err != nil {
		return nil, err
	}
	workDirLockWarn.Do(func() {
		slog.Warn("reconstruct: a working directory could not be locked; another DBTrail process starting on this machine "+
			"keeps it only while something was written in it in the last 24 hours",
			"dir", dir, "error", lockErr)
	})
	return &workDir{path: dir}, nil
}

// remove deletes the directory and lets go of its lock. Safe to call more
// than once.
func (w *workDir) remove() error {
	// Removed while held, so no sweep works on it at the same time.
	err := os.RemoveAll(w.path)
	if w.lock != nil {
		w.lock.Close()
		w.lock = nil
	}
	return err
}

// workDirHost is the hostname as it goes into a directory's name: no path
// separators, and no "*", which os.MkdirTemp reads as where the random part
// goes.
func workDirHost() string {
	h, _ := os.Hostname()
	h = strings.Map(func(r rune) rune {
		if r == '/' || r == os.PathSeparator || r == '*' || r == 0 {
			return '_'
		}
		return r
	}, h)
	if h == "" {
		return "unknown"
	}
	return h
}

// parseWorkDirName reads the name of a working directory: a marked one,
// "bintrail-<kind>-v2-<host>-<digits>" (the host may hold dashes, so the
// digits are taken from the right), or one of a version from before the lock,
// "bintrail-<kind>-<digits>" (legacy, no host). ok false for anything else.
func parseWorkDirName(name string) (host string, legacy, ok bool) {
	for _, kind := range workDirKinds {
		rest, found := strings.CutPrefix(name, "bintrail-"+kind+"-")
		if !found {
			continue
		}
		if allDigits(rest) {
			return "", true, true
		}
		rest, found = strings.CutPrefix(rest, workDirMark)
		if !found {
			return "", false, false
		}
		i := strings.LastIndexByte(rest, '-')
		if i <= 0 || !allDigits(rest[i+1:]) {
			return "", false, false
		}
		return rest[:i], false, true
	}
	return "", false, false
}

func allDigits(s string) bool {
	if s == "" {
		return false
	}
	for i := range len(s) {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

var workDirSweep sync.Once

// SweepDeadWorkDirs removes, once per process, the working directories that
// processes killed in the middle of a fold left in the system's temporary
// directory. It runs before this process makes its first one; the daemon also
// calls it as it starts, so what a killed daemon left goes at the restart and
// not at the next fold that happens to need the disk. It never fails: what it
// cannot remove it says, and leaves.
func SweepDeadWorkDirs() {
	workDirSweep.Do(func() {
		sweepWorkDirs(os.TempDir(), workDirHost(), time.Now().Add(-workDirLegacyIdle))
	})
}

// sweepWorkDirs removes the working directories under parent that no process
// uses: this user's, and either marked with host and held by no process, or
// of a version from before the lock and with nothing written since
// legacyCutoff. Anything else stays: another user's, another host's, a link,
// a file, a name that only looks like one, and any directory whose state
// could not be read.
func sweepWorkDirs(parent, host string, legacyCutoff time.Time) {
	entries, err := os.ReadDir(parent)
	if err != nil {
		slog.Debug("reconstruct: could not list the temporary directory for working directories a stopped process left", "dir", parent, "error", err)
		return
	}
	for _, e := range entries {
		h, legacy, ok := parseWorkDirName(e.Name())
		if !ok || (!legacy && h != host) {
			continue
		}
		dir := filepath.Join(parent, e.Name())
		// Lstat: a link by that name is not a directory, and is left alone.
		fi, err := os.Lstat(dir)
		if err != nil || !fi.IsDir() || !ownedByThisUser(fi) {
			continue
		}
		if legacy && !idleSince(dir, fi, legacyCutoff) {
			continue
		}
		beforeWorkDirSwept(dir)
		lock, lockErr := workDirLock(dir, false)
		if lockErr != nil {
			// Held: a live process uses it. Gone: nothing to do. Any other
			// failure cannot tell it is unused, so it stays too.
			if !errors.Is(lockErr, errWorkDirHeld) && !errors.Is(lockErr, errWorkDirGone) {
				slog.Debug("reconstruct: a working directory was kept: whether a process still uses it is not known", "dir", dir, "error", lockErr)
			}
			continue
		}
		// Removed while held, so no process takes it in between.
		if err := os.RemoveAll(dir); err != nil {
			slog.Warn("reconstruct: could not remove a working directory a stopped process left; it still takes disk, and can be removed by hand",
				"dir", dir, "error", err)
		} else {
			slog.Info("reconstruct: removed a working directory a stopped process left", "dir", dir)
		}
		lock.Close()
	}
}

// idleSince says nothing in dir was written since cutoff: not the directory
// (a file made or removed), not any entry in it. Anything that cannot be read
// counts as written.
func idleSince(dir string, fi os.FileInfo, cutoff time.Time) bool {
	if !fi.ModTime().Before(cutoff) {
		return false
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return false
	}
	for _, e := range entries {
		info, err := e.Info()
		if err != nil || !info.ModTime().Before(cutoff) {
			return false
		}
	}
	return true
}
