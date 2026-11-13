//go:build unix

package baseline

import (
	"errors"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
)

// A reader in ANY process holds the one snapshot it reads (#2675), with a
// shared flock on that snapshot's own _SUCCESS marker, and the prune takes
// the same file exclusively, without waiting, right before it moves the
// snapshot aside (lockForRemoval). A snapshot a reader holds is kept, the
// rest of the folder is pruned as usual.
//
// BeginRead and PinSnapshot (reading.go) are in memory, so a reader of
// another process (`bintrail export iceberg`, `bintrail verify`, `bintrail
// reconstruct`) was not seen and lost its snapshot when the cleanup reached
// it. Why the marker and not a file of its own:
//
//   - The reader only needs to READ the snapshot, which it does anyway: an
//     operator's command run as another user, with no write permission on
//     the folder, still holds it.
//   - Nothing is left behind to sweep: the kernel releases a flock when the
//     process that holds it ends, killed included.
//   - The marker belongs to ONE snapshot. No producer links it forward (a
//     refresh links table files, never markers) and WriteSuccessMarker
//     writes it in place, so one lock never covers two snapshots.
//
// No time limit, unlike the in-memory holds: a hold here keeps only the
// snapshot its process reads, and only while that process lives. The
// in-memory pins have deadlines because an HTTP client decides how long a
// download lasts; these readers are commands someone started.
//
// Not held, as before: a snapshot with no _SUCCESS (written before markers
// existed) and an s3:// one (the S3 cleanup has its own rule).

// afterReadHoldLock runs between taking the shared lock and checking that
// the marker is still the snapshot's: the seam a test moves the snapshot
// aside through. Nil in production.
var afterReadHoldLock func()

// HoldSnapshot keeps the snapshot directory snapDir from the prune of any
// process until release is called. gone reports that the snapshot was moved
// aside or removed before the hold could be taken (the caller finds its
// snapshot again); release is then a no-op. A snapshot that cannot be held
// (no marker, an unreadable one, a non-local path) is read unheld, as before
// this existed: release is a no-op and gone is false.
//
// release is safe to call more than once.
func HoldSnapshot(snapDir string) (release func(), gone bool) {
	noop := func() {}
	if snapDir == "" || strings.HasPrefix(snapDir, "s3://") {
		return noop, false
	}
	marker := filepath.Join(snapDir, SuccessMarker)
	f, err := os.Open(marker)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			if _, serr := os.Stat(snapDir); errors.Is(serr, fs.ErrNotExist) {
				return noop, true
			}
		}
		slog.Debug("baseline: this snapshot is read without a hold against the cleanup of another process", "snapshot", snapDir, "error", err)
		return noop, false
	}
	if err := flockRetry(f, syscall.LOCK_SH); err != nil {
		f.Close()
		slog.Debug("baseline: this snapshot is read without a hold against the cleanup of another process", "snapshot", snapDir, "error", err)
		return noop, false
	}
	if afterReadHoldLock != nil {
		afterReadHoldLock()
	}
	// The prune moves the folder aside while it holds the marker; a reader
	// that opened the marker before that and got the lock after it holds a
	// file no longer at its path.
	held, herr := f.Stat()
	now, nerr := os.Stat(marker)
	if herr != nil || nerr != nil || !os.SameFile(held, now) {
		_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		f.Close()
		return noop, true
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
			f.Close()
		})
	}, false
}

// lockForRemoval takes snapDir's marker exclusively without waiting, for the
// prune, right before the snapshot is moved aside. held reports that a
// reader holds it (HoldSnapshot): the snapshot stays. With no marker, or one
// that cannot be opened or locked for another reason, nothing could hold it
// and the removal goes on as before. unlock is called once the snapshot is
// out of its path.
func lockForRemoval(snapDir string) (unlock func(), held bool) {
	noop := func() {}
	f, err := os.Open(filepath.Join(snapDir, SuccessMarker))
	if err != nil {
		return noop, false
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return noop, true
		}
		slog.Debug("baseline prune: could not lock a snapshot's marker; removing it as before", "snapshot", snapDir, "error", err)
		return noop, false
	}
	return func() {
		_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		f.Close()
	}, false
}

// flockRetry is a blocking flock that goes on after a signal interrupts it.
// The wait is bounded by the prune, which holds the lock only while it moves
// one folder aside.
func flockRetry(f *os.File, how int) error {
	for {
		err := syscall.Flock(int(f.Fd()), how)
		if !errors.Is(err, syscall.EINTR) {
			return err
		}
	}
}
