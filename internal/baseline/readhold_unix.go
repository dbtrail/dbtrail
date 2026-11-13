//go:build unix

package baseline

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
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
// it. Why the marker:
//
//   - The reader only needs to READ the snapshot, which it does anyway: an
//     operator's command run as another user, with no write permission on
//     the folder, still holds it.
//   - The kernel releases a flock when the process that holds it ends,
//     killed included.
//   - The marker belongs to ONE snapshot. No producer links it forward (a
//     refresh links table files, never markers) and WriteSuccessMarker
//     writes it in place, so one lock never covers two snapshots.
//
// Bounded like the in-memory holds, by readHoldLimit: a reader that hangs,
// or anyone able to open the marker, must not stop the cleanup of a folder
// for good and fill the disk. The age of a hold is the EARLIER of two
// clocks:
//
//   - the start the reader wrote beside the snapshot when it took the hold
//     (holdRecordPrefix: its pid and start, 0600, never through a link),
//     which survives a restart of the daemon; a reader that cannot write
//     there (read access only) holds without one;
//   - the first time this process's prune found the marker held, which
//     needs nothing from the reader, so a hold with no record, or one whose
//     record keeps being rewritten, is bounded all the same.
//
// Only LIVE readers count: each reader holds its own record exclusively for
// as long as it reads, so a record the prune can lock is a dead reader's. It
// is removed, and the prune's clock for that marker starts over, so a reader
// that comes after a killed one is never charged with the dead one's age.
//
// On NFS (EFS included) Linux turns flock into POSIX locks, which are seen
// across clients: a reader on one host holds against the cleanup on
// another. Two differences: an exclusive lock needs the file open for
// writing, which is why the prune opens _SUCCESS read-write (it owns the
// folder) and warns once when a lock fails for any other reason than "held";
// and two holders in ONE process do not exclude each other, so there a
// reader in the daemon's own process is protected only by the in-memory
// holds (reading.go), not by this lock.
//
// Past the limit the prune removes the snapshot anyway and says so once
// per snapshot; the reader then fails as it did before this existed.
//
// Neither side waits for long: the prune never waits for the lock, and the
// reader tries for readHoldWait and then reads unheld, saying so.
//
// Not held, as before: a snapshot with no _SUCCESS (written before markers
// existed) and an s3:// one (the S3 cleanup has its own rule).

// holdRecordPrefix names the record a reader writes beside the snapshots it
// holds: <root>/.reading.<snapshot>.<pid>.<nanos>. A dot name, so no listing
// takes it for a snapshot and no upload sends it.
const holdRecordPrefix = ".reading."

// holdRecord is what the record says.
type holdRecord struct {
	PID   int       `json:"pid"`
	Since time.Time `json:"since"`
}

// readHoldWait bounds how long a reader tries for the shared lock, and
// readHoldStep how often. The prune holds the lock exclusively only while it
// moves one folder aside, so a reader that waits longer is up against
// something else holding it, and reads unheld rather than hang. Variables
// for the tests.
var (
	readHoldWait = 5 * time.Second
	readHoldStep = 20 * time.Millisecond
)

// afterReadHoldLock runs between taking the shared lock and checking that
// the marker is still the snapshot's: the seam a test moves the snapshot
// aside through. Nil in production.
var afterReadHoldLock func()

// holdNow is the prune's clock for the age of a hold. A variable for the
// tests.
var holdNow = time.Now

var (
	holdSeenMu sync.Mutex
	// holdSeen is when this process's prune first found each marker held,
	// by path; dropped once the marker is free again.
	holdSeen = map[string]time.Time{}
	// holdOverridden names the markers whose expired hold the prune already
	// said it overrides, so it is said once.
	holdOverridden = map[string]bool{}
)

// flockFn is syscall.Flock: the seam a test injects a lock error through
// that a local filesystem never gives (EBADF, which NFS returns for an
// exclusive lock on a file opened read-only).
var flockFn = syscall.Flock

// errNotAMarker is a _SUCCESS that is not a regular file (a FIFO, a device):
// nothing to hold, and never something to wait on.
var errNotAMarker = errors.New("the snapshot's _SUCCESS is not a regular file")

// openMarker opens a snapshot's marker to lock it: never through a symlink,
// never blocking (O_NONBLOCK: a FIFO named _SUCCESS opens at once and is
// refused, instead of wedging the caller), and only a regular file. forWrite
// opens it read-write when it can: on NFS, Linux turns flock into a POSIX
// lock, and an EXCLUSIVE one needs the file open for writing (flock(2)), so
// the prune, which owns the folder, asks for that and falls back to
// read-only when it is refused. writable reports which it got. A reader
// opens read-only: a shared lock needs only read access.
func openMarker(snapDir string, forWrite bool) (f *os.File, writable bool, err error) {
	path := filepath.Join(snapDir, SuccessMarker)
	if forWrite {
		f, err = os.OpenFile(path, os.O_RDWR|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
		writable = err == nil
	}
	if f == nil {
		f, err = os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	}
	if err != nil {
		return nil, false, err
	}
	if fi, serr := f.Stat(); serr != nil || !fi.Mode().IsRegular() {
		f.Close()
		return nil, false, errNotAMarker
	}
	return f, writable, nil
}

// HoldSnapshot keeps the snapshot directory snapDir from the prune of any
// process until release is called, for at most readHoldLimit. gone reports
// that the snapshot was moved aside or removed before the hold could be
// taken (the caller finds its snapshot again); release is then a no-op. A
// snapshot that cannot be held (no marker, an unreadable one, a marker that
// is a link or not a regular file, a non-local path, a lock not had within
// readHoldWait) is read unheld, as before this existed: release is a no-op
// and gone is false.
//
// release is safe to call more than once.
func HoldSnapshot(snapDir string) (release func(), gone bool) {
	noop := func() {}
	if snapDir == "" || strings.Contains(snapDir, "://") {
		return noop, false
	}
	f, _, err := openMarker(snapDir, false)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			if _, serr := os.Stat(snapDir); errors.Is(serr, fs.ErrNotExist) {
				return noop, true
			}
		}
		slog.Debug("baseline: this snapshot is read without a hold against the cleanup of another process", "snapshot", snapDir, "error", err)
		return noop, false
	}
	if err := flockShared(f); err != nil {
		f.Close()
		slog.Warn("baseline: could not hold this snapshot against the cleanup in time, so it is read without a hold; if the cleanup removes it meanwhile, this read fails and can be run again",
			"snapshot", snapDir, "waited", readHoldWait, "error", err)
		return noop, false
	}
	if afterReadHoldLock != nil {
		afterReadHoldLock()
	}
	// The prune moves the folder aside while it holds the marker; a reader
	// that opened the marker before that and got the lock after it holds a
	// file no longer at its path.
	marker := filepath.Join(snapDir, SuccessMarker)
	held, herr := f.Stat()
	now, nerr := os.Lstat(marker)
	if herr != nil || nerr != nil || !os.SameFile(held, now) {
		_ = flockFn(int(f.Fd()), syscall.LOCK_UN)
		f.Close()
		return noop, true
	}
	record, recordFile := writeHoldRecord(snapDir)
	var once sync.Once
	return func() {
		once.Do(func() {
			if record != "" {
				_ = os.Remove(record)
				_ = flockFn(int(recordFile.Fd()), syscall.LOCK_UN)
				recordFile.Close()
			}
			_ = flockFn(int(f.Fd()), syscall.LOCK_UN)
			f.Close()
		})
	}, false
}

// flockShared takes the shared lock without blocking, trying again every
// readHoldStep for readHoldWait.
func flockShared(f *os.File) error {
	deadline := time.Now().Add(readHoldWait)
	for {
		err := flockFn(int(f.Fd()), syscall.LOCK_SH|syscall.LOCK_NB)
		if err == nil {
			return nil
		}
		if !errors.Is(err, syscall.EWOULDBLOCK) && !errors.Is(err, syscall.EINTR) {
			return err
		}
		if !time.Now().Before(deadline) {
			return fmt.Errorf("the snapshot's marker stayed locked for %s: %w", readHoldWait, err)
		}
		time.Sleep(readHoldStep)
	}
}

// writeHoldRecord writes the reader's pid and start beside the snapshot and
// holds the record EXCLUSIVELY for as long as the read lasts, which is how
// the prune tells a live reader's record from a dead one's (liveHoldRecords).
// It returns the record's path and the open file that holds the lock, "" and
// nil when it could not (a folder the reader may only read: the hold is then
// bounded by the prune's own clock). 0600, created new, never through a link.
func writeHoldRecord(snapDir string) (string, *os.File) {
	root, name := filepath.Dir(snapDir), filepath.Base(snapDir)
	path := filepath.Join(root, fmt.Sprintf("%s%s.%d.%d", holdRecordPrefix, name, os.Getpid(), time.Now().UnixNano()))
	b, _ := json.Marshal(holdRecord{PID: os.Getpid(), Since: holdNow().UTC()})
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		slog.Debug("baseline: the hold on this snapshot has no record of its start; the cleanup bounds it by its own clock", "snapshot", snapDir, "error", err)
		return "", nil
	}
	if _, err := f.Write(b); err != nil {
		f.Close()
		_ = os.Remove(path)
		return "", nil
	}
	if err := flockFn(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		// A record nobody holds reads as a dead reader's, so it is not left.
		slog.Debug("baseline: the record of this hold could not be locked; the cleanup bounds the hold by its own clock", "snapshot", snapDir, "error", err)
		f.Close()
		_ = os.Remove(path)
		return "", nil
	}
	return path, f
}

// holdRecordPaths lists the records beside root for the snapshot name.
func holdRecordPaths(root, name string) []string {
	m, _ := filepath.Glob(filepath.Join(root, holdRecordPrefix+name+".*"))
	return m
}

// liveHoldRecords reads the records beside root for the snapshot name and
// removes those of readers that are gone: a record its reader no longer
// holds (it can be taken exclusively without waiting) belongs to a process
// that ended, killed included. since is the earliest start among the live
// ones, pids their readers, dead how many were removed. A record that
// cannot be tested (another user's, unreadable) is taken as live: the limit
// still bounds it.
func liveHoldRecords(root, name string) (since time.Time, pids []int, dead int) {
	for _, p := range holdRecordPaths(root, name) {
		f, err := os.OpenFile(p, os.O_RDWR|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
		if err != nil {
			f, err = os.OpenFile(p, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
		}
		if err != nil {
			continue
		}
		fi, serr := f.Stat()
		if serr != nil || !fi.Mode().IsRegular() {
			f.Close()
			continue
		}
		if flockFn(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB) == nil {
			_ = os.Remove(p)
			_ = flockFn(int(f.Fd()), syscall.LOCK_UN)
			f.Close()
			dead++
			continue
		}
		b, rerr := io.ReadAll(f)
		f.Close()
		var r holdRecord
		if rerr != nil || json.Unmarshal(b, &r) != nil || r.Since.IsZero() {
			continue
		}
		pids = append(pids, r.PID)
		if since.IsZero() || r.Since.Before(since) {
			since = r.Since
		}
	}
	return since, pids, dead
}

var (
	lockWarnedMu sync.Mutex
	// lockWarned names each folder and lock error the prune already warned
	// about, so a condition that lasts (a filesystem without flock) is said
	// once, not at every snapshot of every cleanup.
	lockWarned = map[string]bool{}
)

// lockForRemoval takes snapDir's marker exclusively without waiting, for the
// prune, right before the snapshot is moved aside. held reports that a
// reader holds it (HoldSnapshot) and its hold is inside readHoldLimit: the
// snapshot stays. A hold past the limit is overridden, said once, and the
// removal goes on without the lock. With no marker, or one that is a link or
// not a regular file, nothing could hold it and the removal goes on as
// before. A lock that fails for any reason but "held" goes on too, and is
// warned about once per folder and error: a reader of another process may
// then be on it unprotected. unlock is called once the snapshot is out of
// its path.
func lockForRemoval(snapDir string) (unlock func(), held bool) {
	noop := func() {}
	key := filepath.Join(snapDir, SuccessMarker)
	root, name := filepath.Dir(snapDir), filepath.Base(snapDir)
	f, writable, err := openMarker(snapDir, true)
	if err != nil {
		return noop, false
	}
	if err := flockFn(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		if !errors.Is(err, syscall.EWOULDBLOCK) {
			warnKey := root + "\x00" + err.Error()
			lockWarnedMu.Lock()
			say := !lockWarned[warnKey]
			lockWarned[warnKey] = true
			lockWarnedMu.Unlock()
			if say {
				slog.Warn("baseline prune: could not lock a snapshot's _SUCCESS before removing it, so a reader in another process may be reading it unprotected; snapshots of this folder are removed as before (on NFS an exclusive lock needs the file open for writing)",
					"dir", root, "snapshot", name, "opened_for_writing", writable, "error", err)
			}
			return noop, false
		}
		now := holdNow()
		since, pids, dead := liveHoldRecords(root, name)
		holdSeenMu.Lock()
		first, ok := holdSeen[key]
		if !ok || dead > 0 {
			// A reader that died leaves its age behind in this clock too:
			// started over, so the readers still on it are not charged with
			// it (#2675).
			first = now
			holdSeen[key] = now
			delete(holdOverridden, key)
		}
		if since.IsZero() || first.Before(since) {
			since = first
		}
		expired := now.Sub(since) > readHoldLimit
		say := expired && !holdOverridden[key]
		if say {
			holdOverridden[key] = true
		}
		holdSeenMu.Unlock()
		if !expired {
			return noop, true
		}
		if say {
			slog.Warn("baseline prune: a reader has held this snapshot for longer than any read is allowed to, so the cleanup no longer waits for it and removes it; that reader will fail",
				"dir", root, "snapshot", name, "held_since", since.UTC().Format(time.RFC3339), "limit", readHoldLimit, "holder_pids", pids)
		}
		return func() { forgetHold(key, root, name) }, false
	}
	return func() {
		// Records first, while no reader can hold the marker: any left now
		// are of readers that ended without removing theirs.
		forgetHold(key, root, name)
		_ = flockFn(int(f.Fd()), syscall.LOCK_UN)
		f.Close()
	}, false
}

// forgetHold drops what the prune knew of a marker's hold, and the records
// beside the snapshot: it was free, or it is being removed.
func forgetHold(key, root, name string) {
	holdSeenMu.Lock()
	delete(holdSeen, key)
	delete(holdOverridden, key)
	holdSeenMu.Unlock()
	for _, p := range holdRecordPaths(root, name) {
		_ = os.Remove(p)
	}
}

// recordSnapshot is the snapshot a hold record names: the name between the
// prefix and the last two fields (pid, nanos), read from the right so a
// name with a dot in it is still read whole.
func recordSnapshot(base string) (string, bool) {
	rest, ok := strings.CutPrefix(base, holdRecordPrefix)
	if !ok {
		return "", false
	}
	for range 2 {
		i := strings.LastIndex(rest, ".")
		if i <= 0 {
			return "", false
		}
		rest = rest[:i]
	}
	return rest, true
}

// sweepHoldRecords forgets every hold of root that nobody holds now: the
// records of a reader that died or whose snapshot is gone, and the first
// time this process found a marker held. Without it, a reader that comes
// days later to a snapshot the cleanup once found held, or whose earlier
// reader died, would inherit that old start and lose its snapshot at once.
// A marker is free when it can be taken exclusively without waiting; it is
// forgotten while taken, so no reader starts a hold in between. Best-effort.
func sweepHoldRecords(root string) {
	root = filepath.Clean(root)
	names := map[string]bool{}
	matches, _ := filepath.Glob(filepath.Join(root, holdRecordPrefix+"*"))
	for _, p := range matches {
		if name, ok := recordSnapshot(filepath.Base(p)); ok {
			names[name] = true
		}
	}
	holdSeenMu.Lock()
	for key := range holdSeen {
		if snap := filepath.Dir(key); filepath.Dir(snap) == root {
			names[filepath.Base(snap)] = true
		}
	}
	holdSeenMu.Unlock()
	for name := range names {
		snap := filepath.Join(root, name)
		key := filepath.Join(snap, SuccessMarker)
		f, _, err := openMarker(snap, true)
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				forgetHold(key, root, name)
			}
			continue
		}
		if flockFn(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB) == nil {
			forgetHold(key, root, name)
			_ = flockFn(int(f.Fd()), syscall.LOCK_UN)
		} else if _, _, dead := liveHoldRecords(root, name); dead > 0 {
			// Still held, by someone else: the dead readers' records go,
			// and so does the clock they started.
			holdSeenMu.Lock()
			delete(holdSeen, key)
			delete(holdOverridden, key)
			holdSeenMu.Unlock()
		}
		f.Close()
	}
}
