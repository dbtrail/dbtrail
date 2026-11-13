//go:build unix

package baseline

import (
	"encoding/json"
	"errors"
	"fmt"
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

// openMarker opens a snapshot's marker to lock it, never through a symlink.
func openMarker(snapDir string) (*os.File, error) {
	return os.OpenFile(filepath.Join(snapDir, SuccessMarker), os.O_RDONLY|syscall.O_NOFOLLOW, 0)
}

// HoldSnapshot keeps the snapshot directory snapDir from the prune of any
// process until release is called, for at most readHoldLimit. gone reports
// that the snapshot was moved aside or removed before the hold could be
// taken (the caller finds its snapshot again); release is then a no-op. A
// snapshot that cannot be held (no marker, an unreadable one, a marker that
// is a link, a non-local path, a lock not had within readHoldWait) is read
// unheld, as before this existed: release is a no-op and gone is false.
//
// release is safe to call more than once.
func HoldSnapshot(snapDir string) (release func(), gone bool) {
	noop := func() {}
	if snapDir == "" || strings.HasPrefix(snapDir, "s3://") {
		return noop, false
	}
	f, err := openMarker(snapDir)
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
		_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		f.Close()
		return noop, true
	}
	record := writeHoldRecord(snapDir)
	var once sync.Once
	return func() {
		once.Do(func() {
			if record != "" {
				_ = os.Remove(record)
			}
			_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
			f.Close()
		})
	}, false
}

// flockShared takes the shared lock without blocking, trying again every
// readHoldStep for readHoldWait.
func flockShared(f *os.File) error {
	deadline := time.Now().Add(readHoldWait)
	for {
		err := syscall.Flock(int(f.Fd()), syscall.LOCK_SH|syscall.LOCK_NB)
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
// returns its path, "" when it could not (a folder the reader may only read:
// the hold is then bounded by the prune's own clock). 0600, created new,
// never through a link.
func writeHoldRecord(snapDir string) string {
	root, name := filepath.Dir(snapDir), filepath.Base(snapDir)
	path := filepath.Join(root, fmt.Sprintf("%s%s.%d.%d", holdRecordPrefix, name, os.Getpid(), time.Now().UnixNano()))
	b, _ := json.Marshal(holdRecord{PID: os.Getpid(), Since: time.Now().UTC()})
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		slog.Debug("baseline: the hold on this snapshot has no record of its start; the cleanup bounds it by its own clock", "snapshot", snapDir, "error", err)
		return ""
	}
	_, werr := f.Write(b)
	cerr := f.Close()
	if werr != nil || cerr != nil {
		_ = os.Remove(path)
		return ""
	}
	return path
}

// holdRecords reads the records beside root for the snapshot name: the
// earliest start among them, and the pids. Unreadable or malformed records
// are skipped.
func holdRecords(root, name string) (since time.Time, pids []int, paths []string) {
	matches, _ := filepath.Glob(filepath.Join(root, holdRecordPrefix+name+".*"))
	for _, p := range matches {
		paths = append(paths, p)
		fi, err := os.Lstat(p)
		if err != nil || !fi.Mode().IsRegular() {
			continue
		}
		b, err := os.ReadFile(p)
		if err != nil {
			continue
		}
		var r holdRecord
		if json.Unmarshal(b, &r) != nil || r.Since.IsZero() {
			continue
		}
		pids = append(pids, r.PID)
		if since.IsZero() || r.Since.Before(since) {
			since = r.Since
		}
	}
	return since, pids, paths
}

// lockForRemoval takes snapDir's marker exclusively without waiting, for the
// prune, right before the snapshot is moved aside. held reports that a
// reader holds it (HoldSnapshot) and its hold is inside readHoldLimit: the
// snapshot stays. A hold past the limit is overridden, said once, and the
// removal goes on without the lock. With no marker, a marker that is a link,
// or one that cannot be opened or locked for another reason, nothing could
// hold it and the removal goes on as before. unlock is called once the
// snapshot is out of its path.
func lockForRemoval(snapDir string) (unlock func(), held bool) {
	noop := func() {}
	key := filepath.Join(snapDir, SuccessMarker)
	root, name := filepath.Dir(snapDir), filepath.Base(snapDir)
	f, err := openMarker(snapDir)
	if err != nil {
		return noop, false
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		if !errors.Is(err, syscall.EWOULDBLOCK) {
			slog.Debug("baseline prune: could not lock a snapshot's marker; removing it as before", "snapshot", snapDir, "error", err)
			return noop, false
		}
		now := holdNow()
		since, pids, _ := holdRecords(root, name)
		holdSeenMu.Lock()
		first, ok := holdSeen[key]
		if !ok {
			first = now
			holdSeen[key] = now
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
		_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
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
	_, _, paths := holdRecords(root, name)
	for _, p := range paths {
		_ = os.Remove(p)
	}
}

// sweepHoldRecords removes the records beside root whose snapshot is no
// longer there: a reader that died, or one whose snapshot went past the
// limit. Best-effort.
func sweepHoldRecords(root string) {
	matches, _ := filepath.Glob(filepath.Join(root, holdRecordPrefix+"*"))
	for _, p := range matches {
		rest := strings.TrimPrefix(filepath.Base(p), holdRecordPrefix)
		name, _, ok := strings.Cut(rest, ".")
		if !ok || name == "" {
			continue
		}
		if _, err := os.Lstat(filepath.Join(root, name)); errors.Is(err, fs.ErrNotExist) {
			_ = os.Remove(p)
		}
	}
}
