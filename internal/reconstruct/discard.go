package reconstruct

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/snapshotdir"
)

// ErrSnapshotNotDiscardable marks a DELIBERATE refusal by
// DiscardUnpublishedSnapshot: the directory is not an unpublished snapshot, so
// nothing was removed and nothing went wrong.
//
// It exists so a caller can tell "I correctly declined" from "I tried and the
// filesystem refused". Those are the same return shape and completely different
// facts: the first is the guard working, the second means the reclaim cannot
// run for that directory and its disk is leaking. Reporting them at one level,
// under one message, is how the second one goes unnoticed.
var ErrSnapshotNotDiscardable = errors.New("not an unpublished snapshot")

// discardingSuffix names a snapshot directory that is on its way out.
//
// The prefix dot and the suffix are BOTH there so the name cannot parse as a
// snapshot timestamp: every discovery path (FindBaseline, ListBaselines,
// baseline.enumerateLocalSnapshots) skips a directory whose name
// snapshotdir.ParseTime rejects. `bintrail baseline`'s prune already stages the
// same way with ".<ts>.pruning", so this is that convention and not a second
// one.
const discardingSuffix = ".discarding"

// discardingName renders the staging name for one snapshot directory.
func discardingName(base string) string {
	return "." + base + discardingSuffix
}

// removeAllDir is os.RemoveAll, indirected so a test can drive a delete that
// fails PART WAY, which is the failure the rename below exists for and the one
// no fixture can stage on a healthy filesystem. Written by tests only;
// production never reassigns it.
var removeAllDir = os.RemoveAll

// SnapshotDirVacant reports whether dir is free for a new fold to write into:
// absent, or holding nothing but the _INCOMPLETE marker a previous failed run
// left behind.
//
// It is the same test ReconstructTables applies before it creates the snapshot
// directory, exported for one purpose: a caller that intends to RECLAIM what
// its own fold wrote can establish, BEFORE the fold runs, that everything in
// the directory afterwards is its own. Asking afterwards cannot answer that
// question, because by then the caller's own files are in there too.
//
// A directory that cannot be read is reported as NOT vacant, with the error, so
// a caller that cannot see what is in there never concludes it may delete it.
func SnapshotDirVacant(dir string) (bool, error) {
	leftover, err := snapshotDirLeftovers(dir)
	if err != nil {
		return false, err
	}
	return len(leftover) == 0, nil
}

// DiscardUnpublishedSnapshot removes a snapshot directory that a FAILED fold
// wrote, reclaiming the disk it holds. discarded reports whether the directory
// is gone from dir.
//
// It refuses unless the directory is positively marked incomplete: _INCOMPLETE
// present and _SUCCESS absent, which is exactly baseline.SnapshotComplete
// returning false. That is not a formality. A published snapshot carries
// _SUCCESS, and a legacy snapshot carries NEITHER marker and is complete by
// default, so both are refused by the same check. It also makes a path mistake
// fail safe: a directory this fold never wrote has no _INCOMPLETE marker, so a
// caller that computes the wrong path deletes nothing.
//
// The removal is a rename FIRST and a delete second, and the order is the whole
// point. os.RemoveAll deletes children in directory order with no promise that
// _INCOMPLETE goes last, and '_' sorts ahead of a lowercase schema directory,
// so a process killed mid-delete could leave Parquet files behind with no
// marker at all. baseline.SnapshotComplete reads a markerless directory as
// COMPLETE, which would turn an undiscoverable partial snapshot into a
// discoverable one: the exact failure #467 exists to prevent, caused by the
// cleanup meant to help. A rename inside one directory is atomic, and once it
// lands nothing can discover the snapshot however the delete then goes.
//
// A delete that fails after the rename returns discarded=true WITH the error:
// the snapshot is already beyond every reader, and only the bytes are still
// there. Callers report both.
func DiscardUnpublishedSnapshot(dir string) (discarded bool, err error) {
	info, err := os.Lstat(dir)
	if os.IsNotExist(err) {
		// The run failed before it created the directory. Nothing to reclaim,
		// and nothing to report.
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("inspect snapshot directory %s: %w", dir, err)
	}
	// Lstat, so a symlink is not followed: a fold creates a real directory, so
	// anything else here is not ours.
	if !info.IsDir() {
		return false, fmt.Errorf("%w: %s is not a directory, so it is not a snapshot this run wrote",
			ErrSnapshotNotDiscardable, dir)
	}
	if baseline.SnapshotComplete(dir) {
		return false, fmt.Errorf("%w: %s does not carry the %s marker",
			ErrSnapshotNotDiscardable, dir, baseline.IncompleteMarker)
	}
	staging := filepath.Join(filepath.Dir(dir), discardingName(filepath.Base(dir)))
	if err := os.Rename(dir, staging); err != nil {
		return false, fmt.Errorf("move %s aside before deleting it: %w", dir, err)
	}
	if err := removeAllDir(staging); err != nil {
		return true, fmt.Errorf("%s is no longer discoverable, but the files left at %s could not be deleted: %w",
			dir, staging, err)
	}
	return true, nil
}

// SweepDiscardedSnapshots removes ".<ts>.discarding" staging directories a
// previous discard did not finish deleting, and reports how many it reclaimed.
//
// This is the other half of the convention discardingSuffix borrows. Prune
// stages the same way AND sweeps its leftovers at the top of every cycle
// (baseline.sweepPruningLeftovers), and the sweep is what makes the leftovers
// bounded rather than permanent. Adopting only the rename would trade one leak
// for a rarer leak in a shape nothing names: a staging directory is invisible
// to every discovery path BY DESIGN, so no listing, no status and no console
// panel would ever mention the disk it holds.
//
// The producer is not only a disk fault. Nothing joins the refresh goroutine at
// shutdown, so a daemon killed during the delete exits with a part-deleted
// staging tree, and that path logs nothing at all because the process is gone
// before the caller's own line is written.
//
// Best-effort by contract: it is housekeeping running beside real work, so a
// directory it cannot read or delete is reported, never fatal. An unreadable
// ROOT yields (0, nil) rather than an error, matching prune's sweeper, because
// the caller's own work fails on the same root and would say so first.
func SweepDiscardedSnapshots(root string) (removed int, err error) {
	entries, readErr := os.ReadDir(root)
	if readErr != nil {
		return 0, nil
	}
	var errs []error
	for _, e := range entries {
		if !e.IsDir() || !isDiscardingName(e.Name()) {
			continue
		}
		p := filepath.Join(root, e.Name())
		if rmErr := removeAllDir(p); rmErr != nil {
			errs = append(errs, fmt.Errorf("sweep leftover staging directory %s: %w", p, rmErr))
			continue
		}
		removed++
	}
	return removed, errors.Join(errs...)
}

// isDiscardingName recognizes a staging directory this package wrote. Both
// halves are required: a directory an operator happens to have named
// ".backups.discarding" is not one of ours, and neither is a snapshot directory
// whose name merely ends in the suffix. So the part between the dot and the
// suffix must parse as a snapshot time, the exact shape discardingName makes.
func isDiscardingName(name string) bool {
	if !strings.HasPrefix(name, ".") || !strings.HasSuffix(name, discardingSuffix) {
		return false
	}
	_, ok := snapshotdir.ParseTime(strings.TrimSuffix(strings.TrimPrefix(name, "."), discardingSuffix))
	return ok
}
