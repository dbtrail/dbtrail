package baseline

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
)

// Snapshot completeness markers (#467).
//
// A baseline run converts one table at a time into
// <output>/<timestamp>/<db>/<table>.parquet. Per-table cleanup removes a
// table's own partial Parquet on failure, but the tables that converted BEFORE
// a mid-run failure stay on disk — a snapshot directory that is byte-for-byte
// indistinguishable from a complete one. Discovery (FindBaseline /
// DiscoverBaselines / ListBaselines) would then treat it as the newest
// snapshot and silently reconstruct missing tables from an older one (#461) or
// return ErrNoBaseline.
//
// The fix is a snapshot-level marker, NOT an atomic rename-on-complete: the
// --retry resume path reuses already-converted Parquet under <output>/<tsDir>,
// so renaming the directory away on failure would leave nothing to resume from.
//
//   - SuccessMarker   ("_SUCCESS")    written ONLY when every table converted.
//   - IncompleteMarker ("_INCOMPLETE") written when a run fails or is cancelled
//     mid-way, so a NEW partial snapshot is positively flagged as incomplete
//     and excluded from discovery.
//
// Backward compatibility: snapshots produced before this change have NEITHER
// marker. They are treated as complete-by-default — SnapshotComplete returns
// true when neither marker is present — so existing baselines keep working.
// Only an explicit IncompleteMarker (a new partial run) is treated as
// incomplete; the SuccessMarker is the affirmative signal for runs that have it.
const (
	// SuccessMarker is written under <output>/<timestamp>/ on full success.
	SuccessMarker = "_SUCCESS"
	// IncompleteMarker is written under <output>/<timestamp>/ when a run fails
	// or is cancelled before every table converted.
	IncompleteMarker = "_INCOMPLETE"
)

// SnapshotViewsName is the DuckDB schema file published INSIDE a snapshot,
// next to its markers (#1583): a pinned views.sql naming the files that sit
// beside it, so the file cannot be out of step with the data it describes.
// The NAME lives here rather than in internal/views because upload.go and the
// hook below need it and views imports this package; internal/views aliases
// it (views.SnapshotFileName) rather than spelling a second literal.
const SnapshotViewsName = "views.sql"

// snapshotViewsWriter publishes SnapshotViewsName into a completing snapshot.
// It is a hook, not an import, because the generator lives in internal/views,
// which imports THIS package for the markers and the pointer — the arrow
// cannot point both ways. internal/views arms it from its init(), so every
// binary that links the generator (all three producers' binaries do) gets the
// artifact through the same single door as the pointer: WriteSuccessMarker.
//
// Contract: best-effort. The hook logs its own failures and never fails a
// completed snapshot — same rule as the pointer publish below.
var snapshotViewsWriter func(snapshotDir string)

// SetSnapshotViewsWriter arms the publish-time views.sql writer. Nil disarms
// (tests). See snapshotViewsWriter for why this is a hook.
func SetSnapshotViewsWriter(f func(snapshotDir string)) { snapshotViewsWriter = f }

// SnapshotViewsWriterArmed reports whether a writer is installed, for the
// per-binary wiring tests: arming rides the import graph, and an import that
// silently falls away would take this artifact with it, with nothing red.
func SnapshotViewsWriterArmed() bool { return snapshotViewsWriter != nil }

// WriteSuccessMarker writes the _SUCCESS marker into snapshotDir and removes any
// stale _INCOMPLETE marker left by an earlier failed attempt (the --retry path
// can complete a snapshot that a previous run flagged incomplete).
//
// Only the _SUCCESS write is fatal: once it lands, SnapshotComplete reports
// complete (it checks _SUCCESS first), so a failure to remove a leftover
// _INCOMPLETE is harmless bookkeeping — surfacing it as an error would make a
// genuinely-completed snapshot read as "could not write _SUCCESS". So we log
// that case rather than return it.
func WriteSuccessMarker(snapshotDir string) error {
	// The snapshot's own views file goes in BEFORE the marker (#1583), so
	// _SUCCESS keeps meaning "everything in place" and the S3 upload — which
	// defers _SUCCESS to the very end — carries the file inside the same
	// crash-safety bracket as the data. This is the single place a snapshot
	// becomes complete (the doc below says why), which makes it the single
	// place the artifact is published from too. Best-effort by the hook's own
	// contract: the writer logs and a missing views.sql costs a convenience,
	// never the snapshot.
	if snapshotViewsWriter != nil {
		snapshotViewsWriter(snapshotDir)
	}
	if err := os.WriteFile(filepath.Join(snapshotDir, SuccessMarker), nil, 0o644); err != nil {
		return fmt.Errorf("write %s marker: %w", SuccessMarker, err)
	}
	if err := os.Remove(filepath.Join(snapshotDir, IncompleteMarker)); err != nil && !os.IsNotExist(err) {
		slog.Warn("could not remove stale incomplete-snapshot marker (harmless; _SUCCESS decides completeness)",
			"dir", snapshotDir, "marker", IncompleteMarker, "error", err)
	}
	// Move the `current` pointer AFTER _SUCCESS lands, never before: the pointer
	// must not name a snapshot that is still being written. This is the single
	// place a snapshot becomes complete, which is why the pointer lives here
	// rather than in each producer — `bintrail baseline`, the Postgres baseline
	// and `reconstruct --output-format parquet` (the engine behind `baseline
	// refresh` and the console's periodic refresh) all arrive through this
	// function, and so will any producer added later.
	//
	// Best-effort, like the _INCOMPLETE removal above: the snapshot IS complete
	// and every recovery path finds it by its own name. Only the convenience of
	// an already-generated views file following along is lost, so say exactly
	// that rather than failing a finished snapshot.
	if err := PublishCurrentPointer(snapshotDir); err != nil {
		// Error, not Warn. The snapshot IS complete and recovery is unaffected,
		// so this does not fail the run -- but an already-generated views file
		// that follows the pointer told its reader it would move, and it now
		// will not. Nothing at query time says so: every path still resolves
		// and the rows simply stop changing. This log line is the only signal
		// that exists, so it must not sit at the level routine bookkeeping does.
		slog.Error("could not update the `current` baseline pointer; the snapshot is complete and recovery is unaffected, but DuckDB views generated to follow the pointer will go on reading the PREVIOUS snapshot, with no error at query time, until this is fixed",
			"dir", snapshotDir, "pointer", CurrentLinkName, "error", err)
	}
	return nil
}

// WriteIncompleteMarker writes the _INCOMPLETE marker into snapshotDir. It is
// best-effort: a run already returning a failure error should not be masked by
// a marker-write error, so callers log rather than propagate.
func WriteIncompleteMarker(snapshotDir string) error {
	if err := os.WriteFile(filepath.Join(snapshotDir, IncompleteMarker), nil, 0o644); err != nil {
		return fmt.Errorf("write %s marker: %w", IncompleteMarker, err)
	}
	return nil
}

// SnapshotComplete reports whether the snapshot directory snapshotDir is a
// COMPLETE baseline, for local discovery. The rule (see the package comment):
//   - _INCOMPLETE present, _SUCCESS absent → incomplete (false).
//   - otherwise (marker-absent legacy snapshot, or _SUCCESS present) → complete.
//
// Operator note: snapshots produced before this release carry NEITHER marker
// and are trusted as complete-by-default. If a pre-release run may have been
// interrupted mid-way, re-run `bintrail baseline` for that snapshot — there is
// no marker to flag it incomplete retroactively.
func SnapshotComplete(snapshotDir string) bool {
	if _, err := os.Stat(filepath.Join(snapshotDir, SuccessMarker)); err == nil {
		return true
	}
	if _, err := os.Stat(filepath.Join(snapshotDir, IncompleteMarker)); err == nil {
		return false
	}
	// Neither marker: a pre-marker (legacy) snapshot — complete by default.
	return true
}

// incompleteWarned is every incomplete snapshot directory a listing has
// already warned about in this process (#2180).
var incompleteWarned sync.Map

// WarnIncompleteSnapshotOnce is the listings' "skipping incomplete snapshot"
// warning, said once per directory per process and at Debug after that.
// Listings run on every page load and on every statement on the time-travel
// port, so a per-listing warning for one leftover was hundreds of identical
// lines an hour, which teaches an operator to stop reading the log. The
// directory is still skipped every time; only the repetition is quieter. A
// new leftover is a new path and is still said.
func WarnIncompleteSnapshotOnce(msg, path string) {
	if _, seen := incompleteWarned.LoadOrStore(path, struct{}{}); seen {
		slog.Debug(msg, "path", path)
		return
	}
	slog.Warn(msg, "path", path)
}
