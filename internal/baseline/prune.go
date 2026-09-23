package baseline

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/dbtrail/dbtrail/internal/storage"
)

// baselinePruneMinAge is a safety floor on how young a snapshot may be and still
// be pruned, independent of the retention window. A just-completed snapshot is
// the most likely to be the target of an in-flight reconstruct read; the floor
// keeps it on disk for at least an hour regardless of how aggressive Retain is.
// In practice Retain (parsed via cliutil.ParseRetain, whose smallest unit is 1h)
// already subsumes this, so it is belt-and-suspenders against a future caller
// that passes a sub-hour Retain programmatically.
const baselinePruneMinAge = time.Hour

// pruningSuffix marks a snapshot directory that has been renamed aside for
// deletion. A ".<timestamp>.pruning" name does NOT parse via
// parseBaselineDirTimestamp, so it is invisible to every reader/discovery walk
// the instant it is renamed — a crash mid-delete therefore self-excludes the
// half-removed tree rather than leaving a _SUCCESS-marked directory with missing
// table files (which a reader would treat as complete and then get ErrNoBaseline).
const pruningSuffix = ".pruning"

// readDir is os.ReadDir, indirected ONLY so a test can force an enumeration
// failure for a specific snapshot directory and exercise the unreadable
// force-keep path deterministically — a chmod-based test is skipped under a
// root-running CI, where it would otherwise be the safety net. Used solely by
// listSnapshotTables (the per-snapshot enumeration that gates deletion); the
// top-level directory walks deliberately keep os.ReadDir.
var readDir = os.ReadDir

// PruneOptions configures a local baseline-snapshot prune (#616).
//
// Two modes, chosen by whether the snapshots have an external destination:
//
//   - S3URL set: a snapshot is reclaimed only once its copy is confirmed at
//     the destination and it is older than Retain. This mirrors rotation's
//     archive invariant exactly — `PruneLocalAfterUpload && ArchiveS3 != ""`
//     (internal/rotation/rotation.go). KeepNewest is ignored.
//   - S3URL empty, KeepNewest > 0 (#1681): the local snapshots ARE the only
//     copies, so the invariant is "never leave fewer than KeepNewest": the
//     newest KeepNewest complete snapshots are always kept and older ones are
//     reclaimed. Retain is optional here and, when set, only keeps more.
//
// S3URL empty and KeepNewest zero is a deliberate no-op, logged loudly.
type PruneOptions struct {
	// LocalDir is the baseline output root pruned, laid out as
	// <LocalDir>/<timestamp>/<schema>/<table>.parquet. Required.
	LocalDir string
	// S3URL is the durable S3 prefix the snapshots were uploaded to
	// (s3://bucket/prefix/). Empty disables pruning entirely.
	S3URL string
	// S3Region is the optional AWS region for the durability probe; empty lets
	// the SDK resolve it from the ambient chain (AWS_REGION / ~/.aws / IAM role).
	S3Region string
	// Retain prunes COMPLETE snapshots older than this. Required (> 0) when
	// S3URL is set; optional with KeepNewest, where a snapshot younger than
	// Retain is kept even outside the newest KeepNewest.
	Retain time.Duration
	// KeepNewest is the local-only retention (#1681): with no S3URL, keep,
	// for every table, the newest KeepNewest complete snapshots holding it,
	// and reclaim snapshots outside all of those. Only complete, readable
	// snapshots dated at or before Now count as copies; everything else is
	// kept by its own rule and never displaces a real copy. 0 = no local-only
	// pruning (the pre-#1681 behavior).
	KeepNewest int
	// Now is an injectable clock for tests; the zero value means time.Now().UTC().
	Now time.Time
	// DryRun logs what would be pruned without deleting anything. NOTE: the zero
	// value deletes — set DryRun to preview (matches rotation's convention).
	DryRun bool
}

// PruneResult reports what a prune did, for the caller to log/surface.
//
// The Kept* counters are FIRST-MATCH reason codes, not independent set
// memberships: a snapshot that is both a keeper and recent counts once, as
// KeptKeeper (the order is incomplete > unreadable > keeper > newest > recent
// > not-durable). Together with len(Pruned) they partition the enumerated
// snapshot set — every snapshot lands in exactly one bucket.
type PruneResult struct {
	// Pruned holds the snapshot directory names actually removed (or, under
	// DryRun, that would be removed). A snapshot counts as removed once it has
	// been renamed aside, because from that instant no listing shows it, even
	// if deleting the renamed tree then fails and is retried next cycle.
	Pruned []string
	// ReclaimedBytes is the disk the removals freed: files with no other link.
	// A carried-forward file is a hard link shared with a newer snapshot, and
	// deleting one of its names frees nothing, so it is not counted. Under
	// DryRun the count is a lower bound (a file shared only among snapshots
	// that would all be removed is not counted).
	ReclaimedBytes int64
	// KeptNewest counts snapshots retained because they are among the newest
	// KeepNewest (local-only mode) and not already kept as a keeper.
	KeptNewest int
	// Busy reports that another prune held this directory's lock, so this one
	// did nothing. Not an error: the other prune is doing the same job.
	Busy bool
	// Unremoved counts snapshots the prune planned to remove and could not
	// move aside; they are still listed, and the attempt is recorded as a
	// failure (LastPruneFailureFile).
	Unremoved int
	// firstRemoveErr is the first of those errors, for the failure record.
	firstRemoveErr string
	// Undeleted counts snapshots that left the listing (renamed aside, so in
	// Pruned) whose files could not all be deleted: the disk they hold is
	// not back yet. A failure, recorded with the first error.
	Undeleted      int
	firstDeleteErr string
	// SweepFailures counts leftovers of earlier removals (".<ts>.pruning")
	// that still could not be deleted this cycle. Without it the cycle after
	// a failed delete plans nothing, counts as a success, and clears the
	// record while the disk stays full.
	SweepFailures int
	firstSweepErr string
	// firstUnreadable is the path and error of the first snapshot counted in
	// KeptUnreadable, for the failure record.
	firstUnreadable string
	// KeptKeeper counts snapshots retained because they are the newest snapshot
	// containing some table — reconstruct.FindBaseline's per-table target.
	KeptKeeper int
	// KeptNotDurable counts snapshots retained because no durable S3 copy was
	// confirmed (never delete the only copy).
	KeptNotDurable int
	// KeptRecent counts snapshots retained because they are within the retention
	// window or younger than the safety floor.
	KeptRecent int
	// KeptIncomplete counts snapshots skipped because they carry an _INCOMPLETE
	// marker (an in-progress write or a partial run resumable via --retry — never
	// our business).
	KeptIncomplete int
	// KeptUnreadable counts complete snapshots kept because their directory could
	// not be fully enumerated (a transient os.ReadDir failure: fd exhaustion, an
	// NFS/permission blip). We cannot prove such a snapshot is redundant, so the
	// fail-safe direction for a delete is to keep it — reconstruct can still
	// os.Stat its table files even when the dir cannot be listed.
	KeptUnreadable int
	// ProbeErrors counts snapshots whose S3 durability could not be confirmed due
	// to a probe error (not a clean 404). It is a SUBSET of KeptNotDurable — those
	// kept because the probe errored rather than cleanly reported absence; the two
	// are different axes (where it landed vs why we couldn't confirm) and nothing
	// sums them. A nonzero value means retention is not reclaiming everything it
	// could because S3 was unreachable.
	ProbeErrors int
}

// durableProbe reports whether a snapshot directory has a confirmed durable copy
// in S3. It is the testing seam: PruneLocal builds an S3 HeadObject probe;
// pruneWithProbe takes it so the invariant logic is exercised without a live
// client (mirrors upload.go's s3UploadOps seam).
type durableProbe func(ctx context.Context, snapshotName string) (bool, error)

// PruneLocal removes redundant local baseline snapshots under opts.LocalDir,
// honoring these invariants (#616), plus a recency floor and a force-keep for
// snapshots whose directory can't be enumerated — both of which only ever keep
// MORE:
//
//   - Never delete the only copy: a snapshot is pruned only when its _SUCCESS
//     marker is confirmed present in S3 at the exact same timestamp prefix.
//   - Never delete the newest usable snapshot: the newest COMPLETE snapshot
//     containing each table is kept, matching reconstruct.FindBaseline's
//     per-table selection — pruning it would break Time-travel for that table.
//     A snapshot whose directory can't be listed is also force-kept: it may be
//     the newest readable copy of a table (reconstruct os.Stats the file path).
//   - Respect markers: _INCOMPLETE snapshots are never touched (they may be an
//     in-progress write or a partial run resumable via --retry), and a complete
//     snapshot is renamed aside before deletion so a reader never sees a
//     half-removed one.
//
// Pruning only ever narrows how far back local Time-travel reaches; the present
// (an `at=now` reconstruct) always resolves to a kept keeper, never to a pruned
// snapshot — findBaselineLocal filters candidates to at-or-before `at`.
func PruneLocal(ctx context.Context, opts PruneOptions) (PruneResult, error) {
	if opts.LocalDir == "" {
		return PruneResult{}, fmt.Errorf("baseline prune: LocalDir is required")
	}
	if opts.KeepNewest < 0 {
		return PruneResult{}, fmt.Errorf("baseline prune: KeepNewest must not be negative")
	}
	if opts.S3URL == "" {
		if opts.KeepNewest > 0 {
			if opts.Retain < 0 {
				return PruneResult{}, fmt.Errorf("baseline prune: Retain must not be negative")
			}
			// Local-only retention (#1681): no durability probe, the floor is
			// the newest KeepNewest instead.
			return pruneWithProbe(ctx, opts, nil)
		}
		// No durable destination and no count to keep → every local snapshot
		// is the only copy. Refuse, loudly, so a retention setting on a
		// local-only deployment is not a silent no-op that looks like a bug
		// when the disk keeps filling.
		slog.Warn("baseline prune: no S3 destination configured and no number of local snapshots to keep; refusing to prune local snapshots (they are the only copy).",
			"dir", opts.LocalDir)
		return PruneResult{}, nil
	}
	if opts.Retain <= 0 {
		return PruneResult{}, fmt.Errorf("baseline prune: Retain must be positive")
	}

	bucket, prefix, err := storage.ParseS3URL(opts.S3URL)
	if err != nil {
		return PruneResult{}, fmt.Errorf("baseline prune: invalid S3 URL %q: %w", opts.S3URL, err)
	}
	client, err := storage.NewS3ClientForBucket(ctx, bucket, opts.S3Region)
	if err != nil {
		return PruneResult{}, fmt.Errorf("baseline prune: %w", err)
	}

	probe := func(ctx context.Context, snapshotName string) (bool, error) {
		// The exact key Upload published the _SUCCESS marker at:
		// <prefix>/<timestamp>/_SUCCESS. An exact HeadObject (not a glob/prefix
		// match) is the strongest "this snapshot is durable" signal — a loose
		// match could false-positive and prune the only copy.
		key, err := storage.BuildS3Key(opts.LocalDir, filepath.Join(opts.LocalDir, snapshotName, SuccessMarker), prefix)
		if err != nil {
			return false, err
		}
		return storage.S3ObjectExists(ctx, client, bucket, key)
	}
	return pruneWithProbe(ctx, opts, probe)
}

// pruneWithProbe runs one prune attempt and records its outcome beside the
// snapshots (#1681): a failed attempt writes .last-prune-failure.json (when,
// and why), a successful one removes it. A dry run and a prune that stepped
// aside for another touch neither. Recording is best-effort and logged: the
// attempt's own result is returned unchanged.
func pruneWithProbe(ctx context.Context, opts PruneOptions, probe durableProbe) (PruneResult, error) {
	res, err := pruneAttempt(ctx, opts, probe)
	if opts.DryRun || res.Busy {
		return res, err
	}
	now := opts.Now
	if now.IsZero() {
		now = time.Now().UTC()
	}
	if reason := pruneFailureReason(res, err); reason != "" {
		if werr := writeFailureRecord(opts.LocalDir, PruneFailure{At: now, Reason: reason}); werr != nil {
			slog.Error("baseline prune: the attempt failed and the record of the failure could not be written either; the page will not say it",
				"dir", opts.LocalDir, "reason", reason, "error", werr)
		}
	} else if rerr := removeRecord(filepath.Join(opts.LocalDir, LastPruneFailureFile)); rerr != nil && !os.IsNotExist(rerr) {
		slog.Error("baseline prune: the attempt succeeded but the record of an earlier failure could not be removed; the page will go on showing it",
			"dir", opts.LocalDir, "error", rerr)
	}
	return res, err
}

// pruneFailureReason is why an attempt counts as failed, in words the page
// can show, or "" when it did everything it planned. Snapshots kept for lack
// of a confirmed S3 copy are policy, not failure; snapshots that could not be
// CHECKED are a failure, because retention stalls while the page would say
// it runs.
func pruneFailureReason(res PruneResult, err error) string {
	if err != nil {
		return err.Error()
	}
	var why []string
	if res.Unremoved > 0 {
		why = append(why, fmt.Sprintf("%s could not be removed: %s.", snapshotsWord(res.Unremoved), res.firstRemoveErr))
	}
	if res.Undeleted > 0 {
		why = append(why, fmt.Sprintf("%s left the list, but their files could not all be deleted, so the disk space is not free yet: %s. DBTrail tries again at the next cleanup; check that it may delete files in that folder.",
			snapshotsWord(res.Undeleted), res.firstDeleteErr))
	}
	if res.SweepFailures > 0 {
		why = append(why, fmt.Sprintf("Files of %s removed earlier could not be deleted: %s. Check that DBTrail may delete files in that folder.",
			snapshotsWord(res.SweepFailures), res.firstSweepErr))
	}
	if res.KeptUnreadable > 0 {
		why = append(why, fmt.Sprintf("%s could not be read, so they are kept and never removed: %s. Check that DBTrail may read that folder.",
			snapshotsWord(res.KeptUnreadable), res.firstUnreadable))
	}
	if res.ProbeErrors > 0 {
		why = append(why, fmt.Sprintf("%s could not be checked at the S3 destination, so they were kept.", snapshotsWord(res.ProbeErrors)))
	}
	return strings.Join(why, " ")
}

// snapshotsWord is "1 snapshot" or "n snapshots".
func snapshotsWord(n int) string {
	if n == 1 {
		return "1 snapshot"
	}
	return fmt.Sprintf("%d snapshots", n)
}

// pathErr names the path an error is about, once: an os error already
// carries it, an injected one may not.
func pathErr(path string, err error) string {
	if strings.Contains(err.Error(), path) {
		return err.Error()
	}
	return path + ": " + err.Error()
}

// pruneAttempt is PruneLocal's IO body with the durability check injected, so
// the keeper/marker/age invariants are testable without S3. With no S3URL it
// is the local-only keep-newest mode (#1681): there is nothing to confirm
// durable, probe is unused, and the newest opts.KeepNewest snapshots take the
// probe's place as the floor. The mode is decided by S3URL, never by the
// probe, so a destination always means "delete only what it confirmed".
func pruneAttempt(ctx context.Context, opts PruneOptions, probe durableProbe) (PruneResult, error) {
	now := opts.Now
	if now.IsZero() {
		now = time.Now().UTC()
	}
	localOnly := opts.S3URL == ""
	if localOnly && opts.KeepNewest <= 0 {
		// Defensive: no destination and no floor would reclaim every
		// non-keeper. PruneLocal never builds this; refuse rather than trust it.
		return PruneResult{}, fmt.Errorf("baseline prune: local-only pruning needs KeepNewest > 0")
	}
	if !localOnly && probe == nil {
		return PruneResult{}, fmt.Errorf("baseline prune: a destination needs a durability probe")
	}

	// One prune per directory at a time (#1681): `bintrail baseline
	// --baseline-retain`, which prunes after its upload, and the daemon's loop
	// can meet on one root, and each decides from its own listing. A missing directory has nothing to prune and nothing to
	// lock.
	if _, err := os.Stat(opts.LocalDir); os.IsNotExist(err) {
		return PruneResult{}, nil
	}
	unlock, busy, err := lockPrune(opts.LocalDir)
	if err != nil {
		return PruneResult{}, fmt.Errorf("baseline prune: %w", err)
	}
	if busy {
		slog.Info("baseline prune: another prune is running on this directory; skipping this cycle", "dir", opts.LocalDir)
		return PruneResult{Busy: true}, nil
	}
	defer unlock()

	// Sweep any ".<ts>.pruning" leftovers from a previous crashed cycle first —
	// they are already invisible to discovery, but they still occupy disk.
	sweepFailures, firstSweepErr := sweepPruningLeftovers(opts.LocalDir)

	snaps, err := enumerateLocalSnapshots(opts.LocalDir)
	if err != nil {
		return PruneResult{}, fmt.Errorf("baseline prune: %w", err)
	}
	// Carried on the result whatever the plan says: a leftover that cannot be
	// deleted is a failure even in a cycle that has nothing new to remove.
	withSweep := func(res PruneResult) PruneResult {
		res.SweepFailures, res.firstSweepErr = sweepFailures, firstSweepErr
		for _, s := range snaps {
			if s.unreadable {
				res.firstUnreadable = s.readErr
				break
			}
		}
		return res
	}
	keepers := computeKeepers(snaps, now)
	keepPointerTarget(opts.LocalDir, keepers)

	if localOnly {
		newest := computeNewestN(snaps, opts.KeepNewest, now)
		pruneNames, res := planPruneKeepNewest(snaps, keepers, newest, opts.Retain, baselinePruneMinAge, now)
		return withSweep(removePlanned(opts, now, pruneNames, res, "no external destination; keeping the newest "+fmt.Sprint(opts.KeepNewest))), nil
	}

	// Confirm durability only for snapshots that could actually be pruned —
	// complete, readable, not a keeper, and already past the retention/min-age
	// floor. Keepers and recent snapshots are kept regardless of S3, so probing
	// them would waste HeadObject calls. A probe error is fail-safe: treat as NOT
	// durable (keep), never as durable (which could delete the only copy).
	durable := make(map[string]bool)
	var probeErrors int
	for _, s := range snaps {
		if !s.complete || s.unreadable || keepers[s.name] {
			continue
		}
		age := now.Sub(s.ts)
		if age < opts.Retain || age < baselinePruneMinAge {
			continue // too recent to prune → no need to confirm durability
		}
		ok, perr := probe(ctx, s.name)
		if perr != nil {
			slog.Warn("baseline prune: could not confirm S3 durability; keeping snapshot",
				"snapshot", s.name, "error", perr)
			probeErrors++
			continue // absent from `durable` → planPrune keeps it
		}
		durable[s.name] = ok
	}

	pruneNames, res := planPrune(snaps, keepers, durable, opts.Retain, baselinePruneMinAge, now)
	res.ProbeErrors = probeErrors
	if probeErrors > 0 {
		// One aggregate signal so a systemic S3 outage reads as "retention stalled
		// because S3 is unreachable", not just a scatter of per-snapshot warns.
		slog.Warn("baseline prune: S3 durability could not be confirmed for some snapshots; retention is not reclaiming all eligible disk",
			"unconfirmed", probeErrors)
	}

	return withSweep(removePlanned(opts, now, pruneNames, res, "durable copy in S3")), nil
}

// removePlanned deletes the planned snapshots, tallies what left the listing,
// and records the prune beside the snapshots when anything did (#1681). why is
// the reason each removal is logged under.
func removePlanned(opts PruneOptions, now time.Time, pruneNames []string, res PruneResult, why string) PruneResult {
	for _, name := range pruneNames {
		size := reclaimableSize(filepath.Join(opts.LocalDir, name))
		if opts.DryRun {
			slog.Info("baseline prune (dry-run): would remove redundant local snapshot",
				"snapshot", name, "bytes", size, "reason", why)
			res.Pruned = append(res.Pruned, name)
			res.ReclaimedBytes += size
			continue
		}
		staged, err := removeSnapshot(opts.LocalDir, name)
		if !staged {
			slog.Warn("baseline prune: could not remove snapshot; it will be retried next cycle",
				"snapshot", name, "error", err)
			res.Unremoved++
			if res.firstRemoveErr == "" {
				res.firstRemoveErr = err.Error()
			}
			continue
		}
		// Renamed aside = gone from every listing, so it counts as removed
		// even when the delete below it failed: leaving it out would make a
		// copy vanish from the page with a count that does not include it.
		res.Pruned = append(res.Pruned, name)
		if err != nil {
			// Still a failure: the disk it holds is not back, and the record
			// says so until a cycle deletes the leftover (the sweep).
			slog.Warn("baseline prune: snapshot removed from the listing, but its files could not all be deleted yet; the disk is reclaimed next cycle",
				"snapshot", name, "error", err)
			res.Undeleted++
			if res.firstDeleteErr == "" {
				res.firstDeleteErr = err.Error()
			}
			continue
		}
		slog.Info("baseline prune: removed redundant local snapshot",
			"snapshot", name, "bytes", size, "reason", why)
		res.ReclaimedBytes += size
	}
	if !opts.DryRun && len(res.Pruned) > 0 {
		if err := writeRecord(opts.LocalDir, LastPrune{At: now, Removed: len(res.Pruned)}); err != nil {
			// The prune happened; only the record of it failed. Loud, because
			// that record is what tells the operator why copies are gone. The
			// PREVIOUS record is removed too: left in place it would show an
			// older date and count as the latest, which is worse than none.
			slog.Error("baseline prune: snapshots were removed but the record of it could not be written; the page will not say why they are gone",
				"dir", opts.LocalDir, "removed", len(res.Pruned), "error", err)
			if rerr := removeRecord(filepath.Join(opts.LocalDir, LastPruneFile)); rerr != nil && !os.IsNotExist(rerr) {
				slog.Error("baseline prune: the previous prune record could not be removed either; the page may show an older prune as the latest",
					"dir", opts.LocalDir, "error", rerr)
			}
		}
	}
	return res
}

// planPruneKeepNewest is the local-only decision (#1681): planPrune's order
// with the newest-N floor in place of the durability check. No IO.
func planPruneKeepNewest(snaps []localSnapshot, keepers, newest map[string]bool, retain, minAge time.Duration, now time.Time) ([]string, PruneResult) {
	var prune []string
	var res PruneResult
	for _, s := range snaps {
		switch {
		case !s.complete:
			res.KeptIncomplete++
		case s.unreadable:
			res.KeptUnreadable++
		case keepers[s.name]:
			res.KeptKeeper++
		case newest[s.name]:
			// One of the newest N: the only copies there are, never fewer.
			res.KeptNewest++
		case now.Sub(s.ts) < minAge || (retain > 0 && now.Sub(s.ts) < retain):
			res.KeptRecent++
		default:
			prune = append(prune, s.name)
		}
	}
	return prune, res
}

// computeNewestN returns, for EVERY table, the newest n snapshots holding it
// that count as copies: complete, readable (an unreadable one lists no
// tables) and dated at or before now. The union is kept. Counting per table
// rather than per snapshot is what stops runs over a few tables from using up
// the places of the tables they leave out: three runs over one table would
// otherwise push out an older full snapshot and leave every other table with
// a single copy. For snapshots that all hold every table the two are the
// same thing, the newest n. Snapshots that are not copies are kept (or not)
// by their own rules and never take a place.
func computeNewestN(snaps []localSnapshot, n int, now time.Time) map[string]bool {
	byTable := map[string][]localSnapshot{}
	for _, s := range snaps {
		// Only complete, readable snapshots list tables (enumerate leaves
		// s.tables empty otherwise), so they are the only ones counted here.
		if s.ts.After(now) {
			continue
		}
		for _, tbl := range s.tables {
			byTable[tbl] = append(byTable[tbl], s)
		}
	}
	out := map[string]bool{}
	for _, list := range byTable {
		sort.Slice(list, func(i, j int) bool { return list[i].ts.After(list[j].ts) })
		for i := 0; i < len(list) && i < n; i++ {
			out[list[i].name] = true
		}
	}
	return out
}

// planPrune is the pure prune decision: given the enumerated snapshots, the
// keeper set, and the confirmed-durable set, it returns the names to prune and a
// per-reason tally of what was kept. No IO — every invariant lives here so a unit
// test pins it without a filesystem or S3.
func planPrune(snaps []localSnapshot, keepers, durable map[string]bool, retain, minAge time.Duration, now time.Time) ([]string, PruneResult) {
	var prune []string
	var res PruneResult
	for _, s := range snaps {
		switch {
		case !s.complete:
			// _INCOMPLETE: an in-progress write or a partial run resumable via --retry.
			res.KeptIncomplete++
		case s.unreadable:
			// Could not enumerate the directory → cannot prove it is redundant.
			// Force-keep (reconstruct can still os.Stat its table files).
			res.KeptUnreadable++
		case keepers[s.name]:
			// Newest snapshot for some table — FindBaseline's target.
			res.KeptKeeper++
		case now.Sub(s.ts) < retain || now.Sub(s.ts) < minAge:
			res.KeptRecent++
		case !durable[s.name]:
			// No confirmed durable S3 copy → never delete the only copy.
			res.KeptNotDurable++
		default:
			prune = append(prune, s.name)
		}
	}
	return prune, res
}

// localSnapshot is one snapshot directory under a baseline root, classified for
// pruning. tables is populated only for complete snapshots ("schema/table").
// unreadable is set when a complete snapshot's directory could not be fully
// enumerated; such a snapshot is force-kept (we cannot prove it is redundant).
type localSnapshot struct {
	name       string
	ts         time.Time
	complete   bool
	tables     []string
	unreadable bool
	// readErr is why an unreadable snapshot could not be listed: the path
	// and the error, for the failure record.
	readErr string
}

// enumerateLocalSnapshots lists the snapshot directories under dir. Entries whose
// name does not parse as a baseline timestamp are skipped (this also excludes any
// ".<ts>.pruning" staging dirs). A missing dir is "nothing to prune", not an
// error — the daemon may run before any baseline has been created.
func enumerateLocalSnapshots(dir string) ([]localSnapshot, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read baseline directory %q: %w", dir, err)
	}
	var out []localSnapshot
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		ts, ok := parseBaselineDirTimestamp(e.Name())
		if !ok {
			continue
		}
		snapDir := filepath.Join(dir, e.Name())
		s := localSnapshot{name: e.Name(), ts: ts, complete: SnapshotComplete(snapDir)}
		if s.complete {
			tables, readErr := listSnapshotTables(snapDir)
			s.tables = tables
			s.unreadable = readErr != ""
			s.readErr = readErr
		}
		out = append(out, s)
	}
	return out, nil
}

// listSnapshotTables returns the "schema/table" pairs present in a snapshot
// directory, mirroring the <timestamp>/<schema>/<table>.parquet layout that
// DiscoverBaselines and reconstruct.ListBaselines both walk. ok is false when the
// directory (or a schema subdir) could not be listed — a transient os.ReadDir
// failure (fd exhaustion on a busy daemon, an NFS/permission blip).
//
// This is the fail-safe seam for the prune (the sibling LISTING walks Warn-and
// -skip because under-reporting a listing is harmless). reconstruct selects a
// table's baseline by os.Stat-ing the exact <schema>/<table>.parquet path, which
// needs only search permission and survives a listing failure — so a snapshot we
// CANNOT enumerate may still be the newest readable copy of a table. Reporting
// such a snapshot as "contains no tables" would drop it from every table's keeper
// set and make it prunable: a delete of data reconstruct can still read. So on
// any enumeration failure we return ok=false, and the caller force-keeps the
// snapshot rather than risk deleting a usable baseline. readErr is "" when the
// listing worked, else the path and error, for the failure record.
func listSnapshotTables(snapDir string) (tables []string, readErr string) {
	dbDirs, err := readDir(snapDir)
	if err != nil {
		slog.Warn("baseline prune: unreadable snapshot directory; keeping it (cannot prove it is redundant)",
			"path", snapDir, "error", err)
		return nil, pathErr(snapDir, err)
	}
	for _, dbDir := range dbDirs {
		if !dbDir.IsDir() {
			continue
		}
		schemaDir := filepath.Join(snapDir, dbDir.Name())
		files, err := readDir(schemaDir)
		if err != nil {
			slog.Warn("baseline prune: unreadable schema directory; keeping the snapshot",
				"path", schemaDir, "error", err)
			return nil, pathErr(schemaDir, err) // a table here could be this snapshot's unique copy
		}
		for _, f := range files {
			if f.IsDir() || !strings.HasSuffix(f.Name(), ".parquet") {
				continue
			}
			tables = append(tables, dbDir.Name()+"/"+strings.TrimSuffix(f.Name(), ".parquet"))
		}
	}
	return tables, ""
}

// computeKeepers returns the set of snapshot directory names that must never be
// pruned to keep Time-travel intact: for every table, the newest COMPLETE
// snapshot AT-OR-BEFORE now that contains it. This is exactly what
// reconstruct.findBaselineLocal selects for an `at=now` reconstruct (newest
// complete snapshot containing the table, filtered to `!t.After(at)`) — deleting
// any member would make at least one table's present-time reconstruct return
// ErrNoBaseline.
//
// The `now` filter matters: a future-dated snapshot (clock skew on the dump host,
// or an explicit --timestamp) is invisible to findBaselineLocal for at=now, so it
// must NOT be allowed to shadow the real present keeper here — otherwise the
// genuine newest-at-or-before-now snapshot would look non-keeper and get pruned,
// stranding the table (the future snapshot itself is kept by the recency floor).
func computeKeepers(snaps []localSnapshot, now time.Time) map[string]bool {
	newestName := make(map[string]string)  // table → snapshot dir name
	newestTS := make(map[string]time.Time) // table → snapshot timestamp
	for _, s := range snaps {
		if !s.complete || s.ts.After(now) {
			continue
		}
		for _, tbl := range s.tables {
			if cur, ok := newestTS[tbl]; !ok || s.ts.After(cur) {
				newestTS[tbl] = s.ts
				newestName[tbl] = s.name
			}
		}
	}
	keepers := make(map[string]bool, len(newestName))
	for _, name := range newestName {
		keepers[name] = true
	}
	return keepers
}

// keepPointerTarget adds the snapshot <root>/current names to keepers, so
// retention can never leave the pointer dangling.
//
// Usually redundant: the pointer follows the newest complete snapshot, which is
// already the keeper for every table it holds. It stops being redundant exactly
// when publication FAILED -- a read-only root, a `current` an operator replaced
// with a real directory -- because the pointer then lags behind the newest
// snapshot, and a lagging snapshot whose tables all appear in a newer one is
// prunable. Deleting it would turn a quiet staleness into every followed views
// file failing at once, on a deployment where nothing else had gone wrong.
func keepPointerTarget(dir string, keepers map[string]bool) {
	if name, ok := ResolveCurrentPointer(dir); ok {
		keepers[name] = true
	}
}

// renameAside and removeAll are os.Rename and os.RemoveAll, indirected only so
// a test can fail either half of removeSnapshot deterministically (a chmod
// cannot, under a root-running CI).
var (
	renameAside = os.Rename
	removeAll   = os.RemoveAll
)

// removeSnapshot deletes a snapshot directory atomically-then-lazily: it renames
// <dir>/<name> to <dir>/.<name>.pruning (an atomic same-filesystem rename that
// instantly hides the tree from discovery), then RemoveAll's the staged path. If
// the rename succeeds but RemoveAll partially fails, the leftover ".pruning" dir
// is harmless (invisible to readers) and swept next cycle.
//
// staged reports whether the rename happened, which is what decides whether
// the snapshot left the listing; err may still be set when it did.
func removeSnapshot(dir, name string) (staged bool, err error) {
	src := filepath.Join(dir, name)
	stagedPath := filepath.Join(dir, "."+name+pruningSuffix)
	if err := renameAside(src, stagedPath); err != nil {
		return false, fmt.Errorf("stage snapshot %q for removal: %w", name, err)
	}
	if err := removeAll(stagedPath); err != nil {
		return true, fmt.Errorf("remove staged snapshot %q: %w", stagedPath, err)
	}
	return true, nil
}

// sweepPruningLeftovers removes ".<ts>.pruning" staging directories left by a
// crashed prune or a delete that failed. Failures are logged and returned
// (how many, and the first error): not fatal to the prune, but a failure of
// it, because the disk those leftovers hold is not back.
func sweepPruningLeftovers(dir string) (failures int, firstErr string) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0, "" // the enumeration right after reports this
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		n := e.Name()
		if strings.HasPrefix(n, ".") && strings.HasSuffix(n, pruningSuffix) {
			p := filepath.Join(dir, n)
			if err := removeAll(p); err != nil {
				slog.Warn("baseline prune: could not sweep leftover staging dir", "path", p, "error", err)
				failures++
				if firstErr == "" {
					firstErr = pathErr(p, err)
				}
			}
		}
	}
	return failures, firstErr
}
