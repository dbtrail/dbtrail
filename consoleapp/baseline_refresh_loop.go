package consoleapp

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/cliutil"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// refreshRequest is one server's periodic baseline refresh.
type refreshRequest struct {
	ServerID    string
	ServerName  string
	IndexDSN    string
	BaselineDir string
	// CarryForwardUnchanged is the EFFECTIVE setting for this cycle: a console
	// override if one is saved, else the daemon's own flag. Resolved per cycle
	// rather than at boot so a change made in the settings panel takes effect
	// on the next tick, the same way a rotation override does.
	CarryForwardUnchanged bool
	// Trigger is stamped onto the history record: BaselineRunTriggerScheduled
	// when the per-server backup schedule started this fold, empty for the
	// daemon-wide interval loop.
	Trigger string
	// BaselineS3, when set, is the destination the finished snapshot is
	// uploaded to, and the source the fold reads its previous snapshot from.
	//
	// This field is what decides whether this loop uploads at all, and that is
	// deliberate (#1539). The daemon-wide --baseline-refresh-interval leaves it
	// EMPTY and keeps the original behaviour, because that flag names no
	// destination and a loop uploading on the operator's behalf would be
	// deciding something it was not told. The per-server schedule sets it from
	// the server's own configured backup destination, which IS a destination
	// the operator named. The gate is therefore data rather than a mode flag:
	// there is no way to reach the upload without a destination to reach.
	BaselineS3 string
	// FoldSource, when set, is where THIS run reads its previous snapshot
	// from, resolved by resolveFoldSource just before the fold. Empty means
	// the standing rule (baselineFoldSource): the bucket when the server has
	// one, else the local directory. It is per run on purpose: whether the
	// local copy IS the bucket's newest snapshot is only true at the moment
	// it was checked.
	FoldSource string
}

// TriggerRefresh starts a periodic baseline refresh for a server, sharing the
// supervisor's single-flight with the manual dump path.
//
// The shared lock is the point, not an implementation detail: a refresh folds
// the newest snapshot forward while a dump writes a new one, and letting them
// overlap on the same server would have the refresh anchored on a snapshot that
// is being written underneath it. ErrBaselineRunning here means "something else
// is already producing this server's baseline" — the loop skips this tick and
// tries again at the next one, which is exactly right for a periodic job.
func (s *baselineSupervisor) TriggerRefresh(req refreshRequest, interval time.Duration) error {
	s.mu.Lock()
	if s.busyLocked(req.ServerID) {
		s.mu.Unlock()
		return console.ErrBaselineRunning
	}
	at := time.Now().UTC()
	s.refreshes[req.ServerID] = &console.BaselineStatus{State: "running", Since: nowStamp(),
		At: at.Format(time.RFC3339)}
	s.mu.Unlock()

	slog.Info("baseline refresh: starting", "server", req.ServerName, "id", req.ServerID)
	go s.runRefresh(req, at, interval)
	return nil
}

// RefreshStatus reports the last periodic refresh for a server.
func (s *baselineSupervisor) RefreshStatus(serverID string) console.BaselineStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	if st, ok := s.refreshes[serverID]; ok {
		return *st
	}
	return console.BaselineStatus{State: "idle"}
}

// busyLocked reports whether any of the four baseline job kinds (dump,
// refresh, restore, sql export) is in flight for a server. Callers must
// hold s.mu.
func (s *baselineSupervisor) busyLocked(serverID string) bool {
	if st, ok := s.jobs[serverID]; ok && st.State == "running" {
		return true
	}
	if st, ok := s.refreshes[serverID]; ok && st.State == "running" {
		return true
	}
	if st, ok := s.restores[serverID]; ok && st.State == "running" {
		return true
	}
	if st, ok := s.exports[serverID]; ok && st.State == "running" {
		return true
	}
	return false
}

func (s *baselineSupervisor) runRefresh(req refreshRequest, at time.Time, interval time.Duration) {
	// The cycle's own recover in runBaselineRefreshCycle cannot reach here: it
	// sits on the near side of the `go` in TriggerRefresh, so it guards the
	// dispatch and not the fold. See recoverBaselineJob.
	defer s.recoverBaselineJob(baselineJobRefresh, req.ServerID, req.ServerName)
	started := time.Now().UTC()
	// Separate capture for the ELAPSED time, and the duplication is not
	// redundant: t.UTC() strips the monotonic reading, so time.Since(started)
	// would subtract wall clocks. A daemon whose folds run for minutes to
	// hours is exactly where an NTP step lands mid-measurement, and it would
	// move the number this change exists to get right, in either direction.
	// started stays for the RFC3339 stamp, which wants the wall clock.
	elapsed := time.Now()
	// Every cycle, not only a failing one: a staging directory a killed daemon
	// left behind is invisible to every listing, so nothing else will ever
	// mention it, and a server whose refusals stopped would keep it forever.
	sweepDiscardedSnapshots(req)
	// Asked BEFORE the fold, and it has to be: the question is whether the
	// snapshot directory holds anything this run did not write, and once the
	// fold has run its own files are in there too. See claimSnapshotDir.
	unclaimed := claimSnapshotDir(refreshSnapshotDir(req, at))
	req.FoldSource = resolveFoldSource(s.ctx, req)
	tables, refused, reuse, err := s.executeRefresh(req, at)
	// Publishing is not finished until the snapshot is where this server's
	// backups live. A fold that wrote a perfect local snapshot for a server
	// whose destination is S3 has produced a copy on one box, which is not
	// what "the backups go to S3" promises: it is outside retention (a prune
	// confirms the S3 copy), outside anything reading the bucket, and gone
	// with the host. Reporting that as published would be reporting a backup
	// the destination does not have.
	//
	// Ordered AFTER the fold and gated on its success: there is nothing to
	// upload otherwise, and an incomplete snapshot must never reach the
	// destination. baseline.Upload writes the _INCOMPLETE marker first and
	// _SUCCESS last, so a crash mid-upload leaves the remote copy excluded from
	// discovery rather than half-visible.
	var uploaded int
	if err == nil && req.BaselineS3 != "" {
		uploaded, err = uploadRefreshedSnapshot(s.ctx, req, at)
	}
	// Measured HERE, on the far side of the `go` in TriggerRefresh, because
	// this is where the fold actually happens. Timing the dispatch loop
	// instead measures how long it takes to spawn a goroutine, which is
	// microseconds no matter what the refresh costs.
	took := time.Since(elapsed)
	s.recordRun(req.ServerID, req.ServerName, foldRunCounts(console.BaselineRunRecord{
		Kind: console.BaselineRunRefresh, Trigger: req.Trigger, StartedAt: started.Format(time.RFC3339),
		SnapshotTime: publishedSnapshotTime(at, err),
		// Zero means "nothing was sent" for a server with no destination AND
		// "these files reached the bucket" otherwise, so the count is what
		// makes a successful upload visible at all: without it the only
		// evidence the snapshot got there is the absence of a failure line.
		Uploaded: uploaded,
	}, tables, refused, reuse), err)
	if err != nil {
		// Reported and reclaimed OUTSIDE s.mu. Deleting a directory is
		// filesystem work of unbounded duration, and s.mu is the lock every
		// baseline job takes to start: holding it across a delete would make a
		// slow disk block the next dump, restore or export.
		reportRefusedRefresh(req, at, refused, unclaimed, err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.refreshes[req.ServerID]
	if st == nil { // defensive; never cleared under lock
		st = &console.BaselineStatus{}
		s.refreshes[req.ServerID] = st
	}
	applyFoldStatus(st, tables, refused, reuse, err)
	if err != nil {
		// The refusal itself was already reported above, before this lock was
		// taken. What happens HERE is deliberately nothing: no duration report.
		// reportRefreshDuration's advice is "raise the interval, or refresh
		// fewer tables", which is the wrong remediation for a run that
		// published nothing: a capture gap, a schema change or a shutdown
		// mid-fold are not fixed by scheduling. The fan-out runs every table to
		// completion before it reports a refusal, so a refusal costs about what
		// a success costs and WOULD trip the overrun threshold, printing tuning
		// advice above the actual cause.
		return
	}
	pub := []any{"server", req.ServerName, "id", req.ServerID, "tables", tables,
		"reused", reuse.reused, "reused_copied", reuse.copied}
	// A reused count of zero reads as "nothing happened to be unchanged", which
	// is indistinguishable from "this path cannot reuse anything" — and on an
	// S3 source it is always the second (carryForwardEligible refuses any
	// s3:// previous snapshot, since carrying a file forward means hard-linking
	// it). The operator turned the setting on and the console said "Unchanged
	// tables will be reused", so a count that CANNOT be nonzero has to say so.
	if req.CarryForwardUnchanged && strings.HasPrefix(baselineFoldSource(req), "s3://") {
		pub = append(pub, "reuse_unchanged", "not applicable: the previous backup is read from S3, and reusing a file means linking it on disk")
	}
	slog.Info("baseline refresh: published", pub...)
	reportRefreshDuration(req.ServerName, interval, took)
}

// baselineFoldSource is where this request's fold reads the PREVIOUS snapshot
// from. Mirrors console.BaselineFoldSource, on the loop's own request type,
// unless resolveFoldSource already settled it for this run.
func baselineFoldSource(req refreshRequest) string {
	if req.FoldSource != "" {
		return req.FoldSource
	}
	if req.BaselineS3 != "" {
		return req.BaselineS3
	}
	return req.BaselineDir
}

// resolveFoldSource decides where one run reads its previous snapshot from.
//
// The standing rule (#1539) reads the bucket whenever the server has one,
// because the local directory may hold less than the bucket: what this daemon
// folded since it started, minus what retention reclaimed. That rule has a
// cost on a server with BOTH: carrying an unchanged table forward means
// hard-linking its previous file (carryForwardEligible refuses an s3://
// source), so every table was rewritten and re-uploaded at every slot even
// when the local directory held the very snapshot the bucket had (#1626), and
// after the first fold it always does: the fold writes locally and uploads,
// and retention never removes the newest snapshot.
//
// So: when the local directory's newest complete snapshot is the SAME one the
// bucket reports (same instant) and holds every table the bucket's copy
// holds, read the local copy. Anything else (no local copy, an older one, a
// listing that failed, a local copy missing a table) keeps the bucket, so a
// stale local directory can never be folded from — the hazard the standing
// rule exists to avoid. The upload is unaffected either way.
func resolveFoldSource(ctx context.Context, req refreshRequest) string {
	standing := baselineFoldSource(req)
	if req.FoldSource != "" || req.BaselineS3 == "" || req.BaselineDir == "" {
		return standing
	}
	// The local listing goes first: it is a directory read, and an empty or
	// unreadable directory settles the answer without a bucket listing, which
	// is two globs over S3. The #1539 shape this does not apply to (a bucket
	// server whose directory holds nothing yet) then costs no extra S3 work.
	local, err := listBaselines(ctx, req.BaselineDir)
	if err != nil || len(local) == 0 {
		return standing
	}
	remote, err := listBaselines(ctx, req.BaselineS3)
	if err != nil || len(remote) == 0 {
		return standing
	}
	remoteAt, remoteTables := newestSnapshotOf(remote)
	localAt, localTables := newestSnapshotOf(local)
	if !localAt.Equal(remoteAt) {
		slog.Debug("baseline refresh: local copy is not the bucket's newest snapshot, reading the bucket",
			"server", req.ServerName, "local", localAt.UTC().Format(time.RFC3339), "bucket", remoteAt.UTC().Format(time.RFC3339))
		return standing
	}
	for t := range remoteTables {
		if _, ok := localTables[t]; !ok {
			slog.Debug("baseline refresh: local copy of the newest snapshot is missing a table, reading the bucket",
				"server", req.ServerName, "table", t)
			return standing
		}
	}
	slog.Info("baseline refresh: the local copy is the bucket's newest snapshot, reading it instead of the bucket",
		"server", req.ServerName, "snapshot", localAt.UTC().Format(time.RFC3339), "dir", req.BaselineDir)
	return req.BaselineDir
}

// newestSnapshotOf returns the newest snapshot time in a ListBaselines result
// (newest first) and the schema.table set of that snapshot.
func newestSnapshotOf(files []reconstruct.BaselineFile) (time.Time, map[string]struct{}) {
	newest := files[0].SnapshotTime
	tables := map[string]struct{}{}
	for _, f := range files {
		if f.SnapshotTime.Equal(newest) {
			tables[f.Schema+"."+f.Table] = struct{}{}
		}
	}
	return newest, tables
}

// errSnapshotNotUploaded marks the ONE failure that leaves a complete snapshot
// behind: the fold finished and marked it, and only sending it to the backup
// destination failed.
//
// It exists because the scheduled watcher takes a FULL backup whenever an
// update fails (backup_schedule_loop.go, fallBack), on the reasoning that the
// update produced nothing so a backup is still owed. That reasoning does not
// survive this failure: the update DID produce a snapshot, and a full backup
// would have to clear the very upload gate that just refused it. Falling back
// here would answer one S3 permission error with a full lock-and-read of
// production that publishes nothing new, which is the exact cost #1539 exists
// to remove.
//
// A sentinel, not a message match: the verdict must not depend on wording that
// an edit to a string can change.
var errSnapshotNotUploaded = errors.New("the snapshot was not sent to the backup destination")

// foldPublished reports whether a finished fold left a complete snapshot in the
// server's local directory, which is true both when the run fully succeeded and
// when only the upload failed. Callers that ask "is a backup owed?" must use
// this rather than err == nil.
func foldPublished(err error) bool {
	return err == nil || errors.Is(err, errSnapshotNotUploaded)
}

// uploadRefreshedSnapshot copies the snapshot this cycle just published to the
// server's configured S3 destination.
//
// The URL is the destination root joined with the snapshot's OWN directory
// name, and baseline.Upload builds its keys relative to the directory it is
// given. Passing the destination root and the local ROOT instead would upload
// every snapshot on disk on every cycle; passing the destination root and the
// SNAPSHOT directory would drop the timestamp level and write one table's
// Parquet where the snapshot directory belongs, which discovery reads as a
// snapshot with no tables.
func uploadRefreshedSnapshot(ctx context.Context, req refreshRequest, at time.Time) (int, error) {
	name := reconstruct.SnapshotDirName(at)
	dest := strings.TrimSuffix(req.BaselineS3, "/") + "/" + name
	n, err := uploadSnapshot(ctx, refreshSnapshotDir(req, at), dest, "", false)
	if err != nil {
		// Names the local path on purpose: the snapshot itself is intact and
		// complete, and an operator reading this needs to know the run's work
		// still exists rather than that a backup was lost.
		return 0, fmt.Errorf("%w: it was written to %s but could not be uploaded to %s. The next update folds a NEW "+
			"snapshot rather than re-sending this one; a full backup uploads the whole directory, so one of those "+
			"sweeps it up: %w",
			errSnapshotNotUploaded, refreshSnapshotDir(req, at), dest, err)
	}
	return n, nil
}

// refreshSnapshotDir names the directory one refresh cycle folds into: the
// snapshot directory reconstruct derives from the instant the cycle targets.
//
// The loop knows both halves, which is why the cleanup can live here at all.
// Deriving it a second time is also what makes a mistake harmless rather than
// dangerous: DiscardUnpublishedSnapshot refuses any directory that does not
// carry the incomplete marker, so a path that does not match the fold's own
// deletes nothing.
func refreshSnapshotDir(req refreshRequest, at time.Time) string {
	return filepath.Join(req.BaselineDir, reconstruct.SnapshotDirName(at))
}

// claimSnapshotDir establishes, before the fold starts, that the snapshot
// directory holds nothing this run did not put there. It returns "" when the
// directory is this run's to reclaim, or the reason it is not.
//
// A refresh runs unattended every interval, so the directory it is about to
// write into is normally absent. It can legitimately hold the incomplete marker
// a previous failed run of the same instant left, which is why that one entry
// does not disqualify it (the fold applies the same rule before it writes).
// Anything else means another writer got there first, and a same-second
// collision with a `bintrail baseline` run is the one case where a directory
// carrying an incomplete marker is somebody else's live output.
func claimSnapshotDir(dir string) string {
	vacant, err := reconstruct.SnapshotDirVacant(dir)
	switch {
	case err != nil:
		// Not swallowed and not duplicated: this same unreadable directory
		// makes the fold itself fail, and that failure is reported as the run's
		// own error on the line below. What this branch decides is only whether
		// the cleanup may run, and the answer is no, which is reported as the
		// reason the directory was kept.
		return fmt.Sprintf("the directory could not be read before the refresh started: %v", err)
	case !vacant:
		return "the directory already held files before this refresh started, so they are not all this run's"
	}
	return ""
}

// reportRefusedRefresh reports a refresh cycle that published nothing, and
// reclaims the partial snapshot the cycle wrote.
//
// Warn, never Error: a refusal is the fail-closed contract working, and the
// next tick retries. Nothing about the daemon is unhealthy.
//
// The message is kept byte-stable because operators grep and alert on it; what
// happened to the files is carried in the attributes.
func reportRefusedRefresh(req refreshRequest, at time.Time, refused int, unclaimed string, err error) {
	args := []any{"server", req.ServerName, "id", req.ServerID, "refused", refused, "error", err}
	args = append(args, reclaimPartialSnapshot(refreshSnapshotDir(req, at), refused, unclaimed)...)
	if errors.Is(err, errSnapshotNotUploaded) {
		// A different headline, because operators alert on this one. Saying
		// "published nothing" over a finished snapshot sends them looking for
		// a fold problem that did not happen, and the remedy (the credentials
		// or the bucket policy) is not where that message points.
		slog.Warn("baseline refresh: the snapshot was written but not sent to the backup destination", args...)
		return
	}
	slog.Warn("baseline refresh: published nothing", args...)
}

// reclaimPartialSnapshot deletes the partial snapshot a refused cycle wrote and
// returns the attributes describing what it did, for the caller's log line.
//
// This is the whole point of #1473. A refresh that refuses still folds every
// table that CAN fold, and each one writes its Parquet into the snapshot
// directory as it finishes; only the completeness marker at the end says the
// result is unusable. On a server where one table carries a permanent capture
// gap, every cycle therefore leaves a near-complete snapshot that discovery
// correctly ignores and that retention cannot reclaim, because a prune needs a
// confirmed S3 copy and a refused cycle never reaches the upload (which is
// gated on the fold succeeding). At a one-hour interval that is 24 a day; at
// the one-minute floor it is 1440.
//
// Only the LOOP does this. The CLI and the operator-triggered point-in-time
// restore share the same fold and keep their fragments: someone who typed a
// command is watching its output and may want to look at what came out. An
// unattended job that will repeat in a minute is the case where nobody will.
func reclaimPartialSnapshot(dir string, refused int, unclaimed string) []any {
	published := snapshotPublished(dir)
	if reason := keepPartialSnapshotBecause(refused, unclaimed, holdsTableData(dir), published); reason != "" {
		if !dirExists(dir) {
			// The run failed before it created the directory (no snapshot to
			// fold, an unreachable index). Naming a path that is not there
			// would send an operator looking for it.
			return nil
		}
		if unclaimed != "" {
			// A DIFFERENT key, because this directory may not be a partial
			// snapshot at all: the reason it was refused is that somebody else's
			// files are in it, and one of the shapes that produces is a real
			// backup published into the same second. Calling that
			// partial_snapshot invites a cleanup script to delete it.
			return []any{"unclaimed_dir", dir, "kept_because", reason}
		}
		if published {
			// Its own key for the same reason, and a stronger one (#1539):
			// this directory is a FINISHED, marked snapshot whose upload
			// failed, which makes it the operator's whole remaining result. A
			// cleanup script keyed on partial_snapshot would delete exactly
			// the thing this branch exists to preserve.
			return []any{"published_snapshot", dir, "kept_because", reason}
		}
		return []any{"partial_snapshot", dir, "kept_because", reason}
	}
	discarded, err := reconstruct.DiscardUnpublishedSnapshot(dir)
	switch {
	case discarded && err != nil:
		// Renamed out of every discovery path but not fully deleted, so the disk
		// is leaking. Its own line, at Error, because this Warn is the expected
		// one an operator with a permanent capture gap has already learned to
		// skip past, and burying a leak in it is how the leak is never seen.
		slog.Error("baseline refresh: could not delete the partial snapshot it moved aside, so that disk is not "+
			"reclaimed. Nothing can read it and the next cycle sweeps it, but if this repeats, delete it by hand "+
			"and check the filesystem.", "dir", dir, "error", err)
		return []any{"removed_partial_snapshot", dir, "cleanup_error", err}
	case discarded:
		return []any{"removed_partial_snapshot", dir}
	case errors.Is(err, reconstruct.ErrSnapshotNotDiscardable):
		// The guard declining, which is it working. An attribute on the refusal
		// line is the right weight for that.
		return []any{"partial_snapshot", dir, "kept_because", err.Error()}
	case err != nil:
		// Tried and could not. Same class as the delete failure above: the
		// reclaim cannot run for this directory, so it accumulates.
		slog.Error("baseline refresh: could not reclaim the partial snapshot this run left behind, so that disk "+
			"is not reclaimed and the directory will accumulate at every interval.", "dir", dir, "error", err)
		return []any{"partial_snapshot", dir, "kept_because", err.Error()}
	default:
		return nil
	}
}

// sweepDiscardedSnapshots clears staging directories a killed daemon left in
// this server's baseline root.
//
// Silent when there is nothing to do, which is every cycle on a healthy host.
// It speaks only when it actually reclaimed something, because that means a
// previous delete did not finish and the operator has no other way to learn it
// happened: a staging directory is skipped by every listing by design.
func sweepDiscardedSnapshots(req refreshRequest) {
	removed, err := reconstruct.SweepDiscardedSnapshots(req.BaselineDir)
	if err != nil {
		slog.Warn("baseline refresh: could not clear a leftover staging directory from an interrupted cleanup; "+
			"it holds disk that nothing else will reclaim or report", "server", req.ServerName, "error", err)
	}
	if removed > 0 {
		slog.Info("baseline refresh: cleared staging directories left by an interrupted cleanup",
			"server", req.ServerName, "dirs", removed, "baseline_dir", req.BaselineDir)
	}
}

// holdsTableData reports whether the snapshot directory holds anything beyond
// the incomplete marker. It is the difference between a fold that wrote tables
// and one that never got that far, and it decides whether the refused == 0
// guard has anything to protect.
func holdsTableData(dir string) bool {
	vacant, err := reconstruct.SnapshotDirVacant(dir)
	if err != nil {
		// Cannot see inside, so assume there is something worth keeping. The
		// conservative answer is the one that keeps the directory.
		return true
	}
	return !vacant
}

// keepPartialSnapshotBecause states why a refused cycle's snapshot directory
// must be left alone, or "" when it may be reclaimed.
//
// unclaimed comes first because it is the earlier and more specific fact: a
// directory that already held files is refused by the fold at its own leftovers
// check, which returns before a single table folds, so it also arrives here
// with refused == 0. Reporting that as "the fold reported no table failure"
// would name the wrong reason for the right decision.
//
// refused == 0 is the guard that keeps a COMPLETE snapshot out of the delete.
// foldOutcome sets refused to the number of tables that failed, so the case
// this exists for (one table with a permanent capture gap out of twelve)
// arrives with refused > 0, while three paths fail a run whose tables all
// folded: the integrity manifest could not be written, the _SUCCESS marker
// could not be written, or the daemon was cancelled after the last table
// finished. In all three the bytes on disk are a whole snapshot that only
// failed to be MARKED, and deleting one would destroy work that is complete.
//
// holdsData is what stops that guard from protecting nothing. A run that failed
// BEFORE the first table folded (an unreachable index, a missing schema
// snapshot, archive discovery refusing) has already created the directory and
// stamped the incomplete marker, and it also arrives with refused == 0, so
// without this the whole failure family accumulated one empty directory per
// interval: the same symptom, minus the bytes, plus a "skipping incomplete
// snapshot" warning on every later listing. A directory holding nothing but the
// marker cannot be a complete snapshot, so the guard has nothing to protect and
// stands down.
//
// The residual cost is a shutdown that lands BETWEEN tables. A table already
// folding propagates the cancellation as an ordinary table error (carryForward
// returns ctx.Err(); an in-flight fetch returns context.Canceled), so a
// shutdown mid-fold usually arrives with refused > 0 and IS reclaimed; it is
// only a cancellation observed while no table is in flight that returns with no
// failure recorded and keeps its fragment. Rare, bounded by restarts rather
// than by the interval, and the direction to be wrong in.
func keepPartialSnapshotBecause(refused int, unclaimed string, holdsData, published bool) string {
	if unclaimed != "" {
		return unclaimed
	}
	if published {
		// The only way to reach the refusal path with the completeness marker
		// already written is an upload that failed after a fold that did not
		// (#1539). Saying so beats the heuristic below, which would report a
		// finished, marked snapshot as one that "may be complete" and "failed
		// to be marked" — both halves false, on the one shape where the local
		// copy is the operator's whole remaining result.
		return "the fold finished and marked the snapshot; only sending it to the backup destination failed"
	}
	if refused == 0 && holdsData {
		return "the fold reported no table failure, so what is on disk may be a complete snapshot that only " +
			"failed to be marked"
	}
	return ""
}

// snapshotPublished reports whether dir carries the completeness marker the
// fold writes last.
//
// Deliberately NOT baseline.SnapshotComplete, which answers true for a
// directory carrying NEITHER marker (legacy snapshots are complete by
// default). Here the question is whether THIS run finished and marked it, and
// a markerless directory is the shape a killed daemon leaves, which the
// caller must keep for a different reason and under a different key.
func snapshotPublished(dir string) bool {
	_, err := os.Stat(filepath.Join(dir, baseline.SuccessMarker))
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		// Not "not published": an unreadable marker is a real IO answer, and
		// answering false would send a finished snapshot down the heuristic
		// below, which reports it as possibly-partial under the key a cleanup
		// script deletes. Same treatment as snapshotDirsWithSuccess, which
		// refuses the same swallow for the same reason.
		slog.Warn("baseline refresh: could not read the completeness marker, so the snapshot is kept rather than "+
			"judged", "dir", dir, "error", err)
		return true
	}
	return err == nil
}

// dirExists reports whether path is an existing directory.
func dirExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.IsDir()
}

// foldRunCounts writes a finished fold's counts onto its history record —
// applyFoldStatus's sibling for the DURABLE copy, and split out for the same
// reason stated on it below: both callers (runRefresh, runRestore) sit behind
// a `go` and a live fold, so zeroing CarriedCopied in either record literal
// compiled and passed the whole suite while the run-history note quietly went
// back to rendering copied reuses as disk savings (#1578).
func foldRunCounts(rec console.BaselineRunRecord, tables, refused int, reuse reuseTally) console.BaselineRunRecord {
	rec.Tables, rec.Refused = tables, refused
	rec.Carried, rec.CarriedCopied = reuse.reused, reuse.copied
	return rec
}

// applyFoldStatus writes a finished fold's outcome onto the status the console
// polls. Shared by the refresh and the restore, which had byte-identical copies
// of it.
//
// Split out for the reason the rest of this file keeps splitting things out:
// both callers sit behind a `go` and a live fold, so nothing at the unit tier
// could reach them, and dropping the reused count from either copy compiled and
// passed the whole suite. It is also the deduplication: two copies of a
// five-count assignment is exactly how one of them silently loses a field.
func applyFoldStatus(st *console.BaselineStatus, tables, refused int, reuse reuseTally, err error) {
	st.FinishedAt = nowStamp()
	st.Tables = tables
	st.Refused = refused
	st.Carried = reuse.reused
	st.CarriedCopied = reuse.copied
	// Set on BOTH branches, never left from a previous run: this is what the
	// scheduled watcher reads to decide whether a full backup is still owed,
	// and a stale true there is a skipped backup.
	st.Published = foldPublished(err)
	if err != nil {
		st.State = "failed"
		st.LastError = err.Error()
		return
	}
	st.State = "succeeded"
	st.LastError = ""
}

// executeRefresh folds the newest snapshot forward. Returns the table count and
// the number of tables that refused.
//
// RESOURCE POSTURE — read before changing. Every DuckDB budget here is left at
// its zero value on purpose, which resolves to duckdbutil.DefaultTuning (2
// threads / 4 GB) and the container-safe archive fetcher. This is a long-lived
// daemon that is also streaming replication and serving a console; --ultrafast
// exists for offline commands that own the machine (#510), and letting a
// background refresh self-tune to ~80% of host RAM would starve the capture path
// it depends on. If this ever needs to go faster, it needs its own bounded knob,
// not the offline one.
//
// The zero value is NOT uniformly the safe choice on that struct, which is the
// trap this note used to leave open. It is safe for the DuckDB budgets and the
// archive fetcher, where zero resolves to the container-safe default. It is the
// OPPOSITE for Parallelism (zero means runtime.NumCPU()) and for
// WarnEventThreshold (zero means the volume warning never fires). Those two are
// therefore set explicitly in refreshFoldConfig; see the constants above it.
func (s *baselineSupervisor) executeRefresh(req refreshRequest, at time.Time) (tables, refused int, reuse reuseTally, err error) {
	// Listed where the fold READS (refreshFoldConfig's BaselineSrc), not where
	// it writes. On an S3-backed server those differ, and listing the local
	// directory here would refuse with "no baseline snapshot" on exactly the
	// server the scheduler just picked this producer for: its previous
	// snapshots live in the bucket, and the local directory holds only what
	// this daemon has folded since it started.
	src := baselineFoldSource(req)
	tableList, err := newestSnapshotTables(s.ctx, src)
	if err != nil {
		return 0, 0, reuseTally{}, fmt.Errorf("list the snapshot to refresh: %w", err)
	}
	if len(tableList) == 0 {
		return 0, 0, reuseTally{}, fmt.Errorf("no baseline snapshot to refresh under %s", src)
	}
	return s.foldSnapshot(req, at, tableList)
}

// The bounded knobs EVERY in-daemon fold shares: the periodic refresh, the
// point-in-time restore, and the SQL export build. All three fold inside the
// process that is also capturing, so they get one posture rather than three
// opinions. Both are spelled out rather than left at zero because, unlike
// every other budget on FullTableConfig, their zero values mean the opposite
// of conservative.
const (
	// daemonFoldWarnEventThreshold is the same RAW value every CLI path ships
	// (internal/cli/reconstruct.go, cliapp/baseline_refresh.go, the hardcoded
	// one in internal/cli/drill.go, and the config init template in
	// cliapp/config.go). Zero DISABLES the warning outright: shouldWarnEvents is
	// `threshold > 0 && n > threshold`. Silence is backwards here, because the
	// operator who typed a command is watching its output and this job has
	// nobody reading it.
	//
	// Do NOT read "same raw value" as "warns at the same point per table". The
	// threshold reported is scaledEventThreshold(raw, effectiveParallelism),
	// so the per-table trigger here is 5M/2 = 2.5M against an attended run's
	// 5M/NumCPU. What the shared raw value actually equalizes is the TOTAL
	// concurrent event volume at which either warns, which is the quantity #842
	// scaling exists to hold steady and the one that tracks RAM. Note also that
	// effectiveParallelism clamps to len(Tables): a SINGLE-table refresh divides
	// by 1, so the full 5M applies there whatever this constant's sibling says.
	//
	// The bound below is what protects the process; this threshold is only what
	// tells someone it happened.
	daemonFoldWarnEventThreshold = 5_000_000

	// daemonFoldParallelism bounds how many tables fold concurrently. Zero means
	// runtime.NumCPU(), and peak resident memory is the SUM of the
	// concurrently-folding tables' change maps (the reason scaledEventThreshold
	// divides by parallelism at all, #842), each holding one entry per distinct
	// touched primary key (#1107). Inheriting the core count therefore ties this
	// daemon's peak memory to the size of the host it happens to run on, inside
	// the process that is also capturing. Two lets a slow table overlap with the
	// next one without letting the peak track the hardware; lower it before
	// raising it.
	//
	// It is not only a memory knob: fulltable.go sizes the index connection
	// pool as SetMaxOpenConns(2 * Parallelism), so moving this also moves the
	// fold's share of the index server's connections (4 here, against 2*NumCPU
	// before). Anyone tuning it is tuning both.
	daemonFoldParallelism = 2

	// daemonFoldRemediation replaces the volume warning's default advice, which
	// names --at, --parallelism and --warn-event-threshold. bintrail-console
	// registers none of the three: its only persistent flags are --log-level and
	// --log-format, and these folds' budgets are the constants above. Telling an
	// operator to lower a flag their binary does not have is worse than saying
	// nothing, so this names what they CAN actually reach.
	daemonFoldRemediation = "shorten the window this fold covers: for the scheduled refresh, " +
		"lower --baseline-refresh-interval so each fold starts from a fresher backup; " +
		"for a restore or a SQL export, pick a moment closer to an existing backup"
)

// refreshFoldConfig is the configuration one refresh cycle folds with.
//
// Split out of foldSnapshot so the settings it carries are checkable without
// standing up an index and a baseline: this is the last hop of the chain that
// starts at a console toggle, and it was the only one nothing could observe.
func refreshFoldConfig(req refreshRequest, at time.Time, tableList []string) reconstruct.FullTableConfig {
	return reconstruct.FullTableConfig{
		IndexDSN: req.IndexDSN,
		// Read from the bucket when there is one, write to the filesystem
		// always. On an S3-backed server the previous snapshot may exist ONLY
		// in the bucket, so folding from the local directory would find nothing
		// to fold from; BaselineSrc takes an s3:// URL and FindBaseline
		// dispatches on the prefix. The one exception is resolved per run by
		// resolveFoldSource: a local copy that IS the bucket's newest snapshot
		// is read locally so unchanged tables can be hard-linked (#1626).
		// OutputDir cannot follow it: the Parquet writer needs a real
		// directory, and the upload below is what moves the finished snapshot
		// to the destination.
		BaselineSrc:           baselineFoldSource(req),
		Tables:                tableList,
		At:                    at,
		OutputDir:             req.BaselineDir,
		OutputFormat:          reconstruct.OutputFormatParquet,
		CarryForwardUnchanged: req.CarryForwardUnchanged,
		Parallelism:           daemonFoldParallelism,
		WarnEventThreshold:    daemonFoldWarnEventThreshold,
		RemediationHint:       daemonFoldRemediation,
		// AllowGaps stays FALSE. An unattended job must never publish a
		// knowingly-incomplete baseline: accepting a permanent capture loss is a
		// decision with consequences for every future reconstruct, and nobody is
		// watching this one to make it.
	}
}

// reuseTally counts one fold's carried tables, split by whether the bytes
// were actually shared (#1578). reused is every table published by reuse
// (the fold and the source read were saved either way); copied is the subset
// written as a full copy because the file could not be hard linked — no disk
// was saved for those, and a surface that renders "reused" as a disk saving
// must subtract them or it confirms a saving the daemon log denies.
type reuseTally struct {
	reused int
	copied int
}

// countReuse tallies the tables a fold published by reusing the previous
// snapshot's file.
//
// A separate function for the same reason refreshFoldConfig is one: this is the
// last hop of the reuse feature and the only evidence it produced anything, and
// inside foldSnapshot nothing could reach it without standing up an index and a
// baseline. Returning len(reports) here, or 0, compiles and passes every test
// that does not call this directly.
func countReuse(reports []*reconstruct.TableReport) (tally reuseTally) {
	for _, rep := range reports {
		if rep != nil && rep.CarriedForward {
			tally.reused++
			if !rep.CarriedByLink {
				tally.copied++
			}
		}
	}
	return tally
}

// foldSnapshot is the fold both the periodic refresh and the point-in-time
// restore share: reconstruct every table at `at` and publish the result as a
// new snapshot in the server's own baseline store, all-or-nothing.
//
// carried counts the tables published by reusing the previous snapshot's file.
// It is read out of the per-table reports rather than inferred from the
// setting, because asking for reuse is not getting it: a table with changes,
// with a capture gap, or on the S3 path is folded anyway.
func (s *baselineSupervisor) foldSnapshot(req refreshRequest, at time.Time, tableList []string) (tables, refused int, reuse reuseTally, err error) {
	reports, failures, runErr := foldTables(s.ctx, refreshFoldConfig(req, at, tableList))
	return foldOutcome(tableList, reports, failures, runErr)
}

// foldTables is reconstruct.ReconstructTablesDetailed behind a seam, shared by
// the refresh, the restore and the sql export — the three jobs whose work IS
// the fold.
//
// It exists because that call is otherwise the one thing in these job
// goroutines a unit test cannot reach: it needs a live index and a real
// baseline, and consoleapp has no fixture that stands those up (foldOutcome's
// doc states the same residual from the other side). Without the seam nothing
// below the `go` in each Trigger is drivable, which is exactly the gap #1472
// was: the panic guard on these goroutines would have had no test that reaches
// past the dispatch.
//
// Written by tests only, like checkMydumperPrivileges. Production never
// reassigns it, so the job goroutines only ever read it. A test that replaces
// it must not restore it until the job it started has reached a terminal
// state: the jobs run in their own goroutines, and restoring while one is
// still folding is a data race on this variable.
var foldTables = reconstruct.ReconstructTablesDetailed

// newestSnapshotTables and uploadSnapshot are indirected for the same reason
// foldTables is, and carry the same rule about when a test may restore them:
// since #1539 both address the server's S3 destination on an S3-backed server,
// and a unit test that reached a bucket would be neither hermetic nor
// offline-safe.
var (
	newestSnapshotTables = reconstruct.NewestSnapshotTables
	uploadSnapshot       = baseline.Upload
	// listBaselines feeds resolveFoldSource; it addresses the bucket on an
	// S3-backed server, same rule as the two above.
	listBaselines = reconstruct.ListBaselines
)

// foldOutcome is everything foldSnapshot decides once the fold has run, split
// out because the fold itself needs a live index and a real baseline and this
// does not.
//
// Left inline, zeroing the carried count compiled and passed the entire suite:
// the only path that reads it runs against real MySQL, so the number the
// console reports had no unit-tier guard at all. That is the same shape as
// refreshFoldConfig one function up.
//
// carried is reported even when runErr is set, and that is deliberate rather
// than an oversight. Publication is all-or-nothing, so a failed run published
// nothing at all; the count still describes the work the fold did, and the UI
// renders it only under a succeeded state. ReconstructTablesDetailed routes a
// failed table into failures and never into reports, so this can never count a
// table that did not actually reuse its file.
//
// Residual, stated rather than papered over: the one-line delegation in
// foldSnapshot is still only reachable with a live index and a real baseline,
// and consoleapp has no fixture that stands those up. A mutation that bypasses
// this function survives the unit tier. What the split buys is that the
// unguarded surface is now a single call rather than the whole decision, and
// the equivalent end-to-end behaviour is pinned at the integration tier by
// internal/reconstruct's TestReconstructParquet_doesNotCarryForwardUnlessAsked.
func foldOutcome(tableList []string, reports []*reconstruct.TableReport,
	failures []reconstruct.TableFailure, runErr error) (tables, refused int, reuse reuseTally, err error) {
	reuse = countReuse(reports)
	if runErr != nil {
		return len(tableList), len(failures), reuse, runErr
	}
	return len(tableList), 0, reuse, nil
}

// startBaselineRefreshLoop launches the opt-in periodic baseline refresh
// (#1171). intervalRaw empty = disabled, which is the default.
//
// Isolation matches the rotation and prune loops: it runs in its own goroutine,
// recovers from a panic, and logs failures without touching the stream or the
// supervisor. A baseline that stopped refreshing is a degradation; a daemon that
// stopped capturing is an outage, and the first must never cause the second.
func startBaselineRefreshLoop(ctx context.Context, reg *console.Registry, sup *baselineSupervisor,
	globalDSN, globalBaselineDir, intervalRaw string, carryDefault bool) error {
	if intervalRaw == "" {
		return nil
	}
	interval, err := cliutil.ParseInterval(intervalRaw)
	if err != nil {
		return fmt.Errorf("--baseline-refresh-interval: %w", err)
	}
	if interval <= 0 {
		return fmt.Errorf("--baseline-refresh-interval must be positive, got %q", intervalRaw)
	}
	if sup == nil {
		// Unreachable from watch.go, which builds the supervisor whenever this
		// interval is set. Kept so a future caller that forgets fails loudly
		// instead of running a console with a flag that is silently inert.
		return fmt.Errorf("internal: --baseline-refresh-interval was set without a baseline supervisor")
	}
	targets, skipped := baselineRefreshTargets(registryEntries(reg), globalDSN, globalBaselineDir)
	logSkippedRefreshTargets(skipped)
	// Name the effective reuse setting AND where it came from, once, at the one
	// moment an operator is reading the log to see whether their configuration
	// took. A console override beats the command line silently by design, so
	// without this line the only symptom of a stale saved toggle is work that
	// keeps happening, or stops happening, for no stated reason.
	carryOn, carrySource := carryForwardProvenance(reg, carryDefault)
	slog.Info("baseline refresh loop enabled", "interval", interval, "servers", len(targets),
		"reuse_unchanged", carryOn, "reuse_set_by", carrySource)
	if len(targets) == 0 {
		// WARN, not a refusal — and the distinction is load-bearing. Every tick
		// recomputes the target set, so "nothing to refresh" is a state a daemon
		// legitimately starts in and grows out of: a source-less `watch` lists no
		// servers at all until they are added FROM THE CONSOLE, and per-server
		// baseline directories live in the registry, not on the command line.
		// Refusing here would mean a compose file carrying the interval could not
		// boot a fresh install — the operator would have to add a server through a
		// console that is not running. The visibility this warning gives is what
		// the refusal was actually for.
		slog.Warn("baseline refresh: no server is refreshable yet, so nothing will run until one has BOTH an " +
			"index DSN and a LOCAL baseline directory (a refresh writes Parquet to a filesystem, so it needs one " +
			"to fold into; its previous snapshot may be in the bucket). Servers added later are " +
			"picked up automatically.")
	}
	// RETENTION INTERPLAY (#616), stated at startup on purpose. A refreshed
	// snapshot is written locally and is NOT uploaded, and baseline.PruneLocal
	// only reclaims a snapshot whose _SUCCESS marker it can confirm in S3 — so
	// nothing THIS loop publishes is prunable, with or without an S3 destination
	// configured.
	//
	// Scoped to this loop, and since #1539 that scope is load-bearing: a
	// PER-SERVER schedule on a server with an S3 destination uploads what it
	// folds, so those snapshots ARE prunable (baselinePruneTargets already
	// covers any entry with both a directory and a bucket). This flag names no
	// destination, which is exactly why its own output stays local. Unattended that is one full-table snapshot per interval,
	// forever. An operator who discovers this from a full disk discovers it far
	// too late, and the loop has no business quietly deciding to upload on their
	// behalf.
	slog.Warn("baseline refresh: snapshots from this interval are written locally and never uploaded, so retention "+
		"cannot reclaim them (a prune needs a confirmed S3 copy of the snapshot). Upload and prune on your own "+
		"schedule, or size the disk for one full-table snapshot per server per interval, at the rate below.",
		diskArgs(interval, targets)...)
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				refreshTick(ctx, reg, sup, globalDSN, globalBaselineDir, interval, carryDefault)
			}
		}
	}()
	return nil
}

// reportRefreshDuration states how long ONE server's refresh took, and says so
// when it took longer than the interval that was asked for.
//
// This is per refresh rather than per tick, and that is the whole correction:
// a tick only DISPATCHES. TriggerRefresh ends in `go s.runRefresh(...)` and
// returns, so anything timed around the dispatch loop measures goroutine
// launch, is always microseconds, and can never exceed any interval
// ParseInterval accepts. An overrun warning built on that span is unreachable
// code, and the reassuring line beside it is false.
//
// Fires once per PUBLISHED refresh. Not per completed one: a run that refused
// costs about what a success costs, because the fan-out runs every table to
// completion before it reports the refusal, so reporting an overrun for it
// would put "raise the interval" above the capture gap that actually stopped
// it. And not per tick: a tick only dispatches.
//
// The duration is also the honest measure of what a full rewrite costs on real
// data: a refresh rewrites every table that CHANGED in full, however little of
// it
// changed. An estimate of that is a guess about someone else's data; this is
// theirs.
func reportRefreshDuration(server string, interval, took time.Duration) {
	if interval <= 0 || took <= interval {
		slog.Debug("baseline refresh finished", "server", server, "took", took, "interval", interval)
		return
	}
	slog.Warn("baseline refresh: this server's refresh took longer than the configured interval, so it cannot "+
		"run as often as requested. A refresh rewrites every table that changed in full, however little of it "+
		"changed (a table with no events at all is carried forward instead), "+
		"so this is the cost of the rewrite, not of the schedule. Raise the interval to match, or refresh "+
		"fewer tables.",
		"server", server, "took", took, "interval", interval)
}

// diskArgs builds the disk warning's attributes, omitting the projection when
// it is not meaningful rather than logging a misleading zero.
func diskArgs(interval time.Duration, targets []refreshRequest) []any {
	args := []any{"interval", interval}
	if n := snapshotsPer30Days(interval); n > 0 {
		args = append(args, "full_table_snapshots_per_server_per_30d", n)
	}
	return append(args, "dirs", refreshTargetDirs(targets))
}

// snapshotsPer30Days projects how many full-table snapshots the configured
// interval produces over a month, for the startup warning.
//
// The warning has said "one snapshot per interval, forever" since the interval
// floor was an hour, where the reader could do the arithmetic and the answer
// was 24 a day. Minutes make that a much worse number and a much easier one to
// skip over: "every 5m" and "8,640 a month per server, none of them
// reclaimable" are the same fact and land differently.
//
// Per SERVER, and the attribute name says so: a tick triggers one refresh for
// every eligible server, so a deployment monitoring several multiplies this.
// Named rather than multiplied because the target set is recomputed every
// tick, so a count fixed at startup would go stale.
//
// Thirty days rather than one, because the warning is about DISK and disk
// fills over weeks. A per-DAY projection also divides to zero for any interval
// longer than a day, so a --baseline-refresh-interval of 7d would have
// reported "0 per day", which reads as "none" and is the opposite of the
// truth.
//
// Returns 0 when the projection is not meaningful (a non-positive interval, or
// one longer than the horizon itself). The caller omits the figure entirely
// rather than print a zero, since the interval it logs alongside already tells
// that story. Reported as a count rather than bytes because a snapshot's size
// depends on the tables, which this loop does not know at startup.
func snapshotsPer30Days(interval time.Duration) int64 {
	if interval <= 0 {
		return 0
	}
	return int64(30 * 24 * time.Hour / interval)
}

// refreshTick is one tick: run a cycle, then report what it did.
//
// Extracted from the ticker's anonymous func on purpose. It is the only place
// the two counters are bound and forwarded, and inside the closure no test
// could reach it: swapping dispatched and skipped compiled and passed, because
// both are ints and every test drove runBaselineRefreshCycle and reportDispatch
// separately. That is the same shape this file already carries a correction
// for one function over, so it gets a seam rather than a comment.
func refreshTick(ctx context.Context, reg *console.Registry, sup *baselineSupervisor,
	globalDSN, globalBaselineDir string, interval time.Duration, carryDefault bool) {
	dispatched, skipped, carry := runBaselineRefreshCycle(ctx, reg, sup, globalDSN, globalBaselineDir, interval, carryDefault)
	// carry comes back from the cycle rather than being resolved again here: a
	// PUT landing between the two reads would make the logged value disagree
	// with what was actually dispatched, which is the one thing this log exists
	// to settle.
	reportDispatch(interval, dispatched, skipped, carry)
}

// refreshTargetsFor builds this cycle's requests with the effective settings
// already applied.
//
// The resolution lives next to the target construction rather than inside the
// cycle's loop body so that "what a request carries" is reachable without
// running a refresh: the loop body is otherwise only observable by letting a
// fold start.
func refreshTargetsFor(reg *console.Registry, globalDSN, globalBaselineDir string, carryDefault bool) []refreshRequest {
	return refreshTargetsWith(reg, globalDSN, globalBaselineDir, effectiveCarryForward(reg, carryDefault))
}

// refreshTargetsWith is the same thing with the setting ALREADY resolved, so
// one cycle resolves it once and logs exactly the value it dispatched with.
func refreshTargetsWith(reg *console.Registry, globalDSN, globalBaselineDir string, carry bool) []refreshRequest {
	reqs, skipped := baselineRefreshTargets(registryEntries(reg), globalDSN, globalBaselineDir)
	logSkippedRefreshTargets(skipped)
	for i := range reqs {
		reqs[i].CarryForwardUnchanged = carry
	}
	return reqs
}

// effectiveCarryForward resolves what this cycle should do: a console-saved
// override wins over the daemon's own flag.
//
// Read per cycle, not cached at boot. A console override is meant to apply to a
// loop that is already running, which is the same contract the rotation panel
// has, and caching would make the panel look inert until a restart.
//
// A registry that cannot be consulted falls back to the daemon flag rather than
// to false: the operator's explicit command line is a better answer than a
// silent no.
func effectiveCarryForward(reg *console.Registry, daemonDefault bool) bool {
	on, _ := carryForwardProvenance(reg, daemonDefault)
	return on
}

// carryForwardProvenance resolves the same value and also names WHERE it came
// from, which is the half that has to be logged.
//
// The two sources disagree silently by design: a saved override of false beats
// a command line saying true, and that is the point of the tri-state. It also
// means an operator can pass the flag, watch every table get rewritten, and
// have nothing anywhere tell them a console toggle from months ago is the
// reason. The provenance string exists so one log line can.
func carryForwardProvenance(reg *console.Registry, daemonDefault bool) (on bool, source string) {
	if reg == nil {
		return daemonDefault, "daemon flag or environment"
	}
	if bc, ok := reg.BaselineRefresh(); ok {
		return bc.CarryForwardUnchanged, "console setting, which overrides the daemon flag"
	}
	return daemonDefault, "daemon flag or environment"
}

// runBaselineRefreshCycle triggers one refresh per eligible server.
//
// Deliberately NOT run once at startup, unlike the prune loop: a refresh is a
// full-table fold over every table, and doing that in the same seconds a daemon
// is establishing replication and opening its console would make every restart
// the most expensive moment in the process's life.
func runBaselineRefreshCycle(ctx context.Context, reg *console.Registry, sup *baselineSupervisor,
	globalDSN, globalBaselineDir string, interval time.Duration, carryDefault bool) (dispatched, skipped int, carry bool) {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("baseline refresh cycle panicked; refreshes continue next tick", "panic", r)
		}
	}()
	carry = effectiveCarryForward(reg, carryDefault)
	if ctx.Err() != nil {
		return dispatched, skipped, carry
	}
	for _, req := range refreshTargetsWith(reg, globalDSN, globalBaselineDir, carry) {
		switch err := sup.TriggerRefresh(req, interval); {
		case err == nil:
			dispatched++
		case errors.Is(err, console.ErrBaselineRunning):
			// Expected: a refresh still folding, or a manual dump in flight.
			// Counted rather than only logged, because at a short interval this
			// stops being an edge case and becomes the steady state — it is the
			// evidence that the interval is shorter than a refresh takes, and
			// the caller needs the number to say so.
			//
			// The per-server line stays at Debug ALONGSIDE the count. Counting
			// alone traded "invisible but specific" for "visible but
			// anonymous": on a multi-server deployment `skipped=2` cannot be
			// acted on, because nothing else names which two, and the
			// "starting" line only fires for servers that were NOT skipped.
			skipped++
			slog.Debug("baseline refresh skipped this tick", "server", req.ServerName, "reason", err)
		default:
			// Nothing else is expected today. If that changes, it must not
			// become invisible: this used to swallow every error at Debug,
			// below the console binary's default level.
			slog.Warn("baseline refresh: could not start", "server", req.ServerName, "error", err)
		}
	}
	return dispatched, skipped, carry
}

// reportDispatch reports what a tick actually did, which is dispatch and
// nothing more.
//
// Quiet at Debug while every server started, because a healthy loop at a short
// interval would otherwise emit a line a minute forever. Visible as soon as
// anything was skipped, because a skip is the ONLY loop-level evidence that
// refreshes are not keeping up, and it was previously logged at Debug where the
// default level hides it. reportRefreshDuration carries the matching per-server
// detail when the slow refresh eventually lands.
func reportDispatch(interval time.Duration, dispatched, skipped int, carry bool) {
	if skipped == 0 {
		slog.Debug("baseline refresh: dispatched", "servers", dispatched, "interval", interval,
			"reuse_unchanged", carry)
		return
	}
	slog.Info("baseline refresh: a server was still busy with another baseline job, so this tick did not start "+
		"a refresh for it. That job is a refresh still folding, a manual dump, a restore or a SQL export: they "+
		"share one lock per server. If it is the previous refresh, the interval is shorter than a refresh "+
		"takes, and that refresh logs its own duration when it lands.",
		"dispatched", dispatched, "skipped", skipped, "interval", interval, "reuse_unchanged", carry)
}

// refreshTargetDirs lists the directories that will grow, for the retention
// warning. Naming globalBaselineDir instead would print "" whenever the
// refreshable servers came from the registry — a disk-growth warning that names
// no directory is the wrong half of the message.
func refreshTargetDirs(targets []refreshRequest) []string {
	seen := map[string]bool{}
	var out []string
	for _, t := range targets {
		if seen[t.BaselineDir] {
			continue
		}
		seen[t.BaselineDir] = true
		out = append(out, t.BaselineDir)
	}
	return out
}

func registryEntries(reg *console.Registry) []console.ServerEntry {
	if reg == nil {
		return nil
	}
	return reg.List()
}

// baselineRefreshTargets collects the servers a refresh can run for — an index
// DSN to fold from and a LOCAL baseline directory to fold into — plus the
// names of servers skipped for having only an S3 destination.
//
// PURE on purpose (#1579): it used to slog.Warn the S3-only skip itself, and
// the same computation now also answers GET /api/baseline-refresh live, where
// a warning per page load would be log spam. The callers that dispatch work
// log the skip via logSkippedRefreshTargets, preserving the old visibility.
func baselineRefreshTargets(entries []console.ServerEntry, globalDSN, globalBaselineDir string) ([]refreshRequest, []string) {
	var out []refreshRequest
	var skippedS3Only []string
	seen := map[string]bool{}
	add := func(id, name, dsn, dir string) {
		if dsn == "" || dir == "" || seen[id] {
			return
		}
		seen[id] = true
		out = append(out, refreshRequest{ServerID: id, ServerName: name, IndexDSN: dsn, BaselineDir: dir})
	}
	add("default", "boot", globalDSN, globalBaselineDir)
	for _, e := range entries {
		if e.DSN != "" && e.BaselineDir == "" && e.BaselineS3 != "" {
			skippedS3Only = append(skippedS3Only, e.Name)
			continue
		}
		add(e.ID, e.Name, e.DSN, e.BaselineDir)
	}
	return out, skippedS3Only
}

// logSkippedRefreshTargets is the warning half baselineRefreshTargets no
// longer carries: a server with only an S3 baseline destination is skipped
// WITH a warning rather than silently — the refresh writes files, so an
// in-place S3 refresh is not something the loop can do, and an operator who
// configured S3-only baselines and set the interval would otherwise see
// nothing happen and no reason why.
func logSkippedRefreshTargets(skipped []string) {
	for _, name := range skipped {
		slog.Warn("baseline refresh: server has an S3-only baseline destination and will not be refreshed "+
			"(a refresh writes Parquet to a filesystem, so it needs a local directory to fold into)", "server", name)
	}
}
