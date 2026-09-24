package consoleapp

import (
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// Custom .sql backup (the Snapshots page's "build a snapshot for a moment"):
// fold the newest snapshot at-or-before the chosen instant forward through
// the index's deltas and write the result as a mydumper-format dump —
// schema files, INSERT chunks and the coordinates `metadata` file — that
// `myloader` (or the mysql client, applying mydumper's session assumptions)
// loads directly, no bintrail needed on the restore side.
//
// Same engine as `reconstruct --output-format mydumper --at T`, same
// posture as the other supervisor folds: all-or-nothing (the #842
// completeness marker gates the download), fail-closed on capture gaps and
// schema changes, conservative DuckDB budget, and the per-server
// single-flight shared with dump/refresh/restore — every one of these
// writes or reads the same baseline store, and all but the dump the same
// index.
//
// Deliberately NOT recorded in the baseline run history: an export
// publishes no snapshot, so FindBySnapshot (the history's one consumer)
// could never match its record, while each one would still consume the
// per-server cap and evict a dump/refresh/restore record the Backups
// detail view needs.
//
// LIFETIME (#1448): the build is a full plaintext copy of every row parked
// on the index host, so it lives exactly as long as it is useful. A
// finished build is removed once a download completes, when sqlExportTTL
// passes without one, when its files vanish from under it, or when a new
// build for the same server starts; a failed build (refused OR panicked) is
// removed at once; and every build a previous process left behind is swept
// at boot (sweepSQLExportStaging).
//
// Two rules make the removals trustworthy:
//
//   - The STATE follows the DISK, never the other way round. A build moves
//     to "downloaded" or "expired" only after removeStagedBuild returned
//     nil. A removal that fails (a read-only chunk, an NFS EBUSY, a guard
//     refusal) leaves the state where it was, records the error in
//     StagingError, keeps the build on the Storage card with its bytes,
//     and is retried on every reaper tick until it succeeds. The card must
//     never say "the file was removed" over bytes that are still there.
//   - Every removal goes through removeStagedBuild, which refuses any path
//     outside the sql-export staging base and any path that crosses a
//     symbolic link, so no state this supervisor can reach makes it delete
//     something it did not write.
//   - A build a NEWER build could not clear is not forgotten. The trigger
//     replaces the server's entry, so the old build's own retry is gone with
//     it; the pre-build wipe records every sibling it could not remove in
//     exportOrphans, where the reaper retries it every tick, the Storage
//     card counts it, and the new build's StagingError names it until the
//     removal succeeds. Otherwise that directory would be exactly the
//     invisible space this lifecycle exists to prevent, until the next boot.

// sqlExportTTL is how long a finished build stays downloadable. Long enough
// to survive a coffee break and a slow browser; short enough that a forgotten
// build does not park gigabytes on the index host until someone finds it
// with du. The Snapshots page tells the operator the deadline.
const sqlExportTTL = 4 * time.Hour

// sqlExportReapEvery is how often the background reaper looks for builds
// past their TTL and retries removals that failed. Status reads expire
// lazily too, so this only bounds how long an UNWATCHED build outlives its
// deadline.
const sqlExportReapEvery = time.Minute

// The reasons a build is removed. Each one names the terminal state the
// build reaches once the removal has SUCCEEDED (see removeSQLExportBuild).
const (
	removeDownloaded = "downloaded"
	removeExpired    = "download deadline passed"
	removeVanished   = "files removed from the staging directory"
	removeFailed     = "build failed"
)

// sqlExportRun is the per-server bookkeeping beside the BaselineStatus the
// API serves: where the current build lives, whether a download is
// streaming it, and whether a removal is owed.
type sqlExportRun struct {
	dir string
	// holds counts downloads streaming this build right now. A held build
	// is never expired or removed under the stream: a TTL that lands at
	// hour 3:59 of a multi-gigabyte download must not turn it into an
	// aborted archive.
	holds int
	// pending is the reason a removal is owed and has not yet succeeded
	// ("" = none). Set BEFORE the removal starts, so a download cannot
	// take a hold on a build that is being torn down, and left set when
	// the removal fails, so the reaper retries it.
	pending string
	// problem is THIS build's staging-side problem (its removal failed, or
	// its marker could not be read); "" when fine. The status's
	// StagingError is composed from it plus the server's orphans by
	// syncStagingErrorLocked, so clearing one never erases the other.
	problem string
}

// removeStagedBuildFn is the seam every in-process removal goes through.
// A variable so a test can observe or interrupt a removal mid-flight
// (a trigger that lands while a removal runs is the race the re-check in
// removeSQLExportBuild exists for); production never reassigns it.
var removeStagedBuildFn = removeStagedBuild

// sqlExportBase is the one directory every build lives under. It is the
// boundary removeStagedBuild enforces.
func (s *baselineSupervisor) sqlExportBase() string {
	return filepath.Join(s.stagingDir, "sql-export")
}

// sqlExportRoot holds a server's builds. Each build writes into its OWN
// subdirectory: a shared path reused across builds would let a rebuild
// interleave two instants into one archive whose per-file guards all pass
// (fresh Stat, matching sizes) — the silent half of the wipe race.
func (s *baselineSupervisor) sqlExportRoot(serverID string) string {
	return filepath.Join(s.sqlExportBase(), serverID)
}

// clock is the export lifecycle's time source. Injectable so the TTL is
// testable without waiting for it; nil means the wall clock.
func (s *baselineSupervisor) clock() time.Time {
	if s.now != nil {
		return s.now()
	}
	return time.Now()
}

// TriggerSQLExport starts a build. console.ErrBaselineRunning when another
// baseline job for the server is in flight.
func (s *baselineSupervisor) TriggerSQLExport(req console.SQLExportRequest) error {
	s.mu.Lock()
	if s.busyLocked(req.ServerID) {
		s.mu.Unlock()
		return console.ErrBaselineRunning
	}
	now := s.clock().UTC()
	dir := filepath.Join(s.sqlExportRoot(req.ServerID), strconv.FormatInt(now.UnixNano(), 10))
	s.exports[req.ServerID] = &console.BaselineStatus{State: "running", Since: now.Format(time.RFC3339),
		At: req.At.UTC().Format(time.RFC3339)}
	s.exportRuns[req.ServerID] = &sqlExportRun{dir: dir}
	// A previous build this server could not clear stays on the new build's
	// status from its first poll, not from the wipe's next failure.
	s.syncStagingErrorLocked(req.ServerID)
	s.mu.Unlock()

	slog.Info("sql export: starting", "server", req.ServerName, "id", req.ServerID,
		"at", req.At.UTC().Format(time.RFC3339))
	go s.runSQLExport(req, dir)
	return nil
}

// SQLExportStatus reports the last build for a server (idle if none ran here).
// It expires first, so a build past its deadline, or one whose files were
// removed from under it, reads "expired" on the very poll that finds it
// rather than "succeeded" over a download that would 409.
func (s *baselineSupervisor) SQLExportStatus(serverID string) console.BaselineStatus {
	s.expireSQLExports()
	s.mu.Lock()
	defer s.mu.Unlock()
	if st, ok := s.exports[serverID]; ok {
		return *st
	}
	return console.BaselineStatus{State: "idle"}
}

// SQLExportDir returns the finished dump's directory plus the status it
// belongs to, from ONE locked read — two separate reads let a trigger land
// between them and label the old build's bytes with the new build's
// instant. ok is false until the build has SUCCEEDED, is not owed a
// removal, and its directory affirmatively carries the #842 _SUCCESS marker
// without an _INCOMPLETE one. Affirmative on purpose: baseline.
// SnapshotComplete is complete-by-default when NO marker exists (a legacy
// rule for pre-marker snapshots), and the marker-less shapes here are never
// legacy — they are a torn or externally-removed staging directory (the
// default staging root lives under the system temp dir, which some hosts
// reap), and must read not-ready, not stream a 500. A marker that cannot
// be stat'ed at all reads not-ready too; expireSQLExports has already
// recorded why in StagingError.
func (s *baselineSupervisor) SQLExportDir(serverID string) (string, console.BaselineStatus, bool) {
	s.expireSQLExports()
	s.mu.Lock()
	st := console.BaselineStatus{State: "idle"}
	var dir string
	if p, ok := s.exports[serverID]; ok {
		st = *p
		if run := s.exportRuns[serverID]; run != nil && run.pending == "" {
			dir = run.dir
		}
	}
	s.mu.Unlock()
	if st.State != "succeeded" || dir == "" {
		return "", st, false
	}
	if _, err := os.Stat(filepath.Join(dir, baseline.SuccessMarker)); err != nil {
		return "", st, false
	}
	if _, err := os.Stat(filepath.Join(dir, baseline.IncompleteMarker)); err == nil {
		return "", st, false
	}
	return dir, st, true
}

// SQLExportHold registers a download of the build in dir. While at least
// one hold is open the build is neither expired nor removed; release drops
// the hold (idempotent). ok is false when dir is no longer the server's
// finished, removal-free build — the caller answers "not ready" instead of
// streaming a directory that is being torn down.
func (s *baselineSupervisor) SQLExportHold(serverID, dir string) (func(), bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	run := s.exportRuns[serverID]
	st := s.exports[serverID]
	if run == nil || st == nil || dir == "" || run.dir != dir || run.pending != "" || st.State != "succeeded" {
		return func() {}, false
	}
	run.holds++
	var once sync.Once
	return func() {
		once.Do(func() {
			s.mu.Lock()
			run.holds--
			s.mu.Unlock()
		})
	}, true
}

// SQLExportDelivered is the download handler's signal that the whole
// archive reached the client: the build has done its job, so its rows leave
// the disk now instead of waiting for the TTL. dir pins WHICH build was
// delivered — a trigger that landed mid-stream owns a different directory,
// and that one must not be removed on the strength of the old one's
// download. An "expired" build that is still the server's current one is
// accepted too: the deadline may have passed under a stream that then
// completed, and delivered is the truer verdict.
func (s *baselineSupervisor) SQLExportDelivered(serverID, dir string) {
	s.mu.Lock()
	st := s.exports[serverID]
	run := s.exportRuns[serverID]
	if st == nil || run == nil || dir == "" || run.dir != dir || (st.State != "succeeded" && st.State != "expired") {
		s.mu.Unlock()
		return
	}
	st.DownloadedAt = s.clock().UTC().Format(time.RFC3339)
	s.mu.Unlock()
	s.removeSQLExportBuild(serverID, dir, removeDownloaded)
}

// SQLExportStaged reports what the sql-export staging holds right now, for
// the Storage page's disk picture: every build in progress, waiting for its
// download, or owed a removal that has not succeeded yet (a failed build
// whose partial files could not be removed included), plus every previous
// build a newer one could not clear (state "replaced"), each with its LIVE
// size. A size the walk could not establish is reported as unknown, never
// as zero. Expires first, so a build past its deadline never counts.
func (s *baselineSupervisor) SQLExportStaged() console.SQLExportStagingInfo {
	s.expireSQLExports()
	info := console.SQLExportStagingInfo{Dir: s.sqlExportBase(), TTL: sqlExportTTL}
	s.mu.Lock()
	type held struct {
		id  string
		st  console.BaselineStatus
		dir string
		// orphan marks a previous build the wipe could not remove; st then
		// carries only the removal error, in StagingError.
		orphan bool
	}
	var builds []held
	for id, st := range s.exports {
		run := s.exportRuns[id]
		if run == nil {
			continue
		}
		if st.State != "running" && st.State != "succeeded" && run.pending == "" {
			continue
		}
		builds = append(builds, held{id: id, st: *st, dir: run.dir})
	}
	for id, orphans := range s.exportOrphans {
		for path, msg := range orphans {
			builds = append(builds, held{id: id, dir: path, orphan: true, st: console.BaselineStatus{
				State:        "replaced",
				StagingError: "could not remove the staged files (replaced by a newer build): " + msg,
			}})
		}
	}
	s.mu.Unlock()
	// The current build first, then its orphans by path, so the card reads
	// top-down the way the disk filled up.
	sort.Slice(builds, func(i, j int) bool {
		a, b := builds[i], builds[j]
		if a.id != b.id {
			return a.id < b.id
		}
		if a.orphan != b.orphan {
			return !a.orphan
		}
		return a.dir < b.dir
	})
	for _, b := range builds {
		bytes, err := dirBytes(b.dir)
		if err != nil {
			slog.Warn("sql export: could not measure a staged build", "id", b.id, "dir", b.dir, "error", err)
		}
		info.Builds = append(info.Builds, console.SQLExportStagedBuild{
			ServerID: b.id, State: b.st.State, At: b.st.At, ExpiresAt: b.st.ExpiresAt,
			Bytes: bytes, BytesKnown: err == nil, StagingError: b.st.StagingError,
		})
	}
	return info
}

// expireSQLExports is the reaper's tick and the lazy half of every read:
// it retries removals still owed, expires finished builds past their
// deadline, and expires finished builds whose files are gone from under
// them. The disk is consulted OUTSIDE the supervisor mutex (this runs on
// every status poll); the state change is made under it by
// removeSQLExportBuild, which re-checks that the entry still points at the
// same directory. A held build (download in flight) is skipped entirely.
//
// A marker that cannot be stat'ed for any reason other than not existing
// is NOT "files removed": an EACCES or an unmounted volume would otherwise
// burn a build that may be intact. That build keeps its state, the error
// is recorded in StagingError for the card, and the next poll looks again.
func (s *baselineSupervisor) expireSQLExports() {
	now := s.clock()
	type candidate struct {
		id, dir, pending, expiresAt string
	}
	var cands []candidate
	s.mu.Lock()
	for id, st := range s.exports {
		run := s.exportRuns[id]
		if run == nil || run.holds > 0 {
			continue
		}
		if run.pending == "" && st.State != "succeeded" {
			continue
		}
		cands = append(cands, candidate{id: id, dir: run.dir, pending: run.pending, expiresAt: st.ExpiresAt})
	}
	s.mu.Unlock()
	for _, c := range cands {
		if c.pending != "" {
			s.removeSQLExportBuild(c.id, c.dir, c.pending)
			continue
		}
		present, err := markerPresent(c.dir)
		switch {
		case err != nil:
			slog.Warn("sql export: could not read a staged build's completeness marker; the build is kept and checked again on the next poll",
				"id", c.id, "dir", c.dir, "error", err)
			s.setStagingError(c.id, c.dir, "the staging directory could not be read: "+err.Error())
		case !present:
			s.removeSQLExportBuild(c.id, c.dir, removeVanished)
		case sqlExportExpired(c.expiresAt, now):
			s.removeSQLExportBuild(c.id, c.dir, removeExpired)
		default:
			s.setStagingError(c.id, c.dir, "")
		}
	}
	// Previous builds a newer build could not clear: retried on every tick
	// and every poll, exactly like a removal the current build is owed.
	type orphan struct{ id, path string }
	var orphans []orphan
	s.mu.Lock()
	for id, paths := range s.exportOrphans {
		for path := range paths {
			orphans = append(orphans, orphan{id: id, path: path})
		}
	}
	s.mu.Unlock()
	for _, o := range orphans {
		s.removeOrphan(o.id, o.path)
	}
}

// setStagingError records (or clears) the current build's own staging-side
// problem, only while the entry still points at dir. The server's orphans
// stay on the status either way.
func (s *baselineSupervisor) setStagingError(serverID, dir, msg string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	run := s.exportRuns[serverID]
	st := s.exports[serverID]
	if run == nil || st == nil || run.dir != dir || run.pending != "" {
		return
	}
	run.problem = msg
	s.syncStagingErrorLocked(serverID)
}

// syncStagingErrorLocked recomputes the status's StagingError for a server
// from the two things it reports: the current build's own problem first,
// then every previous build under the server's staging root that could not
// be removed, by path. Composed rather than written in place so that the
// paths which clear one part (a successful removal, a marker readable
// again) cannot silently drop the other. Caller holds s.mu.
func (s *baselineSupervisor) syncStagingErrorLocked(serverID string) {
	st := s.exports[serverID]
	if st == nil {
		return
	}
	var parts []string
	if run := s.exportRuns[serverID]; run != nil && run.problem != "" {
		parts = append(parts, run.problem)
	}
	paths := make([]string, 0, len(s.exportOrphans[serverID]))
	for p := range s.exportOrphans[serverID] {
		paths = append(paths, p)
	}
	sort.Strings(paths)
	for _, p := range paths {
		parts = append(parts, "a previous build could not be removed: "+p+": "+s.exportOrphans[serverID][p])
	}
	st.StagingError = strings.Join(parts, "; ")
}

// noteOrphan records a previous build under serverID's staging root that a
// removal could not clear, with the error, and puts it on the server's
// status. Idempotent: a retry that fails again only refreshes the error.
// Reports whether the record CHANGED (a new path, or a different error
// text), which is what decides whether the caller logs it: the retry runs
// every minute and on every poll, and a sibling the guard will refuse
// forever must not warn on each of them.
func (s *baselineSupervisor) noteOrphan(serverID, path string, err error) (changed bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.exportOrphans == nil {
		s.exportOrphans = make(map[string]map[string]string)
	}
	if s.exportOrphans[serverID] == nil {
		s.exportOrphans[serverID] = make(map[string]string)
	}
	prev, had := s.exportOrphans[serverID][path]
	s.exportOrphans[serverID][path] = err.Error()
	s.syncStagingErrorLocked(serverID)
	return !had || prev != err.Error()
}

// forgetOrphan drops a previous build whose removal succeeded, and takes it
// off the server's status. A path that was never recorded is a no-op.
func (s *baselineSupervisor) forgetOrphan(serverID, path string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	paths := s.exportOrphans[serverID]
	if _, ok := paths[path]; !ok {
		return
	}
	delete(paths, path)
	if len(paths) == 0 {
		delete(s.exportOrphans, serverID)
	}
	s.syncStagingErrorLocked(serverID)
}

// removeOrphan retries the removal of a previous build the wipe could not
// clear. Through the same guard as every removal; no hold is consulted
// because the run that could have held it was replaced by the trigger
// (the same posture as the wipe itself).
//
// Never the directory the CURRENT run owns: an orphan can only enter the
// map from the wipe, which skips the build that is starting, so under an
// advancing clock the two never coincide; this check is what makes that a
// property of the code rather than of the clock.
func (s *baselineSupervisor) removeOrphan(serverID, path string) {
	s.mu.Lock()
	run := s.exportRuns[serverID]
	owned := run != nil && run.dir == path
	s.mu.Unlock()
	if owned {
		s.forgetOrphan(serverID, path)
		return
	}
	freed, sized, err := removeStagedBuildFn(s.sqlExportBase(), path)
	if err != nil {
		if s.noteOrphan(serverID, path, err) {
			slog.Warn("sql export: could not remove a previous build; it stays on disk and the removal is retried every minute (this line repeats only when the error changes)",
				"id", serverID, "path", path, "error", err)
		} else {
			slog.Debug("sql export: a previous build still could not be removed", "id", serverID, "path", path, "error", err)
		}
		return
	}
	s.forgetOrphan(serverID, path)
	if sized {
		slog.Info("sql export: removed a previous build", "id", serverID, "path", path, "bytes", freed)
	} else {
		slog.Info("sql export: removed a previous build", "id", serverID, "path", path, "bytes", "unknown")
	}
}

// markerPresent reports whether dir carries the #842 _SUCCESS marker.
// Only "does not exist" is a clean absence; any other stat failure is an
// error the caller must not read as "removed".
func markerPresent(dir string) (bool, error) {
	if dir == "" {
		return false, nil
	}
	_, err := os.Stat(filepath.Join(dir, baseline.SuccessMarker))
	switch {
	case err == nil:
		return true, nil
	case errors.Is(err, fs.ErrNotExist):
		return false, nil
	default:
		return false, err
	}
}

// sqlExportExpired reports whether a build stamped with expiresAt is past
// its deadline at now. A stamp this build cannot parse counts as expired:
// keeping an unbounded build is the failure this exists to prevent.
func sqlExportExpired(expiresAt string, now time.Time) bool {
	exp, err := time.Parse(time.RFC3339, expiresAt)
	if err != nil {
		return true
	}
	return !now.Before(exp)
}

// runSQLExportReaper expires builds on a timer until the daemon stops. It
// exists for the build nobody is watching: the Snapshots page polls only
// while it is open, and a build left behind after the tab closed would
// otherwise sit on disk until the next visit. Each tick is guarded the way
// every other goroutine in `watch` is: this process is also the capture
// plane, and a panic in housekeeping must not stop it.
func (s *baselineSupervisor) runSQLExportReaper() {
	every := s.exportReapEvery
	if every <= 0 {
		every = sqlExportReapEvery
	}
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-t.C:
			s.reapSQLExportsGuarded()
		}
	}
}

func (s *baselineSupervisor) reapSQLExportsGuarded() {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("sql export: the staging reaper hit an internal error and skipped a tick. Capture and the "+
				"web interface keep running. Please report this with the stack recorded here.",
				"panic", r, "stack", string(debug.Stack()))
		}
	}()
	s.expireSQLExports()
}

// removeSQLExportBuild owes the build in dir a removal for the given reason
// and attempts it now. The reason is recorded FIRST, under the lock, so no
// download can take a hold on a build that is about to go; a build that IS
// held is left for the reaper (its holds drop when the stream ends). The
// state moves to the reason's terminal state ONLY when the removal
// succeeded: a failure keeps the state, records the error in StagingError
// (the card shows it, the Storage card keeps counting the bytes) and stays
// owed, so every reaper tick retries it. A removal that succeeds after the
// entry moved on to a newer build changes nothing but the disk: the
// re-check after the removal is what keeps a trigger that landed
// mid-removal from having its fresh "running" build stamped with the old
// build's terminal state.
func (s *baselineSupervisor) removeSQLExportBuild(serverID, dir, why string) {
	if dir == "" {
		return
	}
	s.mu.Lock()
	run := s.exportRuns[serverID]
	if run == nil || run.dir != dir {
		s.mu.Unlock()
		return
	}
	run.pending = why
	if st := s.exports[serverID]; st != nil {
		st.RemovalOwed = true
	}
	if run.holds > 0 {
		s.mu.Unlock()
		return
	}
	s.mu.Unlock()

	freed, sized, err := removeStagedBuildFn(s.sqlExportBase(), dir)

	s.mu.Lock()
	run = s.exportRuns[serverID]
	st := s.exports[serverID]
	if run == nil || st == nil || run.dir != dir {
		s.mu.Unlock()
		return
	}
	if err != nil {
		run.problem = "could not remove the staged files (" + why + "): " + err.Error()
		s.syncStagingErrorLocked(serverID)
		s.mu.Unlock()
		slog.Warn("sql export: could not remove a staged build; it stays on disk and the removal is retried every minute",
			"id", serverID, "dir", dir, "why", why, "error", err)
		return
	}
	run.pending = ""
	run.problem = ""
	st.RemovalOwed = false
	s.syncStagingErrorLocked(serverID)
	switch {
	case st.State == "downloaded":
		// Terminal already: a "vanished" or "expired" removal that lands
		// after the delivery (the expiry pass snapshots its candidates
		// outside the lock) finds the directory gone and must not turn a
		// downloaded build into an expired one.
	case st.State == "expired" && why != removeDownloaded:
		// Same, from the other side: only a delivery upgrades an expired
		// build (the deadline caught a stream that then completed).
	case why == removeDownloaded:
		st.State = "downloaded"
	case why == removeFailed:
		// Stays "failed": the verdict is the fold's, not the cleanup's.
	default:
		st.State = "expired"
	}
	s.mu.Unlock()
	if sized {
		slog.Info("sql export: removed staged build", "id", serverID, "dir", dir, "why", why, "bytes", freed)
	} else {
		slog.Info("sql export: removed staged build", "id", serverID, "dir", dir, "why", why, "bytes", "unknown")
	}
}

// removeStagedBuild deletes dir, which must be a build directory strictly
// below base (the sql-export staging base), and reports the bytes it held
// (sized is false when they could not be measured). Two refusals, both
// loud, both before anything is touched:
//
//   - dir outside base (an absolute elsewhere, a ".." escape, base itself):
//     nothing this supervisor writes lives there.
//   - base or any component below it is a symbolic link: os.RemoveAll does
//     not follow links, but a link swapped in for a directory would still
//     redirect a path this code trusts onto a tree it must not delete. The
//     components ABOVE base are not checked: an operator may legitimately
//     point BINTRAIL_CONSOLE_BASELINE_STAGING at a link to a bigger disk.
//
// A dir that is already gone is a success: the goal is "not there".
func removeStagedBuild(base, dir string) (freed int64, sized bool, err error) {
	base, dir = filepath.Clean(base), filepath.Clean(dir)
	// A relative base would resolve against whatever the process's working
	// directory happens to be; every caller passes an absolute one, and a
	// guard that trusts that is no guard.
	if !filepath.IsAbs(base) {
		return 0, false, fmt.Errorf("refusing to remove %q: the staging directory %q is not an absolute path", dir, base)
	}
	rel, err := filepath.Rel(base, dir)
	if err != nil || rel == "." || rel == ".." || filepath.IsAbs(rel) ||
		strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return 0, false, fmt.Errorf("refusing to remove %q: not inside the sql-export staging directory %q", dir, base)
	}
	if fi, err := os.Lstat(base); err == nil && fi.Mode()&fs.ModeSymlink != 0 {
		return 0, false, fmt.Errorf("refusing to remove %q: the staging directory %q is a symbolic link", dir, base)
	}
	cur := base
	for _, part := range strings.Split(rel, string(filepath.Separator)) {
		cur = filepath.Join(cur, part)
		fi, err := os.Lstat(cur)
		if errors.Is(err, fs.ErrNotExist) {
			return 0, true, nil
		}
		if err != nil {
			return 0, false, err
		}
		if fi.Mode()&fs.ModeSymlink != 0 {
			return 0, false, fmt.Errorf("refusing to remove %q: %q is a symbolic link", dir, cur)
		}
	}
	freed, sizeErr := dirBytes(dir)
	if err := os.RemoveAll(dir); err != nil {
		return 0, false, err
	}
	return freed, sizeErr == nil, nil
}

// dirBytes sums the regular files under dir without following symbolic
// links (WalkDir reports a link as itself, never as its target). A missing
// ROOT is zero bytes, not an error; any failure below it is an error, so a
// build with an unreadable corner is reported as "size unknown" rather
// than as the fraction the walk managed to count.
func dirBytes(dir string) (int64, error) {
	if _, err := os.Lstat(dir); errors.Is(err, fs.ErrNotExist) {
		return 0, nil
	}
	var n int64
	err := filepath.WalkDir(dir, func(_ string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.Type().IsRegular() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		n += info.Size()
		return nil
	})
	if err != nil {
		return 0, err
	}
	return n, nil
}

// sweepSQLExportStaging removes every build a previous process left under
// stagingDir/sql-export. A restart empties the in-memory exports map, so
// any dump a dead process built (finished, interrupted mid-fold, or
// abandoned when the operator closed the tab) is unreachable from the API
// and would otherwise sit on disk indefinitely. Called from watch startup
// UNCONDITIONALLY as well as from the supervisor constructor, because a
// restart that turned the baseline features off would otherwise keep the
// old artifact forever (no supervisor would ever sweep it).
//
// One build at a time through removeStagedBuild, never one RemoveAll over
// the base: the guard is what makes a boot-time delete safe to run on a
// path an operator may have rearranged while the daemon was down.
func sweepSQLExportStaging(stagingDir string) {
	base := filepath.Join(stagingDir, "sql-export")
	fi, err := os.Lstat(base)
	if errors.Is(err, fs.ErrNotExist) {
		return
	}
	if err != nil {
		slog.Warn("sql export: could not sweep the staging directory", "dir", base, "error", err)
		return
	}
	if fi.Mode()&fs.ModeSymlink != 0 {
		slog.Warn("sql export: the staging directory is a symbolic link; not sweeping it. Remove the link and restart to let old builds be cleared here", "dir", base)
		return
	}
	servers, err := os.ReadDir(base)
	if err != nil {
		slog.Warn("sql export: could not sweep the staging directory", "dir", base, "error", err)
		return
	}
	var builds, unsized int
	var freed int64
	for _, srv := range servers {
		srvDir := filepath.Join(base, srv.Name())
		if !srv.IsDir() {
			// A stray file or a link where a server directory should be:
			// nothing this code wrote, so nothing this code removes.
			slog.Warn("sql export: skipping an entry this daemon did not write", "path", srvDir)
			continue
		}
		runs, err := os.ReadDir(srvDir)
		if err != nil {
			slog.Warn("sql export: could not sweep a server's staged builds", "dir", srvDir, "error", err)
			continue
		}
		for _, run := range runs {
			n, sized, err := removeStagedBuild(base, filepath.Join(srvDir, run.Name()))
			if err != nil {
				slog.Warn("sql export: could not remove a staged build", "error", err)
				continue
			}
			builds++
			if sized {
				freed += n
			} else {
				unsized++
			}
		}
		// Only an empty directory goes; anything skipped above keeps it.
		_ = os.Remove(srvDir)
	}
	_ = os.Remove(base)
	if builds > 0 {
		slog.Info("sql export: removed staged builds left by a previous run", "builds", builds, "bytes", freed, "builds_of_unknown_size", unsized)
	}
}

func (s *baselineSupervisor) runSQLExport(req console.SQLExportRequest, dir string) {
	// Registered FIRST so it runs LAST, after the recover below has moved a
	// panicking build to "failed": the partial files of a fold that died
	// are as unusable as those of one that refused, and would otherwise
	// stay on disk until the next build or boot.
	defer s.removeFailedSQLExport(req.ServerID, dir)
	defer s.recoverBaselineJob(baselineJobExport, req.ServerID, req.ServerName)
	tables, rows, bytes, err := s.executeSQLExport(req, dir)
	s.finishSQLExport(req, dir, tables, rows, bytes, err)
}

// removeFailedSQLExport removes the build in dir if the run ended "failed"
// (by refusal or by panic). A refused fold can have written gigabytes
// under _INCOMPLETE, and nothing will ever download them.
func (s *baselineSupervisor) removeFailedSQLExport(serverID, dir string) {
	s.mu.Lock()
	st := s.exports[serverID]
	run := s.exportRuns[serverID]
	failed := st != nil && st.State == "failed" && run != nil && run.dir == dir
	s.mu.Unlock()
	if failed {
		s.removeSQLExportBuild(serverID, dir, removeFailed)
	}
}

// finishSQLExport is the status tail of a build: it publishes the verdict
// and, on success, stamps the download deadline. The failed build's
// directory is removed by removeFailedSQLExport, which also covers the
// panic path this tail never reaches.
func (s *baselineSupervisor) finishSQLExport(req console.SQLExportRequest, dir string, tables int, rows, bytes int64, err error) {
	now := s.clock().UTC()
	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.exports[req.ServerID]
	if st == nil { // defensive; never cleared under lock
		st = &console.BaselineStatus{}
		s.exports[req.ServerID] = st
	}
	if s.exportRuns[req.ServerID] == nil {
		s.exportRuns[req.ServerID] = &sqlExportRun{dir: dir}
	}
	st.FinishedAt = now.Format(time.RFC3339)
	st.Tables = tables
	st.Rows = rows
	st.Bytes = bytes
	if err != nil {
		// Attempt-scoped partials read as progress to a status-API consumer
		// ({state:"failed", rows:12000} looks half-done); a failed build
		// published nothing, so report nothing.
		st.Tables, st.Rows, st.Bytes = 0, 0, 0
		st.State = "failed"
		st.LastError = err.Error()
		// Warn, never Error: a refusal (gap, schema change) is the fail-closed
		// contract working; the operator picks another moment.
		slog.Warn("sql export: failed, nothing downloadable", "server", req.ServerName,
			"id", req.ServerID, "error", err)
		return
	}
	st.State = "succeeded"
	st.LastError = ""
	st.ExpiresAt = now.Add(sqlExportTTL).Format(time.RFC3339)
	slog.Info("sql export: ready", "server", req.ServerName, "id", req.ServerID,
		"tables", tables, "rows", rows, "at", st.At, "expires_at", st.ExpiresAt)
}

func (s *baselineSupervisor) executeSQLExport(req console.SQLExportRequest, dir string) (tables int, rows, bytes int64, err error) {
	tableList, err := reconstruct.SnapshotTablesAt(s.ctx, req.BaselineSrc, req.At)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("list the snapshot to build from: %w", err)
	}
	if len(tableList) == 0 {
		return 0, 0, 0, fmt.Errorf("no snapshot exists at or before %s; the build folds an existing snapshot forward, so pick a moment after your oldest snapshot", req.At.UTC().Format("2006-01-02 15:04:05"))
	}
	// Tear down previous builds as this one starts; the new build writes only
	// into its own directory, so an in-flight download of an OLD build either
	// completes byte-exact (files already open survive the unlink) or dies
	// loudly on its abort guard — never silently mixed. A sibling that
	// cannot be removed (a stuck previous build, or one the guard refuses:
	// a link, a stray entry) does not fail this build, which would read as
	// an internal bug; it is recorded as an orphan instead, so it stays on
	// the Storage card, on this build's status, and on the reaper's retry
	// list rather than on disk until someone runs du.
	root := filepath.Dir(dir)
	if err := os.MkdirAll(root, 0o700); err != nil {
		return 0, 0, 0, fmt.Errorf("create staging directory: %w", err)
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("scan staging directory: %w", err)
	}
	for _, ent := range entries {
		if ent.Name() == filepath.Base(dir) {
			continue
		}
		old := filepath.Join(root, ent.Name())
		if _, _, err := removeStagedBuildFn(s.sqlExportBase(), old); err != nil {
			s.noteOrphan(req.ServerID, old, err)
			slog.Warn("sql export: could not clear a previous build; it stays on disk and the removal is retried every minute",
				"id", req.ServerID, "path", old, "error", err)
			continue
		}
		s.forgetOrphan(req.ServerID, old)
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return 0, 0, 0, fmt.Errorf("create build directory: %w", err)
	}
	reports, _, runErr := foldTables(s.ctx, sqlExportFoldConfig(req, dir, tableList))
	for _, rep := range reports {
		rows += rep.RowsWritten
	}
	// The artifact's weight, for the UI's blob-download confirm. Best-effort:
	// a stat failure here must not fail a build the fold already finished.
	if built, err := os.ReadDir(dir); err == nil {
		for _, ent := range built {
			if info, err := ent.Info(); err == nil && !ent.IsDir() {
				bytes += info.Size()
			}
		}
	}
	return len(tableList), rows, bytes, sqlExportRunError(reports, runErr)
}

// sqlExportFoldConfig is the configuration one SQL export build folds with.
//
// Split out of executeSQLExport for the same reason refreshFoldConfig is split
// out of foldSnapshot: the budgets it carries are then checkable without
// running a fold or touching a filesystem.
//
// RESOURCE POSTURE: this build folds inside the process that is also capturing
// and serving the console, so it takes the shared in-daemon posture. The DuckDB
// budget stays at its zero value, which resolves to the container-safe
// DefaultTuning. Parallelism and WarnEventThreshold do NOT, so both are named:
// zero would mean runtime.NumCPU() and a warning that never fires. They are the
// same constants the refresh and restore folds use, because a host that cannot
// afford one of these folds cannot afford another.
func sqlExportFoldConfig(req console.SQLExportRequest, dir string, tableList []string) reconstruct.FullTableConfig {
	return reconstruct.FullTableConfig{
		IndexDSN:           req.IndexDSN,
		BaselineSrc:        req.BaselineSrc,
		Tables:             tableList,
		At:                 req.At.UTC(),
		OutputDir:          dir,
		OutputFormat:       reconstruct.OutputFormatMydumper,
		Parallelism:        daemonFoldParallelism,
		WarnEventThreshold: daemonFoldWarnEventThreshold,
		MaxTouchedRows:     daemonFoldMaxTouchedRows,
		RemediationHint:    daemonFoldRemediation,
		SpaceCheck:         newDiskSpaceCheck(),
		// AllowGaps stays FALSE: a dump the operator will load somewhere is
		// the last artifact that may be knowingly incomplete.
	}
}

// sqlExportRunError folds the engine's per-table reports into the run
// verdict. The engine's binlog-only fallback (baseline vanished mid-build)
// warns and keeps going in mydumper mode; here it must fail the run: such a
// table holds only the rows the window touched, and drill's doctrine
// applies — a dump without its baseline is never a PASS.
func sqlExportRunError(reports []*reconstruct.TableReport, runErr error) error {
	for _, rep := range reports {
		if rep.BinlogOnly {
			runErr = errors.Join(runErr, fmt.Errorf(
				"table %s.%s lost its snapshot mid-build and was rebuilt from recorded changes only; that dump would silently miss every row those changes never touched", rep.Schema, rep.Table))
		}
	}
	return runErr
}
