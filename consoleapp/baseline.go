package consoleapp

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/cliutil"
	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/mydumperlock"
	"github.com/dbtrail/dbtrail/internal/notify"
	"github.com/dbtrail/dbtrail/internal/pgbaseline"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/storage"
)

// checkMydumperPrivileges is mydumperlock.CheckPrivileges behind a seam, so a
// test can observe the LOCK MODE this call site actually forwards. Without it
// the argument is unverifiable: every mode fails identically against an
// unreachable source, so hardcoding one here passes the whole suite while
// re-introducing #1381 — an operator who selected lock-all gets judged against
// ftwrl's requirements and told to grant BACKUP_ADMIN, which RDS refuses.
var checkMydumperPrivileges = mydumperlock.CheckPrivileges

// baselineSupervisor implements console.BaselineController by running the
// dump→convert→upload pipeline IN-PROCESS (#613): the console image bundles
// mydumper, so a baseline never starts a sibling container and the daemon never
// mounts the docker socket. One job at a time per server, tracked in-memory —
// the durable record is the snapshot itself (listed by /api/baselines).
type baselineSupervisor struct {
	ctx        context.Context // daemon lifecycle; cancels an in-flight dump on shutdown
	stagingDir string          // base dir for temp dump + staged Parquet (S3-destined runs)

	// lockMode selects how mydumper synchronizes its worker threads onto one
	// instant for MySQL/MariaDB dumps — see internal/baseline.LockMode for the
	// measured trade-offs. Defaults to baseline.DefaultLockMode (FTWRL): a
	// baseline is the seed state reconstruct merges deltas onto, so a snapshot
	// that can be torn must be asked for, never landed on (#1377). Set via
	// BINTRAIL_CONSOLE_BASELINE_LOCK_MODE. No effect on PostgreSQL baselines
	// (executePG uses pgoutput's own consistent-point LSN unconditionally).
	lockMode baseline.LockMode
	// reg is the settings store a saved lock mode is read from, per job
	// (#1682). nil in tests and on a console with no registry, which is what
	// makes the boot value above the fallback rather than an alternative.
	reg *console.Registry
	// configErr, when set, makes every MySQL/MariaDB Trigger refuse with it. A misconfigured
	// lock mode must disable BASELINES, never the daemon: under `watch` this
	// process is also the capture plane, and refusing to boot over a baseline
	// setting would turn a typo into permanently lost events. Same reasoning
	// as audit readability gating nothing in the capture path.
	configErr error

	// tableDeltas is --baseline-table-deltas (#1638): a refresh keeps a changed
	// table's file and writes the change beside it. Read by executeRefresh.
	tableDeltas bool

	mu   sync.Mutex
	jobs map[string]*console.BaselineStatus
	// restores tracks point-in-time restore jobs, keyed by server id —
	// a third kind alongside jobs (dumps) and refreshes, all sharing the
	// single-flight in busyLocked.
	restores map[string]*console.BaselineStatus
	// exports tracks custom .sql backup builds, keyed by server id — the
	// fourth job kind under the shared single-flight.
	exports map[string]*console.BaselineStatus
	// compacts is the table-delta compaction job's slot (#1723), one per
	// server, sharing the single-flight: it reads the chain a refresh would
	// extend and a full backup would replace.
	compacts map[string]*console.BaselineStatus
	// exportRuns is each server's CURRENT build: its directory (unique per
	// build; see sqlExportRoot for why builds never share a path), the
	// downloads streaming it, and the removal it is owed.
	exportRuns map[string]*sqlExportRun
	// exportOrphans is, per server id, every previous build under that
	// server's staging root that the pre-build wipe could not remove, by
	// path, with the last error. The trigger replaced the entry that owed
	// each one its retry, so this is where the reaper, the Storage card and
	// the new build's StagingError find it until the removal succeeds.
	exportOrphans map[string]map[string]string
	// now is the sql-export lifecycle's clock (nil = time.Now); tests inject
	// one to cross the download TTL without waiting for it.
	now func() time.Time
	// exportReapEvery overrides sqlExportReapEvery (zero = the constant);
	// tests shorten it to drive the reaper loop.
	exportReapEvery time.Duration
	// history, when non-nil, records every finished run (dump/refresh/
	// restore) so the backups page can report exact durations. Failures to
	// save are logged, never returned: history must not fail a run.
	history *console.BaselineRunHistory

	// produce runs the production half of a full backup (mydumper → Parquet)
	// and hands the outcome to completeDump: execute in the daemon (set by
	// newBaselineSupervisor; nil falls back to it), a fake in the tests that
	// drive Trigger → run past the publish without a mydumper.
	produce func(console.BaselineRequest) (dumpOutcome, error)
	// uploading: snapshots this process is sending to a destination right
	// now, keyed "<server id>/<snapshot dir name>", so a sweep does not send
	// one a second time (#1725). Guarded by mu.
	uploading map[string]bool
	// refreshes tracks the PERIODIC refresh jobs (#1171), kept apart from jobs
	// so a manual dump cannot erase the evidence that the automatic refresh has
	// been failing. Both share the single-flight (busyLocked).
	refreshes map[string]*console.BaselineStatus
	// refreshPaces is what the previous PUBLISHED refresh of each server
	// measured, keyed by server id, and it exists so the overrun warning can
	// tell a refresh that is falling behind from one whose cost is a floor it
	// settles on. One run cannot tell them apart; see refreshPace (#1693).
	refreshPaces map[string]refreshPace
	// foldedMarks is what the last fold of each server saw and left behind
	// (#1689): how far the index had been written, and when the snapshot it
	// published is dated. A cycle whose marks match this, and whose published
	// snapshot is still inside the window the index can fold from, has nothing
	// to do and does not start.
	//
	// Written only by a cycle that read its mark AND finished clean — the error
	// it checks covers the fold, the snapshot listing and the upload alike, so an
	// upload failure leaves no memo even though a local snapshot was published.
	// Every omission costs at most one fold that applies nothing.
	//
	// A refusal leaves it alone on purpose: a capture gap or a schema change does
	// not move the index, so memoizing a refused cycle would silence the retry
	// AND the refusal with it, and the operator's only standing signal would
	// vanish.
	//
	// In memory, not on disk. The cost of losing it is one fold that applies
	// nothing after a daemon restart, which corrects itself; persisting it
	// would add a durable file whose staleness is a new failure of its own.
	foldedMarks map[string]foldMemo
	// refreshPrior is the status TriggerRefresh displaced when it claimed a
	// server's refresh slot, dropped by both of that cycle's normal exits; a
	// panicking cycle leaves it for the next TriggerRefresh to overwrite (#1689).
	//
	// It exists because the claim has to happen BEFORE the cycle knows whether
	// it has anything to do. Claiming is what makes the single-flight work —
	// busyLocked reads that slot — and it happens under s.mu, which the gate
	// cannot run under: the gate opens the index. So the cycle claims first,
	// and a cycle the gate then skips puts back what it displaced instead of
	// writing a terminal status for a run that never happened.
	refreshPrior map[string]*console.BaselineStatus
	// refreshGateSkips names the claim the #1689 gate last released for each
	// server, by the Since stamp TriggerRefresh wrote on it.
	//
	// It exists for one reader: the backup schedule watches the job it
	// dispatched and reads the outcome off the refresh slot, so a gated cycle —
	// which restores the slot instead of writing an outcome — looks to it like
	// a job whose end nobody saw, and it files a skip blaming another job for
	// taking the server. The skip is right and the reason is wrong, so what
	// this carries is the reason.
	refreshGateSkips map[string]string
	// gateEdge rate-limits the two things the #1689 gate says about itself, so
	// a condition that persists for months is one line a day per server rather
	// than one per cycle. Both are conditions, not events: "I cannot evaluate
	// this server" and "I am re-anchoring this backup and the fold keeps
	// refusing" are states, and a daemon at a five-minute interval would print
	// either of them 288 times a day. That is the shape that teaches an
	// operator to stop reading the log.
	gateEdge *notify.Edge
	// retainInForce reports the retention the operator has CONFIGURED, read
	// fresh per call so a console edit applies on the next cycle. nil on a
	// supervisor that does not run rotation, which means no policy cap and the
	// observed partitions as the only bound. See withinRetentionPolicy.
	retainInForce func() time.Duration
}

// newBaselineSupervisor builds a supervisor bound to the daemon context. The
// staging dir is created lazily per run. lockMode selects the MySQL dump's sync
// mode for every run this supervisor executes — see the field doc. Builds a
// previous process staged are swept here (sweepSQLExportStaging); the TTL
// reaper for this process's own builds is started by the caller
// (runSQLExportReaper), so a supervisor built for a test does not park a
// ticker goroutine.
func newBaselineSupervisor(ctx context.Context, stagingDir string, lockMode baseline.LockMode) *baselineSupervisor {
	sweepSQLExportStaging(stagingDir)
	s := &baselineSupervisor{
		ctx:              ctx,
		stagingDir:       stagingDir,
		lockMode:         lockMode,
		jobs:             make(map[string]*console.BaselineStatus),
		refreshes:        make(map[string]*console.BaselineStatus),
		refreshPaces:     make(map[string]refreshPace),
		foldedMarks:      make(map[string]foldMemo),
		refreshPrior:     make(map[string]*console.BaselineStatus),
		refreshGateSkips: make(map[string]string),
		gateEdge:         notify.NewEdge(notify.DefaultRepeatEvery),
		restores:         make(map[string]*console.BaselineStatus),
		exports:          make(map[string]*console.BaselineStatus),
		compacts:         make(map[string]*console.BaselineStatus),
		exportRuns:       make(map[string]*sqlExportRun),
		exportOrphans:    make(map[string]map[string]string),
	}
	s.produce = s.execute
	return s
}

// lockModeNow resolves the lock mode for THIS job: a value saved from the
// interface wins over the one this process started with, and an unreadable
// saved value falls back to it. The error is the boot misconfiguration that
// refuses MySQL dumps — cleared when a readable value is saved, which is the
// whole point of the setting being editable while the daemon runs.
func (s *baselineSupervisor) lockModeNow() (baseline.LockMode, error) {
	return effectiveLockMode(s.reg, s.lockMode, s.configErr)
}

// Trigger starts a baseline in the background; returns console.ErrBaselineRunning
// if one is already in flight for this server.
func (s *baselineSupervisor) Trigger(req console.BaselineRequest) error {
	// Scoped to MySQL/MariaDB: executePG anchors on pgoutput's own
	// consistent-point LSN and never consults lockMode, so refusing a
	// Postgres baseline over a MySQL-only knob would take away a working
	// button for a setting that cannot affect it.
	if _, err := s.lockModeNow(); err != nil && req.Flavor != console.FlavorPostgres {
		return err
	}
	s.mu.Lock()
	// Shared with the periodic refresh (#1171): a dump writing a new snapshot
	// while a refresh folds the newest one forward would leave the refresh
	// anchored on a snapshot being written underneath it.
	if s.busyLocked(req.ServerID) {
		s.mu.Unlock()
		return console.ErrBaselineRunning
	}
	s.jobs[req.ServerID] = &console.BaselineStatus{State: "running", Since: nowStamp()}
	s.mu.Unlock()

	slog.Info("baseline: starting in-process snapshot", "server", req.ServerName, "id", req.ServerID)
	go s.run(req)
	return nil
}

// Status returns a copy of the latest known job state (idle if never run here).
func (s *baselineSupervisor) Status(serverID string) console.BaselineStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	if st, ok := s.jobs[serverID]; ok {
		return *st
	}
	return console.BaselineStatus{State: "idle"}
}

func (s *baselineSupervisor) run(req console.BaselineRequest) {
	// The guard reads own at panic time (a closure, not a bound argument):
	// once the run has published, the map entry may belong to a later
	// backup, and the guard must then fail THIS run's entry, not that one.
	var own dumpOwn
	// recover() is called by the deferred closure itself: called one frame
	// deeper (inside recoverDumpJob) it would return nil and catch nothing.
	defer func() { s.recoverDumpJob(req, &own, recover()) }()
	started := time.Now().UTC()
	if req.Flavor == console.FlavorPostgres {
		// The PG producer uploads inside executePG and stamps the snapshot
		// server-side; it keeps the one-phase shape.
		stats, uploaded, err := s.executePG(req)
		s.finishDump(req, started, dumpOutcome{stats: stats}, nil, uploaded, 0, err)
		return
	}
	produce := s.produce
	if produce == nil {
		produce = s.execute
	}
	// Read BEFORE the dump: its anchor is stamped inside produce, and an
	// event indexed between a later read and that anchor would be applied
	// by the next update without being counted, reading the rate too slow,
	// which is the direction that cuts over to full backups (#1737). Read
	// earlier, the few events in between are counted and not applied.
	mark := s.dumpIndexMark(req)
	out, err := produce(req)
	out.indexMark = mark
	s.completeDump(req, started, out, err, &own)
}

// dumpIndexMark is the index's event high-water mark as a full backup
// starts, or zero when the request names no index or the index does not
// answer. Bounded like the window probe (windowProbeTimeout, dial included):
// it delays a full backup, and an index that has gone silent must cost
// seconds and a missing measurement, never the backup.
func (s *baselineSupervisor) dumpIndexMark(req console.BaselineRequest) uint64 {
	if req.IndexDSN == "" {
		return 0
	}
	ctx, cancel := context.WithTimeout(s.ctx, windowProbeTimeout)
	defer cancel()
	mark, known := readIndexMark(ctx, probeDSN(req.IndexDSN))
	if !known {
		return 0
	}
	return mark.events
}

// dumpOwn is what a full backup owns once it has published: its status
// entry (nil before the publish, when the map entry is still its own and
// the generic guard applies) and how far its upload got, so the panic guard
// can say what is true about the snapshot.
type dumpOwn struct {
	st *console.BaselineStatus
	// uploaded: this run's OWN upload returned nil; a later panic is in the
	// sweep of older snapshots, and the destination has this one.
	uploaded bool
}

// recoverDumpJob is recoverBaselineJob for the dump, which alone can be past
// its publish when it panics: the map entry then reads "succeeded" (or
// belongs to a later backup that claimed the free slot), and the generic
// guard would either leave Uploading set forever or fail the later backup's
// entry, freeing the slot under a running job. With own.st set, the guard
// fails this run's own entry, wherever it is, and only that — and only
// while that entry is still mid-upload: finishDump clears Uploading as its
// first status write, so a panic in its tail (a log argument, say) must
// not report a completed backup as failed while its files sit on disk and
// in the bucket. r is recover()'s result, taken by the deferred closure
// (see run).
func (s *baselineSupervisor) recoverDumpJob(req console.BaselineRequest, own *dumpOwn, r any) {
	if own == nil || own.st == nil {
		s.failPanickedJob(baselineJobDump, req.ServerID, req.ServerName, r)
		return
	}
	if r == nil {
		return
	}
	phase, what := "the upload", "the local snapshot is complete and the next full backup sends it"
	if own.uploaded {
		phase, what = "the sweep of older snapshots", "this run's own snapshot had already reached the destination; the next full backup sweeps again"
	}
	slog.Error(string(baselineJobDump)+": "+phase+" hit an internal error and stopped after the snapshot was published. Capture and the web interface keep "+
		"running; "+what+". Please report this with the stack recorded here.",
		"server", req.ServerName, "id", req.ServerID, "panic", r, "stack", string(debug.Stack()))
	s.mu.Lock()
	defer s.mu.Unlock()
	if !own.st.Uploading {
		// finishDump already wrote this run's terminal status; the panic
		// came after it. The log above is the record.
		return
	}
	own.st.State = "failed"
	own.st.Uploading = false
	own.st.LastError = fmt.Sprintf("internal error during %s: %v", phase, r)
	own.st.FinishedAt = nowStamp()
}

// dumpOutcome is what producing a full backup leaves behind for publication:
// the snapshot directory (persistent under the server's local directory, or
// staged under a temp dir when the destination is S3 only), its instant, and
// the cleanup that removes the staging.
type dumpOutcome struct {
	stats   baseline.Stats
	snapDir string
	at      time.Time
	// staged: snapDir lives under a temp dir removed by cleanup (S3-only).
	staged  bool
	cleanup func()
	// indexMark is the index's event high-water mark read before the dump
	// started (dumpIndexMark); zero when unknown.
	indexMark uint64
}

// completeDump is the second half of a full backup: publish, upload, record.
//
// With a local directory the snapshot IS published once it is complete on
// disk (#1725): the status says so and the server's job slot is free before
// the upload starts, so a scheduled refresh runs alongside the upload — it
// reads the local copy (resolveFoldSource) — instead of being skipped for as
// long as the copy takes to reach the destination. S3-only keeps the slot
// through the upload: its staging is temporary, and nothing is published
// until the destination has it.
//
// own receives the status entry this run owns once it has published (see
// finishDump) and how far its upload got; the caller's panic guard reads it.
func (s *baselineSupervisor) completeDump(req console.BaselineRequest, started time.Time, out dumpOutcome, err error, own *dumpOwn) {
	if err != nil {
		s.finishDump(req, started, out, nil, 0, 0, err)
		return
	}
	if out.cleanup != nil {
		defer out.cleanup()
	}
	if req.S3 == "" {
		s.finishDump(req, started, out, nil, 0, 0, nil)
		return
	}
	if req.LocalDir != "" {
		own.st = s.publishDump(req, out)
	}
	root := strings.TrimSuffix(req.S3, "/")
	name := reconstruct.SnapshotDirName(out.at)
	uploaded, err := s.uploadDump(req, out, root+"/"+name)
	swept := 0
	if err == nil {
		own.uploaded = true
		if req.LocalDir != "" {
			swept = s.sweepUnuploaded(req, root, name)
		}
	}
	s.finishDump(req, started, out, own.st, uploaded, swept, err)
}

// publishDump marks a full backup published: its snapshot is complete in the
// server's local directory, the upload is still to come, and the job slot is
// free (busyLocked reads "running" only).
//
// Returns the status entry this run owns: once the slot is free a later full
// backup may claim the map entry for itself, and this run's completion must
// then not write over it (finishDump).
func (s *baselineSupervisor) publishDump(req console.BaselineRequest, out dumpOutcome) *console.BaselineStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.dumpStatusLocked(req.ServerID)
	st.State = "succeeded"
	st.Published = true
	st.Uploading = true
	st.LastError = ""
	st.Tables = out.stats.TablesProcessed
	st.Rows = out.stats.RowsWritten
	st.FinishedAt = nowStamp()
	slog.Info("baseline: snapshot published locally; uploading it to the backup destination in the background",
		"server", req.ServerName, "id", req.ServerID, "snapshot", out.snapDir, "destination", req.S3)
	return st
}

func (s *baselineSupervisor) dumpStatusLocked(serverID string) *console.BaselineStatus {
	st := s.jobs[serverID]
	if st == nil { // defensive: never overwritten away under lock, but don't panic
		st = &console.BaselineStatus{}
		s.jobs[serverID] = st
	}
	return st
}

// uploadDump sends the new snapshot to the destination — exactly that
// snapshot, to its own key under the destination root, the way the refresh
// does; handing the uploader the local ROOT re-sent every snapshot on disk on
// every full backup (#1725: 3,822 objects for 13 new files) — and then, with a
// local directory, sweeps up any other complete local snapshot the
// destination lacks, which is what the refresh's failed-upload message
// promises will happen.
func (s *baselineSupervisor) uploadDump(req console.BaselineRequest, out dumpOutcome, dest string) (int, error) {
	uploaded, err := s.uploadMarked(req.ServerID, reconstruct.SnapshotDirName(out.at), out.snapDir, dest, false)
	if err != nil {
		if out.staged {
			// The staging is removed on return: naming it would send the
			// operator to a directory that no longer exists.
			return 0, fmt.Errorf("upload: %w", err)
		}
		return 0, fmt.Errorf("upload: the snapshot was written to %s but could not be uploaded to %s; the next full backup sends it: %w",
			out.snapDir, dest, err)
	}
	return uploaded, nil
}

// uploadMarked sends one snapshot directory with its in-flight mark held for
// exactly the duration of the upload. The release is DEFERRED: an upload
// that panics must not leave the mark behind, or every later sweep would
// skip that snapshot for as long as the daemon runs.
func (s *baselineSupervisor) uploadMarked(serverID, name, dir, dest string, skipExisting bool) (int, error) {
	release := s.markUploading(serverID, name)
	defer release()
	return uploadSnapshot(s.ctx, dir, dest, "", skipExisting)
}

// markUploading registers a snapshot this process is sending to the
// destination, so a sweep running alongside (this dump's, another dump's)
// does not send it a second time; the returned func unregisters it. Keyed by
// server and snapshot directory name.
func (s *baselineSupervisor) markUploading(serverID, name string) (release func()) {
	key := serverID + "/" + name
	s.mu.Lock()
	if s.uploading == nil {
		s.uploading = map[string]bool{}
	}
	s.uploading[key] = true
	s.mu.Unlock()
	return func() {
		s.mu.Lock()
		delete(s.uploading, key)
		s.mu.Unlock()
	}
}

func (s *baselineSupervisor) isUploading(serverID, name string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.uploading[serverID+"/"+name]
}

// sweepUnuploaded sends every complete local snapshot other than the one
// named own whose _SUCCESS the destination does not have, skipping objects
// already there and snapshots this process is sending right now (a refresh
// running alongside this upload sends its own). A probe or an upload that
// fails is logged and skipped: the full backup's own upload succeeded, and
// the next full backup sweeps again. Compared by directory NAME: the
// snapshot instant on disk has second resolution, the run's has nanoseconds.
func (s *baselineSupervisor) sweepUnuploaded(req console.BaselineRequest, root, own string) int {
	files, err := listBaselines(s.ctx, req.LocalDir)
	if err != nil {
		slog.Warn("baseline: could not list the local backups to sweep unuploaded snapshots", "server", req.ServerName, "error", err)
		return 0
	}
	seen := map[string]bool{own: true}
	swept := 0
	for _, f := range files {
		if s.ctx.Err() != nil {
			// Shutdown: what is left is the next full backup's, and one line
			// says so instead of one warning per snapshot.
			slog.Info("baseline: sweep of unuploaded snapshots interrupted by shutdown; the next full backup continues it", "server", req.ServerName)
			return swept
		}
		name := reconstruct.SnapshotDirName(f.SnapshotTime)
		if seen[name] {
			continue
		}
		seen[name] = true
		if s.isUploading(req.ServerID, name) {
			continue
		}
		if _, err := os.Stat(filepath.Join(req.LocalDir, name, baseline.SuccessMarker)); err != nil {
			// The listing admits a snapshot with NEITHER marker (written
			// before the markers existed) and the uploader refuses one
			// without _SUCCESS, so this one can never be sent: say so
			// instead of probing and failing on every full backup.
			slog.Info("baseline: a local snapshot has no _SUCCESS marker (written by an older build) and cannot be sent to the destination; re-create it to have it there",
				"server", req.ServerName, "snapshot", name)
			continue
		}
		dest := root + "/" + name
		present, err := s3ObjectPresent(s.ctx, dest+"/"+baseline.SuccessMarker)
		if err != nil {
			slog.Warn("baseline: could not tell whether the destination has a local snapshot; not swept this time (a HEAD on a missing key needs s3:ListBucket to answer 404 rather than 403)",
				"server", req.ServerName, "snapshot", name, "error", err)
			continue
		}
		if present {
			continue
		}
		n, err := s.uploadMarked(req.ServerID, name, filepath.Join(req.LocalDir, name), dest, true)
		if err != nil {
			slog.Warn("baseline: a local snapshot the destination lacks could not be sent; the next full backup tries again",
				"server", req.ServerName, "snapshot", name, "error", err)
			continue
		}
		slog.Info("baseline: sent a local snapshot the destination lacked", "server", req.ServerName, "snapshot", name, "uploaded", n)
		swept++
	}
	return swept
}

// finishDump records the run and writes the terminal status. A failed upload
// after a local publish is a failed run that KEEPS the snapshot (Published
// stays true): the local copy is what the schedule now reads, and the error
// names where it is and where it did not get to.
//
// own is the status entry a published run owns (nil before a publish, when
// the run still holds the slot and the map entry is its own). If a later
// full backup has since claimed the map entry — the slot was free — this
// run's outcome goes to its own detached entry and the log, never over the
// running one: writing "succeeded" there would free the slot under a job
// that is still running.
func (s *baselineSupervisor) finishDump(req console.BaselineRequest, started time.Time, out dumpOutcome, own *console.BaselineStatus, uploaded, swept int, err error) {
	rec := console.BaselineRunRecord{
		Kind: console.BaselineRunDump, Trigger: req.Trigger, StartedAt: started.Format(time.RFC3339),
		Tables: out.stats.TablesProcessed, Rows: out.stats.RowsWritten, Uploaded: uploaded,
		// The reason this was a full backup travels with the run (#1604):
		// recomputed later it would name whatever is true THEN.
		Why: req.Why, WhyCode: console.BackupWhyCode(req.Why),
	}
	// A snapshot instant is recorded when a snapshot was published: a
	// success, or a local publish whose upload failed.
	if !out.at.IsZero() && (err == nil || (out.snapDir != "" && !out.staged)) {
		rec.SnapshotTime = out.at.UTC().Format(time.RFC3339)
	}
	// The base the next update from this snapshot counts its events from
	// (#1737). Only on a success, the only record IndexMarkFor reads.
	if err == nil && rec.SnapshotTime != "" {
		rec.IndexMark = out.indexMark
	}
	s.recordRun(req.ServerID, req.ServerName, rec, err)

	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.dumpStatusLocked(req.ServerID)
	if own != nil && st != own {
		slog.Info("baseline: a later full backup took over this server's status while the upload ran; this run's outcome is recorded in the history only",
			"server", req.ServerName, "id", req.ServerID, "snapshot", out.snapDir)
		st = own
	}
	st.FinishedAt = nowStamp()
	st.Uploading = false
	if err != nil {
		st.State = "failed"
		st.LastError = err.Error()
		if st.Published {
			if errors.Is(err, context.Canceled) {
				// A routine restart, not a lost backup: the local snapshot is
				// complete and the next full backup sends it.
				slog.Warn("baseline: the upload was interrupted by daemon shutdown; the local snapshot is complete and the next full backup sends it",
					"server", req.ServerName, "id", req.ServerID, "snapshot", out.snapDir)
				return
			}
			slog.Error("baseline: the snapshot was written but not sent to the backup destination",
				"server", req.ServerName, "id", req.ServerID, "snapshot", out.snapDir, "error", err)
			return
		}
		slog.Error("baseline: snapshot failed", "server", req.ServerName, "id", req.ServerID, "error", err)
		return
	}
	st.State = "succeeded"
	st.LastError = ""
	st.Tables = out.stats.TablesProcessed
	st.Rows = out.stats.RowsWritten
	st.Uploaded = uploaded
	st.Swept = swept
	st.Published = st.Published || out.snapDir != ""
	slog.Info("baseline: snapshot complete", "server", req.ServerName, "id", req.ServerID,
		"tables", out.stats.TablesProcessed, "rows", out.stats.RowsWritten, "uploaded", uploaded, "swept", swept)
}

// s3ObjectPresent reports whether one object exists at an s3:// URL — the
// sweep's "does the destination have this snapshot" probe on its _SUCCESS.
// A variable so tests answer it without S3.
var s3ObjectPresent = func(ctx context.Context, url string) (bool, error) {
	bucket, key, err := storage.ParseS3URL(url)
	if err != nil {
		return false, err
	}
	client, err := storage.NewS3ClientForBucket(ctx, bucket, "")
	if err != nil {
		return false, err
	}
	return storage.S3ObjectExists(ctx, client, bucket, key)
}

// execute runs the production half of a full backup: mydumper → baseline.Run.
// For a local-dir destination the Parquet is written there persistently; for
// an S3-only destination it is staged under a fresh temp dir that the returned
// cleanup removes once the upload is done. The upload itself is completeDump's.
func (s *baselineSupervisor) execute(req console.BaselineRequest) (dumpOutcome, error) {
	if err := os.MkdirAll(s.stagingDir, 0o755); err != nil {
		return dumpOutcome{}, fmt.Errorf("create staging dir: %w", err)
	}

	dumpDir, err := os.MkdirTemp(s.stagingDir, "dump-")
	if err != nil {
		return dumpOutcome{}, fmt.Errorf("create dump dir: %w", err)
	}
	defer os.RemoveAll(dumpDir)

	// Captured immediately before invoking mydumper: since this pipeline runs
	// mydumper and baseline.Run in the same process, we can pass our own UTC
	// wall-clock time straight through as the snapshot anchor instead of
	// letting baseline.Run re-parse mydumper's "Started dump at" metadata
	// line — which is written in the dump host's LOCAL time and would
	// otherwise be misread as UTC verbatim, skewing the replay window by the
	// host's UTC offset (#768).
	dumpStartedAt := time.Now().UTC()
	// Resolved HERE, not at boot: a lock mode saved from the interface governs
	// the very next dump. Trigger already refused an unreadable one, so the
	// error is spent — taking the mode alone keeps this call site to one line.
	lockMode, _ := s.lockModeNow()
	if err := runMydumper(s.ctx, req.SourceDSN, req.Schemas, dumpDir, lockMode); err != nil {
		return dumpOutcome{}, fmt.Errorf("dump: %w", err)
	}
	// A dump that cannot be anchored is refused here, never published (#1688).
	// mydumper exits 0 with no position in its metadata when binary logging is
	// off, when the dump user cannot read it, and (measured) for a build older
	// than 0.16.3 against MySQL 8.4; baseline.Run would convert that dump with a
	// warning at most, and the next update from it would fall back to
	// timestamps. Metadata that cannot be read at all is refused the same way,
	// where baseline.Run only logs at Info: this daemon is unattended.
	if err := baseline.RequireDumpPosition(dumpDir); err != nil {
		if errors.Is(err, baseline.ErrDumpNotAnchored) {
			return dumpOutcome{}, fmt.Errorf("dump: %w", err)
		}
		return dumpOutcome{}, fmt.Errorf("dump: cannot read mydumper's metadata, so the backup cannot be anchored to a binlog position: %w", err)
	}

	out := dumpOutcome{at: dumpStartedAt, cleanup: func() {}}
	outputDir := req.LocalDir
	if outputDir == "" { // S3-only: stage, upload, discard the staging
		outputDir, err = os.MkdirTemp(s.stagingDir, "baseline-")
		if err != nil {
			return dumpOutcome{}, fmt.Errorf("create baseline staging dir: %w", err)
		}
		out.staged = true
		out.cleanup = func() { os.RemoveAll(outputDir) }
	}

	stats, err := baseline.Run(s.ctx, baseline.Config{
		InputDir:    dumpDir,
		OutputDir:   outputDir,
		Compression: "zstd",
		Timestamp:   dumpStartedAt,
		TableDeltas: s.tableDeltas,
	})
	if err != nil {
		out.cleanup()
		return dumpOutcome{}, fmt.Errorf("convert: %w", err)
	}
	out.stats = stats
	out.snapDir = filepath.Join(outputDir, reconstruct.SnapshotDirName(dumpStartedAt))
	return out, nil
}

// executePG produces a PostgreSQL baseline in-process via internal/pgbaseline —
// COPY straight to Parquet, anchored at the slot's consistent-point LSN. No
// mydumper subprocess and no #768 timestamp skew: pgbaseline self-stamps the
// snapshot time from the database's own now(). Destination handling mirrors
// execute(): a local dir is written persistently; S3-only stages in a temp dir,
// uploads via the same source-agnostic baseline.Upload, and discards the staging.
func (s *baselineSupervisor) executePG(req console.BaselineRequest) (baseline.Stats, int, error) {
	if err := os.MkdirAll(s.stagingDir, 0o755); err != nil {
		return baseline.Stats{}, 0, fmt.Errorf("create staging dir: %w", err)
	}
	outputDir := req.LocalDir
	if outputDir == "" { // S3-only: stage then upload, discard staging
		var err error
		outputDir, err = os.MkdirTemp(s.stagingDir, "pgbaseline-")
		if err != nil {
			return baseline.Stats{}, 0, fmt.Errorf("create baseline staging dir: %w", err)
		}
		defer os.RemoveAll(outputDir)
	}

	cfg, err := pgBaselineConfig(req, outputDir)
	if err != nil {
		return baseline.Stats{}, 0, err
	}
	pgStats, err := pgbaseline.Run(s.ctx, cfg)
	if err != nil {
		return baseline.Stats{}, 0, fmt.Errorf("pg baseline: %w", err)
	}

	var uploaded int
	if req.S3 != "" {
		uploaded, err = baseline.Upload(s.ctx, outputDir, req.S3, "", false)
		if err != nil {
			return baseline.Stats{}, 0, fmt.Errorf("upload: %w", err)
		}
	}
	return baseline.Stats{
		TablesProcessed: pgStats.TablesProcessed,
		RowsWritten:     pgStats.RowsWritten,
		FilesWritten:    pgStats.FilesWritten,
	}, uploaded, nil
}

// pgBaselineConfig builds the pgbaseline.Config for a PG source, mirroring
// cmd/bintrail-pg's pgBaselineConfigFromFlags. The replication DSN is derived
// from the stored query DSN (console.PGReplDSN — the one home for that
// derivation), needed so pgbaseline can CREATE the slot when a user baselines
// BEFORE the first monitor start; harmless if the slot already exists. Pure —
// unit-testable without a live PG. The registry carries only a schema filter.
func pgBaselineConfig(req console.BaselineRequest, outputDir string) (pgbaseline.Config, error) {
	replDSN, err := console.PGReplDSN(req.SourceDSN)
	if err != nil {
		return pgbaseline.Config{}, err
	}
	return pgbaseline.Config{
		QueryDSN:    req.SourceDSN,
		ReplDSN:     replDSN,
		SlotName:    req.Slot,
		Publication: req.Publication,
		Filters:     cliutil.BuildIndexFilters(strings.Join(req.Schemas, ","), ""),
		OutputDir:   outputDir,
		Compression: "zstd",
	}, nil
}

// mydumperPlan is what the mydumper on this host can do for one lock mode
// (#1688). The console used to pass --sync-thread-lock-mode unconditionally, so
// on a host whose mydumper came from the distribution (Ubuntu 24.04 packages
// 0.10.1, which prints "mydumper 0.10.0") every scheduled full backup and every
// Create backup died on "Unknown option", once per slot, while capture kept the
// daemon looking healthy.
type mydumperPlan struct {
	path          string // the binary probed; the dump runs this same file
	sendLockFlags bool   // --sync-thread-lock-mode and --trx-tables
	preflight     bool   // run the privilege preflight before launching
	fallback      string // non-empty: the dump proceeds without the flags; log why
	// version is what --version printed, when it could be read. runMydumper
	// uses it to refuse, before any lock is taken, a build that cannot record
	// the binlog position on the source's MySQL version.
	version      mydumperlock.Version
	versionKnown bool
}

// sourceServerVersion reads SELECT VERSION() from the source. A seam, so the
// tests need no server.
var sourceServerVersion = func(ctx context.Context, dsn string) (string, error) {
	db, err := config.Connect(dsn)
	if err != nil {
		return "", err
	}
	defer db.Close()
	qctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	var v string
	err = db.QueryRowContext(qctx, "SELECT VERSION()").Scan(&v)
	return v, err
}

// planMydumper reads the version of the mydumper on PATH and decides, the way
// `bintrail dump` does (#219, #460, #1686), what this run can ask of it.
//
// The CLI refuses an EXPLICIT --lock-mode it cannot send and falls back for the
// default. The console has no flag; its mode is ftwrl unless
// BINTRAIL_CONSOLE_BASELINE_LOCK_MODE names another. The split here is by what
// the mode MEANS rather than by where it came from: a build below 0.18 takes
// FTWRL by default (its --help: "--lock-all-tables  Use LOCK TABLE for all,
// instead of FTWRL"), so dropping the flag keeps ftwrl's kind of lock. How long
// that build holds it is its own business and was not measured here, and
// without --trx-tables it no longer refuses a non-transactional table. What it
// can get wrong silently, a dump with no binlog position, is refused twice: by
// runMydumper before the dump when the build is older than 0.16.3 and the
// source is MySQL 8.4 or newer (the case measured), and by
// baseline.RequireDumpPosition in execute after any dump. For any other mode, dropping the
// flag would dump under a lock the operator did not choose, which is the silent
// wrong answer the CLI's refusal exists to prevent, so the run refuses and
// names the remedy.
func planMydumper(lockMode baseline.LockMode) (mydumperPlan, error) {
	path, err := exec.LookPath("mydumper")
	if err != nil {
		return mydumperPlan{}, fmt.Errorf("mydumper is not installed where DBTrail can run it (%v). "+
			"Full backups of MySQL and MariaDB servers run the mydumper on the PATH of the DBTrail process; "+
			"install mydumper %s or newer, which supports every lock mode",
			err, mydumperlock.LockModeFloor)
	}
	v, verErr := mydumperlock.ProbeVersion(path)
	ftwrl := lockMode == baseline.LockModeFTWRL
	switch {
	case errors.Is(verErr, mydumperlock.ErrNotRunnable):
		// #1699: a binary that does not run is not an "unknown version", and
		// routing it there would make the first error a privilege refusal.
		return mydumperPlan{}, fmt.Errorf("%w. Full backups run that same binary, so fix or replace it with mydumper %s or newer",
			verErr, mydumperlock.LockModeFloor)
	case verErr != nil:
		if !ftwrl {
			return mydumperPlan{}, fmt.Errorf("lock mode %s cannot be used: the version of %s could not be read (%v), "+
				"so DBTrail cannot tell whether it accepts --sync-thread-lock-mode. "+
				"Install mydumper %s or newer, or remove BINTRAIL_CONSOLE_BASELINE_LOCK_MODE to back up with ftwrl",
				lockMode, path, verErr, mydumperlock.LockModeFloor)
		}
		return mydumperPlan{path: path, sendLockFlags: false, preflight: true,
			fallback: fmt.Sprintf("could not read the mydumper version (%v), so the dump runs without "+
				"--sync-thread-lock-mode and --trx-tables, after the privilege check", verErr)}, nil
	case !v.SupportsLockMode():
		if !ftwrl {
			return mydumperPlan{}, fmt.Errorf("lock mode %s needs mydumper %s or newer, and %s is mydumper %s, "+
				"which does not accept --sync-thread-lock-mode. Install mydumper %s or newer (distribution packages are often older), "+
				"or remove BINTRAIL_CONSOLE_BASELINE_LOCK_MODE to back up with ftwrl, which this build uses by default",
				lockMode, mydumperlock.LockModeFloor, path, v, mydumperlock.LockModeFloor)
		}
		fallback := fmt.Sprintf("mydumper %s is older than %s, so the dump runs without --sync-thread-lock-mode "+
			"and --trx-tables and takes that build's own FTWRL", v, mydumperlock.LockModeFloor)
		if v.Less(mydumperlock.PositionFloor) {
			fallback += fmt.Sprintf("; against MySQL 8.4 and newer a build older than %s cannot record the binlog position, "+
				"so backups of those sources are refused before they start", mydumperlock.PositionFloor)
		}
		// The #800 privilege check is skipped only for a build that takes no
		// backup lock (measured: the packaged 0.10 does not; 0.16.3 and 1.0.3
		// do). Skipping it for every pre-0.18 build would switch the check off
		// for a whole band that DOES take the lock, and that check exists
		// because the failure it prevents is a segfault.
		return mydumperPlan{path: path, sendLockFlags: false,
			preflight: v.TakesBackupLock() && lockMode.NeedsElevatedPrivileges(),
			fallback:  fallback, version: v, versionKnown: true}, nil
	default:
		return mydumperPlan{path: path, sendLockFlags: true, preflight: lockMode.NeedsElevatedPrivileges(),
			version: v, versionKnown: true}, nil
	}
}

// mydumperBootWarning is the startup line for a daemon that may take full
// backups (#1688): the verdict every run will reach, said once before the first
// slot as well as by each run. Empty when the local mydumper accepts the
// configured mode as is.
func mydumperBootWarning(lockMode baseline.LockMode) string {
	plan, err := planMydumper(lockMode)
	switch {
	case err != nil:
		return "full backups of MySQL and MariaDB servers will fail until this is fixed: " + err.Error()
	case plan.fallback != "":
		return "full backups of MySQL and MariaDB servers will run, but " + plan.fallback
	default:
		return ""
	}
}

// runMydumper invokes the mydumper on the PATH against the source DSN, writing a
// dump (with binlog coordinates in its metadata, which baseline.Run reads) into
// dumpDir. In the console image that is the pinned build the compose
// baseline-dump pipeline also uses; on a native install it is whatever the host
// has, which is why planMydumper reads its version first (#1688). lockMode
// selects the sync mode when the build accepts it; see buildConsoleMydumperArgs.
func runMydumper(ctx context.Context, sourceDSN string, schemas []string, dumpDir string, lockMode baseline.LockMode) error {
	host, port, user, password, err := config.ParseSourceDSN(sourceDSN)
	if err != nil {
		return err
	}

	// Probed on every run, not once at boot: the fix for an old or broken
	// mydumper is to install another one, and that has to take effect without
	// restarting the process that also captures changes. One exec of
	// --version costs nothing next to a dump.
	plan, err := planMydumper(lockMode)
	if err != nil {
		return err
	}
	if plan.fallback != "" {
		slog.Warn("baseline: "+plan.fallback, "mydumper", plan.path, "lock_mode", string(lockMode))
	}
	// A build older than 0.16.3 exits 0 against MySQL 8.4 with no position in
	// its metadata (measured). execute refuses that dump afterwards; refusing
	// here instead spares the source a full dump under FTWRL that would be
	// thrown away. A source whose version cannot be read goes ahead: the
	// after-dump check still guards it.
	if plan.versionKnown && plan.version.Less(mydumperlock.PositionFloor) {
		sv, verr := sourceServerVersion(ctx, sourceDSN)
		if verr != nil {
			// The dump still runs and its own metadata is checked afterwards,
			// but that check comes AFTER a full dump: say why the cheap
			// refusal could not be made instead of dropping it in silence.
			slog.Warn("console backup: could not read the source server's version, so an old mydumper cannot be refused before it dumps",
				"mydumper", plan.version.String(), "error", verr)
		}
		if verr == nil && !plan.version.RecordsPositionOn(sv) {
			return fmt.Errorf("mydumper %s cannot record the binlog position on MySQL %s: builds older than %s read it "+
				"with SHOW MASTER STATUS, which MySQL 8.4 removed, so the backup would be refused after a full dump and "+
				"the dump was not started. Install mydumper %s or newer",
				plan.version, sv, mydumperlock.PositionFloor, mydumperlock.LockModeFloor)
		}
	}

	if lockMode.NeedsElevatedPrivileges() {
		// Hard gate, unlike the NO_LOCK warning below: granting BACKUP_ADMIN
		// without RELOAD/FLUSH_TABLES does not fail cleanly in mydumper — it
		// SEGFAULTS (verified against the pinned build, #800). Skipped ONLY for
		// a build READ as older than mydumperlock.LockInstanceExemptBelow:
		// requiresBackupAdmin decides from the SERVER's version, so demanding
		// BACKUP_ADMIN from a 0.10 build, which takes no backup lock at all,
		// would refuse a dump that works — while 0.16 and 0.17 DO take it and
		// keep the gate. An unreadable version keeps the gate too.
		if plan.preflight {
			if err := checkMydumperPrivileges(ctx, sourceDSN, lockMode, mydumperlock.RemedyConsole, schemas); err != nil {
				return err
			}
		}
	} else if lockMode == baseline.LockModeNoLock {
		// Only for no-lock. safe-no-lock reaches this branch too, but it
		// ABORTS on thread skew instead of writing it, so warning about
		// cross-table inconsistency there would cry wolf about the one
		// low-privilege mode that cannot produce it.
		// Best-effort, advisory only — never blocks or fails the dump. See
		// warnIfMultiTableNoLock.
		warnIfMultiTableNoLock(ctx, sourceDSN, schemas)
	}

	args := buildConsoleMydumperArgs(host, port, user, schemas, dumpDir, lockMode, plan.sendLockFlags)
	// plan.path, not the bare name: the dump must run the very binary whose
	// version was just read.
	cmd := exec.CommandContext(ctx, plan.path, args...)
	// Deliver the source password out of band via MYSQL_PWD (honored by the
	// MySQL client library mydumper links against) so it never lands on argv,
	// where it would be world-readable in `ps aux` / /proc/<pid>/cmdline. The
	// child's /proc/<pid>/environ is mode 0400 (#811).
	if password != "" {
		cmd.Env = append(os.Environ(), "MYSQL_PWD="+password)
	}
	out, err := cmd.CombinedOutput()
	if err != nil {
		if msg := strings.TrimSpace(string(out)); msg != "" {
			return fmt.Errorf("mydumper failed: %w; output: %s", err, msg)
		}
		return fmt.Errorf("mydumper failed: %w", err)
	}
	return nil
}

// systemSchemaExcludeRegex dumps every USER schema but excludes the MySQL system
// schemas, matching the compose baseline-dump pipeline (#612). A least-privilege
// capture user (REPLICATION + SELECT, no SHOW VIEW) cannot read the sys views, so
// an unfiltered mydumper dies with "SHOW VIEW command denied … sys.host_summary";
// the system schemas are useless as a baseline anyway. mydumper uses PCRE, so the
// negative lookahead drops a system db both bare and as <db>.<table>.
const systemSchemaExcludeRegex = `^(?!(mysql|sys|performance_schema|information_schema)($|\.))`

// buildConsoleMydumperArgs builds the mydumper argument slice for the console's
// in-process dump. It mirrors `bintrail dump` / the compose baseline-dump
// invocation for the shared flags; lockMode picks --sync-thread-lock-mode
// (#800, #1377) when sendLockFlags is true. internal/baseline.LockMode carries
// the measured comparison of the three modes; the two consequences specific to
// THIS call site, both about a build that receives the flags:
//
//   - EVERY point-consistent mode covers TRANSACTIONAL tables only — LOCK_ALL
//     exactly as much as FTWRL, verified for each — and --trx-tables makes
//     mydumper REFUSE the whole dump when it finds a non-transactional one
//     ("Non transactional table found ... Restart backup using
//     --trx-tables=0"), which the console propagates as the run's error. The
//     same flag under NO_LOCK only warns and proceeds — verified empirically
//     on the identical MyISAM table (#800). The refusal is gated to an actual
//     "consistent backup attempt" in mydumper's own wording, which NO_LOCK is
//     explicitly not making. So this is NOT a reason to move an RDS source off
//     LOCK_ALL: switching modes among the consistent ones cannot avoid it.
//   - FTWRL needs RELOAD/FLUSH_TABLES on every flavor, plus BACKUP_ADMIN on
//     MySQL/Percona 8.0+ (for LOCK INSTANCE FOR BACKUP). Granting BACKUP_ADMIN
//     WITHOUT RELOAD does not fail cleanly — the pinned build SEGFAULTS — which
//     is why mydumperlock.CheckPrivileges runs first and never lets mydumper
//     attempt it half-privileged. The one fallback, a build that cannot take
//     the flags, is planMydumper's, and it is logged.
//
// sendLockFlags is false when the mydumper is older than 0.18.1, which rejects
// both --sync-thread-lock-mode and --trx-tables with "Unknown option", or when
// its version could not be read and the mode is ftwrl (#1688); see
// planMydumper for when the dump may proceed without them.
func buildConsoleMydumperArgs(host string, port uint16, user string, schemas []string, dumpDir string, lockMode baseline.LockMode, sendLockFlags bool) []string {
	args := []string{
		"--host", host,
		"--port", strconv.Itoa(int(port)),
		"--user", user,
		"--threads", "4",
		"--compress-protocol",
		"--complete-insert",
	}
	if sendLockFlags {
		args = append(args, "--sync-thread-lock-mode", lockMode.MydumperValue(), "--trx-tables")
	}
	switch {
	case len(schemas) == 1:
		args = append(args, "--database", schemas[0])
	case len(schemas) > 1:
		args = append(args, "--regex", "^("+strings.Join(schemas, "|")+")\\.")
	default:
		args = append(args, "--regex", systemSchemaExcludeRegex)
	}
	// --outputdir last: docker wrapper scripts read the last arg for the mount.
	args = append(args, "--outputdir", dumpDir)
	return args
}

// dumpableTableCountQuery builds the information_schema.TABLES COUNT(*) query and
// its args, approximating buildConsoleMydumperArgs' own schema selection (single
// schema, an explicit list, or every non-system schema when none is given). Used
// only by warnIfMultiTableNoLock below — an advisory approximation, not a
// guarantee: a concurrent DDL between this query and mydumper's own table
// discovery could disagree, and that is fine, since the warning is advisory, not
// a correctness gate. Pure and unit-testable without a live database.
func dumpableTableCountQuery(schemas []string) (string, []any) {
	const base = "SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND "
	if len(schemas) == 0 {
		return base + "TABLE_SCHEMA NOT IN ('mysql','sys','performance_schema','information_schema')", nil
	}
	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(schemas)), ",")
	args := make([]any, len(schemas))
	for i, s := range schemas {
		args[i] = s
	}
	return base + "TABLE_SCHEMA IN (" + placeholders + ")", args
}

// warnIfMultiTableNoLock logs an advisory warning when a no-lock dump is about
// to span more than one table, pointing operators back at the point-consistent
// default and the docs (#800). It is best-effort: opening the source or running
// the count query never fails or delays the dump — an error here is logged at
// Debug and swallowed, since this is a UX nudge, not a correctness gate.
//
// Called ONLY for no-lock, not for safe-no-lock: the latter aborts on thread
// skew instead of writing it, so warning there would cry wolf about the one
// low-privilege mode that cannot produce the condition this describes.
func warnIfMultiTableNoLock(ctx context.Context, sourceDSN string, schemas []string) {
	db, err := config.Connect(sourceDSN)
	if err != nil {
		slog.Debug("baseline: could not open source to check table count for the NO_LOCK skew warning", "error", err)
		return
	}
	defer db.Close()

	count, err := countDumpableTables(ctx, db, schemas)
	if err != nil {
		slog.Debug("baseline: could not count tables for the no-lock skew warning", "error", err)
		return
	}
	if count > 1 {
		slog.Warn("baseline: dumping multiple tables under no-lock — each table's snapshot is "+
			"anchored at a slightly different instant (no cross-table synchronization barrier), so a multi-table "+
			"reconstruct (e.g. a parent/child FK pair) can be mutually inconsistent; set "+
			"BINTRAIL_CONSOLE_BASELINE_LOCK_MODE=lock-all for a point-consistent snapshot (needs only LOCK TABLES, and "+
			"is the mode that works on managed MySQL such as RDS), or unset it for the ftwrl default (requires the "+
			"RELOAD or FLUSH_TABLES privilege; MySQL/Percona 8.0+ also requires BACKUP_ADMIN, which RDS will not grant "+
			"and which MariaDB and MySQL 5.7 do not have) — see docs/dump-and-baseline.md",
			"tables", count)
	}
}

// countDumpableTables runs dumpableTableCountQuery against db and returns the
// result.
func countDumpableTables(ctx context.Context, db *sql.DB, schemas []string) (int, error) {
	query, args := dumpableTableCountQuery(schemas)
	var count int
	if err := db.QueryRowContext(ctx, query, args...).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

// nowStamp is the RFC3339 timestamp used in job status fields.
func nowStamp() string { return time.Now().UTC().Format(time.RFC3339) }

// recordRun appends one finished run to the history (no-op without one).
// finishedAt is stamped here so every producer records the same clock.
func (s *baselineSupervisor) recordRun(serverID, serverName string, rec console.BaselineRunRecord, runErr error) {
	if s.history == nil {
		return
	}
	rec.ServerID = serverID
	rec.ServerName = serverName
	rec.FinishedAt = time.Now().UTC().Format(time.RFC3339)
	if runErr != nil {
		rec.Error = runErr.Error()
	}
	if err := s.history.Append(rec); err != nil {
		slog.Warn("baseline history: could not record run (durations for this snapshot will fall back to file timestamps)",
			"server", serverName, "kind", rec.Kind, "error", err)
	}
}

// publishedSnapshotTime is the SnapshotTime a fold run records: the anchor on
// success, empty on failure — publication is all-or-nothing, so a failed fold
// has no snapshot for the history to name.
func publishedSnapshotTime(at time.Time, err error) string {
	// foldPublished, not err == nil: an upload failure leaves the snapshot on
	// disk at this very anchor (#1539), and a history record that named none
	// would describe the run as having produced nothing when the operator can
	// still restore from it.
	if !foldPublished(err) {
		return ""
	}
	return at.UTC().Format(time.RFC3339)
}
