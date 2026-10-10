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
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/cliutil"
	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/doctor"
	"github.com/dbtrail/dbtrail/internal/mydumperlock"
	"github.com/dbtrail/dbtrail/internal/notify"
	"github.com/dbtrail/dbtrail/internal/pgbaseline"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/rotation"
	"github.com/dbtrail/dbtrail/internal/serverid"
	"github.com/dbtrail/dbtrail/internal/storage"
)

// checkMydumperPrivileges is mydumperlock.CheckPrivileges behind a seam, so a
// test can observe the LOCK MODE this call site actually forwards. Without it
// the argument is unverifiable: every mode fails identically against an
// unreachable source, so hardcoding one here passes the whole suite while
// re-introducing #1381 — an operator who selected lock-all gets judged against
// ftwrl's requirements and told to grant BACKUP_ADMIN, which RDS refuses.
//
// It connects with the source's TLS settings (connectSource), so a server
// that only accepts encrypted connections is checked over TLS, as capture
// reaches it (#1996).
var checkMydumperPrivileges = func(ctx context.Context, sourceDSN string, ssl config.SSL, mode baseline.LockMode, remedy mydumperlock.Remedy, schemas []string) error {
	return mydumperlock.CheckPrivilegesWith(ctx, func() (*sql.DB, error) { return connectSource(sourceDSN, ssl) }, mode, remedy, schemas)
}

// baselineSupervisor implements console.BaselineController by running the
// dump→convert→upload pipeline IN-PROCESS (#613): the console image bundles
// mydumper, so a baseline never starts a sibling container and the daemon never
// mounts the docker socket. One job at a time per server, tracked in-memory —
// the durable record is the snapshot itself (listed by /api/baselines).
type baselineSupervisor struct {
	ctx        context.Context // daemon lifecycle; cancels an in-flight dump on shutdown
	stagingDir string          // base dir for temp dump + staged Parquet (S3-destined runs)

	// The cached answer of stagedUpdatesRefusal (#2212), so the schedule's
	// gates, read on every page load, do not probe the disk each time.
	stagingMu      sync.Mutex
	stagingChecked time.Time
	stagingErr     error

	// lockMode selects how mydumper synchronizes its worker threads onto one
	// instant for MySQL/MariaDB dumps — see internal/baseline.LockMode for the
	// measured trade-offs. Defaults to baseline.DefaultLockMode (FTWRL): a
	// baseline is the seed state reconstruct merges deltas onto, so a snapshot
	// that can be torn must be asked for, never landed on (#1377). Set via
	// BINTRAIL_CONSOLE_BASELINE_LOCK_MODE. No effect on PostgreSQL baselines
	// (executePG uses pgoutput's own consistent-point LSN unconditionally).
	lockMode baseline.LockMode
	// lockModeChosen says an operator set lockMode (the variable was not
	// empty). Without it the mode is automatic (#1986): each dump picks from
	// its source host, see lockModeFor.
	lockModeChosen bool
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
	// sqlChainLine is the console's live SQL-on-the-copy line (#2210); a
	// refresh ends a table's chain at half of it. Unset: no such rule (a
	// supervisor without a console, as in most tests). Atomic: watch starts
	// the refresh loop before the console exists and sets it after.
	sqlChainLine atomic.Pointer[func() int64]

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
	// compactRetry is when a server's compaction job may be tried again after
	// a run that failed (compactRetryEvery). Guarded by mu.
	compactRetry map[string]time.Time
	// resolving is the snapshot folders whose resolved pairs are being
	// written (#2231, maybeResolve), by absolute path. Not a job slot: it
	// only keeps two runs from writing the same pairs. Guarded by mu.
	resolving map[string]bool
	// resolveRetry is when a table whose pair could not be written may be
	// tried again, keyed by resolveRetryKey. Guarded by mu.
	resolveRetry map[string]time.Time
	// postRefresh counts the goroutines that run what follows a refresh
	// (the compaction's scan, the resolved pairs), which outlive the
	// refresh's own status: a test waits on it before it restores a seam
	// they read.
	postRefresh sync.WaitGroup
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
	// jobsDir is where each running job keeps its lock file (#2180),
	// beside the history whose journal points at them. Empty: no journal,
	// and nothing a killed job leaves is ever reclaimed. Set with history.
	jobsDir string

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
	// refreshChecked is when a refresh cycle for each server last ENDED, whether
	// it folded or the gate skipped it (#1705), as RefreshStatus reports it.
	//
	// It exists because a skipped cycle writes no run record and no status, on
	// purpose, so on the daemon-wide interval loop a server with nothing to do
	// and a loop that died three weeks ago showed the same last run. This says
	// which one it is without adding an outcome: it is a time, and nothing
	// reads it to decide anything.
	//
	// Kept beside the status slots rather than on them. A skipped cycle
	// restores the slot it displaced, and that status has to come back as it
	// was.
	//
	// None of the per-server maps on this supervisor is pruned when a server is
	// deleted from the registry, these four gate maps included. Decided, not
	// overlooked (#1705): each holds one small entry per server id this process
	// has ever seen, so the growth is bounded by the number of servers an
	// operator creates between restarts. A memo left behind cannot speak for a
	// new server either, because it names the index and the destination it was
	// read for, and a new server gets a new id.
	refreshChecked map[string]string
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
	// rotationInForce is the whole of those settings, for what retainInForce
	// cannot say: whether the operator chose that retention, or each index
	// drops on its own (coverageRuleFor). nil too when this daemon's rotation
	// loop is not running. Both are set by followRotation.
	rotationInForce func() rotation.Settings
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
		refreshChecked:   make(map[string]string),
		gateEdge:         notify.NewEdge(notify.DefaultRepeatEvery),
		restores:         make(map[string]*console.BaselineStatus),
		exports:          make(map[string]*console.BaselineStatus),
		compacts:         make(map[string]*console.BaselineStatus),
		compactRetry:     make(map[string]time.Time),
		resolving:        make(map[string]bool),
		resolveRetry:     make(map[string]time.Time),
		exportRuns:       make(map[string]*sqlExportRun),
		exportOrphans:    make(map[string]map[string]string),
	}
	s.produce = s.execute
	return s
}

// lockModeNow resolves the lock mode an operator chose for THIS job, and
// whether one did: a value saved from the interface wins over the one this
// process started with, and an unreadable saved value falls back to it. The
// error is the boot misconfiguration that refuses MySQL dumps, cleared when a
// readable value is saved, which is the whole point of the setting being
// editable while the daemon runs. It does not look at any host: Trigger and
// the schedule card ask it only whether a dump is refused.
func (s *baselineSupervisor) lockModeNow() (baseline.LockMode, lockModeSource, error) {
	return effectiveLockMode(s.reg, s.lockMode, s.lockModeChosen, s.configErr)
}

// lockModeFor is the lock mode a dump of req's source uses (#1986). An
// operator's choice wins, whatever the host. With none, an Amazon RDS or
// Aurora endpoint gets lock-all, because no user there may take the global
// read lock ftwrl needs (not even the master user), and every other host
// gets ftwrl. Only lockModeAutomatic allows execute's one retry with lock-all.
// Nothing is stored: the host is in every request.
func (s *baselineSupervisor) lockModeFor(req console.BaselineRequest) (mode baseline.LockMode, src lockModeSource, err error) {
	mode, src, err = s.lockModeNow()
	if err != nil || src.chosen() {
		return mode, src, err
	}
	if sourceIsManaged(req.SourceDSN) {
		return baseline.LockModeLockAll, lockModeAutomatic, nil
	}
	return baseline.LockModeFTWRL, lockModeAutomatic, nil
}

// sourceIsManaged says whether the source DSN's host is an Amazon RDS or
// Aurora endpoint name (RDS Proxy included), read the way the Connect screen
// reads it (doctor.ManagedFromHost). A DSN that cannot be read is not
// managed: refusing a dump over it would be worse than trying ftwrl, whose
// refusal the retry in execute covers.
func sourceIsManaged(dsn string) bool {
	host, _, _, _, err := config.ParseSourceDSN(dsn)
	if err != nil {
		return false
	}
	managed, _ := doctor.ManagedFromHost(host)
	return managed != ""
}

// Trigger starts a baseline in the background; returns console.ErrBaselineRunning
// if one is already in flight for this server.
func (s *baselineSupervisor) Trigger(req console.BaselineRequest) error {
	// Scoped to MySQL/MariaDB: executePG anchors on pgoutput's own
	// consistent-point LSN and never consults lockMode, so refusing a
	// Postgres baseline over a MySQL-only knob would take away a working
	// button for a setting that cannot affect it.
	if _, _, err := s.lockModeNow(); err != nil && req.Flavor != console.FlavorPostgres {
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
	// Journaled for its whole life, upload included (#2180): every folder
	// it creates is named to the journal as it is made (journalDir), and a
	// kill leaves them for the next start to clean up.
	job := s.beginJob(console.BaselineRunDump, req.ServerID, req.ServerName, req.Trigger, req.Why, started)
	defer job.release()
	if job != nil {
		req.Journal = job
	}
	if req.Flavor == console.FlavorPostgres {
		// The PG producer uploads inside executePG and stamps the snapshot
		// server-side; it keeps the one-phase shape. No index mark either:
		// with the snapshot's instant out of this process's sight, no
		// record could name the snapshot the next update folds from, so
		// that update goes unmeasured and the one after it is measured
		// from the memo (#1737).
		out, uploaded, err := s.executePG(req)
		s.finishDump(req, started, out, nil, uploaded, 0, err)
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
//
// It counts what the index holds, not what the dump contains: events the
// capture had not indexed yet when the dump started are inside the dump
// and are counted by the next update all the same. Seconds of lag are
// noise; after a capture stall the first update's count, and so the rate
// and the size the history proves an update can do, read too high for as
// long as that update stays in the model's sample. That errs toward
// updates, which put no load on the source.
func (s *baselineSupervisor) dumpIndexMark(req console.BaselineRequest) uint64 {
	if req.IndexDSN == "" {
		return 0
	}
	ctx, cancel := context.WithTimeout(s.ctx, windowProbeTimeout)
	defer cancel()
	mark, known := readIndexMark(ctx, probeDSN(req.IndexDSN))
	if !known {
		// Once per full backup, which is rare enough not to rate-limit.
		// Without it the only trace is a Debug line worded for a refresh.
		slog.Info("baseline: could not read the index before the full read; the update after it will not be measured, "+
			"so the snapshot schedule's cost model waits one more update before it can choose a full read again",
			"server", req.ServerName, "id", req.ServerID)
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
	phase, what := "the upload", "the local snapshot is complete and the next full read sends it"
	if own.uploaded {
		phase, what = "the sweep of older snapshots", "this run's own snapshot had already reached the destination; the next full read sweeps again"
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
	own.st.Failure = nil
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
	st.Failure = nil
	st.Tables = out.stats.TablesProcessed
	st.ViewsSkipped = len(out.stats.ViewsSkipped)
	st.LeftOutTables, st.LeftOutTablesOmitted = console.LeftOutTablesOf(leftOutOf(out.stats))
	st.Rows = out.stats.RowsWritten
	st.FinishedAt = nowStamp()
	// The snapshot is written, so a low disk was not this read's end (#1938).
	st.DiskCheck, st.DiskNote = dumpDiskOnceItFit(st.DiskCheck, st.DiskNote)
	slog.Info("baseline: snapshot published locally; uploading it to the snapshot destination in the background",
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
		return 0, fmt.Errorf("upload: the snapshot was written to %s but could not be uploaded to %s; the next full read sends it: %w",
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
		slog.Warn("baseline: could not list the local snapshots to sweep unuploaded snapshots", "server", req.ServerName, "error", err)
		return 0
	}
	seen := map[string]bool{own: true}
	swept := 0
	for _, f := range files {
		if s.ctx.Err() != nil {
			// Shutdown: what is left is the next full backup's, and one line
			// says so instead of one warning per snapshot.
			slog.Info("baseline: sweep of unuploaded snapshots interrupted by shutdown; the next full read continues it", "server", req.ServerName)
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
			slog.Warn("baseline: a local snapshot the destination lacks could not be sent; the next full read tries again",
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
		ViewsSkipped: len(out.stats.ViewsSkipped),
		// The reason this was a full backup travels with the run (#1604):
		// recomputed later it would name whatever is true THEN.
		Why: req.Why, WhyCode: console.BackupWhyCode(req.Why),
	}
	rec.LeftOutTables, rec.LeftOutTablesOmitted = console.LeftOutTablesOf(leftOutOf(out.stats))
	rec.LeftOutKeys = console.LeftOutKeys(leftOutOf(out.stats))
	rec.DiskCheck, rec.DiskNote = s.dumpDiskOf(req.ServerID, own)
	rec.TransportNote = s.dumpTransportOf(req.ServerID, own)
	// A snapshot instant is recorded when a snapshot was published: a
	// success, or a local publish whose upload failed.
	if !out.at.IsZero() && (err == nil || (out.snapDir != "" && !out.staged)) {
		rec.SnapshotTime = out.at.UTC().Format(time.RFC3339)
	}
	// A low-disk warning is about the dump and its conversion (#1938). Once a
	// snapshot was written they fit, whatever the upload did afterwards, and
	// the note says so in place of "this read may fail"; a read that wrote
	// none keeps the warning as it was given. wroteSnapshot is the condition
	// above without the timestamp, which a Postgres read does not have here.
	wroteSnapshot := err == nil || (out.snapDir != "" && !out.staged)
	if wroteSnapshot {
		rec.DiskCheck, rec.DiskNote = dumpDiskOnceItFit(rec.DiskCheck, rec.DiskNote)
	}
	// The base the next update from this snapshot counts its events from
	// (#1737): whenever a snapshot was published, including one whose
	// upload failed, since the next update folds from that local copy.
	if rec.SnapshotTime != "" {
		rec.IndexMark = out.indexMark
	}
	// Why it failed, as data, for the page's plain-words card (#1986). Only
	// when nothing was published: a snapshot whose upload failed exists, and
	// its card is a different one.
	if rec.SnapshotTime == "" {
		rec.Failure = snapshotFailureOf(err, req)
	}
	job, _ := req.Journal.(*jobRun)
	s.recordJobRun(job, req.ServerID, req.ServerName, rec, err)

	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.dumpStatusLocked(req.ServerID)
	if own != nil && st != own {
		slog.Info("baseline: a later full read took over this server's status while the upload ran; this run's outcome is recorded in the history only",
			"server", req.ServerName, "id", req.ServerID, "snapshot", out.snapDir)
		st = own
	}
	st.FinishedAt = nowStamp()
	st.Uploading = false
	if wroteSnapshot {
		st.DiskCheck, st.DiskNote = dumpDiskOnceItFit(st.DiskCheck, st.DiskNote)
	}
	if err != nil {
		st.State = "failed"
		st.LastError = err.Error()
		st.Failure = rec.Failure
		// A snapshot written to the server's own folder before the failure
		// exists (a Postgres upload that failed after the snapshot; a MySQL
		// one was already marked by publishDump).
		if rec.SnapshotTime != "" {
			st.Published = true
		}
		// A full read refused for disk (#1938), or one that found the disk
		// full anyway, is recorded as a disk failure. fullReadStandsIn reads
		// the mark for updates only, so a failed scheduled full read is
		// tried again at its next slot either way.
		st.DiskRefused = foldDiskRefused(err)
		if st.Published {
			if errors.Is(err, context.Canceled) {
				// A routine restart, not a lost backup: the local snapshot is
				// complete and the next full backup sends it.
				slog.Warn("baseline: the upload was interrupted by daemon shutdown; the local snapshot is complete and the next full read sends it",
					"server", req.ServerName, "id", req.ServerID, "snapshot", out.snapDir)
				return
			}
			slog.Error("baseline: the snapshot was written but not sent to the snapshot destination",
				"server", req.ServerName, "id", req.ServerID, "snapshot", out.snapDir, "error", err)
			return
		}
		slog.Error("baseline: snapshot failed", "server", req.ServerName, "id", req.ServerID, "error", err)
		return
	}
	st.State = "succeeded"
	st.LastError = ""
	st.Failure = nil
	st.Tables = out.stats.TablesProcessed
	st.ViewsSkipped = len(out.stats.ViewsSkipped)
	st.LeftOutTables, st.LeftOutTablesOmitted = console.LeftOutTablesOf(leftOutOf(out.stats))
	st.Rows = out.stats.RowsWritten
	st.Uploaded = uploaded
	st.Swept = swept
	st.Published = st.Published || out.snapDir != ""
	slog.Info("baseline: snapshot complete", "server", req.ServerName, "id", req.ServerID,
		"tables", out.stats.TablesProcessed, "views_skipped", len(out.stats.ViewsSkipped), "tables_left_out", len(out.stats.TablesLeftOut),
		"rows", out.stats.RowsWritten, "uploaded", uploaded, "swept", swept)
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
		return dumpOutcome{}, fmt.Errorf("create the working folder %s: %w", s.stagingDir, err)
	}
	// Before anything is created or dumped (#1938): the whole dump lands in
	// the staging folder before it becomes Parquet. The verdict goes on the
	// job's status now, so the run's history record and the schedule card
	// carry a low-disk warning even if the read then fails.
	check, note, err := s.checkDumpDisk(req)
	if err != nil {
		return dumpOutcome{}, err
	}
	s.noteDumpDisk(req.ServerID, check, note)

	// Resolved HERE, not at boot: a lock mode saved from the interface governs
	// the very next dump. Trigger refused an unreadable one already, but the
	// saved value can be cleared between Trigger and here, bringing back an
	// invalid environment value; that refuses too, never runs automatic.
	lockMode, src, err := s.lockModeFor(req)
	if err != nil {
		return dumpOutcome{}, err
	}
	if src == lockModeAutomatic && lockMode == baseline.LockModeLockAll {
		slog.Info("console snapshot: the source is an Amazon RDS or Aurora endpoint, so this snapshot uses lock mode lock-all",
			"server", req.ServerID)
	}
	att, err := s.dumpAttempt(req, lockMode, src)
	// One retry, and only for the automatic mode (#1986): ftwrl was refused
	// for want of a privilege no user can have on a managed server (the
	// global read lock itself, or RELOAD/BACKUP_ADMIN before mydumper ran).
	// A host name that does not say RDS (an IP, a CNAME) lands here. The
	// retry is lock-all, never a mode without a lock: whether this user has
	// LOCK TABLES is for lock-all's own privilege check to say. An operator's
	// explicit ftwrl is never second-guessed.
	if err != nil && src == lockModeAutomatic && lockMode == baseline.LockModeFTWRL && ftwrlRefused(err) && s.ctx.Err() == nil {
		slog.Info("console snapshot: lock mode ftwrl was refused by the source; retrying this snapshot once with lock-all",
			"server", req.ServerID, "error", err)
		first := err
		att, err = s.dumpAttempt(req, baseline.LockModeLockAll, lockModeAutomatic)
		if err != nil {
			// Both causes reach the operator: the lock-all failure is the
			// one to act on, and the ftwrl refusal before it is why lock-all
			// was tried at all.
			slog.Warn("console snapshot: the automatic retry with lock-all failed too",
				"server", req.ServerID, "ftwrl_refusal", first, "error", err)
			err = fmt.Errorf("lock mode ftwrl was refused by the source (%s), and the automatic retry with lock-all failed too: %w",
				refusalSummary(first), err)
		}
	}
	if err != nil {
		if src == lockModeSaved && s.reg != nil {
			// The saved value wins over the environment, so a refusal that
			// names how to change it must also say where it lives.
			err = fmt.Errorf("%w (this snapshot used lock mode %s, saved in %s under backup_settings.lock_mode)", err, lockMode, s.reg.Path())
		}
		if src.chosen() {
			err = &chosenModeError{err: err}
		}
		return dumpOutcome{}, err
	}
	dumpDir, dumpStartedAt, ddlMark, eventMark := att.dir, att.startedAt, att.ddlMark, att.eventMark
	defer os.RemoveAll(dumpDir)

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
		return dumpOutcome{}, fmt.Errorf("dump: cannot read mydumper's metadata, so the snapshot cannot be anchored to a binlog position: %w", err)
	}

	out := dumpOutcome{at: dumpStartedAt, cleanup: func() {}}
	outputDir := req.LocalDir
	if outputDir == "" { // S3-only: stage, upload, discard the staging
		outputDir, err = os.MkdirTemp(s.stagingDir, "baseline-")
		if err != nil {
			return dumpOutcome{}, fmt.Errorf("create this run's folder in the working folder %s: %w", s.stagingDir, err)
		}
		journalDir(req, s.stagingDir, filepath.Base(outputDir))
		out.staged = true
		out.cleanup = func() { os.RemoveAll(outputDir) }
	}

	// ownSnapshot is the snapshot folder this run alone writes, "" when it
	// cannot be said to be this run's.
	ownSnapshot := ""
	if !out.staged {
		// The snapshot directory baseline.Run is about to create, journaled
		// only when it is vacant now: one that already holds files is not
		// this run's alone (a same-second CLI run), and a kill must never make
		// it reclaimable.
		name := reconstruct.SnapshotDirName(dumpStartedAt)
		if vacant, verr := reconstruct.SnapshotDirVacant(filepath.Join(outputDir, name)); verr == nil && vacant {
			journalDir(req, outputDir, name)
			ownSnapshot = name
		}
	}
	stats, err := baseline.Run(s.ctx, s.dumpBaselineConfig(req, dumpDir, outputDir, dumpStartedAt, ddlMark, eventMark))
	if err != nil {
		out.cleanup()
		convErr := err
		err = fmt.Errorf("convert: %w", err)
		if ownSnapshot != "" {
			if said := discardFailedSnapshot(outputDir, ownSnapshot, req.ServerID, convErr); said != "" {
				err = fmt.Errorf("%w. %s", err, said)
			}
		}
		return dumpOutcome{}, err
	}
	out.stats = stats
	out.snapDir = filepath.Join(outputDir, reconstruct.SnapshotDirName(dumpStartedAt))
	return out, nil
}

// discardFailedSnapshot removes the snapshot folder a full read created in the
// server's own folder when its conversion failed (#1938), and returns a
// sentence for the run's error only when the folder is still there and
// holds nothing usable.
//
// It is what the next start does for a run that DIED (reclaimJobDir, #2180),
// done now for a run that failed and lived: the job's journal is cleared when
// the job ends, prune never touches an _INCOMPLETE folder and the sweep only
// knows discards that were interrupted, so nothing else would ever remove it,
// and a scheduled read failing on a full disk left one more on that disk at
// every slot. The rules are the reclaim's own (reclaimSnapshotDir): a finished,
// marked snapshot stays, and so does a folder with files and no marker, which
// every reader takes as complete. The caller passes only a folder that was
// vacant when the run started, so it never holds anyone else's files.
//
// One failure keeps the folder: its marker vanished under the run
// (baseline.ErrIncompleteMarkerVanished), so someone else has touched it.
//
// A run that wrote every table and failed only at the finishing (table
// deltas, integrity manifest, _SUCCESS) is NOT kept. Its folder is still
// marked incomplete, so no listing shows it and nothing publishes it; the
// dump it came from is already removed, so nothing can finish it; and the
// usual reason for that failure is the full disk the folder would stay on.
// (The refresh keeps a folder of that shape, see keepPartialSnapshotBecause.
// That rule is the refresh's own and is not changed here.)
//
// The discard renames the folder to a hidden name before it deletes it. A
// delete that fails after the rename leaves that hidden folder, which only
// the sweep removes, and the sweep otherwise runs on update cycles alone; so
// it runs here first, and the next failed read of a server that only does
// full reads retries what this one could not finish.
func discardFailedSnapshot(root, name, serverID string, convErr error) (said string) {
	p := filepath.Join(root, name)
	if n, err := reconstruct.SweepDiscardedSnapshots(root); err != nil {
		slog.Warn("console snapshot: could not clear a folder an earlier failed read left half removed", "server", serverID, "root", root, "error", err)
	} else if n > 0 {
		slog.Info("console snapshot: cleared folders an earlier failed read left half removed", "server", serverID, "root", root, "dirs", n)
	}
	switch {
	case errors.Is(convErr, baseline.ErrIncompleteMarkerVanished):
		slog.Warn("console snapshot: kept the snapshot folder of a full read whose marker vanished while it ran",
			"server", serverID, "path", p)
		return ""
	}
	res := reclaimSnapshotDir(p, name)
	switch {
	case res.err != nil:
		slog.Error("console snapshot: could not remove the snapshot folder of a full read whose conversion failed; "+
			"no listing shows it, and only the next failed full read of this server retries",
			"server", serverID, "path", p, "error", res.err)
		return fmt.Sprintf("The unfinished snapshot folder this read started could not be removed (%s); it holds nothing usable and still takes room",
			firstLineOf(res.err.Error()))
	case res.keptBecause != "":
		slog.Warn("console snapshot: kept the snapshot folder of a full read whose conversion failed",
			"server", serverID, "path", p, "reason", res.keptBecause)
	case res.removed:
		slog.Info("console snapshot: removed the unfinished snapshot folder of a full read whose conversion failed", "server", serverID, "path", p)
		return "The unfinished snapshot folder this read started was removed"
	}
	return ""
}

// journalDir names a folder the run just created to its journal (#2180).
func journalDir(req console.BaselineRequest, root, name string) {
	if req.Journal != nil {
		req.Journal.Created(root, name)
	}
}

// dumpAttempt is one run of mydumper for execute: its own dump folder, start
// time and DDL mark. Everything an attempt reads or writes is made here, so
// the retry with lock-all (#1986) starts as clean as the first try did; the
// folder of a failed attempt is removed before returning.
type dumpAttempt struct {
	dir       string
	startedAt time.Time
	ddlMark   string
	eventMark string
}

func (s *baselineSupervisor) dumpAttempt(req console.BaselineRequest, lockMode baseline.LockMode, src lockModeSource) (dumpAttempt, error) {
	dir, err := os.MkdirTemp(s.stagingDir, "dump-")
	if err != nil {
		return dumpAttempt{}, fmt.Errorf("create dump dir: %w", err)
	}
	journalDir(req, s.stagingDir, filepath.Base(dir))
	// Captured immediately before invoking mydumper: since this pipeline runs
	// mydumper and baseline.Run in the same process, we can pass our own UTC
	// wall-clock time straight through as the snapshot anchor instead of
	// letting baseline.Run re-parse mydumper's "Started dump at" metadata
	// line — which is written in the dump host's LOCAL time and would
	// otherwise be misread as UTC verbatim, skewing the replay window by the
	// host's UTC offset (#768).
	a := dumpAttempt{dir: dir, startedAt: time.Now().UTC()}
	// The DDL mark (#1912) BEFORE mydumper starts: a TRUNCATE already in the
	// index ran on the source before this dump began, so the dump holds its
	// effect, and no update from this snapshot has to place it by position.
	a.ddlMark = dumpDDLMarkFunc(req)
	a.eventMark = dumpEventMarkFunc(req)
	ctx := withTransportNote(s.ctx, func(note string) { s.noteDumpTransport(req.ServerID, note) })
	if err := runMydumperFunc(ctx, req.SourceDSN, req.SourceSSL, req.Schemas, dir, lockMode, src); err != nil {
		// Read BEFORE the removal (#1938): once the partial dump is gone the
		// folder has room again, and a disk that filled would read as one
		// that did not.
		free, filled := s.dumpFilledWorkingFolder(err)
		rmErr := os.RemoveAll(dir)
		if rmErr != nil {
			slog.Warn("console snapshot: could not remove the folder of a failed dump", "path", dir, "error", rmErr)
		}
		if filled {
			err = &workingFolderFullError{err: err, folder: s.stagingDir, free: free, dumpDir: dir, rmErr: rmErr}
		}
		return dumpAttempt{}, fmt.Errorf("dump: %w", err)
	}
	return a, nil
}

// mydumperNoSpaceText is the C library's wording for ENOSPC, which mydumper
// prints when a write to one of its output files fails (measured with 1.0.3-1
// on ext4 and tmpfs: "Couldn't write data to a file(13): No space left on
// device", once per failed file, exit status 1 after the last table). It is
// strerror(ENOSPC) under the C locale, not a sentence of mydumper's own, so a
// mydumper that prints the system's error at all prints these words; only
// 1.0.3-1 was measured. A host whose messages are translated, or a build that
// words the failure itself, prints something else, and that failure stays
// unclassified.
const mydumperNoSpaceText = "No space left on device"

// mydumperNoSpaceError marks a mydumper failure whose output says a disk had
// no space left. Made in runMydumper, where the output is in hand, so nothing
// after it reads the words. It does NOT say which disk: the source server
// reports its own full tmpdir in the same words, relayed by mydumper, which
// is why dumpFilledWorkingFolder also measures. The message is unchanged.
type mydumperNoSpaceError struct{ err error }

func (e *mydumperNoSpaceError) Error() string { return e.err.Error() }
func (e *mydumperNoSpaceError) Unwrap() error { return e.err }

// dumpFullFreeBelow is how little free space the working folder must have,
// right after mydumper said "no space", for the failure to be called this
// folder's. A disk that filled measured zero on ext4 and tmpfs; a filesystem
// can also be left with the last blocks no write fitted into. The margin is
// for that, and is small on purpose, so a folder with real room is not blamed
// for someone else's full disk.
const dumpFullFreeBelow = 16 << 20

// dumpFilledWorkingFolder reports whether a failed dump filled the working
// folder, and what the folder had free when asked. Both halves are required:
// mydumper's output said a disk had no space, and this folder measures as
// having none. Free space that cannot be measured is "cannot tell", so the
// failure keeps its own words.
func (s *baselineSupervisor) dumpFilledWorkingFolder(err error) (free uint64, filled bool) {
	var noSpace *mydumperNoSpaceError
	if !errors.As(err, &noSpace) {
		return 0, false
	}
	free, ok, _ := measureFree(s.stagingDir)
	return free, ok && free < dumpFullFreeBelow
}

// workingFolderFullError is a full read whose dump stopped because the
// working folder filled (#1938): the check before the dump works from an
// estimate, and another read, an export or anything else on that disk can
// take the room meanwhile. It is errFoldDiskFull to the page, like a read
// refused before it started, and keeps mydumper's own failure in the chain.
type workingFolderFullError struct {
	err     error
	folder  string
	free    uint64
	dumpDir string
	rmErr   error
}

// Error says the cause first, on one line, so the status and the run history
// open with it; mydumper's words follow on the next. The failure card shows
// summary instead.
func (e *workingFolderFullError) Error() string {
	left := "The partial dump was deleted."
	if e.rmErr != nil {
		left = fmt.Sprintf("The partial dump at %s could not be removed (%s); it still takes room there.", e.dumpDir, firstLineOf(e.rmErr.Error()))
	}
	return fmt.Sprintf("%s: the working folder %s filled up during the full read (%s free when the dump stopped). "+
		"No new snapshot was made, and earlier snapshots are unchanged. %s %s\n%s",
		errFoldDiskFull, e.folder, humanSize(int64(e.free)), left, moveStagingHint, e.err)
}

func (e *workingFolderFullError) Unwrap() []error { return []error{errFoldDiskFull, e.err} }

// summary is the failure card's one line: the cause and the fix, short enough
// to be shown whole. Error()'s first line is not: refusalSummary cuts at 300
// runes, which lands inside the fix, and after the lock-all retry the ftwrl
// refusal comes first and can take the whole line.
func (e *workingFolderFullError) summary() string {
	s := fmt.Sprintf("The working folder %s filled up during the full read (%s free when the dump stopped). "+
		"Free space there, or move it to a bigger disk with the \"Working folder\" setting and restart DBTrail.",
		e.folder, humanSize(int64(e.free)))
	if e.rmErr != nil {
		// The one thing still taking that room is ours, and nothing else
		// will remove it or say where it is.
		s += fmt.Sprintf(" The partial dump at %s could not be removed and still takes room there.", e.dumpDir)
	}
	return s
}

// ftwrlDeniedError is mydumper's refusal of the global read lock that lock
// mode ftwrl takes, as on RDS and Aurora (see mydumperlock.FTWRLDeniedHint).
// It is made where the hint matched, so execute's retry (#1986) never reads
// the words; the message is unchanged and the exit error stays reachable.
type ftwrlDeniedError struct{ err error }

func (e *ftwrlDeniedError) Error() string { return e.err.Error() }
func (e *ftwrlDeniedError) Unwrap() error { return e.err }

// refusalSummary is an error's first line, without mydumper's output, short
// enough to sit inside another error's sentence.
func refusalSummary(err error) string {
	s, _, _ := strings.Cut(firstLineOf(err.Error()), "; output:")
	const max = 300
	if r := []rune(s); len(r) > max {
		s = string(r[:max]) + "..."
	}
	return s
}

// ftwrlRefused says a dump in lock mode ftwrl failed for want of what no user
// has on a managed server: the global read lock itself (mydumper's refusal),
// or RELOAD/BACKUP_ADMIN (the privilege check before it).
func ftwrlRefused(err error) bool {
	var denied *ftwrlDeniedError
	return errors.As(err, &denied) || errors.Is(err, mydumperlock.ErrFTWRLPrivilegesMissing)
}

// dumpBaselineConfig is how a dump of req's server is converted: split out
// so what the conversion is told, the writer it signs with among it, is
// checked without running mydumper.
func (s *baselineSupervisor) dumpBaselineConfig(req console.BaselineRequest, dumpDir, outputDir string, at time.Time, ddlMark, eventMark string) baseline.Config {
	return baseline.Config{
		InputDir:    dumpDir,
		OutputDir:   outputDir,
		Compression: "zstd",
		Timestamp:   at,
		TableDeltas: s.tableDeltas,
		WriterID:    snapshotWriterID(req),
		DDLMark:     ddlMark,
		EventMark:   eventMark,
		// The dump folder is this run's own and is removed when the run ends
		// (#1938), so each table's dump data goes as soon as its Parquet file
		// is written: the dump shrinks while the snapshot grows, instead of
		// both being whole on disk at the end.
		RemoveConvertedData: true,
	}
}

// snapshotWriterID is the identity a full snapshot of req's server is signed
// with (#1762): the bintrail_id its index records, the same one the scheduled
// updates of that server sign with, since they read it from the same index.
// Empty when the index names none yet, or cannot be read: the snapshot is
// then published unsigned, never refused.
func snapshotWriterID(req console.BaselineRequest) string {
	if req.IndexDSN == "" {
		return ""
	}
	id, err := snapshotWriterIDFunc(req.IndexDSN)
	if err != nil {
		slog.Warn("could not read the server's bintrail_id, so this snapshot is published unsigned and takes no part in noticing two writers on one snapshot location",
			"server", req.ServerID, "error", err)
		return ""
	}
	return id
}

// snapshotWriterIDFunc reads the identity from an index; a test replaces it.
var snapshotWriterIDFunc = func(indexDSN string) (string, error) {
	db, err := config.Connect(indexDSN)
	if err != nil {
		return "", fmt.Errorf("connect: %w", err)
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return serverid.SnapshotWriterID(ctx, db)
}

// dumpDDLMarkFunc reads the DDL mark a dump of req's server carries: the
// newest schema_changes row of its index, and only when a stream writes that
// index (the dump and the capture then read the same source). "" when there is
// no index, no stream, no row, or the read fails: the snapshot is published
// without one, and updates from it place statements by position as before.
// A variable so a test answers it without an index.
var dumpDDLMarkFunc = func(req console.BaselineRequest) string {
	if req.IndexDSN == "" {
		return ""
	}
	db, err := config.Connect(req.IndexDSN)
	if err != nil {
		slog.Warn("could not reach the index for the snapshot's DDL mark; the snapshot is published without one",
			"server", req.ServerID, "error", err)
		return ""
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	captured, err := query.StreamCaptured(ctx, db)
	if err != nil || !captured {
		return ""
	}
	m, err := reconstruct.ReadDDLMark(ctx, db)
	if err != nil {
		slog.Warn("could not read the index for the snapshot's DDL mark; the snapshot is published without one",
			"server", req.ServerID, "error", err)
		return ""
	}
	if m == nil {
		return ""
	}
	return m.Encode()
}

// dumpEventMarkFunc reads the event mark (#2160) a dump of req's server is
// stamped with: the newest event in its index, read BEFORE mydumper starts,
// like the DDL mark. Every event indexed after it follows it in the source's
// binary log, so an update from this snapshot that finds one sorting before
// it knows the numbering started over. "" when it cannot be read, which only
// leaves the snapshot without that check. A var so tests can count it.
var dumpEventMarkFunc = func(req console.BaselineRequest) string {
	if req.IndexDSN == "" {
		return ""
	}
	db, err := config.Connect(req.IndexDSN)
	if err != nil {
		slog.Warn("could not reach the index for the snapshot's event mark; the snapshot is published without one",
			"server", req.ServerID, "error", err)
		return ""
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return reconstruct.ReadStreamEventMark(ctx, db)
}

// executePG produces a PostgreSQL baseline in-process via internal/pgbaseline —
// COPY straight to Parquet, anchored at the slot's consistent-point LSN. No
// mydumper subprocess and no #768 timestamp skew: pgbaseline self-stamps the
// snapshot time from the database's own now(). Destination handling mirrors
// execute(): a local dir is written persistently; S3-only stages in a temp dir,
// uploads via the same source-agnostic baseline.Upload, and discards the staging.
// pgBaselineRun is pgbaseline.Run; a test replaces it.
var pgBaselineRun = pgbaseline.Run

// executePG returns the snapshot it wrote even when the upload after it
// failed: with a local directory that snapshot exists (out.snapDir set,
// not staged), and finishDump then marks the run published and names its
// time, so no page calls it unfinished (#1991 review).
func (s *baselineSupervisor) executePG(req console.BaselineRequest) (dumpOutcome, int, error) {
	if err := os.MkdirAll(s.stagingDir, 0o755); err != nil {
		return dumpOutcome{}, 0, fmt.Errorf("create the working folder %s: %w", s.stagingDir, err)
	}
	outputDir := req.LocalDir
	staged := outputDir == ""
	if staged { // S3-only: stage then upload, discard staging
		var err error
		outputDir, err = os.MkdirTemp(s.stagingDir, "pgbaseline-")
		if err != nil {
			return dumpOutcome{}, 0, fmt.Errorf("create this run's folder in the working folder %s: %w", s.stagingDir, err)
		}
		journalDir(req, s.stagingDir, filepath.Base(outputDir))
		defer os.RemoveAll(outputDir)
	}

	cfg, err := pgBaselineConfig(req, outputDir)
	if err != nil {
		return dumpOutcome{}, 0, err
	}
	pgStats, err := pgBaselineRun(s.ctx, cfg)
	if err != nil {
		return dumpOutcome{}, 0, fmt.Errorf("pg baseline: %w", err)
	}
	out := dumpOutcome{stats: baseline.Stats{
		TablesProcessed: pgStats.TablesProcessed,
		RowsWritten:     pgStats.RowsWritten,
		FilesWritten:    pgStats.FilesWritten,
	}, at: pgStats.SnapshotTime, staged: staged}
	if !pgStats.SnapshotTime.IsZero() {
		out.snapDir = filepath.Join(outputDir, reconstruct.SnapshotDirName(pgStats.SnapshotTime))
	}

	var uploaded int
	if req.S3 != "" {
		uploaded, err = uploadSnapshot(s.ctx, outputDir, req.S3, "", false)
		if err != nil {
			return out, 0, fmt.Errorf("upload: %w", err)
		}
	}
	return out, uploaded, nil
}

// pgBaselineConfig builds the pgbaseline.Config for a PG source, mirroring
// cmd/bintrail-pg's pgBaselineConfigFromFlags. The replication DSN is derived
// from the stored query DSN (console.PGReplDSN — the one home for that
// derivation), needed so pgbaseline can CREATE the slot when a user baselines
// BEFORE the first monitor start; harmless if the slot already exists.
// Unit-testable without a live PG: the one thing it reads is the writer the
// snapshot is signed with, from the server's index, behind a seam. The registry carries only a schema filter.
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
		WriterID:    snapshotWriterID(req),
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
	// lib is the client library --version named; it decides how a TLS mode
	// is spelled for this build (mydumperTLSArgs).
	lib mydumperlock.ClientLibrary
}

// sourceServerVersion reads SELECT VERSION() from the source. A seam, so the
// tests need no server.
var sourceServerVersion = func(ctx context.Context, dsn string, ssl config.SSL) (string, error) {
	db, err := connectSource(dsn, ssl)
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
//
// src words the refusal (#1986): a mode the automatic choice made must not
// tell the operator to remove a variable nobody set, and a saved mode is
// changed where it was saved, not in the environment it wins over.
func planMydumper(lockMode baseline.LockMode, src lockModeSource) (mydumperPlan, error) {
	path, err := exec.LookPath("mydumper")
	if err != nil {
		return mydumperPlan{}, fmt.Errorf("mydumper is not installed where DBTrail can run it (%v). "+
			"Full reads of MySQL and MariaDB servers run the mydumper on the PATH of the DBTrail process; "+
			"install mydumper %s or newer, which supports every lock mode",
			err, mydumperlock.LockModeFloor)
	}
	v, lib, verErr := mydumperlock.ProbeBuild(path)
	ftwrl := lockMode == baseline.LockModeFTWRL
	switch {
	case errors.Is(verErr, mydumperlock.ErrNotRunnable):
		// #1699: a binary that does not run is not an "unknown version", and
		// routing it there would make the first error a privilege refusal.
		return mydumperPlan{}, fmt.Errorf("%w. Full reads run that same binary, so fix or replace it with mydumper %s or newer",
			verErr, mydumperlock.LockModeFloor)
	case verErr != nil:
		if !ftwrl {
			if src == lockModeAutomatic {
				return mydumperPlan{}, fmt.Errorf("%s, and the version of %s could not be read (%v), "+
					"so DBTrail cannot tell whether it accepts --sync-thread-lock-mode. Install mydumper %s or newer",
					autoNeedsMode(lockMode), path, verErr, mydumperlock.LockModeFloor)
			}
			return mydumperPlan{}, fmt.Errorf("lock mode %s cannot be used: the version of %s could not be read (%v), "+
				"so DBTrail cannot tell whether it accepts --sync-thread-lock-mode. "+
				"Install mydumper %s or newer, or %s",
				lockMode, path, verErr, mydumperlock.LockModeFloor, chosenModeUndo(src))
		}
		return mydumperPlan{path: path, sendLockFlags: false, preflight: true, lib: lib,
			fallback: fmt.Sprintf("could not read the mydumper version (%v), so the dump runs without "+
				"--sync-thread-lock-mode and --trx-tables, after the privilege check", verErr)}, nil
	case !v.SupportsLockMode():
		if !ftwrl {
			if src == lockModeAutomatic {
				return mydumperPlan{}, &mydumperTooOldError{min: mydumperlock.LockModeFloor, err: fmt.Errorf("%s, which needs mydumper %s or newer, and %s is mydumper %s, "+
					"which does not accept --sync-thread-lock-mode. Install mydumper %s or newer (distribution packages are often older)",
					autoNeedsMode(lockMode), mydumperlock.LockModeFloor, path, v, mydumperlock.LockModeFloor)}
			}
			return mydumperPlan{}, &mydumperTooOldError{min: mydumperlock.LockModeFloor, err: fmt.Errorf("lock mode %s needs mydumper %s or newer, and %s is mydumper %s, "+
				"which does not accept --sync-thread-lock-mode. Install mydumper %s or newer (distribution packages are often older), "+
				"or %s, which this build uses by default",
				lockMode, mydumperlock.LockModeFloor, path, v, mydumperlock.LockModeFloor, chosenModeUndo(src))}
		}
		fallback := fmt.Sprintf("mydumper %s is older than %s, so the dump runs without --sync-thread-lock-mode "+
			"and --trx-tables and takes that build's own FTWRL", v, mydumperlock.LockModeFloor)
		if v.Less(mydumperlock.PositionFloor) {
			fallback += fmt.Sprintf("; against MySQL 8.4 and newer a build older than %s cannot record the binlog position, "+
				"so snapshots of those sources are refused before they start", mydumperlock.PositionFloor)
		}
		// The #800 privilege check is skipped only for a build that takes no
		// backup lock (measured: the packaged 0.10 does not; 0.16.3 and 1.0.3
		// do). Skipping it for every pre-0.18 build would switch the check off
		// for a whole band that DOES take the lock, and that check exists
		// because the failure it prevents is a segfault.
		return mydumperPlan{path: path, sendLockFlags: false,
			preflight: v.TakesBackupLock() && lockMode.NeedsElevatedPrivileges(),
			fallback:  fallback, version: v, versionKnown: true, lib: lib}, nil
	default:
		return mydumperPlan{path: path, sendLockFlags: true, preflight: lockMode.NeedsElevatedPrivileges(),
			version: v, versionKnown: true, lib: lib}, nil
	}
}

// autoNeedsMode opens a refusal of a mode the automatic choice made (#1986).
func autoNeedsMode(m baseline.LockMode) string {
	return fmt.Sprintf("this source needs lock mode %s (it is an Amazon RDS or Aurora host, or it refused ftwrl)", m)
}

// chosenModeUndo is how an operator goes back from a mode they chose to ftwrl.
func chosenModeUndo(src lockModeSource) string {
	if src == lockModeSaved {
		return "save lock mode ftwrl (" + mydumperlock.SavedLockModeEndpoint + ` with {"value":"ftwrl"}) to back up with ftwrl`
	}
	return "remove BINTRAIL_CONSOLE_BASELINE_LOCK_MODE to back up with ftwrl"
}

// mydumperBootWarning is the startup line for a daemon that may take full
// backups (#1688): the verdict every run will reach, said once before the first
// slot as well as by each run. Empty when the local mydumper accepts the
// configured mode as is.
//
// In the automatic mode (#1986) the line is about ftwrl, the mode most
// sources get, plus a second sentence when this mydumper cannot do lock-all:
// every Amazon RDS or Aurora source would then fail, and only a startup line
// says so before the first slot.
func mydumperBootWarning(lockMode baseline.LockMode, src lockModeSource) string {
	plan, err := planMydumper(lockMode, src)
	var msg string
	switch {
	case err != nil:
		return "full reads of MySQL and MariaDB servers will fail until this is fixed: " + err.Error()
	case plan.fallback != "":
		msg = "full reads of MySQL and MariaDB servers will run, but " + plan.fallback
	}
	if src == lockModeAutomatic && lockMode != baseline.LockModeLockAll {
		if _, laErr := planMydumper(baseline.LockModeLockAll, lockModeAutomatic); laErr != nil {
			rds := "snapshots of Amazon RDS and Aurora servers, and of any server that refuses ftwrl, will fail until mydumper is upgraded: " + laErr.Error()
			if msg == "" {
				return rds
			}
			msg += ". Also, " + rds
		}
	}
	return msg
}

// runMydumper invokes the mydumper on the PATH against the source DSN, writing a
// dump (with binlog coordinates in its metadata, which baseline.Run reads) into
// dumpDir. In the console image that is the pinned build the compose
// baseline-dump pipeline also uses; on a native install it is whatever the host
// has, which is why planMydumper reads its version first (#1688). lockMode
// selects the sync mode when the build accepts it; see buildConsoleMydumperArgs.
// runMydumperFunc runs mydumper; a test replaces it.
var runMydumperFunc = runMydumper

func runMydumper(ctx context.Context, sourceDSN string, ssl config.SSL, schemas []string, dumpDir string, lockMode baseline.LockMode, src lockModeSource) error {
	remedy := mydumperlock.RemedyConsole
	if src == lockModeSaved {
		remedy = mydumperlock.RemedyConsoleSaved
	}
	host, port, user, password, err := config.ParseSourceDSN(sourceDSN)
	if err != nil {
		return err
	}

	// Probed on every run, not once at boot: the fix for an old or broken
	// mydumper is to install another one, and that has to take effect without
	// restarting the process that also captures changes. One exec of
	// --version costs nothing next to a dump.
	plan, err := planMydumper(lockMode, src)
	if err != nil {
		return err
	}
	if plan.fallback != "" {
		slog.Warn("baseline: "+plan.fallback, "mydumper", plan.path, "lock_mode", string(lockMode))
	}
	// How mydumper connects: the TLS capture and the pre-checks use (#1996).
	// Decided before any lock or privilege check, so a TLS setting the dump
	// cannot honor is refused before anything touches the source's locks.
	tlsPlan, err := resolveDumpTLS(ctx, sourceDSN, ssl)
	if err != nil {
		return err
	}
	if !tlsPlan.encrypt {
		// The one log line per read about it (the pre-checks log their own
		// cleartext fallback at Debug), louder when nobody chose it:
		// preferred fell back because the source offers no TLS. disabled or
		// a DSN's tls=false are choices. The run records it too, so the
		// card and the history say it, not only the log.
		level := slog.LevelInfo
		if tlsPlan.fellBack {
			level = slog.LevelWarn
		}
		slog.Log(ctx, level, "console snapshot: mydumper reads the source WITHOUT encryption: "+tlsPlan.why, "host", host)
		reportTransportNote(ctx, "Read without encryption: "+tlsPlan.why+".")
	}
	// A build older than 0.16.3 exits 0 against MySQL 8.4 with no position in
	// its metadata (measured). execute refuses that dump afterwards; refusing
	// here instead spares the source a full dump under FTWRL that would be
	// thrown away. A source whose version cannot be read goes ahead: the
	// after-dump check still guards it.
	if plan.versionKnown && plan.version.Less(mydumperlock.PositionFloor) {
		sv, verr := sourceServerVersion(ctx, sourceDSN, ssl)
		if verr != nil {
			// The dump still runs and its own metadata is checked afterwards,
			// but that check comes AFTER a full dump: say why the cheap
			// refusal could not be made instead of dropping it in silence.
			slog.Warn("console snapshot: could not read the source server's version, so an old mydumper cannot be refused before it dumps",
				"mydumper", plan.version.String(), "error", verr)
		}
		if verr == nil && !plan.version.RecordsPositionOn(sv) {
			return &mydumperTooOldError{min: mydumperlock.LockModeFloor, err: fmt.Errorf("mydumper %s cannot record the binlog position on MySQL %s: builds older than %s read it "+
				"with SHOW MASTER STATUS, which MySQL 8.4 removed, so the snapshot would be refused after a full dump and "+
				"the dump was not started. Install mydumper %s or newer",
				plan.version, sv, mydumperlock.PositionFloor, mydumperlock.LockModeFloor)}
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
			if err := checkMydumperPrivileges(ctx, sourceDSN, ssl, lockMode, remedy, schemas); err != nil {
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
		multiTableNoLockCheck(ctx, sourceDSN, ssl, schemas)
	}

	tlsArgs := mydumperTLSArgs(tlsPlan, plan.lib)
	// A build linked against the MySQL client library enforces --ssl-mode
	// REQUIRED itself. Any other does not reliably (Connector/C, measured:
	// it dumps in cleartext from a server without TLS), so the dump is
	// preceded by a connection with TLS mandatory, in every lock mode, and on
	// Connector/C pinned to that connection's certificate (sourceTLSPin).
	if tlsPlan.encrypt && plan.lib != mydumperlock.LibMySQL {
		fp, err := sourceTLSPin(ctx, sourceDSN, tlsPlan)
		if err != nil {
			return fmt.Errorf("the full read must reach the source encrypted, and a connection with TLS required failed: %w", err)
		}
		if plan.lib == mydumperlock.LibMariaDB {
			pin, remove, err := writeMydumperTLSPin(filepath.Dir(dumpDir), fp)
			if err != nil {
				return fmt.Errorf("could not write the TLS settings file for mydumper: %w", err)
			}
			defer remove()
			tlsArgs = append(tlsArgs, "--defaults-extra-file", pin)
		} else {
			// Unknown library: the check proved the server encrypts, but
			// mydumper is not pinned, so an attacker between the check
			// and the dump is not stopped. Said on the run, not hidden.
			slog.Warn("console snapshot: the source was checked to encrypt, but mydumper's client library could not be "+
				"identified from its --version, so it is not pinned to that certificate", "mydumper", plan.path, "host", host)
			reportTransportNote(ctx, "Encrypted, not pinned: mydumper's client library could not be identified, so the "+
				"source was checked to encrypt but mydumper was not tied to its certificate.")
		}
	}
	// A list of schemas goes as --database a,b on a mydumper measured to write
	// each schema's CREATE DATABASE that way (1.0.3-1): under --regex it
	// writes none, and a schema it renames (a non-ASCII or "@" name) then
	// cannot be read back from the dump (#2006).
	listSchemas := plan.versionKnown && !plan.version.Less(mydumperlock.Version{Major: 1})
	if listSchemas && len(schemas) == 0 {
		// No schema list set: the user schemas, asked from the source, go as
		// that list too, so a schema mydumper renames can be read back here
		// as well (the default setup is where most servers are). A schema
		// created between this question and the dump is not read; the next
		// update names it as a new table. If the source cannot be asked, the
		// dump runs as before, under --regex.
		if all, lerr := listUserSchemas(ctx, sourceDSN, ssl); lerr != nil {
			slog.Warn("snapshot: could not list the source's schemas; the full read selects them by pattern, so a schema mydumper renames cannot be read back", "error", lerr)
		} else if len(all) > 0 {
			schemas = all
		}
	}
	args := buildConsoleMydumperArgs(host, port, user, schemas, dumpDir, lockMode, plan.sendLockFlags, tlsArgs, listSchemas)
	// plan.path, not the bare name: the dump must run the very binary whose
	// version was just read.
	cmd := exec.CommandContext(ctx, plan.path, args...)
	// Deliver the source password out of band via MYSQL_PWD (honored by the
	// MySQL client library mydumper links against) so it never lands on argv,
	// where it would be world-readable in `ps aux` / /proc/<pid>/cmdline. The
	// child's /proc/<pid>/environ is mode 0400 (#811).
	cmd.Env = mydumperEnv(os.Environ(), password)
	prepareMydumperCmd(cmd)
	out, err := runMydumperCmd(cmd)
	if err != nil {
		if msg := strings.TrimSpace(string(out)); msg != "" {
			if hint := mydumperTLSHint(msg, tlsPlan); hint != "" {
				return fmt.Errorf("mydumper failed: %w: %s; output: %s", err, hint, msg)
			}
			if hint := mydumperlock.FTWRLDeniedHint(lockMode, msg, remedy); hint != "" {
				return &ftwrlDeniedError{err: fmt.Errorf("mydumper failed: %w: %s; output: %s", err, hint, msg)}
			}
			if strings.Contains(msg, mydumperNoSpaceText) {
				return &mydumperNoSpaceError{err: fmt.Errorf("mydumper failed: %w; output: %s", err, msg)}
			}
			return fmt.Errorf("mydumper failed: %w; output: %s", err, msg)
		}
		return fmt.Errorf("mydumper failed: %w", err)
	}
	// How the read was locked (#1380), for baseline.Run to put in the
	// snapshot. Only when the mode was sent: a build that did not take the
	// flag took its own, which is not on record.
	baseline.RecordDumpLockMode(dumpDir, lockMode, plan.sendLockFlags)
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
//     "consistent snapshot attempt" in mydumper's own wording, which NO_LOCK is
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
func buildConsoleMydumperArgs(host string, port uint16, user string, schemas []string, dumpDir string, lockMode baseline.LockMode, sendLockFlags bool, tlsArgs []string, listSchemas bool) []string {
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
	case len(schemas) > 1 && listSchemas && !slices.ContainsFunc(schemas, func(s string) bool { return strings.Contains(s, ",") }):
		args = append(args, "--database", strings.Join(schemas, ","))
	case len(schemas) > 1:
		args = append(args, "--regex", "^("+strings.Join(schemas, "|")+")\\.")
	default:
		args = append(args, "--regex", systemSchemaExcludeRegex)
	}
	// --outputdir last: docker wrapper scripts read the last arg for the mount.
	// TLS options (#1996): how this source's TLS mode is spelled for this
	// mydumper build, from mydumperTLSArgs.
	args = append(args, tlsArgs...)
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
	where, args := dumpableTablesWhere(schemas)
	return "SELECT COUNT(*) FROM information_schema.TABLES WHERE " + where, args
}

// dumpableTablesWhere is the information_schema.TABLES filter for the tables
// a console dump selects, shared by the no-lock count above and the disk
// check's size estimate (dumpSizeQuery, #1938) so the two cannot drift apart.
func dumpableTablesWhere(schemas []string) (string, []any) {
	// SYSTEM VERSIONED is how MariaDB lists a system-versioned table; it is
	// a table mydumper dumps like any other (#1993 reads this list to find
	// tables a snapshot lacks, and would never see one created that way).
	const base = "TABLE_TYPE IN ('BASE TABLE', 'SYSTEM VERSIONED') AND "
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
// multiTableNoLockCheck is warnIfMultiTableNoLock behind a seam, so a test
// can see the TLS settings runMydumper hands it.
var multiTableNoLockCheck = warnIfMultiTableNoLock

func warnIfMultiTableNoLock(ctx context.Context, sourceDSN string, ssl config.SSL, schemas []string) {
	db, err := connectSource(sourceDSN, ssl)
	if err != nil {
		// Warn, not Debug: when this check cannot run, the warning it exists
		// to give (a no-lock dump of several tables is not point-consistent)
		// is lost, and the log is the only place that can say so.
		slog.Warn("baseline: could not open the source to count its tables, so a no-lock dump of several tables is not warned about", "error", err)
		return
	}
	defer db.Close()

	count, err := countDumpableTables(ctx, db, schemas)
	if err != nil {
		slog.Warn("baseline: could not count the source's tables, so a no-lock dump of several tables is not warned about", "error", err)
		return
	}
	if count > 1 {
		slog.Warn("baseline: dumping multiple tables under no-lock — each table's snapshot is "+
			"anchored at a slightly different instant (no cross-table synchronization barrier), so a multi-table "+
			"reconstruct (e.g. a parent/child FK pair) can be mutually inconsistent; set "+
			"BINTRAIL_CONSOLE_BASELINE_LOCK_MODE=lock-all for a point-consistent snapshot (needs only LOCK TABLES, and "+
			"is the mode that works on managed MySQL such as RDS), or ftwrl (requires the "+
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
	s.recordRunFor("", serverID, serverName, rec, runErr)
}

// recordRunFor is recordRun for the journaled job runID ("" for none): the
// record and that job's "recorded" mark are one save (#2180).
func (s *baselineSupervisor) recordRunFor(runID, serverID, serverName string, rec console.BaselineRunRecord, runErr error) {
	if s.history == nil {
		return
	}
	rec.ServerID = serverID
	rec.ServerName = serverName
	rec.FinishedAt = time.Now().UTC().Format(time.RFC3339)
	if runErr != nil {
		rec.Error = runErr.Error()
	}
	rec.RefusedTables, rec.RefusedTablesOmitted = refusedTablesIn(runErr)
	if err := s.history.FinishJob(runID, rec); err != nil {
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

// mydumperEnv is the environment mydumper runs with: the daemon's, the
// source password out of band (MYSQL_PWD, #811), and a UTF-8 character
// locale unless the one in effect already names UTF-8. Measured with mydumper
// 1.0.3-1 in the console image, which sets no locale: a schema name outside
// ASCII on its command line ("ventas_año") fails with "option parsing failed:
// Invalid byte sequence in conversion input" before it connects (#2006).
// LANG=C fails the same way, so "set" is not enough: the effective value
// (LC_ALL, else LC_CTYPE, else LANG) must name UTF-8. Only the character
// type changes (LC_CTYPE), unless LC_ALL is set, which overrides it and so is
// what has to change. An operator's UTF-8 locale is kept as it is, even one
// not installed on the host (which this cannot see).
func mydumperEnv(base []string, password string) []string {
	get := func(k string) string {
		for i := len(base) - 1; i >= 0; i-- {
			if name, v, ok := strings.Cut(base[i], "="); ok && name == k {
				return v
			}
		}
		return ""
	}
	env := append([]string(nil), base...)
	if password != "" {
		env = append(env, "MYSQL_PWD="+password)
	}
	effective := get("LC_ALL")
	if effective == "" {
		effective = get("LC_CTYPE")
	}
	if effective == "" {
		effective = get("LANG")
	}
	if u := strings.ToUpper(effective); strings.Contains(u, "UTF-8") || strings.Contains(u, "UTF8") {
		return env
	}
	if get("LC_ALL") != "" {
		return append(env, "LC_ALL=C.UTF-8")
	}
	return append(env, "LC_CTYPE=C.UTF-8")
}

// leftOutOf is the converter's left-out tables in the console's type.
func leftOutOf(st baseline.Stats) []console.LeftOut {
	out := make([]console.LeftOut, len(st.TablesLeftOut))
	for i, l := range st.TablesLeftOut {
		out[i] = console.LeftOut{Table: l.Table, Reason: l.Reason, Schema: l.Schema, Name: l.Name}
	}
	return out
}

// listUserSchemas lists the source's schemas a full read with no schema list
// covers: every one but the system schemas (the same ones
// systemSchemaExcludeRegex leaves out). Indirected for tests.
var listUserSchemas = func(ctx context.Context, sourceDSN string, ssl config.SSL) ([]string, error) {
	db, err := connectSource(sourceDSN, ssl)
	if err != nil {
		return nil, err
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(ctx, sourceTablesTimeout)
	defer cancel()
	rows, err := db.QueryContext(ctx, "SELECT SCHEMA_NAME FROM information_schema.SCHEMATA "+
		"WHERE SCHEMA_NAME NOT IN ('mysql', 'sys', 'performance_schema', 'information_schema') ORDER BY SCHEMA_NAME")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		out = append(out, name)
	}
	return out, rows.Err()
}
