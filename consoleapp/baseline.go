package consoleapp

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
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
	return &baselineSupervisor{
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
		exportRuns:       make(map[string]*sqlExportRun),
		exportOrphans:    make(map[string]map[string]string),
	}
}

// Trigger starts a baseline in the background; returns console.ErrBaselineRunning
// if one is already in flight for this server.
func (s *baselineSupervisor) Trigger(req console.BaselineRequest) error {
	// Scoped to MySQL/MariaDB: executePG anchors on pgoutput's own
	// consistent-point LSN and never consults lockMode, so refusing a
	// Postgres baseline over a MySQL-only knob would take away a working
	// button for a setting that cannot affect it.
	if s.configErr != nil && req.Flavor != console.FlavorPostgres {
		return s.configErr
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
	defer s.recoverBaselineJob(baselineJobDump, req.ServerID, req.ServerName)
	started := time.Now().UTC()
	stats, uploaded, snapTime, err := s.execute(req)
	rec := console.BaselineRunRecord{
		Kind: console.BaselineRunDump, Trigger: req.Trigger, StartedAt: started.Format(time.RFC3339),
		Tables: stats.TablesProcessed, Rows: stats.RowsWritten, Uploaded: uploaded,
		// The reason this was a full backup travels with the run (#1604):
		// recomputed later it would name whatever is true THEN.
		Why: req.Why, WhyCode: console.BackupWhyCode(req.Why),
	}
	if err == nil && !snapTime.IsZero() {
		rec.SnapshotTime = snapTime.UTC().Format(time.RFC3339)
	}
	s.recordRun(req.ServerID, req.ServerName, rec, err)

	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.jobs[req.ServerID]
	if st == nil { // defensive: never overwritten away under lock, but don't panic
		st = &console.BaselineStatus{}
		s.jobs[req.ServerID] = st
	}
	st.FinishedAt = nowStamp()
	if err != nil {
		st.State = "failed"
		st.LastError = err.Error()
		slog.Error("baseline: snapshot failed", "server", req.ServerName, "id", req.ServerID, "error", err)
		return
	}
	st.State = "succeeded"
	st.LastError = ""
	st.Tables = stats.TablesProcessed
	st.Rows = stats.RowsWritten
	st.Uploaded = uploaded
	slog.Info("baseline: snapshot complete", "server", req.ServerName, "id", req.ServerID,
		"tables", stats.TablesProcessed, "rows", stats.RowsWritten, "uploaded", uploaded)
}

// execute runs the full pipeline: mydumper → baseline.Run → (S3) baseline.Upload.
// For a local-dir destination the Parquet is written there persistently and not
// uploaded; for an S3 destination it is staged under a fresh temp dir, uploaded,
// and the staging removed (so a re-run never re-uploads an old snapshot).
// The third return is the published snapshot's anchor instant (its directory
// name), zero when unknown: the PG producer stamps the snapshot server-side,
// out of this process's sight, and a failed run published nothing.
func (s *baselineSupervisor) execute(req console.BaselineRequest) (baseline.Stats, int, time.Time, error) {
	if req.Flavor == console.FlavorPostgres {
		stats, uploaded, err := s.executePG(req)
		return stats, uploaded, time.Time{}, err
	}
	if err := os.MkdirAll(s.stagingDir, 0o755); err != nil {
		return baseline.Stats{}, 0, time.Time{}, fmt.Errorf("create staging dir: %w", err)
	}

	dumpDir, err := os.MkdirTemp(s.stagingDir, "dump-")
	if err != nil {
		return baseline.Stats{}, 0, time.Time{}, fmt.Errorf("create dump dir: %w", err)
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
	if err := runMydumper(s.ctx, req.SourceDSN, req.Schemas, dumpDir, s.lockMode); err != nil {
		return baseline.Stats{}, 0, time.Time{}, fmt.Errorf("dump: %w", err)
	}

	outputDir := req.LocalDir
	if outputDir == "" { // S3-only: stage then upload, discard staging
		outputDir, err = os.MkdirTemp(s.stagingDir, "baseline-")
		if err != nil {
			return baseline.Stats{}, 0, time.Time{}, fmt.Errorf("create baseline staging dir: %w", err)
		}
		defer os.RemoveAll(outputDir)
	}

	stats, err := baseline.Run(s.ctx, baseline.Config{
		InputDir:    dumpDir,
		OutputDir:   outputDir,
		Compression: "zstd",
		Timestamp:   dumpStartedAt,
		TableDeltas: s.tableDeltas,
	})
	if err != nil {
		return baseline.Stats{}, 0, time.Time{}, fmt.Errorf("convert: %w", err)
	}

	var uploaded int
	if req.S3 != "" {
		// Region/credentials come from the ambient AWS chain (env / ~/.aws / IAM
		// role), like every other S3 read the console does.
		uploaded, err = baseline.Upload(s.ctx, outputDir, req.S3, "", false)
		if err != nil {
			return baseline.Stats{}, 0, time.Time{}, fmt.Errorf("upload: %w", err)
		}
	}
	return stats, uploaded, dumpStartedAt, nil
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

// runMydumper invokes the bundled mydumper binary against the source DSN, writing
// a dump (with binlog coordinates in its metadata, which baseline.Run reads) into
// dumpDir. The image pins the SAME mydumper version the compose baseline-dump
// pipeline uses, so a console-created baseline matches a CLI/compose one exactly.
// lockMode selects the sync mode — see buildConsoleMydumperArgs.
func runMydumper(ctx context.Context, sourceDSN string, schemas []string, dumpDir string, lockMode baseline.LockMode) error {
	host, port, user, password, err := config.ParseSourceDSN(sourceDSN)
	if err != nil {
		return err
	}

	if lockMode.NeedsElevatedPrivileges() {
		// Hard gate, unlike the NO_LOCK warning below: granting BACKUP_ADMIN
		// without RELOAD/FLUSH_TABLES does not fail cleanly in mydumper — it
		// SEGFAULTS (verified against the pinned build, #800). Never skipped.
		if err := checkMydumperPrivileges(ctx, sourceDSN, lockMode, mydumperlock.RemedyConsole, schemas); err != nil {
			return err
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

	args := buildConsoleMydumperArgs(host, port, user, schemas, dumpDir, lockMode)
	cmd := exec.CommandContext(ctx, "mydumper", args...)
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
// (#800, #1377). internal/baseline.LockMode carries the measured comparison of
// the three modes; the two consequences specific to THIS call site:
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
//     attempt it half-privileged, and why this code never silently falls back.
func buildConsoleMydumperArgs(host string, port uint16, user string, schemas []string, dumpDir string, lockMode baseline.LockMode) []string {
	syncMode := lockMode.MydumperValue()
	args := []string{
		"--host", host,
		"--port", strconv.Itoa(int(port)),
		"--user", user,
		"--threads", "4",
		"--compress-protocol",
		"--complete-insert",
		"--sync-thread-lock-mode", syncMode, "--trx-tables",
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
