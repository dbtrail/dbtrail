package consoleapp

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"regexp"
	"sync"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/ext"
	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/doctor"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/pgstreamrun"
	"github.com/dbtrail/dbtrail/internal/serverid"
	"github.com/dbtrail/dbtrail/internal/streamdeps"
	"github.com/dbtrail/dbtrail/internal/streamrun"
)

// monitorSupervisor is the control plane behind `bintrail up --console`: it
// implements console.MonitorController and runs one supervised streamrun.One per
// monitored registry entry. The approved architecture is index-DATABASE per
// source — each monitored entry streams into its own database
// (bintrail_idx_<entry-id>) on the daemon's index MySQL server, so per-source
// state stays structurally isolated (single-row stream_state per DB, no
// cross-source schema_snapshots confusion) and the console's existing
// multi-connection switcher lists it with zero new read code.
//
// The supervisor is a WRITER — it creates databases, tables, and runs
// EnsureSchema on the per-source DBs it provisions, exactly the role the cmd
// layer already plays for the boot DSN. The console's "registry servers are
// never migrated by request handlers" invariant is untouched: the console
// bundle for a monitored entry still opens the (already-provisioned) DB
// read-only.
type monitorSupervisor struct {
	// baseCtx is the daemon's lifecycle: streams derive from it, NOT from the
	// HTTP request that started them.
	baseCtx context.Context
	// bootIndexDSN is the daemon's index server connection; per-source
	// databases are derived from it (same server, same credentials,
	// different DBName).
	bootIndexDSN string
	// registry lists the other monitored entries — Doctor compares a
	// candidate source against them for replica/duplicate detection. May be
	// nil (tests); the check is skipped then.
	registry *console.Registry
	// rotateRetain is the rotation window the daemon's built-in loop uses; the
	// Doctor capacity projection assumes it (0 = rotation disabled). Set at
	// construction so the supervisor never reads the cmd-layer upRotationCfg global.
	rotateRetain time.Duration
	// streamFn runs one supervised MySQL/MariaDB stream; a seam for unit tests,
	// streamrun.One in production.
	streamFn func(ctx context.Context, cfg streamrun.Config) error
	// pgStreamFn runs one supervised PostgreSQL stream; pgstreamrun.One in
	// production, a seam for tests. Selected when the entry's flavor is postgres.
	pgStreamFn func(ctx context.Context, cfg pgstreamrun.Config) error
	// loopbackRetry is where the doctor retries a failed connection to
	// localhost or 127.0.0.1, to prove the container case (#1803):
	// doctor.DockerHostRetry in production, a seam for tests.
	loopbackRetry func(host, port string) string

	mu   sync.Mutex
	jobs map[string]*monitorJob
	wg   sync.WaitGroup
}

// Supervisor health thresholds. Vars, not consts, so tests can shrink them.
var (
	// monitorStalledAfter: a running stream that has neither saved a
	// checkpoint nor flushed a batch for this long is reported "stalled".
	// The checkpoint ticker fires even with zero events, so an idle-but-
	// healthy source never trips this — only a genuinely wedged loop does.
	monitorStalledAfter = 5 * time.Minute
	// monitorGiveUpAfter: a stream that has been crash-looping continuously
	// for this long (no healthy run in between) stops retrying and reports
	// a permanent "failed" — the circuit breaker against a misconfigured
	// source retrying forever. Press Start (or restart the daemon) to re-arm.
	monitorGiveUpAfter = 6 * time.Hour
	// Crash-loop backoff: retry delay doubles from base to cap; a run that
	// survives monitorHealthyReset resets both the attempt counter and the
	// circuit-breaker clock.
	monitorBackoffBase  = 15 * time.Second
	monitorBackoffCap   = 5 * time.Minute
	monitorHealthyReset = 10 * time.Minute
	// monitorReloadDrainTimeout: how long ReloadSchema waits for a cancelled
	// stream to release its advisory lock before giving up. Start's GET_LOCK
	// has a ZERO timeout, so relaunching early fails outright instead of
	// queueing — waiting is the only way to hand the lock over.
	monitorReloadDrainTimeout = 15 * time.Second
)

// monitorJob is one supervised stream.
type monitorJob struct {
	cancel context.CancelFunc
	done   chan struct{}
	// indexDSN is the entry's per-source index database — set once at job
	// creation (before the job is published), immutable after. Stop uses it
	// to clear the durable gap-loss record with its own short-lived
	// connection (lockDB belongs to the run goroutine; sharing it from Stop
	// would race Start's provisioning window).
	indexDSN string
	// lockDB's single dedicated connection holds the advisory lock for this
	// entry; closing it releases the lock. Written by Start before the run
	// goroutine launches and read only by run's teardown — never from other
	// goroutines.
	lockDB *sql.DB

	mu      sync.Mutex
	state   string // stored: pending|running|failed|stopped
	lastErr string
	since   time.Time
	// lastProgress is when the stream last proved liveness (checkpoint saved
	// or batch flushed) — feeds the derived "stalled" state.
	lastProgress time.Time
	// sourceConnected: this run's stream has opened the source connection
	// (the OnSourceConnected hook). Every new run starts false.
	sourceConnected bool
	// phase is the long startup step the stream is inside right now (the
	// OnPhase hook), "" when none. EVERY transition out of it clears it —
	// set, setRetrying and the pending→running flip in progress() — because a
	// phase only ever describes the run that is executing, and must not
	// survive a failure, a retry wait, a stop, or capture actually starting
	// (#1690). progress() is belt: the only phase today is announced before
	// StartSync and cleared before it, so no checkpoint or flush can land
	// while one is set. A phase added LATER in startup would not have that
	// luxury, and a row reading CLEANING UP over a capturing stream is the
	// exact kind of stale reassurance this change exists to remove.
	phase string
	// retrying: the stored failure is one run() will retry after its backoff,
	// not a setup failure in Start or a give-up. Any other set clears it.
	retrying bool
	// lostPosition, when non-empty, records that an unfillable binlog gap
	// forced an auto-advance: events were permanently lost. The fact is
	// also persisted in stream_state (gap_lost_at/_detail) and re-hydrated
	// by Start, so it survives daemon restarts; only an explicit Stop (the
	// operator's acknowledgment) clears it — feeds the derived
	// "lost_position" state.
	lostPosition string
}

func (j *monitorJob) set(state, lastErr string) {
	j.mu.Lock()
	j.state, j.lastErr, j.since = state, lastErr, time.Now().UTC()
	j.retrying = false
	j.phase = "" // a state change ends whatever startup step was running
	if state == "pending" {
		j.sourceConnected = false // every run connects again
	}
	j.mu.Unlock()
}

// storedState returns the raw state-machine value (pending|running|failed|
// stopped) without the derived stalled/lost_position presentation — for
// callers that need goroutine liveness, not operator-facing health.
func (j *monitorJob) storedState() string {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.state
}

// progress records stream liveness and performs the pending→running flip:
// the supervisor reports "pending" from launch until the stream saves its
// first checkpoint (or flushes its first batch) — before that the goroutine
// is still connecting/snapshotting and a RUNNING badge would lie (#407).
func (j *monitorJob) progress() {
	j.mu.Lock()
	j.lastProgress = time.Now().UTC()
	if j.state == "pending" {
		j.state, j.lastErr, j.since = "running", "", j.lastProgress
		j.phase = "" // capture is producing: no startup step is still running
	}
	j.mu.Unlock()
}

// setRetrying stores a failure the run loop will retry after its backoff.
func (j *monitorJob) setRetrying(lastErr string) {
	j.mu.Lock()
	j.state, j.lastErr, j.since, j.retrying = "failed", lastErr, time.Now().UTC(), true
	j.phase = ""
	j.mu.Unlock()
}

// markSourceConnected records that this run's stream reached the source.
func (j *monitorJob) markSourceConnected() {
	j.mu.Lock()
	j.sourceConnected = true
	j.mu.Unlock()
}

// setPhase records the long startup step the stream is inside ("" = none).
func (j *monitorJob) setPhase(phase string) {
	j.mu.Lock()
	j.phase = phase
	j.mu.Unlock()
}

func (j *monitorJob) markLostPosition(detail string) {
	j.mu.Lock()
	j.lostPosition = detail
	j.mu.Unlock()
}

// streamHooks wires this job as its MySQL stream's liveness observer.
func (j *monitorJob) streamHooks() *streamrun.Hooks {
	return &streamrun.Hooks{
		OnCheckpoint:      j.progress,
		OnIndexed:         func(int64) { j.progress() },
		OnGapAutoAdvance:  j.markLostPosition,
		OnSourceConnected: j.markSourceConnected,
		OnPhase:           j.setPhase,
	}
}

// pgStreamHooks wires this job as its PostgreSQL stream's liveness observer.
// No OnGapAutoAdvance: a lost PG slot is fatal (pgstreamrun.One returns it and
// the supervisor reconnects), not a continue-after-loss — the durable
// gap_lost_detail persisted by the capturer is re-hydrated by Start instead.
// No OnPhase either: the phase that exists (#1690) is the MySQL resume-time
// dedup, and PG capture has no equivalent — it resumes from the slot's
// confirmed LSN, so there is no replayed window to delete first.
func (j *monitorJob) pgStreamHooks() *pgstreamrun.Hooks {
	return &pgstreamrun.Hooks{
		OnCheckpoint:      j.progress,
		OnIndexed:         func(int64) { j.progress() },
		OnSourceConnected: j.markSourceConnected,
	}
}

// snapshot reports the job's state, deriving the two "running but not
// healthy" presentations at read time (the stored state machine stays
// pending|running|failed|stopped):
//   - "stalled":       running, but no checkpoint/flush for monitorStalledAfter
//   - "lost_position": running, but a gap auto-advance lost events
//
// A wedged stream beats a historical data-loss note, so stalled wins when
// both apply.
func (j *monitorJob) snapshot() console.MonitorStatus {
	j.mu.Lock()
	defer j.mu.Unlock()
	st := console.MonitorStatus{State: j.state, LastError: j.lastErr, SourceConnected: j.sourceConnected, Retrying: j.retrying, Phase: j.phase}
	if j.state == "running" {
		if idle := time.Since(j.lastProgress); !j.lastProgress.IsZero() && idle > monitorStalledAfter {
			st.State = "stalled"
			st.LastError = fmt.Sprintf("no progress for %s: no events indexed and no checkpoint saved; the stream is connected but not advancing", idle.Round(time.Second))
		} else if j.lostPosition != "" {
			st.State = "lost_position"
			st.LastError = j.lostPosition
		}
	}
	if !j.since.IsZero() {
		st.Since = j.since.Format(time.RFC3339)
	}
	return st
}

// newMonitorSupervisor builds the control plane. reg may be nil (tests) —
// Doctor's replica/duplicate detection is skipped then.
func newMonitorSupervisor(baseCtx context.Context, bootIndexDSN string, reg *console.Registry, retain time.Duration) *monitorSupervisor {
	return &monitorSupervisor{
		baseCtx:       baseCtx,
		bootIndexDSN:  bootIndexDSN,
		registry:      reg,
		rotateRetain:  retain,
		streamFn:      streamrun.One,
		pgStreamFn:    pgstreamrun.One,
		loopbackRetry: doctor.DockerHostRetry,
		jobs:          map[string]*monitorJob{},
	}
}

// dbNameRE is the only shape the supervisor will CREATE DATABASE for. Derived
// names always match; a hand-edited registry DSN with anything fancier is
// refused rather than interpolated into DDL.
var dbNameRE = regexp.MustCompile(`^[A-Za-z0-9_$]+$`)

// DeriveIndexDSN implements console.MonitorController: the daemon's index
// server with a per-entry database name.
func (m *monitorSupervisor) DeriveIndexDSN(entryID string) (string, error) {
	if !dbNameRE.MatchString(entryID) {
		return "", fmt.Errorf("entry id %q cannot form a database name", entryID)
	}
	cfg, err := mysql.ParseDSN(m.bootIndexDSN)
	if err != nil {
		return "", fmt.Errorf("daemon index DSN: %s", config.ScrubDSNError(err, m.bootIndexDSN))
	}
	cfg.DBName = "bintrail_idx_" + entryID
	return cfg.FormatDSN(), nil
}

// Doctor implements console.MonitorController by running the same preflight
// as `bintrail doctor` and mapping the report to the console's wire shape.
// The entry's index DB may not exist yet — doctor treats that as fine (init
// creates it), per #384.
func (m *monitorSupervisor) Doctor(ctx context.Context, e console.ServerEntry) (*console.DoctorReport, error) {
	return m.doctor(ctx, e)
}

// DoctorUnsaved implements console.MonitorController: the same checks as
// Doctor on a server not saved yet, source half only (#1767).
func (m *monitorSupervisor) DoctorUnsaved(ctx context.Context, e console.ServerEntry) (*console.DoctorReport, error) {
	return m.doctor(ctx, e, doctor.ForUnsavedServer())
}

func (m *monitorSupervisor) doctor(ctx context.Context, e console.ServerEntry, opts ...doctor.BuildOption) (*console.DoctorReport, error) {
	if e.SourceDSN == "" {
		return nil, errors.New("entry has no source configured")
	}
	// The per-source databases are rotated by the daemon's built-in loop, so
	// the capacity projection uses its window (0 when rotation is disabled).
	// PostgreSQL runs the pgstreamrun preflight (slot / wal_level / publication
	// coverage / REPLICA IDENTITY FULL) instead, which returns the identical
	// *doctor.Report shape so the mapping loop below is unchanged. A missing slot
	// is a Skip (never blocks first start); a publication that doesn't cover the
	// tables is a Fail — the operator must CREATE it (validate-don't-create).
	var r *doctor.Report
	switch e.SourceFlavor() {
	case console.FlavorPostgres:
		r = pgstreamrun.BuildPGReport(ctx, pgstreamrun.PGDoctorConfig{
			QueryDSN:    e.SourceDSN,
			SlotName:    e.SourceSlot,
			Publication: e.SourcePublication,
			Schemas:     e.Schemas,
		})
	default:
		opts = append(opts, doctor.WithLoopbackRetry(m.loopbackRetry))
		r = doctor.Build(ctx, e.SourceDSN, e.DSN, e.Schemas, m.rotateRetain, opts...)
	}
	out := &console.DoctorReport{
		Passed:   r.Passed,
		Failed:   r.Failed,
		Warnings: r.Warnings,
		Skipped:  r.Skipped,
		Checks:   make([]console.DoctorCheck, len(r.Checks)),
	}
	for i, c := range r.Checks {
		out.Checks[i] = console.DoctorCheck{
			Name:        c.Name,
			Status:      string(c.Status),
			Detail:      config.ScrubDSNText(c.Detail, e.SourceDSN, e.DSN),
			Remediation: c.Remediation,
			Kind:        c.Kind,
			Subjects:    c.Subjects,
			Statements:  c.Statements,
		}
		// Per-check trace so `--log-level debug` shows the full preflight from
		// the host, not just the pass/fail tally returned to the browser.
		slog.Debug("monitor: preflight check",
			"server", e.Name, "id", e.ID,
			"check", out.Checks[i].Name, "status", out.Checks[i].Status,
			"detail", out.Checks[i].Detail)
	}

	// Replica/duplicate detection against the other monitored entries —
	// warn-only per the approved decision (#402): an amber card, never a
	// block. Supervisor-only: the standalone `bintrail doctor` has no
	// registry to compare against.
	if c := m.replicaOverlapCheck(ctx, e); c != nil {
		c.Detail = config.ScrubDSNText(c.Detail, e.SourceDSN, e.DSN)
		out.Checks = append(out.Checks, *c)
		tallyCheck(out, c.Status)
	}
	return out, nil
}

// tallyCheck counts one appended check into the report's totals. A "fail" is
// a failure: Test connection's answer for a new server is Failed == 0 (#1767),
// so a failure counted as a pass would read as ready.
func tallyCheck(out *console.DoctorReport, status string) {
	switch status {
	case "fail":
		out.Failed++
	case "warn":
		out.Warnings++
	case "skip":
		out.Skipped++
	default:
		out.Passed++
	}
}

// Start implements console.MonitorController: provision the per-source index
// database (CREATE DATABASE + tables + schema migration), take the advisory
// lock, and launch the supervised stream on the daemon's lifecycle.
// Idempotent for an entry that is already running or starting.
func (m *monitorSupervisor) Start(ctx context.Context, e console.ServerEntry) error {
	if e.SourceDSN == "" {
		return errors.New("entry has no source configured")
	}
	if e.DSN == "" {
		return errors.New("entry has no index DSN (derive or set one first)")
	}

	m.mu.Lock()
	if j, ok := m.jobs[e.ID]; ok {
		// Gate on the STORED state, not the derived presentation: stalled
		// and lost_position are running variants (the goroutine still holds
		// the advisory lock; superseding it would deadlock on our own lock —
		// restart a stalled stream via Stop+Start), and checking the stored
		// machine means new derived states can never fall through to the
		// cancel below by omission.
		switch j.storedState() {
		case "running", "pending":
			m.mu.Unlock()
			return nil // idempotent
		}
		// A failed/stopped job is superseded below; make sure it is dead.
		j.cancel()
	}
	// Reserve the slot as pending while provisioning runs outside the lock.
	jobCtx, cancel := context.WithCancel(m.baseCtx)
	job := &monitorJob{cancel: cancel, done: make(chan struct{}), indexDSN: e.DSN}
	job.set("pending", "")
	m.jobs[e.ID] = job
	m.mu.Unlock()

	fail := func(err error) error {
		scrubbed := config.ScrubDSNError(err, e.SourceDSN, e.DSN)
		job.set("failed", scrubbed)
		cancel()
		close(job.done)
		return errors.New(scrubbed)
	}

	// A panic during provisioning would otherwise leave this entry reserved as
	// "pending" for the life of the process, which nothing heals: Start is
	// idempotent on "pending" (the switch above), so a later press of Start
	// returns success and does nothing; snapshot() ages out only "running", so
	// it never becomes stalled; Reconcile goes through Start too. The servers
	// list counts "pending" as live and therefore offers Stop rather than
	// Start, so the operator is not even shown the control that would help.
	//
	// That state is reachable only because a caller in this daemon now RECOVERS
	// such a panic instead of dying on it: the schema-snapshot refresh reaches
	// Start through ReloadSchema, and #1497 guards its goroutines. So make the
	// reserved slot terminal FIRST, with the same fail() every error path here
	// uses, and then re-panic. The re-panic is the point: it leaves the crash
	// exactly as loud as it is today for every caller that does not guard, and
	// it costs no diagnostics, because a stack taken by a later recover still
	// walks the original panicking frames through a re-panic.
	//
	// launched scopes this to the provisioning window. m.run closes job.done on
	// exit, so calling fail once that goroutine exists would close it twice.
	launched := false
	defer func() {
		if r := recover(); r != nil {
			if !launched {
				_ = fail(fmt.Errorf("internal error: %v", r))
			}
			panic(r)
		}
	}()

	// ── Provision the per-source index database ──────────────────────────
	idxCfg, err := mysql.ParseDSN(e.DSN)
	if err != nil {
		return fail(fmt.Errorf("index DSN: %w", err))
	}
	if idxCfg.DBName == "" || !dbNameRE.MatchString(idxCfg.DBName) {
		return fail(fmt.Errorf("index database name %q is not provisionable", idxCfg.DBName))
	}
	if err := indexer.EnsureDatabase(idxCfg, idxCfg.DBName, nil); err != nil {
		return fail(err)
	}
	idxDB, err := config.Connect(e.DSN)
	if err != nil {
		return fail(fmt.Errorf("connect provisioned index: %w", err))
	}
	if err := indexer.CreateIndexTables(ctx, idxDB, 48, false, nil); err != nil {
		idxDB.Close()
		return fail(err)
	}
	if err := indexer.EnsureSchema(idxDB); err != nil {
		idxDB.Close()
		return fail(fmt.Errorf("schema migration: %w", err))
	}
	// Say WHERE this source's events will land (#1731). The operator runs the
	// CLI with the daemon's own env file, which names the boot index, and gets
	// zero of everything — during an incident that reads as "no history for
	// this table". The name is in the console's API and in a collapsed part of
	// the edit form; neither is where anyone looks first. No password: the
	// database name and the server's address are what someone needs to point
	// --index-dsn at it.
	slog.Info("source index database ready",
		"server", e.Name, "server_id", e.ID,
		"index_database", idxCfg.DBName, "index_address", idxCfg.Addr,
		"note", "run the CLI against THIS database to see this source's events")
	// Re-hydrate a durable gap-loss record (#402): once the stream persisted
	// its advanced checkpoint, a restarted daemon sees no gap and the hook
	// never re-fires — the lost_position state must be restored from
	// stream_state or the data loss silently un-surfaces. Cleared only by an
	// explicit Stop (the operator's acknowledgment). ErrNoRows is the normal
	// fresh-start case; any other error means a recorded loss may go
	// un-surfaced this run, which deserves a breadcrumb.
	var gapDetail sql.NullString
	err = idxDB.QueryRowContext(ctx,
		`SELECT gap_lost_detail FROM stream_state WHERE id = 1`).Scan(&gapDetail)
	switch {
	case err == nil && gapDetail.Valid && gapDetail.String != "":
		job.markLostPosition(gapDetail.String)
	case err != nil && !errors.Is(err, sql.ErrNoRows):
		slog.Warn("could not re-hydrate gap-loss record; a recorded data loss may not be re-surfaced this run",
			"entry", e.ID, "error", config.ScrubDSNError(err, e.SourceDSN, e.DSN))
	}
	idxDB.Close()

	// ── Advisory lock: refuse to double-stream one entry ─────────────────
	// GET_LOCK is held by a dedicated connection on the index server; a
	// second daemon pointed at the same registry fails here with a clear
	// message instead of double-indexing the source. Closing lockDB (job
	// teardown) releases it.
	lockDB, err := config.Connect(e.DSN)
	if err != nil {
		return fail(fmt.Errorf("connect for advisory lock: %w", err))
	}
	lockDB.SetMaxOpenConns(1)
	lockDB.SetMaxIdleConns(1)
	lockDB.SetConnMaxIdleTime(0)
	lockDB.SetConnMaxLifetime(0)
	var got int
	lockName := "bintrail_monitor_" + e.ID
	if err := lockDB.QueryRowContext(ctx, "SELECT GET_LOCK(?, 0)", lockName).Scan(&got); err != nil {
		lockDB.Close()
		return fail(fmt.Errorf("acquire advisory lock: %w", err))
	}
	if got != 1 {
		lockDB.Close()
		return fail(fmt.Errorf("another bintrail process is already monitoring this server (advisory lock %s is held)", lockName))
	}
	job.lockDB = lockDB

	// ── Launch the supervised stream ─────────────────────────────────────
	// One circuit-breaker loop (run) drives either engine; the flavor only
	// selects which One is called with which config + liveness hooks.
	flavor := e.SourceFlavor()
	serverID, err := m.deriveSourceIdentity(e, flavor)
	if err != nil {
		lockDB.Close()
		return fail(err)
	}
	var runOnce func(context.Context) error
	switch flavor {
	case console.FlavorPostgres:
		pgcfg, cErr := sourcePGStreamConfig(e, serverID, upBatchSize)
		if cErr != nil {
			lockDB.Close()
			return fail(cErr)
		}
		pgcfg.Hooks = job.pgStreamHooks()
		runOnce = func(c context.Context) error { return m.pgStreamFn(c, pgcfg) }
	default:
		cfg := sourceStreamConfig(e, serverID, upBatchSize)
		cfg.Hooks = job.streamHooks()
		runOnce = func(c context.Context) error { return m.streamFn(c, cfg) }
	}

	// Extension source jobs (ext.RegisterSourceJob) run alongside the supervised
	// stream, bound to jobCtx — the per-source lifecycle context, created once per
	// (re)start and cancelled on Stop, daemon shutdown, OR the supervised stream's
	// own terminal exit (crash-loop give-up / clean return — m.run defers
	// job.cancel(), see run). Placing this here (after index-DB provisioning and
	// the advisory lock, before the stream goroutine) ties one set of jobs to each
	// monitored source's lifetime: not per stream-reconnect (m.run reuses jobCtx,
	// so no per-retry goroutine leak), and only for a source this daemon actually
	// streams (the advisory lock holder) — jobCtx dies with the lock, so a second
	// daemon that re-acquires the freed lock never double-runs these jobs.
	// No-op in the stock binary.
	ext.RunSourceJobs(jobCtx, ext.SourceJobInfo{SourceDSN: e.SourceDSN, IndexDSN: e.DSN, Flavor: flavor})

	m.wg.Add(1)
	go m.run(jobCtx, job, e, flavor, runOnce)
	return nil
}

// The Connect rollback finds DiscardNew by type assertion, so a renamed or
// re-signed method would be skipped in silence and leave databases behind.
// This line makes that a compile error instead.
var _ console.NewEntryDiscarder = (*monitorSupervisor)(nil)

// DiscardNew implements console.NewEntryDiscarder (#1803): it
// takes back what a FAILED Start provisioned for a server that the Connect
// check created a moment earlier in the same request — the job slot Start
// reserved, and the per-server index database it may already have created
// (every step of Start after EnsureDatabase can still fail). Without it the
// check's rollback removed the registry entry and left `bintrail_idx_<id>`
// behind on the index server, owned by nothing.
//
// Deliberately narrow, because it DROPs a database:
//   - only the database this supervisor DERIVES for the id, and only when the
//     entry's DSN is exactly that derived DSN — a server that brings its own
//     index is never touched;
//   - never while the job is running or starting: a stream that holds the
//     database is not a failed first start.
//
// The id is minted by the request that calls this, so the derived database
// cannot have held anything before that request.
func (m *monitorSupervisor) DiscardNew(ctx context.Context, e console.ServerEntry) error {
	m.mu.Lock()
	if j, ok := m.jobs[e.ID]; ok {
		switch j.storedState() {
		case "running", "pending":
			m.mu.Unlock()
			return fmt.Errorf("server %s is %s; not discarding it", e.ID, j.storedState())
		}
		j.cancel()
		delete(m.jobs, e.ID)
	}
	m.mu.Unlock()

	derived, err := m.DeriveIndexDSN(e.ID)
	if err != nil {
		return err
	}
	if e.DSN != derived {
		return nil // an index the server brought itself: never ours to drop
	}
	cfg, err := mysql.ParseDSN(derived)
	if err != nil {
		return fmt.Errorf("daemon index DSN: %s", config.ScrubDSNError(err, derived))
	}
	name := cfg.DBName
	if !dbNameRE.MatchString(name) {
		return fmt.Errorf("index database name %q is not one this supervisor provisions", name)
	}
	cfg.DBName = ""
	db, err := config.Connect(cfg.FormatDSN())
	if err != nil {
		return fmt.Errorf("connect to drop %s: %s", name, config.ScrubDSNError(err, derived))
	}
	defer db.Close()
	if _, err := db.ExecContext(ctx, "DROP DATABASE IF EXISTS `"+name+"`"); err != nil {
		return fmt.Errorf("drop %s: %s", name, config.ScrubDSNError(err, derived))
	}
	return nil
}

// deriveSourceIdentity resolves the stream's server_id. MySQL/MariaDB derive it
// from the source DSN (serverid.DeriveServerID parses a MySQL DSN and fails on a
// postgres:// connstring). PostgreSQL identity is the replication slot, so
// server_id is only a stream_state label — an explicit SourceServerID wins,
// else a stable non-zero hash of the (registry-unique) entry id.
func (m *monitorSupervisor) deriveSourceIdentity(e console.ServerEntry, flavor string) (uint32, error) {
	if e.SourceServerID != 0 {
		return e.SourceServerID, nil
	}
	if flavor == console.FlavorPostgres {
		h := fnv.New32a()
		_, _ = h.Write([]byte(e.ID))
		if id := h.Sum32(); id != 0 {
			return id, nil
		}
		return 1, nil
	}
	id, err := serverid.DeriveServerID(e.SourceDSN)
	if err != nil {
		return 0, fmt.Errorf("derive server id: %w", err)
	}
	return id, nil
}

// sourcePGStreamConfig builds the supervised PG stream's pgstreamrun.Config from
// a registry entry (Hooks attached by the caller). The replication DSN is
// derived from the stored query DSN (console.PGReplDSN adds replication=database
// — the one place that derivation lives); the slot and publication are the
// operator-supplied stored fields. Pure — unit-testable without a live DB.
func sourcePGStreamConfig(e console.ServerEntry, serverID uint32, batchSize int) (pgstreamrun.Config, error) {
	replDSN, err := console.PGReplDSN(e.SourceDSN)
	if err != nil {
		return pgstreamrun.Config{}, err
	}
	return pgstreamrun.Config{
		IndexDSN:    e.DSN,
		ReplDSN:     replDSN,
		QueryDSN:    e.SourceDSN,
		SlotName:    e.SourceSlot,
		Publication: e.SourcePublication,
		ServerID:    serverID,
		BatchSize:   streamBatchSize(batchSize),
		Schemas:     e.Schemas,
		Checkpoint:  10 * time.Second,
	}, nil
}

// sourceStreamConfig builds the supervised stream's streamrun.Config from a
// registry entry (Hooks are attached by the caller — they need the live job).
// The source connection's TLS comes from the entry's ssl_* fields (#879): an
// empty SSLMode defaults to "preferred", preserving pre-#879 behavior for
// entries with no TLS configured. Flavor is the entry's resolved source flavor
// ("mysql"/"mariadb" — this builder is only reached in the non-postgres branch):
// without it the stream normalized an empty Flavor to "mysql", so a console-
// monitored MariaDB source was captured with the MySQL GTID parser AND the ext
// source job was told a flavor the pipeline did not actually run with. Pure —
// extracted from Start so the entry→config fan-out (SSL especially) is
// unit-testable without a live DB.
func sourceStreamConfig(e console.ServerEntry, serverID uint32, batchSize int) streamrun.Config {
	sslMode := e.SSLMode
	if sslMode == "" {
		sslMode = "preferred"
	}
	return streamrun.Config{
		IndexDSN:  e.DSN,
		SourceDSN: e.SourceDSN,
		ServerID:  serverID,
		Flavor:    e.SourceFlavor(),
		BatchSize: streamBatchSize(batchSize),
		Schemas:   e.Schemas,
		// MetricsSource keys this stream's Prometheus series; MetricsAddr
		// stays empty on purpose — the daemon serves ONE /metrics endpoint
		// for all supervised streams (per-stream binds would conflict).
		MetricsSource: e.ID,
		Checkpoint:    10,
		SSLMode:       sslMode,
		SSLCA:         e.SSLCA,
		SSLCert:       e.SSLCert,
		SSLKey:        e.SSLKey,
		Format:        "text",
		GapTimeout:    30,
		Deps:          streamdeps.Default(),
	}
}

// run supervises one stream with crash-loop backoff: a stream that errors is
// restarted (15s doubling to a 5m cap, counter reset after 10 healthy
// minutes); the job reports "failed" with the scrubbed error between
// attempts. The job stays "pending" from launch until the stream's first
// checkpoint/flush flips it to "running" via the liveness hooks. A stream
// that crash-loops continuously for monitorGiveUpAfter trips the circuit
// breaker: permanent "failed", no more retries, advisory lock released —
// Start (or a daemon restart) re-arms it. Cancellation (stop verb / daemon
// shutdown) exits cleanly.
//
// Terminal exit (give-up or clean return) also cancels jobCtx via the
// defer below, tearing down the ext source jobs launched on it in Start.
// This keeps the job's lifetime bound to the advisory lock's: the deferred
// lock release and the job cancellation fire together, so a source this
// daemon stops streaming can never leave its source jobs running past the
// lock — otherwise a second daemon that re-acquires the freed lock would
// double-run them.
func (m *monitorSupervisor) run(ctx context.Context, job *monitorJob, e console.ServerEntry, flavor string, runOnce func(context.Context) error) {
	defer m.wg.Done()
	defer close(job.done)
	defer func() {
		if job.lockDB != nil {
			job.lockDB.Close() // releases the advisory lock
		}
	}()
	// Cancel jobCtx on every terminal return (give-up, clean exit, cancellation).
	// Declared last so it runs first under LIFO — the ext source jobs stop before
	// the advisory lock is released above. A no-op when jobCtx is already
	// cancelled (Stop/Shutdown/daemon-cancel paths). Reconnects stay inside the
	// for-loop below, so this never fires mid-retry.
	defer job.cancel()

	var policy crashLoopPolicy
	for {
		job.set("pending", "")
		started := time.Now()
		err := runOnce(ctx)
		if ctx.Err() != nil || err == nil {
			job.set("stopped", "")
			return
		}
		scrubbed := config.ScrubDSNError(err, e.SourceDSN, e.DSN)
		delay, looping, giveUp := policy.failed(started, time.Now())
		if giveUp {
			slog.Error("monitored stream crash-looped past the give-up threshold; not retrying",
				"server", e.Name, "entry", e.ID, "looping_for", looping.Round(time.Minute), "error", scrubbed)
			job.set("failed", fmt.Sprintf("%s (gave up after %s of crash-looping; fix the issue, then press Start to retry)",
				scrubbed, looping.Round(time.Minute)))
			return
		}
		slog.Warn("monitored stream failed; retrying with backoff",
			"server", e.Name, "entry", e.ID, "delay", delay, "error", scrubbed)
		job.setRetrying(scrubbed + " (retrying)")
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			job.set("stopped", "")
			return
		}
	}
}

// ReloadSchema restarts the supervised stream for one entry so it loads the
// newest schema snapshot (#1296). A stream swaps its metadata resolver only on
// a DDL event, so a snapshot taken out of band is invisible to it until it
// restarts — without this, a "refresh the schema snapshot" action would leave
// capture skipping exactly the tables it was asked to fix.
//
// Deliberately NOT Stop+Start. Stop clears the durable gap-loss record as the
// operator's acknowledgment of permanently lost events; routing a schema
// refresh through it would erase a data-loss alarm as a side effect of pressing
// an unrelated button. This cancels the job and relaunches it, leaving
// stream_state.gap_lost_at untouched — Start then re-hydrates lost_position
// from it, so the alarm survives.
//
// reloaded is false with a nil error when this process does not supervise the
// entry: there is no stream here to reload, which is not an error (the snapshot
// it was called for is still valid) but must never be reported as a restart —
// the entry may well be captured by another process, still decoding against the
// old snapshot.
func (m *monitorSupervisor) ReloadSchema(ctx context.Context, e console.ServerEntry) (reloaded bool, err error) {
	m.mu.Lock()
	job, ok := m.jobs[e.ID]
	if ok {
		delete(m.jobs, e.ID)
	}
	m.mu.Unlock()
	if !ok {
		return false, nil
	}
	// restore re-publishes the job on the paths that do not relaunch. Being
	// absent from m.jobs is not merely a wrong Status: ActiveJobs feeds the
	// rotation provider, so a dropped entry stops having its per-source index
	// archived and pruned — silently, with not even the per-cycle warning a
	// terminally-failed job keeps producing.
	restore := func() {
		m.mu.Lock()
		if _, taken := m.jobs[e.ID]; !taken {
			m.jobs[e.ID] = job
		}
		m.mu.Unlock()
	}
	job.cancel()
	// Wait for the run goroutine to finish before relaunching. Its outermost
	// defer closes done AFTER closing lockDB, and Start takes the advisory lock
	// with GET_LOCK(name, 0) — a zero timeout. Starting early would therefore
	// not queue behind the dying stream, it would FAIL to get the lock and mark
	// the new job terminally failed, with no retry loop to converge later.
	//
	// The two non-success branches leave this entry's capture STOPPED: the job
	// is already cancelled and unpublished, and nothing here restarts it. That
	// is reported, not smoothed over — an operator who pressed a refresh button
	// must not be left believing capture is running when it is not.
	select {
	case <-job.done:
	case <-time.After(monitorReloadDrainTimeout):
		restore()
		return false, errors.New("the stream did not stop in time, so capture for this server is now stopping and was NOT restarted; it is still shutting down in the background; press Start once it has, to resume on the new schema snapshot")
	case <-ctx.Done():
		restore()
		return false, fmt.Errorf("capture for this server was stopped and could not be restarted: %w", ctx.Err())
	}
	if err := m.Start(ctx, e); err != nil {
		return false, err
	}
	return true, nil
}

// Stop implements console.MonitorController. Idempotent; waits briefly for
// the stream to flush its final checkpoint. An explicit Stop is also the
// operator's acknowledgment of a recorded data loss: it clears the durable
// gap-loss record so the next Start begins clean. Daemon shutdown does NOT
// come through here (Shutdown cancels jobs directly), so a restart preserves
// the record.
func (m *monitorSupervisor) Stop(ctx context.Context, entryID string) error {
	m.mu.Lock()
	job, ok := m.jobs[entryID]
	if ok {
		delete(m.jobs, entryID)
	}
	m.mu.Unlock()
	if !ok {
		return nil
	}
	// Clear with a short-lived connection of our own: lockDB belongs to the
	// run goroutine (reading it here would race Start's provisioning window,
	// and it is already closed when the stream gave up or exited). On
	// failure the record survives — the next Start re-raises lost_position,
	// which fails safe: a real past loss is re-surfaced, never dropped.
	if job.indexDSN != "" {
		if db, err := config.Connect(job.indexDSN); err != nil {
			slog.Warn("could not clear gap-loss record on stop", "entry", entryID, "error", config.ScrubDSNText(err.Error(), job.indexDSN))
		} else {
			if _, err := db.ExecContext(ctx, `UPDATE stream_state
				SET gap_lost_at = NULL, gap_lost_detail = NULL WHERE id = 1`); err != nil {
				slog.Warn("could not clear gap-loss record on stop", "entry", entryID, "error", config.ScrubDSNText(err.Error(), job.indexDSN))
			}
			db.Close()
		}
	}
	job.cancel()
	select {
	case <-job.done:
	case <-time.After(15 * time.Second):
		return errors.New("stream did not stop within 15s; it will finish shutting down in the background")
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

// ActiveJob pairs a supervised entry's id with its per-source index DSN, so the
// rotation provider can look the entry up in the registry (for its ArchiveS3)
// and read its resolved bintrail_id.
type ActiveJob struct {
	EntryID  string
	IndexDSN string
}

// ActiveJobs returns one ActiveJob per supervised job with a known index DSN —
// the per-source databases the built-in rotation covers alongside the boot
// index. Jobs in every state are included: a crash-looping stream's database
// still ages past retention, and a DSN whose database is mid-provisioning logs
// one transient rotation warning and self-heals next tick. A job whose
// provisioning failed TERMINALLY (bad perms, DDL error) keeps producing a
// per-cycle rotation warning until superseded or stopped — deliberate: the
// broken entry should stay loud, and Stop() removes it from the map.
func (m *monitorSupervisor) ActiveJobs() []ActiveJob {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]ActiveJob, 0, len(m.jobs))
	for id, j := range m.jobs {
		if j.indexDSN != "" {
			out = append(out, ActiveJob{EntryID: id, IndexDSN: j.indexDSN})
		}
	}
	return out
}

// Status implements console.MonitorController.
func (m *monitorSupervisor) Status(entryID string) console.MonitorStatus {
	m.mu.Lock()
	job, ok := m.jobs[entryID]
	m.mu.Unlock()
	if !ok {
		return console.MonitorStatus{State: "stopped"}
	}
	return job.snapshot()
}

// Reconcile starts every registry entry whose desired state is "monitoring"
// — called once at daemon boot so a restart resumes exactly what the
// operator had running (streams pick up from their stream_state checkpoints).
// Failures are recorded on the job (visible in the UI) and logged, never
// fatal to the daemon.
func (m *monitorSupervisor) Reconcile(reg *console.Registry) {
	for _, e := range reg.List() {
		if !e.MonitorDesired || e.SourceDSN == "" {
			continue
		}
		if err := m.Start(m.baseCtx, e); err != nil {
			slog.Warn("boot reconcile: could not start monitoring", "server", e.Name, "entry", e.ID, "error", err)
		}
	}
}

// Shutdown stops every stream and waits for their final checkpoints.
func (m *monitorSupervisor) Shutdown() {
	m.mu.Lock()
	for _, j := range m.jobs {
		j.cancel()
	}
	m.mu.Unlock()
	m.wg.Wait()
}

// defaultStreamBatchSize is what a supervised source captured with before
// --batch-size reached it (#1747), and what it still captures with when the
// flag is left at zero by a caller that never parsed it (tests, and any
// future entry point).
const defaultStreamBatchSize = 1000

// streamBatchSize turns the daemon's --batch-size into the batch a supervised
// stream uses. The flag used to reach ONLY the --source-dsn stream typed on
// the command line, so on the deployment the console documents — a daemon
// with no source, every source added from the interface — the documented
// remedy for replication lag ("raise --batch-size") could not be followed at
// all.
//
// No ceiling here on purpose: indexer.New clamps anything above
// indexer.MaxBatchSize and says so, and a second ceiling in a second place is
// how the two drift.
func streamBatchSize(n int) int {
	if n <= 0 {
		return defaultStreamBatchSize
	}
	return n
}
