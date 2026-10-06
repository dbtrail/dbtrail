package consoleapp

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"regexp"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/ext"
	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/doctor"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/installid"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/pgstreamrun"
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
	// stream to release its capture lock before giving up. Relaunching early
	// would not fail (Start waits for a held lock, #2105), but the new job
	// would show "waiting for another DBTrail process" for a lock this same
	// process holds.
	monitorReloadDrainTimeout = 15 * time.Second
)

// monitorJob is one supervised stream.
type monitorJob struct {
	cancel context.CancelFunc
	done   chan struct{}
	// indexDSN is the entry's per-source index database — set once at job
	// creation (before the job is published), immutable after. Stop uses it
	// to clear the durable gap-loss record with its own short-lived
	// connection (the capture lock belongs to the run goroutine; sharing it
	// from Stop would race Start's provisioning window).
	indexDSN string
	// lock is this entry's capture lock (captureLock); releasing it lets
	// another process capture the entry. Written by Start (or by the
	// goroutine that waited for the lock) before the run goroutine launches,
	// and read only by run — never from other goroutines.
	lock *captureLock
	// waitingLock: the job is in Start's background wait for the capture
	// lock (a phase or an index error on screen), so this process streams
	// nothing for it yet.
	waitingLock atomic.Bool

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
	// phaseDetail qualifies phase in a few words (the OnPhaseDetail hook). It
	// is cleared wherever phase is: a detail never outlives its phase.
	phaseDetail string
	// errCode names the cause of the stored failure for the ones a screen
	// acts on (console.MonitorErr*), "" for every other failure. Set with the
	// failure by fail, cleared by every other transition.
	errCode string
	// flavorWarning says the server contradicts the Source type saved with
	// its registry entry. Set on every flavor resolution ("" when they agree);
	// never a failure: the saved type is a hint and capture follows the server.
	flavorWarning string
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
	j.errCode = ""
	j.phase, j.phaseDetail = "", "" // a state change ends whatever startup step was running
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
		j.phase, j.phaseDetail = "", "" // capture is producing: no startup step is still running
	}
	j.mu.Unlock()
}

// setRetrying stores a failure the run loop will retry after its backoff.
func (j *monitorJob) setRetrying(lastErr string) {
	j.fail(lastErr, "", true)
}

// fail stores a failed run together with the code of its cause, in one step so
// no reader sees the failure without it. retrying as in setRetrying.
func (j *monitorJob) fail(lastErr, code string, retrying bool) {
	j.mu.Lock()
	j.state, j.lastErr, j.since, j.retrying = "failed", lastErr, time.Now().UTC(), retrying
	j.errCode = code
	j.phase, j.phaseDetail = "", ""
	j.mu.Unlock()
}

// mariadbErrSlaveSameID is MariaDB's ER_SLAVE_SAME_ID: the error packet a
// binlog reader receives when another one connects with its server id.
const mariadbErrSlaveSameID = 4052

// monitorErrorCode maps a stream's error to the code the console sends with
// it. Matched on the error's type or number, never on its text.
func monitorErrorCode(err error) string {
	if errors.Is(err, streamrun.ErrEarlierCleanupRunning) {
		return console.MonitorErrEarlierCleanup
	}
	// *gomysql.MyError is an error packet from the replication connection;
	// index statements fail with the SQL driver's own type. MySQL numbers a
	// different error 4052 (an account statement's, which a binlog stream
	// never receives), and has no counterpart to classify here: it tells
	// readers apart by the UUID go-mysql generates per connection, so two
	// with one server id are not disconnected.
	var my *gomysql.MyError
	if errors.As(err, &my) && my != nil && my.Code == mariadbErrSlaveSameID {
		return console.MonitorErrSameReplicationID
	}
	return ""
}

// markSourceConnected records that this run's stream reached the source.
// setFlavorWarning records (or clears, with "") the Source type warning.
func (j *monitorJob) setFlavorWarning(w string) {
	j.mu.Lock()
	j.flavorWarning = w
	j.mu.Unlock()
}

func (j *monitorJob) markSourceConnected() {
	j.mu.Lock()
	j.sourceConnected = true
	j.mu.Unlock()
}

// setPhase records the long startup step the stream is inside ("" = none).
func (j *monitorJob) setPhase(phase string) {
	j.mu.Lock()
	j.phase = phase
	j.phaseDetail = ""
	j.mu.Unlock()
}

// setPhaseDetail qualifies the phase the stream is inside. Dropped when no
// phase is set: the hooks fire in order, so that is a detail arriving after
// its phase was cleared by a state change.
func (j *monitorJob) setPhaseDetail(detail string) {
	j.mu.Lock()
	if j.phase != "" {
		j.phaseDetail = detail
	}
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
		OnPhaseDetail:     j.setPhaseDetail,
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
	st := console.MonitorStatus{State: j.state, LastError: j.lastErr, SourceConnected: j.sourceConnected, Retrying: j.retrying, Phase: j.phase, PhaseDetail: j.phaseDetail, ErrorCode: j.errCode, FlavorWarning: j.flavorWarning}
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
		return "", fmt.Errorf("the index DSN DBTrail was started with: %s", config.ScrubDSNError(err, m.bootIndexDSN))
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
		opts = append(opts, doctor.ForConsole(), doctor.WithLoopbackRetry(m.loopbackRetry), doctor.WithSourceSSL(e.SourceSSL()))
		r = doctor.Build(ctx, e.SourceDSN, e.DSN, e.Schemas, m.rotateRetain, opts...)
	}
	out := &console.DoctorReport{
		Passed:   r.Passed,
		Failed:   r.Failed,
		Warnings: r.Warnings,
		Skipped:  r.Skipped,
		Optional: r.Optional,
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
			Optional:    c.Optional,
			Light:       doctor.LightFor(c.Name, c.Kind),
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
		c.Light = doctor.LightOther
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
	return m.start(ctx, e, nil, nil)
}

// errNotCurrent: start was asked to replace a job that is no longer the
// entry's (an operator's Stop or Start got there first).
var errNotCurrent = errors.New("the job to replace is no longer this server's")

// start is Start, with one more condition when expect is not nil: the entry's
// job must still be expect when the slot is reserved, checked under the same
// lock that reserves it. A restart decided in the background (lockLost) uses
// it so it can never undo an operator's Stop that landed in between. When
// reserved is not nil it receives the job this call reserved, so a caller can
// mark that one and never a job an operator started a moment later.
func (m *monitorSupervisor) start(ctx context.Context, e console.ServerEntry, expect *monitorJob, reserved **monitorJob) error {
	if e.SourceDSN == "" {
		return errors.New("entry has no source configured")
	}
	if e.DSN == "" {
		return errors.New("entry has no index DSN (derive or set one first)")
	}

	m.mu.Lock()
	if expect != nil && m.jobs[e.ID] != expect {
		m.mu.Unlock()
		return errNotCurrent
	}
	if j, ok := m.jobs[e.ID]; ok {
		// Gate on the STORED state, not the derived presentation: stalled
		// and lost_position are running variants (the goroutine still holds
		// the capture lock; superseding it would leave the new job waiting on
		// this process's own lock — restart a stalled stream via Stop+Start),
		// and checking the stored
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
	if reserved != nil {
		*reserved = job
	}

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
				if job.lock != nil {
					job.lock.Release() // taken by launch before the panic
				}
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

	// ── Capture lock: one process captures an entry (#2105) ──────────────
	// See captureLock. A second daemon pointed at the same registry, which is
	// what a rolling deployment runs for a while, waits here instead of
	// double-indexing the source, and starts on its own once the first one
	// lets go.
	lockName := monitorLockName(e.ID)
	// runStarted: m.run owns the job (it closes done) once it is launched.
	runStarted := false
	launch := func(lock *captureLock) error {
		job.lock = lock
		// ── Launch the supervised stream ─────────────────────────────────────
		// One circuit-breaker loop (run) drives either engine; the flavor only
		// selects which One is called with which config + liveness hooks.
		flavor := e.SourceFlavor()
		serverID, err := m.deriveSourceIdentity(e, flavor)
		if err != nil {
			lock.Release()
			return fail(err)
		}
		var runOnce func(context.Context) error
		// startJobs launches the extension source jobs (see below) with the
		// flavor capture runs as.
		startJobs := func(f string) {
			ext.RunSourceJobs(jobCtx, entrySourceJobInfo(e, f))
		}
		switch flavor {
		case console.FlavorPostgres:
			pgcfg, cErr := sourcePGStreamConfig(e, serverID, upBatchSize)
			if cErr != nil {
				lock.Release()
				return fail(cErr)
			}
			pgcfg.Hooks = job.pgStreamHooks()
			runOnce = func(c context.Context) error { return m.pgStreamFn(c, pgcfg) }
			startJobs(flavor)
		default:
			// The saved Source type of a MySQL-family entry is a hint: the stream
			// asks the server and captures as what it reports, the jobs start from
			// that, and a contradiction shows on the server's status as a warning.
			cfg := sourceStreamConfig(e, serverID, upBatchSize)
			cfg.Hooks = job.streamHooks()
			cfg.Hooks.OnFlavorResolved = registryFlavorHook(job, e.Flavor, m.flavorCorrector(e.ID), startJobs)
			runOnce = func(c context.Context) error { return explainRegistryFlavorError(m.streamFn(c, cfg)) }
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
		// daemon that re-acquires the freed lock never double-runs these jobs
		// (after a lost lock they stop when run returns, within moments of the
		// heartbeat noticing).
		// No-op in the stock binary. The PostgreSQL branch above starts them
		// directly; the MySQL-family branch starts them from the stream's
		// OnFlavorResolved, still bound to jobCtx and still once per Start.

		launched, runStarted = true, true
		m.wg.Add(1)
		go m.run(jobCtx, job, e, flavor, runOnce)
		return nil
	}
	// jobCtx, not the request's ctx: a browser closing mid-Connect must not
	// show up as an index that did not answer.
	lock, err := tryCaptureLock(jobCtx, e.DSN, lockName)
	if lock != nil {
		return launch(lock)
	}
	// Held by another process, or the index did not answer: wait. Both heal
	// on their own, so neither may become a failure that waits for someone to
	// press Start (#2105).
	scrub := func(err error) string { return config.ScrubDSNError(err, e.SourceDSN, e.DSN) }
	onBusy := func() {
		job.set("pending", "")
		job.setPhase(monitorPhaseLockWaiting)
	}
	var lastWarn time.Time
	onErr := func(err error) {
		job.fail("could not ask the index for the capture lock: "+scrub(err)+" (retrying)", "", true)
		if time.Since(lastWarn) >= time.Minute {
			lastWarn = time.Now()
			slog.Warn("could not ask the index for the capture lock; asking again",
				"server", e.Name, "entry", e.ID, "error", scrub(err))
		}
	}
	if err != nil {
		onErr(err)
	} else {
		onBusy()
		slog.Warn("another DBTrail process holds this server's capture lock; this one waits and starts capturing when that one lets go",
			"server", e.Name, "entry", e.ID, "lock", lockName)
	}
	// The waiter owns the job from here: it closes done on every path, so the
	// provisioning panic guard above must not call fail on it again.
	launched = true
	job.waitingLock.Store(true)
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		// Nothing up the stack recovers this goroutine, and a panic here would
		// end the whole daemon: capture for every server. Fail this job
		// instead, loudly, as the baseline jobs do (#1472).
		defer func() {
			if r := recover(); r != nil {
				slog.Error("internal error while waiting to start capture", "server", e.Name, "entry", e.ID,
					"panic", r, "stack", string(debug.Stack()))
				if !runStarted {
					if job.lock != nil {
						job.lock.Release() // or it stays held, and every later start waits on it
					}
					job.set("failed", fmt.Sprintf("internal error: %v", r))
					cancel()
					close(job.done)
				}
			}
		}()
		stopped := func() {
			// jobCtx ended: Stop, daemon shutdown, or a Start that superseded
			// this job. Nothing was launched, so nothing else closes done.
			job.set("stopped", "")
			close(job.done)
		}
		lock, err := waitCaptureLock(jobCtx, e.DSN, lockName, onBusy, onErr)
		if err != nil {
			stopped()
			return
		}
		// Held now: no longer "waiting for another DBTrail", nor an index
		// error. Starting, from the screen's point of view.
		job.set("pending", "")
		// Taking a lock someone else held: that one may have LOST it rather
		// than let go (a cut connection), and it keeps writing until its own
		// heartbeat notices. Give it that long before writing beside it.
		select {
		case <-time.After(captureLockTakeoverDelay()):
		case <-jobCtx.Done():
			lock.Release()
			stopped()
			return
		}
		slog.Info("took the capture lock; starting capture", "server", e.Name, "entry", e.ID, "lock", lockName)
		job.waitingLock.Store(false)
		if err := launch(lock); err != nil {
			// On the direct path the caller logs this; here nobody would.
			slog.Error("could not start capture after taking the capture lock", "server", e.Name, "entry", e.ID, "error", err)
		}
	}()
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
		return fmt.Errorf("the index DSN DBTrail was started with: %s", config.ScrubDSNError(err, derived))
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
// from the source DSN and this installation's index (serverid.DeriveForInstall
// parses a MySQL DSN and fails on a postgres:// connstring). PostgreSQL identity is the replication slot, so
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
	// e.DSN is this source's own index database: with the index server's
	// identity it makes the id this installation's, so another installation
	// capturing the same source does not derive the same one.
	base := m.baseCtx
	if base == nil {
		base = context.Background()
	}
	ctx, cancel := context.WithTimeout(base, 15*time.Second)
	defer cancel()
	id, sourceOnly, err := installid.DeriveForInstall(ctx, e.SourceDSN, e.DSN)
	if err != nil {
		return 0, fmt.Errorf("derive server id: %w", err)
	}
	if sourceOnly != nil {
		installid.WarnSourceOnly(id, e.Name, sourceOnly)
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
// The source connection's TLS comes from the entry's ssl_* fields (#879) via
// ServerEntry.SourceSSL, the one place an empty SSLMode becomes "preferred":
// the console's checks of the same source read it there too. Flavor is the entry's resolved source flavor
// ("mysql"/"mariadb" — this builder is only reached in the non-postgres branch):
// without it the stream normalized an empty Flavor to "mysql", so a console-
// monitored MariaDB source was captured with the MySQL GTID parser AND the ext
// source job was told a flavor the pipeline did not actually run with. Pure —
// extracted from Start so the entry→config fan-out (SSL especially) is
// unit-testable without a live DB.
func sourceStreamConfig(e console.ServerEntry, serverID uint32, batchSize int) streamrun.Config {
	ssl := e.SourceSSL()
	return streamrun.Config{
		IndexDSN:  e.DSN,
		SourceDSN: e.SourceDSN,
		ServerID:  serverID,
		// Never declared: the Source type saved with the entry is a hint
		// (registryFlavorHook compares it), the stream detects the flavor.
		Flavor:    "",
		BatchSize: streamBatchSize(batchSize),
		Schemas:   e.Schemas,
		// MetricsSource keys this stream's Prometheus series; MetricsAddr
		// stays empty on purpose — the daemon serves ONE /metrics endpoint
		// for all supervised streams (per-stream binds would conflict).
		MetricsSource: e.ID,
		Checkpoint:    10,
		SSLMode:       ssl.Mode,
		SSLCA:         ssl.CA,
		SSLCert:       ssl.Cert,
		SSLKey:        ssl.Key,
		Format:        "text",
		GapTimeout:    30,
		Deps:          streamdeps.Default(),
	}
}

// explainRegistryFlavorError rewrites the fix in a "could not detect the
// source flavor" refusal for a server saved in the console: it has no
// --source-flavor to point at. Any other error passes through unchanged.
func explainRegistryFlavorError(err error) error {
	var ue *metadata.FlavorUndetectedError
	if errors.As(err, &ue) {
		ue.Fix = "Check that DBTrail can still connect to this server with its saved user and password."
	}
	return err
}

// registryFlavorHook is the OnFlavorResolved of a supervised MySQL-family
// stream. On every resolution (a restart re-checks) it saves the flavor the
// server reported as the entry's Source type through correct, so the server
// list, the snapshot trigger and the capture status read all say what capture
// runs as. The warning on the server's status is only for a correction that
// could not be saved (correct nil means there is no registry to save to).
// The source jobs start once.
func registryFlavorHook(job *monitorJob, hint string, correct func(string) (bool, error), startJobs func(string)) func(string) {
	once := streamrun.FlavorOnce(startJobs)
	return func(flavor string) {
		var correctErr error
		if correct == nil {
			correctErr = errors.New("no server list to save it to")
		} else if changed, err := correct(flavor); err != nil {
			correctErr = err
			slog.Warn("could not save the Source type the server reports", "entry_flavor", hint, "detected", flavor, "error", err)
		} else if changed {
			slog.Info("saved the Source type the server reports", "entry_flavor", hint, "detected", flavor)
		}
		w := registryFlavorWarning(hint, flavor, correctErr)
		if w != "" {
			slog.Warn(w, "entry_flavor", hint, "detected", flavor)
		}
		job.setFlavorWarning(w)
		once(flavor)
	}
}

// registryFlavorWarning is the text shown when the server contradicts the
// Source type saved with its entry and the saved type could not be changed
// (correctErr), "" when they agree or the change was saved.
// It never advises removing the server: the one that comes back is a new
// entry with a new index, and the old one's history is left behind.
//
// A blank saved type reads as MySQL everywhere (ServerEntry.SourceFlavor), so
// it is compared as MySQL: a failed save leaves that label just as wrong.
func registryFlavorWarning(hint, detected string, correctErr error) string {
	h, err := console.NormalizeFlavor(hint)
	if err != nil || h == detected || correctErr == nil {
		return ""
	}
	return fmt.Sprintf("This server is saved with Source type %s, but the server reports %s. DBTrail captures it as %s. The saved Source type could not be changed to %s: %v.",
		sourceTypeLabel(h), sourceTypeLabel(detected), sourceTypeLabel(detected), sourceTypeLabel(detected), correctErr)
}

// flavorCorrector is the correct func of registryFlavorHook for entry id: the
// registry's CorrectSourceFlavor, or nil when the supervisor has no registry.
func (m *monitorSupervisor) flavorCorrector(id string) func(string) (bool, error) {
	if m.registry == nil {
		return nil
	}
	return func(detected string) (bool, error) { return m.registry.CorrectSourceFlavor(id, detected) }
}

// sourceTypeLabel is the Source type option label the console form shows.
func sourceTypeLabel(flavor string) string {
	if flavor == console.FlavorMariaDB {
		return "MariaDB"
	}
	return "MySQL"
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
		if job.lock != nil {
			job.lock.Release()
		}
	}()
	// Cancel jobCtx on every terminal return (give-up, clean exit, cancellation).
	// Declared last so it runs first under LIFO — the ext source jobs stop before
	// the advisory lock is released above. A no-op when jobCtx is already
	// cancelled (Stop/Shutdown/daemon-cancel paths). Reconnects stay inside the
	// for-loop below, so this never fires mid-retry.
	defer job.cancel()

	// lost closes when the heartbeat finds the capture lock gone. A job
	// without a lock (unit tests drive run directly) never loses one: a nil
	// channel never fires.
	var lost <-chan struct{}
	if job.lock != nil {
		lost = job.lock.Lost()
	}
	var policy crashLoopPolicy
	for {
		job.set("pending", "")
		started := time.Now()
		// A run stops writing the moment the lock is lost (#2105): another
		// process may hold it by now and be capturing the same source.
		runCtx, cancelRun := context.WithCancelCause(ctx)
		go func() {
			select {
			case <-lost:
				cancelRun(streamrun.ErrStopWithoutFlush)
			case <-runCtx.Done():
			}
		}()
		err := runOnce(runCtx)
		cancelRun(nil)
		if ctx.Err() != nil {
			job.set("stopped", "")
			return
		}
		if chanClosed(lost) {
			m.lockLost(job, e)
			return
		}
		if err == nil {
			job.set("stopped", "")
			return
		}
		scrubbed := config.ScrubDSNError(err, e.SourceDSN, e.DSN)
		delay, looping, giveUp := policy.failed(started, time.Now())
		if giveUp {
			slog.Error("monitored stream crash-looped past the give-up threshold; not retrying",
				"server", e.Name, "entry", e.ID, "looping_for", looping.Round(time.Minute), "error", scrubbed)
			job.fail(fmt.Sprintf("%s (gave up after %s of crash-looping; fix the issue, then press Start to retry)",
				scrubbed, looping.Round(time.Minute)), monitorErrorCode(err), false)
			return
		}
		slog.Warn("monitored stream failed; retrying with backoff",
			"server", e.Name, "entry", e.ID, "delay", delay, "error", scrubbed)
		job.fail(scrubbed+" (retrying)", monitorErrorCode(err), true)
		select {
		case <-time.After(delay):
		case <-lost:
			m.lockLost(job, e)
			return
		case <-ctx.Done():
			job.set("stopped", "")
			return
		}
	}
}

// lockLost ends a job whose capture lock was lost and starts the entry again
// once this job is gone. The new start waits for the lock like any other: if
// another process took it, this one waits until that one stops; if nobody
// did, it takes it back at once. Ending the job, not only the stream, also
// stops the extension source jobs bound to it, so they never run beside the
// process that holds the lock now.
//
// The commonest cause of a lost lock is the index being unreachable for a
// while, and then the new start fails too (provisioning connects to it). So a
// failed start is retried with the stream's own backoff for as long as that
// failed job is still the entry's: an operator's Stop or Start ends the loop,
// and the entry is read again from the registry each time, so a restart
// never uses settings the operator has since changed or a server since
// removed or stopped.
//
// Called from run, which still holds its own wg count, so the Add below can
// never race Shutdown's Wait at zero.
func (m *monitorSupervisor) lockLost(job *monitorJob, e console.ServerEntry) {
	job.fail("this process lost its capture lock on the index, so it stopped writing; capture starts again once it holds the lock (retrying)", "", true)
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		<-job.done
		expect, delay := job, lockLostBackoffBase
		for m.baseCtx.Err() == nil {
			cur := e
			if m.registry != nil {
				got, ok := m.registry.Get(e.ID)
				if !ok || !got.MonitorDesired || got.SourceDSN == "" || got.DSN == "" {
					return
				}
				cur = got
			}
			var reserved *monitorJob
			err := m.start(m.baseCtx, cur, expect, &reserved)
			if err == nil || errors.Is(err, errNotCurrent) || reserved == nil {
				return
			}
			// The job this start reserved and failed; an operator's Start or
			// Stop after it makes the next start refuse.
			expect = reserved
			expect.setRetrying(err.Error() + " (retrying)")
			slog.Warn("could not start capture again after losing the capture lock; retrying",
				"server", e.Name, "entry", e.ID, "delay", delay, "error", err)
			select {
			case <-time.After(delay):
			case <-m.baseCtx.Done():
				return
			}
			delay = min(delay*2, monitorBackoffCap)
		}
	}()
}

// lockLostBackoffBase is the first wait between restarts after a lost lock;
// the stream's own base, a variable so a test can shorten it.
var lockLostBackoffBase = monitorBackoffBase

// chanClosed reports whether c is closed, without blocking. A nil channel is
// never closed.
func chanClosed(c <-chan struct{}) bool {
	select {
	case <-c:
		return true
	default:
		return false
	}
}

// monitorLockName is the capture lock of a registry entry. The name is the
// one every release since the lock existed has used: an older daemon running
// beside a newer one during an upgrade still sees the newer one's lock.
func monitorLockName(entryID string) string { return "bintrail_monitor_" + entryID }

// monitorPhaseLockWaiting is the phase of a pending job waiting for another
// process to release the entry's capture lock.
const monitorPhaseLockWaiting = "lock_waiting"

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
	// A job waiting for the capture lock streams nothing here: the process
	// that holds the lock does (#2105). Restarting the waiter would report a
	// reload of a stream that may still decode against the old snapshot.
	if ok && job.waitingLock.Load() {
		m.mu.Unlock()
		return false, nil
	}
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
	// defer closes done AFTER releasing the capture lock. Starting early would
	// find the lock still held by the dying stream, and the new job would wait
	// for it (#2105) showing "waiting for another DBTrail process", which this
	// process is not.
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
	// Clear with a short-lived connection of our own: the lock belongs to the
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
