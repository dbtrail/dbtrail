package consoleapp

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/metadata"
)

// schemaSnapshotSupervisor implements console.SchemaSnapshotController: it
// re-reads a monitored source's column layout into that source's index and then
// puts the running capture stream onto the result (#1296).
//
// The reload half is the point. A stream holds its metadata resolver in memory
// and swaps it only when it decodes a DDL event, so a snapshot written into the
// index underneath a running stream changes NOTHING — the stream keeps decoding
// against the layout it loaded at startup and keeps skipping the same table. A
// "refresh" button without the reload would look like a fix and be a no-op,
// which is a worse failure than the missing button this replaces.
type schemaSnapshotSupervisor struct {
	ctx context.Context // daemon lifecycle; cancels an in-flight job on shutdown

	// reload restarts the supervised stream for one entry so it loads the
	// snapshot just written, reporting whether it ACTUALLY restarted one. A
	// false with no error means this process supervises no stream for that
	// entry — which must never render as a restart: the entry may be captured
	// by another process, still decoding against the old snapshot. nil is
	// allowed (tests, and any wiring with no control plane).
	reload func(ctx context.Context, entryID string) (bool, error)

	// snapshotFn takes the snapshot itself — a seam, like monitorSupervisor's
	// streamFn: production connects to both DSNs and calls
	// metadata.TakeSnapshotExcludingInvalid, tests substitute a stub so the
	// reload contract can be exercised without a live MySQL.
	snapshotFn func(req console.SchemaSnapshotRequest) (metadata.SnapshotStats, error)

	// timeout bounds one job. Per-supervisor rather than a package var so a
	// test that shrinks it touches only its own instance: a global is read by
	// the job goroutine of every OTHER test in the package, and those outlive
	// the test that spawned them, so writing one is a data race that -race
	// catches and a plain `go test` does not.
	timeout time.Duration

	mu   sync.Mutex
	jobs map[string]*console.SchemaSnapshotStatus
	// gens counts triggered runs PER SERVER. A run whose generation is no
	// longer its server's current one has been superseded (its predecessor
	// timed out and the operator retried) and must neither publish its outcome
	// nor restart a stream. Per-server, not global: one counter would let a
	// snapshot on server B silently abandon an in-flight one on server A.
	gens map[string]uint64
}

// reloadStreamSchema binds the snapshot supervisor's reload hook to the monitor
// supervisor. The lookup happens at reload time, not at wiring time: the entry
// may have been edited (or deleted) between the daemon starting and the button
// being pressed, and Start needs the CURRENT entry.
func reloadStreamSchema(sup *monitorSupervisor, reg *console.Registry) func(context.Context, string) (bool, error) {
	return func(ctx context.Context, entryID string) (bool, error) {
		e, ok := reg.Get(entryID)
		if !ok {
			return false, fmt.Errorf("server %q is no longer in the registry", entryID)
		}
		return sup.ReloadSchema(ctx, e)
	}
}

// defaultSchemaSnapshotTimeout is what a supervisor gets unless a caller
// narrows it.
//
// A bound exists because the snapshot itself cannot be cancelled: metadata's
// snapshot taker holds no context, and config.Connect's timeout covers only the
// TCP handshake — a source whose information_schema read blocks behind a
// metadata lock hangs the job forever. Without a deadline the job stays
// "running" for the life of the process, every later Trigger answers 409, and
// the only recovery is a daemon restart: an endpoint permanently unable to do
// the thing it exists for.
const defaultSchemaSnapshotTimeout = 10 * time.Minute

func newSchemaSnapshotSupervisor(ctx context.Context, reload func(context.Context, string) (bool, error)) *schemaSnapshotSupervisor {
	return &schemaSnapshotSupervisor{
		ctx:        ctx,
		reload:     reload,
		snapshotFn: takeSchemaSnapshot,
		timeout:    defaultSchemaSnapshotTimeout,
		jobs:       make(map[string]*console.SchemaSnapshotStatus),
		gens:       make(map[string]uint64),
	}
}

// takeSchemaSnapshot is the production snapshot step: connect to the source and
// the entry's index, then re-read the column layout.
//
// ExcludingInvalid matches the stream's own DDL hook: one PK-less table must not
// reject the whole snapshot and leave capture running on the stale one. The
// excluded names come back in the stats so the caller can report them — a
// "succeeded" that silently omits the tables that will KEEP being skipped is
// the same half-truth this issue is about.
func takeSchemaSnapshot(req console.SchemaSnapshotRequest) (metadata.SnapshotStats, error) {
	sourceDB, err := connectSourceAsked(req.SourceDSN, req.SourceSSL)
	if err != nil {
		return metadata.SnapshotStats{}, err
	}
	defer sourceDB.Close()
	indexDB, err := config.Connect(req.IndexDSN)
	if err != nil {
		return metadata.SnapshotStats{}, err
	}
	defer indexDB.Close()
	return metadata.TakeSnapshotExcludingInvalid(sourceDB, indexDB, req.Schemas)
}

// Trigger starts a snapshot in the background; returns
// console.ErrSchemaSnapshotRunning when one is already in flight for this
// server. One at a time per server: two concurrent runs would race to restart
// the same stream.
func (s *schemaSnapshotSupervisor) Trigger(req console.SchemaSnapshotRequest) error {
	s.mu.Lock()
	if st, ok := s.jobs[req.ServerID]; ok && st.State == "running" {
		s.mu.Unlock()
		return console.ErrSchemaSnapshotRunning
	}
	s.jobs[req.ServerID] = &console.SchemaSnapshotStatus{State: "running", Since: nowStamp()}
	s.gens[req.ServerID]++
	gen := s.gens[req.ServerID]
	s.mu.Unlock()

	slog.Info("schema snapshot: refreshing from the source", "server", req.ServerName, "id", req.ServerID)
	go s.run(req, gen)
	return nil
}

// Status returns a copy of the latest known job state (idle if none ran here).
func (s *schemaSnapshotSupervisor) Status(serverID string) console.SchemaSnapshotStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	if st, ok := s.jobs[serverID]; ok {
		return *st
	}
	return console.SchemaSnapshotStatus{State: "idle"}
}

// run executes one job under s.timeout. On timeout the job is
// reported failed so the endpoint becomes usable again; the abandoned goroutine
// cannot corrupt anything after that, because publish drops a result from a
// superseded generation and execute declines to restart a stream it no longer
// owns.
func (s *schemaSnapshotSupervisor) run(req console.SchemaSnapshotRequest, gen uint64) {
	// This goroutine's own frames are the select below and publish, neither of
	// which can reach the stream restart, so its report carries no capture
	// caveat. It CAN fire while the inner goroutine is inside the restart, but
	// that goroutine reports its own panic, so the caveat still travels with
	// the panic that earned it.
	defer recoverSnapshotJob(req, s.runPanicReporter(req, gen))

	type outcome struct {
		st  console.SchemaSnapshotStatus
		err error
	}
	done := make(chan outcome, 1)
	go func() {
		// recover() is per goroutine: the guard run defers above does NOT
		// cover this one, and believing it did is the mistake #1472's first
		// attempt made.
		//
		// prog is written by execute and read here. Same goroutine, so it needs
		// no synchronisation, and it is what lets the report name the half that
		// died instead of hedging over both.
		var prog executeProgress
		defer recoverSnapshotJob(req, func(err error) {
			st, err := prog.panicOutcome(err)
			// The panic is routed into the SAME channel a normal outcome uses,
			// so run publishes it at once. Logging alone would leave run
			// waiting out s.timeout and then reporting that the source did not
			// answer: this daemon's own internal error, blamed on a metadata
			// lock the source is not holding.
			//
			// Non-blocking on purpose. The send cannot fill the buffer today
			// (the ordinary send below is reached only if execute returned,
			// which it did not), but that is a property of a statement further
			// down that a later edit could move, and blocking here would leak
			// this goroutine AND lose the report.
			select {
			case done <- outcome{st, err}:
			default:
			}
			// If run already gave up, nobody will read that value and its own
			// timeout or shutdown text stands, which blames the source for what
			// this daemon did. amendTerminalReport replaces it. Not closed: run
			// leaving the select between the check inside amendTerminalReport
			// and its own publish still lands the misleading text. That needs
			// the panic to coincide with the deadline, unlike the reachable
			// case this covers, where the slot went terminal minutes earlier.
			s.amendTerminalReport(req, gen, st, err)
		})
		st, err := s.execute(req, gen, &prog)
		done <- outcome{st, err}
	}()
	select {
	case o := <-done:
		s.publish(req, gen, o.st, o.err)
	case <-time.After(s.timeout):
		slog.Warn("schema snapshot timed out; the attempt may still be finishing in the background",
			"server", req.ServerName, "id", req.ServerID, "timeout", s.timeout)
		s.publish(req, gen, console.SchemaSnapshotStatus{},
			fmt.Errorf("the source did not answer within %s; it may be holding a metadata lock. The attempt may still finish in the background; capture was not restarted", s.timeout))
	case <-s.ctx.Done():
		s.publish(req, gen, console.SchemaSnapshotStatus{}, errors.New("DBTrail is shutting down; capture was not restarted"))
	}
}

// publish records a job's outcome, unless a newer run for this server has
// already superseded it — a late finisher must not overwrite the newer job's
// state (or resurrect a "succeeded" over it).
func (s *schemaSnapshotSupervisor) publish(req console.SchemaSnapshotRequest, gen uint64, st console.SchemaSnapshotStatus, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.gens[req.ServerID] != gen {
		return
	}
	prev := s.jobs[req.ServerID]
	st.Since = ""
	if prev != nil {
		st.Since = prev.Since
	}
	st.FinishedAt = nowStamp()
	if err != nil {
		// Scrub both DSNs: this string is served over HTTP, and a driver error
		// commonly embeds the whole connection string, password included.
		st.State = "failed"
		st.LastError = config.ScrubDSNError(err, req.SourceDSN, req.IndexDSN)
		slog.Warn("schema snapshot failed", "server", req.ServerName, "id", req.ServerID, "error", st.LastError)
	}
	s.jobs[req.ServerID] = &st
}

// execute does the work: snapshot, then reload the stream. A reload failure is
// NOT a job failure — the snapshot is durable and correct, capture is simply
// still running on the old one — so it is reported in its own field with the
// state left "succeeded". Folding it into LastError would hide that the
// snapshot itself worked and invite the operator to run it again.
// prog records how far this got, for the panic guard in the caller's
// goroutine. It may be nil for callers with no guard (tests).
func (s *schemaSnapshotSupervisor) execute(req console.SchemaSnapshotRequest, gen uint64, prog *executeProgress) (console.SchemaSnapshotStatus, error) {
	st := console.SchemaSnapshotStatus{State: "succeeded"}

	stats, err := s.snapshotFn(req)
	if err != nil {
		return st, err
	}
	st.SnapshotID, st.Tables, st.ExcludedTables = stats.SnapshotID, stats.TableCount, stats.ExcludedTables
	slog.Info("schema snapshot taken", "server", req.ServerName, "snapshot_id", stats.SnapshotID,
		"tables", stats.TableCount, "excluded_tables", strings.Join(stats.ExcludedTables, ", "))
	// Recorded only now: the snapshot is durable at this point, so a panic
	// after this can say so instead of reporting the run as a total loss.
	if prog != nil {
		prog.taken = &st
	}

	if s.reload == nil {
		st.ReloadError = notSupervisedNote
		return st, nil
	}
	if s.superseded(req.ServerID, gen) {
		// This run timed out and the operator retried: a newer run owns the
		// stream now. Restarting it here would fight that one.
		st.ReloadError = "this attempt was superseded by a newer one; capture was not restarted by it"
		return st, nil
	}
	// Set immediately before the call, never cleared: from here on a panic may
	// have left this server's capture cancelled, and that has to be reported
	// whether it was raised inside the restart or after it returned.
	if prog != nil {
		prog.reloading = true
	}
	reloaded, err := s.reload(s.ctx, req.ServerID)
	if err != nil {
		st.ReloadError = config.ScrubDSNError(err, req.SourceDSN, req.IndexDSN)
		slog.Warn("schema snapshot: the capture stream was not restarted onto the new snapshot",
			"server", req.ServerName, "id", req.ServerID, "error", st.ReloadError)
		return st, nil
	}
	if !reloaded {
		// No stream here to restart. Never report this as a reload: whoever
		// captures this source is still on the previous snapshot.
		st.ReloadError = notSupervisedNote
		return st, nil
	}
	st.StreamReloaded = true
	return st, nil
}

// notSupervisedNote is what an operator is told when no stream was restarted
// here. It states the consequence (capture is still on the old snapshot)
// because that is the difference between a fix and a durable no-op.
const notSupervisedNote = "this process does not supervise capture for this server, so nothing was restarted; " +
	"restart whatever captures it to pick the new snapshot up"

// superseded reports whether a newer run for the same server has started.
func (s *schemaSnapshotSupervisor) superseded(serverID string, gen uint64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.gens[serverID] != gen
}

// executeProgress records how far execute got before a panic, so the guard in
// its goroutine can report WHICH half died.
//
// Both directions are wrong for an operator, not just one. Never warning about
// capture would hide a stopped stream, which is silent data loss. But warning
// on every panic, including the ones raised while reading the source's columns
// with the restart not yet reached, is a false alarm on the one surface an
// operator has to be able to trust in an incident, and a warning that cries
// wolf is worse than none.
type executeProgress struct {
	// taken points at the snapshot half's result once that half is durable.
	// nil means the snapshot did not finish, so nothing about it can be
	// claimed.
	taken *console.SchemaSnapshotStatus
	// reloading records that the stream restart had begun.
	reloading bool
}

// panicOutcome renders one recovered panic into the status and error the run
// should report, in the operator's terms rather than the stack's.
func (p executeProgress) panicOutcome(err error) (console.SchemaSnapshotStatus, error) {
	if !p.reloading {
		// The restart is not reached until after the snapshot returns, so this
		// is a positive statement, not an absence of evidence.
		return console.SchemaSnapshotStatus{}, fmt.Errorf("%w. Capture for this server was not touched", err)
	}
	// Reported as failed even though the snapshot is durable: an internal error
	// is not a success. But the counts are carried, and the text says the
	// snapshot is recorded, so nobody reads this as "the refresh did nothing"
	// and goes hunting for a snapshot that is already there.
	st := console.SchemaSnapshotStatus{}
	if p.taken != nil {
		st.SnapshotID, st.Tables, st.ExcludedTables = p.taken.SnapshotID, p.taken.Tables, p.taken.ExcludedTables
	}
	return st, fmt.Errorf("%w. The schema snapshot itself was taken and recorded. This happened while "+
		"restarting capture for this server, which may have stopped it: open Manage servers and press "+
		"Start if it is not running", err)
}

// recoverSnapshotJob is the panic guard BOTH schema-snapshot goroutines defer
// as their first statement.
//
// Under `bintrail-console watch` this process is also the capture plane, so an
// unrecovered panic in either goroutine ends replication capture for every
// monitored source. A schema snapshot that did not refresh is a degradation, a
// daemon that stopped capturing is an outage, and the first must never cause
// the second. Mirrors recoverBaselineJob and verifySupervisor's guard, the same
// hazard on the neighbouring supervisors.
//
// It is a free function, not a method, because it holds no supervisor state:
// everything it can do about the panic is whatever the caller's report does.
//
// Swallowing the panic quietly would trade a loud outage for a silent
// degradation, which is worse. So two things happen, and BOTH are load-bearing:
//
//   - The panic is logged at error level WITH the stack, which is the only
//     place the panic site is recorded now that the process no longer dies
//     printing it.
//   - It is handed to report, which is that goroutine's OWN failure route, so
//     the run reaches "failed" the way any other failure does. Trigger refuses
//     a new snapshot while this server's slot reads "running", so a guard that
//     logged and left it there would refuse that server's schema refresh until
//     the daemon restarted, and the operator's only clue would be a 409 on a
//     button. That cure would be worse than the disease.
//
// The log line claims only that the daemon survived. Whether CAPTURE survived
// depends on where the panic was raised, which the guard cannot see; the
// reported error covers that at the one call site that can reach the restart.
//
// SCOPE, because a recovered panic reads too easily as "and nothing else
// happened": this contains the CRASH, not every side effect the panicking
// frames already had. The one that matters is the stream restart.
// monitorSupervisor.ReloadSchema takes the entry out of its job map, cancels
// the stream, and puts the entry back only on the paths it returns from, so a
// panic between those two points leaves that server's capture cancelled and its
// entry unpublished, which also drops it from the rotation provider's active
// list. Nothing here can undo either, which is why the reported error names the
// operator's move instead of implying the job was the only casualty. Closing
// that window belongs in that supervisor.
func recoverSnapshotJob(req console.SchemaSnapshotRequest, report func(error)) {
	r := recover()
	if r == nil {
		return
	}
	// debug.Stack() here still walks the panicking frames: the deferred
	// function runs on top of them, so the stack names where the panic came
	// from and not just this guard.
	slog.Error("schema snapshot: the job hit an internal error and stopped. The daemon is still running. "+
		"Please report this with the stack recorded here.",
		"server", req.ServerName, "id", req.ServerID, "panic", r, "stack", string(debug.Stack()))
	report(fmt.Errorf("internal error: %v", r))
}

// runPanicReporter is the report half of run's guard, named so that a test can
// drive the SAME closure production does. Inlining it at the defer left the two
// interchangeable: substituting publish for failIfRunning there passed the whole
// suite, because every test that reached failIfRunning built its own copy of
// this closure instead of calling run's.
func (s *schemaSnapshotSupervisor) runPanicReporter(req console.SchemaSnapshotRequest, gen uint64) func(error) {
	return func(err error) { s.failIfRunning(req, gen, err) }
}

// failIfRunning is how the run goroutine's guard reports a panic: it moves this
// run's status slot to "failed", which is what frees Trigger to accept a new
// snapshot for this server.
//
// The generation check is load-bearing and drops a superseded run, exactly as
// publish does: a newer Trigger has already reset the slot to "running", and a
// stale guard must not fail the run that owns it now.
//
// The still-running check is defensive rather than live. It exists so a panic
// raised in a run's TAIL cannot restate an outcome that already published, and
// the sibling guard in baseline_job_guard.go needs exactly that, because those
// jobs write their terminal state and THEN log from the same locked region. In
// THIS file publish logs first and writes the map last, and its log sits inside
// `if err != nil`, so no reachable panic site leaves this slot terminal with a
// matching generation. Keep it anyway: it is one comparison, and it is what
// makes adding a step after publish safe. Do not read it as covering a hazard
// that exists here today. Wedge-safety never needed it: a terminal state
// already frees Trigger.
//
// The panic value is scrubbed like any other reported error. A driver panic can
// carry the whole connection string, and this string is served over HTTP.
func (s *schemaSnapshotSupervisor) failIfRunning(req console.SchemaSnapshotRequest, gen uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.gens[req.ServerID] != gen {
		return
	}
	st := s.jobs[req.ServerID]
	if st == nil || st.State != "running" {
		return
	}
	st.State = "failed"
	st.LastError = config.ScrubDSNError(err, req.SourceDSN, req.IndexDSN)
	st.FinishedAt = nowStamp()
}

// amendTerminalReport replaces an already-published outcome with the panic that
// really ended this run.
//
// It exists for one reachable shape: the snapshot half outlives s.timeout, run
// publishes "the source did not answer within 10m; it may be holding a metadata
// lock", and only afterwards does the work panic. Without this the operator is
// told the source is stuck when the daemon is the one that broke, and the
// capture consequence, the one thing this guard exists to surface, is dropped
// with the unread channel value.
//
// Overwriting a terminal state is safe HERE and nowhere else, which is why this
// is not failIfRunning's job: it runs only from the execute goroutine's guard,
// which fires only when execute did NOT return, so no run of this generation
// can have published a success for it to destroy. The generation check still
// applies, because a newer Trigger owns the slot after a retry.
func (s *schemaSnapshotSupervisor) amendTerminalReport(req console.SchemaSnapshotRequest, gen uint64, st console.SchemaSnapshotStatus, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.gens[req.ServerID] != gen {
		return
	}
	prev := s.jobs[req.ServerID]
	if prev == nil || prev.State == "running" {
		// run has not published yet, so the channel send above is the report
		// and this must not race it.
		return
	}
	st.State = "failed"
	st.LastError = config.ScrubDSNError(err, req.SourceDSN, req.IndexDSN)
	st.Since, st.FinishedAt = prev.Since, nowStamp()
	s.jobs[req.ServerID] = &st
}
