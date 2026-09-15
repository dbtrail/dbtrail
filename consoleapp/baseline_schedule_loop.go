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

	"github.com/dbtrail/dbtrail/internal/console"
)

// Per-server backup schedule (#1442). The loop ticks once a minute, reads the
// registry every tick (a schedule saved from the page applies without a
// restart, the way a rotation override does), and starts a job when a
// server's schedule crosses a slot boundary. It is EDGE-triggered on the slot
// grid:
//
//   - a slot that passed while the daemon was down never fires: cron
//     semantics, and the only ones that keep a restart from turning into a
//     surprise full dump of production at 09:00 on a Monday;
//   - a schedule is observed from the moment it is SAVED (the API tells the
//     loop) or from boot (the loop seeds every schedule it finds), keyed by
//     the schedule's identity, so an add and an edit are both silent for
//     the slot already in progress and fire at the next one. That next one
//     is exactly the next_run the page showed when the operator saved, even
//     when it falls inside the minute before the first tick.
//
// Isolation matches the refresh and rotation loops: one loop goroutine with
// a recover around each tick and around each server's fire, one watcher
// goroutine per started job (it follows the job to its end and, for a failed
// update, takes the full backup that stands in for it, minutes after the
// slot), and none of it touches the stream. A schedule that stopped is a
// degradation; a daemon that stopped capturing is an outage.

// backupScheduleTick is how often the loop looks at the clock. A slot fires
// within this much of its instant.
const backupScheduleTick = time.Minute

// backupScheduler is the loop's state and the console's view of it.
type backupScheduler struct {
	sup *baselineSupervisor
	reg *console.Registry
	// fullBackups: the daemon's baseline-creation opt-in. Without it a slot
	// whose chosen producer is a full backup is skipped and recorded as
	// such; an update from the recorded changes does not need it.
	fullBackups bool
	// carryDefault is the daemon's --baseline-carry-forward-unchanged, the
	// fallback when the console has not saved an override; a scheduled
	// rebuild honours the same setting the refresh loop and a restore do.
	carryDefault bool

	mu sync.Mutex
	// seen is the latest slot observed per server, together with the
	// identity of the schedule it was observed under. A different identity
	// is a first observation: that is what makes an edit silent.
	seen map[string]seenSlot
	// started is the last job this schedule started per server: which
	// supervisor slot to look at, the exact stamp the supervisor gave it (so
	// a later manual job in the same slot is not mistaken for ours unless
	// it started in the same second, the stamp's resolution), and,
	// once observed, its terminal status, kept so a later manual job taking
	// the slot does not erase the schedule's last outcome from the page.
	started map[string]scheduledStart
	// skipped is the last slot this schedule could not start per server.
	// The history has the durable copy; this one is what the page gets when
	// the history is unavailable.
	skipped map[string]scheduledSkip
	// fallback is the last slot per server where the update from the
	// recorded changes failed and a full backup was STARTED instead, for
	// the page. Written only once the full backup's trigger returned nil:
	// a collision there records a skip, and the page must not say a full
	// backup was taken next to a line saying nothing ran.
	fallback map[string]scheduledFallback
	// warned holds the servers whose unreadable schedule was already reported,
	// so the log says it once rather than every minute.
	warned map[string]bool
	// watchers counts the watchScheduled goroutines alive. Shutdown does not
	// wait on them (they leave within a poll of the cancel); tests do, so an
	// assertion about what the watcher did NOT start is made after it is
	// gone rather than racing its last poll.
	watchers sync.WaitGroup
}

type seenSlot struct {
	identity string
	slot     time.Time
}

type scheduledStart struct {
	method string
	// at is the loop's own stamp (RFC3339 UTC), reported as LastStartedAt.
	at string
	// since is the supervisor's Since for the job, read back right after
	// the trigger. Attribution compares on it exactly.
	since string
	// why is the reason a full backup was chosen (#1604); "" for an update.
	why string
	// last is the job's status once it was observed in a terminal state,
	// nil until then.
	last *console.BaselineStatus
	// fallback: this is the full backup that stood in for a failed update.
	// Its success says nothing about the update path, so it does not end
	// the fallback alarm; any other scheduled job that succeeds does.
	fallback bool
}

type scheduledSkip struct {
	at     string
	reason string
}

type scheduledFallback struct {
	at     string
	reason string
}

func newBackupScheduler(sup *baselineSupervisor, reg *console.Registry, fullBackups, carryDefault bool) *backupScheduler {
	return &backupScheduler{
		sup: sup, reg: reg, fullBackups: fullBackups, carryDefault: carryDefault,
		seen:     make(map[string]seenSlot),
		started:  make(map[string]scheduledStart),
		skipped:  make(map[string]scheduledSkip),
		fallback: make(map[string]scheduledFallback),
		warned:   make(map[string]bool),
	}
}

// newBackupScheduleReporter is the watch daemon's wiring in one place: nil
// supervisor (no baseline feature on this daemon) means no loop, and the
// interface it returns is then a true nil rather than a typed nil, which the
// console would otherwise take for a running loop and dereference. The
// second value is the same object for startBackupScheduleLoop.
func newBackupScheduleReporter(sup *baselineSupervisor, reg *console.Registry, fullBackups, carryDefault bool) (console.BackupScheduleReporter, *backupScheduler) {
	if sup == nil {
		return nil, nil
	}
	s := newBackupScheduler(sup, reg, fullBackups, carryDefault)
	return s, s
}

// FullBackups implements console.BackupScheduleReporter: the opt-in, and the
// supervisor's standing refusal (a lock-mode misconfiguration) when there
// is one.
func (b *backupScheduler) FullBackups() (bool, error) {
	return b.fullBackups, b.sup.configErr
}

// Observe implements console.BackupScheduleReporter: the API calls it when a
// schedule is saved, so the slot in progress at that instant is the one the
// loop treats as already seen. Without it the first TICK was the first
// observation, and a boundary between the save and that tick (up to a
// minute) was silently dropped while the page had just promised it as the
// next run. Cheap and lock-only; an unparseable schedule is ignored here and
// reported by the tick.
func (b *backupScheduler) Observe(serverID string, sched console.BackupSchedule, at time.Time) {
	p, err := sched.Parse()
	if err != nil {
		return
	}
	b.mu.Lock()
	b.seen[serverID] = seenSlot{identity: sched.Identity(), slot: p.SlotAtOrBefore(at.UTC())}
	b.mu.Unlock()
}

// observeAll seeds the observation of every schedule in the registry at one
// instant: boot. A boundary in the first minute of uptime then fires (the
// daemon was up for it), and one before boot does not.
func (b *backupScheduler) observeAll(at time.Time) {
	for _, e := range b.reg.List() {
		if e.BackupSchedule != nil {
			b.Observe(e.ID, *e.BackupSchedule, at)
			if p, err := e.BackupSchedule.Parse(); err == nil {
				warnBackupScheduleRate(e, p)
			}
		}
	}
}

// warnBackupScheduleRate is the schedule's version of the refresh loop's
// disk warning: every backup it publishes is a full-table snapshot, and
// PruneLocal only removes what it confirmed durable in S3, so on a server
// without an S3 destination nothing ever removes them. Logged at boot and
// at save, with the 30-day count, so the operator reads the rate before
// the disk does.
func warnBackupScheduleRate(e console.ServerEntry, p console.ParsedBackupSchedule) {
	slog.Warn("backup schedule: every run publishes a full-table snapshot",
		"server", e.Name, "every", p.Every, "backups_per_30d", snapshotsPer30Days(p.Every),
		"local_only", e.BaselineS3 == "", "dir", e.BaselineDir)
}

// ScheduleState implements console.BackupScheduleReporter.
func (b *backupScheduler) ScheduleState(serverID string) console.BackupScheduleState {
	b.mu.Lock()
	st, started := b.started[serverID]
	sk, skipped := b.skipped[serverID]
	fb, fell := b.fallback[serverID]
	b.mu.Unlock()
	var out console.BackupScheduleState
	if skipped {
		out.LastSkippedAt, out.LastSkipReason = sk.at, sk.reason
	}
	if fell {
		out.LastFallbackAt, out.LastFallbackReason = fb.at, fb.reason
	}
	if !started {
		return out
	}
	out.LastStartedAt, out.LastMethod, out.LastWhy = st.at, st.method, st.why
	var cur console.BaselineStatus
	if st.method == console.BackupMethodRefresh {
		cur = b.sup.RefreshStatus(serverID)
	} else {
		cur = b.sup.Status(serverID)
	}
	switch {
	case cur.Since == st.since:
		// The slot is shared with manual jobs of the same kind. Only the job
		// whose Since is exactly the one read back at trigger time is ours.
		out.Last = &cur
		out.Running = cur.State == "running"
		if !out.Running {
			// Keep the outcome: a later manual job overwrites the slot, and
			// a job that panicked has no history record, so this copy is
			// the page's only evidence until the schedule's next run.
			b.mu.Lock()
			if cur2, ok := b.started[serverID]; ok && cur2.since == st.since {
				cur2.last = &cur
				b.started[serverID] = cur2
				if !st.fallback && cur.State == "succeeded" {
					// The fallback line is an alarm about the update path.
					// A later scheduled job that went through, an update
					// or a full backup the rule picked (the server now
					// goes to S3, say), is the evidence the schedule is
					// producing backups again, so the alarm ends here
					// rather than at the next restart; only the fallback's
					// OWN full backup proves nothing. Inside the same
					// re-check as the copy above: a slot that fired,
					// failed and fell back between the status read and
					// this lock must not have its fresh alarm deleted by
					// a stale reader.
					delete(b.fallback, serverID)
					out.LastFallbackAt, out.LastFallbackReason = "", ""
				}
			}
			b.mu.Unlock()
		}
	case st.last != nil:
		out.Last = st.last
	}
	return out
}

// startBackupScheduleLoop launches the loop. sched nil = no baseline features
// on this daemon = no loop; the console then refuses the schedule endpoints.
func startBackupScheduleLoop(ctx context.Context, sched *backupScheduler) {
	if sched == nil {
		return
	}
	sched.observeAll(time.Now().UTC())
	slog.Info("backup schedule loop enabled", "tick", backupScheduleTick, "full_backups", sched.fullBackups)
	go func() {
		t := time.NewTicker(backupScheduleTick)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-t.C:
				sched.tick(ctx, now.UTC())
			}
		}
	}()
}

// tick is one look at the clock: for every scheduled server, record the
// current slot and fire when it moved.
func (b *backupScheduler) tick(ctx context.Context, now time.Time) {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("backup schedule tick panicked; schedules continue next tick", "panic", r, "stack", string(debug.Stack()))
		}
	}()
	if ctx.Err() != nil {
		return
	}
	entries := b.reg.List()
	live := make(map[string]bool, len(entries))
	for _, e := range entries {
		if e.BackupSchedule == nil {
			continue
		}
		live[e.ID] = true
		p, err := e.BackupSchedule.Parse()
		if err != nil {
			// The API never saves one of these, so this is a hand-edited
			// file, or one written by a newer bintrail with a wider grammar.
			// The listing reports it as not runnable; the log says it once
			// per server, at a level the default configuration shows.
			b.mu.Lock()
			first := !b.warned[e.ID]
			b.warned[e.ID] = true
			b.mu.Unlock()
			if first {
				slog.Warn("backup schedule: this server's schedule cannot be read and will not run until it is fixed",
					"server", e.Name, "error", err)
			}
			continue
		}
		b.mu.Lock()
		delete(b.warned, e.ID)
		b.mu.Unlock()
		if !b.crossed(e.ID, e.BackupSchedule.Identity(), p.SlotAtOrBefore(now)) {
			continue
		}
		b.fireGuarded(e, p, now)
	}
	// Forget servers whose schedule is gone: a schedule removed and later
	// re-added starts silent again rather than firing on a stale slot, and
	// its last outcome is not reported under a schedule that no longer
	// exists.
	b.mu.Lock()
	for id := range b.seen {
		if !live[id] {
			delete(b.seen, id)
		}
	}
	for id := range b.warned {
		if !live[id] {
			delete(b.warned, id)
		}
	}
	for id := range b.started {
		if !live[id] {
			delete(b.started, id)
		}
	}
	for id := range b.skipped {
		if !live[id] {
			delete(b.skipped, id)
		}
	}
	for id := range b.fallback {
		if !live[id] {
			delete(b.fallback, id)
		}
	}
	b.mu.Unlock()
}

// crossed records slot for the server under the schedule's identity and
// reports whether it moved past the previously observed one. The first
// observation of an identity records and reports false, so an add and an
// edit are both silent (the API's Observe normally gets there first).
func (b *backupScheduler) crossed(serverID, identity string, slot time.Time) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	prev, ok := b.seen[serverID]
	b.seen[serverID] = seenSlot{identity: identity, slot: slot}
	return ok && prev.identity == identity && slot.After(prev.slot)
}

// fireGuarded is fire with its own recover: a panic while firing one
// server's slot must not cost the other servers this tick, and the slot,
// already recorded as crossed, would otherwise vanish with no page
// evidence. It is noted as a skip naming the internal error, in memory
// and the log only: the history write is inside the region this recover
// guards, and re-entering it from here could panic a second time, which
// this recover could not catch (the same rule as recoverBaselineJob).
func (b *backupScheduler) fireGuarded(e console.ServerEntry, p console.ParsedBackupSchedule, now time.Time) {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("backup schedule: firing a slot panicked", "server", e.Name, "panic", r, "stack", string(debug.Stack()))
			b.noteSkip(e, now, fmt.Sprintf("internal error: %v", r))
		}
	}()
	b.fire(e, p, now)
}

// fire starts the scheduled job for e, or records why it could not. HOW is
// decided here per slot (console.ChooseBackupMethod): update the newest
// backup from the recorded changes when that is the right producer, a full
// backup from the source otherwise. Every started job is watched to its
// end (watchScheduled) so its outcome is in the loop's own view even when
// nobody loads the page; an update that fails falls back to a full backup
// at the same slot when the daemon may take one (fallBack).
func (b *backupScheduler) fire(e console.ServerEntry, p console.ParsedBackupSchedule, now time.Time) {
	gates := b.gates()
	if err := console.CheckBackupSchedule(e, *e.BackupSchedule, gates); err != nil {
		b.skip(e, now, console.RefusalReason(err))
		return
	}
	method, why, err := console.ChooseBackupMethod(b.sup.ctx, e, gates)
	if err != nil {
		b.skip(e, now, err.Error())
		return
	}
	// A full backup chosen because the previous one could not be READ is not
	// the plan the operator set, and it is the expensive producer. Carried into
	// the run record so the history says why, and logged because the page's own
	// reason is recomputed live: by the time anyone looks, a transient bucket
	// error is gone and the page would show the cheap producer as if that is
	// what ran.
	degraded := ""
	if method == console.BackupMethodFull && strings.HasPrefix(why, console.BackupWhyUnreadablePrefix) {
		degraded = why
		slog.Warn("backup schedule: taking a full backup because the previous one could not be read",
			"server", e.Name, "id", e.ID, "reason", why)
	}
	stamp := now.Format(time.RFC3339)
	if method == console.BackupMethodRefresh {
		switch err := b.startRebuild(e, p, stamp); {
		case err == nil:
			b.watch(e, stamp, method)
		case errors.Is(err, console.ErrBaselineRunning):
			b.skip(e, now, "another backup job was running for this server at the scheduled time")
		default:
			// TriggerRefresh has no other error today; defensive, so a
			// future one is a recorded skip rather than a silent miss.
			b.skip(e, now, err.Error())
		}
		return
	}
	if b.startFull(e, stamp, now, degraded, why) {
		b.watch(e, stamp, method)
	}
}

// watch starts watchScheduled for the job at stamp, counted in watchers.
func (b *backupScheduler) watch(e console.ServerEntry, stamp, method string) {
	b.watchers.Add(1)
	go func() {
		defer b.watchers.Done()
		b.watchScheduled(e, stamp, method)
	}()
}

// gates is what the checker needs to know about this daemon.
func (b *backupScheduler) gates() console.BackupScheduleGates {
	enabled, refusal := b.FullBackups()
	g := console.BackupScheduleGates{LoopRunning: true, FullBackups: enabled}
	if refusal != nil {
		g.FullBackupsErr = refusal.Error()
	}
	return g
}

// startRebuild triggers the fold and records the job as the schedule's.
func (b *backupScheduler) startRebuild(e console.ServerEntry, p console.ParsedBackupSchedule, stamp string) error {
	req := refreshRequest{
		ServerID: e.ID, ServerName: e.Name, IndexDSN: e.DSN, BaselineDir: e.BaselineDir,
		// The destination this server's backups already go to. Set ONLY here:
		// it is what lets the fold reach S3 (#1539), and the daemon-wide
		// interval loop must keep leaving it empty. See refreshRequest.
		BaselineS3:            e.BaselineS3,
		CarryForwardUnchanged: effectiveCarryForward(b.reg, b.carryDefault),
		Trigger:               console.BaselineRunTriggerScheduled,
	}
	// The interval is what the overrun warning measures against and names;
	// for a scheduled rebuild that is the schedule's own `every`, which is
	// where "raise the interval" is acted on.
	if err := b.sup.TriggerRefresh(req, p.Every); err != nil {
		return err
	}
	b.record(e, console.BackupMethodRefresh, stamp, b.sup.RefreshStatus(e.ID).Since, false, "")
	return nil
}

// startFull triggers a full backup and records the job as the schedule's,
// or records the skip; reports whether the job started. because, when set,
// is the failed update this full backup stands in for, so a collision skip
// carries both facts. why is the reason a full backup was chosen at all
// (#1604), carried on the request so the run record keeps it.
func (b *backupScheduler) startFull(e console.ServerEntry, stamp string, now time.Time, because, why string) bool {
	req := console.BaselineRequestFor(e)
	req.Trigger = console.BaselineRunTriggerScheduled
	req.Why = why
	prefix, when := "", "at the scheduled time"
	if because != "" {
		prefix, when = because+"; ", "when the full backup was tried"
	}
	switch err := b.sup.Trigger(req); {
	case err == nil:
		b.record(e, console.BackupMethodFull, stamp, b.sup.Status(e.ID).Since, because != "", why)
		return true
	case errors.Is(err, console.ErrBaselineRunning):
		// The collision the issue names: a manual backup, restore or export
		// (or the previous scheduled run) holds the server. Skip, do not
		// queue: a queued dump would fire at an unscheduled moment.
		b.skip(e, now, prefix+"another backup job was running for this server "+when)
	default:
		b.skip(e, now, prefix+err.Error())
	}
	return false
}

// record notes the job the schedule just started. The supervisor's own
// stamp is read back right after the trigger: it is the key ScheduleState
// attributes the slot by, and the trigger returned, so the slot is ours
// until the job finishes and something else claims it.
func (b *backupScheduler) record(e console.ServerEntry, method, stamp, since string, fallback bool, why string) {
	b.mu.Lock()
	b.started[e.ID] = scheduledStart{method: method, at: stamp, since: since, fallback: fallback, why: why}
	b.mu.Unlock()
	slog.Info("backup schedule: started", "server", e.Name, "method", method, "every", e.BackupSchedule.Every)
}

// fallbackPoll is how often watchScheduled looks at its job. A var so tests
// make the watcher instant instead of waiting real seconds.
var fallbackPoll = time.Second

// watchScheduled follows the job started at stamp until it is terminal.
// Two jobs: ScheduleState copies a terminal outcome into the loop's view on
// READ, and this is the read that is guaranteed to happen, so a scheduled
// full backup that panicked (no history record, by the guard's contract)
// is on the page even if a manual job takes the slot before anyone loads
// it. And for an update from the recorded changes that failed, it takes a
// full backup at the same slot (fallBack). Any failure that PUBLISHED
// NOTHING qualifies, a crash included: the output is the same as a full
// backup's, and a backup is what the operator scheduled. An update whose
// fold finished and whose upload failed is the exception and takes no
// fallback (see the Published check below). Nothing is started when the
// daemon is shutting down, when the schedule was forgotten or a newer
// scheduled slot superseded this one (logged), or when another job took the
// supervisor slot before this job's end was seen (a recorded skip: the
// run history has the outcome unless the job crashed, but no fallback is
// taken for a job whose end nobody saw).
func (b *backupScheduler) watchScheduled(e console.ServerEntry, stamp, method string) {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("backup schedule: watching a scheduled job panicked", "server", e.Name, "panic", r, "stack", string(debug.Stack()))
		}
	}()
	t := time.NewTicker(fallbackPoll)
	defer t.Stop()
	for {
		<-t.C
		// Checked on every tick rather than as a select arm beside it: with
		// both ready, select picks either, and a fold that failed BECAUSE
		// the daemon is shutting down is not a refusal. A full read of
		// production is not how to shut down. Shutdown does not wait on
		// this goroutine, so leaving within a poll of the cancel is enough.
		if b.sup.ctx.Err() != nil {
			return
		}
		st := b.ScheduleState(e.ID)
		if st.LastStartedAt != stamp {
			slog.Info("backup schedule: stopped watching a scheduled job; the schedule was removed or a newer slot started",
				"server", e.Name, "method", method, "started", stamp)
			return
		}
		if st.Last == nil {
			b.skip(e, time.Now().UTC(), "another backup job took the server before the scheduled "+jobNoun(method)+
				" was seen finishing, so no full backup could stand in for it; its result is in the run history unless it crashed")
			return
		}
		if st.Running {
			continue
		}
		// Published, not State: an update whose fold finished and whose UPLOAD
		// failed already produced the snapshot a backup would have produced,
		// and a full backup would have to clear the same upload gate that just
		// refused it. Falling back there answers one S3 permission error with a
		// full lock-and-read of production that publishes nothing new (#1539).
		// DiskRefused too (#1614): a full backup stages its dump and then writes
		// the converted backup into the same directory that just refused the
		// update, so on that disk it would fail the same way, after reading the
		// source in full.
		if method == console.BackupMethodRefresh && st.Last.State == "failed" && !st.Last.Published && !st.Last.DiskRefused {
			b.fallBack(e, st.Last.LastError)
		}
		return
	}
}

// fallBack takes the full backup that stands in for a failed update, at
// the slot the update was scheduled for. The entry is re-read first: the
// update ran for minutes, and an operator who removed or changed the
// schedule meanwhile, perhaps because backups were misbehaving, must not
// get a full read of production from a schedule that no longer exists.
func (b *backupScheduler) fallBack(e console.ServerEntry, reason string) {
	cur, ok := b.reg.Get(e.ID)
	if !ok || cur.BackupSchedule == nil || cur.BackupSchedule.Identity() != e.BackupSchedule.Identity() {
		slog.Warn("backup schedule: the update from the recorded changes failed, but the schedule was removed or changed meanwhile; no full backup taken",
			"server", e.Name, "reason", reason)
		return
	}
	e = cur
	failed := console.BackupWhyFoldRefusedPrefix
	if strings.HasPrefix(reason, "internal error") {
		failed = console.BackupWhyFoldCrashedPrefix
	}
	because := failed + " (" + reason + ")"
	now := time.Now().UTC()
	if err := console.FullBackupPossible(e, b.gates()); err != nil {
		b.skip(e, now, because+" and a full backup cannot start here: "+err.Error())
		return
	}
	slog.Warn("backup schedule: "+failed+", trying a full backup instead", "server", e.Name, "reason", reason)
	// Last look before the trigger: Forget landing between the registry
	// read above and here drops the observation, and a full read of
	// production for a schedule that was just removed is the thing this
	// whole function exists to avoid.
	b.mu.Lock()
	_, observed := b.seen[e.ID]
	b.mu.Unlock()
	if !observed {
		slog.Warn("backup schedule: the update failed, but the schedule was removed meanwhile; no full backup taken", "server", e.Name)
		return
	}
	stamp := now.Format(time.RFC3339)
	if b.startFull(e, stamp, now, because, because) {
		b.mu.Lock()
		b.fallback[e.ID] = scheduledFallback{at: stamp, reason: reason}
		b.mu.Unlock()
		// Watched like any other scheduled job, so its outcome reaches the
		// loop's view without a page load.
		b.watch(e, stamp, console.BackupMethodFull)
	}
}

// Forget implements console.BackupScheduleReporter: the schedule for
// serverID was removed, so its observation, last outcome, skip and fallback
// are dropped now rather than at the next tick. A schedule re-added within
// the minute starts silent and reports nothing stale.
func (b *backupScheduler) Forget(serverID string) {
	b.mu.Lock()
	delete(b.seen, serverID)
	delete(b.warned, serverID)
	delete(b.started, serverID)
	delete(b.skipped, serverID)
	delete(b.fallback, serverID)
	b.mu.Unlock()
}

// noteSkip is skip without the history write: the log and the in-memory
// copy the page reads when the history is unavailable.
func (b *backupScheduler) noteSkip(e console.ServerEntry, now time.Time, reason string) string {
	stamp := now.Format(time.RFC3339)
	slog.Warn("backup schedule: scheduled backup did not start", "server", e.Name, "reason", reason)
	b.mu.Lock()
	b.skipped[e.ID] = scheduledSkip{at: stamp, reason: reason}
	b.mu.Unlock()
	return stamp
}

// jobNoun is the page's word for a producer.
func jobNoun(method string) string {
	if method == console.BackupMethodRefresh {
		return "update from the recorded changes"
	}
	return "full backup"
}

// skip records a slot that did not start: in memory (the page's view when
// the history is unavailable), in the history (so it survives a restart)
// and in the log.
func (b *backupScheduler) skip(e console.ServerEntry, now time.Time, reason string) {
	stamp := b.noteSkip(e, now, reason)
	if b.sup.history == nil {
		return
	}
	// Filed under the dump kind whatever the producer would have been;
	// nothing reads a skip's Kind today (runMethod applies to runs).
	_, err := b.sup.history.AppendSkip(console.BaselineRunRecord{
		ServerID: e.ID, ServerName: e.Name, Kind: console.BaselineRunDump, SkipReason: reason,
		StartedAt: stamp, FinishedAt: stamp,
	})
	if err != nil {
		slog.Warn("backup schedule: could not record the skip in the history", "server", e.Name, "error", err)
	}
}
