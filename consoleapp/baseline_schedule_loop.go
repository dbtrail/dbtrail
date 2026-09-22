package consoleapp

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/go-sql-driver/mysql"
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
	// windows caches each server's last measured window (#1721) for
	// windowCacheFor, so the Snapshots page, which asks on every load, does
	// not open the index once per server per load.
	windows map[string]windowSample
	// window is the probe both the loop's and the API's gates carry:
	// measureWindow in the daemon; nil in the scheduler tests whose fixture
	// snapshot is dated weeks before their slots (the age rule would turn
	// every one of their updates into a full backup), set back by the
	// tests OF the cut-over.
	window console.BackupWindowProbe
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
	// seenFull is seen for the full-copy timetable (#1564): its own grid, so
	// its own observation, under the same identity rule. Kept apart from seen
	// rather than under a suffixed key, which the cleanup loops (live ids
	// only) would delete on every tick.
	seenFull map[string]seenSlot
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
	// fullMissed is the last slot per server of the full-backup timetable
	// that did not start (#1564), for the page when the history is
	// unavailable; dropped when a full backup of the timetable starts.
	fullMissed map[string]scheduledSkip
	// fullOwed holds, per server, the identity of the schedule whose
	// full-backup slot found another job holding the server (#1564): the
	// next scheduled run takes the full backup instead of waiting a whole
	// FullEvery for the next slot. A collision is usually the previous run
	// still going, or the daemon-wide refresh loop, and on a weekly
	// timetable "skip it" meant a week with no independent read. Dropped
	// when a full backup of the timetable starts, when it cannot start at
	// all (a refusal is not retried every run), on any save of the schedule,
	// and with the schedule.
	fullOwed map[string]string
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
	// fullCopy: this is a full copy the full-copy timetable asked for
	// (#1564). Same reason as fallback: a full backup that went through
	// proves nothing about updates, so it does not end their alarm either,
	// unless noUpdates.
	fullCopy bool
	// noUpdates: the schedule makes no updates at all (every run is a full
	// backup, FullEvery == Every), so an alarm about refused updates is
	// about a path this schedule no longer takes, and a full backup that
	// went through does end it; otherwise nothing ever would.
	noUpdates bool
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
	b := &backupScheduler{
		sup: sup, reg: reg, fullBackups: fullBackups, carryDefault: carryDefault,
		seen:       make(map[string]seenSlot),
		seenFull:   make(map[string]seenSlot),
		started:    make(map[string]scheduledStart),
		skipped:    make(map[string]scheduledSkip),
		fullMissed: make(map[string]scheduledSkip),
		fullOwed:   make(map[string]string),
		fallback:   make(map[string]scheduledFallback),
		warned:     make(map[string]bool),
	}
	b.window = b.measureWindow
	return b
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
// refusal a MySQL dump would hit right now (a lock-mode misconfiguration no
// saved setting has fixed) when there is one.
func (b *backupScheduler) FullBackups() (bool, error) {
	// The lock mode the next dump would really use (lockModeNow), not the
	// refusal this process started with: a bad lock mode fixed from the
	// Snapshots page lets dumps through again at once, and the
	// schedule's card, next-run prediction and gates must say so too rather
	// than wait for a restart.
	_, err := b.sup.lockModeNow()
	return b.fullBackups, err
}

// WindowProbe measures what an update would fold for one server (#1721).
func (b *backupScheduler) WindowProbe() console.BackupWindowProbe {
	return b.window
}

// windowProbeTimeout bounds the one index read the probe makes: it runs on
// every schedule decision AND every load of the Snapshots page, so an index
// that does not answer must cost a bounded wait and an "unknown", never a
// hung page.
const windowProbeTimeout = 3 * time.Second

// windowCacheFor is how long a measured window is reused before the index
// is read again: the loop ticks once a minute, and a page reloaded five
// times in that minute should cost one index read, not five.
const windowCacheFor = time.Minute

type windowSample struct {
	anchor time.Time
	at     time.Time
	w      console.BackupWindow
}

// measureWindow answers ChooseBackupMethod's question for e: how far the
// index has moved since the previous snapshot, how fast recent updates
// folded, and how long the last full backup took. Each part is unknown on
// its own terms, and the rule (console.CutoverToFull) says what it can
// decide with what it has:
//
//   - the events since the anchor need a base mark for THAT snapshot and
//     one read of the current mark: the base is this daemon's memo of its
//     fold of the snapshot (foldedMarks, in memory), or the mark the run
//     history recorded for the run that published it (see windowBase); no
//     base, a mark that went backwards (an index rebuilt) or an index that
//     did not answer all leave it unknown;
//   - the update model, what the history proves an update can do, the
//     last full backup's duration and whether an update was measured
//     after it come from the run history, and are unknown without one. The
//     history also keeps the mark each update and each full backup read,
//     so the count falls back to the record of the run that published the
//     anchor: after a restart, and after a full backup (#1737), which the
//     model then abstains on until an update is measured again.
func (b *backupScheduler) measureWindow(ctx context.Context, e console.ServerEntry, anchor time.Time) console.BackupWindow {
	b.mu.Lock()
	// Not across the cut-over (#1791): a window cached just before the anchor
	// passed it carries no capture verdict, and reused past it would take a
	// full backup the source could have ruled out.
	if c, ok := b.windows[e.ID]; ok && c.anchor.Equal(anchor) && time.Since(c.at) < windowCacheFor &&
		!(c.w.Events == 0 && c.w.Capture == "" && c.w.CaptureDetail == "" && pastCutover(e, c.w, time.Now())) {
		b.mu.Unlock()
		return c.w
	}
	b.mu.Unlock()
	w := console.BackupWindow{Anchor: anchor, Events: -1}
	base, known := b.windowBase(e, anchor)
	if b.sup.history != nil {
		w.FoldFixed, w.FoldRate = b.sup.history.UpdateModel(e.ID)
		w.LastFull = b.sup.history.LastFullBackup(e.ID)
		if w.LastFull > 0 {
			w.Proven = b.sup.history.ProvenUpdate(e.ID, w.LastFull)
		}
		w.UnmeasuredSinceFull = !b.sup.history.MeasuredSinceFull(e.ID)
		if !anchor.IsZero() {
			w.AnchorFullFinished = b.sup.history.FullBackupFinished(e.ID, anchor.UTC().Format(time.RFC3339))
		}
	}
	if known {
		ctx, cancel := context.WithTimeout(ctx, windowProbeTimeout)
		defer cancel()
		cur, answered := readIndexMark(ctx, probeDSN(e.DSN))
		b.reportWindowBlind(e, answered)
		if answered && cur.events >= base {
			w.Events = int64(cur.events - base)
		}
	}
	if w.Events == 0 && pastCutover(e, w, time.Now()) {
		var sourceRead time.Time
		if b.sup.history != nil {
			sourceRead = b.sup.history.LastSourceRead(e.ID, anchor)
		}
		w.Capture, w.CaptureDetail = b.captureVerdict(ctx, e, anchor, sourceRead)
	}
	if ctx.Err() != nil {
		// The caller went away mid-probe (a page request aborted): what was
		// read is its timeout, not an answer, and cached it would decide the
		// loop's next slot for a minute.
		return w
	}
	b.mu.Lock()
	if b.windows == nil {
		b.windows = map[string]windowSample{}
	}
	b.windows[e.ID] = windowSample{anchor: anchor, at: time.Now(), w: w}
	b.mu.Unlock()
	return w
}

// pastCutover reports whether w's anchor is older than e's cut-over age,
// counted the way console.CutoverToFull counts it: the only case in which
// the capture verdict can change a decision, and so the only one in which
// the source is asked. A schedule that does not parse counts as no interval
// (the two-hour floor), which asks earlier, never later.
func pastCutover(e console.ServerEntry, w console.BackupWindow, now time.Time) bool {
	if w.Anchor.IsZero() {
		return false
	}
	var interval time.Duration
	if e.BackupSchedule != nil {
		if p, err := e.BackupSchedule.Parse(); err == nil {
			interval = p.Every
		}
	}
	since := w.Anchor
	if w.AnchorFullFinished.After(since) {
		since = w.AnchorFullFinished
	}
	return now.Sub(since) > console.BackupCutoverAge(interval)
}

// captureVerdict asks whether the source wrote anything the capture has not
// recorded since anchor (#1791, probeCapture), for the servers it can be
// asked about, and says what it found in the log (reportCaptureProbe).
// sourceRead is when the newest full backup that read the source started,
// which the capture's dropped rows are dated against. A caller that went
// away mid-probe learned nothing about the source, so nothing is logged.
func (b *backupScheduler) captureVerdict(ctx context.Context, e console.ServerEntry, anchor, sourceRead time.Time) (verdict, detail string) {
	var r captureProbeResult
	switch {
	case e.SourceDSN == "":
		r.detail = "this server has no source to ask"
	case e.IsPostgres():
		r.detail = "PostgreSQL sources are not compared yet"
	default:
		r = probeCapture(ctx, e.DSN, e.SourceDSN, anchor, sourceRead)
	}
	if ctx.Err() != nil {
		return r.verdict, r.detail
	}
	b.reportCaptureProbe(e, anchor, r)
	return r.verdict, r.detail
}

// reportCaptureProbe says, once per condition and server (gateEdge), why a
// server that indexed nothing still takes a full backup on age, or that it
// no longer does. The window probe runs on page loads too, so an unlimited
// line would repeat every minute. Warn when a read failed or the source is
// ahead, which is worth a look; Info when the reason is structural (a
// position-mode capture, a PostgreSQL source), since those hold for the
// server's lifetime and a daily warning about them would teach an operator
// to ignore the line.
func (b *backupScheduler) reportCaptureProbe(e console.ServerEntry, anchor time.Time, r captureProbeResult) {
	quiet := "capture-caught-up:" + e.ID
	loud := "capture-probe:" + e.ID
	if r.verdict == console.CaptureCaughtUp {
		b.sup.gateEdge.Resolve(loud)
		if b.sup.gateEdge.Fire(quiet, anchor.UTC().Format(time.RFC3339)) {
			slog.Info("backup schedule: nothing was indexed since the previous backup and the source confirms it wrote nothing the capture has not recorded; updating instead of taking a full backup on age",
				"server", e.Name, "id", e.ID, "previous_backup", anchor.UTC().Format(time.RFC3339))
		}
		return
	}
	b.sup.gateEdge.Resolve(quiet)
	if !b.sup.gateEdge.Fire(loud, r.detail) {
		return
	}
	args := []any{"server", e.Name, "id", e.ID, "reason", r.detail}
	if r.cause != "" {
		args = append(args, "error", r.cause)
	}
	msg := "backup schedule: nothing was indexed since the previous backup, but the source could not confirm it wrote nothing; the full backup on age stays in place"
	if r.cause != "" || r.verdict == console.CaptureBehind {
		slog.Warn(msg, args...)
		return
	}
	slog.Info(msg, args...)
}

// probeDSN bounds the DIAL of the probe's index connection too: the context
// above bounds the queries only, and config.Connect pings with the DSN's own
// dial budget (ten seconds by default), so a host that accepts the
// connection and goes silent would hold a page load for that long before
// the three seconds started. Same shape as the console's test-connection
// probe. A DSN that does not parse is handed on as is: the read then fails
// for its own reason.
func probeDSN(dsn string) string {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return dsn
	}
	if cfg.Timeout == 0 || cfg.Timeout > windowProbeTimeout {
		cfg.Timeout = windowProbeTimeout
	}
	return cfg.FormatDSN()
}

// windowBase is the index mark an update from anchor counts its events from:
// the in-memory memo of this daemon's fold that published it, or the mark
// the run history recorded for it (after a restart emptied the memo, or when
// a full backup published it, #1737). Like measuredEvents, a memo on another
// index means the server was re-pointed since this daemon's last fold, and
// no recorded mark can be trusted to come from the current one.
func (b *backupScheduler) windowBase(e console.ServerEntry, anchor time.Time) (uint64, bool) {
	if anchor.IsZero() {
		return 0, false
	}
	b.sup.mu.Lock()
	memo, seen := b.sup.foldedMarks[e.ID]
	b.sup.mu.Unlock()
	if seen && memo.indexDSN != e.DSN {
		return 0, false
	}
	if seen && reconstruct.SnapshotDirName(memo.publishedAt) == reconstruct.SnapshotDirName(anchor) {
		return memo.mark.events, true
	}
	if b.sup.history != nil {
		return b.sup.history.IndexMarkFor(e.ID, anchor.UTC().Format(time.RFC3339))
	}
	return 0, false
}

// reportWindowBlind says once, at Warn, that the index stopped answering
// the window probe: from then on the cut-over runs on the age of the
// previous backup alone, and a full read of production on age would
// otherwise be diagnosed as the rule misfiring. Resolved when it answers.
func (b *backupScheduler) reportWindowBlind(e console.ServerEntry, answered bool) {
	key := "window-probe:" + e.ID
	if answered {
		b.sup.gateEdge.Resolve(key)
		return
	}
	if b.sup.gateEdge.Fire(key, "") {
		slog.Warn("backup schedule: the index did not answer the update-size probe in time; until it does, the choice between "+
			"an update and a full backup falls back to the age of the previous backup alone",
			"server", e.Name, "id", e.ID, "timeout", windowProbeTimeout)
	}
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
	// A save is a fresh start for the full-backup timetable: a debt from
	// before it is not carried into it, and a removed timetable leaves no
	// miss behind to come back if it is set again.
	delete(b.fullOwed, serverID)
	if p.FullEvery > 0 {
		b.seenFull[serverID] = seenSlot{identity: sched.Identity(), slot: p.FullSlotAtOrBefore(at.UTC())}
	} else {
		delete(b.seenFull, serverID)
		delete(b.fullMissed, serverID)
	}
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
				b.noteFullMissedWhileDown(e, p, at)
			}
		}
	}
}

// noteFullMissedWhileDown records the full-backup slot that passed while the
// daemon was not running (#1564). A missed slot is never made up (a full
// read of production at boot is a surprise nobody scheduled), but for the
// full-backup timetable it is not silent either: a weekly slot lost to a
// restart is a week with no independent read, and the page says so the way
// it says any other miss. Only the newest slot at or before boot, and only
// when it belongs to the timetable in force: after FullSince, and not
// already accounted for by a run or a skip of the timetable in the history.
// Without FullSince, or without a history, nothing is recorded: a slot from
// before the timetable existed must not be reported as its miss.
func (b *backupScheduler) noteFullMissedWhileDown(e console.ServerEntry, p console.ParsedBackupSchedule, boot time.Time) {
	if p.FullEvery <= 0 || b.sup == nil || b.sup.history == nil {
		return
	}
	since, err := time.Parse(time.RFC3339, e.BackupSchedule.FullSince)
	if err != nil {
		return
	}
	slot := p.FullSlotAtOrBefore(boot.UTC())
	if !slot.After(since) {
		return
	}
	stamp := slot.Format(time.RFC3339)
	run, skip := b.sup.history.LastFullCopy(e.ID)
	if (run != nil && run.StartedAt >= stamp) || (skip != nil && skip.FinishedAt >= stamp) {
		return
	}
	b.skip(e, slot, console.FullCopySkipReason("DBTrail was not running at the scheduled time, or stopped before the full backup finished"))
}

// warnBackupScheduleRate is the schedule's version of the refresh loop's
// disk warning: every backup it publishes is a full-table snapshot, and
// PruneLocal only removes what it confirmed durable in S3, so on a server
// without an S3 destination nothing ever removes them. Logged at boot and
// at save, with the 30-day count, so the operator reads the rate before
// the disk does.
func warnBackupScheduleRate(e console.ServerEntry, p console.ParsedBackupSchedule) {
	slog.Warn("backup schedule: every run publishes a full-table snapshot",
		"server", e.Name, "every", p.Every, "backups_per_30d", p.BackupsPer30Days(),
		"local_only", e.BaselineS3 == "", "dir", e.BaselineDir,
		"full_every", p.FullEvery, "full_copies_per_30d", p.FullCopiesPer30Days())
}

// ScheduleState implements console.BackupScheduleReporter.
func (b *backupScheduler) ScheduleState(serverID string) console.BackupScheduleState {
	b.mu.Lock()
	st, started := b.started[serverID]
	sk, skipped := b.skipped[serverID]
	fm, fullMissed := b.fullMissed[serverID]
	_, owed := b.fullOwed[serverID]
	fb, fell := b.fallback[serverID]
	b.mu.Unlock()
	var out console.BackupScheduleState
	out.FullOwed = owed
	if skipped {
		out.LastSkippedAt, out.LastSkipReason = sk.at, sk.reason
	}
	if fullMissed {
		out.LastFullMissedAt, out.LastFullMissedReason = fm.at, fm.reason
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
		// A full backup published locally while its copy to the destination
		// still runs (#1725) is in flight for the schedule too: the copy
		// below is taken once, so taking it now would freeze "uploaded: 0"
		// as the run's record, and the fallback alarm would end on a
		// backup the destination does not have yet.
		out.Running = cur.State == "running" || cur.Uploading
		if !out.Running {
			// Keep the outcome: a later manual job overwrites the slot, and
			// a job that panicked has no history record, so this copy is
			// the page's only evidence until the schedule's next run.
			b.mu.Lock()
			if cur2, ok := b.started[serverID]; ok && cur2.since == st.since {
				cur2.last = &cur
				b.started[serverID] = cur2
				if !st.fallback && (!st.fullCopy || st.noUpdates) && cur.State == "succeeded" {
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
	// Logged HERE and not only in the refresh loop: a daemon that runs backup
	// schedules and no refresh interval never reaches that other line, so
	// before #1681 flipped the default there was no surface at all naming
	// what this daemon does with a table that did not change.
	//
	// BOTH keys, because either one alone misleads: with table deltas on (the
	// default) an unchanged table keeps its file whatever the reuse flag
	// says, so a bare reuse_unchanged=false would read as "every table is
	// rewritten" on a daemon that rewrites nothing.
	slog.Info("backup schedule loop enabled", "tick", backupScheduleTick, "full_backups", sched.fullBackups,
		"reuse_unchanged_path", sched.carryDefault, "table_deltas", sched.sup.tableDeltas)
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
		// Both grids are observed on every tick, whichever fires, so neither
		// one's edge is lost to the other's.
		regular := b.crossed(e.ID, e.BackupSchedule.Identity(), p.SlotAtOrBefore(now))
		full := b.crossedFull(e.ID, e.BackupSchedule.Identity(), p, now)
		if !regular && !full {
			continue
		}
		// A full backup a busy server kept from starting is taken by the
		// next run, not a whole FullEvery later.
		if regular && !full && b.owesFull(e.ID, e.BackupSchedule.Identity()) {
			full = true
		}
		b.fireGuarded(e, p, now, regular, full)
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
	for id := range b.seenFull {
		if !live[id] {
			delete(b.seenFull, id)
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
	for id := range b.fullMissed {
		if !live[id] {
			delete(b.fullMissed, id)
		}
	}
	for id := range b.fullOwed {
		if !live[id] {
			delete(b.fullOwed, id)
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

// crossedFull is crossed on the full-copy timetable (#1564), with the same
// rules: the first observation of an identity is silent, so adding or
// editing the full copy never starts one on the spot. A schedule without one
// forgets any observation, so one added later starts silent too.
func (b *backupScheduler) crossedFull(serverID, identity string, p console.ParsedBackupSchedule, now time.Time) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if p.FullEvery <= 0 {
		delete(b.seenFull, serverID)
		delete(b.fullMissed, serverID)
		delete(b.fullOwed, serverID)
		return false
	}
	slot := p.FullSlotAtOrBefore(now)
	prev, ok := b.seenFull[serverID]
	b.seenFull[serverID] = seenSlot{identity: identity, slot: slot}
	return ok && prev.identity == identity && slot.After(prev.slot)
}

// fireGuarded is fire with its own recover: a panic while firing one
// server's slot must not cost the other servers this tick, and the slot,
// already recorded as crossed, would otherwise vanish with no page
// evidence. It is noted as a skip naming the internal error, in memory
// and the log only: the history write is inside the region this recover
// guards, and re-entering it from here could panic a second time, which
// this recover could not catch (the same rule as recoverBaselineJob).
func (b *backupScheduler) fireGuarded(e console.ServerEntry, p console.ParsedBackupSchedule, now time.Time, regular, full bool) {
	// firingFull says which half panicked, so the skip lands on the line
	// that outlasts the next run (the full-backup timetable's) only when it
	// was the full backup that never started.
	firingFull := false
	defer func() {
		if r := recover(); r != nil {
			slog.Error("backup schedule: firing a slot panicked", "server", e.Name, "panic", r, "stack", string(debug.Stack()))
			reason := fmt.Sprintf("internal error: %v", r)
			if firingFull {
				reason = console.FullCopySkipReason(reason)
			}
			b.noteSkip(e, now, reason)
		}
	}()
	if full {
		firingFull = true
		if b.fireFullCopy(e, p, now) {
			// The full copy takes the slot: a run on the same instant is
			// served by it, not queued behind it (one job per server).
			return
		}
		firingFull = false
	}
	if regular {
		b.fire(e, p, now)
	}
}

// owesFull reports whether a full-backup slot of this schedule found the
// server busy and is still owed (see fullOwed).
func (b *backupScheduler) owesFull(serverID, identity string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	owed, ok := b.fullOwed[serverID]
	if ok && owed != identity {
		delete(b.fullOwed, serverID)
		return false
	}
	return ok
}

// fireFullCopy starts the full copy the full-copy timetable asks for at this
// slot (#1564), and reports whether the slot is taken care of: true when it
// started, and when it could not start because another job holds the server
// (a run at the same instant would hit the same wall). False when full
// backups cannot start here at all: that is recorded as a skip naming the
// reason, loudly, and the update a run on the same instant would make still
// runs, since losing the scheduled backup because its weekly full copy
// cannot start would be the wrong trade.
func (b *backupScheduler) fireFullCopy(e console.ServerEntry, p console.ParsedBackupSchedule, now time.Time) bool {
	gates := b.gates()
	// Every skip here carries the timetable's own prefix, so the page keeps
	// it on its own line until a full backup starts again (see
	// console.BackupSkipFullCopyPrefix).
	if err := console.CheckBackupSchedule(e, *e.BackupSchedule, gates); err != nil {
		b.dropFullDebt(e.ID)
		b.skip(e, now, console.FullCopySkipReason(console.RefusalReason(err)))
		return true
	}
	if err := console.CheckFullCopy(e, *e.BackupSchedule, gates); err != nil {
		// Not owed: a refusal would refuse the same way at every run, and
		// the page already says so in red until it changes.
		b.dropFullDebt(e.ID)
		b.skip(e, now, console.FullCopySkipReason(console.RefusalReason(err)))
		return false
	}
	why := console.FullCopyWhy(*e.BackupSchedule)
	slog.Info("backup schedule: taking the full backup the schedule asks for", "server", e.Name, "id", e.ID, "reason", why)
	stamp := now.Format(time.RFC3339)
	if b.startFullCopy(e, stamp, now, why, p.FullEvery == p.Every) {
		b.watch(e, stamp, console.BackupMethodFull)
	}
	return true
}

func (b *backupScheduler) dropFullDebt(serverID string) {
	b.mu.Lock()
	delete(b.fullOwed, serverID)
	b.mu.Unlock()
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
	method, why, err := console.ChooseBackupMethodAt(b.sup.ctx, e, gates, now)
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
	// The #1721 cut-over is the plan working as designed, not a degradation,
	// but it is the expensive producer replacing the cheap one, so it is
	// said in the log with the numbers the decision was made on, before the
	// run starts and not only on the page afterwards.
	if code := console.BackupWhyCode(why); method == console.BackupMethodFull && (code == "window_measured" || code == "window_age") {
		args := []any{"server", e.Name, "id", e.ID, "reason", why}
		if code == "window_measured" {
			args = append(args, b.modelLogArgs(e.ID, now)...)
		}
		slog.Info("backup schedule: taking a full backup instead of an update from the recorded changes; the update would cost more, or its starting point is too old to fold cheaply", args...)
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

// modelLogArgs is what the log says about the update model next to a full
// backup it chose (#1737): the rate, the fixed cost, how many updates it was
// fitted from and how old the newest of them is, so a model that stopped
// learning is visible in the daemon log and not only in the reason on the
// Snapshots page. Read from the history again rather than carried from the
// decision: nothing is recorded between the two, one tick apart at most.
func (b *backupScheduler) modelLogArgs(serverID string, now time.Time) []any {
	h := b.sup.history
	if h == nil {
		return nil
	}
	fixed, rate := h.UpdateModel(serverID)
	n, newest := h.UpdateSample(serverID)
	// Unknown, not zero: the fixed-cost verdict chooses a full backup with
	// no rate at all, and a logged 0 would read as a measured one.
	var perSecond any = "unknown"
	if rate > 0 {
		perSecond = math.Round(rate*100) / 100
	}
	args := []any{"fold_rate_events_per_second", perSecond, "fold_fixed", fixed.Round(time.Second), "fold_samples", n}
	if !newest.IsZero() {
		args = append(args, "newest_sample_age", now.Sub(newest).Round(time.Second))
	}
	return args
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
	g := console.BackupScheduleGates{LoopRunning: true, FullBackups: enabled, Window: b.window}
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
	// for a scheduled rebuild that is the schedule's own `every`. Passing the
	// wrong one here does not only mislabel a log attribute: it decides
	// whether the warning fires at all, since every reading past
	// refreshOnTime is gated on took exceeding it. Note the remedy the
	// warning offers is NOT always this setting; a refresh whose cost is
	// rising with its window is not fixed by any `every`. See gradeRefresh.
	since, err := b.sup.TriggerRefresh(req, p.Every)
	if err != nil {
		return err
	}
	b.record(e, console.BackupMethodRefresh, stamp, since, false, "")
	return nil
}

// startFull triggers a full backup and records the job as the schedule's,
// or records the skip; reports whether the job started. because, when set,
// is the failed update this full backup stands in for, so a collision skip
// carries both facts. why is the reason a full backup was chosen at all
// (#1604), carried on the request so the run record keeps it.
func (b *backupScheduler) startFull(e console.ServerEntry, stamp string, now time.Time, because, why string) bool {
	return b.startFullBackup(e, stamp, now, because, why, false, false)
}

// startFullCopy is startFull for the full-copy timetable (#1564). noUpdates:
// the schedule makes no updates at all (see scheduledStart).
func (b *backupScheduler) startFullCopy(e console.ServerEntry, stamp string, now time.Time, why string, noUpdates bool) bool {
	return b.startFullBackup(e, stamp, now, "", why, true, noUpdates)
}

func (b *backupScheduler) startFullBackup(e console.ServerEntry, stamp string, now time.Time, because, why string, fullCopy, noUpdates bool) bool {
	// A full-copy slot's skip carries the timetable's prefix, so a collision
	// is recorded as the weekly full backup that did not happen, not as an
	// ordinary missed run the next hour makes up.
	skipText := func(s string) string {
		if fullCopy {
			return console.FullCopySkipReason(s)
		}
		return s
	}
	req := console.BaselineRequestFor(e)
	req.Trigger = console.BaselineRunTriggerScheduled
	req.Why = why
	prefix, when := "", "at the scheduled time"
	if because != "" {
		prefix, when = because+"; ", "when the full backup was tried"
	}
	switch err := b.sup.Trigger(req); {
	case err == nil:
		b.recordStart(e, scheduledStart{method: console.BackupMethodFull, at: stamp, since: b.sup.Status(e.ID).Since,
			fallback: because != "", fullCopy: fullCopy, noUpdates: noUpdates, why: why})
		return true
	case errors.Is(err, console.ErrBaselineRunning):
		// The collision the issue names: a manual backup, restore or export
		// (or the previous scheduled run) holds the server. Skip, do not
		// queue: a queued dump would fire at an unscheduled moment. The
		// full-backup timetable's slot is owed to the next scheduled run
		// instead, which is a moment the operator did schedule.
		if fullCopy {
			b.mu.Lock()
			b.fullOwed[e.ID] = e.BackupSchedule.Identity()
			b.mu.Unlock()
			// The reason states only the collision. That the next run takes
			// the full backup is said by the page while the debt is live
			// (FullOwed): the debt is in memory, and a restart or a save
			// drops it, which a promise written into the history would outlive.
			b.skip(e, now, skipText(prefix+"another backup job was running for this server "+when))
			return false
		}
		b.skip(e, now, skipText(prefix+"another backup job was running for this server "+when))
	default:
		b.skip(e, now, skipText(prefix+err.Error()))
	}
	return false
}

// record notes the job the schedule just started. The supervisor's own
// stamp is read back right after the trigger: it is the key ScheduleState
// attributes the slot by, and the trigger returned, so the slot is ours
// until the job finishes and something else claims it.
func (b *backupScheduler) record(e console.ServerEntry, method, stamp, since string, fallback bool, why string) {
	b.recordStart(e, scheduledStart{method: method, at: stamp, since: since, fallback: fallback, why: why})
}

func (b *backupScheduler) recordStart(e console.ServerEntry, st scheduledStart) {
	b.mu.Lock()
	b.started[e.ID] = st
	if st.fullCopy {
		// The timetable's full backup started: its missed-slot line ends,
		// and whatever a busy server left owed is paid.
		delete(b.fullMissed, e.ID)
		delete(b.fullOwed, e.ID)
	}
	b.mu.Unlock()
	slog.Info("backup schedule: started", "server", e.Name, "method", st.method, "every", e.BackupSchedule.Every, "full_copy", st.fullCopy)
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
			// A cycle the #1689 gate skipped leaves no outcome on the slot,
			// deliberately: it restores what it displaced rather than
			// reporting a run that never happened. To the branch below that
			// reads exactly like a job whose end nobody saw, and the skip it
			// files would name another job as the cause. The slot IS
			// unproduced and a skip is the right record; only the reason
			// differs, and on a quiet server this is the reason every period.
			b.mu.Lock()
			since := b.started[e.ID].since
			b.mu.Unlock()
			// Guarded on the method as well as the stamp. A scheduled FULL
			// backup reads a different slot, so a stale gate-skip entry could
			// otherwise explain a dump with "nothing had been indexed". Not
			// reachable today; it costs nothing to make it structural.
			if method == console.BackupMethodRefresh && b.sup.refreshSkippedAsUnchanged(e.ID, since) {
				b.skip(e, time.Now().UTC(), "nothing had been indexed since the last backup, so this "+
					"slot had nothing to add to it")
				return
			}
			b.mu.Lock()
			fullCopy := b.started[e.ID].fullCopy
			b.mu.Unlock()
			if fullCopy {
				b.skip(e, time.Now().UTC(), console.FullCopySkipReason("another backup job took the server before the full backup was "+
					"seen finishing; its result is in the run history unless it crashed"))
				return
			}
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
	delete(b.seenFull, serverID)
	delete(b.warned, serverID)
	delete(b.started, serverID)
	delete(b.skipped, serverID)
	delete(b.fullMissed, serverID)
	delete(b.fullOwed, serverID)
	delete(b.fallback, serverID)
	delete(b.windows, serverID)
	b.mu.Unlock()
}

// noteSkip is skip without the history write: the log and the in-memory
// copy the page reads when the history is unavailable.
func (b *backupScheduler) noteSkip(e console.ServerEntry, now time.Time, reason string) string {
	stamp := now.Format(time.RFC3339)
	slog.Warn("backup schedule: scheduled backup did not start", "server", e.Name, "reason", reason)
	b.mu.Lock()
	if console.IsFullCopySkip(reason) {
		b.fullMissed[e.ID] = scheduledSkip{at: stamp, reason: reason}
	} else {
		b.skipped[e.ID] = scheduledSkip{at: stamp, reason: reason}
	}
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
	// Filed under the dump kind whatever the producer would have been. The
	// history's LastFullBackup walks records by this kind and relies on its
	// SkipReason guard to pass these over (#1721): a skip must never read
	// as a full backup's duration.
	_, err := b.sup.history.AppendSkip(console.BaselineRunRecord{
		ServerID: e.ID, ServerName: e.Name, Kind: console.BaselineRunDump, SkipReason: reason,
		StartedAt: stamp, FinishedAt: stamp,
	})
	if err != nil {
		slog.Warn("backup schedule: could not record the skip in the history", "server", e.Name, "error", err)
	}
}
