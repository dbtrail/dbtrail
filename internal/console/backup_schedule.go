package console

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"strconv"
	"strings"
	"time"

	"github.com/dbtrail/dbtrail/internal/cliutil"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// Backup schedule (#1442): a per-server timer for unattended backups. Stored
// on the server's registry entry, edited from the Backups page, and consumed
// by the watch daemon's schedule loop, which reads the registry every tick so
// a schedule saved from the page applies without a restart. (The registry is
// in memory once loaded: a schedule typed into the file by hand is seen after
// the next start.)
//
// The operator picks WHEN. HOW is the daemon's decision, made per slot
// (ChooseBackupMethod): the two producers this daemon has, a full backup from
// the source and a rebuild of the newest backup from the recorded change
// history, publish the same thing, a new backup in the list, and which one is
// right depends on facts the operator should not have to track: whether a
// previous backup exists to rebuild from, whether this server has a local
// directory for the fold to write into, whether the recorded history has a
// gap or a schema change in the window. The first cut put that choice in a dropdown; the
// product owner's verdict was that it asked the user to understand the fold
// to do something they think of as "backups every night".
//
// The grid is FIXED, not relative to the last run: slots sit at
// epoch + At + k*Every for every integer k. "every 1d at 03:00" is 03:00 UTC
// daily, "every 6h at 03:00" is 03:00/09:00/15:00/21:00, and the next run is
// computable from the clock alone, which is what lets the page show it and
// what keeps a restart from shifting the whole schedule to whenever the
// daemon happened to come back.

// BackupMethod values name the producer a scheduled run used. They are the
// wire format of a run's method on the Backups page, not an input.
const (
	// BackupMethodFull is a full backup from the source database (mydumper,
	// or pgbaseline for PostgreSQL): the same job the Create backup button
	// runs. It reads production and needs the console's baseline-creation
	// opt-in; when the server has an S3 destination it uploads there.
	BackupMethodFull = "backup"
	// BackupMethodRefresh rebuilds the newest backup from the recorded change
	// history alone (the same fold as --baseline-refresh-interval), reading
	// nothing from the source. It needs the server's own local directory to
	// write into; the previous backup it reads may be in the bucket, and on
	// a server whose backups go to S3 the scheduled path uploads its result
	// there afterwards (#1539).
	BackupMethodRefresh = "refresh"
)

// BackupScheduleMinEvery is the shortest interval a schedule accepts. A full
// backup every few minutes is a footgun on the source, and a rebuild that
// often is one on the disk of a server with no S3 destination, where nothing
// uploads and so retention cannot reclaim it; the floor is generous enough for every real
// cadence and low enough to try the feature out.
const BackupScheduleMinEvery = 5 * time.Minute

// backupScheduleMinEveryText is the floor as an operator types it, for the
// refusal message. Kept next to the constant so the two cannot drift apart
// unnoticed (a test pins them equal).
const backupScheduleMinEveryText = "5m"

// BackupSchedule is the per-server schedule as stored in the registry. The
// fields hold the operator-typed strings so they round-trip exactly; Parse
// turns them into durations with the same grammar as the daemon flags.
type BackupSchedule struct {
	// Every is the interval: Nm, Nh or Nd (cliutil.ParseInterval).
	Every string `yaml:"every"`
	// At is the UTC clock time the grid is aligned to, HH:MM. Empty means
	// 00:00. For whole days (1d, 7d) it is the time of day the backup runs;
	// for an interval that divides a day (6h, 15m) it is the alignment of the
	// slots; an interval that does not divide a day evenly (5h, 36h) drifts
	// through the day, and the page shows the next run so that is visible.
	At string `yaml:"at,omitempty"`
	// Extra preserves future keys the way ServerEntry.Extra does.
	Extra map[string]any `yaml:",inline"`
}

// Identity is the schedule as a comparable string (every|at, with the
// defaults resolved), for a consumer that must notice a CHANGED schedule: the
// loop treats an edit as a new schedule, so the "first observation is silent"
// rule covers edits the way it covers adds. Unparseable schedules identify
// by their raw fields.
func (b BackupSchedule) Identity() string {
	if n, err := b.Normalized(); err == nil {
		return n.Every + "|" + n.At
	}
	return b.Every + "|" + b.At
}

// ParsedBackupSchedule is a validated schedule, ready for slot arithmetic.
type ParsedBackupSchedule struct {
	Every time.Duration
	At    time.Duration // offset from midnight UTC
}

// BackupsPer30Days is how many backups this schedule publishes in 30 days:
// every one a full-table snapshot, and on a server without an S3
// destination none of them ever removed automatically.
func (p ParsedBackupSchedule) BackupsPer30Days() int64 {
	if p.Every <= 0 {
		return 0
	}
	return int64(30 * 24 * time.Hour / p.Every)
}

// Parse validates the schedule and resolves its defaults. Every error names
// the field and the accepted grammar, because the message is what the Backups
// page shows back.
func (b BackupSchedule) Parse() (ParsedBackupSchedule, error) {
	var p ParsedBackupSchedule
	every, err := cliutil.ParseInterval(strings.TrimSpace(b.Every))
	if err != nil {
		return p, fmt.Errorf("every: %w", err)
	}
	if every < BackupScheduleMinEvery {
		return p, fmt.Errorf("every: %s is too often; the shortest schedule is %s",
			strings.TrimSpace(b.Every), backupScheduleMinEveryText)
	}
	at, err := parseClockTime(b.At)
	if err != nil {
		return p, fmt.Errorf("at: %w", err)
	}
	return ParsedBackupSchedule{Every: every, At: at}, nil
}

// Normalized returns the schedule with its defaults spelled out, for storage:
// "" becomes "00:00", so the file says what runs.
func (b BackupSchedule) Normalized() (BackupSchedule, error) {
	p, err := b.Parse()
	if err != nil {
		return BackupSchedule{}, err
	}
	return BackupSchedule{
		Every: strings.TrimSpace(b.Every),
		At:    formatClockTime(p.At),
		Extra: b.Extra,
	}, nil
}

// parseClockTime parses HH:MM (24h, UTC) into an offset from midnight. Empty
// is midnight.
func parseClockTime(s string) (time.Duration, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, nil
	}
	hh, mm, ok := strings.Cut(s, ":")
	if !ok || len(hh) == 0 || len(hh) > 2 || len(mm) != 2 {
		return 0, fmt.Errorf("%q is not a time of day; use HH:MM in UTC, e.g. 03:00", s)
	}
	h, err1 := strconv.Atoi(hh)
	m, err2 := strconv.Atoi(mm)
	if err1 != nil || err2 != nil || h < 0 || h > 23 || m < 0 || m > 59 {
		return 0, fmt.Errorf("%q is not a time of day; use HH:MM in UTC, e.g. 03:00", s)
	}
	return time.Duration(h)*time.Hour + time.Duration(m)*time.Minute, nil
}

func formatClockTime(d time.Duration) string {
	return fmt.Sprintf("%02d:%02d", int(d.Hours()), int(d.Minutes())%60)
}

// scheduleEpoch anchors the slot grid. Any fixed instant at midnight UTC
// works; the Unix epoch is the one nobody has to look up.
var scheduleEpoch = time.Unix(0, 0).UTC()

// SlotAtOrBefore returns the latest slot at or before now.
func (p ParsedBackupSchedule) SlotAtOrBefore(now time.Time) time.Time {
	base := scheduleEpoch.Add(p.At)
	elapsed := now.UTC().Sub(base)
	k := elapsed / p.Every
	if elapsed < 0 && elapsed%p.Every != 0 {
		k-- // integer division truncates toward zero; the grid wants the floor
	}
	return base.Add(k * p.Every)
}

// NextRun returns the first slot strictly after now.
func (p ParsedBackupSchedule) NextRun(now time.Time) time.Time {
	return p.SlotAtOrBefore(now).Add(p.Every)
}

// ErrBackupScheduleNotRunnable is the class of every "this schedule cannot run
// on this daemon as configured" refusal; errors.Is matches it. The reason is
// carried separately (ScheduleRefusal) because it is what the page shows.
var ErrBackupScheduleNotRunnable = errors.New("the schedule cannot run")

// ScheduleRefusal is CheckBackupSchedule's error: the class plus the reason.
type ScheduleRefusal struct{ Reason string }

func (r *ScheduleRefusal) Error() string {
	return ErrBackupScheduleNotRunnable.Error() + ": " + r.Reason
}

// Is makes errors.Is(err, ErrBackupScheduleNotRunnable) true.
func (r *ScheduleRefusal) Is(target error) bool { return target == ErrBackupScheduleNotRunnable }

func notRunnable(reason string) error { return &ScheduleRefusal{Reason: reason} }

// RefusalReason returns the reason inside a ScheduleRefusal, or the whole
// message for any other error.
func RefusalReason(err error) string {
	var r *ScheduleRefusal
	if errors.As(err, &r) {
		return r.Reason
	}
	return err.Error()
}

// The refusal texts the checker and the schedule endpoints share, so a saved
// schedule is reported with the same words a write is refused with.
const (
	scheduleRefusalReadOnly = "scheduled backups run in the watch daemon (bintrail-console watch), not the read-only console"
	scheduleRefusalNoLoop   = "backup features are turned off on this daemon: BINTRAIL_CONSOLE_BASELINE_TRIGGER is not set to 1 and no refresh interval is set (CLI: --baseline-refresh-interval), so nothing can run a schedule"
	scheduleRefusalNoDumps  = "creating backups from the console is turned off on this daemon (BINTRAIL_CONSOLE_BASELINE_TRIGGER is not set to 1)"
)

// BackupScheduleGates is what the daemon can do, as the schedule checker
// needs to know it. All of it is decided at boot.
type BackupScheduleGates struct {
	// LoopRunning: this process runs the schedule loop at all (a watch daemon
	// with a baseline supervisor).
	LoopRunning bool
	// FullBackups: this process may take full backups from the source (the
	// baseline-creation opt-in). A rebuild does not need it.
	FullBackups bool
	// FullBackupsErr, when set, is why a full backup cannot START even with
	// the opt-in on: the lock-mode misconfiguration the supervisor refuses
	// every MySQL dump with.
	FullBackupsErr string
	// ReadOnlyConsole: this process is the standalone `serve` console, which
	// runs no loop of any kind; names the daemon in the reason.
	ReadOnlyConsole bool
}

// FullBackupPossible reports whether a full backup can start for e on a
// daemon with these gates, and why not. Exported for the loop's fallback
// decision, which needs exactly this half of ChooseBackupMethod.
func FullBackupPossible(e ServerEntry, gates BackupScheduleGates) error {
	if !gates.FullBackups {
		return errors.New(scheduleRefusalNoDumps)
	}
	if gates.FullBackupsErr != "" && !e.IsPostgres() {
		// Same scope as the supervisor's refusal: a PostgreSQL dump never
		// consults the MySQL lock mode.
		return errors.New(gates.FullBackupsErr)
	}
	return baselineTriggerPrecheck(e)
}

// BaselineFoldSource is where a fold for e reads its PREVIOUS snapshot from,
// which is not always where it writes the new one.
//
// The two diverge exactly when backups go to S3 (#1539): the previous snapshot
// lives in the bucket, so a fold that read the local directory would find an
// empty one on a server whose backups have only ever been uploaded, and report
// "no previous backup to update" while the bucket holds dozens. FindBaseline
// and ListBaselines both dispatch on the s3:// prefix, so the remote source
// needs no separate code path here.
//
// Note this is NOT how time-travel resolves a baseline: bundle.findBaseline
// prefers the local directory and treats the bucket only as a per-table
// fallback (#766), so on a server with both, a table whose local copy is
// stale is read stale by time-travel and fresh by the fold. The Backups
// listing merges both locations (#1571) and the point-in-time restore reads
// this same source (#1541); do not "fix" the divergence by making the fold
// read local.
//
// The OUTPUT stays local whatever this returns: the fold writes Parquet to a
// filesystem, and the upload is a separate step afterwards.
func BaselineFoldSource(e ServerEntry) string {
	if e.BaselineS3 != "" {
		return e.BaselineS3
	}
	return e.BaselineDir
}

// rebuildPossible reports whether a rebuild from the change history can be
// attempted for e (the fold itself may still refuse), and why not.
func rebuildPossible(e ServerEntry) error {
	if e.DSN == "" {
		return errors.New("this server has no index connection to read the recorded changes from")
	}
	if e.BaselineDir == "" {
		// The fold WRITES Parquet to a filesystem, so it needs the server's
		// own local directory. Its READ may be remote (see BaselineFoldSource);
		// only the write is the constraint. The refresh loop refuses on the
		// same condition.
		return errors.New("an update from the recorded changes needs a local backup directory")
	}
	return nil
}

// CheckBackupSchedule reports whether e's schedule can run on a daemon with
// these gates, and why not. Nil error means at least one producer can run.
// Called on PUT (so a schedule that could never run is refused with the
// reason instead of saved) and on every listing (so a schedule the
// environment later invalidated is reported, not silently skipped by the
// loop).
func CheckBackupSchedule(e ServerEntry, sched BackupSchedule, gates BackupScheduleGates) error {
	if _, err := sched.Parse(); err != nil {
		return notRunnable(err.Error())
	}
	if gates.ReadOnlyConsole {
		return notRunnable(scheduleRefusalReadOnly)
	}
	if !gates.LoopRunning {
		return notRunnable(scheduleRefusalNoLoop)
	}
	fullErr := FullBackupPossible(e, gates)
	if fullErr == nil {
		return nil
	}
	if rebuildErr := rebuildPossible(e); rebuildErr != nil {
		return notRunnable(strings.TrimSuffix(fullErr.Error(), " (Backup settings page)") + "; " + rebuildErr.Error() + " (Backup settings page)")
	}
	// Only a rebuild is possible. That is a runnable schedule (it is what
	// --baseline-refresh-interval does), but only once there is a backup to
	// rebuild from; the loop reports that per slot.
	return nil
}

// newestSnapshotTables is reconstruct.NewestSnapshotTables, indirected so the
// decision can be tested without a bucket: since #1539 the probe runs against
// BaselineFoldSource, which is an s3:// URL on an S3-backed server, and a unit
// test that reached the network would be neither hermetic nor offline-safe.
var newestSnapshotTables = reconstruct.NewestSnapshotTables

// ChooseBackupMethod decides how the next scheduled run for e will be made,
// and why, on a daemon with these gates. The rule, in order:
//
//   - a server an update cannot serve (no index DSN, no local backup
//     directory) gets a FULL backup, with that refusal as the why;
//   - a server with no previous backup gets a FULL backup: there is nothing
//     to update. A directory that does not exist yet is that case; one that
//     cannot be READ is its own error, never "no backup yet". The previous
//     backup is looked for where the fold would READ it (BaselineFoldSource),
//     which is the bucket on an S3-backed server;
//   - otherwise the newest backup is UPDATED from the recorded changes, with
//     no load on the source. If that update fails (a capture gap, a schema
//     change, a crash), the loop takes a full backup at the same slot when
//     the daemon may take one, and records a skip naming both otherwise.
//
// A rule that picks a producer the daemon cannot run (the opt-in off, no
// source) is reported as such; the caller decides whether that is a skip.
func ChooseBackupMethod(ctx context.Context, e ServerEntry, gates BackupScheduleGates) (method, why string, err error) {
	fullErr := FullBackupPossible(e, gates)
	rebuildErr := rebuildPossible(e)
	if rebuildErr != nil {
		if fullErr != nil {
			return BackupMethodFull, "", fmt.Errorf("%v; %v", fullErr, rebuildErr)
		}
		return BackupMethodFull, rebuildErr.Error(), nil
	}
	// An S3 destination used to force a full backup unconditionally, on the
	// reasoning that only a full backup can upload. That is no longer true
	// (#1539): the fold reads the previous snapshot straight from the s3://
	// source, writes the new one into the server's local directory, and the
	// scheduled path uploads it to the same destination a full backup would.
	//
	// The local directory is still REQUIRED and rebuildPossible above still
	// refuses without one, because the fold writes Parquet to a filesystem. On
	// an S3-only server that refusal is what keeps the old behaviour, and its
	// message is what tells the operator which setting unlocks the cheap path.
	source := BaselineFoldSource(e)
	tables, listErr := newestSnapshotTables(ctx, source)
	if listErr != nil && errors.Is(listErr, fs.ErrNotExist) {
		// A directory the first full backup has not created yet IS "no
		// backup yet". Local sources only: an S3 listing answers with an
		// empty result or an S3 error, never fs.ErrNotExist, so an empty
		// bucket reaches the len(tables) == 0 branch below instead.
		listErr, tables = nil, nil
	}
	if listErr != nil {
		// Never "no backup yet" — that reason would be false, and it would
		// quietly turn a no-load update into a nightly full read of production
		// with the page naming a cause that did not happen.
		//
		// A REMOTE source must not cost the slot either: this probe reaches the
		// network on an S3-backed server (a DuckDB httpfs listing), and before
		// #1539 those servers were guaranteed a full backup every slot without
		// touching it. Losing the night's backup to a throttled ListObjectsV2
		// would be a worse trade than an expensive backup, so the producer that
		// does not need the previous snapshot takes over, saying exactly why.
		//
		// A LOCAL one still refuses, and the scope is the whole point: an
		// unreadable directory is a permission or IO fault that a full backup
		// writing into that same directory would hit too, and it is persistent
		// where a bucket error is usually not. Degrading there would trade a
		// precise red alarm for a full read of production that then fails on
		// the way out, reporting something else.
		if fullErr != nil || !strings.HasPrefix(source, "s3://") {
			return BackupMethodFull, "", fmt.Errorf("the backup location %s could not be read: %w", source, listErr)
		}
		return BackupMethodFull, "the previous backup could not be read from the backup destination (" +
			listErr.Error() + "), so a full backup is taken instead", nil
	}
	if len(tables) == 0 {
		if fullErr != nil {
			return BackupMethodFull, "", fmt.Errorf("no previous backup to update under %s, and a full backup cannot start: %w", source, fullErr)
		}
		return BackupMethodFull, "no previous backup to update", nil
	}
	return BackupMethodRefresh, "no load on your database", nil
}

// BackupScheduleState is the schedule loop's in-memory view of one server,
// for the Backups page: the job the schedule last started in this process,
// as the supervisor's slot reports it. The durable view (runs and skips
// across restarts) is the baseline run history; this one exists because the
// history can be unavailable (an unreadable file) or silent (a job that
// panicked writes no record), and a failed scheduled backup must reach the
// page either way.
type BackupScheduleState struct {
	// LastStartedAt is when the loop last started a job for this server
	// (RFC3339 UTC), empty if never in this process.
	LastStartedAt string
	// LastMethod is that job's producer (BackupMethodFull/Refresh).
	LastMethod string
	// Last is the supervisor's status for THAT job: live while it is in the
	// slot, afterwards the copy ScheduleState took when it saw the job
	// terminal (so a later manual job taking the slot does not erase it),
	// nil when nothing was started here. The copy is taken on READ; the
	// loop's watcher is the read that always happens, so it exists for
	// every scheduled job, not only the ones a page load caught in time.
	Last *BaselineStatus
	// Running: the job this schedule last started has not finished.
	Running bool
	// LastSkippedAt / LastSkipReason describe the last slot this process
	// could not start, empty if none. The history has the durable copy.
	LastSkippedAt  string
	LastSkipReason string
	// LastFallbackAt / LastFallbackReason describe the last slot where the
	// update from the recorded changes failed and a full backup was STARTED
	// in its place (never a collision, which is a skip); cleared when a
	// later scheduled job other than that full backup succeeds. This
	// process only.
	LastFallbackAt     string
	LastFallbackReason string
}

// BackupScheduleReporter is the schedule loop as the console sees it. nil when
// this process runs no loop (the read-only console, or a watch daemon with
// every baseline feature off), which the schedule endpoints refuse on and
// the listing reports as not runnable.
type BackupScheduleReporter interface {
	ScheduleState(serverID string) BackupScheduleState
	// FullBackups reports whether the loop may run full backups (the
	// daemon's baseline-creation opt-in), and, when it may, whether the
	// supervisor would still refuse to start one (a lock-mode
	// misconfiguration): nil when full backups can start.
	FullBackups() (enabled bool, refusal error)
	// Observe tells the loop a schedule was saved at `at`, so the slot in
	// progress then counts as seen and the NEXT boundary fires, even one
	// that falls before the loop's next tick. Without it the next_run the
	// page showed at save time could be silently skipped.
	Observe(serverID string, sched BackupSchedule, at time.Time)
	// Forget tells the loop the schedule for serverID was removed, so its
	// observation and last outcome are dropped now, not at the next tick.
	Forget(serverID string)
}
