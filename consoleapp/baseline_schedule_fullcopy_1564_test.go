package consoleapp

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// #1564: a schedule can ask for a full copy on a timetable of its own, not
// only accept one as the rule's fallback.

// withFullCopy gives e the schedule every/fullEvery at 00:00 and a source
// that makes a full backup fail before mydumper, so a test can see which
// producer a slot started without running one.
func withFullCopy(t *testing.T, reg *console.Registry, e console.ServerEntry, every, fullEvery string) console.ServerEntry {
	t.Helper()
	e.SourceDSN = "not a dsn"
	e.BackupSchedule = &console.BackupSchedule{Every: every, At: "00:00", FullEvery: fullEvery}
	if err := reg.Update(e); err != nil {
		t.Fatal(err)
	}
	return e
}

func noFold(t *testing.T) {
	holdFold(t, func(context.Context, reconstruct.FullTableConfig) ([]*reconstruct.TableReport, []reconstruct.TableFailure, error) {
		return nil, nil, nil
	})
}

// On an instant both timetables land on, the full copy takes the slot: one
// job, a full backup recording why, no update and no skip.
func TestBackupScheduler_fullCopyTakesACoincidingSlot(t *testing.T) {
	noFold(t)
	b, reg, sup := newScheduleFixture(t, true)
	e := withFullCopy(t, reg, addScheduled(t, reg, true), "1h", "1d")
	b.tick(context.Background(), time.Date(2026, 8, 27, 23, 30, 0, 0, time.UTC))
	b.tick(context.Background(), time.Date(2026, 8, 28, 0, 0, 5, 0, time.UTC))
	st := waitTerminalMethod(t, b, e.ID, console.BackupMethodFull)
	if !strings.HasPrefix(st.LastWhy, console.BackupWhyFullCopyPrefix) || !strings.Contains(st.LastWhy, "every 1d") {
		t.Fatalf("the full copy recorded why = %q", st.LastWhy)
	}
	if rs := sup.RefreshStatus(e.ID); rs.State != "idle" {
		t.Fatalf("an update also started on the full copy's slot: %+v", rs)
	}
	if st.LastSkippedAt != "" {
		t.Fatalf("the run the full copy served was filed as a skip: %q", st.LastSkipReason)
	}
	run, _ := sup.history.LastScheduled(e.ID)
	if run == nil || run.Kind != console.BaselineRunDump || run.WhyCode != "full_copy" {
		t.Fatalf("history = %+v, want the full backup with why code full_copy", run)
	}
	// The next hour is an ordinary run again: the rule picks the update.
	b.tick(context.Background(), time.Date(2026, 8, 28, 1, 0, 5, 0, time.UTC))
	if st := waitTerminalMethod(t, b, e.ID, console.BackupMethodRefresh); st.LastWhy != "" {
		t.Fatalf("the next run = %+v, want an update", st)
	}
}

// A full copy whose instant is not a run's is a run of its own.
func TestBackupScheduler_fullCopyBetweenRuns(t *testing.T) {
	noFold(t)
	b, reg, sup := newScheduleFixture(t, true)
	e := withFullCopy(t, reg, addScheduled(t, reg, true), "5h", "1d")
	p, err := e.BackupSchedule.Parse()
	if err != nil {
		t.Fatal(err)
	}
	// A day whose 00:00 is not on the 5h grid, found rather than assumed.
	day := time.Date(2026, 8, 28, 0, 0, 0, 0, time.UTC)
	for p.SlotAtOrBefore(day).Equal(day) {
		day = day.Add(24 * time.Hour)
	}
	before, at := day.Add(-time.Minute), day.Add(5*time.Second)
	if !p.SlotAtOrBefore(before).Equal(p.SlotAtOrBefore(at)) {
		t.Fatalf("fixture: a run's slot falls between %s and %s", before, at)
	}
	b.tick(context.Background(), before)
	b.tick(context.Background(), at)
	if st := waitTerminalMethod(t, b, e.ID, console.BackupMethodFull); !strings.HasPrefix(st.LastWhy, console.BackupWhyFullCopyPrefix) {
		t.Fatalf("the full copy between runs = %+v", st)
	}
	if rs := sup.RefreshStatus(e.ID); rs.State != "idle" {
		t.Fatalf("an update started with no run's slot crossed: %+v", rs)
	}
}

// Full backups turned off after the schedule was saved: at the full copy's
// slot the refusal is recorded as a skip that names it, never silently, and
// the update the same slot asks for still runs.
func TestBackupScheduler_refusedFullCopyIsASkipAndTheUpdateRuns(t *testing.T) {
	noFold(t)
	b, reg, sup := newScheduleFixture(t, false)
	e := withFullCopy(t, reg, addScheduled(t, reg, true), "1h", "1d")
	b.tick(context.Background(), time.Date(2026, 8, 27, 23, 30, 0, 0, time.UTC))
	now := time.Date(2026, 8, 28, 0, 0, 5, 0, time.UTC)
	b.tick(context.Background(), now)
	st := waitTerminalMethod(t, b, e.ID, console.BackupMethodRefresh)
	// On the full backup's own line, not the ordinary skip line: that one
	// stops showing when the next run ends, and this one must not.
	if st.LastFullMissedAt != now.Format(time.RFC3339) || !console.IsFullCopySkip(st.LastFullMissedReason) ||
		!strings.Contains(st.LastFullMissedReason, "the full backup every 1d reads your database") ||
		!strings.Contains(st.LastFullMissedReason, "BINTRAIL_CONSOLE_BASELINE_TRIGGER") {
		t.Fatalf("full backup miss = %q at %q, want the refusal at the slot", st.LastFullMissedReason, st.LastFullMissedAt)
	}
	if st.LastSkippedAt != "" {
		t.Fatalf("the full backup's refusal was also filed as an ordinary skip: %q", st.LastSkipReason)
	}
	if _, skip := sup.history.LastFullCopy(e.ID); skip == nil || !strings.Contains(skip.SkipReason, "full backup every") {
		t.Fatalf("the refusal is not in the history: %+v", skip)
	}
	if _, skip := sup.history.LastScheduled(e.ID); skip != nil {
		t.Fatalf("the full backup's refusal is on the ordinary skip line of the history: %+v", skip)
	}
	if ds := sup.Status(e.ID); ds.State != "idle" {
		t.Fatalf("a full backup started with full backups off: %+v", ds)
	}
}

// Adding a full copy to a schedule never starts one on the spot: the edit is
// a first observation for both timetables, and the run at the next slot is
// an ordinary one.
func TestBackupScheduler_addingAFullCopyNeverFiresOnTheSpot(t *testing.T) {
	noFold(t)
	b, reg, sup := newScheduleFixture(t, true)
	e := addScheduled(t, reg, true)
	b.tick(context.Background(), time.Date(2026, 8, 27, 23, 30, 0, 0, time.UTC))
	// Saved three seconds after midnight, the full copy's own boundary.
	e = withFullCopy(t, reg, e, "1h", "1d")
	b.tick(context.Background(), time.Date(2026, 8, 28, 0, 0, 5, 0, time.UTC))
	if ds, rs := sup.Status(e.ID), sup.RefreshStatus(e.ID); ds.State != "idle" || rs.State != "idle" {
		t.Fatalf("adding a full copy started a job on the spot: dump %+v, update %+v", ds, rs)
	}
	b.tick(context.Background(), time.Date(2026, 8, 28, 1, 0, 5, 0, time.UTC))
	if st := waitTerminalMethod(t, b, e.ID, console.BackupMethodRefresh); st.LastWhy != "" {
		t.Fatalf("the next run = %+v, want an update", st)
	}
	if ds := sup.Status(e.ID); ds.State != "idle" {
		t.Fatalf("a full backup started at a run that is not the full copy's: %+v", ds)
	}
}

// A full copy that went through proves nothing about updates, so it does not
// end the alarm that updates are being refused (same rule as the fallback's
// own full backup).
func TestBackupScheduler_fullCopyDoesNotEndTheFallbackAlarm(t *testing.T) {
	b, reg, sup := newScheduleFixture(t, true)
	e := addScheduled(t, reg, true)
	b.fallback[e.ID] = scheduledFallback{at: "2026-08-28T09:00:07Z", reason: "capture gap"}
	b.started[e.ID] = scheduledStart{method: console.BackupMethodFull, at: "2026-08-29T00:00:00Z", since: "2026-08-29T00:00:00Z", fullCopy: true}
	sup.jobs[e.ID] = &console.BaselineStatus{State: "succeeded", Since: "2026-08-29T00:00:00Z"}
	if st := b.ScheduleState(e.ID); st.LastFallbackAt == "" {
		t.Fatalf("a scheduled full copy ended the alarm about refused updates: %+v", st)
	}
}

// Forgetting a schedule, and removing it from the registry, drop the full
// copy's observation too, so one re-added starts silent.
func TestBackupScheduler_forgetDropsTheFullCopyObservation(t *testing.T) {
	b, reg, _ := newScheduleFixture(t, true)
	e := withFullCopy(t, reg, addScheduled(t, reg, true), "1h", "1d")
	b.Observe(e.ID, *e.BackupSchedule, time.Date(2026, 8, 27, 23, 30, 0, 0, time.UTC))
	if _, ok := b.seenFull[e.ID]; !ok {
		t.Fatal("Observe did not record the full copy's slot")
	}
	b.Forget(e.ID)
	if _, ok := b.seenFull[e.ID]; ok {
		t.Fatal("Forget kept the full copy's observation")
	}
	b.Observe(e.ID, *e.BackupSchedule, time.Date(2026, 8, 27, 23, 30, 0, 0, time.UTC))
	if err := reg.Delete(e.ID); err != nil {
		t.Fatal(err)
	}
	b.tick(context.Background(), time.Date(2026, 8, 27, 23, 31, 0, 0, time.UTC))
	if _, ok := b.seenFull[e.ID]; ok {
		t.Fatal("a removed server's full copy observation survived the tick")
	}
	// A schedule that drops its full copy forgets it at the next tick.
	e2 := withFullCopy(t, reg, addScheduled(t, reg, true), "1h", "1d")
	b.tick(context.Background(), time.Date(2026, 8, 27, 23, 32, 0, 0, time.UTC))
	if _, ok := b.seenFull[e2.ID]; !ok {
		t.Fatal("the tick did not observe the full copy")
	}
	e2.BackupSchedule = &console.BackupSchedule{Every: "1h", At: "00:00"}
	if err := reg.Update(e2); err != nil {
		t.Fatal(err)
	}
	b.tick(context.Background(), time.Date(2026, 8, 27, 23, 33, 0, 0, time.UTC))
	if _, ok := b.seenFull[e2.ID]; ok {
		t.Fatal("a schedule without a full copy kept its observation")
	}
}

// A full backup that finds another job holding the server is recorded as
// the full backup that did not happen, with the timetable's prefix, and a
// full backup of the timetable that starts later ends that record.
func TestBackupScheduler_fullCopyCollisionIsAFullCopyMiss(t *testing.T) {
	noFold(t)
	b, reg, sup := newScheduleFixture(t, true)
	e := withFullCopy(t, reg, addScheduled(t, reg, true), "1h", "1d")
	sup.jobs[e.ID] = &console.BaselineStatus{State: "running", Since: "2026-08-27T23:50:00Z"}
	b.tick(context.Background(), time.Date(2026, 8, 27, 23, 30, 0, 0, time.UTC))
	now := time.Date(2026, 8, 28, 0, 0, 5, 0, time.UTC)
	b.tick(context.Background(), now)
	st := b.ScheduleState(e.ID)
	if st.LastFullMissedAt != now.Format(time.RFC3339) || !console.IsFullCopySkip(st.LastFullMissedReason) ||
		!strings.Contains(st.LastFullMissedReason, "another backup job was running") {
		t.Fatalf("collision = %q at %q, want a full backup miss naming the job", st.LastFullMissedReason, st.LastFullMissedAt)
	}
	if rs := sup.RefreshStatus(e.ID); rs.State != "idle" {
		t.Fatalf("an update started beside the collision: %+v", rs)
	}
	// The next day's full backup starts: the miss is over.
	delete(sup.jobs, e.ID)
	b.tick(context.Background(), time.Date(2026, 8, 29, 0, 0, 5, 0, time.UTC))
	st = waitTerminalMethod(t, b, e.ID, console.BackupMethodFull)
	if st.LastFullMissedAt != "" {
		t.Fatalf("a full backup of the timetable started and the miss is still on record: %+v", st)
	}
}

// When every run is a full backup (full every == every) the schedule makes
// no updates, so a full backup that went through does end an alarm about
// refused updates; otherwise it would never end.
func TestBackupScheduler_onlyFullBackupsEndTheFallbackAlarm(t *testing.T) {
	b, reg, sup := newScheduleFixture(t, true)
	e := addScheduled(t, reg, true)
	b.fallback[e.ID] = scheduledFallback{at: "2026-08-28T09:00:07Z", reason: "capture gap"}
	b.started[e.ID] = scheduledStart{method: console.BackupMethodFull, at: "2026-08-29T00:00:00Z", since: "2026-08-29T00:00:00Z", fullCopy: true, noUpdates: true}
	sup.jobs[e.ID] = &console.BaselineStatus{State: "succeeded", Since: "2026-08-29T00:00:00Z"}
	if st := b.ScheduleState(e.ID); st.LastFallbackAt != "" {
		t.Fatalf("a schedule of full backups only kept the alarm about updates it no longer makes: %+v", st)
	}
	// And the flag is set from the schedule itself.
	noFold(t)
	b2, reg2, _ := newScheduleFixture(t, true)
	e2 := withFullCopy(t, reg2, addScheduled(t, reg2, true), "1d", "1d")
	b2.tick(context.Background(), time.Date(2026, 8, 27, 23, 30, 0, 0, time.UTC))
	b2.tick(context.Background(), time.Date(2026, 8, 28, 0, 0, 5, 0, time.UTC))
	waitTerminalMethod(t, b2, e2.ID, console.BackupMethodFull)
	b2.mu.Lock()
	got := b2.started[e2.ID]
	b2.mu.Unlock()
	if !got.fullCopy || !got.noUpdates {
		t.Fatalf("started = %+v, want a full copy of a schedule that makes no updates", got)
	}
}

// A full-backup slot that finds the server busy is owed: the next scheduled
// run takes the full backup, not the next slot a whole FullEvery later. On a
// weekly timetable "skip it" was a week with no independent read.
func TestBackupScheduler_busyFullCopyIsTakenByTheNextRun(t *testing.T) {
	noFold(t)
	b, reg, sup := newScheduleFixture(t, true)
	e := withFullCopy(t, reg, addScheduled(t, reg, true), "1h", "7d")
	p, _ := e.BackupSchedule.Parse()
	slot := p.NextFullRun(time.Date(2026, 8, 20, 0, 0, 0, 0, time.UTC))
	sup.jobs[e.ID] = &console.BaselineStatus{State: "running", Since: slot.Add(-10 * time.Minute).Format(time.RFC3339)}
	b.tick(context.Background(), slot.Add(-30*time.Minute))
	b.tick(context.Background(), slot.Add(5*time.Second))
	st := b.ScheduleState(e.ID)
	if !st.FullOwed || !strings.Contains(st.LastFullMissedReason, "another backup job was running") {
		t.Fatalf("a busy full-backup slot = %+v, want it owed and the collision recorded", st)
	}
	// The recorded reason states the collision and nothing more: the debt is
	// in memory, and a restart or a save drops it, which a promise written
	// into the history would outlive. The page says it while it is live.
	if strings.Contains(st.LastFullMissedReason, "next") {
		t.Fatalf("the recorded reason promises what a restart can undo: %q", st.LastFullMissedReason)
	}
	delete(sup.jobs, e.ID)
	b.tick(context.Background(), slot.Add(time.Hour+5*time.Second))
	st = waitTerminalMethod(t, b, e.ID, console.BackupMethodFull)
	if !strings.HasPrefix(st.LastWhy, console.BackupWhyFullCopyPrefix) || st.FullOwed || st.LastFullMissedAt != "" {
		t.Fatalf("the next run = %+v, want the owed full backup, the debt paid and the miss over", st)
	}
	if rs := sup.RefreshStatus(e.ID); rs.State != "idle" {
		t.Fatalf("an update also started on the run that took the owed full backup: %+v", rs)
	}
	// Paid: the run after that is an ordinary one.
	b.tick(context.Background(), slot.Add(2*time.Hour+5*time.Second))
	if st := waitTerminalMethod(t, b, e.ID, console.BackupMethodRefresh); st.LastWhy != "" {
		t.Fatalf("the run after the paid debt = %+v, want an update", st)
	}
}

// A debt is not carried where it no longer applies: a refusal (it would
// refuse at every run), a save of the schedule, a timetable removed, Forget,
// and a schedule changed behind the loop's back.
func TestBackupScheduler_fullCopyDebtIsDropped(t *testing.T) {
	owe := func(t *testing.T) (*backupScheduler, *console.Registry, *baselineSupervisor, console.ServerEntry, time.Time) {
		t.Helper()
		noFold(t)
		b, reg, sup := newScheduleFixture(t, true)
		e := withFullCopy(t, reg, addScheduled(t, reg, true), "1h", "1d")
		sup.jobs[e.ID] = &console.BaselineStatus{State: "running", Since: "2026-08-27T23:50:00Z"}
		b.tick(context.Background(), time.Date(2026, 8, 27, 23, 30, 0, 0, time.UTC))
		b.tick(context.Background(), time.Date(2026, 8, 28, 0, 0, 5, 0, time.UTC))
		if !b.ScheduleState(e.ID).FullOwed {
			t.Fatal("fixture: the busy slot is not owed")
		}
		delete(sup.jobs, e.ID)
		return b, reg, sup, e, time.Date(2026, 8, 28, 1, 0, 5, 0, time.UTC)
	}
	t.Run("refused at the next run", func(t *testing.T) {
		b, _, sup, e, next := owe(t)
		b.fullBackups = false
		b.tick(context.Background(), next)
		st := waitTerminalMethod(t, b, e.ID, console.BackupMethodRefresh)
		if st.FullOwed || !strings.Contains(st.LastFullMissedReason, "reads your database") {
			t.Fatalf("after a refusal = %+v, want the debt dropped and the refusal on the full backup's line", st)
		}
		if ds := sup.Status(e.ID); ds.State != "idle" {
			t.Fatalf("a full backup started with full backups off: %+v", ds)
		}
	})
	t.Run("a save", func(t *testing.T) {
		b, _, _, e, _ := owe(t)
		b.Observe(e.ID, *e.BackupSchedule, time.Date(2026, 8, 28, 0, 10, 0, 0, time.UTC))
		if b.ScheduleState(e.ID).FullOwed {
			t.Fatal("a save kept a debt from before it")
		}
	})
	t.Run("the timetable removed", func(t *testing.T) {
		b, reg, _, e, next := owe(t)
		e.BackupSchedule = &console.BackupSchedule{Every: "1h", At: "00:00"}
		if err := reg.Update(e); err != nil {
			t.Fatal(err)
		}
		// The edit changes the schedule's identity, so this tick is a
		// first observation and starts nothing; the debt and the miss go.
		b.tick(context.Background(), next)
		if st := b.ScheduleState(e.ID); st.FullOwed || st.LastFullMissedAt != "" {
			t.Fatalf("a removed timetable left its debt or its miss: %+v", st)
		}
	})
	t.Run("forget", func(t *testing.T) {
		b, _, _, e, _ := owe(t)
		b.Forget(e.ID)
		if b.ScheduleState(e.ID).FullOwed {
			t.Fatal("Forget kept the debt")
		}
	})
	t.Run("changed behind the loop", func(t *testing.T) {
		b, _, _, e, _ := owe(t)
		if b.owesFull(e.ID, "1h|00:00|full 7d") {
			t.Fatal("a debt of another schedule was honoured")
		}
		if b.ScheduleState(e.ID).FullOwed {
			t.Fatal("a debt of another schedule was kept")
		}
	})
}

// A panic while firing a slot that is only the full backup's lands on the
// full backup's line, which outlasts the next run; filed as an ordinary
// skip it vanished an hour later.
func TestBackupScheduler_panicWhileFiringAFullCopyIsItsMiss(t *testing.T) {
	b, reg, _ := newScheduleFixture(t, true)
	e := withFullCopy(t, reg, addScheduled(t, reg, true), "1h", "1d")
	p, _ := e.BackupSchedule.Parse()
	broken := e
	broken.BackupSchedule = nil
	b.fireGuarded(broken, p, time.Date(2026, 8, 28, 0, 0, 5, 0, time.UTC), false, true)
	st := b.ScheduleState(e.ID)
	if !console.IsFullCopySkip(st.LastFullMissedReason) || !strings.Contains(st.LastFullMissedReason, "internal error: ") {
		t.Fatalf("a panic while firing the full backup = %+v, want it on the full backup's line", st)
	}
	if st.LastSkippedAt != "" {
		t.Fatalf("the full backup's panic was also filed as an ordinary skip: %q", st.LastSkipReason)
	}
}

// Boot: the full-backup timetable is seeded from the same observation as
// the runs. A slot in the first minute of uptime fires; one just before
// boot does not start anything.
func TestBackupScheduler_bootSeedsTheFullCopyTimetable(t *testing.T) {
	t.Run("a slot after boot fires", func(t *testing.T) {
		noFold(t)
		b, reg, _ := newScheduleFixture(t, true)
		e := withFullCopy(t, reg, addScheduled(t, reg, true), "7h", "1d")
		b.observeAll(time.Date(2026, 8, 27, 23, 59, 30, 0, time.UTC))
		b.tick(context.Background(), time.Date(2026, 8, 28, 0, 0, 20, 0, time.UTC))
		if st := waitTerminalMethod(t, b, e.ID, console.BackupMethodFull); !strings.HasPrefix(st.LastWhy, console.BackupWhyFullCopyPrefix) {
			t.Fatalf("the slot after boot = %+v, want the full backup", st)
		}
	})
	t.Run("a slot before boot does not", func(t *testing.T) {
		b, reg, sup := newScheduleFixture(t, true)
		e := withFullCopy(t, reg, addScheduled(t, reg, true), "7h", "1d")
		b.observeAll(time.Date(2026, 8, 28, 0, 0, 30, 0, time.UTC))
		b.tick(context.Background(), time.Date(2026, 8, 28, 0, 1, 30, 0, time.UTC))
		if ds, rs := sup.Status(e.ID), sup.RefreshStatus(e.ID); ds.State != "idle" || rs.State != "idle" {
			t.Fatalf("a slot before boot started a job: dump %+v, update %+v", ds, rs)
		}
	})
}

// A full-backup slot that passed while the daemon was down is never made up
// (no surprise read of production at boot) and never silent either: boot
// records it as the timetable's miss. Only a slot of the timetable in
// force, and only once.
func TestBackupScheduler_fullCopyMissedWhileDownIsRecordedAtBoot(t *testing.T) {
	boot := time.Date(2026, 8, 28, 6, 0, 0, 0, time.UTC)
	slot := time.Date(2026, 8, 28, 0, 0, 0, 0, time.UTC)
	setup := func(t *testing.T, since string) (*backupScheduler, *baselineSupervisor, console.ServerEntry) {
		t.Helper()
		b, reg, sup := newScheduleFixture(t, true)
		e := withFullCopy(t, reg, addScheduled(t, reg, true), "1h", "1d")
		e.BackupSchedule.FullSince = since
		if err := reg.Update(e); err != nil {
			t.Fatal(err)
		}
		return b, sup, e
	}
	t.Run("recorded", func(t *testing.T) {
		b, sup, e := setup(t, "2026-08-20T10:00:00Z")
		b.observeAll(boot)
		_, skip := sup.history.LastFullCopy(e.ID)
		if skip == nil || skip.FinishedAt != slot.Format(time.RFC3339) || !strings.Contains(skip.SkipReason, "was not running at the scheduled time") {
			t.Fatalf("history = %+v, want the slot at %s recorded as missed", skip, slot)
		}
		if ds, rs := sup.Status(e.ID), sup.RefreshStatus(e.ID); ds.State != "idle" || rs.State != "idle" {
			t.Fatalf("boot started a job: dump %+v, update %+v", ds, rs)
		}
		// A second restart does not record it twice.
		b.observeAll(boot.Add(time.Minute))
		n := 0
		for _, r := range sup.history.List(e.ID) {
			if console.IsFullCopySkip(r.SkipReason) {
				n++
			}
		}
		if n != 1 {
			t.Fatalf("%d full-backup misses recorded, want 1", n)
		}
	})
	t.Run("the timetable did not exist yet", func(t *testing.T) {
		b, sup, e := setup(t, "2026-08-28T01:00:00Z")
		b.observeAll(boot)
		if _, skip := sup.history.LastFullCopy(e.ID); skip != nil {
			t.Fatalf("a slot before the timetable was set was recorded as its miss: %+v", skip)
		}
	})
	t.Run("no record of when it was set", func(t *testing.T) {
		b, sup, e := setup(t, "")
		b.observeAll(boot)
		if _, skip := sup.history.LastFullCopy(e.ID); skip != nil {
			t.Fatalf("recorded a miss without knowing the timetable existed then: %+v", skip)
		}
	})
	t.Run("the slot ran", func(t *testing.T) {
		b, sup, e := setup(t, "2026-08-20T10:00:00Z")
		if err := sup.history.Append(console.BaselineRunRecord{ServerID: e.ID, Kind: console.BaselineRunDump,
			Trigger: console.BaselineRunTriggerScheduled, WhyCode: console.BackupWhyCodeFullCopy,
			StartedAt: "2026-08-28T00:00:03Z", FinishedAt: "2026-08-28T00:20:00Z"}); err != nil {
			t.Fatal(err)
		}
		b.observeAll(boot)
		if _, skip := sup.history.LastFullCopy(e.ID); skip != nil {
			t.Fatalf("a slot that ran was recorded as missed: %+v", skip)
		}
	})
}

// A full backup of the timetable displaced before its end was seen is the
// timetable's miss, in words that fit it (the ordinary sentence says "no
// full backup could stand in for it", about an update).
func TestBackupScheduler_displacedFullCopyIsItsMiss(t *testing.T) {
	b, reg, sup := newScheduleFixture(t, true)
	e := withFullCopy(t, reg, addScheduled(t, reg, true), "1h", "1d")
	old := fallbackPoll
	fallbackPoll = time.Millisecond
	t.Cleanup(func() { fallbackPoll = old })
	stamp := "2026-08-28T00:00:05Z"
	b.recordStart(e, scheduledStart{method: console.BackupMethodFull, at: stamp, since: "2026-08-28T00:00:05Z", fullCopy: true})
	// Another job holds the slot: its Since is not ours, and ours was never
	// seen terminal.
	sup.jobs[e.ID] = &console.BaselineStatus{State: "running", Since: "2026-08-28T00:00:09Z"}
	b.watchScheduled(e, stamp, console.BackupMethodFull)
	st := b.ScheduleState(e.ID)
	if !console.IsFullCopySkip(st.LastFullMissedReason) || strings.Contains(st.LastFullMissedReason, "stand in for it") {
		t.Fatalf("a displaced full backup = %q, want the timetable's miss in its own words", st.LastFullMissedReason)
	}
}
