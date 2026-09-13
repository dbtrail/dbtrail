package consoleapp

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/mydumperlock"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// The watcher polls every fallbackPoll; real seconds would make every test
// that fires wait for them, and a test that returned BEFORE the first poll
// proved nothing about the watcher at all (the original bug: every success
// path returned in milliseconds and the "do not fall back after a success"
// rule was unreachable).
func init() { fallbackPoll = 10 * time.Millisecond }

// newScheduleFixture builds a scheduler over a real supervisor and a real
// history file. The context is cancelled BEFORE the temp dirs are removed
// (Cleanup runs LIFO), but a fired job takes no context on its history
// write, so every test that fires must also await the job: see waitTerminal.
func newScheduleFixture(t *testing.T, fullBackups bool) (*backupScheduler, *console.Registry, *baselineSupervisor) {
	t.Helper()
	staging := t.TempDir()
	histDir := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	sup := newBaselineSupervisor(ctx, staging, baseline.DefaultLockMode)
	h, err := console.OpenBaselineHistory(histDir + "/h.json")
	if err != nil {
		t.Fatal(err)
	}
	sup.history = h
	reg, err := console.LoadRegistry("")
	if err != nil {
		t.Fatal(err)
	}
	return newBackupScheduler(sup, reg, fullBackups, false), reg, sup
}

// addScheduled adds an hourly schedule on a server with a source and a
// local backup directory. withBackup writes a fake previous backup into it,
// which is what makes the daemon choose a rebuild; without one it takes a
// full backup, and a source DSN that does not parse makes that fail before
// mydumper.
func addScheduled(t *testing.T, reg *console.Registry, withBackup bool) console.ServerEntry {
	t.Helper()
	dir := t.TempDir()
	source := "not a dsn"
	if withBackup {
		writeFakeSnapshot(t, dir)
		source = "src:pw@tcp(127.0.0.1:3306)/"
	}
	e, err := reg.Add(console.ServerEntry{Name: "wp", DSN: "idx:pw@tcp(127.0.0.1:3306)/idx",
		SourceDSN: source, BaselineDir: dir,
		BackupSchedule: &console.BackupSchedule{Every: "1h", At: "00:00"}})
	if err != nil {
		t.Fatal(err)
	}
	return e
}

// waitTerminal polls the loop's own view until the job it started reached a
// terminal state. The job goroutine writes the history BEFORE it flips the
// status, so waiting on the history alone let a test return with the job
// still finishing, and its last writes then landed in a temp dir the test
// had already removed (CI: "TempDir RemoveAll cleanup: directory not empty").
func waitTerminal(t *testing.T, b *backupScheduler, id string) console.BackupScheduleState {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		st := b.ScheduleState(id)
		if st.Last != nil && !st.Running {
			// The watcher's last poll, and any fallback it started: the
			// snapshot is returned, not a re-read, because after a fallback
			// the re-read would describe the full backup instead.
			b.watchers.Wait()
			return st
		}
		if time.Now().After(deadline) {
			t.Fatalf("the scheduled job never reached a terminal state in the loop's view: %+v", st)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// waitTerminalMethod is waitTerminal for a specific producer: a fallback
// changes the method, and the caller wants the second job.
func waitTerminalMethod(t *testing.T, b *backupScheduler, id, method string) console.BackupScheduleState {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		st := b.ScheduleState(id)
		if st.LastMethod == method && st.Last != nil && !st.Running {
			b.watchers.Wait()
			return st
		}
		if time.Now().After(deadline) {
			t.Fatalf("no terminal %s job in the loop's view: %+v", method, st)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// fireAt drives the two ticks that fire a fresh 1h schedule: the observation
// and the next boundary.
func fireAt(b *backupScheduler, t0 time.Time) {
	b.tick(context.Background(), t0)
	b.tick(context.Background(), t0.Add(time.Hour))
}

// holdFold replaces the fold seam with one that returns err (or panics when
// err is nil and panicWith is set) and restores it once the job under test
// is terminal; the caller must await the job before the test ends.
func holdFold(t *testing.T, fn func(context.Context, reconstruct.FullTableConfig) ([]*reconstruct.TableReport, []reconstruct.TableFailure, error)) {
	t.Helper()
	prev := foldTables
	foldTables = fn
	t.Cleanup(func() { foldTables = prev })
}

// The first observation records the slot and never fires; a later, newer
// slot fires; the same slot again does not; a different identity (an edited
// schedule) is a first observation again.
func TestBackupScheduler_crossedIsEdgeTriggered(t *testing.T) {
	b, _, _ := newScheduleFixture(t, true)
	s1 := time.Date(2026, 8, 28, 9, 0, 0, 0, time.UTC)
	if b.crossed("a", "1h|00:00", s1) {
		t.Fatal("the first observation fired")
	}
	if b.crossed("a", "1h|00:00", s1) {
		t.Fatal("the same slot fired twice")
	}
	if !b.crossed("a", "1h|00:00", s1.Add(time.Hour)) {
		t.Fatal("a newer slot did not fire")
	}
	if b.crossed("a", "1h|00:00", s1) {
		t.Fatal("an older slot fired")
	}
	if b.crossed("a", "30m|00:00", s1.Add(2*time.Hour)) {
		t.Fatal("a changed schedule fired on its first observation")
	}
	if !b.crossed("a", "30m|00:00", s1.Add(3*time.Hour)) {
		t.Fatal("the changed schedule did not fire on its next slot")
	}
}

// Saving a schedule starts nothing: the tick that first sees it only
// records the slot. The next slot boundary is what fires, and a boundary
// that passed before the schedule was seen is not replayed.
func TestBackupScheduler_savingNeverFiresOnTheSpot(t *testing.T) {
	b, reg, sup := newScheduleFixture(t, true)
	e := addScheduled(t, reg, true)
	holdFold(t, func(context.Context, reconstruct.FullTableConfig) ([]*reconstruct.TableReport, []reconstruct.TableFailure, error) {
		return nil, nil, nil
	})
	now := time.Date(2026, 8, 28, 9, 0, 30, 0, time.UTC) // 30s past the 09:00 slot
	b.tick(context.Background(), now)
	if st := sup.RefreshStatus(e.ID); st.State != "idle" {
		t.Fatalf("the first tick started a job: %+v", st)
	}
	if run, skip := sup.history.LastScheduled(e.ID); run != nil || skip != nil {
		t.Fatal("the first tick wrote to the history")
	}
	b.tick(context.Background(), now.Add(30*time.Minute))
	if st := sup.RefreshStatus(e.ID); st.State != "idle" {
		t.Fatalf("a tick inside the same slot started a job: %+v", st)
	}
	// 10:00: the boundary crossed while the loop was watching.
	b.tick(context.Background(), now.Add(time.Hour))
	if st := sup.RefreshStatus(e.ID); st.State == "idle" {
		t.Fatal("the slot boundary did not start the scheduled rebuild")
	}
	if st := waitTerminal(t, b, e.ID); st.LastStartedAt == "" || st.LastMethod != console.BackupMethodRefresh {
		t.Fatalf("the loop did not record what it started: %+v", st)
	}
}

// Editing a schedule is as silent as adding one. Before the identity was
// part of the observation, "every 1d at 03:00" observed at 03:00 and edited
// to "every 6h" in the afternoon fired a full dump of production within a
// minute of Save, while the toast said it would run at the next slot.
func TestBackupScheduler_editingNeverFiresOnTheSpot(t *testing.T) {
	b, reg, sup := newScheduleFixture(t, true)
	e := addScheduled(t, reg, true)
	holdFold(t, func(context.Context, reconstruct.FullTableConfig) ([]*reconstruct.TableReport, []reconstruct.TableFailure, error) {
		return nil, nil, nil
	})
	e.BackupSchedule = &console.BackupSchedule{Every: "1d", At: "03:00"}
	if err := reg.Update(e); err != nil {
		t.Fatal(err)
	}
	b.tick(context.Background(), time.Date(2026, 8, 28, 3, 0, 20, 0, time.UTC))
	// 15:30: the operator edits to every 6h at 03:00. The slot at or before
	// now under the new grid is 15:00, newer than the 03:00 seen under the
	// old one.
	e.BackupSchedule = &console.BackupSchedule{Every: "6h", At: "03:00"}
	if err := reg.Update(e); err != nil {
		t.Fatal(err)
	}
	b.tick(context.Background(), time.Date(2026, 8, 28, 15, 30, 0, 0, time.UTC))
	if st := sup.RefreshStatus(e.ID); st.State != "idle" {
		t.Fatalf("editing the schedule started a job on the spot: %+v", st)
	}
	// 21:00 under the new grid: that one fires.
	b.tick(context.Background(), time.Date(2026, 8, 28, 21, 0, 10, 0, time.UTC))
	if st := sup.RefreshStatus(e.ID); st.State == "idle" {
		t.Fatal("the edited schedule's next slot did not fire")
	}
	waitTerminal(t, b, e.ID)
}

// The API observes a schedule at the instant it is saved, so a boundary
// that falls between the save and the loop's next tick fires: the next_run
// the page reported is kept. Without Observe, the tick at 03:00:20 was the
// first observation and today's 03:00 was silently dropped.
func TestBackupScheduler_observeAtSaveKeepsThePromisedNextRun(t *testing.T) {
	holdFold(t, func(context.Context, reconstruct.FullTableConfig) ([]*reconstruct.TableReport, []reconstruct.TableFailure, error) {
		return nil, nil, nil
	})
	b, reg, sup := newScheduleFixture(t, true)
	e := addScheduled(t, reg, true)
	sched := console.BackupSchedule{Every: "1d", At: "03:00"}
	e.BackupSchedule = &sched
	if err := reg.Update(e); err != nil {
		t.Fatal(err)
	}
	b.Observe(e.ID, sched, time.Date(2026, 8, 28, 2, 59, 50, 0, time.UTC))
	b.tick(context.Background(), time.Date(2026, 8, 28, 3, 0, 20, 0, time.UTC))
	if st := sup.RefreshStatus(e.ID); st.State == "idle" {
		t.Fatal("the boundary between the save and the first tick was dropped")
	}
	waitTerminal(t, b, e.ID)

	// Saved just AFTER the boundary: the slot in progress is the 03:00 one,
	// and the first tick must not fire it.
	b2, reg2, sup2 := newScheduleFixture(t, true)
	e2 := addScheduled(t, reg2, true)
	e2.BackupSchedule = &sched
	if err := reg2.Update(e2); err != nil {
		t.Fatal(err)
	}
	b2.Observe(e2.ID, sched, time.Date(2026, 8, 28, 3, 0, 10, 0, time.UTC))
	b2.tick(context.Background(), time.Date(2026, 8, 28, 3, 0, 30, 0, time.UTC))
	if st := sup2.RefreshStatus(e2.ID); st.State != "idle" {
		t.Fatalf("a save after the boundary fired the slot in progress: %+v", st)
	}
}

// At boot every schedule is observed at the boot instant: a boundary in the
// first minute of uptime fires (the daemon was up for it), a boundary before
// boot does not.
func TestBackupScheduler_observeAllAtBoot(t *testing.T) {
	holdFold(t, func(context.Context, reconstruct.FullTableConfig) ([]*reconstruct.TableReport, []reconstruct.TableFailure, error) {
		return nil, nil, nil
	})
	b, reg, sup := newScheduleFixture(t, true)
	e := addScheduled(t, reg, true)
	b.observeAll(time.Date(2026, 8, 28, 8, 59, 40, 0, time.UTC))
	b.tick(context.Background(), time.Date(2026, 8, 28, 9, 0, 40, 0, time.UTC))
	if st := sup.RefreshStatus(e.ID); st.State == "idle" {
		t.Fatal("a boundary in the first minute of uptime did not fire")
	}
	waitTerminal(t, b, e.ID)
}

// A slot the daemon's clock moved past while it was down is not replayed:
// the first tick after boot is a first observation.
func TestBackupScheduler_missedSlotsAreNotReplayed(t *testing.T) {
	b, reg, sup := newScheduleFixture(t, true)
	e := addScheduled(t, reg, true)
	b.observeAll(time.Date(2026, 8, 28, 14, 59, 0, 0, time.UTC))
	b.tick(context.Background(), time.Date(2026, 8, 28, 14, 59, 30, 0, time.UTC))
	if st := sup.RefreshStatus(e.ID); st.State != "idle" {
		t.Fatalf("a slot from before boot was replayed: %+v", st)
	}
}

// How each run is made is the daemon's decision: a server with a previous
// backup on disk gets a rebuild, one without gets a full backup. Both stamp
// the trigger on their history record, which is what the page attributes
// runs by; deleting either stamp compiled and passed before this.
func TestBackupScheduler_choosesTheProducerAndStampsTheTrigger(t *testing.T) {
	for _, tc := range []struct {
		name       string
		withBackup bool
		wantMethod string
		wantKind   string
	}{
		{"previous backup on disk: rebuild", true, console.BackupMethodRefresh, console.BaselineRunRefresh},
		{"no backup yet: full backup", false, console.BackupMethodFull, console.BaselineRunDump},
	} {
		t.Run(tc.name, func(t *testing.T) {
			holdFold(t, func(context.Context, reconstruct.FullTableConfig) ([]*reconstruct.TableReport, []reconstruct.TableFailure, error) {
				return nil, nil, errors.New("seam: refused")
			})
			b, reg, sup := newScheduleFixture(t, true)
			e := addScheduled(t, reg, tc.withBackup)
			e.SourceDSN = "not a dsn" // whichever producer runs, it fails before any network
			if err := reg.Update(e); err != nil {
				t.Fatal(err)
			}
			fireAt(b, time.Date(2026, 8, 28, 9, 0, 5, 0, time.UTC))
			// The update's refusal is followed by a fallback full backup, so
			// the slot settles on the full backup either way. Waiting for
			// the update's own terminal state raced the fallback's record
			// (a 10ms window) and was flaky by construction.
			st := waitTerminalMethod(t, b, e.ID, console.BackupMethodFull)
			if st.Last.State != "failed" {
				t.Fatalf("the fixture was supposed to fail fast: %+v", st)
			}
			// Every record the slot wrote carries the scheduled stamp: the
			// update's own (which the newest-record read used to hide behind
			// the fallback's) and the full backup's.
			recs := sup.history.List(e.ID)
			var kinds []string
			for _, r := range recs {
				if r.Trigger != console.BaselineRunTriggerScheduled {
					t.Fatalf("record %+v is not stamped scheduled", r)
				}
				kinds = append(kinds, r.Kind)
			}
			want := []string{console.BaselineRunDump}
			if tc.withBackup {
				want = []string{console.BaselineRunRefresh, console.BaselineRunDump}
			}
			// The history is append-ordered, so the update comes first and
			// its fallback second; accepting either order would let the
			// fallback's record pass for the update's.
			if strings.Join(kinds, ",") != strings.Join(want, ",") {
				t.Fatalf("records = %v, want kinds %v", kinds, want)
			}
			if st.LastMethod != console.BackupMethodFull || (tc.withBackup && st.LastFallbackAt == "") {
				t.Fatalf("slot state = %+v, want the full backup on record%s", st, map[bool]string{true: " as a fallback", false: ""}[tc.withBackup])
			}
		})
	}
}

// A rebuild the fold refuses falls back to a full backup at the same slot,
// and the fallback is on record for the page. On a daemon that cannot take
// a full backup, the refusal becomes a skip that names both reasons.
func TestBackupScheduler_refusedRebuildFallsBackToAFullBackup(t *testing.T) {
	holdFold(t, func(context.Context, reconstruct.FullTableConfig) ([]*reconstruct.TableReport, []reconstruct.TableFailure, error) {
		return nil, nil, errors.New("capture gap in the reconstruction window")
	})
	b, reg, sup := newScheduleFixture(t, true)
	e := addScheduled(t, reg, true)
	e.SourceDSN = "not a dsn" // the fallback full backup fails fast too
	if err := reg.Update(e); err != nil {
		t.Fatal(err)
	}
	fireAt(b, time.Date(2026, 8, 28, 9, 0, 5, 0, time.UTC))
	st := waitTerminalMethod(t, b, e.ID, console.BackupMethodFull)
	if st.LastFallbackAt == "" || !strings.Contains(st.LastFallbackReason, "capture gap") {
		t.Fatalf("the fallback was not recorded: %+v", st)
	}
	if sup.Status(e.ID).State != "failed" {
		t.Fatalf("the full backup did not run after the refusal: %+v", sup.Status(e.ID))
	}
	if run, _ := sup.history.LastScheduled(e.ID); run == nil || run.Kind != console.BaselineRunDump {
		t.Fatalf("the newest scheduled record is not the fallback's full backup: %+v", run)
	}
	// The fallback line is an alarm about the rebuild path, so it outlives
	// the full backup it caused, and ends when a later scheduled rebuild
	// goes through. Not at restart, not never.
	if st := b.ScheduleState(e.ID); st.LastFallbackAt == "" {
		t.Fatal("the fallback was forgotten as soon as its full backup finished")
	}
	holdFold(t, func(context.Context, reconstruct.FullTableConfig) ([]*reconstruct.TableReport, []reconstruct.TableFailure, error) {
		return nil, nil, nil
	})
	b.tick(context.Background(), time.Date(2026, 8, 28, 11, 0, 5, 0, time.UTC))
	if st := waitTerminalMethod(t, b, e.ID, console.BackupMethodRefresh); st.Last.State != "succeeded" || st.LastFallbackAt != "" {
		t.Fatalf("a rebuild that went through did not end the fallback alarm: %+v", st)
	}

	// Without the creation opt-in there is no fallback: a skip with both
	// reasons, and no dump started.
	holdFold(t, func(context.Context, reconstruct.FullTableConfig) ([]*reconstruct.TableReport, []reconstruct.TableFailure, error) {
		return nil, nil, errors.New("capture gap in the reconstruction window")
	})
	b2, reg2, sup2 := newScheduleFixture(t, false)
	e2 := addScheduled(t, reg2, true)
	fireAt(b2, time.Date(2026, 8, 28, 9, 0, 5, 0, time.UTC))
	waitTerminal(t, b2, e2.ID)
	// The in-memory skip lands BEFORE the history write; waiting on the
	// history too keeps the test from returning mid-write into its TempDir.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, skip := sup2.history.LastScheduled(e2.ID); skip != nil && b2.ScheduleState(e2.ID).LastSkippedAt != "" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	st2 := b2.ScheduleState(e2.ID)
	if !strings.Contains(st2.LastSkipReason, "capture gap") || !strings.Contains(st2.LastSkipReason, "not set to 1") {
		t.Fatalf("skip reason = %q, want the refusal and why a full backup cannot start", st2.LastSkipReason)
	}
	// The reason is about THIS server: it has no S3 destination and the
	// message must not invent one (the first cut routed the decision through
	// a forced S3 field and pasted its sentence into the skip).
	if strings.Contains(st2.LastSkipReason, "S3") {
		t.Fatalf("skip reason names S3 on a server that has none: %q", st2.LastSkipReason)
	}
	if sup2.Status(e2.ID).State != "idle" {
		t.Fatal("a full backup was started without the opt-in")
	}
}

// A rebuild that fails because the daemon is shutting down is not a
// refusal: no full backup of production is started on the way out.
func TestBackupScheduler_noFallbackDuringShutdown(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	holdFold(t, func(ctx context.Context, _ reconstruct.FullTableConfig) ([]*reconstruct.TableReport, []reconstruct.TableFailure, error) {
		close(started)
		<-release
		return nil, nil, ctx.Err()
	})
	ctx, cancel := context.WithCancel(context.Background())
	sup := newBaselineSupervisor(ctx, t.TempDir(), baseline.DefaultLockMode)
	h, err := console.OpenBaselineHistory(t.TempDir() + "/h.json")
	if err != nil {
		t.Fatal(err)
	}
	sup.history = h
	reg, _ := console.LoadRegistry("")
	b := newBackupScheduler(sup, reg, true, false)
	e := addScheduled(t, reg, true)
	e.SourceDSN = "src:pw@tcp(127.0.0.1:3306)/"
	if err := reg.Update(e); err != nil {
		t.Fatal(err)
	}
	fireAt(b, time.Date(2026, 8, 28, 9, 0, 5, 0, time.UTC))
	<-started
	cancel() // the daemon is going down while the rebuild is in flight
	close(release)
	waitTerminal(t, b, e.ID)
	if st := sup.Status(e.ID); st.State != "idle" {
		t.Fatalf("a full backup was started during shutdown: %+v", st)
	}
	if st := b.ScheduleState(e.ID); st.LastFallbackAt != "" {
		t.Fatalf("a shutdown-caused failure was recorded as a fallback: %+v", st)
	}
}

// A rebuild that SUCCEEDS starts nothing else: the watcher sees the success
// and leaves. The mutant that ignored the outcome (falling back after every
// rebuild) survived every earlier test, because they all returned before
// the watcher's first poll; this one waits for it.
func TestBackupScheduler_successfulRebuildDoesNotFallBack(t *testing.T) {
	holdFold(t, func(context.Context, reconstruct.FullTableConfig) ([]*reconstruct.TableReport, []reconstruct.TableFailure, error) {
		return nil, nil, nil
	})
	b, reg, sup := newScheduleFixture(t, true)
	e := addScheduled(t, reg, true)
	fireAt(b, time.Date(2026, 8, 28, 9, 0, 5, 0, time.UTC))
	if st := waitTerminal(t, b, e.ID); st.Last.State != "succeeded" {
		t.Fatalf("rebuild = %+v, want succeeded", st.Last)
	}
	if st := sup.Status(e.ID); st.State != "idle" {
		t.Fatalf("a full backup was started after a successful rebuild: %+v", st)
	}
	if st := b.ScheduleState(e.ID); st.LastFallbackAt != "" || st.LastSkippedAt != "" {
		t.Fatalf("a successful rebuild left a fallback or skip behind: %+v", st)
	}
}

// A crashed rebuild is a failed one: with the opt-in on, the watcher falls
// back to a full backup and the fallback names the crash.
func TestBackupScheduler_crashedRebuildFallsBack(t *testing.T) {
	holdFold(t, func(context.Context, reconstruct.FullTableConfig) ([]*reconstruct.TableReport, []reconstruct.TableFailure, error) {
		panic("boom")
	})
	b, reg, _ := newScheduleFixture(t, true)
	e := addScheduled(t, reg, true)
	e.SourceDSN = "not a dsn" // the fallback full backup fails fast
	if err := reg.Update(e); err != nil {
		t.Fatal(err)
	}
	fireAt(b, time.Date(2026, 8, 28, 9, 0, 5, 0, time.UTC))
	st := waitTerminalMethod(t, b, e.ID, console.BackupMethodFull)
	if !strings.HasPrefix(st.LastFallbackReason, "internal error") {
		t.Fatalf("fallback = %+v, want the crash as its reason", st)
	}
}

// The fallback alarm is about the update path, so the fallback's OWN full
// backup succeeding does not end it; only a later successful scheduled
// update does. The firing test cannot pin this (its fallback full backup
// fails by construction), so the state is set by hand.
func TestBackupScheduler_fallbackAlarmSurvivesItsOwnFullBackup(t *testing.T) {
	b, reg, sup := newScheduleFixture(t, true)
	e := addScheduled(t, reg, true)
	b.fallback[e.ID] = scheduledFallback{at: "2026-08-28T09:00:07Z", reason: "capture gap"}
	b.started[e.ID] = scheduledStart{method: console.BackupMethodFull, at: "2026-08-28T09:00:07Z", since: "2026-08-28T09:00:07Z", fallback: true}
	sup.jobs[e.ID] = &console.BaselineStatus{State: "succeeded", Since: "2026-08-28T09:00:07Z"}
	if st := b.ScheduleState(e.ID); st.LastFallbackAt == "" || st.Last == nil || st.Last.State != "succeeded" {
		t.Fatalf("the fallback's own full backup ended the alarm: %+v", st)
	}
	// A full backup the RULE picked at a later slot (the server now goes to
	// S3, say) does end it: the alarm would otherwise be immortal on a
	// server that never takes the update path again.
	b.started[e.ID] = scheduledStart{method: console.BackupMethodFull, at: "2026-08-28T10:00:00Z", since: "2026-08-28T10:00:00Z"}
	sup.jobs[e.ID] = &console.BaselineStatus{State: "succeeded", Since: "2026-08-28T10:00:00Z"}
	if st := b.ScheduleState(e.ID); st.LastFallbackAt != "" {
		t.Fatalf("a later successful full backup picked by the rule did not end the alarm: %+v", st)
	}
	// And so does a successful scheduled update.
	b.fallback[e.ID] = scheduledFallback{at: "2026-08-28T11:00:07Z", reason: "capture gap"}
	b.started[e.ID] = scheduledStart{method: console.BackupMethodRefresh, at: "2026-08-28T12:00:00Z", since: "2026-08-28T12:00:00Z"}
	sup.refreshes[e.ID] = &console.BaselineStatus{State: "succeeded", Since: "2026-08-28T12:00:00Z"}
	if st := b.ScheduleState(e.ID); st.LastFallbackAt != "" {
		t.Fatalf("a later successful update did not end the alarm: %+v", st)
	}
}

// When the fallback full backup cannot start because another job holds the
// server, the page must not say a full backup was taken: the skip carries
// both facts and no fallback is recorded.
func TestBackupScheduler_fallbackCollisionIsASkipNotAFallback(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	holdFold(t, func(context.Context, reconstruct.FullTableConfig) ([]*reconstruct.TableReport, []reconstruct.TableFailure, error) {
		close(started)
		<-release
		return nil, nil, errors.New("capture gap in the reconstruction window")
	})
	b, reg, sup := newScheduleFixture(t, true)
	e := addScheduled(t, reg, true)
	fireAt(b, time.Date(2026, 8, 28, 9, 0, 5, 0, time.UTC))
	<-started
	// A manual backup takes the server while the rebuild is failing.
	sup.mu.Lock()
	sup.jobs[e.ID] = &console.BaselineStatus{State: "running", Since: "2099-01-01T00:00:00Z"}
	sup.mu.Unlock()
	close(release)
	waitTerminal(t, b, e.ID)
	deadline := time.Now().Add(5 * time.Second)
	for b.ScheduleState(e.ID).LastSkippedAt == "" && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	st := b.ScheduleState(e.ID)
	if st.LastFallbackAt != "" {
		t.Fatalf("a fallback was recorded although no full backup started: %+v", st)
	}
	if !strings.Contains(st.LastSkipReason, "was refused (capture gap") || !strings.Contains(st.LastSkipReason, "another backup job was running for this server when the full backup was tried") {
		t.Fatalf("skip reason = %q, want both the failed update and the collision", st.LastSkipReason)
	}
	sup.mu.Lock()
	delete(sup.jobs, e.ID)
	sup.mu.Unlock()
}

// The rebuild ran for minutes; the schedule was changed meanwhile. The
// fallback re-reads the entry and starts no full backup for a schedule
// that no longer exists in that form.
func TestBackupScheduler_changedScheduleGetsNoFallback(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	holdFold(t, func(context.Context, reconstruct.FullTableConfig) ([]*reconstruct.TableReport, []reconstruct.TableFailure, error) {
		close(started)
		<-release
		return nil, nil, errors.New("capture gap in the reconstruction window")
	})
	b, reg, sup := newScheduleFixture(t, true)
	e := addScheduled(t, reg, true)
	fireAt(b, time.Date(2026, 8, 28, 9, 0, 5, 0, time.UTC))
	<-started
	e, _ = reg.Get(e.ID)
	e.BackupSchedule = &console.BackupSchedule{Every: "6h", At: "03:00"}
	if err := reg.Update(e); err != nil {
		t.Fatal(err)
	}
	close(release)
	waitTerminal(t, b, e.ID)
	if st := sup.Status(e.ID); st.State != "idle" {
		t.Fatalf("a full backup was started for a schedule that was changed meanwhile: %+v", st)
	}
	if st := b.ScheduleState(e.ID); st.LastFallbackAt != "" || st.LastSkippedAt != "" {
		t.Fatalf("something was recorded for the changed schedule: %+v", st)
	}
}

// Forget (what the DELETE route calls) drops the loop's whole view of the
// server at once, so a schedule re-added within the minute starts silent.
func TestBackupScheduler_forgetDropsEverything(t *testing.T) {
	b, reg, _ := newScheduleFixture(t, true)
	e := addScheduled(t, reg, true)
	b.seen[e.ID] = seenSlot{identity: "1h|00:00", slot: time.Date(2026, 8, 28, 9, 0, 0, 0, time.UTC)}
	b.started[e.ID] = scheduledStart{method: console.BackupMethodFull, at: "x", since: "x"}
	b.skipped[e.ID] = scheduledSkip{at: "x", reason: "r"}
	b.fallback[e.ID] = scheduledFallback{at: "x", reason: "r"}
	b.warned[e.ID] = true
	b.Forget(e.ID)
	if st := b.ScheduleState(e.ID); st.LastStartedAt != "" || st.LastSkippedAt != "" || st.LastFallbackAt != "" {
		t.Fatalf("Forget left state behind: %+v", st)
	}
	if b.warned[e.ID] {
		t.Fatal("Forget kept the warn-once mark, so a re-added unreadable schedule would never be reported")
	}
	if b.crossed(e.ID, "1h|00:00", time.Date(2026, 8, 28, 10, 0, 0, 0, time.UTC)) {
		t.Fatal("after Forget the next slot fired without a first observation")
	}
}

// A panic while firing one server's slot is recorded as that server's skip
// and does not cost the other servers the tick.
func TestBackupScheduler_panicWhileFiringIsASkip(t *testing.T) {
	b, reg, sup := newScheduleFixture(t, true)
	e := addScheduled(t, reg, true)
	// A registry entry whose schedule pointer is nil at fire time makes
	// fire dereference nil: the guard turns that into a skip.
	p, _ := e.BackupSchedule.Parse()
	broken := e
	broken.BackupSchedule = nil
	b.fireGuarded(broken, p, time.Date(2026, 8, 28, 9, 0, 5, 0, time.UTC))
	st := b.ScheduleState(e.ID)
	if !strings.HasPrefix(st.LastSkipReason, "internal error: ") {
		t.Fatalf("a panic while firing left no skip: %+v", st)
	}
	// In memory and the log only: the history write is inside the guarded
	// region, and re-entering it from the recover could panic a second
	// time, which nothing would catch (the recoverBaselineJob rule).
	if _, skip := sup.history.LastScheduled(e.ID); skip != nil {
		t.Fatalf("the recover re-entered the history: %+v", skip)
	}
}

// The fallback's own full backup is watched like any other scheduled job:
// a fallback dump that crashed (no history record) stays on the page even
// when a manual job takes the slot before anyone loads it. Polls
// sup.Status, not ScheduleState, because ScheduleState is the copy the
// watcher exists to make.
func TestBackupScheduler_fallbackFullBackupIsWatched(t *testing.T) {
	holdFold(t, func(context.Context, reconstruct.FullTableConfig) ([]*reconstruct.TableReport, []reconstruct.TableFailure, error) {
		return nil, nil, errors.New("capture gap in the reconstruction window")
	})
	prev := checkMydumperPrivileges
	checkMydumperPrivileges = func(context.Context, string, baseline.LockMode, mydumperlock.Remedy, []string) error {
		panic("boom in the fallback dump")
	}
	t.Cleanup(func() { checkMydumperPrivileges = prev })
	b, reg, sup := newScheduleFixture(t, true)
	e := addScheduled(t, reg, true)
	fireAt(b, time.Date(2026, 8, 28, 9, 0, 5, 0, time.UTC))
	deadline := time.Now().Add(10 * time.Second)
	for {
		if st := sup.Status(e.ID); st.State == "failed" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the fallback dump never failed: %+v", sup.Status(e.ID))
		}
		time.Sleep(10 * time.Millisecond)
	}
	b.watchers.Wait()
	sup.mu.Lock()
	sup.jobs[e.ID] = &console.BaselineStatus{State: "running", Since: "2099-01-01T00:00:00Z"}
	sup.mu.Unlock()
	st := b.ScheduleState(e.ID)
	if st.LastMethod != console.BackupMethodFull || st.Last == nil || st.Last.State != "failed" || !strings.Contains(st.Last.LastError, "boom") {
		t.Fatalf("the crashed fallback dump is not on record after a manual job took the slot: %+v", st)
	}
}

// Another job replaced the supervisor slot before the watcher saw the
// scheduled job end: a recorded skip saying so, no fallback, no panic.
func TestBackupScheduler_slotTheftIsASkip(t *testing.T) {
	b, reg, sup := newScheduleFixture(t, true)
	e := addScheduled(t, reg, true)
	b.seen[e.ID] = seenSlot{identity: "1h|00:00", slot: time.Date(2026, 8, 28, 9, 0, 0, 0, time.UTC)}
	b.started[e.ID] = scheduledStart{method: console.BackupMethodRefresh, at: "2026-08-28T09:00:00Z", since: "2026-08-28T09:00:00Z"}
	sup.refreshes[e.ID] = &console.BaselineStatus{State: "failed", Since: "2026-08-28T09:00:03Z", LastError: "capture gap"}
	b.watchScheduled(e, "2026-08-28T09:00:00Z", console.BackupMethodRefresh)
	st := b.ScheduleState(e.ID)
	if !strings.Contains(st.LastSkipReason, "took the server") || st.LastFallbackAt != "" {
		t.Fatalf("slot theft = %+v, want a skip naming it and no fallback", st)
	}
	if _, skip := sup.history.LastScheduled(e.ID); skip == nil {
		t.Fatal("the slot-theft skip did not reach the history")
	}
	if sup.Status(e.ID).State != "idle" {
		t.Fatal("a full backup was started for a job whose end nobody saw")
	}
}

// A watcher whose slot was superseded (a newer scheduled fire, or Forget)
// leaves without recording anything against the newer slot.
func TestBackupScheduler_supersededWatcherRecordsNothing(t *testing.T) {
	b, reg, sup := newScheduleFixture(t, true)
	e := addScheduled(t, reg, true)
	b.started[e.ID] = scheduledStart{method: console.BackupMethodRefresh, at: "2026-08-28T10:00:00Z", since: "2026-08-28T10:00:00Z"}
	sup.refreshes[e.ID] = &console.BaselineStatus{State: "running", Since: "2026-08-28T10:00:00Z"}
	// The 09:00 watcher, one slot behind. Driven with a deadline: without
	// its exit it would follow the 10:00 job, which never finishes here,
	// and a hang is a worse red than a message.
	done := make(chan struct{})
	go func() {
		defer close(done)
		b.watchScheduled(e, "2026-08-28T09:00:00Z", console.BackupMethodRefresh)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("the superseded watcher kept following the newer slot's job")
	}
	if st := b.ScheduleState(e.ID); st.LastSkippedAt != "" || st.LastFallbackAt != "" || st.LastStartedAt != "2026-08-28T10:00:00Z" {
		t.Fatalf("the superseded watcher touched the newer slot: %+v", st)
	}
}

// startFull's non-collision refusal keeps the failed update in front of it,
// so the skip says both what failed and why nothing stood in for it.
func TestBackupScheduler_startFullKeepsTheBecause(t *testing.T) {
	b, reg, sup := newScheduleFixture(t, true)
	e := addScheduled(t, reg, true)
	sup.configErr = errors.New("bad lock mode")
	now := time.Date(2026, 8, 28, 9, 0, 7, 0, time.UTC)
	if b.startFull(e, now.Format(time.RFC3339), now, "the update was refused (capture gap)") {
		t.Fatal("a refused trigger reported a started job")
	}
	if st := b.ScheduleState(e.ID); !strings.HasPrefix(st.LastSkipReason, "the update was refused (capture gap); ") || !strings.Contains(st.LastSkipReason, "bad lock mode") {
		t.Fatalf("skip reason = %q, want the because in front of the refusal", st.LastSkipReason)
	}
}

// The scheduled rebuild carries the effective reuse setting: the console's
// saved override over the daemon flag, the same resolution the refresh loop
// and a restore use. Hardcoding either value compiled and passed.
func TestBackupScheduler_rebuildCarriesTheEffectiveCarryForward(t *testing.T) {
	for _, want := range []bool{true, false} {
		t.Run(map[bool]string{true: "override on", false: "override off"}[want], func(t *testing.T) {
			var mu sync.Mutex
			var got []bool
			holdFold(t, func(_ context.Context, cfg reconstruct.FullTableConfig) ([]*reconstruct.TableReport, []reconstruct.TableFailure, error) {
				mu.Lock()
				got = append(got, cfg.CarryForwardUnchanged)
				mu.Unlock()
				return nil, nil, nil
			})
			b, reg, _ := newScheduleFixture(t, true)
			b.carryDefault = !want // the flag says the opposite; the override must win
			if err := reg.SetBaselineRefresh(&console.BaselineRefreshConfig{CarryForwardUnchanged: want}); err != nil {
				t.Fatal(err)
			}
			e := addScheduled(t, reg, true)
			fireAt(b, time.Date(2026, 8, 28, 9, 0, 5, 0, time.UTC))
			waitTerminal(t, b, e.ID)
			mu.Lock()
			defer mu.Unlock()
			if len(got) != 1 || got[0] != want {
				t.Fatalf("fold config CarryForwardUnchanged = %v, want [%v]", got, want)
			}
		})
	}
}

// A scheduled job that PANICS reaches the page: the guard flips the slot to
// failed with the panic value, the loop attributes it, and the copy it keeps
// survives a later manual job taking the slot, which is the case the history
// cannot cover (the guard writes no record, by design). A crash IS a
// failure, so the watcher would fall back; this fixture has no opt-in, so it
// records a skip instead. What this test pins is the kept failure.
func TestBackupScheduler_panickedJobStaysVisible(t *testing.T) {
	holdFold(t, func(context.Context, reconstruct.FullTableConfig) ([]*reconstruct.TableReport, []reconstruct.TableFailure, error) {
		panic("boom")
	})
	// No opt-in: a crash is a failure like any other and the watcher would
	// fall back, but a full backup cannot start here, so it records a skip;
	// the kept failure is what this test is about.
	b, reg, sup := newScheduleFixture(t, false)
	e := addScheduled(t, reg, true)
	fireAt(b, time.Date(2026, 8, 28, 9, 0, 5, 0, time.UTC))
	st := waitTerminal(t, b, e.ID)
	if st.Last.State != "failed" || !strings.Contains(st.Last.LastError, "boom") {
		t.Fatalf("panicked job = %+v, want failed with the panic value", st.Last)
	}
	// The page tells a crash from a refusal by this prefix (app.js
	// /^internal error/); the guard's wording is a wire contract.
	if !strings.HasPrefix(st.Last.LastError, "internal error: ") {
		t.Fatalf("panic error %q does not carry the internal error prefix the page keys on", st.Last.LastError)
	}
	if run, _ := sup.history.LastScheduled(e.ID); run != nil {
		t.Fatalf("the guard wrote a history record (%+v); this test assumes it does not, so the in-memory copy is the only evidence", run)
	}
	// The skip that stands in for the fallback names the crash as one. A
	// re-read: waitTerminal returns the snapshot from BEFORE the watcher's
	// last poll, and the skip is written by that poll.
	if st = b.ScheduleState(e.ID); !strings.Contains(st.LastSkipReason, "hit an internal error") || !strings.Contains(st.LastSkipReason, "not set to 1") {
		t.Fatalf("skip reason = %q, want the crash named and why no full backup ran", st.LastSkipReason)
	}
	// A later manual rebuild overwrites the slot. The schedule's outcome
	// must not vanish with it.
	sup.mu.Lock()
	sup.refreshes[e.ID] = &console.BaselineStatus{State: "running", Since: "2099-01-01T00:00:00Z"}
	sup.mu.Unlock()
	st = b.ScheduleState(e.ID)
	if st.Running || st.Last == nil || !strings.Contains(st.Last.LastError, "boom") {
		t.Fatalf("after a manual job took the slot: %+v, want the kept failure, not running", st)
	}
}

// The collision the issue names: another backup job holds the server at the
// scheduled time. The slot is skipped, not queued, and the skip is written
// to the history AND kept in memory so the page can show it either way.
func TestBackupScheduler_collisionSkipsAndRecords(t *testing.T) {
	b, reg, sup := newScheduleFixture(t, true)
	e := addScheduled(t, reg, false)
	sup.jobs[e.ID] = &console.BaselineStatus{State: "running", Since: "2026-08-28T08:30:00Z"}
	now := time.Date(2026, 8, 28, 9, 0, 5, 0, time.UTC)
	b.tick(context.Background(), now.Add(-time.Hour))
	b.tick(context.Background(), now)
	run, skip := sup.history.LastScheduled(e.ID)
	if run != nil {
		t.Fatalf("a run was recorded through a busy server: %+v", run)
	}
	if skip == nil || skip.SkipReason == "" {
		t.Fatalf("skip = %+v, want a skip with a reason", skip)
	}
	if skip.FinishedAt != now.Format(time.RFC3339) {
		t.Fatalf("skip stamped %s, want the slot's tick %s", skip.FinishedAt, now.Format(time.RFC3339))
	}
	st := b.ScheduleState(e.ID)
	if st.LastSkippedAt != skip.FinishedAt || st.LastSkipReason != skip.SkipReason {
		t.Fatalf("in-memory skip = %+v, want the same slot and reason as the history", st)
	}
	// The manual job that was already running is not reported as ours.
	if st.Running || st.Last != nil {
		t.Fatalf("a manual job in flight was reported as the schedule's: %+v", st)
	}
	// Without a history the skip is still on record in memory.
	sup.history = nil
	b.tick(context.Background(), now.Add(time.Hour))
	if st := b.ScheduleState(e.ID); st.LastSkippedAt != now.Add(time.Hour).Format(time.RFC3339) {
		t.Fatalf("with no history the skip was lost: %+v", st)
	}
}

// A schedule the daemon cannot serve at this slot is skipped with the reason
// the page already knows, never silently: no backup to rebuild from and no
// creation opt-in, or the supervisor's standing refusal (a lock-mode
// misconfiguration) on a server whose backups go to S3.
func TestBackupScheduler_skipsWithTheReasonWhenNothingCanRun(t *testing.T) {
	for _, tc := range []struct {
		name string
		opt  bool
		cfg  error
		s3   bool
		want string
	}{
		{"no backup yet, opt-in off", false, nil, false, "not set to 1"},
		{"S3 destination, lock mode misconfigured", true, errors.New("BINTRAIL_CONSOLE_BASELINE_LOCK_MODE: unknown mode \"lock-sometimes\""), true, "lock-sometimes"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b, reg, sup := newScheduleFixture(t, tc.opt)
			sup.configErr = tc.cfg
			e := addScheduled(t, reg, false)
			if tc.s3 {
				e.BaselineDir, e.BaselineS3 = "", "s3://bucket/backups/"
				if err := reg.Update(e); err != nil {
					t.Fatal(err)
				}
			}
			fireAt(b, time.Date(2026, 8, 28, 9, 0, 5, 0, time.UTC))
			if st := sup.Status(e.ID); st.State != "idle" {
				t.Fatalf("a dump was started: %+v", st)
			}
			_, skip := sup.history.LastScheduled(e.ID)
			if skip == nil || !strings.Contains(skip.SkipReason, tc.want) {
				t.Fatalf("skip = %+v, want a reason mentioning %q", skip, tc.want)
			}
		})
	}
}

// A removed schedule is forgotten (observation, warning, last run, last
// skip, last fallback), so re-adding one starts silent and reports nothing
// stale.
func TestBackupScheduler_removedScheduleIsForgotten(t *testing.T) {
	holdFold(t, func(context.Context, reconstruct.FullTableConfig) ([]*reconstruct.TableReport, []reconstruct.TableFailure, error) {
		return nil, nil, nil
	})
	b, reg, sup := newScheduleFixture(t, true)
	e := addScheduled(t, reg, true)
	t0 := time.Date(2026, 8, 28, 9, 0, 5, 0, time.UTC)
	fireAt(b, t0)
	waitTerminal(t, b, e.ID)
	b.skipped[e.ID] = scheduledSkip{at: "x", reason: "y"}
	b.fallback[e.ID] = scheduledFallback{at: "x", reason: "y"}
	b.warned[e.ID] = true
	e.BackupSchedule = nil
	if err := reg.Update(e); err != nil {
		t.Fatal(err)
	}
	b.tick(context.Background(), t0.Add(2*time.Hour))
	if _, ok := b.seen[e.ID]; ok {
		t.Fatal("a server with no schedule is still observed")
	}
	if st := b.ScheduleState(e.ID); st.Last != nil || st.LastStartedAt != "" || st.LastSkippedAt != "" || st.LastFallbackAt != "" {
		t.Fatalf("a removed schedule still reports state: %+v", st)
	}
	if b.warned[e.ID] {
		t.Fatal("the warn-once mark outlived the schedule")
	}
	// Re-added with the SAME identity, between ticks: still a first
	// observation, so the tick two hours on records and does not fire.
	e.BackupSchedule = &console.BackupSchedule{Every: "1h", At: "00:00"}
	if err := reg.Update(e); err != nil {
		t.Fatal(err)
	}
	before := sup.RefreshStatus(e.ID)
	b.tick(context.Background(), t0.Add(3*time.Hour))
	if after := sup.RefreshStatus(e.ID); after.Since != before.Since || sup.Status(e.ID).State != "idle" {
		t.Fatalf("a re-added schedule fired on first sight: %+v", after)
	}
}

// The history record a scheduled job writes carries the trigger, for both
// producers, and a manual one does not. Drives the supervisor half directly.
func TestScheduledRuns_stampTheTrigger(t *testing.T) {
	_, _, sup := newScheduleFixture(t, true)
	sup.jobs["d"] = &console.BaselineStatus{State: "running"}
	sup.run(console.BaselineRequest{ServerID: "d", ServerName: "d", SourceDSN: "not a dsn",
		Trigger: console.BaselineRunTriggerScheduled})
	if run, _ := sup.history.LastScheduled("d"); run == nil || run.Kind != console.BaselineRunDump || run.Error == "" {
		t.Fatalf("dump record = %+v, want a failed scheduled dump", run)
	}
	sup.jobs["m"] = &console.BaselineStatus{State: "running"}
	sup.run(console.BaselineRequest{ServerID: "m", ServerName: "m", SourceDSN: "not a dsn"})
	if run, _ := sup.history.LastScheduled("m"); run != nil {
		t.Fatalf("a manual dump was attributed to the schedule: %+v", run)
	}
	sup.refreshes["r"] = &console.BaselineStatus{State: "running"}
	sup.runRefresh(refreshRequest{ServerID: "r", ServerName: "r", IndexDSN: "d", BaselineDir: t.TempDir(),
		Trigger: console.BaselineRunTriggerScheduled}, time.Now().UTC(), time.Hour)
	if run, _ := sup.history.LastScheduled("r"); run == nil || run.Kind != console.BaselineRunRefresh {
		t.Fatalf("refresh record = %+v, want a scheduled refresh", run)
	}
}

// Attribution is by the exact Since the supervisor stamped on the job the
// loop started. A job of the same kind that began before it, or a manual one
// that took the slot after it finished, is not the schedule's, in any state;
// the schedule's own outcome, once observed, outlives the slot.
func TestBackupScheduler_attributesOnlyTheJobItStarted(t *testing.T) {
	b, _, sup := newScheduleFixture(t, true)
	b.started["a"] = scheduledStart{method: console.BackupMethodRefresh, at: "2026-08-28T09:00:00Z", since: "2026-08-28T09:00:00Z"}
	sup.refreshes["a"] = &console.BaselineStatus{State: "running", Since: "2026-08-28T09:00:00Z"}
	if st := b.ScheduleState("a"); !st.Running || st.Last == nil {
		t.Fatalf("the scheduled rebuild in flight is not reported: %+v", st)
	}
	sup.refreshes["a"].State = "failed"
	sup.refreshes["a"].LastError = "capture gap"
	if st := b.ScheduleState("a"); st.Running || st.Last == nil || st.Last.LastError != "capture gap" {
		t.Fatalf("the finished job's outcome did not reach the state: %+v", st)
	}
	sup.jobs["a"] = &console.BaselineStatus{State: "running", Since: "2026-08-28T09:00:00Z"}
	if st := b.ScheduleState("a"); st.Running || st.Last == nil || st.Last.LastError != "capture gap" {
		t.Fatalf("a manual dump changed the schedule's reported outcome: %+v", st)
	}
	sup.refreshes["a"] = &console.BaselineStatus{State: "running", Since: "2026-08-28T11:00:00Z"}
	if st := b.ScheduleState("a"); st.Running || st.Last == nil || st.Last.LastError != "capture gap" {
		t.Fatalf("a later manual job was reported as the schedule's, or erased its outcome: %+v", st)
	}
	b.started["b"] = scheduledStart{method: console.BackupMethodFull, at: "2026-08-28T10:00:00Z", since: "2026-08-28T10:00:00Z"}
	sup.jobs["b"] = &console.BaselineStatus{State: "running", Since: "2026-08-28T09:30:00Z"}
	if st := b.ScheduleState("b"); st.Running || st.Last != nil {
		t.Fatalf("a dump that began before the scheduled slot was reported as the schedule's: %+v", st)
	}
	sup.jobs["b"] = &console.BaselineStatus{State: "running", Since: "2026-08-28T10:00:00Z"}
	if st := b.ScheduleState("b"); !st.Running {
		t.Fatal("the scheduled dump in flight is not reported running")
	}
	if st := b.ScheduleState("never"); st.Running || st.LastStartedAt != "" || st.Last != nil {
		t.Fatal("an unknown server reported state")
	}
}

// Running after a REAL fire: the stamp the loop attributes by is read back
// from the supervisor, not reconstructed, so whatever format either side
// uses they agree. The full backup (no previous backup on disk) is held at
// the privilege-check seam so the job is observably in flight, then released
// and observed finished.
func TestBackupScheduler_runningAfterARealFire(t *testing.T) {
	b, reg, sup := newScheduleFixture(t, true)
	e := addScheduled(t, reg, false)
	e.SourceDSN = "src:pw@tcp(127.0.0.1:3306)/" // parseable, so the dump reaches the seam
	if err := reg.Update(e); err != nil {
		t.Fatal(err)
	}
	release := make(chan struct{})
	var releaseOnce sync.Once
	entered := make(chan struct{}, 1)
	prev := checkMydumperPrivileges
	checkMydumperPrivileges = func(ctx context.Context, dsn string, mode baseline.LockMode, remedy mydumperlock.Remedy, schemas []string) error {
		entered <- struct{}{}
		<-release
		return errors.New("held by the test")
	}
	// On every exit: let the job go, wait for it, THEN restore the seam.
	// Restoring while the goroutine may still read it is a data race.
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(release) })
		deadline := time.Now().Add(10 * time.Second)
		for b.ScheduleState(e.ID).Running && time.Now().Before(deadline) {
			time.Sleep(10 * time.Millisecond)
		}
		checkMydumperPrivileges = prev
	})
	fireAt(b, time.Date(2026, 8, 28, 9, 0, 5, 0, time.UTC))
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the scheduled dump never reached the privilege check")
	}
	if st := b.ScheduleState(e.ID); !st.Running || st.Last == nil || st.LastMethod != console.BackupMethodFull {
		t.Fatalf("in flight: ScheduleState = %+v, want a running full backup attributed", st)
	}
	releaseOnce.Do(func() { close(release) })
	st := waitTerminal(t, b, e.ID)
	if st.Last.State != "failed" {
		t.Fatalf("finished: ScheduleState = %+v, want failed", st)
	}
	run, _ := sup.history.LastScheduled(e.ID)
	if run == nil || !strings.Contains(run.Error, "held by the test") {
		t.Fatalf("record = %+v, want the seam's error", run)
	}
}

// A panic inside a tick must not reach the daemon that is also capturing.
func TestBackupScheduler_tickSurvivesAPanic(t *testing.T) {
	b, reg, _ := newScheduleFixture(t, true)
	addScheduled(t, reg, false)
	b.seen = nil // a nil map write panics inside crossed
	b.tick(context.Background(), time.Now().UTC())
}

// An unreadable schedule is reported once per server at Warn, not every
// minute at Debug, and again after it is fixed and broken again.
func TestBackupScheduler_unreadableScheduleWarnsOnce(t *testing.T) {
	prev := slog.Default()
	t.Cleanup(func() { slog.SetDefault(prev) })
	var buf bytes.Buffer
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))

	b, reg, _ := newScheduleFixture(t, true)
	e := addScheduled(t, reg, false)
	e.BackupSchedule = &console.BackupSchedule{Every: "soon"}
	if err := reg.Update(e); err != nil {
		t.Fatal(err)
	}
	t0 := time.Now().UTC()
	b.tick(context.Background(), t0)
	b.tick(context.Background(), t0.Add(time.Minute))
	b.tick(context.Background(), t0.Add(2*time.Minute))
	if n := strings.Count(buf.String(), "cannot be read"); n != 1 {
		t.Fatalf("warned %d times over three ticks, want once: %s", n, buf.String())
	}
	e.BackupSchedule = &console.BackupSchedule{Every: "1h"}
	if err := reg.Update(e); err != nil {
		t.Fatal(err)
	}
	b.tick(context.Background(), t0.Add(3*time.Minute))
	e.BackupSchedule = &console.BackupSchedule{Every: "later"}
	if err := reg.Update(e); err != nil {
		t.Fatal(err)
	}
	b.tick(context.Background(), t0.Add(4*time.Minute))
	if n := strings.Count(buf.String(), "cannot be read"); n != 2 {
		t.Fatalf("a schedule fixed and broken again was not reported again: %d warnings", n)
	}
}

// The watch wiring: a nil supervisor yields a nil INTERFACE, not a typed nil
// the console would take for a running loop and dereference on the first
// listing with a schedule.
func TestNewBackupScheduleReporter_nilSupervisorIsANilInterface(t *testing.T) {
	reg, _ := console.LoadRegistry("")
	rep, sched := newBackupScheduleReporter(nil, reg, true, false)
	if rep != nil || sched != nil {
		t.Fatalf("nil supervisor produced a reporter (%v, %v); the console would advertise a loop that does not exist", rep, sched)
	}
	sup := newBaselineSupervisor(context.Background(), t.TempDir(), baseline.DefaultLockMode)
	rep, sched = newBackupScheduleReporter(sup, reg, true, false)
	if rep == nil || sched == nil || rep.(*backupScheduler) != sched {
		t.Fatal("a supervisor did not produce one scheduler behind both values")
	}
	if enabled, refusal := rep.FullBackups(); !enabled || refusal != nil {
		t.Fatalf("FullBackups = (%v, %v), want the opt-in carried through", enabled, refusal)
	}
	rep, sched = newBackupScheduleReporter(sup, reg, false, true)
	if enabled, _ := rep.FullBackups(); enabled || !sched.carryDefault {
		t.Fatal("the two flags were not carried through in order")
	}
}

// The failure this whole PR must not create: an S3 permission error answered
// with a full lock-and-read of production.
//
// The scheduled watcher takes a full backup whenever an update fails, on the
// reasoning that the update produced nothing so a backup is still owed. An
// upload failure breaks that reasoning in both halves: the update DID produce
// a snapshot, and the full backup that would stand in has to clear the very
// upload gate that just refused. So one throttled PutObject would cost a full
// read of the source and publish nothing new (#1539).
//
// Driven at the watcher rather than through a whole scheduled slot on purpose:
// the producer DECISION reads a seam in internal/console and the fold reads one
// in consoleapp, so no test in either package can stub both. What has to hold
// is one thing anyway, and it is here: the watcher must consult Published, not
// State, and both statuses below are ones runRefresh really produces
// (applyFoldStatus sets Published from foldPublished on both branches).
func TestWatchScheduled_fallsBackOnlyWhenNothingWasPublished(t *testing.T) {
	for _, tc := range []struct {
		name         string
		last         console.BaselineStatus
		wantFallback bool
	}{
		// The fold refused: nothing exists, a backup is still owed.
		{"a refused fold", console.BaselineStatus{State: "failed", LastError: "capture gap"}, true},
		// The fold finished and only the upload failed: the snapshot exists,
		// and a full backup would have to clear the same gate.
		{"only the upload failed", console.BaselineStatus{State: "failed", Published: true,
			LastError: errSnapshotNotUploaded.Error() + ": AccessDenied"}, false},
		{"a clean run", console.BaselineStatus{State: "succeeded", Published: true}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b, reg, sup := newScheduleFixture(t, true)
			e := addScheduled(t, reg, true)
			e.SourceDSN = "not a dsn" // any fallback full backup fails fast
			if err := reg.Update(e); err != nil {
				t.Fatal(err)
			}
			// Stage the terminal status the watcher will read back, under the
			// Since the loop recorded, which is how ScheduleState decides the
			// job in the slot is the one it started.
			since := "2026-08-28T09:00:05Z"
			st := tc.last
			st.Since = since
			sup.mu.Lock()
			sup.refreshes[e.ID] = &st
			sup.mu.Unlock()
			b.mu.Lock()
			b.started[e.ID] = scheduledStart{method: console.BackupMethodRefresh, at: since, since: since}
			// fallBack refuses for a schedule this loop has not observed, so
			// the refused-fold case would take no fallback for a reason that
			// has nothing to do with what is under test.
			b.seen[e.ID] = seenSlot{identity: e.BackupSchedule.Identity()}
			b.mu.Unlock()

			b.watchScheduled(e, since, console.BackupMethodRefresh)
			b.watchers.Wait()

			got := b.ScheduleState(e.ID).LastFallbackAt != ""
			if got != tc.wantFallback {
				t.Fatalf("fallback taken = %v, want %v (status %+v)", got, tc.wantFallback, tc.last)
			}
		})
	}
}

// The mirror, so the fix above cannot be "never fall back": a fold that
// genuinely published nothing still owes a full backup.
func TestBackupScheduler_aRefusedFoldStillFallsBack(t *testing.T) {
	holdFold(t, func(context.Context, reconstruct.FullTableConfig) ([]*reconstruct.TableReport, []reconstruct.TableFailure, error) {
		return nil, nil, errors.New("capture gap in the reconstruction window")
	})
	b, reg, _ := newScheduleFixture(t, true)
	e := addScheduled(t, reg, true)
	e.SourceDSN = "not a dsn" // the fallback full backup fails fast
	if err := reg.Update(e); err != nil {
		t.Fatal(err)
	}
	fireAt(b, time.Date(2026, 8, 28, 9, 0, 5, 0, time.UTC))
	st := waitTerminalMethod(t, b, e.ID, console.BackupMethodFull)
	if st.LastFallbackAt == "" {
		t.Fatalf("a fold that published nothing took no fallback: %+v", st)
	}
}

// The passthrough that joins the two halves: the producer DECISION resolves
// the fold source in internal/console, and the fold reads it from the request
// the schedule builds here. Deleting `BaselineS3: e.BaselineS3` in startRebuild
// passed the entire suite, and the result is not a small regression: the
// decision still says "update, the previous backup is in the bucket" while the
// fold looks in the empty local directory, refuses, and the watcher answers
// that with a full lock-and-read of production. Every slot, silently.
func TestStartRebuild_carriesTheDestinationIntoTheFold(t *testing.T) {
	realList := newestSnapshotTables
	t.Cleanup(func() { newestSnapshotTables = realList })
	stubBucketListing(t)
	newestSnapshotTables = func(ctx context.Context, src string) ([]string, error) {
		if !strings.HasPrefix(src, "s3://") {
			return realList(ctx, src)
		}
		return []string{"shop.orders"}, nil
	}
	got := make(chan string, 1)
	holdFold(t, func(_ context.Context, cfg reconstruct.FullTableConfig) ([]*reconstruct.TableReport, []reconstruct.TableFailure, error) {
		select {
		case got <- cfg.BaselineSrc:
		default:
		}
		return nil, nil, errors.New("stop here; the source is what this pins")
	})

	b, reg, _ := newScheduleFixture(t, true)
	e := addScheduled(t, reg, true)
	e.BaselineS3 = "s3://bucket/backups/"
	if err := reg.Update(e); err != nil {
		t.Fatal(err)
	}
	p, err := e.BackupSchedule.Parse()
	if err != nil {
		t.Fatal(err)
	}
	if err := b.startRebuild(e, p, "2026-08-28T09:00:00Z"); err != nil {
		t.Fatalf("startRebuild: %v", err)
	}
	waitTerminalMethod(t, b, e.ID, console.BackupMethodRefresh)

	select {
	case src := <-got:
		if src != "s3://bucket/backups/" {
			t.Fatalf("the fold read %q, want the bucket: the destination never reached it", src)
		}
	default:
		t.Fatal("the fold never ran, so nothing pins where it would have read")
	}
}
