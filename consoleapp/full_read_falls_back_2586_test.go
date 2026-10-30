package consoleapp

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// #2586: a full read the cut-over chose over an update, which then fails
// (mydumper refuses while the source holds a statement older than its
// guard), is followed by the update at the same slot instead of being
// chosen again at every slot while the changes behind it keep growing.

// Which failed full reads an update stands in for: only one the cut-over
// chose over a possible update, that published nothing and was not refused
// on the disk or the destination the update writes to as well.
func TestUpdateStandsIn(t *testing.T) {
	measured := "the measured window: 17,000,000 events since the previous snapshot would take about 71m to apply"
	measured = console.BackupWhyWindowPrefix + ": " + measured[len("the measured window: "):]
	failed := console.BaselineStatus{State: "failed", LastError: "mydumper: queries running longer than 60s"}
	full := scheduledStart{method: console.BackupMethodFull, why: measured}
	aged := scheduledStart{method: console.BackupMethodFull, why: console.BackupWhyStaleAnchorPrefix + ": it is 49h old and the cut-over is 48h; the full read costs less"}
	// Nothing indexed may mean the capture stopped: only a read of the
	// source settles that, an update over the same index cannot.
	doubted := scheduledStart{method: console.BackupMethodFull, why: console.BackupWhyStaleAnchorPrefix + ": it is 49h old and the cut-over is 48h; " + console.BackupWhyNothingIndexedWords + ", the source could not be checked"}
	for _, s := range []scheduledStart{aged, doubted} {
		if console.BackupWhyCode(s.why) != "window_age" {
			t.Fatalf("the fixture's reason codes as %q", console.BackupWhyCode(s.why))
		}
	}
	if console.BackupWhyCode(full.why) != "window_measured" {
		t.Fatalf("the fixture's reason codes as %q", console.BackupWhyCode(full.why))
	}
	cases := []struct {
		name string
		own  scheduledStart
		last console.BaselineStatus
		want bool
	}{
		{"chosen on the measured cost, failed", full, failed, true},
		{"succeeded", full, console.BaselineStatus{State: "succeeded"}, false},
		{"an update", scheduledStart{method: console.BackupMethodRefresh}, failed, false},
		{"the first backup", scheduledStart{method: console.BackupMethodFull, why: console.BackupWhyFirstBackup}, failed, false},
		{"no reason", scheduledStart{method: console.BackupMethodFull}, failed, false},
		{"standing in for a failed update", scheduledStart{method: console.BackupMethodFull, why: measured, fallback: true}, failed, false},
		{"a full copy the timetable asked for", scheduledStart{method: console.BackupMethodFull, why: measured, fullCopy: true}, failed, false},
		{"published, its upload failed", full, console.BaselineStatus{State: "failed", Published: true}, false},
		{"refused on the disk", full, console.BaselineStatus{State: "failed", DiskRefused: true}, false},
		{"another writer's destination", full, console.BaselineStatus{State: "failed", ForeignSource: true}, false},
		{"chosen on the snapshot's age", aged, failed, true},
		{"chosen because nothing was indexed", doubted, failed, false},
	}
	for _, c := range cases {
		if got := updateStandsIn(c.own, c.last); got != c.want {
			t.Errorf("%s: %v, want %v", c.name, got, c.want)
		}
	}
}

// cutOverToFull sets the server's history and index so the slot's cut-over
// chooses a full read on the measured cost (as in
// TestBackupScheduler_cutsOverToAFullBackupOnTheMeasuredWindow); its source
// does not parse, so the full read fails at once.
func cutOverToFull(t *testing.T, b *backupScheduler, reg *console.Registry, sup *baselineSupervisor) console.ServerEntry {
	t.Helper()
	b.window = b.measureWindow
	e := addScheduled(t, reg, true)
	e.SourceDSN = "not a dsn"
	if err := reg.Update(e); err != nil {
		t.Fatal(err)
	}
	for _, rec := range []console.BaselineRunRecord{
		{ServerID: e.ID, Kind: console.BaselineRunDump, StartedAt: "2026-08-20T05:00:00Z", FinishedAt: "2026-08-20T05:08:00Z"},
		{ServerID: e.ID, Kind: console.BaselineRunRefresh, Events: 1000, UpdateSeconds: 60},
		{ServerID: e.ID, Kind: console.BaselineRunRefresh, Events: 241_000, UpdateSeconds: 120},
	} {
		if err := sup.history.Append(rec); err != nil {
			t.Fatal(err)
		}
	}
	sup.foldedMarks[e.ID] = foldMemo{mark: indexMark{events: 1000}, publishedAt: snapshotAnchor, indexDSN: e.DSN}
	mark := indexMark{events: 17_001_000}
	stubIndexMark(t, &mark, true)
	stubCoverage(t, true, true)
	return e
}

func runsOf(sup *baselineSupervisor, id, kind string) int {
	n := 0
	for _, r := range sup.history.List(id) {
		if r.Kind == kind && r.SkipReason == "" && r.FinishedAt >= "2026-08-21" {
			n++
		}
	}
	return n
}

// The full read fails, the update runs at the same slot and goes through,
// and nothing else is started.
func TestBackupScheduler_failedFullReadIsFollowedByAnUpdate(t *testing.T) {
	folds := 0
	holdFold(t, func(context.Context, reconstruct.FullTableConfig) ([]*reconstruct.TableReport, []reconstruct.TableFailure, error) {
		folds++
		return nil, nil, nil
	})
	b, reg, sup := newScheduleFixture(t, true)
	e := cutOverToFull(t, b, reg, sup)
	logs := captureSlogFor(t)
	fireAt(b, time.Date(2026, 8, 21, 13, 0, 5, 0, time.UTC))
	st := waitTerminalMethod(t, b, e.ID, console.BackupMethodRefresh)
	if st.Last.State != "succeeded" || folds != 1 {
		t.Fatalf("the update did not stand in: %+v (folds %d)", st, folds)
	}
	if !strings.Contains(logs.String(), "the full read failed; updating from the recorded changes instead") {
		t.Fatalf("the stand-in was not logged: %q", logs.String())
	}
	if sup.Status(e.ID).State != "failed" {
		t.Fatalf("the full read did not run first: %+v", sup.Status(e.ID))
	}
	// The update that went through is not an alarm about updates.
	if st.LastFallbackAt != "" {
		t.Fatalf("the stand-in was recorded as the opposite fallback: %+v", st)
	}
	b.watchers.Wait()
	if st := b.ScheduleState(e.ID); st.LastMethod != console.BackupMethodRefresh {
		t.Fatalf("something was started after the update: %+v", st)
	}
}

// The update standing in fails too: one full read, one update, and a skip
// that says both; no full read is started in the update's place.
func TestBackupScheduler_failedStandInUpdateStartsNoFullRead(t *testing.T) {
	holdFold(t, func(context.Context, reconstruct.FullTableConfig) ([]*reconstruct.TableReport, []reconstruct.TableFailure, error) {
		return nil, nil, errors.New("capture gap in the reconstruction window")
	})
	b, reg, sup := newScheduleFixture(t, true)
	e := cutOverToFull(t, b, reg, sup)
	logs := captureSlogFor(t)
	fireAt(b, time.Date(2026, 8, 21, 13, 0, 5, 0, time.UTC))
	waitTerminalMethod(t, b, e.ID, console.BackupMethodRefresh)
	b.watchers.Wait()
	st := b.ScheduleState(e.ID)
	if st.LastMethod != console.BackupMethodRefresh {
		t.Fatalf("a full read was started after the stand-in failed: %+v", st)
	}
	if !strings.Contains(logs.String(), "failed too") || !strings.Contains(logs.String(), "capture gap") {
		t.Fatalf("the log does not name the update's failure after the full read's: %q", logs.String())
	}
	// Both runs carry their own errors in the history: no skip record per
	// slot on top of them.
	for _, r := range sup.history.List(e.ID) {
		if r.SkipReason != "" {
			t.Fatalf("a skip was recorded for a slot that started twice: %+v", r)
		}
	}
	if st.LastFallbackAt != "" {
		t.Fatalf("the failed stand-in started the opposite fallback: %+v", st)
	}
	if n := runsOf(sup, e.ID, console.BaselineRunDump); n != 1 {
		t.Fatalf("%d full reads recorded, want 1", n)
	}
}

// A first backup that fails has nothing to update: no update is tried.
func TestBackupScheduler_failedFirstBackupTakesNoUpdate(t *testing.T) {
	folds := 0
	holdFold(t, func(context.Context, reconstruct.FullTableConfig) ([]*reconstruct.TableReport, []reconstruct.TableFailure, error) {
		folds++
		return nil, nil, nil
	})
	b, reg, _ := newScheduleFixture(t, true)
	e := addScheduled(t, reg, false) // no previous snapshot, a source that does not parse
	fireAt(b, time.Date(2026, 8, 28, 9, 0, 5, 0, time.UTC))
	waitTerminalMethod(t, b, e.ID, console.BackupMethodFull)
	b.watchers.Wait()
	if st := b.ScheduleState(e.ID); st.LastMethod != console.BackupMethodFull || folds != 0 {
		t.Fatalf("a failed first backup was followed by an update: %+v (folds %d)", st, folds)
	}
}

// The schedule changed while the full read ran: no update for a schedule
// that is not the one that chose it.
func TestUpdateInstead_scheduleChangedMeanwhile(t *testing.T) {
	folds := 0
	holdFold(t, func(context.Context, reconstruct.FullTableConfig) ([]*reconstruct.TableReport, []reconstruct.TableFailure, error) {
		folds++
		return nil, nil, nil
	})
	b, reg, _ := newScheduleFixture(t, true)
	e := addScheduled(t, reg, true)
	b.mu.Lock()
	b.seen[e.ID] = seenSlot{}
	b.mu.Unlock()
	changed := e
	changed.BackupSchedule = &console.BackupSchedule{Every: "2h", At: "00:00"}
	if err := reg.Update(changed); err != nil {
		t.Fatal(err)
	}
	b.updateInstead(e, console.BaselineStatus{State: "failed", LastError: "x"})
	b.watchers.Wait()
	if st := b.ScheduleState(e.ID); st.LastStartedAt != "" || folds != 0 {
		t.Fatalf("an update was started for a changed schedule: %+v", st)
	}
}

// The update that stood in left out tables created since the previous
// snapshot: the promise of a full read for them is withdrawn, since that is
// the read that just failed, and none is started.
func TestStandInNewTables_withdrawsTheFullReadPromise(t *testing.T) {
	b, _, sup, e := newTablesFixture(t, true, []string{"shop.kept"})
	b.mu.Lock()
	b.newTablesPending[e.ID] = newTablesTry{names: map[string]bool{"shop.orders": true}}
	b.mu.Unlock()
	done := console.BaselineStatus{State: "succeeded", NewTables: []string{"shop.orders"}, NewTablesAction: console.NewTablesActionFullRead}
	sup.mu.Lock()
	cp := done
	sup.refreshes[e.ID] = &cp
	sup.mu.Unlock()
	b.standInNewTables(e, done)
	if got := sup.RefreshStatus(e.ID).NewTablesAction; got != "" {
		t.Fatalf("the status still promises a full read: %q", got)
	}
	b.mu.Lock()
	_, pending := b.newTablesPending[e.ID]
	b.mu.Unlock()
	if pending {
		t.Fatal("the pending promise was kept")
	}
	b.watchers.Wait()
	if st := b.ScheduleState(e.ID); st.LastStartedAt != "" {
		t.Fatalf("a full read was started for the new tables: %+v", st)
	}
}
