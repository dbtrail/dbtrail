package consoleapp

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// #2006 / #2007: an update refused for a reason a full read cannot cure (a
// table the snapshot holds under a name the index does not know, a table
// shape the update cannot fold) used to drive a full read of the source at
// every slot, forever. One full read is tried; when the update right after
// it is refused again for the same tables, no further full read is taken.

// refuseWith makes every update refuse the tables refuse() names, with err.
func refuseWith(t *testing.T, refuse func() []reconstruct.TableFailure) {
	t.Helper()
	realList := newestSnapshotTables
	t.Cleanup(func() { newestSnapshotTables = realList })
	newestSnapshotTables = func(context.Context, string) (time.Time, []string, error) {
		return refreshAt.Add(-time.Hour), []string{"demo.mydumper_0", "demo.mydumper_1", "demo.orders", "demo.plain"}, nil
	}
	holdFold(t, func(context.Context, reconstruct.FullTableConfig) ([]*reconstruct.TableReport, []reconstruct.TableFailure, error) {
		failed := refuse()
		if len(failed) == 0 {
			return []*reconstruct.TableReport{{Schema: "demo", Table: "plain"}}, nil, nil
		}
		var errs []error
		for _, f := range failed {
			errs = append(errs, f.Err)
		}
		return nil, failed, errors.Join(errs...)
	})
}

// fullReadsSucceed makes every full read publish a snapshot without a
// mydumper, counting them; fail makes the next ones fail instead.
type fakeFullReads struct {
	mu    sync.Mutex
	n     int
	fail  bool
	clock time.Time
}

func (f *fakeFullReads) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.n
}

func stubFullReads(t *testing.T, sup *baselineSupervisor) *fakeFullReads {
	t.Helper()
	mark := indexMark{}
	stubIndexMark(t, &mark, false)
	f := &fakeFullReads{clock: time.Date(2026, 8, 28, 9, 30, 0, 0, time.UTC)}
	sup.produce = func(req console.BaselineRequest) (dumpOutcome, error) {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.n++
		if f.fail {
			return dumpOutcome{}, errors.New("dump: mydumper exit 2")
		}
		f.clock = f.clock.Add(time.Minute)
		return dumpOutcomeAt(t, req.LocalDir, f.clock), nil
	}
	return f
}

func notFound(table string) reconstruct.TableFailure {
	return reconstruct.TableFailure{Schema: "demo", Table: table,
		Err: fmt.Errorf("full-table reconstruct failed: resolve schema for demo.%s: table demo.%s not found in snapshot 8; consider re-running `bintrail snapshot`", table, table)}
}

func schemaChanged(table string) reconstruct.TableFailure {
	return reconstruct.TableFailure{Schema: "demo", Table: table, Err: fmt.Errorf("demo.%s: %w", table, reconstruct.ErrSchemaChanged)}
}

// slotAt fires the slot at hour h and waits for everything it started.
func slotAt(t *testing.T, b *backupScheduler, id string, h int) console.BackupScheduleState {
	t.Helper()
	b.tick(context.Background(), time.Date(2026, 8, 28, h, 0, 5, 0, time.UTC))
	st := waitTerminal(t, b, id)
	b.watchers.Wait()
	// A fallback full read started by the watcher: wait for it too.
	st = b.ScheduleState(id)
	for deadline := time.Now().Add(10 * time.Second); st.Running && time.Now().Before(deadline); {
		time.Sleep(10 * time.Millisecond)
		st = b.ScheduleState(id)
	}
	b.watchers.Wait()
	return b.ScheduleState(id)
}

func stuckRecords(sup *baselineSupervisor, id string) []console.BaselineRunRecord {
	var out []console.BaselineRunRecord
	for _, r := range sup.history.List(id) {
		if strings.Contains(r.SkipReason, "a full read does not fix") {
			out = append(out, r)
		}
	}
	return out
}

func TestBackupScheduler_aRefusalAFullReadCannotCureStopsTheFullReads_2006(t *testing.T) {
	refuseWith(t, func() []reconstruct.TableFailure { return []reconstruct.TableFailure{notFound("mydumper_0")} })
	b, reg, sup := newScheduleFixture(t, true)
	reads := stubFullReads(t, sup)
	e := addScheduled(t, reg, true)

	// First slot after the save is only observed; the next one fires.
	b.tick(context.Background(), time.Date(2026, 8, 28, 8, 0, 5, 0, time.UTC))
	st := slotAt(t, b, e.ID, 9)
	if reads.count() != 1 || st.LastFallbackAt == "" || st.LastFallbackStoppedAt != "" {
		t.Fatalf("the first refusal must fall back once, as before: reads %d, state %+v", reads.count(), st)
	}

	// The update right after that full read is refused the same way: a full
	// read did not fix it, so none is taken in its place.
	st = slotAt(t, b, e.ID, 10)
	if reads.count() != 1 {
		t.Fatalf("a second full read was taken for a refusal the first one did not cure (%d reads)", reads.count())
	}
	if st.LastMethod != console.BackupMethodRefresh || st.LastFallbackStoppedAt == "" {
		t.Fatalf("state = %+v, want the update on record and the stop said", st)
	}
	if len(st.LastFallbackRefusedTables) != 1 || st.LastFallbackRefusedTables[0].Name != "demo.mydumper_0" {
		t.Fatalf("the stop does not name the table: %+v", st.LastFallbackRefusedTables)
	}
	recs := stuckRecords(sup, e.ID)
	if len(recs) != 1 {
		t.Fatalf("history holds %d records of the stop, want 1", len(recs))
	}
	if r := recs[0].SkipReason; !strings.Contains(r, "1 table") || strings.Contains(r, "mydumper_0") || strings.Contains(r, "bintrail snapshot") || strings.Contains(r, "\u2014") {
		t.Errorf("the recorded reason = %q: it must count the table without naming it (a data profile may hide names), with no command and no em dash", r)
	}

	// And it stays stopped, said once, while the refusal lasts.
	st = slotAt(t, b, e.ID, 11)
	if reads.count() != 1 || st.LastFallbackStoppedAt == "" || len(stuckRecords(sup, e.ID)) != 1 {
		t.Fatalf("third slot: reads %d, state %+v, records %d", reads.count(), st, len(stuckRecords(sup, e.ID)))
	}

	// An update that goes through ends it.
	refuseWith(t, func() []reconstruct.TableFailure { return nil })
	st = slotAt(t, b, e.ID, 12)
	if st.LastFallbackAt != "" || st.LastFallbackStoppedAt != "" {
		t.Fatalf("a published update did not end the alarm: %+v", st)
	}
}

// A schema change IS cured by a full read: each one that stops an update
// still gets its full read, also right after another.
func TestBackupScheduler_aSchemaChangeStillFallsBackEveryTime_2006(t *testing.T) {
	noEmergencyCap(t) // this test is about another rule; the daily cap has its own (#2006)
	refuseWith(t, func() []reconstruct.TableFailure { return []reconstruct.TableFailure{schemaChanged("orders")} })
	b, reg, sup := newScheduleFixture(t, true)
	reads := stubFullReads(t, sup)
	e := addScheduled(t, reg, true)
	b.tick(context.Background(), time.Date(2026, 8, 28, 8, 0, 5, 0, time.UTC))
	slotAt(t, b, e.ID, 9)
	st := slotAt(t, b, e.ID, 10)
	if reads.count() != 2 || st.LastFallbackStoppedAt != "" || len(stuckRecords(sup, e.ID)) != 0 {
		t.Fatalf("a schema change after a full read must fall back again: reads %d, state %+v", reads.count(), st)
	}
}

// A new table in the refused list is a new reason: it gets its full read.
func TestBackupScheduler_aNewRefusedTableFallsBackOnceMore_2006(t *testing.T) {
	noEmergencyCap(t) // this test is about another rule; the daily cap has its own (#2006)
	refused := []reconstruct.TableFailure{notFound("mydumper_0")}
	var mu sync.Mutex
	refuseWith(t, func() []reconstruct.TableFailure { mu.Lock(); defer mu.Unlock(); return refused })
	b, reg, sup := newScheduleFixture(t, true)
	reads := stubFullReads(t, sup)
	e := addScheduled(t, reg, true)
	b.tick(context.Background(), time.Date(2026, 8, 28, 8, 0, 5, 0, time.UTC))
	slotAt(t, b, e.ID, 9)
	if st := slotAt(t, b, e.ID, 10); reads.count() != 1 || st.LastFallbackStoppedAt == "" {
		t.Fatalf("setup: reads %d, state %+v", reads.count(), st)
	}
	mu.Lock()
	refused = []reconstruct.TableFailure{notFound("mydumper_0"), notFound("mydumper_1")}
	mu.Unlock()
	st := slotAt(t, b, e.ID, 11)
	if reads.count() != 2 || st.LastFallbackStoppedAt != "" {
		t.Fatalf("a new refused table must get a full read: reads %d, state %+v", reads.count(), st)
	}
	// Then the same pair again: stopped, and said a second time.
	st = slotAt(t, b, e.ID, 12)
	if reads.count() != 2 || st.LastFallbackStoppedAt == "" || len(stuckRecords(sup, e.ID)) != 2 {
		t.Fatalf("reads %d, state %+v, records %d", reads.count(), st, len(stuckRecords(sup, e.ID)))
	}
}

// A full read that failed proves nothing about the refusal: the next one
// falls back again, as before.
func TestBackupScheduler_aFailedFullReadDoesNotStopTheFallback_2006(t *testing.T) {
	noEmergencyCap(t) // this test is about another rule; the daily cap has its own (#2006)
	refuseWith(t, func() []reconstruct.TableFailure { return []reconstruct.TableFailure{notFound("mydumper_0")} })
	b, reg, sup := newScheduleFixture(t, true)
	reads := stubFullReads(t, sup)
	reads.fail = true
	e := addScheduled(t, reg, true)
	b.tick(context.Background(), time.Date(2026, 8, 28, 8, 0, 5, 0, time.UTC))
	slotAt(t, b, e.ID, 9)
	st := slotAt(t, b, e.ID, 10)
	if reads.count() != 2 || st.LastFallbackStoppedAt != "" {
		t.Fatalf("after a failed full read the next refusal must fall back: reads %d, state %+v", reads.count(), st)
	}
}

// The matching rule on its own.
func TestSameIncurableRefusal_2006(t *testing.T) {
	r := func(name, verdict string) console.RefusedTable {
		return console.RefusedTable{Name: name, Verdict: verdict, Reason: "why"}
	}
	prev := []console.RefusedTable{r("demo.a", "refused"), r("demo.b", "refused")}
	for _, c := range []struct {
		name      string
		cur       []console.RefusedTable
		prevLeft  int
		curLeft   int
		wantStuck bool
	}{
		{"same", prev, 0, 0, true},
		{"fewer: the rest was cured", []console.RefusedTable{r("demo.a", "refused")}, 0, 0, true},
		{"a new table", []console.RefusedTable{r("demo.a", "refused"), r("demo.c", "refused")}, 0, 0, false},
		{"same table, other verdict", []console.RefusedTable{r("demo.a", "refused-gap")}, 0, 0, false},
		{"schema change", []console.RefusedTable{r("demo.a", "refused-ddl")}, 0, 0, false},
		{"same verdict, new reason", []console.RefusedTable{{Name: "demo.a", Verdict: "refused", Reason: "a primary-key change at 10:05"}}, 0, 0, false},
		{"empty list", nil, 0, 0, false},
		{"list cut short now", prev, 0, 3, false},
		{"list cut short before", prev, 3, 0, false},
	} {
		if got := sameIncurableRefusal(prev, c.prevLeft, c.cur, c.curLeft); got != c.wantStuck {
			t.Errorf("%s: stuck = %v, want %v", c.name, got, c.wantStuck)
		}
	}
	gapBefore := []console.RefusedTable{r("demo.a", "refused-gap")}
	if sameIncurableRefusal(gapBefore, 0, gapBefore, 0) {
		t.Error("a capture gap after a full read is a new gap: it must fall back")
	}
}

// A stop lasts a day; then one full read is tried again.
func TestBackupScheduler_aStopRetriesAfterADay_2006(t *testing.T) {
	noEmergencyCap(t) // this test is about another rule; the daily cap has its own (#2006)
	refuseWith(t, func() []reconstruct.TableFailure { return []reconstruct.TableFailure{notFound("mydumper_0")} })
	b, reg, sup := newScheduleFixture(t, true)
	reads := stubFullReads(t, sup)
	e := addScheduled(t, reg, true)
	b.tick(context.Background(), time.Date(2026, 8, 28, 8, 0, 5, 0, time.UTC))
	slotAt(t, b, e.ID, 9)
	if st := slotAt(t, b, e.ID, 10); reads.count() != 1 || st.LastFallbackStoppedAt == "" {
		t.Fatalf("setup: reads %d, state %+v", reads.count(), st)
	}
	b.mu.Lock()
	fb := b.fallback[e.ID]
	fb.stoppedAt = time.Now().Add(-stuckRetryEvery - time.Minute).UTC().Format(time.RFC3339)
	b.fallback[e.ID] = fb
	b.mu.Unlock()
	st := slotAt(t, b, e.ID, 11)
	if reads.count() != 2 || st.LastFallbackStoppedAt != "" {
		t.Fatalf("a day into a stop, one full read must be tried: reads %d, state %+v", reads.count(), st)
	}
}

// noEmergencyCap turns the daily cap off for a test of another rule.
func noEmergencyCap(t *testing.T) {
	t.Helper()
	prev := emergencyCap
	emergencyCap = 0
	t.Cleanup(func() { emergencyCap = prev })
}

// The daily cap (#2006): whatever the reason, the schedule reads a server in
// full on its own at most once a day. A schema change IS cured by a full
// read, but a second one within the day waits, and the card says until when.
func TestBackupScheduler_emergencyCapHoldsBackASecondFallback_2006(t *testing.T) {
	refuseWith(t, func() []reconstruct.TableFailure { return []reconstruct.TableFailure{schemaChanged("orders")} })
	b, reg, sup := newScheduleFixture(t, true)
	reads := stubFullReads(t, sup)
	e := addScheduled(t, reg, true)
	b.tick(context.Background(), time.Date(2026, 8, 28, 8, 0, 5, 0, time.UTC))
	slotAt(t, b, e.ID, 9)
	st := slotAt(t, b, e.ID, 10)
	if reads.count() != 1 {
		t.Fatalf("a second emergency full read within the day: %d reads", reads.count())
	}
	if !strings.Contains(st.LastSkipReason, "at most once a day") || !strings.Contains(st.LastSkipReason, "the next one is allowed after") {
		t.Fatalf("the card does not say the cap held it back: %q", st.LastSkipReason)
	}
	// Survives a restart: a new loop reads the cap from the run history.
	b2 := newBackupScheduler(sup, reg, true, false)
	if _, held := b2.emergencyHeld(e.ID, time.Now().UTC()); !held {
		t.Fatal("after a restart the cap forgot the emergency read in the run history")
	}
}

// What counts against the cap: scheduled full reads the schedule took on its
// own (a fallback, new tables), failed ones too. A full read the operator
// asked for (manual, the full-copy timetable) or the schedule's normal choice
// (the first read) neither counts nor waits.
func TestEmergencyHeld_whatCounts_2006(t *testing.T) {
	now := time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC)
	for _, c := range []struct {
		name string
		rec  console.BaselineRunRecord
		held bool
	}{
		{"fallback", console.BaselineRunRecord{Trigger: console.BaselineRunTriggerScheduled, WhyCode: "fold_refused"}, true},
		{"failed fallback", console.BaselineRunRecord{Trigger: console.BaselineRunTriggerScheduled, WhyCode: "fold_crashed", Error: "mydumper exit 1"}, true},
		{"new tables", console.BaselineRunRecord{Trigger: console.BaselineRunTriggerScheduled, WhyCode: console.BackupWhyCodeNewTables}, true},
		{"full-copy timetable", console.BaselineRunRecord{Trigger: console.BaselineRunTriggerScheduled, WhyCode: console.BackupWhyCodeFullCopy}, false},
		{"first backup", console.BaselineRunRecord{Trigger: console.BaselineRunTriggerScheduled, WhyCode: "first_backup"}, false},
		{"manual", console.BaselineRunRecord{Trigger: "manual", WhyCode: "fold_refused"}, false},
		{"skip", console.BaselineRunRecord{Trigger: console.BaselineRunTriggerScheduled, WhyCode: "fold_refused", SkipReason: "busy"}, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			b, _, sup := newScheduleFixture(t, true)
			c.rec.ServerID, c.rec.Kind = "s", console.BaselineRunDump
			c.rec.StartedAt, c.rec.FinishedAt = now.Add(-2*time.Hour).Format(time.RFC3339), now.Add(-time.Hour).Format(time.RFC3339)
			if err := sup.history.Append(c.rec); err != nil {
				t.Fatal(err)
			}
			next, held := b.emergencyHeld("s", now)
			if held != c.held {
				t.Fatalf("held = %v, want %v", held, c.held)
			}
			if held && !next.Equal(now.Add(-2*time.Hour).Add(emergencyCap)) {
				t.Errorf("next = %v, want a day after the read started", next)
			}
			if _, held := b.emergencyHeld("s", now.Add(23*time.Hour)); held {
				t.Error("still held a day after the read")
			}
		})
	}
}

// New tables are under the same cap: one full read for them a day, even
// when newTablesMaxAttempts would allow more.
func TestEmergencyCap_newTablesPlannerWaits_2006(t *testing.T) {
	b, reg, _ := newScheduleFixture(t, true)
	e := addScheduled(t, reg, true)
	b.noteEmergency(e.ID, time.Now().UTC())
	action, reason := b.newTablesPlanner(e)([]string{"demo.fresh"})
	if action != console.NewTablesActionNotPossible || !strings.Contains(reason, "at most once a day") {
		t.Fatalf("planner = %q, %q; want not possible, saying the daily cap", action, reason)
	}
}
