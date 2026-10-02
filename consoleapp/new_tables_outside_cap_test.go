package consoleapp

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/console"
)

// Full reads that include new tables sit outside the daily cap on emergency
// full reads: they neither wait for it nor use it up. Their own bound is
// newTablesMaxAttempts per server per newTablesWindow, counted from starts
// recorded durably, so a restart does not reset it.

// newTablesRead runs the planner for tables and, when it promises a full
// read, starts it and waits until the stubbed read ran. It returns the
// planner's verdict.
func newTablesRead(t *testing.T, b *backupScheduler, reads *fakeFullReads, e console.ServerEntry, tables ...string) (string, string) {
	t.Helper()
	action, reason := b.newTablesPlanner(e)(tables)
	if action != console.NewTablesActionFullRead {
		return action, reason
	}
	before := reads.count()
	b.includeNewTables(e, console.BaselineStatus{State: "succeeded", NewTables: tables, NewTablesAction: console.NewTablesActionFullRead})
	deadline := time.Now().Add(10 * time.Second)
	for reads.count() == before || b.ScheduleState(e.ID).Running {
		if time.Now().After(deadline) {
			t.Fatalf("the new-tables full read for %v never ran to the end", tables)
		}
		time.Sleep(10 * time.Millisecond)
	}
	b.watchers.Wait()
	return action, reason
}

func TestNewTables_notHeldNorConsumingTheEmergencyCap(t *testing.T) {
	b, reg, sup := newScheduleFixture(t, true)
	reads := stubFullReads(t, sup)
	e := addScheduled(t, reg, true)
	b.tick(context.Background(), time.Date(2026, 8, 28, 8, 0, 5, 0, time.UTC))

	// An emergency full read (a fallback) ten hours ago.
	now := time.Now().UTC()
	emergencyAt := now.Add(-10 * time.Hour)
	b.noteEmergency(e.ID, emergencyAt)
	nextBefore, held := b.emergencyHeld(e.ID, now)
	if !held {
		t.Fatal("fixture: the emergency read ten hours ago must hold the cap")
	}

	// New tables appear: the full read to include them starts anyway.
	if action, reason := newTablesRead(t, b, reads, e, "demo.fresh", "demo.other"); action != console.NewTablesActionFullRead {
		t.Fatalf("new tables held back by the emergency cap: %q %q", action, reason)
	}
	if reads.count() != 1 {
		t.Fatalf("full reads = %d, want 1", reads.count())
	}

	// An hour later a refused update still finds the cap where the fallback
	// left it: the new-tables read neither reset nor used it.
	next, held := b.emergencyHeld(e.ID, now.Add(time.Hour))
	if !held {
		t.Fatal("after the new-tables read a fallback is no longer held by the daily cap")
	}
	if !next.Equal(nextBefore) {
		t.Fatalf("the new-tables read moved the emergency cap: next %v, was %v", next, nextBefore)
	}
	if got := sup.history.EmergencyStarted(e.ID); got != emergencyAt.Format(time.RFC3339) {
		t.Fatalf("durable emergency start = %q, want the fallback's %q", got, emergencyAt.Format(time.RFC3339))
	}
	for _, r := range sup.history.List(e.ID) {
		if r.WhyCode == console.BackupWhyCodeNewTables && console.IsEmergencyWhyCode(r.WhyCode) {
			t.Fatal("a new_tables record counts as an emergency read")
		}
	}
}

func TestNewTables_noEmergencyBeforeAndNoneAfter(t *testing.T) {
	b, reg, sup := newScheduleFixture(t, true)
	reads := stubFullReads(t, sup)
	e := addScheduled(t, reg, true)
	b.tick(context.Background(), time.Date(2026, 8, 28, 8, 0, 5, 0, time.UTC))
	if action, reason := newTablesRead(t, b, reads, e, "demo.fresh"); action != console.NewTablesActionFullRead {
		t.Fatalf("planner = %q %q", action, reason)
	}
	if got := sup.history.EmergencyStarted(e.ID); got != "" {
		t.Fatalf("the new-tables read was recorded as an emergency read: %q", got)
	}
	if _, held := b.emergencyHeld(e.ID, time.Now().UTC()); held {
		t.Fatal("a new-tables read holds back the next fallback")
	}
}

func TestNewTables_boundedPerDayAcrossRestart(t *testing.T) {
	b, reg, sup := newScheduleFixture(t, true)
	reads := stubFullReads(t, sup)
	e := addScheduled(t, reg, true)
	b.tick(context.Background(), time.Date(2026, 8, 28, 8, 0, 5, 0, time.UTC))

	// A different set of new tables each time: the per-set rules alone would
	// let every one of them through.
	for i := 0; i < newTablesMaxAttempts; i++ {
		tbl := "demo.t" + string(rune('a'+i))
		if action, reason := newTablesRead(t, b, reads, e, tbl); action != console.NewTablesActionFullRead {
			t.Fatalf("read %d: planner = %q %q", i+1, action, reason)
		}
	}
	if reads.count() != newTablesMaxAttempts {
		t.Fatalf("full reads = %d, want %d", reads.count(), newTablesMaxAttempts)
	}
	action, reason := b.newTablesPlanner(e)([]string{"demo.zz"})
	if action != console.NewTablesActionNotPossible || !strings.Contains(reason, "the next one is allowed after") {
		t.Fatalf("read %d the same day: %q %q; want not possible with the next allowed time", newTablesMaxAttempts+1, action, reason)
	}
	if strings.Contains(reason, "at most once a day") {
		t.Fatalf("the new-tables bound is worded as the emergency cap: %q", reason)
	}

	// Restart: a new scheduler over the history reopened from disk.
	h2, err := console.OpenBaselineHistory(sup.history.Path())
	if err != nil {
		t.Fatal(err)
	}
	sup.history = h2
	b2 := newBackupScheduler(sup, reg, true, false)
	b2.window = nil
	if action, reason := b2.newTablesPlanner(e)([]string{"demo.zz"}); action != console.NewTablesActionNotPossible {
		t.Fatalf("after a restart the new-tables bound was forgotten: %q %q", action, reason)
	}

	// A day later the bound lets the next one through.
	if _, held := b2.newTablesHeld(e.ID, time.Now().UTC().Add(newTablesWindow+time.Minute)); held {
		t.Fatal("still held a day after the reads")
	}
}

func TestNewTablesHeld_onlyNewTablesStartsCount(t *testing.T) {
	b, _, sup := newScheduleFixture(t, true)
	now := time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC)
	for i := 0; i < newTablesMaxAttempts+2; i++ {
		b.noteEmergency("s", now.Add(-time.Duration(i)*time.Hour))
		if err := sup.history.Append(console.BaselineRunRecord{ServerID: "s", Kind: console.BaselineRunDump, Trigger: console.BaselineRunTriggerScheduled,
			WhyCode: "fold_refused", StartedAt: now.Add(-time.Duration(i) * time.Hour).Format(time.RFC3339)}); err != nil {
			t.Fatal(err)
		}
	}
	if _, held := b.newTablesHeld("s", now); held {
		t.Fatal("emergency reads count against the new-tables bound")
	}
	for i := 0; i < newTablesMaxAttempts; i++ {
		b.noteNewTablesStart("s", now.Add(-time.Duration(30-i)*time.Hour)) // outside the window
	}
	if _, held := b.newTablesHeld("s", now); held {
		t.Fatal("reads older than the window still count")
	}
	first := now.Add(-5 * time.Hour)
	for i := 0; i < newTablesMaxAttempts; i++ {
		b.noteNewTablesStart("s", first.Add(time.Duration(i)*time.Hour))
	}
	next, held := b.newTablesHeld("s", now)
	if !held || !next.Equal(first.Add(newTablesWindow)) {
		t.Fatalf("held = %v next = %v; want held until %v", held, next, first.Add(newTablesWindow))
	}
}
