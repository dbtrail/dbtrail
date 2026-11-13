package consoleapp

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/console"
)

// #2674, points 1, 4 and 6.

// Point 1: the fixed-cost rule applies on a server WITH a per-event rate
// when the count of changes is unknown. Before, the estimate was skipped
// (no count) and the fixed-cost rule asked for no rate, so a server whose
// cheapest measured update was clearly dearer than its last full read
// updated anyway.
func TestMeasureWindow_anUnknownCountWithARateCutsOverOnTheFixedCost_2674(t *testing.T) {
	b, reg, _ := newScheduleFixture(t, true)
	b.window = b.measureWindow
	e := addScheduled(t, reg, true) // no record names a mark for its snapshot
	for _, rec := range []console.BaselineRunRecord{
		{ServerID: e.ID, Kind: console.BaselineRunDump, StartedAt: "2026-08-19T05:00:00Z", FinishedAt: "2026-08-19T05:02:00Z"},
		// Six minutes for a thousand events, twelve for fifty thousand: a
		// fixed cost of six minutes and a rate the model reads.
		{ServerID: e.ID, Kind: console.BaselineRunRefresh, Events: 1000, UpdateSeconds: 360},
		{ServerID: e.ID, Kind: console.BaselineRunRefresh, Events: 50_000, UpdateSeconds: 720},
	} {
		if err := b.sup.history.Append(rec); err != nil {
			t.Fatal(err)
		}
	}
	w := b.measureWindow(context.Background(), e, snapshotAnchor)
	if w.Events != -1 || w.FoldRate <= 0 || w.FoldFixed != 6*time.Minute || w.LastFull != 2*time.Minute {
		t.Fatalf("window = %+v, want an unknown count, a rate, six-minute updates and a two-minute full read", w)
	}
	method, why := freshDecision(t, b, e, snapshotAnchor.Add(8*24*time.Hour))
	t.Log(why)
	if method != console.BackupMethodFull || why != console.BackupWhyWindowPrefix+": the cheapest recent update took 6m, more than twice and over 30s longer than the last full read, which took 2m" {
		t.Fatalf("method=%q why=%q, want the full read on the measured fixed cost", method, why)
	}
}

// Point 4: the reason of the update that ran where the slot would have been
// skipped is kept on its run record, and a plain update's is not.
func TestBackupScheduler_theReanchorReasonIsKeptOnTheRun_2674(t *testing.T) {
	b, sup, e, mark, _ := quietUnconfirmed(t, captureProbeResult{detail: positionMode})
	stubCoverage(t, false, true)
	t0 := time.Date(2026, 8, 28, 9, 0, 5, 0, time.UTC)
	b.tick(context.Background(), t0)
	forgetWindow(b)
	b.tick(context.Background(), t0.Add(time.Hour))
	st := waitTerminalMethod(t, b, e.ID, console.BackupMethodRefresh)
	want := console.UnconfirmedUpdateWhy(positionMode,
		"the previous snapshot is close to the oldest change the index still keeps, and could not be updated once rotation drops those hours")
	if st.LastUpdateWhy != want {
		t.Fatalf("the live view's update why = %q, want %q", st.LastUpdateWhy, want)
	}
	recs := sup.history.List(e.ID)
	last := recs[len(recs)-1]
	t.Logf("recorded: %s", last.UpdateWhy)
	if last.Kind != console.BaselineRunRefresh || last.UpdateWhy != want || last.Why != "" || last.WhyCode != "" {
		t.Fatalf("the run's record: %+v, want the update with its reason in update_why and no full-read why", last)
	}
	// A change is indexed: the next update is a plain one, and says nothing.
	mark.events++
	forgetWindow(b)
	b.tick(context.Background(), t0.Add(2*time.Hour))
	st = waitTerminalMethod(t, b, e.ID, console.BackupMethodRefresh)
	recs = sup.history.List(e.ID)
	if last := recs[len(recs)-1]; last.UpdateWhy != "" || st.LastUpdateWhy != "" {
		t.Fatalf("a plain update carries an update why: record %q, live %q", last.UpdateWhy, st.LastUpdateWhy)
	}
}

// Point 6: while the only job is the send to S3 of a snapshot kept on this
// machine, the full read is refused saying so, not as "a baseline is
// already running". Still ErrBaselineRunning, so the endpoint answers 409.
func TestTrigger_saysWhatHoldsTheServer_2674(t *testing.T) {
	since := "2026-11-12T10:00:05Z"
	cases := []struct {
		name string
		put  func(s *baselineSupervisor, id string)
		want string
	}{
		{"the send of a held snapshot", func(s *baselineSupervisor, id string) {
			s.refreshes[id] = &console.BaselineStatus{State: "running", Since: since, SendingHeld: true}
		}, "a snapshot kept on this machine is being sent to S3 since 2026-11-12 10:00:05 UTC; the full read can start once it finishes"},
		{"an update", func(s *baselineSupervisor, id string) {
			s.refreshes[id] = &console.BaselineStatus{State: "running", Since: since}
		}, "an update of this server's snapshot from the recorded changes is running since 2026-11-12 10:00:05 UTC; the full read can start once it finishes"},
		{"a full read", func(s *baselineSupervisor, id string) {
			s.jobs[id] = &console.BaselineStatus{State: "running", Since: since}
		}, "a full read of this server is already running since 2026-11-12 10:00:05 UTC"},
		{"a restore", func(s *baselineSupervisor, id string) {
			s.restores[id] = &console.BaselineStatus{State: "running", Since: since}
		}, "a snapshot restore for this server is running since 2026-11-12 10:00:05 UTC; the full read can start once it finishes"},
		{"a SQL export", func(s *baselineSupervisor, id string) {
			s.exports[id] = &console.BaselineStatus{State: "running", Since: since}
		}, "a SQL export of this server's snapshot is running since 2026-11-12 10:00:05 UTC; the full read can start once it finishes"},
		{"a compaction", func(s *baselineSupervisor, id string) {
			s.compacts[id] = &console.BaselineStatus{State: "running", Since: since}
		}, "the table changes of this server's snapshots are being merged since 2026-11-12 10:00:05 UTC; the full read can start once it finishes"},
		{"a start time that does not parse", func(s *baselineSupervisor, id string) {
			s.refreshes[id] = &console.BaselineStatus{State: "running", Since: "soon", SendingHeld: true}
		}, "a snapshot kept on this machine is being sent to S3; the full read can start once it finishes"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, _, sup := newScheduleFixture(t, true)
			sup.mu.Lock()
			for _, m := range []*map[string]*console.BaselineStatus{&sup.jobs, &sup.refreshes, &sup.restores, &sup.exports, &sup.compacts} {
				if *m == nil {
					*m = map[string]*console.BaselineStatus{}
				}
			}
			c.put(sup, "srv-1")
			sup.mu.Unlock()
			err := sup.Trigger(console.BaselineRequest{ServerID: "srv-1", ServerName: "wp", LocalDir: t.TempDir(), SourceDSN: "src:pw@tcp(127.0.0.1:3306)/"})
			t.Logf("refusal: %v", err)
			if !errors.Is(err, console.ErrBaselineRunning) || err.Error() != c.want {
				t.Fatalf("err = %v, want ErrBaselineRunning saying %q", err, c.want)
			}
			if strings.Contains(err.Error(), "baseline") {
				t.Errorf("the refusal %q says baseline, a word the page does not use", err)
			}
		})
	}
	// A job in no slot of busyLocked's: the plain sentinel, not a guess.
	_, _, sup := newScheduleFixture(t, true)
	sup.mu.Lock()
	err := sup.busyErrLocked("nobody")
	sup.mu.Unlock()
	if err != console.ErrBaselineRunning {
		t.Fatalf("no job named: %v", err)
	}
}
