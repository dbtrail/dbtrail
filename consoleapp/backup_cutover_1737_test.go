package consoleapp

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// #1737: the daemon's half. A full backup reads the index mark before its
// dump and records it; the update that folds from that full backup counts
// its events from it; and the probe tells the rule when no update has been
// measured since the last full backup, so the model abstains until one is.

func TestDump_recordsTheIndexMarkReadBeforeTheDump(t *testing.T) {
	at := time.Date(2026, 9, 18, 11, 23, 49, 0, time.UTC)
	setup := func(t *testing.T, dsn string) (*baselineSupervisor, console.BaselineRequest, string) {
		sup := newBaselineSupervisor(context.Background(), t.TempDir(), baseline.DefaultLockMode)
		sup.history = openHistoryForTest(t)
		local := t.TempDir()
		sup.jobs["a"] = &console.BaselineStatus{State: "running"}
		return sup, console.BaselineRequest{ServerID: "a", ServerName: "a", LocalDir: local, IndexDSN: dsn}, local
	}
	const dsn = "u:p@tcp(10.0.0.1:3306)/idx"
	t.Run("read before the dump, recorded on the full backup", func(t *testing.T) {
		sup, req, local := setup(t, dsn)
		mark := indexMark{events: 5000}
		p := stubIndexMark(t, &mark, true)
		readsBefore := -1
		sup.produce = func(console.BaselineRequest) (dumpOutcome, error) {
			readsBefore = len(p.marks())
			mark.events = 9000 // indexed during the dump: after the read, so not in the base
			return dumpOutcomeAt(t, local, at), nil
		}
		sup.run(req)
		if readsBefore != 1 {
			t.Fatalf("the mark was read %d times before the dump started, want once", readsBefore)
		}
		if got := p.marks(); len(got) != 1 || !strings.Contains(got[0], "timeout=3s") {
			t.Fatalf("index reads = %v, want one, with its dial bounded", got)
		}
		runs := sup.history.List("a")
		if len(runs) != 1 || runs[0].Kind != console.BaselineRunDump || runs[0].IndexMark != 5000 || runs[0].SnapshotTime != at.Format(time.RFC3339) {
			t.Fatalf("history = %+v, want the full backup with the mark read before it (5000)", runs)
		}
		if base, ok := sup.history.IndexMarkFor("a", at.Format(time.RFC3339)); !ok || base != 5000 {
			t.Fatalf("IndexMarkFor the full backup's snapshot = %d,%v", base, ok)
		}
	})
	t.Run("a failed full backup records no mark", func(t *testing.T) {
		sup, req, _ := setup(t, dsn)
		mark := indexMark{events: 5000}
		stubIndexMark(t, &mark, true)
		sup.produce = func(console.BaselineRequest) (dumpOutcome, error) {
			return dumpOutcome{}, errors.New("dump: mydumper exit 2")
		}
		sup.run(req)
		if runs := sup.history.List("a"); len(runs) != 1 || runs[0].Error == "" || runs[0].IndexMark != 0 {
			t.Fatalf("history = %+v, want a failed run with no mark", runs)
		}
	})
	t.Run("no index on the request: no read, no mark", func(t *testing.T) {
		sup, req, local := setup(t, "")
		mark := indexMark{events: 5000}
		p := stubIndexMark(t, &mark, true)
		sup.produce = func(console.BaselineRequest) (dumpOutcome, error) { return dumpOutcomeAt(t, local, at), nil }
		sup.run(req)
		if len(p.marks()) != 0 {
			t.Fatalf("the index was read with no DSN: %v", p.marks())
		}
		if runs := sup.history.List("a"); len(runs) != 1 || runs[0].Error != "" || runs[0].IndexMark != 0 {
			t.Fatalf("history = %+v, want a successful full backup with no mark", runs)
		}
	})
	t.Run("the index does not answer: the full backup runs, unmeasured", func(t *testing.T) {
		sup, req, local := setup(t, dsn)
		mark := indexMark{events: 5000}
		stubIndexMark(t, &mark, false)
		sup.produce = func(console.BaselineRequest) (dumpOutcome, error) { return dumpOutcomeAt(t, local, at), nil }
		sup.run(req)
		if runs := sup.history.List("a"); len(runs) != 1 || runs[0].Error != "" || runs[0].IndexMark != 0 {
			t.Fatalf("history = %+v, want a successful full backup with no mark", runs)
		}
	})
	t.Run("the read is bounded", func(t *testing.T) {
		sup, req, local := setup(t, dsn)
		prev := readIndexMark
		t.Cleanup(func() { readIndexMark = prev })
		var deadline time.Time
		var hasDeadline bool
		readIndexMark = func(ctx context.Context, _ string) (indexMark, bool) {
			deadline, hasDeadline = ctx.Deadline()
			return indexMark{events: 1}, true
		}
		sup.produce = func(console.BaselineRequest) (dumpOutcome, error) { return dumpOutcomeAt(t, local, at), nil }
		before := time.Now()
		sup.run(req)
		if !hasDeadline || deadline.After(before.Add(windowProbeTimeout+time.Second)) {
			t.Fatalf("deadline=%v (%v), want within %s of the start", deadline, hasDeadline, windowProbeTimeout)
		}
	})
	// The request the button and the schedule build carries the index.
	if got := console.BaselineRequestFor(console.ServerEntry{ID: "a", DSN: dsn}).IndexDSN; got != dsn {
		t.Fatalf("BaselineRequestFor carries IndexDSN %q, want the entry's index", got)
	}
}

// The update that folds from a full backup's snapshot counts its events from
// the mark that full backup recorded; before #1737 it counted none.
func TestRunRefresh_measuresTheUpdateAfterAFullBackup(t *testing.T) {
	stubCoverage(t, true, true)
	injectFold(t, 0, nil)
	full := refreshAt.Add(-5 * time.Minute)
	fullStamp := full.Format(time.RFC3339)
	fold := func(t *testing.T, recs []console.BaselineRunRecord, memo *foldMemo, current uint64) console.BaselineRunRecord {
		t.Helper()
		local := t.TempDir()
		writeSnapshotFiles(t, filepath.Join(local, reconstruct.SnapshotDirName(full)), baseline.SuccessMarker)
		sup := newBaselineSupervisor(context.Background(), t.TempDir(), baseline.DefaultLockMode)
		sup.history = openHistoryForTest(t)
		for _, r := range recs {
			r.ServerID = "s"
			if err := sup.history.Append(r); err != nil {
				t.Fatal(err)
			}
		}
		if memo != nil {
			sup.foldedMarks["s"] = *memo
		}
		mark := indexMark{events: current, schemaChanges: 1}
		stubIndexMark(t, &mark, true)
		sup.refreshes["s"] = &console.BaselineStatus{State: "running"}
		sup.runRefresh(refreshRequest{ServerID: "s", ServerName: "s", IndexDSN: "d", BaselineDir: local}, refreshAt, time.Minute)
		runs := sup.history.List("s")
		return runs[len(runs)-1]
	}
	dumpRec := console.BaselineRunRecord{Kind: console.BaselineRunDump, SnapshotTime: fullStamp, IndexMark: 9000,
		StartedAt: "2026-08-27T02:00:00Z", FinishedAt: "2026-08-27T02:08:00Z"}
	// This daemon's previous fold, of the snapshot before the full backup.
	olderFold := &foldMemo{mark: indexMark{events: 1000, schemaChanges: 1}, publishedAt: full.Add(-time.Hour), indexDSN: "d"}
	cases := []struct {
		name    string
		recs    []console.BaselineRunRecord
		memo    *foldMemo
		current uint64
		want    int64
	}{
		{"memo of an older fold, same index: counted from the full backup's mark", []console.BaselineRunRecord{dumpRec}, olderFold, 12_000, 3000},
		{"no memo (a restart since): counted from the full backup's mark", []console.BaselineRunRecord{dumpRec}, nil, 12_000, 3000},
		{"no memo, an update published the snapshot before the restart", []console.BaselineRunRecord{
			{Kind: console.BaselineRunRefresh, SnapshotTime: fullStamp, IndexMark: 9000, Events: 50, UpdateSeconds: 1}}, nil, 12_000, 3000},
		{"memo of an older fold on ANOTHER index: the index was re-pointed, not counted",
			[]console.BaselineRunRecord{dumpRec}, &foldMemo{mark: olderFold.mark, publishedAt: olderFold.publishedAt, indexDSN: "old"}, 12_000, 0},
		{"the full backup failed: no base", []console.BaselineRunRecord{{Kind: console.BaselineRunDump, SnapshotTime: fullStamp,
			IndexMark: 9000, Error: "upload: denied"}}, olderFold, 12_000, 0},
		{"the full backup recorded no mark", []console.BaselineRunRecord{{Kind: console.BaselineRunDump, SnapshotTime: fullStamp}}, olderFold, 12_000, 0},
		{"a mark for another snapshot only", []console.BaselineRunRecord{{Kind: console.BaselineRunDump,
			SnapshotTime: full.Add(-time.Hour).Format(time.RFC3339), IndexMark: 9000}}, nil, 12_000, 0},
		{"the mark went backwards since the full backup (an index rebuilt)", []console.BaselineRunRecord{dumpRec}, olderFold, 8000, 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := fold(t, c.recs, c.memo, c.current)
			if r.Kind != console.BaselineRunRefresh || r.Error != "" || r.Events != c.want {
				t.Fatalf("update = %+v, want %d events counted", r, c.want)
			}
		})
	}
}

// After a full backup the probe knows the count (the full backup's mark) and
// says the model is older than it; a measured update clears that.
func TestMeasureWindow_afterAFullBackup(t *testing.T) {
	anchor := time.Date(2026, 9, 18, 5, 0, 0, 0, time.UTC)
	e := console.ServerEntry{ID: "a", Name: "a", DSN: "idx"}
	b, _, sup := newScheduleFixture(t, true)
	for _, rec := range []console.BaselineRunRecord{
		{ServerID: "a", Kind: console.BaselineRunRefresh, Events: 1000, UpdateSeconds: 60},
		{ServerID: "a", Kind: console.BaselineRunRefresh, Events: 241_000, UpdateSeconds: 120},
		{ServerID: "a", Kind: console.BaselineRunDump, SnapshotTime: anchor.Format(time.RFC3339), IndexMark: 1000,
			StartedAt: "2026-09-18T04:52:00Z", FinishedAt: "2026-09-18T05:00:00Z"},
	} {
		if err := sup.history.Append(rec); err != nil {
			t.Fatal(err)
		}
	}
	mark := indexMark{events: 17_001_000}
	stubIndexMark(t, &mark, true)
	w := b.measureWindow(context.Background(), e, anchor)
	if w.Events != 17_000_000 || !w.UnmeasuredSinceFull || w.FoldRate != 4000 || w.LastFull != 8*time.Minute {
		t.Fatalf("window after a full backup = %+v, want 17,000,000 events, the model marked older than the full backup", w)
	}
	// The rate alone would cut this over (about 71 minutes against 8); the
	// rule abstains and the update runs.
	if why := console.CutoverToFull(w, time.Hour, anchor.Add(5*time.Minute)); why != "" {
		t.Fatalf("the slot after a full backup cut over on a model older than it: %q", why)
	}
	if err := sup.history.Append(console.BaselineRunRecord{ServerID: "a", Kind: console.BaselineRunRefresh, Events: 50_000, UpdateSeconds: 70}); err != nil {
		t.Fatal(err)
	}
	b.mu.Lock()
	b.windows = nil
	b.mu.Unlock()
	if w := b.measureWindow(context.Background(), e, anchor); w.UnmeasuredSinceFull {
		t.Fatalf("window after a measured update = %+v, want the model current", w)
	}
}

// The slots themselves: a full backup the measured rule chose is followed by
// an update that is measured, so the model gets a sample newer than the
// rate that chose it. Before #1737 that update recorded no events, and the
// slot after it chose a full backup on the same rate again.
func TestBackupScheduler_theUpdateAfterAChosenFullBackupIsMeasured(t *testing.T) {
	b, reg, sup := newScheduleFixture(t, true)
	b.window = b.measureWindow
	e := addScheduled(t, reg, true) // the fake snapshot at snapshotAnchor; a source that parses
	for _, rec := range []console.BaselineRunRecord{
		{ServerID: e.ID, Kind: console.BaselineRunDump, StartedAt: "2026-08-20T05:00:00Z", FinishedAt: "2026-08-20T05:08:00Z"},
		{ServerID: e.ID, Kind: console.BaselineRunRefresh, Events: 1000, UpdateSeconds: 60, FinishedAt: "2026-08-20T12:25:00Z"},
		{ServerID: e.ID, Kind: console.BaselineRunRefresh, Events: 241_000, UpdateSeconds: 120, FinishedAt: "2026-08-20T12:30:00Z"}, // 4000 events/s beyond the fixed minute
	} {
		if err := sup.history.Append(rec); err != nil {
			t.Fatal(err)
		}
	}
	sup.foldedMarks[e.ID] = foldMemo{mark: indexMark{events: 1000}, publishedAt: snapshotAnchor, indexDSN: e.DSN}
	mark := indexMark{events: 17_001_000} // 17 M events since the anchor: ~71 min at the rate, against an 8 min full backup
	stubIndexMark(t, &mark, true)
	stubCoverage(t, true, true)
	injectFold(t, 0, nil)
	dumpAt := time.Now().UTC().Truncate(time.Second)
	sup.produce = func(console.BaselineRequest) (dumpOutcome, error) {
		// Long enough that the full backup's duration is a whole second
		// in its record: LastFullBackup reads zero otherwise, and the model
		// would then decide nothing whether it abstains or not.
		time.Sleep(1100 * time.Millisecond)
		return dumpOutcomeAt(t, e.BaselineDir, dumpAt), nil
	}
	logs := captureSlogFor(t)
	t0 := time.Date(2026, 8, 20, 13, 0, 5, 0, time.UTC)

	// Slot 1: the measured rule chooses a full backup, and says on what.
	fireAt(b, t0)
	st := waitTerminalMethod(t, b, e.ID, console.BackupMethodFull)
	if console.BackupWhyCode(st.LastWhy) != "window_measured" || st.Last.State != "succeeded" {
		t.Fatalf("slot 1 = %+v, want a successful full backup on the measured window", st)
	}
	for _, want := range []string{"fold_rate_events_per_second=4000", "fold_fixed=1m0s", "fold_samples=2", "newest_sample_age=1h30m5s"} {
		if !strings.Contains(logs.String(), want) {
			t.Fatalf("the full backup's log line lacks %q: %q", want, logs.String())
		}
	}
	if base, ok := sup.history.IndexMarkFor(e.ID, dumpAt.Format(time.RFC3339)); !ok || base != 17_001_000 {
		t.Fatalf("the full backup recorded mark %d,%v, want the one read before it", base, ok)
	}

	// Slot 2: 100,000 events since the full backup. The rate would call
	// that dearer than the (one-second) full backup; the model is older
	// than it, so the update runs and is measured from its mark.
	mark = indexMark{events: 17_101_000}
	b.tick(context.Background(), t0.Add(2*time.Hour))
	if st := waitTerminalMethod(t, b, e.ID, console.BackupMethodRefresh); st.Last.State != "succeeded" || st.LastWhy != "" {
		t.Fatalf("slot 2 = %+v, want a successful update", st)
	}
	runs := sup.history.List(e.ID)
	if r := runs[len(runs)-1]; r.Kind != console.BaselineRunRefresh || r.Events != 100_000 || r.UpdateSeconds <= 0 {
		t.Fatalf("the update after the full backup = %+v, want 100,000 events measured", r)
	}
	if !sup.history.MeasuredSinceFull(e.ID) {
		t.Fatal("the model is still older than the full backup after a measured update")
	}
}
