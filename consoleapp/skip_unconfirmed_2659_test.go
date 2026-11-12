package consoleapp

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/notify"
)

// #2659, the remaining case, in the real slot: nothing indexed since an old
// snapshot on a source that cannot be asked whether capture recorded
// everything it wrote. The slot is skipped: no full read, no update, one
// record in the history for the whole quiet stretch and one log line for
// it, and the first indexed change ends it.

const (
	unconfirmedSkipLine   = "no snapshot is made at this slot, nor at the following ones while this lasts"
	unconfirmedUpdateLine = "updating the snapshot instead of skipping the slot"
	unconfirmedEndedLine  = "scheduled snapshots are made again"
	positionMode          = "the capture runs in binlog-position mode, which is not compared"
)

// quietUnconfirmed sets up a server whose index recorded nothing since its
// snapshot (a memo at 1000 and a mark at 1000) and whose source cannot be
// asked; mark is the index's current mark, for the test to move.
func quietUnconfirmed(t *testing.T, probe captureProbeResult) (*backupScheduler, *baselineSupervisor, console.ServerEntry, *indexMark, *atomic.Int32) {
	t.Helper()
	b, reg, sup := newScheduleFixture(t, true)
	b.window = b.measureWindow
	e := addScheduled(t, reg, true) // the fake snapshot at snapshotAnchor (2026-08-20), hourly
	sup.foldedMarks[e.ID] = foldMemo{mark: indexMark{events: 1000}, publishedAt: snapshotAnchor, indexDSN: e.DSN,
		destination: refreshDestination(b.rebuildRequest(e, mustParse(t, e))), readsFrom: snapshotAnchor, readsFromKnown: true}
	mark := &indexMark{events: 1000}
	stubIndexMark(t, mark, true)
	stubCaptureProbe(t, probe)
	var folds atomic.Int32
	countFolds(t, &folds)
	return b, sup, e, mark, &folds
}

func mustParse(t *testing.T, e console.ServerEntry) console.ParsedBackupSchedule {
	t.Helper()
	p, err := e.BackupSchedule.Parse()
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// scheduledSkips is the skips on record for id.
func scheduledSkips(sup *baselineSupervisor, id string) []console.BaselineRunRecord {
	var out []console.BaselineRunRecord
	for _, r := range sup.history.List(id) {
		if r.SkipReason != "" {
			out = append(out, r)
		}
	}
	return out
}

// forgetWindow drops the cached window, as the minute between two slots does.
func forgetWindow(b *backupScheduler) {
	b.mu.Lock()
	b.windows = nil
	b.mu.Unlock()
}

func TestBackupScheduler_skipsTheSlotWhenTheSourceCannotBeAsked_2659(t *testing.T) {
	b, sup, e, mark, folds := quietUnconfirmed(t, captureProbeResult{detail: positionMode})
	stubCoverage(t, true, true)
	logs := captureSlogFor(t)
	t0 := time.Date(2026, 8, 28, 9, 0, 5, 0, time.UTC) // eight days after the snapshot; the hourly cut-over age is six hours
	b.tick(context.Background(), t0)
	want := console.SkipUnconfirmedReason(positionMode)
	// Five slots of a quiet server.
	for i := 1; i <= 5; i++ {
		forgetWindow(b)
		slot := t0.Add(time.Duration(i) * time.Hour)
		b.tick(context.Background(), slot)
		st := b.ScheduleState(e.ID)
		if st.LastStartedAt != "" || st.Running {
			t.Fatalf("slot %d: a job was started: %+v", i, st)
		}
		if st.LastSkipReason != want {
			t.Fatalf("slot %d: skip reason %q, want %q", i, st.LastSkipReason, want)
		}
		// The page's line names the slot that was just skipped, not the
		// first one of the stretch.
		if at, err := time.Parse(time.RFC3339, st.LastSkippedAt); err != nil || at.Before(slot.Truncate(time.Hour)) {
			t.Fatalf("slot %d: skipped at %q, want this slot (%s)", i, st.LastSkippedAt, slot.Truncate(time.Hour).Format(time.RFC3339))
		}
		skips := scheduledSkips(sup, e.ID)
		if len(skips) != 1 || skips[0].SkipReason != want || skips[0].FinishedAt != st.LastSkippedAt || skips[0].Trigger != console.BaselineRunTriggerScheduled {
			t.Fatalf("slot %d: skips on record %+v, want one, moved to this slot", i, skips)
		}
	}
	t.Logf("recorded skip: %s", want)
	if folds.Load() != 0 || lastScheduledDump(sup, e.ID) != nil {
		t.Fatalf("%d folds and a full read %+v, want neither", folds.Load(), lastScheduledDump(sup, e.ID))
	}
	out := logs.String()
	line := logLine(out, unconfirmedSkipLine)
	t.Logf("log: %s", line)
	if strings.Count(out, unconfirmedSkipLine) != 1 {
		t.Fatalf("the skip was logged %d times over five slots, want once: %q", strings.Count(out, unconfirmedSkipLine), out)
	}
	if !strings.Contains(line, "level=INFO") || !strings.Contains(line, positionMode) || !strings.Contains(line, "the copy stays at its last snapshot") {
		t.Fatalf("the skip's line reads %q", line)
	}
	for _, never := range []string{"scheduled snapshot did not start", "taking a full read", unconfirmedUpdateLine, unconfirmedEndedLine} {
		if strings.Contains(out, never) {
			t.Fatalf("the quiet slots logged %q: %q", never, out)
		}
	}
	// The page is told what the next run will be: none.
	forgetWindow(b)
	method, why, err := console.ChooseBackupMethodAt(context.Background(), e, b.gates(), t0.Add(6*time.Hour))
	if err != nil || method != console.BackupMethodSkip || why != want {
		t.Fatalf("the next run as the page asks for it: method=%q why=%q err=%v", method, why, err)
	}

	// A change is indexed: the next slot updates, and says the skips ended.
	mark.events = 1001
	forgetWindow(b)
	b.tick(context.Background(), t0.Add(7*time.Hour))
	st := waitTerminalMethod(t, b, e.ID, console.BackupMethodRefresh)
	if folds.Load() != 1 || st.LastWhy != "" {
		t.Fatalf("after a change: %d folds, state %+v, want the one update", folds.Load(), st)
	}
	// The update's record says its mark was read, which is what makes a
	// zero a mark after a restart.
	recs := sup.history.List(e.ID)
	if last := recs[len(recs)-1]; last.Kind != console.BaselineRunRefresh || last.Error != "" || last.IndexMark != 1001 || !last.IndexMarkKnown {
		t.Fatalf("the update's record: %+v, want its mark recorded as read", last)
	}
	out = logs.String()
	t.Logf("log: %s", logLine(out, unconfirmedEndedLine))
	if strings.Count(out, unconfirmedEndedLine) != 1 || b.sup.gateEdge.Active(unconfirmedSkipKey+e.ID) {
		t.Fatalf("the end of the skips was not said once: %q", out)
	}
}

// The same quiet stretch reminds once a day, not at every slot.
func TestBackupScheduler_theSkipIsSaidOnceADay_2659(t *testing.T) {
	b, _, _, _, _ := quietUnconfirmed(t, captureProbeResult{detail: positionMode})
	stubCoverage(t, true, true)
	clock := time.Date(2026, 8, 28, 9, 0, 5, 0, time.UTC)
	b.sup.gateEdge = notify.NewEdgeWithClock(notify.DefaultRepeatEvery, func() time.Time { return clock })
	logs := captureSlogFor(t)
	t0 := clock
	b.tick(context.Background(), t0)
	for i := 1; i <= 49; i++ { // 49 hourly slots: two days and an hour
		forgetWindow(b)
		clock = t0.Add(time.Duration(i) * time.Hour)
		b.tick(context.Background(), clock)
	}
	if got := strings.Count(logs.String(), unconfirmedSkipLine); got != 3 {
		t.Fatalf("49 hourly slots logged the skip %d times, want 3 (the first, and one a day)", got)
	}
}

// What is evidence keeps the full read: a source that reports transactions
// capture has not recorded, and a capture whose own record says it lost
// changes. Neither is skipped.
func TestBackupScheduler_evidenceStillReadsInFull_2659(t *testing.T) {
	for _, c := range []struct {
		name  string
		probe captureProbeResult
		tail  string
	}{
		{"the source is ahead", captureProbeResult{verdict: console.CaptureBehind, detail: "the source reports transactions the capture's checkpoint does not include"},
			"but the source reports transactions the capture's checkpoint does not include, so the capture may have stopped or fallen behind"},
		{"a capture loss on record", captureProbeResult{detail: captureDetailLost}, "could not be checked (" + captureDetailLost + ")"},
		{"dropped rows no full read has read since", captureProbeResult{detail: captureDetailDropped}, "could not be checked (" + captureDetailDropped + ")"},
		{"the capture holds what the source does not", captureProbeResult{detail: captureDetailDiverged}, "could not be checked (" + captureDetailDiverged + ")"},
	} {
		t.Run(c.name, func(t *testing.T) {
			b, sup, e, _, folds := quietUnconfirmed(t, c.probe)
			stubCoverage(t, true, true) // the snapshot could wait: it is the evidence that decides
			e.SourceDSN = "not a dsn"   // the full read fails fast; that it STARTED is the point
			if err := b.reg.Update(e); err != nil {
				t.Fatal(err)
			}
			logs := captureSlogFor(t)
			fireAt(b, time.Date(2026, 8, 28, 9, 0, 5, 0, time.UTC))
			st := waitTerminalMethod(t, b, e.ID, console.BackupMethodFull)
			run := lastScheduledDump(sup, e.ID)
			if run == nil || run.WhyCode != "window_age" || !strings.HasSuffix(run.Why, c.tail) {
				t.Fatalf("the full read on record: %+v (state %+v)", run, st)
			}
			t.Log(run.Why)
			if folds.Load() != 0 || len(scheduledSkips(sup, e.ID)) != 0 || strings.Contains(logs.String(), unconfirmedSkipLine) {
				t.Fatalf("%d folds, skips %+v: evidence was skipped or updated over", folds.Load(), scheduledSkips(sup, e.ID))
			}
		})
	}
}

// A snapshot that cannot wait is updated, not skipped: the daemon-wide gate
// re-anchors a quiet snapshot before rotation drops the hours it reads from
// (#1689), and a slot skipped past that point would leave a snapshot no
// update can start from. Each thing that stops the skip is said in the
// update's reason, and logged once.
func TestBackupScheduler_updatesWhenTheSnapshotCannotWait_2659(t *testing.T) {
	for _, c := range []struct {
		name   string
		setup  func(t *testing.T)
		whyNot string
	}{
		{"close to the oldest hour the index keeps", func(t *testing.T) { stubCoverage(t, false, true) },
			"the previous snapshot is close to the oldest change the index still keeps, and could not be updated once rotation drops those hours"},
		{"the index's reach could not be read", func(t *testing.T) { stubCoverage(t, false, false) },
			"how far back the index still keeps changes could not be read"},
		{"a table was created", func(t *testing.T) {
			stubCoverage(t, true, true)
			prev := tablesCreatedSince
			t.Cleanup(func() { tablesCreatedSince = prev })
			tablesCreatedSince = func(context.Context, string, time.Time) (bool, error) { return true, nil }
		}, "a table was created since the previous snapshot"},
		{"whether a table was created could not be read", func(t *testing.T) {
			stubCoverage(t, true, true)
			prev := tablesCreatedSince
			t.Cleanup(func() { tablesCreatedSince = prev })
			tablesCreatedSince = func(context.Context, string, time.Time) (bool, error) { return false, errors.New("index down") }
		}, "whether a table was created since the previous snapshot could not be read"},
		{"a capture loss on record", func(t *testing.T) {
			stubCoverage(t, true, true)
			prev := lossOnRecordSince
			t.Cleanup(func() { lossOnRecordSince = prev })
			lossOnRecordSince = func(context.Context, string, time.Time) (bool, error) { return true, nil }
		}, "capture lost changes since the previous snapshot"},
		{"whether capture lost changes could not be read", func(t *testing.T) {
			stubCoverage(t, true, true)
			prev := lossOnRecordSince
			t.Cleanup(func() { lossOnRecordSince = prev })
			lossOnRecordSince = func(context.Context, string, time.Time) (bool, error) { return false, errors.New("index down") }
		}, "whether capture lost changes since the previous snapshot could not be read"},
	} {
		t.Run(c.name, func(t *testing.T) {
			b, _, e, _, _ := quietUnconfirmed(t, captureProbeResult{detail: "PostgreSQL sources are not compared yet"})
			c.setup(t)
			now := time.Date(2026, 8, 28, 10, 0, 5, 0, time.UTC)
			w := b.measureWindow(context.Background(), e, snapshotAnchor)
			if w.Events != 0 || w.Capture != "" || w.CanWait || w.CannotWaitWhy != c.whyNot {
				t.Fatalf("window = %+v, want a known zero, a source that cannot be asked and a snapshot that cannot wait because %q", w, c.whyNot)
			}
			method, why := freshDecision(t, b, e, now)
			t.Log(why)
			if method != console.BackupMethodRefresh || why != console.UnconfirmedUpdateWhy("PostgreSQL sources are not compared yet", c.whyNot) {
				t.Fatalf("method=%q why=%q, want the update that says what stopped the skip", method, why)
			}
		})
	}
	// In the real slot: the update runs (the gate folds a snapshot that is
	// no longer safely covered), is logged once over three slots, and no
	// skip of this kind is recorded.
	b, sup, e, _, folds := quietUnconfirmed(t, captureProbeResult{detail: positionMode})
	stubCoverage(t, false, true)
	logs := captureSlogFor(t)
	t0 := time.Date(2026, 8, 28, 9, 0, 5, 0, time.UTC)
	b.tick(context.Background(), t0)
	memo := sup.foldedMarks[e.ID]
	for i := 1; i <= 3; i++ {
		// The fold seam publishes nothing, so the newest snapshot stays the
		// same one; the memo is put back on it, as if each re-anchoring
		// fold had been refused. The same state at every slot.
		sup.mu.Lock()
		sup.foldedMarks[e.ID] = memo
		sup.mu.Unlock()
		forgetWindow(b)
		b.tick(context.Background(), t0.Add(time.Duration(i)*time.Hour))
		waitTerminalMethod(t, b, e.ID, console.BackupMethodRefresh)
	}
	if folds.Load() != 3 {
		t.Fatalf("%d folds over three slots, want one each", folds.Load())
	}
	out := logs.String()
	t.Logf("log: %s", logLine(out, unconfirmedUpdateLine))
	if strings.Count(out, unconfirmedUpdateLine) != 1 || strings.Contains(out, unconfirmedSkipLine) {
		t.Fatalf("log = %q, want the update said once and no skip", out)
	}
	for _, s := range scheduledSkips(sup, e.ID) {
		if console.IsUnconfirmedSkip(s.SkipReason) {
			t.Fatalf("a skip was recorded where the update ran: %+v", s)
		}
	}
}

// Where the snapshot's readers start comes from the daemon's memo of its
// fold, and from the snapshot's files when there is no memo for it (after a
// restart, or when a full read published it). Unknown is "cannot wait".
func TestQuietSnapshotCanWait_whereTheReadersStart_2659(t *testing.T) {
	b, sup, e, _, _ := quietUnconfirmed(t, captureProbeResult{detail: positionMode})
	sup.tableDeltas = true
	cov := stubCoverage(t, true, true)
	now := snapshotAnchor.Add(8 * 24 * time.Hour)
	chainStart := snapshotAnchor.Add(-20 * time.Hour)
	graded := func() time.Time {
		cov.mu.Lock()
		defer cov.mu.Unlock()
		return cov.coverages[len(cov.coverages)-1].publishedAt
	}

	// The memo names the snapshot: its instant is graded, and no file is read.
	sup.foldedMarks[e.ID] = foldMemo{mark: indexMark{events: 1000}, publishedAt: snapshotAnchor, indexDSN: e.DSN, readsFrom: chainStart, readsFromKnown: true}
	reads := stubReadsFrom(t, time.Time{}, errors.New("must not be read"))
	if ok, why := sup.quietSnapshotCanWait(context.Background(), e, snapshotAnchor, now, time.Hour); !ok || !graded().Equal(chainStart) || reads.Load() != 0 {
		t.Fatalf("with a memo: ok=%v why=%q graded=%s reads=%d", ok, why, graded(), reads.Load())
	}
	// The memo could not read it after its fold: not vouched for.
	sup.foldedMarks[e.ID] = foldMemo{mark: indexMark{events: 1000}, publishedAt: snapshotAnchor, indexDSN: e.DSN}
	if ok, why := sup.quietSnapshotCanWait(context.Background(), e, snapshotAnchor, now, time.Hour); ok || why != "where the readers of the previous snapshot start could not be read" {
		t.Fatalf("with a memo that does not know: ok=%v why=%q", ok, why)
	}
	// No memo (a restart): read from the snapshot's files.
	delete(sup.foldedMarks, e.ID)
	stubReadsFrom(t, chainStart, nil)
	if ok, why := sup.quietSnapshotCanWait(context.Background(), e, snapshotAnchor, now, time.Hour); !ok || !graded().Equal(chainStart) {
		t.Fatalf("with no memo: ok=%v why=%q graded=%s, want the instant read from the files", ok, why, graded())
	}
	stubReadsFrom(t, time.Time{}, errors.New("a chain's start could not be read"))
	if ok, why := sup.quietSnapshotCanWait(context.Background(), e, snapshotAnchor, now, time.Hour); ok || why != "where the readers of the previous snapshot start could not be read" {
		t.Fatalf("with files that cannot be read: ok=%v why=%q", ok, why)
	}
	// Without table deltas no chain exists: the snapshot's own instant.
	sup.tableDeltas = false
	if ok, _ := sup.quietSnapshotCanWait(context.Background(), e, snapshotAnchor, now, time.Hour); !ok || !graded().Equal(snapshotAnchor) {
		t.Fatalf("without table deltas: ok=%v graded=%s, want the snapshot's own instant", ok, graded())
	}
	// The rule the gate folds on: the schedule's interval, and this index.
	cov.mu.Lock()
	last := cov.coverages[len(cov.coverages)-1]
	cov.mu.Unlock()
	if last.rule.interval != time.Hour || !last.now.Equal(now) {
		t.Fatalf("coverage asked with %+v at %s, want the schedule's hour at the slot's time", last.rule, last.now)
	}
	_ = b
}

// The known zero (#2659): an index that never held an event. After a daemon
// restart (no memo) the base comes from the run history, where a mark
// recorded as zero now reads as a mark, so the count is a known zero and the
// rule applies. A record from an earlier build, whose zero meant "not read"
// as well, leaves the count unknown, and the line that says so stays true.
func TestMeasureWindow_aRecordedZeroIsAKnownCount_2659(t *testing.T) {
	now := snapshotAnchor.Add(8 * 24 * time.Hour)
	stamp := snapshotAnchor.Format(time.RFC3339)
	for _, c := range []struct {
		name   string
		rec    console.BaselineRunRecord
		events int64
		method string
	}{
		{"a full read that recorded the empty index", console.BaselineRunRecord{Kind: console.BaselineRunDump, StartedAt: stamp, FinishedAt: stamp, SnapshotTime: stamp, IndexMarkKnown: true},
			0, console.BackupMethodSkip},
		{"an update that recorded the empty index", console.BaselineRunRecord{Kind: console.BaselineRunRefresh, StartedAt: stamp, FinishedAt: stamp, SnapshotTime: stamp, IndexMarkKnown: true},
			0, console.BackupMethodSkip},
		{"a record from an earlier build: zero, and no word on whether it was read", console.BaselineRunRecord{Kind: console.BaselineRunDump, StartedAt: stamp, FinishedAt: stamp, SnapshotTime: stamp},
			-1, console.BackupMethodRefresh},
	} {
		t.Run(c.name, func(t *testing.T) {
			b, reg, sup := newScheduleFixture(t, true)
			b.window = b.measureWindow
			e := addScheduled(t, reg, true)
			c.rec.ServerID = e.ID
			if err := sup.history.Append(c.rec); err != nil {
				t.Fatal(err)
			}
			mark := indexMark{} // the index holds no event
			stubIndexMark(t, &mark, true)
			stubCaptureProbe(t, captureProbeResult{detail: positionMode})
			stubCoverage(t, true, true)
			logs := captureSlogFor(t)
			w := b.measureWindow(context.Background(), e, snapshotAnchor)
			if w.Events != c.events {
				t.Fatalf("window = %+v, want events %d", w, c.events)
			}
			method, why := freshDecision(t, b, e, now)
			t.Logf("%s: %s", method, why)
			if method != c.method {
				t.Fatalf("method=%q why=%q, want %q", method, why, c.method)
			}
			line := logLine(logs.String(), countUnknownLine)
			if (c.events < 0) != (line != "") {
				t.Fatalf("the count-unknown line: %q, with events %d", line, c.events)
			}
			if line != "" {
				t.Log(line)
				if !strings.Contains(line, "or an earlier version made it while the index held no event") {
					t.Fatalf("the line %q does not name the cause this record is", line)
				}
			}
			// The first event indexed is counted from that zero.
			if c.events == 0 {
				mark.events = 3
				forgetWindow(b)
				if w := b.measureWindow(context.Background(), e, snapshotAnchor); w.Events != 3 {
					t.Fatalf("after three events: window = %+v", w)
				}
				if method, why := freshDecision(t, b, e, now); method != console.BackupMethodRefresh || why != "no load on your database" {
					t.Fatalf("after three events: method=%q why=%q, want the plain update", method, why)
				}
			}
		})
	}
}

// What the writers record: a full read and an update each say the mark was
// read, also when it read zero.
func TestDumpIndexMark_saysWhetherItWasRead_2659(t *testing.T) {
	_, _, sup := newScheduleFixture(t, true)
	mark := indexMark{}
	stubIndexMark(t, &mark, true)
	if got, known := sup.dumpIndexMark(console.BaselineRequest{IndexDSN: "idx:pw@tcp(127.0.0.1:3306)/idx"}); got != 0 || !known {
		t.Fatalf("an empty index: mark=%d known=%v, want a known zero", got, known)
	}
	mark.events = 9
	if got, known := sup.dumpIndexMark(console.BaselineRequest{IndexDSN: "idx:pw@tcp(127.0.0.1:3306)/idx"}); got != 9 || !known {
		t.Fatalf("mark=%d known=%v, want 9 known", got, known)
	}
	if _, known := sup.dumpIndexMark(console.BaselineRequest{}); known {
		t.Fatal("a request with no index reads as a known mark")
	}
	stubIndexMark(t, &mark, false)
	if _, known := sup.dumpIndexMark(console.BaselineRequest{IndexDSN: "idx:pw@tcp(127.0.0.1:3306)/idx"}); known {
		t.Fatal("an index that did not answer reads as a known mark")
	}
}
