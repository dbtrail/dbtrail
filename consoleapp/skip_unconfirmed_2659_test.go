package consoleapp

import (
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/notify"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// #2659, the remaining case, in the real slot: nothing indexed since an old
// snapshot on a source that cannot be asked whether capture recorded
// everything it wrote. The slot is skipped: no full read, no update, one
// record in the history for the whole quiet stretch and one log line for
// it, and the first indexed change ends it.

const (
	unconfirmedSkipLine   = "no snapshot is made at this slot, nor at the following ones while this lasts"
	unconfirmedUpdateLine = "an update runs at this slot instead of skipping it"
	unconfirmedEndedLine  = "scheduled slots are no longer skipped"
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
	if !strings.Contains(line, "level=INFO") || !strings.Contains(line, positionMode) || !strings.Contains(line, "the copy keeps the data it has") {
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
		{"a capture loss on record", captureProbeResult{detail: captureDetailLost}, "ago, and " + captureDetailLost},
		{"dropped rows no full read has read since", captureProbeResult{detail: captureDetailDropped}, "ago, and " + captureDetailDropped},
		{"the capture holds what the source does not", captureProbeResult{detail: captureDetailDiverged}, "ago, and " + captureDetailDiverged},
		// A loss that cannot be ruled out, and a reason nobody classified.
		{"the record of dropped rows is unreadable", captureProbeResult{detail: captureDetailSkipsUnreadable}, "could not be checked (" + captureDetailSkipsUnreadable + ")"},
		{"an index older than the loss record", captureProbeResult{detail: captureDetailNoLossRecord}, "could not be checked (" + captureDetailNoLossRecord + ")"},
		{"a GTID set that does not parse", captureProbeResult{detail: captureDetailSourceUnparsed}, "could not be checked (" + captureDetailSourceUnparsed + ")"},
		{"a reason added tomorrow", captureProbeResult{detail: "a refusal nobody classified yet"}, "could not be checked (a refusal nobody classified yet)"},
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
			if w.Events != 0 || w.Capture != console.CaptureUnaskable || w.CanWait || w.CannotWaitWhy != c.whyNot {
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

// A skipped slot starts no cycle, and a cycle's quiet gate is what sends a
// snapshot the upload interval (#2596) kept on this machine once its upload
// slot comes. So the skipped slot sends it itself. The schedule runs every
// 15 minutes and sends to S3 every 6 hours: the snapshot of 12:15 is held,
// the slots are skipped from 14:15 (two hours old, nothing indexed, a
// source that cannot be asked), and at 18:00 the upload is due. It is sent
// once, at that slot, and at no slot before or after.
func TestBackupScheduler_aSkippedSlotSendsTheOwedHeldSnapshot_2659(t *testing.T) {
	b, reg, sup := newScheduleFixture(t, true)
	b.window = b.measureWindow
	e := addScheduled(t, reg, true) // a snapshot at snapshotAnchor, 2026-08-20 12:00
	held := snapshotAnchor.Add(15 * time.Minute)
	heldName := reconstruct.SnapshotDirName(held)
	writeSnapshotFiles(t, filepath.Join(e.BaselineDir, heldName), baseline.SuccessMarker)
	if err := baseline.MarkUploadHeld(e.BaselineDir, heldName); err != nil {
		t.Fatal(err)
	}
	e.BaselineS3 = "s3://bkt/p/"
	e.BackupSchedule = &console.BackupSchedule{Every: "15m", At: "00:00", UploadEvery: "6h"}
	if err := reg.Update(e); err != nil {
		t.Fatal(err)
	}
	// The bucket's newest is the one sent at 12:10, in the 12:00 upload
	// slot, until an upload puts a newer one there.
	var sentMu sync.Mutex
	var sent []string
	bucketAt := snapshotAnchor.Add(10 * time.Minute)
	inBucket := func() time.Time {
		sentMu.Lock()
		defer sentMu.Unlock()
		return bucketAt
	}
	realList := listBaselines
	t.Cleanup(func() { listBaselines = realList })
	listBaselines = func(ctx context.Context, src string) ([]reconstruct.BaselineFile, error) {
		if !strings.HasPrefix(src, "s3://") {
			return realList(ctx, src)
		}
		return []reconstruct.BaselineFile{{Schema: "shop", Table: "orders", SnapshotTime: inBucket()}}, nil
	}
	realNewest := reconstruct.NewestSnapshot
	t.Cleanup(console.SetNewestSnapshotForTest(func(ctx context.Context, src string) (time.Time, []string, error) {
		if strings.HasPrefix(src, "s3://") {
			return inBucket(), []string{"shop.orders"}, nil
		}
		return realNewest(ctx, src)
	}))
	prevUpload := uploadSnapshot
	t.Cleanup(func() { uploadSnapshot = prevUpload })
	var whileSending console.BaselineStatus
	uploadSnapshot = func(_ context.Context, dir, dest, _ string, _ bool) (int, error) {
		// What the page is told while the upload runs.
		during := sup.RefreshStatus(e.ID)
		sentMu.Lock()
		defer sentMu.Unlock()
		whileSending = during
		sent = append(sent, dir+" -> "+dest)
		bucketAt = held
		return 1, nil
	}
	uploads := func() int {
		sentMu.Lock()
		defer sentMu.Unlock()
		return len(sent)
	}
	sup.foldedMarks[e.ID] = foldMemo{mark: indexMark{events: 1000}, publishedAt: held, indexDSN: e.DSN,
		destination: refreshDestination(b.rebuildRequest(e, mustParse(t, e))), readsFrom: held, readsFromKnown: true}
	mark := indexMark{events: 1000}
	stubIndexMark(t, &mark, true)
	stubCaptureProbe(t, captureProbeResult{detail: positionMode})
	stubCoverage(t, true, true)
	var folds atomic.Int32
	countFolds(t, &folds)
	logs := captureSlogFor(t)

	day := time.Date(2026, 8, 20, 0, 0, 5, 0, time.UTC)
	at := func(h, m int) time.Time { return day.Add(time.Duration(h)*time.Hour + time.Duration(m)*time.Minute) }
	b.tick(context.Background(), at(14, 0)) // the observation; nothing fires
	want := console.SkipUnconfirmedReason(positionMode)
	for slot := at(14, 15); !slot.After(at(18, 45)); slot = slot.Add(15 * time.Minute) {
		forgetWindow(b)
		b.tick(context.Background(), slot)
		sup.postRefresh.Wait()
		st := b.ScheduleState(e.ID)
		if st.LastSkipReason != want || st.LastStartedAt != "" {
			t.Fatalf("%s: state %+v, want the slot skipped and no job started", slot.Format("15:04"), st)
		}
		wantSent := 0
		if !slot.Before(at(18, 0)) {
			wantSent = 1
		}
		if got := uploads(); got != wantSent {
			t.Fatalf("%s: %d uploads so far (%v), want %d: the held snapshot goes once, when the 18:00 upload slot comes", slot.Format("15:04"), got, sent, wantSent)
		}
		// The slot is given back each time: nothing is left running.
		if rs := sup.RefreshStatus(e.ID); rs.State == "running" {
			t.Fatalf("%s: the refresh slot was left claimed: %+v", slot.Format("15:04"), rs)
		}
	}
	if wantDest := filepath.Join(e.BaselineDir, heldName) + " -> s3://bkt/p/" + heldName; sent[0] != wantDest {
		t.Fatalf("sent %q, want %q", sent[0], wantDest)
	}
	if stillHeld, _ := baseline.UploadHeld(e.BaselineDir, heldName); stillHeld {
		t.Fatal("the mark outlived the upload")
	}
	// While it ran the slot was busy and said what it was busy with: a
	// send, not an update. Afterwards neither.
	if whileSending.State != "running" || !whileSending.SendingHeld {
		t.Fatalf("the status during the upload: %+v, want running and marked as sending a kept snapshot", whileSending)
	}
	if after := sup.RefreshStatus(e.ID); after.State == "running" || after.SendingHeld {
		t.Fatalf("the status after the upload: %+v", after)
	}
	if folds.Load() != 0 || lastScheduledDump(sup, e.ID) != nil {
		t.Fatalf("%d folds, a full read %+v: the upload must not start a run", folds.Load(), lastScheduledDump(sup, e.ID))
	}
	line := logLine(logs.String(), "sent the newest snapshot the upload interval had kept on this machine")
	t.Logf("log: %s", line)
	if line == "" || strings.Count(logs.String(), "sent the newest snapshot the upload interval had kept on this machine") != 1 {
		t.Fatalf("the upload was not said once: %q", logs.String())
	}
}

// What sendOwedForSkippedSlot does not do: nothing for a server with no S3
// destination (the slot is not even taken), nothing to send when the newest
// snapshot is not held, and nothing at all while another snapshot job holds
// the server.
func TestSendOwedForSkippedSlot_whenThereIsNothingToSend_2659(t *testing.T) {
	var sent atomic.Int32
	prevUpload := uploadSnapshot
	t.Cleanup(func() { uploadSnapshot = prevUpload })
	uploadSnapshot = func(context.Context, string, string, string, bool) (int, error) {
		sent.Add(1)
		return 1, nil
	}
	local := t.TempDir()
	newest := time.Date(2026, 11, 1, 10, 5, 0, 0, time.UTC)
	name := reconstruct.SnapshotDirName(newest)
	writeSnapshotFiles(t, filepath.Join(local, name), baseline.SuccessMarker)
	stubBucketNewest(t, newest.Add(-7*time.Hour), nil)
	now := newest.Add(7 * time.Hour)
	s := newBaselineSupervisor(context.Background(), t.TempDir(), "")
	req := refreshRequest{ServerID: "s", ServerName: "s", BaselineDir: local, BaselineS3: "s3://bkt/p/", UploadEvery: 6 * time.Hour}

	noS3 := req
	noS3.BaselineS3 = ""
	if s.sendOwedForSkippedSlot(noS3, now) {
		t.Fatal("a server with no S3 destination took the refresh slot")
	}
	// Not held: the slot is taken and given back, and nothing is sent.
	if !s.sendOwedForSkippedSlot(req, now) {
		t.Fatal("the slot was not taken on a free server")
	}
	s.postRefresh.Wait()
	if sent.Load() != 0 || s.RefreshStatus("s").State == "running" {
		t.Fatalf("nothing held: %d uploads, status %+v", sent.Load(), s.RefreshStatus("s"))
	}
	// Held and due, but a full read is running: left for the next slot.
	if err := baseline.MarkUploadHeld(local, name); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	s.jobs["s"] = &console.BaselineStatus{State: "running", Since: "2026-11-01T17:00:00Z"}
	s.mu.Unlock()
	if s.sendOwedForSkippedSlot(req, now) {
		t.Fatal("the slot was taken while a full read was running")
	}
	s.postRefresh.Wait()
	if sent.Load() != 0 {
		t.Fatalf("%d uploads beside a running full read", sent.Load())
	}
	// The same server once the full read is done: sent, once, and the
	// status the slot held before is put back.
	s.mu.Lock()
	delete(s.jobs, "s")
	prior := &console.BaselineStatus{State: "succeeded", Since: "2026-11-01T10:05:00Z"}
	s.refreshes["s"] = prior
	s.mu.Unlock()
	if !s.sendOwedForSkippedSlot(req, now) {
		t.Fatal("the slot was not taken")
	}
	s.postRefresh.Wait()
	if rs := s.RefreshStatus("s"); sent.Load() != 1 || rs.State != "succeeded" || rs.Since != prior.Since {
		t.Fatalf("held and due: %d uploads, status %+v, want one upload and the previous status back", sent.Load(), rs)
	}
}

// The log over a stretch that a re-anchoring update interrupts reads as it
// happened: a start, an end that says the snapshot is updated because it
// cannot wait, and a new start when the skipping resumes. Never two starts
// with no end between them.
func TestBackupScheduler_theLogOfAStretchInterruptedByAnUpdate_2659(t *testing.T) {
	b, sup, e, _, folds := quietUnconfirmed(t, captureProbeResult{detail: "PostgreSQL sources are not compared yet"})
	covered := true
	prevCov := snapshotStillCovered
	t.Cleanup(func() { snapshotStillCovered = prevCov })
	snapshotStillCovered = func(context.Context, string, time.Time, time.Time, coverageRule) (bool, bool) { return covered, true }
	memo := sup.foldedMarks[e.ID]
	logs := captureSlogFor(t)
	t0 := time.Date(2026, 8, 28, 9, 0, 5, 0, time.UTC)
	b.tick(context.Background(), t0)
	slot := func(i int) {
		t.Helper()
		// The fold seam publishes nothing: the memo is put back on the
		// snapshot that is still the newest.
		sup.mu.Lock()
		sup.foldedMarks[e.ID] = memo
		sup.mu.Unlock()
		forgetWindow(b)
		b.tick(context.Background(), t0.Add(time.Duration(i)*time.Hour))
	}
	slot(1) // skipped: the start
	slot(2) // skipped: nothing said
	covered = false
	slot(3) // the snapshot nears what the index keeps: the update
	waitTerminalMethod(t, b, e.ID, console.BackupMethodRefresh)
	covered = true
	slot(4) // skipped again: a new start
	slot(5)
	if folds.Load() != 1 {
		t.Fatalf("%d folds, want the one re-anchoring update", folds.Load())
	}
	const endedByUpdate = "scheduled slots are no longer skipped: an update runs at this slot instead"
	var seq []string
	for _, l := range strings.Split(logs.String(), "\n") {
		switch {
		case strings.Contains(l, unconfirmedSkipLine):
			seq = append(seq, "start")
		case strings.Contains(l, endedByUpdate):
			seq = append(seq, "end-by-update")
			t.Logf("log: %s", l)
			// True whether the update publishes a snapshot or its own gate
			// finds nothing to add and publishes none.
			for _, want := range []string{"If it publishes a snapshot, slots are skipped again once that snapshot is old enough", "if it finds nothing to add, from the next slot",
				"only while nothing is indexed and the source still cannot be asked", "close to the oldest change the index still keeps"} {
				if !strings.Contains(l, want) {
					t.Errorf("the end line lacks %q: %q", want, l)
				}
			}
			if strings.Contains(l, "the snapshot is updated at this slot") || strings.Contains(l, "once the new snapshot") {
				t.Errorf("the end line promises a snapshot the update may not publish: %q", l)
			}
		case strings.Contains(l, unconfirmedEndedLine):
			seq = append(seq, "end")
		case strings.Contains(l, unconfirmedUpdateLine):
			seq = append(seq, "update")
		}
	}
	if got := strings.Join(seq, ", "); got != "start, end-by-update, start" {
		t.Fatalf("the log reads %q, want start, end-by-update, start", got)
	}
}

// captureReasonFuncs are the functions that return a reason and nothing
// else that could be mistaken for one: the pure halves of the probe. Their
// returns are checked below, and a probe result may take its detail from a
// call to one of them.
var captureReasonFuncs = map[string]bool{
	"captureComparable": true, "checkpointComparable": true, "compareGTIDSets": true,
	"gtidModeRefusal": true, "compareMariaDBPositions": true,
}

// Every reason the probe can return is a constant with a decision
// (captureClasses), and no code path returns a reason written as a bare
// string. A reason added without a decision fails here, and until then it
// gets the default: the full read, never the skip. Read from EVERY file of
// the package that ships, not from the probe's file alone: a constant or a
// probe result declared anywhere else is held to the same rule.
func TestCaptureClasses_everyReasonIsDecided_2659(t *testing.T) {
	type decided struct {
		text  string
		class captureClass
	}
	want := map[string]decided{
		// The source cannot be asked: the slot is skipped.
		"captureDetailPostgres":         {"PostgreSQL sources are not compared yet", captureCannotAsk},
		"captureDetailNoSource":         {"this server has no source to ask", captureCannotAsk},
		"captureDetailPositionMode":     {"the capture runs in binlog-position mode, which is not compared", captureCannotAsk},
		"captureDetailNoCheckpoint":     {"the capture has no GTID checkpoint", captureCannotAsk},
		"captureDetailNoGTIDMode":       {"the source has no gtid_mode (a MariaDB source), which is not compared", captureCannotAsk},
		"captureDetailSameServer":       {"the index lives on the source server, whose own checkpoint writes keep the source ahead of the capture", captureCannotAsk},
		"captureDetailNotToldApart":     {"the index and the source could not be told apart", captureCannotAsk},
		"captureDetailSourceSilent":     {"the source did not answer", captureCannotAsk},
		"captureDetailSourceGTIDUnread": {"the source did not report its GTID set", captureCannotAsk},
		"captureDetailSourceSlow":       {"the source did not answer in time", captureCannotAsk},
		"captureDetailNoCapture":        {"the index has no live capture on record", captureCannotAsk},
		// Evidence that changes are missing, or are about to be: the full read.
		"captureDetailLost":              {"the capture lost events to a binlog gap after the previous snapshot", captureEvidence},
		"captureDetailDropped":           {"the capture dropped events that no full read has read from the source since", captureEvidence},
		"captureDetailDiverged":          {"the capture holds transactions the source does not have: the source may have been reset, restored or replaced", captureEvidence},
		"captureDetailBehind":            {"the source reports transactions the capture's checkpoint does not include", captureEvidence},
		"captureDetailGTIDModeOff":       {"GTIDs are off on the source (gtid_mode=OFF) while capture follows it by GTID, so capture cannot read from it", captureEvidence},
		"captureDetailGTIDOffPermissive": {"the source's gtid_mode is OFF_PERMISSIVE, not ON, while capture follows it by GTID, so capture cannot read from it", captureEvidence},
		"captureDetailGTIDOnPermissive":  {"the source's gtid_mode is ON_PERMISSIVE, not ON, while capture follows it by GTID, so capture cannot read from it", captureEvidence},
		"captureDetailGTIDModeOther":     {"the source's gtid_mode is not ON while capture follows it by GTID, so capture cannot read from it", captureEvidence},
		// A loss cannot be ruled out: the full read.
		"captureDetailSkipsUnreadable": {"the capture's record of dropped events is not readable", captureUnsettled},
		"captureDetailNoLossRecord":    {"the index predates the capture's loss record", captureUnsettled},
		"captureDetailCaptureUnparsed": {"the capture's GTID set does not parse", captureUnsettled},
		"captureDetailSourceUnparsed":  {"the source's GTID set does not parse", captureUnsettled},
		// The index did not give the capture's record: the update.
		"captureDetailIndexSilent":      {"the index did not answer", captureIndexUnread},
		"captureDetailIndexSlow":        {"the index did not answer in time", captureIndexUnread},
		"captureDetailCheckpointUnread": {"the capture's checkpoint could not be read", captureIndexUnread},
		"captureDetailCancelled":        {"the request that asked was cancelled", captureIndexUnread},
		"captureDetailTimedOut":         {"the probe did not finish in time", captureIndexUnread},
		// The capture status read's own, never returned to the schedule:
		// decided like a reason nobody listed, the full read.
		"captureDetailMariaDBNoPositionVar":  {"the source has no MariaDB GTID position: is it a MariaDB server?", captureUnsettled},
		"captureDetailMariaDBPositionUnread": {"the source did not report its GTID position", captureUnsettled},
		"captureDetailMariaDBNoPosition":     {"the source reported no GTID position", captureUnsettled},
		"captureDetailMariaDBSourceUnparsed": {"the source's GTID position does not parse", captureUnsettled},
		"captureDetailMariaDBAhead":          {"the source is ahead of the capture's checkpoint; a MariaDB source is only compared for an exact match", captureUnsettled},
		"captureDetailMariaDBMismatch":       {"the capture's checkpoint and the source's GTID position do not match: the source may have been reset, restored or replaced", captureUnsettled},
		"captureDetailPanicked":              {"the source could not be read", captureUnsettled},
	}
	fset := token.NewFileSet()
	names, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	var files []*ast.File
	for _, name := range names {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, f)
	}
	if len(files) < 20 {
		t.Fatalf("only %d files of the package were read", len(files))
	}
	where := func(n ast.Node) string { return fset.Position(n.Pos()).String() }
	// 1. The constants in the code, in whatever file, are exactly the ones
	// decided here, with these texts and these classes, and each is in the
	// table explicitly.
	declared := map[string]string{}
	for _, f := range files {
		ast.Inspect(f, func(n ast.Node) bool {
			vs, ok := n.(*ast.ValueSpec)
			if !ok {
				return true
			}
			for i, name := range vs.Names {
				if !strings.HasPrefix(name.Name, "captureDetail") {
					continue
				}
				if i >= len(vs.Values) {
					t.Errorf("%s: %s has no value of its own", where(vs), name.Name)
					continue
				}
				lit, ok := vs.Values[i].(*ast.BasicLit)
				if !ok {
					t.Errorf("%s: %s is not a plain string constant", where(vs), name.Name)
					continue
				}
				text, err := strconv.Unquote(lit.Value)
				if err != nil {
					t.Fatal(err)
				}
				if _, twice := declared[name.Name]; twice {
					t.Errorf("%s: %s is declared twice", where(vs), name.Name)
				}
				declared[name.Name] = text
			}
			return true
		})
	}
	for name, text := range declared {
		w, ok := want[name]
		if !ok {
			t.Errorf("%s (%q) is a new reason with no decision in this test: decide whether a quiet slot is skipped, read in full or updated on it", name, text)
			continue
		}
		if w.text != text {
			t.Errorf("%s reads %q, decided here as %q", name, text, w.text)
		}
		class, listed := captureClasses[text]
		if !listed {
			t.Errorf("%s (%q) is not in captureClasses: it gets the full read by default, and nobody decided that", name, text)
		}
		if class != w.class || captureClassOf(text) != w.class {
			t.Errorf("%s (%q) is classed %d, decided here as %d", name, text, class, w.class)
		}
	}
	for name := range want {
		if _, ok := declared[name]; !ok {
			t.Errorf("%s is decided here and declared in no file of the package", name)
		}
	}
	if len(captureClasses) != len(want) {
		t.Errorf("captureClasses holds %d reasons, %d are decided here", len(captureClasses), len(want))
	}
	// 2. A reason that is in no table, one added tomorrow, is never a skip.
	if got := captureClassOf("a refusal nobody classified yet"); got != captureUnsettled || captureUnsettled != 0 {
		t.Fatalf("an unknown reason is classed %d; the zero value must be the full read", got)
	}
	// 3. No reason is written as a bare string, in any file: every detail
	// of a probe result (in its literal, or assigned to its field) is one
	// of the constants, or a variable that the same function filled from a
	// call to one of the reason functions; and each of those functions
	// returns constants only.
	isConst := func(e ast.Expr) bool {
		id, ok := e.(*ast.Ident)
		return ok && strings.HasPrefix(id.Name, "captureDetail")
	}
	// fromReasonFunc reports whether the identifier name is assigned, in
	// fn, from a call to a reason function and from nothing else.
	fromReasonFunc := func(fn *ast.FuncDecl, name string) bool {
		good, bad := 0, 0
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			as, ok := n.(*ast.AssignStmt)
			if !ok {
				return true
			}
			for _, lhs := range as.Lhs {
				id, ok := lhs.(*ast.Ident)
				if !ok || id.Name != name {
					continue
				}
				call, ok := as.Rhs[0].(*ast.CallExpr)
				callee, isIdent := ast.Expr(nil), false
				if ok {
					callee = call.Fun
					_, isIdent = callee.(*ast.Ident)
				}
				if ok && isIdent && len(as.Rhs) == 1 && captureReasonFuncs[callee.(*ast.Ident).Name] {
					good++
				} else {
					bad++
				}
			}
			return true
		})
		return good > 0 && bad == 0
	}
	okDetail := func(fn *ast.FuncDecl, e ast.Expr) bool {
		if isConst(e) {
			return true
		}
		id, ok := e.(*ast.Ident)
		return ok && fn != nil && fromReasonFunc(fn, id.Name)
	}
	results, returns := 0, 0
	for _, f := range files {
		for _, decl := range f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				switch x := n.(type) {
				case *ast.CompositeLit:
					if id, ok := x.Type.(*ast.Ident); !ok || id.Name != "captureProbeResult" {
						return true
					}
					for _, el := range x.Elts {
						kv, ok := el.(*ast.KeyValueExpr)
						if !ok {
							t.Errorf("%s: a probe result built without field names", where(x))
							continue
						}
						if k, ok := kv.Key.(*ast.Ident); ok && k.Name == "detail" {
							results++
							if !okDetail(fn, kv.Value) {
								t.Errorf("%s: in %s, a probe result's detail is neither a captureDetail constant nor the answer of a reason function", where(kv), fn.Name.Name)
							}
						}
					}
				case *ast.AssignStmt:
					for i, lhs := range x.Lhs {
						sel, ok := lhs.(*ast.SelectorExpr)
						if !ok || sel.Sel.Name != "detail" || i >= len(x.Rhs) {
							continue
						}
						results++
						if !okDetail(fn, x.Rhs[i]) {
							t.Errorf("%s: in %s, a .detail is assigned something that is neither a captureDetail constant nor the answer of a reason function", where(x), fn.Name.Name)
						}
					}
				case *ast.ReturnStmt:
					if !captureReasonFuncs[fn.Name.Name] || len(x.Results) == 0 {
						return true
					}
					returns++
					last := x.Results[len(x.Results)-1]
					if lit, ok := last.(*ast.BasicLit); ok && lit.Value == `""` {
						return true
					}
					// `return checkpointComparable(st)`: another reason function.
					if call, ok := last.(*ast.CallExpr); ok && len(x.Results) == 1 {
						if id, ok := call.Fun.(*ast.Ident); ok && captureReasonFuncs[id.Name] {
							return true
						}
					}
					if !isConst(last) {
						t.Errorf("%s: %s returns a reason that is not a captureDetail constant", where(x), fn.Name.Name)
					}
				}
				return true
			})
		}
	}
	for name := range captureReasonFuncs {
		found := false
		for _, f := range files {
			for _, decl := range f.Decls {
				if fn, ok := decl.(*ast.FuncDecl); ok && fn.Name.Name == name {
					found = true
				}
			}
		}
		if !found {
			t.Errorf("the reason function %s is gone; this check names a function that no longer exists", name)
		}
	}
	if results < 20 || returns < 20 {
		t.Fatalf("%d probe results and %d returns of reason functions were found; this check no longer reads the code it guards", results, returns)
	}
}

// Each class, through the schedule's own probe and decision: what the slot
// does, for every reason there is.
func TestCaptureClasses_whatTheSlotDoes_2659(t *testing.T) {
	now := snapshotAnchor.Add(8 * 24 * time.Hour)
	for text, class := range captureClasses {
		if text == captureDetailBehind {
			continue // a verdict of its own, tested beside the evidence
		}
		t.Run(text, func(t *testing.T) {
			b, _, e, _, _ := quietUnconfirmed(t, captureProbeResult{detail: text})
			stubCoverage(t, true, true)
			method, why := freshDecision(t, b, e, now)
			t.Logf("%s: %s", method, why)
			wantMethod := map[captureClass]string{captureCannotAsk: console.BackupMethodSkip, captureEvidence: console.BackupMethodFull,
				captureUnsettled: console.BackupMethodFull, captureIndexUnread: console.BackupMethodRefresh}[class]
			if method != wantMethod {
				t.Fatalf("method=%q why=%q, want %q", method, why, wantMethod)
			}
			switch class {
			case captureEvidence:
				if !strings.HasSuffix(why, "ago, and "+text) {
					t.Errorf("evidence is not worded as what was found: %q", why)
				}
			case captureUnsettled:
				if !strings.HasSuffix(why, "could not be checked ("+text+")") {
					t.Errorf("an unsettled reason reads %q", why)
				}
			case captureIndexUnread:
				if why != console.IndexUnreadUpdateWhy(text) || strings.Contains(why, "the source cannot confirm") {
					t.Errorf("the update's why reads %q", why)
				}
			case captureCannotAsk:
				if text == captureDetailNoCapture {
					if !strings.Contains(why, "(the index has no live capture on record: no streaming capture has run against this index; if you expect one, check that it is running)") {
						t.Errorf("the reason for an index with no capture does not say what to check: %q", why)
					}
				} else if why != console.SkipUnconfirmedReason(text) {
					t.Errorf("the skip's reason reads %q", why)
				}
			}
		})
	}
	// A reason that is in no table: the full read.
	b, _, e, _, _ := quietUnconfirmed(t, captureProbeResult{detail: "a refusal nobody classified yet"})
	stubCoverage(t, true, true)
	if method, why := freshDecision(t, b, e, now); method != console.BackupMethodFull || console.BackupWhyCode(why) != "window_age" {
		t.Fatalf("an unclassified reason: method=%q why=%q, want the full read", method, why)
	}
	// The two the schedule gives without trying the probe.
	b, _, e, _, _ = quietUnconfirmed(t, captureProbeResult{verdict: console.CaptureCaughtUp})
	stubCoverage(t, true, true)
	pg := e
	pg.Flavor = "postgres"
	if !pg.IsPostgres() {
		t.Fatal("the fixture's PostgreSQL entry is not one")
	}
	if method, why := freshDecision(t, b, pg, now); method != console.BackupMethodSkip || why != console.SkipUnconfirmedReason(captureDetailPostgres) {
		t.Fatalf("a PostgreSQL source: method=%q why=%q", method, why)
	}
	none := e
	none.SourceDSN = ""
	if method, why := freshDecision(t, b, none, now); method != console.BackupMethodSkip || why != console.SkipUnconfirmedReason(captureDetailNoSource) {
		t.Fatalf("a server with no source: method=%q why=%q", method, why)
	}
}

// A slow index is not a broken one: when the budget the reads share runs
// out, the update's reason says the index did not answer in time, not that
// something could not be read.
func TestQuietSnapshotCanWait_saysWhenTheIndexWasSlow_2659(t *testing.T) {
	_, sup, e, _, _ := quietUnconfirmed(t, captureProbeResult{detail: positionMode})
	prevCov := snapshotStillCovered
	t.Cleanup(func() { snapshotStillCovered = prevCov })
	// The coverage read outlives the budget.
	snapshotStillCovered = func(ctx context.Context, _ string, _, _ time.Time, _ coverageRule) (bool, bool) {
		<-ctx.Done()
		return false, false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	ok, why := sup.quietSnapshotCanWait(ctx, e, snapshotAnchor, snapshotAnchor.Add(8*24*time.Hour), time.Hour)
	t.Log(why)
	if ok || why != "the index did not answer within 3s, so how far back the index still keeps changes is not known" {
		t.Fatalf("ok=%v why=%q", ok, why)
	}
	// The same read failing at once is said as a failed read.
	snapshotStillCovered = func(context.Context, string, time.Time, time.Time, coverageRule) (bool, bool) { return false, false }
	ok, why = sup.quietSnapshotCanWait(context.Background(), e, snapshotAnchor, snapshotAnchor.Add(8*24*time.Hour), time.Hour)
	if ok || why != "how far back the index still keeps changes could not be read" {
		t.Fatalf("ok=%v why=%q", ok, why)
	}
}
