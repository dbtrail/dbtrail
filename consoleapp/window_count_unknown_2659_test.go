package consoleapp

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/console"
)

// #2659: what the schedule logs about a window it could not measure, pinned
// against what the schedule then does. Each sentence that names an action is
// checked beside the real decision (console.ChooseBackupMethodAt over the
// scheduler's own gates and probe), so a sentence cannot claim a full read
// the slot does not take, or an update where a full read is chosen.

// freshDecision is the slot's decision for e with nothing cached.
func freshDecision(t *testing.T, b *backupScheduler, e console.ServerEntry, now time.Time) (method, why string) {
	t.Helper()
	b.mu.Lock()
	b.windows = nil
	b.mu.Unlock()
	method, why, err := console.ChooseBackupMethodAt(context.Background(), e, b.gates(), now)
	if err != nil {
		t.Fatalf("the decision failed: %v", err)
	}
	return method, why
}

// logLine is the one captured log line that carries part, for reading.
func logLine(out, part string) string {
	for _, l := range strings.Split(out, "\n") {
		if strings.Contains(l, part) {
			return l
		}
	}
	return ""
}

const countUnknownLine = "the number of changes since the previous snapshot could not be established"

// An unknown count on an old snapshot used to be a full read whose reason
// said the count was missing. Now the update runs, and each cause of the
// count being unknown says so once: the count could not be established,
// and the update runs without an estimate.
func TestMeasureWindow_anUnknownCountIsSaidAndTheUpdateRuns_2659(t *testing.T) {
	now := snapshotAnchor.Add(8 * 24 * time.Hour) // far past the hourly schedule's six-hour cut-over age
	cases := []struct {
		name  string
		setup func(t *testing.T, sup *baselineSupervisor, e console.ServerEntry)
		why   string
		level string
	}{
		{"no mark on record for that snapshot (history lost, or another process made it)",
			func(*testing.T, *baselineSupervisor, console.ServerEntry) {},
			countUnknownNoBase, "level=INFO"},
		{"the server was pointed at another index",
			func(_ *testing.T, sup *baselineSupervisor, e console.ServerEntry) {
				sup.foldedMarks[e.ID] = foldMemo{mark: indexMark{events: 1000}, publishedAt: snapshotAnchor, indexDSN: "another:pw@tcp(127.0.0.1:3306)/idx"}
			},
			countUnknownOtherIndex, "level=WARN"},
		{"the index's mark went backwards (rebuilt)",
			func(t *testing.T, sup *baselineSupervisor, e console.ServerEntry) {
				sup.foldedMarks[e.ID] = foldMemo{mark: indexMark{events: 1000}, publishedAt: snapshotAnchor, indexDSN: e.DSN}
				mark := indexMark{events: 400}
				stubIndexMark(t, &mark, true)
			},
			countUnknownWentBackwards, "level=WARN"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			b, reg, sup := newScheduleFixture(t, true)
			b.window = b.measureWindow
			e := addScheduled(t, reg, true)
			c.setup(t, sup, e)
			logs := captureSlogFor(t)
			for range 3 {
				if w := b.measureWindow(context.Background(), e, snapshotAnchor); w.Events != -1 {
					t.Fatalf("window = %+v, want the count unknown", w)
				}
				// The action the line names: the update, on a snapshot eight
				// days old with nothing measured.
				if method, why := freshDecision(t, b, e, now); method != console.BackupMethodRefresh {
					t.Fatalf("method=%q why=%q, want the update", method, why)
				}
			}
			out := logs.String()
			line := logLine(out, countUnknownLine)
			t.Log(line)
			if strings.Count(out, countUnknownLine) != 1 {
				t.Fatalf("log = %q, want the line exactly once", out)
			}
			for _, want := range []string{c.level, c.why, "the schedule's choice is the update, with no estimate of what it will cost", "previous_backup=" + snapshotAnchor.Format(time.RFC3339)} {
				if !strings.Contains(line, want) {
					t.Errorf("the line %q lacks %q", line, want)
				}
			}
			if strings.Contains(line, "no full read is chosen") {
				t.Errorf("the line %q rules out a full read the fixed-cost rule can still choose", line)
			}
		})
	}
}

// The condition ends when the count is known again, and a different cause
// under the same server is said as a new one.
func TestMeasureWindow_theUnknownCountLineResolvesAndNamesANewCause_2659(t *testing.T) {
	b, reg, sup := newScheduleFixture(t, true)
	b.window = b.measureWindow
	e := addScheduled(t, reg, true)
	sup.foldedMarks[e.ID] = foldMemo{mark: indexMark{events: 1000}, publishedAt: snapshotAnchor, indexDSN: e.DSN}
	mark := indexMark{events: 400}
	stubIndexMark(t, &mark, true)
	logs := captureSlogFor(t)
	probe := func() console.BackupWindow {
		b.mu.Lock()
		b.windows = nil
		b.mu.Unlock()
		return b.measureWindow(context.Background(), e, snapshotAnchor)
	}
	probe()
	key := "window-count:" + e.ID
	if !b.sup.gateEdge.Active(key) {
		t.Fatal("a mark that went backwards raised no condition")
	}
	mark = indexMark{events: 1500}
	if w := probe(); w.Events != 500 || b.sup.gateEdge.Active(key) {
		t.Fatalf("with the count known again: window=%+v active=%v", w, b.sup.gateEdge.Active(key))
	}
	// Re-pointed at another index: a new cause, said at once.
	sup.mu.Lock()
	sup.foldedMarks[e.ID] = foldMemo{mark: indexMark{events: 1000}, publishedAt: snapshotAnchor, indexDSN: "another"}
	sup.mu.Unlock()
	probe()
	out := logs.String()
	if strings.Count(out, countUnknownLine) != 2 || !strings.Contains(out, countUnknownWentBackwards) || !strings.Contains(out, countUnknownOtherIndex) {
		t.Fatalf("log = %q, want one line per cause", out)
	}
	// No snapshot to count from is not a condition: nothing is said.
	before := strings.Count(logs.String(), countUnknownLine)
	b.measureWindow(context.Background(), console.ServerEntry{ID: "new", Name: "new", DSN: "idx"}, time.Time{})
	if got := strings.Count(logs.String(), countUnknownLine); got != before {
		t.Fatalf("a server with no snapshot logged the line: %q", logs.String())
	}
}

// An index that does not answer the probe leaves the count unknown too: the
// update runs, and the line says that, not that no full read can follow.
func TestMeasureWindow_aSilentIndexIsSaidAndTheUpdateRuns_2659(t *testing.T) {
	b, reg, sup := newScheduleFixture(t, true)
	b.window = b.measureWindow
	e := addScheduled(t, reg, true)
	sup.foldedMarks[e.ID] = foldMemo{mark: indexMark{events: 1000}, publishedAt: snapshotAnchor, indexDSN: e.DSN}
	mark := indexMark{}
	stubIndexMark(t, &mark, false)
	logs := captureSlogFor(t)
	if method, why := freshDecision(t, b, e, snapshotAnchor.Add(8*24*time.Hour)); method != console.BackupMethodRefresh {
		t.Fatalf("method=%q why=%q, want the update", method, why)
	}
	line := logLine(logs.String(), "did not answer the update-size probe")
	t.Log(line)
	for _, want := range []string{"level=WARN", "so the number of changes since the previous snapshot is unknown", "the schedule's choice is the update, with no estimate of what it will cost",
		"the exception is a server whose cheapest update measured since its last full read took clearly longer than that read, where a full read is still chosen"} {
		if !strings.Contains(line, want) {
			t.Errorf("the line %q lacks %q", line, want)
		}
	}
	if strings.Contains(line, "no full read is chosen") || strings.Contains(logs.String(), countUnknownLine) {
		t.Errorf("log = %q: a full read ruled out, or the same condition said twice", logs.String())
	}
}

// The exception both lines name is real: with the count unknown, a server
// with no per-event rate whose cheapest update since its last full read took clearly longer
// than its last full read still gets the full read, on that measurement.
func TestMeasureWindow_anUnknownCountStillCutsOverOnTheFixedCost_2659(t *testing.T) {
	for _, silent := range []bool{false, true} {
		b, reg, sup := newScheduleFixture(t, true)
		b.window = b.measureWindow
		e := addScheduled(t, reg, true)
		for _, rec := range []console.BaselineRunRecord{
			{ServerID: e.ID, Kind: console.BaselineRunDump, StartedAt: "2026-08-19T05:00:00Z", FinishedAt: "2026-08-19T05:02:00Z"},
			// Two updates of the same size and about the same six minutes:
			// a fixed cost, and no per-event rate to read off them.
			{ServerID: e.ID, Kind: console.BaselineRunRefresh, Events: 1000, UpdateSeconds: 360},
			{ServerID: e.ID, Kind: console.BaselineRunRefresh, Events: 1000, UpdateSeconds: 361},
		} {
			if err := sup.history.Append(rec); err != nil {
				t.Fatal(err)
			}
		}
		line := countUnknownLine
		if silent {
			sup.foldedMarks[e.ID] = foldMemo{mark: indexMark{events: 1000}, publishedAt: snapshotAnchor, indexDSN: e.DSN}
			mark := indexMark{}
			stubIndexMark(t, &mark, false)
			line = "did not answer the update-size probe"
		}
		logs := captureSlogFor(t)
		w := b.measureWindow(context.Background(), e, snapshotAnchor)
		if w.Events != -1 || w.FoldRate != 0 || w.FoldFixed != 6*time.Minute || w.LastFull != 2*time.Minute {
			t.Fatalf("silent=%v: window = %+v, want an unknown count, no rate, six-minute updates and a two-minute full read", silent, w)
		}
		method, why := freshDecision(t, b, e, snapshotAnchor.Add(8*24*time.Hour))
		if method != console.BackupMethodFull || console.BackupWhyCode(why) != "window_measured" {
			t.Fatalf("silent=%v: method=%q why=%q, want the full read on the measured fixed cost", silent, method, why)
		}
		t.Log(why)
		if got := logLine(logs.String(), line); !strings.Contains(got, "where a full read is still chosen") {
			t.Fatalf("silent=%v: the line %q does not name the exception the slot just took", silent, got)
		}
	}
}

// The capture probe's own lines say what the source answered and no more.
// On a server with a measured rate, nothing indexed since an old snapshot
// and a source that is ahead, the slot is an UPDATE (the estimate is the
// fixed cost, under the last full read), so a line saying a full read is
// taken would be false there.
func TestReportCaptureProbe_saysWhatWasFoundNotWhatTheSlotDoes_2659(t *testing.T) {
	now := snapshotAnchor.Add(8 * time.Hour) // an hourly schedule: past its six-hour cut-over age
	const found = "the source does not confirm that capture recorded everything it wrote, so the index may be missing changes"
	for _, c := range []struct {
		name     string
		withRate bool
		probe    captureProbeResult
		level    string
		method   string
	}{
		{"a measured rate, the source ahead: found, and the slot updates", true,
			captureProbeResult{verdict: console.CaptureBehind, detail: "the source reports transactions the capture's checkpoint does not include"}, "level=WARN", console.BackupMethodRefresh},
		{"a measured rate, the source cannot be asked: found, and the slot updates", true,
			captureProbeResult{detail: "the capture runs in binlog-position mode, which is not compared"}, "level=INFO", console.BackupMethodRefresh},
		{"no rate, the source ahead: found, and the slot reads in full", false,
			captureProbeResult{verdict: console.CaptureBehind, detail: "the source reports transactions the capture's checkpoint does not include"}, "level=WARN", console.BackupMethodFull},
	} {
		t.Run(c.name, func(t *testing.T) {
			b, reg, sup := newScheduleFixture(t, true)
			b.window = b.measureWindow
			e := addScheduled(t, reg, true)
			if c.withRate {
				for _, rec := range []console.BaselineRunRecord{
					{ServerID: e.ID, Kind: console.BaselineRunDump, StartedAt: "2026-08-20T05:00:00Z", FinishedAt: "2026-08-20T05:08:00Z"},
					{ServerID: e.ID, Kind: console.BaselineRunRefresh, Events: 1000, UpdateSeconds: 60},
					{ServerID: e.ID, Kind: console.BaselineRunRefresh, Events: 241_000, UpdateSeconds: 120},
				} {
					if err := sup.history.Append(rec); err != nil {
						t.Fatal(err)
					}
				}
			}
			sup.foldedMarks[e.ID] = foldMemo{mark: indexMark{events: 1000}, publishedAt: snapshotAnchor, indexDSN: e.DSN}
			mark := indexMark{events: 1000} // nothing indexed since the snapshot
			stubIndexMark(t, &mark, true)
			asked := stubCaptureProbe(t, c.probe)
			logs := captureSlogFor(t)
			method, why := freshDecision(t, b, e, now)
			if method != c.method {
				t.Fatalf("method=%q why=%q, want %s", method, why, c.method)
			}
			if len(*asked) == 0 {
				t.Fatal("the source was not asked")
			}
			line := logLine(logs.String(), found)
			t.Log(line)
			if line == "" || !strings.Contains(line, c.level) || !strings.Contains(line, c.probe.detail) {
				t.Fatalf("log = %q, want the finding at %s with the source's answer", logs.String(), c.level)
			}
			if strings.Contains(line, "full read") || strings.Contains(line, "update") {
				t.Fatalf("the probe's line names an action it does not take: %q", line)
			}
			if c.method == console.BackupMethodFull && console.BackupWhyCode(why) != "window_age" {
				t.Fatalf("why = %q, want the full read chosen because the index is in doubt", why)
			}
		})
	}
	// Caught up: the finding, with no action in it either.
	b, _, _ := newScheduleFixture(t, true)
	logs := captureSlogFor(t)
	b.reportCaptureProbe(console.ServerEntry{ID: "a", Name: "a"}, snapshotAnchor, captureProbeResult{verdict: console.CaptureCaughtUp})
	line := logLine(logs.String(), "the source confirms it wrote nothing the capture has not recorded")
	t.Log(line)
	if line == "" || strings.Contains(line, "full read") || strings.Contains(line, "updating") {
		t.Fatalf("the caught-up line: %q", logs.String())
	}
}
