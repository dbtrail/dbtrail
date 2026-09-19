package consoleapp

import (
	"context"
	"errors"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// #1721: the daemon's side of the cut-over — what it can measure and what it
// must answer "unknown" to.

// TestMeasureWindow: events since the anchor need a memo for exactly that
// snapshot on the same index and a current mark that did not go backwards;
// the rate and the last full backup come from the history.
func TestMeasureWindow(t *testing.T) {
	anchor := time.Date(2026, 9, 18, 5, 0, 0, 0, time.UTC)
	e := console.ServerEntry{ID: "a", Name: "a", DSN: "idx"}
	newSched := func(t *testing.T) *backupScheduler {
		b, _, sup := newScheduleFixture(t, true)
		sup.foldedMarks["a"] = foldMemo{mark: indexMark{events: 1000}, publishedAt: anchor.Add(300 * time.Millisecond), indexDSN: "idx"}
		return b
	}
	if b, _, _ := newScheduleFixture(t, true); b.WindowProbe() != nil {
		t.Fatal("the fixture did not switch the probe off")
	}
	t.Run("memo for this snapshot, index answers: events counted", func(t *testing.T) {
		b := newSched(t)
		mark := indexMark{events: 18_000}
		p := stubIndexMark(t, &mark, true)
		w := b.measureWindow(context.Background(), e, anchor)
		if w.Events != 17_000 || !w.Anchor.Equal(anchor) {
			t.Fatalf("window = %+v, want 17000 events since the anchor", w)
		}
		if got := p.marks(); len(got) != 1 || got[0] != "idx" {
			t.Fatalf("the probe read %v, want the server's index once", got)
		}
	})
	t.Run("no memo: unknown, and the index is not read", func(t *testing.T) {
		b, _, _ := newScheduleFixture(t, true)
		mark := indexMark{events: 18_000}
		p := stubIndexMark(t, &mark, true)
		if w := b.measureWindow(context.Background(), e, anchor); w.Events != -1 {
			t.Fatalf("window = %+v, want events unknown", w)
		}
		if len(p.marks()) != 0 {
			t.Fatal("the index was read with nothing to count from")
		}
	})
	t.Run("memo for another snapshot (a full backup since): unknown", func(t *testing.T) {
		b := newSched(t)
		mark := indexMark{events: 18_000}
		stubIndexMark(t, &mark, true)
		if w := b.measureWindow(context.Background(), e, anchor.Add(time.Hour)); w.Events != -1 {
			t.Fatalf("window = %+v, want events unknown", w)
		}
	})
	t.Run("memo for another index: unknown", func(t *testing.T) {
		b := newSched(t)
		mark := indexMark{events: 18_000}
		stubIndexMark(t, &mark, true)
		other := e
		other.DSN = "idx2"
		if w := b.measureWindow(context.Background(), other, anchor); w.Events != -1 {
			t.Fatalf("window = %+v, want events unknown", w)
		}
	})
	t.Run("index does not answer: unknown", func(t *testing.T) {
		b := newSched(t)
		mark := indexMark{}
		stubIndexMark(t, &mark, false)
		if w := b.measureWindow(context.Background(), e, anchor); w.Events != -1 {
			t.Fatalf("window = %+v, want events unknown", w)
		}
	})
	t.Run("mark went backwards (index rebuilt): unknown", func(t *testing.T) {
		b := newSched(t)
		mark := indexMark{events: 10}
		stubIndexMark(t, &mark, true)
		if w := b.measureWindow(context.Background(), e, anchor); w.Events != -1 {
			t.Fatalf("window = %+v, want events unknown", w)
		}
	})
	t.Run("model, proven size and last full backup come from the history", func(t *testing.T) {
		b, _, sup := newScheduleFixture(t, true)
		for _, rec := range []console.BaselineRunRecord{
			{ServerID: "a", Kind: console.BaselineRunDump, StartedAt: "2026-09-18T05:00:00Z", FinishedAt: "2026-09-18T05:07:41Z"},
			{ServerID: "a", Kind: console.BaselineRunRefresh, Events: 1000, UpdateSeconds: 60},
			{ServerID: "a", Kind: console.BaselineRunRefresh, Events: 121_000, UpdateSeconds: 120},
		} {
			if err := sup.history.Append(rec); err != nil {
				t.Fatal(err)
			}
		}
		mark := indexMark{}
		stubIndexMark(t, &mark, true)
		w := b.measureWindow(context.Background(), e, anchor)
		if w.FoldFixed != time.Minute || w.FoldRate != 120_000/60.0 || w.Proven != 121_000 || w.LastFull != 7*time.Minute+41*time.Second || w.Events != -1 {
			t.Fatalf("window = %+v, want fixed 1m, rate 120000/60 (marginal), proven 121000, last full 7m41s, events unknown", w)
		}
	})
	t.Run("no memo but the history recorded the mark for this snapshot (a restart): events counted", func(t *testing.T) {
		b, _, sup := newScheduleFixture(t, true)
		rec := console.BaselineRunRecord{ServerID: "a", Kind: console.BaselineRunRefresh, Events: 10, UpdateSeconds: 1,
			SnapshotTime: anchor.Format(time.RFC3339), IndexMark: 1000}
		if err := sup.history.Append(rec); err != nil {
			t.Fatal(err)
		}
		mark := indexMark{events: 18_000}
		stubIndexMark(t, &mark, true)
		if w := b.measureWindow(context.Background(), e, anchor); w.Events != 17_000 {
			t.Fatalf("window = %+v, want 17000 events from the recorded mark", w)
		}
	})
	t.Run("the measurement is cached for a minute: one index read for many page loads", func(t *testing.T) {
		b := newSched(t)
		mark := indexMark{events: 18_000}
		p := stubIndexMark(t, &mark, true)
		for range 5 {
			if w := b.measureWindow(context.Background(), e, anchor); w.Events != 17_000 {
				t.Fatalf("window = %+v", w)
			}
		}
		if got := p.marks(); len(got) != 1 {
			t.Fatalf("the index was read %d times in a minute, want once", len(got))
		}
		// Another anchor (a newer snapshot) is measured afresh.
		mark = indexMark{events: 20_000}
		b.sup.mu.Lock()
		b.sup.foldedMarks["a"] = foldMemo{mark: indexMark{events: 19_000}, publishedAt: anchor.Add(time.Hour), indexDSN: "idx"}
		b.sup.mu.Unlock()
		if w := b.measureWindow(context.Background(), e, anchor.Add(time.Hour)); w.Events != 1000 || len(p.marks()) != 2 {
			t.Fatalf("window for a new anchor = %+v after %d reads", w, len(p.marks()))
		}
	})
	t.Run("the cache is per server, and expires", func(t *testing.T) {
		b := newSched(t)
		b.sup.foldedMarks["b"] = foldMemo{mark: indexMark{events: 5000}, publishedAt: anchor, indexDSN: "idx2"}
		mark := indexMark{events: 18_000}
		p := stubIndexMark(t, &mark, true)
		other := console.ServerEntry{ID: "b", Name: "b", DSN: "idx2"}
		if w := b.measureWindow(context.Background(), e, anchor); w.Events != 17_000 {
			t.Fatalf("a = %+v", w)
		}
		if w := b.measureWindow(context.Background(), other, anchor); w.Events != 13_000 {
			t.Fatalf("b = %+v, want its own base (5000) counted, not a's cached window", w)
		}
		if got := p.marks(); len(got) != 2 || got[0] != "idx" || got[1] != "idx2" {
			t.Fatalf("index reads = %v, want one per server", got)
		}
		b.mu.Lock()
		c := b.windows["a"]
		c.at = time.Now().Add(-2 * windowCacheFor)
		b.windows["a"] = c
		b.mu.Unlock()
		mark = indexMark{events: 19_000}
		if w := b.measureWindow(context.Background(), e, anchor); w.Events != 18_000 || len(p.marks()) != 3 {
			t.Fatalf("after expiry = %+v with %d reads, want a fresh read", w, len(p.marks()))
		}
	})
	t.Run("the dial is bounded too", func(t *testing.T) {
		// The context bounds the queries; the DSN's own dial budget (ten
		// seconds by default) bounds the connect, so the probe rewrites it.
		for dsn, want := range map[string]string{
			"u:p@tcp(10.0.0.1:3306)/idx":             "timeout=3s",
			"u:p@tcp(10.0.0.1:3306)/idx?timeout=30s": "timeout=3s",
			"u:p@tcp(10.0.0.1:3306)/idx?timeout=1s":  "timeout=1s",
			"idx":                                    "idx", // not a DSN: handed on, fails for its own reason
		} {
			if got := probeDSN(dsn); !strings.Contains(got, want) {
				t.Errorf("probeDSN(%q) = %q, want it to carry %q", dsn, got, want)
			}
		}
	})
	t.Run("the index read is bounded", func(t *testing.T) {
		b := newSched(t)
		prev := readIndexMark
		t.Cleanup(func() { readIndexMark = prev })
		var deadline time.Time
		var hasDeadline bool
		readIndexMark = func(ctx context.Context, _ string) (indexMark, bool) {
			deadline, hasDeadline = ctx.Deadline()
			return indexMark{events: 18_000}, true
		}
		before := time.Now()
		b.measureWindow(context.Background(), e, anchor)
		if !hasDeadline || deadline.After(before.Add(windowProbeTimeout+time.Second)) {
			t.Fatalf("deadline=%v (%v), want within %s of the call", deadline, hasDeadline, windowProbeTimeout)
		}
	})
	t.Run("the warning resolves when the index answers again, and fires again on the next outage", func(t *testing.T) {
		b := newSched(t)
		mark := indexMark{events: 18_000}
		known := false
		stubIndexMarkSwitchable(t, &mark, &known)
		logs := captureSlogFor(t)
		probe := func() console.BackupWindow {
			b.mu.Lock()
			b.windows = nil // each call is a fresh probe
			b.mu.Unlock()
			return b.measureWindow(context.Background(), e, anchor)
		}
		probe()
		known = true
		if w := probe(); w.Events != 17_000 || b.sup.gateEdge.Active("window-probe:a") {
			t.Fatalf("after the index answered: window=%+v active=%v", w, b.sup.gateEdge.Active("window-probe:a"))
		}
		known = false
		probe()
		if out := logs.String(); strings.Count(out, "did not answer the update-size probe") != 2 {
			t.Fatalf("log = %q, want one warning per outage", out)
		}
	})
	t.Run("index silent with a base to count from: one warning, then quiet", func(t *testing.T) {
		b := newSched(t)
		mark := indexMark{}
		stubIndexMark(t, &mark, false)
		logs := captureSlogFor(t)
		for range 3 {
			b.windows = nil // defeat the cache: each call is a fresh probe
			if w := b.measureWindow(context.Background(), e, anchor); w.Events != -1 {
				t.Fatalf("window = %+v, want events unknown", w)
			}
		}
		if out := logs.String(); strings.Count(out, "did not answer the update-size probe") != 1 {
			t.Fatalf("log = %q, want exactly one warning", out)
		}
	})
	// The daemon's wiring: the loop's gates and the reporter both carry the
	// probe (the first draft wired the API only, and the rule was dead in
	// the daemon).
	b := newBackupScheduler(newBaselineSupervisor(context.Background(), t.TempDir(), baseline.DefaultLockMode), nil, true, false)
	if b.WindowProbe() == nil || b.gates().Window == nil {
		t.Fatal("the loop reports no probe: the daemon would never cut over")
	}
}

// TestRunRefresh_recordsTheMeasuredRate: a successful fold records the
// events it applied (the mark's advance since this daemon's previous fold
// of the SAME snapshot) and its own seconds; a fold after a full backup, or
// the first one, records no events (nothing to count from).
func TestRunRefresh_recordsTheMeasuredRate(t *testing.T) {
	stubCoverage(t, true, true)
	local := t.TempDir()
	injectFold(t, 0, nil)
	sup := newBaselineSupervisor(context.Background(), t.TempDir(), baseline.DefaultLockMode)
	sup.history = openHistoryForTest(t)
	req := refreshRequest{ServerID: "s", ServerName: "s", IndexDSN: "d", BaselineDir: local}
	// A previous backup to fold from (a full backup's, say).
	writeSnapshotFiles(t, filepath.Join(local, reconstruct.SnapshotDirName(refreshAt.Add(-5*time.Minute))), baseline.SuccessMarker)
	// First fold: no previous mark to count from.
	mark := indexMark{events: 1000, schemaChanges: 1}
	stubIndexMark(t, &mark, true)
	sup.refreshes["s"] = &console.BaselineStatus{State: "running"}
	sup.runRefresh(req, refreshAt, time.Minute)
	runs := sup.history.List("s")
	if len(runs) != 1 || runs[0].Events != 0 || runs[0].UpdateSeconds <= 0 || runs[0].IndexMark != 1000 {
		t.Fatalf("first fold = %+v, want no events counted, the run timed, the mark recorded", runs)
	}
	// Second fold from the snapshot the first published: the advance counts.
	mark = indexMark{events: 6000, schemaChanges: 1}
	sup.refreshes["s"] = &console.BaselineStatus{State: "running"}
	sup.runRefresh(req, refreshAt.Add(5*time.Minute), time.Minute)
	runs = sup.history.List("s")
	if len(runs) != 2 || runs[1].Events != 5000 || runs[1].UpdateSeconds <= 0 || runs[1].IndexMark != 6000 {
		t.Fatalf("second fold = %+v, want 5000 events counted", runs)
	}
	// A full backup in between publishes a newer snapshot the memo does not
	// name: the next fold counts nothing rather than the full backup's
	// window too.
	full := refreshAt.Add(8 * time.Minute)
	writeSnapshotFiles(t, filepath.Join(local, reconstruct.SnapshotDirName(full)), baseline.SuccessMarker)
	mark = indexMark{events: 9000, schemaChanges: 1}
	sup.refreshes["s"] = &console.BaselineStatus{State: "running"}
	sup.runRefresh(req, refreshAt.Add(10*time.Minute), time.Minute)
	runs = sup.history.List("s")
	if len(runs) != 3 || runs[2].Events != 0 {
		t.Fatalf("fold after a full backup = %+v, want no events counted", runs)
	}
	// After a restart the count for the newest refresh snapshot comes from
	// its record, not the (gone) memo.
	if mark, ok := sup.history.IndexMarkFor("s", refreshAt.Add(5*time.Minute).Format(time.RFC3339)); !ok || mark != 6000 {
		t.Fatalf("IndexMarkFor the second fold's snapshot = %d,%v", mark, ok)
	}
	// A memo for another index (re-pointed DSN: an unrelated event_id
	// space), another destination, or a mark that went backwards (an index
	// rebuilt) counts nothing rather than poisoning the rate.
	fold := func(req refreshRequest, at time.Time, events uint64) console.BaselineRunRecord {
		mark = indexMark{events: events, schemaChanges: 1}
		sup.refreshes["s"] = &console.BaselineStatus{State: "running"}
		sup.runRefresh(req, at, time.Minute)
		runs := sup.history.List("s")
		return runs[len(runs)-1]
	}
	at := refreshAt.Add(15 * time.Minute)
	if r := fold(req, at, 12_000); r.Events != 3000 { // 12000 - 9000: the previous fold's memo, same index, same destination
		t.Fatalf("control fold = %+v, want 3000 events", r)
	}
	other := req
	other.IndexDSN = "d2"
	if r := fold(other, at.Add(5*time.Minute), 20_000); r.Events != 0 {
		t.Fatalf("fold on another index = %+v, want no events counted", r)
	}
	moved := req
	moved.IndexDSN = "d2" // the memo now names d2; only the destination differs
	moved.BaselineS3 = "s3://b/p/"
	stubBucketListing(t)
	realUp := uploadSnapshot
	t.Cleanup(func() { uploadSnapshot = realUp })
	uploadSnapshot = func(context.Context, string, string, string, bool) (int, error) { return 0, nil }
	if r := fold(moved, at.Add(10*time.Minute), 25_000); r.Events != 0 {
		t.Fatalf("fold to another destination = %+v, want no events counted", r)
	}
	if r := fold(moved, at.Add(15*time.Minute), 100); r.Events != 0 { // 100 < the memo's 25000
		t.Fatalf("fold after the mark went backwards = %+v, want no events counted", r)
	}
	// A fold that fails records no measurement at all.
	injectFold(t, 0, errors.New("capture gap"))
	if r := fold(moved, at.Add(20*time.Minute), 30_000); r.Error == "" || r.Events != 0 || r.UpdateSeconds != 0 || r.IndexMark != 0 {
		t.Fatalf("failed fold = %+v, want no measurement", r)
	}
}

// TestBackupScheduler_cutsOverToAFullBackupOnTheMeasuredWindow drives the
// REAL slot: the loop's own gates carry the probe (the API's did from the
// start; the loop's did not, and the rule was dead in the daemon until this
// test), so a measured window past the last full backup starts a full
// backup with the numbers as its reason.
func TestBackupScheduler_cutsOverToAFullBackupOnTheMeasuredWindow(t *testing.T) {
	b, reg, sup := newScheduleFixture(t, true)
	b.window = b.measureWindow      // the cut-over under test
	e := addScheduled(t, reg, true) // the fake snapshot at snapshotAnchor
	e.SourceDSN = "not a dsn"       // the full backup fails fast; that it STARTED is the point
	if err := reg.Update(e); err != nil {
		t.Fatal(err)
	}
	for _, rec := range []console.BaselineRunRecord{
		{ServerID: e.ID, Kind: console.BaselineRunDump, StartedAt: "2026-08-20T05:00:00Z", FinishedAt: "2026-08-20T05:08:00Z"},
		{ServerID: e.ID, Kind: console.BaselineRunRefresh, Events: 1000, UpdateSeconds: 60},
		{ServerID: e.ID, Kind: console.BaselineRunRefresh, Events: 241_000, UpdateSeconds: 120}, // 4000 events/s beyond the fixed minute
	} {
		if err := sup.history.Append(rec); err != nil {
			t.Fatal(err)
		}
	}
	sup.foldedMarks[e.ID] = foldMemo{mark: indexMark{events: 1000}, publishedAt: snapshotAnchor, indexDSN: e.DSN}
	mark := indexMark{events: 17_001_000} // 17 M events since the anchor: ~71 min at the rate, against an 8 min full backup
	stubIndexMark(t, &mark, true)
	logs := captureSlogFor(t)
	fireAt(b, time.Date(2026, 8, 20, 13, 0, 5, 0, time.UTC))
	st := waitTerminalMethod(t, b, e.ID, console.BackupMethodFull)
	if console.BackupWhyCode(st.LastWhy) != "window_measured" || !strings.Contains(st.LastWhy, "17,000,000 events") {
		t.Fatalf("the slot did not cut over on the measured window: %+v", st)
	}
	if run, _ := sup.history.LastScheduled(e.ID); run == nil || run.Kind != console.BaselineRunDump || run.WhyCode != "window_measured" {
		t.Fatalf("the full backup's record does not carry the reason: %+v", run)
	}
	if !strings.Contains(logs.String(), "taking a full backup instead of an update") {
		t.Fatalf("the decision was not logged: %q", logs.String())
	}
	// The same server with a window the model estimates well under the full
	// backup (200,000 events: about 110 s against 8 minutes) stays an update;
	// the proven-size guard has its own case in TestCutoverToFull.
	b2, reg2, sup2 := newScheduleFixture(t, true)
	b2.window = b2.measureWindow
	e2 := addScheduled(t, reg2, true)
	for _, rec := range []console.BaselineRunRecord{
		{ServerID: e2.ID, Kind: console.BaselineRunDump, StartedAt: "2026-08-20T05:00:00Z", FinishedAt: "2026-08-20T05:08:00Z"},
		{ServerID: e2.ID, Kind: console.BaselineRunRefresh, Events: 1000, UpdateSeconds: 60},
		{ServerID: e2.ID, Kind: console.BaselineRunRefresh, Events: 241_000, UpdateSeconds: 120},
	} {
		if err := sup2.history.Append(rec); err != nil {
			t.Fatal(err)
		}
	}
	sup2.foldedMarks[e2.ID] = foldMemo{mark: indexMark{events: 1000}, publishedAt: snapshotAnchor, indexDSN: e2.DSN}
	mark = indexMark{events: 201_000}
	holdFold(t, func(context.Context, reconstruct.FullTableConfig) ([]*reconstruct.TableReport, []reconstruct.TableFailure, error) {
		return nil, nil, nil
	})
	stubCoverage(t, true, true)
	fireAt(b2, time.Date(2026, 8, 20, 13, 0, 5, 0, time.UTC))
	if st := waitTerminalMethod(t, b2, e2.ID, console.BackupMethodRefresh); st.LastWhy != "" {
		t.Fatalf("a proven-cheap window was cut over: %+v", st)
	}
}

// captureSlogFor routes the default logger into a buffer for the test.
func captureSlogFor(t *testing.T) *lockedWriter {
	t.Helper()
	prev := slog.Default()
	t.Cleanup(func() { slog.SetDefault(prev) })
	lw := &lockedWriter{}
	slog.SetDefault(slog.New(slog.NewTextHandler(lw, &slog.HandlerOptions{Level: slog.LevelInfo})))
	return lw
}

type lockedWriter struct {
	mu sync.Mutex
	sb strings.Builder
}

func (l *lockedWriter) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.sb.Write(p)
}

func (l *lockedWriter) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.sb.String()
}

// TestBackupScheduler_cutsOverOnAgeInTheRealSlot: nothing measured (no
// memo, empty history) and a previous backup weeks older than the slot: the
// slot starts a full backup whose reason names the age and what was missing.
func TestBackupScheduler_cutsOverOnAgeInTheRealSlot(t *testing.T) {
	b, reg, sup := newScheduleFixture(t, true)
	b.window = b.measureWindow
	e := addScheduled(t, reg, true) // the fake snapshot at snapshotAnchor (2026-08-20)
	e.SourceDSN = "not a dsn"
	if err := reg.Update(e); err != nil {
		t.Fatal(err)
	}
	logs := captureSlogFor(t)
	fireAt(b, time.Date(2026, 8, 28, 9, 0, 5, 0, time.UTC)) // eight days later, on an hourly schedule (cut-over 6 h)
	st := waitTerminalMethod(t, b, e.ID, console.BackupMethodFull)
	if console.BackupWhyCode(st.LastWhy) != "window_age" ||
		!strings.Contains(st.LastWhy, "no count of the changes since it, no usable update rate (none measured, or the measured updates all cost about the same), no full backup on record") {
		t.Fatalf("the slot did not cut over on age: %+v", st)
	}
	if run, _ := sup.history.LastScheduled(e.ID); run == nil || run.Kind != console.BaselineRunDump || run.WhyCode != "window_age" {
		t.Fatalf("the full backup's record does not carry the reason: %+v", run)
	}
	if !strings.Contains(logs.String(), "taking a full backup instead of an update") {
		t.Fatalf("the decision was not logged: %q", logs.String())
	}
}

func openHistoryForTest(t *testing.T) *console.BaselineRunHistory {
	t.Helper()
	h, err := console.OpenBaselineHistory(filepath.Join(t.TempDir(), "h.json"))
	if err != nil {
		t.Fatal(err)
	}
	return h
}
