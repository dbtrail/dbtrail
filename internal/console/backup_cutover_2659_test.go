package console

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"math"
	"strings"
	"testing"
	"time"
)

// #2659: the age of the previous snapshot alone never chooses a full read.
// A full read reads every row of the source and takes a lock, so it is
// chosen on what was measured (the update clearly dearer than the last full
// read), or because the index may be missing writes of the source (nothing
// indexed since an old snapshot, and the capture not confirmed caught up).

// The cases the age rule used to decide, each one now an update. The anchor
// is far past the old cut-over on every schedule tried.
func TestCutoverToFull_ageAloneNeverChoosesAFullRead_2659(t *testing.T) {
	now := time.Date(2026, 11, 11, 10, 30, 0, 0, time.UTC)
	old := now.Add(-30 * 24 * time.Hour)
	cases := []struct {
		name string
		w    BackupWindow
	}{
		{"no count of the events", BackupWindow{Anchor: old, Events: -1, FoldRate: 4000, LastFull: 8 * time.Minute}},
		{"no rate measured (a new server, a long stop)", BackupWindow{Anchor: old, Events: 3_000_000, LastFull: 6 * time.Hour}},
		{"no rate, a fixed cost under the full read", BackupWindow{Anchor: old, Events: 3_000_000, FoldFixed: 90 * time.Second, LastFull: 6 * time.Hour}},
		{"no full read on record", BackupWindow{Anchor: old, Events: 100, FoldRate: 1000}},
		{"nothing measured at all", BackupWindow{Anchor: old, Events: -1}},
		{"nothing measured, events counted", BackupWindow{Anchor: old, Events: 100}},
		{"the estimate within the margin", BackupWindow{Anchor: old, Events: 720_000, FoldRate: 1000, LastFull: 8 * time.Minute}},
		{"the fixed cost within the margin", BackupWindow{Anchor: old, Events: 1, FoldFixed: 9 * time.Minute, FoldRate: 4000, LastFull: 8 * time.Minute}},
		{"dear by the estimate, nothing measured since the last full read", BackupWindow{Anchor: old, Events: 17_000_000, FoldRate: 4000, LastFull: 8 * time.Minute, UnmeasuredSinceFull: true}},
		{"dear by the fixed cost, nothing measured since the last full read", BackupWindow{Anchor: old, Events: 40, FoldFixed: 6 * time.Minute, LastFull: 2 * time.Minute, UnmeasuredSinceFull: true}},
		{"a full read that finished long ago", BackupWindow{Anchor: old, AnchorFullFinished: old.Add(time.Hour), Events: -1}},
		{"the capture behind, with events to apply", BackupWindow{Anchor: old, Events: 5, Capture: CaptureBehind}},
		{"the capture behind, the count unknown", BackupWindow{Anchor: old, Events: -1, Capture: CaptureBehind}},
		{"an event count past what fits a duration, no rate", BackupWindow{Anchor: old, Events: math.MaxInt64, LastFull: time.Second}},
	}
	for _, c := range cases {
		for _, interval := range []time.Duration{0, 5 * time.Minute, 24 * time.Hour} {
			t.Run(fmt.Sprintf("%s, every %s", c.name, interval), func(t *testing.T) {
				if why := CutoverToFull(c.w, interval, now); why != "" {
					t.Fatalf("chose a full read on %q, want the update", why)
				}
			})
		}
	}
}

// The issue's example through the whole decision: a five-minute schedule,
// the daemon stopped for three hours, no usable rate on restart.
func TestChooseBackupMethod_aLongStopWithNoRateUpdates_2659(t *testing.T) {
	dir := t.TempDir()
	fakeSnapshot(t, dir)
	now := time.Date(2026, 11, 11, 12, 0, 0, 0, time.UTC)
	stopped := func(_ context.Context, _ ServerEntry, _ time.Time) BackupWindow {
		return BackupWindow{Anchor: now.Add(-3 * time.Hour), Events: 40_000_000, LastFull: 9 * time.Hour}
	}
	for _, every := range []string{"5m", "1h", "1d"} {
		e := ServerEntry{DSN: "idx", SourceDSN: "src", BaselineDir: dir, BackupSchedule: &BackupSchedule{Every: every}}
		method, why, err := ChooseBackupMethodAt(context.Background(), e,
			BackupScheduleGates{LoopRunning: true, FullBackups: true, Window: stopped}, now)
		if err != nil || method != BackupMethodRefresh {
			t.Fatalf("every %s: method=%q why=%q err=%v, want the update", every, method, why, err)
		}
	}
}

// Every combination of what the probe can hand the rule, checked against
// what each answer is allowed to rest on. A full read has two reasons only:
//
//   - window_measured: the update is clearly dearer than the last full read,
//     on numbers measured since that full read;
//   - window_age: nothing indexed since a snapshot past the cut-over age, and
//     evidence that the capture is missing changes (the source ahead, or
//     the capture's own record of a loss).
//
// And the age of the snapshot changes nothing while there are events to
// apply, or while their count is unknown.
func TestCutoverToFull_everyCombination_2659(t *testing.T) {
	// The rule logs each update it takes on proven evidence; thousands of
	// those lines would bury a failure.
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })
	now := time.Date(2026, 11, 11, 10, 30, 0, 0, time.UTC)
	events := []int64{-1, 0, 1, 40, 720_000, 17_000_000, math.MaxInt64}
	rates := []float64{0, 0.0000001, 1000}
	fulls := []time.Duration{0, 6 * time.Second, 8 * time.Minute, 9 * time.Hour}
	fixeds := []time.Duration{0, 15 * time.Second, 20 * time.Minute}
	provens := []int64{0, 15_000_000}
	captures := []string{"", CaptureBehind, CaptureSuspect, CaptureUnaskable, CaptureCaughtUp}
	intervals := []time.Duration{0, 5 * time.Minute, 24 * time.Hour}
	// Anchor ages: none, fresh, between the two-hour floor and a daily
	// schedule's six days, and past both.
	ages := []time.Duration{-1, 5 * time.Minute, 5 * time.Hour, 30 * 24 * time.Hour}
	// A full read that finished after the anchor: none, or five minutes ago.
	finishes := []time.Duration{-1, 5 * time.Minute}

	n, fullMeasured, fullAge := 0, 0, 0
	for _, ev := range events {
		for _, rate := range rates {
			for _, full := range fulls {
				for _, fixed := range fixeds {
					for _, proven := range provens {
						for _, unmeasured := range []bool{false, true} {
							for _, capture := range captures {
								for _, interval := range intervals {
									// codes of this window at each anchor age, to compare across ages
									byAge := map[time.Duration]string{}
									for _, age := range ages {
										for _, fin := range finishes {
											w := BackupWindow{Events: ev, FoldRate: rate, LastFull: full, FoldFixed: fixed,
												Proven: proven, UnmeasuredSinceFull: unmeasured, Capture: capture}
											if age >= 0 {
												w.Anchor = now.Add(-age)
											}
											if fin >= 0 {
												w.AnchorFullFinished = now.Add(-fin)
											}
											why := CutoverToFull(w, interval, now)
											code := BackupWhyCode(why)
											n++
											desc := fmt.Sprintf("%+v every %s", w, interval)
											switch {
											case why == "":
											case code == "window_measured":
												fullMeasured++
												if unmeasured || full <= 0 {
													t.Fatalf("%s: a measured full read (%q) with no full read on record, or on numbers older than it", desc, why)
												}
												est := fixed.Seconds()
												if rate > 0 {
													if ev < 0 {
														t.Fatalf("%s: a measured full read (%q) on an estimate with no event count", desc, why)
													}
													est += float64(ev) / rate
													if float64(ev) <= BackupProvenMargin*float64(proven) {
														t.Fatalf("%s: a measured full read (%q) on an update proven cheaper", desc, why)
													}
												} else if ev == 0 {
													t.Fatalf("%s: a measured full read (%q) with nothing to apply", desc, why)
												}
												if !updateClearlyDearer(est, full.Seconds()) {
													t.Fatalf("%s: a measured full read (%q) on an update of %.0fs, not clearly dearer than the full read", desc, why, est)
												}
											case code == "window_age":
												fullAge++
												since := w.Anchor
												if w.AnchorFullFinished.After(since) {
													since = w.AnchorFullFinished
												}
												// A source that is ahead, a capture whose
												// own record says it lost changes, or one
												// not confirmed for no listed reason (""). A
												// source that cannot be asked chooses no
												// full read, and a confirmed one neither.
												if ev != 0 || capture == CaptureUnaskable || capture == CaptureCaughtUp || w.Anchor.IsZero() || now.Sub(since) <= BackupCutoverAge(interval) {
													t.Fatalf("%s: a full read on the snapshot's age (%q); only nothing indexed since an old snapshot with evidence against the capture may", desc, why)
												}
												if !strings.Contains(why, BackupWhyNothingIndexedWords) {
													t.Fatalf("%s: the reason %q does not say nothing was indexed", desc, why)
												}
											default:
												t.Fatalf("%s: unexpected reason %q (code %q)", desc, why, code)
											}
											if fin < 0 {
												byAge[age] = code
											}
										}
									}
									if ev != 0 {
										for _, age := range ages {
											if byAge[age] != byAge[ages[0]] {
												t.Fatalf("events %d rate %v full %s fixed %s proven %d unmeasured %v capture %q every %s: the answer changes with the snapshot's age (%v)",
													ev, rate, full, fixed, proven, unmeasured, capture, interval, byAge)
											}
										}
									}
								}
							}
						}
					}
				}
			}
		}
	}
	// The matrix must reach both kinds of full read, or the checks above
	// proved nothing about them.
	if fullMeasured == 0 || fullAge == 0 {
		t.Fatalf("of %d combinations, %d measured full reads and %d on an index that may be missing writes; want both reached", n, fullMeasured, fullAge)
	}
	t.Logf("%d combinations: %d measured full reads, %d full reads on an index that may be missing writes", n, fullMeasured, fullAge)
}

// The two measured rules are complete as well as sound: wherever their
// conditions hold, the full read is taken, whatever the anchor's age.
func TestCutoverToFull_measuredRulesStillCutOver_2659(t *testing.T) {
	now := time.Date(2026, 11, 11, 10, 30, 0, 0, time.UTC)
	for _, age := range []time.Duration{5 * time.Minute, 5 * time.Hour, 30 * 24 * time.Hour} {
		anchor := now.Add(-age)
		for name, w := range map[string]BackupWindow{
			"the estimate":             {Anchor: anchor, Events: 17_000_000, FoldRate: 4000, LastFull: 8 * time.Minute},
			"the estimate, overflowed": {Anchor: anchor, Events: math.MaxInt64, FoldRate: 0.0000001, LastFull: 8 * time.Minute},
			"the fixed cost":           {Anchor: anchor, Events: 40, FoldFixed: 6 * time.Minute, LastFull: 2 * time.Minute},
			"the fixed cost, no count": {Anchor: anchor, Events: -1, FoldFixed: 6 * time.Minute, LastFull: 2 * time.Minute},
		} {
			why := CutoverToFull(w, 5*time.Minute, now)
			if BackupWhyCode(why) != "window_measured" {
				t.Errorf("%s, anchor %s old: got %q, want the measured full read", name, age, why)
			}
			if strings.Contains(why, "-") {
				t.Errorf("%s: the reason %q carries a negative number", name, why)
			}
		}
	}
}

// The sentences an operator reads, with real values.
func TestCutoverToFull_reasons_2659(t *testing.T) {
	now := time.Date(2026, 11, 11, 10, 30, 0, 0, time.UTC)
	old := now.Add(-5*time.Hour - 30*time.Minute)
	behind := BackupWindow{Anchor: old, Events: 0, LastFull: 40 * time.Minute, Capture: CaptureBehind}
	unknown := BackupWindow{Anchor: old, Events: 0, LastFull: 40 * time.Minute, Capture: CaptureSuspect, CaptureDetail: "the capture dropped events that no full read has read from the source since"}
	for _, c := range []struct {
		w    BackupWindow
		want string
	}{
		{BackupWindow{Anchor: old, Events: 17_000_000, FoldRate: 4000, LastFull: 7*time.Minute + 41*time.Second},
			"an update from the recorded changes would take longer than a full read: 17,000,000 events since the previous snapshot would take about 1h 11m to apply at the measured rate, more than twice and over 30s longer than the last full read, which took 8m"},
		{BackupWindow{Anchor: old, Events: 40, FoldFixed: 6 * time.Minute, LastFull: 2 * time.Minute},
			"an update from the recorded changes would take longer than a full read: the cheapest recent update took 6m, more than twice and over 30s longer than the last full read, which took 2m"},
		{behind,
			"the index may be missing changes made since the previous snapshot: nothing new was indexed since it was taken 5h 30m ago, but the source reports transactions the capture's checkpoint does not include, so the capture may have stopped or fallen behind"},
		{unknown,
			"the index may be missing changes made since the previous snapshot: nothing new was indexed since it was taken 5h 30m ago, and the capture dropped events that no full read has read from the source since"},
	} {
		why := CutoverToFull(c.w, 5*time.Minute, now)
		t.Log(why)
		if why != c.want {
			t.Errorf("reason\n got %q\nwant %q", why, c.want)
		}
	}
}
