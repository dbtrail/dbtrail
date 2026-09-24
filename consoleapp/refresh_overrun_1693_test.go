package consoleapp

import (
	"bytes"
	"context"
	"log/slog"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// The overrun warning used to say one thing for every overrun: that the cost
// was the full-table rewrite, and that raising the interval would fix it.
// Measured on a sustained write load (#1693), a refresh that outran the window
// of changes it folded handed the next one a larger window, the durations grew
// by a factor of 3.7 in ninety minutes, and the line repeated itself ten
// times.
//
// The first fix attempted here was one comparison with no history, and it was
// wrong for the ordinary case; gradeRefresh's doc carries the counterexample
// and TestGradeRefresh drives it. What the strong verdict needs is two
// published runs, so the tests below split in two: the reading itself is
// decided by a pure function and tested exhaustively, and the call site is
// tested for the only thing it can get wrong, which is feeding that function
// numbers from somewhere other than where it says they came from.

func TestGradeRefresh(t *testing.T) {
	cases := []struct {
		name string
		run  refreshRun
		prev refreshPace
		want refreshVerdict
	}{
		{
			name: "inside the interval it was given",
			run:  refreshRun{interval: 10 * time.Minute, took: 4 * time.Minute, window: time.Minute},
			want: refreshOnTime,
		}, {
			// TriggerRefresh has exactly two callers, the interval loop and
			// the backup schedule, and both pass a positive interval. The
			// guard is for the value, not for a caller: an interval of zero
			// makes every run an overrun, and the loop would warn forever.
			name: "no interval to be late against",
			run:  refreshRun{interval: 0, took: time.Hour, window: time.Minute},
			want: refreshOnTime,
		}, {
			name: "the window could not be measured",
			run:  refreshRun{interval: 5 * time.Minute, took: 12 * time.Minute, window: 0},
			want: refreshWindowUnknown,
		}, {
			name: "over the interval, inside the window it folded",
			run:  refreshRun{interval: 5 * time.Minute, took: 8 * time.Minute, window: 10 * time.Minute},
			want: refreshCadence,
		}, {
			// Exactly at the window is not past it: the next window is the
			// same size, so the lag holds where it is.
			name: "exactly at the window it folded",
			run:  refreshRun{interval: 5 * time.Minute, took: 10 * time.Minute, window: 10 * time.Minute},
			want: refreshCadence,
		}, {
			name: "past its window, with nothing to compare against",
			run:  refreshRun{interval: 5 * time.Minute, took: 26 * time.Minute, window: 20 * time.Minute},
			want: refreshOverWindow,
		},

		// The counterexample that killed the one-comparison rule. A five
		// minute interval and a rewrite floor of eight minutes: the first run
		// is past its window and the second is not, and NOTHING is falling
		// behind. Under the rule this replaced these two lines fired minutes
		// apart, contradicted each other, and the first was the false one.
		{
			name: "a fixed rewrite floor, first run: past its window",
			run:  refreshRun{interval: 5 * time.Minute, took: 8 * time.Minute, window: 5 * time.Minute},
			want: refreshOverWindow,
		}, {
			name: "a fixed rewrite floor, second run: the larger window is absorbed",
			run:  refreshRun{interval: 5 * time.Minute, took: 8 * time.Minute, window: 10 * time.Minute},
			prev: refreshPace{window: 5 * time.Minute, took: 8 * time.Minute},
			want: refreshCadence,
		},

		// The measured series from #1693: the window grew and the duration
		// grew with it. Two consecutive published runs are what says so.
		{
			// 94 seconds more cost for 65 seconds more window: slope above one.
			name: "the measured series, second run: cost rose FASTER than the window",
			run:  refreshRun{interval: 5 * time.Minute, took: 579 * time.Second, window: 485 * time.Second},
			prev: refreshPace{window: 420 * time.Second, took: 485 * time.Second, fold: 485 * time.Second},
			want: refreshFallingBehind,
		},

		// Convergence grows BOTH numbers too, which is why direction is not
		// enough. A fold costing eight minutes plus 0.3 of the window settles
		// at a fifteen minute window; every step on the way up is past its own
		// window and longer than the step before it, and none of them is a
		// runaway. Ninety seconds of extra cost for five minutes of extra
		// window is a slope of 0.3, and the run after this one lands inside
		// its window and stops.
		{
			name: "climbing toward a fixed point is not falling behind",
			run:  refreshRun{interval: 5 * time.Minute, took: 11 * time.Minute, window: 10 * time.Minute},
			prev: refreshPace{window: 5 * time.Minute, took: 570 * time.Second, fold: 570 * time.Second},
			want: refreshOverWindow,
		}, {
			// The same server one step later, landing inside its window.
			name: "the step that reaches the fixed point",
			run:  refreshRun{interval: 5 * time.Minute, took: 750 * time.Second, window: 15 * time.Minute},
			prev: refreshPace{window: 10 * time.Minute, took: 11 * time.Minute, fold: 11 * time.Minute},
			want: refreshCadence,
		},

		// A destination that slowed down grows took and the window with it,
		// and none of that is the fold costing more per minute of window.
		// Fold 6m then 6m30s, upload 5m then 9m.
		{
			// The growth sits in the UPLOAD, and the reading still fires: the
			// next run waits for the upload too, so the backup falls behind
			// whichever half grew. A rule that ran the slope on the fold alone
			// stayed silent right through this. Fold 6m then 10m, upload 5m
			// then 10m, window 10m then 15m: took grew 9m against 5m.
			name: "growth that sits in the upload is still falling behind",
			run: refreshRun{interval: 5 * time.Minute, took: 20 * time.Minute, upload: 10 * time.Minute,
				window: 15 * time.Minute},
			prev: refreshPace{window: 10 * time.Minute, took: 11 * time.Minute, fold: 6 * time.Minute},
			want: refreshFallingBehind,
		}, {
			// The mirror, and the reason the reading cannot run on the fold:
			// the fold's slope is above one (8m to 14m against 5m of window)
			// while the whole run got SHORTER, because a stalled destination
			// recovered. Nothing is falling behind in that instant, and the
			// next window is smaller than this one.
			name: "the fold grew but a recovered upload made the run shorter",
			run: refreshRun{interval: 5 * time.Minute, took: 16 * time.Minute, upload: 3 * time.Minute,
				window: 15 * time.Minute},
			prev: refreshPace{window: 10 * time.Minute, took: 20 * time.Minute, fold: 6 * time.Minute},
			want: refreshOverWindow,
		},

		// Three ways the second sample refuses to confirm.
		{
			// The window grew and the cost grew, and it is still not two runs
			// of falling behind: the previous run finished INSIDE its window.
			// One run's cost can jump for its own reasons (a burst of writes,
			// a schema change), and the next run is what says whether it was
			// the start of something.
			name: "the previous run was inside its own window",
			run:  refreshRun{interval: 5 * time.Minute, took: 26 * time.Minute, window: 20 * time.Minute},
			prev: refreshPace{window: 10 * time.Minute, took: 8 * time.Minute, fold: 8 * time.Minute},
			want: refreshOverWindow,
		}, {
			// A full backup published between the two runs resets the anchor,
			// so this run's window is SMALLER than the previous one's. That is
			// the way out of the loop, not evidence of it.
			name: "the window shrank between the two runs",
			run:  refreshRun{interval: 5 * time.Minute, took: 30 * time.Minute, window: 5 * time.Minute},
			prev: refreshPace{window: 20 * time.Minute, took: 26 * time.Minute, fold: 26 * time.Minute},
			want: refreshOverWindow,
		}, {
			name: "the window grew but the run did not cost more",
			run:  refreshRun{interval: 5 * time.Minute, took: 25 * time.Minute, window: 22 * time.Minute},
			prev: refreshPace{window: 20 * time.Minute, took: 26 * time.Minute, fold: 26 * time.Minute},
			want: refreshOverWindow,
		},

		// A pace whose window was never measured must not confirm anything:
		// prev.took > prev.window is true for {0, anything positive}.
		{
			// Chosen so the SLOPE passes (18m of cost for 10m of window) and
			// only the window guard stands between this and the strong
			// reading; otherwise the case would pass with the guard deleted.
			name: "the previous sample carries no window",
			run:  refreshRun{interval: 5 * time.Minute, took: 26 * time.Minute, window: 10 * time.Minute},
			prev: refreshPace{window: 0, took: 8 * time.Minute, fold: 8 * time.Minute},
			want: refreshOverWindow,
		}, {
			name: "no previous run at all",
			run:  refreshRun{interval: 5 * time.Minute, took: 26 * time.Minute, window: 20 * time.Minute},
			prev: refreshPace{},
			want: refreshOverWindow,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := gradeRefresh(tc.run, tc.prev); got != tc.want {
				t.Errorf("gradeRefresh(%+v, %+v) = %v, want %v", tc.run, tc.prev, got, tc.want)
			}
		})
	}
}

// A run nobody could measure a window for must not become half of a two-run
// claim, and the only place that is enforced is where the sample is built.
func TestRefreshSample(t *testing.T) {
	if got := refreshSample(refreshRun{took: 26 * time.Minute}); got != (refreshPace{}) {
		t.Errorf("a run with no window left a sample behind: %+v", got)
	}
	got := refreshSample(refreshRun{took: 26 * time.Minute, upload: 6 * time.Minute, window: 20 * time.Minute})
	want := refreshPace{window: 20 * time.Minute, took: 26 * time.Minute, fold: 20 * time.Minute}
	if got != want {
		t.Errorf("sample = %+v, want %+v: the fold is took without the upload, and the next run needs it "+
			"apart from took to say which half of its own growth actually grew", got, want)
	}
}

func captureOverrun(t *testing.T, run refreshRun, prev refreshPace) string {
	t.Helper()
	restore := slog.Default()
	t.Cleanup(func() { slog.SetDefault(restore) })
	var buf bytes.Buffer
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
	reportRefreshDuration("srv", run, prev)
	return buf.String()
}

func TestReportRefreshDuration_saysWhatEachReadingSupports(t *testing.T) {
	// The advice #1693 measured as wrong, in the two forms it took.
	const wrongAttribution = "cost of the rewrite"
	const wrongRemedy = "Raise the interval"

	t.Run("falling behind names both samples and the way out", func(t *testing.T) {
		out := captureOverrun(t,
			refreshRun{interval: 5 * time.Minute, took: 579 * time.Second, window: 485 * time.Second},
			refreshPace{window: 420 * time.Second, took: 485 * time.Second, fold: 485 * time.Second})
		for _, want := range []string{"level=WARN", "second published run in a row", "further behind than the one before it did",
			"A longer interval does not change that", "full read", "took=9m39s", "window=8m5s",
			"previous_window=7m0s", "previous_took=8m5s", "server=srv",
			// The reading is over the WHOLE run, upload included, so the
			// remedy may not point at the fold alone: an S3 destination that
			// slowed down grows took exactly as a slower fold does, and the
			// line would otherwise send the operator to a table whose own
			// time never moved. It points at the measured half instead.
			"growth_mostly_in on this line points", "growth_mostly_in="} {
			if !strings.Contains(out, want) {
				t.Errorf("the falling-behind reading lost %q: %q", want, out)
			}
		}
		for _, bad := range []string{wrongRemedy, wrongAttribution} {
			if strings.Contains(out, bad) {
				t.Errorf("the falling-behind reading still says %q, which #1693 measured as wrong: %q", bad, out)
			}
		}
	})

	// The reading is over the whole run, so the growth can sit in either half,
	// and which half decides where an operator spends the afternoon. Both
	// cases below reach the SAME reading and differ only in the attribution,
	// and the previous run's upload is non-zero in both: computing the fold's
	// growth against the previous run's whole duration instead of its fold
	// flips the answer, which is the one arithmetic slip available here.
	t.Run("the growth is attributed to the half that actually grew", func(t *testing.T) {
		// prev: window 10m, took 20m of which 12m upload, so fold 8m.
		// run:  window 15m, took 26m of which 12m upload, so fold 14m.
		// The fold grew 6m, the upload not at all.
		foldSide := captureOverrun(t,
			refreshRun{interval: 5 * time.Minute, took: 26 * time.Minute, upload: 12 * time.Minute,
				window: 15 * time.Minute},
			refreshPace{window: 10 * time.Minute, took: 20 * time.Minute, fold: 8 * time.Minute})
		for _, want := range []string{`growth_mostly_in="the fold"`, "fold_grew_by=6m0s", "upload_grew_by=0s"} {
			if !strings.Contains(foldSide, want) {
				t.Errorf("the fold grew and the upload did not, and the line does not say so: missing %q in %q", want, foldSide)
			}
		}
		// prev: window 10m, took 11m of which 5m upload, so fold 6m.
		// run:  window 15m, took 20m of which 10m upload, so fold 10m.
		// Fold grew 4m, upload grew 5m.
		uploadSide := captureOverrun(t,
			refreshRun{interval: 5 * time.Minute, took: 20 * time.Minute, upload: 10 * time.Minute,
				window: 15 * time.Minute},
			refreshPace{window: 10 * time.Minute, took: 11 * time.Minute, fold: 6 * time.Minute})
		for _, want := range []string{`growth_mostly_in="the upload`, "fold_grew_by=4m0s", "upload_grew_by=5m0s"} {
			if !strings.Contains(uploadSide, want) {
				t.Errorf("the destination grew more than the fold and the line blamed the fold: missing %q in %q", want, uploadSide)
			}
		}
		// Neither line may send the reader to the slowest table on its own,
		// which is the misdirection this attribution replaced.
		for _, out := range []string{foldSide, uploadSide} {
			if !strings.Contains(out, "growth_mostly_in on this line points") {
				t.Errorf("the remedy does not point at the measured half: %q", out)
			}
		}
	})

	t.Run("one run past its window says only that", func(t *testing.T) {
		out := captureOverrun(t,
			refreshRun{interval: 5 * time.Minute, took: 8 * time.Minute, window: 5 * time.Minute},
			refreshPace{})
		for _, want := range []string{"level=WARN", "larger window", "On its own that is not falling behind",
			"not what this pair of runs shows", "took=8m0s", "window=5m0s"} {
			if !strings.Contains(out, want) {
				t.Errorf("the one-run reading lost %q: %q", want, out)
			}
		}
		// The whole point of the second sample: this line must not make the
		// claim that needs two runs, and must not carry samples it has not got.
		for _, bad := range []string{"further behind than the one before it", "previous_window=", "previous_took="} {
			if strings.Contains(out, bad) {
				t.Errorf("one run made a claim that needs two, or showed a sample it has not got: %q in %q", bad, out)
			}
		}

		// This reading is the CATCH-ALL, not only "no second run yet": it is
		// also what a run gets when the comparison DID run and came out
		// negative. When a sample exists it goes on the line, or the reader is
		// told no comparison happened when one did.
		compared := captureOverrun(t,
			refreshRun{interval: 5 * time.Minute, took: 11 * time.Minute, window: 10 * time.Minute},
			refreshPace{window: 5 * time.Minute, took: 570 * time.Second, fold: 570 * time.Second})
		for _, want := range []string{"previous_window=5m0s", "previous_took=9m30s", "not what this pair of runs shows"} {
			if !strings.Contains(compared, want) {
				t.Errorf("the comparison ran and came out negative, and the line hid it: missing %q in %q", want, compared)
			}
		}
	})

	t.Run("a longer cadence is where raising the interval belongs", func(t *testing.T) {
		out := captureOverrun(t,
			refreshRun{interval: 5 * time.Minute, took: 8 * time.Minute, window: 10 * time.Minute},
			refreshPace{})
		for _, want := range []string{"level=WARN", "no larger than this one", "lag is not growing",
			wrongRemedy, "took=8m0s", "window=10m0s", "those are the parts that were measured"} {
			if !strings.Contains(out, want) {
				t.Errorf("the cadence reading lost %q: %q", want, out)
			}
		}
		if strings.Contains(out, "further behind than the one before it") || strings.Contains(out, wrongAttribution) {
			t.Errorf("the cadence reading overstates: %q", out)
		}
	})

	// Reachable only through a clock that moved or a snapshot directory
	// stamped in the future: a run with no snapshot at all refuses before it
	// ever reports a duration. So the line has to name THAT, not "unknown".
	t.Run("an unmeasurable window names why it is unmeasurable", func(t *testing.T) {
		out := captureOverrun(t,
			refreshRun{interval: 5 * time.Minute, took: 12 * time.Minute, window: 0},
			refreshPace{})
		for _, want := range []string{"level=WARN", "could not be told", "at or after this run's own instant",
			"clock", "took=12m0s"} {
			if !strings.Contains(out, want) {
				t.Errorf("the unmeasurable-window reading lost %q: %q", want, out)
			}
		}
		if strings.Contains(out, "window=") {
			t.Errorf("a window nobody measured was printed as a value, which reads as a measurement: %q", out)
		}
	})

	// One grep has to find every overrun line. The readings split after this
	// clause, and a reader who searches for the wording they saw last week
	// must not silently miss the one that changed (#1693 review).
	t.Run("every reading past the interval shares one opening clause", func(t *testing.T) {
		const shared = "baseline refresh: this server's refresh took longer than the configured interval"
		runs := []struct {
			run  refreshRun
			prev refreshPace
		}{
			{refreshRun{interval: 5 * time.Minute, took: 579 * time.Second, window: 485 * time.Second},
				refreshPace{window: 420 * time.Second, took: 485 * time.Second, fold: 485 * time.Second}},
			{refreshRun{interval: 5 * time.Minute, took: 8 * time.Minute, window: 5 * time.Minute}, refreshPace{}},
			{refreshRun{interval: 5 * time.Minute, took: 8 * time.Minute, window: 10 * time.Minute}, refreshPace{}},
			{refreshRun{interval: 5 * time.Minute, took: 12 * time.Minute, window: 0}, refreshPace{}},
		}
		for i, r := range runs {
			if out := captureOverrun(t, r.run, r.prev); !strings.Contains(out, shared) {
				t.Errorf("reading %d does not open with the shared clause: %q", i, out)
			}
		}
	})

	t.Run("inside the interval stays quiet whatever the window says", func(t *testing.T) {
		if out := captureOverrun(t, refreshRun{interval: time.Hour, took: 5 * time.Minute, window: time.Minute}, refreshPace{}); out != "" {
			t.Errorf("a refresh inside its interval warned: %q", out)
		}
	})
}

// Everything the line reports about cost is measured, so everything it does
// not measure has to be absent rather than zero: an empty table name and a 0s
// duration both read as findings.
func TestReportRefreshDuration_measuredCostOnly(t *testing.T) {
	base := refreshRun{interval: 5 * time.Minute, took: 26 * time.Minute, window: 20 * time.Minute}

	full := base
	full.upload = 90 * time.Second
	full.cost = reuseTally{slowest: "shop.order_line", slowestTook: 22 * time.Minute, events: 3_120_269}
	out := captureOverrun(t, full, refreshPace{})
	for _, want := range []string{"slowest_table=shop.order_line", "slowest_took=22m0s",
		"events_applied=3120269", "upload_took=1m30s"} {
		if !strings.Contains(out, want) {
			t.Errorf("a measured number did not reach the line: missing %q in %q", want, out)
		}
	}
	if strings.Contains(out, "slowest_carried_forward") {
		t.Errorf("a rewritten table was reported as carried forward: %q", out)
	}

	none := captureOverrun(t, base, refreshPace{})
	for _, bad := range []string{"slowest_table=", "slowest_took=", "events_applied=", "upload_took="} {
		if strings.Contains(none, bad) {
			t.Errorf("a fold that measured nothing printed %q, which reads as a measurement: %q", bad, none)
		}
	}

	// A run that carried 18 tables and then carries none costs far more for a
	// reason that is not the window, and the fold's flip to the bucket (where
	// carrying forward is refused outright) is reported at Debug. So on a
	// server that asked for reuse the count goes on the warning line, and it
	// goes there AT ZERO: zero is the measurement here, not its absence.
	off := base
	off.cost = reuseTally{reused: 0}
	if got := captureOverrun(t, off, refreshPace{}); strings.Contains(got, "reused=") {
		t.Errorf("a server that never asked for reuse was told how much it reused: %q", got)
	}
	on := base
	on.reuseEnabled = true
	on.cost = reuseTally{reused: 0}
	if got := captureOverrun(t, on, refreshPace{}); !strings.Contains(got, "reused=0") {
		t.Errorf("a reuse server that carried nothing did not say so, which is the one number that "+
			"separates a growing window from a fold that stopped reusing: %q", got)
	}
	some := base
	some.reuseEnabled = true
	some.cost = reuseTally{reused: 18, copied: 2}
	if got := captureOverrun(t, some, refreshPace{}); !strings.Contains(got, "reused=18") || !strings.Contains(got, "reused_copied=2") {
		t.Errorf("the reuse counts did not reach the line: %q", got)
	}

	// A carried-forward table was never rewritten: its time went to reading
	// the events that showed it had none (#1689). The advice on this line is
	// about what a table costs to write, so the line has to say when the
	// slowest one was not written.
	carried := base
	carried.cost = reuseTally{slowest: "shop.archive", slowestTook: 19 * time.Minute, slowestCarried: true}
	if got := captureOverrun(t, carried, refreshPace{}); !strings.Contains(got, "slowest_carried_forward=true") {
		t.Errorf("the slowest table was carried forward and the line did not say so: %q", got)
	}
}

// countReuse is where the per-table reports are last seen before they become
// counts, so the fold's cost has to be tallied there or it is gone.
func TestCountReuse_carriesTheFoldCost(t *testing.T) {
	rep := func(schema, table string, took time.Duration, events int64) *reconstruct.TableReport {
		return &reconstruct.TableReport{Schema: schema, Table: table, Duration: took, EventsApplied: events}
	}
	got := countReuse([]*reconstruct.TableReport{
		rep("shop", "users", 3*time.Second, 100),
		nil, // a nil report must not be dereferenced, same as the reuse half
		rep("shop", "order_line", 22*time.Minute, 3_000_000),
		rep("shop", "orders", 4*time.Minute, 120_269),
	})
	if got.slowest != "shop.order_line" || got.slowestTook != 22*time.Minute {
		t.Errorf("slowest = %s (%v), want shop.order_line (22m)", got.slowest, got.slowestTook)
	}
	if got.slowestCarried {
		t.Errorf("a rewritten table was tallied as carried forward: %+v", got)
	}
	if got.events != 3_120_369 {
		t.Errorf("events = %d, want the sum 3120369", got.events)
	}

	// The slowest table of a run can be one that was never rewritten: the
	// event scan runs before the carry decision (#1689).
	slowCarry := &reconstruct.TableReport{Schema: "shop", Table: "archive",
		Duration: time.Hour, CarriedForward: true, CarriedByLink: true}
	if c := countReuse([]*reconstruct.TableReport{rep("shop", "users", time.Second, 1), slowCarry}); !c.slowestCarried || c.slowest != "shop.archive" {
		t.Errorf("a carried-forward table was the slowest and the tally hid it: %+v", c)
	}

	// Reports that carry no duration (every table carried forward, or a fake
	// fold) leave the cost zero, so the reuse-only equality checks elsewhere
	// keep meaning what they say.
	if z := countReuse([]*reconstruct.TableReport{{CarriedForward: true, CarriedByLink: true}, {CarriedForward: true, CarriedByLink: true}}); z != (reuseTally{reused: 2}) {
		t.Errorf("zero-duration reports produced a cost: %+v", z)
	}
}

func TestFoldWindow(t *testing.T) {
	at := time.Date(2026, 9, 16, 20, 50, 27, 0, time.UTC)
	cases := []struct {
		name string
		prev time.Time
		want time.Duration
	}{
		{"no previous snapshot", time.Time{}, 0},
		{"previous equals this run", at, 0},
		{"previous after this run (clock moved)", at.Add(time.Minute), 0},
		{"previous before this run", at.Add(-20 * time.Minute), 20 * time.Minute},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := foldWindow(tc.prev, at); got != tc.want {
				t.Errorf("foldWindow = %v, want %v", got, tc.want)
			}
		})
	}
}

// stubFold answers the two seams a whole refresh needs, and reports where the
// snapshot listing was actually pointed. The durations are real: a fake fold
// takes microseconds, so a previous snapshot one nanosecond before the anchor
// makes this run outrun its window for real, and one an hour before makes it
// keep up for real. No hand-written `took` anywhere below.
func stubFold(t *testing.T, prevBefore, foldFor time.Duration) *[]string {
	t.Helper()
	realList, realFold := newestSnapshotTables, foldTables
	t.Cleanup(func() { newestSnapshotTables, foldTables = realList, realFold })
	var listed []string
	newestSnapshotTables = func(_ context.Context, src string) (time.Time, []string, error) {
		listed = append(listed, src)
		return refreshAt.Add(-prevBefore), []string{"shop.orders", "shop.users"}, nil
	}
	foldTables = func(_ context.Context, cfg reconstruct.FullTableConfig) (
		[]*reconstruct.TableReport, []reconstruct.TableFailure, error) {
		// Real time, so the run's own duration separates the fold from the
		// upload instead of both rounding to the same microsecond.
		time.Sleep(foldFor)
		writeSnapshotFiles(t, filepath.Join(cfg.OutputDir, reconstruct.SnapshotDirName(cfg.At)), baseline.SuccessMarker)
		return []*reconstruct.TableReport{
			{Schema: "shop", Table: "orders", Duration: 3 * time.Millisecond, EventsApplied: 40},
			{Schema: "shop", Table: "users", Duration: time.Millisecond, EventsApplied: 2},
		}, nil, nil
	}
	return &listed
}

// attrDuration pulls one duration attribute off a slog text line, so a test
// can compare two of them instead of matching a formatted string.
func attrDuration(t *testing.T, line, key string) time.Duration {
	t.Helper()
	// \b matters: without it, a search for "took" finds "upload_took" first,
	// and the two are exactly the pair this helper exists to compare.
	m := regexp.MustCompile(`\b` + regexp.QuoteMeta(key) + `=(\S+)`).FindStringSubmatch(line)
	if m == nil {
		t.Fatalf("no %s= on the line: %q", key, line)
	}
	d, err := time.ParseDuration(m[1])
	if err != nil {
		t.Fatalf("%s=%q is not a duration: %v", key, m[1], err)
	}
	return d
}

// The reading is only as good as the numbers runRefresh feeds it, and the
// window is the one that can come from the wrong place: the fold reads the
// PREVIOUS snapshot from baselineFoldSource(req), which on an S3-backed server
// is the bucket and not the local directory the snapshot is written to. A
// window measured against the local directory there would be the age of
// whatever this daemon happened to have folded since it started.
func TestRunRefresh_theWindowComesFromWhereTheFoldRead(t *testing.T) {
	uploads, _ := stubS3Fold(t, nil, nil)                 // for uploadSnapshot and the bucket listing
	listed := stubFold(t, time.Hour, 30*time.Millisecond) // overrides newestSnapshotTables again, after stubS3Fold

	restore := slog.Default()
	t.Cleanup(func() { slog.SetDefault(restore) })
	var buf bytes.Buffer
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	sup := newBaselineSupervisor(ctx, t.TempDir(), baseline.DefaultLockMode)
	sup.refreshes["s"] = &console.BaselineStatus{State: "running"}
	local := t.TempDir()
	req := refreshRequest{ServerID: "s", ServerName: "s", IndexDSN: "d",
		BaselineDir: local, BaselineS3: "s3://bucket/prefix"}
	sup.runRefresh(req, refreshAt, time.Nanosecond)

	if len(*uploads) == 0 {
		t.Fatalf("the run did not reach the upload, so it did not publish and no reading was produced")
	}
	if len(*listed) != 1 || (*listed)[0] != "s3://bucket/prefix" {
		t.Fatalf("the fold listed %v, want only the bucket: the window must be measured against the "+
			"snapshot the fold actually read, not against %s", *listed, local)
	}
	out := buf.String()
	if !strings.Contains(out, "window=1h0m0s") {
		t.Errorf("the window on the line is not the one the listing returned: %q", out)
	}
	// This server HAS a destination, so the upload span is a real measurement
	// and belongs on the line. Its absence here, next to its absence on the
	// local run below, is what pins the span to the upload itself rather than
	// to the difference between two whole-run readings.
	if !strings.Contains(out, "upload_took=") {
		t.Errorf("a run that uploaded did not report how long the upload took: %q", out)
	}
	// And it is the upload's OWN slice, not the run restated. The stub fold
	// sleeps 30ms and the stub upload returns at once, so an upload span that
	// reaches anywhere near the total is measuring the wrong thing.
	if up, all := attrDuration(t, out, "upload_took"), attrDuration(t, out, "took"); up > all/2 {
		t.Errorf("upload_took=%v of took=%v, but the fold is the part that slept: the upload span is "+
			"being measured over more than the upload", up, all)
	}
	// And the sample this run LEAVES carries the same split, so the next run
	// can say which half grew. A sample built from a second literal, or built
	// without the upload, stores fold == took and the attribution is gone.
	kept := sup.refreshPaces["s"]
	if kept.fold <= 0 || kept.fold >= kept.took {
		t.Errorf("stored sample = %+v: a run that uploaded must leave a fold strictly under its total, or "+
			"the next run cannot attribute its own growth", kept)
	}
}

// gradeRefresh is exercised exhaustively above, but only ONE path in the
// daemon can reach its strongest reading, and it reaches it with a sample this
// function has to have read back out of the supervisor. Tested apart from the
// pure function because a pace that never leaves the map makes
// refreshFallingBehind dead code with the whole suite green.
func TestRunRefresh_theStrongestReadingIsReachableFromTheLoop(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	sup := newBaselineSupervisor(ctx, t.TempDir(), baseline.DefaultLockMode)
	sup.refreshes["s"] = &console.BaselineStatus{State: "running"}
	// A previous run that was itself past its window, over a window smaller
	// than this run's and in less time than this run will take. The fold below
	// sleeps 20ms, so "less time than this run" is anything under that.
	sup.refreshPaces["s"] = refreshPace{window: 100 * time.Microsecond, took: 200 * time.Microsecond,
		fold: 200 * time.Microsecond}

	restore := slog.Default()
	t.Cleanup(func() { slog.SetDefault(restore) })
	var buf bytes.Buffer
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))

	stubFold(t, time.Millisecond, 20*time.Millisecond)
	sup.runRefresh(refreshRequest{ServerID: "s", ServerName: "s", IndexDSN: "d", BaselineDir: t.TempDir(),
		// Set here and nowhere else: whether the reuse counts appear is this
		// server's setting, so the reading has to carry it across from the
		// request rather than infer it from a count that is zero either way.
		CarryForwardUnchanged: true},
		refreshAt, time.Nanosecond)

	out := buf.String()
	for _, want := range []string{"further behind than the one before it", "previous_window=100µs",
		"previous_took=200µs", "reused=0"} {
		if !strings.Contains(out, want) {
			t.Errorf("the loop never reached the strongest reading, or reached it without the sample it "+
				"rests on: missing %q in %q", want, out)
		}
	}
	// And this run replaced the sample rather than leaving the seeded one.
	if got := sup.refreshPaces["s"]; got.window != time.Millisecond {
		t.Errorf("pace after the run = %+v, want this run's own window", got)
	}
}

// executeRefresh returns an instant and an error side by side, and zero is its
// word for "no window". The fold's OWN refusals (a capture gap, a schema
// change, the touched row budget) are the common error here and they arrive
// with a real listed instant in hand, so that path is where the convention
// gets broken quietly: the two listing errors above it never had an instant to
// leak in the first place.
func TestExecuteRefresh_anErrorNeverCarriesAnInstant(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	sup := newBaselineSupervisor(ctx, t.TempDir(), baseline.DefaultLockMode)

	listed := refreshAt.Add(-42 * time.Minute)
	realList, realFold := newestSnapshotTables, foldTables
	t.Cleanup(func() { newestSnapshotTables, foldTables = realList, realFold })
	newestSnapshotTables = func(context.Context, string) (time.Time, []string, error) {
		return listed, []string{"shop.orders"}, nil
	}
	foldTables = func(context.Context, reconstruct.FullTableConfig) (
		[]*reconstruct.TableReport, []reconstruct.TableFailure, error) {
		return nil, []reconstruct.TableFailure{{Schema: "shop", Table: "orders",
			Err: reconstruct.ErrCaptureGap}}, reconstruct.ErrCaptureGap
	}

	prev, _, _, _, err := sup.executeRefresh(refreshRequest{ServerID: "s", ServerName: "s", IndexDSN: "d",
		BaselineDir: t.TempDir()}, refreshAt)
	if err == nil {
		t.Fatal("the stubbed fold did not fail, so the error path was never taken")
	}
	if !prev.IsZero() {
		t.Errorf("a refused fold returned the instant it had listed (%v) beside its error; zero is this "+
			"return's word for \"no window\", and a caller handed both has to guess which convention holds", prev)
	}
}

// The second sample lives on the supervisor, and two things have to be true of
// it: a published run leaves it, and a run that publishes nothing takes it away. The refusal
// matters because a refused run publishes nothing, so the next window reaches
// back past it for a reason that has nothing to do with the fold's cost.
func TestRunRefresh_theSampleIsKeptOnPublishAndDroppedOnRefusal(t *testing.T) {
	// Asserts on a Warn buffer; the gate's blindness warning must not be able
	// to satisfy it. See stubGateReads.
	stubGateReads(t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	sup := newBaselineSupervisor(ctx, t.TempDir(), baseline.DefaultLockMode)
	sup.refreshes["s"] = &console.BaselineStatus{State: "running"}

	restore := slog.Default()
	t.Cleanup(func() { slog.SetDefault(restore) })
	var buf bytes.Buffer
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))

	stubFold(t, time.Hour, 0)
	req := refreshRequest{ServerID: "s", ServerName: "s", IndexDSN: "d", BaselineDir: t.TempDir()}
	sup.runRefresh(req, refreshAt, time.Nanosecond)

	// No destination, so no upload happened and the line must not carry a
	// span for one. A span derived from two whole-run readings is never
	// exactly zero, so it would show up here as a few hundred nanoseconds of
	// upload this server cannot perform.
	if out := buf.String(); strings.Contains(out, "upload_took=") {
		t.Errorf("a server whose snapshots stay on disk reported an upload duration: %q", out)
	}

	kept, ok := sup.refreshPaces["s"]
	if !ok || kept.window != time.Hour {
		t.Fatalf("pace after a published run = %+v (present=%v), want the hour-long window it folded", kept, ok)
	}
	if kept.took <= 0 {
		t.Errorf("pace kept no duration, so the next run has nothing to compare against: %+v", kept)
	}

	// An empty baseline directory makes executeRefresh refuse: nothing to fold.
	newestSnapshotTables = reconstruct.NewestSnapshot
	sup.refreshes["s"].State = "running"
	sup.runRefresh(refreshRequest{ServerID: "s", ServerName: "s", IndexDSN: "d", BaselineDir: t.TempDir()},
		refreshAt.Add(time.Hour), time.Nanosecond)
	if got, ok := sup.refreshPaces["s"]; ok {
		t.Errorf("a refused run left the previous sample in place (%+v); the next run would read the "+
			"window the refusal opened as the source outrunning the fold", got)
	}
}

// The refusal branch is not the only exit that publishes nothing. A panic in
// the fold is caught by the job's own recover (#1472, #1497) and unwinds past
// everything at the end of runRefresh, so a sample cleared only there would
// survive a crash and be compared against by the next published run, over a
// window that grew because the daemon crashed.
func TestRunRefresh_aPanicLeavesNoSampleBehind(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	sup := newBaselineSupervisor(ctx, t.TempDir(), baseline.DefaultLockMode)
	sup.refreshes["s"] = &console.BaselineStatus{State: "running"}
	sup.refreshPaces["s"] = refreshPace{window: 10 * time.Minute, took: 26 * time.Minute, fold: 26 * time.Minute}

	stubFold(t, time.Hour, 0)
	realFold := foldTables
	t.Cleanup(func() { foldTables = realFold })
	foldTables = func(context.Context, reconstruct.FullTableConfig) (
		[]*reconstruct.TableReport, []reconstruct.TableFailure, error) {
		panic("the fold died mid-run")
	}

	sup.runRefresh(refreshRequest{ServerID: "s", ServerName: "s", IndexDSN: "d", BaselineDir: t.TempDir()},
		refreshAt, time.Nanosecond)

	if got, ok := sup.refreshPaces["s"]; ok {
		t.Errorf("a run that panicked left the previous sample in place (%+v); the next published run "+
			"would read the window the crash opened as the cost rising with the window", got)
	}
}
