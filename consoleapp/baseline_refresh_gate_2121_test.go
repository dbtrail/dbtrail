package consoleapp

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/rotation"
)

// #2121: the margin of an hour and an interval above the floor exists because
// rotation can move the floor before the next cycle looks. On an index younger
// than its retention rotation has nothing to drop, and the margin ended every
// chain of the first stretch: each update wrote every table in full.

// The issue's own timeline: capture from 03:28, first full read at 03:38:54,
// a 5-minute schedule, the default 48h retention.
func TestCoverageRule_aYoungIndexKeepsItsFirstChain(t *testing.T) {
	floor := time.Date(2026, 10, 5, 3, 0, 0, 0, time.UTC)
	chainStart := time.Date(2026, 10, 5, 3, 38, 54, 0, time.UTC)
	rule := coverageRule{retain: 48 * time.Hour, interval: 5 * time.Minute, dropsAfter: 48 * time.Hour}
	for at := time.Date(2026, 10, 5, 3, 45, 41, 0, time.UTC); at.Before(floor.Add(3 * time.Hour)); at = at.Add(5 * time.Minute) {
		line, ok := rule.reanchorBy(floor, at)
		if !ok {
			t.Fatalf("%s: no line with a known floor", at.Format(time.TimeOnly))
		}
		if !chainStart.After(line) {
			t.Fatalf("%s: the chain that started at %s is ended on the line %s, with 48h before rotation reaches the oldest hour",
				at.Format(time.TimeOnly), chainStart.Format(time.TimeOnly), line.Format(time.TimeOnly))
		}
	}
	// Without knowing what the index drops on, the margin stays.
	blind := coverageRule{retain: 48 * time.Hour, interval: 5 * time.Minute}
	at := time.Date(2026, 10, 5, 3, 45, 41, 0, time.UTC)
	if line, _ := blind.reanchorBy(floor, at); chainStart.After(line) {
		t.Errorf("a rule that does not know the index's window dropped the margin: line %s", line.Format(time.TimeOnly))
	}
}

// Knowing the window never draws a later line, and changes nothing once
// rotation is within an hour and an interval of the oldest hour: every index
// that has been running for its retention is graded exactly as before.
func TestCoverageRule_theWindowOnlyMovesTheLineOnAYoungIndex(t *testing.T) {
	now := time.Date(2026, 9, 28, 12, 0, 0, 0, time.UTC)
	for _, retain := range []time.Duration{48 * time.Hour, 12 * time.Hour, 6 * time.Hour, 2 * time.Hour, time.Hour} {
		for _, interval := range []time.Duration{0, 5 * time.Minute, 30 * time.Minute, 2 * time.Hour} {
			before := coverageRule{retain: retain, interval: interval}
			after := coverageRule{retain: retain, interval: interval, dropsAfter: retain}
			reach := now.Add(-retain).Add(time.Hour + interval) // where the margin would sit over the policy's own floor
			for floor := now.Add(-60 * time.Hour); !floor.After(now.Add(2 * time.Hour)); floor = floor.Add(7 * time.Minute) {
				was, _ := before.reanchorBy(floor, now)
				is, _ := after.reanchorBy(floor, now)
				if is.After(was) {
					t.Fatalf("retain %s, interval %s, floor %s: the line moved LATER (%s to %s)", retain, interval, floor.Format(time.RFC3339), was, is)
				}
				if floor.Before(reach) && !is.Equal(was) {
					t.Fatalf("retain %s, interval %s, floor %s: rotation is within reach of the oldest hour and the line moved (%s to %s)",
						retain, interval, floor.Format(time.RFC3339), was, is)
				}
				if is.Before(floor) && now.After(floor) {
					t.Fatalf("retain %s, interval %s, floor %s: line %s is under the floor", retain, interval, floor.Format(time.RFC3339), is)
				}
			}
		}
	}
}

func TestCoverageRule_whenTheMarginComesBack(t *testing.T) {
	floor := time.Date(2026, 10, 5, 3, 0, 0, 0, time.UTC)
	chainStart := floor.Add(38 * time.Minute)
	const interval = 5 * time.Minute
	for _, tc := range []struct {
		name  string
		rule  coverageRule
		at    time.Time
		ended bool
	}{
		{"retention lowered to 2h in the panel, an hour before the drop: still out of reach",
			coverageRule{retain: 2 * time.Hour, interval: interval, dropsAfter: 2 * time.Hour}, floor.Add(50 * time.Minute), false},
		{"the same, once the drop is an hour and an interval away",
			coverageRule{retain: 2 * time.Hour, interval: interval, dropsAfter: 2 * time.Hour}, floor.Add(time.Hour), true},
		{"a retention no chain fits in: the margin never left",
			coverageRule{retain: time.Hour, interval: interval, dropsAfter: time.Hour}, floor.Add(45 * time.Minute), true},
		{"the daemon was down for 47h: rotation is about to reach the oldest hour",
			coverageRule{retain: 48 * time.Hour, interval: interval, dropsAfter: 48 * time.Hour}, floor.Add(47 * time.Hour), true},
		// The two retentions come from different reads and the shorter one
		// bounds: a panel save is in retain before the index's record says so.
		{"the configured retention is the shorter of the two",
			coverageRule{retain: 2 * time.Hour, interval: interval, dropsAfter: 48 * time.Hour}, floor.Add(time.Hour), true},
		{"the index's own window is the shorter of the two",
			coverageRule{retain: 48 * time.Hour, interval: interval, dropsAfter: 2 * time.Hour}, floor.Add(time.Hour), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			line, _ := tc.rule.reanchorBy(floor, tc.at)
			if got := !chainStart.After(line); got != tc.ended {
				t.Errorf("chain ended = %v, want %v (line %s)", got, tc.ended, line.Format(time.RFC3339))
			}
		})
	}
}

// youngResult is what a simulated index did from the hour it was created.
type youngResult struct {
	firstStretch int // rewrites forced by the line in the first two hours
	forced       int
	back2back    bool
	worst        time.Duration
}

// simulateFromBirth is simulate for an index that starts empty: its oldest
// partition is the hour it was created in, and rotation drops a partition as
// soon as its start is older than the retention (rotation.Perform's test,
// applied at every instant, which is the most a real loop can drop). The first
// full read lands ten minutes after the first event.
func simulateFromBirth(retain, interval, horizon time.Duration, busy bool) youngResult {
	born := time.Date(2026, 10, 5, 3, 28, 54, 0, time.UTC)
	floorAt := func(at time.Time) time.Time {
		floor := born.Truncate(time.Hour)
		if cut := at.Add(-retain); cut.After(floor) {
			floor = cut.Truncate(time.Hour)
			if floor.Before(cut) {
				floor = floor.Add(time.Hour)
			}
		}
		return floor
	}
	rule := coverageRule{retain: retain, interval: interval, dropsAfter: retain}
	start := born.Add(10 * time.Minute)
	res := youngResult{worst: time.Duration(1<<63 - 1)}
	rewrotePrev := false
	for at := start.Add(interval); !at.After(born.Add(horizon)); at = at.Add(interval) {
		floor := floorAt(at)
		if d := start.Sub(floor); d < res.worst {
			res.worst = d
		}
		line, _ := rule.reanchorBy(floor, at)
		if !busy && start.After(line) {
			rewrotePrev = false
			continue
		}
		rewrote := false
		switch {
		case at.Sub(start) > 24*time.Hour:
			rewrote = true
		case !start.After(line):
			res.forced++
			if at.Before(born.Add(2 * time.Hour)) {
				res.firstStretch++
			}
			rewrote = true
		}
		if rewrote {
			if rewrotePrev {
				res.back2back = true
			}
			start = at
		}
		rewrotePrev = rewrote
	}
	return res
}

func TestCoverageRule_aNewIndexDoesNotRewriteItsFirstStretch(t *testing.T) {
	const horizon = 6 * 24 * time.Hour
	for _, retain := range []time.Duration{48 * time.Hour, 24 * time.Hour, 12 * time.Hour, 6 * time.Hour, 4 * time.Hour} {
		for _, interval := range []time.Duration{5 * time.Minute, 30 * time.Minute, time.Hour} {
			for _, busy := range []bool{true, false} {
				r := simulateFromBirth(retain, interval, horizon, busy)
				name := retain.String() + " retention, every " + interval.String()
				if busy {
					name += ", busy"
				} else {
					name += ", quiet"
				}
				t.Run(name, func(t *testing.T) {
					if r.worst < 0 {
						t.Errorf("a reader's start fell %s below the floor: a restore would refuse", -r.worst)
					}
					if r.firstStretch != 0 {
						t.Errorf("%d rewrite(s) forced in the index's first two hours, with %s before rotation reaches its oldest hour",
							r.firstStretch, retain)
					}
					if r.back2back {
						t.Error("two cycles in a row rewrote the table: a rewrite storm")
					}
				})
			}
		}
	}
}

func stubIndexRetention(t *testing.T, e rotation.Effective) *atomic.Int32 {
	t.Helper()
	var n atomic.Int32
	prev := readIndexRetention
	t.Cleanup(func() { readIndexRetention = prev })
	readIndexRetention = func(context.Context, string) rotation.Effective {
		n.Add(1)
		return e
	}
	return &n
}

// The gate and the fold have to draw ONE line (#1904), so both are handed the
// same rule, with the window the index really drops on.
func TestRunRefresh_theGateAndTheFoldShareTheIndexsWindow(t *testing.T) {
	folds := probeFolds(t)
	covers := stubCoverage(t, true, true)
	floor := refreshAt.Add(-40 * time.Minute)
	stubLiveFloor(t, floor, true)
	chainStart := refreshAt.Add(-10 * time.Minute)
	stubReadsFrom(t, chainStart, nil)
	stubIndexRetention(t, rotation.Effective{Retain: 48 * time.Hour, Raw: "48h", Source: rotation.RetainRecorded})
	sup, _, run := deltasRig(t, true)
	// The daemon default differs from the index's record on purpose: what the
	// loop drops on is the record.
	sup.followRotation(func() rotation.Settings {
		return rotation.Settings{Enabled: true, Retain: 12 * time.Hour, RetainRaw: "12h"}
	}, true)

	run(refreshAt)
	want := coverageRule{retain: 12 * time.Hour, interval: 5 * time.Minute, dropsAfter: 48 * time.Hour}
	cfgs := folds.all()
	if len(cfgs) != 1 {
		t.Fatalf("folded %d time(s), want 1", len(cfgs))
	}
	line, _ := want.reanchorBy(floor, refreshAt)
	if got := cfgs[0].ChainStartFloor; !got.Equal(line) {
		t.Errorf("ChainStartFloor = %s, want %s: the fold did not draw its line with the index's window", got.Format(time.RFC3339Nano), line.Format(time.RFC3339Nano))
	}
	if !chainStart.After(cfgs[0].ChainStartFloor) {
		t.Errorf("a chain ten minutes old is ended on a 40-minute-old index (line %s)", cfgs[0].ChainStartFloor.Format(time.RFC3339))
	}

	run(refreshAt.Add(5 * time.Minute)) // quiet: the gate grades
	got := covers.coverage()
	if len(got) != 1 {
		t.Fatalf("coverage asked %d time(s), want 1", len(got))
	}
	if got[0].rule != want {
		t.Errorf("the gate graded with %+v, want %+v, the rule the fold drew its line with", got[0].rule, want)
	}
}

func TestCoverageRuleFor(t *testing.T) {
	recorded := rotation.Effective{Retain: 48 * time.Hour, Raw: "48h", Source: rotation.RetainRecorded}
	implicit := func() rotation.Settings { return rotation.Settings{Enabled: true, Retain: 12 * time.Hour} }
	explicit := func() rotation.Settings {
		return rotation.Settings{Enabled: true, Retain: 6 * time.Hour, Explicit: true}
	}
	for _, tc := range []struct {
		name      string
		settings  func() rotation.Settings
		loopOff   bool
		effective rotation.Effective
		want      coverageRule
		reads     int32
		warns     bool
	}{
		{"a supervisor that runs no rotation", nil, false, recorded, coverageRule{interval: time.Minute}, 0, false},
		{"rotation off",
			func() rotation.Settings { return rotation.Settings{} }, false, recorded,
			coverageRule{interval: time.Minute}, 0, false},
		{"an explicit window is the answer, with no read of the index",
			explicit, false, recorded,
			coverageRule{retain: 6 * time.Hour, interval: time.Minute, dropsAfter: 6 * time.Hour}, 0, false},
		// The daemon started with rotation off and a retention was saved in
		// the panel since: the settings read as enabled and the loop is not
		// running. Whatever rotates this index is not these settings.
		{"a retention saved on a daemon whose loop is not running vouches for nothing",
			explicit, true, recorded,
			coverageRule{retain: 6 * time.Hour, interval: time.Minute}, 0, false},
		{"no explicit window: the index's record",
			implicit, false, recorded,
			coverageRule{retain: 12 * time.Hour, interval: time.Minute, dropsAfter: 48 * time.Hour}, 1, false},
		{"a record that cannot be read vouches for nothing, and says so",
			implicit, false, rotation.Effective{Retain: 30 * 24 * time.Hour, Source: rotation.RetainUnreadable},
			coverageRule{retain: 12 * time.Hour, interval: time.Minute}, 1, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logs := captureSlogFor(t)
			reads := stubIndexRetention(t, tc.effective)
			sup := newBaselineSupervisor(context.Background(), t.TempDir(), "")
			if tc.settings != nil {
				sup.followRotation(tc.settings, !tc.loopOff)
			}
			req := refreshRequest{ServerID: "s", ServerName: "shop", IndexDSN: "d"}
			if got := sup.coverageRuleFor(context.Background(), req, time.Minute); got != tc.want {
				t.Errorf("rule = %+v, want %+v", got, tc.want)
			}
			if reads.Load() != tc.reads {
				t.Errorf("read the index's record %d time(s), want %d", reads.Load(), tc.reads)
			}
			// Twice a cycle, every cycle: the line is said once.
			sup.coverageRuleFor(context.Background(), req, time.Minute)
			if n := strings.Count(logs.String(), "cannot read the retention this index was created under"); (n == 1) != tc.warns || n > 1 {
				t.Errorf("said %d time(s) that the index's retention is unreadable, want said=%v and at most once:\n%s", n, tc.warns, logs.String())
			}
		})
	}
}
