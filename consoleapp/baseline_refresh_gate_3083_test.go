package consoleapp

import (
	"math"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/status"
)

// #3083: on an index younger than its retention, the first bound of
// reanchorBy (the aging band over now minus the oldest live hour) measured the
// install's age, so a table's file was rewritten whole once it was older than
// four fifths of it: in the first hours of every install, and again and again
// while the index was younger than its retention. While rotation is known to
// leave the oldest live hour alone until the next cycle (floorHolds), the line
// is the retention's own band, and never under that hour.

// lineBefore3083 is reanchorBy as it stood before #3083, kept here so the
// tests can say exactly where the new line differs from it and where it must
// not.
func lineBefore3083(r coverageRule, liveFloor, now time.Time) (time.Time, bool) {
	if liveFloor.IsZero() {
		return time.Time{}, false
	}
	line := liveFloor.Add(-time.Nanosecond)
	if span := now.Sub(liveFloor); span > 0 {
		line = now.Add(-time.Duration(math.Ceil(status.BaselineAgingFraction * float64(span))))
	}
	floor := liveFloor
	if r.retain > 0 {
		if p := now.Add(-time.Duration(float64(r.retain) * status.BaselineAgingFraction)); p.After(line) {
			line = p
		}
		if p := now.Add(-r.retain); p.After(floor) {
			floor = p
		}
	}
	if m := floor.Add(cycleMargin(r.interval)); m.After(line) && !r.floorHolds(liveFloor, now) {
		line = m
	}
	return line, true
}

// youngLine is what the line must be on a young index: the band over the
// window the index drops on, and never under its oldest hour.
func youngLine(window time.Duration, liveFloor, now time.Time) time.Time {
	line := now.Add(-time.Duration(math.Ceil(status.BaselineAgingFraction * float64(window))))
	if liveFloor.After(line) {
		line = liveFloor
	}
	return line
}

// The report's case: three hours and twenty minutes after the first snapshot,
// on the stock 48 hours, the table's file is not rewritten.
func TestCoverageRule_aYoungIndexDoesNotRewriteOnItsAge_3083(t *testing.T) {
	floor := time.Date(2026, 11, 21, 9, 0, 0, 0, time.UTC)
	first := floor.Add(12 * time.Minute)
	at := first.Add(3*time.Hour + 20*time.Minute)
	rule := coverageRule{retain: 48 * time.Hour, interval: 5 * time.Minute, dropsAfter: 48 * time.Hour}
	if old, _ := lineBefore3083(rule, floor, at); first.After(old) {
		t.Fatalf("the old line %s already kept the file: this test no longer shows the report", old.Format(time.RFC3339))
	}
	line, ok := rule.reanchorBy(floor, at)
	if !ok || !first.After(line) {
		t.Errorf("a file 3h20m old on a 3h32m-old index with 48h of retention is rewritten (line %s)", line.Format(time.RFC3339))
	}
	if !snapshotCoveredBy(floor, first, at, rule) {
		t.Error("the gate calls the same snapshot no longer covered")
	}
	// The verdict the Snapshots page shows, graded as the page grades it.
	if v := status.BaselineStalenessFor(first, floor, at); v != status.BaselineAging {
		t.Fatalf("on the span alone the file grades %s: this test no longer shows the warning", v)
	}
	shared := rule.shared()
	if v := (status.DeltaFloor{Hour: floor, Live: floor, Refresh: &shared}).Grade(first, at); v != status.BaselineOK {
		t.Errorf("the page grades the file the update keeps as %s, want ok", v)
	}
}

// A chain begun at the first snapshot lives, while the index is young, exactly
// as long as the retention's band allows, and no shorter: from its start until
// it is four fifths of the window old.
func TestCoverageRule_aYoungIndexEndsAChainOnTheRetentionsBand_3083(t *testing.T) {
	for _, window := range []time.Duration{48 * time.Hour, 24 * time.Hour, 12 * time.Hour, 6 * time.Hour} {
		for _, interval := range []time.Duration{5 * time.Minute, 30 * time.Minute} {
			floor := time.Date(2026, 11, 21, 9, 0, 0, 0, time.UTC)
			start := floor.Add(10 * time.Minute)
			rule := coverageRule{retain: window, interval: interval, dropsAfter: window}
			band := time.Duration(math.Ceil(status.BaselineAgingFraction * float64(window)))
			for at := start.Add(interval); rule.floorHolds(floor, at); at = at.Add(interval) {
				line, ok := rule.reanchorBy(floor, at)
				if !ok {
					t.Fatalf("window %s: no line with a known floor", window)
				}
				ended := !start.After(line)
				if want := at.Sub(start) >= band; ended != want {
					t.Fatalf("window %s, interval %s, index %s old, file %s old: ended = %v, want %v (line %s)",
						window, interval, at.Sub(floor), at.Sub(start), ended, want, line.Format(time.RFC3339))
				}
			}
		}
	}
}

// Where the line is new and where it is not. On an index as old as its window
// (rotation within an hour and an interval of the oldest hour), and on a rule
// that does not know the window, nothing changes. Where floorHolds, the line is
// the band over the shorter window, clamped at the oldest hour, and never later
// than it was.
func TestCoverageRule_theYoungLineAndNothingElseMoves_3083(t *testing.T) {
	now := time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)
	rules := []coverageRule{}
	for _, retain := range []time.Duration{0, 48 * time.Hour, 12 * time.Hour, 6 * time.Hour, 2 * time.Hour} {
		for _, dropsAfter := range []time.Duration{0, retain, 2 * retain, retain / 2, 24 * time.Hour} {
			for _, interval := range []time.Duration{0, 5 * time.Minute, 30 * time.Minute, 2 * time.Hour} {
				rules = append(rules, coverageRule{retain: retain, interval: interval, dropsAfter: dropsAfter})
			}
		}
	}
	for _, rule := range rules {
		for floor := now.Add(-60 * time.Hour); !floor.After(now.Add(2 * time.Hour)); floor = floor.Add(7 * time.Minute) {
			was, _ := lineBefore3083(rule, floor, now)
			is, ok := rule.reanchorBy(floor, now)
			if !ok {
				t.Fatalf("%+v: no line with a known floor", rule)
			}
			if !rule.floorHolds(floor, now) {
				if !is.Equal(was) {
					t.Fatalf("%+v, floor %s: rotation is within reach of the oldest hour (or not known) and the line moved (%s to %s)",
						rule, floor.Format(time.RFC3339), was, is)
				}
				continue
			}
			if is.After(was) {
				t.Fatalf("%+v, floor %s: the line moved LATER (%s to %s)", rule, floor.Format(time.RFC3339), was, is)
			}
			if !now.After(floor) {
				// A clock behind the index: as before, a start on the floor is
				// covered and one under it is not.
				if !is.Equal(floor.Add(-time.Nanosecond)) {
					t.Fatalf("%+v, floor %s in the future: line %s, want a nanosecond under the floor", rule, floor.Format(time.RFC3339), is)
				}
				continue
			}
			if want := youngLine(rule.dropsOn(), floor, now); !is.Equal(want) {
				t.Fatalf("%+v, floor %s: line %s, want %s, the band over the %s window the index drops on",
					rule, floor.Format(time.RFC3339), is.Format(time.RFC3339Nano), want.Format(time.RFC3339Nano), rule.dropsOn())
			}
		}
	}
}

// The edges, one by one.
func TestCoverageRule_youngIndexEdges_3083(t *testing.T) {
	now := time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)
	const w = 48 * time.Hour
	const interval = 5 * time.Minute
	rule := coverageRule{retain: w, interval: interval, dropsAfter: w}
	band := now.Add(-time.Duration(math.Ceil(status.BaselineAgingFraction * float64(w))))

	t.Run("younger than the window by exactly an hour and an interval: still young", func(t *testing.T) {
		floor := now.Add(-w).Add(cycleMargin(interval))
		if line, _ := rule.reanchorBy(floor, now); !line.Equal(band) {
			t.Errorf("line %s, want the retention's band %s", line.Format(time.RFC3339), band.Format(time.RFC3339))
		}
	})
	t.Run("a nanosecond older: the old rule", func(t *testing.T) {
		floor := now.Add(-w).Add(cycleMargin(interval)).Add(-time.Nanosecond)
		is, _ := rule.reanchorBy(floor, now)
		if was, _ := lineBefore3083(rule, floor, now); !is.Equal(was) {
			t.Errorf("line %s, want %s as before", is, was)
		}
	})
	for _, age := range []time.Duration{w, w + time.Hour, 10 * w} {
		t.Run("index "+age.String()+" old: the old rule", func(t *testing.T) {
			floor := now.Add(-age)
			is, _ := rule.reanchorBy(floor, now)
			if was, _ := lineBefore3083(rule, floor, now); !is.Equal(was) {
				t.Errorf("line %s, want %s as before", is, was)
			}
		})
	}
	t.Run("no retention and no window known: the old rule, on a young index too", func(t *testing.T) {
		for _, r := range []coverageRule{{interval: interval}, {retain: w, interval: interval}} {
			floor := now.Add(-3 * time.Hour)
			is, _ := r.reanchorBy(floor, now)
			if was, _ := lineBefore3083(r, floor, now); !is.Equal(was) {
				t.Errorf("%+v: line %s, want %s as before: nothing says when rotation reaches the oldest hour", r, is, was)
			}
		}
	})
	t.Run("only the index's own window known", func(t *testing.T) {
		r := coverageRule{interval: interval, dropsAfter: 12 * time.Hour}
		floor := now.Add(-10 * time.Hour)
		want := now.Add(-time.Duration(math.Ceil(status.BaselineAgingFraction * float64(12*time.Hour))))
		if line, _ := r.reanchorBy(floor, now); !line.Equal(want) {
			t.Errorf("line %s, want %s, the band over the index's 12h", line.Format(time.RFC3339), want.Format(time.RFC3339))
		}
	})
	t.Run("the index drops on a shorter window than the one configured", func(t *testing.T) {
		r := coverageRule{retain: 48 * time.Hour, interval: interval, dropsAfter: 6 * time.Hour}
		floor := now.Add(-3 * time.Hour)
		start := now.Add(-5 * time.Hour) // older than the floor: rewritten anyway
		if line, _ := r.reanchorBy(floor, now); start.After(line) {
			t.Errorf("a file older than the oldest hour kept (line %s)", line)
		}
		start = floor.Add(time.Minute) // 2h59m old: under 0.8 of 6h
		if line, _ := r.reanchorBy(floor, now); !start.After(line) {
			t.Errorf("a file inside the 6h band rewritten (line %s)", line)
		}
		floor = now.Add(-5*time.Hour + time.Minute)
		r.interval = 0
		start = floor.Add(time.Minute) // 4h58m old: past 0.8 of 6h (4h48m), well under 0.8 of 48h
		if !r.floorHolds(floor, now) {
			t.Fatal("the case does not sit where the floor holds")
		}
		if line, _ := r.reanchorBy(floor, now); start.After(line) {
			t.Errorf("a file past the band of the 6h the index drops on was kept by the 48h configured (line %s)", line.Format(time.RFC3339))
		}
	})
	t.Run("no oldest hour yet", func(t *testing.T) {
		if _, ok := rule.reanchorBy(time.Time{}, now); ok {
			t.Error("a line was drawn with no floor")
		}
	})
	t.Run("the oldest hour in the future", func(t *testing.T) {
		floor := now.Add(time.Hour)
		line, ok := rule.reanchorBy(floor, now)
		if !ok || !line.Equal(floor.Add(-time.Nanosecond)) {
			t.Errorf("line %s, want a nanosecond under the floor %s", line, floor)
		}
	})
	t.Run("a file older than what the index still holds is rewritten", func(t *testing.T) {
		floor := now.Add(-2 * time.Hour)
		for _, start := range []time.Time{floor.Add(-time.Hour), floor.Add(-time.Nanosecond), floor} {
			if line, _ := rule.reanchorBy(floor, now); start.After(line) {
				t.Errorf("start %s, floor %s: kept on the line %s", start.Format(time.RFC3339Nano), floor.Format(time.RFC3339), line.Format(time.RFC3339Nano))
			}
			if snapshotCoveredBy(floor, start, now, rule) {
				t.Errorf("start %s: the gate calls it covered", start.Format(time.RFC3339Nano))
			}
		}
		if start := floor.Add(time.Nanosecond); !snapshotCoveredBy(floor, start, now, rule) {
			t.Error("a start just over the oldest hour is not covered on a two-hour-old index")
		}
	})
}

// From birth through rotation: no reader's start ever falls under the floor,
// no rewrite is forced while the index is young except by the retention's own
// band, and no two cycles in a row rewrite. Rotation is simulated as
// rotation.Perform drops: a partition goes as soon as its start is older than
// the window, at every instant.
func TestCoverageRule_fromBirthThroughRotation_3083(t *testing.T) {
	const horizon = 6 * 24 * time.Hour
	for _, window := range []time.Duration{48 * time.Hour, 24 * time.Hour, 12 * time.Hour, 6 * time.Hour, 4 * time.Hour} {
		for _, interval := range []time.Duration{5 * time.Minute, 30 * time.Minute, time.Hour} {
			for _, busy := range []bool{true, false} {
				born := time.Date(2026, 11, 21, 9, 28, 54, 0, time.UTC)
				floorAt := func(at time.Time) time.Time {
					floor := born.Truncate(time.Hour)
					for floor.Before(at.Add(-window)) {
						floor = floor.Add(time.Hour)
					}
					return floor
				}
				rule := coverageRule{retain: window, interval: interval, dropsAfter: window}
				band := time.Duration(math.Ceil(status.BaselineAgingFraction * float64(window)))
				start := born.Add(10 * time.Minute)
				rewrotePrev := false
				name := window.String() + " every " + interval.String()
				if busy {
					name += " busy"
				}
				for at := start.Add(interval); !at.After(born.Add(horizon)); at = at.Add(interval) {
					floor := floorAt(at)
					if start.Before(floor) {
						t.Fatalf("%s at %s: a reader's start %s is under the floor %s", name, at, start, floor)
					}
					line, _ := rule.reanchorBy(floor, at)
					// The page's verdict on the same file, on the same floor:
					// aging exactly when this cycle rewrites it (#3083).
					shared := rule.shared()
					switch v := (status.DeltaFloor{Hour: floor, Live: floor, Refresh: &shared}).Grade(start, at); {
					case v == status.BaselineAging && start.After(line):
						t.Fatalf("%s at %s: a file %s old the update keeps shows as aging", name, at, at.Sub(start))
					case v != status.BaselineAging && !start.After(line):
						t.Fatalf("%s at %s: a file %s old the update rewrites shows as %s", name, at, at.Sub(start), v)
					case v == status.BaselineAging && rule.floorHolds(floor, at) && at.Sub(start) < band:
						t.Fatalf("%s at %s: a file %s old shows as aging on a young index", name, at, at.Sub(start))
					}
					if !busy && start.After(line) {
						rewrotePrev = false
						continue
					}
					rewrote := false
					switch {
					case at.Sub(start) > 24*time.Hour:
						rewrote = true
					case !start.After(line):
						if rule.floorHolds(floor, at) && at.Sub(start) < band {
							t.Fatalf("%s at %s (index %s old): a file %s old rewritten on a young index, under the %s band",
								name, at, at.Sub(born), at.Sub(start), band)
						}
						rewrote = true
					}
					if rewrote {
						if rewrotePrev {
							t.Fatalf("%s at %s: two cycles in a row rewrote the table", name, at)
						}
						start = at
					}
					rewrotePrev = rewrote
				}
			}
		}
	}
}
