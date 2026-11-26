package status

import (
	"math"
	"time"
)

// RefreshRule is what, besides the live partitions, decides how long a
// snapshot may go without being replaced (#1904): the retention the operator
// has configured (zero: rotation off or unreadable), the time between two
// snapshot updates, and how old THIS index's oldest partition has to be
// before rotation drops it (#2121, rotation.DropWindow; zero when not known).
// DropsAfter is not Retain under another name: with no retention chosen each
// index drops on the window it was created under, which the daemon-wide
// setting does not say.
//
// It lives here, and not beside the snapshot update that draws it, so the
// staleness verdict can grade on the very same line (#3083, DeltaFloor.Refresh):
// a file the update keeps never shows as aging, and a file the update is going
// to rewrite does.
type RefreshRule struct {
	Retain     time.Duration
	Interval   time.Duration
	DropsAfter time.Duration
}

// Line is THE line, for the refresh gate, the fold and the staleness verdict
// alike (#1904, #3083): a snapshot whose readers start at or before it is no
// longer safely covered, a chain of table deltas whose start is at or before
// it is ended (the table is written in full), and a file whose start is at or
// before it grades aging. ok is false when there is no floor to draw it from.
//
// One function on purpose. The gate opens when a start reaches the line, and
// the fold it releases is what has to move that start; if the two drew their
// lines apart, a gate that opened would release a fold that appended a pair,
// kept the start, and opened the gate again on every cycle, repairing nothing.
// The verdict is drawn on it for the same reason: on any other line it either
// warns about files the update keeps, or stays quiet about files it rewrites.
//
// Three bounds, and the line is the latest of them:
//
//   - the product's aging band over the LIVE floor, exactly the verdict
//     BaselineStalenessFor gives over it. Not while rotation is known to be
//     out of reach of the floor (FloorHolds): on an index younger than its
//     retention the span is the install's age, and this band rewrote every
//     table in the first hours of every install (#3083). There the band is
//     drawn over the window the index drops on (DropsOn, which is FloorHolds'
//     own window whenever it holds), never under the oldest hour. That is safe
//     because the oldest hour is still there at the next cycle, and that cycle
//     draws the line again;
//   - the same band over the retention the operator CONFIGURED. The observed
//     floor is an observation of that policy and the policy can change under
//     it in a step: the rotation panel saves a new retain and the next
//     rotation drops every newly expired partition in one statement, 30 days
//     to 7 skipping the aging verdict entirely. Asked against the policy, a
//     cycle between the save and the drop already sees the new line;
//   - one hour plus one interval above the floor. The floor moves in whole
//     hours, since partitions are hourly, and nothing looks again for an
//     interval, so a start closer than that can be past the floor at the next
//     cycle. On a long retention the band is wider and this never binds. It
//     assumes the next cycle does come one interval later: a cycle skipped
//     because another job holds the server, or a daemon that is down, stretches
//     the gap past what the margin covers. Not drawn while FloorHolds: on an
//     index younger than its retention this bound ended every chain of the
//     first hour, and each of those updates wrote every table in full (#2121).
//
// A busy table does not rewrite on every cycle: a rewrite starts a new chain
// at the run, and the next cycle only ends it again when the retention is
// shorter than two intervals plus an hour, where no chain could live anyway.
func (r RefreshRule) Line(liveFloor, now time.Time) (time.Time, bool) {
	if liveFloor.IsZero() {
		return time.Time{}, false
	}
	holds := r.FloorHolds(liveFloor, now)
	// BaselineStalenessFor: aging once now-start >= 0.8 of now-floor, broken
	// before the floor, and never aging over a floor not behind now.
	line := liveFloor.Add(-time.Nanosecond)
	if span := now.Sub(liveFloor); span > 0 {
		if holds {
			line = now.Add(-agingBand(r.DropsOn()))
			if liveFloor.After(line) {
				line = liveFloor
			}
		} else {
			line = now.Add(-agingBand(span))
		}
	}
	floor := liveFloor
	if r.Retain > 0 {
		if p := now.Add(-time.Duration(float64(r.Retain) * BaselineAgingFraction)); p.After(line) {
			line = p
		}
		if p := now.Add(-r.Retain); p.After(floor) {
			floor = p
		}
	}
	if m := floor.Add(CycleMargin(r.Interval)); m.After(line) && !holds {
		line = m
	}
	return line, true
}

// agingBand is the aging band over a span, rounded up so a start exactly on
// the band's edge falls on the line.
func agingBand(span time.Duration) time.Duration {
	return time.Duration(math.Ceil(BaselineAgingFraction * float64(span)))
}

// CycleMargin is how far above the index's floor a start has to be for the
// next cycle to still find it there: the hour the floor moves by and the
// interval nothing looks again for (Line's third bound, and FloorHolds).
func CycleMargin(interval time.Duration) time.Duration { return time.Hour + interval }

// DropsOn is the window this index's events are dropped on as far as the rule
// knows: the configured retention, or the index's own window when that is
// shorter (no retention chosen, and an index created under a shorter one).
// Zero when neither is known.
func (r RefreshRule) DropsOn() time.Duration {
	w := r.Retain
	if r.DropsAfter > 0 && (w <= 0 || r.DropsAfter < w) {
		w = r.DropsAfter
	}
	return max(w, 0)
}

// FloorHolds reports whether rotation is known to leave the oldest live hour
// alone until after the next cycle has looked (#2121): that hour is dropped
// once it is older than the index's window, so while that hour is at least an
// hour and an interval inside the window, the floor does not move and the
// third bound has nothing to guard. The same hour and interval the bound
// itself keeps, so a cycle that runs late is covered as far here as it is
// there.
//
// The shorter of the two retentions the rule holds: with no retention chosen,
// Retain is the daemon default and DropsAfter the window this index was
// created under, and the two are read by separate calls. With DropsAfter not
// known it never holds: the configured retention alone does not say what
// this index drops on.
func (r RefreshRule) FloorHolds(liveFloor, now time.Time) bool {
	window := r.DropsAfter
	if r.Retain > 0 && r.Retain < window {
		window = r.Retain
	}
	if window <= 0 {
		return false
	}
	return !liveFloor.Before(now.Add(-window).Add(CycleMargin(r.Interval)))
}
