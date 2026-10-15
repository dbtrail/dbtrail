package rotation

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// startHourlyLoop runs StartLoop with an hour-long interval, so any attempt
// the test sees inside a few hundred milliseconds came from the first cycle
// or from the new-target poll, never from a tick.
func startHourlyLoop(t *testing.T, targets func() []RotateTarget) {
	t.Helper()
	prev := newTargetPoll
	newTargetPoll = 5 * time.Millisecond
	t.Cleanup(func() { newTargetPoll = prev })
	s := Settings{Enabled: true, Retain: 24 * time.Hour, RetainRaw: "24h", Interval: time.Hour, AddFuture: 1, Explicit: true}
	ctx, cancel := context.WithCancel(context.Background())
	done := StartLoop(ctx, func() Settings { return s }, targets)
	t.Cleanup(func() { cancel(); <-done })
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal(what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// TestStartLoop_rotatesANewTargetWithoutWaitingForTheTick: an index the
// provider returns only after the first cycle (a console server that came up
// after startup, or one added later) is rotated within a poll, not an
// interval later (#2412).
func TestStartLoop_rotatesANewTargetWithoutWaitingForTheTick(t *testing.T) {
	calls := fakeRotate(t, nil)
	var n atomic.Int32
	startHourlyLoop(t, func() []RotateTarget {
		if n.Add(1) < 3 {
			return []RotateTarget{{DSN: "boot"}}
		}
		return []RotateTarget{{DSN: "boot"}, {DSN: "late"}}
	})
	waitFor(t, "the index that appeared after the first cycle was not rotated before the tick", func() bool { return calls("late") == 1 })
	time.Sleep(100 * time.Millisecond)
	if c := calls("boot"); c != 1 {
		t.Errorf("the index of the first cycle was rotated %d times before the tick, want 1", c)
	}
	if c := calls("late"); c != 1 {
		t.Errorf("the new index was rotated %d times before the tick, want 1", c)
	}
}

// TestStartLoop_aDSNListedTwiceIsRotatedOnce: a new DSN the provider lists
// twice, beside an empty one, is rotated once.
func TestStartLoop_aDSNListedTwiceIsRotatedOnce(t *testing.T) {
	calls := fakeRotate(t, nil)
	var n atomic.Int32
	startHourlyLoop(t, func() []RotateTarget {
		if n.Add(1) < 3 {
			return nil
		}
		return []RotateTarget{{DSN: ""}, {DSN: "dup"}, {DSN: "dup"}}
	})
	waitFor(t, "the new index was never rotated", func() bool { return calls("dup") >= 1 })
	time.Sleep(100 * time.Millisecond)
	if c := calls("dup"); c != 1 {
		t.Errorf("a DSN listed twice was rotated %d times, want 1", c)
	}
	if c := calls(""); c != 0 {
		t.Errorf("an empty DSN was rotated %d times", c)
	}
}

// fakeRotate swaps rotateIndex for a counter that fails each DSN's first
// failFirst[dsn] attempts.
func fakeRotate(t *testing.T, failFirst map[string]int) func(string) int {
	t.Helper()
	var mu sync.Mutex
	calls := map[string]int{}
	prev := rotateIndex
	rotateIndex = func(_ context.Context, rt RotateTarget, _ Settings) (int, error) {
		mu.Lock()
		defer mu.Unlock()
		calls[rt.DSN]++
		if calls[rt.DSN] <= failFirst[rt.DSN] {
			return 0, errors.New("Unknown database")
		}
		return 0, nil
	}
	t.Cleanup(func() { rotateIndex = prev })
	return func(dsn string) int { mu.Lock(); defer mu.Unlock(); return calls[dsn] }
}

// TestStartLoop_aNewTargetThatFailsIsTriedAgain: a server added while its
// database is still being created fails its first try; the poll tries it
// again instead of leaving it to the next full cycle (#2412).
func TestStartLoop_aNewTargetThatFailsIsTriedAgain(t *testing.T) {
	calls := fakeRotate(t, map[string]int{"late": 1})
	var n atomic.Int32
	startHourlyLoop(t, func() []RotateTarget {
		if n.Add(1) < 3 {
			return nil
		}
		return []RotateTarget{{DSN: "late"}}
	})
	waitFor(t, "the new index that failed once was not tried again", func() bool { return calls("late") == 2 })
	time.Sleep(100 * time.Millisecond)
	if c := calls("late"); c != 2 {
		t.Errorf("a new index that succeeded on its second try was rotated %d times before the tick, want 2", c)
	}
}

// TestStartLoop_aNewTargetThatKeepsFailingWaitsForTheTick: the poll gives up
// after newTargetTries; the full cycle retries and reports it.
func TestStartLoop_aNewTargetThatKeepsFailingWaitsForTheTick(t *testing.T) {
	calls := fakeRotate(t, map[string]int{"late": 1000})
	var n atomic.Int32
	startHourlyLoop(t, func() []RotateTarget {
		if n.Add(1) < 3 {
			return nil
		}
		return []RotateTarget{{DSN: "late"}}
	})
	waitFor(t, "the new index was not tried", func() bool { return calls("late") >= newTargetTries })
	time.Sleep(100 * time.Millisecond)
	if c := calls("late"); c != newTargetTries {
		t.Errorf("a new index that keeps failing was tried %d times before the tick, want %d", c, newTargetTries)
	}
}
