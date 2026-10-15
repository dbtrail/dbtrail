package rotation

import (
	"context"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"
)

// attemptsOn counts the rotation attempts the loop made on index database db:
// each target here is unreachable, so every attempt logs one "cannot connect".
func (c *logCapture) attemptsOn(db string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, r := range c.records {
		if r.Message != "built-in rotation: cannot connect to index database" {
			continue
		}
		r.Attrs(func(a slog.Attr) bool {
			if a.Key == "db" && a.Value.String() == db {
				n++
				return false
			}
			return true
		})
	}
	return n
}

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
	logs := captureSlog(t)
	var calls atomic.Int32
	startHourlyLoop(t, func() []RotateTarget {
		if calls.Add(1) < 3 {
			return []RotateTarget{{DSN: "root:x@tcp(127.0.0.1:1)/boot"}}
		}
		return []RotateTarget{{DSN: "root:x@tcp(127.0.0.1:1)/boot"}, {DSN: "root:x@tcp(127.0.0.1:1)/late"}}
	})
	waitFor(t, "the index that appeared after the first cycle was not rotated before the tick", func() bool { return logs.attemptsOn("late") == 1 })
	time.Sleep(100 * time.Millisecond)
	if n := logs.attemptsOn("boot"); n != 1 {
		t.Errorf("the index of the first cycle was rotated %d times before the tick, want 1", n)
	}
	if n := logs.attemptsOn("late"); n != 1 {
		t.Errorf("the new index was rotated %d times before the tick, want 1 (a failed one waits for the tick)", n)
	}
}

// TestStartLoop_newTargetPollIgnoresEmptyAndDuplicateDSNs: the poll dedupes
// like a full cycle does.
func TestStartLoop_newTargetPollIgnoresEmptyAndDuplicateDSNs(t *testing.T) {
	logs := captureSlog(t)
	var calls atomic.Int32
	startHourlyLoop(t, func() []RotateTarget {
		if calls.Add(1) < 3 {
			return nil
		}
		return []RotateTarget{{DSN: ""}, {DSN: "root:x@tcp(127.0.0.1:1)/dup"}, {DSN: "root:x@tcp(127.0.0.1:1)/dup"}}
	})
	waitFor(t, "the new index was never rotated", func() bool { return logs.attemptsOn("dup") >= 1 })
	time.Sleep(100 * time.Millisecond)
	if n := logs.attemptsOn("dup"); n != 1 {
		t.Errorf("a DSN listed twice was rotated %d times, want 1", n)
	}
}
