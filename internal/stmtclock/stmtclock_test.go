package stmtclock

import (
	"context"
	"errors"
	"testing"
	"time"
)

func ended(ctx context.Context, within time.Duration) bool {
	select {
	case <-ctx.Done():
		return true
	case <-time.After(within):
		return false
	}
}

// Not held, it is context.WithTimeout: it ends with DeadlineExceeded after
// its time, and reports a deadline.
func TestWithTimeout_runsOutLikeAContextTimeout(t *testing.T) {
	var g Group
	ctx, cancel := g.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()
	if dl, ok := ctx.Deadline(); !ok || time.Until(dl) > 60*time.Millisecond || time.Until(dl) < 0 {
		t.Errorf("Deadline() = %v, %v; want a time within the next 60ms", dl, ok)
	}
	if ctx.Err() != nil {
		t.Fatalf("ended before its time: %v", ctx.Err())
	}
	if !ended(ctx, 2*time.Second) {
		t.Fatal("never ran out")
	}
	if !errors.Is(ctx.Err(), context.DeadlineExceeded) {
		t.Errorf("Err() = %v, want DeadlineExceeded", ctx.Err())
	}
}

// Held, the clock does not run: a wait several times the cap does not end
// the statement, and released it has what it had left, not a fresh cap.
func TestHold_theWaitIsNotCounted(t *testing.T) {
	var g Group
	ctx, cancel := g.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	time.Sleep(50 * time.Millisecond) // 100ms left
	release := Hold(ctx)
	if _, ok := ctx.Deadline(); ok {
		t.Error("a held clock reports a deadline")
	}
	if ended(ctx, 400*time.Millisecond) {
		t.Fatalf("ended while held: %v", ctx.Err())
	}
	release()
	start := time.Now()
	if !ended(ctx, 2*time.Second) {
		t.Fatal("never ran out after the hold was released")
	}
	if took := time.Since(start); took > 140*time.Millisecond {
		t.Errorf("ran %v more after the hold, want what was left (about 100ms), not a fresh 150ms", took)
	}
	if !errors.Is(ctx.Err(), context.DeadlineExceeded) {
		t.Errorf("Err() = %v, want DeadlineExceeded", ctx.Err())
	}
}

// The line is given a context DERIVED from the statement's, and a routed
// statement has two clocks: holding through the child of one holds both.
func TestHold_holdsEveryClockOfTheConnectionThroughADerivedContext(t *testing.T) {
	var g Group
	outer, cancelOuter := g.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancelOuter()
	inner, cancelInner := g.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancelInner()
	child, cancelChild := context.WithCancel(inner)
	defer cancelChild()
	release := Hold(child)
	if ended(outer, 300*time.Millisecond) || inner.Err() != nil || child.Err() != nil {
		t.Fatalf("a clock ran while held: outer %v, inner %v, child %v", outer.Err(), inner.Err(), child.Err())
	}
	release()
	if !ended(outer, 2*time.Second) || !ended(inner, 2*time.Second) || !ended(child, 2*time.Second) {
		t.Fatal("a clock never ran out after the hold was released")
	}
	// Another connection's clock is not this group's to hold.
	var other Group
	theirs, cancelTheirs := other.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancelTheirs()
	defer Hold(child)()
	if !ended(theirs, 2*time.Second) {
		t.Error("holding one connection's clocks held another's")
	}
}

// Holds nest, and a release called twice counts once.
func TestHold_nestsAndReleasesOnce(t *testing.T) {
	var g Group
	ctx, cancel := g.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()
	first, second := Hold(ctx), Hold(ctx)
	first()
	first()
	if ended(ctx, 250*time.Millisecond) {
		t.Fatal("ran with one hold still in place")
	}
	second()
	if !ended(ctx, 2*time.Second) {
		t.Fatal("never ran out after the last hold was released")
	}
}

// A clock made while the connection's clocks are held starts held.
func TestWithTimeout_startsHeldUnderAHold(t *testing.T) {
	var g Group
	first, cancelFirst := g.WithTimeout(context.Background(), time.Hour)
	defer cancelFirst()
	release := Hold(first)
	late, cancelLate := g.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancelLate()
	if ended(late, 250*time.Millisecond) {
		t.Fatal("a clock made under a hold ran")
	}
	release()
	if !ended(late, 2*time.Second) {
		t.Fatal("never ran out after the hold was released")
	}
}

// What ends a held statement is its client leaving: the parent's cancel
// reaches it held or not, with the parent's error, and so does its own
// cancel function. Neither is a deadline.
func TestClock_endsWithItsParentAndItsCancelEvenWhileHeld(t *testing.T) {
	var g Group
	parent, leave := context.WithCancel(context.Background())
	ctx, cancel := g.WithTimeout(parent, time.Hour)
	defer cancel()
	defer Hold(ctx)()
	leave()
	if ctx.Err() == nil {
		t.Fatal("Err() is nil right after the parent ended")
	}
	if !ended(ctx, 2*time.Second) {
		t.Fatal("the parent ending did not end a held clock")
	}
	if !errors.Is(ctx.Err(), context.Canceled) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
		t.Errorf("Err() = %v, want the parent's Canceled", ctx.Err())
	}

	own, cancelOwn := g.WithTimeout(context.Background(), time.Hour)
	cancelOwn()
	cancelOwn()
	if !ended(own, 2*time.Second) || !errors.Is(own.Err(), context.Canceled) {
		t.Errorf("after its cancel: Err() = %v, want Canceled", own.Err())
	}
	g.mu.Lock()
	left := len(g.clocks)
	g.mu.Unlock()
	if left != 0 {
		t.Errorf("%d ended clocks still in the group", left)
	}

	gone, already := context.WithCancel(context.Background())
	already()
	dead, cancelDead := g.WithTimeout(gone, time.Hour)
	defer cancelDead()
	select {
	case <-dead.Done():
	default:
		t.Error("a clock under a parent that had already ended was handed out not ended")
	}
}

// A context with no such clock is left alone, and values pass through.
func TestHold_aPlainContextIsLeftAlone(t *testing.T) {
	Hold(context.Background())()
	type k struct{}
	var g Group
	ctx, cancel := g.WithTimeout(context.WithValue(context.Background(), k{}, "v"), time.Hour)
	defer cancel()
	if ctx.Value(k{}) != "v" {
		t.Error("the parent's values do not pass through")
	}
}
