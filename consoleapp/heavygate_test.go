package consoleapp

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// acquired runs acquire on a goroutine and reports when it got in.
func acquireAsync(t *testing.T, g *heavyGate, ctx context.Context, lane heavyLane, waited *atomic.Bool) (got <-chan func(), errc <-chan error) {
	t.Helper()
	gc, ec := make(chan func(), 1), make(chan error, 1)
	go func() {
		rel, err := g.acquire(ctx, lane, func() {
			if waited != nil {
				waited.Store(true)
			}
		})
		if err != nil {
			ec <- err
			return
		}
		gc <- rel
	}()
	return gc, ec
}

func mustAcquire(t *testing.T, g *heavyGate, lane heavyLane) func() {
	t.Helper()
	rel, err := g.acquire(context.Background(), lane, func() { t.Fatalf("lane %d had to wait", lane) })
	if err != nil {
		t.Fatal(err)
	}
	return rel
}

func notYet(t *testing.T, got <-chan func(), what string) {
	t.Helper()
	select {
	case <-got:
		t.Fatalf("%s got in; it should wait", what)
	case <-time.After(50 * time.Millisecond):
	}
}

func within(t *testing.T, got <-chan func(), what string) func() {
	t.Helper()
	select {
	case rel := <-got:
		return rel
	case <-time.After(2 * time.Second):
		t.Fatalf("%s never got in", what)
		return nil
	}
}

// TestHeavyGate_snapshotNeverWaitsForCheck: a refresh starts at once while a
// check table or an Explain runs; the lanes are independent that way.
func TestHeavyGate_snapshotNeverWaitsForCheck(t *testing.T) {
	g := newHeavyGate()
	relCheck := mustAcquire(t, g, laneCheck)
	defer relCheck()
	mustAcquire(t, g, laneSnapshot)()
}

// TestHeavyGate_checkWaitsForSnapshot: a check table does not start while
// snapshot work runs, says it is waiting, and starts when it ends.
func TestHeavyGate_checkWaitsForSnapshot(t *testing.T) {
	g := newHeavyGate()
	relSnap := mustAcquire(t, g, laneSnapshot)
	var waited atomic.Bool
	got, _ := acquireAsync(t, g, context.Background(), laneCheck, &waited)
	notYet(t, got, "a check table while a refresh runs")
	if !waited.Load() {
		t.Error("onWait was not called for a check that has to wait")
	}
	relSnap()
	within(t, got, "the check after the refresh ended")()
}

// TestHeavyGate_waitingSnapshotGoesFirst: with a refresh running and a second
// one waiting, a check that arrives does not slip in between them.
func TestHeavyGate_waitingSnapshotGoesFirst(t *testing.T) {
	g := newHeavyGate()
	relA := mustAcquire(t, g, laneSnapshot)
	gotB, _ := acquireAsync(t, g, context.Background(), laneSnapshot, nil)
	notYet(t, gotB, "a second refresh")
	gotC, _ := acquireAsync(t, g, context.Background(), laneCheck, nil)
	relA()
	relB := within(t, gotB, "the waiting refresh")
	notYet(t, gotC, "a check while the second refresh runs")
	relB()
	within(t, gotC, "the check after both refreshes")()
}

// TestHeavyGate_oneCheckAtATime: two check tables (two servers' checks, or a
// check and an Explain) do not run together.
func TestHeavyGate_oneCheckAtATime(t *testing.T) {
	g := newHeavyGate()
	rel := mustAcquire(t, g, laneCheck)
	got, _ := acquireAsync(t, g, context.Background(), laneCheck, nil)
	notYet(t, got, "a second check table")
	rel()
	within(t, got, "the second check table")()
}

// TestHeavyGate_cancelWhileWaiting: a canceled wait returns the context's
// error and holds nothing; a canceled snapshot waiter stops holding checks
// back.
func TestHeavyGate_cancelWhileWaiting(t *testing.T) {
	g := newHeavyGate()
	relA := mustAcquire(t, g, laneSnapshot)
	defer relA()
	ctx, cancel := context.WithCancel(context.Background())
	_, errc := acquireAsync(t, g, ctx, laneSnapshot, nil)
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case err := <-errc:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("a canceled wait did not return")
	}
	g.mu.Lock()
	w := g.snapWaiting
	g.mu.Unlock()
	if w != 0 {
		t.Fatalf("snapWaiting = %d after the waiter left, want 0", w)
	}
	relA()
	mustAcquire(t, g, laneCheck)()
}

// TestHeavyGate_releaseTwice: a second release does not free a lane someone
// else now holds.
func TestHeavyGate_releaseTwice(t *testing.T) {
	g := newHeavyGate()
	rel := mustAcquire(t, g, laneSnapshot)
	rel()
	rel2 := mustAcquire(t, g, laneSnapshot)
	defer rel2()
	rel()
	got, _ := acquireAsync(t, g, context.Background(), laneSnapshot, nil)
	notYet(t, got, "a refresh after a stale release")
}
