//go:build unix

package sqlsandbox

import (
	"context"
	"fmt"
	"sync"
	"testing"
)

// #2546 with standby workers: at most one per slot, the count it is read
// against being the current one.

// Lowered, the standbys past the new count are stopped, not left waiting
// for slots that are no longer there.
func TestSetMaxInFlight_lowerStopsTheStandbysPastIt_2546(t *testing.T) {
	f := newCopyFixture(t)
	r := newStandbyRunner(t, Config{MaxInFlight: 3})
	mustRun(t, r, f.job("SELECT 1"))
	before := waitStandbys(t, r, 3)
	if err := r.SetMaxInFlight(1); err != nil {
		t.Fatal(err)
	}
	after := waitStandbys(t, r, 1)
	kept := false
	for _, p := range before {
		if p == after[0] {
			kept = true
		}
	}
	if !kept {
		t.Fatalf("the standby left (%d) is not one of those that waited (%v): lowering started a new one", after[0], before)
	}
	// The next statements run, and never bring more than one back.
	for range 3 {
		mustRun(t, r, f.job("SELECT 1"))
		if n := len(standbyPids(r)); n > 1 {
			t.Fatalf("%d standbys with 1 slot", n)
		}
	}
	waitStandbys(t, r, 1)
}

// Raised, the standbys are topped up to the new count without waiting for a
// statement.
func TestSetMaxInFlight_raiseTopsUpTheStandbys_2546(t *testing.T) {
	f := newCopyFixture(t)
	r := newStandbyRunner(t, Config{MaxInFlight: 1})
	mustRun(t, r, f.job("SELECT 1"))
	waitStandbys(t, r, 1)
	if err := r.SetMaxInFlight(3); err != nil {
		t.Fatal(err)
	}
	waitStandbys(t, r, 3)
}

// Raised before any statement ran, nothing is started: an install nobody
// runs SQL on never starts a worker.
func TestSetMaxInFlight_raiseBeforeAnyStatementStartsNothing_2546(t *testing.T) {
	r := newStandbyRunner(t, Config{MaxInFlight: 1})
	if err := r.SetMaxInFlight(3); err != nil {
		t.Fatal(err)
	}
	r.topUps.Wait()
	if n := r.started.Load(); n != 0 {
		t.Fatalf("%d workers started with no statement ever run", n)
	}
}

// Kept workers (workers that answer many statements) that come back from
// their statements after the count was lowered past them: one is kept for the
// one slot, the other is stopped, and none stays counted as out.
func TestSetMaxInFlight_workersBackPastTheCountAreNotKept_2546(t *testing.T) {
	f := newCopyFixture(t)
	r := newReuseRunner(t, Config{MaxInFlight: 2, MaxWait: -1})
	mustRun(t, r, f.job("SELECT 1"))
	waitStandbys(t, r, 2)
	// mustRun's pid hook would be written by both statements at once.
	r.onStart = nil

	// Both statements have answered and wait to be kept; the count drops to
	// 1 before either is.
	var arrived sync.WaitGroup
	arrived.Add(2)
	release := make(chan struct{})
	r.beforeKeep = func(*worker) {
		arrived.Done()
		<-release
	}
	done := make(chan error, 2)
	for i := range 2 {
		go func() {
			_, err := r.Run(context.Background(), f.job(fmt.Sprintf("SELECT %d", i)))
			done <- err
		}()
	}
	arrived.Wait()
	if err := r.SetMaxInFlight(1); err != nil {
		t.Fatal(err)
	}
	close(release)
	for range 2 {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	waitStandbys(t, r, 1)
	r.mu.Lock()
	lent := r.lent
	r.mu.Unlock()
	if lent != 0 {
		t.Fatalf("%d workers counted as out with a statement after both ended", lent)
	}
}
