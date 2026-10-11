package sqlsandbox

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// waitUntilWaiting blocks until n callers are parked in Reserve.
func waitUntilWaiting(t *testing.T, r *Runner, n int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for r.waitingNow() != n {
		if time.Now().After(deadline) {
			t.Fatalf("%d callers waiting, want %d", r.waitingNow(), n)
		}
		time.Sleep(time.Millisecond)
	}
}

// #2327: a freed slot goes to the caller that has waited longest. With every
// waiter woken to race for it, a long line served them in no order, and some
// were refused at the end of their wait while later arrivals were served.
func TestReserve_servesWaitersInTheOrderTheyArrived_2327(t *testing.T) {
	r := waitRunner(1, 10*time.Second, 64)
	held, err := r.Reserve(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	const n = 20
	var mu sync.Mutex
	var served []int
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s, err := r.Reserve(context.Background(), "")
			if err != nil {
				t.Errorf("waiter %d: %v", i, err)
				return
			}
			mu.Lock()
			served = append(served, i)
			mu.Unlock()
			s.Release()
		}()
		// Each is in line before the next arrives: the order of arrival is
		// the order of the loop.
		waitUntilWaiting(t, r, i+1)
	}
	held.Release()
	wg.Wait()
	for i, got := range served {
		if got != i {
			t.Fatalf("served in the order %v, want the order of arrival", served)
		}
	}
	if len(served) != n || r.waitingNow() != 0 || r.Running() != 0 {
		t.Errorf("served %d of %d; %d still waiting, %d running", len(served), n, r.waitingNow(), r.Running())
	}
}

// A long line under a short wait: the callers at the head are served, in
// their turn, and the wait runs out only for those it cannot reach. With
// every waiter woken to race for each freed slot, the ones at the head could
// lose every race and be refused though they had waited longest.
func TestReserve_theHeadOfALongLineIsServed_2327(t *testing.T) {
	r := waitRunner(1, 2*time.Second, 1000)
	held, err := r.Reserve(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	const n = 40
	refused := make([]bool, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s, err := r.Reserve(context.Background(), "")
			if err != nil {
				var busy *BusyError
				if !errors.As(err, &busy) {
					t.Errorf("waiter %d: %v, want the wait running out", i, err)
				}
				refused[i] = true
				return
			}
			// Each holds its slot long enough that the wait cannot reach
			// the whole line.
			time.Sleep(100 * time.Millisecond)
			s.Release()
		}()
		waitUntilWaiting(t, r, i+1)
	}
	held.Release()
	wg.Wait()
	// The wait is twenty times what one caller holds its slot for, and the
	// line is forty long: the first five are served with room to spare,
	// whatever the scheduler does, and the last cannot be reached.
	for i := range 5 {
		if refused[i] {
			t.Fatalf("waiter %d, at the head of the line, was refused: %v", i, refused)
		}
	}
	if !refused[n-1] {
		t.Errorf("the last of %d was served: the line was not long enough for the wait to run out, and this test shows nothing", n)
	}
}

// A waiter whose own slot is busy is passed over and keeps its place: the
// caller behind it takes the freed slot, and when its own slot comes free it
// is served before a later arrival of the same person.
func TestReserve_aPassedOverWaiterKeepsItsPlace_2327(t *testing.T) {
	r := waitRunner(2, 10*time.Second, 16)
	a1, err := r.Reserve(context.Background(), "a")
	if err != nil {
		t.Fatal(err)
	}
	b1, err := r.Reserve(context.Background(), "b")
	if err != nil {
		t.Fatal(err)
	}
	order := make(chan string, 4)
	slots := map[string]*Slot{}
	var mu sync.Mutex
	take := func(name, user string) {
		go func() {
			s, err := r.Reserve(context.Background(), user)
			if err != nil {
				order <- name + ": " + err.Error()
				return
			}
			mu.Lock()
			slots[name] = s
			mu.Unlock()
			order <- name
		}()
	}
	release := func(name string) {
		mu.Lock()
		s := slots[name]
		mu.Unlock()
		s.Release()
	}
	take("a2", "a") // first in line, and cannot run while a1 does
	waitUntilWaiting(t, r, 1)
	take("c", "c") // behind it, and can use any free slot
	waitUntilWaiting(t, r, 2)
	take("a3", "a") // the same person again, last in line
	waitUntilWaiting(t, r, 3)
	b1.Release()
	if got := <-order; got != "c" {
		t.Fatalf("the freed slot went to %q, want c: a waiter whose own slot is busy must not hold up the line", got)
	}
	a1.Release()
	if got := <-order; got != "a2" {
		t.Fatalf("after a's slot was freed: %q, want a2, which arrived before a3", got)
	}
	release("a2")
	if got := <-order; got != "a3" {
		t.Fatalf("then: %q, want a3", got)
	}
	release("a3")
	release("c")
	if r.Running() != 0 || r.waitingNow() != 0 {
		t.Errorf("%d running and %d waiting at the end", r.Running(), r.waitingNow())
	}
}

// A caller that leaves the line as a slot is handed to it gives the slot
// back, and the next in line gets it: nothing is lost. The moment is made
// here, not waited for: the caller's context ends while the test holds the
// Runner's lock, so it has left its wait and stands at the lock when the
// slot is handed to it.
func TestReserve_aSlotHandedToACallerThatLeftGoesToTheNext_2327(t *testing.T) {
	r := waitRunner(1, 10*time.Second, 16)
	held, err := r.Reserve(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	left := make(chan error, 1)
	go func() {
		s, err := r.Reserve(ctx, "")
		if err == nil {
			s.Release()
		}
		left <- err
	}()
	waitUntilWaiting(t, r, 1)
	next := make(chan error, 1)
	go func() {
		s, err := r.Reserve(context.Background(), "")
		if err == nil {
			s.Release()
		}
		next <- err
	}()
	waitUntilWaiting(t, r, 2)

	r.mu.Lock()
	cancel()
	// Time for the caller to wake on its context and reach the lock.
	time.Sleep(50 * time.Millisecond)
	// What held.Release does, under the lock already held.
	r.inFlight--
	r.grantLocked()
	handedToTheOneThatLeft := len(r.waiters) == 1
	r.mu.Unlock()
	held.released = true

	if !handedToTheOneThatLeft {
		t.Fatal("the slot was not handed to the first in line: this test did not make the moment it is about")
	}
	if err := <-left; !errors.Is(err, context.Canceled) {
		t.Errorf("the caller that left: %v, want its context's error", err)
	}
	select {
	case err := <-next:
		if err != nil {
			t.Fatalf("the next in line: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the next in line never got the slot: it was lost with the caller that left")
	}
	if r.Running() != 0 || r.waitingNow() != 0 {
		t.Fatalf("%d running and %d waiting after everyone finished", r.Running(), r.waitingNow())
	}
}
