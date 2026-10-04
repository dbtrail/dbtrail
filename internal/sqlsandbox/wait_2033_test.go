package sqlsandbox

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// waitRunner is a Runner with a short wait, for the gate alone (no worker).
func waitRunner(maxInFlight int, wait time.Duration, waiters int) *Runner {
	return New(Config{MaxInFlight: maxInFlight, MaxWait: wait, MaxWaiters: waiters})
}

// #2033: a statement that finds no free slot waits for one, up to a bound,
// instead of being refused at once. A burst (a dashboard, two people) turns
// slow instead of failing.
func TestReserve_waitsForAFreedSlot_2033(t *testing.T) {
	r := waitRunner(1, 5*time.Second, 4)
	held, err := r.Reserve(context.Background(), "a")
	if err != nil {
		t.Fatal(err)
	}
	time.AfterFunc(100*time.Millisecond, held.Release)
	start := time.Now()
	s, err := r.Reserve(context.Background(), "b")
	if err != nil {
		t.Fatalf("waiter: %v, want the freed slot", err)
	}
	if w := time.Since(start); w < 80*time.Millisecond {
		t.Errorf("got a slot after %v, before it was freed", w)
	}
	s.Release()
}

// The per-person gate waits too: the same person's second statement runs
// after the first instead of failing.
func TestReserve_samePersonWaitsForTheirOwn_2033(t *testing.T) {
	r := waitRunner(2, 5*time.Second, 4)
	first, _ := r.Reserve(context.Background(), "a")
	time.AfterFunc(100*time.Millisecond, first.Release)
	s, err := r.Reserve(context.Background(), "a")
	if err != nil {
		t.Fatalf("same person's second statement: %v", err)
	}
	s.Release()
}

// A waiter whose own per-person slot is busy must not hold up anyone else:
// b gets the free global slot at once while a's second statement waits.
func TestReserve_aBlockedWaiterDoesNotBlockOthers_2033(t *testing.T) {
	r := waitRunner(2, 2*time.Second, 4)
	first, _ := r.Reserve(context.Background(), "a")
	waiterDone := make(chan struct{})
	go func() {
		defer close(waiterDone)
		if s, err := r.Reserve(context.Background(), "a"); err == nil {
			s.Release()
		}
	}()
	defer func() { first.Release(); <-waiterDone }()
	for r.waitingNow() != 1 { // a's second is in line
		time.Sleep(5 * time.Millisecond)
	}
	start := time.Now()
	s, err := r.Reserve(context.Background(), "b")
	if err != nil {
		t.Fatalf("b: %v", err)
	}
	if w := time.Since(start); w > 50*time.Millisecond {
		t.Errorf("b waited %v behind a's waiter for a slot that was free", w)
	}
	s.Release()
}

// Past the bound the statement is refused, as busy, saying it waited.
func TestReserve_givesUpAfterTheBound_2033(t *testing.T) {
	r := waitRunner(1, 150*time.Millisecond, 4)
	held, _ := r.Reserve(context.Background(), "a")
	defer held.Release()
	start := time.Now()
	_, err := r.Reserve(context.Background(), "b")
	if !errors.Is(err, ErrBusy) {
		t.Fatalf("err = %v, want ErrBusy", err)
	}
	if w := time.Since(start); w < 140*time.Millisecond {
		t.Errorf("gave up after %v, before the bound", w)
	}
	if !strings.Contains(err.Error(), "waited") {
		t.Errorf("message %q does not say it waited", err)
	}
	if n := r.waitingNow(); n != 0 {
		t.Errorf("%d waiters left behind", n)
	}
}

// A caller that goes away (client hung up, request cancelled) leaves the
// line at once with its own error, not busy.
func TestReserve_cancelLeavesTheLine_2033(t *testing.T) {
	r := waitRunner(1, 5*time.Second, 4)
	held, _ := r.Reserve(context.Background(), "a")
	defer held.Release()
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(50*time.Millisecond, cancel)
	start := time.Now()
	_, err := r.Reserve(ctx, "b")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if w := time.Since(start); w > time.Second {
		t.Errorf("left the line after %v", w)
	}
	if n := r.waitingNow(); n != 0 {
		t.Errorf("%d waiters left behind", n)
	}
}

// The line is bounded: past MaxWaiters a statement is refused at once.
func TestReserve_fullLineRefusesAtOnce_2033(t *testing.T) {
	r := waitRunner(1, 5*time.Second, 1)
	held, _ := r.Reserve(context.Background(), "a")
	defer held.Release()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _, _ = r.Reserve(ctx, "b") }()
	for r.waitingNow() != 1 {
		time.Sleep(5 * time.Millisecond)
	}
	start := time.Now()
	_, err := r.Reserve(context.Background(), "c")
	if !errors.Is(err, ErrBusy) {
		t.Fatalf("err = %v, want ErrBusy", err)
	}
	if w := time.Since(start); w > 50*time.Millisecond {
		t.Errorf("a full line made c wait %v", w)
	}
}

// A negative MaxWait keeps the old behaviour: refused at once.
func TestReserve_noWaitRefusesAtOnce_2033(t *testing.T) {
	r := waitRunner(1, -1, 4)
	held, _ := r.Reserve(context.Background(), "a")
	defer held.Release()
	start := time.Now()
	if _, err := r.Reserve(context.Background(), "b"); !errors.Is(err, ErrBusy) {
		t.Fatalf("err = %v, want ErrBusy", err)
	}
	if w := time.Since(start); w > 50*time.Millisecond {
		t.Errorf("no-wait runner waited %v", w)
	}
}

// Many waiters, slots freed one by one: never more running than the cap,
// and every waiter is served (run with -race).
func TestReserve_neverOverAdmits_2033(t *testing.T) {
	const max, callers = 2, 12
	r := waitRunner(max, 10*time.Second, callers)
	var running, peak atomic.Int32
	var wg sync.WaitGroup
	var failed atomic.Int32
	for i := range callers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			s, err := r.Reserve(context.Background(), string(rune('a'+i)))
			if err != nil {
				failed.Add(1)
				return
			}
			n := running.Add(1)
			for {
				p := peak.Load()
				if n <= p || peak.CompareAndSwap(p, n) {
					break
				}
			}
			time.Sleep(20 * time.Millisecond)
			running.Add(-1)
			s.Release()
		}(i)
	}
	wg.Wait()
	if failed.Load() != 0 {
		t.Errorf("%d callers refused, want every one served", failed.Load())
	}
	if peak.Load() > max {
		t.Errorf("peak running = %d, cap %d", peak.Load(), max)
	}
}
