package sqlsandbox

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// A line with no time bound (NoMaxWait): many more callers than slots, all on
// the shared slot the way the router's port takes it, are all served. None is
// told "busy" for having waited, however long the line was when it joined.
func TestReserve_noTimeBoundServesEveryone(t *testing.T) {
	const callers, slots = 256, 2
	r := waitRunner(slots, NoMaxWait, callers)
	var served, refused, running, most atomic.Int32
	var wg sync.WaitGroup
	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s, err := r.Reserve(context.Background(), "")
			if err != nil {
				refused.Add(1)
				return
			}
			n := running.Add(1)
			for {
				m := most.Load()
				if n <= m || most.CompareAndSwap(m, n) {
					break
				}
			}
			time.Sleep(time.Millisecond)
			running.Add(-1)
			s.Release()
			served.Add(1)
		}()
	}
	wg.Wait()
	if refused.Load() != 0 || served.Load() != callers {
		t.Fatalf("served %d and refused %d of %d callers; a line with no time bound refuses nobody", served.Load(), refused.Load(), callers)
	}
	if m := most.Load(); m > slots {
		t.Errorf("%d ran at once, over the %d slots", m, slots)
	}
	if n := r.waitingNow(); n != 0 {
		t.Errorf("%d waiters left behind", n)
	}
}

// With no time bound, what ends a wait is the caller leaving: its context is
// what a closed client connection cancels. The one that left is not served
// and the slot goes to the caller behind it.
func TestReserve_noTimeBoundACallerThatLeavesFreesItsPlace(t *testing.T) {
	r := waitRunner(1, NoMaxWait, 8)
	held, err := r.Reserve(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	gone, leave := context.WithCancel(context.Background())
	leftErr := make(chan error, 1)
	go func() {
		_, err := r.Reserve(gone, "")
		leftErr <- err
	}()
	eventually(t, func() bool { return r.waitingNow() == 1 })
	next := make(chan error, 1)
	go func() {
		s, err := r.Reserve(context.Background(), "")
		if err == nil {
			s.Release()
		}
		next <- err
	}()
	eventually(t, func() bool { return r.waitingNow() == 2 })
	leave()
	if err := <-leftErr; !errors.Is(err, context.Canceled) {
		t.Fatalf("the caller that left got %v, want its context's error", err)
	}
	held.Release()
	select {
	case err := <-next:
		if err != nil {
			t.Fatalf("the caller behind got %v, want the slot", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the caller behind the one that left was never served")
	}
}

// The default is unchanged: a Runner given no MaxWait still bounds the wait.
func TestNew_theDefaultWaitIsStillBounded(t *testing.T) {
	if got := New(Config{}).MaxWait(); got != DefaultMaxWait {
		t.Errorf("default MaxWait = %v, want %v", got, DefaultMaxWait)
	}
	if got := New(Config{MaxWait: NoMaxWait}).MaxWait(); got != NoMaxWait {
		t.Errorf("MaxWait = %v, want NoMaxWait kept as given", got)
	}
}

// eventually polls cond until it holds, and fails the test if it never does.
func eventually(t *testing.T, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(time.Millisecond) {
		if cond() {
			return
		}
	}
	t.Fatal("condition never held")
}
