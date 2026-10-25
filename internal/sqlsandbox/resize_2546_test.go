package sqlsandbox

import (
	"context"
	"sync"
	"testing"
	"time"
)

// #2546: the number of statements that run at once changes while the Runner
// serves, so a router can make room for the copy's tails when a server turns
// to fresh mode and give it back when the last one turns fast. Nothing that
// runs is stopped: the count only decides who is let in next.

func TestSetMaxInFlight_refusesFewerThanOne_2546(t *testing.T) {
	r := waitRunner(3, time.Second, 4)
	for _, n := range []int{0, -1} {
		if err := r.SetMaxInFlight(n); err == nil {
			t.Errorf("SetMaxInFlight(%d) accepted", n)
		}
	}
	if got := r.MaxInFlight(); got != 3 {
		t.Fatalf("MaxInFlight = %d after refused values, want the 3 it had", got)
	}
}

func TestSetMaxInFlight_sameValueChangesNothing_2546(t *testing.T) {
	r := waitRunner(2, time.Second, 4)
	if err := r.SetMaxInFlight(2); err != nil {
		t.Fatal(err)
	}
	if got := r.MaxInFlight(); got != 2 {
		t.Fatalf("MaxInFlight = %d, want 2", got)
	}
}

// Lowered below what runs now, the statements running keep their slots, and
// none is let in until enough of them have ended.
func TestSetMaxInFlight_lowerKeepsRunningAndHoldsTheNext_2546(t *testing.T) {
	r := waitRunner(3, 5*time.Second, 8)
	var held []*Slot
	for _, u := range []string{"a", "b", "c"} {
		s, err := r.Reserve(context.Background(), u)
		if err != nil {
			t.Fatal(err)
		}
		held = append(held, s)
	}
	if err := r.SetMaxInFlight(1); err != nil {
		t.Fatal(err)
	}
	if got := r.Running(); got != 3 {
		t.Fatalf("Running = %d after lowering, want the 3 that were running", got)
	}
	got := make(chan error, 1)
	go func() {
		s, err := r.Reserve(context.Background(), "d")
		if err == nil {
			s.Release()
		}
		got <- err
	}()
	// Two of three end: still one running, which is the new count.
	held[0].Release()
	held[1].Release()
	select {
	case err := <-got:
		t.Fatalf("a statement was let in with 1 running on 1 slot: %v", err)
	case <-time.After(150 * time.Millisecond):
	}
	held[2].Release()
	select {
	case err := <-got:
		if err != nil {
			t.Fatalf("the waiting statement: %v, want the freed slot", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the waiting statement never got a slot after the last running one ended")
	}
}

// Raised, the statements already waiting are let in at once, not at the next
// release.
func TestSetMaxInFlight_raiseLetsTheWaitingIn_2546(t *testing.T) {
	r := waitRunner(1, 5*time.Second, 8)
	first, err := r.Reserve(context.Background(), "a")
	if err != nil {
		t.Fatal(err)
	}
	defer first.Release()
	got := make(chan *Slot, 2)
	for _, u := range []string{"b", "c"} {
		go func() {
			s, err := r.Reserve(context.Background(), u)
			if err != nil {
				t.Error(err)
				got <- nil
				return
			}
			got <- s
		}()
	}
	deadline := time.Now().Add(3 * time.Second)
	for r.Waiting() < 2 {
		if time.Now().After(deadline) {
			t.Fatal("the two statements never started waiting")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if err := r.SetMaxInFlight(3); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		select {
		case s := <-got:
			if s != nil {
				defer s.Release()
			}
		case <-time.After(time.Second):
			t.Fatal("a waiting statement was not let in when the count was raised")
		}
	}
	if n := r.Running(); n != 3 {
		t.Fatalf("Running = %d, want 3", n)
	}
}

// Never more running than the highest count it was set to, while it moves
// under statements that come and go, and no unguarded read of the count
// (-race). That each LOWER count holds the next statement back is
// lowerKeepsRunningAndHoldsTheNext's.
func TestSetMaxInFlight_neverOverTheHighestWhileItMoves_2546(t *testing.T) {
	r := waitRunner(4, 10*time.Second, 1000)
	var mu sync.Mutex
	running, worst := 0, 0
	stop := make(chan struct{})
	go func() {
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			n := []int{4, 1, 2, 3}[i%4]
			_ = r.SetMaxInFlight(n)
			time.Sleep(time.Millisecond)
		}
	}()
	var wg sync.WaitGroup
	for range 200 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s, err := r.Reserve(context.Background(), "")
			if err != nil {
				t.Error(err)
				return
			}
			mu.Lock()
			running++
			worst = max(worst, running)
			mu.Unlock()
			time.Sleep(200 * time.Microsecond)
			mu.Lock()
			running--
			mu.Unlock()
			s.Release()
		}()
	}
	wg.Wait()
	close(stop)
	if worst > 4 {
		t.Fatalf("%d statements ran at once, more than the highest count of 4", worst)
	}
	// Read from another goroutine than the one setting it: -race catches an
	// unguarded read.
	_ = r.MaxInFlight()
}

func TestSetMaxInFlight_closedRunnerIgnoresIt_2546(t *testing.T) {
	r := waitRunner(2, time.Second, 4)
	r.Close()
	if err := r.SetMaxInFlight(5); err == nil {
		t.Fatal("SetMaxInFlight on a closed Runner accepted")
	}
}
