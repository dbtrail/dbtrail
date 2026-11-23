package consoleapp

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// #3046: a connection a client keeps idle holds a slot only until another
// client needs it.
func TestPGSourceSlotsIdleGivesWay_3046(t *testing.T) {
	ctx := context.Background()
	p := newPGSourceSlots(1)
	p.wait = 0 // no wait: a slot is had at once or not at all
	release, ok := p.acquire(ctx)
	if !ok {
		t.Fatal("the first slot")
	}
	var reclaimed atomic.Int32
	unpark := p.park(func() { reclaimed.Add(1); release() })

	// Another client: the idle one gives way at once.
	start := time.Now()
	r2, ok := p.acquire(ctx)
	if !ok {
		t.Fatal("a client waited behind an idle connection")
	}
	if took := time.Since(start); took > time.Second {
		t.Errorf("took %v", took)
	}
	if n := reclaimed.Load(); n != 1 {
		t.Errorf("reclaimed %d times, want 1", n)
	}
	if unpark() {
		t.Error("the reclaimed connection was handed back to its client")
	}

	// Taken back by its client first: never reclaimed, and the slot stays
	// its own.
	unpark2 := p.park(func() { reclaimed.Add(1) })
	if !unpark2() {
		t.Fatal("the client could not take back its own connection")
	}
	if unpark2() {
		t.Error("taken back twice")
	}
	if _, ok := p.acquire(ctx); ok {
		t.Error("a slot was had while every one is in use")
	}
	if n := reclaimed.Load(); n != 1 {
		t.Errorf("reclaimed %d times, want 1", n)
	}
	r2()
}

// The one kept longest gives way first.
func TestPGSourceSlotsOldestIdleFirst_3046(t *testing.T) {
	ctx := context.Background()
	p := newPGSourceSlots(2)
	p.wait = 0
	var order []int
	for i := range 2 {
		release, ok := p.acquire(ctx)
		if !ok {
			t.Fatal(i)
		}
		p.park(func() { order = append(order, i); release() })
	}
	if _, ok := p.acquire(ctx); !ok {
		t.Fatal("no slot")
	}
	if len(order) != 1 || order[0] != 0 {
		t.Errorf("reclaimed %v, want the first kept", order)
	}
}

// A client waiting for a slot takes one that becomes idle while it waits.
func TestPGSourceSlotsWaiterTakesOneParkedWhileItWaits_3046(t *testing.T) {
	ctx := context.Background()
	p := newPGSourceSlots(1)
	p.wait = 5 * time.Second
	release, ok := p.acquire(ctx)
	if !ok {
		t.Fatal("the first slot")
	}
	got := make(chan bool, 1)
	go func() { _, ok := p.acquire(ctx); got <- ok }()
	time.Sleep(50 * time.Millisecond)
	start := time.Now()
	p.park(release)
	select {
	case ok := <-got:
		if !ok {
			t.Error("the waiter got no slot")
		}
		if took := time.Since(start); took > time.Second {
			t.Errorf("took %v", took)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the waiter was not told of the idle connection")
	}
}

// Many clients over few slots, each keeping its connection after its turn:
// nobody is refused while connections sit idle, and no wake-up is lost.
func TestPGSourceSlotsNoLostWakeup_3046(t *testing.T) {
	ctx := context.Background()
	p := newPGSourceSlots(2)
	p.wait = 3 * time.Second
	var refused atomic.Int32
	var wg sync.WaitGroup
	for range 12 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 100 {
				release, ok := p.acquire(ctx)
				if !ok {
					refused.Add(1)
					return
				}
				time.Sleep(100 * time.Microsecond)
				unpark := p.park(release)
				time.Sleep(50 * time.Microsecond)
				if unpark() {
					release()
				}
			}
		}()
	}
	wg.Wait()
	if n := refused.Load(); n != 0 {
		t.Errorf("%d clients refused", n)
	}
}
