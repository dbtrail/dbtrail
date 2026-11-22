package consoleapp

import (
	"context"
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
