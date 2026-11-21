package consoleapp

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// #2985: the connections to the sources are a line, as the copy's slots are:
// a statement that finds them all taken waits for one, up to the wait, and
// is refused only then.

// 50 clients over 16: every one gets a connection, none more than 16 at once.
func TestPGSourceSlots_2985_manyWaitAndNoneIsRefused(t *testing.T) {
	const clients, slots, each = 50, 16, 20
	p := newPGSourceSlots(slots)
	if p.wait != pgSourceSlotWait {
		t.Fatalf("wait %v, want %v", p.wait, pgSourceSlotWait)
	}
	var held, most, refused atomic.Int32
	var wg sync.WaitGroup
	for range clients {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range each {
				release, ok := p.acquire(context.Background())
				if !ok {
					refused.Add(1)
					continue
				}
				n := held.Add(1)
				for m := most.Load(); n > m && !most.CompareAndSwap(m, n); m = most.Load() {
				}
				time.Sleep(time.Millisecond)
				held.Add(-1)
				release()
				release() // twice gives back one
			}
		}()
	}
	wg.Wait()
	if n := refused.Load(); n != 0 {
		t.Errorf("%d of %d refused", n, clients*each)
	}
	if m := most.Load(); m > slots {
		t.Errorf("%d held at once, over %d", m, slots)
	}
	if n := len(p.ch); n != 0 {
		t.Errorf("%d still taken after every one was given back", n)
	}
}

// With every connection taken: refused once the wait runs out, or sooner when
// the statement's context ends; a connection given back during the wait is
// taken.
func TestPGSourceSlots_2985_theWaitIsBounded(t *testing.T) {
	p := newPGSourceSlots(1)
	p.wait = 100 * time.Millisecond
	first, ok := p.acquire(context.Background())
	if !ok {
		t.Fatal("the first was refused")
	}
	start := time.Now()
	if _, ok := p.acquire(context.Background()); ok {
		t.Fatal("a second was given while the first is held")
	}
	if took := time.Since(start); took < 90*time.Millisecond || took > 5*time.Second {
		t.Errorf("refused after %v, want the wait of %v", took, p.wait)
	}

	p.wait = time.Minute
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start = time.Now()
	if _, ok := p.acquire(ctx); ok {
		t.Fatal("given with the first held")
	}
	if took := time.Since(start); took > 10*time.Second {
		t.Errorf("a statement whose context ended waited %v", took)
	}

	go func() {
		time.Sleep(50 * time.Millisecond)
		first()
	}()
	second, ok := p.acquire(context.Background())
	if !ok {
		t.Fatal("not given once the first was given back")
	}
	second()

	// No wait at all: refused at once, as before.
	p.wait = 0
	held, _ := p.acquire(context.Background())
	if _, ok := p.acquire(context.Background()); ok {
		t.Error("given past the limit with no wait")
	}
	held()
}
