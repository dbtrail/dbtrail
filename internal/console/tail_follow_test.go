package console

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/views"
)

// A statement reads the tails the follower last published, and reads the
// index itself only for a table the follower has not brought up to date over
// its cut. Each case steps the follower (followOnce) and awaits the builds it
// starts (settle), so what a statement sees is deterministic; the last ones
// run the follower's own goroutine. updates counts the updates started: one
// more after a statement means the statement read the index.
func TestTailFollower(t *testing.T) {
	ctx := context.Background()
	orders, customers := keeperTable("orders"), keeperTable("customers")
	ko := tailKey("shop", "orders")
	s := &Server{}
	// The clock is read by builds on their own goroutines (install) and
	// moved by the test: under a lock, or the race detector sees the write.
	var clockMu sync.Mutex
	clock := time.Date(2026, 10, 21, 13, 0, 0, 0, time.UTC)
	now := func() time.Time { clockMu.Lock(); defer clockMu.Unlock(); return clock }
	tick := func(d time.Duration) { clockMu.Lock(); clock = clock.Add(d); clockMu.Unlock() }
	newKeeper := func(f *fakeIndex) *tailKeeper {
		k := newTailKeeper(f, t.TempDir(), tailBudget{Keys: 3}, time.Minute, time.Hour)
		k.now = now
		return k
	}
	follow := func(k *tailKeeper) {
		k.followOnce(ctx, time.Minute)
		k.settle()
		k.followOnce(ctx, time.Minute)
	}
	updates := func(k *tailKeeper) uint64 {
		k.pub.mu.Lock()
		defer k.pub.mu.Unlock()
		return k.pub.gen
	}
	one := func(t views.BaselineTable) []views.BaselineTable { return []views.BaselineTable{t} }

	t.Run("a table the follower never had: the statement brings it up to date itself", func(t *testing.T) {
		f := &fakeIndex{}
		f.add("orders", "1")
		k := newKeeper(f)
		// Its build runs in the background (#2483): none for this statement.
		if _, _, ok := s.statementTails(ctx, k, one(orders), one(orders)); ok {
			t.Fatal("a tail before its build")
		}
		if updates(k) != 1 {
			t.Fatalf("%d updates by the first statement, want 1", updates(k))
		}
		k.settle()
		follow(k)
		out, through, ok := s.statementTails(ctx, k, one(orders), one(orders))
		if !ok || out[0].Tail == nil || !through.Equal(f.at(1)) {
			t.Fatalf("after an update: %v %v %+v", ok, through, out)
		}
	})

	t.Run("a statement over what was published reads no index", func(t *testing.T) {
		f := &fakeIndex{}
		f.add("orders", "1")
		k := newKeeper(f)
		s.statementTails(ctx, k, one(orders), one(orders))
		k.settle()
		follow(k)
		f.mu.Lock()
		f.newestErr = errors.New("index down")
		f.mu.Unlock()
		n := updates(k)
		if _, _, ok := s.statementTails(ctx, k, one(orders), one(orders)); !ok || updates(k) != n {
			t.Fatalf("a statement read the index: tails %v, updates %d → %d", ok, n, updates(k))
		}
		// The follower meets the error: nothing published until an update
		// succeeds, and statements do not read the index for it each.
		if !k.followOnce(ctx, time.Minute) {
			t.Fatal("the follower stopped with a table asked for")
		}
		n = updates(k)
		if _, _, ok := s.statementTails(ctx, k, one(orders), one(orders)); ok {
			t.Fatal("the last version was kept as current after a failed update")
		}
		if updates(k) != n {
			t.Error("a statement read the index after the follower failed on it")
		}
		f.mu.Lock()
		f.newestErr = nil
		f.mu.Unlock()
		f.add("orders", "2")
		follow(k)
		if out, through, ok := s.statementTails(ctx, k, one(orders), one(orders)); !ok || !through.Equal(f.at(2)) || out[0].Tail == nil {
			t.Fatalf("after the index came back: %v %v", ok, through)
		}
	})

	t.Run("a newer cut: never the older cut's version", func(t *testing.T) {
		f := &fakeIndex{}
		f.add("orders", "1")
		k := newKeeper(f)
		s.statementTails(ctx, k, one(orders), one(orders))
		k.settle()
		follow(k)
		newer := newerCut(orders)
		n := updates(k)
		if _, _, ok := s.statementTails(ctx, k, one(newer), one(newer)); ok {
			t.Fatal("a version brought up to date over the older cut was laid over the newer one")
		}
		if updates(k) != n+1 {
			t.Error("the statement over a newer cut did not bring it up to date")
		}
		k.settle()
		follow(k)
		out, _, ok := s.statementTails(ctx, k, one(newer), one(newer))
		if !ok || out[0].Tail == nil {
			t.Fatalf("after an update over the newer cut: %v %+v", ok, out)
		}
		// A statement still over the older cut (it read the copy before the
		// refresh) reads none: the version is the newer cut's.
		if _, _, ok := s.statementTails(ctx, k, one(orders), one(orders)); ok {
			t.Fatal("a version over the newer cut was laid over the older one")
		}
	})

	t.Run("a table with no tail is not brought up to date by every statement", func(t *testing.T) {
		f := &fakeIndex{startWhy: map[string]string{tailKey("shop", "customers"): "no key"}}
		f.add("orders", "1")
		k := newKeeper(f)
		both := []views.BaselineTable{orders, customers}
		s.statementTails(ctx, k, both, both)
		k.settle()
		follow(k)
		n := updates(k)
		if _, _, ok := s.statementTails(ctx, k, both, both); ok {
			t.Fatal("a statement over a table without a tail read tails")
		}
		if updates(k) != n {
			t.Error("a refused table had a statement read the index")
		}
		if _, _, ok := s.statementTails(ctx, k, both[:1], both[:1]); !ok {
			t.Error("the table with a tail has none on its own")
		}
	})

	t.Run("an update running: the statement does not wait behind it", func(t *testing.T) {
		f := &fakeIndex{}
		f.add("orders", "1")
		k := newKeeper(f)
		k.pub.updating, k.pub.started = true, now()
		if _, _, ok := s.statementTails(ctx, k, one(orders), one(orders)); ok || updates(k) != 0 {
			t.Fatalf("tails %v, updates %d", ok, updates(k))
		}
		if k.pub.stuck {
			t.Fatal("said stuck within its deadline")
		}
		tick(2*k.pub.deadline + time.Second)
		s.statementTails(ctx, k, one(orders), one(orders))
		if !k.pub.stuck {
			t.Error("an update far past its deadline was not said")
		}
	})

	t.Run("an older update that ends last is not published", func(t *testing.T) {
		f := &fakeIndex{}
		f.add("orders", "1")
		k := newKeeper(f)
		s.statementTails(ctx, k, one(orders), one(orders))
		k.settle()
		follow(k)
		k.pub.mu.Lock()
		k.pub.pubGen = k.pub.gen + 5 // a newer update was published
		views := k.pub.views
		k.pub.mu.Unlock()
		f.mu.Lock()
		f.newestErr = errors.New("index down")
		f.mu.Unlock()
		k.followOnce(ctx, time.Minute)
		k.pub.mu.Lock()
		defer k.pub.mu.Unlock()
		if k.pub.views == nil || len(k.pub.views) != len(views) {
			t.Fatal("an older update's failure replaced a newer one's version")
		}
	})

	t.Run("tables no statement asked for in idle are left out; with none it stops", func(t *testing.T) {
		f := &fakeIndex{}
		f.add("orders", "1")
		f.add("customers", "9")
		k := newKeeper(f)
		s.statementTails(ctx, k, one(orders), one(orders))
		tick(30 * time.Second)
		s.statementTails(ctx, k, one(customers), one(customers))
		k.settle()
		tick(45 * time.Second) // orders asked 75 s ago, customers 45 s
		follow(k)
		k.pub.mu.Lock()
		_, hasOrders := k.pub.cuts[ko]
		k.pub.mu.Unlock()
		if hasOrders {
			t.Error("a table no statement asked for in idle was brought up to date")
		}
		tick(time.Minute)
		k.pub.running = true // as the goroutine would be
		if k.followOnce(ctx, time.Minute) {
			t.Fatal("the follower went on with no table asked for")
		}
		k.pub.mu.Lock()
		defer k.pub.mu.Unlock()
		if k.pub.running || k.pub.views != nil {
			t.Fatalf("stopped but still marked running (%v) or publishing %v", k.pub.running, k.pub.views)
		}
	})

	t.Run("the goroutine: started by a statement, ended by close", func(t *testing.T) {
		f := &fakeIndex{}
		f.add("orders", "1")
		k := newTailKeeper(f, t.TempDir(), tailBudget{Keys: 3}, time.Minute, time.Hour)
		k.pub.auto, k.pub.every = true, 5*time.Millisecond
		read := one(orders)
		s.statementTails(ctx, k, read, read)
		k.settle()
		// No statement reads the index from here: the goroutine publishes.
		f.add("orders", "2")
		waitFor(t, "a tail through the newest event from the goroutine", func() bool {
			_, through, ok := s.statementTails(ctx, k, read, read)
			return ok && through.Equal(f.at(2))
		})
		k.close()
		waitFor(t, "the follower to end with its keeper", func() bool {
			k.pub.mu.Lock()
			defer k.pub.mu.Unlock()
			return !k.pub.running
		})
		if _, _, ok := s.statementTails(ctx, k, read, read); ok {
			t.Fatal("a closed keeper's tails were read")
		}
		k.settle()
	})

	t.Run("a panic stops the goroutine unmarked, and the next waits", func(t *testing.T) {
		f := &fakeIndex{}
		f.add("orders", "1")
		armed := &atomic.Bool{}
		k := newTailKeeper(panicIndex{f, armed}, t.TempDir(), tailBudget{Keys: 3}, time.Minute, time.Hour)
		k.pub.auto, k.pub.every = true, 5*time.Millisecond
		defer k.close()
		read := one(orders)
		s.statementTails(ctx, k, read, read)
		armed.Store(true)
		waitFor(t, "the follower to end on its panic", func() bool {
			k.pub.mu.Lock()
			defer k.pub.mu.Unlock()
			return !k.pub.running && k.pub.panics == 1
		})
		armed.Store(false)
		s.statementTails(ctx, k, read, read) // within retryMin of the panic
		k.pub.mu.Lock()
		running := k.pub.running
		k.pub.mu.Unlock()
		if running {
			t.Fatal("a follower started again right after a panic: one that recurs would log a stack per statement")
		}
		k.settle()
	})
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for " + what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// panicIndex is an index whose newest event cannot be read without a panic
// while armed.
type panicIndex struct {
	*fakeIndex
	armed *atomic.Bool
}

func (p panicIndex) Newest(ctx context.Context) (tailMark, error) {
	if p.armed.Load() {
		panic("newest")
	}
	return p.fakeIndex.Newest(ctx)
}
