package console

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/views"
)

// A statement reads the tails the follower last published and never reads
// the index itself. Each case steps the follower (followOnce) and awaits the
// builds it starts (settle), so what a statement sees is deterministic; the
// last ones run the follower's own goroutine.
func TestTailFollower(t *testing.T) {
	ctx := context.Background()
	orders, customers := keeperTable("orders"), keeperTable("customers")
	ko := tailKey("shop", "orders")
	s := &Server{}
	clock := time.Date(2026, 10, 21, 13, 0, 0, 0, time.UTC)
	newKeeper := func(f *fakeIndex) *tailKeeper {
		k := newTailKeeper(f, t.TempDir(), 3, time.Minute, time.Hour)
		k.now = func() time.Time { return clock }
		return k
	}
	follow := func(k *tailKeeper) {
		k.followOnce(ctx)
		k.settle()
		k.followOnce(ctx)
	}
	kicked := func(k *tailKeeper) bool {
		select {
		case <-k.pub.kick:
			return true
		default:
			return false
		}
	}

	t.Run("the first statement over a table reads none and wakes the follower", func(t *testing.T) {
		f := &fakeIndex{}
		f.add("orders", "1")
		k := newKeeper(f)
		if _, _, ok := s.statementTails(k, []views.BaselineTable{orders}, []views.BaselineTable{orders}); ok {
			t.Fatal("a tail before any update")
		}
		if !kicked(k) {
			t.Error("the follower was not woken for a table it was never asked for")
		}
		follow(k)
		out, through, ok := s.statementTails(k, []views.BaselineTable{orders}, []views.BaselineTable{orders})
		if !ok || out[0].Tail == nil || !through.Equal(f.at(1)) {
			t.Fatalf("after an update: %v %v %+v", ok, through, out)
		}
	})

	t.Run("a statement reads no index: one that fails leaves it what was published", func(t *testing.T) {
		f := &fakeIndex{}
		f.add("orders", "1")
		k := newKeeper(f)
		s.statementTails(k, []views.BaselineTable{orders}, []views.BaselineTable{orders})
		follow(k)
		f.mu.Lock()
		f.newestErr = errors.New("index down")
		f.mu.Unlock()
		if _, _, ok := s.statementTails(k, []views.BaselineTable{orders}, []views.BaselineTable{orders}); !ok {
			t.Fatal("a statement read the index")
		}
		// The follower meets the error: nothing published until an update
		// succeeds, as a statement that met it did before.
		if !k.followOnce(ctx) {
			t.Fatal("the follower stopped with a table asked for")
		}
		if _, _, ok := s.statementTails(k, []views.BaselineTable{orders}, []views.BaselineTable{orders}); ok {
			t.Fatal("the last version was kept as current after a failed update")
		}
		f.mu.Lock()
		f.newestErr = nil
		f.mu.Unlock()
		f.add("orders", "2")
		follow(k)
		if out, through, ok := s.statementTails(k, []views.BaselineTable{orders}, []views.BaselineTable{orders}); !ok || !through.Equal(f.at(2)) || out[0].Tail == nil {
			t.Fatalf("after the index came back: %v %v", ok, through)
		}
	})

	t.Run("a newer cut reads none until an update over it", func(t *testing.T) {
		f := &fakeIndex{}
		f.add("orders", "1")
		k := newKeeper(f)
		s.statementTails(k, []views.BaselineTable{orders}, []views.BaselineTable{orders})
		follow(k)
		kicked(k)
		newer := newerCut(orders)
		if _, _, ok := s.statementTails(k, []views.BaselineTable{newer}, []views.BaselineTable{newer}); ok {
			t.Fatal("a version brought up to date over the older cut was laid over the newer one")
		}
		if !kicked(k) {
			t.Error("the follower was not woken for a newer cut")
		}
		follow(k)
		out, _, ok := s.statementTails(k, []views.BaselineTable{newer}, []views.BaselineTable{newer})
		if !ok || out[0].Tail == nil {
			t.Fatalf("after an update over the newer cut: %v %+v", ok, out)
		}
		// A statement still over the older cut (it read the copy before the
		// refresh) reads none: the version is the newer cut's.
		if _, _, ok := s.statementTails(k, []views.BaselineTable{orders}, []views.BaselineTable{orders}); ok {
			t.Fatal("a version over the newer cut was laid over the older one")
		}
	})

	t.Run("a table with no tail does not wake the follower again", func(t *testing.T) {
		f := &fakeIndex{startWhy: map[string]string{tailKey("shop", "customers"): "no key"}}
		f.add("orders", "1")
		k := newKeeper(f)
		both := []views.BaselineTable{orders, customers}
		s.statementTails(k, both, both)
		follow(k)
		kicked(k)
		if _, _, ok := s.statementTails(k, both, both); ok {
			t.Fatal("a statement over a table without a tail read tails")
		}
		if kicked(k) {
			t.Error("a refused table woke the follower: it would update back to back")
		}
		if _, _, ok := s.statementTails(k, both[:1], both[:1]); !ok {
			t.Error("the table with a tail has none on its own")
		}
	})

	t.Run("tables no statement asked for in idle are left out; with none it stops", func(t *testing.T) {
		f := &fakeIndex{}
		f.add("orders", "1")
		f.add("customers", "9")
		k := newKeeper(f)
		s.statementTails(k, []views.BaselineTable{orders}, []views.BaselineTable{orders})
		clock = clock.Add(30 * time.Second)
		s.statementTails(k, []views.BaselineTable{customers}, []views.BaselineTable{customers})
		clock = clock.Add(45 * time.Second) // orders asked 75 s ago, customers 45 s
		follow(k)
		k.pub.mu.Lock()
		_, hasOrders := k.pub.cuts[ko]
		k.pub.mu.Unlock()
		if hasOrders {
			t.Error("a table no statement asked for in idle was brought up to date")
		}
		clock = clock.Add(time.Minute)
		k.pub.running = true // as the goroutine would be
		if k.followOnce(ctx) {
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
		k := newTailKeeper(f, t.TempDir(), 3, time.Minute, time.Hour)
		k.pub.auto, k.pub.every = true, 5*time.Millisecond
		read := []views.BaselineTable{orders}
		deadline := time.Now().Add(10 * time.Second)
		for {
			if _, _, ok := s.statementTails(k, read, read); ok {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("no tail from the follower's goroutine")
			}
			time.Sleep(5 * time.Millisecond)
		}
		k.close()
		for {
			k.pub.mu.Lock()
			running := k.pub.running
			k.pub.mu.Unlock()
			if !running {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("the follower outlived its keeper")
			}
			time.Sleep(5 * time.Millisecond)
		}
		if _, _, ok := s.statementTails(k, read, read); ok {
			t.Fatal("a closed keeper's tails were read")
		}
		k.settle()
	})

	t.Run("a panic stops the goroutine unmarked, so the next statement starts another", func(t *testing.T) {
		f := &fakeIndex{}
		f.add("orders", "1")
		k := newTailKeeper(panicIndex{f}, t.TempDir(), 3, time.Minute, time.Hour)
		k.pub.auto = true
		defer k.close()
		read := []views.BaselineTable{orders}
		s.statementTails(k, read, read)
		deadline := time.Now().Add(10 * time.Second)
		for {
			k.pub.mu.Lock()
			running := k.pub.running
			k.pub.mu.Unlock()
			if !running {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("the follower is still marked running after its panic")
			}
			time.Sleep(5 * time.Millisecond)
		}
	})
}

// panicIndex is an index whose newest event cannot be read without a panic.
type panicIndex struct{ *fakeIndex }

func (panicIndex) Newest(context.Context) (tailMark, error) { panic("newest") }
