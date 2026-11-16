package console

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/views"
)

// Fresh mode never answers from the snapshot alone without saying so. A
// concurrency bench found about 1 % of fresh answers read the snapshot alone,
// up to 3 minutes behind, with nothing in the answer to say so, and one
// connection read a counter that only goes up go back from 1319 to 339.
// Three ways led there, one case each below, and the rule that closes them.

// Way 1: an edit of a server that leaves its index alone (its application
// login on the port) makes a new bundle over the same connection, and the
// keeper was replaced for it: every statement read the snapshot alone until
// the tails were built again.
func TestTailKeeperOutlivesABundleOverTheSameIndex(t *testing.T) {
	t.Setenv("BINTRAIL_SQL_TAILS", "on")
	reg, ids := freshTestRegistry(t, "prod")
	id := ids[0]
	s := &Server{cm: &connManager{reg: reg}}
	open := func() *sql.DB {
		db, err := sql.Open("mysql", "u:p@tcp(127.0.0.1:1)/idx") // never dialed
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { db.Close() })
		return db
	}
	db := open()
	k1 := s.tailKeeperFor(id, &bundle{db: db, dbName: "idx"})
	if k1 == nil {
		t.Fatal("no keeper for a fresh server")
	}
	t.Cleanup(func() {
		s.tails.mu.Lock()
		defer s.tails.mu.Unlock()
		for _, k := range s.tails.by {
			k.close()
		}
	})

	// rebuildDerived: a new bundle, the same connection.
	if k2 := s.tailKeeperFor(id, &bundle{db: db, dbName: "idx"}); k2 != k1 {
		t.Fatal("a new bundle over the same index connection replaced the keeper: its tails were lost")
	}
	if k1.life.Err() != nil {
		t.Fatal("the keeper was closed")
	}
	// Another connection (the index's DSN changed): another index.
	if k3 := s.tailKeeperFor(id, &bundle{db: open(), dbName: "idx"}); k3 == k1 {
		t.Fatal("a new index connection kept the old keeper")
	}
	// What capture records changed: the tails cannot vouch for it.
	k4 := s.tailKeeperFor(id, s.tails.by[id].src.(*indexTailSource).b)
	if err := reg.UpdateWith(id, func(e ServerEntry) (ServerEntry, bool) { e.Schemas = "shop"; return e, true }); err != nil {
		t.Fatal(err)
	}
	if k5 := s.tailKeeperFor(id, k4.src.(*indexTailSource).b); k5 == k4 {
		t.Fatal("the schemas capture records changed and the keeper was kept")
	}
}

// Way 3: an update that failed (a statement's own, in its 2 s, or the
// follower's) published no views at all, so every statement read the
// snapshot alone until one succeeded.
func TestAFailedUpdateKeepsTheLastViews(t *testing.T) {
	ctx := context.Background()
	f := &fakeIndex{}
	f.add("orders", "1")
	k := newTailKeeper(f, t.TempDir(), tailBudget{Keys: 3}, time.Minute, time.Hour)
	s := &Server{}
	orders := []views.BaselineTable{keeperTable("orders")}
	s.statementTails(ctx, k, orders, orders)
	k.settle()
	k.followOnce(ctx, time.Minute)
	k.followOnce(ctx, time.Minute)
	if _, _, ok := s.statementTails(ctx, k, orders, orders); !ok {
		t.Fatal("the rig has no tail")
	}
	f.mu.Lock()
	f.newestErr = errors.New("the index went away")
	f.mu.Unlock()
	k.followOnce(ctx, time.Minute)
	if _, _, ok := s.statementTails(ctx, k, orders, orders); !ok {
		t.Fatal("after a failed update the statement has no tails: it reads the snapshot alone")
	}
	// Over a newer cut the old view does not stand: that table is waited
	// for (statementTailsFresh), not answered with an older one.
	newer := []views.BaselineTable{newerCut(keeperTable("orders"))}
	k.pub.mu.Lock()
	k.pub.asked[tailKey("shop", "orders")] = tailAsk{table: newer[0], at: k.now()}
	k.pub.mu.Unlock()
	k.followOnce(ctx, time.Minute)
	if v, _ := k.published(newer); len(v) != 0 {
		t.Fatalf("a view over the old cut stands for the new one: %v", v)
	}
}

// Kept views are kept only while their files are.
func TestAFailedUpdateDropsViewsOlderThanTheirFiles(t *testing.T) {
	ctx := context.Background()
	f := &fakeIndex{}
	f.add("orders", "1")
	var mu sync.Mutex
	clock := time.Date(2026, 11, 16, 13, 0, 0, 0, time.UTC)
	k := newTailKeeper(f, t.TempDir(), tailBudget{Keys: 3}, time.Minute, time.Hour)
	k.now = func() time.Time { mu.Lock(); defer mu.Unlock(); return clock }
	s := &Server{}
	orders := []views.BaselineTable{keeperTable("orders")}
	s.statementTails(ctx, k, orders, orders)
	k.settle()
	k.followOnce(ctx, time.Minute)
	k.followOnce(ctx, time.Minute)
	f.mu.Lock()
	f.newestErr = errors.New("the index went away")
	f.mu.Unlock()
	mu.Lock()
	clock = clock.Add(k.keepOld)
	mu.Unlock()
	k.followOnce(ctx, time.Minute)
	if v, _ := k.published(orders); len(v) != 0 {
		t.Fatal("views kept past half the time their replaced files are kept")
	}
}

// The rule: a table whose tail is on its way is waited for; one refused
// until the next snapshot is not; past the wait the statement is told why.
func TestStatementTailsFresh(t *testing.T) {
	ctx := context.Background()
	s := &Server{}
	newRig := func(f *fakeIndex) *tailKeeper {
		f.add("orders", "1")
		return newTailKeeper(f, t.TempDir(), tailBudget{Keys: 3}, time.Minute, time.Hour)
	}
	orders := []views.BaselineTable{keeperTable("orders")}

	t.Run("a tail on its way is waited for, the follower woken", func(t *testing.T) {
		k := newRig(&fakeIndex{})
		type res struct {
			ok      bool
			missing string
		}
		done := make(chan res, 1)
		go func() {
			_, _, _, ok, missing := s.statementTailsFresh(ctx, k, orders, orders, 10*time.Second)
			done <- res{ok, missing}
		}()
		// The test is the follower: it takes the wake and updates.
		for i := 0; ; i++ {
			select {
			case <-k.pub.wake:
			case r := <-done:
				t.Fatalf("the statement ended before its tail was ready: %+v", r)
			case <-time.After(10 * time.Second):
				t.Fatal("the follower was not woken")
			}
			k.settle()
			k.followOnce(ctx, time.Minute)
			select {
			case r := <-done:
				if !r.ok || r.missing != "" {
					t.Fatalf("got %+v, want the tail", r)
				}
				return
			case <-time.After(200 * time.Millisecond):
			}
			if i > 20 {
				t.Fatal("the statement never got its tail")
			}
		}
	})

	t.Run("past the wait the statement is told, not answered with the snapshot as fresh", func(t *testing.T) {
		f := &fakeIndex{fillGate: map[string]chan struct{}{tailKey("shop", "orders"): make(chan struct{})}}
		k := newRig(f)
		start := time.Now()
		_, _, _, ok, missing := s.statementTailsFresh(ctx, k, orders, orders, 150*time.Millisecond)
		if ok || !strings.Contains(missing, "shop.orders") || !strings.Contains(missing, "not ready") {
			t.Fatalf("ok %v, missing %q", ok, missing)
		}
		if took := time.Since(start); took < 150*time.Millisecond || took > 5*time.Second {
			t.Fatalf("waited %v with a wait of 150ms", took)
		}
		close(f.fillGate[tailKey("shop", "orders")])
		k.close()
		k.settle()
	})

	t.Run("a table that cannot have a tail is read from its snapshot, as without fresh mode, at once", func(t *testing.T) {
		f := &fakeIndex{startWhy: map[string]string{tailKey("shop", "orders"): "its key is not made of integers"}}
		k := newRig(f)
		s.statementTails(ctx, k, orders, orders)
		k.settle()
		k.followOnce(ctx, time.Minute)
		start := time.Now()
		_, _, _, ok, missing := s.statementTailsFresh(ctx, k, orders, orders, 10*time.Second)
		if ok || missing != "" {
			t.Fatalf("ok %v, missing %q: a table that cannot have a tail is not sent to MySQL", ok, missing)
		}
		if took := time.Since(start); took > time.Second {
			t.Fatalf("a refused tail was waited for %v", took)
		}
	})

	t.Run("the client leaves while it waits", func(t *testing.T) {
		f := &fakeIndex{fillGate: map[string]chan struct{}{tailKey("shop", "orders"): make(chan struct{})}}
		k := newRig(f)
		cctx, cancel := context.WithCancel(ctx)
		go func() { time.Sleep(50 * time.Millisecond); cancel() }()
		start := time.Now()
		if _, _, _, ok, _ := s.statementTailsFresh(cctx, k, orders, orders, 10*time.Second); ok {
			t.Fatal("a tail with its build held")
		}
		if took := time.Since(start); took > 3*time.Second {
			t.Fatalf("the wait went on %v after the client left", took)
		}
		close(f.fillGate[tailKey("shop", "orders")])
		k.close()
		k.settle()
	})
}

func TestSeenFloor(t *testing.T) {
	ctx := context.Background()
	if seenFloor(ctx) != 0 || seenFloor(withSeenFloor(ctx, 0)) != 0 {
		t.Fatal("a floor where none was set")
	}
	if got := seenFloor(withSeenFloor(ctx, 42)); got != 42 {
		t.Fatalf("floor %d, want 42", got)
	}
}

func TestFreshSnapshotNote(t *testing.T) {
	at := time.Date(2026, 11, 16, 2, 5, 23, 0, time.UTC)
	got := freshSnapshotNote("the newest captured changes of bank.accounts are not ready yet", at)
	want := "Answered from its snapshot of 2026-11-16 02:05:23 UTC without the newest captured changes: the newest captured changes of bank.accounts are not ready yet."
	if got != want {
		t.Fatalf("note\n got %q\nwant %q", got, want)
	}
	if !strings.Contains(freshSnapshotNote("x", time.Time{}), "its last snapshot") {
		t.Fatal("a note with no snapshot time")
	}
}

// The bench's trigger, end to end in this process: the daemon saves an
// application login on the port for a server, the router reads the servers
// file again, and the server's tails stay.
func TestAPortLoginEditKeepsTheTails(t *testing.T) {
	t.Setenv("BINTRAIL_SQL_TAILS", "on")
	owner, srv, prod, _ := followerServer(t)
	db, err := sql.Open("mysql", "u:p@tcp(127.0.0.1:1)/idx") // never dialed
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	srv.cm.mu.Lock()
	b := srv.cm.newBundle(db, "idx", prod)
	srv.cm.bundles[prod.ID] = b
	srv.cm.mu.Unlock()
	k := srv.tailKeeperFor(prod.ID, b)
	if k == nil {
		t.Fatal("no keeper for a fresh server")
	}
	defer k.close()
	if _, err := owner.SetPortLogin(prod.ID, "app", "pw"); err != nil {
		t.Fatal(err)
	}
	srv.RefreshFollowedFiles()
	srv.cm.mu.Lock()
	nb := srv.cm.bundles[prod.ID]
	srv.cm.mu.Unlock()
	if nb == b || nb == nil || nb.db != db {
		t.Fatalf("the edit did not make a new bundle over the same connection (the case this pins): %p %p", b, nb)
	}
	if srv.tailKeeperFor(prod.ID, nb) != k || k.life.Err() != nil {
		t.Fatal("a login saved on the port replaced the server's tails: its statements read the snapshot alone")
	}
}

// An index with no captured change has no tails, and no wait brings one:
// the statement reads the snapshot at once, as without fresh mode.
func TestStatementTailsFreshOverAnEmptyIndex(t *testing.T) {
	ctx := context.Background()
	k := newTailKeeper(&fakeIndex{}, t.TempDir(), tailBudget{Keys: 3}, time.Minute, time.Hour)
	orders := []views.BaselineTable{keeperTable("orders")}
	s := &Server{}
	s.statementTails(ctx, k, orders, orders)
	k.settle()
	k.followOnce(ctx, time.Minute)
	start := time.Now()
	_, _, _, ok, missing := s.statementTailsFresh(ctx, k, orders, orders, 10*time.Second)
	if ok || missing != "" {
		t.Fatalf("ok %v, missing %q: an index with nothing captured has no tails, by design", ok, missing)
	}
	if took := time.Since(start); took > time.Second {
		t.Fatalf("waited %v over an empty index", took)
	}
}

// One table refused until its next snapshot and another on its way: all or
// nothing, so the statement reads the snapshot at once.
func TestStatementTailsFreshARefusedTableEndsTheWait(t *testing.T) {
	ctx := context.Background()
	f := &fakeIndex{startWhy: map[string]string{tailKey("shop", "customers"): "its key is not made of integers"}}
	f.add("orders", "1")
	f.add("customers", "1")
	k := newTailKeeper(f, t.TempDir(), tailBudget{Keys: 3}, time.Minute, time.Hour)
	s := &Server{}
	customers := []views.BaselineTable{keeperTable("customers")}
	s.statementTails(ctx, k, customers, customers)
	k.settle()
	k.followOnce(ctx, time.Minute)
	f.mu.Lock()
	f.fillGate = map[string]chan struct{}{tailKey("shop", "orders"): make(chan struct{})}
	f.mu.Unlock()
	both := []views.BaselineTable{keeperTable("orders"), keeperTable("customers")}
	start := time.Now()
	_, _, _, ok, missing := s.statementTailsFresh(ctx, k, both, both, 10*time.Second)
	if ok || missing != "" {
		t.Fatalf("ok %v, missing %q", ok, missing)
	}
	if took := time.Since(start); took > 2*time.Second {
		t.Fatalf("waited %v for a tail that could not be used", took)
	}
	f.mu.Lock()
	close(f.fillGate[tailKey("shop", "orders")])
	f.mu.Unlock()
	k.close()
	k.settle()
}

// An update that started every tail over (the index was rebuilt) and then
// failed keeps no view: they are of what it threw away.
func TestAFailedUpdateAfterARebuildKeepsNoViews(t *testing.T) {
	ctx := context.Background()
	f := &fakeIndex{}
	f.add("orders", "1")
	k := newTailKeeper(f, t.TempDir(), tailBudget{Keys: 3}, time.Minute, time.Hour)
	s := &Server{}
	orders := []views.BaselineTable{keeperTable("orders")}
	s.statementTails(ctx, k, orders, orders)
	k.settle()
	k.followOnce(ctx, time.Minute)
	k.followOnce(ctx, time.Minute)
	if _, _, ok := s.statementTails(ctx, k, orders, orders); !ok {
		t.Fatal("the rig has no tail")
	}
	f.mu.Lock()
	f.events[0].StartPos += 7 // the mark's event is not what it was: rebuilt
	f.changedErr = errors.New("schema_changes unreadable")
	f.mu.Unlock()
	k.followOnce(ctx, time.Minute)
	if v, _ := k.published(orders); len(v) != 0 {
		t.Fatalf("views of the tails a rebuilt index threw away: %v", v)
	}
}
