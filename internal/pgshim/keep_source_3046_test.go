package pgshim

import (
	"context"
	"sync"
	"testing"
	"time"
)

const keepLookup = "SELECT n FROM t WHERE id = 1"

var keepAnswer = costOutcome{"42", "", RouteSource, ReasonCheapPlan}

// #3046: a client's connection to the source is kept between its
// statements, as the MySQL port keeps its own, when the port's pool can take
// it back (Forward.Park); without one it is ended after every answer, as
// before.
func TestKeepSource_3046_oneConnectionForAClientsStatements(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	for _, tc := range []struct {
		name   string
		keep   bool
		opened int32
	}{
		{"kept", true, 1},
		{"without a pool that takes it back", false, 5},
	} {
		t.Run(tc.name, func(t *testing.T) {
			l := &costLogin{src: startFakeSource(t, costPlans)}
			if tc.keep {
				l.keep = &keptSources{}
			}
			c := costPortConn(t, ctx, l)
			for i := range 5 {
				if got := costSimple(ctx, c, l, keepLookup); got != keepAnswer {
					t.Fatalf("statement %d: %+v", i, got)
				}
			}
			if n := l.opened.Load(); n != tc.opened {
				t.Errorf("opened %d connections to the source, want %d", n, tc.opened)
			}
			want := int32(0)
			if tc.keep {
				want = 1
			}
			if n := l.held.Load(); n != want {
				t.Errorf("between statements %d held, want %d", n, want)
			}
			c.Close(ctx)
			waitUntilTest(t, "the slot given back when the client leaves", func() bool { return l.held.Load() == 0 })
			if tc.keep && l.keep.parked() != 0 {
				t.Errorf("%d connections still offered to the pool after the client left", l.keep.parked())
			}
		})
	}
}

// Taken back by the pool, or ended by the source while idle: the next
// statement opens another, and is answered as if nothing happened.
func TestKeepSource_3046_endedWhileIdle(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	src := startFakeSource(t, costPlans)
	l := &costLogin{src: src, keep: &keptSources{}}
	c := costPortConn(t, ctx, l)
	if got := costSimple(ctx, c, l, keepLookup); got != keepAnswer {
		t.Fatal(got)
	}
	if n := l.keep.reclaimAll(); n != 1 {
		t.Fatalf("%d connections offered to the pool, want 1", n)
	}
	if n := l.held.Load(); n != 0 {
		t.Errorf("after the pool took it back %d held", n)
	}
	if got := costSimple(ctx, c, l, keepLookup); got != keepAnswer {
		t.Errorf("after the pool took it back: %+v", got)
	}
	if n := l.opened.Load(); n != 2 {
		t.Errorf("opened %d, want 2", n)
	}
	// The source ends it (idle_session_timeout, a restart).
	src.dropConns()
	waitUntilTest(t, "the source to end its connections", func() bool {
		src.mu.Lock()
		defer src.mu.Unlock()
		return len(src.conns) == 0
	})
	if got := costSimple(ctx, c, l, keepLookup); got != keepAnswer {
		t.Errorf("after the source ended it: %+v", got)
	}
	if n := l.opened.Load(); n != 3 {
		t.Errorf("opened %d, want 3", n)
	}
	if n := l.held.Load(); n != 1 {
		t.Errorf("%d held, want the one kept", n)
	}
	// The source says it ends it, and has not closed it yet.
	src.sayToConns()
	time.Sleep(20 * time.Millisecond)
	if got := costSimple(ctx, c, l, keepLookup); got != keepAnswer {
		t.Errorf("after the source said it ends it: %+v", got)
	}
	if n := l.opened.Load(); n != 4 {
		t.Errorf("opened %d, want 4", n)
	}
}

// What lives on the source is not parked: a transaction block holds the
// connection as before, and the connection is kept once it ends.
func TestKeepSource_3046_aBlock(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	l := &costLogin{src: startFakeSource(t, costPlans), keep: &keptSources{}}
	c := costPortConn(t, ctx, l)
	if _, err := c.Exec(ctx, "BEGIN").ReadAll(); err != nil {
		t.Fatal(err)
	}
	if got := costSimple(ctx, c, l, keepLookup); got.answer != "42" {
		t.Fatalf("in the block: %+v", got)
	}
	if n := l.keep.parked(); n != 0 {
		t.Errorf("in a block %d connections offered to the pool", n)
	}
	if n := l.keep.reclaimAll(); n != 0 {
		t.Errorf("the pool could take %d connections holding a block", n)
	}
	if _, err := c.Exec(ctx, "COMMIT").ReadAll(); err != nil {
		t.Fatal(err)
	}
	if n := l.keep.parked(); n != 1 {
		t.Errorf("after the block %d offered, want 1", n)
	}
	if c.TxStatus() != 'I' {
		t.Errorf("status %c", c.TxStatus())
	}
}

// A connection the host bars while it is kept: nothing is sent on it, and
// it is ended.
func TestKeepSource_3046_barredWhileKept(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	src := startFakeSource(t, costPlans)
	var mu sync.Mutex
	why := ""
	l := &costLogin{src: src, keep: &keptSources{}, barred: func() string { mu.Lock(); defer mu.Unlock(); return why }}
	c := costPortConn(t, ctx, l)
	if got := costSimple(ctx, c, l, keepLookup); got != keepAnswer {
		t.Fatal(got)
	}
	mu.Lock()
	why = "hidden rules"
	mu.Unlock()
	ran := len(src.ran())
	if got := costSimple(ctx, c, l, keepLookup); got.side != RouteCopy {
		t.Errorf("barred: %+v, want the copy", got)
	}
	if d := len(src.ran()) - ran; d != 0 {
		t.Errorf("barred: the source ran %d statements", d)
	}
	waitUntilTest(t, "the barred connection ended", func() bool { return l.held.Load() == 0 })
}

// The pool takes connections back from its own goroutines while the client
// goes on: every answer is the source's, and nothing is used by two at once
// (run with -race).
func TestKeepSource_3046_reclaimedWhileTheClientRuns(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	l := &costLogin{src: startFakeSource(t, costPlans), keep: &keptSources{}}
	c := costPortConn(t, ctx, l)
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
				l.keep.reclaimAll()
				time.Sleep(50 * time.Microsecond)
			}
		}
	}()
	for i := range 200 {
		if got := costSimple(ctx, c, l, keepLookup); got != keepAnswer {
			close(stop)
			<-done
			t.Fatalf("statement %d: %+v", i, got)
		}
	}
	close(stop)
	<-done
	if l.held.Load() > 1 {
		t.Errorf("%d held", l.held.Load())
	}
}

func waitUntilTest(t *testing.T, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !ok() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// A rule saved while the client keeps a connection, then removed: the kept
// connection is ended, and the session reaches the source again afterwards
// (a lost connection of the client's own would end that).
func TestKeepSource_3046_barredThenAllowedAgain(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	src := startFakeSource(t, costPlans)
	var mu sync.Mutex
	why := ""
	l := &costLogin{src: src, keep: &keptSources{}, barred: func() string { mu.Lock(); defer mu.Unlock(); return why }}
	c := costPortConn(t, ctx, l)
	if got := costSimple(ctx, c, l, keepLookup); got != keepAnswer {
		t.Fatal(got)
	}
	mu.Lock()
	why = "hidden rules"
	mu.Unlock()
	if got := costSimple(ctx, c, l, keepLookup); got.side != RouteCopy {
		t.Fatalf("barred: %+v", got)
	}
	mu.Lock()
	why = ""
	mu.Unlock()
	if got := costSimple(ctx, c, l, keepLookup); got != keepAnswer {
		t.Errorf("allowed again: %+v, want the source's answer", got)
	}
}

// A connection kept idle longer than sourceKeepIdle is not used: another is
// opened.
func TestKeepSource_3046_keptTooLong(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	defer func(d time.Duration) { sourceKeepIdle = d }(sourceKeepIdle)
	sourceKeepIdle = 50 * time.Millisecond
	l := &costLogin{src: startFakeSource(t, costPlans), keep: &keptSources{}}
	c := costPortConn(t, ctx, l)
	for range 2 {
		if got := costSimple(ctx, c, l, keepLookup); got != keepAnswer {
			t.Fatal(got)
		}
	}
	if n := l.opened.Load(); n != 1 {
		t.Fatalf("back to back: opened %d, want 1", n)
	}
	time.Sleep(100 * time.Millisecond)
	if got := costSimple(ctx, c, l, keepLookup); got != keepAnswer {
		t.Fatal(got)
	}
	if n := l.opened.Load(); n != 2 {
		t.Errorf("after the keep ran out: opened %d, want 2", n)
	}
	if n := l.held.Load(); n != 1 {
		t.Errorf("%d held, want 1", n)
	}
}

// A client that drops its socket while its connection to the source is
// kept: the connection is taken back from the pool and ended, its slot given
// back.
func TestKeepSource_3046_aClientThatDropsItsSocket(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	l := &costLogin{src: startFakeSource(t, costPlans), keep: &keptSources{}}
	c := costPortConn(t, ctx, l)
	if got := costSimple(ctx, c, l, keepLookup); got != keepAnswer {
		t.Fatal(got)
	}
	if l.keep.parked() != 1 {
		t.Fatal("nothing kept")
	}
	_ = c.Conn().Close() // no Terminate
	waitUntilTest(t, "the kept connection ended", func() bool { return l.held.Load() == 0 && l.keep.parked() == 0 })
}
