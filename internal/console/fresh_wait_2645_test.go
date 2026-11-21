package console

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/views"
)

// #2645: fresh mode's wait for the source.

func TestFreshWaitSetting_2645(t *testing.T) {
	for _, tc := range []struct {
		raw  string
		want time.Duration
		warn bool
	}{
		{"", 0, false},
		{"off", 0, false},
		{" OFF ", 0, false},
		{"0", 0, false},
		{"no", 0, false},
		{"on", freshWaitDefault, false},
		{"ON", freshWaitDefault, false},
		{"1", freshWaitDefault, false},
		{"true", freshWaitDefault, false},
		{"500ms", 500 * time.Millisecond, false},
		{" 2s ", 2 * time.Second, false},
		{"1ms", time.Millisecond, false},
		{"10s", 10 * time.Second, false},
		{"0ms", 0, true},
		{"500us", 0, true},
		{"11s", 0, true},
		{"-200ms", 0, true},
		{"200", 0, true},
		{"soon", 0, true},
	} {
		got, warn := freshWaitFrom(tc.raw)
		if got != tc.want || (warn != "") != tc.warn {
			t.Errorf("freshWaitFrom(%q) = %v, %q; want %v, warning %v", tc.raw, got, warn, tc.want, tc.warn)
		}
	}
}

func TestPositionReached_2645(t *testing.T) {
	pos := func(f string, p uint64) SourcePosition { return SourcePosition{File: f, Pos: p} }
	for _, tc := range []struct {
		name              string
		want, saved       SourcePosition
		reached, compares bool
	}{
		{"same file, saved at the position", pos("binlog.000007", 900), pos("binlog.000007", 900), true, true},
		{"same file, saved past it", pos("binlog.000007", 900), pos("binlog.000007", 1200), true, true},
		{"same file, saved short of it", pos("binlog.000007", 900), pos("binlog.000007", 869), false, true},
		{"rotated: saved in the next file", pos("binlog.000007", 900), pos("binlog.000008", 4), true, true},
		{"rotated: the source in the next file", pos("binlog.000008", 157), pos("binlog.000007", 9000), false, true},
		{"sequence past six digits", pos("binlog.999999", 900), pos("binlog.1000000", 4), true, true},
		{"sequence past six digits, behind", pos("binlog.1000000", 4), pos("binlog.999999", 900), false, true},
		{"another binary log name", pos("mysql-bin.000007", 900), pos("binlog.000007", 900), false, false},
		{"no sequence", pos("binlog", 900), pos("binlog", 900), false, false},
		{"nothing saved in the file", pos("binlog.000007", 900), pos("", 0), false, false},
	} {
		r, c := positionReached(tc.want, tc.saved)
		if r != tc.reached || c != tc.compares {
			t.Errorf("%s: positionReached = %v, %v; want %v, %v", tc.name, r, c, tc.reached, tc.compares)
		}
	}
}

func TestWakeFollowerIsOneWakeForMany_2645(t *testing.T) {
	k := newTailKeeper(&fakeIndex{}, t.TempDir(), tailBudget{Keys: 3}, time.Minute, time.Hour)
	for range 100 {
		k.wakeFollower() // never blocks
	}
	if n := len(k.pub.wake); n != 1 {
		t.Fatalf("100 wakes left %d pending, want 1", n)
	}
}

// fakePositions is a source whose position the test sets.
type fakePositions struct {
	mu    sync.Mutex
	pos   SourcePosition
	err   error
	calls atomic.Int64
	gate  chan struct{} // when set, a read waits for it
}

func (f *fakePositions) SourcePosition(ctx context.Context, _ ServerEntry) (SourcePosition, error) {
	f.calls.Add(1)
	f.mu.Lock()
	gate := f.gate
	f.mu.Unlock()
	if gate != nil {
		select {
		case <-gate:
		case <-ctx.Done():
			return SourcePosition{}, ctx.Err()
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.pos, f.err
}

// waitRig is a server with one registered source, a keeper over a fake
// index stepped by the test, a fake source and a saved position the test
// moves.
type waitRig struct {
	s       *Server
	id      string
	k       *tailKeeper
	src     *fakePositions
	read    []views.BaselineTable
	tick    func(time.Duration)
	savedMu sync.Mutex
	saved   SourcePosition
	none    bool
	err     error
}

func newWaitRig(t *testing.T, limit time.Duration) *waitRig {
	t.Helper()
	reg, err := LoadRegistry(t.TempDir() + "/console-servers.yaml")
	if err != nil {
		t.Fatal(err)
	}
	e, err := reg.Add(ServerEntry{Name: "prod", DSN: "u:p@tcp(idx:3306)/i", SourceDSN: "repl:pw@tcp(src:3306)/"})
	if err != nil {
		t.Fatal(err)
	}
	r := &waitRig{id: e.ID, src: &fakePositions{pos: SourcePosition{File: "binlog.000007", Pos: 900}},
		saved: SourcePosition{File: "binlog.000007", Pos: 900}}
	r.s = &Server{cm: &connManager{reg: reg}, sourcePos: r.src, freshWait: limit}
	r.s.freshSavedHook = func(context.Context) (SourcePosition, bool, error) {
		r.savedMu.Lock()
		defer r.savedMu.Unlock()
		return r.saved, r.none, r.err
	}
	var clockMu sync.Mutex
	clock := time.Date(2026, 11, 16, 13, 0, 0, 0, time.UTC)
	f := &fakeIndex{}
	f.add("orders", "1")
	r.k = newTailKeeper(f, t.TempDir(), tailBudget{Keys: 3}, time.Minute, time.Hour)
	r.k.now = func() time.Time { clockMu.Lock(); defer clockMu.Unlock(); return clock }
	r.tick = func(d time.Duration) { clockMu.Lock(); clock = clock.Add(d); clockMu.Unlock() }
	r.read = []views.BaselineTable{keeperTable("orders")}
	return r
}

// publish brings the table's tail up and publishes it, as the follower does.
func (r *waitRig) publish(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	r.s.statementTails(ctx, r.k, r.read, r.read)
	r.k.settle()
	// The build's install woke the follower (#3088); the test is the
	// follower, and this update is the one that wake asked for. Left
	// pending, a case's "<-r.k.pub.wake" took it in place of its
	// statement's own wake, ran its update before the statement looked, and
	// the statement's wake was found pending at the end (#3180).
	select {
	case <-r.k.pub.wake:
	default:
		t.Fatal("the build's install did not wake the follower")
	}
	r.k.followOnce(ctx, time.Minute)
	r.k.settle()
	r.k.followOnce(ctx, time.Minute)
	if v, _ := r.k.published(r.read); len(v) != 1 {
		t.Fatalf("the rig's tail is not published: %v", v)
	}
	if n := len(r.k.pub.wake); n != 0 {
		t.Fatalf("the rig left %d wakes pending: a case would take one for its statement's", n)
	}
	// Time goes on after the update: a position first seen from here on is
	// after the update began, as on a real clock.
	r.tick(time.Millisecond)
}

func (r *waitRig) setSaved(p SourcePosition) {
	r.savedMu.Lock()
	r.saved = p
	r.savedMu.Unlock()
}

func (r *waitRig) wait(ctx context.Context) time.Duration {
	start := time.Now()
	r.s.waitForSource(ctx, r.id, nil, r.k, r.read)(true)
	return time.Since(start)
}

func (r *waitRig) numbers() RouterFreshWait {
	return r.s.freshWaits.of(r.id).numbers(r.s.freshWait, time.Now())
}

func TestWaitForSource_2645(t *testing.T) {
	ctx := context.Background()

	t.Run("off: nothing waits and nothing is counted", func(t *testing.T) {
		r := newWaitRig(t, 0)
		r.publish(t)
		r.wait(ctx)
		if r.src.calls.Load() != 0 {
			t.Fatal("the source was read with the wait off")
		}
		if n := r.numbers(); n.Statements != 0 {
			t.Fatalf("counted %+v with the wait off", n)
		}
	})

	t.Run("a table with no tail does not wait", func(t *testing.T) {
		r := newWaitRig(t, time.Second)
		if took := r.wait(ctx); took > 100*time.Millisecond {
			t.Fatalf("a statement over a table with no tail waited %v", took)
		}
		if n := r.numbers(); n.Skipped[freshSkipNoTail] != 1 || r.src.calls.Load() != 0 {
			t.Fatalf("got %+v with %d source reads, want one no_tail skip and none", n, r.src.calls.Load())
		}
	})

	t.Run("an update that began after capture reached the position ends the wait", func(t *testing.T) {
		r := newWaitRig(t, 5*time.Second)
		r.publish(t)
		r.src.pos = SourcePosition{File: "binlog.000007", Pos: 1500}
		r.setSaved(SourcePosition{File: "binlog.000007", Pos: 900})
		done := make(chan time.Duration, 1)
		go func() { done <- r.wait(ctx) }()
		time.Sleep(60 * time.Millisecond)
		select {
		case <-done:
			t.Fatal("the statement stopped waiting before capture saved the source's position")
		default:
		}
		r.setSaved(SourcePosition{File: "binlog.000007", Pos: 1500})
		// The statement wakes the follower once capture is there; the
		// test is the follower: it takes the wake and runs an update.
		select {
		case <-r.k.pub.wake:
		case <-time.After(5 * time.Second):
			t.Fatal("the follower was not woken")
		}
		r.tick(time.Millisecond)
		r.k.followOnce(ctx, time.Minute)
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("the statement did not end after the update")
		}
		if n := r.numbers(); n.Reached != 1 || n.Statements != 1 || n.MedianWaitMS == nil {
			t.Fatalf("got %+v, want one reached with a median", n)
		}
	})

	t.Run("a quiet source: the tails published after the position was first seen need no wait", func(t *testing.T) {
		r := newWaitRig(t, 5*time.Second)
		r.publish(t)
		// First statement: capture's position is first seen now, after the
		// last update began, so it waits for one more.
		done := make(chan struct{})
		go func() { r.wait(ctx); close(done) }()
		<-r.k.pub.wake
		r.tick(time.Millisecond)
		r.k.followOnce(ctx, time.Minute)
		<-done
		// The position does not move: the next statements need nothing.
		time.Sleep(2 * freshWaitPoll)
		for range 5 {
			if took := r.wait(ctx); took > 100*time.Millisecond {
				t.Fatalf("a statement on a quiet source waited %v", took)
			}
		}
		if len(r.k.pub.wake) != 0 {
			t.Fatal("a statement on a quiet source woke the follower")
		}
		if n := r.numbers(); n.Reached != 6 {
			t.Fatalf("got %+v, want 6 reached", n)
		}
	})

	t.Run("rotation between the read and capture reaching it", func(t *testing.T) {
		r := newWaitRig(t, 5*time.Second)
		r.publish(t)
		r.src.pos = SourcePosition{File: "binlog.000007", Pos: 900}
		r.setSaved(SourcePosition{File: "binlog.000007", Pos: 500})
		done := make(chan struct{})
		go func() { r.wait(ctx); close(done) }()
		time.Sleep(30 * time.Millisecond)
		r.setSaved(SourcePosition{File: "binlog.000008", Pos: 4})
		<-r.k.pub.wake
		r.tick(time.Millisecond)
		r.k.followOnce(ctx, time.Minute)
		<-done
		if n := r.numbers(); n.Reached != 1 {
			t.Fatalf("got %+v, want one reached", n)
		}
	})

	t.Run("past the limit the statement answers with what is ready", func(t *testing.T) {
		r := newWaitRig(t, 80*time.Millisecond)
		r.publish(t)
		r.src.pos = SourcePosition{File: "binlog.000007", Pos: 1500}
		r.setSaved(SourcePosition{File: "binlog.000007", Pos: 900})
		took := r.wait(ctx)
		if took < 80*time.Millisecond || took > time.Second {
			t.Fatalf("waited %v with a limit of 80ms", took)
		}
		if n := r.numbers(); n.RanOut != 1 || n.MedianWaitMS == nil || *n.MedianWaitMS < 80 {
			t.Fatalf("got %+v, want one ran_out of about 80 ms", n)
		}
	})

	t.Run("a follower update longer than the limit: ran out", func(t *testing.T) {
		r := newWaitRig(t, 80*time.Millisecond)
		r.publish(t)
		r.src.pos = SourcePosition{File: "binlog.000007", Pos: 1500}
		r.setSaved(SourcePosition{File: "binlog.000007", Pos: 1500})
		r.wait(ctx) // nobody runs the update
		if n := r.numbers(); n.RanOut != 1 {
			t.Fatalf("got %+v, want one ran_out", n)
		}
	})

	t.Run("capture known to be behind: no wait", func(t *testing.T) {
		r := newWaitRig(t, 5*time.Second)
		r.publish(t)
		r.setSaved(SourcePosition{File: "binlog.000007", Pos: 900})
		r.src.pos = SourcePosition{File: "binlog.000007", Pos: 5000}
		w := r.s.freshWaits.of(r.id)
		w.mu.Lock()
		w.samples = append(w.samples, positionSample{pos: SourcePosition{File: "binlog.000007", Pos: 3000}, at: time.Now().Add(-3 * time.Second)})
		w.mu.Unlock()
		if took := r.wait(ctx); took > 200*time.Millisecond {
			t.Fatalf("waited %v behind a capture seconds late", took)
		}
		if n := r.numbers(); n.Skipped[freshSkipBehind] != 1 {
			t.Fatalf("got %+v, want one capture_behind skip", n)
		}
	})

	t.Run("a refused read: no wait, said once, not asked again for a while", func(t *testing.T) {
		r := newWaitRig(t, 5*time.Second)
		r.publish(t)
		r.src.err = fmt.Errorf("%w: Error 1227: Access denied; you need the REPLICATION CLIENT privilege", ErrSourcePositionRefused)
		r.wait(ctx)
		r.wait(ctx)
		r.wait(ctx)
		if c := r.src.calls.Load(); c != 1 {
			t.Fatalf("the source was asked %d times after it refused, want 1", c)
		}
		n := r.numbers()
		if n.Skipped[freshSkipRefused] != 3 || n.PositionRefused == "" {
			t.Fatalf("got %+v, want three position_refused skips and the reason", n)
		}
	})

	t.Run("a read that fails otherwise: no wait, the detail kept", func(t *testing.T) {
		r := newWaitRig(t, 5*time.Second)
		r.publish(t)
		r.src.err = errors.New("connection refused")
		r.wait(ctx)
		if n := r.numbers(); n.Skipped[freshSkipUnread] != 1 || n.LastSkipDetail == "" {
			t.Fatalf("got %+v, want one position_unread with its detail", n)
		}
	})

	t.Run("another binary log (the source was replaced): no wait", func(t *testing.T) {
		r := newWaitRig(t, 5*time.Second)
		r.publish(t)
		r.src.pos = SourcePosition{File: "mysql-bin.000001", Pos: 157}
		r.wait(ctx)
		if n := r.numbers(); n.Skipped[freshSkipOtherLog] != 1 {
			t.Fatalf("got %+v, want one other_binary_log", n)
		}
	})

	t.Run("no capture position saved: no wait", func(t *testing.T) {
		r := newWaitRig(t, 5*time.Second)
		r.publish(t)
		r.savedMu.Lock()
		r.none = true
		r.savedMu.Unlock()
		r.wait(ctx)
		if n := r.numbers(); n.Skipped[freshSkipNoCapture] != 1 {
			t.Fatalf("got %+v, want one no_capture_position", n)
		}
	})

	t.Run("the client leaves while it waits", func(t *testing.T) {
		r := newWaitRig(t, 10*time.Second)
		r.publish(t)
		r.src.pos = SourcePosition{File: "binlog.000007", Pos: 1500}
		cctx, cancel := context.WithCancel(ctx)
		go func() { time.Sleep(50 * time.Millisecond); cancel() }()
		if took := r.wait(cctx); took > 2*time.Second {
			t.Fatalf("the wait went on %v after the client left", took)
		}
		if n := r.numbers(); n.Cancelled != 1 || n.RanOut != 0 {
			t.Fatalf("got %+v, want one cancelled", n)
		}
	})

	t.Run("a burst of statements shares one read of the source", func(t *testing.T) {
		r := newWaitRig(t, 5*time.Second)
		r.publish(t)
		gate := make(chan struct{})
		r.src.mu.Lock()
		r.src.gate = gate
		r.src.mu.Unlock()
		var wg sync.WaitGroup
		for range 20 {
			wg.Add(1)
			go func() { defer wg.Done(); r.wait(ctx) }()
		}
		time.Sleep(50 * time.Millisecond)
		close(gate)
		<-r.k.pub.wake
		r.tick(time.Millisecond)
		r.k.followOnce(ctx, time.Minute)
		wg.Wait()
		// The first read was sent before the other 19 arrived: they share
		// the next one.
		if c := r.src.calls.Load(); c > 2 {
			t.Fatalf("20 statements at once read the source %d times, want at most 2", c)
		}
		if n := r.numbers(); n.Reached != 20 {
			t.Fatalf("got %+v, want 20 reached", n)
		}
	})

	t.Run("a read sent before the statement arrived is not joined", func(t *testing.T) {
		r := newWaitRig(t, 5*time.Second)
		r.publish(t)
		gate := make(chan struct{})
		r.src.mu.Lock()
		r.src.gate = gate
		r.src.mu.Unlock()
		w := r.s.freshWaits.of(r.id)
		got := make(chan SourcePosition, 2)
		go func() { p, _ := w.sourcePosition(ctx, r.src, ServerEntry{}); got <- p }()
		for r.src.calls.Load() == 0 {
			time.Sleep(time.Millisecond)
		}
		// The first read is on its way to the source, maybe answered
		// already: the client's write after it is not in it.
		r.src.mu.Lock()
		r.src.pos = SourcePosition{File: "binlog.000007", Pos: 2000}
		r.src.mu.Unlock()
		go func() { p, _ := w.sourcePosition(ctx, r.src, ServerEntry{}); got <- p }()
		time.Sleep(30 * time.Millisecond)
		close(gate)
		a, b := <-got, <-got
		if c := r.src.calls.Load(); c != 2 {
			t.Fatalf("%d reads, want 2: the second statement joined a read sent before it arrived", c)
		}
		if a.Pos != 2000 || b.Pos != 2000 {
			// Both read after the change here (the fake answers at the
			// gate); what matters is the second read.
			t.Logf("positions %d %d", a.Pos, b.Pos)
		}
	})

	t.Run("capture's position of another server: no wait", func(t *testing.T) {
		r := newWaitRig(t, 5*time.Second)
		r.publish(t)
		r.src.pos = SourcePosition{File: "binlog.000007", Pos: 900, Identity: "3f2a1c88-0000-0000-0000-00000000000a"}
		r.setSaved(SourcePosition{File: "binlog.000500", Pos: 900, Identity: "3f2a1c88-0000-0000-0000-00000000000b"})
		r.wait(ctx)
		if n := r.numbers(); n.Skipped[freshSkipOtherSource] != 1 || n.Reached != 0 {
			t.Fatalf("got %+v, want one other_source", n)
		}
	})

	t.Run("reached but the statement read no tails: not counted as reached", func(t *testing.T) {
		r := newWaitRig(t, 5*time.Second)
		r.publish(t)
		done := make(chan func(bool), 1)
		go func() { done <- r.s.waitForSource(ctx, r.id, nil, r.k, r.read) }()
		<-r.k.pub.wake
		r.tick(time.Millisecond)
		r.k.followOnce(ctx, time.Minute)
		(<-done)(false)
		if n := r.numbers(); n.Reached != 0 || n.Skipped[freshSkipNotCurrent] != 1 {
			t.Fatalf("got %+v, want one tails_not_current", n)
		}
	})

	t.Run("a server with no source connection does not wait", func(t *testing.T) {
		r := newWaitRig(t, 5*time.Second)
		r.publish(t)
		r.s.waitForSource(ctx, "unknown-id", nil, r.k, r.read)(true)
		if n := r.s.freshWaits.of("unknown-id").numbers(time.Second, time.Now()); n.Skipped[freshSkipNoSource] != 1 {
			t.Fatalf("got %+v, want one no_source", n)
		}
	})
}

// The counts reach /status only while the wait is on.
func TestRouterNumbersFreshWait_2645(t *testing.T) {
	r := newWaitRig(t, 200*time.Millisecond)
	r.s.routing = newRoutingStats(time.Now(), nil)
	r.publish(t)
	r.wait(context.Background()) // ran out: no update runs
	n, ok := r.s.RouterNumbers().Servers[r.id]
	if !ok || n.FreshWait == nil || n.FreshWait.LimitMS != 200 || n.FreshWait.Statements != 1 {
		t.Fatalf("status for the server: %+v (fresh_wait %+v)", n, n.FreshWait)
	}
	r.s.freshWait = 0
	if n := r.s.RouterNumbers().Servers[r.id]; n.FreshWait != nil {
		t.Fatalf("fresh_wait %+v with the wait off", n.FreshWait)
	}
}
