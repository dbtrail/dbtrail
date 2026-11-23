package pgstreamrun

import (
	"context"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/indexer"
)

// TestStreamLoopPGHooks_idleCheckpointLiveness proves the load-bearing liveness
// invariant behind console-driven PG capture (#1020): a connected-but-idle
// source (nothing committed, lastCommitLSN==0) still fires OnCheckpoint on the
// ticker, so a supervised job flips out of "pending" even before the first row.
// checkpoint() early-returns at lastCommitLSN==0, which is exactly why the hook
// fires on the ticker branch and not inside checkpoint(). No live DB: an empty
// batch never flushes, so the nil-db indexer is never touched.
func TestStreamLoopPGHooks_idleCheckpointLiveness(t *testing.T) {
	var checkpoints, indexed int64
	hooks := &Hooks{
		OnCheckpoint: func() { atomic.AddInt64(&checkpoints, 1) },
		OnIndexed:    func(n int64) { atomic.AddInt64(&indexed, n) },
	}

	ctx, cancel := context.WithCancel(context.Background())
	events := make(chan event.Event) // stays open, never receives — a silent, connected source
	idx := indexer.New(nil, 1)       // BatchSize() only reads a field; never flushes here
	state := &pgStreamState{serverID: 1}

	done := make(chan error, 1)
	go func() {
		done <- streamLoopPG(ctx, events, idx, nil /*indexDB*/, nil /*cap*/, 5*time.Millisecond, nil /*probe*/, time.Hour, state, slog.New(slog.DiscardHandler), hooks)
	}()

	time.Sleep(60 * time.Millisecond) // ~12 ticker ticks
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("streamLoopPG did not return after cancel")
	}

	if atomic.LoadInt64(&checkpoints) == 0 {
		t.Error("OnCheckpoint never fired for an idle source — a supervised PG job would be stuck pending forever")
	}
	if got := atomic.LoadInt64(&indexed); got != 0 {
		t.Errorf("OnIndexed fired (%d) with no rows streamed", got)
	}
}

// The stream loop's tick says nothing until replication has started (#3178).
func TestHooksAfterStart(t *testing.T) {
	var ticks atomic.Int32
	loop, started := hooksAfterStart(&Hooks{OnCheckpoint: func() { ticks.Add(1) }})
	loop.OnCheckpoint()
	if n := ticks.Load(); n != 0 {
		t.Fatalf("%d ticks before replication started", n)
	}
	started()
	loop.OnCheckpoint()
	if n := ticks.Load(); n != 1 {
		t.Fatalf("%d ticks after replication started, want 1", n)
	}
	if h, f := hooksAfterStart(nil); h != nil || f == nil {
		t.Fatalf("nil hooks: %v", h)
	}
}
