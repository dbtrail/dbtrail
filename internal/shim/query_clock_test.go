package shim

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
)

// A statement's time cap (Config.QueryTimeout) is on what it runs. While it
// waits its turn for a worker of the copy the cap does not run, for any of
// the contexts the connection has in hand: a routed statement holds two (the
// decision's and the copy's), and the first is the one a forward to the
// source would be given afterwards.
func TestQueryContext_theWaitForTheCopyIsNotCounted(t *testing.T) {
	h := NewHandlerWithConfig(nil, Config{QueryTimeout: 120 * time.Millisecond}, nil)
	h.BindConnContext(context.Background())
	decision, cancelDecision := h.queryContext()
	defer cancelDecision()
	onCopy, cancelCopy := h.queryContext()
	defer cancelCopy()

	r := sqlsandbox.New(sqlsandbox.Config{MaxInFlight: 1, MaxWait: sqlsandbox.NoMaxWait})
	held, err := r.Reserve(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	// Four times the cap in the line, then the turn.
	time.AfterFunc(480*time.Millisecond, held.Release)
	slot, err := r.Reserve(onCopy, "")
	if err != nil {
		t.Fatalf("the statement that waited got %v, want its turn: the wait was counted against its %v cap", err, h.cfg.QueryTimeout)
	}
	defer slot.Release()
	if decision.Err() != nil || onCopy.Err() != nil {
		t.Fatalf("a cap ran out during the wait: decision %v, copy %v", decision.Err(), onCopy.Err())
	}
	// Out of the line, the cap runs again and is the cap, not a longer one.
	select {
	case <-onCopy.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("the cap never ran out once the statement had its turn")
	}
	if !errors.Is(onCopy.Err(), context.DeadlineExceeded) {
		t.Errorf("Err() = %v, want DeadlineExceeded", onCopy.Err())
	}
}

// The client leaving still ends a statement that waits, cap held or not.
func TestQueryContext_aClientThatLeavesEndsTheWait(t *testing.T) {
	h := NewHandlerWithConfig(nil, Config{QueryTimeout: time.Hour}, nil)
	conn, leave := context.WithCancel(context.Background())
	h.BindConnContext(conn)
	ctx, cancel := h.queryContext()
	defer cancel()
	r := sqlsandbox.New(sqlsandbox.Config{MaxInFlight: 1, MaxWait: sqlsandbox.NoMaxWait})
	held, _ := r.Reserve(context.Background(), "")
	defer held.Release()
	time.AfterFunc(80*time.Millisecond, leave)
	if _, err := r.Reserve(ctx, ""); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want the connection's Canceled", err)
	}
}
