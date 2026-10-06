//go:build integration

package streamrun

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/observe"
	"github.com/dbtrail/dbtrail/internal/parser"
)

// A stream cancelled with ErrStopWithoutFlush writes nothing more: not the
// events it holds, not its position. A plain cancellation (a shutdown) still
// writes both, as before. The difference is the whole point of #2105's lock:
// a process that lost it must not write beside the one that has it now.
func TestStreamLoop_stopWithoutFlushWritesNothing(t *testing.T) {
	for _, tc := range []struct {
		name      string
		cause     error
		wantRows  int
		wantState int
		wantErr   error
	}{
		{"shutdown flushes", nil, 3, 1, nil},
		{"lost lock writes nothing", ErrStopWithoutFlush, 0, 0, ErrStopWithoutFlush},
	} {
		t.Run(tc.name, func(t *testing.T) {
			idx, db := lagTestIndex(t) // batch size 10: three events stay pending
			events := make(chan parser.Event, 4)
			for _, ev := range lagEvents(3, time.Second, 10*time.Second) {
				events <- ev
			}
			ctx, cancel := context.WithCancelCause(context.Background())
			done := make(chan error, 1)
			go func() {
				done <- streamLoop(ctx, events, idx, db, time.Hour,
					&streamState{mode: "position", serverID: 1}, observe.ForSource("stopflush-"+tc.name), nil)
			}()
			for len(events) > 0 { // consumed into the pending batch
				time.Sleep(10 * time.Millisecond)
			}
			time.Sleep(100 * time.Millisecond)
			cancel(tc.cause)
			var err error
			select {
			case err = <-done:
			case <-time.After(10 * time.Second):
				t.Fatal("streamLoop did not return")
			}
			if !errors.Is(err, tc.wantErr) && !(tc.wantErr == nil && err == nil) {
				t.Fatalf("streamLoop: %v, want %v", err, tc.wantErr)
			}
			var rows, state int
			if err := db.QueryRow("SELECT COUNT(*) FROM binlog_events").Scan(&rows); err != nil {
				t.Fatal(err)
			}
			if err := db.QueryRow("SELECT COUNT(*) FROM stream_state").Scan(&state); err != nil {
				t.Fatal(err)
			}
			if rows != tc.wantRows || state != tc.wantState {
				t.Fatalf("after the stop: %d events, %d checkpoint rows; want %d and %d", rows, state, tc.wantRows, tc.wantState)
			}
		})
	}
}
