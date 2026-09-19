//go:build integration

package reconstruct

import (
	"context"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestResolveSnapshotCut_anEventIndexedBetweenTheTwoReadsIsNeverLost is #1695's
// acceptance test. Capture keeps inserting while a fold resolves its cut, so an
// event whose timestamp is past at can land between the cut's two statements.
// This fold's time filter drops it (Until: at), and the next fold admits only
// start_pos >= cut, so the cut must land at or before that event's start_pos:
// past it, the event is folded into no snapshot, and nothing says so.
//
// The event is inserted through the hook that runs exactly in that gap, on a
// real MySQL, because the loss depends on what each statement can see.
func TestResolveSnapshotCut_anEventIndexedBetweenTheTwoReadsIsNeverLost(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	// A refresh resolves its cut for "now", a moment in the past by the time
	// the statements run; seconds are what event_timestamp holds.
	at := time.Now().UTC().Truncate(time.Second).Add(-30 * time.Second)
	ts := func(d time.Duration) string { return at.Add(d).Format("2006-01-02 15:04:05") }

	// Already indexed, at or before at: this fold's.
	testutil.InsertEvent(t, db, "binlog.000007", 100, 200, ts(-10*time.Second), nil, "shop", "orders", 2, "1", nil, nil, []byte(`{"id":1}`))

	// Indexed in the gap, past at, later in the binlog: the next fold's. A gap
	// between the two positions (200 -> 250) lets the assertion below tell a
	// search that saw this event (cut 250) from one that did not (cut 200).
	const lateStart = 250
	inserted := false
	afterNewestEventForTest = func() {
		testutil.InsertEvent(t, db, "binlog.000007", lateStart, 350, ts(time.Second), nil, "shop", "orders", 2, "2", nil, nil, []byte(`{"id":2}`))
		inserted = true
	}
	t.Cleanup(func() { afterNewestEventForTest = func() {} })

	cut, err := ResolveSnapshotCut(ctx, db, at)
	if err != nil {
		t.Fatalf("ResolveSnapshotCut: %v", err)
	}
	if !inserted {
		t.Fatal("the hook never ran, so this test proved nothing about the gap")
	}
	if cut == nil || cut.File != "binlog.000007" {
		t.Fatalf("cut = %+v, want a cut in binlog.000007", cut)
	}
	if cut.Pos > lateStart {
		t.Fatalf("cut = %d, past the start (%d) of an event indexed past at: this fold drops it by time and the next skips it by position, so it is folded into no snapshot",
			cut.Pos, lateStart)
	}
	// The search runs after the insert, so it must have found the event.
	if cut.Pos != lateStart {
		t.Errorf("cut = %d, want %d: the search did not see an event indexed before it ran", cut.Pos, lateStart)
	}
}
