package consoleapp

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// setCaptureLockTimings sets the lock's waits for one test and restores them
// afterwards.
func setCaptureLockTimings(t *testing.T, poll, beat, limit time.Duration) {
	t.Helper()
	p, b, l := captureLockPoll, captureLockHeartbeat, captureLockUnconfirmedLimit
	captureLockPoll, captureLockHeartbeat, captureLockUnconfirmedLimit = poll, beat, limit
	t.Cleanup(func() { captureLockPoll, captureLockHeartbeat, captureLockUnconfirmedLimit = p, b, l })
}

// shortCaptureLockTimings makes the lock's waits and heartbeat fast enough to
// test.
func shortCaptureLockTimings(t *testing.T) {
	t.Helper()
	setCaptureLockTimings(t, 100*time.Millisecond, 100*time.Millisecond, time.Second)
}

// heartbeatOnly starts the heartbeat of a lock with no session behind it:
// check stands in for the question to the index.
func heartbeatOnly(t *testing.T, check func(context.Context) error) *captureLock {
	t.Helper()
	l := newCaptureLock("test_lock", nil, nil)
	l.checkFn = check
	go l.heartbeat()
	t.Cleanup(l.Release)
	return l
}

func lostWithin(l *captureLock, d time.Duration) bool {
	select {
	case <-l.Lost():
		return true
	case <-time.After(d):
		return false
	}
}

// The limit a holder tolerates without a confirmation has to end before a
// process that took the lock over starts writing, with room for the holder's
// stream to stop; and before a release older than the limit would start
// (during an upgrade or a rollback that one can be the process waiting).
// Checked on the shipped values: no test may have changed them here.
func TestCaptureLockLimitEndsBeforeATakeoverWrites(t *testing.T) {
	if captureLockTakeoverMargin < time.Second {
		t.Errorf("takeover margin = %s, want at least 1s for the previous holder's stream to stop", captureLockTakeoverMargin)
	}
	if got, floor := captureLockTakeoverDelay(), captureLockUnconfirmedLimit+time.Second; got < floor {
		t.Errorf("takeover delay = %s, want at least %s: the limit (%s) plus a second; otherwise the new holder writes while the old one may still be writing",
			got, floor, captureLockUnconfirmedLimit)
	}
	if captureLockUnconfirmedLimit+time.Second > captureLockOldestTakeoverDelay {
		t.Errorf("limit = %s, want at most %s: older releases start writing %s after taking the lock over",
			captureLockUnconfirmedLimit, captureLockOldestTakeoverDelay-time.Second, captureLockOldestTakeoverDelay)
	}
	// A limit no longer than the interval would call every quiet interval a loss.
	if captureLockHeartbeat*2 > captureLockUnconfirmedLimit {
		t.Errorf("heartbeat = %s, want at most half the limit (%s)", captureLockHeartbeat, captureLockUnconfirmedLimit)
	}
}

// One answer that takes most of the limit, between prompt ones: the lock is
// still this session's, and the capture must not restart for it.
func TestCaptureLockHeartbeatOneSlowAnswerIsNotALoss(t *testing.T) {
	setCaptureLockTimings(t, time.Second, 20*time.Millisecond, 600*time.Millisecond)
	var calls atomic.Int32
	l := heartbeatOnly(t, func(context.Context) error {
		if calls.Add(1) == 3 {
			time.Sleep(400 * time.Millisecond) // two thirds of the limit
		}
		return nil
	})
	if lostWithin(l, 1500*time.Millisecond) {
		t.Fatalf("Lost closed after one slow answer (%d questions asked)", calls.Load())
	}
	if n := calls.Load(); n < 10 {
		t.Fatalf("%d questions asked in 1.5s, want the heartbeat to go on after the slow one", n)
	}
}

// Every answer slow, each well past the heartbeat interval: still held, as
// long as each one arrives inside the limit counted from the one before.
func TestCaptureLockHeartbeatSteadySlowAnswersAreNotALoss(t *testing.T) {
	setCaptureLockTimings(t, time.Second, 20*time.Millisecond, 600*time.Millisecond)
	var calls atomic.Int32
	l := heartbeatOnly(t, func(context.Context) error {
		calls.Add(1)
		time.Sleep(150 * time.Millisecond) // a quarter of the limit, 7 intervals
		return nil
	})
	if lostWithin(l, 1500*time.Millisecond) {
		t.Fatalf("Lost closed while every question was answered (%d asked)", calls.Load())
	}
	if n := calls.Load(); n < 5 {
		t.Fatalf("%d questions asked in 1.5s, want one after another", n)
	}
}

// An index that stops answering: lost when the limit runs out, not before,
// and the question left in flight is cut so its connection closes and the
// server frees the lock.
func TestCaptureLockHeartbeatSilenceIsALossAtTheLimit(t *testing.T) {
	setCaptureLockTimings(t, time.Second, 20*time.Millisecond, 400*time.Millisecond)
	cut := make(chan struct{})
	start := time.Now()
	l := heartbeatOnly(t, func(ctx context.Context) error {
		<-ctx.Done()
		close(cut)
		return ctx.Err()
	})
	if lostWithin(l, 300*time.Millisecond) {
		t.Fatalf("Lost closed %s after the lock was taken, before the %s limit", time.Since(start), captureLockUnconfirmedLimit)
	}
	if !lostWithin(l, 2*time.Second) {
		t.Fatal("the index never answered and Lost never closed")
	}
	select {
	case <-cut:
	case <-time.After(2 * time.Second):
		t.Fatal("the unanswered question was never cut: its connection would keep the lock")
	}
	done := make(chan struct{})
	go func() { l.Release(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Release did not return after a loss by silence")
	}
}

// A "yes" that arrives after the limit is too late: the holder already
// stopped being sure, and a process taking over counts on that.
func TestCaptureLockHeartbeatLateYesIsStillALoss(t *testing.T) {
	setCaptureLockTimings(t, time.Second, 20*time.Millisecond, 300*time.Millisecond)
	l := heartbeatOnly(t, func(context.Context) error {
		time.Sleep(700 * time.Millisecond)
		return nil
	})
	if !lostWithin(l, 600*time.Millisecond) {
		t.Fatal("Lost did not close at the limit while a question was still unanswered")
	}
}

// The heartbeat can read a "yes" after the limit has passed with its timer
// not yet served (a process that was not scheduled for a while): the clock
// decides, so the answer does not extend a limit that already ran out. The
// loop cannot be made to read both at once on demand, so the rule is pinned
// where it is written.
func TestCaptureLockAnswerAfterTheLimitIsTooLate(t *testing.T) {
	setCaptureLockTimings(t, time.Second, time.Second, 10*time.Second)
	confirmed := time.Now()
	for _, c := range []struct {
		after time.Duration
		late  bool
	}{
		{0, false},
		{9 * time.Second, false},
		{10 * time.Second, false},
		{10*time.Second + time.Millisecond, true},
		{time.Minute, true},
	} {
		if got := captureLockAnswerTooLate(confirmed, confirmed.Add(c.after)); got != c.late {
			t.Errorf("an answer read %s after the last confirmation: too late = %v, want %v", c.after, got, c.late)
		}
	}
}

// A question that fails means the connection is gone (or the server refused):
// lost at once, never after waiting out the limit.
func TestCaptureLockHeartbeatErrorIsALossAtOnce(t *testing.T) {
	setCaptureLockTimings(t, time.Second, 20*time.Millisecond, 5*time.Second)
	var calls atomic.Int32
	l := heartbeatOnly(t, func(context.Context) error {
		if calls.Add(1) < 3 {
			return nil
		}
		return errors.New("invalid connection")
	})
	if !lostWithin(l, time.Second) {
		t.Fatalf("a failed question did not close Lost within 1s (limit %s): a dead connection must not wait for the limit", captureLockUnconfirmedLimit)
	}
	if n := calls.Load(); n != 3 {
		t.Errorf("%d questions asked, want the heartbeat to stop at the failed third", n)
	}
}

// A clean stop while a question is in flight is not a loss: Release returns
// without waiting for the answer and Lost stays open.
func TestCaptureLockReleaseDuringASlowQuestionIsNotALoss(t *testing.T) {
	setCaptureLockTimings(t, time.Second, 20*time.Millisecond, 5*time.Second)
	asked, cut := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	l := heartbeatOnly(t, func(ctx context.Context) error {
		if calls.Add(1) == 1 {
			close(asked)
		}
		<-ctx.Done()
		close(cut)
		return ctx.Err()
	})
	select {
	case <-asked:
	case <-time.After(2 * time.Second):
		t.Fatal("no question was asked")
	}
	done := make(chan struct{})
	go func() { l.Release(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Release waited for a question that was not answering")
	}
	select {
	case <-cut:
	case <-time.After(time.Second):
		t.Fatal("Release left the question in flight")
	}
	if lostWithin(l, 200*time.Millisecond) {
		t.Fatal("a clean stop closed Lost")
	}
	if !l.abandoned {
		t.Error("abandoned = false after cutting a question: Release would ask a closed connection to free the lock")
	}
}
