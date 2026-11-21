package consoleapp

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// setCaptureLockTimings sets the lock's waits for one test and restores them
// afterwards.
func setCaptureLockTimings(t *testing.T, poll, beat, limit time.Duration) {
	t.Helper()
	p, b, l, w := captureLockPoll, captureLockHeartbeat, captureLockUnconfirmedLimit, captureLockSlowWarnEvery
	captureLockPoll, captureLockHeartbeat, captureLockUnconfirmedLimit = poll, beat, limit
	t.Cleanup(func() {
		captureLockPoll, captureLockHeartbeat, captureLockUnconfirmedLimit, captureLockSlowWarnEvery = p, b, l, w
	})
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
	return heartbeatTakenAt(t, time.Now(), check)
}

// heartbeatTakenAt is heartbeatOnly for a lock whose GET_LOCK was sent at
// taken.
func heartbeatTakenAt(t *testing.T, taken time.Time, check func(context.Context) error) *captureLock {
	t.Helper()
	l := newCaptureLock("test_lock", nil, nil, taken)
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
	// The documentation's figures come from these two: a dead connection
	// noticed within about a second, one slow answer tolerated for the limit
	// less one heartbeat (9 s). Changing either means rereading
	// docs/console.md and docs/deployment.md.
	if captureLockHeartbeat != time.Second {
		t.Errorf("heartbeat = %s, want 1s", captureLockHeartbeat)
	}
	if captureLockUnconfirmedLimit != 10*time.Second {
		t.Errorf("limit = %s, want 10s", captureLockUnconfirmedLimit)
	}
	// A limit no longer than the interval would call every quiet interval a loss.
	if captureLockHeartbeat*2 > captureLockUnconfirmedLimit {
		t.Errorf("heartbeat = %s, want at most half the limit (%s)", captureLockHeartbeat, captureLockUnconfirmedLimit)
	}
}

// The first limit starts when the GET_LOCK was sent, not when the heartbeat
// started: a lock that took a while to be granted has that much less before
// its first confirmation is due.
func TestCaptureLockFirstLimitStartsWhenTheLockWasAskedFor(t *testing.T) {
	setCaptureLockTimings(t, time.Second, 20*time.Millisecond, 2*time.Second)
	silent := func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	}
	// Asked for 1.7 s ago: 300 ms of the limit are left, not 2 s.
	l := heartbeatTakenAt(t, time.Now().Add(-1700*time.Millisecond), silent)
	if !lostWithin(l, time.Second) {
		t.Fatal("a lock asked for 1.7s ago with a 2s limit was not lost within 1s of silence: the limit started at the heartbeat, not at the GET_LOCK")
	}
	// A prompt answer moves the limit on like any other.
	l = heartbeatTakenAt(t, time.Now().Add(-1700*time.Millisecond), func(context.Context) error { return nil })
	if lostWithin(l, time.Second) {
		t.Fatal("a lock asked for 1.7s ago was lost although its questions were answered")
	}
}

// An index slow enough to matter leaves a line in the log while the lock is
// kept, and a slow spell does not fill the log with it.
func TestCaptureLockSlowAnswerWarnsOncePerInterval(t *testing.T) {
	slowEveryFourth := func() func(context.Context) error {
		var calls atomic.Int32
		return func(context.Context) error {
			if calls.Add(1)%4 == 0 {
				time.Sleep(400 * time.Millisecond) // over half the 600ms limit
			}
			return nil
		}
	}
	const line = "the index was slow to confirm the capture lock"
	for _, c := range []struct {
		name  string
		every time.Duration
		ok    func(n int) bool
		want  string
	}{
		{"at most one a minute", time.Minute, func(n int) bool { return n == 1 }, "exactly 1"},
		{"each one when the interval is zero", 0, func(n int) bool { return n >= 2 }, "at least 2"},
	} {
		t.Run(c.name, func(t *testing.T) {
			setCaptureLockTimings(t, time.Second, 20*time.Millisecond, 600*time.Millisecond)
			captureLockSlowWarnEvery = c.every
			logs := captureSlogFor(t)
			l := heartbeatOnly(t, slowEveryFourth())
			if lostWithin(l, 1800*time.Millisecond) {
				t.Fatalf("Lost closed; log:\n%s", logs.String())
			}
			l.Release()
			out := logs.String()
			if n := strings.Count(out, line); !c.ok(n) {
				t.Fatalf("%d slow-answer warnings, want %s; log:\n%s", n, c.want, out)
			}
			for _, field := range []string{"level=WARN", "answer_took=", "left_before_the_limit=", "limit=600ms"} {
				if !strings.Contains(out, field) {
					t.Errorf("the warning lacks %q; log:\n%s", field, out)
				}
			}
		})
	}
}

// Prompt answers leave nothing in the log.
func TestCaptureLockPromptAnswersDoNotWarn(t *testing.T) {
	setCaptureLockTimings(t, time.Second, 20*time.Millisecond, 600*time.Millisecond)
	captureLockSlowWarnEvery = 0
	logs := captureSlogFor(t)
	l := heartbeatOnly(t, func(context.Context) error {
		time.Sleep(100 * time.Millisecond) // five intervals, a sixth of the limit
		return nil
	})
	if lostWithin(l, 800*time.Millisecond) {
		t.Fatal("Lost closed")
	}
	l.Release()
	if out := logs.String(); strings.Contains(out, "slow to confirm") || strings.Contains(out, "level=WARN") {
		t.Fatalf("a held lock with answers under half the limit warned:\n%s", out)
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
	logs := captureSlogFor(t)
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
	// The reason says what was measured: since when, and how long the cut
	// question had waited.
	out := logs.String()
	for _, part := range []string{"the last question the index answered was sent ", " ago, past the limit of 400ms", "the question cut had been waiting for its answer for "} {
		if !strings.Contains(out, part) {
			t.Errorf("the loss does not say %q; log:\n%s", part, out)
		}
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

// The limit does not wait for a question in flight, even one that would
// come back "yes" later: the holder stops when the limit runs out, and a
// process taking over counts on that.
func TestCaptureLockHeartbeatLosesAtTheLimitWithAQuestionInFlight(t *testing.T) {
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
	logs := captureSlogFor(t)
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
	if out := logs.String(); strings.Count(out, "the connection was closed without releasing the lock") != 1 || !strings.Contains(out, "level=INFO") {
		t.Errorf("want one INFO line saying the lock was not released explicitly; log:\n%s", out)
	}
}
