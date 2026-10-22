package oracle

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestPosition_before(t *testing.T) {
	cases := []struct {
		a, b   Position
		before bool
	}{
		{Position{"binlog.000003", 100}, Position{"binlog.000003", 100}, false},
		{Position{"binlog.000003", 99}, Position{"binlog.000003", 100}, true},
		{Position{"binlog.000003", 101}, Position{"binlog.000003", 100}, false},
		{Position{"binlog.000002", 9999}, Position{"binlog.000003", 4}, true},
		{Position{"binlog.000004", 4}, Position{"binlog.000003", 9999}, false},
		// A file number that grew a digit sorts by number, not by text.
		{Position{"binlog.999999", 500}, Position{"binlog.1000000", 4}, true},
		{Position{"binlog.1000000", 4}, Position{"binlog.999999", 500}, false},
		// No checkpoint yet reads as before everything.
		{Position{}, Position{"binlog.000001", 4}, true},
	}
	for _, c := range cases {
		if got := c.a.Before(c.b); got != c.before {
			t.Errorf("%s before %s = %v, want %v", c.a, c.b, got, c.before)
		}
	}
}

// TestWaitPosition_reachesOrTimesOut drives the barrier over a fake
// checkpoint: it waits while the checkpoint is behind or absent, returns
// once the checkpoint is at or past the target, and on the deadline fails
// naming both positions.
func TestWaitPosition_reachesOrTimesOut(t *testing.T) {
	want := Position{"binlog.000007", 5000}
	seq := []struct {
		p  Position
		ok bool
	}{{Position{}, false}, {Position{"binlog.000007", 4000}, true}, {Position{"binlog.000007", 5000}, true}}
	i := 0
	poll := func(context.Context) (Position, bool, error) {
		p := seq[min(i, len(seq)-1)]
		i++
		return p.p, p.ok, nil
	}
	got, err := waitPosition(context.Background(), poll, want, time.Second, time.Millisecond)
	if err != nil {
		t.Fatalf("barrier: %v", err)
	}
	if got != want || i != 3 {
		t.Fatalf("reached %s after %d polls, want %s after 3", got, i, want)
	}

	stuck := func(context.Context) (Position, bool, error) { return Position{"binlog.000007", 4000}, true, nil }
	_, err = waitPosition(context.Background(), stuck, want, 30*time.Millisecond, time.Millisecond)
	if err == nil {
		t.Fatal("a checkpoint stuck behind the target did not fail")
	}
	for _, s := range []string{"binlog.000007:5000", "binlog.000007:4000", "30ms"} {
		if !strings.Contains(err.Error(), s) {
			t.Errorf("barrier error %q lacks %q", err, s)
		}
	}

	boom := errors.New("index gone")
	_, err = waitPosition(context.Background(), func(context.Context) (Position, bool, error) { return Position{}, false, boom }, want, time.Second, time.Millisecond)
	if !errors.Is(err, boom) {
		t.Fatalf("a poll error was not returned: %v", err)
	}
}

// TestWaitPosition_captureDeathIsTheError: when the capture goroutine exits
// while the barrier waits, the barrier fails with the capture's own error
// at once, not with "did not reach X in 60s" after the deadline, and the
// error is put back on the channel so the test's stop() still sees it.
func TestWaitPosition_captureDeathIsTheError(t *testing.T) {
	want := Position{"binlog.000007", 5000}
	behind := func(context.Context) (Position, bool, error) { return Position{"binlog.000007", 4000}, true, nil }
	errCh := make(chan error, 1)
	died := errors.New("binlog syncer: connection reset")
	errCh <- died
	start := time.Now()
	_, err := waitPosition(context.Background(), withCapture(behind, errCh), want, 10*time.Second, time.Millisecond)
	if !errors.Is(err, died) || !strings.Contains(err.Error(), "capture stopped") || !strings.Contains(err.Error(), "connection reset") {
		t.Fatalf("barrier error = %v; want the capture's error, named as the capture stopping", err)
	}
	if time.Since(start) > time.Second {
		t.Fatal("the barrier waited for its deadline instead of failing on the capture's death")
	}
	select {
	case back := <-errCh:
		if back != died {
			t.Fatalf("channel holds %v after the barrier, want the capture's error put back", back)
		}
	default:
		t.Fatal("the barrier consumed the capture's error; stop() would block on the channel")
	}
	// A capture that exited CLEANLY (nil) while the barrier waits is just as
	// fatal: nothing will index the rest.
	errCh <- nil
	if _, err := waitPosition(context.Background(), withCapture(behind, errCh), want, 10*time.Second, time.Millisecond); err == nil || !strings.Contains(err.Error(), "capture stopped") || !strings.Contains(err.Error(), "returned nil") {
		t.Fatalf("a capture that returned nil mid-run did not fail the barrier: %v", err)
	}
	// With the channel quiet (the nil result put back above drained) the
	// barrier behaves as before.
	<-errCh
	if _, err := waitPosition(context.Background(), withCapture(behind, errCh), want, 20*time.Millisecond, time.Millisecond); err == nil || !strings.Contains(err.Error(), "did not reach") {
		t.Fatalf("quiet channel: %v", err)
	}
}

// TestWaitPosition_firstCheckpoint: the zero target is "any checkpoint" (the
// test's wait for capture to attach), and a poll error there is returned,
// not retried for 60 s.
func TestWaitPosition_firstCheckpoint(t *testing.T) {
	seq := []bool{false, false, true}
	i := 0
	poll := func(context.Context) (Position, bool, error) {
		ok := seq[min(i, len(seq)-1)]
		i++
		return Position{"binlog.000001", 4}, ok, nil
	}
	if got, err := waitPosition(context.Background(), poll, Position{}, time.Second, time.Millisecond); err != nil || got != (Position{"binlog.000001", 4}) || i != 3 {
		t.Fatalf("first checkpoint: got %s err %v after %d polls", got, err, i)
	}
	boom := errors.New("Error 1146: Table 'stream_state' doesn't exist")
	start := time.Now()
	_, err := waitPosition(context.Background(), func(context.Context) (Position, bool, error) { return Position{}, false, boom }, Position{}, 10*time.Second, time.Millisecond)
	if !errors.Is(err, boom) || time.Since(start) > time.Second {
		t.Fatalf("poll error: %v after %s; want it returned at once", err, time.Since(start))
	}
}

func TestTableRows_key(t *testing.T) {
	r := TableRows{Table: "t_comp", Columns: []string{"a", "b", "v"}, PK: []string{"a", "b"}, Rows: [][]string{{"1", "2", "9"}}}
	if k := r.Key(0); k != "1|2" {
		t.Fatalf("key = %q, want 1|2", k)
	}
}
