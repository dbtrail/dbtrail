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

func TestTableRows_key(t *testing.T) {
	r := TableRows{Table: "t_comp", Columns: []string{"a", "b", "v"}, PK: []string{"a", "b"}, Rows: [][]string{{"1", "2", "9"}}}
	if k := r.Key(0); k != "1|2" {
		t.Fatalf("key = %q, want 1|2", k)
	}
}
