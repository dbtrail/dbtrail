package console

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// #1778: the Overview's window counts were reused for 30 minutes whatever
// they cost, so a new index cached its first "0 deletes" and kept it beside
// the first delete for half an hour. How long an aggregate is reused now
// follows what it cost to compute, and a cheap one is recomputed while the
// request waits.

// TestActivityTTLFollowsCost pins the scale: a hundred times the compute, never
// under a second, never over #1352's 30 minutes.
func TestActivityTTLFollowsCost(t *testing.T) {
	for _, tc := range []struct {
		cost, want time.Duration
	}{
		{0, time.Second},
		{2 * time.Millisecond, time.Second},
		{10 * time.Millisecond, time.Second},
		{100 * time.Millisecond, 10 * time.Second},
		{3 * time.Second, 5 * time.Minute},
		{18 * time.Second, 30 * time.Minute},
		{time.Hour, 30 * time.Minute},
	} {
		if got := activityTTL(tc.cost); got != tc.want {
			t.Errorf("activityTTL(%v) = %v, want %v", tc.cost, got, tc.want)
		}
	}
}

// primeActivity puts one entry in the cache the way a finished flight would,
// aged by age and with the compute cost given.
func primeActivity(c *activityCache, key string, resp activityResponse, age, cost time.Duration) {
	c.mu.Lock()
	c.entries[key] = resp
	c.stamps[key] = time.Now().Add(-age)
	c.costs[key] = cost
	c.prevCosts[key] = cost
	c.mu.Unlock()
}

func countingCompute(resp activityResponse, err error) (func(context.Context) (activityResponse, error), *int, *sync.Mutex) {
	var mu sync.Mutex
	n := 0
	return func(context.Context) (activityResponse, error) {
		mu.Lock()
		n++
		mu.Unlock()
		return resp, err
	}, &n, &mu
}

// TestActivityCheapStaleEntryIsRecomputedWhileTheRequestWaits is the #1778
// path end to end through the HTTP handler: the first render of a new index
// caches a zero, the first change lands, and the next render (the one the
// Getting started list triggers) must show it, not the cached zero.
func TestActivityCheapStaleEntryIsRecomputedWhileTheRequestWaits(t *testing.T) {
	db, mock, closeDB := newSQLMock(t)
	defer closeDB()
	mock.ExpectQuery(`COUNT\(\*\) AS n FROM binlog_events`).WillReturnRows(activityResult())
	mock.ExpectQuery(`COUNT\(\*\) AS n FROM binlog_events`).
		WillReturnRows(activityResult(aRow{"shop", "orders", 3, 1}))
	srv := newBootServer(db)

	if code, first, body := getActivity(t, srv, "/api/activity"); code != 200 {
		t.Fatalf("first code = %d, body = %s", code, body)
	} else if first.Deletes != 0 || first.Tables != 0 {
		t.Fatalf("first = %d deletes / %d tables, want the empty index's zeros", first.Deletes, first.Tables)
	}

	// The Getting started list polls every 3 s at the fastest, so the redraw
	// comes seconds after the first render. Two seconds is past the floor.
	c := srv.cm.boot.activity
	c.mu.Lock()
	for k := range c.stamps {
		c.stamps[k] = c.stamps[k].Add(-2 * time.Second)
	}
	c.mu.Unlock()

	code, next, body := getActivity(t, srv, "/api/activity")
	if code != 200 {
		t.Fatalf("second code = %d, body = %s", code, body)
	}
	if next.Deletes != 1 || next.Tables != 1 {
		t.Errorf("the redraw after the first change shows %d deletes / %d tables, want 1 / 1: "+
			"a cheap aggregate was served from the cache instead of recomputed", next.Deletes, next.Tables)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("the second request did not recompute: %v", err)
	}
}

// TestActivityCheapEntryWithinItsTTLIsReused keeps the floor honest: requests
// inside a second share one aggregate, cheap or not.
func TestActivityCheapEntryWithinItsTTLIsReused(t *testing.T) {
	c := newActivityCache()
	primeActivity(c, "", activityResponse{Deletes: 4}, 200*time.Millisecond, time.Millisecond)
	compute, n, mu := countingCompute(activityResponse{Deletes: 9}, nil)
	got, err := c.get(context.Background(), "", compute)
	if err != nil || got.Deletes != 4 {
		t.Fatalf("get = %+v, %v; want the cached 4", got, err)
	}
	time.Sleep(50 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	if *n != 0 {
		t.Errorf("computed %d time(s) for an entry inside its TTL", *n)
	}
}

// TestActivityCheapRefreshThatFailsKeepsTheOldEntry: a failed foreground
// refresh is not an error page. The old aggregate stays, with its own
// refreshed_at, the same stale-but-disclosed rule as a failed background one.
func TestActivityCheapRefreshThatFailsKeepsTheOldEntry(t *testing.T) {
	c := newActivityCache()
	primeActivity(c, "", activityResponse{Deletes: 4, RefreshedAt: "old"}, time.Minute, time.Millisecond)
	compute, _, _ := countingCompute(activityResponse{}, errors.New("index went away"))
	got, err := c.get(context.Background(), "", compute)
	if err != nil {
		t.Fatalf("a failed refresh surfaced as an error: %v", err)
	}
	if got.Deletes != 4 || got.RefreshedAt != "old" {
		t.Errorf("got %+v, want the previous aggregate as it was", got)
	}
}

// TestActivitySlowRefreshDoesNotHoldThePage: an aggregate that was cheap last
// time can be slow now (a busy server). The request waits at most inlineWait,
// then serves the previous aggregate; the flight keeps going, and its result
// is what the next request gets.
func TestActivitySlowRefreshDoesNotHoldThePage(t *testing.T) {
	c := newActivityCache()
	c.inlineWait = 50 * time.Millisecond
	primeActivity(c, "", activityResponse{Deletes: 4}, time.Minute, time.Millisecond)
	release := make(chan struct{})
	slow := func(ctx context.Context) (activityResponse, error) {
		select {
		case <-release:
		case <-ctx.Done():
		}
		return activityResponse{Deletes: 9}, nil
	}

	start := time.Now()
	got, err := c.get(context.Background(), "", slow)
	if err != nil || got.Deletes != 4 {
		t.Fatalf("get = %+v, %v; want the previous 4 once the wait ran out", got, err)
	}
	if waited := time.Since(start); waited > 2*time.Second {
		t.Errorf("the request waited %v for a slow refresh", waited)
	}

	close(release)
	deadline := time.Now().Add(5 * time.Second)
	for {
		c.mu.Lock()
		landed := c.entries[""].Deletes == 9 && c.flights[""] == nil
		c.mu.Unlock()
		if landed {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the slow flight never published its result")
		}
		time.Sleep(10 * time.Millisecond)
	}
	fresh, _ := c.get(context.Background(), "", slow)
	if fresh.Deletes != 9 {
		t.Errorf("the request after the flight landed got %d, want 9", fresh.Deletes)
	}
}

// TestActivityExpensiveEntryIsNotRecomputedEarly: an aggregate that took 20 s
// keeps #1352's 30 minutes. A cheap-TTL regression would re-scan a large index
// on every page load.
func TestActivityExpensiveEntryIsNotRecomputedEarly(t *testing.T) {
	c := newActivityCache()
	primeActivity(c, "", activityResponse{Deletes: 4}, 10*time.Minute, 20*time.Second)
	compute, n, mu := countingCompute(activityResponse{Deletes: 9}, nil)
	got, err := c.get(context.Background(), "", compute)
	if err != nil || got.Deletes != 4 {
		t.Fatalf("get = %+v, %v; want the cached 4", got, err)
	}
	time.Sleep(50 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	if *n != 0 {
		t.Errorf("a 20 s aggregate was recomputed after 10 minutes (%d time(s))", *n)
	}
}

// TestActivityMidCostStaleEntryRefreshesBehind: past its TTL, an aggregate
// that costs a second or more is served stale and refreshed in the background,
// as before #1778, so the page never waits on a scan that long.
func TestActivityMidCostStaleEntryRefreshesBehind(t *testing.T) {
	c := newActivityCache()
	primeActivity(c, "", activityResponse{Deletes: 4}, 6*time.Minute, 3*time.Second)
	release := make(chan struct{})
	defer close(release)
	started := make(chan struct{}, 1)
	compute := func(ctx context.Context) (activityResponse, error) {
		started <- struct{}{}
		select {
		case <-release:
		case <-ctx.Done():
		}
		return activityResponse{Deletes: 9}, nil
	}
	start := time.Now()
	got, err := c.get(context.Background(), "", compute)
	if err != nil || got.Deletes != 4 {
		t.Fatalf("get = %+v, %v; want the stale 4", got, err)
	}
	if waited := time.Since(start); waited >= c.inlineWait {
		t.Errorf("the request waited %v on a 3 s aggregate; it must serve stale at once", waited)
	}
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("no background refresh started for a stale entry")
	}
}

// TestActivityRecordsTheComputeCost: the cost that picks the TTL is the one
// the flight measured, so a cheap aggregate is not mistaken for an expensive
// one or the other way round.
func TestActivityRecordsTheComputeCost(t *testing.T) {
	c := newActivityCache()
	compute := func(context.Context) (activityResponse, error) {
		time.Sleep(30 * time.Millisecond)
		return activityResponse{Deletes: 1}, nil
	}
	if _, err := c.get(context.Background(), "", compute); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	cost := c.costs[""]
	c.mu.Unlock()
	if cost < 30*time.Millisecond || cost > 5*time.Second {
		t.Errorf("recorded cost = %v, want about 30ms", cost)
	}
}

// TestActivityEvictionDropsTheCost: evicting an entry drops its cost too, so
// the maps cannot drift apart and a re-added scope starts clean.
func TestActivityEvictionDropsTheCost(t *testing.T) {
	c := newActivityCache()
	for i := 0; i <= activityMaxProfiles; i++ {
		primeActivity(c, string(rune('a'+i)), activityResponse{}, time.Duration(activityMaxProfiles-i)*time.Second, time.Millisecond)
	}
	c.mu.Lock()
	c.evictOverCapLocked()
	ne, ns, nc, np := len(c.entries), len(c.stamps), len(c.costs), len(c.prevCosts)
	c.mu.Unlock()
	if ne != activityMaxProfiles || ns != ne || nc != ne || np != ne {
		t.Errorf("after eviction: %d entries, %d stamps, %d costs, %d previous costs; want %d of each", ne, ns, nc, np, activityMaxProfiles)
	}
}

// TestActivityHungRefreshIsWaitedOnOnce: with the index hung, only the
// requests in a flight's first inlineWait wait for it. A later request gets
// the stale entry at once, as it did before #1778, instead of every page load
// paying the full wait while the flight runs out its timeout.
func TestActivityHungRefreshIsWaitedOnOnce(t *testing.T) {
	c := newActivityCache()
	c.inlineWait = 100 * time.Millisecond
	primeActivity(c, "", activityResponse{Deletes: 4}, time.Minute, time.Millisecond)
	release := make(chan struct{})
	defer close(release)
	hung := func(ctx context.Context) (activityResponse, error) {
		select {
		case <-release:
		case <-ctx.Done():
		}
		return activityResponse{}, errors.New("index hung")
	}
	if got, err := c.get(context.Background(), "", hung); err != nil || got.Deletes != 4 {
		t.Fatalf("first get = %+v, %v; want the stale 4", got, err)
	}
	start := time.Now()
	got, err := c.get(context.Background(), "", hung)
	if err != nil || got.Deletes != 4 {
		t.Fatalf("second get = %+v, %v; want the stale 4", got, err)
	}
	if waited := time.Since(start); waited >= c.inlineWait/2 {
		t.Errorf("a request waited %v on a flight already past its wait", waited)
	}
}

// TestActivityOneSlowSampleDoesNotFreezeACheapIndex: the TTL follows the
// faster of the last two computes, so one slow sample on a small index (a
// lock wait, a cold cache) does not hold its count for 30 minutes.
func TestActivityOneSlowSampleDoesNotFreezeACheapIndex(t *testing.T) {
	c := newActivityCache()
	primeActivity(c, "", activityResponse{Deletes: 4}, 5*time.Second, 20*time.Second)
	c.mu.Lock()
	c.prevCosts[""] = 10 * time.Millisecond
	c.mu.Unlock()
	compute, _, _ := countingCompute(activityResponse{Deletes: 9}, nil)
	got, err := c.get(context.Background(), "", compute)
	if err != nil || got.Deletes != 9 {
		t.Errorf("get = %+v, %v; one 20 s sample after a 10 ms one held the count", got, err)
	}
}

// TestActivityTwoSlowSamplesKeepTheLongTTL: an index that is slow every time
// keeps #1352's protection.
func TestActivityTwoSlowSamplesKeepTheLongTTL(t *testing.T) {
	c := newActivityCache()
	primeActivity(c, "", activityResponse{Deletes: 4}, 10*time.Minute, 20*time.Second)
	c.mu.Lock()
	c.prevCosts[""] = 25 * time.Second
	c.mu.Unlock()
	compute, n, mu := countingCompute(activityResponse{Deletes: 9}, nil)
	if got, _ := c.get(context.Background(), "", compute); got.Deletes != 4 {
		t.Errorf("got %d, want the cached 4", got.Deletes)
	}
	time.Sleep(50 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	if *n != 0 {
		t.Errorf("an index slow twice in a row was recomputed after 10 minutes")
	}
}

// TestActivityKeepsThePreviousSample: each successful flight moves the last
// cost to prevCosts, which is what lets one outlier be outvoted.
func TestActivityKeepsThePreviousSample(t *testing.T) {
	c := newActivityCache()
	flight := func(d time.Duration) {
		f := &activityFlight{done: make(chan struct{})}
		c.mu.Lock()
		c.flights[""] = f
		c.mu.Unlock()
		c.run("", f, func(context.Context) (activityResponse, error) {
			time.Sleep(d)
			return activityResponse{}, nil
		}, false)
	}
	flight(30 * time.Millisecond)
	flight(90 * time.Millisecond)
	c.mu.Lock()
	cur, prev := c.costs[""], c.prevCosts[""]
	c.mu.Unlock()
	if cur < 90*time.Millisecond || prev < 30*time.Millisecond || prev >= 90*time.Millisecond {
		t.Errorf("costs = %v, prevCosts = %v; want the last and the one before it", cur, prev)
	}
}
