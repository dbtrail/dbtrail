package consoleapp

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/pgshim"
)

// The PostgreSQL router's status line (#3021): one a minute per server, the
// interval's numbers, and a line in a quiet minute too.

func TestPGPortStatusLine(t *testing.T) {
	before := pgPortCounts{copy: 100, source: 40, refused: 3, copyErr: 1, copySecs: 50, sourceSecs: 2}
	now := pgPortCounts{copy: 210, source: 52, refused: 5, copyErr: 2, sourceErr: 1, copySecs: 96.2, sourceSecs: 2.36, behind: secs(45)}
	got := lineOf(pgPortStatusAttrs("shop", time.Minute, now, before))
	want := "level=INFO msg=status service=pg-port server=shop interval=1m0s queries=124 dbtrail=110 postgresql=12 refused=2 errors=2 dbtrail_avg_ms=420 postgresql_avg_ms=30 copy_behind_s=45"
	if got != want {
		t.Errorf("the line:\n got %s\nwant %s", got, want)
	}
	for what, tc := range map[string]struct {
		now, before pgPortCounts
		want        string
	}{
		"a quiet minute still has its line, with no average and no lag": {before, before,
			"level=INFO msg=status service=pg-port server=shop interval=1m0s queries=0 dbtrail=0 postgresql=0 refused=0 errors=0"},
		"a lag of zero is written, not left out": {pgPortCounts{behind: secs(0)}, pgPortCounts{},
			"level=INFO msg=status service=pg-port server=shop interval=1m0s queries=0 dbtrail=0 postgresql=0 refused=0 errors=0 copy_behind_s=0"},
		"the first minute counts from zero": {pgPortCounts{copy: 7, copySecs: 0.7}, pgPortCounts{},
			"level=INFO msg=status service=pg-port server=shop interval=1m0s queries=7 dbtrail=7 postgresql=0 refused=0 errors=0 dbtrail_avg_ms=100"},
		"numbers that started over are the interval's, never a wrapped count": {pgPortCounts{source: 3, sourceSecs: 0.3}, pgPortCounts{source: 900, sourceSecs: 400},
			"level=INFO msg=status service=pg-port server=shop interval=1m0s queries=3 dbtrail=0 postgresql=3 refused=0 errors=0 postgresql_avg_ms=100"},
		"only refusals": {pgPortCounts{refused: 4}, pgPortCounts{refused: 1},
			"level=INFO msg=status service=pg-port server=shop interval=1m0s queries=3 dbtrail=0 postgresql=0 refused=3 errors=0"},
	} {
		if got := lineOf(pgPortStatusAttrs("shop", time.Minute, tc.now, tc.before)); got != tc.want {
			t.Errorf("%s:\n got %s\nwant %s", what, got, tc.want)
		}
	}
	// A name with a space is quoted, so the line still splits on spaces.
	if got := lineOf(pgPortStatusAttrs("my shop", time.Minute, pgPortCounts{}, pgPortCounts{})); !strings.Contains(got, `server="my shop"`) {
		t.Errorf("a name with a space: %s", got)
	}
}

// The counts are read off the status document the router serves: what each
// side finished, errors and time included, and the copy's last age.
func TestPGPortCountsOf(t *testing.T) {
	r := newPGRouteStats(time.Now(), false)
	r.note("p", pgshim.RouteNote{Side: pgshim.RouteCopy, Reason: pgshim.ReasonAnswersTheSame, Took: 400 * time.Millisecond})
	r.note("p", pgshim.RouteNote{Side: pgshim.RouteCopy, Reason: pgshim.ReasonAnswersTheSame, Took: 600 * time.Millisecond, Failed: true})
	r.note("p", pgshim.RouteNote{Side: pgshim.RouteSource, Reason: pgshim.ReasonVeto, Took: 30 * time.Millisecond})
	r.note("p", pgshim.RouteNote{Side: pgshim.RouteRefused, Reason: pgshim.ReasonReadOnly, Failed: true})
	c := pgPortCountsOf(r.doc("").Servers["p"])
	if c.copy != 2 || c.copyErr != 1 || c.source != 1 || c.sourceErr != 0 || c.refused != 1 || c.behind != nil {
		t.Fatalf("the counts: %+v", c)
	}
	if c.copySecs < 0.99 || c.copySecs > 1.01 || c.sourceSecs < 0.029 || c.sourceSecs > 0.031 {
		t.Fatalf("the times: %+v", c)
	}
	r.age("p", console.CopyRunAge{Ran: ageRan, Through: ageRan.Add(-1200 * time.Millisecond), Tailed: true})
	if c = pgPortCountsOf(r.doc("").Servers["p"]); c.behind == nil || *c.behind != 2 {
		t.Fatalf("the copy's age after a fresh answer: %+v", c)
	}
	got := lineOf(pgPortStatusAttrs("p", time.Minute, c, pgPortCounts{}))
	if want := "level=INFO msg=status service=pg-port server=p interval=1m0s queries=4 dbtrail=2 postgresql=1 refused=1 errors=1 dbtrail_avg_ms=500 postgresql_avg_ms=30 copy_behind_s=2"; got != want {
		t.Errorf("the line from a real tally:\n got %s\nwant %s", got, want)
	}
	// A server nothing reached yet.
	if c := pgPortCountsOf(pgRouterServerNumbers{}); c != (pgPortCounts{}) {
		t.Fatalf("an empty server: %+v", c)
	}
}

// The logger writes one line per server, in a fixed order, each about its own
// interval, and one line when there is no server at all.
func TestPGPortStatusLogger(t *testing.T) {
	r := newPGRouteStats(time.Now(), false)
	var mu sync.Mutex
	var lines []string
	p := &pgPortStatusLogger{
		servers: func() map[string]pgRouterServerNumbers { return r.doc("").Servers },
		nameOf:  func(id string) string { return "name-of-" + id },
		every:   time.Minute, last: map[string]pgPortCounts{},
		log: func(msg string, attrs ...any) {
			if msg != statusLogMsg {
				t.Errorf("message %q", msg)
			}
			mu.Lock()
			defer mu.Unlock()
			lines = append(lines, strings.TrimPrefix(lineOf(attrs), "level=INFO msg=status "))
		},
	}
	take := func() []string {
		mu.Lock()
		defer mu.Unlock()
		out := lines
		lines = nil
		return out
	}
	p.write()
	if got := take(); len(got) != 1 || got[0] != `service=pg-port interval=1m0s queries=0 note="there is no PostgreSQL server yet"` {
		t.Fatalf("with no server: %q", got)
	}
	r.note("b", pgshim.RouteNote{Side: pgshim.RouteCopy, Took: 100 * time.Millisecond})
	r.note("a", pgshim.RouteNote{Side: pgshim.RouteSource, Took: 20 * time.Millisecond})
	p.write()
	if got := take(); len(got) != 2 ||
		got[0] != "service=pg-port server=name-of-a interval=1m0s queries=1 dbtrail=0 postgresql=1 refused=0 errors=0 postgresql_avg_ms=20" ||
		got[1] != "service=pg-port server=name-of-b interval=1m0s queries=1 dbtrail=1 postgresql=0 refused=0 errors=0 dbtrail_avg_ms=100" {
		t.Fatalf("the first minute: %q", got)
	}
	// The next minute is about itself, not about everything since the start.
	r.note("b", pgshim.RouteNote{Side: pgshim.RouteCopy, Took: 300 * time.Millisecond})
	p.write()
	if got := take(); len(got) != 2 ||
		got[0] != "service=pg-port server=name-of-a interval=1m0s queries=0 dbtrail=0 postgresql=0 refused=0 errors=0" ||
		got[1] != "service=pg-port server=name-of-b interval=1m0s queries=1 dbtrail=1 postgresql=0 refused=0 errors=0 dbtrail_avg_ms=300" {
		t.Fatalf("the second minute: %q", got)
	}
}

// Turned off, nothing is written and nothing runs.
func TestStartPGPortStatusLog_off(t *testing.T) {
	t.Setenv(statusLogIntervalEnv, "off")
	called := make(chan struct{}, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	startPGPortStatusLog(ctx, func() map[string]pgRouterServerNumbers {
		called <- struct{}{}
		return nil
	}, func(id string) string { return id })
	select {
	case <-called:
		t.Fatal("the status log read the numbers though it is off")
	case <-time.After(50 * time.Millisecond):
	}
}

// On, a line is written each interval, through the service's own logger.
func TestStartPGPortStatusLog_writesEachInterval(t *testing.T) {
	t.Setenv(statusLogIntervalEnv, "1s")
	read := make(chan struct{}, 4)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	startPGPortStatusLog(ctx, func() map[string]pgRouterServerNumbers {
		select {
		case read <- struct{}{}:
		default:
		}
		return map[string]pgRouterServerNumbers{}
	}, func(id string) string { return id })
	select {
	case <-read:
	case <-time.After(10 * time.Second):
		t.Fatal("no line was written within ten intervals")
	}
}
