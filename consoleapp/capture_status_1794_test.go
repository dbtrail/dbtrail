package consoleapp

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/status"
)

// #1794: the Overview asks the source whether capture is caught up. The
// three answers, and above all what is NOT an answer: a read that failed,
// a position-mode capture, a source one statement ahead.

var captureT0 = time.Date(2026, 9, 28, 12, 0, 0, 0, time.UTC)

func behindRead(captured, executed string, checkpoint time.Time) captureProbeResult {
	v, d := compareGTIDSets(captured, executed)
	return captureProbeResult{verdict: v, detail: d, captured: captured, executed: executed, checkpoint: checkpoint}
}

func TestCaptureStatusFrom(t *testing.T) {
	now := captureT0
	earlier := func(executed string, ago time.Duration) *captureSample {
		return &captureSample{executed: executed, at: now.Add(-ago)}
	}
	cases := []struct {
		name     string
		r        captureProbeResult
		prev     *captureSample
		state    string
		detail   string // prefix
		retry    bool
		keepPrev bool // the sample handed on is prev itself
	}{
		{name: "the sets are equal", r: behindRead(uuidB+":1-10", uuidB+":1-10", now),
			state: console.CaptureStateUpToDate},
		{name: "equal, whatever an earlier read said", r: behindRead(uuidB+":1-10", uuidB+":1-10", now),
			prev: earlier(uuidB+":1-10", time.Minute), state: console.CaptureStateUpToDate},

		// Could not ask, or the answer settles nothing.
		{name: "the source did not answer", r: captureProbeResult{detail: "the source did not answer", cause: "dial tcp: i/o timeout"},
			state: console.CaptureStateUnknown, detail: "the source did not answer", keepPrev: true},
		{name: "the source did not answer, after a read that found it ahead",
			r:    captureProbeResult{detail: "the source did not answer", cause: "dial tcp: i/o timeout"},
			prev: earlier(uuidB+":1-20", time.Minute), state: console.CaptureStateUnknown, detail: "the source did not answer", keepPrev: true},
		{name: "the probe timed out", r: captureProbeResult{detail: "the probe did not finish in time", cause: "timed out after 3s"},
			state: console.CaptureStateUnknown, detail: "the probe did not finish in time", keepPrev: true},
		{name: "position mode", r: captureProbeResult{detail: "the capture runs in binlog-position mode, which is not compared"},
			state: console.CaptureStateUnknown, detail: "the capture runs in binlog-position mode", keepPrev: true},
		{name: "a read that says nothing at all", r: captureProbeResult{},
			state: console.CaptureStateUnknown, detail: "the source was not read", keepPrev: true},
		{name: "the capture holds more than the source", r: behindRead(uuidB+":1-11", uuidB+":1-10", now),
			prev: earlier(uuidB+":1-10", time.Minute), state: console.CaptureStateUnknown, detail: "the capture holds transactions the source does not have", keepPrev: true},
		{name: "a verdict with sets that do not parse", r: captureProbeResult{verdict: console.CaptureBehind, captured: "0-1-100", executed: "0-1-200", checkpoint: now},
			prev: earlier(uuidB+":1-10", time.Minute), state: console.CaptureStateUnknown, detail: "the GTID sets do not parse", keepPrev: true},

		// Ahead, but not behind.
		{name: "one transaction ahead, twice: a trailing statement", r: behindRead(uuidB+":1-10", uuidB+":1-11", now),
			prev: earlier(uuidB+":1-11", time.Minute), state: console.CaptureStateUnknown, detail: "the source is one transaction ahead", keepPrev: true},
		{name: "one transaction ahead, of another server", r: behindRead(uuidB+":1-10", uuidA+":1,"+uuidB+":1-10", now),
			prev: earlier(uuidA+":1,"+uuidB+":1-10", time.Minute), state: console.CaptureStateUnknown, detail: "the source is one transaction ahead", keepPrev: true},
		{name: "tagged GTIDs on the source", r: behindRead(uuidB+":1-10", uuidB+":1-10:audit:1-50", now),
			prev: earlier(uuidB+":1-10:audit:1-50", time.Minute), state: console.CaptureStateUnknown, detail: "the source has tagged GTIDs", keepPrev: true},
		{name: "ahead on a first read", r: behindRead(uuidB+":1-10", uuidB+":1-30", now),
			state: console.CaptureStateUnknown, detail: "the source looked ahead on a first read", retry: true},
		{name: "ahead, and capture has reached what the source had before", r: behindRead(uuidB+":1-25", uuidB+":1-40", now),
			prev: earlier(uuidB+":1-20", time.Minute), state: console.CaptureStateUnknown, detail: "the source keeps writing and capture is reading it"},
		{name: "ahead, and no position saved since the first read", r: behindRead(uuidB+":1-10", uuidB+":1-30", now.Add(-2*time.Minute)),
			prev: earlier(uuidB+":1-30", time.Minute), state: console.CaptureStateUnknown, detail: "capture has not saved its position", retry: true, keepPrev: true},
		{name: "ahead, and the position saved too soon after the first read", r: behindRead(uuidB+":1-10", uuidB+":1-30", now.Add(-time.Minute).Add(captureConfirmGap-time.Second)),
			prev: earlier(uuidB+":1-30", time.Minute), state: console.CaptureStateUnknown, detail: "capture has not saved its position", retry: true, keepPrev: true},

		// Behind.
		{name: "ahead twice, the position saved since, and still short", r: behindRead(uuidB+":1-10", uuidB+":1-30", now),
			prev: earlier(uuidB+":1-30", time.Minute), state: console.CaptureStateBehind, detail: "the source reports transactions", keepPrev: true},
		{name: "ahead twice, capture moving but short of the first read", r: behindRead(uuidB+":1-15", uuidB+":1-40", now),
			prev: earlier(uuidB+":1-30", time.Minute), state: console.CaptureStateBehind, detail: "the source reports transactions", keepPrev: true},
		{name: "the newline MySQL puts between blocks", r: behindRead(uuidB+":1-10", uuidA+":1-5,\n"+uuidB+":1-30", now),
			prev: earlier(uuidA+":1-5,\n"+uuidB+":1-30", time.Minute), state: console.CaptureStateBehind, keepPrev: true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, next := captureStatusFrom(c.r, c.prev, now)
			if got.State != c.state || !strings.HasPrefix(got.Detail, c.detail) {
				t.Fatalf("got %+v, want state %q detail %q…", got, c.state, c.detail)
			}
			if got.State == console.CaptureStateUnknown && got.Detail == "" {
				t.Fatal("unknown must say why")
			}
			if (got.RetryInSeconds > 0) != c.retry {
				t.Fatalf("retry_in_seconds = %d, want set: %v", got.RetryInSeconds, c.retry)
			}
			if c.keepPrev != (next == c.prev) {
				t.Fatalf("the sample handed on: %+v (prev %+v), want prev kept: %v", next, c.prev, c.keepPrev)
			}
			if !c.keepPrev && (next == nil || next.executed != c.r.executed || !next.at.Equal(now)) {
				t.Fatalf("the new sample = %+v, want what the source had at %s", next, now)
			}
		})
	}
}

// #1794 review: a source that loaded a dump taken on another server (or
// ran SET @@GLOBAL.gtid_purged by hand) counts those transactions as
// executed, and no binlog carries them. Capture can never read them, and
// saves its position all the same, so a second read used to confirm
// "behind" for as long as the daemon ran. Purged transactions may turn
// behind into unknown, never anything into up to date.
func TestCaptureStatusFrom_purgedTransactions(t *testing.T) {
	now := captureT0
	withPurged := func(r captureProbeResult, purged string) captureProbeResult { r.purged = purged; return r }
	read := func(captured, executed, purged string, checkpoint time.Time) captureProbeResult {
		return withPurged(behindRead(captured, executed, checkpoint), purged)
	}
	dump := uuidA + ":1-100"
	// The reviewer's case: two reads, the checkpoint moving, only purged
	// transactions ahead.
	first, sample := captureStatusFrom(read(uuidB+":1-10", dump+","+uuidB+":1-10", dump, now), nil, now)
	later := now.Add(time.Minute)
	second, _ := captureStatusFrom(read(uuidB+":1-10", dump+","+uuidB+":1-10", dump, later), sample, later)
	for i, got := range []console.CaptureStatus{first, second} {
		if got.State != console.CaptureStateUnknown || !strings.Contains(got.Detail, "purged") {
			t.Errorf("read %d, only purged transactions ahead: %+v, want unknown", i+1, got)
		}
	}
	// Never up to date on purged transactions.
	if got, _ := captureStatusFrom(read(uuidB+":1-10", dump+","+uuidB+":1-10", dump, later), sample, later); got.State == console.CaptureStateUpToDate {
		t.Errorf("purged transactions made up to date: %+v", got)
	}
	// A busy source after the dump: capture has reached everything the
	// earlier read had except what was purged. It is reading, not behind:
	// the earlier read's purged set goes with it.
	first, sample = captureStatusFrom(read(uuidB+":1-10", dump+","+uuidB+":1-30", dump, now), nil, now)
	if sample == nil || sample.purged != dump {
		t.Fatalf("the sample does not keep the purged set: %+v (first %+v)", sample, first)
	}
	if got, _ := captureStatusFrom(read(uuidB+":1-30", dump+","+uuidB+":1-50", dump, later), sample, later); got.State != console.CaptureStateUnknown || !strings.Contains(got.Detail, "capture is reading") {
		t.Errorf("busy source after a dump, capture reading: %+v, want unknown", got)
	}
	// Real lag beside purged transactions is still behind.
	first, sample = captureStatusFrom(read(uuidB+":1-10", dump+","+uuidB+":1-30", dump, now), nil, now)
	if first.State != console.CaptureStateUnknown || first.RetryInSeconds == 0 {
		t.Errorf("purged plus 20 ahead, first read: %+v", first)
	}
	if got, _ := captureStatusFrom(read(uuidB+":1-10", dump+","+uuidB+":1-30", dump, later), sample, later); got.State != console.CaptureStateBehind {
		t.Errorf("purged plus 20 ahead, second read: %+v, want behind", got)
	}
	// Ordinary binlog expiry: capture already holds what was purged.
	first, sample = captureStatusFrom(read(uuidB+":1-10", uuidB+":1-30", uuidB+":1-5", now), nil, now)
	if got, _ := captureStatusFrom(read(uuidB+":1-10", uuidB+":1-30", uuidB+":1-5", later), sample, later); got.State != console.CaptureStateBehind {
		t.Errorf("expired binlogs capture had read, 20 ahead: %+v (first %+v), want behind", got, first)
	}
	// One real transaction ahead beside purged ones: the one-ahead rule.
	if got, _ := captureStatusFrom(read(uuidB+":1-10", dump+","+uuidB+":1-11", dump, later), &captureSample{executed: dump + "," + uuidB + ":1-11", purged: dump, at: now}, later); got.State != console.CaptureStateUnknown {
		t.Errorf("purged plus one ahead: %+v, want unknown", got)
	}
	// A purged value that is not inside executed does not parse into a
	// verdict.
	if got, _ := captureStatusFrom(read(uuidB+":1-10", uuidB+":1-30", uuidA+":1-5", later), &captureSample{executed: uuidB + ":1-30", at: now}, later); got.State != console.CaptureStateUnknown {
		t.Errorf("purged outside executed: %+v, want unknown", got)
	}
	if got, _ := captureStatusFrom(read(uuidB+":1-10", uuidB+":1-30", "not a set", later), &captureSample{executed: uuidB + ":1-30", at: now}, later); got.State != console.CaptureStateUnknown {
		t.Errorf("purged does not parse: %+v, want unknown", got)
	}
	// Something purged between the two reads that capture never read is a
	// real loss: judged against the EARLIER read's purged set, it still
	// reaches the checkpoint check.
	if got, _ := captureStatusFrom(read(uuidB+":1-10", uuidB+":1-30", uuidB+":1-20", later), &captureSample{executed: uuidB + ":1-30", at: now}, later); got.State == console.CaptureStateUpToDate {
		t.Errorf("a purge between the reads: %+v", got)
	}
}

func TestCountGTIDs(t *testing.T) {
	for set, want := range map[string]int64{
		uuidB + ":1":                           1,
		uuidB + ":1-10":                        10,
		uuidB + ":1-10:12-13":                  12,
		uuidA + ":5," + uuidB + ":1-10":        11,
		uuidB + ":1-10:audit:1-3":              13,
		uuidA + ":1-5,\n" + uuidB + ":1-30:40": 36,
	} {
		h, _, ok := parseGTIDPair(set, set)
		if !ok {
			t.Fatalf("%q does not parse", set)
		}
		if got := countGTIDs(h); got != want {
			t.Errorf("countGTIDs(%q) = %d, want %d", set, got, want)
		}
	}
}

// captureReporter is a reporter over a fixed clock and a counted read.
func captureReporter(boot string, read func(ctx context.Context, indexDSN, sourceDSN string) captureProbeResult) (*captureStatusReporter, *time.Time) {
	clock := captureT0
	c := newCaptureStatusReporter(boot)
	c.now = func() time.Time { return clock }
	c.read = read
	return c, &clock
}

var (
	captureEntryA = console.ServerEntry{ID: "a", Name: "a", DSN: "u:p@tcp(idx:3306)/a", SourceDSN: "u:p@tcp(src-a:3306)/"}
	captureEntryB = console.ServerEntry{ID: "b", Name: "b", DSN: "u:p@tcp(idx:3306)/b", SourceDSN: "u:p@tcp(src-b:3306)/"}
)

func TestCaptureStatus_notAskedWithoutASourceToAsk(t *testing.T) {
	c, _ := captureReporter("", func(context.Context, string, string) captureProbeResult {
		t.Error("a server that cannot be compared was read")
		return captureProbeResult{verdict: console.CaptureCaughtUp}
	})
	for name, e := range map[string]console.ServerEntry{
		"no source":  {ID: "n", DSN: "u:p@tcp(idx:3306)/n"},
		"PostgreSQL": {ID: "p", DSN: "u:p@tcp(idx:3306)/p", SourceDSN: "postgres://u:p@h/db", Flavor: console.FlavorPostgres},
		"the daemon's own index, started without a source": {ID: bootCaptureServerID, DSN: "u:p@tcp(idx:3306)/boot"},
	} {
		got := c.CaptureStatus(context.Background(), e)
		if got.State != console.CaptureStateUnknown || got.Detail == "" || got.ServerID != e.ID {
			t.Errorf("%s: got %+v, want unknown with a reason, for %q", name, got, e.ID)
		}
	}
}

func TestCaptureStatus_theDaemonsOwnIndexAsksItsOwnSource(t *testing.T) {
	var asked []string
	c, _ := captureReporter("u:p@tcp(boot-src:3306)/", func(_ context.Context, idx, src string) captureProbeResult {
		asked = append(asked, idx+" <- "+src)
		return captureProbeResult{verdict: console.CaptureCaughtUp}
	})
	// A source DSN on the entry is not the boot capture's.
	got := c.CaptureStatus(context.Background(), console.ServerEntry{ID: bootCaptureServerID, DSN: "u:p@tcp(idx:3306)/boot", SourceDSN: "u:p@tcp(other:3306)/"})
	if got.State != console.CaptureStateUpToDate || len(asked) != 1 || asked[0] != "u:p@tcp(idx:3306)/boot <- u:p@tcp(boot-src:3306)/" {
		t.Fatalf("got %+v, asked %v", got, asked)
	}
}

// A read that failed, timed out or found a position-mode capture is unknown
// through the whole reporter, whatever was said before it.
func TestCaptureStatus_aFailedReadIsNeverAnAnswer(t *testing.T) {
	next := captureProbeResult{verdict: console.CaptureCaughtUp, captured: uuidB + ":1-10", executed: uuidB + ":1-10"}
	c, clock := captureReporter("", func(context.Context, string, string) captureProbeResult { return next })
	if got := c.CaptureStatus(context.Background(), captureEntryA); got.State != console.CaptureStateUpToDate {
		t.Fatalf("first read: %+v", got)
	}
	for _, failed := range []captureProbeResult{
		{detail: "the source did not answer", cause: "dial tcp 10.0.0.1:3306: i/o timeout"},
		{detail: "the probe did not finish in time", cause: "timed out after 3s"},
		{detail: "the capture runs in binlog-position mode, which is not compared"},
		{detail: "the source did not report its GTID set", cause: "Error 1227"},
	} {
		*clock = clock.Add(captureStatusTTL)
		next = failed
		got := c.CaptureStatus(context.Background(), captureEntryA)
		if got.State != console.CaptureStateUnknown || got.Detail != failed.detail {
			t.Errorf("%q: got %+v, want unknown with that reason", failed.detail, got)
		}
		if strings.Contains(got.Detail, "10.0.0.1") || strings.Contains(got.Detail, "1227") {
			t.Errorf("the cause reached the page: %+v", got)
		}
	}
}

// The read that takes longer than the bound is unknown, and is not waited
// for.
func TestCaptureStatus_aSlowSourceIsUnknown(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	c := newCaptureStatusReporter("")
	c.read = func(ctx context.Context, _, _ string) captureProbeResult {
		if _, ok := ctx.Deadline(); !ok {
			t.Error("the read carries no deadline")
		}
		select {
		case <-ctx.Done():
		case <-release:
		}
		<-release
		return captureProbeResult{verdict: console.CaptureCaughtUp}
	}
	start := time.Now()
	got := c.CaptureStatus(context.Background(), captureEntryA)
	if got.State != console.CaptureStateUnknown || !strings.HasPrefix(got.Detail, "the probe did not finish in time") {
		t.Fatalf("got %+v, want unknown: the probe did not finish in time", got)
	}
	if took := time.Since(start); took > windowProbeTimeout+2*time.Second {
		t.Fatalf("the page waited %s for a source bounded at %s", took, windowProbeTimeout)
	}
}

func TestCaptureStatus_answersAreReusedForAShortWhile(t *testing.T) {
	var reads int
	next := captureProbeResult{verdict: console.CaptureCaughtUp, captured: uuidB + ":1-10", executed: uuidB + ":1-10"}
	c, clock := captureReporter("", func(ctx context.Context, _, _ string) captureProbeResult {
		reads++
		if ctx.Err() != nil {
			t.Errorf("the read inherited the request's cancellation: %v", ctx.Err())
		}
		return next
	})
	gone, cancel := context.WithCancel(context.Background())
	cancel() // the tab went away
	for range 10 {
		if got := c.CaptureStatus(gone, captureEntryA); got.State != console.CaptureStateUpToDate {
			t.Fatalf("got %+v", got)
		}
	}
	if reads != 1 {
		t.Fatalf("ten page loads inside the window read the source %d times, want 1", reads)
	}
	*clock = clock.Add(captureStatusTTL - time.Second)
	c.CaptureStatus(context.Background(), captureEntryA)
	if reads != 1 {
		t.Fatalf("a load one second before the window ends read the source again (%d reads)", reads)
	}
	*clock = clock.Add(time.Second)
	next = captureProbeResult{detail: "the source did not answer", cause: "x"}
	if got := c.CaptureStatus(context.Background(), captureEntryA); reads != 2 || got.State != console.CaptureStateUnknown {
		t.Fatalf("after the window: %d reads, %+v; want a second read and its answer", reads, got)
	}
	// A page open for an hour, asking every five seconds.
	reads = 0
	for range 720 {
		*clock = clock.Add(5 * time.Second)
		c.CaptureStatus(context.Background(), captureEntryA)
	}
	if max := int(time.Hour / captureStatusTTL); reads > max {
		t.Fatalf("an hour of asking every 5s read the source %d times, want at most %d", reads, max)
	}
	// The worst case: every read finds the source ahead and unconfirmed,
	// which is kept for the shorter time.
	reads = 0
	next = behindRead(uuidB+":1-10", uuidB+":1-30", captureT0.Add(-time.Hour))
	for range 720 {
		*clock = clock.Add(5 * time.Second)
		c.CaptureStatus(context.Background(), captureEntryA)
	}
	if max := int(time.Hour / captureStatusPendingTTL); reads > max || reads == 0 {
		t.Fatalf("an hour of asking every 5s, never settled, read the source %d times, want at most %d", reads, max)
	}
}

func TestCaptureStatus_behindTakesTwoReads(t *testing.T) {
	var reads int
	checkpoint := captureT0
	c, clock := captureReporter("", func(context.Context, string, string) captureProbeResult {
		reads++
		return behindRead(uuidB+":1-10", uuidB+":1-30", checkpoint)
	})
	first := c.CaptureStatus(context.Background(), captureEntryA)
	if first.State != console.CaptureStateUnknown || first.RetryInSeconds <= 0 {
		t.Fatalf("first read: %+v, want unknown and a time to ask again", first)
	}
	// The page asks again when it was told to: that must read the source,
	// not find the first answer still kept.
	*clock = clock.Add(time.Duration(first.RetryInSeconds) * time.Second)
	checkpoint = *clock
	second := c.CaptureStatus(context.Background(), captureEntryA)
	if reads != 2 || second.State != console.CaptureStateBehind || second.RetryInSeconds != 0 {
		t.Fatalf("second read: %d reads, %+v; want behind", reads, second)
	}
}

func TestCaptureStatus_eachServerItsOwnAnswer(t *testing.T) {
	var asked []string
	c, clock := captureReporter("", func(_ context.Context, _, src string) captureProbeResult {
		asked = append(asked, src)
		if src == captureEntryA.SourceDSN {
			return captureProbeResult{verdict: console.CaptureCaughtUp, captured: uuidA + ":1-10", executed: uuidA + ":1-10"}
		}
		return behindRead(uuidB+":1-10", uuidB+":1-30", captureT0.Add(time.Hour))
	})
	a := c.CaptureStatus(context.Background(), captureEntryA)
	b := c.CaptureStatus(context.Background(), captureEntryB)
	if a.State != console.CaptureStateUpToDate || a.ServerID != "a" || b.State != console.CaptureStateUnknown || b.ServerID != "b" {
		t.Fatalf("a = %+v, b = %+v: the answer kept for one server was given for the other", a, b)
	}
	if len(asked) != 2 || asked[0] != captureEntryA.SourceDSN || asked[1] != captureEntryB.SourceDSN {
		t.Fatalf("asked %v, want each server's own source", asked)
	}
	// b's first sample must not count as a's earlier read, nor a's as b's.
	*clock = clock.Add(captureStatusTTL)
	if b := c.CaptureStatus(context.Background(), captureEntryB); b.State != console.CaptureStateBehind || b.ServerID != "b" {
		t.Fatalf("b, second read = %+v, want behind", b)
	}
	if a := c.CaptureStatus(context.Background(), captureEntryA); a.State != console.CaptureStateUpToDate || a.ServerID != "a" {
		t.Fatalf("a, second read = %+v, want up to date", a)
	}
}

// An edited server starts over: what another source had executed is not
// the earlier read of this one.
func TestCaptureStatus_anEditedSourceStartsOver(t *testing.T) {
	var reads int
	c, clock := captureReporter("", func(context.Context, string, string) captureProbeResult {
		reads++
		return behindRead(uuidB+":1-10", uuidB+":1-30", captureT0.Add(time.Hour))
	})
	c.CaptureStatus(context.Background(), captureEntryA)
	moved := captureEntryA
	moved.SourceDSN = "u:p@tcp(src-new:3306)/"
	got := c.CaptureStatus(context.Background(), moved)
	if reads != 2 {
		t.Fatalf("the answer of the old source was reused for the new one (%d reads)", reads)
	}
	if got.State != console.CaptureStateUnknown || got.RetryInSeconds <= 0 {
		t.Fatalf("got %+v, want a first read: the old source's sample must not confirm it", got)
	}
	*clock = clock.Add(captureStatusTTL)
	if got := c.CaptureStatus(context.Background(), moved); got.State != console.CaptureStateBehind {
		t.Fatalf("second read of the new source = %+v, want behind", got)
	}
}

func TestCaptureStatus_oneReadInFlightPerServer(t *testing.T) {
	var reads atomic.Int32
	started := make(chan struct{})
	release := make(chan struct{})
	c := newCaptureStatusReporter("")
	c.read = func(_ context.Context, _, src string) captureProbeResult {
		if reads.Add(1) == 1 {
			close(started)
		}
		<-release
		return captureProbeResult{verdict: console.CaptureCaughtUp, captured: uuidB + ":1-10", executed: uuidB + ":1-10"}
	}
	var wg sync.WaitGroup
	answers := make([]console.CaptureStatus, 10)
	launch := func(i int) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			answers[i] = c.CaptureStatus(context.Background(), captureEntryA)
		}()
	}
	launch(0)
	<-started
	for i := 1; i < 10; i++ {
		launch(i)
	}
	// A request that gives up while waiting is unknown, and takes nothing
	// from the read on its way.
	gone, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if got := c.CaptureStatus(gone, captureEntryA); got.State != console.CaptureStateUnknown {
		t.Errorf("a request that gave up got %+v", got)
	}
	close(release)
	wg.Wait()
	if n := reads.Load(); n != 1 {
		t.Fatalf("ten requests at once read the source %d times, want 1", n)
	}
	for i, a := range answers {
		if a.State != console.CaptureStateUpToDate {
			t.Errorf("request %d got %+v", i, a)
		}
	}
}

// headFromState from a real stream_state to the verdict, with both servers
// mocked. What differs from the schedule's compareCapture: a dropped row or
// a loss does not refuse, since the page says those on lines of their own.
func TestHeadFromState(t *testing.T) {
	uuidQ := regexp.QuoteMeta("SELECT @@GLOBAL.server_uuid")
	// The whole statement: both sets of one instant, and the purged one read
	// here only (the snapshot schedule's query is TestCompareCapture's).
	gtidQ := "^" + regexp.QuoteMeta("SELECT @@GLOBAL.gtid_mode, @@GLOBAL.gtid_executed, @@GLOBAL.gtid_purged") + "$"
	uuidRow := func(u string) *sqlmock.Rows { return sqlmock.NewRows([]string{"u"}).AddRow(u) }
	twoServers := func(executed, purged string) (func(sqlmock.Sqlmock), func(sqlmock.Sqlmock)) {
		return func(m sqlmock.Sqlmock) { m.ExpectQuery(uuidQ).WillReturnRows(uuidRow(uuidA)) },
			func(m sqlmock.Sqlmock) {
				m.ExpectQuery(uuidQ).WillReturnRows(uuidRow(uuidB))
				m.ExpectQuery(gtidQ).WillReturnRows(sqlmock.NewRows([]string{"m", "e", "p"}).AddRow("ON", executed, purged))
			}
	}
	checkpoint := captureT0.Add(-5 * time.Second)
	state := func(set string, edit func(*status.StreamStateInfo)) *status.StreamStateInfo {
		s := streamStateFor(set)
		s.LastCheckpoint = checkpoint
		if edit != nil {
			edit(s)
		}
		return s
	}
	type tc struct {
		name      string
		st        *status.StreamStateInfo
		executed  string // "": the source is never read
		purged    string
		wantPurge string
		sameHost  bool
		gtidOff   bool
		openFails bool
		verdict   string
		detail    string
		wantErr   bool
		opened    bool
	}
	cases := []tc{
		{name: "equal", st: state(uuidB+":1-10", nil), executed: uuidB + ":1-10", verdict: console.CaptureCaughtUp, opened: true},
		{name: "equal, with dropped rows on record", st: state(uuidB+":1-10", func(s *status.StreamStateInfo) {
			s.CaptureSkips = skipLedger(4, captureT0.Add(-time.Hour))
		}), executed: uuidB + ":1-10", verdict: console.CaptureCaughtUp, opened: true},
		{name: "the source ahead", st: state(uuidB+":1-10", nil), executed: uuidB + ":1-30", verdict: console.CaptureBehind, detail: "the source reports transactions", opened: true},
		{name: "the purged set is read, its whitespace dropped", st: state(uuidB+":1-10", nil), executed: uuidA + ":1-100," + uuidB + ":1-10",
			purged: uuidA + ":1-100,\n" + uuidB + ":1-3", wantPurge: uuidA + ":1-100," + uuidB + ":1-3", verdict: console.CaptureBehind, detail: "the source reports transactions", opened: true},
		{name: "position mode: the source never opened", st: state(uuidB+":1-10", func(s *status.StreamStateInfo) { s.Mode = "position" }),
			detail: "the capture runs in binlog-position mode"},
		{name: "no capture on record: the source never opened", st: nil, detail: "the index has no live capture on record"},
		{name: "no GTID checkpoint: the source never opened", st: state("  ", nil), detail: "the capture has no GTID checkpoint"},
		{name: "a MariaDB set in the checkpoint", st: state("0-1-100", nil), gtidOff: true, detail: "the source reported no GTID set", opened: true},
		{name: "GTIDs off on the source", st: state(uuidB+":1-10", nil), gtidOff: true, detail: "the source reported no GTID set", opened: true},
		{name: "the index on the source server", st: state(uuidB+":1-10", nil), sameHost: true, detail: "the index lives on the source server", opened: true},
		{name: "the source does not answer", st: state(uuidB+":1-10", nil), openFails: true, detail: "the source did not answer", wantErr: true, opened: true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			idx, im, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer idx.Close()
			src, sm, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			switch {
			case c.sameHost:
				im.ExpectQuery(uuidQ).WillReturnRows(uuidRow(uuidB))
				sm.ExpectQuery(uuidQ).WillReturnRows(uuidRow(uuidB))
			case c.gtidOff:
				im.ExpectQuery(uuidQ).WillReturnRows(uuidRow(uuidA))
				sm.ExpectQuery(uuidQ).WillReturnRows(uuidRow(uuidB))
				sm.ExpectQuery(gtidQ).WillReturnRows(sqlmock.NewRows([]string{"m", "e", "p"}).AddRow("OFF", "", ""))
			case c.executed != "":
				i, s := twoServers(c.executed, c.purged)
				i(im)
				s(sm)
			}
			sm.ExpectClose()
			opened := false
			r, err := headFromState(context.Background(), idx, c.st, func() (*sql.DB, error) {
				opened = true
				if c.openFails {
					return nil, fmt.Errorf("dial tcp 10.0.0.1:3306: connect: connection refused")
				}
				return src, nil
			})
			if (err != nil) != c.wantErr || r.verdict != c.verdict || !strings.HasPrefix(r.detail, c.detail) || opened != c.opened {
				t.Fatalf("got %+v err=%v opened=%v, want verdict %q detail %q… err=%v opened=%v", r, err, opened, c.verdict, c.detail, c.wantErr, c.opened)
			}
			// A checkpoint that cannot be compared is said apart from a
			// source that was not reached (#2085: the watermark is void).
			if comparable, _ := checkpointComparable(c.st); r.uncomparable == comparable {
				t.Fatalf("uncomparable = %v for a checkpoint whose comparable is %v", r.uncomparable, comparable)
			}
			if c.verdict != "" && (r.captured == "" || r.executed != c.executed || r.purged != c.wantPurge || !r.checkpoint.Equal(checkpoint)) {
				t.Fatalf("a verdict without what it was read from: %+v", r)
			}
			// What the page is told, from this read alone: never behind.
			got, _ := captureStatusFrom(r, nil, captureT0)
			want := console.CaptureStateUnknown
			if c.verdict == console.CaptureCaughtUp {
				want = console.CaptureStateUpToDate
			}
			if got.State != want {
				t.Fatalf("the page is told %+v, want %q", got, want)
			}
			if err := im.ExpectationsWereMet(); err != nil {
				t.Fatalf("index: %v", err)
			}
			if c.opened && !c.openFails {
				if err := sm.ExpectationsWereMet(); err != nil {
					t.Fatalf("source (closed too): %v", err)
				}
			} else {
				src.Close()
			}
		})
	}
}

// The wiring, end to end: the web interface's route, the id it gives the
// daemon's own index, and the reporter that must recognise that id as the
// capture started with --source-dsn. With the two ids apart, the boot
// capture is never asked about.
func TestCaptureStatus_routeReachesTheBootSource(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	reg, err := console.LoadRegistry(t.TempDir() + "/console-servers.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var asked []string
	rep, _ := captureReporter("u:p@tcp(boot-src:3306)/", func(_ context.Context, idx, src string) captureProbeResult {
		asked = append(asked, idx+" <- "+src)
		return captureProbeResult{verdict: console.CaptureCaughtUp}
	})
	srv, err := console.New(console.Config{
		Listen: "127.0.0.1:8090", Token: "t", Registry: reg, CaptureStatus: rep,
		DB: db, DBName: "binlog_index", BootDSN: "cli:pw@tcp(127.0.0.1:3306)/binlog_index",
	})
	if err != nil {
		t.Fatal(err)
	}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "http://127.0.0.1:8090/api/capture-status", nil)
	req.Header.Set("Authorization", "Bearer t")
	srv.Handler().ServeHTTP(rec, req)
	var got console.CaptureStatus
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("code %d, body %q: %v", rec.Code, rec.Body.String(), err)
	}
	if got.State != console.CaptureStateUpToDate || got.ServerID != bootCaptureServerID {
		t.Fatalf("got %+v", got)
	}
	if len(asked) != 1 || asked[0] != "cli:pw@tcp(127.0.0.1:3306)/binlog_index <- u:p@tcp(boot-src:3306)/" {
		t.Fatalf("asked %v", asked)
	}
}

// Both watch entry points build their console from upConsoleConfig: the
// reporter is wired there, with the daemon's own source.
func TestCaptureStatus_watchWiresTheReporter(t *testing.T) {
	old := upSourceDSN
	t.Cleanup(func() { upSourceDSN = old })
	upSourceDSN = "u:p@tcp(boot-src:3306)/"
	cfg, err := upConsoleConfig(nil, "user:pass@tcp(127.0.0.1:3306)/binlog_index", consoleOpts{Listen: "127.0.0.1:8090", Token: "tok"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	rep, ok := cfg.CaptureStatus.(*captureStatusReporter)
	if !ok || rep == nil || rep.bootSourceDSN != upSourceDSN {
		t.Fatalf("CaptureStatus = %#v, want the reporter with the daemon's source", cfg.CaptureStatus)
	}
}

// A read that panics is an answer, unknown, and the next load reads again:
// the daemon that draws this page is the one that captures.
func TestCaptureStatus_aPanicIsUnknownAndHoldsNothing(t *testing.T) {
	var reads int
	c, clock := captureReporter("", func(context.Context, string, string) captureProbeResult {
		reads++
		if reads == 1 {
			panic("boom")
		}
		return captureProbeResult{verdict: console.CaptureCaughtUp, captured: uuidB + ":1-10", executed: uuidB + ":1-10"}
	})
	if got := c.CaptureStatus(context.Background(), captureEntryA); got.State != console.CaptureStateUnknown || got.Detail == "" {
		t.Fatalf("a read that panicked: %+v", got)
	}
	*clock = clock.Add(captureStatusTTL)
	done := make(chan console.CaptureStatus, 1)
	go func() { done <- c.CaptureStatus(context.Background(), captureEntryA) }()
	select {
	case got := <-done:
		if got.State != console.CaptureStateUpToDate || reads != 2 {
			t.Fatalf("the load after it: %+v, %d reads", got, reads)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the load after a panic never returned: the read in flight was never released")
	}
}

// A failure after the read (here the clock) must not leave the server's
// read marked in flight: the next load reads again instead of waiting on a
// channel nobody closes.
func TestCaptureStatus_aFailureAfterTheReadHoldsNothing(t *testing.T) {
	var reads, calls int
	c := newCaptureStatusReporter("")
	c.read = func(context.Context, string, string) captureProbeResult {
		reads++
		return captureProbeResult{verdict: console.CaptureCaughtUp, captured: uuidB + ":1-10", executed: uuidB + ":1-10"}
	}
	c.now = func() time.Time {
		calls++
		if calls == 2 { // the clock read after the source answered (the first is before the read, #2085)
			panic("clock")
		}
		return captureT0.Add(time.Duration(calls) * time.Hour)
	}
	func() {
		defer func() { _ = recover() }()
		c.CaptureStatus(context.Background(), captureEntryA)
	}()
	done := make(chan console.CaptureStatus, 1)
	go func() { done <- c.CaptureStatus(context.Background(), captureEntryA) }()
	select {
	case got := <-done:
		if reads != 2 || got.State != console.CaptureStateUpToDate {
			t.Fatalf("the load after it: %+v, %d reads", got, reads)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the load after a failure waited on a read that was over")
	}
}
