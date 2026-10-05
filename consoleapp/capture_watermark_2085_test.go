package consoleapp

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/console"
)

// #2085: the watermark is the newest instant at which the source was asked
// and capture is known to have held everything it had executed then. Each
// case is one read; what must never happen is an instant capture had not
// reached being handed out.
func TestAdvanceWatermark(t *testing.T) {
	asked := captureT0
	before := captureT0.Add(-time.Minute)
	long := captureT0.Add(-time.Hour)
	sample := func(executed string, at time.Time) *captureSample {
		return &captureSample{executed: executed, at: at}
	}
	read := func(captured, executed string) captureProbeResult {
		return behindRead(captured, executed, captureT0)
	}
	cases := []struct {
		name        string
		r           captureProbeResult
		through     time.Time
		pending     *captureSample
		wantThrough time.Time
		// wantPending: "" none, "kept" the one passed in, else the executed
		// set of a new sample taken at this read.
		wantPending string
		wantWhy     string // prefix; "" when a watermark is known
	}{
		{name: "equal sets: complete as of this read",
			r: read(uuidB+":1-10", uuidB+":1-10"), wantThrough: asked},
		{name: "equal sets drop a sample that was waiting",
			r: read(uuidB+":1-30", uuidB+":1-30"), pending: sample(uuidB+":1-20", before), wantThrough: asked},
		{name: "ahead on a first read: no watermark, a sample left",
			r: read(uuidB+":1-10", uuidB+":1-30"), wantPending: uuidB + ":1-30", wantWhy: "the source is ahead"},
		{name: "ahead, and capture has reached the earlier sample: complete as of THAT read",
			r: read(uuidB+":1-25", uuidB+":1-40"), pending: sample(uuidB+":1-20", before),
			wantThrough: before, wantPending: uuidB + ":1-40"},
		{name: "ahead, and capture has not reached the earlier sample: nothing proven, the sample waits",
			r: read(uuidB+":1-15", uuidB+":1-40"), pending: sample(uuidB+":1-20", before),
			wantPending: "kept", wantWhy: "the source is ahead"},
		{name: "not reached yet, an older proof stands",
			r: read(uuidB+":1-15", uuidB+":1-40"), through: long, pending: sample(uuidB+":1-20", before),
			wantThrough: long, wantPending: "kept"},
		{name: "the watermark never moves back",
			r: read(uuidB+":1-25", uuidB+":1-40"), through: asked.Add(-time.Second), pending: sample(uuidB+":1-20", before),
			wantThrough: asked.Add(-time.Second), wantPending: uuidB + ":1-40"},
		{name: "one transaction short of the sample is not reached",
			r: read(uuidB+":1-19", uuidB+":1-40"), pending: sample(uuidB+":1-20", before),
			wantPending: "kept", wantWhy: "the source is ahead"},
		{name: "a sample of another server's transactions is not reached by this one's",
			r: read(uuidB+":1-50", uuidA+":1-5,"+uuidB+":1-60"), pending: sample(uuidA+":1-5,"+uuidB+":1-20", before),
			wantPending: "kept", wantWhy: "the source is ahead"},

		// Nothing learned: what was proven stays, and ages.
		{name: "the source did not answer",
			r: captureProbeResult{detail: "the source did not answer"}, through: long, pending: sample(uuidB+":1-20", before),
			wantThrough: long, wantPending: "kept"},
		{name: "the source did not answer and nothing was ever proven",
			r: captureProbeResult{detail: "the source did not answer"}, wantWhy: "the source did not answer"},
		{name: "position mode", r: captureProbeResult{detail: "the capture runs in binlog-position mode, which is not compared"},
			wantWhy: "the capture runs in binlog-position mode"},
		{name: "a read that says nothing at all", r: captureProbeResult{}, wantWhy: "the source was not read"},

		// No longer comparable: everything is dropped.
		{name: "the capture holds more than the source: the source was replaced",
			r: read(uuidB+":1-11", uuidB+":1-10"), through: before, pending: sample(uuidB+":1-10", before),
			wantWhy: "the capture holds transactions the source does not have"},
		{name: "tagged GTIDs on the source",
			r: read(uuidB+":1-10", uuidB+":1-10:audit:1-50"), through: before, wantWhy: "the source has tagged GTIDs"},
		{name: "sets that do not parse",
			r: captureProbeResult{captured: "0-1-100", executed: "0-1-200"}, through: before, wantWhy: "the GTID sets do not parse"},

		// The source purged transactions capture never held (a dump loaded
		// with SET @@GLOBAL.gtid_purged): the status read allows for them,
		// the watermark does not. No binlog carried them to the index.
		{name: "equal only if what the source purged is counted: not complete",
			r: func() captureProbeResult {
				r := read(uuidB+":1-10", uuidA+":1-5,"+uuidB+":1-10")
				r.purged = uuidA + ":1-5"
				return r
			}(), wantPending: uuidA + ":1-5," + uuidB + ":1-10", wantWhy: "the source is ahead"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			through, pending, why := advanceWatermark(c.r, c.through, c.pending, asked)
			if !through.Equal(c.wantThrough) {
				t.Errorf("through = %v, want %v", through, c.wantThrough)
			}
			switch c.wantPending {
			case "":
				if pending != nil {
					t.Errorf("a sample was left waiting: %+v", pending)
				}
			case "kept":
				if pending != c.pending {
					t.Errorf("pending = %+v, want the sample that was waiting (%+v)", pending, c.pending)
				}
			default:
				if pending == nil || pending.executed != c.wantPending || !pending.at.Equal(asked) {
					t.Errorf("pending = %+v, want what the source had (%s) when this read began", pending, c.wantPending)
				}
			}
			if through.IsZero() == (why == "") {
				t.Errorf("through %v with why %q: no watermark must say why, and a watermark needs no excuse", through, why)
			}
			if !strings.HasPrefix(why, c.wantWhy) {
				t.Errorf("why = %q, want prefix %q", why, c.wantWhy)
			}
		})
	}
}

// The reporter end to end, with its reads replaced: a busy source has no
// watermark on the first read, and the instant of that first read on the
// second, once capture's position includes what the source had then. The
// instant is the one BEFORE the read began.
func TestCaptureWatermark_busySourceNeedsTwoReads_2085(t *testing.T) {
	now := captureT0
	reads := []captureProbeResult{
		behindRead(uuidB+":1-10", uuidB+":1-30", captureT0),
		behindRead(uuidB+":1-35", uuidB+":1-60", captureT0),
		{detail: "the source did not answer", cause: "dial tcp: i/o timeout"},
	}
	n := 0
	c := newCaptureStatusReporter("")
	c.now = func() time.Time { return now }
	c.read = func(context.Context, string, string) captureProbeResult {
		// The read takes a second: the watermark is when it BEGAN.
		now = now.Add(time.Second)
		r := reads[n]
		n++
		return r
	}
	e := console.ServerEntry{ID: "a1", Name: "prod", DSN: "idx", SourceDSN: "src"}

	first := c.CaptureWatermark(context.Background(), e)
	if !first.Through.IsZero() || !strings.HasPrefix(first.Detail, "the source is ahead") {
		t.Fatalf("first read: %+v, want no watermark and why", first)
	}
	// Within the answer's lifetime nothing is read again.
	now = now.Add(5 * time.Second)
	if again := c.CaptureWatermark(context.Background(), e); !again.Through.IsZero() || n != 1 {
		t.Fatalf("within the TTL: %+v after %d reads, want the same answer and no new read", again, n)
	}
	now = now.Add(captureStatusTTL)
	second := c.CaptureWatermark(context.Background(), e)
	if !second.Through.Equal(captureT0) {
		t.Fatalf("second read: through = %v, want the instant the first read began (%v)", second.Through, captureT0)
	}
	// A read that fails keeps what was proven.
	now = now.Add(captureStatusTTL)
	third := c.CaptureWatermark(context.Background(), e)
	if !third.Through.Equal(captureT0) || n != 3 {
		t.Fatalf("after a failed read: through = %v (%d reads), want it kept", third.Through, n)
	}
	// An edited server starts over: the proof was about another source.
	e.SourceDSN = "other"
	reads = append(reads, captureProbeResult{detail: "the source did not answer"})
	if edited := c.CaptureWatermark(context.Background(), e); !edited.Through.IsZero() {
		t.Fatalf("after the source DSN changed: through = %v, want none", edited.Through)
	}
}

// What cannot have a watermark says so without a read, and the capture's
// filters travel with the answer.
func TestCaptureWatermark_noWatermarkAndFilters_2085(t *testing.T) {
	c := newCaptureStatusReporter("boot-src").withBootFilters("shop,crm", "shop.orders")
	c.now = func() time.Time { return captureT0 }
	c.read = func(context.Context, string, string) captureProbeResult {
		return behindRead(uuidB+":1-10", uuidB+":1-10", captureT0)
	}
	c.readMariaDB = func(context.Context, string, string) captureProbeResult {
		t.Fatal("a MariaDB source was read for a watermark")
		return captureProbeResult{}
	}
	maria := console.ServerEntry{ID: "m1", DSN: "idx", SourceDSN: "src", Flavor: console.FlavorMariaDB}
	if wm := c.CaptureWatermark(context.Background(), maria); !wm.Through.IsZero() || !strings.Contains(wm.Detail, "MariaDB") {
		t.Errorf("MariaDB source: %+v, want no watermark", wm)
	}
	if wm := c.CaptureWatermark(context.Background(), console.ServerEntry{ID: "n1", DSN: "idx"}); !wm.Through.IsZero() || wm.Detail == "" {
		t.Errorf("a server with no source: %+v, want no watermark and why", wm)
	}
	// A registry server: its own schema filter, no table filter.
	reg := c.CaptureWatermark(context.Background(), console.ServerEntry{ID: "a1", DSN: "idx", SourceDSN: "src", Schemas: "shop"})
	if reg.Through.IsZero() || reg.Captures == nil || !reg.Captures("shop", "lines") || reg.Captures("crm", "people") {
		t.Errorf("registry server filtered to shop: %+v", reg)
	}
	// No filter at all: every table.
	if all := c.CaptureWatermark(context.Background(), console.ServerEntry{ID: "a2", DSN: "idx", SourceDSN: "src"}); all.Captures != nil {
		t.Error("a capture with no filter must report it records every table (nil Captures)")
	}
	// The daemon's own capture: its flags, both dimensions.
	boot := c.CaptureWatermark(context.Background(), console.ServerEntry{ID: bootCaptureServerID, DSN: "idx"})
	if boot.Through.IsZero() || boot.Captures == nil || !boot.Captures("shop", "orders") || boot.Captures("shop", "lines") || boot.Captures("erp", "orders") {
		t.Errorf("boot capture filtered to shop.orders: %+v", boot)
	}
}
