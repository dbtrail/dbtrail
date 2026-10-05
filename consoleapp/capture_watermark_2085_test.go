package consoleapp

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/go-sql-driver/mysql"
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
		r := behindRead(captured, executed, captureT0)
		r.logsAll = true
		return r
	}
	cases := []struct {
		name        string
		r           captureProbeResult
		through     time.Time
		pending     *captureSample
		last        string // the capture's saved set at the previous read
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

		// The capture's saved position went backward since the last read: it
		// was restarted from an earlier point, and what was proven is void.
		{name: "capture's position went backward: an older proof is dropped",
			r: read(uuidB+":1-15", uuidB+":1-40"), last: uuidB + ":1-30", through: before, pending: sample(uuidB+":1-35", before),
			wantPending: uuidB + ":1-40", wantWhy: "the source is ahead"},
		{name: "capture's position went backward and now equals the source: proven anew, as of this read",
			r: read(uuidB+":1-20", uuidB+":1-20"), last: uuidB + ":1-30", through: long, wantThrough: asked},
		{name: "capture's position moved forward: the proof stands",
			r: read(uuidB+":1-35", uuidB+":1-40"), last: uuidB + ":1-30", through: before, wantThrough: before, wantPending: uuidB + ":1-40"},

		// Nothing learned: what was proven stays, and ages.
		{name: "the source did not answer",
			r: captureProbeResult{detail: "the source did not answer"}, through: long, pending: sample(uuidB+":1-20", before),
			wantThrough: long, wantPending: "kept"},
		{name: "the source did not answer and nothing was ever proven",
			r: captureProbeResult{detail: "the source did not answer"}, wantWhy: "the source did not answer"},
		{name: "position mode", r: captureProbeResult{detail: "the capture runs in binlog-position mode, which is not compared"},
			wantWhy: "the capture runs in binlog-position mode"},
		{name: "a read that says nothing at all", r: captureProbeResult{}, wantWhy: "the source was not read"},
		// The checkpoint itself stopped being comparable (a --reset to
		// position mode, a capture stopped and its record gone): not the
		// same as a source that did not answer. What was proven was about a
		// capture that no longer exists in that form.
		{name: "the checkpoint is no longer comparable: what was proven is void",
			r:       captureProbeResult{detail: "the capture runs in binlog-position mode, which is not compared", uncomparable: true},
			through: long, pending: sample(uuidB+":1-20", before), last: uuidB + ":1-15",
			wantWhy: "the capture runs in binlog-position mode"},

		// No longer comparable: everything is dropped.
		{name: "the capture holds more than the source: the source was replaced",
			r: read(uuidB+":1-11", uuidB+":1-10"), through: before, pending: sample(uuidB+":1-10", before),
			wantWhy: "the capture holds transactions the source does not have"},
		{name: "tagged GTIDs on the source",
			r: read(uuidB+":1-10", uuidB+":1-10:audit:1-50"), through: before, wantWhy: "the source has tagged GTIDs"},
		{name: "sets that do not parse",
			r: captureProbeResult{captured: "0-1-100", executed: "0-1-200", logsAll: true}, through: before, wantWhy: "the GTID sets do not parse"},
		{name: "the source filters its binary log: equal sets prove nothing",
			r: func() captureProbeResult {
				r := read(uuidB+":1-10", uuidB+":1-10")
				r.logsAll, r.logFilter = false, "the source leaves some databases out of its binary log (binlog-ignore-db)"
				return r
			}(), through: before, pending: sample(uuidB+":1-5", before), wantWhy: "the source leaves some databases out"},
		{name: "the binary log's filters were not read: equal sets prove nothing",
			r: behindRead(uuidB+":1-10", uuidB+":1-10", captureT0), through: before, wantWhy: "the source's binary log filters were not read"},

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
			through, pending, why := advanceWatermark(c.r, c.through, c.pending, c.last, asked)
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
	logged := func(r captureProbeResult) captureProbeResult { r.logsAll = true; return r }
	reads := []captureProbeResult{
		logged(behindRead(uuidB+":1-10", uuidB+":1-30", captureT0)),
		logged(behindRead(uuidB+":1-35", uuidB+":1-60", captureT0)),
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
	// A capture reset to position mode: the proof does not outlive it, and
	// a later GTID capture starts from nothing (its first read finds the
	// source ahead, with no earlier saved set to measure against).
	now = now.Add(captureStatusTTL)
	reads = append(reads, captureProbeResult{detail: "the capture runs in binlog-position mode, which is not compared", uncomparable: true})
	if reset := c.CaptureWatermark(context.Background(), e); !reset.Through.IsZero() || !strings.Contains(reset.Detail, "position mode") {
		t.Fatalf("after the checkpoint stopped being comparable: %+v, want no watermark and why", reset)
	}
	now = now.Add(captureStatusTTL)
	reads = append(reads, logged(behindRead(uuidB+":1-5", uuidB+":1-70", captureT0)))
	if back := c.CaptureWatermark(context.Background(), e); !back.Through.IsZero() || !strings.HasPrefix(back.Detail, "the source is ahead") {
		t.Fatalf("a GTID capture again, behind the source: %+v, want no watermark yet", back)
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
		r := behindRead(uuidB+":1-10", uuidB+":1-10", captureT0)
		r.logsAll = true
		return r
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

// The clock is read before the source is (the watermark's instant). A
// failure there must release the read in flight like any other, or the next
// load waits on a channel nobody closes.
func TestCaptureStatus_aFailureBeforeTheReadHoldsNothing_2085(t *testing.T) {
	var reads, calls int
	c := newCaptureStatusReporter("")
	c.read = func(context.Context, string, string) captureProbeResult {
		reads++
		return captureProbeResult{verdict: console.CaptureCaughtUp, captured: uuidB + ":1-10", executed: uuidB + ":1-10"}
	}
	c.now = func() time.Time {
		calls++
		if calls == 1 { // the clock read before the source is asked
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
		if reads != 1 || got.State != console.CaptureStateUpToDate {
			t.Fatalf("the load after it: %+v, %d reads", got, reads)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the load after a failure waited on a read that was over")
	}
}

// The binary log's filters, read by column name from whichever statement the
// server has. Only "the statement answered and both are empty" is logsAll.
func TestReadBinlogFilters_2085(t *testing.T) {
	cols := []string{"File", "Position", "Binlog_Do_DB", "Binlog_Ignore_DB", "Executed_Gtid_Set"}
	row := func(do, ignore string) *sqlmock.Rows {
		return sqlmock.NewRows(cols).AddRow("binlog.000004", 4604766, do, ignore, "")
	}
	cases := []struct {
		name    string
		expect  func(m sqlmock.Sqlmock)
		logsAll bool
		why     string
	}{
		{"no filter (8.4)", func(m sqlmock.Sqlmock) {
			m.ExpectQuery("SHOW BINARY LOG STATUS").WillReturnRows(row("", ""))
		}, true, ""},
		{"no filter, on a server that only has the older statement", func(m sqlmock.Sqlmock) {
			m.ExpectQuery("SHOW BINARY LOG STATUS").WillReturnError(&mysql.MySQLError{Number: 1064, Message: "You have an error in your SQL syntax"})
			m.ExpectQuery("SHOW MASTER STATUS").WillReturnRows(row("", ""))
		}, true, ""},
		{"binlog-do-db", func(m sqlmock.Sqlmock) {
			m.ExpectQuery("SHOW BINARY LOG STATUS").WillReturnRows(row("shop", ""))
		}, false, "binlog-do-db"},
		{"binlog-ignore-db", func(m sqlmock.Sqlmock) {
			m.ExpectQuery("SHOW BINARY LOG STATUS").WillReturnRows(row("", "scratch,tmp"))
		}, false, "binlog-ignore-db"},
		{"the account may not ask", func(m sqlmock.Sqlmock) {
			denied := &mysql.MySQLError{Number: 1227, Message: "Access denied; you need (at least one of) the SUPER, REPLICATION CLIENT privilege(s)"}
			m.ExpectQuery("SHOW BINARY LOG STATUS").WillReturnError(denied)
			m.ExpectQuery("SHOW MASTER STATUS").WillReturnError(denied)
		}, false, "could not be read"},
		{"binary logging off: an empty answer", func(m sqlmock.Sqlmock) {
			m.ExpectQuery("SHOW BINARY LOG STATUS").WillReturnRows(sqlmock.NewRows(cols))
			m.ExpectQuery("SHOW MASTER STATUS").WillReturnError(&mysql.MySQLError{Number: 1064, Message: "syntax"})
		}, false, "could not be read"},
		{"an answer without the two columns is not 'no filter'", func(m sqlmock.Sqlmock) {
			two := sqlmock.NewRows([]string{"File", "Position"}).AddRow("binlog.000004", 4)
			m.ExpectQuery("SHOW BINARY LOG STATUS").WillReturnRows(two)
			m.ExpectQuery("SHOW MASTER STATUS").WillReturnRows(sqlmock.NewRows([]string{"File", "Position"}).AddRow("binlog.000004", 4))
		}, false, "could not be read"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			c.expect(mock)
			logsAll, why := readBinlogFilters(context.Background(), db)
			if logsAll != c.logsAll || !strings.Contains(why, c.why) || (logsAll && why != "") {
				t.Errorf("readBinlogFilters = %v, %q; want %v and %q", logsAll, why, c.logsAll, c.why)
			}
		})
	}
}
