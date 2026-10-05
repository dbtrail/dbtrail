package consoleapp

import (
	"context"
	"time"

	"github.com/dbtrail/dbtrail/internal/cliutil"
	"github.com/dbtrail/dbtrail/internal/console"
	gomysql "github.com/go-mysql-org/go-mysql/mysql"
)

// How far capture is known to have read the source (#2085).
//
// The capture status (capture_status.go) answers "is capture caught up right
// now", which on a source that is being written is almost never yes: the
// capture saves its position every few seconds and the source has moved on by
// the time the two are compared. The read router needs less and can have it
// on a busy source too: an instant, not long ago, at which capture is known
// to have held everything the source had executed. That is this file's
// watermark, and it comes from the same reads:
//
//   - a read that finds the two GTID sets equal proves it for the instant
//     that read began;
//   - a read that finds the source ahead leaves a sample (what the source had
//     executed, and when it was asked). A later read whose capture position
//     includes that sample proves it for the instant the sample was asked
//     for, however far ahead the source is by then.
//
// What "includes" rests on: the stream flushes a batch before it saves its
// position, and a transaction enters the saved GTID set at its commit, so
// every row of every transaction in that set is in the index (rows the
// capture dropped are not, which the index records apart and the caller
// checks).
//
// The watermark only moves forward, and it is dropped when the two sets stop
// being comparable (the capture holds a transaction the source does not, so
// the source was reset or replaced; a tagged GTID, which capture does not
// record). A read that learns nothing about the source leaves it as it was:
// what was proven stays proven, and it grows old on its own.
//
// Nothing here runs on a timer. A read happens when somebody asks and the
// last answer is older than captureStatusTTL, so the first statement after a
// quiet spell on a busy source finds no watermark and leaves the sample the
// next one is measured against.

// withBootFilters sets the filters of the daemon's own capture.
func (c *captureStatusReporter) withBootFilters(schemas, tables string) *captureStatusReporter {
	c.bootSchemas, c.bootTables = schemas, tables
	return c
}

// advanceWatermark is the watermark after one read r: through and pending
// are the slot's, asked when this read began. Pure.
func advanceWatermark(r captureProbeResult, through time.Time, pending *captureSample, asked time.Time) (time.Time, *captureSample, string) {
	if r.executed == "" {
		// The source was not read, or reported no GTID set: nothing was
		// learned about it.
		why := r.detail
		if why == "" {
			why = "the source was not read"
		}
		if !through.IsZero() {
			why = ""
		}
		return through, pending, why
	}
	have, wrote, ok := parseGTIDPair(r.captured, r.executed)
	if !ok {
		return time.Time{}, nil, "the GTID sets do not parse"
	}
	if hasTaggedGTIDs(wrote) {
		return time.Time{}, nil, "the source has tagged GTIDs, which capture does not record"
	}
	// Strictly what capture holds. The capture status counts what the source
	// purged as reachable so as not to call a healthy capture behind; here
	// the same allowance would call a table unchanged over transactions no
	// binlog carried to the index. A source that executed transactions
	// capture never read (a dump loaded with SET @@GLOBAL.gtid_purged) has
	// no watermark, ever.
	if !wrote.Contain(have) {
		return time.Time{}, nil, "the capture holds transactions the source does not have: the source may have been reset, restored or replaced"
	}
	if pending != nil {
		earlier, err := gomysql.ParseMysqlGTIDSet(pending.executed)
		if err != nil {
			pending = nil
		} else if have.Contain(earlier) {
			if pending.at.After(through) {
				through = pending.at
			}
			pending = nil
		}
	}
	if have.Equal(wrote) {
		// Capture holds everything the source has now, so everything it had
		// when this read began.
		if asked.After(through) {
			through = asked
		}
		pending = nil
	} else if pending == nil {
		pending = &captureSample{executed: r.executed, at: asked}
	}
	why := ""
	if through.IsZero() {
		why = "the source is ahead of capture's saved position, and no later read has confirmed capture reached it yet"
	}
	return through, pending, why
}

// CaptureWatermark is console.CaptureWatermarkReporter: the capture status
// read for e (kept and single-flight like every other), and the watermark
// its reads have established.
func (c *captureStatusReporter) CaptureWatermark(ctx context.Context, e console.ServerEntry) console.CaptureWatermark {
	_, flavor, _ := c.sourceOf(e)
	if flavor == console.FlavorMariaDB {
		// Its read compares positions per domain and keeps no sample; until
		// it does, a MariaDB source has no watermark.
		return console.CaptureWatermark{Detail: "a MariaDB source is not compared for this yet"}
	}
	status := c.CaptureStatus(ctx, e)
	c.mu.Lock()
	var through time.Time
	why := ""
	if slot := c.slots[e.ID]; slot != nil {
		through, why = slot.through, slot.throughWhy
	}
	c.mu.Unlock()
	wm := console.CaptureWatermark{Through: through, Detail: why}
	if through.IsZero() && wm.Detail == "" {
		wm.Detail = status.Detail
	}
	schemas, tables := e.Schemas, ""
	if e.ID == bootCaptureServerID {
		schemas, tables = c.bootSchemas, c.bootTables
	}
	if schemas != "" || tables != "" {
		filters := cliutil.BuildIndexFilters(schemas, tables)
		wm.Captures = filters.Matches
	}
	return wm
}
