package shim

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/dbtrail/dbtrail/internal/readrouter"
)

// clockProbeSQL reads the source session's clock (#2611): @@timestamp, which
// is NOW() there, less SYSDATE(), which SET timestamp does not move, and
// @@timestamp itself.
const clockProbeSQL = "SELECT @@session.timestamp - UNIX_TIMESTAMP(SYSDATE(6)), @@session.timestamp"

// clockKeepsCopyFromAnswering says why the copy must not answer a routed
// statement that calls NOW(), CURDATE(), UTC_TIMESTAMP() or UTC_DATE()
// because of the source's clock, or "" when it may. The copy answers them from
// this host's clock, so the source's NOW() must be the time there and this
// host's must be close to it. Asked before every such statement, not once
// with the session: a session that sets its clock to the present
// (SET timestamp = UNIX_TIMESTAMP()) reads as the present when it is asked and
// falls behind from then on. A probe that cannot be read keeps the statement
// on MySQL.
func (h *Handler) clockKeepsCopyFromAnswering(ctx context.Context) string {
	var buf readrouter.BufferSink
	if _, err := h.router.Forward(ctx, clockProbeSQL, &buf); err != nil {
		return "the source's clock could not be read (" + shortErr(err) + ")"
	}
	if len(buf.Rows) != 1 || len(buf.Rows[0]) != 2 {
		return fmt.Sprintf("the source answered %d rows for its clock", len(buf.Rows))
	}
	lag, ok1 := cellText(buf.Rows[0][0])
	ts, ok2 := cellText(buf.Rows[0][1])
	if !ok1 || !ok2 {
		return "the source answered NULL for its clock"
	}
	return clockVerdict(lag, ts, time.Now())
}

// clockVerdict reads the probe: lag is @@timestamp less SYSDATE() on the
// source, ts @@timestamp, both in seconds. A lag past a second is a clock the
// session set. With no lag, @@timestamp is still held to this host's clock,
// for a source whose SYSDATE() is NOW() (--sysdate-is-now), where the two move
// together, and for two hosts whose clocks disagree: the copy's NOW() would
// be that far from MySQL's.
func clockVerdict(lag, ts string, now time.Time) string {
	l, err1 := strconv.ParseFloat(strings.TrimSpace(lag), 64)
	v, err2 := strconv.ParseFloat(strings.TrimSpace(ts), 64)
	if err1 != nil || err2 != nil {
		return fmt.Sprintf("the source's clock could not be read (%q, %q)", lag, ts)
	}
	if math.Abs(l) > 1 {
		return "the session's clock is set (SET timestamp), so NOW() on MySQL is that moment, and the copy reads its own clock"
	}
	if off := v - float64(now.UnixMicro())/1e6; math.Abs(off) > clockSkewMax.Seconds() {
		return fmt.Sprintf("the source's clock reads %.0f s from this host's (two clocks that disagree, or SET timestamp on a source started with --sysdate-is-now), "+
			"and the copy's NOW() reads this host's", off)
	}
	return ""
}

// clockSkewMax is how far the source's clock may be from this host's for the
// copy to answer NOW() from its own. A second: a report of "the last N days"
// is as of a moment either way, and two hosts kept by NTP sit well inside it.
const clockSkewMax = time.Second
