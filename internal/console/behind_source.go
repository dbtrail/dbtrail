package console

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/go-sql-driver/mysql"
)

// How far behind the source the data of the copy's last answer was (#2556),
// read like MySQL's Seconds_Behind_Source: one number, the copy's distance to
// the newest indexed data plus capture's distance to the source, 0 when
// capture is caught up.

// captureHeartbeatPeriod is the replication heartbeat period capture asks
// the source for (streamrun.SourceHeartbeatPeriod; a test pins them equal).
// An idle source gets a caught-up stamp about once a period, written at the
// next checkpoint.
const captureHeartbeatPeriod = 30 * time.Second

// caughtUpWindow is how close to an answer a caught-up stamp must be for
// capture to count as caught up when the answer ran. Two periods: an idle
// source is stamped about once a period, plus up to a checkpoint interval
// (10 s by default) before the stamp is written; shorter would flip an idle
// source between the two formulas. With --checkpoint above 30 s an idle
// source can read as behind, the safe direction.
//
// The bound this leaves, said plainly: for up to one window after capture
// stops, or falls behind under a burst, the number shows only the copy's
// distance to the newest indexed data. Past it the formula is ran − through,
// which grows with every answer and never freezes. That is the same shape as
// Seconds_Behind_Source before the replica notices its source went quiet.
const caughtUpWindow = 2 * captureHeartbeatPeriod

// captureSample is what the router last read of one server's capture state:
// when capture was last caught up, on the ROUTER's clock (zero when never:
// no stamp, an older capture build, a PostgreSQL source), and the source
// event time of the newest indexed event (zero when none). Read together,
// once per loop, so a newer newest never meets an older stamp.
type captureSample struct {
	caughtUp, newest, readAt time.Time
}

// behindSourceOf is how far behind the source an answer that ran at ran and
// read data through through was. Capture caught up within the window around
// ran: newest − through, never below zero (last_event_time is whole seconds,
// and a snapshot can be cut past it). Otherwise ran − through.
func behindSourceOf(ran, through time.Time, s captureSample) time.Duration {
	if !s.caughtUp.IsZero() {
		if d := ran.Sub(s.caughtUp); d <= caughtUpWindow && d >= -caughtUpWindow {
			// A zero newest (nothing indexed) is far in the past: 0.
			return max(s.newest.Sub(through), 0)
		}
	}
	return copyAgeOf(ran, through)
}

// readCaptureSample reads stream_state from a server's index. The stamp is
// read as its AGE on the index's own clock and placed on the router's at the
// moment of the read (now), so the two machines' clocks are never compared;
// last_event_time is a source event time, like the copy's through. The
// router never migrates the index: a caught_up_at column that is not there
// yet (error 1054) is no stamp, not an error.
func readCaptureSample(ctx context.Context, db *sql.DB, now func() time.Time) (captureSample, error) {
	var ago sql.NullInt64
	var newest sql.NullTime
	err := db.QueryRowContext(ctx, `SELECT TIMESTAMPDIFF(MICROSECOND, caught_up_at, UTC_TIMESTAMP(6)), last_event_time FROM stream_state WHERE id = 1`).Scan(&ago, &newest)
	if me := (*mysql.MySQLError)(nil); errors.As(err, &me) && me.Number == 1054 {
		ago = sql.NullInt64{}
		err = db.QueryRowContext(ctx, `SELECT last_event_time FROM stream_state WHERE id = 1`).Scan(&newest)
	}
	s := captureSample{readAt: now()}
	if errors.Is(err, sql.ErrNoRows) {
		return s, nil
	}
	if err != nil {
		return captureSample{}, err
	}
	if ago.Valid {
		s.caughtUp = s.readAt.Add(-time.Duration(ago.Int64) * time.Microsecond)
	}
	if newest.Valid {
		s.newest = newest.Time.UTC()
	}
	return s, nil
}
