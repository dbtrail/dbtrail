package console

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/status"
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

// captureSample is what the router last read of one server's capture state,
// all of it in one read so a newer part never meets an older one:
//   - caughtUp: when capture was last caught up, on the ROUTER's clock (zero
//     when never: no stamp, an older capture build, a PostgreSQL source);
//   - maxID: the id of the newest event in the index (0 when none, or when
//     ids are not the index's order: an index `bintrail index` also wrote);
//   - newest: the source event time of the newest indexed event (the latest
//     event_timestamp, not stream_state.last_event_time, which is the LAST
//     event read and goes back with a transaction that began early); read
//     only when the stamp is recent, the only time it is used, and zero
//     otherwise;
//   - readAt: when it was read, on the router's clock.
type captureSample struct {
	caughtUp, newest, readAt time.Time
	maxID                    uint64
}

// behindSourceOf is how far behind the source an answer that ran at ran was,
// its data holding every indexed event through through (a source event
// time) and through the event throughID (0 = unknown).
//
// Capture not caught up within the window around ran: ran − through.
// Caught up:
//   - the answer held every event the index held when the sample was read,
//     after the answer ran: 0;
//   - otherwise newest − through when that is positive: events the answer
//     did not read are that much newer than its data;
//   - otherwise ran − through. Events are missing but their times do not
//     say by how much: a transaction that began before the data's instant
//     and committed after carries an older time. Said as the conservative
//     distance, never as 0.
func behindSourceOf(ran, through time.Time, throughID uint64, s captureSample) time.Duration {
	if s.caughtUp.IsZero() {
		return copyAgeOf(ran, through)
	}
	if d := ran.Sub(s.caughtUp); d > caughtUpWindow || d < -caughtUpWindow {
		return copyAgeOf(ran, through)
	}
	// An unknown answer id (0) never reaches a known maxID (at least 1).
	if s.maxID != 0 && throughID >= s.maxID && !s.readAt.Before(ran) {
		return 0
	}
	// A zero newest (nothing indexed) is far in the past: not positive.
	if d := s.newest.Sub(through); d > 0 {
		return d
	}
	return copyAgeOf(ran, through)
}

// leastEventID is the least of ids, each the event a part of an answer
// holds every event through, so the answer holds every event through it;
// 0 (unknown) when there are none or any is unknown.
func leastEventID(ids ...uint64) uint64 {
	if len(ids) == 0 {
		return 0
	}
	least := ids[0]
	for _, id := range ids[1:] {
		least = min(least, id)
	}
	return least
}

// readCaptureSample reads a server's capture state from its index (dbName).
// The stamp is read as its AGE on the index's own clock and placed on the
// router's at the moment of the read (now), so the two machines' clocks are
// never compared; newest is a source event time, like the copy's through.
// The router never migrates the index: a caught_up_at column that is not
// there yet (error 1054) is no stamp, not an error.
func readCaptureSample(ctx context.Context, db *sql.DB, dbName string, now func() time.Time) (captureSample, error) {
	var ago sql.NullInt64
	err := db.QueryRowContext(ctx, `SELECT TIMESTAMPDIFF(MICROSECOND, caught_up_at, UTC_TIMESTAMP(6)) FROM stream_state WHERE id = 1`).Scan(&ago)
	if me := (*mysql.MySQLError)(nil); errors.As(err, &me) && me.Number == 1054 {
		ago, err = sql.NullInt64{}, nil
	}
	s := captureSample{readAt: now()}
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return s, nil
	case err != nil:
		return captureSample{}, err
	case !ago.Valid:
		return s, nil
	}
	s.caughtUp = s.readAt.Add(-time.Duration(ago.Int64) * time.Microsecond)
	if d := s.readAt.Sub(s.caughtUp); d > caughtUpWindow+captureReadEvery {
		// Too old to put any answer this sample serves inside the window:
		// nothing else is needed (and the newest probe is spared on a busy
		// source, whose stamp is always old).
		return s, nil
	}
	if !indexBackfilled(ctx, db) {
		var id sql.NullInt64
		err := db.QueryRowContext(ctx, `SELECT event_id FROM binlog_events FORCE INDEX (PRIMARY) ORDER BY event_id DESC LIMIT 1`).Scan(&id)
		switch {
		case errors.Is(err, sql.ErrNoRows):
		case err != nil:
			return captureSample{}, err
		case id.Valid && id.Int64 > 0:
			s.maxID = uint64(id.Int64)
		}
	}
	newest, err := status.NewestIndexedEvent(ctx, db, dbName)
	if err != nil {
		return captureSample{}, err
	}
	if !newest.IsZero() {
		s.newest = newest.UTC()
	}
	return s, nil
}
