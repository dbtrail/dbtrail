package pgcapture

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
)

// SourceFlushedLSN asks a PostgreSQL source how far its WAL is flushed: the
// position a logical walsender reads up to, so the one a capture's
// sent-through position (stream_state.pg_sent_through_lsn, #2746) is compared
// with. On a standby it is the replayed position instead, which is as far as
// a walsender there can read.
//
// One short-lived plain connection and one read; the caller bounds it with
// ctx. Nothing is written and nothing is installed in the source.
func SourceFlushedLSN(ctx context.Context, queryDSN string) (uint64, error) {
	conn, err := pgx.Connect(ctx, queryDSN)
	if err != nil {
		return 0, fmt.Errorf("pgcapture: flushed position connect: %w", err)
	}
	defer conn.Close(context.Background())
	return QueryFlushedLSN(ctx, conn)
}

// QueryFlushedLSN is SourceFlushedLSN on a connection the caller holds.
func QueryFlushedLSN(ctx context.Context, conn *pgx.Conn) (uint64, error) {
	lsn, _, err := QueryFlushed(ctx, conn)
	return lsn, err
}

// QueryFlushed is QueryFlushedLSN with the source's own clock for that
// reading: a moment every change past the position committed after, on the
// clock commit times are on.
//
//   - On a primary it is the source's time now. A change whose commit is not
//     flushed yet committed about now or later.
//   - On a standby it is the commit time of the last transaction replayed
//     (pg_last_xact_replay_timestamp): what is not replayed yet is later in
//     the WAL, so it committed later on the primary. The standby's own time
//     would not do: replay can be hours behind (recovery_min_apply_delay, a
//     paused replay), and a change replayed later carries the primary's
//     commit time from before.
//
// Zero when the source has none to give (a standby that has replayed no
// transaction). Read in the same statement as the position, before it.
func QueryFlushed(ctx context.Context, conn *pgx.Conn) (lsn uint64, sourceAt time.Time, err error) {
	var text *string
	var at *time.Time
	// CASE evaluates one branch: pg_current_wal_flush_lsn() raises on a
	// standby, pg_last_wal_replay_lsn() is NULL on a primary.
	err = conn.QueryRow(ctx, `SELECT
		CASE WHEN pg_is_in_recovery() THEN pg_last_xact_replay_timestamp() ELSE clock_timestamp() END,
		CASE WHEN pg_is_in_recovery() THEN pg_last_wal_replay_lsn() ELSE pg_current_wal_flush_lsn() END::text`).Scan(&at, &text)
	if err != nil {
		return 0, time.Time{}, fmt.Errorf("pgcapture: read the flushed WAL position: %w", err)
	}
	if text == nil {
		return 0, time.Time{}, fmt.Errorf("pgcapture: the source reported no WAL position")
	}
	parsed, err := pglogrepl.ParseLSN(*text)
	if err != nil {
		return 0, time.Time{}, fmt.Errorf("pgcapture: the source's WAL position %q does not parse: %w", *text, err)
	}
	if at != nil {
		sourceAt = at.UTC()
	}
	return uint64(parsed), sourceAt, nil
}
