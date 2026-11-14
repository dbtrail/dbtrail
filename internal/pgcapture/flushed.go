package pgcapture

import (
	"context"
	"fmt"

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
	var text *string
	// CASE evaluates one branch: pg_current_wal_flush_lsn() raises on a
	// standby, pg_last_wal_replay_lsn() is NULL on a primary.
	err := conn.QueryRow(ctx, `SELECT CASE WHEN pg_is_in_recovery()
		THEN pg_last_wal_replay_lsn() ELSE pg_current_wal_flush_lsn() END::text`).Scan(&text)
	if err != nil {
		return 0, fmt.Errorf("pgcapture: read the flushed WAL position: %w", err)
	}
	if text == nil {
		return 0, fmt.Errorf("pgcapture: the source reported no WAL position")
	}
	lsn, err := pglogrepl.ParseLSN(*text)
	if err != nil {
		return 0, fmt.Errorf("pgcapture: the source's WAL position %q does not parse: %w", *text, err)
	}
	return uint64(lsn), nil
}
