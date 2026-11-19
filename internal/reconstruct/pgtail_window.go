package reconstruct

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/query"
)

// What the source of a PostgreSQL table's tail (#2756) needs of this
// package's PostgreSQL fold, by its own name: the fold's rules are the
// tail's, and a second copy of one would drift from it.

// PGLSNText is an LSN as the index spells it in an event's position.
func PGLSNText(lsn uint64) string { return lsnText(lsn) }

// PGRowLSN is an event's commit LSN; ok is false for an event that carries
// none (a binary log position, or a position whose two spellings disagree).
func PGRowLSN(ev *query.ResultRow) (uint64, bool) { return pgRowLSN(ev) }

// PGCheckpoint is what a PostgreSQL capture last saved: the commit LSN of
// the last complete transaction, and the newest event at or before it when a
// transaction last ended (stream_state.pg_checkpoint_event_id). No event up
// to eventID has an LSN between that event's own and lsn, and among the
// events up to eventID whose LSN is at or before its own, every transaction
// is there delivered whole at least once: an earlier delivery of one, cut
// short by a restart, can be there too, before the whole one. Rows above
// eventID may be a transaction still arriving, or one arriving a second
// time after a restart, under the same LSN.
//
// ok is false when the index records no such pair: no PostgreSQL capture
// yet, one that has not ended a transaction since it began to record the
// event, an index of another family, or an index no capture of this version
// has opened (the column is not there).
func PGCheckpoint(ctx context.Context, db *sql.DB) (lsn, eventID uint64, ok bool, err error) {
	var pos, id sql.NullInt64
	err = db.QueryRowContext(ctx, `SELECT binlog_position, pg_checkpoint_event_id FROM stream_state WHERE id = 1 AND flavor = 'postgres'`).Scan(&pos, &id)
	var missing *mysql.MySQLError
	switch {
	case err == sql.ErrNoRows:
		return 0, 0, false, nil
	case errors.As(err, &missing) && missing.Number == 1054:
		// Unknown column: the capture that adds it has not opened this index.
		return 0, 0, false, nil
	case err != nil:
		return 0, 0, false, fmt.Errorf("read the capture's saved position: %w", err)
	case !pos.Valid || pos.Int64 <= 0 || !id.Valid || id.Int64 <= 0:
		return 0, 0, false, nil
	}
	return uint64(pos.Int64), uint64(id.Int64), true, nil
}

// PGSentThrough is the position below which every commit of the source is
// in the index, as the PostgreSQL capture last saved it
// (stream_state.pg_sent_through_lsn, #2746): the walsender's own position,
// which moves on a source with no commit too. ok is false when the index
// records none it can be held to: no PostgreSQL capture, one that has not
// received a position since it started, a save by a writer that does not set
// it (an older capture, a reset: the two times differ then), or an index no
// capture of that version has opened.
func PGSentThrough(ctx context.Context, db *sql.DB) (lsn uint64, ok bool, err error) {
	var pos sql.NullInt64
	err = db.QueryRowContext(ctx, `SELECT IF(pg_sent_through_at <=> last_checkpoint, pg_sent_through_lsn, NULL)
		FROM stream_state WHERE id = 1 AND flavor = 'postgres'`).Scan(&pos)
	var missing *mysql.MySQLError
	switch {
	case err == sql.ErrNoRows:
		return 0, false, nil
	case errors.As(err, &missing) && missing.Number == 1054:
		return 0, false, nil
	case err != nil:
		return 0, false, fmt.Errorf("read how far the capture has indexed the source: %w", err)
	case !pos.Valid || pos.Int64 <= 0:
		return 0, false, nil
	}
	return uint64(pos.Int64), true, nil
}

// PGShapeKey checks the shapes the stream recorded for schema.table from
// floor up to cut against rel, the shape its snapshot recorded, as the fold
// does (checkPGShapeKey): the table made again, renamed, a column added,
// dropped or retyped, a key that changed inside the window. It returns the
// key those shapes name, nil when they name none.
func PGShapeKey(schema, table string, rel baseline.PGRelation, floor, cut uint64, shapes []metadata.PGShape) ([]string, error) {
	return checkPGShapeKey(schema, table, rel, floor, cut, shapes)
}

// PGNeverPartitionedBefore is the hour before which a read of a PostgreSQL
// table's window may go through hours the index never had a partition for
// (pgNeverPartitionedBefore), and the zero time when it may not.
func PGNeverPartitionedBefore(ctx context.Context, db *sql.DB, dbName, schema, table string, floor uint64) time.Time {
	return pgNeverPartitionedBefore(ctx, db, dbName, schema, table, floor)
}
