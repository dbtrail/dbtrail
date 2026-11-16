package reconstruct

import (
	"context"
	"database/sql"
	"fmt"
	"time"

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

// PGCheckpoint is the commit LSN of the last complete transaction a
// PostgreSQL capture saved: what its events are whole up to. ok is false
// when the index records none (no capture yet, or another family's).
func PGCheckpoint(ctx context.Context, db *sql.DB) (lsn uint64, ok bool, err error) {
	var pos sql.NullInt64
	err = db.QueryRowContext(ctx, `SELECT binlog_position FROM stream_state WHERE id = 1 AND flavor = 'postgres'`).Scan(&pos)
	switch {
	case err == sql.ErrNoRows:
		return 0, false, nil
	case err != nil:
		return 0, false, fmt.Errorf("read the capture's saved position: %w", err)
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
