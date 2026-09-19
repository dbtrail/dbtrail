package indexer

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	mysql "github.com/go-sql-driver/mysql"
)

// DefaultRotateRetain is the built-in rotation's retention for an index whose
// operator never sets one (#1709). CreateIndexTables records it on a NEW index,
// and that record, not this constant, is what the rotation loop reads from
// then on: changing this value moves the indexes created afterwards and never
// one that already exists.
//
// 48 hours, lowered from 30 days in the release this comment ships in. The
// index is a change log that grows with the source's write rate, so at 30 days
// nobody who did not set the flag was protected from filling the disk:
// measured at ~180 transactions a second, 13 GB of binlog_events per hour, 158
// GB after two days on a 200 GB disk, with rotation running every cycle and
// dropping nothing, because two days is less than thirty. The restore path
// does not need a month of the LIVE index — a backup update folds from the
// newest backup forward, and an archive tier keeps everything older as Parquet
// that query and reconstruct read anyway. 48 rather than 12 hours so a chain
// of table deltas (capped at 24h) plus a day of margin fits inside it out of
// the box.
const DefaultRotateRetain = "48h"

// DDLRotationPolicy is the single-row record of the retention an index started
// with. The CHECK constraint needs its own name: MySQL scopes those names to
// the database, and stream_state already uses single_row.
const DDLRotationPolicy = `CREATE TABLE IF NOT EXISTS rotation_policy (
    id             INT UNSIGNED PRIMARY KEY DEFAULT 1,
    initial_retain VARCHAR(16)  NOT NULL COMMENT 'built-in rotation retention this index was created under (#1709); read only while the operator sets none. No row = an index created before this record existed',
    recorded_at    DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT rotation_policy_single_row CHECK (id = 1)
) ENGINE=InnoDB`

// recordInitialRetainIfNew writes DefaultRotateRetain when binlog_events does
// not exist yet, i.e. when this call is creating the index.
//
// It runs BEFORE binlog_events is created, on purpose. Written after it, an init
// that failed between the two statements would leave an index with events and
// no record, which every later run reads as "created before the record" and
// keeps on the old retention forever. Written first, a retry still finds
// binlog_events missing and INSERT IGNORE leaves the earlier row alone.
//
// An existing binlog_events gets NO row: that absence is how the rotation loop
// recognises an index an older build created.
func recordInitialRetainIfNew(ctx context.Context, db *sql.DB) error {
	var n int
	if err := db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM information_schema.TABLES
		  WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'binlog_events'`).Scan(&n); err != nil {
		return fmt.Errorf("check whether binlog_events exists: %w", err)
	}
	if n > 0 {
		return nil
	}
	if _, err := db.ExecContext(ctx,
		"INSERT IGNORE INTO rotation_policy (id, initial_retain) VALUES (1, ?)", DefaultRotateRetain); err != nil {
		return fmt.Errorf("record the initial rotation retention: %w", err)
	}
	return nil
}

// ReadInitialRetain returns the retention recorded when the index was created,
// and WHEN it was recorded. found is false when there is no record, which is
// also what a missing table means: both are an index an older build created.
// The value comes back as stored (trimmed); parsing it is the caller's job.
//
// recordedAt is not decoration: it is what separates an index whose history
// accumulated under the recorded window from one that was created empty and
// then FILLED with older history (restore-index rebuilding an index from the
// archives, or `bintrail index` over months of old binlog files). The rotation
// loop needs that difference — see rotation.rotateOneIndex.
func ReadInitialRetain(ctx context.Context, db *sql.DB, dbName string) (retain string, recordedAt time.Time, found bool, err error) {
	err = db.QueryRowContext(ctx,
		"SELECT initial_retain, recorded_at FROM `"+dbName+"`.rotation_policy WHERE id = 1").Scan(&retain, &recordedAt)
	var me *mysql.MySQLError
	switch {
	case err == nil:
		return strings.TrimSpace(retain), recordedAt, true, nil
	case errors.Is(err, sql.ErrNoRows):
		return "", time.Time{}, false, nil
	case errors.As(err, &me) && me.Number == 1146: // ER_NO_SUCH_TABLE
		return "", time.Time{}, false, nil
	default:
		return "", time.Time{}, false, fmt.Errorf("read rotation_policy: %w", err)
	}
}
