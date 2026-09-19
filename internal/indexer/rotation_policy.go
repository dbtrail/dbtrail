package indexer

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	mysql "github.com/go-sql-driver/mysql"
)

// DefaultRotateRetain is the built-in rotation's retention for an index whose
// operator never sets one (#1709). CreateIndexTables records it on a NEW index,
// and that record, not this constant, is what the rotation loop reads from
// then on: changing this value moves the indexes created afterwards and never
// one that already exists.
const DefaultRotateRetain = "30d"

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

// ReadInitialRetain returns the retention recorded when the index was created.
// found is false when there is no record, which is also what a missing table
// means: both are an index an older build created. The value comes back as
// stored (trimmed); parsing it is the caller's job.
func ReadInitialRetain(ctx context.Context, db *sql.DB, dbName string) (retain string, found bool, err error) {
	err = db.QueryRowContext(ctx,
		"SELECT initial_retain FROM `"+dbName+"`.rotation_policy WHERE id = 1").Scan(&retain)
	var me *mysql.MySQLError
	switch {
	case err == nil:
		return strings.TrimSpace(retain), true, nil
	case errors.Is(err, sql.ErrNoRows):
		return "", false, nil
	case errors.As(err, &me) && me.Number == 1146: // ER_NO_SUCH_TABLE
		return "", false, nil
	default:
		return "", false, fmt.Errorf("read rotation_policy: %w", err)
	}
}
