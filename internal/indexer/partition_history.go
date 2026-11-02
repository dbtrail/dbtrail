package indexer

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/go-sql-driver/mysql"
)

// DDLPartitionHistory is the single-row record of what rotation has dropped
// from this index (#2606), so that a young index is not mistaken for a
// rotated one.
//
// binlog_events is RANGE-partitioned by hour and its oldest partition has no
// lower bound: it holds every event older than its own hour, including one a
// long statement stamped before the index existed. The oldest live
// partition's hour is a floor on what the index still holds only once some
// partition has been dropped. Without this record a reader cannot tell the
// two apart, and the console's tail and unchanged lookups (which need the
// index to hold every change from an hour before the snapshot) refused every
// table for the first hour or two of a new index.
//
// The times are UTC epoch seconds, never DATETIME: the index server would
// read a DATETIME in its own time zone.
const DDLPartitionHistory = `CREATE TABLE IF NOT EXISTS partition_history (
    id                 INT UNSIGNED PRIMARY KEY DEFAULT 1,
    history_known      TINYINT UNSIGNED NOT NULL COMMENT '1 = recorded from the creation of binlog_events, so every drop since is in dropped_below; 0 = the history before this row is unknown (a restored index). No row = an index created before this record existed (#2606)',
    created_unix       BIGINT UNSIGNED  NOT NULL COMMENT 'when binlog_events was created, UTC epoch seconds; its first partition is this hour',
    dropped_below_unix BIGINT UNSIGNED  DEFAULT NULL COMMENT 'upper bound of the highest partition rotation ever dropped, UTC epoch seconds, written BEFORE the drop; NULL = none dropped',
    CONSTRAINT partition_history_single_row CHECK (id = 1)
) ENGINE=InnoDB`

// recordPartitionHistoryIfNew writes the history of an index this call is
// creating: binlog_events does not exist yet. It runs BEFORE binlog_events is
// created, for the reason recordInitialRetainIfNew gives: written after, an
// init that failed between the two statements would leave an index whose
// later runs find binlog_events and never write the row. now is the instant
// the partitions are built from, so the first partition is now's hour.
//
// An existing binlog_events gets NO row: that absence is how a reader
// recognises an index an older build created, whose drops were never
// recorded. INSERT IGNORE keeps the row an earlier attempt, or a restore
// (MarkPartitionHistoryUnknown), already wrote.
func recordPartitionHistoryIfNew(ctx context.Context, db *sql.DB, now time.Time) error {
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
		"INSERT IGNORE INTO partition_history (id, history_known, created_unix) VALUES (1, 1, ?)", now.UTC().Unix()); err != nil {
		return fmt.Errorf("record the index's partition history: %w", err)
	}
	return nil
}

// MarkPartitionHistoryUnknown records that the index's history before now is
// not known: restore-index builds binlog_events from archives, which may not
// hold every hour. It creates the table if needed and runs BEFORE
// CreateIndexTables, whose INSERT IGNORE then leaves this row alone.
func MarkPartitionHistoryUnknown(ctx context.Context, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, DDLPartitionHistory); err != nil {
		return fmt.Errorf("create partition_history: %w", err)
	}
	if _, err := db.ExecContext(ctx,
		`INSERT INTO partition_history (id, history_known, created_unix) VALUES (1, 0, ?)
		 ON DUPLICATE KEY UPDATE history_known = 0`, time.Now().UTC().Unix()); err != nil {
		return fmt.Errorf("record the index's partition history as unknown: %w", err)
	}
	return nil
}

// MarkPartitionsDropped records, BEFORE rotation drops them, the upper bound
// of the highest of the named partitions. DDL commits on its own, so the two
// cannot share a transaction: a drop that fails after the mark leaves the
// mark too high, which only refuses more. An error means the drop must not
// run. An index with no record (or no table: an older build's) is left as it
// is: readers already treat it as one whose drops are unknown.
func MarkPartitionsDropped(ctx context.Context, db *sql.DB, dbName string, names []string) error {
	bound, err := droppedBound(names)
	if err != nil {
		return err
	}
	_, err = db.ExecContext(ctx,
		"UPDATE `"+dbName+"`.partition_history SET dropped_below_unix = GREATEST(COALESCE(dropped_below_unix, 0), ?) WHERE id = 1",
		bound.Unix())
	var me *mysql.MySQLError
	if errors.As(err, &me) && me.Number == 1146 { // ER_NO_SUCH_TABLE
		return nil
	}
	if err != nil {
		return fmt.Errorf("record the partitions about to be dropped: %w", err)
	}
	return nil
}

// droppedBound is the upper bound of the highest named partition: partition
// p_YYYYMMDDHH holds every event before the next hour. A name that is not an
// hourly partition is an error, so a drop this record cannot describe never
// runs.
func droppedBound(names []string) (time.Time, error) {
	if len(names) == 0 {
		return time.Time{}, errors.New("no partition to record as dropped")
	}
	var top time.Time
	for _, n := range names {
		d, ok := PartitionDate(n)
		if !ok {
			return time.Time{}, fmt.Errorf("partition %q is not an hourly partition, so its drop cannot be recorded", n)
		}
		if end := d.Add(time.Hour); end.After(top) {
			top = end
		}
	}
	return top, nil
}

// PartitionHistory is the index's partition_history row. Known is false when
// there is no row (an index an older build created), no table, or the row
// says the history is unknown (a restored index).
type PartitionHistory struct {
	Known        bool
	CreatedAt    time.Time
	DroppedBelow time.Time // zero: nothing dropped
}

// ReadPartitionHistory reads the index's partition history. A missing table
// or row is an index whose drops are unknown, not an error.
func ReadPartitionHistory(ctx context.Context, db *sql.DB, dbName string) (PartitionHistory, error) {
	var known int
	var created int64
	var dropped sql.NullInt64
	err := db.QueryRowContext(ctx,
		"SELECT history_known, created_unix, dropped_below_unix FROM `"+dbName+"`.partition_history WHERE id = 1").Scan(&known, &created, &dropped)
	var me *mysql.MySQLError
	switch {
	case errors.Is(err, sql.ErrNoRows), errors.As(err, &me) && me.Number == 1146:
		return PartitionHistory{}, nil
	case err != nil:
		return PartitionHistory{}, fmt.Errorf("read the index's partition history: %w", err)
	}
	h := PartitionHistory{Known: known == 1, CreatedAt: time.Unix(created, 0).UTC()}
	if dropped.Valid {
		h.DroppedBelow = time.Unix(dropped.Int64, 0).UTC()
	}
	return h, nil
}

// LossBound is the instant below which the index may have lost events, given
// its oldest live partition's hour and since, the earliest instant a lookup
// needs from (on the source's clock). ok is false when there is no live
// partition to measure from. A zero bound with ok means nothing was lost: the
// oldest partition has no lower bound and holds every older event.
//
// The record decides only for an index it describes from its start, created
// no later than since, whose oldest partition is still the one it was
// created with (its creation hour). Otherwise the bound is the oldest
// partition's hour, the rule before the record existed:
//   - no record, or history unknown: an older build's index, or a restored one;
//   - created after since: emptied and created again after the snapshot, so
//     the events between are not in it;
//   - the oldest partition moved: dropped by something that did not record
//     it (an older build's rotate, a hand-run DROP PARTITION), or a partition
//     was added below the first.
//
// A recorded drop counts whatever partition it took, so a middle partition's
// drop is covered too.
func (h PartitionHistory) LossBound(oldest, since time.Time) (time.Time, bool) {
	if oldest.IsZero() {
		return time.Time{}, false
	}
	if !h.Known || since.IsZero() || h.CreatedAt.After(since) {
		return oldest, true
	}
	bound := h.DroppedBelow
	if first := h.CreatedAt.UTC().Truncate(time.Hour); !oldest.Equal(first) {
		bound = slices.MaxFunc([]time.Time{bound, oldest}, time.Time.Compare)
	}
	return bound, true
}
