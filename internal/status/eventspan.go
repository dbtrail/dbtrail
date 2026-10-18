package status

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/go-sql-driver/mysql"
)

// erUnknownPartition is MySQL's ER_UNKNOWN_PARTITION: a PARTITION (p) clause
// naming a partition the table no longer has. Rotation drops partitions while
// this runs, so a name listed a moment ago can be gone by the time it is read.
const erUnknownPartition = 1735

// loadEventSpan fills the oldest and newest event and the event count of the
// live index without reading the whole of binlog_events.
//
// One SELECT MIN, MAX, COUNT(*) over the table used to do this, and its cost
// grows with everything the index retains: the capture daemon runs it every
// minute for its gauges and the Overview polls it, so on an index holding a
// week it scans the week, all day, for three numbers. binlog_events is RANGE
// partitioned by event_timestamp, so the oldest event is the MIN of the first
// partition that has a row and the newest the MAX of the last one: exact, at
// the cost of reading those two partitions. Each partition is asked with a
// real query in partition order; TABLE_ROWS is never used to decide that a
// partition is empty, because it is an estimate that can read 0 for a
// partition that has rows.
//
// The count is the sum of TABLE_ROWS, InnoDB's estimate, and TotalEventsEstimated
// says so. When the sum is 0 while a partition holds a row (statistics not yet
// updated after the first writes), the count is taken exactly: an index whose
// statistics have not caught up yet is small, and a 0 next to a real oldest
// event would contradict itself. A binlog_events that is not partitioned keeps
// the single exact query, and so does a partition list that changed while it
// was read (see the end of the function).
func loadEventSpan(ctx context.Context, db *sql.DB, c *CoverageInfo) error {
	names, estimate, byTime, err := listEventPartitions(ctx, db)
	if err != nil {
		return err
	}
	if len(names) == 0 || !byTime {
		return fullEventSpan(ctx, db, c)
	}

	for _, name := range names {
		found, err := probePartition(ctx, db, "MIN", name, &c.EarliestEvent)
		if err != nil {
			return err
		}
		if found {
			break
		}
	}
	if c.EarliestEvent.Valid {
		for i := len(names) - 1; i >= 0; i-- {
			found, err := probePartition(ctx, db, "MAX", names[i], &c.LatestEvent)
			if err != nil {
				return err
			}
			if found {
				break
			}
		}
	}

	// The partitions can change under the probes: rotation reorganises
	// p_future into new hourly partitions (moving its rows into names this
	// list does not have) and drops old ones (a probe of a dropped one reads
	// as empty). A list that changed means the answers above may describe a
	// table that no longer exists; the full query then gives the exact answer.
	// Rotation changes partitions once an hour, so this is rare.
	again, _, _, err := listEventPartitions(ctx, db)
	if err != nil {
		return err
	}
	if !slices.Equal(names, again) {
		*c = CoverageInfo{}
		return fullEventSpan(ctx, db, c)
	}

	if !c.EarliestEvent.Valid {
		// Every partition answered NULL. Confirm with one row before calling
		// the index empty, since "empty" is reported as exact.
		var one int
		switch err := db.QueryRowContext(ctx, `SELECT 1 FROM binlog_events LIMIT 1`).Scan(&one); {
		case errors.Is(err, sql.ErrNoRows):
			c.TotalEvents = 0
			return nil
		case err != nil:
			return fmt.Errorf("check binlog_events for rows: %w", err)
		default:
			*c = CoverageInfo{}
			return fullEventSpan(ctx, db, c)
		}
	}
	if estimate > 0 {
		c.TotalEvents, c.TotalEventsEstimated = estimate, true
		return nil
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM binlog_events`).Scan(&c.TotalEvents); err != nil {
		return fmt.Errorf("count binlog_events: %w", err)
	}
	return nil
}

// fullEventSpan is the exact span in one query over the whole table: the
// fallback for a table that is not partitioned by event time, and for a
// partition list that changed while it was being read.
func fullEventSpan(ctx context.Context, db *sql.DB, c *CoverageInfo) error {
	return db.QueryRowContext(ctx, `
		SELECT MIN(event_timestamp), MAX(event_timestamp), COUNT(*)
		FROM binlog_events`).Scan(&c.EarliestEvent, &c.LatestEvent, &c.TotalEvents)
}

// listEventPartitions returns binlog_events' partitions in order, the sum of
// their row estimates, and whether the table is RANGE partitioned on
// event_timestamp: only then is partition order time order, which the span
// relies on. The layout bintrail creates always is; a table an operator
// re-partitioned another way reads through the full query instead.
func listEventPartitions(ctx context.Context, db *sql.DB) (names []string, estimate int64, byTime bool, err error) {
	rows, err := db.QueryContext(ctx, `
		SELECT PARTITION_NAME, COALESCE(TABLE_ROWS, 0),
		       COALESCE(PARTITION_METHOD, ''), COALESCE(PARTITION_EXPRESSION, ''),
		       SUBPARTITION_NAME IS NOT NULL
		FROM information_schema.PARTITIONS
		WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'binlog_events'
		  AND PARTITION_NAME IS NOT NULL
		ORDER BY PARTITION_ORDINAL_POSITION, SUBPARTITION_ORDINAL_POSITION`)
	if err != nil {
		return nil, 0, false, fmt.Errorf("list binlog_events partitions: %w", err)
	}
	defer rows.Close()
	byTime = true
	for rows.Next() {
		var name, method, expr string
		var n int64
		var sub bool
		if err := rows.Scan(&name, &n, &method, &expr, &sub); err != nil {
			return nil, 0, false, fmt.Errorf("list binlog_events partitions: %w", err)
		}
		if sub || !strings.HasPrefix(strings.ToUpper(method), "RANGE") || !strings.Contains(strings.ToLower(expr), "event_timestamp") {
			byTime = false
		}
		if len(names) == 0 || names[len(names)-1] != name {
			names = append(names, name)
		}
		estimate += n
	}
	if err := rows.Err(); err != nil {
		return nil, 0, false, fmt.Errorf("list binlog_events partitions: %w", err)
	}
	return names, estimate, byTime, nil
}

// probePartition reads agg(event_timestamp) of one partition into dst and says
// whether it held a row. A partition rotation dropped since it was listed
// reads as empty; the second listing in loadEventSpan then sees the change.
func probePartition(ctx context.Context, db *sql.DB, agg, name string, dst *sql.NullTime) (bool, error) {
	q := "SELECT " + agg + "(event_timestamp) FROM binlog_events PARTITION (`" + strings.ReplaceAll(name, "`", "``") + "`)"
	var t sql.NullTime
	if err := db.QueryRowContext(ctx, q).Scan(&t); err != nil {
		var me *mysql.MySQLError
		if errors.As(err, &me) && me.Number == erUnknownPartition {
			return false, nil
		}
		return false, fmt.Errorf("read the %s event of partition %s: %w", strings.ToLower(agg), name, err)
	}
	if !t.Valid {
		return false, nil
	}
	*dst = t
	return true, nil
}
