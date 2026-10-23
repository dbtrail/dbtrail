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
	parts, estimate, byTime, err := listEventPartitions(ctx, db)
	if err != nil {
		return err
	}
	names := partitionNames(parts)
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
	if !slices.Equal(names, partitionNames(again)) {
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

// loadEventSpanForGauges fills the span for the capture daemon's gauges,
// which it refreshes every minute, unattended, for as long as it runs
// (#2509): it never reads a partition's rows. loadEventSpan's exact probes,
// MIN and MAX of event_timestamp over the edge partitions, read every row
// of each (no index starts with event_timestamp), which on a busy index is
// millions of rows for one answer, a minute apart, forever: measured at 97 %
// of the time the index spent on SELECTs, competing with capture's writes
// and the refresh's reads. The exact span stays what `bintrail status`, the
// console and MCP read on demand.
//
// The newest event is the capture's own last checkpointed event (newest,
// from stream_state): the daemon that scrapes is the capture, so this is at
// most one checkpoint behind what the index holds and never ahead; without
// a checkpoint, the newest row by primary key. The oldest is the lower hour
// bound of the oldest partition whose TABLE_ROWS estimate is not zero, a
// gauge to the hour, which is the granularity the retention horizon and gap
// hours already have; when that partition is p_future (no bound) or no
// partition carries an estimate, the oldest row by primary key. A table
// that is not partitioned by time keeps the exact query: it is small. The
// count is the estimate loadEventSpan already reports; this never counts.
//
// The primary-key reads assume event_id order is time order, which capture
// in binlog order gives; an older binlog indexed after live capture breaks
// it for the oldest (the horizon then reads too new) until the next hour's
// partition carries an estimate.
func loadEventSpanForGauges(ctx context.Context, db *sql.DB, c *CoverageInfo) error {
	parts, estimate, byTime, err := listEventPartitions(ctx, db)
	if err != nil {
		return err
	}
	if len(parts) == 0 || !byTime {
		return fullEventSpan(ctx, db, c)
	}
	oldestByKey := true
	for _, p := range parts {
		if p.rows == 0 {
			continue
		}
		if h, ok := parsePartitionName(p.name); ok {
			c.EarliestEvent = sql.NullTime{Time: h, Valid: true}
			oldestByKey = false
		}
		break
	}
	if oldestByKey {
		found, err := edgeRowByKey(ctx, db, "ASC", &c.EarliestEvent)
		if err != nil {
			return err
		}
		if !found {
			c.TotalEvents = 0
			return nil
		}
	}
	if _, err := edgeRowByKey(ctx, db, "DESC", &c.LatestEvent); err != nil {
		return err
	}
	c.TotalEvents, c.TotalEventsEstimated = estimate, true
	c.TotalEventsUnknown = estimate == 0
	return nil
}

// edgeRowByKey reads the event_timestamp of the first (ASC) or last (DESC)
// row by primary key into dst: one row per partition, merged. It says
// whether the table held a row.
func edgeRowByKey(ctx context.Context, db *sql.DB, order string, dst *sql.NullTime) (bool, error) {
	var t sql.NullTime
	err := db.QueryRowContext(ctx, "SELECT event_timestamp FROM binlog_events FORCE INDEX (PRIMARY) ORDER BY event_id "+order+" LIMIT 1").Scan(&t)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return false, nil
	case err != nil:
		return false, fmt.Errorf("read the %s event by key: %w", map[string]string{"ASC": "oldest", "DESC": "newest"}[order], err)
	}
	*dst = t
	return t.Valid, nil
}

// fullEventSpan is the exact span in one query over the whole table: the
// fallback for a table that is not partitioned by event time, and for a
// partition list that changed while it was being read.
func fullEventSpan(ctx context.Context, db *sql.DB, c *CoverageInfo) error {
	return db.QueryRowContext(ctx, `
		SELECT MIN(event_timestamp), MAX(event_timestamp), COUNT(*)
		FROM binlog_events`).Scan(&c.EarliestEvent, &c.LatestEvent, &c.TotalEvents)
}

// eventPartition is one partition of binlog_events: its name and its
// TABLE_ROWS estimate (never a count; 0 can be a partition that has rows).
type eventPartition struct {
	name string
	rows int64
}

func partitionNames(parts []eventPartition) []string {
	names := make([]string, len(parts))
	for i, p := range parts {
		names[i] = p.name
	}
	return names
}

// listEventPartitions returns binlog_events' partitions in order with their
// row estimates, the sum of those estimates, and whether the table is RANGE
// partitioned on event_timestamp: only then is partition order time order,
// which the span relies on. The layout bintrail creates always is; a table
// an operator re-partitioned another way reads through the full query
// instead.
func listEventPartitions(ctx context.Context, db *sql.DB) (parts []eventPartition, estimate int64, byTime bool, err error) {
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
		if len(parts) == 0 || parts[len(parts)-1].name != name {
			parts = append(parts, eventPartition{name: name})
		}
		parts[len(parts)-1].rows += n
		estimate += n
	}
	if err := rows.Err(); err != nil {
		return nil, 0, false, fmt.Errorf("list binlog_events partitions: %w", err)
	}
	return parts, estimate, byTime, nil
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
