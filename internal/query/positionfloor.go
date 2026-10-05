package query

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"
)

// CoarseSinceFloor is the time bound a fetch anchored on a binlog position
// (Options.SincePos) actually applies for Options.Since: the start of that
// hour, less one more hour (#797). buildQuery uses it for the index and
// PartitionHeads uses it to know which partitions that bound leaves out; the
// two must be the same instant, which is why it has one definition.
func CoarseSinceFloor(since time.Time) time.Time {
	return since.Truncate(time.Hour).Add(-time.Hour)
}

// partitionHead is one partition of binlog_events and its newest row.
type partitionHead struct {
	// name is the partition's name; "" for a table that is not partitioned.
	name string
	// lower is the lowest event_timestamp the partition can hold: the upper
	// bound of the partition before it. open is set for the first partition,
	// which has no lower bound.
	lower time.Time
	open  bool
	// empty: the partition holds no row.
	empty bool
	// pos is where the row with the highest event_id starts. unknown is set
	// when that row carries no usable coordinate.
	pos     BinlogPos
	unknown bool
}

// PartitionHeads answers one question for a fetch that starts at a binlog
// position: how far back in TIME can an event after that position be (#2138).
//
// # Why the question exists
//
// Such a fetch is exact by position (Options.SincePos) and also carries a time
// (Options.Since), which buildQuery turns into a floor on event_timestamp so
// the index and the archives can be pruned. Every caller derives that time
// from a clock that is not the source's: a refresh stamps what it writes with
// its own clock, a dump with the dump host's. event_timestamp is when the
// statement ran ON THE SOURCE. Whenever the index receives an event later
// than it ran (capture catching up after an outage, a source that is itself
// a delayed replica, a transaction open for hours) the event sits after the
// position and BEFORE the floor: the fetch drops it, with no error, and a
// refresh then publishes a snapshot without that change.
//
// # How it is answered
//
// By looking, not by assuming a margin. The row with the highest event_id of
// a partition is the last one written into it, so under the premise that
// ascending event_id is binlog order its position is the highest the
// partition holds. A partition whose newest row starts before the anchor
// holds nothing after the anchor and can be left out; one whose newest row is
// at or after it may, and the fetch must reach it. That costs one primary-key
// seek per partition, once (measured on MySQL 8.4.9: see the #2138 entry in
// docs/CHANGELOG.md).
//
// The premise is the one ResolveSnapshotCut already stakes the cut on. It
// holds for an index a stream writes and for binlog files indexed in order.
// It does not hold while `bintrail index` adds files to an index a stream
// writes: those rows get the newest ids with old positions and can sit on top
// of a partition, hiding later positions below them. index_state says when
// that last happened, and a fetch anchored before it treats every partition
// as one that may hold a later event.
//
// The answer is only ever an EARLIER time than the caller's: the floor moves
// back or stays. What it cannot see is an event whose partition left the live
// index (rotated into an archive) between being indexed and this fetch; for
// that one the caller's own time still decides, as it did before.
//
// A value is a snapshot of the index. Load it AFTER the fetch's upper bound is
// fixed (a refresh: after its cut), or a late event indexed in between is
// under the bound and not in the picture.
type PartitionHeads struct {
	parts []partitionHead
	// streamCaptured and lastFileIndexed are the two facts the premise is
	// checked against: whether a stream writes this index, and when a binlog
	// file was last being indexed into it (zero: never).
	streamCaptured  bool
	lastFileIndexed time.Time
}

// fileIndexingMargin is how much earlier than a fetch's own time a file
// indexing run must have ended to be counted as over before it. The two times
// come from different clocks (the index server's and the host that stamped
// the snapshot), and erring long only widens a fetch.
const fileIndexingMargin = time.Hour

// LoadPartitionHeads reads the picture PartitionHeads answers from.
func LoadPartitionHeads(ctx context.Context, db *sql.DB) (*PartitionHeads, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT PARTITION_NAME, PARTITION_DESCRIPTION
		FROM information_schema.PARTITIONS
		WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'binlog_events'
		ORDER BY PARTITION_ORDINAL_POSITION`)
	if err != nil {
		return nil, fmt.Errorf("list the partitions of binlog_events: %w", err)
	}
	h := &PartitionHeads{}
	prev, first := time.Time{}, true
	for rows.Next() {
		var name, desc sql.NullString
		if err := rows.Scan(&name, &desc); err != nil {
			rows.Close()
			return nil, fmt.Errorf("list the partitions of binlog_events: %w", err)
		}
		p := partitionHead{name: name.String, lower: prev, open: first}
		first = false
		if name.Valid && desc.String != "MAXVALUE" {
			secs, perr := strconv.ParseInt(desc.String, 10, 64)
			if perr != nil {
				rows.Close()
				return nil, fmt.Errorf("partition %s of binlog_events has a bound this build cannot read (%q)", name.String, desc.String)
			}
			prev = time.Unix(secs-mysqlToSecondsConst, 0).UTC()
		}
		h.parts = append(h.parts, p)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, fmt.Errorf("list the partitions of binlog_events: %w", err)
	}
	rows.Close()
	if len(h.parts) == 0 {
		return nil, errors.New("binlog_events is not in this index database")
	}

	for i := range h.parts {
		p := &h.parts[i]
		var file sql.NullString
		var pos sql.NullInt64
		// The primary key leads with event_id, so this is one seek from the
		// end of the partition's own index.
		err := db.QueryRowContext(ctx, "SELECT binlog_file, start_pos FROM binlog_events"+
			partitionClause([]string{p.name})+" ORDER BY event_id DESC LIMIT 1").Scan(&file, &pos)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			p.empty = true
		case err != nil:
			return nil, fmt.Errorf("read the newest event of partition %s: %w", p.name, err)
		case file.String == "" || pos.Int64 < 0:
			p.unknown = true
		default:
			p.pos = BinlogPos{File: file.String, Pos: uint64(pos.Int64)}
		}
	}

	// After the heads, on purpose: a file indexing run that put a row on top
	// of a partition before the read above had already recorded itself here.
	if h.streamCaptured, err = StreamCaptured(ctx, db); err != nil {
		return nil, err
	}
	var last sql.NullTime
	err = db.QueryRowContext(ctx, "SELECT MAX(COALESCE(completed_at, UTC_TIMESTAMP())) FROM index_state").Scan(&last)
	if err != nil && !isMissingTableErr(err) {
		return nil, fmt.Errorf("read index_state: %w", err)
	}
	if err == nil && last.Valid {
		h.lastFileIndexed = last.Time.UTC()
	}
	return h, nil
}

// partitionClause is the ` PARTITION (...)` selector for names, or "" for a
// table that is not partitioned (its one entry is named "").
func partitionClause(names []string) string {
	if len(names) == 0 || names[0] == "" {
		return ""
	}
	quoted := make([]string, len(names))
	for i, n := range names {
		quoted[i] = "`" + strings.ReplaceAll(n, "`", "``") + "`"
	}
	return " PARTITION (" + strings.Join(quoted, ", ") + ")"
}

// orderProven reports whether, for a fetch whose own time is since, the
// newest row of a partition is known to carry its highest position.
func (h *PartitionHeads) orderProven(since time.Time) bool {
	if !h.streamCaptured || h.lastFileIndexed.IsZero() {
		// One writer kind only: a stream alone, or files alone (indexed in
		// order, the input a refresh chain supports).
		return true
	}
	return h.lastFileIndexed.Before(since.Add(-fileIndexingMargin))
}

// below names the partitions that the floor of a fetch at since leaves out,
// in whole or in part, and that may hold an event at or after anchor.
func (h *PartitionHeads) below(since time.Time, anchor BinlogPos) []string {
	floor := CoarseSinceFloor(since)
	proven := h.orderProven(since)
	var names []string
	for _, p := range h.parts {
		if p.empty || !(p.open || p.lower.Before(floor)) {
			continue
		}
		if !proven || p.unknown || anchor.AtOrBefore(p.pos) {
			names = append(names, p.name)
		}
	}
	return names
}

// SinceFor returns the time a fetch with these options must start from: its
// own Since, or the oldest event of its table in the partitions that may hold
// an event after its position and that its floor would leave out. Options
// without both Since and SincePos are returned as they are.
//
// An error means the question could not be answered. The caller must not go
// on with its own time: that is the silent loss this exists to stop.
func (h *PartitionHeads) SinceFor(ctx context.Context, db *sql.DB, opts Options) (*time.Time, error) {
	if opts.Since == nil || opts.SincePos == nil {
		return opts.Since, nil
	}
	since := *opts.Since
	names := h.below(since, *opts.SincePos)
	if len(names) == 0 {
		return opts.Since, nil
	}
	// The first row in time order, read off idx_row_lookup (schema_name,
	// table_name, event_timestamp): one seek per named partition.
	q := "SELECT event_timestamp FROM binlog_events" + partitionClause(names) + " WHERE event_timestamp < ?"
	args := []any{CoarseSinceFloor(since)}
	if opts.Schema != "" {
		q += " AND schema_name = ?"
		args = append(args, opts.Schema)
	}
	if opts.Table != "" {
		q += " AND table_name = ?"
		args = append(args, opts.Table)
	}
	q += " ORDER BY event_timestamp LIMIT 1"
	var oldest time.Time
	err := db.QueryRowContext(ctx, q, args...).Scan(&oldest)
	if errors.Is(err, sql.ErrNoRows) {
		return opts.Since, nil
	}
	if err != nil {
		return nil, fmt.Errorf("find how far back the changes after %s:%d reach for %s.%s: %w",
			opts.SincePos.File, opts.SincePos.Pos, opts.Schema, opts.Table, err)
	}
	slog.Info("the fetch starts earlier than the time of the snapshot it continues: "+
		"the index holds events that may come after the snapshot's position and that ran on the source before that time",
		"schema", opts.Schema, "table", opts.Table,
		"snapshot_time", since.UTC().Format(time.RFC3339), "fetch_from", oldest.UTC().Format(time.RFC3339),
		"anchor", opts.SincePos.File+":"+strconv.FormatUint(opts.SincePos.Pos, 10),
		"partitions", len(names), "order_proven", h.orderProven(since))
	return &oldest, nil
}
