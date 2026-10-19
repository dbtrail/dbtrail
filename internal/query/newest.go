package query

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"

	drivermysql "github.com/go-sql-driver/mysql"
)

// newestWalkAttempts is how many times a walk whose partition list changed
// under it is redone before the fetch falls back to one statement over the
// whole table. The list changes when rotation runs, and that is the worst
// moment for the fallback: the long statement holds a metadata lock on
// binlog_events, rotation's ALTER waits behind it, and every later read waits
// behind the ALTER. Rotation changes the list once per run, so a second walk
// almost always sees a stable list.
const newestWalkAttempts = 3

// newestWalkStep, when set, runs before each partition the walk reads, with
// that partition's name. Tests use it to change the partitions mid-walk and
// to see that the walk ran; nil in production.
var newestWalkStep func(partition string)

// newestWalkEligible reports whether a fetch can be answered by walking the
// partitions of binlog_events from the newest one back (fetchNewestByPartition).
//
// It can when the fetch wants the newest Limit events in (event_timestamp,
// event_id) DESC order: the partitions are disjoint time ranges, so every row
// of a newer partition sorts before every row of an older one, and the newest
// Limit rows overall are the newest rows of the newest partitions, read in
// order until Limit are found. Every row filter is a WHERE clause that applies
// inside one partition the same way it applies to the table, so filters keep
// their meaning; a few are left to the single statement because an index
// already makes it cheap and the walk could only add round trips:
//   - schema AND table: idx_row_lookup (schema_name, table_name,
//     event_timestamp) already reads one table's events newest first, a
//     backward index scan that stops at Limit with no sort (measured: 2.5 ms
//     on two million rows, where the walk's one query per partition took
//     60 ms). Schema alone, or table alone, does not pin that order and walks;
//   - a primary key filter (PKValues, PKValuesIn, PKRange) and GTID are
//     answered by idx_pk_hash / idx_gtid in every partition at once;
//   - EventAnchor already prunes to its one hour.
//
// Not walkable at all: a per-key cap (LimitPerPK, LatestPerPKCandidates),
// whose window spans partitions; no Limit, which reads everything anyway;
// ascending order, left as it was.
func newestWalkEligible(opts Options) bool {
	return opts.Limit > 0 && OrderDirection(opts.Order) == "DESC" &&
		opts.LimitPerPK == 0 && !opts.LatestPerPKCandidates &&
		(opts.Schema == "" || opts.Table == "") &&
		opts.PKValues == "" && len(opts.PKValuesIn) == 0 && opts.PKRange == nil &&
		opts.GTID == "" && opts.EventAnchor == nil
}

// fetchNewestByPartition answers a newest-first fetch with a small Limit by
// reading binlog_events one partition at a time, newest first, and stopping
// once Limit rows are found.
//
// Why: no index of binlog_events orders all of its rows by time (the primary
// key leads with event_id, idx_row_lookup with the table name), so the one
// statement "newest N" sorts every row the table retains to keep N. On an
// index holding tens of millions of events that ran for minutes, and the
// console's Overview asks it on every refresh. Read per partition, only the
// partitions up to the one that fills the page are touched, and inside each
// one the rows come from one short index scan per table (walkNewest) rather
// than a sort of the whole partition. The empty partitions ahead of the data
// (init creates 48 future ones, plus p_future) cost one trivial query each.
// Emptiness is always decided by a real query, never by TABLE_ROWS, which is
// an estimate.
//
// walked false (with a nil error) means the caller must run the single
// statement: the fetch is not eligible, the table is not RANGE partitioned on
// event_timestamp, it has no idx_row_lookup, or the partition list kept
// changing while it was read.
func fetchNewestByPartition(ctx context.Context, db *sql.DB, opts Options) (rows []ResultRow, walked bool, err error) {
	if !newestWalkEligible(opts) {
		return nil, false, nil
	}
	for range newestWalkAttempts {
		names, byTime, err := listEventPartitions(ctx, db)
		if err != nil {
			return nil, false, err
		}
		if len(names) == 0 || !byTime {
			return nil, false, nil
		}
		rows, outcome, err := walkNewest(ctx, db, opts, names)
		if err != nil {
			return nil, false, err
		}
		switch outcome {
		case walkRelist:
			continue
		case walkFallBack:
			return nil, false, nil
		}
		// Rotation can reorganise p_future into new hourly partitions while
		// the walk runs, moving rows into names the walk never read; the
		// page would then skip the newest events. A list that is the same
		// after the walk as before it rules that out.
		again, _, err := listEventPartitions(ctx, db)
		if err != nil {
			return nil, false, err
		}
		if slices.Equal(names, again) {
			return rows, true, nil
		}
	}
	return nil, false, nil
}

// walkOutcome is how a walk over one partition list ended.
type walkOutcome int

const (
	walkDone     walkOutcome = iota // the page is exact for that list
	walkRelist                      // a listed partition is gone: list again
	walkFallBack                    // the walk cannot answer: run the single statement
)

// newestBranchTables caps the tables one partition's step reads as separate
// index scans; a partition touching more tables than this is sorted whole.
const newestBranchTables = 64

// walkNewest reads the partitions in names (oldest first, as listed) from the
// last back, asking each for the newest rows still missing.
//
// Each step first lists the tables the partition holds rows for (a real
// query, so an empty partition is known empty). Then, instead of sorting the
// partition's rows by time, which on the current hour of a busy index is
// millions of rows, it reads each table's newest rows from idx_row_lookup
// (schema_name, table_name, event_timestamp): with both names pinned the index
// is already in (event_timestamp, event_id) order, as the stream shape relies
// on (isStreamShape), so a backward scan stops after Limit rows. The union of
// those few rows per table is sorted and cut to Limit. Two tables that differ
// only in case or trailing spaces are one group under the column's collation,
// and the same collation decides the branch's equality, so a branch reads all
// of its group's rows and no row is read by two branches.
func walkNewest(ctx context.Context, db *sql.DB, opts Options, names []string) ([]ResultRow, walkOutcome, error) {
	var out []ResultRow
	read := false // some partition held rows and its read ran
	for i := len(names) - 1; i >= 0 && len(out) < opts.Limit; i-- {
		part := names[i]
		if newestWalkStep != nil {
			newestWalkStep(part)
		}
		tables, err := partitionTables(ctx, db, part)
		if isUnknownPartitionErr(err) {
			return nil, walkRelist, nil // rotation dropped it since it was listed
		}
		if err != nil {
			return nil, 0, err
		}
		if len(tables) == 0 {
			continue
		}
		o := opts
		o.Limit = opts.Limit - len(out)
		var q string
		var args []any
		if len(tables) <= newestBranchTables {
			q, args = buildNewestBranches(o, part, tables)
		} else {
			q, args = buildQueryIn(o, []string{part})
		}
		got, err := queryRows(ctx, db, q, args)
		switch {
		case isUnknownPartitionErr(err):
			return nil, walkRelist, nil
		case isMissingIndexErr(err):
			// An index created without idx_row_lookup (every `bintrail
			// init` creates it): the single statement still answers.
			return nil, walkFallBack, nil
		case err != nil:
			return nil, 0, fmt.Errorf("query failed (partition %s): %w", part, err)
		}
		read = true
		out = append(out, got...)
	}
	if !read {
		// No partition holds a row: the table is empty, so the single
		// statement costs nothing, and it is the one that reports a table
		// this build cannot read (a legacy index missing a column answers
		// 1054 there, which the console turns into its actionable refusal;
		// the walk alone would answer an empty page).
		return nil, walkFallBack, nil
	}
	return out, walkDone, nil
}

// partitionTables lists the (schema, table) groups partition part holds rows
// for, a loose scan of idx_row_lookup's leading columns.
func partitionTables(ctx context.Context, db *sql.DB, part string) ([]SchemaTable, error) {
	rows, err := db.QueryContext(ctx, "SELECT DISTINCT schema_name, table_name FROM binlog_events"+partitionClause([]string{part}))
	if err != nil {
		if isUnknownPartitionErr(err) {
			return nil, err
		}
		return nil, fmt.Errorf("list the tables of partition %s: %w", part, err)
	}
	defer rows.Close()
	var out []SchemaTable
	for rows.Next() {
		var st SchemaTable
		if err := rows.Scan(&st.Schema, &st.Table); err != nil {
			return nil, fmt.Errorf("list the tables of partition %s: %w", part, err)
		}
		out = append(out, st)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list the tables of partition %s: %w", part, err)
	}
	return out, nil
}

// buildNewestBranches is the fetch of opts' newest opts.Limit rows of one
// partition as one backward index scan per table, unioned, sorted and cut,
// then joined back to the wide columns like buildQuery's statement (see the
// late-materialization comment there: only narrow keys are sorted).
func buildNewestBranches(opts Options, part string, tables []SchemaTable) (string, []any) {
	where, whereArgs := buildWhere(opts)
	filter := ""
	if len(where) > 0 {
		filter = " AND " + strings.Join(where, " AND ")
	}
	from := "binlog_events" + partitionClause([]string{part}) + " FORCE INDEX (idx_row_lookup)"
	branches := make([]string, len(tables))
	var args []any
	for i, st := range tables {
		branches[i] = "(SELECT event_id, event_timestamp FROM " + from +
			" WHERE schema_name = ? AND table_name = ?" + filter +
			" ORDER BY event_timestamp DESC, event_id DESC LIMIT ?)"
		args = append(args, st.Schema, st.Table)
		args = append(args, whereArgs...)
		args = append(args, opts.Limit)
	}
	keys := "SELECT event_id, event_timestamp FROM (" + strings.Join(branches, " UNION ALL ") +
		") AS u ORDER BY event_timestamp DESC, event_id DESC LIMIT ?"
	args = append(args, opts.Limit)
	return "SELECT " + resultColumns + " FROM binlog_events AS be JOIN (" + keys + ") AS k" +
		" ON be.event_id = k.event_id AND be.event_timestamp = k.event_timestamp", args
}

// isMissingIndexErr reports MySQL's ER_KEY_DOES_NOT_EXITS (1176): a FORCE
// INDEX naming an index the table does not have.
func isMissingIndexErr(err error) bool {
	var me *drivermysql.MySQLError
	return errors.As(err, &me) && me.Number == 1176
}

func queryRows(ctx context.Context, db *sql.DB, q string, args []any) ([]ResultRow, error) {
	rows, err := db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanRows(rows)
}

// listEventPartitions returns binlog_events' partitions in partition order
// and whether the table is RANGE partitioned on event_timestamp without
// subpartitions: only then is partition order time order, which the walk
// relies on. The layout bintrail creates always is; a table re-partitioned
// another way reads through the single statement. DATABASE() is the schema the
// unqualified binlog_events of every fetch resolves to on this connection.
func listEventPartitions(ctx context.Context, db *sql.DB) (names []string, byTime bool, err error) {
	rows, err := db.QueryContext(ctx, `
		SELECT PARTITION_NAME, COALESCE(PARTITION_METHOD, ''), COALESCE(PARTITION_EXPRESSION, ''),
		       SUBPARTITION_NAME IS NOT NULL
		FROM information_schema.PARTITIONS
		WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'binlog_events'
		  AND PARTITION_NAME IS NOT NULL
		ORDER BY PARTITION_ORDINAL_POSITION, SUBPARTITION_ORDINAL_POSITION`)
	if err != nil {
		return nil, false, fmt.Errorf("list binlog_events partitions: %w", err)
	}
	defer rows.Close()
	byTime = true
	for rows.Next() {
		var name, method, expr string
		var sub bool
		if err := rows.Scan(&name, &method, &expr, &sub); err != nil {
			return nil, false, fmt.Errorf("list binlog_events partitions: %w", err)
		}
		if sub || !strings.HasPrefix(strings.ToUpper(method), "RANGE") ||
			!strings.Contains(strings.ToLower(expr), "event_timestamp") {
			byTime = false
		}
		if len(names) == 0 || names[len(names)-1] != name {
			names = append(names, name)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, false, fmt.Errorf("list binlog_events partitions: %w", err)
	}
	return names, byTime, nil
}
