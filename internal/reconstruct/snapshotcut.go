package reconstruct

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"

	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/query"
)

// toSecondsEpoch converts a Go time to MySQL's TO_SECONDS() domain (seconds
// since year 0). TO_SECONDS('1970-01-01') is 62167219200; the same constant
// backs query.mysqlToSeconds (which does exactly this for the fetch's own
// partition hints) and indexer.DescriptionToHuman in the opposite direction.
// Kept local rather than exported from query: one arithmetic line, pinned by
// the integration test that asserts the resolved cut against real rows.
//
// It is inlined as a literal into the WHERE clause rather than bound as a
// parameter because MySQL cannot prune partitions from a parameterised
// comparison — the identical trick query.buildQuery uses for its Since/Until
// hints. Measured on MySQL 8.4 (#1692), that hint prunes NOTHING: the
// predicate is on an expression of the column, not the column, so it only
// filters rows the scan already visits. What did prune there was the plain
// `event_timestamp > ?` comparison, parameter and all — and even that keeps
// the table's OLDEST partition in the plan whatever the value. What bounds
// the snapshot-cut scan is the explicit PARTITION clause built below; the
// hint stays as a harmless filter until every such hint is audited on 8.0.
const toSecondsEpochOffset = 62167219200

func toSeconds(t time.Time) int64 { return t.UTC().Unix() + toSecondsEpochOffset }

// ErrNoIndexedCoordinates is returned by ResolveSnapshotCut when the index holds
// events but none of them carries a usable binlog coordinate. A reconstructed
// baseline cannot be anchored in that case, and an unanchored baseline is worse
// than none: the next fold would have to guess where deltas resume.
var ErrNoIndexedCoordinates = errors.New("no indexed event carries a binlog file/position")

// ResolveSnapshotCut returns the exact binlog coordinate that separates "already
// folded into the snapshot" from "still to come", for a snapshot targeting the
// point in time at.
//
// # Why a position and not a timestamp
//
// The snapshot this cut anchors is a BASELINE: the next reconstruct anchored on
// it fetches deltas with Options.SincePos = this coordinate, i.e. every event
// whose start_pos is at-or-after it. For the chain to lose nothing and
// double-apply nothing, the set folded HERE and the set fetched NEXT TIME must
// partition the binlog exactly. buildQuery's two position predicates already do
// that — UntilPos admits `end_pos <= C`, SincePos admits `start_pos >= C` — so a
// single coordinate C used on both sides is a seam with no gap and no overlap.
//
// A timestamp cannot play that role. binlog_events.event_timestamp is the
// statement's EXECUTION time, not its commit time (the skew #797 exists to route
// around), so "the events at-or-before at" and "the events committed before some
// position" are different sets, and the difference is silently LOST: dropped by
// this fold's `event_timestamp <= at` filter and again by the next fold's
// positional lower bound, which never consults a timestamp at all.
//
// # How the coordinate is chosen
//
// C is the start_pos of the FIRST event, in commit order, whose timestamp is
// past at. Every event that ends at-or-before C therefore committed before that
// event, and so — because that event is the first one past at — carries a
// timestamp at-or-before at as well. That is the property that makes it safe to
// keep the caller's exact `Until: at` time filter alongside `UntilPos: C`: the
// time filter is a superset of the positional window, so it can never exclude a
// row the position admits. Inverting the two (deriving a position from the time
// cut) does not have that property, which is why this is written as a search for
// the first event past at rather than the last event before it.
//
// When no indexed event is past at (the ordinary case for a refresh targeting
// "now"), the cut is the end_pos of the newest event: everything indexed is
// folded, and the next fold resumes exactly after it.
//
// # The one assumption
//
// Commit order is read as ascending event_id. event_id is AUTO_INCREMENT and the
// capturer inserts in stream order, so it tracks binlog order — the same
// assumption query.OldestIndexedEvent already makes to report the index's
// starting coordinate. It can be violated by `bintrail index` fed explicit
// --files out of order; such an index is not a supported input for a
// self-refreshing baseline chain.
//
// Returns (nil, nil) when the index holds no events at all — there is nothing to
// fold, and the caller keeps the source baseline's own coordinates.
func ResolveSnapshotCut(ctx context.Context, db *sql.DB, at time.Time) (*query.BinlogPos, error) {
	return resolveSnapshotCut(ctx, db, at)
}

// resolveSnapshotCut holds ResolveSnapshotCut's implementation behind a
// variable so a test can count every resolution, whichever name the caller
// spells (#1635): one fold resolves one cut, whatever the number of tables.
var resolveSnapshotCut = resolveSnapshotCutOnce

// partitionDateOrFuture recognises the two partition names binlog_events can
// carry: the hourly p_YYYYMMDDHH and the p_future catch-all. Anything else
// makes the caller give up on the bound, which also keeps an unexpected name
// out of the SQL it is interpolated into.
func partitionDateOrFuture(name string) (time.Time, bool) {
	if name == "p_future" {
		return time.Time{}, true
	}
	return indexer.PartitionDate(name)
}

// errUnrecognisedPartition is returned when binlog_events carries a partition
// name this code cannot place on the hourly layout. The bound is then dropped,
// and the caller says so: silently searching the whole table again would be
// the very cost #1692 was opened for, back with no signal.
var errUnrecognisedPartition = errors.New("binlog_events carries a partition name this build does not recognise")

// partitionsAtOrAfter picks, from binlog_events' partition names in table
// order, the ones that can hold an event whose timestamp is past at: the
// partition of at's hour, every later one, and p_future.
//
// That set is exact, not a heuristic. A partition p_H holds the events with
// timestamps in [H, H+1h), except the OLDEST one, whose lower bound is open
// and which therefore also holds everything before its hour. An event past at
// cannot sit in a partition whose upper bound H+1h is at or before at, and for
// the oldest partition that is the same test. So dropping every partition
// before at's hour never drops a candidate, and it drops the one partition
// MySQL's own pruning keeps regardless of the filter (#1692).
//
// The reasoning holds only for the names init and rotation produce, so any
// other name is an error naming it, and nothing else is ever interpolated
// into a statement.
func partitionsAtOrAfter(names []string, at time.Time) ([]string, error) {
	floor := at.UTC().Truncate(time.Hour)
	var keep []string
	for _, n := range names {
		h, ok := partitionDateOrFuture(n)
		if !ok {
			return nil, fmt.Errorf("%w: %q", errUnrecognisedPartition, n)
		}
		if n == "p_future" || !h.Before(floor) {
			keep = append(keep, n)
		}
	}
	return keep, nil
}

// cutBound is the partition layout one snapshot-cut search was bounded by:
// every partition as listed, and the subset the clause names.
type cutBound struct {
	names []string
	keep  []string
}

// clause is the ` PARTITION (...)` selector for the bound, or "" when there is
// nothing to bound by (no bound, an unpartitioned table, or a layout in which
// no partition can hold a row past at and p_future is missing).
func (b *cutBound) clause() string {
	if b == nil || len(b.keep) == 0 {
		return ""
	}
	return " PARTITION (" + strings.Join(b.keep, ", ") + ")"
}

// listCutBound reads binlog_events' partitions and derives the bound for at.
// A listing error and an unrecognised name are both returned as errors; the
// caller decides that searching the whole table is the right fallback.
func listCutBound(ctx context.Context, db *sql.DB, at time.Time) (*cutBound, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT PARTITION_NAME FROM information_schema.PARTITIONS
		WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'binlog_events'
		  AND PARTITION_NAME IS NOT NULL
		ORDER BY PARTITION_ORDINAL_POSITION`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var names []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			return nil, err
		}
		names = append(names, n)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	keep, err := partitionsAtOrAfter(names, at)
	if err != nil {
		return nil, err
	}
	return &cutBound{names: names, keep: keep}, nil
}

// maxCutBoundAttempts bounds how many times firstEventPast re-reads the
// partition layout after it moved under a bounded search. Rotation adds
// partitions once per run, so a layout that keeps changing is not rotation.
const maxCutBoundAttempts = 3

// firstEventPast returns the coordinate of the first event, in commit order,
// whose timestamp is past at, or nil when the index holds none.
//
// The search is bounded to the partitions that can hold such an event (#1692):
// this statement walks PRIMARY upward from the oldest row and stops at the
// first match, and in the ordinary case (a refresh targeting "now") there is
// no match at all, so without a bound it reads to the end of every partition
// in the plan. MySQL's pruning drops the partitions between the oldest and
// at's hour but never the oldest one itself, and on a real index that
// partition was ~10 GB read on every refresh before any table was folded.
// Naming the partitions is the exact bound; see partitionsAtOrAfter for why
// it cannot lose a candidate.
//
// # The layout can move between the listing and the search
//
// Rotation's `REORGANIZE PARTITION p_future INTO (...)` moves p_future's rows
// into partitions the clause did not name, and `watch` runs rotation and the
// refresh in one process. A search that missed such a row would report
// "nothing past at", the cut would land after that row, and the row would be
// dropped by this fold's time filter and skipped by the next fold's positional
// lower bound: the silent loss ResolveSnapshotCut's doc explains. So after a
// bounded search the layout is listed again, and only an unchanged listing
// proves the clause saw every candidate. A changed one repeats the search on
// the new layout, a few times; past that, or when the layout cannot be read
// at all, the search runs unbounded, which is slower and always complete.
func firstEventPast(ctx context.Context, db *sql.DB, at time.Time) (*query.BinlogPos, error) {
	bound, err := listCutBound(ctx, db, at)
	if err != nil {
		if ctx.Err() != nil {
			return nil, err
		}
		slog.Warn("resolve snapshot cut: cannot bound the search to partitions; searching the whole table",
			"error", err)
		bound = nil
	}
	for attempt := 1; ; attempt++ {
		cut, err := firstEventPastIn(ctx, db, at, bound.clause())
		if err != nil {
			return nil, err
		}
		if bound.clause() == "" {
			return cut, nil
		}
		again, err := listCutBound(ctx, db, at)
		if err != nil {
			if ctx.Err() != nil {
				return nil, err
			}
			slog.Warn("resolve snapshot cut: cannot confirm the partition layout after a bounded search; searching the whole table",
				"error", err)
			bound = nil
			continue
		}
		if slices.Equal(again.names, bound.names) {
			return cut, nil
		}
		if attempt >= maxCutBoundAttempts {
			slog.Warn("resolve snapshot cut: the partition layout kept changing during the search; searching the whole table",
				"attempts", attempt)
			bound = nil
			continue
		}
		bound = again
	}
}

// firstEventPastSQL is the statement firstEventPastIn runs, with at bound as
// its one parameter. Shared with the integration test that EXPLAINs it.
func firstEventPastSQL(at time.Time, partClause string) string {
	return fmt.Sprintf(
		`SELECT binlog_file, start_pos FROM binlog_events%s
		  WHERE TO_SECONDS(event_timestamp) >= %d
		    AND event_timestamp > ?
		    AND binlog_file IS NOT NULL AND start_pos IS NOT NULL
		  ORDER BY event_id ASC LIMIT 1`, partClause, toSeconds(at.Truncate(time.Hour)))
}

// firstEventPastIn is one bounded (or, with an empty clause, unbounded)
// search for the first event past at. nil, nil means none was found.
func firstEventPastIn(ctx context.Context, db *sql.DB, at time.Time, partClause string) (*query.BinlogPos, error) {
	var (
		file string
		pos  uint64
	)
	err := db.QueryRowContext(ctx, firstEventPastSQL(at, partClause), at).Scan(&file, &pos)
	switch {
	case err == nil:
		return &query.BinlogPos{File: file, Pos: pos}, nil
	case errors.Is(err, sql.ErrNoRows):
		return nil, nil
	default:
		return nil, fmt.Errorf("resolve snapshot cut (first event past %s): %w",
			at.UTC().Format(time.RFC3339), err)
	}
}

func resolveSnapshotCutOnce(ctx context.Context, db *sql.DB, at time.Time) (*query.BinlogPos, error) {
	// Rows with a NULL coordinate (#318 drift rows) are unusable as an anchor and
	// are skipped on both branches; they are also not events the position
	// predicates could ever admit, so skipping them changes no window.
	cut, err := firstEventPast(ctx, db, at)
	if err != nil {
		return nil, err
	}
	if cut != nil {
		return cut, nil
	}

	// Nothing indexed past at: fold everything, and resume after the newest event.
	var (
		file string
		pos  uint64
	)
	err = db.QueryRowContext(ctx,
		`SELECT binlog_file, end_pos FROM binlog_events
		  WHERE binlog_file IS NOT NULL AND end_pos IS NOT NULL
		  ORDER BY event_id DESC LIMIT 1`).Scan(&file, &pos)
	switch {
	case err == nil:
		return &query.BinlogPos{File: file, Pos: pos}, nil
	case errors.Is(err, sql.ErrNoRows):
		// Either the index is empty, or every row lacks a coordinate. Distinguish
		// them: an empty index is a legitimate "nothing to fold", while an index
		// full of coordinate-less rows cannot anchor a baseline and must fail loud
		// rather than publish a snapshot the next fold cannot resume from.
		var n int64
		if cErr := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM binlog_events`).Scan(&n); cErr != nil {
			return nil, fmt.Errorf("resolve snapshot cut (probe for indexed events): %w", cErr)
		}
		if n == 0 {
			return nil, nil
		}
		return nil, ErrNoIndexedCoordinates
	default:
		return nil, fmt.Errorf("resolve snapshot cut (newest indexed event): %w", err)
	}
}
