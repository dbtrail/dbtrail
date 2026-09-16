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
	mysqldriver "github.com/go-sql-driver/mysql"
)

// toSecondsEpoch converts a Go time to MySQL's TO_SECONDS() domain (seconds
// since year 0). TO_SECONDS('1970-01-01') is 62167219200; the same constant
// backs query.mysqlToSeconds (which does exactly this for the fetch's own
// partition hints) and indexer.DescriptionToHuman in the opposite direction.
// Kept local rather than exported from query: one arithmetic line, pinned by
// the integration test that asserts the resolved cut against real rows.
//
// It is inlined as a literal into the WHERE clause rather than bound as a
// parameter on the premise that MySQL cannot prune partitions from a
// parameterised comparison — the same premise behind query.buildQuery's
// Since/Until hints. Measured on MySQL 8.4 (#1692), that premise did not
// hold for this statement: the TO_SECONDS hint pruned nothing, while the
// plain `event_timestamp > ?` comparison, parameter and all, did prune —
// though it kept the table's OLDEST partition for every value tried. What
// bounds the snapshot-cut scan is therefore the explicit PARTITION clause
// firstEventPast builds. The hint is kept as a redundant filter: dropping it
// belongs with the same measurement of buildQuery's hints on 8.0, which
// #1692 asks for and which has not been made.
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
// # Assumptions
//
// Commit order is read as ascending event_id. event_id is AUTO_INCREMENT and the
// capturer inserts in stream order, so it tracks binlog order — the same
// assumption query.OldestIndexedEvent already makes to report the index's
// starting coordinate. It can be violated by `bintrail index` fed explicit
// --files out of order; such an index is not a supported input for a
// self-refreshing baseline chain.
//
// The partition bound trusts names: p_YYYYMMDDHH is taken to be VALUES LESS
// THAN the next hour and p_future the MAXVALUE catch-all — the layout init,
// rotation and restore-index write. firstEventPast never reads
// PARTITION_DESCRIPTION, so a hand-made partition carrying one of those names
// with a different bound would silently shrink the search.
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

// partitionDateOrFuture recognises the two shapes of partition name
// binlog_events carries: the hourly p_YYYYMMDDHH and the p_future catch-all.
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
// That set never loses a candidate. A partition p_H is VALUES LESS THAN H+1h:
// its upper bound is H+1h and its lower bound is the previous partition's
// upper bound — open for the OLDEST one, which therefore also holds everything
// before its hour, and wider than an hour after a gap in the names. An event
// past at cannot sit in a partition whose upper bound is at or before at,
// whatever its lower bound. So dropping every partition named before at's
// hour never drops a candidate, and it drops the oldest one, which the
// server's own pruning kept on MySQL 8.4 (#1692).
//
// The reasoning holds only for those two name shapes; any other name is
// errUnrecognisedPartition, and nothing unrecognised is interpolated into SQL.
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

// cutBound is the set of partitions one snapshot-cut search was bounded to.
type cutBound struct {
	keep []string
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
//
// The listing is scoped with DATABASE() rather than a dbName parameter, unlike
// the sibling listings in status and rotation: the search statements this
// bound feeds name `binlog_events` unqualified, so both resolve against the
// connection's default database and cannot disagree about which table they
// mean.
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
	return &cutBound{keep: keep}, nil
}

// maxCutBoundAttempts caps the bounded searches firstEventPast runs before it
// gives up on the bound. A search whose candidate set moved underneath it is
// retried on the new set. One rotation run changes the layout several times
// (one DROP per archived partition, then one REORGANIZE of p_future), but only
// the REORGANIZE touches the candidate set for a refresh targeting "now", so a
// set still moving after this many attempts is treated as unknowable and the
// search runs unbounded, which is slower but complete.
const maxCutBoundAttempts = 3

// isUnknownPartition reports MySQL error 1735 (ER_UNKNOWN_PARTITION): a
// partition the statement named no longer exists.
func isUnknownPartition(err error) bool {
	var me *mysqldriver.MySQLError
	return errors.As(err, &me) && me.Number == 1735
}

// firstEventPast returns the coordinate of the first event, in commit order,
// whose timestamp is past at, or nil when the index holds none.
//
// The search is bounded to the partitions that can hold such an event (#1692):
// this statement walks PRIMARY upward from the oldest row and stops at the
// first match, and in the ordinary case (a refresh targeting "now") there is
// no match at all, so without a bound it reads to the end of every partition
// in the plan. On MySQL 8.4 (#1692) the server's own pruning dropped the
// partitions between the oldest and at's hour but kept the oldest one, and
// on the measured index that partition was ~10 GB read on every refresh
// before any table was folded. Naming the partitions is a bound that cannot
// lose a candidate; see partitionsAtOrAfter for why.
//
// # The layout can move between the listing and the search
//
// Rotation's `REORGANIZE PARTITION p_future INTO (...)` moves p_future's rows
// into partitions the clause did not name, and `watch` runs rotation and the
// refresh in one process. A search that missed such a row would report
// "nothing past at", the cut would land after that row, and the row would be
// dropped by this fold's time filter and skipped by the next fold's positional
// lower bound: the silent loss ResolveSnapshotCut's doc explains. So after a
// bounded search the layout is listed again, and only an unchanged candidate
// set proves the clause saw every candidate. (The full listing is not
// compared: rotation also drops partitions older than at's hour, one by one,
// and those can hold nothing past at.) A changed set repeats the search on
// the new one, a few times; so does a search refused because a partition it
// named was dropped meanwhile. Past that, or when the layout cannot be read
// at all, the search runs unbounded, which is slower and always complete. An
// empty clause (an unpartitioned table) runs unbounded without a warning:
// there is nothing to bound by and no layout to confirm.
func firstEventPast(ctx context.Context, db *sql.DB, at time.Time) (*query.BinlogPos, error) {
	atStamp := at.UTC().Format(time.RFC3339)
	bound, err := listCutBound(ctx, db, at)
	if err != nil {
		if ctx.Err() != nil {
			return nil, err
		}
		slog.Warn("resolve snapshot cut: cannot bound the search to partitions; searching the whole table",
			"at", atStamp, "error", err)
		bound = nil
	}
	for attempt := 1; ; attempt++ {
		cut, err := firstEventPastIn(ctx, db, at, bound.clause())
		// A partition the clause named was dropped between the listing and
		// the search. The layout moved and this attempt saw nothing; it is
		// retried below like any other move, never accepted.
		moved := err != nil && bound.clause() != "" && isUnknownPartition(err)
		if err != nil && !moved {
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
				"at", atStamp, "error", err)
			bound = nil
			continue
		}
		if !moved && slices.Equal(again.keep, bound.keep) {
			return cut, nil
		}
		if attempt >= maxCutBoundAttempts {
			slog.Warn("resolve snapshot cut: the bounded search could not be confirmed after repeated attempts; searching the whole table",
				"at", atStamp, "attempts", attempt, "refused", moved, "was", bound.keep, "now", again.keep)
			bound = nil
			continue
		}
		bound = again
	}
}

// firstEventPastSQL is the statement firstEventPastIn runs, with at bound as
// its one parameter. Shared with the integration test that EXPLAINs it. Rows
// with a NULL coordinate (#318 drift rows) are unusable as an anchor and are
// skipped here and by the newest-event statement alike; they are also not
// events the position predicates could ever admit, so skipping them changes
// no window.
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
