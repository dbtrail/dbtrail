package reconstruct

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

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
// hints.
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
// resolveSnapshotCut is the call the fold makes, indirected so a test can count
// it (#1635): one run resolves one cut, whatever the number of tables.
var resolveSnapshotCut = ResolveSnapshotCut

func ResolveSnapshotCut(ctx context.Context, db *sql.DB, at time.Time) (*query.BinlogPos, error) {
	// Rows with a NULL coordinate (#318 drift rows) are unusable as an anchor and
	// are skipped on both branches; they are also not events the position
	// predicates could ever admit, so skipping them changes no window.
	var (
		file string
		pos  uint64
	)
	// Partition-pruning hint: every event past at lives in a partition at-or-after
	// at's hour, so the scan starts there instead of at the oldest partition.
	err := db.QueryRowContext(ctx, fmt.Sprintf(
		`SELECT binlog_file, start_pos FROM binlog_events
		  WHERE TO_SECONDS(event_timestamp) >= %d
		    AND event_timestamp > ?
		    AND binlog_file IS NOT NULL AND start_pos IS NOT NULL
		  ORDER BY event_id ASC LIMIT 1`, toSeconds(at.Truncate(time.Hour))), at).
		Scan(&file, &pos)
	switch {
	case err == nil:
		return &query.BinlogPos{File: file, Pos: pos}, nil
	case !errors.Is(err, sql.ErrNoRows):
		return nil, fmt.Errorf("resolve snapshot cut (first event past %s): %w",
			at.UTC().Format(time.RFC3339), err)
	}

	// Nothing indexed past at: fold everything, and resume after the newest event.
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
