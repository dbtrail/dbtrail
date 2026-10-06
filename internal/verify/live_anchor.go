package verify

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/dbtrail/dbtrail/internal/consistency"
	"github.com/dbtrail/dbtrail/internal/query"
)

// indexBehind starts every coverage verdict that says the index has not yet
// reached the snapshot (indexCovers, indexCoversMariaDB): the one verdict
// that waiting can change (waitIndexCovers).
const indexBehind = "index is behind the source snapshot"

// DefaultCoverageWait is how long live-source verify waits, per table, for a
// running capture to reach the snapshot it just read when Config.CoverageWait
// is zero (#2150). On a table with steady writes the capture is always a
// little behind the snapshot (it saves its position every few seconds), so
// without the wait the check could only ever report inconclusive there.
const DefaultCoverageWait = 60 * time.Second

// coverageStale: a capture whose checkpoint is older than this is not
// running, and nothing is waited for. coveragePoll is how often the
// checkpoint is read while waiting. Variables so tests do not sleep.
var (
	coverageStale = 60 * time.Second
	coveragePoll  = 500 * time.Millisecond
)

// waitIndexCovers is indexCovers, waiting up to wait while the only reason
// the index does not cover the snapshot is that it has not reached it yet and
// the capture is running (its checkpoint is fresh). Any other verdict, a
// stale checkpoint, or the end of the wait returns the last verdict as is.
//
// A cancelled ctx is an error, not a verdict: the index was not found behind,
// the check was stopped.
func waitIndexCovers(ctx context.Context, indexDB *sql.DB, set, flavor string, wait time.Duration) (bool, string, error) {
	deadline := time.Now().Add(wait)
	for {
		covered, note := indexCovers(ctx, indexDB, set, flavor)
		if err := ctx.Err(); err != nil {
			return false, "", err
		}
		if covered || !strings.HasPrefix(note, indexBehind) {
			return covered, note, nil
		}
		running, why := captureRunning(ctx, indexDB)
		if !running {
			return covered, note + ". " + why, nil
		}
		if !time.Now().Before(deadline) {
			return covered, note, nil
		}
		select {
		case <-ctx.Done():
			return false, "", ctx.Err()
		case <-time.After(coveragePoll):
		}
	}
}

// captureRunning reports whether the index's checkpoint was saved within
// coverageStale, by the index server's clock (the one that stamped it); when
// not, why says what was seen, for the verdict.
func captureRunning(ctx context.Context, indexDB *sql.DB) (bool, string) {
	var age sql.NullInt64
	err := indexDB.QueryRowContext(ctx,
		"SELECT TIMESTAMPDIFF(SECOND, last_checkpoint, UTC_TIMESTAMP()) FROM stream_state WHERE id = 1").Scan(&age)
	switch {
	case err != nil:
		return false, "The capture's last checkpoint could not be read (" + err.Error() + "), so it was not waited for"
	case !age.Valid:
		return false, "The capture has no checkpoint time, so it was not waited for"
	case time.Duration(age.Int64)*time.Second > coverageStale:
		return false, fmt.Sprintf("The capture's last checkpoint is %ds old: is the capture running?", age.Int64)
	}
	return true, ""
}

// liveCut is how the reconstruction of one live-source check is bounded.
type liveCut struct {
	// pos is the exact upper bound (query.Options.UntilPos): the end of the
	// last indexed change the snapshot holds. nil when the read is not cut.
	pos *query.BinlogPos
	// note, when pos is nil, says why the read is not cut and what that
	// means for the verdict; it is carried on every verdict.
	note string
	// inconclusive, when set, is the reason the check cannot be made at
	// all: the snapshot's position is exact but the index cannot place it.
	inconclusive string
}

// unanchoredNote is the result's note when the read is not cut at the
// snapshot. why says which condition was missing.
func unanchoredNote(why string) string {
	return "the reconstruction is not cut at the snapshot's position (" + why + "), so a write to the table while it was read shows as a mismatch: check a table that takes no writes during the read"
}

// resolveLiveCut decides how the reconstruction is cut for snapshot src,
// given the baseline it starts from (snapshotTime, sincePos).
func resolveLiveCut(ctx context.Context, indexDB *sql.DB, src consistency.TableChecksum, snapshotTime time.Time, sincePos *query.BinlogPos) (liveCut, error) {
	if src.Anchor == "" {
		switch {
		case strings.TrimSpace(src.GTIDSet) == "" && src.GTIDFlavor == consistency.GTIDFlavorMySQL:
			return liveCut{note: unanchoredNote("the source reports no executed GTIDs: gtid_mode=OFF")}, nil
		case src.GTIDMode != "":
			return liveCut{note: unanchoredNote("the source runs with gtid_mode=" + src.GTIDMode + ", so its GTID set does not name what the snapshot holds")}, nil
		case src.AnchorLockRefused:
			return liveCut{note: unanchoredNote("pinning the position on this server needs a read lock on the table: RELOAD and LOCK TABLES, or at least LOCK TABLES, which the source account does not have")}, nil
		case src.GTIDFlavor == consistency.GTIDFlavorMariaDB:
			return liveCut{note: unanchoredNote("the server did not report the snapshot's binary log position")}, nil
		default:
			return liveCut{note: unanchoredNote("the server reports no GTID position")}, nil
		}
	}
	member, err := newSnapshotMembership(src.GTIDFlavor, src.GTIDSet)
	if errors.Is(err, errTaggedGTIDSet) {
		return liveCut{note: unanchoredNote(err.Error())}, nil
	}
	if err != nil {
		return liveCut{inconclusive: "the read cannot be cut at the snapshot: " + err.Error()}, nil
	}
	// The walk reads the index in event_id order as binary log order, which
	// only one stream writing the index guarantees.
	proof, err := query.IDsFollowBinlog(ctx, indexDB, snapshotTime)
	if err != nil {
		return liveCut{}, fmt.Errorf("check the index's change order: %w", err)
	}
	if proof != query.IDsFollowStream {
		return liveCut{note: unanchoredNote("the index's change ids do not follow the binary log order, because a stream did not write all of it")}, nil
	}
	pos, firstOut, why, err := snapshotCut(ctx, indexDB, member)
	if err != nil {
		return liveCut{}, err
	}
	if why != "" {
		return liveCut{inconclusive: why}, nil
	}
	// The snapshot the reconstruction starts from must not hold a change the
	// read does not: one taken while the table was read would start the
	// reconstruction past the point it is compared at. Its position may sit
	// past the cut without that (the cut ends at the last ROW event the
	// snapshot holds, the baseline's position after that transaction's
	// commit), so only a baseline at or past the first change the snapshot
	// does NOT hold is newer than the read. Then the delta window is empty
	// and the reconstruction is the baseline, which is right.
	if sincePos != nil && firstOut != nil && firstOut.AtOrBefore(*sincePos) && *firstOut != *sincePos {
		return liveCut{inconclusive: fmt.Sprintf("the snapshot the reconstruction starts from (%s:%d) holds changes the read of the table does not (from %s:%d); run the check again",
			sincePos.File, sincePos.Pos, firstOut.File, firstOut.Pos)}, nil
	}
	return liveCut{pos: pos}, nil
}
