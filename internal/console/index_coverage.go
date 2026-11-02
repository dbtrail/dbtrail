package console

import (
	"context"
	"database/sql"
	"time"

	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/status"
)

// indexMayLackSince reports whether the index may no longer hold every event
// from floor on, which the tail (indexTailSource.Fill) and the unchanged
// lookup (copyUnchanged) need: they look for changes from an hour before the
// snapshot, a margin for a statement that began before it and committed
// after. since is the earliest instant the lookups start from (on the
// source's clock), anchors the snapshots' binlog coordinates the lookups
// start after, parts the live partitions, read after the lookups.
//
// It used to be the oldest live partition's hour alone, which refused every
// table of a young index: a new index's oldest partition is its creation
// hour, always after the floor of a snapshot taken in that hour, although
// nothing was ever dropped and that partition, which has no lower bound,
// holds every older event (#2606). The index's partition history says when
// nothing was (indexer.PartitionHistory.LossBound); an index without one is
// measured as before.
func indexMayLackSince(ctx context.Context, db *sql.DB, dbName string, parts []status.PartitionStat, floor, since time.Time, anchors []query.BinlogPos) (bool, error) {
	hist, err := indexer.ReadPartitionHistory(ctx, db, dbName)
	if err != nil {
		return false, err
	}
	bound, ok := hist.LossBound(status.OldestLivePartitionHour(parts), since, capturedFrom(hist, anchors))
	return !ok || bound.After(floor), nil
}

// capturedFrom reports whether capture holds every event from each anchor
// on: it began, by the history's record, at or before every one of them.
// Coordinates on one source, compared in binlog order; an anchor or a start
// that is not known, or one that is a PostgreSQL LSN (whose capture records
// no start), is not.
func capturedFrom(hist indexer.PartitionHistory, anchors []query.BinlogPos) bool {
	if hist.CaptureFromFile == "" || len(anchors) == 0 {
		return false
	}
	from := query.BinlogPos{File: hist.CaptureFromFile, Pos: hist.CaptureFromPos}
	if from.IsLSN() {
		return false
	}
	for _, a := range anchors {
		if a.File == "" || a.IsLSN() || !from.AtOrBefore(a) {
			return false
		}
	}
	return true
}
