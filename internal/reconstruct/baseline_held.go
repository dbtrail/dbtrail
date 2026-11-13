package reconstruct

import (
	"context"
	"fmt"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// FindHeld runs find and holds the snapshot it names against the prune of
// any process (baseline.HoldSnapshotOfTable, #2675) until release is called.
// A snapshot moved aside between the lookup and the hold is looked up once
// more: the prune removed it, and the next lookup names the one that now
// answers for that moment. Gone twice is an error, never a read of a
// snapshot that is not there.
//
// release is never nil, and a no-op when err is set. An s3:// path and a
// snapshot with no marker are returned unheld, as before.
func FindHeld(ctx context.Context, find FindFunc, schema, table string, at time.Time) (path string, snap time.Time, stale StaleWarning, release func(), err error) {
	noop := func() {}
	for attempt := 0; attempt < 2; attempt++ {
		path, snap, stale, err = find(ctx, schema, table, at)
		if err != nil {
			return path, snap, stale, noop, err
		}
		rel, gone := baseline.HoldSnapshotOfTable(path)
		if !gone {
			return path, snap, stale, rel, nil
		}
	}
	return "", time.Time{}, StaleWarning{}, noop, fmt.Errorf("the snapshot of %s.%s was removed by the cleanup twice while this read started; run it again", schema, table)
}

// FindBaselineHeld is FindHeld over FindBaseline on src.
func FindBaselineHeld(ctx context.Context, src, schema, table string, at time.Time) (string, time.Time, StaleWarning, func(), error) {
	return FindHeld(ctx, func(ctx context.Context, schema, table string, at time.Time) (string, time.Time, StaleWarning, error) {
		return FindBaseline(ctx, src, schema, table, at)
	}, schema, table, at)
}
