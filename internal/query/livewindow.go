package query

import (
	"context"
	"database/sql"
	"time"
)

// LiveWindowContiguous reports whether the live index still holds a partition
// for every hour of [since, until] — that is, whether a LIVE-ONLY scan of that
// window can be complete. Archive coverage is deliberately NOT consulted: an
// hour that was rotated out is exactly what such a scan cannot see, whether
// or not a Parquet archive holds it (#1615).
//
// Fail-closed: an unreadable partition list is returned as an error, and a
// window the planner cannot classify at all (no partitions, no database name)
// reports false. Neither is ever "contiguous".
func LiveWindowContiguous(ctx context.Context, db *sql.DB, dbName string, since, until time.Time) (bool, error) {
	// noArchive=true makes buildPlan classify every hour the live partitions
	// do not hold as a gap, regardless of archive_state — the single authority
	// for "rotated out of live" that reconstruct's AllowGaps=false path also
	// relies on.
	plan, err := Plan(ctx, db, dbName, &since, &until, true, AllArchives())
	if err != nil {
		return false, err
	}
	if plan == nil {
		return false, nil
	}
	return len(plan.GapHours) == 0, nil
}
