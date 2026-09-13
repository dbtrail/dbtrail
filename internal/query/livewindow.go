package query

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// LiveWindowContiguous reports whether the live index still holds every hour
// of [since, until] — that is, whether a LIVE-ONLY scan of that window can be
// complete. Archive coverage is deliberately NOT consulted: an hour that was
// rotated out is exactly what such a scan cannot see, whether or not a Parquet
// archive holds it (#1615).
//
// Hours are classified from the hourly partitions, with one deliberate
// asymmetry at the two edges. An hour BEFORE the oldest explicit partition is
// a gap: it was rotated out, or the index never held it, and either way the
// scan cannot see it. An hour AFTER the newest explicit partition is LIVE: its
// rows sit in p_future (the add-future horizon lapsed — a standalone stream
// with no rotate cron, or rotation switched off), which is never dropped, so
// by construction nothing there was rotated out. The metrics scraper bounds
// its gap count the same way (streamrun.gapScrapeRange); without the cap every
// cascade on such an index would lose Phase-2 the moment the horizon lapsed,
// with a caveat asserting a rotation that never happened.
//
// Partitions only: a stamped permanent capture loss inside the window
// (stream_state.gap_lost_at, #765) is invisible here and must be checked by
// the caller — cascade.LiveWindowProbe does both.
//
// Fail-closed: an unreadable partition list is returned as an error, and an
// index with no explicit hourly partition at all, or no database name, reports
// false. Neither is ever "contiguous".
func LiveWindowContiguous(ctx context.Context, db *sql.DB, dbName string, since, until time.Time) (bool, error) {
	if db == nil || dbName == "" {
		return false, nil
	}
	liveHours, err := loadLivePartitionHours(ctx, db, dbName)
	if err != nil {
		return false, fmt.Errorf("load partition info for the live-window check: %w", err)
	}
	if len(liveHours) == 0 {
		return false, nil
	}
	live := make(map[time.Time]bool, len(liveHours))
	newest := liveHours[0]
	for _, h := range liveHours {
		live[h] = true
		if h.After(newest) {
			newest = h
		}
	}
	end := until.UTC().Truncate(time.Hour)
	for h := since.UTC().Truncate(time.Hour); !h.After(end); h = h.Add(time.Hour) {
		if h.After(newest) {
			break // past the horizon: p_future, never rotated
		}
		if !live[h] {
			return false, nil
		}
	}
	return true, nil
}
