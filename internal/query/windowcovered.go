package query

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"
)

// WindowCovered reports whether a scan of [since, until] can be complete:
// every hour of the window is held by a live partition or, unless noArchive,
// by a registered archive. noArchive mirrors the caller's own read — a
// --no-archive or profile-confined scan cannot see an archived hour, so for it
// that hour is a gap whether or not a Parquet file holds it (#1615).
//
// Archive coverage comes from archive_state labels, the same registry the
// merged read resolves its sources from; an index whose archive_state cannot
// be read reports the archived hours as gaps (fail-closed) rather than as an
// error, because the live half of the answer is still true — the caller
// merely loses the archive credit.
//
// Live hours are classified from the hourly partitions, with one deliberate
// asymmetry at the two edges. An hour BEFORE the oldest explicit partition is
// a gap unless archived: it was rotated out, or the index never held it. An
// hour AFTER the newest explicit partition is LIVE: its rows sit in p_future
// (the add-future horizon lapsed — a standalone stream with no rotate cron, or
// rotation switched off), which is never dropped, so by construction nothing
// there was rotated out. The metrics scraper bounds its gap count the same way
// (streamrun.gapScrapeRange); without the cap every cascade on such an index
// would lose Phase-2 the moment the horizon lapsed, with a caveat asserting a
// rotation that never happened.
//
// Partitions and archives only: a stamped permanent capture loss inside the
// window (stream_state.gap_lost_at, #765) is invisible here and must be
// checked by the caller — cascade.WindowProbe does both.
//
// Fail-closed: an unreadable partition list is returned as an error, and an
// index with no explicit hourly partition at all, or no database name, reports
// false. Neither is ever "covered".
func WindowCovered(ctx context.Context, db *sql.DB, dbName string, since, until time.Time, noArchive bool) (bool, error) {
	if db == nil || dbName == "" {
		return false, nil
	}
	liveHours, err := loadLivePartitionHours(ctx, db, dbName)
	if err != nil {
		return false, fmt.Errorf("load partition info for the window-coverage check: %w", err)
	}
	if len(liveHours) == 0 {
		return false, nil
	}
	covered := make(map[time.Time]bool, len(liveHours))
	newest := liveHours[0]
	for _, h := range liveHours {
		covered[h] = true
		if h.After(newest) {
			newest = h
		}
	}
	if !noArchive {
		cov, err := loadArchiveCoverage(ctx, db, AllArchives())
		if err != nil && !isMissingTableErr(err) {
			slog.Warn("window-coverage check: could not read archive coverage; archived hours count as gaps", "error", err)
		}
		for _, h := range expandArchiveHours(cov) {
			covered[h] = true
		}
	}
	end := until.UTC().Truncate(time.Hour)
	for h := since.UTC().Truncate(time.Hour); !h.After(end); h = h.Add(time.Hour) {
		if h.After(newest) {
			break // past the horizon: p_future, never rotated
		}
		if !covered[h] {
			return false, nil
		}
	}
	return true, nil
}
