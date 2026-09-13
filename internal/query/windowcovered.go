package query

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// WindowCovered reports whether a scan of [since, until] can be complete:
// every hour of the window is held by a live partition or by an archive in
// scope. scope names the archives the scan will actually OPEN
// (MergedFetcher.Scope) — coverage recorded by an archive the read does not
// open is not coverage (#1232), so a --no-archive or profile-confined scan
// passes a scope that opens nothing and an archived hour is then a gap for it
// whether or not a Parquet file holds it (#1615).
//
// Archive coverage comes from archive_state labels. An archive_state that
// exists but cannot be read is an ERROR, not a silent "no credit": the
// caller's caveat then names the failed check instead of guessing at a
// rotation (a missing table — an index that never archived — is no error).
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
func WindowCovered(ctx context.Context, db *sql.DB, dbName string, since, until time.Time, scope ArchiveScope) (bool, error) {
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
	if !scope.opensNone() {
		cov, err := loadArchiveCoverage(ctx, db, scope)
		if err != nil && !isMissingTableErr(err) {
			return false, fmt.Errorf("read archive coverage for the window-coverage check: %w", err)
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
