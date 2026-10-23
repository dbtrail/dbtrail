package streamrun

import (
	"context"
	"database/sql"
	"log/slog"
	"time"

	drivermysql "github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/observe"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/status"
)

// defaultIndexMetricsInterval is how often the bintrail_index_* gauges are
// refreshed from a status snapshot when no interval is configured (#351).
const defaultIndexMetricsInterval = 60 * time.Second

// startIndexMetricsScraper launches a goroutine that periodically publishes the
// bintrail_index_* gauges (recovery floor, gap hours, storage bytes, partition
// counts) for source, derived from a status snapshot of the index DB. It
// returns immediately and stops when ctx is cancelled. A scrape failure is
// logged and the previous gauge values are left in place — a slightly stale
// reading beats a gap or a misleading zero. The work is a handful of
// information_schema queries on a timer, not a per-event path: the snapshot
// is the gauges' one (status.CollectStatusForGauges, #2509), whose event
// span never reads a partition's rows; the exact span costs two whole
// partitions per scrape, which a minute apart forever was most of what the
// index spent on reads.
func startIndexMetricsScraper(ctx context.Context, db *sql.DB, indexDSN, source string, intervalSeconds int) {
	interval := time.Duration(intervalSeconds) * time.Second
	if interval <= 0 {
		interval = defaultIndexMetricsInterval
	}
	dbName := ""
	if c, err := drivermysql.ParseDSN(indexDSN); err == nil {
		dbName = c.DBName
	} else {
		slog.Warn("index metrics scraper: could not parse index DSN for schema name; metrics disabled", "error", err)
		return
	}
	m := observe.IndexForSource(source)
	go func() {
		// Scrape once promptly so the gauges populate without waiting a full
		// interval, then on the ticker.
		scrapeIndexMetrics(ctx, db, dbName, m)
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				scrapeIndexMetrics(ctx, db, dbName, m)
			}
		}
	}()
}

// collectForGauges is the status snapshot the scraper takes; a seam so a
// test can pin that it is the gauges' one, not the exact one.
var collectForGauges = status.CollectStatusForGauges

func scrapeIndexMetrics(ctx context.Context, db *sql.DB, dbName string, m *observe.IndexMetrics) {
	data, err := collectForGauges(ctx, db, dbName)
	if err != nil {
		slog.Warn("index metrics scrape: could not collect status", "error", err)
		return
	}

	snap := observe.IndexSnapshot{HaveCoverage: data.Coverage != nil}
	if data.Coverage != nil {
		if data.Coverage.EarliestEvent.Valid {
			snap.OldestEvent = data.Coverage.EarliestEvent.Time
		}
		if data.Coverage.LatestEvent.Valid {
			snap.NewestEvent = data.Coverage.LatestEvent.Time
		}
		snap.Events = data.Coverage.TotalEvents
		snap.EventsUnknown = data.Coverage.TotalEventsUnknown
		snap.MySQLBytes = data.Coverage.IndexSizeBytes
	}
	var newestExplicit time.Time // newest EXPLICIT hourly partition (excludes p_future)
	for _, p := range data.Parts {
		if p.Name == "p_future" {
			snap.FuturePartitions++
			continue
		}
		snap.ActivePartitions++
		if h, ok := query.ParsePartitionName(p.Name); ok && h.After(newestExplicit) {
			newestExplicit = h
		}
	}
	if data.Archives != nil {
		snap.ParquetBytes = data.Archives.TotalSizeBytes
	}

	// Gap hours: hours ROTATED OUT of MySQL and never archived — holes in
	// recovery coverage. Two bounds matter: since reaches back to the earliest
	// data that should exist (oldest live event or earliest archived hour) so a
	// hole anywhere in the retained span is seen; until is the end of the EXPLICIT
	// hourly partitions, because hours beyond that live in p_future (current data,
	// never rotated) and counting them as gaps would make a no-rotation standalone
	// stream report a steadily climbing false gap_hours (loadLivePartitionHours
	// excludes p_future). A concrete range also keeps the scraper off query.Plan's
	// nil-range short-circuit (the earlier nil-deref panic).
	var archiveEarliest sql.NullTime
	if data.Coverage != nil {
		archiveEarliest = data.Coverage.ArchiveEarliestHour
	}
	if since, until, ok := gapScrapeRange(snap.OldestEvent, archiveEarliest, newestExplicit); ok {
		// AllArchives (#1232): this gauge is about the INDEX, not about any
		// one reader's archive set. bintrail_index_gap_hours answers "is
		// there a hole in what this index retains", so every registered
		// archive counts — scoping it to a reader's subset would make the
		// daemon's metric depend on who happens to be querying.
		if plan, err := query.Plan(ctx, db, dbName, &since, &until, false, query.AllArchives()); err != nil {
			slog.Warn("index metrics scrape: could not plan gap hours", "error", err)
		} else if plan != nil { // belt-and-suspenders: Plan can still return nil
			snap.GapHours = len(plan.GapHours)
		}
	}

	m.Set(snap, time.Now())
}

// gapScrapeRange returns the [since, until] window over which to count coverage
// gap hours, or ok=false when there is no rotated span to measure. since is the
// earlier of the oldest live event and the earliest archived hour (so a hole
// anywhere in the retained span is seen); until is one hour past the newest
// EXPLICIT hourly partition, so the not-yet-rotated p_future tail is excluded —
// otherwise a no-rotation standalone stream over-counts current hours as gaps.
// A concrete (non-nil) range also keeps the scraper off query.Plan's nil-range
// short-circuit (the cause of an earlier nil-deref panic).
func gapScrapeRange(oldest time.Time, archiveEarliest sql.NullTime, newestExplicit time.Time) (since, until time.Time, ok bool) {
	since = oldest
	if archiveEarliest.Valid && (since.IsZero() || archiveEarliest.Time.Before(since)) {
		since = archiveEarliest.Time
	}
	// No data to anchor since, or no explicit partition to bound until → there is
	// no rotated span to measure.
	if since.IsZero() || newestExplicit.IsZero() {
		return time.Time{}, time.Time{}, false
	}
	until = newestExplicit.Add(time.Hour)
	if !until.After(since) {
		return time.Time{}, time.Time{}, false
	}
	return since, until, true
}
