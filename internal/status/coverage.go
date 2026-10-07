package status

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"sort"
	"time"
)

// ContinuityStatus is the single rule for the machine-readable continuity
// verdict STRINGS — the status JSON and GET /api/coverage (#1194) both call
// it, so those two surfaces can never disagree (the Report.ExitError()
// discipline). The text renderer and --fail-on-gap re-derive equivalent
// buckets from the same StreamStateInfo fields for their own wording/exit
// concerns; the wire vocabulary lives here.
//
//	"ok"          — no gap in the captured range (NOT a liveness assertion)
//	"gap_lost"    — an unfillable gap was stamped: events permanently lost
//	"unknown"     — legacy index without the gap columns; never a false "ok"
//	"unavailable" — stream_state could not be read; never a false "ok"
//	"none"        — no stream row (file-mode index): no capture ran, so no
//	                continuity could break — a genuine no-claim, not a hole
func ContinuityStatus(stream *StreamStateInfo, streamErr error) string {
	switch {
	case stream == nil && streamErr != nil:
		return ContinuityUnavailable
	case stream == nil:
		return ContinuityNone
	case stream.GapLostAt.Valid:
		return ContinuityGapLost
	case !stream.GapColumnsPresent:
		return ContinuityUnknown
	default:
		return ContinuityOK
	}
}

// The continuity verdict vocabulary, named so a consumer branches on a symbol
// instead of a literal. Only ContinuityGapLost and ContinuityOK are claims
// about the captured range; a consumer that treats everything-but-gap_lost as
// healthy folds the two "never a false ok" verdicts into a pass, which is the
// error this vocabulary exists to prevent.
const (
	ContinuityOK          = "ok"
	ContinuityGapLost     = "gap_lost"
	ContinuityUnknown     = "unknown"
	ContinuityUnavailable = "unavailable"
	ContinuityNone        = "none"
)

// CoverageSummary is the lean live-RPO view behind the console's coverage
// card (#1194): the reconstructable delta window, capture lag, and the
// continuity verdict. Deliberately cheap — no COUNT(*), no index-size scan,
// and the upper edge comes from per-partition MAX probes (see
// newestIndexedEvent) rather than a whole-table MAX — because it loads on
// every server switch; CollectStatus stays the full report.
type CoverageSummary struct {
	// Floor is the delta-coverage floor (OldestDeltaFromDB — the #1213 strict
	// rule); Floor.Hour zero = unknown, never assumed. The whole DeltaFloor
	// travels, not just the hour: on a multi-source index Hour is the
	// LIVE-partition floor and BelowIsUnknown says so, and a consumer that
	// grades baselines against the hour ALONE turns this narrower floor into
	// false "broken" verdicts (#1219). Grade through Floor, never through
	// BaselineStalenessFor with Floor.Hour.
	//
	// Caveat that outlives this field: partition existence is the coverage
	// rule, so a source that started capturing after the oldest live
	// partition reads a wider window here than it can actually restore.
	Floor DeltaFloor
	// DeltaTo is the newest INDEXED event — never the wall clock: claiming
	// restorability "up to now" while the stream is down would be unearned
	// assurance. LagSeconds is what says how close to now the edge is.
	DeltaTo time.Time
	// LagSeconds = now − DeltaTo, present only when a stream row exists AND
	// at least one event is indexed — a file-mode index has no liveness to
	// measure, and an empty index has no edge to measure from.
	LagSeconds *int64
	Continuity string
	// Freshness is the LIVENESS verdict (#1226/#1227), computed here from the
	// same stream row Continuity uses so the console cannot reach a different
	// answer than `bintrail status`. It is what disambiguates LagSeconds: a big
	// lag under "stalled" is a dead daemon, the same number under "idle" is a
	// source nobody wrote to, and those two need opposite operator responses.
	Freshness string
	// CheckpointAgeSeconds is how long ago the daemon last wrote stream_state.
	// Nil when there is no checkpoint to age — never 0, which would render as a
	// confident "just now" for a daemon that has never checkpointed.
	CheckpointAgeSeconds *int64
	// Note: capture-health drops (#1034 capture_skips) are deliberately NOT
	// folded into this summary — they are a capture-plane verdict with their
	// own surface (status Capture health, --fail-on-gap). The delta window
	// here is about what the index CONTAINS.
}

// CollectCoverageSummary computes the summary against one index. The floor
// degrades to unknown on error (warn-and-degrade, CollectStatus's stance),
// as does a stream_state read failure ("unavailable"); a failure to read the
// newest event is fatal — without the window's upper edge there is nothing
// to state.
func CollectCoverageSummary(ctx context.Context, db *sql.DB, dbName string, now time.Time) (*CoverageSummary, error) {
	sum := &CoverageSummary{}
	if floor, err := OldestDeltaFromDB(ctx, db, dbName); err != nil {
		slog.Warn("could not determine the delta-coverage floor; coverage window start is unknown", "db", dbName, "error", err)
	} else {
		sum.Floor = floor
	}
	latest, err := newestIndexedEvent(ctx, db, dbName)
	if err != nil {
		return nil, err
	}
	sum.DeltaTo = latest
	stream, streamErr := LoadStreamState(ctx, db)
	if streamErr != nil {
		slog.Warn("could not load stream state for the coverage summary", "db", dbName, "error", streamErr)
	}
	sum.Continuity = ContinuityStatus(stream, streamErr)
	sum.Freshness = FreshnessStatus(stream, streamErr, now, 0, 0)
	if age, ok := CheckpointAge(stream, now); ok {
		secs := int64(age / time.Second)
		sum.CheckpointAgeSeconds = &secs
	}
	if stream != nil && !sum.DeltaTo.IsZero() {
		lag := int64(now.Sub(sum.DeltaTo) / time.Second)
		if lag < 0 {
			lag = 0
		}
		sum.LagSeconds = &lag
	}
	return sum, nil
}

// NewestIndexedEvent is newestIndexedEvent for a caller outside this package:
// the snapshot fold reads it beside its index mark (#2201), so the instant it
// records as the newest change a copy holds is measured exactly the way the
// coverage window's upper edge is. Zero with a nil error is an empty index.
func NewestIndexedEvent(ctx context.Context, db *sql.DB, dbName string) (time.Time, error) {
	return newestIndexedEvent(ctx, db, dbName)
}

// newestIndexedEvent finds the newest event_timestamp with per-partition MAX
// probes, newest partition first, stopping at the first non-empty one. A
// whole-table MAX(event_timestamp) would be a full index scan — no index
// leads with event_timestamp (the PK leads with event_id) — which is exactly
// the O(rows) cost this summary exists to avoid. p_future is probed FIRST:
// it is the catch-all that holds the NEWEST rows whenever the future-
// partition horizon lags. Cost: one probe per empty trailing partition, one
// probe on the first non-empty — each bounded by a single hourly partition.
func newestIndexedEvent(ctx context.Context, db *sql.DB, dbName string) (time.Time, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT PARTITION_NAME FROM information_schema.PARTITIONS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'binlog_events' AND PARTITION_NAME IS NOT NULL`, dbName)
	if err != nil {
		return time.Time{}, fmt.Errorf("list partitions for newest event: %w", err)
	}
	defer rows.Close()
	var future bool
	var dated []string
	byName := map[string]time.Time{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return time.Time{}, err
		}
		if name == "p_future" {
			future = true
			continue
		}
		if t, ok := parsePartitionName(name); ok {
			dated = append(dated, name)
			byName[name] = t
		}
	}
	if err := rows.Err(); err != nil {
		return time.Time{}, err
	}
	sort.Slice(dated, func(i, j int) bool { return byName[dated[i]].After(byName[dated[j]]) })
	probe := dated
	if future {
		probe = append([]string{"p_future"}, dated...)
	}
	for _, name := range probe {
		var maxTS sql.NullTime
		if err := db.QueryRowContext(ctx, "SELECT MAX(event_timestamp) FROM binlog_events PARTITION (`"+name+"`)").Scan(&maxTS); err != nil {
			return time.Time{}, fmt.Errorf("read newest indexed event (partition %s): %w", name, err)
		}
		if maxTS.Valid {
			return maxTS.Time, nil
		}
	}
	return time.Time{}, nil // empty index — an honest zero, not an error
}
