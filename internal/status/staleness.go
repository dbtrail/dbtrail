package status

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/go-sql-driver/mysql"
)

// Baseline staleness (#1193): full-table reconstruct = newest usable baseline
// + deltas since it. Deltas are bounded by rotation retention plus archives;
// when a baseline's anchor slides toward — or past — the oldest available
// delta, the restore window is shrinking or already broken, and the operator
// must learn that from status/console/alerts NOW, not at restore time.
type BaselineStalenessVerdict string

const (
	BaselineOK     BaselineStalenessVerdict = "ok"
	BaselineAging  BaselineStalenessVerdict = "aging"
	BaselineBroken BaselineStalenessVerdict = "broken"
	// BaselineUnknown = no evaluable delta floor (unpartitioned/unreachable
	// index, no archives). Unknown is never "ok" — the same fail-closed rule
	// as continuity's "unknown" verdict.
	BaselineUnknown BaselineStalenessVerdict = "unknown"
)

// baselineAgingFraction: a baseline older than this fraction of the delta
// coverage span is "aging" — the restore window is shrinking.
//
// Known bootstrap artifact: until rotation saturates retention, the span is
// the install's age, so a young install reads "aging" within hours of its
// first baseline. That is why "aging" stays an informational verdict on
// status/console and is deliberately NOT wired to the webhook channel — an
// alert that fires on every fresh install would be cried-wolf into a mute
// before it ever mattered.
const baselineAgingFraction = 0.8

// BaselineAgingFraction is the same band, exported for a caller that has to
// bound something against it without a floor to grade — see the baseline
// refresh gate, which caps how long it will leave a backup unrepublished
// against the CONFIGURED retention rather than against the partitions that
// happen to exist at that moment.
const BaselineAgingFraction = baselineAgingFraction

// BaselineStalenessFor grades one snapshot anchor against the oldest instant
// deltas are available from.
func BaselineStalenessFor(snapshotTime, oldestDelta, now time.Time) BaselineStalenessVerdict {
	if snapshotTime.IsZero() || oldestDelta.IsZero() {
		return BaselineUnknown
	}
	if snapshotTime.Before(oldestDelta) {
		return BaselineBroken
	}
	span := now.Sub(oldestDelta)
	if span <= 0 {
		return BaselineOK
	}
	if float64(now.Sub(snapshotTime)) >= baselineAgingFraction*float64(span) {
		return BaselineAging
	}
	return BaselineOK
}

// DeltaFloor is the delta-coverage floor plus how to read a snapshot older
// than it. It exists because the two halves of the floor have different
// scopes (#1219): the live partitions are SHARED — every source writes into
// the same range-partitioned binlog_events, so their oldest hour is a valid
// floor for every source without attributing anything — while archive_state
// rows are PER-SOURCE. Extending the floor backwards with the union MIN of a
// multi-source index hands source A's archive coverage to source B.
type DeltaFloor struct {
	// Hour is the floor to grade against; zero = unknown, never assumed.
	Hour time.Time
	// BelowIsUnknown marks Hour as the LIVE-partition floor only, because the
	// archives could not be attributed to the source that owns the graded
	// baselines. A snapshot older than Hour may still be covered by that
	// source's own archives, so it grades unknown rather than broken —
	// reporting "broken" on an unattributable snapshot is a false alarm, and
	// a false alarm is worse than no check at all.
	BelowIsUnknown bool
}

// Grade returns the staleness verdict of one snapshot against this floor. It
// is the single place the ambiguity demotion lives: below an unattributable
// floor, "broken" becomes "unknown".
//
// Only "broken" is demoted, deliberately. A snapshot ABOVE the floor is
// covered by the live partitions every source shares, so ok/aging need no
// attribution — and an unattributable floor makes the span shorter, so
// "aging" fires earlier than it would against the archive-extended floor.
// That reads as a conservative "your provable window is shrinking", which is
// the honest thing to say; demoting it too would erase the one signal still
// standing on those indexes.
func (f DeltaFloor) Grade(snapshotTime, now time.Time) BaselineStalenessVerdict {
	v := BaselineStalenessFor(snapshotTime, f.Hour, now)
	if v == BaselineBroken && f.BelowIsUnknown {
		return BaselineUnknown
	}
	return v
}

// OldestLivePartitionHour is the live half of the delta floor: the hour of
// the oldest non-future binlog_events partition. Partition EXISTENCE is
// coverage — the planner's own rule. MIN(event_timestamp) must NOT be used
// here: it is the first WRITE, not the coverage start, and on a quiet
// database it would fabricate a "broken" verdict (baseline taken before the
// first write) on a perfectly restorable index.
func OldestLivePartitionHour(parts []PartitionStat) time.Time {
	var out time.Time
	for _, p := range parts {
		t, ok := parsePartitionName(p.Name)
		if !ok {
			continue // p_future / malformed
		}
		if out.IsZero() || t.Before(out) {
			out = t
		}
	}
	return out
}

// OldestDeltaFromDB computes the delta-coverage floor: the live-partition
// floor extended backwards by contiguous archives. It is the ONLY floor
// implementation — the CLI, console, and watcher all use it (CoverageInfo's
// ArchiveEarliestHour is a best-effort DISPLAY figure that reports a read
// failure as ArchiveUnavailable rather than returning it, so a verdict must
// never be built on it). Error semantics are
// strict in the anti-cry-wolf direction: any failure that could make the
// floor read LATER than reality — and so fabricate "broken" on healthy
// archives — is returned (the caller degrades to unknown), never swallowed.
// Only a missing archive_state table (older indexes) is tolerated.
func OldestDeltaFromDB(ctx context.Context, db *sql.DB, dbName string) (DeltaFloor, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT PARTITION_NAME FROM information_schema.PARTITIONS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'binlog_events' AND PARTITION_NAME IS NOT NULL`, dbName)
	if err != nil {
		return DeltaFloor{}, fmt.Errorf("list live partitions: %w", err)
	}
	defer rows.Close()
	var parts []PartitionStat
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return DeltaFloor{}, err
		}
		parts = append(parts, PartitionStat{Name: name})
	}
	if err := rows.Err(); err != nil {
		return DeltaFloor{}, err
	}
	floor := DeltaFloor{Hour: OldestLivePartitionHour(parts)}

	// COUNT(DISTINCT bintrail_id) rides the query that was already here — no
	// extra round trip. Every writer stamps a non-empty id (CLI rotate via
	// errNoArchiveBintrailID, the watch control plane via its own empty-id
	// drop-only branch, and reconcile --repair / restore-index from the
	// bintrail_id=<id> path segment), but the COLUMN is nullable: a NULL row
	// would drop out of the COUNT while still feeding MIN(partition_name), so
	// the count is treated as evidence of attribution, never as proof — see
	// the archivedSources == 0 branch below.
	var minPartition, maxPartition sql.NullString
	var archivedSources int
	if err := db.QueryRowContext(ctx, `SELECT MIN(partition_name), MAX(partition_name), COUNT(DISTINCT bintrail_id) FROM archive_state`).Scan(&minPartition, &maxPartition, &archivedSources); err != nil {
		var myErr *mysql.MySQLError
		if errors.As(err, &myErr) && myErr.Number == 1146 {
			return floor, nil // archive_state absent on older indexes — live floor only
		}
		return DeltaFloor{}, fmt.Errorf("read archive floor: %w", err)
	}
	if !minPartition.Valid {
		return floor, nil // no archives: nothing to extend, nothing to attribute
	}
	minT, ok := parsePartitionName(minPartition.String)
	if !ok {
		// Our own naming scheme failing to parse is drift, and silently
		// dropping the archive floor would fabricate "broken".
		return DeltaFloor{}, fmt.Errorf("archive_state MIN(partition_name) %q is unparseable", minPartition.String)
	}
	maxT, ok := parsePartitionName(maxPartition.String)
	if !ok {
		return DeltaFloor{}, fmt.Errorf("archive_state MAX(partition_name) %q is unparseable", maxPartition.String)
	}

	// Per-source scoping (#1219). Two signals, because either alone leaves a
	// hole: more than one ARCHIVED source is the direct evidence, and more
	// than one KNOWN source catches the case where only one of them has
	// archived so far — whose union MIN would still be handed to the other's
	// baselines. Neither identifies WHICH source owns a given baseline (a
	// baseline snapshot carries no source identity), so the answer here can
	// only be "attributable" or "not".
	if archivedSources == 0 {
		// Archived hours exist but carry NO source identity (only reachable
		// through a hand-written or pre-identity row, since the column is
		// nullable). Rows that name no source are the strongest case of
		// "cannot attribute", so they must not extend anyone's floor — the
		// count reading 0 while MIN is valid is exactly that.
		floor.BelowIsUnknown = true
		return floor, nil
	}
	multiSource := archivedSources > 1
	if !multiSource {
		known, err := knownSourceCount(ctx, db)
		if err != nil {
			return DeltaFloor{}, err
		}
		multiSource = known > 1
	}
	if multiSource {
		// Archives belonging to sources this call cannot tell apart never
		// extend the floor: the live partitions (shared by every source) stay
		// the floor, and everything below becomes unknowable rather than
		// either covered or broken.
		floor.BelowIsUnknown = true
		return floor, nil
	}

	// Archives extend the floor backwards ONLY when their range reaches
	// the live partitions: if the newest archived hour ends before the
	// oldest live partition begins (archiving stopped, middle range
	// pruned), every restore anchored before the live floor crosses that
	// hole — so the live floor IS the coverage floor, and extending it
	// would grade those baselines with an unearned "ok". Interior holes
	// within the archive range are still invisible here; reconstruct's
	// planner gap check catches those at restore time.
	contiguous := floor.Hour.IsZero() || !maxT.Add(time.Hour).Before(floor.Hour)
	if contiguous && (floor.Hour.IsZero() || minT.Before(floor.Hour)) {
		floor.Hour = minT
	}
	return floor, nil
}

// knownSourceCount counts the source servers this index has ever identified.
// Decommissioned rows count: their archives still sit in archive_state and
// would extend another source's floor just the same. A missing table (1146)
// is a legacy or file-mode index — zero known sources, single-source
// semantics preserved.
func knownSourceCount(ctx context.Context, db *sql.DB) (int, error) {
	var n int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM bintrail_servers`).Scan(&n); err != nil {
		var myErr *mysql.MySQLError
		if errors.As(err, &myErr) && myErr.Number == 1146 {
			return 0, nil
		}
		return 0, fmt.Errorf("count known sources: %w", err)
	}
	return n, nil
}

// AnnotateBaselineStaleness stamps each entry's verdict in place. Every entry
// is graded on its own anchor — a superseded old snapshot showing "broken" is
// honest (it IS unusable); what decides the headline is
// OverallBaselineStaleness, which only looks at each table's newest snapshot.
func AnnotateBaselineStaleness(baselines []BaselineInfo, floor DeltaFloor, now time.Time) {
	for i := range baselines {
		baselines[i].Staleness = floor.Grade(baselines[i].SnapshotTime, now)
	}
}

// OverallBaselineStaleness is the worst verdict across each table's NEWEST
// snapshot. "" when the list is empty or unannotated.
func OverallBaselineStaleness(baselines []BaselineInfo) BaselineStalenessVerdict {
	// Unknown outranks aging: aging is informational (it fires on every young
	// install and never alerts), while unknown means the restore window could
	// not be established at all. #1219 makes unknown the ROUTINE verdict for
	// below-floor snapshots on multi-source indexes, so ranking it under
	// aging would headline "mildly old" over "could not be checked".
	rank := map[BaselineStalenessVerdict]int{BaselineOK: 1, BaselineAging: 2, BaselineUnknown: 3, BaselineBroken: 4}
	newest := make(map[string]BaselineInfo, len(baselines))
	for _, b := range baselines {
		k := b.Database + "." + b.Table
		if cur, ok := newest[k]; !ok || cur.SnapshotTime.Before(b.SnapshotTime) {
			newest[k] = b
		}
	}
	var out BaselineStalenessVerdict
	for _, b := range newest {
		if rank[b.Staleness] > rank[out] {
			out = b.Staleness
		}
	}
	return out
}
