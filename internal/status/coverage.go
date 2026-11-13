package status

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/go-sql-driver/mysql"
)

// ContinuityStatus is the single rule for the machine-readable continuity
// verdict STRINGS — the status JSON and GET /api/coverage (#1194) both call
// it, so those two surfaces can never disagree (the Report.ExitError()
// discipline). The text renderer branches on the same StreamStateInfo
// predicates (GapUnacknowledged, GapOnRecord) for its own wording, and
// --fail-on-gap on GapUnacknowledged alone; the wire vocabulary lives here.
//
//	"ok"               — no loss on record for the range of changes still
//	                     held (NOT a liveness assertion)
//	"gap_lost"         — an unfillable gap was stamped and nobody has
//	                     acknowledged it: events permanently lost. However
//	                     old the loss is: the window below never applies to it
//	"gap_acknowledged" — an unfillable gap was stamped, an operator
//	                     acknowledged it, and it is inside the window of
//	                     changes still held (see GapOnRecord). The alarm is
//	                     silenced; the events are as lost as under "gap_lost",
//	                     and a window that spans the loss is still refused.
//	                     Never "ok" while the window reaches it
//	"unknown"          — legacy index without the gap columns; never a false
//	                     "ok"
//	"unavailable"      — stream_state could not be read; never a false "ok"
//	"none"             — no stream row (file-mode index): no capture ran, so
//	                     no continuity could break — a genuine no-claim, not
//	                     a hole
//
// The window is stream.Floor, which only a collector fills (CollectStatus,
// CollectCoverageSummary, or LoadGapWindow after a bare LoadStreamState). A
// stream without it grades an acknowledged loss "gap_acknowledged" however
// old it is: the conservative answer, and the one this function gave before
// the window existed. The window can start before the delta floor the
// coverage summary reports as its start (see lookBelowFloor), never after it.
func ContinuityStatus(stream *StreamStateInfo, streamErr error) string {
	switch {
	case stream == nil && streamErr != nil:
		return ContinuityUnavailable
	case stream == nil:
		return ContinuityNone
	case stream.GapUnacknowledged():
		return ContinuityGapLost
	case stream.GapOnRecord():
		// Its own verdict, never ContinuityOK: "ok" over a hole this index
		// knows about is what a consumer paints green. What an
		// acknowledgement silences is the ALARM (--fail-on-gap, the webhook,
		// the gauge, lost_position), and each of those decides on
		// GapUnacknowledged, not on this string.
		return ContinuityGapAcknowledged
	case !stream.GapColumnsPresent:
		return ContinuityUnknown
	default:
		return ContinuityOK
	}
}

// The continuity verdict vocabulary, named so a consumer branches on a symbol
// instead of a literal. Only ContinuityGapLost, ContinuityGapAcknowledged and
// ContinuityOK are claims about the captured range, and only ContinuityOK says
// nothing is known to be missing; a consumer that treats everything-but-gap_lost
// as healthy folds an acknowledged loss and the two "never a false ok" verdicts
// into a pass, which is the error this vocabulary exists to prevent.
const (
	ContinuityOK      = "ok"
	ContinuityGapLost = "gap_lost"
	// ContinuityGapAcknowledged: a loss is on record and an operator
	// acknowledged it. Not healthy and not alarming: on record.
	ContinuityGapAcknowledged = "gap_acknowledged"
	ContinuityUnknown         = "unknown"
	ContinuityUnavailable     = "unavailable"
	ContinuityNone            = "none"
)

// CoverageSummary is the lean live-RPO view behind the console's coverage
// card (#1194): the reconstructable delta window, capture lag, and the
// continuity verdict. Deliberately cheap — no COUNT(*), no index-size scan,
// and the upper edge comes from per-partition MAX probes (see
// NewestIndexedEvent) rather than a whole-table MAX — because it loads on
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
	// GapLostAt and GapAckedAt are the recorded loss and its acknowledgement,
	// set only when Continuity is ContinuityGapAcknowledged: what a surface
	// needs to say WHEN changes were lost and that someone acknowledged it,
	// instead of a bare verdict word.
	GapLostAt  time.Time
	GapAckedAt time.Time
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

// acknowledgedLossInWindow is the continuity verdict of a stream that holds an
// ACKNOWLEDGED loss stamped at lostAt: still on record while the loss is
// inside the window of changes still held, which starts at floor (the delta
// floor, moved back by lookBelowFloor to an older change still held), and ok
// once the window starts after it. The record itself is never erased, so without this a loss from
// years ago would keep every later verdict off "ok" although nothing still
// held spans that moment. A loss AT the floor counts as inside. With no
// floor, or a floor that archives may extend further back (BelowIsUnknown),
// the window's start is not known and the loss stays on record: "ok" is never
// asserted from a start that could not be established.
//
// The ONLY implementation of that rule: every surface reaches it through
// StreamStateInfo.GapOnRecord.
func acknowledgedLossInWindow(lostAt time.Time, floor DeltaFloor) string {
	if !floor.Hour.IsZero() && !floor.BelowIsUnknown && lostAt.Before(floor.Hour) {
		return ContinuityOK
	}
	return ContinuityGapAcknowledged
}

// GapOnRecord reports whether an acknowledged loss is still reported: one is
// recorded, an operator acknowledged it, and it is inside the window of
// changes still held (s.Floor, through acknowledgedLossInWindow). It is what
// every DISPLAY of an acknowledged loss keys on: the continuity verdict, the
// status text and JSON, the coverage summary.
//
// False for an unacknowledged loss, which is GapUnacknowledged's and is never
// narrowed by the window. Readers that decide whether a window is complete
// must not use it either: they read GapLostAt against their own window.
func (s *StreamStateInfo) GapOnRecord() bool {
	return s.GapAcknowledged() &&
		acknowledgedLossInWindow(s.GapLostAt.Time, s.Floor) == ContinuityGapAcknowledged
}

// LoadGapWindow fills stream.Floor, the start of the window of changes still
// held, so the continuity verdict of an ACKNOWLEDGED loss can tell a loss
// that window still reaches from one it no longer does. It reads the floor
// (OldestDeltaFromDB) only when the stream holds an acknowledged loss: no
// other verdict depends on it, and CollectStatus runs on a timer. Then
// lookBelowFloor checks what the floor cannot see.
//
// A floor that cannot be read is left unknown, which keeps the loss on record,
// and is said once per condition (floorWarnings). Safe on a nil stream.
func LoadGapWindow(ctx context.Context, db *sql.DB, dbName string, stream *StreamStateInfo) {
	if !stream.GapAcknowledged() {
		return
	}
	floor, err := OldestDeltaFromDB(ctx, db, dbName)
	if err != nil {
		floorWarnings.failed(ctx, db, "gap-window",
			"could not determine the delta-coverage floor; the acknowledged capture loss stays on record", dbName, err)
		return
	}
	floorWarnings.answered(db, "gap-window",
		"the delta-coverage floor can be read again; the acknowledged capture loss is graded against it", dbName)
	stream.Floor = floor
	lookBelowFloor(ctx, db, dbName, stream)
}

// lookBelowFloor moves stream.Floor back to a change still held from at or
// before an acknowledged loss, when the delta floor alone would put the loss
// outside the window (#2669). The delta floor is the right start for grading
// snapshots and misses two places that still serve older changes:
//
//   - archived hours that do not reach the live partitions without a break.
//     The floor stops at the break, but those hours can still be queried, and
//     a query over them and the live hours spans the loss;
//   - events dated before the name of the partition that holds them. The
//     oldest partition takes every event older than its bound, so an old
//     binlog indexed after rotation puts changes from before the loss under a
//     later hour's name. Archived the same way, they are seen through
//     archive_state.min_event_ts.
//
// It runs only when the verdict would otherwise be "ok", so the common case
// costs nothing. A check that fails, a cancelled request included, leaves the
// window's start unknown, which keeps the loss on record: "ok" is never said
// from a look that did not finish. Only stream.Floor changes; the floor a
// caller grades snapshots against or reports as delta_from is its own copy.
func lookBelowFloor(ctx context.Context, db *sql.DB, dbName string, stream *StreamStateInfo) {
	if !stream.GapAcknowledged() || stream.GapOnRecord() {
		return
	}
	held, err := heldAtOrBefore(ctx, db, stream.GapLostAt.Time)
	if err != nil {
		stream.Floor = DeltaFloor{}
		floorWarnings.failed(ctx, db, "gap-below",
			"could not check whether changes from before the acknowledged capture loss are still held; it stays on record", dbName, err)
		return
	}
	floorWarnings.answered(db, "gap-below",
		"changes from before the acknowledged capture loss can be checked again", dbName)
	if !held.IsZero() {
		stream.Floor.Hour = held.UTC().Truncate(time.Hour)
	}
}

// floorWarnings says a failed read of the window's start at Warn once per
// condition: on the first failure for an index, and again only after a read of
// it has worked (#2669). The readers run on timers and on every page load, so
// a Warn per read turned one outage into a line per request. A read whose own
// request was cancelled (a browser that navigated away, a daemon stopping) is
// not a condition of the index and is logged at Debug only, without touching
// the state; one that ran out of time is.
var floorWarnings = &onceWarner{failing: map[warnKey]bool{}}

// warnKey is one condition: one question asked of one index connection. The
// *sql.DB tells apart two index servers whose databases share a name; every
// caller holds its handle for the life of the index (the console's bundle,
// the MCP target, one `bintrail status` run). An entry lives only while its
// condition stands, so a handle that is gone leaves at most one behind.
type warnKey struct {
	db       *sql.DB
	question string
}

type onceWarner struct {
	mu      sync.Mutex
	failing map[warnKey]bool
}

func (w *onceWarner) failed(ctx context.Context, db *sql.DB, question, msg, dbName string, err error) {
	// Only a cancellation is the caller going away. A deadline is a read that
	// did not answer in time, which is exactly what this exists to report.
	if errors.Is(ctx.Err(), context.Canceled) {
		slog.Debug(msg, "db", dbName, "error", err, "cancelled", ctx.Err())
		return
	}
	key := warnKey{db, question}
	w.mu.Lock()
	first := !w.failing[key]
	w.failing[key] = true
	w.mu.Unlock()
	if first {
		slog.Warn(msg, "db", dbName, "error", err)
		return
	}
	slog.Debug(msg, "db", dbName, "error", err)
}

func (w *onceWarner) answered(db *sql.DB, question, msg, dbName string) {
	key := warnKey{db, question}
	w.mu.Lock()
	was := w.failing[key]
	delete(w.failing, key)
	w.mu.Unlock()
	if was {
		slog.Info(msg, "db", dbName)
	}
}

// heldAtOrBefore returns a moment at or before at from which the index or its
// archives still hold a change, or zero when they hold none. It is not the
// oldest such moment, only one: the caller needs to know whether the window
// reaches at, not how far it goes.
//
// The archives answer first, from archive_state: an archived hour that starts
// at or before at, or an archived file whose recorded oldest event is. The
// index answers next, through idx_row_lookup (one dive per table, not a scan
// of the partition) and partition pruning, which leaves only the oldest
// partition to look in. A missing archive_state is an index with no archive
// tier.
func heldAtOrBefore(ctx context.Context, db *sql.DB, at time.Time) (time.Time, error) {
	at = at.UTC()
	held, err := archivedAtOrBefore(ctx, db, at)
	if err != nil {
		return time.Time{}, err
	}
	if !held.IsZero() {
		return held, nil
	}
	var ev sql.NullTime
	err = db.QueryRowContext(ctx, `SELECT MIN(event_timestamp) FROM binlog_events WHERE event_timestamp <= ? GROUP BY schema_name, table_name LIMIT 1`, at).Scan(&ev)
	if errors.Is(err, sql.ErrNoRows) {
		return time.Time{}, nil
	}
	if err != nil {
		return time.Time{}, fmt.Errorf("look for indexed changes from before %s: %w", at.Format(TSFmt), err)
	}
	if !ev.Valid || ev.Time.IsZero() || ev.Time.After(at) {
		return time.Time{}, nil
	}
	return ev.Time, nil
}

// archivedAtOrBefore is heldAtOrBefore's question to archive_state, zero when
// no archived change at or before at is known. Only values at or before at
// count: the MIN over the matching rows can be a later hour's name when a row
// matched by its recorded oldest event.
//
// A min_event_ts stored as the zero date (a non-strict sql_mode, or old data)
// is not an event time. It is left out in the query, so it can neither win the
// MIN over a real recorded event nor match the row by itself, and ignored here
// as a second belt; that row still counts by its hour's name. An archive_state
// without the column (1054: created before it and not migrated, as the
// planner allows for) is asked by hour names only.
func archivedAtOrBefore(ctx context.Context, db *sql.DB, at time.Time) (time.Time, error) {
	hour := at.Format("p_2006010215")
	var minPartition sql.NullString
	var minEvent sql.NullTime
	err := db.QueryRowContext(ctx, `SELECT MIN(partition_name), MIN(CASE WHEN min_event_ts > '1000-01-01' THEN min_event_ts END) FROM archive_state WHERE partition_name <= ? OR (min_event_ts <= ? AND min_event_ts > '1000-01-01')`,
		hour, at).Scan(&minPartition, &minEvent)
	var myErr *mysql.MySQLError
	if errors.As(err, &myErr) && myErr.Number == 1054 {
		minEvent = sql.NullTime{}
		err = db.QueryRowContext(ctx, `SELECT MIN(partition_name) FROM archive_state WHERE partition_name <= ?`, hour).Scan(&minPartition)
	}
	switch {
	case err == nil:
	case isMissingTableErr(err):
		return time.Time{}, nil
	default:
		return time.Time{}, fmt.Errorf("look for archived changes from before %s: %w", at.Format(TSFmt), err)
	}
	var held time.Time
	if minPartition.Valid {
		t, ok := parsePartitionName(minPartition.String)
		if !ok {
			return time.Time{}, fmt.Errorf("archive_state partition_name %q is unparseable", minPartition.String)
		}
		if !t.After(at) {
			held = t
		}
	}
	if minEvent.Valid && !minEvent.Time.IsZero() && !minEvent.Time.After(at) && (held.IsZero() || minEvent.Time.Before(held)) {
		held = minEvent.Time
	}
	return held, nil
}

// CollectCoverageSummary computes the summary against one index. The floor
// degrades to unknown on error (warn-and-degrade, CollectStatus's stance),
// as does a stream_state read failure ("unavailable"); a failure to read the
// newest event is fatal — without the window's upper edge there is nothing
// to state.
func CollectCoverageSummary(ctx context.Context, db *sql.DB, dbName string, now time.Time) (*CoverageSummary, error) {
	sum := &CoverageSummary{}
	if floor, err := OldestDeltaFromDB(ctx, db, dbName); err != nil {
		floorWarnings.failed(ctx, db, "coverage",
			"could not determine the delta-coverage floor; coverage window start is unknown", dbName, err)
	} else {
		floorWarnings.answered(db, "coverage",
			"the delta-coverage floor can be read again; the coverage window has a start", dbName)
		sum.Floor = floor
	}
	latest, err := NewestIndexedEvent(ctx, db, dbName)
	if err != nil {
		return nil, err
	}
	sum.DeltaTo = latest
	stream, streamErr := LoadStreamState(ctx, db)
	if streamErr != nil {
		slog.Warn("could not load stream state for the coverage summary", "db", dbName, "error", streamErr)
	}
	if stream != nil {
		// The floor this summary already read starts the window: one read,
		// and the window can only start at or before the delta_from beside
		// it, through the same look below it LoadGapWindow takes.
		stream.Floor = sum.Floor
		lookBelowFloor(ctx, db, dbName, stream)
	}
	sum.Continuity = ContinuityStatus(stream, streamErr)
	if sum.Continuity == ContinuityGapAcknowledged {
		sum.GapLostAt, sum.GapAckedAt = stream.GapLostAt.Time, stream.GapAckedAt.Time
	}
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

// NewestIndexedRow is the event_timestamp of the index's newest row by
// primary key: one index dive per partition, merged, where NewestIndexedEvent
// reads the newest partition whole (no index leads with event_timestamp).
// It is at most the exact newest event, never past it: the newest row by id
// is the last one capture wrote, whose time is the newest unless an older
// binlog was indexed after live capture, when it reads older until the next
// live batch. The snapshot update records it as the newest change a copy
// holds (#2201, #2509): that record must be older than the copy's data,
// never newer, and this is, at the cost of one partition dive instead of
// one partition scan per update. Zero with a nil error is an empty index.
func NewestIndexedRow(ctx context.Context, db *sql.DB) (time.Time, error) {
	var t sql.NullTime
	found, err := edgeRowByKey(ctx, db, "DESC", &t)
	if err != nil || !found {
		return time.Time{}, err
	}
	return t.Time, nil
}

// NewestIndexedEvent finds the newest event_timestamp with per-partition MAX
// probes, newest partition first, stopping at the first non-empty one. A
// whole-table MAX(event_timestamp) would be a full index scan — no index
// leads with event_timestamp (the PK leads with event_id) — which is exactly
// the O(rows) cost this summary exists to avoid. p_future is probed FIRST:
// it is the catch-all that holds the NEWEST rows whenever the future-
// partition horizon lags. Cost: one probe per empty trailing partition, one
// probe on the first non-empty — each bounded by a single hourly partition.
func NewestIndexedEvent(ctx context.Context, db *sql.DB, dbName string) (time.Time, error) {
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
