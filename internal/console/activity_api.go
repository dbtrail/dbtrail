package console

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/query"
)

// GET /api/activity — the Overview's window aggregate (#1300, redesigned #1352).
//
// Why this exists at all: the Overview used to derive "N deletes" and "N tables
// touched" client-side from a 200-event /api/events fetch. That made the tiles
// describe an arbitrary FETCH window (the newest 200 rows) while the tile beside
// them described the whole index, and it pulled ~700 kB of row images across the
// wire to produce four integers. Both halves are fixed here: the counts come
// from a grouped COUNT(*) over a stated time window, so the tile can name its
// own scope and the page no longer needs the row images.
//
// PRECOMPUTED since #1352, superseding #1300's "deliberately NOT cached" stance.
// That stance reasoned that a cache would freeze exactly the numbers an operator
// is watching — but it lost to an unusable page: the per-request scan put the
// landing page's first paint behind an aggregate measured in tens of seconds.
// Declared staleness beats a per-request scan: the aggregate is materialized
// per server (per bundle), refreshed when older than a TTL that follows what
// the aggregate cost to compute (activityTTL, #1778; at most activityRefreshTTL), and
// every response carries refreshed_at, which the tile renders ("as of …") — a
// frozen number that SAYS when it froze is disclosure, not a lie. The refresh
// is lazy (recompute-on-request-if-stale: a cheap aggregate while the request
// waits, an expensive one in the background while the previous one is
// served), so an idle console runs no scans
// and no daemon loop is needed — the mechanism works identically under `serve`
// and `watch`.
//
// SINGLE-TIER since #1352: the aggregate reads live binlog_events EXCLUSIVELY —
// no archive counts, and no archive_state read for a completeness caveat,
// because the window IS the live retention (see liveWindow). A window equal to
// live coverage cannot contain archived-only hours, so the old planner-backed
// "N hour(s) … archived" caveat has nothing to caveat and left this page.
//
// Deliberately NOT audited. ext.Record fires from surfaces that serve HISTORICAL
// ROW DATA; this endpoint returns counts and table names only — never a row
// image, never a PK — so it sits with /api/status and /api/coverage on the
// metadata-only side of the line ext/audit.go draws. Emitting query.run here
// would put a page load in the trail next to real data reads and dilute it.
const (
	// activityTopTables caps the per-table breakdown returned to the browser.
	// The totals and the distinct-table count are computed over EVERY group —
	// only the rendered list is truncated, so "9 tables touched" is never the
	// length of this slice.
	activityTopTables = 12

	// activityMaxGroups bounds the (schema, table, event_type) groups this
	// handler will fold before it gives up on exactness. MySQL materializes the
	// grouping; the console is a shared daemon (under `bintrail-console watch`
	// it also runs capture), so an index with a pathological table count must
	// not be able to turn a refresh into an unbounded Go-side map. Reaching
	// the cap sets Truncated, and the response then says the counts are a floor
	// rather than presenting them as the window's total.
	activityMaxGroups = 20000

	// activityRefreshTTL is the most a cached aggregate may age before a
	// request triggers a recompute, reached by the aggregate that takes tens
	// of seconds (#1352). The tile prints refreshed_at, so within this budget
	// the numbers are stale-but-disclosed, never stale-and-silent.
	activityRefreshTTL = 30 * time.Minute

	// activityCostFactor ties how long an aggregate is reused to what it cost
	// (#1778): it is recomputed at most once per activityCostFactor times its
	// own compute time, so refreshing spends about 1% of one connection. A
	// flat 30 minutes made a new index, whose aggregate takes milliseconds,
	// hold its first "0 deletes" beside its first delete for half an hour.
	// The cost is measured, not inferred from the result: a zero from a
	// narrow profile on a busy index can be as expensive as any other count.
	activityCostFactor = 100

	// activityMinTTL is the floor: requests within a second share one
	// aggregate however cheap it is.
	activityMinTTL = time.Second

	// activityInlineCost is the compute time below which a stale aggregate is
	// recomputed while the request waits, rather than served stale with the
	// refresh behind it. The Overview fetches the counts once per render, so
	// a refresh behind would show the new number one render late: after the
	// first change, the render that should show it would still show zero.
	activityInlineCost = time.Second

	// activityInlineWait bounds that wait. An aggregate cheap last time can be
	// slow now; past this the request serves the previous one and the flight
	// finishes for the next request.
	activityInlineWait = 2 * time.Second

	// activityComputeTimeout bounds one materialization flight. Flights run on
	// a background context (a browser navigating away must not abort the
	// refresh every OTHER tab is waiting on), so they need their own leash.
	activityComputeTimeout = 5 * time.Minute

	// activityMaxProfiles caps the cached aggregates per server. The cache is
	// keyed by the session's RBAC deny set (each deny profile must see its own
	// counts — a table NAME is exactly what a deny profile withholds), and
	// profiles are operator-configured and few; the cap only stops a
	// pathological profile churn from growing the map without bound.
	activityMaxProfiles = 16

	// activityFallbackWindow is the window when the live floor cannot be
	// determined (no dated partitions yet, or a bundle with no database name):
	// bounded and stated, like the pre-#1352 default.
	activityFallbackWindow = 24 * time.Hour
	activityFallbackLabel  = "last 24 h"
)

// activityTable is one table's breakdown inside the window.
type activityTable struct {
	Schema string `json:"schema"`
	Table  string `json:"table"`
	Insert int64  `json:"insert"`
	Update int64  `json:"update"`
	Delete int64  `json:"delete"`
	Total  int64  `json:"total"`
}

// activityResponse is the GET /api/activity body. Every count in it describes
// [since, until) and nothing else — the frontend prints Label on the tiles so
// the scope travels WITH the number instead of sitting in prose above the card.
type activityResponse struct {
	// Label is what a tile prints ("live retention · ~14 h"). The server owns
	// the wording so the tile and the window can never disagree. Since/Until
	// are the measured window's exact bounds; Until is the refresh time, so a
	// cached response's window edge matches its refreshed_at instead of
	// claiming a currency nobody measured.
	Label string `json:"label"`
	Since string `json:"since"`
	Until string `json:"until"`
	// RefreshedAt is when this aggregate was computed. The counts are a
	// materialization refreshed at most every activityRefreshTTL (#1352),
	// sooner when it is cheap to compute (#1778), and
	// this is the field that keeps that honest: the UI renders it on the tile
	// ("as of …"), so a stale number is visibly stale, never silently so.
	RefreshedAt string `json:"refreshed_at"`

	Total   int64 `json:"total"`
	Inserts int64 `json:"inserts"`
	Updates int64 `json:"updates"`
	Deletes int64 `json:"deletes"`
	// Other is every remaining event_type (DDL, snapshot rows, ...) so
	// inserts+updates+deletes+other == total holds for any index.
	Other int64 `json:"other"`

	// Tables is the DISTINCT (schema, table) count over the whole window, not
	// the length of TopTables.
	Tables    int             `json:"tables"`
	TopTables []activityTable `json:"top_tables"`

	// Complete is false when something in the window is knowably NOT in these
	// numbers; Notes then says what. Since #1352 the only such case is
	// Truncated (the window is the live retention by construction, so no
	// archived-only hours can fall inside it), but the field stays: a tile
	// must never present a narrower number under a wider label.
	Complete bool     `json:"complete"`
	Notes    []string `json:"notes,omitempty"`
	// Truncated reports that activityMaxGroups tripped: the counts are a floor.
	Truncated bool `json:"truncated,omitempty"`
}

func (s *Server) handleActivity(w http.ResponseWriter, r *http.Request) {
	b := s.resolveOr(w, r)
	if b == nil {
		return
	}
	if b.db == nil {
		// An unopened bundle connection must not render as a quiet zero: "0
		// deletes" reads as an assurance nobody earned.
		writeJSONError(w, http.StatusBadGateway, "the selected server's index connection is not open")
		return
	}

	// RBAC: the same deny AND allow rules every read on this server carries —
	// startup floor plus the request session's profile and policy restrictions
	// (#1449). Denied tables are excluded from the SQL and, in allow-list
	// mode, so is every table not listed — they contribute to neither the
	// counts nor the table list (a table NAME is exactly what a restricted
	// session is withheld elsewhere). The cache is keyed by the resolved
	// deny+allow scope for the same reason: two sessions under different
	// policies must never share a materialization.
	opts, err := s.applySessionProfile(r.Context(), r, b, query.Options{
		DenyTables:    s.denyTables,
		RedactColumns: s.redactCols,
		ProfileActive: s.profileActive,
	})
	if err != nil {
		writeSessionProfileError(w, r, err)
		return
	}

	resp, err := b.activityFor(r.Context(), opts.DenyTables, opts.AllowTables)
	if err != nil {
		writeFetchError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

// activityFor returns the materialized aggregate for this server under the
// given deny set, computing it when no cached copy exists (single-flight:
// concurrent misses share one computation) and otherwise as activityCache.get
// describes: a stale cheap copy is recomputed while the request waits, a stale
// expensive one is served with its original refreshed_at while it recomputes
// in the background.
func (b *bundle) activityFor(ctx context.Context, deny, allow []query.SchemaTable) (activityResponse, error) {
	compute := func(cctx context.Context) (activityResponse, error) {
		return computeActivity(cctx, b.db, b.dbName, deny, allow)
	}
	if b.activity == nil {
		// Test-constructed bundles only: every production bundle gets its cache
		// in newBundleDerived (pinned by TestBundleAlwaysCarriesActivityCache).
		return compute(ctx)
	}
	return b.activity.get(ctx, activityScopeKey(deny, allow), compute)
}

// computeActivity is one materialization flight: derive the live-retention
// window, run the aggregate over live binlog_events, fold, and stamp the
// result. This is the ONLY data path behind /api/activity — there is no
// archive read to fall back to (see the #1352 header comment).
func computeActivity(ctx context.Context, db *sql.DB, dbName string, deny, allow []query.SchemaTable) (activityResponse, error) {
	until := time.Now().UTC().Truncate(time.Second)
	since, label, err := liveWindow(ctx, db, dbName, until)
	if err != nil {
		return activityResponse{}, err
	}
	agg, err := collectActivity(ctx, db, since, until, deny, allow)
	if err != nil {
		return activityResponse{}, err
	}
	resp := agg.response(label, since, until)
	resp.RefreshedAt = until.Format(consoleTSFormat)
	resp.Complete = !resp.Truncated
	if resp.Truncated {
		resp.Notes = append(resp.Notes, fmt.Sprintf(
			"This index has more than %d table/event-type groups in the window; the counts below are a floor, not the total.", activityMaxGroups))
	}
	return resp, nil
}

// liveWindow derives the Overview's window from what the live table actually
// holds: since = the oldest dated live partition's hour, until = now. The
// window IS the live retention (#1352) — if rotation keeps 12 hours live, the
// window is 12 hours — which is what makes the single-tier read honest by
// construction: a window equal to live coverage cannot contain archived-only
// hours, so no completeness caveat is needed and none is computed.
//
// When the floor cannot be determined — no dated partitions yet (a fresh index
// whose rows all sit in p_future) or a bundle with no database name — the
// window falls back to a bounded, stated activityFallbackWindow rather than an
// unbounded scan or a fabricated floor.
func liveWindow(ctx context.Context, db *sql.DB, dbName string, until time.Time) (time.Time, string, error) {
	if dbName == "" {
		return until.Add(-activityFallbackWindow), activityFallbackLabel, nil
	}
	rows, err := db.QueryContext(ctx, `
		SELECT PARTITION_NAME FROM information_schema.PARTITIONS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'binlog_events' AND PARTITION_NAME IS NOT NULL`, dbName)
	if err != nil {
		return time.Time{}, "", fmt.Errorf("list live partitions: %w", err)
	}
	defer rows.Close()
	var oldest time.Time
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return time.Time{}, "", err
		}
		t, ok := indexer.PartitionDate(name)
		if !ok {
			continue // p_future / malformed
		}
		if oldest.IsZero() || t.Before(oldest) {
			oldest = t
		}
	}
	if err := rows.Err(); err != nil {
		return time.Time{}, "", err
	}
	if oldest.IsZero() || !oldest.Before(until) {
		return until.Add(-activityFallbackWindow), activityFallbackLabel, nil
	}
	return oldest, "live retention · " + windowLabel(until.Sub(oldest)), nil
}

// windowLabel renders an approximate width for the tile ("~14 h", "~3 d"). The
// exact bounds travel in since/until; the label is the human-sized summary, and
// the tilde marks it as rounded so it never overclaims.
func windowLabel(d time.Duration) string {
	hrs := int(d.Round(time.Hour) / time.Hour)
	if hrs < 1 {
		hrs = 1
	}
	if hrs < 48 {
		return fmt.Sprintf("~%d h", hrs)
	}
	return fmt.Sprintf("~%d d", (hrs+12)/24)
}

// activityScopeKey canonicalizes the resolved deny+allow scope into a cache
// key: same tables in any order → same key, so a session's counts are shared
// per POLICY, not per request ordering. The two halves are keyed separately
// and joined with a distinct separator, so a deny of X can never share a
// materialization with an allow of X — and an allow-list session (whose deny
// half is typically empty) can never share the full-access entry, which is
// the cross-trust-boundary collision the old deny-only key allowed.
func activityScopeKey(deny, allow []query.SchemaTable) string {
	if len(deny) == 0 && len(allow) == 0 {
		return ""
	}
	return tableSetKey(deny) + "\x02" + tableSetKey(allow)
}

func tableSetKey(set []query.SchemaTable) string {
	if len(set) == 0 {
		return ""
	}
	keys := make([]string, len(set))
	for i, st := range set {
		keys[i] = st.Schema + "\x00" + st.Table
	}
	sort.Strings(keys)
	return strings.Join(keys, "\x01")
}

// activityCache is one server's materialized Overview aggregates, keyed by
// RBAC deny set. Lives on the bundle so it shares the connection's lifecycle:
// evicting a server (DSN edit/delete) drops its materialization with it.
type activityCache struct {
	mu      sync.Mutex
	entries map[string]activityResponse
	stamps  map[string]time.Time
	// costs is how long each entry's last compute took, prevCosts the one
	// before. The faster of the two (activityCache.costLocked) picks the
	// entry's TTL and whether a stale read waits for the refresh, so one slow
	// sample on a small index (a lock wait, a cold cache) cannot hold its
	// count for 30 minutes, and an index that really grew is caught on the
	// next compute.
	costs     map[string]time.Duration
	prevCosts map[string]time.Duration
	flights   map[string]*activityFlight
	// inlineWait is activityInlineWait; a field so tests can shorten it.
	inlineWait time.Duration
}

// activityFlight is one in-progress materialization. resp/err are written
// before done is closed; waiters read them only after <-done.
type activityFlight struct {
	done chan struct{}
	// started bounds the waiting: a request waits only for what is left of
	// inlineWait since the flight began, so a hung index costs the wait once
	// per flight rather than once per request.
	started time.Time
	resp    activityResponse
	err     error
}

func newActivityCache() *activityCache {
	return &activityCache{
		entries:    map[string]activityResponse{},
		stamps:     map[string]time.Time{},
		costs:      map[string]time.Duration{},
		prevCosts:  map[string]time.Duration{},
		flights:    map[string]*activityFlight{},
		inlineWait: activityInlineWait,
	}
}

// activityTTL is how long an aggregate that took cost to compute is reused:
// activityCostFactor times the cost, between activityMinTTL and
// activityRefreshTTL (#1778).
func activityTTL(cost time.Duration) time.Duration {
	return min(max(cost*activityCostFactor, activityMinTTL), activityRefreshTTL)
}

// costLocked is the cost that decides key's TTL: the faster of its last two
// computes. Caller holds c.mu.
func (c *activityCache) costLocked(key string) time.Duration {
	cost := c.costs[key]
	if prev, ok := c.prevCosts[key]; ok && prev < cost {
		return prev
	}
	return cost
}

// get implements the read path described on handleActivity:
//   - cached and within its TTL (activityTTL of its cost) → return it;
//   - cached and stale, and cheap (cost under activityInlineCost) → join or
//     start ONE recompute and wait for it, up to inlineWait from the moment
//     that recompute began; if it fails or takes longer, return the stale
//     entry, whose refreshed_at discloses the age;
//   - cached and stale, and expensive → return it AS IS and start ONE
//     background recompute for the next request;
//   - not cached → compute now, single-flight (concurrent misses wait for the
//     same flight rather than each scanning).
//
// The compute always runs on a background context with its own timeout: the
// flight outlives the request that started it on purpose, so a canceled tab
// neither aborts the refresh other waiters share nor leaves the cache cold.
func (c *activityCache) get(ctx context.Context, key string, compute func(context.Context) (activityResponse, error)) (activityResponse, error) {
	c.mu.Lock()
	if resp, ok := c.entries[key]; ok {
		cost := c.costLocked(key)
		if time.Since(c.stamps[key]) < activityTTL(cost) {
			c.mu.Unlock()
			return resp, nil
		}
		f := c.flights[key]
		if f == nil {
			f = &activityFlight{done: make(chan struct{}), started: time.Now()}
			c.flights[key] = f
			go c.run(key, f, compute, true)
		}
		wait := c.inlineWait - time.Since(f.started)
		c.mu.Unlock()
		if cost >= activityInlineCost {
			return resp, nil
		}
		timer := time.NewTimer(wait)
		defer timer.Stop()
		select {
		case <-f.done:
			if f.err == nil {
				return f.resp, nil
			}
		case <-timer.C:
		case <-ctx.Done():
		}
		return resp, nil
	}
	f := c.flights[key]
	if f == nil {
		f = &activityFlight{done: make(chan struct{}), started: time.Now()}
		c.flights[key] = f
		go c.run(key, f, compute, false)
	}
	c.mu.Unlock()
	select {
	case <-f.done:
		return f.resp, f.err
	case <-ctx.Done():
		return activityResponse{}, ctx.Err()
	}
}

// run executes one flight and publishes its result. A FAILED background
// refresh keeps the previous entry (stale-but-disclosed beats an error page on
// the landing tile) and logs; the flight is cleared either way, so the next
// request retries instead of waiting for a TTL that will never restamp.
func (c *activityCache) run(key string, f *activityFlight, compute func(context.Context) (activityResponse, error), background bool) {
	ctx, cancel := context.WithTimeout(context.Background(), activityComputeTimeout)
	defer cancel()
	start := time.Now()
	resp, err := compute(ctx)
	cost := time.Since(start)
	c.mu.Lock()
	if err == nil {
		c.entries[key] = resp
		c.stamps[key] = time.Now()
		if last, ok := c.costs[key]; ok {
			c.prevCosts[key] = last
		}
		c.costs[key] = cost
		c.evictOverCapLocked()
	}
	delete(c.flights, key)
	c.mu.Unlock()
	if err != nil && background {
		slog.Warn("console: activity refresh failed; the previous aggregate stays on screen with its refreshed_at", "error", err)
	}
	f.resp, f.err = resp, err
	close(f.done)
}

// evictOverCapLocked drops the oldest entries beyond activityMaxProfiles.
// Caller holds c.mu.
func (c *activityCache) evictOverCapLocked() {
	for len(c.entries) > activityMaxProfiles {
		var oldestKey string
		var oldestAt time.Time
		first := true
		for k, at := range c.stamps {
			if first || at.Before(oldestAt) {
				oldestKey, oldestAt, first = k, at, false
			}
		}
		delete(c.entries, oldestKey)
		delete(c.stamps, oldestKey)
		delete(c.costs, oldestKey)
		delete(c.prevCosts, oldestKey)
	}
}

// activityAgg folds the grouped rows. It is separate from the SQL so the fold
// (which owns "tables touched" and the insert/update/delete split) is testable
// without a database.
type activityAgg struct {
	total, inserts, updates, deletes, other int64
	byTable                                 map[string]*activityTable
	truncated                               bool
}

func newActivityAgg() *activityAgg {
	return &activityAgg{byTable: make(map[string]*activityTable)}
}

// add folds one (schema, table, event_type) group. Returns false once
// activityMaxGroups distinct TABLES have been seen and this group would add
// another, which tells the caller to stop scanning; the aggregate is then
// marked truncated. The cap is checked only on a table the map does not yet
// hold, so a wide index is never truncated over groups that cost no memory.
func (a *activityAgg) add(schema, table string, etype uint8, n int64) bool {
	k := schema + "." + table
	if _, seen := a.byTable[k]; !seen && len(a.byTable) >= activityMaxGroups {
		a.truncated = true
		return false
	}
	a.total += n
	switch event.EventType(etype) {
	case event.EventInsert:
		a.inserts += n
	case event.EventUpdate:
		a.updates += n
	case event.EventDelete:
		a.deletes += n
	default:
		a.other += n
	}
	t := a.byTable[k]
	if t == nil {
		t = &activityTable{Schema: schema, Table: table}
		a.byTable[k] = t
	}
	t.Total += n
	switch event.EventType(etype) {
	case event.EventInsert:
		t.Insert += n
	case event.EventUpdate:
		t.Update += n
	case event.EventDelete:
		t.Delete += n
	}
	return true
}

// response renders the aggregate. TopTables is sorted by total descending with
// the table key as the tiebreaker so the panel does not reshuffle between two
// loads of the same data.
func (a *activityAgg) response(label string, since, until time.Time) activityResponse {
	tables := make([]activityTable, 0, len(a.byTable))
	for _, t := range a.byTable {
		tables = append(tables, *t)
	}
	sort.Slice(tables, func(i, j int) bool {
		if tables[i].Total != tables[j].Total {
			return tables[i].Total > tables[j].Total
		}
		return tables[i].Schema+"."+tables[i].Table < tables[j].Schema+"."+tables[j].Table
	})
	if len(tables) > activityTopTables {
		tables = tables[:activityTopTables]
	}
	return activityResponse{
		Label:     label,
		Since:     since.Format(consoleTSFormat),
		Until:     until.Format(consoleTSFormat),
		Total:     a.total,
		Inserts:   a.inserts,
		Updates:   a.updates,
		Deletes:   a.deletes,
		Other:     a.other,
		Tables:    len(a.byTable),
		TopTables: tables,
		Truncated: a.truncated,
	}
}

// buildActivitySQL renders the aggregate query and its arguments.
//
// The time predicate is on the BARE event_timestamp column, both bounds, and
// nothing wraps it: binlog_events is PARTITION BY RANGE (TO_SECONDS(
// event_timestamp)), and MySQL only prunes partitions when the column appears
// unwrapped in the comparison. A DATE()/TO_SECONDS() wrapper here would still
// return the right answer while silently scanning every partition in the index.
// The upper bound is not optional either: a >=-only predicate would sweep in
// clock-skewed future rows sitting in p_future and count them under a label
// that says they are inside the window.
func buildActivitySQL(since, until time.Time, deny, allow []query.SchemaTable) (string, []any) {
	var sb strings.Builder
	sb.WriteString("SELECT schema_name, table_name, event_type, COUNT(*) AS n FROM binlog_events" +
		" WHERE event_timestamp >= ? AND event_timestamp < ?")
	args := []any{since, until}
	// Allow-list mode (#1449) mirrors buildQuery, BINARY included: a
	// case-insensitive allow would also count a distinct same-name-other-case
	// table (see buildQuery's allow clause for the full rationale).
	if len(allow) > 0 {
		ors := make([]string, len(allow))
		for i, at := range allow {
			ors[i] = "(BINARY schema_name = ? AND BINARY table_name = ?)"
			args = append(args, at.Schema, at.Table)
		}
		sb.WriteString(" AND (" + strings.Join(ors, " OR ") + ")")
	}
	for _, dt := range deny {
		sb.WriteString(" AND NOT (schema_name = ? AND table_name = ?)")
		args = append(args, dt.Schema, dt.Table)
	}
	sb.WriteString(" GROUP BY schema_name, table_name, event_type")
	return sb.String(), args
}

// collectActivity runs the aggregate and folds it.
func collectActivity(ctx context.Context, db *sql.DB, since, until time.Time, deny, allow []query.SchemaTable) (*activityAgg, error) {
	q, args := buildActivitySQL(since, until, deny, allow)
	rows, err := db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	agg := newActivityAgg()
	for rows.Next() {
		var schema, table string
		var etype uint8
		var n int64
		if err := rows.Scan(&schema, &table, &etype, &n); err != nil {
			return nil, err
		}
		if !agg.add(schema, table, etype, n) {
			break
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return agg, nil
}
