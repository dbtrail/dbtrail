package console

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dbtrail/dbtrail/internal/event"
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
// and `watch`. How one count reads the index (one at a time per index
// database under a MySQL named lock, partition by partition, keeping the
// counts of hours already past) is described in activity_scan.go.
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

	// activityColdWait is how long a request with NOTHING cached waits for the
	// first count before it is answered with a 503 that says the count is
	// still running. A request never holds the page for a whole scan: on a
	// large index the first count takes minutes, and waiting for it is how a
	// request used to come back as a bare 500 when the flight's own deadline
	// fired.
	activityColdWait = 10 * time.Second

	// activityRetryBackoff is how long after a FAILED count (an error, a
	// timeout, or the index's lock held elsewhere) the next one may start. A
	// page that asks every few seconds would otherwise turn one failing
	// statement into a scan per request.
	activityRetryBackoff = 30 * time.Second

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

	// steadyCost, when set, is what the next refresh is expected to cost
	// (activityAgg.steady); the cache takes the TTL from it instead of from
	// this count's own duration. Not on the wire.
	steadyCost time.Duration
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
		// Nothing cached and no count to give: say why (still running, or the
		// last one failed and when the next starts) instead of a bare 500.
		var unavail *activityUnavailableError
		if errors.As(err, &unavail) {
			retry := activityColdWait
			if !unavail.running {
				retry = max(time.Until(unavail.retryAt), time.Second)
			}
			secs := int(retry.Round(time.Second) / time.Second)
			w.Header().Set("Retry-After", strconv.Itoa(secs))
			// retry_after repeats the header in the body, where the page's
			// fetch helper (which keeps the parsed body on the error) reads it.
			writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": err.Error(), "retry_after": secs})
			return
		}
		writeFetchError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

// activityFor returns the materialized aggregate for this server under the
// given deny/allow scope, as activityCache.get describes.
func (b *bundle) activityFor(ctx context.Context, deny, allow []query.SchemaTable) (activityResponse, error) {
	key := activityScopeKey(deny, allow)
	sc := activityScan{dbName: b.dbName, deny: deny, allow: allow, storeBudget: -1, lockWait: activityLockWait}
	if b.activity == nil {
		// Test-constructed bundles only: every production bundle gets its cache
		// in newBundleDerived (pinned by TestBundleAlwaysCarriesActivityCache).
		return computeActivity(ctx, b.db, sc)
	}
	c := b.activity
	return c.get(ctx, key, func(cctx context.Context) (activityResponse, error) {
		// Runs inside activityCache.run, which holds scanMu: the partition
		// store is only ever touched there.
		sc.store, sc.storeBudget = c.partStoreLocked(key)
		return computeActivity(cctx, b.db, sc)
	})
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
// RBAC scope. Lives on the bundle so it shares the connection's lifecycle:
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
	// retryAt/lastErr remember a failed count: no new one starts for the key
	// before retryAt, and a request with nothing cached is told lastErr.
	retryAt map[string]time.Time
	lastErr map[string]error

	// scanMu serializes counts across scope keys: one count at a time per
	// server from this process (the index's named lock covers every other
	// process). It also guards parts, which only run touches.
	scanMu sync.Mutex
	parts  map[string]*activityPartStore

	// Fields so tests can shorten them.
	inlineWait   time.Duration // activityInlineWait
	coldWait     time.Duration // activityColdWait
	retryBackoff time.Duration // activityRetryBackoff
	runTimeout   time.Duration // activityRunTimeout
}

// activityFlight is one in-progress materialization. resp/err are written
// before done is closed; waiters read them only after <-done.
type activityFlight struct {
	done chan struct{}
	// started bounds the waiting: a request waits only for what is left of
	// inlineWait since the flight began, so a hung index costs the wait once
	// per flight rather than once per request.
	started time.Time
	// counting is when run got the scan lock (zero while it waits behind
	// another scope's count). Guarded by the cache's mu.
	counting time.Time
	resp     activityResponse
	err      error
}

func newActivityCache() *activityCache {
	return &activityCache{
		entries:      map[string]activityResponse{},
		stamps:       map[string]time.Time{},
		costs:        map[string]time.Duration{},
		prevCosts:    map[string]time.Duration{},
		flights:      map[string]*activityFlight{},
		retryAt:      map[string]time.Time{},
		lastErr:      map[string]error{},
		parts:        map[string]*activityPartStore{},
		inlineWait:   activityInlineWait,
		coldWait:     activityColdWait,
		retryBackoff: activityRetryBackoff,
		runTimeout:   activityRunTimeout,
	}
}

// partStoreLocked returns key's partition store and how many more groups it
// may keep under activityMaxStoredGroups. Caller holds scanMu.
func (c *activityCache) partStoreLocked(key string) (*activityPartStore, int) {
	st := c.parts[key]
	if st == nil {
		st = newActivityPartStore()
		c.parts[key] = st
	}
	used := 0
	for _, other := range c.parts {
		used += other.groups()
	}
	return st, max(activityMaxStoredGroups-used, 0)
}

// activityUnavailableError is the answer to a request with nothing cached
// that could not be given a count: the first one is still running, or the
// last one failed. The handler turns it into a 503 whose body says which.
type activityUnavailableError struct {
	// running: a count is pending; since is when it got the scan lock, zero
	// while it still waits behind another scope's count on this server.
	running bool
	since   time.Time
	// cause: the last count failed with it; retryAt is when the next starts.
	cause   error
	retryAt time.Time
}

func (e *activityUnavailableError) Error() string {
	if e.running && e.since.IsZero() {
		return "the activity counts for this server are waiting for another count on this server to finish; they appear here once theirs has run"
	}
	if e.running {
		return fmt.Sprintf("the activity counts for this server are still being computed (started %s ago); they appear here once the first count finishes",
			time.Since(e.since).Round(time.Second))
	}
	wait := max(time.Until(e.retryAt), 0).Round(time.Second)
	return fmt.Sprintf("the activity counts for this server could not be computed: %v; the next attempt starts in %s", e.cause, wait)
}

func (e *activityUnavailableError) Unwrap() error { return e.cause }

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

// startLocked returns key's running flight, starting one unless a failed
// count's backoff is still running (then nil). Caller holds c.mu.
func (c *activityCache) startLocked(key string, compute func(context.Context) (activityResponse, error), background bool) *activityFlight {
	if f := c.flights[key]; f != nil {
		return f
	}
	if time.Now().Before(c.retryAt[key]) {
		return nil
	}
	f := &activityFlight{done: make(chan struct{}), started: time.Now()}
	c.flights[key] = f
	go c.run(key, f, compute, background)
	return f
}

// get implements the read path described on handleActivity:
//   - cached and within its TTL (activityTTL of its cost) → return it;
//   - cached and stale, and cheap (cost under activityInlineCost) → join or
//     start ONE recompute and wait for it, up to inlineWait from the moment
//     that recompute began; if it fails or takes longer, return the stale
//     entry, whose refreshed_at discloses the age;
//   - cached and stale, and expensive → return it AS IS and start ONE
//     background recompute for the next request;
//   - not cached → join or start ONE compute and wait up to coldWait; past
//     that, or when the last count failed, an *activityUnavailableError.
//
// The compute always runs in its own goroutine on a background context: the
// request only ever WAITS for it, so a canceled tab neither aborts the refresh
// other waiters share nor leaves the cache cold. A failed count holds off the
// next one for retryBackoff.
func (c *activityCache) get(ctx context.Context, key string, compute func(context.Context) (activityResponse, error)) (activityResponse, error) {
	c.mu.Lock()
	if resp, ok := c.entries[key]; ok {
		cost := c.costLocked(key)
		if time.Since(c.stamps[key]) < activityTTL(cost) {
			c.mu.Unlock()
			return resp, nil
		}
		f := c.startLocked(key, compute, true)
		// A refresh that failed is said on the tiles, not only in the log:
		// otherwise a refresh failing for hours (a dead session holding the
		// lock, a statement that always times out) shows the old numbers as
		// if nothing were wrong.
		resp = withRefreshNote(resp, c.lastErr[key])
		c.mu.Unlock()
		if f == nil || cost >= activityInlineCost {
			return resp, nil
		}
		timer := time.NewTimer(c.inlineWait - time.Since(f.started))
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
	f := c.startLocked(key, compute, false)
	if f == nil {
		e := &activityUnavailableError{cause: c.lastErr[key], retryAt: c.retryAt[key]}
		c.mu.Unlock()
		return activityResponse{}, e
	}
	c.mu.Unlock()
	timer := time.NewTimer(c.coldWait)
	defer timer.Stop()
	select {
	case <-f.done:
		if f.err != nil {
			c.mu.Lock()
			retryAt := c.retryAt[key]
			c.mu.Unlock()
			return activityResponse{}, &activityUnavailableError{cause: f.err, retryAt: retryAt}
		}
		return f.resp, nil
	case <-timer.C:
		c.mu.Lock()
		counting := f.counting
		c.mu.Unlock()
		return activityResponse{}, &activityUnavailableError{running: true, since: counting}
	case <-ctx.Done():
		return activityResponse{}, ctx.Err()
	}
}

// run executes one flight and publishes its result. It holds scanMu for the
// whole count, so this server runs one count at a time across every scope. A
// FAILED refresh keeps the previous entry (stale-but-disclosed beats an error
// page on the landing tile), logs, and starts the retry backoff; the flight is
// cleared either way.
func (c *activityCache) run(key string, f *activityFlight, compute func(context.Context) (activityResponse, error), background bool) {
	c.scanMu.Lock()
	defer c.scanMu.Unlock()
	c.mu.Lock()
	f.counting = time.Now()
	c.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), c.runTimeout)
	defer cancel()
	start := time.Now()
	resp, err := safeActivityCompute(ctx, compute)
	cost := time.Since(start)
	if resp.steadyCost > 0 && resp.steadyCost < cost {
		cost = resp.steadyCost
	}
	c.mu.Lock()
	if err == nil {
		c.entries[key] = resp
		c.stamps[key] = time.Now()
		if last, ok := c.costs[key]; ok {
			c.prevCosts[key] = last
		}
		c.costs[key] = cost
		delete(c.retryAt, key)
		delete(c.lastErr, key)
		c.evictOverCapLocked()
	} else {
		c.retryAt[key] = time.Now().Add(c.retryBackoff)
		c.lastErr[key] = err
	}
	delete(c.flights, key)
	c.mu.Unlock()
	if err != nil {
		if background {
			slog.Warn("console: activity refresh failed; the previous aggregate stays on screen with its refreshed_at", "error", err)
		} else {
			slog.Warn("console: activity count failed; the Overview tiles show no number until one succeeds", "error", err)
		}
	}
	f.resp, f.err = resp, err
	close(f.done)
}

// safeActivityCompute turns a panic in a count into an error. Under `watch`
// the console and capture share one process, so a panic here would take
// capture down; and a flight whose done never closes would hang every request
// that waits on it.
func safeActivityCompute(ctx context.Context, compute func(context.Context) (activityResponse, error)) (resp activityResponse, err error) {
	defer func() {
		if p := recover(); p != nil {
			slog.Error("console: activity count panicked", "panic", p, "stack", string(debug.Stack()))
			resp, err = activityResponse{}, fmt.Errorf("activity count panicked: %v", p)
		}
	}()
	return compute(ctx)
}

// withRefreshNote returns resp with a note saying its last refresh failed,
// when err is set. resp is a cached value: its Notes slice is copied.
func withRefreshNote(resp activityResponse, err error) activityResponse {
	if err == nil {
		return resp
	}
	notes := append([]string(nil), resp.Notes...)
	resp.Notes = append(notes, fmt.Sprintf("These counts are from %s UTC: refreshing them failed (%v).", resp.RefreshedAt, err))
	return resp
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
		// Callers hold scanMu too (evictOverCapLocked runs inside run).
		delete(c.parts, oldestKey)
	}
}

// activityAgg folds the grouped rows. It is separate from the SQL so the fold
// (which owns "tables touched" and the insert/update/delete split) is testable
// without a database.
type activityAgg struct {
	total, inserts, updates, deletes, other int64
	byTable                                 map[string]*activityTable
	truncated                               bool
	// steady is what a refresh costs when nothing but the open hour changed
	// (set by the partitioned count; zero otherwise).
	steady time.Duration
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
	return buildActivitySQLFor("", 0, since, until, deny, allow)
}

// buildActivitySQLFor is buildActivitySQL restricted to one partition (when
// partition is set) and carrying a server-side MAX_EXECUTION_TIME (when
// timeout is positive): the server, not the client, ends a statement that runs
// too long, so no statement outlives the scan that issued it (see
// activity_scan.go). The time predicate stays even with the PARTITION clause:
// the first partition also holds rows older than the window, and the open
// partition rows newer than "now".
func buildActivitySQLFor(partition string, timeout time.Duration, since, until time.Time, deny, allow []query.SchemaTable) (string, []any) {
	var sb strings.Builder
	sb.WriteString("SELECT ")
	if ms := timeout.Milliseconds(); ms > 0 {
		fmt.Fprintf(&sb, "/*+ MAX_EXECUTION_TIME(%d) */ ", ms)
	}
	sb.WriteString("schema_name, table_name, event_type, COUNT(*) AS n FROM binlog_events")
	if partition != "" {
		sb.WriteString(" PARTITION (" + quotePartition(partition) + ")")
	}
	sb.WriteString(" WHERE event_timestamp >= ? AND event_timestamp < ?")
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
