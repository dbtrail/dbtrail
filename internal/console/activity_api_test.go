package console

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"

	"github.com/dbtrail/dbtrail/ext"
	"github.com/dbtrail/dbtrail/internal/query"
)

// aRow is one grouped row of the aggregate SELECT: (schema, table,
// event_type, count). event_type is the numeric TINYINT the column stores.
type aRow struct {
	schema, table string
	etype         uint8
	n             int64
}

// activityResult builds the grouped result set the aggregate SELECT returns.
func activityResult(rows ...aRow) *sqlmock.Rows {
	r := sqlmock.NewRows([]string{"schema_name", "table_name", "event_type", "n"})
	for _, row := range rows {
		r.AddRow(row.schema, row.table, row.etype, row.n)
	}
	return r
}

// partitionsResult builds the information_schema.PARTITIONS listing the
// live-window derivation reads.
func partitionsResult(names ...string) *sqlmock.Rows {
	r := sqlmock.NewRows([]string{"PARTITION_NAME"})
	for _, n := range names {
		r.AddRow(n)
	}
	return r
}

func getActivity(t *testing.T, srv *Server, path string) (int, activityResponse, string) {
	t.Helper()
	rec, body := doServersReq(t, srv, "GET", path, "")
	var got activityResponse
	if rec.Code == 200 {
		if err := json.Unmarshal(body, &got); err != nil {
			t.Fatalf("unmarshal %s: %v", body, err)
		}
	}
	return rec.Code, got, string(body)
}

// TestActivityAPICounts pins the point of the endpoint (#1300): the tiles'
// numbers describe a STATED window, are exact (not a fetch-window artefact),
// and carry the label the tile prints. It also pins that Tables is the distinct
// table count over the WHOLE window, not the length of the truncated top list,
// and — since #1352 — that the response stamps refreshed_at, the freshness the
// tile renders. The bundle carries no dbName here, so the window is the stated
// 24 h fallback (the live-retention path is pinned separately below).
func TestActivityAPICounts(t *testing.T) {
	db, mock, closeDB := newSQLMock(t)
	defer closeDB()
	mock.ExpectQuery(`COUNT\(\*\) AS n FROM binlog_events`).WillReturnRows(activityResult(
		aRow{"shop", "orders", 1, 10}, // INSERT
		aRow{"shop", "orders", 2, 5},  // UPDATE
		aRow{"shop", "orders", 3, 2},  // DELETE
		aRow{"shop", "users", 3, 7},   // DELETE
		aRow{"shop", "audit", 4, 3},   // DDL → Other
	))
	srv := newBootServer(db)

	code, got, body := getActivity(t, srv, "/api/activity")
	if code != 200 {
		t.Fatalf("code = %d, body = %s", code, body)
	}
	if got.Label != activityFallbackLabel {
		t.Errorf("label = %q, want %q (no dbName → stated fallback window)", got.Label, activityFallbackLabel)
	}
	if got.Total != 27 || got.Inserts != 10 || got.Updates != 5 || got.Deletes != 9 || got.Other != 3 {
		t.Errorf("counts = total %d ins %d upd %d del %d other %d; want 27/10/5/9/3",
			got.Total, got.Inserts, got.Updates, got.Deletes, got.Other)
	}
	if got.Tables != 3 {
		t.Errorf("tables = %d, want 3 distinct tables in the window", got.Tables)
	}
	if len(got.TopTables) != 3 || got.TopTables[0].Table != "orders" || got.TopTables[0].Total != 17 {
		t.Errorf("top_tables = %+v, want orders first with 17", got.TopTables)
	}
	if got.TopTables[0].Delete != 2 {
		t.Errorf("orders delete = %d, want 2", got.TopTables[0].Delete)
	}
	// The fallback window must be exactly as wide as stated, so the label is
	// not a claim about a different span than the counts.
	since, err := time.Parse(consoleTSFormat, got.Since)
	if err != nil {
		t.Fatal(err)
	}
	until, err := time.Parse(consoleTSFormat, got.Until)
	if err != nil {
		t.Fatal(err)
	}
	if d := until.Sub(since); d != activityFallbackWindow {
		t.Errorf("until-since = %s, want %s", d, activityFallbackWindow)
	}
	// Freshness is part of the wire contract (#1352): the aggregate is a
	// materialization, and the tile renders "as of <refreshed_at>".
	if got.RefreshedAt == "" {
		t.Error("refreshed_at is empty — a materialized aggregate must disclose when it was computed")
	}
	if got.RefreshedAt != got.Until {
		t.Errorf("refreshed_at = %q, until = %q — the window edge is the refresh time by construction", got.RefreshedAt, got.Until)
	}
	if !got.Complete {
		t.Errorf("complete = false with no truncation; notes = %v", got.Notes)
	}
}

// TestActivityWindowIsLiveRetention pins #1352 point 3: the window IS the live
// retention, derived from the oldest dated live partition — not a fixed picker
// period. If rotation keeps 3 hours live, the window is 3 hours, and the label
// says so.
func TestActivityWindowIsLiveRetention(t *testing.T) {
	db, mock, closeDB := newSQLMock(t)
	defer closeDB()
	now := time.Now().UTC()
	oldest := now.Add(-3 * time.Hour).Truncate(time.Hour)

	mock.ExpectQuery("PARTITION_NAME FROM information_schema.PARTITIONS").
		WillReturnRows(partitionsResult(
			now.Format("p_2006010215"),
			oldest.Format("p_2006010215"), // the floor, listed out of order on purpose
			now.Add(-time.Hour).Format("p_2006010215"),
			"p_future", // never a floor
		))
	mock.ExpectQuery(`COUNT\(\*\) AS n FROM binlog_events`).WillReturnRows(activityResult())

	srv := newBootServer(db)
	srv.cm.boot.dbName = "binlog_index"

	code, got, body := getActivity(t, srv, "/api/activity")
	if code != 200 {
		t.Fatalf("code = %d, body = %s", code, body)
	}
	if got.Since != oldest.Format(consoleTSFormat) {
		t.Errorf("since = %q, want the oldest live partition hour %q", got.Since, oldest.Format(consoleTSFormat))
	}
	if !strings.HasPrefix(got.Label, "live retention · ~") {
		t.Errorf("label = %q, want a live-retention label naming the measured window", got.Label)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("window derivation did not run as expected: %v", err)
	}
}

// TestActivityReadsLiveTierOnly pins #1352 point 2 by construction: the
// ordered mock expects EXACTLY the partition listing and the live aggregate —
// nothing else. Reintroducing the pre-#1352 archive-completeness pass (the
// query.Plan / archive_state read) issues an unexpected query, which errors the
// aggregate and fails the 200 assertion; and with the whole archive tier gone
// the response is still complete with no notes, because the window equals live
// coverage and can contain no archived-only hours.
func TestActivityReadsLiveTierOnly(t *testing.T) {
	db, mock, closeDB := newSQLMock(t)
	defer closeDB()
	now := time.Now().UTC()

	mock.ExpectQuery("PARTITION_NAME FROM information_schema.PARTITIONS").
		WillReturnRows(partitionsResult(now.Add(-2 * time.Hour).Format("p_2006010215")))
	mock.ExpectQuery(`COUNT\(\*\) AS n FROM binlog_events`).
		WillReturnRows(activityResult(aRow{"shop", "orders", 3, 4}))

	srv := newBootServer(db)
	srv.cm.boot.dbName = "binlog_index"

	code, got, body := getActivity(t, srv, "/api/activity")
	if code != 200 {
		t.Fatalf("code = %d, body = %s", code, body)
	}
	if !got.Complete || len(got.Notes) != 0 {
		t.Errorf("complete = %v notes = %v; the single-tier window has nothing to caveat", got.Complete, got.Notes)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unexpected query traffic (an archive-tier read?): %v", err)
	}
}

// TestActivityIsMaterializedAndDisclosesFreshness pins #1352 point 1: the
// aggregate is precomputed. The mock carries expectations for exactly ONE
// compute; a second request must be a cache read — same numbers, same
// refreshed_at (the visible freshness) — and issue no query at all. If the
// per-request scan came back, the second request would hit sqlmock with an
// unexpected query and fail loudly.
func TestActivityIsMaterializedAndDisclosesFreshness(t *testing.T) {
	db, mock, closeDB := newSQLMock(t)
	defer closeDB()
	mock.ExpectQuery(`COUNT\(\*\) AS n FROM binlog_events`).
		WillReturnRows(activityResult(aRow{"shop", "orders", 3, 4}))
	srv := newBootServer(db)

	code, first, body := getActivity(t, srv, "/api/activity")
	if code != 200 {
		t.Fatalf("first code = %d, body = %s", code, body)
	}
	code, second, body := getActivity(t, srv, "/api/activity")
	if code != 200 {
		t.Fatalf("second code = %d, body = %s", code, body)
	}
	if second.RefreshedAt != first.RefreshedAt || second.Deletes != first.Deletes {
		t.Errorf("second response (%q, %d deletes) is not the cached first (%q, %d deletes)",
			second.RefreshedAt, second.Deletes, first.RefreshedAt, first.Deletes)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("the second request re-scanned instead of reading the cache: %v", err)
	}
}

// TestActivityStaleCacheServesOldAndRefreshes pins the refresh mechanism for
// an EXPENSIVE aggregate, the large index #1352 was about: a request against a
// stale cache returns the OLD aggregate immediately — with its ORIGINAL
// refreshed_at, which is what makes the staleness visible — and starts one
// background recompute, whose result the next request serves. A cheap one is
// recomputed while the request waits instead (#1778, pinned in
// activity_cost_ttl_1778_test.go).
func TestActivityStaleCacheServesOldAndRefreshes(t *testing.T) {
	db, mock, closeDB := newSQLMock(t)
	defer closeDB()
	mock.ExpectQuery(`COUNT\(\*\) AS n FROM binlog_events`).
		WillReturnRows(activityResult(aRow{"shop", "orders", 3, 4}))
	// The background refresh's compute: different counts, so serving it is
	// observable.
	mock.ExpectQuery(`COUNT\(\*\) AS n FROM binlog_events`).
		WillReturnRows(activityResult(aRow{"shop", "orders", 3, 9}))
	srv := newBootServer(db)

	if code, first, body := getActivity(t, srv, "/api/activity"); code != 200 {
		t.Fatalf("first code = %d, body = %s", code, body)
	} else if first.Deletes != 4 {
		t.Fatalf("first deletes = %d, want 4", first.Deletes)
	}

	// Age the entry past the TTL, and make it an aggregate that took long to
	// compute: sqlmock answers in microseconds, which would make it a cheap one.
	c := srv.cm.boot.activity
	c.mu.Lock()
	for k := range c.stamps {
		c.stamps[k] = c.stamps[k].Add(-activityRefreshTTL - time.Minute)
		c.costs[k] = 40 * time.Second
	}
	c.mu.Unlock()

	// The stale read serves the previous aggregate as-is: old count, old
	// refreshed_at — stale-but-disclosed, never blocked on the recompute.
	code, stale, body := getActivity(t, srv, "/api/activity")
	if code != 200 {
		t.Fatalf("stale code = %d, body = %s", code, body)
	}
	if stale.Deletes != 4 {
		t.Errorf("stale read deletes = %d, want the previous aggregate's 4", stale.Deletes)
	}

	// The background flight it started lands the second compute; poll for it.
	deadline := time.Now().Add(5 * time.Second)
	for {
		if mock.ExpectationsWereMet() == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("background refresh never ran")
		}
		time.Sleep(10 * time.Millisecond)
	}
	// The refresh publishes under the cache lock after the query completes;
	// poll the served value, not the mock, for the flip.
	deadline = time.Now().Add(5 * time.Second)
	for {
		code, refreshed, body := getActivity(t, srv, "/api/activity")
		if code != 200 {
			t.Fatalf("refreshed code = %d, body = %s", code, body)
		}
		if refreshed.Deletes == 9 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("refreshed deletes = %d, want the recomputed 9", refreshed.Deletes)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestActivityDenyKey pins the cache key: order-insensitive over the same deny
// set (one profile, one materialization) and distinct across different sets
// (two profiles must never share counts — a table NAME is exactly what a deny
// profile withholds).
func TestActivityDenyKey(t *testing.T) {
	a := []query.SchemaTable{{Schema: "shop", Table: "secrets"}, {Schema: "hr", Table: "salaries"}}
	b := []query.SchemaTable{{Schema: "hr", Table: "salaries"}, {Schema: "shop", Table: "secrets"}}
	if activityScopeKey(a, nil) != activityScopeKey(b, nil) {
		t.Error("same deny set in different order produced different cache keys")
	}
	if activityScopeKey(a, nil) == activityScopeKey(a[:1], nil) {
		t.Error("different deny sets share a cache key — a redaction bypass via the cache")
	}
	if activityScopeKey(nil, nil) != "" || activityScopeKey(nil, nil) == activityScopeKey(a, nil) {
		t.Error("the empty scope must key separately from every profile")
	}
	// The #1449 dimensions: an allow of X must never share with a deny of X
	// (opposite meanings), and an allow-list session must never share the
	// full-access entry — the cross-trust-boundary collision the deny-only
	// key allowed.
	if activityScopeKey(a, nil) == activityScopeKey(nil, a) {
		t.Error("deny(X) and allow(X) share a cache key — opposite scopes, one materialization")
	}
	if activityScopeKey(nil, a) == activityScopeKey(nil, nil) {
		t.Error("an allow-list scope shares the full-access cache entry")
	}
	if activityScopeKey(nil, a) != activityScopeKey(nil, b) {
		t.Error("same allow set in different order produced different cache keys")
	}
}

// TestBuildActivitySQL_allowList pins allow-list mode on the aggregate: the
// clause mirrors buildQuery's, and deny still composes over it.
func TestBuildActivitySQL_allowList(t *testing.T) {
	since := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	allow := []query.SchemaTable{{Schema: "app", Table: "users"}, {Schema: "app", Table: "orders"}}
	deny := []query.SchemaTable{{Schema: "app", Table: "orders"}}
	q, args := buildActivitySQL(since, since.Add(time.Hour), deny, allow)
	if !strings.Contains(q, "AND ((BINARY schema_name = ? AND BINARY table_name = ?) OR (BINARY schema_name = ? AND BINARY table_name = ?))") {
		t.Errorf("exact-match allow-list clause missing: %s", q)
	}
	if !strings.Contains(q, "AND NOT (schema_name = ? AND table_name = ?)") {
		t.Errorf("deny clause must still compose over the allow list: %s", q)
	}
	if len(args) != 2+4+2 {
		t.Errorf("args = %d, want window(2) + allow(4) + deny(2): %v", len(args), args)
	}
}

// TestBundleAlwaysCarriesActivityCache pins the wiring that keeps the cached
// path from silently degrading to per-request scans: every production bundle
// (newBundleDerived is the single production constructor) carries a cache, and
// a derived-only rebuild keeps the SAME cache warm.
func TestBundleAlwaysCarriesActivityCache(t *testing.T) {
	b := newBundleDerived(nil, "db", ServerEntry{ID: "x"}, false)
	if b.activity == nil {
		t.Fatal("newBundleDerived built a bundle with no activity cache — /api/activity would re-scan per request")
	}
	cm := newConnManager(nil, false)
	cm.bundles["x"] = b
	cm.rebuildDerived(ServerEntry{ID: "x"})
	if cm.bundles["x"].activity != b.activity {
		t.Error("rebuildDerived dropped the warm activity cache")
	}
}

// TestActivitySQLPrunesPartitions pins the two properties the aggregate's cost
// and correctness both rest on: the time predicate is BOUNDED ON BOTH SIDES and
// compares the BARE event_timestamp column. Wrapping it (DATE(), TO_SECONDS())
// still returns right answers while scanning every partition in the index, and
// dropping the upper bound sweeps clock-skewed p_future rows into a window that
// says it excludes them.
func TestActivitySQLPrunesPartitions(t *testing.T) {
	since := time.Date(2026, 8, 9, 10, 0, 0, 0, time.UTC)
	until := since.Add(24 * time.Hour)
	q, args := buildActivitySQL(since, until, nil, nil)

	if !strings.Contains(q, "event_timestamp >= ?") {
		t.Errorf("missing bare-column lower bound in %q", q)
	}
	if !strings.Contains(q, "event_timestamp < ?") {
		t.Errorf("missing bare-column upper bound in %q", q)
	}
	for _, wrapper := range []string{"TO_SECONDS(event_timestamp", "DATE(event_timestamp", "CAST(event_timestamp"} {
		if strings.Contains(q, wrapper) {
			t.Errorf("time predicate is wrapped in %s — partition pruning is lost: %q", wrapper, q)
		}
	}
	if !strings.Contains(q, "GROUP BY schema_name, table_name, event_type") {
		t.Errorf("missing grouping in %q", q)
	}
	if len(args) != 2 || args[0] != any(since) || args[1] != any(until) {
		t.Errorf("args = %v, want [since until]", args)
	}
}

// TestActivitySQLExcludesDeniedTables pins RBAC: a denied table contributes to
// neither the counts nor the table list. A table NAME is exactly what a deny
// profile withholds elsewhere, so leaking it through an aggregate would be a
// redaction bypass with extra steps.
func TestActivitySQLExcludesDeniedTables(t *testing.T) {
	since := time.Date(2026, 8, 9, 10, 0, 0, 0, time.UTC)
	deny := []query.SchemaTable{{Schema: "shop", Table: "secrets"}, {Schema: "hr", Table: "salaries"}}
	q, args := buildActivitySQL(since, since.Add(time.Hour), deny, nil)

	if n := strings.Count(q, "NOT (schema_name = ? AND table_name = ?)"); n != 2 {
		t.Errorf("deny clauses = %d, want 2 in %q", n, q)
	}
	want := []any{since, since.Add(time.Hour), "shop", "secrets", "hr", "salaries"}
	if len(args) != len(want) {
		t.Fatalf("args = %v, want %v", args, want)
	}
	for i := range want {
		if args[i] != want[i] {
			t.Errorf("args[%d] = %v, want %v", i, args[i], want[i])
		}
	}
}

// TestActivityDenyTablesReachTheQuery pins the WIRING, not just the builder:
// the startup profile's deny list must arrive in the SQL the handler runs.
// Deleting the DenyTables plumbing in handleActivity leaves the builder test
// above green and only fails here.
func TestActivityDenyTablesReachTheQuery(t *testing.T) {
	db, mock, closeDB := newSQLMock(t)
	defer closeDB()
	mock.ExpectQuery(`NOT \(schema_name = \? AND table_name = \?\)`).
		WillReturnRows(activityResult())
	srv := newBootServer(db)
	srv.denyTables = []query.SchemaTable{{Schema: "shop", Table: "secrets"}}
	srv.profileActive = true

	if code, _, body := getActivity(t, srv, "/api/activity"); code != 200 {
		t.Fatalf("code = %d, body = %s", code, body)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("the deny clause never reached the query: %v", err)
	}
}

// TestActivityRequiresQueryPermission pins the TIER, not merely that the route
// is classified (which the completeness tests already do). /api/activity
// aggregates the indexed row data and reports which tables changed and how
// often; the fact that its handler must apply the profile's DenyTables to be
// safe is what disqualifies it from the status:read floor. Without this, a
// future edit can slide it down a tier and nothing notices.
func TestActivityRequiresQueryPermission(t *testing.T) {
	perm, classified := permForRoute("GET", "/api/activity")
	if !classified {
		t.Fatal("/api/activity is not classified in apiRoutePerms")
	}
	if perm != ext.PermQueryExecute {
		t.Errorf("permission = %q, want %q — activity reads indexed row data, it is not a health read",
			perm, ext.PermQueryExecute)
	}
}

// TestActivityAggFold pins the fold: the type split, the Other bucket for
// non-DML event types, distinct-table counting independent of the rendered
// list, and a deterministic order for the panel.
func TestActivityAggFold(t *testing.T) {
	a := newActivityAgg()
	// Two tables with an identical total: the tie must break on the name, or
	// the panel reshuffles between two loads of the same data.
	a.add("db", "bbb", 1, 4)
	a.add("db", "aaa", 1, 4)
	a.add("db", "big", 2, 9)
	a.add("db", "big", 5, 1) // EventGTID → Other, but still this table's activity
	resp := a.response("last 1 h", time.Now(), time.Now())

	if resp.Total != 18 || resp.Inserts != 8 || resp.Updates != 9 || resp.Deletes != 0 || resp.Other != 1 {
		t.Errorf("fold = total %d ins %d upd %d del %d other %d", resp.Total, resp.Inserts, resp.Updates, resp.Deletes, resp.Other)
	}
	if resp.Tables != 3 {
		t.Errorf("tables = %d, want 3", resp.Tables)
	}
	got := []string{resp.TopTables[0].Table, resp.TopTables[1].Table, resp.TopTables[2].Table}
	want := []string{"big", "aaa", "bbb"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("top_tables order = %v, want %v", got, want)
		}
	}
	// Other-typed events count toward the table's total but into no DML column.
	if resp.TopTables[0].Total != 10 || resp.TopTables[0].Update != 9 {
		t.Errorf("big = %+v, want total 10 / update 9", resp.TopTables[0])
	}
}

// TestActivityAggCapsRenderedTables pins that only the RENDERED list is capped:
// the distinct-table tile must keep reporting the real number, or "tables
// touched" silently becomes "tables shown".
func TestActivityAggCapsRenderedTables(t *testing.T) {
	a := newActivityAgg()
	for i := 0; i < activityTopTables+5; i++ {
		a.add("db", string(rune('a'+i)), 1, int64(i+1))
	}
	resp := a.response("last 1 h", time.Now(), time.Now())
	if len(resp.TopTables) != activityTopTables {
		t.Errorf("top_tables = %d, want %d", len(resp.TopTables), activityTopTables)
	}
	if resp.Tables != activityTopTables+5 {
		t.Errorf("tables = %d, want %d", resp.Tables, activityTopTables+5)
	}
}

// TestWindowLabel pins the human-sized width summary: rounded, tilde-marked,
// hours under two days and days beyond.
func TestWindowLabel(t *testing.T) {
	for _, tc := range []struct {
		d    time.Duration
		want string
	}{
		{30 * time.Minute, "~1 h"},
		{90 * time.Minute, "~2 h"},
		{14 * time.Hour, "~14 h"},
		{47 * time.Hour, "~47 h"},
		{49 * time.Hour, "~2 d"},
		{6 * 24 * time.Hour, "~6 d"},
	} {
		if got := windowLabel(tc.d); got != tc.want {
			t.Errorf("windowLabel(%s) = %q, want %q", tc.d, got, tc.want)
		}
	}
}

// TestActivityUnopenedIndexIsNotZero: a server whose index connection never
// opened must fail loudly. "0 deletes" is an assurance, and nobody measured it.
func TestActivityUnopenedIndexIsNotZero(t *testing.T) {
	srv := &Server{token: "t", cm: newConnManager(nil, false)}
	srv.cm.boot = &bundle{db: nil, noArchive: true}
	srv.mux = srv.buildHandler()

	code, _, body := getActivity(t, srv, "/api/activity")
	if code == 200 {
		t.Fatalf("code = 200 with no index connection; body = %s", body)
	}
}
