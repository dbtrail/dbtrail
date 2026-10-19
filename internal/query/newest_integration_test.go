//go:build integration

package query

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// The newest-first partition walk on real partitions. Every answer is checked
// against the single statement over the whole table (fetchOnce), the
// behaviour the walk replaces, so "same page" is measured, not assumed.

// walkFixture is an index whose binlog_events has one partition per hour in
// hours plus p_future.
func walkFixture(t *testing.T, hours []time.Time) (*sql.DB, *Engine) {
	t.Helper()
	testutil.SkipIfNoMySQL(t)
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if len(hours) > 0 {
		testutil.SetupPartitionedTable(t, db, dbName, hours)
	}
	return db, New(db)
}

func hoursFrom(start time.Time, n int) []time.Time {
	out := make([]time.Time, n)
	for i := range out {
		out[i] = start.Add(time.Duration(i) * time.Hour)
	}
	return out
}

func putEvent(t *testing.T, db *sql.DB, at time.Time, table string, et event.EventType) {
	t.Helper()
	testutil.InsertEvent(t, db, "bin.000001", 4, 40, at.UTC().Format("2006-01-02 15:04:05"), nil,
		"app", table, uint8(et), "1", nil, nil, []byte(`{"id":1}`))
}

// countSteps records the partitions the walk reads for the duration of a test.
func countSteps(t *testing.T) *[]string {
	t.Helper()
	var steps []string
	newestWalkStep = func(p string) { steps = append(steps, p) }
	t.Cleanup(func() { newestWalkStep = nil })
	return &steps
}

func eventIDs(rows []ResultRow) []uint64 {
	out := make([]uint64, len(rows))
	for i, r := range rows {
		out[i] = r.EventID
	}
	return out
}

// sameAsSingleStatement runs opts through Fetch and through the single
// statement and requires the same events in the same order.
func sameAsSingleStatement(t *testing.T, e *Engine, opts Options) []ResultRow {
	t.Helper()
	ctx := context.Background()
	got, err := e.Fetch(ctx, opts)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	want, err := e.fetchOnce(ctx, opts)
	if err != nil {
		t.Fatalf("single statement: %v", err)
	}
	sortResults(want, OrderDirection(opts.Order))
	if !slices.Equal(eventIDs(got), eventIDs(want)) {
		t.Fatalf("walk %v, single statement %v", eventIDs(got), eventIDs(want))
	}
	return got
}

func TestNewestWalkIntegration_emptyIndex(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Hour)
	_, e := walkFixture(t, hoursFrom(now.Add(-2*time.Hour), 5))
	steps := countSteps(t)
	if got := sameAsSingleStatement(t, e, Options{Limit: 8, Order: "DESC"}); len(got) != 0 {
		t.Fatalf("got %d rows from an empty index", len(got))
	}
	if len(*steps) != 6 {
		t.Fatalf("an empty index is walked to the end: read %v", *steps)
	}
}

// Only p_future exists (InitIndexTables' layout before any rotation).
func TestNewestWalkIntegration_onlyPFuture(t *testing.T) {
	db, e := walkFixture(t, nil)
	now := time.Now().UTC()
	for i := range 5 {
		putEvent(t, db, now.Add(time.Duration(i)*time.Second), "t", event.EventInsert)
	}
	steps := countSteps(t)
	if got := sameAsSingleStatement(t, e, Options{Limit: 3, Order: "DESC"}); len(got) != 3 {
		t.Fatalf("got %d rows", len(got))
	}
	if !slices.Equal(*steps, []string{"p_future"}) {
		t.Fatalf("read %v", *steps)
	}
}

// Rows past the newest hourly partition land in p_future, which sorts last
// in partition order and is read first.
func TestNewestWalkIntegration_rowsInPFutureAndOlder(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Hour)
	db, e := walkFixture(t, hoursFrom(now.Add(-3*time.Hour), 2))
	putEvent(t, db, now.Add(-3*time.Hour+time.Minute), "t", event.EventInsert)
	putEvent(t, db, now.Add(5*time.Minute), "t", event.EventInsert) // p_future
	steps := countSteps(t)
	if got := sameAsSingleStatement(t, e, Options{Limit: 1, Order: "DESC"}); len(got) != 1 {
		t.Fatalf("got %d rows", len(got))
	}
	if !slices.Equal(*steps, []string{"p_future"}) {
		t.Fatalf("a page filled from p_future reads nothing else: read %v", *steps)
	}
}

// The layout init leaves: 48 empty future hours (and p_future) ahead of the
// data. The walk passes them one cheap query each and stops in the first
// hour that fills the page; the hours behind it are never read.
func TestNewestWalkIntegration_emptyFutureHorizonBeforeTheData(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Hour)
	db, e := walkFixture(t, hoursFrom(now.Add(-3*time.Hour), 3+1+48))
	for h := -3; h <= 0; h++ {
		for i := range 10 {
			putEvent(t, db, now.Add(time.Duration(h)*time.Hour+time.Duration(i)*time.Minute), "t", event.EventInsert)
		}
	}
	steps := countSteps(t)
	sameAsSingleStatement(t, e, Options{Limit: 8, Order: "DESC"})
	if len(*steps) != 1+48+1 {
		t.Fatalf("want p_future, 48 empty hours and the current hour, read %d: %v", len(*steps), *steps)
	}
	if last := (*steps)[len(*steps)-1]; last != now.Format("p_2006010215") {
		t.Fatalf("stopped in %s, want the current hour", last)
	}
}

func TestNewestWalkIntegration_fewerRowsThanTheLimit(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Hour)
	db, e := walkFixture(t, hoursFrom(now.Add(-3*time.Hour), 6))
	putEvent(t, db, now.Add(-3*time.Hour), "t", event.EventInsert)
	putEvent(t, db, now.Add(-time.Hour), "t", event.EventInsert)
	if got := sameAsSingleStatement(t, e, Options{Limit: 50, Order: "DESC"}); len(got) != 2 {
		t.Fatalf("got %d rows, want both", len(got))
	}
}

// Exactly the limit, split across two partitions: the second is asked only
// for what the first left missing, and nothing older is read.
func TestNewestWalkIntegration_exactlyTheLimitAcrossTwoPartitions(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Hour)
	db, e := walkFixture(t, hoursFrom(now.Add(-3*time.Hour), 4))
	putEvent(t, db, now.Add(-3*time.Hour), "t", event.EventInsert) // must not be reached
	for i := range 3 {
		putEvent(t, db, now.Add(-2*time.Hour+time.Duration(i)*time.Minute), "t", event.EventInsert)
	}
	for i := range 2 {
		putEvent(t, db, now.Add(-time.Hour+time.Duration(i)*time.Minute), "t", event.EventInsert)
	}
	steps := countSteps(t)
	if got := sameAsSingleStatement(t, e, Options{Limit: 5, Order: "DESC"}); len(got) != 5 {
		t.Fatalf("got %d rows", len(got))
	}
	want := []string{"p_future", now.Format("p_2006010215"), now.Add(-time.Hour).Format("p_2006010215"), now.Add(-2 * time.Hour).Format("p_2006010215")}
	if !slices.Equal(*steps, want) {
		t.Fatalf("read %v, want %v", *steps, want)
	}
}

// Many events in one second: the limit cuts inside the second and event_id
// decides which of them are kept, exactly as in the single statement.
func TestNewestWalkIntegration_tiesOnTheSecondOrderByEventID(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Hour)
	db, e := walkFixture(t, hoursFrom(now.Add(-2*time.Hour), 3))
	sec := now.Add(-time.Hour + 30*time.Minute)
	for range 7 {
		putEvent(t, db, sec, "t", event.EventInsert)
	}
	putEvent(t, db, sec.Add(-time.Second), "t", event.EventInsert)
	got := sameAsSingleStatement(t, e, Options{Limit: 4, Order: "DESC"})
	for i := 1; i < len(got); i++ {
		if !got[i].EventTimestamp.Equal(sec) || got[i].EventID >= got[i-1].EventID {
			t.Fatalf("want the four highest ids of the shared second, got %v", eventIDs(got))
		}
	}
}

// Every filter the walk accepts keeps its meaning: same page as the single
// statement, on a fixture where each filter changes the answer.
func TestNewestWalkIntegration_filtersKeepTheirMeaning(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Hour)
	db, e := walkFixture(t, hoursFrom(now.Add(-4*time.Hour), 6))
	for h := -4; h <= 1; h++ { // h = 1 is p_future
		for i := range 6 {
			tbl := []string{"orders", "users", "items"}[i%3]
			et := []event.EventType{event.EventInsert, event.EventUpdate, event.EventDelete}[(i+h+4)%3]
			putEvent(t, db, now.Add(time.Duration(h)*time.Hour+time.Duration(i)*time.Minute), tbl, et)
		}
	}
	// One statement digest on the users rows of two hours, so the hash filter
	// matches some rows and not others.
	hash := strings.Repeat("a", 64)
	testutil.MustExec(t, db, "UPDATE binlog_events SET query_hash = ? WHERE table_name = 'users' AND event_timestamp >= ?",
		hash, now.Add(-2*time.Hour))
	del := event.EventDelete
	since := now.Add(-3*time.Hour + 2*time.Minute)
	until := now.Add(-time.Hour + 3*time.Minute)
	recent, err := e.fetchOnce(context.Background(), Options{Limit: 40, Order: "DESC"})
	if err != nil {
		t.Fatal(err)
	}
	sortResults(recent, "DESC")
	cursor := &EventCursor{Timestamp: recent[10].EventTimestamp, EventID: recent[10].EventID}
	cases := map[string]Options{
		"schema only":    {Schema: "app"},
		"event type":     {EventType: &del},
		"window":         {Since: &since, Until: &until},
		"page two":       {BeforeEvent: cursor},
		"deny":           {DenyTables: []SchemaTable{{"app", "orders"}}},
		"allow":          {AllowTables: []SchemaTable{{"app", "items"}}},
		"no match":       {ColumnEq: []ColumnEq{{Column: "id", Value: "no such value"}}},
		"since event":    {SinceEventID: recent[20].EventID},
		"profile named":  {ProfileActive: true, RedactColumns: []SchemaTableColumn{{"app", "users", "id"}}},
		"column value":   {ColumnEq: []ColumnEq{{Column: "id", Value: "1"}}},
		"statement hash": {QueryHash: hash},
		"flag":           {Flag: "pii"},
	}
	testutil.MustExec(t, db, "INSERT INTO table_flags (schema_name, table_name, column_name, flag) VALUES ('app', 'items', '', 'pii')")
	steps := countSteps(t)
	t.Run("schema and table keep the single statement", func(t *testing.T) {
		*steps = nil
		sameAsSingleStatement(t, e, Options{Limit: 7, Order: "DESC", Schema: "app", Table: "users"})
		if len(*steps) != 0 {
			t.Fatalf("walked %v", *steps)
		}
	})
	t.Run("no table of that name keeps the single statement", func(t *testing.T) {
		o := Options{Limit: 7, Order: "DESC", Table: "nothing"}
		sameAsSingleStatement(t, e, o)
		if _, walked, err := fetchNewestByPartition(context.Background(), db, o); err != nil || walked {
			t.Fatalf("walked %v (%v): no partition lists the table, so the free single statement answers", walked, err)
		}
	})
	for name, o := range cases {
		t.Run(name, func(t *testing.T) {
			o.Limit, o.Order = 7, "DESC"
			got := sameAsSingleStatement(t, e, o)
			if (name == "no match") != (len(got) == 0) {
				t.Fatalf("%d rows: the fixture must make this filter match some rows (none for no match)", len(got))
			}
			// The walk itself answered, not its fallback.
			if _, walked, err := fetchNewestByPartition(context.Background(), db, o); err != nil || !walked {
				t.Fatalf("walked %v (%v)", walked, err)
			}
		})
	}
}

// A partition dropped by rotation after the walk listed it: the read answers
// 1735, the walk lists again and serves the table as it now is.
func TestNewestWalkIntegration_partitionDroppedMidWalk(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Hour)
	hours := hoursFrom(now.Add(-3*time.Hour), 4)
	db, e := walkFixture(t, hours)
	for _, h := range hours {
		putEvent(t, db, h.Add(time.Minute), "t", event.EventInsert)
	}
	oldest := hours[0].Format("p_2006010215")
	var steps []string
	dropped := false
	newestWalkStep = func(p string) {
		steps = append(steps, p)
		if !dropped {
			dropped = true
			testutil.MustExec(t, db, "ALTER TABLE binlog_events DROP PARTITION "+oldest)
		}
	}
	t.Cleanup(func() { newestWalkStep = nil })
	got := sameAsSingleStatement(t, e, Options{Limit: 10, Order: "DESC"})
	if len(got) != 3 {
		t.Fatalf("got %d rows, want the 3 that survive the drop", len(got))
	}
	if n := slices.Index(steps, oldest); n < 0 || slices.Index(steps[n+1:], "p_future") < 0 {
		t.Fatalf("want the dropped partition read, then a second walk: %v", steps)
	}
}

// Rotation reorganises p_future into a new hour while the walk runs: its rows
// move to a partition the walk never listed. The walk sees the changed list
// and walks again instead of serving a page without the newest events.
func TestNewestWalkIntegration_pFutureReorganisedMidWalk(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Hour)
	hours := hoursFrom(now.Add(-2*time.Hour), 2) // p_future holds the current hour
	db, e := walkFixture(t, hours)
	putEvent(t, db, now.Add(-2*time.Hour+time.Minute), "t", event.EventInsert)
	putEvent(t, db, now.Add(10*time.Minute), "t", event.EventInsert)
	newest := now.Add(10 * time.Minute)
	reorganised := false
	newestWalkStep = func(string) {
		if !reorganised {
			reorganised = true
			testutil.MustExec(t, db, fmt.Sprintf(
				"ALTER TABLE binlog_events REORGANIZE PARTITION p_future INTO (PARTITION %s VALUES LESS THAN (TO_SECONDS('%s')), PARTITION p_future VALUES LESS THAN MAXVALUE)",
				now.Format("p_2006010215"), now.Add(time.Hour).Format("2006-01-02 15:04:05")))
		}
	}
	t.Cleanup(func() { newestWalkStep = nil })
	got := sameAsSingleStatement(t, e, Options{Limit: 1, Order: "DESC"})
	if len(got) != 1 || !got[0].EventTimestamp.Equal(newest) {
		t.Fatalf("want the newest event (moved by the reorganise), got %v", got)
	}
}

// Ascending fetches never walk.
func TestNewestWalkIntegration_ascendingUnchanged(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Hour)
	db, e := walkFixture(t, hoursFrom(now.Add(-2*time.Hour), 3))
	putEvent(t, db, now.Add(-time.Hour), "t", event.EventInsert)
	steps := countSteps(t)
	sameAsSingleStatement(t, e, Options{Limit: 3})
	if len(*steps) != 0 {
		t.Fatalf("an ascending fetch walked: %v", *steps)
	}
}

// The console's Recent changes read, through the merged fetch it calls
// (FetchMergedFull), with the page-plus-one limit it sends: the walk runs.
func TestNewestWalkIntegration_throughFetchMergedFull(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Hour)
	db, e := walkFixture(t, hoursFrom(now.Add(-2*time.Hour), 3+48))
	for i := range 20 {
		putEvent(t, db, now.Add(-2*time.Hour+time.Duration(i)*5*time.Minute), "t", event.EventInsert)
	}
	var dbName string
	if err := db.QueryRow("SELECT DATABASE()").Scan(&dbName); err != nil {
		t.Fatal(err)
	}
	steps := countSteps(t)
	opts := Options{Limit: 8 + 1, Order: "DESC"}
	rows, _, _, _, _, err := FetchMergedFull(context.Background(), db, e, FetchMergedOptions{
		Opts: opts, DBName: dbName, NoArchive: true, AllowGaps: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	want, err := e.fetchOnce(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	sortResults(want, "DESC")
	if !slices.Equal(eventIDs(rows), eventIDs(want)) {
		t.Fatalf("merged %v, single statement %v", eventIDs(rows), eventIDs(want))
	}
	if len(*steps) == 0 {
		t.Fatal("the merged fetch did not walk")
	}
}

// TestNewestWalkMeasure times the walk against the single statement on an
// index large enough for the difference to show. Opt-in: it writes about two
// million rows. BINTRAIL_NEWEST_MEASURE=1 to run it.
func TestNewestWalkMeasure(t *testing.T) {
	if os.Getenv("BINTRAIL_NEWEST_MEASURE") == "" {
		t.Skip("set BINTRAIL_NEWEST_MEASURE=1 to measure")
	}
	now := time.Now().UTC().Truncate(time.Hour)
	db, e := walkFixture(t, hoursFrom(now.Add(-23*time.Hour), 24+48))
	start := now.Add(-23 * time.Hour)
	span := 86400 // seconds the rows spread over; 3600 puts them all in one hour
	if v := os.Getenv("BINTRAIL_NEWEST_MEASURE_SPAN"); v != "" {
		fmt.Sscan(v, &span)
	}
	putEvent(t, db, start, "orders", event.EventInsert)
	for range 21 {
		testutil.MustExec(t, db, `INSERT INTO binlog_events
			(binlog_file, start_pos, end_pos, event_timestamp, schema_name, table_name, event_type, pk_values, row_after)
			SELECT binlog_file, start_pos, end_pos,
			       ? + INTERVAL FLOOR(RAND() * ?) SECOND,
			       schema_name, ELT(1 + FLOOR(RAND() * 4), 'orders', 'users', 'items', 'stock'), event_type, pk_values, row_after
			FROM binlog_events`, start, span)
	}
	if os.Getenv("BINTRAIL_NEWEST_MEASURE_ANALYZE") != "" {
		testutil.MustExec(t, db, "ANALYZE TABLE binlog_events")
	}
	var total int
	if err := db.QueryRow("SELECT COUNT(*) FROM binlog_events").Scan(&total); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for name, o := range map[string]Options{
		"unfiltered":   {Limit: 9, Order: "DESC"},
		"schema+table": {Limit: 9, Order: "DESC", Schema: "app", Table: "users"},
	} {
		for range 3 { // later runs: warm buffer pool
			t0 := time.Now()
			if _, err := e.fetchOnce(ctx, o); err != nil {
				t.Fatal(err)
			}
			single := time.Since(t0)
			t0 = time.Now()
			if _, err := e.Fetch(ctx, o); err != nil {
				t.Fatal(err)
			}
			walk := time.Since(t0)
			t0 = time.Now()
			if _, err := partitionTables(ctx, db, start.Format("p_2006010215"), Options{}); err != nil {
				t.Fatal(err)
			}
			t.Logf("  table listing of the oldest data hour alone: %v", time.Since(t0))
			t.Logf("%s, %d rows over %d s + 48 empty hours: single statement %v, walk %v", name, total, span, single, walk)
		}
		part := now.Format("p_2006010215")
		if span <= 3600 {
			part = start.Format("p_2006010215")
		}
		tables, err := partitionTables(ctx, db, part, o)
		if err != nil {
			t.Fatal(err)
		}
		bq, bargs := buildNewestBranchesFor(o, part, tables)
		explainLog(t, db, "walk tables "+part, "SELECT DISTINCT schema_name, table_name FROM binlog_events PARTITION (`"+part+"`)")
		explainLog(t, db, "walk branches "+part, bq, bargs...)
		q, args := buildQuery(o)
		explainLog(t, db, "single "+name, q, args...)
	}
}

func explainLog(t *testing.T, db *sql.DB, label, q string, args ...any) {
	t.Helper()
	{
		rows, err := db.Query("EXPLAIN "+q, args...)
		if err != nil {
			t.Fatal(err)
		}
		cols, _ := rows.Columns()
		for rows.Next() {
			vals := make([]sql.NullString, len(cols))
			ptrs := make([]any, len(cols))
			for i := range vals {
				ptrs[i] = &vals[i]
			}
			if err := rows.Scan(ptrs...); err != nil {
				t.Fatal(err)
			}
			line := ""
			for i, c := range cols {
				if c == "table" || c == "partitions" || c == "type" || c == "key" || c == "rows" || c == "Extra" {
					v := vals[i].String
					if c == "partitions" && len(v) > 40 {
						v = v[:40] + "..."
					}
					line += c + "=" + v + " "
				}
			}
			t.Logf("EXPLAIN %s: %s", label, line)
		}
		rows.Close()
	}
}

// Table names that the index's collation folds together (case) or keeps apart
// (trailing spaces under a NO PAD collation): the per-table branches neither
// read a row twice nor miss one, whatever the collation decides.
func TestNewestWalkIntegration_tableNamesTheCollationFolds(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Hour)
	db, e := walkFixture(t, hoursFrom(now.Add(-2*time.Hour), 3))
	names := []string{"Orders", "orders", "ORDERS", "t", "t ", "é", "e"}
	for i := range 21 {
		putEvent(t, db, now.Add(-time.Hour+time.Duration(i)*time.Minute), names[i%len(names)], event.EventInsert)
	}
	steps := countSteps(t)
	for _, n := range []int{1, 5, 21, 30} {
		got := sameAsSingleStatement(t, e, Options{Limit: n, Order: "DESC"})
		if want := min(n, 21); len(got) != want {
			t.Fatalf("limit %d: got %d rows, want %d", n, len(got), want)
		}
	}
	if len(*steps) == 0 {
		t.Fatal("the walk did not run")
	}
}

// Rows that commit between a step's table listing and its read: table T2's
// first row, then a newer row of T1. Read in one snapshot the page holds
// neither; read as two snapshots it would hold T1's newer row without T2's
// older one, a hole a newest-first scroll never returns to.
func TestNewestWalkIntegration_rowsCommittedMidStepLeaveNoHole(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Hour)
	db, e := walkFixture(t, hoursFrom(now.Add(-2*time.Hour), 3))
	base := now.Add(-time.Hour + 10*time.Minute)
	putEvent(t, db, base, "t1", event.EventInsert)
	part := now.Add(-time.Hour).Format("p_2006010215")
	done := false
	newestWalkListed = func(p string) {
		if p == part && !done {
			done = true
			putEvent(t, db, base.Add(time.Minute), "t2", event.EventInsert)
			putEvent(t, db, base.Add(2*time.Minute), "t1", event.EventInsert)
		}
	}
	t.Cleanup(func() { newestWalkListed = nil })
	got, err := e.Fetch(context.Background(), Options{Limit: 5, Order: "DESC"})
	if err != nil {
		t.Fatal(err)
	}
	if !done {
		t.Fatal("the hook never fired")
	}
	var sawT1Late, sawT2 bool
	for _, r := range got {
		sawT1Late = sawT1Late || (r.TableName == "t1" && r.EventTimestamp.Equal(base.Add(2*time.Minute)))
		sawT2 = sawT2 || r.TableName == "t2"
	}
	if sawT1Late && !sawT2 {
		t.Fatalf("page holds t1's later row but not t2's older one: %v", eventIDs(got))
	}
}

// A window that ends hours ago, on the init layout (48 empty future hours):
// the partitions after it are passed without a query.
func TestNewestWalkIntegration_oldWindowSkipsTheNewerPartitions(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Hour)
	db, e := walkFixture(t, hoursFrom(now.Add(-5*time.Hour), 6+48))
	for h := -5; h <= 0; h++ {
		putEvent(t, db, now.Add(time.Duration(h)*time.Hour+time.Minute), "t", event.EventInsert)
	}
	since, until := now.Add(-4*time.Hour), now.Add(-3*time.Hour+30*time.Minute)
	steps := countSteps(t)
	sameAsSingleStatement(t, e, Options{Limit: 9, Order: "DESC", Since: &since, Until: &until})
	want := []string{now.Add(-3 * time.Hour).Format("p_2006010215"), now.Add(-4 * time.Hour).Format("p_2006010215")}
	if !slices.Equal(*steps, want) {
		t.Fatalf("read %v, want only %v", *steps, want)
	}
}

// An access profile long enough that its filter values, repeated in one
// branch per table, would pass a prepared statement's 65,535 placeholders
// (1390): 600 denied tables (1,200 values) times 64 tables in the newest
// partition. The step sorts the partition whole instead, whose statement
// carries the values once, and the walk still answers.
func TestNewestWalkIntegration_longProfileStaysUnderThePlaceholderCap(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Hour)
	db, e := walkFixture(t, hoursFrom(now.Add(-2*time.Hour), 3))
	for i := range 64 {
		putEvent(t, db, now.Add(-time.Hour+time.Duration(i)*time.Second), fmt.Sprintf("t%02d", i), event.EventInsert)
	}
	deny := make([]SchemaTable, 600)
	for i := range deny {
		deny[i] = SchemaTable{Schema: "app", Table: fmt.Sprintf("denied%03d", i)}
	}
	deny[0].Table = "t05" // one real table denied, so the filter changes the page
	o := Options{Limit: 20, Order: "DESC", DenyTables: deny}
	got := sameAsSingleStatement(t, e, o)
	if len(got) != 20 {
		t.Fatalf("got %d rows", len(got))
	}
	if _, walked, err := fetchNewestByPartition(context.Background(), db, o); err != nil || !walked {
		t.Fatalf("walked %v (%v): the walk must answer under a long profile", walked, err)
	}
}
