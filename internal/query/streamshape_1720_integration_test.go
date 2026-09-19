//go:build integration

package query

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// #1720, against a real optimizer (CI runs MySQL 8.0 and 8.4): the stream
// shape is read in index order with no sort and no join, the event_id floor is
// evaluated inside the index range, and the rows it returns are exactly the
// rows the position gate admits — including the execute-before/commit-after
// row #797 exists for, and across a binlog file-name rollover (#840).

type probeEvent struct {
	file  string
	start uint64
	ts    time.Time
	table string
	pk    int
}

// seedInterleaved writes three tables' events interleaved in binlog order,
// positions monotone, ids assigned in insert order (as `stream` assigns them).
// Returns the events in insert order; index i has event_id i+1.
func seedInterleaved(t *testing.T, db *sql.DB, h0 time.Time) []probeEvent {
	t.Helper()
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	stmt, err := tx.Prepare("INSERT INTO binlog_events (binlog_file,start_pos,end_pos,event_timestamp,schema_name,table_name,event_type,pk_values,row_after) VALUES (?,?,?,?,?,?,?,?,?)")
	if err != nil {
		t.Fatal(err)
	}
	var evs []probeEvent
	pos := uint64(1000)
	file := "binlog.999999"
	for i := range 3000 {
		if i == 2000 {
			// The rollover: a longer name that sorts BEFORE the shorter one
			// lexically. A plain string comparison would put it first.
			file, pos = "binlog.1000000", 4
		}
		ts := h0.Add(time.Duration(i) * 2 * time.Second)
		ev := probeEvent{file: file, start: pos, ts: ts, table: []string{"a", "b", "c"}[i%3], pk: i}
		if i == 1501 {
			// #797: executed 40 minutes before its neighbours (a long
			// transaction) but committed, and logged, right here.
			ev.ts = ts.Add(-40 * time.Minute)
		}
		if _, err := stmt.Exec(ev.file, ev.start, ev.start+100, ev.ts.Format("2006-01-02 15:04:05"), "shop", ev.table, 2, fmt.Sprint(ev.pk), `{"id":1}`); err != nil {
			t.Fatal(err)
		}
		evs = append(evs, ev)
		pos += 100
	}
	stmt.Close()
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("ANALYZE TABLE binlog_events"); err != nil {
		t.Fatal(err)
	}
	return evs
}

func explainTree(t *testing.T, db *sql.DB, q string, args ...any) string {
	t.Helper()
	var out string
	if err := db.QueryRow("EXPLAIN FORMAT=TREE "+q, args...).Scan(&out); err != nil {
		t.Fatalf("EXPLAIN: %v\n%s", err, q)
	}
	return out
}

func TestStreamShape_indexOrderNoSortNoJoin(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()
	db, dbName := testutil.CreateTestDB(t)
	if err := indexer.CreateIndexTables(ctx, db, 48, false, nil); err != nil {
		t.Fatal(err)
	}
	h0 := time.Now().UTC().Truncate(time.Hour).Add(time.Hour)
	evs := seedInterleaved(t, db, h0)

	// Anchor: the position of event 1500 (id 1501). Cut: event 2500's end.
	anchor := &BinlogPos{File: evs[1500].file, Pos: evs[1500].start}
	cut := &BinlogPos{File: evs[2500].file, Pos: evs[2500].start + 100}
	since := evs[1500].ts
	until := evs[2500].ts.Add(time.Minute)

	// Ground truth from the seed: table b, admitted by POSITION, in
	// (event_timestamp, event_id) order.
	var want []int
	for i, ev := range evs {
		if ev.table != "b" {
			continue
		}
		afterAnchor := len(ev.file) > len(anchor.File) || (len(ev.file) == len(anchor.File) && ev.file > anchor.File) || (ev.file == anchor.File && ev.start >= anchor.Pos)
		beforeCut := len(ev.file) < len(cut.File) || (len(ev.file) == len(cut.File) && ev.file < cut.File) || (ev.file == cut.File && ev.start+100 <= cut.Pos)
		if afterAnchor && beforeCut {
			want = append(want, i+1)
		}
	}
	// The #797 row (i=1501, table b) is admitted by position and sits before
	// the others by time; a fetch that tightened the time floor would lose it.
	if want[0] != 1502 {
		t.Fatalf("fixture: want[0] = %d, expected the early-executed row 1502 first", want[0])
	}

	opts := Options{Schema: "shop", Table: "b", Since: &since, SincePos: anchor, Until: &until, UntilPos: cut, Limit: 10000, Order: "ASC"}
	q, args := buildQuery(opts)
	tree := explainTree(t, db, q, args...)
	switch {
	case !strings.Contains(tree, "using idx_row_lookup"):
		t.Errorf("stream shape does not range-scan idx_row_lookup:\n%s", tree)
	case strings.Contains(tree, "Sort"):
		t.Errorf("stream shape sorts (the index order should satisfy the ORDER BY):\n%s", tree)
	case strings.Contains(tree, "Nested loop") || strings.Contains(tree, "Materialize"):
		t.Errorf("stream shape still joins a materialised key set:\n%s", tree)
	}

	got := fetchIDs(t, db, opts)
	if !equalInts(got, want) {
		t.Fatalf("stream shape rows = %v\nwant %v", got, want)
	}

	// The event_id floor: the anchor's own id. Same rows, and the floor is in
	// the index range, not a filter over fetched rows.
	opts.SinceEventID = 1501
	q, args = buildQuery(opts)
	tree = explainTree(t, db, q, args...)
	if !strings.Contains(tree, "< event_id") && !strings.Contains(tree, "event_id >") {
		t.Errorf("event_id floor is neither in the index range nor in the index condition:\n%s", tree)
	}
	if i := strings.Index(tree, "Filter:"); i >= 0 {
		line, _, _ := strings.Cut(tree[i:], "\n")
		if strings.Contains(line, "event_id") {
			t.Errorf("event_id floor is evaluated as a row filter:\n%s", tree)
		}
	}
	if got := fetchIDs(t, db, opts); !equalInts(got, want) {
		t.Fatalf("with the event_id floor rows = %v\nwant %v", got, want)
	}

	// Paged through the stream, three pages, same set in the same order.
	var paged []int
	_, err := FetchMergedStream(ctx, db, New(db), FetchMergedOptions{Opts: Options{
		Schema: "shop", Table: "b", Since: &since, SincePos: anchor, Until: &until, UntilPos: cut, SinceEventID: 1501, Order: "ASC",
	}, NoArchive: true, DBName: dbName}, 120, func(page []ResultRow) error {
		for _, r := range page {
			paged = append(paged, int(r.EventID))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !equalInts(paged, want) {
		t.Fatalf("paged rows = %v\nwant %v", paged, want)
	}
}

func fetchIDs(t *testing.T, db *sql.DB, opts Options) []int {
	t.Helper()
	rows, err := New(db).Fetch(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	out := make([]int, len(rows))
	for i, r := range rows {
		out[i] = int(r.EventID)
	}
	return out
}

func equalInts(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
