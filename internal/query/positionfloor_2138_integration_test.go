//go:build integration

package query

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// #2138 against a real index: what LoadPartitionHeads reads off real
// partitions, and what SinceFor answers.

func headsRig(t *testing.T, partitions int) (*sql.DB, time.Time) {
	t.Helper()
	testutil.SkipIfNoMySQL(t)
	db, _ := testutil.CreateTestDB(t)
	if err := indexer.CreateIndexTables(context.Background(), db, partitions, false, nil); err != nil {
		t.Fatalf("CreateIndexTables: %v", err)
	}
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	return db, time.Now().UTC().Truncate(time.Hour).Add(time.Hour)
}

func insertHeadEvent(t *testing.T, db *sql.DB, table string, start uint64, at time.Time) {
	t.Helper()
	testutil.MustExec(t, db, fmt.Sprintf(`INSERT INTO binlog_events
		(binlog_file, start_pos, end_pos, event_timestamp, schema_name, table_name, event_type, pk_values, row_after)
		VALUES ('binlog.000001', %d, %d, '%s', 'shop', '%s', 1, '1', '{"id":1}')`,
		start, start+100, at.Format("2006-01-02 15:04:05"), table))
}

func TestPartitionHeads_readsTheNewestRowOfEachPartition(t *testing.T) {
	ctx := context.Background()
	db, first := headsRig(t, 12)
	hour := func(n int) time.Time { return first.Add(time.Duration(n)*time.Hour + 10*time.Minute) }
	// Written in binlog order. Hour 2 receives a late row LAST (position 900).
	insertHeadEvent(t, db, "orders", 100, hour(2))
	insertHeadEvent(t, db, "orders", 200, hour(5))
	insertHeadEvent(t, db, "items", 300, hour(8))
	insertHeadEvent(t, db, "items", 900, hour(2).Add(time.Minute))

	h, err := LoadPartitionHeads(ctx, db)
	if err != nil {
		t.Fatalf("LoadPartitionHeads: %v", err)
	}
	var got []string
	for i, p := range h.parts {
		if i == 0 != p.open {
			t.Fatalf("partition %d (%s): open = %v", i, p.name, p.open)
		}
		if i > 0 && !p.lower.After(h.parts[i-1].lower) && !h.parts[i-1].open {
			t.Fatalf("partition %s: lower bound %s is not after the previous one's", p.name, p.lower)
		}
		if !p.empty {
			got = append(got, fmt.Sprintf("%s@%d", p.lower.Format("15"), p.pos.Pos))
		}
	}
	want := []string{
		fmt.Sprintf("%s@900", hour(2).Format("15")),
		fmt.Sprintf("%s@200", hour(5).Format("15")),
		fmt.Sprintf("%s@300", hour(8).Format("15")),
	}
	if strings.Join(got, " ") != strings.Join(want, " ") {
		t.Fatalf("heads (lower-bound hour @ position) = %v, want %v", got, want)
	}
	if last := h.parts[len(h.parts)-1]; last.name != "p_future" || !last.empty {
		t.Fatalf("last partition = %+v, want an empty p_future", last)
	}
	if h.streamCaptured || !h.lastFileIndexed.IsZero() {
		t.Fatalf("a fresh index: streamCaptured=%v lastFileIndexed=%s", h.streamCaptured, h.lastFileIndexed)
	}

	// A fetch for a snapshot stamped at hour 9, anchored at 800: only the
	// late row is after the anchor, and it is in hour 2.
	since := hour(9)
	anchor := &BinlogPos{File: "binlog.000001", Pos: 800}
	for _, tc := range []struct {
		name   string
		opts   Options
		want   time.Time
		widens bool
	}{
		// items has the late row in hour 2: the fetch starts there.
		{"the table with the late event", Options{Schema: "shop", Table: "items", Since: &since, SincePos: anchor}, hour(2).Add(time.Minute), true},
		// orders has a row in that partition too. It is before the anchor,
		// but the newest row of the partition is not, and the picture is per
		// partition: the fetch reaches it and the position gate drops the row.
		{"another table with rows in that partition", Options{Schema: "shop", Table: "orders", Since: &since, SincePos: anchor}, hour(2), true},
		{"a table with nothing there", Options{Schema: "shop", Table: "absent", Since: &since, SincePos: anchor}, since, false},
		{"an anchor after everything", Options{Schema: "shop", Table: "items", Since: &since, SincePos: &BinlogPos{File: "binlog.000001", Pos: 901}}, since, false},
		{"no table named", Options{Since: &since, SincePos: anchor}, hour(2), true},
		{"no position: the time is the caller's exact filter", Options{Schema: "shop", Table: "items", Since: &since}, since, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := h.SinceFor(ctx, db, tc.opts)
			if err != nil {
				t.Fatalf("SinceFor: %v", err)
			}
			if !got.Equal(tc.want) {
				t.Fatalf("SinceFor = %s, want %s", got.Format(time.RFC3339), tc.want.Format(time.RFC3339))
			}
			if (got != tc.opts.Since) != tc.widens {
				t.Fatalf("SinceFor returned a new time = %v, want %v", got != tc.opts.Since, tc.widens)
			}
		})
	}

	// And through the fetch itself: the late row comes back.
	rows, _, err := FetchMerged(ctx, db, New(db), FetchMergedOptions{
		Opts:      Options{Schema: "shop", Table: "items", Since: &since, SincePos: anchor},
		NoArchive: true, AllowGaps: true,
	})
	if err != nil || len(rows) != 1 || rows[0].StartPos != 900 {
		t.Fatalf("FetchMerged: %d rows, err=%v; want the one event at position 900", len(rows), err)
	}

	// A stream index into which a file was being indexed: recorded, and an
	// in-progress run counts as now.
	testutil.MustExec(t, db, `INSERT INTO stream_state (id, mode, binlog_file, binlog_position, last_checkpoint, server_id)
		VALUES (1, 'position', 'binlog.000001', 4, NOW(), 1)`)
	testutil.MustExec(t, db, `INSERT INTO index_state (binlog_file, file_size, last_position, events_indexed, status, started_at)
		VALUES ('binlog.000001', 1, 4, 0, 'in_progress', '2020-01-01 00:00:00')`)
	if h, err = LoadPartitionHeads(ctx, db); err != nil {
		t.Fatalf("LoadPartitionHeads: %v", err)
	}
	if !h.streamCaptured || time.Since(h.lastFileIndexed) > 5*time.Minute {
		t.Fatalf("streamCaptured=%v lastFileIndexed=%s, want a stream and a run in progress now", h.streamCaptured, h.lastFileIndexed)
	}
}

// TestPartitionHeads_notPartitioned: an index whose binlog_events has no
// partitions is one partition with no bounds.
func TestPartitionHeads_notPartitioned(t *testing.T) {
	ctx := context.Background()
	db, first := headsRig(t, 4)
	testutil.MustExec(t, db, "ALTER TABLE binlog_events REMOVE PARTITIONING")
	insertHeadEvent(t, db, "items", 900, first.Add(10*time.Minute))
	h, err := LoadPartitionHeads(ctx, db)
	if err != nil {
		t.Fatalf("LoadPartitionHeads: %v", err)
	}
	if len(h.parts) != 1 || h.parts[0].name != "" || !h.parts[0].open || h.parts[0].pos.Pos != 900 {
		t.Fatalf("parts = %+v, want one unnamed open partition with its newest row at 900", h.parts)
	}
	since := first.Add(9 * time.Hour)
	got, err := h.SinceFor(ctx, db, Options{Schema: "shop", Table: "items", Since: &since, SincePos: &BinlogPos{File: "binlog.000001", Pos: 800}})
	if err != nil || !got.Equal(first.Add(10*time.Minute)) {
		t.Fatalf("SinceFor = %v, err=%v; want the event's own time", got, err)
	}
}

// TestMeasurePartitionHeads prints what the #2138 floor costs on an index of
// BINTRAIL_MEASURE_2138 events (for example 3000000) spread over 720 hourly
// partitions and 50 tables. Not an assertion: skipped unless the variable is
// set.
func TestMeasurePartitionHeads(t *testing.T) {
	n, _ := strconv.Atoi(os.Getenv("BINTRAIL_MEASURE_2138"))
	if n <= 0 {
		t.Skip("set BINTRAIL_MEASURE_2138=<events> to measure")
	}
	const parts = 720
	ctx := context.Background()
	db, first := headsRig(t, parts)
	testutil.MustExec(t, db, `INSERT INTO stream_state (id, mode, binlog_file, binlog_position, last_checkpoint, server_id)
		VALUES (1, 'position', 'binlog.000001', 4, NOW(), 1)`)
	span := int64((parts - 2) * 3600)
	const chunk = 100000
	for off := 0; off < n; off += chunk {
		testutil.MustExec(t, db, fmt.Sprintf(`INSERT /*+ SET_VAR(cte_max_recursion_depth = 200000) */ INTO binlog_events
			(binlog_file, start_pos, end_pos, event_timestamp, schema_name, table_name, event_type, pk_values, row_before, row_after)
			WITH RECURSIVE s (i) AS (SELECT %d UNION ALL SELECT i + 1 FROM s WHERE i + 1 < %d)
			SELECT 'binlog.000001', i * 100, i * 100 + 99,
			       TIMESTAMPADD(SECOND, FLOOR(i * %d / %d), '%s'),
			       'shop', CONCAT('t', i %% 50), 2, i %% 100000,
			       JSON_OBJECT('id', i %% 100000, 'status', 'before', 'note', REPEAT('x', 120)),
			       JSON_OBJECT('id', i %% 100000, 'status', 'after', 'note', REPEAT('y', 120))
			FROM s`, off, min(off+chunk, n), span, n, first.Format("2006-01-02 15:04:05")))
	}
	testutil.MustExec(t, db, "ANALYZE TABLE binlog_events")
	timeIt := func(what string, f func() string) {
		start := time.Now()
		detail := f()
		t.Logf("%-58s %8.1f ms  %s", what, float64(time.Since(start).Microseconds())/1000, detail)
	}
	var h *PartitionHeads
	for range 3 {
		timeIt(fmt.Sprintf("LoadPartitionHeads, %d partitions", parts+1), func() string {
			var err error
			if h, err = LoadPartitionHeads(ctx, db); err != nil {
				t.Fatal(err)
			}
			return ""
		})
	}
	// A daily refresh: the previous snapshot is 24 h before the newest event.
	end := first.Add(time.Duration(span) * time.Second)
	since := end.Add(-24 * time.Hour)
	cutID := uint64(int64(n) - int64(n)*24*3600/span)
	steady := Options{Schema: "shop", Table: "t7", Since: &since, SincePos: &BinlogPos{File: "binlog.000001", Pos: cutID * 100}, SinceEventID: cutID}
	count := func(o Options) string {
		q, args := buildQuery(o)
		var c int
		if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM ("+q+") x", args...).Scan(&c); err != nil {
			t.Fatal(err)
		}
		return fmt.Sprintf("%d rows", c)
	}
	for range 2 {
		timeIt("SinceFor, nothing late (steady state)", func() string {
			got, err := h.SinceFor(ctx, db, steady)
			if err != nil {
				t.Fatal(err)
			}
			return fmt.Sprintf("widened=%v", got != steady.Since)
		})
		timeIt("fetch of one table, floor as before", func() string { return count(steady) })
	}
	// Capture 26 h behind when the previous snapshot was written: the events
	// after its position start 26 h before its stamp.
	lagID := uint64(int64(n) - int64(n)*50*3600/span)
	lag := steady
	lag.SincePos, lag.SinceEventID = &BinlogPos{File: "binlog.000001", Pos: lagID * 100}, lagID
	var widened *time.Time
	for range 2 {
		timeIt("SinceFor, 26 partitions hold late events", func() string {
			var err error
			if widened, err = h.SinceFor(ctx, db, lag); err != nil {
				t.Fatal(err)
			}
			return fmt.Sprintf("from %s to %s", since.Format("01-02T15:04"), widened.Format("01-02T15:04"))
		})
		timeIt("fetch of one table, floor as before (loses rows)", func() string { return count(lag) })
		w := lag
		w.Since = widened
		timeIt("fetch of one table, floor from the index", func() string { return count(w) })
	}
	// The alternative that was weighed: no time floor at all, the event id
	// alone (and the position).
	noFloor := lag
	noFloor.Since = nil
	for range 2 {
		timeIt("fetch of one table, no time floor (id and position only)", func() string { return count(noFloor) })
	}
	// File-indexing since the snapshot: every partition below the floor.
	h.lastFileIndexed = since.Add(time.Minute)
	for range 2 {
		timeIt("SinceFor, files indexed since the snapshot (all partitions)", func() string {
			got, err := h.SinceFor(ctx, db, steady)
			if err != nil {
				t.Fatal(err)
			}
			w := steady
			w.Since = got
			return "then the fetch: " + count(w)
		})
	}
	q := "EXPLAIN SELECT event_timestamp FROM binlog_events" + partitionClause(h.below(since, *lag.SincePos)) +
		" WHERE event_timestamp < ? AND schema_name = ? AND table_name = ? ORDER BY event_timestamp LIMIT 1"
	rows, err := db.QueryContext(ctx, q, CoarseSinceFloor(since), "shop", "t7")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	cols, _ := rows.Columns()
	for rows.Next() {
		vals := make([]sql.RawBytes, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatal(err)
		}
		var sb strings.Builder
		for i, c := range cols {
			if c == "partitions" {
				fmt.Fprintf(&sb, "partitions=%d ", strings.Count(string(vals[i]), ",")+1)
				continue
			}
			fmt.Fprintf(&sb, "%s=%s ", c, vals[i])
		}
		t.Log("EXPLAIN oldest-row query: " + sb.String())
	}
}
