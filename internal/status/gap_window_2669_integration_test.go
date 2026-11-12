//go:build integration

package status_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/status"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// #2669 against a real index, through the collectors: CollectStatus (behind
// `bintrail status`, GET /api/status and the MCP status tool) and
// CollectCoverageSummary (behind GET /api/coverage). An acknowledged loss is
// still reported while the index or its archives serve changes from both sides
// of it, which the delta floor alone does not see in two places: archived hours
// separated from the index by a stretch nobody archived, and events older than
// the name of the partition that holds them.
func TestAcknowledgedLossWindow_belowTheFloor(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()
	db, dbName := testutil.CreateTestDB(t)
	if err := indexer.CreateIndexTables(ctx, db, 4, false, nil); err != nil {
		t.Fatalf("CreateIndexTables: %v", err)
	}
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	exec := func(q string, args ...any) {
		t.Helper()
		if _, err := db.ExecContext(ctx, q, args...); err != nil {
			t.Fatalf("exec %q: %v", q, err)
		}
	}
	exec("INSERT INTO stream_state (id, mode, server_id, last_checkpoint) VALUES (1, 'gtid', 7, UTC_TIMESTAMP())")

	live, err := status.OldestDeltaFromDB(ctx, db, dbName)
	if err != nil || live.Hour.IsZero() || live.BelowIsUnknown {
		t.Fatalf("fixture: delta floor = %+v, err %v; want a known floor", live, err)
	}
	F := live.Hour
	day := 24 * time.Hour
	pname := func(t time.Time) string { return t.UTC().Format("p_2006010215") }
	ts := func(t time.Time) string { return t.UTC().Format("2006-01-02 15:04:05") }

	// Point 6 is real: an event dated weeks before the oldest partition is
	// accepted, and lands IN that partition, under a later hour's name.
	exec(`INSERT INTO binlog_events (binlog_file, start_pos, end_pos, event_timestamp, schema_name, table_name, event_type, pk_values)
		VALUES ('mysql-bin.000001', 4, 100, ?, 'shop', 'orders', 1, '1')`, ts(F.Add(-10*day)))
	var inOldest int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM binlog_events PARTITION (`"+pname(F)+"`)").Scan(&inOldest); err != nil {
		t.Fatal(err)
	}
	if inOldest != 1 {
		t.Fatalf("the old event is not in the oldest partition %s (count %d)", pname(F), inOldest)
	}
	t.Logf("an event dated %s sits in partition %s", ts(F.Add(-10*day)), pname(F))
	explain(t, db, "EXPLAIN SELECT MIN(event_timestamp) FROM binlog_events WHERE event_timestamp <= ? GROUP BY schema_name, table_name LIMIT 1", ts(F.Add(-5*day)))
	exec("DELETE FROM binlog_events")

	type archived struct {
		hour     time.Time
		minEvent time.Time // zero = not recorded
	}
	for _, c := range []struct {
		name     string
		archives []archived
		oldEvent time.Time // zero = none
		lost     time.Time
		want     string
	}{
		{"archives separated from the index, loss among them",
			[]archived{{hour: F.Add(-30 * day)}, {hour: F.Add(-20 * day)}}, time.Time{}, F.Add(-25 * day), status.ContinuityGapAcknowledged},
		{"loss in the stretch nobody archived",
			[]archived{{hour: F.Add(-30 * day)}, {hour: F.Add(-20 * day)}}, time.Time{}, F.Add(-10 * day), status.ContinuityGapAcknowledged},
		{"loss at the start of the oldest archived hour",
			[]archived{{hour: F.Add(-30 * day)}, {hour: F.Add(-20 * day)}}, time.Time{}, F.Add(-30 * day), status.ContinuityGapAcknowledged},
		{"loss one second before every archive",
			[]archived{{hour: F.Add(-30 * day)}, {hour: F.Add(-20 * day)}}, time.Time{}, F.Add(-30*day - time.Second), status.ContinuityOK},
		{"re-indexed old binlog in the oldest partition, loss after its events",
			nil, F.Add(-10 * day), F.Add(-5 * day), status.ContinuityGapAcknowledged},
		{"re-indexed old binlog in the oldest partition, loss before its events",
			nil, F.Add(-10 * day), F.Add(-15 * day), status.ContinuityOK},
		{"an archived hour holds re-indexed changes older than the loss",
			[]archived{{hour: F.Add(-48 * time.Hour), minEvent: F.Add(-20 * day)}}, time.Time{}, F.Add(-10 * day), status.ContinuityGapAcknowledged},
		{"an event at the very second of the loss",
			nil, F.Add(-10 * day), F.Add(-10 * day), status.ContinuityGapAcknowledged},
		{"an event one second after the loss",
			nil, F.Add(-10*day + time.Second), F.Add(-10 * day), status.ContinuityOK},
		{"nothing older than the loss anywhere",
			[]archived{{hour: F.Add(-48 * time.Hour), minEvent: F.Add(-47 * time.Hour)}}, time.Time{}, F.Add(-10 * day), status.ContinuityOK},
	} {
		t.Run(c.name, func(t *testing.T) {
			exec("DELETE FROM archive_state")
			exec("DELETE FROM binlog_events")
			for _, a := range c.archives {
				var minEvent any
				if !a.minEvent.IsZero() {
					minEvent = ts(a.minEvent)
				}
				exec("INSERT INTO archive_state (partition_name, bintrail_id, local_path, min_event_ts) VALUES (?, 'id-1', '/tmp/x', ?)", pname(a.hour), minEvent)
			}
			if !c.oldEvent.IsZero() {
				exec(`INSERT INTO binlog_events (binlog_file, start_pos, end_pos, event_timestamp, schema_name, table_name, event_type, pk_values)
					VALUES ('mysql-bin.000001', 4, 100, ?, 'shop', 'orders', 1, '1')`, ts(c.oldEvent))
			}
			exec("UPDATE stream_state SET gap_lost_at = ?, gap_lost_detail = 'binlogs purged', gap_acked_at = ? WHERE id = 1",
				ts(c.lost), ts(c.lost.Add(time.Hour)))

			// The floor snapshots are graded against does not move.
			if floor, err := status.OldestDeltaFromDB(ctx, db, dbName); err != nil || !floor.Hour.Equal(F) {
				t.Errorf("snapshot floor = %+v, err %v; want %v", floor, err, F)
			}

			data, err := status.CollectStatus(ctx, db, dbName)
			if err != nil {
				t.Fatalf("CollectStatus: %v", err)
			}
			var tb, jb bytes.Buffer
			data.Write(&tb)
			if err := data.WriteJSON(&jb); err != nil {
				t.Fatal(err)
			}
			var d struct {
				Stream struct {
					Continuity struct {
						Status string `json:"status"`
					} `json:"continuity"`
				} `json:"stream"`
			}
			if err := json.Unmarshal(jb.Bytes(), &d); err != nil {
				t.Fatalf("invalid JSON: %v\n%s", err, jb.String())
			}
			sum, err := status.CollectCoverageSummary(ctx, db, dbName, time.Now().UTC())
			if err != nil {
				t.Fatalf("CollectCoverageSummary: %v", err)
			}
			t.Logf("loss %s: status %q, coverage %q (delta_from %s)", ts(c.lost), d.Stream.Continuity.Status, sum.Continuity, ts(sum.Floor.Hour))
			for _, l := range strings.Split(tb.String(), "\n") {
				if strings.Contains(l, "Continuity:") || strings.Contains(l, "Acknowledged loss") ||
					strings.Contains(l, "refused.") || strings.Contains(l, "or earlier") {
					t.Log(l)
				}
			}
			if d.Stream.Continuity.Status != c.want {
				t.Errorf("status continuity = %q, want %q", d.Stream.Continuity.Status, c.want)
			}
			if sum.Continuity != d.Stream.Continuity.Status {
				t.Errorf("coverage summary says %q, status says %q", sum.Continuity, d.Stream.Continuity.Status)
			}
		})
	}
}

// explain logs the plan of q: which partitions it reads, and how.
func explain(t *testing.T, db *sql.DB, q string, args ...any) {
	t.Helper()
	rows, err := db.Query(q, args...)
	if err != nil {
		t.Fatalf("explain: %v", err)
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
		var parts []string
		for i, c := range cols {
			parts = append(parts, c+"="+string(vals[i]))
		}
		t.Logf("plan: %s", strings.Join(parts, " "))
	}
}

// A min_event_ts stored as the zero date (non-strict sql_mode, old data) is
// not an event time. It must neither erase an archived hour found before the
// loss nor hide a real oldest event recorded for another file (review of
// #2711).
func TestAcknowledgedLossWindow_zeroDateMinEvent(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()
	db, dbName := testutil.CreateTestDB(t)
	if err := indexer.CreateIndexTables(ctx, db, 4, false, nil); err != nil {
		t.Fatalf("CreateIndexTables: %v", err)
	}
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	live, err := status.OldestDeltaFromDB(ctx, db, dbName)
	if err != nil || live.Hour.IsZero() {
		t.Fatalf("fixture: floor %+v, %v", live, err)
	}
	F := live.Hour
	day := 24 * time.Hour
	pname := func(t time.Time) string { return t.UTC().Format("p_2006010215") }
	ts := func(t time.Time) string { return t.UTC().Format("2006-01-02 15:04:05") }
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	exec := func(q string, args ...any) {
		t.Helper()
		if _, err := conn.ExecContext(ctx, q, args...); err != nil {
			t.Fatalf("exec %q: %v", q, err)
		}
	}
	exec("SET SESSION sql_mode = ''")
	exec("INSERT INTO stream_state (id, mode, server_id, last_checkpoint) VALUES (1, 'gtid', 7, UTC_TIMESTAMP())")
	for _, c := range []struct {
		name string
		rows [][2]any // partition_name, min_event_ts
		lost time.Time
	}{
		{"an archived hour before the loss beside a zero-date row",
			[][2]any{{pname(F.Add(-30 * day)), nil}, {pname(F.Add(-48 * time.Hour)), "0000-00-00 00:00:00"}}, F.Add(-25 * day)},
		{"a recorded oldest event before the loss beside a zero-date row",
			[][2]any{{pname(F.Add(-48 * time.Hour)), ts(F.Add(-20 * day))}, {pname(F.Add(-47 * time.Hour)), "0000-00-00 00:00:00"}}, F.Add(-10 * day)},
	} {
		t.Run(c.name, func(t *testing.T) {
			exec("DELETE FROM archive_state")
			for _, r := range c.rows {
				exec("INSERT INTO archive_state (partition_name, bintrail_id, local_path, min_event_ts) VALUES (?, 'id-1', '/tmp/x', ?)", r[0], r[1])
			}
			var zeros int
			if err := conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM archive_state WHERE min_event_ts = '0000-00-00 00:00:00'").Scan(&zeros); err != nil || zeros != 1 {
				t.Fatalf("fixture: %d zero-date rows, err %v", zeros, err)
			}
			exec("UPDATE stream_state SET gap_lost_at = ?, gap_lost_detail = 'binlogs purged', gap_acked_at = ? WHERE id = 1",
				ts(c.lost), ts(c.lost.Add(time.Hour)))
			data, err := status.CollectStatus(ctx, db, dbName)
			if err != nil {
				t.Fatalf("CollectStatus: %v", err)
			}
			got := status.ContinuityStatus(data.Stream, data.StreamErr)
			sum, err := status.CollectCoverageSummary(ctx, db, dbName, time.Now().UTC())
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("loss %s: status %q, coverage %q", ts(c.lost), got, sum.Continuity)
			if got != status.ContinuityGapAcknowledged || sum.Continuity != got {
				t.Errorf("status %q, coverage %q; want gap_acknowledged from both", got, sum.Continuity)
			}
		})
	}
}
