package query

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// The "latest N per key" window without LatestPerPKCandidates is byte for
// byte what it was before #2156: the PostgreSQL verify path (verify_pg.go,
// these options exactly) and every caller that did not opt in read what they
// always read. The string was captured from the build before the change.
func TestBuildQuery_limitPerPKGolden2156(t *testing.T) {
	since := time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)
	until := since.Add(time.Hour)
	opts := Options{Schema: "app", Table: "orders", Since: &since, Until: &until, LimitPerPK: 1}
	const golden = "SELECT be.event_id, be.binlog_file, be.start_pos, be.end_pos, be.event_timestamp,\n\t         be.gtid, be.connection_id, be.schema_name, be.table_name, be.event_type, be.pk_values,\n\t         be.changed_columns, be.row_before, be.row_after, be.schema_version, be.query_text, be.query_hash,\n\t         be.commit_ts_us FROM binlog_events AS be JOIN (SELECT event_id, event_timestamp FROM (SELECT event_id, event_timestamp, ROW_NUMBER() OVER (PARTITION BY pk_values ORDER BY event_timestamp DESC, event_id DESC) AS bt_rn FROM binlog_events WHERE schema_name = ? AND table_name = ? AND TO_SECONDS(event_timestamp) >= 63958420800 AND event_timestamp >= ? AND TO_SECONDS(event_timestamp) < 63958428000 AND event_timestamp <= ?) AS w WHERE bt_rn <= ?) AS k ON be.event_id = k.event_id AND be.event_timestamp = k.event_timestamp"
	q, args := buildQuery(opts)
	if q != golden {
		t.Fatalf("the window changed for a caller that did not opt in:\n got %q\nwant %q", q, golden)
	}
	if got := fmt.Sprint(args); got != fmt.Sprint([]any{"app", "orders", since, until, 1}) {
		t.Fatalf("args = %s", got)
	}

	// Opted in: the same statement plus the rank by event_id, and the bind
	// argument once more.
	opts.LatestPerPKCandidates = true
	q, args = buildQuery(opts)
	// And the key span, carried through the key subquery to the outer row.
	want := strings.Replace(golden, "AS bt_rn FROM", "AS bt_rn, ROW_NUMBER() OVER (PARTITION BY pk_values ORDER BY event_id DESC) AS bt_rn_id"+
		", MIN(CAST(CONCAT(LPAD(LENGTH(COALESCE(binlog_file, '')), 4, '0'), COALESCE(binlog_file, '')) AS BINARY)) OVER (PARTITION BY pk_values) AS bt_kf"+
		", MAX(CAST(CONCAT(LPAD(LENGTH(COALESCE(binlog_file, '')), 4, '0'), COALESCE(binlog_file, ''), LPAD(COALESCE(start_pos, 0), 20, '0')) AS BINARY)) OVER (PARTITION BY pk_values) AS bt_kc FROM", 1)
	want = strings.Replace(want, "WHERE bt_rn <= ?)", "WHERE bt_rn <= ? OR bt_rn_id <= ?)", 1)
	want = strings.Replace(want, "SELECT event_id, event_timestamp FROM (SELECT", "SELECT event_id, event_timestamp, bt_kf, bt_kc FROM (SELECT", 1)
	want = strings.Replace(want, "be.commit_ts_us FROM", "be.commit_ts_us, k.bt_kf, k.bt_kc FROM", 1)
	if q != want {
		t.Fatalf("candidates:\n got %q\nwant %q", q, want)
	}
	if got := fmt.Sprint(args); got != fmt.Sprint([]any{"app", "orders", since, until, 1, 1}) {
		t.Fatalf("args = %s", got)
	}
}
