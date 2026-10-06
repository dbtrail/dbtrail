package parquetquery

import (
	"context"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/buffer"
	"github.com/dbtrail/dbtrail/internal/query"
)

// The DuckDB twin of the "latest N per key" window, byte for byte what it was
// before #2156 for a caller that does not opt in (the PostgreSQL verify path's
// options). Captured from the build before the change.
func TestBuildQuery_limitPerPKGolden2156(t *testing.T) {
	since := time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)
	until := since.Add(time.Hour)
	opts := query.Options{Schema: "app", Table: "orders", Since: &since, Until: &until, LimitPerPK: 1}
	const golden = "SELECT event_id, binlog_file, start_pos, end_pos, event_timestamp, gtid, connection_id, schema_name, table_name, event_type, pk_values, changed_columns, row_before, row_after, schema_version, query_text, query_hash, commit_ts_us FROM parquet_scan('/arc/*.parquet', hive_partitioning=true, union_by_name=true) WHERE schema_name = ? AND table_name = ? AND event_timestamp >= ? AND event_timestamp <= ? QUALIFY ROW_NUMBER() OVER (PARTITION BY pk_values ORDER BY event_timestamp DESC, event_id DESC) <= ? ORDER BY event_timestamp ASC, event_id ASC"
	q, args := buildQuery("/arc/*.parquet", opts)
	if q != golden {
		t.Fatalf("the window changed for a caller that did not opt in:\n got %q\nwant %q", q, golden)
	}
	if got := fmt.Sprint(args); got != fmt.Sprint([]any{"app", "orders", since, until, 1}) {
		t.Fatalf("args = %s", got)
	}
	opts.LatestPerPKCandidates = true
	for _, q := range []string{
		mustBuildQuery(opts),
		mustBuildQueryForFile(opts, map[string]bool{"connection_id": true}),
		mustBuildQueryFromFiles(opts),
		mustBuildUnsorted(opts),
	} {
		assertContains(t, q, "QUALIFY ROW_NUMBER() OVER (PARTITION BY pk_values ORDER BY event_timestamp DESC, event_id DESC) <= ?"+
			" OR ROW_NUMBER() OVER (PARTITION BY pk_values ORDER BY event_id DESC) <= ?")
		assertContains(t, q, " AS bt_kf, max(")
		assertContains(t, q, " AS bt_kc FROM parquet_scan(")
	}
}

// latestPerPK2156Rows is the data of the agreement tests: per key, one of
// #2151's shapes or none, on an index a stream writes (event ids follow the
// binary log), in table "orders".
func latestPerPK2156Rows() []query.ResultRow {
	t0 := time.Date(2026, 10, 5, 12, 10, 0, 0, time.UTC)
	at := func(s int) time.Time { return t0.Add(time.Duration(s) * time.Second) }
	row := func(pk string, id uint64, pos uint64, ts time.Time) query.ResultRow {
		return latestPerPK2156Row("orders", pk, id, "binlog.000004", pos, ts)
	}
	return []query.ResultRow{
		// lock wait: B (id 2) started before A (id 1), committed after it
		row("1", 1, 100, at(2)), row("1", 2, 200, at(0)),
		// A holds the lock from an earlier change: A1, A2, B
		row("2", 3, 300, at(3)), row("2", 4, 400, at(6)), row("2", 5, 500, at(4)),
		// long UPDATE started before a quick one that committed first
		row("3", 6, 600, at(9)), row("3", 7, 700, at(7)),
		// SET TIMESTAMP an hour back, then the same session: now, past
		row("4", 8, 800, at(10)), row("4", 9, 900, at(10-3600)),
		// no shape: the orders agree
		row("5", 10, 1000, at(11)), row("5", 11, 1100, at(12)),
		// one change
		row("6", 12, 1200, at(13)),
	}
}

// latestPerPK2156FileOnlyRows is an index built with `bintrail index` only,
// in table "files", where binlog.000002 was indexed BEFORE binlog.000001 (its
// change has the lower id). Row 1: both candidates (id 22 by time, id 23 by
// id) are in binlog.000001, the latest in the binary log is id 21 in
// binlog.000002. Row 2: one file, a lock wait, which the ids place. Row 3:
// the two latest sets agree (id 26), the last position is id 24's, in the
// other file.
func latestPerPK2156FileOnlyRows() []query.ResultRow {
	t0 := time.Date(2026, 10, 5, 12, 20, 0, 0, time.UTC)
	at := func(s int) time.Time { return t0.Add(time.Duration(s) * time.Second) }
	row := func(pk string, id uint64, file string, pos uint64, s int) query.ResultRow {
		return latestPerPK2156Row("files", pk, id, file, pos, at(s))
	}
	return []query.ResultRow{
		row("1", 21, "binlog.000002", 300, 1), row("1", 22, "binlog.000001", 100, 2), row("1", 23, "binlog.000001", 200, 0),
		row("2", 27, "binlog.000001", 400, 5), row("2", 28, "binlog.000001", 500, 4),
		row("3", 24, "binlog.000002", 600, 6), row("3", 25, "binlog.000001", 700, 7), row("3", 26, "binlog.000001", 800, 8),
	}
}

func latestPerPK2156Row(table, pk string, id uint64, file string, pos uint64, ts time.Time) query.ResultRow {
	return query.ResultRow{
		EventID: id, BinlogFile: file, StartPos: pos, EndPos: pos + 10,
		EventTimestamp: ts, SchemaName: "app", TableName: table, EventType: 2,
		PKValues: pk, RowAfter: map[string]any{"id": pk, "v": fmt.Sprint(id)},
	}
}

func idsOf(rows []query.ResultRow) []uint64 {
	out := make([]uint64, len(rows))
	for i := range rows {
		out[i] = rows[i].EventID
	}
	slices.Sort(out)
	return out
}

// truth2156 is the ground truth per key: query.OrderByBinlog over ALL the
// key's changes, its latest n in binary log order when shown, by statement
// time otherwise; and whether it refused.
func truth2156(all []query.ResultRow, n int, proof func([]query.ResultRow) query.IDProof) (ids []uint64, refused map[string]bool) {
	byKey := map[string][]query.ResultRow{}
	for _, r := range all {
		byKey[r.PKValues] = append(byKey[r.PKValues], r)
	}
	refused = map[string]bool{}
	for pk, rows := range byKey {
		rows = query.MergeResults(rows, 0, "ASC")
		d := query.OrderByBinlog(rows, proof)
		refused[pk] = d.Warning() != ""
		for _, r := range rows[max(0, len(rows)-n):] {
			ids = append(ids, r.EventID)
		}
	}
	slices.Sort(ids)
	return ids, refused
}

func historyOf(all []query.ResultRow) func([]string) ([]query.ResultRow, error) {
	return func(pks []string) ([]query.ResultRow, error) {
		var out []query.ResultRow
		for _, r := range all {
			if slices.Contains(pks, r.PKValues) {
				out = append(out, r)
			}
		}
		return out, nil
	}
}

// spanOf2156 is the key span the SQL windows must carry, written out here:
// per key, the least "4-digit length + file" and the greatest "4-digit length
// + file + 20-digit position".
func spanOf2156(all []query.ResultRow) map[string][2]string {
	out := map[string][2]string{}
	for _, r := range all {
		f := fmt.Sprintf("%04d%s", len(r.BinlogFile), r.BinlogFile)
		c := f + fmt.Sprintf("%020d", r.StartPos)
		cur, ok := out[r.PKValues]
		if !ok || f < cur[0] {
			cur[0] = f
		}
		if c > cur[1] {
			cur[1] = c
		}
		out[r.PKValues] = cur
	}
	return out
}

// checkLatestPerPK2156 is the check the DuckDB unit test and the MySQL
// integration test run on a source's candidates: each carries its key's span
// as written out above, and the pick over them (reading a disagreeing key's
// history) is the ground truth, with a note for every key the truth refused
// whose answer the candidates could not settle alone.
func checkLatestPerPK2156(t *testing.T, source string, all, candidates []query.ResultRow, n int, proof func([]query.ResultRow) query.IDProof) query.LatestPerPKOrder {
	t.Helper()
	spans := spanOf2156(all)
	for _, r := range candidates {
		if want := spans[r.PKValues]; r.KeySpanFirst != want[0] || r.KeySpanLast != want[1] {
			t.Fatalf("%s n=%d: event %d carries span (%q, %q), want (%q, %q)", source, n, r.EventID, r.KeySpanFirst, r.KeySpanLast, want[0], want[1])
		}
	}
	wantIDs, refused := truth2156(all, n, proof)
	got, order, err := query.LatestPerPKInBinlog(slices.Clone(candidates), n, proof, historyOf(all))
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(idsOf(got), wantIDs) {
		t.Fatalf("%s n=%d: pick %v, ground truth %v (%+v)", source, n, idsOf(got), wantIDs, order)
	}
	t.Logf("%s n=%d: candidates %v, pick %v, truth refused %v, note %q", source, n, idsOf(candidates), idsOf(got), refused, order.Note())
	return order
}

func streamProof([]query.ResultRow) query.IDProof   { return query.IDsFollowStream }
func filesOnlyProof([]query.ResultRow) query.IDProof { return query.IDsFollowFileIndexing }

// DuckDB returns the candidates and spans the Go pick needs, and the pick
// over them is the ground truth, on a stream index and on a file-only one.
// The MySQL side is in the integration test.
func TestLatestPerPKCandidates_duckDBAgreesWithGo2156(t *testing.T) {
	for _, tc := range []struct {
		table string
		rows  []query.ResultRow
		proof func([]query.ResultRow) query.IDProof
	}{
		{"orders", latestPerPK2156Rows(), streamProof},
		{"files", latestPerPK2156FileOnlyRows(), filesOnlyProof},
	} {
		base := filepath.Join(t.TempDir(), "bintrail_id=src")
		if _, err := buffer.WriteParquet(tc.rows, filepath.Join(base, "event_date=2026-10-05", "event_hour=12", "events.parquet"), "none"); err != nil {
			t.Fatal(err)
		}
		for _, n := range []int{1, 2} {
			opts := query.Options{Schema: "app", Table: tc.table, LimitPerPK: n, LatestPerPKCandidates: true}
			cand, err := Fetch(context.Background(), opts, base)
			if err != nil {
				t.Fatalf("Fetch: %v", err)
			}
			order := checkLatestPerPK2156(t, "duckdb "+tc.table, tc.rows, cand, n, tc.proof)
			if tc.table == "files" && n == 1 && (order.Refused != 2 || order.Sorted != 1 ||
				!strings.Contains(order.Note(), "built with `bintrail index` only")) {
				t.Fatalf("file-only index: %+v, note %q: want rows 1 and 3 refused, row 2 sorted", order, order.Note())
			}
			opts.LatestPerPKCandidates = false
			plain, err := Fetch(context.Background(), opts, base)
			if err != nil {
				t.Fatalf("Fetch: %v", err)
			}
			if want := idsOf(query.LimitPerPK(query.MergeResults(slices.Clone(tc.rows), 0, "ASC"), n)); !slices.Equal(idsOf(plain), want) {
				t.Fatalf("without candidates duckdb keeps %v, the Go trim %v", idsOf(plain), want)
			}
			if len(plain) > 0 && (plain[0].KeySpanFirst != "" || plain[0].KeySpanLast != "") {
				t.Fatal("a span without candidates")
			}
		}
	}
}
