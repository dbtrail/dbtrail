package parquetquery

import (
	"context"
	"fmt"
	"path/filepath"
	"slices"
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
	}
}

// latestPerPK2156Rows is the data of the agreement test: per key, one of
// #2151's shapes or none. Event ids follow the binary log (a stream index).
func latestPerPK2156Rows() []query.ResultRow {
	t0 := time.Date(2026, 10, 5, 12, 10, 0, 0, time.UTC)
	at := func(s int) time.Time { return t0.Add(time.Duration(s) * time.Second) }
	row := func(pk string, id uint64, pos uint64, ts time.Time) query.ResultRow {
		return query.ResultRow{
			EventID: id, BinlogFile: "binlog.000004", StartPos: pos, EndPos: pos + 10,
			EventTimestamp: ts, SchemaName: "app", TableName: "orders", EventType: 2,
			PKValues: pk, RowAfter: map[string]any{"id": pk, "v": fmt.Sprint(id)},
		}
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

func idsOf(rows []query.ResultRow) []uint64 {
	out := make([]uint64, len(rows))
	for i := range rows {
		out[i] = rows[i].EventID
	}
	slices.Sort(out)
	return out
}

// checkLatestPerPK2156 is the check both the unit test below (DuckDB against
// Go) and the MySQL integration test of this package run: the pick over the
// candidates a source returned is the Go pick over ALL the rows.
func checkLatestPerPK2156(t *testing.T, source string, candidates []query.ResultRow, n int) {
	t.Helper()
	all := latestPerPK2156Rows()
	proof := func([]query.ResultRow) query.IDProof { return query.IDsFollowStream }
	want, wantOrder := query.LatestPerPKInBinlog(slices.Clone(all), n, proof)
	got, gotOrder := query.LatestPerPKInBinlog(slices.Clone(candidates), n, proof)
	if !slices.Equal(idsOf(got), idsOf(want)) || gotOrder != wantOrder {
		t.Fatalf("%s n=%d: pick over its candidates %v (%+v), over all rows %v (%+v)", source, n, idsOf(got), gotOrder, idsOf(want), wantOrder)
	}
	byTime := query.LimitPerPK(query.MergeResults(slices.Clone(all), 0, "ASC"), n)
	t.Logf("%s n=%d: binary log %v, statement time %v, candidates %v", source, n, idsOf(want), idsOf(byTime), idsOf(candidates))
}

// DuckDB returns the candidates the Go pick needs, and the pick over them is
// the pick over all the rows. The MySQL side is in the integration test.
func TestLatestPerPKCandidates_duckDBAgreesWithGo2156(t *testing.T) {
	base := filepath.Join(t.TempDir(), "bintrail_id=src")
	file := filepath.Join(base, "event_date=2026-10-05", "event_hour=12", "events.parquet")
	if _, err := buffer.WriteParquet(latestPerPK2156Rows(), file, "none"); err != nil {
		t.Fatal(err)
	}
	for _, n := range []int{1, 2} {
		opts := query.Options{Schema: "app", Table: "orders", LimitPerPK: n, LatestPerPKCandidates: true}
		cand, err := Fetch(context.Background(), opts, base)
		if err != nil {
			t.Fatalf("Fetch: %v", err)
		}
		checkLatestPerPK2156(t, "duckdb", cand, n)
		if n == 1 {
			// The exact candidates: per key the latest by time and by id.
			want := []uint64{1, 2, 4, 5, 6, 7, 8, 9, 11, 12}
			if got := idsOf(cand); !slices.Equal(got, want) {
				t.Fatalf("duckdb candidates %v, want %v", got, want)
			}
		}
		opts.LatestPerPKCandidates = false
		plain, err := Fetch(context.Background(), opts, base)
		if err != nil {
			t.Fatalf("Fetch: %v", err)
		}
		if want := idsOf(query.LimitPerPK(query.MergeResults(latestPerPK2156Rows(), 0, "ASC"), n)); !slices.Equal(idsOf(plain), want) {
			t.Fatalf("without candidates duckdb keeps %v, the Go trim %v", idsOf(plain), want)
		}
	}
}
