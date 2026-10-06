//go:build integration

package parquetquery

import (
	"context"
	"database/sql"
	"encoding/json"
	"path/filepath"
	"slices"
	"testing"

	"github.com/dbtrail/dbtrail/internal/buffer"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

func insertLatestPerPK2156(t *testing.T, db *sql.DB, rows []query.ResultRow) {
	t.Helper()
	for _, r := range rows {
		after, err := json.Marshal(r.RowAfter)
		if err != nil {
			t.Fatal(err)
		}
		testutil.MustExec(t, db, `INSERT INTO binlog_events
			(event_id, binlog_file, start_pos, end_pos, event_timestamp, schema_name, table_name, event_type, pk_values, row_after)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			r.EventID, r.BinlogFile, r.StartPos, r.EndPos, r.EventTimestamp, r.SchemaName, r.TableName, r.EventType, r.PKValues, after)
	}
}

// The three places that pick "the latest N per key" agree (#2156): the MySQL
// window, the DuckDB window and the Go pick, on #2151's shapes, on an index a
// stream writes and on one built with `bintrail index` only. MySQL and DuckDB
// return the same candidates with the same key spans; the pick over them is
// the ground truth (query.OrderByBinlog over every change of the key); and
// FetchMerged, with the rows split between the live index and an archive,
// returns it, reading a disagreeing key's history from both.
func TestIntegrationLatestPerPKThreePlacesAgree2156(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name  string
		table string
		rows  []query.ResultRow
		proof func([]query.ResultRow) query.IDProof
		// what FetchMerged must report for n = 1
		sorted, refused int
	}{
		{"stream", "orders", latestPerPK2156Rows(), streamProof, 4, 0},
		{"files only", "files", latestPerPK2156FileOnlyRows(), filesOnlyProof, 1, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, _ := testutil.CreateTestDB(t)
			testutil.InitIndexTables(t, db)
			insertLatestPerPK2156(t, db, tc.rows)
			base := filepath.Join(t.TempDir(), "bintrail_id=src")
			if _, err := buffer.WriteParquet(tc.rows, filepath.Join(base, "event_date=2026-10-05", "event_hour=12", "events.parquet"), "none"); err != nil {
				t.Fatal(err)
			}
			for _, n := range []int{1, 2} {
				opts := query.Options{Schema: "app", Table: tc.table, LimitPerPK: n, LatestPerPKCandidates: true}
				mysqlCand, err := query.New(db).Fetch(ctx, opts)
				if err != nil {
					t.Fatalf("MySQL fetch: %v", err)
				}
				duckCand, err := Fetch(ctx, opts, base)
				if err != nil {
					t.Fatalf("DuckDB fetch: %v", err)
				}
				if !slices.Equal(idsOf(mysqlCand), idsOf(duckCand)) {
					t.Fatalf("n=%d: MySQL candidates %v, DuckDB candidates %v", n, idsOf(mysqlCand), idsOf(duckCand))
				}
				checkLatestPerPK2156(t, "mysql", tc.rows, mysqlCand, n, tc.proof)
				checkLatestPerPK2156(t, "duckdb", tc.rows, duckCand, n, tc.proof)

				opts.LatestPerPKCandidates = false
				plain, err := query.New(db).Fetch(ctx, opts)
				if err != nil {
					t.Fatalf("MySQL fetch: %v", err)
				}
				if want := idsOf(query.LimitPerPK(query.MergeResults(slices.Clone(tc.rows), 0, "ASC"), n)); !slices.Equal(idsOf(plain), want) {
					t.Fatalf("n=%d: without candidates MySQL keeps %v, the Go trim %v", n, idsOf(plain), want)
				}
			}

			// Split: the live index keeps the even ids, the archive the odd
			// ones, so the shapes' changes sit in different sources.
			testutil.MustExec(t, db, "DELETE FROM binlog_events WHERE event_id % 2 = 1")
			var odd []query.ResultRow
			for _, r := range tc.rows {
				if r.EventID%2 == 1 {
					odd = append(odd, r)
				}
			}
			split := filepath.Join(t.TempDir(), "bintrail_id=split")
			if _, err := buffer.WriteParquet(odd, filepath.Join(split, "event_date=2026-10-05", "event_hour=12", "events.parquet"), "none"); err != nil {
				t.Fatal(err)
			}
			if tc.proof(nil) == query.IDsFollowStream {
				testutil.MustExec(t, db, `INSERT INTO stream_state (id, mode, last_checkpoint, server_id) VALUES (1, 'position', UTC_TIMESTAMP(), 1)`)
			}
			for _, n := range []int{1, 2} {
				var order query.LatestPerPKOrder
				rows, _, err := query.FetchMerged(ctx, db, query.New(db), query.FetchMergedOptions{
					Opts:           query.Options{Schema: "app", Table: tc.table, LimitPerPK: n},
					AllowGaps:      true,
					ArchiveFetcher: Fetch,
					SourceResolver: func(context.Context, *sql.DB) ([]string, error) { return []string{split}, nil },
					LatestInBinlog: true,
					LatestOrder:    &order,
				})
				if err != nil {
					t.Fatalf("FetchMerged: %v", err)
				}
				want, _ := truth2156(tc.rows, n, tc.proof)
				if !slices.Equal(idsOf(rows), want) {
					t.Fatalf("n=%d: merged %v (%+v), ground truth %v", n, idsOf(rows), order, want)
				}
				if n == 1 && (order.Sorted != tc.sorted || order.Refused != tc.refused) {
					t.Fatalf("n=%d: order %+v, note %q: want %d sorted, %d refused", n, order, order.Note(), tc.sorted, tc.refused)
				}
				t.Logf("n=%d: %v, note %q", n, idsOf(rows), order.Note())
			}
		})
	}
}
