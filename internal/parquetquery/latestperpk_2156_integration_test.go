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
// window, the DuckDB window and the Go pick, on #2151's shapes. MySQL and
// DuckDB return the same candidates from the same rows; the pick over them is
// the Go pick over all the rows; and FetchMerged, with the rows split between
// the live index and an archive, returns that pick.
func TestIntegrationLatestPerPKThreePlacesAgree2156(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	all := latestPerPK2156Rows()
	insertLatestPerPK2156(t, db, all)

	base := filepath.Join(t.TempDir(), "bintrail_id=src")
	if _, err := buffer.WriteParquet(all, filepath.Join(base, "event_date=2026-10-05", "event_hour=12", "events.parquet"), "none"); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for _, n := range []int{1, 2} {
		opts := query.Options{Schema: "app", Table: "orders", LimitPerPK: n, LatestPerPKCandidates: true}
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
		checkLatestPerPK2156(t, "mysql", mysqlCand, n)

		opts.LatestPerPKCandidates = false
		plain, err := query.New(db).Fetch(ctx, opts)
		if err != nil {
			t.Fatalf("MySQL fetch: %v", err)
		}
		if want := idsOf(query.LimitPerPK(query.MergeResults(latestPerPK2156Rows(), 0, "ASC"), n)); !slices.Equal(idsOf(plain), want) {
			t.Fatalf("n=%d: without candidates MySQL keeps %v, the Go trim %v", n, idsOf(plain), want)
		}
	}

	// Split: the live index keeps the even ids, the archive the odd ones, so
	// every shape's two changes sit in different sources.
	testutil.MustExec(t, db, "DELETE FROM binlog_events WHERE event_id % 2 = 1")
	var odd []query.ResultRow
	for _, r := range all {
		if r.EventID%2 == 1 {
			odd = append(odd, r)
		}
	}
	split := filepath.Join(t.TempDir(), "bintrail_id=split")
	if _, err := buffer.WriteParquet(odd, filepath.Join(split, "event_date=2026-10-05", "event_hour=12", "events.parquet"), "none"); err != nil {
		t.Fatal(err)
	}
	// A stream writes this index: its ids follow the binary log.
	testutil.MustExec(t, db, `INSERT INTO stream_state (id, mode, last_checkpoint, server_id) VALUES (1, 'position', UTC_TIMESTAMP(), 1)`)
	for _, n := range []int{1, 2} {
		var order query.LatestPerPKOrder
		rows, _, err := query.FetchMerged(ctx, db, query.New(db), query.FetchMergedOptions{
			Opts:           query.Options{Schema: "app", Table: "orders", LimitPerPK: n},
			AllowGaps:      true,
			ArchiveFetcher: Fetch,
			SourceResolver: func(context.Context, *sql.DB) ([]string, error) { return []string{split}, nil },
			LatestInBinlog: true,
			LatestOrder:    &order,
		})
		if err != nil {
			t.Fatalf("FetchMerged: %v", err)
		}
		want, wantOrder := query.LatestPerPKInBinlog(latestPerPK2156Rows(), n, func([]query.ResultRow) query.IDProof { return query.IDsFollowStream })
		if !slices.Equal(idsOf(rows), idsOf(want)) || order != wantOrder {
			t.Fatalf("n=%d: merged %v (%+v), want %v (%+v)", n, idsOf(rows), order, idsOf(want), wantOrder)
		}
		if order.Sorted == 0 || order.Note() != "" {
			t.Fatalf("n=%d: order %+v, note %q: want the shapes taken in binary log order, no note", n, order, order.Note())
		}
	}
}
