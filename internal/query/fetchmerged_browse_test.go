package query

import (
	"context"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
)

// The default browse — newest-first, no since/until — through the REAL
// FetchMergedFull stack (#1353): resolveMergeSources must build the browse
// plan from partition metadata + scoped archive coverage, and a filled DESC
// page must then skip every archive source AND say so (archivesElided). A
// short page must fail open to the merged read and NOT claim elision.
//
// All timestamps are fixed (the browse plan reads no clock), so this cannot
// flake across an hour boundary.
func TestFetchMergedFull_defaultBrowse(t *testing.T) {
	wideCols := []string{
		"event_id", "binlog_file", "start_pos", "end_pos", "event_timestamp",
		"gtid", "connection_id", "schema_name", "table_name", "event_type", "pk_values",
		"changed_columns", "row_before", "row_after", "schema_version", "query_text", "query_hash",
		"commit_ts_us",
	}
	liveHour := time.Date(2026, 6, 10, 12, 0, 0, 0, time.UTC)
	liveRows := func(n int) *sqlmock.Rows {
		rows := sqlmock.NewRows(wideCols)
		for i := range n {
			rows.AddRow(uint64(100-i), "mysql-bin.000001", 100, 200, liveHour.Add(-time.Duration(i)*time.Minute),
				nil, nil, "shop", "orders", 2, "1",
				nil, nil, nil, 1, nil, nil,
				nil)
		}
		return rows
	}
	// Arms the prologue every default browse runs: source discovery (one S3
	// source, so no filesystem is touched), then the browse plan's partition
	// and scoped-coverage reads. The archived hour sits below the live floor —
	// the layout rotation always produces.
	armPrologue := func(mock sqlmock.Sqlmock) {
		mock.ExpectQuery("SELECT bintrail_id").WillReturnRows(
			sqlmock.NewRows([]string{"bintrail_id", "sample_local", "sample_bucket", "sample_key"}).
				AddRow("x", nil, "bkt", "archives/bintrail_id=x/date=2026-06-09/hour=00/events.parquet"))
		mock.ExpectQuery("information_schema.PARTITIONS").WillReturnRows(
			sqlmock.NewRows([]string{"PARTITION_NAME"}).
				AddRow("p_2026061010").AddRow("p_2026061011").AddRow("p_2026061012"))
		mock.ExpectQuery("SELECT partition_name, min_event_ts").WithArgs("x").WillReturnRows(
			sqlmock.NewRows([]string{"partition_name", "min_event_ts", "max_event_ts"}).
				AddRow("p_2026060900", nil, nil))
	}

	const limit = 3
	archivedRow := ResultRow{EventID: 7, EventTimestamp: time.Date(2026, 6, 9, 0, 30, 0, 0, time.UTC),
		SchemaName: "shop", TableName: "orders", EventType: 2, PKValues: "9"}

	t.Run("filled page skips archives and reports the elision", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		armPrologue(mock)
		expectNoEventPartitions(mock)
		mock.ExpectQuery("FROM binlog_events").WillReturnRows(liveRows(limit))

		var archiveCalls int
		rows, plan, skipped, _, elided, err := FetchMergedFull(context.Background(), db, New(db), FetchMergedOptions{
			Opts:      Options{Limit: limit, Order: "DESC"},
			DBName:    "testdb",
			AllowGaps: true,
			ArchiveFetcher: func(context.Context, Options, string) ([]ResultRow, error) {
				archiveCalls++
				return []ResultRow{archivedRow}, nil
			},
		})
		if err != nil {
			t.Fatalf("FetchMergedFull: %v", err)
		}
		if archiveCalls != 0 {
			t.Errorf("archive fetcher called %d time(s); the browse plan must let a filled page skip S3", archiveCalls)
		}
		if !elided {
			t.Error("archivesElided = false; the skip must be reportable in the response, not silent")
		}
		if plan == nil || len(plan.MySQLRanges) != 1 {
			t.Errorf("browse plan not built: %+v", plan)
		}
		if len(rows) != limit || len(skipped) != 0 {
			t.Errorf("rows=%d skipped=%v, want %d rows and no skips", len(rows), skipped, limit)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("unmet sqlmock expectations: %v", err)
		}
	})

	t.Run("short page reads archives and does not claim elision", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		armPrologue(mock)
		expectNoEventPartitions(mock)
		mock.ExpectQuery("FROM binlog_events").WillReturnRows(liveRows(limit - 1))

		var archiveCalls int
		rows, _, _, _, elided, err := FetchMergedFull(context.Background(), db, New(db), FetchMergedOptions{
			Opts:      Options{Limit: limit, Order: "DESC"},
			DBName:    "testdb",
			AllowGaps: true,
			ArchiveFetcher: func(context.Context, Options, string) ([]ResultRow, error) {
				archiveCalls++
				return []ResultRow{archivedRow}, nil
			},
		})
		if err != nil {
			t.Fatalf("FetchMergedFull: %v", err)
		}
		if archiveCalls != 1 {
			t.Errorf("archive fetcher called %d time(s), want 1 — a short live page means the archives genuinely extend it", archiveCalls)
		}
		if elided {
			t.Error("archivesElided = true on a page that READ the archives")
		}
		if len(rows) != limit {
			t.Fatalf("rows = %d, want %d (live %d + archived 1)", len(rows), limit, limit-1)
		}
		if rows[limit-1].EventID != archivedRow.EventID {
			t.Errorf("the archived row did not survive the merge: %+v", rows[limit-1])
		}
	})

	t.Run("NoArchive never claims elision", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		// Exclusion is the caller's decision (profiled session, --no-archive):
		// no discovery, no browse plan, one live fetch — and the fetch must
		// not report "skipped because they could not matter" when the truth is
		// "excluded by policy" (#1311's distinction).
		expectNoEventPartitions(mock)
		mock.ExpectQuery("FROM binlog_events").WillReturnRows(liveRows(limit))
		_, _, _, _, elided, err := FetchMergedFull(context.Background(), db, New(db), FetchMergedOptions{
			Opts:      Options{Limit: limit, Order: "DESC"},
			DBName:    "testdb",
			NoArchive: true,
			AllowGaps: true,
		})
		if err != nil {
			t.Fatalf("FetchMergedFull: %v", err)
		}
		if elided {
			t.Error("archivesElided = true under NoArchive; exclusion and elision are different facts")
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("unmet sqlmock expectations: %v", err)
		}
	})
}
