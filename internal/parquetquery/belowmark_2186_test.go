package parquetquery

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// writeEvents writes an archive-shaped Parquet file holding the given rows:
// (event_id, binlog_file, end_pos, minute past 10:00 on 2026-10-01, table).
func writeEvents(t *testing.T, rows [][5]any) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "events.parquet")
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var vals []string
	for _, r := range rows {
		vals = append(vals, fmt.Sprintf("(%d::UBIGINT, '%s', %d::UBIGINT, %d::UBIGINT, TIMESTAMP '2026-10-01 10:00:00' + INTERVAL %d MINUTE, 'shop', '%s')",
			r[0], r[1], r[2].(int)-50, r[2], r[3], r[4]))
	}
	q := "COPY (SELECT * FROM (VALUES " + strings.Join(vals, ",") +
		") t(event_id, binlog_file, start_pos, end_pos, event_timestamp, schema_name, table_name)) TO '" + path + "' (FORMAT PARQUET)"
	if _, err := db.Exec(q); err != nil {
		t.Fatal(err)
	}
	return path
}

// #2186: the archive twin of the live check's question. The mark is event
// 100 ending at binlog.000007:500.
func TestFirstBelowMark_2186(t *testing.T) {
	q := BelowMark{Schema: "shop", Table: "orders", AfterID: 100, MarkFile: "binlog.000007", MarkEnd: 500, Base: "binlog"}
	at := func(min int) time.Time { return time.Date(2026, 10, 1, 10, min, 0, 0, time.UTC) }
	for _, tc := range []struct {
		name     string
		rows     [][5]any
		from, to time.Time
		want     uint64 // 0: none
	}{
		{name: "steady state: every change after the mark sorts after it",
			rows: [][5]any{{99, "binlog.000007", 400, 1, "orders"}, {101, "binlog.000007", 600, 2, "orders"}, {102, "binlog.000008", 100, 3, "orders"}}},
		{name: "a later batch of the mark's own event ends where the mark does",
			rows: [][5]any{{101, "binlog.000007", 500, 1, "orders"}}},
		{name: "rows indexed before the mark may sort anywhere",
			rows: [][5]any{{50, "binlog.000001", 100, 1, "orders"}}},
		{name: "the numbering started over",
			rows: [][5]any{{101, "binlog.000007", 600, 1, "orders"}, {102, "binlog.000001", 300, 2, "orders"}}, want: 102},
		{name: "an earlier offset in the mark's own file",
			rows: [][5]any{{101, "binlog.000007", 450, 1, "orders"}}, want: 101},
		{name: "a shorter file name sorts first",
			rows: [][5]any{{101, "binlog.99", 9000, 1, "orders"}}, want: 101},
		{name: "a longer file name sorts after, but another base name is not comparable",
			rows: [][5]any{{101, "mysql-bin.000001", 100, 1, "orders"}}, want: 101},
		{name: "the rollover past .999999 sorts after",
			rows: [][5]any{{101, "binlog.1000000", 100, 1, "orders"}}},
		{name: "another table only",
			rows: [][5]any{{101, "binlog.000001", 300, 1, "customers"}}},
		{name: "outside the read's window",
			rows: [][5]any{{101, "binlog.000001", 300, 40, "orders"}}, from: at(0), to: at(30)},
		{name: "inside the read's window",
			rows: [][5]any{{101, "binlog.000001", 300, 20, "orders"}}, from: at(0), to: at(30), want: 101},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := writeEvents(t, tc.rows)
			qq := q
			qq.From, qq.Until = tc.from, tc.to
			row, found, err := FirstBelowMark(context.Background(), f, qq)
			if err != nil {
				t.Fatal(err)
			}
			if found != (tc.want != 0) || (found && row.EventID != tc.want) {
				t.Fatalf("= %+v, %v; want event %d", row, found, tc.want)
			}
		})
	}
	t.Run("no base name: no base-name clause", func(t *testing.T) {
		f := writeEvents(t, [][5]any{{101, "other-name.1", 900, 1, "orders"}})
		qq := q
		qq.MarkFile, qq.Base = "binlog", ""
		if _, found, err := FirstBelowMark(context.Background(), f, qq); err != nil || found {
			t.Fatalf("= %v, %v; want none", found, err)
		}
	})
	t.Run("a file that does not read is an error", func(t *testing.T) {
		if _, _, err := FirstBelowMark(context.Background(), filepath.Join(t.TempDir(), "missing.parquet"), q); err == nil {
			t.Fatal("no error")
		}
	})
}
