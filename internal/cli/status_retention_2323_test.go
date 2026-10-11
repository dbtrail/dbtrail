package cli

import (
	"context"
	"database/sql"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"

	"github.com/dbtrail/dbtrail/internal/indexer"
)

// statusRetention is the wiring between the rotation package's two answers
// (the window, and whether the loop would refuse to drop on it) and the
// report: without it status names a window nothing is being dropped on.
func TestStatusRetention_2323(t *testing.T) {
	index := func(t *testing.T, ages ...time.Duration) (*sql.DB, sqlmock.Sqlmock) {
		t.Helper()
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { db.Close() })
		mock.ExpectQuery("rotation_policy").WillReturnError(sql.ErrNoRows)
		rows := sqlmock.NewRows([]string{"PARTITION_NAME", "PARTITION_DESCRIPTION", "PARTITION_ORDINAL_POSITION"})
		for i, age := range ages {
			rows.AddRow(indexer.PartitionName(time.Now().UTC().Add(-age)), "0", i+1)
		}
		mock.ExpectQuery("information_schema.PARTITIONS").WillReturnRows(rows)
		return db, mock
	}

	t.Run("history 100 days deep, no record: needs an explicit retention", func(t *testing.T) {
		db, mock := index(t, 100*24*time.Hour, time.Hour)
		got := statusRetention(context.Background(), db, "binlog_index")
		if got == nil || got.Raw != "30d" || got.Basis != "legacy" || !got.NeedsExplicitRetain || got.OldestHistory == "" {
			t.Fatalf("retention = %+v, want the legacy 30d held back by the guard", got)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("recent history: the window, nothing else", func(t *testing.T) {
		db, mock := index(t, 10*24*time.Hour)
		got := statusRetention(context.Background(), db, "binlog_index")
		if got == nil || got.Raw != "30d" || got.NeedsExplicitRetain || got.OldestHistory != "" {
			t.Fatalf("retention = %+v, want the plain legacy 30d", got)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("the guard must have been asked: %v", err)
		}
	})

	t.Run("the listing fails: the window is still reported, not held back", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		mock.ExpectQuery("rotation_policy").WillReturnError(sql.ErrNoRows)
		mock.ExpectQuery("information_schema.PARTITIONS").WillReturnError(sql.ErrConnDone)
		got := statusRetention(context.Background(), db, "binlog_index")
		if got == nil || got.Raw != "30d" || got.NeedsExplicitRetain {
			t.Fatalf("retention = %+v, want the legacy 30d", got)
		}
	})
}
