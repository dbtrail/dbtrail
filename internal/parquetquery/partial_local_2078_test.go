package parquetquery

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/dbtrail/dbtrail/internal/buffer"
	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/query"
)

// #2078 at the reader: the shape `watch` with an S3 location leaves behind
// while one hour's upload is unconfirmed. Hour 03 was uploaded and its local
// file removed, hour 04 is still on local disk, and both are registered with
// a local path AND an S3 location. The reader cannot notice the missing
// hour (it globs the base and gets one row, no error), which is why the
// resolver must not hand it that base.
func TestLeftoverLocalHourDoesNotShadowS3(t *testing.T) {
	base := filepath.Join(t.TempDir(), "bintrail_id=src")
	hour03 := filepath.Join(base, "event_date=2026-05-01", "event_hour=03", "events.parquet")
	hour04 := filepath.Join(base, "event_date=2026-05-01", "event_hour=04", "events.parquet")
	if err := os.MkdirAll(filepath.Dir(hour04), 0o755); err != nil {
		t.Fatal(err)
	}
	row := query.ResultRow{
		EventID: 2, BinlogFile: "binlog.000001", StartPos: 4, EndPos: 8,
		EventTimestamp: time.Date(2026, 5, 1, 4, 10, 0, 0, time.UTC),
		SchemaName:     "shop", TableName: "orders", EventType: event.EventInsert,
		PKValues: "2", RowAfter: map[string]any{"id": 2},
	}
	if _, err := buffer.WriteParquet([]query.ResultRow{row}, hour04, "none"); err != nil {
		t.Fatal(err)
	}

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	// MIN(local_path) is the OLDEST hour's path: the pruned one.
	const s3Key = "arch/bintrail_id=src/event_date=2026-05-01/event_hour=0%d/events.parquet"
	mock.ExpectQuery(`MIN\(local_path\)`).WillReturnRows(
		sqlmock.NewRows([]string{"bintrail_id", "sample_local", "sample_bucket", "sample_key"}).
			AddRow("src", hour03, "bkt", fmt.Sprintf(s3Key, 3)))
	mock.ExpectQuery(`SELECT local_path, s3_bucket, s3_key FROM archive_state WHERE bintrail_id = \?`).WithArgs("src").
		WillReturnRows(sqlmock.NewRows([]string{"local_path", "s3_bucket", "s3_key"}).
			AddRow(hour03, "bkt", fmt.Sprintf(s3Key, 3)).
			AddRow(hour04, "bkt", fmt.Sprintf(s3Key, 4)))

	sources, err := query.ResolveArchiveSources(context.Background(), db)
	if err != nil {
		t.Fatal(err)
	}
	if len(sources) != 1 || sources[0] != "s3://bkt/arch/bintrail_id=src" {
		t.Errorf("sources = %v, want the S3 copy: the local base holds one hour of two", sources)
	}
	// Why the resolver has to be the one that knows: handed the local base,
	// the reader returns the leftover hour and no error.
	rows, err := Fetch(context.Background(), query.Options{Limit: 100}, base)
	if err != nil || len(rows) != 1 {
		t.Errorf("reading the partial local base: %d row(s), err=%v; this test assumes that read succeeds with the one leftover row", len(rows), err)
	}
}
