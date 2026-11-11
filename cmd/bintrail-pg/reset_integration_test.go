//go:build integration

package main

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestClearCheckpoint_Integration covers the index half of `bintrail-pg reset` against
// a real MySQL index (#1082 semantics): discarding a real checkpoint clears the cursor
// AND stamps the discard as a permanent continuity loss in the SAME row (replacing a
// prior unacknowledged record's detail, never erasing the record); a second clear is a
// no-checkpoint clear that leaves the loss record untouched; and a missing stream_state
// table reports tableMissing rather than erroring.
func TestClearCheckpoint_Integration(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()
	db, _ := testutil.CreateTestDB(t)
	if err := indexer.CreateIndexTables(ctx, db, 4, false, nil); err != nil {
		t.Fatalf("CreateIndexTables: %v", err)
	}

	// A real PG checkpoint (LSN 0/1A2B3C4) WITH a prior loss record — the exact
	// state the old DELETE used to silently launder away. The prior loss is
	// ACKNOWLEDGED: the discard is a new loss and must withdraw that.
	if _, err := db.ExecContext(ctx, `
		INSERT INTO stream_state
			(id, mode, flavor, binlog_file, binlog_position, gtid_set,
			 events_indexed, server_id, last_checkpoint, gap_lost_at, gap_lost_detail, gap_acked_at)
		VALUES (1, 'gtid', 'postgres', '0/1A2B3C4', 27440068, '0/1A2B3C4',
			 42, 9, UTC_TIMESTAMP(), UTC_TIMESTAMP(), 'prior slot loss', UTC_TIMESTAMP())`); err != nil {
		t.Fatalf("seed checkpoint: %v", err)
	}

	res, err := clearCheckpoint(ctx, db)
	if err != nil || res.tableMissing || res.rows != 1 {
		t.Fatalf("clear real checkpoint = (%+v, %v), want rows=1", res, err)
	}
	if !strings.Contains(res.lossDetail, "0/1A2B3C4") {
		t.Errorf("loss detail should name the discarded LSN, got %q", res.lossDetail)
	}

	var (
		pos       uint64
		file      string
		gtidSet   sql.NullString
		events    int64
		gapAt     sql.NullTime
		gapDetail sql.NullString
	)
	readBack := func() {
		t.Helper()
		if err := db.QueryRowContext(ctx, `
			SELECT binlog_position, binlog_file, gtid_set, events_indexed, gap_lost_at, gap_lost_detail
			FROM stream_state WHERE id = 1`).
			Scan(&pos, &file, &gtidSet, &events, &gapAt, &gapDetail); err != nil {
			t.Fatalf("the stream_state row must SURVIVE a reset (never DELETEd): %v", err)
		}
	}
	readBack()
	if pos != 0 || file != "" || gtidSet.Valid || events != 0 {
		t.Errorf("cursor not cleared: pos=%d file=%q gtid=%v events=%d", pos, file, gtidSet, events)
	}
	if !gapAt.Valid || gapDetail.String != res.lossDetail {
		t.Errorf("discard not stamped: gap_lost_at=%v detail=%q (want %q)", gapAt, gapDetail.String, res.lossDetail)
	}
	var acked sql.NullTime
	if err := db.QueryRowContext(ctx, `SELECT gap_acked_at FROM stream_state WHERE id = 1`).Scan(&acked); err != nil {
		t.Fatal(err)
	}
	if acked.Valid {
		t.Errorf("a discard is a new loss: the earlier acknowledgement must be withdrawn, got gap_acked_at=%v", acked.Time)
	}

	// Second clear: no checkpoint left to discard — the loss record from the first
	// reset must survive verbatim (the no-op path never touches gap_lost_*).
	resetDetail := gapDetail.String
	res2, err := clearCheckpoint(ctx, db)
	if err != nil || res2.rows != 1 || res2.lossDetail != "" {
		t.Fatalf("second clear = (%+v, %v), want rows=1 and no new loss", res2, err)
	}
	readBack()
	if !gapAt.Valid || gapDetail.String != resetDetail {
		t.Errorf("no-checkpoint clear erased/altered the loss record: at=%v detail=%q", gapAt, gapDetail.String)
	}

	if _, err := db.ExecContext(ctx, "DROP TABLE stream_state"); err != nil {
		t.Fatalf("drop stream_state: %v", err)
	}
	res3, err := clearCheckpoint(ctx, db)
	if err != nil || !res3.tableMissing {
		t.Fatalf("clear with no table = (%+v, %v), want tableMissing", res3, err)
	}
}
