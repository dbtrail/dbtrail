//go:build integration

package pgstreamrun

import (
	"context"
	"database/sql"
	"log/slog"
	"testing"

	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/status"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestPersistSlotLostPG pins the #532 upsert contract (white-box, MySQL index only):
//   - with NO stream_state row (a slot detected lost before the first checkpoint), the
//     stamp must SEED a complete row — a bare UPDATE would match zero rows and record
//     nothing (the false-negative the CRITICAL review caught);
//   - with an existing checkpoint row, the stamp must set gap_lost_* WITHOUT clobbering
//     the saved position/checkpoint.
func TestPersistSlotLostPG(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()
	indexDB, _ := testutil.CreateTestDB(t)
	if err := indexer.CreateIndexTables(ctx, indexDB, 4, false, nil); err != nil {
		t.Fatalf("CreateIndexTables: %v", err)
	}
	logger := slog.New(slog.DiscardHandler)

	// (1) No row yet → the stamp must seed one.
	persistSlotLostPG(indexDB, 51, "replication slot invalidated (wal_status=lost)", logger)

	var gapAt sql.NullTime
	var gapDetail sql.NullString
	var serverID uint32
	var mode string
	if err := indexDB.QueryRowContext(ctx,
		"SELECT mode, server_id, gap_lost_at, gap_lost_detail FROM stream_state WHERE id=1").
		Scan(&mode, &serverID, &gapAt, &gapDetail); err != nil {
		t.Fatalf("no row was seeded by persistSlotLostPG (the CRITICAL no-op): %v", err)
	}
	if !gapAt.Valid || gapDetail.String != "replication slot invalidated (wal_status=lost)" {
		t.Errorf("seeded row missing gap_lost: at=%v detail=%q", gapAt, gapDetail.String)
	}
	if serverID != 51 || mode != "gtid" {
		t.Errorf("seeded row has wrong NOT NULL fields: server_id=%d mode=%q", serverID, mode)
	}

	// (2) Existing checkpoint row → the stamp must preserve the position/checkpoint and
	// only update gap_lost_*. Write a real checkpoint first, date the loss from (1) in
	// the past and acknowledge it NOW: the second stamp is a NEW loss, so it must move
	// gap_lost_at forward (an old fixed time is what makes that visible: a stamp that
	// left the time alone would still read "set") and withdraw the acknowledgement in
	// the same statement. Left in place, that acknowledgement would be newer than or
	// equal to the new loss and the new loss would read as acknowledged.
	if _, err := indexDB.ExecContext(ctx,
		`UPDATE stream_state SET binlog_position=999, binlog_file='5/ABC',
			gap_lost_at='2020-01-02 03:04:05', gap_acked_at=UTC_TIMESTAMP() WHERE id=1`); err != nil {
		t.Fatalf("seed checkpoint: %v", err)
	}
	persistSlotLostPG(indexDB, 51, "lost again", logger)

	var pos uint64
	var file string
	if err := indexDB.QueryRowContext(ctx,
		"SELECT binlog_position, binlog_file, gap_lost_detail FROM stream_state WHERE id=1").
		Scan(&pos, &file, &gapDetail); err != nil {
		t.Fatalf("read back: %v", err)
	}
	if pos != 999 || file != "5/ABC" {
		t.Errorf("stamp clobbered the checkpoint position: pos=%d file=%q (want 999 / 5/ABC)", pos, file)
	}
	if gapDetail.String != "lost again" {
		t.Errorf("gap_lost_detail not updated on the existing row: %q", gapDetail.String)
	}
	var restamped, ackSet bool
	if err := indexDB.QueryRowContext(ctx,
		"SELECT gap_lost_at > '2020-01-02 03:04:05', gap_acked_at IS NOT NULL FROM stream_state WHERE id=1").
		Scan(&restamped, &ackSet); err != nil {
		t.Fatalf("read back the acknowledgement: %v", err)
	}
	if !restamped {
		t.Error("the stamp on an existing row must move gap_lost_at to now; it still reads the old time")
	}
	if ackSet {
		t.Error("a new loss must not be born acknowledged: gap_acked_at survived the stamp")
	}
	// Through the reader every surface uses: on record, and not acknowledged.
	if st, err := status.LoadStreamState(ctx, indexDB); err != nil || st == nil || !st.GapUnacknowledged() {
		t.Errorf("after the second stamp the loss must read as unacknowledged: %+v, %v", st, err)
	}
}

// TestSaveSourceHealth pins the #599 upsert contract (white-box, MySQL index only),
// mirroring TestPersistSlotLostPG: a health write BEFORE the first checkpoint must SEED a
// complete row (the slot exists from startup, so the daemon polls health before any
// commit — a bare UPDATE would match zero rows and the console would show no health on an
// idle stream), and a write against an existing checkpoint must set source_health WITHOUT
// clobbering the position/checkpoint columns saveCheckpointPG owns. JSON is read back via
// a path extraction because MySQL re-serializes JSON column values.
func TestSaveSourceHealth(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()
	indexDB, _ := testutil.CreateTestDB(t)
	if err := indexer.CreateIndexTables(ctx, indexDB, 4, false, nil); err != nil {
		t.Fatalf("CreateIndexTables: %v", err)
	}
	logger := slog.New(slog.DiscardHandler)

	// (1) No row yet → the write must seed one.
	saveSourceHealth(indexDB, 77, []byte(`{"wal_status":"reserved"}`), logger)

	var mode string
	var serverID uint32
	var ws sql.NullString
	if err := indexDB.QueryRowContext(ctx,
		`SELECT mode, server_id, source_health->>'$.wal_status' FROM stream_state WHERE id=1`).
		Scan(&mode, &serverID, &ws); err != nil {
		t.Fatalf("no row was seeded by saveSourceHealth (the no-op false-negative): %v", err)
	}
	if !ws.Valid || ws.String != "reserved" {
		t.Errorf("seeded row missing source_health.wal_status: %v", ws)
	}
	if serverID != 77 || mode != "gtid" {
		t.Errorf("seeded row has wrong NOT NULL fields: server_id=%d mode=%q", serverID, mode)
	}

	// (2) Existing checkpoint row → the write must preserve the position/file and update
	// ONLY source_health.
	if _, err := indexDB.ExecContext(ctx,
		`UPDATE stream_state SET binlog_position=999, binlog_file='5/ABC' WHERE id=1`); err != nil {
		t.Fatalf("seed checkpoint: %v", err)
	}
	saveSourceHealth(indexDB, 77, []byte(`{"wal_status":"lost"}`), logger)

	var pos uint64
	var file string
	if err := indexDB.QueryRowContext(ctx,
		`SELECT binlog_position, binlog_file, source_health->>'$.wal_status' FROM stream_state WHERE id=1`).
		Scan(&pos, &file, &ws); err != nil {
		t.Fatalf("read back: %v", err)
	}
	if pos != 999 || file != "5/ABC" {
		t.Errorf("source-health write clobbered the checkpoint position: pos=%d file=%q (want 999 / 5/ABC)", pos, file)
	}
	if ws.String != "lost" {
		t.Errorf("source_health not updated on the existing row: %q", ws.String)
	}
}
