//go:build integration

package streamrun

import (
	"context"
	"database/sql"
	"testing"

	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// #2606: a capture that starts with no checkpoint records, before its first
// one, the source coordinate it holds every event from. In GTID mode that is
// read after the start set, so it is never before the source's position when
// the run began (a coordinate before it would vouch for transactions the set
// already held, which never stream), and not after the position once the run
// has checkpointed.
//
// A row is written to the source right before that read: the recorded
// coordinate must be past it. The dedup anchor, read before the start set,
// is not, so a start that reused it fails here.
func runFreshStartRecordsCaptureFrom(t *testing.T, src dupSource) {
	t.Helper()
	ctx := context.Background()
	indexDB, indexName := testutil.CreateTestDB(t)
	if err := indexer.CreateIndexTables(ctx, indexDB, 3, false, nil); err != nil {
		t.Fatal(err)
	}
	pos := func(db *sql.DB) query.BinlogPos {
		t.Helper()
		f, p, err := config.CurrentBinlogPosition(db)
		if err != nil {
			t.Fatal(err)
		}
		return query.BinlogPos{File: f, Pos: uint64(p)}
	}
	before := pos(src.db)
	var written query.BinlogPos
	real := captureFromPosition
	t.Cleanup(func() { captureFromPosition = real })
	captureFromPosition = func(db *sql.DB) (string, uint32, error) {
		testutil.MustExec(t, src.db, "INSERT INTO orders (amount) VALUES (1.00)")
		written = pos(src.db)
		return real(db)
	}
	cfg := src.config(indexName)
	cfg.Checkpoint = 3600
	if err := runOneUntil(t, cfg, false, nil, func() bool {
		st, err := loadStreamState(indexDB)
		if err != nil {
			t.Fatalf("poll stream_state: %v", err)
		}
		return st != nil
	}); err != nil {
		t.Fatalf("One: %v", err)
	}
	after := pos(src.db)
	h, err := indexer.ReadPartitionHistory(ctx, indexDB, indexName)
	if err != nil {
		t.Fatal(err)
	}
	from := query.BinlogPos{File: h.CaptureFromFile, Pos: h.CaptureFromPos}
	if from.File == "" {
		t.Fatalf("no capture start recorded: %+v", h)
	}
	if written.File == "" {
		t.Fatal("the capture start was not read through captureFromPosition")
	}
	if !before.AtOrBefore(from) || !from.AtOrBefore(after) {
		t.Errorf("capture start %v, want between %v (before the run) and %v (after it)", from, before, after)
	}
	if !written.AtOrBefore(from) {
		t.Errorf("capture start %v is before %v, the row written just before it was read: a coordinate read before the start set", from, written)
	}
}

func TestIntegrationFreshStartRecordsCaptureFrom_2606(t *testing.T) {
	runFreshStartRecordsCaptureFrom(t, mysqlGTIDDupSource(t, 99970))
}

func TestOne_MariaDB_freshStartRecordsCaptureFrom_2606(t *testing.T) {
	runFreshStartRecordsCaptureFrom(t, mariadbDupSource(t, 99971))
}
