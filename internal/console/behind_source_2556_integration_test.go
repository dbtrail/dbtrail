//go:build integration

package console

import (
	"context"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/testutil"
)

// #2556 against a real index: the router reads capture's stamp on the
// index's clock, the newest event id and time, and the status document says
// the last answer's distance to the source. An index without the column yet
// (an older capture last wrote it) reads as "no stamp", never as an error.
func TestIntegrationRouterBehindSource_2556(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	srv, err := New(Config{DB: db, DBName: dbName, Listen: "127.0.0.1:8090", Token: intToken, NoArchive: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.cm.CloseAll)
	id := srv.cm.defaultID()

	// Two events, the source idle for an hour since the second; capture
	// caught up 7 seconds ago.
	second := time.Now().UTC().Add(-time.Hour).Truncate(time.Second)
	first := second.Add(-5 * time.Minute)
	testutil.MustExec(t, db, `INSERT INTO binlog_events
		(binlog_file, start_pos, end_pos, event_timestamp, schema_name, table_name, event_type, pk_values, row_after)
		VALUES ('bin.000001', 100, 200, ?, 'shop', 'orders', 1, '1', '{"id":1}'),
		       ('bin.000001', 200, 300, ?, 'shop', 'orders', 1, '2', '{"id":2}')`, first, second)
	var firstID, secondID uint64
	if err := db.QueryRow(`SELECT MIN(event_id), MAX(event_id) FROM binlog_events`).Scan(&firstID, &secondID); err != nil {
		t.Fatal(err)
	}
	testutil.MustExec(t, db, `INSERT INTO stream_state
		(id, mode, binlog_file, binlog_position, last_event_time, last_checkpoint, server_id, caught_up_at)
		VALUES (1, 'position', 'bin.000001', 300, ?, UTC_TIMESTAMP(), 1, UTC_TIMESTAMP(6) - INTERVAL 7 SECOND)`, second)
	srv.routing.record(id, "copy", "")
	behind := func(through time.Time, throughID uint64) RouterServerNumbers {
		t.Helper()
		srv.copyAges.note(id, copyAnswer{side: "copy", ranAt: time.Now(), tailThrough: through, tailThroughID: throughID, allTailed: true})
		srv.ResolveCopyAges(context.Background())
		return srv.RouterNumbers().Servers[id]
	}

	// The answer held every indexed event: 0, not an hour.
	n := behind(second, secondID)
	if n.BehindSourceSeconds == nil || *n.BehindSourceSeconds != 0 || n.BehindSourceRan == "" {
		t.Fatalf("idle source, every event held: %+v, want 0 behind", n)
	}
	if n.DataAgeSeconds == nil || *n.DataAgeSeconds < 3600 {
		t.Fatalf("data_age_seconds changed meaning: %+v", n.DataAgeSeconds)
	}
	// The answer missed the second event: the 5 minutes between them.
	if n := behind(first, firstID); n.BehindSourceSeconds == nil || *n.BehindSourceSeconds != 300 {
		t.Fatalf("one event missed: %+v, want 300 behind", n)
	}

	// The same index before capture's migration: no column, no stamp, so the
	// answer is ran − through, about an hour.
	testutil.MustExec(t, db, `ALTER TABLE stream_state DROP COLUMN caught_up_at`)
	n = behind(second, secondID)
	if n.BehindSourceSeconds == nil || *n.BehindSourceSeconds < 3600 || *n.BehindSourceSeconds > 3700 || n.BehindSourceUnknownReason != "" {
		t.Fatalf("index without the column: %+v, want about 3600 behind and no reason", n)
	}
}
