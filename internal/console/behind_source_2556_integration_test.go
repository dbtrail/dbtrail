//go:build integration

package console

import (
	"context"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/testutil"
)

// #2556 against a real index: the router's loop reads the capture stamp from
// stream_state on the index's clock, and the status document says the last
// answer's distance to the source. An index without the column yet (an older
// capture last wrote it) reads as "no stamp", never as an error.
func TestIntegrationRouterBehindSource_2556(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	srv, err := New(Config{DB: db, DBName: dbName, Listen: "127.0.0.1:8090", Token: intToken, NoArchive: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.cm.CloseAll)
	id := srv.cm.defaultID()

	// The source sat idle for an hour; the copy's last answer read through
	// its newest event; capture was caught up 7 seconds ago.
	newest := time.Now().UTC().Add(-time.Hour).Truncate(time.Second)
	testutil.MustExec(t, db, `INSERT INTO stream_state
		(id, mode, binlog_file, binlog_position, last_event_time, last_checkpoint, server_id, caught_up_at)
		VALUES (1, 'position', 'binlog.000001', 4, ?, UTC_TIMESTAMP(), 1, UTC_TIMESTAMP(6) - INTERVAL 7 SECOND)`, newest)
	srv.routing.record(id, "copy", "")
	srv.copyAges.note(id, copyAnswer{side: "copy", ranAt: time.Now(), tailThrough: newest, allTailed: true})

	srv.ResolveCopyAges(context.Background())
	n := srv.RouterNumbers().Servers[id]
	if n.BehindSourceSeconds == nil || *n.BehindSourceSeconds != 0 || n.BehindSourceRan == "" {
		t.Fatalf("idle source, capture caught up: %+v, want 0 behind", n)
	}
	if n.DataAgeSeconds == nil || *n.DataAgeSeconds < 3600 {
		t.Fatalf("data_age_seconds changed meaning: %+v", n.DataAgeSeconds)
	}

	// The same index before capture's migration: no column, no stamp, so the
	// answer is ran − through, about an hour.
	testutil.MustExec(t, db, `ALTER TABLE stream_state DROP COLUMN caught_up_at`)
	srv.copyAges.note(id, copyAnswer{side: "copy", ranAt: time.Now(), tailThrough: newest, allTailed: true})
	srv.ResolveCopyAges(context.Background())
	n = srv.RouterNumbers().Servers[id]
	if n.BehindSourceSeconds == nil || *n.BehindSourceSeconds < 3600 || *n.BehindSourceSeconds > 3700 || n.BehindSourceUnknownReason != "" {
		t.Fatalf("index without the column: %+v, want about 3600 behind and no reason", n)
	}
}
