//go:build integration

package console

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestIntegrationLoadFirstRunIndex is #1606: the evidence behind each step is
// read from the server's own index database, which does not exist until Start
// creates it, and a database that cannot be read marks nothing done.
func TestIntegrationLoadFirstRunIndex(t *testing.T) {
	ctx := context.Background()
	load := func(dsn string) firstRunInput {
		var in firstRunInput
		loadFirstRunIndex(ctx, dsn, &in)
		return in
	}
	exists := func(in firstRunInput) string {
		if in.IndexExists == nil {
			return "unknown"
		}
		if *in.IndexExists {
			return "yes"
		}
		return "no"
	}

	t.Run("no index DSN yet", func(t *testing.T) {
		if in := load(""); exists(in) != "no" || in.CheckError != "" {
			t.Fatalf("got %s %+v", exists(in), in)
		}
	})
	t.Run("the database has not been created", func(t *testing.T) {
		testutil.SkipIfNoMySQL(t)
		in := load(testutil.BaseDSN() + "/bintrail_firstrun_never_created")
		if exists(in) != "no" || in.CheckError != "" {
			t.Fatalf("got %s %+v", exists(in), in)
		}
	})
	t.Run("created, tables not there yet", func(t *testing.T) {
		_, name := testutil.CreateTestDB(t)
		in := load(testutil.BaseDSN() + "/" + name)
		if exists(in) != "yes" || in.SnapshotTaken || in.StreamStarted || in.CheckError != "" {
			t.Fatalf("got %s %+v", exists(in), in)
		}
	})

	db, name := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	dsn := testutil.BaseDSN() + "/" + name
	t.Run("tables created, nothing read yet", func(t *testing.T) {
		in := load(dsn)
		if exists(in) != "yes" || in.SnapshotTaken || in.StreamStarted || in.CheckError != "" {
			t.Fatalf("got %s %+v", exists(in), in)
		}
	})
	testutil.InsertSnapshot(t, db, 1, "2026-09-15 10:00:00", "shop", "t", "id", 1, "PRI", "int", "NO")
	t.Run("table structure read", func(t *testing.T) {
		if in := load(dsn); !in.SnapshotTaken || in.StreamStarted || in.CheckError != "" {
			t.Fatalf("got %+v", in)
		}
	})
	// What a PostgreSQL daemon's health poll writes before any commit.
	testutil.MustExec(t, db, `INSERT INTO stream_state (id, mode, flavor, last_checkpoint, server_id, source_health) VALUES (1, 'gtid', 'postgres', UTC_TIMESTAMP(), 1, '{}')`)
	t.Run("a row with no saved position is not a started stream", func(t *testing.T) {
		if in := load(dsn); in.StreamStarted || in.CheckError != "" {
			t.Fatalf("got %+v", in)
		}
	})
	testutil.MustExec(t, db, `UPDATE stream_state SET binlog_position = 23456789`)
	t.Run("first position saved, no change yet", func(t *testing.T) {
		if in := load(dsn); !in.StreamStarted || in.EventsIndexed != 0 || in.CheckError != "" {
			t.Fatalf("got %+v", in)
		}
	})
	testutil.MustExec(t, db, `UPDATE stream_state SET events_indexed = 7`)
	t.Run("changes indexed", func(t *testing.T) {
		in := load(dsn)
		if in.EventsIndexed != 7 || !firstRunSteps(in).Complete {
			t.Fatalf("got %+v", in)
		}
	})
	testutil.MustExec(t, db, `UPDATE stream_state SET events_indexed = 0`)
	testutil.InsertEvent(t, db, "binlog.000001", 100, 200, time.Now().UTC().Format("2006-01-02 15:04:05"), nil,
		"shop", "t", 1, "1", nil, nil, []byte(`{"id":1}`))
	t.Run("a reset counter with changes in the index still completes", func(t *testing.T) {
		in := load(dsn)
		if in.EventsIndexed != 0 || !in.HasEvents || !firstRunSteps(in).Complete {
			t.Fatalf("got %+v", in)
		}
	})
	t.Run("an index server that cannot be reached marks nothing done and keeps the password out", func(t *testing.T) {
		in := load("root:s3cr3t-pw@tcp(127.0.0.1:1)/bintrail_idx_x")
		if exists(in) != "unknown" || in.CheckError == "" || strings.Contains(in.CheckError, "s3cr3t-pw") {
			t.Fatalf("got %s %+v", exists(in), in)
		}
	})
}
