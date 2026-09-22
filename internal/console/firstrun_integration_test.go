//go:build integration

package console

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
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

// TestIntegrationFirstRunReadsTheBackupLocation is #1801 end to end over a
// real index and a real folder: once every capture step is done, the handler
// reads the server's own backup location, finds the snapshot there and ends
// the list. It is the other half of the unit test that pins the locations are
// NOT read while an earlier step is pending: with both, neither "always read"
// nor "never read" passes.
func TestIntegrationFirstRunReadsTheBackupLocation(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, name := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	testutil.InsertSnapshot(t, db, 1, "2026-09-15 10:00:00", "shop", "t", "id", 1, "PRI", "int", "NO")
	testutil.MustExec(t, db, `INSERT INTO stream_state (id, mode, flavor, last_checkpoint, server_id, binlog_position, events_indexed) VALUES (1, 'gtid', 'mysql', UTC_TIMESTAMP(), 1, 23456789, 7)`)

	backups := t.TempDir()
	snapDir := filepath.Join(backups, "2026-09-22T11-52-50Z", "shop")
	if err := os.MkdirAll(snapDir, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, f := range []string{filepath.Join(snapDir, "t.parquet"), filepath.Join(backups, "2026-09-22T11-52-50Z", "_SUCCESS")} {
		if err := os.WriteFile(f, nil, 0o644); err != nil {
			t.Fatal(err)
		}
	}

	srv, _ := newBaselineTriggerServer(t)
	srv.monitorCtrl.(*stubMonitorCtrl).status = MonitorStatus{State: "running", SourceConnected: true}
	e, err := srv.cm.reg.Add(ServerEntry{
		Name: "live", DSN: testutil.BaseDSN() + "/" + name,
		SourceDSN: "src:srcpw@tcp(127.0.0.1:2)/", BaselineDir: backups,
	})
	if err != nil {
		t.Fatal(err)
	}
	rec, body := doServersReq(t, srv, "GET", "/api/servers/"+e.ID+"/first-run", "")
	var rep FirstRunReport
	if rec.Code != 200 || json.Unmarshal(body, &rep) != nil || len(rep.Steps) == 0 {
		t.Fatalf("code = %d, body = %s", rec.Code, body)
	}
	last := rep.Steps[len(rep.Steps)-1]
	if !rep.Complete || last.State != firstRunDone {
		t.Fatalf("complete = %v, backup step = %+v; the backup in %s was not found", rep.Complete, last, backups)
	}

	// And with the folder empty, the same server keeps the list up: the read
	// happened and answered "none", rather than being skipped.
	e.BaselineDir = t.TempDir()
	if err := srv.cm.reg.Update(e); err != nil {
		t.Fatal(err)
	}
	rec, body = doServersReq(t, srv, "GET", "/api/servers/"+e.ID+"/first-run", "")
	rep = FirstRunReport{}
	if rec.Code != 200 || json.Unmarshal(body, &rep) != nil {
		t.Fatalf("code = %d, body = %s", rec.Code, body)
	}
	if last = rep.Steps[len(rep.Steps)-1]; rep.Complete || last.State == firstRunDone {
		t.Fatalf("an empty folder ended the list: complete = %v, %+v", rep.Complete, last)
	}
}
