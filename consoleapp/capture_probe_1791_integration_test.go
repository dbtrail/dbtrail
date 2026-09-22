//go:build integration

package consoleapp

import (
	"context"
	"testing"

	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// #1791 against a real server: the index's real stream_state and the
// source's real binlog status, so the probe's queries are read from the
// other side of the seam rather than from a mock written to match them.
// The test server is both the source and the index.
func TestIntegrationProbeCapture(t *testing.T) {
	db, name := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	indexDSN := testutil.DefaultDSN + "/" + name
	sourceDSN := testutil.DefaultDSN + "/"
	ctx := context.Background()

	// A file-mode index: no stream_state row.
	if verdict, detail := probeCaptureFromDBs(ctx, indexDSN, sourceDSN); verdict != "" || detail != "the index has no live capture on record" {
		t.Fatalf("no capture on record: verdict=%q detail=%q", verdict, detail)
	}

	if _, _, err := config.CurrentBinlogPosition(db); err != nil {
		t.Skipf("the test server reports no binlog position: %v", err)
	}
	// The checkpoint is written with binary logging off for its session:
	// written normally, the write itself would move the server's binlog past
	// the position it records, and "caught up" could never be observed.
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, "SET SESSION sql_log_bin = 0"); err != nil {
		t.Skipf("cannot write the checkpoint outside the binlog: %v", err)
	}
	checkpoint := func(mode, file string, pos uint32, gtid any) {
		t.Helper()
		if _, err := conn.ExecContext(ctx, `REPLACE INTO stream_state (id, mode, binlog_file, binlog_position, gtid_set, flavor, last_checkpoint, server_id)
			VALUES (1, ?, ?, ?, ?, 'mysql', UTC_TIMESTAMP(), 1)`, mode, file, pos, gtid); err != nil {
			t.Fatal(err)
		}
	}
	// Other packages' tests write to the same server in parallel, so the
	// head can move between reading it and probing: caught up has to be seen
	// once in a few tries, behind is then forced by a write of our own.
	var file string
	var pos uint32
	caught := false
	for range 20 {
		if file, pos, err = config.CurrentBinlogPosition(db); err != nil {
			t.Fatal(err)
		}
		checkpoint("position", file, pos, nil)
		if verdict, _ := probeCaptureFromDBs(ctx, indexDSN, sourceDSN); verdict == console.CaptureCaughtUp {
			caught = true
			break
		}
	}
	if !caught {
		t.Fatal("position checkpointed at the source's head never read as caught up")
	}
	// The source writes: its binlog moves past the checkpoint.
	if _, err := db.Exec("CREATE TABLE moved (id INT PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	if verdict, detail := probeCaptureFromDBs(ctx, indexDSN, sourceDSN); verdict != console.CaptureBehind {
		t.Fatalf("position after a write: verdict=%q detail=%q, want behind", verdict, detail)
	}

	// GTID mode: the answer depends on the server. With gtid_mode OFF the
	// source reports no set and the verdict is unknown; with it ON, a set
	// that contains gtid_executed is caught up.
	executed, err := config.CurrentGTIDExecuted(db)
	if err != nil {
		t.Fatal(err)
	}
	if executed == "" {
		checkpoint("gtid", file, pos, "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-10")
		if verdict, detail := probeCaptureFromDBs(ctx, indexDSN, sourceDSN); verdict != "" || detail != "the source reported no GTID set" {
			t.Fatalf("GTID capture, GTIDs off on the source: verdict=%q detail=%q", verdict, detail)
		}
		return
	}
	caught = false
	for range 20 {
		if executed, err = config.CurrentGTIDExecuted(db); err != nil {
			t.Fatal(err)
		}
		checkpoint("gtid", file, pos, executed)
		if verdict, _ := probeCaptureFromDBs(ctx, indexDSN, sourceDSN); verdict == console.CaptureCaughtUp {
			caught = true
			break
		}
	}
	if !caught {
		t.Fatal("a GTID set equal to the source's never read as caught up")
	}
	if _, err := db.Exec("CREATE TABLE moved_again (id INT PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	if verdict, detail := probeCaptureFromDBs(ctx, indexDSN, sourceDSN); verdict != console.CaptureBehind {
		t.Fatalf("GTID after a write: verdict=%q detail=%q, want behind", verdict, detail)
	}
}
