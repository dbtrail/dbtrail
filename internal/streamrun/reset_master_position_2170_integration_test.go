//go:build integration

package streamrun

import (
	"database/sql"
	"strconv"
	"testing"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"

	"github.com/dbtrail/dbtrail/internal/testutil"
)

// resetSourceBinlogs starts the source's binlog numbering over: RESET BINARY
// LOGS AND GTIDS on MySQL 8.2+, RESET MASTER before that (8.4 removed it).
func resetSourceBinlogs(t *testing.T, db *sql.DB) {
	t.Helper()
	if _, err := db.Exec("RESET BINARY LOGS AND GTIDS"); err == nil {
		return
	}
	testutil.MustExec(t, db, "RESET MASTER")
}

// TestIntegrationPositionResetMasterKeepsCapturedRows is #2170 on a real
// server. A position-mode stream indexes rows, some of them after its last
// durable checkpoint; the source's binlog numbering then starts over. On the
// restart the saved file is gone, so capture jumps to binlog.000001:4 and
// stamps a capture loss. Every row captured before the reset is a real change
// that the new numbering will never send again, so the restart's cleanup must
// keep all of them.
//
//	run 1  clean: index 1-5, checkpoint durably.
//	run 2  crash: index 6-10 while every checkpoint write fails.
//	       RESET the source's binlogs.
//	run 3  restart: jumps, stamps the loss, checkpoints in the new numbering
//	       and stops before indexing anything new. 1-10 must all survive.
//	run 4  crash again, in the new numbering: index 11-13 past the checkpoint.
//	run 5  restart: the ordinary cleanup must delete 11-13 (they are replayed)
//	       and nothing of the old numbering. 1-14 exactly once.
//
// Run 3 is what main got wrong first (its cleanup compared binlog.000001:4 with
// rows of the old numbering). Run 5 is the second way to lose the same rows:
// the jump carried the old cleanup floor into the new numbering's checkpoint,
// so the next ordinary cleanup read old rows as "after" a binlog.000001 position.
func TestIntegrationPositionResetMasterKeepsCapturedRows(t *testing.T) {
	indexDB, indexName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)

	sourceDB, sourceName := testutil.CreateTestDB(t)
	sourceDSN := testutil.IntegrationDSN(sourceName)
	const serverIDBase = 99940

	var logBin string
	if err := sourceDB.QueryRow("SELECT @@log_bin").Scan(&logBin); err != nil || logBin != "1" {
		t.Skip("skipping: binary logging not enabled on test MySQL")
	}
	var gtidMode string
	if err := sourceDB.QueryRow("SELECT @@gtid_mode").Scan(&gtidMode); err != nil {
		t.Fatalf("read @@gtid_mode: %v", err)
	}
	if gtidMode == "ON" {
		t.Skip("skipping: the source runs gtid_mode=ON, so a fresh run captures in GTID mode, not position mode")
	}

	testutil.MustExec(t, sourceDB, `CREATE TABLE orders (id INT PRIMARY KEY, amount INT NOT NULL)`)
	insert := func(lo, hi int) func() {
		return func() {
			for i := lo; i <= hi; i++ {
				testutil.MustExec(t, sourceDB, "INSERT INTO orders (id, amount) VALUES (?, ?)", i, i*10)
			}
		}
	}
	indexedThrough := func(hi int) func() bool {
		return func() bool {
			var n int
			if err := indexDB.QueryRow(`SELECT COUNT(*) FROM binlog_events
				WHERE schema_name = ? AND table_name = 'orders' AND pk_values = ?`,
				sourceName, strconv.Itoa(hi)).Scan(&n); err != nil {
				t.Fatalf("poll indexed pk %d: %v", hi, err)
			}
			return n > 0
		}
	}
	cfg := func(n uint32) Config {
		return Config{
			IndexDSN:   testutil.IntegrationDSN(indexName),
			SourceDSN:  sourceDSN,
			Flavor:     gomysql.MySQLFlavor,
			ServerID:   serverIDBase + n,
			BatchSize:  1,
			Schemas:    sourceName,
			Checkpoint: 1,
			GapTimeout: 30,
			Format:     "text",
			SSLMode:    "preferred",
			Deps:       testStreamDeps(),
		}
	}
	never := func() bool { return true }

	// run 1
	if err := runOneUntil(t, cfg(0), true, insert(1, 5), indexedThrough(5)); err != nil {
		t.Fatalf("run 1 (clean): %v", err)
	}
	durable, err := loadStreamState(indexDB)
	if err != nil || durable == nil || durable.mode != "position" {
		t.Fatalf("run 1 checkpoint = %+v, err %v: want a position-mode checkpoint", durable, err)
	}

	// run 2
	lift := blockCheckpoints(t, indexDB)
	if err := runOneUntil(t, cfg(1), false, insert(6, 10), indexedThrough(10)); err != nil {
		t.Fatalf("run 2 (crash): %v", err)
	}
	if s, _ := loadStreamState(indexDB); s.binlogFile != durable.binlogFile || s.binlogPos != durable.binlogPos {
		t.Fatalf("run 2 moved the checkpoint to %s:%d; the crash was not simulated", s.binlogFile, s.binlogPos)
	}
	assertExactlyOnce(t, indexedPKs(t, indexDB, sourceName, "orders"), pkRange(1, 10))
	lift()

	resetSourceBinlogs(t, sourceDB)

	// run 3
	if err := runOneUntil(t, cfg(2), true, nil, never); err != nil {
		t.Fatalf("run 3 (restart after the reset): %v", err)
	}
	var lostAt sql.NullTime
	if err := indexDB.QueryRow("SELECT gap_lost_at FROM stream_state WHERE id = 1").Scan(&lostAt); err != nil {
		t.Fatalf("read gap_lost_at: %v", err)
	}
	if !lostAt.Valid {
		t.Fatal("run 3 stamped no capture loss: the restart did not jump to the new numbering, so this test is not exercising #2170")
	}
	jumped, err := loadStreamState(indexDB)
	if err != nil {
		t.Fatal(err)
	}
	if jumped.binlogFile == durable.binlogFile && jumped.binlogPos >= durable.binlogPos {
		t.Fatalf("run 3 checkpoint %s:%d does not sort below the old one %s:%d: the reset did not start the numbering over",
			jumped.binlogFile, jumped.binlogPos, durable.binlogFile, durable.binlogPos)
	}
	assertExactlyOnce(t, indexedPKs(t, indexDB, sourceName, "orders"), pkRange(1, 10))
	if t.Failed() {
		t.FailNow()
	}

	// run 4
	lift = blockCheckpoints(t, indexDB)
	if err := runOneUntil(t, cfg(3), false, insert(11, 13), indexedThrough(13)); err != nil {
		t.Fatalf("run 4 (crash in the new numbering): %v", err)
	}
	assertExactlyOnce(t, indexedPKs(t, indexDB, sourceName, "orders"), pkRange(1, 13))
	lift()

	// run 5
	if err := runOneUntil(t, cfg(4), false, insert(14, 14), indexedThrough(14)); err != nil {
		t.Fatalf("run 5 (restart in the new numbering): %v", err)
	}
	assertExactlyOnce(t, indexedPKs(t, indexDB, sourceName, "orders"), pkRange(1, 14))
}
