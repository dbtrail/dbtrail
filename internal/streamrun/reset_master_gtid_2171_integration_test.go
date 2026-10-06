//go:build integration

package streamrun

import (
	"database/sql"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"

	"github.com/dbtrail/dbtrail/internal/testutil"
)

// These tests RESET the binary logs of the shared test servers (MySQL and
// MariaDB), which deletes the files any other running stream reads. CI runs
// the integration packages one at a time and nothing in this package uses
// t.Parallel; locally, run this package alone (-p 1).

// resetGTIDSourceBinlogs starts a GTID source's binlog and GTID numbering
// over: RESET BINARY LOGS AND GTIDS on MySQL 8.2+, RESET MASTER before that
// and on MariaDB.
func resetGTIDSourceBinlogs(t *testing.T, db *sql.DB) {
	t.Helper()
	if _, err := db.Exec("RESET BINARY LOGS AND GTIDS"); err == nil {
		return
	}
	testutil.MustExec(t, db, "RESET MASTER")
}

func insertOrders(t *testing.T, db *sql.DB, lo, hi int) {
	t.Helper()
	for i := lo; i <= hi; i++ {
		testutil.MustExec(t, db, "INSERT INTO orders (id, amount) VALUES (?, ?)", i, i)
	}
}

func ordersIndexedThrough(t *testing.T, indexDB *sql.DB, schema string, pk int) func() bool {
	return func() bool {
		var n int
		if err := indexDB.QueryRow(`SELECT COUNT(*) FROM binlog_events
			WHERE schema_name = ? AND table_name = 'orders' AND pk_values = ?`,
			schema, strconv.Itoa(pk)).Scan(&n); err != nil {
			t.Fatalf("poll indexed pk %d: %v", pk, err)
		}
		return n > 0
	}
}

func gapLost(t *testing.T, indexDB *sql.DB) (sql.NullTime, string) {
	t.Helper()
	var at sql.NullTime
	var detail sql.NullString
	if err := indexDB.QueryRow("SELECT gap_lost_at, gap_lost_detail FROM stream_state WHERE id = 1").Scan(&at, &detail); err != nil {
		t.Fatalf("read gap_lost_*: %v", err)
	}
	return at, detail.String
}

// ownGTIDCount is the highest transaction number the MySQL source has given
// its own server_uuid.
func ownGTIDCount(t *testing.T, db *sql.DB) int64 {
	t.Helper()
	var own, executed string
	if err := db.QueryRow("SELECT @@server_uuid, @@GLOBAL.gtid_executed").Scan(&own, &executed); err != nil {
		t.Fatalf("read server_uuid / gtid_executed: %v", err)
	}
	set, err := gomysql.ParseMysqlGTIDSet(NormalizeGTIDSet(executed))
	if err != nil {
		t.Fatalf("parse gtid_executed %q: %v", executed, err)
	}
	var max int64
	for sid, tags := range *set.(*gomysql.MysqlGTIDSet) {
		if !strings.EqualFold(sid.String(), own) {
			continue
		}
		for _, ivs := range tags {
			for _, iv := range ivs {
				if iv.Stop-1 > max {
					max = iv.Stop - 1
				}
			}
		}
	}
	return max
}

// runGTIDResetScenario drives one capture across a RESET of the source:
//
//	run 1 (clean): index 1-5, checkpoint durably.
//	run 2 (crash): index 6-8 while every checkpoint write fails, so they sit
//	               after the durable checkpoint.
//	RESET the source's binlogs and GTIDs, then write post rows from 9.
//	run 3: restart and wait for the last post-reset row.
//
// Rows 6-8 are real changes of the old numbering that the source will never
// send again; rows 9.. are the new numbering's first transactions, whose GTIDs
// fall inside the saved set. All of them must be indexed exactly once, and the
// break in continuity must be stamped as a capture loss.
func runGTIDResetScenario(t *testing.T, src dupSource, indexDB *sql.DB, indexName, wantDetail string, post func(oldOwn int64) int) {
	t.Helper()
	// A small, known numbering to start from, with one transaction in it so a
	// fresh capture starts in GTID mode (an empty executed set starts in
	// position mode on MySQL).
	resetGTIDSourceBinlogs(t, src.db)
	testutil.MustExec(t, src.db, "INSERT INTO orders (id, amount) VALUES (-1, 0)")
	// The old numbering's checkpoint lands in a later file than the new
	// numbering's first one, as it does on any server that has rotated.
	for range 3 {
		testutil.MustExec(t, src.db, "FLUSH BINARY LOGS")
	}
	cfg := func(n uint32) Config {
		c := src.config(indexName)
		c.ServerID = src.serverID + n
		c.Checkpoint = 1
		return c
	}
	if err := runOneUntil(t, cfg(0), true, func() { insertOrders(t, src.db, 1, 5) }, ordersIndexedThrough(t, indexDB, src.schema, 5)); err != nil {
		t.Fatalf("run 1 (clean): %v", err)
	}
	durable, err := loadStreamState(indexDB)
	if err != nil || durable == nil || durable.mode != "gtid" {
		t.Fatalf("run 1 checkpoint = %+v, err %v: want a GTID-mode checkpoint", durable, err)
	}

	lift := blockCheckpoints(t, indexDB)
	if err := runOneUntil(t, cfg(1), false, func() { insertOrders(t, src.db, 6, 8) }, ordersIndexedThrough(t, indexDB, src.schema, 8)); err != nil {
		t.Fatalf("run 2 (crash): %v", err)
	}
	lift()
	assertExactlyOnce(t, indexedPKs(t, indexDB, src.schema, "orders"), pkRange(1, 8))

	var oldOwn int64
	if src.flavor != gomysql.MariaDBFlavor {
		oldOwn = ownGTIDCount(t, src.db)
	}
	resetGTIDSourceBinlogs(t, src.db)
	last := post(oldOwn)
	insertOrders(t, src.db, 9, last)

	if src.flavor == gomysql.MariaDBFlavor {
		mariadbResumeAfterReset(t, src, indexDB, cfg(2), cfg(3), cfg(4), last)
		return
	}

	err = runOneUntil(t, cfg(2), false, nil, ordersIndexedThrough(t, indexDB, src.schema, last))
	if err != nil {
		t.Fatalf("run 3 (restart after the reset): %v", err)
	}
	at, detail := gapLost(t, indexDB)
	t.Logf("run 3 stamped gap_lost_at=%v detail=%q", at, detail)
	if !at.Valid {
		t.Fatal("run 3 stamped no capture loss after the source's GTID numbering went backwards")
	}
	if !strings.Contains(detail, wantDetail) {
		t.Errorf("run 3 loss detail = %q, want it to say %q", detail, wantDetail)
	}
	assertExactlyOnce(t, indexedPKs(t, indexDB, src.schema, "orders"), pkRange(1, last))
	if t.Failed() {
		return
	}

	// run 4 crashes in the new numbering, run 5 restarts: the ordinary cleanup
	// must remove only run 4's rows past the checkpoint (they are replayed)
	// and nothing of the old numbering, whose rows share file names with the
	// new one. And the restart must not stamp a second loss.
	lift = blockCheckpoints(t, indexDB)
	if err := runOneUntil(t, cfg(3), false, func() { insertOrders(t, src.db, last+1, last+3) }, ordersIndexedThrough(t, indexDB, src.schema, last+3)); err != nil {
		t.Fatalf("run 4 (crash in the new numbering): %v", err)
	}
	lift()
	if err := runOneUntil(t, cfg(4), true, func() { insertOrders(t, src.db, last+4, last+4) }, ordersIndexedThrough(t, indexDB, src.schema, last+4)); err != nil {
		t.Fatalf("run 5 (restart in the new numbering): %v", err)
	}
	assertExactlyOnce(t, indexedPKs(t, indexDB, src.schema, "orders"), pkRange(1, last+4))
	at5, detail5 := gapLost(t, indexDB)
	if !at5.Time.Equal(at.Time) || detail5 != detail {
		t.Errorf("run 5 re-stamped the capture loss (%v %q, was %v %q): a restart in the new numbering is not a new break",
			at5, detail5, at, detail)
	}
}

// mariadbResumeAfterReset: on MariaDB the restart refuses, deletes nothing,
// and says how to resume; following those steps captures the new numbering
// from its first transaction and stamps the loss.
func mariadbResumeAfterReset(t *testing.T, src dupSource, indexDB *sql.DB, restart, resume, back Config, last int) {
	t.Helper()
	var earliest string
	var size int64
	rows, err := src.db.Query("SHOW BINARY LOGS")
	if err != nil {
		t.Fatalf("SHOW BINARY LOGS: %v", err)
	}
	if rows.Next() {
		cols, _ := rows.Columns()
		vals := make([]any, len(cols))
		vals[0], vals[1] = &earliest, &size
		for i := 2; i < len(cols); i++ {
			vals[i] = new(sql.RawBytes)
		}
		if err := rows.Scan(vals...); err != nil {
			t.Fatalf("scan SHOW BINARY LOGS: %v", err)
		}
	}
	rows.Close()

	err = runOneUntil(t, restart, false, nil, func() bool { return false })
	t.Logf("run 3 (restart after the reset) returned: %v", err)
	var refused *SourceRenumberedError
	if !errors.As(err, &refused) {
		t.Fatalf("run 3 = %v, want a SourceRenumberedError", err)
	}
	steps := "--reset --start-file " + earliest + " --start-pos 4"
	if !strings.Contains(err.Error(), steps) {
		t.Errorf("run 3 error does not carry the resume steps %q: %v", steps, err)
	}
	assertExactlyOnce(t, indexedPKs(t, indexDB, src.schema, "orders"), pkRange(1, 8))
	if at, _ := gapLost(t, indexDB); at.Valid {
		t.Errorf("run 3 refused but stamped a capture loss at %v", at)
	}
	if t.Failed() {
		return
	}

	// The steps, exactly as the error gives them.
	resume.Reset = true
	resume.StartFile = earliest
	resume.StartPos = 4
	if err := runOneUntil(t, resume, false, nil, ordersIndexedThrough(t, indexDB, src.schema, last)); err != nil {
		t.Fatalf("run 4 (the error's resume steps): %v", err)
	}
	assertExactlyOnce(t, indexedPKs(t, indexDB, src.schema, "orders"), pkRange(1, last))
	at, detail := gapLost(t, indexDB)
	if !at.Valid {
		t.Fatal("resuming with the error's steps stamped no capture loss")
	}
	t.Logf("run 4 stamped gap_lost_at=%v detail=%q", at, detail)

	// The error's way back to GTID mode: the stopped capture's checkpoint,
	// translated with BINLOG_GTID_POS on the source. Nothing skipped, nothing
	// twice, no new loss.
	cp, err := loadStreamState(indexDB)
	if err != nil || cp == nil || cp.mode != "position" {
		t.Fatalf("checkpoint after run 4 = %+v, err %v: want position mode", cp, err)
	}
	var startGTID string
	if err := src.db.QueryRow("SELECT BINLOG_GTID_POS(?, ?)", cp.binlogFile, cp.binlogPos).Scan(&startGTID); err != nil {
		t.Fatalf("BINLOG_GTID_POS(%s, %d): %v", cp.binlogFile, cp.binlogPos, err)
	}
	insertOrders(t, src.db, last+1, last+2)
	back.StartGTID = startGTID
	if err := runOneUntil(t, back, false, nil, ordersIndexedThrough(t, indexDB, src.schema, last+2)); err != nil {
		t.Fatalf("run 5 (back to GTID mode from BINLOG_GTID_POS of the checkpoint %s:%d = %q): %v", cp.binlogFile, cp.binlogPos, startGTID, err)
	}
	assertExactlyOnce(t, indexedPKs(t, indexDB, src.schema, "orders"), pkRange(1, last+2))
	if st, _ := loadStreamState(indexDB); st == nil || st.mode != "gtid" {
		t.Errorf("after run 5 the checkpoint is %+v, want GTID mode", st)
	}
	if at5, detail5 := gapLost(t, indexDB); !at5.Time.Equal(at.Time) || detail5 != detail {
		t.Errorf("returning to GTID mode stamped a new loss (%v %q, was %v %q)", at5, detail5, at, detail)
	}
}

// TestIntegrationGTIDResetMasterRestartBeforeRenumberingPasses: the restart
// comes while the new numbering is still below the saved one (the source's
// gtid_executed does not contain the saved set).
func TestIntegrationGTIDResetMasterRestartBeforeRenumberingPasses(t *testing.T) {
	indexDB, indexName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)
	src := mysqlGTIDDupSource(t, 99920)
	runGTIDResetScenario(t, src, indexDB, indexName, "numbering went backwards", func(int64) int { return 10 })
}

// TestIntegrationGTIDResetMasterRestartAfterRenumberingPasses: capture is down
// until the new numbering has passed the saved one, so the source's
// gtid_executed contains the saved set again, by number only.
func TestIntegrationGTIDResetMasterRestartAfterRenumberingPasses(t *testing.T) {
	indexDB, indexName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)
	src := mysqlGTIDDupSource(t, 99924)
	runGTIDResetScenario(t, src, indexDB, indexName, "numbering started over", func(oldOwn int64) int { return 8 + int(oldOwn) + 3 })
}

// TestIntegrationGTIDResetMasterFileNameReused: after the reset the new
// numbering rotates back past the old checkpoint's file name before capture
// restarts, still below the old numbering. The file name proves nothing
// about its content: the restart must read it as a reset and capture the new
// transactions, not keep the old numbers and skip them.
func TestIntegrationGTIDResetMasterFileNameReused(t *testing.T) {
	indexDB, indexName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)
	src := mysqlGTIDDupSource(t, 99940)
	runGTIDResetScenario(t, src, indexDB, indexName, "numbering went backwards", func(int64) int {
		for range 5 {
			testutil.MustExec(t, src.db, "FLUSH BINARY LOGS")
		}
		return 10
	})
}

// TestOne_MariaDB_resetMasterStopsWithResumeSteps: the MariaDB sibling.
func TestOne_MariaDB_resetMasterStopsWithResumeSteps(t *testing.T) {
	indexDB, indexName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)
	src := mariadbDupSource(t, 99928)
	runGTIDResetScenario(t, src, indexDB, indexName, "", func(int64) int { return 10 })
}

// TestIntegrationGTIDResetMasterCrashRightAfterRestart: the restart after the
// reset persists its jump to the start of the source's binary log and then
// crashes before any later checkpoint, with the new numbering's rows already
// indexed. The next restart's ordinary cleanup replays from that jump: it
// must delete only those new rows (they are sent again), never the old
// numbering's rows 6-8, which sit after the old checkpoint and above its
// cleanup floor. Only the fresh floor the jump persisted keeps them out.
func TestIntegrationGTIDResetMasterCrashRightAfterRestart(t *testing.T) {
	indexDB, indexName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)
	src := mysqlGTIDDupSource(t, 99932)
	resetGTIDSourceBinlogs(t, src.db)
	testutil.MustExec(t, src.db, "INSERT INTO orders (id, amount) VALUES (-1, 0)")
	for range 3 {
		testutil.MustExec(t, src.db, "FLUSH BINARY LOGS")
	}
	cfg := func(n uint32) Config {
		c := src.config(indexName)
		c.ServerID = src.serverID + n
		c.Checkpoint = 1
		return c
	}
	if err := runOneUntil(t, cfg(0), true, func() { insertOrders(t, src.db, 1, 5) }, ordersIndexedThrough(t, indexDB, src.schema, 5)); err != nil {
		t.Fatalf("run 1 (clean): %v", err)
	}
	lift := blockCheckpoints(t, indexDB)
	if err := runOneUntil(t, cfg(1), false, func() { insertOrders(t, src.db, 6, 8) }, ordersIndexedThrough(t, indexDB, src.schema, 8)); err != nil {
		t.Fatalf("run 2 (crash): %v", err)
	}
	lift()

	resetGTIDSourceBinlogs(t, src.db)
	insertOrders(t, src.db, 9, 10)

	unfreeze := freezeCheckpointAfterGTIDRestart(t, indexDB)
	if err := runOneUntil(t, cfg(2), false, nil, ordersIndexedThrough(t, indexDB, src.schema, 10)); err != nil {
		t.Fatalf("run 3 (restart after the reset, crash after the jump): %v", err)
	}
	unfreeze()
	frozen, err := loadStreamState(indexDB)
	if err != nil {
		t.Fatal(err)
	}
	if frozen.binlogPos != 4 {
		t.Fatalf("run 3 durable checkpoint = %s:%d, want the jump's :4", frozen.binlogFile, frozen.binlogPos)
	}
	if at, _ := gapLost(t, indexDB); !at.Valid {
		t.Fatal("run 3 stamped no capture loss")
	}

	if err := runOneUntil(t, cfg(3), true, func() { insertOrders(t, src.db, 11, 11) }, ordersIndexedThrough(t, indexDB, src.schema, 11)); err != nil {
		t.Fatalf("run 4 (restart from the jump): %v", err)
	}
	assertExactlyOnce(t, indexedPKs(t, indexDB, src.schema, "orders"), pkRange(1, 11))
}

// freezeCheckpointAfterGTIDRestart lets the loss stamp and the jump to :4
// through, then refuses every later checkpoint: a crash right after the
// restart persisted its jump.
func freezeCheckpointAfterGTIDRestart(t *testing.T, indexDB *sql.DB) func() {
	t.Helper()
	const name = "bintrail_test_freeze_after_gtid_restart"
	testutil.MustExec(t, indexDB, "DROP TRIGGER IF EXISTS "+name)
	testutil.MustExec(t, indexDB, `
		CREATE TRIGGER `+name+` BEFORE UPDATE ON stream_state
		FOR EACH ROW BEGIN
		  IF OLD.binlog_position = 4 THEN
		    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'simulated crash right after the jump';
		  END IF;
		END`)
	lifted := false
	t.Cleanup(func() {
		if !lifted {
			indexDB.Exec("DROP TRIGGER IF EXISTS " + name)
		}
	})
	return func() {
		lifted = true
		testutil.MustExec(t, indexDB, "DROP TRIGGER IF EXISTS "+name)
	}
}

// TestIntegrationGTIDResetMasterWhileStreaming: the reset hits a running
// capture. The source ends the binlog dump ("could not find next log"); the
// restart that follows (the supervisor's, here the test's) must see the
// source went backwards, stamp the loss, and capture the new numbering.
func TestIntegrationGTIDResetMasterWhileStreaming(t *testing.T) {
	indexDB, indexName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)
	src := mysqlGTIDDupSource(t, 99936)
	resetGTIDSourceBinlogs(t, src.db)
	testutil.MustExec(t, src.db, "INSERT INTO orders (id, amount) VALUES (-1, 0)")
	for range 3 {
		testutil.MustExec(t, src.db, "FLUSH BINARY LOGS")
	}
	cfg := src.config(indexName)
	cfg.Checkpoint = 1
	err := runOneUntil(t, cfg, true, func() {
		insertOrders(t, src.db, 1, 3)
		for !ordersIndexedThrough(t, indexDB, src.schema, 3)() {
			time.Sleep(20 * time.Millisecond)
		}
		time.Sleep(1500 * time.Millisecond) // a checkpoint past row 3
		resetGTIDSourceBinlogs(t, src.db)
		insertOrders(t, src.db, 4, 6)
	}, ordersIndexedThrough(t, indexDB, src.schema, 6))
	t.Logf("the running capture, across the reset, returned: %v", err)
	if err == nil {
		t.Fatal("the running capture indexed the new numbering without stopping; the scenario did not happen")
	}

	cfg.ServerID++
	if err := runOneUntil(t, cfg, false, nil, ordersIndexedThrough(t, indexDB, src.schema, 6)); err != nil {
		t.Fatalf("restart after the reset: %v", err)
	}
	assertExactlyOnce(t, indexedPKs(t, indexDB, src.schema, "orders"), pkRange(1, 6))
	if at, detail := gapLost(t, indexDB); !at.Valid {
		t.Error("the restart after a reset during capture stamped no capture loss")
	} else {
		t.Logf("stamped: %q", detail)
	}
}

// TestIntegrationGTIDCheckpointFromAnotherServer: binlog file numbers of two
// servers are not comparable. A checkpoint whose file sorts after the
// source's newest one, written against ANOTHER server (capture moved from a
// replica to its primary behind the same address), whose GTIDs the source
// all has, is a normal resume: no loss stamped, nothing skipped. The same
// checkpoint written against THIS server is a numbering that started over.
func TestIntegrationGTIDCheckpointFromAnotherServer(t *testing.T) {
	for _, c := range []struct {
		name      string
		identity  func(own string) string
		wantStamp bool
	}{
		{"another server", func(string) string { return "11111111-2222-3333-4444-555555555555" }, false},
		{"older checkpoint without an identity", func(string) string { return "" }, false},
		{"this server", func(own string) string { return own }, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			indexDB, indexName := testutil.CreateTestDB(t)
			testutil.InitIndexTables(t, indexDB)
			src := mysqlGTIDDupSource(t, 99944)
			// Only GTID transactions in the binary log: a restart from its
			// start cannot replay transactions written before gtid_mode=ON.
			resetGTIDSourceBinlogs(t, src.db)
			insertOrders(t, src.db, 1, 1)
			var own, executed string
			if err := src.db.QueryRow("SELECT @@server_uuid, @@GLOBAL.gtid_executed").Scan(&own, &executed); err != nil {
				t.Fatal(err)
			}
			if err := saveCheckpoint(indexDB, &streamState{
				mode: "gtid", binlogFile: "binlog.999990", binlogPos: 4,
				gtidSet: NormalizeGTIDSet(executed), flavor: gomysql.MySQLFlavor,
				serverID: src.serverID, sourceIdentity: c.identity(own),
			}); err != nil {
				t.Fatalf("seed checkpoint: %v", err)
			}
			cfg := src.config(indexName)
			cfg.ServerID++
			if err := runOneUntil(t, cfg, true, func() { insertOrders(t, src.db, 2, 3) }, ordersIndexedThrough(t, indexDB, src.schema, 3)); err != nil {
				t.Fatalf("resume: %v", err)
			}
			// A renumbering restarts from the source's oldest binlog, which
			// still holds row 1 (written before the seeded checkpoint).
			want := pkRange(2, 3)
			if c.wantStamp {
				want = pkRange(1, 3)
			}
			assertExactlyOnce(t, indexedPKs(t, indexDB, src.schema, "orders"), want)
			at, detail := gapLost(t, indexDB)
			if at.Valid != c.wantStamp {
				t.Errorf("capture loss stamped = %v (%q), want %v", at.Valid, detail, c.wantStamp)
			}
			if st, _ := loadStreamState(indexDB); st == nil || !strings.EqualFold(st.sourceIdentity, own) {
				t.Errorf("checkpoint source_identity = %+v, want this server's %s", st, own)
			}
		})
	}
}
