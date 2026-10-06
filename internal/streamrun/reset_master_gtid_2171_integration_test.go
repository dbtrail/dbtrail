//go:build integration

package streamrun

import (
	"database/sql"
	"strconv"
	"strings"
	"testing"

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
func runGTIDResetScenario(t *testing.T, src dupSource, indexDB *sql.DB, indexName string, post func(oldOwn int64) int) {
	t.Helper()
	// A small, known numbering to start from, with one transaction in it so a
	// fresh capture starts in GTID mode (an empty executed set starts in
	// position mode on MySQL).
	resetGTIDSourceBinlogs(t, src.db)
	testutil.MustExec(t, src.db, "INSERT INTO orders (id, amount) VALUES (-1, 0)")
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

	err = runOneUntil(t, cfg(2), false, nil, ordersIndexedThrough(t, indexDB, src.schema, last))
	t.Logf("run 3 (restart after the reset) returned: %v", err)
	at, detail := gapLost(t, indexDB)
	t.Logf("gap_lost_at=%v detail=%q", at, detail)
	if err != nil {
		t.Fatalf("run 3 (restart after the reset): %v", err)
	}
	if !at.Valid {
		t.Error("run 3 stamped no capture loss after the source's GTID numbering went backwards")
	}
	assertExactlyOnce(t, indexedPKs(t, indexDB, src.schema, "orders"), pkRange(1, last))
}

// TestIntegrationGTIDResetMasterRestartBeforeRenumberingPasses: the restart
// comes while the new numbering is still below the saved one (the source's
// gtid_executed does not contain the saved set).
func TestIntegrationGTIDResetMasterRestartBeforeRenumberingPasses(t *testing.T) {
	indexDB, indexName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)
	src := mysqlGTIDDupSource(t, 99920)
	runGTIDResetScenario(t, src, indexDB, indexName, func(int64) int { return 10 })
}

// TestIntegrationGTIDResetMasterRestartAfterRenumberingPasses: capture is down
// until the new numbering has passed the saved one, so the source's
// gtid_executed contains the saved set again, by number only.
func TestIntegrationGTIDResetMasterRestartAfterRenumberingPasses(t *testing.T) {
	indexDB, indexName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)
	src := mysqlGTIDDupSource(t, 99924)
	runGTIDResetScenario(t, src, indexDB, indexName, func(oldOwn int64) int { return 8 + int(oldOwn) + 3 })
}

// TestOne_MariaDB_resetMasterStopsWithResumeSteps: the MariaDB sibling.
func TestOne_MariaDB_resetMasterStopsWithResumeSteps(t *testing.T) {
	indexDB, indexName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)
	src := mariadbDupSource(t, 99928)
	runGTIDResetScenario(t, src, indexDB, indexName, func(int64) int { return 10 })
}
