//go:build integration

package streamrun

import (
	"database/sql"
	"errors"
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
		mariadbResumeAfterReset(t, src, indexDB, cfg(2), cfg(3), last)
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
func mariadbResumeAfterReset(t *testing.T, src dupSource, indexDB *sql.DB, restart, resume Config, last int) {
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
	if at, detail := gapLost(t, indexDB); !at.Valid {
		t.Error("resuming with the error's steps stamped no capture loss")
	} else {
		t.Logf("run 4 stamped gap_lost_at=%v detail=%q", at, detail)
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

// TestOne_MariaDB_resetMasterStopsWithResumeSteps: the MariaDB sibling.
func TestOne_MariaDB_resetMasterStopsWithResumeSteps(t *testing.T) {
	indexDB, indexName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)
	src := mariadbDupSource(t, 99928)
	runGTIDResetScenario(t, src, indexDB, indexName, "", func(int64) int { return 10 })
}
