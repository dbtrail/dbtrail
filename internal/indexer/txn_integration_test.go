//go:build integration

package indexer

import (
	"database/sql"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/parser"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

func txnRow(n int) parser.Event {
	return parser.Event{
		BinlogFile: "binlog.000001", StartPos: uint64(n * 100), EndPos: uint64(n*100 + 100),
		Timestamp: time.Date(2026, 2, 19, 14, 0, 0, 0, time.UTC),
		Schema:    "testdb", Table: "orders", EventType: parser.EventInsert,
		PKValues: strconv.Itoa(n), RowAfter: map[string]any{"id": int64(n)},
	}
}

func countEvents(t *testing.T, db *sql.DB) int {
	t.Helper()
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM binlog_events`).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	return n
}

// openTrxCount is the number of InnoDB transactions open on db's server for
// this test database's tables: a connection handed back to the pool inside a
// transaction shows up here.
func openTrxCount(t *testing.T, db *sql.DB) int {
	t.Helper()
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM information_schema.innodb_trx t
		JOIN information_schema.processlist p ON p.id = t.trx_mysql_thread_id
		WHERE p.db = DATABASE()`).Scan(&n); err != nil {
		t.Fatalf("innodb_trx: %v", err)
	}
	return n
}

// Batches written through a Txn are invisible to other connections until
// Commit, and all become visible at once.
func TestTxnCommitPublishesAllBatchesAtOnce(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	idx := New(db, 10)

	tx, err := idx.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	defer tx.Rollback()
	for _, b := range [][]parser.Event{{txnRow(1), txnRow(2)}, {txnRow(3)}} {
		if _, err := tx.InsertBatch(b); err != nil {
			t.Fatalf("InsertBatch: %v", err)
		}
		if n := countEvents(t, db); n != 0 {
			t.Fatalf("a reader sees %d rows of an uncommitted index transaction, want 0", n)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if n := countEvents(t, db); n != 3 {
		t.Fatalf("after Commit a reader sees %d rows, want 3", n)
	}
	if err := tx.Rollback(); err != nil {
		t.Errorf("Rollback after Commit must be a no-op, got %v", err)
	}
	if _, err := tx.InsertBatch([]parser.Event{txnRow(4)}); err == nil {
		t.Error("a spent Txn accepted an INSERT")
	}
}

// A failed INSERT inside a Txn, then Rollback: nothing the Txn wrote survives,
// and its connection does not go back to the pool still inside a transaction,
// where the next writer's statements would silently join it.
func TestTxnRollbackAfterAFailedInsertLeavesNoOpenTransaction(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	db.SetMaxOpenConns(2) // the Txn's connection plus one for the test's reads
	idx := New(db, 10)

	tx, err := idx.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if _, err := tx.InsertBatch([]parser.Event{txnRow(1)}); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}
	bad := txnRow(2)
	bad.Schema = strings.Repeat("s", 300) // longer than the column: strict mode refuses it
	if _, err := tx.InsertBatch([]parser.Event{bad}); err == nil {
		t.Fatal("the over-long schema name was accepted; the fixture cannot fail an INSERT")
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}
	if n := countEvents(t, db); n != 0 {
		t.Fatalf("%d rows survived the rollback, want 0", n)
	}
	if n := openTrxCount(t, db); n != 0 {
		t.Fatalf("%d transactions are still open on this database after Rollback", n)
	}
	// Every pooled connection must write in autocommit again.
	for i := range 2 {
		if _, err := idx.InsertBatch([]parser.Event{txnRow(10 + i)}); err != nil {
			t.Fatalf("InsertBatch after Rollback: %v", err)
		}
	}
	if n := openTrxCount(t, db); n != 0 {
		t.Fatalf("a pooled connection is inside a transaction after Rollback (%d open)", n)
	}
	if n := countEvents(t, db); n != 2 {
		t.Fatalf("want the two autocommit rows, got %d", n)
	}
}

// sessionsWithWaitTimeout counts this test database's sessions whose
// wait_timeout is secs.
func sessionsWithWaitTimeout(t *testing.T, db *sql.DB, secs int) int {
	t.Helper()
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM performance_schema.variables_by_thread v
		JOIN performance_schema.threads th ON th.thread_id = v.thread_id
		WHERE v.variable_name = 'wait_timeout' AND v.variable_value = ? AND th.processlist_db = DATABASE()`,
		strconv.Itoa(secs)).Scan(&n); err != nil {
		t.Fatalf("variables_by_thread: %v", err)
	}
	return n
}

// The connection holding an index transaction gets a short idle timeout, so a
// client cut off by the network does not leave the transaction open on the
// server for its default eight hours, and gets the server's back after Commit.
func TestTxnBoundsItsConnectionsIdleLife(t *testing.T) {
	db, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	idx := New(db, 10)
	tx, err := idx.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	defer tx.Rollback()
	if n := sessionsWithWaitTimeout(t, db, txnIdleSeconds); n != 1 {
		t.Fatalf("want the transaction's connection at wait_timeout=%d, found %d such sessions", txnIdleSeconds, n)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if n := sessionsWithWaitTimeout(t, db, txnIdleSeconds); n != 0 {
		t.Fatalf("after Commit %d pooled sessions still carry the short wait_timeout", n)
	}
}
