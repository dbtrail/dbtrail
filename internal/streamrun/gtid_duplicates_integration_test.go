//go:build integration

package streamrun

import (
	"bytes"
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"

	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// Two ways a GTID-mode capture used to index rows twice, each driven through
// the real One entry point against a real server, MySQL and MariaDB:
//
//  1. Reconnect. go-mysql resumes a dropped connection from the GTID set it
//     held BEFORE the last transaction it read, so the source re-sends that
//     transaction. The test kills the source's binlog dump thread right after
//     a commit and checks every row is indexed once.
//  2. Crash before the first file-bearing checkpoint. A fresh GTID start used
//     to checkpoint binlog_file = '' until its first row event, and both
//     resume cleanups skip an empty file. The test restores the first
//     checkpoint after rows were indexed (what a crash between the first
//     batch and the next checkpoint leaves behind), restarts, and checks every
//     row is indexed once.
//
// The MariaDB cases are named *MariaDB* for the MariaDB-source CI job; the
// MySQL ones avoid that word and "Flavor" so they run only in the MySQL job.

// dupSource is one GTID source under test.
type dupSource struct {
	db       *sql.DB
	schema   string
	dsn      string
	flavor   string
	serverID uint32
	// indexDSNSuffix is appended to the index DSN. On MySQL the index lives
	// on the source server, so its own writes are GTID transactions in the
	// same binlog; "&sql_log_bin=0" keeps them out, so the last transaction
	// before a reconnect is a captured row and not an index write.
	indexDSNSuffix string
}

func (s dupSource) config(indexName string) Config {
	cfg := flavorTestConfig(indexName, s.dsn, s.schema, "", s.serverID)
	cfg.IndexDSN += s.indexDSNSuffix
	cfg.BatchSize = 1
	return cfg
}

// executedSet reads the source's executed GTID set in its own dialect.
func (s dupSource) executedSet(t *testing.T) string {
	t.Helper()
	var q string
	if s.flavor == gomysql.MariaDBFlavor {
		q = "SELECT @@GLOBAL.gtid_binlog_pos"
	} else {
		q = "SELECT @@GLOBAL.gtid_executed"
	}
	var v string
	if err := s.db.QueryRow(q).Scan(&v); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	return strings.Join(strings.Fields(v), "")
}

// checkpointReached reports whether the durable GTID checkpoint contains want.
func (s dupSource) checkpointReached(t *testing.T, indexDB *sql.DB, want string) bool {
	t.Helper()
	st, err := loadStreamState(indexDB)
	if err != nil {
		t.Fatalf("poll stream_state: %v", err)
	}
	if st == nil || st.mode != "gtid" || want == "" {
		return false
	}
	if s.flavor == gomysql.MariaDBFlavor {
		cp, err := parseMariadbSet(st.gtidSet)
		if err != nil {
			return false
		}
		w, err := parseMariadbSet(want)
		if err != nil {
			t.Fatalf("parse %q: %v", want, err)
		}
		return mariadbCheckpointCoversFloor(cp, w)
	}
	cp, err := parseGTIDSetForFlavor(s.flavor, NormalizeGTIDSet(st.gtidSet))
	if err != nil {
		return false
	}
	w, err := parseGTIDSetForFlavor(s.flavor, NormalizeGTIDSet(want))
	if err != nil {
		t.Fatalf("parse %q: %v", want, err)
	}
	return cp.Contain(w)
}

func (s dupSource) insert(t *testing.T, n int) {
	t.Helper()
	for range n {
		testutil.MustExec(t, s.db, "INSERT INTO orders (amount) VALUES (1)")
	}
}

// dumpThreads lists the source's binlog dump threads (MySQL names them
// "Binlog Dump GTID" in GTID mode, MariaDB "Binlog Dump").
func dumpThreads(t *testing.T, db *sql.DB) []int64 {
	t.Helper()
	rows, err := db.Query("SELECT ID FROM information_schema.PROCESSLIST WHERE COMMAND LIKE 'Binlog Dump%'")
	if err != nil {
		t.Fatalf("list dump threads: %v", err)
	}
	defer rows.Close()
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("scan dump thread: %v", err)
		}
		ids = append(ids, id)
	}
	return ids
}

// lockedBuffer is a bytes.Buffer safe for the log writes of several goroutines.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// teeLogs tees the default slog logger into a buffer for the rest of the
// test. The stream parser takes slog.Default() when One builds it, so this
// must run before One starts.
func teeLogs(t *testing.T) *lockedBuffer {
	t.Helper()
	buf := &lockedBuffer{}
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(io.MultiWriter(buf, os.Stderr), nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return buf
}

// resendDropped is the log line the re-send guard writes when it acts.
const resendDropped = "dropping a transaction the source re-sent after a reconnect"

// runReconnectAfterCommit: rows 1-2 committed and checkpointed, the dump
// thread killed, rows 3-4 written after go-mysql reconnected on its own.
func runReconnectAfterCommit(t *testing.T, src dupSource, indexDB *sql.DB, indexName string) {
	t.Helper()
	logs := teeLogs(t)
	cfg := src.config(indexName)
	cfg.Checkpoint = 1
	killed := false
	// The target set is read once, right after the last write. Re-reading it
	// while polling would chase the index's own checkpoint writes when the
	// index lives on the source server (MySQL here).
	var wantAfter string
	if err := runOneUntil(t, cfg, true,
		func() {
			src.insert(t, 2)
			want := src.executedSet(t)
			deadline := time.Now().Add(60 * time.Second)
			for !src.checkpointReached(t, indexDB, want) {
				if time.Now().After(deadline) {
					t.Fatalf("checkpoint never reached %s", want)
				}
				time.Sleep(50 * time.Millisecond)
			}
			// Right after the commit was captured: drop the connection.
			before := dumpThreads(t, src.db)
			if len(before) == 0 {
				t.Fatal("no binlog dump thread on the source: the capture is not connected")
			}
			for _, id := range before {
				if _, err := src.db.Exec(fmt.Sprintf("KILL %d", id)); err != nil {
					t.Fatalf("KILL %d: %v", id, err)
				}
			}
			killed = true
			// Wait for go-mysql to reconnect: a dump thread that was not there before.
			for deadline := time.Now().Add(60 * time.Second); ; {
				now := dumpThreads(t, src.db)
				fresh := false
				for _, id := range now {
					isOld := false
					for _, old := range before {
						isOld = isOld || id == old
					}
					fresh = fresh || !isOld
				}
				if fresh {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("the capture did not reconnect within 60s")
				}
				time.Sleep(100 * time.Millisecond)
			}
			// Give the re-sent transaction time to arrive before more writes.
			time.Sleep(2 * time.Second)
			src.insert(t, 2)
			wantAfter = src.executedSet(t)
		},
		func() bool {
			return indexedOrders(t, indexDB, src.schema) >= 4 && src.checkpointReached(t, indexDB, wantAfter)
		},
	); err != nil {
		t.Fatalf("One: %v", err)
	}
	if !killed {
		t.Fatal("the dump thread was never killed")
	}
	// The done condition waited for the checkpoint to reach the source past
	// rows 3-4, so a re-sent copy of row 2 (which precedes them) has landed.
	assertExactlyOnce(t, indexedPKs(t, indexDB, src.schema, "orders"), pkRange(1, 4))
	// Without this the test could pass with no re-send at all (another
	// transaction landing between row 2 and the KILL would be the one
	// re-sent): the guard must have acted, exactly once.
	if n := strings.Count(logs.String(), resendDropped); n != 1 {
		t.Errorf("the re-send guard acted %d times, want exactly 1: the scenario did not produce the re-send under test", n)
	}
}

// runCrashBeforeFirstFileCheckpoint: the first checkpoint of a fresh GTID
// start is saved aside, rows are indexed, the capture stops, the saved
// checkpoint is put back (a crash before the next checkpoint), and the
// restart must not index those rows twice.
func runCrashBeforeFirstFileCheckpoint(t *testing.T, src dupSource, indexDB *sql.DB, indexName string) {
	t.Helper()
	type cp struct {
		mode, file string
		pos        uint64
		gtid       sql.NullString
	}
	var first cp
	cfg := src.config(indexName)
	cfg.Checkpoint = 1 // the first tick fires before any event: that is the checkpoint under test
	if err := runOneUntil(t, cfg, true,
		func() {
			if err := indexDB.QueryRow(`SELECT mode, binlog_file, binlog_position, gtid_set FROM stream_state WHERE id = 1`).
				Scan(&first.mode, &first.file, &first.pos, &first.gtid); err != nil {
				t.Fatalf("read the first checkpoint: %v", err)
			}
			src.insert(t, 3)
		},
		func() bool { return indexedOrders(t, indexDB, src.schema) >= 3 },
	); err != nil {
		t.Fatalf("run 1: %v", err)
	}
	if first.mode != "gtid" {
		t.Fatalf("first checkpoint mode = %q, want gtid", first.mode)
	}
	if first.file == "" {
		t.Errorf("the first checkpoint of a fresh GTID start has no binlog_file: resume cleanup cannot run")
	}
	assertExactlyOnce(t, indexedPKs(t, indexDB, src.schema, "orders"), pkRange(1, 3))

	// The crash: the durable checkpoint is still the first one.
	testutil.MustExec(t, indexDB, `UPDATE stream_state SET mode = ?, binlog_file = ?, binlog_position = ?, gtid_set = ?,
		dedup_floor_event_id = NULL WHERE id = 1`, first.mode, first.file, first.pos, first.gtid)

	src.insert(t, 1)
	want := src.executedSet(t)
	cfg2 := src.config(indexName)
	cfg2.Checkpoint = 1
	if err := runOneUntil(t, cfg2, false, nil, func() bool {
		return indexedOrders(t, indexDB, src.schema) >= 4 && src.checkpointReached(t, indexDB, want)
	}); err != nil {
		t.Fatalf("run 2 (restart after the crash): %v", err)
	}
	assertExactlyOnce(t, indexedPKs(t, indexDB, src.schema, "orders"), pkRange(1, 4))
}

func mariadbDupSource(t *testing.T, serverID uint32) dupSource {
	t.Helper()
	db, schema := mariadbFlavorSource(t)
	return dupSource{db: db, schema: schema, dsn: mariadbSourceDSN(schema), flavor: gomysql.MariaDBFlavor, serverID: serverID}
}

func mysqlGTIDDupSource(t *testing.T, serverID uint32) dupSource {
	t.Helper()
	db, schema := testutil.CreateTestDB(t)
	var logBin string
	if err := db.QueryRow("SELECT @@log_bin").Scan(&logBin); err != nil || logBin != "1" {
		t.Skip("skipping: binary logging not enabled on test MySQL")
	}
	stepGTIDModeOn(t, db)
	testutil.MustExec(t, db, `CREATE TABLE orders (
		id     INT PRIMARY KEY AUTO_INCREMENT,
		amount DECIMAL(10,2) NOT NULL
	)`)
	return dupSource{db: db, schema: schema, dsn: testutil.IntegrationDSN(schema), flavor: gomysql.MySQLFlavor, serverID: serverID,
		indexDSNSuffix: "&sql_log_bin=0"}
}

func TestOne_MariaDB_reconnectAfterCommitIndexesOnce(t *testing.T) {
	indexDB, indexName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)
	runReconnectAfterCommit(t, mariadbDupSource(t, 99960), indexDB, indexName)
}

func TestIntegrationGTIDReconnectAfterCommitIndexesOnce(t *testing.T) {
	indexDB, indexName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)
	runReconnectAfterCommit(t, mysqlGTIDDupSource(t, 99961), indexDB, indexName)
}

func TestOne_MariaDB_crashBeforeFirstFileCheckpointIndexesOnce(t *testing.T) {
	indexDB, indexName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)
	runCrashBeforeFirstFileCheckpoint(t, mariadbDupSource(t, 99962), indexDB, indexName)
}

func TestIntegrationGTIDCrashBeforeFirstFileCheckpointIndexesOnce(t *testing.T) {
	indexDB, indexName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)
	runCrashBeforeFirstFileCheckpoint(t, mysqlGTIDDupSource(t, 99963), indexDB, indexName)
}

// runResetKeepsOlderHistory: an index that already holds history is taken over
// by a --reset (a fresh start), which now lands in GTID mode with binlog
// coordinates in its first checkpoint. The restart's cleanup must touch only
// rows this capture wrote: not history under a file name that sorts after the
// new one, and not history in the same file whose GTID the new set lacks.
func runResetKeepsOlderHistory(t *testing.T, src dupSource, indexDB *sql.DB, indexName, foreignGTID string) {
	t.Helper()
	file, pos, err := config.CurrentBinlogPosition(src.db)
	if err != nil {
		t.Fatalf("CurrentBinlogPosition: %v", err)
	}
	if _, err := metadata.TakeSnapshot(src.db, indexDB, []string{src.schema}); err != nil {
		t.Fatalf("TakeSnapshot: %v", err)
	}
	testutil.MustExec(t, indexDB, `INSERT INTO stream_state
		(id, mode, binlog_file, binlog_position, flavor, last_checkpoint, server_id)
		VALUES (1, 'position', ?, ?, ?, UTC_TIMESTAMP(), ?)`, file, pos, src.flavor, src.serverID)
	testutil.MustExec(t, indexDB, `INSERT INTO binlog_events
		(binlog_file, start_pos, end_pos, event_timestamp, gtid, schema_name, table_name, event_type, pk_values, row_after) VALUES
		('zzzzzzzzzzzzzzzz-bin.000009', 100, 200, UTC_TIMESTAMP(), NULL, 'history', 'orders', 1, '1', '{"id":1}'),
		(?, 1, 2, UTC_TIMESTAMP(), ?, 'history', 'orders', 1, '2', '{"id":2}')`, file, foreignGTID)
	history := func() int {
		var n int
		if err := indexDB.QueryRow(`SELECT COUNT(*) FROM binlog_events WHERE schema_name = 'history'`).Scan(&n); err != nil {
			t.Fatalf("count history: %v", err)
		}
		return n
	}
	if history() != 2 {
		t.Fatal("history fixture not written")
	}

	cfg := src.config(indexName)
	cfg.Reset = true
	if err := runOneUntil(t, cfg, true, nil, func() bool { return true }); err != nil {
		t.Fatalf("run 1 (--reset): %v", err)
	}
	if st, _ := loadStreamState(indexDB); st == nil || st.mode != "gtid" || st.binlogFile == "" {
		t.Fatalf("after --reset the checkpoint is %+v, want GTID mode with binlog coordinates", st)
	}
	// The restart runs the resume cleanup before it attaches.
	if err := runOneUntil(t, src.config(indexName), true, nil, func() bool { return true }); err != nil {
		t.Fatalf("run 2 (restart): %v", err)
	}
	if n := history(); n != 2 {
		t.Errorf("the restart's cleanup deleted %d of 2 history rows it never wrote", 2-n)
	}
}

func TestOne_MariaDB_resetKeepsOlderHistory(t *testing.T) {
	indexDB, indexName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)
	runResetKeepsOlderHistory(t, mariadbDupSource(t, 99964), indexDB, indexName, "9-9-2")
}

func TestIntegrationGTIDResetKeepsOlderHistory(t *testing.T) {
	indexDB, indexName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)
	runResetKeepsOlderHistory(t, mysqlGTIDDupSource(t, 99965), indexDB, indexName, "3e11fa47-71ca-11e1-9e33-c80aa9429562:77")
}

// cutRestartLogged is the log line One writes when it restarts after the
// parser stopped on a transaction cut by a disconnect.
const cutRestartLogged = "restarting capture from its last checkpoint so a transaction cut by a disconnect is indexed once"

// runCutMidTransaction: one large transaction (many single-row statements, so
// many row events) is committed; while its rows are still arriving, the
// source's dump thread is killed. The source re-sends the whole transaction
// after go-mysql reconnects. Every row must end up indexed exactly once, and
// the restart path must be the one that got it there.
//
// The transaction's size is what makes the kill a cut. The kill is sent when
// the first row shows up in the index, and by then the source may already have
// written everything that fits between it and the indexer: go-mysql's queue of
// 10240 binlog events (two or three per single-row statement, so about 5000
// rows), the parser's channel of 1000 rows, one batch, and the TCP buffers on
// the way (up to 6 MiB to receive and 4 MiB to send per socket by Linux's
// defaults, twice over behind Docker's port proxy: about 20 MiB). A
// transaction that fits in that was sent whole before the kill, nothing is
// cut, and the run proves nothing: 20000 rows of 1.2 kB (24 MiB) did fit, on
// some CI runs. 30000 rows of 4 kB are about 120 MiB against the 45 MiB those
// hold at this row size (6100 rows, 25 MiB, plus the 20 MiB of buffers).
// Single-row statements on purpose: go-mysql's queue counts events, not bytes.
func runCutMidTransaction(t *testing.T, src dupSource, indexDB *sql.DB, indexName string) {
	t.Helper()
	const n = 30000
	logs := teeLogs(t)
	testutil.MustExec(t, src.db, "ALTER TABLE orders ADD COLUMN pad VARCHAR(4000) NOT NULL DEFAULT ''")

	cfg := src.config(indexName)
	cfg.BatchSize = 100
	cfg.Checkpoint = 1
	var want string
	if err := runOneUntil(t, cfg, true,
		func() {
			tx, err := src.db.Begin()
			if err != nil {
				t.Fatalf("begin: %v", err)
			}
			stmt, err := tx.Prepare("INSERT INTO orders (amount, pad) VALUES (1, REPEAT('x', 4000))")
			if err != nil {
				t.Fatalf("prepare: %v", err)
			}
			for range n {
				if _, err := stmt.Exec(); err != nil {
					t.Fatalf("insert: %v", err)
				}
			}
			stmt.Close()
			if err := tx.Commit(); err != nil {
				t.Fatalf("commit: %v", err)
			}
			want = src.executedSet(t)
			// Kill while the transaction's rows are still arriving.
			for deadline := time.Now().Add(60 * time.Second); indexedOrders(t, indexDB, src.schema) == 0; {
				if time.Now().After(deadline) {
					t.Fatal("no row of the transaction was indexed within 60s")
				}
				time.Sleep(5 * time.Millisecond)
			}
			if got := indexedOrders(t, indexDB, src.schema); got >= n {
				t.Fatalf("the whole transaction (%d rows) was indexed before the kill; the scenario needs a cut", got)
			}
			for _, id := range dumpThreads(t, src.db) {
				if _, err := src.db.Exec(fmt.Sprintf("KILL %d", id)); err != nil {
					t.Fatalf("KILL %d: %v", id, err)
				}
			}
		},
		func() bool {
			return indexedOrders(t, indexDB, src.schema) >= n && src.checkpointReached(t, indexDB, want)
		},
	); err != nil {
		t.Fatalf("One: %v", err)
	}
	if !strings.Contains(logs.String(), cutRestartLogged) {
		t.Errorf("capture never restarted on a cut transaction: the kill did not cut the transaction, so this run proved nothing")
	}
	assertExactlyOnce(t, indexedPKs(t, indexDB, src.schema, "orders"), pkRange(1, n))
}

func TestOne_MariaDB_cutMidTransactionIndexesOnce(t *testing.T) {
	indexDB, indexName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)
	runCutMidTransaction(t, mariadbDupSource(t, 99966), indexDB, indexName)
}

func TestIntegrationGTIDCutMidTransactionIndexesOnce(t *testing.T) {
	indexDB, indexName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)
	runCutMidTransaction(t, mysqlGTIDDupSource(t, 99967), indexDB, indexName)
}

// runFreshStartCheckpointsAtOnce: a run with no checkpoint writes its start
// point before it reads any event, not at the first ticker tick. Until then a
// restart would find no checkpoint and start over at the source's CURRENT
// position, skipping what was sent in between. The ticker is set to an hour,
// so only the start checkpoint can satisfy this.
func runFreshStartCheckpointsAtOnce(t *testing.T, src dupSource, indexDB *sql.DB, indexName string) {
	t.Helper()
	cfg := src.config(indexName)
	cfg.Checkpoint = 3600
	if err := runOneUntil(t, cfg, false, nil, func() bool {
		st, err := loadStreamState(indexDB)
		if err != nil {
			t.Fatalf("poll stream_state: %v", err)
		}
		return st != nil
	}); err != nil {
		t.Fatalf("One: %v", err)
	}
}

func TestOne_MariaDB_freshStartCheckpointsAtOnce(t *testing.T) {
	indexDB, indexName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)
	runFreshStartCheckpointsAtOnce(t, mariadbDupSource(t, 99968), indexDB, indexName)
}

func TestIntegrationGTIDFreshStartCheckpointsAtOnce(t *testing.T) {
	indexDB, indexName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)
	runFreshStartCheckpointsAtOnce(t, mysqlGTIDDupSource(t, 99969), indexDB, indexName)
}
