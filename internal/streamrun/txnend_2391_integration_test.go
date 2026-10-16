//go:build integration

package streamrun

import (
	"context"
	"database/sql"
	"net"
	"strconv"
	"testing"
	"time"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	drivermysql "github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/observe"
	"github.com/dbtrail/dbtrail/internal/parser"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// #2391, first half: every transaction end is announced, and only a transaction
// end moves the position-mode resume point. The test server runs with GTIDs
// off by default, which is exactly the shape that used to announce nothing.

// TestIntegrationEveryTransactionEndIsAnnounced_2391 reads a real binlog: an
// InnoDB transaction of two statements (ends in an XID) and an autocommit write
// to a MyISAM table (ends in a QUERY "COMMIT"). Each must produce exactly one
// EventCommit, at the position the server wrote after it, and nothing in
// between may.
func TestIntegrationEveryTransactionEndIsAnnounced_2391(t *testing.T) {
	sourceDB, sourceName := testutil.CreateTestDB(t)
	var logBin string
	if err := sourceDB.QueryRow("SELECT @@log_bin").Scan(&logBin); err != nil || logBin != "1" {
		t.Skip("skipping: binary logging not enabled on test MySQL")
	}
	var gtidMode string
	if err := sourceDB.QueryRow("SELECT @@GLOBAL.gtid_mode").Scan(&gtidMode); err != nil {
		t.Fatalf("gtid_mode: %v", err)
	}
	testutil.MustExec(t, sourceDB, `CREATE TABLE orders (id INT PRIMARY KEY, amount INT NOT NULL) ENGINE=InnoDB`)
	testutil.MustExec(t, sourceDB, `CREATE TABLE notes (id INT PRIMARY KEY, body VARCHAR(20)) ENGINE=MyISAM`)

	startFile, startPos, err := config.CurrentBinlogPosition(sourceDB)
	if err != nil {
		t.Skipf("skipping: cannot read binlog position: %v", err)
	}

	tx, err := sourceDB.Begin()
	if err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{"INSERT INTO orders VALUES (1, 10)", "UPDATE orders SET amount = 20 WHERE id = 1"} {
		if _, err := tx.Exec(q); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	_, afterInnoDB, err := config.CurrentBinlogPosition(sourceDB)
	if err != nil {
		t.Fatal(err)
	}
	testutil.MustExec(t, sourceDB, `INSERT INTO notes VALUES (1, 'x')`)
	endFile, endPos, err := config.CurrentBinlogPosition(sourceDB)
	if err != nil {
		t.Fatal(err)
	}
	if endFile != startFile {
		t.Skipf("skipping: the binlog rotated during the test (%s -> %s)", startFile, endFile)
	}

	streamer := liveStreamer(t, sourceName, startFile, startPos)
	sp := parser.NewStreamParser(nil, parser.Filters{Schemas: map[string]bool{sourceName: true}}, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	out := make(chan parser.Event, 100)
	go func() { _ = sp.Run(ctx, streamer, out) }()

	var commits []parser.Event
	for len(commits) < 2 {
		select {
		case ev := <-out:
			if ev.EventType == parser.EventCommit {
				commits = append(commits, ev)
			}
		case <-ctx.Done():
			t.Fatalf("saw %d transaction ends before the timeout, want 2: %+v", len(commits), commits)
		}
	}
	if commits[0].EndPos != uint64(afterInnoDB) {
		t.Errorf("the InnoDB transaction's end is at %d, the server's position after it is %d", commits[0].EndPos, afterInnoDB)
	}
	if commits[1].EndPos != uint64(endPos) {
		t.Errorf("the MyISAM write's end is at %d, the server's position after it is %d", commits[1].EndPos, endPos)
	}
	if gtidMode == "OFF" {
		for i, c := range commits {
			if c.GTID != "" {
				t.Errorf("commit %d carries GTID %q on a source with GTIDs off", i, c.GTID)
			}
		}
	}
}

// TestIntegrationPositionResumeReplaysAnOpenTransactionWhole_2391 stops
// capture while a transaction is half read, part of it already in an index
// transaction, then resumes the way a restart does. The open transaction's
// rows are not kept, the checkpoint names the end of the last COMPLETE
// transaction, and the replay leaves each row exactly once.
func TestIntegrationPositionResumeReplaysAnOpenTransactionWhole_2391(t *testing.T) {
	idx, db := floorIndex(t, 2) // batches of two, so the open transaction's rows reach an index transaction

	// Transaction 1: rows 1 and 2, committed (its end is at 400).
	// Transaction 2: rows 4 and 5, read and written uncommitted, then capture stops.
	run1 := []parser.Event{floorEvent(1, ""), floorEvent(2, ""), floorCommit(3, ""), floorEvent(4, ""), floorEvent(5, "")}
	state := &streamState{mode: "position", serverID: 1, binlogFile: "binlog.000007", binlogPos: 100,
		safeFile: "binlog.000007", safePos: 100, flavor: gomysql.MySQLFlavor}
	if err := streamLoop(t.Context(), feedClosed(run1), idx, db, time.Hour, state,
		observe.ForSource("txnend-run1"), nil); err != nil {
		t.Fatalf("first run: %v", err)
	}
	if n := countRows(t, db); n != 2 {
		t.Fatalf("only the complete transaction's 2 rows may be kept, got %d", n)
	}
	saved := savedCheckpoint(t, db)
	if saved.binlogFile != "binlog.000007" || saved.binlogPos != 400 {
		t.Fatalf("the checkpoint must name the end of the last complete transaction, binlog.000007:400; got %s:%d",
			saved.binlogFile, saved.binlogPos)
	}

	// Restart: the cleanup a resume runs, then the source sends transaction 2
	// again from its start.
	if _, err := deleteEventsSinceCheckpoint(db, saved.binlogFile, saved.binlogPos, saved.dedupFloorID); err != nil {
		t.Fatalf("resume cleanup: %v", err)
	}
	run2 := []parser.Event{floorEvent(4, ""), floorEvent(5, ""), floorCommit(6, "")}
	resumed := &streamState{mode: "position", serverID: 1, binlogFile: saved.binlogFile, binlogPos: saved.binlogPos,
		safeFile: saved.binlogFile, safePos: saved.binlogPos, flavor: gomysql.MySQLFlavor,
		dedupFloorID: saved.dedupFloorID, eventsIndexed: saved.eventsIndexed}
	if err := streamLoop(t.Context(), feedClosed(run2), idx, db, time.Hour, resumed,
		observe.ForSource("txnend-run2"), nil); err != nil {
		t.Fatalf("resumed run: %v", err)
	}

	rows, err := db.Query(`SELECT pk_values, COUNT(*) FROM binlog_events GROUP BY pk_values ORDER BY pk_values`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	got := map[string]int{}
	for rows.Next() {
		var pk string
		var n int
		if err := rows.Scan(&pk, &n); err != nil {
			t.Fatal(err)
		}
		got[pk] = n
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	for _, pk := range []string{"1", "2", "4", "5"} {
		if got[pk] != 1 {
			t.Errorf("row %s is in the index %d times, want exactly once (all: %v)", pk, got[pk], got)
		}
	}
	if len(got) != 4 {
		t.Errorf("want rows 1, 2, 4 and 5 only, got %v", got)
	}
}

// countRows is the number of rows in binlog_events.
func countRows(t *testing.T, db *sql.DB) int {
	t.Helper()
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM binlog_events`).Scan(&n); err != nil {
		t.Fatalf("count binlog_events: %v", err)
	}
	return n
}

// liveStreamer opens a replication stream on the test server from file:pos,
// as the account the test DSN names. It skips when replication is not granted.
func liveStreamer(t *testing.T, dbName, file string, pos uint32) *replication.BinlogStreamer {
	t.Helper()
	mc, err := drivermysql.ParseDSN(testutil.IntegrationDSN(dbName))
	if err != nil {
		t.Fatalf("ParseDSN: %v", err)
	}
	host, portStr, err := net.SplitHostPort(mc.Addr)
	if err != nil {
		t.Fatalf("SplitHostPort: %v", err)
	}
	port, err := strconv.ParseUint(portStr, 10, 16)
	if err != nil {
		t.Fatalf("port: %v", err)
	}
	syncer := replication.NewBinlogSyncer(replication.BinlogSyncerConfig{
		ServerID: 99391, Flavor: "mysql", Host: host, Port: uint16(port), User: mc.User, Password: mc.Passwd,
	})
	t.Cleanup(syncer.Close)
	streamer, err := syncer.StartSync(gomysql.Position{Name: file, Pos: pos})
	if err != nil {
		t.Skipf("skipping: StartSync failed (replication may not be granted): %v", err)
	}
	return streamer
}
