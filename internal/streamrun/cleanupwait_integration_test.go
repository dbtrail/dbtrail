//go:build integration

package streamrun

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	drivermysql "github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestIntegrationStartWaitsForAnEarlierCleanup is #1708 against a real server.
//
// The earlier cleanup is the REAL statement, sent by the real function on its
// own connection, and kept executing by a third connection that holds a lock
// on the rows it wants. That is what a restart leaves behind: a DELETE that
// runs on in the server with nobody on the client side.
//
// Every connection that matters logs in as a user with rights on the index
// database only. No PROCESS privilege, no grant on performance_schema: what
// the look can see as that user is the question the test answers.
func TestIntegrationStartWaitsForAnEarlierCleanup(t *testing.T) {
	root, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, root)
	for i := range 5 {
		testutil.InsertEvent(t, root, "binlog.000042", uint64(5000+100*i), uint64(5100+100*i),
			"2026-02-19 14:00:00", nil, "shop", "orders", 1, fmt.Sprint(i), nil, nil, []byte(`{"id":1}`))
	}

	user := "w1708_" + dbName
	if len(user) > 32 {
		user = user[:32]
	}
	testutil.MustExec(t, root, "DROP USER IF EXISTS '"+user+"'@'%'")
	testutil.MustExec(t, root, "CREATE USER '"+user+"'@'%' IDENTIFIED BY 'w1708pass'")
	testutil.MustExec(t, root, "GRANT ALL PRIVILEGES ON `"+dbName+"`.* TO '"+user+"'@'%'")
	t.Cleanup(func() { root.Exec("DROP USER IF EXISTS '" + user + "'@'%'") })

	base, err := drivermysql.ParseDSN(testutil.IntegrationDSN(dbName))
	if err != nil {
		t.Fatal(err)
	}
	open := func(lockWait int) *sql.DB {
		c := base.Clone()
		c.User, c.Passwd = user, "w1708pass"
		c.Params = map[string]string{"innodb_lock_wait_timeout": fmt.Sprint(lockWait)}
		db, err := sql.Open("mysql", c.FormatDSN())
		if err != nil {
			t.Fatal(err)
		}
		if err := db.Ping(); err != nil {
			t.Fatalf("connect as the index user: %v", err)
		}
		t.Cleanup(func() { db.Close() })
		return db
	}

	// The lock holder: what keeps the earlier cleanup executing.
	holder, err := root.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer holder.Close()
	if _, err := holder.ExecContext(context.Background(), "BEGIN"); err != nil {
		t.Fatal(err)
	}
	if _, err := holder.ExecContext(context.Background(), "UPDATE binlog_events SET end_pos = end_pos WHERE binlog_file = 'binlog.000042'"); err != nil {
		t.Fatal(err)
	}
	released := false
	release := func() {
		if !released {
			released = true
			holder.ExecContext(context.Background(), "ROLLBACK")
		}
	}
	defer release()

	// The earlier cleanup.
	earlier := open(120)
	earlierDone := make(chan error, 1)
	go func() {
		_, err := deleteEventsSinceCheckpoint(earlier, "binlog.000042", 5000, 1)
		earlierDone <- err
	}()

	// What a start did before #1708: send a second cleanup, which waits on the
	// same locks and fails at the lock timeout. Pinned here because what the
	// screen may promise depends on the class of this error: it is NOT a write
	// deadline, so the main source's restart loop does not take it.
	blunt := open(1)
	waitForProcess(t, blunt, 1)
	_, err = deleteEventsSinceCheckpoint(blunt, "binlog.000042", 5000, 1)
	var myErr *drivermysql.MySQLError
	if !errors.As(err, &myErr) || myErr.Number != 1205 {
		t.Fatalf("a second cleanup did not fail on the lock timeout: %v", err)
	}
	if errors.Is(err, indexer.ErrWriteDeadline) {
		t.Errorf("the lock timeout is tagged as a write deadline: %v", err)
	}
	if errors.Is(err, ErrEarlierCleanupRunning) {
		t.Errorf("the raw lock timeout carries the new sentinel: %v", err)
	}

	// What a start does now.
	starting := open(1)
	var log phaseLog
	waitDone := make(chan error, 1)
	go func() {
		waitDone <- waitForEarlierCleanup(context.Background(), processListProbe(starting), log.hooks(), time.Minute, 50*time.Millisecond)
	}()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if phase, _, _ := log.last(); phase == PhaseResumeCleanupWaiting {
			break
		}
		select {
		case err := <-waitDone:
			t.Fatalf("the start did not wait for the earlier cleanup (returned %v)", err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("the start never reported that it is waiting")
		}
		time.Sleep(10 * time.Millisecond)
	}
	_, detail, _ := log.last()
	var earlierID uint64
	if err := root.QueryRow("SELECT ID FROM information_schema.PROCESSLIST WHERE USER = ? AND INFO LIKE 'DELETE FROM binlog_events%'", user).Scan(&earlierID); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(detail, fmt.Sprintf("connection %d, running for ", earlierID)) {
		t.Errorf("the wait reports %q, want connection %d", detail, earlierID)
	}
	t.Logf("while waiting: %s", detail)

	// It keeps waiting for as long as the earlier cleanup runs.
	select {
	case err := <-waitDone:
		t.Fatalf("the wait ended while the earlier cleanup was still running: %v", err)
	case <-time.After(500 * time.Millisecond):
	}

	release()
	if err := <-earlierDone; err != nil {
		t.Fatalf("the earlier cleanup: %v", err)
	}
	select {
	case err := <-waitDone:
		if err != nil {
			t.Fatalf("the wait failed after the earlier cleanup finished: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the earlier cleanup finished and the start is still waiting")
	}
	if phase, _, _ := log.last(); phase != "" {
		t.Errorf("the phase outlived the wait: %q", phase)
	}
	// And this run's own cleanup now goes through.
	if _, err := deleteEventsSinceCheckpoint(starting, "binlog.000042", 5000, 1); err != nil {
		t.Errorf("this run's cleanup after the wait: %v", err)
	}
}

// waitForProcess blocks until the index user sees n cleanups in the list.
func waitForProcess(t *testing.T, db *sql.DB, n int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		rows, err := processListProbe(db)(context.Background())
		if err != nil {
			t.Fatalf("the index user cannot read the process list: %v", err)
		}
		found, err := earlierCleanups(rows)
		if err != nil {
			t.Fatal(err)
		}
		if len(found) >= n {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the index user sees %d cleanups in the process list, want %d; rows: %+v", len(found), n, rows)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
