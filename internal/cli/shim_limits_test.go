package cli

import (
	"context"
	"database/sql"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	mysqldrv "github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/shim"
)

// TestServeLoopMaxConnectionsRefusesWith1040 drives the #823 connection
// cap over real wire bytes: with the single slot held by an idle raw
// TCP connection, a full MySQL client must be refused with error 1040
// "Too many connections" (the ERR-instead-of-handshake packet a real
// mysqld sends), and closing the idle connection must free the slot.
func TestServeLoopMaxConnectionsRefusesWith1040(t *testing.T) {
	addr := startTestShimFull(t, map[string]string{"tenant_a": "pw"},
		nil, shim.Config{}, nil, 1)

	c1, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c1.Close()
	// Reading the server greeting proves handleConn holds the slot.
	one := make([]byte, 1)
	_ = c1.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := c1.Read(one); err != nil {
		t.Fatalf("read handshake greeting on the first connection: %v", err)
	}

	err = pingWithUser(t, addr, "tenant_a", "pw")
	var myErr *mysqldrv.MySQLError
	if !errors.As(err, &myErr) || myErr.Number != 1040 {
		t.Fatalf("second connection: want MySQL error 1040, got %v", err)
	}
	if !strings.Contains(myErr.Message, "Too many connections") {
		t.Errorf("refusal message = %q, want it to say 'Too many connections'", myErr.Message)
	}

	// Freeing the slot readmits clients (poll: the release happens
	// when handleConn returns, asynchronously after the close).
	c1.Close()
	deadline := time.Now().Add(3 * time.Second)
	for {
		if err := pingWithUser(t, addr, "tenant_a", "pw"); err == nil {
			break
		} else if time.Now().After(deadline) {
			t.Fatalf("slot not freed within 3s of closing the first connection: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// expectShimFetch registers the two index queries a NoArchive
// _flashback full-table resolution performs, delaying the row fetch so
// the query is reliably in flight when the test interferes with it.
func expectShimFetch(mock sqlmock.Sqlmock, delay time.Duration) {
	mock.MatchExpectationsInOrder(false)
	mock.ExpectQuery("information_schema.PARTITIONS").
		WillReturnRows(sqlmock.NewRows([]string{"PARTITION_NAME", "PARTITION_DESCRIPTION"}))
	mock.ExpectQuery("FROM binlog_events").
		WillDelayFor(delay).
		WillReturnRows(sqlmock.NewRows([]string{"event_id"}))
}

// TestShim_QueryTimeoutWireError is the end-to-end proof of
// --query-timeout (#823): a real MySQL client running a time-travel
// query whose index fetch outlives the configured deadline must receive
// wire error 1317 within the deadline's order of magnitude — not block
// for the fetch's full duration.
func TestShim_QueryTimeoutWireError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	expectShimFetch(mock, 30*time.Second)

	cfg := shim.Config{
		AllowGaps: true, NoArchive: true, IndexDBName: "bintrail_index",
		QueryTimeout: 300 * time.Millisecond,
	}
	addr := startTestShimFull(t, map[string]string{"tenant_a": "pw"},
		map[string]string{"tenant_a": "myapp"}, cfg, db, 0)

	client, err := sql.Open("mysql", "tenant_a:pw@tcp("+addr+")/?timeout=2s&readTimeout=10s")
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	start := time.Now()
	_, qerr := client.Query("SELECT * FROM _flashback.orders AS OF '2026-01-01 00:00:00'")
	elapsed := time.Since(start)

	var myErr *mysqldrv.MySQLError
	if !errors.As(qerr, &myErr) || myErr.Number != 1317 {
		t.Fatalf("want MySQL error 1317 (query interrupted), got %v", qerr)
	}
	if !strings.Contains(myErr.Message, "query-timeout") {
		t.Errorf("wire message = %q, want a pointer at --query-timeout", myErr.Message)
	}
	if elapsed > 5*time.Second {
		t.Errorf("client got the error after %v; the 300ms deadline should have reaped the query", elapsed)
	}
}

// TestShim_ClientDisconnectCancelsInFlightFetch is the end-to-end proof
// of the #823 disconnect pump over real wire bytes, with NO query
// timeout configured — isolating disconnect detection. The client
// abandons a query whose index fetch is delayed 30s; the shim must
// abort the server-side fetch promptly (observed via the index pool's
// in-use connection count) instead of letting the orphaned fetch run to
// completion. Fails on the pre-#823 code: nothing observed the closed
// socket, so the fetch stayed in flight for the full delay.
func TestShim_ClientDisconnectCancelsInFlightFetch(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	expectShimFetch(mock, 30*time.Second)

	cfg := shim.Config{AllowGaps: true, NoArchive: true, IndexDBName: "bintrail_index"}
	addr := startTestShimFull(t, map[string]string{"tenant_a": "pw"},
		map[string]string{"tenant_a": "myapp"}, cfg, db, 0)

	client, err := sql.Open("mysql", "tenant_a:pw@tcp("+addr+")/?timeout=2s&readTimeout=60s")
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	qctx, qcancel := context.WithCancel(context.Background())
	defer qcancel()
	qdone := make(chan struct{})
	go func() {
		defer close(qdone)
		_, _ = client.QueryContext(qctx, "SELECT * FROM _flashback.orders AS OF '2026-01-01 00:00:00'")
	}()

	// Wait until the delayed fetch is in flight server-side: sqlmock
	// marks an expectation fulfilled at MATCH time, before serving its
	// delay, so all-expectations-met == the 30s fetch has started.
	matched := time.Now().Add(5 * time.Second)
	for mock.ExpectationsWereMet() != nil {
		if time.Now().After(matched) {
			t.Fatal("index fetch never started")
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Abandon the query: go-sql-driver's context watcher closes the
	// client's TCP connection, which is exactly what a timed-out ORM
	// does. The shim's pump must see the close and cancel the fetch.
	qcancel()
	<-qdone

	freed := time.Now().Add(5 * time.Second)
	for db.Stats().InUse != 0 {
		if time.Now().After(freed) {
			t.Fatalf("server-side fetch still holds %d index connection(s) 5s after the client disconnected; "+
				"disconnect did not cancel the in-flight query", db.Stats().InUse)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestShim_ShutdownCancelsInFlightFetch is the end-to-end proof of the
// #823 daemon-shutdown path, as distinct from the two siblings above:
// TestShim_QueryTimeoutWireError covers --query-timeout and
// TestShim_ClientDisconnectCancelsInFlightFetch covers a client-initiated
// close, but neither cancels serveLoop's own ctx (the SIGTERM path)
// while a query is genuinely in flight inside handleConn. This test
// drives serveLoop directly (bypassing startTestShimFull, whose
// t.Cleanup only cancels ctx after the test body — too late to observe
// the in-flight case) so it can cancel the parent ctx itself, with the
// client context left live throughout. It proves both halves of the
// handleConn contract: the connection's own goroutine returns (so
// wg.Wait in serveLoop unblocks — asserted via done closing) and the
// abandoned index fetch is actually aborted, not merely orphaned
// (asserted via db.Stats().InUse dropping back to 0). Fails on a
// regression that reverts handleConn to ignoring ctx (pre-#823): done
// never closes and this test times out instead of passing quickly.
func TestShim_ShutdownCancelsInFlightFetch(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	expectShimFetch(mock, 30*time.Second)

	cfg := shim.Config{AllowGaps: true, NoArchive: true, IndexDBName: "bintrail_index"}
	auth, err := shim.NewTenantAuth(map[string]string{"tenant_a": "pw"})
	if err != nil {
		t.Fatalf("NewTenantAuth: %v", err)
	}
	srv, err := shim.NewMySQLServer(cfg.AuthMethod)
	if err != nil {
		t.Fatalf("NewMySQLServer: %v", err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer listener.Close()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		serveLoop(ctx, listener, db, srv, auth, cfg,
			map[string]string{"tenant_a": "myapp"}, nil, 0)
	}()

	client, err := sql.Open("mysql", "tenant_a:pw@tcp("+listener.Addr().String()+")/?timeout=2s&readTimeout=60s")
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	// The client's own context is never canceled — only the server-side
	// (parent) ctx dies below — so any observed abort is attributable
	// to the shutdown path, not to TestShim_ClientDisconnectCancelsInFlightFetch's
	// mechanism.
	qdone := make(chan struct{})
	go func() {
		defer close(qdone)
		_, _ = client.Query("SELECT * FROM _flashback.orders AS OF '2026-01-01 00:00:00'")
	}()

	matched := time.Now().Add(5 * time.Second)
	for mock.ExpectationsWereMet() != nil {
		if time.Now().After(matched) {
			t.Fatal("index fetch never started")
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Simulate SIGTERM: cancel serveLoop's ctx and close the listener,
	// exactly what runShim's shutdown sequence does.
	cancel()
	_ = listener.Close()

	select {
	case <-done:
		// serveLoop's wg.Wait returned: handleConn's goroutine for the
		// in-flight query exited instead of blocking forever.
	case <-time.After(5 * time.Second):
		t.Fatal("serveLoop did not return within 5s of ctx cancel; " +
			"handleConn is still blocked on the in-flight query, so wg.Wait never unblocks")
	}
	<-qdone

	freed := time.Now().Add(5 * time.Second)
	for db.Stats().InUse != 0 {
		if time.Now().After(freed) {
			t.Fatalf("server-side fetch still holds %d index connection(s) 5s after shutdown; "+
				"daemon shutdown did not cancel the in-flight query", db.Stats().InUse)
		}
		time.Sleep(50 * time.Millisecond)
	}
}
