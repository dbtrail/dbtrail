//go:build integration

package consoleapp

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/readrouter"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// #2084, end to end against a real MySQL: the router as its own console,
// over a registry file another registry (the daemon's) owns. The source's
// orders has 3 rows that say "live", the copy 2 rows that say "copy", so
// every statement is asserted by WHICH SIDE answered.
func TestIntegrationRouter(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	now := time.Now().UTC().Truncate(time.Hour)
	indexDSN := seedFlashbackIndex(t, "alice", now)
	srcDB, srcName := testutil.CreateTestDB(t)
	for _, q := range []string{
		"CREATE TABLE orders (id INT NOT NULL PRIMARY KEY, status VARCHAR(32))",
		"INSERT INTO orders VALUES (1,'live'),(2,'live'),(3,'live')",
		"ANALYZE TABLE orders",
	} {
		if _, err := srcDB.Exec(q); err != nil {
			t.Fatalf("%.80s: %v", q, err)
		}
	}
	sourceDSN := testutil.IntegrationDSN(srcName)
	// A copy a month old: inside watch, with any maximum age a person would
	// set, this statement would be MySQL's.
	baseDir := t.TempDir()
	writeRoutingBaseline(t, baseDir, srcName, 30*24*time.Hour, 2)

	f := newRouterFiles(t)
	ent, err := f.owner.Add(console.ServerEntry{Name: "srva", DSN: indexDSN, SourceDSN: sourceDSN, BaselineDir: baseDir})
	if err != nil {
		t.Fatal(err)
	}
	st := f.settings("tok")
	// The scan rule alone, at 2 rows: a full scan over the 3-row table goes
	// to the copy, a primary-key lookup stays on MySQL.
	st.Policy = readrouter.Policy{ScanRows: 2}
	srv, runner, err := newRouterConsole(st, console.SQLPool{Workers: 2, Threads: 2})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(runner.Close)
	ctx, cancel := context.WithCancel(context.Background())
	srv.RefreshFollowedFiles()
	go srv.FollowFiles(ctx, 50*time.Millisecond)
	addr := freeAddr(t)
	port := &routerPort{srv: srv, addr: addr, cfg: routerFlashbackConfig(st), poll: 50 * time.Millisecond, out: &bytes.Buffer{}}
	done := make(chan error, 1)
	go func() { done <- port.serve(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("the router ended with %v", err)
		}
	})
	waitUntil(t, "the port to open", func() bool { return port.listening() != "" })

	conn := openFlashback(t, addr, ent.ID, "tok", srcName)
	defer conn.Close()
	side := func(q string) string {
		t.Helper()
		got := scanStrings(t, conn, q)
		if len(got) == 0 {
			t.Fatalf("%s: no rows", q)
		}
		return got[0]
	}
	if got := side("SELECT status FROM orders WHERE id = 1"); got != "live" {
		t.Errorf("point lookup answered %q, want live (MySQL)", got)
	}
	if got := side("SELECT status, count(*) FROM orders GROUP BY status"); got != "copy" {
		t.Errorf("full scan answered %q, want the month-old copy", got)
	}
	// A write is forwarded, and inside a transaction a read is MySQL's.
	if _, err := conn.Exec("UPDATE orders SET status = 'edited' WHERE id = 3"); err != nil {
		t.Fatalf("UPDATE: %v", err)
	}
	if got := side("SELECT status FROM orders WHERE id = 3"); got != "edited" {
		t.Errorf("after the UPDATE the lookup answered %q", got)
	}

	// Several clients on the one server, each with expensive reads, all at
	// once: every one is answered by the copy. Inside watch a server's
	// statements on the copy run one at a time; here two workers share them.
	var wg sync.WaitGroup
	errs := make(chan string, 64)
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c := openFlashback(t, addr, ent.Name, "tok", srcName)
			defer c.Close()
			for range 5 {
				var status string
				var n int
				if err := c.QueryRow("SELECT status, count(*) FROM orders GROUP BY status").Scan(&status, &n); err != nil {
					errs <- err.Error()
					return
				}
				if status != "copy" || n != 2 {
					errs <- "answered " + status
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Errorf("a concurrent expensive read: %s", e)
	}
	// 41 statements went to the copy; the workers that answered them are
	// the pool's two, kept from one statement to the next. A process each
	// would be 41 starts; the margin is for a worker replaced on a slow
	// machine.
	if got := runner.WorkersStarted(); got > 6 {
		t.Errorf("%d worker processes were started for 41 statements on a pool of 2; workers are not being kept", got)
	}

	// The daemon gives the server a forwarding account. The router's open
	// connections still hold the previous account's session on the source,
	// so they are closed; a client reconnects into the account in force.
	if err := conn.Ping(); err != nil {
		t.Fatal(err)
	}
	raw, err := conn.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	if _, err := raw.ExecContext(context.Background(), "SELECT 1"); err != nil {
		t.Fatal(err)
	}
	ent.RouteDSN = testutil.IntegrationDSN(srcName) + "&interpolateParams=true"
	if err := f.owner.Update(ent); err != nil {
		t.Fatal(err)
	}
	waitUntil(t, "the connection that forwards with the previous account to be closed", func() bool {
		_, err := raw.ExecContext(context.Background(), "SELECT 1")
		return err != nil
	})
	if got := side("SELECT status FROM orders WHERE id = 1"); got != "live" {
		t.Errorf("after reconnecting: %q", got)
	}

	// This router has a token, so a password created in the web interface
	// does not open it.
	f.savePortFile(t, true, "made-in-the-web")
	time.Sleep(200 * time.Millisecond)
	if code := mysqlLoginQuiet(addr, "made-in-the-web"); code != 1045 {
		t.Errorf("login with the web interface's password: %d, want 1045", code)
	}

	// A server with no source to forward to cannot be routed. Its client
	// is told so on its first statement and is not answered from the copy:
	// connected in place of MySQL, it would read every row as the copy had
	// it with nothing saying so.
	view, err := f.owner.Add(console.ServerEntry{Name: "viewonly", DSN: indexDSN, BaselineDir: baseDir})
	if err != nil {
		t.Fatal(err)
	}
	waitUntil(t, "the router to see the new server", func() bool {
		_, err := srv.ResolveFlashback(context.Background(), view.Name)
		return err == nil
	})
	vc := openFlashback(t, addr, view.Name, "tok", srcName)
	defer vc.Close()
	_, err = vc.Query("SELECT status, count(*) FROM orders GROUP BY status")
	if err == nil || mysqlCode(err) != 1105 || !strings.Contains(err.Error(), "cannot be routed") || !strings.Contains(err.Error(), "no source database") {
		t.Errorf("a statement for a server with no source: %v, want error 1105 that says it cannot be routed and why", err)
	}

	// And the router wrote nothing in the daemon's directory but what the
	// test, as the daemon, saved there.
	entries, err := os.ReadDir(filepath.Dir(f.servers))
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if e.Name() != "console-servers.yaml" && e.Name() != console.FlashbackFileName {
			t.Errorf("a file appeared in the daemon's directory: %s", e.Name())
		}
	}
}

// The router serves a server whose index is away: what does not read the
// index (a statement forwarded to the source, an expensive read over the
// copy's tables) is answered as if it were there, and what does read it says
// why it cannot. Nothing listens on the index address below.
func TestIntegrationRouter_theIndexIsAway(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	srcDB, srcName := testutil.CreateTestDB(t)
	for _, q := range []string{
		"CREATE TABLE orders (id INT NOT NULL PRIMARY KEY, status VARCHAR(32))",
		"INSERT INTO orders VALUES (1,'live'),(2,'live'),(3,'live')",
		"ANALYZE TABLE orders",
	} {
		if _, err := srcDB.Exec(q); err != nil {
			t.Fatalf("%.80s: %v", q, err)
		}
	}
	baseDir := t.TempDir()
	writeRoutingBaseline(t, baseDir, srcName, time.Hour, 2)
	f := newRouterFiles(t)
	ent, err := f.owner.Add(console.ServerEntry{Name: "srva", DSN: "idx:secretpw@tcp(127.0.0.1:1)/binlog_index?timeout=2s",
		SourceDSN: testutil.IntegrationDSN(srcName), BaselineDir: baseDir})
	if err != nil {
		t.Fatal(err)
	}
	st := f.settings("tok")
	st.Policy = readrouter.Policy{ScanRows: 2}
	srv, runner, err := newRouterConsole(st, console.SQLPool{Workers: 2, Threads: 2})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(runner.Close)
	ctx, cancel := context.WithCancel(context.Background())
	srv.RefreshFollowedFiles()
	addr := freeAddr(t)
	port := &routerPort{srv: srv, addr: addr, cfg: routerFlashbackConfig(st), poll: 50 * time.Millisecond, out: &bytes.Buffer{}}
	done := make(chan error, 1)
	go func() { done <- port.serve(ctx) }()
	t.Cleanup(func() { cancel(); <-done })
	waitUntil(t, "the port to open", func() bool { return port.listening() != "" })

	conn := openFlashback(t, addr, ent.ID, "tok", srcName)
	defer conn.Close()
	if got := scanStrings(t, conn, "SELECT status FROM orders WHERE id = 1"); len(got) != 1 || got[0] != "live" {
		t.Errorf("a point lookup with the index away: %v, want MySQL's row", got)
	}
	if got := scanStrings(t, conn, "SELECT status, count(*) FROM orders GROUP BY status"); len(got) != 1 || got[0] != "copy" {
		t.Errorf("an expensive read with the index away: %v, want the copy's", got)
	}
	if _, err := conn.Exec("UPDATE orders SET status = 'edited' WHERE id = 3"); err != nil {
		t.Errorf("a write with the index away: %v", err)
	}
	// Time travel reads the index: refused, with a reason that is not the
	// DSN's password.
	_, err = conn.Query("SELECT * FROM _flashback.orders AS OF '2026-01-01 00:00:00'")
	if err == nil || strings.Contains(err.Error(), "secretpw") || !strings.Contains(err.Error(), "archive_state") || !strings.Contains(err.Error(), "connection refused") {
		t.Errorf("time travel with the index away: %v, want an error that says the index could not be read, without the password", err)
	}
	// The events view lists its files from the index too. The copy cannot
	// build it, so the statement goes to MySQL, which has no such table and
	// says so: an error, and never an answer from something else.
	_, err = conn.Query("SELECT event_type, count(*) FROM events GROUP BY event_type")
	if err == nil || strings.Contains(err.Error(), "secretpw") || mysqlCode(err) != 1146 {
		t.Errorf("events with the index away: %v, want MySQL's own 1146 for a table it does not have", err)
	}
}
