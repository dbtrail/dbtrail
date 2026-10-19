//go:build integration

package console

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// The console's index reads run on a pool that stops a SELECT on the server
// when the request that asked for it goes away. go-sql-driver alone only
// closes its socket, and the statement goes on to its end: a page that gives
// up and retries stacks one full scan per retry on the index server.
//
// These tests start a slow statement through the console's real pool (the
// bundle connManager builds, both the eager and the lazy branch), abandon it,
// and look for it in PROCESSLIST.

func abandonMarker(t *testing.T) string {
	t.Helper()
	b := make([]byte, 6)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return "qk-" + hex.EncodeToString(b)
}

// stillRunning reports whether a statement carrying marker is executing.
func stillRunning(t *testing.T, observer *sql.DB, marker string) bool {
	t.Helper()
	var n int
	err := observer.QueryRow(
		"SELECT COUNT(*) FROM information_schema.PROCESSLIST WHERE COMMAND <> 'Sleep' AND INFO LIKE ? AND INFO NOT LIKE '%PROCESSLIST%'",
		"%"+marker+"%").Scan(&n)
	if err != nil {
		t.Fatalf("read PROCESSLIST: %v", err)
	}
	return n > 0
}

func waitRunning(t *testing.T, observer *sql.DB, marker string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !stillRunning(t, observer, marker) {
		if time.Now().After(deadline) {
			t.Fatalf("statement %s never showed up in PROCESSLIST", marker)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func goneWithin(t *testing.T, observer *sql.DB, marker string, d time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if !stillRunning(t, observer, marker) {
			return true
		}
		time.Sleep(50 * time.Millisecond)
	}
	return false
}

// killLeftover ends a statement the control case left running on purpose: the
// test server is shared.
func killLeftover(t *testing.T, observer *sql.DB, marker string) {
	rows, err := observer.Query(
		"SELECT ID FROM information_schema.PROCESSLIST WHERE INFO LIKE ? AND INFO NOT LIKE '%PROCESSLIST%'", "%"+marker+"%")
	if err != nil {
		return
	}
	defer rows.Close()
	var ids []int64
	for rows.Next() {
		var id int64
		if rows.Scan(&id) == nil {
			ids = append(ids, id)
		}
	}
	for _, id := range ids {
		_, _ = observer.Exec("KILL QUERY ?", id)
	}
}

func consoleBundleDB(t *testing.T, lazy bool) (*sql.DB, *sql.DB) {
	t.Helper()
	observer, dbName := testutil.CreateTestDB(t)
	reg, err := LoadRegistry(filepath.Join(t.TempDir(), "console-servers.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	entry, err := reg.Add(ServerEntry{Name: "idx", DSN: testutil.IntegrationDSN(dbName)})
	if err != nil {
		t.Fatal(err)
	}
	cm := newConnManager(reg, false)
	cm.lazyIndex = lazy
	b, err := cm.buildBundle(entry)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.db.Close() })
	return b.db, observer
}

func TestIntegrationAbandonedConsoleReadIsKilled(t *testing.T) {
	requireAbandonedSleepSurvives(t)
	for _, tc := range []struct {
		name string
		lazy bool
		args bool
	}{
		{"eager pool, no bind args", false, false},
		{"eager pool, bind args (prepared path)", false, true},
		{"lazy pool, bind args (prepared path)", true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, observer := consoleBundleDB(t, tc.lazy)
			marker := abandonMarker(t)
			t.Cleanup(func() { killLeftover(t, observer, marker) })
			// Pin one pool connection to learn the thread the KILL will hit;
			// it goes back to the pool on Close, like any request's.
			conn, err := db.Conn(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			var killedID int64
			if err := conn.QueryRowContext(context.Background(), "SELECT CONNECTION_ID()").Scan(&killedID); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			errc := make(chan error, 1)
			go func() {
				var err error
				if tc.args {
					_, err = conn.QueryContext(ctx, "SELECT /* "+marker+" */ SLEEP(?)", 30)
				} else {
					_, err = conn.QueryContext(ctx, "SELECT /* "+marker+" */ SLEEP(30)")
				}
				conn.Close()
				errc <- err
			}()
			waitRunning(t, observer, marker)
			cancel()
			select {
			case <-errc:
			case <-time.After(10 * time.Second):
				t.Fatal("the abandoned read did not return")
			}
			if !goneWithin(t, observer, marker, 3*time.Second) {
				t.Fatal("the abandoned read is still running on the index server")
			}
			// The pool still serves after a KILL, and not on the killed
			// connection: that one was discarded, not handed out again.
			var nextID int64
			if err := db.QueryRowContext(context.Background(), "SELECT CONNECTION_ID()").Scan(&nextID); err != nil {
				t.Fatalf("pool broken after the KILL: %v", err)
			}
			if nextID == killedID {
				t.Fatalf("the pool reused connection %d after a KILL was sent for it", killedID)
			}
		})
	}
}

// abandonedSleepSurvives is the premise every kill assertion here rests on:
// on a plain pool (what the console used before) an abandoned SLEEP is still
// running 3 s after its caller left. On a server that ends it by itself, "gone
// from PROCESSLIST" would pass with the KILL broken, so the kill tests skip
// there instead of reading green. Measured once per test binary.
var (
	premiseOnce     sync.Once
	premiseSurvives bool
)

func abandonedSleepSurvives(t *testing.T) bool {
	t.Helper()
	premiseOnce.Do(func() {
		observer, dbName := testutil.CreateTestDB(t)
		plain, err := config.Connect(testutil.IntegrationDSN(dbName))
		if err != nil {
			t.Fatal(err)
		}
		defer plain.Close()
		marker := abandonMarker(t)
		defer killLeftover(t, observer, marker)
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() { _, _ = plain.QueryContext(ctx, "SELECT /* "+marker+" */ SLEEP(?)", 20); close(done) }()
		waitRunning(t, observer, marker)
		cancel()
		<-done
		premiseSurvives = !goneWithin(t, observer, marker, 3*time.Second)
	})
	return premiseSurvives
}

func requireAbandonedSleepSurvives(t *testing.T) {
	t.Helper()
	if !abandonedSleepSurvives(t) {
		t.Skip("this server ends an abandoned SLEEP on its own when the client disconnects, so a statement leaving PROCESSLIST would not prove the KILL")
	}
}

// The control, as its own test: if this ever skips, the driver or the server
// changed, and the guard deserves a second look.
func TestIntegrationAbandonedReadOnPlainPoolKeepsRunning(t *testing.T) {
	requireAbandonedSleepSurvives(t)
}

// A background job runs on its own context; the request that started it can
// go away without the job's statement being killed.
func TestIntegrationActivityFlightSurvivesItsRequest(t *testing.T) {
	db, observer := consoleBundleDB(t, false)
	marker := abandonMarker(t)
	t.Cleanup(func() { killLeftover(t, observer, marker) })
	cache := newActivityCache()
	reqCtx, cancel := context.WithCancel(context.Background())
	got := make(chan error, 1)
	go func() {
		_, err := cache.get(reqCtx, "k", func(ctx context.Context) (activityResponse, error) {
			var n int
			err := db.QueryRowContext(ctx, "SELECT /* "+marker+" */ SLEEP(?)", 3).Scan(&n)
			return activityResponse{}, err
		})
		got <- err
	}()
	waitRunning(t, observer, marker)
	cancel()
	if err := <-got; err == nil {
		t.Fatal("the cancelled request got an answer")
	}
	time.Sleep(500 * time.Millisecond)
	if !stillRunning(t, observer, marker) {
		t.Fatal("the background flight's statement was killed when the request that started it left")
	}
	if !goneWithin(t, observer, marker, 10*time.Second) {
		t.Fatal("the flight's statement never finished")
	}
}

// A guarded read still carries the server's column types.
func TestIntegrationGuardedReadKeepsColumnTypes(t *testing.T) {
	db, _ := consoleBundleDB(t, false)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rows, err := db.QueryContext(ctx, "SELECT CAST(? AS SIGNED) AS n", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	ct, err := rows.ColumnTypes()
	if err != nil || len(ct) != 1 {
		t.Fatalf("ColumnTypes: %v %v", ct, err)
	}
	if got := ct[0].DatabaseTypeName(); got != "BIGINT" {
		t.Fatalf("DatabaseTypeName = %q, want BIGINT", got)
	}
}
