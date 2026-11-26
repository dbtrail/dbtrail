//go:build integration

package consoleapp

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/readrouter"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// On the real MySQL port, in fresh mode: a statement answered by the copy
// does not wait for an update of the tails and returns the same rows. With
// the tails' directory swapped during the update it waits.
func TestIntegrationRouterTailDirDuringAnUpdate(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	t.Setenv("BINTRAIL_SQL_TAILS", "")
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
	baseDir := t.TempDir()
	writeRoutingBaseline(t, baseDir, srcName, time.Hour, 2)
	f := newRouterFiles(t)
	ent, err := f.owner.Add(console.ServerEntry{Name: "srva", DSN: indexDSN, SourceDSN: sourceDSN, RouteDSN: routeAccountDSN(t, sourceDSN), BaselineDir: baseDir})
	if err != nil {
		t.Fatal(err)
	}
	fresh := true
	if _, err := f.owner.SetSQLMode(ent.ID, &fresh, nil, nil); err != nil {
		t.Fatal(err)
	}
	st := f.settings("tok")
	f.savePortFile(t, true, "made-in-the-web")
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
	const scan = "SELECT status, count(*) FROM orders GROUP BY status"
	read := func() string { return fmt.Sprint(scanStrings(t, conn, scan)) }
	want := read()
	var hold func()
	var dir string
	waitUntil(t, "the keeper's directory", func() bool {
		var ok bool
		_ = read()
		hold, dir, ok = srv.HoldTailsUpdateForTest(ent.ID)
		return ok
	})
	begin := time.Now()
	got := read()
	if took := time.Since(begin); took > 2*time.Second {
		t.Errorf("waited %v for the update of the tails", took)
	}
	if got != want {
		t.Errorf("during the update %s, with none %s", got, want)
	}
	hold()
	for _, sw := range []struct {
		name string
		swap func(string) error
	}{
		{"removed", os.RemoveAll},
		{"replaced by another directory", func(d string) error {
			if err := os.Rename(d, d+".old"); err != nil {
				return err
			}
			return os.Mkdir(d, 0o700)
		}},
		{"a link swapped in", func(d string) error {
			if err := os.Rename(d, d+".old"); err != nil {
				return err
			}
			if err := os.Mkdir(d+".other", 0o700); err != nil {
				return err
			}
			return os.Symlink(d+".other", d)
		}},
	} {
		t.Run(sw.name, func(t *testing.T) {
			waitUntil(t, "the keeper's directory", func() bool {
				var ok bool
				_ = read()
				hold, dir, ok = srv.HoldTailsUpdateForTest(ent.ID)
				return ok
			})
			defer hold()
			if err := sw.swap(dir); err != nil {
				t.Fatal(err)
			}
			answer := make(chan string, 1)
			go func() { answer <- fmt.Sprint(scanStrings(t, conn, scan)) }()
			select {
			case got := <-answer:
				t.Fatalf("answered %s during the update, with its directory swapped", got)
			case <-time.After(400 * time.Millisecond):
			}
			hold()
			if got := <-answer; got != want {
				t.Errorf("after the update %s, want %s", got, want)
			}
		})
	}
}
