//go:build integration

package consoleapp

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/pgshim"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// On the real PostgreSQL port: a statement reads a table's recent change
// during an update of the tails, without waiting for it. With the tails'
// directory swapped during the update it waits, and the client still gets
// the current value.
func TestIntegrationPGRouterTailDirDuringAnUpdate(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	t.Setenv("BINTRAIL_SQL_TAILS", "")
	t.Cleanup(func() { console.EnablePostgresTails(false) })
	const tbl, slot, pub, role, pass = "pgtaildir_t", "bintrail_pgtaildir_slot", "bintrail_pgtaildir_pub", "dbtrail_fwd_taildir", "fwd-pw-taildir"
	baseDSN, source, exec1 := pgTestSource(t, ctx, tbl, slot, pub, "id int PRIMARY KEY, d date NOT NULL, v int NOT NULL")
	exec1("INSERT INTO " + tbl + " SELECT g, DATE '2026-01-01' + (g % 300), g FROM generate_series(1, 2000) g")
	exec1("ANALYZE " + tbl)
	exec1(`DO $$ BEGIN IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '` + role + `') THEN EXECUTE 'DROP OWNED BY ` + role + `'; END IF; END $$`)
	exec1("DROP ROLE IF EXISTS " + role)
	exec1("CREATE ROLE " + role + " LOGIN PASSWORD '" + pass + "'")
	exec1("GRANT SELECT ON " + tbl + " TO " + role)
	t.Cleanup(func() {
		for _, s := range []string{"DROP OWNED BY " + role, "DROP ROLE IF EXISTS " + role} {
			_, _ = source.Exec(context.Background(), s).ReadAll()
		}
	})
	mon, bootDSN := pgTestMonitor(t, ctx, 200*time.Millisecond)
	_, _, sup := newScheduleFixture(t, true)
	f := newRouterFiles(t)
	e, err := f.owner.Add(console.ServerEntry{Name: "pg-taildir", Flavor: console.FlavorPostgres,
		SourceDSN: baseDSN, SourceSlot: slot, SourcePublication: pub, Schemas: "public", BaselineDir: t.TempDir(),
		RouteDSN: withAccount(t, baseDSN, role, pass)})
	if err != nil {
		t.Fatal(err)
	}
	if e.DSN, err = mon.DeriveIndexDSN(e.ID); err != nil {
		t.Fatal(err)
	}
	if err := f.owner.Update(e); err != nil {
		t.Fatal(err)
	}
	pgTestStart(t, ctx, mon, bootDSN, source, e)
	if err := sup.Trigger(console.BaselineRequestFor(e)); err != nil {
		t.Fatal(err)
	}
	pgTestWait(t, "the snapshot", 2*time.Minute, func() (bool, string) {
		st := sup.Status(e.ID)
		if st.State == "failed" {
			t.Fatalf("the snapshot failed: %s", st.LastError)
		}
		return st.State == "succeeded", st.State
	})
	fresh := true
	if _, err := f.owner.SetSQLMode(e.ID, &fresh, nil, nil); err != nil {
		t.Fatal(err)
	}
	f.savePortFile(t, true, "tok")
	srv, runner, err := newPGRouterConsole(f.settings(""), console.SQLPool{Workers: 3, Threads: 2})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(runner.Close)
	srv.RefreshFollowedFiles()
	// Every plan is expensive here: each statement heads for the copy.
	cost := pgshim.CostPolicy{CostThreshold: 1}
	router := &pgRouterPort{srv: srv, addr: freeAddr(t), maxConns: 8, poll: 20 * time.Millisecond, out: io.Discard,
		routes: newPGRouteStats(time.Now(), true), cost: &cost, sources: newPGSourceSlots(4)}
	portCtx, stopPort := context.WithCancel(context.Background())
	served := make(chan struct{})
	go func() { _ = router.serve(portCtx); close(served) }()
	defer func() { stopPort(); <-served }()
	waitUntil(t, "the port", func() bool { return router.listening() != "" })
	host, portNum, _ := net.SplitHostPort(router.listening())
	dsn := fmt.Sprintf("host=%s port=%s sslmode=disable user=anyone password=tok dbname=pg-taildir", host, portNum)

	// The table's tail is up: a change made after the snapshot is read.
	exec1("UPDATE " + tbl + " SET v = -1 WHERE id = 1")
	pgTestWait(t, "the tail to show the change", 60*time.Second, func() (bool, string) {
		c := pgClient(t, ctx, dsn, nil)
		defer c.Close(context.Background())
		got := one(ctx, c, "SELECT v FROM "+tbl+" WHERE id = 1")
		return got == "-1", got
	})

	read := "SELECT v FROM " + tbl + " WHERE id = 1"
	c := pgClient(t, ctx, dsn, nil)
	defer c.Close(context.Background())
	if got := one(ctx, c, read); got != "-1" {
		t.Fatalf("with no update running: %q", got)
	}
	release, dir, ok := srv.HoldTailsUpdateForTest(e.ID)
	if !ok {
		t.Fatal("the server has no tail keeper")
	}
	defer release()
	begin := time.Now()
	got := one(ctx, c, read)
	if took := time.Since(begin); took > 2*time.Second {
		t.Errorf("waited %v for the update of the tails", took)
	}
	if got != "-1" {
		t.Errorf("during the update: %q, want the recent change", got)
	}
	release()
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
			pgTestWait(t, "the tail back", 60*time.Second, func() (bool, string) {
				got := one(ctx, c, read)
				return got == "-1", got
			})
			var hold func()
			pgTestWait(t, "the keeper's directory", 30*time.Second, func() (bool, string) {
				var ok bool
				hold, dir, ok = srv.HoldTailsUpdateForTest(e.ID)
				return ok, dir
			})
			defer hold()
			if err := sw.swap(dir); err != nil {
				t.Fatal(err)
			}
			answer := make(chan string, 1)
			go func() { answer <- one(ctx, c, read) }()
			select {
			case got := <-answer:
				t.Fatalf("answered %q during the update, with its directory swapped", got)
			case <-time.After(400 * time.Millisecond):
			}
			hold()
			if got := <-answer; got != "-1" {
				t.Errorf("after the update: %q, want the current value", got)
			}
		})
	}
}
