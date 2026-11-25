//go:build integration

package consoleapp

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/pgshim"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// An install that upgrades with a PostgreSQL server an older build saved:
// a source, no forwarding user, nothing said about the capture account
// (0.140.0). The daemon's boot says so and goes on, capture records changes
// as before, and the PostgreSQL port refuses a client at login with the
// cause and both ways out. Once the server allows forwarding, it is served.
func TestIntegrationPGPortServesOnlyAServerThatForwards(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	const table, slot, pub = "pgreq_orders", "bintrail_pgreq_slot", "bintrail_pgreq_pub"
	baseDSN, source, exec1 := pgTestSource(t, ctx, table, slot, pub, "id int PRIMARY KEY, status text")

	mon, bootDSN := pgTestMonitor(t, ctx, 500*time.Millisecond)
	f := newRouterFiles(t)
	e := console.ServerEntry{
		ID: fmt.Sprintf("pgreq%d", time.Now().UnixNano()%1e9), Name: "pgold",
		Flavor: console.FlavorPostgres, SourceDSN: baseDSN, SourceSlot: slot, SourcePublication: pub, Schemas: "public",
	}
	var err error
	if e.DSN, err = mon.DeriveIndexDSN(e.ID); err != nil {
		t.Fatal(err)
	}
	saved, err := f.owner.Add(e)
	if err != nil {
		t.Fatal(err)
	}

	// The daemon's boot: one warning for the server, and nothing stops.
	var logs bytes.Buffer
	if n := warnPGPortNotServedAtBoot(f.owner, slog.New(slog.NewTextHandler(&logs, nil))); n != 1 {
		t.Fatalf("named %d servers at boot, want 1:\n%s", n, logs.String())
	}
	if !strings.Contains(logs.String(), "server=pgold") || !strings.Contains(logs.String(), "its capture goes on") {
		t.Fatalf("the boot warning:\n%s", logs.String())
	}

	// Capture records changes as before.
	pgTestStart(t, ctx, mon, bootDSN, source, saved)
	exec1("INSERT INTO " + table + " VALUES (1, 'new')")
	idx, err := sql.Open("mysql", saved.DSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { idx.Close() })
	pgTestWait(t, "the change indexed", 60*time.Second, func() (bool, string) {
		var n int
		if err := idx.QueryRow("SELECT COUNT(*) FROM binlog_events WHERE table_name = ?", table).Scan(&n); err != nil {
			return false, err.Error()
		}
		return n == 1, fmt.Sprint(n, " events")
	})

	// The port refuses at login.
	f.savePortFile(t, true, "tok")
	srv, runner, err := newRouterConsole(f.settings(""), console.SQLPool{Workers: 1, Threads: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(runner.Close)
	srv.RefreshFollowedFiles()
	router := &pgRouterPort{srv: srv, addr: freeAddr(t), maxConns: 8, poll: 20 * time.Millisecond, out: io.Discard,
		routes: newPGRouteStats(time.Now(), true), cost: &pgshim.CostPolicy{}}
	portCtx, stopPort := context.WithCancel(context.Background())
	served := make(chan struct{})
	go func() { _ = router.serve(portCtx); close(served) }()
	t.Cleanup(func() { stopPort(); <-served })
	waitUntil(t, "the port to open", func() bool { return router.listening() != "" })
	host, port, _ := net.SplitHostPort(router.listening())
	dsn := fmt.Sprintf("host=%s port=%s user=anyone password=tok dbname=pgold sslmode=disable", host, port)

	c, err := pgconn.Connect(ctx, dsn)
	if err == nil {
		c.Close(ctx)
		t.Fatal("a server with no way to forward was served")
	}
	var pe *pgconn.PgError
	if !errors.As(err, &pe) || pe.Code != "08004" || pe.Severity != "FATAL" {
		t.Fatalf("the refusal: %v", err)
	}
	if want := console.PGPortNotServed("pgold", console.PGRouteRefusedNoAccount); pe.Message != want {
		t.Fatalf("the refusal says\n%s\nwant\n%s", pe.Message, want)
	}

	// Forwarding allowed: the same server is served, without a restart.
	if _, err := f.owner.SetForwardWithCaptureAccount(saved.ID, true); err != nil {
		t.Fatal(err)
	}
	srv.RefreshFollowedFiles()
	c, err = pgconn.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("a server that forwards is refused: %v", err)
	}
	c.Close(ctx)
}
