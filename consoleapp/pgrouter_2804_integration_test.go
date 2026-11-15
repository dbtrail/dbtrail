//go:build integration

package consoleapp

import (
	"context"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/pgshim"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// The port as the command runs it counts what it answers in its result
// budget (#2804): a time-travel row read from the server's index is counted
// by its cells and given back once it is written. What the budget does when
// it is full is tested where it lives (internal/pgshim); this is the wiring,
// from the port's loop to a connection.
func TestIntegrationPGRouter_answersAreCountedInThePortsResultBudget(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	t.Setenv("BINTRAIL_SQL_TAILS", "")
	pgRouterNoTails()
	f := newRouterFiles(t)
	hour := time.Now().UTC().Truncate(time.Hour)
	dsn := seedFlashbackIndex(t, "alice", hour)
	if _, err := f.owner.Add(console.ServerEntry{Name: "pgsrv", DSN: dsn, Flavor: console.FlavorPostgres, BaselineDir: t.TempDir()}); err != nil {
		t.Fatal(err)
	}
	f.savePortFile(t, true, "tok")
	srv, runner, err := newRouterConsole(f.settings(""), console.SQLPool{Workers: 1, Threads: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(runner.Close)
	srv.RefreshFollowedFiles()
	budget := pgshim.NewResultBudget(1 << 20)
	router := &pgRouterPort{srv: srv, addr: freeAddr(t), maxConns: 4, poll: 20 * time.Millisecond, out: io.Discard, budget: budget}
	portCtx, stopPort := context.WithCancel(context.Background())
	served := make(chan struct{})
	go func() { _ = router.serve(portCtx); close(served) }()
	defer func() { stopPort(); <-served }()
	waitUntil(t, "the port to open", func() bool { return router.listening() != "" })
	host, portNum, _ := net.SplitHostPort(router.listening())

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	cfg, err := pgconn.ParseConfig(fmt.Sprintf("host=%s port=%s sslmode=disable", host, portNum))
	if err != nil {
		t.Fatal(err)
	}
	cfg.User, cfg.Password, cfg.Database = "anyone", "tok", "pgsrv"
	port, err := pgconn.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("connect to the port: %v", err)
	}
	defer port.Close(context.Background())

	asOf := hour.Add(6 * time.Minute).Format("2006-01-02 15:04:05")
	names, rows, err := pgText(t, port, fmt.Sprintf("SELECT * FROM myapp.users WHERE id = 1 AS OF '%s'", asOf))
	if err != nil {
		t.Fatalf("time travel on the port: %v", err)
	}
	if fmt.Sprint(names) != "[id name]" || len(rows) != 1 || fmt.Sprint(rows[0]) != "[1 alice]" {
		t.Fatalf("time travel answered %v %v, want one row [1 alice] under [id name]", names, rows)
	}
	waitUntil(t, "the answer to be given back", func() bool { return budget.Held() == 0 })
	// "1" and "alice".
	if budget.Charged() != 6 {
		t.Fatalf("the port's budget was charged %d bytes, want the 6 of the row's cells", budget.Charged())
	}
}
