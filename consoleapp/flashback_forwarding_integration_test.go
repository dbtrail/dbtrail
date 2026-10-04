//go:build integration

package consoleapp

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	drivermysql "github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/readrouter"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestIntegrationFlashbackForwardingAccount: with a forwarding account set on
// a server (#2079), the routed port does everything on the source as that
// account, and never opens the source account.
//
// The proof that the source account is not used AT ALL: the entry's source
// DSN carries credentials the source refuses. Every way the router touches
// the source (the EXPLAIN it decides on, a forwarded statement, a prepared
// one, USE) must still work, because it goes through the forwarding account;
// the control server, with the same broken source DSN and no forwarding
// account, fails on its first statement.
func TestIntegrationFlashbackForwardingAccount(t *testing.T) {
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
	// A read-only account on the source, for this test alone.
	fwdUser := fmt.Sprintf("fwd_%d", time.Now().UnixNano()%1_000_000_000)
	const fwdPass = "Fwd-pw-2079"
	if _, err := srcDB.Exec(fmt.Sprintf("CREATE USER '%s'@'%%' IDENTIFIED BY '%s'", fwdUser, fwdPass)); err != nil {
		t.Fatalf("create the forwarding user: %v", err)
	}
	t.Cleanup(func() { _, _ = srcDB.Exec(fmt.Sprintf("DROP USER IF EXISTS '%s'@'%%'", fwdUser)) })
	if _, err := srcDB.Exec(fmt.Sprintf("GRANT SELECT ON `%s`.* TO '%s'@'%%'", srcName, fwdUser)); err != nil {
		t.Fatalf("grant: %v", err)
	}

	good, err := drivermysql.ParseDSN(testutil.IntegrationDSN(srcName))
	if err != nil {
		t.Fatal(err)
	}
	broken := good.Clone()
	broken.User, broken.Passwd = "dbtrail_no_such_capture_user", "wrong"
	route := good.Clone()
	route.User, route.Passwd = fwdUser, fwdPass

	baseDir := t.TempDir()
	writeRoutingBaseline(t, baseDir, srcName, time.Minute, 2)
	reg, err := console.LoadRegistry(t.TempDir() + "/servers.yaml")
	if err != nil {
		t.Fatal(err)
	}
	withFwd, err := reg.Add(console.ServerEntry{Name: "withfwd", DSN: indexDSN, SourceDSN: broken.FormatDSN(), RouteDSN: route.FormatDSN(), BaselineDir: baseDir})
	if err != nil {
		t.Fatal(err)
	}
	control, err := reg.Add(console.ServerEntry{Name: "control", DSN: indexDSN, SourceDSN: broken.FormatDSN(), BaselineDir: baseDir})
	if err != nil {
		t.Fatal(err)
	}
	srv, err := console.New(console.Config{Listen: "127.0.0.1:0", Token: "tok", Registry: reg, FlashbackListen: "127.0.0.1:3308",
		ReadRouting: console.ReadRoutingConfig{MaxCopyAge: time.Hour, ScanRows: 2}})
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	served := make(chan struct{})
	go func() {
		_ = serveFlashback(ctx, srv, ln, flashbackConfig{RouteMaxCopyAge: time.Hour, RoutePolicy: readrouter.Policy{ScanRows: 2}})
		close(served)
	}()
	t.Cleanup(func() { cancel(); <-served })
	addr := ln.Addr().String()

	conn := openFlashback(t, addr, withFwd.ID, "tok", srcName)
	defer conn.Close()
	conn.SetMaxOpenConns(1)
	wantUser := fwdUser + "@%"

	// A forwarded statement (CURRENT_USER() is kept on MySQL by the veto list).
	if got := scanStrings(t, conn, "SELECT CURRENT_USER()"); len(got) != 1 || got[0] != wantUser {
		t.Fatalf("forwarded statement ran as %v, want %s", got, wantUser)
	}
	// A prepared statement (binary protocol), prepared and executed on the source.
	var who string
	if err := conn.QueryRow("SELECT CURRENT_USER() FROM orders WHERE id = ?", 1).Scan(&who); err != nil || who != wantUser {
		t.Fatalf("prepared statement ran as %q (%v), want %s", who, err, wantUser)
	}
	// The decision: a cheap plan stays on MySQL and an expensive one reaches
	// the copy, and both need the EXPLAIN to have run on the source.
	if got := scanStrings(t, conn, "SELECT status FROM orders WHERE id = 1"); len(got) != 1 || got[0] != "live" {
		t.Errorf("point lookup = %v, want MySQL's row", got)
	}
	if got := scanStrings(t, conn, "SELECT status, count(*) FROM orders GROUP BY status ORDER BY status LIMIT 1"); len(got) == 0 || got[0] != "copy" {
		t.Errorf("full scan = %v, want the copy's row (the EXPLAIN did not run as the forwarding account?)", got)
	}
	// USE on the upstream connection.
	if _, err := conn.Exec("USE " + srcName); err != nil {
		t.Errorf("USE: %v", err)
	}
	if got := scanStrings(t, conn, "SELECT status FROM orders WHERE id = 2"); len(got) != 1 || got[0] != "live" {
		t.Errorf("after USE: %v", got)
	}
	// The account's grants bound the port: SELECT only, so a write is
	// MySQL's own refusal, and the source is unchanged.
	_, err = conn.Exec("INSERT INTO orders VALUES (4,'written')")
	var me *drivermysql.MySQLError
	if !errors.As(err, &me) || me.Number != 1142 || !strings.Contains(me.Message, fwdUser) {
		t.Errorf("INSERT as the forwarding account: got %v, want MySQL's 1142 naming %s", err, fwdUser)
	}
	var rows int
	if err := srcDB.QueryRow("SELECT count(*) FROM orders").Scan(&rows); err != nil || rows != 3 {
		t.Errorf("the source holds %d rows (%v), want 3", rows, err)
	}

	// The control: the same source DSN with no forwarding account is what
	// the router uses, and the source refuses it.
	ctl := openFlashback(t, addr, control.ID, "tok", srcName)
	defer ctl.Close()
	if _, err := ctl.Query("SELECT CURRENT_USER()"); err == nil {
		t.Fatal("the control server answered: its source DSN is not refused, so the checks above prove nothing")
	} else if !strings.Contains(err.Error(), "2006") && !strings.Contains(err.Error(), "gone away") {
		t.Logf("control server failed with: %v", err)
	}
}
