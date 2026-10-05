//go:build integration

package consoleapp

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http/httptest"
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

	// The page is told what the source said and about which account: the
	// control server's clients only ever see error 2006.
	api := func(method, path, body string) (int, string) {
		t.Helper()
		req := httptest.NewRequest(method, "http://127.0.0.1"+path, strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer tok")
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		srv.Handler().ServeHTTP(rec, req)
		return rec.Code, rec.Body.String()
	}
	refusedNote := func(id string) string {
		t.Helper()
		code, body := api("GET", "/api/flashback", "")
		var fb struct {
			Routing struct {
				Servers map[string]struct {
					AccountRefused string `json:"account_refused"`
				} `json:"servers"`
			} `json:"routing"`
		}
		if err := json.Unmarshal([]byte(body), &fb); err != nil || code != 200 {
			t.Fatalf("GET /api/flashback: %d %s (%v)", code, body, err)
		}
		if strings.Contains(body, fwdPass) || strings.Contains(body, ":wrong@") {
			t.Fatalf("the page's data carries a password: %s", body)
		}
		return fb.Routing.Servers[id].AccountRefused
	}
	note := refusedNote(control.ID)
	t.Logf("control server, on the page: %s", note)
	if !strings.Contains(note, "the source account dbtrail_no_such_capture_user") || !strings.Contains(note, "1045") {
		t.Errorf("the page says %q about the control server, want the source account and MySQL's 1045", note)
	}
	if note := refusedNote(withFwd.ID); note != "" {
		t.Errorf("the server whose forwarding account logs in carries a refusal: %q", note)
	}

	// A connection that is OPEN when the account changes does not keep the
	// previous account: it is closed, and the next one logs in as the new
	// account. A dedicated connection, so database/sql cannot hide the loss
	// by retrying on a fresh one.
	fwd2 := fwdUser + "_b"
	if _, err := srcDB.Exec(fmt.Sprintf("CREATE USER '%s'@'%%' IDENTIFIED BY '%s'", fwd2, fwdPass)); err != nil {
		t.Fatalf("create the second forwarding user: %v", err)
	}
	t.Cleanup(func() { _, _ = srcDB.Exec(fmt.Sprintf("DROP USER IF EXISTS '%s'@'%%'", fwd2)) })
	if _, err := srcDB.Exec(fmt.Sprintf("GRANT SELECT ON `%s`.* TO '%s'@'%%'", srcName, fwd2)); err != nil {
		t.Fatalf("grant: %v", err)
	}
	idx, err := drivermysql.ParseDSN(indexDSN)
	if err != nil {
		t.Fatal(err)
	}
	ih, ip, _ := net.SplitHostPort(idx.Addr)
	entryBody := func(extra string) string {
		return fmt.Sprintf(`{"name":"withfwd","host":%q,"port":%q,"user":%q,"dbname":%q,"baseline_dir":%q%s}`, ih, ip, idx.User, idx.DBName, baseDir, extra)
	}
	held, err := conn.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	currentUser := func(c *sql.Conn) (string, error) {
		qctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		var who string
		err := c.QueryRowContext(qctx, "SELECT CURRENT_USER()").Scan(&who)
		return who, err
	}
	if who, err := currentUser(held); err != nil || who != wantUser {
		t.Fatalf("before the edit: %q (%v), want %s", who, err, wantUser)
	}
	// An edit that leaves the account alone leaves the connection alone.
	if code, body := api("PUT", "/api/servers/"+withFwd.ID, entryBody(``)); code != 200 {
		t.Fatalf("unrelated PUT: %d %s", code, body)
	}
	if who, err := currentUser(held); err != nil || who != wantUser {
		t.Fatalf("after an edit that changed no account: %q (%v), want the same open connection as %s", who, err, wantUser)
	}
	// The account changes.
	if code, body := api("PUT", "/api/servers/"+withFwd.ID, entryBody(fmt.Sprintf(`,"route_user":%q,"route_password":%q`, fwd2, fwdPass))); code != 200 {
		t.Fatalf("PUT the new account: %d %s", code, body)
	}
	start := time.Now()
	who, err = currentUser(held)
	t.Logf("the connection open across the edit: %q, %v (after %s)", who, err, time.Since(start).Round(time.Millisecond))
	if err == nil {
		t.Fatalf("a connection open across the edit still answers, as %q: it kept the previous account", who)
	}
	if time.Since(start) > 5*time.Second {
		t.Errorf("the open connection took %s to fail: a hang, not a clean connection loss", time.Since(start))
	}
	again := openFlashback(t, addr, withFwd.ID, "tok", srcName)
	defer again.Close()
	if got := scanStrings(t, again, "SELECT CURRENT_USER()"); len(got) != 1 || got[0] != fwd2+"@%" {
		t.Fatalf("after the edit a new connection ran as %v, want %s@%%", got, fwd2)
	}
	// The account is removed: the open connection goes, and the port is
	// back on the source account (which this entry's source refuses).
	held2, err := again.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer held2.Close()
	if who, err := currentUser(held2); err != nil || who != fwd2+"@%" {
		t.Fatalf("before the removal: %q (%v)", who, err)
	}
	if code, body := api("PUT", "/api/servers/"+withFwd.ID, entryBody(`,"route_user":""`)); code != 200 {
		t.Fatalf("PUT removing the account: %d %s", code, body)
	}
	if who, err := currentUser(held2); err == nil {
		t.Fatalf("a connection open across the removal still answers, as %q", who)
	}
	after := openFlashback(t, addr, withFwd.ID, "tok", srcName)
	defer after.Close()
	if _, err := after.Query("SELECT CURRENT_USER()"); err == nil {
		t.Fatal("after the removal the port still reaches the source: it is not on the source account")
	}
	if note := refusedNote(withFwd.ID); !strings.Contains(note, "the source account dbtrail_no_such_capture_user") {
		t.Errorf("after the removal the page says %q, want the source account refused", note)
	}
}
