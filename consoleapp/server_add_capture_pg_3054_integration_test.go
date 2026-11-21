//go:build integration

package consoleapp

import (
	"bytes"
	"context"
	"database/sql"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/console"
)

// #3054 against a real PostgreSQL, a real index and the daemon's own
// handlers, the way the Docker dbtrail command runs it: the option file on
// stdin, nobody to ask, and capture-account=yes under [dbtrail-forward]. The
// server is added and the PostgreSQL port would forward for it with the
// capture account.
func TestIntegrationPGServerAddCaptureAccountFromStdin(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	const table, slot, pub = "add3054_t", "add3054_slot", "add3054_pub"
	baseDSN, _, exec1 := pgTestSource(t, ctx, table, slot, pub, "id int PRIMARY KEY, v text")
	exec1("INSERT INTO " + table + " VALUES (1, 'one')")

	mon, bootDSN := pgTestMonitor(t, ctx, time.Second)
	reg, err := console.LoadRegistry(filepath.Join(t.TempDir(), "console-servers.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	srv, err := console.New(console.Config{Listen: "127.0.0.1:0", Token: "tok", Registry: reg, MonitorCtrl: mon})
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	c := &ctlClient{base: ts.URL, token: "tok", hc: ts.Client()}
	t.Cleanup(func() {
		for _, e := range reg.List() {
			_ = mon.Stop(context.Background(), e.ID)
			if db, err := sql.Open("mysql", bootDSN); err == nil {
				_, _ = db.Exec("DROP DATABASE IF EXISTS bintrail_idx_" + e.ID)
				_ = db.Close()
			}
		}
	})

	capture, err := url.Parse(baseDSN)
	if err != nil {
		t.Fatal(err)
	}
	capturePW, _ := capture.User.Password()
	cnf := "[client]\nhost=" + capture.Hostname() + "\nport=" + capture.Port() + "\nuser=" + capture.User.Username() + "\npassword=" + capturePW + "\n[dbtrail-forward]\ncapture-account = yes\n"
	f := addFlags{DefaultsFile: "-", Name: "add3054", Flavor: "postgres", Database: strings.TrimPrefix(capture.Path, "/"), Slot: slot, Publication: pub}
	// A terminal that must not be asked anything: stdin is the file.
	in, err := resolveAddInput(f, &seqAsker{t: t, tty: true}, strings.NewReader(cnf))
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := runServerAdd(ctx, c, in, false, &out); err != nil {
		t.Fatalf("err = %v\n%s", err, out.String())
	}
	want := `"add3054" forwards with the user DBTrail captures with (` + capture.User.Username() + `): the PostgreSQL port sends to your PostgreSQL, read-only, a SELECT the copy does not answer, and only when every table it reads is a table of the copy. Anything else is refused and not sent. A server that hides tables or columns sends nothing.` + "\n\nNext, the first copy of its tables:  bintrail-console snapshot run add3054\n"
	if !strings.HasSuffix(out.String(), want) {
		t.Fatalf("output:\n%s\nwant it to end with:\n%s", out.String(), want)
	}
	if capturePW != "" && strings.Contains(out.String(), capturePW) {
		t.Fatalf("a password in the output:\n%s", out.String())
	}
	entries := reg.List()
	if len(entries) != 1 {
		t.Fatalf("servers saved: %d", len(entries))
	}
	fwd := srv.PostgresForwardTarget(entries[0].ID)
	if !fwd.CaptureAccount || fwd.DSN != entries[0].SourceDSN || fwd.Refused != "" || entries[0].RouteDSN != "" {
		t.Fatalf("the port's target: capture %v refused %q, forwarding user set %v", fwd.CaptureAccount, fwd.Refused, entries[0].RouteDSN != "")
	}
	t.Logf("server add with the option file on stdin:\n%s", out.String())
}
