package consoleapp

import (
	"bytes"
	"context"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/pgshim"
)

// #3054: `server forward-user <server> --capture-account` and
// `--no-capture-account`, against DBTrail's real web handlers, whole outputs
// compared.

func newCaptureDaemon(t *testing.T, e console.ServerEntry) (*console.Registry, string, *ctlClient) {
	t.Helper()
	f := newRouterFiles(t)
	added, err := f.owner.Add(e)
	if err != nil {
		t.Fatal(err)
	}
	srv, err := console.New(console.Config{Listen: "127.0.0.1:0", Token: "tok", Registry: f.owner})
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	return f.owner, added.ID, &ctlClient{base: ts.URL, token: "tok", hc: ts.Client()}
}

func pgCaptureEntry() console.ServerEntry {
	return console.ServerEntry{Name: "shop", DSN: "idxuser:idxsecret@tcp(127.0.0.1:1)/idx_shop", Flavor: console.FlavorPostgres,
		SourceDSN: pgAddSourceURL, SourceSlot: "bintrail_shopdb", SourcePublication: "bintrail_pub"}
}

func mysqlCaptureEntry() console.ServerEntry {
	return console.ServerEntry{Name: "shop", DSN: "idxuser:idxsecret@tcp(127.0.0.1:1)/idx_shop", SourceDSN: "dbtrail:capture-pw@tcp(db1:3306)/"}
}

func runFwd(t *testing.T, c *ctlClient, f fwdFlags) (string, error) {
	t.Helper()
	var out bytes.Buffer
	err := runServerForwardUser(context.Background(), c, "shop", f, &seqAsker{t: t}, strings.NewReader(""), &out)
	return out.String(), err
}

func TestServerForwardUserCaptureAccountPostgres(t *testing.T) {
	reg, id, c := newCaptureDaemon(t, pgCaptureEntry())
	answer := func() bool {
		e, _ := reg.Get(id)
		return e.ForwardWithCaptureAccount
	}
	steps := []struct {
		name  string
		flags fwdFlags
		stdin string
		want  string
		saved bool
	}{
		{"yes, no forwarding user", fwdFlags{CaptureAccount: true}, "",
			`"shop" forwards with the user DBTrail captures with (dbtrail): the PostgreSQL port sends to your PostgreSQL, read-only, a SELECT the copy does not answer, and only when every table it reads is a table of the copy. Anything else is refused and not sent. A server that hides tables or columns sends nothing.` + "\n", true},
		{"a forwarding user set over the yes", fwdFlags{User: "app_ro", PasswordStdin: true}, "fwd-pw\n", strings.TrimSuffix(pgForwardsWant("app_ro"), "\n"), true},
		{"yes again, with that user", fwdFlags{CaptureAccount: true}, "",
			`Saved. "shop" keeps forwarding as its forwarding user, app_ro, while that one is set. Removed (--remove), the user DBTrail captures with takes its place.` + "\n", true},
		{"no, with that user", fwdFlags{NoCaptureAccount: true}, "",
			`"shop" does not forward with the user DBTrail captures with. It forwards as its forwarding user, app_ro, as before.` + "\n", false},
		{"the user removed", fwdFlags{Remove: true}, "",
			`"shop" has no forwarding user now, so the PostgreSQL port does not serve it: a client that connects gets an error that says why and how to serve it. Serve it again:  bintrail-console server forward-user shop --capture-account` + "\n", false},
		{"no, and no user", fwdFlags{NoCaptureAccount: true}, "",
			`"shop" does not forward with the user DBTrail captures with. It has no forwarding user of its own, so the PostgreSQL port does not serve it: a client that connects gets an error that says why and how to serve it.` + "\n", false},
	}
	for _, s := range steps {
		var out bytes.Buffer
		err := runServerForwardUser(context.Background(), c, "shop", s.flags, &seqAsker{t: t}, strings.NewReader(s.stdin), &out)
		if err != nil {
			t.Fatalf("%s: %v", s.name, err)
		}
		if got := out.String(); got != s.want {
			t.Errorf("%s: output\n%s\nwant\n%s", s.name, got, s.want)
		}
		if answer() != s.saved {
			t.Errorf("%s: saved answer %v, want %v", s.name, answer(), s.saved)
		}
		t.Logf("%s:\n%s", s.name, out.String())
	}
}

func TestServerForwardUserCaptureAccountMySQL(t *testing.T) {
	reg, id, c := newCaptureDaemon(t, mysqlCaptureEntry())
	out, err := runFwd(t, c, fwdFlags{CaptureAccount: true})
	if err != nil {
		t.Fatal(err)
	}
	want := "Saved, and not used yet: the MySQL port does not forward with the user DBTrail captures with. Until \"shop\" has a forwarding user, a statement the copy cannot answer is refused:\n  bintrail-console server forward-user shop -u <user> -p\n"
	if out != want {
		t.Errorf("output\n%s\nwant\n%s", out, want)
	}
	if e, _ := reg.Get(id); !e.ForwardWithCaptureAccount || e.RouteDSN != "" {
		t.Errorf("saved: %+v", e)
	}
	out, err = runFwd(t, c, fwdFlags{NoCaptureAccount: true})
	if err != nil {
		t.Fatal(err)
	}
	if want := "\"shop\" does not forward with the user DBTrail captures with. It has no forwarding user, so a statement the copy cannot answer is refused.\n"; out != want {
		t.Errorf("output\n%s\nwant\n%s", out, want)
	}
	t.Logf("MySQL, yes:\n%s", want)
}

func TestServerForwardUserCaptureAccountRefusals(t *testing.T) {
	_, _, c := newCaptureDaemon(t, pgCaptureEntry())
	const both = "--capture-account and --no-capture-account take nothing else: they answer one question, whether the port may forward with the user DBTrail captures with"
	for _, f := range []fwdFlags{
		{CaptureAccount: true, NoCaptureAccount: true},
		{CaptureAccount: true, Remove: true},
		{CaptureAccount: true, User: "app_ro"},
		{NoCaptureAccount: true, AskPassword: true},
		{NoCaptureAccount: true, PasswordStdin: true},
	} {
		if _, err := runFwd(t, c, f); err == nil || err.Error() != both {
			t.Errorf("%+v: %v", f, err)
		}
	}
	// A server with no source has nothing to forward to: the daemon says so.
	_, _, c2 := newCaptureDaemon(t, console.ServerEntry{Name: "shop", DSN: "idxuser:idxsecret@tcp(127.0.0.1:1)/idx_shop"})
	if _, err := runFwd(t, c2, fwdFlags{CaptureAccount: true}); err == nil || !strings.Contains(err.Error(), "the server has no source database, so there is nothing to forward to; set its source first") {
		t.Errorf("no source: %v", err)
	}
}

// SHOW dbtrail.router_status names the account, from a real binding's
// Forward, whole rows compared.
func TestPGPortStatusNamesTheForwardingAccount(t *testing.T) {
	now := time.Date(2026, 11, 21, 15, 0, 0, 0, time.UTC)
	for _, c := range []struct {
		name string
		f    *pgshim.Forward
		want string
	}{
		{"the capture account", &pgshim.Forward{DSN: pgAddSourceURL, CaptureAccount: true},
			"server=7f4d577430b48821\nforwarding=on\nforwarding_account=capture account, read-only, tables of the copy only\ncopy_snapshot=unknown\ncopy_age_seconds=unknown\nlast_answer=none\n"},
		{"a forwarding user", &pgshim.Forward{DSN: "postgres://app_ro:pw@pg1:5432/shopdb"},
			"server=7f4d577430b48821\nforwarding=on\nforwarding_account=forwarding user app_ro\ncopy_snapshot=unknown\ncopy_age_seconds=unknown\nlast_answer=none\n"},
		{"none", &pgshim.Forward{Refused: console.PGRouteRefusedNoAccount},
			"server=7f4d577430b48821\nforwarding=off\nforwarding_off_reason=" + console.PGRouteRefusedNoAccount + "\nforwarding_account=none\ncopy_snapshot=unknown\ncopy_age_seconds=unknown\nlast_answer=none\n"},
	} {
		in := pgPortStatusInput{Server: "7f4d577430b48821", Now: now}
		in.ForwardingOff = pgForwardingOff(c.f, nil)
		in.ForwardingAccount = pgForwardingAccount(c.f)
		got := statusText(pgPortStatusRows(in))
		if got != c.want {
			t.Errorf("%s:\n got:\n%s\nwant:\n%s", c.name, got, c.want)
		}
		t.Logf("%s:\n%s", c.name, got)
	}
}

func TestServerForwardUserCaptureAccountOtherCases(t *testing.T) {
	// A PostgreSQL with the answer yes: removing its forwarding user hands
	// forwarding to the capture account, and the command says so.
	reg, id, c := newCaptureDaemon(t, pgCaptureEntry())
	if _, err := reg.SetForwardWithCaptureAccount(id, true); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := runServerForwardUser(context.Background(), c, "shop", fwdFlags{User: "app_ro", PasswordStdin: true}, &seqAsker{t: t}, strings.NewReader("fwd-pw\n"), &out); err != nil {
		t.Fatal(err)
	}
	got, err := runFwd(t, c, fwdFlags{Remove: true})
	if err != nil {
		t.Fatal(err)
	}
	if want := `"shop" has no forwarding user now. It may forward with the user DBTrail captures with, so the PostgreSQL port does that from now on: read-only, and only a SELECT whose every table is a table of the copy. Take that back with --no-capture-account.` + "\n"; got != want {
		t.Errorf("--remove with yes:\n%s\nwant\n%s", got, want)
	}

	// A MySQL with a forwarding user: the yes is saved and changes nothing.
	_, _, c = newCaptureDaemon(t, func() console.ServerEntry {
		e := mysqlCaptureEntry()
		e.RouteDSN = "app_ro:pw@tcp(db1:3306)/"
		return e
	}())
	got, err = runFwd(t, c, fwdFlags{CaptureAccount: true})
	if err != nil {
		t.Fatal(err)
	}
	if want := `Saved, and not used: the MySQL port does not forward with the user DBTrail captures with. "shop" keeps forwarding as its forwarding user, app_ro.` + "\n"; got != want {
		t.Errorf("MySQL with a forwarding user:\n%s\nwant\n%s", got, want)
	}

	// A PostgreSQL whose saved forwarding user is the capture user by hand:
	// with the yes, the port forwards as the capture account (0.140.0).
	_, _, c = newCaptureDaemon(t, func() console.ServerEntry {
		e := pgCaptureEntry()
		e.RouteDSN = pgAddSourceURL
		return e
	}())
	got, err = runFwd(t, c, fwdFlags{CaptureAccount: true})
	if err != nil {
		t.Fatal(err)
	}
	if want := `"shop" forwards with the user DBTrail captures with (dbtrail): the PostgreSQL port sends to your PostgreSQL, read-only, a SELECT the copy does not answer, and only when every table it reads is a table of the copy. Anything else is refused and not sent. A server that hides tables or columns sends nothing.` + "\n"; got != want {
		t.Errorf("a saved capture user:\n%s\nwant\n%s", got, want)
	}

	// A PostgreSQL whose source asks for replication.
	_, _, c = newCaptureDaemon(t, func() console.ServerEntry {
		e := pgCaptureEntry()
		e.SourceDSN = pgAddSourceURL + "&replication=database"
		return e
	}())
	got, err = runFwd(t, c, fwdFlags{CaptureAccount: true})
	if err != nil {
		t.Fatal(err)
	}
	if want := "Saved, and not used: the server's source URL asks for replication: the port forwards with the capture account only over a plain postgres:// URL with a host, a user, and no parameter that names the account or asks for replication; set a separate forwarding user instead, or correct the source.\n"; got != want {
		t.Errorf("a replication URL:\n%s\nwant\n%s", got, want)
	}
}
