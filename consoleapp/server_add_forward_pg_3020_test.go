package consoleapp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/console"
)

// #3020: `server add --flavor postgres` takes the forwarding user the way a
// MySQL add does, and saves it the way `server forward-user` does.

// seqAsker answers each secret in turn, so the capture password and the
// forwarding user's can differ (fakeAsker gives one answer to both).
type seqAsker struct {
	t       *testing.T
	tty     bool
	lines   map[string]string
	secrets []string
	asked   []string
	asks    []string // the secret questions, as asked
}

func (a *seqAsker) Interactive() bool { return a.tty }

func (a *seqAsker) Line(q, def string) (string, error) {
	a.asked = append(a.asked, q)
	ans, ok := a.lines[q]
	if !ok {
		a.t.Fatalf("asked %q, which the test did not expect", q)
	}
	if ans == "" {
		return def, nil
	}
	return ans, nil
}

func (a *seqAsker) Secret(q string) (string, error) {
	a.asks = append(a.asks, q)
	if len(a.secrets) == 0 {
		a.t.Fatalf("asked the secret %q, which the test did not expect", q)
	}
	s := a.secrets[0]
	a.secrets = a.secrets[1:]
	return s, nil
}

func TestResolveAddInputPostgresForward(t *testing.T) {
	const plain = "[client]\nhost=pg1\nuser=dbtrail\npassword=capture-pw\n"
	const withGroup = plain + "[dbtrail-forward]\nuser=app_ro\npassword=fwd-pw\n"
	pgFlags := func(f addFlags) addFlags {
		f.Flavor, f.Database, f.Publication = "postgres", "shop", "bintrail_pub"
		return f
	}
	term := addFlags{Host: "pg1", User: "dbtrail", AskPassword: true}
	for _, tc := range []struct {
		name     string
		flags    addFlags
		cnf      string
		tty      bool
		secrets  []string
		wantUser string
		wantPW   string
		wantAsk  bool
		wantErr  string // the whole message
		wantAsks []string
	}{
		{name: "nothing named, a terminal: asked once the server is added", flags: term, tty: true, secrets: []string{"capture-pw"},
			wantAsk: true, wantAsks: []string{"Password of dbtrail on pg1"}},
		{name: "nothing named, no terminal: not asked", cnf: plain},
		{name: "--no-forward on a terminal: not asked", flags: addFlags{Host: "pg1", User: "dbtrail", AskPassword: true, NoForward: true},
			tty: true, secrets: []string{"capture-pw"}, wantAsks: []string{"Password of dbtrail on pg1"}},
		{name: "--no-forward without a terminal", cnf: plain, flags: addFlags{NoForward: true}},
		{name: "the option file's group", cnf: withGroup, wantUser: "app_ro", wantPW: "fwd-pw"},
		{name: "the group's password with --forward-user naming the same user", cnf: withGroup, flags: addFlags{ForwardUser: "app_ro"},
			wantUser: "app_ro", wantPW: "fwd-pw"},
		{name: "--forward-user names another user than the group, no terminal", cnf: withGroup, flags: addFlags{ForwardUser: "report_ro"},
			wantErr: "no password for the forwarding user report_ro: this is not a terminal, so it cannot be asked for. Put it under [dbtrail-forward] in the option file, or use --no-forward"},
		{name: "--forward-user on a terminal: its password is asked before anything is sent",
			flags: addFlags{Host: "pg1", User: "dbtrail", AskPassword: true, ForwardUser: "app_ro"}, tty: true, secrets: []string{"capture-pw", "fwd-pw"},
			wantUser: "app_ro", wantPW: "fwd-pw", wantAsks: []string{"Password of dbtrail on pg1", "Password of the forwarding user app_ro"}},
		{name: "--forward-user on a terminal, and an empty password typed",
			flags: addFlags{Host: "pg1", User: "dbtrail", AskPassword: true, ForwardUser: "app_ro"}, tty: true, secrets: []string{"capture-pw", ""},
			wantErr: "the forwarding user's password is empty"},
		{name: "--forward-user of spaces alone is no user: asked later",
			flags: addFlags{Host: "pg1", User: "dbtrail", AskPassword: true, ForwardUser: "   "}, tty: true, secrets: []string{"capture-pw"},
			wantAsk: true, wantAsks: []string{"Password of dbtrail on pg1"}},
		{name: "spaces around the name are dropped; capitals and a space inside are the role's",
			flags: addFlags{Host: "pg1", User: "dbtrail", AskPassword: true, ForwardUser: "  App RO "}, tty: true, secrets: []string{"capture-pw", "fwd-pw"},
			wantUser: "App RO", wantPW: "fwd-pw", wantAsks: []string{"Password of dbtrail on pg1", "Password of the forwarding user App RO"}},
		{name: "quotes in the name are kept as typed",
			flags: addFlags{Host: "pg1", User: "dbtrail", AskPassword: true, ForwardUser: `o'brien"ro`}, tty: true, secrets: []string{"capture-pw", "fwd-pw"},
			wantUser: `o'brien"ro`, wantPW: "fwd-pw", wantAsks: []string{"Password of dbtrail on pg1", `Password of the forwarding user o'brien"ro`}},
		// A PostgreSQL role's name is case-sensitive: DBTRAIL is another role.
		{name: "the capture user's name in capitals is another role", cnf: plain + "[dbtrail-forward]\nuser=DBTRAIL\npassword=x\n",
			wantUser: "DBTRAIL", wantPW: "x"},
		{name: "a group with a user and no password, no terminal", cnf: plain + "[dbtrail-forward]\nuser=app_ro\n",
			wantErr: "no password for the forwarding user app_ro: this is not a terminal, so it cannot be asked for. Put it under [dbtrail-forward] in the option file, or use --no-forward"},
		{name: "a group with a password and no user", cnf: plain + "[dbtrail-forward]\npassword=fwd-pw\n",
			wantErr: "[dbtrail-forward] in the option file has a password and no user: add a user line"},
		{name: "the group names the capture user", cnf: plain + "[dbtrail-forward]\nuser=dbtrail\npassword=x\n",
			wantErr: "dbtrail is the user DBTrail reads this database with, and the port never forwards as that one: it can read far more than an application should. Name another user, with your application's grants"},
		{name: "--forward-user names the capture user", cnf: plain, flags: addFlags{ForwardUser: "dbtrail"},
			wantErr: "dbtrail is the user DBTrail reads this database with, and the port never forwards as that one: it can read far more than an application should. Name another user, with your application's grants"},
		{name: "--no-forward and --forward-user", cnf: plain, flags: addFlags{NoForward: true, ForwardUser: "app_ro"},
			wantErr: "--no-forward and --forward-user say opposite things: use one"},
		{name: "--no-forward and the group", cnf: withGroup, flags: addFlags{NoForward: true},
			wantErr: "--no-forward and [dbtrail-forward] in the option file say opposite things: use one"},
		// [client] and [dbtrail] hold the capture user, never the forwarding one.
		{name: "[client] and [dbtrail] are not the forwarding account", cnf: plain + "[dbtrail]\nuser=dbtrail\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := pgFlags(tc.flags)
			if tc.cnf != "" {
				f.DefaultsFile = writeCnf(t, tc.cnf)
			}
			if tc.tty {
				// Everything a PostgreSQL needs is on the command line but
				// the slot, whose default is offered.
				f.Slot = "bintrail_shop"
			}
			ask := &seqAsker{t: t, tty: tc.tty, secrets: tc.secrets}
			in, err := resolveAddInput(f, ask, strings.NewReader(""))
			if tc.wantErr != "" {
				if err == nil || err.Error() != tc.wantErr {
					t.Fatalf("err = %v\nwant  %s", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if in.Flavor != console.FlavorPostgres || in.Database != "shop" || in.Publication != "bintrail_pub" || in.Slot != "bintrail_shop" {
				t.Fatalf("the PostgreSQL's own fields: %+v", in)
			}
			if in.FwdUser != tc.wantUser || in.FwdPassword != tc.wantPW || in.FwdAsk != tc.wantAsk {
				t.Fatalf("forward user %q password %q ask %v, want %q %q %v", in.FwdUser, in.FwdPassword, in.FwdAsk, tc.wantUser, tc.wantPW, tc.wantAsk)
			}
			if in.FwdAsk && in.ask == nil {
				t.Fatal("to be asked, with nobody to ask")
			}
			if !slices.Equal(ask.asks, tc.wantAsks) {
				t.Fatalf("secrets asked %q, want %q", ask.asks, tc.wantAsks)
			}
			if len(ask.secrets) != 0 {
				t.Fatalf("secrets left unasked: %d", len(ask.secrets))
			}
		})
	}
}

// The option file on stdin (the Docker command sends it so): both groups come
// out of it for a PostgreSQL too, and nothing is left to ask with.
func TestResolveAddInputPostgresForwardFromStdin(t *testing.T) {
	cnf := "[client]\nhost=pg1\nuser=dbtrail\npassword=capture-pw\n[dbtrail-forward]\nuser=app_ro\npassword=fwd-pw\n"
	f := addFlags{DefaultsFile: "-", Flavor: "postgres", Database: "shop", Publication: "bintrail_pub"}
	in, err := resolveAddInput(f, &seqAsker{t: t, tty: true}, strings.NewReader(cnf))
	if err != nil {
		t.Fatal(err)
	}
	if in.Password != "capture-pw" || in.FwdUser != "app_ro" || in.FwdPassword != "fwd-pw" || in.FwdAsk || in.Slot != "bintrail_shop" {
		t.Fatalf("got %+v", in)
	}
	in, err = resolveAddInput(f, &seqAsker{t: t, tty: true}, strings.NewReader("[client]\nhost=pg1\nuser=dbtrail\npassword=capture-pw\n"))
	if err != nil || in.FwdAsk || in.FwdUser != "" {
		t.Fatalf("file on stdin, no group: %+v, %v", in, err)
	}
}

// A MySQL's flags are read as before: the forwarding user of a MySQL add is
// not touched by a PostgreSQL's.
func TestResolveAddInputMySQLForwardUnchangedBy3020(t *testing.T) {
	in, err := resolveAddInput(addFlags{Host: "db1", User: "dbtrail", AskPassword: true, ForwardUser: "shop_fwd"},
		&seqAsker{t: t, tty: true, secrets: []string{"capture-pw", "fwd-pw"}}, strings.NewReader(""))
	if err != nil {
		t.Fatal(err)
	}
	in.ask = nil
	want := addInput{Host: "db1", Port: "3306", User: "dbtrail", Password: "capture-pw", FwdUser: "shop_fwd", FwdPassword: "fwd-pw"}
	if in != want {
		t.Fatalf("got %+v\nwant %+v", in, want)
	}
}

// pgAddDaemon is DBTrail's own web server over a registry that holds one
// PostgreSQL server, "shop", as `server add` would have just saved it. The
// check is answered here (its checks need a PostgreSQL), with the server as
// the real one lists it; every other request, the forwarding user's among
// them, is answered by the real handlers. So what is saved, and each refusal,
// is DBTrail's own.
type pgAddDaemon struct {
	t     *testing.T
	reg   *console.Registry
	id    string
	calls []string
}

const pgAddSourceURL = "postgres://dbtrail:capture-pw@pg1:5432/shopdb?sslmode=require"

func newPGAddDaemon(t *testing.T, sourceURL string) (*pgAddDaemon, *ctlClient) {
	t.Helper()
	f := newRouterFiles(t)
	e, err := f.owner.Add(console.ServerEntry{Name: "shop", DSN: "idxuser:idxsecret@tcp(127.0.0.1:1)/idx_shop", Flavor: console.FlavorPostgres,
		SourceDSN: sourceURL, SourceSlot: "bintrail_shopdb", SourcePublication: "bintrail_pub"})
	if err != nil {
		t.Fatal(err)
	}
	srv, err := console.New(console.Config{Listen: "127.0.0.1:0", Token: "tok", Registry: f.owner})
	if err != nil {
		t.Fatal(err)
	}
	d := &pgAddDaemon{t: t, reg: f.owner, id: e.ID}
	daemon := srv.Handler()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		d.calls = append(d.calls, r.Method+" "+r.URL.Path)
		if r.Method != http.MethodPost || r.URL.Path != "/api/servers/check" {
			daemon.ServeHTTP(w, r)
			return
		}
		// The server, as the real list has it.
		rec := httptest.NewRecorder()
		lr := httptest.NewRequest(http.MethodGet, "/api/servers", nil)
		lr.Host = r.Host
		lr.Header.Set("Authorization", r.Header.Get("Authorization"))
		daemon.ServeHTTP(rec, lr)
		var list struct {
			Servers []json.RawMessage `json:"servers"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &list); err != nil {
			t.Errorf("the real list: %v\n%s", err, rec.Body.String())
		}
		var dto json.RawMessage
		for _, raw := range list.Servers {
			var s ctlServer
			if json.Unmarshal(raw, &s) == nil && s.ID == d.id {
				dto = raw
			}
		}
		if dto == nil {
			t.Errorf("the real list does not hold the server: %s", rec.Body.String())
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "name": "shop", "started": true, "doctor": passReport(), "server": dto,
			"port": map[string]any{"enabled": true, "turned_on": true, "routing_on": true, "port": "3309", "password": "bfp_portpw"}})
	}))
	t.Cleanup(ts.Close)
	return d, &ctlClient{base: ts.URL, token: "tok", hc: ts.Client()}
}

// forwardURL is the forwarding account as it is saved, "" for none.
func (d *pgAddDaemon) forwardURL() string {
	e, ok := d.reg.Get(d.id)
	if !ok {
		d.t.Fatal("the server is gone from the registry")
	}
	return e.RouteDSN
}

func (d *pgAddDaemon) routeCalls() int {
	n := 0
	for _, c := range d.calls {
		if strings.HasSuffix(c, "/route") {
			n++
		}
	}
	return n
}

func pgAddInput() addInput {
	return addInput{Host: "pg1", Port: "5432", User: "dbtrail", Password: "capture-pw", Name: "shop", Flavor: "postgres",
		Database: "shopdb", Slot: "bintrail_shopdb", Publication: "bintrail_pub"}
}

// afterAdded is what `server add` printed from "Added" on: the part that is
// about the port and the forwarding user.
func afterAdded(t *testing.T, out string) string {
	t.Helper()
	i := strings.Index(out, "\nAdded ")
	if i < 0 {
		t.Fatalf("no Added line:\n%s", out)
	}
	return out[i:]
}

const pgAddedHead = `
Added "shop". DBTrail is recording its changes.

The port for SQL clients is on now. PostgreSQL clients connect to the PostgreSQL port (5433, unless the PostgreSQL router was given another), with the server's name as the database and any user:

  psql "host=127.0.0.1 port=5433 dbname=shop user=app sslmode=require"

Password (shown only now, keep it): bfp_portpw
Statements for "shop" are answered once its first copy is ready.
`

const pgNoForwardYet = `Until "shop" has a forwarding user, the copy answers its statements alone, and a statement it cannot answer gets an error:
  bintrail-console server forward-user shop -u <user> -p
`

const pgNextLine = "Next, the first copy of its tables:  bintrail-console snapshot run shop\n"

func TestServerAddPostgresSetsTheForwardingUserItWasGiven(t *testing.T) {
	d, c := newPGAddDaemon(t, pgAddSourceURL)
	in := pgAddInput()
	in.FwdUser, in.FwdPassword = "app_ro", "fwd-pw"
	var out bytes.Buffer
	if err := runServerAdd(context.Background(), c, in, false, &out); err != nil {
		t.Fatalf("err = %v\n%s", err, out.String())
	}
	want := pgAddedHead + "\n" +
		`"shop" forwards as app_ro: the PostgreSQL port sends to your PostgreSQL, as that user and read-only, every statement the copy is not known to answer as PostgreSQL does, and a client's transaction blocks.` + "\n\n" +
		pgNextLine
	if got := afterAdded(t, out.String()); got != want {
		t.Fatalf("output:\n%s\nwant:\n%s", got, want)
	}
	// Laid over the source's own address, database and parameters.
	if got := d.forwardURL(); got != "postgres://app_ro:fwd-pw@pg1:5432/shopdb?sslmode=require" {
		t.Fatalf("saved forwarding account = %q", got)
	}
	if strings.Contains(out.String(), "fwd-pw") || strings.Contains(out.String(), "capture-pw") {
		t.Fatalf("a password in the output:\n%s", out.String())
	}
}

// A role whose name holds capitals, a space and quotes, and a password with
// what a URL reads as syntax: saved as typed, and read back as typed.
func TestServerAddPostgresForwardingUserWithAnOddName(t *testing.T) {
	d, c := newPGAddDaemon(t, pgAddSourceURL)
	in := pgAddInput()
	in.FwdUser, in.FwdPassword = `App "RO" o'b`, "p@ss/w:rd #1?"
	var out bytes.Buffer
	if err := runServerAdd(context.Background(), c, in, false, &out); err != nil {
		t.Fatalf("err = %v\n%s", err, out.String())
	}
	u, err := url.Parse(d.forwardURL())
	if err != nil {
		t.Fatal(err)
	}
	pw, _ := u.User.Password()
	if u.User.Username() != `App "RO" o'b` || pw != "p@ss/w:rd #1?" || u.Host != "pg1:5432" || u.Path != "/shopdb" || u.Query().Get("sslmode") != "require" {
		t.Fatalf("saved forwarding account reads back as user %q password %q host %q path %q query %q", u.User.Username(), pw, u.Host, u.Path, u.RawQuery)
	}
	if !strings.Contains(out.String(), `"shop" forwards as App "RO" o'b: the PostgreSQL port sends`) {
		t.Fatalf("output:\n%s", out.String())
	}
}

func TestServerAddPostgresWithNoForwardingUserSaysHowToSetOne(t *testing.T) {
	d, c := newPGAddDaemon(t, pgAddSourceURL)
	var out bytes.Buffer
	// Nothing named and nobody to ask: a script, or --no-forward.
	if err := runServerAdd(context.Background(), c, pgAddInput(), false, &out); err != nil {
		t.Fatalf("err = %v\n%s", err, out.String())
	}
	want := pgAddedHead + pgNoForwardYet + "\n" + pgNextLine
	if got := afterAdded(t, out.String()); got != want {
		t.Fatalf("output:\n%s\nwant:\n%s", got, want)
	}
	if d.routeCalls() != 0 || d.forwardURL() != "" {
		t.Fatalf("calls %v, saved %q", d.calls, d.forwardURL())
	}
}

const pgQuestionIntro = "\nThe forwarding user: what DBTrail's PostgreSQL port runs statements as on your PostgreSQL, read-only, when the copy is not known to answer them as PostgreSQL does, and a client's transaction blocks. A role of its own that can log in, with SELECT on what the port should read and no more.\n"

func TestServerAddPostgresAsksForTheForwardingUser(t *testing.T) {
	t.Run("Enter skips it", func(t *testing.T) {
		d, c := newPGAddDaemon(t, pgAddSourceURL)
		ask := &seqAsker{t: t, tty: true, lines: map[string]string{"Forwarding user (Enter to skip)": ""}}
		in := pgAddInput()
		in.FwdAsk, in.ask = true, ask
		var out bytes.Buffer
		if err := runServerAdd(context.Background(), c, in, false, &out); err != nil {
			t.Fatalf("err = %v\n%s", err, out.String())
		}
		want := pgAddedHead + pgNoForwardYet + "\n" + pgQuestionIntro +
			"Skipped: the copy answers alone. Set it later:  bintrail-console server forward-user shop -u <user> -p\n\n" + pgNextLine
		if got := afterAdded(t, out.String()); got != want {
			t.Fatalf("output:\n%s\nwant:\n%s", got, want)
		}
		if d.routeCalls() != 0 || d.forwardURL() != "" || len(ask.asks) != 0 {
			t.Fatalf("calls %v, saved %q, secrets asked %q", d.calls, d.forwardURL(), ask.asks)
		}
	})
	t.Run("a user and its password", func(t *testing.T) {
		d, c := newPGAddDaemon(t, pgAddSourceURL)
		ask := &seqAsker{t: t, tty: true, secrets: []string{"fwd-pw"}, lines: map[string]string{"Forwarding user (Enter to skip)": " app_ro "}}
		in := pgAddInput()
		in.FwdAsk, in.ask = true, ask
		var out bytes.Buffer
		if err := runServerAdd(context.Background(), c, in, false, &out); err != nil {
			t.Fatalf("err = %v\n%s", err, out.String())
		}
		want := pgAddedHead + pgNoForwardYet + "\n" + pgQuestionIntro +
			`"shop" forwards as app_ro: the PostgreSQL port sends to your PostgreSQL, as that user and read-only, every statement the copy is not known to answer as PostgreSQL does, and a client's transaction blocks.` + "\n\n" + pgNextLine
		if got := afterAdded(t, out.String()); got != want {
			t.Fatalf("output:\n%s\nwant:\n%s", got, want)
		}
		if !slices.Equal(ask.asks, []string{"Password of app_ro on your PostgreSQL"}) {
			t.Fatalf("secrets asked %q", ask.asks)
		}
		if got := d.forwardURL(); got != "postgres://app_ro:fwd-pw@pg1:5432/shopdb?sslmode=require" {
			t.Fatalf("saved forwarding account = %q", got)
		}
	})
	t.Run("the capture user typed at the question", func(t *testing.T) {
		d, c := newPGAddDaemon(t, pgAddSourceURL)
		ask := &seqAsker{t: t, tty: true, lines: map[string]string{"Forwarding user (Enter to skip)": "dbtrail"}}
		in := pgAddInput()
		in.FwdAsk, in.ask = true, ask
		var out bytes.Buffer
		err := runServerAdd(context.Background(), c, in, false, &out)
		const want = `the database was added and is recording, without a forwarding user. dbtrail is the user DBTrail reads "shop" with, and the port never forwards as that one. Set another one:  bintrail-console server forward-user shop -u <user> -p`
		if !errors.Is(err, errAddedNoForward) || err.Error() != want {
			t.Fatalf("err = %v\nwant  %s", err, want)
		}
		if d.routeCalls() != 0 || d.forwardURL() != "" || len(ask.asks) != 0 {
			t.Fatalf("calls %v, saved %q, secrets asked %q", d.calls, d.forwardURL(), ask.asks)
		}
		if !strings.HasSuffix(out.String(), pgNextLine) {
			t.Fatalf("the next step is not said:\n%s", out.String())
		}
	})
	t.Run("an empty password typed", func(t *testing.T) {
		d, c := newPGAddDaemon(t, pgAddSourceURL)
		ask := &seqAsker{t: t, tty: true, secrets: []string{""}, lines: map[string]string{"Forwarding user (Enter to skip)": "app_ro"}}
		in := pgAddInput()
		in.FwdAsk, in.ask = true, ask
		err := runServerAdd(context.Background(), c, in, false, &bytes.Buffer{})
		const want = `the database was added and is recording, without a forwarding user: the password is empty. Set it:  bintrail-console server forward-user shop -u app_ro -p`
		if !errors.Is(err, errAddedNoForward) || err.Error() != want {
			t.Fatalf("err = %v\nwant  %s", err, want)
		}
		if d.routeCalls() != 0 || d.forwardURL() != "" {
			t.Fatalf("calls %v, saved %q", d.calls, d.forwardURL())
		}
	})
}

// The capture user given where resolveForward did not see it (a caller that
// built the input itself): setForwardUser refuses it before anything is sent,
// and names the source as a PostgreSQL.
func TestServerAddPostgresRefusesTheCaptureUser(t *testing.T) {
	d, c := newPGAddDaemon(t, pgAddSourceURL)
	in := pgAddInput()
	in.FwdUser, in.FwdPassword = "dbtrail", "capture-pw"
	var out bytes.Buffer
	err := runServerAdd(context.Background(), c, in, false, &out)
	const want = `the database was added and is recording, without a forwarding user: dbtrail is the user DBTrail reads "shop" with, and the port never forwards as that one: it can read far more than an application should. Create another user on your PostgreSQL, with the application's grants, and set that one
Set it once that is fixed:  bintrail-console server forward-user shop -u dbtrail -p`
	if !errors.Is(err, errAddedNoForward) || err.Error() != want {
		t.Fatalf("err = %v\nwant  %s", err, want)
	}
	if d.routeCalls() != 0 || d.forwardURL() != "" {
		t.Fatalf("calls %v, saved %q", d.calls, d.forwardURL())
	}
}

// DBTrail itself refuses the account: the server stays added and recording,
// the refusal is DBTrail's own words, and the port's password was already
// printed. Here the source is reached through a socket directory, which a
// forwarding account cannot be laid over.
func TestServerAddPostgresWhoseForwardingUserDBTrailRefusesStaysAdded(t *testing.T) {
	d, c := newPGAddDaemon(t, "postgres://dbtrail:capture-pw@pg1:5432/shopdb?host=/var/run/postgresql")
	in := pgAddInput()
	in.FwdUser, in.FwdPassword = `App RO`, "fwd-pw"
	var out bytes.Buffer
	err := runServerAdd(context.Background(), c, in, false, &out)
	if !errors.Is(err, errAddedNoForward) {
		t.Fatalf("err = %v, want errAddedNoForward\n%s", err, out.String())
	}
	// A name the shell would split is not pasted into the command.
	const tail = "\nSet it once that is fixed:  bintrail-console server forward-user shop -u <user> -p"
	const head = "the database was added and is recording, without a forwarding user: a forwarding account needs a source reached over the network, and this one is a socket directory; "
	if msg := err.Error(); !strings.HasPrefix(msg, head) || !strings.HasSuffix(msg, tail) {
		t.Fatalf("err = %s", msg)
	}
	if d.routeCalls() != 1 || d.forwardURL() != "" {
		t.Fatalf("calls %v, saved %q", d.calls, d.forwardURL())
	}
	if _, ok := d.reg.Get(d.id); !ok {
		t.Fatal("the server was removed")
	}
	want := pgAddedHead + "\n" + pgNextLine
	if got := afterAdded(t, out.String()); got != want {
		t.Fatalf("output:\n%s\nwant:\n%s", got, want)
	}
}

// `server forward-user` still sets, changes and removes it afterwards, with
// the sentence `server add` prints.
func TestServerForwardUserStillWorksAfterAPostgresAdd(t *testing.T) {
	d, c := newPGAddDaemon(t, pgAddSourceURL)
	in := pgAddInput()
	in.FwdUser, in.FwdPassword = "app_ro", "fwd-pw"
	if err := runServerAdd(context.Background(), c, in, false, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	err := runServerForwardUser(context.Background(), c, "shop", fwdFlags{User: "report_ro", PasswordStdin: true}, &seqAsker{t: t}, strings.NewReader("report-pw\n"), &out)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := out.String(), `"shop" forwards as report_ro: the PostgreSQL port sends to your PostgreSQL, as that user and read-only, every statement the copy is not known to answer as PostgreSQL does, and a client's transaction blocks.`+"\n"; got != want {
		t.Fatalf("output:\n%s\nwant:\n%s", got, want)
	}
	if got := d.forwardURL(); got != "postgres://report_ro:report-pw@pg1:5432/shopdb?sslmode=require" {
		t.Fatalf("saved forwarding account = %q", got)
	}
	out.Reset()
	if err := runServerForwardUser(context.Background(), c, "shop", fwdFlags{Remove: true}, &seqAsker{t: t}, strings.NewReader(""), &out); err != nil {
		t.Fatal(err)
	}
	if got, want := out.String(), `"shop" has no forwarding user now: the PostgreSQL port answers its clients from the copy alone. A statement the copy cannot answer gets an error, and nothing is sent to your PostgreSQL.`+"\n"; got != want {
		t.Fatalf("output:\n%s\nwant:\n%s", got, want)
	}
	if d.forwardURL() != "" {
		t.Fatalf("still saved: %q", d.forwardURL())
	}
}

// Nothing is asked, and nothing set, for a PostgreSQL whose checks failed or
// whose answer does not say which server was saved.
func TestServerAddPostgresForwardingUserWhenTheAddDidNotGoThrough(t *testing.T) {
	t.Run("the checks failed", func(t *testing.T) {
		d, c := newFakeDaemon(t)
		d.on("POST /api/servers/check", 200, map[string]any{"ok": false, "name": "shop", "doctor": map[string]any{"failed": 1, "checks": []map[string]any{{"name": "wal_level is logical", "status": "fail"}}}})
		ask := &seqAsker{t: t, tty: true}
		in := pgAddInput()
		in.FwdAsk, in.ask = true, ask
		if err := runServerAdd(context.Background(), c, in, false, &bytes.Buffer{}); !errors.Is(err, errNotAdded) {
			t.Fatalf("err = %v", err)
		}
		if len(ask.asked) != 0 || len(d.calls) != 1 {
			t.Fatalf("asked %v, calls %v", ask.asked, d.calls)
		}
	})
	t.Run("an answer that does not say which server was saved", func(t *testing.T) {
		d, c := newFakeDaemon(t)
		d.on("POST /api/servers/check", 200, map[string]any{"ok": true, "name": "shop", "started": true, "doctor": passReport()})
		in := pgAddInput()
		in.FwdUser, in.FwdPassword = "app_ro", "x"
		err := runServerAdd(context.Background(), c, in, false, &bytes.Buffer{})
		const want = `the database was added and is recording, without a forwarding user: DBTrail did not say which server it saved. Set it:  bintrail-console server forward-user shop -u app_ro -p`
		if !errors.Is(err, errAddedNoForward) || err.Error() != want {
			t.Fatalf("err = %v\nwant  %s", err, want)
		}
		if len(d.calls) != 1 {
			t.Fatalf("calls %v", d.calls)
		}
	})
	// DBTrail says it saved the user and the server does not forward as it.
	t.Run("an answer that took the user and did not save it", func(t *testing.T) {
		d, c := newFakeDaemon(t)
		d.on("POST /api/servers/check", 200, map[string]any{"ok": true, "name": "shop", "started": true, "doctor": passReport(),
			"server": map[string]any{"id": "id9", "name": "shop", "has_source": true, "source_user": "dbtrail"}})
		d.on("PUT /api/servers/id9/route", 200, map[string]any{"id": "id9", "name": "shop"})
		in := pgAddInput()
		in.FwdUser, in.FwdPassword = "app_ro", "x"
		err := runServerAdd(context.Background(), c, in, false, &bytes.Buffer{})
		const want = "the database was added and is recording, without a forwarding user: DBTrail took the request and \"shop\" does not forward as app_ro. See `bintrail-console server list`\nSet it once that is fixed:  bintrail-console server forward-user shop -u app_ro -p"
		if !errors.Is(err, errAddedNoForward) || err.Error() != want {
			t.Fatalf("err = %v\nwant  %s", err, want)
		}
	})
}

func TestPGShellUser(t *testing.T) {
	for in, want := range map[string]string{
		"app_ro": "app_ro", "App.RO-2@x": "App.RO-2@x", "": "<user>", "App RO": "<user>", `o'brien`: "<user>", `a"b`: "<user>", "a;b": "<user>", "a$b": "<user>", "ünï": "<user>",
	} {
		if got := pgShellUser(in); got != want {
			t.Errorf("pgShellUser(%q) = %q, want %q", in, got, want)
		}
	}
}
