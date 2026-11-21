package console

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	yaml "go.yaml.in/yaml/v2"
)

// #3054: the saved answer about forwarding with the capture account, and
// what each port does with it.

const capPGSource = "postgres://repl:cap@db:5432/shop"

var addPGSeq int

func addPG(t *testing.T, reg *Registry, name string, e ServerEntry) ServerEntry {
	t.Helper()
	if name == "" {
		addPGSeq++
		name = fmt.Sprintf("pg%d", addPGSeq)
	}
	e.Name, e.Flavor = name, FlavorPostgres
	if e.DSN == "" {
		e.DSN = "u:p@tcp(h:3306)/idx_" + name
	}
	if e.SourceSlot == "" {
		e.SourceSlot, e.SourcePublication = "s", "p"
	}
	added, err := reg.Add(e)
	if err != nil {
		t.Fatal(err)
	}
	return added
}

func answerYes(t *testing.T, reg *Registry, id string) {
	t.Helper()
	if _, err := reg.SetForwardWithCaptureAccount(id, true); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresForwardTargetCaptureAnswer(t *testing.T) {
	s, reg := newFlashbackServer(t, "tok")
	if s.routed == nil {
		s.routed = newRoutedConns()
	}
	for _, c := range []struct {
		name, source, route string
		yes                 bool
		want                PGForwardTarget
	}{
		{name: "no answer saved: as before", source: capPGSource,
			want: PGForwardTarget{Refused: PGRouteRefusedNoAccount}},
		{name: "yes, no account of its own: the capture account", source: capPGSource, yes: true,
			want: PGForwardTarget{DSN: capPGSource, CaptureAccount: true, Account: PGForwardsWithCapture}},
		{name: "yes, and an account of its own: that one", source: capPGSource, route: "postgres://app_ro:pw@db/shop", yes: true,
			want: PGForwardTarget{DSN: "postgres://app_ro:pw@db/shop", Account: "forwarding user app_ro"}},
		{name: "no answer, an account of its own", source: capPGSource, route: "postgres://app_ro:pw@db/shop",
			want: PGForwardTarget{DSN: "postgres://app_ro:pw@db/shop", Account: "forwarding user app_ro"}},
		{name: "yes, the forwarding field holds the capture account by hand: refused as before", source: capPGSource, route: "postgres://repl:x@db/shop", yes: true,
			want: PGForwardTarget{Refused: PGRouteRefusedIsCapture}},
		{name: "yes, the forwarding field unreadable: refused as before", source: capPGSource, route: "u:p@tcp(db:3306)/", yes: true,
			want: PGForwardTarget{Refused: PGRouteRefusedUnreadable}},
		{name: "yes, a source that asks for replication", source: capPGSource + "?replication=database", yes: true,
			want: PGForwardTarget{Refused: "the server's source URL asks for replication: " + pgCaptureFix}},
		{name: "yes, a source that asks for replication in capitals", source: capPGSource + "?Replication=true", yes: true,
			want: PGForwardTarget{Refused: "the server's source URL asks for replication: " + pgCaptureFix}},
		{name: "yes, a source that names its account in a parameter", source: capPGSource + "?user=postgres", yes: true,
			want: PGForwardTarget{Refused: "the server's source URL cannot be used to forward: " + pgCaptureFix}},
		{name: "yes, a source on a socket", source: "postgres://repl:cap@/shop?host=/var/run/postgresql", yes: true,
			want: PGForwardTarget{Refused: "the server's source URL cannot be used to forward: " + pgCaptureFix}},
	} {
		t.Run(c.name, func(t *testing.T) {
			e := addPG(t, reg, "", ServerEntry{SourceDSN: c.source, RouteDSN: c.route})
			if c.yes {
				answerYes(t, reg, e.ID)
			}
			got := s.PostgresForwardTarget(e.ID)
			got.Gen = 0
			if got != c.want {
				t.Errorf("got  %+v\nwant %+v", got, c.want)
			}
			// The MySQL port's view of a PostgreSQL server is unchanged.
			if dsn, why := s.flashbackForwardDSN(e.ID); dsn != "" || why != RouteRefusedNotMySQL {
				t.Errorf("flashbackForwardDSN = %q, %q", dsn, why)
			}
		})
	}
	if got := s.PostgresForwardTarget("nope"); got.DSN != "" || got.Refused != "" || got.CaptureAccount {
		t.Errorf("an unknown id: %+v", got)
	}
}

// The MySQL port has no gate for the capture account: a yes is saved and
// never acted on there, and it says so.
func TestMySQLPortDoesNotForwardWithTheCaptureAccount(t *testing.T) {
	s, reg := newFlashbackServer(t, "tok")
	none := addByHand(t, reg, ServerEntry{Name: "none", DSN: "u:p@tcp(h:3306)/i1", SourceDSN: "repl:cap@tcp(db:3306)/"})
	yes := addByHand(t, reg, ServerEntry{Name: "yes", DSN: "u:p@tcp(h:3306)/i2", SourceDSN: "repl:cap@tcp(db:3306)/"})
	answerYes(t, reg, yes.ID)
	own := addByHand(t, reg, ServerEntry{Name: "own", DSN: "u:p@tcp(h:3306)/i3", SourceDSN: "repl:cap@tcp(db:3306)/", RouteDSN: "app:pw@tcp(db:3306)/"})
	answerYes(t, reg, own.ID)

	for _, c := range []struct {
		id, dsn, refused string
		rows             [][2]string
	}{
		{none.ID, "", RouteRefusedNoAccount, [][2]string{{"forwarding_account", "none"}, {"forwarding_account_reason", RouteRefusedNoAccount}}},
		{yes.ID, "", RouteRefusedCaptureNotOnThisPort, [][2]string{{"forwarding_account", "none"}, {"forwarding_account_reason", RouteRefusedCaptureNotOnThisPort}}},
		{own.ID, "app:pw@tcp(db:3306)/", "", [][2]string{{"forwarding_account", "forwarding user app"}}},
	} {
		dsn, refused := s.flashbackForwardDSN(c.id)
		if dsn != c.dsn || refused != c.refused {
			t.Errorf("%s: flashbackForwardDSN = %q, %q; want %q, %q", c.id, dsn, refused, c.dsn, c.refused)
		}
		if got := s.forwardingRows(c.id); !slices.Equal(got, c.rows) {
			t.Errorf("%s: rows = %q, want %q", c.id, got, c.rows)
		}
		if strings.Contains(dsn, "repl") {
			t.Errorf("the capture account reached the MySQL port: %q", dsn)
		}
	}
	// The line as a client of the port reads it, from the real rows.
	rows := s.RouterStatus(t.Context(), yes.ID, nil)
	if len(rows) < 3 || rows[1] != [2]string{"forwarding_account", "none"} || rows[2][0] != "forwarding_account_reason" {
		t.Fatalf("SHOW ROUTER STATUS: %q", rows)
	}
	t.Logf("SHOW ROUTER STATUS, a MySQL server with the answer yes and no forwarding user:\n%s | %s\n%s | %s", rows[1][0], rows[1][1], rows[2][0], rows[2][1])
}

func TestForwardWithCaptureAccountRegistryOwned(t *testing.T) {
	_, reg := newFlashbackServer(t, "tok")
	// Add never stores it: the answer is given, not carried in.
	e := addPG(t, reg, "pg", ServerEntry{SourceDSN: capPGSource, ForwardWithCaptureAccount: true})
	if e.ForwardWithCaptureAccount {
		t.Fatal("Add stored the answer")
	}
	// No source: nothing to forward to.
	view := addPG(t, reg, "view", ServerEntry{})
	if _, err := reg.SetForwardWithCaptureAccount(view.ID, true); !errors.Is(err, errNoSourceToForward) {
		t.Errorf("yes with no source: %v", err)
	}
	if _, err := reg.SetForwardWithCaptureAccount(view.ID, false); err != nil {
		t.Errorf("no with no source: %v", err)
	}
	if _, err := reg.SetForwardWithCaptureAccount("nope", true); !errors.Is(err, ErrUnknownServer) {
		t.Errorf("unknown id: %v", err)
	}

	for _, c := range []struct {
		name, source string
		keep         bool
	}{
		{"the same source", capPGSource, true},
		{"a new password", "postgres://repl:rotated@db:5432/shop", true},
		{"the host in capitals and with its trailing dot", "postgres://repl:cap@DB.:5432/shop", true},
		{"the port left out (5432)", "postgres://repl:cap@db/shop", true},
		{"another database", "postgres://repl:cap@db:5432/other", true},
		{"another user", "postgres://postgres:cap@db:5432/shop", false},
		{"the user in capitals (PostgreSQL roles are case-sensitive)", "postgres://Repl:cap@db:5432/shop", false},
		{"another host", "postgres://repl:cap@db2:5432/shop", false},
		{"another port", "postgres://repl:cap@db:6432/shop", false},
		{"the source removed", "", false},
		{"a source that does not read", "::not a url", false},
	} {
		t.Run(c.name, func(t *testing.T) {
			cur := addPG(t, reg, "", ServerEntry{SourceDSN: capPGSource})
			answerYes(t, reg, cur.ID)
			cur, _ = reg.Get(cur.ID)
			next := cur
			next.SourceDSN = c.source
			next.ForwardWithCaptureAccount = false // an edit form that does not carry it
			if err := reg.Update(next); err != nil {
				t.Fatal(err)
			}
			got, _ := reg.Get(cur.ID)
			if got.ForwardWithCaptureAccount != c.keep {
				t.Errorf("after the edit: %v, want %v", got.ForwardWithCaptureAccount, c.keep)
			}
			// An edit cannot set it either.
			if !c.keep {
				next.ForwardWithCaptureAccount = true
				next.SourceDSN = capPGSource
				if err := reg.Update(next); err != nil {
					t.Fatal(err)
				}
				if got, _ := reg.Get(cur.ID); got.ForwardWithCaptureAccount {
					t.Error("an edit set the answer")
				}
			}
		})
	}
}

func TestForwardWithCaptureAccountMySQLSourceEdits(t *testing.T) {
	_, reg := newFlashbackServer(t, "tok")
	for _, c := range []struct {
		name, source string
		keep         bool
	}{
		{"a new password", "repl:new@tcp(db:3306)/", true},
		{"another host", "repl:cap@tcp(db2:3306)/", false},
		{"the port left out", "repl:cap@tcp(db)/", true},
		{"another user", "root:cap@tcp(db:3306)/", false},
		{"the user in capitals (MySQL user names are case-sensitive)", "REPL:cap@tcp(db:3306)/", false},
	} {
		t.Run(c.name, func(t *testing.T) {
			addPGSeq++
			n := fmt.Sprintf("m%d", addPGSeq)
			e := addByHand(t, reg, ServerEntry{Name: n, DSN: "u:p@tcp(h:3306)/" + n, SourceDSN: "repl:cap@tcp(db:3306)/"})
			answerYes(t, reg, e.ID)
			e, _ = reg.Get(e.ID)
			e.SourceDSN = c.source
			if err := reg.Update(e); err != nil {
				t.Fatal(err)
			}
			if got, _ := reg.Get(e.ID); got.ForwardWithCaptureAccount != c.keep {
				t.Errorf("after the edit: %v, want %v", got.ForwardWithCaptureAccount, c.keep)
			}
		})
	}
}

// An entry saved before the field existed reads as no; the field is written
// only once it is yes, under its own key, so a file nobody answered for is
// written as before.
func TestForwardWithCaptureAccountYAML(t *testing.T) {
	var e ServerEntry
	if err := yaml.Unmarshal([]byte("id: a\nname: shop\nindex_dsn: u:p@tcp(h)/i\nsource_dsn: postgres://repl:cap@db/shop\nflavor: postgres\n"), &e); err != nil {
		t.Fatal(err)
	}
	if e.ForwardWithCaptureAccount {
		t.Fatal("an entry from before the field reads as yes")
	}
	out, err := yaml.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(out), "capture") {
		t.Errorf("no is written: %s", out)
	}
	e.ForwardWithCaptureAccount = true
	out, _ = yaml.Marshal(e)
	if !strings.Contains(string(out), "\nforward_with_capture_account: true\n") {
		t.Errorf("yes is not written under its key: %s", out)
	}
	// A build that predates the field reads the key as unknown: it keeps it
	// and does not act on it (Extra), which is no.
	var old struct {
		ID    string         `yaml:"id"`
		Extra map[string]any `yaml:",inline"`
	}
	if err := yaml.Unmarshal(out, &old); err != nil || old.Extra["forward_with_capture_account"] != true {
		t.Errorf("an older build's view: %v %v", old.Extra, err)
	}
}

func TestForwardChanged(t *testing.T) {
	pg := ServerEntry{Flavor: FlavorPostgres, SourceDSN: capPGSource}
	yes := pg
	yes.ForwardWithCaptureAccount = true
	own, ownYes := pg, yes
	own.RouteDSN, ownYes.RouteDSN = "postgres://app:pw@db/shop", "postgres://app:pw@db/shop"
	my := ServerEntry{SourceDSN: "repl:cap@tcp(db:3306)/"}
	myYes := my
	myYes.ForwardWithCaptureAccount = true
	for _, c := range []struct {
		name      string
		old, next ServerEntry
		want      bool
	}{
		{"nothing changed", pg, pg, false},
		{"yes given, no account of its own", pg, yes, true},
		{"yes taken back", yes, pg, true},
		{"yes given beside an account of its own", own, ownYes, false},
		{"an account of its own set over a yes", yes, ownYes, true},
		{"a MySQL server's yes, no account of its own", my, myYes, true},
	} {
		if got := forwardChanged(c.old, c.next); got != c.want {
			t.Errorf("%s: %v, want %v", c.name, got, c.want)
		}
	}
}

// The API: PUT /api/servers/{id}/route with "capture_account" alone.
func TestRouteCaptureAccountAPI(t *testing.T) {
	srv, _ := newSupervisorServer(t)
	id := addedWithSchemas(t, srv)
	before := entryOf(t, srv, id)
	put := func(body string) (int, string) {
		rec, b := doServersReq(t, srv, "PUT", "/api/servers/"+id+"/route", body)
		return rec.Code, strings.TrimSpace(string(b))
	}
	if code, b := put(`{"capture_account":true}`); code != 200 {
		t.Fatalf("yes: %d %s", code, b)
	} else {
		var dto serverDTO
		if err := json.Unmarshal([]byte(b), &dto); err != nil || !dto.ForwardWithCaptureAccount {
			t.Errorf("answer: %s %v", b, err)
		}
	}
	after := entryOf(t, srv, id)
	want := before
	want.ForwardWithCaptureAccount = true
	if a, b := entryJSON(t, after), entryJSON(t, want); a != b {
		t.Errorf("something other than the answer changed:\n got %s\nwant %s", a, b)
	}
	for _, c := range []struct{ body, msg string }{
		{`{"capture_account":true,"user":"x","password":"y"}`, `send "capture_account" alone, or "user" and "password" without it: they are two settings`},
		{`{"capture_account":false,"user":"x"}`, `send "capture_account" alone, or "user" and "password" without it: they are two settings`},
		{`{"capture_account":null}`, `send "user", "password" or both, none of them empty, or "capture_account" alone; DELETE removes the forwarding account`},
		{`{"capture_account":"yes"}`, ""},
		{`{"capture_account":1}`, ""},
	} {
		code, b := put(c.body)
		if code != 400 {
			t.Errorf("%s: %d %s", c.body, code, b)
			continue
		}
		if c.msg != "" && !strings.Contains(b, c.msg) {
			t.Errorf("%s: %s", c.body, b)
		}
	}
	if !entryOf(t, srv, id).ForwardWithCaptureAccount {
		t.Fatal("a refused request changed the answer")
	}
	// Removing the forwarding account leaves the answer: it is another
	// setting.
	if rec, b := doServersReq(t, srv, "DELETE", "/api/servers/"+id+"/route", ""); rec.Code != 200 {
		t.Fatalf("delete: %d %s", rec.Code, b)
	}
	if !entryOf(t, srv, id).ForwardWithCaptureAccount {
		t.Error("DELETE took the answer back")
	}
	// The edit form's route replaces the entry and does not carry it: it
	// is kept.
	if rec, b := doServersReq(t, srv, "PUT", "/api/servers/"+id, `{"name":"shop2"}`); rec.Code != 200 {
		t.Fatalf("edit: %d %s", rec.Code, b)
	}
	if !entryOf(t, srv, id).ForwardWithCaptureAccount {
		t.Error("a full edit took the answer back")
	}
	if code, b := put(`{"capture_account":false}`); code != 200 || entryOf(t, srv, id).ForwardWithCaptureAccount {
		t.Errorf("no: %d %s", code, b)
	}
	if rec, b := doServersReq(t, srv, "PUT", "/api/servers/nope/route", `{"capture_account":true}`); rec.Code != 404 {
		t.Errorf("unknown server: %d %s", rec.Code, b)
	}
	if rec, b := doServersReq(t, srv, "PUT", "/api/servers/"+bootServerID+"/route", `{"capture_account":true}`); rec.Code != 409 {
		t.Errorf("the command-line server: %d %s", rec.Code, b)
	}
}
