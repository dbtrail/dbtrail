package console

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

// The PostgreSQL port serves a server only when it can forward (0.140.0):
// the reason a server is not served names it, says why, and gives both ways
// to serve it.
func TestPGPortNotServed(t *testing.T) {
	for _, c := range []struct {
		name, server, cause string
		has, hasNot         []string
	}{
		{name: "no account", server: "shop", cause: PGRouteRefusedNoAccount, has: []string{
			`the PostgreSQL port does not serve server "shop": it has no forwarding user, and forwarding as the account DBTrail captures with is turned off for it.`,
			"dbtrail server forward-user shop --capture-account",
			"dbtrail server forward-user shop -u <user> -p",
			"Then reconnect.",
		}},
		{name: "a cause that ends in a full stop is not stopped twice", server: "shop", cause: "the server's source URL asks for replication: " + pgCaptureFix,
			has: []string{"replication: " + strings.TrimSuffix(pgCaptureFix, ".") + ". "}, hasNot: []string{".."}},
		{name: "a name the shell would split is quoted in the commands", server: "my shop's db", cause: PGRouteRefusedUnreadable, has: []string{
			`server "my shop's db": its saved forwarding user cannot be read.`,
			`dbtrail server forward-user 'my shop'\''s db' --capture-account`,
		}},
		{name: "no cause: the server has no source", server: "shop", cause: "", has: []string{`"shop": ` + PGRouteRefusedNoSource + ". Set up its source first"},
			hasNot: []string{"--capture-account", "-u <user>"}},
		{name: "no source, said", server: "shop", cause: PGRouteRefusedNoSource, has: []string{"Set up its source first"},
			hasNot: []string{"--capture-account", "-u <user>"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			got := PGPortNotServed(c.server, c.cause)
			for _, w := range c.has {
				if !strings.Contains(got, w) {
					t.Errorf("missing %q in\n%s", w, got)
				}
			}
			for _, w := range c.hasNot {
				if strings.Contains(got, w) {
					t.Errorf("has %q in\n%s", w, got)
				}
			}
		})
	}
}

// What GET /api/servers says of a server the PostgreSQL port does not serve,
// so the web card and `server add` can say it before a client finds out.
func TestServerDTOPGPortRefused(t *testing.T) {
	s, reg := newFlashbackServer(t, "tok")
	none := addPG(t, reg, "", ServerEntry{SourceDSN: capPGSource})
	yes := addPG(t, reg, "", ServerEntry{SourceDSN: capPGSource})
	answerYes(t, reg, yes.ID)
	own := addPG(t, reg, "", ServerEntry{SourceDSN: capPGSource, RouteDSN: "postgres://app_ro:pw@db/shop"})
	capRoute := addPG(t, reg, "", ServerEntry{SourceDSN: capPGSource, RouteDSN: "postgres://repl:x@db/shop"})
	capRouteYes := addPG(t, reg, "", ServerEntry{SourceDSN: capPGSource, RouteDSN: "postgres://repl:x@db/shop"})
	answerYes(t, reg, capRouteYes.ID)
	noSource := addPG(t, reg, "", ServerEntry{})
	my := addByHand(t, reg, ServerEntry{Name: "my", DSN: "u:p@tcp(h:3306)/i9", SourceDSN: "repl:cap@tcp(db:3306)/"})
	for _, c := range []struct {
		name string
		e    ServerEntry
		want string
	}{
		{"no account", none, PGPortNotServed(none.Name, PGRouteRefusedNoAccount)},
		{"yes", yes, ""},
		{"an account of its own", own, ""},
		{"the capture account saved, no answer", capRoute, PGPortNotServed(capRoute.Name, PGRouteRefusedIsCapture)},
		{"the capture account saved, and yes", capRouteYes, ""},
		{"no source", noSource, PGPortNotServed(noSource.Name, PGRouteRefusedNoSource)},
		{"a MySQL server", my, ""},
	} {
		e, _ := reg.Get(c.e.ID)
		if got := s.entryDTO(e).PGPortRefused; got != c.want {
			t.Errorf("%s: %q, want %q", c.name, got, c.want)
		}
		if c.e.IsPostgres() {
			if got := s.PGPortRefusal(c.e.ID); got != c.want {
				t.Errorf("%s: PGPortRefusal %q, want %q", c.name, got, c.want)
			}
		}
	}
	if got := s.PGPortRefusal("nope"); got == "" {
		t.Error("an unknown server is served")
	}
}

// The boot warning lists every PostgreSQL server the port will not serve,
// and nothing else.
func TestPGServersNotServed(t *testing.T) {
	_, reg := newFlashbackServer(t, "tok")
	none := addPG(t, reg, "zeta", ServerEntry{SourceDSN: capPGSource})
	yes := addPG(t, reg, "alpha", ServerEntry{SourceDSN: capPGSource})
	answerYes(t, reg, yes.ID)
	other := addPG(t, reg, "beta", ServerEntry{SourceDSN: capPGSource, RouteDSN: "u:p@tcp(db:3306)/"})
	addByHand(t, reg, ServerEntry{Name: "my", DSN: "u:p@tcp(h:3306)/i8", SourceDSN: "repl:cap@tcp(db:3306)/"})
	got := PGServersNotServed(reg)
	if len(got) != 2 || got[0].Name != other.Name || got[1].Name != none.Name {
		t.Fatalf("got %+v", got)
	}
	if got[0].Reason != PGPortNotServed(other.Name, PGRouteRefusedUnreadable) || got[1].Reason != PGPortNotServed(none.Name, PGRouteRefusedNoAccount) {
		t.Errorf("reasons %q / %q", got[0].Reason, got[1].Reason)
	}
}

// warnerCtrl is a MonitorController that says a capture role is powerful.
type warnerCtrl struct {
	*stubMonitorCtrl
	asked int
}

func (w *warnerCtrl) CaptureRoleWarning(_ context.Context, e ServerEntry) string {
	w.asked++
	return "The role \"repl\" is a superuser"
}

// Saving the yes about the capture account answers with what the checks say
// of that account's power, so `server forward-user --capture-account`, the
// command upgraders are told to run, says it too.
func TestRouteCaptureAccountAPIWarnsOfAPowerfulRole(t *testing.T) {
	reg, err := LoadRegistry(t.TempDir() + "/console-servers.yaml")
	if err != nil {
		t.Fatal(err)
	}
	ctrl := &warnerCtrl{stubMonitorCtrl: &stubMonitorCtrl{}}
	srv, err := New(Config{Listen: "127.0.0.1:8090", Token: "t", Registry: reg, MonitorCtrl: ctrl})
	if err != nil {
		t.Fatal(err)
	}
	pg := addPG(t, reg, "", ServerEntry{SourceDSN: capPGSource})
	own := addPG(t, reg, "", ServerEntry{SourceDSN: capPGSource, RouteDSN: "postgres://app_ro:pw@db/shop"})
	for _, c := range []struct {
		name, id, body, want string
		asked                int
	}{
		{"yes, as the capture account", pg.ID, `{"capture_account":true}`, `The role "repl" is a superuser`, 1},
		{"no", pg.ID, `{"capture_account":false}`, "", 1},
		{"yes beside an account of its own, which is used instead", own.ID, `{"capture_account":true}`, "", 1},
	} {
		rec, b := doServersReq(t, srv, "PUT", "/api/servers/"+c.id+"/route", c.body)
		var dto serverDTO
		if rec.Code != 200 || json.Unmarshal(b, &dto) != nil {
			t.Fatalf("%s: %d %s", c.name, rec.Code, b)
		}
		if dto.CaptureAccountWarning != c.want || ctrl.asked != c.asked {
			t.Errorf("%s: warning %q (asked %d times), want %q (%d)", c.name, dto.CaptureAccountWarning, ctrl.asked, c.want, c.asked)
		}
	}
}
