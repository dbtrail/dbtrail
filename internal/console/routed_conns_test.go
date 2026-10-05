package console

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/config"
)

// TestRoutedConns: a tracked connection is closed by a drop, once; one that
// ended on its own is not; and a connection whose target was read before the
// drop is refused when it comes to register.
func TestRoutedConns(t *testing.T) {
	r := newRoutedConns()
	var a, b, c, other atomic.Int32
	gen := r.generation("s1")
	untrackA, ok := r.track("s1", gen, func() { a.Add(1) }, func() uint32 { return 11 })
	if !ok {
		t.Fatal("first track refused")
	}
	if _, ok := r.track("s1", gen, func() { b.Add(1) }, func() uint32 { return 12 }); !ok {
		t.Fatal("second track refused")
	}
	if _, ok := r.track("s2", r.generation("s2"), func() { other.Add(1) }, func() uint32 { return 13 }); !ok {
		t.Fatal("other server's track refused")
	}
	untrackA() // ended on its own
	underLock := 0
	threads := r.drop("s1", func() { underLock++ })
	if len(threads) != 1 || threads[0]() != 12 || a.Load() != 0 || b.Load() != 1 || other.Load() != 0 || underLock != 1 {
		t.Fatalf("drop closed %d (a=%d b=%d other=%d), want only b, and its source thread", len(threads), a.Load(), b.Load(), other.Load())
	}
	if n := len(r.drop("s1", nil)); n != 0 || b.Load() != 1 {
		t.Errorf("a second drop closed %d again (b=%d)", n, b.Load())
	}
	// Something recorded for a connection of a previous generation is not
	// recorded: the account it speaks of is no longer the server's.
	ran := 0
	r.whileCurrent("s1", gen, func() { ran++ })
	r.whileCurrent("s1", r.generation("s1"), func() { ran += 10 })
	if ran != 10 {
		t.Errorf("whileCurrent ran %d, want only the current generation's (10)", ran)
	}
	// Read before the drops, registering after: refused, never tracked.
	if _, ok := r.track("s1", gen, func() { c.Add(1) }, nil); ok {
		t.Error("a connection bound from a target read before the drop was accepted")
	}
	if _, ok := r.track("s1", r.generation("s1"), func() { c.Add(1) }, nil); !ok {
		t.Error("a connection bound after the drop was refused")
	}
	if len(r.drop("s1", nil)) != 1 || c.Load() != 1 {
		t.Errorf("the connection bound after the drop was not tracked (c=%d)", c.Load())
	}
}

// TestServersAPI_AccountChangeDropsRoutedConns: saving, changing or removing
// the forwarding account, changing the source, and deleting the server each
// close that server's open port connections; an edit that leaves the account
// alone closes nothing.
func TestServersAPI_AccountChangeDropsRoutedConns(t *testing.T) {
	srv := newRegistryServer(t)
	// The serving layer's KILL: what it was asked to end, and as whom.
	type kill struct {
		user, addr string
		ids        []uint32
	}
	var killMu sync.Mutex
	var kills []kill
	srv.killSourceThreads = func(_ context.Context, dsn string, _ config.SSL, ids []uint32) error {
		cfg, err := mysql.ParseDSN(dsn)
		if err != nil {
			return err
		}
		killMu.Lock()
		kills = append(kills, kill{cfg.User, cfg.Addr, ids})
		killMu.Unlock()
		return nil
	}
	rec, body := doServersReq(t, srv, "POST", "/api/servers",
		`{"name":"prod","host":"h","user":"u","dbname":"db","source_host":"db.prod","source_user":"repl","source_password":"replpw"}`)
	if rec.Code != 201 {
		t.Fatalf("create: %d %s", rec.Code, body)
	}
	var dto serverDTO
	if err := json.Unmarshal(body, &dto); err != nil {
		t.Fatal(err)
	}
	var dropped atomic.Int32
	open := func() {
		t.Helper()
		// As the port does: the generation from the target, then track.
		gen := srv.routed.generation(dto.ID)
		if _, ok := srv.TrackRoutedConn(dto.ID, gen, func() { dropped.Add(1) }, func() uint32 { return 77 }); !ok {
			t.Fatal("track refused")
		}
		// One that never opened a connection to the source: nothing to end.
		if _, ok := srv.TrackRoutedConn(dto.ID, gen, func() {}, func() uint32 { return 0 }); !ok {
			t.Fatal("track refused")
		}
	}
	// killAs: the account whose statements are ended on the source, the one
	// the connections were opened with BEFORE the edit.
	steps := []struct {
		name, method, body string
		wantDrop           bool
		killAs             string
	}{
		{"an unrelated edit", "PUT", `{"name":"prod-2","host":"h","user":"u","dbname":"db"}`, false, ""},
		{"saving a forwarding account", "PUT", `{"name":"prod-2","host":"h","user":"u","dbname":"db","route_user":"fwd","route_password":"pw"}`, true, "repl"},
		{"the form's plain save", "PUT", `{"name":"prod-2","host":"h","user":"u","dbname":"db","route_user":"fwd"}`, false, ""},
		{"changing its password", "PUT", `{"name":"prod-2","host":"h","user":"u","dbname":"db","route_password":"pw2"}`, true, "fwd"},
		{"removing it", "PUT", `{"name":"prod-2","host":"h","user":"u","dbname":"db","route_user":""}`, true, "fwd"},
		{"removing it again", "PUT", `{"name":"prod-2","host":"h","user":"u","dbname":"db","route_user":""}`, false, ""},
		{"changing the source's password", "PUT", `{"name":"prod-2","host":"h","user":"u","dbname":"db","source_password":"other"}`, true, "repl"},
		{"deleting the server", "DELETE", ``, true, "repl"},
	}
	for _, st := range steps {
		open()
		gen := srv.routed.generation(dto.ID)
		srv.RecordRouteAccountRefused(dto.ID, gen, "the forwarding account fwd: MySQL error 1045")
		before := dropped.Load()
		killMu.Lock()
		kills = nil
		killMu.Unlock()
		rec, body := doServersReq(t, srv, st.method, "/api/servers/"+dto.ID, st.body)
		if rec.Code >= 300 {
			t.Fatalf("%s: %d %s", st.name, rec.Code, body)
		}
		got := dropped.Load() - before
		if st.wantDrop && got == 0 {
			t.Errorf("%s: the open connection was left on the previous account", st.name)
		}
		if !st.wantDrop && got != 0 {
			t.Errorf("%s: closed %d connection(s) for an edit that changed no account", st.name, got)
		}
		// What the source said about the previous account goes with it; an
		// edit that changed no account keeps what is known about this one.
		note := srv.routing.snapshot()[dto.ID].AccountRefused
		if st.wantDrop && note != "" {
			t.Errorf("%s: the page still says the previous account was refused: %q", st.name, note)
		}
		if !st.wantDrop && note == "" {
			t.Errorf("%s: an edit that changed no account forgot that this one is refused", st.name)
		}
		// The statements those connections were running are ended on the
		// source, as the account they ran as, in one go; a connection that
		// never reached the source is not named.
		srv.routedKills.Wait()
		killMu.Lock()
		got2 := append([]kill(nil), kills...)
		killMu.Unlock()
		switch {
		case !st.wantDrop && len(got2) != 0:
			t.Errorf("%s: asked the source to end statements (%+v) for an edit that changed no account", st.name, got2)
		case st.wantDrop && (len(got2) != 1 || got2[0].user != st.killAs || got2[0].addr != "db.prod:3306" || len(got2[0].ids) != 1 || got2[0].ids[0] != 77):
			t.Errorf("%s: asked the source to end %+v, want thread 77 as %s at db.prod:3306, once", st.name, got2, st.killAs)
		}
		// A connection bound before the edit that reports its login only
		// now speaks of the previous account: not recorded.
		if st.wantDrop {
			srv.RecordRouteAccountRefused(dto.ID, gen, "the previous account: MySQL error 1045")
			if note := srv.routing.snapshot()[dto.ID].AccountRefused; note != "" {
				t.Errorf("%s: a connection bound before the edit recorded a refusal after it: %q", st.name, note)
			}
			srv.RecordRouteAccountRefused(dto.ID, gen+1, "the new account: MySQL error 1045")
			if note := srv.routing.snapshot()[dto.ID].AccountRefused; note == "" && st.method != "DELETE" {
				t.Errorf("%s: a connection bound after the edit could not record a refusal", st.name)
			}
			srv.RecordRouteAccountOK(dto.ID, gen) // the old connection's late login clears nothing
			if note := srv.routing.snapshot()[dto.ID].AccountRefused; note == "" && st.method != "DELETE" {
				t.Errorf("%s: a login of the previous account cleared the new account's refusal", st.name)
			}
			srv.RecordRouteAccountOK(dto.ID, gen+1)
		}
		if !st.wantDrop {
			srv.routed.drop(dto.ID, nil) // start the next step clean
		}
	}
}

// TestFlashbackAPI_AccountRefused: what the source said when it turned the
// port's account away reaches the Connect page's data, and goes when a
// connection logs in.
func TestFlashbackAPI_AccountRefused(t *testing.T) {
	s, err := New(Config{Listen: "127.0.0.1:8090", Token: "secret-tok", FlashbackListen: "127.0.0.1:3308",
		ReadRouting: ReadRoutingConfig{MaxCopyAge: 15 * time.Minute, CostThreshold: 10000, ScanRows: 100000}})
	if err != nil {
		t.Fatal(err)
	}
	read := func() routingServerDTO {
		rec := doJSON(t, s, "GET", "/api/flashback", "secret-tok")
		var got flashbackStatusDTO
		if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
			t.Fatalf("decode: %v (%s)", err, rec.Body.String())
		}
		return got.Routing.Servers["srv-1"]
	}
	s.RecordRouteAccountRefused("srv-1", 0, "the forwarding account report_ro (MySQL error 1045: Access denied)")
	if got := read(); got.AccountRefused != "the forwarding account report_ro (MySQL error 1045: Access denied)" || got.Reasons == nil {
		t.Errorf("after a refusal: %+v", got)
	}
	s.RecordRouteDecision("srv-1", "mysql", "upstream_lost")
	if got := read(); got.AccountRefused == "" || got.MySQL != 1 {
		t.Errorf("a refusal beside a tally: %+v", got)
	}
	s.RecordRouteAccountOK("srv-1", 0)
	if got := read(); got.AccountRefused != "" {
		t.Errorf("after a login the note is still there: %+v", got)
	}
}

// fakeSource stands for the serving layer's login probe
// (Config.RouteAccountProbe): it knows one account, fwd / pw, at any
// address, and records every login it was asked to try.
type fakeSource struct {
	mu     sync.Mutex
	tried  map[string]int // address -> attempts
	logins map[string]int // address -> accepted
	ssl    []config.SSL
	// alsoKnows is a second user the source accepts (password pw).
	alsoKnows string
}

func (f *fakeSource) probe(_ context.Context, dsn string, ssl config.SSL, _ time.Duration) error {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.tried[cfg.Addr]++
	f.ssl = append(f.ssl, ssl)
	if (cfg.User != "fwd" && cfg.User != f.alsoKnows) || cfg.Passwd != "pw" {
		return fmt.Errorf("ERROR 1045 (28000): Access denied for user '%s'@'10.0.0.5' (using password: YES)", cfg.User)
	}
	f.logins[cfg.Addr]++
	return nil
}

// TestServersAPI_TestConnectionTriesTheForwardingAccount: Test connection
// logs in with the forwarding account, saved or being typed, and says which
// account failed and why. A saved password is never sent to a host or user
// it was not saved for.
func TestServersAPI_TestConnectionTriesTheForwardingAccount(t *testing.T) {
	src := &fakeSource{tried: map[string]int{}, logins: map[string]int{}}
	const host, port, otherPort = "db.prod", "3306", "3307"
	srcAddr, elsewhere := host+":"+port, host+":"+otherPort
	srv := newRegistryServer(t)
	srv.routeAccountProbe = src.probe
	const secret = "pw"
	base := `"name":"prod","dsn":"u:p@tcp(127.0.0.1:1)/db","source_host":"` + host + `","source_port":"` + port + `","source_user":"repl","source_password":"replpw"`
	rec, body := doServersReq(t, srv, "POST", "/api/servers", `{`+base+`,"route_user":"fwd","route_password":"`+secret+`"}`)
	if rec.Code != 201 {
		t.Fatalf("create: %d %s", rec.Code, body)
	}
	var dto serverDTO
	if err := json.Unmarshal(body, &dto); err != nil {
		t.Fatal(err)
	}
	// The server's TLS mode lives in the registry file, not in the API.
	entry, _ := srv.cm.reg.Get(dto.ID)
	entry.SSLMode = "required"
	if err := srv.cm.reg.Update(entry); err != nil {
		t.Fatal(err)
	}
	probe := func(what, reqBody string) *routeProbeResult {
		t.Helper()
		rec, body := doServersReq(t, srv, "POST", "/api/servers/"+dto.ID+"/test", reqBody)
		if rec.Code != 200 {
			t.Fatalf("%s: %d %s", what, rec.Code, body)
		}
		if strings.Contains(string(body), `"`+secret+`"`) || strings.Contains(string(body), "fwd:") || strings.Contains(string(body), "wrong-pw") {
			t.Fatalf("%s: the answer carries a password or a DSN: %s", what, body)
		}
		var resp testResponse
		if err := json.Unmarshal(body, &resp); err != nil {
			t.Fatal(err)
		}
		return resp.Route
	}
	// The row's button: the saved account, as saved.
	if r := probe("saved", `{}`); r == nil || !r.OK || r.User != "fwd" || r.Error != "" {
		t.Errorf("saved account: %+v, want ok for user fwd", r)
	}
	if src.logins[srcAddr] != 1 {
		t.Errorf("the source saw %d login(s), want 1", src.logins[srcAddr])
	}
	// With the server's own TLS settings, the ones the port uses.
	if len(src.ssl) < 1 || src.ssl[0].Mode != "required" {
		t.Errorf("the login was tried with TLS settings %+v, want the server's (required)", src.ssl)
	}
	// The page said the source had refused this account; a login that works
	// takes that back. One for an account only typed in the form does not:
	// it says nothing about the saved one.
	gen := srv.routed.generation(dto.ID)
	refusal := func() string { return srv.routing.snapshot()[dto.ID].AccountRefused }
	srv.RecordRouteAccountRefused(dto.ID, gen, "the forwarding account fwd: MySQL error 1045")
	src.mu.Lock()
	src.alsoKnows = "typed"
	src.mu.Unlock()
	if r := probe("typed, not saved", `{"route_user":"typed","route_password":"pw"}`); r == nil || !r.OK {
		t.Fatalf("typed account: %+v", r)
	}
	if refusal() == "" {
		t.Error("a login with an account that is not the saved one cleared the saved one's refusal")
	}
	if r := probe("wrong, saved stays refused", `{"route_user":"fwd","route_password":"wrong-pw"}`); r == nil || r.OK || refusal() == "" {
		t.Errorf("a failed login cleared the refusal: %+v", r)
	}
	if r := probe("saved again", `{}`); r == nil || !r.OK || refusal() != "" {
		t.Errorf("a login with the saved account left the refusal on the page: %+v, %q", r, refusal())
	}
	// A wrong password being typed: refused by the source, said as such.
	r := probe("wrong password", `{"route_user":"fwd","route_password":"wrong-pw"}`)
	if r == nil || r.OK || r.User != "fwd" || !strings.Contains(r.Error, "Access denied") {
		t.Errorf("wrong password: %+v, want the source's Access denied for user fwd", r)
	}
	// The source's own words, not what a client of the port would get.
	if r != nil && (strings.Contains(r.Error, "gone away") || strings.Contains(r.Error, "reconnect")) {
		t.Errorf("wrong password: the answer is the port's error 2006, not the source's: %q", r.Error)
	}
	// A user that is not there.
	if r := probe("unknown user", `{"route_user":"nobody","route_password":"pw"}`); r == nil || r.OK || r.User != "nobody" || r.Error == "" {
		t.Errorf("unknown user: %+v", r)
	}
	// The source moved in the form, the forwarding password not re-typed:
	// the saved password must not go to the new address.
	before := src.tried[elsewhere]
	r = probe("moved, password not typed", `{"source_host":"`+host+`","source_port":"`+otherPort+`","route_user":"fwd"}`)
	if r == nil || !r.NeedsPassword || r.OK || r.Error != "" {
		t.Errorf("moved without the password: %+v, want needs_password", r)
	}
	if src.tried[elsewhere] != before {
		t.Error("the saved forwarding password was sent to an address it was not saved for")
	}
	// Typed again, it is tried there.
	if r := probe("moved, password typed", `{"source_host":"`+host+`","source_port":"`+otherPort+`","route_user":"fwd","route_password":"pw"}`); r == nil || !r.OK {
		t.Errorf("moved with the password: %+v, want ok", r)
	}
	if src.logins[elsewhere] != 1 {
		t.Errorf("the new address saw %d login(s), want 1", src.logins[elsewhere])
	}
	// A request the server would refuse to save says why here too.
	if r := probe("capture account", `{"route_user":"repl","route_password":"x"}`); r == nil || r.OK || !strings.Contains(r.Error, "capture") {
		t.Errorf("the capture account as forwarding account: %+v", r)
	}
	// Removing it in the form: nothing to try.
	if r := probe("being removed", `{"route_user":""}`); r != nil {
		t.Errorf("an account being removed was probed: %+v", r)
	}
	// A server with none: no result at all.
	doServersReq(t, srv, "PUT", "/api/servers/"+dto.ID, `{`+base+`,"route_user":""}`)
	if r := probe("none", `{}`); r != nil {
		t.Errorf("a server with no forwarding account got a result: %+v", r)
	}
	// A server not saved yet, with the account typed on its form.
	rec, body = doServersReq(t, srv, "POST", "/api/servers/test", `{`+base+`,"route_user":"fwd","route_password":"pw"}`)
	var resp testResponse
	if err := json.Unmarshal(body, &resp); err != nil || rec.Code != 200 {
		t.Fatalf("unsaved: %d %s (%v)", rec.Code, body, err)
	}
	if resp.Route == nil || !resp.Route.OK || resp.Route.User != "fwd" {
		t.Errorf("unsaved server: %+v, want ok for user fwd", resp.Route)
	}
	// A process with no port to forward from tries no login, and still says
	// what the server would refuse to save.
	srv.routeAccountProbe = nil
	tried := len(src.ssl)
	doServersReq(t, srv, "PUT", "/api/servers/"+dto.ID, `{`+base+`,"route_user":"fwd","route_password":"pw"}`)
	if r := probe("no client", `{}`); r == nil || r.OK || r.Error != "" || r.User != "fwd" || !strings.Contains(r.Skipped, "no MySQL port") {
		t.Errorf("with no client: %+v, want the account named and said as not tried", r)
	}
	if r := probe("no client, capture account", `{"route_user":"repl","route_password":"x"}`); r == nil || !strings.Contains(r.Error, "capture") {
		t.Errorf("with no client the refusal is lost: %+v", r)
	}
	if len(src.ssl) != tried {
		t.Error("a login was tried with no client configured")
	}
}

// TestConfigRouteAccountProbeReachesTheServer: the probe the serving layer
// supplies is the one Test connection calls.
func TestConfigRouteAccountProbeReachesTheServer(t *testing.T) {
	called := 0
	s, err := New(Config{Listen: "127.0.0.1:8090", Token: "tok", RouteAccountProbe: func(context.Context, string, config.SSL, time.Duration) error {
		called++
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	req := serverRequest{SourceHost: "db.prod", SourceUser: "repl", SourcePassword: strPtr("x"), RouteUser: strPtr("fwd"), RoutePassword: strPtr("pw")}
	if r := s.probeRouteAccount(context.Background(), req, ServerEntry{}, false); r == nil || !r.OK || called != 1 {
		t.Errorf("probe result %+v after %d call(s), want one call and ok", r, called)
	}
}
