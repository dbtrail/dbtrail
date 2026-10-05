package console

import (
	"encoding/json"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/server"
)

// TestRoutedConns: a tracked connection is closed by a drop, once; one that
// ended on its own is not; and a connection whose target was read before the
// drop is refused when it comes to register.
func TestRoutedConns(t *testing.T) {
	r := newRoutedConns()
	var a, b, c, other atomic.Int32
	gen := r.generation("s1")
	untrackA, ok := r.track("s1", gen, func() { a.Add(1) })
	if !ok {
		t.Fatal("first track refused")
	}
	if _, ok := r.track("s1", gen, func() { b.Add(1) }); !ok {
		t.Fatal("second track refused")
	}
	if _, ok := r.track("s2", r.generation("s2"), func() { other.Add(1) }); !ok {
		t.Fatal("other server's track refused")
	}
	untrackA() // ended on its own
	if n := r.drop("s1"); n != 1 || a.Load() != 0 || b.Load() != 1 || other.Load() != 0 {
		t.Fatalf("drop closed %d (a=%d b=%d other=%d), want only b", n, a.Load(), b.Load(), other.Load())
	}
	if n := r.drop("s1"); n != 0 || b.Load() != 1 {
		t.Errorf("a second drop closed %d again (b=%d)", n, b.Load())
	}
	// Read before the drops, registering after: refused, never tracked.
	if _, ok := r.track("s1", gen, func() { c.Add(1) }); ok {
		t.Error("a connection bound from a target read before the drop was accepted")
	}
	if _, ok := r.track("s1", r.generation("s1"), func() { c.Add(1) }); !ok {
		t.Error("a connection bound after the drop was refused")
	}
	if r.drop("s1") != 1 || c.Load() != 1 {
		t.Errorf("the connection bound after the drop was not tracked (c=%d)", c.Load())
	}
}

// TestServersAPI_AccountChangeDropsRoutedConns: saving, changing or removing
// the forwarding account, changing the source, and deleting the server each
// close that server's open port connections; an edit that leaves the account
// alone closes nothing.
func TestServersAPI_AccountChangeDropsRoutedConns(t *testing.T) {
	srv := newRegistryServer(t)
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
		if _, ok := srv.TrackRoutedConn(dto.ID, gen, func() { dropped.Add(1) }); !ok {
			t.Fatal("track refused")
		}
	}
	steps := []struct {
		name, method, body string
		wantDrop           bool
	}{
		{"an unrelated edit", "PUT", `{"name":"prod-2","host":"h","user":"u","dbname":"db"}`, false},
		{"saving a forwarding account", "PUT", `{"name":"prod-2","host":"h","user":"u","dbname":"db","route_user":"fwd","route_password":"pw"}`, true},
		{"the form's plain save", "PUT", `{"name":"prod-2","host":"h","user":"u","dbname":"db","route_user":"fwd"}`, false},
		{"changing its password", "PUT", `{"name":"prod-2","host":"h","user":"u","dbname":"db","route_password":"pw2"}`, true},
		{"removing it", "PUT", `{"name":"prod-2","host":"h","user":"u","dbname":"db","route_user":""}`, true},
		{"removing it again", "PUT", `{"name":"prod-2","host":"h","user":"u","dbname":"db","route_user":""}`, false},
		{"changing the source's password", "PUT", `{"name":"prod-2","host":"h","user":"u","dbname":"db","source_password":"other"}`, true},
		{"deleting the server", "DELETE", ``, true},
	}
	for _, st := range steps {
		open()
		srv.RecordRouteAccountRefused(dto.ID, "the forwarding account fwd: MySQL error 1045")
		before := dropped.Load()
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
		if !st.wantDrop {
			srv.routed.drop(dto.ID) // start the next step clean
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
	s.RecordRouteAccountRefused("srv-1", "the forwarding account report_ro (MySQL error 1045: Access denied)")
	if got := read(); got.AccountRefused != "the forwarding account report_ro (MySQL error 1045: Access denied)" || got.Reasons == nil {
		t.Errorf("after a refusal: %+v", got)
	}
	s.RecordRouteDecision("srv-1", "mysql", "upstream_lost")
	if got := read(); got.AccountRefused == "" || got.MySQL != 1 {
		t.Errorf("a refusal beside a tally: %+v", got)
	}
	s.RecordRouteAccountOK("srv-1")
	if got := read(); got.AccountRefused != "" {
		t.Errorf("after a login the note is still there: %+v", got)
	}
}

// fakeMySQL is a MySQL-protocol endpoint that knows one account (fwd / pw)
// and counts the logins it accepted and the connections it saw.
type fakeMySQL struct {
	addr          string
	conns, logins atomic.Int32
}

type fakeMySQLHandler struct{ server.EmptyHandler }

func (fakeMySQLHandler) HandleQuery(string) (*gomysql.Result, error) {
	rs, err := gomysql.BuildSimpleTextResultset([]string{"x"}, [][]any{{"1"}})
	if err != nil {
		return nil, err
	}
	return gomysql.NewResult(rs), nil
}

func newFakeMySQL(t *testing.T) *fakeMySQL {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	conf := server.NewDefaultServer()
	auth := server.NewInMemoryAuthenticationHandler(gomysql.AUTH_NATIVE_PASSWORD)
	if err := auth.AddUser("fwd", "pw"); err != nil {
		t.Fatal(err)
	}
	f := &fakeMySQL{addr: ln.Addr().String()}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			f.conns.Add(1)
			go func() {
				defer c.Close()
				mc, err := server.NewCustomizedConn(c, conf, auth, fakeMySQLHandler{})
				if err != nil {
					return
				}
				f.logins.Add(1)
				for mc.HandleCommand() == nil {
				}
			}()
		}
	}()
	return f
}

// TestServersAPI_TestConnectionTriesTheForwardingAccount: Test connection
// logs in with the forwarding account, saved or being typed, and says which
// account failed and why. A saved password is never sent to a host or user
// it was not saved for.
func TestServersAPI_TestConnectionTriesTheForwardingAccount(t *testing.T) {
	src := newFakeMySQL(t)
	elsewhere := newFakeMySQL(t)
	host, port, _ := net.SplitHostPort(src.addr)
	_, otherPort, _ := net.SplitHostPort(elsewhere.addr)
	srv := newRegistryServer(t)
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
	if src.logins.Load() != 1 {
		t.Errorf("the source saw %d login(s), want 1", src.logins.Load())
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
	before := elsewhere.conns.Load()
	r = probe("moved, password not typed", `{"source_host":"`+host+`","source_port":"`+otherPort+`","route_user":"fwd"}`)
	if r == nil || !r.NeedsPassword || r.OK || r.Error != "" {
		t.Errorf("moved without the password: %+v, want needs_password", r)
	}
	if elsewhere.conns.Load() != before {
		t.Error("the saved forwarding password was sent to an address it was not saved for")
	}
	// Typed again, it is tried there.
	if r := probe("moved, password typed", `{"source_host":"`+host+`","source_port":"`+otherPort+`","route_user":"fwd","route_password":"pw"}`); r == nil || !r.OK {
		t.Errorf("moved with the password: %+v, want ok", r)
	}
	if elsewhere.logins.Load() != 1 {
		t.Errorf("the new address saw %d login(s), want 1", elsewhere.logins.Load())
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
}
