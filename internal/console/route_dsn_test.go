package console

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/go-sql-driver/mysql"
)

// TestBuildRouteDSN: every way a request can set, keep or clear the
// forwarding account (#2079), over what is stored and over the source the
// same request ends up with.
func TestBuildRouteDSN(t *testing.T) {
	const (
		src    = "repl:replpw@tcp(db.prod:3306)/shop?tls=skip-verify"
		src2   = "repl:replpw@tcp(db2.prod:3307)/shop?tls=skip-verify"
		stored = "fwd:fwdpw@tcp(db.prod:3306)/shop?tls=skip-verify"
		other  = "fwd:fwdpw@tcp(replica.prod:3306)/"
	)
	parts := func(dsn string) string {
		t.Helper()
		if dsn == "" {
			return ""
		}
		cfg, err := mysql.ParseDSN(dsn)
		if err != nil {
			t.Fatalf("result %q does not parse: %v", dsn, err)
		}
		return cfg.User + ":" + cfg.Passwd + "@" + cfg.Addr + "/" + cfg.DBName + "?tls=" + cfg.TLSConfig
	}
	cases := []struct {
		name                         string
		req                          serverRequest
		stored, oldSource, newSource string
		flavor                       string
		want                         string // parts(); "" = no forwarding account
		wantErr                      string
	}{
		{name: "nothing sent, nothing stored", oldSource: src, newSource: src},
		{name: "nothing sent keeps what is stored", stored: stored, oldSource: src, newSource: src, want: "fwd:fwdpw@db.prod:3306/shop?tls=skip-verify"},
		{name: "kept account follows the source to its new address", stored: stored, oldSource: src, newSource: src2, want: "fwd:fwdpw@db2.prod:3307/shop?tls=skip-verify"},
		{name: "kept account follows the source's TLS setting", stored: stored, oldSource: src, newSource: "repl:replpw@tcp(db.prod:3306)/shop?tls=true",
			want: "fwd:fwdpw@db.prod:3306/shop?tls=true"},
		{name: "kept account follows the source's TLS setting being removed", stored: stored, oldSource: src, newSource: "repl:replpw@tcp(db.prod:3306)/shop",
			want: "fwd:fwdpw@db.prod:3306/shop?tls="},
		{name: "kept account follows the source's database", stored: stored, oldSource: src, newSource: "repl:replpw@tcp(db.prod:3306)/other?tls=skip-verify",
			want: "fwd:fwdpw@db.prod:3306/other?tls=skip-verify"},
		{name: "kept account follows address and settings together", stored: stored, oldSource: src, newSource: "repl:replpw@tcp(db9.prod:3309)/other?tls=true",
			want: "fwd:fwdpw@db9.prod:3309/other?tls=true"},
		{name: "a source password change alone leaves the account as stored", stored: "fwd:fwdpw@tcp(db.prod:3306)/own?tls=true", oldSource: src, newSource: "repl:newpw@tcp(db.prod:3306)/shop?tls=skip-verify",
			want: "fwd:fwdpw@db.prod:3306/own?tls=true"},
		{name: "an unrelated edit leaves a raw DSN on the source address with its own settings", stored: "fwd:fwdpw@tcp(db.prod:3306)/own?tls=true", oldSource: src, newSource: src,
			want: "fwd:fwdpw@db.prod:3306/own?tls=true"},
		{name: "a form resend is a keep: a raw DSN on the source address keeps its own settings", req: serverRequest{RouteUser: strPtr("fwd")}, stored: "fwd:fwdpw@tcp(db.prod:3306)/own?tls=true", oldSource: src, newSource: src,
			want: "fwd:fwdpw@db.prod:3306/own?tls=true"},
		{name: "a form resend follows the source's new TLS setting", req: serverRequest{RouteUser: strPtr(" fwd ")}, stored: stored, oldSource: src, newSource: "repl:replpw@tcp(db.prod:3306)/shop?tls=true",
			want: "fwd:fwdpw@db.prod:3306/shop?tls=true"},
		{name: "kept account on another address ignores the source's new settings", stored: other, oldSource: src, newSource: "repl:replpw@tcp(db.prod:3306)/other?tls=true",
			want: "fwd:fwdpw@replica.prod:3306/?tls="},
		{name: "kept account on another address stays there", stored: other, oldSource: src, newSource: src2, want: "fwd:fwdpw@replica.prod:3306/?tls="},
		{name: "source cleared clears the account", stored: stored, oldSource: src, newSource: ""},
		{name: "empty user clears", req: serverRequest{RouteUser: strPtr("")}, stored: stored, oldSource: src, newSource: src},
		{name: "blank user clears", req: serverRequest{RouteUser: strPtr("   ")}, stored: stored, oldSource: src, newSource: src},
		{name: "empty dsn clears", req: serverRequest{RouteDSN: strPtr("")}, stored: stored, oldSource: src, newSource: src},
		{name: "empty user with nothing stored", req: serverRequest{RouteUser: strPtr("")}, oldSource: src, newSource: src},
		{name: "user and password over the source address", req: serverRequest{RouteUser: strPtr("fwd"), RoutePassword: strPtr("pw")}, oldSource: src, newSource: src,
			want: "fwd:pw@db.prod:3306/shop?tls=skip-verify"},
		{name: "user is trimmed", req: serverRequest{RouteUser: strPtr("  fwd "), RoutePassword: strPtr("pw")}, oldSource: src, newSource: src,
			want: "fwd:pw@db.prod:3306/shop?tls=skip-verify"},
		{name: "structured edit lands on the NEW source address", req: serverRequest{RouteUser: strPtr("fwd"), RoutePassword: strPtr("pw")}, stored: stored, oldSource: src, newSource: src2,
			want: "fwd:pw@db2.prod:3307/shop?tls=skip-verify"},
		{name: "same user, password omitted: keep the password", req: serverRequest{RouteUser: strPtr("fwd")}, stored: stored, oldSource: src, newSource: src,
			want: "fwd:fwdpw@db.prod:3306/shop?tls=skip-verify"},
		{name: "password alone replaces it", req: serverRequest{RoutePassword: strPtr("new")}, stored: stored, oldSource: src, newSource: src,
			want: "fwd:new@db.prod:3306/shop?tls=skip-verify"},
		{name: "empty password is a password", req: serverRequest{RouteUser: strPtr("fwd"), RoutePassword: strPtr("")}, oldSource: src, newSource: src,
			want: "fwd:@db.prod:3306/shop?tls=skip-verify"},
		{name: "a password with every DSN delimiter", req: serverRequest{RouteUser: strPtr("fwd"), RoutePassword: strPtr("p@ss:w/rd?(x)")}, oldSource: src, newSource: src,
			want: "fwd:p@ss:w/rd?(x)@db.prod:3306/shop?tls=skip-verify"},
		{name: "form resend keeps an account at another address where it is", req: serverRequest{RouteUser: strPtr("fwd")}, stored: other, oldSource: src, newSource: src,
			want: "fwd:fwdpw@replica.prod:3306/?tls="},
		{name: "form resend while the source moves keeps an account at another address", req: serverRequest{RouteUser: strPtr("fwd")}, stored: other, oldSource: src, newSource: src2,
			want: "fwd:fwdpw@replica.prod:3306/?tls="},
		{name: "password change on an account at another address stays there", req: serverRequest{RouteUser: strPtr("fwd"), RoutePassword: strPtr("new")}, stored: other, oldSource: src, newSource: src,
			want: "fwd:new@replica.prod:3306/?tls="},
		{name: "form resend follows the source to its new address", req: serverRequest{RouteUser: strPtr("fwd")}, stored: stored, oldSource: src, newSource: src2,
			want: "fwd:fwdpw@db2.prod:3307/shop?tls=skip-verify"},
		{name: "empty user clears even over a source DSN that does not parse", req: serverRequest{RouteUser: strPtr("")}, stored: stored, oldSource: "not a dsn(", newSource: "not a dsn("},
		{name: "empty dsn clears even over a source DSN that does not parse", req: serverRequest{RouteDSN: strPtr("")}, stored: stored, oldSource: "not a dsn(", newSource: "not a dsn("},
		{name: "nothing sent over a source DSN that does not parse keeps it", stored: stored, oldSource: "not a dsn(", newSource: "not a dsn(", want: "fwd:fwdpw@db.prod:3306/shop?tls=skip-verify"},
		{name: "a stored account that already is the capture account does not stop an unrelated edit", stored: "repl:replpw@tcp(db.prod:3306)/", oldSource: src, newSource: src,
			want: "repl:replpw@db.prod:3306/?tls="},
		{name: "raw dsn is used as given", req: serverRequest{RouteDSN: strPtr(other)}, oldSource: src, newSource: src, want: "fwd:fwdpw@replica.prod:3306/?tls="},

		{name: "new user without a password", req: serverRequest{RouteUser: strPtr("fwd")}, oldSource: src, newSource: src, wantErr: "route_password"},
		{name: "changed user without a password", req: serverRequest{RouteUser: strPtr("fwd2")}, stored: stored, oldSource: src, newSource: src, wantErr: "route_password"},
		{name: "user differing in case is another user", req: serverRequest{RouteUser: strPtr("FWD")}, stored: stored, oldSource: src, newSource: src, wantErr: "route_password"},
		{name: "password with no user anywhere", req: serverRequest{RoutePassword: strPtr("pw")}, oldSource: src, newSource: src, wantErr: "route_user"},
		{name: "the capture account is not a separate account", req: serverRequest{RouteUser: strPtr("repl"), RoutePassword: strPtr("replpw")}, oldSource: src, newSource: src, wantErr: "capture"},
		{name: "raw dsn naming the capture account", req: serverRequest{RouteDSN: strPtr("repl:replpw@tcp(db.prod:3306)/")}, oldSource: src, newSource: src, wantErr: "capture"},
		{name: "the source user becomes the forwarding user", stored: stored, oldSource: src, newSource: "fwd:other@tcp(db.prod:3306)/shop", wantErr: "capture"},
		{name: "empty dsn with a structured user", req: serverRequest{RouteDSN: strPtr(""), RouteUser: strPtr("fwd")}, stored: stored, oldSource: src, newSource: src, wantErr: "either"},
		{name: "a colon in the user would be stored as another account", req: serverRequest{RouteUser: strPtr("fwd:x"), RoutePassword: strPtr("pw")}, oldSource: src, newSource: src, wantErr: "cannot contain a colon"},
		{name: "a colon in the user that hides the capture account", req: serverRequest{RouteUser: strPtr("repl:x"), RoutePassword: strPtr("pw")}, oldSource: src, newSource: src, wantErr: "cannot contain a colon"},
		{name: "the capture account under another spelling of the host", req: serverRequest{RouteDSN: strPtr("repl:replpw@tcp(DB.PROD:3306)/")}, oldSource: src, newSource: src, wantErr: "capture"},
		{name: "the capture account with the default port left out", req: serverRequest{RouteDSN: strPtr("repl:replpw@tcp(db.prod)/")}, oldSource: src, newSource: src, wantErr: "capture"},
		{name: "the capture account under a loopback alias", req: serverRequest{RouteDSN: strPtr("repl:replpw@tcp(127.0.0.1:3306)/")}, oldSource: "repl:replpw@tcp(localhost:3306)/", newSource: "repl:replpw@tcp(localhost:3306)/", wantErr: "capture"},
		{name: "the capture account under the IPv6 loopback", req: serverRequest{RouteDSN: strPtr("repl:replpw@tcp([::1]:3306)/")}, oldSource: "repl:replpw@tcp(LOCALHOST:3306)/", newSource: "repl:replpw@tcp(LOCALHOST:3306)/", wantErr: "capture"},
		{name: "a password alone over a stored account that cannot be read", req: serverRequest{RoutePassword: strPtr("pw")}, stored: "not a dsn(", oldSource: src, newSource: src, wantErr: "route_user"},
		{name: "raw dsn and structured fields", req: serverRequest{RouteDSN: strPtr(other), RouteUser: strPtr("fwd")}, oldSource: src, newSource: src, wantErr: "either"},
		{name: "raw dsn and structured password", req: serverRequest{RouteDSN: strPtr(other), RoutePassword: strPtr("x")}, oldSource: src, newSource: src, wantErr: "either"},
		{name: "raw dsn over a unix socket", req: serverRequest{RouteDSN: strPtr("fwd:pw@unix(/tmp/mysql.sock)/")}, oldSource: src, newSource: src, wantErr: "TCP"},
		{name: "raw dsn with no user", req: serverRequest{RouteDSN: strPtr("tcp(db.prod:3306)/")}, oldSource: src, newSource: src, wantErr: "user"},
		{name: "raw dsn that does not parse", req: serverRequest{RouteDSN: strPtr("fwd:s3cr3tpw@tcp(db.prod:3306")}, oldSource: src, newSource: src, wantErr: "invalid route_dsn"},
		{name: "account with no source", req: serverRequest{RouteUser: strPtr("fwd"), RoutePassword: strPtr("pw")}, wantErr: "source"},
		{name: "raw dsn with no source", req: serverRequest{RouteDSN: strPtr(other)}, wantErr: "source"},
		{name: "account on a PostgreSQL source", req: serverRequest{RouteUser: strPtr("fwd"), RoutePassword: strPtr("pw")}, oldSource: "postgres://u:p@h/db", newSource: "postgres://u:p@h/db", flavor: FlavorPostgres, wantErr: "MySQL"},
		{name: "PostgreSQL with nothing sent", oldSource: "postgres://u:p@h/db", newSource: "postgres://u:p@h/db", flavor: FlavorPostgres},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			flavor := tc.flavor
			if flavor == "" {
				flavor = FlavorMySQL
			}
			got, err := buildRouteDSN(tc.req, tc.stored, tc.oldSource, tc.newSource, flavor)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("got %q, %v; want an error holding %q", got, err, tc.wantErr)
				}
				for _, secret := range []string{"s3cr3tpw", "replpw", "fwdpw"} {
					if strings.Contains(err.Error(), secret) {
						t.Errorf("the error carries a password: %v", err)
					}
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if parts(got) != tc.want {
				t.Errorf("got %q, want %q", parts(got), tc.want)
			}
		})
	}
}

// TestServersAPI_ForwardingAccount: the account round-trips through the real
// routes: set, masked on every read, kept by an unrelated edit, replaced,
// cleared, and gone with the source.
func TestServersAPI_ForwardingAccount(t *testing.T) {
	srv := newRegistryServer(t)
	const secret = "fwd-s3cr3t-pw"
	rec, body := doServersReq(t, srv, "POST", "/api/servers",
		`{"name":"prod","host":"h","user":"u","dbname":"db","source_host":"db.prod","source_port":"3307","source_user":"repl","source_password":"replpw",`+
			`"route_user":"fwd","route_password":"`+secret+`"}`)
	if rec.Code != 201 {
		t.Fatalf("create: code=%d body=%s", rec.Code, body)
	}
	var dto serverDTO
	if err := json.Unmarshal(body, &dto); err != nil {
		t.Fatal(err)
	}
	stored := func() string {
		e, _ := srv.cm.reg.Get(dto.ID)
		return e.RouteDSN
	}
	masked := func(what string, body []byte) serverDTO {
		t.Helper()
		if strings.Contains(string(body), secret) || strings.Contains(string(body), "route_dsn") {
			t.Fatalf("%s: the response carries the forwarding password or DSN: %s", what, body)
		}
		var d serverDTO
		if err := json.Unmarshal(body, &d); err != nil {
			t.Fatal(err)
		}
		return d
	}
	if d := masked("create", body); !d.HasRoute || d.RouteUser != "fwd" || !d.HasRoutePassword || d.RouteHost != "" || d.RoutePort != "" {
		t.Errorf("create DTO = %+v, want has_route, user fwd, has_route_password and no address (it is the source's)", d)
	}
	if got := stored(); got != "fwd:"+secret+"@tcp(db.prod:3307)/" {
		t.Errorf("stored route DSN = %q", got)
	}
	// The list and the single read mask it too.
	_, body = doServersReq(t, srv, "GET", "/api/servers", "")
	if strings.Contains(string(body), secret) || !strings.Contains(string(body), `"route_user":"fwd"`) {
		t.Errorf("list: %s", body)
	}
	_, body = doServersReq(t, srv, "GET", "/api/servers/"+dto.ID, "")
	masked("get", body)

	// An edit that says nothing about it keeps it.
	rec, body = doServersReq(t, srv, "PUT", "/api/servers/"+dto.ID, `{"name":"prod-2","host":"h","user":"u","dbname":"db"}`)
	if rec.Code != 200 || stored() != "fwd:"+secret+"@tcp(db.prod:3307)/" {
		t.Fatalf("unrelated edit: code=%d stored=%q body=%s", rec.Code, stored(), body)
	}
	// What the form sends on a plain save: the user again, no password.
	rec, body = doServersReq(t, srv, "PUT", "/api/servers/"+dto.ID, `{"name":"prod-2","host":"h","user":"u","dbname":"db","route_user":"fwd"}`)
	if rec.Code != 200 || stored() != "fwd:"+secret+"@tcp(db.prod:3307)/" {
		t.Fatalf("form resend: code=%d stored=%q body=%s", rec.Code, stored(), body)
	}
	// The source moves: the account follows it.
	rec, body = doServersReq(t, srv, "PUT", "/api/servers/"+dto.ID, `{"name":"prod-2","host":"h","user":"u","dbname":"db","source_host":"db2.prod","route_user":"fwd"}`)
	if rec.Code != 200 || stored() != "fwd:"+secret+"@tcp(db2.prod:3307)/" {
		t.Fatalf("source move: code=%d stored=%q body=%s", rec.Code, stored(), body)
	}
	// A new user with no password is refused, and nothing changes.
	rec, body = doServersReq(t, srv, "PUT", "/api/servers/"+dto.ID, `{"name":"prod-2","host":"h","user":"u","dbname":"db","route_user":"other"}`)
	if rec.Code != 400 || !strings.Contains(string(body), "route_password") || stored() != "fwd:"+secret+"@tcp(db2.prod:3307)/" {
		t.Fatalf("new user without password: code=%d stored=%q body=%s", rec.Code, stored(), body)
	}
	// Empty user clears.
	rec, body = doServersReq(t, srv, "PUT", "/api/servers/"+dto.ID, `{"name":"prod-2","host":"h","user":"u","dbname":"db","route_user":""}`)
	if rec.Code != 200 || stored() != "" {
		t.Fatalf("clear: code=%d stored=%q body=%s", rec.Code, stored(), body)
	}
	if d := masked("clear", body); d.HasRoute || d.RouteUser != "" || d.HasRoutePassword {
		t.Errorf("cleared DTO = %+v", d)
	}
	// Set again, then clear the source: the account goes with it.
	doServersReq(t, srv, "PUT", "/api/servers/"+dto.ID, `{"name":"prod-2","host":"h","user":"u","dbname":"db","route_user":"fwd","route_password":"x"}`)
	if stored() == "" {
		t.Fatal("could not set the account again")
	}
	rec, body = doServersReq(t, srv, "PUT", "/api/servers/"+dto.ID, `{"name":"prod-2","host":"h","user":"u","dbname":"db","source_dsn":""}`)
	if rec.Code != 200 || stored() != "" {
		t.Fatalf("source cleared: code=%d stored=%q body=%s", rec.Code, stored(), body)
	}
}

// TestRegistry_RouteDSNRoundTrips: the field is written to the registry file
// under its own key and read back, and a file without it loads as before.
func TestRegistry_RouteDSNRoundTrips(t *testing.T) {
	path := t.TempDir() + "/servers.yaml"
	reg, err := LoadRegistry(path)
	if err != nil {
		t.Fatal(err)
	}
	e, err := reg.Add(ServerEntry{Name: "a", DSN: "u:p@tcp(i:3306)/idx", SourceDSN: "repl:pw@tcp(s:3306)/", RouteDSN: "fwd:pw2@tcp(s:3306)/"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reg.Add(ServerEntry{Name: "b", DSN: "u:p@tcp(i:3306)/idx2", SourceDSN: "repl:pw@tcp(s2:3306)/"}); err != nil {
		t.Fatal(err)
	}
	again, err := LoadRegistry(path)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := again.Get(e.ID)
	if got.RouteDSN != "fwd:pw2@tcp(s:3306)/" {
		t.Errorf("route_dsn read back as %q", got.RouteDSN)
	}
	if _, inExtra := got.Extra["route_dsn"]; inExtra {
		t.Error("route_dsn landed in the forward-compat catch-all instead of its field")
	}
	for _, other := range again.List() {
		if other.Name == "b" && other.RouteDSN != "" {
			t.Errorf("an entry saved without the field reads back with %q", other.RouteDSN)
		}
	}
}

// TestFlashbackForwardDSN: the port forwards with the forwarding account when
// one is set, and with the source account when none is.
func TestFlashbackForwardDSN(t *testing.T) {
	reg, err := LoadRegistry(t.TempDir() + "/servers.yaml")
	if err != nil {
		t.Fatal(err)
	}
	with, err := reg.Add(ServerEntry{Name: "with", DSN: "u:p@tcp(i:3306)/idx", SourceDSN: "repl:pw@tcp(s:3306)/shop", RouteDSN: "fwd:pw2@tcp(s:3306)/shop"})
	if err != nil {
		t.Fatal(err)
	}
	without, err := reg.Add(ServerEntry{Name: "without", DSN: "u:p@tcp(i:3306)/idx2", SourceDSN: "repl:pw@tcp(s:3306)/shop"})
	if err != nil {
		t.Fatal(err)
	}
	view, err := reg.Add(ServerEntry{Name: "view", DSN: "u:p@tcp(i:3306)/idx3"})
	if err != nil {
		t.Fatal(err)
	}
	// A hand-edited file can hold a forwarding account with no source: there
	// is no source to forward to, so the port must not route there.
	orphan, err := reg.Add(ServerEntry{Name: "orphan", DSN: "u:p@tcp(i:3306)/idx4", RouteDSN: "fwd:pw2@tcp(s:3306)/shop"})
	if err != nil {
		t.Fatal(err)
	}
	srv, err := New(Config{Listen: "127.0.0.1:8090", Token: "t", Registry: reg})
	if err != nil {
		t.Fatal(err)
	}
	isCapture, err := reg.Add(ServerEntry{Name: "iscapture", DSN: "u:p@tcp(i:3306)/idx5", SourceDSN: "repl:pw@tcp(s:3306)/shop", RouteDSN: "repl:pw@tcp(S:3306)/shop"})
	if err != nil {
		t.Fatal(err)
	}
	unreadable, err := reg.Add(ServerEntry{Name: "unreadable", DSN: "u:p@tcp(i:3306)/idx6", SourceDSN: "repl:pw@tcp(s:3306)/shop", RouteDSN: "fwd:pw@tcp(s:3306"})
	if err != nil {
		t.Fatal(err)
	}
	type fwd struct {
		dsn                  string
		separate, routeField bool
	}
	for name, tc := range map[string]struct {
		id   string
		want fwd
	}{
		"a forwarding account":                {with.ID, fwd{"fwd:pw2@tcp(s:3306)/shop", true, true}},
		"none":                                {without.ID, fwd{"repl:pw@tcp(s:3306)/shop", false, false}},
		"no source":                           {view.ID, fwd{}},
		"a forwarding account with no source": {orphan.ID, fwd{}},
		"the capture account in the field":    {isCapture.ID, fwd{"repl:pw@tcp(S:3306)/shop", false, true}},
		"a value that cannot be read":         {unreadable.ID, fwd{"fwd:pw@tcp(s:3306", false, true}},
		"unknown server":                      {"nope", fwd{}},
	} {
		dsn, separate, routeField := srv.flashbackForwardDSN(tc.id)
		if got := (fwd{dsn, separate, routeField}); got != tc.want {
			t.Errorf("%s: %+v, want %+v", name, got, tc.want)
		}
	}
}

// TestFormatRouteDSN: what is stored reads back as the account that was
// asked for, or it is not stored.
func TestFormatRouteDSN(t *testing.T) {
	ok := mysql.NewConfig()
	ok.User, ok.Passwd, ok.Net, ok.Addr = "fwd", "p@ss:w/rd(1)", "tcp", "db.prod:3306"
	dsn, err := formatRouteDSN(ok)
	if err != nil {
		t.Fatalf("a plain account with an awkward password: %v", err)
	}
	if back, err := mysql.ParseDSN(dsn); err != nil || back.User != "fwd" || back.Passwd != "p@ss:w/rd(1)" || back.Addr != "db.prod:3306" {
		t.Errorf("stored %+v (%v), want it back as asked", back, err)
	}
	// "a:b" as a user reads back as user a with a password that starts
	// with b: another account.
	bad := ok.Clone()
	bad.User = "fwd:x"
	if dsn, err := formatRouteDSN(bad); err == nil || dsn != "" || strings.Contains(err.Error(), "p@ss") {
		t.Errorf("a user that reads back as another account was stored: %q, %v", dsn, err)
	}
}

// TestBuildRouteDSN_unreadableStoredIsLeftAlone: a stored forwarding account
// that cannot be read (a hand-edited registry file) does not stop an edit
// that says nothing about it, and that edit leaves it exactly as it is. It
// can still be replaced or removed.
func TestBuildRouteDSN_unreadableStoredIsLeftAlone(t *testing.T) {
	const src, broken = "repl:replpw@tcp(db.prod:3306)/shop", "fwd:pw@tcp(db.prod:3306"
	for name, newSource := range map[string]string{"an unrelated edit": src, "the source moves": "repl:replpw@tcp(db2.prod:3306)/shop"} {
		got, err := buildRouteDSN(serverRequest{}, broken, src, newSource, FlavorMySQL)
		if err != nil || got != broken {
			t.Errorf("%s: got %q, %v; want the stored value untouched and no error", name, got, err)
		}
	}
	if got, err := buildRouteDSN(serverRequest{RouteUser: strPtr("")}, broken, src, src, FlavorMySQL); err != nil || got != "" {
		t.Errorf("remove: got %q, %v", got, err)
	}
	if got, err := buildRouteDSN(serverRequest{RouteUser: strPtr("fwd"), RoutePassword: strPtr("pw")}, broken, src, src, FlavorMySQL); err != nil || got != "fwd:pw@tcp(db.prod:3306)/shop" {
		t.Errorf("replace: got %q, %v", got, err)
	}
}

// TestBuildRouteDSN_storesWhatWasAsked: whatever user and password are
// accepted come back out of the stored DSN exactly as they went in; anything
// that would not is refused. A name the DSN syntax splits differently would
// otherwise be saved as another account.
func TestBuildRouteDSN_storesWhatWasAsked(t *testing.T) {
	const src = "repl:replpw@tcp(db.prod:3306)/shop"
	for _, user := range []string{"fwd", "fwd@host", "fwd/x", "fwd(x)", "f?w=d", "fwd x", "ünï", "a:b", ":", "fwd:", "@", "(", "fwd@tcp(evil:3306)/"} {
		for _, password := range []string{"pw", "", "p:w", "p@w", "p@tcp(x)/w", "pass word", "/?&="} {
			got, err := buildRouteDSN(serverRequest{RouteUser: &user, RoutePassword: &password}, "", src, src, FlavorMySQL)
			if err != nil {
				continue
			}
			cfg, perr := mysql.ParseDSN(got)
			if perr != nil || cfg.User != user || cfg.Passwd != password || cfg.Addr != "db.prod:3306" {
				t.Errorf("user %q password %q was accepted and stored as user %q password %q address %q (%v)", user, password, cfgUser(cfg), cfgPass(cfg), cfgAddr(cfg), perr)
			}
		}
	}
	// The plain cases are accepted, so the loop above is not vacuous.
	if _, err := buildRouteDSN(serverRequest{RouteUser: strPtr("fwd"), RoutePassword: strPtr("p:w@x")}, "", src, src, FlavorMySQL); err != nil {
		t.Errorf("a plain user with an odd password was refused: %v", err)
	}
}

func cfgUser(c *mysql.Config) string {
	if c == nil {
		return ""
	}
	return c.User
}
func cfgPass(c *mysql.Config) string {
	if c == nil {
		return ""
	}
	return c.Passwd
}
func cfgAddr(c *mysql.Config) string {
	if c == nil {
		return ""
	}
	return c.Addr
}

// TestRouteAccountView: what the API says about a server's forwarding
// account, decided from the account the port would actually use and not
// from the field being filled in.
func TestRouteAccountView(t *testing.T) {
	const src = "repl:replpw@tcp(db.prod:3306)/shop"
	cases := []struct {
		name, route, source string
		want                serverDTO
	}{
		{name: "none", source: src},
		{name: "separate, on the source's address", route: "fwd:pw@tcp(db.prod:3306)/shop", source: src,
			want: serverDTO{HasRoute: true, RouteUser: "fwd", HasRoutePassword: true}},
		{name: "separate, no password", route: "fwd@tcp(db.prod:3306)/", source: src,
			want: serverDTO{HasRoute: true, RouteUser: "fwd"}},
		{name: "separate, on another address: the address is shown", route: "fwd:pw@tcp(replica.prod:3307)/", source: src,
			want: serverDTO{HasRoute: true, RouteUser: "fwd", HasRoutePassword: true, RouteHost: "replica.prod", RoutePort: "3307"}},
		{name: "the same user on another address is a separate account there", route: "repl:pw@tcp(replica.prod:3306)/", source: src,
			want: serverDTO{HasRoute: true, RouteUser: "repl", HasRoutePassword: true, RouteHost: "replica.prod", RoutePort: "3306"}},
		{name: "the capture account itself is not a separate one", route: "repl:other@tcp(DB.prod)/", source: src,
			want: serverDTO{RouteIsCapture: true, RouteUser: "repl"}},
		{name: "a value that cannot be read", route: "fwd:pw@tcp(db.prod:3306", source: src,
			want: serverDTO{RouteUnreadable: true}},
		{name: "no source: nothing to forward to, so no account", route: "fwd:pw@tcp(db.prod:3306)/"},
	}
	for _, tc := range cases {
		var got serverDTO
		fillRouteDSNParts(&got, tc.route, tc.source)
		if !reflect.DeepEqual(got, tc.want) {
			t.Errorf("%s:\n got  %+v\n want %+v", tc.name, got, tc.want)
		}
	}
}
