package console

import (
	"encoding/json"
	"strings"
	"testing"
)

// addedWithSchemas adds a server through the Connect step, with a schema
// filter, and returns its id.
func addedWithSchemas(t *testing.T, srv *Server) string {
	t.Helper()
	rec, body := doServersReq(t, srv, "POST", "/api/servers/check",
		`{"name":"shop","source_host":"db.example.com","source_user":"dbtrail","source_password":"Ab3-xyz","schemas":"shop,billing"}`)
	got := decodeCheck(t, body)
	if rec.Code != 200 || !got.OK || got.Server == nil {
		t.Fatalf("add: code=%d body=%s", rec.Code, body)
	}
	return got.Server.ID
}

func entryJSON(t *testing.T, e ServerEntry) string {
	t.Helper()
	b, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func entryOf(t *testing.T, srv *Server, id string) ServerEntry {
	t.Helper()
	e, ok := srv.cm.reg.Get(id)
	if !ok {
		t.Fatalf("server %s is gone", id)
	}
	return e
}

// The point of the route: it changes the forwarding account and leaves every
// other field of the server as it was.
func TestRouteUserPutChangesOnlyTheForwardingAccount(t *testing.T) {
	srv, _ := newSupervisorServer(t)
	id := addedWithSchemas(t, srv)
	before := entryOf(t, srv, id)

	rec, body := doServersReq(t, srv, "PUT", "/api/servers/"+id+"/route", `{"user":"shop_fwd","password":"Fw9-abc"}`)
	if rec.Code != 200 {
		t.Fatalf("code=%d body=%s", rec.Code, body)
	}
	var dto serverDTO
	if err := json.Unmarshal(body, &dto); err != nil {
		t.Fatal(err)
	}
	if !dto.HasRoute || dto.RouteUser != "shop_fwd" {
		t.Errorf("answer: has_route=%v route_user=%q", dto.HasRoute, dto.RouteUser)
	}
	if strings.Contains(string(body), "Fw9-abc") {
		t.Errorf("the answer carries the password: %s", body)
	}
	after := entryOf(t, srv, id)
	if !strings.Contains(after.RouteDSN, "shop_fwd:Fw9-abc@") || !strings.Contains(after.RouteDSN, "db.example.com") {
		t.Errorf("stored forwarding account = %q", after.RouteDSN)
	}
	want := before
	want.RouteDSN = after.RouteDSN
	if a, b := entryJSON(t, after), entryJSON(t, want); a != b {
		t.Errorf("something other than the forwarding account changed:\n got %s\nwant %s", a, b)
	}
	if after.Schemas != "shop,billing" {
		t.Errorf("schemas = %q", after.Schemas)
	}
}

// The control, and the reason the route exists: the edit form's route
// replaces the entry, so the same change sent there with nothing else clears
// the schema filter. If this ever stops being true the comment on
// route_user_api.go is out of date, not this test.
func TestServersPutWithOnlyTheForwardingAccountClearsTheSchemaFilter(t *testing.T) {
	srv, _ := newSupervisorServer(t)
	id := addedWithSchemas(t, srv)
	rec, body := doServersReq(t, srv, "PUT", "/api/servers/"+id, `{"name":"shop","route_user":"shop_fwd","route_password":"Fw9-abc"}`)
	if rec.Code != 200 {
		t.Fatalf("code=%d body=%s", rec.Code, body)
	}
	if got := entryOf(t, srv, id).Schemas; got != "" {
		t.Errorf("schemas = %q: the edit route now keeps what it is not sent", got)
	}
}

func TestRouteUserPasswordAloneAndUserAlone(t *testing.T) {
	srv, _ := newSupervisorServer(t)
	id := addedWithSchemas(t, srv)
	put := func(body string) (int, string) {
		rec, b := doServersReq(t, srv, "PUT", "/api/servers/"+id+"/route", body)
		return rec.Code, string(b)
	}
	// A password with no account stored has nothing to be the password of,
	// and a new name with no password must not be saved with none.
	if code, b := put(`{"password":"Fw9-abc"}`); code != 400 {
		t.Errorf("password alone, no account: code=%d %s", code, b)
	}
	if code, b := put(`{"user":"shop_fwd"}`); code != 400 {
		t.Errorf("new user with no password: code=%d %s", code, b)
	}
	if got := entryOf(t, srv, id).RouteDSN; got != "" {
		t.Fatalf("a refused request saved %q", got)
	}
	if code, b := put(`{"user":"shop_fwd","password":"Fw9-abc"}`); code != 200 {
		t.Fatalf("code=%d %s", code, b)
	}
	// Now a password alone replaces that account's password, and the same
	// user alone keeps it.
	if code, b := put(`{"password":"New-pw1"}`); code != 200 {
		t.Fatalf("password alone: code=%d %s", code, b)
	}
	if got := entryOf(t, srv, id).RouteDSN; !strings.Contains(got, "shop_fwd:New-pw1@") {
		t.Errorf("after a new password: %q", got)
	}
	if code, b := put(`{"user":"shop_fwd"}`); code != 200 {
		t.Fatalf("same user alone: code=%d %s", code, b)
	}
	if got := entryOf(t, srv, id).RouteDSN; !strings.Contains(got, "shop_fwd:New-pw1@") {
		t.Errorf("the same user alone changed the password: %q", got)
	}
}

func TestRouteUserDelete(t *testing.T) {
	srv, _ := newSupervisorServer(t)
	id := addedWithSchemas(t, srv)
	doServersReq(t, srv, "PUT", "/api/servers/"+id+"/route", `{"user":"shop_fwd","password":"Fw9-abc"}`)
	rec, body := doServersReq(t, srv, "DELETE", "/api/servers/"+id+"/route", "")
	if rec.Code != 200 {
		t.Fatalf("code=%d body=%s", rec.Code, body)
	}
	e := entryOf(t, srv, id)
	if e.RouteDSN != "" || e.Schemas != "shop,billing" {
		t.Errorf("after the delete: route=%q schemas=%q", e.RouteDSN, e.Schemas)
	}
	// Removing what is not there is not an error.
	if rec, body := doServersReq(t, srv, "DELETE", "/api/servers/"+id+"/route", ""); rec.Code != 200 {
		t.Errorf("second delete: code=%d body=%s", rec.Code, body)
	}
}

func TestRouteUserRefusals(t *testing.T) {
	srv, _ := newSupervisorServer(t)
	id := addedWithSchemas(t, srv)
	for name, c := range map[string]struct {
		method, path, body string
		want               int
	}{
		"nothing sent":            {"PUT", "/api/servers/" + id + "/route", `{}`, 400},
		"empty user":              {"PUT", "/api/servers/" + id + "/route", `{"user":"  ","password":"x"}`, 400},
		"empty password":          {"PUT", "/api/servers/" + id + "/route", `{"user":"u","password":""}`, 400},
		"a field it does not own": {"PUT", "/api/servers/" + id + "/route", `{"user":"u","password":"p","schemas":"x"}`, 400},
		"not JSON":                {"PUT", "/api/servers/" + id + "/route", `user=u`, 400},
		"two values":              {"PUT", "/api/servers/" + id + "/route", `{"user":"u","password":"p"}{}`, 400},
		"unknown server":          {"PUT", "/api/servers/nope/route", `{"user":"u","password":"p"}`, 404},
		"unknown server, delete":  {"DELETE", "/api/servers/nope/route", ``, 404},
		"the command-line server": {"PUT", "/api/servers/" + bootServerID + "/route", `{"user":"u","password":"p"}`, 409},
	} {
		rec, body := doServersReq(t, srv, c.method, c.path, c.body)
		if rec.Code != c.want {
			t.Errorf("%s: code=%d, want %d: %s", name, rec.Code, c.want, body)
		}
	}
	if got := entryOf(t, srv, id); got.RouteDSN != "" || got.Schemas != "shop,billing" {
		t.Errorf("a refused request changed the server: route=%q schemas=%q", got.RouteDSN, got.Schemas)
	}
}
