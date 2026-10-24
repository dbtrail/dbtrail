package console

import (
	"encoding/json"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/hiderules"
)

func createHiddenTestServer(t *testing.T, srv *Server) string {
	t.Helper()
	rec, body := doServersReq(t, srv, "POST", "/api/servers",
		`{"name":"prod","host":"h","user":"u","password":"p","dbname":"db"}`)
	if rec.Code != 201 {
		t.Fatalf("create code = %d, body = %s", rec.Code, body)
	}
	var created serverDTO
	if err := json.Unmarshal(body, &created); err != nil {
		t.Fatal(err)
	}
	return created.ID
}

func decodeHidden(t *testing.T, body []byte) hiddenDTO {
	t.Helper()
	var d hiddenDTO
	if err := json.Unmarshal(body, &d); err != nil {
		t.Fatalf("decode %s: %v", body, err)
	}
	return d
}

func TestHiddenAPISaveReadClear(t *testing.T) {
	srv := newRegistryServer(t)
	id := createHiddenTestServer(t, srv)

	// A server with no rules answers empty lists, never null.
	rec, body := doServersReq(t, srv, "GET", "/api/servers/"+id+"/hidden", "")
	if rec.Code != 200 || !strings.Contains(string(body), `"tables":[]`) || !strings.Contains(string(body), `"columns":[]`) {
		t.Fatalf("empty GET = %d %s", rec.Code, body)
	}

	rec, body = doServersReq(t, srv, "PUT", "/api/servers/"+id+"/hidden",
		`{"tables":[" shop.audit_log ","SHOP.AUDIT_LOG",""],"columns":["shop.customers.email"]}`)
	if rec.Code != 200 {
		t.Fatalf("PUT = %d %s", rec.Code, body)
	}
	d := decodeHidden(t, body)
	if !slices.Equal(d.Tables, []string{"shop.audit_log"}) || !slices.Equal(d.Columns, []string{"shop.customers.email"}) || !d.Editable {
		t.Fatalf("PUT answered %+v, want the canonical rules", d)
	}

	// Saved to the file, not only to memory.
	raw, err := os.ReadFile(srv.cm.reg.Path())
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), "shop.customers.email") {
		t.Fatalf("registry file does not hold the rules:\n%s", raw)
	}
	reloaded, err := LoadRegistry(srv.cm.reg.Path())
	if err != nil {
		t.Fatal(err)
	}
	if e, _ := reloaded.Get(id); e.Hidden == nil || !slices.Equal(e.Hidden.Tables, []string{"shop.audit_log"}) {
		t.Fatalf("reloaded entry hidden = %+v", e.Hidden)
	}

	// The server list carries them too.
	_, body = doServersReq(t, srv, "GET", "/api/servers/"+id, "")
	if !strings.Contains(string(body), `"hidden":{"tables":["shop.audit_log"]`) {
		t.Fatalf("server DTO lacks the rules: %s", body)
	}

	// Empty lists remove the rules, and the file no longer names them.
	rec, body = doServersReq(t, srv, "PUT", "/api/servers/"+id+"/hidden", `{"tables":[],"columns":[]}`)
	if rec.Code != 200 {
		t.Fatalf("clear = %d %s", rec.Code, body)
	}
	if e, _ := srv.cm.reg.Get(id); e.Hidden != nil {
		t.Fatalf("cleared entry still has %+v", e.Hidden)
	}
	raw, _ = os.ReadFile(srv.cm.reg.Path())
	if strings.Contains(string(raw), "hidden") {
		t.Fatalf("cleared registry file still names hidden rules:\n%s", raw)
	}
}

func TestHiddenAPIRefusesBadEntriesAndSavesNothing(t *testing.T) {
	srv := newRegistryServer(t)
	id := createHiddenTestServer(t, srv)
	if rec, body := doServersReq(t, srv, "PUT", "/api/servers/"+id+"/hidden", `{"tables":["shop.ok"],"columns":[]}`); rec.Code != 200 {
		t.Fatalf("seed = %d %s", rec.Code, body)
	}
	rec, body := doServersReq(t, srv, "PUT", "/api/servers/"+id+"/hidden",
		`{"tables":["shop.fine","audit_log"],"columns":["shop.customers.*"]}`)
	if rec.Code != 400 {
		t.Fatalf("bad PUT = %d %s, want 400", rec.Code, body)
	}
	for _, want := range []string{`audit_log`, `shop.customers.*`} {
		if !strings.Contains(string(body), want) {
			t.Errorf("refusal %s does not name %q", body, want)
		}
	}
	if e, _ := srv.cm.reg.Get(id); e.Hidden == nil || !slices.Equal(e.Hidden.Tables, []string{"shop.ok"}) {
		t.Fatalf("a refused PUT changed the stored rules: %+v", e.Hidden)
	}
}

func TestHiddenAPIBootAndUnknown(t *testing.T) {
	srv := newRegistryServer(t)
	rec, body := doServersReq(t, srv, "PUT", "/api/servers/"+bootServerID+"/hidden", `{"tables":["a.b"],"columns":[]}`)
	if rec.Code != 409 || !strings.Contains(string(body), "+ Add server") {
		t.Fatalf("boot PUT = %d %s, want 409 naming + Add server", rec.Code, body)
	}
	rec, body = doServersReq(t, srv, "GET", "/api/servers/"+bootServerID+"/hidden", "")
	if d := decodeHidden(t, body); rec.Code != 200 || d.Editable || d.Refusal == "" {
		t.Fatalf("boot GET = %d %+v, want not editable with a reason", rec.Code, d)
	}
	for _, m := range []string{"GET", "PUT"} {
		if rec, body := doServersReq(t, srv, m, "/api/servers/nope/hidden", `{}`); rec.Code != 404 {
			t.Errorf("%s unknown = %d %s, want 404", m, rec.Code, body)
		}
	}
}

// The connection form and the backup settings rebuild or rewrite the whole
// entry. Neither may drop the rules, and neither may set them: SetHidden is
// their only writer.
func TestHiddenRulesSurviveOtherEdits(t *testing.T) {
	srv := newRegistryServer(t)
	id := createHiddenTestServer(t, srv)
	if rec, body := doServersReq(t, srv, "PUT", "/api/servers/"+id+"/hidden",
		`{"tables":["shop.audit_log"],"columns":["shop.customers.email"]}`); rec.Code != 200 {
		t.Fatalf("seed = %d %s", rec.Code, body)
	}
	want := func(when string) {
		t.Helper()
		e, _ := srv.cm.reg.Get(id)
		if e.Hidden == nil || !slices.Equal(e.Hidden.Tables, []string{"shop.audit_log"}) ||
			!slices.Equal(e.Hidden.Columns, []string{"shop.customers.email"}) {
			t.Fatalf("after %s the rules are %+v", when, e.Hidden)
		}
	}
	if rec, body := doServersReq(t, srv, "PUT", "/api/servers/"+id,
		`{"name":"prod-renamed","host":"h","user":"u","dbname":"db"}`); rec.Code != 200 {
		t.Fatalf("connection edit = %d %s", rec.Code, body)
	}
	want("a connection edit that does not send them")
	if rec, body := doServersReq(t, srv, "PUT", "/api/backup-settings/servers/"+id, `{}`); rec.Code != 200 {
		t.Fatalf("backup settings edit = %d %s", rec.Code, body)
	}
	want("a backup settings edit")

	// The case Update's own guard exists for: an edit built from a copy read
	// BEFORE the rules changed, saved after. It must not put the old rules back.
	stale, _ := srv.cm.reg.Get(id)
	if rec, body := doServersReq(t, srv, "PUT", "/api/servers/"+id+"/hidden",
		`{"tables":["shop.newer"],"columns":[]}`); rec.Code != 200 {
		t.Fatalf("second save = %d %s", rec.Code, body)
	}
	stale.Name = "prod-stale-edit"
	if err := srv.cm.reg.Update(stale); err != nil {
		t.Fatal(err)
	}
	if e, _ := srv.cm.reg.Get(id); e.Hidden == nil || !slices.Equal(e.Hidden.Tables, []string{"shop.newer"}) || e.Hidden.Columns != nil {
		t.Fatalf("a stale edit put old rules back: %+v", e.Hidden)
	}
}

// Empty lists remove every rule, so a body that only LOOKS empty because of
// a typo, a wrong nesting or a null must be refused, never saved.
func TestHiddenAPIStrictBody(t *testing.T) {
	srv := newRegistryServer(t)
	id := createHiddenTestServer(t, srv)
	if rec, body := doServersReq(t, srv, "PUT", "/api/servers/"+id+"/hidden",
		`{"tables":["shop.audit_log"],"columns":[]}`); rec.Code != 200 {
		t.Fatalf("seed = %d %s", rec.Code, body)
	}
	for _, bad := range []string{
		`null`,
		`{}`,
		`{"table":["shop.x"],"columns":[]}`,
		`{"hidden":{"tables":[],"columns":[]}}`,
		`{"tables":[]}`,
		// A typo'd key next to valid ones: saving would silently drop the
		// column rule the operator meant to add.
		`{"tables":["shop.audit_log"],"columns":[],"colums":["shop.customers.email"]}`,
		`{"tables":[],"columns":[]} {"tables":[],"columns":[]}`,
	} {
		rec, body := doServersReq(t, srv, "PUT", "/api/servers/"+id+"/hidden", bad)
		if rec.Code != 400 {
			t.Errorf("body %s answered %d %s, want 400", bad, rec.Code, body)
		}
		if e, _ := srv.cm.reg.Get(id); e.Hidden == nil || !slices.Equal(e.Hidden.Tables, []string{"shop.audit_log"}) {
			t.Fatalf("body %s changed the stored rules to %+v", bad, e.Hidden)
		}
	}
}

func TestRegistryAddNeverCarriesHidden(t *testing.T) {
	reg, err := LoadRegistry(t.TempDir() + "/console-servers.yaml")
	if err != nil {
		t.Fatal(err)
	}
	e, err := reg.Add(ServerEntry{Name: "x", DSN: "u:p@tcp(h:3306)/db", Hidden: &hiderules.Rules{Tables: []string{"a.b"}}})
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := reg.Get(e.ID); got.Hidden != nil {
		t.Fatalf("Add stored hidden rules %+v; SetHidden is their only writer", got.Hidden)
	}
}

func TestHiddenAPIAuthzRows(t *testing.T) {
	for _, c := range []struct{ method, want string }{{"GET", "servers:read"}, {"PUT", "servers:write"}} {
		perm, ok := permForRoute(c.method, "/api/servers/abc/hidden")
		if !ok || string(perm) != c.want {
			t.Errorf("%s /api/servers/{id}/hidden = %q classified=%v, want %s", c.method, perm, ok, c.want)
		}
	}
}

// A server hiding only columns lists "tables": [] on the server list, never
// null, so a page can read its length.
func TestServerListHiddenNeverNull(t *testing.T) {
	srv := newRegistryServer(t)
	id := createHiddenTestServer(t, srv)
	if rec, body := doServersReq(t, srv, "PUT", "/api/servers/"+id+"/hidden",
		`{"tables":[],"columns":["shop.customers.email"]}`); rec.Code != 200 {
		t.Fatalf("save = %d %s", rec.Code, body)
	}
	_, body := doServersReq(t, srv, "GET", "/api/servers/"+id, "")
	if !strings.Contains(string(body), `"hidden":{"tables":[],"columns":["shop.customers.email"]}`) {
		t.Fatalf("server DTO hidden = %s", body)
	}
}

// Keys a newer build wrote under hidden: survive this build's saves, both an
// unrelated edit of the server and a save of the rules themselves.
func TestHiddenRulesKeepFutureKeys(t *testing.T) {
	path := t.TempDir() + "/console-servers.yaml"
	yamlText := "version: 1\nservers:\n- id: s1\n  name: prod\n  dsn: u:p@tcp(h:3306)/db\n" +
		"  hidden:\n    tables:\n    - shop.audit_log\n    future_rule: keep-me\n"
	if err := os.WriteFile(path, []byte(yamlText), 0o600); err != nil {
		t.Fatal(err)
	}
	reg, err := LoadRegistry(path)
	if err != nil {
		t.Fatal(err)
	}
	e, ok := reg.Get("s1")
	if !ok {
		t.Fatal("entry not loaded")
	}
	e.Name = "prod-renamed"
	if err := reg.Update(e); err != nil {
		t.Fatal(err)
	}
	if raw, _ := os.ReadFile(path); !strings.Contains(string(raw), "future_rule: keep-me") {
		t.Fatalf("an unrelated edit dropped the future key:\n%s", raw)
	}
	set, err := hiderules.Parse(hiderules.Rules{Columns: []string{"shop.customers.email"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reg.SetHidden("s1", set); err != nil {
		t.Fatal(err)
	}
	raw, _ := os.ReadFile(path)
	if !strings.Contains(string(raw), "future_rule: keep-me") || !strings.Contains(string(raw), "shop.customers.email") ||
		strings.Contains(string(raw), "shop.audit_log") {
		t.Fatalf("SetHidden must replace the lists and keep the future key:\n%s", raw)
	}
}

// Clearing the lists keeps the keys a newer build wrote, and GET names them
// so the operator is not told "nothing hidden" while the surfaces refuse.
func TestHiddenClearKeepsFutureKeysAndNamesThem(t *testing.T) {
	path := t.TempDir() + "/console-servers.yaml"
	yamlText := "version: 1\nservers:\n- id: s1\n  name: prod\n  dsn: u:p@tcp(h:3306)/db\n" +
		"  hidden:\n    tables:\n    - shop.audit_log\n    future_rule: keep-me\n"
	if err := os.WriteFile(path, []byte(yamlText), 0o600); err != nil {
		t.Fatal(err)
	}
	clearStores(t)
	reg, err := LoadRegistry(path)
	if err != nil {
		t.Fatal(err)
	}
	srv, err := New(Config{Listen: "127.0.0.1:8090", Token: "t", Registry: reg})
	if err != nil {
		t.Fatal(err)
	}
	rec, body := doServersReq(t, srv, "PUT", "/api/servers/s1/hidden", `{"tables":[],"columns":[]}`)
	if rec.Code != 200 {
		t.Fatalf("clear = %d %s", rec.Code, body)
	}
	if raw, _ := os.ReadFile(path); !strings.Contains(string(raw), "future_rule: keep-me") || strings.Contains(string(raw), "shop.audit_log") {
		t.Fatalf("clearing the lists must keep only the future key:\n%s", raw)
	}
	_, body = doServersReq(t, srv, "GET", "/api/servers/s1/hidden", "")
	if d := decodeHidden(t, body); !slices.Equal(d.UnreadableKeys, []string{"future_rule"}) {
		t.Fatalf("GET = %s, want unreadable_keys [future_rule]", body)
	}
}

func TestHiddenGetReadOnlyRegistryIsNotEditable(t *testing.T) {
	path := t.TempDir() + "/console-servers.yaml"
	if err := os.WriteFile(path, []byte("version: 99\nservers:\n- id: s1\n  name: prod\n  dsn: u:p@tcp(h:3306)/db\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	clearStores(t)
	reg, err := LoadRegistry(path)
	if err != nil {
		t.Fatal(err)
	}
	if !reg.ReadOnly() {
		t.Fatal("fixture: a newer registry version must open read-only")
	}
	srv, err := New(Config{Listen: "127.0.0.1:8090", Token: "t", Registry: reg})
	if err != nil {
		t.Fatal(err)
	}
	rec, body := doServersReq(t, srv, "GET", "/api/servers/s1/hidden", "")
	if d := decodeHidden(t, body); rec.Code != 200 || d.Editable || d.Refusal == "" {
		t.Fatalf("read-only GET = %d %s, want not editable with a reason", rec.Code, body)
	}
}
