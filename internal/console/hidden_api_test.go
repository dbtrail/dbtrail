package console

import (
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
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

// Rules this build cannot enforce are said on every surface that reports
// them (#2529): GET and PUT carry the refusal the analytical surfaces give,
// and the server list keeps the server marked as hiding something even when
// its rules are ONLY a key a newer build wrote, or a hand edit that does not
// parse. Without that, the console would say "nothing is hidden" about a
// server whose SQL is refused.
func TestHiddenUnenforceableRulesAreSaid(t *testing.T) {
	cases := []struct {
		name, hidden, wantRefused string
		wantKeys                  []string
		wantTables                []string
	}{
		{name: "only an unknown key", hidden: "  hidden:\n    future_rule: keep-me\n",
			wantRefused: "future_rule, which this build cannot read", wantKeys: []string{"future_rule"}, wantTables: []string{}},
		{name: "a hand edit that does not parse", hidden: "  hidden:\n    tables:\n    - audit_log\n",
			wantRefused: "do not parse", wantTables: []string{"audit_log"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			path := t.TempDir() + "/console-servers.yaml"
			if err := os.WriteFile(path, []byte("version: 1\nservers:\n- id: s1\n  name: prod\n  dsn: u:p@tcp(h:3306)/db\n"+c.hidden), 0o600); err != nil {
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
			e, _ := reg.Get("s1")
			_, lerr := hiderules.Load(e.Hidden)
			if lerr == nil {
				t.Fatal("the fixture loads; it must not")
			}
			_, body := doServersReq(t, srv, "GET", "/api/servers/s1/hidden", "")
			d := decodeHidden(t, body)
			if d.Refused != lerr.Error() || !strings.Contains(d.Refused, c.wantRefused) || !slices.Equal(d.UnreadableKeys, c.wantKeys) ||
				!slices.Equal(d.Tables, c.wantTables) {
				t.Fatalf("GET = %s, want refused %q", body, lerr.Error())
			}
			_, body = doServersReq(t, srv, "GET", "/api/servers/s1", "")
			var dto serverDTO
			if err := json.Unmarshal(body, &dto); err != nil {
				t.Fatal(err)
			}
			if dto.Hidden == nil || !dto.Hidden.Unreadable || !slices.Equal(dto.Hidden.Tables, c.wantTables) || dto.Hidden.Columns == nil {
				t.Fatalf("server DTO hidden = %s, want it present and marked unreadable", body)
			}
			// A save that leaves an unknown key in place still answers refused.
			rec, body := doServersReq(t, srv, "PUT", "/api/servers/s1/hidden", `{"tables":[],"columns":[]}`)
			if rec.Code != 200 {
				t.Fatalf("PUT = %d %s", rec.Code, body)
			}
			d = decodeHidden(t, body)
			if (len(c.wantKeys) > 0) != (d.Refused != "") {
				t.Fatalf("PUT = %s: refused must follow what is still stored", body)
			}
		})
	}
	// A server whose rules all load carries no refusal and no mark.
	srv := newRegistryServer(t)
	id := createHiddenTestServer(t, srv)
	_, body := doServersReq(t, srv, "PUT", "/api/servers/"+id+"/hidden", `{"tables":["shop.a"],"columns":[]}`)
	if d := decodeHidden(t, body); d.Refused != "" {
		t.Fatalf("PUT = %s", body)
	}
	_, body = doServersReq(t, srv, "GET", "/api/servers/"+id, "")
	var dto serverDTO
	if err := json.Unmarshal(body, &dto); err != nil {
		t.Fatal(err)
	}
	if dto.Hidden == nil || dto.Hidden.Unreadable || !slices.Equal(dto.Hidden.Tables, []string{"shop.a"}) {
		t.Fatalf("server DTO hidden = %+v, want the rule and not unreadable", dto.Hidden)
	}
}

// Rules are reported the way they are enforced: through hiderules.Load. A
// stored list of blank entries hides nothing, so it is no rule at all on the
// server list, and GET answers empty lists with no refusal.
func TestHiddenBlankEntriesHideNothing(t *testing.T) {
	path := t.TempDir() + "/console-servers.yaml"
	if err := os.WriteFile(path, []byte("version: 1\nservers:\n- id: s1\n  name: prod\n  dsn: u:p@tcp(h:3306)/db\n"+
		"  hidden:\n    tables:\n    - \"  \"\n    columns:\n    - \"\"\n"), 0o600); err != nil {
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
	_, body := doServersReq(t, srv, "GET", "/api/servers/s1", "")
	var dto serverDTO
	if err := json.Unmarshal(body, &dto); err != nil {
		t.Fatal(err)
	}
	if dto.Hidden != nil {
		t.Fatalf("server DTO hidden = %+v, want none", dto.Hidden)
	}
	_, body = doServersReq(t, srv, "GET", "/api/servers/s1/hidden", "")
	if d := decodeHidden(t, body); len(d.Tables)+len(d.Columns) != 0 || d.Refused != "" {
		t.Fatalf("GET = %s", body)
	}
}

// hiddenGet calls the GET handler straight, for a fixture server built
// without the HTTP routes.
func hiddenGet(t *testing.T, s *Server, id string) (int, []byte) {
	t.Helper()
	req := httptest.NewRequest("GET", "/api/servers/"+id+"/hidden", nil)
	req.SetPathValue("id", id)
	rec := httptest.NewRecorder()
	s.handleHiddenGet(rec, req)
	return rec.Code, rec.Body.Bytes()
}

// A rule that matches nothing in the copy is reported (#2529: "a rule that
// matches no column is reported in the settings"), against the newest
// snapshot the SQL surfaces read: shop.orders(id, status).
func TestHiddenUnmatchedAgainstTheCopy(t *testing.T) {
	f, _ := hiddenSQLFixture(t, &hiderules.Rules{
		Tables:  []string{"shop.orders", "shop.nothere", "nope.*", "SHOP.*"},
		Columns: []string{"shop.orders.STATUS", "shop.orders.stauts", "shop.*.status", "shop.missing.status", "other.*.id"},
	})
	// shop.customers(id, email) carries its CREATE TABLE, so its columns are
	// known; shop.orders does not, so a column rule on it is never called
	// unmatched (shop.orders.stauts counts as a possible match).
	createSQL := "CREATE TABLE `customers` (\n  `id` int NOT NULL,\n  `email` varchar(64) DEFAULT NULL,\n  PRIMARY KEY (`id`)\n);\n"
	cols, err := baseline.ParseSchemaText(createSQL)
	if err != nil {
		t.Fatal(err)
	}
	w, err := baseline.NewWriter(filepath.Join(f.schemaDir, "customers.parquet"), cols, baseline.WriterConfig{Compression: "none", RowGroupSize: 100,
		Metadata: map[string]string{baseline.MetaKeyCreateTableSQL: createSQL}})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.WriteRow([]string{"1", "a@x"}, make([]bool, 2)); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	reg := f.s.cm.reg
	id := f.s.cm.boot.serverID
	set, err := hiderules.Parse(hiderules.Rules{
		Tables:  []string{"shop.orders", "shop.nothere", "nope.*", "SHOP.*"},
		Columns: []string{"shop.orders.STATUS", "shop.orders.stauts", "shop.*.status", "shop.missing.status", "other.*.id", "shop.customers.EMAIL", "shop.customers.emial"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reg.SetHidden(id, set); err != nil {
		t.Fatal(err)
	}
	code, body := hiddenGet(t, f.s, id)
	if code != 200 {
		t.Fatalf("GET = %d %s", code, body)
	}
	d := decodeHidden(t, body)
	want := []string{"shop.nothere", "nope.*", "shop.missing.status", "other.*.id", "shop.customers.emial"}
	if d.Unmatched == nil || !slices.Equal(*d.Unmatched, want) || d.Unchecked != "" {
		t.Fatalf("GET = %s, want unmatched %q", body, want)
	}
}

// A copy that cannot be read is never reported as "every rule matches" nor
// as "no rule matches": the field is left out and the reason given.
func TestHiddenUnmatchedNotCheckedWithoutACopy(t *testing.T) {
	f, _ := hiddenSQLFixture(t, &hiderules.Rules{Tables: []string{"shop.nothere"}})
	id := f.s.cm.boot.serverID
	f.s.cm.boot.baselineSrc = t.TempDir() // no snapshot in it
	code, body := hiddenGet(t, f.s, id)
	d := decodeHidden(t, body)
	if code != 200 || d.Unmatched != nil || d.Unchecked == "" || strings.Contains(string(body), `"unmatched"`) {
		t.Fatalf("GET = %d %s, want no unmatched field and a reason", code, body)
	}
	if !strings.Contains(d.Unchecked, "no snapshot of this server on this host yet") {
		t.Fatalf("empty folder reason = %q", d.Unchecked)
	}
	// A snapshot folder that will not open is not "no snapshot yet": the
	// operator would wait for a snapshot that already exists.
	locked := t.TempDir()
	if err := os.Chmod(locked, 0); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(locked, 0o700) })
	f.s.cm.boot.baselineSrc = locked
	_, body = hiddenGet(t, f.s, id)
	if d := decodeHidden(t, body); d.Unmatched != nil || !strings.Contains(d.Unchecked, "could not be read") || strings.Contains(d.Unchecked, "yet") {
		t.Fatalf("unreadable folder GET = %s, want the read error as the reason", body)
	}
	f.s.cm.boot.baselineSrc = "s3://bucket/snaps"
	_, body = hiddenGet(t, f.s, id)
	if d := decodeHidden(t, body); d.Unmatched != nil || d.Unchecked == "" {
		t.Fatalf("S3-only GET = %s, want no unmatched field and a reason", body)
	}
	// No rules: nothing to check and nothing said.
	f2, _ := hiddenSQLFixture(t, nil)
	_, body = hiddenGet(t, f2.s, f2.s.cm.boot.serverID)
	if d := decodeHidden(t, body); d.Unmatched != nil || d.Unchecked != "" {
		t.Fatalf("no rules GET = %s", body)
	}
}
