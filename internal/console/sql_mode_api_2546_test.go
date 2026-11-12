package console

import (
	"strconv"
	"encoding/json"
	"os"
	"testing"
)

// #2546: PUT /api/servers/{id}/sql-mode saves a server's fresh mode. The
// server list carries what is saved; the router says what it applies.

func decodeSQLMode(t *testing.T, body []byte) sqlModeDTO {
	t.Helper()
	var d sqlModeDTO
	if err := json.Unmarshal(body, &d); err != nil {
		t.Fatalf("decode %s: %v", body, err)
	}
	return d
}

func TestSQLModeAPI_savesAndTheListSaysIt_2546(t *testing.T) {
	t.Setenv("BINTRAIL_SQL_TAILS", "")
	srv := newRegistryServer(t)
	id := createHiddenTestServer(t, srv)
	path := "/api/servers/" + id + "/sql-mode"
	for _, on := range []bool{true, false, true} {
		body := `{"fresh":false}`
		if on {
			body = `{"fresh":true}`
		}
		rec, out := doServersReq(t, srv, "PUT", path, body)
		if rec.Code != 200 || decodeSQLMode(t, out).Fresh != on {
			t.Fatalf("PUT %s = %d %s", body, rec.Code, out)
		}
		if e, _ := srv.cm.reg.Get(id); e.SQLFreshOn() != on || !e.SQLFreshSaved() {
			t.Fatalf("PUT %s stored sql_mode %q", body, e.SQLMode)
		}
		// Applied in this process too, not only at the next reading.
		if srv.tailsOnFor(id) != on {
			t.Fatalf("PUT %s: this process's statements read tails = %v", body, srv.tailsOnFor(id))
		}
	}
	rec, out := doServersReq(t, srv, "GET", "/api/servers", "")
	if rec.Code != 200 {
		t.Fatalf("list = %d %s", rec.Code, out)
	}
	var list struct {
		Servers []serverDTO `json:"servers"`
	}
	if err := json.Unmarshal(out, &list); err != nil {
		t.Fatal(err)
	}
	if len(list.Servers) != 1 || !list.Servers[0].SQLFresh {
		t.Fatalf("the list does not say the saved mode: %s", out)
	}
}

// A body that only LOOKS like a choice is refused and changes nothing: {}
// must never turn fresh mode off.
func TestSQLModeAPI_strictBody_2546(t *testing.T) {
	t.Setenv("BINTRAIL_SQL_TAILS", "")
	srv := newRegistryServer(t)
	id := createHiddenTestServer(t, srv)
	path := "/api/servers/" + id + "/sql-mode"
	// Over each saved choice: a refused body must leave it saved as it was,
	// not turn it to the other and not clear it to the default.
	for _, seed := range []bool{true, false} {
		if rec, out := doServersReq(t, srv, "PUT", path, `{"fresh":`+strconv.FormatBool(seed)+`}`); rec.Code != 200 {
			t.Fatalf("seed = %d %s", rec.Code, out)
		}
		for _, bad := range []string{``, `null`, `{}`, `{"fresh":null}`, `{"fresh":"false"}`, `{"fresh":0}`, `{"fresh":"true"}`, `{"fresh":1}`,
			`{"Fresh":false,"mode":"fast"}`, `{"fresh":false,"extra":1}`, `{"fresh":false} {"fresh":false}`,
			`{"fresh":true,"extra":1}`, `{"fresh":true} {"fresh":true}`} {
			rec, out := doServersReq(t, srv, "PUT", path, bad)
			if rec.Code != 400 {
				t.Errorf("body %q answered %d %s, want 400", bad, rec.Code, out)
			}
			if e, _ := srv.cm.reg.Get(id); e.SQLFreshOn() != seed || !e.SQLFreshSaved() {
				t.Fatalf("over fresh=%v saved, body %q left sql_mode %q", seed, bad, e.SQLMode)
			}
		}
	}
}

func TestSQLModeAPI_refusals_2546(t *testing.T) {
	srv := newRegistryServer(t)
	if rec, out := doServersReq(t, srv, "PUT", "/api/servers/"+bootServerID+"/sql-mode", `{"fresh":true}`); rec.Code != 409 {
		t.Errorf("boot server = %d %s, want 409", rec.Code, out)
	}
	if rec, out := doServersReq(t, srv, "PUT", "/api/servers/nope/sql-mode", `{"fresh":true}`); rec.Code != 404 {
		t.Errorf("unknown server = %d %s, want 404", rec.Code, out)
	}
}

func TestSQLModeAPI_readOnlyRegistry_2546(t *testing.T) {
	path := t.TempDir() + "/console-servers.yaml"
	if err := os.WriteFile(path, []byte("version: 99\nservers:\n- id: s1\n  name: prod\n  index_dsn: u:p@tcp(h:3306)/db\n"), 0o600); err != nil {
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
	if rec, out := doServersReq(t, srv, "PUT", "/api/servers/s1/sql-mode", `{"fresh":true}`); rec.Code != 409 {
		t.Fatalf("read-only registry = %d %s, want 409", rec.Code, out)
	}
}

// Per-server configuration, like the hidden tables beside it.
func TestSQLModeAPI_authzRow_2546(t *testing.T) {
	perm, ok := permForRoute("PUT", "/api/servers/abc/sql-mode")
	if !ok || string(perm) != "servers:write" {
		t.Fatalf("PUT /api/servers/{id}/sql-mode = %q classified=%v, want servers:write", perm, ok)
	}
}
