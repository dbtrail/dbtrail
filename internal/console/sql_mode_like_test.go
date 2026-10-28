package console

import (
	"encoding/json"
	"testing"
)

// The LIKE switch rides on PUT /api/servers/{id}/sql-mode beside the fresh
// mode: each is saved only when it is sent, so one choice never resets the
// other, and the server list says both.
func TestSQLModeAPI_likeIgnoresCase(t *testing.T) {
	t.Setenv("BINTRAIL_SQL_TAILS", "")
	srv := newRegistryServer(t)
	id := createHiddenTestServer(t, srv)
	path := "/api/servers/" + id + "/sql-mode"
	put := func(body string) sqlModeDTO {
		t.Helper()
		rec, out := doServersReq(t, srv, "PUT", path, body)
		if rec.Code != 200 {
			t.Fatalf("PUT %s = %d %s", body, rec.Code, out)
		}
		return decodeSQLMode(t, out)
	}
	if d := put(`{"fresh":true}`); !d.Fresh || d.LikeIgnoresCase {
		t.Fatalf("fresh alone: %+v", d)
	}
	if d := put(`{"like_ignores_case":true}`); !d.Fresh || !d.LikeIgnoresCase {
		t.Fatalf("LIKE alone must keep fresh: %+v", d)
	}
	if e, _ := srv.cm.reg.Get(id); !e.SQLFresh || !e.SQLLikeIgnoresCase {
		t.Fatalf("stored %+v", e)
	}
	if d := put(`{"fresh":false}`); d.Fresh || !d.LikeIgnoresCase {
		t.Fatalf("fresh alone must keep LIKE: %+v", d)
	}
	if d := put(`{"fresh":true,"like_ignores_case":false}`); !d.Fresh || d.LikeIgnoresCase {
		t.Fatalf("both: %+v", d)
	}
	if d := put(`{"like_ignores_case":true}`); !d.LikeIgnoresCase {
		t.Fatalf("on again: %+v", d)
	}

	rec, out := doServersReq(t, srv, "GET", "/api/servers", "")
	var list struct {
		Servers []serverDTO `json:"servers"`
	}
	if rec.Code != 200 || json.Unmarshal(out, &list) != nil || len(list.Servers) != 1 || !list.Servers[0].SQLLikeIgnoresCase {
		t.Fatalf("the list does not say the saved switch: %d %s", rec.Code, out)
	}

	// The connection's target carries it: the port reads it there.
	if !srv.flashbackLikeIgnoresCase(id) {
		t.Error("the port's target does not carry the saved switch")
	}
	if srv.flashbackLikeIgnoresCase("nope") || srv.flashbackLikeIgnoresCase(bootServerID) {
		t.Error("an unknown server or the boot entry has the switch on")
	}

	// A body that only looks like a choice changes nothing.
	for _, bad := range []string{`{}`, `{"like_ignores_case":null}`, `{"like_ignores_case":"true"}`, `{"like_ignores_case":1}`, `{"likeIgnoresCase":false}`} {
		rec, out := doServersReq(t, srv, "PUT", path, bad)
		if rec.Code != 400 {
			t.Errorf("body %q answered %d %s, want 400", bad, rec.Code, out)
		}
		if e, _ := srv.cm.reg.Get(id); !e.SQLLikeIgnoresCase || !e.SQLFresh {
			t.Fatalf("body %q changed the saved choices: %+v", bad, e)
		}
	}
}
