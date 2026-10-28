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

// Changing the LIKE switch closes the server's open connections on the port,
// whichever process changed it: one opened before keeps the switch it read,
// and turning it off is how an operator stops a wrong answer.
func TestSQLModeAPI_likeSwitchClosesOpenConnections(t *testing.T) {
	t.Setenv("BINTRAIL_SQL_TAILS", "")
	srv := newRegistryServer(t)
	id := createHiddenTestServer(t, srv)
	path := "/api/servers/" + id + "/sql-mode"
	dropped := 0
	track := func() {
		t.Helper()
		if _, ok := srv.TrackRoutedConn(id, srv.routed.generation(id), func() { dropped++ }, nil); !ok {
			t.Fatal("track refused")
		}
	}
	track()
	// What the source said about the forwarding account is still true
	// after a LIKE change, which does not touch the account.
	srv.routing.setAccountRefused(id, "access denied")
	if rec, out := doServersReq(t, srv, "PUT", path, `{"fresh":true}`); rec.Code != 200 || dropped != 0 {
		t.Fatalf("fresh alone: %d %s, dropped %d, want none dropped", rec.Code, out, dropped)
	}
	if rec, out := doServersReq(t, srv, "PUT", path, `{"like_ignores_case":true}`); rec.Code != 200 || dropped != 1 {
		t.Fatalf("switch on: %d %s, dropped %d, want 1", rec.Code, out, dropped)
	}
	srv.routing.mu.Lock()
	got := srv.routing.accountRefused[id]
	srv.routing.mu.Unlock()
	if got != "access denied" {
		t.Errorf("a LIKE change cleared the account refusal: %q", got)
	}
	track()
	if rec, _ := doServersReq(t, srv, "PUT", path, `{"like_ignores_case":true}`); rec.Code != 200 || dropped != 1 {
		t.Fatalf("saved again unchanged: dropped %d, want still 1", dropped)
	}

	// The same from another process's write, read by the follower.
	c := RegistryChange{Old: mustEntry(t, srv, id), New: mustEntry(t, srv, id)}
	c.New.SQLLikeIgnoresCase = !c.Old.SQLLikeIgnoresCase
	srv.applyRegistryChange(c)
	if dropped != 2 {
		t.Fatalf("a change read from the file: dropped %d, want 2", dropped)
	}
}

func mustEntry(t *testing.T, srv *Server, id string) ServerEntry {
	t.Helper()
	e, ok := srv.cm.reg.Get(id)
	if !ok {
		t.Fatalf("no entry %s", id)
	}
	return e
}
