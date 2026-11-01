package console

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/ext"
)

// #2596 part 3: a per-server age past which DBTrail removes snapshots from
// the server's S3 destination. Off unless set.

func TestParseS3Retain(t *testing.T) {
	for _, c := range []struct {
		in   string
		want time.Duration
		bad  string
	}{
		{"", 0, ""},
		{"   ", 0, ""},
		{"7d", 7 * 24 * time.Hour, ""},
		{" 30d ", 30 * 24 * time.Hour, ""},
		{"24h", 24 * time.Hour, ""},
		{"1d", 24 * time.Hour, ""},
		{"12h", 0, "shorter than a day"},
		{"0d", 0, "delete from S3 after"},
		{"5m", 0, "delete from S3 after"},
		{"7D", 0, "delete from S3 after"},
		{"seven", 0, "delete from S3 after"},
	} {
		got, err := ParseS3Retain(c.in)
		if c.bad != "" {
			if err == nil || !strings.Contains(err.Error(), c.bad) {
				t.Errorf("%q: err=%v, want one saying %q", c.in, err, c.bad)
			}
			continue
		}
		if err != nil || got != c.want {
			t.Errorf("%q = %v, %v; want %v", c.in, got, err, c.want)
		}
	}
}

func TestS3RetentionFor(t *testing.T) {
	week, month := 7*24*time.Hour, 30*24*time.Hour
	e := func(id, s3, retain string) ServerEntry {
		return ServerEntry{ID: id, Name: id, BaselineS3: s3, BaselineS3Retain: retain}
	}
	for _, c := range []struct {
		name    string
		entries []ServerEntry
		global  string
		dest    string
		local   time.Duration
		want    time.Duration
		on      bool
	}{
		{"one server", []ServerEntry{e("a", "s3://b/p", "7d")}, "", "s3://b/p", 0, week, true},
		{"spelled with a slash", []ServerEntry{e("a", "s3://b/p/", "7d")}, "", "s3://b/p", 0, week, true},
		{"off when not set", []ServerEntry{e("a", "s3://b/p", "")}, "", "s3://b/p", 0, 0, false},
		{"a value this build cannot read is off", []ServerEntry{e("a", "s3://b/p", "7x")}, "", "s3://b/p", 0, 0, false},
		{"shared: the longest", []ServerEntry{e("a", "s3://b/p", "7d"), e("b", "s3://b/p", "30d")}, "", "s3://b/p", 0, month, true},
		{"shared with one that has none: off", []ServerEntry{e("a", "s3://b/p", "7d"), e("b", "s3://b/p", "")}, "", "s3://b/p", 0, 0, false},
		{"the daemon's own destination: off", []ServerEntry{e("a", "s3://b/p", "7d")}, "s3://b/p/", "s3://b/p", 0, 0, false},
		{"never shorter than the local age", []ServerEntry{e("a", "s3://b/p", "7d")}, "", "s3://b/p", month, month, true},
		{"a nested prefix is another destination", []ServerEntry{e("a", "s3://b/p", "7d"), e("b", "s3://b/p/b", "")}, "", "s3://b/p", 0, week, true},
		{"no server uses it", []ServerEntry{e("a", "s3://b/q", "7d")}, "", "s3://b/p", 0, 0, false},
	} {
		got, ids, on := S3RetentionFor(c.entries, c.global, c.dest, c.local)
		if got != c.want || on != c.on {
			t.Errorf("%s: = %v, %v (sharers %v); want %v, %v", c.name, got, on, ids, c.want, c.on)
		}
	}
	_, ids, _ := S3RetentionFor([]ServerEntry{e("a", "s3://b/p", "7d"), e("b", "s3://b/p/", "30d"), e("c", "s3://b/q", "7d")}, "", "s3://b/p", 0)
	if strings.Join(ids, ",") != "a,b" {
		t.Errorf("sharers = %v, want a,b", ids)
	}
}

func TestBackupSettingsAPI_s3Retain(t *testing.T) {
	srv, id := newScheduleServer(t, nil)
	path := "/api/backup-settings/servers/" + id
	put := func(body string) (int, string) {
		t.Helper()
		rec, raw := doServersReq(t, srv, "PUT", path, body)
		return rec.Code, string(raw)
	}
	// No destination: nothing to remove from.
	if code, body := put(`{"baseline_s3_retain":"7d"}`); code != 400 || !strings.Contains(body, "no S3 destination") {
		t.Fatalf("without a destination: %d %s", code, body)
	}
	if code, body := put(`{"baseline_s3":"s3://bkt/snaps","baseline_s3_retain":"7d"}`); code != 200 {
		t.Fatalf("set: %d %s", code, body)
	} else {
		var dto struct {
			BaselineS3Retain string `json:"baseline_s3_retain"`
		}
		if err := json.Unmarshal([]byte(body), &dto); err != nil || dto.BaselineS3Retain != "7d" {
			t.Fatalf("response %s", body)
		}
	}
	if e, _ := srv.cm.reg.Get(id); e.BaselineS3Retain != "7d" {
		t.Fatalf("stored %q", e.BaselineS3Retain)
	}
	// Omitted: kept.
	if code, body := put(`{"no_archive":false}`); code != 200 {
		t.Fatalf("%d %s", code, body)
	}
	if e, _ := srv.cm.reg.Get(id); e.BaselineS3Retain != "7d" {
		t.Fatalf("an unrelated save changed it: %q", e.BaselineS3Retain)
	}
	if code, body := put(`{"baseline_s3_retain":"12h"}`); code != 400 || !strings.Contains(body, "shorter than a day") {
		t.Fatalf("too short: %d %s", code, body)
	}
	// Cleared: off.
	if code, body := put(`{"baseline_s3_retain":""}`); code != 200 {
		t.Fatalf("%d %s", code, body)
	}
	if e, _ := srv.cm.reg.Get(id); e.BaselineS3Retain != "" {
		t.Fatalf("not cleared: %q", e.BaselineS3Retain)
	}
	// Set, then the destination removed: kept inert, and a later save that
	// does not mention it is not refused for it.
	if code, body := put(`{"baseline_s3_retain":"7d"}`); code != 200 {
		t.Fatalf("%d %s", code, body)
	}
	if code, body := put(`{"baseline_s3":""}`); code != 200 {
		t.Fatalf("removing the destination: %d %s", code, body)
	}
	if code, body := put(`{"no_archive":true}`); code != 200 {
		t.Fatalf("an inert age blocked a save: %d %s", code, body)
	}
}

// Removing data from the bucket is the tier of the daemon-wide "Delete by
// age" (settings:write), not this row's (servers:write): a session without
// it cannot set or change the age, and can still turn it off. A save that
// moves the destination turns the age off, so a set age is never aimed at a
// new prefix by a session that could not set it there.
func TestBackupSettingsAPI_s3RetainNeedsSettingsWrite(t *testing.T) {
	srv, id := newScheduleServer(t, nil)
	do := func(perms []ext.Permission, body string) (int, string) {
		t.Helper()
		req := httptest.NewRequest("PUT", "/api/backup-settings/servers/"+id, strings.NewReader(body))
		if perms != nil {
			req = req.WithContext(withPolicy(&ext.AccessPolicy{Permissions: perms}))
		}
		req.SetPathValue("id", id)
		rec := httptest.NewRecorder()
		srv.handleBackupSettingsServerUpdate(rec, req)
		return rec.Code, rec.Body.String()
	}
	operator := []ext.Permission{ext.PermServersWrite}
	admin := []ext.Permission{ext.PermServersWrite, ext.PermSettingsWrite}
	if code, body := do(operator, `{"baseline_s3":"s3://bkt/snaps"}`); code != 200 {
		t.Fatalf("set destination: %d %s", code, body)
	}
	if code, body := do(operator, `{"baseline_s3_retain":"7d"}`); code != 403 || !strings.Contains(body, string(ext.PermSettingsWrite)) {
		t.Fatalf("operator set the age: %d %s", code, body)
	}
	if code, body := do(admin, `{"baseline_s3_retain":"7d"}`); code != 200 {
		t.Fatalf("admin: %d %s", code, body)
	}
	// Saved again unchanged by the operator (a page sending the row whole).
	if code, body := do(operator, `{"baseline_s3_retain":"7d","no_archive":true}`); code != 200 {
		t.Fatalf("an unchanged age refused the operator's save: %d %s", code, body)
	}
	if code, body := do(operator, `{"baseline_s3_retain":"30d"}`); code != 403 {
		t.Fatalf("operator changed the age: %d %s", code, body)
	}
	// Moving the destination turns it off.
	if code, body := do(operator, `{"baseline_s3":"s3://bkt/elsewhere"}`); code != 200 {
		t.Fatalf("move: %d %s", code, body)
	}
	if e, _ := srv.cm.reg.Get(id); e.BaselineS3Retain != "" {
		t.Fatalf("the age followed the destination to a new prefix: %q", e.BaselineS3Retain)
	}
	if code, body := do(admin, `{"baseline_s3_retain":"7d"}`); code != 200 {
		t.Fatalf("%d %s", code, body)
	}
	// Turning it off is allowed to the operator.
	if code, body := do(operator, `{"baseline_s3_retain":""}`); code != 200 {
		t.Fatalf("operator could not turn it off: %d %s", code, body)
	}
	// Moving the destination with the age in the same save is the admin's.
	if code, _ := do(operator, `{"baseline_s3":"s3://bkt/third","baseline_s3_retain":"7d"}`); code != 403 {
		t.Fatalf("operator moved and set at once: %d", code)
	}
	if code, body := do(admin, `{"baseline_s3":"s3://bkt/third","baseline_s3_retain":"7d"}`); code != 200 {
		t.Fatalf("admin moved and set at once: %d %s", code, body)
	}
	if e, _ := srv.cm.reg.Get(id); e.BaselineS3 != "s3://bkt/third" || e.BaselineS3Retain != "7d" {
		t.Fatalf("stored %q %q", e.BaselineS3, e.BaselineS3Retain)
	}
}

// Spellings S3 itself tells apart stay apart, and the one it does not (a
// trailing slash) is one destination: what the backend writes decides.
func TestS3RetentionFor_spellings(t *testing.T) {
	e := []ServerEntry{{ID: "a", BaselineS3: "s3://b/p", BaselineS3Retain: "7d"}, {ID: "b", BaselineS3: "s3://b/p/"}}
	if _, _, on := S3RetentionFor(e, "", "s3://b/p", 0); on {
		t.Fatal("a trailing slash made two destinations of one")
	}
}

// An edit of the connection rebuilds the entry; the S3 age survives it while
// the destination stays, and goes with a moved one.
func TestServersAPI_editKeepsTheS3Age(t *testing.T) {
	srv := newRegistryServer(t)
	dir := t.TempDir()
	_, body := doServersReq(t, srv, "POST", "/api/servers",
		`{"name":"prod","host":"h","user":"u","password":"pw","dbname":"db","baseline_dir":"`+dir+`","baseline_s3":"s3://bkt/snaps"}`)
	var created serverDTO
	if err := json.Unmarshal(body, &created); err != nil || created.ID == "" {
		t.Fatalf("create: %s", body)
	}
	e, _ := srv.cm.reg.Get(created.ID)
	e.BaselineS3Retain = "7d"
	if err := srv.cm.reg.Update(e); err != nil {
		t.Fatal(err)
	}
	if rec, b := doServersReq(t, srv, "PUT", "/api/servers/"+created.ID,
		`{"name":"prod","host":"h","user":"u","password":"new","dbname":"db","baseline_dir":"`+dir+`","baseline_s3":"s3://bkt/snaps/"}`); rec.Code != 200 {
		t.Fatalf("edit: %d %s", rec.Code, b)
	}
	if e, _ = srv.cm.reg.Get(created.ID); e.BaselineS3Retain != "7d" {
		t.Fatalf("a password change turned the S3 age off: %q", e.BaselineS3Retain)
	}
	if rec, b := doServersReq(t, srv, "PUT", "/api/servers/"+created.ID,
		`{"name":"prod","host":"h","user":"u","dbname":"db","baseline_dir":"`+dir+`","baseline_s3":"s3://bkt/other"}`); rec.Code != 200 {
		t.Fatalf("move: %d %s", rec.Code, b)
	}
	if e, _ = srv.cm.reg.Get(created.ID); e.BaselineS3Retain != "" {
		t.Fatalf("the S3 age followed the server to a new prefix: %q", e.BaselineS3Retain)
	}
}

// The age of a shared destination comes from every server on it, so moving
// or deleting one can switch the cleanup on for another, or shorten it.
// That asks for settings:write too.
func TestS3AgeLoweredByAnotherServer(t *testing.T) {
	srv := newRegistryServer(t)
	add := func(name, age string) string {
		t.Helper()
		e, err := srv.cm.reg.Add(ServerEntry{Name: name, DSN: "u:p@tcp(h:3306)/" + name, BaselineDir: t.TempDir(), BaselineS3: "s3://b/shared", BaselineS3Retain: age})
		if err != nil {
			t.Fatal(err)
		}
		srv.cm.bundles[e.ID] = &bundle{}
		return e.ID
	}
	x := add("x", "30d")
	y := add("y", "")
	z := add("z", "7d")
	do := func(perms []ext.Permission, method, path, id, body string) int {
		t.Helper()
		req := httptest.NewRequest(method, path, strings.NewReader(body))
		req = req.WithContext(withPolicy(&ext.AccessPolicy{Permissions: perms}))
		req.SetPathValue("id", id)
		rec := httptest.NewRecorder()
		switch {
		case method == "DELETE":
			srv.handleServersDelete(rec, req)
		default:
			srv.handleBackupSettingsServerUpdate(rec, req)
		}
		return rec.Code
	}
	operator := []ext.Permission{ext.PermServersWrite}
	admin := []ext.Permission{ext.PermServersWrite, ext.PermSettingsWrite}
	// y has no age, so nothing is cleaned; removing y would switch it on.
	if c := do(operator, "DELETE", "/api/servers/"+y, y, ""); c != 403 {
		t.Fatalf("operator deleted the server that kept the cleanup off: %d", c)
	}
	if c := do(operator, "PUT", "/api/backup-settings/servers/"+y, y, `{"baseline_s3":"s3://b/elsewhere"}`); c != 403 {
		t.Fatalf("operator moved the server that kept the cleanup off: %d", c)
	}
	if c := do(admin, "PUT", "/api/backup-settings/servers/"+y, y, `{"baseline_s3":"s3://b/elsewhere"}`); c != 200 {
		t.Fatalf("admin move: %d", c)
	}
	// Now x (30d) and z (7d) share it, at 30d; removing x shortens it to 7d.
	if c := do(operator, "DELETE", "/api/servers/"+x, x, ""); c != 403 {
		t.Fatalf("operator shortened the age by deleting x: %d", c)
	}
	// Removing z keeps 30d: not lowered, allowed.
	if c := do(operator, "DELETE", "/api/servers/"+z, z, ""); c != 204 {
		t.Fatalf("operator delete that lowers nothing: %d", c)
	}
}

// Two deletes at once, each harmless alone: y and z each keep the shared
// cleanup off. The check and the write are one step, so the second is
// checked against the registry the first left, and refused.
func TestS3AgeLowered_concurrentDeletes(t *testing.T) {
	for range 20 {
		srv := newRegistryServer(t)
		ids := map[string]string{}
		for _, n := range []string{"x", "y", "z"} {
			age := ""
			if n == "x" {
				age = "7d"
			}
			e, err := srv.cm.reg.Add(ServerEntry{Name: n, DSN: "u:p@tcp(h:3306)/" + n, BaselineDir: t.TempDir(), BaselineS3: "s3://b/shared", BaselineS3Retain: age})
			if err != nil {
				t.Fatal(err)
			}
			srv.cm.bundles[e.ID] = &bundle{}
			ids[n] = e.ID
		}
		codes := make(chan int, 2)
		for _, n := range []string{"y", "z"} {
			go func(id string) {
				req := httptest.NewRequest("DELETE", "/api/servers/"+id, nil)
				req = req.WithContext(withPolicy(&ext.AccessPolicy{Permissions: []ext.Permission{ext.PermServersWrite}}))
				req.SetPathValue("id", id)
				rec := httptest.NewRecorder()
				srv.handleServersDelete(rec, req)
				codes <- rec.Code
			}(ids[n])
		}
		a, b := <-codes, <-codes
		if a == 204 && b == 204 {
			t.Fatal("both deletes went through: the shared cleanup is now on without settings:write")
		}
		if _, _, on := S3RetentionFor(srv.cm.reg.List(), "", "s3://b/shared", 0); on {
			t.Fatal("the cleanup was switched on")
		}
	}
}
