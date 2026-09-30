package console

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The saved Connect form never holds the database password (#1804). It is one
// form shared by every session allowed to add servers, a data-profile session
// included, so a password stored in it would be readable by a person it was
// never typed for. The screen keeps the password only in the page and asks for
// it again after a reload.
//
// Every way a password could reach the draft is driven here: the check that
// saves the form before it runs, and a PUT that sends one anyway. Both the
// answer to GET and the raw bytes on disk are read, since either one leaking
// is the bug.
func TestConnectDraftNeverStoresThePassword(t *testing.T) {
	const secret = "Zq9-never-kept"
	dir := t.TempDir()
	srv, ctrl := newSupervisorServer(t)
	path := filepath.Join(dir, "console-connect-draft.yaml")
	srv.drafts = NewDraftStore(path)

	assertClean := func(when string) {
		t.Helper()
		rec, body := doServersReq(t, srv, "GET", "/api/servers/draft", "")
		if rec.Code != 200 {
			t.Fatalf("%s: GET code=%d body=%s", when, rec.Code, body)
		}
		if !strings.Contains(string(body), `"found":true`) {
			t.Fatalf("%s: nothing was saved, so this proves nothing: %s", when, body)
		}
		if strings.Contains(string(body), secret) || strings.Contains(string(body), "source_password") {
			t.Errorf("%s: the saved form hands back the password: %s", when, body)
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("%s: read the file: %v", when, err)
		}
		if strings.Contains(string(raw), secret) || strings.Contains(string(raw), "source_password") {
			t.Errorf("%s: the password is on disk:\n%s", when, raw)
		}
		if !strings.Contains(string(raw), "db.example.com") {
			t.Errorf("%s: the host is not on disk, so the file check proves nothing:\n%s", when, raw)
		}
	}

	ctrl.report = &DoctorReport{Failed: 1, Checks: []DoctorCheck{{Name: "x", Status: "fail"}}}
	doServersReq(t, srv, "POST", "/api/servers/check",
		`{"source_host":"db.example.com","source_user":"dbtrail","source_password":"`+secret+`"}`)
	assertClean("after a failed check")

	if rec, body := doServersReq(t, srv, "PUT", "/api/servers/draft",
		`{"source_host":"db.example.com","source_user":"dbtrail","source_password":"`+secret+`"}`); rec.Code != 200 {
		t.Fatalf("PUT: code=%d body=%s", rec.Code, body)
	} else if strings.Contains(string(body), secret) {
		t.Errorf("the PUT answer echoes the password: %s", body)
	}
	assertClean("after a PUT that sent one")
}

// The Connect screen asks for no name (#1953): the server is named after its
// address when the check saves it. A draft never stores a name nobody typed,
// so none can come back later as if somebody had.
func TestConnectDraftStoresNoAutomaticName(t *testing.T) {
	srv, _ := newSupervisorServer(t)
	rec, body := doServersReq(t, srv, "PUT", "/api/servers/draft",
		`{"source_host":"DB.Example.COM","source_port":"3307","source_user":"u"}`)
	if rec.Code != 200 {
		t.Fatalf("PUT: code=%d body=%s", rec.Code, body)
	}
	d, _, _ := srv.drafts.Load()
	if d.Name != "" {
		t.Errorf("a name was stored that nobody typed: %q", d.Name)
	}
}

// The name the check gives is made unique against the servers already
// registered, not the bare name from the host.
func TestConnectCheckNamesTheServerUniquely(t *testing.T) {
	srv, ctrl := newSupervisorServer(t)
	if _, err := srv.cm.reg.Add(ServerEntry{Name: "db-3307", DSN: "u:p@tcp(h:3306)/d", SourceDSN: "u:p@tcp(db:3307)/"}); err != nil {
		t.Fatal(err)
	}
	ctrl.report = &DoctorReport{Failed: 1, Checks: []DoctorCheck{{Name: "x", Status: "fail"}}}
	_, cbody := doServersReq(t, srv, "POST", "/api/servers/check", `{"source_host":"db","source_port":"3307","source_user":"u","source_password":"p"}`)
	if !strings.Contains(string(cbody), `"name":"db-3307-2"`) {
		t.Errorf("the check names the server differently from the suggestion: %s", cbody)
	}
}
