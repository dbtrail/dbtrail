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

// The automatic name is what the name field shows as its placeholder. It is
// worked out on every answer and never stored, so it can never come back as
// a name somebody typed.
func TestConnectDraftAnswersWithTheAutomaticName(t *testing.T) {
	srv, _ := newSupervisorServer(t)
	rec, body := doServersReq(t, srv, "PUT", "/api/servers/draft",
		`{"source_host":"DB.Example.COM","source_port":"3307","source_user":"u"}`)
	if rec.Code != 200 {
		t.Fatalf("PUT: code=%d body=%s", rec.Code, body)
	}
	if !strings.Contains(string(body), `"auto_name":"db.example.com-3307"`) {
		t.Errorf("PUT answer lacks the automatic name: %s", body)
	}
	_, body = doServersReq(t, srv, "GET", "/api/servers/draft", "")
	if !strings.Contains(string(body), `"auto_name":"db.example.com-3307"`) {
		t.Errorf("GET answer lacks the automatic name: %s", body)
	}
	d, _, _ := srv.drafts.Load()
	if d.Name != "" {
		t.Errorf("the automatic name was stored as typed: %q", d.Name)
	}
	// A typed name wins, and there is then no automatic one to show.
	_, body = doServersReq(t, srv, "PUT", "/api/servers/draft", `{"name":"orders","source_host":"db"}`)
	if strings.Contains(string(body), "auto_name") {
		t.Errorf("a typed name still gets an automatic one: %s", body)
	}
	// No host, no name to work out.
	_, body = doServersReq(t, srv, "PUT", "/api/servers/draft", `{"source_user":"u"}`)
	if strings.Contains(string(body), "auto_name") {
		t.Errorf("no host, yet an automatic name: %s", body)
	}
}
