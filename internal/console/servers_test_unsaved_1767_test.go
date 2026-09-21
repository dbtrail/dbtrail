package console

import (
	"encoding/json"
	"strings"
	"testing"
)

// unsavedTestBody is what the add-server form sends for a new monitored
// source: the index fields empty, the source typed.
const unsavedTestBody = `{"name":"shop-db","flavor":"mysql","host":"","port":"","user":"","dbname":"",` +
	`"source_host":"db.example","source_port":"23306","source_user":"dbtrail","source_password":"s3cret-pw",` +
	`"schemas":"shop","archive_s3":"","s3_endpoint":"","s3_path_style":"","s3_region":"","s3_access_key_id":""}`

// TestServersTest_unsavedServerRunsTheSourceChecks (#1767): Test on a new
// server answered "nothing to test", because it only ever built a DSN from the
// index fields, which a monitor-first install leaves empty. It now runs the
// source half of the startup checks Save runs, on the source as typed.
func TestServersTest_unsavedServerRunsTheSourceChecks(t *testing.T) {
	srv, ctrl := newSupervisorServer(t)
	ctrl.report = &DoctorReport{Failed: 1, Passed: 3, Checks: []DoctorCheck{
		{Name: "binlog_format=ROW", Status: "fail", Detail: "STATEMENT"},
	}}
	rec, raw := doServersReq(t, srv, "POST", "/api/servers/test", unsavedTestBody)
	if rec.Code != 200 {
		t.Fatalf("status %d: %s", rec.Code, raw)
	}
	var got testResponse
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	if got.Doctor == nil || len(got.Doctor.Checks) != 1 || got.OK {
		t.Fatalf("want the startup checks with ok=false for a failing check, got %s", raw)
	}
	if len(ctrl.unsaved) != 1 {
		t.Fatalf("DoctorUnsaved ran %d times, want once", len(ctrl.unsaved))
	}
	e := ctrl.unsaved[0]
	if !strings.Contains(e.SourceDSN, "dbtrail:s3cret-pw@tcp(db.example:23306)/") || e.Schemas != "shop" || e.Flavor != FlavorMySQL {
		t.Errorf("the checks ran on the wrong source: dsn=%q schemas=%q flavor=%q", e.SourceDSN, e.Schemas, e.Flavor)
	}
	if e.DSN != "" || e.ID != "" {
		t.Errorf("an unsaved server has no index and no id, got dsn=%q id=%q", e.DSN, e.ID)
	}
	if strings.Contains(string(raw), "s3cret-pw") {
		t.Error("the response carries the typed password")
	}
	if ids := srv.cm.reg.List(); len(ids) != 0 {
		t.Errorf("Test saved %d entries; it must save nothing", len(ids))
	}

	// All checks passing reads as ok.
	ctrl.report = &DoctorReport{Passed: 4}
	rec, raw = doServersReq(t, srv, "POST", "/api/servers/test", unsavedTestBody)
	if err := json.Unmarshal(raw, &got); err != nil || rec.Code != 200 || !got.OK {
		t.Errorf("a clean report must read ok: %d %s", rec.Code, raw)
	}
}

// The branch is narrow on purpose: only a new server, only under a console
// that can capture, only with no index fields typed. Every other case keeps
// the index probe it had.
func TestServersTest_unsavedBranchIsNarrow(t *testing.T) {
	srv, ctrl := newSupervisorServer(t)

	// A form that names its own index keeps the index probe (a dead one here).
	body := strings.Replace(unsavedTestBody, `"host":"","port":"","user":""`, `"host":"127.0.0.1","port":"1","user":"u"`, 1)
	body = strings.Replace(body, `"dbname":""`, `"dbname":"idx"`, 1)
	rec, raw := doServersReq(t, srv, "POST", "/api/servers/test", body)
	if rec.Code != 200 || strings.Contains(string(raw), `"doctor"`) || len(ctrl.unsaved) != 0 {
		t.Errorf("a draft with its own index ran the source checks: %d %s", rec.Code, raw)
	}

	// A saved server's Test keeps testing its index.
	saved, err := srv.cm.reg.Add(ServerEntry{Name: "saved", DSN: "u:p@tcp(127.0.0.1:1)/idx", SourceDSN: "r:p@tcp(db:3306)/"})
	if err != nil {
		t.Fatal(err)
	}
	rec, raw = doServersReq(t, srv, "POST", "/api/servers/"+saved.ID+"/test", `{}`)
	if rec.Code != 200 || strings.Contains(string(raw), `"doctor"`) || len(ctrl.unsaved) != 0 {
		t.Errorf("a saved server's Test ran the unsaved checks: %d %s", rec.Code, raw)
	}

	// A console that cannot capture has no startup checks to run.
	plain := newRegistryServer(t)
	rec, raw = doServersReq(t, plain, "POST", "/api/servers/test", unsavedTestBody)
	if strings.Contains(string(raw), `"doctor"`) {
		t.Errorf("a serve console ran startup checks: %d %s", rec.Code, raw)
	}
}

// What is missing is named in words, before anything connects.
func TestServersTest_unsavedServerSaysWhatIsMissing(t *testing.T) {
	srv, ctrl := newSupervisorServer(t)
	for _, tc := range []struct{ name, body, want string }{
		{"only a name", `{"name":"x","flavor":"mysql","host":"","port":"","user":"","dbname":"","source_host":"","source_port":"","source_user":""}`, "fill in the source host, user and password"},
		{"no user", `{"name":"x","flavor":"mysql","host":"","port":"","user":"","dbname":"","source_host":"db","source_port":"","source_user":""}`, "source user is required"},
		{"no host", `{"name":"x","flavor":"mysql","host":"","port":"","user":"","dbname":"","source_host":"","source_port":"","source_user":"u"}`, "source host is required"},
	} {
		rec, raw := doServersReq(t, srv, "POST", "/api/servers/test", tc.body)
		if rec.Code != 400 || !strings.Contains(string(raw), tc.want) {
			t.Errorf("%s: %d %s, want 400 %q", tc.name, rec.Code, raw, tc.want)
		}
	}
	if len(ctrl.unsaved) != 0 {
		t.Errorf("the checks ran %d times on an incomplete source", len(ctrl.unsaved))
	}
}

// A stray value in the folded index fields (a browser autofilling the index
// password or user) does not turn a new server's Test into an index probe:
// Save would derive the index anyway, and Test decides the same way.
func TestServersTest_autofilledIndexFieldsStillTestTheSource(t *testing.T) {
	srv, ctrl := newSupervisorServer(t)
	body := strings.Replace(unsavedTestBody, `"user":""`, `"user":"autofilled","password":"autofilled-pw"`, 1)
	rec, raw := doServersReq(t, srv, "POST", "/api/servers/test", body)
	if rec.Code != 200 || !strings.Contains(string(raw), `"doctor"`) || len(ctrl.unsaved) != 1 {
		t.Errorf("autofilled index fields made Test probe an index Save would not use: %d %s", rec.Code, raw)
	}
}
