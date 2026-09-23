package console

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/ext"
)

// POST /api/servers/check is the one call behind the Connect step: it runs the
// startup checks against the database as typed and, only if they all pass,
// saves the server and starts capturing. The defect it exists to close is the
// old two-step — a failed check used to leave a saved, stopped server behind.

func decodeCheck(t *testing.T, body []byte) connectCheckResponse {
	t.Helper()
	var out connectCheckResponse
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("decode %s: %v", body, err)
	}
	return out
}

const checkBody = `{"source_host":"db.example.com","source_user":"dbtrail","source_password":"Ab3-xyz"}`

func TestCheckStartsAndSavesWhenEverythingPasses(t *testing.T) {
	srv, ctrl := newSupervisorServer(t)
	rec, body := doServersReq(t, srv, "POST", "/api/servers/check", checkBody)
	if rec.Code != 200 {
		t.Fatalf("code=%d body=%s", rec.Code, body)
	}
	got := decodeCheck(t, body)
	if !got.OK || !got.Started {
		t.Fatalf("ok=%v started=%v, want both true: %s", got.OK, got.Started, body)
	}
	if got.Server == nil || got.Server.ID == "" {
		t.Fatalf("no server in the answer: %s", body)
	}
	if srv.cm.reg.Len() != 1 {
		t.Errorf("registry holds %d entries, want 1", srv.cm.reg.Len())
	}
	if len(ctrl.started) != 1 {
		t.Errorf("Start was called %d times, want 1", len(ctrl.started))
	}
	// The checks ran against the source as typed, before anything was saved.
	if len(ctrl.unsaved) != 1 {
		t.Fatalf("the unsaved checks ran %d times, want 1", len(ctrl.unsaved))
	}
	if !strings.Contains(ctrl.unsaved[0].SourceDSN, "db.example.com") {
		t.Errorf("the checks ran against %q", ctrl.unsaved[0].SourceDSN)
	}
}

// The name nobody typed comes from the address of the database.
func TestCheckNamesTheServerAfterItsHost(t *testing.T) {
	srv, _ := newSupervisorServer(t)
	_, body := doServersReq(t, srv, "POST", "/api/servers/check", checkBody)
	got := decodeCheck(t, body)
	if got.Name != "db.example.com" || got.Server == nil || got.Server.Name != "db.example.com" {
		t.Fatalf("name=%q server=%+v, want db.example.com", got.Name, got.Server)
	}
	// A second database on the same host gets a name of its own.
	_, body = doServersReq(t, srv, "POST", "/api/servers/check", checkBody)
	second := decodeCheck(t, body)
	if second.Name != "db.example.com-2" {
		t.Errorf("the second server is named %q, want db.example.com-2", second.Name)
	}
}

func TestCheckKeepsATypedName(t *testing.T) {
	srv, _ := newSupervisorServer(t)
	_, body := doServersReq(t, srv, "POST", "/api/servers/check",
		`{"name":"orders","source_host":"db.example.com","source_user":"u","source_password":"p"}`)
	if got := decodeCheck(t, body); got.Name != "orders" {
		t.Errorf("name=%q, want orders", got.Name)
	}
}

// The defect this endpoint closes: a failed check saves NOTHING.
func TestCheckThatFailsSavesNoServer(t *testing.T) {
	srv, ctrl := newSupervisorServer(t)
	ctrl.report = &DoctorReport{Failed: 1, Checks: []DoctorCheck{{
		Name: "Every table has a PRIMARY KEY", Status: "fail", Kind: "no_primary_key",
		Subjects:   []string{"shop.carts"},
		Statements: []string{"ALTER TABLE `shop`.`carts` ADD COLUMN id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY FIRST;"},
	}}}
	rec, body := doServersReq(t, srv, "POST", "/api/servers/check", checkBody)
	if rec.Code != 200 {
		t.Fatalf("a failed check is a result, not a transport error: code=%d body=%s", rec.Code, body)
	}
	got := decodeCheck(t, body)
	if got.OK || got.Started {
		t.Errorf("ok=%v started=%v, want both false", got.OK, got.Started)
	}
	if srv.cm.reg.Len() != 0 {
		t.Errorf("a failed check left %d servers behind", srv.cm.reg.Len())
	}
	if len(ctrl.started) != 0 {
		t.Errorf("Start was called after a failed check")
	}
	// The typed finding reaches the answer, which is what the screen draws.
	if got.Doctor == nil || len(got.Doctor.Checks) != 1 {
		t.Fatalf("no checks in the answer: %s", body)
	}
	c := got.Doctor.Checks[0]
	if c.Kind != "no_primary_key" || len(c.Subjects) != 1 || len(c.Statements) != 1 {
		t.Errorf("the typed finding did not survive the round trip: %+v", c)
	}
}

// Warnings are not failures: capture starts and carries them.
func TestCheckStartsWithWarnings(t *testing.T) {
	srv, ctrl := newSupervisorServer(t)
	ctrl.report = &DoctorReport{Passed: 1, Warnings: 1, Checks: []DoctorCheck{
		{Name: "ok", Status: "pass"},
		{Name: "No FK CASCADE constraints", Status: "warn"},
	}}
	_, body := doServersReq(t, srv, "POST", "/api/servers/check", checkBody)
	got := decodeCheck(t, body)
	if !got.OK || !got.Started {
		t.Fatalf("a warning blocked the start: %s", body)
	}
	if got.Doctor == nil || got.Doctor.Warnings != 1 {
		t.Errorf("the warning did not reach the answer: %s", body)
	}
}

// Checks passed, the stream refused to launch: the server that was created a
// moment ago must be gone again, because the person will press the button once
// more and a leftover would be refused as a duplicate name.
func TestCheckRollsBackWhenCaptureCannotStart(t *testing.T) {
	srv, ctrl := newSupervisorServer(t)
	ctrl.startErr = errors.New("index server refused the connection")
	rec, body := doServersReq(t, srv, "POST", "/api/servers/check", checkBody)
	if rec.Code != 200 {
		t.Fatalf("code=%d body=%s", rec.Code, body)
	}
	got := decodeCheck(t, body)
	if got.OK || got.Started {
		t.Errorf("ok=%v started=%v, want both false", got.OK, got.Started)
	}
	if got.Kept {
		t.Errorf("kept=true, but the entry was removable: %s", body)
	}
	if srv.cm.reg.Len() != 0 {
		t.Fatalf("a failed start left %d servers behind", srv.cm.reg.Len())
	}
	if got.Error == "" || !strings.Contains(got.Error, "index server refused") {
		t.Errorf("the answer does not say why the start failed: %q", got.Error)
	}
	// And pressing it again works, rather than colliding with the leftover.
	rec, body = doServersReq(t, srv, "POST", "/api/servers/check", checkBody)
	ctrl.startErr = nil
	if rec.Code != 200 {
		t.Fatalf("retry: code=%d body=%s", rec.Code, body)
	}
	rec, body = doServersReq(t, srv, "POST", "/api/servers/check", checkBody)
	if got := decodeCheck(t, body); !got.Started {
		t.Errorf("the retry did not start: %s", body)
	}
}

// If the rollback itself fails there IS a saved, stopped server, and saying
// "nothing happened" would be the same lie the two-step told. The answer must
// say the server is still there.
func TestCheckSaysSoWhenTheRollbackFails(t *testing.T) {
	srv, ctrl := newSupervisorServer(t)
	ctrl.startErr = errors.New("no")
	srv.cm.reg.readOnly = true // every registry write is refused from here on
	rec, body := doServersReq(t, srv, "POST", "/api/servers/check", checkBody)
	if rec.Code != 200 && rec.Code != 409 {
		t.Fatalf("code=%d body=%s", rec.Code, body)
	}
	// A read-only registry refuses the Add too, so nothing was created and
	// there is nothing to keep. This case is about the answer NEVER claiming a
	// clean rollback it did not do: with the entry created and Delete refused,
	// kept must be true. Drive that directly.
	srv2, ctrl2 := newSupervisorServer(t)
	ctrl2.startErr = errors.New("no")
	e, err := srv2.cm.reg.Add(ServerEntry{Name: "x", DSN: "u:p@tcp(h:3306)/d", SourceDSN: "u:p@tcp(db:3306)/"})
	if err != nil {
		t.Fatal(err)
	}
	srv2.cm.reg.readOnly = true
	res := srv2.startNewEntry(t.Context(), e)
	if res.Started {
		t.Fatal("the stub start was supposed to fail")
	}
	if !res.Kept {
		t.Errorf("kept=false although the entry could not be removed; registry still holds %d", srv2.cm.reg.Len())
	}
	if srv2.cm.reg.Len() != 1 {
		t.Errorf("registry holds %d entries, want the one that could not be removed", srv2.cm.reg.Len())
	}
}

// Two very different things can go wrong between "the checks passed" and
// "capture is running", and they answer differently: the registry refusing the
// write that records the intent, and the stream refusing to launch. They are
// told apart by a wrapped sentinel rather than by reading the error's text,
// which is what makes the second case safe: a launch failure whose message
// happens to contain a word the registry mapping looks for ("required") must
// still be reported as a failure of this server, not as a bad request.
func TestStartTellsARegistryRefusalFromALaunchFailure(t *testing.T) {
	srv, ctrl := newSupervisorServer(t)
	e, err := srv.cm.reg.Add(ServerEntry{Name: "x", DSN: "u:p@tcp(h:3306)/d", SourceDSN: "u:p@tcp(db:3306)/"})
	if err != nil {
		t.Fatal(err)
	}
	ctrl.startErr = errors.New("a replication user is required on the database")
	rec, body := doServersReq(t, srv, "POST", "/api/servers/"+e.ID+"/monitor/start", "{}")
	if rec.Code != 500 {
		t.Errorf("a launch failure answered %d (%s), want 500", rec.Code, body)
	}
	if !strings.Contains(string(body), "start monitoring") {
		t.Errorf("the answer does not say the launch is what failed: %s", body)
	}

	// The other side: the registry refusing to record the intent is its own
	// refusal and keeps the registry's status.
	srv2, ctrl2 := newSupervisorServer(t)
	e2, err := srv2.cm.reg.Add(ServerEntry{Name: "y", DSN: "u:p@tcp(h:3306)/d", SourceDSN: "u:p@tcp(db:3306)/"})
	if err != nil {
		t.Fatal(err)
	}
	srv2.cm.reg.readOnly = true
	rec, body = doServersReq(t, srv2, "POST", "/api/servers/"+e2.ID+"/monitor/start", "{}")
	if rec.Code != 409 {
		t.Errorf("a refused registry write answered %d (%s), want 409", rec.Code, body)
	}
	if len(ctrl2.started) != 0 {
		t.Error("the stream was launched although the intent was never recorded")
	}
}

func TestCheckRefusesOnAConsoleThatCannotCapture(t *testing.T) {
	srv := newRegistryServer(t) // no MonitorCtrl
	if rec, body := doServersReq(t, srv, "POST", "/api/servers/check", checkBody); rec.Code != 403 {
		t.Errorf("code=%d body=%s, want 403", rec.Code, body)
	}
}

func TestCheckRefusesAnEmptySource(t *testing.T) {
	srv, _ := newSupervisorServer(t)
	for _, body := range []string{`{}`, `{"source_user":"u","source_password":"p"}`} {
		rec, got := doServersReq(t, srv, "POST", "/api/servers/check", body)
		if rec.Code != 400 {
			t.Errorf("%s: code=%d body=%s, want 400", body, rec.Code, got)
		}
		if srv.cm.reg.Len() != 0 {
			t.Fatalf("%s created a server", body)
		}
	}
}

// A body that describes a complete INDEX and no database to read is the one
// shape that builds a valid entry with nothing to check. It must come back as
// a request to fill in the address — not as the startup checks failing to run,
// which is what asking the supervisor about a sourceless entry produces.
func TestCheckRefusesAnIndexWithNothingToRead(t *testing.T) {
	srv, ctrl := newSupervisorServer(t)
	rec, body := doServersReq(t, srv, "POST", "/api/servers/check",
		`{"name":"idx","host":"h","port":"3306","user":"u","password":"p","dbname":"binlog_index"}`)
	if rec.Code != 400 {
		t.Fatalf("code=%d body=%s, want 400", rec.Code, body)
	}
	if !strings.Contains(string(body), "address") {
		t.Errorf("the refusal does not name the field to fill in: %s", body)
	}
	if len(ctrl.unsaved) != 0 {
		t.Errorf("the checks were asked to run against an entry with nothing to read")
	}
	if srv.cm.reg.Len() != 0 {
		t.Errorf("it created a server")
	}
}

func TestCheckRefusesAMalformedBody(t *testing.T) {
	srv, _ := newSupervisorServer(t)
	rec, body := doServersReq(t, srv, "POST", "/api/servers/check", `{"source_host":`)
	if rec.Code != 400 {
		t.Errorf("code=%d body=%s, want 400", rec.Code, body)
	}
	if _, ok, _ := srv.drafts.Load(); ok {
		t.Error("a body that could not be read was saved as a draft")
	}
}

// The draft is written BEFORE the checks run, because the checks are the slow
// part and a reload in the middle of them is the case it exists for.
func TestCheckSavesTheDraftBeforeItRunsTheChecks(t *testing.T) {
	srv, ctrl := newSupervisorServer(t)
	ctrl.report = &DoctorReport{Failed: 1, Checks: []DoctorCheck{{Name: "x", Status: "fail"}}}
	doServersReq(t, srv, "POST", "/api/servers/check",
		`{"source_host":"db.example.com","source_user":"dbtrail","source_password":"Ab3-xyz"}`)
	d, ok, err := srv.drafts.Load()
	if err != nil || !ok {
		t.Fatalf("no draft after a failed check: (%v, %v)", ok, err)
	}
	if d.SourceHost != "db.example.com" || d.SourcePassword != "Ab3-xyz" {
		t.Errorf("the draft did not keep what was typed: %+v", d)
	}
	// And the name it derived is in the draft too, so the form comes back with
	// the name the person saw.
	if d.Name != "db.example.com" {
		t.Errorf("draft name = %q, want the derived db.example.com", d.Name)
	}
}

// Once capture is running the draft is finished business and must not come
// back the next time somebody opens Connect.
func TestCheckDiscardsTheDraftOnceCaptureStarts(t *testing.T) {
	srv, _ := newSupervisorServer(t)
	if rec, body := doServersReq(t, srv, "POST", "/api/servers/check", checkBody); rec.Code != 200 {
		t.Fatalf("code=%d body=%s", rec.Code, body)
	}
	if _, ok, _ := srv.drafts.Load(); ok {
		t.Error("the draft outlived the server it became")
	}
}

// A start that failed is not finished business: what was typed must still be
// there when the page comes back.
func TestCheckKeepsTheDraftWhenTheStartFails(t *testing.T) {
	srv, ctrl := newSupervisorServer(t)
	ctrl.startErr = errors.New("no")
	doServersReq(t, srv, "POST", "/api/servers/check", checkBody)
	if _, ok, _ := srv.drafts.Load(); !ok {
		t.Error("the draft was discarded although capture never started")
	}
}

// The check route and Save build the entry the same way, so Check cannot pass
// a shape Save would refuse. A PostgreSQL source with no replication slot is
// the cheapest proof: Save refuses it, so Check must too.
func TestCheckRefusesWhatSaveRefuses(t *testing.T) {
	srv, _ := newSupervisorServer(t)
	body := `{"flavor":"postgres","source_host":"pg.example.com","source_user":"u","source_password":"p","source_database":"appdb","source_publication":"pub"}`
	rec, save := doServersReq(t, srv, "POST", "/api/servers", body)
	if rec.Code != 400 {
		t.Fatalf("Save accepted it: code=%d body=%s", rec.Code, save)
	}
	rec, check := doServersReq(t, srv, "POST", "/api/servers/check", body)
	if rec.Code != 400 {
		t.Errorf("Check accepted what Save refused: code=%d body=%s", rec.Code, check)
	}
}

// Save gained the derived name too, so the two paths agree about what a
// nameless server is called.
func TestCreateDerivesTheNameWhenNobodyTypedOne(t *testing.T) {
	srv, _ := newSupervisorServer(t)
	rec, body := doServersReq(t, srv, "POST", "/api/servers",
		`{"source_host":"DB.Example.COM","source_port":"3307","source_user":"u","source_password":"p"}`)
	if rec.Code != 201 {
		t.Fatalf("code=%d body=%s", rec.Code, body)
	}
	var dto serverDTO
	if err := json.Unmarshal(body, &dto); err != nil {
		t.Fatal(err)
	}
	if dto.Name != "db.example.com-3307" {
		t.Errorf("name = %q, want db.example.com-3307", dto.Name)
	}
}

// With no source to derive from, the registry's own rule still holds.
func TestCreateStillNeedsANameWithNoSource(t *testing.T) {
	srv := newRegistryServer(t)
	rec, body := doServersReq(t, srv, "POST", "/api/servers", `{"host":"h","user":"u","dbname":"d"}`)
	if rec.Code != 400 {
		t.Errorf("code=%d body=%s, want 400", rec.Code, body)
	}
}

// The check route is classified with creating a server, never with reading one.
func TestCheckRouteIsWriteTier(t *testing.T) {
	perm, ok := permForRoute("POST", "/api/servers/check")
	if !ok {
		t.Fatal("POST /api/servers/check is not classified")
	}
	// Against the permission itself, never against the constant the route is
	// declared with — see TestDraftRoutesAreWriteTier.
	if perm != ext.PermServersWrite {
		t.Errorf("POST /api/servers/check requires %q, want %q", perm, ext.PermServersWrite)
	}
}
