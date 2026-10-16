package console

import (
	"encoding/json"
	"strings"
	"testing"
)

// stubBaselineCtrl is a recording BaselineController for unit tests.
type stubBaselineCtrl struct {
	triggered []BaselineRequest
	err       error
	status    BaselineStatus
	refresh   BaselineStatus
}

func (c *stubBaselineCtrl) Trigger(req BaselineRequest) error {
	c.triggered = append(c.triggered, req)
	return c.err
}

// RefreshStatus makes the stub usable as a BaselineRefreshReporter too. It
// reports what the state means when set: refreshed once, successfully.
func (c *stubBaselineCtrl) RefreshStatus(string) BaselineStatus { return c.refresh }

func (c *stubBaselineCtrl) Status(string) BaselineStatus { return c.status }

// newBaselineTriggerServer builds a control-plane console with a recording baseline
// controller wired in (mirrors newSupervisorServer for the monitor surface).
func newBaselineTriggerServer(t *testing.T) (*Server, *stubBaselineCtrl) {
	t.Helper()
	reg, err := LoadRegistry(t.TempDir() + "/console-servers.yaml")
	if err != nil {
		t.Fatal(err)
	}
	ctrl := &stubBaselineCtrl{status: BaselineStatus{State: "idle"}}
	srv, err := New(Config{
		Listen: "127.0.0.1:8090", Token: "t", Registry: reg,
		MonitorCtrl: &stubMonitorCtrl{}, BaselineCtrl: ctrl,
	})
	if err != nil {
		t.Fatal(err)
	}
	return srv, ctrl
}

const baselineSecretPW = "s3cr3t-baseline-pw"

// addBaselineEntry adds a registry entry with the given source/destination
// configured, returning its generated id.
func addBaselineEntry(t *testing.T, srv *Server, source, baselineS3, baselineDir, schemas string) string {
	t.Helper()
	e, err := srv.cm.reg.Add(ServerEntry{
		Name:        "wp",
		DSN:         "idx:idxpw@tcp(127.0.0.1:3306)/binlog_index",
		SourceDSN:   source,
		BaselineS3:  baselineS3,
		BaselineDir: baselineDir,
		Schemas:     schemas,
	})
	if err != nil {
		t.Fatal(err)
	}
	return e.ID
}

// TestCapabilityBaselineTriggerGate: only a daemon that opted in (Config.BaselineCtrl)
// advertises baseline_trigger; the standalone read-only console never does.
func TestCapabilityBaselineTriggerGate(t *testing.T) {
	for _, enabled := range []bool{true, false} {
		reg, _ := LoadRegistry("")
		cfg := Config{Listen: "127.0.0.1:8090", Token: "t", Registry: reg}
		if enabled {
			cfg.BaselineCtrl = &stubBaselineCtrl{}
		}
		srv, err := New(cfg)
		if err != nil {
			t.Fatal(err)
		}
		srv.cm.boot = &bundle{}
		rec, body := doServersReq(t, srv, "GET", "/api/capabilities", "")
		if rec.Code != 200 {
			t.Fatalf("capabilities: code=%d body=%s", rec.Code, body)
		}
		var caps capabilitiesResponse
		if err := json.Unmarshal(body, &caps); err != nil {
			t.Fatal(err)
		}
		if caps.BaselineTrigger != enabled {
			t.Errorf("baseline_trigger capability = %v, want %v", caps.BaselineTrigger, enabled)
		}
	}
}

// TestBaselineTrigger_disabledConsole: with no BaselineCtrl wired in, the trigger
// endpoint refuses with 403 even on a control-plane console.
func TestBaselineTrigger_disabledConsole(t *testing.T) {
	reg, _ := LoadRegistry(t.TempDir() + "/console-servers.yaml")
	srv, err := New(Config{Listen: "127.0.0.1:8090", Token: "t", Registry: reg, MonitorCtrl: &stubMonitorCtrl{}})
	if err != nil {
		t.Fatal(err)
	}
	id := addBaselineEntry(t, srv, "u:p@tcp(127.0.0.1:3306)/", "s3://b/baselines/", "/srv/snaps", "")
	rec, _ := doServersReq(t, srv, "POST", "/api/servers/"+id+"/baseline", "")
	if rec.Code != 403 {
		t.Fatalf("disabled console: code=%d, want 403", rec.Code)
	}
}

// TestBaselineTrigger_requiresSource: an entry with a destination but no source
// configured cannot be dumped — 400.
func TestBaselineTrigger_requiresSource(t *testing.T) {
	srv, ctrl := newBaselineTriggerServer(t)
	id := addBaselineEntry(t, srv, "", "s3://b/baselines/", "", "")
	rec, _ := doServersReq(t, srv, "POST", "/api/servers/"+id+"/baseline", "")
	if rec.Code != 400 {
		t.Fatalf("no source: code=%d, want 400", rec.Code)
	}
	if len(ctrl.triggered) != 0 {
		t.Fatalf("controller must not be triggered without a source")
	}
}

// TestBaselineTrigger_requiresDestination: an entry with a source but no baseline
// dir/S3 has nowhere for the snapshot to live — 400.
func TestBaselineTrigger_requiresDestination(t *testing.T) {
	srv, ctrl := newBaselineTriggerServer(t)
	id := addBaselineEntry(t, srv, "u:p@tcp(127.0.0.1:3306)/", "", "", "")
	rec, _ := doServersReq(t, srv, "POST", "/api/servers/"+id+"/baseline", "")
	if rec.Code != 400 {
		t.Fatalf("no destination: code=%d, want 400", rec.Code)
	}
	if len(ctrl.triggered) != 0 {
		t.Fatalf("controller must not be triggered without a destination")
	}
}

// TestBaselineTrigger_requiresALocalFolder: an S3 location alone is no
// place to write since #2431 — 400, naming the folder.
func TestBaselineTrigger_requiresALocalFolder(t *testing.T) {
	srv, ctrl := newBaselineTriggerServer(t)
	id := addBaselineEntry(t, srv, "u:p@tcp(127.0.0.1:3306)/", "s3://b/baselines/", "", "")
	rec, body := doServersReq(t, srv, "POST", "/api/servers/"+id+"/baseline", "")
	if rec.Code != 400 || !strings.Contains(string(body), "no local snapshot folder") {
		t.Fatalf("S3 only: code=%d body=%s, want 400 naming the folder", rec.Code, body)
	}
	if len(ctrl.triggered) != 0 {
		t.Fatalf("controller must not be triggered without a local folder")
	}
}

// TestBaselineTrigger_unknownServer: a bad server id is 404, never a trigger.
func TestBaselineTrigger_unknownServer(t *testing.T) {
	srv, _ := newBaselineTriggerServer(t)
	rec, _ := doServersReq(t, srv, "POST", "/api/servers/deadbeef/baseline", "")
	if rec.Code != 404 {
		t.Fatalf("unknown server: code=%d, want 404", rec.Code)
	}
}

// TestBaselineTrigger_happy: a fully-configured entry triggers a job (202), the
// controller receives the source DSN + S3 destination + parsed schemas, and the
// HTTP response never leaks the source password.
func TestBaselineTrigger_happy(t *testing.T) {
	srv, ctrl := newBaselineTriggerServer(t)
	source := "repl:" + baselineSecretPW + "@tcp(10.0.0.5:3306)/"
	id := addBaselineEntry(t, srv, source, "s3://my-bucket/baselines/", "/srv/snaps", "wordpress, shop")

	rec, body := doServersReq(t, srv, "POST", "/api/servers/"+id+"/baseline", "")
	if rec.Code != 202 {
		t.Fatalf("trigger: code=%d body=%s, want 202", rec.Code, body)
	}
	if len(ctrl.triggered) != 1 {
		t.Fatalf("controller triggered %d times, want 1", len(ctrl.triggered))
	}
	got := ctrl.triggered[0]
	if got.SourceDSN != source {
		t.Errorf("SourceDSN = %q, want the entry's source", got.SourceDSN)
	}
	if got.S3 != "s3://my-bucket/baselines/" {
		t.Errorf("S3 = %q, want the entry's baseline S3", got.S3)
	}
	if len(got.Schemas) != 2 || got.Schemas[0] != "wordpress" || got.Schemas[1] != "shop" {
		t.Errorf("Schemas = %v, want [wordpress shop]", got.Schemas)
	}
	// The source password must never reach the HTTP response.
	if strings.Contains(string(body), baselineSecretPW) {
		t.Fatalf("response leaked the source password: %s", body)
	}
}

// TestBaselineTrigger_alreadyRunning: ErrBaselineRunning from the controller maps
// to 409 Conflict (one baseline at a time per server).
func TestBaselineTrigger_alreadyRunning(t *testing.T) {
	srv, ctrl := newBaselineTriggerServer(t)
	ctrl.err = ErrBaselineRunning
	id := addBaselineEntry(t, srv, "u:p@tcp(127.0.0.1:3306)/", "s3://b/baselines/", "/srv/snaps", "")
	rec, _ := doServersReq(t, srv, "POST", "/api/servers/"+id+"/baseline", "")
	if rec.Code != 409 {
		t.Fatalf("already running: code=%d, want 409", rec.Code)
	}
}

// TestBaselineStatus_returnsControllerState: GET reflects the controller's status.
func TestBaselineStatus_returnsControllerState(t *testing.T) {
	srv, ctrl := newBaselineTriggerServer(t)
	ctrl.status = BaselineStatus{State: "succeeded", Tables: 47, Rows: 33391, Uploaded: 48}
	id := addBaselineEntry(t, srv, "u:p@tcp(127.0.0.1:3306)/", "s3://b/baselines/", "/srv/snaps", "")

	rec, body := doServersReq(t, srv, "GET", "/api/servers/"+id+"/baseline", "")
	if rec.Code != 200 {
		t.Fatalf("status: code=%d body=%s, want 200", rec.Code, body)
	}
	var resp struct {
		Baseline BaselineStatus `json:"baseline"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Baseline.State != "succeeded" || resp.Baseline.Tables != 47 || resp.Baseline.Uploaded != 48 {
		t.Errorf("status = %+v, want the controller's succeeded state", resp.Baseline)
	}
}

// TestSplitSchemas covers the comma-separated parse used to build the request.
// addPGBaselineEntry adds a PostgreSQL registry entry (source + slot/publication
// + a local baseline destination) and returns its generated id.
func addPGBaselineEntry(t *testing.T, srv *Server, slot, publication string) string {
	t.Helper()
	e, err := srv.cm.reg.Add(ServerEntry{
		Name:              "pgwp",
		DSN:               "idx:idxpw@tcp(127.0.0.1:3306)/binlog_index",
		SourceDSN:         "postgres://repl:secret@pg:5432/appdb",
		BaselineDir:       t.TempDir(),
		Flavor:            FlavorPostgres,
		SourceSlot:        slot,
		SourcePublication: publication,
	})
	if err != nil {
		t.Fatal(err)
	}
	return e.ID
}

// TestBaselineTrigger_postgresHappy: a PG entry with a slot + publication triggers
// a baseline whose request carries the flavor/slot/publication the PG producer needs.
func TestBaselineTrigger_postgresHappy(t *testing.T) {
	srv, ctrl := newBaselineTriggerServer(t)
	id := addPGBaselineEntry(t, srv, "bintrail_slot", "bintrail_pub")
	rec, _ := doServersReq(t, srv, "POST", "/api/servers/"+id+"/baseline", "")
	if rec.Code != 202 {
		t.Fatalf("pg happy: code=%d, want 202", rec.Code)
	}
	if len(ctrl.triggered) != 1 {
		t.Fatalf("want 1 triggered request, got %d", len(ctrl.triggered))
	}
	got := ctrl.triggered[0]
	if got.Flavor != FlavorPostgres || got.Slot != "bintrail_slot" || got.Publication != "bintrail_pub" {
		t.Errorf("PG baseline request missing flavor/slot/publication: %+v", got)
	}
}

// TestBaselineTrigger_postgresRequiresSlot: a PG entry with no slot/publication
// cannot be baselined — a clear 400 (not a mydumper "no source" mislabel), and
// the controller is never triggered.
func TestBaselineTrigger_postgresRequiresSlot(t *testing.T) {
	srv, ctrl := newBaselineTriggerServer(t)
	id := addPGBaselineEntry(t, srv, "", "bintrail_pub")
	rec, _ := doServersReq(t, srv, "POST", "/api/servers/"+id+"/baseline", "")
	if rec.Code != 400 {
		t.Fatalf("pg without slot: code=%d, want 400", rec.Code)
	}
	if len(ctrl.triggered) != 0 {
		t.Fatal("controller must not be triggered for a PG entry without a slot")
	}
}

func TestSplitSchemas(t *testing.T) {
	cases := map[string][]string{
		"":              nil,
		"a":             {"a"},
		" a , b ,, c ":  {"a", "b", "c"},
		"wordpress,wp2": {"wordpress", "wp2"},
	}
	for in, want := range cases {
		got := splitSchemas(in)
		if len(got) != len(want) {
			t.Errorf("splitSchemas(%q) = %v, want %v", in, got, want)
			continue
		}
		for i := range want {
			if got[i] != want[i] {
				t.Errorf("splitSchemas(%q)[%d] = %q, want %q", in, i, got[i], want[i])
			}
		}
	}
}

// TestBaselineRefreshDoesNotUnGateTheDumpTrigger pins the split between the two
// baseline features (#1171). They are independently opt-in — a refresh needs no
// mydumper and no BINTRAIL_CONSOLE_BASELINE_TRIGGER=1 — so wiring the refresh
// reporter must not hand the operator a Create-baseline button they never asked
// for and whose dependency may not be installed.
//
// The inverse (a refresh-only daemon refusing to start) was the original defect:
// the refresh used to READ its status off BaselineController, which forced the
// two features to share one opt-in.
func TestBaselineRefreshDoesNotUnGateTheDumpTrigger(t *testing.T) {
	reg, err := LoadRegistry(t.TempDir() + "/console-servers.yaml")
	if err != nil {
		t.Fatal(err)
	}
	srv, err := New(Config{
		Listen: "127.0.0.1:8090", Token: "t", Registry: reg,
		MonitorCtrl: &stubMonitorCtrl{},
		// Refresh reporter ONLY — no BaselineCtrl.
		BaselineRefresh: &stubBaselineCtrl{refresh: BaselineStatus{State: "succeeded", Tables: 3}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if srv.baselineCtrl != nil {
		t.Fatal("wiring BaselineRefresh must not populate baselineCtrl: that is what gates the dump trigger")
	}
	if srv.baselineRefresh == nil {
		t.Fatal("BaselineRefresh was not wired through to the server")
	}

	id := addBaselineEntry(t, srv, "u:p@tcp(127.0.0.1:3306)/", "s3://b/baselines/", "/srv/snaps", "")
	rec, _ := doServersReq(t, srv, "POST", "/api/servers/"+id+"/baseline", "")
	if rec.Code != 403 {
		t.Fatalf("baseline trigger with refresh-only wiring = %d, want 403 (the mydumper dump was never enabled)", rec.Code)
	}

	srv.cm.boot = &bundle{}
	rec, body := doServersReq(t, srv, "GET", "/api/capabilities", "")
	if rec.Code != 200 {
		t.Fatalf("capabilities: code=%d body=%s", rec.Code, body)
	}
	var caps capabilitiesResponse
	if err := json.Unmarshal(body, &caps); err != nil {
		t.Fatal(err)
	}
	if caps.BaselineTrigger {
		t.Error("baseline_trigger capability advertised on a refresh-only daemon")
	}
}
