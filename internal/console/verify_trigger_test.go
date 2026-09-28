package console

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/query"
)

// stubVerifyCtrl is a recording VerifyController for unit tests.
type stubVerifyCtrl struct {
	triggered  []VerifyRequest
	err        error
	status     VerifyStatus
	explainErr error
	explain    *VerifyExplanation
}

func (c *stubVerifyCtrl) Trigger(req VerifyRequest) error {
	c.triggered = append(c.triggered, req)
	return c.err
}

func (c *stubVerifyCtrl) Status(string) VerifyStatus { return c.status }

func (c *stubVerifyCtrl) Explain(serverID, schema, table string) (*VerifyExplanation, error) {
	if c.explainErr != nil {
		return nil, c.explainErr
	}
	return c.explain, nil
}

// newVerifyTriggerServer builds a control-plane console with a recording
// verify controller wired in (mirrors newBaselineTriggerServer).
func newVerifyTriggerServer(t *testing.T) (*Server, *stubVerifyCtrl) {
	t.Helper()
	reg, err := LoadRegistry(t.TempDir() + "/console-servers.yaml")
	if err != nil {
		t.Fatal(err)
	}
	ctrl := &stubVerifyCtrl{status: VerifyStatus{State: "idle"}}
	srv, err := New(Config{
		Listen: "127.0.0.1:8090", Token: "t", Registry: reg,
		MonitorCtrl: &stubMonitorCtrl{}, VerifyCtrl: ctrl,
	})
	if err != nil {
		t.Fatal(err)
	}
	return srv, ctrl
}

const verifySecretPW = "s3cr3t-verify-pw"

// doServersReqHeader is doServersReq with an explicit X-Bintrail-Server
// selection header, for capability checks scoped to a non-default server.
func doServersReqHeader(t *testing.T, srv *Server, method, path, body, serverID string) (*httptest.ResponseRecorder, []byte) {
	t.Helper()
	req := httptest.NewRequest(method, "http://127.0.0.1:8090"+path, strings.NewReader(body))
	req.Host = "127.0.0.1:8090"
	req.Header.Set("Authorization", "Bearer t")
	req.Header.Set("X-Bintrail-Server", serverID)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	return rec, rec.Body.Bytes()
}

// addVerifyEntry adds a registry entry with the given source/destination
// configured, returning its generated id.
func addVerifyEntry(t *testing.T, srv *Server, source, baselineS3, baselineDir string) string {
	t.Helper()
	e, err := srv.cm.reg.Add(ServerEntry{
		Name:        "wp",
		DSN:         "idx:idxpw@tcp(127.0.0.1:3306)/binlog_index",
		SourceDSN:   source,
		BaselineS3:  baselineS3,
		BaselineDir: baselineDir,
	})
	if err != nil {
		t.Fatal(err)
	}
	return e.ID
}

// TestCapabilityVerifyTriggerGate: only a daemon that opted in
// (Config.VerifyCtrl) advertises verify_trigger; the standalone read-only
// console never does.
func TestCapabilityVerifyTriggerGate(t *testing.T) {
	for _, enabled := range []bool{true, false} {
		reg, _ := LoadRegistry("")
		cfg := Config{Listen: "127.0.0.1:8090", Token: "t", Registry: reg}
		if enabled {
			cfg.VerifyCtrl = &stubVerifyCtrl{}
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
		if caps.VerifyTrigger != enabled {
			t.Errorf("verify_trigger capability = %v, want %v", caps.VerifyTrigger, enabled)
		}
	}
}

// TestCapabilityVerify_perServerAndRBAC: Verify/VerifyLiveSource reflect the
// SELECTED server's own baseline/source config, and both collapse to false
// under an active RBAC profile even though verify_trigger stays true
// (process-global) — the endpoint, not just the capability, must enforce
// this (see TestVerifyTrigger_rbacBlocked).
func TestCapabilityVerify_perServerAndRBAC(t *testing.T) {
	reg, _ := LoadRegistry(t.TempDir() + "/console-servers.yaml")
	srv, err := New(Config{
		Listen: "127.0.0.1:8090", Token: "t", Registry: reg,
		MonitorCtrl: &stubMonitorCtrl{}, VerifyCtrl: &stubVerifyCtrl{},
	})
	if err != nil {
		t.Fatal(err)
	}
	id := addVerifyEntry(t, srv, "u:p@tcp(10.0.0.5:3306)/", "", "s3://b/baselines/")
	// Force the bundle to resolve without a real connection.
	srv.cm.bundles[id] = &bundle{baselineSrc: "s3://b/baselines/", baselineConfigured: true}

	req := func() (*capabilitiesResponse, int) {
		rec, body := doServersReqHeader(t, srv, "GET", "/api/capabilities", "", id)
		var caps capabilitiesResponse
		if err := json.Unmarshal(body, &caps); err != nil {
			t.Fatal(err)
		}
		return &caps, rec.Code
	}

	caps, code := req()
	if code != 200 {
		t.Fatalf("capabilities: code=%d", code)
	}
	if !caps.Verify {
		t.Error("verify should be true: server has a baseline destination and no active profile")
	}
	if !caps.VerifyLiveSource {
		t.Error("verify_live_source should be true: server has a source DSN")
	}

	// Now with an active RBAC profile: both must collapse to false.
	reg2, _ := LoadRegistry(t.TempDir() + "/console-servers2.yaml")
	srvRBAC, err := New(Config{
		Listen: "127.0.0.1:8090", Token: "t", Registry: reg2,
		MonitorCtrl: &stubMonitorCtrl{}, VerifyCtrl: &stubVerifyCtrl{},
		DenyTables: []query.SchemaTable{{Schema: "a", Table: "b"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	id2 := addVerifyEntry(t, srvRBAC, "u:p@tcp(10.0.0.5:3306)/", "", "s3://b/baselines/")
	srvRBAC.cm.bundles[id2] = &bundle{baselineSrc: "s3://b/baselines/", baselineConfigured: false}
	rec, body := doServersReqHeader(t, srvRBAC, "GET", "/api/capabilities", "", id2)
	if rec.Code != 200 {
		t.Fatalf("capabilities: code=%d body=%s", rec.Code, body)
	}
	var capsRBAC capabilitiesResponse
	if err := json.Unmarshal(body, &capsRBAC); err != nil {
		t.Fatal(err)
	}
	if capsRBAC.Verify || capsRBAC.VerifyLiveSource {
		t.Errorf("an active RBAC profile must disable both verify capabilities, got %+v", capsRBAC)
	}
}

// TestVerifyTrigger_disabledConsole: with no VerifyCtrl wired in, the trigger
// endpoint refuses with 403 even on a control-plane console.
func TestVerifyTrigger_disabledConsole(t *testing.T) {
	reg, _ := LoadRegistry(t.TempDir() + "/console-servers.yaml")
	srv, err := New(Config{Listen: "127.0.0.1:8090", Token: "t", Registry: reg, MonitorCtrl: &stubMonitorCtrl{}})
	if err != nil {
		t.Fatal(err)
	}
	id := addVerifyEntry(t, srv, "u:p@tcp(127.0.0.1:3306)/", "s3://b/baselines/", "")
	rec, _ := doServersReq(t, srv, "POST", "/api/servers/"+id+"/verify", "")
	if rec.Code != 403 {
		t.Fatalf("disabled console: code=%d, want 403", rec.Code)
	}
}

// TestVerifyTrigger_rbacBlocked: an active RBAC profile refuses verify even
// though VerifyCtrl is wired — its engine carries no redaction.
func TestVerifyTrigger_rbacBlocked(t *testing.T) {
	reg, _ := LoadRegistry(t.TempDir() + "/console-servers.yaml")
	ctrl := &stubVerifyCtrl{}
	srv, err := New(Config{
		Listen: "127.0.0.1:8090", Token: "t", Registry: reg,
		MonitorCtrl: &stubMonitorCtrl{}, VerifyCtrl: ctrl,
		RedactColumns: []query.SchemaTableColumn{{Schema: "a", Table: "b", Column: "c"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	id := addVerifyEntry(t, srv, "u:p@tcp(127.0.0.1:3306)/", "s3://b/baselines/", "")
	rec, _ := doServersReq(t, srv, "POST", "/api/servers/"+id+"/verify", "")
	if rec.Code != 403 {
		t.Fatalf("rbac active: code=%d, want 403", rec.Code)
	}
	if len(ctrl.triggered) != 0 {
		t.Fatal("controller must not be triggered while an RBAC profile is active")
	}
}

// TestVerifyTrigger_requiresBaselineDestination: baseline-anchored mode (the
// default) needs a baseline dir/S3 — 400 without one.
func TestVerifyTrigger_requiresBaselineDestination(t *testing.T) {
	srv, ctrl := newVerifyTriggerServer(t)
	id := addVerifyEntry(t, srv, "u:p@tcp(127.0.0.1:3306)/", "", "")
	rec, _ := doServersReq(t, srv, "POST", "/api/servers/"+id+"/verify", "")
	if rec.Code != 400 {
		t.Fatalf("no destination: code=%d, want 400", rec.Code)
	}
	if len(ctrl.triggered) != 0 {
		t.Fatal("controller must not be triggered without a baseline destination")
	}
}

// TestVerifyTrigger_processBaselineFallback (#1684): an entry with no
// baseline of its own does not inherit the process-wide --baseline-dir any
// more, so baseline-anchored verification is refused for it, matching the
// Verify capability derived from the same entry. Once the migration has
// given it that location, the request carries it.
func TestVerifyTrigger_processBaselineFallback(t *testing.T) {
	reg, _ := LoadRegistry(t.TempDir() + "/console-servers.yaml")
	ctrl := &stubVerifyCtrl{status: VerifyStatus{State: "idle"}}
	srv, err := New(Config{
		Listen: "127.0.0.1:8090", Token: "t", Registry: reg,
		MonitorCtrl: &stubMonitorCtrl{}, VerifyCtrl: ctrl,
		BaselineDir: "/var/bintrail/baselines",
	})
	if err != nil {
		t.Fatal(err)
	}
	id := addVerifyEntry(t, srv, "u:p@tcp(127.0.0.1:3306)/", "", "")
	rec, body := doServersReq(t, srv, "POST", "/api/servers/"+id+"/verify", "")
	if rec.Code != 400 || len(ctrl.triggered) != 0 {
		t.Fatalf("no location of its own: code=%d body=%s triggered=%d, want 400 and nothing run", rec.Code, body, len(ctrl.triggered))
	}
	reg.MigrateProcessBaselineLocation("/var/bintrail/baselines", "")
	rec, body = doServersReq(t, srv, "POST", "/api/servers/"+id+"/verify", "")
	if rec.Code != 202 {
		t.Fatalf("migrated: code=%d body=%s, want 202", rec.Code, body)
	}
	if len(ctrl.triggered) != 1 {
		t.Fatalf("triggered %d runs, want 1", len(ctrl.triggered))
	}
	if got := ctrl.triggered[0].BaselineDir; got != "/var/bintrail/baselines" {
		t.Errorf("VerifyRequest.BaselineDir=%q, want the migrated --baseline-dir", got)
	}
}

// TestVerifyTrigger_requiresSourceForLiveSource: live-source mode needs a
// source DSN — 400 without one, even with a baseline destination configured.
func TestVerifyTrigger_requiresSourceForLiveSource(t *testing.T) {
	srv, ctrl := newVerifyTriggerServer(t)
	id := addVerifyEntry(t, srv, "", "s3://b/baselines/", "")
	rec, _ := doServersReq(t, srv, "POST", "/api/servers/"+id+"/verify", `{"mode":"live-source"}`)
	if rec.Code != 400 {
		t.Fatalf("no source: code=%d, want 400", rec.Code)
	}
	if len(ctrl.triggered) != 0 {
		t.Fatal("controller must not be triggered without a source")
	}
}

// TestVerifyTrigger_liveSourceRequiresBaselineDestination: live-source mode
// ALSO needs a baseline destination — internal/verify.VerifyTable still
// reconstructs from baseline + deltas, so without one every table would
// degrade to inconclusive only AFTER a full off-peak read of the live
// table. A source DSN alone must not be enough to trigger it.
func TestVerifyTrigger_liveSourceRequiresBaselineDestination(t *testing.T) {
	srv, ctrl := newVerifyTriggerServer(t)
	id := addVerifyEntry(t, srv, "u:p@tcp(127.0.0.1:3306)/", "", "")
	rec, _ := doServersReq(t, srv, "POST", "/api/servers/"+id+"/verify", `{"mode":"live-source"}`)
	if rec.Code != 400 {
		t.Fatalf("no baseline destination: code=%d, want 400", rec.Code)
	}
	if len(ctrl.triggered) != 0 {
		t.Fatal("controller must not be triggered without a baseline destination, even in live-source mode")
	}
}

// TestVerifyTrigger_unknownMode: an unrecognized mode string is 400, not
// silently defaulted.
func TestVerifyTrigger_unknownMode(t *testing.T) {
	srv, ctrl := newVerifyTriggerServer(t)
	id := addVerifyEntry(t, srv, "u:p@tcp(127.0.0.1:3306)/", "s3://b/baselines/", "")
	rec, _ := doServersReq(t, srv, "POST", "/api/servers/"+id+"/verify", `{"mode":"bogus"}`)
	if rec.Code != 400 {
		t.Fatalf("unknown mode: code=%d, want 400", rec.Code)
	}
	if len(ctrl.triggered) != 0 {
		t.Fatal("controller must not be triggered with an unknown mode")
	}
}

// TestVerifyTrigger_unknownServer: a bad server id is 404, never a trigger.
func TestVerifyTrigger_unknownServer(t *testing.T) {
	srv, _ := newVerifyTriggerServer(t)
	rec, _ := doServersReq(t, srv, "POST", "/api/servers/deadbeef/verify", "")
	if rec.Code != 404 {
		t.Fatalf("unknown server: code=%d, want 404", rec.Code)
	}
}

// addVerifyEntryPG adds a PostgreSQL registry entry (source + baseline set) so
// the flavor-scoped verify branches can be exercised without a live PG.
func addVerifyEntryPG(t *testing.T, srv *Server) string {
	t.Helper()
	e, err := srv.cm.reg.Add(ServerEntry{
		Name:       "pg",
		DSN:        "idx:idxpw@tcp(127.0.0.1:3306)/binlog_index",
		Flavor:     FlavorPostgres,
		SourceDSN:  "postgres://repl:p@pg.prod:5432/appdb",
		BaselineS3: "s3://b/baselines/",
	})
	if err != nil {
		t.Fatal(err)
	}
	return e.ID
}

// TestVerifyTrigger_postgresAcceptsLiveSource: live-source verify has a
// PG-native checksum path since #1024, so a PostgreSQL entry with a source and
// a baseline destination is ACCEPTED (this endpoint used to hard-400 it) and
// the controller receives the request with the source DSN it needs — the
// supervisor routes to the PG engine by the index's recorded flavor.
func TestVerifyTrigger_postgresAcceptsLiveSource(t *testing.T) {
	srv, ctrl := newVerifyTriggerServer(t)
	id := addVerifyEntryPG(t, srv)
	rec, body := doServersReq(t, srv, "POST", "/api/servers/"+id+"/verify", `{"mode":"live-source"}`)
	if rec.Code != 202 {
		t.Fatalf("PG live-source: code=%d, want 202 (body=%s)", rec.Code, body)
	}
	if len(ctrl.triggered) != 1 {
		t.Fatalf("controller triggered %d times, want 1", len(ctrl.triggered))
	}
	req := ctrl.triggered[0]
	if req.Mode != VerifyModeLiveSource {
		t.Errorf("mode = %q, want live-source", req.Mode)
	}
	if req.SourceDSN == "" {
		t.Error("request must carry the source DSN for the live fingerprint")
	}
}

// TestVerifyTrigger_postgresAllowsBaselineAnchored: the refusal above is scoped
// to live-source ONLY. The default mode (baseline-anchored) reads the index, not
// the live source, so it IS supported for PG and reaches the controller — this
// is the parity the refusal must not overreach and break.
func TestVerifyTrigger_postgresAllowsBaselineAnchored(t *testing.T) {
	srv, ctrl := newVerifyTriggerServer(t)
	id := addVerifyEntryPG(t, srv)
	rec, body := doServersReq(t, srv, "POST", "/api/servers/"+id+"/verify", "")
	if rec.Code != 202 {
		t.Fatalf("PG baseline-anchored: code=%d, want 202 (body=%s)", rec.Code, body)
	}
	if len(ctrl.triggered) != 1 {
		t.Fatalf("baseline-anchored PG verify must trigger the controller, got %d", len(ctrl.triggered))
	}
}

// TestVerifyTrigger_happy: a fully-configured entry triggers a default
// (baseline-anchored) run (202), the controller receives the index DSN and
// baseline destination, and the HTTP response never leaks the index password.
func TestVerifyTrigger_happy(t *testing.T) {
	srv, ctrl := newVerifyTriggerServer(t)
	id := addVerifyEntry(t, srv, "u:p@tcp(127.0.0.1:3306)/", "s3://my-bucket/baselines/", "")
	// Overwrite the entry's index DSN with one carrying a distinctive secret.
	e, _ := srv.cm.reg.Get(id)
	e.DSN = "idx:" + verifySecretPW + "@tcp(127.0.0.1:3306)/binlog_index"
	if err := srv.cm.reg.Update(e); err != nil {
		t.Fatal(err)
	}

	rec, body := doServersReq(t, srv, "POST", "/api/servers/"+id+"/verify", "")
	if rec.Code != 202 {
		t.Fatalf("trigger: code=%d body=%s, want 202", rec.Code, body)
	}
	if len(ctrl.triggered) != 1 {
		t.Fatalf("controller triggered %d times, want 1", len(ctrl.triggered))
	}
	got := ctrl.triggered[0]
	if got.Mode != VerifyModeBaselineAnchored {
		t.Errorf("Mode = %q, want the default baseline-anchored", got.Mode)
	}
	if !strings.Contains(got.IndexDSN, verifySecretPW) {
		t.Errorf("IndexDSN = %q, want the entry's index DSN", got.IndexDSN)
	}
	if got.BaselineS3 != "s3://my-bucket/baselines/" {
		t.Errorf("BaselineS3 = %q, want the entry's baseline S3", got.BaselineS3)
	}
	if strings.Contains(string(body), verifySecretPW) {
		t.Fatalf("response leaked the index password: %s", body)
	}
}

// TestVerifyTrigger_alreadyRunning: ErrVerifyRunning from the controller maps
// to 409 Conflict (one verify run at a time per server).
func TestVerifyTrigger_alreadyRunning(t *testing.T) {
	srv, ctrl := newVerifyTriggerServer(t)
	ctrl.err = ErrVerifyRunning
	id := addVerifyEntry(t, srv, "u:p@tcp(127.0.0.1:3306)/", "s3://b/baselines/", "")
	rec, _ := doServersReq(t, srv, "POST", "/api/servers/"+id+"/verify", "")
	if rec.Code != 409 {
		t.Fatalf("already running: code=%d, want 409", rec.Code)
	}
}

// TestVerifyStatus_returnsControllerState: GET reflects the controller's
// accumulated per-table results and summary.
func TestVerifyStatus_returnsControllerState(t *testing.T) {
	srv, ctrl := newVerifyTriggerServer(t)
	ctrl.status = VerifyStatus{
		State: "succeeded", Mode: VerifyModeBaselineAnchored,
		Results: []VerifyTableResult{{Schema: "wp", Table: "posts", Status: "match"}},
		Summary: VerifySummary{Match: 1},
	}
	id := addVerifyEntry(t, srv, "u:p@tcp(127.0.0.1:3306)/", "s3://b/baselines/", "")

	rec, body := doServersReq(t, srv, "GET", "/api/servers/"+id+"/verify", "")
	if rec.Code != 200 {
		t.Fatalf("status: code=%d body=%s, want 200", rec.Code, body)
	}
	var resp struct {
		Verify VerifyStatus `json:"verify"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Verify.State != "succeeded" || resp.Verify.Summary.Match != 1 || len(resp.Verify.Results) != 1 {
		t.Errorf("status = %+v, want the controller's succeeded state", resp.Verify)
	}
}

// TestVerifyExplain_requiresSchemaAndTable: missing schema/table is 400.
func TestVerifyExplain_requiresSchemaAndTable(t *testing.T) {
	srv, _ := newVerifyTriggerServer(t)
	id := addVerifyEntry(t, srv, "u:p@tcp(127.0.0.1:3306)/", "s3://b/baselines/", "")
	rec, _ := doServersReq(t, srv, "GET", "/api/servers/"+id+"/verify/explain", "")
	if rec.Code != 400 {
		t.Fatalf("missing schema/table: code=%d, want 400", rec.Code)
	}
}

// TestVerifyExplain_unavailable: ErrExplainUnavailable maps to 404.
func TestVerifyExplain_unavailable(t *testing.T) {
	srv, ctrl := newVerifyTriggerServer(t)
	ctrl.explainErr = ErrExplainUnavailable
	id := addVerifyEntry(t, srv, "u:p@tcp(127.0.0.1:3306)/", "s3://b/baselines/", "")
	rec, _ := doServersReq(t, srv, "GET", "/api/servers/"+id+"/verify/explain?schema=wp&table=posts", "")
	if rec.Code != 404 {
		t.Fatalf("unavailable: code=%d, want 404", rec.Code)
	}
}

// TestVerifyExplain_running: while the drill-down is being computed the
// handler answers 202 with a plain running state — NOT an error body (#1375).
// A poll tick is a normal state; rendering it as a failure would put a
// spurious "explain failed" in front of an operator who is merely waiting,
// which is the shape this whole change exists to remove.
func TestVerifyExplain_running(t *testing.T) {
	srv, ctrl := newVerifyTriggerServer(t)
	ctrl.explainErr = ErrExplainRunning
	id := addVerifyEntry(t, srv, "u:p@tcp(127.0.0.1:3306)/", "s3://b/baselines/", "")
	rec, body := doServersReq(t, srv, "GET", "/api/servers/"+id+"/verify/explain?schema=wp&table=posts", "")
	if rec.Code != 202 {
		t.Fatalf("running: code=%d body=%s, want 202", rec.Code, body)
	}
	var resp map[string]any
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("body not JSON: %v (%s)", err, body)
	}
	if resp["state"] != "running" {
		t.Errorf("body = %s, want state=running", body)
	}
	if _, isErr := resp["error"]; isErr {
		t.Errorf("202 carries an error field (%s) — a poll tick must not read as a failure", body)
	}
}

// TestVerifyExplain_happy: a successful drill-down round-trips through JSON.
func TestVerifyExplain_happy(t *testing.T) {
	srv, ctrl := newVerifyTriggerServer(t)
	ctrl.explain = &VerifyExplanation{
		Schema: "wp", Table: "posts", Anchor: "mysql-bin.000123:456", Total: 1,
		Diffs: []VerifyRowDiff{{PK: "id=1", Kind: "changed", Cells: []VerifyCellDiff{
			{Column: "title", Recovery: "old", Baseline: "new"},
		}}},
		Rendered: "--- mismatch drill-down ---",
	}
	id := addVerifyEntry(t, srv, "u:p@tcp(127.0.0.1:3306)/", "s3://b/baselines/", "")
	rec, body := doServersReq(t, srv, "GET", "/api/servers/"+id+"/verify/explain?schema=wp&table=posts", "")
	if rec.Code != 200 {
		t.Fatalf("explain: code=%d body=%s, want 200", rec.Code, body)
	}
	var resp struct {
		Explain VerifyExplanation `json:"explain"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Explain.Total != 1 || len(resp.Explain.Diffs) != 1 || resp.Explain.Diffs[0].PK != "id=1" {
		t.Errorf("explain = %+v, want the controller's explanation", resp.Explain)
	}
}
