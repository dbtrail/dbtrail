package console

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// newBackupSettingsServer builds a Server over a real registry file with the
// daemon-wide defaults the page reports.
func newBackupSettingsServer(t *testing.T, defaults BackupSettingsDefaults, baselineDir, baselineS3 string) *Server {
	t.Helper()
	clearStores(t)
	reg, err := LoadRegistry(t.TempDir() + "/console-servers.yaml")
	if err != nil {
		t.Fatal(err)
	}
	srv, err := New(Config{
		Listen: "127.0.0.1:8090", Token: "t", Registry: reg,
		BaselineDir: baselineDir, BaselineS3: baselineS3,
		BackupSettingsDefaults: defaults,
	})
	if err != nil {
		t.Fatal(err)
	}
	return srv
}

func backupSettingsGet(t *testing.T, srv *Server) backupSettingsDTO {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/api/backup-settings", nil)
	srv.handleBackupSettingsGet(rec, req)
	if rec.Code != 200 {
		t.Fatalf("GET /api/backup-settings: code = %d, body = %s", rec.Code, rec.Body.String())
	}
	var dto backupSettingsDTO
	if err := json.Unmarshal(rec.Body.Bytes(), &dto); err != nil {
		t.Fatal(err)
	}
	return dto
}

// TestBackupSettings_provenance drives the three source states the page
// exists to distinguish: a server with its own location, one backed by the
// daemon default (the shape that used to render an empty field,
// indistinguishable from unconfigured), and one with nothing.
func TestBackupSettings_provenance(t *testing.T) {
	srv := newBackupSettingsServer(t, BackupSettingsDefaults{}, "/data/baselines", "")
	own, err := srv.cm.reg.Add(ServerEntry{Name: "own", DSN: "u:p@tcp(h:3306)/idx", BaselineDir: "/mine"})
	if err != nil {
		t.Fatal(err)
	}
	backed, err := srv.cm.reg.Add(ServerEntry{Name: "backed", DSN: "u:p@tcp(h:3306)/idx2"})
	if err != nil {
		t.Fatal(err)
	}

	dto := backupSettingsGet(t, srv)
	byID := map[string]backupSettingsServerDTO{}
	for _, s := range dto.Servers {
		byID[s.ID] = s
	}
	if got := byID[own.ID]; got.Source != "server" || got.ResolvedDir != "/mine" {
		t.Errorf("own-location server: source=%q resolved=%q; want server + /mine", got.Source, got.ResolvedDir)
	}
	// The daemon-default arm: raw stays EMPTY (it is the editable half) while
	// resolved carries what findBaseline will actually open.
	if got := byID[backed.ID]; got.Source != "default" || got.BaselineDir != "" || got.ResolvedDir != "/data/baselines" {
		t.Errorf("default-backed server: source=%q raw=%q resolved=%q; want default + \"\" + /data/baselines",
			got.Source, got.BaselineDir, got.ResolvedDir)
	}
}

func TestBackupSettings_noneWhenNothingBacksAServer(t *testing.T) {
	srv := newBackupSettingsServer(t, BackupSettingsDefaults{}, "", "")
	bare, err := srv.cm.reg.Add(ServerEntry{Name: "bare", DSN: "u:p@tcp(h:3306)/idx"})
	if err != nil {
		t.Fatal(err)
	}
	dto := backupSettingsGet(t, srv)
	for _, s := range dto.Servers {
		if s.ID == bare.ID && s.Source != "none" {
			t.Errorf("unbacked server reports source %q; want none", s.Source)
		}
	}
}

// TestBackupSettings_daemonRowsCarryTheirNames pins the contract of the
// read-only half: every row names the exact flag or variable to change, and
// says on the row that a restart is what applies it.
func TestBackupSettings_daemonRowsCarryTheirNames(t *testing.T) {
	srv := newBackupSettingsServer(t, BackupSettingsDefaults{
		BaselineRetain: "7d", RefreshEvery: "6h", LockMode: "ftwrl",
		TriggerOn: true, StagingDir: "/stage", VerifyInterval: "24h", VerifyTables: "shop.orders",
	}, "/data/baselines", "s3://bkt/b/")
	dto := backupSettingsGet(t, srv)
	want := map[string]string{
		"baseline_dir":    "--baseline-dir",
		"baseline_s3":     "--baseline-s3",
		"baseline_retain": "--baseline-retain",
		"refresh_every":   "--baseline-refresh-interval",
		"lock_mode":       "BINTRAIL_CONSOLE_BASELINE_LOCK_MODE",
		"trigger":         "BINTRAIL_CONSOLE_BASELINE_TRIGGER",
		"staging_dir":     "BINTRAIL_CONSOLE_BASELINE_STAGING",
		"verify_interval": "--verify-interval",
		"verify_tables":   "--verify-tables",
	}
	got := map[string]backupSettingRow{}
	for _, r := range dto.Daemon {
		got[r.Key] = r
	}
	for key, cli := range want {
		r, ok := got[key]
		switch {
		case !ok:
			t.Errorf("daemon rows are missing %s; a setting with no row has no provenance", key)
		case r.CLI != cli:
			t.Errorf("%s names %q; want %q — the row's whole job is the exact name to change", key, r.CLI, cli)
		case !r.NeedsRestart:
			t.Errorf("%s does not carry needs_restart; the restart split lives on the row", key)
		}
	}
	if r := got["trigger"]; r.On == nil || !*r.On {
		t.Errorf("trigger row does not report On=true; booleans ride the on field, not a stringly value")
	}
	if r := got["baseline_retain"]; r.Value != "7d" {
		t.Errorf("baseline_retain value = %q; want the verbatim flag value 7d", r.Value)
	}
}

// TestBackupSettings_scheduleRefusalReachesTheWire: a stored schedule that
// cannot run on this process must say so on the row (the schedule reads the
// RAW entry, so this page's own PUT can strand one). The refusal semantics
// live in CheckBackupSchedule's own tests; this pins the wiring — with a
// schedule the field carries the reason, without one it stays empty.
func TestBackupSettings_scheduleRefusalReachesTheWire(t *testing.T) {
	srv := newBackupSettingsServer(t, BackupSettingsDefaults{}, "", "")
	sched, err := srv.cm.reg.Add(ServerEntry{Name: "sch", DSN: "u:p@tcp(h:3306)/idx",
		SourceDSN: "u:p@tcp(s:3306)/", BackupSchedule: &BackupSchedule{Every: "1d"}})
	if err != nil {
		t.Fatal(err)
	}
	plain, err := srv.cm.reg.Add(ServerEntry{Name: "plain", DSN: "u:p@tcp(h:3306)/idx2"})
	if err != nil {
		t.Fatal(err)
	}
	dto := backupSettingsGet(t, srv)
	byID := map[string]backupSettingsServerDTO{}
	for _, s := range dto.Servers {
		byID[s.ID] = s
	}
	if got := byID[sched.ID]; got.ScheduleRefusal == "" {
		t.Error("a schedule this process cannot run carries no refusal; the row promises runs that will not happen")
	}
	if got := byID[plain.ID]; got.ScheduleRefusal != "" {
		t.Errorf("a server with no schedule carries a refusal %q; the field must mean the schedule, not the server", got.ScheduleRefusal)
	}
}

// TestBackupSettings_lockModeRejectionOutranksTheFallback: when the env value
// was rejected, the row must carry the rejection and its consequence, not
// just the fallback default the page would otherwise present as chosen.
func TestBackupSettings_lockModeRejectionOutranksTheFallback(t *testing.T) {
	srv := newBackupSettingsServer(t, BackupSettingsDefaults{
		LockMode: "ftwrl", LockModeErr: `BINTRAIL_CONSOLE_BASELINE_LOCK_MODE: unknown lock mode "nope"`,
	}, "", "")
	dto := backupSettingsGet(t, srv)
	for _, r := range dto.Daemon {
		if r.Key != "lock_mode" {
			if r.Err != "" {
				t.Errorf("row %s carries an err %q; only the rejected row may", r.Key, r.Err)
			}
			continue
		}
		if !strings.Contains(r.Err, `unknown lock mode "nope"`) {
			t.Errorf("lock_mode row err = %q; the rejection never reached the page", r.Err)
		}
		if !strings.Contains(r.Err, "dumps are refused") {
			t.Errorf("lock_mode row err = %q; it does not state the consequence", r.Err)
		}
	}
}

// TestBackupSettings_updatePatchesOnlyTheBackupFields is the PUT's contract:
// pointer semantics, and everything else on the entry survives untouched —
// unlike PUT /api/servers/{id}, which replaces the entry whole.
func TestBackupSettings_updatePatchesOnlyTheBackupFields(t *testing.T) {
	srv := newBackupSettingsServer(t, BackupSettingsDefaults{}, "", "")
	entry, err := srv.cm.reg.Add(ServerEntry{
		Name: "prod", DSN: "u:secret@tcp(h:3306)/idx",
		SourceDSN:      "repl:secret2@tcp(src:3306)/",
		Schemas:        "shop",
		BackupSchedule: &BackupSchedule{Every: "6h", At: "03:00"},
	})
	if err != nil {
		t.Fatal(err)
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("PUT", "/api/backup-settings/servers/"+entry.ID,
		strings.NewReader(`{"baseline_dir":" /data/bl ","no_archive":true}`))
	req.SetPathValue("id", entry.ID)
	srv.handleBackupSettingsServerUpdate(rec, req)
	if rec.Code != 200 {
		t.Fatalf("PUT: code = %d, body = %s", rec.Code, rec.Body.String())
	}
	var dto backupSettingsServerDTO
	if err := json.Unmarshal(rec.Body.Bytes(), &dto); err != nil {
		t.Fatal(err)
	}
	if dto.BaselineDir != "/data/bl" || !dto.NoArchive || dto.Source != "server" {
		t.Errorf("PUT answer: dir=%q no_archive=%v source=%q; want trimmed /data/bl, true, server",
			dto.BaselineDir, dto.NoArchive, dto.Source)
	}

	after, ok := srv.cm.reg.Get(entry.ID)
	if !ok {
		t.Fatal("entry vanished")
	}
	// The omitted pointer keeps the stored value; everything the request never
	// mentions survives. This is the wipe hazard the endpoint exists to avoid.
	if after.BaselineS3 != "" || after.DSN != entry.DSN || after.SourceDSN != entry.SourceDSN ||
		after.Schemas != "shop" || after.BackupSchedule == nil || after.BackupSchedule.Every != "6h" {
		t.Errorf("PUT touched fields it was never sent: %+v", after)
	}
}

func TestBackupSettings_updateRefusals(t *testing.T) {
	srv := newBackupSettingsServer(t, BackupSettingsDefaults{}, "", "")

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("PUT", "/api/backup-settings/servers/"+bootServerID, strings.NewReader(`{}`))
	req.SetPathValue("id", bootServerID)
	srv.handleBackupSettingsServerUpdate(rec, req)
	if rec.Code != http.StatusConflict {
		t.Errorf("boot entry: code = %d, want 409 (it mirrors the daemon's own flags)", rec.Code)
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest("PUT", "/api/backup-settings/servers/nope", strings.NewReader(`{}`))
	req.SetPathValue("id", "nope")
	srv.handleBackupSettingsServerUpdate(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Errorf("unknown id: code = %d, want 404", rec.Code)
	}
}
