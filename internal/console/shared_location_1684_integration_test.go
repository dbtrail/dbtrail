//go:build integration

package console

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/testutil"
)

// Edge case 1 of #1684, end to end: two servers that both read the startup
// --baseline-dir are both given it, and once each has written a snapshot
// there, each one's listing carries the #1762 shared-writer warning, naming
// the other. Real index databases (each with its own bintrail_id), the real
// lazy open through the connection manager, the real listing.
//
// The warning comes from snapshot signatures, so it appears once both
// servers have written into the shared folder (a second installation, or
// snapshots from before the upgrade). Within this daemon neither server may
// write there at all: every write path refuses the shared location, which is
// checked here over the same real servers.
func TestSharedStartupLocation_bothServersWarn(t *testing.T) {
	clearStores(t)
	const idA = "aaaaaaaa-0000-0000-0000-00000000000a"
	const idB = "bbbbbbbb-0000-0000-0000-00000000000b"
	index := func(bintrailID string) string {
		db, name := testutil.CreateTestDB(t)
		testutil.MustExec(t, db, "CREATE TABLE stream_state (id INT UNSIGNED PRIMARY KEY, bintrail_id CHAR(36) NULL)")
		testutil.MustExec(t, db, "INSERT INTO stream_state (id, bintrail_id) VALUES (1, ?)", bintrailID)
		return testutil.IntegrationDSN(name)
	}
	dsnA, dsnB := index(idA), index(idB)

	shared := t.TempDir()
	writeBaselineFixture(t, shared, "2026-06-01T00-00-00Z", "shop", "orders.parquet")
	writeBaselineFixture(t, shared, "2026-06-01T00-00-00Z", "_SUCCESS")
	writeBaselineFixture(t, shared, "2026-06-01T00-00-00Z", "_WRITER."+idA)
	writeBaselineFixture(t, shared, "2026-06-02T00-00-00Z", "shop", "orders.parquet")
	writeBaselineFixture(t, shared, "2026-06-02T00-00-00Z", "_SUCCESS")
	writeBaselineFixture(t, shared, "2026-06-02T00-00-00Z", "_WRITER."+idB)

	path := filepath.Join(t.TempDir(), "console-servers.yaml")
	body := "version: 1\nservers:\n" +
		"  - id: aaaaaaaaaaaaaaa1\n    name: a\n    index_dsn: " + dsnA + "\n    source_dsn: " + dsnA + "\n" +
		"  - id: aaaaaaaaaaaaaaa2\n    name: b\n    index_dsn: " + dsnB + "\n    source_dsn: " + dsnB + "\n"
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	reg := loadReg(t, path)
	rep := reg.MigrateProcessBaselineLocation(shared, "")
	if len(rep.Migrated) != 2 {
		t.Fatalf("migrated %v, want both", rep.Migrated)
	}
	ctrl := &stubBaselineCtrl{status: BaselineStatus{State: "idle"}}
	restorer := &stubRestorer{}
	srv, err := New(Config{Listen: "127.0.0.1:8090", Token: "t", Registry: reg, BaselineDir: shared, LocalPruneLoop: true,
		MonitorCtrl: &stubMonitorCtrl{}, BaselineCtrl: ctrl, BaselineRestore: restorer})
	if err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct{ server, own, other string }{
		{"aaaaaaaaaaaaaaa1", idA, idB},
		{"aaaaaaaaaaaaaaa2", idB, idA},
	} {
		rec, raw := doServersReqHeader(t, srv, "GET", "/api/baselines", "", tc.server)
		if rec.Code != 200 {
			t.Fatalf("%s: code = %d, body = %s", tc.server, rec.Code, raw)
		}
		var got baselinesResponse
		if err := json.Unmarshal(raw, &got); err != nil {
			t.Fatal(err)
		}
		want := []snapshotWritersDTO{{Source: shared, Own: tc.own, Others: []string{tc.other}}}
		if !reflect.DeepEqual(got.SharedWith, want) {
			t.Errorf("%s: shared_with = %+v, want %+v", tc.server, got.SharedWith, want)
		}
	}

	// The writes: refused for both, each naming the other and the fix.
	for _, tc := range []struct{ server, other string }{{"aaaaaaaaaaaaaaa1", "b"}, {"aaaaaaaaaaaaaaa2", "a"}} {
		for _, req := range []struct{ path, body string }{
			{"/api/servers/" + tc.server + "/baseline", ""},
			{"/api/servers/" + tc.server + "/baseline/restore", `{"at":"2026-06-02 00:00:00"}`},
		} {
			rec, raw := doServersReq(t, srv, "POST", req.path, req.body)
			if rec.Code != 409 || !strings.Contains(string(raw), "with "+tc.other+",") || !strings.Contains(string(raw), "give this server its own folder or prefix") {
				t.Errorf("POST %s: code=%d body=%s; want 409 naming %s and the fix", req.path, rec.Code, raw, tc.other)
			}
		}
		e, _ := reg.Get(tc.server)
		if err := CheckBackupSchedule(e, BackupSchedule{Every: "1d", At: "03:00"}, srv.scheduleGates()); err == nil {
			t.Errorf("%s: a schedule on the shared folder is runnable", tc.server)
		}
	}
	if len(ctrl.triggered) != 0 || restorer.last != nil {
		t.Errorf("a write started: dumps=%+v restore=%+v", ctrl.triggered, restorer.last)
	}

	dto := backupSettingsGet(t, srv)
	for _, s := range dto.Servers {
		if s.Source != backupSourceServer || s.BaselineDir != shared || !s.KeepBlocked {
			t.Errorf("%s: source=%q dir=%q keep_blocked=%v; want its own location, the shared folder, and pruning held off",
				s.Name, s.Source, s.BaselineDir, s.KeepBlocked)
		}
	}
}
