package console

import (
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// #2675, points 3 and 4: decided under the registry's lock, and written
// field by field. The folder check ("this folder already holds snapshots")
// is asked of the folder and the count the write leaves; the connection form
// writes only what it changed; stopping capture writes only its own field.

// lockFixture is a server with no count, its own folder holding one
// snapshot, and a second folder b with one snapshot of its own.
func lockFixture(t *testing.T) (*Server, string, string) {
	t.Helper()
	srv, id := newScheduleServer(t, &stubScheduleReporter{full: true})
	e, _ := srv.cm.reg.Get(id)
	fakeSnapshot(t, e.BaselineDir)
	e.LocalKeepNewest = 0
	if err := srv.cm.reg.Update(e); err != nil {
		t.Fatal(err)
	}
	b := filepath.Join(t.TempDir(), "b")
	fakeSnapshot(t, b)
	return srv, id, b
}

// settingsWhile sends the snapshot settings save body, with meanwhile run
// between the handler's read of the entry and its write.
func settingsWhile(t *testing.T, srv *Server, id, body string, meanwhile func()) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest("PUT", "/api/backup-settings/servers/"+id, &midSave{once: meanwhile, body: strings.NewReader(body)})
	req.SetPathValue("id", id)
	rec := httptest.NewRecorder()
	srv.handleBackupSettingsServerUpdate(rec, req)
	return rec
}

// formWhile sends the connection form's PUT, with meanwhile run the same way.
func formWhile(t *testing.T, srv *Server, id, body string, meanwhile func()) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest("PUT", "http://127.0.0.1:8090/api/servers/"+id, &midSave{once: meanwhile, body: strings.NewReader(body)})
	req.Host = "127.0.0.1:8090"
	req.Header.Set("Authorization", "Bearer t")
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	return rec
}

func mustOK(t *testing.T, rec *httptest.ResponseRecorder, what string) {
	t.Helper()
	if rec.Code != 200 {
		t.Fatalf("%s: %d %s", what, rec.Code, rec.Body.String())
	}
}

// emptyDir is a folder that exists and holds no snapshot.
func emptyDir(t *testing.T, name string) string {
	t.Helper()
	d := filepath.Join(t.TempDir(), name)
	if err := os.MkdirAll(d, 0o755); err != nil {
		t.Fatal(err)
	}
	return d
}

func formBody(name, dir string) string {
	return `{"name":"` + name + `","host":"127.0.0.1","port":"3306","user":"idx","dbname":"idx","baseline_dir":"` + dir + `"}`
}

func TestSettingsSave_theFolderCheckIsDecidedUnderTheLock_2675(t *testing.T) {
	t.Run("the folder moved while another save raised the count: refused", func(t *testing.T) {
		srv, id, b := lockFixture(t)
		was, _ := srv.cm.reg.Get(id)
		rec := settingsWhile(t, srv, id, `{"baseline_dir":"`+b+`"}`, func() {
			mustOK(t, putBackupSettings(t, srv, id, `{"keep_newest":3}`), "the other save")
		})
		t.Logf("refusal: %s", rec.Body.String())
		if rec.Code != 400 || !strings.Contains(rec.Body.String(), "already holds 1 snapshot, and with Keep the newest at 3") {
			t.Fatalf("%d %s, want the folder refused with the count the write would leave", rec.Code, rec.Body.String())
		}
		if e, _ := srv.cm.reg.Get(id); e.BaselineDir != was.BaselineDir || e.LocalKeepNewest != 3 {
			t.Fatalf("folder %q count %d, want the folder unchanged and the other save's count", e.BaselineDir, e.LocalKeepNewest)
		}
	})
	t.Run("the count raised while another save moved the folder to one with snapshots: refused", func(t *testing.T) {
		srv, id, b := lockFixture(t)
		rec := settingsWhile(t, srv, id, `{"keep_newest":3}`, func() {
			mustOK(t, putBackupSettings(t, srv, id, `{"baseline_dir":"`+b+`"}`), "the other save")
		})
		if rec.Code != 400 || !strings.Contains(rec.Body.String(), "already holds 1 snapshot") {
			t.Fatalf("%d %s, want the count refused on the folder this request did not see", rec.Code, rec.Body.String())
		}
		if e, _ := srv.cm.reg.Get(id); e.BaselineDir != b || e.LocalKeepNewest != 0 {
			t.Fatalf("folder %q count %d, want the other save's folder and no count", e.BaselineDir, e.LocalKeepNewest)
		}
	})
	t.Run("the folder moved while another save raised the count, and the folder is empty: saved", func(t *testing.T) {
		srv, id, _ := lockFixture(t)
		empty := emptyDir(t, "empty")
		mustOK(t, settingsWhile(t, srv, id, `{"baseline_dir":"`+empty+`"}`, func() {
			mustOK(t, putBackupSettings(t, srv, id, `{"keep_newest":3}`), "the other save")
		}), "the move")
		if e, _ := srv.cm.reg.Get(id); e.BaselineDir != empty || e.LocalKeepNewest != 3 {
			t.Fatalf("folder %q count %d", e.BaselineDir, e.LocalKeepNewest)
		}
	})
	t.Run("a count on the folder the request saw is not asked about, as before", func(t *testing.T) {
		srv, id, _ := lockFixture(t)
		mustOK(t, settingsWhile(t, srv, id, `{"keep_newest":3}`, nil), "the count")
		if e, _ := srv.cm.reg.Get(id); e.LocalKeepNewest != 3 {
			t.Fatalf("count %d", e.LocalKeepNewest)
		}
	})
	t.Run("a move to the folder another save already moved the entry to: saved", func(t *testing.T) {
		srv, id, _ := lockFixture(t)
		c := emptyDir(t, "c")
		mustOK(t, settingsWhile(t, srv, id, `{"baseline_dir":"`+c+`"}`, func() {
			mustOK(t, putBackupSettings(t, srv, id, `{"baseline_dir":"`+c+`","keep_newest":3}`), "the other save")
			// The server's own snapshot, made there since.
			fakeSnapshot(t, c)
		}), "the second move")
		if e, _ := srv.cm.reg.Get(id); e.BaselineDir != c || e.LocalKeepNewest != 3 {
			t.Fatalf("folder %q count %d", e.BaselineDir, e.LocalKeepNewest)
		}
	})
	t.Run("keep everything: never refused", func(t *testing.T) {
		srv, id, b := lockFixture(t)
		mustOK(t, settingsWhile(t, srv, id, `{"baseline_dir":"`+b+`","keep_newest":0}`, func() {
			mustOK(t, putBackupSettings(t, srv, id, `{"keep_newest":3}`), "the other save")
		}), "the move")
		if e, _ := srv.cm.reg.Get(id); e.BaselineDir != b || e.LocalKeepNewest != 0 {
			t.Fatalf("folder %q count %d", e.BaselineDir, e.LocalKeepNewest)
		}
	})
}

func TestConnectionForm_writesOnlyWhatItChanged_2675(t *testing.T) {
	t.Run("a folder moved meanwhile is not put back", func(t *testing.T) {
		srv, id, _ := lockFixture(t)
		was, _ := srv.cm.reg.Get(id)
		c := emptyDir(t, "c")
		mustOK(t, formWhile(t, srv, id, formBody("renamed", was.BaselineDir), func() {
			mustOK(t, putBackupSettings(t, srv, id, `{"baseline_dir":"`+c+`"}`), "the other save")
		}), "the form")
		if e, _ := srv.cm.reg.Get(id); e.BaselineDir != c || e.Name != "renamed" {
			t.Fatalf("folder %q name %q, want the other save's folder and this form's name", e.BaselineDir, e.Name)
		}
	})
	t.Run("a stopped capture and the SQL mirror's switch are not put back", func(t *testing.T) {
		srv, id, _ := lockFixture(t)
		e, _ := srv.cm.reg.Get(id)
		e.MonitorDesired = true
		if err := srv.cm.reg.Update(e); err != nil {
			t.Fatal(err)
		}
		mustOK(t, formWhile(t, srv, id, formBody("renamed", e.BaselineDir), func() {
			if err := srv.stopIntent(e); err != nil {
				t.Fatal(err)
			}
			mustOK(t, putBackupSettings(t, srv, id, `{"no_sql_mirror":true}`), "the other save")
		}), "the form")
		if got, _ := srv.cm.reg.Get(id); got.MonitorDesired || !got.NoSQLMirror || got.Name != "renamed" {
			t.Fatalf("monitor %v mirror off %v name %q: the form put back what it read", got.MonitorDesired, got.NoSQLMirror, got.Name)
		}
	})
	t.Run("the form moves the folder while another save raised the count: refused", func(t *testing.T) {
		srv, id, b := lockFixture(t)
		was, _ := srv.cm.reg.Get(id)
		rec := formWhile(t, srv, id, formBody(was.Name, b), func() {
			mustOK(t, putBackupSettings(t, srv, id, `{"keep_newest":3}`), "the other save")
		})
		if rec.Code != 400 || !strings.Contains(rec.Body.String(), "already holds 1 snapshot, and with Keep the newest at 3") {
			t.Fatalf("%d %s, want the folder refused with the count it would have", rec.Code, rec.Body.String())
		}
		if e, _ := srv.cm.reg.Get(id); e.BaselineDir != was.BaselineDir || e.LocalKeepNewest != 3 {
			t.Fatalf("folder %q count %d", e.BaselineDir, e.LocalKeepNewest)
		}
	})
	t.Run("a field the form changes is saved", func(t *testing.T) {
		srv, id, _ := lockFixture(t)
		was, _ := srv.cm.reg.Get(id)
		body := `{"name":"wp","host":"127.0.0.1","port":"3306","user":"idx","dbname":"idx","baseline_dir":"` + was.BaselineDir + `","schemas":"shop","archive_s3":"s3://arch/x/"}`
		mustOK(t, formWhile(t, srv, id, body, nil), "the form")
		if e, _ := srv.cm.reg.Get(id); e.Schemas != "shop" || e.ArchiveS3 != "s3://arch/x/" {
			t.Fatalf("schemas %q archive %q", e.Schemas, e.ArchiveS3)
		}
	})
}

// Point 4: stopping capture writes only its own field, with an entry read
// before another save.
func TestStopIntent_writesOnlyItsOwnField_2675(t *testing.T) {
	srv, id, _ := lockFixture(t)
	e, _ := srv.cm.reg.Get(id)
	e.MonitorDesired, e.LocalKeepNewest = true, 3
	if err := srv.cm.reg.Update(e); err != nil {
		t.Fatal(err)
	}
	stale, _ := srv.cm.reg.Get(id)
	mustOK(t, putBackupSettings(t, srv, id, `{"keep_newest":0,"no_sql_mirror":true}`), "the other save")
	if err := srv.stopIntent(stale); err != nil {
		t.Fatal(err)
	}
	got, _ := srv.cm.reg.Get(id)
	if got.MonitorDesired || got.LocalKeepNewest != 0 || !got.NoSQLMirror {
		t.Fatalf("monitor %v count %d mirror off %v: stopping capture wrote back the entry it read", got.MonitorDesired, got.LocalKeepNewest, got.NoSQLMirror)
	}
}
