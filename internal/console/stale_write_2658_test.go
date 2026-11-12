package console

import (
	"io"
	"net/http/httptest"
	"strings"
	"testing"
)

// midSave is a request body that makes another save land while the handler
// is reading this one: every handler here reads the server's entry first and
// the body second, so the first Read is the moment between "entry read" and
// "entry written" that two people saving at once fall into.
type midSave struct {
	once func()
	body io.Reader
}

func (m *midSave) Read(p []byte) (int, error) {
	if m.once != nil {
		m.once()
		m.once = nil
	}
	return m.body.Read(p)
}

// Two saves at once never put an old count back (#2658). The count deletes
// local snapshots on a server with an S3 destination too, so a save of
// something else (the schedule, the connection, the monitoring intent) that
// wrote back the entry as it read it would undo "keep them all" and the next
// cleanup would remove snapshots.
func TestSavesNeverPutAStaleCountBack(t *testing.T) {
	send := func(t *testing.T, srv *Server, method, path, body string, meanwhile func()) {
		t.Helper()
		req := httptest.NewRequest(method, "http://127.0.0.1:8090"+path, &midSave{once: meanwhile, body: strings.NewReader(body)})
		req.Host = "127.0.0.1:8090"
		req.Header.Set("Authorization", "Bearer t")
		rec := httptest.NewRecorder()
		srv.Handler().ServeHTTP(rec, req)
		if rec.Code != 200 {
			t.Fatalf("%s %s: code=%d body=%s", method, path, rec.Code, rec.Body.String())
		}
	}
	setup := func(t *testing.T) (*Server, string) {
		srv, id := newScheduleServer(t, &stubScheduleReporter{full: true})
		e, _ := srv.cm.reg.Get(id)
		fakeSnapshot(t, e.BaselineDir)
		e.BaselineS3, e.LocalKeepNewest = "s3://bucket/snaps/", 3
		if err := srv.cm.reg.Update(e); err != nil {
			t.Fatal(err)
		}
		return srv, id
	}
	// keepAll is the other person's save: Keep by count emptied.
	keepAll := func(t *testing.T, srv *Server, id string) func() {
		return func() {
			if rec := putBackupSettings(t, srv, id, `{"keep_newest":0}`); rec.Code != 200 {
				t.Fatalf("the concurrent save: %d %s", rec.Code, rec.Body.String())
			}
		}
	}
	count := func(srv *Server, id string) int {
		e, _ := srv.cm.reg.Get(id)
		return e.LocalKeepNewest
	}

	t.Run("saving the schedule", func(t *testing.T) {
		srv, id := setup(t)
		send(t, srv, "PUT", "/api/servers/"+id+"/backup-schedule", `{"every":"5m"}`, keepAll(t, srv, id))
		e, _ := srv.cm.reg.Get(id)
		if e.LocalKeepNewest != 0 || e.BackupSchedule == nil || e.BackupSchedule.Every != "5m" {
			t.Fatalf("count %d, schedule %+v: want the count the other save set (0) and the schedule this one set", e.LocalKeepNewest, e.BackupSchedule)
		}
	})
	t.Run("removing the schedule", func(t *testing.T) {
		srv, id := setup(t)
		send(t, srv, "PUT", "/api/servers/"+id+"/backup-schedule", `{"every":"5m"}`, nil)
		send(t, srv, "DELETE", "/api/servers/"+id+"/backup-schedule", "", nil)
		// DELETE has no body to read, so the stale read is staged by hand:
		// the entry the handler would hold is the one before keepAll.
		srv2, id2 := setup(t)
		send(t, srv2, "PUT", "/api/servers/"+id2+"/backup-schedule", `{"every":"5m"}`, nil)
		keepAll(t, srv2, id2)()
		send(t, srv2, "DELETE", "/api/servers/"+id2+"/backup-schedule", "", nil)
		if e, _ := srv2.cm.reg.Get(id2); e.LocalKeepNewest != 0 || e.BackupSchedule != nil {
			t.Fatalf("count %d, schedule %+v", e.LocalKeepNewest, e.BackupSchedule)
		}
		_ = srv
		_ = id
	})
	t.Run("saving the connection form", func(t *testing.T) {
		srv, id := setup(t)
		e, _ := srv.cm.reg.Get(id)
		body := `{"name":"wp","host":"127.0.0.1","port":"3306","user":"idx","dbname":"idx","baseline_dir":"` + e.BaselineDir + `","baseline_s3":"s3://bucket/snaps/"}`
		send(t, srv, "PUT", "/api/servers/"+id, body, func() {
			keepAll(t, srv, id)()
			// And a schedule saved meanwhile is not lost either.
			if rec, out := doServersReq(t, srv, "PUT", "/api/servers/"+id+"/backup-schedule", `{"every":"15m"}`); rec.Code != 200 {
				t.Fatalf("the concurrent schedule save: %d %s", rec.Code, out)
			}
		})
		got, _ := srv.cm.reg.Get(id)
		if got.LocalKeepNewest != 0 || got.BackupSchedule == nil || got.BackupSchedule.Every != "15m" {
			t.Fatalf("count %d, schedule %+v: the connection form put back what it read", got.LocalKeepNewest, got.BackupSchedule)
		}
	})
	t.Run("saving the snapshot settings keeps a schedule saved meanwhile", func(t *testing.T) {
		srv, id := setup(t)
		req := httptest.NewRequest("PUT", "/api/backup-settings/servers/"+id, &midSave{body: strings.NewReader(`{"no_archive":true}`), once: func() {
			keepAll(t, srv, id)()
			if rec, out := doServersReq(t, srv, "PUT", "/api/servers/"+id+"/backup-schedule", `{"every":"15m"}`); rec.Code != 200 {
				t.Fatalf("the concurrent schedule save: %d %s", rec.Code, out)
			}
		}})
		req.SetPathValue("id", id)
		rec := httptest.NewRecorder()
		srv.handleBackupSettingsServerUpdate(rec, req)
		if rec.Code != 200 {
			t.Fatalf("PUT settings: %d %s", rec.Code, rec.Body.String())
		}
		got, _ := srv.cm.reg.Get(id)
		if got.LocalKeepNewest != 0 || got.BackupSchedule == nil || !got.NoArchive {
			t.Fatalf("count %d, schedule %+v, no_archive %v: a save that did not mention the count put it back, or lost the schedule", got.LocalKeepNewest, got.BackupSchedule, got.NoArchive)
		}
	})
	t.Run("stopping capture", func(t *testing.T) {
		srv, id := setup(t)
		e, _ := srv.cm.reg.Get(id)
		// The entry as a request read it before the other save.
		stale := e
		keepAll(t, srv, id)()
		if err := srv.startEntry(t.Context(), stale); err != nil {
			t.Fatal(err)
		}
		if n := count(srv, id); n != 0 {
			t.Fatalf("starting capture with an entry read earlier put the count back to %d", n)
		}
		if got, _ := srv.cm.reg.Get(id); !got.MonitorDesired {
			t.Fatal("the intent to capture was not saved")
		}
	})
}
