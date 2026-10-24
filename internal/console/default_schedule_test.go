package console

import (
	"strings"
	"testing"
)

// The SQL port answers from the last snapshot by default, so a new server is
// offered a snapshot every DefaultBackupEvery: it starts once the operator
// takes the first snapshot, never on its own (the first one reads the whole
// source), and only where its copies stay bounded on disk.
func TestDefaultSchedule(t *testing.T) {
	newEntry := func(dir, s3 string) ServerEntry {
		return ServerEntry{Name: "shop", DSN: "idx:pw@tcp(127.0.0.1:3306)/idx", SourceDSN: "src:pw@tcp(127.0.0.1:3306)/",
			BaselineDir: dir, BaselineS3: s3}
	}
	create := func(t *testing.T, rep *stubScheduleReporter, s3 string) (*Server, ServerEntry) {
		t.Helper()
		var srv *Server
		if rep == nil {
			srv, _ = newScheduleServer(t, nil)
		} else {
			srv, _ = newScheduleServer(t, rep)
		}
		srv.mayCreateFolders = true
		added, _, err := srv.persistNewEntry(newEntry(t.TempDir()+"/snaps", s3), false, "shop", newLocalCopy{})
		if err != nil {
			t.Fatal(err)
		}
		return srv, added
	}
	t.Run("created marked, with no schedule: nothing reads the source on its own", func(t *testing.T) {
		rep := &stubScheduleReporter{full: true}
		srv, added := create(t, rep, "")
		got, _ := srv.cm.reg.Get(added.ID)
		if !got.DefaultScheduleOnFirst || got.BackupSchedule != nil || len(rep.observed) != 0 {
			t.Fatalf("mark %v, schedule %+v, observed %v", got.DefaultScheduleOnFirst, got.BackupSchedule, rep.observed)
		}
	})
	t.Run("the first snapshot starts every 5m, once", func(t *testing.T) {
		rep := &stubScheduleReporter{full: true}
		srv, added := create(t, rep, "")
		srv.ActivateDefaultSchedule(added.ID)
		got, _ := srv.cm.reg.Get(added.ID)
		if got.BackupSchedule == nil || got.BackupSchedule.Every != "5m" || got.DefaultScheduleOnFirst {
			t.Fatalf("schedule %+v, mark %v", got.BackupSchedule, got.DefaultScheduleOnFirst)
		}
		if len(rep.observed) != 1 || !strings.HasPrefix(rep.observed[0], added.ID+" ") {
			t.Errorf("loop told %v", rep.observed)
		}
		// Turned off afterwards: the next snapshot does not bring it back.
		got.BackupSchedule = nil
		if err := srv.cm.reg.Update(got); err != nil {
			t.Fatal(err)
		}
		srv.ActivateDefaultSchedule(added.ID)
		if again, _ := srv.cm.reg.Get(added.ID); again.BackupSchedule != nil || len(rep.observed) != 1 {
			t.Errorf("a later snapshot started it again: %+v", again.BackupSchedule)
		}
	})
	t.Run("an S3 server is not marked: its copies are not bounded", func(t *testing.T) {
		srv, added := create(t, &stubScheduleReporter{full: true}, "s3://b/p/")
		srv.ActivateDefaultSchedule(added.ID)
		if got, _ := srv.cm.reg.Get(added.ID); got.DefaultScheduleOnFirst || got.BackupSchedule != nil {
			t.Fatalf("mark %v, schedule %+v", got.DefaultScheduleOnFirst, got.BackupSchedule)
		}
	})
	t.Run("a schedule saved or removed before the first snapshot wins", func(t *testing.T) {
		rep := &stubScheduleReporter{full: true}
		srv, added := create(t, rep, "")
		if rec, body := doServersReq(t, srv, "PUT", "/api/servers/"+added.ID+"/backup-schedule", `{"every":"1d"}`); rec.Code != 200 {
			t.Fatalf("PUT: %d %s", rec.Code, body)
		}
		srv.ActivateDefaultSchedule(added.ID)
		if got, _ := srv.cm.reg.Get(added.ID); got.BackupSchedule == nil || got.BackupSchedule.Every != "1d" || got.DefaultScheduleOnFirst {
			t.Fatalf("chosen 1d became %+v (mark %v)", got.BackupSchedule, got.DefaultScheduleOnFirst)
		}
		srv2, added2 := create(t, rep, "")
		if rec, body := doServersReq(t, srv2, "DELETE", "/api/servers/"+added2.ID+"/backup-schedule", ""); rec.Code != 200 {
			t.Fatalf("DELETE: %d %s", rec.Code, body)
		}
		srv2.ActivateDefaultSchedule(added2.ID)
		if got, _ := srv2.cm.reg.Get(added2.ID); got.BackupSchedule != nil || got.DefaultScheduleOnFirst {
			t.Fatalf("turned off, then started: %+v (mark %v)", got.BackupSchedule, got.DefaultScheduleOnFirst)
		}
	})
	t.Run("a schedule that cannot run: none, mark cleared", func(t *testing.T) {
		srv, added := create(t, nil, "")
		srv.ActivateDefaultSchedule(added.ID)
		if got, _ := srv.cm.reg.Get(added.ID); got.BackupSchedule != nil || got.DefaultScheduleOnFirst {
			t.Fatalf("schedule %+v, mark %v", got.BackupSchedule, got.DefaultScheduleOnFirst)
		}
	})
	t.Run("an existing server is never given one", func(t *testing.T) {
		rep := &stubScheduleReporter{full: true}
		srv, id := newScheduleServer(t, rep)
		srv.ActivateDefaultSchedule(id)
		if got, _ := srv.cm.reg.Get(id); got.BackupSchedule != nil || len(rep.observed) != 0 {
			t.Fatalf("schedule %+v", got.BackupSchedule)
		}
	})
}

// UpdateWith edits the entry as it is under the lock, and saves only when
// asked.
func TestRegistryUpdateWith(t *testing.T) {
	srv, id := newScheduleServer(t, nil)
	reg := srv.cm.reg
	if err := reg.UpdateWith(id, func(e ServerEntry) (ServerEntry, bool) {
		e.DefaultScheduleOnFirst = true
		return e, false
	}); err != nil {
		t.Fatal(err)
	}
	if got, _ := reg.Get(id); got.DefaultScheduleOnFirst {
		t.Error("saved without being asked")
	}
	if err := reg.UpdateWith(id, func(e ServerEntry) (ServerEntry, bool) {
		e.DefaultScheduleOnFirst = true
		return e, true
	}); err != nil {
		t.Fatal(err)
	}
	if got, _ := reg.Get(id); !got.DefaultScheduleOnFirst {
		t.Error("not saved")
	}
	if err := reg.UpdateWith("nope", func(e ServerEntry) (ServerEntry, bool) { return e, true }); err == nil {
		t.Error("an unknown id was saved")
	}
}
