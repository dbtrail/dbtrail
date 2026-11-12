package console

import (
	"strings"
	"testing"
	"time"
)

// The SQL port answers from the last snapshot by default, so a new server is
// offered a snapshot every DefaultBackupEvery: it starts once the operator
// takes the first snapshot, never on its own (the first one reads the whole
// source), and only where its copies stay bounded on disk. With an S3
// destination too since #2658, sending there once a day.
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
	t.Run("an S3 server is marked too, and starts every 5m sending to S3 once a day", func(t *testing.T) {
		rep := &stubScheduleReporter{full: true}
		srv, added := create(t, rep, "s3://b/p/")
		got, _ := srv.cm.reg.Get(added.ID)
		if !got.DefaultScheduleOnFirst || got.BackupSchedule != nil || got.LocalKeepNewest != DefaultLocalKeepNewest {
			t.Fatalf("created: mark %v, schedule %+v, count %d", got.DefaultScheduleOnFirst, got.BackupSchedule, got.LocalKeepNewest)
		}
		srv.ActivateDefaultSchedule(added.ID)
		got, _ = srv.cm.reg.Get(added.ID)
		if got.BackupSchedule == nil || got.BackupSchedule.Every != "5m" || got.BackupSchedule.UploadEvery != "1d" || got.DefaultScheduleOnFirst {
			t.Fatalf("schedule %+v, mark %v; want every 5m, sent to S3 every 1d", got.BackupSchedule, got.DefaultScheduleOnFirst)
		}
		if len(rep.observed) != 1 {
			t.Errorf("loop told %v", rep.observed)
		}
		// The schedule it started is one the page's own checks accept.
		if err := CheckBackupSchedule(got, *got.BackupSchedule, srv.scheduleGates()); err != nil {
			t.Errorf("the default schedule of a server with S3 does not pass the schedule check: %v", err)
		}
		if p, err := got.BackupSchedule.Parse(); err != nil || p.UploadEvery != 24*time.Hour {
			t.Errorf("parsed %+v err %v, want an upload interval of 24h", p, err)
		}
		t.Log(defaultScheduleStartedMsg(*got.BackupSchedule))
		if msg := defaultScheduleStartedMsg(*got.BackupSchedule); !strings.Contains(msg, "a snapshot every 5m starts now, and one is sent to S3 every 1d; change it on the Snapshots page") {
			t.Errorf("the daemon's line: %q", msg)
		}
	})
	t.Run("without S3 the default schedule has no upload interval", func(t *testing.T) {
		srv, added := create(t, &stubScheduleReporter{full: true}, "")
		srv.ActivateDefaultSchedule(added.ID)
		got, _ := srv.cm.reg.Get(added.ID)
		if got.BackupSchedule == nil || got.BackupSchedule.UploadEvery != "" {
			t.Fatalf("schedule %+v, want no upload interval", got.BackupSchedule)
		}
		t.Log(defaultScheduleStartedMsg(*got.BackupSchedule))
		if msg := defaultScheduleStartedMsg(*got.BackupSchedule); strings.Contains(msg, "S3") {
			t.Errorf("the daemon's line names S3 for a server without it: %q", msg)
		}
	})
	t.Run("the upload interval a new schedule starts with", func(t *testing.T) {
		_, added := create(t, &stubScheduleReporter{full: true}, "")
		if got := defaultUploadEvery(added, DefaultBackupEvery); got != "" {
			t.Fatalf("no S3: default upload interval %q", got)
		}
		added.BaselineS3 = "s3://b/later/"
		if got := defaultUploadEvery(added, DefaultBackupEvery); got != "1d" {
			t.Fatalf("with S3: default upload interval %q, want 1d", got)
		}
		if got := defaultUploadEvery(added, "2d"); got != "" {
			t.Fatalf("every 2d: default upload interval %q, want none (each run is rarer than a day)", got)
		}
		if got := defaultUploadEvery(added, "1d"); got != "1d" {
			t.Fatalf("every 1d: default upload interval %q, want 1d", got)
		}
		if got := defaultUploadEvery(added, "garbage"); got != "" {
			t.Fatalf("an unreadable every: default upload interval %q", got)
		}
	})
	t.Run("a bucket set between the check and the write: no schedule for the old location", func(t *testing.T) {
		// The check ran on the entry without a bucket; the settings saved one
		// before the write. The schedule checked (no upload interval) is not
		// written over a location it was not checked for. The mark is
		// cleared all the same: it is a one-time offer.
		rep := &stubScheduleReporter{full: true}
		srv, added := create(t, rep, "")
		rep.onGates = func() {
			cur, _ := srv.cm.reg.Get(added.ID)
			cur.BaselineS3 = "s3://b/later/"
			if err := srv.cm.reg.Update(cur); err != nil {
				t.Fatal(err)
			}
		}
		srv.ActivateDefaultSchedule(added.ID)
		got, _ := srv.cm.reg.Get(added.ID)
		if got.BackupSchedule != nil || got.DefaultScheduleOnFirst || len(rep.observed) != 0 {
			t.Fatalf("schedule %+v, mark %v, loop told %v: want none, written for a location that changed", got.BackupSchedule, got.DefaultScheduleOnFirst, rep.observed)
		}
	})
	t.Run("an existing server with S3 is never given one", func(t *testing.T) {
		// Saved before the change: no mark (an S3 server never got one) and,
		// read by this build, no count.
		clearStores(t)
		path := writeRegistryFile(t, "version: 1\nservers:\n  - id: aaaaaaaaaaaaaaa1\n    name: old\n    index_dsn: u:p@tcp(h:3306)/one\n    baseline_dir: "+t.TempDir()+"\n    baseline_s3: s3://bucket/one/\n    local_keep_newest: 3\n")
		reg := loadReg(t, path)
		rep := &stubScheduleReporter{full: true}
		srv, err := New(Config{Listen: "127.0.0.1:8090", Token: "t", Registry: reg, BackupSchedules: rep, LocalPruneLoop: true, MayCreateFolders: true})
		if err != nil {
			t.Fatal(err)
		}
		srv.ActivateDefaultSchedule("aaaaaaaaaaaaaaa1")
		got, _ := srv.cm.reg.Get("aaaaaaaaaaaaaaa1")
		if got.BackupSchedule != nil || got.DefaultScheduleOnFirst || got.LocalKeepNewest != 0 || len(rep.observed) != 0 {
			t.Fatalf("an existing server with S3: schedule %+v, mark %v, count %d, loop told %v", got.BackupSchedule, got.DefaultScheduleOnFirst, got.LocalKeepNewest, rep.observed)
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
