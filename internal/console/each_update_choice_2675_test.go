package console

import (
	"net/http"
	"testing"
)

// #2675, point 7: removing an S3 destination and adding one again turned a
// chosen "each update" into the default of once a day, because the registry
// did not record that the empty interval was in force beside a destination.

func TestEachUpdate_survivesADestinationRemovedAndAddedAgain_2675(t *testing.T) {
	// A server sending at each update to s3, set up the way the 2658 tests
	// set one up: the destination first, the schedule after.
	setup := func(t *testing.T, upload string) (*Server, string) {
		t.Helper()
		srv, id := newScheduleServer(t, &stubScheduleReporter{full: true})
		e, _ := srv.cm.reg.Get(id)
		fakeSnapshot(t, e.BaselineDir)
		e.BaselineS3 = "s3://bucket/had/"
		if err := srv.cm.reg.Update(e); err != nil {
			t.Fatal(err)
		}
		e.BackupSchedule = &BackupSchedule{Every: "5m", At: "00:00", UploadEvery: upload}
		if err := srv.cm.reg.Update(e); err != nil {
			t.Fatal(err)
		}
		return srv, id
	}
	sched := func(srv *Server, id string) BackupSchedule {
		e, _ := srv.cm.reg.Get(id)
		return *e.BackupSchedule
	}
	settings := func(t *testing.T, srv *Server, id, body string) {
		t.Helper()
		if rec := putBackupSettings(t, srv, id, body); rec.Code != http.StatusOK {
			t.Fatalf("PUT settings %s: %d %s", body, rec.Code, rec.Body.String())
		}
	}
	form := func(t *testing.T, srv *Server, id, s3 string) {
		t.Helper()
		e, _ := srv.cm.reg.Get(id)
		body := `{"name":"wp","host":"127.0.0.1","port":"3306","user":"idx","dbname":"idx","baseline_dir":"` + e.BaselineDir + `","baseline_s3":"` + s3 + `"}`
		if rec, out := doServersReq(t, srv, "PUT", "/api/servers/"+id, body); rec.Code != 200 {
			t.Fatalf("PUT server: %d %s", rec.Code, out)
		}
	}
	scheduleSave := func(t *testing.T, srv *Server, id, body string) {
		t.Helper()
		if rec, out := doServersReq(t, srv, "PUT", "/api/servers/"+id+"/backup-schedule", body); rec.Code != 200 {
			t.Fatalf("PUT schedule %s: %d %s", body, rec.Code, out)
		}
	}

	t.Run("removed and added again on the Snapshots page", func(t *testing.T) {
		srv, id := setup(t, "")
		settings(t, srv, id, `{"baseline_s3":""}`)
		if s := sched(srv, id); !s.UploadEachUpdate || s.UploadEvery != "" {
			t.Fatalf("after removing the destination: %+v, want each update recorded", s)
		}
		settings(t, srv, id, `{"baseline_s3":"s3://bucket/again/"}`)
		if s := sched(srv, id); s.UploadEvery != "" {
			t.Fatalf("each update became %q when a destination came back", s.UploadEvery)
		}
	})
	t.Run("removed and added again through the server form", func(t *testing.T) {
		srv, id := setup(t, "")
		form(t, srv, id, "")
		form(t, srv, id, "s3://bucket/again/")
		if s := sched(srv, id); s.UploadEvery != "" {
			t.Fatalf("each update became %q", s.UploadEvery)
		}
	})
	t.Run("the schedule's time saved in between keeps the record", func(t *testing.T) {
		srv, id := setup(t, "")
		settings(t, srv, id, `{"baseline_s3":""}`)
		scheduleSave(t, srv, id, `{"every":"15m","at":"00:00"}`)
		if s := sched(srv, id); !s.UploadEachUpdate || s.Every != "15m" {
			t.Fatalf("after a schedule save: %+v", s)
		}
		scheduleSave(t, srv, id, `{"every":"15m","at":"01:00","upload_every":""}`)
		settings(t, srv, id, `{"baseline_s3":"s3://bucket/again/"}`)
		if s := sched(srv, id); s.UploadEvery != "" {
			t.Fatalf("each update became %q", s.UploadEvery)
		}
	})
	t.Run("an interval chosen later ends the record", func(t *testing.T) {
		srv, id := setup(t, "")
		settings(t, srv, id, `{"baseline_s3":""}`)
		settings(t, srv, id, `{"baseline_s3":"s3://bucket/again/"}`)
		scheduleSave(t, srv, id, `{"every":"5m","at":"00:00","upload_every":"6h"}`)
		if s := sched(srv, id); s.UploadEachUpdate || s.UploadEvery != "6h" {
			t.Fatalf("after choosing 6h: %+v", s)
		}
	})
	t.Run("an interval is kept across removal, as before", func(t *testing.T) {
		srv, id := setup(t, "1h")
		settings(t, srv, id, `{"baseline_s3":""}`)
		if s := sched(srv, id); s.UploadEachUpdate {
			t.Fatalf("an interval recorded as each update: %+v", s)
		}
		settings(t, srv, id, `{"baseline_s3":"s3://bucket/again/"}`)
		if s := sched(srv, id); s.UploadEvery != "1h" {
			t.Fatalf("the interval became %q", s.UploadEvery)
		}
	})
	t.Run("a server that never had a destination still starts the day", func(t *testing.T) {
		srv, id := newScheduleServer(t, &stubScheduleReporter{full: true})
		e, _ := srv.cm.reg.Get(id)
		fakeSnapshot(t, e.BaselineDir)
		e.BackupSchedule = &BackupSchedule{Every: "5m", At: "00:00"}
		if err := srv.cm.reg.Update(e); err != nil {
			t.Fatal(err)
		}
		settings(t, srv, id, `{"baseline_s3":"s3://bucket/first/"}`)
		if s := sched(srv, id); s.UploadEvery != DefaultUploadEvery || s.UploadEachUpdate {
			t.Fatalf("a first destination: %+v, want %s", s, DefaultUploadEvery)
		}
	})
}

func TestEachUpdate_normalizedDropsTheRecordBesideAnInterval_2675(t *testing.T) {
	for _, c := range []struct {
		in   BackupSchedule
		want bool
	}{
		{BackupSchedule{Every: "5m", UploadEachUpdate: true}, true},
		{BackupSchedule{Every: "5m", UploadEvery: "1h", UploadEachUpdate: true}, false},
		{BackupSchedule{Every: "5m", UploadEvery: "  ", UploadEachUpdate: true}, true},
		{BackupSchedule{Every: "5m"}, false},
	} {
		got, err := c.in.Normalized()
		if err != nil {
			t.Fatal(err)
		}
		if got.UploadEachUpdate != c.want {
			t.Errorf("%+v normalized to %+v, want the record %v", c.in, got, c.want)
		}
	}
}
