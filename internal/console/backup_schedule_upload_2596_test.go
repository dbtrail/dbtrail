package console

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// #2596: send to S3 on its own interval.

func TestBackupSchedule_ParseUploadEvery(t *testing.T) {
	cases := []struct {
		every, upload string
		want          time.Duration
		err           string
	}{
		{every: "5m", upload: "", want: 0},
		{every: "5m", upload: "1h", want: time.Hour},
		{every: "5m", upload: " 1h ", want: time.Hour},
		{every: "5m", upload: "5m", want: 5 * time.Minute},
		{every: "1h", upload: "1d", want: 24 * time.Hour},
		{every: "1h", upload: "30m", err: "send to S3 every: 30m is more often than the schedule runs (every 1h)"},
		{every: "5m", upload: "1H", err: "send to S3 every:"},
		{every: "5m", upload: "1 h", err: "send to S3 every:"},
		{every: "5m", upload: "soon", err: "send to S3 every:"},
		{every: "5m", upload: "0m", err: "send to S3 every:"},
	}
	for _, c := range cases {
		p, err := BackupSchedule{Every: c.every, UploadEvery: c.upload}.Parse()
		if c.err != "" {
			if err == nil || !strings.Contains(err.Error(), c.err) {
				t.Errorf("every %q upload %q: err = %v, want %q", c.every, c.upload, err, c.err)
			}
			continue
		}
		if err != nil || p.UploadEvery != c.want {
			t.Errorf("every %q upload %q: got %v, %v; want %v", c.every, c.upload, p.UploadEvery, err, c.want)
		}
	}
}

func TestBackupSchedule_uploadEveryIsStoredButNotIdentity(t *testing.T) {
	n, err := BackupSchedule{Every: "5m", UploadEvery: " 1h "}.Normalized()
	if err != nil {
		t.Fatal(err)
	}
	if n.UploadEvery != "1h" {
		t.Fatalf("normalized upload_every = %q, want 1h", n.UploadEvery)
	}
	// Changing where the snapshots go does not move when they are made: the
	// loop treats a new identity as a new schedule.
	if a, b := (BackupSchedule{Every: "5m"}).Identity(), n.Identity(); a != b {
		t.Fatalf("identity moved with the upload interval: %q vs %q", a, b)
	}
}

func TestParsedBackupSchedule_UploadDue(t *testing.T) {
	p, err := BackupSchedule{Every: "5m", At: "00:10", UploadEvery: "1h"}.Parse()
	if err != nil {
		t.Fatal(err)
	}
	at := func(hhmm string) time.Time {
		v, err := time.Parse(time.RFC3339, "2026-11-01T"+hhmm+":00Z")
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	cases := []struct {
		name         string
		run, newest  string // newest "" = the bucket holds none
		bucketListed bool
		want         bool
	}{
		{"empty bucket: send", "10:15", "", true, true},
		{"bucket not listed: send", "10:15", "09:15", false, true},
		{"already sent in this slot: hold", "10:15", "10:10", true, false},
		{"sent at the slot itself: hold", "10:55", "10:10", true, false},
		{"the run AT the slot: send", "11:10", "10:10", true, true},
		{"a slot was crossed since: send", "11:15", "10:55", true, true},
		// The grid, not the gap: sent at 10:15, a run at 11:10 is the next
		// slot even though less than an hour has passed. Measured as a gap
		// it would slip to 11:15, then 12:20, and so on.
		{"no drift: next slot sends", "11:10", "10:15", true, true},
		{"bucket newer than the run (a full read landed): hold", "10:15", "10:20", true, false},
	}
	for _, c := range cases {
		var newest time.Time
		if c.newest != "" {
			newest = at(c.newest)
		}
		if got := p.UploadDue(at(c.run), newest, c.bucketListed); got != c.want {
			t.Errorf("%s: UploadDue = %v, want %v", c.name, got, c.want)
		}
	}
	// No interval: every run sends, whatever the bucket holds.
	none, _ := BackupSchedule{Every: "5m"}.Parse()
	if !none.UploadDue(at("10:15"), at("10:14"), true) {
		t.Fatal("without an interval a run held its upload")
	}
}

func TestBackupScheduleAPI_uploadEvery(t *testing.T) {
	rep := &stubScheduleReporter{full: true}
	srv, id := newScheduleServer(t, rep)
	path := "/api/servers/" + id + "/backup-schedule"
	e, _ := srv.cm.reg.Get(id)
	fakeSnapshot(t, e.BaselineDir)
	e.BaselineS3 = "s3://bucket/snaps/"
	if err := srv.cm.reg.Update(e); err != nil {
		t.Fatal(err)
	}

	rec, body := doServersReq(t, srv, "PUT", path, `{"every":"5m","upload_every":"1h"}`)
	if rec.Code != 200 {
		t.Fatalf("PUT code=%d body=%s", rec.Code, body)
	}
	if got := scheduleOf(t, body); got.UploadEvery != "1h" {
		t.Fatalf("PUT response upload_every = %q", got.UploadEvery)
	}
	if e, _ = srv.cm.reg.Get(id); e.BackupSchedule == nil || e.BackupSchedule.UploadEvery != "1h" {
		t.Fatalf("stored = %+v", e.BackupSchedule)
	}
	// Saved without the field (the page does not send it yet): kept.
	if rec, body = doServersReq(t, srv, "PUT", path, `{"every":"10m"}`); rec.Code != 200 {
		t.Fatalf("PUT code=%d body=%s", rec.Code, body)
	}
	if e, _ = srv.cm.reg.Get(id); e.BackupSchedule.UploadEvery != "1h" || e.BackupSchedule.Every != "10m" {
		t.Fatalf("a save that did not mention the upload interval changed it: %+v", e.BackupSchedule)
	}
	// Saved empty: each update sends again.
	if rec, body = doServersReq(t, srv, "PUT", path, `{"every":"10m","upload_every":""}`); rec.Code != 200 {
		t.Fatalf("PUT code=%d body=%s", rec.Code, body)
	}
	if e, _ = srv.cm.reg.Get(id); e.BackupSchedule.UploadEvery != "" {
		t.Fatalf("cleared upload interval kept %q", e.BackupSchedule.UploadEvery)
	}
	// More often than the runs: refused, nothing saved.
	if rec, body = doServersReq(t, srv, "PUT", path, `{"every":"1h","upload_every":"5m"}`); rec.Code != 400 ||
		!strings.Contains(string(body), "more often than the schedule runs") {
		t.Fatalf("upload more often than runs: code=%d body=%s", rec.Code, body)
	}
	if e, _ = srv.cm.reg.Get(id); e.BackupSchedule.Every != "10m" {
		t.Fatal("a refused schedule was saved")
	}

	// A server with no S3 destination has nothing to send: refused with the
	// reason rather than saved as a setting that does nothing.
	e.BaselineS3 = ""
	if err := srv.cm.reg.Update(e); err != nil {
		t.Fatal(err)
	}
	if rec, body = doServersReq(t, srv, "PUT", path, `{"every":"10m","upload_every":"1h"}`); rec.Code != 400 ||
		!strings.Contains(string(body), "no S3 destination") {
		t.Fatalf("upload interval without a destination: code=%d body=%s", rec.Code, body)
	}
}

// The method choice anchors on the snapshot the update will fold from. With
// the upload interval the bucket's newest ages by up to a whole interval
// while held snapshots on this machine stay fresh, and the fold reads the
// newer local one (consoleapp resolveFoldSource). Anchored on the bucket, a
// server with every 15m and upload_every 6h would have its index doubted,
// and production read in full, two hours after each upload whenever nothing
// was indexed since that upload (the probe below: no events, the source not
// asked), while the snapshot it really updates from is minutes old.
func TestChooseBackupMethod_anchorsOnTheHeldLocalSnapshot(t *testing.T) {
	now := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	bucketAt := now.Add(-5 * time.Hour)
	heldAt := now.Add(-15 * time.Minute)
	dir := t.TempDir()
	heldName := reconstruct.SnapshotDirName(heldAt)
	real := newestSnapshot
	t.Cleanup(func() { newestSnapshot = real })
	newestSnapshot = func(_ context.Context, src string) (time.Time, []string, error) {
		if strings.HasPrefix(src, "s3://") {
			return bucketAt, []string{"shop.orders"}, nil
		}
		return heldAt, []string{"shop.orders"}, nil
	}
	var anchors []time.Time
	probe := func(_ context.Context, _ ServerEntry, anchor time.Time) BackupWindow {
		anchors = append(anchors, anchor)
		return BackupWindow{Anchor: anchor, Events: 0}
	}
	gates := BackupScheduleGates{LoopRunning: true, FullBackups: true, Window: probe}
	e := ServerEntry{DSN: "idx", SourceDSN: "src", BaselineDir: dir, BaselineS3: "s3://bucket/snaps/",
		BackupSchedule: &BackupSchedule{Every: "15m", UploadEvery: "6h"}}

	// Not held: the bucket stays the anchor, as before (a newer local copy
	// that is not held is a pending or failed upload, not this feature).
	method, _, err := ChooseBackupMethodAt(context.Background(), e, gates, now)
	if err != nil || method != BackupMethodFull || !anchors[0].Equal(bucketAt) {
		t.Fatalf("unheld: method=%q err=%v anchor=%v, want the bucket's 5h-old anchor and a full read", method, err, anchors)
	}
	if err := baseline.MarkUploadHeld(dir, heldName); err != nil {
		t.Fatal(err)
	}
	method, why, err := ChooseBackupMethodAt(context.Background(), e, gates, now)
	if err != nil || method != BackupMethodRefresh || !anchors[1].Equal(heldAt) {
		t.Fatalf("held: method=%q why=%q err=%v anchor=%v, want an update anchored on the held snapshot", method, why, err, anchors[1])
	}
}

// A server that lost its S3 destination keeps its saved upload interval as
// an inert value, and the page no longer shows the row that sets it. A save
// that does not mention the interval must not be refused for it: the inert
// value that the new update interval left too short is dropped, and the rest
// is saved. With the destination still there, the same save is refused, so
// a script that omits the field does not lose a live setting unasked.
func TestBackupScheduleAPI_anInertUploadIntervalDoesNotBlockASave(t *testing.T) {
	srv, id := newScheduleServer(t, &stubScheduleReporter{full: true})
	path := "/api/servers/" + id + "/backup-schedule"
	e, _ := srv.cm.reg.Get(id)
	fakeSnapshot(t, e.BaselineDir)
	e.BaselineS3 = "s3://bucket/snaps/"
	if err := srv.cm.reg.Update(e); err != nil {
		t.Fatal(err)
	}
	if rec, body := doServersReq(t, srv, "PUT", path, `{"every":"15m","upload_every":"1h"}`); rec.Code != 200 {
		t.Fatalf("PUT code=%d body=%s", rec.Code, body)
	}
	// Live destination: the omitted interval is kept, and too short is refused.
	if rec, body := doServersReq(t, srv, "PUT", path, `{"every":"6h"}`); rec.Code != 400 ||
		!strings.Contains(string(body), "more often than the schedule runs") {
		t.Fatalf("live destination: code=%d body=%s", rec.Code, body)
	}
	e, _ = srv.cm.reg.Get(id)
	e.BaselineS3 = ""
	if err := srv.cm.reg.Update(e); err != nil {
		t.Fatal(err)
	}
	// Inert: a longer update interval still fits? No, 1h < 6h: dropped.
	if rec, body := doServersReq(t, srv, "PUT", path, `{"every":"6h"}`); rec.Code != 200 {
		t.Fatalf("inert interval blocked the save: code=%d body=%s", rec.Code, body)
	}
	if e, _ = srv.cm.reg.Get(id); e.BackupSchedule.Every != "6h" || e.BackupSchedule.UploadEvery != "" {
		t.Fatalf("stored = %+v, want every 6h and the inert 1h dropped", e.BackupSchedule)
	}
	// An inert value that still fits is kept, as before.
	e.BackupSchedule.UploadEvery = "1d"
	if err := srv.cm.reg.Update(e); err != nil {
		t.Fatal(err)
	}
	if rec, body := doServersReq(t, srv, "PUT", path, `{"every":"1h"}`); rec.Code != 200 {
		t.Fatalf("PUT code=%d body=%s", rec.Code, body)
	}
	if e, _ = srv.cm.reg.Get(id); e.BackupSchedule.UploadEvery != "1d" {
		t.Fatalf("an inert interval that still fits was dropped: %+v", e.BackupSchedule)
	}
}
