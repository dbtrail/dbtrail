package console

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// #2596 part 2: the listing says how old the newest S3 copy is. With an upload
// interval the newest snapshot sits on this machine for hours before one
// reaches the bucket, so the newest row of the listing says nothing about S3.

type s3CopyListing struct {
	snaps   []time.Time
	skipped int
	err     error
}

func s3CopyServer(t *testing.T, withS3 bool) (*Server, string, string) {
	t.Helper()
	return s3CopyServerWith(t, nil, withS3)
}

func s3CopyServerWith(t *testing.T, rep *stubScheduleReporter, withS3 bool) (*Server, string, string) {
	t.Helper()
	srv, id := newScheduleServer(t, rep)
	e, _ := srv.cm.reg.Get(id)
	if withS3 {
		e.BaselineS3 = "s3://bkt/snaps"
		if err := srv.cm.reg.Update(e); err != nil {
			t.Fatal(err)
		}
		srv.cm.bundles[id].baselineFallbackSrc = e.BaselineS3
	}
	srv.cm.bundles[id].baselineSrc = e.BaselineDir
	prev := listBaselinesForPage
	t.Cleanup(func() { listBaselinesForPage = prev })
	return srv, id, e.BaselineDir
}

// stubListings answers each location from listings: one orders file per
// snapshot, the skip count, or the error.
func stubListings(listings map[string]s3CopyListing) {
	listBaselinesForPage = func(_ context.Context, source string, _ int) ([]reconstruct.BaselineFile, int, bool, error) {
		l := listings[source]
		if l.err != nil {
			return nil, 0, false, l.err
		}
		var files []reconstruct.BaselineFile
		for _, at := range l.snaps {
			files = append(files, reconstruct.BaselineFile{SnapshotTime: at, Schema: "shop", Table: "orders",
				Path: source + "/" + reconstruct.SnapshotDirName(at) + "/shop/orders.parquet"})
		}
		return files, l.skipped, false, nil
	}
}

type s3CopyResp struct {
	S3Destination bool `json:"s3_destination"`
	S3            *struct {
		Newest         string  `json:"newest"`
		NewestAgeHours float64 `json:"newest_age_hours"`
		Unread         bool    `json:"unread"`
		HeldNewer      bool    `json:"held_newer"`
	} `json:"s3"`
	Snapshots []json.RawMessage `json:"snapshots"`
}

func getS3Copy(t *testing.T, srv *Server, id string) s3CopyResp {
	t.Helper()
	rec, body := doServersReqHeader(t, srv, "GET", "/api/baselines", "", id)
	if rec.Code != 200 {
		t.Fatalf("code=%d body=%s", rec.Code, body)
	}
	var resp s3CopyResp
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatal(err)
	}
	return resp
}

func TestHandleBaselines_s3Copy(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	s3 := "s3://bkt/snaps"

	t.Run("no S3 location: no S3 block and no destination", func(t *testing.T) {
		srv, id, dir := s3CopyServer(t, false)
		stubListings(map[string]s3CopyListing{dir: {snaps: []time.Time{now}}})
		got := getS3Copy(t, srv, id)
		if got.S3 != nil || got.S3Destination {
			t.Fatalf("s3=%+v destination=%v, want neither on a server with no S3 location", got.S3, got.S3Destination)
		}
	})

	t.Run("an empty bucket is read and says nothing is there", func(t *testing.T) {
		srv, id, dir := s3CopyServer(t, true)
		stubListings(map[string]s3CopyListing{dir: {snaps: []time.Time{now}}, s3: {}})
		got := getS3Copy(t, srv, id)
		if !got.S3Destination || got.S3 == nil || got.S3.Newest != "" || got.S3.Unread {
			t.Fatalf("destination=%v s3=%+v, want a read, empty S3 block", got.S3Destination, got.S3)
		}
	})

	t.Run("a bucket that cannot be listed gives no age", func(t *testing.T) {
		srv, id, dir := s3CopyServer(t, true)
		stubListings(map[string]s3CopyListing{dir: {snaps: []time.Time{now}}, s3: {err: errors.New("AccessDenied")}})
		got := getS3Copy(t, srv, id)
		if got.S3 == nil || !got.S3.Unread || got.S3.Newest != "" {
			t.Fatalf("s3=%+v, want unread with no newest", got.S3)
		}
	})

	t.Run("a bucket listed in part gives no age", func(t *testing.T) {
		srv, id, dir := s3CopyServer(t, true)
		stubListings(map[string]s3CopyListing{dir: {snaps: []time.Time{now}}, s3: {snaps: []time.Time{now.Add(-time.Hour)}, skipped: 1}})
		got := getS3Copy(t, srv, id)
		if got.S3 == nil || !got.S3.Unread || got.S3.Newest != "" {
			t.Fatalf("s3=%+v, want unread: a skipped folder may be the newest", got.S3)
		}
	})

	t.Run("the S3 copy is found past the rows the page is sent", func(t *testing.T) {
		// Every 5 minutes, sent every 6 hours: 72 local snapshots are newer
		// than the bucket's newest, and the page gets 50.
		var local []time.Time
		for i := range 72 {
			local = append(local, now.Add(-time.Duration(i)*5*time.Minute))
		}
		s3At := now.Add(-6 * time.Hour)
		srv, id, dir := s3CopyServer(t, true)
		stubListings(map[string]s3CopyListing{dir: {snaps: local}, s3: {snaps: []time.Time{s3At, s3At.Add(-6 * time.Hour)}}})
		got := getS3Copy(t, srv, id)
		if len(got.Snapshots) != baselinesMaxSnapshots {
			t.Fatalf("snapshots=%d, want the page cap %d (the S3 copy must be past it)", len(got.Snapshots), baselinesMaxSnapshots)
		}
		if got.S3 == nil || got.S3.Newest != s3At.Format(consoleTSFormat) {
			t.Fatalf("s3=%+v, want newest %s", got.S3, s3At.Format(consoleTSFormat))
		}
		if got.S3.NewestAgeHours < 5.9 || got.S3.NewestAgeHours > 6.5 {
			t.Fatalf("newest_age_hours=%v, want about 6", got.S3.NewestAgeHours)
		}
	})

	t.Run("a snapshot in both places counts as in S3", func(t *testing.T) {
		// The merge keeps the LOCAL path for a file in both places, so the
		// path alone would call this one local only.
		srv, id, dir := s3CopyServer(t, true)
		stubListings(map[string]s3CopyListing{dir: {snaps: []time.Time{now}}, s3: {snaps: []time.Time{now}}})
		got := getS3Copy(t, srv, id)
		if got.S3 == nil || got.S3.Newest != now.Format(consoleTSFormat) || got.S3.HeldNewer {
			t.Fatalf("s3=%+v, want newest %s and nothing held", got.S3, now.Format(consoleTSFormat))
		}
	})

	held := func(t *testing.T, mark bool, localAt, s3At time.Time) bool {
		t.Helper()
		srv, id, dir := s3CopyServer(t, true)
		stubListings(map[string]s3CopyListing{dir: {snaps: []time.Time{localAt}}, s3: {snaps: []time.Time{s3At}}})
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		if mark {
			if err := baseline.MarkUploadHeld(dir, reconstruct.SnapshotDirName(localAt)); err != nil {
				t.Fatal(err)
			}
			if _, err := os.Stat(filepath.Join(dir, ".upload-held."+reconstruct.SnapshotDirName(localAt))); err != nil {
				t.Fatalf("the mark was not written where the test expects it: %v", err)
			}
		}
		got := getS3Copy(t, srv, id)
		if got.S3 == nil {
			t.Fatal("no S3 block")
		}
		return got.S3.HeldNewer
	}
	t.Run("a newer local snapshot with the held mark is reported", func(t *testing.T) {
		if !held(t, true, now, now.Add(-3*time.Hour)) {
			t.Fatal("held_newer=false, want true")
		}
	})
	t.Run("a newer local snapshot with no mark is not held", func(t *testing.T) {
		if held(t, false, now, now.Add(-3*time.Hour)) {
			t.Fatal("held_newer=true for an unmarked snapshot: that is a pending or failed upload")
		}
	})
	t.Run("an unreadable bucket does not hide that the newest is held here", func(t *testing.T) {
		srv, id, dir := s3CopyServer(t, true)
		stubListings(map[string]s3CopyListing{dir: {snaps: []time.Time{now}}, s3: {err: errors.New("AccessDenied")}})
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := baseline.MarkUploadHeld(dir, reconstruct.SnapshotDirName(now)); err != nil {
			t.Fatal(err)
		}
		got := getS3Copy(t, srv, id)
		if got.S3 == nil || !got.S3.Unread || !got.S3.HeldNewer {
			t.Fatalf("s3=%+v, want unread and held", got.S3)
		}
	})
	t.Run("a local folder listed in part is not a no", func(t *testing.T) {
		srv, id, dir := s3CopyServer(t, true)
		stubListings(map[string]s3CopyListing{dir: {snaps: []time.Time{now}, skipped: 1}, s3: {snaps: []time.Time{now.Add(-time.Hour)}}})
		var raw struct {
			S3 map[string]any `json:"s3"`
		}
		_, body := doServersReqHeader(t, srv, "GET", "/api/baselines", "", id)
		if err := json.Unmarshal(body, &raw); err != nil {
			t.Fatal(err)
		}
		if raw.S3["held_unknown"] != true {
			t.Fatalf("s3=%v, want held_unknown: a skipped local folder may be the newest", raw.S3)
		}
	})
	t.Run("a stale mark on a snapshot already in S3 is not held", func(t *testing.T) {
		if held(t, true, now, now) {
			t.Fatal("held_newer=true, but the bucket already has that snapshot")
		}
	})
}

// The settings row counts what reaches S3 on the upload interval, so it gets
// the interval as Go parses it, like the schedule's own.
func TestBackupSettingsDTO_carriesTheUploadInterval(t *testing.T) {
	srv, id := newScheduleServer(t, nil)
	e, _ := srv.cm.reg.Get(id)
	e.BaselineS3 = "s3://bkt/snaps"
	e.BackupSchedule = &BackupSchedule{Every: "15m", At: "00:00", UploadEvery: "6h"}
	if err := srv.cm.reg.Update(e); err != nil {
		t.Fatal(err)
	}
	e, _ = srv.cm.reg.Get(id)
	dto := srv.backupSettingsServerDTO(e)
	if dto.ScheduleUploadEvery != "6h" || dto.ScheduleUploadEveryMinutes != 360 {
		t.Fatalf("upload_every=%q minutes=%d, want 6h and 360", dto.ScheduleUploadEvery, dto.ScheduleUploadEveryMinutes)
	}
}
