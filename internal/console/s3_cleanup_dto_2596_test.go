package console

import (
	"slices"
	"testing"
)

// #2596 part 3b: the settings row says whether the S3 cleanup runs for the
// server, at what age, why not when its age is set and it does not, and
// what the last cleanup did.
func TestBackupSettingsDTO_s3Cleanup(t *testing.T) {
	srv := newRegistryServer(t)
	srv.localPruneLoop = true
	add := func(name, s3, age string) ServerEntry {
		t.Helper()
		e, err := srv.cm.reg.Add(ServerEntry{Name: name, DSN: "u:p@tcp(h:3306)/" + name, BaselineDir: t.TempDir(), BaselineS3: s3, BaselineS3Retain: age})
		if err != nil {
			t.Fatal(err)
		}
		return e
	}
	alone := add("alone", "s3://b/alone", "7d")
	dto := srv.backupSettingsServerDTO(alone)
	if dto.S3RetainMinutes != 7*1440 || dto.S3RetainOff != "" {
		t.Fatalf("alone: minutes=%d off=%q", dto.S3RetainMinutes, dto.S3RetainOff)
	}
	off := add("off", "s3://b/off", "")
	if d := srv.backupSettingsServerDTO(off); d.S3RetainMinutes != 0 || d.S3RetainOff != "" {
		t.Fatalf("no age: %+v", d.S3RetainOff)
	}
	shared := add("shared", "s3://b/shared", "7d")
	add("quiet", "s3://b/shared/", "")
	d := srv.backupSettingsServerDTO(shared)
	if d.S3RetainMinutes != 0 || d.S3RetainOff != "shared" || !slices.Equal(d.S3RetainSharedWith, []string{"quiet"}) {
		t.Fatalf("shared: minutes=%d off=%q with=%v", d.S3RetainMinutes, d.S3RetainOff, d.S3RetainSharedWith)
	}
	srv.cm.defaultBaselineS3 = "s3://b/daemon"
	daemon := add("daemon", "s3://b/daemon", "7d")
	if d := srv.backupSettingsServerDTO(daemon); d.S3RetainOff != "startup_destination" {
		t.Fatalf("daemon's destination: off=%q", d.S3RetainOff)
	}

	// The last cleanup, recorded by the loop, keyed by destination however
	// it is spelled.
	if dto.S3Cleanup != nil {
		t.Fatal("a cleanup that never ran is reported")
	}
	RecordS3Cleanup("s3://b/alone/", S3CleanupRecord{At: "2026-11-01T12:00:00Z", Removed: 3, KeptNewest: 1})
	t.Cleanup(func() { resetS3Cleanups() })
	got := srv.backupSettingsServerDTO(alone).S3Cleanup
	if got == nil || got.Removed != 3 || got.KeptNewest != 1 {
		t.Fatalf("last cleanup = %+v", got)
	}
}

// The row never says the cleanup runs where it cannot know: a process that
// runs no cleanup loop (serve), a destination that is not s3://, an own age
// shorter than a day; and a sharer whose age cannot be read is not "unset".
func TestBackupSettingsDTO_s3CleanupOffReasons(t *testing.T) {
	srv := newRegistryServer(t)
	add := func(name, s3, age string) ServerEntry {
		t.Helper()
		e, err := srv.cm.reg.Add(ServerEntry{Name: name, DSN: "u:p@tcp(h:3306)/" + name, BaselineDir: t.TempDir(), BaselineS3: s3, BaselineS3Retain: age})
		if err != nil {
			t.Fatal(err)
		}
		return e
	}
	srv.localPruneLoop = false
	a := add("a", "s3://b/a", "7d")
	if d := srv.backupSettingsServerDTO(a); d.S3RetainMinutes != 0 || d.S3RetainOff != "no_loop" {
		t.Fatalf("no loop: minutes=%d off=%q", d.S3RetainMinutes, d.S3RetainOff)
	}
	srv.localPruneLoop = true
	short := add("short", "s3://b/short", "12h")
	if d := srv.backupSettingsServerDTO(short); d.S3RetainOff != "too_short" {
		t.Fatalf("12h: off=%q", d.S3RetainOff)
	}
	// A destination that is not s3:// (a hand-edited file) removes nothing.
	gs := add("gs", "s3://b/gs", "7d")
	gs.BaselineS3 = "gs://b/gs"
	if err := srv.cm.reg.Update(gs); err != nil {
		t.Fatal(err)
	}
	if d := srv.backupSettingsServerDTO(gs); d.S3RetainOff != "not_s3" || d.S3RetainMinutes != 0 {
		t.Fatalf("not s3: off=%q minutes=%d", d.S3RetainOff, d.S3RetainMinutes)
	}
	bad := add("bad", "s3://b/bad", "seven")
	if d := srv.backupSettingsServerDTO(bad); d.S3RetainOff != "unreadable" {
		t.Fatalf("garbage: off=%q", d.S3RetainOff)
	}
	s := add("s", "s3://b/s", "7d")
	add("u", "s3://b/s", "")
	add("v", "s3://b/s", "x1")
	if d := srv.backupSettingsServerDTO(s); d.S3RetainOff != "shared" || !slices.Equal(d.S3RetainSharedWith, []string{"u"}) || !slices.Equal(d.S3RetainSharedUnreadable, []string{"v"}) {
		t.Fatalf("shared: off=%q unset=%v unreadable=%v", d.S3RetainOff, d.S3RetainSharedWith, d.S3RetainSharedUnreadable)
	}
}

// A skipped cleanup keeps the error of the last one that ran: a failure
// must not vanish from the page behind restores.
func TestRecordS3Cleanup_skipKeepsTheLastError(t *testing.T) {
	t.Cleanup(resetS3Cleanups)
	RecordS3Cleanup("s3://b/k", S3CleanupRecord{At: "1", Error: "AccessDenied", Snapshot: "x", AccessDenied: true})
	RecordS3Cleanup("s3://b/k/", S3CleanupRecord{At: "2", Skipped: true})
	got := lastS3Cleanup("s3://b/k")
	if got == nil || !got.Skipped || got.At != "2" || got.Error != "AccessDenied" || !got.AccessDenied {
		t.Fatalf("got %+v", got)
	}
	RecordS3Cleanup("s3://b/k", S3CleanupRecord{At: "3", Removed: 1})
	if got := lastS3Cleanup("s3://b/k"); got.Error != "" {
		t.Fatalf("a cleanup that ran clean kept the old error: %+v", got)
	}
}
