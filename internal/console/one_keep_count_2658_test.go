package console

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// #2658: one count for every server. The count governs a server's local
// folder with or without an S3 destination. What must not change is what an
// existing server does: a count saved while it did nothing beside a bucket
// is cleared, once, where the file is read.

const oldS3Registry = `version: 1
servers:
  - id: aaaaaaaaaaaaaaa1
    name: with-s3
    index_dsn: u:p@tcp(h:3306)/one
    baseline_dir: /srv/one
    baseline_s3: s3://bucket/one/
    local_keep_newest: 3
  - id: aaaaaaaaaaaaaaa2
    name: local-only
    index_dsn: u:p@tcp(h:3306)/two
    baseline_dir: /srv/two
    local_keep_newest: 3
  - id: aaaaaaaaaaaaaaa3
    name: s3-no-count
    index_dsn: u:p@tcp(h:3306)/three
    baseline_dir: /srv/three
    baseline_s3: s3://bucket/three/
`

// An existing server with an S3 destination reads with no count, so nothing
// starts removing its local snapshots. A local-only server keeps its count:
// that one was already in force.
func TestOneKeepCount_countsSavedUnderTheOldRuleAreCleared(t *testing.T) {
	clearStores(t)
	path := writeRegistryFile(t, oldS3Registry)
	r := loadReg(t, path)
	if e := mustGet(t, r, "aaaaaaaaaaaaaaa1"); e.LocalKeepNewest != 0 {
		t.Errorf("a server with S3 saved before the change keeps a count of %d", e.LocalKeepNewest)
	}
	if e := mustGet(t, r, "aaaaaaaaaaaaaaa2"); e.LocalKeepNewest != 3 {
		t.Errorf("a local-only server lost its count: %d", e.LocalKeepNewest)
	}
	if got := LocalKeepTargets(r.List()); len(got) != 1 || got["/srv/two"] != 3 {
		t.Errorf("folders pruned to a count = %v, want only /srv/two", got)
	}
	// Reading changes no file: the record is written by the process that
	// owns it.
	if raw, _ := os.ReadFile(path); string(raw) != oldS3Registry {
		t.Fatalf("reading the registry rewrote it:\n%s", raw)
	}
	// A second process that only reads lands on the same answer, and so does
	// its reload after the owner touched the file without saving the record.
	f, err := LoadRegistryFollower(path)
	if err != nil {
		t.Fatal(err)
	}
	if e := mustGet(t, f, "aaaaaaaaaaaaaaa1"); e.LocalKeepNewest != 0 {
		t.Errorf("a follower reads the old count: %d", e.LocalKeepNewest)
	}
	if err := os.WriteFile(path, []byte(oldS3Registry+"# touched\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Reload(); err != nil {
		t.Fatal(err)
	}
	if e := mustGet(t, f, "aaaaaaaaaaaaaaa1"); e.LocalKeepNewest != 0 {
		t.Errorf("a follower's reload reads the old count: %d", e.LocalKeepNewest)
	}

	// The owner saves the record, once.
	r.SaveLocalKeepMigration()
	raw, _ := os.ReadFile(path)
	if !strings.Contains(string(raw), "local_keep_with_s3:") || !strings.Contains(string(raw), "aaaaaaaaaaaaaaa1") {
		t.Fatalf("the record was not saved:\n%s", raw)
	}
	// From then on a count on a server with S3 is one somebody set: it stays.
	e := mustGet(t, r, "aaaaaaaaaaaaaaa3")
	e.LocalKeepNewest = 4
	if err := r.Update(e); err != nil {
		t.Fatal(err)
	}
	again := loadReg(t, path)
	if e := mustGet(t, again, "aaaaaaaaaaaaaaa3"); e.LocalKeepNewest != 4 {
		t.Errorf("a count set after the upgrade was cleared at the next read: %d", e.LocalKeepNewest)
	}
	if got := LocalKeepTargets(again.List()); got["/srv/three"] != 4 {
		t.Errorf("the count set after the upgrade is not in force: %v", got)
	}
}

// A registry that starts empty has nothing saved under the old rule: a
// server added with a bucket keeps the count it is given, across a restart.
func TestOneKeepCount_aNewRegistryKeepsTheCountOfANewServer(t *testing.T) {
	clearStores(t)
	path := filepath.Join(t.TempDir(), "console-servers.yaml")
	r := loadReg(t, path)
	e, err := r.Add(ServerEntry{Name: "new", DSN: "u:p@tcp(h:3306)/a", BaselineDir: "/srv/new", BaselineS3: "s3://bucket/new/", LocalKeepNewest: 3})
	if err != nil {
		t.Fatal(err)
	}
	if got := mustGet(t, loadReg(t, path), e.ID); got.LocalKeepNewest != 3 {
		t.Fatalf("a new server's count after a restart = %d, want 3", got.LocalKeepNewest)
	}
}

// A new server with an S3 destination gets the same count as one without,
// through the real create, and the Snapshots page is told it is in force.
func TestOneKeepCount_aNewServerWithS3GetsTheCount(t *testing.T) {
	srv, _ := newLocalCopyServer(t)
	rec, dto := createServer(t, srv, newServerBody+`,"baseline_s3":"s3://bucket/prod/"}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("create: %d %s", rec.Code, rec.Body.String())
	}
	e, _ := srv.cm.reg.Get(dto.ID)
	if e.LocalKeepNewest != DefaultLocalKeepNewest || e.BaselineDir == "" || e.BaselineS3 == "" {
		t.Fatalf("a new server with S3: count %d, folder %q, bucket %q", e.LocalKeepNewest, e.BaselineDir, e.BaselineS3)
	}
	if got := srv.localRetentionOf(e.ID); got == nil || got.KeepNewest != DefaultLocalKeepNewest {
		t.Fatalf("local_retention for a new server with S3 = %+v, want keep %d", got, DefaultLocalKeepNewest)
	}
	row := srv.backupSettingsServerDTO(e)
	if row.KeepNewest != 3 || row.KeepInForce != 3 || row.KeepBlocked {
		t.Fatalf("the settings row: keep %d, in force %d, blocked %v", row.KeepNewest, row.KeepInForce, row.KeepBlocked)
	}
	// A folder that already held snapshots still gets no count (#2475 is
	// not this change's to fix), with a bucket as without.
	full := filepath.Join(t.TempDir(), "full")
	makeSnapshotDir(t, full)
	rec, dto = createServer(t, srv, `{"name":"over","host":"h","port":"3306","user":"u","password":"p","dbname":"i2","baseline_s3":"s3://bucket/over/","baseline_dir":"`+full+`"}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("create over snapshots: %d %s", rec.Code, rec.Body.String())
	}
	if e, _ := srv.cm.reg.Get(dto.ID); e.LocalKeepNewest != 0 {
		t.Errorf("a new server with S3 over existing snapshots got a count of %d", e.LocalKeepNewest)
	}
}

// A server with a count is not moved onto a folder that already holds
// snapshots, with an S3 destination as without: they would be sent to the
// bucket and then removed, and nobody chose that for them.
func TestOneKeepCount_aFolderWithSnapshotsIsNotAdoptedWithS3Either(t *testing.T) {
	srv, state := newLocalCopyServer(t)
	full := filepath.Join(state, "full")
	makeSnapshotDir(t, full)
	_, counted := createServer(t, srv, newServerBody+`,"baseline_s3":"s3://bucket/prod/"}`)
	rec := putBackupSettings(t, srv, counted.ID, `{"baseline_dir":"`+full+`"}`)
	t.Logf("refusal, as the page shows it: %s", rec.Body.String())
	if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "already holds 1 snapshot,") {
		t.Fatalf("moving a counting server with S3 onto snapshots: %d %s", rec.Code, rec.Body.String())
	}
	req := httptest.NewRequest("PUT", "/api/servers/"+counted.ID, strings.NewReader(
		`{"name":"prod","host":"h","port":"3306","user":"u","dbname":"idx","baseline_s3":"s3://bucket/prod/","baseline_dir":"`+full+`"}`))
	req.SetPathValue("id", counted.ID)
	rec = httptest.NewRecorder()
	srv.handleServersUpdate(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("connection edit of a counting server with S3 onto snapshots: %d %s", rec.Code, rec.Body.String())
	}
	if e, _ := srv.cm.reg.Get(counted.ID); e.BaselineDir == full {
		t.Fatal("a refused save moved the folder")
	}
}

// The card's count of snapshots S3 does not have is read where a prune
// against S3 runs for the folder: with a count in force and no age, too.
// Before, only an age refreshed it, so only an age showed it.
func TestOneKeepCount_notInS3IsShownUnderACountAlone(t *testing.T) {
	srv, _ := newLocalCopyServer(t)
	_, dto := createServer(t, srv, newServerBody+`,"baseline_s3":"s3://bucket/prod/"}`)
	e, _ := srv.cm.reg.Get(dto.ID)
	rec := baseline.NotInDestination{At: time.Date(2026, 11, 11, 3, 0, 0, 0, time.UTC), Count: 2, Names: []string{"2026-11-10T03-00-00Z", "2026-11-10T03-05-00Z"}}
	if err := baseline.RewriteNotInDestination(e.BaselineDir, rec); err != nil {
		t.Fatal(err)
	}
	row := srv.backupSettingsServerDTO(e)
	if row.DeleteAfterMinutes != 0 || row.NotInS3 == nil || row.NotInS3.Count != 2 {
		raw, _ := json.Marshal(row)
		t.Fatalf("a count in force and no age: the row does not carry the count of snapshots not in S3: %s", raw)
	}
	// With the count emptied and no age, nothing refreshes the record: not read.
	e.LocalKeepNewest = 0
	if err := srv.cm.reg.Update(e); err != nil {
		t.Fatal(err)
	}
	if row := srv.backupSettingsServerDTO(e); row.NotInS3 != nil {
		t.Fatal("no count and no age: a frozen record is shown as today's")
	}
}

// A server that gets its FIRST S3 destination while its schedule has no
// upload interval starts sending once a day, in the same write, through
// every path that can set the destination. Without it the schedule's empty
// interval, which was never a choice (there was no bucket), would mean a
// snapshot in the bucket at every update. A server that already had a
// destination keeps "each update", and so does one whose destination moves.
func TestOneKeepCount_aFirstDestinationStartsTheUploadInterval(t *testing.T) {
	setup := func(t *testing.T, s3, upload string) (*Server, string) {
		srv, id := newScheduleServer(t, &stubScheduleReporter{full: true})
		e, _ := srv.cm.reg.Get(id)
		fakeSnapshot(t, e.BaselineDir)
		// The destination first, the schedule after: a server that had its
		// bucket before its schedule (one write with both is a first
		// destination, and starts the interval).
		e.BaselineS3 = s3
		if err := srv.cm.reg.Update(e); err != nil {
			t.Fatal(err)
		}
		e.BackupSchedule = &BackupSchedule{Every: "5m", At: "00:00", UploadEvery: upload}
		if err := srv.cm.reg.Update(e); err != nil {
			t.Fatal(err)
		}
		return srv, id
	}
	upload := func(srv *Server, id string) string {
		e, _ := srv.cm.reg.Get(id)
		return e.BackupSchedule.UploadEvery
	}
	settings := func(t *testing.T, srv *Server, id, body string) backupSettingsServerDTO {
		t.Helper()
		rec := putBackupSettings(t, srv, id, body)
		if rec.Code != http.StatusOK {
			t.Fatalf("PUT settings %s: %d %s", body, rec.Code, rec.Body.String())
		}
		var dto backupSettingsServerDTO
		if err := json.Unmarshal(rec.Body.Bytes(), &dto); err != nil {
			t.Fatal(err)
		}
		return dto
	}
	form := func(t *testing.T, srv *Server, id, s3 string) {
		t.Helper()
		e, _ := srv.cm.reg.Get(id)
		body := `{"name":"wp","host":"127.0.0.1","port":"3306","user":"idx","dbname":"idx","baseline_dir":"` + e.BaselineDir + `","baseline_s3":"` + s3 + `"}`
		if rec, out := doServersReq(t, srv, "PUT", "/api/servers/"+id, body); rec.Code != 200 {
			t.Fatalf("PUT server: %d %s", rec.Code, out)
		}
	}

	t.Run("the snapshot settings (the page, and dbtrail snapshot location --s3)", func(t *testing.T) {
		srv, id := setup(t, "", "")
		dto := settings(t, srv, id, `{"baseline_s3":"s3://bucket/first/"}`)
		if got := upload(srv, id); got != DefaultUploadEvery {
			t.Fatalf("after the first destination the schedule sends every %q, want %s", got, DefaultUploadEvery)
		}
		if dto.ScheduleUploadSet != DefaultUploadEvery || dto.ScheduleUploadEvery != DefaultUploadEvery {
			t.Fatalf("the answer does not say the interval it started: set %q, schedule %q", dto.ScheduleUploadSet, dto.ScheduleUploadEvery)
		}
		t.Log(uploadIntervalStartedMsg(DefaultUploadEvery))
		// A later save says nothing: nothing started.
		if dto := settings(t, srv, id, `{"no_archive":true}`); dto.ScheduleUploadSet != "" {
			t.Fatalf("a save that started nothing says %q", dto.ScheduleUploadSet)
		}
	})
	t.Run("the server form and PUT /api/servers", func(t *testing.T) {
		srv, id := setup(t, "", "")
		form(t, srv, id, "s3://bucket/first/")
		if got := upload(srv, id); got != DefaultUploadEvery {
			t.Fatalf("after the first destination the schedule sends every %q, want %s", got, DefaultUploadEvery)
		}
	})
	t.Run("a server that already had a destination keeps each update", func(t *testing.T) {
		srv, id := setup(t, "s3://bucket/had/", "")
		settings(t, srv, id, `{"no_archive":true}`)
		form(t, srv, id, "s3://bucket/had/")
		if got := upload(srv, id); got != "" {
			t.Fatalf("a server that already sent each update now sends every %q", got)
		}
	})
	t.Run("moving to another bucket keeps the schedule as it is", func(t *testing.T) {
		srv, id := setup(t, "s3://bucket/had/", "")
		if dto := settings(t, srv, id, `{"baseline_s3":"s3://bucket/moved/"}`); dto.ScheduleUploadSet != "" {
			t.Fatalf("a moved destination says it started %q", dto.ScheduleUploadSet)
		}
		form(t, srv, id, "s3://bucket/again/")
		if got := upload(srv, id); got != "" {
			t.Fatalf("a moved destination changed each update into %q", got)
		}
	})
	t.Run("no schedule: nothing to start, and no server without a bucket gets one", func(t *testing.T) {
		srv, id := newScheduleServer(t, &stubScheduleReporter{full: true})
		if dto := settings(t, srv, id, `{"baseline_s3":"s3://bucket/first/"}`); dto.ScheduleUploadSet != "" {
			t.Fatalf("no schedule, and the answer says %q", dto.ScheduleUploadSet)
		}
		if e, _ := srv.cm.reg.Get(id); e.BackupSchedule != nil {
			t.Fatalf("a destination created a schedule: %+v", e.BackupSchedule)
		}
		srv2, id2 := setup(t, "", "")
		settings(t, srv2, id2, `{"no_archive":true}`)
		if got := upload(srv2, id2); got != "" {
			t.Fatalf("a server with no bucket got an upload interval %q", got)
		}
	})
}

// The documented downgrade case, pinned as it behaves: a registry that
// already records the migration and holds a server with an S3 destination
// and a count (an older version saved the 3 it does not use, after a
// downgrade) is read with that count in force. docs/upgrade.md says to empty
// Keep by count on such a server before upgrading again.
func TestOneKeepCount_aCountSavedAfterADowngradeApplies(t *testing.T) {
	clearStores(t)
	path := writeRegistryFile(t, `version: 1
local_keep_with_s3:
  at: "2026-11-11T00:00:00Z"
servers:
  - id: aaaaaaaaaaaaaaa1
    name: added-on-the-old-version
    index_dsn: u:p@tcp(h:3306)/one
    baseline_dir: /srv/one
    baseline_s3: s3://bucket/one/
    local_keep_newest: 3
`)
	r := loadReg(t, path)
	if e := mustGet(t, r, "aaaaaaaaaaaaaaa1"); e.LocalKeepNewest != 3 {
		t.Fatalf("count %d, want the stored 3 (the migration ran before this server existed)", e.LocalKeepNewest)
	}
	if got := LocalKeepTargets(r.List()); got["/srv/one"] != 3 {
		t.Fatalf("folders pruned to a count = %v, want /srv/one at 3", got)
	}
}

// Two servers whose S3 destinations are the same prefix are never pruned to a
// count. The prune asks the bucket for <prefix>/<snapshot name>/_SUCCESS and
// the name carries no server, so the bucket would answer for the OTHER
// server's upload and a local snapshot whose own upload failed would be
// removed with no copy anywhere. The daemon's own --baseline-s3 counts as
// another writer of that prefix.
func TestOneKeepCount_aSharedS3DestinationIsNotCounted(t *testing.T) {
	entries := []ServerEntry{
		{ID: "a", Name: "a", BaselineDir: "/srv/a", BaselineS3: "s3://bucket/shared/", LocalKeepNewest: 3},
		{ID: "b", Name: "b", BaselineDir: "/srv/b", BaselineS3: "s3://bucket/shared", LocalKeepNewest: 3},
		{ID: "c", Name: "c", BaselineDir: "/srv/c", BaselineS3: "s3://bucket/own/", LocalKeepNewest: 3},
		{ID: "d", Name: "d", BaselineDir: "/srv/d", BaselineS3: "s3://bucket/daemon/", LocalKeepNewest: 3},
		{ID: "e", Name: "e", BaselineDir: "/srv/e", BaselineS3: "s3://bucket/shared/sub/", LocalKeepNewest: 3},
		{ID: "f", Name: "f", BaselineDir: "/srv/f", LocalKeepNewest: 3},
	}
	got := LocalKeepTargets(entries, "/var/lib/startup", "s3://bucket/daemon")
	if len(got) != 3 || got["/srv/c"] != 3 || got["/srv/e"] != 3 || got["/srv/f"] != 3 {
		t.Fatalf("folders pruned to a count = %v, want only /srv/c, /srv/e (a nested prefix is its own) and /srv/f", got)
	}
	for _, e := range entries[:2] {
		if !LocalKeepSharedS3(entries, e) || !LocalKeepBlocked(entries, e, "/var/lib/startup", "s3://bucket/daemon") {
			t.Errorf("%s: two servers on one prefix, and its folder is not reported as never counted", e.Name)
		}
	}
	if !LocalKeepSharedS3(entries, entries[3], "s3://bucket/daemon") || LocalKeepSharedS3(entries, entries[3]) {
		t.Error("the daemon's own S3 location is not counted as another writer of that prefix, or is without being passed")
	}
	for _, e := range entries[4:] {
		if LocalKeepSharedS3(entries, e, "s3://bucket/daemon") || LocalKeepBlocked(entries, e, "/var/lib/startup", "s3://bucket/daemon") {
			t.Errorf("%s: a destination of its own is reported shared", e.Name)
		}
	}

	// Through the server: the settings row carries it, and the listing
	// announces no count for either.
	srv, _ := newLocalCopyServer(t)
	_, one := createServer(t, srv, newServerBody+`,"baseline_s3":"s3://bucket/shared/"}`)
	two, err := srv.cm.reg.Add(ServerEntry{Name: "two", DSN: "u:p@tcp(h:3306)/two", BaselineDir: t.TempDir(), BaselineS3: "s3://bucket/shared", LocalKeepNewest: 3})
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{one.ID, two.ID} {
		e, _ := srv.cm.reg.Get(id)
		row := srv.backupSettingsServerDTO(e)
		if !row.KeepSharedS3 || !row.KeepBlocked || row.KeepInForce != 0 || srv.localRetentionOf(id) != nil {
			t.Fatalf("%s: shared %v, blocked %v, in force %d, announced %+v", e.Name, row.KeepSharedS3, row.KeepBlocked, row.KeepInForce, srv.localRetentionOf(id))
		}
	}
}
