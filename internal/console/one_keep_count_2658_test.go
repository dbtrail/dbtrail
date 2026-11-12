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
