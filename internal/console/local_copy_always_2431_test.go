package console

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/copymirror"
)

// #2431: every server keeps a local copy of its snapshots. No request can
// make a server keep them only in S3, and an entry saved that way before is
// given its default folder by the daemon at load.

func TestLocalCopyAlways_createRefusesNoLocalCopy(t *testing.T) {
	srv, _ := newLocalCopyServer(t)
	for _, body := range []string{
		newServerBody + `,"local_copy":false}`,
		newServerBody + `,"local_copy":false,"baseline_s3":"s3://b/p/"}`,
		newServerBody + `,"local_copy":false,"baseline_s3":"s3://b/p/","baseline_dir":"/tmp/x"}`,
	} {
		rec, _ := createServer(t, srv, body)
		if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "every server keeps a copy of its snapshots on this machine") {
			t.Errorf("%s: %d %s, want 400 naming the rule", body, rec.Code, rec.Body.String())
		}
	}
	if srv.cm.reg.Len() != 0 {
		t.Fatal("a refused create left an entry")
	}
	// Omitted and true are a local copy, with S3 beside it.
	for i, extra := range []string{`,"baseline_s3":"s3://b/p/"}`, `,"local_copy":true,"baseline_s3":"s3://b/q/"}`} {
		body := strings.Replace(newServerBody, `"prod"`, fmt.Sprintf(`"prod%d"`, i), 1)
		body = strings.Replace(body, `"idx"`, fmt.Sprintf(`"idx%d"`, i), 1)
		rec, dto := createServer(t, srv, body+extra)
		if rec.Code != http.StatusCreated {
			t.Fatalf("case %d: %d %s", i, rec.Code, rec.Body.String())
		}
		if e, _ := srv.cm.reg.Get(dto.ID); e.BaselineDir == "" {
			t.Errorf("case %d: created with no local folder", i)
		}
	}
}

func TestLocalCopyAlways_backupSettingsRefuseNoLocalCopy(t *testing.T) {
	srv, _ := newLocalCopyServer(t)
	_, dto := createServer(t, srv, newServerBody+`,"baseline_s3":"s3://b/p/"}`)
	before, _ := srv.cm.reg.Get(dto.ID)
	for _, body := range []string{
		`{"local_copy":false}`,
		`{"local_copy":false,"baseline_s3":"s3://b/p/"}`,
		`{"baseline_dir":""}`,
		`{"baseline_dir":"   "}`,
		`{"baseline_dir":"","baseline_s3":"s3://b/p/"}`,
	} {
		rec := putBackupSettings(t, srv, dto.ID, body)
		if rec.Code != http.StatusBadRequest {
			t.Errorf("%s: code %d, want 400 (%s)", body, rec.Code, rec.Body.String())
		}
		if e, _ := srv.cm.reg.Get(dto.ID); e.BaselineDir != before.BaselineDir || e.BaselineS3 != before.BaselineS3 {
			t.Fatalf("%s: a refused save changed dir=%q s3=%q", body, e.BaselineDir, e.BaselineS3)
		}
	}
	// What the page sends: yes with an emptied folder is the default one.
	if rec := putBackupSettings(t, srv, dto.ID, `{"local_copy":true,"baseline_dir":"","baseline_s3":"s3://b/p/"}`); rec.Code != http.StatusOK {
		t.Fatalf("yes + empty folder: %d %s", rec.Code, rec.Body.String())
	}
	if e, _ := srv.cm.reg.Get(dto.ID); e.BaselineDir == "" {
		t.Fatal("yes + empty folder left no folder")
	}
	// A save that does not touch the folder, on an entry that already has
	// none (an old file), is not refused for it: only emptying one is.
	old, err := srv.cm.reg.Add(ServerEntry{Name: "old", DSN: "u:p@tcp(h:3306)/o", BaselineS3: "s3://b/o/"})
	if err != nil {
		t.Fatal(err)
	}
	if rec := putBackupSettings(t, srv, old.ID, `{"no_archive":true}`); rec.Code != http.StatusOK {
		t.Errorf("untouched folder on an old entry: %d %s", rec.Code, rec.Body.String())
	}
}

func TestLocalCopyAlways_connectionEditCannotEmptyTheFolder(t *testing.T) {
	srv, _ := newLocalCopyServer(t)
	_, dto := createServer(t, srv, newServerBody+`,"baseline_s3":"s3://b/p/"}`)
	before, _ := srv.cm.reg.Get(dto.ID)
	send := func(dirField string) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest("PUT", "/api/servers/"+dto.ID, strings.NewReader(
			`{"name":"prod","host":"h","port":"3306","user":"u","dbname":"idx"`+dirField+`}`))
		req.SetPathValue("id", dto.ID)
		srv.handleServersUpdate(rec, req)
		return rec
	}
	for _, f := range []string{`,"baseline_dir":""`, `,"baseline_dir":"  "`} {
		if rec := send(f); rec.Code != http.StatusBadRequest {
			t.Errorf("%s: code %d, want 400 (%s)", f, rec.Code, rec.Body.String())
		}
		if e, _ := srv.cm.reg.Get(dto.ID); e.BaselineDir != before.BaselineDir {
			t.Fatalf("%s: folder changed to %q", f, e.BaselineDir)
		}
	}
	// Left out, the folder is kept, as before #2431.
	if rec := send(""); rec.Code != http.StatusOK {
		t.Fatalf("folder left out: %d %s", rec.Code, rec.Body.String())
	}
	if e, _ := srv.cm.reg.Get(dto.ID); e.BaselineDir != before.BaselineDir {
		t.Fatalf("folder left out changed it to %q", e.BaselineDir)
	}
}

// The load-time step: an entry with a bucket and no folder gets its default
// folder, created 0700 and saved; every other entry is left exactly as it
// was; a second load changes nothing.
func TestGiveEveryServerALocalCopy(t *testing.T) {
	clearStores(t)
	state := t.TempDir()
	path := filepath.Join(state, "console-servers.yaml")
	legacy := "version: 1\nservers:\n" +
		"- id: aaaa000000000001\n  name: bare\n  index_dsn: u:p@tcp(h:3306)/a\n" +
		"- id: aaaa000000000002\n  name: s3only\n  index_dsn: u:p@tcp(h:3306)/b\n  baseline_s3: s3://b/p/\n" +
		"- id: aaaa000000000003\n  name: localonly\n  index_dsn: u:p@tcp(h:3306)/c\n  baseline_dir: /srv/snaps\n" +
		"- id: aaaa000000000004\n  name: both\n  index_dsn: u:p@tcp(h:3306)/d\n  baseline_dir: /srv/both\n  baseline_s3: s3://b/d/\n" +
		"- id: aaaa000000000005\n  name: spaces\n  index_dsn: u:p@tcp(h:3306)/e\n  baseline_dir: '  '\n  baseline_s3: s3://b/e/\n"
	if err := os.WriteFile(path, []byte(legacy), 0o600); err != nil {
		t.Fatal(err)
	}
	reg, err := LoadRegistry(path)
	if err != nil {
		t.Fatal(err)
	}
	got := reg.GiveEveryServerALocalCopy()
	if len(got) != 2 {
		t.Fatalf("assignments = %+v, want s3only and spaces", got)
	}
	for _, a := range got {
		if a.Err != nil {
			t.Fatalf("%s: %v", a.Name, a.Err)
		}
		want := filepath.Join(state, "snapshots", a.ID)
		if a.Dir != want {
			t.Errorf("%s: dir %q, want %q", a.Name, a.Dir, want)
		}
		wantMode(t, want, 0o700)
	}
	want := map[string]string{
		"bare": "", "s3only": filepath.Join(state, "snapshots", "aaaa000000000002"),
		"localonly": "/srv/snaps", "both": "/srv/both", "spaces": filepath.Join(state, "snapshots", "aaaa000000000005"),
	}
	reread, err := LoadRegistry(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range reread.List() {
		if e.BaselineDir != want[e.Name] {
			t.Errorf("%s: saved dir %q, want %q", e.Name, e.BaselineDir, want[e.Name])
		}
		if e.LocalKeepNewest != 0 {
			t.Errorf("%s: got a retention of %d", e.Name, e.LocalKeepNewest)
		}
	}
	saved, _ := os.ReadFile(path)
	if again := reread.GiveEveryServerALocalCopy(); len(again) != 0 {
		t.Errorf("second load assigned %+v", again)
	}
	if after, _ := os.ReadFile(path); string(after) != string(saved) {
		t.Error("a second load rewrote the registry")
	}
}

// A folder that cannot be made is reported, and the entry is left as it was.
func TestGiveEveryServerALocalCopy_unusableFolder(t *testing.T) {
	clearStores(t)
	state := t.TempDir()
	path := filepath.Join(state, "console-servers.yaml")
	if err := os.WriteFile(path, []byte("version: 1\nservers:\n- id: aaaa000000000002\n  name: s3only\n  index_dsn: u:p@tcp(h:3306)/b\n  baseline_s3: s3://b/p/\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	// A file where the snapshots folder goes.
	if err := os.WriteFile(filepath.Join(state, "snapshots"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	reg, err := LoadRegistry(path)
	if err != nil {
		t.Fatal(err)
	}
	got := reg.GiveEveryServerALocalCopy()
	if len(got) != 1 || got[0].Err == nil {
		t.Fatalf("assignments = %+v, want one failure", got)
	}
	if e, _ := reg.Get("aaaa000000000002"); e.BaselineDir != "" {
		t.Errorf("failed assignment saved dir %q", e.BaselineDir)
	}
}

// A registry with no file has no state directory: nothing to assign, and
// nothing is reported as assigned.
func TestGiveEveryServerALocalCopy_inMemory(t *testing.T) {
	clearStores(t)
	reg, _ := LoadRegistry("")
	if _, err := reg.Add(ServerEntry{Name: "s", DSN: "u:p@tcp(h:3306)/a", BaselineS3: "s3://b/p/"}); err != nil {
		t.Fatal(err)
	}
	got := reg.GiveEveryServerALocalCopy()
	if len(got) != 1 || got[0].Err == nil || got[0].Dir != "" {
		t.Fatalf("assignments = %+v, want one failure with no folder", got)
	}
}

// The mirror an S3-only server's SQL read is moved into its new folder, so
// the copy that was readable before the upgrade still is.
func TestGiveEveryServerALocalCopy_movesTheMirrorIn(t *testing.T) {
	clearStores(t)
	state := t.TempDir()
	path := filepath.Join(state, "console-servers.yaml")
	if err := os.WriteFile(path, []byte("version: 1\nservers:\n- id: aaaa000000000002\n  name: s3only\n  index_dsn: u:p@tcp(h:3306)/b\n  baseline_s3: s3://b/p/\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	reg, err := LoadRegistry(path)
	if err != nil {
		t.Fatal(err)
	}
	e, _ := reg.Get("aaaa000000000002")
	mirror := reg.CopyMirrorDir(e)
	if mirror == "" {
		t.Fatal("fixture: the S3-only entry has no mirror")
	}
	snap := "2026-10-01T00-00-00Z"
	for _, f := range []string{copymirror.SourceFile, snap + "/db/t.parquet", snap + "/" + baseline.SuccessMarker} {
		p := filepath.Join(mirror, filepath.FromSlash(f))
		if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, nil, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	got := reg.GiveEveryServerALocalCopy()
	if len(got) != 1 || got[0].Err != nil || got[0].MoveErr != nil || len(got[0].Moved) != 1 {
		t.Fatalf("assignments = %+v, want one with the snapshot moved", got)
	}
	if _, err := os.Stat(filepath.Join(state, "snapshots", e.ID, snap, "db", "t.parquet")); err != nil {
		t.Fatalf("the mirrored snapshot is not in the folder: %v", err)
	}
	after, _ := reg.Get(e.ID)
	if reg.CopyMirrorDir(after) != "" {
		t.Error("the server still has a mirror after getting a folder")
	}
}

// A registry a newer version wrote is read-only: nothing is created, saved,
// or reported.
func TestGiveEveryServerALocalCopy_readOnlyRegistry(t *testing.T) {
	clearStores(t)
	state := t.TempDir()
	path := filepath.Join(state, "console-servers.yaml")
	if err := os.WriteFile(path, []byte("version: 99\nservers:\n- id: aaaa000000000002\n  name: s3only\n  index_dsn: u:p@tcp(h:3306)/b\n  baseline_s3: s3://b/p/\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	reg, err := LoadRegistry(path)
	if err != nil {
		t.Fatal(err)
	}
	if !reg.ReadOnly() {
		t.Fatal("fixture: registry is not read-only")
	}
	if got := reg.GiveEveryServerALocalCopy(); len(got) != 0 {
		t.Fatalf("assignments = %+v, want none", got)
	}
	if _, err := os.Stat(filepath.Join(state, "snapshots")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("a read-only registry got a folder made: %v", err)
	}
}

// A save that fails (here: two entries with one name, which Update refuses)
// leaves no folder this call made.
func TestGiveEveryServerALocalCopy_failedSaveRemovesTheFolder(t *testing.T) {
	clearStores(t)
	state := t.TempDir()
	path := filepath.Join(state, "console-servers.yaml")
	if err := os.WriteFile(path, []byte("version: 1\nservers:\n"+
		"- id: aaaa000000000002\n  name: same\n  index_dsn: u:p@tcp(h:3306)/b\n  baseline_s3: s3://b/p/\n"+
		"- id: aaaa000000000003\n  name: same\n  index_dsn: u:p@tcp(h:3306)/c\n  baseline_dir: /srv/c\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	reg, err := LoadRegistry(path)
	if err != nil {
		t.Fatal(err)
	}
	got := reg.GiveEveryServerALocalCopy()
	if len(got) != 1 || got[0].Err == nil {
		t.Skipf("fixture: the save did not fail (%+v); Update accepts duplicate names here", got)
	}
	if _, err := os.Stat(got[0].Dir); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("the folder made for a failed save is still there: %v", err)
	}
}

// A server with no folder cannot be given an S3 destination alone through
// either edit route; one already saved that way keeps its bucket on a save
// that does not change it.
func TestLocalCopyAlways_noBucketAloneOnAServerWithNoFolder(t *testing.T) {
	srv, _ := newLocalCopyServer(t)
	bare, err := srv.cm.reg.Add(ServerEntry{Name: "bare", DSN: "u:p@tcp(h:3306)/a"})
	if err != nil {
		t.Fatal(err)
	}
	old, err := srv.cm.reg.Add(ServerEntry{Name: "old", DSN: "u:p@tcp(h:3306)/o", BaselineS3: "s3://b/o/"})
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct{ id, body string }{
		{bare.ID, `{"baseline_s3":"s3://b/a/"}`},
		{old.ID, `{"baseline_s3":"s3://b/other/"}`},
	} {
		if rec := putBackupSettings(t, srv, c.id, c.body); rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "needs a folder beside it") {
			t.Errorf("settings %s: %d %s, want 400", c.body, rec.Code, rec.Body.String())
		}
	}
	edit := func(id, name, db, extra string) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest("PUT", "/api/servers/"+id, strings.NewReader(
			`{"name":"`+name+`","host":"h","port":"3306","user":"u","dbname":"`+db+`"`+extra+`}`))
		req.SetPathValue("id", id)
		srv.handleServersUpdate(rec, req)
		return rec
	}
	if rec := edit(bare.ID, "bare", "a", `,"baseline_s3":"s3://b/a/"`); rec.Code != http.StatusBadRequest {
		t.Errorf("connection edit, bucket alone: %d %s, want 400", rec.Code, rec.Body.String())
	}
	if rec := edit(old.ID, "old", "o", ``); rec.Code != http.StatusOK {
		t.Errorf("connection edit leaving the old bucket alone: %d %s", rec.Code, rec.Body.String())
	}
	if e, _ := srv.cm.reg.Get(bare.ID); e.BaselineS3 != "" {
		t.Errorf("bare got a bucket alone: %q", e.BaselineS3)
	}
	// With a folder beside it, the bucket is taken.
	if rec := putBackupSettings(t, srv, bare.ID, `{"local_copy":true,"baseline_s3":"s3://b/a/"}`); rec.Code != http.StatusOK {
		t.Errorf("bucket with a folder: %d %s", rec.Code, rec.Body.String())
	}
}

// On the daemon, a create whose default folder cannot be made is undone
// instead of saved with no folder.
func TestLocalCopyAlways_createWhoseFolderCannotBeMadeIsUndone(t *testing.T) {
	srv, state := newLocalCopyServer(t)
	if err := os.WriteFile(filepath.Join(state, "snapshots"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	rec, _ := createServer(t, srv, newServerBody+`,"baseline_s3":"s3://b/p/"}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("code %d %s, want 400", rec.Code, rec.Body.String())
	}
	if srv.cm.reg.Len() != 0 {
		t.Fatal("the create left an entry with no folder")
	}
}
