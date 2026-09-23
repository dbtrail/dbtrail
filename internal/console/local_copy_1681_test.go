package console

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// #1681: every NEW server keeps a local copy of its snapshots by default, in
// <state dir>/baselines/<id>; the per-server question is one yes/no; a folder
// that cannot be used is refused at save instead of shown with a tick.

// newLocalCopyServer is a Server over a registry FILE (the default folder
// needs a state directory), returning the state directory too.
func newLocalCopyServer(t *testing.T) (*Server, string) {
	t.Helper()
	clearStores(t)
	state := t.TempDir()
	reg, err := LoadRegistry(filepath.Join(state, "console-servers.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	srv, err := New(Config{Listen: "127.0.0.1:8090", Token: "t", Registry: reg, LocalPruneLoop: true})
	if err != nil {
		t.Fatal(err)
	}
	return srv, state
}

func createServer(t *testing.T, srv *Server, body string) (*httptest.ResponseRecorder, serverDTO) {
	t.Helper()
	rec := httptest.NewRecorder()
	srv.handleServersCreate(rec, httptest.NewRequest("POST", "/api/servers", strings.NewReader(body)))
	var dto serverDTO
	if rec.Code == http.StatusCreated {
		if err := json.Unmarshal(rec.Body.Bytes(), &dto); err != nil {
			t.Fatal(err)
		}
	}
	return rec, dto
}

func putBackupSettings(t *testing.T, srv *Server, id, body string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("PUT", "/api/backup-settings/servers/"+id, strings.NewReader(body))
	req.SetPathValue("id", id)
	srv.handleBackupSettingsServerUpdate(rec, req)
	return rec
}

func wantMode(t *testing.T, path string, want os.FileMode) {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	if got := info.Mode().Perm(); got != want {
		t.Errorf("%s mode = %o, want %o", path, got, want)
	}
}

const newServerBody = `{"name":"prod","host":"h","port":"3306","user":"u","password":"p","dbname":"idx"`

func TestLocalCopy_newServerGetsItsOwnFolderByID(t *testing.T) {
	srv, state := newLocalCopyServer(t)
	rec, dto := createServer(t, srv, newServerBody+`}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("create: %d %s", rec.Code, rec.Body.String())
	}
	want := filepath.Join(state, "baselines", dto.ID)
	e, _ := srv.cm.reg.Get(dto.ID)
	if e.BaselineDir != want {
		t.Fatalf("folder = %q, want %q", e.BaselineDir, want)
	}
	if e.LocalKeepNewest != DefaultLocalKeepNewest {
		t.Errorf("keep newest = %d, want %d for a new server", e.LocalKeepNewest, DefaultLocalKeepNewest)
	}
	wantMode(t, want, 0o700)
	// Keyed by id: a rename moves nothing.
	e.Name = "renamed"
	if err := srv.cm.reg.Update(e); err != nil {
		t.Fatal(err)
	}
	after, _ := srv.cm.reg.Get(dto.ID)
	if after.BaselineDir != want {
		t.Errorf("a rename changed the folder to %q", after.BaselineDir)
	}
	// And it is persisted, not computed on read.
	reg2, err := LoadRegistry(filepath.Join(state, "console-servers.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := reg2.Get(dto.ID); got.BaselineDir != want || got.LocalKeepNewest != DefaultLocalKeepNewest {
		t.Errorf("reloaded entry: dir=%q keep=%d", got.BaselineDir, got.LocalKeepNewest)
	}
}

func TestLocalCopy_newServerWithoutALocalCopyNeedsADestination(t *testing.T) {
	srv, _ := newLocalCopyServer(t)
	if rec, _ := createServer(t, srv, newServerBody+`,"local_copy":false}`); rec.Code != http.StatusBadRequest {
		t.Fatalf("no copy anywhere: code %d, want 400", rec.Code)
	}
	if srv.cm.reg.Len() != 0 {
		t.Fatal("a refused create left an entry")
	}
	rec, dto := createServer(t, srv, newServerBody+`,"local_copy":false,"baseline_s3":"s3://b/p/"}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("S3 only: %d %s", rec.Code, rec.Body.String())
	}
	if e, _ := srv.cm.reg.Get(dto.ID); e.BaselineDir != "" {
		t.Errorf("S3-only server got a local folder %q", e.BaselineDir)
	}
	if rec, _ := createServer(t, srv, `{"name":"x","host":"h","port":"3306","user":"u","password":"p","dbname":"i2","local_copy":false,"baseline_s3":"s3://b/q/","baseline_dir":"/tmp/x"}`); rec.Code != http.StatusBadRequest {
		t.Errorf("no local copy with a folder: code %d, want 400", rec.Code)
	}
}

func TestLocalCopy_namedFolderIsCheckedAtCreate(t *testing.T) {
	srv, state := newLocalCopyServer(t)
	if rec, _ := createServer(t, srv, newServerBody+`,"baseline_dir":"relative/snaps"}`); rec.Code != http.StatusBadRequest {
		t.Fatalf("relative folder: code %d, want 400", rec.Code)
	}
	named := filepath.Join(state, "elsewhere", "snaps")
	rec, dto := createServer(t, srv, newServerBody+`,"baseline_dir":"  `+named+`  "}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("named folder: %d %s", rec.Code, rec.Body.String())
	}
	if e, _ := srv.cm.reg.Get(dto.ID); e.BaselineDir != named {
		t.Errorf("folder = %q, want %q (trimmed)", e.BaselineDir, named)
	}
	wantMode(t, named, 0o700)
}

// An in-memory registry has no state directory, so no default folder.
func TestLocalCopy_noStateDirectoryNoDefault(t *testing.T) {
	reg, _ := LoadRegistry("")
	if got := reg.DefaultBaselineDir("abc"); got != "" {
		t.Fatalf("in-memory default = %q, want empty", got)
	}
}

// Existing servers are not touched: an entry saved before #1681, with no
// folder or with a folder and no retention, loads and is served as it was.
func TestLocalCopy_existingServersAreUnchanged(t *testing.T) {
	state := t.TempDir()
	path := filepath.Join(state, "console-servers.yaml")
	legacy := "version: 1\nservers:\n" +
		"- id: aaaa000000000001\n  name: bare\n  index_dsn: u:p@tcp(h:3306)/a\n" +
		"- id: aaaa000000000002\n  name: s3only\n  index_dsn: u:p@tcp(h:3306)/b\n  baseline_s3: s3://b/p/\n" +
		"- id: aaaa000000000003\n  name: localonly\n  index_dsn: u:p@tcp(h:3306)/c\n  baseline_dir: /srv/snaps\n"
	if err := os.WriteFile(path, []byte(legacy), 0o600); err != nil {
		t.Fatal(err)
	}
	clearStores(t)
	reg, err := LoadRegistry(path)
	if err != nil {
		t.Fatal(err)
	}
	srv, err := New(Config{Listen: "127.0.0.1:8090", Token: "t", Registry: reg, LocalPruneLoop: true})
	if err != nil {
		t.Fatal(err)
	}
	dto := backupSettingsGet(t, srv)
	for _, s := range dto.Servers {
		e, _ := reg.Get(s.ID)
		if e.LocalKeepNewest != 0 {
			t.Errorf("%s: an existing server got a retention of %d", s.Name, e.LocalKeepNewest)
		}
		switch s.Name {
		case "bare", "s3only":
			if s.LocalCopy || e.BaselineDir != "" {
				t.Errorf("%s: an existing server got a local copy (%q)", s.Name, e.BaselineDir)
			}
		case "localonly":
			if !s.LocalCopy || e.BaselineDir != "/srv/snaps" {
				t.Errorf("localonly: dir %q", e.BaselineDir)
			}
		}
	}
	if _, err := os.Stat(filepath.Join(state, "baselines")); !os.IsNotExist(err) {
		t.Errorf("reading the settings created a snapshot folder: %v", err)
	}
	after, _ := os.ReadFile(path)
	if string(after) != legacy {
		t.Errorf("reading the settings rewrote the registry:\n%s", after)
	}
}

// A plain edit of the connection keeps the retention it never shows.
func TestLocalCopy_connectionEditKeepsTheRetention(t *testing.T) {
	srv, _ := newLocalCopyServer(t)
	_, dto := createServer(t, srv, newServerBody+`}`)
	e, _ := srv.cm.reg.Get(dto.ID)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("PUT", "/api/servers/"+dto.ID, strings.NewReader(
		`{"name":"prod2","host":"h","port":"3306","user":"u","dbname":"idx","baseline_dir":"`+e.BaselineDir+`"}`))
	req.SetPathValue("id", dto.ID)
	srv.handleServersUpdate(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("edit: %d %s", rec.Code, rec.Body.String())
	}
	if after, _ := srv.cm.reg.Get(dto.ID); after.LocalKeepNewest != DefaultLocalKeepNewest || after.Name != "prod2" {
		t.Fatalf("after edit: keep=%d name=%q", after.LocalKeepNewest, after.Name)
	}
}

// A connection edit that CHANGES the folder has it checked; one that does not
// change it saves even when the folder is broken.
func TestLocalCopy_connectionEditChecksAChangedFolderOnly(t *testing.T) {
	srv, _ := newLocalCopyServer(t)
	e, err := srv.cm.reg.Add(ServerEntry{Name: "old", DSN: "u:p@tcp(h:3306)/idx", BaselineDir: "/definitely/not/here/snaps"})
	if err != nil {
		t.Fatal(err)
	}
	send := func(dir string) int {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest("PUT", "/api/servers/"+e.ID, strings.NewReader(
			`{"name":"old","host":"h","port":"3306","user":"u","dbname":"idx","baseline_dir":"`+dir+`"}`))
		req.SetPathValue("id", e.ID)
		srv.handleServersUpdate(rec, req)
		return rec.Code
	}
	if code := send("/definitely/not/here/snaps"); code != http.StatusOK {
		t.Errorf("unchanged broken folder: code %d, want 200", code)
	}
	if code := send("relative"); code != http.StatusBadRequest {
		t.Errorf("changed to a relative folder: code %d, want 400", code)
	}
}

// ── the yes/no on the backup settings ────────────────────────────────────────

func TestLocalCopy_answeringNoClearsTheFolderButNeedsADestination(t *testing.T) {
	srv, _ := newLocalCopyServer(t)
	_, dto := createServer(t, srv, newServerBody+`}`)
	before, _ := srv.cm.reg.Get(dto.ID)

	if rec := putBackupSettings(t, srv, dto.ID, `{"local_copy":false}`); rec.Code != http.StatusBadRequest {
		t.Fatalf("no copy anywhere: code %d, want 400", rec.Code)
	}
	if e, _ := srv.cm.reg.Get(dto.ID); e.BaselineDir != before.BaselineDir {
		t.Fatalf("a refused save changed the folder to %q", e.BaselineDir)
	}
	rec := putBackupSettings(t, srv, dto.ID, `{"local_copy":false,"baseline_s3":"s3://b/p/"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("no + S3: %d %s", rec.Code, rec.Body.String())
	}
	e, _ := srv.cm.reg.Get(dto.ID)
	if e.BaselineDir != "" || e.BaselineS3 != "s3://b/p/" {
		t.Fatalf("after no: dir=%q s3=%q", e.BaselineDir, e.BaselineS3)
	}
	// The snapshots already there stay: answering no deletes nothing.
	if _, err := os.Stat(before.BaselineDir); err != nil {
		t.Errorf("answering no removed the folder: %v", err)
	}
	var got backupSettingsServerDTO
	_ = json.Unmarshal(rec.Body.Bytes(), &got)
	if got.LocalCopy {
		t.Error("the answer still says local copy")
	}
}

func TestLocalCopy_answeringYesUsesTheDefaultFolder(t *testing.T) {
	srv, state := newLocalCopyServer(t)
	e, err := srv.cm.reg.Add(ServerEntry{Name: "s3", DSN: "u:p@tcp(h:3306)/idx", BaselineS3: "s3://b/p/"})
	if err != nil {
		t.Fatal(err)
	}
	rec := putBackupSettings(t, srv, e.ID, `{"local_copy":true}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("yes: %d %s", rec.Code, rec.Body.String())
	}
	want := filepath.Join(state, "baselines", e.ID)
	after, _ := srv.cm.reg.Get(e.ID)
	if after.BaselineDir != want {
		t.Fatalf("folder = %q, want %q", after.BaselineDir, want)
	}
	wantMode(t, want, 0o700)
	// Turning the copy on does not invent a retention for an existing server.
	if after.LocalKeepNewest != 0 {
		t.Errorf("yes set a retention of %d on an existing server", after.LocalKeepNewest)
	}
	var got backupSettingsServerDTO
	_ = json.Unmarshal(rec.Body.Bytes(), &got)
	if !got.LocalCopy || got.DefaultDir != want {
		t.Errorf("answer: local=%v default=%q", got.LocalCopy, got.DefaultDir)
	}
}

func TestLocalCopy_folderChecks(t *testing.T) {
	srv, state := newLocalCopyServer(t)
	e, err := srv.cm.reg.Add(ServerEntry{Name: "s", DSN: "u:p@tcp(h:3306)/idx"})
	if err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(state, "a-file")
	if err := os.WriteFile(file, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	for name, dir := range map[string]string{
		"relative":        "snaps",
		"a file":          file,
		"under a file":    filepath.Join(file, "snaps"),
		"dot-dot sneaked": "../../snaps",
	} {
		rec := putBackupSettings(t, srv, e.ID, `{"baseline_dir":"`+dir+`"}`)
		if rec.Code != http.StatusBadRequest {
			t.Errorf("%s (%s): code %d, want 400", name, dir, rec.Code)
		}
		if name == "a file" && !strings.Contains(rec.Body.String(), "is a file, not a folder") {
			t.Errorf("a file is refused as a file, got %s", rec.Body.String())
		}
		if got, _ := srv.cm.reg.Get(e.ID); got.BaselineDir != "" {
			t.Fatalf("%s: a refused folder was saved: %q", name, got.BaselineDir)
		}
	}
	// The defect this fixes: a folder that does not exist is created, not
	// saved with a tick and left missing.
	missing := filepath.Join(state, "new", "place")
	if rec := putBackupSettings(t, srv, e.ID, `{"baseline_dir":"`+missing+`"}`); rec.Code != http.StatusOK {
		t.Fatalf("missing folder: %d %s", rec.Code, rec.Body.String())
	}
	wantMode(t, missing, 0o700)
	// No write-check file is left behind.
	entries, _ := os.ReadDir(missing)
	if len(entries) != 0 {
		t.Errorf("the check left %v in the folder", entries)
	}
}

func TestLocalCopy_unwritableFolderIsRefused(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root writes through permissions; prepareLocalSnapshotDir's write probe is exercised by the other cases")
	}
	srv, state := newLocalCopyServer(t)
	e, _ := srv.cm.reg.Add(ServerEntry{Name: "s", DSN: "u:p@tcp(h:3306)/idx"})
	ro := filepath.Join(state, "ro")
	if err := os.Mkdir(ro, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(ro, 0o700) })
	if rec := putBackupSettings(t, srv, e.ID, `{"baseline_dir":"`+ro+`"}`); rec.Code != http.StatusBadRequest {
		t.Fatalf("read-only folder: code %d, want 400", rec.Code)
	}
}

// A save that does not change the folder does not re-check it: the archive
// toggle still saves on a server whose folder has gone missing.
func TestLocalCopy_unchangedFolderIsNotRechecked(t *testing.T) {
	srv, _ := newLocalCopyServer(t)
	e, _ := srv.cm.reg.Add(ServerEntry{Name: "s", DSN: "u:p@tcp(h:3306)/idx", BaselineDir: "/definitely/not/here"})
	if rec := putBackupSettings(t, srv, e.ID, `{"no_archive":true}`); rec.Code != http.StatusOK {
		t.Fatalf("toggle on a broken folder: %d %s", rec.Code, rec.Body.String())
	}
}

func TestLocalCopy_keepNewestIsSavedAndBounded(t *testing.T) {
	srv, _ := newLocalCopyServer(t)
	_, dto := createServer(t, srv, newServerBody+`}`)
	for _, bad := range []string{"-1", "1001"} {
		if rec := putBackupSettings(t, srv, dto.ID, `{"keep_newest":`+bad+`}`); rec.Code != http.StatusBadRequest {
			t.Errorf("keep_newest %s: code %d, want 400", bad, rec.Code)
		}
	}
	if e, _ := srv.cm.reg.Get(dto.ID); e.LocalKeepNewest != DefaultLocalKeepNewest {
		t.Fatalf("a refused count was saved: %d", e.LocalKeepNewest)
	}
	for _, n := range []int{5, 0} {
		rec := putBackupSettings(t, srv, dto.ID, `{"keep_newest":`+itoa(n)+`}`)
		if rec.Code != http.StatusOK {
			t.Fatalf("keep %d: %d %s", n, rec.Code, rec.Body.String())
		}
		var got backupSettingsServerDTO
		_ = json.Unmarshal(rec.Body.Bytes(), &got)
		if got.KeepNewest != n || !got.PruneLoop {
			t.Errorf("answer: keep=%d loop=%v, want %d true", got.KeepNewest, got.PruneLoop, n)
		}
	}
}

func itoa(n int) string {
	b, _ := json.Marshal(n)
	return string(b)
}

// On a daemon started with its own snapshot location, a create that does not
// answer keeps the #1010 fallback; an explicit yes gets the server's folder.
func TestLocalCopy_daemonDefaultKeepsTheFallbackUnlessAsked(t *testing.T) {
	clearStores(t)
	state := t.TempDir()
	reg, err := LoadRegistry(filepath.Join(state, "console-servers.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	srv, err := New(Config{Listen: "127.0.0.1:8090", Token: "t", Registry: reg, BaselineDir: "/var/bintrail/baselines"})
	if err != nil {
		t.Fatal(err)
	}
	_, quiet := createServer(t, srv, newServerBody+`}`)
	if e, _ := reg.Get(quiet.ID); e.BaselineDir != "" {
		t.Errorf("unanswered create on a daemon with a default got %q", e.BaselineDir)
	}
	rec, asked := createServer(t, srv, `{"name":"b","host":"h","port":"3306","user":"u","password":"p","dbname":"i2","local_copy":true}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("create: %d %s", rec.Code, rec.Body.String())
	}
	if e, _ := reg.Get(asked.ID); e.BaselineDir != filepath.Join(state, "baselines", asked.ID) {
		t.Errorf("explicit yes got %q", e.BaselineDir)
	}
}
