package console

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
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
