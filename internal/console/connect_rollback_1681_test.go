package console

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// A Connect that saves a server and then cannot start capture rolls the
// server back (#1681). Two things that rollback must get right about snapshot
// folders: it must not mark the folder of the server it pointed at as held
// (it never took a snapshot there), and it must not leave behind a folder it
// created itself, a new one on every retry.

// newFolderServer is a supervisor over a file registry that creates folders,
// as the watch daemon does.
func newFolderServer(t *testing.T) (*Server, *stubMonitorCtrl, string) {
	t.Helper()
	clearStores(t)
	state := t.TempDir()
	reg, err := LoadRegistry(filepath.Join(state, "console-servers.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	ctrl := &stubMonitorCtrl{}
	srv, err := New(Config{Listen: "127.0.0.1:8090", Token: "t", Registry: reg, MonitorCtrl: ctrl,
		LocalPruneLoop: true, MayCreateFolders: true})
	if err != nil {
		t.Fatal(err)
	}
	return srv, ctrl, state
}

func wantNoSuchDir(t *testing.T, dir, why string) {
	t.Helper()
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Errorf("%s: %s is still there (stat err %v)", why, dir, err)
	}
}

func wantEmptyOrMissing(t *testing.T, dir, why string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Errorf("%s: %s holds %d entries, the first %q", why, dir, len(entries), entries[0].Name())
	}
}

func TestConnectRollback_doesNotHoldTheOtherServersFolder(t *testing.T) {
	srv, ctrl, state := newFolderServer(t)
	x := filepath.Join(state, "x")
	if err := os.MkdirAll(x, 0o700); err != nil {
		t.Fatal(err)
	}
	a, err := srv.cm.reg.Add(ServerEntry{Name: "a", DSN: "u:p@tcp(h:3306)/a", BaselineDir: x, LocalKeepNewest: 3})
	if err != nil {
		t.Fatal(err)
	}
	ctrl.startErr = errors.New("index server refused the connection")
	rec, body := doServersReq(t, srv, "POST", "/api/servers/check",
		`{"source_host":"db.example.com","source_user":"dbtrail","source_password":"Ab3-xyz","baseline_dir":"`+x+`"}`)
	if rec.Code != 200 || decodeCheck(t, body).Started {
		t.Fatalf("test premise: the start was supposed to fail: code=%d body=%s", rec.Code, body)
	}
	if srv.cm.reg.Len() != 1 {
		t.Fatalf("the rollback left %d servers", srv.cm.reg.Len())
	}
	if e, _ := srv.cm.reg.Get(a.ID); heldNow(e) {
		t.Error("the rollback marked a's folder held; b never took a snapshot there")
	}
	if n := LocalKeepTargets(srv.cm.reg.List())[canonicalDir(x)]; n != 3 {
		t.Errorf("a's folder is pruned to %d after the rollback, want 3", n)
	}
	// A folder that existed before the request is never removed.
	if _, err := os.Stat(x); err != nil {
		t.Errorf("the rollback removed a folder it did not create: %v", err)
	}
}

func TestConnectRollback_removesTheFolderItCreated(t *testing.T) {
	srv, ctrl, state := newFolderServer(t)
	ctrl.startErr = errors.New("no")
	// The default folder: <state>/snapshots/<the id the rollback took back>.
	for i := 0; i < 2; i++ { // and again on a retry
		rec, body := doServersReq(t, srv, "POST", "/api/servers/check", checkBody)
		if rec.Code != 200 || decodeCheck(t, body).Started {
			t.Fatalf("test premise: the start was supposed to fail: code=%d body=%s", rec.Code, body)
		}
	}
	wantEmptyOrMissing(t, filepath.Join(state, localSnapshotsDirName), "a failed Connect left its default folder")
	// A named folder the request created is removed too.
	fresh := filepath.Join(state, "fresh")
	rec, body := doServersReq(t, srv, "POST", "/api/servers/check",
		`{"source_host":"db.example.com","source_user":"dbtrail","source_password":"Ab3-xyz","baseline_dir":"`+fresh+`"}`)
	if rec.Code != 200 || decodeCheck(t, body).Started {
		t.Fatalf("test premise: the start was supposed to fail: code=%d body=%s", rec.Code, body)
	}
	wantNoSuchDir(t, fresh, "a failed Connect left the named folder it created")
}

// Creates that are refused AFTER the folder was made clean up the same way:
// a name that is already taken (the add fails), and an index database that
// could not be chosen (the add is undone).
func TestCreateRefused_removesTheFolderItCreated(t *testing.T) {
	srv, ctrl, state := newFolderServer(t)
	if _, err := srv.cm.reg.Add(ServerEntry{Name: "taken", DSN: "u:p@tcp(h:3306)/t"}); err != nil {
		t.Fatal(err)
	}
	dup := filepath.Join(state, "dup")
	rec, body := doServersReq(t, srv, "POST", "/api/servers",
		`{"name":"taken","host":"h","port":"3306","user":"u","password":"p","dbname":"i","baseline_dir":"`+dup+`"}`)
	if rec.Code < 400 {
		t.Fatalf("test premise: a duplicate name was accepted: %d %s", rec.Code, body)
	}
	wantNoSuchDir(t, dup, "a create refused for its name left the folder it created")

	ctrl.deriveErr = errors.New("no index server")
	derive := filepath.Join(state, "derive")
	rec, body = doServersReq(t, srv, "POST", "/api/servers",
		`{"source_host":"db.example.com","source_user":"dbtrail","source_password":"Ab3-xyz","baseline_dir":"`+derive+`"}`)
	if rec.Code < 400 {
		t.Fatalf("test premise: a failed index choice was accepted: %d %s", rec.Code, body)
	}
	wantNoSuchDir(t, derive, "a create whose index could not be chosen left the folder it created")
	// And its default folder, when none was named.
	rec, body = doServersReq(t, srv, "POST", "/api/servers",
		`{"source_host":"db2.example.com","source_user":"dbtrail","source_password":"Ab3-xyz"}`)
	if rec.Code < 400 {
		t.Fatalf("test premise: a failed index choice was accepted: %d %s", rec.Code, body)
	}
	wantEmptyOrMissing(t, filepath.Join(state, localSnapshotsDirName), "a create whose index could not be chosen left a default folder")
}

// The cleanup only ever removes an EMPTY folder: if anything is in it by the
// time of the rollback, it stays, whatever that is.
func TestRemoveCreatedDir_neverRemovesAFolderWithSomethingInIt(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "made")
	if err := os.MkdirAll(filepath.Join(dir, "2026-01-01T00-00-00Z"), 0o700); err != nil {
		t.Fatal(err)
	}
	removeCreatedDir(dir)
	if _, err := os.Stat(filepath.Join(dir, "2026-01-01T00-00-00Z")); err != nil {
		t.Fatalf("the cleanup removed a folder that was not empty: %v", err)
	}
	removeCreatedDir("") // nothing created: nothing to do
	empty := filepath.Join(t.TempDir(), "empty")
	if err := os.Mkdir(empty, 0o700); err != nil {
		t.Fatal(err)
	}
	removeCreatedDir(empty)
	wantNoSuchDir(t, empty, "an empty created folder")
}
