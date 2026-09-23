package console

import (
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
)

// A folder two servers shared is never pruned (#1681), because a snapshot
// does not record which server wrote it. That must still hold after the
// folder STOPS being shared: the other server's snapshots are still in it,
// and would count as the remaining server's copies. These tests go through
// the real handlers for the three ways a folder stops being shared.

// sharedPair creates A (its own default folder, keep 3) and B pointed at A's
// folder, with a snapshot already in it, and returns both ids and the folder.
func sharedPair(t *testing.T, srv *Server) (a, b, dir string) {
	t.Helper()
	rec, da := createServer(t, srv, `{"name":"a","host":"h","port":"3306","user":"u","password":"p","dbname":"ia"}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("create a: %d %s", rec.Code, rec.Body.String())
	}
	ea, _ := srv.cm.reg.Get(da.ID)
	if ea.LocalKeepNewest <= 0 || ea.BaselineDir == "" {
		t.Fatalf("test premise: a has no folder or count: %+v", ea)
	}
	dir = ea.BaselineDir
	makeSnapshotDir(t, dir)
	rec, db := createServer(t, srv, `{"name":"b","host":"h","port":"3306","user":"u","password":"p","dbname":"ib","baseline_dir":"`+dir+`"}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("create b: %d %s", rec.Code, rec.Body.String())
	}
	if _, ok := LocalKeepTargets(srv.cm.reg.List())[canonicalDir(dir)]; ok {
		t.Fatal("test premise: a shared folder is a prune target")
	}
	return da.ID, db.ID, dir
}

// wantHeld asserts dir is not a prune target and id's row says why.
func wantHeld(t *testing.T, srv *Server, id, dir string) {
	t.Helper()
	if n, ok := LocalKeepTargets(srv.cm.reg.List())[canonicalDir(dir)]; ok {
		t.Fatalf("the folder that held another server's snapshots is pruned to %d", n)
	}
	if r := srv.localRetentionOf(id); r != nil {
		t.Errorf("the listing reports a retention of %d for it", r.KeepNewest)
	}
	for _, row := range backupSettingsGet(t, srv).Servers {
		if row.ID == id && (!row.KeepBlocked || !row.KeepHeld) {
			t.Errorf("row %s: keep_blocked=%v keep_held=%v, want both true", row.Name, row.KeepBlocked, row.KeepHeld)
		}
	}
}

func TestLocalKeepHeld_deletingTheOtherServerKeepsTheFolderUnpruned(t *testing.T) {
	srv, _ := newLocalCopyServer(t)
	a, b, dir := sharedPair(t, srv)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("DELETE", "/api/servers/"+b, nil)
	req.SetPathValue("id", b)
	srv.handleServersDelete(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("delete b: %d %s", rec.Code, rec.Body.String())
	}
	wantHeld(t, srv, a, dir)

	// Survives a restart: the registry file carries it.
	reg2, err := LoadRegistry(srv.cm.reg.path)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := LocalKeepTargets(reg2.List())[canonicalDir(dir)]; ok {
		t.Fatal("after a reload the folder is a prune target again")
	}
}

func TestLocalKeepHeld_theOtherServerAnsweringNoKeepsTheFolderUnpruned(t *testing.T) {
	srv, _ := newLocalCopyServer(t)
	a, b, dir := sharedPair(t, srv)
	if rec := putBackupSettings(t, srv, b, `{"local_copy":false,"baseline_s3":"s3://bucket/b/"}`); rec.Code != http.StatusOK {
		t.Fatalf("b answers no: %d %s", rec.Code, rec.Body.String())
	}
	wantHeld(t, srv, a, dir)
}

func TestLocalKeepHeld_theOwnerMovingAwayLeavesTheOtherUnpruned(t *testing.T) {
	srv, state := newLocalCopyServer(t)
	a, b, dir := sharedPair(t, srv)
	// B needs a count of its own for the folder to be a target at all.
	if rec := putBackupSettings(t, srv, b, `{"keep_newest":2}`); rec.Code != http.StatusOK {
		t.Fatalf("b count: %d %s", rec.Code, rec.Body.String())
	}
	fresh := filepath.Join(state, "a-new")
	if rec := putBackupSettings(t, srv, a, `{"baseline_dir":"`+fresh+`"}`); rec.Code != http.StatusOK {
		t.Fatalf("a moves: %d %s", rec.Code, rec.Body.String())
	}
	wantHeld(t, srv, b, dir)
	// A's new, empty folder is its own and is counted.
	if n := LocalKeepTargets(srv.cm.reg.List())[canonicalDir(fresh)]; n <= 0 {
		t.Errorf("a's new folder is not a target (count %d)", n)
	}

	// The way out the page names: a new empty folder. B moves, and is counted.
	other := filepath.Join(state, "b-new")
	if rec := putBackupSettings(t, srv, b, `{"baseline_dir":"`+other+`"}`); rec.Code != http.StatusOK {
		t.Fatalf("b moves: %d %s", rec.Code, rec.Body.String())
	}
	if n := LocalKeepTargets(srv.cm.reg.List())[canonicalDir(other)]; n != 2 {
		t.Errorf("b's new folder is pruned to %d, want 2", n)
	}
	if e, _ := srv.cm.reg.Get(b); heldNow(e) {
		t.Error("moving to a new folder did not clear the hold")
	}
}

// The hold belongs to the registry: an edit that does not move the folder
// keeps it whatever the caller sends, and a new entry never starts held.
func TestLocalKeepHeld_callersCannotClearOrSetIt(t *testing.T) {
	srv, _ := newLocalCopyServer(t)
	a, b, _ := sharedPair(t, srv)
	if err := srv.cm.reg.Delete(b); err != nil {
		t.Fatal(err)
	}
	e, _ := srv.cm.reg.Get(a)
	if !heldNow(e) {
		t.Fatal("test premise: a is not held")
	}
	e.LocalKeepHeldDir = ""
	e.LocalKeepNewest = 5
	if err := srv.cm.reg.Update(e); err != nil {
		t.Fatal(err)
	}
	if e2, _ := srv.cm.reg.Get(a); !heldNow(e2) {
		t.Error("an edit that kept the folder cleared the hold")
	}
	added, err := srv.cm.reg.Add(ServerEntry{Name: "n", DSN: "u:p@tcp(h:3306)/n", LocalKeepHeldDir: "/somewhere"})
	if err != nil {
		t.Fatal(err)
	}
	if added.LocalKeepHeldDir != "" {
		t.Error("a new entry started held")
	}
}

// Undoing a create that failed half way is not a folder "stopping being
// shared": the new server never wrote a snapshot, so it must not hold the
// folder of the server it pointed at.
func TestLocalKeepHeld_undoingAFailedCreateHoldsNothing(t *testing.T) {
	srv, _ := newLocalCopyServer(t)
	a, err := srv.cm.reg.Add(ServerEntry{Name: "a", DSN: "u:p@tcp(h:3306)/a", BaselineDir: t.TempDir(), LocalKeepNewest: 3})
	if err != nil {
		t.Fatal(err)
	}
	b, err := srv.cm.reg.Add(ServerEntry{Name: "b", DSN: "u:p@tcp(h:3306)/b", BaselineDir: a.BaselineDir})
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.cm.reg.UndoAdd(b.ID); err != nil {
		t.Fatal(err)
	}
	if e, _ := srv.cm.reg.Get(a.ID); heldNow(e) {
		t.Fatal("undoing a failed create held the other server's folder")
	}
	if _, ok := srv.cm.reg.Get(b.ID); ok {
		t.Fatal("the undone entry is still listed")
	}
}

// Answering no and then yes on the same folder, or moving away and back,
// does not forget the hold: the other server's snapshots are still there.
func TestLocalKeepHeld_noThenYesOnTheSameFolderStaysHeld(t *testing.T) {
	srv, _ := newLocalCopyServer(t)
	a, b, dir := sharedPair(t, srv)
	if err := srv.cm.reg.Delete(b); err != nil {
		t.Fatal(err)
	}
	if rec := putBackupSettings(t, srv, a, `{"local_copy":false,"baseline_s3":"s3://bucket/a/"}`); rec.Code != http.StatusOK {
		t.Fatalf("a answers no: %d %s", rec.Code, rec.Body.String())
	}
	if rec := putBackupSettings(t, srv, a, `{"local_copy":true,"baseline_dir":"`+dir+`","baseline_s3":"","keep_newest":0}`); rec.Code != http.StatusOK {
		t.Fatalf("a answers yes again: %d %s", rec.Code, rec.Body.String())
	}
	if rec := putBackupSettings(t, srv, a, `{"keep_newest":2}`); rec.Code != http.StatusOK {
		t.Fatalf("a sets a count: %d %s", rec.Code, rec.Body.String())
	}
	wantHeld(t, srv, a, dir)
}
