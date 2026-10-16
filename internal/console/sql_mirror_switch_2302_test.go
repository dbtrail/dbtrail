package console

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// The per-server switch that turns the SQL mirror off (#2302).

// Off means no mirror folder, and "off" is said only of a server that would
// have a mirror with the switch on.
func TestCopyMirrorOff(t *testing.T) {
	reg, err := LoadRegistry(filepath.Join(t.TempDir(), "console-servers.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	mem, err := LoadRegistry("")
	if err != nil {
		t.Fatal(err)
	}
	for name, c := range map[string]struct {
		reg *Registry
		e   ServerEntry
		dir bool // it has a mirror folder
		off bool
	}{
		"S3 only":                    {reg, ServerEntry{ID: "abc", BaselineS3: "s3://b/p"}, true, false},
		"S3 only, switched off":      {reg, ServerEntry{ID: "abc", BaselineS3: "s3://b/p", NoSQLMirror: true}, false, true},
		"local folder, switched off": {reg, ServerEntry{ID: "abc", BaselineDir: "/d", BaselineS3: "s3://b/p", NoSQLMirror: true}, false, false},
		"no bucket, switched off":    {reg, ServerEntry{ID: "abc", NoSQLMirror: true}, false, false},
		"an id that is not a name":   {reg, ServerEntry{ID: "../x", BaselineS3: "s3://b/p", NoSQLMirror: true}, false, false},
		"registry in memory, off":    {mem, ServerEntry{ID: "abc", BaselineS3: "s3://b/p", NoSQLMirror: true}, false, false},
		"no registry, off":           {nil, ServerEntry{ID: "abc", BaselineS3: "s3://b/p", NoSQLMirror: true}, false, false},
	} {
		if got := c.reg.CopyMirrorDir(c.e) != ""; got != c.dir {
			t.Errorf("%s: has a mirror folder = %v, want %v", name, got, c.dir)
		}
		if got := c.reg.CopyMirrorOff(c.e); got != c.off {
			t.Errorf("%s: CopyMirrorOff = %v, want %v", name, got, c.off)
		}
	}
}

// Every way a bundle is built or rebuilt learns the switch along with the folder.
func TestSQLMirrorSwitch_everyBundleLearnsIt(t *testing.T) {
	reg, err := LoadRegistry(filepath.Join(t.TempDir(), "console-servers.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	entry, err := reg.Add(ServerEntry{Name: "shop", DSN: "u:p@tcp(127.0.0.1:1)/idx", BaselineS3: "s3://bucket/prefix", NoSQLMirror: true})
	if err != nil {
		t.Fatal(err)
	}
	cm := newConnManager(reg, false)
	cm.lazyIndex = true
	b, err := cm.buildBundle(entry)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.db.Close() })
	if b.sqlMirrorDir != "" || !b.sqlMirrorOff {
		t.Errorf("buildBundle: dir %q off %v, want no folder and off", b.sqlMirrorDir, b.sqlMirrorOff)
	}
	cm.mu.Lock()
	cm.bundles[entry.ID] = b
	cm.mu.Unlock()
	cached := func() *bundle {
		cm.mu.Lock()
		defer cm.mu.Unlock()
		return cm.bundles[entry.ID]
	}
	on := entry
	on.NoSQLMirror = false
	cm.rebuildDerived(on)
	if got := cached(); got.sqlMirrorDir != reg.CopyMirrorDir(on) || got.sqlMirrorDir == "" || got.sqlMirrorOff {
		t.Errorf("rebuildDerived, on: dir %q off %v", got.sqlMirrorDir, got.sqlMirrorOff)
	}
	cm.rebuildDerived(entry)
	if got := cached(); got.sqlMirrorDir != "" || !got.sqlMirrorOff {
		t.Errorf("rebuildDerived, off again: dir %q off %v", got.sqlMirrorDir, got.sqlMirrorOff)
	}
}

// Switched off, the page says so and nothing else: not a download, not an
// old failure, not that the service is missing. SQL is not pending, and a
// statement is refused with the reason.
func TestSQLMirrorSwitch_stateAndRefusal(t *testing.T) {
	for _, fills := range []bool{true, false} {
		runner := &fakeSQLRunner{busy: true}
		f := newSQLMirrorFixture(t, runner)
		f.s.fillsCopyMirrors = fills
		f.s.cm.boot.sqlMirrorDir, f.s.cm.boot.sqlMirrorOff = "", true
		f.s.copyMirror = fakeMirrorReporter{bootServerID: {Source: "s3://bucket/prefix", CheckedAt: time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC),
			Err: "access denied", Bringing: "2026-10-11T10-00-00Z"}}
		got := f.s.sqlMirrorOf(f.s.cm.boot, bootServerID)
		if got == nil || got.State != "off" || got.Ready || got.Error != "" || got.Bringing != nil || got.CheckedAt != nil || got.Newest != nil {
			t.Errorf("fills=%v: state = %+v, want off and nothing else", fills, got)
		}
		if f.s.sqlPending(f.s.cm.boot) {
			t.Errorf("fills=%v: SQL is pending on a server whose mirror is off", fills)
		}
		w := httptest.NewRecorder()
		f.s.handleCapabilities(w, httptest.NewRequest("GET", "/api/capabilities", nil))
		var caps capabilitiesResponse
		if err := json.Unmarshal(w.Body.Bytes(), &caps); err != nil {
			t.Fatal(err)
		}
		if caps.SQL || caps.SQLPending {
			t.Errorf("fills=%v: capabilities sql=%v pending=%v, want neither", fills, caps.SQL, caps.SQLPending)
		}
		res := postSQL(t, f.s, `{"sql":"SELECT 1"}`)
		if res.Code != http.StatusConflict || !strings.Contains(res.Body.String(), sqlCopyMirrorOffMessage[:60]) {
			t.Errorf("fills=%v: code=%d body=%s, want 409 %q", fills, res.Code, res.Body.String(), sqlCopyMirrorOffMessage)
		}
	}
	// A stale flag on a server whose copy is a local folder says nothing.
	local := newSQLFixture(t, &fakeSQLRunner{}, false)
	local.s.cm.boot.sqlMirrorOff = true
	if got := local.s.sqlMirrorOf(local.s.cm.boot, bootServerID); got != nil {
		t.Errorf("a server with a local folder: %+v", got)
	}
	if got := local.s.sqlNotLocalMessage(local.s.cm.boot); got == sqlCopyMirrorOffMessage {
		t.Error("a server with a local folder is refused as switched off")
	}
	if len(sqlCopyMirrorOffMessage) > 512 {
		t.Errorf("the refusal is %d bytes; a MySQL client cuts a message at 512", len(sqlCopyMirrorOffMessage))
	}
	if !strings.Contains(sqlCopyMirrorOffMessage, "no_sql_mirror") {
		t.Error("the refusal does not name the setting that turns the copy back on")
	}
}

// The switch is saved by the per-server settings, survives a reload of the
// file, is kept by a save that does not mention it and by an edit of the
// connection, and reaches the open bundle at once.
func TestSQLMirrorSwitch_savedAndKept(t *testing.T) {
	srv, state := newLocalCopyServer(t)
	dto := addS3OnlyEntry(t, srv)
	stored := func() ServerEntry {
		t.Helper()
		e, ok := srv.cm.reg.Get(dto.ID)
		if !ok {
			t.Fatal("the server is gone")
		}
		return e
	}
	saved := func(body string) backupSettingsServerDTO {
		t.Helper()
		rec := putBackupSettings(t, srv, dto.ID, body)
		if rec.Code != http.StatusOK {
			t.Fatalf("PUT %s: %d %s", body, rec.Code, rec.Body.String())
		}
		var out backupSettingsServerDTO
		if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
			t.Fatal(err)
		}
		return out
	}
	if e := stored(); e.NoSQLMirror || srv.cm.reg.CopyMirrorDir(e) == "" {
		t.Fatalf("a new S3-only server: %+v, want the mirror on", e)
	}
	if out := saved(`{"no_sql_mirror":true}`); !out.NoSQLMirror {
		t.Errorf("the answer does not carry the switch: %+v", out)
	}
	if e := stored(); !e.NoSQLMirror || e.BaselineS3 != "s3://bucket/prefix" || e.BaselineDir != "" || srv.cm.reg.CopyMirrorDir(e) != "" {
		t.Errorf("after turning it off: %+v", e)
	}
	// A save about something else keeps it.
	if out := saved(`{"no_archive":false}`); !out.NoSQLMirror {
		t.Errorf("a save that does not mention the switch dropped it: %+v", out)
	}
	// So does the file, read again.
	again, err := LoadRegistry(filepath.Join(state, "console-servers.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if e, _ := again.Get(dto.ID); !e.NoSQLMirror {
		t.Errorf("the registry file lost the switch: %+v", e)
	}
	// And an edit of the connection, which replaces the entry.
	req := httptest.NewRequest("PUT", "/api/servers/"+dto.ID, strings.NewReader(newServerBody+`}`))
	req.SetPathValue("id", dto.ID)
	w := httptest.NewRecorder()
	srv.handleServersUpdate(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("edit: %d %s", w.Code, w.Body.String())
	}
	if e := stored(); !e.NoSQLMirror || e.BaselineS3 != "s3://bucket/prefix" {
		t.Errorf("an edit of the connection dropped the switch: %+v", e)
	}
	if out := saved(`{"no_sql_mirror":false}`); out.NoSQLMirror {
		t.Errorf("the answer still carries the switch: %+v", out)
	}
	if e := stored(); e.NoSQLMirror || srv.cm.reg.CopyMirrorDir(e) == "" {
		t.Errorf("after turning it back on: %+v", e)
	}
	// Not written while it is the default: older versions read the file too.
	raw := mustReadFile(t, filepath.Join(state, "console-servers.yaml"))
	if strings.Contains(raw, "no_sql_mirror") {
		t.Errorf("the default is written to the file:\n%s", raw)
	}
}

// The save reaches a bundle that is already open.
func TestSQLMirrorSwitch_saveReachesTheOpenBundle(t *testing.T) {
	srv, _ := newLocalCopyServer(t)
	dto := addS3OnlyEntry(t, srv)
	entry, _ := srv.cm.reg.Get(dto.ID)
	srv.cm.lazyIndex = true
	b, err := srv.cm.buildBundle(entry)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.db.Close() })
	srv.cm.mu.Lock()
	srv.cm.bundles[dto.ID] = b
	srv.cm.mu.Unlock()
	if rec := putBackupSettings(t, srv, dto.ID, `{"no_sql_mirror":true}`); rec.Code != http.StatusOK {
		t.Fatalf("PUT: %d %s", rec.Code, rec.Body.String())
	}
	srv.cm.mu.Lock()
	got := srv.cm.bundles[dto.ID]
	srv.cm.mu.Unlock()
	if got.sqlMirrorDir != "" || !got.sqlMirrorOff {
		t.Errorf("the open bundle after the save: dir %q off %v", got.sqlMirrorDir, got.sqlMirrorOff)
	}
	if st := srv.sqlMirrorOf(got, dto.ID); st == nil || st.State != "off" {
		t.Errorf("the page's state after the save: %+v", st)
	}
}

// Two of the places a bundle is made need a live index to reach (the open
// that connects, and the one that lost a race with a save), so the rule is
// held by construction: in the manager, newBundle is the only caller of
// newBundleDerived.
func TestSQLMirror_theManagerMakesBundlesOneWay(t *testing.T) {
	src := mustReadFile(t, "manager.go")
	if n := strings.Count(src, "newBundleDerived("); n != 2 {
		t.Errorf("manager.go names newBundleDerived( %d times, want 2: its definition and the call in newBundle", n)
	}
	if !strings.Contains(functionBody(t, src, "func (cm *connManager) newBundle("), "newBundleDerived(db, dbName, entry, cm.profileActive)") {
		t.Error("newBundle no longer builds on newBundleDerived")
	}
}

// addS3OnlyEntry is a server saved with a bucket and no folder, as a file
// written before #2431 holds one: no request can make one any more.
func addS3OnlyEntry(t *testing.T, srv *Server) ServerEntry {
	t.Helper()
	e, err := srv.cm.reg.Add(ServerEntry{Name: "prod", DSN: "u:p@tcp(h:3306)/idx", BaselineS3: "s3://bucket/prefix"})
	if err != nil {
		t.Fatal(err)
	}
	return e
}
