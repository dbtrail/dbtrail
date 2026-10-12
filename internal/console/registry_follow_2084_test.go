package console

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/config"
)

// #2084: a process beside the daemon reads the daemon's files and never
// writes them, and sees what the daemon saves without being restarted.

// ownerAndFollower is the daemon's registry (which saves) and a follower of
// the same file.
func ownerAndFollower(t *testing.T) (owner, follower *Registry, path string) {
	t.Helper()
	clearStores(t)
	path = filepath.Join(t.TempDir(), "console-servers.yaml")
	owner, err := LoadRegistry(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := owner.Add(ServerEntry{Name: "prod", DSN: "u:p@tcp(10.0.0.5:3306)/idx", SourceDSN: "repl:pw@tcp(db.prod:3306)/app"}); err != nil {
		t.Fatal(err)
	}
	follower, err = LoadRegistryFollower(path)
	if err != nil {
		t.Fatal(err)
	}
	return owner, follower, path
}

func mustRead(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// Nothing a follower can be asked to do writes the file: every mutation is
// refused, and the migration that rewrites the file at a daemon's start
// leaves it alone.
func TestRegistryFollower_neverWritesTheFile(t *testing.T) {
	_, f, path := ownerAndFollower(t)
	before := mustRead(t, path)
	e := f.List()[0]
	e.Name = "renamed"
	attempts := map[string]error{
		"Update":      f.Update(e),
		"Delete":      f.Delete(e.ID),
		"UndoAdd":     f.UndoAdd(e.ID),
		"SetRotation": f.SetRotation(RotationConfig{}),
	}
	_, attempts["Add"] = f.Add(ServerEntry{Name: "another", DSN: "u:p@tcp(h:3306)/idx2"})
	_, attempts["AddAutoNamed"] = f.AddAutoNamed(ServerEntry{DSN: "u:p@tcp(h:3306)/idx3"}, "auto")
	for name, err := range attempts {
		if !errors.Is(err, ErrRegistryReadOnly) {
			t.Errorf("%s on a follower: %v, want ErrRegistryReadOnly", name, err)
		}
	}
	f.MigrateProcessBaselineLocation(t.TempDir(), "s3://bucket/prefix/")
	if _, err := f.CorrectSourceFlavor(e.ID, "mariadb"); err == nil {
		t.Error("CorrectSourceFlavor on a follower changed the entry")
	}
	if after := mustRead(t, path); after != before {
		t.Fatalf("a follower wrote the file:\n%s\nwas:\n%s", after, before)
	}
	if got := f.List(); len(got) != 1 || got[0].Name != "prod" {
		t.Errorf("a refused mutation changed what the follower holds: %+v", got)
	}
}

// A follower needs a file, and only a follower reloads.
func TestRegistryFollower_whoMayReload(t *testing.T) {
	if _, err := LoadRegistryFollower(""); err == nil {
		t.Error("a follower of no file was loaded")
	}
	owner, _, _ := ownerAndFollower(t)
	if _, err := owner.Reload(); err == nil {
		t.Error("a registry that saves reloaded; it would lose what it holds in memory")
	}
}

func changeNames(cs []RegistryChange) []string {
	var out []string
	for _, c := range cs {
		switch {
		case c.Added():
			out = append(out, "added "+c.New.Name)
		case c.Removed():
			out = append(out, "removed "+c.Old.Name)
		default:
			out = append(out, "changed "+c.Old.Name+" to "+c.New.Name)
		}
	}
	slices.Sort(out)
	return out
}

// Reload reports each server the daemon added, changed or removed, once, and
// nothing when the file holds what it held.
func TestRegistryFollower_reloadReportsWhatChanged(t *testing.T) {
	owner, f, path := ownerAndFollower(t)
	if cs, err := f.Reload(); err != nil || cs != nil {
		t.Fatalf("a reload with nothing saved: %v, %v", cs, err)
	}
	prod := owner.List()[0]
	staging, err := owner.Add(ServerEntry{Name: "staging", DSN: "u:p@tcp(10.0.0.6:3306)/idx"})
	if err != nil {
		t.Fatal(err)
	}
	cs, err := f.Reload()
	if err != nil || strings.Join(changeNames(cs), "; ") != "added staging" {
		t.Fatalf("after an add: %v, %v", changeNames(cs), err)
	}
	if got, ok := f.Get(staging.ID); !ok || got.DSN != staging.DSN {
		t.Fatalf("the follower does not hold the added server: %+v", got)
	}

	prod.RouteDSN = "fwd:pw@tcp(db.prod:3306)/app"
	if err := owner.Update(prod); err != nil {
		t.Fatal(err)
	}
	if err := owner.Delete(staging.ID); err != nil {
		t.Fatal(err)
	}
	cs, err = f.Reload()
	if err != nil || strings.Join(changeNames(cs), "; ") != "changed prod to prod; removed staging" {
		t.Fatalf("after an edit and a delete: %v, %v", changeNames(cs), err)
	}
	for _, c := range cs {
		if !c.Removed() && (c.Old.RouteDSN != "" || c.New.RouteDSN != prod.RouteDSN) {
			t.Errorf("the change does not carry both sides: old %q, new %q", c.Old.RouteDSN, c.New.RouteDSN)
		}
	}
	if f.Len() != 1 {
		t.Errorf("the follower holds %d servers, want 1", f.Len())
	}
	if cs, err := f.Reload(); err != nil || cs != nil {
		t.Fatalf("a second reload of the same file: %v, %v", cs, err)
	}

	// The same servers written another way (a comment, as a person editing
	// the file by hand leaves) are not a change to any server.
	if err := os.WriteFile(path, []byte("# edited by hand\n"+mustRead(t, path)), 0o600); err != nil {
		t.Fatal(err)
	}
	if cs, err := f.Reload(); err != nil || len(cs) != 0 {
		t.Fatalf("a comment added to the file: %v, %v", changeNames(cs), err)
	}
}

// A file that is unreadable, not YAML, missing or empty where there were
// servers changes nothing: the servers read before stay, and it is an error.
// When the file is good again the follower catches up.
func TestRegistryFollower_aBadFileKeepsWhatWasRead(t *testing.T) {
	owner, f, path := ownerAndFollower(t)
	good := mustRead(t, path)
	for name, damage := range map[string]func(){
		"not YAML": func() { _ = os.WriteFile(path, []byte("servers: [\n\t{{{"), 0o600) },
		"empty":    func() { _ = os.WriteFile(path, nil, 0o600) },
		"blank":    func() { _ = os.WriteFile(path, []byte("\n  \n"), 0o600) },
		"missing":  func() { _ = os.Remove(path) },
	} {
		damage()
		cs, err := f.Reload()
		if err == nil || cs != nil {
			t.Errorf("%s: changes %v, err %v; want an error and no change", name, changeNames(cs), err)
		}
		if got := f.List(); len(got) != 1 || got[0].Name != "prod" {
			t.Errorf("%s: the follower now holds %+v", name, got)
		}
		if err := os.WriteFile(path, []byte(good), 0o600); err != nil {
			t.Fatal(err)
		}
		if cs, err := f.Reload(); err != nil || len(cs) != 0 {
			t.Errorf("%s, then the file as it was: %v, %v", name, changeNames(cs), err)
		}
	}
	// A registry the daemon really emptied is not a damaged file.
	if err := owner.Delete(owner.List()[0].ID); err != nil {
		t.Fatal(err)
	}
	if cs, err := f.Reload(); err != nil || strings.Join(changeNames(cs), "; ") != "removed prod" || f.Len() != 0 {
		t.Fatalf("the daemon deleted its last server: %v, %v, follower holds %d", changeNames(cs), err, f.Len())
	}
}

// followerServer is a console Server over a follower registry, with one
// monitored server the daemon's own registry (owner) can change.
func followerServer(t *testing.T) (owner *Registry, srv *Server, prod ServerEntry, portFile string) {
	t.Helper()
	owner, f, path := ownerAndFollower(t)
	portFile = filepath.Join(filepath.Dir(path), FlashbackFileName)
	srv, err := New(Config{Listen: "127.0.0.1:8090", Token: "tok", Registry: f, FlashbackPath: portFile})
	if err != nil {
		t.Fatal(err)
	}
	return owner, srv, owner.List()[0], portFile
}

// What the daemon changes about a server reaches the connections this
// process holds for it: an account that is no longer the one in force, or a
// server that is gone, closes them; anything else leaves them alone.
func TestFollowFiles_registryChangesReachOpenConnections(t *testing.T) {
	owner, srv, prod, _ := followerServer(t)
	var killed atomic.Int32
	srv.killSourceThreads = func(context.Context, string, config.SSL, []uint32) error {
		killed.Add(1)
		return nil
	}
	var dropped atomic.Int32
	open := func() {
		t.Helper()
		gen := srv.routed.generation(prod.ID)
		if _, ok := srv.TrackRoutedConn(prod.ID, gen, func() { dropped.Add(1) }, func() uint32 { return 77 }); !ok {
			t.Fatal("track refused")
		}
	}
	steps := []struct {
		name     string
		edit     func(e *ServerEntry)
		remove   bool
		wantDrop bool
	}{
		{"nothing saved", nil, false, false},
		{"a rename", func(e *ServerEntry) { e.Name = "prod-2" }, false, false},
		{"a forwarding account saved", func(e *ServerEntry) { e.RouteDSN = "fwd:pw@tcp(db.prod:3306)/app" }, false, true},
		{"its password changed", func(e *ServerEntry) { e.RouteDSN = "fwd:pw2@tcp(db.prod:3306)/app" }, false, true},
		{"the snapshot folder changed", func(e *ServerEntry) { e.BaselineDir = "/snapshots" }, false, false},
		{"the forwarding account removed", func(e *ServerEntry) { e.RouteDSN = "" }, false, true},
		{"the source's password changed", func(e *ServerEntry) { e.SourceDSN = "repl:other@tcp(db.prod:3306)/app" }, false, true},
		{"the server deleted", nil, true, true},
	}
	for _, st := range steps {
		open()
		before, gen := dropped.Load(), srv.routed.generation(prod.ID)
		switch {
		case st.remove:
			if err := owner.Delete(prod.ID); err != nil {
				t.Fatal(err)
			}
		case st.edit != nil:
			st.edit(&prod)
			if err := owner.Update(prod); err != nil {
				t.Fatal(err)
			}
		}
		srv.RefreshFollowedFiles()
		if got := dropped.Load() > before; got != st.wantDrop {
			t.Errorf("%s: connections closed = %v, want %v", st.name, got, st.wantDrop)
		}
		// A connection that read the server before the change and binds
		// after it is refused, so it cannot hold the account that was.
		if _, ok := srv.TrackRoutedConn(prod.ID, gen, func() {}, nil); ok == st.wantDrop {
			t.Errorf("%s: a connection that read the server before the change was tracked = %v", st.name, ok)
		}
		if st.wantDrop {
			dropped.Store(0)
		} else {
			// Left open by this step: close the books on it for the next.
			srv.routed.drop(prod.ID, nil)
			dropped.Store(0)
		}
	}
	srv.routedKills.Wait()
	if killed.Load() == 0 {
		t.Error("no statement of a dropped connection was ended on the source")
	}
	if _, err := srv.ResolveFlashback(context.Background(), "prod-2"); !errors.Is(err, ErrUnknownServer) {
		t.Errorf("the deleted server still resolves: %v", err)
	}
}

// The switch of the saved setting is what a process that follows the file
// opens and closes its port by (#2341), apart from any password: on with no
// password yet is on, and turning it off closes the connections that came
// in with the token, which is still a password.
func TestFollowFiles_theSwitch(t *testing.T) {
	_, srv, _, portFile := followerServer(t)
	closed := 0
	srv.OnFlashbackPasswordWithdrawn(func() { closed++ })
	save := func(f FlashbackFile) {
		t.Helper()
		if err := saveFlashbackFile(portFile, &f); err != nil {
			t.Fatal(err)
		}
		srv.RefreshFollowedFiles()
	}
	srv.RefreshFollowedFiles()
	if srv.FlashbackSwitchOn() {
		t.Fatal("on with no saved setting")
	}
	save(FlashbackFile{Enabled: true})
	if !srv.FlashbackSwitchOn() || closed != 0 {
		t.Fatalf("saved as on: on=%v, closed %d time(s)", srv.FlashbackSwitchOn(), closed)
	}
	save(FlashbackFile{Enabled: false})
	if srv.FlashbackSwitchOn() || closed != 1 {
		t.Fatalf("turned off with no password in the file: on=%v, connections closed %d time(s), want off and 1", srv.FlashbackSwitchOn(), closed)
	}
	save(FlashbackFile{Enabled: true, Password: "pw"})
	if err := os.WriteFile(portFile, []byte("enabled: [\n{{"), 0o600); err != nil {
		t.Fatal(err)
	}
	srv.RefreshFollowedFiles()
	if srv.FlashbackSwitchOn() || closed != 2 {
		t.Fatalf("a file that does not parse: on=%v, closed %d time(s), want off and 2", srv.FlashbackSwitchOn(), closed)
	}
}

// In a process that follows the saved switch, the switch gates the passwords
// themselves: while it is off none is accepted, the token included, so a
// login that arrives as the port is closing is refused and not let in on
// the token.
func TestFollowFiles_theSwitchGatesTheToken(t *testing.T) {
	_, f, path := ownerAndFollower(t)
	portFile := filepath.Join(filepath.Dir(path), FlashbackFileName)
	srv, err := New(Config{Listen: "127.0.0.1:8090", Token: "tok", Registry: f, FlashbackPath: portFile, FlashbackFollowsSwitch: true})
	if err != nil {
		t.Fatal(err)
	}
	passwords := func(f *FlashbackFile) string {
		t.Helper()
		if f != nil {
			if err := saveFlashbackFile(portFile, f); err != nil {
				t.Fatal(err)
			}
		}
		srv.RefreshFollowedFiles()
		return strings.Join(srv.FlashbackPasswords(), ",")
	}
	if got := passwords(nil); got != "" {
		t.Fatalf("with no saved setting: %q, want no password at all", got)
	}
	if got := passwords(&FlashbackFile{Enabled: false, Password: "pw"}); got != "" {
		t.Fatalf("saved as off: %q, want no password at all", got)
	}
	if got := passwords(&FlashbackFile{Enabled: true, Password: "pw"}); got != "tok,pw" {
		t.Fatalf("turned on: %q, want the token and the saved password", got)
	}
	if got := passwords(&FlashbackFile{Enabled: true}); got != "tok" {
		t.Fatalf("on with no saved password: %q, want the token", got)
	}
	if got := passwords(&FlashbackFile{Enabled: false}); got != "" {
		t.Fatalf("turned off: %q, want no password at all", got)
	}
}

// Read routing is the saved setting's too (#2341): on only while the port is
// on and its file says so, and off for a file that cannot be read. Turning
// it on or off closes the connections made under the other setting, once,
// and a save that changes nothing closes none.
func TestFollowFiles_theRoutingSwitch(t *testing.T) {
	_, srv, _, portFile := followerServer(t)
	closed, wantClosed := 0, 0
	srv.OnFlashbackPasswordWithdrawn(func() { closed++ })
	routing := func(step string) bool {
		t.Helper()
		srv.RefreshFollowedFiles()
		if closed != wantClosed {
			t.Errorf("%s: open connections were closed %d time(s), want %d", step, closed, wantClosed)
			wantClosed = closed
		}
		return srv.FlashbackRoutingOn()
	}
	save := func(f FlashbackFile) {
		t.Helper()
		if err := saveFlashbackFile(portFile, &f); err != nil {
			t.Fatal(err)
		}
	}
	if routing("no saved setting") {
		t.Fatal("routing is on with no saved setting")
	}
	save(FlashbackFile{Enabled: true, Password: "pw"})
	if routing("the port turned on, routing never set") {
		t.Fatal("routing is on for a file that does not say so")
	}
	save(FlashbackFile{Enabled: true, Password: "pw", Routing: true})
	wantClosed++
	if !routing("routing turned on") {
		t.Fatal("routing is off after it was saved as on")
	}
	save(FlashbackFile{Enabled: true, Password: "pw", Routing: true})
	if !routing("saved again, unchanged") {
		t.Fatal("routing went off on a save that changed nothing")
	}
	// A new password and routing off at once: one closing, not two.
	save(FlashbackFile{Enabled: true, Password: "pw2", Routing: false})
	wantClosed++
	if routing("a new password and routing off together") {
		t.Fatal("routing stayed on")
	}
	save(FlashbackFile{Enabled: true, Password: "pw2", Routing: true})
	wantClosed++
	if !routing("routing turned on again") {
		t.Fatal("routing is off")
	}
	// The port turned off keeps the setting in the file and routes nothing.
	save(FlashbackFile{Enabled: false, Password: "pw2", Routing: true})
	wantClosed++
	if routing("the port turned off") {
		t.Fatal("routing is on while the port is off")
	}
	save(FlashbackFile{Enabled: true, Password: "pw2", Routing: true})
	if !routing("the port turned on again, routing as saved") {
		t.Fatal("routing is off after the port came back with routing saved as on")
	}
	if err := os.WriteFile(portFile, []byte("routing: [\n{{"), 0o600); err != nil {
		t.Fatal(err)
	}
	wantClosed++
	if routing("a file that does not parse") {
		t.Fatal("routing is on from a file that cannot be read")
	}
	// The field survives a load and a save by this version.
	save(FlashbackFile{Enabled: true, Password: "pw3", Routing: true})
	back, err := LoadFlashbackFile(portFile)
	if err != nil || !back.Routing {
		t.Fatalf("routing after a load: %+v (%v)", back, err)
	}
}

// The password the web interface generates for the port is accepted as soon
// as the daemon saves it with the port on, stops being accepted when it is
// replaced or removed or the port is turned off, and is not accepted from a
// file that cannot be read. The token always is.
func TestFollowFiles_thePortPassword(t *testing.T) {
	_, srv, _, portFile := followerServer(t)
	// closed counts the times the port was told to close its connections.
	closed, wantClosed := 0, 0
	srv.OnFlashbackPasswordWithdrawn(func() { closed++ })
	passwords := func() string {
		t.Helper()
		srv.RefreshFollowedFiles()
		if closed != wantClosed {
			t.Errorf("open connections were closed %d time(s), want %d", closed, wantClosed)
			wantClosed = closed
		}
		return strings.Join(srv.FlashbackPasswords(), ",")
	}
	if got := passwords(); got != "tok" {
		t.Fatalf("with no saved setting: %q", got)
	}
	save := func(f FlashbackFile) {
		t.Helper()
		if err := saveFlashbackFile(portFile, &f); err != nil {
			t.Fatal(err)
		}
	}
	save(FlashbackFile{Enabled: true, Listen: "127.0.0.1:3309", Password: "made-in-the-web"})
	if got := passwords(); got != "tok,made-in-the-web" {
		t.Fatalf("after the daemon saved a password: %q", got)
	}
	// Replaced: the old one opens nothing more, and whoever came in with it
	// does not stay.
	save(FlashbackFile{Enabled: true, Listen: "127.0.0.1:3309", Password: "made-again"})
	wantClosed++
	if got := passwords(); got != "tok,made-again" {
		t.Fatalf("after the password was replaced: %q", got)
	}
	if err := os.WriteFile(portFile, []byte("password: [\n{{"), 0o600); err != nil {
		t.Fatal(err)
	}
	// A password that stops being accepted takes the connections made with
	// it: the daemon's port closes when it is turned off, and this one must
	// not keep them.
	wantClosed++
	if got := passwords(); got != "tok" {
		t.Fatalf("with a file that does not parse: %q", got)
	}
	save(FlashbackFile{Enabled: true, Password: "back"})
	if got := passwords(); got != "tok,back" {
		t.Fatalf("after the file was fixed: %q", got)
	}
	// Turned off in the web interface: the daemon keeps the password in the
	// file and closes its port. This port is still open, so the password
	// must stop opening it.
	save(FlashbackFile{Enabled: false, Listen: "127.0.0.1:3309", Password: "back"})
	wantClosed++
	if got := passwords(); got != "tok" {
		t.Fatalf("after the port was turned off in the web interface: %q", got)
	}
	save(FlashbackFile{Enabled: true, Listen: "127.0.0.1:3309", Password: "back"})
	if got := passwords(); got != "tok,back" {
		t.Fatalf("after the port was turned on again: %q", got)
	}
	if err := os.Remove(portFile); err != nil {
		t.Fatal(err)
	}
	wantClosed++
	if got := passwords(); got != "tok" {
		t.Fatalf("after the file was removed: %q", got)
	}
	// Still off: nothing more to close.
	if got := passwords(); got != "tok" {
		t.Fatalf("a second look at the removed file: %q", got)
	}
	// This process wrote nothing beside the registry.
	entries, err := os.ReadDir(filepath.Dir(portFile))
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if e.Name() != "console-servers.yaml" {
			t.Errorf("a file appeared beside the registry: %s", e.Name())
		}
	}
}

// FollowFiles looks at the files until its context ends, and has looked once
// by the time anything else can run after it was started.
func TestFollowFiles_followsUntilItsContextEnds(t *testing.T) {
	owner, srv, prod, _ := followerServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		srv.FollowFiles(ctx, 10*time.Millisecond)
		close(done)
	}()
	prod.Name = "seen-by-the-follower"
	if err := owner.Update(prod); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		if e, _ := srv.cm.reg.Get(prod.ID); e.Name == prod.Name {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the follower never saw the rename")
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("FollowFiles did not return when its context ended")
	}
}

// A follower started before the daemon has saved its first server has no
// file to read. That is not a fault: it holds no servers, says nothing, and
// reads the file when it appears.
func TestRegistryFollower_theFileDoesNotExistYet(t *testing.T) {
	clearStores(t)
	path := filepath.Join(t.TempDir(), "console-servers.yaml")
	f, err := LoadRegistryFollower(path)
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if cs, err := f.Reload(); err != nil || cs != nil || f.Len() != 0 {
			t.Fatalf("with no file yet: %v, %v, %d servers", changeNames(cs), err, f.Len())
		}
	}
	owner, err := LoadRegistry(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := owner.Add(ServerEntry{Name: "first", DSN: "u:p@tcp(h:3306)/idx"}); err != nil {
		t.Fatal(err)
	}
	if cs, err := f.Reload(); err != nil || strings.Join(changeNames(cs), "; ") != "added first" {
		t.Fatalf("once the daemon saved its first server: %v, %v", changeNames(cs), err)
	}
	// And from then on a missing file is one that was lost.
	if err := owner.Delete(owner.List()[0].ID); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Reload(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Reload(); err == nil {
		t.Error("a file that was there and is gone read as one not written yet")
	}
}
