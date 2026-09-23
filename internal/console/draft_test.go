package console

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/ext"
)

// The Connect draft is the half-filled form somebody leaves behind when they
// go and run the SQL on their database. It lives next to the server registry,
// which is where this product already keeps credentials, and never in the
// browser.

func TestDraftStoreRoundTripsEveryField(t *testing.T) {
	path := filepath.Join(t.TempDir(), "console-connect-draft.yaml")
	d := NewDraftStore(path)
	want := ConnectDraft{
		Name: "prod", Flavor: FlavorPostgres,
		SourceHost: "db.example.com", SourcePort: "5433", SourceUser: "dbtrail",
		Schemas:        "shop,billing",
		SourceDatabase: "appdb", SourceSlot: "slot", SourcePublication: "pub",
	}
	if err := d.Save(want); err != nil {
		t.Fatal(err)
	}
	got, ok, err := d.Load()
	if err != nil || !ok {
		t.Fatalf("Load() = (_, %v, %v), want (_, true, nil)", ok, err)
	}
	got.SavedAt = ""
	if got != want {
		t.Errorf("round trip lost something:\n got %+v\nwant %+v", got, want)
	}
}

// Reading it back is the whole point: the person left the page to run the SQL
// block, and what they typed has to be there when they come back.
func TestDraftStoreSurvivesANewProcess(t *testing.T) {
	path := filepath.Join(t.TempDir(), "console-connect-draft.yaml")
	if err := NewDraftStore(path).Save(ConnectDraft{SourceHost: "db", SourceUser: "dbtrail"}); err != nil {
		t.Fatal(err)
	}
	got, ok, err := NewDraftStore(path).Load()
	if err != nil || !ok {
		t.Fatalf("a second store did not find the draft: (%v, %v)", ok, err)
	}
	if got.SourceHost != "db" || got.SourceUser != "dbtrail" {
		t.Errorf("draft came back as %+v", got)
	}
}

// It names a database and its user, so it is written like the registry: only
// its owner may read it.
func TestDraftStoreFileIsPrivate(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "state")
	path := filepath.Join(dir, "console-connect-draft.yaml")
	if err := NewDraftStore(path).Save(ConnectDraft{SourceHost: "db", SourceUser: "u"}); err != nil {
		t.Fatal(err)
	}
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if mode := fi.Mode().Perm(); mode != 0o600 {
		t.Errorf("draft file mode = %o, want 600", mode)
	}
	di, err := os.Stat(dir)
	if err != nil {
		t.Fatal(err)
	}
	if mode := di.Mode().Perm(); mode != 0o700 {
		t.Errorf("draft directory mode = %o, want 700", mode)
	}
}

func TestDraftStoreMissingFileIsNotAnError(t *testing.T) {
	d := NewDraftStore(filepath.Join(t.TempDir(), "nothing-here.yaml"))
	got, ok, err := d.Load()
	if err != nil {
		t.Fatalf("Load() on a missing file: %v", err)
	}
	if ok {
		t.Errorf("Load() found a draft that was never saved: %+v", got)
	}
}

// An empty file is the shape a crash between create and write leaves. It must
// read as "no draft", not as a draft with every field blank, which the screen
// would restore over what the person is typing.
func TestDraftStoreEmptyFileIsNoDraft(t *testing.T) {
	path := filepath.Join(t.TempDir(), "console-connect-draft.yaml")
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := NewDraftStore(path).Load(); err != nil || ok {
		t.Errorf("an empty file read as (%v, %v), want (false, nil)", ok, err)
	}
}

// A file that is not YAML at all must say so rather than read as no draft: a
// silent "nothing saved" would send somebody to type it all again while the
// thing they typed is sitting on disk.
func TestDraftStoreUnreadableFileIsAnError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "console-connect-draft.yaml")
	if err := os.WriteFile(path, []byte("\tname: [unclosed\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := NewDraftStore(path).Load(); err == nil {
		t.Errorf("a corrupt draft read as (%v, nil), want an error", ok)
	}
}

func TestDraftStoreDiscardRemovesIt(t *testing.T) {
	path := filepath.Join(t.TempDir(), "console-connect-draft.yaml")
	d := NewDraftStore(path)
	if err := d.Save(ConnectDraft{SourceHost: "db"}); err != nil {
		t.Fatal(err)
	}
	if err := d.Discard(); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := d.Load(); ok {
		t.Error("the draft is still there after Discard")
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Errorf("the file is still on disk: %v", err)
	}
	// Discarding again is what a second press of the button does.
	if err := d.Discard(); err != nil {
		t.Errorf("a second Discard: %v", err)
	}
}

// Saving twice keeps the second one: the person edits, checks, edits again.
func TestDraftStoreSaveReplaces(t *testing.T) {
	d := NewDraftStore(filepath.Join(t.TempDir(), "console-connect-draft.yaml"))
	if err := d.Save(ConnectDraft{SourceHost: "first", Schemas: "a"}); err != nil {
		t.Fatal(err)
	}
	if err := d.Save(ConnectDraft{SourceHost: "second"}); err != nil {
		t.Fatal(err)
	}
	got, _, err := d.Load()
	if err != nil {
		t.Fatal(err)
	}
	if got.SourceHost != "second" || got.Schemas != "" {
		t.Errorf("the second save did not replace the first: %+v", got)
	}
}

// With no path (the in-memory registry unit tests use) nothing touches disk,
// and the draft still works for the life of the process.
func TestDraftStoreWithoutAPathStaysInMemory(t *testing.T) {
	d := NewDraftStore("")
	if err := d.Save(ConnectDraft{SourceHost: "db"}); err != nil {
		t.Fatal(err)
	}
	got, ok, err := d.Load()
	if err != nil || !ok || got.SourceHost != "db" {
		t.Errorf("in-memory draft = (%+v, %v, %v)", got, ok, err)
	}
	if err := d.Discard(); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := d.Load(); ok {
		t.Error("the in-memory draft survived Discard")
	}
}

// The draft file is named beside the registry, so a console pointed at its own
// state directory keeps its draft there too.
func TestDefaultConnectDraftPathIsASiblingOfTheRegistry(t *testing.T) {
	got := DefaultConnectDraftPath("/var/lib/bintrail/console-servers.yaml")
	if want := "/var/lib/bintrail/console-connect-draft.yaml"; got != want {
		t.Errorf("DefaultConnectDraftPath = %q, want %q", got, want)
	}
	// An empty registry path means an in-memory registry: no file for it to be
	// a sibling of, and nothing must be written into the working directory.
	if got := DefaultConnectDraftPath(""); got != "" {
		t.Errorf("DefaultConnectDraftPath(\"\") = %q, want \"\"", got)
	}
}

// Config.Registry is optional, so a nil registry is a real shape here: asking
// it where it lives must answer "nowhere", not crash. It did crash — every
// caller that builds a console without a registry panicked in New.
func TestRegistryPathIsNilSafe(t *testing.T) {
	var r *Registry
	if got := r.Path(); got != "" {
		t.Errorf("(*Registry)(nil).Path() = %q, want \"\"", got)
	}
	srv, err := New(Config{Listen: "127.0.0.1:8090", Token: "t"})
	if err != nil {
		t.Fatalf("a console with no registry: %v", err)
	}
	if srv.drafts == nil {
		t.Fatal("the draft store is nil, so every Connect call would crash")
	}
	if err := srv.drafts.Save(ConnectDraft{SourceHost: "db"}); err != nil {
		t.Errorf("saving a draft with no registry: %v", err)
	}
}

// ─── over HTTP ───────────────────────────────────────────────────────────────

func TestDraftEndpointRoundTrip(t *testing.T) {
	srv, _ := newSupervisorServer(t)
	rec, body := doServersReq(t, srv, "GET", "/api/servers/draft", "")
	if rec.Code != 200 || !strings.Contains(string(body), `"found":false`) {
		t.Fatalf("GET with nothing saved: code=%d body=%s", rec.Code, body)
	}
	// And no draft object at all beside it: a screen handed an object of
	// blanks could restore it over what somebody is typing.
	if strings.Contains(string(body), `"draft"`) {
		t.Errorf("nothing is saved, yet the answer carries a draft: %s", body)
	}
	rec, body = doServersReq(t, srv, "PUT", "/api/servers/draft",
		`{"name":"prod","flavor":"mysql","source_host":"db.example.com","source_port":"3307","source_user":"dbtrail","source_password":"Ab3-xyz","schemas":"shop"}`)
	if rec.Code != 200 {
		t.Fatalf("PUT: code=%d body=%s", rec.Code, body)
	}
	rec, body = doServersReq(t, srv, "GET", "/api/servers/draft", "")
	if rec.Code != 200 {
		t.Fatalf("GET: code=%d body=%s", rec.Code, body)
	}
	for _, want := range []string{`"found":true`, `"source_host":"db.example.com"`, `"source_port":"3307"`} {
		if !strings.Contains(string(body), want) {
			t.Errorf("GET body missing %s: %s", want, body)
		}
	}
	if rec, body := doServersReq(t, srv, "DELETE", "/api/servers/draft", ""); rec.Code != 204 {
		t.Fatalf("DELETE: code=%d body=%s", rec.Code, body)
	}
	if _, body := doServersReq(t, srv, "GET", "/api/servers/draft", ""); !strings.Contains(string(body), `"found":false`) || strings.Contains(string(body), `"draft"`) {
		t.Errorf("the draft survived DELETE: %s", body)
	}
}

// A draft is not a server. It must never appear in the list, or somebody would
// try to open a server that does not exist.
func TestDraftIsNeverListedAsAServer(t *testing.T) {
	srv, _ := newSupervisorServer(t)
	if rec, body := doServersReq(t, srv, "PUT", "/api/servers/draft", `{"name":"ghost","source_host":"db.example.com","source_user":"u","source_password":"p"}`); rec.Code != 200 {
		t.Fatalf("PUT: code=%d body=%s", rec.Code, body)
	}
	rec, body := doServersReq(t, srv, "GET", "/api/servers", "")
	if rec.Code != 200 {
		t.Fatalf("list: code=%d body=%s", rec.Code, body)
	}
	if strings.Contains(string(body), "ghost") || strings.Contains(string(body), "db.example.com") {
		t.Errorf("the draft leaked into the server list: %s", body)
	}
	if srv.cm.reg.Len() != 0 {
		t.Errorf("saving a draft created %d registry entries", srv.cm.reg.Len())
	}
}

// Reading the draft is not a read-tier action: it is classified with creating
// a server, not with listing one.
func TestDraftRoutesAreWriteTier(t *testing.T) {
	for _, m := range []struct{ method, path string }{
		{"GET", "/api/servers/draft"},
		{"PUT", "/api/servers/draft"},
		{"DELETE", "/api/servers/draft"},
	} {
		perm, ok := permForRoute(m.method, m.path)
		if !ok {
			t.Errorf("%s %s is not classified", m.method, m.path)
			continue
		}
		// Compared against the permission itself, not against the constant
		// the routes are declared with: that would assert the constant equals
		// itself and stay green if the whole tier were lowered to a read.
		if perm != ext.PermServersWrite {
			t.Errorf("%s %s requires %q, want %q ; a read-tier session must not read a half-filled Connect form",
				m.method, m.path, perm, ext.PermServersWrite)
		}
		if perm == ext.PermServersRead {
			t.Errorf("%s %s is readable by a session that may only list servers", m.method, m.path)
		}
	}
}
