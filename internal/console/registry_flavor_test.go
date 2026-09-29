package console

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// CorrectSourceFlavor replaces the saved Source type of a MySQL-family entry
// with the one the server reported, and touches nothing else.
func TestRegistryCorrectSourceFlavor(t *testing.T) {
	cases := []struct {
		name        string
		saved       string
		detected    string
		wantChanged bool
		wantSaved   string
		wantErr     bool
	}{
		{name: "mysql saved, mariadb reported", saved: "mysql", detected: "mariadb", wantChanged: true, wantSaved: "mariadb"},
		{name: "blank saved (old entry), mariadb reported", saved: "", detected: "mariadb", wantChanged: true, wantSaved: "mariadb"},
		{name: "mariadb saved, mysql reported", saved: "mariadb", detected: "mysql", wantChanged: true, wantSaved: "mysql"},
		{name: "already right", saved: "mariadb", detected: "mariadb", wantSaved: "mariadb"},
		{name: "blank saved, mysql reported: left blank", saved: "", detected: "mysql", wantSaved: ""},
		{name: "hand-edited case, same flavor", saved: " MariaDB ", detected: "mariadb", wantSaved: " MariaDB "},
		{name: "postgres saved is never touched", saved: "postgres", detected: "mariadb", wantSaved: "postgres"},
		{name: "reported postgres is refused", saved: "mysql", detected: "postgres", wantSaved: "mysql", wantErr: true},
		{name: "reported nothing is refused", saved: "mysql", detected: "", wantSaved: "mysql", wantErr: true},
		{name: "reported garbage is refused", saved: "mysql", detected: "oracle", wantSaved: "mysql", wantErr: true},
		{name: "reported with other case and spaces", saved: "mysql", detected: " MARIADB ", wantChanged: true, wantSaved: "mariadb"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r, path := tmpRegistry(t)
			added, err := r.Add(ServerEntry{Name: "src", DSN: "u:p@tcp(idx:3306)/i", SourceDSN: "u:p@tcp(src:3306)/", Flavor: tc.saved, BaselineDir: "/b"})
			if err != nil {
				t.Fatal(err)
			}
			changed, err := r.CorrectSourceFlavor(added.ID, tc.detected)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, want error %v", err, tc.wantErr)
			}
			if changed != tc.wantChanged {
				t.Errorf("changed = %v, want %v", changed, tc.wantChanged)
			}
			reloaded, err := LoadRegistry(path)
			if err != nil {
				t.Fatal(err)
			}
			got, _ := reloaded.Get(added.ID)
			if got.Flavor != tc.wantSaved {
				t.Errorf("saved flavor on disk = %q, want %q", got.Flavor, tc.wantSaved)
			}
			if got.Name != "src" || got.SourceDSN != added.SourceDSN || got.BaselineDir != "/b" {
				t.Errorf("other fields changed: %+v", got)
			}
		})
	}
}

func TestRegistryCorrectSourceFlavor_unknownServer(t *testing.T) {
	r, _ := tmpRegistry(t)
	if _, err := r.CorrectSourceFlavor("nope", FlavorMariaDB); !errors.Is(err, ErrUnknownServer) {
		t.Fatalf("err = %v, want ErrUnknownServer", err)
	}
}

func TestRegistryCorrectSourceFlavor_readOnly(t *testing.T) {
	path := filepath.Join(t.TempDir(), "console-servers.yaml")
	content := "version: 99\nservers:\n  - id: aaaa\n    name: future\n    index_dsn: u:p@tcp(h:3306)/db\n    flavor: mysql\n"
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	r, err := LoadRegistry(path)
	if err != nil {
		t.Fatal(err)
	}
	changed, err := r.CorrectSourceFlavor("aaaa", FlavorMariaDB)
	if !errors.Is(err, ErrRegistryReadOnly) || changed {
		t.Fatalf("changed=%v err=%v, want ErrRegistryReadOnly", changed, err)
	}
	if got, _ := r.Get("aaaa"); got.Flavor != FlavorMySQL {
		t.Errorf("in-memory flavor = %q, want it unchanged", got.Flavor)
	}
}

// A save that fails leaves the entry as it was, in memory too.
func TestRegistryCorrectSourceFlavor_saveFailureRollsBack(t *testing.T) {
	r, path := tmpRegistry(t)
	added, err := r.Add(ServerEntry{Name: "src", DSN: "u:p@tcp(idx:3306)/i", Flavor: FlavorMySQL})
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Dir(path)
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })
	changed, err := r.CorrectSourceFlavor(added.ID, FlavorMariaDB)
	if err == nil || changed {
		t.Fatalf("changed=%v err=%v, want a save error", changed, err)
	}
	if got, _ := r.Get(added.ID); got.Flavor != FlavorMySQL {
		t.Errorf("in-memory flavor = %q after a failed save, want mysql", got.Flavor)
	}
}

// An edit made from a copy read before the correction must not put the old
// Source type back: within MySQL and MariaDB the saved type is the
// registry's, and the edit form cannot change it anyway.
func TestRegistryUpdateKeepsACorrectedFlavor(t *testing.T) {
	r, path := tmpRegistry(t)
	added, err := r.Add(ServerEntry{Name: "src", DSN: "u:p@tcp(idx:3306)/i", Flavor: FlavorMySQL})
	if err != nil {
		t.Fatal(err)
	}
	stale, _ := r.Get(added.ID)
	if _, err := r.CorrectSourceFlavor(added.ID, FlavorMariaDB); err != nil {
		t.Fatal(err)
	}
	stale.BaselineDir = "/edited"
	if err := r.Update(stale); err != nil {
		t.Fatal(err)
	}
	reloaded, err := LoadRegistry(path)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := reloaded.Get(added.ID)
	if got.Flavor != FlavorMariaDB || got.BaselineDir != "/edited" {
		t.Errorf("after the stale edit: flavor %q, baseline dir %q; want mariadb and /edited", got.Flavor, got.BaselineDir)
	}
}

// Update still stores a flavor change that leaves the MySQL family: nothing
// in the registry decides that one, so it is not the registry's to keep.
func TestRegistryUpdateStillStoresAFlavorOutsideTheMySQLFamily(t *testing.T) {
	r, _ := tmpRegistry(t)
	added, err := r.Add(ServerEntry{Name: "src", DSN: "u:p@tcp(idx:3306)/i", Flavor: FlavorMySQL})
	if err != nil {
		t.Fatal(err)
	}
	added.Flavor = FlavorPostgres
	if err := r.Update(added); err != nil {
		t.Fatal(err)
	}
	if got, _ := r.Get(added.ID); got.Flavor != FlavorPostgres {
		t.Errorf("flavor = %q, want postgres", got.Flavor)
	}
}

// An edit form opened before capture corrected the Source type still sends
// the old MySQL-family value. That is not a request to change it: the edit is
// saved and the corrected type stays. A move to or from PostgreSQL is still
// refused (TestHandleServersUpdate_flavorImmutable).
func TestHandleServersUpdate_staleMySQLFamilyFlavorIsKept(t *testing.T) {
	srv := newRegistryServer(t)
	rec, resp := doServersReq(t, srv, "POST", "/api/servers",
		`{"name":"m1","host":"idx","user":"bt","password":"ipw","dbname":"binlog_index","flavor":"mysql"}`)
	if rec.Code != 201 {
		t.Fatalf("create code=%d body=%s", rec.Code, resp)
	}
	var dto serverDTO
	if err := json.Unmarshal(resp, &dto); err != nil {
		t.Fatal(err)
	}
	if _, err := srv.cm.reg.CorrectSourceFlavor(dto.ID, FlavorMariaDB); err != nil {
		t.Fatal(err)
	}
	rec, resp = doServersReq(t, srv, "PUT", "/api/servers/"+dto.ID,
		`{"name":"m1-renamed","host":"idx","user":"bt","dbname":"binlog_index","flavor":"mysql"}`)
	if rec.Code != 200 {
		t.Fatalf("stale form edit: code=%d, want 200 (body=%s)", rec.Code, resp)
	}
	got, _ := srv.cm.reg.Get(dto.ID)
	if got.Name != "m1-renamed" || got.SourceFlavor() != FlavorMariaDB {
		t.Errorf("after the edit: name %q flavor %q, want m1-renamed and mariadb", got.Name, got.Flavor)
	}
	rec, _ = doServersReq(t, srv, "PUT", "/api/servers/"+dto.ID,
		`{"name":"m1-renamed","host":"idx","user":"bt","dbname":"binlog_index","flavor":"postgres"}`)
	if rec.Code != 400 {
		t.Errorf("a move to postgres: code=%d, want 400", rec.Code)
	}
}
