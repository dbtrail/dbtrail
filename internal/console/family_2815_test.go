package console

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// One source family per installation (#2815).

func TestFamilyOfFlavor(t *testing.T) {
	for _, tc := range []struct{ flavor, want string }{
		{"", FamilyMySQL}, // a blank entry is MySQL, as SourceFlavor has it
		{"mysql", FamilyMySQL},
		{"MySQL", FamilyMySQL},
		{"mariadb", FamilyMySQL},
		{" MariaDB ", FamilyMySQL},
		{"postgres", FamilyPostgres},
		{"postgresql", FamilyPostgres},
		{" POSTGRES\n", FamilyPostgres},
		{"postgres ", FamilyPostgres},
		{"oracle", FamilyMySQL},  // not a flavor this build knows: a hand edit, read as SourceFlavor reads it
		{"postgre", FamilyMySQL}, // a prefix is not the name
		{"pg", FamilyMySQL},      // nor is a short form
		{"postgres/", FamilyMySQL},
	} {
		if got := FamilyOfFlavor(tc.flavor); got != tc.want {
			t.Errorf("FamilyOfFlavor(%q) = %q, want %q", tc.flavor, got, tc.want)
		}
		if got := (ServerEntry{Flavor: tc.flavor}).Family(); got != tc.want {
			t.Errorf("ServerEntry{Flavor: %q}.Family() = %q, want %q", tc.flavor, got, tc.want)
		}
	}
}

func familyTestEntry(name, flavor string) ServerEntry {
	return ServerEntry{Name: name, Flavor: flavor, DSN: "u:p@tcp(127.0.0.1:1)/idx_" + name}
}

// The first server fixes the family; one of the other is refused and leaves
// the registry and its file as they were.
func TestRegistryAdd_refusesTheOtherFamily(t *testing.T) {
	for _, tc := range []struct {
		name          string
		first, second string
		refused       bool
		installed     string
	}{
		{"mysql then mysql", "mysql", "mysql", false, ""},
		{"mysql then mariadb", "mysql", "mariadb", false, ""},
		{"mariadb then mysql", "mariadb", "", false, ""},
		{"blank then mariadb", "", "mariadb", false, ""},
		{"postgres then postgres", "postgres", "postgresql", false, ""},
		{"mysql then postgres", "mysql", "postgres", true, "MySQL and MariaDB"},
		{"blank then postgres", "", "postgres", true, "MySQL and MariaDB"},
		{"mariadb then postgres", "mariadb", "postgres", true, "MySQL and MariaDB"},
		{"postgres then mysql", "postgres", "mysql", true, "PostgreSQL"},
		{"postgres then blank", "postgres", "", true, "PostgreSQL"},
		{"postgres then mariadb", "postgres", "mariadb", true, "PostgreSQL"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "servers.yaml")
			reg, err := LoadRegistry(path)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := reg.Add(familyTestEntry("one", tc.first)); err != nil {
				t.Fatalf("the first server: %v", err)
			}
			before, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			_, err = reg.Add(familyTestEntry("two", tc.second))
			if !tc.refused {
				if err != nil {
					t.Fatalf("refused: %v", err)
				}
				return
			}
			var of *OtherFamilyError
			if !errors.As(err, &of) || !errors.Is(err, ErrOtherFamily) {
				t.Fatalf("err = %v, want the other-family refusal", err)
			}
			msg := err.Error()
			for _, want := range []string{"This installation serves " + tc.installed + " servers", "Nothing was changed", "a second installation", "one database family"} {
				if !strings.Contains(msg, want) {
					t.Errorf("the refusal does not say %q:\n%s", want, msg)
				}
			}
			if reg.Len() != 1 {
				t.Errorf("%d servers after a refused add, want 1", reg.Len())
			}
			after, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if string(after) != string(before) {
				t.Error("the servers file changed on a refused add")
			}
		})
	}
}

// The family is that of the servers there are, not a setting: with the last
// one removed, the other family can be added.
func TestRegistryAdd_theFamilyIsTheServersThereAre(t *testing.T) {
	reg, err := LoadRegistry(filepath.Join(t.TempDir(), "servers.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	pg, err := reg.Add(familyTestEntry("pg", "postgres"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reg.Add(familyTestEntry("my", "mysql")); !errors.Is(err, ErrOtherFamily) {
		t.Fatalf("a MySQL server beside a PostgreSQL one: %v", err)
	}
	if err := reg.Delete(pg.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := reg.Add(familyTestEntry("my", "mysql")); err != nil {
		t.Fatalf("a MySQL server in a registry with no server left: %v", err)
	}
	if _, err := reg.Add(familyTestEntry("pg2", "postgres")); !errors.Is(err, ErrOtherFamily) {
		t.Fatalf("a PostgreSQL server beside a MySQL one: %v", err)
	}
}

// mixedRegistryFile writes a servers file that holds both families, the way
// only a hand edit (or a file from before this rule) can.
func mixedRegistryFile(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "console-servers.yaml")
	body := `version: 1
servers:
  - id: aaaa000000000001
    name: shop
    index_dsn: u:p@tcp(127.0.0.1:1)/idx_shop
  - id: aaaa000000000002
    name: ledger
    flavor: postgres
    index_dsn: u:p@tcp(127.0.0.1:1)/idx_ledger
  - id: aaaa000000000003
    name: wiki
    flavor: mariadb
    index_dsn: u:p@tcp(127.0.0.1:1)/idx_wiki
`
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// A file that already holds both families is read whole: nothing refuses to
// start. Adding is refused for either family, naming the servers; removing
// one family ends it.
func TestRegistry_aMixedFileIsReadAndTakesNoNewServer(t *testing.T) {
	path := mixedRegistryFile(t)
	reg, err := LoadRegistry(path)
	if err != nil {
		t.Fatalf("a mixed file stopped the registry from loading: %v", err)
	}
	if reg.Len() != 3 {
		t.Fatalf("%d servers read, want all 3", reg.Len())
	}
	postgres, mysql, mixed := reg.MixedFamilies()
	if !mixed || strings.Join(postgres, ",") != "ledger" || strings.Join(mysql, ",") != "shop,wiki" {
		t.Fatalf("MixedFamilies = %v %v %v", postgres, mysql, mixed)
	}
	for _, flavor := range []string{"mysql", "postgres", "mariadb", ""} {
		_, err := reg.Add(familyTestEntry("new", flavor))
		if !errors.Is(err, ErrOtherFamily) {
			t.Fatalf("a %q server was added to a mixed registry: %v", flavor, err)
		}
		msg := err.Error()
		for _, want := range []string{"both database families", "PostgreSQL: ledger", "MySQL and MariaDB: shop, wiki", "Nothing was changed", "Remove the servers of one family", "no server can be added"} {
			if !strings.Contains(msg, want) {
				t.Errorf("the refusal does not say %q:\n%s", want, msg)
			}
		}
	}
	if reg.Len() != 3 {
		t.Fatalf("%d servers after refused adds, want 3", reg.Len())
	}
	// What the program writes about a server on its own still goes through:
	// capture goes on for every server of a mixed file.
	if err := reg.UpdateWith("aaaa000000000002", func(cur ServerEntry) (ServerEntry, bool) {
		cur.NoArchive = !cur.NoArchive
		return cur, true
	}); err != nil {
		t.Fatalf("a write of the program's own was refused on a mixed registry: %v", err)
	}
	if changed, err := reg.CorrectSourceFlavor("aaaa000000000001", FlavorMariaDB); err != nil || !changed {
		t.Fatalf("MySQL corrected to MariaDB on a mixed registry: changed=%v err=%v", changed, err)
	}
	// One family removed: the registry takes a server of the one left.
	if err := reg.Delete("aaaa000000000002"); err != nil {
		t.Fatal(err)
	}
	if _, _, mixed := reg.MixedFamilies(); mixed {
		t.Fatal("still mixed with its only PostgreSQL server removed")
	}
	if _, err := reg.Add(familyTestEntry("new", "mysql")); err != nil {
		t.Fatalf("a MySQL server after the PostgreSQL one was removed: %v", err)
	}
	if _, err := reg.Add(familyTestEntry("pg", "postgres")); !errors.Is(err, ErrOtherFamily) {
		t.Fatalf("a PostgreSQL server now: %v", err)
	}
}

// MySQL corrected to MariaDB (capture found the server to be one) stays in
// the family, in a registry of one family too.
func TestRegistry_mysqlToMariaDBIsTheSameFamily(t *testing.T) {
	reg, err := LoadRegistry(filepath.Join(t.TempDir(), "servers.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	e, err := reg.Add(familyTestEntry("one", "mysql"))
	if err != nil {
		t.Fatal(err)
	}
	if changed, err := reg.CorrectSourceFlavor(e.ID, FlavorMariaDB); err != nil || !changed {
		t.Fatalf("changed=%v err=%v", changed, err)
	}
	if _, err := reg.Add(familyTestEntry("two", "mysql")); err != nil {
		t.Fatalf("a MySQL server beside one corrected to MariaDB: %v", err)
	}
}

// captureLog routes the default logger to a buffer for the test.
func captureLog(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(old) })
	return &buf
}

// A mixed file is said in the log where it is read, with the servers' names:
// that line is the only way its owner learns of it short of a refusal.
func TestRegistry_aMixedFileIsLoggedWhereItIsRead(t *testing.T) {
	buf := captureLog(t)
	if _, err := LoadRegistry(mixedRegistryFile(t)); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	for _, want := range []string{"level=ERROR", "both database families", "ledger", "shop, wiki", "no server can be added"} {
		if !strings.Contains(out, want) {
			t.Errorf("the log does not say %q:\n%s", want, out)
		}
	}
	// A file of one family says nothing.
	buf.Reset()
	path := filepath.Join(t.TempDir(), "servers.yaml")
	reg, err := LoadRegistry(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reg.Add(familyTestEntry("one", "mariadb")); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadRegistry(path); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(buf.String(), "famil") {
		t.Errorf("a file of one family was logged about:\n%s", buf.String())
	}
}

// A flavor this build does not know is read as MySQL, and the log says which
// server and what was written: without it a PostgreSQL server spelled "pgsql"
// shows up as a MySQL one nobody can find.
func TestRegistry_anUnknownFlavorIsNamedInTheLog(t *testing.T) {
	buf := captureLog(t)
	path := filepath.Join(t.TempDir(), "servers.yaml")
	body := `version: 1
servers:
  - id: aaaa000000000001
    name: ledger
    flavor: postgres
    index_dsn: u:p@tcp(127.0.0.1:1)/idx_ledger
  - id: aaaa000000000002
    name: books
    flavor: pgsql
    index_dsn: u:p@tcp(127.0.0.1:1)/idx_books
`
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	reg, err := LoadRegistry(path)
	if err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	for _, want := range []string{"does not know", `books (flavor \"pgsql\")`, "both database families"} {
		if !strings.Contains(out, want) {
			t.Errorf("the log does not say %q:\n%s", want, out)
		}
	}
	if _, mysql, mixed := reg.MixedFamilies(); !mixed || strings.Join(mysql, ",") != "books" {
		t.Fatalf("mixed=%v mysql=%v, want books read as MySQL", mixed, mysql)
	}
}

// The follower (the router's view of the file) reads a mixed file, says so
// when the file comes to hold both families, and does not say it again on
// every later change of the file: the daemon rewrites it for its own marks.
func TestRegistryFollower_saysAMixedFileOnce(t *testing.T) {
	buf := captureLog(t)
	path := filepath.Join(t.TempDir(), "console-servers.yaml")
	one := "version: 1\nservers:\n  - id: aaaa000000000001\n    name: shop\n    index_dsn: u:p@tcp(127.0.0.1:1)/idx_shop\n"
	if err := os.WriteFile(path, []byte(one), 0o600); err != nil {
		t.Fatal(err)
	}
	reg, err := LoadRegistryFollower(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reg.Reload(); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(buf.String(), "both database families") {
		t.Fatalf("said of a file of one family:\n%s", buf.String())
	}
	write := func(extra string) {
		t.Helper()
		body := one + "  - id: aaaa000000000002\n    name: ledger\n    flavor: postgres\n    index_dsn: u:p@tcp(127.0.0.1:1)/idx_ledger\n" + extra
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := reg.Reload(); err != nil {
			t.Fatalf("reload: %v", err)
		}
	}
	write("")
	if n := strings.Count(buf.String(), "both database families"); n != 1 {
		t.Fatalf("said %d times when the file came to hold both families, want once:\n%s", n, buf.String())
	}
	if reg.Len() != 2 {
		t.Fatalf("%d servers, want both", reg.Len())
	}
	// The file changes again, the families do not: nothing more is said.
	for i := range 3 {
		write(fmt.Sprintf("    no_archive: %v\n", i%2 == 0))
	}
	if n := strings.Count(buf.String(), "both database families"); n != 1 {
		t.Fatalf("said %d times over changes that left the families as they were, want once", n)
	}
	// One family is left, then the mix comes back: said again.
	if err := os.WriteFile(path, []byte(one), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := reg.Reload(); err != nil {
		t.Fatal(err)
	}
	write("")
	if n := strings.Count(buf.String(), "both database families"); n != 2 {
		t.Fatalf("said %d times after the mix came back, want twice", n)
	}
}

func familyRefusalText(t *testing.T, body []byte) string {
	t.Helper()
	var out struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("not a JSON error: %s", body)
	}
	return out.Error
}

// Through the API, which is what the web interface and `dbtrail server add`
// call: the refusal is a 409 whose text stands on its own, in one paragraph
// with no markup, and no folder was made for the server that was not added.
func TestServersAPI_refusesTheOtherFamily(t *testing.T) {
	srv := newRegistryServer(t)
	rec, body := doServersReq(t, srv, "POST", "/api/servers", `{"name":"shop","host":"h","user":"u","password":"p","dbname":"db"}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("the first server: %d %s", rec.Code, body)
	}
	req, _ := json.Marshal(map[string]any{
		"name": "ledger", "flavor": "postgres", "host": "h", "user": "u", "password": "p", "dbname": "db",
		// A folder that does not exist: refused for the family first, never
		// for the folder (which would be a 400).
		"baseline_dir": filepath.Join(t.TempDir(), "snapshots-of-a-server-never-added"),
	})
	rec, body = doServersReq(t, srv, "POST", "/api/servers", string(req))
	if rec.Code != http.StatusConflict {
		t.Fatalf("a PostgreSQL server beside a MySQL one: %d %s", rec.Code, body)
	}
	msg := familyRefusalText(t, body)
	for _, want := range []string{"This installation serves MySQL and MariaDB servers", "a PostgreSQL server cannot be added", "Nothing was changed", "a second installation"} {
		if !strings.Contains(msg, want) {
			t.Errorf("the refusal does not say %q:\n%s", want, msg)
		}
	}
	if strings.ContainsAny(msg, "\n\t`*<>") {
		t.Errorf("the refusal carries markup or line breaks a terminal would print as they are: %q", msg)
	}
	if len(msg) > 400 {
		t.Errorf("the refusal is %d characters: too long to read in a terminal", len(msg))
	}
	if srv.cm.reg.Len() != 1 {
		t.Errorf("%d servers, want 1", srv.cm.reg.Len())
	}
	// The same family still goes in.
	rec, body = doServersReq(t, srv, "POST", "/api/servers", `{"name":"wiki","flavor":"mariadb","host":"h","user":"u","password":"p","dbname":"db"}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("a MariaDB server beside a MySQL one: %d %s", rec.Code, body)
	}
}

// A registry that holds both families takes no new server, and says which
// servers are in the way. Editing one stays allowed: an edit cannot change a
// server's family, and it is how a rotated password is put right without
// removing a whole family first. Removing one family ends it.
func TestServersAPI_aMixedRegistryTakesEditsAndRemovalsButNoNewServer(t *testing.T) {
	clearStores(t)
	reg, err := LoadRegistry(mixedRegistryFile(t))
	if err != nil {
		t.Fatal(err)
	}
	srv, err := New(Config{Listen: "127.0.0.1:8090", Token: "t", Registry: reg})
	if err != nil {
		t.Fatalf("a mixed servers file stopped the console from starting: %v", err)
	}
	// Every server is listed: nothing is hidden or dropped.
	rec, body := doServersReq(t, srv, "GET", "/api/servers", "")
	if rec.Code != http.StatusOK || !strings.Contains(string(body), "shop") || !strings.Contains(string(body), "ledger") || !strings.Contains(string(body), "wiki") {
		t.Fatalf("list: %d %s", rec.Code, body)
	}
	// An add of either family is refused.
	for _, add := range []string{
		`{"name":"more","host":"h","user":"u","password":"p","dbname":"db"}`,
		`{"name":"more","flavor":"postgres","host":"h","user":"u","password":"p","dbname":"db"}`,
	} {
		rec, body = doServersReq(t, srv, "POST", "/api/servers", add)
		if rec.Code != http.StatusConflict {
			t.Fatalf("an add on a mixed registry: %d %s", rec.Code, body)
		}
		msg := familyRefusalText(t, body)
		if !strings.Contains(msg, "both database families") || !strings.Contains(msg, "ledger") || !strings.Contains(msg, "no server can be added") {
			t.Errorf("the refusal: %s", msg)
		}
		if strings.Contains(msg, "edited") {
			t.Errorf("the refusal says a server cannot be edited, and it can: %s", msg)
		}
	}
	// An edit goes in, on a server of each family: this is how a password
	// that was rotated is put right while the file is mixed.
	for _, id := range []string{"aaaa000000000001", "aaaa000000000002"} {
		for _, method := range []string{"PUT", "PATCH"} {
			rec, body = doServersReq(t, srv, method, "/api/servers/"+id, `{"name":"renamed-`+id[len(id)-1:]+`","host":"h","user":"u","password":"new-password","dbname":"db"}`)
			if rec.Code == http.StatusConflict || strings.Contains(string(body), "database famil") {
				t.Fatalf("%s %s on a mixed registry was refused for the families: %d %s", method, id, rec.Code, body)
			}
		}
	}
	if e, _ := reg.Get("aaaa000000000002"); e.Family() != FamilyPostgres {
		t.Fatalf("an edit changed a PostgreSQL server's family to %q", e.Family())
	}
	// An edit cannot move a server to the other family.
	rec, body = doServersReq(t, srv, "PATCH", "/api/servers/aaaa000000000001", `{"flavor":"postgres"}`)
	if e, _ := reg.Get("aaaa000000000001"); e.Family() != FamilyMySQL {
		t.Fatalf("an edit moved a MySQL server to PostgreSQL (%d %s)", rec.Code, body)
	}
	// Removing the PostgreSQL server is allowed, and an add then goes in.
	rec, body = doServersReq(t, srv, "DELETE", "/api/servers/aaaa000000000002", "")
	if rec.Code >= 300 {
		t.Fatalf("removing a server from a mixed registry: %d %s", rec.Code, body)
	}
	rec, body = doServersReq(t, srv, "POST", "/api/servers", `{"name":"more","host":"h","user":"u","password":"p","dbname":"db"}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("an add once one family is left: %d %s", rec.Code, body)
	}
}

// addByHand puts e in reg past the family rule, as a hand edit of the servers
// file does. For tests whose subject is one server's behaviour and that keep
// servers of both families in one registry for it: such a registry still
// serves every server (TestRegistry_aMixedFileIsReadAndTakesNoNewServer).
func addByHand(t *testing.T, reg *Registry, e ServerEntry) ServerEntry {
	t.Helper()
	reg.mu.Lock()
	defer reg.mu.Unlock()
	added, err := reg.insertLocked(e)
	if err != nil {
		t.Fatalf("add %s by hand: %v", e.Name, err)
	}
	return added
}

// The server given on the command line counts as the first one: the registry
// cannot see it, so the console's own check does.
func TestFamilyRefusal_theCommandLineServerCounts(t *testing.T) {
	for _, tc := range []struct {
		name    string
		servers []ServerEntry
		boot    string
		flavor  string
		want    string // in the refusal, "" for none
	}{
		{"nothing at all", nil, "", "postgres", ""},
		{"a MySQL boot server, a MySQL server", nil, FamilyMySQL, "mysql", ""},
		{"a MySQL boot server, a MariaDB server", nil, FamilyMySQL, "mariadb", ""},
		{"a MySQL boot server, a PostgreSQL server", nil, FamilyMySQL, "postgres", "This installation serves MySQL and MariaDB servers"},
		{"a PostgreSQL boot server, a MySQL server", nil, FamilyPostgres, "", "This installation serves PostgreSQL servers"},
		{"a PostgreSQL boot server, a PostgreSQL server", nil, FamilyPostgres, "postgresql", ""},
		{"a MySQL boot server beside PostgreSQL servers already", []ServerEntry{familyTestEntry("ledger", "postgres")}, FamilyMySQL, "postgres", "the server this was started with"},
		{"a MySQL boot server beside PostgreSQL servers, a MySQL one asked", []ServerEntry{familyTestEntry("ledger", "postgres")}, FamilyMySQL, "mysql", "both database families"},
	} {
		err := familyRefusal(tc.servers, tc.boot, tc.flavor)
		switch {
		case tc.want == "" && err != nil:
			t.Errorf("%s: refused: %v", tc.name, err)
		case tc.want != "" && (err == nil || !errors.Is(err, ErrOtherFamily) || !strings.Contains(err.Error(), tc.want)):
			t.Errorf("%s: err = %v, want a refusal saying %q", tc.name, err, tc.want)
		}
	}
}

func TestServersAPI_theCommandLineServerFixesTheFamily(t *testing.T) {
	for _, tc := range []struct {
		name     string
		bootIsPG bool
		body     string
		refused  bool
	}{
		{"a MySQL boot server refuses PostgreSQL", false, `{"name":"ledger","flavor":"postgres","host":"h","user":"u","password":"p","dbname":"db"}`, true},
		{"a MySQL boot server takes MariaDB", false, `{"name":"wiki","flavor":"mariadb","host":"h","user":"u","password":"p","dbname":"db"}`, false},
		{"a PostgreSQL boot server refuses MySQL", true, `{"name":"shop","host":"h","user":"u","password":"p","dbname":"db"}`, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := newRegistryServer(t)
			srv.cm.mu.Lock()
			srv.cm.boot = &bundle{pgSource: tc.bootIsPG}
			srv.cm.mu.Unlock()
			rec, body := doServersReq(t, srv, "POST", "/api/servers", tc.body)
			if tc.refused {
				if rec.Code != http.StatusConflict || !strings.Contains(familyRefusalText(t, body), "This installation serves") {
					t.Fatalf("%d %s", rec.Code, body)
				}
				if srv.cm.reg.Len() != 0 {
					t.Fatalf("%d servers after a refused add", srv.cm.reg.Len())
				}
				return
			}
			if rec.Code != http.StatusCreated {
				t.Fatalf("%d %s", rec.Code, body)
			}
		})
	}
}

// A registry with many servers of each family names a few and counts the
// rest: the refusal is still one line a terminal shows whole.
func TestOtherFamilyError_namesAFewAndCountsTheRest(t *testing.T) {
	var servers []ServerEntry
	for i := range 30 {
		servers = append(servers, familyTestEntry(fmt.Sprintf("my%02d", i), "mysql"), familyTestEntry(fmt.Sprintf("pg%02d", i), "postgres"))
	}
	msg := familyRefusal(servers, "", "mysql").Error()
	for _, want := range []string{"pg00, pg01, pg02, pg03 and 26 more", "my00, my01, my02, my03 and 26 more", "Nothing was changed"} {
		if !strings.Contains(msg, want) {
			t.Errorf("the refusal does not say %q:\n%s", want, msg)
		}
	}
	if len(msg) > 600 {
		t.Errorf("the refusal is %d characters for 60 servers", len(msg))
	}
}

// On the Connect step and from `dbtrail server add` (both POST
// /api/servers/check), the family is refused BEFORE the checks run against
// the source and before the form is kept: nobody fixes their database for a
// server that can never be added, and a refused form does not come back
// after a reload.
func TestCheck_refusesTheOtherFamilyBeforeTheChecks(t *testing.T) {
	srv, ctrl := newSupervisorServer(t)
	rec, body := doServersReq(t, srv, "POST", "/api/servers/check", checkBody)
	if rec.Code != 200 || !decodeCheck(t, body).Started {
		t.Fatalf("the first server (MySQL): %d %s", rec.Code, body)
	}
	checksBefore, startsBefore := len(ctrl.unsaved), len(ctrl.started)
	if err := srv.drafts.Discard(); err != nil {
		t.Fatal(err)
	}

	rec, body = doServersReq(t, srv, "POST", "/api/servers/check",
		`{"flavor":"postgres","source_host":"pg.example.com","source_user":"dbtrail","source_password":"Ab3-xyz","source_database":"app","source_slot":"s","source_publication":"p"}`)
	if rec.Code != http.StatusConflict {
		t.Fatalf("a PostgreSQL server on a MySQL installation: %d %s", rec.Code, body)
	}
	if msg := familyRefusalText(t, body); !strings.Contains(msg, "This installation serves MySQL and MariaDB servers") || !strings.Contains(msg, "Nothing was changed") {
		t.Errorf("the refusal: %s", msg)
	}
	if len(ctrl.unsaved) != checksBefore {
		t.Errorf("the checks ran against the source of a server that was refused (%d runs, %d before)", len(ctrl.unsaved), checksBefore)
	}
	if len(ctrl.started) != startsBefore {
		t.Errorf("capture was started for a refused server")
	}
	if srv.cm.reg.Len() != 1 {
		t.Errorf("%d servers, want 1", srv.cm.reg.Len())
	}
	if _, kept, err := srv.drafts.Load(); err != nil || kept {
		t.Errorf("the refused form was kept as a draft (kept=%v err=%v)", kept, err)
	}
}

// A command-line server of one family beside servers of the other in the
// file: the file alone holds one family, so reading it says nothing, and the
// console says it at start. The refusal then says what removes a server that
// is in no list.
func TestServer_theCommandLineServerOfAnotherFamilyIsSaidAtStart(t *testing.T) {
	srv := newRegistryServer(t)
	if _, err := srv.cm.reg.Add(familyTestEntry("ledger", "postgres")); err != nil {
		t.Fatal(err)
	}
	buf := captureLog(t)
	srv.warnBootFamily()
	if buf.Len() != 0 {
		t.Fatalf("said with no command-line server:\n%s", buf.String())
	}
	srv.cm.mu.Lock()
	srv.cm.boot = &bundle{pgSource: true}
	srv.cm.mu.Unlock()
	srv.warnBootFamily()
	if buf.Len() != 0 {
		t.Fatalf("said of a command-line server of the same family:\n%s", buf.String())
	}
	srv.cm.mu.Lock()
	srv.cm.boot = &bundle{}
	srv.cm.mu.Unlock()
	srv.warnBootFamily()
	out := buf.String()
	for _, want := range []string{"level=ERROR", "different database families", "ledger", "--index-dsn", "no server can be added"} {
		if !strings.Contains(out, want) {
			t.Errorf("the log does not say %q:\n%s", want, out)
		}
	}
	rec, body := doServersReq(t, srv, "POST", "/api/servers", `{"name":"more","flavor":"postgres","host":"h","user":"u","password":"p","dbname":"db"}`)
	if rec.Code != http.StatusConflict {
		t.Fatalf("%d %s", rec.Code, body)
	}
	msg := familyRefusalText(t, body)
	for _, want := range []string{"the server this was started with", "--index-dsn or --source-dsn", "starting DBTrail without it"} {
		if !strings.Contains(msg, want) {
			t.Errorf("the refusal does not say %q:\n%s", want, msg)
		}
	}
	// A hidden command-line server (a daemon started with no source) counts
	// for nothing: it captures nothing.
	srv.cm.mu.Lock()
	srv.cm.hideBoot = true
	srv.cm.mu.Unlock()
	rec, body = doServersReq(t, srv, "POST", "/api/servers", `{"name":"more","flavor":"postgres","host":"h","user":"u","password":"p","dbname":"db"}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("a PostgreSQL server beside PostgreSQL ones and a hidden command-line server: %d %s", rec.Code, body)
	}
}
