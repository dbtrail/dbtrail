package console

import (
	"strings"
	"testing"
)

// The name a new server gets when nobody typed one is derived from the address
// of the database it reads. These cases are the ones that decide whether that
// name is usable: what the person typed in the host box is not always a bare
// hostname, and the name it produces ends up in the switcher, in the YAML file
// and in every log line about that server.

func TestDeriveServerName(t *testing.T) {
	cases := []struct {
		what, host, port, flavor, want string
	}{
		{"a plain host is the name", "db.example.com", "", FlavorMySQL, "db.example.com"},
		{"the usual port is left out", "db.example.com", "3306", FlavorMySQL, "db.example.com"},
		{"an unusual port is part of the name", "db.example.com", "3307", FlavorMySQL, "db.example.com-3307"},
		{"a port typed into the host box", "db:3307", "", FlavorMySQL, "db-3307"},
		{"the usual port typed into the host box is still left out", "db:3306", "", FlavorMySQL, "db"},
		{"the port box wins over a port typed into the host box", "db:3307", "3308", FlavorMySQL, "db-3308"},
		{"spaces around the values", "  db.example.com  ", "  3307 ", FlavorMySQL, "db.example.com-3307"},
		{"capitals fold down", "DB.Example.COM", "", FlavorMySQL, "db.example.com"},
		{"a trailing dot is dropped", "db.example.com.", "", FlavorMySQL, "db.example.com"},
		{"an address keeps its dots", "127.0.0.1", "", FlavorMySQL, "127.0.0.1"},
		{"localhost is a name like any other", "localhost", "", FlavorMySQL, "localhost"},
		{"a v6 address in brackets", "[::1]", "", FlavorMySQL, "ipv6-1"},
		{"a bare v6 address", "2001:db8::1", "", FlavorMySQL, "ipv6-2001-db8-1"},
		{"a v6 address with a port", "[::1]:3307", "", FlavorMySQL, "ipv6-1-3307"},
		{"a v6 address with the usual port", "[::1]:3306", "", FlavorMySQL, "ipv6-1"},
		{"a v6 address with no digits left after folding", "::", "", FlavorMySQL, "ipv6"},
		{"PostgreSQL leaves out its own usual port", "pg.example.com", "5432", FlavorPostgres, "pg.example.com"},
		{"PostgreSQL keeps an unusual port", "pg.example.com", "5433", FlavorPostgres, "pg.example.com-5433"},
		{"MySQL's usual port is unusual for PostgreSQL", "pg.example.com", "3306", FlavorPostgres, "pg.example.com-3306"},
		{"MariaDB shares MySQL's usual port", "maria.example.com", "3306", FlavorMariaDB, "maria.example.com"},
		{"a port that is not a number is left out", "db", "abc", FlavorMySQL, "db"},
		{"port zero is left out", "db", "0", FlavorMySQL, "db"},
		{"a port past the end of the range is left out", "db", "70000", FlavorMySQL, "db"},
		{"a negative port is left out", "db", "-1", FlavorMySQL, "db"},
		{"characters a name cannot carry become dashes", "db/prod space", "", FlavorMySQL, "db-prod-space"},
		{"runs of them collapse into one", "db///prod", "", FlavorMySQL, "db-prod"},
		{"accents are not carried through", "pról.db", "", FlavorMySQL, "pr-l.db"},
		{"nothing typed derives nothing", "", "3306", FlavorMySQL, ""},
		{"only spaces derives nothing", "   ", "", FlavorMySQL, ""},
		{"something typed that has no usable characters still gets a name", "///", "", FlavorMySQL, "database"},
		{"a host that is only dots still gets a name", "...", "", FlavorMySQL, "database"},
	}
	for _, c := range cases {
		t.Run(c.what, func(t *testing.T) {
			if got := DeriveServerName(c.host, c.port, c.flavor); got != c.want {
				t.Errorf("DeriveServerName(%q, %q, %q) = %q, want %q", c.host, c.port, c.flavor, got, c.want)
			}
		})
	}
}

// A very long host must not become a very long name: the label is shown in a
// switcher and written into a YAML file. Two hosts sharing a long prefix
// therefore derive the SAME name, which is what the uniqueness rule is for —
// see TestAddAutoNamedTruncatedHostsStayDistinct.
func TestDeriveServerNameCapsTheLength(t *testing.T) {
	long := strings.Repeat("a", 200) + ".example.com"
	got := DeriveServerName(long, "3307", FlavorMySQL)
	if len(got) > serverNameMaxLen+len("-3307") {
		t.Errorf("derived name is %d characters: %q", len(got), got)
	}
	if !strings.HasSuffix(got, "-3307") {
		t.Errorf("the port was cut off the end: %q", got)
	}
	if strings.HasSuffix(strings.TrimSuffix(got, "-3307"), "-") {
		t.Errorf("the cut left a dangling separator: %q", got)
	}
}

// The cut must not leave a trailing dot or dash either, whatever character the
// host happens to have at that position.
func TestDeriveServerNameCutNeverEndsOnASeparator(t *testing.T) {
	for _, tail := range []string{".", "-", "..", "--", ".-"} {
		host := strings.Repeat("a", serverNameMaxLen-len(tail)) + tail + "more.example.com"
		got := DeriveServerName(host, "", FlavorMySQL)
		if strings.HasSuffix(got, ".") || strings.HasSuffix(got, "-") {
			t.Errorf("host ending the cut on %q derived %q, which ends on a separator", tail, got)
		}
	}
}

// ─── uniqueness ──────────────────────────────────────────────────────────────

// Two servers on the same host must end up with different names: the name is
// the label the switcher shows, and the registry refuses a duplicate outright.
func TestAddAutoNamedMakesTheNameUnique(t *testing.T) {
	r, _ := LoadRegistry("")
	for i, want := range []string{"db.example.com", "db.example.com-2", "db.example.com-3"} {
		e, err := r.AddAutoNamed(ServerEntry{DSN: "u:p@tcp(h:3306)/x"}, "db.example.com")
		if err != nil {
			t.Fatalf("add %d: %v", i, err)
		}
		if e.Name != want {
			t.Errorf("add %d named %q, want %q", i, e.Name, want)
		}
	}
}

// The suffix must skip a name somebody typed by hand, not collide with it.
func TestAddAutoNamedSkipsAHandTypedSuffix(t *testing.T) {
	r, _ := LoadRegistry("")
	if _, err := r.Add(ServerEntry{Name: "db-2", DSN: "u:p@tcp(h:3306)/x"}); err != nil {
		t.Fatal(err)
	}
	first, err := r.AddAutoNamed(ServerEntry{DSN: "u:p@tcp(h:3306)/x"}, "db")
	if err != nil {
		t.Fatal(err)
	}
	if first.Name != "db" {
		t.Fatalf("first derived name = %q, want db", first.Name)
	}
	second, err := r.AddAutoNamed(ServerEntry{DSN: "u:p@tcp(h:3306)/x"}, "db")
	if err != nil {
		t.Fatal(err)
	}
	if second.Name != "db-3" {
		t.Errorf("second derived name = %q, want db-3 (db-2 was typed by hand)", second.Name)
	}
}

// "default" is the reserved name of the command-line server, so a host called
// that must not derive it — the registry refuses it and the switcher would
// show two entries with one label.
func TestAddAutoNamedNeverTakesTheReservedName(t *testing.T) {
	r, _ := LoadRegistry("")
	e, err := r.AddAutoNamed(ServerEntry{DSN: "u:p@tcp(h:3306)/x"}, bootServerID)
	if err != nil {
		t.Fatalf("add: %v", err)
	}
	if e.Name == bootServerID {
		t.Fatalf("derived the reserved name %q", bootServerID)
	}
	if e.Name != bootServerID+"-2" {
		t.Errorf("derived %q, want %q", e.Name, bootServerID+"-2")
	}
}

// Two long hosts sharing a prefix derive one truncated name; the entries must
// still be distinct.
func TestAddAutoNamedTruncatedHostsStayDistinct(t *testing.T) {
	r, _ := LoadRegistry("")
	prefix := strings.Repeat("a", serverNameMaxLen+10)
	seen := map[string]bool{}
	for _, suffix := range []string{"one.example.com", "two.example.com", "three.example.com"} {
		base := DeriveServerName(prefix+suffix, "", FlavorMySQL)
		e, err := r.AddAutoNamed(ServerEntry{DSN: "u:p@tcp(h:3306)/x"}, base)
		if err != nil {
			t.Fatalf("add %s: %v", suffix, err)
		}
		if seen[e.Name] {
			t.Fatalf("two entries named %q", e.Name)
		}
		seen[e.Name] = true
	}
	if len(seen) != 3 {
		t.Errorf("got %d distinct names, want 3", len(seen))
	}
}

// A name the caller DID supply is used as-is: deriving one is what happens
// when nobody typed one, never a rename of what somebody chose.
func TestAddAutoNamedKeepsATypedName(t *testing.T) {
	r, _ := LoadRegistry("")
	e, err := r.AddAutoNamed(ServerEntry{Name: "chosen", DSN: "u:p@tcp(h:3306)/x"}, "db.example.com")
	if err != nil {
		t.Fatal(err)
	}
	if e.Name != "chosen" {
		t.Errorf("name = %q, want chosen", e.Name)
	}
	// And a typed duplicate is still refused, rather than silently suffixed.
	if _, err := r.AddAutoNamed(ServerEntry{Name: "chosen", DSN: "u:p@tcp(h:3306)/x"}, "db.example.com"); err == nil {
		t.Error("a typed duplicate name was accepted")
	}
}

// With no name and nothing to derive one from, the registry's own rule still
// holds: an entry always has a name.
func TestAddAutoNamedWithNoBaseIsRefused(t *testing.T) {
	r, _ := LoadRegistry("")
	if _, err := r.AddAutoNamed(ServerEntry{DSN: "u:p@tcp(h:3306)/x"}, ""); err == nil {
		t.Error("an entry with no name and no base was accepted")
	}
}
