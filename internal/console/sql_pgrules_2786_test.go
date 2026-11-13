package console

import "testing"

// #2786: which servers get PostgreSQL's session rules for SQL on the copy.
func TestSourceIsPostgres(t *testing.T) {
	reg := map[string]ServerEntry{
		"pg":      {ID: "pg", Flavor: "postgres"},
		"pgalias": {ID: "pgalias", Flavor: " PostgreSQL "},
		"my":      {ID: "my", Flavor: "mysql"},
		"maria":   {ID: "maria", Flavor: "mariadb"},
		"blank":   {ID: "blank"},
		"typo":    {ID: "typo", Flavor: "postgre"},
	}
	get := func(id string) (ServerEntry, bool) { e, ok := reg[id]; return e, ok }
	flavor := func(f string) func() string { return func() string { return f } }
	cases := []struct {
		name string
		id   string
		boot func() string
		get  func(string) (ServerEntry, bool)
		want bool
	}{
		{"a PostgreSQL entry", "pg", nil, get, true},
		{"its alias, any case, blanks around", "pgalias", nil, get, true},
		{"a MySQL entry", "my", nil, get, false},
		{"a MariaDB entry", "maria", nil, get, false},
		{"an entry with no flavor", "blank", nil, get, false},
		{"a flavor that does not normalize", "typo", nil, get, false},
		{"an entry no longer in the registry", "gone", nil, get, false},
		{"no registry", "pg", nil, nil, false},
		// The boot server's flavor never decides for a registry entry.
		{"a MySQL entry beside a PostgreSQL boot server", "my", flavor("postgres"), get, false},
		{"a PostgreSQL entry beside a MySQL boot server", "pg", flavor("mysql"), get, true},
		{"the boot server, PostgreSQL", bootServerID, flavor("postgres"), get, true},
		{"the boot server, alias", bootServerID, flavor("postgresql"), get, true},
		{"the boot server, MySQL", bootServerID, flavor("mysql"), get, false},
		{"the boot server, nothing reported yet", bootServerID, flavor(""), get, false},
		{"the boot server, no reporter", bootServerID, nil, get, false},
		{"no server id reads as the boot server", "", flavor("postgres"), get, true},
		{"the boot server, an unknown flavor", bootServerID, flavor("oracle"), get, false},
	}
	for _, c := range cases {
		if got := sourceIsPostgres(c.id, c.boot, c.get); got != c.want {
			t.Errorf("%s: %v, want %v", c.name, got, c.want)
		}
	}
}

func TestSQLCopySession_fromTheBundlesServer(t *testing.T) {
	s := &Server{bootSourceFlavor: func() string { return "postgres" }}
	if s.sqlCopySession(nil).PostgresRules {
		t.Error("no bundle got PostgreSQL's rules")
	}
	if !s.sqlCopySession(&bundle{serverID: bootServerID}).PostgresRules {
		t.Error("the PostgreSQL boot server did not get PostgreSQL's rules")
	}
	// A registry server with no registry to ask: the MySQL session.
	if s.sqlCopySession(&bundle{serverID: "x"}).PostgresRules {
		t.Error("a server that could not be looked up got PostgreSQL's rules")
	}
}
