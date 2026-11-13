package console

import (
	"context"
	"database/sql"
	"net/http"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

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
		idx  func() string
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
		// The index of the server on the command line never decides for a
		// registry entry.
		{"a MySQL entry beside a PostgreSQL boot index", "my", flavor("postgres"), get, false},
		{"a PostgreSQL entry beside a MySQL boot index", "pg", flavor("mysql"), get, true},
		{"the boot server, its index stamped postgres", bootServerID, flavor("postgres"), get, true},
		{"the boot server, its index stamped mysql", bootServerID, flavor("mysql"), get, false},
		{"the boot server, its index stamped mariadb", bootServerID, flavor("mariadb"), get, false},
		{"the boot server, an index no stream wrote to", bootServerID, flavor(""), get, false},
		{"the boot server, no index to ask", bootServerID, nil, get, false},
		{"no server id reads as the boot server", "", flavor("postgres"), get, true},
		{"the boot server, a flavor this build does not know", bootServerID, flavor("oracle"), get, false},
	}
	for _, c := range cases {
		if got := sourceIsPostgres(c.id, c.idx, c.get); got != c.want {
			t.Errorf("%s: %v, want %v", c.name, got, c.want)
		}
	}
}

// The SQL card asks its runner for PostgreSQL's rules when the index of the
// server it reads says the source is PostgreSQL, and for nothing otherwise:
// the job as the runner received it.
func TestSQLAPI_postgresSourceRunsUnderPostgresRules(t *testing.T) {
	for _, c := range []struct {
		flavor string
		want   bool
	}{{"postgres", true}, {"mysql", false}, {"mariadb", false}, {"", false}} {
		f := newSQLFixture(t, &fakeSQLRunner{res: oneRowResult()}, true)
		f.mock.ExpectQuery("SELECT flavor FROM stream_state").
			WillReturnRows(sqlmock.NewRows([]string{"flavor"}).AddRow(c.flavor))
		w := f.post(t, `{"sql":"SELECT 1"}`)
		if w.Code != http.StatusOK {
			t.Fatalf("flavor %q: code=%d body=%s", c.flavor, w.Code, w.Body.String())
		}
		if got := f.runner.last(t).Session.PostgresRules; got != c.want {
			t.Errorf("flavor %q: PostgresRules = %v, want %v", c.flavor, got, c.want)
		}
	}
	// An index no stream has written to has no row: the MySQL session.
	f := newSQLFixture(t, &fakeSQLRunner{res: oneRowResult()}, true)
	f.mock.ExpectQuery("SELECT flavor FROM stream_state").WillReturnError(sql.ErrNoRows)
	if w := f.post(t, `{"sql":"SELECT 1"}`); w.Code != http.StatusOK {
		t.Fatalf("code=%d body=%s", w.Code, w.Body.String())
	}
	if f.runner.last(t).Session.PostgresRules {
		t.Error("an index with no stream row got PostgreSQL's rules")
	}
}

// The MCP sql tool takes the same path.
func TestMCPSQL_postgresSourceRunsUnderPostgresRules(t *testing.T) {
	for _, c := range []struct {
		flavor string
		want   bool
	}{{"postgres", true}, {"mysql", false}} {
		f := newSQLFixture(t, &fakeSQLRunner{res: oneRowResult()}, true)
		f.mock.ExpectQuery("SELECT flavor FROM stream_state").
			WillReturnRows(sqlmock.NewRows([]string{"flavor"}).AddRow(c.flavor))
		f.expectArchive()
		if _, err := f.s.mcpSQLRunner(f.s.cm.boot)(context.Background(), "SELECT 1", "", 10); err != nil {
			t.Fatalf("flavor %q: %v", c.flavor, err)
		}
		if got := f.runner.last(t).Session.PostgresRules; got != c.want {
			t.Errorf("flavor %q: PostgresRules = %v, want %v", c.flavor, got, c.want)
		}
	}
}

func TestSQLCopySession_fromTheBundlesServer(t *testing.T) {
	s := &Server{}
	if s.sqlCopySession(nil).PostgresRules {
		t.Error("no bundle got PostgreSQL's rules")
	}
	// The boot server with no index open, and a registry server with no
	// registry to ask: the MySQL session.
	if s.sqlCopySession(&bundle{serverID: bootServerID}).PostgresRules {
		t.Error("a boot server with no index got PostgreSQL's rules")
	}
	if s.sqlCopySession(&bundle{serverID: "x"}).PostgresRules {
		t.Error("a server that could not be looked up got PostgreSQL's rules")
	}
}
