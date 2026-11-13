package console

import (
	"context"
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
	cases := []struct {
		name string
		id   string
		boot bool
		get  func(string) (ServerEntry, bool)
		want bool
	}{
		{"a PostgreSQL entry", "pg", false, get, true},
		{"its alias, any case, blanks around", "pgalias", false, get, true},
		{"a MySQL entry", "my", false, get, false},
		{"a MariaDB entry", "maria", false, get, false},
		{"an entry with no flavor", "blank", false, get, false},
		{"a flavor that does not normalize", "typo", false, get, false},
		{"an entry no longer in the registry", "gone", false, get, false},
		{"no registry", "pg", false, nil, false},
		// The server on the command line never decides for a registry entry.
		{"a MySQL entry beside a PostgreSQL boot server", "my", true, get, false},
		{"a PostgreSQL entry beside a MySQL boot server", "pg", false, get, true},
		{"the boot server, PostgreSQL", bootServerID, true, get, true},
		{"the boot server, not PostgreSQL", bootServerID, false, get, false},
		{"no server id reads as the boot server", "", true, nil, true},
	}
	for _, c := range cases {
		if got := sourceIsPostgres(c.id, c.boot, c.get); got != c.want {
			t.Errorf("%s: %v, want %v", c.name, got, c.want)
		}
	}
}

func TestFlavorIsPostgres(t *testing.T) {
	for f, want := range map[string]bool{
		"postgres": true, "postgresql": true, " PostgreSQL ": true,
		"": false, "mysql": false, "mariadb": false, "oracle": false, "postgre": false,
	} {
		if got := flavorIsPostgres(f); got != want {
			t.Errorf("flavorIsPostgres(%q) = %v, want %v", f, got, want)
		}
	}
}

// New reads the flavor of the command-line server's index once, at start.
func TestNew_bootServerReadsItsFlavorOnce(t *testing.T) {
	for _, c := range []struct {
		flavor string
		want   bool
	}{{"postgres", true}, {"mysql", false}} {
		db, mock, closeDB := newSQLMock(t)
		mock.MatchExpectationsInOrder(false)
		mock.ExpectQuery("SELECT flavor FROM stream_state").
			WillReturnRows(sqlmock.NewRows([]string{"flavor"}).AddRow(c.flavor))
		s, err := New(Config{DB: db, DBName: "bintrail_index", Token: "t"})
		if err != nil {
			closeDB()
			t.Fatal(err)
		}
		if got := s.cm.boot.pgSource; got != c.want {
			t.Errorf("flavor %q: pgSource = %v, want %v", c.flavor, got, c.want)
		}
		if got := s.sqlCopySession(s.cm.boot).PostgresRules; got != c.want {
			t.Errorf("flavor %q: PostgresRules = %v, want %v", c.flavor, got, c.want)
		}
		closeDB()
	}
}

// The SQL card asks its runner for PostgreSQL's rules when the index of the
// server it reads says the source is PostgreSQL, and for nothing otherwise:
// the job as the runner received it.
func TestSQLAPI_postgresSourceRunsUnderPostgresRules(t *testing.T) {
	for _, pg := range []bool{true, false} {
		f := newSQLFixture(t, &fakeSQLRunner{res: oneRowResult()}, true)
		f.s.cm.boot.pgSource = pg
		w := f.post(t, `{"sql":"SELECT 1"}`)
		if w.Code != http.StatusOK {
			t.Fatalf("pgSource %v: code=%d body=%s", pg, w.Code, w.Body.String())
		}
		if got := f.runner.last(t).Session.PostgresRules; got != pg {
			t.Errorf("pgSource %v: PostgresRules = %v", pg, got)
		}
	}
}

// The MCP sql tool takes the same path.
func TestMCPSQL_postgresSourceRunsUnderPostgresRules(t *testing.T) {
	for _, pg := range []bool{true, false} {
		f := newSQLFixture(t, &fakeSQLRunner{res: oneRowResult()}, true)
		f.s.cm.boot.pgSource = pg
		f.expectArchive()
		if _, err := f.s.mcpSQLRunner(f.s.cm.boot)(context.Background(), "SELECT 1", "", 10); err != nil {
			t.Fatalf("pgSource %v: %v", pg, err)
		}
		if got := f.runner.last(t).Session.PostgresRules; got != pg {
			t.Errorf("pgSource %v: PostgresRules = %v", pg, got)
		}
	}
}

func TestSQLCopySession_fromTheBundlesServer(t *testing.T) {
	s := &Server{}
	if s.sqlCopySession(nil).PostgresRules {
		t.Error("no bundle got PostgreSQL's rules")
	}
	if !s.sqlCopySession(&bundle{serverID: bootServerID, pgSource: true}).PostgresRules {
		t.Error("a PostgreSQL boot server did not get PostgreSQL's rules")
	}
	// A registry server with no registry to ask: the MySQL session, whatever
	// the field says.
	if s.sqlCopySession(&bundle{serverID: "x", pgSource: true}).PostgresRules {
		t.Error("a registry server took the boot server's answer")
	}
}
