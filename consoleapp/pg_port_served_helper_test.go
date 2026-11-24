package consoleapp

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/hiderules"
)

// addServedPG adds a PostgreSQL server the PostgreSQL port serves (0.140.0:
// only one that can forward): e with a source, when it has none, and the
// yes about forwarding as the account DBTrail captures with. For the tests
// of the port's login, whose servers had no source before; the source is
// never reached by them.
func addServedPG(t *testing.T, reg *console.Registry, e console.ServerEntry) (console.ServerEntry, error) {
	t.Helper()
	e.Flavor = console.FlavorPostgres
	if e.SourceDSN == "" {
		e.SourceDSN = "postgres://repl:pw@127.0.0.1:1/" + e.Name + "?sslmode=disable"
	}
	added, err := reg.Add(e)
	if err != nil {
		return added, err
	}
	return reg.SetForwardWithCaptureAccount(added.ID, true)
}

// servedPGYAML is the lines of a servers file that make a PostgreSQL server
// one the port serves.
const servedPGYAML = `    source_dsn: postgres://repl:pw@127.0.0.1:1/ledger?sslmode=disable
    forward_with_capture_account: true
    forward_with_capture_account_for: repl@127.0.0.1:1
`

// pgCopyOnlyHidden is a table no fixture has.
const pgCopyOnlyHidden = "public.bintrail_fixture_never"

// hideNothingThere makes server id hide a table nobody has: a server that
// hides tables or columns sends nothing to its source, so the copy answers
// every statement alone, the one way a server the port serves still does
// (0.140.0). For the tests of the copy's own answers and refusals.
func hideNothingThere(t *testing.T, reg *console.Registry, id string) console.ServerEntry {
	t.Helper()
	set, err := hiderules.Parse(hiderules.Rules{Tables: []string{pgCopyOnlyHidden}})
	if err != nil {
		t.Fatal(err)
	}
	e, err := reg.SetHidden(id, set)
	if err != nil {
		t.Fatal(err)
	}
	return e
}

// ensureFixtureRole makes, on the test PostgreSQL, the forwarding user the
// port fixtures give their servers (pgFixtureRole): it logs in and reads
// every table, nothing else.
func ensureFixtureRole(t *testing.T, ctx context.Context) {
	t.Helper()
	c, err := pgconn.Connect(ctx, os.Getenv("BINTRAIL_TEST_PG_DSN"))
	if err != nil {
		t.Fatalf("connect to the source: %v", err)
	}
	defer c.Close(context.Background())
	for _, stmt := range []string{
		"DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '" + pgFixtureRole + "') THEN CREATE ROLE " + pgFixtureRole + " LOGIN PASSWORD '" + pgFixturePass + "'; END IF; END $$",
		"GRANT pg_read_all_data TO " + pgFixtureRole,
	} {
		if _, err := c.Exec(ctx, stmt).ReadAll(); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
}

// pgFixtureRole is the forwarding user of a fixture's server: it can log in
// and read the fixture's table, nothing else.
const (
	pgFixtureRole = "bintrail_fixture_ro"
	pgFixturePass = "fixture-ro-pw"
)
