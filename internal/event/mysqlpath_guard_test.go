package event_test

import (
	"os/exec"
	"strings"
	"testing"
)

// TestMySQLPathDoesNotLinkPostgreSQL is the #2816 guard, the other direction
// of TestReadLayerDoesNotLinkGoMySQL: the packages a MySQL source is served
// through must not link anything that exists for PostgreSQL alone.
//
// Those packages are shared ground: the MySQL port and its routing, the copy
// (how a column is written, the views over it, the session a statement runs
// in), the index and what is read back from it. Work for PostgreSQL edits
// some of them on purpose, with a case of its own (a cast, a session rule).
// What it must never do is make them depend on PostgreSQL's wire, capture or
// snapshot code: a MySQL installation would then carry, and could be broken
// by, code no MySQL statement asks for. Every one is clean today; this keeps
// it so. A change that needs one of these imports here is a design decision
// to make on purpose, by editing this list.
func TestMySQLPathDoesNotLinkPostgreSQL(t *testing.T) {
	const module = "github.com/dbtrail/dbtrail/"
	banned := []string{
		"github.com/jackc/pgx", // the driver, and pgproto3 under it
		"github.com/jackc/pglogrepl",
		module + "internal/pgshim",
		module + "internal/pgcapture",
		module + "internal/pgstreamrun",
		module + "internal/pgbaseline",
		module + "internal/pgverifysource",
		module + "internal/pgcompare",
	}
	mysqlPath := []string{
		module + "internal/shim",       // the MySQL-protocol port
		module + "internal/readrouter", // Engine Routing
		module + "internal/sqlsandbox", // the session a statement on the copy runs in
		module + "internal/views",      // the views over the copy
		module + "internal/baseline",   // how the copy's columns are written and cast
		module + "internal/indexer",    // the index writer
		module + "internal/query",
		module + "internal/reconstruct",
	}
	// The guard can see what it looks for: a package that is PostgreSQL's
	// own does link the banned ones. Without this, a list that matched
	// nothing (a module path that changed, a library that moved) would pass
	// for ever.
	control, err := exec.Command("go", "list", "-deps", module+"internal/pgshim").CombinedOutput()
	if err != nil {
		t.Fatalf("go list -deps internal/pgshim: %v\n%s", err, control)
	}
	if !strings.Contains(string(control), "github.com/jackc/pgx") {
		t.Fatal("internal/pgshim does not appear to link github.com/jackc/pgx: this guard can no longer see what it bans")
	}
	for _, pkg := range mysqlPath {
		out, err := exec.Command("go", "list", "-deps", pkg).CombinedOutput()
		if err != nil {
			t.Fatalf("go list -deps %s: %v\n%s", pkg, err, out)
		}
		deps := strings.Split(strings.TrimSpace(string(out)), "\n")
		if len(deps) < 2 {
			t.Fatalf("go list -deps %s listed %d packages: nothing was checked", pkg, len(deps))
		}
		for _, line := range deps {
			dep := strings.TrimSpace(line)
			for _, b := range banned {
				if dep == b || strings.HasPrefix(dep, b+"/") {
					t.Errorf("%s links %s: a package on the MySQL path must not depend on code that exists for PostgreSQL alone", pkg, dep)
				}
			}
		}
	}
}
