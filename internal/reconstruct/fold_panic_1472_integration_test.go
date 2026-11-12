//go:build integration

package reconstruct

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestReconstructTables_containsAPanicInOneTablesFold is the child-goroutine
// half of #1472.
//
// recover() is per-goroutine. consoleapp guards its four baseline job
// goroutines, but the fold fans out one goroutine PER TABLE from here, so a
// panic in a table's own fold ends the process no matter how well the parent
// is guarded. Under `bintrail-console watch` that process is also the capture
// plane, so it is an outage, and it is the surface the issue named: this fold
// runs DuckDB over a customer's Parquet with column types from their own
// CREATE TABLE.
//
// This test lives at the integration tier because the fan-out is only
// reachable with a live index and a schema snapshot. Without the guard it
// kills the test binary, which is what the panic does to the daemon.
//
// Two tables, one of them panicking, so the assertions can tell containment
// from a run that simply aborted:
//
//   - the surviving table still folds, and is still reported;
//   - the panicking table is reported as a FAILURE carrying the panic value,
//     not silently dropped;
//   - the run as a whole fails, so nothing downstream publishes.
func TestReconstructTables_containsAPanicInOneTablesFold(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()

	db, dbName := testutil.CreateTestDB(t)
	if err := indexer.CreateIndexTables(ctx, db, 48, false, nil); err != nil {
		t.Fatalf("CreateIndexTables: %v", err)
	}
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	dsn := testutil.BaseDSN() + "/" + dbName

	const schema = "shop"
	at := time.Now().UTC().Truncate(time.Hour)
	ts := at.Format("2006-01-02 15:04:05")
	for _, table := range []string{"orders", "customers"} {
		testutil.InsertSnapshot(t, db, 1, ts, schema, table, "id", 1, "PRI", "int", "NO")
		testutil.InsertSnapshot(t, db, 1, ts, schema, table, "status", 2, "", "varchar", "YES")
	}

	const sentinel = "induced panic folding one table"
	prev := foldOneTable
	// Deferred so a t.Fatalf below cannot leave the seam replaced for the rest
	// of the package's run.
	defer func() { foldOneTable = prev }()
	foldOneTable = func(ctx context.Context, cfg FullTableConfig, sch, table string,
		db *sql.DB, engine *query.Engine, archSources []string,
		resolver *metadata.Resolver, dbName string) (*TableReport, error) {
		if table == "orders" {
			panic(sentinel)
		}
		return prev(ctx, cfg, sch, table, db, engine, archSources, resolver, dbName)
	}

	out := t.TempDir()
	reports, failures, err := ReconstructTablesDetailed(ctx, FullTableConfig{
		IndexDSN:     dsn,
		BaselineSrc:  t.TempDir(), // no snapshot: the surviving table takes the binlog-only path
		Tables:       []string{schema + ".orders", schema + ".customers"},
		At:           at,
		OutputDir:    out,
		OutputFormat: OutputFormatMydumper,
		Parallelism:  2,
		AllowGaps:    true, // an empty index must not refuse before the fold runs
		// The surviving table must fold for real, not be refused up front.
		AllowChangesOnly: true,
	})

	// 1. The run failed. A panicked table must never be folded into a run that
	//    goes on to publish.
	if err == nil {
		t.Fatal("ReconstructTablesDetailed returned nil error; a table that crashed must fail the run")
	}
	if !strings.Contains(err.Error(), sentinel) {
		t.Errorf("run error = %v, want it to carry the panic %q", err, sentinel)
	}

	// 2. The panic is reported as that table's own failure, so a caller that
	//    reports per-table refusals names the right table. Asserting on the
	//    sentinel is what stops this passing for the unrelated reason that the
	//    table was never attempted.
	var found bool
	for _, f := range failures {
		if f.Table == "orders" {
			found = true
			if f.Err == nil || !strings.Contains(f.Err.Error(), sentinel) {
				t.Errorf("failure for orders = %v, want it to carry the panic %q", f.Err, sentinel)
			}
		}
	}
	if !found {
		t.Errorf("orders is missing from failures %+v; its crash was not reported anywhere per-table", failures)
	}

	// 3. One table's crash does not stop its siblings.
	for _, rep := range reports {
		if rep != nil && rep.Table == "orders" {
			t.Error("orders produced a report; a table that panicked published nothing")
		}
	}
	if len(reports) != 1 || (len(reports) == 1 && reports[0].Table != "customers") {
		t.Errorf("reports = %+v, want only customers: a sibling table must still fold", reports)
	}
}
