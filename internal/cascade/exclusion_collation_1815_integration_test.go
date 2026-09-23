//go:build integration

package cascade_test

import (
	"context"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/cascade"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// #1815, the reader's side of the seam: the ChildExcludedFromSnapshot join
// compares snapshot_exclusions against fk_constraints by name. Under an
// insensitive key an excluded `Nopk_child` also flagged its captured twin
// `nopk_child`, a false "provably partial" on a recovery that is complete.
// With the binary name columns the join names only the excluded table.
func TestChildExcludedFlagsOnlyTheExactTable_1815(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()
	sourceDB, sourceName := testutil.CreateTestDB(t)
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)

	var lctn int
	if err := sourceDB.QueryRow("SELECT @@lower_case_table_names").Scan(&lctn); err != nil {
		t.Fatal(err)
	}
	// The accent pair exists under every lower_case_table_names; the case
	// pair only where the server keeps case (0, the Linux default).
	excluded, captured := "nopk_chïld", "nopk_child"
	if lctn == 0 {
		excluded = "Nopk_child"
	}

	testutil.MustExec(t, sourceDB, `CREATE TABLE parent (id INT PRIMARY KEY) ENGINE=InnoDB`)
	testutil.MustExec(t, sourceDB, "CREATE TABLE `"+excluded+"` ("+`
		pid INT,
		CONSTRAINT fk_a FOREIGN KEY (pid) REFERENCES parent(id) ON DELETE CASCADE
	) ENGINE=InnoDB`)
	testutil.MustExec(t, sourceDB, "CREATE TABLE `"+captured+"` ("+`
		id INT PRIMARY KEY, pid INT,
		CONSTRAINT fk_b FOREIGN KEY (pid) REFERENCES parent(id) ON DELETE CASCADE
	) ENGINE=InnoDB`)

	if _, err := metadata.TakeSnapshotExcludingInvalid(sourceDB, indexDB, []string{sourceName}); err != nil {
		t.Fatalf("TakeSnapshotExcludingInvalid: %v", err)
	}
	fks, _, _, err := cascade.LoadCascadeFKsForParent(ctx, indexDB, sourceName, time.Now())
	if err != nil {
		t.Fatalf("LoadCascadeFKsForParent: %v", err)
	}
	seen := map[string]bool{}
	for _, fk := range fks {
		seen[fk.Table] = true
		switch fk.Table {
		case excluded:
			if !fk.ChildExcludedFromSnapshot {
				t.Errorf("%s was excluded and must be flagged", fk.Table)
			}
		case captured:
			if fk.ChildExcludedFromSnapshot {
				t.Errorf("%s was captured and must not be flagged because its twin %s was excluded", fk.Table, excluded)
			}
		}
	}
	if !seen[excluded] || !seen[captured] {
		t.Fatalf("both edges must load, got tables %v", seen)
	}
}
