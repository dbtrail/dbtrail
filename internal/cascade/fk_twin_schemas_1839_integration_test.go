//go:build integration

package cascade_test

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/cascade"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// #1839, the reader's side of the seam. fk_constraints.schema_name is exact
// now, and both loaders must still resolve a schema name as the user typed
// it, returning every twin under its stored name. The delete events arrive
// the same way (binlog_events matches the typed name insensitively), so a
// loader that dropped a twin would leave that twin's cascade victims
// unsynthesized with no caveat.
func TestFKLoadersResolveTypedNameToEveryTwin_1839(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()
	sourceDB, _ := testutil.CreateTestDB(t)
	indexDB, _ := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)

	var lctn int
	if err := sourceDB.QueryRow("SELECT @@lower_case_table_names").Scan(&lctn); err != nil {
		t.Fatal(err)
	}
	sfx := fmt.Sprintf("%d", time.Now().UnixNano()%1_000_000)
	// The accent pair exists under every lower_case_table_names; the case
	// pair only where the server keeps case (0, the Linux default).
	twins := []string{"cafe1839c_" + sfx, "café1839c_" + sfx}
	if lctn == 0 {
		twins = []string{"shop1839c_" + sfx, "Shop1839c_" + sfx}
	}
	root, err := sql.Open("mysql", testutil.BaseDSN()+"/?parseTime=true")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		for _, n := range twins {
			root.Exec("DROP DATABASE IF EXISTS `" + n + "`") //nolint:errcheck
		}
		root.Close()
	})
	for _, n := range twins {
		testutil.MustExec(t, root, "CREATE DATABASE `"+n+"`")
		testutil.MustExec(t, root, "CREATE TABLE `"+n+"`.parent (id INT PRIMARY KEY) ENGINE=InnoDB")
		testutil.MustExec(t, root, "CREATE TABLE `"+n+"`.child (id INT PRIMARY KEY, pid INT, "+
			"CONSTRAINT fk_x FOREIGN KEY (pid) REFERENCES parent (id) ON DELETE CASCADE) ENGINE=InnoDB")
	}
	if _, err := metadata.TakeSnapshot(sourceDB, indexDB, twins); err != nil {
		t.Fatalf("TakeSnapshot over twin schemas: %v", err)
	}
	want := append([]string(nil), twins...)
	sort.Strings(want)
	// A spelling that is neither twin byte for byte.
	typed := strings.ToUpper(strings.Replace(twins[0], "é", "e", 1))

	schemasOf := func(fks []cascade.CascadeFK) []string {
		var out []string
		for _, fk := range fks {
			out = append(out, fk.Schema)
		}
		sort.Strings(out)
		return out
	}

	byChild, err := cascade.LoadCascadeFKs(ctx, indexDB, []string{typed}, time.Now())
	if err != nil {
		t.Fatalf("LoadCascadeFKs(%q): %v", typed, err)
	}
	if got := schemasOf(byChild); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("LoadCascadeFKs(%q) schemas = %q, want %q", typed, got, want)
	}

	byParent, _, _, err := cascade.LoadCascadeFKsForParent(ctx, indexDB, typed, time.Now())
	if err != nil {
		t.Fatalf("LoadCascadeFKsForParent(%q): %v", typed, err)
	}
	if got := schemasOf(byParent); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("LoadCascadeFKsForParent(%q) schemas = %q, want %q", typed, got, want)
	}
}
