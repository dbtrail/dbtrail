//go:build integration

package status_test

import (
	"context"
	"slices"
	"testing"

	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/status"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// #1815 on the report's side: two excluded tables whose names differ only in
// case or accent are two entries on the uncaptured list. Before the fix the
// snapshot that should have recorded them failed outright, so neither
// appeared; a reader that folded names into one key would hide one.
func TestIntegrationLoadTableCapture_listsCaseAndAccentTwins_1815(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()
	source, src := testutil.CreateTestDB(t)
	index := uncapturedIndex(t)

	var lctn int
	if err := source.QueryRow("SELECT @@lower_case_table_names").Scan(&lctn); err != nil {
		t.Fatal(err)
	}
	twins := []string{"cafe", "café"}
	if lctn == 0 {
		twins = append(twins, "Audit_Log", "audit_log")
	}
	testutil.MustExec(t, source, "CREATE TABLE orders (id INT PRIMARY KEY) ENGINE=InnoDB")
	for _, n := range twins {
		testutil.MustExec(t, source, "CREATE TABLE `"+n+"` (v INT) ENGINE=InnoDB")
	}
	if _, err := metadata.TakeSnapshotExcludingInvalid(source, index, []string{src}); err != nil {
		t.Fatalf("TakeSnapshotExcludingInvalid: %v", err)
	}

	c := status.LoadTableCapture(ctx, index)
	if c.State != status.TableCaptureChecked {
		t.Fatalf("State = %v, want checked", c.State)
	}
	var got []string
	for _, u := range c.Uncaptured {
		got = append(got, u.Table)
	}
	slices.Sort(got)
	want := slices.Clone(twins)
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Fatalf("uncaptured = %q, want every twin listed on its own: %q", got, want)
	}
}
