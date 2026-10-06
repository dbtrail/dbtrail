//go:build integration

package verify

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestIntegrationSnapshotCutWalkIsPruned2150: the walk that places the cut
// reads only the partitions at or after its floor (the read's opening, less
// an hour), not every hourly partition of the index on every page.
func TestIntegrationSnapshotCutWalkIsPruned2150(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Hour)
	var hours []time.Time
	for h := now.Add(-48 * time.Hour); !h.After(now.Add(time.Hour)); h = h.Add(time.Hour) {
		hours = append(hours, h)
	}
	testutil.SetupPartitionedTable(t, db, dbName, hours)

	floor := time.Now().UTC().Add(-snapshotCutFloorMargin)
	var parts sql.NullString
	clause, _, err := reconstruct.PartitionsAtOrAfter(context.Background(), db, floor)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := db.Query("EXPLAIN "+snapshotCutFirstPageSQL(floor, clause, snapshotCutPage), floor)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	cols, _ := rows.Columns()
	vals := make([]sql.RawBytes, len(cols))
	ptrs := make([]any, len(cols))
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	if !rows.Next() {
		t.Fatal("EXPLAIN returned no row")
	}
	if err := rows.Scan(ptrs...); err != nil {
		t.Fatal(err)
	}
	for i, c := range cols {
		if c == "partitions" {
			parts = sql.NullString{String: string(vals[i]), Valid: vals[i] != nil}
		}
	}
	got := strings.Split(parts.String, ",")
	t.Logf("partitions read: %s", parts.String)
	if !parts.Valid || len(got) > 4 {
		t.Fatalf("the walk reads %d partitions (%s) of %d; want only those at or after its floor's hour", len(got), parts.String, len(hours)+1)
	}
	if strings.Contains(parts.String, indexer.PartitionName(now.Add(-48*time.Hour))) {
		t.Fatalf("the walk reads the oldest partition: %s", parts.String)
	}
}
