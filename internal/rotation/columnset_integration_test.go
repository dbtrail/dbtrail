//go:build integration

package rotation_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/archive"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/rotation"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// The FORWARD half of #1535: an archive rotation writes now carries its own
// column set, so a deployment on this build never needs the reconcile backfill
// for anything it archives from here on.
//
// It is a separate test from the reconcile one on purpose. That one inserts its
// archive_state row BY HAND, so it exercises the repair and not the wiring from
// ArchivePartition's report into rotation's upsert — which is why blanking
// `st.Columns` at that call site survived the whole suite, unit and integration
// alike, leaving the feature inert with CI green.
func TestIntegrationRotateRecordsTheColumnSet(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	ctx := context.Background()

	// One partition old enough for retention to archive and drop it.
	h := time.Now().UTC().Add(-48 * time.Hour).Truncate(time.Hour)
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{h})
	testutil.InsertEvent(t, db, "binlog.000001", 100, 200,
		h.Add(30*time.Minute).Format("2006-01-02 15:04:05"), nil,
		"testdb", "orders", 1, "42", nil, nil, []byte(`{"id":42}`))

	const bintrailID = "deadbeef-dead-beef-dead-beefdeadbeef"
	if _, err := rotation.Perform(ctx, db, dbName, rotation.Options{
		RetainDur:          24 * time.Hour,
		RetainRaw:          "24h",
		ArchiveDir:         t.TempDir(),
		ArchiveCompression: "zstd",
		BintrailID:         bintrailID,
		Format:             "json",
	}); err != nil {
		t.Fatalf("rotation.Perform: %v", err)
	}

	var recorded sql.NullString
	if err := db.QueryRowContext(ctx,
		`SELECT column_set FROM archive_state WHERE bintrail_id = ?`, bintrailID).Scan(&recorded); err != nil {
		t.Fatalf("read back column_set: %v", err)
	}
	want := archive.ColumnSet(archive.BinlogEventColumns)
	if !recorded.Valid || recorded.String != want {
		t.Fatalf("rotate recorded column_set = %+v, want %q.\n"+
			"An empty value is worse than a NULL one: reconcile then reports "+
			"\"column set drift\" instead of \"not recorded\", and the views stay on the per-file bind.",
			recorded, want)
	}
}
