//go:build integration

package verify

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// End to end through the index: a change whose earlier row holds the stored
// not-captured marker is counted on the table's result as not checkable, never
// as a mismatch, and the fully captured row beside it is still checked.
func TestVerifyRecoverInputs_NotCapturedIsCountedNotAMismatch(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	for _, c := range []struct {
		name             string
		ord              int
		key, dt, colType string
	}{
		{"id", 1, "PRI", "int", "int"},
		{"name", 2, "", "varchar", "varchar(64)"},
	} {
		testutil.MustExec(t, db, `INSERT INTO schema_snapshots
			(snapshot_id, snapshot_time, schema_name, table_name, column_name,
			 ordinal_position, column_key, data_type, column_type, is_nullable, is_generated)
			VALUES (1, UTC_TIMESTAMP(), ?, 'orders', ?, ?, ?, ?, ?, 'NO', 0)`,
			dbName, c.name, c.ord, c.key, c.dt, c.colType)
	}
	now := time.Now().UTC()
	curHour := now.Truncate(time.Hour)
	h1 := curHour.Add(-time.Hour)
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{h1, curHour})

	const tsFmt = "2006-01-02 15:04:05"
	at := func(min int) string { return now.Add(time.Duration(-min) * time.Minute).Format(tsFmt) }
	testutil.InsertEvent(t, db, "binlog.000001", 100, 200, at(5), nil, dbName, "orders", 1, "7", nil,
		nil, []byte(`{"id":7,"name":"a"}`))
	testutil.InsertEvent(t, db, "binlog.000001", 200, 300, at(4), nil, dbName, "orders", 2, "7", nil,
		[]byte(`{"id":7,"name":{"__bintrail_not_captured__":true}}`), []byte(`{"id":7,"name":"b"}`))
	testutil.InsertEvent(t, db, "binlog.000001", 300, 400, at(3), nil, dbName, "orders", 1, "8", nil,
		nil, []byte(`{"id":8,"name":"x"}`))
	testutil.InsertEvent(t, db, "binlog.000001", 400, 500, at(2), nil, dbName, "orders", 2, "8", nil,
		[]byte(`{"id":8,"name":"x"}`), []byte(`{"id":8,"name":"y"}`))

	resolver, err := metadata.NewResolver(db, 1)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}
	res, err := VerifyRecoverInputs(context.Background(), RecoverInputsConfig{
		IndexDB: db, Resolver: resolver, IndexDBName: dbName, NoArchive: true, Since: h1, Until: now,
	}, dbName, "orders")
	if err != nil {
		t.Fatalf("VerifyRecoverInputs: %v", err)
	}
	t.Logf("detail as shown: %s", res.Detail)
	if res.Status != StatusMatch {
		t.Fatalf("status = %s, want match: %s", res.Status, res.Detail)
	}
	if res.ChangesNotCaptured != 1 {
		t.Errorf("ChangesNotCaptured = %d, want 1", res.ChangesNotCaptured)
	}
	if !strings.Contains(res.Detail, "could not be checked: the earlier row was not captured") {
		t.Errorf("detail must say why:\n%s", res.Detail)
	}
}
