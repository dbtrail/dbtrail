//go:build integration

package indexer

import (
	"context"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/testutil"
)

// A new index records the retention it starts with (#1709). The rotation loop
// reads that record, not the running binary's default, so a later change of
// default moves new indexes only.
func TestCreateIndexTables_recordsTheDefaultOnANewIndex(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	ctx := context.Background()

	// stream_state already names its CHECK constraint single_row, and MySQL
	// scopes those names to the database: this call is also what proves the
	// new table does not collide with it.
	if err := CreateIndexTables(ctx, db, 2, false, nil); err != nil {
		t.Fatalf("CreateIndexTables: %v", err)
	}
	got, recordedAt, found, err := ReadInitialRetain(ctx, db, dbName)
	if err != nil || !found || got != DefaultRotateRetain {
		t.Fatalf("ReadInitialRetain = %q, %v, %v; want %q, true, nil", got, found, err, DefaultRotateRetain)
	}
	// The rotation loop keys the upgrade-guard exemption on this timestamp, so
	// a zero one would exempt an index whose history predates its own record.
	if recordedAt.IsZero() || time.Since(recordedAt) > time.Hour {
		t.Errorf("recorded_at = %v, want the moment the index was created", recordedAt)
	}
}

// Running init again (every `up` and `watch` boot does) must not rewrite the
// record: it is the retention the index STARTED with.
func TestCreateIndexTables_keepsTheFirstRecord(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	ctx := context.Background()

	if err := CreateIndexTables(ctx, db, 2, false, nil); err != nil {
		t.Fatalf("CreateIndexTables: %v", err)
	}
	// What an older build with another default would have written.
	testutil.MustExec(t, db, "UPDATE rotation_policy SET initial_retain = '7d' WHERE id = 1")
	if err := CreateIndexTables(ctx, db, 2, false, nil); err != nil {
		t.Fatalf("CreateIndexTables again: %v", err)
	}
	got, _, found, err := ReadInitialRetain(ctx, db, dbName)
	if err != nil || !found || got != "7d" {
		t.Fatalf("ReadInitialRetain = %q, %v, %v; want the first record 7d kept", got, found, err)
	}
	var rows int
	if err := db.QueryRow("SELECT COUNT(*) FROM rotation_policy").Scan(&rows); err != nil || rows != 1 {
		t.Fatalf("rotation_policy rows = %d, %v; want exactly 1", rows, err)
	}
}

// An index an older build created has binlog_events and no record. Running
// this build's init over it creates the table but must NOT write the current
// default: the absence is how the loop knows the index predates the record.
func TestCreateIndexTables_existingIndexGetsNoRecord(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	ctx := context.Background()

	testutil.InitIndexTables(t, db) // binlog_events and friends, no rotation_policy
	if err := CreateIndexTables(ctx, db, 2, false, nil); err != nil {
		t.Fatalf("CreateIndexTables over an existing index: %v", err)
	}
	got, _, found, err := ReadInitialRetain(ctx, db, dbName)
	if err != nil {
		t.Fatalf("ReadInitialRetain: %v", err)
	}
	if found {
		t.Fatalf("an index that existed before the record got one (%q): it would be read as new and lose its history to the new default", got)
	}
}

// The table missing entirely (an index no build of this series has touched)
// reads as "no record", not as an error.
func TestReadInitialRetain_missingTableIsNoRecord(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	got, _, found, err := ReadInitialRetain(context.Background(), db, dbName)
	if err != nil || found || got != "" {
		t.Fatalf("ReadInitialRetain = %q, %v, %v; want \"\", false, nil", got, found, err)
	}
}

// A record read back is returned as stored, trimmed; the caller parses it.
func TestReadInitialRetain_returnsTheStoredValue(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.MustExec(t, db, DDLRotationPolicy)
	testutil.MustExec(t, db, "INSERT INTO rotation_policy (id, initial_retain) VALUES (1, ' 48h ')")

	got, _, found, err := ReadInitialRetain(context.Background(), db, dbName)
	if err != nil || !found || got != "48h" {
		t.Fatalf("ReadInitialRetain = %q, %v, %v; want \"48h\", true, nil", got, found, err)
	}
}
