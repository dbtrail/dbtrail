//go:build integration

package cliapp

import (
	"context"
	"testing"

	"github.com/dbtrail/dbtrail/internal/testutil"
)

// `bintrail init` against an index created before idx_type_lookup is the
// explicit way to build it: at once, online.
func TestRunInit_buildsTheTypeIndexOnAnExistingIndex(t *testing.T) {
	db, name := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	testutil.MustExec(t, db, "ALTER TABLE binlog_events DROP INDEX idx_type_lookup")

	savedDSN, savedBucket, savedARN, savedFormat := initIndexDSN, initS3Bucket, initS3ARN, initFormat
	t.Cleanup(func() { initIndexDSN, initS3Bucket, initS3ARN, initFormat = savedDSN, savedBucket, savedARN, savedFormat })
	initIndexDSN, initS3Bucket, initS3ARN, initFormat = testutil.IntegrationDSN(name), "", "", "json"
	initCmd.SetContext(context.Background())
	if err := runInit(initCmd, nil); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM information_schema.STATISTICS
		WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'binlog_events' AND INDEX_NAME = 'idx_type_lookup'`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 4 {
		t.Fatalf("idx_type_lookup has %d key columns after init, want 4", n)
	}
}
