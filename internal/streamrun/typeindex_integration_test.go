//go:build integration

package streamrun

import (
	"context"
	"testing"
	"time"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"

	"github.com/dbtrail/dbtrail/internal/testutil"
)

// The capture daemon builds idx_type_lookup on an index that lacks it, in the
// background: here the source is unreachable, so One fails at once, and the
// build it started before reaching the source still completes.
func TestOne_buildsTheTypeIndexInTheBackground(t *testing.T) {
	indexDB, indexName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, indexDB)
	testutil.MustExec(t, indexDB, "ALTER TABLE binlog_events DROP INDEX idx_type_lookup")

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	err := One(ctx, Config{
		IndexDSN:   testutil.IntegrationDSN(indexName),
		SourceDSN:  testutil.IntegrationDSN(indexName + "_no_such_source"),
		Flavor:     gomysql.MySQLFlavor,
		ServerID:   99893,
		BatchSize:  1,
		Checkpoint: 1,
		GapTimeout: 30,
		Format:     "text",
		SSLMode:    "preferred",
		Deps:       testStreamDeps(),
	})
	if err == nil {
		t.Fatal("One against a missing source returned nil")
	}
	for {
		var n int
		if err := indexDB.QueryRow(`SELECT COUNT(*) FROM information_schema.STATISTICS
			WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'binlog_events' AND INDEX_NAME = 'idx_type_lookup'`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n == 4 {
			return
		}
		if ctx.Err() != nil {
			t.Fatal("the capture daemon never built idx_type_lookup")
		}
		time.Sleep(100 * time.Millisecond)
	}
}
