//go:build integration

package mcptools

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// TestIntegrationLiveFirstArchiveSkip is the #1410 wiring proof against a real
// planner: hourly live partitions, one registered archive BELOW them whose
// only file is not Parquet. Reading that archive fails (query warns, recover
// refuses), so each assertion tells a skip from a read. The unit tests pin the
// decision with a fake plan; this pins that the real Plan/PlanBrowse, scoped
// to the discovered source, hands the proofs what they need.
func TestIntegrationLiveFirstArchiveSkip(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)
	if err := indexer.EnsureSchema(db); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	floor := time.Date(2026, 6, 10, 10, 0, 0, 0, time.UTC)
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{floor, floor.Add(time.Hour), floor.Add(2 * time.Hour)})
	testutil.InsertEvent(t, db, "bin.000001", 100, 200, "2026-06-10 11:00:00", nil,
		"app", "orders", 2, "1", nil,
		[]byte(`{"id":1,"status":"old"}`), []byte(`{"id":1,"status":"new"}`))

	base := filepath.Join(t.TempDir(), "bintrail_id=skip1410")
	hourDir := filepath.Join(base, "event_date=2026-06-01", "event_hour=12")
	if err := os.MkdirAll(hourDir, 0o755); err != nil {
		t.Fatal(err)
	}
	garbage := filepath.Join(hourDir, "events.parquet")
	if err := os.WriteFile(garbage, []byte("not parquet"), 0o644); err != nil {
		t.Fatal(err)
	}
	testutil.MustExec(t, db, `INSERT INTO archive_state
		(partition_name, bintrail_id, local_path, row_count, s3_bucket, s3_key, s3_uploaded_at)
		VALUES ('p_2026060112', 'skip1410', ?, 1, NULL, NULL, NULL)`, garbage)

	withName := Config{Resolve: func(context.Context, string) (*Target, error) {
		return &Target{DB: db, DBName: dbName}, nil
	}}
	noName := Config{Resolve: func(context.Context, string) (*Target, error) {
		return &Target{DB: db}, nil
	}}
	ctx := context.Background()

	t.Run("recover: the row's latest event is live", func(t *testing.T) {
		res, _, err := MakeRecoverTool(withName)(ctx, &mcp.CallToolRequest{},
			RecoverArgs{Schema: "app", Table: "orders", PK: "1", LimitPerPK: 1})
		if err != nil {
			t.Fatal(err)
		}
		txt := resultText(res)
		if res.IsError {
			t.Fatalf("the live index answers this reversal; the archive must not be read: %s", txt)
		}
		if !strings.Contains(txt, "old") || !strings.Contains(txt, recoverArchivesSkippedNote()) {
			t.Errorf("want the reversal plus the skip record, got: %s", txt)
		}
	})

	t.Run("recover: fewer live events than asked", func(t *testing.T) {
		res, _, err := MakeRecoverTool(withName)(ctx, &mcp.CallToolRequest{},
			RecoverArgs{Schema: "app", Table: "orders", PK: "1", LimitPerPK: 2})
		if err != nil {
			t.Fatal(err)
		}
		if !res.IsError || !strings.Contains(resultText(res), base) {
			t.Fatalf("the archive may extend this row, so it is read and its failure refuses: %s", resultText(res))
		}
	})

	t.Run("recover: target without a database name reads the archives", func(t *testing.T) {
		res, _, err := MakeRecoverTool(noName)(ctx, &mcp.CallToolRequest{},
			RecoverArgs{Schema: "app", Table: "orders", PK: "1", LimitPerPK: 1})
		if err != nil {
			t.Fatal(err)
		}
		if !res.IsError {
			t.Fatalf("without a database name no plan exists, so the archive is read and refuses: %s", resultText(res))
		}
	})

	t.Run("query: since exactly at the live floor", func(t *testing.T) {
		res, _, err := MakeQueryTool(withName)(ctx, &mcp.CallToolRequest{},
			QueryArgs{Schema: "app", Table: "orders", Since: "2026-06-10 10:00:00"})
		if err != nil {
			t.Fatal(err)
		}
		txt := resultText(res)
		if strings.Contains(txt, "archive_source_skipped") || !strings.Contains(txt, queryArchivesSkippedNote()) {
			t.Errorf("since at the live floor must skip the archive and say so, got: %s", txt)
		}
	})

	t.Run("query: since one second below the live floor", func(t *testing.T) {
		res, _, err := MakeQueryTool(withName)(ctx, &mcp.CallToolRequest{},
			QueryArgs{Schema: "app", Table: "orders", Since: "2026-06-10 09:59:59"})
		if err != nil {
			t.Fatal(err)
		}
		txt := resultText(res)
		if !strings.Contains(txt, "archive_source_skipped") || strings.Contains(txt, "archives_skipped:") {
			t.Errorf("below the floor the archive must be read (and warn), with no skip claimed, got: %s", txt)
		}
	})
}
