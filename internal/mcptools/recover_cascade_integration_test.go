//go:build integration

package mcptools

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// seedCascadeIndex builds an index holding an ON DELETE CASCADE topology: two
// child INSERTs (id=10,11 → pid=1) followed by the parent DELETE (id=1), with
// the FK snapshot and a schema snapshot — the same fixture shape the console's
// recover-cascade endpoint tests use, so the two surfaces are proven over the
// same topology.
func seedCascadeIndex(t *testing.T) (*sql.DB, string) {
	t.Helper()
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	h := time.Now().UTC().Add(-1 * time.Hour).Truncate(time.Hour)
	testutil.SetupPartitionedTable(t, db, dbName, []time.Time{h})
	childTs := h.Add(10 * time.Minute).Format("2006-01-02 15:04:05")
	parentTs := h.Add(20 * time.Minute).Format("2006-01-02 15:04:05")

	testutil.InsertEvent(t, db, "binlog.000001", 100, 200, childTs, nil,
		dbName, "child", 1 /*INSERT*/, "10", nil, nil, []byte(`{"id":10,"pid":1}`))
	testutil.InsertEvent(t, db, "binlog.000001", 200, 300, childTs, nil,
		dbName, "child", 1 /*INSERT*/, "11", nil, nil, []byte(`{"id":11,"pid":1}`))
	testutil.InsertEvent(t, db, "binlog.000001", 300, 400, parentTs, nil,
		dbName, "parent", 3 /*DELETE*/, "1", nil, []byte(`{"id":1}`), nil)

	testutil.MustExec(t, db, `INSERT INTO fk_constraints
		(snapshot_id, constraint_name, schema_name, table_name, column_name, ordinal_position,
		 referenced_schema_name, referenced_table_name, referenced_column_name, delete_rule, update_rule)
		VALUES (1, 'fk_child', ?, 'child', 'pid', 1, ?, 'parent', 'id', 'CASCADE', 'RESTRICT')`,
		dbName, dbName)

	snapTs := h.Format("2006-01-02 15:04:05")
	testutil.InsertSnapshot(t, db, 1, snapTs, dbName, "parent", "id", 1, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, db, 1, snapTs, dbName, "child", "id", 1, "PRI", "int", "NO")
	testutil.InsertSnapshot(t, db, 1, snapTs, dbName, "child", "pid", 2, "", "int", "YES")
	return db, dbName
}

// cascadeSession connects an in-memory MCP client over the standalone posture
// with recover_cascade registered.
func cascadeSession(t *testing.T, db *sql.DB, dbName string) *mcp.ClientSession {
	t.Helper()
	resolver, err := metadata.NewResolver(db, 0)
	if err != nil {
		t.Fatalf("load resolver: %v", err)
	}
	cfg := Config{
		Version:             "test",
		RecoverCascade:      true,
		AllowBaselineParams: true,
		Resolve: func(ctx context.Context, _ string) (*Target, error) {
			return &Target{DB: db, DBName: dbName, Resolver: resolver, ResolverLoaded: true}, nil
		},
	}

	ctx := context.Background()
	clientT, serverT := mcp.NewInMemoryTransports()
	ss, err := NewServer(cfg).Connect(ctx, serverT, nil)
	if err != nil {
		t.Fatalf("server connect: %v", err)
	}
	t.Cleanup(func() { _ = ss.Close() })

	client := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "2025-06-18"}, nil)
	cs, err := client.Connect(ctx, clientT, nil)
	if err != nil {
		t.Fatalf("client connect: %v", err)
	}
	t.Cleanup(func() { _ = cs.Close() })
	return cs
}

// TestIntegrationRecoverCascadeTool drives the tool end-to-end over a seeded
// ON DELETE CASCADE: the payload must re-insert the parent and both
// cascade-deleted children inside the FK-checks wrapper, report complete, and
// carry the structured counts an agent needs to sanity-check the script.
func TestIntegrationRecoverCascadeTool(t *testing.T) {
	db, dbName := seedCascadeIndex(t)
	cs := cascadeSession(t, db, dbName)

	res, err := cs.CallTool(context.Background(), &mcp.CallToolParams{
		Name:      "recover_cascade",
		Arguments: map[string]any{"schema": dbName, "table": "parent"},
	})
	if err != nil {
		t.Fatalf("CallTool recover_cascade: %v", err)
	}
	text := resultText(res)
	if res.IsError {
		t.Fatalf("recover_cascade returned a tool error: %s", text)
	}
	var out recoverCascadeResult
	if err := json.Unmarshal([]byte(text), &out); err != nil {
		t.Fatalf("decode payload: %v (payload=%s)", err, text)
	}

	for _, want := range []string{
		"SET FOREIGN_KEY_CHECKS=0;",
		"SET FOREIGN_KEY_CHECKS=1;",
		"Phase-1",
		"`" + dbName + "`.`parent`",
		"`" + dbName + "`.`child`",
	} {
		if !strings.Contains(out.SQL, want) {
			t.Errorf("SQL missing %q\n---\n%s", want, out.SQL)
		}
	}
	if c := strings.Count(out.SQL, "`"+dbName+"`.`child`"); c != 2 {
		t.Errorf("want 2 child INSERTs, got %d\n---\n%s", c, out.SQL)
	}
	if out.Children != 2 || out.ParentDeletes != 1 || out.Parents != 1 {
		t.Errorf("children=%d parent_deletes=%d parents=%d, want 2/1/1", out.Children, out.ParentDeletes, out.Parents)
	}
	if out.StatementCount != 3 {
		t.Errorf("statement_count = %d, want 3 (parent + 2 children)", out.StatementCount)
	}
	if !out.Complete || len(out.Incomplete) != 0 {
		t.Errorf("a clean cascade with no archives must be complete; incomplete=%v", out.Incomplete)
	}
	if out.BaselineActive {
		t.Error("baseline_active must be false without a configured baseline")
	}
}

// TestIntegrationRecoverCascadeToolPKFilter pins that the pk filter scopes the
// parent scan: a pk that matches no parent change produces a complete, empty
// script plus the empty-match advisory.
func TestIntegrationRecoverCascadeToolPKFilter(t *testing.T) {
	db, dbName := seedCascadeIndex(t)
	cs := cascadeSession(t, db, dbName)

	res, err := cs.CallTool(context.Background(), &mcp.CallToolParams{
		Name:      "recover_cascade",
		Arguments: map[string]any{"schema": dbName, "table": "parent", "pk": "999"},
	})
	if err != nil {
		t.Fatalf("CallTool recover_cascade: %v", err)
	}
	if res.IsError {
		t.Fatalf("tool error: %s", resultText(res))
	}
	var out recoverCascadeResult
	if err := json.Unmarshal([]byte(resultText(res)), &out); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if out.StatementCount != 0 || !out.Complete {
		t.Errorf("statement_count=%d complete=%v, want 0/true", out.StatementCount, out.Complete)
	}
	advisory := false
	for _, w := range out.Warnings {
		if strings.Contains(w, "no parent DELETE or UPDATE events matched") {
			advisory = true
		}
	}
	if !advisory {
		t.Errorf("warnings must carry the empty-match advisory, got %v", out.Warnings)
	}
}

// TestIntegrationRecoverCascadeTool_archivesOutsideWindowKeepBaseline pins the
// MCP wiring of the #1615 gate: an unrelated archived partition must not skip
// baseline augmentation when the live index holds the whole [snapshot, T]
// window — the untouched baseline child (12) is recovered and the run is
// complete.
func TestIntegrationRecoverCascadeTool_archivesOutsideWindowKeepBaseline(t *testing.T) {
	db, dbName := seedCascadeIndex(t)
	cs := cascadeSession(t, db, dbName)
	testutil.MustExec(t, db, `INSERT INTO archive_state
		(bintrail_id, partition_name, local_path, s3_bucket, s3_key)
		VALUES ('bt', 'p_2026010100', '', 'bucket', 'pfx/bintrail_id=bt/p_2026010100.parquet')`)

	h := time.Now().UTC().Add(-1 * time.Hour).Truncate(time.Hour) // the fixture's live hour
	dir := t.TempDir()
	path := filepath.Join(dir, h.Add(5*time.Minute).Format("2006-01-02T15-04-05Z"), dbName, "child.parquet")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	cols := []baseline.Column{
		{Name: "id", MySQLType: "int", ParquetType: baseline.MysqlToParquetNode("int")},
		{Name: "pid", MySQLType: "int", ParquetType: baseline.MysqlToParquetNode("int")},
	}
	w, err := baseline.NewWriter(path, cols, baseline.WriterConfig{Compression: "none", RowGroupSize: 10})
	if err != nil {
		t.Fatalf("baseline.NewWriter: %v", err)
	}
	for _, r := range [][]string{{"10", "1"}, {"11", "1"}, {"12", "1"}} {
		if err := w.WriteRow(r, []bool{false, false}); err != nil {
			t.Fatalf("WriteRow: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("writer close: %v", err)
	}

	res, err := cs.CallTool(context.Background(), &mcp.CallToolParams{
		Name:      "recover_cascade",
		Arguments: map[string]any{"schema": dbName, "table": "parent", "baseline_dir": dir},
	})
	if err != nil {
		t.Fatalf("CallTool recover_cascade: %v", err)
	}
	text := resultText(res)
	if res.IsError {
		t.Fatalf("recover_cascade returned a tool error: %s", text)
	}
	var out recoverCascadeResult
	if err := json.Unmarshal([]byte(text), &out); err != nil {
		t.Fatalf("decode payload: %v (payload=%s)", err, text)
	}
	if out.Children != 3 || !out.BaselineActive {
		t.Errorf("children=%d baseline_active=%v, want 3/true (untouched baseline child 12 recovered)\n---\n%s", out.Children, out.BaselineActive, out.SQL)
	}
	if !out.Complete {
		t.Errorf("a live-contiguous window must be complete despite an unrelated archive; incomplete=%v", out.Incomplete)
	}
}
