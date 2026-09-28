//go:build integration

package mcptools

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// A TRUNCATE that ran before the snapshot's time and was indexed after it
// (capture was behind, #1912): the reconstruct tool refuses, in words an MCP
// client can act on, and one from before the snapshot's position does not.
func TestIntegrationReconstructToolLateTruncateRefuses(t *testing.T) {
	db, dbName, baseDir := seedReconstructIndex(t)
	cs := reconstructSession(t, db, dbName)

	// A newer snapshot than the seeded one, anchored at bin.000009:500.
	snapTime := time.Date(2026, 6, 1, 11, 0, 0, 0, time.UTC)
	w, err := baseline.NewWriter(
		filepath.Join(baseDir, reconstruct.SnapshotDirName(snapTime), "app", "users.parquet"),
		[]baseline.Column{
			{Name: "id", MySQLType: "int", ParquetType: baseline.MysqlToParquetNode("int")},
			{Name: "name", MySQLType: "varchar", ParquetType: baseline.MysqlToParquetNode("varchar")},
		},
		baseline.WriterConfig{Compression: "none", RowGroupSize: 100, Metadata: map[string]string{
			baseline.MetaKeyBinlogFile: "bin.000009",
			baseline.MetaKeyBinlogPos:  "500",
		}})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.WriteRow([]string{"1", "alice"}, []bool{false, false}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	args := map[string]any{
		"schema": "app", "table": "users", "pk": "1",
		"at": "2026-06-01 11:30:00", "baseline_dir": baseDir, "allow_gaps": true,
	}
	record := func(file string, endPos uint64, ranAt string) {
		t.Helper()
		testutil.MustExec(t, db, `INSERT INTO schema_changes
			(detected_at, binlog_file, binlog_pos, schema_name, table_name, ddl_type, ddl_query)
			VALUES (?, ?, ?, 'app', 'users', 'TRUNCATE TABLE', 'TRUNCATE TABLE users')`, ranAt, file, endPos)
	}

	record("bin.000008", 900, "2026-06-01 10:00:00")
	if r := decodeReconstruct(t, callReconstructTool(t, cs, args)); r.State["name"] != "alice" {
		t.Fatalf("a TRUNCATE from before the snapshot: %+v", r)
	}

	record("bin.000010", 120, "2026-06-01 10:59:00")
	res := callReconstructTool(t, cs, args)
	if !res.IsError {
		t.Fatalf("the tool answered over a TRUNCATE indexed late: %s", resultText(res))
	}
	text := resultText(res)
	for _, want := range []string{"TRUNCATE TABLE on app.users", "2026-06-01T10:59:00Z", "bin.000010:120", "Take a new snapshot"} {
		if !strings.Contains(text, want) {
			t.Errorf("the refusal does not say %q: %s", want, text)
		}
	}
	if strings.Contains(text, "--") {
		t.Errorf("the refusal hands an MCP client a command-line flag: %s", text)
	}
}
