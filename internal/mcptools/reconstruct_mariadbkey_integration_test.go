//go:build integration

package mcptools

import (
	"encoding/base64"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// The MCP twin of the CLI's TestRunReconstruct_MariaDBUUIDKeyWithoutIndexTypesRefuses:
// with no schema snapshot typing the table at the baseline's time, a MariaDB
// UUID key cannot be spelled
// the way the index stores it, and the tool must refuse instead of returning
// the baseline-era row as the state at `at`.
func TestIntegrationReconstructTool_MariaDBUUIDKeyWithoutIndexTypesRefuses(t *testing.T) {
	db, dbName, baseDir := seedReconstructIndex(t)
	dir := filepath.Join(baseDir, strings.ReplaceAll(time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC).Format(time.RFC3339), ":", "-"), "mdb")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	create := "CREATE TABLE `devices` (\n  `u` uuid NOT NULL,\n  `n` int(11) DEFAULT NULL,\n  PRIMARY KEY (`u`)\n) ENGINE=InnoDB;\n"
	cols := []baseline.Column{
		{Name: "u", MySQLType: "uuid", ParquetType: baseline.MysqlToParquetNode("uuid")},
		{Name: "n", MySQLType: "int", ParquetType: baseline.MysqlToParquetNode("int")},
	}
	w, err := baseline.NewWriter(filepath.Join(dir, "devices.parquet"), cols, baseline.WriterConfig{
		Compression: "none", RowGroupSize: 100, Metadata: map[string]string{baseline.MetaKeyCreateTableSQL: create}})
	if err != nil {
		t.Fatal(err)
	}
	const key = "00000000-0000-0000-0000-000000000001"
	if err := w.WriteRow([]string{key, "1"}, []bool{false, false}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	kb, _ := hex.DecodeString("00000000000000000000000000000001")
	pk := event.BuildPKValues([]metadata.ColumnMeta{{Name: "u", DataType: "uuid"}}, map[string]any{"u": kb})
	u := base64.StdEncoding.EncodeToString(kb)
	testutil.InsertEvent(t, db, "bin.000001", 200, 240, "2026-06-01 12:00:00", nil, "mdb", "devices", 2, pk,
		nil, []byte(`{"u":"`+u+`","n":1}`), []byte(`{"u":"`+u+`","n":2}`))

	// The table joins the schema snapshots only AFTER the baseline was taken:
	// the tool knows the key column from the latest snapshot, but the
	// snapshot in effect at the baseline (which types the key) lacks it.
	testutil.InsertSnapshot(t, db, 2, "2026-06-01 06:00:00", "mdb", "devices", "u", 1, "PRI", "uuid", "NO")
	testutil.InsertSnapshot(t, db, 2, "2026-06-01 06:00:00", "mdb", "devices", "n", 2, "", "int", "YES")
	cs := reconstructSession(t, db, dbName)
	res := callReconstructTool(t, cs, map[string]any{
		"schema": "mdb", "table": "devices", "pk": key,
		"at": "2026-06-01 13:30:00", "baseline_dir": baseDir, "allow_gaps": true,
	})
	text := resultText(res)
	if !res.IsError {
		t.Fatalf("the tool answered a UUID key without the index's column types: %s", text)
	}
	if !strings.Contains(text, "bintrail snapshot") {
		t.Errorf("refusal should name the fix (bintrail snapshot): %s", text)
	}
}
